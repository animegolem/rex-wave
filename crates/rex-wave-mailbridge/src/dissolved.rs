//! Dissolved content-addressed (`sha256`) mail storage, atomic Markdown mirror
//! export, drop-folder ingest, crash-recovery startup `reconcile`, lossless
//! `rebuild` from mirror, and read-only `migrate-verify`.
//!
//! Conforms directly to `specs/mail/MailStore.tla`:
//! - Single commit point = SQLite `StreamId::Mail` transaction (before export).
//! - Immutable blob store `<mirror>/objects/<sha256>.md` (`write_atomic_if_absent_path`).
//! - Human-readable working-tree letter `<mirror>/<rel_path>` (`SingleHashPerPath`:
//!   never overwrites an edited working-tree file).
//! - Post-ack edits on an already-acked path are recorded as `mutated_after_ack`
//!   (`flagged`) and are NEVER ringable (`EditNeverRingable`).
//! - Startup `reconcile` exports any committed SQLite messages/acks missing on
//!   disk and ingests any unindexed drop/working-tree/blob files.
//! - Deleting `journal.sqlite3` and running `rebuild` from `<mirror>` reproduces
//!   100% of `db`, `acked`, `flagged`, FTS5 index, and DM/thread room projections.

use crate::acks::{
    AckIndex, AckRecord, AliasMap, letter_paths_match, normalize_letter_path, normalize_seat_token,
    parse_acks_jsonl,
};
use crate::frontmatter::{
    FrontmatterDoc, is_receipt_only_entry, parse_recipient_list, unquote_yaml,
    yaml_quote_if_needed, yaml_quote_inline,
};
use crate::presence::{DeliveryStatus, PresenceState, format_delivery_status};
use crate::shadow::{ReadOnlyInterchange, extract_letter_timestamp_detailed};
use crate::{
    MAX_LETTER_BYTES, SANDBOX_DIR_MODE, SANDBOX_FILE_MODE, SandboxRoot, sanitize_topic_for_file,
    sha256_bytes, sha256_short16,
};
use base64::Engine as _;
use rex_wave_api::mail::*;
use rex_wave_api::room::{
    Body, Mentions, PostOrigin, PrincipalPair, RoomEvent, RoomId, RoomOrigin, RoomPost, RoomRef,
    RoomScope,
};
use rex_wave_api::*;
use rex_wave_fs::{
    ConfinedDir, EntryKind, OutsidePath, ProtectedRootsGuard, Protection, RelPath, SafeName,
    SandboxError,
};
use rex_wave_runtime::{DecideCtx, Service};
use rex_wave_store::{Appended, Journal, StreamWriter};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;

const MAX_ACKS_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ROOM_BODY_BYTES: usize = 64 * 1024;

/// True when `path` is an Emacs lock symlink (`.#*`) or autosave file (`#*#`).
pub fn is_emacs_lock_or_autosave(name: &str) -> bool {
    name.starts_with(".#") || (name.len() >= 2 && name.starts_with('#') && name.ends_with('#'))
}

/// True when an Emacs `.#<filename>` lock symlink exists alongside `target`.
pub fn has_emacs_lock(target: &Path) -> bool {
    let Some(name) = target.file_name().and_then(|s| s.to_str()) else {
        return false;
    };
    let lock_name = format!(".#{name}");
    let lock_path = match target.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(lock_name),
        _ => PathBuf::from(lock_name),
    };
    rex_wave_fs::entry_exists(&lock_path)
}

fn is_ignored_md_filename(name: &str) -> bool {
    name.starts_with('.')
        || name.ends_with('~')
        || is_emacs_lock_or_autosave(name)
        || !name.ends_with(".md")
        || name.to_ascii_lowercase().starts_with("readme")
        || name.eq_ignore_ascii_case("chatter.md")
}

/// Exact raw bytes of a committed [`MailMessage`], preserving byte-for-byte
/// `sha256` fidelity even for non-UTF-8 historical files.
pub fn message_raw_bytes(msg: &MailMessage) -> Result<Vec<u8>, ApiError> {
    if let Some(ref b64) = msg.raw_bytes_b64 {
        base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| ApiError::Internal(format!("decode mail raw_bytes_b64: {e}")))
    } else {
        Ok(msg.raw_markdown.as_bytes().to_vec())
    }
}

fn sanitize_channel_slug(raw: &str, max_len: usize) -> String {
    let mut out = String::new();
    let mut prev_dash = false;
    for ch in raw.chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_lowercase() || lower.is_ascii_digit() || lower == '_' {
            out.push(lower);
            prev_dash = false;
        } else if (lower == '-' || lower.is_whitespace() || matches!(lower, '/' | '.' | ':'))
            && !prev_dash
            && !out.is_empty()
        {
            out.push('-');
            prev_dash = true;
        }
        if out.len() >= max_len {
            break;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    while out.starts_with('-') {
        out.remove(0);
    }
    out
}

fn strip_reply_prefixes(mut subject: &str) -> &str {
    loop {
        let trimmed = subject.trim();
        let lower = trimmed.to_ascii_lowercase();
        if let Some(rest) = lower
            .strip_prefix("re:")
            .or_else(|| lower.strip_prefix("fwd:"))
            .or_else(|| lower.strip_prefix("fw:"))
        {
            let offset = trimmed.len() - rest.len();
            subject = &trimmed[offset..];
        } else {
            return trimmed;
        }
    }
}

/// Derives the projected [`RoomId`] for a dissolved mail message:
/// - 1:1 mail between two distinct seats (`to.len() == 1 && cc.is_empty() && from != to[0]`):
///   `RoomId::Direct` so it appears directly under `▾ Direct Messages` in `SidebarProjection` / TUI!
/// - Multi-seat mail (`>= 2` distinct participants across `from`, `to`, `cc` when not 1:1 DM):
///   global channel `RoomId::Channel { scope: Global, name: "mail-<slug>" }`.
/// - Self-mail or recipient-less historical notes: `None`.
pub fn derive_mail_room(
    from: &str,
    to: &[SeatName],
    cc: &[SeatName],
    topic: Option<&str>,
    subject: &str,
    sha256: &Sha256Hex,
) -> Option<RoomId> {
    let from_seat = SeatName::parse(normalize_seat_token(from)).ok();
    let mut all_recips: Vec<SeatName> = Vec::new();
    for s in to.iter().chain(cc.iter()) {
        if !all_recips.contains(s) {
            all_recips.push(s.clone());
        }
    }
    if let Some(ref fs) = from_seat
        && to.len() == 1
        && cc.is_empty()
        && &to[0] != fs
    {
        let pair = PrincipalPair::new(
            Principal::Seat { seat: fs.clone() },
            Principal::Seat {
                seat: to[0].clone(),
            },
        )
        .ok()?;
        return Some(RoomId::Direct { pair });
    }

    let mut distinct_seats = BTreeSet::new();
    if let Some(ref fs) = from_seat {
        distinct_seats.insert(fs.clone());
    }
    for s in &all_recips {
        distinct_seats.insert(s.clone());
    }
    if distinct_seats.len() < 2 {
        return None;
    }

    let base_slug = topic
        .map(|t| sanitize_channel_slug(t, 56))
        .filter(|s| !s.is_empty())
        .or_else(|| {
            let clean_subj = strip_reply_prefixes(subject);
            let s = sanitize_channel_slug(clean_subj, 48);
            (!s.is_empty()).then_some(s)
        })
        .unwrap_or_else(|| sha256.as_str()[..12].to_owned());
    let chan_name = format!("mail-{base_slug}");
    let truncated = if chan_name.len() > 64 {
        chan_name[..64].trim_end_matches('-').to_owned()
    } else {
        chan_name
    };
    ChannelName::parse(truncated)
        .ok()
        .map(|name| RoomId::Channel {
            scope: RoomScope::Global,
            name,
        })
}

fn format_yaml_seat_list(seats: &[SeatName]) -> String {
    if seats.len() == 1 {
        seats[0].as_str().to_owned()
    } else {
        let joined: Vec<&str> = seats.iter().map(SeatName::as_str).collect();
        format!("[{}]", joined.join(", "))
    }
}

fn format_yaml_string_list(items: &[String]) -> String {
    let joined: Vec<String> = items
        .iter()
        .map(|s| yaml_quote_if_needed(s.trim()))
        .collect();
    format!("[{}]", joined.join(", "))
}

fn sanitize_slug_segment(raw: &str, max_len: usize) -> String {
    let mut out: String = raw
        .trim()
        .chars()
        .filter_map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
                Some(c.to_ascii_lowercase())
            } else if c.is_whitespace() || c == '/' || c == '.' {
                Some('-')
            } else {
                None
            }
        })
        .collect();
    while out.starts_with('-') {
        out.remove(0);
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.len() > max_len {
        out.truncate(max_len);
        while out.ends_with('-') {
            out.pop();
        }
    }
    if out.is_empty() {
        out.push_str("seat");
    }
    out
}

/// Synthesizes a canonical deterministic Markdown [`MailMessage`] from a
/// [`MailSend`] request, computing the exact `sha256` of the rendered bytes.
#[allow(clippy::too_many_arguments)]
pub fn synthesize_mail_message(
    from: &str,
    to: Vec<SeatName>,
    cc: Vec<SeatName>,
    subject: &str,
    body: &str,
    priority: LetterPriority,
    topic: Option<&str>,
    in_reply_to: Option<&str>,
    refs: &[String],
    created_at: UnixMs,
) -> Result<MailMessage, ApiError> {
    if to.is_empty() {
        return Err(ApiError::Invalid(InvalidId {
            what: "to".into(),
            reason: "at least one recipient seat is required".into(),
        }));
    }
    let subject_clean: String = subject
        .trim()
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .filter(|c| !c.is_control())
        .collect();
    if subject_clean.is_empty() {
        return Err(ApiError::Invalid(InvalidId {
            what: "subject".into(),
            reason: "must be non-empty without control characters".into(),
        }));
    }
    if body.is_empty() || body.len() as u64 > MAX_LETTER_BYTES {
        return Err(ApiError::Invalid(InvalidId {
            what: "body".into(),
            reason: format!("body must be 1..={MAX_LETTER_BYTES} bytes"),
        }));
    }

    let mut unique_to = Vec::with_capacity(to.len());
    for seat in to {
        if !unique_to.contains(&seat) {
            unique_to.push(seat);
        }
    }
    let mut unique_cc = Vec::with_capacity(cc.len());
    for seat in cc {
        if !unique_to.contains(&seat) && !unique_cc.contains(&seat) {
            unique_cc.push(seat);
        }
    }

    let topic_clean = topic
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(sanitize_topic_for_file);
    let in_reply_to_clean = in_reply_to
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    if let Some(ref r) = in_reply_to_clean
        && r.chars().any(char::is_control)
    {
        return Err(ApiError::Invalid(InvalidId {
            what: "in_reply_to".into(),
            reason: "must not contain control characters".into(),
        }));
    }
    let mut clean_refs = Vec::new();
    for r in refs {
        let trimmed = r.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.chars().any(char::is_control) {
            return Err(ApiError::Invalid(InvalidId {
                what: "refs".into(),
                reason: "must not contain control characters".into(),
            }));
        }
        if !clean_refs.iter().any(|existing| existing == trimmed) {
            clean_refs.push(trimmed.to_owned());
        }
    }

    let from_norm = normalize_seat_token(from);
    let from_clean = if from_norm.is_empty() {
        "unknown".to_owned()
    } else {
        from_norm
    };
    let body_normalized = if body.ends_with('\n') {
        body.to_owned()
    } else {
        format!("{body}\n")
    };

    let mut fingerprint = Sha256::new();
    fingerprint.update(from_clean.as_bytes());
    fingerprint.update(b"\n");
    for s in &unique_to {
        fingerprint.update(s.as_str().as_bytes());
        fingerprint.update(b",");
    }
    fingerprint.update(b"\n");
    for s in &unique_cc {
        fingerprint.update(s.as_str().as_bytes());
        fingerprint.update(b",");
    }
    fingerprint.update(b"\n");
    fingerprint.update(subject_clean.as_bytes());
    fingerprint.update(b"\n");
    fingerprint.update(priority.as_str().as_bytes());
    fingerprint.update(b"\n");
    if let Some(ref t) = topic_clean {
        fingerprint.update(t.as_bytes());
    }
    fingerprint.update(b"\n");
    if let Some(ref r) = in_reply_to_clean {
        fingerprint.update(r.as_bytes());
    }
    fingerprint.update(b"\n");
    for r in &clean_refs {
        fingerprint.update(r.as_bytes());
        fingerprint.update(b",");
    }
    fingerprint.update(b"\n");
    fingerprint.update(body_normalized.as_bytes());
    let fp_hex: String = fingerprint
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let short8 = &fp_hex[..8];

    let from_slug = sanitize_slug_segment(&from_clean, 24);
    let to_slug = sanitize_slug_segment(unique_to[0].as_str(), 24);
    let mut id = match &topic_clean {
        Some(t) => format!(
            "{}-{}-to-{}-{}-{short8}",
            created_at.0,
            from_slug,
            to_slug,
            sanitize_slug_segment(t, 32)
        ),
        None => format!("{}-{}-to-{}-{short8}", created_at.0, from_slug, to_slug),
    };
    if id.len() > 110 {
        id.truncate(110);
        while id.ends_with('-') {
            id.pop();
        }
    }
    let path = PathBuf::from(format!("mail/{id}.md"));
    let created_at_iso = created_at.to_rfc3339_sec();

    let mut raw = String::new();
    raw.push_str("---\n");
    raw.push_str(&format!("id: {id}\n"));
    raw.push_str(&format!("from: {from_clean}\n"));
    raw.push_str(&format!("to: {}\n", format_yaml_seat_list(&unique_to)));
    if !unique_cc.is_empty() {
        raw.push_str(&format!("cc: {}\n", format_yaml_seat_list(&unique_cc)));
    }
    raw.push_str(&format!("subject: {}\n", yaml_quote_inline(&subject_clean)));
    raw.push_str(&format!("priority: {}\n", priority.as_str()));
    if let Some(ref t) = topic_clean {
        raw.push_str(&format!("topic: {t}\n"));
    }
    raw.push_str(&format!("created_at: {created_at_iso}\n"));
    if let Some(ref parent) = in_reply_to_clean {
        raw.push_str(&format!("in_reply_to: {}\n", yaml_quote_if_needed(parent)));
    }
    if !clean_refs.is_empty() {
        raw.push_str(&format!("refs: {}\n", format_yaml_string_list(&clean_refs)));
    }
    raw.push_str("---\n\n");
    raw.push_str(&body_normalized);

    if raw.len() as u64 > MAX_LETTER_BYTES {
        return Err(ApiError::Invalid(InvalidId {
            what: "body".into(),
            reason: format!("rendered letter exceeds {MAX_LETTER_BYTES} bytes"),
        }));
    }

    let sha256 = sha256_bytes(raw.as_bytes());
    let room = derive_mail_room(
        &from_clean,
        &unique_to,
        &unique_cc,
        topic_clean.as_deref(),
        &subject_clean,
        &sha256,
    );

    Ok(MailMessage {
        sha256,
        id,
        from: from_clean,
        to: unique_to,
        cc: unique_cc,
        subject: subject_clean,
        body: body_normalized,
        path,
        priority,
        topic: topic_clean,
        in_reply_to: in_reply_to_clean,
        refs: clean_refs,
        created_at,
        created_at_iso,
        receipt_only: false,
        room,
        raw_markdown: raw,
        raw_bytes_b64: None,
        origin: MailOrigin::Send,
    })
}

fn parse_yaml_string_list(val: &str) -> Vec<String> {
    let trimmed = val.trim().trim_start_matches('[').trim_end_matches(']');
    let mut out = Vec::new();
    for item in trimmed.split(',') {
        let cleaned = unquote_yaml(item).trim().to_owned();
        if !cleaned.is_empty() && !out.contains(&cleaned) {
            out.push(cleaned);
        }
    }
    out
}

fn parse_priority_str(raw: &str) -> LetterPriority {
    match raw.trim().to_ascii_lowercase().as_str() {
        "low" => LetterPriority::Low,
        "high" => LetterPriority::High,
        "blocking" | "urgent" | "critical" => LetterPriority::Blocking,
        _ => LetterPriority::Normal,
    }
}

fn fallback_subject_from_body_or_path(body: &str, rel_path: &str) -> String {
    for line in body.lines().take(20) {
        let trimmed = line.trim();
        if let Some(heading) = trimmed.strip_prefix("# ") {
            let clean = heading.trim();
            if !clean.is_empty() {
                return clean.chars().take(160).collect();
            }
        }
    }
    let basename = rel_path.rsplit('/').next().unwrap_or(rel_path);
    basename.strip_suffix(".md").unwrap_or(basename).to_owned()
}

/// Parses raw on-disk Markdown bytes into a [`MailMessage`], preserving exact
/// raw-byte `sha256` without mutating or re-rendering the bytes.
pub fn parse_mail_message_from_bytes(
    rel_path: &str,
    raw_bytes: &[u8],
    box_seat: Option<&SeatName>,
    extra_recipients: &[SeatName],
    mtime_ms: Option<UnixMs>,
    aliases: &AliasMap,
    origin: MailOrigin,
) -> MailMessage {
    let sha256 = sha256_bytes(raw_bytes);
    let (raw_markdown, raw_bytes_b64) = match std::str::from_utf8(raw_bytes) {
        Ok(s) => (s.to_owned(), None),
        Err(_) => (
            String::from_utf8_lossy(raw_bytes).into_owned(),
            Some(base64::engine::general_purpose::STANDARD.encode(raw_bytes)),
        ),
    };

    let doc = FrontmatterDoc::parse(&raw_markdown);
    let entries = if doc.has_frontmatter {
        doc.entries.clone()
    } else {
        FrontmatterDoc::parse_headers_lenient(&raw_markdown)
    };

    let mut from_raw: Option<String> = None;
    let mut authors = Vec::new();
    let mut to_cands = Vec::new();
    let mut cc_cands = Vec::new();
    let mut subject: Option<String> = None;
    let mut id_field: Option<String> = None;
    let mut date_ms: Option<String> = None;
    let mut date_field: Option<String> = None;
    let mut created_at_field: Option<String> = None;
    let mut ts_field: Option<String> = None;
    let mut in_reply_to: Option<String> = None;
    let mut topic: Option<String> = None;
    let mut priority = LetterPriority::Normal;
    let mut refs: Vec<String> = Vec::new();
    let mut receipt_only = false;

    for entry in entries {
        let key = entry.key.to_ascii_lowercase();
        let val = entry.value;
        if val.is_empty() {
            continue;
        }
        match key.as_str() {
            "from" | "author" => {
                authors.extend(parse_recipient_list(&val));
                if from_raw.is_none() || key == "from" {
                    from_raw = Some(val);
                }
            }
            "to" => to_cands.extend(parse_recipient_list(&val)),
            "cc" => cc_cands.extend(parse_recipient_list(&val)),
            "subject" => subject = Some(val),
            "id" => id_field = Some(val),
            "date_ms" => date_ms = Some(val),
            "date" => date_field = Some(val),
            "created_at" => created_at_field = Some(val),
            "ts" => ts_field = Some(val),
            "in_reply_to" => in_reply_to = Some(val),
            "topic" => topic = Some(val),
            "priority" => priority = parse_priority_str(&val),
            "refs" => {
                for r in parse_yaml_string_list(&val) {
                    if !refs.contains(&r) {
                        refs.push(r);
                    }
                }
            }
            other if is_receipt_only_entry(other, &val) => {
                receipt_only = true;
            }
            _ => {}
        }
    }

    let parsed_time = extract_letter_timestamp_detailed(
        rel_path,
        id_field.as_deref(),
        date_ms.as_deref(),
        date_field.as_deref().or(created_at_field.as_deref()),
        ts_field.as_deref(),
        in_reply_to.as_deref(),
        mtime_ms,
    );

    let from = from_raw
        .as_deref()
        .map(|s| {
            let canon = aliases.canonicalize(s);
            if canon.is_empty() {
                s.trim().to_owned()
            } else {
                canon
            }
        })
        .unwrap_or_else(|| "unknown".to_owned());

    let mut author_seats = BTreeSet::new();
    for cand in &authors {
        if let Some(seat) = aliases.canonical_seat_name(&cand.raw) {
            author_seats.insert(seat);
        }
    }

    let mut to_seats: Vec<SeatName> = Vec::new();
    let mut cc_seats: Vec<SeatName> = Vec::new();
    if let Some(seat) = box_seat {
        to_seats.push(seat.clone());
    }
    for cand in &to_cands {
        if cand.require_known_alias && !aliases.contains_known(&cand.raw) {
            continue;
        }
        if let Some(seat) = aliases.canonical_seat_name(&cand.raw)
            && !to_seats.contains(&seat)
        {
            to_seats.push(seat);
        }
    }
    for cand in &cc_cands {
        if cand.require_known_alias && !aliases.contains_known(&cand.raw) {
            continue;
        }
        if let Some(seat) = aliases.canonical_seat_name(&cand.raw)
            && !to_seats.contains(&seat)
            && !cc_seats.contains(&seat)
        {
            cc_seats.push(seat);
        }
    }
    for seat in extra_recipients {
        if !to_seats.contains(seat) && !cc_seats.contains(seat) {
            to_seats.push(seat.clone());
        }
    }
    if box_seat.is_none() && to_seats.len() + cc_seats.len() > 1 {
        to_seats.retain(|s| !author_seats.contains(s));
        cc_seats.retain(|s| !author_seats.contains(s));
    }

    let body = doc
        .body
        .strip_prefix("\r\n")
        .or_else(|| doc.body.strip_prefix('\n'))
        .unwrap_or(&doc.body)
        .to_owned();
    let basename = rel_path.rsplit('/').next().unwrap_or(rel_path);
    let id =
        id_field.unwrap_or_else(|| basename.strip_suffix(".md").unwrap_or(basename).to_owned());
    let subject = subject.unwrap_or_else(|| fallback_subject_from_body_or_path(&body, rel_path));
    let created_at = parsed_time.ts;
    let created_at_iso = created_at.to_rfc3339_sec();
    let room = derive_mail_room(
        &from,
        &to_seats,
        &cc_seats,
        topic.as_deref(),
        &subject,
        &sha256,
    );

    MailMessage {
        sha256,
        id,
        from,
        to: to_seats,
        cc: cc_seats,
        subject,
        body,
        path: PathBuf::from(rel_path),
        priority,
        topic,
        in_reply_to,
        refs,
        created_at,
        created_at_iso,
        receipt_only,
        room,
        raw_markdown,
        raw_bytes_b64,
        origin,
    }
}

/// Formats a dissolved [`MailMessage`] into a bounded room [`Body`] for
/// projection into `RoomId::Direct` or a multi-seat mail channel.
pub fn format_mail_room_body(msg: &MailMessage) -> Body {
    let sha16 = &msg.sha256.as_str()[..16];
    let to_list: Vec<&str> = msg.to.iter().map(SeatName::as_str).collect();
    let mut text = format!(
        "**{}**\n`mail` · `from:@{}` · `to:{}` · `sha256:{sha16}` · `{}`\n\n{}",
        msg.subject,
        msg.from,
        to_list.join(","),
        msg.path.display(),
        msg.body.trim_end()
    );
    if text.len() > MAX_ROOM_BODY_BYTES {
        text = truncate_with_notice(&text, MAX_ROOM_BODY_BYTES);
    }
    if text.is_empty() {
        text = format!("(empty mail {sha16})");
    }
    Body::parse(text).expect("bounded non-empty mail room body")
}

/// Canonical room labels attached to a projected mail message.
pub fn mail_room_labels(msg: &MailMessage) -> Vec<Label> {
    let mut labels = Vec::new();
    if let Ok(l) = Label::parse("kind=mail") {
        labels.push(l);
    }
    if let Ok(l) = Label::parse(format!("sha16={}", &msg.sha256.as_str()[..16])) {
        labels.push(l);
    }
    if msg.priority != LetterPriority::Normal
        && let Ok(l) = Label::parse(format!("priority={}", msg.priority.as_str()))
    {
        labels.push(l);
    }
    labels
}

/// One committed dissolved mail entry stored in [`MailState`].
#[derive(Clone, Debug)]
pub struct CommittedMailEntry {
    pub seq: Seq,
    pub message: MailMessage,
    pub mutated_after_ack: bool,
    pub prior_ack_sha: Option<String>,
}

/// One committed ACK record stored in [`MailState`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedAckRecord {
    pub seq: Seq,
    pub seat: SeatName,
    pub sha256: Sha256Hex,
    pub path: PathBuf,
    pub note: Option<String>,
    pub at: UnixMs,
}

/// Deterministic in-memory projection of `StreamId::Mail` used by `MailBridge`,
/// startup `reconcile`, offline `rebuild`, and `MailStore.tla` conformance checks.
#[derive(Clone, Debug, Default)]
pub struct MailState {
    pub messages: BTreeMap<Sha256Hex, CommittedMailEntry>,
    pub by_path: BTreeMap<String, Vec<Sha256Hex>>,
    pub by_id: BTreeMap<String, Sha256Hex>,
    pub acked: BTreeSet<(SeatName, Sha256Hex)>,
    pub ack_records: BTreeMap<(SeatName, Sha256Hex), CommittedAckRecord>,
    pub acks_by_path: BTreeMap<String, Vec<(SeatName, Sha256Hex)>>,
    pub acks_by_basename: BTreeMap<String, Vec<(SeatName, Sha256Hex)>>,
    pub acked_shas: BTreeSet<Sha256Hex>,
    pub flagged_shas: BTreeSet<Sha256Hex>,
    pub room_post_seqs: BTreeMap<(RoomId, Sha256Hex), Seq>,
    pub seen_wakes: HashSet<WakeId>,
}

fn derive_ack_seat(
    env: &Envelope,
    path: &Path,
    explicit_seat: Option<&SeatName>,
) -> Option<SeatName> {
    if let Some(s) = explicit_seat {
        return Some(s.clone());
    }
    if let Some(norm) = normalize_letter_path(&path.to_string_lossy())
        && let Some((first, _)) = norm.canonical.split_once('/')
        && let Some(raw_seat) = first.strip_prefix("to-")
        && let Ok(seat) = SeatName::parse(raw_seat)
    {
        return Some(seat);
    }
    match &env.by {
        Principal::Operator { seat } | Principal::Seat { seat } => Some(seat.clone()),
        _ => None,
    }
}

impl MailState {
    /// Replays all `StreamId::Mail` entries from `journal` into a fresh [`MailState`].
    pub fn from_journal(journal: &Journal) -> Result<Self, ApiError> {
        let mut state = Self::default();
        let mut after = Seq::ORIGIN;
        loop {
            let page = journal
                .read(&StreamId::Mail, after, 512)
                .map_err(|e| ApiError::Internal(e.to_string()))?;
            if page.is_empty() {
                break;
            }
            for env in &page {
                after = env.seq;
                state.apply(env);
            }
        }
        Ok(state)
    }

    pub fn apply(&mut self, env: &Envelope) {
        if env.stream != StreamId::Mail {
            return;
        }
        match &env.event {
            Event::Mail(MailEvent::Banked { letter, .. }) => {
                self.seen_wakes.insert(letter.wake_id.clone());
            }
            Event::Mail(MailEvent::Projected { item }) => {
                let path_str = item.reference.path.to_string_lossy();
                if let Ok(seat) = SeatName::parse(&item.to)
                    && let Some((box_name, file_name)) = path_str.rsplit_once('/')
                {
                    self.seen_wakes.insert(crate::wake_id(
                        &seat,
                        box_name,
                        file_name,
                        &item.reference.sha256,
                    ));
                }
            }
            Event::Mail(MailEvent::MessageCommitted {
                message,
                mutated_after_ack,
                prior_ack_sha,
            }) => {
                let sha = message.sha256.clone();
                if let Some(norm) = normalize_letter_path(&message.path.to_string_lossy()) {
                    let list = self.by_path.entry(norm.canonical).or_default();
                    if !list.contains(&sha) {
                        list.push(sha.clone());
                    }
                }
                if !message.id.is_empty() {
                    self.by_id
                        .entry(message.id.clone())
                        .or_insert_with(|| sha.clone());
                }
                if *mutated_after_ack {
                    self.flagged_shas.insert(sha.clone());
                }
                self.messages
                    .entry(sha)
                    .or_insert_with(|| CommittedMailEntry {
                        seq: env.seq,
                        message: (**message).clone(),
                        mutated_after_ack: *mutated_after_ack,
                        prior_ack_sha: prior_ack_sha.clone(),
                    });
            }
            Event::Mail(MailEvent::Acked {
                path,
                sha256,
                seat,
                note,
            }) => {
                if let Some(acker) = derive_ack_seat(env, path, seat.as_ref()) {
                    let key = (acker.clone(), sha256.clone());
                    self.acked.insert(key.clone());
                    self.acked_shas.insert(sha256.clone());
                    self.ack_records
                        .entry(key.clone())
                        .or_insert_with(|| CommittedAckRecord {
                            seq: env.seq,
                            seat: acker.clone(),
                            sha256: sha256.clone(),
                            path: path.clone(),
                            note: note.clone(),
                            at: env.at,
                        });
                    if let Some(norm) = normalize_letter_path(&path.to_string_lossy()) {
                        let by_p = self.acks_by_path.entry(norm.canonical).or_default();
                        if !by_p.contains(&key) {
                            by_p.push(key.clone());
                        }
                        let by_b = self.acks_by_basename.entry(norm.basename).or_default();
                        if !by_b.contains(&key) {
                            by_b.push(key);
                        }
                    }
                }
            }
            Event::Mail(MailEvent::BellRang { .. }) => {}
            _ => {}
        }
    }

    /// `MailStore.tla` `IsEdit(m, acked)`: returns `Some(prior_ack_sha)` when
    /// `rel_path` already had an ACKed hash `m0 != candidate_sha` and `candidate_sha`
    /// is not itself already ACKed.
    pub fn is_edit_after_ack(&self, rel_path: &str, candidate_sha: &Sha256Hex) -> Option<String> {
        if self.acked_shas.contains(candidate_sha) {
            return None;
        }
        let norm = normalize_letter_path(rel_path)?;
        if let Some(entries) = self.acks_by_path.get(&norm.canonical) {
            for (_, prior) in entries {
                if prior != candidate_sha {
                    return Some(prior.as_str().to_owned());
                }
            }
        }
        if let Some(entries) = self.acks_by_basename.get(&norm.basename) {
            for (seat, prior) in entries {
                if prior != candidate_sha
                    && let Some(rec) = self.ack_records.get(&(seat.clone(), prior.clone()))
                    && letter_paths_match(&rec.path.to_string_lossy(), &norm.canonical)
                {
                    return Some(prior.as_str().to_owned());
                }
            }
        }
        None
    }

    /// Returns the set of ringable (`db \ (acked ∪ flagged)`) message hashes for `seat`.
    pub fn ringable_for_seat(&self, seat: &SeatName) -> BTreeSet<Sha256Hex> {
        let mut out = BTreeSet::new();
        for (sha, entry) in &self.messages {
            let is_recipient = entry.message.to.contains(seat) || entry.message.cc.contains(seat);
            if !is_recipient
                || entry.mutated_after_ack
                || entry.message.receipt_only
                || self.acked.contains(&(seat.clone(), sha.clone()))
            {
                continue;
            }
            out.insert(sha.clone());
        }
        out
    }

    /// Returns all committed `sha256`s (`db` in `MailStore.tla`).
    pub fn db_set(&self) -> BTreeSet<Sha256Hex> {
        self.messages.keys().cloned().collect()
    }

    /// Resolves a message by exact `sha256` or normalized relative `path`.
    pub fn lookup_target(
        &self,
        path: Option<&Path>,
        sha256: Option<&Sha256Hex>,
    ) -> Option<&CommittedMailEntry> {
        if let Some(sha) = sha256 {
            return self.messages.get(sha);
        }
        let p = path?;
        let norm = normalize_letter_path(&p.to_string_lossy())?;
        if let Some(shas) = self.by_path.get(&norm.canonical) {
            // Prefer the most recent unflagged message on that path, else latest.
            if let Some(entry) = shas
                .iter()
                .rev()
                .filter_map(|s| self.messages.get(s))
                .find(|e| !e.mutated_after_ack)
            {
                return Some(entry);
            }
            return shas.last().and_then(|s| self.messages.get(s));
        }
        // Fallback: match bare filename against unique path entry.
        let mut matched: Vec<&CommittedMailEntry> = Vec::new();
        for (p_key, shas) in &self.by_path {
            if letter_paths_match(p_key, &norm.canonical) {
                for s in shas {
                    if let Some(e) = self.messages.get(s) {
                        matched.push(e);
                    }
                }
            }
        }
        if matched.len() == 1 {
            matched.into_iter().next()
        } else {
            matched.into_iter().rev().find(|e| !e.mutated_after_ack)
        }
    }

    /// Builds a `MailSummary` for `entry` from `viewer_seat`'s perspective.
    pub fn summarize_for(
        &self,
        entry: &CommittedMailEntry,
        viewer_seat: Option<&SeatName>,
    ) -> MailSummary {
        let msg = &entry.message;
        let acked = match viewer_seat {
            Some(seat) => self.acked.contains(&(seat.clone(), msg.sha256.clone())),
            None => self.acked_shas.contains(&msg.sha256),
        };
        MailSummary {
            sha256: msg.sha256.clone(),
            sha16: msg.sha256.as_str()[..16].to_owned(),
            id: msg.id.clone(),
            from: msg.from.clone(),
            to: msg.to.clone(),
            cc: msg.cc.clone(),
            subject: msg.subject.clone(),
            path: msg.path.clone(),
            priority: msg.priority,
            topic: msg.topic.clone(),
            in_reply_to: msg.in_reply_to.clone(),
            created_at: msg.created_at,
            acked,
            mutated_after_ack: entry.mutated_after_ack,
            prior_ack_sha: entry.prior_ack_sha.clone(),
            receipt_only: msg.receipt_only,
            room: msg.room.clone(),
            snippet: mail_snippet(&msg.body, 160),
        }
    }

    /// Fast `mail check` projection for `seat`, including sender delivery notices
    /// when any recipient of an unacked letter previously sent by `seat` is
    /// unregistered, dormant, retired, or offline.
    pub fn check_for_seat(
        &self,
        seat: &SeatName,
        include_all: bool,
        limit: Option<u32>,
        presence: &PresenceState,
        now: UnixMs,
    ) -> MailCheckView {
        let cap = limit.unwrap_or(200).clamp(1, 1000) as usize;
        let mut all_for_seat: Vec<&CommittedMailEntry> = self
            .messages
            .values()
            .filter(|e| e.message.to.contains(seat) || e.message.cc.contains(seat))
            .collect();
        all_for_seat
            .sort_by(|a, b| (b.message.created_at, b.seq).cmp(&(a.message.created_at, a.seq)));

        let mut unread_count = 0usize;
        let mut filtered: Vec<MailSummary> = Vec::new();
        for entry in all_for_seat {
            let summary = self.summarize_for(entry, Some(seat));
            let is_unread = !summary.acked && !summary.mutated_after_ack && !summary.receipt_only;
            if is_unread {
                unread_count += 1;
            }
            if include_all || is_unread {
                filtered.push(summary);
            }
        }
        let total_count = filtered.len();
        let truncated = total_count > cap;
        filtered.truncate(cap);

        let delivery_notices = self.sender_delivery_notices_for(seat, presence, now);

        MailCheckView {
            seat: seat.clone(),
            unread_count,
            total_count,
            truncated,
            messages: filtered,
            delivery_notices,
        }
    }

    /// Computes delivery notices for recipients of unacked letters sent by `sender`
    /// who are unregistered, dormant, retired, or offline in `presence`.
    pub fn sender_delivery_notices_for(
        &self,
        sender: &SeatName,
        presence: &PresenceState,
        now: UnixMs,
    ) -> Vec<MailDeliveryNotice> {
        let mut pending_recipients: BTreeSet<SeatName> = BTreeSet::new();
        for entry in self.messages.values() {
            if entry.mutated_after_ack || entry.message.receipt_only {
                continue;
            }
            if normalize_seat_token(&entry.message.from) != sender.as_str() {
                continue;
            }
            for recip in entry.message.to.iter().chain(entry.message.cc.iter()) {
                if recip == sender {
                    continue;
                }
                if !self
                    .acked
                    .contains(&(recip.clone(), entry.message.sha256.clone()))
                {
                    pending_recipients.insert(recip.clone());
                }
            }
        }
        delivery_notices_for_seats(pending_recipients.iter(), presence, now)
    }

    /// Looks up the projected room `Seq` of a parent message referenced by
    /// `in_reply_to` (matching either a full `sha256`, 16-hex prefix, or `id`).
    pub fn resolve_parent_room_seq(&self, room: &RoomId, in_reply_to: &str) -> Option<Seq> {
        let trimmed = in_reply_to.trim();
        if let Ok(sha) = Sha256Hex::parse(trimmed.to_ascii_lowercase())
            && let Some(&seq) = self.room_post_seqs.get(&(room.clone(), sha))
        {
            return Some(seq);
        }
        if let Some(sha) = self.by_id.get(trimmed)
            && let Some(&seq) = self.room_post_seqs.get(&(room.clone(), sha.clone()))
        {
            return Some(seq);
        }
        let lower = trimmed.to_ascii_lowercase();
        if lower.len() >= 8 && lower.bytes().all(|b| b.is_ascii_hexdigit()) {
            for ((r, sha), &seq) in &self.room_post_seqs {
                if r == room && sha.as_str().starts_with(&lower) {
                    return Some(seq);
                }
            }
        }
        None
    }
}

/// Evaluates presence delivery status for a set of recipient seats.
pub fn delivery_notices_for_seats<'a>(
    seats: impl IntoIterator<Item = &'a SeatName>,
    presence: &PresenceState,
    now: UnixMs,
) -> Vec<MailDeliveryNotice> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for seat in seats {
        if !seen.insert(seat.clone()) {
            continue;
        }
        if presence.house().is_operator_seat(seat.as_str()) {
            continue;
        }
        let unregistered = presence.seat(seat).is_none() && presence.bindings_for(seat).is_empty();
        match format_delivery_status(presence, seat, now) {
            DeliveryStatus::Live { .. } => {}
            DeliveryStatus::Dormant { notice, .. } => out.push(MailDeliveryNotice {
                seat: seat.clone(),
                status: "dormant".into(),
                notice,
            }),
            DeliveryStatus::Retired { notice, .. } => out.push(MailDeliveryNotice {
                seat: seat.clone(),
                status: "retired".into(),
                notice,
            }),
            DeliveryStatus::Offline { notice, .. } => out.push(MailDeliveryNotice {
                seat: seat.clone(),
                status: if unregistered {
                    "unregistered".into()
                } else {
                    "offline".into()
                },
                notice: if unregistered {
                    format!("recipient @{seat} is unregistered; banked in journal and will deliver on claim/bind")
                } else {
                    notice
                },
            }),
        }
    }
    out
}

pub fn mail_snippet(body: &str, max_chars: usize) -> String {
    let trimmed = body.trim();
    let mut out = String::new();
    let mut count = 0usize;
    let mut prev_ws = false;
    for ch in trimmed.chars() {
        if ch.is_control() || ch.is_whitespace() {
            if !prev_ws && !out.is_empty() {
                out.push(' ');
                count += 1;
                prev_ws = true;
            }
        } else {
            out.push(ch);
            count += 1;
            prev_ws = false;
        }
        if count >= max_chars {
            out.push('…');
            break;
        }
    }
    out
}

/// Verified mirror + optional drop-folder target enforced by `protected-roots`.
#[derive(Clone, Debug)]
pub struct MirrorTarget {
    dir: ConfinedDir,
    guard: ProtectedRootsGuard,
    drop_dir: Option<PathBuf>,
}

impl MirrorTarget {
    pub fn from_sandbox(sandbox: &SandboxRoot, drop_dir: Option<PathBuf>) -> Self {
        Self {
            dir: sandbox.dir.clone(),
            guard: sandbox.guard.clone(),
            drop_dir,
        }
    }

    /// Opens (creating if missing ONLY after verifying outside `protected-roots`)
    /// a verified mirror directory and optional separate drop directory using an
    /// already-resolved [`ProtectedRootsGuard`].
    pub fn open_with_guard(
        mirror_path: &Path,
        drop_dir: Option<&Path>,
        guard: ProtectedRootsGuard,
    ) -> Result<Self, SandboxError> {
        let canon = verify_or_create_dir_outside(&guard, mirror_path)?;
        let dir = ConfinedDir::open(&canon).map_err(|e| SandboxError::Io(e.to_string()))?;

        let verified_drop = match drop_dir {
            Some(d) => {
                let drop_canon = verify_or_create_dir_outside(&guard, d)?;
                if drop_canon == canon {
                    None
                } else {
                    Some(drop_canon)
                }
            }
            None => None,
        };

        Ok(Self {
            dir,
            guard,
            drop_dir: verified_drop,
        })
    }

    /// Opens (creating if missing ONLY after verifying outside `protected-roots`)
    /// a verified mirror directory and optional separate drop directory.
    pub fn open_verified(
        mirror_path: &Path,
        drop_dir: Option<&Path>,
        protection: &Protection,
    ) -> Result<Self, SandboxError> {
        let guard = ProtectedRootsGuard::resolve(protection)?;
        Self::open_with_guard(mirror_path, drop_dir, guard)
    }

    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    pub fn drop_dir(&self) -> Option<&Path> {
        self.drop_dir.as_deref()
    }

    fn ensure_subdir_segments(&self, segments: &[&str]) -> Result<PathBuf, ApiError> {
        if segments.is_empty() {
            return Ok(self.dir.path().to_path_buf());
        }
        let names = segments
            .iter()
            .map(|seg| SafeName::parse(seg))
            .collect::<Result<Vec<_>, _>>()
            .and_then(RelPath::from_names)
            .map_err(|e| {
                ApiError::Invalid(InvalidId {
                    what: "mirror subdir".into(),
                    reason: format!("invalid directory segment: {e}"),
                })
            })?;
        let sub = self
            .dir
            .ensure_subdir(&names, SANDBOX_DIR_MODE)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::InvalidInput => ApiError::Denied {
                    principal: "mailbridge".into(),
                    action: format!("write through symlink: {e}"),
                },
                std::io::ErrorKind::NotADirectory => ApiError::conflict(e.to_string()),
                _ => ApiError::Internal(format!("mkdir {names}: {e}")),
            })?;
        if self.guard.check(&sub, &sub).is_err() {
            return Err(ApiError::Denied {
                principal: "mailbridge".into(),
                action: format!("escape mirror via {}", sub.display()),
            });
        }
        Ok(sub)
    }

    /// Exports immutable blob `<mirror>/objects/<sha256>.md` (`blobStore` in
    /// `MailStore.tla`) using `write_atomic_if_absent_path`. Returns `Ok(true)`
    /// when newly created, `Ok(false)` when already present.
    pub fn export_blob_if_absent(
        &self,
        sha256: &Sha256Hex,
        raw_bytes: &[u8],
    ) -> Result<bool, ApiError> {
        let objects_dir = self.ensure_subdir_segments(&["objects"])?;
        let dest = objects_dir.join(format!("{}.md", sha256.as_str()));
        rex_wave_fs::write_atomic_if_absent_path(&dest, raw_bytes, SANDBOX_FILE_MODE, true)
            .map_err(|e| ApiError::Internal(format!("export blob {}: {e}", dest.display())))
    }

    /// Exports human-readable working-tree letter `<mirror>/<rel_path>`
    /// (`mirrorPath` in `MailStore.tla`) using `write_atomic_if_absent_path`.
    /// NEVER overwrites an existing working-tree file so `SingleHashPerPath`
    /// and in-place post-ack edits are preserved.
    pub fn export_working_tree_if_absent(
        &self,
        rel_path: &Path,
        raw_bytes: &[u8],
    ) -> Result<bool, ApiError> {
        let raw_str = rel_path.to_string_lossy();
        let norm = normalize_letter_path(&raw_str).ok_or_else(|| {
            ApiError::Invalid(InvalidId {
                what: "path".into(),
                reason: format!("invalid relative mail path '{}'", rel_path.display()),
            })
        })?;
        let parts: Vec<&str> = norm.canonical.split('/').collect();
        if parts.is_empty()
            || parts
                .iter()
                .any(|p| p.is_empty() || *p == "." || *p == "..")
        {
            return Err(ApiError::Invalid(InvalidId {
                what: "path".into(),
                reason: format!("unsafe relative mail path '{}'", norm.canonical),
            }));
        }
        let parent_segs = &parts[..parts.len() - 1];
        let file_name = parts[parts.len() - 1];
        if file_name
            .chars()
            .any(|c| c == '/' || c == '\\' || c.is_control())
        {
            return Err(ApiError::Invalid(InvalidId {
                what: "path".into(),
                reason: format!("unsafe filename '{file_name}'"),
            }));
        }
        let parent_dir = self.ensure_subdir_segments(parent_segs)?;
        let dest = parent_dir.join(file_name);
        let (confined_abs, _, _) =
            self.guard
                .confine(self.dir.path(), &dest)
                .map_err(|e| ApiError::Denied {
                    principal: "mailbridge".into(),
                    action: format!("confine working-tree path {}: {e}", dest.display()),
                })?;
        rex_wave_fs::write_atomic_if_absent_path(&confined_abs, raw_bytes, SANDBOX_FILE_MODE, true)
            .map_err(|e| {
                ApiError::Internal(format!("export letter {}: {e}", confined_abs.display()))
            })
    }

    /// Exports one ACK receipt to `<mirror>/state/acks.jsonl` idempotently.
    /// Returns `Ok(true)` when a new line was appended, `Ok(false)` when already present.
    pub fn append_ack_if_absent(
        &self,
        seat: &SeatName,
        rel_path: &Path,
        sha256: &Sha256Hex,
        at: UnixMs,
        note: Option<&str>,
    ) -> Result<bool, ApiError> {
        let state_dir = self.ensure_subdir_segments(&["state"])?;
        let acks_file = state_dir.join("acks.jsonl");
        let aliases = AliasMap::empty();

        let (already_present, needs_leading_newline) =
            match rex_wave_fs::read_regular_bounded(&acks_file, MAX_ACKS_FILE_BYTES) {
                Ok(bytes) => {
                    let text = String::from_utf8_lossy(&bytes);
                    let report = parse_acks_jsonl(&text, &aliases);
                    let idx = report.index();
                    let matched = idx.is_acked_by_digest(seat.as_str(), sha256, &aliases);
                    let needs_nl = !bytes.is_empty() && !bytes.ends_with(b"\n");
                    (matched, needs_nl)
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (false, false),
                Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => {
                    return Err(ApiError::Denied {
                        principal: "mailbridge".into(),
                        action: format!("read non-regular acks.jsonl: {e}"),
                    });
                }
                Err(e) => return Err(ApiError::Internal(format!("read acks.jsonl: {e}"))),
            };
        if already_present {
            return Ok(false);
        }

        let path_str = normalize_letter_path(&rel_path.to_string_lossy())
            .map(|n| n.canonical)
            .unwrap_or_else(|| rel_path.to_string_lossy().into_owned());
        let mut obj = serde_json::json!({
            "schema": 1,
            "ts": at.to_rfc3339_sec(),
            "letter": path_str,
            "path": path_str,
            "sha256": sha256.as_str(),
            "sha256_full": sha256.as_str(),
            "acked_by": seat.as_str(),
            "seat": seat.as_str(),
        });
        if let Some(n) = note.map(str::trim).filter(|n| !n.is_empty())
            && let Some(map) = obj.as_object_mut()
        {
            map.insert("note".into(), serde_json::Value::String(n.to_owned()));
        }

        let (mut f, _) =
            rex_wave_fs::open_append_nofollow(&acks_file, SANDBOX_FILE_MODE).map_err(|e| {
                if e.kind() == std::io::ErrorKind::InvalidInput {
                    ApiError::Denied {
                        principal: "mailbridge".into(),
                        action: format!("write through symlink {}", acks_file.display()),
                    }
                } else {
                    ApiError::Internal(format!("open acks.jsonl: {e}"))
                }
            })?;
        let mut line = Vec::new();
        if needs_leading_newline {
            line.push(b'\n');
        }
        serde_json::to_writer(&mut line, &obj).map_err(|e| ApiError::Internal(e.to_string()))?;
        line.push(b'\n');
        f.write_all(&line)
            .map_err(|e| ApiError::Internal(format!("append acks.jsonl: {e}")))?;
        f.sync_all()
            .map_err(|e| ApiError::Internal(format!("fsync acks.jsonl: {e}")))?;
        rex_wave_fs::sync_dir(&state_dir)
            .map_err(|e| ApiError::Internal(format!("fsync state dir: {e}")))?;
        Ok(true)
    }

    /// Reads `<mirror>/state/acks.jsonl` safely without creating it.
    pub fn read_acks(&self, aliases: &AliasMap) -> Result<Vec<AckRecord>, ApiError> {
        let acks_file = self.dir.path().join("state").join("acks.jsonl");
        let bytes = match rex_wave_fs::read_regular_bounded(&acks_file, MAX_ACKS_FILE_BYTES) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(ApiError::Internal(format!("read acks.jsonl: {e}"))),
        };
        let text = String::from_utf8_lossy(&bytes);
        Ok(parse_acks_jsonl(&text, aliases).records)
    }
}

fn verify_or_create_dir_outside(
    guard: &ProtectedRootsGuard,
    path: &Path,
) -> Result<PathBuf, SandboxError> {
    let lex = rex_wave_fs::lexical_normalize(path)?;
    guard.check(&lex, path)?;
    if !rex_wave_fs::entry_exists(&lex) {
        let canon_prefix = rex_wave_fs::canonicalize_existing_prefix(&lex)?;
        guard.check(&canon_prefix, path)?;
        fs::create_dir_all(&lex).map_err(|e| SandboxError::Io(e.to_string()))?;
    }
    guard.verify_dir(&lex)
}

/// One scanned disk file discovered in a mirror working tree, object store, or
/// drop folder.
#[derive(Clone, Debug)]
pub struct DiskMailFile {
    pub abs_path: PathBuf,
    pub rel_path: String,
    pub box_seat: Option<SeatName>,
    pub sha256: Sha256Hex,
    pub raw_bytes: Vec<u8>,
    pub mtime_ms: Option<UnixMs>,
}

fn file_mtime_ms(path: &Path) -> Option<UnixMs> {
    fs::metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| UnixMs(d.as_millis() as u64))
}

/// Scans working-tree `.md` letters under `root` (`mail/`, `to-*/`, and root `*.md`),
/// ignoring dotfiles, `.#*` Emacs lockfiles, files currently locked by `.#<name>`,
/// `#*#` autosaves, `README*`, and symlinks.
pub fn scan_working_tree_dir(
    root: &Path,
    aliases: &AliasMap,
    is_drop_dir: bool,
) -> Result<Vec<DiskMailFile>, ApiError> {
    if rex_wave_fs::entry_kind(root).ok() != Some(EntryKind::Dir) {
        return Ok(Vec::new());
    }
    let top = fs::read_dir(root)
        .map_err(|e| ApiError::Internal(format!("scan {}: {e}", root.display())))?;
    let mut dirs: Vec<(String, PathBuf)> = Vec::new();
    let mut root_files: Vec<(String, PathBuf)> = Vec::new();

    for entry in top.flatten() {
        let fname = entry.file_name();
        let Some(name) = fname.to_str() else {
            continue;
        };
        if name.starts_with('.') || name == "objects" || name == "state" {
            continue;
        }
        let Ok(ft) = entry.file_type() else {
            continue;
        };
        if ft.is_symlink() {
            continue;
        }
        if ft.is_dir() && (name == "mail" || name.starts_with("to-")) {
            dirs.push((name.to_owned(), entry.path()));
        } else if ft.is_file() && !is_ignored_md_filename(name) {
            root_files.push((name.to_owned(), entry.path()));
        }
    }
    dirs.sort_by(|a, b| a.0.cmp(&b.0));
    root_files.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out = Vec::new();
    for (name, path) in root_files {
        if has_emacs_lock(&path) {
            continue;
        }
        let Ok(raw_bytes) = rex_wave_fs::read_regular_bounded(&path, MAX_LETTER_BYTES) else {
            continue;
        };
        let sha256 = sha256_bytes(&raw_bytes);
        let rel_path = if is_drop_dir {
            format!("mail/{name}")
        } else {
            name
        };
        out.push(DiskMailFile {
            mtime_ms: file_mtime_ms(&path),
            abs_path: path,
            rel_path,
            box_seat: None,
            sha256,
            raw_bytes,
        });
    }

    for (dir_name, dir_path) in dirs {
        let box_seat = if dir_name == "mail" || dir_name == "to-desktop" {
            None
        } else {
            dir_name
                .strip_prefix("to-")
                .and_then(|raw| aliases.canonical_seat_name(raw))
        };
        let Ok(entries) = fs::read_dir(&dir_path) else {
            continue;
        };
        let mut files: Vec<(String, PathBuf)> = Vec::new();
        for f in entries.flatten() {
            let fname = f.file_name();
            let Some(name) = fname.to_str() else {
                continue;
            };
            if is_ignored_md_filename(name) {
                continue;
            }
            if f.file_type()
                .is_ok_and(|ft| ft.is_file() && !ft.is_symlink())
            {
                files.push((name.to_owned(), f.path()));
            }
        }
        files.sort_by(|a, b| a.0.cmp(&b.0));
        for (fname, fpath) in files {
            if has_emacs_lock(&fpath) {
                continue;
            }
            let Ok(raw_bytes) = rex_wave_fs::read_regular_bounded(&fpath, MAX_LETTER_BYTES) else {
                continue;
            };
            let sha256 = sha256_bytes(&raw_bytes);
            out.push(DiskMailFile {
                mtime_ms: file_mtime_ms(&fpath),
                abs_path: fpath,
                rel_path: format!("{dir_name}/{fname}"),
                box_seat: box_seat.clone(),
                sha256,
                raw_bytes,
            });
        }
    }

    out.sort_by(|a, b| (&a.rel_path, &a.sha256).cmp(&(&b.rel_path, &b.sha256)));
    Ok(out)
}

/// Scans `<mirror>/objects/<sha256>.md` verifying exact content-hash integrity.
pub fn scan_object_blobs(
    mirror_root: &Path,
) -> Result<Vec<(Sha256Hex, Vec<u8>, Option<UnixMs>)>, ApiError> {
    let objects_dir = mirror_root.join("objects");
    if rex_wave_fs::entry_kind(&objects_dir).ok() != Some(EntryKind::Dir) {
        return Ok(Vec::new());
    }
    let entries = fs::read_dir(&objects_dir)
        .map_err(|e| ApiError::Internal(format!("scan {}: {e}", objects_dir.display())))?;
    let mut files: Vec<(Sha256Hex, PathBuf)> = Vec::new();
    for entry in entries.flatten() {
        let fname = entry.file_name();
        let Some(name) = fname.to_str() else {
            continue;
        };
        if name.starts_with('.') {
            continue;
        }
        let Some(stem) = name.strip_suffix(".md") else {
            continue;
        };
        let Ok(expected_sha) = Sha256Hex::parse(stem) else {
            continue;
        };
        if entry
            .file_type()
            .is_ok_and(|ft| ft.is_file() && !ft.is_symlink())
        {
            files.push((expected_sha, entry.path()));
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out = Vec::with_capacity(files.len());
    for (expected_sha, path) in files {
        let bytes = rex_wave_fs::read_regular_bounded(&path, MAX_LETTER_BYTES)
            .map_err(|e| ApiError::Internal(format!("read blob {}: {e}", path.display())))?;
        let actual_sha = sha256_bytes(&bytes);
        if actual_sha != expected_sha {
            return Err(ApiError::conflict(format!(
                "corrupt object blob {}: expected sha256 {}, got {}",
                path.display(),
                expected_sha,
                actual_sha
            )));
        }
        out.push((actual_sha, bytes, file_mtime_ms(&path)));
    }
    Ok(out)
}

/// Projects a dissolved [`MailMessage`] into its target room via a live `RoomCommand` service.
pub async fn project_message_to_room_service(
    room_svc: &dyn Service<RoomCommand>,
    msg: &MailMessage,
    parent_seq: Option<Seq>,
    caller_ctx: Option<&DecideCtx>,
) -> Option<Seq> {
    let room = msg.room.clone()?;
    let idem = IdemKey::parse(format!("mail:{}", msg.sha256)).ok()?;
    let post = RoomPost {
        room: RoomRef::Id { id: room.clone() },
        body: format_mail_room_body(msg),
        re: parent_seq,
        after: None,
        labels: mail_room_labels(msg),
        task: None,
        idem: Some(idem),
        origin: PostOrigin::Direct,
    };
    let ctx = match caller_ctx {
        Some(c) => match &room {
            RoomId::Direct { pair } if pair.contains(c.by()) || c.by().is_operator_or_system() => {
                c.clone()
            }
            RoomId::Channel { .. } => c.clone(),
            _ => DecideCtx::for_system_part(SystemPart::MailBridge),
        },
        None => DecideCtx::for_system_part(SystemPart::MailBridge),
    };
    if let Ok(WireReply::RoomPost(posted)) =
        room_svc.dispatch(RoomCommand::RoomPost(post), ctx).await
    {
        // For multi-seat mail channels, also join all recipients so everyone sees membership.
        if matches!(room, RoomId::Channel { .. }) {
            let sys_ctx = DecideCtx::for_system_part(SystemPart::MailBridge);
            for seat in msg.to.iter().chain(msg.cc.iter()) {
                let _ = room_svc
                    .dispatch(
                        RoomCommand::RoomJoin(rex_wave_api::room::RoomJoin {
                            room: RoomRef::Id { id: room.clone() },
                            member: Some(Principal::Seat { seat: seat.clone() }),
                            handle: None,
                        }),
                        sys_ctx.clone(),
                    )
                    .await;
            }
        }
        return Some(posted.seq);
    }
    None
}

/// Projects a batch of dissolved [`MailMessage`]s directly into `StreamId::Room`
/// streams on an offline `Journal` (used by `rebuild` and `migrate-verify`).
pub fn project_messages_to_journal_rooms(
    journal: &Journal,
    messages: &[MailMessage],
) -> Result<(usize, usize), ApiError> {
    let mut by_room: BTreeMap<RoomId, Vec<&MailMessage>> = BTreeMap::new();
    for msg in messages {
        if let Some(ref room) = msg.room {
            by_room.entry(room.clone()).or_default().push(msg);
        }
    }

    let mut dm_rooms = 0usize;
    let mut thread_channels = 0usize;

    for (room, mut room_msgs) in by_room {
        room_msgs.sort_by(|a, b| (a.created_at, &a.sha256).cmp(&(b.created_at, &b.sha256)));
        match &room {
            RoomId::Direct { .. } => dm_rooms += 1,
            RoomId::Channel { .. } => thread_channels += 1,
        }

        let stream = StreamId::Room { room: room.clone() };
        let mut writer = journal
            .claim(stream.clone())
            .map_err(|e| ApiError::Internal(format!("claim room stream: {e}")))?;

        let mut opened = writer.head() > Seq::ORIGIN;
        let mut joined: BTreeSet<Principal> = BTreeSet::new();
        let mut used_handles: BTreeSet<String> = BTreeSet::new();
        let mut seq_by_sha: BTreeMap<Sha256Hex, Seq> = BTreeMap::new();
        let mut seq_by_id: BTreeMap<String, Seq> = BTreeMap::new();

        for msg in room_msgs {
            let idem = IdemKey::parse(format!("mail:{}", msg.sha256))?;
            if let Some(existing) = writer
                .lookup(&idem)
                .map_err(|e| ApiError::Internal(e.to_string()))?
            {
                if let Some(post_env) = existing
                    .iter()
                    .find(|e| matches!(e.event, Event::Room(RoomEvent::Posted { .. })))
                {
                    seq_by_sha.insert(msg.sha256.clone(), post_env.seq);
                    if !msg.id.is_empty() {
                        seq_by_id.insert(msg.id.clone(), post_env.seq);
                    }
                }
                continue;
            }

            let sender = SeatName::parse(normalize_seat_token(&msg.from))
                .map(|seat| Principal::Seat { seat })
                .unwrap_or(Principal::System {
                    part: SystemPart::MailBridge,
                });
            let mut events: Vec<Event> = Vec::new();
            if !opened {
                events.push(Event::Room(RoomEvent::Opened {
                    id: room.clone(),
                    origin: RoomOrigin::Manual,
                    topic: msg.topic.clone(),
                }));
                opened = true;
                if let RoomId::Direct { pair } = &room {
                    let (a, b): (Principal, Principal) = pair.clone().into();
                    for member in [a, b] {
                        if joined.insert(member.clone()) {
                            let handle = offline_handle(&member, &mut used_handles);
                            events.push(Event::Room(RoomEvent::MemberJoined { member, handle }));
                        }
                    }
                }
            }
            if matches!(room, RoomId::Channel { .. }) {
                let mut participants: Vec<Principal> = Vec::new();
                if !matches!(sender, Principal::System { .. }) {
                    participants.push(sender.clone());
                }
                for seat in msg.to.iter().chain(msg.cc.iter()) {
                    participants.push(Principal::Seat { seat: seat.clone() });
                }
                for member in participants {
                    if joined.insert(member.clone()) {
                        let handle = offline_handle(&member, &mut used_handles);
                        events.push(Event::Room(RoomEvent::MemberJoined { member, handle }));
                    }
                }
            }

            let re = msg.in_reply_to.as_deref().and_then(|parent_ref| {
                let trimmed = parent_ref.trim();
                Sha256Hex::parse(trimmed.to_ascii_lowercase())
                    .ok()
                    .and_then(|sha| seq_by_sha.get(&sha).copied())
                    .or_else(|| seq_by_id.get(trimmed).copied())
            });

            events.push(Event::Room(RoomEvent::Posted {
                body: format_mail_room_body(msg),
                mentions: Mentions::default(),
                re,
                after: None,
                labels: mail_room_labels(msg),
                task: None,
                idem: Some(idem.clone()),
                origin: PostOrigin::Direct,
            }));

            let appended = writer
                .append(&sender, msg.created_at, Some(&idem), events)
                .map_err(|e| ApiError::Internal(e.to_string()))?;
            if let Some(post_env) = appended
                .envelopes()
                .iter()
                .find(|e| matches!(e.event, Event::Room(RoomEvent::Posted { .. })))
            {
                seq_by_sha.insert(msg.sha256.clone(), post_env.seq);
                if !msg.id.is_empty() {
                    seq_by_id.insert(msg.id.clone(), post_env.seq);
                }
            }
        }
    }

    Ok((dm_rooms, thread_channels))
}

fn offline_handle(member: &Principal, used: &mut BTreeSet<String>) -> Handle {
    let base = match member {
        Principal::Operator { seat } | Principal::Seat { seat } => seat.as_str().to_owned(),
        Principal::Worker { role, .. } => role.as_str().to_owned(),
        Principal::System { .. } => "system".to_owned(),
    };
    if used.insert(base.clone())
        && let Ok(h) = Handle::parse(base.clone())
    {
        return h;
    }
    let short = &base[..base.len().min(24)];
    for n in 2u32.. {
        let cand = format!("{short}_{n}");
        if used.insert(cand.clone())
            && let Ok(h) = Handle::parse(cand)
        {
            return h;
        }
    }
    unreachable!("u32 handle suffix space exhausted")
}

/// Runs a complete `MailStore.tla` `Reconciled` pass between `journal`/`state`
/// and `mirror` (+ optional `drop_dir`), exporting missing blobs, working-tree
/// files, and acks (when `export_enabled` is true), and ingesting any unindexed
/// files or acks from disk.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_mail_store(
    state: &mut MailState,
    writer: &mut StreamWriter,
    mirror: &MirrorTarget,
    aliases: &AliasMap,
    room_svc: Option<&dyn Service<RoomCommand>>,
    export_enabled: bool,
    now: UnixMs,
) -> Result<(MailReconcileReport, Vec<Envelope>), ApiError> {
    let mut report = MailReconcileReport::default();
    let mut fresh_envelopes = Vec::new();
    let sys_by = Principal::System {
        part: SystemPart::MailBridge,
    };

    // 1. Ingest mirror state/acks.jsonl -> SQLite (union-safe across machines).
    let disk_acks = mirror.read_acks(aliases)?;
    let disk_blobs = scan_object_blobs(mirror.path())?;
    let disk_working = scan_working_tree_dir(mirror.path(), aliases, false)?;
    let drop_files = match mirror.drop_dir() {
        Some(drop_path) => scan_working_tree_dir(drop_path, aliases, true)?,
        None => Vec::new(),
    };

    // Map sha16 -> unique full Sha256Hex across known DB + disk blobs + working tree + drop.
    let mut sha16_index: BTreeMap<String, Option<Sha256Hex>> = BTreeMap::new();
    let mut register_sha = |sha: &Sha256Hex| {
        let short = sha.as_str()[..16].to_owned();
        sha16_index
            .entry(short)
            .and_modify(|slot| {
                if slot.as_ref() != Some(sha) {
                    *slot = None;
                }
            })
            .or_insert_with(|| Some(sha.clone()));
    };
    for sha in state.messages.keys() {
        register_sha(sha);
    }
    for (sha, _, _) in &disk_blobs {
        register_sha(sha);
    }
    for f in disk_working.iter().chain(drop_files.iter()) {
        register_sha(&f.sha256);
    }

    let mut path_hint_by_sha: BTreeMap<Sha256Hex, String> = BTreeMap::new();
    for rec in &disk_acks {
        let Some(ref raw_seat) = rec.seat else {
            continue;
        };
        let Ok(seat) = SeatName::parse(raw_seat) else {
            continue;
        };
        let resolved_sha = rec.sha256_full.clone().or_else(|| {
            rec.sha16
                .as_deref()
                .and_then(|s| sha16_index.get(s).and_then(Clone::clone))
        });
        let Some(sha) = resolved_sha else {
            continue;
        };
        let rel_path = rec
            .path
            .as_ref()
            .map(|p| p.canonical.clone())
            .or_else(|| {
                state
                    .messages
                    .get(&sha)
                    .map(|m| m.message.path.to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| format!("objects/{}.md", sha.as_str()));
        path_hint_by_sha
            .entry(sha.clone())
            .or_insert_with(|| rel_path.clone());
        if state.acked.contains(&(seat.clone(), sha.clone())) {
            continue;
        }
        let ack_at = rec
            .ts
            .as_deref()
            .and_then(crate::shadow::parse_recorded_datetime_ms)
            .unwrap_or(now);
        let idem_hash = sha256_short16(&format!("ack/2\n{seat}\n{sha}"));
        let idem = IdemKey::parse(format!("ack:{idem_hash}"))?;
        let appended = writer
            .append(
                &sys_by,
                ack_at,
                Some(&idem),
                vec![Event::Mail(MailEvent::Acked {
                    path: PathBuf::from(&rel_path),
                    sha256: sha,
                    seat: Some(seat),
                    note: None,
                })],
            )
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        if let Appended::Fresh(envs) = appended {
            report.acks_ingested += 1;
            for e in envs {
                state.apply(&e);
                fresh_envelopes.push(e);
            }
        }
    }

    // 2. Ingest unindexed working-tree files from <mirror>.
    for f in &disk_working {
        if state.messages.contains_key(&f.sha256) {
            continue;
        }
        let prior_ack_sha = state.is_edit_after_ack(&f.rel_path, &f.sha256);
        let mutated_after_ack = prior_ack_sha.is_some();
        let msg = parse_mail_message_from_bytes(
            &f.rel_path,
            &f.raw_bytes,
            f.box_seat.as_ref(),
            &[],
            f.mtime_ms,
            aliases,
            MailOrigin::Mirror,
        );
        let idem = IdemKey::parse(format!("mail_msg:{}", msg.sha256))?;
        let appended = writer
            .append(
                &sys_by,
                msg.created_at,
                Some(&idem),
                vec![Event::Mail(MailEvent::MessageCommitted {
                    message: Box::new(msg.clone()),
                    mutated_after_ack,
                    prior_ack_sha,
                })],
            )
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        if let Appended::Fresh(envs) = appended {
            report.files_ingested += 1;
            if mutated_after_ack {
                report.mutated_after_ack_flagged += 1;
            }
            for e in envs {
                state.apply(&e);
                fresh_envelopes.push(e);
            }
            if !mutated_after_ack
                && let Some(rsvc) = room_svc
                && let Some(ref room) = msg.room
            {
                let parent_seq = msg
                    .in_reply_to
                    .as_deref()
                    .and_then(|p| state.resolve_parent_room_seq(room, p));
                if let Some(seq) =
                    project_message_to_room_service(rsvc, &msg, parent_seq, None).await
                {
                    state
                        .room_post_seqs
                        .insert((room.clone(), msg.sha256.clone()), seq);
                }
            }
        }
    }

    // 3. Ingest unindexed immutable blobs from <mirror>/objects/<sha256>.md.
    for (sha, raw_bytes, mtime_ms) in &disk_blobs {
        if state.messages.contains_key(sha) {
            continue;
        }
        let hinted_path = path_hint_by_sha.get(sha).cloned().unwrap_or_else(|| {
            let text = String::from_utf8_lossy(raw_bytes);
            let doc = FrontmatterDoc::parse(&text);
            if let Some(id_val) = doc.get("id")
                && !id_val.is_empty()
                && !id_val.contains('/')
                && !id_val.contains('\\')
                && !id_val.contains("..")
            {
                let file = if id_val.ends_with(".md") {
                    id_val.to_owned()
                } else {
                    format!("{id_val}.md")
                };
                return format!("mail/{file}");
            }
            format!("objects/{}.md", sha.as_str())
        });
        let prior_ack_sha = state.is_edit_after_ack(&hinted_path, sha);
        let mutated_after_ack = prior_ack_sha.is_some();
        let msg = parse_mail_message_from_bytes(
            &hinted_path,
            raw_bytes,
            None,
            &[],
            *mtime_ms,
            aliases,
            MailOrigin::Mirror,
        );
        let idem = IdemKey::parse(format!("mail_msg:{}", msg.sha256))?;
        let appended = writer
            .append(
                &sys_by,
                msg.created_at,
                Some(&idem),
                vec![Event::Mail(MailEvent::MessageCommitted {
                    message: Box::new(msg.clone()),
                    mutated_after_ack,
                    prior_ack_sha,
                })],
            )
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        if let Appended::Fresh(envs) = appended {
            report.files_ingested += 1;
            if mutated_after_ack {
                report.mutated_after_ack_flagged += 1;
            }
            for e in envs {
                state.apply(&e);
                fresh_envelopes.push(e);
            }
            if !mutated_after_ack
                && let Some(rsvc) = room_svc
                && let Some(ref room) = msg.room
            {
                let parent_seq = msg
                    .in_reply_to
                    .as_deref()
                    .and_then(|p| state.resolve_parent_room_seq(room, p));
                if let Some(seq) =
                    project_message_to_room_service(rsvc, &msg, parent_seq, None).await
                {
                    state
                        .room_post_seqs
                        .insert((room.clone(), msg.sha256.clone()), seq);
                }
            }
        }
    }

    // 4. Ingest files from separate <drop_dir> -> commit to SQLite FIRST, export to mirror, then consume drop file.
    for f in &drop_files {
        let already_in_db = state.messages.contains_key(&f.sha256);
        let mut mutated_after_ack = false;
        let msg = if let Some(existing) = state.messages.get(&f.sha256) {
            existing.message.clone()
        } else {
            let prior_ack_sha = state.is_edit_after_ack(&f.rel_path, &f.sha256);
            mutated_after_ack = prior_ack_sha.is_some();
            let parsed = parse_mail_message_from_bytes(
                &f.rel_path,
                &f.raw_bytes,
                f.box_seat.as_ref(),
                &[],
                f.mtime_ms,
                aliases,
                MailOrigin::Drop,
            );
            let idem = IdemKey::parse(format!("mail_msg:{}", parsed.sha256))?;
            let appended = writer
                .append(
                    &sys_by,
                    parsed.created_at,
                    Some(&idem),
                    vec![Event::Mail(MailEvent::MessageCommitted {
                        message: Box::new(parsed.clone()),
                        mutated_after_ack,
                        prior_ack_sha,
                    })],
                )
                .map_err(|e| ApiError::Internal(e.to_string()))?;
            if let Appended::Fresh(envs) = appended {
                report.files_ingested += 1;
                if mutated_after_ack {
                    report.mutated_after_ack_flagged += 1;
                }
                for e in envs {
                    state.apply(&e);
                    fresh_envelopes.push(e);
                }
            }
            parsed
        };

        if !already_in_db
            && !mutated_after_ack
            && let Some(rsvc) = room_svc
            && let Some(ref room) = msg.room
        {
            let parent_seq = msg
                .in_reply_to
                .as_deref()
                .and_then(|p| state.resolve_parent_room_seq(room, p));
            if let Some(seq) = project_message_to_room_service(rsvc, &msg, parent_seq, None).await {
                state
                    .room_post_seqs
                    .insert((room.clone(), msg.sha256.clone()), seq);
            }
        }

        // Export to mirror BEFORE deleting consumed drop-folder file.
        if export_enabled {
            if mirror.export_blob_if_absent(&msg.sha256, &f.raw_bytes)? {
                report.blobs_exported += 1;
            }
            if mirror.export_working_tree_if_absent(&msg.path, &f.raw_bytes)? {
                report.working_tree_exported += 1;
            }
            if fs::remove_file(&f.abs_path).is_ok() {
                report.drop_files_consumed += 1;
            }
        }
    }

    if export_enabled {
        // 5. Export any SQLite messages missing from <mirror>/objects/<sha>.md or <mirror>/<rel_path>.
        let messages_snapshot: Vec<MailMessage> =
            state.messages.values().map(|e| e.message.clone()).collect();
        for msg in &messages_snapshot {
            let raw = message_raw_bytes(msg)?;
            if mirror.export_blob_if_absent(&msg.sha256, &raw)? {
                report.blobs_exported += 1;
            }
            if !msg.path.starts_with("objects")
                && mirror.export_working_tree_if_absent(&msg.path, &raw)?
            {
                report.working_tree_exported += 1;
            }
        }

        // 6. Export any SQLite ACKs missing from <mirror>/state/acks.jsonl.
        let acks_snapshot: Vec<CommittedAckRecord> = state.ack_records.values().cloned().collect();
        for ack in &acks_snapshot {
            if mirror.append_ack_if_absent(
                &ack.seat,
                &ack.path,
                &ack.sha256,
                ack.at,
                ack.note.as_deref(),
            )? {
                report.acks_exported += 1;
            }
        }
    }

    Ok((report, fresh_envelopes))
}

/// Report returned by [`rebuild_mail_from_mirror`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailRebuildReport {
    pub mirror_dir: PathBuf,
    pub journal_path: PathBuf,
    pub messages_rebuilt: usize,
    pub acks_rebuilt: usize,
    pub mutated_after_ack_flagged: usize,
    pub dm_rooms_projected: usize,
    pub thread_channels_projected: usize,
    pub fts_indexed: usize,
}

fn verify_journal_parent_outside_protected(
    journal_path: &Path,
    protection: &Protection,
) -> Result<PathBuf, SandboxError> {
    if journal_path
        .components()
        .any(|c| matches!(c, Component::ParentDir))
    {
        return Err(SandboxError::Io(format!(
            "journal path {} must not contain '..'",
            journal_path.display()
        )));
    }
    let guard = ProtectedRootsGuard::resolve(protection)?;
    let lex = rex_wave_fs::lexical_normalize(journal_path)?;
    guard.check(&lex, journal_path)?;
    let canon_prefix = rex_wave_fs::canonicalize_existing_prefix(&lex)?;
    guard.check(&canon_prefix, journal_path)?;
    if let Some(parent) = lex.parent() {
        verify_or_create_dir_outside(&guard, parent)?;
    }
    Ok(lex)
}

/// Rebuilds a complete `journal.sqlite3` (including `StreamId::Mail`, SQLite FTS5,
/// `mutated_after_ack` flags, and `RoomId::Direct`/multi-seat channel projections)
/// from `<mirror>` (`objects/*.md` + working tree + `state/acks.jsonl`) alone.
pub fn rebuild_mail_from_mirror(
    mirror_dir: &Path,
    journal_path: &Path,
    aliases: &AliasMap,
    protection: &Protection,
) -> Result<MailRebuildReport, SandboxError> {
    let ro = ReadOnlyInterchange::open(mirror_dir)?;
    let verified_journal = verify_journal_parent_outside_protected(journal_path, protection)?;

    let acks_report = ro.read_acks(aliases)?;
    let working = scan_working_tree_dir(ro.root(), aliases, false)
        .map_err(|e| SandboxError::Io(e.to_string()))?;
    let blobs = scan_object_blobs(ro.root()).map_err(|e| SandboxError::Io(e.to_string()))?;

    // Collect all unique sha256 content blobs from working tree + objects/.
    struct RebuiltCandidate {
        rel_path: String,
        box_seat: Option<SeatName>,
        extra_recipients: Vec<SeatName>,
        raw_bytes: Vec<u8>,
        mtime_ms: Option<UnixMs>,
    }

    let mut by_sha: BTreeMap<Sha256Hex, RebuiltCandidate> = BTreeMap::new();
    for f in working {
        by_sha
            .entry(f.sha256.clone())
            .and_modify(|existing| {
                let prefer_new =
                    f.rel_path.starts_with("mail/") && !existing.rel_path.starts_with("mail/");
                if prefer_new {
                    if let Some(prev_seat) = existing.box_seat.take()
                        && !existing.extra_recipients.contains(&prev_seat)
                    {
                        existing.extra_recipients.push(prev_seat);
                    }
                    existing.rel_path = f.rel_path.clone();
                    existing.box_seat = f.box_seat.clone();
                }
                if let Some(ref seat) = f.box_seat
                    && !existing.extra_recipients.contains(seat)
                {
                    existing.extra_recipients.push(seat.clone());
                }
            })
            .or_insert_with(|| RebuiltCandidate {
                rel_path: f.rel_path,
                box_seat: f.box_seat,
                extra_recipients: Vec::new(),
                raw_bytes: f.raw_bytes,
                mtime_ms: f.mtime_ms,
            });
    }

    // Build sha16 lookup for resolving 16-hex ack records against discovered messages.
    let mut sha16_map: BTreeMap<String, Option<Sha256Hex>> = BTreeMap::new();
    for sha in by_sha.keys().chain(blobs.iter().map(|(s, _, _)| s)) {
        let short = sha.as_str()[..16].to_owned();
        sha16_map
            .entry(short)
            .and_modify(|slot| {
                if slot.as_ref() != Some(sha) {
                    *slot = None;
                }
            })
            .or_insert_with(|| Some(sha.clone()));
    }

    // Resolve all ack records from state/acks.jsonl into canonical (seat, sha256, path, ts).
    let mut resolved_acks: BTreeMap<(SeatName, Sha256Hex), (String, UnixMs)> = BTreeMap::new();
    let mut ack_path_by_sha: BTreeMap<Sha256Hex, String> = BTreeMap::new();
    for rec in &acks_report.records {
        let Some(ref raw_seat) = rec.seat else {
            continue;
        };
        let Ok(seat) = SeatName::parse(raw_seat) else {
            continue;
        };
        let resolved_sha = rec.sha256_full.clone().or_else(|| {
            rec.sha16
                .as_deref()
                .and_then(|s| sha16_map.get(s).and_then(Clone::clone))
        });
        let Some(sha) = resolved_sha else {
            continue;
        };
        let rel = rec
            .path
            .as_ref()
            .map(|p| p.canonical.clone())
            .or_else(|| by_sha.get(&sha).map(|c| c.rel_path.clone()))
            .unwrap_or_else(|| format!("objects/{}.md", sha.as_str()));
        ack_path_by_sha
            .entry(sha.clone())
            .or_insert_with(|| rel.clone());
        let ack_at = rec
            .ts
            .as_deref()
            .and_then(crate::shadow::parse_recorded_datetime_ms)
            .unwrap_or(UnixMs(0));
        resolved_acks.entry((seat, sha)).or_insert((rel, ack_at));
    }

    // Merge blobs that are no longer at their working-tree path (e.g. pre-edit historical blobs).
    for (sha, raw_bytes, mtime_ms) in blobs {
        by_sha.entry(sha.clone()).or_insert_with(|| {
            let rel_path = ack_path_by_sha.get(&sha).cloned().unwrap_or_else(|| {
                let text = String::from_utf8_lossy(&raw_bytes);
                let doc = FrontmatterDoc::parse(&text);
                if let Some(id_val) = doc.get("id")
                    && !id_val.is_empty()
                    && !id_val.contains('/')
                    && !id_val.contains('\\')
                    && !id_val.contains("..")
                {
                    let file = if id_val.ends_with(".md") {
                        id_val.to_owned()
                    } else {
                        format!("{id_val}.md")
                    };
                    return format!("mail/{file}");
                }
                format!("objects/{}.md", sha.as_str())
            });
            RebuiltCandidate {
                rel_path,
                box_seat: None,
                extra_recipients: Vec::new(),
                raw_bytes,
                mtime_ms,
            }
        });
    }

    // Build canonical AckIndex from the exact resolved ack set so `mutated_after_ack`
    // (`IsEdit(m, ledger)`) is 100% deterministic between live import and rebuild.
    let canonical_ack_records: Vec<AckRecord> = resolved_acks
        .iter()
        .enumerate()
        .map(|(i, ((seat, sha), (rel, _)))| AckRecord {
            line_number: i + 1,
            seat: Some(seat.as_str().to_owned()),
            raw_seat: Some(seat.as_str().to_owned()),
            path: normalize_letter_path(rel),
            sha256_full: Some(sha.clone()),
            sha16: Some(sha.as_str()[..16].to_owned()),
            ts: None,
        })
        .collect();
    let canonical_ack_index = AckIndex::from_records(canonical_ack_records);
    let acked_shas: BTreeSet<Sha256Hex> =
        resolved_acks.keys().map(|(_, sha)| sha.clone()).collect();

    let mut parsed_messages: Vec<(MailMessage, bool, Option<String>)> =
        Vec::with_capacity(by_sha.len());
    for cand in by_sha.into_values() {
        let msg = parse_mail_message_from_bytes(
            &cand.rel_path,
            &cand.raw_bytes,
            cand.box_seat.as_ref(),
            &cand.extra_recipients,
            cand.mtime_ms,
            aliases,
            MailOrigin::Mirror,
        );
        let prior_ack_sha = if acked_shas.contains(&msg.sha256) {
            None
        } else {
            canonical_ack_index.mutated_after_ack("", &cand.rel_path, &msg.sha256, aliases)
        };
        let mutated_after_ack = prior_ack_sha.is_some();
        parsed_messages.push((msg, mutated_after_ack, prior_ack_sha));
    }
    parsed_messages
        .sort_by(|a, b| (a.0.created_at, &a.0.sha256).cmp(&(b.0.created_at, &b.0.sha256)));

    let journal = Journal::open(&verified_journal).map_err(|e| SandboxError::Io(e.to_string()))?;
    let mut mail_writer = journal
        .claim(StreamId::Mail)
        .map_err(|e| SandboxError::Io(e.to_string()))?;
    let sys_by = Principal::System {
        part: SystemPart::MailBridge,
    };

    let mut messages_rebuilt = 0usize;
    let mut mutated_after_ack_flagged = 0usize;
    let mut non_flagged_for_rooms = Vec::new();
    for (msg, mutated_after_ack, prior_ack_sha) in &parsed_messages {
        let idem = IdemKey::parse(format!("mail_msg:{}", msg.sha256))
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        let appended = mail_writer
            .append(
                &sys_by,
                msg.created_at,
                Some(&idem),
                vec![Event::Mail(MailEvent::MessageCommitted {
                    message: Box::new(msg.clone()),
                    mutated_after_ack: *mutated_after_ack,
                    prior_ack_sha: prior_ack_sha.clone(),
                })],
            )
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        if matches!(appended, Appended::Fresh(_)) {
            messages_rebuilt += 1;
            if *mutated_after_ack {
                mutated_after_ack_flagged += 1;
            }
        }
        if !mutated_after_ack {
            non_flagged_for_rooms.push(msg.clone());
        }
    }

    let mut acks_rebuilt = 0usize;
    for ((seat, sha), (rel, ack_at)) in &resolved_acks {
        let idem_hash = sha256_short16(&format!("ack/2\n{seat}\n{sha}"));
        let idem = IdemKey::parse(format!("ack:{idem_hash}"))
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        let appended = mail_writer
            .append(
                &sys_by,
                *ack_at,
                Some(&idem),
                vec![Event::Mail(MailEvent::Acked {
                    path: PathBuf::from(rel),
                    sha256: sha.clone(),
                    seat: Some(seat.clone()),
                    note: None,
                })],
            )
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        if matches!(appended, Appended::Fresh(_)) {
            acks_rebuilt += 1;
        }
    }
    drop(mail_writer);

    let (dm_rooms_projected, thread_channels_projected) =
        project_messages_to_journal_rooms(&journal, &non_flagged_for_rooms)
            .map_err(|e| SandboxError::Io(e.to_string()))?;
    let fts_backfilled = journal
        .backfill_mail_fts()
        .map_err(|e| SandboxError::Io(e.to_string()))?;

    Ok(MailRebuildReport {
        mirror_dir: ro.root().to_path_buf(),
        journal_path: verified_journal,
        messages_rebuilt,
        acks_rebuilt,
        mutated_after_ack_flagged,
        dm_rooms_projected,
        thread_channels_projected,
        fts_indexed: messages_rebuilt.max(fts_backfilled),
    })
}

/// Full parity verification report returned by [`migrate_verify_interchange`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailMigrateVerifyReport {
    pub interchange_dir: PathBuf,
    pub work_dir: PathBuf,
    pub source_files_scanned: usize,
    pub unique_sha256_count: usize,
    pub duplicate_content_copies: usize,
    pub source_ack_lines_parsed: usize,
    pub source_ack_lines_skipped: usize,
    pub imported_messages: usize,
    pub imported_acks: usize,
    pub imported_mutated_after_ack: usize,
    pub imported_dm_rooms: usize,
    pub imported_thread_channels: usize,
    pub rebuilt_messages: usize,
    pub rebuilt_acks: usize,
    pub rebuilt_mutated_after_ack: usize,
    pub rebuilt_dm_rooms: usize,
    pub rebuilt_thread_channels: usize,
    pub blob_parity_count: usize,
    pub working_tree_parity_count: usize,
    pub fts_sample_query_hits: usize,
    pub db_parity: bool,
    pub ack_parity: bool,
    pub flagged_parity: bool,
    pub dm_room_parity: bool,
    pub parity_ok: bool,
}

/// Runs the strictly read-only migration + delete-journal + lossless-rebuild
/// verification gate over `interchange_dir`, writing ONLY inside `work_dir`
/// (verified outside `protected-roots` AND outside `interchange_dir`).
pub fn migrate_verify_interchange(
    interchange_dir: &Path,
    work_dir: &Path,
    aliases: &AliasMap,
    protection: &Protection,
) -> Result<MailMigrateVerifyReport, SandboxError> {
    let ro = ReadOnlyInterchange::open(interchange_dir)?;

    // Strictly verify work_dir is outside BOTH protected-roots AND interchange_dir.
    let guard = ProtectedRootsGuard::resolve(protection)?;
    let verified_work = verify_or_create_dir_outside(&guard, work_dir)?;
    OutsidePath::verify(&verified_work.join("probe"), ro.root())
        .map_err(|e| SandboxError::Production(PathBuf::from(e.to_string())))?;

    let mirror_dir = verified_work.join("mirror");
    let journal1_path = verified_work.join("journal.sqlite3");
    let journal2_path = verified_work.join("journal-rebuilt.sqlite3");

    let mirror = MirrorTarget::open_verified(&mirror_dir, None, protection)?;
    rex_wave_fs::write_atomic_path(
        &mirror.path().join(crate::MARKER_FILE),
        b"rex-wave migrate-verify sandbox\n",
        SANDBOX_FILE_MODE,
        false,
    )
    .map_err(|e| SandboxError::Io(e.to_string()))?;

    // 1. Read source interchange strictly read-only.
    let acks_report = ro.read_acks(aliases)?;
    let scanned_raw = ro.scan_letters_raw(aliases)?;
    let source_files_scanned = scanned_raw.len();

    struct SourceEntry {
        rel_path: String,
        box_seat: Option<SeatName>,
        extra_recipients: Vec<SeatName>,
        raw_bytes: Vec<u8>,
        seen_at: UnixMs,
    }
    let mut by_sha: BTreeMap<Sha256Hex, SourceEntry> = BTreeMap::new();
    for (letter, raw_bytes) in scanned_raw {
        let box_seat = letter.rel_path.split_once('/').and_then(|(first, _)| {
            if first == "mail" || first == "to-desktop" {
                None
            } else {
                first
                    .strip_prefix("to-")
                    .and_then(|raw| aliases.canonical_seat_name(raw))
            }
        });
        by_sha
            .entry(letter.sha256.clone())
            .and_modify(|existing| {
                let prefer_new =
                    letter.rel_path.starts_with("mail/") && !existing.rel_path.starts_with("mail/");
                if prefer_new {
                    existing.rel_path = letter.rel_path.clone();
                    existing.box_seat = box_seat.clone();
                }
                for r in &letter.recipients {
                    if !existing.extra_recipients.contains(r) {
                        existing.extra_recipients.push(r.clone());
                    }
                }
            })
            .or_insert_with(|| SourceEntry {
                rel_path: letter.rel_path,
                box_seat,
                extra_recipients: letter.recipients,
                raw_bytes,
                seen_at: letter.seen_at,
            });
    }
    let unique_sha256_count = by_sha.len();
    let duplicate_content_copies = source_files_scanned.saturating_sub(unique_sha256_count);

    let mut sha16_map: BTreeMap<String, Option<Sha256Hex>> = BTreeMap::new();
    for sha in by_sha.keys() {
        let short = sha.as_str()[..16].to_owned();
        sha16_map
            .entry(short)
            .and_modify(|slot| {
                if slot.as_ref() != Some(sha) {
                    *slot = None;
                }
            })
            .or_insert_with(|| Some(sha.clone()));
    }

    let mut resolved_acks: BTreeMap<(SeatName, Sha256Hex), (String, UnixMs)> = BTreeMap::new();
    for rec in &acks_report.records {
        let Some(ref raw_seat) = rec.seat else {
            continue;
        };
        let Ok(seat) = SeatName::parse(raw_seat) else {
            continue;
        };
        let resolved_sha = rec.sha256_full.clone().or_else(|| {
            rec.sha16
                .as_deref()
                .and_then(|s| sha16_map.get(s).and_then(Clone::clone))
        });
        let Some(sha) = resolved_sha else {
            continue;
        };
        let rel = rec
            .path
            .as_ref()
            .map(|p| p.canonical.clone())
            .or_else(|| by_sha.get(&sha).map(|c| c.rel_path.clone()))
            .unwrap_or_else(|| format!("objects/{}.md", sha.as_str()));
        let ack_at = rec
            .ts
            .as_deref()
            .and_then(crate::shadow::parse_recorded_datetime_ms)
            .unwrap_or(UnixMs(0));
        resolved_acks.entry((seat, sha)).or_insert((rel, ack_at));
    }

    let canonical_ack_records: Vec<AckRecord> = resolved_acks
        .iter()
        .enumerate()
        .map(|(i, ((seat, sha), (rel, _)))| AckRecord {
            line_number: i + 1,
            seat: Some(seat.as_str().to_owned()),
            raw_seat: Some(seat.as_str().to_owned()),
            path: normalize_letter_path(rel),
            sha256_full: Some(sha.clone()),
            sha16: Some(sha.as_str()[..16].to_owned()),
            ts: None,
        })
        .collect();
    let canonical_ack_index = AckIndex::from_records(canonical_ack_records);
    let acked_shas: BTreeSet<Sha256Hex> =
        resolved_acks.keys().map(|(_, sha)| sha.clone()).collect();

    // 2. Import into temporary journal1 + export to temporary mirror.
    let journal1 = Journal::open(&journal1_path).map_err(|e| SandboxError::Io(e.to_string()))?;
    let mut writer1 = journal1
        .claim(StreamId::Mail)
        .map_err(|e| SandboxError::Io(e.to_string()))?;
    let sys_by = Principal::System {
        part: SystemPart::MailBridge,
    };

    let mut imported_msgs_for_rooms: Vec<MailMessage> = Vec::with_capacity(by_sha.len());
    let mut expected_working_paths: Vec<(PathBuf, Sha256Hex)> = Vec::with_capacity(by_sha.len());
    let mut sample_query_word: Option<String> = None;

    for src in by_sha.values() {
        let msg = parse_mail_message_from_bytes(
            &src.rel_path,
            &src.raw_bytes,
            src.box_seat.as_ref(),
            &src.extra_recipients,
            Some(src.seen_at),
            aliases,
            MailOrigin::Import,
        );
        if sample_query_word.is_none() {
            sample_query_word = msg
                .subject
                .split(|c: char| !c.is_ascii_alphanumeric())
                .find(|w| w.len() >= 5)
                .map(str::to_owned);
        }
        let prior_ack_sha = if acked_shas.contains(&msg.sha256) {
            None
        } else {
            canonical_ack_index.mutated_after_ack("", &src.rel_path, &msg.sha256, aliases)
        };
        let mutated_after_ack = prior_ack_sha.is_some();

        let idem = IdemKey::parse(format!("mail_msg:{}", msg.sha256))
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        writer1
            .append(
                &sys_by,
                msg.created_at,
                Some(&idem),
                vec![Event::Mail(MailEvent::MessageCommitted {
                    message: Box::new(msg.clone()),
                    mutated_after_ack,
                    prior_ack_sha,
                })],
            )
            .map_err(|e| SandboxError::Io(e.to_string()))?;

        // Export immutable blob + human-readable working-tree file.
        mirror
            .export_blob_if_absent(&msg.sha256, &src.raw_bytes)
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        mirror
            .export_working_tree_if_absent(&msg.path, &src.raw_bytes)
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        expected_working_paths.push((msg.path.clone(), msg.sha256.clone()));

        if !mutated_after_ack {
            imported_msgs_for_rooms.push(msg);
        }
    }

    // Export acks in one atomic file write for speed across 1,000+ historical acks,
    // while committing every ack into StreamId::Mail.
    let mut acks_jsonl_buf = String::new();
    for ((seat, sha), (rel, ack_at)) in &resolved_acks {
        let idem_hash = sha256_short16(&format!("ack/2\n{seat}\n{sha}"));
        let idem = IdemKey::parse(format!("ack:{idem_hash}"))
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        writer1
            .append(
                &sys_by,
                *ack_at,
                Some(&idem),
                vec![Event::Mail(MailEvent::Acked {
                    path: PathBuf::from(rel),
                    sha256: sha.clone(),
                    seat: Some(seat.clone()),
                    note: None,
                })],
            )
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        let line = serde_json::json!({
            "schema": 1,
            "ts": ack_at.to_rfc3339_sec(),
            "letter": rel,
            "path": rel,
            "sha256": sha.as_str(),
            "sha256_full": sha.as_str(),
            "acked_by": seat.as_str(),
            "seat": seat.as_str(),
        });
        acks_jsonl_buf.push_str(&line.to_string());
        acks_jsonl_buf.push('\n');
    }
    drop(writer1);

    let state_dir = mirror
        .ensure_subdir_segments(&["state"])
        .map_err(|e| SandboxError::Io(e.to_string()))?;
    rex_wave_fs::write_atomic_path(
        &state_dir.join("acks.jsonl"),
        acks_jsonl_buf.as_bytes(),
        SANDBOX_FILE_MODE,
        true,
    )
    .map_err(|e| SandboxError::Io(e.to_string()))?;

    let (imported_dm_rooms, imported_thread_channels) =
        project_messages_to_journal_rooms(&journal1, &imported_msgs_for_rooms)
            .map_err(|e| SandboxError::Io(e.to_string()))?;

    // Verify 100% on-disk blob & working-tree sha256 parity.
    let mut blob_parity_count = 0usize;
    for sha in by_sha.keys() {
        let blob_path = mirror
            .path()
            .join("objects")
            .join(format!("{}.md", sha.as_str()));
        let bytes = rex_wave_fs::read_regular_bounded(&blob_path, MAX_LETTER_BYTES)
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        if &sha256_bytes(&bytes) == sha {
            blob_parity_count += 1;
        }
    }
    let mut working_tree_parity_count = 0usize;
    for (rel, expected_sha) in &expected_working_paths {
        let wt_path = mirror.path().join(rel);
        let bytes = rex_wave_fs::read_regular_bounded(&wt_path, MAX_LETTER_BYTES)
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        if &sha256_bytes(&bytes) == expected_sha {
            working_tree_parity_count += 1;
        }
    }

    // Snapshot journal1 state before deleting journal1.
    let state1 = MailState::from_journal(&journal1).map_err(|e| SandboxError::Io(e.to_string()))?;
    let db1 = state1.db_set();
    let acked1 = state1.acked.clone();
    let flagged1 = state1.flagged_shas.clone();
    let dm_rooms1: BTreeSet<RoomId> = journal1
        .streams()
        .map_err(|e| SandboxError::Io(e.to_string()))?
        .into_iter()
        .filter_map(|(s, _)| match s {
            StreamId::Room {
                room: r @ RoomId::Direct { .. },
            } => Some(r),
            _ => None,
        })
        .collect();
    drop(journal1);

    // 3. Delete journal1 (and WAL/SHM sidecars), then rebuild from mirror alone!
    for suffix in ["", "-wal", "-shm"] {
        let mut p = journal1_path.as_os_str().to_os_string();
        p.push(suffix);
        let _ = fs::remove_file(PathBuf::from(p));
    }

    let rebuilt = rebuild_mail_from_mirror(mirror.path(), &journal2_path, aliases, protection)?;
    let journal2 = Journal::open(&journal2_path).map_err(|e| SandboxError::Io(e.to_string()))?;
    let state2 = MailState::from_journal(&journal2).map_err(|e| SandboxError::Io(e.to_string()))?;
    let db2 = state2.db_set();
    let acked2 = state2.acked.clone();
    let flagged2 = state2.flagged_shas.clone();
    let dm_rooms2: BTreeSet<RoomId> = journal2
        .streams()
        .map_err(|e| SandboxError::Io(e.to_string()))?
        .into_iter()
        .filter_map(|(s, _)| match s {
            StreamId::Room {
                room: r @ RoomId::Direct { .. },
            } => Some(r),
            _ => None,
        })
        .collect();

    let fts_sample_query_hits = match sample_query_word {
        Some(ref w) => journal2
            .search_mail(w, None, 20)
            .map_err(|e| SandboxError::Io(e.to_string()))?
            .len(),
        None => 0,
    };

    let source_shas: BTreeSet<Sha256Hex> = by_sha.keys().cloned().collect();
    let db_parity = db1 == source_shas
        && db1 == db2
        && blob_parity_count == unique_sha256_count
        && working_tree_parity_count == unique_sha256_count;
    let ack_parity = acked1 == acked2;
    let flagged_parity = flagged1 == flagged2;
    let dm_room_parity = dm_rooms1 == dm_rooms2;
    let parity_ok = db_parity && ack_parity && flagged_parity && dm_room_parity;

    Ok(MailMigrateVerifyReport {
        interchange_dir: ro.root().to_path_buf(),
        work_dir: verified_work,
        source_files_scanned,
        unique_sha256_count,
        duplicate_content_copies,
        source_ack_lines_parsed: acks_report.records.len(),
        source_ack_lines_skipped: acks_report.skipped_lines.len(),
        imported_messages: db1.len(),
        imported_acks: acked1.len(),
        imported_mutated_after_ack: flagged1.len(),
        imported_dm_rooms,
        imported_thread_channels,
        rebuilt_messages: rebuilt.messages_rebuilt,
        rebuilt_acks: rebuilt.acks_rebuilt,
        rebuilt_mutated_after_ack: rebuilt.mutated_after_ack_flagged,
        rebuilt_dm_rooms: rebuilt.dm_rooms_projected,
        rebuilt_thread_channels: rebuilt.thread_channels_projected,
        blob_parity_count,
        working_tree_parity_count,
        fts_sample_query_hits,
        db_parity,
        ack_parity,
        flagged_parity,
        dm_room_parity,
        parity_ok,
    })
}
