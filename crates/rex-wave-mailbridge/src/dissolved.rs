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
    ConfinedDir, EntryKind, FileId, OutsidePath, ProtectedRootsGuard, Protection, RelPath,
    SafeName, SandboxError,
};
use rex_wave_runtime::{DecideCtx, Hub, Service};
use rex_wave_store::{Appended, Journal, StreamWriter};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::io::{Read as _, Write as _};
use std::os::unix::fs::MetadataExt as _;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::UNIX_EPOCH;

const MAX_ACKS_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_INDEX_FILE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_ROOM_BODY_BYTES: usize = 64 * 1024;
static DROP_CLAIM_NONCE: AtomicU64 = AtomicU64::new(1);

fn mirror_ledger_lock(canon_root: &Path) -> Arc<StdMutex<()>> {
    static LOCKS: LazyLock<StdMutex<BTreeMap<PathBuf, Arc<StdMutex<()>>>>> =
        LazyLock::new(|| StdMutex::new(BTreeMap::new()));
    let mut map = LOCKS.lock().unwrap_or_else(|e| e.into_inner());
    map.entry(canon_root.to_path_buf())
        .or_insert_with(|| Arc::new(StdMutex::new(())))
        .clone()
}

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

    let mut participants_hasher = Sha256::new();
    participants_hasher.update(b"mail-participants/1\n");
    for s in &distinct_seats {
        participants_hasher.update(s.as_str().as_bytes());
        participants_hasher.update(b"\n");
    }
    let participants_hash8: String = participants_hasher
        .finalize()
        .iter()
        .take(4)
        .map(|b| format!("{b:02x}"))
        .collect();

    let base_slug = topic
        .map(|t| sanitize_channel_slug(t, 44))
        .filter(|s| !s.is_empty())
        .or_else(|| {
            let clean_subj = strip_reply_prefixes(subject);
            let s = sanitize_channel_slug(clean_subj, 44);
            (!s.is_empty()).then_some(s)
        })
        .unwrap_or_else(|| sha256.as_str()[..12].to_owned());
    let chan_name = format!("mail-{base_slug}-{participants_hash8}");
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
    let (parse_markdown, raw_markdown, raw_bytes_b64) = match std::str::from_utf8(raw_bytes) {
        Ok(s) => (s.to_owned(), s.to_owned(), None),
        Err(_) => (
            String::from_utf8_lossy(raw_bytes).into_owned(),
            String::new(),
            Some(base64::engine::general_purpose::STANDARD.encode(raw_bytes)),
        ),
    };

    let doc = FrontmatterDoc::parse(&parse_markdown);
    let entries = if doc.has_frontmatter {
        doc.entries.clone()
    } else {
        FrontmatterDoc::parse_headers_lenient(&parse_markdown)
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
    if !doc.has_frontmatter && box_seat.is_none() && to_seats.len() + cc_seats.len() > 1 {
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
    if msg.origin == MailOrigin::Drop
        && let Ok(l) = Label::parse("origin=drop")
    {
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
    pub committed_by: Principal,
    pub message: MailMessage,
    pub mutated_after_ack: bool,
    pub prior_ack_sha: Option<String>,
    pub send_idem: Option<IdemKey>,
}

/// Resolution of an ACK target against [`MailState`], allowing authorization
/// checks to run strictly BEFORE conflict or ambiguity details are returned.
#[derive(Clone, Debug)]
pub enum AckTargetResolution {
    Found {
        entry: CommittedMailEntry,
        expected_sha_mismatch: Option<Sha256Hex>,
        path_mismatch: Option<PathBuf>,
    },
    AmbiguousPath {
        raw_path: PathBuf,
        candidates: Vec<CommittedMailEntry>,
    },
    NotDissolved,
}

/// Append-only mirror provenance row stored in `<mirror>/state/mail-index.jsonl`
/// so offline `rebuild` reproduces 100% of recipients, historical paths,
/// `mutated_after_ack` flags, `created_at` ms, `origin`, and caller `--id` keys.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MirrorIndexRecord {
    pub schema: u8,
    pub sha256: Sha256Hex,
    pub path: String,
    pub to: Vec<SeatName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cc: Vec<SeatName>,
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub mutated_after_ack: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_ack_sha: Option<String>,
    #[serde(default)]
    pub origin: MailOrigin,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_idem: Option<IdemKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_by: Option<Principal>,
}

impl MirrorIndexRecord {
    pub fn from_committed(entry: &CommittedMailEntry) -> Self {
        let norm_path = normalize_letter_path(&entry.message.path.to_string_lossy())
            .map(|n| n.canonical)
            .unwrap_or_else(|| entry.message.path.to_string_lossy().into_owned());
        Self {
            schema: 1,
            sha256: entry.message.sha256.clone(),
            path: norm_path,
            to: entry.message.to.clone(),
            cc: entry.message.cc.clone(),
            created_at_ms: entry.message.created_at.0,
            mutated_after_ack: entry.mutated_after_ack,
            prior_ack_sha: entry.prior_ack_sha.clone(),
            origin: entry.message.origin,
            send_idem: entry.send_idem.clone(),
            send_by: entry.send_idem.as_ref().map(|_| entry.committed_by.clone()),
        }
    }
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
    pub by_send_idem: BTreeMap<IdemKey, Sha256Hex>,
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
    /// Replays all `StreamId::Mail` entries from `journal` into a fresh [`MailState`],
    /// and hydrates `room_post_seqs` from durable `StreamId::Room` posts (`mail:<sha256>`).
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
        state.hydrate_room_posts(journal)?;
        Ok(state)
    }

    pub fn hydrate_room_posts(&mut self, journal: &Journal) -> Result<(), ApiError> {
        let referenced_rooms: BTreeSet<RoomId> = self
            .messages
            .values()
            .filter_map(|e| e.message.room.clone())
            .collect();
        for room in referenced_rooms {
            let stream = StreamId::Room { room: room.clone() };
            let mut after = Seq::ORIGIN;
            loop {
                let page = journal
                    .read(&stream, after, 512)
                    .map_err(|e| ApiError::Internal(e.to_string()))?;
                if page.is_empty() {
                    break;
                }
                for env in &page {
                    after = env.seq;
                    if let Event::Room(RoomEvent::Posted {
                        idem: Some(idem), ..
                    }) = &env.event
                        && let Some(hex) = idem.as_str().strip_prefix("mail:")
                        && let Ok(sha) = Sha256Hex::parse(hex)
                    {
                        self.room_post_seqs.insert((room.clone(), sha), env.seq);
                    }
                }
            }
        }
        Ok(())
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
                send_idem,
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
                if let Some(idem) = send_idem {
                    self.by_send_idem
                        .entry(idem.clone())
                        .or_insert_with(|| sha.clone());
                }
                if *mutated_after_ack {
                    self.flagged_shas.insert(sha.clone());
                }
                self.messages
                    .entry(sha)
                    .or_insert_with(|| CommittedMailEntry {
                        seq: env.seq,
                        committed_by: env.by.clone(),
                        message: (**message).clone(),
                        mutated_after_ack: *mutated_after_ack,
                        prior_ack_sha: prior_ack_sha.clone(),
                        send_idem: send_idem.clone(),
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

    /// Resolves the current (latest) revision at `path` or by exact `sha256`.
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
            return shas.last().and_then(|s| self.messages.get(s));
        }
        let mut matched: Vec<&CommittedMailEntry> = Vec::new();
        for (p_key, shas) in &self.by_path {
            if letter_paths_match(p_key, &norm.canonical)
                && let Some(last_sha) = shas.last()
                && let Some(e) = self.messages.get(last_sha)
            {
                matched.push(e);
            }
        }
        (matched.len() == 1).then(|| matched[0])
    }

    /// Resolves an ACK target strictly to the exact `sha256` or the current
    /// (latest) revision at `path`, refusing ambiguous bare filenames and
    /// preserving candidate entries so recipient authorization runs FIRST.
    pub fn resolve_ack_target(
        &self,
        path: Option<&Path>,
        sha256: Option<&Sha256Hex>,
    ) -> AckTargetResolution {
        if let Some(sha) = sha256 {
            if let Some(entry) = self.messages.get(sha).cloned() {
                let path_mismatch = path.and_then(|p| {
                    (!letter_paths_match(
                        &entry.message.path.to_string_lossy(),
                        &p.to_string_lossy(),
                    ))
                    .then(|| p.to_path_buf())
                });
                return AckTargetResolution::Found {
                    entry,
                    expected_sha_mismatch: None,
                    path_mismatch,
                };
            }
            if let Some(p) = path {
                return match self.resolve_ack_target(Some(p), None) {
                    AckTargetResolution::Found { entry, .. } => AckTargetResolution::Found {
                        entry,
                        expected_sha_mismatch: Some(sha.clone()),
                        path_mismatch: None,
                    },
                    other => other,
                };
            }
            return AckTargetResolution::NotDissolved;
        }

        let Some(p) = path else {
            return AckTargetResolution::NotDissolved;
        };
        let Some(norm) = normalize_letter_path(&p.to_string_lossy()) else {
            return AckTargetResolution::NotDissolved;
        };
        if let Some(shas) = self.by_path.get(&norm.canonical)
            && let Some(latest) = shas.last().and_then(|s| self.messages.get(s)).cloned()
        {
            return AckTargetResolution::Found {
                entry: latest,
                expected_sha_mismatch: None,
                path_mismatch: None,
            };
        }
        let mut candidates = Vec::new();
        for (p_key, shas) in &self.by_path {
            if letter_paths_match(p_key, &norm.canonical)
                && let Some(latest) = shas.last().and_then(|s| self.messages.get(s)).cloned()
            {
                candidates.push(latest);
            }
        }
        match candidates.len() {
            0 => AckTargetResolution::NotDissolved,
            1 => AckTargetResolution::Found {
                entry: candidates.remove(0),
                expected_sha_mismatch: None,
                path_mismatch: None,
            },
            _ => AckTargetResolution::AmbiguousPath {
                raw_path: p.to_path_buf(),
                candidates,
            },
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
        // Check protected descendants BEFORE creating any directory on disk.
        let mut candidate = self.dir.path().to_path_buf();
        for seg in names.segments() {
            candidate.push(seg.as_str());
            if self.guard.check(&candidate, &candidate).is_err() {
                return Err(ApiError::Denied {
                    principal: "mailbridge".into(),
                    action: format!("protected descendant refused: {}", candidate.display()),
                });
            }
        }
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

    fn acquire_state_ledger_lock(&self, state_dir: &Path) -> Result<fs::File, ApiError> {
        let lock_rel = RelPath::parse("state/.ledger.lock")
            .map_err(|e| ApiError::Internal(format!("ledger lock relpath: {e}")))?;
        let lock_path = self.dir.resolve(&lock_rel).map_err(|e| {
            if e.kind() == std::io::ErrorKind::InvalidInput {
                ApiError::Denied {
                    principal: "mailbridge".into(),
                    action: format!("state dir symlink refused: {e}"),
                }
            } else {
                ApiError::Internal(format!("resolve {}: {e}", state_dir.display()))
            }
        })?;
        let lock_file = rex_wave_fs::open_lock_nofollow(&lock_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::InvalidInput {
                ApiError::Denied {
                    principal: "mailbridge".into(),
                    action: format!("lock through symlink {}: {e}", lock_path.display()),
                }
            } else {
                ApiError::Internal(format!("open {}: {e}", lock_path.display()))
            }
        })?;
        rex_wave_fs::lock_exclusive_blocking(&lock_file)
            .map_err(|e| ApiError::Internal(format!("flock {}: {e}", lock_path.display())))?;
        Ok(lock_file)
    }

    /// Exports a batch of ACK receipts to `<mirror>/state/acks.jsonl` idempotently
    /// under serialized process + file locking, parsing the existing ledger at most
    /// once and reading/writing through a single `O_NOFOLLOW` descriptor.
    pub fn append_acks_batch_if_absent(
        &self,
        acks: &[CommittedAckRecord],
    ) -> Result<usize, ApiError> {
        if acks.is_empty() {
            return Ok(0);
        }
        let state_dir = self.ensure_subdir_segments(&["state"])?;
        let proc_mtx = mirror_ledger_lock(self.dir.path());
        let _proc_guard = proc_mtx.lock().unwrap_or_else(|e| e.into_inner());
        let _flock_guard = self.acquire_state_ledger_lock(&state_dir)?;

        let acks_rel = RelPath::parse("state/acks.jsonl")
            .map_err(|e| ApiError::Internal(format!("acks relpath: {e}")))?;
        let acks_file = self.dir.resolve(&acks_rel).map_err(|e| {
            if e.kind() == std::io::ErrorKind::InvalidInput {
                ApiError::Denied {
                    principal: "mailbridge".into(),
                    action: format!("resolve acks.jsonl refused: {e}"),
                }
            } else {
                ApiError::Internal(format!("resolve acks.jsonl: {e}"))
            }
        })?;
        let (mut f, meta) = rex_wave_fs::open_rw_append_nofollow(&acks_file, SANDBOX_FILE_MODE)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::InvalidInput {
                    ApiError::Denied {
                        principal: "mailbridge".into(),
                        action: format!("open non-regular acks.jsonl {}: {e}", acks_file.display()),
                    }
                } else {
                    ApiError::Internal(format!("open acks.jsonl: {e}"))
                }
            })?;
        if meta.len() > MAX_ACKS_FILE_BYTES {
            return Err(ApiError::Internal(format!(
                "acks.jsonl exceeds {MAX_ACKS_FILE_BYTES} bytes"
            )));
        }
        let mut existing_bytes = Vec::with_capacity(meta.len() as usize);
        (&mut f)
            .take(MAX_ACKS_FILE_BYTES + 1)
            .read_to_end(&mut existing_bytes)
            .map_err(|e| ApiError::Internal(format!("read acks.jsonl: {e}")))?;
        if existing_bytes.len() as u64 > MAX_ACKS_FILE_BYTES {
            return Err(ApiError::Internal(format!(
                "acks.jsonl exceeds {MAX_ACKS_FILE_BYTES} bytes"
            )));
        }

        let aliases = AliasMap::empty();
        let text = std::str::from_utf8(&existing_bytes).map_err(|_| {
            ApiError::Internal(
                "corrupt non-UTF-8 content in state/acks.jsonl; repair before appending".into(),
            )
        })?;
        for (idx_line, raw_line) in text.split('\n').enumerate() {
            if raw_line.len() > crate::MAX_ACK_LINE_BYTES {
                return Err(ApiError::Internal(format!(
                    "oversized row at state/acks.jsonl:{} exceeds {} bytes",
                    idx_line + 1,
                    crate::MAX_ACK_LINE_BYTES
                )));
            }
        }
        let parsed_report = parse_acks_jsonl(text, &aliases);
        if let Some(bad) = parsed_report.skipped_lines.first() {
            return Err(ApiError::Internal(format!(
                "corrupt JSON row at state/acks.jsonl:{} ({})",
                bad.line_number, bad.reason
            )));
        }
        let idx = parsed_report.into_index();
        let mut seen_in_batch: BTreeSet<(SeatName, Sha256Hex)> = BTreeSet::new();
        let mut out_buf = Vec::new();
        let mut needs_leading_nl = !existing_bytes.is_empty() && !existing_bytes.ends_with(b"\n");
        let mut appended_count = 0usize;

        for ack in acks {
            if !seen_in_batch.insert((ack.seat.clone(), ack.sha256.clone())) {
                continue;
            }
            if idx.is_acked_by_digest(ack.seat.as_str(), &ack.sha256, &aliases) {
                continue;
            }
            let path_str = normalize_letter_path(&ack.path.to_string_lossy())
                .map(|n| n.canonical)
                .unwrap_or_else(|| ack.path.to_string_lossy().into_owned());
            let mut obj = serde_json::json!({
                "schema": 1,
                "ts": ack.at.to_rfc3339_sec(),
                "at_ms": ack.at.0,
                "letter": path_str,
                "path": path_str,
                "sha256": ack.sha256.as_str(),
                "sha256_full": ack.sha256.as_str(),
                "acked_by": ack.seat.as_str(),
                "seat": ack.seat.as_str(),
            });
            if let Some(n) = ack.note.as_deref().map(str::trim).filter(|n| !n.is_empty())
                && let Some(map) = obj.as_object_mut()
            {
                map.insert("note".into(), serde_json::Value::String(n.to_owned()));
            }
            if needs_leading_nl {
                out_buf.push(b'\n');
                needs_leading_nl = false;
            }
            serde_json::to_writer(&mut out_buf, &obj)
                .map_err(|e| ApiError::Internal(e.to_string()))?;
            out_buf.push(b'\n');
            appended_count += 1;
        }

        if appended_count > 0 {
            f.write_all(&out_buf)
                .map_err(|e| ApiError::Internal(format!("append acks.jsonl: {e}")))?;
            f.sync_all()
                .map_err(|e| ApiError::Internal(format!("fsync acks.jsonl: {e}")))?;
            rex_wave_fs::sync_dir(&state_dir)
                .map_err(|e| ApiError::Internal(format!("fsync state dir: {e}")))?;
        }
        Ok(appended_count)
    }

    /// Exports one ACK receipt to `<mirror>/state/acks.jsonl` idempotently.
    pub fn append_ack_if_absent(
        &self,
        seat: &SeatName,
        rel_path: &Path,
        sha256: &Sha256Hex,
        at: UnixMs,
        note: Option<&str>,
    ) -> Result<bool, ApiError> {
        let single = CommittedAckRecord {
            seq: Seq::ORIGIN,
            seat: seat.clone(),
            sha256: sha256.clone(),
            path: rel_path.to_path_buf(),
            note: note.map(str::to_owned),
            at,
        };
        Ok(self.append_acks_batch_if_absent(&[single])? > 0)
    }

    /// Exports provenance rows to `<mirror>/state/mail-index.jsonl` idempotently
    /// under serialized process + file locking through a single `O_NOFOLLOW` fd.
    pub fn append_index_batch_if_absent(
        &self,
        entries: &[MirrorIndexRecord],
    ) -> Result<usize, ApiError> {
        if entries.is_empty() {
            return Ok(0);
        }
        let state_dir = self.ensure_subdir_segments(&["state"])?;
        let proc_mtx = mirror_ledger_lock(self.dir.path());
        let _proc_guard = proc_mtx.lock().unwrap_or_else(|e| e.into_inner());
        let _flock_guard = self.acquire_state_ledger_lock(&state_dir)?;

        let idx_rel = RelPath::parse("state/mail-index.jsonl")
            .map_err(|e| ApiError::Internal(format!("mail-index relpath: {e}")))?;
        let idx_file = self.dir.resolve(&idx_rel).map_err(|e| {
            if e.kind() == std::io::ErrorKind::InvalidInput {
                ApiError::Denied {
                    principal: "mailbridge".into(),
                    action: format!("resolve mail-index.jsonl refused: {e}"),
                }
            } else {
                ApiError::Internal(format!("resolve mail-index.jsonl: {e}"))
            }
        })?;
        let (mut f, meta) = rex_wave_fs::open_rw_append_nofollow(&idx_file, SANDBOX_FILE_MODE)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::InvalidInput {
                    ApiError::Denied {
                        principal: "mailbridge".into(),
                        action: format!(
                            "open non-regular mail-index.jsonl {}: {e}",
                            idx_file.display()
                        ),
                    }
                } else {
                    ApiError::Internal(format!("open mail-index.jsonl: {e}"))
                }
            })?;
        if meta.len() > MAX_INDEX_FILE_BYTES {
            return Err(ApiError::Internal(format!(
                "mail-index.jsonl exceeds {MAX_INDEX_FILE_BYTES} bytes"
            )));
        }
        let mut existing_bytes = Vec::with_capacity(meta.len() as usize);
        (&mut f)
            .take(MAX_INDEX_FILE_BYTES + 1)
            .read_to_end(&mut existing_bytes)
            .map_err(|e| ApiError::Internal(format!("read mail-index.jsonl: {e}")))?;

        let mut existing_keys: HashSet<(Sha256Hex, Option<IdemKey>)> = HashSet::new();
        for line in String::from_utf8_lossy(&existing_bytes).lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Ok(rec) = serde_json::from_str::<MirrorIndexRecord>(trimmed) {
                existing_keys.insert((rec.sha256, rec.send_idem));
            }
        }

        let mut out_buf = Vec::new();
        let mut needs_leading_nl = !existing_bytes.is_empty() && !existing_bytes.ends_with(b"\n");
        let mut appended = 0usize;
        for entry in entries {
            let key = (entry.sha256.clone(), entry.send_idem.clone());
            if !existing_keys.insert(key) {
                continue;
            }
            if needs_leading_nl {
                out_buf.push(b'\n');
                needs_leading_nl = false;
            }
            serde_json::to_writer(&mut out_buf, entry)
                .map_err(|e| ApiError::Internal(e.to_string()))?;
            out_buf.push(b'\n');
            appended += 1;
        }
        if appended > 0 {
            f.write_all(&out_buf)
                .map_err(|e| ApiError::Internal(format!("append mail-index.jsonl: {e}")))?;
            f.sync_all()
                .map_err(|e| ApiError::Internal(format!("fsync mail-index.jsonl: {e}")))?;
            rex_wave_fs::sync_dir(&state_dir)
                .map_err(|e| ApiError::Internal(format!("fsync state dir: {e}")))?;
        }
        Ok(appended)
    }

    /// Reads `<mirror>/state/acks.jsonl` safely through `ConfinedDir` without creating it.
    pub fn read_acks(&self, aliases: &AliasMap) -> Result<Vec<AckRecord>, ApiError> {
        let Ok(rel) = RelPath::parse("state/acks.jsonl") else {
            return Ok(Vec::new());
        };
        let bytes = match self.dir.read_regular(&rel, MAX_ACKS_FILE_BYTES) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => {
                return Err(ApiError::Denied {
                    principal: "mailbridge".into(),
                    action: format!("read acks.jsonl refused: {e}"),
                });
            }
            Err(e) => return Err(ApiError::Internal(format!("read acks.jsonl: {e}"))),
        };
        let text = String::from_utf8_lossy(&bytes);
        Ok(parse_acks_jsonl(&text, aliases).records)
    }

    /// Reads `<mirror>/state/mail-index.jsonl` safely through `ConfinedDir` without creating it.
    pub fn read_mail_index(&self) -> Result<Vec<MirrorIndexRecord>, ApiError> {
        let Ok(rel) = RelPath::parse("state/mail-index.jsonl") else {
            return Ok(Vec::new());
        };
        let bytes = match self.dir.read_regular(&rel, MAX_INDEX_FILE_BYTES) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => {
                return Err(ApiError::Denied {
                    principal: "mailbridge".into(),
                    action: format!("read mail-index.jsonl refused: {e}"),
                });
            }
            Err(e) => return Err(ApiError::Internal(format!("read mail-index.jsonl: {e}"))),
        };
        Ok(parse_mail_index_jsonl(&String::from_utf8_lossy(&bytes)))
    }
}

pub fn parse_mail_index_jsonl(contents: &str) -> Vec<MirrorIndexRecord> {
    let mut out = Vec::new();
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(rec) = serde_json::from_str::<MirrorIndexRecord>(trimmed) {
            out.push(rec);
        }
    }
    out
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

/// Metadata fingerprint `(dev, ino, len, mtime_sec, mtime_nsec)` used to skip
/// re-reading and re-hashing unchanged mirror files across periodic reconciles.
pub type StatSig = (u64, u64, u64, i64, i64);

fn stat_sig(meta: &fs::Metadata) -> StatSig {
    (
        meta.dev(),
        meta.ino(),
        meta.len(),
        meta.mtime(),
        meta.mtime_nsec(),
    )
}

fn meta_mtime_ms(meta: &fs::Metadata) -> Option<UnixMs> {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| UnixMs(d.as_millis() as u64))
}

/// Persistent metadata -> `sha256` cache held outside `MailBridge`'s writer mutex.
#[derive(Clone, Debug, Default)]
pub struct DiskScanCache {
    pub blobs: BTreeMap<PathBuf, (StatSig, Option<UnixMs>)>,
    pub working: BTreeMap<PathBuf, (StatSig, Sha256Hex, Option<UnixMs>)>,
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
    pub claimed_sig: Option<(FileId, u64)>,
}

fn strip_drop_claim_name(name: &str) -> Option<&str> {
    let rest = name.strip_prefix(".rex-claim-")?;
    let (_, orig) = rest.split_once("--")?;
    (!orig.is_empty() && !is_ignored_md_filename(orig)).then_some(orig)
}

/// Scans working-tree `.md` letters under `root` (`mail/`, `to-*/`, and root `*.md`),
/// ignoring dotfiles, `.#*` Emacs lockfiles, files currently locked by `.#<name>`,
/// `#*#` autosaves, `README*`, and symlinks.
pub fn scan_working_tree_dir(
    root: &Path,
    aliases: &AliasMap,
    is_drop_dir: bool,
) -> Result<Vec<DiskMailFile>, ApiError> {
    let mut dummy_cache = DiskScanCache::default();
    let empty_known = HashSet::new();
    let (files, _) = if is_drop_dir {
        (
            scan_and_claim_drop_dir(Some(root), aliases, false)?,
            HashSet::new(),
        )
    } else {
        scan_working_tree_cached(root, aliases, &empty_known, &mut dummy_cache)?
    };
    Ok(files)
}

/// Cached scan of `<mirror>` working-tree files outside the writer lock.
/// Files whose `(dev, ino, len, mtime)` matches `cache` AND whose `sha256` is
/// already in `known_shas` are recorded in `present_paths` without reading or
/// hashing their contents.
pub fn scan_working_tree_cached(
    root: &Path,
    aliases: &AliasMap,
    known_shas: &HashSet<Sha256Hex>,
    cache: &mut DiskScanCache,
) -> Result<(Vec<DiskMailFile>, HashSet<String>), ApiError> {
    if rex_wave_fs::entry_kind(root).ok() != Some(EntryKind::Dir) {
        return Ok((Vec::new(), HashSet::new()));
    }
    let top = fs::read_dir(root)
        .map_err(|e| ApiError::Internal(format!("scan {}: {e}", root.display())))?;
    let mut dirs: Vec<(String, PathBuf)> = Vec::new();
    let mut candidates: Vec<(String, Option<SeatName>, PathBuf)> = Vec::new();

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
            candidates.push((name.to_owned(), None, entry.path()));
        }
    }
    dirs.sort_by(|a, b| a.0.cmp(&b.0));

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
            candidates.push((format!("{dir_name}/{fname}"), box_seat.clone(), fpath));
        }
    }

    let mut unindexed = Vec::new();
    let mut present_paths = HashSet::with_capacity(candidates.len());
    let mut next_cache = BTreeMap::new();

    for (rel_path, box_seat, fpath) in candidates {
        if let Some(norm) = normalize_letter_path(&rel_path) {
            present_paths.insert(norm.canonical);
        }
        if has_emacs_lock(&fpath) {
            continue;
        }
        let Ok((file, meta)) = rex_wave_fs::open_regular_nofollow(&fpath) else {
            continue;
        };
        let sig = stat_sig(&meta);
        if let Some((prev_sig, cached_sha, cached_mtime)) = cache.working.get(&fpath)
            && *prev_sig == sig
            && known_shas.contains(cached_sha)
        {
            next_cache.insert(fpath, (sig, cached_sha.clone(), *cached_mtime));
            continue;
        }
        let Ok(raw_bytes) = rex_wave_fs::read_open_bounded(&fpath, file, &meta, MAX_LETTER_BYTES)
        else {
            continue;
        };
        let sha256 = sha256_bytes(&raw_bytes);
        let mtime_ms = meta_mtime_ms(&meta);
        next_cache.insert(fpath.clone(), (sig, sha256.clone(), mtime_ms));
        if !known_shas.contains(&sha256) {
            unindexed.push(DiskMailFile {
                abs_path: fpath,
                rel_path,
                box_seat,
                sha256,
                raw_bytes,
                mtime_ms,
                claimed_sig: None,
            });
        }
    }
    cache.working = next_cache;
    unindexed.sort_by(|a, b| (&a.rel_path, &a.sha256).cmp(&(&b.rel_path, &b.sha256)));
    Ok((unindexed, present_paths))
}

/// Scans `<drop_dir>` (and recovers any interrupted `.rex-claim-*` files),
/// atomically claiming each regular unlocked drop file before reading its
/// committed payload when `claim_for_consume` is true.
pub fn scan_and_claim_drop_dir(
    drop_root: Option<&Path>,
    aliases: &AliasMap,
    claim_for_consume: bool,
) -> Result<Vec<DiskMailFile>, ApiError> {
    let Some(root) = drop_root else {
        return Ok(Vec::new());
    };
    if rex_wave_fs::entry_kind(root).ok() != Some(EntryKind::Dir) {
        return Ok(Vec::new());
    }
    let confined = ConfinedDir::open(root).map_err(|e| ApiError::Denied {
        principal: "mailbridge".into(),
        action: format!("open drop_dir {}: {e}", root.display()),
    })?;
    let base = confined.path();
    let top = fs::read_dir(base)
        .map_err(|e| ApiError::Internal(format!("scan {}: {e}", base.display())))?;
    let mut dirs: Vec<(String, PathBuf)> = Vec::new();
    let mut entries_to_process: Vec<(String, Option<SeatName>, PathBuf, bool)> = Vec::new();

    for entry in top.flatten() {
        let fname = entry.file_name();
        let Some(name) = fname.to_str() else {
            continue;
        };
        let Ok(ft) = entry.file_type() else {
            continue;
        };
        if ft.is_symlink() {
            continue;
        }
        if let Some(orig) = strip_drop_claim_name(name)
            && ft.is_file()
        {
            entries_to_process.push((format!("mail/{orig}"), None, entry.path(), true));
            continue;
        }
        if name.starts_with('.') || name == "objects" || name == "state" {
            continue;
        }
        if ft.is_dir() && (name == "mail" || name.starts_with("to-")) {
            dirs.push((name.to_owned(), entry.path()));
        } else if ft.is_file() && !is_ignored_md_filename(name) {
            entries_to_process.push((format!("mail/{name}"), None, entry.path(), false));
        }
    }
    dirs.sort_by(|a, b| a.0.cmp(&b.0));

    for (dir_name, dir_path) in dirs {
        let Ok(_) = SafeName::parse(&dir_name) else {
            continue;
        };
        if rex_wave_fs::dir_metadata_nofollow(&dir_path).is_err() {
            continue;
        }
        let box_seat = if dir_name == "mail" || dir_name == "to-desktop" {
            None
        } else {
            dir_name
                .strip_prefix("to-")
                .and_then(|raw| aliases.canonical_seat_name(raw))
        };
        let Ok(sub) = fs::read_dir(&dir_path) else {
            continue;
        };
        for f in sub.flatten() {
            let fname = f.file_name();
            let Some(name) = fname.to_str() else {
                continue;
            };
            let Ok(ft) = f.file_type() else {
                continue;
            };
            if !ft.is_file() || ft.is_symlink() {
                continue;
            }
            if let Some(orig) = strip_drop_claim_name(name) {
                entries_to_process.push((
                    format!("{dir_name}/{orig}"),
                    box_seat.clone(),
                    f.path(),
                    true,
                ));
            } else if !is_ignored_md_filename(name) {
                entries_to_process.push((
                    format!("{dir_name}/{name}"),
                    box_seat.clone(),
                    f.path(),
                    false,
                ));
            }
        }
    }

    let mut out = Vec::new();
    for (rel_path, box_seat, fpath, already_claimed) in entries_to_process {
        if !already_claimed && has_emacs_lock(&fpath) {
            continue;
        }
        let Ok((file, meta)) = rex_wave_fs::open_regular_nofollow(&fpath) else {
            continue;
        };
        let id = rex_wave_fs::file_id(&meta);
        let Ok(initial_bytes) =
            rex_wave_fs::read_open_bounded(&fpath, file, &meta, MAX_LETTER_BYTES)
        else {
            continue;
        };
        if initial_bytes.is_empty() {
            continue;
        }

        let (effective_path, raw_bytes, mtime_ms, claimed_sig) =
            if claim_for_consume && !already_claimed {
                let nonce = DROP_CLAIM_NONCE.fetch_add(1, Ordering::Relaxed);
                let prefix = format!(".rex-claim-{}-{nonce:08x}--", std::process::id());
                let Ok(claim_path) = rex_wave_fs::claim_regular_sibling(&fpath, id, &prefix) else {
                    continue;
                };
                let Ok((cf, cmeta)) = rex_wave_fs::open_regular_nofollow(&claim_path) else {
                    continue;
                };
                let cid = rex_wave_fs::file_id(&cmeta);
                let Ok(cbytes) =
                    rex_wave_fs::read_open_bounded(&claim_path, cf, &cmeta, MAX_LETTER_BYTES)
                else {
                    continue;
                };
                if cbytes.is_empty() {
                    continue;
                }
                let len = cbytes.len() as u64;
                (claim_path, cbytes, meta_mtime_ms(&cmeta), Some((cid, len)))
            } else if claim_for_consume && already_claimed {
                let len = initial_bytes.len() as u64;
                (fpath, initial_bytes, meta_mtime_ms(&meta), Some((id, len)))
            } else {
                (fpath, initial_bytes, meta_mtime_ms(&meta), None)
            };

        let sha256 = sha256_bytes(&raw_bytes);
        out.push(DiskMailFile {
            abs_path: effective_path,
            rel_path,
            box_seat,
            sha256,
            raw_bytes,
            mtime_ms,
            claimed_sig,
        });
    }
    out.sort_by(|a, b| (&a.rel_path, &a.sha256).cmp(&(&b.rel_path, &b.sha256)));
    Ok(out)
}

/// Unlinks a claimed drop file ONLY if its `(dev, ino)` and exact `sha256`
/// still match what was committed and exported, preventing data loss if an
/// open writer appended more bytes while reconciliation was in flight.
pub fn finish_claimed_drop_file(drop: &DiskMailFile) -> bool {
    let Some((expected_id, expected_len)) = drop.claimed_sig else {
        return false;
    };
    let Ok((file, meta)) = rex_wave_fs::open_regular_nofollow(&drop.abs_path) else {
        return false;
    };
    if rex_wave_fs::file_id(&meta) != expected_id || meta.len() != expected_len {
        return false;
    }
    let Ok(after_bytes) =
        rex_wave_fs::read_open_bounded(&drop.abs_path, file, &meta, MAX_LETTER_BYTES)
    else {
        return false;
    };
    if sha256_bytes(&after_bytes) != drop.sha256 {
        return false;
    }
    fs::remove_file(&drop.abs_path).is_ok()
}

/// Scans `<mirror>/objects/<sha256>.md` verifying exact content-hash integrity.
pub fn scan_object_blobs(
    mirror_root: &Path,
) -> Result<Vec<(Sha256Hex, Vec<u8>, Option<UnixMs>)>, ApiError> {
    let mut dummy_cache = DiskScanCache::default();
    let empty_known = HashSet::new();
    let (blobs, _) = scan_object_blobs_cached(mirror_root, &empty_known, &mut dummy_cache)?;
    Ok(blobs)
}

/// Cached scan of `<mirror>/objects/<sha256>.md` outside the writer lock.
/// Blobs whose `(dev, ino, len, mtime)` matches a previously verified run AND
/// whose `sha256` is already in `known_shas` are recorded in `present_shas`
/// without reading or hashing their bytes.
pub fn scan_object_blobs_cached(
    mirror_root: &Path,
    known_shas: &HashSet<Sha256Hex>,
    cache: &mut DiskScanCache,
) -> Result<
    (
        Vec<(Sha256Hex, Vec<u8>, Option<UnixMs>)>,
        HashSet<Sha256Hex>,
    ),
    ApiError,
> {
    let objects_dir = mirror_root.join("objects");
    if rex_wave_fs::entry_kind(&objects_dir).ok() != Some(EntryKind::Dir) {
        return Ok((Vec::new(), HashSet::new()));
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

    let mut unindexed = Vec::new();
    let mut present_shas = HashSet::with_capacity(files.len());
    let mut next_cache = BTreeMap::new();

    for (expected_sha, path) in files {
        let (file, meta) = rex_wave_fs::open_regular_nofollow(&path)
            .map_err(|e| ApiError::Internal(format!("open blob {}: {e}", path.display())))?;
        let sig = stat_sig(&meta);
        if let Some((prev_sig, cached_mtime)) = cache.blobs.get(&path)
            && *prev_sig == sig
            && known_shas.contains(&expected_sha)
        {
            present_shas.insert(expected_sha);
            next_cache.insert(path, (sig, *cached_mtime));
            continue;
        }
        let bytes = rex_wave_fs::read_open_bounded(&path, file, &meta, MAX_LETTER_BYTES)
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
        let mtime_ms = meta_mtime_ms(&meta);
        present_shas.insert(actual_sha.clone());
        next_cache.insert(path, (sig, mtime_ms));
        if !known_shas.contains(&actual_sha) {
            unindexed.push((actual_sha, bytes, mtime_ms));
        }
    }
    cache.blobs = next_cache;
    Ok((unindexed, present_shas))
}

/// Projects a dissolved [`MailMessage`] into its target room via a live `RoomCommand` service.
pub async fn project_message_to_room_service(
    room_svc: &dyn Service<RoomCommand>,
    msg: &MailMessage,
    parent_seq: Option<Seq>,
    caller_ctx: Option<&DecideCtx>,
) -> Option<Seq> {
    let room = msg.room.clone()?;
    // For multi-seat mail channels, open the private `#mail-*` channel as
    // `SystemPart::MailBridge` and join every participant BEFORE posting so
    // participant-only read/write privacy holds at every sequence number.
    if matches!(room, RoomId::Channel { .. }) {
        let sys_ctx = DecideCtx::for_system_part(SystemPart::MailBridge);
        let mut participants: Vec<Principal> = Vec::new();
        if let Ok(sender_seat) = SeatName::parse(normalize_seat_token(&msg.from)) {
            participants.push(Principal::Seat { seat: sender_seat });
        }
        for seat in msg.to.iter().chain(msg.cc.iter()) {
            let p = Principal::Seat { seat: seat.clone() };
            if !participants.contains(&p) {
                participants.push(p);
            }
        }
        for member in participants {
            let _ = room_svc
                .dispatch(
                    RoomCommand::RoomJoin(rex_wave_api::room::RoomJoin {
                        room: RoomRef::Id { id: room.clone() },
                        member: Some(member),
                        handle: None,
                    }),
                    sys_ctx.clone(),
                )
                .await;
        }
    }

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
    project_messages_to_journal_rooms_with_authors(journal, messages, &BTreeMap::new())
}

pub fn project_messages_to_journal_rooms_with_authors(
    journal: &Journal,
    messages: &[MailMessage],
    send_authors: &BTreeMap<Sha256Hex, Principal>,
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

            let author = send_authors
                .get(&msg.sha256)
                .cloned()
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
                if let Ok(from_seat) = SeatName::parse(normalize_seat_token(&msg.from)) {
                    participants.push(Principal::Seat { seat: from_seat });
                }
                for seat in msg.to.iter().chain(msg.cc.iter()) {
                    let p = Principal::Seat { seat: seat.clone() };
                    if !participants.contains(&p) {
                        participants.push(p);
                    }
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
                .append(&author, msg.created_at, Some(&idem), events)
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

fn apply_index_provenance(msg: &mut MailMessage, rec: &MirrorIndexRecord) {
    msg.path = PathBuf::from(&rec.path);
    msg.to = rec.to.clone();
    msg.cc = rec.cc.clone();
    msg.created_at = UnixMs(rec.created_at_ms);
    msg.created_at_iso = msg.created_at.to_rfc3339_sec();
    msg.origin = rec.origin;
    msg.room = derive_mail_room(
        &msg.from,
        &msg.to,
        &msg.cc,
        msg.topic.as_deref(),
        &msg.subject,
        &msg.sha256,
    );
}

/// Pre-scanned disk snapshot collected outside `MailBridge`'s writer mutex.
pub struct DiskReconcileInputs {
    pub disk_acks: Vec<AckRecord>,
    pub disk_index: Vec<MirrorIndexRecord>,
    pub disk_blobs: Vec<(Sha256Hex, Vec<u8>, Option<UnixMs>)>,
    pub present_blob_shas: HashSet<Sha256Hex>,
    pub disk_working: Vec<DiskMailFile>,
    pub present_working_paths: HashSet<String>,
    pub drop_files: Vec<DiskMailFile>,
}

impl DiskReconcileInputs {
    pub fn collect(
        mirror: &MirrorTarget,
        aliases: &AliasMap,
        known_shas: &HashSet<Sha256Hex>,
        export_enabled: bool,
        cache: &mut DiskScanCache,
    ) -> Result<Self, ApiError> {
        let disk_acks = mirror.read_acks(aliases)?;
        let disk_index = mirror.read_mail_index()?;
        let (disk_blobs, present_blob_shas) =
            scan_object_blobs_cached(mirror.path(), known_shas, cache)?;
        let (disk_working, present_working_paths) =
            scan_working_tree_cached(mirror.path(), aliases, known_shas, cache)?;
        let drop_files = scan_and_claim_drop_dir(mirror.drop_dir(), aliases, export_enabled)?;
        Ok(Self {
            disk_acks,
            disk_index,
            disk_blobs,
            present_blob_shas,
            disk_working,
            present_working_paths,
            drop_files,
        })
    }
}

/// Pending side effects (room projections + mirror exports) returned after the
/// fast SQLite commit phase so the writer mutex is never held across room actors
/// or filesystem exports.
pub struct PendingReconcileWork {
    pub report: MailReconcileReport,
    pub fresh_envelopes: Vec<Envelope>,
    pub missing_room_msgs: Vec<(MailMessage, Option<Principal>)>,
    pub blobs_to_export: Vec<(Sha256Hex, Vec<u8>)>,
    pub wt_to_export: Vec<(PathBuf, Vec<u8>)>,
    pub index_to_export: Vec<MirrorIndexRecord>,
    pub acks_to_export: Vec<CommittedAckRecord>,
    pub drop_files_to_finish: Vec<(DiskMailFile, MailMessage)>,
}

/// Commits all disk ingests into `StreamId::Mail` and computes missing room
/// projections and mirror exports. Publishes each `Appended::Fresh` batch onto
/// `hub` immediately on SQLite commit so subsequent export errors never drop
/// live notifications for already-committed rows.
pub fn reconcile_commit_phase(
    state: &mut MailState,
    writer: &mut StreamWriter,
    inputs: DiskReconcileInputs,
    aliases: &AliasMap,
    export_enabled: bool,
    now: UnixMs,
    hub: Option<&Hub>,
) -> Result<PendingReconcileWork, ApiError> {
    let mut report = MailReconcileReport::default();
    let mut fresh_envelopes = Vec::new();
    let sys_by = Principal::System {
        part: SystemPart::MailBridge,
    };
    let publish_batch = |envs: &[Envelope], acc: &mut Vec<Envelope>| {
        acc.extend_from_slice(envs);
        if let Some(h) = hub {
            h.publish(envs);
        }
    };

    let mut index_by_sha: BTreeMap<Sha256Hex, MirrorIndexRecord> = BTreeMap::new();
    let mut disk_index_keys: HashSet<(Sha256Hex, Option<IdemKey>)> = HashSet::new();
    for rec in inputs.disk_index {
        disk_index_keys.insert((rec.sha256.clone(), rec.send_idem.clone()));
        index_by_sha
            .entry(rec.sha256.clone())
            .and_modify(|existing| {
                if existing.send_idem.is_none() && rec.send_idem.is_some() {
                    *existing = rec.clone();
                }
            })
            .or_insert(rec);
    }

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
    for sha in state.messages.keys().chain(inputs.present_blob_shas.iter()) {
        register_sha(sha);
    }
    for (sha, _, _) in &inputs.disk_blobs {
        register_sha(sha);
    }
    for f in inputs.disk_working.iter().chain(inputs.drop_files.iter()) {
        register_sha(&f.sha256);
    }

    // 1. Ingest mirror state/acks.jsonl -> SQLite (preserving at_ms and note).
    let mut disk_ack_keys: HashSet<(SeatName, Sha256Hex)> = HashSet::new();
    let mut path_hint_by_sha: BTreeMap<Sha256Hex, String> = BTreeMap::new();
    for rec in &inputs.disk_acks {
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
        disk_ack_keys.insert((seat.clone(), sha.clone()));
        let rel_path = rec
            .path
            .as_ref()
            .map(|p| p.canonical.clone())
            .or_else(|| index_by_sha.get(&sha).map(|i| i.path.clone()))
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
            .at_ms
            .or_else(|| {
                rec.ts
                    .as_deref()
                    .and_then(crate::shadow::parse_recorded_datetime_ms)
            })
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
                    note: rec.note.clone(),
                })],
            )
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        if let Appended::Fresh(envs) = appended {
            report.acks_ingested += 1;
            for e in &envs {
                state.apply(e);
            }
            publish_batch(&envs, &mut fresh_envelopes);
        }
    }

    // 2. Ingest unindexed working-tree files from <mirror>.
    for f in &inputs.disk_working {
        if state.messages.contains_key(&f.sha256) {
            continue;
        }
        let idx_rec = index_by_sha.get(&f.sha256);
        let eff_path = idx_rec
            .map(|r| r.path.as_str())
            .unwrap_or(f.rel_path.as_str());
        let (mutated_after_ack, prior_ack_sha) = match idx_rec {
            Some(r) => (r.mutated_after_ack, r.prior_ack_sha.clone()),
            None => {
                let prior = state.is_edit_after_ack(eff_path, &f.sha256);
                (prior.is_some(), prior)
            }
        };
        let mut msg = parse_mail_message_from_bytes(
            eff_path,
            &f.raw_bytes,
            f.box_seat.as_ref(),
            &[],
            f.mtime_ms,
            aliases,
            MailOrigin::Mirror,
        );
        if let Some(r) = idx_rec {
            apply_index_provenance(&mut msg, r);
        }
        let send_idem = idx_rec.and_then(|r| r.send_idem.clone());
        let by = idx_rec.and_then(|r| r.send_by.as_ref()).unwrap_or(&sys_by);
        let idem = match &send_idem {
            Some(k) => k.clone(),
            None => IdemKey::parse(format!("mail_msg:{}", msg.sha256))?,
        };
        let appended = writer
            .append(
                by,
                msg.created_at,
                Some(&idem),
                vec![Event::Mail(MailEvent::MessageCommitted {
                    message: Box::new(msg),
                    mutated_after_ack,
                    prior_ack_sha,
                    send_idem,
                })],
            )
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        if let Appended::Fresh(envs) = appended {
            report.files_ingested += 1;
            if mutated_after_ack {
                report.mutated_after_ack_flagged += 1;
            }
            for e in &envs {
                state.apply(e);
            }
            publish_batch(&envs, &mut fresh_envelopes);
        }
    }

    // 3. Ingest unindexed immutable blobs from <mirror>/objects/<sha256>.md.
    for (sha, raw_bytes, mtime_ms) in &inputs.disk_blobs {
        if state.messages.contains_key(sha) {
            continue;
        }
        let idx_rec = index_by_sha.get(sha);
        let hinted_path = idx_rec
            .map(|r| r.path.clone())
            .or_else(|| path_hint_by_sha.get(sha).cloned())
            .unwrap_or_else(|| {
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
        let (mutated_after_ack, prior_ack_sha) = match idx_rec {
            Some(r) => (r.mutated_after_ack, r.prior_ack_sha.clone()),
            None => {
                let prior = state.is_edit_after_ack(&hinted_path, sha);
                (prior.is_some(), prior)
            }
        };
        let mut msg = parse_mail_message_from_bytes(
            &hinted_path,
            raw_bytes,
            None,
            &[],
            *mtime_ms,
            aliases,
            MailOrigin::Mirror,
        );
        if let Some(r) = idx_rec {
            apply_index_provenance(&mut msg, r);
        }
        let send_idem = idx_rec.and_then(|r| r.send_idem.clone());
        let by = idx_rec.and_then(|r| r.send_by.as_ref()).unwrap_or(&sys_by);
        let idem = match &send_idem {
            Some(k) => k.clone(),
            None => IdemKey::parse(format!("mail_msg:{}", msg.sha256))?,
        };
        let appended = writer
            .append(
                by,
                msg.created_at,
                Some(&idem),
                vec![Event::Mail(MailEvent::MessageCommitted {
                    message: Box::new(msg),
                    mutated_after_ack,
                    prior_ack_sha,
                    send_idem,
                })],
            )
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        if let Appended::Fresh(envs) = appended {
            report.files_ingested += 1;
            if mutated_after_ack {
                report.mutated_after_ack_flagged += 1;
            }
            for e in &envs {
                state.apply(e);
            }
            publish_batch(&envs, &mut fresh_envelopes);
        }
    }

    // 4. Ingest files from separate <drop_dir> -> commit to SQLite FIRST.
    let mut drop_files_to_finish = Vec::new();
    for f in inputs.drop_files {
        let msg = if let Some(existing) = state.messages.get(&f.sha256) {
            existing.message.clone()
        } else {
            let prior_ack_sha = state.is_edit_after_ack(&f.rel_path, &f.sha256);
            let mutated_after_ack = prior_ack_sha.is_some();
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
                        send_idem: None,
                    })],
                )
                .map_err(|e| ApiError::Internal(e.to_string()))?;
            if let Appended::Fresh(envs) = appended {
                report.files_ingested += 1;
                if mutated_after_ack {
                    report.mutated_after_ack_flagged += 1;
                }
                for e in &envs {
                    state.apply(e);
                }
                publish_batch(&envs, &mut fresh_envelopes);
            }
            parsed
        };
        if export_enabled {
            drop_files_to_finish.push((f, msg));
        }
    }

    // 5. Collect ALL committed non-flagged messages missing a durable room post
    // (covering both newly ingested files AND crash recovery between SQLite commit and RoomPost).
    let mut missing_entries: Vec<&CommittedMailEntry> = state
        .messages
        .values()
        .filter(|e| {
            !e.mutated_after_ack
                && e.message.room.as_ref().is_some_and(|room| {
                    !state
                        .room_post_seqs
                        .contains_key(&(room.clone(), e.message.sha256.clone()))
                })
        })
        .collect();
    missing_entries
        .sort_by(|a, b| (a.message.created_at, a.seq).cmp(&(b.message.created_at, b.seq)));
    let missing_room_msgs: Vec<(MailMessage, Option<Principal>)> = missing_entries
        .into_iter()
        .map(|e| {
            let auth = e.send_idem.as_ref().map(|_| e.committed_by.clone());
            (e.message.clone(), auth)
        })
        .collect();

    let mut blobs_to_export = Vec::new();
    let mut wt_to_export = Vec::new();
    let mut index_to_export = Vec::new();
    let mut acks_to_export = Vec::new();

    if export_enabled {
        let mut present_blobs = inputs.present_blob_shas;
        let mut present_wt = inputs.present_working_paths;
        for entry in state.messages.values() {
            let msg = &entry.message;
            let need_blob = present_blobs.insert(msg.sha256.clone());
            let norm_wt = normalize_letter_path(&msg.path.to_string_lossy()).map(|n| n.canonical);
            let need_wt = !msg.path.starts_with("objects")
                && norm_wt
                    .as_ref()
                    .is_some_and(|canon| present_wt.insert(canon.clone()));
            if need_blob || need_wt {
                let raw = message_raw_bytes(msg)?;
                if need_blob {
                    blobs_to_export.push((msg.sha256.clone(), raw.clone()));
                }
                if need_wt {
                    wt_to_export.push((msg.path.clone(), raw));
                }
            }
            let idx_rec = MirrorIndexRecord::from_committed(entry);
            if !disk_index_keys.contains(&(idx_rec.sha256.clone(), idx_rec.send_idem.clone())) {
                index_to_export.push(idx_rec);
            }
        }
        for ack in state.ack_records.values() {
            if !disk_ack_keys.contains(&(ack.seat.clone(), ack.sha256.clone())) {
                acks_to_export.push(ack.clone());
            }
        }
    }

    Ok(PendingReconcileWork {
        report,
        fresh_envelopes,
        missing_room_msgs,
        blobs_to_export,
        wt_to_export,
        index_to_export,
        acks_to_export,
        drop_files_to_finish,
    })
}

/// Executes pending mirror exports and drop-file completions outside the writer mutex.
pub fn execute_reconcile_exports(
    mirror: &MirrorTarget,
    work: &mut PendingReconcileWork,
) -> Result<(), ApiError> {
    for (sha, raw) in &work.blobs_to_export {
        if mirror.export_blob_if_absent(sha, raw)? {
            work.report.blobs_exported += 1;
        }
    }
    for (path, raw) in &work.wt_to_export {
        if mirror.export_working_tree_if_absent(path, raw)? {
            work.report.working_tree_exported += 1;
        }
    }
    if !work.index_to_export.is_empty() {
        mirror.append_index_batch_if_absent(&work.index_to_export)?;
    }
    if !work.acks_to_export.is_empty() {
        work.report.acks_exported += mirror.append_acks_batch_if_absent(&work.acks_to_export)?;
    }
    for (drop_file, msg) in &work.drop_files_to_finish {
        if mirror.export_blob_if_absent(&msg.sha256, &drop_file.raw_bytes)? {
            work.report.blobs_exported += 1;
        }
        if mirror.export_working_tree_if_absent(&msg.path, &drop_file.raw_bytes)? {
            work.report.working_tree_exported += 1;
        }
        if finish_claimed_drop_file(drop_file) {
            work.report.drop_files_consumed += 1;
        }
    }
    Ok(())
}

/// Runs a complete `MailStore.tla` `Reconciled` pass between `journal`/`state`
/// and `mirror` (+ optional `drop_dir`).
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
    let known_shas: HashSet<Sha256Hex> = state.messages.keys().cloned().collect();
    let mut cache = DiskScanCache::default();
    let inputs =
        DiskReconcileInputs::collect(mirror, aliases, &known_shas, export_enabled, &mut cache)?;
    let mut work =
        reconcile_commit_phase(state, writer, inputs, aliases, export_enabled, now, None)?;
    if let Some(rsvc) = room_svc {
        for (msg, _) in &work.missing_room_msgs {
            if let Some(ref room) = msg.room {
                let parent_seq = msg
                    .in_reply_to
                    .as_deref()
                    .and_then(|p| state.resolve_parent_room_seq(room, p));
                if let Some(seq) =
                    project_message_to_room_service(rsvc, msg, parent_seq, None).await
                {
                    state
                        .room_post_seqs
                        .insert((room.clone(), msg.sha256.clone()), seq);
                }
            }
        }
    }
    if export_enabled {
        execute_reconcile_exports(mirror, &mut work)?;
    }
    Ok((work.report, work.fresh_envelopes))
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
    let index_records = match rex_wave_fs::read_regular_bounded(
        &ro.root().join("state").join("mail-index.jsonl"),
        MAX_INDEX_FILE_BYTES,
    ) {
        Ok(bytes) => parse_mail_index_jsonl(&String::from_utf8_lossy(&bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(SandboxError::Io(format!("read mail-index.jsonl: {e}"))),
    };
    let mut index_by_sha: BTreeMap<Sha256Hex, MirrorIndexRecord> = BTreeMap::new();
    let mut extra_idem_records: Vec<MirrorIndexRecord> = Vec::new();
    for rec in index_records {
        match index_by_sha.get_mut(&rec.sha256) {
            Some(existing) => {
                if existing.send_idem.is_none() && rec.send_idem.is_some() {
                    *existing = rec;
                } else if rec.send_idem.is_some() && rec.send_idem != existing.send_idem {
                    extra_idem_records.push(rec);
                }
            }
            None => {
                index_by_sha.insert(rec.sha256.clone(), rec);
            }
        }
    }

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

    // Resolve all ack records from state/acks.jsonl into canonical (seat, sha256, path, ts, note).
    let mut resolved_acks: BTreeMap<(SeatName, Sha256Hex), (String, UnixMs, Option<String>)> =
        BTreeMap::new();
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
            .or_else(|| index_by_sha.get(&sha).map(|i| i.path.clone()))
            .or_else(|| by_sha.get(&sha).map(|c| c.rel_path.clone()))
            .unwrap_or_else(|| format!("objects/{}.md", sha.as_str()));
        ack_path_by_sha
            .entry(sha.clone())
            .or_insert_with(|| rel.clone());
        let ack_at = rec
            .at_ms
            .or_else(|| {
                rec.ts
                    .as_deref()
                    .and_then(crate::shadow::parse_recorded_datetime_ms)
            })
            .unwrap_or(UnixMs(0));
        resolved_acks
            .entry((seat, sha))
            .or_insert_with(|| (rel, ack_at, rec.note.clone()));
    }

    // Merge blobs that are no longer at their working-tree path (e.g. pre-edit historical blobs).
    for (sha, raw_bytes, mtime_ms) in blobs {
        by_sha.entry(sha.clone()).or_insert_with(|| {
            let rel_path = index_by_sha
                .get(&sha)
                .map(|i| i.path.clone())
                .or_else(|| ack_path_by_sha.get(&sha).cloned())
                .unwrap_or_else(|| {
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

    let canonical_ack_records: Vec<AckRecord> = resolved_acks
        .iter()
        .enumerate()
        .map(|(i, ((seat, sha), (rel, at_ms, note)))| AckRecord {
            line_number: i + 1,
            seat: Some(seat.as_str().to_owned()),
            raw_seat: Some(seat.as_str().to_owned()),
            path: normalize_letter_path(rel),
            sha256_full: Some(sha.clone()),
            sha16: Some(sha.as_str()[..16].to_owned()),
            ts: None,
            at_ms: Some(*at_ms),
            note: note.clone(),
        })
        .collect();
    let canonical_ack_index = AckIndex::from_records(canonical_ack_records);
    let acked_shas: BTreeSet<Sha256Hex> =
        resolved_acks.keys().map(|(_, sha)| sha.clone()).collect();

    struct RebuiltMessageRow {
        msg: MailMessage,
        mutated_after_ack: bool,
        prior_ack_sha: Option<String>,
        send_idem: Option<IdemKey>,
        send_by: Option<Principal>,
    }

    let mut parsed_messages: Vec<RebuiltMessageRow> = Vec::with_capacity(by_sha.len());
    let mut msg_by_sha_for_extra: BTreeMap<Sha256Hex, MailMessage> = BTreeMap::new();
    let mut send_authors: BTreeMap<Sha256Hex, Principal> = BTreeMap::new();

    for (cand_sha, cand) in by_sha {
        let idx_rec = index_by_sha.get(&cand_sha);
        let eff_path = idx_rec
            .map(|r| r.path.as_str())
            .unwrap_or(cand.rel_path.as_str());
        let mut msg = parse_mail_message_from_bytes(
            eff_path,
            &cand.raw_bytes,
            cand.box_seat.as_ref(),
            &cand.extra_recipients,
            cand.mtime_ms,
            aliases,
            MailOrigin::Mirror,
        );
        if let Some(r) = idx_rec {
            apply_index_provenance(&mut msg, r);
            if let Some(ref author) = r.send_by {
                send_authors.insert(msg.sha256.clone(), author.clone());
            }
        }
        let (mutated_after_ack, prior_ack_sha) = match idx_rec {
            Some(r) => (r.mutated_after_ack, r.prior_ack_sha.clone()),
            None => {
                let prior = if acked_shas.contains(&msg.sha256) {
                    None
                } else {
                    canonical_ack_index.mutated_after_ack("", eff_path, &msg.sha256, aliases)
                };
                (prior.is_some(), prior)
            }
        };
        msg_by_sha_for_extra.insert(msg.sha256.clone(), msg.clone());
        parsed_messages.push(RebuiltMessageRow {
            msg,
            mutated_after_ack,
            prior_ack_sha,
            send_idem: idx_rec.and_then(|r| r.send_idem.clone()),
            send_by: idx_rec.and_then(|r| r.send_by.clone()),
        });
    }
    parsed_messages
        .sort_by(|a, b| (a.msg.created_at, &a.msg.sha256).cmp(&(b.msg.created_at, &b.msg.sha256)));

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
    for row in &parsed_messages {
        let idem = match &row.send_idem {
            Some(k) => k.clone(),
            None => IdemKey::parse(format!("mail_msg:{}", row.msg.sha256))
                .map_err(|e| SandboxError::Io(e.to_string()))?,
        };
        let by = row.send_by.as_ref().unwrap_or(&sys_by);
        let appended = mail_writer
            .append(
                by,
                row.msg.created_at,
                Some(&idem),
                vec![Event::Mail(MailEvent::MessageCommitted {
                    message: Box::new(row.msg.clone()),
                    mutated_after_ack: row.mutated_after_ack,
                    prior_ack_sha: row.prior_ack_sha.clone(),
                    send_idem: row.send_idem.clone(),
                })],
            )
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        if matches!(appended, Appended::Fresh(_)) {
            messages_rebuilt += 1;
            if row.mutated_after_ack {
                mutated_after_ack_flagged += 1;
            }
        }
        if !row.mutated_after_ack {
            non_flagged_for_rooms.push(row.msg.clone());
        }
    }

    for extra in &extra_idem_records {
        if let Some(ref sidem) = extra.send_idem
            && let Some(msg) = msg_by_sha_for_extra.get(&extra.sha256)
        {
            let by = extra.send_by.as_ref().unwrap_or(&sys_by);
            let _ = mail_writer
                .append(
                    by,
                    msg.created_at,
                    Some(sidem),
                    vec![Event::Mail(MailEvent::MessageCommitted {
                        message: Box::new(msg.clone()),
                        mutated_after_ack: extra.mutated_after_ack,
                        prior_ack_sha: extra.prior_ack_sha.clone(),
                        send_idem: Some(sidem.clone()),
                    })],
                )
                .map_err(|e| SandboxError::Io(e.to_string()))?;
        }
    }

    let mut acks_rebuilt = 0usize;
    for ((seat, sha), (rel, ack_at, note)) in &resolved_acks {
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
                    note: note.clone(),
                })],
            )
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        if matches!(appended, Appended::Fresh(_)) {
            acks_rebuilt += 1;
        }
    }
    drop(mail_writer);

    let (dm_rooms_projected, thread_channels_projected) =
        project_messages_to_journal_rooms_with_authors(
            &journal,
            &non_flagged_for_rooms,
            &send_authors,
        )
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

fn create_exclusive_verify_subdir(base: &Path) -> Result<PathBuf, SandboxError> {
    use std::os::unix::fs::DirBuilderExt as _;
    for attempt in 0u64..128 {
        let nonce = DROP_CLAIM_NONCE.fetch_add(1, Ordering::Relaxed);
        let candidate = base.join(format!(
            "verify-{}-{nonce:08x}-{attempt}",
            std::process::id()
        ));
        match fs::DirBuilder::new().mode(0o700).create(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(SandboxError::Io(e.to_string())),
        }
    }
    Err(SandboxError::Io(
        "exhausted exclusive migrate-verify run directory names".into(),
    ))
}

struct ExclusiveDirGuard(PathBuf);

impl Drop for ExclusiveDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CanonicalMailProjection {
    id: String,
    from: String,
    to: Vec<SeatName>,
    cc: Vec<SeatName>,
    subject: String,
    body: String,
    path: PathBuf,
    priority: LetterPriority,
    topic: Option<String>,
    in_reply_to: Option<String>,
    refs: Vec<String>,
    created_at: UnixMs,
    receipt_only: bool,
    room: Option<RoomId>,
    mutated_after_ack: bool,
    prior_ack_sha: Option<String>,
}

fn canonical_mail_projections(state: &MailState) -> BTreeMap<Sha256Hex, CanonicalMailProjection> {
    state
        .messages
        .iter()
        .map(|(sha, e)| {
            let m = &e.message;
            (
                sha.clone(),
                CanonicalMailProjection {
                    id: m.id.clone(),
                    from: m.from.clone(),
                    to: m.to.clone(),
                    cc: m.cc.clone(),
                    subject: m.subject.clone(),
                    body: m.body.clone(),
                    path: m.path.clone(),
                    priority: m.priority,
                    topic: m.topic.clone(),
                    in_reply_to: m.in_reply_to.clone(),
                    refs: m.refs.clone(),
                    created_at: m.created_at,
                    receipt_only: m.receipt_only,
                    room: m.room.clone(),
                    mutated_after_ack: e.mutated_after_ack,
                    prior_ack_sha: e.prior_ack_sha.clone(),
                },
            )
        })
        .collect()
}

type CanonicalAckSnapshot = BTreeMap<(SeatName, Sha256Hex), (PathBuf, Option<String>, UnixMs)>;

fn canonical_ack_snapshot(state: &MailState) -> CanonicalAckSnapshot {
    state
        .ack_records
        .iter()
        .map(|(k, rec)| (k.clone(), (rec.path.clone(), rec.note.clone(), rec.at)))
        .collect()
}

type RoomPostSnapshot = BTreeMap<
    RoomId,
    (
        BTreeSet<Principal>,
        Vec<(Principal, String, Option<Seq>, Vec<Label>)>,
    ),
>;

fn snapshot_all_rooms(journal: &Journal) -> Result<RoomPostSnapshot, SandboxError> {
    let mut out = BTreeMap::new();
    for (stream, _) in journal
        .streams()
        .map_err(|e| SandboxError::Io(e.to_string()))?
    {
        let StreamId::Room { room } = stream.clone() else {
            continue;
        };
        let mut members = BTreeSet::new();
        let mut posts = Vec::new();
        let mut after = Seq::ORIGIN;
        loop {
            let page = journal
                .read(&stream, after, 512)
                .map_err(|e| SandboxError::Io(e.to_string()))?;
            if page.is_empty() {
                break;
            }
            for env in page {
                after = env.seq;
                match env.event {
                    Event::Room(RoomEvent::MemberJoined { member, .. }) => {
                        members.insert(member);
                    }
                    Event::Room(RoomEvent::MemberLeft { member }) => {
                        members.remove(&member);
                    }
                    Event::Room(RoomEvent::Posted {
                        body, re, labels, ..
                    }) => {
                        posts.push((env.by, body.as_str().to_owned(), re, labels));
                    }
                    _ => {}
                }
            }
        }
        out.insert(room, (members, posts));
    }
    Ok(out)
}

/// Runs the strictly read-only migration + delete-journal + lossless-rebuild
/// verification gate over `interchange_dir`, writing ONLY inside an exclusive
/// scratch subdirectory beneath `work_dir` (verified outside `protected-roots`
/// AND outside `interchange_dir` BEFORE creating any directory).
pub fn migrate_verify_interchange(
    interchange_dir: &Path,
    work_dir: &Path,
    aliases: &AliasMap,
    protection: &Protection,
) -> Result<MailMigrateVerifyReport, SandboxError> {
    let ro = ReadOnlyInterchange::open(interchange_dir)?;

    // Strictly verify work_dir is outside BOTH protected-roots AND interchange_dir BEFORE mkdir!
    let guard = ProtectedRootsGuard::resolve(protection)?;
    let lex_work = rex_wave_fs::lexical_normalize(work_dir)?;
    guard.check(&lex_work, work_dir)?;
    let existing_prefix = rex_wave_fs::canonicalize_existing_prefix(&lex_work)?;
    guard.check(&existing_prefix, work_dir)?;
    let resolved_work = rex_wave_fs::resolve_path_preserving_symlinks(&lex_work)
        .unwrap_or_else(|_| lex_work.clone());
    if resolved_work.starts_with(ro.root())
        || ro.root().starts_with(&resolved_work)
        || lex_work.starts_with(ro.root())
        || ro.root().starts_with(&lex_work)
    {
        return Err(SandboxError::Production(work_dir.to_path_buf()));
    }

    let verified_work = verify_or_create_dir_outside(&guard, work_dir)?;
    OutsidePath::verify(&verified_work.join("probe"), ro.root())
        .map_err(|e| SandboxError::Production(PathBuf::from(e.to_string())))?;

    // Create an exclusively owned per-run scratch directory so pre-existing files
    // in `--work-dir` (such as an existing `journal.sqlite3`) are never touched or unlinked.
    let run_dir = create_exclusive_verify_subdir(&verified_work)?;
    let _run_cleanup = ExclusiveDirGuard(run_dir.clone());

    let mirror_dir = run_dir.join("mirror");
    let journal1_path = run_dir.join("journal.sqlite3");
    let journal2_path = run_dir.join("journal-rebuilt.sqlite3");

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
        all_paths: Vec<String>,
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
                if !existing.all_paths.contains(&letter.rel_path) {
                    existing.all_paths.push(letter.rel_path.clone());
                }
                let prefer_new =
                    letter.rel_path.starts_with("mail/") && !existing.rel_path.starts_with("mail/");
                if prefer_new {
                    if let Some(prev_seat) = existing.box_seat.take()
                        && !existing.extra_recipients.contains(&prev_seat)
                    {
                        existing.extra_recipients.push(prev_seat);
                    }
                    existing.rel_path = letter.rel_path.clone();
                    existing.box_seat = box_seat.clone();
                } else if let Some(ref seat) = box_seat
                    && !existing.extra_recipients.contains(seat)
                {
                    existing.extra_recipients.push(seat.clone());
                }
                for r in &letter.recipients {
                    if !existing.extra_recipients.contains(r) {
                        existing.extra_recipients.push(r.clone());
                    }
                }
            })
            .or_insert_with(|| SourceEntry {
                all_paths: vec![letter.rel_path.clone()],
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

    let mut resolved_acks: BTreeMap<(SeatName, Sha256Hex), (String, UnixMs, Option<String>)> =
        BTreeMap::new();
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
            .at_ms
            .or_else(|| {
                rec.ts
                    .as_deref()
                    .and_then(crate::shadow::parse_recorded_datetime_ms)
            })
            .unwrap_or(UnixMs(0));
        resolved_acks
            .entry((seat, sha))
            .or_insert_with(|| (rel, ack_at, rec.note.clone()));
    }

    let canonical_ack_records: Vec<AckRecord> = resolved_acks
        .iter()
        .enumerate()
        .map(|(i, ((seat, sha), (rel, at_ms, note)))| AckRecord {
            line_number: i + 1,
            seat: Some(seat.as_str().to_owned()),
            raw_seat: Some(seat.as_str().to_owned()),
            path: normalize_letter_path(rel),
            sha256_full: Some(sha.clone()),
            sha16: Some(sha.as_str()[..16].to_owned()),
            ts: None,
            at_ms: Some(*at_ms),
            note: note.clone(),
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
    let mut index_jsonl_buf = String::new();
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
                    prior_ack_sha: prior_ack_sha.clone(),
                    send_idem: None,
                })],
            )
            .map_err(|e| SandboxError::Io(e.to_string()))?;

        // Export immutable blob + canonical working-tree file + any additional box paths.
        mirror
            .export_blob_if_absent(&msg.sha256, &src.raw_bytes)
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        let wt_created = mirror
            .export_working_tree_if_absent(&msg.path, &src.raw_bytes)
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        if wt_created {
            expected_working_paths.push((msg.path.clone(), msg.sha256.clone()));
        }
        for extra_p in &src.all_paths {
            if extra_p != &src.rel_path {
                let _ = mirror.export_working_tree_if_absent(Path::new(extra_p), &src.raw_bytes);
            }
        }

        let norm_path = normalize_letter_path(&msg.path.to_string_lossy())
            .map(|n| n.canonical)
            .unwrap_or_else(|| msg.path.to_string_lossy().into_owned());
        let idx_rec = MirrorIndexRecord {
            schema: 1,
            sha256: msg.sha256.clone(),
            path: norm_path,
            to: msg.to.clone(),
            cc: msg.cc.clone(),
            created_at_ms: msg.created_at.0,
            mutated_after_ack,
            prior_ack_sha,
            origin: msg.origin,
            send_idem: None,
            send_by: None,
        };
        let idx_line =
            serde_json::to_string(&idx_rec).map_err(|e| SandboxError::Io(e.to_string()))?;
        index_jsonl_buf.push_str(&idx_line);
        index_jsonl_buf.push('\n');

        if !mutated_after_ack {
            imported_msgs_for_rooms.push(msg);
        }
    }

    let mut acks_jsonl_buf = String::new();
    for ((seat, sha), (rel, ack_at, note)) in &resolved_acks {
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
                    note: note.clone(),
                })],
            )
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        let mut line = serde_json::json!({
            "schema": 1,
            "ts": ack_at.to_rfc3339_sec(),
            "at_ms": ack_at.0,
            "letter": rel,
            "path": rel,
            "sha256": sha.as_str(),
            "sha256_full": sha.as_str(),
            "acked_by": seat.as_str(),
            "seat": seat.as_str(),
        });
        if let Some(n) = note.as_deref().map(str::trim).filter(|s| !s.is_empty())
            && let Some(map) = line.as_object_mut()
        {
            map.insert("note".into(), serde_json::Value::String(n.to_owned()));
        }
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
    rex_wave_fs::write_atomic_path(
        &state_dir.join("mail-index.jsonl"),
        index_jsonl_buf.as_bytes(),
        SANDBOX_FILE_MODE,
        true,
    )
    .map_err(|e| SandboxError::Io(e.to_string()))?;

    let (imported_dm_rooms, imported_thread_channels) =
        project_messages_to_journal_rooms(&journal1, &imported_msgs_for_rooms)
            .map_err(|e| SandboxError::Io(e.to_string()))?;

    // Verify 100% on-disk blob & non-colliding working-tree sha256 parity.
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

    // Snapshot full per-message projections, ack records, flags, and all room streams BEFORE deleting journal1.
    let state1 = MailState::from_journal(&journal1).map_err(|e| SandboxError::Io(e.to_string()))?;
    let db1 = state1.db_set();
    let proj1 = canonical_mail_projections(&state1);
    let acks1 = canonical_ack_snapshot(&state1);
    let flagged1 = state1.flagged_shas.clone();
    let rooms1 = snapshot_all_rooms(&journal1)?;
    drop(journal1);

    // 3. Delete journal1 (and WAL/SHM sidecars inside the exclusive run_dir), then rebuild from mirror alone!
    for suffix in ["", "-wal", "-shm"] {
        let mut p = journal1_path.as_os_str().to_os_string();
        p.push(suffix);
        let _ = fs::remove_file(PathBuf::from(p));
    }

    let rebuilt = rebuild_mail_from_mirror(mirror.path(), &journal2_path, aliases, protection)?;
    let journal2 = Journal::open(&journal2_path).map_err(|e| SandboxError::Io(e.to_string()))?;
    let state2 = MailState::from_journal(&journal2).map_err(|e| SandboxError::Io(e.to_string()))?;
    let db2 = state2.db_set();
    let proj2 = canonical_mail_projections(&state2);
    let acks2 = canonical_ack_snapshot(&state2);
    let flagged2 = state2.flagged_shas.clone();
    let rooms2 = snapshot_all_rooms(&journal2)?;

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
        && proj1 == proj2
        && blob_parity_count == unique_sha256_count
        && working_tree_parity_count == expected_working_paths.len();
    let ack_parity = acks1 == acks2;
    let flagged_parity = flagged1 == flagged2;
    let dm_room_parity = rooms1 == rooms2
        && imported_dm_rooms == rebuilt.dm_rooms_projected
        && imported_thread_channels == rebuilt.thread_channels_projected;
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
        imported_acks: acks1.len(),
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
