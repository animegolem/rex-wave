//! # rex-wave-mailbridge
//!
//! Non-production sandboxed canon mail service ([`SandboxRoot`], [`MailBridge`]),
//! `mailbox.item.schema.json` v1 projection ([`project`], [`wake_id`], [`bell_meta`]),
//! [`BellCourier`] scheduled wake delivery, `Family::Presence` decider
//! ([`presence::Presence`]), tolerant `acks.jsonl` parsing ([`acks`]),
//! structurally read-only shadow reactor ([`shadow`]), and Gate 2 scorer ([`score`]).

pub mod acks;
pub mod dissolved;
pub mod frontmatter;
pub mod presence;
pub mod score;
pub mod shadow;

pub use acks::{
    AckIndex, AckRecord, AcksParseReport, AliasMap, NormalizedLetterPath, SkippedAckLine,
    letter_paths_match, normalize_letter_path, normalize_seat_token, parse_acks_jsonl,
};
pub use dissolved::{
    AckTargetResolution, CommittedAckRecord, CommittedMailEntry, DiskMailFile, DiskReconcileInputs,
    DiskScanCache, MailMigrateVerifyReport, MailRebuildReport, MailState, MirrorIndexRecord,
    MirrorTarget, PendingReconcileWork, delivery_notices_for_seats, derive_mail_room,
    execute_reconcile_exports, finish_claimed_drop_file, format_mail_room_body, has_emacs_lock,
    is_emacs_lock_or_autosave, mail_room_labels, mail_snippet, message_raw_bytes,
    migrate_verify_interchange, parse_mail_index_jsonl, parse_mail_message_from_bytes,
    project_message_to_room_service, project_messages_to_journal_rooms,
    project_messages_to_journal_rooms_with_authors, rebuild_mail_from_mirror,
    reconcile_commit_phase, reconcile_mail_store, scan_and_claim_drop_dir, scan_object_blobs,
    scan_object_blobs_cached, scan_working_tree_cached, scan_working_tree_dir,
    synthesize_mail_message,
};
pub use frontmatter::{
    FrontmatterDoc, FrontmatterEntry, RecipientCandidate, parse_recipient_list, unquote_yaml,
    yaml_quote, yaml_quote_if_needed, yaml_quote_inline,
};
pub use presence::{
    ClaudeSessionFile, DEFAULT_LEASE_TTL_MS, DeliveryStatus, HOUSE_PROFILE_MAX_BYTES,
    IncarnationAffinity, Presence, PresenceState, SeedInterchangePlan, current_process_start,
    format_delivery_status, house_profile_path_from, is_process_alive, load_claude_session_file,
    load_claude_session_file_from, load_house_profile, load_house_profile_from,
    normalize_proc_start, parse_claude_session_json, proc_start_matches, query_process_start,
    seed_records_from_interchange_with_options, seed_records_from_interchange_with_profile,
    select_binding_for_delivery, sender_delivery_notice, try_query_process_start,
};
pub use score::{
    FixtureCoverageReport, GATE1_CASE_IDS, GATE1_CASE_TESTS, MailwatchComparisonReport,
    MailwatchSeenIndex, ShadowScoreReport, check_gate1_fixtures, check_gate1_fixtures_with_profile,
    compare_shadow_with_mailwatch_seen, extract_shadow_aliases, parse_mailwatch_seen_jsonl,
    parse_mailwatch_seen_sources, score_shadow_against_acks, score_shadow_jsonl,
};
pub use shadow::{
    DEFAULT_DIGEST_THRESHOLD, DEFAULT_RECONNECT_GRACE_MS, DEFAULT_RETRY_DEADLINE_MS, DeferKind,
    DeferReason, LetterCandidate, ParsedLetterTime, ReadOnlyInterchange, RetryEntry, RetryState,
    RingDecision, RingDeliveryChannel, RingPolicy, RingReferent, ScannedLetter, ShadowAction,
    ShadowPassReport, ShadowWatchTracker, TimeBasis, WATCH_TRACKER_MAX_ENTRIES, WatchDecisionState,
    WatchDelta, decide_mail_ring, decide_mail_rings_for_seat, extract_letter_timestamp,
    extract_letter_timestamp_detailed, parse_recorded_datetime_ms, run_shadow_pass,
};

use rex_wave_api::http_guard::is_loopback_host;
use rex_wave_api::mail::*;
use rex_wave_api::*;
use rex_wave_runtime::{BoxFut, DecideCtx, Hub, LocalClient, Reactor, Service, can_read_in};
use rex_wave_store::{Appended, Journal, StreamWriter};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};

use rex_wave_fs::{ConfinedDir, EntryKind, RelPath, SafeName};
pub use rex_wave_fs::{
    MAX_PROTECTED_ROOTS, ProtectedRootsGuard, Protection, SandboxError, load_protected_roots,
    load_protected_roots_from, parse_protected_roots, protected_roots_file, resolve_links,
};

pub(crate) const MARKER_FILE: &str = ".rex-wave-sandbox";
const MAX_PROMOTE_SPAN: u64 = 1_000;
const MAX_HTTP_HEADER: usize = 16 * 1024;
const MAX_HTTP_BODY: usize = 128 * 1024;
const MAX_SINK_CONNS: usize = 32;

/// The largest banked wake-bell record read.
const MAX_BELL_FILE_BYTES: u64 = 1024 * 1024;
/// The largest single JSONL line inside `state/acks.jsonl`.
const MAX_ACK_LINE_BYTES: usize = 1024 * 1024;
/// The largest letter read for projection or acknowledgement.
pub(crate) const MAX_LETTER_BYTES: u64 = 16 * 1024 * 1024;
/// Mode of letters, bells and acks written into the sandbox.
pub(crate) const SANDBOX_FILE_MODE: u32 = 0o644;
/// Mode of directories created in the sandbox.
pub(crate) const SANDBOX_DIR_MODE: u32 = 0o755;

#[derive(Clone, Debug)]
pub struct SandboxRoot {
    pub(crate) dir: ConfinedDir,
    pub(crate) guard: ProtectedRootsGuard,
}

impl SandboxRoot {
    /// Verify `path` against what this deployment's protected-roots file lists.
    pub fn verify(path: &Path) -> Result<Self, SandboxError> {
        Self::verify_with(path, &load_protected_roots()?)
    }

    /// Verify `path` against an explicit `Protection`.
    pub fn verify_with(path: &Path, protection: &Protection) -> Result<Self, SandboxError> {
        let guard = ProtectedRootsGuard::resolve(protection)?;
        let canon = guard.verify_dir(path)?;
        if rex_wave_fs::entry_kind(&canon.join(MARKER_FILE)).ok() != Some(EntryKind::File) {
            return Err(SandboxError::Unmarked(canon));
        }
        let dir = ConfinedDir::open(&canon).map_err(|e| SandboxError::Io(e.to_string()))?;
        Ok(SandboxRoot { dir, guard })
    }

    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    fn ensure_dir(&self, segments: &[&str]) -> Result<PathBuf, ApiError> {
        let names = segments
            .iter()
            .map(|seg| SafeName::parse(seg))
            .collect::<Result<Vec<_>, _>>()
            .and_then(RelPath::from_names)
            .map_err(|e| {
                ApiError::Invalid(InvalidId {
                    what: "sandbox directory".into(),
                    reason: format!("invalid directory segment: {e}"),
                })
            })?;
        let dir = self
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
        if self.guard.check(&dir, &dir).is_err() {
            return Err(ApiError::Denied {
                principal: "mailbridge".into(),
                action: format!("escape sandbox via {}", dir.display()),
            });
        }
        Ok(dir)
    }

    fn confine(&self, raw: &Path) -> Result<(PathBuf, String, bool), SandboxError> {
        self.guard.confine(self.dir.path(), raw)
    }

    fn read_state_string(&self, rel: &str, max_bytes: u64) -> Option<String> {
        let rel = RelPath::parse(rel).ok()?;
        self.dir.read_string(&rel, max_bytes).ok()
    }

    fn read_bell_prompt(&self, digest16: &str) -> Option<String> {
        let raw =
            self.read_state_string(&format!("state/bells/{digest16}.json"), MAX_BELL_FILE_BYTES)?;
        let val: serde_json::Value = serde_json::from_str(&raw).ok()?;
        val.get("prompt")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
    }
}

fn read_letter(path: &Path) -> Result<Vec<u8>, ApiError> {
    rex_wave_fs::read_regular_bounded(path, MAX_LETTER_BYTES).map_err(|e| {
        if e.kind() == std::io::ErrorKind::InvalidInput {
            ApiError::Denied {
                principal: "mailbridge".into(),
                action: format!("read {}: {e}", path.display()),
            }
        } else {
            ApiError::Internal(format!("read {}: {e}", path.display()))
        }
    })
}

pub(crate) fn sha256_bytes(bytes: &[u8]) -> Sha256Hex {
    let mut h = Sha256::new();
    h.update(bytes);
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    Sha256Hex::parse(hex).expect("64 lower-hex sha256")
}

pub(crate) fn sha256_short16(input: &str) -> String {
    let mut h = Sha256::new();
    h.update(input.as_bytes());
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    hex[..16].to_owned()
}

pub fn wake_id(seat: &SeatName, mailbox: &str, file: &str, full_sha256: &Sha256Hex) -> WakeId {
    let mut h = Sha256::new();
    h.update(format!("wake/1\n{seat}\n{mailbox}/{file}\n{full_sha256}").as_bytes());
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    WakeId(hex[..16].to_owned())
}

fn is_valid_address_segment(seg: &str) -> bool {
    let bytes = seg.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 40
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes.iter().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        })
}

fn is_valid_address(s: &str) -> bool {
    let mut count = 0usize;
    for seg in s.split('/') {
        count += 1;
        if count > 3 || !is_valid_address_segment(seg) {
            return false;
        }
    }
    count >= 1
}

fn is_valid_token(s: &str) -> bool {
    let bytes = s.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn is_valid_kind(s: &str) -> bool {
    let bytes = s.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 24
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
}

fn is_valid_bell_from(s: &str) -> bool {
    is_valid_address_segment(s)
}

fn is_valid_box(s: &str) -> bool {
    let Some(rest) = s.strip_prefix("to-") else {
        return false;
    };
    !rest.is_empty()
        && rest.len() <= 40
        && rest
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

fn is_valid_md_file(s: &str) -> bool {
    let Some(stem) = s.strip_suffix(".md") else {
        return false;
    };
    let bytes = stem.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 116
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn is_valid_ts(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 20
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T'
        && b[13] == b':'
        && b[16] == b':'
        && b[19] == b'Z'
        && b.iter()
            .enumerate()
            .all(|(i, c)| matches!(i, 4 | 7 | 10 | 13 | 16 | 19) || c.is_ascii_digit())
}

pub fn bell_meta(wake: &WakeRecord) -> BTreeMap<String, String> {
    let mut meta = BTreeMap::new();
    meta.insert("schema".into(), "1".into());
    let event = match wake.event.as_str() {
        "letter" | "changed" | "batch" | "backlog" | "selftest" | "wake" => wake.event.as_str(),
        _ => "wake",
    };
    meta.insert("event".into(), event.into());
    meta.insert(
        "ts".into(),
        if is_valid_ts(&wake.ts) {
            wake.ts.clone()
        } else {
            "1970-01-01T00:00:00Z".into()
        },
    );

    if matches!(event, "letter" | "changed") {
        if wake.wake_id.0.len() == 16
            && wake
                .wake_id
                .0
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            meta.insert("wake_id".into(), wake.wake_id.0.clone());
        }
        if is_valid_kind(&wake.item.kind) {
            meta.insert("kind".into(), wake.item.kind.clone());
        }
        let from_tok = wake.item.from.to_ascii_lowercase();
        meta.insert(
            "from".into(),
            if is_valid_bell_from(&from_tok) {
                from_tok
            } else {
                "?".into()
            },
        );

        let path_str = wake.item.reference.path.to_string_lossy();
        if let Some((box_part, file_part)) = path_str.rsplit_once('/') {
            let box_only = box_part.rsplit('/').next().unwrap_or(box_part);
            if is_valid_box(box_only) {
                meta.insert("box".into(), box_only.to_owned());
            }
            if is_valid_md_file(file_part) {
                meta.insert("file".into(), file_part.to_owned());
            }
        }
        meta.insert(
            "sha16".into(),
            wake.item.reference.sha256.as_str()[..16].to_owned(),
        );
        meta.insert(
            "topic".into(),
            match &wake.item.topic {
                Some(t) if is_valid_token(t) => t.clone(),
                _ => "?".into(),
            },
        );
        meta.insert(
            "round".into(),
            match wake.item.round {
                Some(r) if r <= 9_999 => r.to_string(),
                _ => "?".into(),
            },
        );
    } else if matches!(event, "batch" | "backlog") {
        meta.insert("count".into(), wake.count.max(1).to_string());
        if event == "backlog" {
            meta.insert("unknown".into(), wake.unknown.to_string());
        }
    }

    meta
}

fn principal_mail_address(p: &Principal) -> String {
    match p {
        Principal::Operator { seat } | Principal::Seat { seat } => seat.as_str().to_owned(),
        Principal::Worker { wave, role } => format!("{wave}/{role}"),
        Principal::System { part } => match part {
            SystemPart::WaveConductor => "system/wave_conductor".into(),
            SystemPart::Herald => "system/herald".into(),
            SystemPart::BbBridge => "system/bb_bridge".into(),
            SystemPart::T3Bridge => "system/t3_bridge".into(),
            SystemPart::MailBridge => "system/mail_bridge".into(),
            SystemPart::WakeBell => "system/wake_bell".into(),
            SystemPart::PresenceMonitor => "system/presence_monitor".into(),
            SystemPart::ConfigSync => "system/config_sync".into(),
        },
    }
}

fn priority_word(p: LetterPriority) -> &'static str {
    match p {
        LetterPriority::Low => "low",
        LetterPriority::Normal => "normal",
        LetterPriority::High => "high",
        LetterPriority::Blocking => "blocking",
    }
}

pub(crate) fn sanitize_topic_for_file(raw: &str) -> String {
    let mut out: String = raw
        .trim()
        .chars()
        .filter_map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                Some(c)
            } else if c.is_whitespace() {
                Some('-')
            } else {
                None
            }
        })
        .collect();
    while out.starts_with('.') || out.starts_with('-') {
        out.remove(0);
    }
    if out.is_empty() {
        out.push_str("letter");
    }
    out.truncate(64);
    out
}

fn room_provenance(room: &room::RoomId) -> String {
    match room {
        room::RoomId::Channel {
            scope: room::RoomScope::Global,
            name,
        } => format!("#{name}"),
        room::RoomId::Channel {
            scope: room::RoomScope::Project { project },
            name,
        } => format!("#{project}/{name}"),
        room::RoomId::Direct { pair } => {
            let (a, b): (Principal, Principal) = pair.clone().into();
            format!("dm:{a}<->{b}")
        }
    }
}

pub fn render_letter(req: &MailPromote, by: &Principal, entries: &[Envelope]) -> (String, Vec<u8>) {
    let topic = sanitize_topic_for_file(&req.topic);
    let round = 1u32;
    let file_name = format!("{topic}-r{round}.md");

    let from = principal_mail_address(by);
    let to_yaml = if req.to.len() == 1 {
        req.to[0].as_str().to_owned()
    } else {
        let joined: Vec<&str> = req.to.iter().map(SeatName::as_str).collect();
        format!("[{}]", joined.join(", "))
    };
    let room_label = room_provenance(&req.room);
    let subject_clean: String = req
        .subject
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .filter(|c| !c.is_control())
        .collect();

    let mut sorted: Vec<&Envelope> = entries.iter().collect();
    sorted.sort_by_key(|e| e.seq);

    let mut out = String::new();
    out.push_str("---\n");
    out.push_str(&format!("from: {from}\n"));
    out.push_str(&format!("to: {to_yaml}\n"));
    out.push_str(&format!(
        "subject: {}\n",
        yaml_quote_inline(subject_clean.trim())
    ));
    out.push_str(&format!("topic: {topic}\n"));
    out.push_str(&format!("round: {round}\n"));
    out.push_str(&format!("priority: {}\n", priority_word(req.priority)));
    out.push_str(&format!("room: {}\n", yaml_quote_inline(&room_label)));
    out.push_str(&format!(
        "range: [{}, {}]\n",
        req.range.first().0,
        req.range.last().0
    ));
    out.push_str("---\n\n");
    out.push_str(&format!("# {}\n\n", subject_clean.trim()));
    out.push_str(&format!(
        "Promoted from `{room_label}` (`seq {}..={}`).\n",
        req.range.first().0,
        req.range.last().0
    ));

    for env in sorted {
        if let Event::Room(room::RoomEvent::Posted { body, re, .. }) = &env.event {
            out.push('\n');
            match re {
                Some(r) => {
                    out.push_str(&format!("> **[#{} -> #{}] {}**:\n", env.seq.0, r.0, env.by))
                }
                None => out.push_str(&format!("> **[#{}] {}**:\n", env.seq.0, env.by)),
            }
            for line in body.as_str().lines() {
                if line.is_empty() {
                    out.push_str(">\n");
                } else {
                    out.push_str(&format!("> {line}\n"));
                }
            }
        }
    }

    (file_name, out.into_bytes())
}

#[derive(Default)]
struct Frontmatter {
    from: Option<String>,
    to: Option<String>,
    subject: Option<String>,
    topic: Option<String>,
    round: Option<u32>,
    priority: Option<String>,
    id: Option<String>,
    date_ms: Option<String>,
    date: Option<String>,
    ts: Option<String>,
    in_reply_to: Option<String>,
}

fn parse_frontmatter(text: &str) -> Frontmatter {
    let mut fm = Frontmatter::default();
    let entries = FrontmatterDoc::parse_headers_lenient(text);
    if entries.is_empty() {
        return fm;
    }

    for entry in entries {
        let key = entry.key.to_ascii_lowercase();
        let val = entry.value;
        if val.is_empty() {
            continue;
        }
        match key.as_str() {
            "from" | "author" => {
                if fm.from.is_none() || key == "from" {
                    fm.from = Some(val);
                }
            }
            "to" => {
                let first = val
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .split(',')
                    .next()
                    .unwrap_or("")
                    .trim();
                if !first.is_empty() {
                    fm.to = Some(unquote_yaml(first));
                }
            }
            "subject" => fm.subject = Some(val),
            "topic" => fm.topic = Some(val),
            "round" => fm.round = val.parse::<u32>().ok(),
            "priority" => fm.priority = Some(val.to_ascii_lowercase()),
            "id" => fm.id = Some(val),
            "date_ms" => fm.date_ms = Some(val),
            "date" => fm.date = Some(val),
            "ts" => fm.ts = Some(val),
            "in_reply_to" => fm.in_reply_to = Some(val),
            _ => {}
        }
    }
    fm
}

fn truncate_label(raw: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in raw.chars().filter(|c| !c.is_control()).enumerate() {
        if idx >= max_chars {
            break;
        }
        out.push(ch);
    }
    out
}

struct SandboxAckScan {
    matched: bool,
    needs_leading_newline: bool,
}

fn read_bounded_line<R: std::io::BufRead>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    max_bytes: usize,
) -> std::io::Result<Option<bool>> {
    buf.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok((!buf.is_empty()).then_some(false));
        }
        if let Some(nl) = available.iter().position(|&b| b == b'\n') {
            if buf.len().saturating_add(nl) > max_bytes {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("acks.jsonl line exceeds {max_bytes} bytes"),
                ));
            }
            buf.extend_from_slice(&available[..nl]);
            reader.consume(nl + 1);
            return Ok(Some(true));
        }
        if buf.len().saturating_add(available.len()) > max_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("acks.jsonl line exceeds {max_bytes} bytes"),
            ));
        }
        buf.extend_from_slice(available);
        let n = available.len();
        reader.consume(n);
    }
}

fn scan_sandbox_acks(
    root: &SandboxRoot,
    to_seat: &str,
    rel_path: &str,
    sha256: &Sha256Hex,
    strict_lines: bool,
) -> Result<SandboxAckScan, ApiError> {
    let rel = RelPath::parse("state/acks.jsonl")
        .map_err(|e| ApiError::Internal(format!("invalid acks relpath: {e}")))?;
    let (file, _) = match root.dir.open_regular(&rel) {
        Ok(pair) => pair,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(SandboxAckScan {
                matched: false,
                needs_leading_newline: false,
            });
        }
        Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => {
            return Err(ApiError::Denied {
                principal: "mailbridge".into(),
                action: format!("read non-regular acks.jsonl: {e}"),
            });
        }
        Err(e) => return Err(ApiError::Internal(format!("read acks.jsonl: {e}"))),
    };
    let mut reader = std::io::BufReader::new(file);
    let mut line_buf = Vec::new();
    let aliases = acks::AliasMap::empty();
    let mut matched = false;
    let mut any_bytes = false;
    let mut last_had_newline = true;
    let mut line_no = 0usize;
    while let Some(had_nl) = read_bounded_line(&mut reader, &mut line_buf, MAX_ACK_LINE_BYTES)
        .map_err(|e| ApiError::Internal(format!("read acks.jsonl: {e}")))?
    {
        line_no += 1;
        any_bytes = true;
        last_had_newline = had_nl;
        let line = match std::str::from_utf8(&line_buf) {
            Ok(s) => s,
            Err(_) if !strict_lines => continue,
            Err(_) => {
                return Err(ApiError::Internal(format!(
                    "corrupt non-UTF-8 row at state/acks.jsonl:{line_no}; repair or remove that line before appending new acks"
                )));
            }
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let val: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) if !strict_lines => continue,
            Err(e) => {
                return Err(ApiError::Internal(format!(
                    "corrupt JSON row at state/acks.jsonl:{line_no} ({e}); repair or remove that line before appending new acks"
                )));
            }
        };
        if !val.is_object() {
            if !strict_lines {
                continue;
            }
            return Err(ApiError::Internal(format!(
                "corrupt non-object JSON row at state/acks.jsonl:{line_no}; repair or remove that line before appending new acks"
            )));
        }
        if !matched && ack_row_matches(&val, to_seat, rel_path, sha256, &aliases) {
            matched = true;
        }
    }
    Ok(SandboxAckScan {
        matched,
        needs_leading_newline: any_bytes && !last_had_newline,
    })
}

fn is_acked_in_sandbox(
    root: &SandboxRoot,
    to_seat: &str,
    rel_path: &str,
    sha256: &Sha256Hex,
) -> Result<bool, ApiError> {
    Ok(scan_sandbox_acks(root, to_seat, rel_path, sha256, false)?.matched)
}

fn ack_row_matches(
    val: &serde_json::Value,
    to_seat: &str,
    rel_path: &str,
    sha256: &Sha256Hex,
    aliases: &acks::AliasMap,
) -> bool {
    acks::ack_value_matches(val, to_seat, rel_path, sha256, aliases)
}

pub fn project(root: &SandboxRoot, path: &Path, bytes: &[u8]) -> Result<MailboxItem, SandboxError> {
    let (abs_path, rel_path, exists) = root.confine(path)?;
    let sha256 = sha256_bytes(bytes);
    let text = String::from_utf8_lossy(bytes);
    let fm = parse_frontmatter(&text);

    let box_seat = rel_path
        .split_once('/')
        .and_then(|(first, _)| first.strip_prefix("to-"))
        .filter(|s| is_valid_address(s))
        .map(str::to_owned);

    let to = box_seat
        .or_else(|| {
            fm.to
                .as_deref()
                .map(|s| s.trim_start_matches('@').to_ascii_lowercase())
                .filter(|s| is_valid_address(s))
        })
        .unwrap_or_else(|| "unknown".into());

    let from = fm
        .from
        .as_deref()
        .map(|s| s.trim_start_matches('@').to_ascii_lowercase())
        .filter(|s| is_valid_address(s))
        .unwrap_or_else(|| "unknown".into());

    let mtime_ms = if exists {
        fs::metadata(&abs_path)
            .ok()
            .and_then(|meta| meta.modified().ok())
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|dur| UnixMs(dur.as_millis() as u64))
    } else {
        None
    };

    let parsed_time = shadow::extract_letter_timestamp_detailed(
        &rel_path,
        fm.id.as_deref(),
        fm.date_ms.as_deref(),
        fm.date.as_deref(),
        fm.ts.as_deref(),
        fm.in_reply_to.as_deref(),
        mtime_ms,
    );
    let (ts, ts_basis) = if parsed_time.basis == TimeBasis::FileModified && mtime_ms.is_none() {
        ("1970-01-01T00:00:00Z".to_owned(), "recorded".to_owned())
    } else {
        (
            parsed_time.ts.to_rfc3339_sec(),
            parsed_time.basis.as_mailbox_ts_basis().to_owned(),
        )
    };

    let acked = is_acked_in_sandbox(root, &to, &rel_path, &sha256)
        .map_err(|e| SandboxError::Io(e.to_string()))?;
    let state = if acked {
        "acked".to_owned()
    } else {
        "open".to_owned()
    };

    let topic = fm.topic.filter(|t| is_valid_token(t));
    let round = fm.round.filter(|&r| r <= 9_999);
    let label = match (fm.priority.as_deref(), fm.subject.as_deref()) {
        (Some("blocking"), Some(subj)) if !subj.is_empty() => {
            Some(truncate_label(&format!("[blocking] {subj}"), 160))
        }
        (Some("blocking"), _) => Some("[blocking]".into()),
        (_, Some(subj)) if !subj.is_empty() => Some(truncate_label(subj, 160)),
        _ => None,
    };

    Ok(MailboxItem {
        schema: MailSchemaV1,
        id: format!("letter:{rel_path}"),
        from,
        to,
        kind: "letter".into(),
        state,
        ts,
        ts_basis,
        reference: MailRef {
            store: "letters".into(),
            path: PathBuf::from(&rel_path),
            sha256,
        },
        topic,
        round,
        label,
    })
}

fn atomic_write_if_absent(dir: &Path, file_name: &str, bytes: &[u8]) -> Result<PathBuf, ApiError> {
    let dest = dir.join(file_name);
    let created = rex_wave_fs::write_atomic_if_absent_path(&dest, bytes, SANDBOX_FILE_MODE, true)
        .map_err(|e| ApiError::Internal(format!("write {}: {e}", dest.display())))?;
    if created || read_letter(&dest)? == bytes {
        return Ok(dest);
    }
    Err(ApiError::conflict(format!(
        "canon file {} already exists with different bytes (BoxMonotone)",
        dest.display()
    )))
}

fn canonical_bell_key(schedule_id: &str, mark: &str) -> String {
    let sched = schedule_id.trim();
    let m = mark.trim();
    if m.is_empty() || sched.ends_with(&format!("@{m}")) {
        sched.to_owned()
    } else {
        format!("{sched}@{m}")
    }
}

fn bell_digest16(seat: &SeatName, canonical_key: &str) -> String {
    sha256_short16(&format!("bell/1\n{seat}\n{canonical_key}"))
}

fn scoped_send_idem_key(by: &Principal, raw_idem: &IdemKey) -> Result<IdemKey, ApiError> {
    let principal_key = match by {
        Principal::Operator { seat } => format!("operator:{seat}"),
        Principal::Seat { seat } => format!("seat:{seat}"),
        Principal::Worker { wave, role } => format!("worker:{wave}/{role}"),
        Principal::System { part } => format!("system:{part:?}"),
    };
    let digest = sha256_bytes(
        format!("mail-send-idem/1\n{principal_key}\0{}", raw_idem.as_str()).as_bytes(),
    );
    Ok(IdemKey::parse(format!("send:{}", &digest.as_str()[..32]))?)
}

fn send_request_matches_committed(candidate: &MailMessage, existing: &MailMessage) -> bool {
    candidate.from == existing.from
        && candidate.to == existing.to
        && candidate.cc == existing.cc
        && candidate.subject == existing.subject
        && candidate.body == existing.body
        && candidate.priority == existing.priority
        && candidate.topic == existing.topic
        && candidate.in_reply_to == existing.in_reply_to
        && candidate.refs == existing.refs
}

fn authorize_dissolved_ack(
    ctx: &DecideCtx,
    requested_seat: Option<&SeatName>,
    entry: &CommittedMailEntry,
) -> Result<SeatName, ApiError> {
    match ctx.by() {
        Principal::Worker { .. } => Err(ApiError::Denied {
            principal: ctx.by().to_string(),
            action: "ack mail as worker".into(),
        }),
        Principal::Seat { seat } => {
            if let Some(explicit) = requested_seat
                && explicit != seat
            {
                return Err(ApiError::Denied {
                    principal: ctx.by().to_string(),
                    action: format!("ack mail on behalf of @{explicit}"),
                });
            }
            let is_recipient = entry.message.to.contains(seat) || entry.message.cc.contains(seat);
            if !is_recipient {
                return Err(ApiError::Denied {
                    principal: ctx.by().to_string(),
                    action: format!("ack mail {} not addressed to @{seat}", entry.message.sha256),
                });
            }
            Ok(seat.clone())
        }
        Principal::Operator { seat } => {
            if let Some(explicit) = requested_seat {
                Ok(explicit.clone())
            } else if entry.message.to.contains(seat) || entry.message.cc.contains(seat) {
                Ok(seat.clone())
            } else {
                Err(ApiError::Denied {
                    principal: ctx.by().to_string(),
                    action: format!(
                        "ack mail {} addressed to another seat without explicit --seat",
                        entry.message.sha256
                    ),
                })
            }
        }
        Principal::System { .. } => {
            if let Some(explicit) = requested_seat {
                Ok(explicit.clone())
            } else if entry.message.to.len() == 1 {
                Ok(entry.message.to[0].clone())
            } else {
                Err(ApiError::Invalid(InvalidId {
                    what: "seat".into(),
                    reason: "explicit seat required for system ack".into(),
                }))
            }
        }
    }
}

struct MailWriterState {
    writer: StreamWriter,
    mail: MailState,
    init_error: Option<String>,
}

pub struct MailBridge {
    root: SandboxRoot,
    reader: Journal,
    state: Mutex<MailWriterState>,
    presence_cache: std::sync::Mutex<(Seq, PresenceState)>,
    scan_cache: std::sync::Mutex<DiskScanCache>,
    hub: Option<Hub>,
    room_svc: Option<Arc<dyn Service<RoomCommand>>>,
    config_watch: Option<tokio::sync::watch::Receiver<rex_wave_api::config::RexConfig>>,
    house: rex_wave_api::presence::HouseProfile,
    drop_dir: Option<PathBuf>,
    drop_quiet_ms: u64,
    export_enabled: Arc<AtomicBool>,
}

impl MailBridge {
    /// Fallible constructor propagating any journal replay failure immediately.
    pub fn open(
        root: SandboxRoot,
        reader: Journal,
        writer: StreamWriter,
    ) -> Result<Self, ApiError> {
        let mail = MailState::from_journal(&reader)?;
        let house = rex_wave_api::presence::HouseProfile::default();
        Ok(MailBridge {
            root,
            reader,
            state: Mutex::new(MailWriterState {
                writer,
                mail,
                init_error: None,
            }),
            presence_cache: std::sync::Mutex::new((
                Seq::ORIGIN,
                PresenceState::with_house_profile(house.clone()),
            )),
            scan_cache: std::sync::Mutex::new(DiskScanCache::default()),
            hub: None,
            room_svc: None,
            config_watch: None,
            house,
            drop_dir: None,
            drop_quiet_ms: 0,
            export_enabled: Arc::new(AtomicBool::new(true)),
        })
    }

    pub fn open_with_hub(
        root: SandboxRoot,
        reader: Journal,
        writer: StreamWriter,
        hub: Hub,
    ) -> Result<Self, ApiError> {
        let mut bridge = Self::open(root, reader, writer)?;
        bridge.hub = Some(hub);
        Ok(bridge)
    }

    /// Without a [`Hub`], committed `Mail` entries persist for replay and `Read` only;
    /// live daemons pass a [`Hub`] via [`MailBridge::open_with_hub`]. If journal
    /// hydration fails, the error is latched and returned on every operation rather
    /// than silently running with empty state.
    pub fn new(root: SandboxRoot, reader: Journal, writer: StreamWriter) -> Self {
        let (mail, init_error) = match MailState::from_journal(&reader) {
            Ok(m) => (m, None),
            Err(e) => (MailState::default(), Some(e.to_string())),
        };
        let house = rex_wave_api::presence::HouseProfile::default();
        MailBridge {
            root,
            reader,
            state: Mutex::new(MailWriterState {
                writer,
                mail,
                init_error,
            }),
            presence_cache: std::sync::Mutex::new((
                Seq::ORIGIN,
                PresenceState::with_house_profile(house.clone()),
            )),
            scan_cache: std::sync::Mutex::new(DiskScanCache::default()),
            hub: None,
            room_svc: None,
            config_watch: None,
            house,
            drop_dir: None,
            drop_quiet_ms: 0,
            export_enabled: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn with_hub(root: SandboxRoot, reader: Journal, writer: StreamWriter, hub: Hub) -> Self {
        let mut bridge = Self::new(root, reader, writer);
        bridge.hub = Some(hub);
        bridge
    }

    pub fn with_room_service(mut self, room_svc: Arc<dyn Service<RoomCommand>>) -> Self {
        self.room_svc = Some(room_svc);
        self
    }

    pub fn with_config_watch(
        mut self,
        rx: tokio::sync::watch::Receiver<rex_wave_api::config::RexConfig>,
    ) -> Self {
        self.config_watch = Some(rx);
        self
    }

    pub fn with_house_profile(mut self, house: rex_wave_api::presence::HouseProfile) -> Self {
        *self
            .presence_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = (
            Seq::ORIGIN,
            PresenceState::with_house_profile(house.clone()),
        );
        self.house = house;
        self
    }

    pub fn with_drop_dir(mut self, drop_dir: Option<PathBuf>) -> Self {
        self.drop_dir = drop_dir;
        self
    }

    /// Sets the minimum quiet age (or two-pass `StatSig` stability) required before
    /// claiming a newly written file in `drop_dir`.
    pub fn with_drop_quiet_ms(mut self, quiet_ms: u64) -> Self {
        self.drop_quiet_ms = quiet_ms;
        self
    }

    /// Runtime toggle for mirror export (useful for deterministic crash/recovery tests).
    pub fn set_export_enabled(&self, enabled: bool) {
        self.export_enabled.store(enabled, Ordering::Release);
    }

    pub async fn mail_state_snapshot(&self) -> MailState {
        self.state.lock().await.mail.clone()
    }

    fn should_export(&self) -> bool {
        if !self.export_enabled.load(Ordering::Acquire) {
            return false;
        }
        if let Some(ref rx) = self.config_watch {
            return rx.borrow().mail.export_enabled;
        }
        true
    }

    fn active_mirror(&self) -> Result<MirrorTarget, ApiError> {
        let (cfg_mirror, cfg_drop) = match &self.config_watch {
            Some(rx) => {
                let cfg = rx.borrow();
                (cfg.mail.mirror_dir.clone(), cfg.mail.drop_dir.clone())
            }
            None => (None, None),
        };
        let effective_drop = cfg_drop.or_else(|| self.drop_dir.clone());
        if let Some(mirror_path) = cfg_mirror {
            return MirrorTarget::open_with_guard(
                &mirror_path,
                effective_drop.as_deref(),
                self.root.guard.clone(),
            )
            .map_err(|e| match e {
                SandboxError::Production(_) | SandboxError::Config(_) => ApiError::Denied {
                    principal: "mailbridge".into(),
                    action: format!("mirror_dir refused by protected-roots: {e}"),
                },
                other => ApiError::Internal(format!("open mirror_dir: {other}")),
            });
        }
        if let Some(ref drop_path) = effective_drop {
            let lex = rex_wave_fs::lexical_normalize(drop_path).map_err(|e| {
                ApiError::Invalid(InvalidId {
                    what: "drop_dir".into(),
                    reason: e.to_string(),
                })
            })?;
            self.root
                .guard
                .check(&lex, drop_path)
                .map_err(|e| ApiError::Denied {
                    principal: "mailbridge".into(),
                    action: format!("drop_dir refused by protected-roots: {e}"),
                })?;
            if rex_wave_fs::entry_exists(&lex) {
                self.root
                    .guard
                    .verify_dir(&lex)
                    .map_err(|e| ApiError::Denied {
                        principal: "mailbridge".into(),
                        action: format!("drop_dir refused: {e}"),
                    })?;
            }
        }
        Ok(MirrorTarget::from_sandbox(&self.root, effective_drop))
    }

    fn replay_presence(&self) -> PresenceState {
        let mut guard = self
            .presence_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let (ref mut after, ref mut state) = *guard;
        while let Ok(page) = self.reader.read(&StreamId::Presence, *after, 512) {
            let Some(last) = page.last() else {
                break;
            };
            *after = last.seq;
            for env in &page {
                if let Event::Presence(ev) = &env.event {
                    let meta = rex_wave_runtime::EntryMeta {
                        seq: env.seq,
                        at: env.at,
                        by: env.by.clone(),
                    };
                    <Presence as rex_wave_runtime::Decider>::evolve(state, ev, &meta);
                }
            }
        }
        state.clone()
    }

    fn publish_fresh(&self, appended: &Appended) {
        if let (Appended::Fresh(committed), Some(hub)) = (appended, &self.hub) {
            hub.publish(committed);
        }
    }

    /// Runs a load-bearing `MailStore.tla` `Reconciled` sweep between `journal.sqlite3`
    /// and the active mirror + drop directory, keeping disk scans, room projections,
    /// and mirror exports outside `self.state.lock()`.
    pub async fn reconcile(&self) -> Result<MailReconcileReport, ApiError> {
        let known_shas: std::collections::HashSet<Sha256Hex> = {
            let st = self.state.lock().await;
            if let Some(ref err) = st.init_error {
                return Err(ApiError::Internal(format!(
                    "mail journal hydration failed: {err}"
                )));
            }
            st.mail.messages.keys().cloned().collect()
        };

        let mirror = self.active_mirror()?;
        let presence = self.replay_presence();
        let aliases = AliasMap::from_presence(&presence);
        let export_enabled = self.should_export();
        let now = rex_wave_runtime::now();

        let inputs = {
            let mut cache = self
                .scan_cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            let mirror_scan = mirror.clone();
            let aliases_scan = aliases.clone();
            let quiet_ms = self.drop_quiet_ms;
            let (collected, next_cache) = tokio::task::spawn_blocking(move || {
                let res = DiskReconcileInputs::collect(
                    &mirror_scan,
                    &aliases_scan,
                    &known_shas,
                    export_enabled,
                    quiet_ms,
                    now,
                    &mut cache,
                )?;
                Ok::<_, ApiError>((res, cache))
            })
            .await
            .map_err(|e| ApiError::Internal(format!("reconcile scan task panicked: {e}")))??;
            *self.scan_cache.lock().unwrap_or_else(|e| e.into_inner()) = next_cache;
            collected
        };

        let mut work = {
            let mut st = self.state.lock().await;
            let MailWriterState {
                writer,
                mail,
                init_error: _,
            } = &mut *st;
            reconcile_commit_phase(
                mail,
                writer,
                inputs,
                &aliases,
                export_enabled,
                now,
                self.hub.as_ref(),
            )?
        };

        if let Some(ref rsvc) = self.room_svc {
            for (msg, auth_by) in &work.missing_room_msgs {
                if let Some(ref room) = msg.room {
                    let parent_seq = {
                        let st = self.state.lock().await;
                        msg.in_reply_to
                            .as_deref()
                            .and_then(|p| st.mail.resolve_parent_room_seq(room, p))
                    };
                    let caller_ctx = auth_by
                        .as_ref()
                        .map(|p| DecideCtx::for_internal_replay(p.clone(), msg.created_at));
                    match crate::dissolved::project_message_to_room_service_checked(
                        rsvc.as_ref(),
                        msg,
                        parent_seq,
                        caller_ctx.as_ref(),
                    )
                    .await
                    {
                        crate::dissolved::RoomProjectionOutcome::Posted(seq) => {
                            self.state
                                .lock()
                                .await
                                .mail
                                .room_post_seqs
                                .insert((room.clone(), msg.sha256.clone()), seq);
                        }
                        crate::dissolved::RoomProjectionOutcome::PermanentCollision => {
                            self.state
                                .lock()
                                .await
                                .mail
                                .failed_room_projections
                                .insert((room.clone(), msg.sha256.clone()));
                        }
                        crate::dissolved::RoomProjectionOutcome::TransientFailure => {}
                    }
                }
            }
        }

        if export_enabled {
            let mirror_exp = mirror.clone();
            work = tokio::task::spawn_blocking(move || {
                execute_reconcile_exports(&mirror_exp, &mut work)?;
                Ok::<_, ApiError>(work)
            })
            .await
            .map_err(|e| ApiError::Internal(format!("reconcile export task panicked: {e}")))??;
        }

        Ok(work.report)
    }

    async fn send(&self, req: MailSend, ctx: DecideCtx) -> Result<MailSent, ApiError> {
        let from_seat = match ctx.by() {
            Principal::Worker { .. } => {
                return Err(ApiError::Denied {
                    principal: ctx.by().to_string(),
                    action: "send mail as worker".into(),
                });
            }
            Principal::Seat { seat } => {
                if let Some(ref explicit) = req.from
                    && explicit != seat
                {
                    return Err(ApiError::Denied {
                        principal: ctx.by().to_string(),
                        action: format!("send mail as @{explicit}"),
                    });
                }
                seat.as_str().to_owned()
            }
            Principal::Operator { seat } => req.from.as_ref().unwrap_or(seat).as_str().to_owned(),
            Principal::System { .. } => req
                .from
                .as_ref()
                .ok_or_else(|| {
                    ApiError::Invalid(InvalidId {
                        what: "from".into(),
                        reason: "system principals must supply explicit --from seat".into(),
                    })
                })?
                .as_str()
                .to_owned(),
        };

        let presence = self.replay_presence();
        let aliases = AliasMap::from_presence(&presence);
        let canon_from = aliases.canonicalize(&from_seat);
        let canon_to: Vec<SeatName> = req
            .to
            .into_iter()
            .map(|s| aliases.canonical_seat_name(s.as_str()).unwrap_or(s))
            .collect();
        let canon_cc: Vec<SeatName> = req
            .cc
            .into_iter()
            .map(|s| aliases.canonical_seat_name(s.as_str()).unwrap_or(s))
            .collect();

        let scoped_idem = match &req.idem {
            Some(raw) => Some(scoped_send_idem_key(ctx.by(), raw)?),
            None => None,
        };

        let msg = synthesize_mail_message(
            &canon_from,
            canon_to,
            canon_cc,
            &req.subject,
            &req.body,
            req.priority,
            req.topic.as_deref(),
            req.in_reply_to.as_deref(),
            &req.refs,
            ctx.at(),
        )?;

        let mut st = self.state.lock().await;
        if let Some(ref err) = st.init_error {
            return Err(ApiError::Internal(format!(
                "mail journal hydration failed: {err}"
            )));
        }

        // 1. Check principal-scoped caller idempotency key FIRST, enforcing payload equivalence.
        if let Some(ref sidem) = scoped_idem
            && let Some(existing_envs) = st
                .writer
                .lookup(sidem)
                .map_err(|e| ApiError::Internal(e.to_string()))?
        {
            let existing_msg = existing_envs
                .iter()
                .find_map(|env| match &env.event {
                    Event::Mail(MailEvent::MessageCommitted { message, .. }) => {
                        Some((**message).clone())
                    }
                    _ => None,
                })
                .ok_or_else(|| {
                    ApiError::Internal(
                        "corrupt send idempotency record: missing MessageCommitted".into(),
                    )
                })?;
            if !send_request_matches_committed(&msg, &existing_msg) {
                return Err(ApiError::conflict(format!(
                    "idempotency key {} was already used for a different mail payload",
                    req.idem.as_ref().map(IdemKey::as_str).unwrap_or("")
                )));
            }
            let parent_seq = existing_msg.room.as_ref().and_then(|room| {
                existing_msg
                    .in_reply_to
                    .as_deref()
                    .and_then(|p| st.mail.resolve_parent_room_seq(room, p))
            });
            let existing_room_seq = existing_msg.room.as_ref().and_then(|r| {
                st.mail
                    .room_post_seqs
                    .get(&(r.clone(), existing_msg.sha256.clone()))
                    .copied()
            });
            let idx_recs = st
                .mail
                .messages
                .get(&existing_msg.sha256)
                .map(CommittedMailEntry::mirror_index_records)
                .unwrap_or_default();
            drop(st);

            let room_seq = if existing_room_seq.is_some() {
                existing_room_seq
            } else if let Some(ref rsvc) = self.room_svc
                && let Some(ref room) = existing_msg.room
            {
                let seq = project_message_to_room_service(
                    rsvc.as_ref(),
                    &existing_msg,
                    parent_seq,
                    Some(&ctx),
                )
                .await;
                if let Some(s) = seq {
                    self.state
                        .lock()
                        .await
                        .mail
                        .room_post_seqs
                        .insert((room.clone(), existing_msg.sha256.clone()), s);
                }
                seq
            } else {
                None
            };

            let mut exported = false;
            if self.should_export() {
                let mirror = self.active_mirror()?;
                let raw = message_raw_bytes(&existing_msg)?;
                let exp_sha = existing_msg.sha256.clone();
                let exp_path = existing_msg.path.clone();
                let exp_res = tokio::task::spawn_blocking(move || {
                    mirror.export_blob_if_absent(&exp_sha, &raw)?;
                    mirror.export_working_tree_if_absent(&exp_path, &raw)?;
                    if !idx_recs.is_empty() {
                        mirror.append_index_batch_if_absent(&idx_recs)?;
                    }
                    Ok::<_, ApiError>(())
                })
                .await
                .map_err(|e| ApiError::Internal(format!("send export task panicked: {e}")))?;
                match exp_res {
                    Ok(()) => exported = true,
                    Err(e) if req.wait_export => return Err(e),
                    Err(_) => {}
                }
            }
            let delivery_notices = delivery_notices_for_seats(
                existing_msg.to.iter().chain(existing_msg.cc.iter()),
                &presence,
                ctx.at(),
            );
            return Ok(MailSent {
                sha256: existing_msg.sha256,
                id: existing_msg.id,
                path: existing_msg.path,
                from: existing_msg.from,
                to: existing_msg.to,
                cc: existing_msg.cc,
                subject: existing_msg.subject,
                created_at: existing_msg.created_at,
                room: existing_msg.room,
                room_seq,
                exported,
                replayed: true,
                delivery_notices,
            });
        }

        let mut replayed = false;
        let committed_msg = if scoped_idem.is_none()
            && let Some(existing) = st.mail.messages.get(&msg.sha256)
        {
            replayed = true;
            existing.message.clone()
        } else {
            // Single commit point = SQLite transaction FIRST!
            let effective_idem = match &scoped_idem {
                Some(k) => k.clone(),
                None => IdemKey::parse(format!("mail_msg:{}", msg.sha256))?,
            };
            let appended = st
                .writer
                .append(
                    ctx.by(),
                    ctx.at(),
                    Some(&effective_idem),
                    vec![Event::Mail(MailEvent::MessageCommitted {
                        message: Box::new(msg.clone()),
                        mutated_after_ack: false,
                        prior_ack_sha: None,
                        send_idem: scoped_idem.clone(),
                    })],
                )
                .map_err(|e| ApiError::Internal(e.to_string()))?;
            match &appended {
                Appended::Fresh(envs) => {
                    for env in envs {
                        st.mail.apply(env);
                    }
                    self.publish_fresh(&appended);
                    msg
                }
                Appended::Replayed(envs) => {
                    replayed = true;
                    let found = envs
                        .iter()
                        .find_map(|env| match &env.event {
                            Event::Mail(MailEvent::MessageCommitted { message, .. }) => {
                                Some((**message).clone())
                            }
                            _ => None,
                        })
                        .ok_or_else(|| {
                            ApiError::Internal(
                                "corrupt send idempotency record: missing MessageCommitted".into(),
                            )
                        })?;
                    if scoped_idem.is_some() && !send_request_matches_committed(&msg, &found) {
                        return Err(ApiError::conflict(
                            "idempotency key was already used for a different mail payload",
                        ));
                    }
                    found
                }
            }
        };

        let parent_seq = committed_msg.room.as_ref().and_then(|room| {
            committed_msg
                .in_reply_to
                .as_deref()
                .and_then(|p| st.mail.resolve_parent_room_seq(room, p))
        });
        let existing_room_seq = committed_msg.room.as_ref().and_then(|room| {
            st.mail
                .room_post_seqs
                .get(&(room.clone(), committed_msg.sha256.clone()))
                .copied()
        });
        let idx_recs = st
            .mail
            .messages
            .get(&committed_msg.sha256)
            .map(CommittedMailEntry::mirror_index_records)
            .unwrap_or_else(|| {
                vec![MirrorIndexRecord {
                    schema: 1,
                    sha256: committed_msg.sha256.clone(),
                    path: normalize_letter_path(&committed_msg.path.to_string_lossy())
                        .map(|n| n.canonical)
                        .unwrap_or_else(|| committed_msg.path.to_string_lossy().into_owned()),
                    to: committed_msg.to.clone(),
                    cc: committed_msg.cc.clone(),
                    created_at_ms: committed_msg.created_at.0,
                    mutated_after_ack: false,
                    prior_ack_sha: None,
                    origin: committed_msg.origin,
                    send_idem: scoped_idem,
                    send_by: Some(ctx.by().clone()),
                }]
            });
        drop(st);

        // 2. Project into DM / multi-seat mail thread room.
        let room_seq = if existing_room_seq.is_some() {
            existing_room_seq
        } else if let Some(ref rsvc) = self.room_svc
            && let Some(ref room) = committed_msg.room
        {
            let seq = project_message_to_room_service(
                rsvc.as_ref(),
                &committed_msg,
                parent_seq,
                Some(&ctx),
            )
            .await;
            if let Some(s) = seq {
                self.state
                    .lock()
                    .await
                    .mail
                    .room_post_seqs
                    .insert((room.clone(), committed_msg.sha256.clone()), s);
            }
            seq
        } else {
            None
        };

        // 3. Export to mirror AFTER durable SQLite commit.
        let mut exported = false;
        if self.should_export() {
            let mirror = self.active_mirror()?;
            let raw = message_raw_bytes(&committed_msg)?;
            let exp_sha = committed_msg.sha256.clone();
            let exp_path = committed_msg.path.clone();
            let exp_res = tokio::task::spawn_blocking(move || {
                mirror.export_blob_if_absent(&exp_sha, &raw)?;
                mirror.export_working_tree_if_absent(&exp_path, &raw)?;
                mirror.append_index_batch_if_absent(&idx_recs)?;
                Ok::<_, ApiError>(())
            })
            .await
            .map_err(|e| ApiError::Internal(format!("send export task panicked: {e}")))?;
            match exp_res {
                Ok(()) => exported = true,
                Err(e) if req.wait_export => return Err(e),
                Err(_) => {}
            }
        }

        let delivery_notices = delivery_notices_for_seats(
            committed_msg.to.iter().chain(committed_msg.cc.iter()),
            &presence,
            ctx.at(),
        );

        Ok(MailSent {
            sha256: committed_msg.sha256,
            id: committed_msg.id,
            path: committed_msg.path,
            from: committed_msg.from,
            to: committed_msg.to,
            cc: committed_msg.cc,
            subject: committed_msg.subject,
            created_at: committed_msg.created_at,
            room: committed_msg.room,
            room_seq,
            exported,
            replayed,
            delivery_notices,
        })
    }

    async fn check(&self, req: MailCheck, ctx: DecideCtx) -> Result<MailCheckView, ApiError> {
        let target_seat = match ctx.by() {
            Principal::Worker { .. } => {
                return Err(ApiError::Denied {
                    principal: ctx.by().to_string(),
                    action: "check mail as worker".into(),
                });
            }
            Principal::Seat { seat } => {
                if let Some(ref requested) = req.seat
                    && requested != seat
                {
                    return Err(ApiError::Denied {
                        principal: ctx.by().to_string(),
                        action: format!("check mail for @{requested}"),
                    });
                }
                seat.clone()
            }
            Principal::Operator { seat } => req.seat.unwrap_or_else(|| seat.clone()),
            Principal::System { .. } => req.seat.ok_or_else(|| {
                ApiError::Invalid(InvalidId {
                    what: "seat".into(),
                    reason: "system principal must supply explicit --seat".into(),
                })
            })?,
        };
        let presence = self.replay_presence();
        let st = self.state.lock().await;
        if let Some(ref err) = st.init_error {
            return Err(ApiError::Internal(format!(
                "mail journal hydration failed: {err}"
            )));
        }
        Ok(st.mail.check_for_seat(
            &target_seat,
            req.include_all,
            req.limit,
            &presence,
            ctx.at(),
        ))
    }

    async fn search(&self, req: MailSearch, ctx: DecideCtx) -> Result<MailSearchView, ApiError> {
        let trimmed = req.query.trim();
        if trimmed.is_empty() {
            return Err(ApiError::Invalid(InvalidId {
                what: "query".into(),
                reason: "search query must not be empty".into(),
            }));
        }
        let effective_seat = match ctx.by() {
            Principal::Worker { .. } => {
                return Err(ApiError::Denied {
                    principal: ctx.by().to_string(),
                    action: "search mail as worker".into(),
                });
            }
            Principal::Seat { seat } => {
                if let Some(ref requested) = req.seat
                    && requested != seat
                {
                    return Err(ApiError::Denied {
                        principal: ctx.by().to_string(),
                        action: format!("search mail for @{requested}"),
                    });
                }
                Some(seat.clone())
            }
            Principal::Operator { .. } | Principal::System { .. } => req.seat,
        };
        let limit = req.limit.unwrap_or(50).clamp(1, 500) as usize;
        let matches = self
            .reader
            .search_mail(
                trimmed,
                effective_seat.as_ref().map(SeatName::as_str),
                limit,
            )
            .map_err(|e| ApiError::Internal(format!("mail search: {e}")))?;

        let st = self.state.lock().await;
        if let Some(ref err) = st.init_error {
            return Err(ApiError::Internal(format!(
                "mail journal hydration failed: {err}"
            )));
        }
        let mut hits = Vec::with_capacity(matches.len());
        for m in matches {
            if let Ok(sha) = Sha256Hex::parse(m.sha256)
                && let Some(entry) = st.mail.messages.get(&sha)
            {
                let summary = st.mail.summarize_for(entry, effective_seat.as_ref());
                let snippet = if m.snippet.trim().is_empty() {
                    summary.snippet
                } else {
                    m.snippet
                };
                hits.push(MailSearchHit {
                    sha256: summary.sha256,
                    sha16: summary.sha16,
                    id: summary.id,
                    from: summary.from,
                    to: summary.to,
                    cc: summary.cc,
                    subject: summary.subject,
                    path: summary.path,
                    created_at: summary.created_at,
                    acked: summary.acked,
                    mutated_after_ack: summary.mutated_after_ack,
                    room: summary.room,
                    snippet,
                });
            }
        }
        Ok(MailSearchView {
            query: trimmed.to_owned(),
            seat: effective_seat,
            hits,
        })
    }

    async fn reconcile_cmd(
        &self,
        _req: MailReconcile,
        ctx: DecideCtx,
    ) -> Result<MailReconcileReport, ApiError> {
        if !ctx.by().is_operator_or_system() {
            return Err(ApiError::Denied {
                principal: ctx.by().to_string(),
                action: "reconcile mail store".into(),
            });
        }
        self.reconcile().await
    }

    async fn promote(&self, req: MailPromote, ctx: DecideCtx) -> Result<LetterRef, ApiError> {
        if req.to.is_empty() {
            return Err(ApiError::Invalid(InvalidId {
                what: "to".into(),
                reason: "at least one recipient seat is required".into(),
            }));
        }
        let trimmed_subject = req.subject.trim();
        if trimmed_subject.is_empty() || req.subject.chars().any(char::is_control) {
            return Err(ApiError::Invalid(InvalidId {
                what: "subject".into(),
                reason: "must be non-empty without control characters".into(),
            }));
        }
        if !is_valid_token(req.topic.trim())
            || req.topic.trim().starts_with('.')
            || req.topic.trim().starts_with('-')
        {
            return Err(ApiError::Invalid(InvalidId {
                what: "topic".into(),
                reason: "must be a safe token ([A-Za-z0-9._-], 1..=64 chars)".into(),
            }));
        }

        let span_u64 = req.range.last().0 - req.range.first().0 + 1;
        if span_u64 > MAX_PROMOTE_SPAN {
            return Err(ApiError::Invalid(InvalidId {
                what: "range".into(),
                reason: format!("promotion range may span at most {MAX_PROMOTE_SPAN} seqs"),
            }));
        }

        let room_stream = StreamId::Room {
            room: req.room.clone(),
        };
        can_read_in(ctx.by(), &room_stream, &self.reader, None)?;

        let entries: Vec<Envelope> = self
            .reader
            .read(
                &room_stream,
                Seq(req.range.first().0 - 1),
                span_u64 as usize,
            )
            .map_err(|e| ApiError::Internal(e.to_string()))?
            .into_iter()
            .filter(|e| e.seq <= req.range.last())
            .collect();

        let has_posts = entries
            .iter()
            .any(|e| matches!(e.event, Event::Room(room::RoomEvent::Posted { .. })));
        if entries.is_empty() || !has_posts {
            return Err(ApiError::NotFound {
                what: format!(
                    "posted messages in range {}..={}",
                    req.range.first().0,
                    req.range.last().0
                ),
            });
        }

        let (file_name, bytes) = render_letter(&req, ctx.by(), &entries);
        if bytes.len() as u64 > MAX_LETTER_BYTES {
            return Err(ApiError::Invalid(InvalidId {
                what: "range".into(),
                reason: format!(
                    "promoted letter is {} bytes, exceeding max {MAX_LETTER_BYTES} bytes",
                    bytes.len()
                ),
            }));
        }
        let sha256 = sha256_bytes(&bytes);

        let mut seen_seats = BTreeSet::new();
        let mut unique_seats = Vec::new();
        for seat in &req.to {
            if seen_seats.insert(seat.clone()) {
                unique_seats.push(seat.clone());
            }
        }

        let mut banked_letters = Vec::with_capacity(unique_seats.len());
        for seat in &unique_seats {
            let box_name = format!("to-{seat}");
            let box_dir = self.root.ensure_dir(&[&box_name])?;
            let dest = atomic_write_if_absent(&box_dir, &file_name, &bytes)?;
            let item = project(&self.root, &dest, &bytes)
                .map_err(|e| ApiError::Internal(format!("project banked letter: {e}")))?;
            let wid = wake_id(seat, &box_name, &file_name, &sha256);
            banked_letters.push(LetterRef {
                path: item.reference.path.clone(),
                sha256: sha256.clone(),
                wake_id: wid,
                item,
            });
        }

        let seats_key: Vec<&str> = unique_seats.iter().map(SeatName::as_str).collect();
        let idem_hash = sha256_short16(&format!(
            "promote/1\n{:?}\n{}:{}\n{}\n{sha256}",
            req.room,
            req.range.first().0,
            req.range.last().0,
            seats_key.join(",")
        ));
        let idem = IdemKey::parse(format!("promote:{idem_hash}"))?;

        let events: Vec<Event> = banked_letters
            .iter()
            .map(|letter| {
                Event::Mail(MailEvent::Banked {
                    from_room: req.room.clone(),
                    range: (req.range.first(), req.range.last()),
                    letter: letter.clone(),
                })
            })
            .collect();

        let mut st = self.state.lock().await;
        let appended = st
            .writer
            .append(ctx.by(), ctx.at(), Some(&idem), events)
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        for letter in &banked_letters {
            st.mail.seen_wakes.insert(letter.wake_id.clone());
        }
        drop(st);
        self.publish_fresh(&appended);

        Ok(banked_letters
            .into_iter()
            .next()
            .expect("non-empty recipients"))
    }

    async fn ack(&self, req: MailAck, ctx: DecideCtx) -> Result<MailAcked, ApiError> {
        if matches!(ctx.by(), Principal::Worker { .. }) {
            return Err(ApiError::Denied {
                principal: ctx.by().to_string(),
                action: "ack mail as worker".into(),
            });
        }
        if let Principal::Seat { seat } = ctx.by()
            && let Some(ref explicit) = req.seat
            && explicit != seat
        {
            return Err(ApiError::Denied {
                principal: ctx.by().to_string(),
                action: format!("ack mail on behalf of @{explicit}"),
            });
        }
        if req.sha256.is_none() && req.path.is_none() {
            return Err(ApiError::Invalid(InvalidId {
                what: "target".into(),
                reason: "specify sha256 or path to acknowledge".into(),
            }));
        }

        let mut st = self.state.lock().await;
        if let Some(ref err) = st.init_error {
            return Err(ApiError::Internal(format!(
                "mail journal hydration failed: {err}"
            )));
        }

        let resolved = match st
            .mail
            .resolve_ack_target(req.path.as_deref(), req.sha256.as_ref())
        {
            AckTargetResolution::AmbiguousPath {
                raw_path,
                candidates,
            } => {
                // Filter strictly to candidates visible/authorized for this caller so
                // unrelated seats' letters neither cause false ambiguity nor leak counts.
                let mut visible: Vec<CommittedMailEntry> = candidates
                    .into_iter()
                    .filter(|c| authorize_dissolved_ack(&ctx, req.seat.as_ref(), c).is_ok())
                    .collect();
                match visible.len() {
                    0 => {
                        return Err(ApiError::Denied {
                            principal: ctx.by().to_string(),
                            action: format!("ack mail {}", raw_path.display()),
                        });
                    }
                    1 => {
                        let entry = visible.remove(0);
                        let expected_sha_mismatch = req
                            .sha256
                            .as_ref()
                            .filter(|&w| w != &entry.message.sha256)
                            .cloned();
                        AckTargetResolution::Found {
                            entry,
                            expected_sha_mismatch,
                            path_mismatch: None,
                        }
                    }
                    n => {
                        return Err(ApiError::conflict(format!(
                            "ambiguous mail path {} matches {} letters; specify full path or exact --sha256",
                            raw_path.display(),
                            n
                        )));
                    }
                }
            }
            other => other,
        };

        match resolved {
            AckTargetResolution::AmbiguousPath { .. } => unreachable!("filtered above"),
            AckTargetResolution::Found {
                entry,
                expected_sha_mismatch,
                path_mismatch,
            } => {
                // Recipient authorization MUST run BEFORE any conflict/flag check!
                let acker_seat = authorize_dissolved_ack(&ctx, req.seat.as_ref(), &entry)?;
                if let Some(bad_path) = path_mismatch {
                    return Err(ApiError::conflict(format!(
                        "path {} does not match message sha256 {}",
                        bad_path.display(),
                        entry.message.sha256
                    )));
                }
                if let Some(want_sha) = expected_sha_mismatch {
                    return Err(ApiError::conflict(format!(
                        "sha256 mismatch for {}: expected {}, stored {}",
                        entry.message.path.display(),
                        want_sha,
                        entry.message.sha256
                    )));
                }
                if entry.mutated_after_ack {
                    return Err(ApiError::conflict(format!(
                        "refusing to ack mutated_after_ack letter {} (sha256={})",
                        entry.message.path.display(),
                        entry.message.sha256
                    )));
                }

                let sha = entry.message.sha256.clone();
                let rel_path = entry.message.path.clone();
                let already_acked = st.mail.acked.contains(&(acker_seat.clone(), sha.clone()));

                // Single commit point = SQLite FIRST!
                let idem_hash = sha256_short16(&format!("ack/2\n{acker_seat}\n{sha}"));
                let idem = IdemKey::parse(format!("ack:{idem_hash}"))?;
                let appended = st
                    .writer
                    .append(
                        ctx.by(),
                        ctx.at(),
                        Some(&idem),
                        vec![Event::Mail(MailEvent::Acked {
                            path: rel_path.clone(),
                            sha256: sha.clone(),
                            seat: Some(acker_seat.clone()),
                            note: req.note.clone(),
                        })],
                    )
                    .map_err(|e| ApiError::Internal(e.to_string()))?;
                if let Appended::Fresh(envs) = &appended {
                    for env in envs {
                        st.mail.apply(env);
                    }
                }
                // Export the COMMITTED receipt (preserving original `at` and `note` on retries!).
                let committed_ack = st
                    .mail
                    .ack_records
                    .get(&(acker_seat.clone(), sha.clone()))
                    .cloned()
                    .unwrap_or(CommittedAckRecord {
                        seq: Seq::ORIGIN,
                        seat: acker_seat.clone(),
                        sha256: sha.clone(),
                        path: rel_path.clone(),
                        note: req.note.clone(),
                        at: ctx.at(),
                    });
                drop(st);
                self.publish_fresh(&appended);

                let mut exported = false;
                if self.should_export() {
                    match self.active_mirror() {
                        Ok(mirror) => {
                            let exp_res = tokio::task::spawn_blocking(move || {
                                mirror.append_acks_batch_if_absent(&[committed_ack])
                            })
                            .await
                            .map_err(|e| {
                                ApiError::Internal(format!("ack export task panicked: {e}"))
                            })?;
                            match exp_res {
                                Ok(_) => exported = true,
                                Err(e) => {
                                    eprintln!(
                                        "rex-wave mail: ack committed to journal; deferring mirror export to reconcile: {e}"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "rex-wave mail: ack committed to journal; deferring mirror export to reconcile: {e}"
                            );
                        }
                    }
                }

                return Ok(MailAcked {
                    sha256: sha,
                    path: Some(rel_path),
                    seat: acker_seat,
                    already_acked,
                    exported,
                });
            }
            AckTargetResolution::NotDissolved => {}
        }
        drop(st);

        // Fallback: legacy sandbox letter ack ONLY for recognized `to-<seat>/` box paths
        // (such as letters banked by `MailPromote`). Paths outside `to-<seat>/` never
        // record receipts without dissolved audience checks.
        let Some(ref raw_path) = req.path else {
            return Err(ApiError::NotFound {
                what: format!(
                    "mail {}",
                    req.sha256
                        .as_ref()
                        .map(Sha256Hex::as_str)
                        .unwrap_or("unknown")
                ),
            });
        };
        let (abs_path, rel_path, exists) = self.root.confine(raw_path).map_err(|e| match e {
            SandboxError::Production(_) => ApiError::Denied {
                principal: ctx.by().to_string(),
                action: format!("ack path outside sandbox ({})", raw_path.display()),
            },
            other => ApiError::Invalid(InvalidId {
                what: "path".into(),
                reason: other.to_string(),
            }),
        })?;
        let Some(target) = rel_path
            .split_once('/')
            .and_then(|(first, _)| first.strip_prefix("to-"))
            .filter(|s| *s != "desktop")
        else {
            return Err(ApiError::NotFound { what: rel_path });
        };

        let acker_seat_str = match ctx.by() {
            Principal::Seat { seat } if seat.as_str() == target => seat.as_str().to_owned(),
            Principal::Seat { .. } | Principal::Worker { .. } => {
                return Err(ApiError::Denied {
                    principal: ctx.by().to_string(),
                    action: format!("ack letter in to-{target}"),
                });
            }
            Principal::Operator { seat }
                if seat.as_str() == target
                    || target == "owner"
                    || req.seat.as_ref().is_some_and(|s| s.as_str() == target) =>
            {
                target.to_owned()
            }
            Principal::Operator { .. } => {
                return Err(ApiError::Denied {
                    principal: ctx.by().to_string(),
                    action: format!("ack letter in to-{target}"),
                });
            }
            Principal::System { .. } => target.to_owned(),
        };

        if !exists {
            return Err(ApiError::NotFound { what: rel_path });
        }

        let bytes = read_letter(&abs_path)?;
        let on_disk_sha = sha256_bytes(&bytes);
        if let Some(ref want_sha) = req.sha256
            && &on_disk_sha != want_sha
        {
            return Err(ApiError::conflict(format!(
                "sha256 mismatch for {rel_path}: expected {want_sha}, on-disk {on_disk_sha}"
            )));
        }

        let parsed_seat = SeatName::parse(&acker_seat_str).map_err(|e| {
            ApiError::Invalid(InvalidId {
                what: "seat".into(),
                reason: e.to_string(),
            })
        })?;
        let idem_hash = sha256_short16(&format!(
            "ack/1\n{acker_seat_str}\n{rel_path}\n{on_disk_sha}"
        ));
        let idem = IdemKey::parse(format!("ack:{idem_hash}"))?;

        let mut st = self.state.lock().await;
        let scan = scan_sandbox_acks(&self.root, &acker_seat_str, &rel_path, &on_disk_sha, true)?;
        let _ = scan.needs_leading_newline;
        let already_acked = scan.matched
            || st
                .mail
                .acked
                .contains(&(parsed_seat.clone(), on_disk_sha.clone()));
        let appended = st
            .writer
            .append(
                ctx.by(),
                ctx.at(),
                Some(&idem),
                vec![Event::Mail(MailEvent::Acked {
                    path: PathBuf::from(&rel_path),
                    sha256: on_disk_sha.clone(),
                    seat: Some(parsed_seat.clone()),
                    note: req.note.clone(),
                })],
            )
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        if let Appended::Fresh(envs) = &appended {
            for env in envs {
                st.mail.apply(env);
            }
        }
        let committed_ack = st
            .mail
            .ack_records
            .get(&(parsed_seat.clone(), on_disk_sha.clone()))
            .cloned()
            .unwrap_or(CommittedAckRecord {
                seq: Seq::ORIGIN,
                seat: parsed_seat.clone(),
                sha256: on_disk_sha.clone(),
                path: PathBuf::from(&rel_path),
                note: req.note,
                at: ctx.at(),
            });
        drop(st);
        self.publish_fresh(&appended);

        // Always write legacy `to-<seat>/` receipt to the sandbox `state/acks.jsonl`
        // regardless of `mail.export_enabled` so `project()` and sandbox readers see it.
        let sandbox_mirror = MirrorTarget::from_sandbox(&self.root, None);
        let exported = match tokio::task::spawn_blocking(move || {
            sandbox_mirror.append_acks_batch_if_absent(&[committed_ack])
        })
        .await
        {
            Ok(Ok(_)) => true,
            Ok(Err(e)) => {
                eprintln!(
                    "rex-wave mail: legacy ack committed to journal; sandbox acks.jsonl write deferred: {e}"
                );
                false
            }
            Err(e) => {
                return Err(ApiError::Internal(format!(
                    "legacy ack export task panicked: {e}"
                )));
            }
        };
        Ok(MailAcked {
            sha256: on_disk_sha,
            path: Some(PathBuf::from(rel_path)),
            seat: parsed_seat,
            already_acked,
            exported,
        })
    }

    async fn wake_bell(&self, req: MailWakeBell, ctx: DecideCtx) -> Result<Ack, ApiError> {
        let sched = req.bell_id.trim();
        if sched.is_empty() || sched.chars().any(char::is_control) {
            return Err(ApiError::Invalid(InvalidId {
                what: "bell_id".into(),
                reason: "must be non-empty without control characters".into(),
            }));
        }
        if req.fired_at.chars().any(char::is_control) {
            return Err(ApiError::Invalid(InvalidId {
                what: "fired_at".into(),
                reason: "must not contain control characters".into(),
            }));
        }
        let body = room::Body::parse(&req.prompt)?;
        let canonical = canonical_bell_key(sched, &req.fired_at);
        let digest16 = bell_digest16(&req.seat, &canonical);
        let idem = IdemKey::parse(format!("bell:{digest16}"))?;

        let bell_doc = serde_json::json!({
            "bell_id": canonical,
            "schedule_id": sched,
            "mark": req.fired_at.trim(),
            "seat": req.seat.as_str(),
            "prompt": body.as_str(),
        });
        let mut bell_bytes =
            serde_json::to_vec_pretty(&bell_doc).map_err(|e| ApiError::Internal(e.to_string()))?;
        bell_bytes.push(b'\n');

        let mut st = self.state.lock().await;
        if st
            .writer
            .lookup(&idem)
            .map_err(|e| ApiError::Internal(e.to_string()))?
            .is_some()
        {
            return Ok(Ack);
        }

        let bells_dir = self.root.ensure_dir(&["state", "bells"])?;
        let bell_file = format!("{digest16}.json");
        if !bells_dir.join(&bell_file).exists() {
            atomic_write_if_absent(&bells_dir, &bell_file, &bell_bytes)?;
        }

        let appended = st
            .writer
            .append(
                ctx.by(),
                ctx.at(),
                Some(&idem),
                vec![Event::Mail(MailEvent::BellRang {
                    bell_id: canonical,
                    seat: req.seat,
                })],
            )
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        drop(st);
        self.publish_fresh(&appended);
        Ok(Ack)
    }

    pub async fn rescan(&self) -> Result<u32, ApiError> {
        let entries =
            fs::read_dir(self.root.path()).map_err(|e| ApiError::Internal(e.to_string()))?;
        let mut boxes = Vec::new();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };
            let Some(seat_raw) = name_str.strip_prefix("to-") else {
                continue;
            };
            let Ok(seat) = SeatName::parse(seat_raw) else {
                continue;
            };
            if entry.file_type().is_ok_and(|ft| ft.is_dir()) {
                boxes.push((seat, name_str.to_owned(), entry.path()));
            }
        }
        boxes.sort_by(|a, b| a.1.cmp(&b.1));

        let mut projected_count = 0u32;
        let by = Principal::System {
            part: SystemPart::MailBridge,
        };

        for (seat, box_name, box_path) in boxes {
            let Ok(files) = fs::read_dir(&box_path) else {
                continue;
            };
            let mut md_files = Vec::new();
            for f in files.flatten() {
                let fname = f.file_name();
                let Some(fname_str) = fname.to_str() else {
                    continue;
                };
                if fname_str.starts_with('.') || !fname_str.ends_with(".md") {
                    continue;
                }
                if f.file_type().is_ok_and(|ft| ft.is_file()) {
                    md_files.push((fname_str.to_owned(), f.path()));
                }
            }
            md_files.sort_by(|a, b| a.0.cmp(&b.0));

            for (file_name, file_path) in md_files {
                let bytes = read_letter(&file_path)?;
                let item = project(&self.root, &file_path, &bytes)
                    .map_err(|e| ApiError::Internal(e.to_string()))?;
                let wid = wake_id(&seat, &box_name, &file_name, &item.reference.sha256);
                let idem = IdemKey::parse(format!("proj:{}", wid.0))?;

                let mut st = self.state.lock().await;
                if st.mail.seen_wakes.contains(&wid)
                    || st
                        .writer
                        .lookup(&idem)
                        .map_err(|e| ApiError::Internal(e.to_string()))?
                        .is_some()
                {
                    st.mail.seen_wakes.insert(wid);
                    continue;
                }
                let at = rex_wave_runtime::now();
                let appended = st
                    .writer
                    .append(
                        &by,
                        at,
                        Some(&idem),
                        vec![Event::Mail(MailEvent::Projected { item })],
                    )
                    .map_err(|e| ApiError::Internal(e.to_string()))?;
                st.mail.seen_wakes.insert(wid);
                drop(st);
                if matches!(appended, Appended::Fresh(_)) {
                    projected_count += 1;
                }
                self.publish_fresh(&appended);
            }
        }
        Ok(projected_count)
    }
}

impl Service<MailCommand> for MailBridge {
    fn dispatch(
        &self,
        cmd: MailCommand,
        ctx: DecideCtx,
    ) -> BoxFut<'_, Result<WireReply, ApiError>> {
        Box::pin(async move {
            match cmd {
                MailCommand::MailPromote(req) => {
                    Ok(WireReply::MailPromote(self.promote(req, ctx).await?))
                }
                MailCommand::MailAck(a) => Ok(WireReply::MailAck(self.ack(a, ctx).await?)),
                MailCommand::MailWakeBell(b) => {
                    Ok(WireReply::MailWakeBell(self.wake_bell(b, ctx).await?))
                }
                MailCommand::MailSend(req) => Ok(WireReply::MailSend(self.send(req, ctx).await?)),
                MailCommand::MailCheck(req) => {
                    Ok(WireReply::MailCheck(self.check(req, ctx).await?))
                }
                MailCommand::MailSearch(req) => {
                    Ok(WireReply::MailSearch(self.search(req, ctx).await?))
                }
                MailCommand::MailReconcile(req) => Ok(WireReply::MailReconcile(
                    self.reconcile_cmd(req, ctx).await?,
                )),
            }
        })
    }
}

pub async fn serve_wake_bell_sink<C: Client + 'static>(
    addr: SocketAddr,
    client: C,
) -> std::io::Result<()> {
    if !addr.ip().is_loopback() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "wake-bell sink must bind a loopback address",
        ));
    }
    let listener = TcpListener::bind(addr).await?;
    let client = Arc::new(client);
    let sem = Arc::new(Semaphore::new(MAX_SINK_CONNS));
    loop {
        let (stream, _) = listener.accept().await?;
        let Ok(permit) = sem.clone().acquire_owned().await else {
            break;
        };
        let client = client.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _ = tokio::time::timeout(
                Duration::from_secs(10),
                handle_wake_bell_conn(stream, client.as_ref()),
            )
            .await;
        });
    }
    Ok(())
}

async fn handle_wake_bell_conn<C: Client>(
    mut stream: TcpStream,
    client: &C,
) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    let header_end = loop {
        if buf.len() > MAX_HTTP_HEADER {
            write_http_json(
                &mut stream,
                431,
                "Request Header Fields Too Large",
                &serde_json::json!({"ok": false, "code": "headers_too_large", "detail": "header limit exceeded"}),
            )
            .await?;
            return Ok(());
        }
        let n = match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut chunk)).await {
            Ok(Ok(0)) => return Ok(()),
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Ok(()),
        };
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
    };

    let header_str = String::from_utf8_lossy(&buf[..header_end]);
    let mut lines = header_str.split("\r\n");
    let Some(req_line) = lines.next() else {
        return Ok(());
    };
    let mut parts = req_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    if method != "POST" || path != "/sink" {
        write_http_json(
            &mut stream,
            404,
            "Not Found",
            &serde_json::json!({"ok": false, "code": "not_found", "detail": "expected POST /sink"}),
        )
        .await?;
        return Ok(());
    }

    let mut content_length = 0usize;
    let mut content_type_json = false;
    let mut host_ok = false;
    for line in lines {
        if let Some((name, val)) = line.split_once(':') {
            let header = name.trim();
            if header.eq_ignore_ascii_case("content-length")
                && let Ok(len) = val.trim().parse::<usize>()
            {
                content_length = len;
            } else if header.eq_ignore_ascii_case("content-type") {
                let media = val.split(';').next().unwrap_or("").trim();
                content_type_json = media.eq_ignore_ascii_case("application/json");
            } else if header.eq_ignore_ascii_case("host") {
                host_ok = is_loopback_host(val);
            }
        }
    }

    if content_length > MAX_HTTP_BODY {
        write_http_json(
            &mut stream,
            413,
            "Payload Too Large",
            &serde_json::json!({"ok": false, "code": "payload_too_large", "detail": "body limit exceeded"}),
        )
        .await?;
        return Ok(());
    }

    let body_offset = header_end + 4;
    while buf.len() < body_offset + content_length {
        let need = (body_offset + content_length - buf.len()).min(chunk.len());
        let n = match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut chunk[..need]))
            .await
        {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(e),
            Err(_) => break,
        };
        buf.extend_from_slice(&chunk[..n]);
    }

    if !host_ok {
        write_http_json(
            &mut stream,
            403,
            "Forbidden",
            &serde_json::json!({"ok": false, "code": "forbidden_host", "detail": "Host must be loopback"}),
        )
        .await?;
        return Ok(());
    }

    if !content_type_json {
        write_http_json(
            &mut stream,
            415,
            "Unsupported Media Type",
            &serde_json::json!({"ok": false, "code": "unsupported_media_type", "detail": "Content-Type must be application/json"}),
        )
        .await?;
        return Ok(());
    }

    if buf.len() < body_offset + content_length {
        write_http_json(
            &mut stream,
            400,
            "Bad Request",
            &serde_json::json!({"ok": false, "code": "truncated_body", "detail": "incomplete request body"}),
        )
        .await?;
        return Ok(());
    }

    let payload: MailWakeBell =
        match serde_json::from_slice(&buf[body_offset..body_offset + content_length]) {
            Ok(p) => p,
            Err(e) => {
                write_http_json(
                &mut stream,
                400,
                "Bad Request",
                &serde_json::json!({"ok": false, "code": "invalid_json", "detail": e.to_string()}),
            )
            .await?;
                return Ok(());
            }
        };

    match client.call(payload).await {
        Ok(Ack) => {
            write_http_json(&mut stream, 200, "OK", &serde_json::json!({"ok": true})).await?;
        }
        Err(e) => {
            write_http_json(
                &mut stream,
                422,
                "Unprocessable Entity",
                &serde_json::json!({"ok": false, "code": "dispatch_failed", "detail": e.to_string()}),
            )
            .await?;
        }
    }
    Ok(())
}

async fn write_http_json(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &serde_json::Value,
) -> std::io::Result<()> {
    let payload = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;
    stream.shutdown().await
}

async fn deliver_bell_dm(
    client: &LocalClient,
    sandbox: &SandboxRoot,
    seat: SeatName,
    bell_id: String,
) -> Result<(), ApiError> {
    let digest16 = bell_digest16(&seat, &bell_id);
    let idem = IdemKey::parse(format!("bell:{digest16}"))?;
    let prompt = sandbox
        .read_bell_prompt(&digest16)
        .ok_or_else(|| ApiError::NotFound {
            what: format!("banked wake bell {bell_id} for @{seat}"),
        })?;
    let body = room::Body::parse(prompt)?;
    client
        .call(room::RoomPost {
            idem: Some(idem),
            ..room::RoomPost::new(
                room::RoomRef::Dm {
                    with: Principal::Seat { seat },
                },
                body,
            )
        })
        .await?;
    Ok(())
}

/// Delivers each rung bell's banked prompt, read from the sandbox it was
/// banked in, as a DM to its seat.
pub struct BellCourier {
    sandbox: SandboxRoot,
}

impl BellCourier {
    pub fn new(sandbox: SandboxRoot) -> Self {
        Self { sandbox }
    }
}

impl Reactor for BellCourier {
    const NAME: &'static str = "bell-courier";

    fn interest(&self) -> Interest {
        Interest::Streams {
            streams: vec![StreamId::Mail],
        }
    }

    fn react(
        &mut self,
        frame: Frame,
        client: &LocalClient,
    ) -> impl Future<Output = Result<(), ApiError>> + Send {
        let client = client.clone();
        let sandbox = self.sandbox.clone();
        async move {
            match frame {
                Frame::Committed(Envelope {
                    event: Event::Mail(MailEvent::BellRang { bell_id, seat }),
                    ..
                }) => deliver_bell_dm(&client, &sandbox, seat, bell_id).await,
                _ => Ok(()),
            }
        }
    }
}
