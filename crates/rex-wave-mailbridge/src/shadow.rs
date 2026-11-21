//! Structurally read-only interchange reader (`ReadOnlyInterchange`) and pure
//! mail-ring decision engine (`decide_mail_ring`, `decide_mail_rings_for_seat`,
//! `run_shadow_pass`). Zero write paths exist in this module.

use crate::acks::{AckIndex, AcksParseReport, AliasMap, parse_acks_jsonl};
use crate::presence::{
    IncarnationAffinity, PresenceState, select_binding_for_delivery, sender_delivery_notice,
};
use crate::{SandboxError, sha256_bytes, unquote_yaml};
use rex_wave_api::presence::*;
use rex_wave_api::*;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

/// Default number of queued letters per seat above which rings coalesce into
/// one [`RingDecision::RingDigest`].
pub const DEFAULT_DIGEST_THRESHOLD: usize = 3;

/// Default wall-clock retry window (`60 min`) measured from `first_seen_at`,
/// never from attempt/pass count.
pub const DEFAULT_RETRY_DEADLINE_MS: u64 = 3_600_000;

/// Default reconnect grace window (`15 s`) matching `rex-wave-notify`.
pub const DEFAULT_RECONNECT_GRACE_MS: u64 = 15_000;

/// Structurally read-only handle over an interchange directory. Holds no
/// `StreamWriter`, cursor file, or ring transport, and exposes only `read_dir`
/// and `fs::read` operations so `--shadow-read-only` and dry runs cannot mutate state.
#[derive(Clone, Debug)]
pub struct ReadOnlyInterchange {
    root: PathBuf,
}

impl ReadOnlyInterchange {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, SandboxError> {
        let canon = fs::canonicalize(root.as_ref()).map_err(|e| SandboxError::Io(e.to_string()))?;
        let meta = fs::metadata(&canon).map_err(|e| SandboxError::Io(e.to_string()))?;
        if !meta.is_dir() {
            return Err(SandboxError::Io(format!(
                "{} is not a directory",
                canon.display()
            )));
        }
        Ok(Self { root: canon })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Reads and tolerantly parses `<root>/state/acks.jsonl` without modifying
    /// or creating any file.
    pub fn read_acks(&self, aliases: &AliasMap) -> Result<AcksParseReport, SandboxError> {
        let acks_path = self.root.join("state").join("acks.jsonl");
        let meta = match fs::symlink_metadata(&acks_path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(AcksParseReport::default());
            }
            Err(e) => return Err(SandboxError::Io(e.to_string())),
        };
        if meta.file_type().is_symlink() || !meta.file_type().is_file() {
            return Err(SandboxError::Io(format!(
                "{} is not a regular file",
                acks_path.display()
            )));
        }
        let bytes = fs::read(&acks_path).map_err(|e| SandboxError::Io(e.to_string()))?;
        let text = String::from_utf8_lossy(&bytes);
        Ok(parse_acks_jsonl(&text, aliases))
    }

    /// Scans `mail/*.md`, `to-*/*.md`, and root-level letter `*.md` files using
    /// only `read_dir` and `fs::read`.
    pub fn scan_letters(&self, aliases: &AliasMap) -> Result<Vec<ScannedLetter>, SandboxError> {
        let mut letters = Vec::new();
        let top = fs::read_dir(&self.root).map_err(|e| SandboxError::Io(e.to_string()))?;
        let mut dirs: Vec<(String, PathBuf)> = Vec::new();
        let mut root_files: Vec<(String, PathBuf)> = Vec::new();

        for entry in top.flatten() {
            let fname = entry.file_name();
            let Some(name) = fname.to_str() else {
                continue;
            };
            if name.starts_with('.') {
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
            } else if ft.is_file()
                && name.ends_with(".md")
                && !matches!(name, "README.md" | "chatter.md")
            {
                root_files.push((name.to_owned(), entry.path()));
            }
        }
        dirs.sort_by(|a, b| a.0.cmp(&b.0));
        root_files.sort_by(|a, b| a.0.cmp(&b.0));

        for (name, path) in root_files {
            if let Ok(letter) = read_scanned_letter(&path, &name, None, aliases) {
                letters.push(letter);
            }
        }

        for (dir_name, dir_path) in dirs {
            let box_seat = dir_name
                .strip_prefix("to-")
                .and_then(|raw| aliases.canonical_seat_name(raw));
            let Ok(entries) = fs::read_dir(&dir_path) else {
                continue;
            };
            let mut files: Vec<(String, PathBuf)> = Vec::new();
            for f in entries.flatten() {
                let fname = f.file_name();
                let Some(name) = fname.to_str() else {
                    continue;
                };
                if name.starts_with('.') || !name.ends_with(".md") {
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
                let rel = format!("{dir_name}/{fname}");
                if let Ok(letter) = read_scanned_letter(&fpath, &rel, box_seat.as_ref(), aliases) {
                    letters.push(letter);
                }
            }
        }

        letters.sort_by(|a, b| (&a.rel_path, &a.sha256).cmp(&(&b.rel_path, &b.sha256)));
        Ok(letters)
    }
}

/// Parsed letter discovered on disk by [`ReadOnlyInterchange::scan_letters`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScannedLetter {
    pub rel_path: String,
    pub sha256: Sha256Hex,
    pub sha16: String,
    pub from: Option<String>,
    pub recipients: Vec<SeatName>,
    pub subject: Option<String>,
    pub letter_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub receipt_only: bool,
    pub seen_at: UnixMs,
}

impl ScannedLetter {
    /// Expands this letter into one [`LetterCandidate`] per canonical recipient seat.
    pub fn into_candidates(&self) -> Vec<LetterCandidate> {
        self.recipients
            .iter()
            .map(|seat| LetterCandidate {
                rel_path: self.rel_path.clone(),
                sha256: self.sha256.clone(),
                sha16: self.sha16.clone(),
                from: self.from.clone(),
                to: seat.clone(),
                subject: self.subject.clone(),
                letter_id: self.letter_id.clone(),
                in_reply_to: self.in_reply_to.clone(),
                receipt_only: self.receipt_only,
                seen_at: self.seen_at,
            })
            .collect()
    }
}

#[derive(Default)]
struct ShadowFrontmatter {
    from: Option<String>,
    to: Vec<String>,
    cc: Vec<String>,
    subject: Option<String>,
    id: Option<String>,
    in_reply_to: Option<String>,
    receipt_only: bool,
}

fn parse_recipient_list(val: &str) -> Vec<String> {
    let trimmed = val.trim().trim_start_matches('[').trim_end_matches(']');
    trimmed
        .split(',')
        .map(unquote_yaml)
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect()
}

fn is_receipt_only_status(val: &str) -> bool {
    let lower = val.to_ascii_lowercase();
    lower.contains("receipt only")
        || lower.contains("receipt-only")
        || lower.contains("no return ack")
        || lower.contains("no ack requested")
        || lower.contains("no-ack")
}

fn parse_shadow_frontmatter(text: &str) -> ShadowFrontmatter {
    let mut fm = ShadowFrontmatter::default();
    let rest = if let Some(r) = text.strip_prefix("---\n") {
        r
    } else if let Some(r) = text.strip_prefix("---\r\n") {
        r
    } else {
        return fm;
    };

    for line in rest.lines() {
        let trimmed = line.trim_end_matches('\r');
        if trimmed == "---" {
            break;
        }
        let Some((k, v)) = trimmed.split_once(':') else {
            continue;
        };
        let key = k.trim().to_ascii_lowercase();
        let val = unquote_yaml(v);
        if val.is_empty() {
            continue;
        }
        match key.as_str() {
            "from" | "author" => {
                if fm.from.is_none() || key == "from" {
                    fm.from = Some(val);
                }
            }
            "to" => fm.to.extend(parse_recipient_list(&val)),
            "cc" => fm.cc.extend(parse_recipient_list(&val)),
            "subject" => fm.subject = Some(val),
            "id" => fm.id = Some(val),
            "in_reply_to" => fm.in_reply_to = Some(val),
            "receipt_only" | "receipt-only" | "no_ack" | "no-ack" => {
                if matches!(val.to_ascii_lowercase().as_str(), "true" | "yes" | "1") {
                    fm.receipt_only = true;
                }
            }
            "ack_required" | "return_ack" => {
                if matches!(
                    val.to_ascii_lowercase().as_str(),
                    "false" | "no" | "0" | "none"
                ) {
                    fm.receipt_only = true;
                }
            }
            "ack" => {
                if matches!(
                    val.to_ascii_lowercase().as_str(),
                    "none" | "false" | "no" | "receipt-only" | "receipt_only"
                ) {
                    fm.receipt_only = true;
                }
            }
            "kind" => {
                if matches!(
                    val.to_ascii_lowercase().as_str(),
                    "receipt" | "receipt-only"
                ) {
                    fm.receipt_only = true;
                }
            }
            "status" => {
                if is_receipt_only_status(&val) {
                    fm.receipt_only = true;
                }
            }
            _ => {}
        }
    }
    fm
}

fn read_scanned_letter(
    path: &Path,
    rel_path: &str,
    box_seat: Option<&SeatName>,
    aliases: &AliasMap,
) -> Result<ScannedLetter, SandboxError> {
    let bytes = fs::read(path).map_err(|e| SandboxError::Io(e.to_string()))?;
    let sha256 = sha256_bytes(&bytes);
    let sha16 = sha256.as_str()[..16].to_owned();
    let seen_at = fs::metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| UnixMs(d.as_millis() as u64))
        .unwrap_or(UnixMs(0));

    let text = String::from_utf8_lossy(&bytes);
    let fm = parse_shadow_frontmatter(&text);

    let mut recipients = BTreeSet::new();
    if let Some(seat) = box_seat {
        recipients.insert(seat.clone());
    } else {
        for raw in fm.to.iter().chain(fm.cc.iter()) {
            if let Some(seat) = aliases.canonical_seat_name(raw) {
                recipients.insert(seat);
            }
        }
    }

    Ok(ScannedLetter {
        rel_path: rel_path.to_owned(),
        sha256,
        sha16,
        from: fm.from,
        recipients: recipients.into_iter().collect(),
        subject: fm.subject,
        letter_id: fm.id,
        in_reply_to: fm.in_reply_to,
        receipt_only: fm.receipt_only,
        seen_at,
    })
}

/// Pure input describing one banked letter candidate for one recipient seat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LetterCandidate {
    pub rel_path: String,
    pub sha256: Sha256Hex,
    pub sha16: String,
    pub from: Option<String>,
    pub to: SeatName,
    pub subject: Option<String>,
    pub letter_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub receipt_only: bool,
    pub seen_at: UnixMs,
}

/// Branded mandatory `(path, sha16)` referent required on every ring decision.
/// A ring without both a concrete letter path and a 16-hex SHA-256 prefix is
/// unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RingReferent {
    path: String,
    sha16: String,
    sha256: Sha256Hex,
}

impl RingReferent {
    pub fn new(path: impl Into<String>, sha256: Sha256Hex) -> Result<Self, ApiError> {
        let path = path.into();
        let trimmed = path.trim();
        if trimmed.is_empty() || trimmed.chars().any(char::is_control) {
            return Err(ApiError::Invalid(InvalidId {
                what: "ring referent".into(),
                reason: "ring must carry a non-empty letter path".into(),
            }));
        }
        let sha16 = sha256.as_str()[..16].to_owned();
        Ok(Self {
            path: trimmed.to_owned(),
            sha16,
            sha256,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn sha16(&self) -> &str {
        &self.sha16
    }

    pub fn sha256(&self) -> &Sha256Hex {
        &self.sha256
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "path": self.path,
            "sha16": self.sha16,
            "sha256": self.sha256.as_str(),
        })
    }
}

/// Non-intrusive delivery channel selected for a live seat binding. Raw pane
/// keystroke injection is intentionally absent so rings can never collide with
/// a live turn (`ring-types-into-a-live-turn`).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RingDeliveryChannel {
    ClaudeMessagingSocket { pid: u32, socket: PathBuf },
    BbQueueIfActive { thread_id: BbThreadId },
    T3QueueIfActive { thread_id: T3ThreadId },
    UdsFramePush { conn_id: u64 },
    SelfWatcherLease { harness: String },
    OperatorToast { command: String },
}

impl RingDeliveryChannel {
    pub fn from_endpoint(endpoint: &TransportEndpoint) -> Self {
        match endpoint {
            TransportEndpoint::ClaudeSession {
                pid,
                messaging_socket,
                ..
            } => RingDeliveryChannel::ClaudeMessagingSocket {
                pid: *pid,
                socket: messaging_socket.clone(),
            },
            TransportEndpoint::BbThread { thread_id } => RingDeliveryChannel::BbQueueIfActive {
                thread_id: thread_id.clone(),
            },
            TransportEndpoint::T3Thread { thread_id } => RingDeliveryChannel::T3QueueIfActive {
                thread_id: thread_id.clone(),
            },
            TransportEndpoint::UdsSession { conn_id, .. } => {
                RingDeliveryChannel::UdsFramePush { conn_id: *conn_id }
            }
            TransportEndpoint::SelfWatcher { harness, .. } => {
                RingDeliveryChannel::SelfWatcherLease {
                    harness: harness.clone(),
                }
            }
            TransportEndpoint::OperatorToast { command } => RingDeliveryChannel::OperatorToast {
                command: command.clone(),
            },
        }
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            RingDeliveryChannel::ClaudeMessagingSocket { .. } => "claude_messaging_socket",
            RingDeliveryChannel::BbQueueIfActive { .. } => "bb_queue_if_active",
            RingDeliveryChannel::T3QueueIfActive { .. } => "t3_queue_if_active",
            RingDeliveryChannel::UdsFramePush { .. } => "uds_frame_push",
            RingDeliveryChannel::SelfWatcherLease { .. } => "self_watcher_lease",
            RingDeliveryChannel::OperatorToast { .. } => "operator_toast",
        }
    }
}

/// Wall-clock age tracker for deferred letters. Expiry depends solely on
/// `first_seen_at + deadline_ms`, never on `pass_count`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryEntry {
    pub first_seen_at: UnixMs,
    pub deadline_ms: u64,
    pub target_busy: bool,
    /// Diagnostic count of scheduler passes observed; never triggers give-up.
    pub pass_count: u32,
}

impl RetryEntry {
    pub fn deadline_at(&self) -> UnixMs {
        UnixMs(self.first_seen_at.0.saturating_add(self.deadline_ms))
    }

    pub fn is_expired(&self, now: UnixMs) -> bool {
        now.0 >= self.deadline_at().0
    }
}

#[derive(Clone, Debug, Default)]
pub struct RetryState {
    entries: BTreeMap<(SeatName, Sha256Hex), RetryEntry>,
}

impl RetryState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, seat: SeatName, sha256: Sha256Hex, entry: RetryEntry) {
        self.entries.insert((seat, sha256), entry);
    }

    pub fn get(&self, seat: &SeatName, sha256: &Sha256Hex) -> Option<&RetryEntry> {
        self.entries.get(&(seat.clone(), sha256.clone()))
    }
}

/// Configurable thresholds for [`decide_mail_ring`] and [`decide_mail_rings_for_seat`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingPolicy {
    pub digest_threshold: usize,
    pub retry_deadline_ms: u64,
    pub reconnect_grace_ms: u64,
}

impl Default for RingPolicy {
    fn default() -> Self {
        Self {
            digest_threshold: DEFAULT_DIGEST_THRESHOLD,
            retry_deadline_ms: DEFAULT_RETRY_DEADLINE_MS,
            reconnect_grace_ms: DEFAULT_RECONNECT_GRACE_MS,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeferReason {
    ReconnectGrace { ready_at: UnixMs },
    TargetBusy { pass_count: u32 },
    TransportDown { transport: TransportKind },
}

/// Pure output of the mail-ring decision engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RingDecision {
    SkipAlreadyAcked {
        seat: SeatName,
        sha256: Sha256Hex,
    },
    SkipReceiptOnly {
        seat: SeatName,
        referent: RingReferent,
        notice: String,
    },
    SkipDormantOrRetired {
        seat: SeatName,
        status: SeatStatus,
        referent: RingReferent,
    },
    FlagMutatedAfterAck {
        seat: SeatName,
        referent: RingReferent,
        prior_ack_sha: String,
    },
    Ring {
        seat: SeatName,
        incarnation_id: IncarnationId,
        channel: RingDeliveryChannel,
        referent: RingReferent,
    },
    RingDigest {
        seat: SeatName,
        incarnation_id: IncarnationId,
        channel: RingDeliveryChannel,
        referents: Vec<RingReferent>,
    },
    Defer {
        seat: SeatName,
        referent: RingReferent,
        reason: DeferReason,
        deadline_at: UnixMs,
    },
    RaiseUnrung {
        seat: SeatName,
        referents: Vec<RingReferent>,
        oldest_at: UnixMs,
        sender_notice: String,
    },
    RefuseMissingReferent {
        seat: SeatName,
        reason: String,
    },
}

impl RingDecision {
    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            RingDecision::SkipAlreadyAcked { seat, sha256 } => serde_json::json!({
                "decision": "skip_already_acked",
                "seat": seat.as_str(),
                "sha256": sha256.as_str(),
            }),
            RingDecision::SkipReceiptOnly {
                seat,
                referent,
                notice,
            } => serde_json::json!({
                "decision": "skip_receipt_only",
                "seat": seat.as_str(),
                "referent": referent.to_json_value(),
                "notice": notice,
            }),
            RingDecision::SkipDormantOrRetired {
                seat,
                status,
                referent,
            } => serde_json::json!({
                "decision": "skip_dormant_or_retired",
                "seat": seat.as_str(),
                "status": format!("{status:?}").to_ascii_lowercase(),
                "referent": referent.to_json_value(),
            }),
            RingDecision::FlagMutatedAfterAck {
                seat,
                referent,
                prior_ack_sha,
            } => serde_json::json!({
                "decision": "flag_mutated_after_ack",
                "seat": seat.as_str(),
                "referent": referent.to_json_value(),
                "prior_ack_sha": prior_ack_sha,
            }),
            RingDecision::Ring {
                seat,
                incarnation_id,
                channel,
                referent,
            } => serde_json::json!({
                "decision": "ring",
                "seat": seat.as_str(),
                "incarnation_id": incarnation_id.as_str(),
                "channel": channel.kind_name(),
                "referent": referent.to_json_value(),
            }),
            RingDecision::RingDigest {
                seat,
                incarnation_id,
                channel,
                referents,
            } => serde_json::json!({
                "decision": "ring_digest",
                "seat": seat.as_str(),
                "incarnation_id": incarnation_id.as_str(),
                "channel": channel.kind_name(),
                "count": referents.len(),
                "referents": referents.iter().map(RingReferent::to_json_value).collect::<Vec<_>>(),
            }),
            RingDecision::Defer {
                seat,
                referent,
                reason,
                deadline_at,
            } => serde_json::json!({
                "decision": "defer",
                "seat": seat.as_str(),
                "referent": referent.to_json_value(),
                "reason": format!("{reason:?}"),
                "deadline_at_ms": deadline_at.0,
            }),
            RingDecision::RaiseUnrung {
                seat,
                referents,
                oldest_at,
                sender_notice,
            } => serde_json::json!({
                "decision": "raise_unrung",
                "seat": seat.as_str(),
                "count": referents.len(),
                "oldest_at_ms": oldest_at.0,
                "sender_notice": sender_notice,
                "referents": referents.iter().map(RingReferent::to_json_value).collect::<Vec<_>>(),
            }),
            RingDecision::RefuseMissingReferent { seat, reason } => serde_json::json!({
                "decision": "refuse_missing_referent",
                "seat": seat.as_str(),
                "reason": reason,
            }),
        }
    }
}

/// Pure single-letter mail-ring policy function.
#[allow(clippy::too_many_arguments)]
pub fn decide_mail_ring(
    letter: &LetterCandidate,
    ack_index: &AckIndex,
    presence: &PresenceState,
    affinity: &IncarnationAffinity,
    retry_state: &RetryState,
    policy: &RingPolicy,
    aliases: &AliasMap,
    now: UnixMs,
) -> RingDecision {
    let referent = match RingReferent::new(&letter.rel_path, letter.sha256.clone()) {
        Ok(r) => r,
        Err(e) => {
            return RingDecision::RefuseMissingReferent {
                seat: letter.to.clone(),
                reason: e.to_string(),
            };
        }
    };

    if ack_index.is_acked_by_digest(letter.to.as_str(), &letter.sha256, aliases) {
        return RingDecision::SkipAlreadyAcked {
            seat: letter.to.clone(),
            sha256: letter.sha256.clone(),
        };
    }

    if let Some(prior_ack_sha) =
        ack_index.mutated_after_ack(letter.to.as_str(), referent.path(), &letter.sha256, aliases)
    {
        return RingDecision::FlagMutatedAfterAck {
            seat: letter.to.clone(),
            referent,
            prior_ack_sha,
        };
    }

    if letter.receipt_only {
        return RingDecision::SkipReceiptOnly {
            seat: letter.to.clone(),
            notice: format!(
                "receipt-only letter {} ({}) requires no return ACK",
                referent.path(),
                referent.sha16()
            ),
            referent,
        };
    }

    if let Some(record) = presence.seat(&letter.to)
        && matches!(record.status, SeatStatus::Dormant | SeatStatus::Retired)
    {
        return RingDecision::SkipDormantOrRetired {
            seat: letter.to.clone(),
            status: record.status,
            referent,
        };
    }

    let retry = retry_state.get(&letter.to, &letter.sha256);

    if let Some(binding) = select_binding_for_delivery(
        presence,
        &letter.to,
        None,
        letter.in_reply_to.as_deref(),
        affinity,
        now,
    ) {
        if let Some(entry) = retry
            && entry.target_busy
        {
            if !entry.is_expired(now) {
                return RingDecision::Defer {
                    seat: letter.to.clone(),
                    referent,
                    reason: DeferReason::TargetBusy {
                        pass_count: entry.pass_count,
                    },
                    deadline_at: entry.deadline_at(),
                };
            }
            let notice = format!(
                "banked; retry deadline elapsed for {} while target remained busy",
                letter.to
            );
            return RingDecision::RaiseUnrung {
                seat: letter.to.clone(),
                oldest_at: letter.seen_at,
                referents: vec![referent],
                sender_notice: notice,
            };
        }

        return RingDecision::Ring {
            seat: letter.to.clone(),
            incarnation_id: binding.incarnation_id.clone(),
            channel: RingDeliveryChannel::from_endpoint(&binding.endpoint),
            referent,
        };
    }

    // No live binding: check TransportDown circuit-breaker first.
    if let Some(down_binding) = presence
        .bindings_for(&letter.to)
        .into_iter()
        .find(|b| b.status == BindingStatus::TransportDown)
    {
        let fallback_entry = RetryEntry {
            first_seen_at: letter.seen_at,
            deadline_ms: policy.retry_deadline_ms,
            target_busy: false,
            pass_count: 0,
        };
        let entry = retry.unwrap_or(&fallback_entry);
        if !entry.is_expired(now) {
            return RingDecision::Defer {
                seat: letter.to.clone(),
                referent,
                reason: DeferReason::TransportDown {
                    transport: down_binding.endpoint.kind(),
                },
                deadline_at: entry.deadline_at(),
            };
        }
    }

    // Reconnect grace window (`15 s`) for a recently dropped reconnectable binding;
    // `0 s` when the seat was never bound.
    if presence.was_ever_bound(&letter.to)
        && let Some((dropped_at, true)) = presence.last_drop_info(&letter.to)
    {
        let ready_at = UnixMs(dropped_at.0.saturating_add(policy.reconnect_grace_ms));
        if now < ready_at {
            return RingDecision::Defer {
                seat: letter.to.clone(),
                referent,
                reason: DeferReason::ReconnectGrace { ready_at },
                deadline_at: ready_at,
            };
        }
    }

    let sender_notice = sender_delivery_notice(presence, &letter.to, now).unwrap_or_else(|| {
        format!(
            "banked; no live binding for {} since never; will ring on rebind",
            letter.to
        )
    });
    RingDecision::RaiseUnrung {
        seat: letter.to.clone(),
        oldest_at: letter.seen_at,
        referents: vec![referent],
        sender_notice,
    }
}

/// Evaluates all queued letters for one seat, deduplicating identical
/// `(seat, sha256)` copies across `mail/` and `to-<seat>/`, batching `>3`
/// live rings into one [`RingDecision::RingDigest`], and coalescing offline
/// alerts into one per-seat [`RingDecision::RaiseUnrung`].
#[allow(clippy::too_many_arguments)]
pub fn decide_mail_rings_for_seat(
    seat: &SeatName,
    letters: &[LetterCandidate],
    ack_index: &AckIndex,
    presence: &PresenceState,
    affinity: &IncarnationAffinity,
    retry_state: &RetryState,
    policy: &RingPolicy,
    aliases: &AliasMap,
    now: UnixMs,
) -> Vec<RingDecision> {
    let mut seen_shas: BTreeSet<Sha256Hex> = BTreeSet::new();
    let mut out = Vec::new();
    let mut rings_by_target: BTreeMap<(IncarnationId, RingDeliveryChannel), Vec<RingReferent>> =
        BTreeMap::new();
    let mut unrung_referents: Vec<RingReferent> = Vec::new();
    let mut unrung_oldest: Option<UnixMs> = None;
    let mut unrung_notice: Option<String> = None;

    for letter in letters {
        if &letter.to != seat {
            continue;
        }
        // Deduplicate multiple path copies (`mail/` + `to-<seat>/`) by `(seat, sha256)`.
        if !seen_shas.insert(letter.sha256.clone()) {
            continue;
        }
        match decide_mail_ring(
            letter,
            ack_index,
            presence,
            affinity,
            retry_state,
            policy,
            aliases,
            now,
        ) {
            RingDecision::Ring {
                incarnation_id,
                channel,
                referent,
                ..
            } => {
                rings_by_target
                    .entry((incarnation_id, channel))
                    .or_default()
                    .push(referent);
            }
            RingDecision::RaiseUnrung {
                referents,
                oldest_at,
                sender_notice,
                ..
            } => {
                unrung_oldest = Some(match unrung_oldest {
                    Some(prev) => prev.min(oldest_at),
                    None => oldest_at,
                });
                unrung_notice.get_or_insert(sender_notice);
                unrung_referents.extend(referents);
            }
            other => out.push(other),
        }
    }

    for ((incarnation_id, channel), referents) in rings_by_target {
        if referents.len() > policy.digest_threshold {
            out.push(RingDecision::RingDigest {
                seat: seat.clone(),
                incarnation_id,
                channel,
                referents,
            });
        } else {
            for referent in referents {
                out.push(RingDecision::Ring {
                    seat: seat.clone(),
                    incarnation_id: incarnation_id.clone(),
                    channel: channel.clone(),
                    referent,
                });
            }
        }
    }

    if !unrung_referents.is_empty() {
        out.push(RingDecision::RaiseUnrung {
            seat: seat.clone(),
            referents: unrung_referents,
            oldest_at: unrung_oldest.unwrap_or(now),
            sender_notice: unrung_notice.unwrap_or_default(),
        });
    }

    out
}

/// Complete read-only shadow pass output.
#[derive(Clone, Debug)]
pub struct ShadowPassReport {
    pub letters_scanned: usize,
    pub acks_report: AcksParseReport,
    pub decisions: Vec<RingDecision>,
}

impl ShadowPassReport {
    /// Formats every shadow decision as one JSONL line for `--shadow-read-only` output.
    pub fn to_jsonl(&self) -> String {
        let mut out = String::new();
        for d in &self.decisions {
            if let Ok(line) = serde_json::to_string(&d.to_json_value()) {
                out.push_str(&line);
                out.push('\n');
            }
        }
        out
    }
}

/// Runs one structurally read-only shadow pass over `interchange`. Never writes
/// to `interchange`, never mutates any cursor file, and never invokes a ring transport.
#[allow(clippy::too_many_arguments)]
pub fn run_shadow_pass(
    interchange: &ReadOnlyInterchange,
    presence: &PresenceState,
    affinity: &IncarnationAffinity,
    retry_state: &RetryState,
    policy: &RingPolicy,
    aliases: &AliasMap,
    now: UnixMs,
) -> Result<ShadowPassReport, SandboxError> {
    let acks_report = interchange.read_acks(aliases)?;
    let ack_index = acks_report.index();
    let scanned = interchange.scan_letters(aliases)?;
    let letters_scanned = scanned.len();

    let mut by_seat: BTreeMap<SeatName, Vec<LetterCandidate>> = BTreeMap::new();
    for letter in &scanned {
        for cand in letter.into_candidates() {
            by_seat.entry(cand.to.clone()).or_default().push(cand);
        }
    }

    let mut decisions = Vec::new();
    for (seat, candidates) in &by_seat {
        decisions.extend(decide_mail_rings_for_seat(
            seat,
            candidates,
            &ack_index,
            presence,
            affinity,
            retry_state,
            policy,
            aliases,
            now,
        ));
    }

    Ok(ShadowPassReport {
        letters_scanned,
        acks_report,
        decisions,
    })
}
