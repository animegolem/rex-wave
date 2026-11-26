//! Mail bridge shapes: `mailbox.item.schema.json` v1 projections, letters
//! promoted from rooms, and wake-bell ingress. NON-PROD sandbox only; the
//! sandbox guard lives in `rex-wave-mailbridge::SandboxRoot`.

use crate::id::{IdemKey, IncarnationId, SeatName, Seq, SeqRange, Sha256Hex, UnixMs};
use crate::room::RoomId;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::path::PathBuf;

/// `item` object of `mailbox.item.schema.json` schema 1 (grounding #11), verbatim field names.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailboxItem {
    pub schema: MailSchemaV1,
    pub id: String,
    pub from: String,
    pub to: String,
    pub kind: String,
    pub state: String,
    pub ts: String,
    pub ts_basis: String,
    #[serde(rename = "ref")]
    pub reference: MailRef,
    pub topic: Option<String>,
    pub round: Option<u32>,
    pub label: Option<String>,
}

/// `ref` object: the body stays at its source; we only point at exact bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailRef {
    pub store: String,
    pub path: PathBuf,
    pub sha256: Sha256Hex,
}

/// `wake` object of schema 1.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WakeRecord {
    pub schema: MailSchemaV1,
    pub wake_id: WakeId,
    pub event: String,
    pub to: String,
    pub ts: String,
    pub item: MailboxItem,
    pub count: u32,
    pub unknown: u32,
}

/// The literal `1`. Any other value fails deserialization at the boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct MailSchemaV1;
impl JsonSchema for MailSchemaV1 {
    fn inline_schema() -> bool {
        true
    }
    fn schema_name() -> Cow<'static, str> {
        "MailSchemaV1".into()
    }
    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({ "const": 1 })
    }
}
impl TryFrom<u8> for MailSchemaV1 {
    type Error = String;
    fn try_from(v: u8) -> Result<Self, String> {
        if v == 1 {
            Ok(MailSchemaV1)
        } else {
            Err(format!("unsupported mailbox schema {v}"))
        }
    }
}
impl From<MailSchemaV1> for u8 {
    fn from(_: MailSchemaV1) -> u8 {
        1
    }
}

/// First 16 hex of `sha256("wake/1\n" + seat + "\n" + box + "/" + file + "\n" + full_sha256)`.
/// Derived by `rex_wave_mailbridge::wake_id`, never accepted from callers.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct WakeId(pub String);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LetterPriority {
    Low,
    #[default]
    Normal,
    High,
    Blocking,
}

impl LetterPriority {
    pub const fn as_str(self) -> &'static str {
        match self {
            LetterPriority::Low => "low",
            LetterPriority::Normal => "normal",
            LetterPriority::High => "high",
            LetterPriority::Blocking => "blocking",
        }
    }
}

/// Where a dissolved content-addressed mail message entered the journal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MailOrigin {
    #[default]
    Send,
    Drop,
    Mirror,
    Import,
    Promote,
}

fn is_false(v: &bool) -> bool {
    !*v
}

/// Immutable content-addressed (`sha256`) mail message committed in `StreamId::Mail`
/// and projected into `RoomId::Direct` (1:1 DMs) or a multi-seat mail channel.
#[derive(Clone, Debug, PartialEq, Eq, JsonSchema)]
pub struct MailMessage {
    pub sha256: Sha256Hex,
    pub id: String,
    pub from: String,
    pub to: Vec<SeatName>,
    pub cc: Vec<SeatName>,
    pub subject: String,
    pub body: String,
    pub path: PathBuf,
    pub priority: LetterPriority,
    pub topic: Option<String>,
    pub in_reply_to: Option<String>,
    pub refs: Vec<String>,
    pub created_at: UnixMs,
    pub created_at_iso: String,
    pub receipt_only: bool,
    pub room: Option<RoomId>,
    pub raw_markdown: String,
    /// Optional base64 payload used only when an ingested legacy file contained
    /// non-UTF-8 bytes, guaranteeing 100% byte-for-byte `sha256` preservation.
    pub raw_bytes_b64: Option<String>,
    pub origin: MailOrigin,
}

#[derive(Serialize)]
struct WireMailMessageRef<'a> {
    sha256: &'a Sha256Hex,
    id: &'a str,
    from: &'a str,
    to: &'a [SeatName],
    #[serde(skip_serializing_if = "<[SeatName]>::is_empty")]
    cc: &'a [SeatName],
    subject: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body_suffix_len: Option<usize>,
    path: &'a PathBuf,
    priority: LetterPriority,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<&'a str>,
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    refs: &'a [String],
    created_at: UnixMs,
    created_at_iso: &'a str,
    #[serde(skip_serializing_if = "is_false")]
    receipt_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    room: Option<&'a RoomId>,
    #[serde(skip_serializing_if = "str::is_empty")]
    raw_markdown: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    raw_bytes_b64: Option<&'a str>,
    origin: MailOrigin,
}

#[derive(Deserialize)]
struct WireMailMessageOwned {
    sha256: Sha256Hex,
    id: String,
    from: String,
    to: Vec<SeatName>,
    #[serde(default)]
    cc: Vec<SeatName>,
    subject: String,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    body_suffix_len: Option<usize>,
    path: PathBuf,
    #[serde(default)]
    priority: LetterPriority,
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    in_reply_to: Option<String>,
    #[serde(default)]
    refs: Vec<String>,
    created_at: UnixMs,
    created_at_iso: String,
    #[serde(default)]
    receipt_only: bool,
    #[serde(default)]
    room: Option<RoomId>,
    #[serde(default)]
    raw_markdown: String,
    #[serde(default)]
    raw_bytes_b64: Option<String>,
    #[serde(default)]
    origin: MailOrigin,
}

impl Serialize for MailMessage {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let raw_md = if self.raw_bytes_b64.is_some() {
            ""
        } else {
            self.raw_markdown.as_str()
        };
        let (body, body_suffix_len) =
            if self.raw_bytes_b64.is_none() && raw_md.ends_with(&self.body) {
                (None, Some(self.body.len()))
            } else {
                (Some(self.body.as_str()), None)
            };
        WireMailMessageRef {
            sha256: &self.sha256,
            id: &self.id,
            from: &self.from,
            to: &self.to,
            cc: &self.cc,
            subject: &self.subject,
            body,
            body_suffix_len,
            path: &self.path,
            priority: self.priority,
            topic: self.topic.as_deref(),
            in_reply_to: self.in_reply_to.as_deref(),
            refs: &self.refs,
            created_at: self.created_at,
            created_at_iso: &self.created_at_iso,
            receipt_only: self.receipt_only,
            room: self.room.as_ref(),
            raw_markdown: raw_md,
            raw_bytes_b64: self.raw_bytes_b64.as_deref(),
            origin: self.origin,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MailMessage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let w = WireMailMessageOwned::deserialize(deserializer)?;
        let body = match w.body {
            Some(b) => b,
            None => match w.body_suffix_len {
                Some(len)
                    if len <= w.raw_markdown.len()
                        && w.raw_markdown.is_char_boundary(w.raw_markdown.len() - len) =>
                {
                    w.raw_markdown[w.raw_markdown.len() - len..].to_owned()
                }
                _ => String::new(),
            },
        };
        Ok(MailMessage {
            sha256: w.sha256,
            id: w.id,
            from: w.from,
            to: w.to,
            cc: w.cc,
            subject: w.subject,
            body,
            path: w.path,
            priority: w.priority,
            topic: w.topic,
            in_reply_to: w.in_reply_to,
            refs: w.refs,
            created_at: w.created_at,
            created_at_iso: w.created_at_iso,
            receipt_only: w.receipt_only,
            room: w.room,
            raw_markdown: w.raw_markdown,
            raw_bytes_b64: w.raw_bytes_b64,
            origin: w.origin,
        })
    }
}

/// A banked letter in the sandbox. Exists only after the bytes are on disk
/// (bank before announce, law I2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LetterRef {
    pub path: PathBuf,
    pub sha256: Sha256Hex,
    pub wake_id: WakeId,
    pub item: MailboxItem,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct MailPromote {
    pub room: RoomId,
    pub range: SeqRange,
    pub to: Vec<SeatName>,
    pub subject: String,
    pub topic: String,
    pub priority: LetterPriority,
}

/// Send a dissolved content-addressed (`sha256`) mail message. Commits durably
/// to SQLite FIRST, then projects into a 1:1 DM or multi-seat channel and
/// exports atomically to `<mirror>/objects/<sha256>.md` and `<mirror>/mail/<slug>.md`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct MailSend {
    pub to: Vec<SeatName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cc: Vec<SeatName>,
    pub subject: String,
    pub body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<SeatName>,
    #[serde(default)]
    pub priority: LetterPriority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refs: Vec<String>,
    #[serde(default)]
    pub wait_export: bool,
    #[serde(default, alias = "id", skip_serializing_if = "Option::is_none")]
    pub idem: Option<IdemKey>,
}

/// Delivery notice reported to a sender when a recipient seat is unregistered,
/// dormant, retired, or currently offline.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailDeliveryNotice {
    pub seat: SeatName,
    pub status: String,
    pub notice: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailSent {
    pub sha256: Sha256Hex,
    pub id: String,
    pub path: PathBuf,
    pub from: String,
    pub to: Vec<SeatName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cc: Vec<SeatName>,
    pub subject: String,
    pub created_at: UnixMs,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room: Option<RoomId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room_seq: Option<Seq>,
    pub exported: bool,
    pub replayed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub delivery_notices: Vec<MailDeliveryNotice>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct MailCheck {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seat: Option<SeatName>,
    #[serde(default)]
    pub include_all: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailSummary {
    pub sha256: Sha256Hex,
    pub sha16: String,
    pub id: String,
    pub from: String,
    pub to: Vec<SeatName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cc: Vec<SeatName>,
    pub subject: String,
    pub path: PathBuf,
    pub priority: LetterPriority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    pub created_at: UnixMs,
    pub acked: bool,
    pub mutated_after_ack: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_ack_sha: Option<String>,
    pub receipt_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room: Option<RoomId>,
    pub snippet: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailCheckView {
    pub seat: SeatName,
    pub unread_count: usize,
    pub total_count: usize,
    pub truncated: bool,
    pub messages: Vec<MailSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub delivery_notices: Vec<MailDeliveryNotice>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailAck {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<Sha256Hex>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seat: Option<SeatName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl MailAck {
    /// Legacy sandbox convenience constructor (`path` + exact `sha256`).
    pub fn for_path_and_sha(path: PathBuf, sha256: Sha256Hex) -> Self {
        Self {
            path: Some(path),
            sha256: Some(sha256),
            seat: None,
            note: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailAcked {
    pub seat: SeatName,
    pub sha256: Sha256Hex,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    pub already_acked: bool,
    pub exported: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct MailSearch {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seat: Option<SeatName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailSearchHit {
    pub sha256: Sha256Hex,
    pub sha16: String,
    pub id: String,
    pub from: String,
    pub to: Vec<SeatName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cc: Vec<SeatName>,
    pub subject: String,
    pub path: PathBuf,
    pub created_at: UnixMs,
    pub acked: bool,
    pub mutated_after_ack: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room: Option<RoomId>,
    pub snippet: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailSearchView {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seat: Option<SeatName>,
    pub hits: Vec<MailSearchHit>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct MailReconcile {}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailReconcileReport {
    pub blobs_exported: usize,
    pub working_tree_exported: usize,
    pub acks_exported: usize,
    pub files_ingested: usize,
    pub acks_ingested: usize,
    pub mutated_after_ack_flagged: usize,
    pub drop_files_consumed: usize,
}

/// Matches wake-bell `host/sink.ts` `Sink.deliver` payload (entry id, seat, prompt).
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct MailWakeBell {
    pub bell_id: String,
    pub seat: SeatName,
    pub prompt: String,
    pub fired_at: String,
}

/// Outcome reported by a `couriers.d/` output adapter (`MailMachines.tla`).
/// `Unknown` counts as rung (`counts_as_rung() == true`) so the same `(seat, sha256)`
/// is never rung twice after an ambiguous timeout or non-zero exit.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CourierOutcome {
    Rung,
    Deferred,
    Refused,
    Unknown,
}

impl CourierOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            CourierOutcome::Rung => "rung",
            CourierOutcome::Deferred => "deferred",
            CourierOutcome::Refused => "refused",
            CourierOutcome::Unknown => "unknown",
        }
    }

    pub const fn counts_as_rung(self) -> bool {
        matches!(self, CourierOutcome::Rung | CourierOutcome::Unknown)
    }
}

/// Classification for loud mail delivery failures escalated to `hub_owner_seat` and sender.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryFailureKind {
    Refused,
    GiveUp,
    AdapterDegraded,
}

impl DeliveryFailureKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            DeliveryFailureKind::Refused => "refused",
            DeliveryFailureKind::GiveUp => "give_up",
            DeliveryFailureKind::AdapterDegraded => "adapter_degraded",
        }
    }
}

/// Durable record of a loud delivery failure or courier degradation surfaced passively in
/// `ConfigSnapshot` / `⚙ Settings` and reported to `hub_owner_seat` and sender.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailDeliveryFailure {
    pub seq: Seq,
    pub at: UnixMs,
    pub kind: DeliveryFailureKind,
    pub courier: String,
    pub seat: SeatName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incarnation_id: Option<IncarnationId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sha256s: Vec<Sha256Hex>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_seat: Option<SeatName>,
    pub hub_owner_seat: SeatName,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// System command issued by `SystemPart::CourierHub` to record a courier ring outcome
/// and optional loud delivery failure in `StreamId::Mail`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct MailRecordRing {
    pub courier: String,
    pub seat: SeatName,
    pub machine_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incarnation_id: Option<IncarnationId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sha256s: Vec<Sha256Hex>,
    #[serde(default)]
    pub is_canary: bool,
    pub outcome: CourierOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_seat: Option<SeatName>,
    pub hub_owner_seat: SeatName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_kind: Option<DeliveryFailureKind>,
}

/// Reply returned by [`MailRecordRing`] reporting the authoritative post-commit
/// consecutive-unknown streak and degradation state for the courier.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailRingRecorded {
    pub consecutive_unknown: u32,
    pub degraded: bool,
    pub degraded_newly_triggered: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MailEvent {
    /// Written only after the file is durably on disk.
    Banked {
        from_room: RoomId,
        range: (Seq, Seq),
        letter: LetterRef,
    },
    Acked {
        path: PathBuf,
        sha256: Sha256Hex,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        seat: Option<SeatName>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
    Projected {
        item: MailboxItem,
    },
    BellRang {
        bell_id: String,
        seat: SeatName,
    },
    /// Content-addressed mail message committed durably to SQLite (single commit point)
    /// prior to atomic Markdown mirror export.
    MessageCommitted {
        message: Box<MailMessage>,
        #[serde(default, skip_serializing_if = "is_false")]
        mutated_after_ack: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prior_ack_sha: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        send_idem: Option<IdemKey>,
    },
    /// Records additional mailbox-derived recipients discovered for an already-committed
    /// content-addressed message (e.g. a second `to-<seat>/` copy arriving on a later pass).
    RecipientsAdded {
        sha256: Sha256Hex,
        to: Vec<SeatName>,
    },
    /// Records that `path` in the working tree now holds already-committed revision `sha256`
    /// (e.g. when revision A is restored over revision B at the same path).
    PathHeadUpdated {
        path: PathBuf,
        sha256: Sha256Hex,
    },
    /// Durable courier ring attempt keyed by `(seat, sha256)` (`MailMachines.tla`).
    RingAttempted {
        courier: String,
        seat: SeatName,
        machine_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        incarnation_id: Option<IncarnationId>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        sha256s: Vec<Sha256Hex>,
        #[serde(default, skip_serializing_if = "is_false")]
        is_canary: bool,
        outcome: CourierOutcome,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
    /// Loud mail delivery failure (`refused`, `give_up`, or `adapter_degraded`).
    DeliveryFailed {
        kind: DeliveryFailureKind,
        courier: String,
        seat: SeatName,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        incarnation_id: Option<IncarnationId>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        sha256s: Vec<Sha256Hex>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sender_seat: Option<SeatName>,
        hub_owner_seat: SeatName,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}
