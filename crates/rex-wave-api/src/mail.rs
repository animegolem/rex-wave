//! Mail bridge shapes: `mailbox.item.schema.json` v1 projections, letters
//! promoted from rooms, and wake-bell ingress. NON-PROD sandbox only; the
//! sandbox guard lives in `rex-wave-mailbridge::SandboxRoot`.

use crate::id::{IdemKey, SeatName, Seq, SeqRange, Sha256Hex, UnixMs};
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailMessage {
    pub sha256: Sha256Hex,
    pub id: String,
    pub from: String,
    pub to: Vec<SeatName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cc: Vec<SeatName>,
    pub subject: String,
    pub body: String,
    pub path: PathBuf,
    #[serde(default)]
    pub priority: LetterPriority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refs: Vec<String>,
    pub created_at: UnixMs,
    pub created_at_iso: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub receipt_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room: Option<RoomId>,
    pub raw_markdown: String,
    /// Optional base64 payload used only when an ingested legacy file contained
    /// non-UTF-8 bytes, guaranteeing 100% byte-for-byte `sha256` preservation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_bytes_b64: Option<String>,
    #[serde(default)]
    pub origin: MailOrigin,
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
    },
}
