//! Mail bridge shapes: `mailbox.item.schema.json` v1 projections, letters
//! promoted from rooms, and wake-bell ingress. NON-PROD sandbox only; the
//! sandbox guard lives in `rex-wave-mailbridge::SandboxRoot`.

use crate::id::{SeatName, Seq, SeqRange, Sha256Hex};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LetterPriority {
    Low,
    Normal,
    High,
    Blocking,
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

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct MailAck {
    pub path: PathBuf,
    pub sha256: Sha256Hex,
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
    },
    Projected {
        item: MailboxItem,
    },
    BellRang {
        bell_id: String,
        seat: SeatName,
    },
}
