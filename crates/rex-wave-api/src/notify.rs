//! Inbox: per-principal ranked notifications and read cursors.
//!
//! A notification carries the exact [`WireCommand`]s its buttons run, so
//! every UI (TUI card, GUI, BB panel) executes actions identically without
//! UI-side business logic.

use crate::board::CardRef;
use crate::id::{BbInteractionId, Principal, RoleName, Seq, UnixMs, WaveSlug};
use crate::mail::{LetterRef, MailboxItem};
use crate::registry::WireCommand;
use crate::room::RoomId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Per-inbox notification number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct NotificationId(pub u64);

/// Higher = more urgent. Computed by the pure `rex_wave_notify::rank`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct Rank(pub u16);

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NotifyKind {
    Mention { room: RoomId, seq: Seq },
    DirectMessage { room: RoomId, seq: Seq },
    /// A BB `interaction.pending` from a wave worker: approve/deny/answer.
    Interaction { wave: WaveSlug, role: RoleName, interaction: BbInteractionId },
    WorkerStuck { wave: WaveSlug, role: RoleName, why: String },
    WaveFinished { wave: WaveSlug },
    CardAssigned { card: CardRef },
    /// A sandbox letter projected as `mailbox.item.schema.json` v1.
    Mail { item: MailboxItem },
    LetterBanked { letter: LetterRef },
    /// A wake-bell scheduled prompt.
    Reminder { bell_id: String },
}

/// A button on a card. `run` is sent verbatim through `Client::call_wire`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct NotifyAction {
    pub label: String,
    /// Single-key accelerator in the TUI (`a`, `d`, `p`, ...).
    pub key: Option<char>,
    pub run: Box<WireCommand>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NotifyState {
    Unread,
    Seen,
    Done,
    Dismissed,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct NotificationDraft {
    pub kind: NotifyKind,
    pub title: String,
    pub preview: String,
    pub actions: Vec<NotifyAction>,
    /// Re-raising with the same dedupe key updates instead of duplicating
    /// (e.g. one card per pending interaction, not one per BB event).
    pub dedupe: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Notification {
    pub id: NotificationId,
    pub at: UnixMs,
    pub rank: Rank,
    pub state: NotifyState,
    pub draft: NotificationDraft,
}


#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InboxList {
    #[serde(default)]
    pub include_done: bool,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InboxAck {
    pub ids: Vec<NotificationId>,
    pub state: NotifyState,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InboxMarkRead {
    pub room: RoomId,
    pub upto: Seq,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InboxRaise {
    pub to: Principal,
    pub draft: NotificationDraft,
}


#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InboxPage {
    pub head: Seq,
    /// Sorted by rank desc, then time desc.
    pub items: Vec<Notification>,
    pub unread: u32,
    /// Per-room read cursors (merged with room heads by the read side for unread badges).
    pub cursors: Vec<(RoomId, Seq)>,
}


#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InboxEvent {
    Raised { id: NotificationId, rank: Rank, draft: NotificationDraft },
    Updated { id: NotificationId, rank: Rank, draft: NotificationDraft },
    StateSet { ids: Vec<NotificationId>, state: NotifyState },
    CursorAdvanced { room: RoomId, upto: Seq },
}
