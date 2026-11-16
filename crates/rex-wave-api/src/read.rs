//! Read side: journal paging, waits, the sidebar tree, and BB-owned agent
//! thread pages. None of these mutate.

use crate::board::BoardId;
use crate::id::{BbProjectId, BbSeq, BbThreadId, ModelId, Principal, ProviderId, RoleName, Seq, UnixMs, WaveSlug};
use crate::room::{RoomId, RoomRef};
use crate::stage::Osc7501Status;
use crate::stream::{Cursor, Envelope, Interest, StreamId};
use crate::wave::{Activity, PendingInteraction, WorkerPhase};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Read {
    pub stream: StreamId,
    #[serde(default)]
    pub after: Seq,
    #[serde(default)]
    pub limit: Option<u32>,
}

/// `channel_wait`. Returns as soon as `interest` matches anything after
/// `after`, with the *whole* catch-up slice (never just the trigger), or an
/// empty page at `timeout_ms`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Wait {
    pub interest: Interest,
    pub after: Cursor,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

impl Wait {
    /// Wait on one room after `since`. With `mentions_only`, wake on mentions
    /// of me in `room` after `since` or in any other room from now on.
    pub fn room(room: RoomId, since: Seq, mentions_only: bool) -> Self {
        let stream = StreamId::Room { room: room.clone() };
        if mentions_only {
            Wait { interest: Interest::Mentions, after: Cursor::live().with(stream, since), timeout_ms: None }
        } else {
            Wait { interest: Interest::Room { room: RoomRef::Id { id: room } }, after: Cursor::default().with(stream, since), timeout_ms: None }
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct Sidebar {}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ThreadPage {
    pub wave: WaveSlug,
    pub role: RoleName,
    #[serde(default)]
    pub after: Option<BbSeq>,
    #[serde(default)]
    pub limit: Option<u32>,
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Page {
    pub entries: Vec<Envelope>,
    /// Pass back as `after` to continue without gaps or repeats.
    pub cursor: Cursor,
    pub timed_out: bool,
}

/// The left pane, top to bottom: `Inbox`, `Kanban`, divider, foldable
/// `Projects` (each with `#channels` and `#wave-<slug>` rooms whose children
/// are the wave's agents), `Channels`, `Direct Messages`. A derived,
/// rebuildable projection of the journal, so it owns no state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SidebarTreeSnapshot {
    pub inbox: InboxBadge,
    pub kanban: Vec<BoardBadge>,
    pub projects: Vec<ProjectBranch>,
    pub channels: Vec<ChannelLeaf>,
    pub direct_messages: Vec<DmLeaf>,
    pub osc7501: Osc7501Status,
    /// Journal heads the snapshot reflects.
    pub cursor: Cursor,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxBadge {
    pub unread: u32,
    /// Unread cards that need the viewer to unblock a BB interaction.
    pub blocking: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoardBadge {
    pub board: BoardId,
    pub open: u32,
    pub mine: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectBranch {
    pub project: BbProjectId,
    pub channels: Vec<ChannelLeaf>,
    pub waves: Vec<WaveRoomBranch>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelLeaf {
    pub room: RoomId,
    pub unread: u32,
    pub mentions: u32,
}

/// A `#wave-<slug>` room; unfolding it shows the wave's agents.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaveRoomBranch {
    pub wave: WaveSlug,
    pub room: RoomId,
    pub unread: u32,
    pub mentions: u32,
    pub workers: Vec<WaveWorkerLeaf>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaveWorkerLeaf {
    pub role: RoleName,
    pub provider: ProviderId,
    pub model: Option<ModelId>,
    pub phase: WorkerPhase,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DmLeaf {
    pub room: RoomId,
    pub with: Principal,
    pub unread: u32,
}

/// One agent's BB thread, for the Context Window Inspector stage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ThreadPageView {
    pub thread: BbThreadId,
    pub activity: Activity,
    pub events: Vec<AgentEvent>,
    pub usage: Option<ThreadContextUsage>,
    pub pending: Option<PendingInteraction>,
    pub next: Option<BbSeq>,
}

/// A normalized BB thread event (turn/tool/message), BB-sequenced.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentEvent {
    pub seq: BbSeq,
    pub at: UnixMs,
    pub kind: AgentEventKind,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentEventKind {
    UserMessage { text: String },
    AssistantText { text: String },
    ToolCall { name: String, summary: String },
    ToolResult { name: String, ok: bool, summary: String },
    TurnStarted,
    TurnEnded,
    Usage { usage: ThreadContextUsage },
    Other { bb_type: String },
}

/// Mirrors BB `ThreadContextResponse.usage` (SDK 0.6.132) without its HTTP DTOs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ThreadContextUsage {
    pub used_tokens: u64,
    pub model_context_window: u64,
    pub estimated: bool,
    pub auto_compact_at_tokens: Option<u64>,
    /// Present only when BB reports the cost in USD.
    pub cost_usd: Option<f64>,
    pub categories: Vec<ContextCategory>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextCategoryKind {
    Used,
    Reserved,
    Deferred,
    Free,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextCategory {
    pub id: String,
    pub label: String,
    pub kind: ContextCategoryKind,
    pub tokens: u64,
}
