//! Read side: journal paging, waits, the sidebar tree, and BB-owned agent
//! thread pages. None of these mutate.

use crate::board::BoardId;
use crate::id::{
    BbProjectId, BbSeq, BbThreadId, Label, ModelId, Principal, ProviderId, RoleName, Seq, UnixMs,
    WaveSlug,
};
use crate::room::{RoomId, RoomRef};
use crate::stage::Osc7501Status;
use crate::stream::{Cursor, Envelope, Interest, StreamId};
use crate::wave::{Activity, PendingInteraction, WaveOutcome, WorkerPhase};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Read {
    pub stream: StreamId,
    /// Optional room reference (`#channel`, `@seat`); when present, resolved
    /// server-side (`RoomRef::Dm` falls back to an existing pre-PR-26
    /// `(Operator, peer)` stream when canonical `head == 0`, while
    /// `RoomRef::Id` stays pinned to its exact stream).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room: Option<RoomRef>,
    #[serde(default)]
    pub after: Seq,
    /// Optional resume cursor; when present and `after == Seq::ORIGIN`, the
    /// server looks up `cursor.at(&target_stream)` after resolving `room`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
    #[serde(default)]
    pub limit: Option<u32>,
}

impl Read {
    pub fn stream(stream: StreamId, after: Seq, limit: Option<u32>) -> Self {
        Read {
            stream,
            room: None,
            after,
            cursor: None,
            limit,
        }
    }

    pub fn room_ref(
        room: RoomRef,
        viewer: &Principal,
        after: Seq,
        cursor: Option<Cursor>,
        limit: Option<u32>,
    ) -> Result<Self, crate::id::InvalidId> {
        let fallback_id = match (&room, viewer) {
            (RoomRef::Dm { with }, Principal::Operator { .. }) => {
                let peer = with.as_dm_member();
                if viewer.same_actor(&peer) {
                    room.resolve(viewer)?
                } else {
                    RoomId::Direct {
                        pair: crate::room::PrincipalPair::new(viewer.clone(), peer)?,
                    }
                }
            }
            _ => room.resolve(viewer)?,
        };
        let fallback_stream = StreamId::Room { room: fallback_id };
        let wire_after = if after > Seq::ORIGIN {
            after
        } else if let Some(ref c) = cursor {
            c.at(&fallback_stream)
        } else {
            Seq::ORIGIN
        };
        Ok(Read {
            stream: fallback_stream,
            room: Some(room),
            after: wire_after,
            cursor,
            limit,
        })
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl Wait {
    /// Wait on one room after `since`. With `mentions_only`, wake on mentions
    /// of me in `room` after `since` or in any other room from now on.
    pub fn room(room: RoomId, since: Seq, mentions_only: bool) -> Self {
        let stream = StreamId::Room { room: room.clone() };
        if mentions_only {
            Wait {
                interest: Interest::Mentions,
                after: Cursor::live().with(stream, since),
                timeout_ms: None,
                limit: None,
            }
        } else {
            Wait {
                interest: Interest::Room {
                    room: RoomRef::Id { id: room },
                },
                after: Cursor::default().with(stream, since),
                timeout_ms: None,
                limit: None,
            }
        }
    }

    pub fn room_ref(
        room: RoomRef,
        me: &Principal,
        since: Seq,
        mentions_only: bool,
    ) -> Result<Self, crate::id::InvalidId> {
        let id = room.resolve(me)?;
        let stream = StreamId::Room { room: id };
        if mentions_only {
            Ok(Wait {
                interest: Interest::Mentions,
                after: Cursor::live().with(stream, since),
                timeout_ms: None,
                limit: None,
            })
        } else {
            Ok(Wait {
                interest: Interest::Room { room },
                after: Cursor::default().with(stream, since),
                timeout_ms: None,
                limit: None,
            })
        }
    }

    pub fn room_filtered(room: RoomId, since: Seq, re: Option<Seq>, labels: Vec<Label>) -> Self {
        let stream = StreamId::Room { room: room.clone() };
        Wait {
            interest: Interest::RoomFiltered {
                room: RoomRef::Id { id: room },
                re,
                labels,
            },
            after: Cursor::default().with(stream, since),
            timeout_ms: None,
            limit: None,
        }
    }

    pub fn room_ref_filtered(
        room: RoomRef,
        me: &Principal,
        since: Seq,
        re: Option<Seq>,
        labels: Vec<Label>,
    ) -> Result<Self, crate::id::InvalidId> {
        let id = room.resolve(me)?;
        let stream = StreamId::Room { room: id };
        Ok(Wait {
            interest: Interest::RoomFiltered { room, re, labels },
            after: Cursor::default().with(stream, since),
            timeout_ms: None,
            limit: None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArchiveTarget {
    Project { project: BbProjectId },
    Wave { wave: WaveSlug },
    Room { room: RoomId },
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SetArchived {
    pub target: ArchiveTarget,
    pub archived: bool,
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
    #[serde(default)]
    pub total_count: usize,
    #[serde(default)]
    pub truncated: bool,
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
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub archived: bool,
    pub channels: Vec<ChannelLeaf>,
    pub waves: Vec<WaveRoomBranch>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelLeaf {
    pub room: RoomId,
    pub unread: u32,
    pub mentions: u32,
    #[serde(default)]
    pub archived: bool,
}

/// A `#wave-<slug>` room; unfolding it shows the wave's agents.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaveRoomBranch {
    pub wave: WaveSlug,
    pub room: RoomId,
    pub unread: u32,
    pub mentions: u32,
    #[serde(default)]
    pub finished: Option<WaveOutcome>,
    #[serde(default)]
    pub archived: bool,
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
    #[serde(default)]
    pub archived: bool,
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
    UserMessage {
        text: String,
    },
    AssistantText {
        text: String,
    },
    ToolCall {
        name: String,
        summary: String,
    },
    ToolResult {
        name: String,
        ok: bool,
        summary: String,
    },
    TurnStarted,
    TurnEnded,
    Usage {
        usage: ThreadContextUsage,
    },
    Other {
        bb_type: String,
    },
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
