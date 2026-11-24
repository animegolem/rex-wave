//! The right-hand stage of the 2-pane UI and the terminal status line.
//!
//! The stage is a single slot per session. Opening `Interest::Stage` bumps
//! the session's [`FocusGeneration`] and ends the previous stage watch on the
//! server, and every frame on the new watch is stamped with its generation.
//! A client moving the tree cursor quickly (`hjkl`, mouse hover) can therefore
//! drop any frame whose generation is not the latest, even if it merges all
//! of its watches into one event queue.

use crate::board::BoardId;
use crate::id::{RoleName, WaveSlug};
use crate::read::InboxBadge;
use crate::room::RoomRef;
use crate::stream::{Interest, StreamId};
use crate::wave::{Activity, InteractionKind, WorkerPhase};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Monotonic per session; assigned by the server when a stage watch opens.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(transparent)]
pub struct FocusGeneration(pub u64);

impl FocusGeneration {
    pub fn next(self) -> Self {
        FocusGeneration(self.0 + 1)
    }
}

/// What the focused sidebar node shows on the stage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "target", rename_all = "snake_case")]
pub enum StageTarget {
    /// Notifications and BB approval cards.
    Inbox,
    Kanban {
        board: BoardId,
    },
    /// A project `#channel`, a `#wave-<slug>` room, a global channel, or a DM.
    Room {
        room: RoomRef,
    },
    /// One wave agent's context window and BB thread.
    Agent {
        wave: WaveSlug,
        role: RoleName,
    },
}

impl StageTarget {
    /// Maps a focused stage target to its underlying journal or live agent [`Interest`].
    pub fn to_inner_interest(&self) -> Interest {
        match self {
            StageTarget::Inbox => Interest::MyInbox,
            StageTarget::Kanban { board } => Interest::Streams {
                streams: vec![StreamId::Board {
                    board: board.clone(),
                }],
            },
            StageTarget::Room { room } => Interest::Room { room: room.clone() },
            StageTarget::Agent { wave, role } => Interest::Agent {
                wave: wave.clone(),
                role: role.clone(),
            },
        }
    }
}

/// `OSC 7501` program status for the Rex terminal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Osc7501State {
    Idle,
    Working,
    Done,
    Blocked,
    Error,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Osc7501BlockKind {
    Permission,
    Question,
    Auth,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Osc7501Status {
    pub state: Osc7501State,
    pub block_kind: Option<Osc7501BlockKind>,
    pub progress_pct: Option<u8>,
    pub message: String,
}

impl Default for Osc7501Status {
    fn default() -> Self {
        Osc7501Status {
            state: Osc7501State::Idle,
            block_kind: None,
            progress_pct: None,
            message: String::new(),
        }
    }
}

/// Pure. Precedence is blocked, then error, then working, then done, then idle,
/// because the terminal shows one state and "waiting on you" must win.
pub fn derive_osc7501_status<'a>(
    inbox: &InboxBadge,
    workers: impl IntoIterator<Item = &'a WorkerPhase>,
) -> Osc7501Status {
    let (mut total, mut retired, mut working, mut failed) = (0u32, 0u32, 0u32, 0u32);
    let mut blocked: Option<Osc7501BlockKind> = None;
    let mut blocked_count = 0u32;
    for phase in workers {
        total += 1;
        match phase {
            WorkerPhase::Requested { .. } | WorkerPhase::Retiring { .. } => working += 1,
            WorkerPhase::Retired { .. } => retired += 1,
            WorkerPhase::Failed { .. } => failed += 1,
            WorkerPhase::Live { activity, .. } => match activity {
                Activity::Working | Activity::Parked { .. } | Activity::Idle => working += 1,
                Activity::Errored { .. } | Activity::Refused { .. } => failed += 1,
                Activity::Blocked { interaction } => {
                    blocked_count += 1;
                    blocked.get_or_insert(match interaction.kind {
                        InteractionKind::Question { .. } => Osc7501BlockKind::Question,
                        InteractionKind::Permission | InteractionKind::Extension => {
                            Osc7501BlockKind::Permission
                        }
                    });
                }
            },
        }
    }
    let waiting = blocked_count.max(inbox.blocking);
    if waiting > 0 {
        return Osc7501Status {
            state: Osc7501State::Blocked,
            block_kind: Some(blocked.unwrap_or(Osc7501BlockKind::Permission)),
            progress_pct: None,
            message: format!("{waiting} waiting on you"),
        };
    }
    if failed > 0 {
        return Osc7501Status {
            state: Osc7501State::Error,
            block_kind: None,
            progress_pct: None,
            message: format!("{failed} worker(s) failed"),
        };
    }
    if working > 0 {
        return Osc7501Status {
            state: Osc7501State::Working,
            block_kind: None,
            progress_pct: Some((retired * 100 / total) as u8),
            message: format!("{working} working"),
        };
    }
    if total > 0 {
        return Osc7501Status {
            state: Osc7501State::Done,
            block_kind: None,
            progress_pct: Some(100),
            message: "all workers retired".into(),
        };
    }
    Osc7501Status {
        message: format!("{} unread", inbox.unread),
        ..Osc7501Status::default()
    }
}
