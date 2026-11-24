//! Streams, envelopes, interests, cursors, frames.
//!
//! Every actor owns exactly one stream (single writer). The journal is the
//! source of truth; the live hub is only a doorbell. Clients resume from a
//! [`Cursor`], so a dropped doorbell can never lose data.

use crate::board::{BoardEvent, BoardId};
use crate::id::{BbProjectId, Label, Principal, RoleName, Seq, UnixMs, WaveSlug};
use crate::mail::MailEvent;
use crate::notify::InboxEvent;
use crate::presence::PresenceEvent;
use crate::read::{AgentEvent, SidebarTreeSnapshot};
use crate::room::{RoomEvent, RoomId, RoomRef};
use crate::stage::{FocusGeneration, StageTarget};
use crate::wave::WaveEvent;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The identity of one single-writer log. Also the actor address.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "stream", rename_all = "snake_case")]
pub enum StreamId {
    Room {
        room: RoomId,
    },
    Wave {
        wave: WaveSlug,
    },
    Board {
        board: BoardId,
    },
    /// One inbox per principal: notifications + read cursors. The only owner
    /// of "what has X seen", so unread = room head (room) - cursor (inbox),
    /// merged at the read boundary (per separate-before-serializing-shared-state).
    Inbox {
        owner: Principal,
    },
    /// The mail bridge's promotion / ack ledger (sandbox only).
    Mail,
    /// Durable seat records and ephemeral live session bindings.
    Presence,
}

/// One committed journal entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub stream: StreamId,
    pub seq: Seq,
    pub at: UnixMs,
    /// Stamped by the daemon from the session; never from arguments.
    pub by: Principal,
    pub event: Event,
}

/// Union of every family's events. The variant must match `stream`'s kind;
/// the store enforces it on append via `Event::fits`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "family", content = "event", rename_all = "snake_case")]
pub enum Event {
    Room(RoomEvent),
    Wave(WaveEvent),
    Board(BoardEvent),
    Inbox(InboxEvent),
    Mail(MailEvent),
    Presence(PresenceEvent),
}

impl Event {
    /// True when this event may live in `stream`. Exhaustive on both sides,
    /// so a new family fails the build here until it is placed.
    pub fn fits(&self, stream: &StreamId) -> bool {
        matches!(
            (self, stream),
            (Event::Room(_), StreamId::Room { .. })
                | (Event::Wave(_), StreamId::Wave { .. })
                | (Event::Board(_), StreamId::Board { .. })
                | (Event::Inbox(_), StreamId::Inbox { .. })
                | (Event::Mail(_), StreamId::Mail)
                | (Event::Presence(_), StreamId::Presence)
        )
    }
}

/// What a watcher wants. Resolved server-side against the session principal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "interest", rename_all = "snake_case")]
pub enum Interest {
    /// Exactly these streams.
    Streams { streams: Vec<StreamId> },
    /// One room by reference (DM refs resolve with the session principal).
    Room { room: RoomRef },
    /// One room filtered by optional thread root (`re`) and/or `key=value` labels
    /// (`RoomEvent::Labeled` matches when `labels` match `before || after`).
    RoomFiltered {
        room: RoomRef,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        re: Option<Seq>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        labels: Vec<Label>,
    },
    /// Only entries that @mention me (or @all) in rooms I belong to, plus DMs to me.
    /// This is `channel_wait(mentions_only: true)`.
    Mentions,
    /// My inbox stream (notifications), for the TUI `Inbox` node and OSC 7501.
    MyInbox,
    /// Everything under one BB project: its rooms, waves, and board.
    Project { project: BbProjectId },
    /// Live BB thread events of one wave agent (BB-owned; proxied, not journaled).
    /// Opened when the TUI focuses an agent node and dropped on blur.
    Agent { wave: WaveSlug, role: RoleName },
    /// Operator only: all journaled streams (debug, GUI dashboards).
    Everything,
    /// The left-pane tree: a `Frame::Sidebar` now and after every change that
    /// affects it. Bursts of changes coalesce into one frame.
    Sidebar,
    /// The right-pane stage. One per session: opening a new one ends the old
    /// one, and its frames arrive wrapped in `Frame::Stage` with the new
    /// generation.
    Stage { target: StageTarget },
}

/// A multi-stream resume position. Missing streams mean `Seq::ORIGIN`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Cursor {
    positions: Vec<(StreamId, Seq)>,
    /// Streams not listed start at their head at subscribe time instead of origin.
    #[serde(default)]
    from_head: bool,
    /// True once `Wait` has materialized all subscription-time visible stream
    /// heads into `positions` and cleared `from_head`, so subsequent CLI
    /// mentions waits keep `from_head: false` for streams created between waits.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pinned: bool,
}

impl Cursor {
    /// Start from now: only frames committed after subscription.
    pub fn live() -> Self {
        Cursor {
            positions: Vec::new(),
            from_head: true,
            pinned: false,
        }
    }
    pub fn from_head(&self) -> bool {
        self.from_head
    }
    pub fn is_pinned(&self) -> bool {
        self.pinned
    }
    pub fn with_from_head(mut self, from_head: bool) -> Self {
        self.from_head = from_head;
        if from_head {
            self.pinned = false;
        }
        self
    }
    pub fn pin_head_snapshot(mut self) -> Self {
        self.from_head = false;
        self.pinned = true;
        self
    }
    /// Is `stream` positioned explicitly (rather than by the default)?
    pub fn lists(&self, stream: &StreamId) -> bool {
        self.positions.iter().any(|(s, _)| s == stream)
    }
    pub fn with(mut self, stream: StreamId, seq: Seq) -> Self {
        self.advance(&stream, seq);
        self
    }
    pub fn at(&self, stream: &StreamId) -> Seq {
        self.positions
            .iter()
            .find(|(s, _)| s == stream)
            .map(|(_, q)| *q)
            .unwrap_or(Seq::ORIGIN)
    }
    pub fn try_map_streams<F, E>(&self, mut f: F) -> Result<Cursor, E>
    where
        F: FnMut(StreamId) -> Result<StreamId, E>,
    {
        let mut out = Cursor {
            positions: Vec::with_capacity(self.positions.len()),
            from_head: self.from_head,
            pinned: self.pinned,
        };
        for (stream, seq) in &self.positions {
            out.advance(&f(stream.clone())?, *seq);
        }
        Ok(out)
    }
    /// Advance monotonically (never moves backwards; idempotent on replays).
    pub fn advance(&mut self, stream: &StreamId, seq: Seq) {
        match self.positions.iter_mut().find(|(s, _)| s == stream) {
            Some((_, q)) if *q < seq => *q = seq,
            Some(_) => {}
            None => self.positions.push((stream.clone(), seq)),
        }
    }
}

/// One item on a watch stream.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub enum Frame {
    /// A journaled entry (catch-up or live).
    Committed(Envelope),
    /// Catch-up finished; everything after this is live.
    CaughtUp { cursor: Cursor },
    /// A BB-owned agent event (for `Interest::Agent`).
    Agent {
        wave: WaveSlug,
        role: RoleName,
        event: AgentEvent,
    },
    /// The current sidebar tree (`Interest::Sidebar`).
    Sidebar(Box<SidebarTreeSnapshot>),
    /// First frame of a stage watch: the generation every later frame carries.
    StageFocused {
        generation: FocusGeneration,
        target: StageTarget,
    },
    /// A frame of the stage watch with `generation`. Never nests.
    Stage {
        generation: FocusGeneration,
        inner: Box<Frame>,
    },
}
