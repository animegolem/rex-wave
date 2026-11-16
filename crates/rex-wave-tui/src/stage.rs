//! The right pane: one view per focused sidebar node, each with its own
//! subscription plan. Retargeting = drop the old stage watch, open the new
//! one (multiplexed on the same socket, so on-hover switching is cheap).

use crate::input::HitMap;
use crate::kitty::CardPlacement;
use crate::sidebar::NodeKey;
use rex_wave_api::board::{BoardId, BoardSnapshot};
use rex_wave_api::notify::InboxPage;
use rex_wave_api::read::ThreadPageView;
use rex_wave_api::room::RoomId;
use rex_wave_api::*;
use std::time::Duration;

/// Hover must rest this long before the stage retargets (clicks/Enter are immediate).
pub const HOVER_DEBOUNCE: Duration = Duration::from_millis(120);

/// What the stage is showing. Derived purely from a `NodeKey`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageTarget {
    /// Rich notification & BB approval feed (z=-1 cards).
    Inbox,
    /// Kanban / org-todo. `None` = the board picker (all boards with counts).
    Kanban { board: Option<BoardId> },
    /// Slack-like chat + composer for a `#channel` / `#wave-<slug>` / DM.
    Room { room: RoomId },
    /// One agent's BB thread inspector: events, context bar, pending interaction, steer composer.
    Agent { wave: WaveSlug, role: RoleName },
    /// Headers/projects: a summary card.
    Summary { key: NodeKey },
}

/// Pure: node -> stage target.
pub fn target_for(key: &NodeKey) -> StageTarget {
    match key {
        NodeKey::Inbox => StageTarget::Inbox,
        NodeKey::Kanban => StageTarget::Kanban { board: None },
        NodeKey::Room(room) | NodeKey::Dm(room) => StageTarget::Room { room: room.clone() },
        NodeKey::Agent { wave, role } => StageTarget::Agent { wave: wave.clone(), role: role.clone() },
        other => StageTarget::Summary { key: other.clone() },
    }
}

/// Pure: the initial query and the live watch for a target.
pub struct Plan {
    pub initial: Option<WireCommand>,
    pub watch: Option<(Interest, Cursor)>,
}

pub fn plan(target: &StageTarget) -> Plan {
    let _ = target;
    // Inbox            -> inbox_list ; watch MyInbox (live)
    // Kanban{Some(b)}  -> board_view{b} ; watch Streams[Board{b}] from snapshot head
    // Room{r}          -> read{Room{r}, last 200} ; watch Room{Id{r}} from page cursor
    // Agent{w,r}       -> thread_page{w,r} ; watch Agent{w,r}  (BB-owned live frames, leased)
    // Summary          -> none
    todo!("stage::plan")
}

/// Per-target view state. Each variant is folded from its own watch only.
pub enum Stage {
    Inbox(InboxPage),
    Kanban(KanbanView),
    Room(RoomView),
    Agent(AgentInspector),
    Summary(NodeKey),
    Loading(StageTarget),
}

pub struct KanbanView {
    pub snapshot: BoardSnapshot,
    pub cursor: (usize, usize),
}

pub struct RoomView {
    pub room: RoomId,
    pub entries: Vec<Envelope>,
    pub cursor: Cursor,
    pub selection: Option<(Seq, Seq)>,
    pub composer: String,
    pub scroll: u16,
}

pub struct AgentInspector {
    pub wave: WaveSlug,
    pub role: RoleName,
    pub page: ThreadPageView,
    pub composer: String,
    pub scroll: u16,
}

impl Stage {
    /// Fold one frame from the stage watch into the current view.
    pub fn apply(&mut self, frame: &Frame) {
        let _ = frame;
        todo!("Stage::apply")
    }
    /// Render; notification/approval cards register `CardPlacement`s for the
    /// Kitty layer and keep interior cells `Color::Reset` so z=-1 shows through.
    pub fn render(&self, area: ratatui::layout::Rect, buf: &mut ratatui::buffer::Buffer, hits: &mut HitMap, cards: &mut Vec<CardPlacement>) {
        let _ = (area, buf, hits, cards);
        todo!("Stage::render")
    }
}
