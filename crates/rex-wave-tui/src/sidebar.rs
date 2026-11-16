//! The left pane: a foldable org-tree.
//!
//! ```text
//!   Inbox                  3 ●     <- NodeKey::Inbox      (top, global)
//!   Kanban                 7       <- NodeKey::Kanban
//!  ──────────────────────────      <- divider (not focusable)
//!  ▾ Projects
//!  │ ▾ rex-wave
//!  │ ├─ #general           2
//!  │ ├─ ▾ #wave-auth-v2    ●2 ◐1
//!  │ │  ├─ ● author-jwt [codex]
//!  │ │  └─ ◐ migrator [claude-code]
//!  │ └─ #design
//!  ▾ Channels
//!  │ └─ #lounge
//!  ▾ Direct Messages
//!    ├─ @sol               1
//!    └─ @astra
//! ```
//!
//! The tree is *derived* from `SidebarTreeSnapshot` by [`build`] every time the
//! overview changes; fold state is keyed by [`NodeKey`] (stable identity), not
//! by row index, so folds survive rebuilds and the cursor follows its node.

use crate::input::HitMap;
use rex_wave_api::read::{WaveWorkerLeaf, SidebarTreeSnapshot};
use rex_wave_api::room::RoomId;
use rex_wave_api::*;
use std::collections::BTreeSet;

/// Stable identity of a sidebar node.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeKey {
    Inbox,
    Kanban,
    ProjectsHeader,
    Project(BbProjectId),
    Room(RoomId),
    Agent { wave: WaveSlug, role: RoleName },
    ChannelsHeader,
    DmsHeader,
    Dm(RoomId),
}

/// Status glyph for agents and wave rooms: `●` working, `◐` parked/idle, `▲` blocked, `✕` errored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Glyph {
    Working,
    Waiting,
    Blocked,
    Errored,
    Done,
}

#[derive(Clone, Debug)]
pub struct SidebarNode {
    pub key: NodeKey,
    pub label: String,
    /// Right-aligned unread / mention count.
    pub badge: Option<u32>,
    pub glyph: Option<Glyph>,
    /// Provider tag for agent nodes (`[codex]`).
    pub tag: Option<String>,
    pub children: Vec<SidebarNode>,
}

/// One rendered row after folding.
#[derive(Clone, Debug)]
pub struct Row {
    pub key: NodeKey,
    pub depth: u16,
    /// Precomputed connector prefix: `"│ ├─ "`, `"│ └─ "`, ...
    pub prefix: String,
    /// `Some(true)` = expanded (`▾`), `Some(false)` = folded (`▸`), `None` = leaf.
    pub fold: Option<bool>,
    pub node: SidebarNode,
}

/// Which nodes are expanded. Absent = folded, except defaults in [`FoldState::default`].
#[derive(Clone, Debug)]
pub struct FoldState {
    expanded: BTreeSet<NodeKey>,
}

impl Default for FoldState {
    fn default() -> Self {
        let expanded = [NodeKey::ProjectsHeader, NodeKey::ChannelsHeader, NodeKey::DmsHeader].into_iter().collect();
        FoldState { expanded }
    }
}

impl FoldState {
    pub fn is_expanded(&self, key: &NodeKey) -> bool {
        self.expanded.contains(key)
    }
    pub fn set(&mut self, key: NodeKey, expanded: bool) {
        if expanded {
            self.expanded.insert(key);
        } else {
            self.expanded.remove(&key);
        }
    }
    pub fn toggle(&mut self, key: &NodeKey) {
        let e = self.is_expanded(key);
        self.set(key.clone(), !e);
    }
}

/// The sidebar component state.
pub struct Sidebar {
    pub roots: Vec<SidebarNode>,
    pub folds: FoldState,
    /// The focused node (by identity). Survives rebuilds.
    pub cursor: NodeKey,
    /// The node under the mouse, if any (hover drives the stage after debounce).
    pub hover: Option<NodeKey>,
    pub scroll: u16,
}

/// Pure: overview -> tree. Section order is fixed: Inbox, Kanban, ─, Projects, Channels, DMs.
pub fn build(overview: &SidebarTreeSnapshot) -> Vec<SidebarNode> {
    let _ = overview;
    todo!("sidebar::build")
}

/// Pure: worker phase -> glyph.
pub fn agent_glyph(agent: &WaveWorkerLeaf) -> Glyph {
    let _ = agent;
    todo!("agent_glyph")
}

/// Pure: tree + folds -> visible rows with connector prefixes.
pub fn flatten(roots: &[SidebarNode], folds: &FoldState) -> Vec<Row> {
    let _ = (roots, folds);
    todo!("flatten")
}

impl Sidebar {
    /// Replace the tree from a fresh overview, keeping folds and cursor identity
    /// (cursor falls back to the nearest surviving ancestor if its node vanished).
    pub fn rebuild(&mut self, overview: &SidebarTreeSnapshot) {
        let _ = overview;
        todo!("Sidebar::rebuild")
    }
    /// `j`/`k`/arrows: move over visible focusable rows (skips the divider).
    pub fn move_cursor(&mut self, delta: i32) {
        let _ = delta;
        todo!("move_cursor")
    }
    /// `h`/Left: fold if expanded, else jump to parent.  `l`/Right: unfold if folded, else first child.
    pub fn fold_or_parent(&mut self) {
        todo!("fold_or_parent")
    }
    pub fn unfold_or_child(&mut self) {
        todo!("unfold_or_child")
    }
    /// Render rows into `area`, registering one hit region per row
    /// (`Target::Node`) and one per fold arrow (`Target::Fold`).
    pub fn render(&self, area: ratatui::layout::Rect, buf: &mut ratatui::buffer::Buffer, hits: &mut HitMap) {
        let _ = (area, buf, hits);
        todo!("Sidebar::render")
    }
}
