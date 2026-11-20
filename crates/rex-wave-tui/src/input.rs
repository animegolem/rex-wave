//! Keyboard and mouse input mapped onto one `Action` vocabulary, plus frame hit-testing.
//!
//! Every key binding lives in [`KEYMAP`]. `key_action` resolves keys from it and the
//! `?` help overlay renders it, so the on-screen key hints cannot drift from behavior.

use crate::sidebar::NodeKey;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::{Position, Rect};
use rex_wave_api::{BbInteractionId, RoleName, WaveSlug, notify::NotificationId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pane {
    Sidebar,
    Stage,
    Composer,
    /// A modal overlay (keymap help or message pager) owns the keyboard.
    Overlay,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dir {
    Up,
    Down,
    Left,
    Right,
}

/// Composer editing operations; text insertion stays in `Action::Input` / `Action::Paste`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EditOp {
    Newline,
    Backspace,
    Delete,
    Left,
    Right,
    Home,
    End,
    Up,
    Down,
    KillLine,
    KillWord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Move(Dir),
    /// Step the stage item cursor (inbox card, room message, agent turn, kanban card).
    Select(i8),
    /// Scroll one viewport page (`1` down, `-1` up).
    Page(i8),
    Top,
    Bottom,
    PrevLane,
    NextLane,
    MoveCard,
    NewTask,
    ToggleFold(NodeKey),
    /// Org-mode style global visibility cycle of the sidebar tree.
    CycleFoldAll,
    Activate(NodeKey),
    Hover(NodeKey),
    FocusPane(Pane),
    Scroll {
        pane: Pane,
        lines: i16,
    },
    RunCardAction {
        id: NotificationId,
        index: usize,
    },
    Accelerator(char),
    ResolveAgent {
        wave: WaveSlug,
        role: RoleName,
        interaction: BbInteractionId,
        approve: bool,
    },
    SelectMessage(u64),
    SelectKanbanCard {
        lane: usize,
        card: usize,
    },
    /// `Enter` on the stage: expand a collapsed brief, page the selected item, or compose.
    Open,
    /// `Space` on the stage: toggle the selected brief, otherwise page down.
    StageSpace,
    Compose,
    Submit,
    Cancel,
    Input(char),
    Paste(String),
    Edit(EditOp),
    Promote,
    OpenProjectBoard,
    CycleGraphics,
    ToggleHelp,
    SidebarWidth(i16),
    ToggleSidebar,
    ResizeBegin,
    ResizeDrag(u16),
    ResizeEnd,
    Quit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    Node(NodeKey),
    Fold(NodeKey),
    Card {
        id: NotificationId,
    },
    CardButton {
        id: NotificationId,
        index: usize,
    },
    AgentDecision {
        wave: WaveSlug,
        role: RoleName,
        interaction: BbInteractionId,
        approve: bool,
    },
    KanbanCard {
        lane: usize,
        card: usize,
    },
    Message {
        seq: u64,
    },
    PromoteButton,
    Composer,
    SidebarBody,
    StageBody,
    Divider,
    Overlay,
}

#[derive(Default, Debug)]
pub struct HitMap {
    regions: Vec<(Rect, Target)>,
}

pub type FrameHitMap = HitMap;

impl HitMap {
    pub fn clear(&mut self) {
        self.regions.clear();
    }

    pub fn push(&mut self, area: Rect, target: Target) {
        if area.width > 0 && area.height > 0 {
            self.regions.push((area, target));
        }
    }

    pub fn hit(&self, col: u16, row: u16) -> Option<&Target> {
        let p = Position { x: col, y: row };
        self.regions
            .iter()
            .rev()
            .find(|(r, _)| r.contains(p))
            .map(|(_, t)| t)
    }
}

const NAV: &[Pane] = &[Pane::Sidebar, Pane::Stage];
const NAV_OVERLAY: &[Pane] = &[Pane::Sidebar, Pane::Stage, Pane::Overlay];
const SIDEBAR: &[Pane] = &[Pane::Sidebar];
const STAGE: &[Pane] = &[Pane::Stage];
const COMPOSER: &[Pane] = &[Pane::Composer];
const OVERLAY: &[Pane] = &[Pane::Overlay];
const ANY: &[Pane] = &[Pane::Sidebar, Pane::Stage, Pane::Composer, Pane::Overlay];

/// One key binding. `keys` is the label shown in the help overlay; bindings that share
/// a `group` and `help` text are merged into one help row (`j / ↓`).
pub struct Binding {
    pub code: KeyCode,
    pub mods: KeyModifiers,
    pub panes: &'static [Pane],
    pub keys: &'static str,
    pub group: &'static str,
    pub help: &'static str,
    pub act: fn(&NodeKey) -> Action,
}

const fn bind(
    code: KeyCode,
    mods: KeyModifiers,
    panes: &'static [Pane],
    keys: &'static str,
    group: &'static str,
    help: &'static str,
    act: fn(&NodeKey) -> Action,
) -> Binding {
    Binding {
        code,
        mods,
        panes,
        keys,
        group,
        help,
        act,
    }
}

const N: KeyModifiers = KeyModifiers::NONE;
const C: KeyModifiers = KeyModifiers::CONTROL;
const S: KeyModifiers = KeyModifiers::SHIFT;
const A: KeyModifiers = KeyModifiers::ALT;

use KeyCode::{
    BackTab, Backspace, Char, Delete, Down, End, Enter, Esc, Home, Left, PageDown, PageUp, Right,
    Tab, Up,
};

pub static KEYMAP: &[Binding] = &[
    // Global
    bind(Char('c'), C, ANY, "ctrl+c", "Global", "quit", |_| {
        Action::Quit
    }),
    bind(Char('q'), N, NAV, "q", "Global", "quit", |_| Action::Quit),
    bind(Char('?'), N, NAV, "?", "Global", "toggle this help", |_| {
        Action::ToggleHelp
    }),
    bind(
        Tab,
        N,
        NAV,
        "tab",
        "Global",
        "switch focus sidebar ↔ stage",
        |_| Action::FocusPane(Pane::Stage),
    ),
    bind(Char('<'), N, NAV, "<", "Global", "narrower sidebar", |_| {
        Action::SidebarWidth(-2)
    }),
    bind(Char('>'), N, NAV, ">", "Global", "wider sidebar", |_| {
        Action::SidebarWidth(2)
    }),
    bind(
        Char('\\'),
        N,
        NAV,
        "\\",
        "Global",
        "hide / show sidebar",
        |_| Action::ToggleSidebar,
    ),
    bind(
        Char('v'),
        N,
        NAV_OVERLAY,
        "v",
        "Global",
        "cycle graphics mode",
        |_| Action::CycleGraphics,
    ),
    bind(Esc, N, NAV, "esc", "Global", "clear selection", |_| {
        Action::Cancel
    }),
    // Sidebar
    bind(Char('j'), N, SIDEBAR, "j", "Sidebar", "next node", |_| {
        Action::Move(Dir::Down)
    }),
    bind(Down, N, SIDEBAR, "↓", "Sidebar", "next node", |_| {
        Action::Move(Dir::Down)
    }),
    bind(
        Char('k'),
        N,
        SIDEBAR,
        "k",
        "Sidebar",
        "previous node",
        |_| Action::Move(Dir::Up),
    ),
    bind(Up, N, SIDEBAR, "↑", "Sidebar", "previous node", |_| {
        Action::Move(Dir::Up)
    }),
    bind(
        Char('h'),
        N,
        SIDEBAR,
        "h",
        "Sidebar",
        "fold / go to parent",
        |_| Action::Move(Dir::Left),
    ),
    bind(
        Left,
        N,
        SIDEBAR,
        "←",
        "Sidebar",
        "fold / go to parent",
        |_| Action::Move(Dir::Left),
    ),
    bind(
        Char('l'),
        N,
        SIDEBAR,
        "l",
        "Sidebar",
        "unfold / first child",
        |_| Action::Move(Dir::Right),
    ),
    bind(
        Right,
        N,
        SIDEBAR,
        "→",
        "Sidebar",
        "unfold / first child",
        |_| Action::Move(Dir::Right),
    ),
    bind(Char('g'), N, SIDEBAR, "g", "Sidebar", "top", |_| {
        Action::Top
    }),
    bind(Char('G'), N, SIDEBAR, "G", "Sidebar", "bottom", |_| {
        Action::Bottom
    }),
    bind(PageDown, N, SIDEBAR, "pgdn", "Sidebar", "page down", |_| {
        Action::Page(1)
    }),
    bind(PageUp, N, SIDEBAR, "pgup", "Sidebar", "page up", |_| {
        Action::Page(-1)
    }),
    bind(
        Char(' '),
        N,
        SIDEBAR,
        "space",
        "Sidebar",
        "fold / unfold node",
        |k| Action::ToggleFold(k.clone()),
    ),
    bind(
        BackTab,
        N,
        SIDEBAR,
        "shift+tab",
        "Sidebar",
        "cycle tree fold levels",
        |_| Action::CycleFoldAll,
    ),
    bind(
        Enter,
        N,
        SIDEBAR,
        "enter",
        "Sidebar",
        "open node on stage",
        |k| Action::Activate(k.clone()),
    ),
    // Stage
    bind(
        Char('j'),
        N,
        STAGE,
        "j",
        "Stage",
        "next card / msg / turn",
        |_| Action::Select(1),
    ),
    bind(
        Char('k'),
        N,
        STAGE,
        "k",
        "Stage",
        "prev card / msg / turn",
        |_| Action::Select(-1),
    ),
    bind(
        Down,
        N,
        STAGE,
        "↓",
        "Stage",
        "scroll down one line",
        |_| Action::Move(Dir::Down),
    ),
    bind(Up, N, STAGE, "↑", "Stage", "scroll up one line", |_| {
        Action::Move(Dir::Up)
    }),
    bind(PageDown, N, STAGE, "pgdn", "Stage", "page down", |_| {
        Action::Page(1)
    }),
    bind(PageUp, N, STAGE, "pgup", "Stage", "page up", |_| {
        Action::Page(-1)
    }),
    bind(Char('g'), N, STAGE, "g", "Stage", "top", |_| Action::Top),
    bind(Char('G'), N, STAGE, "G", "Stage", "bottom", |_| {
        Action::Bottom
    }),
    bind(
        Char('h'),
        N,
        STAGE,
        "h",
        "Stage",
        "sidebar / prev lane",
        |_| Action::Move(Dir::Left),
    ),
    bind(
        Left,
        N,
        STAGE,
        "←",
        "Stage",
        "back to sidebar / previous lane",
        |_| Action::Move(Dir::Left),
    ),
    bind(Char('l'), N, STAGE, "l", "Stage", "next lane", |_| {
        Action::Move(Dir::Right)
    }),
    bind(Right, N, STAGE, "→", "Stage", "next lane", |_| {
        Action::Move(Dir::Right)
    }),
    bind(
        Enter,
        N,
        STAGE,
        "enter",
        "Stage",
        "expand / pager / compose",
        |_| Action::Open,
    ),
    bind(
        Char(' '),
        N,
        STAGE,
        "space",
        "Stage",
        "toggle brief / page down",
        |_| Action::StageSpace,
    ),
    bind(
        BackTab,
        N,
        STAGE,
        "shift+tab",
        "Stage",
        "back to sidebar",
        |_| Action::FocusPane(Pane::Sidebar),
    ),
    bind(
        Char('i'),
        N,
        NAV,
        "i",
        "Stage",
        "compose / steer / new task",
        |_| Action::Compose,
    ),
    // Inbox and approvals
    bind(Char('a'), N, NAV, "a", "Inbox", "approve", |_| {
        Action::Accelerator('a')
    }),
    bind(Char('s'), N, NAV, "s", "Inbox", "allow for session", |_| {
        Action::Accelerator('s')
    }),
    bind(Char('d'), N, NAV, "d", "Inbox", "deny", |_| {
        Action::Accelerator('d')
    }),
    bind(Char('r'), N, NAV, "r", "Inbox", "mark card read", |_| {
        Action::Accelerator('r')
    }),
    bind(Char('x'), N, NAV, "x", "Inbox", "dismiss card", |_| {
        Action::Accelerator('x')
    }),
    bind(
        Char('X'),
        N,
        NAV,
        "X",
        "Inbox",
        "dismiss all of this kind",
        |_| Action::Accelerator('X'),
    ),
    // Room
    bind(
        Char('P'),
        N,
        NAV,
        "P",
        "Room",
        "promote to canon letter",
        |_| Action::Promote,
    ),
    // Kanban
    bind(
        Char('B'),
        N,
        NAV,
        "B",
        "Kanban",
        "open project's board",
        |_| Action::OpenProjectBoard,
    ),
    bind(Char('['), N, NAV, "[", "Kanban", "previous lane", |_| {
        Action::PrevLane
    }),
    bind(Char(']'), N, NAV, "]", "Kanban", "next lane", |_| {
        Action::NextLane
    }),
    bind(
        Char('m'),
        N,
        NAV,
        "m",
        "Kanban",
        "move card to next lane",
        |_| Action::MoveCard,
    ),
    bind(Char('n'), N, NAV, "n", "Kanban", "new task", |_| {
        Action::NewTask
    }),
    // Composer
    bind(Enter, N, COMPOSER, "enter", "Composer", "send", |_| {
        Action::Submit
    }),
    bind(
        Enter,
        S,
        COMPOSER,
        "S-enter",
        "Composer",
        "new line",
        |_| Action::Edit(EditOp::Newline),
    ),
    bind(
        Enter,
        A,
        COMPOSER,
        "M-enter",
        "Composer",
        "new line",
        |_| Action::Edit(EditOp::Newline),
    ),
    bind(
        Char('j'),
        C,
        COMPOSER,
        "ctrl+j",
        "Composer",
        "new line",
        |_| Action::Edit(EditOp::Newline),
    ),
    bind(Esc, N, COMPOSER, "esc", "Composer", "cancel draft", |_| {
        Action::Cancel
    }),
    bind(
        Left,
        N,
        COMPOSER,
        "←",
        "Composer",
        "cursor left / right",
        |_| Action::Edit(EditOp::Left),
    ),
    bind(
        Right,
        N,
        COMPOSER,
        "→",
        "Composer",
        "cursor left / right",
        |_| Action::Edit(EditOp::Right),
    ),
    bind(
        Up,
        N,
        COMPOSER,
        "↑",
        "Composer",
        "cursor up / down a line",
        |_| Action::Edit(EditOp::Up),
    ),
    bind(
        Down,
        N,
        COMPOSER,
        "↓",
        "Composer",
        "cursor up / down a line",
        |_| Action::Edit(EditOp::Down),
    ),
    bind(Home, N, COMPOSER, "home", "Composer", "line start", |_| {
        Action::Edit(EditOp::Home)
    }),
    bind(
        Char('a'),
        C,
        COMPOSER,
        "ctrl+a",
        "Composer",
        "line start",
        |_| Action::Edit(EditOp::Home),
    ),
    bind(End, N, COMPOSER, "end", "Composer", "line end", |_| {
        Action::Edit(EditOp::End)
    }),
    bind(
        Char('e'),
        C,
        COMPOSER,
        "ctrl+e",
        "Composer",
        "line end",
        |_| Action::Edit(EditOp::End),
    ),
    bind(
        Backspace,
        N,
        COMPOSER,
        "backspace",
        "Composer",
        "delete back / forward",
        |_| Action::Edit(EditOp::Backspace),
    ),
    bind(
        Delete,
        N,
        COMPOSER,
        "delete",
        "Composer",
        "delete back / forward",
        |_| Action::Edit(EditOp::Delete),
    ),
    bind(
        Char('u'),
        C,
        COMPOSER,
        "ctrl+u",
        "Composer",
        "delete to line start",
        |_| Action::Edit(EditOp::KillLine),
    ),
    bind(
        Char('w'),
        C,
        COMPOSER,
        "ctrl+w",
        "Composer",
        "delete previous word",
        |_| Action::Edit(EditOp::KillWord),
    ),
    // Pager and help overlays
    bind(
        Char('j'),
        N,
        OVERLAY,
        "j",
        "Pager / Help",
        "scroll down / up",
        |_| Action::Move(Dir::Down),
    ),
    bind(
        Down,
        N,
        OVERLAY,
        "↓",
        "Pager / Help",
        "scroll down / up",
        |_| Action::Move(Dir::Down),
    ),
    bind(
        Char('k'),
        N,
        OVERLAY,
        "k",
        "Pager / Help",
        "scroll down / up",
        |_| Action::Move(Dir::Up),
    ),
    bind(
        Up,
        N,
        OVERLAY,
        "↑",
        "Pager / Help",
        "scroll down / up",
        |_| Action::Move(Dir::Up),
    ),
    bind(
        PageDown,
        N,
        OVERLAY,
        "pgdn",
        "Pager / Help",
        "page down / up",
        |_| Action::Page(1),
    ),
    bind(
        Char(' '),
        N,
        OVERLAY,
        "space",
        "Pager / Help",
        "page down / up",
        |_| Action::Page(1),
    ),
    bind(
        PageUp,
        N,
        OVERLAY,
        "pgup",
        "Pager / Help",
        "page down / up",
        |_| Action::Page(-1),
    ),
    bind(
        Char('g'),
        N,
        OVERLAY,
        "g",
        "Pager / Help",
        "top / bottom",
        |_| Action::Top,
    ),
    bind(
        Char('G'),
        N,
        OVERLAY,
        "G",
        "Pager / Help",
        "top / bottom",
        |_| Action::Bottom,
    ),
    bind(Esc, N, OVERLAY, "esc", "Pager / Help", "close", |_| {
        Action::Cancel
    }),
    bind(Char('q'), N, OVERLAY, "q", "Pager / Help", "close", |_| {
        Action::Cancel
    }),
    bind(Char('?'), N, OVERLAY, "?", "Pager / Help", "close", |_| {
        Action::Cancel
    }),
];

fn binding_matches(b: &Binding, key: &KeyEvent) -> bool {
    if b.code != key.code {
        return false;
    }
    match key.code {
        // Terminals disagree on whether shifted characters carry SHIFT; the char itself is the truth.
        Char(_) | BackTab => key.modifiers.difference(S) == b.mods.difference(S),
        _ => key.modifiers == b.mods,
    }
}

pub fn key_action(key: KeyEvent, focus: Pane, cursor: &NodeKey) -> Option<Action> {
    let found = KEYMAP
        .iter()
        .filter(|b| b.panes.contains(&focus))
        .find(|b| binding_matches(b, &key));
    if let Some(b) = found {
        let act = (b.act)(cursor);
        return Some(match act {
            Action::FocusPane(_) if key.code == Tab => {
                Action::FocusPane(if focus == Pane::Sidebar {
                    Pane::Stage
                } else {
                    Pane::Sidebar
                })
            }
            other => other,
        });
    }
    match key.code {
        Char(c)
            if focus == Pane::Composer
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
        {
            Some(Action::Input(c))
        }
        _ => None,
    }
}

/// One merged row of the keymap help overlay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HelpRow {
    pub group: &'static str,
    pub keys: String,
    pub help: &'static str,
}

/// Help rows generated from [`KEYMAP`], grouped in first-appearance order.
pub fn help_rows() -> Vec<HelpRow> {
    let mut groups: Vec<&'static str> = Vec::new();
    for b in KEYMAP {
        if !groups.contains(&b.group) {
            groups.push(b.group);
        }
    }
    let mut rows: Vec<HelpRow> = Vec::new();
    for g in groups {
        for b in KEYMAP.iter().filter(|b| b.group == g) {
            if let Some(row) = rows.iter_mut().find(|r| r.group == g && r.help == b.help) {
                if !row.keys.split(" / ").any(|k| k == b.keys) {
                    row.keys.push_str(" / ");
                    row.keys.push_str(b.keys);
                }
            } else {
                rows.push(HelpRow {
                    group: g,
                    keys: b.keys.to_owned(),
                    help: b.help,
                });
            }
        }
    }
    rows
}

pub fn mouse_action(ev: MouseEvent, hits: &HitMap) -> Option<Action> {
    if let MouseEventKind::Drag(MouseButton::Left) = ev.kind {
        return Some(Action::ResizeDrag(ev.column));
    }
    if let MouseEventKind::Up(MouseButton::Left) = ev.kind {
        return Some(Action::ResizeEnd);
    }
    let target = hits.hit(ev.column, ev.row);
    match (ev.kind, target) {
        (MouseEventKind::Down(MouseButton::Left), Some(Target::Divider)) => {
            Some(Action::ResizeBegin)
        }
        (MouseEventKind::Down(MouseButton::Left), Some(Target::Fold(k))) => {
            Some(Action::ToggleFold(k.clone()))
        }
        (MouseEventKind::Down(MouseButton::Left), Some(Target::Node(k))) => {
            Some(Action::Activate(k.clone()))
        }
        (MouseEventKind::Down(MouseButton::Left), Some(Target::CardButton { id, index })) => {
            Some(Action::RunCardAction {
                id: *id,
                index: *index,
            })
        }
        (
            MouseEventKind::Down(MouseButton::Left),
            Some(Target::AgentDecision {
                wave,
                role,
                interaction,
                approve,
            }),
        ) => Some(Action::ResolveAgent {
            wave: wave.clone(),
            role: role.clone(),
            interaction: interaction.clone(),
            approve: *approve,
        }),
        (MouseEventKind::Down(MouseButton::Left), Some(Target::KanbanCard { lane, card })) => {
            Some(Action::SelectKanbanCard {
                lane: *lane,
                card: *card,
            })
        }
        (MouseEventKind::Down(MouseButton::Left), Some(Target::Message { seq })) => {
            Some(Action::SelectMessage(*seq))
        }
        (MouseEventKind::Down(MouseButton::Left), Some(Target::PromoteButton)) => {
            Some(Action::Promote)
        }
        (MouseEventKind::Down(MouseButton::Left), Some(Target::Composer)) => Some(Action::Compose),
        (
            MouseEventKind::Down(MouseButton::Left),
            Some(Target::Card { .. } | Target::StageBody),
        ) => Some(Action::FocusPane(Pane::Stage)),
        (MouseEventKind::Down(MouseButton::Left), Some(Target::SidebarBody)) => {
            Some(Action::FocusPane(Pane::Sidebar))
        }
        (MouseEventKind::Moved, Some(Target::Node(k) | Target::Fold(k))) => {
            Some(Action::Hover(k.clone()))
        }
        (MouseEventKind::ScrollUp, Some(Target::Overlay)) => Some(Action::Scroll {
            pane: Pane::Overlay,
            lines: -3,
        }),
        (MouseEventKind::ScrollDown, Some(Target::Overlay)) => Some(Action::Scroll {
            pane: Pane::Overlay,
            lines: 3,
        }),
        (
            MouseEventKind::ScrollUp,
            Some(Target::Node(_) | Target::Fold(_) | Target::SidebarBody),
        ) => Some(Action::Scroll {
            pane: Pane::Sidebar,
            lines: -3,
        }),
        (
            MouseEventKind::ScrollDown,
            Some(Target::Node(_) | Target::Fold(_) | Target::SidebarBody),
        ) => Some(Action::Scroll {
            pane: Pane::Sidebar,
            lines: 3,
        }),
        (MouseEventKind::ScrollUp, Some(_)) => Some(Action::Scroll {
            pane: Pane::Stage,
            lines: -3,
        }),
        (MouseEventKind::ScrollDown, Some(_)) => Some(Action::Scroll {
            pane: Pane::Stage,
            lines: 3,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode, mods: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, mods)
    }

    fn act(code: KeyCode, mods: KeyModifiers, pane: Pane) -> Option<Action> {
        key_action(key(code, mods), pane, &NodeKey::Inbox)
    }

    #[test]
    fn inbox_card_keys_reach_the_accelerator() {
        for c in ['a', 'd', 'r', 'x'] {
            assert_eq!(act(Char(c), N, Pane::Stage), Some(Action::Accelerator(c)));
            assert_eq!(act(Char(c), N, Pane::Sidebar), Some(Action::Accelerator(c)));
        }
        assert_eq!(
            act(Char('X'), S, Pane::Stage),
            Some(Action::Accelerator('X'))
        );
    }

    #[test]
    fn composer_rejects_control_chars_and_maps_newlines() {
        assert_eq!(
            act(Char('j'), C, Pane::Composer),
            Some(Action::Edit(EditOp::Newline))
        );
        assert_eq!(act(Char('k'), C, Pane::Composer), None);
        assert_eq!(act(Char('x'), A, Pane::Composer), None);
        assert_eq!(act(Char('J'), S, Pane::Composer), Some(Action::Input('J')));
        assert_eq!(act(Enter, N, Pane::Composer), Some(Action::Submit));
        assert_eq!(
            act(Enter, S, Pane::Composer),
            Some(Action::Edit(EditOp::Newline))
        );
        assert_eq!(
            act(Enter, A, Pane::Composer),
            Some(Action::Edit(EditOp::Newline))
        );
        assert_eq!(act(Char('q'), N, Pane::Composer), Some(Action::Input('q')));
        assert_eq!(act(Char('c'), C, Pane::Composer), Some(Action::Quit));
    }

    #[test]
    fn space_folds_only_in_sidebar_and_backtab_is_scoped() {
        assert_eq!(
            act(Char(' '), N, Pane::Sidebar),
            Some(Action::ToggleFold(NodeKey::Inbox))
        );
        assert_eq!(act(Char(' '), N, Pane::Stage), Some(Action::StageSpace));
        assert_eq!(act(BackTab, S, Pane::Sidebar), Some(Action::CycleFoldAll));
        assert_eq!(
            act(BackTab, S, Pane::Stage),
            Some(Action::FocusPane(Pane::Sidebar))
        );
        assert_eq!(
            act(Tab, N, Pane::Stage),
            Some(Action::FocusPane(Pane::Sidebar))
        );
        assert_eq!(
            act(Tab, N, Pane::Sidebar),
            Some(Action::FocusPane(Pane::Stage))
        );
    }

    #[test]
    fn overlay_keys_scroll_and_close() {
        assert_eq!(act(Char('q'), N, Pane::Overlay), Some(Action::Cancel));
        assert_eq!(act(Char('?'), N, Pane::Overlay), Some(Action::Cancel));
        assert_eq!(act(PageDown, N, Pane::Overlay), Some(Action::Page(1)));
        assert_eq!(act(Char('a'), N, Pane::Overlay), None);
    }

    #[test]
    fn help_rows_cover_every_binding_once() {
        let rows = help_rows();
        for b in KEYMAP {
            let row = rows
                .iter()
                .find(|r| r.group == b.group && r.help == b.help)
                .expect("binding has a help row");
            assert!(row.keys.split(" / ").any(|k| k == b.keys), "{}", b.keys);
        }
        let inbox_r = rows
            .iter()
            .find(|r| r.group == "Inbox" && r.keys == "r")
            .expect("r row");
        assert_eq!(inbox_r.help, "mark card read");
        let sidebar_next = rows
            .iter()
            .find(|r| r.group == "Sidebar" && r.help == "next node")
            .unwrap();
        assert_eq!(sidebar_next.keys, "j / ↓");
    }

    #[test]
    fn no_binding_shadows_another_in_the_same_pane() {
        for (i, a) in KEYMAP.iter().enumerate() {
            for b in &KEYMAP[i + 1..] {
                let overlap = a.panes.iter().any(|p| b.panes.contains(p));
                assert!(
                    !(overlap && a.code == b.code && a.mods == b.mods),
                    "{} shadows {} in {}",
                    a.keys,
                    b.keys,
                    b.group
                );
            }
        }
    }
}
