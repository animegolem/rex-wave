//! Keyboard + mouse -> one `Action` vocabulary, and mouse hit-testing.

use crate::sidebar::NodeKey;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::{Position, Rect};
use rex_wave_api::notify::NotificationId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pane {
    Sidebar,
    Stage,
    Composer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dir {
    Up,
    Down,
    Left,
    Right,
}

/// Everything a user can do. Produced identically by keys and by mouse.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Move(Dir),
    /// `Space` / click on fold arrow.
    ToggleFold(NodeKey),
    /// `Enter` / click on a node: focus it and retarget the stage now.
    Activate(NodeKey),
    /// Mouse moved over a node (debounced before retargeting).
    Hover(NodeKey),
    FocusPane(Pane),
    Scroll { pane: Pane, lines: i16 },
    /// Run button `index` on a notification card (`a`/`d`/... accelerators or click).
    RunCardAction { id: NotificationId, index: usize },
    /// Start composing (`i`) / submit (`Enter` in composer) / cancel (`Esc`).
    Compose,
    Submit,
    Cancel,
    Input(char),
    Backspace,
    /// `P` on a selected message range: promote_to_letter.
    Promote,
    CycleGraphics,
    Quit,
}

/// What lives under a screen rectangle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    Node(NodeKey),
    Fold(NodeKey),
    Card { id: NotificationId },
    CardButton { id: NotificationId, index: usize },
    Message { seq: u64 },
    Composer,
    StageBody,
}

/// Hit regions recorded during `view`. Later pushes are on top (last-wins).
#[derive(Default, Debug)]
pub struct HitMap {
    regions: Vec<(Rect, Target)>,
}

impl HitMap {
    pub fn clear(&mut self) {
        self.regions.clear();
    }
    pub fn push(&mut self, area: Rect, target: Target) {
        self.regions.push((area, target));
    }
    /// Topmost target containing the cell.
    pub fn hit(&self, col: u16, row: u16) -> Option<&Target> {
        let p = Position { x: col, y: row };
        self.regions.iter().rev().find(|(r, _)| r.contains(p)).map(|(_, t)| t)
    }
}

/// Pure keymap. `hjkl` and arrows are synonyms; behavior depends on the focused pane.
pub fn key_action(key: KeyEvent, focus: Pane, cursor: &NodeKey) -> Option<Action> {
    if focus == Pane::Composer {
        return match key.code {
            KeyCode::Enter => Some(Action::Submit),
            KeyCode::Esc => Some(Action::Cancel),
            KeyCode::Backspace => Some(Action::Backspace),
            KeyCode::Char(c) => Some(Action::Input(c)),
            _ => None,
        };
    }
    Some(match (key.code, key.modifiers) {
        (KeyCode::Char('q'), _) | (KeyCode::Char('c'), KeyModifiers::CONTROL) => Action::Quit,
        (KeyCode::Char('j') | KeyCode::Down, _) => Action::Move(Dir::Down),
        (KeyCode::Char('k') | KeyCode::Up, _) => Action::Move(Dir::Up),
        (KeyCode::Char('h') | KeyCode::Left, _) => Action::Move(Dir::Left),
        (KeyCode::Char('l') | KeyCode::Right, _) => Action::Move(Dir::Right),
        (KeyCode::Char(' '), _) => Action::ToggleFold(cursor.clone()),
        (KeyCode::Enter, _) => Action::Activate(cursor.clone()),
        (KeyCode::Tab, _) => Action::FocusPane(if focus == Pane::Sidebar { Pane::Stage } else { Pane::Sidebar }),
        (KeyCode::Char('i'), _) => Action::Compose,
        (KeyCode::Char('P'), _) => Action::Promote,
        (KeyCode::Char('g'), _) => Action::CycleGraphics,
        _ => return None,
    })
}

/// Pure mouse map over the last frame's hit regions.
pub fn mouse_action(ev: MouseEvent, hits: &HitMap) -> Option<Action> {
    let target = hits.hit(ev.column, ev.row);
    match (ev.kind, target) {
        (MouseEventKind::Down(MouseButton::Left), Some(Target::Fold(k))) => Some(Action::ToggleFold(k.clone())),
        (MouseEventKind::Down(MouseButton::Left), Some(Target::Node(k))) => Some(Action::Activate(k.clone())),
        (MouseEventKind::Down(MouseButton::Left), Some(Target::CardButton { id, index })) => {
            Some(Action::RunCardAction { id: *id, index: *index })
        }
        (MouseEventKind::Down(MouseButton::Left), Some(Target::Composer)) => Some(Action::Compose),
        (MouseEventKind::Moved, Some(Target::Node(k))) => Some(Action::Hover(k.clone())),
        (MouseEventKind::ScrollUp, Some(Target::Node(_) | Target::Fold(_))) => Some(Action::Scroll { pane: Pane::Sidebar, lines: -3 }),
        (MouseEventKind::ScrollDown, Some(Target::Node(_) | Target::Fold(_))) => Some(Action::Scroll { pane: Pane::Sidebar, lines: 3 }),
        (MouseEventKind::ScrollUp, Some(_)) => Some(Action::Scroll { pane: Pane::Stage, lines: -3 }),
        (MouseEventKind::ScrollDown, Some(_)) => Some(Action::Scroll { pane: Pane::Stage, lines: 3 }),
        _ => None,
    }
}
