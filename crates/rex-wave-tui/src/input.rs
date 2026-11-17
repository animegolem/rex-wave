//! Keyboard and mouse input mapped onto one `Action` vocabulary, plus frame hit-testing.

use crate::sidebar::NodeKey;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::{Position, Rect};
use rex_wave_api::{BbInteractionId, RoleName, WaveSlug, notify::NotificationId};

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Move(Dir),
    Top,
    Bottom,
    PrevLane,
    NextLane,
    MoveCard,
    NewTask,
    ToggleFold(NodeKey),
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
    Compose,
    Submit,
    Cancel,
    Input(char),
    Backspace,
    Promote,
    CycleGraphics,
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
    GraphicsToggle,
    Composer,
    SidebarBody,
    StageBody,
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

pub fn key_action(key: KeyEvent, focus: Pane, cursor: &NodeKey) -> Option<Action> {
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        return Some(Action::Quit);
    }
    if focus == Pane::Composer {
        return match key.code {
            KeyCode::Enter => Some(Action::Submit),
            KeyCode::Esc => Some(Action::Cancel),
            KeyCode::Backspace => Some(Action::Backspace),
            KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::ALT) => {
                Some(Action::Input(c))
            }
            _ => None,
        };
    }

    Some(match (key.code, key.modifiers) {
        (KeyCode::Char('q'), _) => Action::Quit,
        (KeyCode::Esc, _) => Action::Cancel,
        (KeyCode::Char('j') | KeyCode::Down, _) => Action::Move(Dir::Down),
        (KeyCode::Char('k') | KeyCode::Up, _) => Action::Move(Dir::Up),
        (KeyCode::Char('h') | KeyCode::Left, _) => Action::Move(Dir::Left),
        (KeyCode::Char('l') | KeyCode::Right, _) => Action::Move(Dir::Right),
        (KeyCode::Char('g'), KeyModifiers::NONE) => Action::Top,
        (KeyCode::Char('G'), _) => Action::Bottom,
        (KeyCode::Char('['), _) => Action::PrevLane,
        (KeyCode::Char(']'), _) => Action::NextLane,
        (KeyCode::Char('m'), KeyModifiers::NONE) => Action::MoveCard,
        (KeyCode::Char('n'), KeyModifiers::NONE) => Action::NewTask,
        (KeyCode::Char(' '), _) => Action::ToggleFold(cursor.clone()),
        (KeyCode::Enter, _) => {
            if focus == Pane::Sidebar {
                Action::Activate(cursor.clone())
            } else {
                Action::Compose
            }
        }
        (KeyCode::Tab, _) => Action::FocusPane(if focus == Pane::Sidebar {
            Pane::Stage
        } else {
            Pane::Sidebar
        }),
        (KeyCode::Char('i'), KeyModifiers::NONE) => Action::Compose,
        (KeyCode::Char('P'), _) => Action::Promote,
        (KeyCode::Char('v'), KeyModifiers::NONE) => Action::CycleGraphics,
        (KeyCode::Char('a'), KeyModifiers::NONE) => Action::Accelerator('a'),
        (KeyCode::Char('d'), KeyModifiers::NONE) => Action::Accelerator('d'),
        _ => return None,
    })
}

pub fn mouse_action(ev: MouseEvent, hits: &HitMap) -> Option<Action> {
    let target = hits.hit(ev.column, ev.row);
    match (ev.kind, target) {
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
        (MouseEventKind::Down(MouseButton::Left), Some(Target::GraphicsToggle)) => {
            Some(Action::CycleGraphics)
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
