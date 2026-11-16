//! # rex-wave-tui
//!
//! A pure client of `api::Client`, it could be swapped for a desktop GUI
//! without touching the daemon. Elm-shaped so all behavior is testable
//! without a terminal:
//!
//! ```text
//!  crossterm Event ─┐                       ┌─▶ Effect::Call(WireCommand)  ─▶ client.call_wire
//!  daemon Frame  ───┼─▶ Msg ─▶ update(&mut Model) ─┼─▶ Effect::Retarget(StageTarget) ─▶ drop old watch, open new
//!  debounce tick ───┘                       └─▶ Effect::Terminal(TermOut)  ─▶ OSC 7501 / Kitty APC
//!                     view(&Model) ─▶ ratatui Buffer + HitMap + card placements
//! ```
//!
//! Layout (operator refinement #24): **two panes**.
//! Left = foldable org-tree [`sidebar`]; right = [`stage`] driven by the
//! focused/hovered node. No mode tabs. Keyboard and mouse both produce the
//! same [`input::Action`]s, so there is exactly one behavior per gesture.

pub mod input;
pub mod kitty;
pub mod sidebar;
pub mod stage;

use input::{Action, HitMap, Pane};
use kitty::{CardPlacement, TermOut};
use rex_wave_api::read::SidebarTreeSnapshot;
use rex_wave_api::*;
use sidebar::Sidebar;
use stage::{Stage, StageTarget};

/// Everything the TUI knows. Derived from daemon frames; never authoritative.
pub struct Model {
    pub me: Principal,
    pub sidebar_snapshot: SidebarTreeSnapshot,
    pub sidebar: Sidebar,
    pub stage: Stage,
    pub focus: Pane,
    pub graphics: kitty::GraphicsMode,
    pub quit: bool,
}

/// Inputs to `update`.
#[derive(Debug)]
pub enum Msg {
    Action(Action),
    /// A frame from the sidebar watch (overview-affecting) or the stage watch.
    Frame { watch: WatchSlot, frame: Frame },
    Reply { tag: ReplyTag, result: Result<WireReply, ApiError> },
    /// Hover settled for `stage::HOVER_DEBOUNCE`; commit the retarget.
    HoverSettled(sidebar::NodeKey),
    Resize { cols: u16, rows: u16 },
}

/// Which subscription a frame came from. The stage slot is replaced on retarget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchSlot {
    Tree,
    Stage,
}

/// Correlates async replies with what asked for them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplyTag {
    Sidebar,
    StageInitial,
    UserCommand,
}

/// Outputs of `update`, performed by the shell in [`run`].
#[derive(Debug)]
pub enum Effect {
    Call { tag: ReplyTag, cmd: WireCommand },
    /// Replace the stage watch: drop the old `FrameStream`, open `subscriptions(target)`.
    Retarget(StageTarget),
    /// Start/restart the hover debounce timer.
    DebounceHover(sidebar::NodeKey),
    Terminal(TermOut),
    Quit,
}

/// Pure state transition. All UI policy lives here.
pub fn update(model: &mut Model, msg: Msg) -> Vec<Effect> {
    let _ = (model, msg);
    // Action::Move/Fold/Unfold/Toggle -> sidebar ops; cursor change -> DebounceHover(key)
    // Action::Activate on node        -> Retarget(stage::target_for(key)) immediately
    // HoverSettled(key)               -> Retarget(stage::target_for(key)) if changed
    // Frame{Tree}                     -> overview fold-in -> sidebar.rebuild(); OSC 7501 via kitty::program_status
    // Frame{Stage}                    -> stage.apply(frame)
    // Action::RunCardAction{..}       -> Call(notification.actions[i].run) (the card carries its command)
    // Action::Submit                  -> Call(stage composer -> room_post | wave_tell)
    todo!("update")
}

/// Pure render into the frame; records hit regions and Kitty card placements.
pub fn view(model: &Model, frame: &mut ratatui::Frame<'_>, hits: &mut HitMap, cards: &mut Vec<CardPlacement>) {
    let _ = (model, frame, hits, cards);
    // split: [sidebar: Length(32) | stage: Min(40)]
    // sidebar::render(&model.sidebar, area, buf, hits)
    // stage::render(&model.stage, area, buf, hits, cards)   (card interiors stay Color::Reset, Rule A)
    todo!("view")
}

/// The shell: terminal setup, input thread, watches, effect execution,
/// DECSET 2026 synchronized frames around draw + Kitty placements (Rule B).
pub async fn run<C: Client + Clone + 'static>(client: C) -> std::io::Result<()> {
    let _ = client;
    todo!("run")
}
