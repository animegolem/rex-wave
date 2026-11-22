//! Two-pane Rex TUI client: left foldable org-tree sidebar + right
//! context-sensitive main stage driven over `rex_wave_api::Client`.

pub mod composer;
pub mod demo;
pub mod input;
pub mod kitty;
pub mod overlay;
pub mod richtext;
pub mod scroll;
pub mod sidebar;
pub mod stage;

use composer::Composer;
use crossterm::{
    event::{
        self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
        Event as CtEvent, KeyEventKind, KeyboardEnhancementFlags, PopKeyboardEnhancementFlags,
        PushKeyboardEnhancementFlags,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use input::{Action, Dir, EditOp, HitMap, Pane, Target};
use kitty::{
    CardPlacement, CellPx, GraphicsMode, KittyCompositor, Osc7501, TermOut, encode, program_status,
};
use overlay::Overlay;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Position, Rect},
};
use rex_wave_api::{
    board::{BoardEnsure, BoardId, CardPatch, CardUpsert, LaneName, Title, project_board},
    mail::{LetterPriority, MailPromote},
    notify::{InboxAck, InboxMarkRead, NotifyKind, NotifyState},
    read::{Page, SidebarTreeSnapshot},
    room::{Body, RoomId, RoomPost, RoomRef},
    wave::{InteractionResolution, SteerDeliveryMode, WaveResolve, WaveTell},
    *,
};
use sidebar::{NodeKey, Sidebar};
use stage::{
    AgentInspector, HOVER_DEBOUNCE, INBOX_CARD_HEIGHT, INBOX_CARD_STRIDE, InboxView, KanbanView,
    RoomView, Stage, StageRenderCtx, StageTarget, find_blocked_worker_for_room, plan_for,
    target_for,
};
use std::{
    cell::Cell,
    collections::BTreeMap,
    fmt,
    io::{self, Write as _},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{mpsc, watch};

const ROOM_PAGE_LIMIT: u32 = 200;
const DEFAULT_SIDEBAR_WIDTH: u16 = 38;
const MIN_SIDEBAR_WIDTH: u16 = 18;
const MAX_SIDEBAR_WIDTH: u16 = 80;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ComposerTarget {
    Room { room: RoomId },
    Agent { wave: WaveSlug, role: RoleName },
    NewTask { board: BoardId, lane: LaneName },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ConnectionState {
    #[default]
    Connected,
    Reconnecting,
    Disconnected,
}

/// Debug-printable wrapper around a live `FrameStream` returned from an async stage open.
pub struct OpenedWatch(pub FrameStream);

impl fmt::Debug for OpenedWatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OpenedWatch(..)")
    }
}

pub struct Model {
    pub me: Principal,
    pub sidebar_snapshot: SidebarTreeSnapshot,
    pub sidebar: Sidebar,
    pub stage: Stage,
    pub active_target: StageTarget,
    pub active_generation: FocusGeneration,
    pub stage_req_seq: u64,
    has_focused_watch: bool,
    pub focus: Pane,
    prev_focus: Pane,
    pub composer_target: Option<ComposerTarget>,
    pub overlay: Option<Overlay>,
    pub graphics: GraphicsMode,
    pub sidebar_width: u16,
    pub sidebar_hidden: bool,
    resizing_sidebar: bool,
    pub active_project: Option<BbProjectId>,
    pub connection: ConnectionState,
    pub has_connection_watch: bool,
    pub term_cols: u16,
    pub term_rows: u16,
    pub last_error: Option<String>,
    pub quit: bool,
}

impl Model {
    pub fn new(me: Principal, sidebar_snapshot: SidebarTreeSnapshot) -> Self {
        let sidebar = Sidebar::new(&sidebar_snapshot);
        let active_project = sidebar.project_of(&sidebar.cursor);
        let active_target =
            target_for_node(&sidebar.cursor, active_project.as_ref(), &sidebar_snapshot);
        Self {
            me,
            sidebar_snapshot,
            sidebar,
            stage: Stage::Loading(active_target.clone()),
            active_target,
            active_generation: FocusGeneration::default(),
            stage_req_seq: 1,
            has_focused_watch: false,
            focus: Pane::Sidebar,
            prev_focus: Pane::Sidebar,
            composer_target: None,
            overlay: None,
            graphics: GraphicsMode::Hybrid,
            sidebar_width: DEFAULT_SIDEBAR_WIDTH,
            sidebar_hidden: false,
            resizing_sidebar: false,
            active_project,
            connection: ConnectionState::Connected,
            has_connection_watch: false,
            term_cols: 124,
            term_rows: 34,
            last_error: None,
            quit: false,
        }
    }
}

#[derive(Debug)]
pub enum Msg {
    Action(Action),
    ConnectionChanged(bool),
    ConnectionClosed,
    TreeWatchOpened(Result<OpenedWatch, ApiError>),
    Frame {
        watch: WatchSlot,
        frame: Frame,
    },
    WatchClosed {
        watch: WatchSlot,
        error: Option<ApiError>,
    },
    StageWatchOpened {
        seq: u64,
        watch: Result<Option<OpenedWatch>, ApiError>,
    },
    Reply {
        tag: ReplyTag,
        result: Result<WireReply, ApiError>,
    },
    HoverSettled(NodeKey),
    Resize {
        cols: u16,
        rows: u16,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchSlot {
    Tree,
    Stage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplyTag {
    Sidebar,
    StageInitial(u64),
    UserCommand,
}

#[derive(Debug)]
pub enum Effect {
    Call { tag: ReplyTag, cmd: WireCommand },
    CallBatch { cmds: Vec<WireCommand> },
    EnsureTreeWatch,
    Retarget(StageTarget),
    DebounceHover(NodeKey),
    Terminal(TermOut),
    Quit,
}

fn inbox_viewport_rows(term_rows: u16) -> u16 {
    term_rows.saturating_sub(3).max(INBOX_CARD_HEIGHT)
}

fn max_sidebar_width(term_cols: u16) -> u16 {
    term_cols
        .saturating_sub(24)
        .clamp(MIN_SIDEBAR_WIDTH, MAX_SIDEBAR_WIDTH)
}

fn target_for_node(
    key: &NodeKey,
    active_project: Option<&BbProjectId>,
    overview: &SidebarTreeSnapshot,
) -> StageTarget {
    match key {
        NodeKey::Kanban => {
            let board = active_project
                .map(project_board)
                .filter(|candidate| overview.kanban.iter().any(|b| &b.board == candidate));
            StageTarget::Kanban { board }
        }
        other => target_for(other),
    }
}

fn remember_project(model: &mut Model, key: &NodeKey) {
    if let Some(proj) = model.sidebar.project_of(key) {
        model.active_project = Some(proj);
    }
}

fn is_transport_error(err: &ApiError) -> bool {
    matches!(err, ApiError::Transport(_))
}

fn mark_transport_drop(model: &mut Model) {
    if !model.has_connection_watch && model.connection == ConnectionState::Connected {
        model.connection = ConnectionState::Reconnecting;
    }
}

fn mark_connected(
    model: &mut Model,
    effects: &mut Vec<Effect>,
    fetch_sidebar: bool,
    authoritative: bool,
) {
    if !authoritative && model.has_connection_watch {
        return;
    }
    let was_down = model.connection != ConnectionState::Connected;
    model.connection = ConnectionState::Connected;
    if !was_down {
        return;
    }
    model.last_error = None;
    effects.push(Effect::EnsureTreeWatch);
    if fetch_sidebar {
        effects.push(Effect::Call {
            tag: ReplyTag::Sidebar,
            cmd: WireCommand::Sidebar(read::Sidebar::default()),
        });
    }
    if !matches!(model.active_target, StageTarget::Summary { .. }) {
        model.stage_req_seq = model.stage_req_seq.wrapping_add(1);
        if model.has_focused_watch {
            model.active_generation = model.active_generation.next();
            model.has_focused_watch = false;
        }
        if matches!(model.stage, Stage::Error { .. }) {
            model.stage = Stage::Loading(model.active_target.clone());
        }
        effects.push(Effect::Retarget(model.active_target.clone()));
    }
}

pub fn update(model: &mut Model, msg: Msg) -> Vec<Effect> {
    let mut effects = Vec::new();
    match msg {
        Msg::Action(action) => {
            if !matches!(
                action,
                Action::Hover(_)
                    | Action::Scroll { .. }
                    | Action::ResizeDrag(_)
                    | Action::ResizeEnd
            ) {
                model.last_error = None;
            }
            handle_action(model, action, &mut effects);
        }
        Msg::ConnectionChanged(up) => {
            if up {
                mark_connected(model, &mut effects, true, true);
            } else {
                model.connection = ConnectionState::Reconnecting;
            }
        }
        Msg::ConnectionClosed => {
            model.connection = ConnectionState::Disconnected;
            if model.last_error.is_none() {
                model.last_error = Some("daemon disconnected".to_owned());
            }
        }
        Msg::TreeWatchOpened(res) => {
            if let Err(err) = res {
                if is_transport_error(&err) {
                    mark_transport_drop(model);
                }
                model.last_error = Some(format!("sidebar watch: {err}"));
            }
        }
        Msg::HoverSettled(key) => {
            if !matches!(model.focus, Pane::Composer | Pane::Overlay)
                && (model.sidebar.hover.as_ref() == Some(&key) || model.sidebar.cursor == key)
            {
                remember_project(model, &key);
                let target =
                    target_for_node(&key, model.active_project.as_ref(), &model.sidebar_snapshot);
                retarget_if_changed(model, target, &mut effects);
            }
        }
        Msg::Frame {
            watch: WatchSlot::Tree,
            frame: Frame::Sidebar(snap),
        } => {
            mark_connected(model, &mut effects, false, false);
            let prev = model.sidebar.cursor.clone();
            model.sidebar_snapshot = *snap;
            model.sidebar.rebuild(&model.sidebar_snapshot);
            if model.sidebar.cursor != prev {
                effects.push(Effect::DebounceHover(model.sidebar.cursor.clone()));
            }
        }
        Msg::Frame {
            watch: WatchSlot::Stage,
            frame,
        } => {
            mark_connected(model, &mut effects, true, false);
            match frame {
                Frame::StageFocused { generation, .. } => {
                    if generation >= model.active_generation {
                        model.active_generation = generation;
                        model.has_focused_watch = true;
                    }
                }
                Frame::Stage { generation, inner } => {
                    if generation >= model.active_generation {
                        model.active_generation = generation;
                        model.has_focused_watch = true;
                        model.stage.apply(&inner);
                        sync_agent_ctx_from_stage(model);
                    }
                }
                other => {
                    model.stage.apply(&other);
                    sync_agent_ctx_from_stage(model);
                }
            }
        }
        Msg::Frame { .. } => {
            mark_connected(model, &mut effects, true, false);
        }
        Msg::WatchClosed { watch, error } => match watch {
            WatchSlot::Tree => {
                model.connection = ConnectionState::Disconnected;
                let detail = match error {
                    Some(e) => format!("daemon disconnected: {e}"),
                    None => "daemon disconnected".to_owned(),
                };
                model.last_error = Some(detail);
            }
            WatchSlot::Stage => match error {
                Some(err) => {
                    if is_transport_error(&err) {
                        mark_transport_drop(model);
                    }
                    if matches!(model.stage, Stage::Loading(_)) {
                        model.stage = Stage::Error {
                            target: model.active_target.clone(),
                            error: err.to_string(),
                        };
                    } else {
                        model.last_error = Some(err.to_string());
                    }
                }
                None => {
                    if model.connection == ConnectionState::Connected
                        && model.has_focused_watch
                        && !matches!(model.active_target, StageTarget::Summary { .. })
                    {
                        model.has_focused_watch = false;
                        model.stage_req_seq = model.stage_req_seq.wrapping_add(1);
                        effects.push(Effect::Retarget(model.active_target.clone()));
                    }
                }
            },
        },
        Msg::StageWatchOpened { seq, watch } => {
            if seq == model.stage_req_seq
                && let Err(err) = watch
            {
                if is_transport_error(&err) {
                    mark_transport_drop(model);
                }
                if matches!(model.stage, Stage::Loading(_)) {
                    model.stage = Stage::Error {
                        target: model.active_target.clone(),
                        error: err.to_string(),
                    };
                } else {
                    model.last_error = Some(err.to_string());
                }
            }
        }
        Msg::Reply { tag, result } => handle_reply(model, tag, result, &mut effects),
        Msg::Resize { cols, rows } => {
            model.term_cols = cols;
            model.term_rows = rows;
            model.sidebar_width = model
                .sidebar_width
                .clamp(MIN_SIDEBAR_WIDTH, max_sidebar_width(cols));
            if let Stage::Inbox(inbox) = &mut model.stage {
                inbox.keep_selected_visible(inbox_viewport_rows(model.term_rows));
            }
        }
    }
    effects
}

fn open_overlay(model: &mut Model, overlay: Overlay) {
    if model.focus != Pane::Overlay {
        model.prev_focus = if model.focus == Pane::Composer {
            Pane::Stage
        } else {
            model.focus
        };
    }
    model.composer_target = None;
    model.overlay = Some(overlay);
    model.focus = Pane::Overlay;
}

fn close_overlay(model: &mut Model) {
    model.overlay = None;
    model.focus = if model.sidebar_hidden && model.prev_focus == Pane::Sidebar {
        Pane::Stage
    } else {
        model.prev_focus
    };
}

fn open_composer(model: &mut Model) {
    match &model.stage {
        Stage::Room(r) => {
            model.composer_target = Some(ComposerTarget::Room {
                room: r.room.clone(),
            });
            model.focus = Pane::Composer;
        }
        Stage::Agent(a) => {
            model.composer_target = Some(ComposerTarget::Agent {
                wave: a.wave.clone(),
                role: a.role.clone(),
            });
            model.focus = Pane::Composer;
        }
        Stage::Kanban(k) => {
            let lane = k
                .snapshot
                .lanes
                .get(k.cursor.0)
                .map(|l| l.lane.clone())
                .unwrap_or_else(|| LaneName::parse("todo").expect("valid lane"));
            model.composer_target = Some(ComposerTarget::NewTask {
                board: k.snapshot.board.clone(),
                lane,
            });
            model.focus = Pane::Composer;
        }
        _ => {}
    }
}

fn handle_action(model: &mut Model, action: Action, effects: &mut Vec<Effect>) {
    match action {
        Action::Quit => {
            model.quit = true;
            effects.push(Effect::Terminal(TermOut::DeleteAll));
            effects.push(Effect::Terminal(TermOut::ProgramStatus(Osc7501::clear())));
            effects.push(Effect::Quit);
        }
        Action::CycleGraphics => {
            model.graphics = model.graphics.next();
        }
        Action::ToggleHelp => {
            if matches!(model.overlay, Some(Overlay::Help { .. })) {
                close_overlay(model);
            } else {
                open_overlay(model, Overlay::help());
            }
        }
        Action::SidebarWidth(delta) => {
            model.sidebar_hidden = false;
            let max_w = i32::from(max_sidebar_width(model.term_cols));
            let next = (i32::from(model.sidebar_width) + i32::from(delta))
                .clamp(i32::from(MIN_SIDEBAR_WIDTH), max_w);
            model.sidebar_width = next as u16;
        }
        Action::ToggleSidebar => {
            model.sidebar_hidden = !model.sidebar_hidden;
            if model.sidebar_hidden && model.focus == Pane::Sidebar {
                model.focus = Pane::Stage;
            }
        }
        Action::ResizeBegin => {
            model.resizing_sidebar = true;
        }
        Action::ResizeDrag(col) => {
            if model.resizing_sidebar {
                model.sidebar_hidden = false;
                let max_w = max_sidebar_width(model.term_cols);
                model.sidebar_width = col.saturating_add(1).clamp(MIN_SIDEBAR_WIDTH, max_w);
            }
        }
        Action::ResizeEnd => {
            model.resizing_sidebar = false;
        }
        Action::FocusPane(pane) => {
            model.overlay = None;
            if pane != Pane::Composer {
                model.composer_target = None;
            }
            if pane == Pane::Sidebar && model.sidebar_hidden {
                model.sidebar_hidden = false;
            }
            model.focus = pane;
        }
        Action::ToggleFold(key) => {
            let prev = model.sidebar.cursor.clone();
            model.sidebar.folds.toggle(&key);
            model.sidebar.rebuild(&model.sidebar_snapshot);
            if model.sidebar.cursor != prev {
                effects.push(Effect::DebounceHover(model.sidebar.cursor.clone()));
            }
        }
        Action::CycleFoldAll => {
            let prev = model.sidebar.cursor.clone();
            model.sidebar.cycle_fold_all(&model.sidebar_snapshot);
            if model.sidebar.cursor != prev {
                effects.push(Effect::DebounceHover(model.sidebar.cursor.clone()));
            }
        }
        Action::ToggleArchive(key) => {
            if let Some((target, archived)) =
                sidebar::archive_target_for(&model.sidebar_snapshot, &key)
            {
                effects.push(Effect::Call {
                    tag: ReplyTag::UserCommand,
                    cmd: WireCommand::SetArchived(SetArchived {
                        target,
                        archived: !archived,
                    }),
                });
            }
        }
        Action::ToggleShowArchived => {
            let prev = model.sidebar.cursor.clone();
            model.sidebar.show_archived = !model.sidebar.show_archived;
            model.sidebar.rebuild(&model.sidebar_snapshot);
            if model.sidebar.cursor != prev {
                effects.push(Effect::DebounceHover(model.sidebar.cursor.clone()));
            }
        }
        Action::Activate(key) => {
            remember_project(model, &key);
            model.sidebar.cursor = key.clone();
            model.sidebar.hover = None;
            let target =
                target_for_node(&key, model.active_project.as_ref(), &model.sidebar_snapshot);
            if model.active_target == target && !matches!(model.stage, Stage::Loading(_)) {
                model.focus = Pane::Stage;
            } else {
                retarget_if_changed(model, target, effects);
            }
        }
        Action::OpenProjectBoard => {
            remember_project(model, &model.sidebar.cursor.clone());
            let board = model
                .active_project
                .as_ref()
                .map(project_board)
                .or_else(|| {
                    model
                        .sidebar_snapshot
                        .projects
                        .iter()
                        .find(|p| !p.archived)
                        .or_else(|| model.sidebar_snapshot.projects.first())
                        .map(|p| project_board(&p.project))
                });
            model.sidebar.cursor = NodeKey::Kanban;
            retarget_if_changed(model, StageTarget::Kanban { board }, effects);
            model.focus = Pane::Stage;
        }
        Action::Hover(key) => {
            if model.sidebar.hover.as_ref() != Some(&key) {
                model.sidebar.hover = Some(key.clone());
                effects.push(Effect::DebounceHover(key));
            }
        }
        Action::Select(delta) => {
            let vp = inbox_viewport_rows(model.term_rows);
            select_in_stage(&mut model.stage, delta, vp);
        }
        Action::Page(dir) => match model.focus {
            Pane::Overlay => {
                if let Some(ov) = &mut model.overlay {
                    ov.scroll_mut().by_pages(i32::from(dir));
                }
            }
            Pane::Sidebar => {
                let prev = model.sidebar.cursor.clone();
                let rows = i32::from(model.term_rows.saturating_sub(4).max(1));
                model.sidebar.move_cursor(i32::from(dir) * rows);
                if model.sidebar.cursor != prev {
                    effects.push(Effect::DebounceHover(model.sidebar.cursor.clone()));
                }
            }
            Pane::Stage | Pane::Composer => {
                page_stage(&mut model.stage, dir, inbox_viewport_rows(model.term_rows));
            }
        },
        Action::Move(dir) => match model.focus {
            Pane::Overlay => {
                if let Some(ov) = &mut model.overlay {
                    match dir {
                        Dir::Down => ov.scroll_mut().by(1),
                        Dir::Up => ov.scroll_mut().by(-1),
                        _ => {}
                    }
                }
            }
            Pane::Sidebar => {
                let prev = model.sidebar.cursor.clone();
                match dir {
                    Dir::Down => model.sidebar.move_cursor(1),
                    Dir::Up => model.sidebar.move_cursor(-1),
                    Dir::Left => model.sidebar.fold_or_parent(),
                    Dir::Right => model.sidebar.unfold_or_child(),
                }
                if model.sidebar.cursor != prev {
                    effects.push(Effect::DebounceHover(model.sidebar.cursor.clone()));
                }
            }
            Pane::Stage | Pane::Composer => {
                let vp = inbox_viewport_rows(model.term_rows);
                move_in_stage(
                    &mut model.stage,
                    dir,
                    &mut model.focus,
                    &mut model.sidebar_hidden,
                    vp,
                );
            }
        },
        Action::Top => match model.focus {
            Pane::Overlay => {
                if let Some(ov) = &mut model.overlay {
                    ov.scroll_mut().to_top();
                }
            }
            Pane::Sidebar => {
                let prev = model.sidebar.cursor.clone();
                model.sidebar.move_top();
                if model.sidebar.cursor != prev {
                    effects.push(Effect::DebounceHover(model.sidebar.cursor.clone()));
                }
            }
            Pane::Stage | Pane::Composer => {
                scroll_stage_extreme(&mut model.stage, true, inbox_viewport_rows(model.term_rows));
            }
        },
        Action::Bottom => match model.focus {
            Pane::Overlay => {
                if let Some(ov) = &mut model.overlay {
                    ov.scroll_mut().to_bottom();
                }
            }
            Pane::Sidebar => {
                let prev = model.sidebar.cursor.clone();
                model.sidebar.move_bottom();
                if model.sidebar.cursor != prev {
                    effects.push(Effect::DebounceHover(model.sidebar.cursor.clone()));
                }
            }
            Pane::Stage | Pane::Composer => {
                scroll_stage_extreme(
                    &mut model.stage,
                    false,
                    inbox_viewport_rows(model.term_rows),
                );
            }
        },
        Action::PrevLane => {
            if let Stage::Kanban(k) = &mut model.stage
                && k.cursor.0 > 0
            {
                k.cursor.0 -= 1;
                k.clamp_cursor();
            }
        }
        Action::NextLane => {
            if let Stage::Kanban(k) = &mut model.stage
                && k.cursor.0 + 1 < k.snapshot.lanes.len()
            {
                k.cursor.0 += 1;
                k.clamp_cursor();
            }
        }
        Action::SelectKanbanCard { lane, card } => {
            if let Stage::Kanban(k) = &mut model.stage {
                k.cursor = (lane, card);
                k.clamp_cursor();
                model.focus = Pane::Stage;
            }
        }
        Action::MoveCard => {
            if let Stage::Kanban(k) = &model.stage
                && let (Some(card), Some(next_lane)) = (k.selected_card(), k.next_lane_for_move())
            {
                effects.push(Effect::Call {
                    tag: ReplyTag::UserCommand,
                    cmd: WireCommand::CardUpsert(CardUpsert {
                        board: k.snapshot.board.clone(),
                        card: Some(card.no),
                        expected: Some(card.revision),
                        patch: CardPatch {
                            lane: Some(next_lane),
                            ..CardPatch::default()
                        },
                        idem: None,
                    }),
                });
            }
        }
        Action::NewTask => {
            if let Stage::Kanban(k) = &model.stage {
                let lane = k
                    .snapshot
                    .lanes
                    .get(k.cursor.0)
                    .map(|l| l.lane.clone())
                    .unwrap_or_else(|| LaneName::parse("todo").expect("valid lane"));
                model.composer_target = Some(ComposerTarget::NewTask {
                    board: k.snapshot.board.clone(),
                    lane,
                });
                model.focus = Pane::Composer;
            }
        }
        Action::Open => handle_stage_open(model, effects),
        Action::StageSpace => {
            let vp = inbox_viewport_rows(model.term_rows);
            match &mut model.stage {
                Stage::Room(r) => {
                    if !r.toggle_focused_brief() {
                        r.scroll.by_pages(1);
                    }
                }
                Stage::Agent(a) => a.scroll.by_pages(1),
                other => page_stage(other, 1, vp),
            }
        }
        Action::Compose => open_composer(model),
        Action::Input(ch) => {
            if let Some(buf) = active_composer_mut(&mut model.stage) {
                buf.insert_char(ch);
            }
        }
        Action::Paste(text) => {
            if let Some(buf) = active_composer_mut(&mut model.stage) {
                buf.insert_str(&text);
            }
        }
        Action::Edit(op) => {
            if let Some(buf) = active_composer_mut(&mut model.stage) {
                match op {
                    EditOp::Newline => buf.newline(),
                    EditOp::Backspace => buf.backspace(),
                    EditOp::Delete => buf.delete(),
                    EditOp::Left => buf.move_left(),
                    EditOp::Right => buf.move_right(),
                    EditOp::Home => buf.home(),
                    EditOp::End => buf.end(),
                    EditOp::Up => buf.move_up(),
                    EditOp::Down => buf.move_down(),
                    EditOp::KillLine => buf.kill_to_line_start(),
                    EditOp::KillWord => buf.delete_word_back(),
                }
            }
        }
        Action::Cancel => {
            if model.focus == Pane::Overlay {
                close_overlay(model);
            } else if model.focus == Pane::Composer {
                model.composer_target = None;
                model.focus = Pane::Stage;
            } else {
                match &mut model.stage {
                    Stage::Room(r) => {
                        r.selection = None;
                        r.focused = None;
                    }
                    Stage::Agent(a) => {
                        a.focused = None;
                    }
                    _ => {}
                }
            }
        }
        Action::Submit => submit_composer(model, effects),
        Action::SelectMessage(seq) => {
            if let Stage::Room(r) = &mut model.stage {
                r.focused = Some(Seq(seq));
                r.toggle_select_seq(Seq(seq));
                model.focus = Pane::Stage;
            }
        }
        Action::Promote => {
            if let Stage::Room(r) = &model.stage
                && let Some(range) = r.effective_promote_range()
            {
                let recipient = match &model.me {
                    Principal::Operator { seat } | Principal::Seat { seat } => seat.clone(),
                    _ => SeatName::parse("owner").expect("valid fallback seat"),
                };
                let slug = match &r.room {
                    RoomId::Channel { name, .. } => name.as_str().to_owned(),
                    RoomId::Direct { .. } => "dm-promotion".to_owned(),
                };
                effects.push(Effect::Call {
                    tag: ReplyTag::UserCommand,
                    cmd: WireCommand::MailPromote(MailPromote {
                        room: r.room.clone(),
                        range,
                        to: vec![recipient],
                        subject: format!(
                            "Promoted from {slug} (#{}..#{})",
                            range.first().0,
                            range.last().0
                        ),
                        topic: slug,
                        priority: LetterPriority::Normal,
                    }),
                });
            }
        }
        Action::RunCardAction { id, index } => {
            if let Stage::Inbox(inbox) = &model.stage
                && let Some(card) = inbox.page.items.iter().find(|n| n.id == id)
                && let Some(act) = card.draft.actions.get(index)
            {
                effects.push(Effect::Call {
                    tag: ReplyTag::UserCommand,
                    cmd: (*act.run).clone(),
                });
            }
        }
        Action::ResolveAgent {
            wave,
            role,
            interaction,
            approve,
        } => {
            effects.push(Effect::Call {
                tag: ReplyTag::UserCommand,
                cmd: build_resolve_cmd(wave, role, interaction, approve),
            });
        }
        Action::Accelerator(key_ch) => handle_accelerator(model, key_ch, effects),
        Action::Scroll { pane, lines } => match pane {
            Pane::Overlay => {
                if let Some(ov) = &mut model.overlay {
                    ov.scroll_mut().by(i32::from(lines));
                }
            }
            Pane::Sidebar => {
                if lines < 0 {
                    model.sidebar.scroll = model.sidebar.scroll.saturating_sub((-lines) as u16);
                } else {
                    model.sidebar.scroll = model.sidebar.scroll.saturating_add(lines as u16);
                }
            }
            Pane::Stage | Pane::Composer => scroll_stage(&mut model.stage, lines),
        },
    }
}

fn handle_stage_open(model: &mut Model, effects: &mut Vec<Effect>) {
    match &mut model.stage {
        Stage::Room(r) => {
            if let Some(env) = r.focused_entry() {
                if r.is_collapsed_brief(env) {
                    r.toggle_focused_brief();
                    return;
                }
                if let Some((title, body)) = r.pager_for_focused() {
                    open_overlay(model, Overlay::pager(title, body));
                    return;
                }
            }
            open_composer(model);
        }
        Stage::Agent(a) => {
            if let Some(seq) = a.focused
                && let Some((title, body)) = a.turn_pager(seq)
            {
                open_overlay(model, Overlay::pager(title, body));
                return;
            }
            open_composer(model);
        }
        Stage::Kanban(_) => open_composer(model),
        Stage::Inbox(inbox) => {
            let items = inbox.visible_items();
            let chosen = items
                .get(inbox.selected)
                .copied()
                .or_else(|| items.first().copied());
            let jump = chosen.and_then(|card| match &card.draft.kind {
                NotifyKind::Mention { room, .. } | NotifyKind::DirectMessage { room, .. } => {
                    Some((
                        if matches!(card.draft.kind, NotifyKind::DirectMessage { .. }) {
                            NodeKey::Dm(room.clone())
                        } else {
                            NodeKey::Room(room.clone())
                        },
                        StageTarget::Room { room: room.clone() },
                    ))
                }
                NotifyKind::Interaction { wave, role, .. }
                | NotifyKind::WorkerRefused { wave, role, .. }
                | NotifyKind::WorkerStuck { wave, role, .. } => Some((
                    NodeKey::Agent {
                        wave: wave.clone(),
                        role: role.clone(),
                    },
                    StageTarget::Agent {
                        wave: wave.clone(),
                        role: role.clone(),
                    },
                )),
                _ => None,
            });
            if let Some((node, target)) = jump {
                remember_project(model, &node);
                model.sidebar.cursor = node;
                retarget_if_changed(model, target, effects);
            }
        }
        _ => {}
    }
}

fn handle_accelerator(model: &mut Model, key_ch: char, effects: &mut Vec<Effect>) {
    match &model.stage {
        Stage::Inbox(inbox) => {
            let items = inbox.visible_items();
            let Some(card) = items
                .get(inbox.selected)
                .copied()
                .or_else(|| items.first().copied())
            else {
                return;
            };
            if let Some(act) = card.draft.actions.iter().find(|a| a.key == Some(key_ch)) {
                effects.push(Effect::Call {
                    tag: ReplyTag::UserCommand,
                    cmd: (*act.run).clone(),
                });
                return;
            }
            match key_ch {
                'r' => {
                    effects.push(Effect::Call {
                        tag: ReplyTag::UserCommand,
                        cmd: WireCommand::InboxAck(InboxAck {
                            ids: vec![card.id],
                            state: NotifyState::Seen,
                        }),
                    });
                }
                'x' => {
                    effects.push(Effect::Call {
                        tag: ReplyTag::UserCommand,
                        cmd: WireCommand::InboxAck(InboxAck {
                            ids: vec![card.id],
                            state: NotifyState::Dismissed,
                        }),
                    });
                }
                'X' => {
                    let group = stage::kind_label(&card.draft.kind);
                    let mut ids = Vec::new();
                    let mut room_heads: BTreeMap<RoomId, Seq> = BTreeMap::new();
                    for item in items
                        .iter()
                        .copied()
                        .filter(|n| stage::kind_label(&n.draft.kind) == group)
                    {
                        ids.push(item.id);
                        if let NotifyKind::Mention { room, seq }
                        | NotifyKind::DirectMessage { room, seq } = &item.draft.kind
                        {
                            let entry = room_heads.entry(room.clone()).or_insert(*seq);
                            if *seq > *entry {
                                *entry = *seq;
                            }
                        }
                    }
                    let mut cmds = Vec::new();
                    if !ids.is_empty() {
                        cmds.push(WireCommand::InboxAck(InboxAck {
                            ids,
                            state: NotifyState::Dismissed,
                        }));
                    }
                    for (room, upto) in room_heads {
                        cmds.push(WireCommand::InboxMarkRead(InboxMarkRead { room, upto }));
                    }
                    if cmds.len() == 1 {
                        effects.push(Effect::Call {
                            tag: ReplyTag::UserCommand,
                            cmd: cmds.remove(0),
                        });
                    } else if !cmds.is_empty() {
                        effects.push(Effect::CallBatch { cmds });
                    }
                }
                _ => {}
            }
        }
        Stage::Agent(insp) => {
            if matches!(key_ch, 'a' | 'd')
                && let Some(inter) = insp.active_pending()
            {
                effects.push(Effect::Call {
                    tag: ReplyTag::UserCommand,
                    cmd: build_resolve_cmd(
                        insp.wave.clone(),
                        insp.role.clone(),
                        inter.id.clone(),
                        key_ch == 'a',
                    ),
                });
            }
        }
        Stage::Room(r) => {
            if matches!(key_ch, 'a' | 'd')
                && let Some((wave, role, inter)) =
                    find_blocked_worker_for_room(&model.sidebar_snapshot, &r.room)
            {
                effects.push(Effect::Call {
                    tag: ReplyTag::UserCommand,
                    cmd: build_resolve_cmd(wave, role, inter.id, key_ch == 'a'),
                });
            }
        }
        _ => {}
    }
}

fn build_resolve_cmd(
    wave: WaveSlug,
    role: RoleName,
    interaction: BbInteractionId,
    approve: bool,
) -> WireCommand {
    WireCommand::WaveResolve(WaveResolve {
        wave,
        role,
        interaction,
        resolution: if approve {
            InteractionResolution::AllowOnce {
                granted_permissions: None,
            }
        } else {
            InteractionResolution::Deny
        },
    })
}

fn submit_composer(model: &mut Model, effects: &mut Vec<Effect>) {
    let Some(target) = model.composer_target.take() else {
        model.focus = Pane::Stage;
        return;
    };
    let raw = active_composer_mut(&mut model.stage)
        .map(Composer::take)
        .unwrap_or_default();
    model.focus = Pane::Stage;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return;
    }

    match target {
        ComposerTarget::Room { room } => match Body::parse(trimmed.to_owned()) {
            Ok(body) => effects.push(Effect::Call {
                tag: ReplyTag::UserCommand,
                cmd: WireCommand::RoomPost(RoomPost::new(RoomRef::Id { id: room }, body)),
            }),
            Err(e) => model.last_error = Some(e.to_string()),
        },
        ComposerTarget::Agent { wave, role } => match Body::parse(trimmed.to_owned()) {
            Ok(body) => effects.push(Effect::Call {
                tag: ReplyTag::UserCommand,
                cmd: WireCommand::WaveTell(WaveTell {
                    wave,
                    role,
                    body,
                    mode: SteerDeliveryMode::Auto,
                }),
            }),
            Err(e) => model.last_error = Some(e.to_string()),
        },
        ComposerTarget::NewTask { board, lane } => {
            let flat: String = trimmed
                .lines()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join(" ");
            match Title::parse(flat) {
                Ok(title) => effects.push(Effect::Call {
                    tag: ReplyTag::UserCommand,
                    cmd: WireCommand::CardUpsert(CardUpsert {
                        board,
                        card: None,
                        expected: None,
                        patch: CardPatch {
                            title: Some(title),
                            lane: Some(lane),
                            ..CardPatch::default()
                        },
                        idem: None,
                    }),
                }),
                Err(e) => model.last_error = Some(e.to_string()),
            }
        }
    }
}

fn handle_reply(
    model: &mut Model,
    tag: ReplyTag,
    result: Result<WireReply, ApiError>,
    effects: &mut Vec<Effect>,
) {
    let was_down = model.connection != ConnectionState::Connected;
    if result.is_ok() {
        if !model.has_connection_watch {
            model.connection = ConnectionState::Connected;
            if was_down {
                model.last_error = None;
                effects.push(Effect::EnsureTreeWatch);
            }
        }
    } else if let Err(err) = &result
        && is_transport_error(err)
    {
        mark_transport_drop(model);
    }

    match (tag, result) {
        (ReplyTag::StageInitial(seq), Err(err)) if seq == model.stage_req_seq => {
            model.stage = Stage::Error {
                target: model.active_target.clone(),
                error: err.to_string(),
            };
        }
        (ReplyTag::StageInitial(_), Err(_)) => {}
        (_, Err(err)) => {
            model.last_error = Some(err.to_string());
        }
        (ReplyTag::Sidebar, Ok(WireReply::Sidebar(snap))) => {
            let prev = model.sidebar.cursor.clone();
            model.sidebar_snapshot = snap;
            model.sidebar.rebuild(&model.sidebar_snapshot);
            if model.sidebar.cursor != prev {
                effects.push(Effect::DebounceHover(model.sidebar.cursor.clone()));
            }
        }
        (ReplyTag::StageInitial(seq), Ok(reply)) if seq == model.stage_req_seq => {
            apply_stage_snapshot(model, reply);
        }
        (ReplyTag::UserCommand, Ok(WireReply::SetArchived(_))) => {
            effects.push(Effect::Call {
                tag: ReplyTag::Sidebar,
                cmd: WireCommand::Sidebar(read::Sidebar::default()),
            });
        }
        (ReplyTag::UserCommand, Ok(WireReply::MailPromote(letter))) => {
            if let Stage::Room(r) = &mut model.stage {
                r.status_banner = Some(format!("Banked {}", letter.path.display()));
                r.selection = None;
            }
        }
        (ReplyTag::UserCommand, Ok(reply)) => {
            if matches!(
                reply,
                WireReply::InboxAck(_) | WireReply::InboxMarkRead(_) | WireReply::WaveResolve(_)
            ) {
                effects.push(Effect::Call {
                    tag: ReplyTag::Sidebar,
                    cmd: WireCommand::Sidebar(read::Sidebar::default()),
                });
            }
            let refresh_cmd = match &model.stage {
                Stage::Room(r) => {
                    let room_stream = StreamId::Room {
                        room: r.room.clone(),
                    };
                    Some(WireCommand::Read(read::Read {
                        after: r.cursor.at(&room_stream),
                        stream: room_stream,
                        limit: Some(ROOM_PAGE_LIMIT),
                    }))
                }
                _ => plan_for(&model.active_target, &model.sidebar_snapshot, &model.me).initial,
            };
            if let Some(cmd) = refresh_cmd {
                effects.push(Effect::Call {
                    tag: ReplyTag::StageInitial(model.stage_req_seq),
                    cmd,
                });
            }
        }
        _ => {}
    }
}

fn apply_stage_snapshot(model: &mut Model, reply: WireReply) {
    let vp = inbox_viewport_rows(model.term_rows);
    match reply {
        WireReply::InboxList(page) if matches!(model.active_target, StageTarget::Inbox) => {
            if let Stage::Inbox(existing) = &mut model.stage {
                existing.page = page;
                existing.keep_selected_visible(vp);
            } else {
                model.stage = Stage::Inbox(InboxView::new(page));
            }
        }
        WireReply::BoardView(snap) if matches!(model.active_target, StageTarget::Kanban { .. }) => {
            if let Stage::Kanban(existing) = &mut model.stage
                && existing.snapshot.board == snap.board
            {
                existing.snapshot = snap;
                existing.clamp_cursor();
            } else {
                model.stage = Stage::Kanban(KanbanView::new(snap));
            }
        }
        WireReply::Read(page) => {
            if let StageTarget::Room { room } = &model.active_target {
                if let Stage::Room(existing) = &mut model.stage
                    && &existing.room == room
                {
                    existing.merge_entries(page.entries, page.cursor);
                } else {
                    model.stage =
                        Stage::Room(RoomView::new(room.clone(), page.entries, page.cursor));
                }
            }
        }
        WireReply::ThreadPage(tp) => {
            if let StageTarget::Agent { wave, role } = &model.active_target {
                if let Stage::Agent(existing) = &mut model.stage
                    && &existing.wave == wave
                    && &existing.role == role
                {
                    existing.page = tp;
                } else {
                    model.stage = Stage::Agent(AgentInspector::new(wave.clone(), role.clone(), tp));
                }
                sync_agent_ctx_from_stage(model);
            }
        }
        _ => {}
    }
}

fn retarget_if_changed(model: &mut Model, next: StageTarget, effects: &mut Vec<Effect>) {
    if model.active_target == next && !matches!(model.stage, Stage::Loading(_)) {
        return;
    }
    model.active_target = next.clone();
    model.stage_req_seq = model.stage_req_seq.wrapping_add(1);
    if model.has_focused_watch {
        model.active_generation = model.active_generation.next();
        model.has_focused_watch = false;
    }
    model.composer_target = None;
    if model.focus == Pane::Composer {
        model.focus = Pane::Sidebar;
    }
    model.stage = match &next {
        StageTarget::Summary { key } => Stage::Summary(key.clone()),
        other => Stage::Loading(other.clone()),
    };
    effects.push(Effect::Retarget(next));
}

fn sync_agent_ctx_from_stage(model: &mut Model) {
    if let Stage::Agent(insp) = &model.stage
        && let Some(pct) = insp.context_pct()
    {
        model.sidebar.set_agent_ctx(
            insp.wave.clone(),
            insp.role.clone(),
            pct,
            &model.sidebar_snapshot,
        );
    }
}

fn active_composer_mut(stage: &mut Stage) -> Option<&mut Composer> {
    match stage {
        Stage::Room(r) => Some(&mut r.composer),
        Stage::Agent(a) => Some(&mut a.composer),
        Stage::Kanban(k) => Some(&mut k.composer),
        _ => None,
    }
}

fn select_in_stage(stage: &mut Stage, delta: i8, inbox_vp_rows: u16) {
    match stage {
        Stage::Inbox(inbox) => {
            let count = inbox.visible_items().len();
            if delta > 0 && inbox.selected + 1 < count {
                inbox.selected += 1;
                inbox.keep_selected_visible(inbox_vp_rows);
            } else if delta < 0 && inbox.selected > 0 {
                inbox.selected -= 1;
                inbox.keep_selected_visible(inbox_vp_rows);
            }
        }
        Stage::Kanban(k) => {
            if delta > 0 {
                k.cursor.1 = k.cursor.1.saturating_add(delta as usize);
            } else {
                k.cursor.1 = k.cursor.1.saturating_sub((-delta) as usize);
            }
            k.clamp_cursor();
        }
        Stage::Room(r) => r.select_step(delta),
        Stage::Agent(a) => a.select_step(delta),
        _ => {}
    }
}

fn page_stage(stage: &mut Stage, dir: i8, inbox_vp_rows: u16) {
    match stage {
        Stage::Inbox(inbox) => {
            let count = inbox.visible_items().len();
            if count == 0 {
                return;
            }
            let step = (inbox_vp_rows / INBOX_CARD_STRIDE).max(1) as i64;
            let next =
                (inbox.selected as i64 + i64::from(dir) * step).clamp(0, count as i64 - 1) as usize;
            inbox.selected = next;
            inbox.keep_selected_visible(inbox_vp_rows);
        }
        Stage::Room(r) => r.scroll.by_pages(i32::from(dir)),
        Stage::Agent(a) => a.scroll.by_pages(i32::from(dir)),
        _ => {}
    }
}

fn move_in_stage(
    stage: &mut Stage,
    dir: Dir,
    focus: &mut Pane,
    sidebar_hidden: &mut bool,
    inbox_vp_rows: u16,
) {
    let focus_sidebar = |focus: &mut Pane, sidebar_hidden: &mut bool| {
        *sidebar_hidden = false;
        *focus = Pane::Sidebar;
    };
    match stage {
        Stage::Inbox(inbox) => {
            let count = inbox.visible_items().len();
            match dir {
                Dir::Down if inbox.selected + 1 < count => {
                    inbox.selected += 1;
                    inbox.keep_selected_visible(inbox_vp_rows);
                }
                Dir::Up if inbox.selected > 0 => {
                    inbox.selected -= 1;
                    inbox.keep_selected_visible(inbox_vp_rows);
                }
                Dir::Left => focus_sidebar(focus, sidebar_hidden),
                _ => {}
            }
        }
        Stage::Kanban(k) => match dir {
            Dir::Left => {
                if k.cursor.0 > 0 {
                    k.cursor.0 -= 1;
                    k.clamp_cursor();
                } else {
                    focus_sidebar(focus, sidebar_hidden);
                }
            }
            Dir::Right if k.cursor.0 + 1 < k.snapshot.lanes.len() => {
                k.cursor.0 += 1;
                k.clamp_cursor();
            }
            Dir::Down => {
                k.cursor.1 = k.cursor.1.saturating_add(1);
                k.clamp_cursor();
            }
            Dir::Up => {
                k.cursor.1 = k.cursor.1.saturating_sub(1);
                k.clamp_cursor();
            }
            _ => {}
        },
        Stage::Room(r) => match dir {
            Dir::Down => r.scroll.by(1),
            Dir::Up => r.scroll.by(-1),
            Dir::Left => focus_sidebar(focus, sidebar_hidden),
            Dir::Right => {}
        },
        Stage::Agent(a) => match dir {
            Dir::Down => a.scroll.by(1),
            Dir::Up => a.scroll.by(-1),
            Dir::Left => focus_sidebar(focus, sidebar_hidden),
            Dir::Right => {}
        },
        Stage::Summary(_) | Stage::Loading(_) | Stage::Error { .. } => {
            if dir == Dir::Left {
                focus_sidebar(focus, sidebar_hidden);
            }
        }
    }
}

fn scroll_stage(stage: &mut Stage, lines: i16) {
    match stage {
        Stage::Inbox(v) => {
            if lines < 0 {
                v.scroll = v.scroll.saturating_sub((-lines) as u16);
            } else {
                v.scroll = v.scroll.saturating_add(lines as u16);
            }
        }
        Stage::Room(v) => v.scroll.by(i32::from(lines)),
        Stage::Agent(v) => v.scroll.by(i32::from(lines)),
        _ => {}
    }
}

fn scroll_stage_extreme(stage: &mut Stage, top: bool, inbox_vp_rows: u16) {
    match stage {
        Stage::Inbox(v) => {
            v.selected = if top {
                0
            } else {
                v.visible_items().len().saturating_sub(1)
            };
            if top {
                v.scroll = 0;
            } else {
                v.keep_selected_visible(inbox_vp_rows);
            }
        }
        Stage::Room(v) => {
            if top {
                v.scroll.to_top();
            } else {
                v.scroll.to_bottom();
            }
        }
        Stage::Agent(v) => {
            if top {
                v.scroll.to_top();
            } else {
                v.scroll.to_bottom();
            }
        }
        Stage::Kanban(v) => {
            v.cursor.1 = if top { 0 } else { usize::MAX };
            v.clamp_cursor();
        }
        _ => {}
    }
}

pub fn view(
    model: &Model,
    frame: &mut ratatui::Frame<'_>,
    hits: &mut HitMap,
    cards: &mut Vec<CardPlacement>,
) {
    hits.clear();
    cards.clear();

    let area = frame.area();
    let cursor_out: Cell<Option<(u16, u16)>> = Cell::new(None);
    let stage_area = if model.sidebar_hidden || area.width < 28 {
        area
    } else {
        let max_sidebar = max_sidebar_width(area.width);
        let sidebar_w = model.sidebar_width.clamp(MIN_SIDEBAR_WIDTH, max_sidebar);
        let panes = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(sidebar_w), Constraint::Min(20)])
            .split(area);
        model.sidebar.render_pane(
            panes[0],
            frame.buffer_mut(),
            hits,
            model.focus == Pane::Sidebar,
        );
        let seam_x = panes[0].right().saturating_sub(1);
        let seam_w = 2.min(area.width.saturating_sub(seam_x));
        hits.push(
            Rect::new(seam_x, area.y, seam_w, area.height),
            Target::Divider,
        );
        panes[1]
    };

    model.stage.render_with_ctx(
        stage_area,
        frame.buffer_mut(),
        hits,
        cards,
        StageRenderCtx {
            overview: &model.sidebar_snapshot,
            graphics: model.graphics,
            stage_focused: model.focus == Pane::Stage,
            composing: model.focus == Pane::Composer,
            connection: model.connection,
            last_error: model.last_error.as_deref(),
            cursor_out: &cursor_out,
        },
    );

    if let Some(overlay) = &model.overlay {
        cards.clear();
        overlay.render(area, frame.buffer_mut(), hits, model.graphics);
    } else if model.focus == Pane::Composer
        && let Some((cx, cy)) = cursor_out.get()
    {
        frame.set_cursor_position(Position::new(cx, cy));
    }
}

/// Polls an active `FrameStream` slot, clearing `*slot = None` immediately on
/// `None` or `Err` so a completed `Unfold` stream is never polled a second time.
pub async fn poll_watch_slot(slot: &mut Option<FrameStream>, watch: WatchSlot) -> Msg {
    let Some(stream) = slot.as_mut() else {
        return std::future::pending().await;
    };
    match std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        Some(Ok(frame)) => Msg::Frame { watch, frame },
        Some(Err(err)) => {
            *slot = None;
            Msg::WatchClosed {
                watch,
                error: Some(err),
            }
        }
        None => {
            *slot = None;
            Msg::WatchClosed { watch, error: None }
        }
    }
}

type StageLoadRequest = (u64, SidebarTreeSnapshot, StageTarget);

/// Serialized, preemptible background stage loader. Because the daemon owns one
/// `Interest::Stage` watch slot per session, running at most one `load_stage_target`
/// at a time and cancelling an in-flight load as soon as `retarget` publishes a newer
/// `seq` keeps the UI loop non-blocking without letting an older slow open steal the
/// server's stage watch slot after a newer target has settled.
pub struct StageLoader {
    tx: watch::Sender<Option<StageLoadRequest>>,
    latest_seq: Arc<AtomicU64>,
    handle: tokio::task::JoinHandle<()>,
}

impl StageLoader {
    pub fn new<C: Client + Clone + 'static>(client: C, msg_tx: mpsc::UnboundedSender<Msg>) -> Self {
        let (tx, mut rx) = watch::channel::<Option<StageLoadRequest>>(None);
        let latest_seq = Arc::new(AtomicU64::new(0));
        let seq_ref = latest_seq.clone();
        let handle = tokio::spawn(async move {
            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                loop {
                    let Some((seq, overview, target)) = rx.borrow_and_update().clone() else {
                        break;
                    };
                    if matches!(target, StageTarget::Summary { .. })
                        || seq_ref.load(Ordering::Acquire) != seq
                    {
                        break;
                    }
                    let outcome = tokio::select! {
                        biased;
                        changed = rx.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            continue;
                        }
                        out = load_stage_target_checked(&client, &overview, &target, &seq_ref, seq) => out,
                    };
                    if let Some((initial_res, watch_res)) = outcome
                        && seq_ref.load(Ordering::Acquire) == seq
                    {
                        let _ = msg_tx.send(Msg::StageWatchOpened {
                            seq,
                            watch: watch_res.map(|opt| opt.map(OpenedWatch)),
                        });
                        if let Some(result) = initial_res {
                            let _ = msg_tx.send(Msg::Reply {
                                tag: ReplyTag::StageInitial(seq),
                                result,
                            });
                        }
                    }
                    break;
                }
            }
        });
        Self {
            tx,
            latest_seq,
            handle,
        }
    }

    pub fn retarget(&self, seq: u64, overview: SidebarTreeSnapshot, target: StageTarget) {
        self.latest_seq.store(seq, Ordering::Release);
        let _ = self.tx.send(Some((seq, overview, target)));
    }
}

impl Drop for StageLoader {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn load_stage_target_checked<C: Client>(
    client: &C,
    overview: &SidebarTreeSnapshot,
    target: &StageTarget,
    latest_seq: &AtomicU64,
    seq: u64,
) -> Option<(
    Option<Result<WireReply, ApiError>>,
    Result<Option<FrameStream>, ApiError>,
)> {
    if latest_seq.load(Ordering::Acquire) != seq {
        return None;
    }
    let out = load_stage_target_with_guard(client, overview, target, || {
        latest_seq.load(Ordering::Acquire) == seq
    })
    .await;
    (latest_seq.load(Ordering::Acquire) == seq).then_some(out)
}

/// Loads initial stage data and opens the matching server `Interest::Stage` watch.
/// For rooms, pages `Read` forward to the current head first and starts the watch
/// from the returned `Cursor` so rooms longer than one page never skip entries.
pub async fn load_stage_target<C: Client>(
    client: &C,
    overview: &SidebarTreeSnapshot,
    target: &StageTarget,
) -> (
    Option<Result<WireReply, ApiError>>,
    Result<Option<FrameStream>, ApiError>,
) {
    load_stage_target_with_guard(client, overview, target, || true).await
}

async fn load_stage_target_with_guard<C: Client, F: Fn() -> bool>(
    client: &C,
    overview: &SidebarTreeSnapshot,
    target: &StageTarget,
    still_current: F,
) -> (
    Option<Result<WireReply, ApiError>>,
    Result<Option<FrameStream>, ApiError>,
) {
    if let StageTarget::Room { room } = target {
        let stream_id = StreamId::Room { room: room.clone() };
        let mut after = Seq::ORIGIN;
        let mut all_entries = Vec::new();
        let mut cursor = Cursor::default();

        loop {
            if !still_current() {
                return (None, Ok(None));
            }
            let res = client
                .call_wire(WireCommand::Read(read::Read {
                    stream: stream_id.clone(),
                    after,
                    limit: Some(ROOM_PAGE_LIMIT),
                }))
                .await;
            match res {
                Ok(WireReply::Read(page)) => {
                    let count = page.entries.len();
                    if let Some(last) = page.entries.last() {
                        after = last.seq;
                    }
                    cursor.advance(&stream_id, after);
                    all_entries.extend(page.entries);
                    if count < ROOM_PAGE_LIMIT as usize {
                        break;
                    }
                }
                Ok(other) => {
                    return (
                        Some(Err(ApiError::ReplyMismatch {
                            expected: "read".to_owned(),
                            got: format!("{other:?}"),
                        })),
                        Ok(None),
                    );
                }
                Err(e) => return (Some(Err(e)), Ok(None)),
            }
        }

        if !still_current() {
            return (None, Ok(None));
        }
        let watch_res = client
            .watch(
                Interest::Stage {
                    target: rex_wave_api::StageTarget::Room {
                        room: RoomRef::Id { id: room.clone() },
                    },
                },
                cursor.clone(),
            )
            .await
            .map(Some);
        let total_count = all_entries.len();
        let initial = WireReply::Read(Page {
            entries: all_entries,
            cursor,
            timed_out: false,
            total_count,
            truncated: false,
        });
        return (Some(Ok(initial)), watch_res);
    }

    let plan = plan_for(target, overview, client.me());
    if !still_current() {
        return (None, Ok(None));
    }
    let watch_res = match plan.watch {
        Some((interest, cursor)) => client.watch(interest, cursor).await.map(Some),
        None => Ok(None),
    };
    if !still_current() {
        return (None, Ok(None));
    }
    let initial_res = match plan.initial {
        Some(WireCommand::BoardView(bv)) => {
            match client.call_wire(WireCommand::BoardView(bv.clone())).await {
                Err(ApiError::NotFound { .. }) => {
                    let _ = client
                        .call_wire(WireCommand::BoardEnsure(BoardEnsure {
                            board: bv.board.clone(),
                            lanes: None,
                        }))
                        .await;
                    Some(client.call_wire(WireCommand::BoardView(bv)).await)
                }
                other => Some(other),
            }
        }
        Some(cmd) => Some(client.call_wire(cmd).await),
        None => None,
    };
    (initial_res, watch_res)
}

/// Polls a transport connection watch receiver, clearing `*rx = None` once the
/// channel closes so the event loop never busy-spins on a closed watch.
pub async fn poll_connection(rx: &mut Option<watch::Receiver<bool>>) -> Msg {
    let Some(sub) = rx.as_mut() else {
        return std::future::pending().await;
    };
    match sub.changed().await {
        Ok(()) => Msg::ConnectionChanged(*sub.borrow_and_update()),
        Err(_) => {
            *rx = None;
            Msg::ConnectionClosed
        }
    }
}

pub async fn run<C: Client + Clone + 'static>(client: C) -> io::Result<()> {
    run_inner(client, None).await
}

pub async fn run_with_connection<C: Client + Clone + 'static>(
    client: C,
    connection: watch::Receiver<bool>,
) -> io::Result<()> {
    run_inner(client, Some(connection)).await
}

async fn run_inner<C: Client + Clone + 'static>(
    client: C,
    connection: Option<watch::Receiver<bool>>,
) -> io::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(
        stdout,
        EnterAlternateScreen,
        EnableMouseCapture,
        EnableBracketedPaste
    )?;
    let _ = execute!(
        stdout,
        PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
    );
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let compositor = KittyCompositor::new(CellPx::default());
    let res = run_loop(&mut terminal, client, connection, compositor).await;

    let shutdown = KittyCompositor::new(CellPx::default()).shutdown_bytes();
    let out = terminal.backend_mut();
    let _ = out.write_all(&shutdown);
    let _ = execute!(out, PopKeyboardEnhancementFlags);
    let _ = execute!(
        out,
        DisableBracketedPaste,
        DisableMouseCapture,
        LeaveAlternateScreen
    );
    let _ = out.flush();
    let _ = disable_raw_mode();

    res
}

async fn run_loop<C: Client + Clone + 'static>(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    client: C,
    mut conn_rx: Option<watch::Receiver<bool>>,
    mut compositor: KittyCompositor,
) -> io::Result<()> {
    let initial_overview = match client
        .call_wire(WireCommand::Sidebar(read::Sidebar::default()))
        .await
    {
        Ok(WireReply::Sidebar(s)) => s,
        _ => SidebarTreeSnapshot::default(),
    };

    let mut model = Model::new(client.me().clone(), initial_overview);
    if let Some(rx) = conn_rx.as_mut() {
        model.has_connection_watch = true;
        model.connection = if *rx.borrow_and_update() {
            ConnectionState::Connected
        } else {
            ConnectionState::Reconnecting
        };
    }
    if let Ok(sz) = terminal.size() {
        model.term_cols = sz.width;
        model.term_rows = sz.height;
        model.sidebar_width = model
            .sidebar_width
            .clamp(MIN_SIDEBAR_WIDTH, max_sidebar_width(sz.width));
    }
    let mut hits = HitMap::default();
    let mut cards = Vec::new();

    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<CtEvent>();
    std::thread::spawn(move || {
        while let Ok(ev) = event::read() {
            if ev_tx.send(ev).is_err() {
                break;
            }
        }
    });

    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<Msg>();
    let stage_loader = StageLoader::new(client.clone(), msg_tx.clone());

    let mut tree_watch = match client.watch(Interest::Sidebar, Cursor::live()).await {
        Ok(s) => Some(s),
        Err(e) => {
            if !model.has_connection_watch {
                model.connection = if is_transport_error(&e) {
                    ConnectionState::Reconnecting
                } else {
                    ConnectionState::Disconnected
                };
            }
            model.last_error = Some(format!("sidebar watch: {e}"));
            None
        }
    };
    let mut opening_tree_watch = false;
    let mut stage_watch: Option<FrameStream> = None;
    let mut hover_deadline: Option<(tokio::time::Instant, NodeKey)> = None;

    stage_loader.retarget(
        model.stage_req_seq,
        model.sidebar_snapshot.clone(),
        model.active_target.clone(),
    );

    loop {
        let usage_bar = if model.overlay.is_some() {
            None
        } else {
            model.stage.active_usage_bar()
        };
        let pre = compositor.prepare_pre_draw(model.graphics, usage_bar);
        terminal.backend_mut().write_all(&pre)?;
        terminal.draw(|frame| view(&model, frame, &mut hits, &mut cards))?;
        let post = compositor.finish_post_draw(
            model.graphics,
            &cards,
            &program_status(&model.sidebar_snapshot),
        );
        terminal.backend_mut().write_all(&post)?;
        terminal.backend_mut().flush()?;

        if model.quit {
            break;
        }

        let incoming: Option<Msg> = tokio::select! {
            biased;
            m = poll_connection(&mut conn_rx) => Some(m),
            Some(ct_ev) = ev_rx.recv() => match ct_ev {
                CtEvent::Key(k) if k.kind == KeyEventKind::Press => {
                    input::key_action(k, model.focus, &model.sidebar.cursor).map(Msg::Action)
                }
                CtEvent::Paste(text) if model.focus == Pane::Composer => {
                    Some(Msg::Action(Action::Paste(text)))
                }
                CtEvent::Mouse(m) => input::mouse_action(m, &hits).map(Msg::Action),
                CtEvent::Resize(cols, rows) => {
                    compositor.invalidate_textures();
                    Some(Msg::Resize { cols, rows })
                }
                _ => None,
            },
            Some(m) = msg_rx.recv() => Some(m),
            m = poll_watch_slot(&mut tree_watch, WatchSlot::Tree) => Some(m),
            m = poll_watch_slot(&mut stage_watch, WatchSlot::Stage) => Some(m),
            key = wait_hover(&mut hover_deadline) => Some(Msg::HoverSettled(key)),
        };

        if let Some(msg) = incoming {
            let msg = match msg {
                Msg::StageWatchOpened {
                    seq,
                    watch: Ok(stream_opt),
                } => {
                    if seq == model.stage_req_seq {
                        stage_watch = stream_opt.map(|w| w.0);
                    }
                    continue;
                }
                Msg::TreeWatchOpened(res) => {
                    opening_tree_watch = false;
                    match res {
                        Ok(w) => {
                            tree_watch = Some(w.0);
                            continue;
                        }
                        Err(err) => Msg::TreeWatchOpened(Err(err)),
                    }
                }
                other => other,
            };
            let effects = update(&mut model, msg);
            for effect in effects {
                match effect {
                    Effect::Call { tag, cmd } => {
                        let c = client.clone();
                        let tx = msg_tx.clone();
                        tokio::spawn(async move {
                            let result = c.call_wire(cmd).await;
                            let _ = tx.send(Msg::Reply { tag, result });
                        });
                    }
                    Effect::CallBatch { cmds } => {
                        let c = client.clone();
                        let tx = msg_tx.clone();
                        tokio::spawn(async move {
                            let mut last = None;
                            for cmd in cmds {
                                let res = c.call_wire(cmd).await;
                                let failed = res.is_err();
                                last = Some(res);
                                if failed {
                                    break;
                                }
                            }
                            if let Some(result) = last {
                                let _ = tx.send(Msg::Reply {
                                    tag: ReplyTag::UserCommand,
                                    result,
                                });
                            }
                        });
                    }
                    Effect::EnsureTreeWatch => {
                        if tree_watch.is_none() && !opening_tree_watch {
                            opening_tree_watch = true;
                            let c = client.clone();
                            let tx = msg_tx.clone();
                            tokio::spawn(async move {
                                let res = c.watch(Interest::Sidebar, Cursor::live()).await;
                                let _ = tx.send(Msg::TreeWatchOpened(res.map(OpenedWatch)));
                            });
                        }
                    }
                    Effect::Retarget(target) => {
                        stage_watch = None;
                        stage_loader.retarget(
                            model.stage_req_seq,
                            model.sidebar_snapshot.clone(),
                            target,
                        );
                    }
                    Effect::DebounceHover(key) => {
                        hover_deadline = Some((tokio::time::Instant::now() + HOVER_DEBOUNCE, key));
                    }
                    Effect::Terminal(term_out) => {
                        let bytes = encode(&term_out, CellPx::default(), None);
                        terminal.backend_mut().write_all(&bytes)?;
                    }
                    Effect::Quit => {
                        model.quit = true;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn wait_hover(hover: &mut Option<(tokio::time::Instant, NodeKey)>) -> NodeKey {
    match *hover {
        Some((deadline, ref key)) => {
            let k = key.clone();
            tokio::time::sleep_until(deadline).await;
            *hover = None;
            k
        }
        None => std::future::pending().await,
    }
}
