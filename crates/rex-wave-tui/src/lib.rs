//! Two-pane Rex TUI client: left foldable org-tree sidebar + right
//! context-sensitive main stage driven over `rex_wave_api::Client`.

pub mod demo;
pub mod input;
pub mod kitty;
pub mod sidebar;
pub mod stage;

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event as CtEvent, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use input::{Action, Dir, HitMap, Pane};
use kitty::{
    CardPlacement, CellPx, GraphicsMode, KittyCompositor, Osc7501, TermOut, encode, program_status,
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
};
use rex_wave_api::{
    board::{BoardId, CardPatch, CardUpsert, LaneName, Title},
    mail::{LetterPriority, MailPromote},
    read::{Page, SidebarTreeSnapshot},
    room::{Body, RoomId, RoomPost, RoomRef},
    wave::{InteractionResolution, SteerDeliveryMode, WaveResolve, WaveTell},
    *,
};
use sidebar::{NodeKey, Sidebar};
use stage::{
    AgentInspector, HOVER_DEBOUNCE, INBOX_CARD_HEIGHT, InboxView, KanbanView, RoomView, Stage,
    StageRenderCtx, StageTarget, find_blocked_worker_for_room, plan_for, target_for,
};
use std::io::{self, Write as _};
use tokio::sync::mpsc;

const ROOM_PAGE_LIMIT: u32 = 200;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ComposerTarget {
    Room { room: RoomId },
    Agent { wave: WaveSlug, role: RoleName },
    NewTask { board: BoardId, lane: LaneName },
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
    pub composer_target: Option<ComposerTarget>,
    pub graphics: GraphicsMode,
    pub term_rows: u16,
    pub last_error: Option<String>,
    pub quit: bool,
}

impl Model {
    pub fn new(me: Principal, sidebar_snapshot: SidebarTreeSnapshot) -> Self {
        let sidebar = Sidebar::new(&sidebar_snapshot);
        let active_target = target_for(&sidebar.cursor);
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
            composer_target: None,
            graphics: GraphicsMode::Hybrid,
            term_rows: 34,
            last_error: None,
            quit: false,
        }
    }
}

#[derive(Debug)]
pub enum Msg {
    Action(Action),
    Frame {
        watch: WatchSlot,
        frame: Frame,
    },
    WatchClosed {
        watch: WatchSlot,
        error: Option<ApiError>,
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
    Retarget(StageTarget),
    DebounceHover(NodeKey),
    Terminal(TermOut),
    Quit,
}

fn inbox_viewport_rows(term_rows: u16) -> u16 {
    term_rows.saturating_sub(3).max(INBOX_CARD_HEIGHT)
}

pub fn update(model: &mut Model, msg: Msg) -> Vec<Effect> {
    let mut effects = Vec::new();
    match msg {
        Msg::Action(action) => {
            if !matches!(action, Action::Hover(_) | Action::Scroll { .. }) {
                model.last_error = None;
            }
            handle_action(model, action, &mut effects);
        }
        Msg::HoverSettled(key) => {
            if model.focus != Pane::Composer
                && (model.sidebar.hover.as_ref() == Some(&key) || model.sidebar.cursor == key)
            {
                retarget_if_changed(model, target_for(&key), &mut effects);
            }
        }
        Msg::Frame {
            watch: WatchSlot::Tree,
            frame: Frame::Sidebar(snap),
        } => {
            model.sidebar_snapshot = *snap;
            model.sidebar.rebuild(&model.sidebar_snapshot);
        }
        Msg::Frame {
            watch: WatchSlot::Stage,
            frame,
        } => match frame {
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
        },
        Msg::Frame { .. } => {}
        Msg::WatchClosed { watch, error } => match watch {
            WatchSlot::Tree => {
                let detail = match error {
                    Some(e) => format!("daemon disconnected: {e}"),
                    None => "daemon disconnected".to_owned(),
                };
                model.last_error = Some(detail);
            }
            WatchSlot::Stage => {
                if let Some(err) = error {
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
        },
        Msg::Reply { tag, result } => handle_reply(model, tag, result, &mut effects),
        Msg::Resize { rows, .. } => {
            model.term_rows = rows;
            if let Stage::Inbox(inbox) = &mut model.stage {
                inbox.keep_selected_visible(inbox_viewport_rows(model.term_rows));
            }
        }
    }
    effects
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
        Action::FocusPane(pane) => {
            if pane != Pane::Composer {
                model.composer_target = None;
            }
            model.focus = pane;
        }
        Action::ToggleFold(key) => {
            model.sidebar.folds.toggle(&key);
            model.sidebar.rebuild(&model.sidebar_snapshot);
        }
        Action::Activate(key) => {
            model.sidebar.cursor = key.clone();
            model.sidebar.hover = None;
            let target = target_for(&key);
            if model.active_target == target && !matches!(model.stage, Stage::Loading(_)) {
                model.focus = Pane::Stage;
            } else {
                retarget_if_changed(model, target, effects);
            }
        }
        Action::Hover(key) => {
            if model.sidebar.hover.as_ref() != Some(&key) {
                model.sidebar.hover = Some(key.clone());
                effects.push(Effect::DebounceHover(key));
            }
        }
        Action::Move(dir) => {
            if model.focus == Pane::Sidebar {
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
            } else {
                let vp = inbox_viewport_rows(model.term_rows);
                move_in_stage(&mut model.stage, dir, &mut model.focus, vp);
            }
        }
        Action::Top => {
            if model.focus == Pane::Sidebar {
                let prev = model.sidebar.cursor.clone();
                model.sidebar.move_top();
                if model.sidebar.cursor != prev {
                    effects.push(Effect::DebounceHover(model.sidebar.cursor.clone()));
                }
            } else {
                scroll_stage_extreme(&mut model.stage, true, inbox_viewport_rows(model.term_rows));
            }
        }
        Action::Bottom => {
            if model.focus == Pane::Sidebar {
                let prev = model.sidebar.cursor.clone();
                model.sidebar.move_bottom();
                if model.sidebar.cursor != prev {
                    effects.push(Effect::DebounceHover(model.sidebar.cursor.clone()));
                }
            } else {
                scroll_stage_extreme(
                    &mut model.stage,
                    false,
                    inbox_viewport_rows(model.term_rows),
                );
            }
        }
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
        Action::Compose => match &model.stage {
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
        },
        Action::Input(ch) => {
            if let Some(buf) = active_composer_mut(&mut model.stage) {
                buf.push(ch);
            }
        }
        Action::Backspace => {
            if let Some(buf) = active_composer_mut(&mut model.stage) {
                buf.pop();
            }
        }
        Action::Cancel => {
            if model.focus == Pane::Composer {
                model.composer_target = None;
                model.focus = Pane::Stage;
            } else if let Stage::Room(r) = &mut model.stage {
                r.selection = None;
            }
        }
        Action::Submit => submit_composer(model, effects),
        Action::SelectMessage(seq) => {
            if let Stage::Room(r) = &mut model.stage {
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

fn handle_accelerator(model: &mut Model, key_ch: char, effects: &mut Vec<Effect>) {
    match &model.stage {
        Stage::Inbox(inbox) => {
            let items = inbox.visible_items();
            let chosen = items
                .get(inbox.selected)
                .copied()
                .or_else(|| items.first().copied());
            if let Some(card) = chosen
                && let Some(act) = card.draft.actions.iter().find(|a| a.key == Some(key_ch))
            {
                effects.push(Effect::Call {
                    tag: ReplyTag::UserCommand,
                    cmd: (*act.run).clone(),
                });
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
        .map(std::mem::take)
        .unwrap_or_default();
    model.focus = Pane::Stage;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return;
    }

    match target {
        ComposerTarget::Room { room } => {
            if let Ok(body) = Body::parse(trimmed.to_owned()) {
                effects.push(Effect::Call {
                    tag: ReplyTag::UserCommand,
                    cmd: WireCommand::RoomPost(RoomPost {
                        room: RoomRef::Id { id: room },
                        body,
                        reply_to: None,
                        idem: None,
                    }),
                });
            }
        }
        ComposerTarget::Agent { wave, role } => {
            if let Ok(body) = Body::parse(trimmed.to_owned()) {
                effects.push(Effect::Call {
                    tag: ReplyTag::UserCommand,
                    cmd: WireCommand::WaveTell(WaveTell {
                        wave,
                        role,
                        body,
                        mode: SteerDeliveryMode::Auto,
                    }),
                });
            }
        }
        ComposerTarget::NewTask { board, lane } => {
            if let Ok(title) = Title::parse(trimmed.to_owned()) {
                effects.push(Effect::Call {
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
                });
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
            model.sidebar_snapshot = snap;
            model.sidebar.rebuild(&model.sidebar_snapshot);
        }
        (ReplyTag::StageInitial(seq), Ok(reply)) if seq == model.stage_req_seq => {
            apply_stage_snapshot(model, reply);
        }
        (ReplyTag::UserCommand, Ok(WireReply::MailPromote(letter))) => {
            if let Stage::Room(r) = &mut model.stage {
                r.status_banner = Some(format!("Banked {}", letter.path.display()));
                r.selection = None;
            }
        }
        (ReplyTag::UserCommand, Ok(_)) => {
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
                    model.stage = Stage::Agent(AgentInspector {
                        wave: wave.clone(),
                        role: role.clone(),
                        page: tp,
                        composer: String::new(),
                        scroll: u16::MAX,
                    });
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

fn active_composer_mut(stage: &mut Stage) -> Option<&mut String> {
    match stage {
        Stage::Room(r) => Some(&mut r.composer),
        Stage::Agent(a) => Some(&mut a.composer),
        Stage::Kanban(k) => Some(&mut k.composer),
        _ => None,
    }
}

fn move_in_stage(stage: &mut Stage, dir: Dir, focus: &mut Pane, inbox_vp_rows: u16) {
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
                Dir::Left => *focus = Pane::Sidebar,
                _ => {}
            }
        }
        Stage::Kanban(k) => match dir {
            Dir::Left => {
                if k.cursor.0 > 0 {
                    k.cursor.0 -= 1;
                    k.clamp_cursor();
                } else {
                    *focus = Pane::Sidebar;
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
            Dir::Down => r.scroll = r.scroll.saturating_add(1),
            Dir::Up => {
                if r.scroll == u16::MAX {
                    r.scroll = r.entries.len().saturating_sub(2) as u16;
                } else {
                    r.scroll = r.scroll.saturating_sub(1);
                }
            }
            Dir::Left => *focus = Pane::Sidebar,
            Dir::Right => {}
        },
        Stage::Agent(a) => match dir {
            Dir::Down => a.scroll = a.scroll.saturating_add(1),
            Dir::Up => {
                if a.scroll == u16::MAX {
                    a.scroll = a.page.events.len().saturating_sub(2) as u16;
                } else {
                    a.scroll = a.scroll.saturating_sub(1);
                }
            }
            Dir::Left => *focus = Pane::Sidebar,
            Dir::Right => {}
        },
        Stage::Summary(_) | Stage::Loading(_) | Stage::Error { .. } => {
            if dir == Dir::Left {
                *focus = Pane::Sidebar;
            }
        }
    }
}

fn scroll_stage(stage: &mut Stage, lines: i16) {
    let apply = |cur: &mut u16| {
        if lines < 0 {
            if *cur == u16::MAX {
                *cur = 0;
            } else {
                *cur = cur.saturating_sub((-lines) as u16);
            }
        } else {
            *cur = cur.saturating_add(lines as u16);
        }
    };
    match stage {
        Stage::Inbox(v) => apply(&mut v.scroll),
        Stage::Room(v) => apply(&mut v.scroll),
        Stage::Agent(v) => apply(&mut v.scroll),
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
        Stage::Room(v) => v.scroll = if top { 0 } else { u16::MAX },
        Stage::Agent(v) => v.scroll = if top { 0 } else { u16::MAX },
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
    let sidebar_w = 38.min(area.width * 2 / 5).max(24.min(area.width));
    let panes = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(sidebar_w), Constraint::Min(20)])
        .split(area);

    let buf = frame.buffer_mut();
    model
        .sidebar
        .render_pane(panes[0], buf, hits, model.focus == Pane::Sidebar);
    model.stage.render_with_ctx(
        panes[1],
        buf,
        hits,
        cards,
        StageRenderCtx {
            overview: &model.sidebar_snapshot,
            graphics: model.graphics,
            stage_focused: model.focus == Pane::Stage,
            composing: model.focus == Pane::Composer,
            last_error: model.last_error.as_deref(),
        },
    );
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
    if let StageTarget::Room { room } = target {
        let stream_id = StreamId::Room { room: room.clone() };
        let mut after = Seq::ORIGIN;
        let mut all_entries = Vec::new();
        let mut cursor = Cursor::default();

        loop {
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
        let initial = WireReply::Read(Page {
            entries: all_entries,
            cursor,
            timed_out: false,
        });
        return (Some(Ok(initial)), watch_res);
    }

    let plan = plan_for(target, overview, client.me());
    let watch_res = match plan.watch {
        Some((interest, cursor)) => client.watch(interest, cursor).await.map(Some),
        None => Ok(None),
    };
    let initial_res = match plan.initial {
        Some(cmd) => Some(client.call_wire(cmd).await),
        None => None,
    };
    (initial_res, watch_res)
}

pub async fn run<C: Client + Clone + 'static>(client: C) -> io::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let compositor = KittyCompositor::new(CellPx::default());
    let res = run_loop(&mut terminal, client, compositor).await;

    let shutdown = KittyCompositor::new(CellPx::default()).shutdown_bytes();
    let out = terminal.backend_mut();
    let _ = out.write_all(&shutdown);
    let _ = execute!(out, DisableMouseCapture, LeaveAlternateScreen);
    let _ = out.flush();
    let _ = disable_raw_mode();

    res
}

async fn run_loop<C: Client + Clone + 'static>(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    client: C,
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
    if let Ok(sz) = terminal.size() {
        model.term_rows = sz.height;
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

    let mut tree_watch = match client.watch(Interest::Sidebar, Cursor::live()).await {
        Ok(s) => Some(s),
        Err(e) => {
            model.last_error = Some(format!("sidebar watch: {e}"));
            None
        }
    };
    let mut stage_watch: Option<FrameStream> = None;
    let mut hover_deadline: Option<(tokio::time::Instant, NodeKey)> = None;

    let initial_target = model.active_target.clone();
    open_stage_target(&client, &msg_tx, &mut stage_watch, &model, &initial_target).await;

    loop {
        let pre = compositor.prepare_pre_draw(model.graphics, model.stage.active_usage_bar());
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
            Some(ct_ev) = ev_rx.recv() => match ct_ev {
                CtEvent::Key(k) if k.kind == KeyEventKind::Press => {
                    input::key_action(k, model.focus, &model.sidebar.cursor).map(Msg::Action)
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
                    Effect::Retarget(target) => {
                        open_stage_target(&client, &msg_tx, &mut stage_watch, &model, &target)
                            .await;
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

async fn open_stage_target<C: Client + Clone + 'static>(
    client: &C,
    msg_tx: &mpsc::UnboundedSender<Msg>,
    stage_watch: &mut Option<FrameStream>,
    model: &Model,
    target: &StageTarget,
) {
    *stage_watch = None;
    let seq = model.stage_req_seq;
    let (initial_res, watch_res) = load_stage_target(client, &model.sidebar_snapshot, target).await;
    match watch_res {
        Ok(stream_opt) => *stage_watch = stream_opt,
        Err(err) => {
            let _ = msg_tx.send(Msg::WatchClosed {
                watch: WatchSlot::Stage,
                error: Some(err),
            });
        }
    }
    if let Some(result) = initial_res {
        let _ = msg_tx.send(Msg::Reply {
            tag: ReplyTag::StageInitial(seq),
            result,
        });
    }
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
