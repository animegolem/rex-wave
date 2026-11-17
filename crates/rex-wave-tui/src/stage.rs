//! Context-sensitive right-pane main stage driven by the focused or hovered
//! sidebar node, stamped with server `FocusGeneration`.

use crate::input::{HitMap, Target};
use crate::kitty::{Badge, CONTEXT_BAR_IMAGE, CardPlacement, CardTone, GraphicsMode};
use crate::sidebar::NodeKey;
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Paragraph, Widget},
};
use rex_wave_api::{
    board::{
        Assign, BoardEvent, BoardId, BoardSnapshot, BoardView, Card, LaneCards, LaneName, Priority,
        project_board, seat_board,
    },
    notify::{InboxEvent, InboxList, InboxPage, Notification, NotifyKind, NotifyState},
    read::{
        AgentEvent, AgentEventKind, ContextCategoryKind, SidebarTreeSnapshot, ThreadContextUsage,
        ThreadPage, ThreadPageView,
    },
    room::{RoomEvent, RoomId, RoomRef},
    wave::{Activity, InteractionKind, PendingInteraction, WorkerPhase},
    *,
};
use std::time::Duration;

const C_ACCENT: Color = Color::Rgb(86, 194, 255);
const C_MINT: Color = Color::Rgb(74, 232, 176);
const C_AMBER: Color = Color::Rgb(255, 190, 74);
const C_ROSE: Color = Color::Rgb(255, 107, 138);
const C_LAVENDER: Color = Color::Rgb(186, 148, 255);
const C_DIM: Color = Color::Rgb(126, 138, 164);
const C_TEXT: Color = Color::Rgb(232, 238, 249);
const C_PANEL_FOCUS: Color = Color::Rgb(94, 204, 255);
const C_PANEL_IDLE: Color = Color::Rgb(54, 64, 86);

pub const HOVER_DEBOUNCE: Duration = Duration::from_millis(100);
pub const CONTEXT_BAR_COLS: u16 = 24;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageTarget {
    Inbox,
    Kanban { board: Option<BoardId> },
    Room { room: RoomId },
    Agent { wave: WaveSlug, role: RoleName },
    Summary { key: NodeKey },
}

pub fn target_for(key: &NodeKey) -> StageTarget {
    match key {
        NodeKey::Inbox => StageTarget::Inbox,
        NodeKey::Kanban => StageTarget::Kanban { board: None },
        NodeKey::Room(room) | NodeKey::Dm(room) => StageTarget::Room { room: room.clone() },
        NodeKey::Agent { wave, role } => StageTarget::Agent {
            wave: wave.clone(),
            role: role.clone(),
        },
        other => StageTarget::Summary { key: other.clone() },
    }
}

pub struct Plan {
    pub initial: Option<WireCommand>,
    pub watch: Option<(Interest, Cursor)>,
}

pub fn default_board_id(overview: &SidebarTreeSnapshot, me: &Principal) -> BoardId {
    if let Some(first) = overview.kanban.first() {
        return first.board.clone();
    }
    if let Some(proj) = overview.projects.first() {
        return project_board(&proj.project);
    }
    match me {
        Principal::Operator { seat } | Principal::Seat { seat } => seat_board(seat),
        Principal::Worker { wave, .. } => {
            BoardId::parse(format!("wave:{wave}")).expect("wave slug is printable")
        }
        Principal::System { .. } => BoardId::parse("seat:owner").expect("valid board id"),
    }
}

pub fn plan_for(target: &StageTarget, overview: &SidebarTreeSnapshot, me: &Principal) -> Plan {
    match target {
        StageTarget::Inbox => Plan {
            initial: Some(WireCommand::InboxList(InboxList {
                include_done: false,
                limit: Some(100),
            })),
            watch: Some((
                Interest::Stage {
                    target: rex_wave_api::StageTarget::Inbox,
                },
                Cursor::live(),
            )),
        },
        StageTarget::Kanban { board } => {
            let board_id = board
                .clone()
                .unwrap_or_else(|| default_board_id(overview, me));
            Plan {
                initial: Some(WireCommand::BoardView(BoardView {
                    board: board_id.clone(),
                    filter: None,
                })),
                watch: Some((
                    Interest::Stage {
                        target: rex_wave_api::StageTarget::Kanban { board: board_id },
                    },
                    Cursor::live(),
                )),
            }
        }
        StageTarget::Room { room } => Plan {
            initial: Some(WireCommand::Read(read::Read {
                stream: StreamId::Room { room: room.clone() },
                after: Seq::ORIGIN,
                limit: Some(200),
            })),
            watch: Some((
                Interest::Stage {
                    target: rex_wave_api::StageTarget::Room {
                        room: RoomRef::Id { id: room.clone() },
                    },
                },
                Cursor::live(),
            )),
        },
        StageTarget::Agent { wave, role } => Plan {
            initial: Some(WireCommand::ThreadPage(ThreadPage {
                wave: wave.clone(),
                role: role.clone(),
                after: None,
                limit: Some(200),
            })),
            watch: Some((
                Interest::Stage {
                    target: rex_wave_api::StageTarget::Agent {
                        wave: wave.clone(),
                        role: role.clone(),
                    },
                },
                Cursor::live(),
            )),
        },
        StageTarget::Summary { .. } => Plan {
            initial: None,
            watch: None,
        },
    }
}

pub enum Stage {
    Inbox(InboxView),
    Kanban(KanbanView),
    Room(RoomView),
    Agent(AgentInspector),
    Summary(NodeKey),
    Loading(StageTarget),
    Error { target: StageTarget, error: String },
}

pub const INBOX_CARD_HEIGHT: u16 = 4;
pub const INBOX_CARD_STRIDE: u16 = 5;

pub struct InboxView {
    pub page: InboxPage,
    pub selected: usize,
    pub scroll: u16,
}

impl InboxView {
    pub fn new(page: InboxPage) -> Self {
        Self {
            page,
            selected: 0,
            scroll: 0,
        }
    }

    pub fn visible_items(&self) -> Vec<&Notification> {
        self.page
            .items
            .iter()
            .filter(|n| !matches!(n.state, NotifyState::Done | NotifyState::Dismissed))
            .collect()
    }

    pub fn keep_selected_visible(&mut self, viewport_rows: u16) {
        let count = self.visible_items().len();
        if count == 0 {
            self.selected = 0;
            self.scroll = 0;
            return;
        }
        self.selected = self.selected.min(count - 1);
        let avail = viewport_rows.max(INBOX_CARD_HEIGHT);
        let card_top = (self.selected as u16).saturating_mul(INBOX_CARD_STRIDE);
        let card_bottom = card_top.saturating_add(INBOX_CARD_HEIGHT);
        if card_top < self.scroll {
            self.scroll = card_top;
        } else if card_bottom > self.scroll.saturating_add(avail) {
            self.scroll = card_bottom.saturating_sub(avail);
        }
    }
}

pub struct KanbanView {
    pub snapshot: BoardSnapshot,
    pub cursor: (usize, usize),
    pub composer: String,
}

impl KanbanView {
    pub fn new(mut snapshot: BoardSnapshot) -> Self {
        if snapshot.lanes.is_empty() {
            snapshot.lanes = ["todo", "doing", "review", "done"]
                .into_iter()
                .map(|name| LaneCards {
                    lane: LaneName::parse(name).expect("valid default lane"),
                    cards: Vec::new(),
                })
                .collect();
        }
        Self {
            snapshot,
            cursor: (0, 0),
            composer: String::new(),
        }
    }

    pub fn clamp_cursor(&mut self) {
        if self.snapshot.lanes.is_empty() {
            self.cursor = (0, 0);
            return;
        }
        self.cursor.0 = self.cursor.0.min(self.snapshot.lanes.len() - 1);
        let cards_len = self.snapshot.lanes[self.cursor.0].cards.len();
        self.cursor.1 = if cards_len == 0 {
            0
        } else {
            self.cursor.1.min(cards_len - 1)
        };
    }

    pub fn selected_card(&self) -> Option<&Card> {
        self.snapshot
            .lanes
            .get(self.cursor.0)
            .and_then(|l| l.cards.get(self.cursor.1))
    }

    pub fn next_lane_for_move(&self) -> Option<LaneName> {
        if self.snapshot.lanes.len() < 2 {
            return None;
        }
        let next_idx = (self.cursor.0 + 1) % self.snapshot.lanes.len();
        Some(self.snapshot.lanes[next_idx].lane.clone())
    }
}

pub struct RoomView {
    pub room: RoomId,
    pub entries: Vec<Envelope>,
    pub cursor: Cursor,
    pub selection: Option<(Seq, Seq)>,
    pub composer: String,
    pub scroll: u16,
    pub status_banner: Option<String>,
}

impl RoomView {
    pub fn new(room: RoomId, entries: Vec<Envelope>, cursor: Cursor) -> Self {
        Self {
            room,
            entries,
            cursor,
            selection: None,
            composer: String::new(),
            scroll: u16::MAX,
            status_banner: None,
        }
    }

    pub fn merge_entries(&mut self, incoming: Vec<Envelope>, cursor: Cursor) {
        for env in incoming {
            if !self.entries.iter().any(|e| e.seq == env.seq) {
                self.cursor.advance(&env.stream, env.seq);
                self.entries.push(env);
            }
        }
        self.entries.sort_by_key(|e| e.seq);
        let room_stream = StreamId::Room {
            room: self.room.clone(),
        };
        self.cursor.advance(&room_stream, cursor.at(&room_stream));
    }

    pub fn effective_promote_range(&self) -> Option<SeqRange> {
        if let Some((a, b)) = self.selection {
            return SeqRange::new(a.min(b), a.max(b)).ok();
        }
        let posted: Vec<Seq> = self
            .entries
            .iter()
            .filter(|e| matches!(e.event, Event::Room(RoomEvent::Posted { .. })))
            .map(|e| e.seq)
            .collect();
        let first = *posted.first()?;
        let last = *posted.last()?;
        SeqRange::new(first, last).ok()
    }

    pub fn toggle_select_seq(&mut self, seq: Seq) {
        if seq.0 == 0 {
            return;
        }
        self.selection = match self.selection {
            None => Some((seq, seq)),
            Some((a, b)) if a == b && a != seq => Some((a.min(seq), a.max(seq))),
            Some(_) => Some((seq, seq)),
        };
    }
}

pub struct AgentInspector {
    pub wave: WaveSlug,
    pub role: RoleName,
    pub page: ThreadPageView,
    pub composer: String,
    pub scroll: u16,
}

impl AgentInspector {
    pub fn context_pct(&self) -> Option<u8> {
        let u = self.page.usage.as_ref()?;
        if u.model_context_window == 0 {
            return Some(0);
        }
        Some(((u.used_tokens.saturating_mul(100)) / u.model_context_window).min(100) as u8)
    }

    pub fn active_pending(&self) -> Option<&PendingInteraction> {
        if let Some(p) = &self.page.pending {
            return Some(p);
        }
        if let Activity::Blocked { interaction } = &self.page.activity {
            return Some(interaction);
        }
        None
    }
}

pub struct StageRenderCtx<'a> {
    pub overview: &'a SidebarTreeSnapshot,
    pub graphics: GraphicsMode,
    pub stage_focused: bool,
    pub composing: bool,
    pub last_error: Option<&'a str>,
}

impl Stage {
    pub fn apply(&mut self, frame: &Frame) {
        if let Frame::Stage { inner, .. } = frame {
            self.apply(inner);
            return;
        }

        match (self, frame) {
            (Stage::Inbox(view), Frame::Committed(env)) => {
                if let Event::Inbox(ev) = &env.event {
                    view.page.head = view.page.head.max(env.seq);
                    apply_inbox_event(&mut view.page, ev, env.at);
                    let len = view.visible_items().len();
                    if len > 0 {
                        view.selected = view.selected.min(len - 1);
                    } else {
                        view.selected = 0;
                    }
                }
            }
            (Stage::Kanban(view), Frame::Committed(env)) => {
                if let Event::Board(ev) = &env.event {
                    view.snapshot.head = view.snapshot.head.max(env.seq.0);
                    apply_board_event(&mut view.snapshot, ev);
                    view.clamp_cursor();
                }
            }
            (Stage::Room(view), Frame::Committed(env)) => {
                if env.stream
                    == (StreamId::Room {
                        room: view.room.clone(),
                    })
                    && !view.entries.iter().any(|e| e.seq == env.seq)
                {
                    view.cursor.advance(&env.stream, env.seq);
                    view.entries.push(env.clone());
                    view.entries.sort_by_key(|e| e.seq);
                }
            }
            (Stage::Agent(inspector), Frame::Agent { wave, role, event }) => {
                if wave == &inspector.wave && role == &inspector.role {
                    if let AgentEventKind::Usage { usage } = &event.kind {
                        inspector.page.usage = Some(usage.clone());
                    }
                    match &event.kind {
                        AgentEventKind::TurnStarted => inspector.page.activity = Activity::Working,
                        AgentEventKind::TurnEnded => inspector.page.activity = Activity::Idle,
                        _ => {}
                    }
                    if !inspector.page.events.iter().any(|e| e.seq == event.seq) {
                        inspector.page.events.push(event.clone());
                        inspector.page.events.sort_by_key(|e| e.seq);
                    }
                }
            }
            _ => {}
        }
    }

    pub fn active_usage_bar(&self) -> Option<(u16, &ThreadContextUsage)> {
        match self {
            Stage::Agent(a) => a.page.usage.as_ref().map(|u| (CONTEXT_BAR_COLS, u)),
            _ => None,
        }
    }

    pub fn render_with_ctx(
        &self,
        area: Rect,
        buf: &mut Buffer,
        hits: &mut HitMap,
        cards: &mut Vec<CardPlacement>,
        ctx: StageRenderCtx<'_>,
    ) {
        if area.width < 10 || area.height < 4 {
            return;
        }
        hits.push(area, Target::StageBody);

        match self {
            Stage::Inbox(view) => render_inbox(view, area, buf, hits, cards, &ctx),
            Stage::Kanban(view) => render_kanban(view, area, buf, hits, &ctx),
            Stage::Room(view) => render_room(view, area, buf, hits, &ctx),
            Stage::Agent(inspector) => render_agent(inspector, area, buf, hits, &ctx),
            Stage::Summary(key) => render_summary(key, area, buf, &ctx),
            Stage::Loading(target) => render_loading(target, area, buf, &ctx),
            Stage::Error { target, error } => render_error(target, error, area, buf, &ctx),
        }

        if let Some(err) = ctx.last_error
            && area.height >= 2
            && area.width > 12
        {
            let banner = format!(
                " ✕ {} ",
                truncate_chars(err, area.width.saturating_sub(8) as usize)
            );
            let w = (banner.chars().count() as u16).min(area.width.saturating_sub(4));
            let rect = Rect::new(area.x + 2, area.bottom() - 1, w, 1);
            Paragraph::new(Line::from(Span::styled(
                banner,
                Style::default().fg(C_ROSE).add_modifier(Modifier::BOLD),
            )))
            .render(rect, buf);
        }
    }
}

fn draw_stage_frame(
    title: String,
    area: Rect,
    buf: &mut Buffer,
    hits: &mut HitMap,
    ctx: &StageRenderCtx<'_>,
) -> Rect {
    let focused = ctx.stage_focused || ctx.composing;
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(if focused { C_PANEL_FOCUS } else { C_PANEL_IDLE }))
        .title(Span::styled(
            format!(" {title} "),
            Style::default()
                .fg(if focused { C_ACCENT } else { C_TEXT })
                .add_modifier(Modifier::BOLD),
        ));
    let inner = block.inner(area);
    block.render(area, buf);

    let gfx_pill = format!(" [v] {} ", ctx.graphics.label());
    let gfx_w = gfx_pill.chars().count() as u16;
    if area.width > gfx_w + 20 {
        let gx = area.right().saturating_sub(gfx_w + 2);
        let pill_rect = Rect::new(gx, area.y, gfx_w, 1);
        Paragraph::new(Line::from(Span::styled(
            gfx_pill,
            Style::default().fg(C_LAVENDER).add_modifier(Modifier::BOLD),
        )))
        .render(pill_rect, buf);
        hits.push(pill_rect, Target::GraphicsToggle);
    }
    inner
}

fn render_inbox(
    view: &InboxView,
    area: Rect,
    buf: &mut Buffer,
    hits: &mut HitMap,
    cards: &mut Vec<CardPlacement>,
    ctx: &StageRenderCtx<'_>,
) {
    let items = view.visible_items();
    let title = format!(
        "Inbox · {} active ({} unread)",
        items.len(),
        view.page.unread
    );
    let inner = draw_stage_frame(title, area, buf, hits, ctx);
    if inner.height < 2 || inner.width < 12 {
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(2), Constraint::Length(1)])
        .split(inner);
    let viewport = chunks[0];
    let footer = chunks[1];

    Paragraph::new(Line::from(vec![
        Span::styled(
            " [j/k]",
            Style::default().fg(C_ACCENT).add_modifier(Modifier::BOLD),
        ),
        Span::styled(" select  ", Style::default().fg(C_DIM)),
        Span::styled(
            "[a]",
            Style::default().fg(C_MINT).add_modifier(Modifier::BOLD),
        ),
        Span::styled(" approve  ", Style::default().fg(C_DIM)),
        Span::styled(
            "[d]",
            Style::default().fg(C_ROSE).add_modifier(Modifier::BOLD),
        ),
        Span::styled(" deny/dismiss  ", Style::default().fg(C_DIM)),
        Span::styled(
            "[v]",
            Style::default().fg(C_LAVENDER).add_modifier(Modifier::BOLD),
        ),
        Span::styled(" graphics", Style::default().fg(C_DIM)),
    ]))
    .render(footer, buf);

    if items.is_empty() {
        Paragraph::new(Line::from(Span::styled(
            "  Inbox zero. All waves and rooms are clear.",
            Style::default().fg(C_DIM),
        )))
        .render(
            Rect::new(
                viewport.x,
                viewport.y + 1.min(viewport.height - 1),
                viewport.width,
                1,
            ),
            buf,
        );
        return;
    }

    const CARD_H: i32 = INBOX_CARD_HEIGHT as i32;
    const CARD_STRIDE: i32 = INBOX_CARD_STRIDE as i32;
    let card_x = viewport.x + 1;
    let card_w = viewport.width.saturating_sub(2);
    if card_w < 8 {
        return;
    }

    let scroll = view.scroll as i32;
    let vp_top = viewport.y as i32;
    let vp_bottom = viewport.bottom() as i32;
    let use_z_backdrop = ctx.graphics.uses_backdrop();

    for (idx, item) in items.iter().enumerate() {
        let virtual_top = vp_top + (idx as i32) * CARD_STRIDE - scroll;
        let virtual_bottom = virtual_top + CARD_H;
        let tone = card_tone(&item.draft.kind);

        if use_z_backdrop {
            if virtual_bottom <= vp_top || virtual_top >= vp_bottom {
                continue;
            }
            let clamped_y = virtual_top.max(0) as u16;
            let pre_clipped_top = (0 - virtual_top).max(0) as u16;
            let card_rect = Rect::new(card_x, clamped_y, card_w, CARD_H as u16);
            cards.push(CardPlacement {
                image: tone.image_id(),
                placement: (idx + 1) as u32,
                card: card_rect,
                viewport,
                tone,
                pre_clipped_top,
            });
        } else if virtual_bottom <= vp_top || virtual_top >= vp_bottom {
            continue;
        }

        let is_selected = idx == view.selected;
        let sel_marker = if is_selected { "▸ " } else { "  " };
        let header_color = match tone {
            CardTone::Approval => C_AMBER,
            CardTone::Mention => C_ACCENT,
            CardTone::Mail => C_LAVENDER,
            CardTone::Neutral => C_TEXT,
        };

        let vis_start_row = (vp_top - virtual_top).max(0) as usize;
        let vis_end_row = ((vp_bottom - virtual_top).min(CARD_H)).max(0) as usize;
        if vis_end_row <= vis_start_row {
            continue;
        }

        let vis_rect = Rect::new(
            card_x,
            (virtual_top + vis_start_row as i32) as u16,
            card_w,
            (vis_end_row - vis_start_row) as u16,
        );
        hits.push(vis_rect, Target::Card { id: item.id });

        let kind_badge = kind_label(&item.draft.kind);
        // Rule A: Every cell inside a `z=-1` card keeps `Color::Reset` background so SGR 48;2 never hides the RGBA backdrop.
        let line0 = Line::from(vec![
            Span::styled(
                sel_marker,
                Style::default()
                    .fg(C_ACCENT)
                    .bg(Color::Reset)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("[{kind_badge}] "),
                Style::default()
                    .fg(header_color)
                    .bg(Color::Reset)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                truncate_chars(&item.draft.title, card_w.saturating_sub(16) as usize),
                Style::default()
                    .fg(Color::White)
                    .bg(Color::Reset)
                    .add_modifier(Modifier::BOLD),
            ),
        ]);
        let line1 = Line::from(vec![
            Span::styled("  ", Style::default().bg(Color::Reset)),
            Span::styled(
                truncate_chars(&item.draft.preview, card_w.saturating_sub(4) as usize),
                Style::default().fg(C_TEXT).bg(Color::Reset),
            ),
        ]);

        let mut action_spans: Vec<Span<'static>> =
            vec![Span::styled("  ", Style::default().bg(Color::Reset))];
        let mut button_hit_offsets: Vec<(u16, u16, usize)> = Vec::new();
        let mut cur_col: u16 = 2;
        if item.draft.actions.is_empty() {
            action_spans.push(Span::styled(
                format!("rank {} · {:?}", item.rank.0, item.state),
                Style::default().fg(C_DIM).bg(Color::Reset),
            ));
        } else {
            for (a_idx, act) in item.draft.actions.iter().enumerate() {
                let btn_label = match act.key {
                    Some(k) => format!("[{k}] {}", act.label),
                    None => format!("[{}]", act.label),
                };
                let btn_w = btn_label.chars().count() as u16;
                let btn_fg = match act.key {
                    Some('a') => C_MINT,
                    Some('d') => C_ROSE,
                    _ => C_ACCENT,
                };
                if cur_col + btn_w < card_w {
                    button_hit_offsets.push((cur_col, btn_w, a_idx));
                    action_spans.push(Span::styled(
                        btn_label,
                        Style::default()
                            .fg(btn_fg)
                            .bg(Color::Reset)
                            .add_modifier(Modifier::BOLD),
                    ));
                    action_spans.push(Span::styled("   ", Style::default().bg(Color::Reset)));
                    cur_col += btn_w + 3;
                }
            }
        }
        let line2 = Line::from(action_spans);
        let line3 = if use_z_backdrop {
            Line::from(Span::styled("", Style::default().bg(Color::Reset)))
        } else {
            let rule = "─".repeat(card_w.saturating_sub(2) as usize);
            Line::from(Span::styled(
                format!(" {rule}"),
                Style::default().fg(C_PANEL_IDLE).bg(Color::Reset),
            ))
        };

        let card_lines = [line0, line1, line2, line3];
        for r in vis_start_row..vis_end_row {
            let screen_y = (virtual_top + r as i32) as u16;
            let row_rect = Rect::new(card_x, screen_y, card_w, 1);
            for cx in card_x..card_x + card_w {
                if let Some(cell) = buf.cell_mut((cx, screen_y)) {
                    cell.set_bg(Color::Reset);
                }
            }
            Paragraph::new(card_lines[r].clone()).render(row_rect, buf);
            if r == 2 {
                for &(off_x, w, a_idx) in &button_hit_offsets {
                    hits.push(
                        Rect::new(card_x + off_x, screen_y, w, 1),
                        Target::CardButton {
                            id: item.id,
                            index: a_idx,
                        },
                    );
                }
            }
        }
    }
}

fn render_kanban(
    view: &KanbanView,
    area: Rect,
    buf: &mut Buffer,
    hits: &mut HitMap,
    ctx: &StageRenderCtx<'_>,
) {
    let total_cards: usize = view.snapshot.lanes.iter().map(|l| l.cards.len()).sum();
    let title = format!("Kanban · {} ({total_cards} cards)", view.snapshot.board);
    let inner = draw_stage_frame(title, area, buf, hits, ctx);
    if inner.height < 4 || inner.width < 16 {
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(2)])
        .split(inner);
    let lanes_area = chunks[0];
    let bottom_area = chunks[1];

    let lane_count = view.snapshot.lanes.len().max(1);
    let constraints: Vec<Constraint> = (0..lane_count)
        .map(|_| Constraint::Ratio(1, lane_count as u32))
        .collect();
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(constraints)
        .split(lanes_area);

    for (l_idx, lane) in view.snapshot.lanes.iter().enumerate() {
        let col_area = cols[l_idx];
        if col_area.width < 6 || col_area.height < 3 {
            continue;
        }
        let lane_focused = l_idx == view.cursor.0;
        let border_fg = if lane_focused { C_ACCENT } else { C_PANEL_IDLE };
        let col_block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(border_fg))
            .title(Span::styled(
                format!(" {} ({}) ", lane.lane, lane.cards.len()),
                Style::default()
                    .fg(if lane_focused { Color::White } else { C_DIM })
                    .add_modifier(Modifier::BOLD),
            ));
        let col_inner = col_block.inner(col_area);
        col_block.render(col_area, buf);

        let mut y = col_inner.y;
        for (c_idx, card) in lane.cards.iter().enumerate() {
            if y + 1 >= col_inner.bottom() {
                break;
            }
            let is_sel = lane_focused && c_idx == view.cursor.1;
            let card_h = 2.min(col_inner.bottom() - y);
            let card_rect = Rect::new(col_inner.x, y, col_inner.width, card_h);
            hits.push(
                card_rect,
                Target::KanbanCard {
                    lane: l_idx,
                    card: c_idx,
                },
            );

            let bg = if is_sel {
                Color::Rgb(30, 48, 72)
            } else {
                Color::Reset
            };
            let prio_span = match card.priority {
                Priority::Blocking => Span::styled(
                    "!BLK ",
                    Style::default()
                        .fg(C_ROSE)
                        .bg(bg)
                        .add_modifier(Modifier::BOLD),
                ),
                Priority::High => Span::styled(
                    "HIGH ",
                    Style::default()
                        .fg(C_AMBER)
                        .bg(bg)
                        .add_modifier(Modifier::BOLD),
                ),
                Priority::Normal => Span::raw(""),
                Priority::Low => Span::styled("low ", Style::default().fg(C_DIM).bg(bg)),
            };
            let head = Line::from(vec![
                Span::styled(
                    if is_sel { "▸" } else { " " },
                    Style::default()
                        .fg(C_ACCENT)
                        .bg(bg)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("#{} ", card.no.0),
                    Style::default().fg(C_ACCENT).bg(bg),
                ),
                prio_span,
                Span::styled(
                    truncate_chars(
                        card.title.as_str(),
                        col_inner.width.saturating_sub(6) as usize,
                    ),
                    Style::default()
                        .fg(Color::White)
                        .bg(bg)
                        .add_modifier(if is_sel {
                            Modifier::BOLD
                        } else {
                            Modifier::empty()
                        }),
                ),
            ]);
            Paragraph::new(head)
                .style(Style::default().bg(bg))
                .render(Rect::new(col_inner.x, y, col_inner.width, 1), buf);

            if card_h >= 2 {
                let meta = match &card.assignee {
                    Some(who) => format!("  {who}"),
                    None => "  unassigned".to_owned(),
                };
                Paragraph::new(Line::from(Span::styled(
                    truncate_chars(&meta, col_inner.width as usize),
                    Style::default().fg(C_DIM).bg(bg),
                )))
                .style(Style::default().bg(bg))
                .render(Rect::new(col_inner.x, y + 1, col_inner.width, 1), buf);
            }
            y += card_h + 1;
        }
    }

    hits.push(bottom_area, Target::Composer);
    if ctx.composing {
        let lane_name = view
            .snapshot
            .lanes
            .get(view.cursor.0)
            .map(|l| l.lane.as_str())
            .unwrap_or("todo");
        Paragraph::new(Line::from(vec![
            Span::styled(
                format!(" New task ({lane_name}) > "),
                Style::default().fg(C_MINT).add_modifier(Modifier::BOLD),
            ),
            Span::styled(view.composer.clone(), Style::default().fg(Color::White)),
            Span::styled("█", Style::default().fg(C_ACCENT)),
        ]))
        .render(
            Rect::new(bottom_area.x, bottom_area.y, bottom_area.width, 1),
            buf,
        );
    } else {
        Paragraph::new(Line::from(vec![
            Span::styled(
                " [h/l or [/]]",
                Style::default().fg(C_ACCENT).add_modifier(Modifier::BOLD),
            ),
            Span::styled(" lane  ", Style::default().fg(C_DIM)),
            Span::styled(
                "[j/k]",
                Style::default().fg(C_ACCENT).add_modifier(Modifier::BOLD),
            ),
            Span::styled(" card  ", Style::default().fg(C_DIM)),
            Span::styled(
                "[m]",
                Style::default().fg(C_MINT).add_modifier(Modifier::BOLD),
            ),
            Span::styled(" move card  ", Style::default().fg(C_DIM)),
            Span::styled(
                "[n]",
                Style::default().fg(C_AMBER).add_modifier(Modifier::BOLD),
            ),
            Span::styled(" new task", Style::default().fg(C_DIM)),
        ]))
        .render(
            Rect::new(bottom_area.x, bottom_area.y, bottom_area.width, 1),
            buf,
        );
    }
}

fn render_room(
    view: &RoomView,
    area: Rect,
    buf: &mut Buffer,
    hits: &mut HitMap,
    ctx: &StageRenderCtx<'_>,
) {
    let room_label = format_room_title(&view.room);
    let inner = draw_stage_frame(room_label, area, buf, hits, ctx);
    if inner.height < 4 || inner.width < 16 {
        return;
    }

    let blocked_worker = find_blocked_worker_for_room(ctx.overview, &view.room);
    let banner_h = u16::from(blocked_worker.is_some());
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(banner_h),
            Constraint::Min(2),
            Constraint::Length(2),
        ])
        .split(inner);

    let toolbar = chunks[0];
    let banner_area = chunks[1];
    let feed_area = chunks[2];
    let composer_area = chunks[3];

    let promote_label = match view.effective_promote_range() {
        Some(r) => format!(
            " [P] Promote #{}..#{} to Canon Letter ",
            r.first().0,
            r.last().0
        ),
        None => " [P] Promote to Canon Letter ".to_owned(),
    };
    let promote_w = promote_label.chars().count() as u16;
    Paragraph::new(Line::from(vec![
        Span::styled(
            promote_label,
            Style::default().fg(C_LAVENDER).add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
        Span::styled(
            view.status_banner.as_deref().unwrap_or(""),
            Style::default().fg(C_MINT),
        ),
    ]))
    .render(toolbar, buf);
    hits.push(
        Rect::new(toolbar.x, toolbar.y, promote_w.min(toolbar.width), 1),
        Target::PromoteButton,
    );

    if let Some((wave, role, interaction)) = blocked_worker {
        let approve_txt = "[a] Approve";
        let deny_txt = "[d] Deny";
        let prefix = format!(
            " ◐ @{role} blocked ({}) · {}  ",
            interaction_kind_tag(&interaction.kind),
            truncate_chars(
                &interaction.summary,
                banner_area.width.saturating_sub(42) as usize
            )
        );
        let p_len = prefix.chars().count() as u16;
        let a_len = approve_txt.chars().count() as u16;
        let d_len = deny_txt.chars().count() as u16;
        Paragraph::new(Line::from(vec![
            Span::styled(
                prefix,
                Style::default().fg(C_AMBER).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                approve_txt,
                Style::default().fg(C_MINT).add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(
                deny_txt,
                Style::default().fg(C_ROSE).add_modifier(Modifier::BOLD),
            ),
        ]))
        .render(banner_area, buf);
        if p_len + a_len <= banner_area.width {
            hits.push(
                Rect::new(banner_area.x + p_len, banner_area.y, a_len, 1),
                Target::AgentDecision {
                    wave: wave.clone(),
                    role: role.clone(),
                    interaction: interaction.id.clone(),
                    approve: true,
                },
            );
        }
        if p_len + a_len + 2 + d_len <= banner_area.width {
            hits.push(
                Rect::new(banner_area.x + p_len + a_len + 2, banner_area.y, d_len, 1),
                Target::AgentDecision {
                    wave,
                    role,
                    interaction: interaction.id,
                    approve: false,
                },
            );
        }
    }

    let rendered_rows: Vec<(Seq, Line<'static>)> = view
        .entries
        .iter()
        .filter_map(|env| format_room_entry(env, view.selection, feed_area.width))
        .collect();

    let vis_h = feed_area.height as usize;
    let max_scroll = rendered_rows.len().saturating_sub(vis_h);
    let start = if view.scroll == u16::MAX {
        max_scroll
    } else {
        (view.scroll as usize).min(max_scroll)
    };

    for (off, (seq, line)) in rendered_rows
        .into_iter()
        .skip(start)
        .take(vis_h)
        .enumerate()
    {
        let row_rect = Rect::new(feed_area.x, feed_area.y + off as u16, feed_area.width, 1);
        if seq.0 > 0 {
            hits.push(row_rect, Target::Message { seq: seq.0 });
        }
        Paragraph::new(line).render(row_rect, buf);
    }

    render_composer_box(
        composer_area,
        buf,
        hits,
        ctx.composing,
        &view.composer,
        "Message",
        "[i/Enter] compose  [click] select range  [P] promote to letter",
    );
}

fn render_agent(
    inspector: &AgentInspector,
    area: Rect,
    buf: &mut Buffer,
    hits: &mut HitMap,
    ctx: &StageRenderCtx<'_>,
) {
    let title = format!("Agent Inspector · @{}/{}", inspector.wave, inspector.role);
    let inner = draw_stage_frame(title, area, buf, hits, ctx);
    if inner.height < 6 || inner.width < 20 {
        return;
    }

    let pending = inspector.active_pending();
    let pending_h = if pending.is_some() { 2 } else { 0 };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Length(pending_h),
            Constraint::Min(2),
            Constraint::Length(2),
        ])
        .split(inner);

    let ctx_area = chunks[0];
    let pending_area = chunks[1];
    let log_area = chunks[2];
    let composer_area = chunks[3];

    let (act_label, act_color) = activity_badge(&inspector.page.activity);
    let line0 = Line::from(vec![
        Span::styled(
            format!(" {act_label} "),
            Style::default().fg(act_color).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" thread={} ", inspector.page.thread),
            Style::default().fg(C_DIM),
        ),
    ]);
    Paragraph::new(line0).render(Rect::new(ctx_area.x, ctx_area.y, ctx_area.width, 1), buf);

    if let Some(usage) = &inspector.page.usage {
        let pct = if usage.model_context_window > 0 {
            ((usage.used_tokens.saturating_mul(100)) / usage.model_context_window).min(100)
        } else {
            0
        };
        let est = if usage.estimated { "~" } else { "" };
        let cost = match usage.cost_usd {
            Some(c) => format!(" · ${c:.2}"),
            None => String::new(),
        };
        let compact = match usage.auto_compact_at_tokens {
            Some(c) => format!(" · compact@{}", fmt_tokens(c)),
            None => String::new(),
        };

        let bar_y = ctx_area.y + 1;
        let bar_x = ctx_area.x + 1;
        let bar_cols = CONTEXT_BAR_COLS.min(ctx_area.width.saturating_sub(4));
        if ctx.graphics.uses_placeholder() && bar_cols > 0 {
            let badge = Badge {
                image: CONTEXT_BAR_IMAGE,
                cols: bar_cols,
            };
            badge.write_row(buf, bar_x, bar_y, bar_cols);
        } else if bar_cols > 0 {
            let filled = ((pct as u16 * bar_cols) / 100).min(bar_cols);
            let bar_str = format!(
                "{}{}",
                "█".repeat(filled as usize),
                "░".repeat((bar_cols - filled) as usize)
            );
            Paragraph::new(Line::from(Span::styled(
                bar_str,
                Style::default().fg(C_MINT),
            )))
            .render(Rect::new(bar_x, bar_y, bar_cols, 1), buf);
        }

        let summary_x = bar_x + bar_cols + 2;
        if summary_x < ctx_area.right() {
            let stats = format!(
                "{est}{} / {} ({pct}%){cost}{compact}",
                fmt_tokens(usage.used_tokens),
                fmt_tokens(usage.model_context_window)
            );
            Paragraph::new(Line::from(Span::styled(
                truncate_chars(&stats, (ctx_area.right() - summary_x) as usize),
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            )))
            .render(
                Rect::new(summary_x, bar_y, ctx_area.right() - summary_x, 1),
                buf,
            );
        }

        let mut cat_spans: Vec<Span<'static>> = vec![Span::raw(" ")];
        for cat in &usage.categories {
            let col = match cat.kind {
                ContextCategoryKind::Used => C_MINT,
                ContextCategoryKind::Reserved => C_AMBER,
                ContextCategoryKind::Deferred => C_LAVENDER,
                ContextCategoryKind::Free => C_DIM,
            };
            cat_spans.push(Span::styled(
                format!("{}: {}  ", cat.label, fmt_tokens(cat.tokens)),
                Style::default().fg(col),
            ));
        }
        Paragraph::new(Line::from(cat_spans)).render(
            Rect::new(ctx_area.x, ctx_area.y + 2, ctx_area.width, 1),
            buf,
        );
    } else {
        Paragraph::new(Line::from(Span::styled(
            " Context usage not yet reported by BB",
            Style::default().fg(C_DIM),
        )))
        .render(
            Rect::new(ctx_area.x, ctx_area.y + 1, ctx_area.width, 1),
            buf,
        );
    }

    let sep = "─".repeat(ctx_area.width as usize);
    Paragraph::new(Line::from(Span::styled(
        sep,
        Style::default().fg(C_PANEL_IDLE),
    )))
    .render(
        Rect::new(ctx_area.x, ctx_area.y + 3, ctx_area.width, 1),
        buf,
    );

    if let Some(inter) = pending
        && pending_area.height >= 2
    {
        Paragraph::new(Line::from(vec![
            Span::styled(
                format!(
                    " ◐ Pending {} ({}): ",
                    interaction_kind_tag(&inter.kind),
                    inter.id
                ),
                Style::default().fg(C_AMBER).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                truncate_chars(
                    &inter.summary,
                    pending_area.width.saturating_sub(32) as usize,
                ),
                Style::default().fg(Color::White),
            ),
        ]))
        .render(
            Rect::new(pending_area.x, pending_area.y, pending_area.width, 1),
            buf,
        );

        let btn_y = pending_area.y + 1;
        let approve_lbl = "[a] Approve (AllowOnce)";
        let deny_lbl = "[d] Deny";
        let a_w = approve_lbl.chars().count() as u16;
        let d_w = deny_lbl.chars().count() as u16;
        Paragraph::new(Line::from(vec![
            Span::raw("   "),
            Span::styled(
                approve_lbl,
                Style::default().fg(C_MINT).add_modifier(Modifier::BOLD),
            ),
            Span::raw("   "),
            Span::styled(
                deny_lbl,
                Style::default().fg(C_ROSE).add_modifier(Modifier::BOLD),
            ),
        ]))
        .render(Rect::new(pending_area.x, btn_y, pending_area.width, 1), buf);

        if 3 + a_w <= pending_area.width {
            hits.push(
                Rect::new(pending_area.x + 3, btn_y, a_w, 1),
                Target::AgentDecision {
                    wave: inspector.wave.clone(),
                    role: inspector.role.clone(),
                    interaction: inter.id.clone(),
                    approve: true,
                },
            );
        }
        if 3 + a_w + 3 + d_w <= pending_area.width {
            hits.push(
                Rect::new(pending_area.x + 3 + a_w + 3, btn_y, d_w, 1),
                Target::AgentDecision {
                    wave: inspector.wave.clone(),
                    role: inspector.role.clone(),
                    interaction: inter.id.clone(),
                    approve: false,
                },
            );
        }
    }

    let event_lines: Vec<Line<'static>> = inspector
        .page
        .events
        .iter()
        .map(|ev| format_agent_event(ev, log_area.width))
        .collect();
    let vis_h = log_area.height as usize;
    let max_scroll = event_lines.len().saturating_sub(vis_h);
    let start = if inspector.scroll == u16::MAX {
        max_scroll
    } else {
        (inspector.scroll as usize).min(max_scroll)
    };
    for (off, line) in event_lines.into_iter().skip(start).take(vis_h).enumerate() {
        Paragraph::new(line).render(
            Rect::new(log_area.x, log_area.y + off as u16, log_area.width, 1),
            buf,
        );
    }

    render_composer_box(
        composer_area,
        buf,
        hits,
        ctx.composing,
        &inspector.composer,
        "Steer",
        "[i/Enter] steer agent (wave_tell)  [a/d] resolve pending interaction",
    );
}

fn render_summary(key: &NodeKey, area: Rect, buf: &mut Buffer, ctx: &StageRenderCtx<'_>) {
    let mut dummy_hits = HitMap::default();
    let (title, body) = match key {
        NodeKey::ProjectsHeader => (
            "Projects".to_owned(),
            format!(
                "  {} BB project(s) with active rooms and waves. Select a room or agent on the left.",
                ctx.overview.projects.len()
            ),
        ),
        NodeKey::Project(p) => {
            let counts = ctx
                .overview
                .projects
                .iter()
                .find(|b| &b.project == p)
                .map(|b| (b.channels.len(), b.waves.len()))
                .unwrap_or((0, 0));
            (
                format!("Project · {p}"),
                format!(
                    "  {} channel(s), {} wave(s). Expand with l or Space.",
                    counts.0, counts.1
                ),
            )
        }
        NodeKey::ChannelsHeader => (
            "Global Channels".to_owned(),
            format!("  {} global #channel(s).", ctx.overview.channels.len()),
        ),
        NodeKey::DmsHeader => (
            "Direct Messages".to_owned(),
            format!(
                "  {} active DM conversation(s).",
                ctx.overview.direct_messages.len()
            ),
        ),
        other => (
            format!("{other:?}"),
            "  Select a leaf node to inspect.".to_owned(),
        ),
    };
    let inner = draw_stage_frame(title, area, buf, &mut dummy_hits, ctx);
    if inner.height >= 2 {
        Paragraph::new(Line::from(Span::styled(body, Style::default().fg(C_TEXT))))
            .render(Rect::new(inner.x, inner.y + 1, inner.width, 1), buf);
    }
}

fn render_loading(target: &StageTarget, area: Rect, buf: &mut Buffer, ctx: &StageRenderCtx<'_>) {
    let mut dummy_hits = HitMap::default();
    let inner = draw_stage_frame("Loading".to_owned(), area, buf, &mut dummy_hits, ctx);
    if inner.height >= 1 {
        Paragraph::new(Line::from(Span::styled(
            format!("  Loading {target:?}..."),
            Style::default().fg(C_DIM),
        )))
        .render(Rect::new(inner.x, inner.y, inner.width, 1), buf);
    }
}

fn render_error(
    target: &StageTarget,
    error: &str,
    area: Rect,
    buf: &mut Buffer,
    ctx: &StageRenderCtx<'_>,
) {
    let mut dummy_hits = HitMap::default();
    let inner = draw_stage_frame("Stage Error".to_owned(), area, buf, &mut dummy_hits, ctx);
    if inner.height >= 1 {
        Paragraph::new(Line::from(Span::styled(
            format!(
                "  ✕ {target:?}: {}",
                truncate_chars(error, inner.width.saturating_sub(8) as usize)
            ),
            Style::default().fg(C_ROSE).add_modifier(Modifier::BOLD),
        )))
        .render(Rect::new(inner.x, inner.y, inner.width, 1), buf);
    }
}

fn render_composer_box(
    area: Rect,
    buf: &mut Buffer,
    hits: &mut HitMap,
    composing: bool,
    text: &str,
    label: &str,
    hint: &str,
) {
    if area.height == 0 {
        return;
    }
    hits.push(area, Target::Composer);
    let rule = "─".repeat(area.width as usize);
    Paragraph::new(Line::from(Span::styled(
        rule,
        Style::default().fg(C_PANEL_IDLE),
    )))
    .render(Rect::new(area.x, area.y, area.width, 1), buf);
    if area.height >= 2 {
        let input_rect = Rect::new(area.x, area.y + 1, area.width, 1);
        if composing {
            Paragraph::new(Line::from(vec![
                Span::styled(
                    format!(" {label} > "),
                    Style::default().fg(C_MINT).add_modifier(Modifier::BOLD),
                ),
                Span::styled(text.to_owned(), Style::default().fg(Color::White)),
                Span::styled("█", Style::default().fg(C_ACCENT)),
            ]))
            .render(input_rect, buf);
        } else {
            Paragraph::new(Line::from(Span::styled(
                format!(" {hint}"),
                Style::default().fg(C_DIM),
            )))
            .render(input_rect, buf);
        }
    }
}

fn apply_inbox_event(page: &mut InboxPage, ev: &InboxEvent, at: UnixMs) {
    match ev {
        InboxEvent::Raised { id, rank, draft } => {
            if let Some(existing) = page.items.iter_mut().find(|n| n.id == *id) {
                existing.rank = *rank;
                existing.draft = draft.clone();
            } else {
                page.items.push(Notification {
                    id: *id,
                    at,
                    rank: *rank,
                    state: NotifyState::Unread,
                    draft: draft.clone(),
                });
            }
        }
        InboxEvent::Updated { id, rank, draft } => {
            if let Some(existing) = page.items.iter_mut().find(|n| n.id == *id) {
                existing.rank = *rank;
                existing.draft = draft.clone();
            }
        }
        InboxEvent::StateSet { ids, state } => {
            for item in &mut page.items {
                if ids.contains(&item.id) {
                    item.state = *state;
                }
            }
        }
        InboxEvent::CursorAdvanced { room, upto } => {
            match page.cursors.iter_mut().find(|(r, _)| r == room) {
                Some((_, q)) if *q < *upto => *q = *upto,
                Some(_) => {}
                None => page.cursors.push((room.clone(), *upto)),
            }
        }
    }
    page.items
        .sort_by(|a, b| b.rank.cmp(&a.rank).then_with(|| b.at.cmp(&a.at)));
    page.unread = page
        .items
        .iter()
        .filter(|n| n.state == NotifyState::Unread)
        .count() as u32;
}

fn apply_board_event(snap: &mut BoardSnapshot, ev: &BoardEvent) {
    match ev {
        BoardEvent::Opened { board, lanes } => {
            snap.board = board.clone();
            snap.lanes = lanes
                .iter()
                .map(|l| LaneCards {
                    lane: l.clone(),
                    cards: Vec::new(),
                })
                .collect();
        }
        BoardEvent::CardCreated { card } => {
            for lane in &mut snap.lanes {
                lane.cards.retain(|c| c.no != card.no);
            }
            if let Some(lane) = snap.lanes.iter_mut().find(|l| l.lane == card.lane) {
                lane.cards.push(card.clone());
            } else {
                snap.lanes.push(LaneCards {
                    lane: card.lane.clone(),
                    cards: vec![card.clone()],
                });
            }
        }
        BoardEvent::CardPatched {
            no,
            revision,
            patch,
        } => {
            let mut found: Option<Card> = None;
            for lane in &mut snap.lanes {
                if let Some(pos) = lane.cards.iter().position(|c| c.no == *no) {
                    let mut c = lane.cards.remove(pos);
                    c.revision = *revision;
                    if let Some(t) = &patch.title {
                        c.title = t.clone();
                    }
                    if let Some(l) = &patch.lane {
                        c.lane = l.clone();
                    }
                    if let Some(a) = &patch.assignee {
                        c.assignee = match a {
                            Assign::To { who } => Some(who.clone()),
                            Assign::Clear => None,
                        };
                    }
                    if let Some(p) = patch.priority {
                        c.priority = p;
                    }
                    if let Some(d) = patch.due_ms {
                        c.due_ms = Some(d);
                    }
                    for add in &patch.labels_add {
                        if !c.labels.contains(add) {
                            c.labels.push(add.clone());
                        }
                    }
                    c.labels.retain(|l| !patch.labels_remove.contains(l));
                    for link in &patch.links_add {
                        if !c.links.contains(link) {
                            c.links.push(link.clone());
                        }
                    }
                    found = Some(c);
                    break;
                }
            }
            if let Some(card) = found {
                let target_lane =
                    if let Some(l) = snap.lanes.iter_mut().find(|l| l.lane == card.lane) {
                        l
                    } else {
                        snap.lanes.push(LaneCards {
                            lane: card.lane.clone(),
                            cards: Vec::new(),
                        });
                        snap.lanes.last_mut().expect("just pushed")
                    };
                let idx = patch
                    .position
                    .map(|p| (p as usize).min(target_lane.cards.len()))
                    .unwrap_or(target_lane.cards.len());
                target_lane.cards.insert(idx, card);
            }
        }
        BoardEvent::CardArchived { no, .. } => {
            for lane in &mut snap.lanes {
                lane.cards.retain(|c| c.no != *no);
            }
        }
    }
}

pub fn find_blocked_worker_for_room(
    overview: &SidebarTreeSnapshot,
    room: &RoomId,
) -> Option<(WaveSlug, RoleName, PendingInteraction)> {
    for proj in &overview.projects {
        for wave in &proj.waves {
            if &wave.room == room {
                for worker in &wave.workers {
                    if let WorkerPhase::Live {
                        activity: Activity::Blocked { interaction },
                        ..
                    } = &worker.phase
                    {
                        return Some((wave.wave.clone(), worker.role.clone(), interaction.clone()));
                    }
                }
            }
        }
    }
    None
}

fn card_tone(kind: &NotifyKind) -> CardTone {
    match kind {
        NotifyKind::Interaction { .. } | NotifyKind::WorkerStuck { .. } => CardTone::Approval,
        NotifyKind::Mention { .. } | NotifyKind::DirectMessage { .. } => CardTone::Mention,
        NotifyKind::Mail { .. } | NotifyKind::LetterBanked { .. } => CardTone::Mail,
        NotifyKind::WaveFinished { .. }
        | NotifyKind::CardAssigned { .. }
        | NotifyKind::Reminder { .. } => CardTone::Neutral,
    }
}

fn kind_label(kind: &NotifyKind) -> &'static str {
    match kind {
        NotifyKind::Interaction { .. } => "APPROVAL",
        NotifyKind::WorkerStuck { .. } => "STUCK",
        NotifyKind::Mention { .. } => "MENTION",
        NotifyKind::DirectMessage { .. } => "DM",
        NotifyKind::Mail { .. } => "MAIL",
        NotifyKind::LetterBanked { .. } => "CANON",
        NotifyKind::WaveFinished { .. } => "WAVE",
        NotifyKind::CardAssigned { .. } => "TASK",
        NotifyKind::Reminder { .. } => "BELL",
    }
}

fn interaction_kind_tag(kind: &InteractionKind) -> &'static str {
    match kind {
        InteractionKind::Permission => "permission",
        InteractionKind::Question { .. } => "question",
        InteractionKind::Extension => "extension",
    }
}

fn activity_badge(activity: &Activity) -> (&'static str, Color) {
    match activity {
        Activity::Working => ("● WORKING", C_MINT),
        Activity::Parked { .. } => ("◌ PARKED (wait)", C_ACCENT),
        Activity::Idle => ("◌ IDLE", C_DIM),
        Activity::Blocked { .. } => ("◐ BLOCKED", C_AMBER),
        Activity::Errored { .. } => ("✕ ERRORED", C_ROSE),
    }
}

fn format_room_title(room: &RoomId) -> String {
    match room {
        RoomId::Channel {
            scope: room::RoomScope::Global,
            name,
        } => format!("Room · #{name}"),
        RoomId::Channel {
            scope: room::RoomScope::Project { project },
            name,
        } => format!("Room · #{project}/{name}"),
        RoomId::Direct { pair } => {
            let (a, b) = pair.clone().into();
            format!("Direct Message · {a} ↔ {b}")
        }
    }
}

fn format_room_entry(
    env: &Envelope,
    selection: Option<(Seq, Seq)>,
    width: u16,
) -> Option<(Seq, Line<'static>)> {
    let Event::Room(ev) = &env.event else {
        return None;
    };
    let in_sel = selection.is_some_and(|(a, b)| env.seq >= a && env.seq <= b);
    let bg = if in_sel {
        Color::Rgb(30, 44, 68)
    } else {
        Color::Reset
    };

    match ev {
        RoomEvent::Posted { body, .. } => {
            let prefix = format!(" #{:<3} {}: ", env.seq.0, env.by);
            let avail = width.saturating_sub(prefix.chars().count() as u16) as usize;
            let text = truncate_chars(body.as_str().lines().next().unwrap_or(""), avail);
            let mut spans = vec![
                Span::styled(
                    format!(" #{:<3} ", env.seq.0),
                    Style::default().fg(C_DIM).bg(bg),
                ),
                Span::styled(
                    format!("{}: ", env.by),
                    Style::default()
                        .fg(C_ACCENT)
                        .bg(bg)
                        .add_modifier(Modifier::BOLD),
                ),
            ];
            for (idx, word) in text.split_whitespace().enumerate() {
                if idx > 0 {
                    spans.push(Span::styled(" ", Style::default().bg(bg)));
                }
                if word.starts_with('@') {
                    spans.push(Span::styled(
                        word.to_owned(),
                        Style::default()
                            .fg(C_AMBER)
                            .bg(bg)
                            .add_modifier(Modifier::BOLD),
                    ));
                } else {
                    spans.push(Span::styled(
                        word.to_owned(),
                        Style::default().fg(C_TEXT).bg(bg),
                    ));
                }
            }
            Some((env.seq, Line::from(spans)))
        }
        RoomEvent::TopicSet { topic } => Some((
            Seq::ORIGIN,
            Line::from(Span::styled(
                format!(
                    "      ── topic: {} ──",
                    truncate_chars(topic, width.saturating_sub(20) as usize)
                ),
                Style::default().fg(C_DIM),
            )),
        )),
        RoomEvent::Opened { .. }
        | RoomEvent::MemberJoined { .. }
        | RoomEvent::MemberLeft { .. } => None,
    }
}

fn format_agent_event(ev: &AgentEvent, width: u16) -> Line<'static> {
    let max_body = width.saturating_sub(18) as usize;
    let seq_span = Span::styled(format!(" {:>4} ", ev.seq.0), Style::default().fg(C_DIM));
    match &ev.kind {
        AgentEventKind::UserMessage { text } => Line::from(vec![
            seq_span,
            Span::styled(
                "USER  ",
                Style::default().fg(C_ACCENT).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                truncate_chars(text, max_body),
                Style::default().fg(Color::White),
            ),
        ]),
        AgentEventKind::AssistantText { text } => Line::from(vec![
            seq_span,
            Span::styled(
                "TEXT  ",
                Style::default().fg(C_MINT).add_modifier(Modifier::BOLD),
            ),
            Span::styled(truncate_chars(text, max_body), Style::default().fg(C_TEXT)),
        ]),
        AgentEventKind::ToolCall { name, summary } => Line::from(vec![
            seq_span,
            Span::styled(
                "CALL  ",
                Style::default().fg(C_LAVENDER).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{name} "),
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                truncate_chars(summary, max_body.saturating_sub(name.len() + 1)),
                Style::default().fg(C_DIM),
            ),
        ]),
        AgentEventKind::ToolResult { name, ok, summary } => Line::from(vec![
            seq_span,
            Span::styled(
                if *ok { "✓ OK  " } else { "✕ ERR " },
                Style::default()
                    .fg(if *ok { C_MINT } else { C_ROSE })
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(format!("{name} "), Style::default().fg(C_TEXT)),
            Span::styled(
                truncate_chars(summary, max_body.saturating_sub(name.len() + 1)),
                Style::default().fg(C_DIM),
            ),
        ]),
        AgentEventKind::TurnStarted => Line::from(vec![
            seq_span,
            Span::styled("── turn started ──", Style::default().fg(C_DIM)),
        ]),
        AgentEventKind::TurnEnded => Line::from(vec![
            seq_span,
            Span::styled("── turn ended ──", Style::default().fg(C_DIM)),
        ]),
        AgentEventKind::Usage { usage } => Line::from(vec![
            seq_span,
            Span::styled("CTX   ", Style::default().fg(C_AMBER)),
            Span::styled(
                format!(
                    "{} / {} tokens",
                    fmt_tokens(usage.used_tokens),
                    fmt_tokens(usage.model_context_window)
                ),
                Style::default().fg(C_DIM),
            ),
        ]),
        AgentEventKind::Other { bb_type } => Line::from(vec![
            seq_span,
            Span::styled(format!("EVENT {bb_type}"), Style::default().fg(C_DIM)),
        ]),
    }
}

fn fmt_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn truncate_chars(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let clean: String = s
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect();
    if clean.chars().count() <= max_chars {
        return clean;
    }
    let mut out: String = clean.chars().take(max_chars.saturating_sub(1)).collect();
    out.push('…');
    out
}
