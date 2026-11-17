//! Left pane foldable org-tree sidebar (`Inbox`, `Kanban`, divider, `Projects`,
//! `Channels`, and `Direct Messages`).

use crate::input::{HitMap, Target};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Paragraph, Widget},
};
use rex_wave_api::{
    BbProjectId, RoleName, WaveSlug,
    read::{SidebarTreeSnapshot, WaveWorkerLeaf},
    room::RoomId,
    wave::{Activity, WorkerPhase},
};
use std::collections::{BTreeMap, BTreeSet};

const C_ACCENT: Color = Color::Rgb(86, 194, 255);
const C_MINT: Color = Color::Rgb(74, 232, 176);
const C_AMBER: Color = Color::Rgb(255, 190, 74);
const C_ROSE: Color = Color::Rgb(255, 107, 138);
const C_DIM: Color = Color::Rgb(126, 138, 164);
const C_TEXT: Color = Color::Rgb(232, 238, 249);
const C_PANEL_FOCUS: Color = Color::Rgb(94, 204, 255);
const C_PANEL_IDLE: Color = Color::Rgb(54, 64, 86);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeKey {
    Inbox,
    Kanban,
    Divider,
    ProjectsHeader,
    Project(BbProjectId),
    Room(RoomId),
    Agent { wave: WaveSlug, role: RoleName },
    ChannelsHeader,
    DmsHeader,
    Dm(RoomId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Glyph {
    Working,
    Waiting,
    Blocked,
    Errored,
    Done,
}

impl Glyph {
    pub fn symbol(self) -> &'static str {
        match self {
            Self::Working => "●",
            Self::Waiting => "◌",
            Self::Blocked => "◐",
            Self::Errored => "✕",
            Self::Done => "✓",
        }
    }

    pub fn color(self) -> Color {
        match self {
            Self::Working => C_MINT,
            Self::Waiting => C_ACCENT,
            Self::Blocked => C_AMBER,
            Self::Errored => C_ROSE,
            Self::Done => C_DIM,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SidebarNode {
    pub key: NodeKey,
    pub label: String,
    pub badge: Option<u32>,
    pub glyph: Option<Glyph>,
    pub tag: Option<String>,
    pub children: Vec<SidebarNode>,
}

#[derive(Clone, Debug)]
pub struct Row {
    pub key: NodeKey,
    pub depth: u16,
    pub prefix: String,
    pub fold: Option<bool>,
    pub node: SidebarNode,
}

#[derive(Clone, Debug, Default)]
pub struct FoldState {
    // Storing collapsed keys keeps newly discovered projects and waves expanded on first sight.
    collapsed: BTreeSet<NodeKey>,
}

impl FoldState {
    pub fn is_expanded(&self, key: &NodeKey) -> bool {
        !self.collapsed.contains(key)
    }

    pub fn set(&mut self, key: NodeKey, expanded: bool) {
        if expanded {
            self.collapsed.remove(&key);
        } else {
            self.collapsed.insert(key);
        }
    }

    pub fn toggle(&mut self, key: &NodeKey) {
        let next = !self.is_expanded(key);
        self.set(key.clone(), next);
    }
}

pub struct Sidebar {
    pub roots: Vec<SidebarNode>,
    pub folds: FoldState,
    pub cursor: NodeKey,
    pub hover: Option<NodeKey>,
    pub scroll: u16,
    pub ctx_pct: BTreeMap<(WaveSlug, RoleName), u8>,
}

impl Sidebar {
    pub fn new(overview: &SidebarTreeSnapshot) -> Self {
        let ctx_pct = BTreeMap::new();
        let roots = build_with_ctx(overview, &ctx_pct);
        Self {
            roots,
            folds: FoldState::default(),
            cursor: NodeKey::Inbox,
            hover: None,
            scroll: 0,
            ctx_pct,
        }
    }

    pub fn set_agent_ctx(
        &mut self,
        wave: WaveSlug,
        role: RoleName,
        pct: u8,
        overview: &SidebarTreeSnapshot,
    ) {
        if self.ctx_pct.insert((wave, role), pct.min(100)) != Some(pct.min(100)) {
            self.rebuild(overview);
        }
    }

    pub fn rebuild(&mut self, overview: &SidebarTreeSnapshot) {
        let prev_ancestors = ancestor_chain(&self.roots, &self.cursor).unwrap_or_default();
        self.roots = build_with_ctx(overview, &self.ctx_pct);

        if find_node(&self.roots, &self.cursor).is_none() {
            self.cursor = prev_ancestors
                .into_iter()
                .rev()
                .find(|k| find_node(&self.roots, k).is_some())
                .unwrap_or(NodeKey::Inbox);
        }

        let visible = flatten(&self.roots, &self.folds);
        if !visible.iter().any(|r| r.key == self.cursor)
            && let Some(chain) = ancestor_chain(&self.roots, &self.cursor)
            && let Some(vis_anc) = chain
                .into_iter()
                .rev()
                .find(|k| visible.iter().any(|r| &r.key == k))
        {
            self.cursor = vis_anc;
        }
    }

    pub fn move_cursor(&mut self, delta: i32) {
        let focusable: Vec<NodeKey> = flatten(&self.roots, &self.folds)
            .into_iter()
            .filter(|r| r.key != NodeKey::Divider)
            .map(|r| r.key)
            .collect();
        if focusable.is_empty() {
            return;
        }
        let cur_idx = focusable
            .iter()
            .position(|k| k == &self.cursor)
            .unwrap_or(0) as i32;
        let next_idx = (cur_idx + delta).clamp(0, focusable.len() as i32 - 1) as usize;
        self.cursor = focusable[next_idx].clone();
    }

    pub fn move_top(&mut self) {
        if let Some(first) = flatten(&self.roots, &self.folds)
            .into_iter()
            .find(|r| r.key != NodeKey::Divider)
        {
            self.cursor = first.key;
            self.scroll = 0;
        }
    }

    pub fn move_bottom(&mut self) {
        if let Some(last) = flatten(&self.roots, &self.folds)
            .into_iter()
            .rfind(|r| r.key != NodeKey::Divider)
        {
            self.cursor = last.key;
        }
    }

    pub fn fold_or_parent(&mut self) {
        if let Some(node) = find_node(&self.roots, &self.cursor)
            && is_foldable(node)
            && self.folds.is_expanded(&self.cursor)
        {
            self.folds.set(self.cursor.clone(), false);
            return;
        }
        if let Some(parent) = parent_of(&self.roots, &self.cursor) {
            self.cursor = parent;
        }
    }

    pub fn unfold_or_child(&mut self) {
        let Some(node) = find_node(&self.roots, &self.cursor).cloned() else {
            return;
        };
        if is_foldable(&node) && !self.folds.is_expanded(&self.cursor) {
            self.folds.set(self.cursor.clone(), true);
            return;
        }
        if let Some(first_child) = node.children.first() {
            self.cursor = first_child.key.clone();
        }
    }

    pub fn render(&self, area: Rect, buf: &mut Buffer, hits: &mut HitMap) {
        self.render_pane(area, buf, hits, true);
    }

    pub fn render_pane(&self, area: Rect, buf: &mut Buffer, hits: &mut HitMap, focused: bool) {
        if area.width < 6 || area.height < 3 {
            return;
        }
        hits.push(area, Target::SidebarBody);

        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(if focused { C_PANEL_FOCUS } else { C_PANEL_IDLE }))
            .title(Span::styled(
                " Org Tree ",
                Style::default()
                    .fg(if focused { C_ACCENT } else { C_TEXT })
                    .add_modifier(Modifier::BOLD),
            ));
        let inner = block.inner(area);
        block.render(area, buf);

        let rows = flatten(&self.roots, &self.folds);
        let vis_h = inner.height as usize;
        if vis_h == 0 || rows.is_empty() {
            return;
        }

        let cursor_row_idx = rows.iter().position(|r| r.key == self.cursor).unwrap_or(0);
        let mut start = self.scroll as usize;
        if cursor_row_idx < start {
            start = cursor_row_idx;
        } else if cursor_row_idx >= start + vis_h {
            start = cursor_row_idx + 1 - vis_h;
        }
        start = start.min(rows.len().saturating_sub(vis_h));

        for (offset, row) in rows.iter().skip(start).take(vis_h).enumerate() {
            let y = inner.y + offset as u16;
            let row_rect = Rect::new(inner.x, y, inner.width, 1);

            if row.key == NodeKey::Divider {
                let rule = "─".repeat(inner.width as usize);
                Paragraph::new(Line::from(Span::styled(
                    rule,
                    Style::default().fg(C_PANEL_IDLE),
                )))
                .render(row_rect, buf);
                continue;
            }

            let is_cursor = row.key == self.cursor;
            let is_hover = self.hover.as_ref() == Some(&row.key);
            let bg = if is_cursor && focused {
                Color::Rgb(34, 52, 78)
            } else if is_cursor {
                Color::Rgb(24, 38, 58)
            } else if is_hover {
                Color::Rgb(20, 30, 46)
            } else {
                Color::Reset
            };

            hits.push(row_rect, Target::Node(row.key.clone()));

            let mut spans: Vec<Span<'static>> = Vec::with_capacity(8);
            let mut fold_col_offset: Option<u16> = None;
            let mut col_used: u16 = 0;

            let lead = if row.depth == 0 && row.fold.is_none() {
                if is_cursor { "▸ " } else { "  " }
            } else {
                ""
            };
            if !lead.is_empty() {
                col_used += lead.chars().count() as u16;
                spans.push(Span::styled(lead, Style::default().fg(C_ACCENT).bg(bg)));
            }

            if !row.prefix.is_empty() {
                col_used += row.prefix.chars().count() as u16;
                spans.push(Span::styled(
                    row.prefix.clone(),
                    Style::default().fg(C_DIM).bg(bg),
                ));
            }

            if let Some(expanded) = row.fold {
                fold_col_offset = Some(col_used);
                let arrow = if expanded { "▾ " } else { "▸ " };
                col_used += 2;
                spans.push(Span::styled(arrow, Style::default().fg(C_ACCENT).bg(bg)));
            }

            if let Some(g) = row.node.glyph {
                let sym = format!("{} ", g.symbol());
                col_used += 2;
                spans.push(Span::styled(
                    sym,
                    Style::default()
                        .fg(g.color())
                        .bg(bg)
                        .add_modifier(Modifier::BOLD),
                ));
            }

            let is_section = matches!(
                row.key,
                NodeKey::ProjectsHeader | NodeKey::ChannelsHeader | NodeKey::DmsHeader
            );
            let label_fg = if is_cursor {
                Color::White
            } else if is_section {
                C_DIM
            } else {
                C_TEXT
            };
            let label_mod =
                if is_cursor || is_section || matches!(row.key, NodeKey::Inbox | NodeKey::Kanban) {
                    Modifier::BOLD
                } else {
                    Modifier::empty()
                };
            col_used += row.node.label.chars().count() as u16;
            spans.push(Span::styled(
                row.node.label.clone(),
                Style::default().fg(label_fg).bg(bg).add_modifier(label_mod),
            ));

            if let Some(tag) = &row.node.tag {
                let tag_str = format!(" {tag}");
                col_used += tag_str.chars().count() as u16;
                let tag_color = if tag.contains('◐') { C_AMBER } else { C_DIM };
                spans.push(Span::styled(tag_str, Style::default().fg(tag_color).bg(bg)));
            }

            if let Some(badge) = row.node.badge
                && badge > 0
            {
                let badge_str = format!(" [{badge}]");
                let badge_w = badge_str.chars().count() as u16;
                let pad = inner.width.saturating_sub(col_used + badge_w);
                if pad > 0 {
                    spans.push(Span::styled(
                        " ".repeat(pad as usize),
                        Style::default().bg(bg),
                    ));
                }
                let badge_fg =
                    if row.key == NodeKey::Inbox && row.node.glyph == Some(Glyph::Blocked) {
                        C_AMBER
                    } else {
                        C_ACCENT
                    };
                spans.push(Span::styled(
                    badge_str,
                    Style::default()
                        .fg(badge_fg)
                        .bg(bg)
                        .add_modifier(Modifier::BOLD),
                ));
            } else if col_used < inner.width {
                spans.push(Span::styled(
                    " ".repeat((inner.width - col_used) as usize),
                    Style::default().bg(bg),
                ));
            }

            Paragraph::new(Line::from(spans)).render(row_rect, buf);

            if let Some(off) = fold_col_offset
                && off < inner.width
            {
                let w = 2.min(inner.width - off);
                hits.push(
                    Rect::new(inner.x + off, y, w, 1),
                    Target::Fold(row.key.clone()),
                );
            }
        }
    }
}

pub fn build(overview: &SidebarTreeSnapshot) -> Vec<SidebarNode> {
    build_with_ctx(overview, &BTreeMap::new())
}

pub fn build_with_ctx(
    overview: &SidebarTreeSnapshot,
    ctx_pct: &BTreeMap<(WaveSlug, RoleName), u8>,
) -> Vec<SidebarNode> {
    let inbox_glyph = if overview.inbox.blocking > 0 {
        Some(Glyph::Blocked)
    } else {
        None
    };
    let inbox_tag = if overview.inbox.blocking > 0 {
        Some(format!("◐{}", overview.inbox.blocking))
    } else {
        None
    };
    let inbox = SidebarNode {
        key: NodeKey::Inbox,
        label: "Inbox".to_owned(),
        badge: (overview.inbox.unread > 0).then_some(overview.inbox.unread),
        glyph: inbox_glyph,
        tag: inbox_tag,
        children: Vec::new(),
    };

    let open_tasks: u32 = overview.kanban.iter().map(|b| b.open).sum();
    let mine_tasks: u32 = overview.kanban.iter().map(|b| b.mine).sum();
    let kanban = SidebarNode {
        key: NodeKey::Kanban,
        label: "Kanban".to_owned(),
        badge: (open_tasks > 0).then_some(open_tasks),
        glyph: None,
        tag: (mine_tasks > 0).then(|| format!("@{mine_tasks}")),
        children: Vec::new(),
    };

    let divider = SidebarNode {
        key: NodeKey::Divider,
        label: String::new(),
        badge: None,
        glyph: None,
        tag: None,
        children: Vec::new(),
    };

    let project_children: Vec<SidebarNode> = overview
        .projects
        .iter()
        .map(|proj| {
            let mut room_nodes: Vec<SidebarNode> = proj
                .channels
                .iter()
                .map(|ch| SidebarNode {
                    key: NodeKey::Room(ch.room.clone()),
                    label: room_short_label(&ch.room),
                    badge: (ch.unread > 0).then_some(ch.unread),
                    glyph: None,
                    tag: (ch.mentions > 0).then(|| format!("@{}", ch.mentions)),
                    children: Vec::new(),
                })
                .collect();

            for wave_branch in &proj.waves {
                let mut working_cnt = 0u32;
                let mut blocked_cnt = 0u32;
                let worker_nodes: Vec<SidebarNode> = wave_branch
                    .workers
                    .iter()
                    .map(|worker| {
                        let g = agent_glyph(worker);
                        match g {
                            Glyph::Working => working_cnt += 1,
                            Glyph::Blocked => blocked_cnt += 1,
                            _ => {}
                        }
                        let tag =
                            match ctx_pct.get(&(wave_branch.wave.clone(), worker.role.clone())) {
                                Some(pct) => format!("[{} · {}%]", worker.provider, pct),
                                None => format!("[{}]", worker.provider),
                            };
                        SidebarNode {
                            key: NodeKey::Agent {
                                wave: wave_branch.wave.clone(),
                                role: worker.role.clone(),
                            },
                            label: worker.role.to_string(),
                            badge: None,
                            glyph: Some(g),
                            tag: Some(tag),
                            children: Vec::new(),
                        }
                    })
                    .collect();

                let wave_tag = match (working_cnt > 0, blocked_cnt > 0) {
                    (true, true) => Some(format!("●{working_cnt} ◐{blocked_cnt}")),
                    (true, false) => Some(format!("●{working_cnt}")),
                    (false, true) => Some(format!("◐{blocked_cnt}")),
                    (false, false) => None,
                };
                room_nodes.push(SidebarNode {
                    key: NodeKey::Room(wave_branch.room.clone()),
                    label: format!("#wave-{}", wave_branch.wave),
                    badge: (wave_branch.unread > 0).then_some(wave_branch.unread),
                    glyph: None,
                    tag: wave_tag,
                    children: worker_nodes,
                });
            }

            SidebarNode {
                key: NodeKey::Project(proj.project.clone()),
                label: proj.project.to_string(),
                badge: None,
                glyph: None,
                tag: None,
                children: room_nodes,
            }
        })
        .collect();

    let projects_header = SidebarNode {
        key: NodeKey::ProjectsHeader,
        label: "Projects".to_owned(),
        badge: None,
        glyph: None,
        tag: None,
        children: project_children,
    };

    let channel_children: Vec<SidebarNode> = overview
        .channels
        .iter()
        .map(|ch| SidebarNode {
            key: NodeKey::Room(ch.room.clone()),
            label: room_short_label(&ch.room),
            badge: (ch.unread > 0).then_some(ch.unread),
            glyph: None,
            tag: (ch.mentions > 0).then(|| format!("@{}", ch.mentions)),
            children: Vec::new(),
        })
        .collect();

    let channels_header = SidebarNode {
        key: NodeKey::ChannelsHeader,
        label: "Channels".to_owned(),
        badge: None,
        glyph: None,
        tag: None,
        children: channel_children,
    };

    let dm_children: Vec<SidebarNode> = overview
        .direct_messages
        .iter()
        .map(|dm| SidebarNode {
            key: NodeKey::Dm(dm.room.clone()),
            label: dm.with.to_string(),
            badge: (dm.unread > 0).then_some(dm.unread),
            glyph: None,
            tag: None,
            children: Vec::new(),
        })
        .collect();

    let dms_header = SidebarNode {
        key: NodeKey::DmsHeader,
        label: "Direct Messages".to_owned(),
        badge: None,
        glyph: None,
        tag: None,
        children: dm_children,
    };

    vec![
        inbox,
        kanban,
        divider,
        projects_header,
        channels_header,
        dms_header,
    ]
}

pub fn agent_glyph(agent: &WaveWorkerLeaf) -> Glyph {
    match &agent.phase {
        WorkerPhase::Requested { .. } => Glyph::Working,
        WorkerPhase::Live { activity, .. } => match activity {
            Activity::Working => Glyph::Working,
            Activity::Parked { .. } | Activity::Idle => Glyph::Waiting,
            Activity::Blocked { .. } => Glyph::Blocked,
            Activity::Errored { .. } => Glyph::Errored,
        },
        WorkerPhase::Retiring { .. } | WorkerPhase::Retired { .. } => Glyph::Done,
        WorkerPhase::Failed { .. } => Glyph::Errored,
    }
}

pub fn flatten(roots: &[SidebarNode], folds: &FoldState) -> Vec<Row> {
    let mut out = Vec::new();
    for root in roots {
        let foldable = is_foldable(root);
        let expanded = foldable && folds.is_expanded(&root.key);
        out.push(Row {
            key: root.key.clone(),
            depth: 0,
            prefix: String::new(),
            fold: foldable.then_some(expanded),
            node: root.clone(),
        });
        if expanded {
            let mut active_pipes = Vec::new();
            flatten_children(&root.children, folds, 1, &mut active_pipes, &mut out);
        }
    }
    out
}

fn flatten_children(
    children: &[SidebarNode],
    folds: &FoldState,
    depth: u16,
    active_pipes: &mut Vec<bool>,
    out: &mut Vec<Row>,
) {
    let len = children.len();
    for (idx, child) in children.iter().enumerate() {
        let has_more = idx + 1 < len;
        let mut prefix = String::new();
        for &pipe in active_pipes.iter() {
            prefix.push_str(if pipe { "│ " } else { "  " });
        }
        prefix.push_str(if has_more { "├─" } else { "└─" });
        prefix.push(' ');

        let foldable = is_foldable(child);
        let expanded = foldable && folds.is_expanded(&child.key);
        out.push(Row {
            key: child.key.clone(),
            depth,
            prefix,
            fold: foldable.then_some(expanded),
            node: child.clone(),
        });
        if expanded {
            active_pipes.push(has_more);
            flatten_children(&child.children, folds, depth + 1, active_pipes, out);
            active_pipes.pop();
        }
    }
}

fn is_foldable(node: &SidebarNode) -> bool {
    !node.children.is_empty()
        || matches!(
            node.key,
            NodeKey::ProjectsHeader | NodeKey::ChannelsHeader | NodeKey::DmsHeader
        )
}

fn room_short_label(room: &RoomId) -> String {
    match room {
        RoomId::Channel { name, .. } => format!("#{name}"),
        RoomId::Direct { pair } => {
            let (a, b) = pair.clone().into();
            format!("{a}↔{b}")
        }
    }
}

fn find_node<'a>(nodes: &'a [SidebarNode], key: &NodeKey) -> Option<&'a SidebarNode> {
    for n in nodes {
        if &n.key == key {
            return Some(n);
        }
        if let Some(found) = find_node(&n.children, key) {
            return Some(found);
        }
    }
    None
}

fn parent_of(nodes: &[SidebarNode], key: &NodeKey) -> Option<NodeKey> {
    for n in nodes {
        if n.children.iter().any(|c| &c.key == key) {
            return Some(n.key.clone());
        }
        if let Some(p) = parent_of(&n.children, key) {
            return Some(p);
        }
    }
    None
}

fn ancestor_chain(nodes: &[SidebarNode], target: &NodeKey) -> Option<Vec<NodeKey>> {
    for n in nodes {
        if &n.key == target {
            return Some(Vec::new());
        }
        if let Some(mut sub) = ancestor_chain(&n.children, target) {
            let mut chain = vec![n.key.clone()];
            chain.append(&mut sub);
            return Some(chain);
        }
    }
    None
}
