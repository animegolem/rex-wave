//! Modal overlays drawn above both panes: the `?` keymap help and the message pager.

use crate::input::{HelpRow, HitMap, Target, help_rows};
use crate::kitty::GraphicsMode;
use crate::richtext::{Palette, display_width, markdown_lines};
use crate::scroll::ScrollState;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Clear, Paragraph, Widget},
};

const C_ACCENT: Color = Color::Rgb(86, 194, 255);
const C_MINT: Color = Color::Rgb(74, 232, 176);
const C_AMBER: Color = Color::Rgb(255, 190, 74);
const C_LAVENDER: Color = Color::Rgb(186, 148, 255);
const C_DIM: Color = Color::Rgb(126, 138, 164);
const C_TEXT: Color = Color::Rgb(232, 238, 249);
const C_BG: Color = Color::Rgb(14, 18, 28);

pub enum Overlay {
    Help {
        scroll: ScrollState,
    },
    Pager {
        title: String,
        body: String,
        scroll: ScrollState,
    },
}

impl Overlay {
    pub fn help() -> Self {
        Self::Help {
            scroll: ScrollState::top(),
        }
    }

    pub fn pager(title: String, body: String) -> Self {
        Self::Pager {
            title,
            body,
            scroll: ScrollState::top(),
        }
    }

    pub fn scroll_mut(&mut self) -> &mut ScrollState {
        match self {
            Self::Help { scroll } | Self::Pager { scroll, .. } => scroll,
        }
    }

    pub fn render(&self, area: Rect, buf: &mut Buffer, hits: &mut HitMap, graphics: GraphicsMode) {
        if area.width < 20 || area.height < 6 {
            return;
        }
        let w = area.width.saturating_sub(4).min(118);
        let h = area.height.saturating_sub(2);
        let rect = Rect::new(area.x + (area.width - w) / 2, area.y + 1, w, h);
        Clear.render(rect, buf);
        let (title, scroll) = match self {
            Self::Help { scroll } => (" Keys · rex-wave ".to_owned(), scroll),
            Self::Pager { title, scroll, .. } => (format!(" {title} "), scroll),
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(C_ACCENT))
            .style(Style::default().bg(C_BG))
            .title(Span::styled(
                title,
                Style::default().fg(C_ACCENT).add_modifier(Modifier::BOLD),
            ))
            .title_bottom(Span::styled(
                " j/k ↑/↓ scroll · pgup/pgdn/space page · g/G ends · esc/q/? close ",
                Style::default().fg(C_DIM),
            ));
        let inner = block.inner(rect);
        block.render(rect, buf);
        hits.push(area, Target::Overlay);
        let body = Rect::new(
            inner.x + 1,
            inner.y,
            inner.width.saturating_sub(2),
            inner.height,
        );
        if body.width == 0 || body.height == 0 {
            return;
        }
        let lines = match self {
            Self::Help { .. } => help_lines(body.width, graphics),
            Self::Pager { body: text, .. } => markdown_lines(text, body.width, &pager_palette()),
        };
        let start = scroll.resolve(lines.len(), body.height);
        let total = lines.len();
        for (off, line) in lines
            .into_iter()
            .skip(start)
            .take(body.height as usize)
            .enumerate()
        {
            Paragraph::new(line).render(Rect::new(body.x, body.y + off as u16, body.width, 1), buf);
        }
        if total > body.height as usize {
            let pct = ((start + body.height as usize) * 100 / total).min(100);
            let tag = format!(" {pct}% ");
            let tw = tag.len() as u16;
            Paragraph::new(Line::from(Span::styled(tag, Style::default().fg(C_DIM)))).render(
                Rect::new(rect.right().saturating_sub(tw + 2), rect.y, tw, 1),
                buf,
            );
        }
    }
}

fn pager_palette() -> Palette {
    Palette {
        text: Style::default().fg(C_TEXT),
        heading: Style::default().fg(C_ACCENT).add_modifier(Modifier::BOLD),
        bold: Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
        code: Style::default().fg(C_LAVENDER),
        code_block: Style::default().fg(C_MINT),
        fence: Style::default().fg(C_DIM),
        mention: Style::default().fg(C_AMBER).add_modifier(Modifier::BOLD),
        quote: Style::default().fg(C_DIM).add_modifier(Modifier::ITALIC),
    }
}

fn render_group_column(rows: &[&HelpRow], col_w: usize) -> Vec<Line<'static>> {
    let key_w = rows
        .iter()
        .map(|r| display_width(&r.keys))
        .max()
        .unwrap_or(0)
        .min(24);
    let mut out = Vec::new();
    let mut group = "";
    for row in rows {
        if row.group != group {
            group = row.group;
            out.push(Line::from(Span::styled(
                group.to_owned(),
                Style::default().fg(C_ACCENT).add_modifier(Modifier::BOLD),
            )));
        }
        let keys_shown: String = row.keys.chars().take(key_w).collect();
        let pad = key_w.saturating_sub(display_width(&keys_shown));
        let help_w = col_w.saturating_sub(key_w + 3);
        let help: String = row.help.chars().take(help_w).collect();
        out.push(Line::from(vec![
            Span::raw(" "),
            Span::styled(
                format!("{keys_shown}{}", " ".repeat(pad)),
                Style::default().fg(C_AMBER).add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(help, Style::default().fg(C_TEXT)),
        ]));
    }
    out
}

fn pad_line(mut line: Line<'static>, target_w: usize) -> Vec<Span<'static>> {
    let cur = line.width();
    if cur < target_w {
        line.spans.push(Span::raw(" ".repeat(target_w - cur)));
    }
    line.spans
}

/// Help overlay lines, generated from `input::KEYMAP` via `help_rows`.
pub fn help_lines(width: u16, graphics: GraphicsMode) -> Vec<Line<'static>> {
    let rows = help_rows();
    let mut out = vec![
        Line::from(vec![
            Span::styled(
                "Graphics: ",
                Style::default().fg(C_ACCENT).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                graphics.label(),
                Style::default().fg(C_LAVENDER).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                "  (v cycles) · drag sidebar border or < / > / \\ to resize",
                Style::default().fg(C_DIM),
            ),
        ]),
        Line::default(),
    ];

    if width >= 88 {
        let left_groups = ["Global", "Sidebar", "Inbox", "Kanban"];
        let left_rows: Vec<&HelpRow> = rows
            .iter()
            .filter(|r| left_groups.contains(&r.group))
            .collect();
        let right_rows: Vec<&HelpRow> = rows
            .iter()
            .filter(|r| !left_groups.contains(&r.group))
            .collect();
        let col_w = ((width as usize).saturating_sub(3)) / 2;
        let left = render_group_column(&left_rows, col_w);
        let right = render_group_column(&right_rows, col_w);
        let max_len = left.len().max(right.len());
        for idx in 0..max_len {
            let l = left.get(idx).cloned().unwrap_or_default();
            let r = right.get(idx).cloned().unwrap_or_default();
            let mut spans = pad_line(l, col_w);
            spans.push(Span::styled(" │ ", Style::default().fg(C_DIM)));
            spans.extend(r.spans);
            out.push(Line::from(spans));
        }
    } else {
        let refs: Vec<&HelpRow> = rows.iter().collect();
        out.extend(render_group_column(&refs, width as usize));
    }
    out
}
