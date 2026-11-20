//! Rex and libghostty terminal graphics: Kitty `z=-1` card backdrops, `U+10EEEE`
//! inline badges, `OSC 7501` program status, and `DECSET 2026` synchronized frames.

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use ratatui::{buffer::Buffer, layout::Rect, style::Color};
use rex_wave_api::{
    read::{ContextCategoryKind, SidebarTreeSnapshot, ThreadContextUsage},
    stage::{Osc7501BlockKind, Osc7501State, Osc7501Status, derive_osc7501_status},
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

/// Official Kitty Unicode Placeholder row and column combining diacritics (first 64 entries).
pub const DIACRITICS: [char; 64] = [
    '\u{0305}', '\u{030D}', '\u{030E}', '\u{0310}', '\u{0312}', '\u{033D}', '\u{033E}', '\u{033F}',
    '\u{0346}', '\u{034A}', '\u{034B}', '\u{034C}', '\u{0350}', '\u{0351}', '\u{0352}', '\u{0357}',
    '\u{035B}', '\u{0363}', '\u{0364}', '\u{0365}', '\u{0366}', '\u{0367}', '\u{0368}', '\u{0369}',
    '\u{036A}', '\u{036B}', '\u{036C}', '\u{036D}', '\u{036E}', '\u{036F}', '\u{0483}', '\u{0484}',
    '\u{0485}', '\u{0486}', '\u{0487}', '\u{0592}', '\u{0593}', '\u{0594}', '\u{0595}', '\u{0597}',
    '\u{0598}', '\u{0599}', '\u{059C}', '\u{059D}', '\u{059E}', '\u{059F}', '\u{05A0}', '\u{05A1}',
    '\u{05A8}', '\u{05A9}', '\u{05AB}', '\u{05AC}', '\u{05AF}', '\u{05C4}', '\u{0610}', '\u{0611}',
    '\u{0612}', '\u{0613}', '\u{0614}', '\u{0615}', '\u{0616}', '\u{0617}', '\u{0657}', '\u{0658}',
];

pub const CARD_APPROVAL_IMAGE: ImageId = ImageId(101);
pub const CARD_MENTION_IMAGE: ImageId = ImageId(102);
pub const CARD_MAIL_IMAGE: ImageId = ImageId(103);
pub const CARD_NEUTRAL_IMAGE: ImageId = ImageId(104);
pub const CONTEXT_BAR_IMAGE: ImageId = ImageId(201);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum GraphicsMode {
    #[default]
    Hybrid,
    BackdropOnly,
    PlaceholderOnly,
    CellsOnly,
}

impl GraphicsMode {
    pub fn next(self) -> Self {
        match self {
            Self::Hybrid => Self::BackdropOnly,
            Self::BackdropOnly => Self::PlaceholderOnly,
            Self::PlaceholderOnly => Self::CellsOnly,
            Self::CellsOnly => Self::Hybrid,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Hybrid => "Hybrid z=-1 + U+10EEEE",
            Self::BackdropOnly => "z=-1 only",
            Self::PlaceholderOnly => "U+10EEEE only",
            Self::CellsOnly => "Pure Cells",
        }
    }

    pub fn uses_backdrop(self) -> bool {
        matches!(self, Self::Hybrid | Self::BackdropOnly)
    }

    pub fn uses_placeholder(self) -> bool {
        matches!(self, Self::Hybrid | Self::PlaceholderOnly)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ImageId(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellPx {
    pub w: u16,
    pub h: u16,
}

impl Default for CellPx {
    fn default() -> Self {
        // 12x24 px per cell scales crisply on 2x Retina displays inside Rex.
        Self { w: 12, h: 24 }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Clip {
    pub top_clipped_rows: u16,
    pub visible_rows: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CardPlacement {
    pub image: ImageId,
    pub placement: u32,
    pub card: Rect,
    pub viewport: Rect,
    pub tone: CardTone,
    /// Rows of `card` already scrolled above terminal row `0` (since `Rect::y` is `u16`).
    pub pre_clipped_top: u16,
}

impl CardPlacement {
    pub fn clip(&self) -> Option<Clip> {
        clip_with_pre_top(self.card, self.viewport, self.pre_clipped_top).map(|(_, c)| c)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CardTone {
    Approval,
    Mention,
    Mail,
    Neutral,
}

impl CardTone {
    pub fn image_id(self) -> ImageId {
        match self {
            Self::Approval => CARD_APPROVAL_IMAGE,
            Self::Mention => CARD_MENTION_IMAGE,
            Self::Mail => CARD_MAIL_IMAGE,
            Self::Neutral => CARD_NEUTRAL_IMAGE,
        }
    }

    fn palette(self) -> ((u8, u8, u8), (u8, u8, u8), (u8, u8, u8)) {
        match self {
            Self::Approval => ((64, 38, 16), (38, 20, 32), (255, 190, 74)),
            Self::Mention => ((18, 44, 68), (16, 26, 48), (86, 194, 255)),
            Self::Mail => ((38, 26, 62), (20, 18, 40), (186, 148, 255)),
            Self::Neutral => ((22, 34, 48), (14, 22, 34), (92, 118, 152)),
        }
    }
}

pub fn clip(card: Rect, viewport: Rect) -> Option<Clip> {
    clip_with_pre_top(card, viewport, 0).map(|(_, c)| c)
}

pub fn clip_with_pre_top(card: Rect, viewport: Rect, pre_clipped_top: u16) -> Option<(Rect, Clip)> {
    if card.width == 0 || card.height == 0 || viewport.width == 0 || viewport.height == 0 {
        return None;
    }
    let vis_left = card.x.max(viewport.x);
    let vis_right = card.right().min(viewport.right());
    if vis_right <= vis_left {
        return None;
    }
    let vis_top = card.y.max(viewport.y);
    let top_clipped_rows = pre_clipped_top.saturating_add(vis_top.saturating_sub(card.y));
    if top_clipped_rows >= card.height {
        return None;
    }
    let avail_card_rows = card.height - top_clipped_rows;
    let vis_bottom = (vis_top.saturating_add(avail_card_rows)).min(viewport.bottom());
    if vis_bottom <= vis_top {
        return None;
    }
    let visible = Rect::new(
        vis_left,
        vis_top,
        vis_right - vis_left,
        vis_bottom - vis_top,
    );
    let clip = Clip {
        top_clipped_rows,
        visible_rows: visible.height,
    };
    Some((visible, clip))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TermOut {
    Upload {
        image: ImageId,
        rgba_w: u32,
        rgba_h: u32,
    },
    UploadVirtual {
        image: ImageId,
        rgba_w: u32,
        rgba_h: u32,
        cols: u16,
        rows: u16,
    },
    Place {
        placement: CardPlacement,
        clip: Clip,
    },
    Unplace {
        image: ImageId,
        placement: u32,
    },
    DeleteAll,
    ProgramStatus(Osc7501),
    BeginSync,
    EndSync,
}

pub fn render_backdrop(tone: CardTone, w_px: u32, h_px: u32) -> Vec<u8> {
    let (top_left, bottom_right, border) = tone.palette();
    render_rounded_rgba(w_px, h_px, 16.0, top_left, bottom_right, border)
}

pub fn render_context_bar_rgba(w_px: u32, h_px: u32, usage: &ThreadContextUsage) -> Vec<u8> {
    let mut out = render_rounded_rgba(
        w_px,
        h_px,
        (h_px as f32) * 0.42,
        (16, 28, 42),
        (12, 20, 32),
        (72, 148, 204),
    );
    if w_px < 8 || h_px < 6 {
        return out;
    }

    let window = usage.model_context_window.max(usage.used_tokens).max(1) as f64;
    let pad_x = 4u32;
    let pad_y = 4u32;
    let inner_w = w_px.saturating_sub(pad_x * 2).max(1);

    let mut segments: Vec<(u32, (u8, u8, u8))> = Vec::new();
    if usage.categories.is_empty() {
        let used_px =
            (((usage.used_tokens as f64) / window).clamp(0.0, 1.0) * inner_w as f64).round() as u32;
        segments.push((used_px, (74, 232, 176)));
    } else {
        for cat in &usage.categories {
            if cat.kind == ContextCategoryKind::Free || cat.tokens == 0 {
                continue;
            }
            let seg_w =
                (((cat.tokens as f64) / window).clamp(0.0, 1.0) * inner_w as f64).round() as u32;
            let color = match cat.kind {
                ContextCategoryKind::Used => (74, 232, 176),
                ContextCategoryKind::Reserved => (255, 190, 74),
                ContextCategoryKind::Deferred => (186, 148, 255),
                ContextCategoryKind::Free => (24, 36, 52),
            };
            segments.push((seg_w, color));
        }
    }

    let mut cursor_x = pad_x;
    let max_x = w_px.saturating_sub(pad_x);
    for (seg_w, (sr, sg, sb)) in segments {
        let end_x = (cursor_x + seg_w).min(max_x);
        for x in cursor_x..end_x {
            for y in pad_y..h_px.saturating_sub(pad_y) {
                let idx = ((y * w_px + x) * 4) as usize;
                if out[idx + 3] == 0 {
                    continue;
                }
                out[idx] = sr;
                out[idx + 1] = sg;
                out[idx + 2] = sb;
                out[idx + 3] = 248;
            }
        }
        cursor_x = end_x;
    }

    if let Some(compact_at) = usage.auto_compact_at_tokens {
        let ratio = ((compact_at as f64) / window).clamp(0.0, 1.0);
        let mark_x = (pad_x + (ratio * inner_w as f64).round() as u32).min(max_x.saturating_sub(1));
        for y in 2..h_px.saturating_sub(2) {
            let idx = ((y * w_px + mark_x) * 4) as usize;
            if out[idx + 3] > 0 {
                out[idx] = 255;
                out[idx + 1] = 107;
                out[idx + 2] = 138;
                out[idx + 3] = 255;
            }
        }
    }

    out
}

fn render_rounded_rgba(
    w_px: u32,
    h_px: u32,
    corner_radius: f32,
    top_left: (u8, u8, u8),
    bottom_right: (u8, u8, u8),
    border: (u8, u8, u8),
) -> Vec<u8> {
    let mut out = vec![0u8; (w_px as usize) * (h_px as usize) * 4];
    if w_px == 0 || h_px == 0 {
        return out;
    }
    let half_w = w_px as f32 * 0.5;
    let half_h = h_px as f32 * 0.5;
    let r = corner_radius.min(half_w - 1.0).min(half_h - 1.0).max(2.0);
    let inner_w = half_w - r - 1.0;
    let inner_h = half_h - r - 1.0;

    for y in 0..h_px {
        let py = (y as f32 + 0.5) - half_h;
        let v_t = y as f32 / (h_px.max(1) as f32);
        for x in 0..w_px {
            let px = (x as f32 + 0.5) - half_w;
            let u_t = x as f32 / (w_px.max(1) as f32);

            let qx = px.abs() - inner_w;
            let qy = py.abs() - inner_h;
            let outside = (qx.max(0.0).powi(2) + qy.max(0.0).powi(2)).sqrt();
            let inside = qx.max(qy).min(0.0);
            let sdf = outside + inside - r;

            // Outer corners stay alpha 0 so the terminal background shows around the curve.
            let fill_cov = (0.6 - sdf).clamp(0.0, 1.0);
            if fill_cov <= 0.0 {
                continue;
            }

            let border_dist = (sdf + 1.1).abs();
            let border_mix = (1.35 - border_dist).clamp(0.0, 1.0);
            let grad_t = (u_t * 0.55 + v_t * 0.45).clamp(0.0, 1.0);
            let sheen = (1.0 - v_t * 2.8).clamp(0.0, 1.0) * 0.14;

            let bg_r = lerp_u8(top_left.0, bottom_right.0, grad_t);
            let bg_g = lerp_u8(top_left.1, bottom_right.1, grad_t);
            let bg_b = lerp_u8(top_left.2, bottom_right.2, grad_t);

            let final_r = lerp_f(bg_r as f32 + sheen * 45.0, border.0 as f32, border_mix);
            let final_g = lerp_f(bg_g as f32 + sheen * 55.0, border.1 as f32, border_mix);
            let final_b = lerp_f(bg_b as f32 + sheen * 70.0, border.2 as f32, border_mix);
            let alpha = (lerp_f(228.0, 250.0, border_mix) * fill_cov)
                .round()
                .clamp(0.0, 255.0) as u8;

            let idx = ((y * w_px + x) * 4) as usize;
            out[idx] = final_r.round().clamp(0.0, 255.0) as u8;
            out[idx + 1] = final_g.round().clamp(0.0, 255.0) as u8;
            out[idx + 2] = final_b.round().clamp(0.0, 255.0) as u8;
            out[idx + 3] = alpha;
        }
    }
    out
}

pub fn encode(out: &TermOut, cell: CellPx, rgba: Option<&[u8]>) -> Vec<u8> {
    let cell_w = u32::from(cell.w.max(1));
    let cell_h = u32::from(cell.h.max(1));
    match out {
        TermOut::Upload {
            image,
            rgba_w,
            rgba_h,
        } => {
            let header = format!("a=t,q=2,i={},f=32,t=d,s={rgba_w},v={rgba_h}", image.0);
            encode_chunked_kitty(&header, rgba.unwrap_or(&[])).into_bytes()
        }
        TermOut::UploadVirtual {
            image,
            rgba_w,
            rgba_h,
            cols,
            rows,
        } => {
            let header = format!(
                "a=T,U=1,q=2,i={},f=32,t=d,s={rgba_w},v={rgba_h},c={cols},r={rows}",
                image.0
            );
            encode_chunked_kitty(&header, rgba.unwrap_or(&[])).into_bytes()
        }
        TermOut::Place { placement, clip } => {
            let vis_left = placement.card.x.max(placement.viewport.x);
            let vis_right = placement.card.right().min(placement.viewport.right());
            let vis_cols = vis_right.saturating_sub(vis_left);
            let vis_rows = clip.visible_rows;
            if vis_cols == 0 || vis_rows == 0 {
                return format!(
                    "\x1b_Ga=d,d=i,i={},p={},q=2\x1b\\",
                    placement.image.0, placement.placement
                )
                .into_bytes();
            }
            let vis_y = placement.card.y.max(placement.viewport.y);
            let src_x_px = u32::from(vis_left.saturating_sub(placement.card.x)) * cell_w;
            let src_y_px = u32::from(clip.top_clipped_rows) * cell_h;
            let src_w_px = u32::from(vis_cols) * cell_w;
            let src_h_px = u32::from(vis_rows) * cell_h;
            let cup_r = vis_y + 1;
            let cup_c = vis_left + 1;
            format!(
                "\x1b[{cup_r};{cup_c}H\x1b_Ga=p,q=2,i={},p={},x={src_x_px},y={src_y_px},w={src_w_px},h={src_h_px},c={vis_cols},r={vis_rows},z=-1,C=1\x1b\\",
                placement.image.0, placement.placement
            )
            .into_bytes()
        }
        TermOut::Unplace { image, placement } => {
            // Lowercase `d=i` removes only the placement while keeping the uploaded RGBA texture cached.
            format!("\x1b_Ga=d,d=i,i={},p={placement},q=2\x1b\\", image.0).into_bytes()
        }
        TermOut::DeleteAll => b"\x1b_Ga=d,d=A,q=2\x1b\\".to_vec(),
        TermOut::ProgramStatus(osc) => encode_osc7501(osc).into_bytes(),
        TermOut::BeginSync => b"\x1b[?2026h".to_vec(),
        TermOut::EndSync => b"\x1b[?2026l".to_vec(),
    }
}

fn encode_chunked_kitty(first_header: &str, rgba: &[u8]) -> String {
    // 3072 raw bytes encode to 4096 base64 bytes, Kitty's per-chunk payload cap.
    const RAW_CHUNK: usize = 3072;
    let chunks: Vec<&[u8]> = if rgba.is_empty() {
        vec![&[]]
    } else {
        rgba.chunks(RAW_CHUNK).collect()
    };
    let total = chunks.len();
    let mut out = String::with_capacity((rgba.len() * 4 / 3) + total * 32 + 64);
    for (idx, chunk) in chunks.into_iter().enumerate() {
        let more = u8::from(idx + 1 < total);
        if idx == 0 {
            let _ = write!(out, "\x1b_G{first_header},m={more};");
        } else {
            let _ = write!(out, "\x1b_Gq=2,m={more};");
        }
        B64.encode_string(chunk, &mut out);
        out.push_str("\x1b\\");
    }
    out
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Badge {
    pub image: ImageId,
    pub cols: u16,
}

impl Badge {
    pub fn symbols(&self) -> Vec<String> {
        let cols = self.cols.min(DIACRITICS.len() as u16);
        let r_dia = DIACRITICS[0];
        (0..cols)
            .map(|c| {
                let mut sym = String::with_capacity(12);
                sym.push('\u{10EEEE}');
                sym.push(r_dia);
                sym.push(DIACRITICS[c as usize]);
                sym
            })
            .collect()
    }

    pub fn fg_color(&self) -> Color {
        let id = self.image.0;
        Color::Rgb(
            ((id >> 16) & 0xFF) as u8,
            ((id >> 8) & 0xFF) as u8,
            (id & 0xFF) as u8,
        )
    }

    pub fn write_row(&self, buf: &mut Buffer, x: u16, y: u16, max_width: u16) {
        let fg = self.fg_color();
        for (idx, sym) in self
            .symbols()
            .into_iter()
            .take(max_width as usize)
            .enumerate()
        {
            if let Some(cell) = buf.cell_mut((x + idx as u16, y)) {
                cell.set_symbol(&sym);
                cell.set_fg(fg);
                cell.set_bg(Color::Reset);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Osc7501 {
    pub state: ProgramState,
    pub kind: Option<BlockKind>,
    pub progress: Option<u8>,
    pub id: String,
    pub title: String,
    pub msg: String,
}

impl Osc7501 {
    pub fn clear() -> Self {
        Self {
            state: ProgramState::Clear,
            kind: None,
            progress: None,
            id: String::new(),
            title: String::new(),
            msg: String::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgramState {
    Idle,
    Working,
    Done,
    Blocked,
    Error,
    Clear,
}

impl ProgramState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Working => "working",
            Self::Done => "done",
            Self::Blocked => "blocked",
            Self::Error => "error",
            Self::Clear => "clear",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockKind {
    Permission,
    Question,
    Auth,
}

impl BlockKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Permission => "permission",
            Self::Question => "question",
            Self::Auth => "auth",
        }
    }
}

fn encode_osc7501(osc: &Osc7501) -> String {
    if osc.state == ProgramState::Clear {
        return "\x1b]7501;state=clear\x1b\\".to_owned();
    }
    let mut body = String::with_capacity(144);
    let _ = write!(body, "state={}", osc.state.as_str());
    if let Some(kind) = osc.kind {
        let _ = write!(body, ":kind={}", kind.as_str());
    }
    if let Some(p) = osc.progress {
        let _ = write!(body, ":progress={}", p.min(100));
    }
    if !osc.id.is_empty() {
        let _ = write!(body, ":id={}", sanitize_osc_val(&osc.id));
    }
    let _ = write!(
        body,
        ":app=rex-wave:title={}:msg={}",
        sanitize_osc_val(&osc.title),
        sanitize_osc_val(&osc.msg)
    );
    format!("\x1b]7501;{body}\x1b\\")
}

fn sanitize_osc_val(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_control() && *c != ':' && *c != ';' && *c != '\\')
        .collect()
}

pub fn program_status(sidebar: &SidebarTreeSnapshot) -> Osc7501 {
    let all_phases: Vec<_> = sidebar
        .projects
        .iter()
        .flat_map(|p| p.waves.iter())
        .flat_map(|w| w.workers.iter().map(|wk| &wk.phase))
        .collect();
    let derived = if sidebar.osc7501 == Osc7501Status::default()
        && (!all_phases.is_empty() || sidebar.inbox.unread > 0 || sidebar.inbox.blocking > 0)
    {
        derive_osc7501_status(&sidebar.inbox, all_phases)
    } else {
        sidebar.osc7501.clone()
    };

    let state = match derived.state {
        Osc7501State::Idle => ProgramState::Idle,
        Osc7501State::Working => ProgramState::Working,
        Osc7501State::Done => ProgramState::Done,
        Osc7501State::Blocked => ProgramState::Blocked,
        Osc7501State::Error => ProgramState::Error,
    };
    let kind = derived.block_kind.map(|k| match k {
        Osc7501BlockKind::Permission => BlockKind::Permission,
        Osc7501BlockKind::Question => BlockKind::Question,
        Osc7501BlockKind::Auth => BlockKind::Auth,
    });

    let primary_wave = sidebar.projects.iter().flat_map(|p| p.waves.iter()).next();
    let (id, title) = match primary_wave {
        Some(w) => (format!("wave-{}", w.wave), format!("#wave-{}", w.wave)),
        None => ("rex-wave".to_owned(), "rex-wave".to_owned()),
    };
    let msg = if derived.message.is_empty() {
        "ready".to_owned()
    } else {
        derived.message
    };
    Osc7501 {
        state,
        kind,
        progress: derived.progress_pct,
        id,
        title,
        msg,
    }
}

/// Tracks uploaded GPU textures and active `z=-1` placements across frames so
/// scrolling or switching stages emits minimal `a=p` crops and `a=d,d=i` unplaces.
#[derive(Default)]
pub struct KittyCompositor {
    cell: CellPx,
    uploaded_cards: BTreeMap<CardTone, (u32, u32)>,
    placed_cards: BTreeMap<(ImageId, u32), (CardPlacement, Clip)>,
    uploaded_bar: Option<(u16, u64, u64, Option<u64>, usize)>,
    last_osc: Option<Osc7501>,
}

impl KittyCompositor {
    pub fn new(cell: CellPx) -> Self {
        Self {
            cell,
            ..Self::default()
        }
    }

    pub fn invalidate_textures(&mut self) {
        self.uploaded_cards.clear();
        self.placed_cards.clear();
        self.uploaded_bar = None;
    }

    pub fn prepare_pre_draw(
        &mut self,
        mode: GraphicsMode,
        usage_bar: Option<(u16, &ThreadContextUsage)>,
    ) -> Vec<u8> {
        let mut bytes = encode(&TermOut::BeginSync, self.cell, None);
        if mode.uses_placeholder()
            && let Some((cols, usage)) = usage_bar
            && cols > 0
        {
            let sig = (
                cols,
                usage.used_tokens,
                usage.model_context_window,
                usage.auto_compact_at_tokens,
                usage.categories.len(),
            );
            if self.uploaded_bar != Some(sig) {
                let w_px = u32::from(cols) * u32::from(self.cell.w.max(1));
                let h_px = u32::from(self.cell.h.max(1));
                let rgba = render_context_bar_rgba(w_px, h_px, usage);
                bytes.extend_from_slice(&encode(
                    &TermOut::UploadVirtual {
                        image: CONTEXT_BAR_IMAGE,
                        rgba_w: w_px,
                        rgba_h: h_px,
                        cols,
                        rows: 1,
                    },
                    self.cell,
                    Some(&rgba),
                ));
                self.uploaded_bar = Some(sig);
            }
        }
        bytes
    }

    pub fn finish_post_draw(
        &mut self,
        mode: GraphicsMode,
        cards: &[CardPlacement],
        osc: &Osc7501,
    ) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut next_active = BTreeSet::new();

        if mode.uses_backdrop() {
            let cell_w = u32::from(self.cell.w.max(1));
            let cell_h = u32::from(self.cell.h.max(1));
            for card in cards {
                let w_px = u32::from(card.card.width) * cell_w;
                let h_px = u32::from(card.card.height) * cell_h;
                if w_px == 0 || h_px == 0 {
                    continue;
                }
                if self.uploaded_cards.get(&card.tone) != Some(&(w_px, h_px)) {
                    let rgba = render_backdrop(card.tone, w_px, h_px);
                    bytes.extend_from_slice(&encode(
                        &TermOut::Upload {
                            image: card.image,
                            rgba_w: w_px,
                            rgba_h: h_px,
                        },
                        self.cell,
                        Some(&rgba),
                    ));
                    self.uploaded_cards.insert(card.tone, (w_px, h_px));
                    self.placed_cards.retain(|(img, _), _| *img != card.image);
                }

                if let Some(c) = card.clip() {
                    let key = (card.image, card.placement);
                    next_active.insert(key);
                    let entry = (card.clone(), c);
                    if self.placed_cards.get(&key) != Some(&entry) {
                        bytes.extend_from_slice(&encode(
                            &TermOut::Place {
                                placement: card.clone(),
                                clip: c,
                            },
                            self.cell,
                            None,
                        ));
                        self.placed_cards.insert(key, entry);
                    }
                }
            }
        }

        let stale: Vec<(ImageId, u32)> = self
            .placed_cards
            .keys()
            .filter(|k| !next_active.contains(k))
            .copied()
            .collect();
        for (image, placement) in stale {
            self.placed_cards.remove(&(image, placement));
            bytes.extend_from_slice(&encode(
                &TermOut::Unplace { image, placement },
                self.cell,
                None,
            ));
        }

        if self.last_osc.as_ref() != Some(osc) {
            bytes.extend_from_slice(&encode(
                &TermOut::ProgramStatus(osc.clone()),
                self.cell,
                None,
            ));
            self.last_osc = Some(osc.clone());
        }

        bytes.extend_from_slice(&encode(&TermOut::EndSync, self.cell, None));
        bytes
    }

    pub fn shutdown_bytes(&self) -> Vec<u8> {
        let mut out = encode(&TermOut::DeleteAll, self.cell, None);
        out.extend_from_slice(&encode(
            &TermOut::ProgramStatus(Osc7501::clear()),
            self.cell,
            None,
        ));
        out
    }
}

fn lerp_u8(a: u8, b: u8, t: f32) -> u8 {
    (a as f32 + (b as f32 - a as f32) * t)
        .round()
        .clamp(0.0, 255.0) as u8
}

fn lerp_f(a: f32, b: f32, t: f32) -> f32 {
    a + (b - a) * t
}
