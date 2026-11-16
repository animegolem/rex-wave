//! Rex/libghostty graphics: Kitty z=-1 card backdrops, U+10EEEE badges,
//! OSC 7501 program status, DECSET 2026 synchronized frames.
//!
//! Rules settled empirically in #18:
//! * **A**, card interior cells stay `Color::Reset`; any `SGR 48;2` cell bg hides z=-1.
//! * **B**, wrap `terminal.draw()` *and* the `a=p` placements in one
//!   `CSI ?2026h … CSI ?2026l` so text and backdrop never tear on scroll.
//! * **C**, ratatui diffs cells (no hardware scroll), so we compute each
//!   card's clipped row slice ourselves and re-place with `y/h/r` crop; when a
//!   card scrolls fully offscreen, delete the *placement* (`a=d,d=i` lowercase)
//!   so the uploaded texture stays cached.

use ratatui::layout::Rect;
use rex_wave_api::read::SidebarTreeSnapshot;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum GraphicsMode {
    /// z=-1 cards + U+10EEEE badges (recommended in Rex).
    #[default]
    Hybrid,
    BackdropOnly,
    PlaceholderOnly,
    /// No APCs at all (SSH / non-Kitty terminals).
    CellsOnly,
}

/// Kitty image id of an uploaded RGBA backdrop (uploaded once per size/theme).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ImageId(pub u32);

/// Cell metrics needed to convert rows to pixels.
#[derive(Clone, Copy, Debug)]
pub struct CellPx {
    pub w: u16,
    pub h: u16,
}

/// The visible slice of a card after viewport clipping (Rule C).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Clip {
    pub top_clipped_rows: u16,
    pub visible_rows: u16,
}

/// One card to place behind text this frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CardPlacement {
    pub image: ImageId,
    pub placement: u32,
    /// Full (unclipped) card rect in cells, possibly partly outside `viewport`.
    pub card: Rect,
    pub viewport: Rect,
    pub tone: CardTone,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CardTone {
    Approval,
    Mention,
    Mail,
    Neutral,
}

/// Pure: clip math for Rule C. `None` = fully offscreen (emit a placement delete).
pub fn clip(card: Rect, viewport: Rect) -> Option<Clip> {
    let _ = (card, viewport);
    todo!("clip")
}

/// Terminal output the shell must write (outside ratatui's buffer).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TermOut {
    /// `ESC _G a=t,...` chunked base64 RGBA upload (once per size/tone/theme).
    Upload { image: ImageId, rgba_w: u32, rgba_h: u32 },
    /// `ESC _G a=p,i=..,p=..,x=0,y=..,w=..,h=..,c=..,r=..,z=-1,C=1,q=2`
    Place { placement: CardPlacement, clip: Clip },
    /// `ESC _G a=d,d=i,i=..,p=..` (keeps texture cached)
    Unplace { image: ImageId, placement: u32 },
    /// `ESC ]7501;state=..:kind=..:progress=..:id=..:app=rex-wave:title=..:msg=.. ESC \`
    ProgramStatus(Osc7501),
    BeginSync,
    EndSync,
}

/// Pure: SDF rounded-rect gradient RGBA for a card tone at a pixel size.
pub fn render_backdrop(tone: CardTone, w_px: u32, h_px: u32) -> Vec<u8> {
    let _ = (tone, w_px, h_px);
    todo!("render_backdrop")
}

/// Pure: serialize a `TermOut` into escape bytes (base64 chunking <= 4096 B for uploads).
pub fn encode(out: &TermOut, cell: CellPx, rgba: Option<&[u8]>) -> Vec<u8> {
    let _ = (out, cell, rgba, base64::engine::general_purpose::STANDARD);
    todo!("encode")
}

/// A `U+10EEEE` placeholder badge (inline sparkline / avatar) occupying `cols` cells.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Badge {
    pub image: ImageId,
    pub cols: u16,
}

impl Badge {
    /// Cell symbols: `\u{10EEEE}` + row/col diacritics; image id rides in the 24-bit fg.
    pub fn symbols(&self) -> Vec<String> {
        todo!("Badge::symbols")
    }
}

/// OSC 7501 state per Rex `program_status` (`idle|working|done|blocked|error`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Osc7501 {
    pub state: ProgramState,
    pub kind: Option<BlockKind>,
    pub progress: Option<u8>,
    pub id: String,
    pub title: String,
    pub msg: String,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockKind {
    Permission,
    Question,
    Auth,
}

/// Pure: summarize the whole estate into one Rex pane status. Blocked if any
/// approval is pending (kind=permission/question), else working if any agent
/// works, else idle. Progress = finished workers / total in running waves.
pub fn program_status(sidebar: &SidebarTreeSnapshot) -> Osc7501 {
    let _ = sidebar;
    todo!("program_status")
}
