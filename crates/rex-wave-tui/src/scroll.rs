//! Visual-line scroll state shared by the room feed, agent inspector, pager, and help.
//!
//! `pos == FOLLOW` pins the view to the bottom (new lines stay visible). The renderer
//! records the real maximum offset and viewport height after laying out wrapped lines,
//! so keyboard and wheel math in `update` works in visual lines rather than items.

use std::cell::Cell;

pub const FOLLOW: u16 = u16::MAX;

#[derive(Clone, Debug)]
pub struct ScrollState {
    pub pos: u16,
    max: Cell<u16>,
    page: Cell<u16>,
}

impl Default for ScrollState {
    fn default() -> Self {
        Self::top()
    }
}

impl ScrollState {
    pub fn top() -> Self {
        Self {
            pos: 0,
            max: Cell::new(0),
            page: Cell::new(1),
        }
    }

    pub fn follow() -> Self {
        Self {
            pos: FOLLOW,
            ..Self::top()
        }
    }

    /// Records the laid-out content height and viewport, and returns the first visible line.
    pub fn resolve(&self, total_lines: usize, viewport: u16) -> usize {
        let max = total_lines
            .saturating_sub(viewport as usize)
            .min(u16::MAX as usize - 1) as u16;
        self.max.set(max);
        self.page.set(viewport.max(1));
        self.offset() as usize
    }

    pub fn offset(&self) -> u16 {
        if self.pos == FOLLOW {
            self.max.get()
        } else {
            self.pos.min(self.max.get())
        }
    }

    pub fn by(&mut self, delta: i32) {
        let max = i32::from(self.max.get());
        let next = (i32::from(self.offset()) + delta).clamp(0, max);
        self.pos = if next >= max && delta > 0 {
            FOLLOW
        } else {
            next as u16
        };
    }

    pub fn by_pages(&mut self, pages: i32) {
        let step = i32::from(self.page.get().saturating_sub(1).max(1));
        self.by(pages * step);
    }

    pub fn to_top(&mut self) {
        self.pos = 0;
    }

    pub fn to_bottom(&mut self) {
        self.pos = FOLLOW;
    }

    /// Scrolls the minimum amount so lines `top..top + height` are visible; content taller
    /// than the viewport shows its first line.
    pub fn reveal(&mut self, top: u16, height: u16) {
        let page = self.page.get().max(1);
        let cur = self.offset();
        let target = if top < cur || height > page {
            top
        } else if top.saturating_add(height) > cur.saturating_add(page) {
            top.saturating_add(height).saturating_sub(page)
        } else {
            cur
        };
        self.pos = if target >= self.max.get() && self.max.get() > 0 {
            FOLLOW
        } else {
            target
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn follow_tracks_growth_and_scrolling_up_unpins() {
        let mut s = ScrollState::follow();
        assert_eq!(s.resolve(100, 10), 90);
        assert_eq!(s.resolve(120, 10), 110, "following view sticks to the tail");
        s.by(-3);
        assert_eq!(s.pos, 107);
        assert_eq!(s.resolve(130, 10), 107, "unpinned view holds its place");
        s.by(1000);
        assert_eq!(s.pos, FOLLOW, "scrolling back to the end re-pins");
    }

    #[test]
    fn pages_and_reveal_use_visual_lines() {
        let mut s = ScrollState::top();
        s.resolve(50, 10);
        s.by_pages(1);
        assert_eq!(s.offset(), 9);
        s.by_pages(-5);
        assert_eq!(s.offset(), 0);
        s.reveal(30, 4);
        assert_eq!(s.offset(), 24);
        s.reveal(5, 30);
        assert_eq!(s.offset(), 5, "tall item shows its first line");
        s.reveal(45, 5);
        assert_eq!(s.pos, FOLLOW);
    }

    #[test]
    fn short_content_never_scrolls() {
        let mut s = ScrollState::follow();
        assert_eq!(s.resolve(3, 10), 0);
        s.by(-5);
        assert_eq!(s.offset(), 0);
        s.by(5);
        assert_eq!(s.offset(), 0);
    }
}
