//! Pure multiline text-editing buffer for the TUI composer.
//!
//! [`Composer`] owns the draft text and a cursor (a byte offset that always
//! sits on a char boundary). It knows nothing about keys or terminals: the
//! event loop maps keys and paste events onto its methods, and the renderer
//! asks [`Composer::layout`] for wrapped rows plus the on-screen cursor cell.
//!
//! The buffer only ever holds printable characters and `'\n'`; tabs become
//! four spaces and every other control character is dropped on the way in.

use ratatui::text::Span;

/// Spaces inserted for a tab, whether typed or pasted.
const TAB: &str = "    ";

/// Draft text plus a cursor, edited at char (not byte) granularity.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Composer {
    text: String,
    /// Byte offset into `text`; always on a char boundary.
    cursor: usize,
}

/// Wrapped rows ready to render, plus the cursor cell within them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComposerLayout {
    pub rows: Vec<String>,
    /// `(row, col)`, with `col` in display columns.
    pub cursor: (u16, u16),
}

impl Composer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }

    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    /// Cursor as a byte offset into [`Composer::as_str`].
    pub fn cursor(&self) -> usize {
        self.cursor
    }

    /// Insert one typed character. `'\n'` starts a new line, `'\t'` becomes
    /// four spaces, and any other control character is ignored.
    pub fn insert_char(&mut self, c: char) {
        match c {
            '\n' => self.newline(),
            '\t' => self.insert_clean(TAB),
            c if c.is_control() => {}
            c => {
                self.text.insert(self.cursor, c);
                self.cursor += c.len_utf8();
            }
        }
    }

    /// Insert pasted text. `"\r\n"` and lone `'\r'` become `'\n'`, tabs become
    /// four spaces, and other control characters are dropped.
    pub fn insert_str(&mut self, s: &str) {
        let mut clean = String::with_capacity(s.len());
        let mut chars = s.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '\r' => {
                    if chars.peek() == Some(&'\n') {
                        chars.next();
                    }
                    clean.push('\n');
                }
                '\n' => clean.push('\n'),
                '\t' => clean.push_str(TAB),
                c if c.is_control() => {}
                c => clean.push(c),
            }
        }
        self.insert_clean(&clean);
    }

    pub fn newline(&mut self) {
        self.insert_clean("\n");
    }

    /// Delete the char before the cursor.
    pub fn backspace(&mut self) {
        if let Some(prev) = self.prev_boundary(self.cursor) {
            self.text.replace_range(prev..self.cursor, "");
            self.cursor = prev;
        }
    }

    /// Delete the char under (after) the cursor.
    pub fn delete(&mut self) {
        if let Some(next) = self.next_boundary(self.cursor) {
            self.text.replace_range(self.cursor..next, "");
        }
    }

    pub fn move_left(&mut self) {
        if let Some(prev) = self.prev_boundary(self.cursor) {
            self.cursor = prev;
        }
    }

    pub fn move_right(&mut self) {
        if let Some(next) = self.next_boundary(self.cursor) {
            self.cursor = next;
        }
    }

    /// Start of the current logical line.
    pub fn home(&mut self) {
        self.cursor = self.line_start(self.cursor);
    }

    /// End of the current logical line.
    pub fn end(&mut self) {
        self.cursor = self.line_end(self.cursor);
    }

    /// Previous logical line, keeping the display column (clamped to the
    /// line's end). No-op on the first line.
    pub fn move_up(&mut self) {
        let start = self.line_start(self.cursor);
        if start == 0 {
            return;
        }
        let col = display_width(&self.text[start..self.cursor]);
        let prev_start = self.line_start(start - 1);
        self.cursor = self.offset_at_col(prev_start, col);
    }

    /// Next logical line, keeping the display column (clamped to the line's
    /// end). No-op on the last line.
    pub fn move_down(&mut self) {
        let end = self.line_end(self.cursor);
        if end == self.text.len() {
            return;
        }
        let col = display_width(&self.text[self.line_start(self.cursor)..self.cursor]);
        self.cursor = self.offset_at_col(end + 1, col);
    }

    /// Ctrl+U: delete from the start of the current line to the cursor.
    pub fn kill_to_line_start(&mut self) {
        let start = self.line_start(self.cursor);
        self.text.replace_range(start..self.cursor, "");
        self.cursor = start;
    }

    /// Ctrl+W: skip whitespace backwards, then delete the run of
    /// non-whitespace before it (readline's unix-word-rubout).
    pub fn delete_word_back(&mut self) {
        let before = &self.text[..self.cursor];
        let word_end = before.trim_end_matches(char::is_whitespace).len();
        let word_start = before[..word_end]
            .trim_end_matches(|c: char| !c.is_whitespace())
            .len();
        self.text.replace_range(word_start..self.cursor, "");
        self.cursor = word_start;
    }

    /// Return the text and reset to an empty buffer.
    pub fn take(&mut self) -> String {
        self.cursor = 0;
        std::mem::take(&mut self.text)
    }

    /// Wrap the text into rows of at most `max(width, 1)` display columns.
    ///
    /// Lines break hard at `'\n'` and soft-wrap at char level; a wide char
    /// never splits across rows (it can only overflow when it alone is wider
    /// than the row). A cursor that would sit at `col == width` moves to the
    /// start of a fresh empty row inserted below it.
    pub fn layout(&self, width: u16) -> ComposerLayout {
        let width = usize::from(width.max(1));
        let mut rows = vec![String::new()];
        let mut col = 0usize;
        let mut cursor = None;
        for (i, c) in self.text.char_indices() {
            if c == '\n' {
                if i == self.cursor {
                    cursor = Some((rows.len() - 1, col));
                }
                rows.push(String::new());
                col = 0;
                continue;
            }
            let w = char_width(c);
            if col > 0 && col + w > width {
                rows.push(String::new());
                col = 0;
            }
            if i == self.cursor {
                cursor = Some((rows.len() - 1, col));
            }
            if let Some(row) = rows.last_mut() {
                row.push(c);
            }
            col += w;
        }
        let (mut row, mut col) = cursor.unwrap_or((rows.len() - 1, col));
        if col >= width {
            rows.insert(row + 1, String::new());
            row += 1;
            col = 0;
        }
        ComposerLayout {
            rows,
            cursor: (saturate(row), saturate(col)),
        }
    }

    /// Insert text that is already free of `'\r'`, tabs and other controls.
    fn insert_clean(&mut self, s: &str) {
        self.text.insert_str(self.cursor, s);
        self.cursor += s.len();
    }

    fn prev_boundary(&self, pos: usize) -> Option<usize> {
        self.text[..pos].char_indices().next_back().map(|(i, _)| i)
    }

    fn next_boundary(&self, pos: usize) -> Option<usize> {
        self.text[pos..].chars().next().map(|c| pos + c.len_utf8())
    }

    fn line_start(&self, pos: usize) -> usize {
        self.text[..pos].rfind('\n').map_or(0, |i| i + 1)
    }

    fn line_end(&self, pos: usize) -> usize {
        self.text[pos..]
            .find('\n')
            .map_or(self.text.len(), |i| pos + i)
    }

    /// Offset in the line starting at `line_start` whose display column is
    /// the largest one not exceeding `col` (clamped to the line's end).
    fn offset_at_col(&self, line_start: usize, col: usize) -> usize {
        let line_end = self.line_end(line_start);
        let mut acc = 0usize;
        for (i, c) in self.text[line_start..line_end].char_indices() {
            let w = char_width(c);
            if acc + w > col {
                return line_start + i;
            }
            acc += w;
        }
        line_end
    }
}

fn char_width(c: char) -> usize {
    let mut buf = [0u8; 4];
    Span::raw(&*c.encode_utf8(&mut buf)).width()
}

fn display_width(s: &str) -> usize {
    Span::raw(s).width()
}

fn saturate(n: usize) -> u16 {
    u16::try_from(n).unwrap_or(u16::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a composer from text with `|` marking the cursor.
    fn at(marked: &str) -> Composer {
        let cursor = marked.find('|').expect("cursor marker");
        Composer {
            text: marked.replacen('|', "", 1),
            cursor,
        }
    }

    /// Render a composer back into `|`-marked text.
    fn shown(c: &Composer) -> String {
        let mut s = c.text.clone();
        s.insert(c.cursor, '|');
        s
    }

    fn rows(l: &ComposerLayout) -> Vec<&str> {
        l.rows.iter().map(String::as_str).collect()
    }

    #[test]
    fn new_is_empty() {
        let c = Composer::new();
        assert!(c.is_empty());
        assert_eq!(c.as_str(), "");
        assert_eq!(c.cursor(), 0);
        assert_eq!(c, Composer::default());
    }

    #[test]
    fn insert_char_appends_and_inserts_mid_text() {
        let mut c = Composer::new();
        for ch in "héllo".chars() {
            c.insert_char(ch);
        }
        assert_eq!(shown(&c), "héllo|");
        assert!(!c.is_empty());
        let mut c = at("a|c");
        c.insert_char('b');
        assert_eq!(shown(&c), "ab|c");
        let mut c = at("|x");
        c.insert_char('🦀');
        assert_eq!(shown(&c), "🦀|x");
        assert_eq!(c.cursor(), 4);
    }

    #[test]
    fn insert_char_filters_controls() {
        let mut c = at("a|b");
        for ch in ['\r', '\x1b', '\x07', '\u{7f}', '\0'] {
            c.insert_char(ch);
        }
        assert_eq!(shown(&c), "a|b");
        c.insert_char('\t');
        assert_eq!(shown(&c), "a    |b");
        c.insert_char('\n');
        assert_eq!(shown(&c), "a    \n|b");
    }

    #[test]
    fn insert_str_normalizes_paste() {
        let mut c = Composer::new();
        c.insert_str("a\r\nb\rc\td");
        assert_eq!(shown(&c), "a\nb\nc    d|");
        let mut c = at("<|>");
        c.insert_str("x\x1b[31my\u{7}\r\r\nz");
        assert_eq!(shown(&c), "<x[31my\n\nz|>");
        let mut c = at("ab|");
        c.insert_str("");
        assert_eq!(shown(&c), "ab|");
        let mut c = at("|");
        c.insert_str("\r");
        assert_eq!(shown(&c), "\n|");
    }

    #[test]
    fn newline_splits_line() {
        let mut c = at("ab|cd");
        c.newline();
        assert_eq!(shown(&c), "ab\n|cd");
        let mut c = at("|");
        c.newline();
        assert_eq!(shown(&c), "\n|");
    }

    #[test]
    fn backspace_edges_and_multibyte() {
        let mut c = at("|abc");
        c.backspace();
        assert_eq!(shown(&c), "|abc");
        let mut c = at("a🦀|b");
        c.backspace();
        assert_eq!(shown(&c), "a|b");
        let mut c = at("a\n|b");
        c.backspace();
        assert_eq!(shown(&c), "a|b");
        let mut c = at("ab|");
        c.backspace();
        c.backspace();
        c.backspace();
        assert_eq!(shown(&c), "|");
    }

    #[test]
    fn delete_edges_and_multibyte() {
        let mut c = at("abc|");
        c.delete();
        assert_eq!(shown(&c), "abc|");
        let mut c = at("a|中b");
        c.delete();
        assert_eq!(shown(&c), "a|b");
        let mut c = at("a|\nb");
        c.delete();
        assert_eq!(shown(&c), "a|b");
        let mut c = at("|");
        c.delete();
        assert_eq!(shown(&c), "|");
    }

    #[test]
    fn left_right_step_by_char_and_stop_at_ends() {
        let mut c = at("|é🦀");
        c.move_left();
        assert_eq!(c.cursor(), 0);
        c.move_right();
        assert_eq!(shown(&c), "é|🦀");
        c.move_right();
        assert_eq!(shown(&c), "é🦀|");
        c.move_right();
        assert_eq!(shown(&c), "é🦀|");
        c.move_left();
        assert_eq!(shown(&c), "é|🦀");
        // combining accent is its own char
        let mut c = at("e\u{301}|");
        c.move_left();
        assert_eq!(c.cursor(), 1);
        // crosses newlines
        let mut c = at("a|\nb");
        c.move_right();
        assert_eq!(shown(&c), "a\n|b");
        c.move_left();
        assert_eq!(shown(&c), "a|\nb");
    }

    #[test]
    fn home_end_use_logical_line() {
        let mut c = at("one\ntw|o\nthree");
        c.home();
        assert_eq!(shown(&c), "one\n|two\nthree");
        c.home();
        assert_eq!(shown(&c), "one\n|two\nthree");
        c.end();
        assert_eq!(shown(&c), "one\ntwo|\nthree");
        c.end();
        assert_eq!(shown(&c), "one\ntwo|\nthree");
        let mut c = at("ab|c");
        c.home();
        assert_eq!(shown(&c), "|abc");
        c.end();
        assert_eq!(shown(&c), "abc|");
        let mut c = at("a\n|");
        c.home();
        assert_eq!(shown(&c), "a\n|");
        c.end();
        assert_eq!(shown(&c), "a\n|");
    }

    #[test]
    fn move_up_keeps_column_and_clamps() {
        let mut c = at("abcdef\nxy\nlong li|ne");
        c.move_up();
        assert_eq!(shown(&c), "abcdef\nxy|\nlong line");
        c.move_up();
        assert_eq!(shown(&c), "ab|cdef\nxy\nlong line");
        c.move_up();
        assert_eq!(shown(&c), "ab|cdef\nxy\nlong line");
        let mut c = at("\nab|");
        c.move_up();
        assert_eq!(shown(&c), "|\nab");
    }

    #[test]
    fn move_down_keeps_column_and_clamps() {
        let mut c = at("abcd|ef\nxy\nlong line");
        c.move_down();
        assert_eq!(shown(&c), "abcdef\nxy|\nlong line");
        c.move_down();
        assert_eq!(shown(&c), "abcdef\nxy\nlo|ng line");
        c.move_down();
        assert_eq!(shown(&c), "abcdef\nxy\nlo|ng line");
        let mut c = at("ab|\n");
        c.move_down();
        assert_eq!(shown(&c), "ab\n|");
    }

    #[test]
    fn vertical_moves_use_display_columns() {
        // "中" is two columns wide: column 1 falls inside it, so clamp before it.
        let mut c = at("a|bc\n中x");
        c.move_down();
        assert_eq!(shown(&c), "abc\n|中x");
        let mut c = at("ab|c\n中x");
        c.move_down();
        assert_eq!(shown(&c), "abc\n中|x");
        let mut c = at("中|x\nabc");
        c.move_down();
        assert_eq!(shown(&c), "中x\nab|c");
    }

    #[test]
    fn kill_to_line_start_only_current_line() {
        let mut c = at("keep\ndrop th|is");
        c.kill_to_line_start();
        assert_eq!(shown(&c), "keep\n|is");
        c.kill_to_line_start();
        assert_eq!(shown(&c), "keep\n|is");
        let mut c = at("ab🦀|");
        c.kill_to_line_start();
        assert_eq!(shown(&c), "|");
        let mut c = at("|abc");
        c.kill_to_line_start();
        assert_eq!(shown(&c), "|abc");
    }

    #[test]
    fn delete_word_back_rubout() {
        let mut c = at("hello world|");
        c.delete_word_back();
        assert_eq!(shown(&c), "hello |");
        c.delete_word_back();
        assert_eq!(shown(&c), "|");
        c.delete_word_back();
        assert_eq!(shown(&c), "|");
        // trailing spaces are skipped, punctuation belongs to the word
        let mut c = at("say foo.bar(),   |tail");
        c.delete_word_back();
        assert_eq!(shown(&c), "say |tail");
        // whitespace skip crosses newlines
        let mut c = at("one two\n  |");
        c.delete_word_back();
        assert_eq!(shown(&c), "one |");
        let mut c = at("   |");
        c.delete_word_back();
        assert_eq!(shown(&c), "|");
        let mut c = at("x 日本語|");
        c.delete_word_back();
        assert_eq!(shown(&c), "x |");
    }

    #[test]
    fn take_returns_text_and_resets() {
        let mut c = at("line one\nli|ne two");
        assert_eq!(c.take(), "line one\nline two");
        assert_eq!(c, Composer::new());
        assert_eq!(c.take(), "");
    }

    #[test]
    fn layout_empty() {
        let l = Composer::new().layout(10);
        assert_eq!(rows(&l), [""]);
        assert_eq!(l.cursor, (0, 0));
        let l = Composer::new().layout(0);
        assert_eq!(rows(&l), [""]);
        assert_eq!(l.cursor, (0, 0));
    }

    #[test]
    fn layout_hard_breaks() {
        let l = at("ab\n|cd").layout(10);
        assert_eq!(rows(&l), ["ab", "cd"]);
        assert_eq!(l.cursor, (1, 0));
        let l = at("ab|\ncd").layout(10);
        assert_eq!(l.cursor, (0, 2));
        let l = at("a\n\n|").layout(10);
        assert_eq!(rows(&l), ["a", "", ""]);
        assert_eq!(l.cursor, (2, 0));
    }

    #[test]
    fn layout_trailing_newline_puts_cursor_on_new_row() {
        let mut c = Composer::new();
        c.insert_str("hello\n");
        let l = c.layout(10);
        assert_eq!(rows(&l), ["hello", ""]);
        assert_eq!(l.cursor, (1, 0));
    }

    #[test]
    fn layout_soft_wraps_at_char_level() {
        let l = at("abcdefghij|").layout(4);
        assert_eq!(rows(&l), ["abcd", "efgh", "ij"]);
        assert_eq!(l.cursor, (2, 2));
        let l = at("abcde|fghij").layout(4);
        assert_eq!(l.cursor, (1, 1));
        // cursor just before a wrapped char lands at the start of its row
        let l = at("abcd|efgh").layout(4);
        assert_eq!(rows(&l), ["abcd", "efgh"]);
        assert_eq!(l.cursor, (1, 0));
    }

    #[test]
    fn layout_cursor_at_exact_width_moves_to_new_row() {
        let l = at("abcd|").layout(4);
        assert_eq!(rows(&l), ["abcd", ""]);
        assert_eq!(l.cursor, (1, 0));
        let l = at("abcdefgh|").layout(4);
        assert_eq!(rows(&l), ["abcd", "efgh", ""]);
        assert_eq!(l.cursor, (2, 0));
        // mid-text: the empty row is inserted below the full row
        let l = at("abcd|\nxy").layout(4);
        assert_eq!(rows(&l), ["abcd", "", "xy"]);
        assert_eq!(l.cursor, (1, 0));
        // cursor elsewhere: no extra row
        let l = at("|abcd").layout(4);
        assert_eq!(rows(&l), ["abcd"]);
        assert_eq!(l.cursor, (0, 0));
    }

    #[test]
    fn layout_wide_chars_never_split() {
        // 3 cols used, "中" needs 2: it moves whole to the next row.
        let l = at("abc中|").layout(4);
        assert_eq!(rows(&l), ["abc", "中"]);
        assert_eq!(l.cursor, (1, 2));
        let l = at("abc|中").layout(4);
        assert_eq!(l.cursor, (1, 0));
        // wide char exactly filling the row, cursor after it
        let l = at("ab中|").layout(4);
        assert_eq!(rows(&l), ["ab中", ""]);
        assert_eq!(l.cursor, (1, 0));
        let l = at("🦀🦀🦀|").layout(5);
        assert_eq!(rows(&l), ["🦀🦀", "🦀"]);
        assert_eq!(l.cursor, (1, 2));
        for row in &l.rows {
            assert!(display_width(row) <= 5);
        }
    }

    #[test]
    fn layout_zero_and_tiny_widths() {
        let l = at("ab|c").layout(0);
        assert_eq!(rows(&l), ["a", "b", "c"]);
        assert_eq!(l.cursor, (2, 0));
        let l = at("abc|").layout(1);
        assert_eq!(rows(&l), ["a", "b", "c", ""]);
        assert_eq!(l.cursor, (3, 0));
    }

    #[test]
    fn layout_combining_marks_stay_with_base() {
        let l = at("e\u{301}e\u{301}|").layout(2);
        assert_eq!(rows(&l), ["e\u{301}e\u{301}", ""]);
        assert_eq!(l.cursor, (1, 0));
        let l = at("e\u{301}|x").layout(10);
        assert_eq!(l.cursor, (0, 1));
    }

    #[test]
    fn layout_rows_never_exceed_width() {
        let mut c = Composer::new();
        c.insert_str("The quick 中文 fox 🦀 jumps\tover\r\nthe lazy dog é\u{301}");
        // width 1 can't hold a 2-col char at all, so start at 2
        for width in 2..=12u16 {
            let l = c.layout(width);
            for row in &l.rows {
                assert!(display_width(row) <= usize::from(width), "{width}: {row:?}");
            }
            let joined: String = l.rows.concat();
            assert_eq!(joined, c.as_str().replace('\n', ""));
            let (r, col) = l.cursor;
            assert!(usize::from(r) < l.rows.len());
            assert!(col < width);
        }
    }
}
