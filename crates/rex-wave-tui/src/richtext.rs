//! Markdown-lite rendering for room posts and agent turns.
//!
//! Pure helpers: they turn a message body into word-wrapped ratatui
//! [`Line`]s that never exceed the requested width. Supported syntax is a
//! small, forgiving subset of Markdown: ATX headings, fenced code blocks,
//! `> ` quotes, `-`/`*`/`+`/`1.` list items, `**bold**`, `` `code` `` and
//! `@mentions`. Anything unmatched renders literally.

use ratatui::{
    style::Style,
    text::{Line, Span},
};

/// Styles used by [`markdown_lines`].
#[derive(Clone, Copy, Debug)]
pub struct Palette {
    pub text: Style,
    pub heading: Style,
    pub bold: Style,
    pub code: Style,
    pub code_block: Style,
    pub fence: Style,
    pub mention: Style,
    pub quote: Style,
}

/// Terminal display width of `s` in columns (wide chars count 2).
pub fn display_width(s: &str) -> usize {
    Span::raw(s).width()
}

/// Markdown-lite → word-wrapped lines; every returned Line has width() <= max(width,1).
pub fn markdown_lines(body: &str, width: u16, p: &Palette) -> Vec<Line<'static>> {
    let w = usize::from(width.max(1));
    let mut out: Vec<Line<'static>> = Vec::new();
    let mut in_fence = false;
    // True when the last emitted line came from a blank source line.
    let mut last_blank = false;

    for raw in body.split('\n') {
        let line = sanitize(raw);
        let trimmed = line.trim_start();

        if in_fence {
            if trimmed.starts_with("```") {
                in_fence = false;
                out.extend(hard_wrap(&clusters_of(&[("┄┄".into(), p.fence)]), w));
            } else {
                out.extend(hard_wrap(&clusters_of(&[(line.clone(), p.code_block)]), w));
            }
            last_blank = false;
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix("```") {
            in_fence = true;
            let lang = rest.trim();
            let label = if lang.is_empty() {
                "┄┄".to_string()
            } else {
                format!("┄┄ {lang}")
            };
            out.extend(hard_wrap(&clusters_of(&[(label, p.fence)]), w));
            last_blank = false;
            continue;
        }

        if trimmed.is_empty() {
            if !last_blank {
                out.push(Line::default());
                last_blank = true;
            }
            continue;
        }
        last_blank = false;

        if let Some(text) = heading_text(trimmed) {
            let cl = clusters_of(&[(text.to_string(), p.heading)]);
            out.extend(wrap_clusters(&cl, w, Vec::new(), Vec::new()));
            continue;
        }

        if let Some(content) = quote_text(trimmed) {
            let bar = clusters_of(&[("▎ ".into(), p.quote)]);
            let cl = clusters_of(&inline(content, p.quote, p));
            out.extend(wrap_clusters(&cl, w, bar.clone(), bar));
            continue;
        }

        if let Some((marker, content)) = list_item(&line) {
            let indent = " ".repeat(display_width(&marker));
            let prefix = clusters_of(&[(marker, p.text)]);
            let indent = clusters_of(&[(indent, Style::default())]);
            let cl = clusters_of(&inline(content, p.text, p));
            out.extend(wrap_clusters(&cl, w, prefix, indent));
            continue;
        }

        let cl = clusters_of(&inline(&line, p.text, p));
        out.extend(wrap_clusters(&cl, w, Vec::new(), Vec::new()));
    }

    // A trailing newline (or trailing blank lines) should not leave an
    // empty row at the bottom of the message.
    if last_blank {
        out.pop();
    }
    out
}

// ---------------------------------------------------------------------------
// Block-level helpers

/// Strip `\r`, expand tabs to 4 spaces, drop other control characters.
fn sanitize(raw: &str) -> String {
    let mut s = String::with_capacity(raw.len());
    for c in raw.chars() {
        match c {
            '\t' => s.push_str("    "),
            c if c.is_control() => {}
            c => s.push(c),
        }
    }
    s
}

/// `# Title` .. `###### Title` → `Title`.
fn heading_text(trimmed: &str) -> Option<&str> {
    let hashes = trimmed.bytes().take_while(|&b| b == b'#').count();
    if !(1..=6).contains(&hashes) {
        return None;
    }
    let rest = trimmed[hashes..].strip_prefix(' ')?;
    Some(rest.trim())
}

/// `> text` (or a bare `>`) → `text`.
fn quote_text(trimmed: &str) -> Option<&str> {
    if trimmed == ">" {
        return Some("");
    }
    trimmed.strip_prefix("> ")
}

/// List item → (rendered marker incl. leading spaces and trailing space, content).
fn list_item(line: &str) -> Option<(String, &str)> {
    let lead = line.len() - line.trim_start_matches(' ').len();
    let rest = &line[lead..];
    let pad = " ".repeat(lead);
    for bullet in ["- ", "* ", "+ "] {
        if let Some(content) = rest.strip_prefix(bullet) {
            return Some((format!("{pad}• "), content));
        }
    }
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    if (1..=9).contains(&digits) {
        let after = &rest[digits..];
        for sep in [". ", ") "] {
            if let Some(content) = after.strip_prefix(sep) {
                return Some((format!("{pad}{}{sep}", &rest[..digits]), content));
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Inline parsing

fn push_seg(segs: &mut Vec<(String, Style)>, s: &str, style: Style) {
    if s.is_empty() {
        return;
    }
    match segs.last_mut() {
        Some((last, st)) if *st == style => last.push_str(s),
        _ => segs.push((s.to_string(), style)),
    }
}

fn is_mention_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '-' | '.' | '/' | ':')
}

/// Parse `**bold**`, `` `code` `` and `@mentions` with `base` as the text style.
fn inline(s: &str, base: Style, p: &Palette) -> Vec<(String, Style)> {
    let mut segs = Vec::new();
    inline_into(&mut segs, s, base, p, true);
    segs
}

fn inline_into(
    segs: &mut Vec<(String, Style)>,
    s: &str,
    base: Style,
    p: &Palette,
    allow_bold: bool,
) {
    let mut i = 0;
    let mut lit_start = 0;
    let mut prev: Option<char> = None;
    while i < s.len() {
        let rest = &s[i..];
        // `code`
        if let Some(after) = rest.strip_prefix('`')
            && let Some(end) = after.find('`')
            && end > 0
        {
            push_seg(segs, &s[lit_start..i], base);
            push_seg(segs, &after[..end], p.code);
            i += 1 + end + 1;
            lit_start = i;
            prev = Some('`');
            continue;
        }
        // **bold**
        if allow_bold
            && let Some(after) = rest.strip_prefix("**")
            && let Some(end) = after.find("**")
            && end > 0
            // Like CommonMark flanking: `** x **` is not emphasis.
            && !after.starts_with(char::is_whitespace)
            && !after[..end].ends_with(char::is_whitespace)
        {
            push_seg(segs, &s[lit_start..i], base);
            inline_into(segs, &after[..end], p.bold, p, false);
            i += 2 + end + 2;
            lit_start = i;
            prev = Some('*');
            continue;
        }
        let c = rest.chars().next().unwrap_or(' ');
        // @mention at a word start
        if c == '@'
            && prev.is_none_or(|pc| pc.is_whitespace() || matches!(pc, '(' | '[' | '"' | '\''))
            && rest[1..].chars().next().is_some_and(char::is_alphanumeric)
        {
            let len = 1 + rest[1..]
                .char_indices()
                .find(|&(_, ch)| !is_mention_char(ch))
                .map_or(rest.len() - 1, |(j, _)| j);
            // Trailing sentence punctuation is not part of the handle.
            let handle = rest[..len].trim_end_matches(['.', ':', '-', '/']);
            push_seg(segs, &s[lit_start..i], base);
            push_seg(segs, handle, p.mention);
            i += handle.len();
            lit_start = i;
            prev = handle.chars().last();
            continue;
        }
        i += c.len_utf8();
        prev = Some(c);
    }
    push_seg(segs, &s[lit_start..], base);
}

// ---------------------------------------------------------------------------
// Wrapping

/// A base char plus any following zero-width chars (combining marks,
/// variation selectors, ZWJ). Never split across lines.
#[derive(Clone, Debug)]
struct Cluster {
    s: String,
    w: usize,
    style: Style,
}

impl Cluster {
    fn is_space(&self) -> bool {
        self.s == " "
    }
}

fn char_width(c: char) -> usize {
    let mut buf = [0u8; 4];
    display_width(c.encode_utf8(&mut buf))
}

fn clusters_of(segs: &[(String, Style)]) -> Vec<Cluster> {
    let mut out: Vec<Cluster> = Vec::new();
    for (text, style) in segs {
        let mut seg_start = true;
        for c in text.chars() {
            let cw = char_width(c);
            if cw == 0
                && !seg_start
                && let Some(last) = out.last_mut()
            {
                last.s.push(c);
                continue;
            }
            seg_start = false;
            out.push(Cluster {
                s: c.to_string(),
                w: cw,
                style: *style,
            });
        }
    }
    for c in &mut out {
        // Sequences such as `❤` + VS16 are wider together than apart.
        c.w = c.w.max(display_width(&c.s));
    }
    out
}

fn width_of(cl: &[Cluster]) -> usize {
    cl.iter().map(|c| c.w).sum()
}

/// Trim a prefix/indent so at least `min(2, width)` columns remain for content.
fn fit_prefix(mut prefix: Vec<Cluster>, width: usize) -> Vec<Cluster> {
    let room = width.min(2);
    while !prefix.is_empty() && width_of(&prefix) + room > width {
        prefix.pop();
    }
    prefix
}

/// Placeholder for a cluster that cannot fit even on an empty line (width 1).
fn too_wide(c: &Cluster) -> Cluster {
    Cluster {
        s: "?".into(),
        w: 1,
        style: c.style,
    }
}

fn build_line(mut cur: Vec<Cluster>, width: usize) -> Line<'static> {
    while cur.last().is_some_and(Cluster::is_space) {
        cur.pop();
    }
    loop {
        let mut spans: Vec<Span<'static>> = Vec::new();
        let mut buf = String::new();
        let mut style: Option<Style> = None;
        for c in &cur {
            if style.is_some_and(|s| s != c.style) {
                spans.push(Span::styled(
                    std::mem::take(&mut buf),
                    style.unwrap_or_default(),
                ));
            }
            style = Some(c.style);
            buf.push_str(&c.s);
        }
        if let Some(s) = style {
            spans.push(Span::styled(buf, s));
        }
        let line = Line::from(spans);
        // Guard against width measurements that disagree once clusters are
        // joined; the invariant beats the last glyph.
        if line.width() <= width || cur.is_empty() {
            return line;
        }
        cur.pop();
    }
}

struct Wrapper {
    width: usize,
    indent: Vec<Cluster>,
    out: Vec<Line<'static>>,
    cur: Vec<Cluster>,
    cur_w: usize,
    has_content: bool,
}

impl Wrapper {
    fn flush(&mut self) {
        let cur = std::mem::replace(&mut self.cur, self.indent.clone());
        self.out.push(build_line(cur, self.width));
        self.cur_w = width_of(&self.indent);
        self.has_content = false;
    }

    fn push(&mut self, c: Cluster) {
        self.cur_w += c.w;
        self.cur.push(c);
        self.has_content = true;
    }

    /// Place a word char by char, breaking lines wherever it overflows.
    fn hard_split(&mut self, word: &[Cluster]) {
        for c in word {
            if self.cur_w + c.w > self.width && self.has_content {
                self.flush();
            }
            if self.cur_w + c.w > self.width {
                self.push(too_wide(c));
            } else {
                self.push(c.clone());
            }
        }
    }
}

/// Greedy word wrap. `first` prefixes the first row, `indent` every later
/// row. Leading spaces of the source line are kept on the first row only.
fn wrap_clusters(
    cl: &[Cluster],
    width: usize,
    first: Vec<Cluster>,
    indent: Vec<Cluster>,
) -> Vec<Line<'static>> {
    let first = fit_prefix(first, width);
    let indent = fit_prefix(indent, width);
    let mut wr = Wrapper {
        width,
        cur_w: width_of(&first),
        cur: first,
        indent,
        out: Vec::new(),
        has_content: false,
    };
    let mut pending: Vec<Cluster> = Vec::new();
    let mut at_start = true;
    let mut i = 0;
    while i < cl.len() {
        let space = cl[i].is_space();
        let j = cl[i..]
            .iter()
            .position(|c| c.is_space() != space)
            .map_or(cl.len(), |k| i + k);
        let tok = &cl[i..j];
        i = j;
        if space {
            pending = tok.to_vec();
            continue;
        }
        // Spaces only survive between words, or before the first word.
        if !wr.has_content && !at_start {
            pending.clear();
        }
        at_start = false;
        let ww = width_of(tok);
        let pw = width_of(&pending);
        if wr.cur_w + pw + ww <= width {
            for c in pending.drain(..) {
                wr.push(c);
            }
            for c in tok {
                wr.push(c.clone());
            }
            continue;
        }
        pending.clear();
        if wr.has_content {
            wr.flush();
        }
        if wr.cur_w + ww <= width {
            for c in tok {
                wr.push(c.clone());
            }
        } else {
            wr.hard_split(tok);
        }
    }
    if wr.has_content || wr.out.is_empty() {
        let cur = std::mem::take(&mut wr.cur);
        wr.out.push(build_line(cur, width));
    }
    wr.out
}

/// Verbatim hard wrap (code blocks): no word breaking, spaces preserved.
fn hard_wrap(cl: &[Cluster], width: usize) -> Vec<Line<'static>> {
    let mut wr = Wrapper {
        width,
        indent: Vec::new(),
        out: Vec::new(),
        cur: Vec::new(),
        cur_w: 0,
        has_content: false,
    };
    wr.hard_split(cl);
    if wr.has_content || wr.out.is_empty() {
        let cur = std::mem::take(&mut wr.cur);
        wr.out.push(build_line(cur, width));
    }
    wr.out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::style::{Color, Modifier};

    fn pal() -> Palette {
        Palette {
            text: Style::new().fg(Color::White),
            heading: Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            bold: Style::new().add_modifier(Modifier::BOLD),
            code: Style::new().fg(Color::Yellow),
            code_block: Style::new().fg(Color::Green),
            fence: Style::new().fg(Color::DarkGray),
            mention: Style::new().fg(Color::Magenta),
            quote: Style::new().fg(Color::Gray),
        }
    }

    fn texts(lines: &[Line<'_>]) -> Vec<String> {
        lines.iter().map(ToString::to_string).collect()
    }

    fn spans(lines: &[Line<'_>]) -> Vec<(String, Style)> {
        lines
            .iter()
            .flat_map(|l| l.spans.iter())
            .map(|s| (s.content.to_string(), s.style))
            .collect()
    }

    fn md(body: &str, w: u16) -> Vec<String> {
        texts(&markdown_lines(body, w, &pal()))
    }

    fn squash(s: &str) -> String {
        s.chars().filter(|c| !c.is_whitespace()).collect()
    }

    const NASTY: &[&str] = &[
        "",
        "plain words that go on for a while and then some more",
        "emoji 😀😀😀 family 👨‍👩‍👧 heart ❤️ flag 🇺🇸 done",
        "漢字かな交じり文の長い行は二列で数える漢字漢字漢字",
        "**",
        "** lone ** markers ` and `",
        "```rust\nfn main() { println!(\"漢字😀\"); }\nunclosed fence never ends",
        "- bullet with a fairly long body that must wrap\n  - nested 😀 bullet wrapping too",
        "12. numbered item with words\n> quoted text that keeps going on and on",
        "# Heading that is long enough to wrap\n###### tiny\n####### not heading",
        "@sol @notan: hello @x.y. ok",
        "tab\tseparated\r\nwindows\r\nline\u{7}bell",
        "e\u{301}e\u{301}e\u{301} combining",
    ];

    #[test]
    fn width_invariant_fuzz() {
        let long = "x".repeat(200);
        let long_cjk = "漢".repeat(200);
        let mut inputs: Vec<String> = NASTY.iter().map(ToString::to_string).collect();
        inputs.push(long.clone());
        inputs.push(format!("- {long}"));
        inputs.push(format!("> {long_cjk}"));
        inputs.push(format!("```\n{long_cjk}\n```"));
        inputs.push(NASTY.join("\n"));
        for input in &inputs {
            for w in [0u16, 1, 2, 3, 7, 20, 80] {
                let max = usize::from(w.max(1));
                for line in markdown_lines(input, w, &pal()) {
                    assert!(line.width() <= max, "md w={w} {line:?} from {input:?}");
                }
            }
        }
    }

    #[test]
    fn markdown_conserves_plain_text() {
        let long = "y".repeat(200);
        for input in [
            "the quick brown fox jumps over the lazy dog",
            "漢字かな交じり文の長い行",
            long.as_str(),
            "a b c d e f g h i j k l m n o p",
        ] {
            for w in [2u16, 3, 7, 20, 80] {
                let got: String = texts(&markdown_lines(input, w, &pal())).concat();
                assert_eq!(squash(&got), squash(input), "w={w}");
            }
        }
    }

    #[test]
    fn heading_style_and_markers() {
        let p = pal();
        let lines = markdown_lines("## Title here\n####### nope\n#nope", 80, &p);
        assert_eq!(spans(&lines[..1]), vec![("Title here".into(), p.heading)]);
        assert_eq!(texts(&lines[1..]), vec!["####### nope", "#nope"]);
        assert_eq!(lines[1].spans[0].style, p.text);
    }

    #[test]
    fn inline_bold_code_mention() {
        let p = pal();
        let lines = markdown_lines("hi **there** `x **y** @z` @sol. bye", 80, &p);
        assert_eq!(
            spans(&lines),
            vec![
                ("hi ".into(), p.text),
                ("there".into(), p.bold),
                (" ".into(), p.text),
                ("x **y** @z".into(), p.code),
                (" ".into(), p.text),
                ("@sol".into(), p.mention),
                (". bye".into(), p.text),
            ]
        );
    }

    #[test]
    fn mention_inside_bold_and_not_in_email() {
        let p = pal();
        let s = spans(&markdown_lines("**ask @atlas** mail a@b.c", 80, &p));
        assert!(s.contains(&("ask ".into(), p.bold)));
        assert!(s.contains(&("@atlas".into(), p.mention)));
        assert!(s.contains(&(" mail a@b.c".into(), p.text)));
    }

    #[test]
    fn unmatched_markers_are_literal() {
        let p = pal();
        let lines = markdown_lines("a ** b `c **", 80, &p);
        assert_eq!(spans(&lines), vec![("a ** b `c **".into(), p.text)]);
        assert_eq!(md("**", 80), vec!["**"]);
        assert_eq!(md("``", 80), vec!["``"]);
    }

    #[test]
    fn styles_survive_wrap() {
        let p = pal();
        let lines = markdown_lines("**bold words that wrap**", 10, &p);
        assert!(lines.len() > 1);
        for l in &lines {
            assert!(l.spans.iter().all(|s| s.style == p.bold), "{l:?}");
        }
    }

    #[test]
    fn fence_verbatim() {
        let p = pal();
        let body = "```rust\nfn  main() {}   \n    indented **no** @x\n```\nafter";
        let lines = markdown_lines(body, 80, &p);
        assert_eq!(
            texts(&lines),
            vec![
                "┄┄ rust",
                "fn  main() {}",
                "    indented **no** @x",
                "┄┄",
                "after"
            ]
        );
        assert_eq!(lines[0].spans[0].style, p.fence);
        assert_eq!(spans(&lines[1..3]).len(), 2);
        assert!(spans(&lines[1..3]).iter().all(|(_, s)| *s == p.code_block));
        assert_eq!(lines[3].spans[0].style, p.fence);
        assert_eq!(lines[4].spans[0].style, p.text);
    }

    #[test]
    fn fence_hard_wraps_and_keeps_blanks() {
        assert_eq!(
            md("```\nabcdefghij\n\n\n```", 5),
            vec!["┄┄", "abcde", "fghij", "", "", "┄┄"]
        );
        // Verbatim: spaces are content, not break points.
        assert_eq!(
            md("```\nabcde fghij\n```", 5),
            vec!["┄┄", "abcde", " fghi", "j", "┄┄"]
        );
        assert_eq!(
            md("```\nab cd ef\n```", 4),
            vec!["┄┄", "ab c", "d ef", "┄┄"]
        );
    }

    #[test]
    fn unclosed_fence_runs_to_end() {
        let p = pal();
        let lines = markdown_lines("```\n# not heading\n\n- no bullet", 80, &p);
        assert_eq!(
            texts(&lines),
            vec!["┄┄", "# not heading", "", "- no bullet"]
        );
        assert_eq!(lines[1].spans[0].style, p.code_block);
        assert_eq!(lines[3].spans[0].style, p.code_block);
    }

    #[test]
    fn bullet_hanging_indent() {
        assert_eq!(
            md("- one two three four", 10),
            vec!["• one two", "  three", "  four"]
        );
        assert_eq!(md("* star\n+ plus", 80), vec!["• star", "• plus"]);
        assert_eq!(
            md("  * deep words here wrap", 12),
            vec!["  • deep", "    words", "    here", "    wrap"]
        );
        assert_eq!(
            md("10. alpha beta gamma", 10),
            vec!["10. alpha", "    beta", "    gamma"]
        );
        assert_eq!(md("**bold** start", 80), vec!["bold start"]);
    }

    #[test]
    fn quote_prefix_on_every_row() {
        let p = pal();
        let lines = markdown_lines("> quoted words wrap here", 10, &p);
        assert_eq!(
            texts(&lines),
            vec!["▎ quoted", "▎ words", "▎ wrap", "▎ here"]
        );
        assert!(spans(&lines).iter().all(|(_, s)| *s == p.quote));
    }

    #[test]
    fn blank_lines_collapse() {
        assert_eq!(md("a\n\n\n\nb", 80), vec!["a", "", "b"]);
        assert_eq!(md("a\n   \n\t\nb\n", 80), vec!["a", "", "b"]);
        assert_eq!(md("a\n\n", 80), vec!["a"]);
    }

    #[test]
    fn long_word_and_wide_chars() {
        let long = "x".repeat(200);
        let lines = md(&long, 7);
        assert_eq!(lines.len(), 29);
        assert!(lines[..28].iter().all(|l| l.len() == 7));
        assert_eq!(md("漢字漢字漢字", 5), vec!["漢字", "漢字", "漢字"]);
        assert_eq!(md("ab 漢字漢字", 5), vec!["ab", "漢字", "漢字"]);
        // A double-width glyph cannot fit in one column.
        assert_eq!(md("漢", 1), vec!["?"]);
    }

    #[test]
    fn sanitizes_and_trims() {
        assert_eq!(md("a\tb\r\nc\u{7}d   ", 80), vec!["a    b", "cd"]);
        assert_eq!(md("   lead kept", 80), vec!["   lead kept"]);
        assert_eq!(md("one two   ", 7), vec!["one two"]);
    }

    #[test]
    fn display_width_counts_columns() {
        assert_eq!(display_width("ab"), 2);
        assert_eq!(display_width("漢字"), 4);
        assert_eq!(display_width("😀"), 2);
        assert_eq!(display_width(""), 0);
    }
}
