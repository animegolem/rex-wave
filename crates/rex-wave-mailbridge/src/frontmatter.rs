//! Lossless YAML-subset frontmatter parser, formatter, and header extractors
//! shared across sandbox mail promotion, shadow scanning, and future dissolved
//! `.md` export/import.

/// One recipient token extracted from `to:`, `cc:`, or `from:`/`author:`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecipientCandidate {
    pub raw: String,
    pub require_known_alias: bool,
}

/// Ordered key-value frontmatter entry paired with its unquoted value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontmatterEntry {
    pub key: String,
    pub raw_value: String,
    pub value: String,
}

/// Lossless split of a Markdown document into optional `---` frontmatter and body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontmatterDoc {
    pub has_frontmatter: bool,
    pub crlf: bool,
    pub open_newline: &'static str,
    pub close_fence: String,
    pub close_newline: &'static str,
    pub entries: Vec<FrontmatterEntry>,
    pub raw_frontmatter: String,
    pub body: String,
}

impl Default for FrontmatterDoc {
    fn default() -> Self {
        Self {
            has_frontmatter: false,
            crlf: false,
            open_newline: "\n",
            close_fence: "---".to_owned(),
            close_newline: "\n",
            entries: Vec::new(),
            raw_frontmatter: String::new(),
            body: String::new(),
        }
    }
}

impl FrontmatterDoc {
    /// Splits `text` at a leading `---` YAML frontmatter fence when terminated by
    /// a matching `---` line (allowing trailing spaces/tabs on the closing fence),
    /// preserving exact `open_newline`, `raw_frontmatter`, `close_fence`,
    /// `close_newline` (including EOF with no newline), and `body` so round-trip
    /// reconstruction via [`Self::render_lossless`] is 100% byte-preserving.
    pub fn parse(text: &str) -> Self {
        let (after_open, crlf, open_newline) = if let Some(r) = text.strip_prefix("---\r\n") {
            (r, true, "\r\n")
        } else if let Some(r) = text.strip_prefix("---\n") {
            (r, false, "\n")
        } else {
            let is_crlf = text.contains("\r\n");
            return FrontmatterDoc {
                has_frontmatter: false,
                crlf: is_crlf,
                open_newline: if is_crlf { "\r\n" } else { "\n" },
                close_fence: "---".to_owned(),
                close_newline: if is_crlf { "\r\n" } else { "\n" },
                entries: Vec::new(),
                raw_frontmatter: String::new(),
                body: text.to_owned(),
            };
        };

        let mut cursor = 0usize;
        let mut entries = Vec::new();
        let bytes = after_open.as_bytes();
        while cursor <= bytes.len() {
            let line_end = after_open[cursor..]
                .find('\n')
                .map(|off| cursor + off)
                .unwrap_or(bytes.len());
            let next_cursor = if line_end < bytes.len() {
                line_end + 1
            } else {
                bytes.len() + 1
            };
            let (line_body, line_nl) = if line_end == bytes.len() {
                (&after_open[cursor..line_end], "")
            } else if line_end > cursor && bytes[line_end - 1] == b'\r' {
                (&after_open[cursor..line_end - 1], "\r\n")
            } else {
                (&after_open[cursor..line_end], "\n")
            };
            if line_body.trim_end_matches([' ', '\t', '\r']) == "---" {
                let raw_fm = after_open[..cursor].to_owned();
                let body = if line_end < bytes.len() {
                    after_open[line_end + 1..].to_owned()
                } else {
                    String::new()
                };
                return FrontmatterDoc {
                    has_frontmatter: true,
                    crlf,
                    open_newline,
                    close_fence: line_body.to_owned(),
                    close_newline: line_nl,
                    entries,
                    raw_frontmatter: raw_fm,
                    body,
                };
            }
            let line_no_cr = line_body.trim_end_matches('\r');
            if let Some((k, v)) = line_no_cr.split_once(':') {
                let key = k.trim().to_owned();
                if !key.is_empty() {
                    let raw_value = v.trim().to_owned();
                    let value = unquote_yaml(&raw_value);
                    entries.push(FrontmatterEntry {
                        key,
                        raw_value,
                        value,
                    });
                }
            }
            if next_cursor > bytes.len() {
                break;
            }
            cursor = next_cursor;
        }

        FrontmatterDoc {
            has_frontmatter: false,
            crlf,
            open_newline,
            close_fence: "---".to_owned(),
            close_newline: open_newline,
            entries: Vec::new(),
            raw_frontmatter: String::new(),
            body: text.to_owned(),
        }
    }

    /// Lenient header scan matching historical drop-folder behavior: returns
    /// closed frontmatter entries when present, or scans header lines to EOF
    /// when a leading `---` block is unterminated.
    pub fn parse_headers_lenient(text: &str) -> Vec<FrontmatterEntry> {
        let doc = Self::parse(text);
        if doc.has_frontmatter {
            return doc.entries;
        }
        let Some(after_open) = text
            .strip_prefix("---\r\n")
            .or_else(|| text.strip_prefix("---\n"))
        else {
            return Vec::new();
        };
        let mut entries = Vec::new();
        for raw_line in after_open.lines() {
            let line = raw_line.trim_end_matches('\r');
            if let Some((k, v)) = line.split_once(':') {
                let key = k.trim().to_owned();
                if !key.is_empty() {
                    let raw_value = v.trim().to_owned();
                    let value = unquote_yaml(&raw_value);
                    entries.push(FrontmatterEntry {
                        key,
                        raw_value,
                        value,
                    });
                }
            }
        }
        entries
    }

    /// Reconstructs the exact original document from `open_newline`,
    /// `raw_frontmatter`, `close_fence`, `close_newline`, and `body` when parsed
    /// from source, or renders structured `entries` deterministically.
    pub fn render_lossless(&self) -> String {
        if !self.has_frontmatter {
            return self.body.clone();
        }
        if !self.raw_frontmatter.is_empty() || self.entries.is_empty() {
            let mut out = String::with_capacity(
                3 + self.open_newline.len()
                    + self.raw_frontmatter.len()
                    + self.close_fence.len()
                    + self.close_newline.len()
                    + self.body.len(),
            );
            out.push_str("---");
            out.push_str(self.open_newline);
            out.push_str(&self.raw_frontmatter);
            out.push_str(&self.close_fence);
            out.push_str(self.close_newline);
            out.push_str(&self.body);
            return out;
        }
        self.render_canonical()
    }

    /// Renders `entries` + `body` using canonical `---` fences and deterministic quoting.
    pub fn render_canonical(&self) -> String {
        if !self.has_frontmatter {
            return self.body.clone();
        }
        let nl = if self.crlf { "\r\n" } else { "\n" };
        let mut out = String::new();
        out.push_str("---");
        out.push_str(nl);
        for entry in &self.entries {
            out.push_str(&entry.key);
            out.push_str(": ");
            if !entry.raw_value.is_empty() {
                out.push_str(&entry.raw_value);
            } else {
                out.push_str(&yaml_quote_if_needed(&entry.value));
            }
            out.push_str(nl);
        }
        out.push_str("---");
        out.push_str(nl);
        out.push_str(&self.body);
        out
    }

    /// Case-insensitive lookup returning the last non-empty value for `key`.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .rev()
            .find(|e| e.key.eq_ignore_ascii_case(key) && !e.value.is_empty())
            .map(|e| e.value.as_str())
    }
}

/// Quotes a string as a single-line YAML double-quoted scalar, escaping `\`, `"`,
/// `\n`, `\r`, and `\t`, while stripping other control characters.
pub fn yaml_quote(raw: &str) -> String {
    let mut s = String::with_capacity(raw.len() + 2);
    s.push('"');
    for ch in raw.chars() {
        match ch {
            '\\' => s.push_str("\\\\"),
            '"' => s.push_str("\\\""),
            '\n' => s.push_str("\\n"),
            '\r' => s.push_str("\\r"),
            '\t' => s.push_str("\\t"),
            c if c.is_control() => {}
            c => s.push(c),
        }
    }
    s.push('"');
    s
}

/// Quotes a single-line header scalar where newlines flatten to spaces (used for
/// human-facing `subject:` and `room:` headers in promoted letters).
pub fn yaml_quote_inline(raw: &str) -> String {
    let mut s = String::with_capacity(raw.len() + 2);
    s.push('"');
    for ch in raw.chars() {
        match ch {
            '\\' => s.push_str("\\\\"),
            '"' => s.push_str("\\\""),
            '\n' | '\r' => s.push(' '),
            c if c.is_control() => {}
            c => s.push(c),
        }
    }
    s.push('"');
    s
}

pub fn yaml_quote_if_needed(val: &str) -> String {
    let needs_quote = val.is_empty()
        || val.trim() != val
        || val.contains([
            ':', '#', '"', '\'', '\\', '[', ']', '{', '}', ',', '\n', '\r', '\t',
        ]);
    if needs_quote {
        yaml_quote(val)
    } else {
        val.to_owned()
    }
}

/// Unquotes a YAML scalar (supporting double-quoted `\"`, `\\`, `\n`, `\r`, `\t`
/// escapes and single-quoted `''` escapes).
pub fn unquote_yaml(val: &str) -> String {
    let trimmed = val.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        let inner = &trimmed[1..trimmed.len() - 1];
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                if let Some(next) = chars.next() {
                    match next {
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        other => out.push(other),
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    } else if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        let inner = &trimmed[1..trimmed.len() - 1];
        inner.replace("''", "'")
    } else {
        trimmed.to_owned()
    }
}

fn strip_recipient_prefix(raw: &str) -> &str {
    let s = raw.trim();
    for prefix in ["cc:", "to:", "cc ", "to ", "cc\t", "to\t"] {
        if s.len() >= prefix.len()
            && s.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
        {
            return s[prefix.len()..].trim();
        }
    }
    s
}

/// Parses a `to:` / `cc:` / `from:` scalar or inline `[a, b]` list into
/// [`RecipientCandidate`]s, flagging `/`-split and parenthetical tokens as
/// alias-only so embedded paths never materialize bogus seat names.
pub fn parse_recipient_list(val: &str) -> Vec<RecipientCandidate> {
    let trimmed = val.trim().trim_start_matches('[').trim_end_matches(']');
    let mut out = Vec::new();
    for raw_unit in trimmed.split([',', ';']) {
        let unit = unquote_yaml(raw_unit);
        let unit = unit.trim_matches(['"', '\'']).trim();
        if unit.is_empty() {
            continue;
        }
        let (head_raw, paren_raw) = match unit.split_once('(') {
            Some((h, rest)) => {
                let inner = rest.split_once(')').map_or(rest, |(inside, _)| inside);
                (h, Some(inner))
            }
            None => (unit, None),
        };
        let head_unquoted = unquote_yaml(head_raw);
        let head_trimmed = head_unquoted.trim_matches(['"', '\'']).trim();
        let head = strip_recipient_prefix(head_trimmed);
        if head.contains('/') {
            for seg in head.split('/') {
                let seg_unquoted = unquote_yaml(seg);
                let seg_clean =
                    strip_recipient_prefix(seg_unquoted.trim_matches(['"', '\'']).trim());
                if !seg_clean.is_empty() {
                    out.push(RecipientCandidate {
                        raw: seg_clean.to_owned(),
                        require_known_alias: true,
                    });
                }
            }
        } else if !head.is_empty() {
            out.push(RecipientCandidate {
                raw: head.to_owned(),
                require_known_alias: false,
            });
        }
        if let Some(p_raw) = paren_raw {
            for seg in p_raw.split('/') {
                let inner_unquoted = unquote_yaml(seg);
                let inner = inner_unquoted.trim_matches(['"', '\'']).trim();
                if !inner.is_empty() {
                    out.push(RecipientCandidate {
                        raw: inner.to_owned(),
                        require_known_alias: true,
                    });
                }
            }
        }
    }
    out
}

pub fn is_receipt_only_status(val: &str) -> bool {
    let lower = val.to_ascii_lowercase();
    lower.contains("receipt only")
        || lower.contains("receipt-only")
        || lower.contains("no return ack")
        || lower.contains("no ack requested")
        || lower.contains("no-ack")
}

pub fn is_receipt_only_entry(key_lower: &str, val: &str) -> bool {
    match key_lower {
        "receipt_only" | "receipt-only" | "no_ack" | "no-ack" => {
            matches!(val.to_ascii_lowercase().as_str(), "true" | "yes" | "1")
        }
        "ack_required" | "return_ack" => {
            matches!(
                val.to_ascii_lowercase().as_str(),
                "false" | "no" | "0" | "none"
            )
        }
        "ack" => matches!(
            val.to_ascii_lowercase().as_str(),
            "none" | "false" | "no" | "receipt-only" | "receipt_only"
        ),
        "kind" => matches!(
            val.to_ascii_lowercase().as_str(),
            "receipt" | "receipt-only"
        ),
        "status" => is_receipt_only_status(val),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lossless_parse_and_render_preserves_unknown_keys_and_crlf() {
        let lf_doc = "---\nfrom: alice\nto: [bob, carol]\nsubject: \"hello: \\\"world\\\"\"\ncustom_header: keep-me\n---\n\n# Heading\nBody line.\n";
        let parsed = FrontmatterDoc::parse(lf_doc);
        assert!(parsed.has_frontmatter);
        assert!(!parsed.crlf);
        assert_eq!(parsed.get("from"), Some("alice"));
        assert_eq!(parsed.get("subject"), Some("hello: \"world\""));
        assert_eq!(parsed.get("custom_header"), Some("keep-me"));
        assert_eq!(parsed.body, "\n# Heading\nBody line.\n");
        assert_eq!(parsed.render_lossless(), lf_doc);

        let crlf_doc = "---\r\nfrom: 'o''malley'\r\nto: sam\r\n---\r\nBody\r\n";
        let parsed_crlf = FrontmatterDoc::parse(crlf_doc);
        assert!(parsed_crlf.has_frontmatter);
        assert!(parsed_crlf.crlf);
        assert_eq!(parsed_crlf.get("from"), Some("o'malley"));
        assert_eq!(parsed_crlf.render_lossless(), crlf_doc);

        let unclosed = "---\nfrom: alice\nto: bob\nno closing fence\n";
        let parsed_unclosed = FrontmatterDoc::parse(unclosed);
        assert!(!parsed_unclosed.has_frontmatter);
        assert_eq!(parsed_unclosed.body, unclosed);
        assert_eq!(parsed_unclosed.render_lossless(), unclosed);
        let lenient = FrontmatterDoc::parse_headers_lenient(unclosed);
        assert_eq!(lenient.len(), 2);
        assert_eq!(lenient[0].key, "from");
        assert_eq!(lenient[0].value, "alice");
        assert_eq!(lenient[1].key, "to");
        assert_eq!(lenient[1].value, "bob");

        let eof_close = "---\nfrom: alice\n---";
        let parsed_eof = FrontmatterDoc::parse(eof_close);
        assert!(parsed_eof.has_frontmatter);
        assert_eq!(parsed_eof.close_newline, "");
        assert_eq!(parsed_eof.render_lossless(), eof_close);

        let mixed_nl = "---\r\nfrom: alice\r\n---\nBody with LF\n";
        let parsed_mixed = FrontmatterDoc::parse(mixed_nl);
        assert!(parsed_mixed.has_frontmatter);
        assert_eq!(parsed_mixed.render_lossless(), mixed_nl);

        let spaced_close = "---\nfrom: alice\n---   \nBody\n";
        let parsed_spaced = FrontmatterDoc::parse(spaced_close);
        assert!(parsed_spaced.has_frontmatter);
        assert_eq!(parsed_spaced.get("from"), Some("alice"));
        assert_eq!(parsed_spaced.render_lossless(), spaced_close);

        for cr_edge in [
            "---\nfrom: alice\n---\r",
            "---\nfrom: alice\n---\r\r\nBody\n",
            "---\r\nfrom: alice\r\n---\r\r\r",
        ] {
            let doc = FrontmatterDoc::parse(cr_edge);
            assert!(doc.has_frontmatter);
            assert_eq!(doc.render_lossless(), cr_edge, "lost bytes on {cr_edge:?}");
        }
    }

    #[test]
    fn yaml_quote_and_unquote_roundtrip_special_scalars() {
        let samples = [
            "",
            "plain",
            " trailing space ",
            "colon: inside",
            "quote \" and backslash \\",
            "multi\nline\r\ntext\twith\ttabs",
            "[alice, bob]",
        ];
        for s in samples {
            let quoted = yaml_quote(s);
            assert_eq!(unquote_yaml(&quoted), s, "roundtrip failed for {s:?}");
            let maybe = yaml_quote_if_needed(s);
            assert_eq!(unquote_yaml(&maybe), s, "if_needed failed for {s:?}");
        }
    }
}
