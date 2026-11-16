//! Bidirectional Org-mode codec for Kanban boards and standalone `.org` files.

use crate::model::{
    Assignee, BoardSnapshot, Card, CardNo, CardRevision, ExternalLink, Lane, LaneName, Link, OrgPriority, TaskCard,
    Title,
};
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OrgError {
    #[error("malformed heading at line {line}: {reason}")]
    Heading { line: usize, reason: String },
    #[error("bad property at line {line}: {reason}")]
    Property { line: usize, reason: String },
}

/// Lossless Org-mode document AST preserving `#+TITLE:`, preamble directives,
/// task cards, `:PROPERTIES:` drawers (including unknown user keys), and body notes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrgDocument {
    pub title: Option<String>,
    pub preamble: Vec<String>,
    pub tasks: Vec<TaskCard>,
}

impl OrgDocument {
    pub fn from_snapshot(snapshot: &BoardSnapshot) -> Self {
        let mut tasks = Vec::new();
        for lane in &snapshot.lanes {
            for card in &lane.cards {
                tasks.push(TaskCard::from(card));
            }
        }
        Self {
            title: Some(snapshot.board.as_str().to_owned()),
            preamble: Vec::new(),
            tasks,
        }
    }

    pub fn to_cards(&self) -> Vec<Card> {
        let mut used: BTreeSet<u32> = self.tasks.iter().filter_map(|t| t.id.map(|c| c.0)).collect();
        let mut probe = 1u32;
        let mut cards = Vec::with_capacity(self.tasks.len());
        for task in &self.tasks {
            let fallback = if task.id.is_none() {
                while !used.insert(probe) {
                    probe = probe.checked_add(1).expect("card number space exhausted");
                }
                CardNo(probe)
            } else {
                CardNo(0)
            };
            cards.push(task.to_card(fallback));
        }
        cards
    }
}

pub fn render(snapshot: &BoardSnapshot) -> String {
    render_document(&OrgDocument::from_snapshot(snapshot))
}

pub fn parse(text: &str) -> Result<Vec<Card>, OrgError> {
    Ok(parse_document(text)?.to_cards())
}

pub fn render_document(doc: &OrgDocument) -> String {
    let mut out = String::new();
    if let Some(title) = &doc.title {
        if title.is_empty() {
            out.push_str("#+TITLE:\n");
        } else {
            let _ = writeln!(out, "#+TITLE: {title}");
        }
    }
    for line in &doc.preamble {
        let _ = writeln!(out, "{line}");
    }
    if (doc.title.is_some() || !doc.preamble.is_empty()) && !doc.tasks.is_empty() {
        out.push('\n');
    }

    for (idx, task) in doc.tasks.iter().enumerate() {
        out.push('*');
        out.push(' ');
        out.push_str(&lane_to_org_keyword(&task.lane));
        if let Some(prio) = task.priority {
            out.push(' ');
            out.push_str(prio.cookie());
        }
        out.push(' ');
        out.push_str(task.title.as_str());
        if !task.tags.is_empty() {
            out.push(' ');
            out.push(':');
            for tag in &task.tags {
                out.push_str(&encode_tag(tag));
                out.push(':');
            }
        }
        out.push('\n');

        let has_drawer = task.id.is_some()
            || task.revision.is_some()
            || task.assignee.is_some()
            || task.due_ms.is_some()
            || task.blocking
            || !task.links.is_empty()
            || !task.properties.is_empty();
        if has_drawer {
            out.push_str(":PROPERTIES:\n");
            if let Some(id) = task.id {
                let _ = writeln!(out, ":ID: {}", id.0);
            }
            if let Some(rev) = task.revision {
                let _ = writeln!(out, ":REV: {}", rev.0);
            }
            if let Some(who) = &task.assignee {
                let _ = writeln!(out, ":ASSIGNEE: {who}");
            }
            if let Some(due_ms) = task.due_ms {
                let _ = writeln!(out, ":DUE_MS: {due_ms}");
            }
            if task.blocking {
                out.push_str(":PRIORITY: blocking\n");
            }
            if !task.links.is_empty() {
                out.push_str(":LINKS: ");
                for (link_idx, link) in task.links.iter().enumerate() {
                    if link_idx > 0 {
                        out.push(' ');
                    }
                    out.push_str(&encode_link_token(link));
                }
                out.push('\n');
            }
            for (key, value) in &task.properties {
                if value.is_empty() {
                    let _ = writeln!(out, ":{key}:");
                } else {
                    let _ = writeln!(out, ":{key}: {value}");
                }
            }
            out.push_str(":END:\n");
        }

        let body = task.body.trim_end_matches('\n');
        if !body.is_empty() {
            out.push_str(body);
            out.push('\n');
        }
        if idx + 1 < doc.tasks.len() {
            out.push('\n');
        }
    }
    out
}

pub fn parse_document(text: &str) -> Result<OrgDocument, OrgError> {
    if text.is_empty() {
        return Ok(OrgDocument::default());
    }
    let raw_lines: Vec<&str> = split_lines(text);
    let mut title: Option<String> = None;
    let mut preamble: Vec<String> = Vec::new();
    let mut idx = 0;

    while idx < raw_lines.len() && !is_top_heading(raw_lines[idx]) {
        let line = raw_lines[idx];
        if title.is_none()
            && let Some(rest) = line.strip_prefix("#+TITLE:").or_else(|| line.strip_prefix("#+title:"))
        {
            let value = rest.strip_prefix(' ').unwrap_or(rest).to_owned();
            title = Some(value);
        } else {
            preamble.push(line.to_owned());
        }
        idx += 1;
    }

    if idx < raw_lines.len()
        && (title.is_some() || !preamble.is_empty())
        && preamble.last().is_some_and(String::is_empty)
    {
        preamble.pop();
    }

    let mut tasks = Vec::new();
    let mut seen_ids = BTreeSet::new();

    while idx < raw_lines.len() {
        let heading_line_no = idx + 1;
        let (lane, priority, heading_title, tags) = parse_heading(raw_lines[idx], heading_line_no)?;
        idx += 1;

        let mut id: Option<CardNo> = None;
        let mut revision: Option<CardRevision> = None;
        let mut assignee: Option<Assignee> = None;
        let mut due_ms: Option<u64> = None;
        let mut blocking = false;
        let mut links: Vec<Link> = Vec::new();
        let mut properties: Vec<(String, String)> = Vec::new();

        if idx < raw_lines.len() && raw_lines[idx].trim() == ":PROPERTIES:" {
            let drawer_open_line = idx + 1;
            idx += 1;
            let mut closed = false;
            while idx < raw_lines.len() {
                let prop_line_no = idx + 1;
                let line = raw_lines[idx];
                if is_top_heading(line) {
                    return Err(OrgError::Property {
                        line: drawer_open_line,
                        reason: "unclosed :PROPERTIES: drawer".into(),
                    });
                }
                let trimmed = line.trim();
                if trimmed == ":END:" {
                    closed = true;
                    idx += 1;
                    break;
                }
                let (key, val) = parse_property_line(trimmed, prop_line_no)?;
                match key {
                    "ID" | "CARD" => {
                        let parsed = val.parse::<u32>().map_err(|_| OrgError::Property {
                            line: prop_line_no,
                            reason: format!("invalid :{key}: `{val}`"),
                        })?;
                        let card_no = CardNo(parsed);
                        if !seen_ids.insert(card_no) {
                            return Err(OrgError::Property {
                                line: prop_line_no,
                                reason: format!("duplicate :{key}: {parsed}"),
                            });
                        }
                        id = Some(card_no);
                    }
                    "REV" | "REVISION" => {
                        let parsed = val.parse::<u64>().map_err(|_| OrgError::Property {
                            line: prop_line_no,
                            reason: format!("invalid :{key}: `{val}`"),
                        })?;
                        revision = Some(CardRevision(parsed));
                    }
                    "ASSIGNEE" => {
                        let parsed = Assignee::parse(val).map_err(|e| OrgError::Property {
                            line: prop_line_no,
                            reason: e.to_string(),
                        })?;
                        assignee = Some(parsed);
                    }
                    "DUE_MS" => {
                        let parsed = val.parse::<u64>().map_err(|_| OrgError::Property {
                            line: prop_line_no,
                            reason: format!("invalid :DUE_MS: `{val}`"),
                        })?;
                        due_ms = Some(parsed);
                    }
                    "PRIORITY" if val == "blocking" => {
                        blocking = true;
                    }
                    "LINKS" => {
                        for tok in val.split_ascii_whitespace() {
                            links.push(decode_link_token(tok, prop_line_no)?);
                        }
                    }
                    _ => {
                        properties.push((key.to_owned(), val.to_owned()));
                    }
                }
                idx += 1;
            }
            if !closed {
                return Err(OrgError::Property {
                    line: drawer_open_line,
                    reason: "unclosed :PROPERTIES: drawer".into(),
                });
            }
        }

        let body_start = idx;
        while idx < raw_lines.len() && !is_top_heading(raw_lines[idx]) {
            idx += 1;
        }
        let mut body_lines = raw_lines[body_start..idx].to_vec();
        if idx < raw_lines.len() && body_lines.last() == Some(&"") {
            body_lines.pop();
        }
        let body = body_lines.join("\n");

        tasks.push(TaskCard {
            id,
            revision,
            lane,
            priority,
            blocking,
            title: heading_title,
            tags,
            assignee,
            due_ms,
            links,
            properties,
            body,
        });
    }

    Ok(OrgDocument { title, preamble, tasks })
}

fn split_lines(text: &str) -> Vec<&str> {
    let trimmed = text.strip_suffix('\n').unwrap_or(text);
    if trimmed.is_empty() && text == "\n" {
        return vec![""];
    }
    trimmed.split('\n').map(|l| l.strip_suffix('\r').unwrap_or(l)).collect()
}

fn is_top_heading(line: &str) -> bool {
    line == "*" || line.starts_with("* ") || line.starts_with("*\t")
}

fn lane_to_org_keyword(lane: &LaneName) -> String {
    if let Some(canonical) = Lane::from_lane_name(lane) {
        return canonical.org_keyword().to_owned();
    }
    match lane.as_str() {
        "strt" | "wait" | "blkd" => format!("CUSTOM_{}", lane.as_str().to_ascii_uppercase()),
        other => other.to_ascii_uppercase(),
    }
}

fn lane_from_org_keyword(keyword: &str, line: usize) -> Result<LaneName, OrgError> {
    if let Some(canonical) = Lane::from_org_keyword(keyword) {
        return Ok(canonical.lane_name());
    }
    match keyword {
        "CUSTOM_STRT" => return Ok(LaneName::parse("strt").expect("static lane")),
        "CUSTOM_WAIT" => return Ok(LaneName::parse("wait").expect("static lane")),
        "CUSTOM_BLKD" => return Ok(LaneName::parse("blkd").expect("static lane")),
        _ => {}
    }
    if keyword.is_empty()
        || !keyword
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        return Err(OrgError::Heading {
            line,
            reason: format!("invalid lane keyword `{keyword}`"),
        });
    }
    LaneName::parse(keyword.to_ascii_lowercase()).map_err(|e| OrgError::Heading {
        line,
        reason: e.to_string(),
    })
}

fn parse_heading(
    line: &str,
    line_no: usize,
) -> Result<(LaneName, Option<OrgPriority>, Title, Vec<String>), OrgError> {
    let after_star = line
        .strip_prefix('*')
        .ok_or_else(|| OrgError::Heading { line: line_no, reason: "missing leading `*`".into() })?
        .trim_start_matches([' ', '\t']);
    if after_star.is_empty() {
        return Err(OrgError::Heading {
            line: line_no,
            reason: "missing lane keyword and title".into(),
        });
    }

    let Some((keyword, rest)) = split_first_ws(after_star) else {
        let _ = lane_from_org_keyword(after_star, line_no)?;
        return Err(OrgError::Heading { line: line_no, reason: "missing title".into() });
    };
    let lane = lane_from_org_keyword(keyword, line_no)?;
    let mut rest = rest.trim_start_matches([' ', '\t']);

    let priority = if rest.starts_with("[#") {
        let (cookie_tok, after_cookie) = split_first_ws(rest).unwrap_or((rest, ""));
        let prio = OrgPriority::parse_cookie(cookie_tok).ok_or_else(|| OrgError::Heading {
            line: line_no,
            reason: format!("invalid priority cookie `{cookie_tok}`"),
        })?;
        rest = after_cookie.trim_start_matches([' ', '\t']);
        Some(prio)
    } else {
        None
    };

    if rest.is_empty() || is_tag_block(rest) {
        return Err(OrgError::Heading { line: line_no, reason: "missing title".into() });
    }

    let (raw_title, tags) = split_trailing_tags(rest);
    let parsed_title = Title::parse(raw_title).map_err(|e| OrgError::Heading {
        line: line_no,
        reason: e.to_string(),
    })?;
    Ok((lane, priority, parsed_title, tags))
}

fn split_first_ws(s: &str) -> Option<(&str, &str)> {
    let pos = s.find([' ', '\t'])?;
    Some((&s[..pos], &s[pos + 1..]))
}

fn is_valid_org_tag_segment(seg: &str) -> bool {
    !seg.is_empty()
        && seg
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '_' | '@' | '#' | '%' | '-'))
}

fn is_tag_block(s: &str) -> bool {
    let Some(inner) = s.strip_prefix(':').and_then(|x| x.strip_suffix(':')) else {
        return false;
    };
    !inner.is_empty() && inner.split(':').all(is_valid_org_tag_segment)
}

fn split_trailing_tags(rest: &str) -> (&str, Vec<String>) {
    if !rest.ends_with(':') {
        return (rest.trim_end_matches([' ', '\t']), Vec::new());
    }
    for (byte_idx, ch) in rest.char_indices().rev() {
        if matches!(ch, ' ' | '\t') {
            let candidate = &rest[byte_idx + ch.len_utf8()..];
            if is_tag_block(candidate) {
                let inner = &candidate[1..candidate.len() - 1];
                let tags = inner.split(':').map(decode_tag).collect();
                let title_part = rest[..byte_idx].trim_end_matches([' ', '\t']);
                if !title_part.is_empty() {
                    return (title_part, tags);
                }
            }
        }
    }
    (rest.trim_end_matches([' ', '\t']), Vec::new())
}

fn encode_tag(tag: &str) -> String {
    let needs_escape = tag.is_empty()
        || tag.contains('%')
        || !tag
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '_' | '@' | '#' | '-'));
    if !needs_escape {
        return tag.to_owned();
    }
    let mut out = String::new();
    let mut utf8_buf = [0u8; 4];
    for ch in tag.chars() {
        if ch.is_alphanumeric() || matches!(ch, '_' | '@' | '#' | '-') {
            out.push(ch);
        } else {
            for &b in ch.encode_utf8(&mut utf8_buf).as_bytes() {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

fn decode_tag(seg: &str) -> String {
    if !seg.contains('%') {
        return seg.to_owned();
    }
    percent_decode_lossy(seg)
}

fn parse_property_line(trimmed: &str, line: usize) -> Result<(&str, &str), OrgError> {
    let Some(after_open) = trimmed.strip_prefix(':') else {
        return Err(OrgError::Property {
            line,
            reason: "property must start with `:`".into(),
        });
    };
    let Some((key, after_close)) = after_open.split_once(':') else {
        return Err(OrgError::Property {
            line,
            reason: "property missing closing `:`".into(),
        });
    };
    if key.is_empty() || key.bytes().any(|b| b.is_ascii_whitespace() || b.is_ascii_control()) {
        return Err(OrgError::Property {
            line,
            reason: format!("invalid property key `{key}`"),
        });
    }
    let val = after_close.strip_prefix(' ').unwrap_or(after_close).trim_end();
    Ok((key, val))
}

const ORG_LINK_PREFIX: &str = "org+heading:";

fn encode_link_token(link: &Link) -> String {
    match link {
        Link::External(ext) => {
            let ns = if ext.namespace == "org+heading" {
                "org\\x2Bheading".to_owned()
            } else {
                backslash_escape(&ext.namespace, |b| b <= b' ' || b == 0x7f || b == b':')
            };
            let key = backslash_escape(&ext.key, |b| b <= b' ' || b == 0x7f);
            format!("{ns}:{key}")
        }
        Link::Org { file, outline } => {
            let encoded_file = backslash_escape(&file.to_string_lossy(), |b| {
                b <= b' ' || b == 0x7f || b == b':'
            });
            if outline.is_empty() {
                format!("{ORG_LINK_PREFIX}{encoded_file}")
            } else {
                let joined = outline
                    .iter()
                    .map(|seg| backslash_escape(seg, |b| b <= b' ' || b == 0x7f || b == b'/'))
                    .collect::<Vec<_>>()
                    .join("/");
                format!("{ORG_LINK_PREFIX}{encoded_file}::{joined}")
            }
        }
    }
}

fn decode_link_token(token: &str, line: usize) -> Result<Link, OrgError> {
    if let Some(rest) = token.strip_prefix(ORG_LINK_PREFIX) {
        let (file_raw, outline) = match rest.split_once("::") {
            Some((f, o)) => {
                let segments = o
                    .split('/')
                    .map(|seg| backslash_unescape(seg, line))
                    .collect::<Result<Vec<_>, _>>()?;
                (f, segments)
            }
            None => (rest, Vec::new()),
        };
        let decoded_file = backslash_unescape(file_raw, line)?;
        if decoded_file.is_empty() {
            return Err(OrgError::Property {
                line,
                reason: "empty org link file path".into(),
            });
        }
        return Ok(Link::Org { file: PathBuf::from(decoded_file), outline });
    }

    let Some((raw_ns, raw_key)) = split_unescaped_colon(token) else {
        return Err(OrgError::Property {
            line,
            reason: format!("malformed link `{token}` (expected scheme:target)"),
        });
    };
    let namespace = backslash_unescape(raw_ns, line)?;
    let key = backslash_unescape(raw_key, line)?;
    if namespace.is_empty() || key.is_empty() || key == "//" {
        return Err(OrgError::Property {
            line,
            reason: format!("empty scheme or target in link `{token}`"),
        });
    }
    let ext = ExternalLink::new(namespace, key).map_err(|e| OrgError::Property {
        line,
        reason: e.to_string(),
    })?;
    Ok(Link::External(ext))
}

fn split_unescaped_colon(s: &str) -> Option<(&str, &str)> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            i += 2;
            continue;
        }
        if bytes[i] == b':' {
            return Some((&s[..i], &s[i + 1..]));
        }
        i += 1;
    }
    None
}

fn backslash_escape(input: &str, should_escape: impl Fn(u8) -> bool) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch == '\\' {
            out.push_str("\\\\");
        } else if ch.is_ascii() && should_escape(ch as u8) {
            let _ = write!(out, "\\x{:02X}", ch as u8);
        } else {
            out.push(ch);
        }
    }
    out
}

fn backslash_unescape(input: &str, line: usize) -> Result<String, OrgError> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            if i + 1 >= bytes.len() {
                return Err(OrgError::Property {
                    line,
                    reason: format!("dangling escape in `{input}`"),
                });
            }
            if bytes[i + 1] == b'\\' {
                out.push(b'\\');
                i += 2;
                continue;
            }
            if bytes[i + 1] == b'x' {
                if i + 3 >= bytes.len() {
                    return Err(OrgError::Property {
                        line,
                        reason: format!("truncated hex escape in `{input}`"),
                    });
                }
                let hi = hex_val(bytes[i + 2]).ok_or_else(|| OrgError::Property {
                    line,
                    reason: format!("invalid hex escape in `{input}`"),
                })?;
                let lo = hex_val(bytes[i + 3]).ok_or_else(|| OrgError::Property {
                    line,
                    reason: format!("invalid hex escape in `{input}`"),
                })?;
                out.push((hi << 4) | lo);
                i += 4;
                continue;
            }
            return Err(OrgError::Property {
                line,
                reason: format!("unknown escape in `{input}`"),
            });
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).map_err(|_| OrgError::Property {
        line,
        reason: format!("non-utf8 escape in `{input}`"),
    })
}

fn percent_decode_lossy(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            out.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| input.to_owned())
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
