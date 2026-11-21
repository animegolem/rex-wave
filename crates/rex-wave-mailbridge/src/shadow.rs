//! Structurally read-only interchange reader (`ReadOnlyInterchange`) and pure
//! mail-ring decision engine (`decide_mail_ring`, `decide_mail_rings_for_seat`,
//! `run_shadow_pass`). Zero write paths exist in this module.

use crate::acks::{AckIndex, AcksParseReport, AliasMap, parse_acks_jsonl};
use crate::presence::{
    IncarnationAffinity, PresenceState, select_binding_for_delivery, sender_delivery_notice,
};
use crate::{SandboxError, sha256_bytes, unquote_yaml};
use rex_wave_api::presence::*;
use rex_wave_api::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

/// Source that produced a letter's canonical timestamp, ordered by precedence:
/// `LetterIdMs` > `RecordedDate` > `FileModified`.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum TimeBasis {
    LetterIdMs,
    RecordedDate,
    #[default]
    FileModified,
}

impl TimeBasis {
    pub fn as_str(self) -> &'static str {
        match self {
            TimeBasis::LetterIdMs => "letter_id_ms",
            TimeBasis::RecordedDate => "recorded_date",
            TimeBasis::FileModified => "file_modified",
        }
    }

    /// Maps to the v1 `mailbox.item.schema.json` `ts_basis` label used by `project()`.
    pub fn as_mailbox_ts_basis(self) -> &'static str {
        match self {
            TimeBasis::LetterIdMs => "letter_id_ms",
            TimeBasis::RecordedDate => "recorded",
            TimeBasis::FileModified => "mtime",
        }
    }
}

const MIN_EPOCH_MS: u64 = 1_000_000_000_000;
const MAX_EPOCH_MS: u64 = 4_102_444_800_000;

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn civil_to_unix_secs(
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> Option<i64> {
    if !(1970..=2200).contains(&year)
        || !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let m = month as u64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + (day as u64) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe as i64 - 719_468;
    Some(days * 86_400 + (hour as i64) * 3_600 + (minute as i64) * 60 + (second as i64))
}

fn parse_epoch_ms_13(raw: &str, require_dash_suffix: bool) -> Option<UnixMs> {
    let trimmed = raw.trim();
    let b = trimmed.as_bytes();
    if require_dash_suffix {
        if b.len() < 14 || b[13] != b'-' || !b[..13].iter().all(u8::is_ascii_digit) {
            return None;
        }
    } else if b.len() == 13 {
        if !b.iter().all(u8::is_ascii_digit) {
            return None;
        }
    } else if b.len() > 13 && b[13] == b'-' {
        if !b[..13].iter().all(u8::is_ascii_digit) {
            return None;
        }
    } else {
        return None;
    }
    let ms = trimmed[..13].parse::<u64>().ok()?;
    if (MIN_EPOCH_MS..=MAX_EPOCH_MS).contains(&ms) {
        Some(UnixMs(ms))
    } else {
        None
    }
}

fn parse_tz_offset_secs(rest: &str) -> Option<i64> {
    let s = rest.trim_start();
    if s.is_empty() {
        return Some(0);
    }
    let b = s.as_bytes();
    if (b[0] == b'+' || b[0] == b'-') && b.len() >= 5 {
        let sign: i64 = if b[0] == b'+' { 1 } else { -1 };
        let (hh_str, mm_str) = if b.len() >= 6
            && b[1].is_ascii_digit()
            && b[2].is_ascii_digit()
            && b[3] == b':'
            && b[4].is_ascii_digit()
            && b[5].is_ascii_digit()
        {
            (&s[1..3], &s[4..6])
        } else if b[1..5].iter().all(u8::is_ascii_digit) {
            (&s[1..3], &s[3..5])
        } else {
            return Some(0);
        };
        let hh = hh_str.parse::<i64>().ok()?;
        let mm = mm_str.parse::<i64>().ok()?;
        if hh <= 23 && mm <= 59 {
            return Some(sign * (hh * 3_600 + mm * 60));
        }
        return None;
    }

    let token_end = s
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(s.len());
    let token = &s[..token_end];
    let offset = match token.to_ascii_uppercase().as_str() {
        "" | "Z" | "UTC" | "GMT" => 0,
        "CST" => -6 * 3_600,
        "CDT" => -5 * 3_600,
        "EST" => -5 * 3_600,
        "EDT" => -4 * 3_600,
        "MST" => -7 * 3_600,
        "MDT" => -6 * 3_600,
        "PST" => -8 * 3_600,
        "PDT" => -7 * 3_600,
        _ => 0,
    };
    Some(offset)
}

/// Latest possible UTC offset west of UTC (`UTC-12:00` + full 24h day = 36h)
/// used as the safe upper bound when comparing a date-only (`YYYY-MM-DD`)
/// letter against a seat's `mail_horizon_at` so letters written on the same
/// local calendar day as first bind are never silently dropped as history.
const DATE_ONLY_HORIZON_WINDOW_MS: u64 = 36 * 3_600_000;

/// Parsed letter timestamp paired with its provenance basis and whether only a
/// calendar date (`YYYY-MM-DD`, without an exact time-of-day) was available.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParsedLetterTime {
    pub ts: UnixMs,
    pub basis: TimeBasis,
    pub date_only: bool,
}

fn parse_recorded_datetime_detail(raw: &str) -> Option<(UnixMs, bool)> {
    let trimmed = raw.trim();
    let b = trimmed.as_bytes();
    if b.len() < 10
        || b[4] != b'-'
        || b[7] != b'-'
        || !b[0..4].iter().all(u8::is_ascii_digit)
        || !b[5..7].iter().all(u8::is_ascii_digit)
        || !b[8..10].iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    let year = trimmed[0..4].parse::<i64>().ok()?;
    let month = trimmed[5..7].parse::<u32>().ok()?;
    let day = trimmed[8..10].parse::<u32>().ok()?;

    let mut rest = &trimmed[10..];
    if let Some(after) = rest.strip_prefix('T').or_else(|| rest.strip_prefix('t')) {
        rest = after;
    } else {
        rest = rest.trim_start_matches(|c: char| c == ',' || c.is_ascii_whitespace());
    }

    let rb = rest.as_bytes();
    let (hour, minute, second, frac_ms, tz_offset, date_only) = if rb.len() >= 5
        && rb[0].is_ascii_digit()
        && rb[1].is_ascii_digit()
        && rb[2] == b':'
        && rb[3].is_ascii_digit()
        && rb[4].is_ascii_digit()
    {
        let hour = rest[0..2].parse::<u32>().ok()?;
        let minute = rest[3..5].parse::<u32>().ok()?;
        let mut cursor = &rest[5..];
        let mut second = 0u32;
        if cursor.len() >= 3
            && cursor.as_bytes()[0] == b':'
            && cursor.as_bytes()[1].is_ascii_digit()
            && cursor.as_bytes()[2].is_ascii_digit()
        {
            second = cursor[1..3].parse::<u32>().ok()?;
            cursor = &cursor[3..];
        }
        let mut frac_ms = 0u64;
        if let Some(after_dot) = cursor.strip_prefix('.') {
            let digits_end = after_dot
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(after_dot.len());
            let digits = &after_dot[..digits_end];
            if !digits.is_empty() {
                let mut padded = [b'0'; 3];
                for (i, ch) in digits.bytes().take(3).enumerate() {
                    padded[i] = ch;
                }
                frac_ms = std::str::from_utf8(&padded).ok()?.parse::<u64>().ok()?;
            }
            cursor = &after_dot[digits_end..];
        }
        let tz_offset = parse_tz_offset_secs(cursor)?;
        (hour, minute, second, frac_ms, tz_offset, false)
    } else {
        (0, 0, 0, 0, 0, true)
    };

    let local_secs = civil_to_unix_secs(year, month, day, hour, minute, second)?;
    let utc_secs = local_secs - tz_offset;
    if utc_secs < 0 {
        return None;
    }
    Some((
        UnixMs(
            (utc_secs as u64)
                .saturating_mul(1_000)
                .saturating_add(frac_ms),
        ),
        date_only,
    ))
}

/// Parses recorded letter timestamps (`YYYY-MM-DD`, ISO-8601, or
/// `YYYY-MM-DD[, ]+HH:MM(:SS)? (CST|CDT|UTC|Z|[+-]HH:?MM)?`).
pub fn parse_recorded_datetime_ms(raw: &str) -> Option<UnixMs> {
    parse_recorded_datetime_detail(raw).map(|(ts, _)| ts)
}

fn parse_filename_ymd_prefix(basename: &str) -> Option<UnixMs> {
    let b = basename.as_bytes();
    if b.len() >= 11 && b[10] == b'-' {
        parse_recorded_datetime_ms(&basename[..10])
    } else {
        None
    }
}

/// Resolves a letter's timestamp using the canonical precedence with precision
/// metadata and reply-parent detection:
/// 1. 13-digit millisecond epoch prefix on filename (`^(\d{13})-`) or numeric frontmatter `date_ms` / `id` ([`TimeBasis::LetterIdMs`]),
///    except when a reply filename/`id` reuses the parent letter's `in_reply_to` epoch prefix (in which case the reply's own `date_ms`/`date`/`ts` takes precedence),
/// 2. Recorded frontmatter `date` / `ts` or `YYYY-MM-DD-` filename prefix ([`TimeBasis::RecordedDate`]),
/// 3. Filesystem `metadata().modified()` last ([`TimeBasis::FileModified`]).
pub fn extract_letter_timestamp_detailed(
    rel_path: &str,
    id_field: Option<&str>,
    date_ms_field: Option<&str>,
    date_field: Option<&str>,
    ts_field: Option<&str>,
    in_reply_to: Option<&str>,
    mtime_ms: Option<UnixMs>,
) -> ParsedLetterTime {
    let basename = rel_path.rsplit('/').next().unwrap_or(rel_path);
    let reply_parent_ms = in_reply_to.and_then(|raw| parse_epoch_ms_13(raw, false));

    // 1. 13-digit epoch ms prefix at head of filename or numeric frontmatter date_ms / id.
    //    If this letter is a reply reusing the parent's epoch prefix (e.g., `<parent_ms>-tally-reply.md`),
    //    do NOT mistake the parent's timestamp for the reply's own time — check `date_ms` / `date` / `ts` first.
    let mut deferred_parent_ms: Option<UnixMs> = None;
    if let Some(ts) = parse_epoch_ms_13(basename, true) {
        if reply_parent_ms == Some(ts) {
            deferred_parent_ms = Some(ts);
        } else {
            return ParsedLetterTime {
                ts,
                basis: TimeBasis::LetterIdMs,
                date_only: false,
            };
        }
    }
    if let Some(raw) = date_ms_field
        && let Some(ts) = parse_epoch_ms_13(raw, false)
    {
        return ParsedLetterTime {
            ts,
            basis: TimeBasis::LetterIdMs,
            date_only: false,
        };
    }
    if let Some(raw) = id_field
        && let Some(ts) = parse_epoch_ms_13(raw, false)
    {
        if reply_parent_ms == Some(ts) {
            deferred_parent_ms.get_or_insert(ts);
        } else {
            return ParsedLetterTime {
                ts,
                basis: TimeBasis::LetterIdMs,
                date_only: false,
            };
        }
    }

    // 2. Recorded frontmatter date/ts or YYYY-MM-DD- filename prefix.
    if let Some(raw) = date_field
        && let Some((ts, date_only)) = parse_recorded_datetime_detail(raw)
    {
        return ParsedLetterTime {
            ts,
            basis: TimeBasis::RecordedDate,
            date_only,
        };
    }
    if let Some(raw) = ts_field
        && let Some((ts, date_only)) = parse_recorded_datetime_detail(raw)
    {
        return ParsedLetterTime {
            ts,
            basis: TimeBasis::RecordedDate,
            date_only,
        };
    }
    if let Some(ts) = parse_filename_ymd_prefix(basename) {
        return ParsedLetterTime {
            ts,
            basis: TimeBasis::RecordedDate,
            date_only: true,
        };
    }

    // 3. Filesystem mtime last (or deferred parent ms only when mtime is unavailable).
    if let Some(ts) = mtime_ms {
        return ParsedLetterTime {
            ts,
            basis: TimeBasis::FileModified,
            date_only: false,
        };
    }
    if let Some(ts) = deferred_parent_ms {
        return ParsedLetterTime {
            ts,
            basis: TimeBasis::LetterIdMs,
            date_only: false,
        };
    }
    ParsedLetterTime {
        ts: UnixMs(0),
        basis: TimeBasis::FileModified,
        date_only: false,
    }
}

/// Resolves a letter's `(UnixMs, TimeBasis)` using the canonical precedence.
pub fn extract_letter_timestamp(
    rel_path: &str,
    id_field: Option<&str>,
    date_ms_field: Option<&str>,
    date_field: Option<&str>,
    ts_field: Option<&str>,
    mtime_ms: Option<UnixMs>,
) -> (UnixMs, TimeBasis) {
    let parsed = extract_letter_timestamp_detailed(
        rel_path,
        id_field,
        date_ms_field,
        date_field,
        ts_field,
        None,
        mtime_ms,
    );
    (parsed.ts, parsed.basis)
}

/// Default number of queued letters per seat above which rings coalesce into
/// one [`RingDecision::RingDigest`].
pub const DEFAULT_DIGEST_THRESHOLD: usize = 3;

/// Default wall-clock retry window (`60 min`) measured from `first_seen_at`,
/// never from attempt/pass count.
pub const DEFAULT_RETRY_DEADLINE_MS: u64 = 3_600_000;

/// Default reconnect grace window (`15 s`) matching `rex-wave-notify`.
pub const DEFAULT_RECONNECT_GRACE_MS: u64 = 15_000;

/// Structurally read-only handle over an interchange directory. Holds no
/// `StreamWriter`, cursor file, or ring transport, and exposes only `read_dir`
/// and `fs::read` operations so `--shadow-read-only` and dry runs cannot mutate state.
#[derive(Clone, Debug)]
pub struct ReadOnlyInterchange {
    root: PathBuf,
}

impl ReadOnlyInterchange {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, SandboxError> {
        let canon = fs::canonicalize(root.as_ref()).map_err(|e| SandboxError::Io(e.to_string()))?;
        let meta = fs::metadata(&canon).map_err(|e| SandboxError::Io(e.to_string()))?;
        if !meta.is_dir() {
            return Err(SandboxError::Io(format!(
                "{} is not a directory",
                canon.display()
            )));
        }
        Ok(Self { root: canon })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Reads and tolerantly parses `<root>/state/acks.jsonl` without modifying
    /// or creating any file.
    pub fn read_acks(&self, aliases: &AliasMap) -> Result<AcksParseReport, SandboxError> {
        let acks_path = self.root.join("state").join("acks.jsonl");
        let meta = match fs::symlink_metadata(&acks_path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(AcksParseReport::default());
            }
            Err(e) => return Err(SandboxError::Io(e.to_string())),
        };
        if meta.file_type().is_symlink() || !meta.file_type().is_file() {
            return Err(SandboxError::Io(format!(
                "{} is not a regular file",
                acks_path.display()
            )));
        }
        let bytes = fs::read(&acks_path).map_err(|e| SandboxError::Io(e.to_string()))?;
        let text = String::from_utf8_lossy(&bytes);
        Ok(parse_acks_jsonl(&text, aliases))
    }

    /// Scans `mail/*.md`, `to-*/*.md`, and root-level letter `*.md` files using
    /// only `read_dir` and `fs::read`.
    pub fn scan_letters(&self, aliases: &AliasMap) -> Result<Vec<ScannedLetter>, SandboxError> {
        let mut letters = Vec::new();
        let top = fs::read_dir(&self.root).map_err(|e| SandboxError::Io(e.to_string()))?;
        let mut dirs: Vec<(String, PathBuf)> = Vec::new();
        let mut root_files: Vec<(String, PathBuf)> = Vec::new();

        for entry in top.flatten() {
            let fname = entry.file_name();
            let Some(name) = fname.to_str() else {
                continue;
            };
            if name.starts_with('.') {
                continue;
            }
            let Ok(ft) = entry.file_type() else {
                continue;
            };
            if ft.is_symlink() {
                continue;
            }
            if ft.is_dir() && (name == "mail" || name.starts_with("to-")) {
                dirs.push((name.to_owned(), entry.path()));
            } else if ft.is_file()
                && name.ends_with(".md")
                && !name.to_ascii_lowercase().starts_with("readme")
                && !name.eq_ignore_ascii_case("chatter.md")
            {
                root_files.push((name.to_owned(), entry.path()));
            }
        }
        dirs.sort_by(|a, b| a.0.cmp(&b.0));
        root_files.sort_by(|a, b| a.0.cmp(&b.0));

        for (name, path) in root_files {
            if let Ok(letter) = read_scanned_letter(&path, &name, None, aliases) {
                letters.push(letter);
            }
        }

        for (dir_name, dir_path) in dirs {
            // `mail/` and `to-desktop/` are shared lanes routed by frontmatter
            // `to:`/`cc:` (`mailwatch.py` `SKIP_BOXES` + `shared_letters`).
            // `to-cc/` remains a legacy non-seat intake box (`SkipNonSeatTarget`).
            let box_seat = if dir_name == "mail" || dir_name == "to-desktop" {
                None
            } else {
                dir_name
                    .strip_prefix("to-")
                    .and_then(|raw| aliases.canonical_seat_name(raw))
            };
            let Ok(entries) = fs::read_dir(&dir_path) else {
                continue;
            };
            let mut files: Vec<(String, PathBuf)> = Vec::new();
            for f in entries.flatten() {
                let fname = f.file_name();
                let Some(name) = fname.to_str() else {
                    continue;
                };
                if name.starts_with('.')
                    || !name.ends_with(".md")
                    || name.to_ascii_lowercase().starts_with("readme")
                {
                    continue;
                }
                if f.file_type()
                    .is_ok_and(|ft| ft.is_file() && !ft.is_symlink())
                {
                    files.push((name.to_owned(), f.path()));
                }
            }
            files.sort_by(|a, b| a.0.cmp(&b.0));
            for (fname, fpath) in files {
                let rel = format!("{dir_name}/{fname}");
                if let Ok(letter) = read_scanned_letter(&fpath, &rel, box_seat.as_ref(), aliases) {
                    letters.push(letter);
                }
            }
        }

        letters.sort_by(|a, b| (&a.rel_path, &a.sha256).cmp(&(&b.rel_path, &b.sha256)));
        Ok(letters)
    }
}

/// Parsed letter discovered on disk by [`ReadOnlyInterchange::scan_letters`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScannedLetter {
    pub rel_path: String,
    pub sha256: Sha256Hex,
    pub sha16: String,
    pub from: Option<String>,
    pub recipients: Vec<SeatName>,
    pub subject: Option<String>,
    pub letter_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub receipt_only: bool,
    pub seen_at: UnixMs,
    pub time_basis: TimeBasis,
    pub date_only: bool,
}

impl ScannedLetter {
    /// Expands this letter into one [`LetterCandidate`] per canonical recipient seat.
    pub fn into_candidates(&self) -> Vec<LetterCandidate> {
        self.recipients
            .iter()
            .map(|seat| LetterCandidate {
                rel_path: self.rel_path.clone(),
                sha256: self.sha256.clone(),
                sha16: self.sha16.clone(),
                from: self.from.clone(),
                to: seat.clone(),
                subject: self.subject.clone(),
                letter_id: self.letter_id.clone(),
                in_reply_to: self.in_reply_to.clone(),
                receipt_only: self.receipt_only,
                seen_at: self.seen_at,
                time_basis: self.time_basis,
                date_only: self.date_only,
            })
            .collect()
    }
}

struct RecipientCandidate {
    raw: String,
    require_known_alias: bool,
}

#[derive(Default)]
struct ShadowFrontmatter {
    from: Option<String>,
    authors: Vec<RecipientCandidate>,
    to: Vec<RecipientCandidate>,
    cc: Vec<RecipientCandidate>,
    subject: Option<String>,
    id: Option<String>,
    date_ms: Option<String>,
    date: Option<String>,
    ts: Option<String>,
    in_reply_to: Option<String>,
    receipt_only: bool,
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

fn parse_recipient_list(val: &str) -> Vec<RecipientCandidate> {
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
            // Treat `/`-split segments like parenthetical candidates (known aliases only)
            // so embedded file paths in `to:` never become bogus seats.
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

fn is_receipt_only_status(val: &str) -> bool {
    let lower = val.to_ascii_lowercase();
    lower.contains("receipt only")
        || lower.contains("receipt-only")
        || lower.contains("no return ack")
        || lower.contains("no ack requested")
        || lower.contains("no-ack")
}

fn parse_shadow_frontmatter(text: &str) -> ShadowFrontmatter {
    let mut fm = ShadowFrontmatter::default();
    let rest = if let Some(r) = text.strip_prefix("---\n") {
        r
    } else if let Some(r) = text.strip_prefix("---\r\n") {
        r
    } else {
        return fm;
    };

    for line in rest.lines() {
        let trimmed = line.trim_end_matches('\r');
        if trimmed == "---" {
            break;
        }
        let Some((k, v)) = trimmed.split_once(':') else {
            continue;
        };
        let key = k.trim().to_ascii_lowercase();
        let val = unquote_yaml(v);
        if val.is_empty() {
            continue;
        }
        match key.as_str() {
            "from" | "author" => {
                fm.authors.extend(parse_recipient_list(&val));
                if fm.from.is_none() || key == "from" {
                    fm.from = Some(val);
                }
            }
            "to" => fm.to.extend(parse_recipient_list(&val)),
            "cc" => fm.cc.extend(parse_recipient_list(&val)),
            "subject" => fm.subject = Some(val),
            "id" => fm.id = Some(val),
            "date_ms" => fm.date_ms = Some(val),
            "date" => fm.date = Some(val),
            "ts" => fm.ts = Some(val),
            "in_reply_to" => fm.in_reply_to = Some(val),
            "receipt_only" | "receipt-only" | "no_ack" | "no-ack" => {
                if matches!(val.to_ascii_lowercase().as_str(), "true" | "yes" | "1") {
                    fm.receipt_only = true;
                }
            }
            "ack_required" | "return_ack" => {
                if matches!(
                    val.to_ascii_lowercase().as_str(),
                    "false" | "no" | "0" | "none"
                ) {
                    fm.receipt_only = true;
                }
            }
            "ack" => {
                if matches!(
                    val.to_ascii_lowercase().as_str(),
                    "none" | "false" | "no" | "receipt-only" | "receipt_only"
                ) {
                    fm.receipt_only = true;
                }
            }
            "kind" => {
                if matches!(
                    val.to_ascii_lowercase().as_str(),
                    "receipt" | "receipt-only"
                ) {
                    fm.receipt_only = true;
                }
            }
            "status" => {
                if is_receipt_only_status(&val) {
                    fm.receipt_only = true;
                }
            }
            _ => {}
        }
    }
    fm
}

fn read_scanned_letter(
    path: &Path,
    rel_path: &str,
    box_seat: Option<&SeatName>,
    aliases: &AliasMap,
) -> Result<ScannedLetter, SandboxError> {
    let bytes = fs::read(path).map_err(|e| SandboxError::Io(e.to_string()))?;
    let sha256 = sha256_bytes(&bytes);
    let sha16 = sha256.as_str()[..16].to_owned();
    let mtime_ms = fs::metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| UnixMs(d.as_millis() as u64));

    let text = String::from_utf8_lossy(&bytes);
    let fm = parse_shadow_frontmatter(&text);
    let parsed_time = extract_letter_timestamp_detailed(
        rel_path,
        fm.id.as_deref(),
        fm.date_ms.as_deref(),
        fm.date.as_deref(),
        fm.ts.as_deref(),
        fm.in_reply_to.as_deref(),
        mtime_ms,
    );

    let mut recipients = BTreeSet::new();
    if let Some(seat) = box_seat {
        recipients.insert(seat.clone());
    } else {
        for cand in fm.to.iter().chain(fm.cc.iter()) {
            if cand.require_known_alias
                && (is_operator_surface_seat(&cand.raw)
                    || is_non_seat_target(&cand.raw)
                    || !aliases.contains_known(&cand.raw))
            {
                continue;
            }
            if let Some(seat) = aliases.canonical_seat_name(&cand.raw) {
                recipients.insert(seat);
            }
        }
        // Per `mailwatch.py`'s shared-lane addressing law, a shared-lane letter
        // never rings any seat named in its own `from:` / `author:` fields.
        for cand in &fm.authors {
            if let Some(author_seat) = aliases.canonical_seat_name(&cand.raw) {
                recipients.remove(&author_seat);
            }
        }
    }

    Ok(ScannedLetter {
        rel_path: rel_path.to_owned(),
        sha256,
        sha16,
        from: fm.from,
        recipients: recipients.into_iter().collect(),
        subject: fm.subject,
        letter_id: fm.id,
        in_reply_to: fm.in_reply_to,
        receipt_only: fm.receipt_only,
        seen_at: parsed_time.ts,
        time_basis: parsed_time.basis,
        date_only: parsed_time.date_only,
    })
}

/// Pure input describing one banked letter candidate for one recipient seat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LetterCandidate {
    pub rel_path: String,
    pub sha256: Sha256Hex,
    pub sha16: String,
    pub from: Option<String>,
    pub to: SeatName,
    pub subject: Option<String>,
    pub letter_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub receipt_only: bool,
    pub seen_at: UnixMs,
    pub time_basis: TimeBasis,
    pub date_only: bool,
}

/// Branded mandatory `(path, sha16)` referent required on every ring decision.
/// A ring without both a concrete letter path and a 16-hex SHA-256 prefix is
/// unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RingReferent {
    path: String,
    sha16: String,
    sha256: Sha256Hex,
}

impl RingReferent {
    pub fn new(path: impl Into<String>, sha256: Sha256Hex) -> Result<Self, ApiError> {
        let path = path.into();
        let trimmed = path.trim();
        if trimmed.is_empty() || trimmed.chars().any(char::is_control) {
            return Err(ApiError::Invalid(InvalidId {
                what: "ring referent".into(),
                reason: "ring must carry a non-empty letter path".into(),
            }));
        }
        let sha16 = sha256.as_str()[..16].to_owned();
        Ok(Self {
            path: trimmed.to_owned(),
            sha16,
            sha256,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn sha16(&self) -> &str {
        &self.sha16
    }

    pub fn sha256(&self) -> &Sha256Hex {
        &self.sha256
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "path": self.path,
            "sha16": self.sha16,
            "sha256": self.sha256.as_str(),
        })
    }
}

/// Non-intrusive delivery channel selected for a live seat binding. Raw pane
/// keystroke injection is intentionally absent so rings can never collide with
/// a live turn (`ring-types-into-a-live-turn`).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RingDeliveryChannel {
    ClaudeMessagingSocket { pid: u32, socket: PathBuf },
    BbQueueIfActive { thread_id: BbThreadId },
    T3QueueIfActive { thread_id: T3ThreadId },
    UdsFramePush { conn_id: u64 },
    SelfWatcherLease { harness: String },
    OperatorToast { command: String },
}

impl RingDeliveryChannel {
    pub fn from_endpoint(endpoint: &TransportEndpoint) -> Self {
        match endpoint {
            TransportEndpoint::ClaudeSession {
                pid,
                messaging_socket,
                ..
            } => RingDeliveryChannel::ClaudeMessagingSocket {
                pid: *pid,
                socket: messaging_socket.clone(),
            },
            TransportEndpoint::BbThread { thread_id } => RingDeliveryChannel::BbQueueIfActive {
                thread_id: thread_id.clone(),
            },
            TransportEndpoint::T3Thread { thread_id } => RingDeliveryChannel::T3QueueIfActive {
                thread_id: thread_id.clone(),
            },
            TransportEndpoint::UdsSession { conn_id, .. } => {
                RingDeliveryChannel::UdsFramePush { conn_id: *conn_id }
            }
            TransportEndpoint::SelfWatcher { harness, .. } => {
                RingDeliveryChannel::SelfWatcherLease {
                    harness: harness.clone(),
                }
            }
            TransportEndpoint::OperatorToast { command } => RingDeliveryChannel::OperatorToast {
                command: command.clone(),
            },
        }
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            RingDeliveryChannel::ClaudeMessagingSocket { .. } => "claude_messaging_socket",
            RingDeliveryChannel::BbQueueIfActive { .. } => "bb_queue_if_active",
            RingDeliveryChannel::T3QueueIfActive { .. } => "t3_queue_if_active",
            RingDeliveryChannel::UdsFramePush { .. } => "uds_frame_push",
            RingDeliveryChannel::SelfWatcherLease { .. } => "self_watcher_lease",
            RingDeliveryChannel::OperatorToast { .. } => "operator_toast",
        }
    }
}

/// Wall-clock age tracker for deferred letters. Expiry depends solely on
/// `first_seen_at + deadline_ms`, never on `pass_count`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryEntry {
    pub first_seen_at: UnixMs,
    pub deadline_ms: u64,
    pub target_busy: bool,
    /// Diagnostic count of scheduler passes observed; never triggers give-up.
    pub pass_count: u32,
}

impl RetryEntry {
    pub fn deadline_at(&self) -> UnixMs {
        UnixMs(self.first_seen_at.0.saturating_add(self.deadline_ms))
    }

    pub fn is_expired(&self, now: UnixMs) -> bool {
        now.0 >= self.deadline_at().0
    }
}

#[derive(Clone, Debug, Default)]
pub struct RetryState {
    entries: BTreeMap<(SeatName, Sha256Hex), RetryEntry>,
}

impl RetryState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, seat: SeatName, sha256: Sha256Hex, entry: RetryEntry) {
        self.entries.insert((seat, sha256), entry);
    }

    pub fn get(&self, seat: &SeatName, sha256: &Sha256Hex) -> Option<&RetryEntry> {
        self.entries.get(&(seat.clone(), sha256.clone()))
    }
}

/// Configurable thresholds for [`decide_mail_ring`] and [`decide_mail_rings_for_seat`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingPolicy {
    pub digest_threshold: usize,
    pub retry_deadline_ms: u64,
    pub reconnect_grace_ms: u64,
}

impl Default for RingPolicy {
    fn default() -> Self {
        Self {
            digest_threshold: DEFAULT_DIGEST_THRESHOLD,
            retry_deadline_ms: DEFAULT_RETRY_DEADLINE_MS,
            reconnect_grace_ms: DEFAULT_RECONNECT_GRACE_MS,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeferReason {
    ReconnectGrace { ready_at: UnixMs },
    TargetBusy { pass_count: u32 },
    TransportDown { transport: TransportKind },
}

/// Pure output of the mail-ring decision engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RingDecision {
    SkipAlreadyAcked {
        seat: SeatName,
        sha256: Sha256Hex,
        time_basis: TimeBasis,
    },
    SkipReceiptOnly {
        seat: SeatName,
        referent: RingReferent,
        notice: String,
        time_basis: TimeBasis,
    },
    SkipDormantOrRetired {
        seat: SeatName,
        status: SeatStatus,
        referent: RingReferent,
        time_basis: TimeBasis,
    },
    SkipPreHorizonHistory {
        seat: SeatName,
        referent: RingReferent,
        letter_at: UnixMs,
        horizon_at: UnixMs,
        time_basis: TimeBasis,
    },
    SkipOperatorSurface {
        seat: SeatName,
        referent: RingReferent,
        time_basis: TimeBasis,
    },
    SkipNonSeatTarget {
        seat: SeatName,
        referent: RingReferent,
        time_basis: TimeBasis,
    },
    FlagMutatedAfterAck {
        seat: SeatName,
        referent: RingReferent,
        prior_ack_sha: String,
        time_basis: TimeBasis,
    },
    Ring {
        seat: SeatName,
        incarnation_id: IncarnationId,
        channel: RingDeliveryChannel,
        referent: RingReferent,
        time_basis: TimeBasis,
    },
    RingDigest {
        seat: SeatName,
        incarnation_id: IncarnationId,
        channel: RingDeliveryChannel,
        referents: Vec<RingReferent>,
        time_basis: TimeBasis,
    },
    Defer {
        seat: SeatName,
        referent: RingReferent,
        reason: DeferReason,
        deadline_at: UnixMs,
        time_basis: TimeBasis,
    },
    RaiseUnrung {
        seat: SeatName,
        referents: Vec<RingReferent>,
        oldest_at: UnixMs,
        sender_notice: String,
        time_basis: TimeBasis,
    },
    RefuseMissingReferent {
        seat: SeatName,
        reason: String,
        time_basis: TimeBasis,
    },
}

/// Canonical alias for shadow mail actions emitted by [`decide_mail_ring`].
pub type ShadowAction = RingDecision;

impl RingDecision {
    pub fn time_basis(&self) -> TimeBasis {
        match self {
            RingDecision::SkipAlreadyAcked { time_basis, .. }
            | RingDecision::SkipReceiptOnly { time_basis, .. }
            | RingDecision::SkipDormantOrRetired { time_basis, .. }
            | RingDecision::SkipPreHorizonHistory { time_basis, .. }
            | RingDecision::SkipOperatorSurface { time_basis, .. }
            | RingDecision::SkipNonSeatTarget { time_basis, .. }
            | RingDecision::FlagMutatedAfterAck { time_basis, .. }
            | RingDecision::Ring { time_basis, .. }
            | RingDecision::RingDigest { time_basis, .. }
            | RingDecision::Defer { time_basis, .. }
            | RingDecision::RaiseUnrung { time_basis, .. }
            | RingDecision::RefuseMissingReferent { time_basis, .. } => *time_basis,
        }
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            RingDecision::SkipAlreadyAcked {
                seat,
                sha256,
                time_basis,
            } => serde_json::json!({
                "decision": "skip_already_acked",
                "seat": seat.as_str(),
                "sha256": sha256.as_str(),
                "time_basis": time_basis.as_str(),
            }),
            RingDecision::SkipReceiptOnly {
                seat,
                referent,
                notice,
                time_basis,
            } => serde_json::json!({
                "decision": "skip_receipt_only",
                "seat": seat.as_str(),
                "referent": referent.to_json_value(),
                "notice": notice,
                "time_basis": time_basis.as_str(),
            }),
            RingDecision::SkipDormantOrRetired {
                seat,
                status,
                referent,
                time_basis,
            } => serde_json::json!({
                "decision": "skip_dormant_or_retired",
                "seat": seat.as_str(),
                "status": format!("{status:?}").to_ascii_lowercase(),
                "referent": referent.to_json_value(),
                "time_basis": time_basis.as_str(),
            }),
            RingDecision::SkipPreHorizonHistory {
                seat,
                referent,
                letter_at,
                horizon_at,
                time_basis,
            } => serde_json::json!({
                "decision": "skip_pre_horizon_history",
                "seat": seat.as_str(),
                "referent": referent.to_json_value(),
                "letter_at_ms": letter_at.0,
                "horizon_at_ms": horizon_at.0,
                "time_basis": time_basis.as_str(),
            }),
            RingDecision::SkipOperatorSurface {
                seat,
                referent,
                time_basis,
            } => serde_json::json!({
                "decision": "skip_operator_surface",
                "seat": seat.as_str(),
                "referent": referent.to_json_value(),
                "time_basis": time_basis.as_str(),
            }),
            RingDecision::SkipNonSeatTarget {
                seat,
                referent,
                time_basis,
            } => serde_json::json!({
                "decision": "skip_non_seat_target",
                "seat": seat.as_str(),
                "referent": referent.to_json_value(),
                "time_basis": time_basis.as_str(),
            }),
            RingDecision::FlagMutatedAfterAck {
                seat,
                referent,
                prior_ack_sha,
                time_basis,
            } => serde_json::json!({
                "decision": "flag_mutated_after_ack",
                "seat": seat.as_str(),
                "referent": referent.to_json_value(),
                "prior_ack_sha": prior_ack_sha,
                "time_basis": time_basis.as_str(),
            }),
            RingDecision::Ring {
                seat,
                incarnation_id,
                channel,
                referent,
                time_basis,
            } => serde_json::json!({
                "decision": "ring",
                "seat": seat.as_str(),
                "incarnation_id": incarnation_id.as_str(),
                "channel": channel.kind_name(),
                "referent": referent.to_json_value(),
                "time_basis": time_basis.as_str(),
            }),
            RingDecision::RingDigest {
                seat,
                incarnation_id,
                channel,
                referents,
                time_basis,
            } => serde_json::json!({
                "decision": "ring_digest",
                "seat": seat.as_str(),
                "incarnation_id": incarnation_id.as_str(),
                "channel": channel.kind_name(),
                "count": referents.len(),
                "referents": referents.iter().map(RingReferent::to_json_value).collect::<Vec<_>>(),
                "time_basis": time_basis.as_str(),
            }),
            RingDecision::Defer {
                seat,
                referent,
                reason,
                deadline_at,
                time_basis,
            } => serde_json::json!({
                "decision": "defer",
                "seat": seat.as_str(),
                "referent": referent.to_json_value(),
                "reason": format!("{reason:?}"),
                "deadline_at_ms": deadline_at.0,
                "time_basis": time_basis.as_str(),
            }),
            RingDecision::RaiseUnrung {
                seat,
                referents,
                oldest_at,
                sender_notice,
                time_basis,
            } => serde_json::json!({
                "decision": "raise_unrung",
                "seat": seat.as_str(),
                "count": referents.len(),
                "oldest_at_ms": oldest_at.0,
                "sender_notice": sender_notice,
                "referents": referents.iter().map(RingReferent::to_json_value).collect::<Vec<_>>(),
                "time_basis": time_basis.as_str(),
            }),
            RingDecision::RefuseMissingReferent {
                seat,
                reason,
                time_basis,
            } => serde_json::json!({
                "decision": "refuse_missing_referent",
                "seat": seat.as_str(),
                "reason": reason,
                "time_basis": time_basis.as_str(),
            }),
        }
    }
}

/// Pure single-letter mail-ring policy function.
#[allow(clippy::too_many_arguments)]
pub fn decide_mail_ring(
    letter: &LetterCandidate,
    ack_index: &AckIndex,
    presence: &PresenceState,
    affinity: &IncarnationAffinity,
    retry_state: &RetryState,
    policy: &RingPolicy,
    aliases: &AliasMap,
    now: UnixMs,
) -> RingDecision {
    let time_basis = letter.time_basis;
    let referent = match RingReferent::new(&letter.rel_path, letter.sha256.clone()) {
        Ok(r) => r,
        Err(e) => {
            return RingDecision::RefuseMissingReferent {
                seat: letter.to.clone(),
                reason: e.to_string(),
                time_basis,
            };
        }
    };

    if ack_index.is_acked_by_digest(letter.to.as_str(), &letter.sha256, aliases) {
        return RingDecision::SkipAlreadyAcked {
            seat: letter.to.clone(),
            sha256: letter.sha256.clone(),
            time_basis,
        };
    }

    if let Some(prior_ack_sha) =
        ack_index.mutated_after_ack(letter.to.as_str(), referent.path(), &letter.sha256, aliases)
    {
        return RingDecision::FlagMutatedAfterAck {
            seat: letter.to.clone(),
            referent,
            prior_ack_sha,
            time_basis,
        };
    }

    if letter.receipt_only {
        return RingDecision::SkipReceiptOnly {
            seat: letter.to.clone(),
            notice: format!(
                "receipt-only letter {} ({}) requires no return ACK",
                referent.path(),
                referent.sha16()
            ),
            referent,
            time_basis,
        };
    }

    if is_operator_surface_seat(letter.to.as_str()) {
        return RingDecision::SkipOperatorSurface {
            seat: letter.to.clone(),
            referent,
            time_basis,
        };
    }

    if is_non_seat_target(letter.to.as_str()) {
        return RingDecision::SkipNonSeatTarget {
            seat: letter.to.clone(),
            referent,
            time_basis,
        };
    }

    if let Some(record) = presence.seat(&letter.to) {
        if matches!(record.status, SeatStatus::Dormant | SeatStatus::Retired) {
            return RingDecision::SkipDormantOrRetired {
                seat: letter.to.clone(),
                status: record.status,
                referent,
                time_basis,
            };
        }
        if let Some(horizon_at) = record.mail_horizon_at {
            let is_pre_horizon = if letter.date_only {
                letter.seen_at.0.saturating_add(DATE_ONLY_HORIZON_WINDOW_MS) <= horizon_at.0
            } else {
                letter.seen_at < horizon_at
            };
            if is_pre_horizon {
                return RingDecision::SkipPreHorizonHistory {
                    seat: letter.to.clone(),
                    referent,
                    letter_at: letter.seen_at,
                    horizon_at,
                    time_basis,
                };
            }
        }
    }

    let retry = retry_state.get(&letter.to, &letter.sha256);

    if let Some(binding) = select_binding_for_delivery(
        presence,
        &letter.to,
        None,
        letter.in_reply_to.as_deref(),
        affinity,
        now,
    ) {
        if let Some(entry) = retry
            && entry.target_busy
        {
            if !entry.is_expired(now) {
                return RingDecision::Defer {
                    seat: letter.to.clone(),
                    referent,
                    reason: DeferReason::TargetBusy {
                        pass_count: entry.pass_count,
                    },
                    deadline_at: entry.deadline_at(),
                    time_basis,
                };
            }
            let notice = format!(
                "banked; retry deadline elapsed for {} while target remained busy",
                letter.to
            );
            return RingDecision::RaiseUnrung {
                seat: letter.to.clone(),
                oldest_at: letter.seen_at,
                referents: vec![referent],
                sender_notice: notice,
                time_basis,
            };
        }

        return RingDecision::Ring {
            seat: letter.to.clone(),
            incarnation_id: binding.incarnation_id.clone(),
            channel: RingDeliveryChannel::from_endpoint(&binding.endpoint),
            referent,
            time_basis,
        };
    }

    // No live binding: check TransportDown circuit-breaker first.
    if let Some(down_binding) = presence
        .bindings_for(&letter.to)
        .into_iter()
        .find(|b| b.status == BindingStatus::TransportDown)
    {
        let fallback_entry = RetryEntry {
            first_seen_at: letter.seen_at,
            deadline_ms: policy.retry_deadline_ms,
            target_busy: false,
            pass_count: 0,
        };
        let entry = retry.unwrap_or(&fallback_entry);
        if !entry.is_expired(now) {
            return RingDecision::Defer {
                seat: letter.to.clone(),
                referent,
                reason: DeferReason::TransportDown {
                    transport: down_binding.endpoint.kind(),
                },
                deadline_at: entry.deadline_at(),
                time_basis,
            };
        }
    }

    // Reconnect grace window (`15 s`) for a recently dropped reconnectable binding;
    // `0 s` when the seat was never bound.
    if presence.was_ever_bound(&letter.to)
        && let Some((dropped_at, true)) = presence.last_drop_info(&letter.to)
    {
        let ready_at = UnixMs(dropped_at.0.saturating_add(policy.reconnect_grace_ms));
        if now < ready_at {
            return RingDecision::Defer {
                seat: letter.to.clone(),
                referent,
                reason: DeferReason::ReconnectGrace { ready_at },
                deadline_at: ready_at,
                time_basis,
            };
        }
    }

    let sender_notice = sender_delivery_notice(presence, &letter.to, now).unwrap_or_else(|| {
        format!(
            "banked; no live binding for {} since never; will ring on rebind",
            letter.to
        )
    });
    RingDecision::RaiseUnrung {
        seat: letter.to.clone(),
        oldest_at: letter.seen_at,
        referents: vec![referent],
        sender_notice,
        time_basis,
    }
}

/// Evaluates all queued letters for one seat, deduplicating identical
/// `(seat, sha256)` copies across `mail/` and `to-<seat>/`, batching `>3`
/// live rings into one [`RingDecision::RingDigest`], and coalescing offline
/// alerts into one per-seat [`RingDecision::RaiseUnrung`].
#[allow(clippy::too_many_arguments)]
pub fn decide_mail_rings_for_seat(
    seat: &SeatName,
    letters: &[LetterCandidate],
    ack_index: &AckIndex,
    presence: &PresenceState,
    affinity: &IncarnationAffinity,
    retry_state: &RetryState,
    policy: &RingPolicy,
    aliases: &AliasMap,
    now: UnixMs,
) -> Vec<RingDecision> {
    let mut seen_shas: BTreeSet<Sha256Hex> = BTreeSet::new();
    let mut out = Vec::new();
    let mut rings_by_target: BTreeMap<
        (IncarnationId, RingDeliveryChannel),
        Vec<(RingReferent, UnixMs, TimeBasis)>,
    > = BTreeMap::new();
    let mut unrung_referents: Vec<RingReferent> = Vec::new();
    let mut unrung_oldest: Option<(UnixMs, TimeBasis)> = None;
    let mut unrung_notice: Option<String> = None;

    for letter in letters {
        if &letter.to != seat {
            continue;
        }
        // Deduplicate multiple path copies (`mail/` + `to-<seat>/`) by `(seat, sha256)`.
        if !seen_shas.insert(letter.sha256.clone()) {
            continue;
        }
        match decide_mail_ring(
            letter,
            ack_index,
            presence,
            affinity,
            retry_state,
            policy,
            aliases,
            now,
        ) {
            RingDecision::Ring {
                incarnation_id,
                channel,
                referent,
                time_basis,
                ..
            } => {
                rings_by_target
                    .entry((incarnation_id, channel))
                    .or_default()
                    .push((referent, letter.seen_at, time_basis));
            }
            RingDecision::RaiseUnrung {
                referents,
                oldest_at,
                sender_notice,
                time_basis,
                ..
            } => {
                unrung_oldest = Some(match unrung_oldest {
                    Some(prev) if prev <= (oldest_at, time_basis) => prev,
                    _ => (oldest_at, time_basis),
                });
                unrung_notice.get_or_insert(sender_notice);
                unrung_referents.extend(referents);
            }
            other => out.push(other),
        }
    }

    for ((incarnation_id, channel), items) in rings_by_target {
        if items.len() > policy.digest_threshold {
            let digest_basis = items
                .iter()
                .map(|(_, seen_at, basis)| (*seen_at, *basis))
                .min()
                .map(|(_, basis)| basis)
                .unwrap_or(TimeBasis::FileModified);
            let referents = items.into_iter().map(|(r, _, _)| r).collect();
            out.push(RingDecision::RingDigest {
                seat: seat.clone(),
                incarnation_id,
                channel,
                referents,
                time_basis: digest_basis,
            });
        } else {
            for (referent, _, time_basis) in items {
                out.push(RingDecision::Ring {
                    seat: seat.clone(),
                    incarnation_id: incarnation_id.clone(),
                    channel: channel.clone(),
                    referent,
                    time_basis,
                });
            }
        }
    }

    if !unrung_referents.is_empty() {
        let (oldest_at, time_basis) = unrung_oldest.unwrap_or((now, TimeBasis::FileModified));
        out.push(RingDecision::RaiseUnrung {
            seat: seat.clone(),
            referents: unrung_referents,
            oldest_at,
            sender_notice: unrung_notice.unwrap_or_default(),
            time_basis,
        });
    }

    out
}

/// Complete read-only shadow pass output.
#[derive(Clone, Debug)]
pub struct ShadowPassReport {
    pub letters_scanned: usize,
    pub acks_report: AcksParseReport,
    pub decisions: Vec<RingDecision>,
}

impl ShadowPassReport {
    /// Formats every shadow decision as one JSONL line for `--shadow-read-only` output.
    pub fn to_jsonl(&self) -> String {
        let mut out = String::new();
        for d in &self.decisions {
            if let Ok(line) = serde_json::to_string(&d.to_json_value()) {
                out.push_str(&line);
                out.push('\n');
            }
        }
        out
    }

    /// Formats shadow output as JSONL, prepending a `{"meta":"aliases","aliases":{...}}`
    /// header line whenever `aliases` is non-empty so offline `mail score` preserves
    /// the exact seat-alias mapping from live presence.
    pub fn to_jsonl_with_aliases(&self, aliases: &AliasMap) -> String {
        let mut out = String::new();
        if !aliases.is_empty() {
            let header = serde_json::json!({
                "meta": "aliases",
                "aliases": aliases.entries(),
            });
            if let Ok(line) = serde_json::to_string(&header) {
                out.push_str(&line);
                out.push('\n');
            }
        }
        out.push_str(&self.to_jsonl());
        out
    }
}

/// Runs one structurally read-only shadow pass over `interchange`. Never writes
/// to `interchange`, never mutates any cursor file, and never invokes a ring transport.
#[allow(clippy::too_many_arguments)]
pub fn run_shadow_pass(
    interchange: &ReadOnlyInterchange,
    presence: &PresenceState,
    affinity: &IncarnationAffinity,
    retry_state: &RetryState,
    policy: &RingPolicy,
    aliases: &AliasMap,
    now: UnixMs,
) -> Result<ShadowPassReport, SandboxError> {
    let acks_report = interchange.read_acks(aliases)?;
    let ack_index = acks_report.index();
    let scanned = interchange.scan_letters(aliases)?;
    let letters_scanned = scanned.len();

    let mut by_seat: BTreeMap<SeatName, Vec<LetterCandidate>> = BTreeMap::new();
    for letter in &scanned {
        for cand in letter.into_candidates() {
            by_seat.entry(cand.to.clone()).or_default().push(cand);
        }
    }

    let mut decisions = Vec::new();
    for (seat, candidates) in &by_seat {
        decisions.extend(decide_mail_rings_for_seat(
            seat,
            candidates,
            &ack_index,
            presence,
            affinity,
            retry_state,
            policy,
            aliases,
            now,
        ));
    }

    Ok(ShadowPassReport {
        letters_scanned,
        acks_report,
        decisions,
    })
}
