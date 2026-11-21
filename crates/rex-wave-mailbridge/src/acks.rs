//! Tolerant `acks.jsonl` parser, seat-alias normalizer, path normalizer, and
//! `(canonical_seat, sha256)` receipt index covering historical house schemas.

use crate::presence::PresenceState;
use rex_wave_api::{SeatName, Sha256Hex};
use std::collections::{BTreeMap, BTreeSet};

/// Normalizes a raw seat string (`@Alice-Desk`, `bob (review)`, `Worker-2 (3)`).
pub fn normalize_seat_token(raw: &str) -> String {
    let trimmed = raw.trim().trim_start_matches('@').trim();
    let base = match trimmed.find(" (") {
        Some(idx) => trimmed[..idx].trim(),
        None => trimmed,
    };
    base.to_ascii_lowercase()
}

/// Maps historical display names and seat aliases to canonical seat names.
#[derive(Clone, Debug, Default)]
pub struct AliasMap {
    aliases: BTreeMap<String, String>,
}

impl AliasMap {
    pub fn empty() -> Self {
        Self::default()
    }

    /// Builds an alias map from explicit `(alias, canonical)` pairs supplied by
    /// deployment config or tests.
    pub fn from_pairs<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        let mut map = Self::empty();
        for (alias, canonical) in pairs {
            map.insert(alias, canonical);
        }
        map
    }

    /// Builds an alias map from every durable `SeatRecord`'s canonical name and
    /// aliases in `PresenceState`.
    pub fn from_presence(presence: &PresenceState) -> Self {
        let mut map = Self::empty();
        for (seat, rec) in presence.seats() {
            let canon = seat.as_str();
            map.insert(canon, canon);
            for alias in &rec.aliases {
                map.insert(alias.value.as_str(), canon);
            }
        }
        map
    }

    pub fn insert(&mut self, alias: &str, canonical: &str) {
        let key = normalize_seat_token(alias);
        let val = normalize_seat_token(canonical);
        if !key.is_empty() && !val.is_empty() {
            self.aliases.insert(key, val);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.aliases.is_empty()
    }

    /// Returns `true` when `raw` matches a known canonical seat or alias entry.
    pub fn contains_known(&self, raw: &str) -> bool {
        let norm = normalize_seat_token(raw);
        if self.aliases.contains_key(&norm) {
            return true;
        }
        norm.strip_prefix("to-")
            .is_some_and(|stripped| self.aliases.contains_key(stripped))
    }

    pub fn entries(&self) -> &BTreeMap<String, String> {
        &self.aliases
    }

    pub fn canonicalize(&self, raw: &str) -> String {
        let norm = normalize_seat_token(raw);
        if let Some(mapped) = self.aliases.get(&norm) {
            return mapped.clone();
        }
        if let Some(stripped) = norm.strip_prefix("to-")
            && let Some(mapped) = self.aliases.get(stripped)
        {
            return mapped.clone();
        }
        norm
    }

    pub fn canonical_seat_name(&self, raw: &str) -> Option<SeatName> {
        let c = self.canonicalize(raw);
        SeatName::parse(c).ok()
    }
}

/// Normalized interchange letter path across bare filename, `mail/...`,
/// `to-<seat>/...`, and `interchange/...` spellings.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NormalizedLetterPath {
    pub canonical: String,
    pub basename: String,
}

pub fn normalize_letter_path(raw: &str) -> Option<NormalizedLetterPath> {
    let mut s = raw.trim().replace('\\', "/");
    loop {
        let next = s
            .strip_prefix("./")
            .or_else(|| s.strip_prefix('/'))
            .or_else(|| s.strip_prefix("~/agent-folder/"))
            .or_else(|| s.strip_prefix("agent-folder/"))
            .or_else(|| s.strip_prefix("interchange/"));
        match next {
            Some(rest) => s = rest.to_owned(),
            None => break,
        }
    }
    let canonical = s.trim_matches('/').to_owned();
    if canonical.is_empty() {
        return None;
    }
    let basename = canonical
        .rsplit('/')
        .next()
        .unwrap_or(canonical.as_str())
        .to_owned();
    Some(NormalizedLetterPath {
        canonical,
        basename,
    })
}

pub fn letter_paths_match(a: &str, b: &str) -> bool {
    let (Some(na), Some(nb)) = (normalize_letter_path(a), normalize_letter_path(b)) else {
        return false;
    };
    if na.canonical == nb.canonical {
        return true;
    }
    // A bare filename (`foo.md`) matches `mail/foo.md` or `to-cc/foo.md`,
    // whereas two distinct directory-qualified paths (`to-alice/foo.md` vs
    // `to-bob/foo.md`) do not match by path alone.
    let a_bare = !na.canonical.contains('/');
    let b_bare = !nb.canonical.contains('/');
    (a_bare || b_bare) && na.basename == nb.basename
}

/// One normalized ACK receipt extracted from an `acks.jsonl` line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckRecord {
    pub line_number: usize,
    pub seat: Option<String>,
    pub raw_seat: Option<String>,
    pub path: Option<NormalizedLetterPath>,
    pub sha256_full: Option<Sha256Hex>,
    pub sha16: Option<String>,
    pub ts: Option<String>,
}

impl AckRecord {
    pub fn digest_display(&self) -> Option<&str> {
        self.sha256_full
            .as_ref()
            .map(Sha256Hex::as_str)
            .or(self.sha16.as_deref())
    }

    pub fn matches_sha256(&self, sha256: &Sha256Hex) -> bool {
        if let Some(full) = &self.sha256_full {
            return full == sha256;
        }
        if let Some(short) = &self.sha16 {
            return short.as_str() == &sha256.as_str()[..16];
        }
        false
    }

    pub fn has_different_digest_than(&self, sha256: &Sha256Hex) -> bool {
        if let Some(full) = &self.sha256_full {
            return full != sha256;
        }
        if let Some(short) = &self.sha16 {
            return short.as_str() != &sha256.as_str()[..16];
        }
        false
    }
}

/// A malformed or non-JSON line encountered in `acks.jsonl` (reported, never fatal).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkippedAckLine {
    pub line_number: usize,
    pub reason: String,
    pub raw_preview: String,
}

/// Full result of parsing an `acks.jsonl` file.
#[derive(Clone, Debug, Default)]
pub struct AcksParseReport {
    pub total_lines: usize,
    pub blank_lines: usize,
    pub non_ack_rows: usize,
    pub records: Vec<AckRecord>,
    pub skipped_lines: Vec<SkippedAckLine>,
}

impl AcksParseReport {
    pub fn into_index(self) -> AckIndex {
        AckIndex::from_records(self.records)
    }

    pub fn index(&self) -> AckIndex {
        AckIndex::from_records(self.records.clone())
    }
}

fn parse_hex_digest(raw: &str) -> (Option<Sha256Hex>, Option<String>) {
    let cleaned = raw
        .trim()
        .strip_prefix("sha256:")
        .unwrap_or(raw.trim())
        .to_ascii_lowercase();
    if !cleaned.bytes().all(|b| b.is_ascii_hexdigit()) {
        return (None, None);
    }
    if cleaned.len() == 64 {
        let short = cleaned[..16].to_owned();
        let full = Sha256Hex::parse(cleaned).ok();
        (full, Some(short))
    } else if cleaned.len() == 16 {
        (None, Some(cleaned))
    } else {
        (None, None)
    }
}

fn extract_digests(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> (Option<Sha256Hex>, Option<String>) {
    let mut full: Option<Sha256Hex> = None;
    let mut short: Option<String> = None;

    for key in ["sha256_full", "sha256", "sha256_16", "sha16"] {
        let Some(val) = obj.get(key).and_then(|v| v.as_str()) else {
            continue;
        };
        let (f, s) = parse_hex_digest(val);
        if full.is_none() && f.is_some() {
            full = f;
        }
        if short.is_none() && s.is_some() {
            short = s;
        }
    }
    if let Some(f) = &full
        && short.is_none()
    {
        short = Some(f.as_str()[..16].to_owned());
    }
    (full, short)
}

fn extract_seat(
    obj: &serde_json::Map<String, serde_json::Value>,
    aliases: &AliasMap,
) -> (Option<String>, Option<String>) {
    for key in ["acked_by", "seat", "by"] {
        if let Some(raw) = obj
            .get(key)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let canon = aliases.canonicalize(raw);
            if !canon.is_empty() {
                return (Some(canon), Some(raw.to_owned()));
            }
        }
    }
    (None, None)
}

fn extract_ts(obj: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    for key in ["ts", "date"] {
        if let Some(s) = obj
            .get(key)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return Some(s.to_owned());
        }
    }
    None
}

fn extract_single_path(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Option<NormalizedLetterPath> {
    for key in ["letter", "path", "file"] {
        if let Some(raw) = obj.get(key).and_then(|v| v.as_str())
            && let Some(norm) = normalize_letter_path(raw)
        {
            return Some(norm);
        }
    }
    None
}

/// Extracts zero or more [`AckRecord`]s from a single parsed JSON object row.
pub fn parse_ack_value(
    val: &serde_json::Value,
    line_number: usize,
    aliases: &AliasMap,
) -> Option<Vec<AckRecord>> {
    let obj = val.as_object()?;
    let (parent_seat, parent_raw_seat) = extract_seat(obj, aliases);
    let ts = extract_ts(obj);
    let (parent_full, parent_short) = extract_digests(obj);

    let mut out = Vec::new();

    if let Some(path) = extract_single_path(obj) {
        out.push(AckRecord {
            line_number,
            seat: parent_seat.clone(),
            raw_seat: parent_raw_seat.clone(),
            path: Some(path),
            sha256_full: parent_full.clone(),
            sha16: parent_short.clone(),
            ts: ts.clone(),
        });
    } else if parent_full.is_some() || parent_short.is_some() {
        out.push(AckRecord {
            line_number,
            seat: parent_seat.clone(),
            raw_seat: parent_raw_seat.clone(),
            path: None,
            sha256_full: parent_full.clone(),
            sha16: parent_short.clone(),
            ts: ts.clone(),
        });
    }

    for array_key in ["letters", "items"] {
        let Some(arr) = obj.get(array_key).and_then(|v| v.as_array()) else {
            continue;
        };
        for item in arr {
            if let Some(raw_path) = item.as_str() {
                if let Some(norm) = normalize_letter_path(raw_path) {
                    out.push(AckRecord {
                        line_number,
                        seat: parent_seat.clone(),
                        raw_seat: parent_raw_seat.clone(),
                        path: Some(norm),
                        sha256_full: parent_full.clone(),
                        sha16: parent_short.clone(),
                        ts: ts.clone(),
                    });
                }
            } else if let Some(item_obj) = item.as_object() {
                let (item_seat, item_raw_seat) = extract_seat(item_obj, aliases);
                let (item_full, item_short) = extract_digests(item_obj);
                let path = extract_single_path(item_obj);
                if path.is_some() || item_full.is_some() || item_short.is_some() {
                    out.push(AckRecord {
                        line_number,
                        seat: item_seat.or_else(|| parent_seat.clone()),
                        raw_seat: item_raw_seat.or_else(|| parent_raw_seat.clone()),
                        path,
                        sha256_full: item_full.or_else(|| parent_full.clone()),
                        sha16: item_short.or_else(|| parent_short.clone()),
                        ts: extract_ts(item_obj).or_else(|| ts.clone()),
                    });
                }
            }
        }
    }

    Some(out)
}

fn preview_line(raw: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in raw.chars().enumerate() {
        if idx >= max_chars {
            out.push_str("...");
            break;
        }
        out.push(ch);
    }
    out
}

/// Parses an entire `acks.jsonl` document tolerantly across all historical
/// schemas, recording non-JSON lines in `skipped_lines` without aborting.
pub fn parse_acks_jsonl(contents: &str, aliases: &AliasMap) -> AcksParseReport {
    let mut report = AcksParseReport::default();

    for (idx, raw_line) in contents.lines().enumerate() {
        let line_number = idx + 1;
        report.total_lines += 1;
        let trimmed = raw_line.trim();
        if trimmed.is_empty() {
            report.blank_lines += 1;
            continue;
        }
        let val = match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(v) => v,
            Err(e) => {
                report.skipped_lines.push(SkippedAckLine {
                    line_number,
                    reason: format!("non-JSON line: {e}"),
                    raw_preview: preview_line(trimmed, 120),
                });
                continue;
            }
        };
        match parse_ack_value(&val, line_number, aliases) {
            Some(records) if !records.is_empty() => {
                report.records.extend(records);
            }
            Some(_) => {
                report.non_ack_rows += 1;
            }
            None => {
                report.skipped_lines.push(SkippedAckLine {
                    line_number,
                    reason: "JSON value is not an object".into(),
                    raw_preview: preview_line(trimmed, 120),
                });
            }
        }
    }

    report
}

/// Checks whether a single JSON object line matches a sandbox letter ACK for
/// `(to_seat, rel_path, sha256)` using tolerant schema, alias, and path matching.
pub fn ack_value_matches(
    val: &serde_json::Value,
    to_seat: &str,
    rel_path: &str,
    sha256: &Sha256Hex,
    aliases: &AliasMap,
) -> bool {
    let Some(records) = parse_ack_value(val, 0, aliases) else {
        return false;
    };
    let target_seat = aliases.canonicalize(to_seat);
    for rec in records {
        if let Some(rec_seat) = &rec.seat
            && rec_seat != &target_seat
        {
            continue;
        }
        if !rec.matches_sha256(sha256) {
            continue;
        }
        if let Some(rec_path) = &rec.path
            && !letter_paths_match(&rec_path.canonical, rel_path)
        {
            continue;
        }
        return true;
    }
    false
}

/// Fast read-only index over parsed `acks.jsonl` receipts keyed by
/// `(canonical_seat, sha256_full || sha16)` and normalized path.
#[derive(Clone, Debug, Default)]
pub struct AckIndex {
    records: Vec<AckRecord>,
    by_seat_full: BTreeSet<(String, String)>,
    by_seat_sha16: BTreeSet<(String, String)>,
    by_path_records: BTreeMap<String, Vec<usize>>,
    by_basename_records: BTreeMap<String, Vec<usize>>,
}

impl AckIndex {
    pub fn from_records(records: Vec<AckRecord>) -> Self {
        let mut by_seat_full = BTreeSet::new();
        let mut by_seat_sha16 = BTreeSet::new();
        let mut by_path_records: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        let mut by_basename_records: BTreeMap<String, Vec<usize>> = BTreeMap::new();

        for (idx, rec) in records.iter().enumerate() {
            if let Some(seat) = &rec.seat {
                if let Some(full) = &rec.sha256_full {
                    by_seat_full.insert((seat.clone(), full.as_str().to_owned()));
                    by_seat_sha16.insert((seat.clone(), full.as_str()[..16].to_owned()));
                } else if let Some(short) = &rec.sha16 {
                    by_seat_sha16.insert((seat.clone(), short.clone()));
                }
            }
            if let Some(path) = &rec.path {
                by_path_records
                    .entry(path.canonical.clone())
                    .or_default()
                    .push(idx);
                by_basename_records
                    .entry(path.basename.clone())
                    .or_default()
                    .push(idx);
            }
        }

        Self {
            records,
            by_seat_full,
            by_seat_sha16,
            by_path_records,
            by_basename_records,
        }
    }

    pub fn records(&self) -> &[AckRecord] {
        &self.records
    }

    /// Deduplicates by `(canonical_seat, sha256_full || sha16)` regardless of
    /// whether the letter lives in `mail/` or `to-<seat>/`.
    pub fn is_acked_by_digest(&self, seat: &str, sha256: &Sha256Hex, aliases: &AliasMap) -> bool {
        let canon = aliases.canonicalize(seat);
        let full = sha256.as_str();
        let short = &full[..16];
        if self
            .by_seat_full
            .contains(&(canon.clone(), full.to_owned()))
        {
            return true;
        }
        // Only match a 16-hex receipt when that receipt did not record a
        // conflicting full 64-hex sha256.
        if !self
            .by_seat_sha16
            .contains(&(canon.clone(), short.to_owned()))
        {
            return false;
        }
        self.records
            .iter()
            .any(|rec| rec.seat.as_deref() == Some(canon.as_str()) && rec.matches_sha256(sha256))
    }

    /// Returns the prior ACKed digest when `rel_path` was previously ACKed (by
    /// `seat`, or by any seat if the file was overwritten in place) with a
    /// *different* SHA-256 than `current_sha256`, and `seat` has not ACKed
    /// `current_sha256` yet.
    pub fn mutated_after_ack(
        &self,
        seat: &str,
        rel_path: &str,
        current_sha256: &Sha256Hex,
        aliases: &AliasMap,
    ) -> Option<String> {
        if self.is_acked_by_digest(seat, current_sha256, aliases) {
            return None;
        }
        let norm = normalize_letter_path(rel_path)?;
        let canon_seat = aliases.canonicalize(seat);

        let mut candidate_indices: Vec<usize> = Vec::new();
        if let Some(exact) = self.by_path_records.get(&norm.canonical) {
            candidate_indices.extend_from_slice(exact);
        }
        if let Some(base) = self.by_basename_records.get(&norm.basename) {
            for &idx in base {
                if !candidate_indices.contains(&idx)
                    && let Some(rec_path) = &self.records[idx].path
                    && letter_paths_match(&rec_path.canonical, &norm.canonical)
                {
                    candidate_indices.push(idx);
                }
            }
        }

        // Prefer a prior ACK by the same recipient seat; fall back to any prior
        // ACK on that exact path (filename reuse / in-place overwrite).
        for &idx in &candidate_indices {
            let rec = &self.records[idx];
            if rec.seat.as_deref() == Some(canon_seat.as_str())
                && rec.has_different_digest_than(current_sha256)
            {
                return rec.digest_display().map(str::to_owned);
            }
        }
        for &idx in &candidate_indices {
            let rec = &self.records[idx];
            if rec.has_different_digest_than(current_sha256) {
                return rec.digest_display().map(str::to_owned);
            }
        }
        None
    }
}
