//! Gate 2 scorer comparing `--shadow-read-only` decisions against ground-truth
//! `acks.jsonl` receipts by `(canonical_seat, sha256)`, plus Gate 1 fixture
//! coverage verification (`cases.jsonl`).

use crate::acks::{AckIndex, AliasMap};
use crate::shadow::RingDecision;
use rex_wave_api::{SeatName, Sha256Hex};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The 10 canonical Gate 1 mail-routing cases mapped to the exact Rust
/// integration test function name in
/// `crates/rex-wave-mailbridge/tests/presence_and_shadow_mail.rs` that verifies
/// each case's behavior.
pub const GATE1_CASE_TESTS: &[(&str, &str)] = &[
    ("genga-stale-six-days", "gate1_case_01_genga_stale_six_days"),
    (
        "carbon-crest-misroute",
        "gate1_case_02_carbon_crest_misroute",
    ),
    ("two-tables-one-name", "gate1_case_03_two_tables_one_name"),
    ("dry-run-marks-seen", "gate1_case_04_dry_run_marks_seen"),
    (
        "ring-types-into-a-live-turn",
        "gate1_case_05_ring_types_into_a_live_turn",
    ),
    ("a-transport-dies", "gate1_case_06_a_transport_dies"),
    ("two-seats-one-name", "gate1_case_07_two_seats_one_name"),
    ("session-names-drift", "gate1_case_08_session_names_drift"),
    ("own-watcher-liveness", "gate1_case_09_own_watcher_liveness"),
    ("bank-before-ring", "gate1_case_10_bank_before_ring"),
];

/// The 10 canonical Gate 1 mail-routing case IDs verified in `presence_and_shadow_mail`.
pub const GATE1_CASE_IDS: &[&str] = &[
    "genga-stale-six-days",
    "carbon-crest-misroute",
    "two-tables-one-name",
    "dry-run-marks-seen",
    "ring-types-into-a-live-turn",
    "a-transport-dies",
    "two-seats-one-name",
    "session-names-drift",
    "own-watcher-liveness",
    "bank-before-ring",
];

/// Coverage report comparing a Gate 1 `cases.jsonl` fixture file against
/// [`GATE1_CASE_IDS`] and mapping each case ID to its verifying integration test
/// in `crates/rex-wave-mailbridge/tests/presence_and_shadow_mail.rs`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureCoverageReport {
    pub total_cases: usize,
    pub covered_cases: usize,
    /// True only when the fixture file registers the exact set of [`GATE1_CASE_IDS`]
    /// with no missing and no unknown IDs. Behavioral proof lives in the test
    /// functions listed in `case_test_map`.
    #[serde(alias = "all_covered")]
    pub fixture_ids_registered: bool,
    pub case_ids: Vec<String>,
    pub missing_case_ids: Vec<String>,
    pub unknown_case_ids: Vec<String>,
    #[serde(default)]
    pub case_test_map: BTreeMap<String, String>,
}

impl FixtureCoverageReport {
    pub fn fixture_ids_registered(&self) -> bool {
        self.fixture_ids_registered
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "total_cases": self.total_cases,
            "covered_cases": self.covered_cases,
            "fixture_ids_registered": self.fixture_ids_registered,
            "case_ids": self.case_ids,
            "missing_case_ids": self.missing_case_ids,
            "unknown_case_ids": self.unknown_case_ids,
            "case_test_map": self.case_test_map,
        })
    }
}

/// Parses a Gate 1 `cases.jsonl` fixture document and verifies bidirectional
/// registration against [`GATE1_CASE_IDS`] (`missing_case_ids` and `unknown_case_ids`),
/// attaching the exact verifying test function name per case ID.
pub fn check_gate1_fixtures(cases_jsonl: &str) -> Result<FixtureCoverageReport, String> {
    let expected: BTreeSet<&str> = GATE1_CASE_IDS.iter().copied().collect();
    let mut case_ids = Vec::new();
    let mut seen = BTreeSet::new();
    let mut unknown_case_ids = Vec::new();

    for (idx, raw) in cases_jsonl.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let val: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| format!("fixtures line {}: invalid JSON: {e}", idx + 1))?;
        let id = val
            .get("id")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("fixtures line {}: missing non-empty `id`", idx + 1))?;
        if seen.insert(id.to_owned()) {
            case_ids.push(id.to_owned());
            if !expected.contains(id) {
                unknown_case_ids.push(id.to_owned());
            }
        }
    }

    let missing_case_ids: Vec<String> = GATE1_CASE_IDS
        .iter()
        .filter(|id| !seen.contains(**id))
        .map(|id| (*id).to_owned())
        .collect();
    let total_cases = GATE1_CASE_IDS.len();
    let covered_cases = total_cases.saturating_sub(missing_case_ids.len());
    let fixture_ids_registered = total_cases == GATE1_CASE_IDS.len()
        && missing_case_ids.is_empty()
        && unknown_case_ids.is_empty();
    let case_test_map: BTreeMap<String, String> = GATE1_CASE_TESTS
        .iter()
        .map(|(id, test_fn)| ((*id).to_owned(), (*test_fn).to_owned()))
        .collect();
    Ok(FixtureCoverageReport {
        total_cases,
        covered_cases,
        fixture_ids_registered,
        case_ids,
        missing_case_ids,
        unknown_case_ids,
        case_test_map,
    })
}

/// Snapshot score comparing shadow decisions against one ground-truth [`AckIndex`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShadowScoreReport {
    pub total_decisions: usize,
    pub rings_emitted: usize,
    pub referents_rung: usize,
    /// Rung referents whose `(canonical_seat, sha256)` appears in `acks`.
    pub ring_referents_confirmed_acked: usize,
    /// Rung referents whose `(canonical_seat, sha256)` does not yet appear in `acks`.
    pub ring_referents_pending_ack: usize,
    pub skipped_already_acked: usize,
    pub skipped_receipt_only: usize,
    pub skipped_dormant_or_retired: usize,
    pub skipped_pre_horizon: usize,
    pub skipped_operator_surface: usize,
    pub skipped_non_seat_target: usize,
    pub mutated_after_ack_flags: usize,
    pub deferred: usize,
    pub unrung_alerts: usize,
    /// `RaiseUnrung` referents absent from `acks` (caught silent misses).
    pub unrung_referents_caught: usize,
    /// `RaiseUnrung` referents already present in `acks` (should be 0 for same-snapshot runs).
    pub unrung_referents_already_acked: usize,
    pub missing_referent_refusals: usize,
    pub seats_with_unrung: Vec<SeatName>,
}

impl ShadowScoreReport {
    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "total_decisions": self.total_decisions,
            "rings_emitted": self.rings_emitted,
            "referents_rung": self.referents_rung,
            "ring_referents_confirmed_acked": self.ring_referents_confirmed_acked,
            "ring_referents_pending_ack": self.ring_referents_pending_ack,
            "skipped_already_acked": self.skipped_already_acked,
            "skipped_receipt_only": self.skipped_receipt_only,
            "skipped_dormant_or_retired": self.skipped_dormant_or_retired,
            "skipped_pre_horizon": self.skipped_pre_horizon,
            "skipped_operator_surface": self.skipped_operator_surface,
            "skipped_non_seat_target": self.skipped_non_seat_target,
            "mutated_after_ack_flags": self.mutated_after_ack_flags,
            "deferred": self.deferred,
            "unrung_alerts": self.unrung_alerts,
            "unrung_referents_caught": self.unrung_referents_caught,
            "unrung_referents_already_acked": self.unrung_referents_already_acked,
            "missing_referent_refusals": self.missing_referent_refusals,
            "seats_with_unrung": self.seats_with_unrung.iter().map(SeatName::as_str).collect::<Vec<_>>(),
        })
    }

    fn record_ring_referent(
        &mut self,
        seat: &SeatName,
        sha256: &Sha256Hex,
        acks: &AckIndex,
        aliases: &AliasMap,
    ) {
        self.referents_rung += 1;
        if acks.is_acked_by_digest(seat.as_str(), sha256, aliases) {
            self.ring_referents_confirmed_acked += 1;
        } else {
            self.ring_referents_pending_ack += 1;
        }
    }

    fn record_unrung_referent(
        &mut self,
        seat: &SeatName,
        sha256: &Sha256Hex,
        acks: &AckIndex,
        aliases: &AliasMap,
    ) {
        if acks.is_acked_by_digest(seat.as_str(), sha256, aliases) {
            self.unrung_referents_already_acked += 1;
        } else {
            self.unrung_referents_caught += 1;
        }
    }
}

/// Scores in-memory shadow [`RingDecision`]s against ground-truth `acks.jsonl`.
pub fn score_shadow_against_acks(
    decisions: &[RingDecision],
    acks: &AckIndex,
    aliases: &AliasMap,
) -> ShadowScoreReport {
    let mut report = ShadowScoreReport {
        total_decisions: decisions.len(),
        ..ShadowScoreReport::default()
    };
    let mut unrung_seats = BTreeSet::new();

    for decision in decisions {
        match decision {
            RingDecision::SkipAlreadyAcked { .. } => report.skipped_already_acked += 1,
            RingDecision::SkipReceiptOnly { .. } => report.skipped_receipt_only += 1,
            RingDecision::SkipDormantOrRetired { .. } => report.skipped_dormant_or_retired += 1,
            RingDecision::SkipPreHorizonHistory { .. } => report.skipped_pre_horizon += 1,
            RingDecision::SkipOperatorSurface { .. } => report.skipped_operator_surface += 1,
            RingDecision::SkipNonSeatTarget { .. } => report.skipped_non_seat_target += 1,
            RingDecision::FlagMutatedAfterAck { .. } => report.mutated_after_ack_flags += 1,
            RingDecision::Defer { .. } => report.deferred += 1,
            RingDecision::RefuseMissingReferent { .. } => report.missing_referent_refusals += 1,
            RingDecision::Ring { seat, referent, .. } => {
                report.rings_emitted += 1;
                report.record_ring_referent(seat, referent.sha256(), acks, aliases);
            }
            RingDecision::RingDigest {
                seat, referents, ..
            } => {
                report.rings_emitted += 1;
                for referent in referents {
                    report.record_ring_referent(seat, referent.sha256(), acks, aliases);
                }
            }
            RingDecision::RaiseUnrung {
                seat, referents, ..
            } => {
                report.unrung_alerts += 1;
                unrung_seats.insert(seat.clone());
                for referent in referents {
                    report.record_unrung_referent(seat, referent.sha256(), acks, aliases);
                }
            }
        }
    }

    report.seats_with_unrung = unrung_seats.into_iter().collect();
    report
}

fn extract_referent_sha(val: &serde_json::Value, line_no: usize) -> Result<Sha256Hex, String> {
    let raw = val
        .get("sha256")
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("shadow line {line_no}: referent missing `sha256`"))?;
    Sha256Hex::parse(raw)
        .map_err(|e| format!("shadow line {line_no}: invalid referent `sha256`: {e}"))
}

fn extract_referents_shas(
    obj: &serde_json::Map<String, serde_json::Value>,
    line_no: usize,
) -> Result<Vec<Sha256Hex>, String> {
    let arr = obj
        .get("referents")
        .and_then(|v| v.as_array())
        .ok_or_else(|| format!("shadow line {line_no}: missing `referents` array"))?;
    let mut shas = Vec::with_capacity(arr.len());
    for item in arr {
        shas.push(extract_referent_sha(item, line_no)?);
    }
    Ok(shas)
}

/// Extracts any `{"meta":"aliases","aliases":{...}}` header lines emitted by
/// `ShadowPassReport::to_jsonl_with_aliases` into an [`AliasMap`].
pub fn extract_shadow_aliases(shadow_jsonl: &str) -> Result<AliasMap, String> {
    let mut aliases = AliasMap::empty();
    for (idx, raw) in shadow_jsonl.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let val: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| format!("shadow line {line_no}: invalid JSON: {e}"))?;
        let Some(obj) = val.as_object() else {
            return Err(format!("shadow line {line_no}: expected JSON object"));
        };
        if obj.get("meta").and_then(|v| v.as_str()) == Some("aliases") {
            let map = obj
                .get("aliases")
                .and_then(|v| v.as_object())
                .ok_or_else(|| format!("shadow line {line_no}: missing `aliases` object"))?;
            for (k, v) in map {
                let canon = v.as_str().ok_or_else(|| {
                    format!("shadow line {line_no}: alias target for `{k}` must be a string")
                })?;
                aliases.insert(k, canon);
            }
        }
    }
    Ok(aliases)
}

/// Scores a JSONL shadow log produced by `ShadowPassReport::to_jsonl` or
/// `ShadowPassReport::to_jsonl_with_aliases` against ground-truth `acks.jsonl`,
/// matching [`score_shadow_against_acks`] exactly.
pub fn score_shadow_jsonl(
    shadow_jsonl: &str,
    acks: &AckIndex,
    aliases: &AliasMap,
) -> Result<ShadowScoreReport, String> {
    let mut report = ShadowScoreReport::default();
    let mut unrung_seats = BTreeSet::new();

    for (idx, raw) in shadow_jsonl.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let val: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| format!("shadow line {line_no}: invalid JSON: {e}"))?;
        let obj = val
            .as_object()
            .ok_or_else(|| format!("shadow line {line_no}: expected JSON object"))?;
        if obj.get("meta").and_then(|v| v.as_str()) == Some("aliases") {
            continue;
        }
        let decision = obj
            .get("decision")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("shadow line {line_no}: missing `decision` field"))?;
        let seat_raw = obj
            .get("seat")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("shadow line {line_no}: missing `seat` field"))?;
        let seat = SeatName::parse(seat_raw)
            .map_err(|e| format!("shadow line {line_no}: invalid `seat` '{seat_raw}': {e}"))?;

        report.total_decisions += 1;
        match decision {
            "skip_already_acked" => report.skipped_already_acked += 1,
            "skip_receipt_only" => report.skipped_receipt_only += 1,
            "skip_dormant_or_retired" => report.skipped_dormant_or_retired += 1,
            "skip_pre_horizon_history" => report.skipped_pre_horizon += 1,
            "skip_operator_surface" => report.skipped_operator_surface += 1,
            "skip_non_seat_target" => report.skipped_non_seat_target += 1,
            "flag_mutated_after_ack" => report.mutated_after_ack_flags += 1,
            "defer" => report.deferred += 1,
            "refuse_missing_referent" => report.missing_referent_refusals += 1,
            "ring" => {
                let ref_val = obj
                    .get("referent")
                    .ok_or_else(|| format!("shadow line {line_no}: missing `referent`"))?;
                let sha = extract_referent_sha(ref_val, line_no)?;
                report.rings_emitted += 1;
                report.record_ring_referent(&seat, &sha, acks, aliases);
            }
            "ring_digest" => {
                let shas = extract_referents_shas(obj, line_no)?;
                report.rings_emitted += 1;
                for sha in &shas {
                    report.record_ring_referent(&seat, sha, acks, aliases);
                }
            }
            "raise_unrung" => {
                let shas = extract_referents_shas(obj, line_no)?;
                report.unrung_alerts += 1;
                unrung_seats.insert(seat.clone());
                for sha in &shas {
                    report.record_unrung_referent(&seat, sha, acks, aliases);
                }
            }
            other => {
                return Err(format!(
                    "shadow line {line_no}: unknown decision kind '{other}'"
                ));
            }
        }
    }

    report.seats_with_unrung = unrung_seats.into_iter().collect();
    Ok(report)
}

/// Parsed `.watchers/mailwatch-state/seen.jsonl` ring log indexed by normalized
/// raw key (`<box_or_shared>:<sha16>`) and canonical `(seat, sha16)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MailwatchSeenIndex {
    by_raw_key: BTreeMap<String, String>,
    by_seat_sha16: BTreeMap<(String, String), String>,
}

fn mailwatch_outcome_rank(outcome: &str) -> u8 {
    if matches!(outcome, "rung" | "unknown") {
        4
    } else if outcome == "own-knocker" {
        3
    } else if outcome.starts_with("defer") || outcome.starts_with("unrung") {
        2
    } else {
        1
    }
}

impl MailwatchSeenIndex {
    /// Number of unique normalized keys (`<box_or_shared>:<sha16>`) in `seen.jsonl`.
    pub fn len(&self) -> usize {
        self.by_raw_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_raw_key.is_empty()
    }

    /// Returns the final `mailwatch.py` outcome for `(seat, sha16)` across
    /// `shared-<alias>` and `to-<alias>` lanes after canonicalizing `seat`.
    pub fn outcome_for(&self, seat: &str, sha16: &str, aliases: &AliasMap) -> Option<&str> {
        let canon = aliases.canonicalize(seat);
        self.by_seat_sha16
            .get(&(canon, sha16.trim().to_ascii_lowercase()))
            .map(String::as_str)
    }
}

/// Cross-check summary comparing `rex-wave` shadow rings (`ring` + `ring_digest`)
/// against `mailwatch.py`'s `.watchers/mailwatch-state/seen.jsonl`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailwatchComparisonReport {
    pub seen_entries: usize,
    pub shadow_ring_referents: usize,
    pub agreed_rung: usize,
    pub own_knocker_in_mailwatch: usize,
    pub deferred_or_unrung_in_mailwatch: usize,
    pub other_in_mailwatch: usize,
    pub absent_in_mailwatch: usize,
}

impl MailwatchComparisonReport {
    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "seen_entries": self.seen_entries,
            "shadow_ring_referents": self.shadow_ring_referents,
            "agreed_rung": self.agreed_rung,
            "own_knocker_in_mailwatch": self.own_knocker_in_mailwatch,
            "deferred_or_unrung_in_mailwatch": self.deferred_or_unrung_in_mailwatch,
            "other_in_mailwatch": self.other_in_mailwatch,
            "absent_in_mailwatch": self.absent_in_mailwatch,
        })
    }
}

/// Parses `.watchers/mailwatch-state/seen.jsonl` matching `mailwatch.py`'s
/// `load_seen()` semantics: legacy `"box/file:sha16"` keys normalize to
/// `"box:sha16"`, and later lines overwrite earlier lines for the same
/// normalized key.
pub fn parse_mailwatch_seen_jsonl(
    raw: &str,
    aliases: &AliasMap,
) -> Result<MailwatchSeenIndex, String> {
    let mut by_raw_key: BTreeMap<String, String> = BTreeMap::new();

    for (idx, line_raw) in raw.lines().enumerate() {
        let line_no = idx + 1;
        let line = line_raw.trim();
        if line.is_empty() {
            continue;
        }
        let val: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| format!("mailwatch seen line {line_no}: invalid JSON: {e}"))?;
        let obj = val
            .as_object()
            .ok_or_else(|| format!("mailwatch seen line {line_no}: expected JSON object"))?;
        let key = obj
            .get("key")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("mailwatch seen line {line_no}: missing `key` field"))?;
        let outcome = obj
            .get("outcome")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("mailwatch seen line {line_no}: missing `outcome` field"))?;
        let Some((left, sha_raw)) = key.rsplit_once(':') else {
            return Err(format!(
                "mailwatch seen line {line_no}: key `{key}` missing `:` separator"
            ));
        };
        let box_part = left.split('/').next().unwrap_or(left).trim();
        let sha16 = sha_raw.trim().to_ascii_lowercase();
        if box_part.is_empty() || sha16.is_empty() {
            return Err(format!(
                "mailwatch seen line {line_no}: invalid key `{key}`"
            ));
        }
        by_raw_key.insert(format!("{box_part}:{sha16}"), outcome.to_owned());
    }

    let mut by_seat_sha16: BTreeMap<(String, String), String> = BTreeMap::new();
    for (norm_key, outcome) in &by_raw_key {
        let Some((box_part, sha16)) = norm_key.rsplit_once(':') else {
            continue;
        };
        let Some(raw_seat) = box_part
            .strip_prefix("shared-")
            .or_else(|| box_part.strip_prefix("to-"))
        else {
            continue;
        };
        let canon = aliases.canonicalize(raw_seat);
        if canon.is_empty() {
            continue;
        }
        let map_key = (canon, sha16.to_owned());
        match by_seat_sha16.get_mut(&map_key) {
            Some(prev) => {
                if mailwatch_outcome_rank(outcome) >= mailwatch_outcome_rank(prev) {
                    *prev = outcome.clone();
                }
            }
            None => {
                by_seat_sha16.insert(map_key, outcome.clone());
            }
        }
    }

    Ok(MailwatchSeenIndex {
        by_raw_key,
        by_seat_sha16,
    })
}

/// Compares `ring` and `ring_digest` referents emitted in `shadow_jsonl`
/// against a parsed [`MailwatchSeenIndex`]. Per `mailwatch.py`, both `"rung"`
/// and `"unknown"` count as rung (`agreed_rung`).
pub fn compare_shadow_with_mailwatch_seen(
    shadow_jsonl: &str,
    seen: &MailwatchSeenIndex,
    aliases: &AliasMap,
) -> Result<MailwatchComparisonReport, String> {
    let mut report = MailwatchComparisonReport {
        seen_entries: seen.len(),
        ..MailwatchComparisonReport::default()
    };

    let mut record = |seat: &SeatName, sha: &Sha256Hex| {
        report.shadow_ring_referents += 1;
        let sha16 = &sha.as_str()[..16];
        match seen.outcome_for(seat.as_str(), sha16, aliases) {
            Some("rung" | "unknown") => report.agreed_rung += 1,
            Some("own-knocker") => report.own_knocker_in_mailwatch += 1,
            Some(o) if o.starts_with("defer") || o.starts_with("unrung") => {
                report.deferred_or_unrung_in_mailwatch += 1;
            }
            Some(_) => report.other_in_mailwatch += 1,
            None => report.absent_in_mailwatch += 1,
        }
    };

    for (idx, raw) in shadow_jsonl.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let val: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| format!("shadow line {line_no}: invalid JSON: {e}"))?;
        let obj = val
            .as_object()
            .ok_or_else(|| format!("shadow line {line_no}: expected JSON object"))?;
        if obj.get("meta").and_then(|v| v.as_str()) == Some("aliases") {
            continue;
        }
        let decision = obj
            .get("decision")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("shadow line {line_no}: missing `decision` field"))?;
        match decision {
            "ring" => {
                let seat_raw = obj
                    .get("seat")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| format!("shadow line {line_no}: missing `seat` field"))?;
                let seat = SeatName::parse(seat_raw).map_err(|e| {
                    format!("shadow line {line_no}: invalid `seat` '{seat_raw}': {e}")
                })?;
                let ref_val = obj
                    .get("referent")
                    .ok_or_else(|| format!("shadow line {line_no}: missing `referent`"))?;
                let sha = extract_referent_sha(ref_val, line_no)?;
                record(&seat, &sha);
            }
            "ring_digest" => {
                let seat_raw = obj
                    .get("seat")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| format!("shadow line {line_no}: missing `seat` field"))?;
                let seat = SeatName::parse(seat_raw).map_err(|e| {
                    format!("shadow line {line_no}: invalid `seat` '{seat_raw}': {e}")
                })?;
                let shas = extract_referents_shas(obj, line_no)?;
                for sha in &shas {
                    record(&seat, sha);
                }
            }
            _ => {}
        }
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acks::parse_acks_jsonl;
    use crate::shadow::{RingDeliveryChannel, RingReferent, ShadowPassReport, TimeBasis};
    use rex_wave_api::IncarnationId;
    use std::path::PathBuf;

    #[test]
    fn check_gate1_fixtures_reports_missing_and_unknown_cases_bidirectionally() {
        let all_ten: String = GATE1_CASE_IDS
            .iter()
            .map(|id| format!(r#"{{"id":"{id}"}}"#))
            .collect::<Vec<_>>()
            .join("\n");
        let full = check_gate1_fixtures(&all_ten).unwrap();
        assert!(full.fixture_ids_registered());
        assert_eq!(full.total_cases, 10);
        assert_eq!(full.covered_cases, 10);
        assert!(full.missing_case_ids.is_empty());
        assert!(full.unknown_case_ids.is_empty());
        assert_eq!(full.case_test_map.len(), 10);
        assert_eq!(
            full.case_test_map
                .get("dry-run-marks-seen")
                .map(String::as_str),
            Some("gate1_case_04_dry_run_marks_seen")
        );

        // Verify legacy serde alias `all_covered` still deserializes into `fixture_ids_registered`.
        let legacy: FixtureCoverageReport = serde_json::from_value(serde_json::json!({
            "total_cases": 10,
            "covered_cases": 10,
            "all_covered": true,
            "case_ids": [],
            "missing_case_ids": [],
            "unknown_case_ids": []
        }))
        .unwrap();
        assert!(legacy.fixture_ids_registered());

        let one_line = r#"{"id":"genga-stale-six-days"}"#;
        let subset = check_gate1_fixtures(one_line).unwrap();
        assert!(!subset.fixture_ids_registered());
        assert_eq!(subset.total_cases, 10);
        assert_eq!(subset.covered_cases, 1);
        assert_eq!(subset.missing_case_ids.len(), 9);
        assert!(subset.unknown_case_ids.is_empty());

        let with_unknown = format!("{all_ten}\n{{\"id\":\"surprise-case\"}}");
        let extra = check_gate1_fixtures(&with_unknown).unwrap();
        assert!(!extra.fixture_ids_registered());
        assert_eq!(extra.covered_cases, 10);
        assert!(extra.missing_case_ids.is_empty());
        assert_eq!(extra.unknown_case_ids, vec!["surprise-case".to_owned()]);

        assert!(check_gate1_fixtures("{not-json").is_err());
        assert!(check_gate1_fixtures(r#"{"id":"   "}"#).is_err());
    }

    #[test]
    fn shadow_alias_header_propagates_to_offline_score() {
        let sha = Sha256Hex::parse("a".repeat(64)).unwrap();
        let aliases = AliasMap::from_pairs([("tally-5.2", "tally")]);
        let report = ShadowPassReport {
            letters_scanned: 1,
            acks_report: Default::default(),
            decisions: vec![RingDecision::Ring {
                seat: SeatName::parse("tally").unwrap(),
                incarnation_id: IncarnationId::parse("inc_1").unwrap(),
                channel: RingDeliveryChannel::ClaudeMessagingSocket {
                    socket: PathBuf::from("/tmp/cc.sock"),
                    pid: 42,
                },
                referent: RingReferent::new("mail/01.md", sha.clone()).unwrap(),
                time_basis: TimeBasis::LetterIdMs,
            }],
        };
        let jsonl = report.to_jsonl_with_aliases(&aliases);
        let parsed_aliases = extract_shadow_aliases(&jsonl).unwrap();
        assert_eq!(parsed_aliases.canonicalize("Tally-5.2"), "tally");

        let acks_raw =
            format!(r#"{{"acked_by":"Tally-5.2","letter":"mail/01.md","sha256":"{sha}"}}"#);
        let ack_idx = parse_acks_jsonl(&acks_raw, &parsed_aliases).index();
        let scored = score_shadow_jsonl(&jsonl, &ack_idx, &parsed_aliases).unwrap();
        assert_eq!(scored.rings_emitted, 1);
        assert_eq!(scored.ring_referents_confirmed_acked, 1);
        assert_eq!(scored.ring_referents_pending_ack, 0);
    }

    #[test]
    fn mailwatch_seen_comparison_handles_all_outcomes_overwrites_and_aliases() {
        let sha_rung = Sha256Hex::parse(format!("{}01", "a".repeat(62))).unwrap();
        let sha_unknown = Sha256Hex::parse(format!("{}02", "b".repeat(62))).unwrap();
        let sha_own = Sha256Hex::parse(format!("{}03", "c".repeat(62))).unwrap();
        let sha_unrung = Sha256Hex::parse(format!("{}04", "d".repeat(62))).unwrap();
        let sha_defer = Sha256Hex::parse(format!("{}05", "e".repeat(62))).unwrap();
        let sha_seeded = Sha256Hex::parse(format!("{}06", "f".repeat(62))).unwrap();
        let sha_absent = Sha256Hex::parse(format!("{}07", "1".repeat(62))).unwrap();

        let aliases = AliasMap::from_pairs([("muse", "auburn-cepheus"), ("tally_votp", "tally")]);
        let ch = RingDeliveryChannel::ClaudeMessagingSocket {
            socket: PathBuf::from("/tmp/cc.sock"),
            pid: 99,
        };
        let inc = IncarnationId::parse("inc_1").unwrap();
        let report = ShadowPassReport {
            letters_scanned: 7,
            acks_report: Default::default(),
            decisions: vec![
                RingDecision::Ring {
                    seat: SeatName::parse("tally").unwrap(),
                    incarnation_id: inc.clone(),
                    channel: ch.clone(),
                    referent: RingReferent::new("mail/01.md", sha_rung.clone()).unwrap(),
                    time_basis: TimeBasis::LetterIdMs,
                },
                RingDecision::Ring {
                    seat: SeatName::parse("auburn-cepheus").unwrap(),
                    incarnation_id: inc.clone(),
                    channel: ch.clone(),
                    referent: RingReferent::new("to-muse/02.md", sha_unknown.clone()).unwrap(),
                    time_basis: TimeBasis::LetterIdMs,
                },
                RingDecision::Ring {
                    seat: SeatName::parse("crest").unwrap(),
                    incarnation_id: inc.clone(),
                    channel: ch.clone(),
                    referent: RingReferent::new("mail/03.md", sha_own.clone()).unwrap(),
                    time_basis: TimeBasis::LetterIdMs,
                },
                RingDecision::RingDigest {
                    seat: SeatName::parse("genga").unwrap(),
                    incarnation_id: inc,
                    channel: ch,
                    referents: vec![
                        RingReferent::new("mail/04.md", sha_unrung.clone()).unwrap(),
                        RingReferent::new("mail/05.md", sha_defer.clone()).unwrap(),
                        RingReferent::new("mail/06.md", sha_seeded.clone()).unwrap(),
                        RingReferent::new("mail/07.md", sha_absent.clone()).unwrap(),
                    ],
                    time_basis: TimeBasis::LetterIdMs,
                },
            ],
        };
        let shadow_jsonl = report.to_jsonl_with_aliases(&aliases);

        let seen_raw = [
            // Earlier defer:missing overwritten by later rung for same normalized key
            format!(
                r#"{{"key":"shared-tally_votp:{}","outcome":"defer:missing","attempts":1,"ts":"2026-11-21T19:00:00Z"}}"#,
                &sha_rung.as_str()[..16]
            ),
            format!(
                r#"{{"key":"shared-tally_votp:{}","outcome":"rung","attempts":2,"ts":"2026-11-21T19:02:00Z"}}"#,
                &sha_rung.as_str()[..16]
            ),
            // Legacy box/file:sha16 format with alias `to-muse` + outcome `unknown` (counts as rung)
            format!(
                r#"{{"key":"to-muse/legacy-file.md:{}","outcome":"unknown","attempts":1,"ts":"2026-11-21T19:05:00Z"}}"#,
                &sha_unknown.as_str()[..16]
            ),
            // own-knocker
            format!(
                r#"{{"key":"shared-crest:{}","outcome":"own-knocker","attempts":1,"ts":"2026-11-21T19:06:00Z"}}"#,
                &sha_own.as_str()[..16]
            ),
            // Earlier defer:missing overwritten by unrung:gave-up
            format!(
                r#"{{"key":"shared-genga:{}","outcome":"defer:missing","attempts":29,"ts":"2026-11-21T19:07:00Z"}}"#,
                &sha_unrung.as_str()[..16]
            ),
            format!(
                r#"{{"key":"shared-genga:{}","outcome":"unrung:gave-up","attempts":30,"ts":"2026-11-21T19:09:00Z"}}"#,
                &sha_unrung.as_str()[..16]
            ),
            // Still deferred
            format!(
                r#"{{"key":"shared-genga:{}","outcome":"defer:busy","attempts":3,"ts":"2026-11-21T19:10:00Z"}}"#,
                &sha_defer.as_str()[..16]
            ),
            // Seeded (other)
            format!(
                r#"{{"key":"to-genga/old.md:{}","outcome":"seeded","attempts":0,"ts":"2026-09-27T22:27:27Z"}}"#,
                &sha_seeded.as_str()[..16]
            ),
        ]
        .join("\n");

        let parsed_aliases = extract_shadow_aliases(&shadow_jsonl).unwrap();
        let seen_idx = parse_mailwatch_seen_jsonl(&seen_raw, &parsed_aliases).unwrap();
        // 8 raw lines collapse to 6 normalized keys (2 overwrites).
        assert_eq!(seen_idx.len(), 6);
        assert_eq!(
            seen_idx.outcome_for("tally", &sha_rung.as_str()[..16], &parsed_aliases),
            Some("rung")
        );
        assert_eq!(
            seen_idx.outcome_for("genga", &sha_unrung.as_str()[..16], &parsed_aliases),
            Some("unrung:gave-up")
        );

        let cmp =
            compare_shadow_with_mailwatch_seen(&shadow_jsonl, &seen_idx, &parsed_aliases).unwrap();
        assert_eq!(cmp.seen_entries, 6);
        assert_eq!(cmp.shadow_ring_referents, 7);
        assert_eq!(cmp.agreed_rung, 2); // rung + unknown
        assert_eq!(cmp.own_knocker_in_mailwatch, 1);
        assert_eq!(cmp.deferred_or_unrung_in_mailwatch, 2); // unrung:gave-up + defer:busy
        assert_eq!(cmp.other_in_mailwatch, 1); // seeded
        assert_eq!(cmp.absent_in_mailwatch, 1);

        // Malformed seen.jsonl lines return Err.
        assert!(parse_mailwatch_seen_jsonl("{bad", &parsed_aliases).is_err());
        assert!(
            parse_mailwatch_seen_jsonl(r#"{"key":"no-colon","outcome":"rung"}"#, &parsed_aliases)
                .is_err()
        );
    }
}
