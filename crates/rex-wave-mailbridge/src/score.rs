//! Gate 2 scorer comparing `--shadow-read-only` decisions against ground-truth
//! `acks.jsonl` receipts by `(canonical_seat, sha256)`, plus Gate 1 fixture
//! coverage verification (`cases.jsonl`).

use crate::acks::{AckIndex, AliasMap};
use crate::shadow::{RingDecision, RingReferent};
use rex_wave_api::presence::HouseProfile;
use rex_wave_api::{SeatName, Sha256Hex};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The 10 canonical Gate 1 mail-routing cases mapped to the exact Rust
/// integration test function name in
/// `crates/rex-wave-mailbridge/tests/presence_and_shadow_mail.rs` that verifies
/// each case's behavior.
pub const GATE1_CASE_TESTS: &[(&str, &str)] = &[
    ("seat-stale-six-days", "gate1_case_01_seat_stale_six_days"),
    ("carbon-copy-misroute", "gate1_case_02_carbon_copy_misroute"),
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
    "seat-stale-six-days",
    "carbon-copy-misroute",
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
    check_gate1_fixtures_with_profile(cases_jsonl, &HouseProfile::default())
}

/// Like [`check_gate1_fixtures`], resolving any deployment-specific legacy
/// fixture IDs through [`HouseProfile::fixture_id_aliases`] before matching
/// [`GATE1_CASE_IDS`].
pub fn check_gate1_fixtures_with_profile(
    cases_jsonl: &str,
    house: &HouseProfile,
) -> Result<FixtureCoverageReport, String> {
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
        let raw_id = val
            .get("id")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("fixtures line {}: missing non-empty `id`", idx + 1))?;
        let id = house.resolve_fixture_id(raw_id);
        if seen.insert(id.to_owned()) {
            case_ids.push(id.to_owned());
            if !expected.contains(id) {
                unknown_case_ids.push(raw_id.to_owned());
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

/// Referent-level accumulator shared by [`score_shadow_against_acks`] and
/// [`score_shadow_jsonl`] so both count identically.
///
/// A `--watch` log is a sequence of transitions, so the same referent can
/// appear on several lines. Rung referents count once per
/// `(canonical_seat, sha256)`; a ring logged on an early pass survives a later
/// `skip_already_acked`. Unrung referents also count once, and only when that
/// referent never rang anywhere in the log (resolved at the end, because the
/// ring may come after the unrung line).
struct ScoreAccumulator<'a> {
    report: ShadowScoreReport,
    acks: &'a AckIndex,
    aliases: &'a AliasMap,
    rung: BTreeSet<(String, Sha256Hex)>,
    unrung: BTreeMap<(String, Sha256Hex), SeatName>,
    unrung_seats: BTreeSet<SeatName>,
}

impl<'a> ScoreAccumulator<'a> {
    fn new(acks: &'a AckIndex, aliases: &'a AliasMap) -> Self {
        Self {
            report: ShadowScoreReport::default(),
            acks,
            aliases,
            rung: BTreeSet::new(),
            unrung: BTreeMap::new(),
            unrung_seats: BTreeSet::new(),
        }
    }

    fn ring_line<'s>(&mut self, seat: &SeatName, shas: impl IntoIterator<Item = &'s Sha256Hex>) {
        let canon = self.aliases.canonicalize(seat.as_str());
        let mut fresh = false;
        for sha in shas {
            if !self.rung.insert((canon.clone(), sha.clone())) {
                continue;
            }
            fresh = true;
            self.report
                .record_ring_referent(seat, sha, self.acks, self.aliases);
        }
        if fresh {
            self.report.rings_emitted += 1;
        }
    }

    fn unrung_line<'s>(&mut self, seat: &SeatName, shas: impl IntoIterator<Item = &'s Sha256Hex>) {
        let canon = self.aliases.canonicalize(seat.as_str());
        self.unrung_seats.insert(seat.clone());
        let mut fresh = false;
        for sha in shas {
            if let std::collections::btree_map::Entry::Vacant(e) =
                self.unrung.entry((canon.clone(), sha.clone()))
            {
                e.insert(seat.clone());
                fresh = true;
            }
        }
        if fresh {
            self.report.unrung_alerts += 1;
        }
    }

    fn finish(mut self) -> ShadowScoreReport {
        for ((canon, sha), seat) in &self.unrung {
            if self.rung.contains(&(canon.clone(), sha.clone())) {
                continue;
            }
            self.report
                .record_unrung_referent(seat, sha, self.acks, self.aliases);
        }
        self.report.seats_with_unrung = self.unrung_seats.into_iter().collect();
        assert!(self.report.referents_rung == self.rung.len());
        assert!(
            self.report.unrung_referents_caught + self.report.unrung_referents_already_acked
                <= self.unrung.len()
        );
        self.report
    }
}

/// Scores in-memory shadow [`RingDecision`]s against ground-truth `acks.jsonl`.
pub fn score_shadow_against_acks(
    decisions: &[RingDecision],
    acks: &AckIndex,
    aliases: &AliasMap,
) -> ShadowScoreReport {
    let mut acc = ScoreAccumulator::new(acks, aliases);
    acc.report.total_decisions = decisions.len();
    let report = &mut acc;

    for decision in decisions {
        match decision {
            RingDecision::SkipAlreadyAcked { .. } => report.report.skipped_already_acked += 1,
            RingDecision::SkipReceiptOnly { .. } => report.report.skipped_receipt_only += 1,
            RingDecision::SkipDormantOrRetired { .. } => {
                report.report.skipped_dormant_or_retired += 1;
            }
            RingDecision::SkipPreHorizonHistory { .. } => report.report.skipped_pre_horizon += 1,
            RingDecision::SkipOperatorSurface { .. } => {
                report.report.skipped_operator_surface += 1;
            }
            RingDecision::SkipNonSeatTarget { .. } => report.report.skipped_non_seat_target += 1,
            RingDecision::FlagMutatedAfterAck { .. } => report.report.mutated_after_ack_flags += 1,
            RingDecision::Defer { .. } => report.report.deferred += 1,
            RingDecision::RefuseMissingReferent { .. } => {
                report.report.missing_referent_refusals += 1;
            }
            RingDecision::Ring { seat, referent, .. } => {
                report.ring_line(seat, [referent.sha256()]);
            }
            RingDecision::RingDigest {
                seat, referents, ..
            } => report.ring_line(seat, referents.iter().map(RingReferent::sha256)),
            RingDecision::RaiseUnrung {
                seat, referents, ..
            } => report.unrung_line(seat, referents.iter().map(RingReferent::sha256)),
        }
    }

    acc.finish()
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
/// matching [`score_shadow_against_acks`] exactly. Also accepts append-only
/// `mail shadow --watch` transition logs, including several concatenated
/// logs: referents are deduplicated by `(canonical_seat, sha256)`.
pub fn score_shadow_jsonl(
    shadow_jsonl: &str,
    acks: &AckIndex,
    aliases: &AliasMap,
) -> Result<ShadowScoreReport, String> {
    let mut acc = ScoreAccumulator::new(acks, aliases);

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

        let report = &mut acc.report;
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
                acc.ring_line(&seat, [&sha]);
            }
            "ring_digest" => {
                let shas = extract_referents_shas(obj, line_no)?;
                acc.ring_line(&seat, &shas);
            }
            "raise_unrung" => {
                let shas = extract_referents_shas(obj, line_no)?;
                acc.unrung_line(&seat, &shas);
            }
            other => {
                return Err(format!(
                    "shadow line {line_no}: unknown decision kind '{other}'"
                ));
            }
        }
    }

    Ok(acc.finish())
}

/// Parsed ring logs (`mailwatch.py`'s `.watchers/mailwatch-state/seen.jsonl`
/// and the T3 doorbell's `seen.jsonl`) indexed by normalized raw key and by
/// canonical `(seat, sha16)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MailwatchSeenIndex {
    by_raw_key: BTreeMap<String, SeenRow>,
    by_seat_sha16: BTreeMap<(String, String), String>,
}

/// One normalized `seen.jsonl` row. `seat` is `None` for mailwatch boxes that
/// are not a `shared-<seat>` or `to-<seat>` lane.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SeenRow {
    seat: Option<String>,
    sha16: String,
    outcome: String,
}

/// How a ring watcher's `outcome` string compares with a shadow ring. The
/// order is the precedence used when lanes disagree for one `(seat, sha16)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SeenOutcomeClass {
    Other,
    DeferredOrUnrung,
    OwnKnocker,
    Rung,
}

/// Single classifier for both watcher formats. `mailwatch.py` writes `rung`,
/// `unknown`, `own-knocker`, `defer:*`, `unrung:*`; the T3 doorbell writes
/// `rung`, `outcome_unknown`, `failed*`, `gave-up`, `seeded`. An unknown
/// delivery outcome counts as rung in both, matching the watchers themselves.
fn classify_seen_outcome(outcome: &str) -> SeenOutcomeClass {
    match outcome {
        "rung" | "unknown" | "outcome_unknown" => SeenOutcomeClass::Rung,
        "own-knocker" => SeenOutcomeClass::OwnKnocker,
        "gave-up" => SeenOutcomeClass::DeferredOrUnrung,
        o if o.starts_with("defer") || o.starts_with("unrung") || o.starts_with("failed") => {
            SeenOutcomeClass::DeferredOrUnrung
        }
        _ => SeenOutcomeClass::Other,
    }
}

impl MailwatchSeenIndex {
    /// Number of unique normalized keys across all ingested sources.
    pub fn len(&self) -> usize {
        self.by_raw_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_raw_key.is_empty()
    }

    /// Returns the winning outcome for `(seat, sha16)` across `shared-<alias>`,
    /// `to-<alias>` and doorbell rows after canonicalizing `seat`.
    pub fn outcome_for(&self, seat: &str, sha16: &str, aliases: &AliasMap) -> Option<&str> {
        let canon = aliases.canonicalize(seat);
        self.by_seat_sha16
            .get(&(canon, sha16.trim().to_ascii_lowercase()))
            .map(String::as_str)
    }
}

/// Cross-check summary comparing `rex-wave` shadow rings (`ring` + `ring_digest`)
/// against ring watcher `seen.jsonl` logs.
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

fn seen_field<'a>(
    obj: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Option<&'a str> {
    obj.get(field)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// Parses one `seen.jsonl` line into its normalized key and row.
///
/// - mailwatch: `{"key":"<box>[/file]:<sha16>","outcome":..}`. Legacy
///   `box/file:sha16` keys normalize to `box:sha16` (`mailwatch.py` `load_seen()`).
/// - T3 doorbell: `{"sha16":"<16hex>","seat":"<seat>","outcome":..}`, keyed
///   `t3-doorbell/<seat>:<sha16>` so it never collides with a mailwatch box.
fn parse_seen_line(
    obj: &serde_json::Map<String, serde_json::Value>,
    at: &str,
) -> Result<(String, SeenRow), String> {
    let outcome = seen_field(obj, "outcome")
        .ok_or_else(|| format!("{at}: missing `outcome` field"))?
        .to_owned();
    if let Some(key) = seen_field(obj, "key") {
        let Some((left, sha_raw)) = key.rsplit_once(':') else {
            return Err(format!("{at}: key `{key}` missing `:` separator"));
        };
        let box_part = left.split('/').next().unwrap_or(left).trim();
        let sha16 = sha_raw.trim().to_ascii_lowercase();
        if box_part.is_empty() || sha16.is_empty() {
            return Err(format!("{at}: invalid key `{key}`"));
        }
        let seat = box_part
            .strip_prefix("shared-")
            .or_else(|| box_part.strip_prefix("to-"))
            .map(str::to_owned);
        let norm_key = format!("{box_part}:{sha16}");
        return Ok((
            norm_key,
            SeenRow {
                seat,
                sha16,
                outcome,
            },
        ));
    }
    let (Some(sha_raw), Some(seat_raw)) = (seen_field(obj, "sha16"), seen_field(obj, "seat"))
    else {
        return Err(format!(
            "{at}: expected mailwatch `key` or t3-doorbell `sha16` + `seat` fields"
        ));
    };
    let sha16 = sha_raw.to_ascii_lowercase();
    if sha16.len() != 16 || !sha16.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!("{at}: `sha16` '{sha_raw}' is not 16 hex chars"));
    }
    let seat = seat_raw.trim_start_matches('@').to_ascii_lowercase();
    let norm_key = format!("t3-doorbell/{seat}:{sha16}");
    Ok((
        norm_key,
        SeenRow {
            seat: Some(seat),
            sha16,
            outcome,
        },
    ))
}

/// Parses one or more ring watcher `seen.jsonl` sources (`(label, text)`, in
/// order) into one merged index. Within a normalized key the later line wins;
/// across lanes for one canonical `(seat, sha16)` the strongest outcome wins
/// (rung > own-knocker > deferred/unrung > other). Errors name the source label.
pub fn parse_mailwatch_seen_sources(
    sources: &[(&str, &str)],
    aliases: &AliasMap,
) -> Result<MailwatchSeenIndex, String> {
    let mut by_raw_key: BTreeMap<String, SeenRow> = BTreeMap::new();

    for (label, raw) in sources {
        for (idx, line_raw) in raw.lines().enumerate() {
            let at = format!("{label} line {}", idx + 1);
            let line = line_raw.trim();
            if line.is_empty() {
                continue;
            }
            let val: serde_json::Value =
                serde_json::from_str(line).map_err(|e| format!("{at}: invalid JSON: {e}"))?;
            let obj = val
                .as_object()
                .ok_or_else(|| format!("{at}: expected JSON object"))?;
            let (norm_key, row) = parse_seen_line(obj, &at)?;
            by_raw_key.insert(norm_key, row);
        }
    }

    let mut by_seat_sha16: BTreeMap<(String, String), String> = BTreeMap::new();
    for row in by_raw_key.values() {
        let Some(raw_seat) = row.seat.as_deref() else {
            continue;
        };
        let canon = aliases.canonicalize(raw_seat);
        if canon.is_empty() {
            continue;
        }
        let map_key = (canon, row.sha16.clone());
        match by_seat_sha16.get_mut(&map_key) {
            Some(prev) => {
                if classify_seen_outcome(&row.outcome) >= classify_seen_outcome(prev) {
                    *prev = row.outcome.clone();
                }
            }
            None => {
                by_seat_sha16.insert(map_key, row.outcome.clone());
            }
        }
    }

    Ok(MailwatchSeenIndex {
        by_raw_key,
        by_seat_sha16,
    })
}

/// Parses one `seen.jsonl` (mailwatch or T3 doorbell format). See
/// [`parse_mailwatch_seen_sources`] for merging several files.
pub fn parse_mailwatch_seen_jsonl(
    raw: &str,
    aliases: &AliasMap,
) -> Result<MailwatchSeenIndex, String> {
    parse_mailwatch_seen_sources(&[("mailwatch seen", raw)], aliases)
}

/// Compares `ring` and `ring_digest` referents emitted in `shadow_jsonl`
/// against a parsed [`MailwatchSeenIndex`]. Each `(canonical_seat, sha256)`
/// referent is compared once even if it appears on several lines (watch
/// logs, concatenated logs). `rung`, `unknown` and `outcome_unknown` all count
/// as `agreed_rung`.
pub fn compare_shadow_with_mailwatch_seen(
    shadow_jsonl: &str,
    seen: &MailwatchSeenIndex,
    aliases: &AliasMap,
) -> Result<MailwatchComparisonReport, String> {
    let mut report = MailwatchComparisonReport {
        seen_entries: seen.len(),
        ..MailwatchComparisonReport::default()
    };
    let mut compared: BTreeSet<(String, Sha256Hex)> = BTreeSet::new();

    let mut record = |seat: &SeatName, sha: &Sha256Hex| {
        if !compared.insert((aliases.canonicalize(seat.as_str()), sha.clone())) {
            return;
        }
        report.shadow_ring_referents += 1;
        let sha16 = &sha.as_str()[..16];
        match seen
            .outcome_for(seat.as_str(), sha16, aliases)
            .map(classify_seen_outcome)
        {
            Some(SeenOutcomeClass::Rung) => report.agreed_rung += 1,
            Some(SeenOutcomeClass::OwnKnocker) => report.own_knocker_in_mailwatch += 1,
            Some(SeenOutcomeClass::DeferredOrUnrung) => {
                report.deferred_or_unrung_in_mailwatch += 1;
            }
            Some(SeenOutcomeClass::Other) => report.other_in_mailwatch += 1,
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
        if !matches!(decision, "ring" | "ring_digest") {
            continue;
        }
        let seat_raw = obj
            .get("seat")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("shadow line {line_no}: missing `seat` field"))?;
        let seat = SeatName::parse(seat_raw)
            .map_err(|e| format!("shadow line {line_no}: invalid `seat` '{seat_raw}': {e}"))?;
        if decision == "ring" {
            let ref_val = obj
                .get("referent")
                .ok_or_else(|| format!("shadow line {line_no}: missing `referent`"))?;
            record(&seat, &extract_referent_sha(ref_val, line_no)?);
        } else {
            for sha in &extract_referents_shas(obj, line_no)? {
                record(&seat, sha);
            }
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

        let one_line = r#"{"id":"seat-stale-six-days"}"#;
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

        let mut aliased_house = HouseProfile::default();
        aliased_house.fixture_id_aliases.insert(
            "legacy-case-01".to_owned(),
            "seat-stale-six-days".to_owned(),
        );
        let aliased_jsonl = all_ten.replace("seat-stale-six-days", "legacy-case-01");
        assert!(
            !check_gate1_fixtures(&aliased_jsonl)
                .unwrap()
                .fixture_ids_registered()
        );
        let mapped = check_gate1_fixtures_with_profile(&aliased_jsonl, &aliased_house).unwrap();
        assert!(mapped.fixture_ids_registered());
        assert!(mapped.missing_case_ids.is_empty());
        assert!(mapped.unknown_case_ids.is_empty());
    }

    #[test]
    fn shadow_alias_header_propagates_to_offline_score() {
        let sha = Sha256Hex::parse("a".repeat(64)).unwrap();
        let aliases = AliasMap::from_pairs([("carol-5.2", "carol")]);
        let report = ShadowPassReport {
            letters_scanned: 1,
            acks_report: Default::default(),
            decisions: vec![RingDecision::Ring {
                seat: SeatName::parse("carol").unwrap(),
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
        assert_eq!(parsed_aliases.canonicalize("Carol-5.2"), "carol");

        let acks_raw =
            format!(r#"{{"acked_by":"Carol-5.2","letter":"mail/01.md","sha256":"{sha}"}}"#);
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

        let aliases = AliasMap::from_pairs([("muse", "heidi"), ("carol_old", "carol")]);
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
                    seat: SeatName::parse("carol").unwrap(),
                    incarnation_id: inc.clone(),
                    channel: ch.clone(),
                    referent: RingReferent::new("mail/01.md", sha_rung.clone()).unwrap(),
                    time_basis: TimeBasis::LetterIdMs,
                },
                RingDecision::Ring {
                    seat: SeatName::parse("heidi").unwrap(),
                    incarnation_id: inc.clone(),
                    channel: ch.clone(),
                    referent: RingReferent::new("to-muse/02.md", sha_unknown.clone()).unwrap(),
                    time_basis: TimeBasis::LetterIdMs,
                },
                RingDecision::Ring {
                    seat: SeatName::parse("eve").unwrap(),
                    incarnation_id: inc.clone(),
                    channel: ch.clone(),
                    referent: RingReferent::new("mail/03.md", sha_own.clone()).unwrap(),
                    time_basis: TimeBasis::LetterIdMs,
                },
                RingDecision::RingDigest {
                    seat: SeatName::parse("dave").unwrap(),
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
                r#"{{"key":"shared-carol_old:{}","outcome":"defer:missing","attempts":1,"ts":"2026-11-21T19:00:00Z"}}"#,
                &sha_rung.as_str()[..16]
            ),
            format!(
                r#"{{"key":"shared-carol_old:{}","outcome":"rung","attempts":2,"ts":"2026-11-21T19:02:00Z"}}"#,
                &sha_rung.as_str()[..16]
            ),
            // Legacy box/file:sha16 format with alias `to-muse` + outcome `unknown` (counts as rung)
            format!(
                r#"{{"key":"to-muse/legacy-file.md:{}","outcome":"unknown","attempts":1,"ts":"2026-11-21T19:05:00Z"}}"#,
                &sha_unknown.as_str()[..16]
            ),
            // own-knocker
            format!(
                r#"{{"key":"shared-eve:{}","outcome":"own-knocker","attempts":1,"ts":"2026-11-21T19:06:00Z"}}"#,
                &sha_own.as_str()[..16]
            ),
            // Earlier defer:missing overwritten by unrung:gave-up
            format!(
                r#"{{"key":"shared-dave:{}","outcome":"defer:missing","attempts":29,"ts":"2026-11-21T19:07:00Z"}}"#,
                &sha_unrung.as_str()[..16]
            ),
            format!(
                r#"{{"key":"shared-dave:{}","outcome":"unrung:gave-up","attempts":30,"ts":"2026-11-21T19:09:00Z"}}"#,
                &sha_unrung.as_str()[..16]
            ),
            // Still deferred
            format!(
                r#"{{"key":"shared-dave:{}","outcome":"defer:busy","attempts":3,"ts":"2026-11-21T19:10:00Z"}}"#,
                &sha_defer.as_str()[..16]
            ),
            // Seeded (other)
            format!(
                r#"{{"key":"to-dave/old.md:{}","outcome":"seeded","attempts":0,"ts":"2026-09-27T22:27:27Z"}}"#,
                &sha_seeded.as_str()[..16]
            ),
        ]
        .join("\n");

        let parsed_aliases = extract_shadow_aliases(&shadow_jsonl).unwrap();
        let seen_idx = parse_mailwatch_seen_jsonl(&seen_raw, &parsed_aliases).unwrap();
        // 8 raw lines collapse to 6 normalized keys (2 overwrites).
        assert_eq!(seen_idx.len(), 6);
        assert_eq!(
            seen_idx.outcome_for("carol", &sha_rung.as_str()[..16], &parsed_aliases),
            Some("rung")
        );
        assert_eq!(
            seen_idx.outcome_for("dave", &sha_unrung.as_str()[..16], &parsed_aliases),
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
