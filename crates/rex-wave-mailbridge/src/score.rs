//! Gate 2 scorer comparing `--shadow-read-only` decisions against ground-truth
//! `acks.jsonl` receipts by `(canonical_seat, sha256)`, plus Gate 1 fixture
//! coverage verification (`cases.jsonl`).

use crate::acks::{AckIndex, AliasMap};
use crate::shadow::RingDecision;
use rex_wave_api::{SeatName, Sha256Hex};
use std::collections::BTreeSet;

/// The 10 canonical Gate 1 mail-routing cases verified in `presence_and_shadow_mail`.
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

/// Coverage report comparing a Gate 1 `cases.jsonl` fixture file against [`GATE1_CASE_IDS`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FixtureCoverageReport {
    pub total_cases: usize,
    pub covered_cases: usize,
    pub case_ids: Vec<String>,
    pub uncovered_case_ids: Vec<String>,
}

impl FixtureCoverageReport {
    pub fn all_covered(&self) -> bool {
        self.total_cases > 0 && self.uncovered_case_ids.is_empty()
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "total_cases": self.total_cases,
            "covered_cases": self.covered_cases,
            "all_covered": self.all_covered(),
            "case_ids": self.case_ids,
            "uncovered_case_ids": self.uncovered_case_ids,
        })
    }
}

/// Parses a Gate 1 `cases.jsonl` fixture document and reports coverage against [`GATE1_CASE_IDS`].
pub fn check_gate1_fixtures(cases_jsonl: &str) -> Result<FixtureCoverageReport, String> {
    let known: BTreeSet<&str> = GATE1_CASE_IDS.iter().copied().collect();
    let mut case_ids = Vec::new();
    let mut seen = BTreeSet::new();
    let mut uncovered = Vec::new();

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
            if !known.contains(id) {
                uncovered.push(id.to_owned());
            }
        }
    }

    let total_cases = case_ids.len();
    let covered_cases = total_cases.saturating_sub(uncovered.len());
    Ok(FixtureCoverageReport {
        total_cases,
        covered_cases,
        case_ids,
        uncovered_case_ids: uncovered,
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

/// Scores a JSONL shadow log produced by `ShadowPassReport::to_jsonl` against
/// ground-truth `acks.jsonl`, matching [`score_shadow_against_acks`] exactly.
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
