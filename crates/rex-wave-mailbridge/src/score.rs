//! Gate 2 scorer comparing `--shadow-read-only` decisions against ground-truth
//! `acks.jsonl` receipts by `(canonical_seat, sha256)`.

use crate::acks::{AckIndex, AliasMap};
use crate::shadow::RingDecision;
use rex_wave_api::{ApiError, InvalidId, SeatName, Sha256Hex};
use std::collections::BTreeSet;

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
                report.referents_rung += 1;
                if acks.is_acked_by_digest(seat.as_str(), referent.sha256(), aliases) {
                    report.ring_referents_confirmed_acked += 1;
                } else {
                    report.ring_referents_pending_ack += 1;
                }
            }
            RingDecision::RingDigest {
                seat, referents, ..
            } => {
                report.rings_emitted += 1;
                for referent in referents {
                    report.referents_rung += 1;
                    if acks.is_acked_by_digest(seat.as_str(), referent.sha256(), aliases) {
                        report.ring_referents_confirmed_acked += 1;
                    } else {
                        report.ring_referents_pending_ack += 1;
                    }
                }
            }
            RingDecision::RaiseUnrung {
                seat, referents, ..
            } => {
                report.unrung_alerts += 1;
                unrung_seats.insert(seat.clone());
                for referent in referents {
                    if acks.is_acked_by_digest(seat.as_str(), referent.sha256(), aliases) {
                        report.unrung_referents_already_acked += 1;
                    } else {
                        report.unrung_referents_caught += 1;
                    }
                }
            }
        }
    }

    report.seats_with_unrung = unrung_seats.into_iter().collect();
    report
}

/// Retrospective Gate 2 score comparing shadow decisions produced against
/// `acks_before` with later ground-truth receipts in `acks_after`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplayScoreReport {
    pub true_positive_rings: usize,
    pub duplicate_already_acked_rings: usize,
    pub pending_unacked_rings: usize,
    pub caught_unrung_misses: usize,
    pub unrung_later_acked: usize,
    pub skipped_already_acked: usize,
    pub skipped_dormant_or_retired: usize,
    pub mutated_after_ack_flags: usize,
}

impl ReplayScoreReport {
    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "true_positive_rings": self.true_positive_rings,
            "duplicate_already_acked_rings": self.duplicate_already_acked_rings,
            "pending_unacked_rings": self.pending_unacked_rings,
            "caught_unrung_misses": self.caught_unrung_misses,
            "unrung_later_acked": self.unrung_later_acked,
            "skipped_already_acked": self.skipped_already_acked,
            "skipped_dormant_or_retired": self.skipped_dormant_or_retired,
            "mutated_after_ack_flags": self.mutated_after_ack_flags,
        })
    }
}

/// Evaluates retrospective precision and recall of shadow decisions against
/// before/after `acks.jsonl` snapshots.
pub fn score_replay_against_ground_truth(
    decisions: &[RingDecision],
    acks_before: &AckIndex,
    acks_after: &AckIndex,
    aliases: &AliasMap,
) -> ReplayScoreReport {
    let mut report = ReplayScoreReport::default();

    for decision in decisions {
        match decision {
            RingDecision::SkipAlreadyAcked { .. } => report.skipped_already_acked += 1,
            RingDecision::SkipDormantOrRetired { .. } => report.skipped_dormant_or_retired += 1,
            RingDecision::FlagMutatedAfterAck { .. } => report.mutated_after_ack_flags += 1,
            RingDecision::Ring { seat, referent, .. } => {
                if acks_before.is_acked_by_digest(seat.as_str(), referent.sha256(), aliases) {
                    report.duplicate_already_acked_rings += 1;
                } else if acks_after.is_acked_by_digest(seat.as_str(), referent.sha256(), aliases) {
                    report.true_positive_rings += 1;
                } else {
                    report.pending_unacked_rings += 1;
                }
            }
            RingDecision::RingDigest {
                seat, referents, ..
            } => {
                for referent in referents {
                    if acks_before.is_acked_by_digest(seat.as_str(), referent.sha256(), aliases) {
                        report.duplicate_already_acked_rings += 1;
                    } else if acks_after.is_acked_by_digest(
                        seat.as_str(),
                        referent.sha256(),
                        aliases,
                    ) {
                        report.true_positive_rings += 1;
                    } else {
                        report.pending_unacked_rings += 1;
                    }
                }
            }
            RingDecision::RaiseUnrung {
                seat, referents, ..
            } => {
                for referent in referents {
                    if acks_after.is_acked_by_digest(seat.as_str(), referent.sha256(), aliases) {
                        report.unrung_later_acked += 1;
                    } else {
                        report.caught_unrung_misses += 1;
                    }
                }
            }
            RingDecision::SkipReceiptOnly { .. }
            | RingDecision::Defer { .. }
            | RingDecision::RefuseMissingReferent { .. } => {}
        }
    }

    report
}

/// Scores a JSONL shadow log produced by `ShadowPassReport::to_jsonl()` against
/// `acks.jsonl`, matching every emitted referent by `(canonical_seat, sha256)`.
pub fn score_shadow_jsonl_against_acks(
    shadow_jsonl: &str,
    acks: &AckIndex,
    aliases: &AliasMap,
) -> Result<ShadowScoreReport, ApiError> {
    let mut report = ShadowScoreReport::default();
    let mut unrung_seats = BTreeSet::new();

    for (idx, raw) in shadow_jsonl.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let val: serde_json::Value = serde_json::from_str(line).map_err(|e| {
            ApiError::Invalid(InvalidId {
                what: "shadow_jsonl".into(),
                reason: format!("line {}: {e}", idx + 1),
            })
        })?;
        let obj = val.as_object().ok_or_else(|| {
            ApiError::Invalid(InvalidId {
                what: "shadow_jsonl".into(),
                reason: format!("line {}: expected JSON object", idx + 1),
            })
        })?;
        let kind = obj
            .get("decision")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                ApiError::Invalid(InvalidId {
                    what: "shadow_jsonl".into(),
                    reason: format!("line {}: missing 'decision' field", idx + 1),
                })
            })?;
        let seat_str = obj.get("seat").and_then(|v| v.as_str()).unwrap_or("");
        report.total_decisions += 1;

        match kind {
            "skip_already_acked" => report.skipped_already_acked += 1,
            "skip_receipt_only" => report.skipped_receipt_only += 1,
            "skip_dormant_or_retired" => report.skipped_dormant_or_retired += 1,
            "flag_mutated_after_ack" => report.mutated_after_ack_flags += 1,
            "defer" => report.deferred += 1,
            "refuse_missing_referent" => report.missing_referent_refusals += 1,
            "ring" => {
                report.rings_emitted += 1;
                report.referents_rung += 1;
                if let Some(sha) = obj
                    .get("referent")
                    .and_then(|r| r.get("sha256"))
                    .and_then(|s| s.as_str())
                    .and_then(|s| Sha256Hex::parse(s).ok())
                    && acks.is_acked_by_digest(seat_str, &sha, aliases)
                {
                    report.ring_referents_confirmed_acked += 1;
                } else {
                    report.ring_referents_pending_ack += 1;
                }
            }
            "ring_digest" => {
                report.rings_emitted += 1;
                if let Some(arr) = obj.get("referents").and_then(|v| v.as_array()) {
                    for item in arr {
                        report.referents_rung += 1;
                        if let Some(sha) = item
                            .get("sha256")
                            .and_then(|s| s.as_str())
                            .and_then(|s| Sha256Hex::parse(s).ok())
                            && acks.is_acked_by_digest(seat_str, &sha, aliases)
                        {
                            report.ring_referents_confirmed_acked += 1;
                        } else {
                            report.ring_referents_pending_ack += 1;
                        }
                    }
                }
            }
            "raise_unrung" => {
                report.unrung_alerts += 1;
                if let Ok(seat) = SeatName::parse(seat_str) {
                    unrung_seats.insert(seat);
                }
                if let Some(arr) = obj.get("referents").and_then(|v| v.as_array()) {
                    for item in arr {
                        if let Some(sha) = item
                            .get("sha256")
                            .and_then(|s| s.as_str())
                            .and_then(|s| Sha256Hex::parse(s).ok())
                            && acks.is_acked_by_digest(seat_str, &sha, aliases)
                        {
                            report.unrung_referents_already_acked += 1;
                        } else {
                            report.unrung_referents_caught += 1;
                        }
                    }
                }
            }
            other => {
                return Err(ApiError::Invalid(InvalidId {
                    what: "shadow_jsonl".into(),
                    reason: format!("line {}: unknown decision '{other}'", idx + 1),
                }));
            }
        }
    }

    report.seats_with_unrung = unrung_seats.into_iter().collect();
    Ok(report)
}
