//! Gate 2 scorer comparing `--shadow-read-only` decisions against ground-truth
//! `acks.jsonl` receipts by `(canonical_seat, sha256)`.

use crate::acks::{AckIndex, AliasMap};
use crate::shadow::RingDecision;
use rex_wave_api::SeatName;
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
