//! Formal state-machine conformance suite for `specs/presence/PresenceLeases.tla`
//! and `specs/mail/{MailHub,MailStore,MailMachines}.tla`.
//!
//! Verifies `L3a`/`L3b`/`L3c` epoch + expiry fencing on `Presence`, single-live-per-lane
//! invariants, operator-surface exclusion, `decide_mail_rings_for_seat` safety invariants
//! (`NoDoubleRing`, `NoRingAfterAck`, `EditNeverRingable`, `LoudGiveUp`, `NoFalseGiveUp`),
//! and `ShadowWatchTracker` live-vs-hydrated restart equivalence.

#![allow(clippy::disallowed_methods)]

use proptest::prelude::*;
use rex_wave_api::mail::*;
use rex_wave_api::presence::*;
use rex_wave_api::*;
use rex_wave_mailbridge::{
    AliasMap, DeferReason, IncarnationAffinity, LetterCandidate, MailBridge, MailState, Presence,
    PresenceState, Protection, ReadOnlyInterchange, RetryEntry, RetryState, RingDecision,
    RingPolicy, SandboxRoot, ShadowWatchTracker, TimeBasis, decide_mail_rings_for_seat,
    parse_acks_jsonl, rebuild_mail_from_mirror, run_shadow_pass, scan_object_blobs,
    scan_working_tree_dir,
};
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta, Service};
use rex_wave_store::Journal;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

fn sha_of(bytes: &[u8]) -> Sha256Hex {
    let mut h = Sha256::new();
    h.update(bytes);
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    Sha256Hex::parse(hex).expect("sha256")
}

#[derive(Clone, Debug)]
enum PresenceAction {
    BindSocket {
        seat_idx: u8,
        inc_idx: u8,
        conn_id: u64,
        lane_opt: Option<u8>,
        takeover: bool,
    },
    BindLease {
        seat_idx: u8,
        inc_idx: u8,
        sess_idx: u8,
        ttl_ms: u64,
        takeover: bool,
    },
    Heartbeat {
        seat_idx: u8,
        inc_idx: u8,
        ttl_ms: u64,
    },
    CaptureDisconnect {
        seat_idx: u8,
        inc_idx: u8,
    },
    RunQueuedDisconnect {
        queue_idx: usize,
    },
    CaptureExpireSweep,
    RunQueuedExpire {
        queue_idx: usize,
    },
    AdvanceClock {
        delta_ms: u64,
    },
    TryBindOperator,
}

fn presence_action_strategy() -> impl Strategy<Value = PresenceAction> {
    prop_oneof![
        (
            0u8..2,
            0u8..3,
            1u64..=4,
            proptest::option::of(0u8..2),
            any::<bool>()
        )
            .prop_map(|(seat_idx, inc_idx, conn_id, lane_opt, takeover)| {
                PresenceAction::BindSocket {
                    seat_idx,
                    inc_idx,
                    conn_id,
                    lane_opt,
                    takeover,
                }
            }),
        (0u8..2, 0u8..3, 1u8..=3, 50u64..=250, any::<bool>()).prop_map(
            |(seat_idx, inc_idx, sess_idx, ttl_ms, takeover)| PresenceAction::BindLease {
                seat_idx,
                inc_idx,
                sess_idx,
                ttl_ms,
                takeover,
            }
        ),
        (0u8..2, 0u8..3, 100u64..=400).prop_map(|(seat_idx, inc_idx, ttl_ms)| {
            PresenceAction::Heartbeat {
                seat_idx,
                inc_idx,
                ttl_ms,
            }
        }),
        (0u8..2, 0u8..3).prop_map(|(seat_idx, inc_idx)| PresenceAction::CaptureDisconnect {
            seat_idx,
            inc_idx
        }),
        (0usize..8).prop_map(|queue_idx| PresenceAction::RunQueuedDisconnect { queue_idx }),
        Just(PresenceAction::CaptureExpireSweep),
        (0usize..8).prop_map(|queue_idx| PresenceAction::RunQueuedExpire { queue_idx }),
        (25u64..=180).prop_map(|delta_ms| PresenceAction::AdvanceClock { delta_ms }),
        Just(PresenceAction::TryBindOperator),
    ]
}

fn apply_presence(
    state: &mut PresenceState,
    log: &mut Vec<(PresenceEvent, EntryMeta)>,
    seq: &mut u64,
    now_ms: u64,
    by: Principal,
    cmd: PresenceCommand,
) -> Result<(Vec<Envelope>, WireReply), ApiError> {
    let ctx = DecideCtx::for_test(by.clone(), UnixMs(now_ms));
    let events = Presence::decide_with_probe(state, &cmd, &ctx, |_pid, _start| true)?;
    let mut committed = Vec::with_capacity(events.len());
    for ev in events {
        *seq += 1;
        let meta = EntryMeta {
            seq: Seq(*seq),
            at: UnixMs(now_ms),
            by: by.clone(),
        };
        Presence::evolve(state, &ev, &meta);
        log.push((ev.clone(), meta.clone()));
        committed.push(Envelope {
            stream: StreamId::Presence,
            seq: meta.seq,
            at: meta.at,
            by: by.clone(),
            event: Event::Presence(ev),
        });
    }
    let reply = Presence::reply(state, cmd, &committed);
    Ok((committed, reply))
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn proptest_presence_leases_conformance_matches_tla_invariants(
        actions in prop::collection::vec(presence_action_strategy(), 1..50)
    ) {
        let seats = [
            SeatName::parse("alice").expect("alice"),
            SeatName::parse("bob").expect("bob"),
        ];
        let incs = [
            IncarnationId::parse("inc-a").expect("inc"),
            IncarnationId::parse("inc-b").expect("inc"),
            IncarnationId::parse("inc-c").expect("inc"),
        ];
        let lanes = ["lane-main", "lane-aux"];
        let sys = Principal::System {
            part: SystemPart::PresenceMonitor,
        };
        let operator_seat = HouseProfile::default().operator_seat;

        let mut state = PresenceState::default();
        let mut log: Vec<(PresenceEvent, EntryMeta)> = Vec::new();
        let mut seq = 0u64;
        let mut now_ms = 1_000u64;

        let mut queued_disconnects: Vec<(SeatName, IncarnationId, u64)> = Vec::new();
        let mut queued_expires: Vec<PresenceExpire> = Vec::new();

        for action in actions {
            match action {
                PresenceAction::BindSocket {
                    seat_idx,
                    inc_idx,
                    conn_id,
                    lane_opt,
                    takeover,
                } => {
                    let seat = seats[seat_idx as usize].clone();
                    let inc = incs[inc_idx as usize].clone();
                    let lane = lane_opt.map(|i| lanes[i as usize].to_owned());
                    let endpoint = TransportEndpoint::UdsSession {
                        conn_id,
                        client: format!("cli-{conn_id}"),
                    };
                    let prev_binding = state
                        .bindings_for(&seat)
                        .into_iter()
                        .find(|b| b.incarnation_id == inc)
                        .cloned();

                    let res = apply_presence(
                        &mut state,
                        &mut log,
                        &mut seq,
                        now_ms,
                        Principal::Seat { seat: seat.clone() },
                        PresenceCommand::PresenceBind(PresenceBind {
                            seat: seat.clone(),
                            incarnation_id: Some(inc.clone()),
                            machine_id: None,
                            lane,
                            endpoint: endpoint.clone(),
                            liveness: LivenessMode::SocketLifetime,
                            ttl_ms: None,
                            takeover,
                            allow_fork: false,
                            require_canary: false,
                            mail_horizon_at: None,
                            idem: None,
                        }),
                    );
                    if let Ok((_, WireReply::PresenceBind(bound))) = res
                        && let Some(prev) = prev_binding
                        && prev.endpoint != endpoint
                    {
                        // L3a: changing endpoint on refresh MUST bump epoch monotonically.
                        prop_assert!(
                            bound.epoch > prev.epoch,
                            "L3a violated: endpoint change kept epoch={} (prev={})",
                            bound.epoch,
                            prev.epoch
                        );
                    }
                }
                PresenceAction::BindLease {
                    seat_idx,
                    inc_idx,
                    sess_idx,
                    ttl_ms,
                    takeover,
                } => {
                    let seat = seats[seat_idx as usize].clone();
                    let inc = incs[inc_idx as usize].clone();
                    let endpoint = TransportEndpoint::ClaudeSession {
                        pid: 4000 + u32::from(inc_idx),
                        proc_start: "Sat Nov 21 05:00:00 2026".into(),
                        started_at_ms: 1_000,
                        session_id: format!("sess-{sess_idx}"),
                        messaging_socket: format!("/tmp/cc-{inc_idx}.sock").into(),
                        kind: "interactive".into(),
                        name_source: Some("user".into()),
                        display_name: Some("Agent".into()),
                    };
                    let prev_binding = state
                        .bindings_for(&seat)
                        .into_iter()
                        .find(|b| b.incarnation_id == inc)
                        .cloned();
                    let res = apply_presence(
                        &mut state,
                        &mut log,
                        &mut seq,
                        now_ms,
                        Principal::Seat { seat: seat.clone() },
                        PresenceCommand::PresenceBind(PresenceBind {
                            seat: seat.clone(),
                            incarnation_id: Some(inc),
                            machine_id: None,
                            lane: None,
                            endpoint: endpoint.clone(),
                            liveness: LivenessMode::ProcessAndLease,
                            ttl_ms: Some(ttl_ms),
                            takeover,
                            allow_fork: false,
                            require_canary: false,
                            mail_horizon_at: None,
                            idem: None,
                        }),
                    );
                    if let Ok((_, WireReply::PresenceBind(bound))) = res
                        && let Some(prev) = prev_binding
                        && prev.endpoint != endpoint
                    {
                        prop_assert!(bound.epoch > prev.epoch);
                    }
                }
                PresenceAction::Heartbeat {
                    seat_idx,
                    inc_idx,
                    ttl_ms,
                } => {
                    let seat = seats[seat_idx as usize].clone();
                    let inc = incs[inc_idx as usize].clone();
                    drop(apply_presence(
                        &mut state,
                        &mut log,
                        &mut seq,
                        now_ms,
                        Principal::Seat { seat: seat.clone() },
                        PresenceCommand::PresenceHeartbeat(PresenceHeartbeat {
                            seat,
                            incarnation_id: inc,
                            ttl_ms: Some(ttl_ms),
                        }),
                    ));
                }
                PresenceAction::CaptureDisconnect { seat_idx, inc_idx } => {
                    let seat = &seats[seat_idx as usize];
                    let inc = &incs[inc_idx as usize];
                    if let Some(b) = state
                        .bindings_for(seat)
                        .into_iter()
                        .find(|b| &b.incarnation_id == inc && b.status == BindingStatus::Live)
                    {
                        queued_disconnects.push((seat.clone(), inc.clone(), b.epoch));
                    }
                }
                PresenceAction::RunQueuedDisconnect { queue_idx } => {
                    if !queued_disconnects.is_empty() {
                        let idx = queue_idx % queued_disconnects.len();
                        let (seat, inc, captured_epoch) = queued_disconnects.remove(idx);
                        let cur_live = state
                            .bindings_for(&seat)
                            .into_iter()
                            .find(|b| b.incarnation_id == inc && b.status == BindingStatus::Live)
                            .cloned();

                        apply_presence(
                            &mut state,
                            &mut log,
                            &mut seq,
                            now_ms,
                            sys.clone(),
                            PresenceCommand::PresenceUnbind(PresenceUnbind {
                                seat: seat.clone(),
                                incarnation_id: inc.clone(),
                                expected_epoch: Some(captured_epoch),
                                reason: Some("socket eof".into()),
                            }),
                        )
                        .expect("fenced PresenceUnbind succeeds cleanly");

                        if let Some(cur) = cur_live
                            && cur.epoch > captured_epoch
                        {
                            // L3b: StaleDisconnectNeverKillsReboundIncarnation.
                            let after = state
                                .bindings_for(&seat)
                                .into_iter()
                                .find(|b| b.incarnation_id == inc)
                                .expect("binding exists");
                            prop_assert_eq!(after.status, BindingStatus::Live);
                            prop_assert_eq!(after.epoch, cur.epoch);
                        }
                    }
                }
                PresenceAction::CaptureExpireSweep => {
                    let due = state.collect_expired_sweep_probe(UnixMs(now_ms));
                    queued_expires.extend(due);
                }
                PresenceAction::RunQueuedExpire { queue_idx } => {
                    if !queued_expires.is_empty() {
                        let idx = queue_idx % queued_expires.len();
                        let exp_cmd = queued_expires.remove(idx);
                        let seat = exp_cmd.seat.clone();
                        let inc = exp_cmd.incarnation_id.clone();
                        let cur_live = state
                            .bindings_for(&seat)
                            .into_iter()
                            .find(|b| b.incarnation_id == inc && b.status == BindingStatus::Live)
                            .cloned();

                        apply_presence(
                            &mut state,
                            &mut log,
                            &mut seq,
                            now_ms,
                            sys.clone(),
                            PresenceCommand::PresenceExpire(exp_cmd.clone()),
                        )
                        .expect("fenced PresenceExpire succeeds cleanly");

                        if let Some(cur) = cur_live {
                            let stale_epoch = exp_cmd.expected_epoch.is_some_and(|e| e != cur.epoch);
                            let extended_lease = exp_cmd.lease_expires_at.is_some()
                                && cur.expires_at.is_some_and(|exp| exp > UnixMs(now_ms));
                            if stale_epoch || extended_lease {
                                // L3c: StaleExpireNeverKillsHeartbeatedBinding.
                                let after = state
                                    .bindings_for(&seat)
                                    .into_iter()
                                    .find(|b| b.incarnation_id == inc)
                                    .expect("binding exists");
                                prop_assert_eq!(after.status, BindingStatus::Live);
                            }
                        }
                    }
                }
                PresenceAction::AdvanceClock { delta_ms } => {
                    now_ms += delta_ms;
                }
                PresenceAction::TryBindOperator => {
                    let bind_err = apply_presence(
                        &mut state,
                        &mut log,
                        &mut seq,
                        now_ms,
                        Principal::Operator {
                            seat: operator_seat.clone(),
                        },
                        PresenceCommand::PresenceBind(PresenceBind {
                            seat: operator_seat.clone(),
                            incarnation_id: None,
                            machine_id: None,
                            lane: None,
                            endpoint: TransportEndpoint::UdsSession {
                                conn_id: 99,
                                client: "tui".into(),
                            },
                            liveness: LivenessMode::SocketLifetime,
                            ttl_ms: None,
                            takeover: false,
                            allow_fork: false,
                            require_canary: false,
                            mail_horizon_at: None,
                            idem: None,
                        }),
                    );
                    let is_denied = matches!(bind_err, Err(ApiError::Denied { .. }));
                    prop_assert!(is_denied, "expected Denied binding operator surface");
                    prop_assert!(state.bindings_for(&operator_seat).is_empty());
                }
            }

            // Global invariant: AtMostOneLivePerLane for every seat and lane.
            for seat in &seats {
                let all_live = state.live_bindings_for(seat, None, UnixMs(now_ms));
                let default_count = all_live.iter().filter(|b| b.lane.is_none()).count();
                prop_assert!(
                    default_count <= 1,
                    "AtMostOneLivePerLane violated on default lane for {}: {:?}",
                    seat,
                    all_live
                );
                for lane_name in lanes {
                    let lane_count = state
                        .live_bindings_for(seat, Some(lane_name), UnixMs(now_ms))
                        .len();
                    prop_assert!(
                        lane_count <= 1,
                        "AtMostOneLivePerLane violated on {} for {}",
                        lane_name,
                        seat
                    );
                }
            }

            // Journal replay equivalence.
            let mut replayed = PresenceState::default();
            for (ev, meta) in &log {
                Presence::evolve(&mut replayed, ev, meta);
            }
            let q = PresenceList {
                project: None,
                live_only: false,
                include_retired: true,
            };
            prop_assert_eq!(
                state.snapshot_at(&q, UnixMs(now_ms)),
                replayed.snapshot_at(&q, UnixMs(now_ms))
            );
        }
    }
}

trait PresenceSweepExt {
    fn collect_expired_sweep_probe(&self, now: UnixMs) -> Vec<PresenceExpire>;
}

impl PresenceSweepExt for PresenceState {
    fn collect_expired_sweep_probe(&self, now: UnixMs) -> Vec<PresenceExpire> {
        self.expired_bindings(now, |_pid, _start| true)
    }
}

#[derive(Clone, Debug)]
enum MailPassOp {
    WriteLetter {
        slot: u8,
        mirror_box: bool,
        body_v: u8,
    },
    AckSlot {
        slot: u8,
    },
    MutateAfterAck {
        slot: u8,
    },
    SetLive {
        live: bool,
        busy: bool,
    },
    AdvanceClock {
        delta_ms: u64,
    },
}

fn mail_op_strategy() -> impl Strategy<Value = MailPassOp> {
    prop_oneof![
        (0u8..3, any::<bool>(), 0u8..4).prop_map(|(slot, mirror_box, body_v)| {
            MailPassOp::WriteLetter {
                slot,
                mirror_box,
                body_v,
            }
        }),
        (0u8..3).prop_map(|slot| MailPassOp::AckSlot { slot }),
        (0u8..3).prop_map(|slot| MailPassOp::MutateAfterAck { slot }),
        (any::<bool>(), any::<bool>()).prop_map(|(live, busy)| MailPassOp::SetLive { live, busy }),
        (1_000u64..=40_000).prop_map(|delta_ms| MailPassOp::AdvanceClock { delta_ms }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 40,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn proptest_mail_shadow_invariants_and_watch_hydration_equivalence(
        ops in prop::collection::vec(mail_op_strategy(), 2..20),
        split_frac in 0usize..20,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        fs::create_dir_all(root.join("mail")).expect("mkdir mail");
        fs::create_dir_all(root.join("to-alice")).expect("mkdir to-alice");
        fs::create_dir_all(root.join("state")).expect("mkdir state");
        fs::write(root.join("state/acks.jsonl"), "").expect("init acks");

        let alice = SeatName::parse("alice").expect("alice");
        let inc = IncarnationId::parse("inc-alice-1").expect("inc");
        let aliases = AliasMap::empty();
        let affinity = IncarnationAffinity::new();
        let policy = RingPolicy {
            reconnect_grace_ms: 5_000,
            retry_deadline_ms: 20_000,
            digest_threshold: 3,
        };

        let mut presence = PresenceState::default();
        let mut seq = 0u64;
        let mut log = Vec::new();
        let mut now_ms = 1_795_260_000_000u64;
        let mut is_busy = false;
        let mut retry_state = RetryState::default();
        let mut acked_shas: Vec<Sha256Hex> = Vec::new();
        let mut slot_bytes: [Option<Vec<u8>>; 3] = [None, None, None];
        let mut slot_acked: [bool; 3] = [false, false, false];

        let mut pass_snapshots: Vec<(Vec<RingDecision>, UnixMs)> = Vec::new();

        for op in ops {
            match op {
                MailPassOp::WriteLetter {
                    slot,
                    mirror_box,
                    body_v,
                } => {
                    let idx = slot as usize;
                    if !slot_acked[idx] {
                        let ts = 1_795_260_000_100u64 + u64::from(slot) * 100;
                        let fname = format!("{ts}-bob-to-alice-{slot}.md");
                        let content = format!(
                            "---\nfrom: bob\nto: alice\nsubject: Slot {slot}\n---\nBody v{body_v}\n"
                        )
                        .into_bytes();
                        fs::write(root.join("mail").join(&fname), &content).expect("write mail");
                        if mirror_box {
                            fs::write(root.join("to-alice").join(&fname), &content)
                                .expect("write box");
                        }
                        slot_bytes[idx] = Some(content);
                    }
                }
                MailPassOp::AckSlot { slot } => {
                    let idx = slot as usize;
                    if let Some(bytes) = &slot_bytes[idx] {
                        let sha = sha_of(bytes);
                        let ts = 1_795_260_000_100u64 + u64::from(slot) * 100;
                        let line = serde_json::json!({
                            "ts": "2026-11-24T00:00:00Z",
                            "letter": format!("mail/{ts}-bob-to-alice-{slot}.md"),
                            "sha256": sha.as_str(),
                            "acked_by": "alice",
                        })
                        .to_string();
                        let mut prev =
                            fs::read_to_string(root.join("state/acks.jsonl")).expect("read acks");
                        prev.push_str(&line);
                        prev.push('\n');
                        fs::write(root.join("state/acks.jsonl"), prev).expect("write acks");
                        acked_shas.push(sha);
                        slot_acked[idx] = true;
                    }
                }
                MailPassOp::MutateAfterAck { slot } => {
                    let idx = slot as usize;
                    if slot_acked[idx] {
                        let ts = 1_795_260_000_100u64 + u64::from(slot) * 100;
                        let fname = format!("{ts}-bob-to-alice-{slot}.md");
                        let edited = format!(
                            "---\nfrom: bob\nto: alice\nsubject: Slot {slot}\n---\nMutated after ack!\n"
                        )
                        .into_bytes();
                        fs::write(root.join("mail").join(&fname), &edited).expect("mutate mail");
                        let box_copy = root.join("to-alice").join(&fname);
                        if box_copy.exists() {
                            fs::write(box_copy, &edited).expect("mutate box");
                        }
                        slot_bytes[idx] = Some(edited);
                    }
                }
                MailPassOp::SetLive { live, busy } => {
                    is_busy = busy;
                    if live {
                        drop(apply_presence(
                            &mut presence,
                            &mut log,
                            &mut seq,
                            now_ms,
                            Principal::Seat {
                                seat: alice.clone(),
                            },
                            PresenceCommand::PresenceBind(PresenceBind {
                                seat: alice.clone(),
                                incarnation_id: Some(inc.clone()),
                                machine_id: None,
                                lane: None,
                                endpoint: TransportEndpoint::UdsSession {
                                    conn_id: 1,
                                    client: "alice-cli".into(),
                                },
                                liveness: LivenessMode::SocketLifetime,
                                ttl_ms: None,
                                takeover: true,
                                allow_fork: false,
                                require_canary: false,
                                mail_horizon_at: Some(UnixMs(1_795_259_000_000)),
                                idem: None,
                            }),
                        ));
                    } else {
                        drop(apply_presence(
                            &mut presence,
                            &mut log,
                            &mut seq,
                            now_ms,
                            Principal::Seat {
                                seat: alice.clone(),
                            },
                            PresenceCommand::PresenceUnbind(PresenceUnbind {
                                seat: alice.clone(),
                                incarnation_id: inc.clone(),
                                expected_epoch: None,
                                reason: Some("offline".into()),
                            }),
                        ));
                    }
                }
                MailPassOp::AdvanceClock { delta_ms } => {
                    now_ms += delta_ms;
                }
            }

            // Update busy retry entries for currently scanned unacked letters.
            let ro = ReadOnlyInterchange::open(root).expect("open ro");
            let scanned = ro.scan_letters(&aliases).expect("scan");
            if is_busy {
                let mut next_retry = RetryState::new();
                for sc in &scanned {
                    for cand in sc.into_candidates() {
                        let prev_passes = retry_state
                            .get(&cand.to, &cand.sha256)
                            .map_or(0, |e| e.pass_count);
                        let first_seen = retry_state
                            .get(&cand.to, &cand.sha256)
                            .map_or(UnixMs(now_ms), |e| e.first_seen_at);
                        next_retry.insert(
                            cand.to.clone(),
                            cand.sha256.clone(),
                            RetryEntry {
                                first_seen_at: first_seen,
                                deadline_ms: policy.retry_deadline_ms,
                                target_busy: true,
                                pass_count: prev_passes + 1,
                            },
                        );
                    }
                }
                retry_state = next_retry;
            } else {
                retry_state = RetryState::new();
            }

            let report = run_shadow_pass(
                &ro,
                &presence,
                &affinity,
                &retry_state,
                &policy,
                &aliases,
                UnixMs(now_ms),
            )
            .expect("shadow pass");

            // Check MailHub / MailStore / MailMachines invariants on every pass:
            let mut rung_shas_this_pass = std::collections::BTreeSet::new();
            for d in &report.decisions {
                match d {
                    RingDecision::Ring { referent, .. } => {
                        // NoDoubleRing across mail/ + to-alice/ copies.
                        prop_assert!(rung_shas_this_pass.insert(referent.sha256().clone()));
                        // NoRingAfterAck.
                        prop_assert!(!acked_shas.contains(referent.sha256()));
                        // EditNeverRingable: mutated slot after ack must never ring.
                        for (idx, acked) in slot_acked.iter().enumerate() {
                            if *acked && let Some(cur_b) = &slot_bytes[idx] {
                                prop_assert_ne!(referent.sha256(), &sha_of(cur_b));
                            }
                        }
                    }
                    RingDecision::RingDigest { referents, .. } => {
                        for r in referents {
                            prop_assert!(rung_shas_this_pass.insert(r.sha256().clone()));
                            prop_assert!(!acked_shas.contains(r.sha256()));
                        }
                    }
                    RingDecision::Defer {
                        reason: DeferReason::TargetBusy { .. },
                        deadline_at,
                        ..
                    } => {
                        // NoFalseGiveUp: busy defer only while now < deadline_at.
                        prop_assert!(UnixMs(now_ms) < *deadline_at);
                    }
                    _ => {}
                }
            }

            pass_snapshots.push((report.decisions, UnixMs(now_ms)));
        }

        // ShadowWatchTracker continuous vs mid-stream restart hydration equivalence.
        let mut continuous = ShadowWatchTracker::new();
        let mut continuous_log = String::new();
        for (decisions, at) in &pass_snapshots {
            let delta = continuous
                .observe_pass(decisions, &aliases, *at)
                .expect("continuous observe");
            continuous_log.push_str(&delta.jsonl);
        }

        let split = split_frac % (pass_snapshots.len() + 1);
        let mut first_half = ShadowWatchTracker::new();
        let mut restart_log = String::new();
        for (decisions, at) in &pass_snapshots[..split] {
            let delta = first_half
                .observe_pass(decisions, &aliases, *at)
                .expect("first half observe");
            restart_log.push_str(&delta.jsonl);
        }
        let mut hydrated =
            ShadowWatchTracker::from_existing_jsonl(&restart_log).expect("hydrate from jsonl");
        for (decisions, at) in &pass_snapshots[split..] {
            let delta = hydrated
                .observe_pass(decisions, &aliases, *at)
                .expect("hydrated observe");
            restart_log.push_str(&delta.jsonl);
        }

        prop_assert_eq!(continuous_log, restart_log);
        prop_assert_eq!(continuous.tracked_referents(), hydrated.tracked_referents());
    }
}

#[test]
fn busy_deadline_loud_give_up_transitions_defer_to_raise_unrung() {
    let alice = SeatName::parse("alice").expect("alice");
    let sha = sha_of(b"blocking-letter");
    let cand = LetterCandidate {
        rel_path: "mail/1795260000100-bob-to-alice.md".into(),
        sha16: sha.as_str()[..16].to_owned(),
        sha256: sha.clone(),
        from: Some("bob".into()),
        to: alice.clone(),
        subject: Some("Blocking".into()),
        letter_id: None,
        in_reply_to: None,
        receipt_only: false,
        seen_at: UnixMs(1_795_260_000_100),
        time_basis: TimeBasis::LetterIdMs,
        date_only: false,
    };

    let mut presence = PresenceState::default();
    let mut log = Vec::new();
    let mut seq = 0u64;
    apply_presence(
        &mut presence,
        &mut log,
        &mut seq,
        1_795_260_000_000,
        Principal::Seat {
            seat: alice.clone(),
        },
        PresenceCommand::PresenceBind(PresenceBind {
            seat: alice.clone(),
            incarnation_id: Some(IncarnationId::parse("inc-1").expect("inc")),
            machine_id: None,
            lane: None,
            endpoint: TransportEndpoint::UdsSession {
                conn_id: 1,
                client: "cli".into(),
            },
            liveness: LivenessMode::SocketLifetime,
            ttl_ms: None,
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: Some(UnixMs(1_795_259_000_000)),
            idem: None,
        }),
    )
    .expect("bind");

    let aliases = AliasMap::empty();
    let acks = parse_acks_jsonl("", &aliases).index();
    let affinity = IncarnationAffinity::new();
    let policy = RingPolicy {
        reconnect_grace_ms: 15_000,
        retry_deadline_ms: 10_000,
        digest_threshold: 3,
    };
    let mut retry = RetryState::new();
    retry.insert(
        alice.clone(),
        sha,
        RetryEntry {
            first_seen_at: UnixMs(1_795_260_001_000),
            deadline_ms: 10_000,
            target_busy: true,
            pass_count: 1,
        },
    );

    let before_deadline = decide_mail_rings_for_seat(
        &alice,
        std::slice::from_ref(&cand),
        &acks,
        &presence,
        &affinity,
        &retry,
        &policy,
        &aliases,
        UnixMs(1_795_260_005_000),
    );
    assert!(matches!(
        &before_deadline[..],
        [RingDecision::Defer {
            reason: DeferReason::TargetBusy { pass_count: 1 },
            ..
        }]
    ));

    let after_deadline = decide_mail_rings_for_seat(
        &alice,
        &[cand],
        &acks,
        &presence,
        &affinity,
        &retry,
        &policy,
        &aliases,
        UnixMs(1_795_260_011_000),
    );
    assert!(matches!(
        &after_deadline[..],
        [RingDecision::RaiseUnrung { .. }]
    ));
}

#[test]
fn pre_deadline_in_flight_heartbeat_and_multi_lane_bindings_match_tla() {
    let alice = SeatName::parse("alice").expect("alice");
    let inc1 = IncarnationId::parse("inc-lane-1").expect("inc1");
    let inc2 = IncarnationId::parse("inc-lane-2").expect("inc2");
    let sys = Principal::System {
        part: SystemPart::PresenceMonitor,
    };

    let mut state = PresenceState::default();
    let mut log = Vec::new();
    let mut seq = 0u64;

    apply_presence(
        &mut state,
        &mut log,
        &mut seq,
        1_000,
        Principal::Seat {
            seat: alice.clone(),
        },
        PresenceCommand::PresenceBind(PresenceBind {
            seat: alice.clone(),
            incarnation_id: Some(inc1.clone()),
            machine_id: None,
            lane: Some("lane-a".into()),
            endpoint: TransportEndpoint::ClaudeSession {
                pid: 5001,
                proc_start: "Sat Nov 21 05:00:00 2026".into(),
                started_at_ms: 1_000,
                session_id: "sess-a".into(),
                messaging_socket: "/tmp/cc-a.sock".into(),
                kind: "interactive".into(),
                name_source: Some("user".into()),
                display_name: Some("Agent".into()),
            },
            liveness: LivenessMode::ProcessAndLease,
            ttl_ms: Some(200),
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: None,
            idem: None,
        }),
    )
    .expect("bind lane-a (expires_at = 1_200)");

    apply_presence(
        &mut state,
        &mut log,
        &mut seq,
        1_000,
        Principal::Seat {
            seat: alice.clone(),
        },
        PresenceCommand::PresenceBind(PresenceBind {
            seat: alice.clone(),
            incarnation_id: Some(inc2.clone()),
            machine_id: None,
            lane: Some("lane-b".into()),
            endpoint: TransportEndpoint::UdsSession {
                conn_id: 2,
                client: "cli-2".into(),
            },
            liveness: LivenessMode::SocketLifetime,
            ttl_ms: None,
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: None,
            idem: None,
        }),
    )
    .expect("bind lane-b concurrently without takeover");

    let heartbeat_req_at = 1_150u64;
    let sweep_at = 1_200u64;
    let queued_expires = state.collect_expired_sweep_probe(UnixMs(sweep_at));
    assert_eq!(queued_expires.len(), 1);
    assert_eq!(queued_expires[0].lease_expires_at, Some(UnixMs(1_200)));

    // Accepted pre-deadline heartbeat commits first (extends expires_at to 1_150 + 300 = 1_450).
    apply_presence(
        &mut state,
        &mut log,
        &mut seq,
        heartbeat_req_at,
        Principal::Seat {
            seat: alice.clone(),
        },
        PresenceCommand::PresenceHeartbeat(PresenceHeartbeat {
            seat: alice.clone(),
            incarnation_id: inc1.clone(),
            ttl_ms: Some(300),
        }),
    )
    .expect("pre-deadline heartbeat accepted by decide_heartbeat");

    // Stale queued PresenceExpire executes at t = 1_200 and MUST be fenced by lease_expires_at!
    apply_presence(
        &mut state,
        &mut log,
        &mut seq,
        sweep_at,
        sys,
        PresenceCommand::PresenceExpire(queued_expires.into_iter().next().unwrap()),
    )
    .expect("fenced expire runs cleanly");

    let live_after: Vec<_> = state
        .bindings_for(&alice)
        .into_iter()
        .filter(|b| b.status == BindingStatus::Live)
        .collect();
    assert_eq!(live_after.len(), 2, "both lane-a and lane-b remain Live");
    let b1 = live_after
        .iter()
        .find(|b| b.incarnation_id == inc1)
        .unwrap();
    assert_eq!(b1.expires_at, Some(UnixMs(1_450)));
}

#[derive(Clone, Debug)]
enum MailStoreAction {
    Send { slot: u8, skip_export_crash: bool },
    Drop { slot: u8 },
    Ack { slot: u8, skip_export_crash: bool },
    EditAfterAck { slot: u8, rev: u8 },
    Crash,
    Restart,
    Reconcile,
}

fn mail_store_action_strategy() -> impl Strategy<Value = MailStoreAction> {
    prop_oneof![
        (0u8..4, any::<bool>()).prop_map(|(slot, skip_export_crash)| MailStoreAction::Send {
            slot,
            skip_export_crash,
        }),
        (0u8..4).prop_map(|slot| MailStoreAction::Drop { slot }),
        (0u8..4, any::<bool>()).prop_map(|(slot, skip_export_crash)| MailStoreAction::Ack {
            slot,
            skip_export_crash,
        }),
        (0u8..4, 1u8..=3).prop_map(|(slot, rev)| MailStoreAction::EditAfterAck { slot, rev }),
        Just(MailStoreAction::Crash),
        Just(MailStoreAction::Restart),
        Just(MailStoreAction::Reconcile),
    ]
}

fn open_test_mail_bridge(
    sandbox: &SandboxRoot,
    journal_path: &std::path::Path,
    drop_dir: PathBuf,
) -> (Journal, MailBridge) {
    let journal = Journal::open(journal_path).expect("open journal");
    let writer = journal.claim(StreamId::Mail).expect("claim mail stream");
    let bridge =
        MailBridge::new(sandbox.clone(), journal.clone(), writer).with_drop_dir(Some(drop_dir));
    (journal, bridge)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 24,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    /// Direct state-machine conformance suite for `specs/mail/MailStore.tla`
    /// verifying `DurableSend`, `SingleHashPerPath`, `EditNeverRingable`, and
    /// `RebuildParity` across `Send`, `Drop`, `EditAfterAck`, `Crash`,
    /// `Restart`, `Reconcile`, and `Ack`.
    #[test]
    fn proptest_mail_store_conformance_matches_tla_invariants(
        actions in prop::collection::vec(mail_store_action_strategy(), 3..18)
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");

        rt.block_on(async move {
            let tmp = tempfile::tempdir().expect("tempdir");
            let mirror_dir = tmp.path().join("mirror");
            let drop_dir = tmp.path().join("drop");
            let journal_path = tmp.path().join("journal.sqlite3");
            let rebuild_journal_path = tmp.path().join("rebuilt.sqlite3");
            fs::create_dir_all(&mirror_dir).expect("mkdir mirror");
            fs::create_dir_all(&drop_dir).expect("mkdir drop");
            fs::write(mirror_dir.join(".rex-wave-sandbox"), b"test\n").expect("marker");

            let protection = Protection::default();
            let sandbox = SandboxRoot::verify_with(&mirror_dir, &protection).expect("sandbox");
            let alice = SeatName::parse("alice").expect("alice");
            let bob = SeatName::parse("bob").expect("bob");
            let aliases = AliasMap::empty();

            let mut up = true;
            let mut reconciled = true;
            let mut live: Option<(Journal, MailBridge)> = Some(open_test_mail_bridge(
                &sandbox,
                &journal_path,
                drop_dir.clone(),
            ));

            let mut used_slots: BTreeSet<u8> = BTreeSet::new();
            let mut slot_path: BTreeMap<u8, PathBuf> = BTreeMap::new();
            let mut slot_orig_sha: BTreeMap<u8, Sha256Hex> = BTreeMap::new();
            let mut sent_shas: BTreeSet<Sha256Hex> = BTreeSet::new();
            let mut edit_target_shas: BTreeSet<Sha256Hex> = BTreeSet::new();
            let mut step_ms = 1_795_500_000_000u64;

            for action in actions {
                step_ms += 100;
                match action {
                    MailStoreAction::Send {
                        slot,
                        skip_export_crash,
                    } => {
                        if up && reconciled && !used_slots.contains(&slot) {
                            let (_, bridge) = live.as_ref().expect("hub up");
                            bridge.set_export_enabled(!skip_export_crash);
                            let ctx = DecideCtx::for_test(
                                Principal::Seat {
                                    seat: alice.clone(),
                                },
                                UnixMs(step_ms),
                            );
                            let reply = bridge
                                .dispatch(
                                    MailCommand::MailSend(MailSend {
                                        to: vec![bob.clone()],
                                        cc: Vec::new(),
                                        subject: format!("Slot {slot}"),
                                        body: format!("Original body for slot {slot}"),
                                        from: Some(alice.clone()),
                                        priority: LetterPriority::Normal,
                                        topic: Some(format!("slot-{slot}")),
                                        in_reply_to: None,
                                        refs: Vec::new(),
                                        wait_export: !skip_export_crash,
                                        idem: None,
                                    }),
                                    ctx,
                                )
                                .await
                                .expect("mail send succeeds");
                            let WireReply::MailSend(sent) = reply else {
                                unreachable!("MailSend reply");
                            };
                            used_slots.insert(slot);
                            slot_path.insert(slot, sent.path);
                            slot_orig_sha.insert(slot, sent.sha256.clone());
                            sent_shas.insert(sent.sha256);

                            if skip_export_crash {
                                // Simulated crash immediately after SQLite commit, before export!
                                live = None;
                                up = false;
                                reconciled = false;
                            } else {
                                bridge.set_export_enabled(true);
                            }
                        }
                    }
                    MailStoreAction::Drop { slot } => {
                        // Drop works whether the hub is up OR down!
                        if !used_slots.contains(&slot) {
                            let fname = format!("{step_ms}-alice-to-bob-drop-{slot}.md");
                            let body = format!(
                                "---\nid: {step_ms}-alice-to-bob-drop-{slot}\nfrom: alice\nto: bob\nsubject: Drop {slot}\n---\n\nDropped body {slot}\n"
                            );
                            let bytes = body.into_bytes();
                            let sha = sha_of(&bytes);
                            fs::write(drop_dir.join(&fname), &bytes).expect("write drop");
                            used_slots.insert(slot);
                            slot_path.insert(slot, PathBuf::from(format!("mail/{fname}")));
                            slot_orig_sha.insert(slot, sha.clone());
                            sent_shas.insert(sha);
                        }
                    }
                    MailStoreAction::Ack {
                        slot,
                        skip_export_crash,
                    } => {
                        if up && let Some(sha) = slot_orig_sha.get(&slot).cloned() {
                            let (_, bridge) = live.as_ref().expect("hub up");
                            let snap = bridge.mail_state_snapshot().await;
                            if snap.ringable_for_seat(&bob).contains(&sha) {
                                bridge.set_export_enabled(!skip_export_crash);
                                let ctx = DecideCtx::for_test(
                                    Principal::Seat { seat: bob.clone() },
                                    UnixMs(step_ms),
                                );
                                let _ = bridge
                                    .dispatch(
                                        MailCommand::MailAck(MailAck {
                                            path: None,
                                            sha256: Some(sha),
                                            seat: Some(bob.clone()),
                                            note: Some("acked".into()),
                                        }),
                                        ctx,
                                    )
                                    .await
                                    .expect("ringable mail ack succeeds");
                                if skip_export_crash {
                                    live = None;
                                    up = false;
                                    reconciled = false;
                                } else {
                                    bridge.set_export_enabled(true);
                                }
                            }
                        }
                    }
                    MailStoreAction::EditAfterAck { slot, rev } => {
                        if let (Some(rel_path), Some(orig_sha)) =
                            (slot_path.get(&slot), slot_orig_sha.get(&slot))
                        {
                            let abs = mirror_dir.join(rel_path);
                            if abs.is_file() {
                                let acks_raw = fs::read_to_string(mirror_dir.join("state/acks.jsonl"))
                                    .unwrap_or_default();
                                let idx = parse_acks_jsonl(&acks_raw, &aliases).index();
                                let cur_bytes = fs::read(&abs).expect("read working tree");
                                if idx.is_acked_by_digest("bob", orig_sha, &aliases)
                                    && sha_of(&cur_bytes) == *orig_sha
                                {
                                    let edited = format!(
                                        "---\nfrom: alice\nto: bob\nsubject: Slot {slot}\n---\n\nEdited post-ack rev={rev}\n"
                                    )
                                    .into_bytes();
                                    let edited_sha = sha_of(&edited);
                                    fs::write(&abs, &edited).expect("edit working tree in place");
                                    edit_target_shas.insert(edited_sha);
                                }
                            }
                        }
                    }
                    MailStoreAction::Crash => {
                        if up {
                            live = None;
                            up = false;
                            reconciled = false;
                        }
                    }
                    MailStoreAction::Restart => {
                        if !up {
                            live = Some(open_test_mail_bridge(
                                &sandbox,
                                &journal_path,
                                drop_dir.clone(),
                            ));
                            up = true;
                            reconciled = false;
                        }
                    }
                    MailStoreAction::Reconcile => {
                        if up {
                            let (_, bridge) = live.as_ref().expect("hub up");
                            bridge.set_export_enabled(true);
                            bridge.reconcile().await.expect("reconcile succeeds");
                            reconciled = true;
                        }
                    }
                }

                // Check MailStore.tla invariants after every step:
                let disk_blobs: BTreeSet<Sha256Hex> = scan_object_blobs(&mirror_dir)
                    .expect("scan blobs")
                    .into_iter()
                    .map(|(s, _, _)| s)
                    .collect();
                let disk_working =
                    scan_working_tree_dir(&mirror_dir, &aliases, false).expect("scan working");
                let disk_drops =
                    scan_working_tree_dir(&drop_dir, &aliases, true).expect("scan drop");

                // SingleHashPerPath: every working-tree path holds at most one hash.
                let mut seen_paths = BTreeSet::new();
                for f in &disk_working {
                    prop_assert!(
                        seen_paths.insert(f.rel_path.clone()),
                        "SingleHashPerPath violated for {}",
                        f.rel_path
                    );
                }

                let db_state = if let Some((_, ref bridge)) = live {
                    bridge.mail_state_snapshot().await
                } else {
                    let j = Journal::open(&journal_path).expect("inspect journal while down");
                    MailState::from_journal(&j).expect("replay state")
                };

                // DurableSend: every sent/dropped message exists in db ∪ drop ∪ mirror.
                let mut all_durable: BTreeSet<Sha256Hex> = db_state.db_set();
                all_durable.extend(disk_blobs.iter().cloned());
                all_durable.extend(disk_working.iter().map(|f| f.sha256.clone()));
                all_durable.extend(disk_drops.iter().map(|f| f.sha256.clone()));
                for s in &sent_shas {
                    prop_assert!(
                        all_durable.contains(s),
                        "DurableSend violated: missing hash {s}"
                    );
                }

                // EditNeverRingable: no post-ack edited hash may ever be ringable!
                let ringable_bob = db_state.ringable_for_seat(&bob);
                for edit_sha in &edit_target_shas {
                    prop_assert!(
                        !ringable_bob.contains(edit_sha),
                        "EditNeverRingable violated: post-ack edit {edit_sha} became ringable!"
                    );
                }
            }

            // Bring hub up and reach Quiescent via startup Reconcile, then verify RebuildParity!
            if !up {
                live = Some(open_test_mail_bridge(
                    &sandbox,
                    &journal_path,
                    drop_dir.clone(),
                ));
            }
            let (_, bridge) = live.as_ref().expect("hub up");
            bridge.set_export_enabled(true);
            bridge.reconcile().await.expect("final reconcile");
            let final_state = bridge.mail_state_snapshot().await;
            drop(live);

            // Delete rebuilt target if any and rebuild strictly from mirror alone.
            let _ = fs::remove_file(&rebuild_journal_path);
            rebuild_mail_from_mirror(&mirror_dir, &rebuild_journal_path, &aliases, &protection)
                .expect("offline lossless rebuild");
            let rebuilt_journal = Journal::open(&rebuild_journal_path).expect("open rebuilt");
            let rebuilt_state = MailState::from_journal(&rebuilt_journal).expect("replay rebuilt");

            // MailStore.tla RebuildParity: db = RebuiltDb /\ acked = RebuiltAcked /\ flagged = RebuiltFlags
            prop_assert_eq!(final_state.db_set(), rebuilt_state.db_set());
            prop_assert_eq!(final_state.acked, rebuilt_state.acked);
            prop_assert_eq!(final_state.flagged_shas, rebuilt_state.flagged_shas);
            Ok(())
        })?;
    }
}
