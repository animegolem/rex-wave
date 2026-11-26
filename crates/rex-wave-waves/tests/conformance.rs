//! Formal state-machine conformance suite for `specs/waves/WaveLifecycle.tla`.
//!
//! Exercises `Waves`, `WaveConductor`, `UnsettledFailure`, and `Router` worker
//! `Wait` auto-parking against randomized action traces and targeted fault-injection
//! scenarios (`L1`, `L2`, `L4`, `(attempt, thread)` fencing, pre-spawn cross-talk,
//! `Refused` stickiness, and `NoCompletedWaveWithFailedWorker`).

use proptest::prelude::*;
use rex_wave_api::read::Wait;
use rex_wave_api::room::{Body, RoomId, RoomRef, RoomScope};
use rex_wave_api::wave::*;
use rex_wave_api::*;
use rex_wave_board::Board;
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    BoxFut, DecideCtx, Decider, EntryMeta, Hub, LocalClient, NoAgentFeed, ReadSide, Registry,
    Router, Service, SidebarProjection,
};
use rex_wave_store::Journal;
use rex_wave_waves::{AgentHost, HostError, SpawnRequest, WaveConductor, WaveState, Waves};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn sample_spec() -> WaveSpec {
    WaveSpec {
        slug: WaveSlug::parse("w-conf").expect("slug"),
        project: BbProjectId::parse("proj_demo").expect("project"),
        goal: Body::parse("Verify wave lifecycle conformance").expect("goal"),
        defaults: AgentSpec {
            provider: ProviderId::parse("codex").expect("provider"),
            model: Some(ModelId::parse("gpt-6-demo").expect("model")),
            account: Some("acct-req".into()),
            reasoning: Some("xhigh".into()),
            service_tier: None,
            permission: PermissionMode::AcceptEdits,
            workspace: Workspace::ProjectDefault,
        },
        output_dir: None,
        workers: vec![
            WorkerSpec {
                role: RoleName::parse("author").expect("role"),
                brief: Body::parse("Implement feature").expect("brief"),
                agent: None,
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            },
            WorkerSpec {
                role: RoleName::parse("verifier").expect("role"),
                brief: Body::parse("Audit invariants").expect("brief"),
                agent: None,
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            },
        ],
    }
}

fn thread_for(idx: u8) -> BbThreadId {
    let s = match idx % 3 {
        0 => "th_a",
        1 => "th_b",
        _ => "th_c",
    };
    BbThreadId::parse(s).expect("thread id")
}

#[derive(Clone, Debug)]
enum WaveStep {
    Spawned {
        role_idx: usize,
        attempt: u32,
        thread_idx: u8,
    },
    SpawnFailed {
        role_idx: usize,
        attempt: u32,
    },
    Activity {
        role_idx: usize,
        kind: u8,
        thread_opt: Option<u8>,
        host_seq: Option<u64>,
        key_kind: u8,
    },
    TurnsSuppressed {
        role_idx: usize,
        thread_idx: u8,
        key_kind: u8,
    },
    RuntimeMeta {
        role_idx: usize,
        model_idx: u8,
        thread_opt: Option<u8>,
        attempt_opt: Option<u32>,
    },
    Archived {
        role_idx: usize,
        thread_opt: Option<u8>,
        attempt_opt: Option<u32>,
    },
    RetireWorker {
        role_idx: usize,
    },
    Finish {
        outcome_kind: u8,
        allow_incomplete: bool,
    },
}

fn step_strategy() -> impl Strategy<Value = WaveStep> {
    prop_oneof![
        (0usize..2, 1u32..=4, 0u8..3).prop_map(|(role_idx, attempt, thread_idx)| {
            WaveStep::Spawned {
                role_idx,
                attempt,
                thread_idx,
            }
        }),
        (0usize..2, 1u32..=4)
            .prop_map(|(role_idx, attempt)| WaveStep::SpawnFailed { role_idx, attempt }),
        (
            0usize..2,
            0u8..6,
            proptest::option::of(0u8..3),
            proptest::option::of(1u64..=6),
            0u8..4,
        )
            .prop_map(|(role_idx, kind, thread_opt, host_seq, key_kind)| {
                WaveStep::Activity {
                    role_idx,
                    kind,
                    thread_opt,
                    host_seq,
                    key_kind,
                }
            }),
        (0usize..2, 0u8..3, 1u8..4).prop_map(|(role_idx, thread_idx, key_kind)| {
            WaveStep::TurnsSuppressed {
                role_idx,
                thread_idx,
                key_kind,
            }
        }),
        (
            0usize..2,
            0u8..3,
            proptest::option::of(0u8..3),
            proptest::option::of(1u32..=4),
        )
            .prop_map(|(role_idx, model_idx, thread_opt, attempt_opt)| {
                WaveStep::RuntimeMeta {
                    role_idx,
                    model_idx,
                    thread_opt,
                    attempt_opt,
                }
            }),
        (
            0usize..2,
            proptest::option::of(0u8..3),
            proptest::option::of(1u32..=4),
        )
            .prop_map(|(role_idx, thread_opt, attempt_opt)| WaveStep::Archived {
                role_idx,
                thread_opt,
                attempt_opt,
            }),
        (0usize..2).prop_map(|role_idx| WaveStep::RetireWorker { role_idx }),
        (0u8..3, any::<bool>()).prop_map(|(outcome_kind, allow_incomplete)| WaveStep::Finish {
            outcome_kind,
            allow_incomplete,
        }),
    ]
}

fn keys_for(kind: u8) -> Vec<String> {
    match kind {
        1 => vec!["fe:1:turn_boom".into()],
        2 => vec!["fp:1:cyber_safeguard".into()],
        3 => vec!["ft:1:2".into()],
        _ => Vec::new(),
    }
}

fn activity_for(kind: u8) -> Activity {
    match kind % 6 {
        0 => Activity::Working,
        1 => Activity::Idle,
        2 => Activity::Parked {
            room: Some(RoomId::Channel {
                scope: RoomScope::Global,
                name: ChannelName::parse("general").expect("chan"),
            }),
            since: Seq(5),
        },
        3 => Activity::Blocked {
            interaction: PendingInteraction {
                id: BbInteractionId::parse("ix_1").expect("ix"),
                kind: InteractionKind::Permission,
                summary: "Approve write".into(),
            },
        },
        4 => Activity::Errored {
            error: "provider_503".into(),
        },
        _ => Activity::Refused {
            reason: "safety_refusal".into(),
        },
    }
}

fn apply_wave_cmd(
    state: &mut WaveState,
    log: &mut Vec<Envelope>,
    seq: &mut u64,
    now_ms: &mut u64,
    by: &Principal,
    cmd: WaveCommand,
) -> Result<(Vec<Envelope>, WireReply), ApiError> {
    *now_ms += 10;
    let ctx = DecideCtx::for_test(by.clone(), UnixMs(*now_ms));
    let stream = Waves::address(&cmd, &ctx)?;
    let events = Waves::decide(state, &cmd, &ctx)?;
    let mut committed = Vec::with_capacity(events.len());
    for ev in events {
        *seq += 1;
        let meta = EntryMeta {
            seq: Seq(*seq),
            at: ctx.at(),
            by: by.clone(),
        };
        Waves::evolve(state, &ev, &meta);
        let env = Envelope {
            stream: stream.clone(),
            seq: meta.seq,
            at: meta.at,
            by: by.clone(),
            event: Event::Wave(ev),
        };
        log.push(env.clone());
        committed.push(env);
    }
    let reply = Waves::reply(state, cmd, &committed);
    Ok((committed, reply))
}

fn snapshot_of(state: &WaveState, wave: &WaveSlug, op: &Principal) -> WaveSnapshot {
    let cmd = WaveCommand::WaveView(WaveView { wave: wave.clone() });
    let ctx = DecideCtx::for_test(op.clone(), UnixMs(999_999));
    assert!(
        Waves::decide(state, &cmd, &ctx)
            .expect("view ok")
            .is_empty()
    );
    let WireReply::WaveView(snap) = Waves::reply(state, cmd, &[]) else {
        panic!("expected WaveSnapshot");
    };
    snap
}

fn worker_is_failed_or_refused(phase: &WorkerPhase) -> bool {
    matches!(
        phase,
        WorkerPhase::Failed { .. }
            | WorkerPhase::Live {
                activity: Activity::Errored { .. } | Activity::Refused { .. },
                ..
            }
    ) || phase.error().is_some()
        || phase.refusal().is_some()
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn proptest_wave_decider_conformance_matches_tla_invariants(
        steps in prop::collection::vec(step_strategy(), 1..45)
    ) {
        let op = Principal::Operator {
            seat: SeatName::parse("owner").expect("seat"),
        };
        let sys = Principal::System {
            part: SystemPart::BbBridge,
        };
        let spec = sample_spec();
        let roles = [
            RoleName::parse("author").expect("role"),
            RoleName::parse("verifier").expect("role"),
        ];

        let mut state = WaveState::default();
        let mut log = Vec::new();
        let mut seq = 0u64;
        let mut now_ms = 1_000u64;

        apply_wave_cmd(
            &mut state,
            &mut log,
            &mut seq,
            &mut now_ms,
            &op,
            WaveCommand::WaveStart(WaveStart {
                spec: spec.clone(),
                idem: None, config_defaults: Default::default(), }),
        )
        .expect("WaveStart succeeds");

        let mut max_host_seq: BTreeMap<(usize, BbThreadId), u64> = BTreeMap::new();
        let mut sticky_refusal_threads: BTreeSet<(usize, BbThreadId)> = BTreeSet::new();
        let mut current_attempt: [u32; 2] = [1, 1];

        for step in steps {
            let before = snapshot_of(&state, &spec.slug, &op);
            let before_phases: BTreeMap<RoleName, WorkerPhase> = before
                .workers
                .iter()
                .map(|(r, _, p)| (r.clone(), p.clone()))
                .collect();

            match step {
                WaveStep::Spawned {
                    role_idx,
                    attempt,
                    thread_idx,
                } => {
                    let role = &roles[role_idx];
                    let thread = thread_for(thread_idx);
                    let prev_phase = before_phases[role].clone();
                    let prev_attempt = current_attempt[role_idx];
                    let is_finished = matches!(before.status, WaveStatus::Finished { .. });

                    let res = apply_wave_cmd(
                        &mut state,
                        &mut log,
                        &mut seq,
                        &mut now_ms,
                        &sys,
                        WaveCommand::WaveObserve(WaveObserve {
                            wave: spec.slug.clone(),
                            role: role.clone(),
                            observation: Observation::Spawned {
                                attempt,
                                thread: thread.clone(),
                            },
                        }),
                    )
                    .expect("WaveObserve(Spawned) never errors on existing worker");

                    let after = snapshot_of(&state, &spec.slug, &op);
                    let next_phase = &after
                        .workers
                        .iter()
                        .find(|(r, _, _)| r == role)
                        .expect("worker")
                        .2;

                    // L1 (NoOrphanedHostThread): retiring (or finishing) before Spawned
                    // transitions a late matching Spawned directly into Retiring { thread }.
                    if matches!(prev_phase, WorkerPhase::Retired { thread: None, .. })
                        && attempt == prev_attempt
                    {
                        let is_retiring =
                            matches!(next_phase, WorkerPhase::Retiring { thread: t, .. } if t == &thread);
                        prop_assert!(
                            is_retiring,
                            "L1 violated: expected Retiring({}), got {:?}",
                            thread,
                            next_phase
                        );
                    } else if matches!(prev_phase, WorkerPhase::Requested { attempt: a } if a == attempt)
                        && !is_finished
                    {
                        let is_live =
                            matches!(next_phase, WorkerPhase::Live { thread: t, .. } if t == &thread);
                        prop_assert!(is_live, "expected Live");
                    } else {
                        prop_assert!(
                            res.0.is_empty(),
                            "stale/mismatched Spawned must be a no-op, emitted {:?}",
                            res.0
                        );
                        prop_assert_eq!(next_phase, &prev_phase);
                    }
                }
                WaveStep::SpawnFailed { role_idx, attempt } => {
                    let role = &roles[role_idx];
                    let prev_phase = before_phases[role].clone();
                    let is_finished = matches!(before.status, WaveStatus::Finished { .. });
                    let res = apply_wave_cmd(
                        &mut state,
                        &mut log,
                        &mut seq,
                        &mut now_ms,
                        &sys,
                        WaveCommand::WaveObserve(WaveObserve {
                            wave: spec.slug.clone(),
                            role: role.clone(),
                            observation: Observation::SpawnFailed {
                                attempt,
                                error: "spawn_err".into(),
                            },
                        }),
                    )
                    .expect("SpawnFailed handled");
                    let after = snapshot_of(&state, &spec.slug, &op);
                    let next_phase = &after
                        .workers
                        .iter()
                        .find(|(r, _, _)| r == role)
                        .expect("worker")
                        .2;
                    if let WorkerPhase::Requested { attempt: next_a } = next_phase {
                        current_attempt[role_idx] = *next_a;
                    }
                    if !is_finished
                        && matches!(prev_phase, WorkerPhase::Requested { attempt: a } if a == attempt)
                    {
                        prop_assert!(!res.0.is_empty());
                    } else {
                        prop_assert!(res.0.is_empty());
                    }
                }
                WaveStep::Activity {
                    role_idx,
                    kind,
                    thread_opt,
                    host_seq,
                    key_kind,
                } => {
                    let role = &roles[role_idx];
                    let prev_phase = before_phases[role].clone();
                    let is_finished = matches!(before.status, WaveStatus::Finished { .. });
                    let act = activity_for(kind);
                    let keys = keys_for(key_kind);
                    let obs = match thread_opt {
                        Some(ti) => Observation::activity_with_seq_and_bounds(
                            act.clone(),
                            thread_for(ti),
                            host_seq,
                            None,
                            keys.clone(),
                        ),
                        None => Observation::Activity {
                            activity: act.clone(),
                            thread: None,
                            host_seq,
                            settled_seq: None,
                            suppress_keys: keys.clone(),
                        },
                    };
                    let res = apply_wave_cmd(
                        &mut state,
                        &mut log,
                        &mut seq,
                        &mut now_ms,
                        &sys,
                        WaveCommand::WaveObserve(WaveObserve {
                            wave: spec.slug.clone(),
                            role: role.clone(),
                            observation: obs,
                        }),
                    )
                    .expect("Activity handled");

                    let after = snapshot_of(&state, &spec.slug, &op);
                    let next_phase = &after
                        .workers
                        .iter()
                        .find(|(r, _, _)| r == role)
                        .expect("worker")
                        .2;

                    if let WorkerPhase::Live {
                        thread: live_th, ..
                    } = &prev_phase
                    {
                        let foreign_thread = thread_opt
                            .map(thread_for)
                            .as_ref()
                            .is_some_and(|t| t != live_th);
                        let prev_max = max_host_seq
                            .get(&(role_idx, live_th.clone()))
                            .copied()
                            .unwrap_or(0);
                        let stale_seq = host_seq.is_some_and(|s| s <= prev_max);

                        if is_finished || foreign_thread || stale_seq {
                            // StaleThreadNeverMutatesCurrentAttempt & L2 (MonotonicActivitySeq).
                            prop_assert!(
                                res.0.is_empty(),
                                "foreign/stale Activity must emit 0 events: foreign={} stale_seq={}",
                                foreign_thread,
                                stale_seq
                            );
                            prop_assert_eq!(next_phase, &prev_phase);
                        } else {
                            if let Some(s) = host_seq {
                                max_host_seq.insert((role_idx, live_th.clone()), s);
                            }
                            if matches!(act, Activity::Idle) {
                                sticky_refusal_threads.remove(&(role_idx, live_th.clone()));
                            } else if matches!(act, Activity::Refused { .. })
                                || keys.iter().any(|k| k.starts_with("fp:"))
                            {
                                sticky_refusal_threads.insert((role_idx, live_th.clone()));
                            }
                            // RefusalStickyOverErrored: Errored never downgrades Refused.
                            if sticky_refusal_threads.contains(&(role_idx, live_th.clone()))
                                && matches!(act, Activity::Errored { .. })
                            {
                                prop_assert!(
                                    next_phase.refusal().is_some() && next_phase.error().is_none(),
                                    "RefusalStickyOverErrored violated: {:?}",
                                    next_phase
                                );
                            }
                        }
                    } else {
                        prop_assert!(res.0.is_empty());
                        prop_assert_eq!(next_phase, &prev_phase);
                    }
                }
                WaveStep::TurnsSuppressed {
                    role_idx,
                    thread_idx,
                    key_kind,
                } => {
                    let role = &roles[role_idx];
                    let prev_phase = before_phases[role].clone();
                    let is_finished = matches!(before.status, WaveStatus::Finished { .. });
                    let thread = thread_for(thread_idx);
                    let keys = keys_for(key_kind);
                    let res = apply_wave_cmd(
                        &mut state,
                        &mut log,
                        &mut seq,
                        &mut now_ms,
                        &sys,
                        WaveCommand::WaveObserve(WaveObserve {
                            wave: spec.slug.clone(),
                            role: role.clone(),
                            observation: Observation::turns_suppressed(
                                thread.clone(),
                                keys.clone(),
                            ),
                        }),
                    )
                    .expect("TurnsSuppressed handled");
                    if !is_finished
                        && !res.0.is_empty()
                        && keys.iter().any(|k| k.starts_with("fp:"))
                    {
                        let applies = match &prev_phase {
                            WorkerPhase::Requested { .. } => true,
                            WorkerPhase::Live { thread: t, .. }
                            | WorkerPhase::Retiring { thread: t, .. } => t == &thread,
                            _ => false,
                        };
                        if applies {
                            sticky_refusal_threads.insert((role_idx, thread));
                        }
                    }
                }
                WaveStep::RuntimeMeta {
                    role_idx,
                    model_idx,
                    thread_opt,
                    attempt_opt,
                } => {
                    let role = &roles[role_idx];
                    let prev_phase = before_phases[role].clone();
                    let prev_meta = before.worker_meta[role].clone();
                    let model = ModelId::parse(format!("gpt-6-m{model_idx}")).expect("model");
                    let res = apply_wave_cmd(
                        &mut state,
                        &mut log,
                        &mut seq,
                        &mut now_ms,
                        &sys,
                        WaveCommand::WaveObserve(WaveObserve {
                            wave: spec.slug.clone(),
                            role: role.clone(),
                            observation: Observation::runtime_meta(
                                Some(model),
                                Some(format!("acct-{model_idx}")),
                                thread_opt.map(thread_for),
                                attempt_opt,
                            ),
                        }),
                    )
                    .expect("RuntimeMeta handled");
                    let after = snapshot_of(&state, &spec.slug, &op);
                    let next_meta = &after.worker_meta[role];

                    let expected_valid = attempt_opt.is_none_or(|a| a == current_attempt[role_idx])
                        && match (&prev_phase, thread_opt.map(thread_for)) {
                            (
                                WorkerPhase::Live { thread: t, .. }
                                | WorkerPhase::Retiring { thread: t, .. },
                                Some(obs_t),
                            ) => t == &obs_t,
                            (
                                WorkerPhase::Live { .. } | WorkerPhase::Retiring { .. },
                                None,
                            ) => true,
                            (WorkerPhase::Requested { .. }, _) => true,
                            _ => false,
                        };
                    if !expected_valid {
                        prop_assert!(res.0.is_empty());
                        prop_assert_eq!(next_meta, &prev_meta);
                    }
                }
                WaveStep::Archived {
                    role_idx,
                    thread_opt,
                    attempt_opt,
                } => {
                    let role = &roles[role_idx];
                    let prev_phase = before_phases[role].clone();
                    let res = apply_wave_cmd(
                        &mut state,
                        &mut log,
                        &mut seq,
                        &mut now_ms,
                        &sys,
                        WaveCommand::WaveObserve(WaveObserve {
                            wave: spec.slug.clone(),
                            role: role.clone(),
                            observation: Observation::Archived {
                                thread: thread_opt.map(thread_for),
                                attempt: attempt_opt,
                            },
                        }),
                    )
                    .expect("Archived handled");
                    let after = snapshot_of(&state, &spec.slug, &op);
                    let next_phase = &after
                        .workers
                        .iter()
                        .find(|(r, _, _)| r == role)
                        .expect("worker")
                        .2;

                    if let WorkerPhase::Live { thread: live_t, .. }
                    | WorkerPhase::Retiring { thread: live_t, .. } = &prev_phase
                        && thread_opt.map(thread_for).as_ref().is_some_and(|t| t != live_t)
                    {
                        prop_assert!(
                            res.0.is_empty(),
                            "foreign-thread Archived must not retire current thread"
                        );
                        prop_assert_eq!(next_phase, &prev_phase);
                    }
                    if let WorkerPhase::Requested { attempt: req_a } = &prev_phase {
                        let allowed = attempt_opt.is_none_or(|a| a == *req_a)
                            && (thread_opt.is_none() || attempt_opt.is_some());
                        if !allowed {
                            prop_assert!(res.0.is_empty());
                            prop_assert_eq!(next_phase, &prev_phase);
                        }
                    }
                }
                WaveStep::RetireWorker { role_idx } => {
                    let role = &roles[role_idx];
                    let prev_phase = before_phases[role].clone();
                    let (evs, _) = apply_wave_cmd(
                        &mut state,
                        &mut log,
                        &mut seq,
                        &mut now_ms,
                        &op,
                        WaveCommand::WaveRetireWorker(WaveRetireWorker {
                            wave: spec.slug.clone(),
                            role: role.clone(),
                            reason: Some("retire".into()),
                        }),
                    )
                    .expect("WaveRetireWorker is idempotent");
                    if matches!(
                        prev_phase,
                        WorkerPhase::Retiring { .. }
                            | WorkerPhase::Retired { .. }
                            | WorkerPhase::Failed { .. }
                    ) {
                        prop_assert!(evs.is_empty());
                    }
                }
                WaveStep::Finish {
                    outcome_kind,
                    allow_incomplete,
                } => {
                    let outcome = match outcome_kind % 3 {
                        0 => WaveOutcome::Completed {
                            summary: Body::parse("done").expect("body"),
                        },
                        1 => WaveOutcome::Cancelled {
                            reason: "cancelled".into(),
                        },
                        _ => WaveOutcome::Failed {
                            reason: "failed".into(),
                        },
                    };
                    let any_bad = before_phases.values().any(worker_is_failed_or_refused);
                    let res = apply_wave_cmd(
                        &mut state,
                        &mut log,
                        &mut seq,
                        &mut now_ms,
                        &op,
                        WaveCommand::WaveFinish(WaveFinish {
                            wave: spec.slug.clone(),
                            outcome: outcome.clone(),
                            allow_incomplete,
                        }),
                    );
                    if matches!(before.status, WaveStatus::Running)
                        && matches!(outcome, WaveOutcome::Completed { .. })
                        && any_bad
                        && !allow_incomplete
                    {
                        // NoCompletedWaveWithFailedWorker
                        prop_assert!(
                            matches!(res, Err(ApiError::Conflict { .. })),
                            "WaveFinish(Completed) without allow_incomplete must fail when a worker failed/refused"
                        );
                    }
                }
            }

            // Replay equivalence check after every step.
            let mut replayed = WaveState::default();
            for env in &log {
                let Event::Wave(ev) = &env.event else {
                    unreachable!("only wave events in log");
                };
                Waves::evolve(
                    &mut replayed,
                    ev,
                    &EntryMeta {
                        seq: env.seq,
                        at: env.at,
                        by: env.by.clone(),
                    },
                );
            }
            let live_snap = snapshot_of(&state, &spec.slug, &op);
            let replay_snap = snapshot_of(&replayed, &spec.slug, &op);
            prop_assert_eq!(live_snap.status, replay_snap.status);
            prop_assert_eq!(live_snap.workers, replay_snap.workers);
            prop_assert_eq!(live_snap.counts, replay_snap.counts);
            prop_assert_eq!(live_snap.worker_meta, replay_snap.worker_meta);
        }
    }
}

#[test]
fn pre_spawn_cross_talk_preserves_target_thread_refusal_over_foreign_error() {
    let op = Principal::Operator {
        seat: SeatName::parse("owner").expect("seat"),
    };
    let sys = Principal::System {
        part: SystemPart::BbBridge,
    };
    let spec = sample_spec();
    let author = RoleName::parse("author").expect("role");
    let th_a = BbThreadId::parse("th_target_a").expect("th_a");
    let th_b = BbThreadId::parse("th_foreign_b").expect("th_b");

    let mut state = WaveState::default();
    let mut log = Vec::new();
    let mut seq = 0u64;
    let mut now_ms = 1_000u64;

    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &op,
        WaveCommand::WaveStart(WaveStart {
            spec: spec.clone(),
            idem: None,
            config_defaults: Default::default(),
        }),
    )
    .expect("start");

    // Pre-spawn suppression keys arrive for target thread A (Refused), then foreign thread B (Errored).
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::turns_suppressed(
                th_a.clone(),
                vec!["fp:1:policy_block".into()],
            ),
        }),
    )
    .expect("suppress A");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::turns_suppressed(th_b, vec!["fe:1:foreign_crash".into()]),
        }),
    )
    .expect("suppress B");

    // Spawned(th_a) + Retire + Archived(th_a) must preserve th_a's refusal, never th_b's error.
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: th_a.clone(),
            },
        }),
    )
    .expect("spawn A");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &op,
        WaveCommand::WaveRetireWorker(WaveRetireWorker {
            wave: spec.slug.clone(),
            role: author.clone(),
            reason: None,
        }),
    )
    .expect("retire A");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::archived_for(th_a.clone(), Some(1)),
        }),
    )
    .expect("archive A");

    let snap = snapshot_of(&state, &spec.slug, &op);
    let author_phase = &snap
        .workers
        .iter()
        .find(|(r, _, _)| r == &author)
        .expect("author")
        .2;
    assert_eq!(
        author_phase,
        &WorkerPhase::Retired {
            thread: Some(th_a),
            errored: None,
            refused: Some("policy_block".into()),
        }
    );
}

#[derive(Clone, Default)]
struct ConformingHost {
    retired_threads: Arc<Mutex<Vec<BbThreadId>>>,
    fail_spawn_for: Arc<Mutex<BTreeSet<String>>>,
}

impl AgentHost for ConformingHost {
    fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<BbThreadId, HostError>> {
        let role = req.role.as_str().to_owned();
        let fail = self.fail_spawn_for.lock().expect("lock").contains(&role);
        Box::pin(async move {
            tokio::task::yield_now().await;
            if fail {
                return Err(HostError::Unreachable("deferred in-flight spawn".into()));
            }
            Ok(BbThreadId::parse(format!("th_{}_{}", req.role, req.attempt)).expect("th"))
        })
    }

    fn tell(
        &self,
        _thread: &BbThreadId,
        _body: &Body,
        _mode: SteerDeliveryMode,
    ) -> BoxFut<'_, Result<(), HostError>> {
        Box::pin(async { Ok(()) })
    }

    fn resolve(
        &self,
        _thread: &BbThreadId,
        _interaction: &BbInteractionId,
        _resolution: &InteractionResolution,
    ) -> BoxFut<'_, Result<(), HostError>> {
        Box::pin(async { Ok(()) })
    }

    fn retire(&self, thread: &BbThreadId) -> BoxFut<'_, Result<(), HostError>> {
        let th = thread.clone();
        let retired = self.retired_threads.clone();
        Box::pin(async move {
            tokio::task::yield_now().await;
            retired.lock().expect("lock").push(th);
            Ok(())
        })
    }
}

struct FaultOnceArchiveWaveService {
    inner: Arc<Registry<Waves>>,
    fail_next_archived_observe: AtomicBool,
}

impl Service<WaveCommand> for FaultOnceArchiveWaveService {
    fn dispatch(
        &self,
        cmd: WaveCommand,
        ctx: DecideCtx,
    ) -> BoxFut<'_, Result<WireReply, ApiError>> {
        if let WaveCommand::WaveObserve(obs) = &cmd
            && matches!(obs.observation, Observation::Archived { .. })
            && self
                .fail_next_archived_observe
                .swap(false, Ordering::AcqRel)
        {
            return Box::pin(async {
                Err(ApiError::Transport(
                    "injected WaveObserve(Archived) fault".into(),
                ))
            });
        }
        self.inner.dispatch(cmd, ctx)
    }
}

#[tokio::test]
async fn conductor_retires_late_spawn_and_recovers_faulted_archive_observe() {
    let journal = Journal::in_memory().expect("journal");
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let wave_reg = Registry::<Waves>::new(journal.clone(), hub.clone());
    let fault_svc = Arc::new(FaultOnceArchiveWaveService {
        inner: wave_reg,
        fail_next_archived_observe: AtomicBool::new(false),
    });
    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        board: Registry::<Board>::new(journal.clone(), hub.clone()),
        wave: fault_svc.clone(),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }))
    });

    let op = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").expect("seat"),
        },
    );
    let conductor_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    let bridge_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::BbBridge,
        },
    );

    let host = ConformingHost::default();
    host.fail_spawn_for
        .lock()
        .expect("lock")
        .insert("author".into());
    let mut conductor = WaveConductor::new(host.clone());
    conductor
        .handle_frame(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &conductor_client,
        )
        .await
        .expect("caught up");

    let spec = sample_spec();
    let author = RoleName::parse("author").expect("role");
    let verifier = RoleName::parse("verifier").expect("role");
    op.call(WaveStart {
        spec: spec.clone(),
        idem: None,
        config_defaults: Default::default(),
    })
    .await
    .expect("start wave");

    // Feed initial WaveStart + WorkerRequested frames; author spawn fails transiently while verifier spawns.
    let entries = journal
        .read(
            &StreamId::Wave {
                wave: spec.slug.clone(),
            },
            Seq::ORIGIN,
            50,
        )
        .expect("read");
    let mut fed_seq = Seq::ORIGIN;
    for env in entries {
        fed_seq = env.seq;
        drop(
            conductor
                .handle_frame(Frame::Committed(env), &conductor_client)
                .await,
        );
    }
    host.fail_spawn_for.lock().expect("lock").clear();

    // Operator retires @author while still Requested { attempt: 1 } -> Retired { thread: None }.
    op.call(WaveRetireWorker {
        wave: spec.slug.clone(),
        role: author.clone(),
        reason: Some("cancel before spawn".into()),
    })
    .await
    .expect("retire author");

    // Now the late in-flight Spawned(attempt=1, th_late_author) arrives (L1).
    let late_th = BbThreadId::parse("th_late_author").expect("th");
    bridge_client
        .call(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: late_th.clone(),
            },
        })
        .await
        .expect("late spawn observe");

    let wave_entries = journal
        .read(
            &StreamId::Wave {
                wave: spec.slug.clone(),
            },
            fed_seq,
            50,
        )
        .expect("read");
    for env in wave_entries {
        conductor
            .handle_frame(Frame::Committed(env), &conductor_client)
            .await
            .expect("conductor handles L1 late spawn");
    }

    assert!(
        host.retired_threads
            .lock()
            .expect("lock")
            .contains(&late_th),
        "L1: WaveConductor must retire late-spawned host thread"
    );
    let snap_l1 = op
        .call(WaveView {
            wave: spec.slug.clone(),
        })
        .await
        .expect("view");
    let author_phase = &snap_l1
        .workers
        .iter()
        .find(|(r, _, _)| r == &author)
        .expect("author")
        .2;
    assert_eq!(
        author_phase,
        &WorkerPhase::Retired {
            thread: Some(late_th),
            errored: None,
            refused: None,
        }
    );

    // L4: Spawn a second live worker @reviewer, then retire @verifier (with one transient
    // WaveObserve(Archived) failure) and @reviewer together over a quiet journal.
    // Verify both settle to Retired and host.retire is invoked exactly once per thread.
    let reviewer = RoleName::parse("reviewer").expect("reviewer");
    op.call(rex_wave_api::wave::WaveAddWorker {
        wave: spec.slug.clone(),
        worker: WorkerSpec {
            role: reviewer.clone(),
            brief: Body::parse("review").expect("body"),
            agent: None,
            output_dir: None,
            writes: Vec::new(),
            return_schema: None,
        },
        config_defaults: Default::default(),
    })
    .await
    .expect("spawn reviewer");

    let reactor_handle = rex_wave_runtime::spawn_reactor(conductor, conductor_client.clone());
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snap = op
                .call(WaveView {
                    wave: spec.slug.clone(),
                })
                .await
                .expect("view");
            let rev_phase = &snap
                .workers
                .iter()
                .find(|(r, _, _)| r == &reviewer)
                .expect("reviewer")
                .2;
            if matches!(rev_phase, WorkerPhase::Live { .. }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("reviewer live");

    fault_svc
        .fail_next_archived_observe
        .store(true, Ordering::Release);
    op.call(WaveRetireWorker {
        wave: spec.slug.clone(),
        role: verifier.clone(),
        reason: Some("retire verifier".into()),
    })
    .await
    .expect("retire verifier");
    op.call(WaveRetireWorker {
        wave: spec.slug.clone(),
        role: reviewer.clone(),
        reason: Some("retire reviewer".into()),
    })
    .await
    .expect("retire reviewer");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snap_l4 = op
                .call(WaveView {
                    wave: spec.slug.clone(),
                })
                .await
                .expect("view");
            let ver_done = matches!(
                snap_l4
                    .workers
                    .iter()
                    .find(|(r, _, _)| r == &verifier)
                    .expect("verifier")
                    .2,
                WorkerPhase::Retired {
                    thread: Some(ref t),
                    errored: None,
                    refused: None,
                } if t.as_str() == "th_verifier_1"
            );
            let rev_done = matches!(
                snap_l4
                    .workers
                    .iter()
                    .find(|(r, _, _)| r == &reviewer)
                    .expect("reviewer")
                    .2,
                WorkerPhase::Retired {
                    thread: Some(ref t),
                    errored: None,
                    refused: None,
                } if t.as_str() == "th_reviewer_1"
            );
            if ver_done && rev_done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("L4 reactor backoff settles both Retiring workers to Retired on quiet journal");
    reactor_handle.abort();

    let retired_calls = host.retired_threads.lock().expect("lock").clone();
    assert_eq!(
        retired_calls
            .iter()
            .filter(|t| t.as_str() == "th_verifier_1")
            .count(),
        1,
        "verifier host.retire must run once, got {retired_calls:?}"
    );
    assert_eq!(
        retired_calls
            .iter()
            .filter(|t| t.as_str() == "th_reviewer_1")
            .count(),
        1,
        "reviewer host.retire must run once (not re-retired before Archived folds), got {retired_calls:?}"
    );
}

#[test]
fn pre_spawn_candidate_failures_and_metadata_isolate_per_thread() {
    let spec = sample_spec();
    let author = RoleName::parse("author").expect("author");
    let verifier = RoleName::parse("verifier").expect("verifier");
    let op = Principal::Operator {
        seat: SeatName::parse("owner").expect("seat"),
    };
    let sys = Principal::System {
        part: SystemPart::BbBridge,
    };

    let th_a = BbThreadId::parse("th_cand_a").expect("a");
    let th_b = BbThreadId::parse("th_cand_b").expect("b");

    let mut state = WaveState::default();
    let mut log = Vec::new();
    let mut seq = 0u64;
    let mut now_ms = 1_000u64;

    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &op,
        WaveCommand::WaveStart(WaveStart {
            spec: spec.clone(),
            idem: None,
            config_defaults: Default::default(),
        }),
    )
    .expect("start");

    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::turns_suppressed(th_b.clone(), vec!["s1".into()]),
        }),
    )
    .expect("suppress non-failure B");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::turns_suppressed_with_seq(
                th_a.clone(),
                Some(20),
                vec!["fp:s1:policy_refused".into()],
            ),
        }),
    )
    .expect("suppress refusal A at seq 20");

    let early_finish = apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &op,
        WaveCommand::WaveFinish(WaveFinish {
            wave: spec.slug.clone(),
            outcome: WaveOutcome::Completed {
                summary: Body::parse("premature").expect("body"),
            },
            allow_incomplete: false,
        }),
    );
    assert!(
        early_finish.is_err(),
        "unresolved candidate refusal must defer ordinary WaveFinish(Completed)"
    );

    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::runtime_meta(
                Some(ModelId::parse("foreign-model-b").expect("m")),
                Some("foreign-acct-b".into()),
                Some(th_b.clone()),
                Some(1),
            ),
        }),
    )
    .expect("meta B");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: th_a.clone(),
            },
        }),
    )
    .expect("spawn A");

    let snap = snapshot_of(&state, &spec.slug, &op);
    assert_eq!(
        snap.worker_meta[&author].actual_model, None,
        "pre-spawn model from foreign candidate thread must not leak onto Spawned(A)"
    );
    assert_eq!(
        snap.worker_meta[&author].actual_account, None,
        "pre-spawn account from foreign candidate thread must not leak onto Spawned(A)"
    );

    // Delayed Idle(A, seq=10) after pre-spawn refusal at seq=20 must NOT clear @author's refusal.
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::activity_with_seq_and_bounds(
                Activity::Idle,
                th_a.clone(),
                Some(10),
                Some(1),
                Vec::new(),
            ),
        }),
    )
    .expect("stale idle A(10)");
    assert!(
        apply_wave_cmd(
            &mut state,
            &mut log,
            &mut seq,
            &mut now_ms,
            &op,
            WaveCommand::WaveFinish(WaveFinish {
                wave: spec.slug.clone(),
                outcome: WaveOutcome::Completed {
                    summary: Body::parse("still refused").expect("body"),
                },
                allow_incomplete: false,
            }),
        )
        .is_err(),
        "stale Idle(A, 10) after pre-spawn refusal at seq 20 must not allow completion"
    );

    // Fresh Idle(A, seq=21) settles @author cleanly.
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::activity_with_seq_and_bounds(
                Activity::Idle,
                th_a.clone(),
                Some(21),
                Some(2),
                Vec::new(),
            ),
        }),
    )
    .expect("fresh idle A(21)");

    let th_va = BbThreadId::parse("th_ver_a").expect("va");
    let th_vb = BbThreadId::parse("th_ver_b").expect("vb");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: verifier.clone(),
            observation: Observation::turns_suppressed(th_vb, vec!["fe:s1:other_error".into()]),
        }),
    )
    .expect("ver B err");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: verifier.clone(),
            observation: Observation::turns_suppressed(
                th_va.clone(),
                vec!["fp:s1:ver_refused".into()],
            ),
        }),
    )
    .expect("ver A ref");

    // Retiring an unbound worker @late-unbound that has a pre-spawn refusal
    // leaves it in Retired { thread: None } and MUST STILL block WaveFinish(Completed)
    // until its thread is identified.
    let late_unbound = RoleName::parse("late-unbound").expect("role");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &op,
        WaveCommand::WaveAddWorker(rex_wave_api::wave::WaveAddWorker {
            wave: spec.slug.clone(),
            worker: WorkerSpec {
                role: late_unbound.clone(),
                brief: Body::parse("unbound").expect("body"),
                agent: None,
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            },
            config_defaults: Default::default(),
        }),
    )
    .expect("spawn late-unbound");
    let th_lu = BbThreadId::parse("th_lu_refused").expect("lu");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: late_unbound.clone(),
            observation: Observation::turns_suppressed(
                th_lu.clone(),
                vec!["fp:s1:unbound_refused".into()],
            ),
        }),
    )
    .expect("pre-spawn refusal on late-unbound");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &op,
        WaveCommand::WaveRetireWorker(WaveRetireWorker {
            wave: spec.slug.clone(),
            role: late_unbound.clone(),
            reason: Some("retire before spawn".into()),
        }),
    )
    .expect("retire late-unbound");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: verifier.clone(),
            observation: Observation::archived_for(th_va.clone(), Some(1)),
        }),
    )
    .expect("archive ver A");
    assert!(
        apply_wave_cmd(
            &mut state,
            &mut log,
            &mut seq,
            &mut now_ms,
            &op,
            WaveCommand::WaveFinish(WaveFinish {
                wave: spec.slug.clone(),
                outcome: WaveOutcome::Completed {
                    summary: Body::parse("must fail while Retired(None) has candidate refusal")
                        .expect("body"),
                },
                allow_incomplete: false,
            }),
        )
        .is_err(),
        "Retired(None) with unresolved candidate refusal must block WaveFinish(Completed)"
    );
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: late_unbound.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: th_lu.clone(),
            },
        }),
    )
    .expect("late spawn on Retired(None)");
    let snap_lu = snapshot_of(&state, &spec.slug, &op);
    assert_eq!(
        snap_lu
            .workers
            .iter()
            .find(|(r, _, _)| r == &late_unbound)
            .expect("lu")
            .2,
        WorkerPhase::Retiring {
            thread: th_lu,
            errored: None,
            refused: Some("unbound_refused".into()),
        }
    );

    let snap2 = snapshot_of(&state, &spec.slug, &op);
    let ver_phase = &snap2
        .workers
        .iter()
        .find(|(r, _, _)| r == &verifier)
        .expect("verifier")
        .2;
    assert_eq!(
        ver_phase,
        &WorkerPhase::Retired {
            thread: Some(th_va),
            errored: None,
            refused: Some("ver_refused".into()),
        },
        "Archived(A, 1) from Requested must bind Some(A) and preserve A's refusal"
    );
}

#[tokio::test]
async fn router_worker_wait_parking_fences_stale_thread_sessions() {
    let journal = Journal::in_memory().expect("journal");
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        board: Registry::<Board>::new(journal.clone(), hub.clone()),
        wave: Registry::<Waves>::new(journal.clone(), hub.clone()),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }))
    });

    let op = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").expect("seat"),
        },
    );
    let bridge = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::BbBridge,
        },
    );

    let spec = sample_spec();
    let author = RoleName::parse("author").expect("role");
    let started = op
        .call(WaveStart {
            spec: spec.clone(),
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .expect("start");
    let conductor = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    conductor
        .call(rex_wave_api::room::RoomEnsure {
            room: RoomRef::Id {
                id: started.room.clone(),
            },
            topic: None,
            origin: Some(rex_wave_api::room::RoomOrigin::Wave {
                wave: spec.slug.clone(),
            }),
        })
        .await
        .expect("ensure room");
    let worker_p = Principal::Worker {
        wave: spec.slug.clone(),
        role: author.clone(),
    };
    conductor
        .call(rex_wave_api::room::RoomJoin {
            room: RoomRef::Id {
                id: started.room.clone(),
            },
            member: Some(worker_p.clone()),
            handle: Some(Handle::parse("author").expect("handle")),
        })
        .await
        .expect("join worker");

    let th_old = BbThreadId::parse("th_old_attempt").expect("old");
    let th_cur = BbThreadId::parse("th_cur_attempt").expect("cur");

    // Pre-spawn role-scoped Wait started while @author is still Requested gets NO parking guard,
    // so expiring after Spawned(th_cur) + thread-scoped Wait(th_cur) cannot unpark th_cur.
    let pre_spawn_role_worker = LocalClient::new(router.clone(), worker_p.clone());
    let room_pre = started.room.clone();
    let pre_spawn_wait = tokio::spawn(async move {
        pre_spawn_role_worker
            .call(Wait {
                timeout_ms: Some(60),
                ..Wait::new(
                    Interest::Room {
                        room: RoomRef::Id { id: room_pre },
                    },
                    Cursor::live(),
                )
            })
            .await
            .expect("pre-spawn role wait expires")
    });
    tokio::time::sleep(Duration::from_millis(15)).await;

    bridge
        .call(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: th_cur.clone(),
            },
        })
        .await
        .expect("spawn cur");

    let early_thread_worker =
        LocalClient::with_worker_thread(router.clone(), worker_p.clone(), Some(th_cur.clone()));
    let room_early = started.room.clone();
    let early_thread_wait = tokio::spawn(async move {
        early_thread_worker
            .call(Wait {
                timeout_ms: Some(400),
                ..Wait::new(
                    Interest::Room {
                        room: RoomRef::Id { id: room_early },
                    },
                    Cursor::live(),
                )
            })
            .await
            .expect("early thread wait")
    });
    pre_spawn_wait.await.expect("join pre-spawn wait");
    tokio::time::sleep(Duration::from_millis(10)).await;
    let snap_after_prespawn_wait = op
        .call(WaveView {
            wave: spec.slug.clone(),
        })
        .await
        .expect("view after pre-spawn wait");
    assert!(
        matches!(
            snap_after_prespawn_wait
                .workers
                .iter()
                .find(|(r, _, _)| r == &author)
                .expect("author")
                .2,
            WorkerPhase::Live {
                ref thread,
                activity: Activity::Parked { .. }
            } if thread == &th_cur
        ),
        "expiring pre-spawn role Wait must not unpark live thread Wait"
    );
    op.call(rex_wave_api::room::RoomPost::new(
        RoomRef::Id {
            id: started.room.clone(),
        },
        Body::parse("@author wake early").expect("body"),
    ))
    .await
    .expect("wake early");
    early_thread_wait.await.expect("join early thread wait");
    bridge
        .call(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::activity_with_bounds(
                Activity::Idle,
                th_cur.clone(),
                None,
                Vec::new(),
            ),
        })
        .await
        .expect("idle cur");

    // Stale worker session bound to th_old issues Wait: must NOT park or unpark th_cur.
    let stale_worker =
        LocalClient::with_worker_thread(router.clone(), worker_p.clone(), Some(th_old));
    stale_worker
        .call(Wait {
            timeout_ms: Some(15),
            ..Wait::new(
                Interest::Room {
                    room: RoomRef::Id {
                        id: started.room.clone(),
                    },
                },
                Cursor::live(),
            )
        })
        .await
        .expect("stale wait completes");

    let snap_after_stale = op
        .call(WaveView {
            wave: spec.slug.clone(),
        })
        .await
        .expect("view");
    let phase_after_stale = &snap_after_stale
        .workers
        .iter()
        .find(|(r, _, _)| r == &author)
        .expect("author")
        .2;
    assert_eq!(
        phase_after_stale,
        &WorkerPhase::Live {
            thread: th_cur.clone(),
            activity: Activity::Idle,
        },
        "stale worker_thread Wait must not clobber current thread's Idle activity"
    );

    // Current worker session bound to th_cur issues Wait: parks while waiting and restores Working on wake.
    let cur_worker =
        LocalClient::with_worker_thread(router.clone(), worker_p, Some(th_cur.clone()));
    let room_for_wait = started.room.clone();
    let wait_handle = tokio::spawn(async move {
        cur_worker
            .call(Wait {
                timeout_ms: Some(500),
                ..Wait::new(
                    Interest::Room {
                        room: RoomRef::Id { id: room_for_wait },
                    },
                    Cursor::live(),
                )
            })
            .await
            .expect("current wait")
    });

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let s = op
                .call(WaveView {
                    wave: spec.slug.clone(),
                })
                .await
                .expect("view");
            let ph = &s
                .workers
                .iter()
                .find(|(r, _, _)| r == &author)
                .expect("a")
                .2;
            if matches!(
                ph,
                WorkerPhase::Live {
                    thread,
                    activity: Activity::Parked { .. }
                } if thread == &th_cur
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("current worker session enters Parked");

    op.call(rex_wave_api::room::RoomPost::new(
        RoomRef::Id {
            id: started.room.clone(),
        },
        Body::parse("@author proceed").expect("body"),
    ))
    .await
    .expect("wake post");
    wait_handle.await.expect("join");

    let snap_restored = op
        .call(WaveView {
            wave: spec.slug.clone(),
        })
        .await
        .expect("view");
    let phase_restored = &snap_restored
        .workers
        .iter()
        .find(|(r, _, _)| r == &author)
        .expect("author")
        .2;
    assert_eq!(
        phase_restored,
        &WorkerPhase::Live {
            thread: th_cur.clone(),
            activity: Activity::Working,
        }
    );

    let worker_p2 = Principal::Worker {
        wave: spec.slug.clone(),
        role: author.clone(),
    };
    let role_scoped_worker = LocalClient::new(router.clone(), worker_p2.clone());
    let thread_scoped_worker =
        LocalClient::with_worker_thread(router.clone(), worker_p2, Some(th_cur.clone()));
    let room_for_long = started.room.clone();
    let long_wait = tokio::spawn(async move {
        thread_scoped_worker
            .call(Wait {
                timeout_ms: Some(500),
                ..Wait::new(
                    Interest::Room {
                        room: RoomRef::Id { id: room_for_long },
                    },
                    Cursor::live(),
                )
            })
            .await
            .expect("long thread wait")
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    role_scoped_worker
        .call(Wait {
            timeout_ms: Some(20),
            ..Wait::new(
                Interest::Room {
                    room: RoomRef::Id {
                        id: started.room.clone(),
                    },
                },
                Cursor::live(),
            )
        })
        .await
        .expect("short role wait expires");

    let snap_mid = op
        .call(WaveView {
            wave: spec.slug.clone(),
        })
        .await
        .expect("view mid");
    let phase_mid = &snap_mid
        .workers
        .iter()
        .find(|(r, _, _)| r == &author)
        .expect("author")
        .2;
    assert!(
        matches!(
            phase_mid,
            WorkerPhase::Live {
                thread,
                activity: Activity::Parked { .. }
            } if thread == &th_cur
        ),
        "worker must remain Parked while thread-scoped Wait is still active, got {phase_mid:?}"
    );

    op.call(rex_wave_api::room::RoomPost::new(
        RoomRef::Id { id: started.room },
        Body::parse("@author finish").expect("body"),
    ))
    .await
    .expect("wake second wait");
    long_wait.await.expect("join long wait");
}

#[test]
fn unbound_fencing_covers_post_completion_refusals_retired_archives_and_pre_spawn_seq() {
    let spec = sample_spec();
    let mut state = WaveState::default();
    let mut log = Vec::new();
    let mut seq = 0u64;
    let mut now_ms = 1_000u64;
    let op = Principal::Operator {
        seat: SeatName::parse("owner").expect("seat"),
    };
    let sys = Principal::System {
        part: SystemPart::BbBridge,
    };
    let author = RoleName::parse("author").expect("author");
    let verifier = RoleName::parse("verifier").expect("verifier");

    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &op,
        WaveCommand::WaveStart(WaveStart {
            spec: spec.clone(),
            idem: None,
            config_defaults: Default::default(),
        }),
    )
    .expect("start");

    // 1. Pre-spawn out-of-order TurnsSuppressed on @author preserves higher-seq refusal reason,
    //    and Spawned promotes Refused directly onto WorkerPhase::Live.
    let th_auth = BbThreadId::parse("th_auth_real").expect("th");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::turns_suppressed_with_seq(
                th_auth.clone(),
                Some(20),
                vec!["fp:s2:newer_policy".into()],
            ),
        }),
    )
    .expect("suppressed seq 20");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::turns_suppressed_with_seq(
                th_auth.clone(),
                Some(10),
                vec!["fp:s1:stale_policy".into()],
            ),
        }),
    )
    .expect("suppressed seq 10");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: th_auth.clone(),
            },
        }),
    )
    .expect("spawn author");
    let snap1 = snapshot_of(&state, &spec.slug, &op);
    assert_eq!(
        snap1
            .workers
            .iter()
            .find(|(r, _, _)| r == &author)
            .unwrap()
            .2,
        WorkerPhase::Live {
            thread: th_auth.clone(),
            activity: Activity::Refused {
                reason: "newer_policy".into(),
            },
        }
    );

    // Settle @author via Idle(30) so @verifier can exercise Retired { thread: None } + Archived.
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: author.clone(),
            observation: Observation::activity_with_seq_and_bounds(
                Activity::Idle,
                th_auth,
                Some(30),
                None,
                Vec::new(),
            ),
        }),
    )
    .expect("settle author");

    // 2. Candidate B refuses on @verifier while Requested; operator retires @verifier -> Retired{None}.
    //    Direct Archived(Some(th_ver_real), Some(1)) binds th_ver_real and clears B's block!
    let th_ver_b = BbThreadId::parse("th_ver_cand_b").expect("b");
    let th_ver_real = BbThreadId::parse("th_ver_real_a").expect("a");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: verifier.clone(),
            observation: Observation::turns_suppressed_with_seq(
                th_ver_b,
                Some(5),
                vec!["fp:s1:foreign_refusal".into()],
            ),
        }),
    )
    .expect("cand B refusal");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &op,
        WaveCommand::WaveRetireWorker(WaveRetireWorker {
            wave: spec.slug.clone(),
            role: verifier.clone(),
            reason: Some("retire unbound".into()),
        }),
    )
    .expect("retire verifier");
    assert!(
        apply_wave_cmd(
            &mut state,
            &mut log,
            &mut seq,
            &mut now_ms,
            &op,
            WaveCommand::WaveFinish(WaveFinish {
                wave: spec.slug.clone(),
                outcome: WaveOutcome::Completed {
                    summary: Body::parse("done").expect("summary"),
                },
                allow_incomplete: false,
            }),
        )
        .is_err(),
        "still blocked until actual verifier thread is identified"
    );

    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: verifier.clone(),
            observation: Observation::Archived {
                thread: Some(th_ver_real.clone()),
                attempt: Some(1),
            },
        }),
    )
    .expect("archived real verifier thread");
    let snap2 = snapshot_of(&state, &spec.slug, &op);
    assert_eq!(
        snap2
            .workers
            .iter()
            .find(|(r, _, _)| r == &verifier)
            .unwrap()
            .2,
        WorkerPhase::Retired {
            thread: Some(th_ver_real),
            errored: None,
            refused: None,
        }
    );

    // Add an unbound third worker, complete the wave cleanly, then verify post-completion
    // TurnsSuppressed refusals are fenced while late Spawned still transitions to Retiring!
    let scout = RoleName::parse("scout").expect("scout");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &op,
        WaveCommand::WaveAddWorker(WaveAddWorker {
            wave: spec.slug.clone(),
            worker: WorkerSpec {
                role: scout.clone(),
                brief: Body::parse("scout brief").expect("brief"),
                agent: None,
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            },
            config_defaults: Default::default(),
        }),
    )
    .expect("add scout");
    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &op,
        WaveCommand::WaveFinish(WaveFinish {
            wave: spec.slug.clone(),
            outcome: WaveOutcome::Completed {
                summary: Body::parse("done").expect("summary"),
            },
            allow_incomplete: false,
        }),
    )
    .expect("complete wave with unbound clean scout");

    let th_scout = BbThreadId::parse("th_scout_late").expect("th_scout");
    let (post_evs, _) = apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: scout.clone(),
            observation: Observation::turns_suppressed_with_seq(
                th_scout.clone(),
                Some(12),
                vec!["fp:s1:post_finish_refusal".into()],
            ),
        }),
    )
    .expect("post-finish TurnsSuppressed handled");
    assert!(
        post_evs.is_empty(),
        "post-completion unbound failure keys must be fenced"
    );

    apply_wave_cmd(
        &mut state,
        &mut log,
        &mut seq,
        &mut now_ms,
        &sys,
        WaveCommand::WaveObserve(WaveObserve {
            wave: spec.slug.clone(),
            role: scout.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: th_scout.clone(),
            },
        }),
    )
    .expect("late spawn scout");
    let snap3 = snapshot_of(&state, &spec.slug, &op);
    assert_eq!(
        snap3
            .workers
            .iter()
            .find(|(r, _, _)| r == &scout)
            .unwrap()
            .2,
        WorkerPhase::Retiring {
            thread: th_scout,
            errored: None,
            refused: None,
        },
        "completed wave retires late host thread without acquiring post-completion refusal"
    );
}
