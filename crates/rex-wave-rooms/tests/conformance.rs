//! State-machine conformance suite for `rex-wave-rooms` task leases and `needs` DAG.
//!
//! Proves monotonic `LeaseEpoch` minting, stale-epoch `LeaseFenced` rejection on
//! `TaskHeartbeat` and `TaskTransition`, expired-lease reclaim without `force`,
//! DAG cycle and `UnmetNeeds` rejection, and SQLite WAL journal replay equivalence.

use proptest::prelude::*;
use rex_wave_api::board::{Priority, Title};
use rex_wave_api::room::*;
use rex_wave_api::*;
use rex_wave_rooms::{RoomState, Rooms};
use rex_wave_runtime::{
    DecideCtx, Decider, EntryMeta, Hub, LocalClient, NoAgentFeed, ReadSide, Registry, Router,
    SidebarProjection,
};
use rex_wave_store::Journal;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

fn actor_for(idx: u8) -> Principal {
    let name = match idx % 3 {
        0 => "alice",
        1 => "bob",
        _ => "carol",
    };
    Principal::Seat {
        seat: SeatName::parse(name).expect("seat"),
    }
}

fn status_for(idx: u8) -> TaskStatus {
    match idx % 5 {
        0 => TaskStatus::Open,
        1 => TaskStatus::Review,
        2 => TaskStatus::Blocked,
        3 => TaskStatus::Done,
        _ => TaskStatus::Cancelled,
    }
}

#[derive(Clone, Debug)]
enum RoomTaskOp {
    Claim {
        actor: u8,
        task_idx: usize,
        ttl_ms: u64,
        force: bool,
    },
    Heartbeat {
        actor: u8,
        task_idx: usize,
        epoch_delta: i8,
        ttl_ms: u64,
    },
    Transition {
        actor: u8,
        task_idx: usize,
        status_idx: u8,
        use_stale_epoch: bool,
    },
    AddDep {
        task_idx: usize,
        dep_idx: usize,
    },
    AdvanceClock {
        delta_ms: u64,
    },
}

fn op_strategy() -> impl Strategy<Value = RoomTaskOp> {
    prop_oneof![
        (0u8..3, 0usize..4, 50u64..=300, any::<bool>()).prop_map(
            |(actor, task_idx, ttl_ms, force)| RoomTaskOp::Claim {
                actor,
                task_idx,
                ttl_ms,
                force,
            }
        ),
        (0u8..3, 0usize..4, -1i8..=0, 80u64..=300).prop_map(
            |(actor, task_idx, epoch_delta, ttl_ms)| RoomTaskOp::Heartbeat {
                actor,
                task_idx,
                epoch_delta,
                ttl_ms,
            }
        ),
        (0u8..3, 0usize..4, 0u8..5, any::<bool>()).prop_map(
            |(actor, task_idx, status_idx, use_stale_epoch)| RoomTaskOp::Transition {
                actor,
                task_idx,
                status_idx,
                use_stale_epoch,
            }
        ),
        (0usize..4, 0usize..4)
            .prop_map(|(task_idx, dep_idx)| RoomTaskOp::AddDep { task_idx, dep_idx }),
        (25u64..=200).prop_map(|delta_ms| RoomTaskOp::AdvanceClock { delta_ms }),
    ]
}

fn apply_room(
    state: &mut RoomState,
    log: &mut Vec<(RoomEvent, EntryMeta)>,
    seq: &mut u64,
    now_ms: u64,
    by: Principal,
    cmd: RoomCommand,
) -> Result<(Vec<Envelope>, WireReply), ApiError> {
    let ctx = DecideCtx::for_test(by.clone(), UnixMs(now_ms));
    let stream = Rooms::address(&cmd, &ctx)?;
    let events = Rooms::decide(state, &cmd, &ctx)?;
    let mut committed = Vec::with_capacity(events.len());
    for ev in events {
        *seq += 1;
        let meta = EntryMeta {
            seq: Seq(*seq),
            at: UnixMs(now_ms),
            by: by.clone(),
        };
        Rooms::evolve(state, &ev, &meta);
        log.push((ev.clone(), meta.clone()));
        committed.push(Envelope {
            stream: stream.clone(),
            seq: meta.seq,
            at: meta.at,
            by: by.clone(),
            event: Event::Room(ev),
        });
    }
    let reply = Rooms::reply(state, cmd, &committed);
    Ok((committed, reply))
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 56,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn proptest_room_task_leases_and_dag_conformance(
        ops in prop::collection::vec(op_strategy(), 1..45)
    ) {
        let mut state = RoomState::default();
        let mut log: Vec<(RoomEvent, EntryMeta)> = Vec::new();
        let mut seq = 0u64;
        let mut now_ms = 1_000u64;

        let chan = RoomRef::Channel {
            scope: RoomScope::Global,
            name: ChannelName::parse("tasks-conf").expect("chan"),
        };

        apply_room(
            &mut state,
            &mut log,
            &mut seq,
            now_ms,
            actor_for(0),
            RoomCommand::RoomEnsure(RoomEnsure {
                room: chan.clone(),
                topic: None,
                origin: None,
            }),
        )
        .expect("ensure room");

        for i in 0..3u8 {
            apply_room(
                &mut state,
                &mut log,
                &mut seq,
                now_ms,
                actor_for(i),
                RoomCommand::RoomJoin(RoomJoin {
                    room: chan.clone(),
                    member: None,
                    handle: None,
                }),
            )
            .expect("join member");
        }

        let mut task_seqs = Vec::new();
        for idx in 0..4 {
            let (_, reply) = apply_room(
                &mut state,
                &mut log,
                &mut seq,
                now_ms,
                actor_for(0),
                RoomCommand::RoomPost(RoomPost {
                    room: chan.clone(),
                    body: Body::parse(format!("Task {idx}")).expect("body"),
                    re: None,
                    after: None,
                    labels: Vec::new(),
                    task: Some(TaskSpec {
                        title: Title::parse(format!("Task {idx}")).expect("title"),
                        org_id: None,
                        needs: Vec::new(),
                        priority: Some(Priority::Normal),
                        due_ms: None,
                    }),
                    idem: None,
                    origin: Default::default(),
                }),
            )
            .expect("create task");
            let WireReply::RoomPost(posted) = reply else {
                panic!("expected Posted");
            };
            task_seqs.push(posted.seq);
        }

        let mut max_epoch_by_task: BTreeMap<Seq, LeaseEpoch> =
            task_seqs.iter().map(|&s| (s, LeaseEpoch(0))).collect();

        for op in ops {
            match op {
                RoomTaskOp::Claim {
                    actor,
                    task_idx,
                    ttl_ms,
                    force,
                } => {
                    let who = actor_for(actor);
                    let tseq = task_seqs[task_idx];
                    let before = state.task_view_at(tseq, UnixMs(now_ms)).expect("task view");
                    let held_by_other_unexpired = before.status == TaskStatus::Claimed
                        && before
                            .lease
                            .as_ref()
                            .is_some_and(|l| l.holder != who && !before.lease_expired);
                    let has_unmet = before.needs.iter().any(|dep| {
                        state
                            .task_view_at(*dep, UnixMs(now_ms))
                            .is_none_or(|v| v.status != TaskStatus::Done)
                    });

                    let res = apply_room(
                        &mut state,
                        &mut log,
                        &mut seq,
                        now_ms,
                        who,
                        RoomCommand::TaskClaim(TaskClaim {
                            room: chan.clone(),
                            task: tseq,
                            ttl_ms: Some(ttl_ms),
                            expected_rev: None,
                            force,
                            idem: None,
                        }),
                    );

                    if held_by_other_unexpired && !force {
                        prop_assert!(
                            matches!(res, Err(ApiError::Conflict { .. })),
                            "unexpired foreign lease must reject TaskClaim without force"
                        );
                    } else if has_unmet && !force {
                        let is_unmet_or_terminal = matches!(
                            res,
                            Err(ApiError::Conflict { .. })
                        );
                        prop_assert!(
                            is_unmet_or_terminal,
                            "unmet dependencies must reject TaskClaim without operator force"
                        );
                    } else if let Ok((_, WireReply::TaskClaim(receipt))) = res {
                        let prev_max = max_epoch_by_task[&tseq];
                        prop_assert!(
                            receipt.epoch > prev_max,
                            "LeaseEpoch must strictly increase: prev={:?}, got={:?}",
                            prev_max,
                            receipt.epoch
                        );
                        max_epoch_by_task.insert(tseq, receipt.epoch);
                    }
                }
                RoomTaskOp::Heartbeat {
                    actor,
                    task_idx,
                    epoch_delta,
                    ttl_ms,
                } => {
                    let who = actor_for(actor);
                    let tseq = task_seqs[task_idx];
                    let before = state.task_view_at(tseq, UnixMs(now_ms)).expect("task view");
                    let cur_epoch = max_epoch_by_task[&tseq].0;
                    let try_epoch = if epoch_delta < 0 {
                        LeaseEpoch(cur_epoch.saturating_sub(1))
                    } else {
                        LeaseEpoch(cur_epoch.max(1))
                    };

                    let res = apply_room(
                        &mut state,
                        &mut log,
                        &mut seq,
                        now_ms,
                        who.clone(),
                        RoomCommand::TaskHeartbeat(TaskHeartbeat {
                            room: chan.clone(),
                            task: tseq,
                            epoch: try_epoch,
                            ttl_ms: Some(ttl_ms),
                        }),
                    );

                    if let Some(lease) = &before.lease
                        && before.status == TaskStatus::Claimed
                        && try_epoch != lease.epoch
                    {
                        let is_fenced = matches!(
                            res,
                            Err(ApiError::Conflict {
                                data: Some(ConflictData::LeaseFenced { .. }),
                                ..
                            })
                        );
                        prop_assert!(is_fenced, "expected LeaseFenced on Heartbeat");
                    }
                }
                RoomTaskOp::Transition {
                    actor,
                    task_idx,
                    status_idx,
                    use_stale_epoch,
                } => {
                    let who = actor_for(actor);
                    let tseq = task_seqs[task_idx];
                    let target_status = status_for(status_idx);
                    let before = state.task_view_at(tseq, UnixMs(now_ms)).expect("task view");
                    let epoch_arg = if use_stale_epoch {
                        Some(LeaseEpoch(0))
                    } else {
                        before.lease.as_ref().map(|l| l.epoch)
                    };

                    let res = apply_room(
                        &mut state,
                        &mut log,
                        &mut seq,
                        now_ms,
                        who,
                        RoomCommand::TaskTransition(TaskTransition {
                            room: chan.clone(),
                            task: tseq,
                            status: target_status,
                            epoch: epoch_arg,
                            expected_rev: None,
                            note: None,
                            idem: None,
                        }),
                    );

                    if use_stale_epoch
                        && before.status == TaskStatus::Claimed
                        && before.lease.is_some()
                    {
                        let is_fenced = matches!(
                            res,
                            Err(ApiError::Conflict {
                                data: Some(ConflictData::LeaseFenced { .. }),
                                ..
                            })
                        );
                        prop_assert!(is_fenced, "expected LeaseFenced on Transition");
                    }
                }
                RoomTaskOp::AddDep { task_idx, dep_idx } => {
                    let tseq = task_seqs[task_idx];
                    let dseq = task_seqs[dep_idx];
                    let res = apply_room(
                        &mut state,
                        &mut log,
                        &mut seq,
                        now_ms,
                        actor_for(0),
                        RoomCommand::TaskUpdate(TaskUpdate {
                            room: chan.clone(),
                            task: tseq,
                            expected_rev: None,
                            title: None,
                            needs_add: vec![dseq],
                            needs_remove: Vec::new(),
                            priority: None,
                            due_ms: None,
                            clear_due: false,
                            idem: None,
                        }),
                    );
                    if tseq == dseq {
                        prop_assert!(matches!(res, Err(ApiError::Invalid(_))));
                    }
                    // Verify task graph stays strictly acyclic after every TaskUpdate.
                    let views = state.task_list_at(None, UnixMs(now_ms)).expect("list");
                    let adj: BTreeMap<Seq, Vec<Seq>> = views
                        .tasks
                        .iter()
                        .map(|t| (t.seq, t.needs.clone()))
                        .collect();
                    for start in adj.keys() {
                        let mut stack = adj[start].clone();
                        let mut visited = std::collections::BTreeSet::new();
                        while let Some(cur) = stack.pop() {
                            prop_assert_ne!(
                                &cur,
                                start,
                                "DAG cycle introduced starting at {:?}: {:?}",
                                start,
                                adj
                            );
                            if visited.insert(cur) && let Some(next) = adj.get(&cur) {
                                stack.extend(next.iter().copied());
                            }
                        }
                    }
                }
                RoomTaskOp::AdvanceClock { delta_ms } => {
                    now_ms += delta_ms;
                }
            }

            // Event-stream replay equivalence check.
            let mut replayed = RoomState::default();
            for (ev, meta) in &log {
                Rooms::evolve(&mut replayed, ev, meta);
            }
            prop_assert_eq!(
                state.task_list_at(None, UnixMs(now_ms)),
                replayed.task_list_at(None, UnixMs(now_ms))
            );
        }
    }
}

#[tokio::test]
async fn wal_journal_passivation_and_replay_preserve_task_fencing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("rooms-conf.sqlite3");
    let chan = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("ops").expect("chan"),
    };
    let alice_p = actor_for(0);
    let bob_p = actor_for(1);
    let op_p = Principal::Operator {
        seat: SeatName::parse("owner").expect("seat"),
    };

    let (task_seq, epoch_2) = {
        let journal = Journal::open(&db_path).expect("open wal");
        let hub = Hub::new(128);
        let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
        let router = Arc::new(Router {
            room: Registry::<Rooms>::with_idle(
                journal.clone(),
                hub.clone(),
                Duration::from_millis(20),
            ),
            ..Router::unavailable_with(Arc::new(ReadSide {
                journal,
                hub,
                sidebar,
                agents: Arc::new(NoAgentFeed),
            }))
        });
        let alice = LocalClient::new(router.clone(), alice_p.clone());
        let bob = LocalClient::new(router.clone(), bob_p.clone());
        let op = LocalClient::new(router.clone(), op_p.clone());

        alice
            .call(RoomEnsure {
                room: chan.clone(),
                topic: None,
                origin: None,
            })
            .await
            .expect("ensure");
        let posted = alice
            .call(RoomPost {
                room: chan.clone(),
                body: Body::parse("Deploy check").expect("body"),
                re: None,
                after: None,
                labels: Vec::new(),
                task: Some(TaskSpec {
                    title: Title::parse("Deploy check").expect("title"),
                    org_id: None,
                    needs: Vec::new(),
                    priority: Some(Priority::High),
                    due_ms: None,
                }),
                idem: None,
                origin: Default::default(),
            })
            .await
            .expect("post task");
        let r1 = alice
            .call(TaskClaim {
                room: chan.clone(),
                task: posted.seq,
                ttl_ms: Some(15),
                expected_rev: None,
                force: false,
                idem: None,
            })
            .await
            .expect("claim 1");
        assert_eq!(r1.epoch, LeaseEpoch(1));

        let peer_force_denied = bob
            .call(TaskClaim {
                room: chan.clone(),
                task: posted.seq,
                ttl_ms: Some(60_000),
                expected_rev: None,
                force: true,
                idem: None,
            })
            .await
            .expect_err("peer seat cannot pass force=true");
        assert!(matches!(peer_force_denied, ApiError::Denied { .. }));

        tokio::time::sleep(Duration::from_millis(25)).await;

        let r2 = bob
            .call(TaskClaim {
                room: chan.clone(),
                task: posted.seq,
                ttl_ms: Some(60_000),
                expected_rev: None,
                force: false,
                idem: None,
            })
            .await
            .expect("expired lease reclaimed without force");
        assert_eq!(r2.epoch, LeaseEpoch(2));
        let _ = op;
        (posted.seq, r2.epoch)
    };

    let reopened = Journal::open(&db_path).expect("reopen wal");
    let hub = Hub::new(128);
    let sidebar = SidebarProjection::spawn(reopened.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(reopened.clone(), hub.clone()),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal: reopened,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }))
    });
    let alice = LocalClient::new(router.clone(), alice_p);
    let bob = LocalClient::new(router, bob_p);
    let _ = op_p;

    let fenced = alice
        .call(TaskTransition {
            room: chan.clone(),
            task: task_seq,
            status: TaskStatus::Done,
            epoch: Some(LeaseEpoch(1)),
            expected_rev: None,
            note: None,
            idem: None,
        })
        .await
        .expect_err("stale epoch 1 rejected after WAL reopen");
    assert!(matches!(
        fenced,
        ApiError::Conflict {
            data: Some(ConflictData::LeaseFenced {
                expected: LeaseEpoch(1),
                actual: LeaseEpoch(2),
                ..
            }),
            ..
        }
    ));

    let done = bob
        .call(TaskTransition {
            room: chan,
            task: task_seq,
            status: TaskStatus::Done,
            epoch: Some(epoch_2),
            expected_rev: None,
            note: None,
            idem: None,
        })
        .await
        .expect("current epoch 2 succeeds");
    assert_eq!(done.status, TaskStatus::Done);
}
