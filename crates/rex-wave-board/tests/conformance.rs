//! State-machine and `.org` reconciliation conformance suite for `rex-wave-board`.
//!
//! Proves `Board` `CardRevision` CAS fencing, archived card number non-reuse,
//! idempotent `CardUpsert` receipt stability, journal replay equivalence, and
//! two-way Emacs `.org` sync idempotency (`sync_room_to_org -> sync_org_to_room -> sync_room_to_org == Unchanged`).

#![allow(clippy::disallowed_methods)]

use proptest::prelude::*;
use rex_kanban::Lane;
use rex_wave_api::board::*;
use rex_wave_api::room::{
    Body, RoomEnsure, RoomPost, RoomRef, RoomScope, RoomTasks, TaskClaim, TaskSpec, TaskStatus,
    TaskTransition,
};
use rex_wave_api::*;
use rex_wave_board::{
    Board, BoardActorState, OrgSyncOptions, OrgSyncState, OrgToRoomOutcome, RoomToOrgOutcome,
    sync_org_to_room, sync_room_to_org,
};
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    DecideCtx, Decider, EntryMeta, Hub, LocalClient, NoAgentFeed, ReadSide, Registry, Router,
    SidebarProjection,
};
use rex_wave_store::Journal;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::Arc;

fn lane_for(idx: u8) -> LaneName {
    match idx % 6 {
        0 => Lane::Idea.lane_name(),
        1 => Lane::Todo.lane_name(),
        2 => Lane::Doing.lane_name(),
        3 => Lane::Review.lane_name(),
        4 => Lane::Blocked.lane_name(),
        _ => Lane::Done.lane_name(),
    }
}

#[derive(Clone, Debug)]
enum BoardOp {
    Create {
        title_idx: u8,
        lane_idx: u8,
        pos_opt: Option<u32>,
        idem_opt: Option<u8>,
    },
    PatchExact {
        card_slot: usize,
        lane_idx: u8,
        pos_opt: Option<u32>,
        label_idx: u8,
    },
    PatchStale {
        card_slot: usize,
    },
    ArchiveExact {
        card_slot: usize,
    },
    ArchiveStale {
        card_slot: usize,
    },
}

fn board_op_strategy() -> impl Strategy<Value = BoardOp> {
    prop_oneof![
        (
            0u8..8,
            0u8..6,
            proptest::option::of(0u32..4),
            proptest::option::of(0u8..3),
        )
            .prop_map(|(title_idx, lane_idx, pos_opt, idem_opt)| BoardOp::Create {
                title_idx,
                lane_idx,
                pos_opt,
                idem_opt,
            }),
        (0usize..6, 0u8..6, proptest::option::of(0u32..4), 0u8..4).prop_map(
            |(card_slot, lane_idx, pos_opt, label_idx)| BoardOp::PatchExact {
                card_slot,
                lane_idx,
                pos_opt,
                label_idx,
            }
        ),
        (0usize..6).prop_map(|card_slot| BoardOp::PatchStale { card_slot }),
        (0usize..6).prop_map(|card_slot| BoardOp::ArchiveExact { card_slot }),
        (0usize..6).prop_map(|card_slot| BoardOp::ArchiveStale { card_slot }),
    ]
}

fn apply_board(
    state: &mut BoardActorState,
    log: &mut Vec<(BoardEvent, EntryMeta)>,
    idem_log: &mut BTreeMap<IdemKey, Vec<Envelope>>,
    seq: &mut u64,
    by: &Principal,
    cmd: BoardCommand,
) -> Result<(Vec<Envelope>, WireReply), ApiError> {
    if let Some(key) = Board::idem(&cmd)
        && let Some(earlier) = idem_log.get(&key)
    {
        let reply = Board::reply(state, cmd, earlier);
        return Ok((Vec::new(), reply));
    }
    let idem_opt = Board::idem(&cmd);
    let ctx = DecideCtx::for_test(by.clone(), UnixMs(1_000 + *seq));
    let stream = Board::address(&cmd, &ctx)?;
    let events = Board::decide(state, &cmd, &ctx)?;
    let mut committed = Vec::with_capacity(events.len());
    for ev in events {
        *seq += 1;
        let meta = EntryMeta {
            seq: Seq(*seq),
            at: ctx.at(),
            by: by.clone(),
        };
        Board::evolve(state, &ev, &meta);
        log.push((ev.clone(), meta.clone()));
        committed.push(Envelope {
            stream: stream.clone(),
            seq: meta.seq,
            at: meta.at,
            by: by.clone(),
            event: Event::Board(ev),
        });
    }
    if let Some(key) = idem_opt
        && !committed.is_empty()
    {
        idem_log.insert(key, committed.clone());
    }
    let reply = Board::reply(state, cmd, &committed);
    Ok((committed, reply))
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn proptest_board_cas_archive_and_replay_conformance(
        ops in prop::collection::vec(board_op_strategy(), 1..45)
    ) {
        let board = BoardId::parse("project:proj_conf").expect("board");
        let op_p = Principal::Operator {
            seat: SeatName::parse("owner").expect("seat"),
        };

        let mut state = BoardActorState::default();
        let mut log: Vec<(BoardEvent, EntryMeta)> = Vec::new();
        let mut idem_log: BTreeMap<IdemKey, Vec<Envelope>> = BTreeMap::new();
        let mut seq = 0u64;
        let mut idem_seen: BTreeMap<String, CardNo> = BTreeMap::new();
        let mut created_cards: Vec<CardNo> = Vec::new();
        let mut archived_cards: BTreeSet<CardNo> = BTreeSet::new();
        let mut high_water_no = 0u32;

        apply_board(
            &mut state,
            &mut log,
            &mut idem_log,
            &mut seq,
            &op_p,
            BoardCommand::BoardEnsure(BoardEnsure {
                board: board.clone(),
                lanes: Some(Lane::all_lane_names()),
            }),
        )
        .expect("ensure board");

        for op in ops {
            match op {
                BoardOp::Create {
                    title_idx,
                    lane_idx,
                    pos_opt,
                    idem_opt,
                } => {
                    let idem_key = idem_opt
                        .map(|k| rex_kanban::IdemKey::parse(format!("idem-c-{k}")).expect("idem"));
                    let already = idem_key
                        .as_ref()
                        .and_then(|k| idem_seen.get(k.as_str()).copied());
                    let (evs, reply) = apply_board(
                        &mut state,
                        &mut log,
                        &mut idem_log,
                        &mut seq,
                        &op_p,
                        BoardCommand::CardUpsert(CardUpsert {
                            board: board.clone(),
                            card: None,
                            expected: None,
                            patch: CardPatch {
                                title: Some(
                                    Title::parse(format!("Card title {title_idx}")).expect("title"),
                                ),
                                lane: Some(lane_for(lane_idx)),
                                position: pos_opt,
                                ..Default::default()
                            },
                            idem: idem_key.clone(),
                        }),
                    )
                    .expect("create card");
                    let WireReply::CardUpsert(cref) = reply else {
                        panic!("expected CardRef");
                    };
                    if let Some(prev_no) = already {
                        prop_assert!(
                            evs.is_empty(),
                            "duplicate idem on CardUpsert must emit 0 events"
                        );
                        prop_assert_eq!(cref.no, prev_no);
                    } else {
                        prop_assert!(
                            cref.no.0 > high_water_no,
                            "card numbers must strictly increase and never reuse archived ids"
                        );
                        prop_assert!(!archived_cards.contains(&cref.no));
                        high_water_no = cref.no.0;
                        created_cards.push(cref.no);
                        if let Some(k) = idem_key {
                            idem_seen.insert(k.as_str().to_owned(), cref.no);
                        }
                    }
                }
                BoardOp::PatchExact {
                    card_slot,
                    lane_idx,
                    pos_opt,
                    label_idx,
                } => {
                    let live_cards: Vec<CardNo> = created_cards
                        .iter()
                        .copied()
                        .filter(|c| !archived_cards.contains(c))
                        .collect();
                    if !live_cards.is_empty() {
                        let no = live_cards[card_slot % live_cards.len()];
                        let cur_rev = state.inner().cards[&no].revision;
                        let (_, reply) = apply_board(
                            &mut state,
                            &mut log,
                            &mut idem_log,
                            &mut seq,
                            &op_p,
                            BoardCommand::CardUpsert(CardUpsert {
                                board: board.clone(),
                                card: Some(no),
                                expected: Some(cur_rev),
                                patch: CardPatch {
                                    lane: Some(lane_for(lane_idx)),
                                    position: pos_opt,
                                    labels_add: vec![format!("lbl-{label_idx}")],
                                    ..Default::default()
                                },
                                idem: None,
                            }),
                        )
                        .expect("exact patch succeeds");
                        let WireReply::CardUpsert(cref) = reply else {
                            panic!("expected CardRef");
                        };
                        prop_assert_eq!(cref.revision, CardRevision(cur_rev.0 + 1));
                    }
                }
                BoardOp::PatchStale { card_slot } => {
                    let live_cards: Vec<CardNo> = created_cards
                        .iter()
                        .copied()
                        .filter(|c| !archived_cards.contains(c))
                        .collect();
                    if !live_cards.is_empty() {
                        let no = live_cards[card_slot % live_cards.len()];
                        let cur_rev = state.inner().cards[&no].revision;
                        let stale_rev = CardRevision(cur_rev.0.saturating_sub(1));
                        if stale_rev != cur_rev {
                            let before_snap = state.inner().snapshot(state.head().0, None);
                            let err = apply_board(
                                &mut state,
                                &mut log,
                                &mut idem_log,
                                &mut seq,
                                &op_p,
                                BoardCommand::CardUpsert(CardUpsert {
                                    board: board.clone(),
                                    card: Some(no),
                                    expected: Some(stale_rev),
                                    patch: CardPatch {
                                        title: Some(Title::parse("Stale overwrite").expect("t")),
                                        ..Default::default()
                                    },
                                    idem: None,
                                }),
                            )
                            .expect_err("stale expected revision must fail");
                            let is_rev_conflict = matches!(
                                err,
                                ApiError::Conflict {
                                    data: Some(ConflictData::RevisionConflict { expected, actual }),
                                    ..
                                } if expected == stale_rev.0 && actual == cur_rev.0
                            );
                            prop_assert!(is_rev_conflict, "expected RevisionConflict on PatchStale");
                            prop_assert_eq!(
                                state.inner().snapshot(state.head().0, None),
                                before_snap
                            );
                        }
                    }
                }
                BoardOp::ArchiveExact { card_slot } => {
                    let live_cards: Vec<CardNo> = created_cards
                        .iter()
                        .copied()
                        .filter(|c| !archived_cards.contains(c))
                        .collect();
                    if !live_cards.is_empty() {
                        let no = live_cards[card_slot % live_cards.len()];
                        let cur_rev = state.inner().cards[&no].revision;
                        apply_board(
                            &mut state,
                            &mut log,
                            &mut idem_log,
                            &mut seq,
                            &op_p,
                            BoardCommand::CardArchive(CardArchive {
                                board: board.clone(),
                                card: no,
                                expected: Some(cur_rev),
                            }),
                        )
                        .expect("archive exact");
                        archived_cards.insert(no);
                        prop_assert!(!state.inner().cards.contains_key(&no));
                    }
                }
                BoardOp::ArchiveStale { card_slot } => {
                    let live_cards: Vec<CardNo> = created_cards
                        .iter()
                        .copied()
                        .filter(|c| !archived_cards.contains(c))
                        .collect();
                    if !live_cards.is_empty() {
                        let no = live_cards[card_slot % live_cards.len()];
                        let cur_rev = state.inner().cards[&no].revision;
                        if cur_rev.0 > 1 {
                            let err = apply_board(
                                &mut state,
                                &mut log,
                                &mut idem_log,
                                &mut seq,
                                &op_p,
                                BoardCommand::CardArchive(CardArchive {
                                    board: board.clone(),
                                    card: no,
                                    expected: Some(CardRevision(cur_rev.0 - 1)),
                                }),
                            )
                            .expect_err("stale archive must fail");
                            let is_rev_conflict = matches!(
                                err,
                                ApiError::Conflict {
                                    data: Some(ConflictData::RevisionConflict { .. }),
                                    ..
                                }
                            );
                            prop_assert!(is_rev_conflict, "expected RevisionConflict on ArchiveStale");
                            prop_assert!(state.inner().cards.contains_key(&no));
                        }
                    }
                }
            }

            // Event-stream replay equivalence check.
            let mut replayed = BoardActorState::default();
            for (ev, meta) in &log {
                Board::evolve(&mut replayed, ev, meta);
            }
            prop_assert_eq!(
                state.inner().snapshot(state.head().0, None),
                replayed.inner().snapshot(replayed.head().0, None)
            );
        }
    }
}

#[derive(Clone, Debug)]
struct GeneratedOrgTask {
    priority_idx: u8,
    status_idx: u8,
    has_due: bool,
    dep_prev: bool,
    has_label: bool,
}

fn org_task_strategy() -> impl Strategy<Value = GeneratedOrgTask> {
    (0u8..3, 0u8..5, any::<bool>(), any::<bool>(), any::<bool>()).prop_map(
        |(priority_idx, status_idx, has_due, dep_prev, has_label)| GeneratedOrgTask {
            priority_idx,
            status_idx,
            has_due,
            dep_prev,
            has_label,
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 24,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn proptest_two_way_task_org_sync_idempotency(
        specs in prop::collection::vec(org_task_strategy(), 1..6)
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio rt");

        rt.block_on(async move {
            let dir = tempfile::tempdir().expect("tempdir");
            let org_path = dir.path().join("tasks.org");
            fs::write(
                &org_path,
                "#+TITLE: Custom Room Roadmap\n#+TODO: IDEA TODO STRT WAIT BLKD | DONE CNCL\n\n",
            )
            .expect("seed preamble");

            let journal = Journal::in_memory().expect("journal");
            let hub = Hub::new(128);
            let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
            let router = Arc::new(Router {
                room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
                board: Registry::<Board>::new(journal.clone(), hub.clone()),
                ..Router::unavailable_with(Arc::new(ReadSide {
                    journal,
                    hub,
                    sidebar,
                    agents: Arc::new(NoAgentFeed),
                }))
            });

            let op = LocalClient::new(
                router,
                Principal::Operator {
                    seat: SeatName::parse("owner").expect("seat"),
                },
            );
            let room = RoomRef::Channel {
                scope: RoomScope::Global,
                name: ChannelName::parse("roadmap").expect("chan"),
            };
            op.call(RoomEnsure {
                room: room.clone(),
                topic: Some("Roadmap".into()),
                origin: None,
            })
            .await
            .expect("ensure room");

            let mut prev_seq: Option<Seq> = None;
            let mut prev_done = false;
            for (idx, item) in specs.iter().enumerate() {
                let prio = match item.priority_idx % 3 {
                    0 => Priority::High,
                    1 => Priority::Normal,
                    _ => Priority::Low,
                };
                let needs = match prev_seq {
                    Some(p) if item.dep_prev => vec![p],
                    _ => Vec::new(),
                };
                let labels = if item.has_label {
                    vec![Label::parse(format!("area=mod-{idx}")).expect("label")]
                } else {
                    Vec::new()
                };
                let due_ms = item.has_due.then_some(UnixMs(1_795_348_800_000));
                let custom_id = OrgId::parse(format!("emacs-task-{idx:02}")).expect("org_id");
                let posted = op
                    .call(RoomPost {
                        room: room.clone(),
                        body: Body::parse(format!("Task body {idx} with notes.")).expect("body"),
                        re: None,
                        after: None,
                        labels,
                        task: Some(TaskSpec {
                            title: Title::parse(format!("Deliver step {idx}")).expect("title"),
                            org_id: Some(custom_id),
                            needs: needs.clone(),
                            priority: Some(prio),
                            due_ms,
                        }),
                        idem: None,
                        origin: Default::default(),
                    })
                    .await
                    .expect("create task");

                let ready_deps = needs.is_empty() || prev_done;
                let target_status = match item.status_idx % 5 {
                    0 => TaskStatus::Open,
                    1 if ready_deps => TaskStatus::Claimed,
                    1 => TaskStatus::Blocked,
                    2 => TaskStatus::Review,
                    3 => TaskStatus::Blocked,
                    _ if ready_deps => TaskStatus::Done,
                    _ => TaskStatus::Review,
                };
                if target_status == TaskStatus::Claimed {
                    op.call(TaskClaim {
                        room: room.clone(),
                        task: posted.seq,
                        ttl_ms: Some(600_000),
                        expected_rev: None,
                        force: false,
                        idem: None,
                    })
                    .await
                    .expect("claim");
                } else if target_status != TaskStatus::Open {
                    op.call(TaskTransition {
                        room: room.clone(),
                        task: posted.seq,
                        status: target_status,
                        epoch: None,
                        expected_rev: None,
                        note: None,
                        idem: None,
                    })
                    .await
                    .expect("transition");
                }
                prev_done = target_status == TaskStatus::Done;
                prev_seq = Some(posted.seq);
            }

            let before_tasks = op
                .call(RoomTasks {
                    room: room.clone(),
                    filter: None,
                    limit: None,
                })
                .await
                .expect("room tasks before");

            let mut sync_state = OrgSyncState {
                last_synced_bytes: Some(fs::read(&org_path).expect("read seed")),
                ..OrgSyncState::default()
            };

            let first_export = sync_room_to_org(&op, &room, &org_path, &mut sync_state)
                .await
                .expect("first export");
            assert!(matches!(first_export, RoomToOrgOutcome::Written { .. }));
            let exported_bytes = fs::read(&org_path).expect("read exported");

            let reimport = sync_org_to_room(
                &op,
                &room,
                &org_path,
                &mut sync_state,
                &OrgSyncOptions::default(),
            )
            .await
            .expect("reimport");
            assert_eq!(
                reimport,
                OrgToRoomOutcome::Synced {
                    created: Vec::new(),
                    updated: Vec::new(),
                    transitioned: Vec::new(),
                    labeled: Vec::new(),
                    wrote_back: false,
                }
            );

            let second_export = sync_room_to_org(&op, &room, &org_path, &mut sync_state)
                .await
                .expect("second export");
            assert_eq!(second_export, RoomToOrgOutcome::Unchanged);
            assert_eq!(fs::read(&org_path).expect("read after"), exported_bytes);

            let after_tasks = op
                .call(RoomTasks {
                    room: room.clone(),
                    filter: None,
                    limit: None,
                })
                .await
                .expect("room tasks after");
            assert_eq!(before_tasks.tasks, after_tasks.tasks);
        });
    }
}
