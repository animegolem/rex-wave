use rex_wave_api::mail::*;
use rex_wave_api::notify::*;
use rex_wave_api::presence::*;
use rex_wave_api::read::{InboxBadge, Sidebar};
use rex_wave_api::room::*;
use rex_wave_api::stage::{Osc7501BlockKind, Osc7501State, derive_osc7501_status};
use rex_wave_api::wave::*;
use rex_wave_api::*;
use rex_wave_notify::{
    Herald, HeraldView, Inbox, MailUnrungEval, RECONNECT_GRACE_MS, classify, mail_unrung_draft,
    org_conflict_draft, org_conflict_resolved_draft, rank, transport_down_draft,
    transport_recovered_draft,
};
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    DecideCtx, Decider, EntryMeta, Hub, LocalClient, NoAgentFeed, ReadSide, Registry, Router,
    SidebarProjection, Unavailable, spawn_reactor,
};
use rex_wave_store::Journal;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

struct TestWaves;

#[derive(Default)]
struct TestWaveState {
    spec: Option<WaveSpec>,
    workers: BTreeMap<RoleName, (AgentSpec, WorkerPhase)>,
}

impl Decider for TestWaves {
    type State = TestWaveState;
    type Cmd = WaveCommand;
    type Event = WaveEvent;

    fn address(cmd: &WaveCommand, _ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        let wave = match cmd {
            WaveCommand::WaveStart(c) => c.spec.slug.clone(),
            WaveCommand::WaveAddWorker(c) => c.wave.clone(),
            WaveCommand::WaveRetireWorker(c) => c.wave.clone(),
            WaveCommand::WaveTell(c) => c.wave.clone(),
            WaveCommand::WaveResolve(c) => c.wave.clone(),
            WaveCommand::WaveFinish(c) => c.wave.clone(),
            WaveCommand::WaveObserve(c) => c.wave.clone(),
            WaveCommand::WaveView(c) => c.wave.clone(),
        };
        Ok(StreamId::Wave { wave })
    }

    fn idem(cmd: &WaveCommand) -> Option<IdemKey> {
        match cmd {
            WaveCommand::WaveStart(c) => c.idem.clone(),
            _ => None,
        }
    }

    fn decide(
        _state: &TestWaveState,
        cmd: &WaveCommand,
        _ctx: &DecideCtx,
    ) -> Result<Vec<WaveEvent>, ApiError> {
        match cmd {
            WaveCommand::WaveStart(c) => {
                let room = RoomId::Channel {
                    scope: RoomScope::Project {
                        project: c.spec.project.clone(),
                    },
                    name: ChannelName::parse(format!("wave-{}", c.spec.slug)).unwrap(),
                };
                let board = board::project_board(&c.spec.project);
                let mut events = vec![WaveEvent::Started {
                    spec: c.spec.clone(),
                    room,
                    board,
                }];
                for w in &c.spec.workers {
                    events.push(WaveEvent::WorkerRequested {
                        role: w.role.clone(),
                        agent: w.agent.clone().unwrap_or_else(|| c.spec.defaults.clone()),
                        brief: w.brief.clone(),
                        attempt: 1,
                        output_dir: w.output_dir.clone(),
                        writes: w.writes.clone(),
                        return_schema: w.return_schema.clone(),
                    });
                }
                Ok(events)
            }
            WaveCommand::WaveObserve(c) => Ok(vec![WaveEvent::WorkerObserved {
                role: c.role.clone(),
                observation: c.observation.clone(),
            }]),
            WaveCommand::WaveResolve(c) => Ok(vec![WaveEvent::InteractionResolved {
                role: c.role.clone(),
                interaction: c.interaction.clone(),
                resolution: c.resolution.clone(),
            }]),
            WaveCommand::WaveRetireWorker(c) => Ok(vec![WaveEvent::WorkerRetiring {
                role: c.role.clone(),
                reason: c.reason.clone(),
            }]),
            WaveCommand::WaveFinish(c) => Ok(vec![WaveEvent::Finished {
                outcome: c.outcome.clone(),
            }]),
            WaveCommand::WaveView(_) | WaveCommand::WaveAddWorker(_) | WaveCommand::WaveTell(_) => {
                Ok(vec![])
            }
        }
    }

    fn evolve(state: &mut TestWaveState, event: &WaveEvent, _meta: &EntryMeta) {
        match event {
            WaveEvent::Started { spec, .. } => state.spec = Some(spec.clone()),
            WaveEvent::WorkerRequested {
                role,
                agent,
                attempt,
                ..
            } => {
                state.workers.insert(
                    role.clone(),
                    (agent.clone(), WorkerPhase::Requested { attempt: *attempt }),
                );
            }
            WaveEvent::WorkerObserved { role, observation } => {
                if let Some((_, phase)) = state.workers.get_mut(role) {
                    *phase = phase.observe(observation);
                }
            }
            WaveEvent::WorkerRetiring { role, .. } => {
                if let Some((_, phase)) = state.workers.get_mut(role) {
                    *phase = phase.retiring();
                }
            }
            _ => {}
        }
    }

    fn reply(state: &TestWaveState, cmd: WaveCommand, _committed: &[Envelope]) -> WireReply {
        match cmd {
            WaveCommand::WaveStart(c) => WireReply::WaveStart(WaveStarted {
                wave: c.spec.slug.clone(),
                room: RoomId::Channel {
                    scope: RoomScope::Project {
                        project: c.spec.project.clone(),
                    },
                    name: ChannelName::parse(format!("wave-{}", c.spec.slug)).unwrap(),
                },
                board: board::project_board(&c.spec.project),
            }),
            WaveCommand::WaveObserve(_) => WireReply::WaveObserve(Ack),
            WaveCommand::WaveResolve(_) => WireReply::WaveResolve(Ack),
            WaveCommand::WaveRetireWorker(_) => WireReply::WaveRetireWorker(Ack),
            WaveCommand::WaveFinish(_) => WireReply::WaveFinish(Ack),
            WaveCommand::WaveAddWorker(_) => WireReply::WaveAddWorker(Ack),
            WaveCommand::WaveTell(_) => WireReply::WaveTell(Ack),
            WaveCommand::WaveView(_) => {
                let workers: Vec<_> = state
                    .workers
                    .iter()
                    .map(|(r, (a, p))| (r.clone(), a.clone(), p.clone()))
                    .collect();
                let counts = WaveCounts::from_phases(workers.iter().map(|(_, _, p)| p));
                WireReply::WaveView(WaveSnapshot {
                    spec: state.spec.clone().unwrap(),
                    status: WaveStatus::Running,
                    workers,
                    counts,
                    worker_meta: BTreeMap::new(),
                })
            }
        }
    }

    fn wrap(event: WaveEvent) -> Event {
        Event::Wave(event)
    }

    fn unwrap(event: &Event) -> Option<&WaveEvent> {
        match event {
            Event::Wave(e) => Some(e),
            _ => None,
        }
    }
}

fn sample_mailbox_item(kind: &str, label: Option<&str>) -> MailboxItem {
    MailboxItem {
        schema: MailSchemaV1,
        id: "letter:to-owner/ruling-r1.md".into(),
        from: "sam".into(),
        to: "owner".into(),
        kind: kind.into(),
        state: "open".into(),
        ts: "2026-11-16T10:30:00Z".into(),
        ts_basis: "mtime".into(),
        reference: MailRef {
            store: "letters".into(),
            path: PathBuf::from("to-owner/ruling-r1.md"),
            sha256: Sha256Hex::parse(
                "d4eb1d5a086d8a07b20cb67ca85a9b28f3dde81825c006cb988b15152aa3d678",
            )
            .unwrap(),
        },
        topic: Some("ruling".into()),
        round: Some(1),
        label: label.map(str::to_owned),
    }
}

#[test]
fn rank_orders_tiers_and_decays_with_age() {
    let room = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("general").unwrap(),
    };
    let interaction = NotifyKind::Interaction {
        wave: WaveSlug::parse("auth").unwrap(),
        role: RoleName::parse("migrator").unwrap(),
        interaction: BbInteractionId::parse("int_1").unwrap(),
    };
    let blocking_mail = NotifyKind::Mail {
        item: sample_mailbox_item("letter", Some("[blocking] Need ruling")),
    };
    let dm = NotifyKind::DirectMessage {
        room: room.clone(),
        seq: Seq(1),
    };
    let mention = NotifyKind::Mention { room, seq: Seq(2) };
    let normal_mail = NotifyKind::Mail {
        item: sample_mailbox_item("letter", Some("FYI note")),
    };
    let reminder = NotifyKind::Reminder {
        bell_id: "daily@2026-11-16T09:00".into(),
    };

    assert!(rank(&interaction, 0) > rank(&blocking_mail, 0));
    assert!(rank(&blocking_mail, 0) > rank(&dm, 0));
    assert!(rank(&dm, 0) > rank(&mention, 0));
    assert!(rank(&mention, 0) > rank(&normal_mail, 0));
    assert!(rank(&normal_mail, 0) > rank(&reminder, 0));
    assert!(rank(&mention, 0) > rank(&mention, 300_000));
}

#[test]
fn osc7501_status_transitions_blocked_permission_to_working_to_idle() {
    let thread = BbThreadId::parse("th_1").unwrap();
    let blocked_phase = WorkerPhase::Live {
        thread: thread.clone(),
        activity: Activity::Blocked {
            interaction: PendingInteraction {
                id: BbInteractionId::parse("int_8f2").unwrap(),
                kind: InteractionKind::Permission,
                summary: "Write Cargo.toml".into(),
            },
        },
    };
    let s1 = derive_osc7501_status(
        &InboxBadge {
            unread: 1,
            blocking: 1,
        },
        [&blocked_phase],
    );
    assert_eq!(s1.state, Osc7501State::Blocked);
    assert_eq!(s1.block_kind, Some(Osc7501BlockKind::Permission));

    let working_phase = WorkerPhase::Live {
        thread,
        activity: Activity::Working,
    };
    let s2 = derive_osc7501_status(
        &InboxBadge {
            unread: 0,
            blocking: 0,
        },
        [&working_phase],
    );
    assert_eq!(s2.state, Osc7501State::Working);
    assert_eq!(s2.block_kind, None);

    let s3 = derive_osc7501_status(
        &InboxBadge {
            unread: 0,
            blocking: 0,
        },
        std::iter::empty(),
    );
    assert_eq!(s3.state, Osc7501State::Idle);
    assert_eq!(s3.block_kind, None);
}

#[tokio::test]
async fn herald_raises_mentions_dms_interactions_and_mail_with_embedded_wire_actions() {
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(512);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Registry::<TestWaves>::new(journal.clone(), hub.clone()),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Arc::new(Unavailable(Family::Presence)),
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });

    let herald_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::Herald,
        },
    );
    let herald_task = spawn_reactor(Herald::new(), herald_client);

    let owner = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let alice_p = Principal::Seat {
        seat: SeatName::parse("alice").unwrap(),
    };
    let alice = LocalClient::new(router.clone(), alice_p.clone());
    let sam = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: SeatName::parse("sam").unwrap(),
        },
    );
    let bb_bridge = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::BbBridge,
        },
    );

    let general = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("general").unwrap(),
    };
    owner
        .call(RoomEnsure {
            room: general.clone(),
            topic: None,
            origin: None,
        })
        .await
        .unwrap();
    alice
        .call(RoomJoin {
            room: general.clone(),
            member: None,
            handle: None,
        })
        .await
        .unwrap();
    sam.call(RoomJoin {
        room: general.clone(),
        member: None,
        handle: None,
    })
    .await
    .unwrap();

    let mention_post = sam
        .call(RoomPost {
            room: general.clone(),
            body: Body::parse("hey @alice please review").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();

    let dm_post = sam
        .call(RoomPost {
            room: RoomRef::Dm {
                with: alice_p.clone(),
            },
            body: Body::parse("private heads up").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();

    let alice_inbox = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = alice
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            if page.items.len() == 2 {
                break page;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("alice received both DM and mention notifications");

    assert!(
        matches!(
            alice_inbox.items[0].draft.kind,
            NotifyKind::DirectMessage { .. }
        ),
        "DM ranks higher than channel mention"
    );
    assert!(matches!(
        alice_inbox.items[1].draft.kind,
        NotifyKind::Mention { .. }
    ));
    assert_eq!(alice_inbox.unread, 2);

    let sam_inbox = sam
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .unwrap();
    assert!(
        sam_inbox.items.is_empty(),
        "sender does not notify themselves"
    );

    alice
        .call(InboxMarkRead {
            room: mention_post.room.clone(),
            upto: mention_post.seq,
        })
        .await
        .unwrap();
    alice
        .call(InboxMarkRead {
            room: mention_post.room.clone(),
            upto: Seq(1),
        })
        .await
        .unwrap();
    let after_read = alice
        .call(InboxList {
            include_done: false,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(
        after_read.cursors,
        vec![(mention_post.room.clone(), mention_post.seq)],
        "read cursor advances monotonically and ignores regressions"
    );
    assert_eq!(
        after_read.unread, 1,
        "marking room read transitions unread mention card to Seen while keeping unread DM"
    );
    let _ = dm_post;

    let wave_slug = WaveSlug::parse("auth-v2").unwrap();
    let role = RoleName::parse("migrator").unwrap();
    owner
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave_slug.clone(),
                project: BbProjectId::parse("proj_3vs7nkyeum").unwrap(),
                goal: Body::parse("Ship OAuth").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("codex").unwrap(),
                    model: Some(ModelId::parse("gpt-6.1-sol").unwrap()),
                    account: None,
                    reasoning: Some("xhigh".into()),
                    service_tier: None,
                    permission: PermissionMode::AcceptEdits,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("Migrate tokens").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                }],
            },
            idem: None,
        })
        .await
        .unwrap();

    bb_bridge
        .call(WaveObserve {
            wave: wave_slug.clone(),
            role: role.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: BbThreadId::parse("th_mig").unwrap(),
            },
        })
        .await
        .unwrap();

    let interaction_id = BbInteractionId::parse("int_8f2").unwrap();
    bb_bridge
        .call(WaveObserve {
            wave: wave_slug.clone(),
            role: role.clone(),
            observation: Observation::Activity {
                activity: Activity::Blocked {
                    interaction: PendingInteraction {
                        id: interaction_id.clone(),
                        kind: InteractionKind::Permission,
                        summary: "Execute database migration".into(),
                    },
                },
            },
        })
        .await
        .unwrap();

    let mut mail_writer = journal.claim(StreamId::Mail).unwrap();
    let item = sample_mailbox_item("letter", Some("Ratified auth-v2 ruling"));
    let banked = mail_writer
        .append(
            &Principal::Seat {
                seat: SeatName::parse("sam").unwrap(),
            },
            UnixMs(1_700_000_000_000),
            None,
            vec![Event::Mail(MailEvent::Banked {
                from_room: mention_post.room.clone(),
                range: (Seq(1), Seq(2)),
                letter: LetterRef {
                    path: item.reference.path.clone(),
                    sha256: item.reference.sha256.clone(),
                    wake_id: WakeId("ce2dfdac6a18f680".into()),
                    item,
                },
            })],
        )
        .unwrap();
    hub.publish(banked.envelopes());

    let owner_inbox = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = owner
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            if page.items.len() == 2 {
                break page;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owner received both interaction and banked letter cards");

    let card = &owner_inbox.items[0];
    assert!(matches!(card.draft.kind, NotifyKind::Interaction { .. }));
    assert!(matches!(
        owner_inbox.items[1].draft.kind,
        NotifyKind::LetterBanked { .. }
    ));

    let snap_blocked = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let s = owner.call(Sidebar::default()).await.unwrap();
            if s.osc7501.state == Osc7501State::Blocked {
                break s;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        snap_blocked.osc7501.block_kind,
        Some(Osc7501BlockKind::Permission)
    );

    let allow_btn = card
        .draft
        .actions
        .iter()
        .find(|a| a.key == Some('a'))
        .expect("[a] allow once button");
    let deny_btn = card
        .draft
        .actions
        .iter()
        .find(|a| a.key == Some('d'))
        .expect("[d] deny button");
    assert!(
        matches!(&*deny_btn.run, WireCommand::WaveResolve(r) if r.resolution == InteractionResolution::Deny)
    );

    let reply = owner.call_wire((*allow_btn.run).clone()).await.unwrap();
    assert!(matches!(reply, WireReply::WaveResolve(Ack)));

    bb_bridge
        .call(WaveObserve {
            wave: wave_slug.clone(),
            role: role.clone(),
            observation: Observation::Activity {
                activity: Activity::Working,
            },
        })
        .await
        .unwrap();

    let snap_working = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let s = owner.call(Sidebar::default()).await.unwrap();
            if s.osc7501.state == Osc7501State::Working {
                break s;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(snap_working.osc7501.block_kind, None);

    bb_bridge
        .call(WaveObserve {
            wave: wave_slug.clone(),
            role: role.clone(),
            observation: Observation::Activity {
                activity: Activity::Blocked {
                    interaction: PendingInteraction {
                        id: BbInteractionId::parse("int_q1").unwrap(),
                        kind: InteractionKind::Question {
                            questions: vec![Question {
                                id: "q1".into(),
                                prompt: "Which branch?".into(),
                                options: vec!["main".into()],
                                multi: false,
                            }],
                        },
                        summary: "Select target branch".into(),
                    },
                },
            },
        })
        .await
        .unwrap();

    let q_page = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = owner
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            if page
                .items
                .iter()
                .any(|n| n.draft.dedupe.ends_with(":int_q1"))
            {
                break page;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let q_card = q_page
        .items
        .iter()
        .find(|n| n.draft.dedupe.ends_with(":int_q1"))
        .unwrap();
    assert!(
        q_card.draft.actions.iter().all(|a| a.key != Some('a')),
        "question card omits AllowOnce"
    );
    let q_deny = q_card
        .draft
        .actions
        .iter()
        .find(|a| a.key == Some('d'))
        .unwrap();
    owner.call_wire((*q_deny.run).clone()).await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = owner
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            if !page
                .items
                .iter()
                .any(|n| n.draft.dedupe.ends_with(":int_q1"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let wake_bell_sys = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WakeBell,
        },
    );
    let bell_rang = mail_writer
        .append(
            wake_bell_sys.me(),
            UnixMs(1_700_000_001_000),
            None,
            vec![Event::Mail(MailEvent::BellRang {
                bell_id: "daily@2026-11-16T09:00".into(),
                seat: SeatName::parse("sam").unwrap(),
            })],
        )
        .unwrap();
    hub.publish(bell_rang.envelopes());
    wake_bell_sys
        .call(RoomPost {
            room: RoomRef::Dm {
                with: Principal::Seat {
                    seat: SeatName::parse("sam").unwrap(),
                },
            },
            body: Body::parse("Scheduled check-in").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();

    let sam_after_bell = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = sam
                .call(InboxList {
                    include_done: true,
                    limit: None,
                })
                .await
                .unwrap();
            if !page.items.is_empty() {
                break page;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let sam_settled = sam
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(
        sam_settled.items.len(),
        1,
        "wake bell raises exactly one DM card without duplicate Reminder"
    );
    assert!(matches!(
        sam_after_bell.items[0].draft.kind,
        NotifyKind::DirectMessage { .. }
    ));

    let pre_replay = owner
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .unwrap();
    herald_task.abort();
    let replay_herald = spawn_reactor(
        Herald::new(),
        LocalClient::new(
            router.clone(),
            Principal::System {
                part: SystemPart::Herald,
            },
        ),
    );
    tokio::time::sleep(Duration::from_millis(80)).await;
    let after_replay = owner
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(
        after_replay.head, pre_replay.head,
        "replaying Herald appends zero journal rows onto resolved interactions or existing cards"
    );
    assert_eq!(
        after_replay.items.len(),
        3,
        "replaying Herald from origin deduplicates cards without duplicating or reopening Done items"
    );
    let resolved_card = after_replay.items.iter().find(|n| n.id == card.id).unwrap();
    assert_eq!(resolved_card.state, NotifyState::Done);
    replay_herald.abort();
}

#[tokio::test]
async fn system_posts_skip_operator_mentions_and_worker_stuck_dedupes_per_role() {
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Registry::<TestWaves>::new(journal.clone(), hub.clone()),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Arc::new(Unavailable(Family::Presence)),
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });

    let herald_task = spawn_reactor(
        Herald::new(),
        LocalClient::new(
            router.clone(),
            Principal::System {
                part: SystemPart::Herald,
            },
        ),
    );
    let owner_p = Principal::Operator {
        seat: SeatName::parse("dave").unwrap(),
    };
    let owner = LocalClient::new(router.clone(), owner_p.clone());
    let conductor = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    let bb_bridge = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::BbBridge,
        },
    );

    let wave_slug = WaveSlug::parse("demo-spec-r2").unwrap();
    let role = RoleName::parse("council-gemini").unwrap();
    let started = owner
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave_slug.clone(),
                project: BbProjectId::parse("proj_demo_alpha").unwrap(),
                goal: Body::parse("Review spec").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("acp-opencode").unwrap(),
                    model: Some(ModelId::parse("opencode/gemini-4-argon").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Full,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("Post review to @dave").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                }],
            },
            idem: None,
        })
        .await
        .unwrap();

    let room_ref = RoomRef::Id {
        id: started.room.clone(),
    };
    conductor
        .call(RoomEnsure {
            room: room_ref.clone(),
            topic: None,
            origin: Some(RoomOrigin::Wave {
                wave: wave_slug.clone(),
            }),
        })
        .await
        .unwrap();
    owner
        .call(RoomJoin {
            room: room_ref.clone(),
            member: None,
            handle: None,
        })
        .await
        .unwrap();
    conductor
        .call(RoomPost {
            room: room_ref.clone(),
            body: Body::parse("@council-gemini Post your review to @dave when done.").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();

    for attempt in 1..=3 {
        bb_bridge
            .call(WaveObserve {
                wave: wave_slug.clone(),
                role: role.clone(),
                observation: Observation::SpawnFailed {
                    attempt,
                    error: "HTTP 400: requires a machine selection".into(),
                },
            })
            .await
            .unwrap();
    }
    bb_bridge
        .call(WaveObserve {
            wave: wave_slug.clone(),
            role: role.clone(),
            observation: Observation::Activity {
                activity: Activity::Errored {
                    error: "Command thread.start failed".into(),
                },
            },
        })
        .await
        .unwrap();
    bb_bridge
        .call(WaveObserve {
            wave: wave_slug.clone(),
            role: role.clone(),
            observation: Observation::Activity {
                activity: Activity::Errored {
                    error: "ServiceUnavailableError (503)".into(),
                },
            },
        })
        .await
        .unwrap();

    let inbox = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = owner
                .call(InboxList {
                    include_done: true,
                    limit: None,
                })
                .await
                .unwrap();
            if page
                .items
                .iter()
                .any(|n| n.draft.preview.contains("ServiceUnavailableError"))
            {
                break page;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("updated stuck card arrived");

    assert_eq!(
        inbox.items.len(),
        1,
        "system mention suppressed and 5 worker failures coalesced into 1 card: {:?}",
        inbox.items
    );
    assert_eq!(
        inbox.items[0].draft.dedupe,
        "worker-stuck:demo-spec-r2:council-gemini"
    );
    assert!(matches!(
        &inbox.items[0].draft.kind,
        NotifyKind::WorkerStuck { why, .. } if why == "ServiceUnavailableError (503)"
    ));

    let sb = owner.call(Sidebar::default()).await.unwrap();
    assert_eq!(
        sb.projects[0].waves[0].mentions, 0,
        "system post naming @dave must not bump operator sidebar mention badge"
    );

    let stuck_id = inbox.items[0].id;
    owner
        .call(InboxAck {
            ids: vec![stuck_id],
            state: NotifyState::Dismissed,
        })
        .await
        .unwrap();
    bb_bridge
        .call(WaveObserve {
            wave: wave_slug.clone(),
            role: role.clone(),
            observation: Observation::Activity {
                activity: Activity::Errored {
                    error: "ServiceUnavailableError (503)".into(),
                },
            },
        })
        .await
        .unwrap();
    let reopened = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = owner
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            if page.unread == 1 {
                break page;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("dismissed stuck card reopened as Unread on a newer failure with identical text");
    assert_eq!(reopened.items.len(), 1);
    assert_eq!(reopened.items[0].id, stuck_id);
    assert_eq!(reopened.items[0].state, NotifyState::Unread);

    owner
        .call(InboxAck {
            ids: vec![stuck_id],
            state: NotifyState::Dismissed,
        })
        .await
        .unwrap();
    let pre_head = owner
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .unwrap()
        .head;
    herald_task.abort();
    let replay_task = spawn_reactor(
        Herald::new(),
        LocalClient::new(
            router.clone(),
            Principal::System {
                part: SystemPart::Herald,
            },
        ),
    );
    tokio::time::sleep(Duration::from_millis(60)).await;
    let replayed = owner
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(
        replayed.head, pre_head,
        "replaying multi-error worker_stuck history neither appends events nor reopens dismissed card"
    );
    assert_eq!(replayed.unread, 0);
    replay_task.abort();
}

#[tokio::test]
async fn set_archived_and_wave_finished_fold_into_sidebar_and_survive_replay() {
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Registry::<TestWaves>::new(journal.clone(), hub.clone()),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Arc::new(Unavailable(Family::Presence)),
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });

    let owner = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let sam = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: SeatName::parse("sam").unwrap(),
        },
    );

    let project = BbProjectId::parse("proj_demo_alpha").unwrap();
    let wave = WaveSlug::parse("demo-spec-r2").unwrap();
    let lounge = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("lounge").unwrap(),
    };
    let lounge_info = owner
        .call(RoomEnsure {
            room: lounge,
            topic: None,
            origin: None,
        })
        .await
        .unwrap();

    owner
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse("Spec review").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("codex").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Full,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![],
            },
            idem: None,
        })
        .await
        .unwrap();

    assert!(
        matches!(
            sam.call(SetArchived {
                target: ArchiveTarget::Project {
                    project: project.clone(),
                },
                archived: true,
            })
            .await,
            Err(ApiError::Denied { .. })
        ),
        "non-operator seat cannot archive sidebar branches"
    );

    let outcome = WaveOutcome::Completed {
        summary: Body::parse("Round 1 complete").unwrap(),
    };
    owner
        .call(WaveFinish {
            wave: wave.clone(),
            outcome: outcome.clone(),
            allow_incomplete: false,
        })
        .await
        .unwrap();
    owner
        .call(SetArchived {
            target: ArchiveTarget::Wave { wave: wave.clone() },
            archived: true,
        })
        .await
        .unwrap();
    owner
        .call(SetArchived {
            target: ArchiveTarget::Project {
                project: project.clone(),
            },
            archived: true,
        })
        .await
        .unwrap();
    owner
        .call(SetArchived {
            target: ArchiveTarget::Room {
                room: lounge_info.id.clone(),
            },
            archived: true,
        })
        .await
        .unwrap();

    let snap = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let s = owner.call(Sidebar::default()).await.unwrap();
            if s.projects.first().is_some_and(|p| {
                p.archived
                    && p.waves
                        .first()
                        .is_some_and(|w| w.archived && w.finished.is_some())
            }) && s.channels.first().is_some_and(|c| c.archived)
            {
                break s;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("sidebar folded finished outcome and archive flags");

    assert_eq!(snap.projects[0].waves[0].finished, Some(outcome.clone()));
    assert!(snap.projects[0].archived);
    assert!(snap.projects[0].waves[0].archived);
    assert!(snap.channels[0].archived);

    let rehydrated = SidebarProjection::spawn(journal.clone(), hub.clone()).snapshot(owner.me());
    assert_eq!(rehydrated.projects[0].waves[0].finished, Some(outcome));
    assert!(rehydrated.projects[0].archived);
    assert!(rehydrated.projects[0].waves[0].archived);
    assert!(rehydrated.channels[0].archived);

    owner
        .call(SetArchived {
            target: ArchiveTarget::Wave { wave },
            archived: false,
        })
        .await
        .unwrap();
    let unarchived = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let s = owner.call(Sidebar::default()).await.unwrap();
            if s.projects[0].waves.first().is_some_and(|w| !w.archived) {
                break s;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(!unarchived.projects[0].waves[0].archived);
}

#[tokio::test]
async fn legacy_per_seq_worker_stuck_cards_do_not_duplicate_on_herald_upgrade_replay() {
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Registry::<TestWaves>::new(journal.clone(), hub.clone()),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Arc::new(Unavailable(Family::Presence)),
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });

    let owner_p = Principal::Operator {
        seat: SeatName::parse("dave").unwrap(),
    };
    let owner = LocalClient::new(router.clone(), owner_p.clone());
    let bb_bridge = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::BbBridge,
        },
    );
    let herald_sys = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::Herald,
        },
    );

    let wave = WaveSlug::parse("demo-spec").unwrap();
    let role = RoleName::parse("council-claude").unwrap();
    owner
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: BbProjectId::parse("proj_demo_alpha").unwrap(),
                goal: Body::parse("Spec").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("claude-code").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Full,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("Review").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                }],
            },
            idem: None,
        })
        .await
        .unwrap();
    bb_bridge
        .call(WaveObserve {
            wave: wave.clone(),
            role: role.clone(),
            observation: Observation::SpawnFailed {
                attempt: 1,
                error: "requires a machine selection".into(),
            },
        })
        .await
        .unwrap();

    let wave_entries = journal
        .read(&StreamId::Wave { wave: wave.clone() }, Seq::ORIGIN, 50)
        .unwrap();
    let fail_seq = wave_entries.last().unwrap().seq;

    herald_sys
        .call(InboxRaise {
            to: owner_p.clone(),
            draft: NotificationDraft {
                kind: NotifyKind::WorkerStuck {
                    wave: wave.clone(),
                    role: role.clone(),
                    why: "requires a machine selection".into(),
                    seq: Seq::ORIGIN,
                },
                title: format!("@{wave}/{role} stuck"),
                preview: "requires a machine selection".into(),
                actions: Vec::new(),
                dedupe: format!("worker-stuck:{wave}:{role}:{}", fail_seq.0),
            },
        })
        .await
        .unwrap();
    owner
        .call(InboxAck {
            ids: vec![NotificationId(1)],
            state: NotifyState::Dismissed,
        })
        .await
        .unwrap();

    let before = owner
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(before.unread, 0);

    let herald_task = spawn_reactor(Herald::new(), herald_sys);
    tokio::time::sleep(Duration::from_millis(60)).await;
    let after = owner
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(
        after.head, before.head,
        "upgrading Herald over legacy worker-stuck:{{wave}}:{{role}}:{{seq}} cards raises zero new items"
    );
    assert_eq!(after.unread, 0);
    assert_eq!(after.items.len(), 1);

    bb_bridge
        .call(WaveObserve {
            wave: wave.clone(),
            role: role.clone(),
            observation: Observation::SpawnFailed {
                attempt: 2,
                error: "new failure after upgrade".into(),
            },
        })
        .await
        .unwrap();
    let adopted = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let p = owner
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            if p.unread == 1 {
                break p;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("newer failure adopted and reopened legacy card");
    assert_eq!(adopted.items.len(), 1);
    assert_eq!(adopted.items[0].id, NotificationId(1));
    assert_eq!(
        adopted.items[0].draft.dedupe,
        "worker-stuck:demo-spec:council-claude"
    );
    herald_task.abort();
}

fn letter_ref_for(seat: &str, name: &str, sha_char: char) -> LetterRef {
    let sha = Sha256Hex::parse(sha_char.to_string().repeat(64)).unwrap();
    let rel = PathBuf::from(format!("to-{seat}/{name}"));
    let item = MailboxItem {
        schema: MailSchemaV1,
        id: format!("letter:to-{seat}/{name}"),
        from: "carol".into(),
        to: seat.into(),
        kind: "letter".into(),
        state: "open".into(),
        ts: "2026-11-21T06:00:00Z".into(),
        ts_basis: "mtime".into(),
        reference: MailRef {
            store: "letters".into(),
            path: rel.clone(),
            sha256: sha.clone(),
        },
        topic: Some("presence".into()),
        round: Some(1),
        label: Some("Presence follow-up".into()),
    };
    LetterRef {
        path: rel,
        sha256: sha,
        wake_id: WakeId(format!("{sha_char}").repeat(16)),
        item,
    }
}

fn sample_seat_record(seat: &str, status: SeatStatus, at: UnixMs) -> SeatRecord {
    let seat_name = SeatName::parse(seat).unwrap();
    let by = Principal::Seat {
        seat: seat_name.clone(),
    };
    SeatRecord {
        seat: seat_name,
        status,
        status_provenance: Some(Provenance {
            asserted_by: by.clone(),
            verified_how: VerificationKind::SelfWord,
            at,
        }),
        aliases: Vec::new(),
        role: None,
        projects: Vec::new(),
        home_room: None,
        retired_at: if status == SeatStatus::Retired {
            Some(at)
        } else {
            None
        },
        mail_horizon_at: None,
        work_class: None,
        avatar: None,
        home_dir: None,
        start_cmd: None,
        resume_cmd: None,
        rex_target: None,
        claimed_by: by,
        claimed_at: at,
    }
}

fn sample_uds_binding(seat: &str, inc: &str, at: UnixMs) -> SeatBinding {
    SeatBinding {
        seat: SeatName::parse(seat).unwrap(),
        incarnation_id: IncarnationId::parse(inc).unwrap(),
        epoch: 1,
        lane: None,
        endpoint: TransportEndpoint::UdsSession {
            conn_id: 7,
            client: "presence-hold".into(),
        },
        liveness: LivenessMode::SocketLifetime,
        bound_at: at,
        expires_at: None,
        status: BindingStatus::Live,
    }
}

#[test]
fn mail_unrung_policy_enforces_status_grace_window_and_informational_copy() {
    let mut view = HeraldView::default();
    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    let dave = SeatName::parse("dave").unwrap();
    let nodal = SeatName::parse("nodal").unwrap();
    let quire = SeatName::parse("quire").unwrap();
    let room = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("lounge").unwrap(),
    };

    // Claim dave=Active, nodal=Dormant, quire=Retired.
    for (seq, seat, status) in [
        (1, "dave", SeatStatus::Active),
        (2, "nodal", SeatStatus::Dormant),
        (3, "quire", SeatStatus::Retired),
    ] {
        let env = Envelope {
            stream: StreamId::Presence,
            seq: Seq(seq),
            at: UnixMs(1_000),
            by: owner_p.clone(),
            event: Event::Presence(PresenceEvent::SeatClaimed {
                record: sample_seat_record(seat, status, UnixMs(1_000)),
            }),
        };
        view.observe(&env);
    }

    // Dormant and Retired seats raise ZERO MailUnrung cards.
    for (seq, seat, ch) in [(1, "nodal", 'a'), (2, "quire", 'b')] {
        let env = Envelope {
            stream: StreamId::Mail,
            seq: Seq(seq),
            at: UnixMs(2_000),
            by: Principal::Seat {
                seat: SeatName::parse("carol").unwrap(),
            },
            event: Event::Mail(MailEvent::Banked {
                from_room: room.clone(),
                range: (Seq(1), Seq(1)),
                letter: letter_ref_for(seat, "notice-r1.md", ch),
            }),
        };
        view.observe(&env);
        let emitted = classify(&env, &view);
        assert!(
            !emitted
                .iter()
                .any(|(_, d)| matches!(d.kind, NotifyKind::MailUnrung { .. })),
            "dormant/retired seat '{seat}' must never raise MailUnrung"
        );
    }
    assert_eq!(
        view.evaluate_mail_unrung(&nodal, UnixMs(100_000)),
        MailUnrungEval::Suppress
    );
    assert_eq!(
        view.evaluate_mail_unrung(&quire, UnixMs(100_000)),
        MailUnrungEval::Suppress
    );

    // Active seat that was NEVER bound has 0 s grace and raises immediately.
    let first_letter = Envelope {
        stream: StreamId::Mail,
        seq: Seq(3),
        at: UnixMs(3_000),
        by: Principal::Seat {
            seat: SeatName::parse("carol").unwrap(),
        },
        event: Event::Mail(MailEvent::Banked {
            from_room: room.clone(),
            range: (Seq(1), Seq(1)),
            letter: letter_ref_for("dave", "first-r1.md", 'c'),
        }),
    };
    view.observe(&first_letter);
    let emitted = classify(&first_letter, &view);
    let unrung: Vec<_> = emitted
        .iter()
        .filter(|(_, d)| matches!(d.kind, NotifyKind::MailUnrung { .. }))
        .collect();
    assert_eq!(unrung.len(), 1);
    let (to, draft) = unrung[0];
    assert_eq!(to, &owner_p);
    assert_eq!(draft.dedupe, "mail-unrung:dave");
    assert!(
        draft.actions.is_empty(),
        "informational status notice must carry zero decision buttons"
    );
    assert!(
        !draft.title.contains('?') && !draft.preview.contains('?'),
        "status notice must never ask the operator a question"
    );
    assert!(matches!(
        draft.kind,
        NotifyKind::MailUnrung {
            ref seat,
            unread_count: 1,
            oldest_at: UnixMs(3_000),
        } if seat == &dave
    ));

    // Reconnectable binding drop enforces the exact 15 s grace window.
    let eve = SeatName::parse("eve").unwrap();
    let claim_eve = Envelope {
        stream: StreamId::Presence,
        seq: Seq(4),
        at: UnixMs(10_000),
        by: owner_p.clone(),
        event: Event::Presence(PresenceEvent::SeatClaimed {
            record: sample_seat_record("eve", SeatStatus::Active, UnixMs(10_000)),
        }),
    };
    view.observe(&claim_eve);

    let bind_eve = Envelope {
        stream: StreamId::Presence,
        seq: Seq(5),
        at: UnixMs(10_000),
        by: Principal::Seat { seat: eve.clone() },
        event: Event::Presence(PresenceEvent::Bound {
            binding: sample_uds_binding("eve", "inc-eve-1", UnixMs(10_000)),
            superseded: Vec::new(),
        }),
    };
    view.observe(&bind_eve);

    let drop_eve = Envelope {
        stream: StreamId::Presence,
        seq: Seq(6),
        at: UnixMs(20_000),
        by: Principal::System {
            part: SystemPart::PresenceMonitor,
        },
        event: Event::Presence(PresenceEvent::Unbound {
            seat: eve.clone(),
            incarnation_id: IncarnationId::parse("inc-eve-1").unwrap(),
            status: BindingStatus::Unbound,
            reason: Some("socket EOF".into()),
        }),
    };
    view.observe(&drop_eve);

    // Letter arrives 5 s after socket drop -> still within 15 s grace window.
    let mail_during_grace = Envelope {
        stream: StreamId::Mail,
        seq: Seq(4),
        at: UnixMs(25_000),
        by: Principal::Seat {
            seat: SeatName::parse("carol").unwrap(),
        },
        event: Event::Mail(MailEvent::Banked {
            from_room: room.clone(),
            range: (Seq(1), Seq(1)),
            letter: letter_ref_for("eve", "grace-r1.md", 'd'),
        }),
    };
    view.observe(&mail_during_grace);
    assert!(
        !classify(&mail_during_grace, &view)
            .iter()
            .any(|(_, d)| d.dedupe == "mail-unrung:eve"),
        "must suppress MailUnrung while inside the 15 s reconnect grace window"
    );
    assert_eq!(
        view.evaluate_mail_unrung(&eve, UnixMs(20_000 + RECONNECT_GRACE_MS - 1)),
        MailUnrungEval::WithinGrace {
            ready_at: UnixMs(35_000),
            unread_count: 1,
            oldest_at: UnixMs(25_000),
        }
    );
    assert_eq!(
        view.evaluate_mail_unrung(&eve, UnixMs(20_000 + RECONNECT_GRACE_MS)),
        MailUnrungEval::Raise {
            unread_count: 1,
            oldest_at: UnixMs(25_000),
        }
    );

    // Fast redial inside the grace window suppresses alerting completely.
    let rebind_fast = Envelope {
        stream: StreamId::Presence,
        seq: Seq(7),
        at: UnixMs(27_000),
        by: Principal::Seat { seat: eve.clone() },
        event: Event::Presence(PresenceEvent::Bound {
            binding: sample_uds_binding("eve", "inc-eve-2", UnixMs(27_000)),
            superseded: Vec::new(),
        }),
    };
    view.observe(&rebind_fast);
    assert_eq!(
        view.evaluate_mail_unrung(&eve, UnixMs(50_000)),
        MailUnrungEval::Suppress,
        "fast reconnect within grace window must never raise MailUnrung"
    );
}

#[tokio::test]
async fn herald_deduplicates_per_seat_mail_unrung_and_auto_resolves_on_rebind_and_ack() {
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());

    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Arc::new(Unavailable(Family::Presence)),
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });

    let owner = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let herald_sys = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::Herald,
        },
    );
    let herald_task = spawn_reactor(Herald::new(), herald_sys.clone());

    let mut presence_writer = journal.claim(StreamId::Presence).unwrap();
    let mut mail_writer = journal.claim(StreamId::Mail).unwrap();
    let room = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("lounge").unwrap(),
    };
    let dave = SeatName::parse("dave").unwrap();
    let carol = Principal::Seat {
        seat: SeatName::parse("carol").unwrap(),
    };

    // Register Active seat dave (never bound -> 0 s grace).
    let claim = presence_writer
        .append(
            owner.me(),
            UnixMs(1_700_000_000_000),
            None,
            vec![Event::Presence(PresenceEvent::SeatClaimed {
                record: sample_seat_record("dave", SeatStatus::Active, UnixMs(1_700_000_000_000)),
            })],
        )
        .unwrap();
    hub.publish(claim.envelopes());

    // Two letters arrive while dave has no live binding -> ONE deduped card with unread_count = 2.
    let l1 = letter_ref_for("dave", "first-r1.md", '1');
    let l2 = letter_ref_for("dave", "second-r1.md", '2');
    let b1 = mail_writer
        .append(
            &carol,
            UnixMs(1_700_000_010_000),
            None,
            vec![Event::Mail(MailEvent::Banked {
                from_room: room.clone(),
                range: (Seq(1), Seq(1)),
                letter: l1.clone(),
            })],
        )
        .unwrap();
    hub.publish(b1.envelopes());

    let b2 = mail_writer
        .append(
            &carol,
            UnixMs(1_700_000_020_000),
            None,
            vec![Event::Mail(MailEvent::Banked {
                from_room: room,
                range: (Seq(2), Seq(2)),
                letter: l2.clone(),
            })],
        )
        .unwrap();
    hub.publish(b2.envelopes());

    let page = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let p = owner
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            let hit = p.items.iter().find(|n| {
                matches!(
                    &n.draft.kind,
                    NotifyKind::MailUnrung {
                        seat,
                        unread_count: 2,
                        oldest_at: UnixMs(1_700_000_010_000),
                    } if seat == &dave
                )
            });
            if hit.is_some() {
                break p;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Herald coalesced two unrung letters into one per-seat MailUnrung card");

    let unrung_cards: Vec<_> = page
        .items
        .iter()
        .filter(|n| matches!(n.draft.kind, NotifyKind::MailUnrung { .. }))
        .collect();
    assert_eq!(
        unrung_cards.len(),
        1,
        "must emit at most ONE MailUnrung card per Active seat"
    );
    assert_eq!(unrung_cards[0].draft.dedupe, "mail-unrung:dave");
    assert!(unrung_cards[0].draft.actions.is_empty());

    // Rebinding dave BEFORE ACKing must NOT auto-resolve the card yet.
    let rebind = presence_writer
        .append(
            &Principal::Seat { seat: dave.clone() },
            UnixMs(1_700_000_030_000),
            None,
            vec![Event::Presence(PresenceEvent::Bound {
                binding: sample_uds_binding("dave", "inc-dave-1", UnixMs(1_700_000_030_000)),
                superseded: Vec::new(),
            })],
        )
        .unwrap();
    hub.publish(rebind.envelopes());
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after_rebind = owner
        .call(InboxList {
            include_done: false,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(
        after_rebind
            .items
            .iter()
            .filter(|n| matches!(n.draft.kind, NotifyKind::MailUnrung { .. }))
            .count(),
        1,
        "rebinding without ACKing pending digests keeps MailUnrung open"
    );

    // Partial ACK (letter 1 of 2) decrements unread_count to 1, still open.
    let ack1 = mail_writer
        .append(
            &Principal::Seat { seat: dave.clone() },
            UnixMs(1_700_000_040_000),
            None,
            vec![Event::Mail(MailEvent::Acked {
                path: l1.path.clone(),
                sha256: l1.sha256.clone(),
            })],
        )
        .unwrap();
    hub.publish(ack1.envelopes());

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let p = owner
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            if p.items.iter().any(|n| {
                matches!(
                    &n.draft.kind,
                    NotifyKind::MailUnrung {
                        seat,
                        unread_count: 1,
                        oldest_at: UnixMs(1_700_000_020_000),
                    } if seat == &dave
                )
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("partial ACK updated unread_count to 1 and advanced oldest_at");

    // Final ACK (letter 2 of 2) auto-resolves MailUnrung to Done.
    let ack2 = mail_writer
        .append(
            &Principal::Seat { seat: dave.clone() },
            UnixMs(1_700_000_050_000),
            None,
            vec![Event::Mail(MailEvent::Acked {
                path: l2.path.clone(),
                sha256: l2.sha256.clone(),
            })],
        )
        .unwrap();
    hub.publish(ack2.envelopes());

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let p = owner
                .call(InboxList {
                    include_done: true,
                    limit: None,
                })
                .await
                .unwrap();
            if p.items
                .iter()
                .any(|n| n.draft.dedupe == "mail-unrung:dave" && n.state == NotifyState::Done)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("ACKing all pending digests auto-resolved MailUnrung to Done");

    // TransportDown circuit-breaker raises one deduped card and auto-resolves on recovery.
    let td = presence_writer
        .append(
            &Principal::System {
                part: SystemPart::PresenceMonitor,
            },
            UnixMs(1_700_000_060_000),
            None,
            vec![Event::Presence(PresenceEvent::TransportHealthChanged {
                transport: TransportKind::ClaudeSession,
                down: true,
                detail: Some("/tmp/cc-socks missing".into()),
            })],
        )
        .unwrap();
    hub.publish(td.envelopes());

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let p = owner
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            if p.items.iter().any(|n| {
                n.draft.dedupe == "transport-down:claude_session" && n.state == NotifyState::Unread
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("TransportDown raised deduped informational Operator card");

    let tr = presence_writer
        .append(
            &Principal::System {
                part: SystemPart::PresenceMonitor,
            },
            UnixMs(1_700_000_070_000),
            None,
            vec![Event::Presence(PresenceEvent::TransportHealthChanged {
                transport: TransportKind::ClaudeSession,
                down: false,
                detail: None,
            })],
        )
        .unwrap();
    hub.publish(tr.envelopes());

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let p = owner
                .call(InboxList {
                    include_done: true,
                    limit: None,
                })
                .await
                .unwrap();
            if p.items.iter().any(|n| {
                n.draft.dedupe == "transport-down:claude_session" && n.state == NotifyState::Done
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("TransportDown auto-resolved to Done when transport recovered");

    // OrgConflict cards deduplicate, carry informational copy, auto-resolve, and reopen on fresh conflict.
    let org_id = OrgId::parse("rw-task-42").unwrap();
    let conflict = org_conflict_draft(
        "board.org",
        org_id.clone(),
        "expected :REX_REV: 3, journal at 4",
    );
    assert!(conflict.actions.is_empty());
    assert!(!conflict.title.contains('?') && !conflict.preview.contains('?'));
    herald_sys
        .call(InboxRaise {
            to: owner.me().clone(),
            draft: conflict.clone(),
        })
        .await
        .unwrap();
    herald_sys
        .call(InboxRaise {
            to: owner.me().clone(),
            draft: conflict,
        })
        .await
        .unwrap();
    let with_conflict = owner
        .call(InboxList {
            include_done: false,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(
        with_conflict
            .items
            .iter()
            .filter(|n| n.draft.dedupe == "org-conflict:board.org:rw-task-42")
            .count(),
        1
    );

    herald_sys
        .call(InboxRaise {
            to: owner.me().clone(),
            draft: org_conflict_resolved_draft("board.org", org_id.clone()),
        })
        .await
        .unwrap();
    let after_org_clear = owner
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .unwrap();
    assert!(after_org_clear.items.iter().any(|n| {
        n.draft.dedupe == "org-conflict:board.org:rw-task-42" && n.state == NotifyState::Done
    }));

    // Verify clean helper constructors for MailUnrung and TransportDown directly too.
    let sample_mu = mail_unrung_draft(&dave, 3, UnixMs(1_700_000_000_000));
    assert_eq!(sample_mu.dedupe, "mail-unrung:dave");
    assert!(sample_mu.actions.is_empty());
    let sample_td = transport_down_draft(TransportKind::BbThread, None);
    assert_eq!(sample_td.dedupe, "transport-down:bb_thread");
    let sample_tr = transport_recovered_draft(TransportKind::BbThread);
    assert_eq!(sample_tr.dedupe, "transport-down:bb_thread");

    herald_task.abort();
}

#[tokio::test]
async fn herald_handle_flushes_15s_grace_with_zero_further_entries_and_suppresses_transport_down_mail_unrung()
 {
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());

    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Arc::new(Unavailable(Family::Presence)),
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });

    let owner = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let herald_sys = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::Herald,
        },
    );
    let (herald, handle) = Herald::with_handle();
    let herald_task = spawn_reactor(herald, herald_sys.clone());

    let mut presence_writer = journal.claim(StreamId::Presence).unwrap();
    let mut mail_writer = journal.claim(StreamId::Mail).unwrap();
    let room = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("lounge").unwrap(),
    };
    let eve = SeatName::parse("eve").unwrap();
    let carol = Principal::Seat {
        seat: SeatName::parse("carol").unwrap(),
    };

    let t0 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let init = presence_writer
        .append(
            owner.me(),
            UnixMs(t0),
            None,
            vec![
                Event::Presence(PresenceEvent::SeatClaimed {
                    record: sample_seat_record("eve", SeatStatus::Active, UnixMs(t0)),
                }),
                Event::Presence(PresenceEvent::Bound {
                    binding: sample_uds_binding("eve", "inc-eve-1", UnixMs(t0)),
                    superseded: Vec::new(),
                }),
                Event::Presence(PresenceEvent::Unbound {
                    seat: eve.clone(),
                    incarnation_id: IncarnationId::parse("inc-eve-1").unwrap(),
                    status: BindingStatus::Unbound,
                    reason: Some("socket EOF".into()),
                }),
            ],
        )
        .unwrap();
    hub.publish(init.envelopes());

    let banked = mail_writer
        .append(
            &carol,
            UnixMs(t0 + 2_000),
            None,
            vec![Event::Mail(MailEvent::Banked {
                from_room: room.clone(),
                range: (Seq(1), Seq(1)),
                letter: letter_ref_for("eve", "quiet-grace-r1.md", '9'),
            })],
        )
        .unwrap();
    hub.publish(banked.envelopes());

    // Wait until Herald observes the banked letter and registers the exact 15 s deadline.
    let deadline = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(dl) = handle.next_grace_deadline(UnixMs(t0 + 2_000)) {
                break dl;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("HeraldHandle exposes 15 s reconnect grace deadline after spawn_reactor");
    assert_eq!(deadline, UnixMs(t0 + RECONNECT_GRACE_MS));

    // Flushing 1 ms early raises nothing; flushing at exactly +15 s raises the card
    // with ZERO subsequent journal entries.
    handle
        .flush_due_at(UnixMs(t0 + RECONNECT_GRACE_MS - 1), &herald_sys)
        .await
        .unwrap();
    let before_due = owner
        .call(InboxList {
            include_done: false,
            limit: None,
        })
        .await
        .unwrap();
    assert!(
        !before_due
            .items
            .iter()
            .any(|n| n.draft.dedupe == "mail-unrung:eve"),
        "must not raise before 15 s deadline"
    );

    handle
        .flush_due_at(UnixMs(t0 + RECONNECT_GRACE_MS), &herald_sys)
        .await
        .unwrap();
    let after_due = owner
        .call(InboxList {
            include_done: false,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(
        after_due
            .items
            .iter()
            .filter(|n| n.draft.dedupe == "mail-unrung:eve")
            .count(),
        1,
        "flush_due_at raises MailUnrung after 15 s quiet period"
    );

    // S4 + S5 + S6 + S7 pure invariants:
    // 1. Unrelated room event does NOT re-emit an unchanged MailUnrung draft (S6).
    let mut view = HeraldView::default();
    let owner_p = owner.me().clone();
    view.observe(&Envelope {
        stream: StreamId::Presence,
        seq: Seq(1),
        at: UnixMs(1_000),
        by: owner_p.clone(),
        event: Event::Presence(PresenceEvent::SeatClaimed {
            record: sample_seat_record("dave", SeatStatus::Active, UnixMs(1_000)),
        }),
    });
    let mail_env = Envelope {
        stream: StreamId::Mail,
        seq: Seq(1),
        at: UnixMs(2_000),
        by: carol.clone(),
        event: Event::Mail(MailEvent::Banked {
            from_room: room.clone(),
            range: (Seq(1), Seq(1)),
            letter: letter_ref_for("dave", "once-r1.md", 'e'),
        }),
    };
    view.observe(&mail_env);
    assert_eq!(
        classify(&mail_env, &view)
            .iter()
            .filter(|(_, d)| d.dedupe == "mail-unrung:dave")
            .count(),
        1
    );
    let unrelated = Envelope {
        stream: StreamId::Room { room: room.clone() },
        seq: Seq(2),
        at: UnixMs(3_000),
        by: owner_p.clone(),
        event: Event::Room(RoomEvent::TopicSet {
            topic: "chat".into(),
        }),
    };
    view.observe(&unrelated);
    assert!(
        classify(&unrelated, &view).is_empty(),
        "unrelated events must not re-emit unchanged MailUnrung cards"
    );

    // 2. TransportDown suppresses per-seat MailUnrung and retains bindings created during outage (S4+S5).
    let mut td_view = HeraldView::default();
    let lucas = SeatName::parse("lucas").unwrap();
    td_view.observe(&Envelope {
        stream: StreamId::Presence,
        seq: Seq(1),
        at: UnixMs(1_000),
        by: Principal::System {
            part: SystemPart::PresenceMonitor,
        },
        event: Event::Presence(PresenceEvent::TransportHealthChanged {
            transport: TransportKind::BbThread,
            down: true,
            detail: Some("ws closed".into()),
        }),
    });
    td_view.observe(&Envelope {
        stream: StreamId::Presence,
        seq: Seq(2),
        at: UnixMs(1_500),
        by: Principal::Seat {
            seat: lucas.clone(),
        },
        event: Event::Presence(PresenceEvent::Bound {
            binding: SeatBinding {
                seat: lucas.clone(),
                incarnation_id: IncarnationId::parse("inc-lucas-1").unwrap(),
                epoch: 1,
                lane: None,
                endpoint: TransportEndpoint::BbThread {
                    thread_id: BbThreadId::parse("th_loupe").unwrap(),
                },
                liveness: LivenessMode::SocketLifetime,
                bound_at: UnixMs(1_500),
                expires_at: None,
                status: BindingStatus::TransportDown,
            },
            superseded: Vec::new(),
        }),
    });
    td_view.observe(&Envelope {
        stream: StreamId::Mail,
        seq: Seq(1),
        at: UnixMs(2_000),
        by: carol,
        event: Event::Mail(MailEvent::Banked {
            from_room: room,
            range: (Seq(1), Seq(1)),
            letter: letter_ref_for("lucas", "during-outage-r1.md", 'f'),
        }),
    });
    assert_eq!(
        td_view.evaluate_mail_unrung(&lucas, UnixMs(2_000)),
        MailUnrungEval::Suppress,
        "MailUnrung suppressed during provider TransportDown outage"
    );
    td_view.observe(&Envelope {
        stream: StreamId::Presence,
        seq: Seq(3),
        at: UnixMs(3_000),
        by: Principal::System {
            part: SystemPart::PresenceMonitor,
        },
        event: Event::Presence(PresenceEvent::TransportHealthChanged {
            transport: TransportKind::BbThread,
            down: false,
            detail: None,
        }),
    });
    assert_eq!(
        td_view.evaluate_mail_unrung(&lucas, UnixMs(3_000)),
        MailUnrungEval::Suppress,
        "binding created during outage becomes live automatically on transport recovery"
    );

    // 3. Lapsed lease expires_at is honored by evaluate_mail_unrung (S7).
    let mut lease_view = HeraldView::default();
    let baton = SeatName::parse("baton").unwrap();
    lease_view.observe(&Envelope {
        stream: StreamId::Presence,
        seq: Seq(1),
        at: UnixMs(1_000),
        by: Principal::Seat {
            seat: baton.clone(),
        },
        event: Event::Presence(PresenceEvent::Bound {
            binding: SeatBinding {
                seat: baton.clone(),
                incarnation_id: IncarnationId::parse("inc-baton-1").unwrap(),
                epoch: 1,
                lane: None,
                endpoint: TransportEndpoint::SelfWatcher {
                    harness: "codex".into(),
                    thread_id: None,
                    pid: None,
                    proc_start: None,
                },
                liveness: LivenessMode::LeaseOnly,
                bound_at: UnixMs(1_000),
                expires_at: Some(UnixMs(5_000)),
                status: BindingStatus::Live,
            },
            superseded: Vec::new(),
        }),
    });
    lease_view.observe(&Envelope {
        stream: StreamId::Mail,
        seq: Seq(1),
        at: UnixMs(6_000),
        by: owner_p,
        event: Event::Mail(MailEvent::Banked {
            from_room: RoomId::Channel {
                scope: RoomScope::Global,
                name: ChannelName::parse("lounge").unwrap(),
            },
            range: (Seq(1), Seq(1)),
            letter: letter_ref_for("baton", "after-lease-r1.md", 'a'),
        }),
    });
    assert_eq!(
        lease_view.evaluate_mail_unrung(&baton, UnixMs(6_000)),
        MailUnrungEval::Raise {
            unread_count: 1,
            oldest_at: UnixMs(6_000),
        },
        "lapsed LeaseOnly binding must not look live"
    );

    // R2-3: A custom operator seen after the first MailUnrung raise receives the card.
    let custom_op = Principal::Operator {
        seat: SeatName::parse("operator").unwrap(),
    };
    let custom_op_env = Envelope {
        stream: StreamId::Room {
            room: RoomId::Channel {
                scope: RoomScope::Global,
                name: ChannelName::parse("lounge").unwrap(),
            },
        },
        seq: Seq(2),
        at: UnixMs(7_000),
        by: custom_op.clone(),
        event: Event::Room(RoomEvent::TopicSet {
            topic: "ops".into(),
        }),
    };
    lease_view.observe(&custom_op_env);
    let op_drafts = classify(&custom_op_env, &lease_view);
    assert!(
        op_drafts
            .iter()
            .any(|(to, d)| to == &custom_op && d.dedupe == "mail-unrung:baton"),
        "newly observed operator must receive existing MailUnrung card"
    );

    herald_task.abort();
}

#[test]
fn mail_unrung_suppresses_operator_non_seat_targets_and_pre_horizon_history() {
    let mut view = HeraldView::default();
    let owner = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    let mut carol_rec = sample_seat_record("carol", SeatStatus::Active, UnixMs(10_000));
    carol_rec.mail_horizon_at = Some(UnixMs(10_000));

    // Claim carol with mail_horizon_at = 10_000.
    view.observe(&Envelope {
        stream: StreamId::Presence,
        seq: Seq(1),
        at: UnixMs(10_000),
        by: owner.clone(),
        event: Event::Presence(PresenceEvent::SeatClaimed { record: carol_rec }),
    });

    // Letters to `operator`, `cc`, and pre-horizon letter to `carol` (at 5_000 < 10_000).
    for (idx, (to, sha_ch, at)) in [
        ("operator", '1', UnixMs(20_000)),
        ("cc", '2', UnixMs(20_000)),
        ("all-seats", '3', UnixMs(20_000)),
        ("carol", '4', UnixMs(5_000)),
    ]
    .into_iter()
    .enumerate()
    {
        let env = Envelope {
            stream: StreamId::Mail,
            seq: Seq(idx as u64 + 1),
            at,
            by: owner.clone(),
            event: Event::Mail(MailEvent::Projected {
                item: letter_ref_for(to, "msg.md", sha_ch).item,
            }),
        };
        view.observe(&env);
        assert!(
            !classify(&env, &view)
                .iter()
                .any(|(_, d)| matches!(d.kind, NotifyKind::MailUnrung { .. })),
            "must never raise MailUnrung for target '{to}' at {}",
            at.0
        );
    }

    assert_eq!(
        view.evaluate_mail_unrung(&SeatName::parse("operator").unwrap(), UnixMs(30_000)),
        MailUnrungEval::Suppress
    );
    assert_eq!(
        view.evaluate_mail_unrung(&SeatName::parse("cc").unwrap(), UnixMs(30_000)),
        MailUnrungEval::Suppress
    );
    assert_eq!(
        view.evaluate_mail_unrung(&SeatName::parse("carol").unwrap(), UnixMs(30_000)),
        MailUnrungEval::Suppress
    );

    // Post-horizon letter to offline `carol` (at 12_000 >= 10_000) DOES raise MailUnrung.
    let fresh_env = Envelope {
        stream: StreamId::Mail,
        seq: Seq(10),
        at: UnixMs(12_000),
        by: owner,
        event: Event::Mail(MailEvent::Projected {
            item: letter_ref_for("carol", "fresh.md", '5').item,
        }),
    };
    view.observe(&fresh_env);
    assert_eq!(
        view.evaluate_mail_unrung(&SeatName::parse("carol").unwrap(), UnixMs(30_000)),
        MailUnrungEval::Raise {
            unread_count: 1,
            oldest_at: UnixMs(12_000),
        }
    );
}

#[tokio::test]
async fn worker_refused_raises_high_salience_inbox_card_and_inbox_reports_truncation() {
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(512);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Registry::<TestWaves>::new(journal.clone(), hub.clone()),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Arc::new(Unavailable(Family::Presence)),
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });
    let herald_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::Herald,
        },
    );
    let herald_task = spawn_reactor(Herald::new(), herald_client);
    let owner = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let bb = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::BbBridge,
        },
    );
    let wave = WaveSlug::parse("honesty-refusal").unwrap();
    let role = RoleName::parse("auditor").unwrap();

    owner
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: BbProjectId::parse("proj_3vs7nkyeum").unwrap(),
                goal: Body::parse("Audit safety").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("claude-code").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Full,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("Audit").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                }],
            },
            idem: None,
        })
        .await
        .unwrap();

    let long_reason = format!("cyber_safeguard: {}", "x".repeat(220));
    bb.call(WaveObserve {
        wave: wave.clone(),
        role: role.clone(),
        observation: Observation::Activity {
            activity: Activity::Refused {
                reason: long_reason.clone(),
            },
        },
    })
    .await
    .unwrap();
    // Raise a lower-rank stuck card on another role so we can verify ordering & limit truncation.
    bb.call(WaveObserve {
        wave: wave.clone(),
        role: RoleName::parse("helper").unwrap(),
        observation: Observation::SpawnFailed {
            attempt: 1,
            error: "boom".into(),
        },
    })
    .await
    .unwrap();

    let mut tap = hub.tap();
    let full = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = owner
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            if page.items.len() >= 2 {
                return page;
            }
            let _ = tap.recv().await;
        }
    })
    .await
    .unwrap();

    assert_eq!(full.total_count, 2);
    assert!(!full.truncated);
    assert!(
        matches!(
            &full.items[0].draft.kind,
            NotifyKind::WorkerRefused { role: r, .. } if *r == role
        ),
        "WorkerRefused must outrank WorkerStuck: {:?}",
        full.items
    );
    assert!(
        full.items[0].draft.preview.contains("chars shown"),
        "truncated preview must show N of M marker: {}",
        full.items[0].draft.preview
    );
    assert!(
        rank(&full.items[0].draft.kind, 0) > rank(&full.items[1].draft.kind, 0),
        "WorkerRefused rank should exceed WorkerStuck rank"
    );

    let capped = owner
        .call(InboxList {
            include_done: false,
            limit: Some(1),
        })
        .await
        .unwrap();
    assert_eq!(capped.items.len(), 1);
    assert_eq!(capped.total_count, 2);
    assert!(capped.truncated);

    // Dismissing then receiving a newer refusal seq must reopen to Unread.
    owner
        .call(InboxAck {
            ids: vec![full.items[0].id],
            state: NotifyState::Dismissed,
        })
        .await
        .unwrap();
    bb.call(WaveObserve {
        wave: wave.clone(),
        role: role.clone(),
        observation: Observation::Activity {
            activity: Activity::Refused {
                reason: "second refusal".into(),
            },
        },
    })
    .await
    .unwrap();

    let reopened = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = owner
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            if let Some(item) = page.items.iter().find(|n| {
                matches!(
                    &n.draft.kind,
                    NotifyKind::WorkerRefused { reason, .. } if reason == "second refusal"
                )
            }) {
                return item.clone();
            }
            let _ = tap.recv().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(reopened.state, NotifyState::Unread);
    herald_task.abort();
}
