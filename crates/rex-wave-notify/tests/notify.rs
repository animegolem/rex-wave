use rex_wave_api::mail::*;
use rex_wave_api::notify::*;
use rex_wave_api::read::{InboxBadge, Sidebar};
use rex_wave_api::room::*;
use rex_wave_api::stage::{Osc7501BlockKind, Osc7501State, derive_osc7501_status};
use rex_wave_api::wave::*;
use rex_wave_api::*;
use rex_wave_notify::{Herald, Inbox, rank};
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
            WaveCommand::WaveView(_) => WireReply::WaveView(WaveSnapshot {
                spec: state.spec.clone().unwrap(),
                status: WaveStatus::Running,
                workers: state
                    .workers
                    .iter()
                    .map(|(r, (a, p))| (r.clone(), a.clone(), p.clone()))
                    .collect(),
            }),
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
                    reasoning: Some("xhigh".into()),
                    service_tier: None,
                    permission: PermissionMode::AcceptEdits,
                    workspace: Workspace::ProjectDefault,
                },
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("Migrate tokens").unwrap(),
                    agent: None,
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
        seat: SeatName::parse("genga").unwrap(),
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

    let wave_slug = WaveSlug::parse("pancake-spec-r2").unwrap();
    let role = RoleName::parse("council-gemini").unwrap();
    let started = owner
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave_slug.clone(),
                project: BbProjectId::parse("proj_edaym59nyn").unwrap(),
                goal: Body::parse("Review spec").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("acp-opencode").unwrap(),
                    model: Some(ModelId::parse("opencode/gemini-4-argon").unwrap()),
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Full,
                    workspace: Workspace::ProjectDefault,
                },
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("Post review to @genga").unwrap(),
                    agent: None,
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
            body: Body::parse("@council-gemini Post your review to @genga when done.").unwrap(),
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
        "worker-stuck:pancake-spec-r2:council-gemini"
    );
    assert!(matches!(
        &inbox.items[0].draft.kind,
        NotifyKind::WorkerStuck { why, .. } if why == "ServiceUnavailableError (503)"
    ));

    let sb = owner.call(Sidebar::default()).await.unwrap();
    assert_eq!(
        sb.projects[0].waves[0].mentions, 0,
        "system post naming @genga must not bump operator sidebar mention badge"
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

    let project = BbProjectId::parse("proj_edaym59nyn").unwrap();
    let wave = WaveSlug::parse("pancake-spec-r2").unwrap();
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
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Full,
                    workspace: Workspace::ProjectDefault,
                },
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
        seat: SeatName::parse("genga").unwrap(),
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

    let wave = WaveSlug::parse("pancake-spec").unwrap();
    let role = RoleName::parse("council-claude").unwrap();
    owner
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: BbProjectId::parse("proj_edaym59nyn").unwrap(),
                goal: Body::parse("Spec").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("claude-code").unwrap(),
                    model: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Full,
                    workspace: Workspace::ProjectDefault,
                },
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("Review").unwrap(),
                    agent: None,
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
        "worker-stuck:pancake-spec:council-claude"
    );
    herald_task.abort();
}
