//! State-machine and replay conformance suite for `rex-wave-notify`.
//!
//! Proves `Herald` cold-journal replay (`observe_replay` + `CaughtUp`) produces
//! the exact same per-principal `Inbox` state as live frame-by-frame delivery
//! (`CaughtUp` + `observe`), and verifies the `MailUnrung` reconnect grace window.

use proptest::prelude::*;
use rex_wave_api::board::*;
use rex_wave_api::mail::*;
use rex_wave_api::notify::*;
use rex_wave_api::presence::*;
use rex_wave_api::room::*;
use rex_wave_api::wave::*;
use rex_wave_api::*;
use rex_wave_notify::{Herald, Inbox};
use rex_wave_runtime::{
    Hub, LocalClient, NoAgentFeed, Reactor, ReadSide, Registry, Router, SidebarProjection,
};
use rex_wave_store::Journal;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn sha_hex(tag: u8) -> Sha256Hex {
    Sha256Hex::parse(format!("{tag:064x}")).expect("sha256")
}

#[derive(Clone, Debug)]
enum NotifyStreamOp {
    ChannelMention { from_alice: bool },
    DirectMessage { from_alice: bool },
    WaveBlocked,
    WaveResolved,
    WaveRefused,
    WaveErrored,
    WaveFinished,
    CardAssigned,
    TransportHealth { down: bool },
    SeatBindDropAndBank { ack_after: bool },
}

fn stream_op_strategy() -> impl Strategy<Value = NotifyStreamOp> {
    prop_oneof![
        any::<bool>().prop_map(|from_alice| NotifyStreamOp::ChannelMention { from_alice }),
        any::<bool>().prop_map(|from_alice| NotifyStreamOp::DirectMessage { from_alice }),
        Just(NotifyStreamOp::WaveBlocked),
        Just(NotifyStreamOp::WaveResolved),
        Just(NotifyStreamOp::WaveRefused),
        Just(NotifyStreamOp::WaveErrored),
        Just(NotifyStreamOp::WaveFinished),
        Just(NotifyStreamOp::CardAssigned),
        any::<bool>().prop_map(|down| NotifyStreamOp::TransportHealth { down }),
        any::<bool>().prop_map(|ack_after| NotifyStreamOp::SeatBindDropAndBank { ack_after }),
    ]
}

fn build_inbox_router() -> Arc<Router> {
    let journal = Journal::in_memory().expect("journal");
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    Arc::new(Router {
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }))
    })
}

fn normalized_inbox_items(page: &InboxPage) -> Vec<(String, String, String, String, String)> {
    let mut out: Vec<_> = page
        .items
        .iter()
        .map(|n| {
            (
                n.draft.dedupe.clone(),
                format!("{:?}", n.state),
                n.draft.title.clone(),
                n.draft.preview.clone(),
                format!("{:?}", n.draft.kind),
            )
        })
        .collect();
    out.sort();
    out
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn proptest_herald_replay_vs_live_inbox_equivalence(
        ops in prop::collection::vec(stream_op_strategy(), 1..25)
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio rt");

        rt.block_on(async move {
            let owner = Principal::Operator {
                seat: SeatName::parse("owner").expect("seat"),
            };
            let alice_seat = SeatName::parse("alice").expect("seat");
            let bob_seat = SeatName::parse("bob").expect("seat");
            let alice = Principal::Seat {
                seat: alice_seat.clone(),
            };
            let bob = Principal::Seat {
                seat: bob_seat.clone(),
            };
            let sys_bb = Principal::System {
                part: SystemPart::BbBridge,
            };

            let general = RoomId::Channel {
                scope: RoomScope::Global,
                name: ChannelName::parse("general").expect("chan"),
            };
            let dm_room = RoomRef::Dm { with: bob.clone() }
                .resolve(&alice)
                .expect("dm room");
            let wave = WaveSlug::parse("w-herald").expect("wave");
            let role = RoleName::parse("auditor").expect("role");
            let proj = BbProjectId::parse("proj_demo").expect("proj");
            let board = project_board(&proj);

            let mut envelopes: Vec<Envelope> = Vec::new();
            let mut stream_seqs: std::collections::BTreeMap<StreamId, u64> =
                std::collections::BTreeMap::new();
            let mut now_ms = 1_795_260_000_000u64;

            let mut push_env = |stream: StreamId, by: Principal, event: Event, dt_ms: u64| {
                now_ms += dt_ms;
                let s = stream_seqs.entry(stream.clone()).or_insert(0);
                *s += 1;
                envelopes.push(Envelope {
                    stream,
                    seq: Seq(*s),
                    at: UnixMs(now_ms),
                    by,
                    event,
                });
            };

            // Seed baseline room, wave, and presence records so every generated event has context.
            push_env(
                StreamId::Room {
                    room: general.clone(),
                },
                owner.clone(),
                Event::Room(RoomEvent::Opened {
                    id: general.clone(),
                    origin: RoomOrigin::Manual,
                    topic: None,
                }),
                10,
            );
            for (p, h) in [(&alice, "alice"), (&bob, "bob")] {
                push_env(
                    StreamId::Room {
                        room: general.clone(),
                    },
                    p.clone(),
                    Event::Room(RoomEvent::MemberJoined {
                        member: p.clone(),
                        handle: Handle::parse(h).expect("handle"),
                    }),
                    10,
                );
            }
            push_env(
                StreamId::Wave { wave: wave.clone() },
                owner.clone(),
                Event::Wave(WaveEvent::Started {
                    spec: WaveSpec {
                        slug: wave.clone(),
                        project: proj,
                        goal: Body::parse("Verify herald").expect("goal"),
                        defaults: AgentSpec {
                            provider: ProviderId::parse("codex").expect("provider"),
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
                            brief: Body::parse("Audit").expect("brief"),
                            agent: None,
                            output_dir: None,
                            writes: Vec::new(),
                            return_schema: None,
                        }],
                    },
                    room: general.clone(),
                    board: board.clone(),
                    request_digest: None,
                }),
                10,
            );

            let mut card_rev = 0u64;
            let mut letter_idx = 0u8;

            for op in ops {
                match op {
                    NotifyStreamOp::ChannelMention { from_alice } => {
                        let (sender, recipient, text) = if from_alice {
                            (alice.clone(), bob.clone(), "ping @bob")
                        } else {
                            (bob.clone(), alice.clone(), "ping @alice")
                        };
                        push_env(
                            StreamId::Room {
                                room: general.clone(),
                            },
                            sender,
                            Event::Room(RoomEvent::Posted {
                                body: Body::parse(text).expect("body"),
                                mentions: Mentions {
                                    principals: vec![recipient],
                                    all: false,
                                },
                                re: None,
                                after: None,
                                labels: Vec::new(),
                                task: None,
                                idem: None,
                                origin: PostOrigin::Direct,
                            }),
                            50,
                        );
                    }
                    NotifyStreamOp::DirectMessage { from_alice } => {
                        let sender = if from_alice { alice.clone() } else { bob.clone() };
                        push_env(
                            StreamId::Room {
                                room: dm_room.clone(),
                            },
                            sender,
                            Event::Room(RoomEvent::Posted {
                                body: Body::parse("direct update").expect("body"),
                                mentions: Mentions::default(),
                                re: None,
                                after: None,
                                labels: Vec::new(),
                                task: None,
                                idem: None,
                                origin: PostOrigin::Direct,
                            }),
                            50,
                        );
                    }
                    NotifyStreamOp::WaveBlocked => {
                        push_env(
                            StreamId::Wave { wave: wave.clone() },
                            sys_bb.clone(),
                            Event::Wave(WaveEvent::WorkerObserved {
                                role: role.clone(),
                                observation: Observation::activity(Activity::Blocked {
                                    interaction: PendingInteraction {
                                        id: BbInteractionId::parse("ix_gate").expect("ix"),
                                        kind: InteractionKind::Permission,
                                        summary: "Need approval".into(),
                                    },
                                }),
                            }),
                            50,
                        );
                    }
                    NotifyStreamOp::WaveResolved => {
                        push_env(
                            StreamId::Wave { wave: wave.clone() },
                            owner.clone(),
                            Event::Wave(WaveEvent::InteractionResolved {
                                role: role.clone(),
                                interaction: BbInteractionId::parse("ix_gate").expect("ix"),
                                resolution: InteractionResolution::Deny,
                            }),
                            50,
                        );
                    }
                    NotifyStreamOp::WaveRefused => {
                        push_env(
                            StreamId::Wave { wave: wave.clone() },
                            sys_bb.clone(),
                            Event::Wave(WaveEvent::WorkerObserved {
                                role: role.clone(),
                                observation: Observation::activity(Activity::Refused {
                                    reason: "safety_filter".into(),
                                }),
                            }),
                            50,
                        );
                    }
                    NotifyStreamOp::WaveErrored => {
                        push_env(
                            StreamId::Wave { wave: wave.clone() },
                            sys_bb.clone(),
                            Event::Wave(WaveEvent::WorkerObserved {
                                role: role.clone(),
                                observation: Observation::activity(Activity::Errored {
                                    error: "upstream_503".into(),
                                }),
                            }),
                            50,
                        );
                    }
                    NotifyStreamOp::WaveFinished => {
                        push_env(
                            StreamId::Wave { wave: wave.clone() },
                            owner.clone(),
                            Event::Wave(WaveEvent::Finished {
                                outcome: WaveOutcome::Completed {
                                    summary: Body::parse("All checks green").expect("body"),
                                },
                            }),
                            50,
                        );
                    }
                    NotifyStreamOp::CardAssigned => {
                        card_rev += 1;
                        let ev = BoardEvent::CardPatched {
                            no: CardNo(1),
                            revision: CardRevision(card_rev),
                            patch: CardPatch {
                                assignee: Some(Assign::To {
                                    who: Assignee::parse("@alice").expect("assignee"),
                                }),
                                title: Some(
                                    Title::parse(format!("Review v{card_rev}")).expect("t"),
                                ),
                                ..Default::default()
                            },
                        };
                        push_env(
                            StreamId::Board {
                                board: board.clone(),
                            },
                            owner.clone(),
                            Event::Board(ev),
                            50,
                        );
                    }
                    NotifyStreamOp::TransportHealth { down } => {
                        push_env(
                            StreamId::Presence,
                            sys_bb.clone(),
                            Event::Presence(PresenceEvent::TransportHealthChanged {
                                transport: TransportKind::BbThread,
                                down,
                                detail: down.then(|| "ws disconnected".into()),
                            }),
                            50,
                        );
                    }
                    NotifyStreamOp::SeatBindDropAndBank { ack_after } => {
                        letter_idx = letter_idx.wrapping_add(1);
                        let sha = sha_hex(letter_idx);
                        let rel = PathBuf::from(format!("to-alice/letter-{letter_idx}.md"));
                        let inc = IncarnationId::parse("inc-alice").expect("inc");
                        push_env(
                            StreamId::Presence,
                            alice.clone(),
                            Event::Presence(PresenceEvent::Bound {
                                binding: SeatBinding {
                                    seat: alice_seat.clone(),
                                    incarnation_id: inc.clone(),
                                    epoch: u64::from(letter_idx) + 1,
                                    machine_id: None,
                                    lane: None,
                                    endpoint: TransportEndpoint::UdsSession {
                                        conn_id: 1,
                                        client: "cli".into(),
                                    },
                                    liveness: LivenessMode::SocketLifetime,
                                    bound_at: UnixMs(
                                        1_795_260_000_000 + u64::from(letter_idx) * 100,
                                    ),
                                    expires_at: None,
                                    canary_token: None,
                                    status: BindingStatus::Live,
                                },
                                superseded: Vec::new(),
                            }),
                            10,
                        );
                        push_env(
                            StreamId::Presence,
                            alice.clone(),
                            Event::Presence(PresenceEvent::Unbound {
                                seat: alice_seat.clone(),
                                incarnation_id: inc,
                                status: BindingStatus::Unbound,
                                reason: Some("dropped".into()),
                            }),
                            10,
                        );
                        push_env(
                            StreamId::Mail,
                            owner.clone(),
                            Event::Mail(MailEvent::Banked {
                                from_room: general.clone(),
                                range: (Seq(1), Seq(1)),
                                letter: LetterRef {
                                    path: rel.clone(),
                                    sha256: sha.clone(),
                                    wake_id: WakeId(format!("{letter_idx:016x}")),
                                    item: MailboxItem {
                                        schema: MailSchemaV1,
                                        id: format!("letter:{letter_idx}"),
                                        from: "owner".into(),
                                        to: "alice".into(),
                                        kind: "letter".into(),
                                        state: "open".into(),
                                        ts: "2026-11-24T00:00:00Z".into(),
                                        ts_basis: "letter_id_ms".into(),
                                        reference: MailRef {
                                            store: "letters".into(),
                                            path: rel.clone(),
                                            sha256: sha.clone(),
                                        },
                                        topic: None,
                                        round: None,
                                        label: Some(format!("Notice {letter_idx}")),
                                    },
                                },
                            }),
                            20_000, // advance past 15s grace window so MailUnrung is eligible
                        );
                        if ack_after {
                            push_env(
                                StreamId::Mail,
                                alice.clone(),
                                Event::Mail(MailEvent::Acked {
                                    path: rel,
                                    sha256: sha,
                                    seat: None,
                                    note: None,
                                }),
                                50,
                            );
                        }
                    }
                }
            }

            let final_now = UnixMs(now_ms + 20_000);

            // 1. Live delivery: CaughtUp first, then Committed one by one + due grace flush.
            let live_router = build_inbox_router();
            let live_herald_client = LocalClient::new(
                live_router.clone(),
                Principal::System {
                    part: SystemPart::Herald,
                },
            );
            let mut live_herald = Herald::with_grace(Duration::from_millis(15_000));
            live_herald
                .react(
                    Frame::CaughtUp {
                        cursor: Cursor::default(),
                    },
                    &live_herald_client,
                )
                .await
                .expect("live caught up");
            for env in &envelopes {
                live_herald
                    .react(Frame::Committed(env.clone()), &live_herald_client)
                    .await
                    .expect("live react");
            }
            live_herald
                .flush_due_at(final_now, &live_herald_client)
                .await
                .expect("live flush");

            // 2. Cold journal replay: all Committed while caught_up == false, then CaughtUp + due flush.
            let replay_router = build_inbox_router();
            let replay_herald_client = LocalClient::new(
                replay_router.clone(),
                Principal::System {
                    part: SystemPart::Herald,
                },
            );
            let mut replay_herald = Herald::with_grace(Duration::from_millis(15_000));
            for env in &envelopes {
                replay_herald
                    .react(Frame::Committed(env.clone()), &replay_herald_client)
                    .await
                    .expect("replay observe");
            }
            replay_herald
                .react(
                    Frame::CaughtUp {
                        cursor: Cursor::default(),
                    },
                    &replay_herald_client,
                )
                .await
                .expect("replay caught up");
            replay_herald
                .flush_due_at(final_now, &replay_herald_client)
                .await
                .expect("replay flush");

            for who in [&owner, &alice, &bob] {
                let live_active = LocalClient::new(live_router.clone(), who.clone())
                    .call(InboxList {
                        include_done: false,
                        limit: None,
                    })
                    .await
                    .expect("live active list");
                let replay_active = LocalClient::new(replay_router.clone(), who.clone())
                    .call(InboxList {
                        include_done: false,
                        limit: None,
                    })
                    .await
                    .expect("replay active list");

                assert_eq!(
                    live_active.unread, replay_active.unread,
                    "unread mismatch for {who:?}"
                );
                assert_eq!(
                    live_active.cursors, replay_active.cursors,
                    "cursor mismatch for {who:?}"
                );
                assert_eq!(
                    normalized_inbox_items(&live_active),
                    normalized_inbox_items(&replay_active),
                    "Active Inbox items diverged between live Herald and cold-replay Herald for {who:?}"
                );

                let live_all = LocalClient::new(live_router.clone(), who.clone())
                    .call(InboxList {
                        include_done: true,
                        limit: None,
                    })
                    .await
                    .expect("live full list");
                let replay_all = LocalClient::new(replay_router.clone(), who.clone())
                    .call(InboxList {
                        include_done: true,
                        limit: None,
                    })
                    .await
                    .expect("replay full list");
                let strip_transient_historical_unrung =
                    |items: Vec<(String, String, String, String, String)>| -> Vec<_> {
                        items
                            .into_iter()
                            .filter(|row| !(row.0.starts_with("mail-unrung:") && row.1 == "Done"))
                            .collect()
                    };
                assert_eq!(
                    strip_transient_historical_unrung(normalized_inbox_items(&live_all)),
                    strip_transient_historical_unrung(normalized_inbox_items(&replay_all)),
                    "Full (include_done=true) Inbox items diverged between live Herald and cold-replay Herald for {who:?}"
                );
            }
        });
    }
}

#[tokio::test]
async fn herald_mail_unrung_grace_window_suppresses_within_grace_and_resolves_on_ack() {
    let router = build_inbox_router();
    let herald_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::Herald,
        },
    );
    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").expect("seat"),
    };
    let owner = LocalClient::new(router.clone(), owner_p.clone());
    let alice_seat = SeatName::parse("alice").expect("seat");
    let alice_p = Principal::Seat {
        seat: alice_seat.clone(),
    };
    let general = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("general").expect("chan"),
    };

    let mut herald = Herald::with_grace(Duration::from_millis(15_000));
    herald
        .react(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &herald_client,
        )
        .await
        .expect("caught up");

    // Seed operator visibility by recording an operator event.
    herald
        .react(
            Frame::Committed(Envelope {
                stream: StreamId::Room {
                    room: general.clone(),
                },
                seq: Seq(1),
                at: UnixMs(1_000),
                by: owner_p.clone(),
                event: Event::Room(RoomEvent::Opened {
                    id: general.clone(),
                    origin: RoomOrigin::Manual,
                    topic: None,
                }),
            }),
            &herald_client,
        )
        .await
        .expect("open room");

    let inc = IncarnationId::parse("inc-1").expect("inc");
    herald
        .react(
            Frame::Committed(Envelope {
                stream: StreamId::Presence,
                seq: Seq(1),
                at: UnixMs(1_500),
                by: alice_p.clone(),
                event: Event::Presence(PresenceEvent::Bound {
                    binding: SeatBinding {
                        seat: alice_seat.clone(),
                        incarnation_id: inc.clone(),
                        epoch: 1,
                        machine_id: None,
                        lane: None,
                        endpoint: TransportEndpoint::UdsSession {
                            conn_id: 1,
                            client: "cli".into(),
                        },
                        liveness: LivenessMode::SocketLifetime,
                        bound_at: UnixMs(1_500),
                        expires_at: None,
                        canary_token: None,
                        status: BindingStatus::Live,
                    },
                    superseded: Vec::new(),
                }),
            }),
            &herald_client,
        )
        .await
        .expect("bind");

    herald
        .react(
            Frame::Committed(Envelope {
                stream: StreamId::Presence,
                seq: Seq(2),
                at: UnixMs(2_000),
                by: alice_p.clone(),
                event: Event::Presence(PresenceEvent::Unbound {
                    seat: alice_seat.clone(),
                    incarnation_id: inc,
                    status: BindingStatus::Unbound,
                    reason: Some("eof".into()),
                }),
            }),
            &herald_client,
        )
        .await
        .expect("unbind at t=2000");

    let sha = sha_hex(42);
    let rel = PathBuf::from("to-alice/ping.md");
    herald
        .react(
            Frame::Committed(Envelope {
                stream: StreamId::Mail,
                seq: Seq(1),
                at: UnixMs(5_000),
                by: owner_p,
                event: Event::Mail(MailEvent::Banked {
                    from_room: general,
                    range: (Seq(1), Seq(1)),
                    letter: LetterRef {
                        path: rel.clone(),
                        sha256: sha.clone(),
                        wake_id: WakeId("000000000000002a".into()),
                        item: MailboxItem {
                            schema: MailSchemaV1,
                            id: "letter:42".into(),
                            from: "owner".into(),
                            to: "alice".into(),
                            kind: "letter".into(),
                            state: "open".into(),
                            ts: "2026-11-24T00:00:00Z".into(),
                            ts_basis: "letter_id_ms".into(),
                            reference: MailRef {
                                store: "letters".into(),
                                path: rel.clone(),
                                sha256: sha.clone(),
                            },
                            topic: None,
                            round: None,
                            label: Some("Ping".into()),
                        },
                    },
                }),
            }),
            &herald_client,
        )
        .await
        .expect("bank at t=5000");

    // Within 15s grace window (dropped at 2_000 -> deadline 17_000): suppressed!
    assert_eq!(
        herald.next_grace_deadline(UnixMs(10_000)),
        Some(UnixMs(17_000))
    );
    herald
        .flush_due_at(UnixMs(10_000), &herald_client)
        .await
        .expect("flush at 10s");
    let within = owner
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .expect("list within");
    assert!(
        within.items.is_empty(),
        "MailUnrung must stay suppressed inside 15s reconnect grace window"
    );

    // At deadline t=17_000: raised!
    herald
        .flush_due_at(UnixMs(17_000), &herald_client)
        .await
        .expect("flush at 17s");
    let raised = owner
        .call(InboxList {
            include_done: false,
            limit: None,
        })
        .await
        .expect("list raised");
    assert_eq!(raised.unread, 1);
    assert!(matches!(
        raised.items[0].draft.kind,
        NotifyKind::MailUnrung {
            unread_count: 1,
            ..
        }
    ));

    // Acking the pending letter auto-resolves the MailUnrung card to Done.
    herald
        .react(
            Frame::Committed(Envelope {
                stream: StreamId::Mail,
                seq: Seq(2),
                at: UnixMs(18_000),
                by: alice_p,
                event: Event::Mail(MailEvent::Acked {
                    path: rel,
                    sha256: sha,
                    seat: None,
                    note: None,
                }),
            }),
            &herald_client,
        )
        .await
        .expect("ack");

    let resolved = owner
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .expect("list resolved");
    assert_eq!(resolved.unread, 0);
    assert_eq!(resolved.items[0].state, NotifyState::Done);
}

#[tokio::test]
async fn persisted_mail_unrung_card_resolves_on_cold_replay_when_ack_committed_before_restart() {
    let router = build_inbox_router();
    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").expect("owner"),
    };
    let alice_seat = SeatName::parse("alice").expect("alice");
    let alice_p = Principal::Seat {
        seat: alice_seat.clone(),
    };
    let owner = LocalClient::new(router.clone(), owner_p.clone());
    let herald_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::Herald,
        },
    );

    let general = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("general").expect("chan"),
    };
    let sha = sha_hex(77);
    let rel = PathBuf::from("to-alice/persisted-unrung.md");
    let banked_env = Envelope {
        stream: StreamId::Mail,
        seq: Seq(1),
        at: UnixMs(5_000),
        by: Principal::Operator {
            seat: SeatName::parse("owner").expect("owner"),
        },
        event: Event::Mail(MailEvent::Banked {
            from_room: general,
            range: (Seq(1), Seq(1)),
            letter: LetterRef {
                path: rel.clone(),
                sha256: sha.clone(),
                wake_id: WakeId("000000000000004d".into()),
                item: MailboxItem {
                    schema: MailSchemaV1,
                    id: "letter:77".into(),
                    from: "owner".into(),
                    to: "alice".into(),
                    kind: "letter".into(),
                    state: "open".into(),
                    ts: "2026-11-24T00:00:00Z".into(),
                    ts_basis: "letter_id_ms".into(),
                    reference: MailRef {
                        store: "letters".into(),
                        path: rel.clone(),
                        sha256: sha.clone(),
                    },
                    topic: None,
                    round: None,
                    label: Some("Persisted".into()),
                },
            },
        }),
    };

    // First Herald raises the persisted MailUnrung card in Inbox, then stops before seeing Acked.
    let mut first_herald = Herald::with_grace(Duration::from_millis(15_000));
    first_herald
        .react(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &herald_client,
        )
        .await
        .expect("caught up 1");
    first_herald
        .react(Frame::Committed(banked_env.clone()), &herald_client)
        .await
        .expect("banked 1");
    first_herald
        .flush_due_at(UnixMs(25_000), &herald_client)
        .await
        .expect("flush 1");
    assert_eq!(
        owner
            .call(InboxList {
                include_done: false,
                limit: None,
            })
            .await
            .expect("before restart")
            .unread,
        1
    );

    // Cold-restarted Herald replays Banked + persisted StreamId::Inbox Raised + Acked before CaughtUp!
    let persisted_inbox = herald_client
        .call(rex_wave_api::read::Read::stream(
            StreamId::Inbox { owner: owner_p },
            Seq::ORIGIN,
            Some(100),
        ))
        .await
        .expect("read persisted inbox stream");
    assert_eq!(persisted_inbox.entries.len(), 1);

    let ack_env = Envelope {
        stream: StreamId::Mail,
        seq: Seq(2),
        at: UnixMs(26_000),
        by: alice_p,
        event: Event::Mail(MailEvent::Acked {
            path: rel,
            sha256: sha,
            seat: None,
            note: None,
        }),
    };
    let mut cold_herald = Herald::with_grace(Duration::from_millis(15_000));
    cold_herald
        .react(Frame::Committed(banked_env), &herald_client)
        .await
        .expect("replay banked");
    for inbox_env in persisted_inbox.entries {
        cold_herald
            .react(Frame::Committed(inbox_env), &herald_client)
            .await
            .expect("replay persisted inbox event");
    }
    cold_herald
        .react(Frame::Committed(ack_env), &herald_client)
        .await
        .expect("replay acked");
    cold_herald
        .react(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &herald_client,
        )
        .await
        .expect("cold caught up");

    let after = owner
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .expect("after cold replay");
    assert_eq!(
        after.unread, 0,
        "persisted MailUnrung card must resolve on cold replay"
    );
    assert_eq!(after.items[0].state, NotifyState::Done);

    // Replaying an interaction raise followed by its resolution into an empty Inbox
    // must preserve the Done card under include_done=true.
    let empty_router = build_inbox_router();
    let empty_herald_client = LocalClient::new(
        empty_router.clone(),
        Principal::System {
            part: SystemPart::Herald,
        },
    );
    let wave = WaveSlug::parse("w-replay-done").expect("wave");
    let role = RoleName::parse("auditor").expect("role");
    let th = BbThreadId::parse("th_replay_done").expect("th");
    let int_id = BbInteractionId::parse("ix_replay_1").expect("ix");
    let owner2 = Principal::Operator {
        seat: SeatName::parse("owner").expect("owner"),
    };
    let mut empty_cold_herald = Herald::with_grace(Duration::from_millis(15_000));
    for env in [
        Envelope {
            stream: StreamId::Wave { wave: wave.clone() },
            seq: Seq(1),
            at: UnixMs(1_000),
            by: owner2.clone(),
            event: Event::Wave(WaveEvent::WorkerObserved {
                role: role.clone(),
                observation: Observation::activity_with_bounds(
                    Activity::Blocked {
                        interaction: PendingInteraction {
                            id: int_id.clone(),
                            kind: InteractionKind::Permission,
                            summary: "Approve check".into(),
                        },
                    },
                    th,
                    None,
                    Vec::new(),
                ),
            }),
        },
        Envelope {
            stream: StreamId::Wave { wave: wave.clone() },
            seq: Seq(2),
            at: UnixMs(2_000),
            by: owner2.clone(),
            event: Event::Wave(WaveEvent::InteractionResolved {
                role,
                interaction: int_id,
                resolution: InteractionResolution::AllowOnce {
                    granted_permissions: None,
                },
            }),
        },
    ] {
        empty_cold_herald
            .react(Frame::Committed(env), &empty_herald_client)
            .await
            .expect("replay interaction env");
    }
    empty_cold_herald
        .react(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &empty_herald_client,
        )
        .await
        .expect("caught up empty");
    let rebuilt_done = LocalClient::new(empty_router, owner2)
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .expect("list rebuilt done");
    assert_eq!(rebuilt_done.unread, 0);
    assert_eq!(rebuilt_done.items.len(), 1);
    assert_eq!(rebuilt_done.items[0].state, NotifyState::Done);

    // Persisted Dismissed status card must remain Dismissed (not reopened into Done)
    // when TransportHealth(down) + persisted Inbox(Raised -> Done -> Dismissed) +
    // TransportHealth(healthy) are cold-replayed.
    let dis_router = build_inbox_router();
    let dis_owner_p = Principal::Operator {
        seat: SeatName::parse("owner").expect("owner"),
    };
    let dis_owner = LocalClient::new(dis_router.clone(), dis_owner_p.clone());
    let dis_herald_client = LocalClient::new(
        dis_router.clone(),
        Principal::System {
            part: SystemPart::Herald,
        },
    );
    let down_env = Envelope {
        stream: StreamId::Presence,
        seq: Seq(1),
        at: UnixMs(5_000),
        by: dis_owner_p.clone(),
        event: Event::Presence(PresenceEvent::TransportHealthChanged {
            transport: TransportKind::BbThread,
            down: true,
            detail: Some("connection reset".into()),
        }),
    };
    let up_env = Envelope {
        stream: StreamId::Presence,
        seq: Seq(2),
        at: UnixMs(6_000),
        by: dis_owner_p.clone(),
        event: Event::Presence(PresenceEvent::TransportHealthChanged {
            transport: TransportKind::BbThread,
            down: false,
            detail: None,
        }),
    };
    let mut live_h = Herald::with_grace(Duration::from_millis(15_000));
    live_h
        .react(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &dis_herald_client,
        )
        .await
        .expect("caught up");
    live_h
        .react(Frame::Committed(down_env.clone()), &dis_herald_client)
        .await
        .expect("down");
    live_h
        .react(Frame::Committed(up_env.clone()), &dis_herald_client)
        .await
        .expect("up");
    let done_list = dis_owner
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .expect("list");
    assert_eq!(done_list.items.len(), 1);
    dis_owner
        .call(InboxAck {
            ids: vec![done_list.items[0].id],
            state: NotifyState::Dismissed,
        })
        .await
        .expect("dismiss status card");

    let dis_inbox_stream = dis_herald_client
        .call(rex_wave_api::read::Read::stream(
            StreamId::Inbox {
                owner: dis_owner_p.clone(),
            },
            Seq::ORIGIN,
            Some(100),
        ))
        .await
        .expect("read inbox stream");
    let mut cold_dis_h = Herald::with_grace(Duration::from_millis(15_000));
    cold_dis_h
        .react(Frame::Committed(down_env), &dis_herald_client)
        .await
        .expect("replay down");
    for env in dis_inbox_stream.entries {
        cold_dis_h
            .react(Frame::Committed(env), &dis_herald_client)
            .await
            .expect("replay inbox env");
    }
    cold_dis_h
        .react(Frame::Committed(up_env), &dis_herald_client)
        .await
        .expect("replay up");
    cold_dis_h
        .react(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &dis_herald_client,
        )
        .await
        .expect("cold caught up");
    let after_dis = dis_owner
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .expect("after dis");
    assert_eq!(after_dis.unread, 0);
    assert_eq!(
        after_dis.items[0].state,
        NotifyState::Dismissed,
        "persisted Dismissed status card must stay Dismissed on cold replay"
    );
}
