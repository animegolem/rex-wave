use futures_util::StreamExt;
use rex_wave_api::read::{AgentEvent, AgentEventKind, Read, ThreadPageView, Wait};
use rex_wave_api::room::*;
use rex_wave_api::stage::StageTarget;
use rex_wave_api::wave::Activity;
use rex_wave_api::*;
use rex_wave_runtime::*;
use rex_wave_store::Journal;
use rex_wave_transport::{
    Authenticator, Credential, Endpoint, RemoteClient, TableAuthenticator, bind_unix,
    serve_listener,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct ToyRooms;

#[derive(Default)]
struct ToyState {
    head: Seq,
}

impl Decider for ToyRooms {
    type State = ToyState;
    type Cmd = RoomCommand;
    type Event = RoomEvent;

    fn address(cmd: &RoomCommand, ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        let r = match cmd {
            RoomCommand::RoomEnsure(c) => &c.room,
            RoomCommand::RoomPost(c) => &c.room,
            RoomCommand::RoomJoin(c) => &c.room,
            RoomCommand::RoomLeave(c) => &c.room,
        };
        Ok(StreamId::Room {
            room: r.resolve(&ctx.by)?,
        })
    }

    fn idem(cmd: &RoomCommand) -> Option<IdemKey> {
        match cmd {
            RoomCommand::RoomPost(c) => c.idem.clone(),
            _ => None,
        }
    }

    fn decide(_: &ToyState, cmd: &RoomCommand, _: &DecideCtx) -> Result<Vec<RoomEvent>, ApiError> {
        match cmd {
            RoomCommand::RoomPost(c) => Ok(vec![RoomEvent::Posted {
                body: c.body.clone(),
                mentions: Mentions::default(),
                reply_to: c.reply_to,
                idem: c.idem.clone(),
            }]),
            _ => Ok(vec![]),
        }
    }

    fn evolve(state: &mut ToyState, _: &RoomEvent, meta: &EntryMeta) {
        state.head = meta.seq;
    }

    fn reply(_: &ToyState, cmd: RoomCommand, committed: &[Envelope]) -> WireReply {
        match cmd {
            RoomCommand::RoomPost(_) => {
                let last = committed.last().unwrap();
                let StreamId::Room { room } = &last.stream else {
                    unreachable!()
                };
                WireReply::RoomPost(Posted {
                    room: room.clone(),
                    seq: last.seq,
                    mentions: Mentions::default(),
                })
            }
            _ => WireReply::RoomJoin(Ack),
        }
    }

    fn wrap(event: RoomEvent) -> Event {
        Event::Room(event)
    }

    fn unwrap(event: &Event) -> Option<&RoomEvent> {
        match event {
            Event::Room(e) => Some(e),
            _ => None,
        }
    }
}

struct LeaseFeed(Arc<AtomicUsize>);

struct DropLease(Arc<AtomicUsize>);
impl Drop for DropLease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl AgentFeed for LeaseFeed {
    fn page(
        &self,
        _: &WaveSlug,
        _: &RoleName,
        _: Option<BbSeq>,
        _: u32,
    ) -> BoxFut<'_, Result<ThreadPageView, ApiError>> {
        Box::pin(async {
            Ok(ThreadPageView {
                thread: BbThreadId::parse("th_1").unwrap(),
                activity: Activity::Working,
                events: vec![],
                usage: None,
                pending: None,
                next: None,
            })
        })
    }

    fn live(&self, wave: &WaveSlug, role: &RoleName) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let guard = DropLease(self.0.clone());
        let (wave, role) = (wave.clone(), role.clone());
        Box::pin(async move {
            let stream = futures_util::stream::unfold(
                (guard, false, wave, role),
                |(guard, sent, wave, role)| async move {
                    if !sent {
                        let ev = AgentEvent {
                            seq: BbSeq(1),
                            at: UnixMs(1),
                            kind: AgentEventKind::TurnStarted,
                        };
                        Some((
                            Ok(Frame::Agent {
                                wave: wave.clone(),
                                role: role.clone(),
                                event: ev,
                            }),
                            (guard, true, wave, role),
                        ))
                    } else {
                        std::future::pending::<()>().await;
                        unreachable!()
                    }
                },
            );
            Ok(Box::pin(stream) as FrameStream)
        })
    }
}

#[tokio::test]
async fn uds_hello_multiplexed_calls_watches_and_cancellations_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("daemon.sock");
    let (listener, daemon_uid) = bind_unix(&sock).unwrap();

    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let leases = Arc::new(AtomicUsize::new(0));
    let router = Arc::new(Router {
        room: Registry::<ToyRooms>::new(journal.clone(), hub.clone()),
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Arc::new(Unavailable(Family::Inbox)),
        mail: Arc::new(Unavailable(Family::Mail)),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(LeaseFeed(leases.clone())),
        }),
    });
    let auth: Arc<dyn Authenticator> = Arc::new(TableAuthenticator {
        operator: SeatName::parse("owner").unwrap(),
        daemon_uid: Some(daemon_uid),
        tokens: BTreeMap::from([("tok-sam".into(), SeatName::parse("sam").unwrap())]),
    });

    let server = tokio::spawn(serve_listener(listener, router, auth));
    let endpoint = Endpoint::Unix(sock);

    let bad = RemoteClient::connect(
        endpoint.clone(),
        Credential::Token {
            token: "wrong".into(),
        },
        "test",
    )
    .await;
    assert!(
        matches!(bad, Err(ApiError::Denied { .. })),
        "bad token fails with Denied on Hello"
    );

    let op = RemoteClient::connect(endpoint.clone(), Credential::PeerUid, "tui")
        .await
        .unwrap();
    assert_eq!(
        op.me(),
        &Principal::Operator {
            seat: SeatName::parse("owner").unwrap()
        }
    );

    let sam = RemoteClient::connect(
        endpoint,
        Credential::Token {
            token: "tok-sam".into(),
        },
        "mcp",
    )
    .await
    .unwrap();
    assert_eq!(
        sam.me(),
        &Principal::Seat {
            seat: SeatName::parse("sam").unwrap()
        }
    );
    assert!(matches!(
        sam.watch(Interest::Everything, Cursor::default()).await,
        Err(ApiError::Denied { .. })
    ));

    let general = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("general").unwrap(),
    };
    let mut room_watch = op
        .watch(
            Interest::Room {
                room: general.clone(),
            },
            Cursor::default(),
        )
        .await
        .unwrap();
    assert!(matches!(
        room_watch.next().await,
        Some(Ok(Frame::CaughtUp { .. }))
    ));

    let mut joins = Vec::new();
    for i in 1..=10 {
        let c = sam.clone();
        let g = general.clone();
        joins.push(tokio::spawn(async move {
            c.call(RoomPost {
                room: g,
                body: Body::parse(format!("m-{i}")).unwrap(),
                reply_to: None,
                idem: None,
            })
            .await
            .unwrap()
            .seq
            .0
        }));
    }
    let mut returned: Vec<u64> = Vec::new();
    for j in joins {
        returned.push(j.await.unwrap());
    }
    returned.sort();
    assert_eq!(
        returned,
        (1..=10).collect::<Vec<_>>(),
        "multiplexed calls over one socket all succeeded"
    );

    let mut streamed = Vec::new();
    for _ in 0..10 {
        if let Some(Ok(Frame::Committed(env))) = room_watch.next().await {
            streamed.push(env.seq.0);
        }
    }
    assert_eq!(streamed, (1..=10).collect::<Vec<_>>());

    let general_id = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("general").unwrap(),
    };
    let page = sam
        .call(Read {
            stream: StreamId::Room {
                room: general_id.clone(),
            },
            after: Seq(8),
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 2);

    let waiter = tokio::spawn({
        let sam = sam.clone();
        async move {
            sam.call(Wait::room(general_id, Seq(10), false))
                .await
                .unwrap()
        }
    });
    tokio::time::sleep(Duration::from_millis(15)).await;
    op.call(RoomPost {
        room: general.clone(),
        body: Body::parse("wake!").unwrap(),
        reply_to: None,
        idem: None,
    })
    .await
    .unwrap();
    assert_eq!(waiter.await.unwrap().entries[0].seq, Seq(11));

    let mut agent_watch = op
        .watch(
            Interest::Agent {
                wave: WaveSlug::parse("auth").unwrap(),
                role: RoleName::parse("verifier").unwrap(),
            },
            Cursor::live(),
        )
        .await
        .unwrap();
    assert!(matches!(
        agent_watch.next().await,
        Some(Ok(Frame::Agent { .. }))
    ));
    assert_eq!(leases.load(Ordering::SeqCst), 1);
    drop(agent_watch);
    tokio::time::timeout(Duration::from_secs(2), async {
        while leases.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("dropping RemoteClient watch sent Unwatch and released server lease");

    let mut stage1 = op
        .watch(
            Interest::Stage {
                target: StageTarget::Inbox,
            },
            Cursor::live(),
        )
        .await
        .unwrap();
    assert!(matches!(
        stage1.next().await,
        Some(Ok(Frame::StageFocused {
            generation: FocusGeneration(1),
            ..
        }))
    ));
    let mut stage2 = op
        .watch(
            Interest::Stage {
                target: StageTarget::Room { room: general },
            },
            Cursor::live(),
        )
        .await
        .unwrap();
    assert!(matches!(
        stage2.next().await,
        Some(Ok(Frame::StageFocused {
            generation: FocusGeneration(2),
            ..
        }))
    ));
    let ended = tokio::time::timeout(Duration::from_secs(2), async {
        while stage1.next().await.is_some() {}
    })
    .await;
    assert!(
        ended.is_ok(),
        "opening stage2 over UDS closed stage1 cleanly"
    );

    server.abort();
}

#[tokio::test]
async fn abrupt_socket_hangup_wakes_pending_call_with_transport_error() {
    struct HangingService;
    impl Service<RoomCommand> for HangingService {
        fn dispatch(
            &self,
            _: RoomCommand,
            _: DecideCtx,
        ) -> BoxFut<'_, Result<WireReply, ApiError>> {
            Box::pin(async {
                std::future::pending::<()>().await;
                unreachable!()
            })
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("daemon.sock");
    let (listener, daemon_uid) = bind_unix(&sock).unwrap();

    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(64);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Arc::new(HangingService),
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Arc::new(Unavailable(Family::Inbox)),
        mail: Arc::new(Unavailable(Family::Mail)),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });
    let auth: Arc<dyn Authenticator> = Arc::new(TableAuthenticator {
        operator: SeatName::parse("owner").unwrap(),
        daemon_uid: Some(daemon_uid),
        tokens: BTreeMap::new(),
    });

    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let peer = stream.peer_cred().ok().map(|c| c.uid());
        let conn = tokio::spawn(async move {
            let (l, _) = bind_unix(&sock.with_file_name("inner.sock")).unwrap();
            drop((l, peer));
        });
        drop((conn, router, auth, stream));
        let _ = accepted_tx.send(());
    });
    let res = RemoteClient::connect(
        Endpoint::Unix(dir.path().join("daemon.sock")),
        Credential::PeerUid,
        "tui",
    )
    .await;
    accepted_rx.await.unwrap();
    assert!(
        matches!(res, Err(ApiError::Transport(_))),
        "closed socket during handshake returns Transport error: {res:?}"
    );
}
