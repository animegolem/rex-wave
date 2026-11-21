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
            RoomCommand::RoomLabel(c) => &c.room,
            RoomCommand::TaskClaim(c) => &c.room,
            RoomCommand::TaskHeartbeat(c) => &c.room,
            RoomCommand::TaskTransition(c) => &c.room,
            RoomCommand::TaskUpdate(c) => &c.room,
            RoomCommand::RoomTasks(c) => &c.room,
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
                re: c.re,
                after: c.after,
                labels: c.labels.clone(),
                task: c.task.clone(),
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
                    re: None,
                    org_id: None,
                    delivery_notice: None,
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
        presence: Arc::new(Unavailable(Family::Presence)),
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
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
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
        re: None,
        after: None,
        labels: Vec::new(),
        task: None,
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
        presence: Arc::new(Unavailable(Family::Presence)),
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

/// A reply that arrives in two pieces must survive the client starting another
/// call between them. The client once read frames inside a `select!`, so a new
/// request dropped the half-read frame and every later frame was read from the
/// wrong offset.
#[tokio::test]
async fn reply_split_across_writes_survives_a_call_issued_mid_frame() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn read_json(stream: &mut tokio::net::UnixStream) -> serde_json::Value {
        let len = stream.read_u32().await.unwrap() as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.unwrap();
        serde_json::from_slice(&buf).unwrap()
    }
    fn frame(value: &serde_json::Value) -> Vec<u8> {
        let body = serde_json::to_vec(value).unwrap();
        let mut out = (body.len() as u32).to_be_bytes().to_vec();
        out.extend(body);
        out
    }
    fn reply(id: u64, error: ApiError) -> Vec<u8> {
        frame(&serde_json::json!({
            "t": "reply",
            "id": id,
            "result": Err::<WireReply, ApiError>(error),
        }))
    }

    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("stub.sock");
    let listener = tokio::net::UnixListener::bind(&sock).unwrap();

    let (half_sent_tx, half_sent_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(read_json(&mut stream).await["t"], "hello");
        let me = Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        };
        stream
            .write_all(&frame(&serde_json::json!({
                "t": "welcome",
                "result": Ok::<Principal, ApiError>(me),
                "daemon": "stub",
            })))
            .await
            .unwrap();

        // Answer the first call with only the first half of its reply.
        let first = read_json(&mut stream).await;
        assert_eq!(first["t"], "call");
        let first_reply = reply(first["id"].as_u64().unwrap(), ApiError::Timeout);
        let (head, tail) = first_reply.split_at(first_reply.len() / 2);
        stream.write_all(head).await.unwrap();
        stream.flush().await.unwrap();
        half_sent_tx.send(()).unwrap();

        // The second call arrives while the client is part-way through that reply.
        let second = read_json(&mut stream).await;
        assert_eq!(second["t"], "call");
        stream.write_all(tail).await.unwrap();
        let second_reply = reply(
            second["id"].as_u64().unwrap(),
            ApiError::NotFound {
                what: "second".into(),
            },
        );
        stream.write_all(&second_reply).await.unwrap();
        stream.flush().await.unwrap();
        // Hold the socket open until the client has read both replies.
        let _ = stream.read_u8().await;
    });

    let client = Arc::new(
        RemoteClient::connect(Endpoint::Unix(sock), Credential::PeerUid, "split-test")
            .await
            .unwrap(),
    );
    let first_call = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .call_wire(WireCommand::Sidebar(Default::default()))
                .await
        }
    });
    half_sent_rx.await.unwrap();
    // Let the client take the half frame off the socket before the next call.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let second_result = tokio::time::timeout(
        Duration::from_secs(5),
        client.call_wire(WireCommand::Sidebar(Default::default())),
    )
    .await
    .expect("second call answered");
    let first_result = tokio::time::timeout(Duration::from_secs(5), first_call)
        .await
        .expect("first call answered")
        .unwrap();

    assert_eq!(first_result.unwrap_err(), ApiError::Timeout);
    assert_eq!(
        second_result.unwrap_err(),
        ApiError::NotFound {
            what: "second".into()
        }
    );
    drop(client);
    server.abort();
}

fn general() -> RoomRef {
    RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("general").unwrap(),
    }
}

fn owner() -> Principal {
    Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    }
}

fn router_with(journal: &Journal, room: Arc<dyn Service<RoomCommand>>) -> Arc<Router> {
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    Arc::new(Router {
        room,
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Arc::new(Unavailable(Family::Inbox)),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Arc::new(Unavailable(Family::Presence)),
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    })
}

/// A fresh daemon (router, hub, registry) over `journal`, as after a restart.
fn toy_router(journal: &Journal) -> Arc<Router> {
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    Arc::new(Router {
        room: Registry::<ToyRooms>::new(journal.clone(), hub.clone()),
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Arc::new(Unavailable(Family::Inbox)),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Arc::new(Unavailable(Family::Presence)),
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    })
}

fn serve_on(sock: &std::path::Path, router: Arc<Router>) -> tokio::task::JoinHandle<()> {
    let (listener, daemon_uid) = bind_unix(sock).unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(TableAuthenticator {
        operator: SeatName::parse("owner").unwrap(),
        daemon_uid: Some(daemon_uid),
        tokens: BTreeMap::new(),
    });
    tokio::spawn(async move {
        let _ = serve_listener(listener, router, auth).await;
    })
}

fn post(body: &str) -> RoomPost {
    RoomPost {
        room: general(),
        body: Body::parse(body).unwrap(),
        re: None,
        after: None,
        labels: Vec::new(),
        task: None,
        idem: None,
    }
}

async fn next_frame(stream: &mut FrameStream) -> Frame {
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("frame within 5s")
        .expect("stream still open")
        .expect("frame, not error")
}

fn committed_seq(frame: &Frame) -> u64 {
    match frame {
        Frame::Committed(env) => env.seq.0,
        other => panic!("expected a committed entry, got {other:?}"),
    }
}

/// `(generation, seq)` of a stage frame wrapping a committed entry.
fn stage_committed(frame: &Frame) -> (u64, u64) {
    match frame {
        Frame::Stage { generation, inner } => (generation.0, committed_seq(inner)),
        other => panic!("expected a stage frame, got {other:?}"),
    }
}

/// `ApiError` once failed to serialize for `Transport` and `Internal`, and the
/// daemon's writer closed the socket instead of answering. Every variant must
/// now arrive as itself, and the connection must stay up throughout.
#[tokio::test]
async fn every_api_error_crosses_the_socket_without_dropping_it() {
    let every = vec![
        ApiError::Invalid(InvalidId {
            what: "seat".into(),
            reason: "length out of range".into(),
        }),
        ApiError::Denied {
            principal: "seat:sam".into(),
            action: "watch everything".into(),
        },
        ApiError::NotFound {
            what: "room #nope".into(),
        },
        ApiError::Conflict {
            detail: "worker never spawned".into(),
            data: None,
        },
        ApiError::Unavailable {
            family: Family::Wave,
        },
        ApiError::Timeout,
        ApiError::Transport("bb events page: connection refused".into()),
        ApiError::ReplyMismatch {
            expected: "room_post".into(),
            got: "ack".into(),
        },
        ApiError::Internal("journal locked".into()),
        // Too big for one frame: the daemon must answer with a small error.
        ApiError::Internal("x".repeat(17 * 1024 * 1024)),
    ];

    struct Fails(Vec<ApiError>);
    impl Service<RoomCommand> for Fails {
        fn dispatch(
            &self,
            cmd: RoomCommand,
            _: DecideCtx,
        ) -> BoxFut<'_, Result<WireReply, ApiError>> {
            let RoomCommand::RoomPost(c) = cmd else {
                unreachable!()
            };
            let i: usize = String::from(c.body).parse().unwrap();
            let err = self.0[i].clone();
            Box::pin(async move { Err(err) })
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("daemon.sock");
    let journal = Journal::in_memory().unwrap();
    let server = serve_on(&sock, router_with(&journal, Arc::new(Fails(every.clone()))));
    let client = RemoteClient::connect(Endpoint::Unix(sock), Credential::PeerUid, "tui")
        .await
        .unwrap();
    let mut connection = client.watch_connection();
    connection.borrow_and_update();

    let (oversized, fits) = every.split_last().unwrap();
    for (i, expected) in fits.iter().enumerate() {
        let got = client.call(post(&i.to_string())).await;
        assert_eq!(got.unwrap_err(), *expected, "variant {i} arrives intact");
    }
    let got = client.call(post(&fits.len().to_string())).await;
    match got.unwrap_err() {
        ApiError::Internal(detail) => assert!(
            detail.contains("exceeds 16 MiB") && detail.len() < 1024,
            "oversized reply becomes a short internal error, got {} bytes",
            detail.len()
        ),
        other => panic!("oversized reply should fail as internal, got {other:?} for {oversized:?}"),
    }

    assert!(client.connected());
    assert!(
        !connection.has_changed().unwrap(),
        "the socket never dropped, so the client never reconnected"
    );
    drop(client);
    server.abort();
}

/// A daemon restart unlinks and rebinds the socket. The client must redial,
/// re-open its watches from the last frame it delivered, and serve calls again,
/// so a TUI never freezes on a dead connection.
#[tokio::test]
async fn daemon_restart_reconnects_and_resumes_watches_from_their_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("daemon.sock");
    let journal = Journal::in_memory().unwrap();
    let first = serve_on(&sock, toy_router(&journal));

    let client = RemoteClient::connect(Endpoint::Unix(sock.clone()), Credential::PeerUid, "tui")
        .await
        .unwrap();
    let mut connection = client.watch_connection();
    assert!(*connection.borrow_and_update());

    let mut room = client
        .watch(Interest::Room { room: general() }, Cursor::default())
        .await
        .unwrap();
    assert!(matches!(
        next_frame(&mut room).await,
        Frame::CaughtUp { .. }
    ));
    let mut everything = client
        .watch(Interest::Everything, Cursor::live())
        .await
        .unwrap();
    assert!(matches!(
        next_frame(&mut everything).await,
        Frame::CaughtUp { .. }
    ));
    // Two earlier retargets, so the live stage is generation 3 and a restarted
    // daemon's fresh numbering (1, 2, ...) would read as stale to the TUI.
    for generation in 1..=2 {
        let mut earlier = client
            .watch(
                Interest::Stage {
                    target: StageTarget::Inbox,
                },
                Cursor::live(),
            )
            .await
            .unwrap();
        assert!(matches!(
            next_frame(&mut earlier).await,
            Frame::StageFocused { generation: g, .. } if g == FocusGeneration(generation)
        ));
    }
    let mut stage = client
        .watch(
            Interest::Stage {
                target: StageTarget::Room { room: general() },
            },
            Cursor::live(),
        )
        .await
        .unwrap();
    assert!(matches!(
        next_frame(&mut stage).await,
        Frame::StageFocused {
            generation: FocusGeneration(3),
            ..
        }
    ));
    assert!(matches!(
        next_frame(&mut stage).await,
        Frame::Stage { inner, .. } if matches!(*inner, Frame::CaughtUp { .. })
    ));

    for i in 1..=3 {
        client.call(post(&format!("before-{i}"))).await.unwrap();
    }
    for seq in 1..=3 {
        assert_eq!(committed_seq(&next_frame(&mut room).await), seq);
        assert_eq!(committed_seq(&next_frame(&mut everything).await), seq);
        assert_eq!(stage_committed(&next_frame(&mut stage).await), (3, seq));
    }

    first.abort();
    let _ = first.await;
    tokio::time::timeout(Duration::from_secs(5), connection.wait_for(|up| !*up))
        .await
        .expect("client notices the daemon is gone")
        .unwrap();
    match client.call(post("while-down")).await.unwrap_err() {
        ApiError::Transport(detail) => assert!(
            detail.contains("daemon closed the connection"),
            "a refused call names why the socket dropped: {detail}"
        ),
        other => panic!("expected a transport error while down, got {other:?}"),
    }

    // The restarted daemon commits entries (including a room that did not
    // exist before the crash) before it listens, so the client can only see
    // them by replaying from its cursor.
    let second_router = toy_router(&journal);
    let local = LocalClient::new(second_router.clone(), owner());
    for i in 4..=5 {
        local.call(post(&format!("during-{i}"))).await.unwrap();
    }
    let alerts = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("alerts").unwrap(),
    };
    local
        .call(RoomPost {
            room: alerts,
            body: Body::parse("new-room-while-down").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();
    let second = serve_on(&sock, second_router);
    tokio::time::timeout(Duration::from_secs(5), connection.wait_for(|up| *up))
        .await
        .expect("client redials the rebound socket")
        .unwrap();

    let posted = client.call(post("after-6")).await.unwrap();
    assert_eq!(posted.seq, Seq(6), "calls work again after the redial");
    for seq in 4..=6 {
        assert_eq!(
            committed_seq(&next_frame(&mut room).await),
            seq,
            "room watch resumes after seq 3 with no gap and no repeat"
        );
        assert_eq!(
            stage_committed(&next_frame(&mut stage).await),
            (3, seq),
            "stage watch keeps the generation its caller already saw"
        );
    }
    let mut wild_seqs = Vec::new();
    for _ in 0..4 {
        wild_seqs.push(committed_seq(&next_frame(&mut everything).await));
    }
    wild_seqs.sort_unstable();
    assert_eq!(
        wild_seqs,
        vec![1, 4, 5, 6],
        "live wildcard watch catches a brand-new stream created while down"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), room.next())
            .await
            .is_err(),
        "no duplicate frames or second CaughtUp after the resume"
    );

    let mut retargeted = client
        .watch(
            Interest::Stage {
                target: StageTarget::Inbox,
            },
            Cursor::live(),
        )
        .await
        .unwrap();
    match next_frame(&mut retargeted).await {
        Frame::StageFocused { generation, .. } => assert!(
            generation > FocusGeneration(3),
            "a stage opened after the restart outranks the old one, got {generation:?}"
        ),
        other => panic!("expected StageFocused, got {other:?}"),
    }

    drop((room, everything, stage, retargeted, client));
    second.abort();
}
