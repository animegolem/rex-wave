use futures_util::StreamExt;
use rex_wave_api::read::{AgentEvent, AgentEventKind, Read, ThreadPageView, Wait};
use rex_wave_api::room::*;
use rex_wave_api::stage::StageTarget;
use rex_wave_api::wave::Activity;
use rex_wave_api::*;
use rex_wave_runtime::*;
use rex_wave_store::Journal;
use rex_wave_transport::{
    Authenticator, ConnectionHooks, Credential, Endpoint, FnConnectionHooks,
    PrincipalAuthenticator, PrincipalTokenTable, RemoteClient, StateDirAuthenticator,
    TableAuthenticator, bind_unix, serve_listener, serve_listener_with_hooks,
    write_worker_token_file,
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

#[tokio::test]
async fn uds_stamps_server_conn_id_onto_uds_session_presence_bind_and_saves_principal_tokens() {
    use rex_wave_api::presence::*;
    use std::sync::Mutex;

    struct CapturePresence(Arc<Mutex<Vec<u64>>>);
    impl Service<PresenceCommand> for CapturePresence {
        fn dispatch(
            &self,
            cmd: PresenceCommand,
            ctx: DecideCtx,
        ) -> BoxFut<'_, Result<WireReply, ApiError>> {
            let seen = self.0.clone();
            Box::pin(async move {
                let PresenceCommand::PresenceBind(bind) = cmd else {
                    return Err(ApiError::Unavailable {
                        family: Family::Presence,
                    });
                };
                if let TransportEndpoint::UdsSession { conn_id, .. } = &bind.endpoint {
                    seen.lock().unwrap().push(*conn_id);
                }
                Ok(WireReply::PresenceBind(SeatBinding {
                    seat: bind.seat,
                    incarnation_id: IncarnationId::parse("inc-1").unwrap(),
                    epoch: 1,
                    lane: bind.lane,
                    endpoint: bind.endpoint,
                    liveness: bind.liveness,
                    bound_at: ctx.at,
                    expires_at: None,
                    status: BindingStatus::Live,
                }))
            })
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let tokens_path = tmp.path().join("tokens.json");
    let mut table = PrincipalTokenTable::new();
    table
        .insert_seat("tok_sam", SeatName::parse("sam").unwrap())
        .unwrap();
    table
        .insert_worker(
            "tok_worker",
            WaveSlug::parse("w1").unwrap(),
            RoleName::parse("author").unwrap(),
        )
        .unwrap();
    table.save_tokens_file(&tokens_path).unwrap();
    let reloaded = PrincipalTokenTable::load_tokens_file(&tokens_path, None).unwrap();
    assert_eq!(reloaded, table);

    let sock = tmp.path().join("daemon.sock");
    let (listener, uid) = bind_unix(&sock).unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(64);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Registry::<ToyRooms>::new(journal.clone(), hub.clone()),
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Arc::new(Unavailable(Family::Inbox)),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Arc::new(CapturePresence(seen.clone())),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });
    let auth: Arc<dyn Authenticator> = Arc::new(StateDirAuthenticator::from_state_dir(
        SeatName::parse("owner").unwrap(),
        Some(uid),
        tmp.path(),
    ));
    let server = tokio::spawn(serve_listener(listener, router, auth));

    let sam = RemoteClient::connect(
        Endpoint::Unix(sock),
        Credential::Token {
            token: "tok_sam".into(),
        },
        "hold",
    )
    .await
    .unwrap();
    let bound = sam
        .call(PresenceBind {
            seat: SeatName::parse("sam").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: TransportEndpoint::UdsSession {
                conn_id: 0,
                client: "hold".into(),
            },
            liveness: LivenessMode::SocketLifetime,
            ttl_ms: None,
            takeover: false,
            allow_fork: false,
            mail_horizon_at: None,
            idem: None,
        })
        .await
        .unwrap();
    let stamped = match bound.endpoint {
        TransportEndpoint::UdsSession { conn_id, .. } => conn_id,
        other => panic!("expected UdsSession, got {other:?}"),
    };
    assert_ne!(stamped, 0, "transport must stamp non-zero server conn_id");
    assert_eq!(*seen.lock().unwrap(), vec![stamped]);

    drop(sam);
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

#[tokio::test]
async fn principal_tokens_and_worker_token_files_authenticate_seat_worker_operator() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path();
    let sock = state_dir.join("daemon.sock");
    let (listener, uid) = bind_unix(&sock).unwrap();

    let tokens_path = state_dir.join("tokens.json");
    let tokens_json = serde_json::json!({
        "tok_legacy_sam": "sam",
        "tok_op_golem": { "kind": "operator", "seat": "golem" },
        "tok_worker_inline": { "kind": "worker", "wave": "w1", "role": "verifier" }
    });
    std::fs::write(&tokens_path, serde_json::to_vec(&tokens_json).unwrap()).unwrap();
    std::fs::set_permissions(&tokens_path, std::fs::Permissions::from_mode(0o600)).unwrap();

    let worker_dir = state_dir.join("worker-tokens");
    let t3_worker = Principal::Worker {
        wave: WaveSlug::parse("auth-v2").unwrap(),
        role: RoleName::parse("author").unwrap(),
    };
    write_worker_token_file(&worker_dir, "t3_thread_99", "tok_t3_author", &t3_worker).unwrap();

    // Malformed structured file with seat-shaped values must NEVER turn JSON field
    // names ("token", "principal") into bearer tokens, and must not break valid tokens.
    let malformed_path = worker_dir.join("t3_malformed");
    std::fs::write(&malformed_path, r#"{"token":"alice","principal":"bob"}"#).unwrap();
    std::fs::set_permissions(&malformed_path, std::fs::Permissions::from_mode(0o600)).unwrap();

    let auth: Arc<dyn Authenticator> = Arc::new(StateDirAuthenticator::from_state_dir(
        SeatName::parse("owner").unwrap(),
        Some(uid),
        state_dir,
    ));

    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(64);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
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
            agents: Arc::new(NoAgentFeed),
        }),
    });
    let server = tokio::spawn(serve_listener(listener, router, auth));

    let sam = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "tok_legacy_sam".into(),
        },
        "cli-sam",
    )
    .await
    .unwrap();
    assert_eq!(
        sam.me(),
        &Principal::Seat {
            seat: SeatName::parse("sam").unwrap()
        }
    );

    let op = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "tok_op_golem".into(),
        },
        "cli-op",
    )
    .await
    .unwrap();
    assert_eq!(
        op.me(),
        &Principal::Operator {
            seat: SeatName::parse("golem").unwrap()
        }
    );

    let w_inline = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "tok_worker_inline".into(),
        },
        "cli-w1",
    )
    .await
    .unwrap();
    assert_eq!(
        w_inline.me(),
        &Principal::Worker {
            wave: WaveSlug::parse("w1").unwrap(),
            role: RoleName::parse("verifier").unwrap(),
        }
    );

    let w_file = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "tok_t3_author".into(),
        },
        "t3-author",
    )
    .await
    .unwrap();
    assert_eq!(w_file.me(), &t3_worker);

    // Malformed worker file field names are never accepted as bearer tokens.
    let malformed_err = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "token".into(),
        },
        "t3-malformed",
    )
    .await
    .unwrap_err();
    assert!(matches!(malformed_err, ApiError::Denied { .. }));

    // Hot-reload: a newly written worker token works without restarting the server.
    let w_hot_principal = Principal::Worker {
        wave: WaveSlug::parse("auth-v2").unwrap(),
        role: RoleName::parse("hot-added").unwrap(),
    };
    let hot_path =
        write_worker_token_file(&worker_dir, "t3_thread_hot", "tok_t3_hot", &w_hot_principal)
            .unwrap();
    let w_hot = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "tok_t3_hot".into(),
        },
        "t3-hot",
    )
    .await
    .unwrap();
    assert_eq!(w_hot.me(), &w_hot_principal);

    // A world-readable worker token file is skipped (its token denied) without
    // locking out healthy tokens in `tokens.json` or other `0600` worker files.
    std::fs::set_permissions(&hot_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let bad_file_err = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "tok_t3_hot".into(),
        },
        "t3-bad-mode",
    )
    .await
    .unwrap_err();
    assert!(matches!(bad_file_err, ApiError::Denied { .. }));
    let still_ok = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "tok_legacy_sam".into(),
        },
        "cli-sam-still-ok",
    )
    .await
    .unwrap();
    assert_eq!(still_ok.me(), sam.me());
    drop(still_ok);

    // A world-readable `worker-tokens/` directory is skipped (denying all worker
    // tokens inside it) without locking out healthy `tokens.json` seat tokens.
    std::fs::set_permissions(&worker_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    let bad_dir_err = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "tok_t3_author".into(),
        },
        "t3-bad-dir",
    )
    .await
    .unwrap_err();
    assert!(
        matches!(bad_dir_err, ApiError::Denied { .. }),
        "got {bad_dir_err:?}"
    );
    let sam_survives_bad_worker_dir = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "tok_legacy_sam".into(),
        },
        "cli-sam-bad-worker-dir",
    )
    .await
    .unwrap();
    assert_eq!(sam_survives_bad_worker_dir.me(), sam.me());
    drop(sam_survives_bad_worker_dir);
    std::fs::set_permissions(&worker_dir, std::fs::Permissions::from_mode(0o700)).unwrap();

    // System principals cannot be inserted into a token table or written to disk.
    let mut table = PrincipalTokenTable::new();
    let sys_err = table
        .insert(
            "tok_sys",
            Principal::System {
                part: SystemPart::PresenceMonitor,
            },
        )
        .unwrap_err();
    assert!(matches!(sys_err, ApiError::Denied { .. }));
    assert!(
        write_worker_token_file(
            &worker_dir,
            "bad_sys",
            "tok_sys",
            &Principal::System {
                part: SystemPart::Herald,
            },
        )
        .is_err()
    );

    // PrincipalAuthenticator works in-memory too.
    table
        .insert_worker(
            "tok_mem_worker",
            WaveSlug::parse("w2").unwrap(),
            RoleName::parse("coder").unwrap(),
        )
        .unwrap();
    let mem_auth = PrincipalAuthenticator {
        operator: SeatName::parse("owner").unwrap(),
        daemon_uid: Some(uid),
        tokens: table,
    };
    assert_eq!(
        mem_auth
            .authenticate(
                Some(uid),
                &Credential::Token {
                    token: "tok_mem_worker".into()
                }
            )
            .unwrap(),
        Principal::Worker {
            wave: WaveSlug::parse("w2").unwrap(),
            role: RoleName::parse("coder").unwrap(),
        }
    );

    drop((sam, op, w_inline, w_file, w_hot));
    server.abort();
}

#[tokio::test]
async fn serve_listener_with_hooks_fires_raii_disconnect_on_eof_and_abort() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("daemon.sock");
    let (listener, uid) = bind_unix(&sock).unwrap();

    let mut table = PrincipalTokenTable::new();
    table
        .insert_seat("tok_crest", SeatName::parse("crest").unwrap())
        .unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(PrincipalAuthenticator {
        operator: SeatName::parse("owner").unwrap(),
        daemon_uid: Some(uid),
        tokens: table,
    });

    let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<(String, u64, Principal)>();
    let tx_connect = ev_tx.clone();
    let tx_disconnect = ev_tx.clone();
    let hooks: Arc<dyn ConnectionHooks> = Arc::new(FnConnectionHooks::new(
        move |conn_id, principal, client| {
            let _ = tx_connect.send((format!("connect:{client}"), conn_id, principal.clone()));
        },
        move |conn_id, principal| {
            let _ = tx_disconnect.send(("disconnect".into(), conn_id, principal.clone()));
        },
    ));

    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(64);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
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
            agents: Arc::new(NoAgentFeed),
        }),
    });

    let server = tokio::spawn(serve_listener_with_hooks(listener, router, auth, hooks));

    // Refused handshake fires neither connect nor disconnect.
    let bad = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "wrong".into(),
        },
        "intruder",
    )
    .await
    .unwrap_err();
    assert!(matches!(bad, ApiError::Denied { .. }));
    assert!(
        tokio::time::timeout(Duration::from_millis(60), ev_rx.recv())
            .await
            .is_err(),
        "failed auth must not trigger hooks"
    );

    let crest_principal = Principal::Seat {
        seat: SeatName::parse("crest").unwrap(),
    };

    // Normal connect -> drop fires connect then RAII disconnect immediately.
    let c1 = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "tok_crest".into(),
        },
        "presence-hold",
    )
    .await
    .unwrap();
    let (ev, id1, p1) = tokio::time::timeout(Duration::from_secs(2), ev_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ev, "connect:presence-hold");
    assert_eq!(p1, crest_principal);

    drop(c1);
    let (ev, d_id1, d_p1) = tokio::time::timeout(Duration::from_secs(2), ev_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ev, "disconnect");
    assert_eq!(d_id1, id1);
    assert_eq!(d_p1, crest_principal);

    // Server cancellation/abort also fires RAII disconnect on active connections.
    let _c2 = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "tok_crest".into(),
        },
        "hold-until-shutdown",
    )
    .await
    .unwrap();
    let (ev, id2, p2) = tokio::time::timeout(Duration::from_secs(2), ev_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ev, "connect:hold-until-shutdown");
    assert_ne!(id2, id1);
    assert_eq!(p2, crest_principal);

    server.abort();
    let (ev, d_id2, d_p2) = tokio::time::timeout(Duration::from_secs(2), ev_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ev, "disconnect");
    assert_eq!(d_id2, id2);
    assert_eq!(d_p2, crest_principal);
}

#[tokio::test]
async fn disconnect_hook_waits_for_and_aborts_in_flight_calls_before_firing() {
    struct SlowRecordingService {
        started: tokio::sync::Notify,
        events: tokio::sync::mpsc::UnboundedSender<&'static str>,
    }

    impl Service<RoomCommand> for SlowRecordingService {
        fn dispatch(
            &self,
            cmd: RoomCommand,
            _: DecideCtx,
        ) -> BoxFut<'_, Result<WireReply, ApiError>> {
            let body = match cmd {
                RoomCommand::RoomPost(p) => p.body.as_str().to_owned(),
                _ => String::new(),
            };
            let events = self.events.clone();
            self.started.notify_one();
            Box::pin(async move {
                if body == "quick-promote" {
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    let _ = events.send("quick_finished");
                    return Ok(WireReply::RoomJoin(Ack));
                }
                struct DropMark(tokio::sync::mpsc::UnboundedSender<&'static str>);
                impl Drop for DropMark {
                    fn drop(&mut self) {
                        let _ = self.0.send("call_aborted");
                    }
                }
                let _mark = DropMark(events.clone());
                tokio::time::sleep(Duration::from_secs(10)).await;
                let _ = events.send("call_finished_late");
                Ok(WireReply::RoomJoin(Ack))
            })
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("daemon.sock");
    let (listener, uid) = bind_unix(&sock).unwrap();

    let mut table = PrincipalTokenTable::new();
    table
        .insert_seat("tok_crest", SeatName::parse("crest").unwrap())
        .unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(PrincipalAuthenticator {
        operator: SeatName::parse("owner").unwrap(),
        daemon_uid: Some(uid),
        tokens: table,
    });

    let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<&'static str>();
    let svc = Arc::new(SlowRecordingService {
        started: tokio::sync::Notify::new(),
        events: ev_tx.clone(),
    });
    let svc_wait = svc.clone();

    let hooks: Arc<dyn ConnectionHooks> = Arc::new(FnConnectionHooks::new(
        |_, _, _| {},
        move |_, _| {
            let _ = ev_tx.send("on_disconnect");
        },
    ));

    let journal = Journal::in_memory().unwrap();
    let router = router_with(&journal, svc);
    let server = tokio::spawn(serve_listener_with_hooks(listener, router, auth, hooks));

    // 1. Short in-flight call finishes cleanly within drain grace before on_disconnect runs.
    let client_quick = RemoteClient::connect(
        Endpoint::Unix(sock.clone()),
        Credential::Token {
            token: "tok_crest".into(),
        },
        "quick-in-flight",
    )
    .await
    .unwrap();
    let quick_task = tokio::spawn({
        let c = client_quick.clone();
        async move { c.call(post("quick-promote")).await }
    });
    tokio::time::timeout(Duration::from_secs(2), svc_wait.started.notified())
        .await
        .expect("server started quick call");
    quick_task.abort();
    let _ = quick_task.await;
    drop(client_quick);

    let q1 = tokio::time::timeout(Duration::from_secs(2), ev_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let q2 = tokio::time::timeout(Duration::from_secs(2), ev_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (q1, q2),
        ("quick_finished", "on_disconnect"),
        "short in-flight call must finish before on_disconnect instead of splitting a write"
    );

    // 2. Hung in-flight call is aborted after drain timeout, still strictly before on_disconnect.
    let client = RemoteClient::connect(
        Endpoint::Unix(sock),
        Credential::Token {
            token: "tok_crest".into(),
        },
        "in-flight-test",
    )
    .await
    .unwrap();

    let call_task = tokio::spawn({
        let c = client.clone();
        async move { c.call(post("slow-bind")).await }
    });
    tokio::time::timeout(Duration::from_secs(2), svc_wait.started.notified())
        .await
        .expect("server started slow call");

    call_task.abort();
    let _ = call_task.await;
    drop(client);

    let first = tokio::time::timeout(Duration::from_secs(2), ev_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), ev_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (first, second),
        ("call_aborted", "on_disconnect"),
        "in-flight call must abort and drain before on_disconnect runs"
    );

    server.abort();
}

#[test]
fn legacy_tokens_json_loads_and_worker_tokens_dir_rejects_non_worker_principals() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::tempdir().unwrap();
    let tokens_path = tmp.path().join("tokens.json");
    let legacy_json = serde_json::json!({
        "tok_sam": "sam",
        "tok_legacy_worker": {
            "wave": "auth-v2",
            "role": "author",
            "thread_id": "t3:auth-v2:author:a1"
        },
        "tok_legacy_worker_no_tid": {
            "wave": "auth-v2",
            "role": "reviewer"
        }
    });
    std::fs::write(&tokens_path, serde_json::to_vec(&legacy_json).unwrap()).unwrap();
    std::fs::set_permissions(&tokens_path, std::fs::Permissions::from_mode(0o600)).unwrap();

    let loaded_revoked = PrincipalTokenTable::load_tokens_file(&tokens_path, None).unwrap();
    assert_eq!(
        loaded_revoked.get("tok_sam"),
        Some(&Principal::Seat {
            seat: SeatName::parse("sam").unwrap()
        })
    );
    assert!(
        loaded_revoked.get("tok_legacy_worker").is_none(),
        "thread-scoped legacy worker token must be refused when worker-tokens/<tid> is absent"
    );
    assert_eq!(
        loaded_revoked.get("tok_legacy_worker_no_tid"),
        Some(&Principal::Worker {
            wave: WaveSlug::parse("auth-v2").unwrap(),
            role: RoleName::parse("reviewer").unwrap(),
        })
    );

    let worker_dir = tmp.path().join("worker-tokens");
    std::fs::create_dir_all(&worker_dir).unwrap();
    std::fs::set_permissions(&worker_dir, std::fs::Permissions::from_mode(0o700)).unwrap();

    let legacy_tid_file = worker_dir.join("t3:auth-v2:author:a1");
    std::fs::write(&legacy_tid_file, "tok_legacy_worker\n").unwrap();
    std::fs::set_permissions(&legacy_tid_file, std::fs::Permissions::from_mode(0o600)).unwrap();

    let mut loaded_active = PrincipalTokenTable::load_tokens_file(&tokens_path, None).unwrap();
    assert_eq!(
        loaded_active.get("tok_legacy_worker"),
        Some(&Principal::Worker {
            wave: WaveSlug::parse("auth-v2").unwrap(),
            role: RoleName::parse("author").unwrap(),
        }),
        "thread-scoped legacy worker token authenticates while worker-tokens/<tid> exists and matches"
    );

    loaded_active
        .insert_seat("tok_alice", SeatName::parse("alice").unwrap())
        .unwrap();
    loaded_active.save_tokens_file(&tokens_path).unwrap();
    let saved_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&tokens_path).unwrap()).unwrap();
    assert_eq!(
        saved_json["tok_legacy_worker"]["thread_id"], "t3:auth-v2:author:a1",
        "save_tokens_file must preserve legacy thread_id instead of promoting to permanent worker"
    );

    std::fs::remove_file(&legacy_tid_file).unwrap();
    let after_unlink = PrincipalTokenTable::load_tokens_file(&tokens_path, None).unwrap();
    assert!(
        after_unlink.get("tok_legacy_worker").is_none(),
        "unlinking worker-tokens/<tid> must immediately revoke legacy thread-scoped token"
    );

    assert!(!rex_wave_transport::is_safe_worker_token_id("a..b"));
    assert!(rex_wave_transport::is_safe_worker_token_id(
        "t3:auth-v2:author:a1"
    ));

    let op_principal = Principal::Operator {
        seat: SeatName::parse("golem").unwrap(),
    };
    let seat_principal = Principal::Seat {
        seat: SeatName::parse("sam").unwrap(),
    };
    assert!(write_worker_token_file(&worker_dir, "op_bad", "tok_op", &op_principal).is_err());
    assert!(write_worker_token_file(&worker_dir, "seat_bad", "tok_seat", &seat_principal).is_err());

    let forged_op = worker_dir.join("forged_op");
    std::fs::write(
        &forged_op,
        serde_json::to_vec(&serde_json::json!({
            "token": "tok_forged_op",
            "principal": { "kind": "operator", "seat": "golem" }
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::set_permissions(&forged_op, std::fs::Permissions::from_mode(0o600)).unwrap();

    let forged_seat = worker_dir.join("forged_seat");
    std::fs::write(
        &forged_seat,
        serde_json::to_vec(&serde_json::json!({
            "token": "tok_forged_seat",
            "principal": { "kind": "seat", "seat": "sam" }
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::set_permissions(&forged_seat, std::fs::Permissions::from_mode(0o600)).unwrap();

    let workers_loaded = PrincipalTokenTable::load_worker_tokens_dir(&worker_dir, None).unwrap();
    assert!(workers_loaded.get("tok_forged_op").is_none());
    assert!(workers_loaded.get("tok_forged_seat").is_none());
}
