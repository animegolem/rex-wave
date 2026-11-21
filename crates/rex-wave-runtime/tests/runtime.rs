use futures_util::StreamExt;
use rex_wave_api::notify::*;
use rex_wave_api::read::*;
use rex_wave_api::room::*;
use rex_wave_api::stage::StageTarget;
use rex_wave_api::wave::Activity;
use rex_wave_api::*;
use rex_wave_runtime::*;
use rex_wave_store::Journal;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct ToyRooms;

#[derive(Default)]
struct ToyRoomState {
    members: BTreeMap<Handle, Principal>,
    posts: u64,
    head: Seq,
}

impl Decider for ToyRooms {
    type State = ToyRoomState;
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

    fn decide(
        state: &ToyRoomState,
        cmd: &RoomCommand,
        ctx: &DecideCtx,
    ) -> Result<Vec<RoomEvent>, ApiError> {
        match cmd {
            RoomCommand::RoomEnsure(c) => {
                let id = c.room.resolve(&ctx.by)?;
                if state.head == Seq::ORIGIN {
                    Ok(vec![RoomEvent::Opened {
                        id,
                        origin: c.origin.clone().unwrap_or(RoomOrigin::Manual),
                        topic: c.topic.clone(),
                    }])
                } else {
                    Ok(vec![])
                }
            }
            RoomCommand::RoomJoin(c) => {
                let who = c.member.clone().unwrap_or_else(|| ctx.by.clone());
                let handle = match &c.handle {
                    Some(h) => h.clone(),
                    None => Handle::parse(who.to_string().trim_start_matches('@'))?,
                };
                Ok(vec![RoomEvent::MemberJoined {
                    member: who,
                    handle,
                }])
            }
            RoomCommand::RoomLeave(_) => Ok(vec![RoomEvent::MemberLeft {
                member: ctx.by.clone(),
            }]),
            RoomCommand::RoomPost(c) => {
                let mut principals = Vec::new();
                let mut all = false;
                for token in c.body.as_str().split_whitespace() {
                    if let Some(raw) = token.strip_prefix('@') {
                        if raw == "all" {
                            all = true;
                        } else if let Ok(h) = Handle::parse(raw)
                            && let Some(p) = state.members.get(&h)
                        {
                            principals.push(p.clone());
                        }
                    }
                }
                Ok(vec![RoomEvent::Posted {
                    body: c.body.clone(),
                    mentions: Mentions { principals, all },
                    reply_to: c.reply_to,
                    idem: c.idem.clone(),
                }])
            }
        }
    }

    fn evolve(state: &mut ToyRoomState, event: &RoomEvent, meta: &EntryMeta) {
        state.head = meta.seq;
        match event {
            RoomEvent::MemberJoined { member, handle } => {
                state.members.insert(handle.clone(), member.clone());
            }
            RoomEvent::MemberLeft { member } => state.members.retain(|_, p| p != member),
            RoomEvent::Posted { .. } => state.posts += 1,
            RoomEvent::Opened { .. } | RoomEvent::TopicSet { .. } => {}
        }
    }

    fn reply(state: &ToyRoomState, cmd: RoomCommand, committed: &[Envelope]) -> WireReply {
        match cmd {
            RoomCommand::RoomEnsure(c) => WireReply::RoomEnsure(RoomInfo {
                id: match &committed.first().map(|e| &e.stream) {
                    Some(StreamId::Room { room }) => room.clone(),
                    _ => c
                        .room
                        .resolve(&Principal::System {
                            part: SystemPart::Herald,
                        })
                        .unwrap_or_else(|_| RoomId::Channel {
                            scope: RoomScope::Global,
                            name: ChannelName::parse("fallback").unwrap(),
                        }),
                },
                origin: RoomOrigin::Manual,
                topic: None,
                head: state.head,
                members: state
                    .members
                    .iter()
                    .map(|(h, p)| (h.clone(), p.clone()))
                    .collect(),
            }),
            RoomCommand::RoomPost(_) => {
                let last = committed
                    .last()
                    .expect("post commits or replays one envelope");
                let StreamId::Room { room } = &last.stream else {
                    unreachable!()
                };
                let Event::Room(RoomEvent::Posted { mentions, .. }) = &last.event else {
                    unreachable!()
                };
                WireReply::RoomPost(Posted {
                    room: room.clone(),
                    seq: last.seq,
                    mentions: mentions.clone(),
                })
            }
            RoomCommand::RoomJoin(_) => WireReply::RoomJoin(Ack),
            RoomCommand::RoomLeave(_) => WireReply::RoomLeave(Ack),
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

struct ToyInbox;

#[derive(Default)]
struct ToyInboxState {
    next_id: u64,
    head: Seq,
}

impl Decider for ToyInbox {
    type State = ToyInboxState;
    type Cmd = InboxCommand;
    type Event = InboxEvent;

    fn address(cmd: &InboxCommand, ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        let owner = match cmd {
            InboxCommand::InboxRaise(c) => c.to.clone(),
            _ => ctx.by.clone(),
        };
        Ok(StreamId::Inbox { owner })
    }

    fn idem(_: &InboxCommand) -> Option<IdemKey> {
        None
    }

    fn decide(
        state: &ToyInboxState,
        cmd: &InboxCommand,
        _ctx: &DecideCtx,
    ) -> Result<Vec<InboxEvent>, ApiError> {
        match cmd {
            InboxCommand::InboxRaise(c) => Ok(vec![InboxEvent::Raised {
                id: NotificationId(state.next_id + 1),
                rank: Rank(10),
                draft: c.draft.clone(),
            }]),
            InboxCommand::InboxAck(c) => Ok(vec![InboxEvent::StateSet {
                ids: c.ids.clone(),
                state: c.state,
            }]),
            InboxCommand::InboxMarkRead(c) => Ok(vec![InboxEvent::CursorAdvanced {
                room: c.room.clone(),
                upto: c.upto,
            }]),
            InboxCommand::SetArchived(c) => Ok(vec![InboxEvent::ArchivedSet {
                target: c.target.clone(),
                archived: c.archived,
            }]),
            InboxCommand::InboxList(_) => Ok(vec![]),
        }
    }

    fn evolve(state: &mut ToyInboxState, event: &InboxEvent, meta: &EntryMeta) {
        state.head = meta.seq;
        if let InboxEvent::Raised { id, .. } = event {
            state.next_id = state.next_id.max(id.0);
        }
    }

    fn reply(state: &ToyInboxState, cmd: InboxCommand, _: &[Envelope]) -> WireReply {
        match cmd {
            InboxCommand::InboxList(_) => WireReply::InboxList(InboxPage {
                head: state.head,
                items: vec![],
                unread: 0,
                cursors: vec![],
            }),
            InboxCommand::InboxAck(_) => WireReply::InboxAck(Ack),
            InboxCommand::InboxMarkRead(_) => WireReply::InboxMarkRead(Ack),
            InboxCommand::SetArchived(_) => WireReply::SetArchived(Ack),
            InboxCommand::InboxRaise(_) => WireReply::InboxRaise(Ack),
        }
    }

    fn wrap(event: InboxEvent) -> Event {
        Event::Inbox(event)
    }

    fn unwrap(event: &Event) -> Option<&InboxEvent> {
        match event {
            Event::Inbox(e) => Some(e),
            _ => None,
        }
    }
}

struct LeaseCountingFeed {
    active: Arc<AtomicUsize>,
}

struct LeaseDrop(Arc<AtomicUsize>);
impl Drop for LeaseDrop {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl AgentFeed for LeaseCountingFeed {
    fn page(
        &self,
        _wave: &WaveSlug,
        _role: &RoleName,
        _after: Option<BbSeq>,
        _limit: u32,
    ) -> BoxFut<'_, Result<ThreadPageView, ApiError>> {
        Box::pin(async {
            Ok(ThreadPageView {
                thread: BbThreadId::parse("th_live").unwrap(),
                activity: Activity::Working,
                events: vec![],
                usage: None,
                pending: None,
                next: None,
            })
        })
    }

    fn live(&self, wave: &WaveSlug, role: &RoleName) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let guard = LeaseDrop(self.active.clone());
        let wave = wave.clone();
        let role = role.clone();
        Box::pin(async move {
            let stream = futures_util::stream::unfold(
                (guard, 0u64, wave, role),
                |(guard, n, wave, role)| async move {
                    if n == 0 {
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
                            (guard, 1, wave, role),
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

fn harness(idle: Duration) -> (Arc<Router>, Journal, Arc<AtomicUsize>) {
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let leases = Arc::new(AtomicUsize::new(0));
    let router = Arc::new(Router {
        room: Registry::<ToyRooms>::with_idle(journal.clone(), hub.clone(), idle),
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Registry::<ToyInbox>::with_idle(journal.clone(), hub.clone(), idle),
        mail: Arc::new(Unavailable(Family::Mail)),
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub,
            sidebar,
            agents: Arc::new(LeaseCountingFeed {
                active: leases.clone(),
            }),
        }),
    });
    (router, journal, leases)
}

fn chan(name: &str) -> RoomRef {
    RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse(name).unwrap(),
    }
}

fn post(room: RoomRef, text: &str, idem: Option<&str>) -> RoomPost {
    RoomPost {
        room,
        body: Body::parse(text).unwrap(),
        reply_to: None,
        idem: idem.map(|k| IdemKey::parse(k).unwrap()),
    }
}

#[tokio::test]
async fn decider_rehydrates_after_passivation_and_dedupes_idem() {
    let (router, _journal, _) = harness(Duration::from_millis(20));
    let sam = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("sam").unwrap(),
        },
    );

    let p1 = sam
        .call(post(chan("general"), "first", None))
        .await
        .unwrap();
    let p2 = sam
        .call(post(chan("general"), "second", Some("idem-2")))
        .await
        .unwrap();
    let p2_retry = sam
        .call(post(chan("general"), "mutated-retry", Some("idem-2")))
        .await
        .unwrap();
    assert_eq!(p1.seq, Seq(1));
    assert_eq!(p2.seq, Seq(2));
    assert_eq!(
        p2_retry.seq,
        Seq(2),
        "idempotent retry returns original seq"
    );

    tokio::time::sleep(Duration::from_millis(45)).await;

    let p3 = sam
        .call(post(chan("general"), "after-passivation", None))
        .await
        .unwrap();
    assert_eq!(
        p3.seq,
        Seq(3),
        "rehydrated actor continues from journal head"
    );

    let page = sam
        .call(Read {
            stream: StreamId::Room {
                room: RoomId::Channel {
                    scope: RoomScope::Global,
                    name: ChannelName::parse("general").unwrap(),
                },
            },
            after: Seq::ORIGIN,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(
        page.entries.iter().map(|e| e.seq.0).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn audience_and_privacy_boundaries_hold() {
    let (router, _, _) = harness(Duration::from_secs(60));
    let sam = Principal::Seat {
        seat: SeatName::parse("sam").unwrap(),
    };
    let alice = Principal::Seat {
        seat: SeatName::parse("alice").unwrap(),
    };
    let nodal = Principal::Seat {
        seat: SeatName::parse("nodal").unwrap(),
    };
    let sam_c = LocalClient::new(router.clone(), sam.clone());
    let nodal_c = LocalClient::new(router.clone(), nodal);

    let err = sam_c
        .call(InboxRaise {
            to: alice.clone(),
            draft: NotificationDraft {
                kind: NotifyKind::Reminder {
                    bell_id: "b1".into(),
                },
                title: "t".into(),
                preview: "p".into(),
                actions: vec![],
                dedupe: "d".into(),
            },
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, ApiError::Denied { .. }),
        "seat cannot call System commands"
    );

    assert!(matches!(
        sam_c.watch(Interest::Everything, Cursor::default()).await,
        Err(ApiError::Denied { .. })
    ));

    sam_c
        .call(post(
            RoomRef::Dm {
                with: alice.clone(),
            },
            "secret",
            None,
        ))
        .await
        .unwrap();
    let dm = RoomRef::Dm { with: alice }.resolve(&sam).unwrap();
    let denied = nodal_c
        .call(Read {
            stream: StreamId::Room { room: dm },
            after: Seq::ORIGIN,
            limit: None,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(denied, ApiError::Denied { .. }),
        "outsider cannot read a DM"
    );
}

#[tokio::test]
async fn watch_and_wait_never_sleep_through_concurrent_commits() {
    let (router, _, _) = harness(Duration::from_secs(60));
    let sam = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("sam").unwrap(),
        },
    );
    let alice_p = Principal::Seat {
        seat: SeatName::parse("alice").unwrap(),
    };
    let alice = LocalClient::new(router.clone(), alice_p.clone());

    sam.call(RoomJoin {
        room: chan("general"),
        member: Some(alice_p),
        handle: None,
    })
    .await
    .unwrap();
    for i in 1..=5 {
        sam.call(post(chan("general"), &format!("pre-{i}"), None))
            .await
            .unwrap();
    }

    let writer_client = sam.clone();
    let writer = tokio::spawn(async move {
        for i in 6..=30 {
            writer_client
                .call(post(chan("general"), &format!("msg-{i}"), None))
                .await
                .unwrap();
            if i % 5 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    let mut watch = sam
        .watch(
            Interest::Room {
                room: chan("general"),
            },
            Cursor::default(),
        )
        .await
        .unwrap();
    let mut seqs = Vec::new();
    while seqs.len() < 31 {
        if let Some(Ok(Frame::Committed(env))) = watch.next().await {
            seqs.push(env.seq.0);
        }
    }
    writer.await.unwrap();
    let expected: Vec<u64> = (1..=31).collect();
    assert_eq!(
        seqs, expected,
        "tap-before-replay-and-dedupe delivered 1..=31 with no gap or duplicate"
    );

    let general_id = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("general").unwrap(),
    };
    let catchup = sam
        .call(Wait::room(general_id.clone(), Seq(28), false))
        .await
        .unwrap();
    assert!(!catchup.timed_out);
    assert_eq!(
        catchup.entries.iter().map(|e| e.seq.0).collect::<Vec<_>>(),
        vec![29, 30, 31],
        "Wait returns the full catch-up slice"
    );

    let quiet = alice
        .call(Wait {
            interest: Interest::Mentions,
            after: Cursor::live(),
            timeout_ms: Some(15),
        })
        .await
        .unwrap();
    assert!(quiet.timed_out && quiet.entries.is_empty());

    let wake_cursor = quiet.cursor.clone();
    let waiter = tokio::spawn({
        let alice = alice.clone();
        async move {
            alice
                .call(Wait {
                    interest: Interest::Mentions,
                    after: wake_cursor,
                    timeout_ms: Some(2000),
                })
                .await
                .unwrap()
        }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    sam.call(post(chan("general"), "unrelated noise", None))
        .await
        .unwrap();
    sam.call(post(chan("general"), "hey @alice check this", None))
        .await
        .unwrap();
    let woken = waiter.await.unwrap();
    assert!(!woken.timed_out);
    assert_eq!(
        woken.entries.len(),
        1,
        "only the @alice mention woke the waiter"
    );
}

#[tokio::test]
async fn stage_focus_generation_cancels_prior_watch_and_releases_agent_lease() {
    let (router, _, leases) = harness(Duration::from_secs(60));
    let sam = LocalClient::new(
        router,
        Principal::Operator {
            seat: SeatName::parse("sam").unwrap(),
        },
    );

    let mut s1 = sam
        .watch(
            Interest::Stage {
                target: StageTarget::Agent {
                    wave: WaveSlug::parse("auth").unwrap(),
                    role: RoleName::parse("verifier").unwrap(),
                },
            },
            Cursor::live(),
        )
        .await
        .unwrap();
    assert!(matches!(
        s1.next().await,
        Some(Ok(Frame::StageFocused {
            generation: FocusGeneration(1),
            ..
        }))
    ));
    assert!(matches!(
        s1.next().await,
        Some(Ok(Frame::Stage {
            generation: FocusGeneration(1),
            ..
        }))
    ));
    assert_eq!(leases.load(Ordering::SeqCst), 1);

    let mut s2 = sam
        .watch(
            Interest::Stage {
                target: StageTarget::Room {
                    room: chan("general"),
                },
            },
            Cursor::live(),
        )
        .await
        .unwrap();
    assert!(matches!(
        s2.next().await,
        Some(Ok(Frame::StageFocused {
            generation: FocusGeneration(2),
            ..
        }))
    ));
    assert!(
        s1.next().await.is_none(),
        "opening a newer stage focus ends the older stage stream"
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        leases.load(Ordering::SeqCst),
        0,
        "older agent lease was dropped automatically"
    );
}

struct MentionReactor;

impl Reactor for MentionReactor {
    const NAME: &'static str = "test-herald";
    fn interest(&self) -> Interest {
        Interest::Everything
    }
    async fn react(&mut self, frame: Frame, client: &LocalClient) -> Result<(), ApiError> {
        if let Frame::Committed(Envelope {
            stream: StreamId::Room { room },
            seq,
            event: Event::Room(RoomEvent::Posted { mentions, body, .. }),
            ..
        }) = frame
        {
            for who in mentions.principals {
                client
                    .call(InboxRaise {
                        to: who,
                        draft: NotificationDraft {
                            kind: NotifyKind::Mention {
                                room: room.clone(),
                                seq,
                            },
                            title: "Mention".into(),
                            preview: body.as_str().to_owned(),
                            actions: vec![],
                            dedupe: format!("{seq:?}"),
                        },
                    })
                    .await?;
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn sidebar_projection_and_reactor_update_live() {
    let (router, _, _) = harness(Duration::from_secs(60));
    let sam = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("sam").unwrap(),
        },
    );
    let alice_p = Principal::Seat {
        seat: SeatName::parse("alice").unwrap(),
    };
    let alice = LocalClient::new(router.clone(), alice_p.clone());

    let reactor_handle = spawn_reactor(
        MentionReactor,
        LocalClient::new(
            router.clone(),
            Principal::System {
                part: SystemPart::Herald,
            },
        ),
    );

    let mut sidebar = alice
        .watch(Interest::Sidebar, Cursor::live())
        .await
        .unwrap();
    let Some(Ok(Frame::Sidebar(initial))) = sidebar.next().await else {
        panic!("expected initial sidebar")
    };
    assert_eq!(initial.inbox.unread, 0);

    sam.call(RoomJoin {
        room: chan("general"),
        member: Some(alice_p),
        handle: None,
    })
    .await
    .unwrap();
    let posted = sam
        .call(post(chan("general"), "hello @alice", None))
        .await
        .unwrap();

    let updated = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(Ok(Frame::Sidebar(snap))) = sidebar.next().await
                && snap.inbox.unread == 1
                && snap
                    .channels
                    .first()
                    .is_some_and(|c| c.unread == 1 && c.mentions == 1)
            {
                break *snap;
            }
        }
    })
    .await
    .expect("sidebar converged with unread mention + inbox item");
    assert_eq!(updated.inbox.unread, 1);

    alice
        .call(InboxMarkRead {
            room: posted.room,
            upto: posted.seq,
        })
        .await
        .unwrap();
    alice
        .call(InboxAck {
            ids: vec![NotificationId(1)],
            state: NotifyState::Done,
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(Ok(Frame::Sidebar(snap))) = sidebar.next().await
                && snap.inbox.unread == 0
                && snap.channels.first().is_some_and(|c| c.unread == 0)
            {
                break;
            }
        }
    })
    .await
    .expect("marking read and acking cleared badges");

    reactor_handle.abort();
}

struct DynamicNamesFeed {
    names: tokio::sync::watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>,
}

impl AgentFeed for DynamicNamesFeed {
    fn page(
        &self,
        _wave: &WaveSlug,
        _role: &RoleName,
        _after: Option<BbSeq>,
        _limit: u32,
    ) -> BoxFut<'_, Result<rex_wave_api::read::ThreadPageView, ApiError>> {
        Box::pin(async {
            Err(ApiError::Unavailable {
                family: Family::Wave,
            })
        })
    }

    fn live(
        &self,
        _wave: &WaveSlug,
        _role: &RoleName,
    ) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        Box::pin(async {
            Err(ApiError::Unavailable {
                family: Family::Wave,
            })
        })
    }

    fn project_names(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>> {
        Some(self.names.clone())
    }
}

#[tokio::test]
async fn sidebar_surfaces_human_project_names_and_pushes_updates_when_feed_resolves() {
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(64);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let (names_tx, names_rx) = tokio::sync::watch::channel(Arc::new(BTreeMap::new()));
    let router = Arc::new(Router {
        room: Registry::<ToyRooms>::new(journal.clone(), hub.clone()),
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Registry::<ToyInbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(Unavailable(Family::Mail)),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(DynamicNamesFeed { names: names_rx }),
        }),
    });

    let owner = LocalClient::new(
        router,
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let proj = BbProjectId::parse("proj_edaym59nyn").unwrap();
    owner
        .call(RoomEnsure {
            room: RoomRef::Channel {
                scope: RoomScope::Project {
                    project: proj.clone(),
                },
                name: ChannelName::parse("general").unwrap(),
            },
            topic: None,
            origin: None,
        })
        .await
        .unwrap();

    let mut stream = owner
        .watch(Interest::Sidebar, Cursor::live())
        .await
        .unwrap();
    let initial = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(Ok(Frame::Sidebar(snap))) = stream.next().await
                && !snap.projects.is_empty()
            {
                break *snap;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(initial.projects[0].project, proj);
    assert_eq!(
        initial.projects[0].name, None,
        "graceful None while offline"
    );

    let mut map = BTreeMap::new();
    map.insert(proj.clone(), "pancake-work".to_owned());
    names_tx.send(Arc::new(map)).unwrap();

    let named = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(Ok(Frame::Sidebar(snap))) = stream.next().await
                && snap.projects.first().and_then(|p| p.name.as_deref()) == Some("pancake-work")
            {
                break *snap;
            }
        }
    })
    .await
    .expect("live sidebar watch emitted updated human project name");
    assert_eq!(named.projects[0].name.as_deref(), Some("pancake-work"));
}
