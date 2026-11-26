use futures_util::StreamExt;
use rex_wave_api::notify::*;
use rex_wave_api::presence::*;
use rex_wave_api::read::*;
use rex_wave_api::room::*;
use rex_wave_api::stage::StageTarget;
use rex_wave_api::wave::Activity;
use rex_wave_api::*;
use rex_wave_runtime::*;
use rex_wave_store::Journal;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct ToyRooms;

#[derive(Default)]
struct ToyRoomState {
    members: BTreeMap<Handle, Principal>,
    labels: BTreeMap<Seq, BTreeSet<Label>>,
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
            RoomCommand::RoomLabel(c) => &c.room,
            RoomCommand::TaskClaim(c) => &c.room,
            RoomCommand::TaskHeartbeat(c) => &c.room,
            RoomCommand::TaskTransition(c) => &c.room,
            RoomCommand::TaskUpdate(c) => &c.room,
            RoomCommand::RoomTasks(c) => &c.room,
        };
        Ok(StreamId::Room {
            room: r.resolve(ctx.by())?,
        })
    }

    fn idem(cmd: &RoomCommand) -> Option<IdemKey> {
        match cmd {
            RoomCommand::RoomPost(c) => c.idem.clone(),
            RoomCommand::RoomLabel(c) => c.idem.clone(),
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
                let id = c.room.resolve(ctx.by())?;
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
                let who = c.member.clone().unwrap_or_else(|| ctx.by().clone());
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
                member: ctx.by().clone(),
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
                    re: c.re,
                    after: c.after,
                    labels: c.labels.clone(),
                    task: c.task.clone(),
                    idem: c.idem.clone(),
                    origin: Default::default(),
                }])
            }
            RoomCommand::RoomLabel(c) => {
                let before_set = state.labels.get(&c.seq).cloned().unwrap_or_default();
                let mut after_set = before_set.clone();
                let mut added = Vec::new();
                let mut removed = Vec::new();
                for lbl in &c.remove {
                    if after_set.remove(lbl) {
                        removed.push(lbl.clone());
                    }
                }
                for lbl in &c.add {
                    if after_set.insert(lbl.clone()) {
                        added.push(lbl.clone());
                    }
                }
                Ok(vec![RoomEvent::Labeled {
                    target: c.seq,
                    added,
                    removed,
                    before: before_set.into_iter().collect(),
                    after: after_set.into_iter().collect(),
                }])
            }
            RoomCommand::TaskClaim(_)
            | RoomCommand::TaskHeartbeat(_)
            | RoomCommand::TaskTransition(_)
            | RoomCommand::TaskUpdate(_)
            | RoomCommand::RoomTasks(_) => Err(ApiError::Unavailable {
                family: Family::Room,
            }),
        }
    }

    fn evolve(state: &mut ToyRoomState, event: &RoomEvent, meta: &EntryMeta) {
        state.head = meta.seq;
        match event {
            RoomEvent::MemberJoined { member, handle } => {
                state.members.insert(handle.clone(), member.clone());
            }
            RoomEvent::MemberLeft { member } => state.members.retain(|_, p| p != member),
            RoomEvent::Posted { labels, .. } => {
                state.posts += 1;
                state
                    .labels
                    .insert(meta.seq, labels.iter().cloned().collect());
            }
            RoomEvent::Labeled { target, after, .. } => {
                state
                    .labels
                    .insert(*target, after.iter().cloned().collect());
            }
            RoomEvent::Opened { .. }
            | RoomEvent::TopicSet { .. }
            | RoomEvent::TaskClaimed { .. }
            | RoomEvent::TaskHeartbeat { .. }
            | RoomEvent::TaskTransitioned { .. }
            | RoomEvent::TaskUpdated { .. } => {}
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
                let Event::Room(RoomEvent::Posted { mentions, re, .. }) = &last.event else {
                    unreachable!()
                };
                WireReply::RoomPost(Posted {
                    room: room.clone(),
                    seq: last.seq,
                    mentions: mentions.clone(),
                    re: *re,
                    org_id: None,
                    delivery_notice: None,
                })
            }
            RoomCommand::RoomLabel(c) => {
                let last = committed.last().expect("label commits one envelope");
                let StreamId::Room { room } = &last.stream else {
                    unreachable!()
                };
                let labels = state
                    .labels
                    .get(&c.seq)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                WireReply::RoomLabel(Labeled {
                    room: room.clone(),
                    seq: c.seq,
                    labels,
                })
            }
            RoomCommand::RoomJoin(_) => WireReply::RoomJoin(Ack),
            RoomCommand::RoomLeave(_) => WireReply::RoomLeave(Ack),
            RoomCommand::TaskClaim(_)
            | RoomCommand::TaskHeartbeat(_)
            | RoomCommand::TaskTransition(_)
            | RoomCommand::TaskUpdate(_)
            | RoomCommand::RoomTasks(_) => unreachable!("rejected in decide"),
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
            _ => ctx.by().clone(),
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
                total_count: 0,
                truncated: false,
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
        inbox: Registry::<ToyInbox>::with_idle(journal.clone(), hub.clone(), idle),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal: journal.clone(),
            hub,
            sidebar,
            agents: Arc::new(LeaseCountingFeed {
                active: leases.clone(),
            }),
        }))
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
        idem: idem.map(|k| IdemKey::parse(k).unwrap()),
        ..RoomPost::new(room, Body::parse(text).unwrap())
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
        .call(Read::stream(
            StreamId::Room {
                room: RoomId::Channel {
                    scope: RoomScope::Global,
                    name: ChannelName::parse("general").unwrap(),
                },
            },
            Seq::ORIGIN,
            None,
        ))
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
    let nora = Principal::Seat {
        seat: SeatName::parse("nora").unwrap(),
    };
    let sam_c = LocalClient::new(router.clone(), sam.clone());
    let nora_c = LocalClient::new(router.clone(), nora);

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
    let denied = nora_c
        .call(Read::stream(StreamId::Room { room: dm }, Seq::ORIGIN, None))
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
            timeout_ms: Some(15),
            ..Wait::new(Interest::Mentions, Cursor::live())
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
                    timeout_ms: Some(2000),
                    ..Wait::new(Interest::Mentions, wake_cursor)
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

#[tokio::test]
async fn operator_sidebar_lists_dms_addressed_to_its_seat() {
    let (router, _, _) = harness(Duration::from_secs(60));
    let owner_seat = SeatName::parse("owner").unwrap();
    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: owner_seat.clone(),
        },
    );
    let alice_p = Principal::Seat {
        seat: SeatName::parse("alice").unwrap(),
    };
    let alice = LocalClient::new(router.clone(), alice_p.clone());
    let to_owner = RoomRef::Dm {
        with: Principal::Seat { seat: owner_seat },
    };

    let mut sidebar = operator
        .watch(Interest::Sidebar, Cursor::live())
        .await
        .unwrap();
    let Some(Ok(Frame::Sidebar(initial))) = sidebar.next().await else {
        panic!("expected initial sidebar")
    };
    assert!(initial.direct_messages.is_empty());

    let first = alice
        .call(post(to_owner.clone(), "one", None))
        .await
        .unwrap();
    alice.call(post(to_owner, "two", None)).await.unwrap();

    let catchup = operator
        .call(Wait {
            timeout_ms: Some(1_000),
            ..Wait::new(Interest::Mentions, Cursor::default())
        })
        .await
        .expect("operator Interest::Mentions wakes on DM addressed to its seat");
    assert_eq!(catchup.entries.len(), 2);

    let snap = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(Ok(Frame::Sidebar(snap))) = sidebar.next().await
                && snap.direct_messages.first().is_some_and(|d| d.unread == 2)
            {
                break *snap;
            }
        }
    })
    .await
    .expect("operator sidebar lists the DM addressed to its seat");
    assert_eq!(snap.direct_messages.len(), 1);
    assert_eq!(snap.direct_messages[0].with, alice_p);
    assert_eq!(snap.direct_messages[0].room, first.room);

    let op_reply = operator
        .call(post(
            RoomRef::Dm {
                with: alice_p.clone(),
            },
            "ack from operator",
            None,
        ))
        .await
        .unwrap();
    assert_eq!(
        op_reply.room, first.room,
        "operator replying to @alice must land in the same DM room, never a second split DM"
    );

    operator
        .call(InboxMarkRead {
            room: first.room,
            upto: first.seq,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(Ok(Frame::Sidebar(snap))) = sidebar.next().await
                && snap.direct_messages.len() == 1
                && snap.direct_messages.first().is_some_and(|d| d.unread == 1)
            {
                break;
            }
        }
    })
    .await
    .expect("marking the first DM read leaves one unread (operator's own reply is not unread)");
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
        inbox: Registry::<ToyInbox>::new(journal.clone(), hub.clone()),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(DynamicNamesFeed { names: names_rx }),
        }))
    });

    let owner = LocalClient::new(
        router,
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let proj = BbProjectId::parse("proj_demo_alpha").unwrap();
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
    map.insert(proj.clone(), "demo-work".to_owned());
    names_tx.send(Arc::new(map)).unwrap();

    let named = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(Ok(Frame::Sidebar(snap))) = stream.next().await
                && snap.projects.first().and_then(|p| p.name.as_deref()) == Some("demo-work")
            {
                break *snap;
            }
        }
    })
    .await
    .expect("live sidebar watch emitted updated human project name");
    assert_eq!(named.projects[0].name.as_deref(), Some("demo-work"));
}

#[tokio::test]
async fn presence_router_slot_and_404_visibility_masking() {
    let (router, _, _) = harness(Duration::from_secs(60));
    let sam = Principal::Seat {
        seat: SeatName::parse("sam").unwrap(),
    };
    let alice = Principal::Seat {
        seat: SeatName::parse("alice").unwrap(),
    };
    let eve = Principal::Seat {
        seat: SeatName::parse("eve").unwrap(),
    };
    let sam_c = LocalClient::new(router.clone(), sam.clone());

    let err = sam_c.call(PresenceList::default()).await.unwrap_err();
    assert_eq!(
        err,
        ApiError::Unavailable {
            family: Family::Presence
        }
    );

    assert!(can_read(&sam, &StreamId::Presence).is_ok());

    let dm = StreamId::Room {
        room: RoomId::Direct {
            pair: PrincipalPair::new(sam.clone(), alice.clone()).unwrap(),
        },
    };
    assert!(can_read_visible(&sam, &dm, "/v1/rooms/dm").is_ok());
    assert_eq!(
        can_read_visible(&eve, &dm, "/v1/rooms/dm").unwrap_err(),
        ApiError::NotFound {
            what: "/v1/rooms/dm".into()
        }
    );
    let alice_inbox = StreamId::Inbox { owner: alice };
    assert_eq!(
        can_read_visible(&eve, &alice_inbox, "/v1/inbox").unwrap_err(),
        ApiError::NotFound {
            what: "/v1/inbox".into()
        }
    );
    assert_eq!(
        mask_unauthorized_as_not_found(ApiError::Timeout, "/v1/events"),
        ApiError::Timeout,
        "non-Denied errors stay intact"
    );
}

#[tokio::test]
async fn room_filtered_watch_matches_before_or_after_labels_and_threads_causally() {
    let (router, _, _) = harness(Duration::from_secs(60));
    let op = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let room_ref = chan("tasks");
    let info = op
        .call(RoomEnsure {
            room: room_ref.clone(),
            topic: None,
            origin: None,
        })
        .await
        .unwrap();

    let open_lbl = Label::parse("status=open").unwrap();
    let done_lbl = Label::parse("status=done").unwrap();
    let other_lbl = Label::parse("team=other").unwrap();

    let root = op
        .call(RoomPost {
            room: room_ref.clone(),
            body: Body::parse("root task").unwrap(),
            re: None,
            after: None,
            labels: vec![open_lbl.clone()],
            task: None,
            idem: None,
            origin: Default::default(),
        })
        .await
        .unwrap();
    let unrelated = op
        .call(RoomPost {
            room: room_ref.clone(),
            body: Body::parse("other thread").unwrap(),
            re: None,
            after: None,
            labels: vec![other_lbl.clone()],
            task: None,
            idem: None,
            origin: Default::default(),
        })
        .await
        .unwrap();
    let reply = op
        .call(RoomPost {
            room: room_ref.clone(),
            body: Body::parse("reply before wait").unwrap(),
            re: Some(root.seq),
            after: None,
            labels: vec![],
            task: None,
            idem: None,
            origin: Default::default(),
        })
        .await
        .unwrap();

    let already_landed = op
        .call(Wait {
            timeout_ms: Some(500),
            ..Wait::room_filtered(info.id.clone(), root.seq, Some(root.seq), vec![])
        })
        .await
        .unwrap();
    assert!(!already_landed.timed_out);
    assert_eq!(
        already_landed
            .entries
            .iter()
            .map(|e| e.seq)
            .collect::<Vec<_>>(),
        vec![reply.seq],
        "reply that landed before wait started still returns immediately"
    );

    let mut watch_open = op
        .watch(
            Interest::RoomFiltered {
                room: room_ref.clone(),
                re: None,
                labels: vec![open_lbl.clone()],
            },
            Cursor::live(),
        )
        .await
        .unwrap();
    let mut watch_done = op
        .watch(
            Interest::RoomFiltered {
                room: room_ref.clone(),
                re: None,
                labels: vec![done_lbl.clone()],
            },
            Cursor::live(),
        )
        .await
        .unwrap();
    let mut watch_thread = op
        .watch(
            Interest::RoomFiltered {
                room: room_ref.clone(),
                re: Some(root.seq),
                labels: vec![],
            },
            Cursor::live(),
        )
        .await
        .unwrap();

    assert!(matches!(
        watch_open.next().await,
        Some(Ok(Frame::CaughtUp { .. }))
    ));
    assert!(matches!(
        watch_done.next().await,
        Some(Ok(Frame::CaughtUp { .. }))
    ));
    assert!(matches!(
        watch_thread.next().await,
        Some(Ok(Frame::CaughtUp { .. }))
    ));

    op.call(RoomLabel {
        room: room_ref.clone(),
        seq: unrelated.seq,
        add: vec![Label::parse("team=infra").unwrap()],
        remove: vec![],
        idem: None,
    })
    .await
    .unwrap();

    op.call(RoomLabel {
        room: room_ref.clone(),
        seq: root.seq,
        add: vec![done_lbl.clone()],
        remove: vec![open_lbl.clone()],
        idem: None,
    })
    .await
    .unwrap();

    let open_ev = tokio::time::timeout(Duration::from_secs(2), watch_open.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Frame::Committed(open_env) = open_ev else {
        panic!("expected Committed for transition-out watcher");
    };
    assert!(matches!(
        open_env.event,
        Event::Room(RoomEvent::Labeled { target, .. }) if target == root.seq
    ));

    let done_ev = tokio::time::timeout(Duration::from_secs(2), watch_done.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Frame::Committed(done_env) = done_ev else {
        panic!("expected Committed for transition-in watcher");
    };
    assert_eq!(done_env.seq, open_env.seq);

    let thread_ev = tokio::time::timeout(Duration::from_secs(2), watch_thread.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Frame::Committed(thread_env) = thread_ev else {
        panic!("expected Committed for root label on thread watcher");
    };
    assert_eq!(thread_env.seq, open_env.seq);

    op.call(RoomLabel {
        room: room_ref.clone(),
        seq: reply.seq,
        add: vec![Label::parse("ack=yes").unwrap()],
        remove: vec![],
        idem: None,
    })
    .await
    .unwrap();
    let reply_label_ev = tokio::time::timeout(Duration::from_secs(2), watch_thread.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Frame::Committed(reply_label_env) = reply_label_ev else {
        panic!("expected Committed for label on seeded thread reply");
    };
    assert!(matches!(
        reply_label_env.event,
        Event::Room(RoomEvent::Labeled { target, .. }) if target == reply.seq
    ));
}

struct RecordingWaves {
    observed: Arc<std::sync::Mutex<Vec<(WaveSlug, RoleName, rex_wave_api::wave::Observation)>>>,
    activity: Arc<std::sync::Mutex<Activity>>,
}

impl Service<WaveCommand> for RecordingWaves {
    fn dispatch(
        &self,
        cmd: WaveCommand,
        _ctx: DecideCtx,
    ) -> BoxFut<'_, Result<WireReply, ApiError>> {
        let observed = self.observed.clone();
        let activity = self.activity.clone();
        Box::pin(async move {
            match cmd {
                WaveCommand::WaveObserve(obs) => {
                    if let rex_wave_api::wave::Observation::Activity {
                        activity: ref a, ..
                    } = obs.observation
                    {
                        *activity.lock().unwrap() = a.clone();
                    }
                    observed
                        .lock()
                        .unwrap()
                        .push((obs.wave, obs.role, obs.observation));
                    Ok(WireReply::WaveObserve(
                        rex_wave_api::wave::WaveObserved::APPLIED,
                    ))
                }
                WaveCommand::WaveView(v) => {
                    let act = activity.lock().unwrap().clone();
                    let role = RoleName::parse("author").unwrap();
                    let agent = rex_wave_api::wave::AgentSpec {
                        provider: ProviderId::parse("codex").unwrap(),
                        model: None,
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: rex_wave_api::wave::PermissionMode::AcceptEdits,
                        workspace: rex_wave_api::wave::Workspace::ProjectDefault,
                    };
                    let workers = vec![(
                        role,
                        agent.clone(),
                        rex_wave_api::wave::WorkerPhase::Live {
                            thread: BbThreadId::parse("t3:w1:author:a1").unwrap(),
                            activity: act,
                        },
                    )];
                    let counts = rex_wave_api::wave::WaveCounts::from_phases(
                        workers.iter().map(|(_, _, p)| p),
                    );
                    Ok(WireReply::WaveView(rex_wave_api::wave::WaveSnapshot {
                        spec: rex_wave_api::wave::WaveSpec {
                            slug: v.wave,
                            project: BbProjectId::parse("proj_1").unwrap(),
                            goal: Body::parse("goal").unwrap(),
                            defaults: agent,
                            output_dir: None,
                            workers: vec![],
                        },
                        status: rex_wave_api::wave::WaveStatus::Running,
                        workers,
                        counts,
                        worker_meta: std::collections::BTreeMap::new(),
                    }))
                }
                _ => Err(ApiError::Unavailable {
                    family: Family::Wave,
                }),
            }
        })
    }
}

#[tokio::test]
async fn worker_wait_emits_parked_and_working_and_restores_on_cancel() {
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(128);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let activity = Arc::new(std::sync::Mutex::new(Activity::Working));
    let waves = Arc::new(RecordingWaves {
        observed: observed.clone(),
        activity: activity.clone(),
    });
    let router = Arc::new(Router {
        room: Registry::<ToyRooms>::new(journal.clone(), hub.clone()),
        wave: waves,
        inbox: Registry::<ToyInbox>::new(journal.clone(), hub.clone()),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }))
    });

    let wave = WaveSlug::parse("w1").unwrap();
    let role = RoleName::parse("author").unwrap();
    let worker = LocalClient::new(
        router.clone(),
        Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        },
    );
    let op = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let room_ref = chan("wave-w1");
    let room_id = room_ref.resolve(op.me()).unwrap();

    let _ = worker
        .call(Wait {
            timeout_ms: Some(0),
            ..Wait::new(
                Interest::Room {
                    room: room_ref.clone(),
                },
                Cursor::default().with(
                    StreamId::Room {
                        room: room_id.clone(),
                    },
                    Seq(0),
                ),
            )
        })
        .await
        .unwrap();
    assert!(observed.lock().unwrap().is_empty());

    let waiter = tokio::spawn({
        let worker = worker.clone();
        let room_id = room_id.clone();
        async move {
            let mut w = Wait::room(room_id, Seq(0), false);
            w.timeout_ms = Some(2000);
            worker.call(w).await.unwrap()
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(*activity.lock().unwrap(), Activity::Parked { .. }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("worker observed Parked on Wait entry");

    op.call(post(room_ref.clone(), "wake up", None))
        .await
        .unwrap();
    let page = waiter.await.unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(*activity.lock().unwrap(), Activity::Working);
    {
        let log = observed.lock().unwrap();
        assert_eq!(log.len(), 2);
        assert!(matches!(
            &log[0].2,
            rex_wave_api::wave::Observation::Activity {
                activity: Activity::Parked { room: Some(r), since: Seq(0) },
                ..
            } if r == &room_id
        ));
        assert!(matches!(
            &log[1].2,
            rex_wave_api::wave::Observation::Activity {
                activity: Activity::Working,
                ..
            }
        ));
    }

    let cancelled = tokio::spawn({
        let worker = worker.clone();
        let room_id = room_id.clone();
        async move {
            let mut w = Wait::room(room_id, Seq(10), false);
            w.timeout_ms = Some(30_000);
            let _ = worker.call(w).await;
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(*activity.lock().unwrap(), Activity::Parked { .. }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("worker observed Parked before cancel");
    cancelled.abort();
    let _ = cancelled.await;

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if *activity.lock().unwrap() == Activity::Working {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("cancel drop guard restored Working");
}
