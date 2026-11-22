use futures_util::StreamExt;
use rex_wave_api::read::{Read, Wait};
use rex_wave_api::room::*;
use rex_wave_api::*;
use rex_wave_rooms::{Rooms, resolve_mentions, room_history};
use rex_wave_runtime::{
    Hub, LocalClient, NoAgentFeed, ReadSide, Registry, Router, SidebarProjection, Unavailable,
};
use rex_wave_store::Journal;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

fn build_harness(journal: Journal, idle: Duration) -> Arc<Router> {
    let hub = Hub::new(512);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    Arc::new(Router {
        room: Registry::<Rooms>::with_idle(journal.clone(), hub.clone(), idle),
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
    })
}

fn seat(name: &str) -> Principal {
    Principal::Seat {
        seat: SeatName::parse(name).unwrap(),
    }
}

fn operator(name: &str) -> Principal {
    Principal::Operator {
        seat: SeatName::parse(name).unwrap(),
    }
}

fn worker(wave: &str, role: &str) -> Principal {
    Principal::Worker {
        wave: WaveSlug::parse(wave).unwrap(),
        role: RoleName::parse(role).unwrap(),
    }
}

#[tokio::test]
async fn ensure_is_idempotent_across_global_project_wave_and_canonical_dms() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(&dir.path().join("rooms.sqlite3")).unwrap();
    let router = build_harness(journal.clone(), Duration::from_millis(30));

    let op = LocalClient::new(router.clone(), operator("owner"));
    let sam = LocalClient::new(router.clone(), seat("sam"));
    let alice = LocalClient::new(router.clone(), seat("alice"));
    let nodal = LocalClient::new(router.clone(), seat("nodal"));
    let conductor = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );

    let global_ref = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("general").unwrap(),
    };
    let g1 = sam
        .call(RoomEnsure {
            room: global_ref.clone(),
            topic: Some("Town hall".into()),
            origin: None,
        })
        .await
        .unwrap();
    let g2 = alice
        .call(RoomEnsure {
            room: global_ref.clone(),
            topic: None,
            origin: None,
        })
        .await
        .unwrap();
    assert_eq!(g1.id, g2.id);
    assert_eq!(
        g1.head, g2.head,
        "repeating room_ensure without topic change appends no events"
    );
    assert_eq!(g2.topic.as_deref(), Some("Town hall"));

    let proj = BbProjectId::parse("proj_3vs7nkyeum").unwrap();
    let proj_chan = RoomRef::Channel {
        scope: RoomScope::Project {
            project: proj.clone(),
        },
        name: ChannelName::parse("design").unwrap(),
    };
    let p1 = sam
        .call(RoomEnsure {
            room: proj_chan.clone(),
            topic: None,
            origin: None,
        })
        .await
        .unwrap();
    let p2 = sam
        .call(RoomEnsure {
            room: proj_chan.clone(),
            topic: None,
            origin: None,
        })
        .await
        .unwrap();
    assert_eq!(p1.id, p2.id);
    assert_eq!(p1.head, p2.head);

    let wave_slug = WaveSlug::parse("auth-v2").unwrap();
    let wave_room = RoomRef::Channel {
        scope: RoomScope::Project { project: proj },
        name: ChannelName::parse("wave-auth-v2").unwrap(),
    };
    let denied = sam
        .call(RoomEnsure {
            room: wave_room.clone(),
            topic: None,
            origin: Some(RoomOrigin::Wave {
                wave: wave_slug.clone(),
            }),
        })
        .await
        .unwrap_err();
    assert!(matches!(denied, ApiError::Denied { .. }));

    let squat_denied = sam
        .call(RoomEnsure {
            room: wave_room.clone(),
            topic: None,
            origin: None,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(squat_denied, ApiError::Denied { .. }),
        "wave-* names cannot be squatted as Manual"
    );

    let w1 = conductor
        .call(RoomEnsure {
            room: wave_room.clone(),
            topic: Some("Wave auth-v2".into()),
            origin: Some(RoomOrigin::Wave {
                wave: wave_slug.clone(),
            }),
        })
        .await
        .unwrap();
    let w2 = sam
        .call(RoomEnsure {
            room: wave_room.clone(),
            topic: None,
            origin: None,
        })
        .await
        .unwrap();
    assert_eq!(w1.id, w2.id);
    assert_eq!(w2.origin, RoomOrigin::Wave { wave: wave_slug });
    assert!(
        w1.members.is_empty(),
        "system wave conductor does not add itself as a chat handle"
    );

    let non_member_post = nodal
        .call(RoomPost {
            room: wave_room.clone(),
            body: Body::parse("sneaking in").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(non_member_post, ApiError::Denied { .. }));

    conductor
        .call(RoomJoin {
            room: wave_room.clone(),
            member: Some(worker("auth-v2", "verifier")),
            handle: None,
        })
        .await
        .unwrap();
    let op_post = op
        .call(RoomPost {
            room: wave_room.clone(),
            body: Body::parse("@verifier start review").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(
        op_post.mentions.principals,
        vec![worker("auth-v2", "verifier")]
    );

    let dm_from_sam = sam
        .call(RoomEnsure {
            room: RoomRef::Dm {
                with: seat("alice"),
            },
            topic: None,
            origin: None,
        })
        .await
        .unwrap();
    let dm_from_alice = alice
        .call(RoomEnsure {
            room: RoomRef::Dm { with: seat("sam") },
            topic: None,
            origin: None,
        })
        .await
        .unwrap();
    assert_eq!(
        dm_from_sam.id, dm_from_alice.id,
        "DM natural key normalizes pair order"
    );
    assert_eq!(dm_from_sam.head, dm_from_alice.head);
    assert_eq!(dm_from_sam.members.len(), 2);

    let self_dm = sam
        .call(RoomEnsure {
            room: RoomRef::Dm { with: seat("sam") },
            topic: None,
            origin: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(self_dm, ApiError::Invalid(_)));

    let dm_leave = sam
        .call(RoomLeave {
            room: RoomRef::Dm {
                with: seat("alice"),
            },
        })
        .await
        .unwrap_err();
    assert!(matches!(dm_leave, ApiError::Conflict { .. }));

    let outsider_dm = nodal
        .call(RoomPost {
            room: RoomRef::Id { id: dm_from_sam.id },
            body: Body::parse("intrude").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(outsider_dm, ApiError::Denied { .. }));
}

#[test]
fn resolve_mentions_parses_handles_qualified_roles_all_and_ignores_emails() {
    let mut members = BTreeMap::new();
    let sam = seat("sam");
    let verifier = worker("auth-v2", "verifier");
    let author = worker("auth-v2", "author-jwt");
    members.insert(Handle::parse("sam").unwrap(), sam.clone());
    members.insert(Handle::parse("verifier").unwrap(), verifier.clone());
    members.insert(Handle::parse("author-jwt").unwrap(), author.clone());

    let body = Body::parse(
        "Ping (@verifier, @auth-v2/author-jwt!) and email ops@sam.dev or @missing; cc @sam.",
    )
    .unwrap();
    let m = resolve_mentions(&body, &members);
    assert!(!m.all);
    assert_eq!(m.principals.len(), 3);
    assert!(m.principals.contains(&sam));
    assert!(m.principals.contains(&verifier));
    assert!(m.principals.contains(&author));

    let all_body = Body::parse("Heads up @all!").unwrap();
    let m_all = resolve_mentions(&all_body, &members);
    assert!(m_all.all);
    assert_eq!(m_all.principals.len(), 3);
}

#[test]
fn resolve_mentions_skips_inline_code_and_fenced_blocks() {
    let mut members = BTreeMap::new();
    let sam = seat("sam");
    let verifier = worker("auth-v2", "verifier");
    members.insert(Handle::parse("sam").unwrap(), sam.clone());
    members.insert(Handle::parse("verifier").unwrap(), verifier.clone());

    let quoted = Body::parse(
        "Post one message addressed to `@sam`, or ``@all `x` here``.\n```\n@verifier ok\n```\nDone.",
    )
    .unwrap();
    let m = resolve_mentions(&quoted, &members);
    assert!(!m.all, "@all inside a double-backtick span is quoted");
    assert!(m.principals.is_empty(), "{:?}", m.principals);

    let mixed =
        Body::parse("`@sam` is quoted but @verifier is addressed; stray ` then @sam").unwrap();
    let m = resolve_mentions(&mixed, &members);
    assert_eq!(m.principals.len(), 2, "unclosed backtick is literal");
    assert!(m.principals.contains(&sam));
    assert!(m.principals.contains(&verifier));
}

#[tokio::test]
async fn join_leave_handle_uniqueness_and_room_history_across_passivation() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(&dir.path().join("history.sqlite3")).unwrap();
    let router = build_harness(journal, Duration::from_millis(25));

    let sam = LocalClient::new(router.clone(), seat("sam"));
    let alice = LocalClient::new(router.clone(), seat("alice"));
    let nodal = LocalClient::new(router.clone(), seat("nodal"));
    let w1_ver = LocalClient::new(router.clone(), worker("w1", "verifier"));
    let w2_ver = LocalClient::new(router.clone(), worker("w2", "verifier"));

    let room = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("ops").unwrap(),
    };

    sam.call(RoomEnsure {
        room: room.clone(),
        topic: None,
        origin: None,
    })
    .await
    .unwrap();
    sam.call(RoomJoin {
        room: room.clone(),
        member: Some(seat("alice")),
        handle: None,
    })
    .await
    .unwrap();

    let unauthorized_invite = alice
        .call(RoomJoin {
            room: room.clone(),
            member: Some(seat("nodal")),
            handle: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(unauthorized_invite, ApiError::Denied { .. }));

    let duplicate_handle = nodal
        .call(RoomJoin {
            room: room.clone(),
            member: None,
            handle: Some(Handle::parse("alice").unwrap()),
        })
        .await
        .unwrap_err();
    assert!(matches!(duplicate_handle, ApiError::Conflict { .. }));

    w1_ver
        .call(RoomJoin {
            room: room.clone(),
            member: None,
            handle: None,
        })
        .await
        .unwrap();
    w2_ver
        .call(RoomJoin {
            room: room.clone(),
            member: None,
            handle: None,
        })
        .await
        .unwrap();
    let info = sam
        .call(RoomEnsure {
            room: room.clone(),
            topic: None,
            origin: None,
        })
        .await
        .unwrap();
    assert_eq!(
        info.members.len(),
        4,
        "colliding default worker handles disambiguate cleanly"
    );

    let first = sam
        .call(RoomPost {
            room: room.clone(),
            body: Body::parse("hello @alice").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: Some(IdemKey::parse("post-1").unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(first.mentions.principals, vec![seat("alice")]);

    let bad_reply = alice
        .call(RoomPost {
            room: room.clone(),
            body: Body::parse("bad reply target").unwrap(),
            re: Some(Seq(1)),
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(bad_reply, ApiError::NotFound { .. }),
        "Opened seq cannot be a reply_to target"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;

    let replayed = sam
        .call(RoomPost {
            room: room.clone(),
            body: Body::parse("different text").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: Some(IdemKey::parse("post-1").unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(
        replayed.seq, first.seq,
        "idempotent retry after passivation returns original seq"
    );

    let second = alice
        .call(RoomPost {
            room: room.clone(),
            body: Body::parse("ack @sam").unwrap(),
            re: Some(first.seq),
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();

    alice.call(RoomLeave { room: room.clone() }).await.unwrap();
    let leave_twice = alice
        .call(RoomLeave { room: room.clone() })
        .await
        .unwrap_err();
    assert!(matches!(leave_twice, ApiError::Conflict { .. }));

    let history = room_history(&sam, room.clone(), Seq::ORIGIN, Some(10))
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].seq, first.seq);
    assert_eq!(history[0].body.as_str(), "hello @alice");
    assert_eq!(history[1].seq, second.seq);
    assert_eq!(history[1].reply_to, Some(first.seq));

    let raw = sam
        .call(Read {
            stream: StreamId::Room {
                room: room.resolve(sam.me()).unwrap(),
            },
            after: Seq::ORIGIN,
            limit: None,
        })
        .await
        .unwrap();
    assert!(
        raw.entries.len() > history.len(),
        "room_history filters non-post membership events"
    );
}

#[tokio::test]
async fn concurrent_posts_and_anti_sleep_through_wait_slices() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(&dir.path().join("concurrent.sqlite3")).unwrap();
    let router = build_harness(journal, Duration::from_secs(60));

    let sam = LocalClient::new(router.clone(), seat("sam"));
    let alice = LocalClient::new(router.clone(), seat("alice"));
    let verifier = LocalClient::new(router.clone(), worker("auth-v2", "verifier"));

    let chan = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("coord").unwrap(),
    };
    let room_id = chan.resolve(sam.me()).unwrap();

    sam.call(RoomEnsure {
        room: chan.clone(),
        topic: None,
        origin: None,
    })
    .await
    .unwrap();
    alice
        .call(RoomJoin {
            room: chan.clone(),
            member: None,
            handle: None,
        })
        .await
        .unwrap();
    verifier
        .call(RoomJoin {
            room: chan.clone(),
            member: None,
            handle: None,
        })
        .await
        .unwrap();

    let mut handles = Vec::new();
    for writer_idx in 0..4u32 {
        let c = if writer_idx % 2 == 0 {
            sam.clone()
        } else {
            alice.clone()
        };
        let room_ref = chan.clone();
        handles.push(tokio::spawn(async move {
            let mut seqs = Vec::new();
            for msg_idx in 0..10u32 {
                let text = format!("w{writer_idx}-m{msg_idx}");
                let res = c
                    .call(RoomPost {
                        room: room_ref.clone(),
                        body: Body::parse(text).unwrap(),
                        re: None,
                        after: None,
                        labels: Vec::new(),
                        task: None,
                        idem: None,
                    })
                    .await
                    .unwrap();
                seqs.push(res.seq.0);
            }
            seqs
        }));
    }

    let mut watch = verifier
        .watch(Interest::Room { room: chan.clone() }, Cursor::default())
        .await
        .unwrap();
    let mut committed_posts = Vec::new();
    for h in handles {
        committed_posts.extend(h.await.unwrap());
    }
    committed_posts.sort_unstable();
    committed_posts.dedup();
    assert_eq!(
        committed_posts.len(),
        40,
        "40 concurrent posts receive 40 distinct monotonic seqs"
    );

    let head = *committed_posts.last().unwrap();
    let mut watched_seqs = Vec::new();
    while watched_seqs.last().copied().unwrap_or(0) < head {
        if let Some(Ok(Frame::Committed(env))) = watch.next().await {
            watched_seqs.push(env.seq.0);
        }
    }
    let expected: Vec<u64> = (1..=head).collect();
    assert_eq!(
        watched_seqs, expected,
        "watch stream observes gapless seqs 1..=head under concurrency"
    );

    let room_catchup = verifier
        .call(Wait::room(room_id.clone(), Seq(head - 3), false))
        .await
        .unwrap();
    assert!(!room_catchup.timed_out);
    assert_eq!(
        room_catchup
            .entries
            .iter()
            .map(|e| e.seq.0)
            .collect::<Vec<_>>(),
        vec![head - 2, head - 1, head]
    );

    let pre_mention_noise = sam
        .call(RoomPost {
            room: chan.clone(),
            body: Body::parse("background chatter 1").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();
    let mention_hit = sam
        .call(RoomPost {
            room: chan.clone(),
            body: Body::parse("please check @verifier").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();
    let all_hit = alice
        .call(RoomPost {
            room: chan.clone(),
            body: Body::parse("@all ship gate ready").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();

    let mention_catchup = verifier
        .call(Wait::room(room_id.clone(), Seq(head), true))
        .await
        .unwrap();
    assert!(!mention_catchup.timed_out);
    assert_eq!(
        mention_catchup
            .entries
            .iter()
            .map(|e| e.seq)
            .collect::<Vec<_>>(),
        vec![mention_hit.seq, all_hit.seq],
        "mentions-only Wait skips non-mention chatter ({}) and returns full mention catch-up slice",
        pre_mention_noise.seq.0
    );

    let dm_wait = tokio::spawn({
        let verifier = verifier.clone();
        let after = mention_catchup.cursor.clone();
        async move {
            verifier
                .call(Wait {
                    interest: Interest::Mentions,
                    after,
                    timeout_ms: Some(2000),
                })
                .await
                .unwrap()
        }
    });
    tokio::time::sleep(Duration::from_millis(15)).await;
    let dm_post = sam
        .call(RoomPost {
            room: RoomRef::Dm {
                with: worker("auth-v2", "verifier"),
            },
            body: Body::parse("direct ping").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();
    let woken = dm_wait.await.unwrap();
    assert!(!woken.timed_out);
    assert_eq!(woken.entries.len(), 1);
    assert_eq!(woken.entries[0].seq, dm_post.seq);
}

#[tokio::test]
async fn flat_thread_normalization_after_cas_idempotency_and_auth_isolation() {
    let journal = Journal::in_memory().unwrap();
    let router = build_harness(journal, Duration::from_secs(60));
    let sam = LocalClient::new(router.clone(), seat("sam"));
    let alice = LocalClient::new(router.clone(), seat("alice"));
    let eve = LocalClient::new(router.clone(), seat("eve"));
    let room = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("threads").unwrap(),
    };

    let r1 = sam
        .call(RoomPost {
            after: Some(Seq::ORIGIN),
            labels: vec![
                Label::parse("z=2").unwrap(),
                Label::parse("a=1").unwrap(),
                Label::parse("a=1").unwrap(),
            ],
            ..RoomPost::new(room.clone(), Body::parse("root one").unwrap())
        })
        .await
        .unwrap();
    assert_eq!(r1.re, None);

    let r2 = alice
        .call(RoomPost::new(
            room.clone(),
            Body::parse("root two").unwrap(),
        ))
        .await
        .unwrap();

    let p1 = alice
        .call(RoomPost {
            re: Some(r1.seq),
            after: Some(r1.seq),
            ..RoomPost::new(room.clone(), Body::parse("first reply on r1").unwrap())
        })
        .await
        .unwrap();
    assert_eq!(p1.re, Some(r1.seq));

    let p2 = sam
        .call(RoomPost {
            re: Some(p1.seq),
            after: Some(p1.seq),
            ..RoomPost::new(room.clone(), Body::parse("reply to reply").unwrap())
        })
        .await
        .unwrap();
    assert_eq!(
        p2.re,
        Some(r1.seq),
        "replying to a reply normalizes re to canonical thread root"
    );

    let task_with_re = sam
        .call(RoomPost {
            re: Some(r1.seq),
            task: Some(TaskSpec {
                title: rex_wave_api::board::Title::parse("Illegal child task").unwrap(),
                org_id: None,
                needs: vec![],
                priority: None,
                due_ms: None,
            }),
            ..RoomPost::new(room.clone(), Body::parse("nope").unwrap())
        })
        .await
        .unwrap_err();
    assert!(matches!(task_with_re, ApiError::Invalid(_)));

    let stale_room_after = sam
        .call(RoomPost {
            after: Some(r1.seq),
            ..RoomPost::new(room.clone(), Body::parse("stale top-level").unwrap())
        })
        .await
        .unwrap_err();
    let ApiError::Conflict {
        data:
            Some(ConflictData::NewerMessage {
                expected,
                latest,
                newer_message,
            }),
        ..
    } = stale_room_after
    else {
        panic!("expected NewerMessage conflict, got {stale_room_after:?}");
    };
    assert_eq!(expected, r1.seq);
    assert_eq!(latest, p2.seq);
    assert_eq!(newer_message.seq, p2.seq);
    assert_eq!(newer_message.body.as_str(), "reply to reply");
    assert_eq!(newer_message.re, Some(r1.seq));

    let r2_reply = alice
        .call(RoomPost {
            re: Some(r2.seq),
            ..RoomPost::new(room.clone(), Body::parse("chatter in r2").unwrap())
        })
        .await
        .unwrap();
    let p3_idem = IdemKey::parse("retry-after-r1").unwrap();
    let p3 = sam
        .call(RoomPost {
            re: Some(p2.seq),
            after: Some(p2.seq),
            idem: Some(p3_idem.clone()),
            ..RoomPost::new(room.clone(), Body::parse("guarded reply on r1").unwrap())
        })
        .await
        .unwrap();
    assert_eq!(p3.re, Some(r1.seq));
    assert!(p3.seq > r2_reply.seq);

    let p4 = alice
        .call(RoomPost {
            re: Some(r1.seq),
            after: Some(p3.seq),
            ..RoomPost::new(room.clone(), Body::parse("later reply on r1").unwrap())
        })
        .await
        .unwrap();

    let stale_thread_after = sam
        .call(RoomPost {
            re: Some(r1.seq),
            after: Some(p3.seq),
            ..RoomPost::new(room.clone(), Body::parse("missed p4").unwrap())
        })
        .await
        .unwrap_err();
    assert!(matches!(
        stale_thread_after,
        ApiError::Conflict {
            data: Some(ConflictData::NewerMessage { latest, .. }),
            ..
        } if latest == p4.seq
    ));

    let replayed = sam
        .call(RoomPost {
            re: Some(p2.seq),
            after: Some(p2.seq),
            idem: Some(p3_idem),
            ..RoomPost::new(room.clone(), Body::parse("retried body").unwrap())
        })
        .await
        .unwrap();
    assert_eq!(
        replayed.seq, p3.seq,
        "idempotent retry with stale `after` replays original receipt without false conflict"
    );

    let shared_idem = IdemKey::parse("shared-cross-cmd-k1").unwrap();
    let task_post = sam
        .call(RoomPost {
            idem: Some(shared_idem.clone()),
            task: Some(TaskSpec {
                title: rex_wave_api::board::Title::parse("Cross-cmd idem task").unwrap(),
                org_id: None,
                needs: vec![],
                priority: None,
                due_ms: None,
            }),
            ..RoomPost::new(room.clone(), Body::parse("task root").unwrap())
        })
        .await
        .unwrap();
    let lbl_res = sam
        .call(RoomLabel {
            room: room.clone(),
            seq: task_post.seq,
            add: vec![Label::parse("tier=p1").unwrap()],
            remove: vec![],
            idem: Some(shared_idem.clone()),
        })
        .await
        .unwrap();
    assert_eq!(lbl_res.labels, vec![Label::parse("tier=p1").unwrap()]);
    let claim_res = sam
        .call(TaskClaim {
            room: room.clone(),
            task: task_post.seq,
            ttl_ms: Some(60_000),
            expected_rev: Some(1),
            force: false,
            idem: Some(shared_idem.clone()),
        })
        .await
        .unwrap();
    assert_eq!(claim_res.epoch, LeaseEpoch(1));
    let upd_res = sam
        .call(TaskUpdate {
            room: room.clone(),
            task: task_post.seq,
            expected_rev: Some(2),
            title: Some(rex_wave_api::board::Title::parse("Renamed idem task").unwrap()),
            needs_add: vec![],
            needs_remove: vec![],
            priority: None,
            due_ms: None,
            clear_due: false,
            idem: Some(shared_idem.clone()),
        })
        .await
        .unwrap();
    assert_eq!(upd_res.rev, 3);
    let trans_res = sam
        .call(TaskTransition {
            room: room.clone(),
            task: task_post.seq,
            status: TaskStatus::Done,
            epoch: Some(claim_res.epoch),
            expected_rev: Some(3),
            note: Some(Body::parse("closed cleanly").unwrap()),
            idem: Some(shared_idem.clone()),
        })
        .await
        .unwrap();
    assert_eq!(trans_res.status, TaskStatus::Done);
    let trans_replay = sam
        .call(TaskTransition {
            room: room.clone(),
            task: task_post.seq,
            status: TaskStatus::Done,
            epoch: Some(claim_res.epoch),
            expected_rev: Some(3),
            note: Some(Body::parse("closed cleanly").unwrap()),
            idem: Some(shared_idem),
        })
        .await
        .unwrap();
    assert_eq!(trans_replay.rev, trans_res.rev);

    let dm = sam
        .call(RoomPost::new(
            RoomRef::Dm {
                with: seat("alice"),
            },
            Body::parse("top secret dm payload").unwrap(),
        ))
        .await
        .unwrap();
    let probe = eve
        .call(RoomPost {
            after: Some(Seq::ORIGIN),
            ..RoomPost::new(RoomRef::Id { id: dm.room }, Body::parse("probe").unwrap())
        })
        .await
        .unwrap_err();
    assert!(
        matches!(probe, ApiError::Denied { .. }),
        "unauthorized DM post must fail with Denied, never leaking NewerMessage"
    );
}

#[tokio::test]
async fn room_label_before_after_watch_delivery_noop_and_idem() {
    let journal = Journal::in_memory().unwrap();
    let router = build_harness(journal, Duration::from_secs(60));
    let sam = LocalClient::new(router.clone(), seat("sam"));
    let room = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("labels").unwrap(),
    };

    let open = Label::parse("status=open").unwrap();
    let done = Label::parse("status=done").unwrap();
    let area = Label::parse("area=rooms").unwrap();

    let post = sam
        .call(RoomPost {
            labels: vec![area.clone(), open.clone(), open.clone()],
            ..RoomPost::new(room.clone(), Body::parse("labeled root").unwrap())
        })
        .await
        .unwrap();

    let mut watch_open = sam
        .watch(
            Interest::RoomFiltered {
                room: room.clone(),
                re: None,
                labels: vec![open.clone()],
            },
            Cursor::live(),
        )
        .await
        .unwrap();
    let mut watch_done = sam
        .watch(
            Interest::RoomFiltered {
                room: room.clone(),
                re: None,
                labels: vec![done.clone()],
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

    let label_idem = IdemKey::parse("lbl-transition-1").unwrap();
    let res = sam
        .call(RoomLabel {
            room: room.clone(),
            seq: post.seq,
            add: vec![done.clone()],
            remove: vec![open.clone()],
            idem: Some(label_idem.clone()),
        })
        .await
        .unwrap();
    assert_eq!(res.labels, vec![area.clone(), done.clone()]);

    let ev_open = tokio::time::timeout(Duration::from_secs(2), watch_open.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let ev_done = tokio::time::timeout(Duration::from_secs(2), watch_done.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let (Frame::Committed(env_open), Frame::Committed(env_done)) = (ev_open, ev_done) else {
        panic!("expected Committed frames for both before and after label watchers");
    };
    assert_eq!(env_open.seq, env_done.seq);
    let Event::Room(RoomEvent::Labeled {
        target,
        added,
        removed,
        before,
        after,
    }) = env_open.event
    else {
        panic!("expected RoomEvent::Labeled");
    };
    assert_eq!(target, post.seq);
    assert_eq!(added, vec![done.clone()]);
    assert_eq!(removed, vec![open.clone()]);
    assert_eq!(before, vec![area.clone(), open.clone()]);
    assert_eq!(after, vec![area.clone(), done.clone()]);

    let head_before_noop = sam
        .call(RoomEnsure {
            room: room.clone(),
            topic: None,
            origin: None,
        })
        .await
        .unwrap()
        .head;
    let noop = sam
        .call(RoomLabel {
            room: room.clone(),
            seq: post.seq,
            add: vec![done.clone()],
            remove: vec![open.clone()],
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(noop.labels, vec![area.clone(), done.clone()]);
    let head_after_noop = sam
        .call(RoomEnsure {
            room: room.clone(),
            topic: None,
            origin: None,
        })
        .await
        .unwrap()
        .head;
    assert_eq!(
        head_before_noop, head_after_noop,
        "no-op RoomLabel appends no journal entry"
    );

    let extra = Label::parse("review=yes").unwrap();
    sam.call(RoomLabel {
        room: room.clone(),
        seq: post.seq,
        add: vec![extra.clone()],
        remove: vec![],
        idem: None,
    })
    .await
    .unwrap();
    let replayed = sam
        .call(RoomLabel {
            room,
            seq: post.seq,
            add: vec![done.clone()],
            remove: vec![open],
            idem: Some(label_idem),
        })
        .await
        .unwrap();
    assert_eq!(
        replayed.labels,
        vec![area, done],
        "idempotent RoomLabel replay returns the original Labeled snapshot"
    );
}

#[tokio::test]
async fn task_lifecycle_claim_race_ttl_takeover_fencing_unmet_needs_cycles_and_notes() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(&dir.path().join("tasks.sqlite3")).unwrap();
    let router = build_harness(journal, Duration::from_millis(30));

    let op = LocalClient::new(router.clone(), operator("owner"));
    let sam = LocalClient::new(router.clone(), seat("sam"));
    let w1 = LocalClient::new(router.clone(), worker("w3", "author"));
    let w2 = LocalClient::new(router.clone(), worker("w3", "verifier"));

    let room = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("tasks").unwrap(),
    };
    let chat_msg = sam
        .call(RoomPost::new(
            room.clone(),
            Body::parse("plain chat").unwrap(),
        ))
        .await
        .unwrap();

    let task_a = op
        .call(RoomPost {
            labels: vec![Label::parse("area=core").unwrap()],
            task: Some(TaskSpec {
                title: rex_wave_api::board::Title::parse("Task A foundation").unwrap(),
                org_id: None,
                needs: vec![],
                priority: Some(rex_wave_api::board::Priority::High),
                due_ms: Some(UnixMs(1_900_000_000_000)),
            }),
            ..RoomPost::new(room.clone(), Body::parse("build foundation").unwrap())
        })
        .await
        .unwrap();
    let org_a = task_a
        .org_id
        .clone()
        .expect("omitted org_id deterministically minted");
    assert!(org_a.is_rex_minted());

    let emacs_id = OrgId::parse("src-bush1951").unwrap();
    let task_b = op
        .call(RoomPost {
            task: Some(TaskSpec {
                title: rex_wave_api::board::Title::parse("Task B depends on A").unwrap(),
                org_id: Some(emacs_id.clone()),
                needs: vec![task_a.seq],
                priority: None,
                due_ms: None,
            }),
            ..RoomPost::new(room.clone(), Body::parse("depends on A").unwrap())
        })
        .await
        .unwrap();
    assert_eq!(
        task_b.org_id.as_ref(),
        Some(&emacs_id),
        "Emacs-supplied OrgId is preserved verbatim"
    );

    let dup_org = op
        .call(RoomPost {
            task: Some(TaskSpec {
                title: rex_wave_api::board::Title::parse("Duplicate org id").unwrap(),
                org_id: Some(emacs_id.clone()),
                needs: vec![],
                priority: None,
                due_ms: None,
            }),
            ..RoomPost::new(room.clone(), Body::parse("dup").unwrap())
        })
        .await
        .unwrap_err();
    assert!(matches!(dup_org, ApiError::Conflict { .. }));

    let non_task_dep = op
        .call(RoomPost {
            task: Some(TaskSpec {
                title: rex_wave_api::board::Title::parse("Bad dep").unwrap(),
                org_id: None,
                needs: vec![chat_msg.seq],
                priority: None,
                due_ms: None,
            }),
            ..RoomPost::new(room.clone(), Body::parse("bad dep").unwrap())
        })
        .await
        .unwrap_err();
    assert!(matches!(non_task_dep, ApiError::Invalid(_)));

    let missing_dep = op
        .call(RoomPost {
            task: Some(TaskSpec {
                title: rex_wave_api::board::Title::parse("Missing dep").unwrap(),
                org_id: None,
                needs: vec![Seq(999)],
                priority: None,
                due_ms: None,
            }),
            ..RoomPost::new(room.clone(), Body::parse("missing dep").unwrap())
        })
        .await
        .unwrap_err();
    assert!(matches!(missing_dep, ApiError::NotFound { .. }));

    let ready_initial = op
        .call(RoomTasks {
            room: room.clone(),
            filter: Some(TaskFilter {
                ready_only: true,
                ..Default::default()
            }),
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(
        ready_initial
            .tasks
            .iter()
            .map(|t| t.seq)
            .collect::<Vec<_>>(),
        vec![task_a.seq],
        "only Task A is ready while Task B waits on Task A"
    );

    let unmet = w1
        .call(TaskClaim {
            room: room.clone(),
            task: task_b.seq,
            ttl_ms: Some(10_000),
            expected_rev: None,
            force: false,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        unmet,
        ApiError::Conflict {
            data: Some(ConflictData::UnmetNeeds { ref unmet }),
            ..
        } if unmet == &vec![task_a.seq]
    ));

    let non_op_force = w1
        .call(TaskClaim {
            room: room.clone(),
            task: task_b.seq,
            ttl_ms: Some(10_000),
            expected_rev: None,
            force: true,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(non_op_force, ApiError::Denied { .. }));

    let (r_w1, r_w2) = tokio::join!(
        w1.call(TaskClaim {
            room: room.clone(),
            task: task_a.seq,
            ttl_ms: Some(60_000),
            expected_rev: Some(1),
            force: false,
            idem: None,
        }),
        w2.call(TaskClaim {
            room: room.clone(),
            task: task_a.seq,
            ttl_ms: Some(60_000),
            expected_rev: Some(1),
            force: false,
            idem: None,
        }),
    );
    let (winner_client, loser_client, winner_receipt) = match (r_w1, r_w2) {
        (Ok(win), Err(err)) => {
            assert!(matches!(
                err,
                ApiError::Conflict {
                    data: Some(
                        ConflictData::RevisionConflict { .. } | ConflictData::LeaseFenced { .. }
                    ),
                    ..
                }
            ));
            (w1.clone(), w2.clone(), win)
        }
        (Err(err), Ok(win)) => {
            assert!(matches!(
                err,
                ApiError::Conflict {
                    data: Some(
                        ConflictData::RevisionConflict { .. } | ConflictData::LeaseFenced { .. }
                    ),
                    ..
                }
            ));
            (w2.clone(), w1.clone(), win)
        }
        other => panic!("expected exactly one winner and one conflict, got {other:?}"),
    };
    assert_eq!(winner_receipt.epoch, LeaseEpoch(1));
    assert_eq!(winner_receipt.rev, 2);

    let mine_tasks = winner_client
        .call(RoomTasks {
            room: room.clone(),
            filter: Some(TaskFilter {
                holder: Some(winner_client.me().clone()),
                label: Some(Label::parse("area=core").unwrap()),
                ..Default::default()
            }),
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(mine_tasks.tasks.len(), 1);
    assert_eq!(mine_tasks.tasks[0].seq, task_a.seq);

    let hb_ok = winner_client
        .call(TaskHeartbeat {
            room: room.clone(),
            task: task_a.seq,
            epoch: winner_receipt.epoch,
            ttl_ms: Some(40),
        })
        .await
        .unwrap();
    assert_eq!(hb_ok.rev, 2, "heartbeat does not bump rev");

    let hb_fenced = loser_client
        .call(TaskHeartbeat {
            room: room.clone(),
            task: task_a.seq,
            epoch: winner_receipt.epoch,
            ttl_ms: Some(60_000),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        hb_fenced,
        ApiError::Conflict {
            data: Some(ConflictData::LeaseFenced { .. }),
            ..
        }
    ));

    let trans_unauthenticated_epoch = loser_client
        .call(TaskTransition {
            room: room.clone(),
            task: task_a.seq,
            status: TaskStatus::Done,
            epoch: None,
            expected_rev: None,
            note: None,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        trans_unauthenticated_epoch,
        ApiError::Conflict {
            data: Some(ConflictData::LeaseFenced { .. }),
            ..
        }
    ));

    tokio::time::sleep(Duration::from_millis(60)).await;
    let takeover = loser_client
        .call(TaskClaim {
            room: room.clone(),
            task: task_a.seq,
            ttl_ms: Some(60_000),
            expected_rev: None,
            force: false,
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(takeover.epoch, LeaseEpoch(2));
    assert_eq!(takeover.rev, 3);

    let stale_hb = winner_client
        .call(TaskHeartbeat {
            room: room.clone(),
            task: task_a.seq,
            epoch: LeaseEpoch(1),
            ttl_ms: Some(60_000),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        stale_hb,
        ApiError::Conflict {
            data: Some(ConflictData::LeaseFenced {
                expected: LeaseEpoch(1),
                actual: LeaseEpoch(2),
                ..
            }),
            ..
        }
    ));
    let stale_trans = winner_client
        .call(TaskTransition {
            room: room.clone(),
            task: task_a.seq,
            status: TaskStatus::Done,
            epoch: Some(LeaseEpoch(1)),
            expected_rev: None,
            note: None,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        stale_trans,
        ApiError::Conflict {
            data: Some(ConflictData::LeaseFenced { .. }),
            ..
        }
    ));

    let op_reset = op
        .call(TaskTransition {
            room: room.clone(),
            task: task_a.seq,
            status: TaskStatus::Open,
            epoch: None,
            expected_rev: Some(3),
            note: Some(Body::parse("reopening for @sam").unwrap()),
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(op_reset.status, TaskStatus::Open);
    assert_eq!(op_reset.rev, 4);
    assert!(op_reset.lease.is_none());

    let fenced_after_op = loser_client
        .call(TaskTransition {
            room: room.clone(),
            task: task_a.seq,
            status: TaskStatus::Done,
            epoch: Some(LeaseEpoch(2)),
            expected_rev: None,
            note: None,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        fenced_after_op,
        ApiError::Conflict {
            data: Some(ConflictData::LeaseFenced { .. }),
            ..
        }
    ));

    tokio::time::sleep(Duration::from_millis(50)).await;
    let third_claim = w1
        .call(TaskClaim {
            room: room.clone(),
            task: task_a.seq,
            ttl_ms: Some(60_000),
            expected_rev: Some(4),
            force: false,
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(third_claim.epoch, LeaseEpoch(3));
    assert_eq!(third_claim.rev, 5);

    let bad_claimed_transition = op
        .call(TaskTransition {
            room: room.clone(),
            task: task_a.seq,
            status: TaskStatus::Claimed,
            epoch: None,
            expected_rev: None,
            note: None,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(bad_claimed_transition, ApiError::Invalid(_)));

    let done_a = w1
        .call(TaskTransition {
            room: room.clone(),
            task: task_a.seq,
            status: TaskStatus::Done,
            epoch: Some(third_claim.epoch),
            expected_rev: Some(5),
            note: Some(Body::parse("foundation shipped @sam").unwrap()),
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(done_a.status, TaskStatus::Done);
    assert_eq!(done_a.rev, 6);

    let msgs = room_history(&op, room.clone(), Seq::ORIGIN, Some(50))
        .await
        .unwrap();
    let transition_note = msgs
        .iter()
        .find(|m| m.body.as_str() == "foundation shipped @sam")
        .expect("atomic transition note logged into task thread");
    assert_eq!(transition_note.reply_to, Some(task_a.seq));
    assert_eq!(transition_note.mentions.principals, vec![seat("sam")]);

    let ready_after_a = op
        .call(RoomTasks {
            room: room.clone(),
            filter: Some(TaskFilter {
                ready_only: true,
                ..Default::default()
            }),
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(ready_after_a.total_count, 1);
    assert!(!ready_after_a.truncated);
    assert_eq!(
        ready_after_a
            .tasks
            .iter()
            .map(|t| t.seq)
            .collect::<Vec<_>>(),
        vec![task_b.seq]
    );

    let limited = op
        .call(RoomTasks {
            room: room.clone(),
            filter: None,
            limit: Some(1),
        })
        .await
        .unwrap();
    assert_eq!(limited.tasks.len(), 1);
    assert_eq!(limited.total_count, 2);
    assert!(limited.truncated);

    let task_c = op
        .call(RoomPost {
            task: Some(TaskSpec {
                title: rex_wave_api::board::Title::parse("Task C depends on B").unwrap(),
                org_id: None,
                needs: vec![task_b.seq],
                priority: None,
                due_ms: None,
            }),
            ..RoomPost::new(room.clone(), Body::parse("depends on B").unwrap())
        })
        .await
        .unwrap();
    let op_force = op
        .call(TaskClaim {
            room: room.clone(),
            task: task_c.seq,
            ttl_ms: Some(60_000),
            expected_rev: Some(1),
            force: true,
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(op_force.epoch, LeaseEpoch(1));

    let stale_update = op
        .call(TaskUpdate {
            room: room.clone(),
            task: task_a.seq,
            expected_rev: Some(1),
            title: Some(rex_wave_api::board::Title::parse("Stale").unwrap()),
            needs_add: vec![],
            needs_remove: vec![],
            priority: None,
            due_ms: None,
            clear_due: false,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        stale_update,
        ApiError::Conflict {
            data: Some(ConflictData::RevisionConflict {
                expected: 1,
                actual: 6,
            }),
            ..
        }
    ));

    let cycle_3 = op
        .call(TaskUpdate {
            room: room.clone(),
            task: task_a.seq,
            expected_rev: Some(6),
            title: None,
            needs_add: vec![task_c.seq],
            needs_remove: vec![],
            priority: None,
            due_ms: None,
            clear_due: false,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(cycle_3, ApiError::Invalid(_)),
        "3-cycle A -> C -> B -> A rejected"
    );

    let self_cycle = op
        .call(TaskUpdate {
            room: room.clone(),
            task: task_b.seq,
            expected_rev: Some(1),
            title: None,
            needs_add: vec![task_b.seq],
            needs_remove: vec![],
            priority: None,
            due_ms: None,
            clear_due: false,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(self_cycle, ApiError::Invalid(_)));

    let updated = op
        .call(TaskUpdate {
            room: room.clone(),
            task: task_a.seq,
            expected_rev: Some(6),
            title: Some(rex_wave_api::board::Title::parse("Task A updated").unwrap()),
            needs_add: vec![],
            needs_remove: vec![],
            priority: Some(rex_wave_api::board::Priority::Blocking),
            due_ms: None,
            clear_due: true,
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(updated.rev, 7);
    assert_eq!(updated.title.as_str(), "Task A updated");
    assert_eq!(updated.priority, rex_wave_api::board::Priority::Blocking);
    assert_eq!(updated.due_ms, None);
}

#[test]
fn pure_clock_lease_expiry_and_readiness_evaluation() {
    use rex_wave_rooms::RoomState;
    use rex_wave_runtime::{DecideCtx, Decider, EntryMeta};

    let room = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("clock").unwrap(),
    };
    let w1 = worker("w", "one");
    let w2 = worker("w", "two");
    let mut state = RoomState::default();
    let mut seq = Seq::ORIGIN;

    let apply =
        |state: &mut RoomState, seq: &mut Seq, by: &Principal, at: UnixMs, evs: Vec<RoomEvent>| {
            for ev in evs {
                *seq = seq.next();
                Rooms::evolve(
                    state,
                    &ev,
                    &EntryMeta {
                        seq: *seq,
                        at,
                        by: by.clone(),
                    },
                );
            }
        };

    let post_evs = Rooms::decide(
        &state,
        &RoomCommand::RoomPost(RoomPost {
            task: Some(TaskSpec {
                title: rex_wave_api::board::Title::parse("Clocked task").unwrap(),
                org_id: None,
                needs: vec![],
                priority: None,
                due_ms: None,
            }),
            ..RoomPost::new(room.clone(), Body::parse("clock body").unwrap())
        }),
        &DecideCtx {
            by: w1.clone(),
            at: UnixMs(1_000),
        },
    )
    .unwrap();
    apply(&mut state, &mut seq, &w1, UnixMs(1_000), post_evs);
    let task_seq = seq;

    let claim_evs = Rooms::decide(
        &state,
        &RoomCommand::TaskClaim(TaskClaim {
            room: room.clone(),
            task: task_seq,
            ttl_ms: Some(500),
            expected_rev: Some(1),
            force: false,
            idem: None,
        }),
        &DecideCtx {
            by: w1.clone(),
            at: UnixMs(1_000),
        },
    )
    .unwrap();
    apply(&mut state, &mut seq, &w1, UnixMs(1_000), claim_evs);

    let before_expiry = state.task_view_at(task_seq, UnixMs(1_499)).unwrap();
    assert!(!before_expiry.lease_expired);
    assert!(!before_expiry.ready);

    let at_expiry = state.task_view_at(task_seq, UnixMs(1_500)).unwrap();
    assert!(at_expiry.lease_expired);
    assert!(at_expiry.ready);

    let ready_list = state
        .task_list_at(
            Some(&TaskFilter {
                ready_only: true,
                ..Default::default()
            }),
            UnixMs(1_500),
        )
        .unwrap();
    assert_eq!(ready_list.tasks.len(), 1);

    let held_list = state
        .task_list_at(
            Some(&TaskFilter {
                holder: Some(w1.clone()),
                ..Default::default()
            }),
            UnixMs(1_500),
        )
        .unwrap();
    assert!(
        held_list.tasks.is_empty(),
        "expired lease is no longer actively held by w1"
    );

    let early_takeover = Rooms::decide(
        &state,
        &RoomCommand::TaskClaim(TaskClaim {
            room: room.clone(),
            task: task_seq,
            ttl_ms: Some(500),
            expected_rev: None,
            force: false,
            idem: None,
        }),
        &DecideCtx {
            by: w2.clone(),
            at: UnixMs(1_499),
        },
    )
    .unwrap_err();
    assert!(matches!(
        early_takeover,
        ApiError::Conflict {
            data: Some(ConflictData::LeaseFenced { .. }),
            ..
        }
    ));

    let takeover_evs = Rooms::decide(
        &state,
        &RoomCommand::TaskClaim(TaskClaim {
            room,
            task: task_seq,
            ttl_ms: Some(500),
            expected_rev: None,
            force: false,
            idem: None,
        }),
        &DecideCtx {
            by: w2.clone(),
            at: UnixMs(1_500),
        },
    )
    .unwrap();
    apply(&mut state, &mut seq, &w2, UnixMs(1_500), takeover_evs);
    let after_takeover = state.task_view_at(task_seq, UnixMs(1_500)).unwrap();
    assert_eq!(after_takeover.lease.unwrap().epoch, LeaseEpoch(2));
}

#[tokio::test]
async fn room_post_keeps_raw_idem_keys_for_pre_upgrade_journal_dedupe() {
    let journal = Journal::in_memory().unwrap();
    let room_id = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("legacy-idem").unwrap(),
    };
    let stream = StreamId::Room {
        room: room_id.clone(),
    };
    let raw_key = IdemKey::parse("wave:auth-v2:brief:verifier").unwrap();
    {
        let mut writer = journal.claim(stream).unwrap();
        writer
            .append(
                &seat("sam"),
                UnixMs(1_000),
                Some(&raw_key),
                vec![
                    Event::Room(RoomEvent::Opened {
                        id: room_id.clone(),
                        origin: RoomOrigin::Manual,
                        topic: None,
                    }),
                    Event::Room(RoomEvent::Posted {
                        body: Body::parse("pre-upgrade brief").unwrap(),
                        mentions: Mentions::default(),
                        re: None,
                        after: None,
                        labels: vec![],
                        task: None,
                        idem: Some(raw_key.clone()),
                    }),
                ],
            )
            .unwrap();
    }

    let router = build_harness(journal, Duration::from_secs(60));
    let sam = LocalClient::new(router, seat("sam"));
    let retried = sam
        .call(RoomPost {
            idem: Some(raw_key),
            ..RoomPost::new(
                RoomRef::Id {
                    id: room_id.clone(),
                },
                Body::parse("duplicate retry").unwrap(),
            )
        })
        .await
        .unwrap();
    assert_eq!(
        retried.seq,
        Seq(2),
        "pre-upgrade raw RoomPost idem keys continue deduping after Phase 3"
    );

    let spoofed_claim_idem = IdemKey::parse("__rw_clm:r:K").unwrap();
    let task_post = sam
        .call(RoomPost {
            idem: Some(spoofed_claim_idem.clone()),
            task: Some(TaskSpec {
                title: rex_wave_api::board::Title::parse("Prefix collision guard").unwrap(),
                org_id: None,
                needs: vec![],
                priority: None,
                due_ms: None,
            }),
            ..RoomPost::new(
                RoomRef::Id {
                    id: room_id.clone(),
                },
                Body::parse("spoofed prefix post").unwrap(),
            )
        })
        .await
        .unwrap();
    let claim = sam
        .call(TaskClaim {
            room: RoomRef::Id {
                id: room_id.clone(),
            },
            task: task_post.seq,
            ttl_ms: Some(60_000),
            expected_rev: Some(1),
            force: false,
            idem: Some(IdemKey::parse("K").unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(claim.epoch, LeaseEpoch(1));

    let post_retry = sam
        .call(RoomPost {
            idem: Some(spoofed_claim_idem),
            ..RoomPost::new(
                RoomRef::Id {
                    id: room_id.clone(),
                },
                Body::parse("retry spoofed prefix").unwrap(),
            )
        })
        .await
        .unwrap();
    assert_eq!(post_retry.seq, task_post.seq);
}
