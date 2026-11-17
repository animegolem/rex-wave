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
    let sol = LocalClient::new(router.clone(), seat("sol"));
    let astra = LocalClient::new(router.clone(), seat("astra"));
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
    let g1 = sol
        .call(RoomEnsure {
            room: global_ref.clone(),
            topic: Some("Estate town hall".into()),
            origin: None,
        })
        .await
        .unwrap();
    let g2 = astra
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
    assert_eq!(g2.topic.as_deref(), Some("Estate town hall"));

    let proj = BbProjectId::parse("proj_3vs7nkyeum").unwrap();
    let proj_chan = RoomRef::Channel {
        scope: RoomScope::Project {
            project: proj.clone(),
        },
        name: ChannelName::parse("design").unwrap(),
    };
    let p1 = sol
        .call(RoomEnsure {
            room: proj_chan.clone(),
            topic: None,
            origin: None,
        })
        .await
        .unwrap();
    let p2 = sol
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
    let denied = sol
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

    let squat_denied = sol
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
    let w2 = sol
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
            reply_to: None,
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
            reply_to: None,
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(
        op_post.mentions.principals,
        vec![worker("auth-v2", "verifier")]
    );

    let dm_from_sol = sol
        .call(RoomEnsure {
            room: RoomRef::Dm {
                with: seat("astra"),
            },
            topic: None,
            origin: None,
        })
        .await
        .unwrap();
    let dm_from_astra = astra
        .call(RoomEnsure {
            room: RoomRef::Dm { with: seat("sol") },
            topic: None,
            origin: None,
        })
        .await
        .unwrap();
    assert_eq!(
        dm_from_sol.id, dm_from_astra.id,
        "DM natural key normalizes pair order"
    );
    assert_eq!(dm_from_sol.head, dm_from_astra.head);
    assert_eq!(dm_from_sol.members.len(), 2);

    let self_dm = sol
        .call(RoomEnsure {
            room: RoomRef::Dm { with: seat("sol") },
            topic: None,
            origin: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(self_dm, ApiError::Invalid(_)));

    let dm_leave = sol
        .call(RoomLeave {
            room: RoomRef::Dm {
                with: seat("astra"),
            },
        })
        .await
        .unwrap_err();
    assert!(matches!(dm_leave, ApiError::Conflict { .. }));

    let outsider_dm = nodal
        .call(RoomPost {
            room: RoomRef::Id { id: dm_from_sol.id },
            body: Body::parse("intrude").unwrap(),
            reply_to: None,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(outsider_dm, ApiError::Denied { .. }));
}

#[test]
fn resolve_mentions_parses_handles_qualified_roles_all_and_ignores_emails() {
    let mut members = BTreeMap::new();
    let sol = seat("sol");
    let verifier = worker("auth-v2", "verifier");
    let author = worker("auth-v2", "author-jwt");
    members.insert(Handle::parse("sol").unwrap(), sol.clone());
    members.insert(Handle::parse("verifier").unwrap(), verifier.clone());
    members.insert(Handle::parse("author-jwt").unwrap(), author.clone());

    let body = Body::parse(
        "Ping (@verifier, @auth-v2/author-jwt!) and email ops@sol.dev or @missing; cc @sol.",
    )
    .unwrap();
    let m = resolve_mentions(&body, &members);
    assert!(!m.all);
    assert_eq!(m.principals.len(), 3);
    assert!(m.principals.contains(&sol));
    assert!(m.principals.contains(&verifier));
    assert!(m.principals.contains(&author));

    let all_body = Body::parse("Heads up @all!").unwrap();
    let m_all = resolve_mentions(&all_body, &members);
    assert!(m_all.all);
    assert_eq!(m_all.principals.len(), 3);
}

#[tokio::test]
async fn join_leave_handle_uniqueness_and_room_history_across_passivation() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(&dir.path().join("history.sqlite3")).unwrap();
    let router = build_harness(journal, Duration::from_millis(25));

    let sol = LocalClient::new(router.clone(), seat("sol"));
    let astra = LocalClient::new(router.clone(), seat("astra"));
    let nodal = LocalClient::new(router.clone(), seat("nodal"));
    let w1_ver = LocalClient::new(router.clone(), worker("w1", "verifier"));
    let w2_ver = LocalClient::new(router.clone(), worker("w2", "verifier"));

    let room = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("ops").unwrap(),
    };

    sol.call(RoomEnsure {
        room: room.clone(),
        topic: None,
        origin: None,
    })
    .await
    .unwrap();
    sol.call(RoomJoin {
        room: room.clone(),
        member: Some(seat("astra")),
        handle: None,
    })
    .await
    .unwrap();

    let unauthorized_invite = astra
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
            handle: Some(Handle::parse("astra").unwrap()),
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
    let info = sol
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

    let first = sol
        .call(RoomPost {
            room: room.clone(),
            body: Body::parse("hello @astra").unwrap(),
            reply_to: None,
            idem: Some(IdemKey::parse("post-1").unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(first.mentions.principals, vec![seat("astra")]);

    let bad_reply = astra
        .call(RoomPost {
            room: room.clone(),
            body: Body::parse("bad reply target").unwrap(),
            reply_to: Some(Seq(1)),
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(bad_reply, ApiError::NotFound { .. }),
        "Opened seq cannot be a reply_to target"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;

    let replayed = sol
        .call(RoomPost {
            room: room.clone(),
            body: Body::parse("different text").unwrap(),
            reply_to: None,
            idem: Some(IdemKey::parse("post-1").unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(
        replayed.seq, first.seq,
        "idempotent retry after passivation returns original seq"
    );

    let second = astra
        .call(RoomPost {
            room: room.clone(),
            body: Body::parse("ack @sol").unwrap(),
            reply_to: Some(first.seq),
            idem: None,
        })
        .await
        .unwrap();

    astra.call(RoomLeave { room: room.clone() }).await.unwrap();
    let leave_twice = astra
        .call(RoomLeave { room: room.clone() })
        .await
        .unwrap_err();
    assert!(matches!(leave_twice, ApiError::Conflict { .. }));

    let history = room_history(&sol, room.clone(), Seq::ORIGIN, Some(10))
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].seq, first.seq);
    assert_eq!(history[0].body.as_str(), "hello @astra");
    assert_eq!(history[1].seq, second.seq);
    assert_eq!(history[1].reply_to, Some(first.seq));

    let raw = sol
        .call(Read {
            stream: StreamId::Room {
                room: room.resolve(sol.me()).unwrap(),
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

    let sol = LocalClient::new(router.clone(), seat("sol"));
    let astra = LocalClient::new(router.clone(), seat("astra"));
    let verifier = LocalClient::new(router.clone(), worker("auth-v2", "verifier"));

    let chan = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("coord").unwrap(),
    };
    let room_id = chan.resolve(sol.me()).unwrap();

    sol.call(RoomEnsure {
        room: chan.clone(),
        topic: None,
        origin: None,
    })
    .await
    .unwrap();
    astra
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
            sol.clone()
        } else {
            astra.clone()
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
                        reply_to: None,
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

    let pre_mention_noise = sol
        .call(RoomPost {
            room: chan.clone(),
            body: Body::parse("background chatter 1").unwrap(),
            reply_to: None,
            idem: None,
        })
        .await
        .unwrap();
    let mention_hit = sol
        .call(RoomPost {
            room: chan.clone(),
            body: Body::parse("please check @verifier").unwrap(),
            reply_to: None,
            idem: None,
        })
        .await
        .unwrap();
    let all_hit = astra
        .call(RoomPost {
            room: chan.clone(),
            body: Body::parse("@all ship gate ready").unwrap(),
            reply_to: None,
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
    let dm_post = sol
        .call(RoomPost {
            room: RoomRef::Dm {
                with: worker("auth-v2", "verifier"),
            },
            body: Body::parse("direct ping").unwrap(),
            reply_to: None,
            idem: None,
        })
        .await
        .unwrap();
    let woken = dm_wait.await.unwrap();
    assert!(!woken.timed_out);
    assert_eq!(woken.entries.len(), 1);
    assert_eq!(woken.entries[0].seq, dm_post.seq);
}
