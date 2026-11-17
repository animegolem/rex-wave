use rex_wave_api::mail::*;
use rex_wave_api::read::Read;
use rex_wave_api::room::*;
use rex_wave_api::*;
use rex_wave_mailbridge::{
    BellCourier, MailBridge, SandboxError, SandboxRoot, bell_meta, project, render_letter,
    serve_wake_bell_listener, sha256_bytes, wake_for_letter, wake_id,
};
use rex_wave_rooms::{Rooms, room_history};
use rex_wave_runtime::{
    Hub, LocalClient, NoAgentFeed, ReadSide, Registry, Router, SidebarProjection, Unavailable,
    spawn_reactor,
};
use rex_wave_store::Journal;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

static DIR_COUNTER: AtomicU64 = AtomicU64::new(1);

struct TempSandbox {
    path: PathBuf,
}

impl TempSandbox {
    fn marked() -> (Self, SandboxRoot) {
        let dir = Self::unmarked();
        fs::write(dir.path.join(".rex-wave-sandbox"), b"rex-wave-test-sandbox\n").unwrap();
        let root = SandboxRoot::verify(&dir.path).unwrap();
        (dir, root)
    }

    fn unmarked() -> Self {
        let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("rex-wave-mailbridge-test-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        TempSandbox { path }
    }
}

impl Drop for TempSandbox {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[test]
fn sandbox_root_requires_marker_and_rejects_all_production_paths() {
    let unmarked = TempSandbox::unmarked();
    assert!(matches!(
        SandboxRoot::verify(&unmarked.path),
        Err(SandboxError::Unmarked(_))
    ));

    for forbidden in [
        "/Users/golem/agent-folder/interchange",
        "/Users/golem/agent-folder/interchange/to-owner",
        "/Users/golem/.local/state/carrier",
        "/Users/golem/.local/state/carrier/index.db",
        "/Users/golem/agent-folder/resident_memories",
        "/Users/golem/agent-folder/resident_memories/sol",
        "/Users/golem/agent-folder",
        "/Users/golem",
        "/",
    ] {
        let res = SandboxRoot::verify(Path::new(forbidden));
        assert!(
            matches!(res, Err(SandboxError::Production(_))),
            "expected Production rejection for {forbidden}, got {res:?}"
        );
    }

    #[cfg(unix)]
    {
        let holder = TempSandbox::unmarked();
        let link = holder.path.join("sneaky-link");
        std::os::unix::fs::symlink("/Users/golem/agent-folder/interchange", &link).unwrap();
        let res = SandboxRoot::verify(&link);
        assert!(
            matches!(res, Err(SandboxError::Production(_))),
            "symlink resolving into interchange must be rejected as Production: {res:?}"
        );
    }

    let (_guard, verified) = TempSandbox::marked();
    assert!(verified.path().is_absolute());
}

#[test]
fn schema_v1_wake_id_vector_projection_and_bell_meta() {
    let bytes = b"---\nauthor: tally\nto: sonnet\ntopic: vector\nround: 1\n---\n\nhello\n";
    let sha = sha256_bytes(bytes);
    assert_eq!(
        sha.as_str(),
        "d4eb1d5a086d8a07b20cb67ca85a9b28f3dde81825c006cb988b15152aa3d678"
    );
    let sonnet = SeatName::parse("sonnet").unwrap();
    let wid = wake_id(&sonnet, "to-sonnet", "vector-r1.md", &sha);
    assert_eq!(wid.0, "ce2dfdac6a18f680", "must match mailbox.item.schema.md test vector");

    let (_guard, sandbox) = TempSandbox::marked();
    let box_dir = sandbox.path().join("to-sonnet");
    fs::create_dir_all(&box_dir).unwrap();
    let file_path = box_dir.join("vector-r1.md");
    fs::write(&file_path, bytes).unwrap();

    let item = project(&sandbox, &file_path, bytes).unwrap();
    assert_eq!(item.id, "letter:to-sonnet/vector-r1.md");
    assert_eq!(item.from, "tally");
    assert_eq!(item.to, "sonnet");
    assert_eq!(item.kind, "letter");
    assert_eq!(item.state, "open");
    assert_eq!(item.ts_basis, "mtime");
    assert_eq!(item.topic.as_deref(), Some("vector"));
    assert_eq!(item.round, Some(1));
    assert_eq!(item.reference.store, "interchange");
    assert_eq!(item.reference.path, PathBuf::from("to-sonnet/vector-r1.md"));
    assert_eq!(item.reference.sha256, sha);

    let wake = wake_for_letter(&sonnet, "to-sonnet", "vector-r1.md", item, "2026-10-07T15:41:02Z");
    let meta = bell_meta(&wake);
    assert_eq!(meta.get("schema").map(String::as_str), Some("1"));
    assert_eq!(meta.get("event").map(String::as_str), Some("letter"));
    assert_eq!(meta.get("ts").map(String::as_str), Some("2026-10-07T15:41:02Z"));
    assert_eq!(meta.get("wake_id").map(String::as_str), Some("ce2dfdac6a18f680"));
    assert_eq!(meta.get("kind").map(String::as_str), Some("letter"));
    assert_eq!(meta.get("from").map(String::as_str), Some("tally"));
    assert_eq!(meta.get("box").map(String::as_str), Some("to-sonnet"));
    assert_eq!(meta.get("file").map(String::as_str), Some("vector-r1.md"));
    assert_eq!(meta.get("sha16").map(String::as_str), Some("d4eb1d5a086d8a07"));
    assert_eq!(meta.get("topic").map(String::as_str), Some("vector"));
    assert_eq!(meta.get("round").map(String::as_str), Some("1"));
}

#[tokio::test]
async fn promote_banks_before_announce_enforces_box_monotone_and_acks_exact_sha256() {
    let (_guard, sandbox) = TempSandbox::marked();
    let journal = Journal::open(&sandbox.path().join("journal.sqlite3")).unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let mail_service = Arc::new(MailBridge::with_hub(
        sandbox.clone(),
        journal.clone(),
        journal.claim(StreamId::Mail).unwrap(),
        hub.clone(),
    ));
    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Arc::new(Unavailable(Family::Inbox)),
        mail: mail_service.clone(),
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });

    let owner = LocalClient::new(
        router.clone(),
        Principal::Operator { seat: SeatName::parse("owner").unwrap() },
    );
    let sol = LocalClient::new(
        router.clone(),
        Principal::Seat { seat: SeatName::parse("sol").unwrap() },
    );
    let astra = LocalClient::new(
        router.clone(),
        Principal::Seat { seat: SeatName::parse("astra").unwrap() },
    );
    let nodal = LocalClient::new(
        router.clone(),
        Principal::Seat { seat: SeatName::parse("nodal").unwrap() },
    );

    let wave_room = RoomRef::Channel {
        scope: RoomScope::Project { project: BbProjectId::parse("p_123").unwrap() },
        name: ChannelName::parse("design-review").unwrap(),
    };
    let p1 = sol
        .call(RoomPost {
            room: wave_room.clone(),
            body: Body::parse("Proposal: adopt natural-key RoomId.").unwrap(),
            reply_to: None,
            idem: None,
        })
        .await
        .unwrap();
    let p2 = astra
        .call(RoomPost {
            room: wave_room.clone(),
            body: Body::parse("Ratified. Let's bank this ruling.").unwrap(),
            reply_to: Some(p1.seq),
            idem: None,
        })
        .await
        .unwrap();

    let promote_req = MailPromote {
        room: p1.room.clone(),
        range: SeqRange::new(p1.seq, p2.seq).unwrap(),
        to: vec![SeatName::parse("owner").unwrap()],
        subject: "Natural-key RoomId ruling".into(),
        topic: "roomid-ruling".into(),
        priority: LetterPriority::High,
    };

    let raw_entries = journal
        .read(
            &StreamId::Room { room: p1.room.clone() },
            Seq(p1.seq.0 - 1),
            (p2.seq.0 - p1.seq.0 + 1) as usize,
        )
        .unwrap();
    let (rendered_name, rendered_bytes) = render_letter(&promote_req, sol.me(), &raw_entries);
    assert_eq!(rendered_name, "roomid-ruling-r1.md");

    let letter = sol.call(promote_req.clone()).await.unwrap();
    assert_eq!(letter.path, PathBuf::from("to-owner/roomid-ruling-r1.md"));
    assert_eq!(letter.sha256, sha256_bytes(&rendered_bytes));

    let on_disk_path = sandbox.path().join(&letter.path);
    let on_disk_bytes = fs::read(&on_disk_path).unwrap();
    assert_eq!(on_disk_bytes, rendered_bytes, "on-disk bytes match canonical render_letter output");

    let retry = sol.call(promote_req).await.unwrap();
    assert_eq!(retry, letter, "idempotent promotion returns identical LetterRef");

    let mail_page = owner
        .call(Read { stream: StreamId::Mail, after: Seq::ORIGIN, limit: None })
        .await
        .unwrap();
    assert_eq!(mail_page.entries.len(), 1, "idempotent promotion commits exactly one Banked event");

    let conflict_err = sol
        .call(MailPromote {
            room: p1.room.clone(),
            range: SeqRange::new(p1.seq, p1.seq).unwrap(),
            to: vec![SeatName::parse("owner").unwrap()],
            subject: "Mutated ruling overwrite attempt".into(),
            topic: "roomid-ruling".into(),
            priority: LetterPriority::Normal,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(conflict_err, ApiError::Conflict { .. }),
        "BoxMonotone rejects overwriting an existing letter with different bytes"
    );
    assert_eq!(fs::read(&on_disk_path).unwrap(), rendered_bytes, "existing file bytes never overwritten");
    assert_eq!(
        journal.head(&StreamId::Mail).unwrap(),
        Seq(1),
        "Law I2: failed disk write never appends a Banked journal event"
    );

    sol.call(RoomPost {
        room: RoomRef::Dm { with: Principal::Seat { seat: SeatName::parse("astra").unwrap() } },
        body: Body::parse("private dm note").unwrap(),
        reply_to: None,
        idem: None,
    })
    .await
    .unwrap();
    let dm_id = RoomRef::Dm { with: Principal::Seat { seat: SeatName::parse("astra").unwrap() } }
        .resolve(sol.me())
        .unwrap();
    let dm_exfil = nodal
        .call(MailPromote {
            room: dm_id,
            range: SeqRange::new(Seq(1), Seq(4)).unwrap(),
            to: vec![SeatName::parse("nodal").unwrap()],
            subject: "leak".into(),
            topic: "leak".into(),
            priority: LetterPriority::Low,
        })
        .await
        .unwrap_err();
    assert!(matches!(dm_exfil, ApiError::Denied { .. }));

    let wrong_sha = Sha256Hex::parse("0".repeat(64)).unwrap();
    let bad_ack = owner
        .call(MailAck { path: letter.path.clone(), sha256: wrong_sha })
        .await
        .unwrap_err();
    assert!(matches!(bad_ack, ApiError::Conflict { .. }));
    assert!(!sandbox.path().join("state/acks.jsonl").exists());

    let wrong_seat_ack = nodal
        .call(MailAck { path: letter.path.clone(), sha256: letter.sha256.clone() })
        .await
        .unwrap_err();
    assert!(matches!(wrong_seat_ack, ApiError::Denied { .. }));

    let traversal_ack = owner
        .call(MailAck {
            path: PathBuf::from("../outside.md"),
            sha256: letter.sha256.clone(),
        })
        .await
        .unwrap_err();
    assert!(matches!(traversal_ack, ApiError::Invalid(_) | ApiError::Denied { .. }));

    owner
        .call(MailAck { path: letter.path.clone(), sha256: letter.sha256.clone() })
        .await
        .unwrap();
    owner
        .call(MailAck { path: letter.path.clone(), sha256: letter.sha256.clone() })
        .await
        .unwrap();

    let sidecar = PathBuf::from(format!("{}.ack.json", on_disk_path.display()));
    assert!(sidecar.is_file(), ".ack.json receipt written beside banked letter");
    assert!(sandbox.path().join("state/acks.jsonl").is_file(), "acks.jsonl updated");

    let reprojected = project(&sandbox, &on_disk_path, &on_disk_bytes).unwrap();
    assert_eq!(reprojected.state, "acked");

    let drop_dir = sandbox.path().join("to-astra");
    fs::create_dir_all(&drop_dir).unwrap();
    fs::write(
        drop_dir.join("external-r1.md"),
        b"---\nfrom: tally\nto: astra\nsubject: External drop\ntopic: external\nround: 1\n---\n\nbody\n",
    )
    .unwrap();
    assert_eq!(mail_service.rescan().await.unwrap(), 1);
    assert_eq!(mail_service.rescan().await.unwrap(), 0, "rescan deduplicates already-projected letters");
}

#[tokio::test]
async fn wake_bell_deduplicates_schedule_mark_and_bell_courier_delivers_seat_dm() {
    let (_guard, sandbox) = TempSandbox::marked();
    let journal = Journal::open(&sandbox.path().join("bell.sqlite3")).unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let mail_service = Arc::new(MailBridge::with_hub(
        sandbox,
        journal.clone(),
        journal.claim(StreamId::Mail).unwrap(),
        hub.clone(),
    ));
    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Arc::new(Unavailable(Family::Inbox)),
        mail: mail_service,
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });

    let bell_client = LocalClient::new(
        router.clone(),
        Principal::System { part: SystemPart::WakeBell },
    );
    let courier_task = spawn_reactor(BellCourier, bell_client.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sink_addr = listener.local_addr().unwrap();
    let sink_task = tokio::spawn(serve_wake_bell_listener(listener, bell_client));

    async fn post_sink(addr: std::net::SocketAddr, body: &serde_json::Value) -> (u16, serde_json::Value) {
        let payload = serde_json::to_vec(body).unwrap();
        let req = format!(
            "POST /sink HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            payload.len()
        );
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(req.as_bytes()).await.unwrap();
        stream.write_all(&payload).await.unwrap();
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.unwrap();
        let text = String::from_utf8(resp).unwrap();
        let (head, json_body) = text.split_once("\r\n\r\n").unwrap();
        let code: u16 = head
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        (code, serde_json::from_str(json_body).unwrap())
    }

    let slot1 = serde_json::json!({
        "bell_id": "morning-brief",
        "seat": "sol",
        "prompt": "Summarize overnight PR queue and open blockers.",
        "fired_at": "2026-11-16T09:00",
    });

    for _attempt in 0..3 {
        let (status, body) = post_sink(sink_addr, &slot1).await;
        assert_eq!(status, 200);
        assert_eq!(body["ok"], true);
    }

    let sol = LocalClient::new(
        router.clone(),
        Principal::Seat { seat: SeatName::parse("sol").unwrap() },
    );
    let dm_with_bell = RoomRef::Dm {
        with: Principal::System { part: SystemPart::WakeBell },
    };

    let first_delivery = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let msgs = room_history(&sol, dm_with_bell.clone(), Seq::ORIGIN, Some(10))
                .await
                .unwrap();
            if !msgs.is_empty() {
                break msgs;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("BellCourier delivered wake prompt to sol DM room");

    tokio::time::sleep(Duration::from_millis(40)).await;
    let after_retries = room_history(&sol, dm_with_bell.clone(), Seq::ORIGIN, Some(10))
        .await
        .unwrap();
    assert_eq!(
        after_retries.len(),
        1,
        "three retries of the same (schedule_id, mark) collapse to a single DM post"
    );
    assert_eq!(
        first_delivery[0].body.as_str(),
        "Summarize overnight PR queue and open blockers."
    );
    assert_eq!(journal.head(&StreamId::Mail).unwrap(), Seq(1));

    let slot2 = serde_json::json!({
        "bell_id": "morning-brief",
        "seat": "sol",
        "prompt": "Summarize Tuesday PR queue.",
        "fired_at": "2026-11-17T09:00",
    });
    let (status2, _) = post_sink(sink_addr, &slot2).await;
    assert_eq!(status2, 200);

    let second_delivery = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let msgs = room_history(&sol, dm_with_bell.clone(), Seq::ORIGIN, Some(10))
                .await
                .unwrap();
            if msgs.len() == 2 {
                break msgs;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("next schedule mark delivered a second DM post");
    assert_eq!(second_delivery[1].body.as_str(), "Summarize Tuesday PR queue.");
    assert_eq!(journal.head(&StreamId::Mail).unwrap(), Seq(2));

    courier_task.abort();
    sink_task.abort();
}
