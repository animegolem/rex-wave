//! Integration tests for dissolved content-addressed (`sha256`) mail,
//! planted-crash startup `reconcile`, drop-folder ingest (with Emacs `.#*`
//! lockfile and symlink safety), `EditNeverRingable`, DM/channel projection
//! in `SidebarProjection`, SQLite FTS5 search, lossless offline `rebuild`,
//! and `protected-roots` enforcement.

#![allow(clippy::disallowed_methods)]

use rex_wave_api::mail::*;
use rex_wave_api::presence::*;
use rex_wave_api::room::{PrincipalPair, RoomId, RoomScope};
use rex_wave_api::*;
use rex_wave_mailbridge::{
    AliasMap, MailBridge, MailState, Presence, Protection, SandboxError, SandboxRoot,
    migrate_verify_interchange, rebuild_mail_from_mirror,
};
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    DecideCtx, Hub, LocalClient, NoAgentFeed, ReadSide, Registry, Router, Service,
    SidebarProjection,
};
use rex_wave_store::Journal;
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::symlink;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

fn sha256_of(bytes: &[u8]) -> Sha256Hex {
    let mut h = Sha256::new();
    h.update(bytes);
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    Sha256Hex::parse(hex).expect("sha256")
}

fn make_sandbox(dir: &Path, protection: &Protection) -> SandboxRoot {
    fs::create_dir_all(dir).expect("mkdir sandbox");
    fs::write(dir.join(".rex-wave-sandbox"), b"test-sandbox\n").expect("marker");
    SandboxRoot::verify_with(dir, protection).expect("verify sandbox")
}

/// Mandatory deterministic planted-crash test:
/// Commits `mail send` and `mail ack` durably to `journal.sqlite3` while export
/// is paused (simulating a crash right after SQLite commit and before mirror
/// export), verifies no mirror files or `acks.jsonl` exist yet, boots a fresh
/// `MailBridge`, runs startup `reconcile`, and asserts `<mirror>/objects/<sha>.md`,
/// `<mirror>/mail/<slug>.md`, and `<mirror>/state/acks.jsonl` are populated
/// automatically with exact `sha256` parity.
#[tokio::test]
async fn planted_crash_before_export_recovers_on_startup_reconcile() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mirror_dir = tmp.path().join("mirror");
    let journal_path = tmp.path().join("journal.sqlite3");
    let protection = Protection::default();
    let sandbox = make_sandbox(&mirror_dir, &protection);

    let alice = SeatName::parse("alice").expect("alice");
    let bob = SeatName::parse("bob").expect("bob");

    // 1. First incarnation: pause export to plant a crash right after SQLite commits.
    let (sent_sha, sent_path) = {
        let journal = Journal::open(&journal_path).expect("open journal");
        let writer = journal.claim(StreamId::Mail).expect("claim mail");
        let bridge = MailBridge::new(sandbox.clone(), journal, writer);
        bridge.set_export_enabled(false);

        let WireReply::MailSend(sent) = bridge
            .dispatch(
                MailCommand::MailSend(MailSend {
                    to: vec![bob.clone()],
                    cc: Vec::new(),
                    subject: "Crash recovery test".into(),
                    body: "Committed before crash, exported after restart.".into(),
                    from: Some(alice.clone()),
                    priority: LetterPriority::High,
                    topic: Some("crash-gate".into()),
                    in_reply_to: None,
                    refs: Vec::new(),
                    wait_export: false,
                    idem: Some(IdemKey::parse("crash-send-1").expect("idem")),
                }),
                DecideCtx::for_test(
                    Principal::Seat {
                        seat: alice.clone(),
                    },
                    UnixMs(1_795_500_000_000),
                ),
            )
            .await
            .expect("send commits to SQLite even with export disabled")
        else {
            unreachable!("MailSend reply");
        };
        assert!(
            !sent.exported,
            "export must be skipped during planted crash"
        );

        let WireReply::MailAck(acked) = bridge
            .dispatch(
                MailCommand::MailAck(MailAck {
                    path: None,
                    sha256: Some(sent.sha256.clone()),
                    seat: Some(bob.clone()),
                    note: Some("acked before crash".into()),
                }),
                DecideCtx::for_test(
                    Principal::Seat { seat: bob.clone() },
                    UnixMs(1_795_500_005_000),
                ),
            )
            .await
            .expect("ack commits to SQLite even with export disabled")
        else {
            unreachable!("MailAck reply");
        };
        assert!(!acked.exported, "ack export skipped during planted crash");

        (sent.sha256, sent.path)
    };

    // Assert NO mirror files or acks.jsonl exist before restart reconcile!
    let blob_path = mirror_dir
        .join("objects")
        .join(format!("{}.md", sent_sha.as_str()));
    let working_path = mirror_dir.join(&sent_path);
    let acks_path = mirror_dir.join("state/acks.jsonl");
    assert!(!blob_path.exists(), "blob must not exist before reconcile");
    assert!(
        !working_path.exists(),
        "working-tree letter must not exist before reconcile"
    );
    assert!(
        !acks_path.exists(),
        "acks.jsonl must not exist before reconcile"
    );

    // 2. Restart fresh MailBridge over the same journal.sqlite3 and run startup reconcile!
    let journal2 = Journal::open(&journal_path).expect("reopen journal");
    let writer2 = journal2.claim(StreamId::Mail).expect("claim mail 2");
    let bridge2 = MailBridge::new(sandbox.clone(), journal2, writer2);
    let report = bridge2.reconcile().await.expect("startup reconcile");

    assert_eq!(report.blobs_exported, 1);
    assert_eq!(report.working_tree_exported, 1);
    assert_eq!(report.acks_exported, 1);

    assert!(blob_path.is_file(), "blob exported on startup reconcile");
    assert!(
        working_path.is_file(),
        "working-tree .md exported on startup reconcile"
    );
    assert!(
        acks_path.is_file(),
        "acks.jsonl exported on startup reconcile"
    );

    let blob_bytes = fs::read(&blob_path).expect("read blob");
    let wt_bytes = fs::read(&working_path).expect("read working tree");
    assert_eq!(sha256_of(&blob_bytes), sent_sha, "exact blob sha256 parity");
    assert_eq!(
        sha256_of(&wt_bytes),
        sent_sha,
        "exact working-tree sha256 parity"
    );
    let acks_text = fs::read_to_string(&acks_path).expect("read acks");
    assert!(
        acks_text.contains(sent_sha.as_str()) && acks_text.contains("bob"),
        "acks.jsonl contains exact sha256 and seat: {acks_text}"
    );
}

#[tokio::test]
async fn dissolved_mail_projects_1to1_dms_to_sidebar_multi_seat_channels_and_fts5_search() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mirror_dir = tmp.path().join("mirror");
    let journal_path = tmp.path().join("journal.sqlite3");
    let protection = Protection::default();
    let sandbox = make_sandbox(&mirror_dir, &protection);

    let journal = Journal::open(&journal_path).expect("open journal");
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let room_svc = Registry::<Rooms>::new(journal.clone(), hub.clone());
    let presence_svc = Presence::default().registry(journal.clone(), hub.clone());
    let mail_writer = journal.claim(StreamId::Mail).expect("claim mail");
    let mail_svc = Arc::new(
        MailBridge::with_hub(sandbox, journal.clone(), mail_writer, hub.clone())
            .with_room_service(room_svc.clone()),
    );
    let router = Arc::new(Router {
        room: room_svc,
        mail: mail_svc,
        presence: presence_svc,
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar: sidebar.clone(),
            agents: Arc::new(NoAgentFeed),
        }))
    });

    let alice = SeatName::parse("alice").expect("alice");
    let bob = SeatName::parse("bob").expect("bob");
    let sam = SeatName::parse("sam").expect("sam");
    let owner = SeatName::parse("owner").expect("owner");

    let alice_client = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: alice.clone(),
        },
    );
    let bob_client = LocalClient::new(router.clone(), Principal::Seat { seat: bob.clone() });
    let sam_client = LocalClient::new(router.clone(), Principal::Seat { seat: sam.clone() });
    let owner_client = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: owner.clone(),
        },
    );

    // Claim bob as Dormant so sender receives a delivery notice on send & check.
    owner_client
        .call(PresenceClaim {
            seat: bob.clone(),
            status: Some(SeatStatus::Dormant),
            role: None,
            projects: Vec::new(),
            aliases: Vec::new(),
            home_room: None,
            mail_horizon_at: None,
            work_class: None,
            avatar: None,
            home_dir: None,
            start_cmd: None,
            resume_cmd: None,
            rex_target: None,
            verified_how: None,
            replace: None,
            idem: None,
        })
        .await
        .expect("claim dormant bob");

    // 1. Send 1:1 mail alice -> bob (with idem key) and verify replay returns identical sha256.
    let first = alice_client
        .call(MailSend {
            to: vec![bob.clone()],
            cc: Vec::new(),
            subject: "EdDSA rotation signoff".into(),
            body: "Please ratify the 15-minute refresh token rotation policy.".into(),
            from: None,
            priority: LetterPriority::High,
            topic: Some("eddsa-rotation".into()),
            in_reply_to: None,
            refs: vec!["RFC-42".into()],
            wait_export: true,
            idem: Some(IdemKey::parse("eddsa-send-1").expect("idem")),
        })
        .await
        .expect("send 1:1 mail");
    assert!(first.exported);
    assert!(!first.replayed);
    assert!(matches!(first.room, Some(RoomId::Direct { .. })));
    assert!(first.room_seq.is_some());
    assert!(
        first
            .delivery_notices
            .iter()
            .any(|n| n.seat == bob && n.status == "dormant"),
        "expected dormant delivery notice for bob, got {:?}",
        first.delivery_notices
    );

    let replay = alice_client
        .call(MailSend {
            to: vec![bob.clone()],
            cc: Vec::new(),
            subject: "EdDSA rotation signoff".into(),
            body: "Please ratify the 15-minute refresh token rotation policy.".into(),
            from: None,
            priority: LetterPriority::High,
            topic: Some("eddsa-rotation".into()),
            in_reply_to: None,
            refs: vec!["RFC-42".into()],
            wait_export: true,
            idem: Some(IdemKey::parse("eddsa-send-1").expect("idem")),
        })
        .await
        .expect("replayed send");
    assert!(replay.replayed);
    assert_eq!(replay.sha256, first.sha256);

    // 2. Reply bob -> alice with in_reply_to = first.sha256 -> threaded `re` in the SAME Direct room!
    let reply_msg = bob_client
        .call(MailSend {
            to: vec![alice.clone()],
            cc: Vec::new(),
            subject: "Re: EdDSA rotation signoff".into(),
            body: "Ratified! Ship it.".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("eddsa-rotation".into()),
            in_reply_to: Some(first.sha256.as_str().to_owned()),
            refs: Vec::new(),
            wait_export: true,
            idem: None,
        })
        .await
        .expect("reply 1:1 mail");
    assert_eq!(reply_msg.room, first.room);

    let expected_dm = RoomId::Direct {
        pair: PrincipalPair::new(
            Principal::Seat {
                seat: alice.clone(),
            },
            Principal::Seat { seat: bob.clone() },
        )
        .expect("pair"),
    };
    assert_eq!(first.room.as_ref(), Some(&expected_dm));

    // Wait for SidebarProjection to fold Hub room events and assert 1:1 DM is under Direct Messages!
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snap = sidebar.snapshot(&Principal::Seat { seat: bob.clone() });
            if snap.direct_messages.iter().any(|dm| dm.room == expected_dm) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("1:1 dissolved mail appears directly under Direct Messages in SidebarProjection");

    // 3. Multi-seat mail (alice -> bob + sam) projects into #mail-arch-sync channel.
    let group = alice_client
        .call(MailSend {
            to: vec![bob.clone(), sam.clone()],
            cc: Vec::new(),
            subject: "Quarterly architecture sync".into(),
            body: "Agenda: immutable storage, FTS5 search, and mirror rebuild.".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("arch-sync".into()),
            in_reply_to: None,
            refs: Vec::new(),
            wait_export: true,
            idem: None,
        })
        .await
        .expect("send multi-seat mail");
    assert_eq!(
        group.room,
        Some(RoomId::Channel {
            scope: RoomScope::Global,
            name: ChannelName::parse("mail-arch-sync").expect("chan"),
        })
    );

    // 4. Principal-scoped FTS5 search: sam sees group mail, NEVER sees alice<->bob 1:1 DM!
    let sam_hits = sam_client
        .call(MailSearch {
            query: "EdDSA".into(),
            seat: None,
            limit: Some(10),
        })
        .await
        .expect("sam search");
    assert!(
        sam_hits.hits.is_empty(),
        "sam must not see alice<->bob private mail in FTS5 search"
    );
    let sam_group_hits = sam_client
        .call(MailSearch {
            query: "immutable storage".into(),
            seat: None,
            limit: Some(10),
        })
        .await
        .expect("sam group search");
    assert_eq!(sam_group_hits.hits.len(), 1);
    assert_eq!(sam_group_hits.hits[0].sha256, group.sha256);

    let bob_dm_hits = bob_client
        .call(MailSearch {
            query: "EdDSA".into(),
            seat: None,
            limit: Some(10),
        })
        .await
        .expect("bob search");
    assert_eq!(bob_dm_hits.hits.len(), 2);

    // Non-Operator seat cannot check or search another seat's mailbox.
    assert!(matches!(
        sam_client
            .call(MailCheck {
                seat: Some(bob.clone()),
                include_all: false,
                limit: None,
            })
            .await,
        Err(ApiError::Denied { .. })
    ));
    assert!(matches!(
        sam_client
            .call(MailSearch {
                query: "EdDSA".into(),
                seat: Some(bob.clone()),
                limit: None,
            })
            .await,
        Err(ApiError::Denied { .. })
    ));

    // Alice's `mail check` includes delivery notice for dormant bob while unacked,
    // and clears after bob acks!
    let alice_check = alice_client
        .call(MailCheck {
            seat: None,
            include_all: false,
            limit: None,
        })
        .await
        .expect("alice check");
    assert!(
        alice_check
            .delivery_notices
            .iter()
            .any(|n| n.seat == bob && n.status == "dormant")
    );
    assert!(
        alice_check
            .delivery_notices
            .iter()
            .any(|n| n.seat == sam && n.status == "unregistered")
    );

    // Bob checks, acks by sha256, and re-checks idempotently.
    let bob_check = bob_client
        .call(MailCheck {
            seat: None,
            include_all: false,
            limit: None,
        })
        .await
        .expect("bob check");
    assert_eq!(bob_check.unread_count, 2);

    let ack1 = bob_client
        .call(MailAck {
            path: None,
            sha256: Some(first.sha256.clone()),
            seat: None,
            note: Some("read".into()),
        })
        .await
        .expect("bob ack 1");
    assert!(!ack1.already_acked);
    assert!(ack1.exported);

    let ack1_dup = bob_client
        .call(MailAck {
            path: None,
            sha256: Some(first.sha256.clone()),
            seat: None,
            note: None,
        })
        .await
        .expect("bob ack 1 idempotent");
    assert!(ack1_dup.already_acked);

    // Sam cannot ack alice->bob DM!
    assert!(matches!(
        sam_client
            .call(MailAck {
                path: None,
                sha256: Some(first.sha256.clone()),
                seat: None,
                note: None,
            })
            .await,
        Err(ApiError::Denied { .. })
    ));
}

#[tokio::test]
async fn drop_folder_emacs_lock_symlink_post_ack_edit_herald_and_lossless_rebuild() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mirror_dir = tmp.path().join("mirror");
    let drop_dir = tmp.path().join("drop");
    let journal_path = tmp.path().join("journal.sqlite3");
    let protection = Protection::default();
    let sandbox = make_sandbox(&mirror_dir, &protection);
    fs::create_dir_all(&drop_dir).expect("mkdir drop");

    let alice = SeatName::parse("alice").expect("alice");
    let bob = SeatName::parse("bob").expect("bob");

    // Drop 3 files while daemon is DOWN:
    // 1) Normal dropped letter
    let drop_normal_bytes =
        b"---\nid: 1795501000000-alice-to-bob-dropped\nfrom: alice\nto: bob\nsubject: Offline drop\n---\n\nDropped while offline.\n";
    let drop_normal_sha = sha256_of(drop_normal_bytes);
    fs::write(
        drop_dir.join("1795501000000-alice-to-bob-dropped.md"),
        drop_normal_bytes,
    )
    .expect("write drop normal");

    // 2) Emacs-locked file (.#locked.md symlink + locked.md) -> MUST be skipped until unlocked!
    let locked_bytes =
        b"---\nid: 1795501000100-alice-to-bob-locked\nfrom: alice\nto: bob\nsubject: Locked draft\n---\n\nStill editing in Emacs.\n";
    let locked_sha = sha256_of(locked_bytes);
    fs::write(drop_dir.join("locked.md"), locked_bytes).expect("write locked");
    symlink("user@host.1234", drop_dir.join(".#locked.md")).expect("write emacs lock symlink");

    // 3) Symlink .md file -> MUST be ignored!
    let outside_secret = tmp.path().join("outside.md");
    fs::write(
        &outside_secret,
        b"---\nfrom: alice\nto: bob\nsubject: Outside\n---\nSecret\n",
    )
    .expect("write outside");
    symlink(&outside_secret, drop_dir.join("symlinked.md")).expect("symlink md");

    // Boot MailBridge and run startup reconcile.
    let journal = Journal::open(&journal_path).expect("open journal");
    let hub = Hub::new(256);
    let room_svc = Registry::<Rooms>::new(journal.clone(), hub.clone());
    let mail_writer = journal.claim(StreamId::Mail).expect("claim mail");
    let mail_svc = Arc::new(
        MailBridge::with_hub(sandbox.clone(), journal.clone(), mail_writer, hub.clone())
            .with_room_service(room_svc.clone())
            .with_drop_dir(Some(drop_dir.clone())),
    );
    let router = Arc::new(Router {
        room: room_svc,
        mail: mail_svc.clone(),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar: SidebarProjection::spawn(journal.clone(), hub.clone()),
            agents: Arc::new(NoAgentFeed),
        }))
    });

    let rep1 = mail_svc.reconcile().await.expect("reconcile 1");
    assert_eq!(
        rep1.files_ingested, 1,
        "only unlocked regular drop ingested"
    );
    assert_eq!(rep1.drop_files_consumed, 1);
    assert!(
        !drop_dir
            .join("1795501000000-alice-to-bob-dropped.md")
            .exists(),
        "consumed drop file removed after durable SQLite + mirror export"
    );
    assert!(
        drop_dir.join("locked.md").is_file(),
        "Emacs-locked file left untouched"
    );

    let snap1 = mail_svc.mail_state_snapshot().await;
    assert!(snap1.messages.contains_key(&drop_normal_sha));
    assert!(!snap1.messages.contains_key(&locked_sha));

    // Remove Emacs lock -> next reconcile ingests locked.md!
    fs::remove_file(drop_dir.join(".#locked.md")).expect("remove lock");
    let rep2 = mail_svc.reconcile().await.expect("reconcile 2");
    assert_eq!(rep2.files_ingested, 1);
    assert_eq!(rep2.drop_files_consumed, 1);
    assert!(
        mail_svc
            .mail_state_snapshot()
            .await
            .messages
            .contains_key(&locked_sha)
    );

    // Bob acks drop_normal_sha, then working-tree file is edited IN PLACE!
    let bob_client = LocalClient::new(router.clone(), Principal::Seat { seat: bob.clone() });
    bob_client
        .call(MailAck {
            path: None,
            sha256: Some(drop_normal_sha.clone()),
            seat: None,
            note: None,
        })
        .await
        .expect("bob acks dropped letter");

    let wt_file = mirror_dir.join("mail/1795501000000-alice-to-bob-dropped.md");
    let edited_bytes =
        b"---\nid: 1795501000000-alice-to-bob-dropped\nfrom: alice\nto: bob\nsubject: Offline drop\n---\n\nEdited AFTER ack!\n";
    let edited_sha = sha256_of(edited_bytes);
    fs::write(&wt_file, edited_bytes).expect("edit working-tree file after ack");

    let rep3 = mail_svc.reconcile().await.expect("reconcile post-ack edit");
    assert_eq!(rep3.files_ingested, 1);
    assert_eq!(rep3.mutated_after_ack_flagged, 1);

    // EditNeverRingable: edited_sha is NOT in bob's unread check, NOT ringable, and refused by MailAck!
    let bob_check = bob_client
        .call(MailCheck {
            seat: None,
            include_all: false,
            limit: None,
        })
        .await
        .expect("bob check");
    assert!(
        !bob_check.messages.iter().any(|m| m.sha256 == edited_sha),
        "mutated_after_ack must never appear as unread"
    );
    assert!(matches!(
        bob_client
            .call(MailAck {
                path: None,
                sha256: Some(edited_sha.clone()),
                seat: None,
                note: None,
            })
            .await,
        Err(ApiError::Conflict { .. })
    ));

    // Verify ringable_for_seat excludes edited_sha while keeping the unacked locked letter.
    let snap_after_edit = mail_svc.mail_state_snapshot().await;
    assert!(
        !snap_after_edit
            .ringable_for_seat(&bob)
            .contains(&edited_sha),
        "EditNeverRingable: mutated_after_ack hash must never be ringable"
    );
    assert!(
        snap_after_edit
            .ringable_for_seat(&bob)
            .contains(&locked_sha),
        "unacked non-mutated letter remains ringable"
    );
    let _ = alice;

    // Snapshot pre-delete state, DELETE journal.sqlite3, and run lossless offline rebuild!
    let before = mail_svc.mail_state_snapshot().await;
    let before_db = before.db_set();
    let before_acked = before.acked.clone();
    let before_flagged = before.flagged_shas.clone();
    drop(router);
    drop(mail_svc);
    drop(journal);

    for suffix in ["", "-wal", "-shm"] {
        let mut p = journal_path.as_os_str().to_os_string();
        p.push(suffix);
        let _ = fs::remove_file(std::path::PathBuf::from(p));
    }

    let aliases = AliasMap::empty();
    let rebuilt = rebuild_mail_from_mirror(&mirror_dir, &journal_path, &aliases, &protection)
        .expect("lossless rebuild");
    assert_eq!(rebuilt.messages_rebuilt, before_db.len());
    assert_eq!(rebuilt.acks_rebuilt, before_acked.len());
    assert_eq!(rebuilt.mutated_after_ack_flagged, before_flagged.len());

    let journal_rebuilt = Journal::open(&journal_path).expect("open rebuilt journal");
    let after = MailState::from_journal(&journal_rebuilt).expect("replay rebuilt");
    assert_eq!(after.db_set(), before_db);
    assert_eq!(after.acked, before_acked);
    assert_eq!(after.flagged_shas, before_flagged);
    assert!(after.flagged_shas.contains(&edited_sha));
    assert!(
        !journal_rebuilt
            .search_mail("Dropped while offline", Some("bob"), 10)
            .expect("fts5 after rebuild")
            .is_empty(),
        "FTS5 search works immediately after lossless rebuild"
    );
}

#[test]
fn protected_roots_blocks_rebuild_and_migrate_verify_targets() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let protected_dir = tmp.path().join("protected-interchange");
    let mirror_dir = tmp.path().join("mirror");
    fs::create_dir_all(&protected_dir).expect("mkdir protected");
    fs::create_dir_all(&mirror_dir).expect("mkdir mirror");

    let protection = Protection {
        roots: vec![protected_dir.clone()],
        names: Vec::new(),
    };
    let aliases = AliasMap::empty();

    let rebuild_err = rebuild_mail_from_mirror(
        &mirror_dir,
        &protected_dir.join("journal.sqlite3"),
        &aliases,
        &protection,
    )
    .expect_err("rebuild into protected root must be refused");
    assert!(matches!(rebuild_err, SandboxError::Production(_)));

    let mv_err = migrate_verify_interchange(
        &protected_dir,
        &protected_dir.join("work"),
        &aliases,
        &protection,
    )
    .expect_err("migrate-verify work_dir inside protected root must be refused");
    assert!(matches!(mv_err, SandboxError::Production(_)));
}
