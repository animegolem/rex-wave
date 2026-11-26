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

    // 3. Multi-seat mail (alice -> bob + sam) projects into participant-scoped #mail-arch-sync-<hash8> channel.
    let expected_group_room = rex_wave_mailbridge::derive_mail_room(
        "alice",
        &[bob.clone(), sam.clone()],
        &[],
        Some("arch-sync"),
        "Quarterly architecture sync",
        &first.sha256,
    );
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
    assert_eq!(group.room, expected_group_room);
    assert!(
        matches!(
            &group.room,
            Some(RoomId::Channel {
                scope: RoomScope::Global,
                name
            }) if name.as_str().starts_with("mail-arch-sync-")
        ),
        "expected participant-hashed mail channel, got {:?}",
        group.room
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
    assert!(
        !protected_dir.join("work").exists(),
        "migrate-verify must never create a directory inside protected/interchange root before refusing"
    );
}

#[tokio::test]
async fn council_round2_privacy_scoped_idem_rebuild_edits_extra_recips_crash_and_ack_auth() {
    use rex_wave_api::read::{Read, Sidebar};
    use rex_wave_api::room::{Body, RoomEvent, RoomJoin, RoomPost, RoomRef};

    let tmp = tempfile::tempdir().expect("tempdir");
    let mirror_dir = tmp.path().join("mirror");
    let journal_path = tmp.path().join("journal.sqlite3");
    let protection = Protection {
        roots: Vec::new(),
        names: vec!["forbidden-box".into()],
    };
    let sandbox = make_sandbox(&mirror_dir, &protection);

    let alice = SeatName::parse("alice").unwrap();
    let bob = SeatName::parse("bob").unwrap();
    let carol = SeatName::parse("carol").unwrap();
    let dan = SeatName::parse("dan").unwrap();
    let eve = SeatName::parse("eve").unwrap();
    let owner = SeatName::parse("owner").unwrap();

    // --- Check 1 (Astra B4): Protected descendant refused BEFORE mkdir ---
    let mirror_target =
        rex_wave_mailbridge::MirrorTarget::open_verified(&mirror_dir, None, &protection)
            .expect("open mirror");
    let forbidden_res =
        mirror_target.export_working_tree_if_absent(Path::new("forbidden-box/leak.md"), b"hello\n");
    assert!(matches!(forbidden_res, Err(ApiError::Denied { .. })));
    assert!(
        !mirror_dir.join("forbidden-box").exists(),
        "protected directory name must be rejected BEFORE mkdir"
    );

    // --- Check 2 (Astra B7 + Opus S2): Crash after SQLite commit before RoomPost, then restart repairs room + preserves `re` ---
    let crash_parent_sha = {
        let j1 = Journal::open(&journal_path).unwrap();
        let w1 = j1.claim(StreamId::Mail).unwrap();
        // No room_svc attached -> simulates crash between SQLite MessageCommitted and RoomPost!
        let b1 = MailBridge::open(sandbox.clone(), j1, w1).unwrap();
        let WireReply::MailSend(sent) = b1
            .dispatch(
                MailCommand::MailSend(MailSend {
                    to: vec![bob.clone()],
                    cc: Vec::new(),
                    subject: "Parent before crash".into(),
                    body: "Committed before room projection.".into(),
                    from: Some(alice.clone()),
                    priority: LetterPriority::Normal,
                    topic: Some("thread-repair".into()),
                    in_reply_to: None,
                    refs: Vec::new(),
                    wait_export: true,
                    idem: Some(IdemKey::parse("shared-key").unwrap()),
                }),
                DecideCtx::for_test(
                    Principal::Seat {
                        seat: alice.clone(),
                    },
                    UnixMs(1_795_510_000_000),
                ),
            )
            .await
            .unwrap()
        else {
            unreachable!()
        };
        assert!(
            sent.room_seq.is_none(),
            "no room post yet during planted crash"
        );
        sent.sha256
    };

    // Boot incarnation 2 WITH room_svc -> reconcile repairs the missing DM post!
    let dm_alice_bob = RoomId::Direct {
        pair: PrincipalPair::new(
            Principal::Seat {
                seat: alice.clone(),
            },
            Principal::Seat { seat: bob.clone() },
        )
        .unwrap(),
    };
    let parent_room_seq = {
        let j2 = Journal::open(&journal_path).unwrap();
        let hub2 = Hub::new(128);
        let room_svc2 = Registry::<Rooms>::new(j2.clone(), hub2.clone());
        let w2 = j2.claim(StreamId::Mail).unwrap();
        let b2 = MailBridge::open_with_hub(sandbox.clone(), j2.clone(), w2, hub2)
            .unwrap()
            .with_room_service(room_svc2);
        b2.reconcile().await.unwrap();

        let dm_entries = j2
            .read(
                &StreamId::Room {
                    room: dm_alice_bob.clone(),
                },
                Seq::ORIGIN,
                32,
            )
            .unwrap();
        let posted = dm_entries
            .iter()
            .find(|e| matches!(e.event, Event::Room(RoomEvent::Posted { .. })))
            .expect("reconcile repaired missing room post");
        posted.seq
    };

    // Boot incarnation 3 (cold restart) -> reply naming crash_parent_sha MUST hydrate room_post_seqs and set `re = Some(parent_room_seq)`!
    let journal = Journal::open(&journal_path).unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let room_svc = Registry::<Rooms>::new(journal.clone(), hub.clone());
    let presence_svc = Presence::default().registry(journal.clone(), hub.clone());
    let mail_writer = journal.claim(StreamId::Mail).unwrap();
    let mail_svc = Arc::new(
        MailBridge::open_with_hub(sandbox.clone(), journal.clone(), mail_writer, hub.clone())
            .unwrap()
            .with_room_service(room_svc.clone()),
    );
    let router = Arc::new(Router {
        room: room_svc,
        mail: mail_svc.clone(),
        presence: presence_svc,
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar: sidebar.clone(),
            agents: Arc::new(NoAgentFeed),
        }))
    });

    let alice_c = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: alice.clone(),
        },
    );
    let bob_c = LocalClient::new(router.clone(), Principal::Seat { seat: bob.clone() });
    let carol_c = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: carol.clone(),
        },
    );
    let dan_c = LocalClient::new(router.clone(), Principal::Seat { seat: dan.clone() });
    let eve_c = LocalClient::new(router.clone(), Principal::Seat { seat: eve.clone() });
    let worker_c = LocalClient::new(
        router.clone(),
        Principal::Worker {
            wave: WaveSlug::parse("w1").unwrap(),
            role: RoleName::parse("r1").unwrap(),
        },
    );
    let owner_c = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: owner.clone(),
        },
    );

    let threaded_reply = bob_c
        .call(MailSend {
            to: vec![alice.clone()],
            cc: Vec::new(),
            subject: "Re: Parent before crash".into(),
            body: "Threaded after cold restart.".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("thread-repair".into()),
            in_reply_to: Some(crash_parent_sha.as_str().to_owned()),
            refs: Vec::new(),
            wait_export: true,
            idem: None,
        })
        .await
        .unwrap();
    let dm_entries = journal
        .read(
            &StreamId::Room {
                room: dm_alice_bob.clone(),
            },
            Seq::ORIGIN,
            32,
        )
        .unwrap();
    let reply_env = dm_entries
        .iter()
        .find(|e| Some(e.seq) == threaded_reply.room_seq)
        .expect("reply room post");
    assert!(
        matches!(
            reply_env.event,
            Event::Room(RoomEvent::Posted { re: Some(r), .. }) if r == parent_room_seq
        ),
        "cold-restart reply must link re = Some({parent_room_seq:?}), got {:?}",
        reply_env.event
    );

    // --- Check 3 (Opus B1 + Astra B1): StreamId::Mail blocked for seats/workers, participant-scoped private #mail-* channels ---
    for non_op in [&alice_c, &dan_c, &worker_c] {
        assert!(
            matches!(
                non_op
                    .call(Read::stream(StreamId::Mail, Seq::ORIGIN, None))
                    .await,
                Err(ApiError::Denied { .. })
            ),
            "non-operators must never read StreamId::Mail"
        );
        assert!(
            matches!(
                non_op
                    .watch(
                        Interest::Streams {
                            streams: vec![StreamId::Mail]
                        },
                        Cursor::default()
                    )
                    .await,
                Err(ApiError::Denied { .. })
            ),
            "non-operators must never watch StreamId::Mail"
        );
    }
    assert!(
        owner_c
            .call(Read::stream(StreamId::Mail, Seq::ORIGIN, None))
            .await
            .is_ok(),
        "operator retains StreamId::Mail read access"
    );

    // Two unrelated groups with the SAME topic "status" get distinct private channels!
    let grp1 = alice_c
        .call(MailSend {
            to: vec![bob.clone(), carol.clone()],
            cc: Vec::new(),
            subject: "Status check".into(),
            body: "Secret group 1 status.".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("status".into()),
            in_reply_to: None,
            refs: Vec::new(),
            wait_export: true,
            idem: None,
        })
        .await
        .unwrap();
    let grp2 = alice_c
        .call(MailSend {
            to: vec![dan.clone(), eve.clone()],
            cc: Vec::new(),
            subject: "Status check".into(),
            body: "Secret group 2 status.".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("status".into()),
            in_reply_to: None,
            refs: Vec::new(),
            wait_export: true,
            idem: None,
        })
        .await
        .unwrap();
    let grp1_room = grp1.room.clone().expect("grp1 room");
    let grp2_room = grp2.room.clone().expect("grp2 room");
    assert_ne!(
        grp1_room, grp2_room,
        "unrelated recipient groups on the same topic must never share a channel"
    );

    // Participants (carol) can read grp1_room; non-participants (dan, worker) are Denied on read, join, post, and hidden in Sidebar!
    assert!(
        carol_c
            .call(Read::stream(
                StreamId::Room {
                    room: grp1_room.clone()
                },
                Seq::ORIGIN,
                None
            ))
            .await
            .is_ok()
    );
    for outsider in [&dan_c, &worker_c] {
        assert!(matches!(
            outsider
                .call(Read::stream(
                    StreamId::Room {
                        room: grp1_room.clone()
                    },
                    Seq::ORIGIN,
                    None
                ))
                .await,
            Err(ApiError::Denied { .. })
        ));
        assert!(matches!(
            outsider
                .call(RoomJoin {
                    room: RoomRef::Id {
                        id: grp1_room.clone()
                    },
                    member: None,
                    handle: None,
                })
                .await,
            Err(ApiError::Denied { .. })
        ));
        assert!(matches!(
            outsider
                .call(RoomPost::new(
                    RoomRef::Id {
                        id: grp1_room.clone()
                    },
                    Body::parse("intrude").unwrap(),
                ))
                .await,
            Err(ApiError::Denied { .. })
        ));
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    let dan_sb = dan_c.call(Sidebar {}).await.unwrap();
    assert!(
        !dan_sb.channels.iter().any(|c| c.room == grp1_room),
        "non-participant sidebar must omit private mail channel"
    );

    // --- Check 4 (Opus S3 + Astra B2 + Opus B2): Principal-scoped `--id`, conflict on different payload, self-cc preserved ---
    // Bob using the exact same `--id shared-key` gets his OWN message, never Alice's!
    let bob_same_key = bob_c
        .call(MailSend {
            to: vec![carol.clone()],
            cc: Vec::new(),
            subject: "Bob independent send".into(),
            body: "Different principal, same caller key.".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: None,
            in_reply_to: None,
            refs: Vec::new(),
            wait_export: true,
            idem: Some(IdemKey::parse("shared-key").unwrap()),
        })
        .await
        .unwrap();
    assert!(!bob_same_key.replayed);
    assert_ne!(bob_same_key.sha256, crash_parent_sha);

    // Alice reusing `shared-key` with DIFFERENT payload must return Conflict (409)!
    let idem_conflict = alice_c
        .call(MailSend {
            to: vec![bob.clone()],
            cc: Vec::new(),
            subject: "Parent before crash".into(),
            body: "DIFFERENT BODY REUSING KEY".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("thread-repair".into()),
            in_reply_to: None,
            refs: Vec::new(),
            wait_export: false,
            idem: Some(IdemKey::parse("shared-key").unwrap()),
        })
        .await
        .expect_err("reusing --id with different payload must conflict");
    assert!(matches!(idem_conflict, ApiError::Conflict { .. }));

    // Opus B2: `--from alice --to bob --cc alice` preserves `cc=[alice]` and Channel room across rebuild!
    let self_cc = alice_c
        .call(MailSend {
            to: vec![bob.clone()],
            cc: vec![alice.clone()],
            subject: "Self CC probe".into(),
            body: "Keep explicit frontmatter cc.".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("self-cc-probe".into()),
            in_reply_to: None,
            refs: Vec::new(),
            wait_export: true,
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(self_cc.cc, vec![alice.clone()]);
    assert!(matches!(self_cc.room, Some(RoomId::Channel { .. })));

    // --- Check 5 (Astra B5 + Opus S4 + Astra S1 + Astra B8 + Astra S4 + Astra N1):
    //     3 revisions of a letter without frontmatter `id`, pre-ack edit order, ACK note + ms precision, ACK auth ordering ---
    let plain_path = mirror_dir.join("mail/plain-no-fm.md");
    let rev0 = b"---\nfrom: alice\nto: bob\nsubject: No frontmatter id\n---\n\nRevision zero.\n";
    let rev0_sha = sha256_of(rev0);
    fs::write(&plain_path, rev0).unwrap();
    mail_svc.reconcile().await.unwrap();

    // Ack rev0 with millisecond precision + note!
    mail_svc
        .dispatch(
            MailCommand::MailAck(MailAck {
                path: Some(Path::new("mail/plain-no-fm.md").to_path_buf()),
                sha256: None,
                seat: None,
                note: Some("rev0 note preserved".into()),
            }),
            DecideCtx::for_test(
                Principal::Seat { seat: bob.clone() },
                UnixMs(1_795_520_000_123),
            ),
        )
        .await
        .unwrap();
    // Retry ACK with a different note/time -> committed receipt must NOT be overwritten!
    mail_svc
        .dispatch(
            MailCommand::MailAck(MailAck {
                path: None,
                sha256: Some(rev0_sha.clone()),
                seat: None,
                note: Some("late retry note ignored".into()),
            }),
            DecideCtx::for_test(
                Principal::Seat { seat: bob.clone() },
                UnixMs(1_795_520_999_999),
            ),
        )
        .await
        .unwrap();

    // Edit twice in place after ack!
    let rev1 =
        b"---\nfrom: alice\nto: bob\nsubject: No frontmatter id\n---\n\nRevision ONE after ack.\n";
    let rev1_sha = sha256_of(rev1);
    fs::write(&plain_path, rev1).unwrap();
    mail_svc.reconcile().await.unwrap();

    let rev2 =
        b"---\nfrom: alice\nto: bob\nsubject: No frontmatter id\n---\n\nRevision TWO after ack.\n";
    let rev2_sha = sha256_of(rev2);
    fs::write(&plain_path, rev2).unwrap();
    mail_svc.reconcile().await.unwrap();

    // Opus S4: Edit BEFORE ack (`pre_a` -> `pre_b`), then ack `pre_a` -> `pre_b` stays unflagged across rebuild!
    let pre_path = mirror_dir.join("mail/pre-ack-edit.md");
    let pre_a = b"---\nid: pre-a\nfrom: alice\nto: bob\nsubject: Pre ack\n---\n\nVersion A.\n";
    let pre_a_sha = sha256_of(pre_a);
    fs::write(&pre_path, pre_a).unwrap();
    mail_svc.reconcile().await.unwrap();
    let pre_b =
        b"---\nid: pre-b\nfrom: alice\nto: bob\nsubject: Pre ack\n---\n\nVersion B before ack.\n";
    let pre_b_sha = sha256_of(pre_b);
    fs::write(&pre_path, pre_b).unwrap();
    mail_svc.reconcile().await.unwrap();
    bob_c
        .call(MailAck {
            path: None,
            sha256: Some(pre_a_sha.clone()),
            seat: None,
            note: None,
        })
        .await
        .unwrap();

    // Astra S4: Path-only ACK by bob on `mail/plain-no-fm.md` must resolve to current flagged rev2 and REFUSE with Conflict!
    assert!(matches!(
        bob_c
            .call(MailAck {
                path: Some(Path::new("mail/plain-no-fm.md").to_path_buf()),
                sha256: None,
                seat: None,
                note: None,
            })
            .await,
        Err(ApiError::Conflict { .. })
    ));

    // Astra B8: Non-recipient `dan` probing `mail/plain-no-fm.md` (flagged!) or wrong sha on `mail/plain-no-fm.md`
    // MUST get `Denied` (masked as 404 on HTTP), NEVER `Conflict`!
    assert!(matches!(
        dan_c
            .call(MailAck {
                path: Some(Path::new("mail/plain-no-fm.md").to_path_buf()),
                sha256: None,
                seat: None,
                note: None,
            })
            .await,
        Err(ApiError::Denied { .. })
    ));
    assert!(matches!(
        dan_c
            .call(MailAck {
                path: Some(Path::new("mail/plain-no-fm.md").to_path_buf()),
                sha256: Some(Sha256Hex::parse("f".repeat(64)).unwrap()),
                seat: None,
                note: None,
            })
            .await,
        Err(ApiError::Denied { .. })
    ));

    // Astra N1: FTS5 finds imported letter by frontmatter `id` and `topic` even when filename differs!
    let custom_id_file = mirror_dir.join("mail/unrelated-filename.md");
    fs::write(
        &custom_id_file,
        b"---\nid: custom-msg-id-991\nfrom: alice\nto: bob\nsubject: Plain title\ntopic: custom-topic-772\n---\n\nPlain text.\n",
    )
    .unwrap();
    mail_svc.reconcile().await.unwrap();
    let id_hits = bob_c
        .call(MailSearch {
            query: "custom-msg-id-991".into(),
            seat: None,
            limit: Some(5),
        })
        .await
        .unwrap();
    assert_eq!(id_hits.hits.len(), 1);
    let topic_hits = bob_c
        .call(MailSearch {
            query: "custom-topic-772".into(),
            seat: None,
            limit: Some(5),
        })
        .await
        .unwrap();
    assert_eq!(topic_hits.hits.len(), 1);

    // Delete journal.sqlite3, run offline `rebuild_mail_from_mirror`, and verify:
    // 1) Both rev1_sha and rev2_sha stay flagged (`EditNeverRingable`, NEVER unread!),
    // 2) `pre_b_sha` stays unflagged (`Opus S4`),
    // 3) `self_cc` keeps `cc=[alice]` and its exact Channel room (`Opus B2`),
    // 4) `rev0_sha` ACK keeps `UnixMs(1_795_520_000_123)` and `"rev0 note preserved"` (`Astra S1`),
    // 5) Retrying Alice's original `shared-key` `MailSend` returns `replayed=true` without duplicating (`Astra B2`)!
    let count_before_rebuild = mail_svc.mail_state_snapshot().await.messages.len();
    drop(alice_c);
    drop(bob_c);
    drop(carol_c);
    drop(dan_c);
    drop(eve_c);
    drop(worker_c);
    drop(owner_c);
    drop(router);
    drop(mail_svc);
    drop(journal);

    for suffix in ["", "-wal", "-shm"] {
        let mut p = journal_path.as_os_str().to_os_string();
        p.push(suffix);
        let _ = fs::remove_file(std::path::PathBuf::from(p));
    }

    rebuild_mail_from_mirror(&mirror_dir, &journal_path, &AliasMap::empty(), &protection)
        .expect("rebuild");

    let j_after = Journal::open(&journal_path).unwrap();
    let st_after = MailState::from_journal(&j_after).unwrap();
    assert!(st_after.flagged_shas.contains(&rev1_sha));
    assert!(st_after.flagged_shas.contains(&rev2_sha));
    assert!(!st_after.ringable_for_seat(&bob).contains(&rev1_sha));
    assert!(!st_after.ringable_for_seat(&bob).contains(&rev2_sha));
    assert!(!st_after.flagged_shas.contains(&pre_b_sha));
    assert!(st_after.ringable_for_seat(&bob).contains(&pre_b_sha));

    let rebuilt_self_cc = &st_after.messages[&self_cc.sha256].message;
    assert_eq!(rebuilt_self_cc.cc, vec![alice.clone()]);
    assert_eq!(rebuilt_self_cc.room, self_cc.room);

    let rebuilt_ack = &st_after.ack_records[&(bob.clone(), rev0_sha)];
    assert_eq!(rebuilt_ack.at, UnixMs(1_795_520_000_123));
    assert_eq!(rebuilt_ack.note.as_deref(), Some("rev0 note preserved"));

    let w_after = j_after.claim(StreamId::Mail).unwrap();
    let b_after = MailBridge::open(sandbox, j_after, w_after).unwrap();
    let WireReply::MailSend(replayed_after_rebuild) = b_after
        .dispatch(
            MailCommand::MailSend(MailSend {
                to: vec![bob.clone()],
                cc: Vec::new(),
                subject: "Parent before crash".into(),
                body: "Committed before room projection.".into(),
                from: Some(alice.clone()),
                priority: LetterPriority::Normal,
                topic: Some("thread-repair".into()),
                in_reply_to: None,
                refs: Vec::new(),
                wait_export: false,
                idem: Some(IdemKey::parse("shared-key").unwrap()),
            }),
            DecideCtx::for_test(
                Principal::Seat {
                    seat: alice.clone(),
                },
                UnixMs(1_795_599_000_000),
            ),
        )
        .await
        .unwrap()
    else {
        unreachable!()
    };
    assert!(replayed_after_rebuild.replayed);
    assert_eq!(replayed_after_rebuild.sha256, crash_parent_sha);
    assert_eq!(
        b_after.mail_state_snapshot().await.messages.len(),
        count_before_rebuild,
        "retry after rebuild must not create a duplicate message"
    );

    // --- Check 6 (Astra B3 + Astra B6): migrate-verify preserves mailbox-derived extra recipient & protects pre-existing work-dir journal ---
    let src_interchange = tmp.path().join("src-interchange");
    fs::create_dir_all(src_interchange.join("mail")).unwrap();
    fs::create_dir_all(src_interchange.join("to-dan")).unwrap();
    let shared_letter =
        b"---\nid: 1795530000000-alice-to-bob\nfrom: alice\nto: [bob, carol]\nsubject: Extra mailbox copy\n---\n\nDelivered to Dan via box.\n";
    fs::write(src_interchange.join("mail/shared.md"), shared_letter).unwrap();
    fs::write(src_interchange.join("to-dan/shared.md"), shared_letter).unwrap();

    // Overlap inside read-only source is refused BEFORE mkdir!
    let overlap_target = src_interchange.join("nested-work");
    assert!(
        migrate_verify_interchange(
            &src_interchange,
            &overlap_target,
            &AliasMap::empty(),
            &Protection::default()
        )
        .is_err()
    );
    assert!(
        !overlap_target.exists(),
        "migrate-verify must reject work_dir inside interchange_dir before creating it"
    );

    // Pre-existing journal.sqlite3 in --work-dir is untouched!
    let existing_work = tmp.path().join("existing-work");
    fs::create_dir_all(&existing_work).unwrap();
    let precious_journal = existing_work.join("journal.sqlite3");
    fs::write(&precious_journal, b"precious-user-journal-bytes").unwrap();

    let mv_report = migrate_verify_interchange(
        &src_interchange,
        &existing_work,
        &AliasMap::empty(),
        &Protection::default(),
    )
    .expect("migrate-verify with extra mailbox recipient");
    assert!(
        mv_report.parity_ok,
        "strict parity check must pass with mailbox-derived recipient preserved: {mv_report:?}"
    );
    assert_eq!(
        fs::read(&precious_journal).unwrap(),
        b"precious-user-journal-bytes",
        "pre-existing journal.sqlite3 in --work-dir must never be touched or deleted"
    );
}
