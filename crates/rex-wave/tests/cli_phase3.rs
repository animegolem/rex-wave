#![allow(clippy::disallowed_methods)]
use rex_wave_api::room::{Body, RoomMessageSummary};
use rex_wave_api::wave::{
    Activity, AgentSpec, Observation, PermissionMode, WaveObserve, WaveSpec, WaveStart, WaveView,
    WorkerPhase, WorkerSpec, Workspace,
};
use rex_wave_api::*;
use rex_wave_notify::Inbox;
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    BoxFut, DecideCtx, Hub, LocalClient, NoAgentFeed, ReadSide, Registry, Router, Service,
    SidebarProjection,
};
use rex_wave_store::Journal;
use rex_wave_transport::{Authenticator, StateDirAuthenticator, bind_unix, serve_listener};
use rex_wave_waves::Waves;
use serde_json::Value;
use std::io::Write as _;
use std::os::unix::fs::PermissionsExt;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

mod common;
use common::{fresh_dir, rex_wave_at};

/// Wraps `Registry<Rooms>` so `RoomPost { after: Some(Seq(1)), .. }` triggers a
/// structured `ConflictData::NewerMessage` even before PR-3B merges into `rex-wave-rooms`.
struct ConflictInjectingRooms {
    inner: Arc<dyn Service<RoomCommand>>,
}

impl Service<RoomCommand> for ConflictInjectingRooms {
    fn dispatch(
        &self,
        cmd: RoomCommand,
        ctx: DecideCtx,
    ) -> BoxFut<'_, Result<WireReply, ApiError>> {
        Box::pin(async move {
            if let RoomCommand::RoomPost(ref p) = cmd
                && let Some(expected) = p.after
            {
                return Err(ApiError::conflict_with(
                    "newer post landed in room",
                    ConflictData::NewerMessage {
                        expected,
                        latest: Seq(expected.0 + 1),
                        newer_message: Box::new(RoomMessageSummary {
                            seq: Seq(expected.0 + 1),
                            at: UnixMs(1234),
                            by: ctx.by().clone(),
                            body: Body::parse("landed first").unwrap(),
                            re: p.re,
                            labels: vec![],
                        }),
                    },
                ));
            }
            self.inner.dispatch(cmd, ctx).await
        })
    }
}

#[tokio::test]
async fn cli_post_reply_read_wait_conflict_and_t3_worker_token_pickup() {
    let dir = fresh_dir("verbs");
    let sock = dir.0.join("daemon.sock");
    let journal = Journal::open(&dir.0.join("journal.sqlite3")).unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());

    let inner_rooms: Arc<dyn Service<RoomCommand>> =
        Registry::<Rooms>::new(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Arc::new(ConflictInjectingRooms { inner: inner_rooms }),
        wave: Registry::<Waves>::new(journal.clone(), hub.clone()),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }))
    });

    let (listener, daemon_uid) = bind_unix(&sock).unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(StateDirAuthenticator::from_state_dir(
        SeatName::parse("owner").unwrap(),
        Some(daemon_uid),
        &dir.0,
    ));
    let server = tokio::spawn(serve_listener(listener, router.clone(), auth));
    let sock_str = sock.to_str().unwrap().to_owned();

    let s1 = sock_str.clone();
    let root_out = tokio::task::spawn_blocking(move || {
        let mut child = rex_wave_at(&s1)
            .args(["--socket", &s1, "--json", "post", "#general", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"root from stdin\n")
            .unwrap();
        child.wait_with_output().unwrap()
    })
    .await
    .unwrap();
    assert!(
        root_out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&root_out.stderr)
    );
    let root_json: Value = serde_json::from_slice(&root_out.stdout).unwrap();
    let root_seq = root_json["value"]["seq"].as_u64().unwrap();

    let s2 = sock_str.clone();
    let reply_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s2)
            .args([
                "--socket",
                &s2,
                "--json",
                "reply",
                "#general",
                &root_seq.to_string(),
                "reply before wait",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(reply_out.status.success());
    let reply_json: Value = serde_json::from_slice(&reply_out.stdout).unwrap();
    let reply_seq = reply_json["value"]["seq"].as_u64().unwrap();
    assert!(reply_seq > root_seq);

    let s3 = sock_str.clone();
    let read_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s3)
            .args([
                "--socket",
                &s3,
                "--json",
                "read",
                "#general",
                "--re",
                &root_seq.to_string(),
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(read_out.status.success());
    let read_json: Value = serde_json::from_slice(&read_out.stdout).unwrap();
    assert_eq!(read_json["reply"], "read");
    let entries = read_json["value"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);

    let s4 = sock_str.clone();
    let wait_hit = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s4)
            .args([
                "--socket",
                &s4,
                "--json",
                "wait",
                "#general",
                "--since",
                &root_seq.to_string(),
                "--re",
                &root_seq.to_string(),
                "--timeout",
                "2000",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(wait_hit.status.success());
    let hit_json: Value = serde_json::from_slice(&wait_hit.stdout).unwrap();
    assert_eq!(hit_json["value"]["timed_out"], false);
    assert_eq!(
        hit_json["value"]["entries"][0]["event"]["event"]["body"],
        "reply before wait"
    );

    let s5 = sock_str.clone();
    let wait_to = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s5)
            .args([
                "--socket",
                &s5,
                "--json",
                "wait",
                "#general",
                "--since",
                &reply_seq.to_string(),
                "--all",
                "--timeout",
                "40",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(wait_to.status.code(), Some(124));
    let to_json: Value = serde_json::from_slice(&wait_to.stdout).unwrap();
    assert_eq!(to_json["value"]["timed_out"], true);

    let s6 = sock_str.clone();
    let conflict_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s6)
            .args([
                "--socket",
                &s6,
                "post",
                "#general",
                "stale update",
                "--after",
                "1",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(conflict_out.status.code(), Some(3));
    let stderr = String::from_utf8_lossy(&conflict_out.stderr);
    assert!(stderr.contains("newer_message"), "stderr was: {stderr}");
    assert!(stderr.contains("landed first"), "stderr was: {stderr}");

    let op_client = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let sys_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::T3Bridge,
        },
    );
    let wave = WaveSlug::parse("w1").unwrap();
    let role = RoleName::parse("author").unwrap();
    let thread_id = "t3:w1:author:a1";
    let agent = AgentSpec {
        provider: ProviderId::parse("antigravity").unwrap(),
        model: None,
        account: None,
        reasoning: None,
        service_tier: None,
        permission: PermissionMode::AcceptEdits,
        workspace: Workspace::ProjectDefault,
    };
    op_client
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: BbProjectId::parse("proj_1").unwrap(),
                goal: Body::parse("test t3 worker cli").unwrap(),
                defaults: agent.clone(),
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("work").unwrap(),
                    agent: Some(agent),
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                }],
            },
            idem: None,
        })
        .await
        .unwrap();
    sys_client
        .call(WaveObserve {
            wave: wave.clone(),
            role: role.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: BbThreadId::parse(thread_id).unwrap(),
            },
        })
        .await
        .unwrap();

    let s7 = sock_str.clone();
    let mint_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s7)
            .args([
                "--socket",
                &s7,
                "token",
                "--worker",
                "w1/author",
                "--thread",
                thread_id,
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(mint_out.status.success());
    let tok_file = rex_wave_t3::worker_token_path(&dir.0, thread_id).unwrap();
    assert_eq!(
        std::fs::symlink_metadata(&tok_file)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let s8 = sock_str.clone();
    let worker_post = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s8)
            .env("T3CODE_THREAD_ID", thread_id)
            .args([
                "--socket",
                &s8,
                "--json",
                "post",
                "#general",
                "from t3 worker",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        worker_post.status.success(),
        "worker post failed: {}",
        String::from_utf8_lossy(&worker_post.stderr)
    );
    let wp_json: Value = serde_json::from_slice(&worker_post.stdout).unwrap();
    let wp_seq = wp_json["value"]["seq"].as_u64().unwrap();

    let s9 = sock_str.clone();
    let check_read = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s9)
            .args([
                "--socket",
                &s9,
                "--json",
                "read",
                "#general",
                "--after",
                &(wp_seq - 1).to_string(),
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let cr_json: Value = serde_json::from_slice(&check_read.stdout).unwrap();
    assert_eq!(
        cr_json["value"]["entries"][0]["by"],
        serde_json::json!({ "kind": "worker", "wave": "w1", "role": "author" })
    );

    let s10 = sock_str.clone();
    let worker_wait = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s10)
            .env("T3CODE_THREAD_ID", thread_id)
            .args([
                "--socket",
                &s10,
                "--json",
                "wait",
                "#general",
                "--since",
                &wp_seq.to_string(),
                "--all",
                "--timeout",
                "4000",
            ])
            .output()
            .unwrap()
    });

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snap = op_client
                .call(WaveView { wave: wave.clone() })
                .await
                .unwrap();
            let is_parked = snap.workers.iter().any(|(r, _, phase)| {
                r == &role
                    && matches!(
                        phase,
                        WorkerPhase::Live {
                            activity: Activity::Parked { .. },
                            ..
                        }
                    )
            });
            if is_parked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("T3 worker CLI wait entered Activity::Parked");

    let s11 = sock_str.clone();
    let wake_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s11)
            .args(["--socket", &s11, "post", "#general", "wake worker"])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(wake_out.status.success());

    let ww_out = worker_wait.await.unwrap();
    assert!(ww_out.status.success());

    let snap_after = op_client
        .call(WaveView { wave: wave.clone() })
        .await
        .unwrap();
    assert!(snap_after.workers.iter().any(|(r, _, phase)| {
        r == &role
            && matches!(
                phase,
                WorkerPhase::Live {
                    activity: Activity::Working,
                    ..
                }
            )
    }));

    rex_wave_t3::remove_worker_token_file(&dir.0, thread_id).unwrap();
    let s12 = sock_str.clone();
    let retired_post = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s12)
            .env("T3CODE_THREAD_ID", thread_id)
            .args(["--socket", &s12, "post", "#general", "should fail closed"])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(!retired_post.status.success());

    // Runner-agnostic `REX_WAVE_WORKER_THREAD_ID` authenticates ACP/native worker
    // tokens and fails closed on missing/unlinked files regardless of scheme.
    let acp_tid = rex_wave_api::HostThreadId::parse("acp:w1:author:a2").unwrap();
    rex_wave_transport::provision_worker_token_in_state_dir(&dir.0, &wave, &role, &acp_tid)
        .unwrap();
    let s13 = sock_str.clone();
    let acp_post = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s13)
            .env("REX_WAVE_WORKER_THREAD_ID", acp_tid.as_str())
            .args([
                "--socket",
                &s13,
                "--json",
                "post",
                "#general",
                "from acp worker",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        acp_post.status.success(),
        "REX_WAVE_WORKER_THREAD_ID post failed: {}",
        String::from_utf8_lossy(&acp_post.stderr)
    );
    rex_wave_transport::remove_worker_token_file(&dir.0, "acp:w1:author:a2").unwrap();
    let s14 = sock_str.clone();
    let acp_retired = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s14)
            .env("REX_WAVE_WORKER_THREAD_ID", "unprefixed_worker_tid")
            .args(["--socket", &s14, "post", "#general", "must fail closed"])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        !acp_retired.status.success(),
        "REX_WAVE_WORKER_THREAD_ID must fail closed even for unprefixed worker ids"
    );

    server.abort();
}

#[tokio::test]
async fn cli_wave_honesty_enforces_one_writer_refusals_finish_gate_account_mismatch_and_caps() {
    let dir = fresh_dir("honesty");
    let sock = dir.0.join("daemon.sock");
    let sock_str = sock.to_string_lossy().into_owned();

    let journal = Journal::open(&dir.0.join("journal.sqlite3")).unwrap();
    let hub = Hub::new(128);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Registry::<Waves>::new(journal.clone(), hub.clone()),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }))
    });

    let (listener, daemon_uid) = bind_unix(&sock).unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(StateDirAuthenticator::from_state_dir(
        SeatName::parse("owner").unwrap(),
        Some(daemon_uid),
        &dir.0,
    ));
    let server = tokio::spawn(serve_listener(listener, router.clone(), auth));

    let run_cli = |args: Vec<&str>| {
        let s = sock_str.clone();
        let owned: Vec<String> = args.into_iter().map(str::to_owned).collect();
        tokio::task::spawn_blocking(move || {
            rex_wave_at(&s)
                .arg("--socket")
                .arg(&s)
                .args(&owned)
                .output()
                .unwrap()
        })
    };

    // 1. Structural one-writer violation on wave start exits 3 (Conflict).
    let overlap_out = run_cli(vec![
        "wave",
        "start",
        "bad-overlap",
        "--project",
        "proj_1",
        "--goal",
        "Test overlap",
        "--worker",
        "w1=codex/gpt-6.1-sol",
        "--worker",
        "w2=claude-code/claude-opus-5-5",
        "--worker-writes",
        "w1=src/auth.rs",
        "--worker-writes",
        "w2=./src/auth.rs",
    ])
    .await
    .unwrap();
    assert_eq!(overlap_out.status.code(), Some(3));

    // 2. Clean wave with --account + --output-dir succeeds.
    let start_out = run_cli(vec![
        "wave",
        "start",
        "honesty-cli",
        "--project",
        "proj_1",
        "--goal",
        "Verify wave honesty CLI",
        "--account",
        "work-acct",
        "--output-dir",
        "runs/honesty-cli",
        "--worker",
        "auditor=claude-code/claude-opus-5-5",
        "--worker",
        "builder=codex/gpt-6.1-sol",
    ])
    .await
    .unwrap();
    assert!(
        start_out.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&start_out.stderr)
    );

    let sys = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::BbBridge,
        },
    );
    let wave = WaveSlug::parse("honesty-cli").unwrap();
    let auditor = RoleName::parse("auditor").unwrap();
    let builder = RoleName::parse("builder").unwrap();

    sys.call(WaveObserve {
        wave: wave.clone(),
        role: auditor.clone(),
        observation: Observation::Spawned {
            attempt: 1,
            thread: BbThreadId::parse("th_auditor").unwrap(),
        },
    })
    .await
    .unwrap();
    sys.call(WaveObserve {
        wave: wave.clone(),
        role: builder.clone(),
        observation: Observation::Spawned {
            attempt: 1,
            thread: BbThreadId::parse("th_builder").unwrap(),
        },
    })
    .await
    .unwrap();
    sys.call(WaveObserve {
        wave: wave.clone(),
        role: auditor.clone(),
        observation: Observation::RuntimeMeta {
            actual_model: Some(ModelId::parse("claude-opus-5-5-actual").unwrap()),
            actual_account: Some("personal-acct".into()),
        },
    })
    .await
    .unwrap();
    sys.call(WaveObserve {
        wave: wave.clone(),
        role: auditor.clone(),
        observation: Observation::activity(Activity::Refused {
            reason: "cyber_safeguard: blocked".into(),
        }),
    })
    .await
    .unwrap();

    // 3. wave status prints refused count, ⊘ refusal detail, and ACCOUNT MISMATCH.
    let status_out = run_cli(vec!["wave", "status", "honesty-cli"])
        .await
        .unwrap();
    assert!(status_out.status.success());
    let status_txt = String::from_utf8_lossy(&status_out.stdout);
    assert!(
        status_txt.contains("refused=1"),
        "expected refused=1 in wave status:\n{status_txt}"
    );
    assert!(
        status_txt.contains("⊘ refused: cyber_safeguard: blocked"),
        "expected ⊘ refused detail in wave status:\n{status_txt}"
    );
    assert!(
        status_txt.contains("[ACCOUNT MISMATCH]"),
        "expected ACCOUNT MISMATCH in wave status:\n{status_txt}"
    );
    assert!(
        status_txt.contains("acct=work-acct (unconfirmed)"),
        "expected unconfirmed requested account marker on builder:\n{status_txt}"
    );

    // 4. wave finish without --allow-incomplete exits 3; with --allow-incomplete succeeds.
    let finish_blocked = run_cli(vec!["wave", "finish", "honesty-cli", "--summary", "done"])
        .await
        .unwrap();
    assert_eq!(finish_blocked.status.code(), Some(3));
    let finish_ok = run_cli(vec![
        "wave",
        "finish",
        "honesty-cli",
        "--summary",
        "partial",
        "--allow-incomplete",
    ])
    .await
    .unwrap();
    assert!(
        finish_ok.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&finish_ok.stderr)
    );

    // 5. Lesson 4: read --limit 1 and tasks list --limit 1 print accurate per-command N of M hints,
    //    and tasks list --board --limit is rejected instead of silently ignored.
    for title in ["Task 1", "Task 2"] {
        let out = run_cli(vec!["tasks", "add", "#caps", title]).await.unwrap();
        assert!(out.status.success());
    }
    let read_capped = run_cli(vec!["read", "#caps", "--limit", "1"])
        .await
        .unwrap();
    assert!(read_capped.status.success());
    let read_txt = String::from_utf8_lossy(&read_capped.stdout);
    assert!(
        read_txt.contains("[showing 1 of ")
            && read_txt.contains("use --after ")
            && read_txt.contains("/ --cursor / --limit to page]"),
        "expected N of M cap banner on read --limit 1:\n{read_txt}"
    );

    let tasks_capped = run_cli(vec!["tasks", "list", "#caps", "--limit", "1"])
        .await
        .unwrap();
    assert!(tasks_capped.status.success());
    let tasks_txt = String::from_utf8_lossy(&tasks_capped.stdout);
    assert!(
        tasks_txt.contains("[showing 1 of 2 — raise --limit (max 1000) to see more]"),
        "expected N of M cap banner on tasks list --limit 1:\n{tasks_txt}"
    );

    let board_limit_err = run_cli(vec![
        "tasks",
        "list",
        "--board",
        "seat:owner",
        "--limit",
        "1",
    ])
    .await
    .unwrap();
    assert!(
        !board_limit_err.status.success(),
        "tasks list --board --limit must be rejected instead of silently ignored"
    );

    server.abort();
}

#[tokio::test]
async fn worker_credentials_fail_closed_and_legacy_tokens_do_not_rebind_across_principals() {
    use rex_wave_api::read::{Read, Wait};
    use rex_wave_api::room::{
        Body, Mentions, PostOrigin, PrincipalPair, RoomEvent, RoomId, RoomOrigin, RoomPost, RoomRef,
    };
    use rex_wave_api::{
        HostThreadId, IdemKey, Interest, Principal, RoleName, SeatName, Seq, StreamId, UnixMs,
        WaveSlug,
    };
    use rex_wave_runtime::LocalClient;
    use rex_wave_store::Journal;
    use rex_wave_transport::{PrincipalTokenTable, provision_worker_token_in_state_dir};
    use std::os::unix::fs::PermissionsExt as _;

    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::set_permissions(&state_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let sock = state_dir.join("daemon.sock");

    // 1. Blank REX_WAVE_WORKER_THREAD_ID and malformed or Unicode-padded `t3:` T3CODE_THREAD_ID fail closed (never PeerUid).
    for (env_k, env_v) in [
        ("REX_WAVE_WORKER_THREAD_ID", "   ".to_owned()),
        ("T3CODE_THREAD_ID", format!("t3:{}", "x".repeat(300))),
        ("T3CODE_THREAD_ID", "t3:bad/slash".to_owned()),
        ("T3CODE_THREAD_ID", "\u{00a0}t3:w1:target:a1".to_owned()),
    ] {
        let out = common::rex_wave_at(&sock)
            .arg("inbox")
            .env(env_k, &env_v)
            .output()
            .unwrap();
        assert!(!out.status.success(), "{env_k}={env_v} must fail closed");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("refusing PeerUid fallback")
                || stderr.contains("failed reading worker token"),
            "unexpected error for {env_k}: {stderr}"
        );
    }

    // 2. Legacy plaintext worker token belonging to `@w/other` OR a principal-scoped token in
    //    `tokens.json` without a thread binding rotates instead of rebinding/surviving retirement,
    //    while a thread-bound legacy entry in `tokens.json` upgrades cleanly and revokes on unlink.
    let wave = WaveSlug::parse("w1").unwrap();
    let other_role = RoleName::parse("other").unwrap();
    let target_role = RoleName::parse("target").unwrap();
    let tid = HostThreadId::parse("t3:w1:target:a1").unwrap();
    let wt_dir = state_dir.join("worker-tokens");
    std::fs::create_dir_all(&wt_dir).unwrap();
    std::fs::set_permissions(&wt_dir, std::fs::Permissions::from_mode(0o700)).unwrap();

    let tokens_path = state_dir.join("tokens.json");
    let wt_file = wt_dir.join(tid.as_str());
    std::fs::write(&wt_file, "rw_w_verified_thread_bound\n").unwrap();
    std::fs::set_permissions(&wt_file, std::fs::Permissions::from_mode(0o600)).unwrap();

    let mut table = PrincipalTokenTable::new();
    table
        .insert_worker("rw_w_other_secret", wave.clone(), other_role)
        .unwrap();
    table
        .insert_worker("rw_w_principal_scoped", wave.clone(), target_role.clone())
        .unwrap();
    table
        .insert_legacy_worker_thread(
            "rw_w_verified_thread_bound",
            wave.clone(),
            target_role.clone(),
            tid.as_str(),
        )
        .unwrap();
    table.save_tokens_file(&tokens_path).unwrap();

    std::fs::write(&wt_file, "rw_w_other_secret\n").unwrap();
    std::fs::set_permissions(&wt_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let rotated =
        provision_worker_token_in_state_dir(&state_dir, &wave, &target_role, &tid).unwrap();
    assert!(
        rotated.created,
        "unowned legacy plaintext token must rotate"
    );
    assert_ne!(rotated.token, "rw_w_other_secret");

    std::fs::write(&wt_file, "rw_w_principal_scoped\n").unwrap();
    std::fs::set_permissions(&wt_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let rotated_principal =
        provision_worker_token_in_state_dir(&state_dir, &wave, &target_role, &tid).unwrap();
    assert!(
        rotated_principal.created,
        "principal-scoped tokens.json bearer must NOT be reused as a thread-scoped worker token"
    );
    assert_ne!(rotated_principal.token, "rw_w_principal_scoped");

    std::fs::write(&wt_file, "rw_w_verified_thread_bound\n").unwrap();
    std::fs::set_permissions(&wt_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let upgraded =
        provision_worker_token_in_state_dir(&state_dir, &wave, &target_role, &tid).unwrap();
    assert!(
        !upgraded.created,
        "thread-bound legacy token for same principal and thread upgrades cleanly"
    );
    assert_eq!(upgraded.token, "rw_w_verified_thread_bound");

    let auth = rex_wave_transport::StateDirAuthenticator::from_state_dir(
        SeatName::parse("owner").unwrap(),
        None,
        &state_dir,
    );
    use rex_wave_transport::Authenticator as _;
    assert_eq!(
        auth.authenticate(
            None,
            &rex_wave_transport::Credential::Token {
                token: upgraded.token.clone()
            },
        )
        .unwrap(),
        Principal::Worker {
            wave: wave.clone(),
            role: target_role.clone()
        }
    );
    rex_wave_transport::remove_worker_token_file(&state_dir, tid.as_str()).unwrap();
    assert!(
        auth.authenticate(
            None,
            &rex_wave_transport::Credential::Token {
                token: upgraded.token
            },
        )
        .is_err(),
        "deleting the upgraded worker token file must revoke authentication immediately"
    );

    rex_wave_transport::write_worker_token_record(
        &state_dir,
        tid.as_str(),
        "rw_w_principal_scoped",
        &Principal::Worker {
            wave: wave.clone(),
            role: target_role.clone(),
        },
    )
    .unwrap();
    let rotated_structured =
        provision_worker_token_in_state_dir(&state_dir, &wave, &target_role, &tid).unwrap();
    assert!(
        rotated_structured.created,
        "already-upgraded structured worker file sharing an independent tokens.json grant must rotate"
    );
    assert_ne!(rotated_structured.token, "rw_w_principal_scoped");
    let reused_fresh =
        provision_worker_token_in_state_dir(&state_dir, &wave, &target_role, &tid).unwrap();
    assert!(
        !reused_fresh.created && reused_fresh.token == rotated_structured.token,
        "thread-only structured worker token reuses idempotently across provision calls"
    );
    assert_eq!(
        auth.authenticate(
            None,
            &rex_wave_transport::Credential::Token {
                token: rotated_structured.token.clone()
            },
        )
        .unwrap(),
        Principal::Worker {
            wave: wave.clone(),
            role: target_role.clone()
        }
    );
    rex_wave_transport::remove_worker_token_file(&state_dir, tid.as_str()).unwrap();
    assert!(
        auth.authenticate(
            None,
            &rex_wave_transport::Credential::Token {
                token: rotated_structured.token
            },
        )
        .is_err(),
        "deleting rotated structured worker file revokes its thread bearer immediately"
    );

    // 3. Explicit canonical RoomRef::Id write + Read + RoomFiltered Wait on a legacy-only DM stay symmetric,
    //    while raw StreamId reads/subscriptions remain exact, and Seat/Operator cannot spoof MirroredTurn.
    let db_path = tmp.path().join("legacy-dm.sqlite3");
    let owner_seat = SeatName::parse("owner").unwrap();
    let alice_seat = SeatName::parse("alice").unwrap();
    let op = Principal::Operator {
        seat: owner_seat.clone(),
    };
    let owner_as_seat = Principal::Seat { seat: owner_seat };
    let alice = Principal::Seat { seat: alice_seat };
    let legacy_room = RoomId::Direct {
        pair: PrincipalPair::new(op.clone(), alice.clone()).unwrap(),
    };
    let canon_room = RoomId::Direct {
        pair: PrincipalPair::new(owner_as_seat, alice.clone()).unwrap(),
    };
    {
        let j = Journal::open(&db_path).unwrap();
        let mut w = j
            .claim(StreamId::Room {
                room: legacy_room.clone(),
            })
            .unwrap();
        w.append(
            &op,
            UnixMs(1_000),
            None,
            vec![
                rex_wave_api::Event::Room(RoomEvent::Opened {
                    id: legacy_room.clone(),
                    origin: RoomOrigin::Manual,
                    topic: None,
                }),
                rex_wave_api::Event::Room(RoomEvent::Posted {
                    body: Body::parse("seed legacy").unwrap(),
                    mentions: Mentions::default(),
                    re: None,
                    after: None,
                    labels: Vec::new(),
                    task: None,
                    idem: None,
                    origin: PostOrigin::Direct,
                }),
            ],
        )
        .unwrap();
    }

    let journal = Journal::open(&db_path).unwrap();
    let hub = rex_wave_runtime::Hub::new(64);
    let sidebar = rex_wave_runtime::SidebarProjection::spawn(journal.clone(), hub.clone());
    let read = std::sync::Arc::new(rex_wave_runtime::ReadSide {
        journal: journal.clone(),
        hub: hub.clone(),
        sidebar,
        agents: std::sync::Arc::new(rex_wave_runtime::NoAgentFeed),
    });
    let mut r = rex_wave_runtime::Router::unavailable_with(read);
    r.room = rex_wave_runtime::Registry::<rex_wave_rooms::Rooms>::new(journal.clone(), hub);
    let router = std::sync::Arc::new(r);

    let alice_client = LocalClient::new(router.clone(), alice);
    let denied_mirror = alice_client
        .call(RoomPost::mirrored_turn(
            RoomRef::Id {
                id: canon_room.clone(),
            },
            Body::parse("spoof").unwrap(),
            IdemKey::parse("spoof-1").unwrap(),
        ))
        .await;
    assert!(matches!(
        denied_mirror,
        Err(rex_wave_api::ApiError::Denied { .. })
    ));

    let post_receipt = alice_client
        .call(RoomPost::new(
            RoomRef::Id {
                id: canon_room.clone(),
            },
            Body::parse("explicit id post").unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(post_receipt.room, canon_room);

    let read_via_ref = alice_client
        .call(
            Read::room_ref(
                RoomRef::Id {
                    id: canon_room.clone(),
                },
                alice_client.me(),
                Seq::ORIGIN,
                None,
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        read_via_ref.entries.len(),
        4,
        "Read by explicit canonical RoomRef::Id sees Opened + 2 MemberJoined + Posted written to that exact ID"
    );

    let mut filtered_wait = Wait::room_filtered(canon_room.clone(), Seq::ORIGIN, None, Vec::new());
    filtered_wait.timeout_ms = Some(0);
    let wait_via_ref = alice_client.call(filtered_wait).await.unwrap();
    assert_eq!(
        wait_via_ref.entries.len(),
        4,
        "RoomFiltered Wait by explicit canonical RoomId sees the exact entries written to that ID"
    );

    let raw_canon = alice_client
        .call(Read::stream(
            StreamId::Room {
                room: canon_room.clone(),
            },
            Seq::ORIGIN,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(raw_canon.entries.len(), 4);
    let mut exact_wait = Wait::new(
        Interest::Streams {
            streams: vec![StreamId::Room { room: canon_room }],
        },
        rex_wave_api::Cursor::default(),
    );
    exact_wait.timeout_ms = Some(0);
    let raw_wait = alice_client.call(exact_wait).await.unwrap();
    assert_eq!(raw_wait.entries.len(), 4);
}
