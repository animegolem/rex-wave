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
    SidebarProjection, Unavailable,
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
                            by: ctx.by.clone(),
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
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Arc::new(Unavailable(Family::Presence)),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
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
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Arc::new(Unavailable(Family::Presence)),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
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
        observation: Observation::Activity {
            activity: Activity::Refused {
                reason: "cyber_safeguard: blocked".into(),
            },
        },
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
