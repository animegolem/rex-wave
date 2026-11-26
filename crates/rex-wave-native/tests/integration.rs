use futures_util::StreamExt;
use rex_wave_api::read::{AgentEventKind, ThreadPageView};
use rex_wave_api::room::Body;
use rex_wave_api::wave::{
    Activity, AgentSpec, BaseBranch, InteractionKind, InteractionResolution, PermissionMode,
    SteerDeliveryMode, WaveFinish, WaveOutcome, WaveResolve, WaveRetireWorker, WaveSnapshot,
    WaveSpec, WaveStart, WaveTell, WaveView, WorkerPhase, WorkerSpec, Workspace,
};
use rex_wave_api::{
    Client, Frame, HostSeq, HostThreadId, ModelId, Principal, ProjectId, ProviderId, RoleName,
    RunnerKind, SeatName, SystemPart, WaveSlug,
};
use rex_wave_board::Board;
use rex_wave_mailbridge::{MailBridge, Presence, SandboxRoot};
use rex_wave_native::{
    ClaudeBridge, ClaudeFeed, ClaudeHost, ClaudeRunnerConfig, CodexBridge, CodexFeed, CodexHost,
    CodexRunnerConfig,
};
use rex_wave_notify::Inbox;
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    AgentFeed, ConfigService, FeedRouter, Hub, LocalClient, ReadSide, Registry, Router,
    SidebarProjection, ThreadIdResolver, spawn_reactor,
};
use rex_wave_store::Journal;
use rex_wave_transport::read_worker_token_file;
use rex_wave_waves::{AgentHost, HostRouter, WaveConductor, Waves};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct TestHarness {
    _tmp: tempfile::TempDir,
    project_dir: PathBuf,
    state_dir: PathBuf,
    socket_path: PathBuf,
    claude: Arc<ClaudeBridge>,
    codex: Arc<CodexBridge>,
    router: Arc<Router>,
    operator: LocalClient,
    _conductor: tokio::task::JoinHandle<()>,
}

impl TestHarness {
    async fn new(claude_bin: PathBuf, codex_bin: PathBuf) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let project_dir = tmp.path().join("repo");
        std::fs::create_dir_all(&project_dir).unwrap();
        init_git_repo(&project_dir);

        let state_dir = tmp.path().join("state");
        rex_wave_fs::create_missing_private_dir(&state_dir).unwrap();
        let socket_path = state_dir.join("daemon.sock");
        let journal_path = state_dir.join("journal.sqlite3");

        let sandbox_dir = tmp.path().join("sandbox");
        std::fs::create_dir_all(&sandbox_dir).unwrap();
        rex_wave_fs::write_atomic_path(
            &sandbox_dir.join(".rex-wave-sandbox"),
            b"ok\n",
            0o600,
            false,
        )
        .unwrap();
        let sandbox = SandboxRoot::verify(&sandbox_dir).unwrap();

        let journal = Journal::open(&journal_path).unwrap();
        let hub = Hub::new(1024);
        let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
        let mail_writer = journal.claim(rex_wave_api::StreamId::Mail).unwrap();

        let claude = ClaudeBridge::new(
            ClaudeRunnerConfig::from_env()
                .with_bin(claude_bin)
                .with_state_dir(&state_dir)
                .with_socket_path(&socket_path)
                .with_rex_wave_bin("/usr/local/bin/rex-wave-mock")
                .with_bb_data_dir(None)
                .with_retire_grace(Duration::from_millis(250)),
        );
        let codex = CodexBridge::new(
            CodexRunnerConfig::from_env()
                .with_bin(codex_bin)
                .with_state_dir(&state_dir)
                .with_socket_path(&socket_path)
                .with_bb_data_dir(None)
                .with_retire_grace(Duration::from_millis(250))
                .with_rpc_timeout(Duration::from_secs(10)),
        );

        let thread_resolver: ThreadIdResolver = {
            let journal = journal.clone();
            Arc::new(move |wave: &WaveSlug, role: &RoleName| {
                let journal = journal.clone();
                let stream = rex_wave_api::StreamId::Wave { wave: wave.clone() };
                let role = role.clone();
                Box::pin(async move {
                    let mut after = rex_wave_api::Seq::ORIGIN;
                    let mut latest = None;
                    while let Ok(page) = journal.read(&stream, after, 512) {
                        if page.is_empty() {
                            break;
                        }
                        for env in page {
                            after = env.seq;
                            if let rex_wave_api::Event::Wave(
                                rex_wave_api::wave::WaveEvent::WorkerObserved {
                                    role: r,
                                    observation:
                                        rex_wave_api::wave::Observation::Spawned { thread, .. },
                                },
                            ) = env.event
                                && r == role
                            {
                                latest = Some(thread);
                            }
                        }
                    }
                    latest
                })
            })
        };

        let feed: Arc<dyn AgentFeed> = Arc::new(
            FeedRouter::new([
                (
                    RunnerKind::Claude,
                    Arc::new(ClaudeFeed(claude.clone())) as Arc<dyn AgentFeed>,
                ),
                (
                    RunnerKind::Codex,
                    Arc::new(CodexFeed(codex.clone())) as Arc<dyn AgentFeed>,
                ),
            ])
            .with_thread_resolver(thread_resolver),
        );

        let config_svc = ConfigService::new(journal.clone(), hub.clone()).unwrap();
        let wave_svc = rex_wave_waves::WaveService::new(
            Registry::<Waves>::new(journal.clone(), hub.clone()),
            RunnerKind::Claude,
            Some(config_svc.subscribe()),
        )
        .with_state_dir(state_dir.clone());

        let router = Arc::new(Router {
            room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
            wave: wave_svc.clone(),
            board: Registry::<Board>::new(journal.clone(), hub.clone()),
            inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
            mail: Arc::new(MailBridge::with_hub(
                sandbox,
                journal.clone(),
                mail_writer,
                hub.clone(),
            )),
            presence: Presence::new().registry(journal.clone(), hub.clone()),
            config: config_svc,
            read: Arc::new(ReadSide {
                journal,
                hub,
                sidebar,
                agents: feed,
            }),
        });

        claude.attach_router(router.clone());
        codex.attach_router(router.clone());

        let host_router = Arc::new(
            HostRouter::new(RunnerKind::Claude)
                .with_host(RunnerKind::Claude, ClaudeHost(claude.clone()))
                .with_host(RunnerKind::Codex, CodexHost(codex.clone())),
        );
        wave_svc.bind_host(host_router.clone());

        let conductor = spawn_reactor(
            WaveConductor::new(host_router),
            LocalClient::new(
                router.clone(),
                Principal::System {
                    part: SystemPart::WaveConductor,
                },
            ),
        );

        let operator = LocalClient::new(
            router.clone(),
            Principal::Operator {
                seat: SeatName::parse("owner").unwrap(),
            },
        );

        Self {
            _tmp: tmp,
            project_dir,
            state_dir,
            socket_path,
            claude,
            codex,
            router,
            operator,
            _conductor: conductor,
        }
    }

    async fn wait_wave<F>(&self, wave: &WaveSlug, pred: F) -> WaveSnapshot
    where
        F: Fn(&WaveSnapshot) -> bool,
    {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut last = None;
        while Instant::now() < deadline {
            if let Ok(snap) = self.operator.call(WaveView { wave: wave.clone() }).await {
                if pred(&snap) {
                    return snap;
                }
                last = Some(snap);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for wave {wave} state; last={last:?}");
    }

    async fn wait_page<F>(&self, wave: &WaveSlug, role: &RoleName, pred: F) -> ThreadPageView
    where
        F: Fn(&ThreadPageView) -> bool,
    {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut last = None;
        while Instant::now() < deadline {
            if let Ok(page) = self.router.read.agents.page(wave, role, None, 200).await {
                if pred(&page) {
                    return page;
                }
                last = Some(page);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for feed page @{wave}/{role}; last={last:?}");
    }
}

fn init_git_repo(dir: &Path) {
    let run = |args: &[&str]| {
        let mut cmd = StdCommand::new("git");
        for key in [
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
            "GIT_OBJECT_DIRECTORY",
            "GIT_COMMON_DIR",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            "GIT_PREFIX",
            "GIT_CEILING_DIRECTORIES",
        ] {
            cmd.env_remove(key);
        }
        let out = cmd.arg("-C").arg(dir).args(args).output().unwrap();
        assert!(
            out.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run(&["init", "-q"]);
    run(&["config", "user.email", "test@example.com"]);
    run(&["config", "user.name", "Test Bot"]);
    rex_wave_fs::write_atomic_path(&dir.join("README.md"), b"demo\n", 0o644, false).unwrap();
    run(&["add", "README.md"]);
    run(&["commit", "-q", "-m", "initial"]);
}

fn write_executable_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    rex_wave_fs::write_atomic_path(&path, body.as_bytes(), 0o755, false).unwrap();
    let mut perms = std::fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&path, perms).unwrap();
    path
}

fn process_alive(pid: i32) -> bool {
    let Some(p) = rustix::process::Pid::from_raw(pid) else {
        return false;
    };
    !matches!(
        rustix::process::test_kill_process(p),
        Err(rustix::io::Errno::SRCH)
    )
}

fn worker_phase<'a>(snap: &'a WaveSnapshot, role: &RoleName) -> Option<&'a WorkerPhase> {
    snap.workers
        .iter()
        .find(|(r, _, _)| r == role)
        .map(|(_, _, p)| p)
}

fn wait_for_pid_file(path: &Path) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok(text) = rex_wave_fs::read_regular_string(path, 4096)
            && let Ok(pid) = text.trim().parse::<i32>()
        {
            return pid;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for pid file {}", path.display());
}

#[tokio::test]
async fn claude_end_to_end_streaming_approval_tell_resume_and_retire() {
    let script_dir = tempfile::tempdir().unwrap();
    let args_log = script_dir.path().join("claude_args.jsonl");
    let pid_file = script_dir.path().join("grandchild.pid");
    let first_exited = script_dir.path().join("first_exited");

    let claude_script = format!(
        r#"#!/usr/bin/env python3
import json, os, signal, subprocess, sys, time

args_log = {args_log:?}
pid_file = {pid_file:?}
first_exited = {first_exited:?}

with open(args_log, "a") as f:
    f.write(json.dumps({{
        "argv": sys.argv[1:],
        "cwd": os.getcwd(),
        "thread_id": os.environ.get("REX_WAVE_WORKER_THREAD_ID"),
        "token": os.environ.get("REX_WAVE_TOKEN"),
        "socket": os.environ.get("REX_WAVE_SOCKET"),
    }}) + "\n")

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

resumed = "--resume" in sys.argv[1:]

if not resumed:
    first_line = sys.stdin.readline()
    msg1 = json.loads(first_line)
    assert msg1["type"] == "user"

    emit({{
        "type": "system",
        "subtype": "init",
        "session_id": "sess_cl_001",
        "model": "claude-opus-5-5",
        "account": "acct-primary"
    }})
    emit({{
        "type": "assistant",
        "message": {{
            "model": "claude-opus-5-5",
            "content": [
                {{"type": "text", "text": "Starting work"}},
                {{"type": "tool_use", "id": "tu_1", "name": "Bash", "input": {{"command": "ls -la"}}}}
            ]
        }}
    }})
    emit({{
        "type": "control_request",
        "request_id": "perm_1",
        "request": {{
            "subtype": "can_use_tool",
            "tool_name": "Bash",
            "input": {{"command": "ls -la"}},
            "decision_reason": "List workspace files"
        }}
    }})

    ctrl_line = sys.stdin.readline()
    ctrl = json.loads(ctrl_line)
    assert ctrl["type"] == "control_response"
    inner = ctrl["response"]["response"]
    assert inner["behavior"] == "allow"
    assert inner["updatedInput"] == {{"command": "ls -la"}}

    emit({{
        "type": "user",
        "message": {{
            "role": "user",
            "content": [
                {{"type": "tool_result", "tool_use_id": "tu_1", "content": "README.md", "is_error": False}}
            ]
        }}
    }})
    emit({{
        "type": "assistant",
        "message": {{"content": [{{"type": "text", "text": "Listed workspace"}}]}}
    }})
    emit({{
        "type": "result",
        "subtype": "success",
        "is_error": False,
        "session_id": "sess_cl_001",
        "total_cost_usd": 0.042,
        "usage": {{"input_tokens": 1200, "cache_read_input_tokens": 300, "output_tokens": 250}},
        "modelUsage": {{"claude-opus-5-5": {{"contextWindow": 200000, "costUSD": 0.042}}}}
    }})

    second_line = sys.stdin.readline()
    msg2 = json.loads(second_line)
    assert "live followup" in msg2["message"]["content"][0]["text"]
    emit({{
        "type": "assistant",
        "message": {{"content": [{{"type": "text", "text": "Handled live followup"}}]}}
    }})
    emit({{
        "type": "result",
        "subtype": "success",
        "is_error": False,
        "session_id": "sess_cl_001",
        "total_cost_usd": 0.055,
        "usage": {{"input_tokens": 1400, "output_tokens": 300}},
        "modelUsage": {{"claude-opus-5-5": {{"contextWindow": 200000, "costUSD": 0.055}}}}
    }})
    with open(first_exited, "w") as f:
        f.write("done\n")
    sys.exit(0)
else:
    # Spawn a grandchild ignoring SIGTERM to verify process-group SIGKILL reaping.
    gc = subprocess.Popen(
        [sys.executable, "-c", "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(120)"]
    )
    with open(pid_file, "w") as f:
        f.write(str(gc.pid) + "\n")

    line = sys.stdin.readline()
    msg = json.loads(line)
    assert "after clean exit" in msg["message"]["content"][0]["text"]
    emit({{
        "type": "system",
        "subtype": "init",
        "session_id": "sess_cl_001",
        "model": "claude-opus-5-5"
    }})
    emit({{
        "type": "assistant",
        "message": {{"content": [{{"type": "text", "text": "Resumed cleanly"}}]}}
    }})
    emit({{
        "type": "result",
        "subtype": "success",
        "is_error": False,
        "session_id": "sess_cl_001",
        "usage": {{"input_tokens": 1500, "output_tokens": 320}},
        "modelUsage": {{"claude-opus-5-5": {{"contextWindow": 200000}}}}
    }})
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True:
        time.sleep(1)
"#,
        args_log = args_log.to_string_lossy(),
        pid_file = pid_file.to_string_lossy(),
        first_exited = first_exited.to_string_lossy(),
    );

    let claude_bin = write_executable_script(script_dir.path(), "mock-claude", &claude_script);
    let noop_codex =
        write_executable_script(script_dir.path(), "mock-codex", "#!/bin/sh\nexit 1\n");
    let h = TestHarness::new(claude_bin, noop_codex).await;

    let wave = WaveSlug::parse("w-claude").unwrap();
    let role = RoleName::parse("author").unwrap();
    let spec = WaveSpec {
        slug: wave.clone(),
        project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
        goal: Body::parse("Verify native Claude stream-json runner").unwrap(),
        defaults: AgentSpec {
            provider: ProviderId::parse("claude:claude-code").unwrap(),
            model: Some(ModelId::parse("claude-opus-5-5").unwrap()),
            account: Some("acct-primary".into()),
            reasoning: Some("high".into()),
            service_tier: None,
            permission: PermissionMode::AcceptEdits,
            workspace: Workspace::ManagedWorktree {
                base: BaseBranch::Default,
            },
        },
        output_dir: None,
        workers: vec![WorkerSpec {
            role: role.clone(),
            brief: Body::parse("Inspect repo and report").unwrap(),
            agent: None,
            output_dir: None,
            writes: Vec::new(),
            return_schema: None,
        }],
    };

    h.operator
        .call(WaveStart {
            spec,
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .unwrap();

    // Wait until blocked on control_request `perm_1`.
    let blocked_snap = h
        .wait_wave(&wave, |snap| {
            matches!(
                worker_phase(snap, &role),
                Some(WorkerPhase::Live {
                    activity: Activity::Blocked { interaction },
                    ..
                }) if interaction.id.as_str() == "perm_1"
                    && matches!(interaction.kind, InteractionKind::Permission)
            )
        })
        .await;

    let expected_tid = HostThreadId::parse("claude:w-claude:author:a1").unwrap();
    assert!(matches!(
        worker_phase(&blocked_snap, &role),
        Some(WorkerPhase::Live { thread, .. }) if thread == &expected_tid
    ));

    let meta = blocked_snap.worker_meta.get(&role).unwrap();
    assert_eq!(
        meta.actual_model.as_ref().map(ModelId::as_str),
        Some("claude-opus-5-5")
    );
    assert_eq!(meta.actual_account.as_deref(), Some("acct-primary"));

    // Verify mode-0600 worker token file exists.
    let token = read_worker_token_file(&h.state_dir, expected_tid.as_str())
        .unwrap()
        .expect("worker token file must be provisioned on spawn");
    assert!(token.starts_with("rw_w_"));

    // Open live AgentFeed stream and verify streaming backlog + live delivery.
    let mut live_stream = h.router.read.agents.live(&wave, &role).await.unwrap();
    let mut saw_start_text = false;
    let mut saw_tool_call = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while (!saw_start_text || !saw_tool_call) && Instant::now() < deadline {
        if let Ok(Some(Ok(Frame::Agent { event, .. }))) =
            tokio::time::timeout(Duration::from_secs(1), live_stream.next()).await
        {
            match event.kind {
                AgentEventKind::AssistantText { ref text } if text == "Starting work" => {
                    saw_start_text = true;
                }
                AgentEventKind::ToolCall { ref name, .. } if name == "Bash" => {
                    saw_tool_call = true;
                }
                _ => {}
            }
        }
    }
    assert!(saw_start_text && saw_tool_call);

    // Resolve the permission prompt.
    h.operator
        .call(WaveResolve {
            wave: wave.clone(),
            role: role.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("perm_1").unwrap(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        })
        .await
        .unwrap();

    // Wait for turn completion -> Activity::Idle and check usage + tool result.
    h.wait_wave(&wave, |snap| {
        matches!(
            worker_phase(snap, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            })
        )
    })
    .await;

    let page = h
        .wait_page(&wave, &role, |p| {
            p.usage.as_ref().is_some_and(|u| u.used_tokens == 1750)
                && p.events.iter().any(|e| {
                    matches!(
                        &e.kind,
                        AgentEventKind::ToolResult { name, ok: true, summary }
                            if name == "Bash" && summary.contains("README.md")
                    )
                })
        })
        .await;
    let usage = page.usage.unwrap();
    assert_eq!(usage.model_context_window, 200_000);
    assert_eq!(usage.cost_usd, Some(0.042));

    // Tell #1 over live stdin.
    h.operator
        .call(WaveTell {
            wave: wave.clone(),
            role: role.clone(),
            body: Body::parse("live followup").unwrap(),
            mode: SteerDeliveryMode::Auto,
        })
        .await
        .unwrap();

    h.wait_page(&wave, &role, |p| {
        p.events.iter().any(|e| {
            matches!(&e.kind, AgentEventKind::AssistantText { text } if text == "Handled live followup")
        })
    })
    .await;

    // Wait until the first Claude process exits cleanly so tell #2 must respawn with --resume.
    let exit_deadline = Instant::now() + Duration::from_secs(5);
    while !first_exited.exists() && Instant::now() < exit_deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        first_exited.exists(),
        "first mock process should exit cleanly"
    );
    tokio::time::sleep(Duration::from_millis(80)).await;

    h.operator
        .call(WaveTell {
            wave: wave.clone(),
            role: role.clone(),
            body: Body::parse("after clean exit").unwrap(),
            mode: SteerDeliveryMode::Auto,
        })
        .await
        .unwrap();

    h.wait_page(&wave, &role, |p| {
        p.events.iter().any(|e| {
            matches!(&e.kind, AgentEventKind::AssistantText { text } if text == "Resumed cleanly")
        })
    })
    .await;

    h.wait_wave(&wave, |snap| {
        matches!(
            worker_phase(snap, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            })
        )
    })
    .await;

    let grandchild_pid = wait_for_pid_file(&pid_file);
    assert!(
        process_alive(grandchild_pid),
        "grandchild must be alive before retire"
    );

    // Retire worker -> revokes token, SIGTERM -> grace -> SIGKILL process group, emits Archived.
    h.operator
        .call(WaveRetireWorker {
            wave: wave.clone(),
            role: role.clone(),
            reason: Some("done".into()),
        })
        .await
        .unwrap();

    let retired_snap = h
        .wait_wave(&wave, |snap| {
            matches!(
                worker_phase(snap, &role),
                Some(WorkerPhase::Retired {
                    thread: Some(t),
                    errored: None,
                    refused: None,
                }) if t == &expected_tid
            )
        })
        .await;
    assert_eq!(retired_snap.counts.retired_clean, 1);

    assert_eq!(
        read_worker_token_file(&h.state_dir, expected_tid.as_str()).unwrap(),
        None,
        "worker token file must be unlinked on retire"
    );

    let reap_deadline = Instant::now() + Duration::from_secs(3);
    while process_alive(grandchild_pid) && Instant::now() < reap_deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        !process_alive(grandchild_pid),
        "SIGTERM-ignoring grandchild must be reaped via process-group SIGKILL"
    );

    // Inspect recorded CLI invocations (initial + resumed).
    let raw_log = rex_wave_fs::read_regular_string(&args_log, 64 * 1024).unwrap();
    let invocations: Vec<serde_json::Value> = raw_log
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(invocations.len(), 2);

    let first_argv: Vec<&str> = invocations[0]["argv"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(first_argv.contains(&"-p"));
    assert!(first_argv.contains(&"--input-format"));
    assert!(first_argv.contains(&"stream-json"));
    assert!(first_argv.contains(&"--output-format"));
    assert!(first_argv.contains(&"--verbose"));
    assert!(first_argv.contains(&"--model"));
    assert!(first_argv.contains(&"claude-opus-5-5"));
    assert!(first_argv.contains(&"--effort"));
    assert!(first_argv.contains(&"high"));
    assert!(first_argv.contains(&"--permission-mode"));
    assert!(first_argv.contains(&"acceptEdits"));
    assert!(first_argv.contains(&"--permission-prompt-tool"));
    assert!(first_argv.contains(&"stdio"));
    assert!(first_argv.contains(&"--mcp-config"));
    assert_eq!(
        invocations[0]["thread_id"].as_str(),
        Some(expected_tid.as_str())
    );
    assert_eq!(invocations[0]["token"].as_str(), Some(token.as_str()));
    assert_eq!(
        invocations[0]["socket"].as_str(),
        Some(h.socket_path.to_string_lossy().as_ref())
    );
    let expected_wt_suffix = ".worktrees/rex-wave/w-claude/author";
    assert!(
        invocations[0]["cwd"]
            .as_str()
            .unwrap()
            .ends_with(expected_wt_suffix),
        "unexpected worktree cwd: {}",
        invocations[0]["cwd"]
    );

    let second_argv: Vec<&str> = invocations[1]["argv"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    let resume_idx = second_argv
        .iter()
        .position(|a| *a == "--resume")
        .expect("resumed invocation must pass --resume");
    assert_eq!(second_argv.get(resume_idx + 1), Some(&"sess_cl_001"));

    h.claude.shutdown().await;
}

#[tokio::test]
async fn claude_refusal_error_deny_and_full_permission_mode() {
    let script_dir = tempfile::tempdir().unwrap();
    let args_log = script_dir.path().join("claude_full_args.jsonl");

    let claude_script = format!(
        r#"#!/usr/bin/env python3
import json, sys

with open({args_log:?}, "a") as f:
    f.write(json.dumps(sys.argv[1:]) + "\n")

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

line = sys.stdin.readline()
msg = json.loads(line)
text = msg["message"]["content"][0]["text"]

if "REFUSE_ME" in text:
    emit({{"type": "system", "subtype": "init", "session_id": "s_ref"}})
    emit({{"type": "control_request", "request_id": "deny_1", "request": {{"subtype": "can_use_tool", "tool_name": "Bash", "input": {{"command": "bad"}}}}}})
    ctrl = json.loads(sys.stdin.readline())
    assert ctrl["response"]["response"]["behavior"] == "deny"
    emit({{"type": "result", "subtype": "error_during_execution", "stop_reason": "refusal", "is_error": True, "result": "Request declined by safety policy"}})
elif "ERROR_ME" in text:
    emit({{"type": "system", "subtype": "init", "session_id": "s_err"}})
    emit({{"type": "result", "subtype": "error_during_execution", "is_error": True, "error": "upstream timeout"}})
"#,
        args_log = args_log.to_string_lossy(),
    );

    let claude_bin = write_executable_script(script_dir.path(), "mock-claude-err", &claude_script);
    let noop_codex =
        write_executable_script(script_dir.path(), "mock-codex", "#!/bin/sh\nexit 1\n");
    let h = TestHarness::new(claude_bin, noop_codex).await;

    let wave = WaveSlug::parse("w-cl-cls").unwrap();
    let r_ref = RoleName::parse("refuser").unwrap();
    let r_err = RoleName::parse("errorer").unwrap();
    let spec = WaveSpec {
        slug: wave.clone(),
        project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
        goal: Body::parse("Verify Claude refusal and error classification").unwrap(),
        defaults: AgentSpec {
            provider: ProviderId::parse("claude:claude-code").unwrap(),
            model: None,
            account: None,
            reasoning: None,
            service_tier: None,
            permission: PermissionMode::AcceptEdits,
            workspace: Workspace::ProjectDefault,
        },
        output_dir: None,
        workers: vec![
            WorkerSpec {
                role: r_ref.clone(),
                brief: Body::parse("REFUSE_ME").unwrap(),
                agent: None,
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            },
            WorkerSpec {
                role: r_err.clone(),
                brief: Body::parse("ERROR_ME").unwrap(),
                agent: Some(AgentSpec {
                    provider: ProviderId::parse("claude:claude-code").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Full,
                    workspace: Workspace::ProjectDefault,
                }),
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            },
        ],
    };

    h.operator
        .call(WaveStart {
            spec,
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .unwrap();

    // Wait for deny_1 permission prompt, then Deny it.
    h.wait_wave(&wave, |snap| {
        matches!(
            worker_phase(snap, &r_ref),
            Some(WorkerPhase::Live {
                activity: Activity::Blocked { interaction },
                ..
            }) if interaction.id.as_str() == "deny_1"
        )
    })
    .await;

    h.operator
        .call(WaveResolve {
            wave: wave.clone(),
            role: r_ref.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("deny_1").unwrap(),
            resolution: InteractionResolution::Deny,
        })
        .await
        .unwrap();

    let snap = h
        .wait_wave(&wave, |s| {
            matches!(
                worker_phase(s, &r_ref),
                Some(WorkerPhase::Live {
                    activity: Activity::Refused { .. },
                    ..
                })
            ) && matches!(
                worker_phase(s, &r_err),
                Some(WorkerPhase::Live {
                    activity: Activity::Errored { .. },
                    ..
                })
            )
        })
        .await;
    assert_eq!(snap.counts.refused, 1);
    assert_eq!(snap.counts.errored, 1);

    // WaveFinish(Completed) without allow_incomplete must reject both refused and errored workers.
    let finish_err = h
        .operator
        .call(WaveFinish {
            wave: wave.clone(),
            outcome: WaveOutcome::Completed {
                summary: Body::parse("done").unwrap(),
            },
            allow_incomplete: false,
        })
        .await
        .unwrap_err();
    let msg = finish_err.to_string();
    assert!(msg.contains("@refuser"), "unexpected err: {msg}");
    assert!(msg.contains("@errorer"), "unexpected err: {msg}");

    // Verify PermissionMode::Full passed bypassPermissions and omitted --permission-prompt-tool.
    let raw_args = rex_wave_fs::read_regular_string(&args_log, 64 * 1024).unwrap();
    let full_argv_line = raw_args
        .lines()
        .find(|l| l.contains("bypassPermissions"))
        .expect("Full permission worker must pass bypassPermissions");
    assert!(!full_argv_line.contains("--permission-prompt-tool"));

    h.claude.shutdown().await;
}

#[tokio::test]
async fn codex_end_to_end_streaming_approvals_steer_usage_and_retire() {
    let script_dir = tempfile::tempdir().unwrap();
    let rpc_log = script_dir.path().join("codex_rpc.jsonl");
    let pid_file = script_dir.path().join("codex_grandchild.pid");

    let codex_script = format!(
        r#"#!/usr/bin/env python3
import json, os, signal, subprocess, sys, time

rpc_log = {rpc_log:?}
pid_file = {pid_file:?}

assert len(sys.argv) >= 2 and sys.argv[1] == "app-server", f"bad argv: {{sys.argv}}"

def log_event(obj):
    with open(rpc_log, "a") as f:
        f.write(json.dumps(obj) + "\n")

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

log_event({{
    "kind": "boot",
    "cwd": os.getcwd(),
    "thread_id": os.environ.get("REX_WAVE_WORKER_THREAD_ID"),
    "token": os.environ.get("REX_WAVE_TOKEN"),
    "socket": os.environ.get("REX_WAVE_SOCKET"),
}})

# Spawn a SIGTERM-ignoring grandchild to verify process-group SIGKILL reaping.
gc = subprocess.Popen(
    [sys.executable, "-c", "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(120)"]
)
with open(pid_file, "w") as f:
    f.write(str(gc.pid) + "\n")

signal.signal(signal.SIGTERM, signal.SIG_IGN)

turn_count = 0
for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    msg = json.loads(raw)
    log_event(msg)
    method = msg.get("method")
    req_id = msg.get("id")
    params = msg.get("params") or {{}}

    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": req_id, "result": {{"userAgent": "mock-codex"}}}})
    elif method == "initialized":
        pass
    elif method == "account/read":
        assert params.get("refreshToken") is False
        send({{
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {{
                "account": {{"type": "chatgpt", "email": "acct-codex", "planType": "pro"}},
                "requiresOpenaiAuth": True
            }}
        }})
    elif method == "thread/start":
        assert params.get("approvalPolicy") == "on-request"
        assert params.get("sandbox") == "workspace-write"
        assert params.get("model") == "gpt-6.1-sol"
        send({{
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {{
                "thread": {{"id": "cx_th_1"}},
                "model": "gpt-6.1-sol",
                "account": "acct-codex",
                "cwd": params.get("cwd"),
                "approvalPolicy": "on-request",
                "sandbox": "workspace-write"
            }}
        }})
    elif method == "turn/start":
        turn_count += 1
        tid = f"turn_{{turn_count}}"
        assert params.get("threadId") == "cx_th_1"
        assert params["input"][0]["type"] == "text"
        send({{"jsonrpc": "2.0", "id": req_id, "result": {{"turn": {{"id": tid, "status": "inProgress"}}}}}})
        send({{"jsonrpc": "2.0", "method": "turn/started", "params": {{"threadId": "cx_th_1", "turn": {{"id": tid, "status": "inProgress"}}}}}})

        if turn_count == 1:
            assert params.get("effort") == "xhigh"
            send({{
                "jsonrpc": "2.0",
                "method": "item/started",
                "params": {{
                    "threadId": "cx_th_1",
                    "turnId": tid,
                    "startedAtMs": 1000,
                    "item": {{"type": "commandExecution", "id": "cmd_1", "command": "cargo test", "status": "inProgress"}}
                }}
            }})
            send({{
                "jsonrpc": "2.0",
                "id": 901,
                "method": "item/commandExecution/requestApproval",
                "params": {{
                    "threadId": "cx_th_1",
                    "turnId": tid,
                    "itemId": "cmd_1",
                    "command": "cargo test",
                    "reason": "Run workspace tests",
                    "startedAtMs": 1001
                }}
            }})
    elif method is None and req_id == 901:
        assert msg["result"]["decision"] == "accept"
        send({{
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {{
                "threadId": "cx_th_1",
                "turnId": "turn_1",
                "completedAtMs": 1005,
                "item": {{"type": "commandExecution", "id": "cmd_1", "command": "cargo test", "status": "completed", "exitCode": 0, "aggregatedOutput": "all green"}}
            }}
        }})
        send({{
            "jsonrpc": "2.0",
            "id": 902,
            "method": "item/fileChange/requestApproval",
            "params": {{
                "threadId": "cx_th_1",
                "turnId": "turn_1",
                "itemId": "fc_1",
                "reason": "Patch src/lib.rs",
                "startedAtMs": 1006
            }}
        }})
    elif method is None and req_id == 902:
        assert msg["result"]["decision"] == "acceptForSession"
        send({{
            "jsonrpc": "2.0",
            "method": "item/started",
            "params": {{
                "threadId": "cx_th_1",
                "turnId": "turn_1",
                "startedAtMs": 1007,
                "item": {{"type": "fileChange", "id": "fc_1", "changes": [{{"path": "src/lib.rs"}}], "status": "inProgress"}}
            }}
        }})
        send({{
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {{
                "threadId": "cx_th_1",
                "turnId": "turn_1",
                "completedAtMs": 1008,
                "item": {{"type": "fileChange", "id": "fc_1", "changes": [{{"path": "src/lib.rs"}}], "status": "completed"}}
            }}
        }})
        send({{
            "jsonrpc": "2.0",
            "method": "item/agentMessage/delta",
            "params": {{"threadId": "cx_th_1", "turnId": "turn_1", "itemId": "am_1", "delta": "Hello "}}
        }})
        send({{
            "jsonrpc": "2.0",
            "method": "item/agentMessage/delta",
            "params": {{"threadId": "cx_th_1", "turnId": "turn_1", "itemId": "am_1", "delta": "world"}}
        }})
        send({{
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {{
                "threadId": "cx_th_1",
                "turnId": "turn_1",
                "completedAtMs": 1009,
                "item": {{"type": "agentMessage", "id": "am_1", "text": "Hello world"}}
            }}
        }})
        send({{
            "jsonrpc": "2.0",
            "method": "thread/tokenUsage/updated",
            "params": {{
                "threadId": "cx_th_1",
                "turnId": "turn_1",
                "tokenUsage": {{
                    "modelContextWindow": 256000,
                    "total": {{
                        "inputTokens": 1000,
                        "cachedInputTokens": 200,
                        "outputTokens": 300,
                        "reasoningOutputTokens": 100,
                        "totalTokens": 1600
                    }},
                    "last": {{
                        "inputTokens": 1000,
                        "cachedInputTokens": 200,
                        "outputTokens": 300,
                        "reasoningOutputTokens": 100,
                        "totalTokens": 1600
                    }}
                }}
            }}
        }})
    elif method == "turn/steer":
        assert params.get("expectedTurnId") == "turn_1"
        assert "pivot now" in params["input"][0]["text"]
        send({{"jsonrpc": "2.0", "id": req_id, "result": {{}}}})
        send({{
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {{"threadId": "cx_th_1", "turn": {{"id": "turn_1", "status": "completed"}}}}
        }})
    elif method == "turn/interrupt":
        send({{"jsonrpc": "2.0", "id": req_id, "result": {{}}}})
        # Keep running ignoring SIGTERM so retire exercises bounded grace + SIGKILL.
        while True:
            time.sleep(1)
"#,
        rpc_log = rpc_log.to_string_lossy(),
        pid_file = pid_file.to_string_lossy(),
    );

    let noop_claude =
        write_executable_script(script_dir.path(), "mock-claude", "#!/bin/sh\nexit 1\n");
    let codex_bin = write_executable_script(script_dir.path(), "mock-codex", &codex_script);
    let h = TestHarness::new(noop_claude, codex_bin).await;

    let wave = WaveSlug::parse("w-codex").unwrap();
    let role = RoleName::parse("author").unwrap();
    let spec = WaveSpec {
        slug: wave.clone(),
        project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
        goal: Body::parse("Verify native Codex app-server runner").unwrap(),
        defaults: AgentSpec {
            provider: ProviderId::parse("codex:codex").unwrap(),
            model: Some(ModelId::parse("gpt-6.1-sol").unwrap()),
            account: Some("acct-codex".into()),
            reasoning: Some("xhigh".into()),
            service_tier: None,
            permission: PermissionMode::AcceptEdits,
            workspace: Workspace::ManagedWorktree {
                base: BaseBranch::Default,
            },
        },
        output_dir: None,
        workers: vec![WorkerSpec {
            role: role.clone(),
            brief: Body::parse("Run tests and patch lib").unwrap(),
            agent: None,
            output_dir: None,
            writes: Vec::new(),
            return_schema: None,
        }],
    };

    h.operator
        .call(WaveStart {
            spec,
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .unwrap();

    // 1. Wait for commandExecution approval prompt.
    let blocked1 = h
        .wait_wave(&wave, |snap| {
            matches!(
                worker_phase(snap, &role),
                Some(WorkerPhase::Live {
                    activity: Activity::Blocked { interaction },
                    ..
                }) if interaction.id.as_str() == "cmd_1"
            )
        })
        .await;

    let expected_tid = HostThreadId::parse("codex:w-codex:author:a1").unwrap();
    assert!(matches!(
        worker_phase(&blocked1, &role),
        Some(WorkerPhase::Live { thread, .. }) if thread == &expected_tid
    ));
    let meta = blocked1.worker_meta.get(&role).unwrap();
    assert_eq!(
        meta.actual_model.as_ref().map(ModelId::as_str),
        Some("gpt-6.1-sol")
    );
    assert_eq!(meta.actual_account.as_deref(), Some("acct-codex"));

    let token = read_worker_token_file(&h.state_dir, expected_tid.as_str())
        .unwrap()
        .expect("Codex worker token file must exist");
    assert!(token.starts_with("rw_w_"));

    // Resolve command approval (AllowOnce -> "accept").
    h.operator
        .call(WaveResolve {
            wave: wave.clone(),
            role: role.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("cmd_1").unwrap(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        })
        .await
        .unwrap();

    // 2. Wait for fileChange approval prompt and resolve with AllowForSession -> "acceptForSession".
    h.wait_wave(&wave, |snap| {
        matches!(
            worker_phase(snap, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Blocked { interaction },
                ..
            }) if interaction.id.as_str() == "fc_1"
        )
    })
    .await;

    h.operator
        .call(WaveResolve {
            wave: wave.clone(),
            role: role.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("fc_1").unwrap(),
            resolution: InteractionResolution::AllowForSession {
                granted_permissions: None,
            },
        })
        .await
        .unwrap();

    // Wait for streaming deltas + token usage while turn_1 is still active.
    let mid_page = h
        .wait_page(&wave, &role, |p| {
            p.usage.as_ref().is_some_and(|u| u.used_tokens == 1600)
        })
        .await;
    assert_eq!(
        mid_page.usage.as_ref().unwrap().model_context_window,
        256_000
    );
    let assistant_texts: Vec<&str> = mid_page
        .events
        .iter()
        .filter_map(|e| match &e.kind {
            AgentEventKind::AssistantText { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        assistant_texts,
        vec!["Hello ", "world"],
        "deltas stream without duplicate item/completed text"
    );

    // 3. Tell with Steer while turn_1 is active -> exercises turn/steer.
    h.operator
        .call(WaveTell {
            wave: wave.clone(),
            role: role.clone(),
            body: Body::parse("pivot now").unwrap(),
            mode: SteerDeliveryMode::Steer,
        })
        .await
        .unwrap();

    h.wait_wave(&wave, |snap| {
        matches!(
            worker_phase(snap, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            })
        )
    })
    .await;

    // 4. Tell while idle -> exercises turn/start (turn_2).
    h.operator
        .call(WaveTell {
            wave: wave.clone(),
            role: role.clone(),
            body: Body::parse("second turn").unwrap(),
            mode: SteerDeliveryMode::Auto,
        })
        .await
        .unwrap();

    h.wait_wave(&wave, |snap| {
        matches!(
            worker_phase(snap, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Working,
                ..
            })
        )
    })
    .await;

    let grandchild_pid = wait_for_pid_file(&pid_file);
    assert!(process_alive(grandchild_pid));

    // 5. Retire -> revokes token, sends turn/interrupt, reaps process group (including grandchild), emits Archived.
    h.operator
        .call(WaveRetireWorker {
            wave: wave.clone(),
            role: role.clone(),
            reason: Some("complete".into()),
        })
        .await
        .unwrap();

    let retired = h
        .wait_wave(&wave, |snap| {
            matches!(
                worker_phase(snap, &role),
                Some(WorkerPhase::Retired {
                    thread: Some(t),
                    errored: None,
                    refused: None,
                }) if t == &expected_tid
            )
        })
        .await;
    assert_eq!(retired.counts.retired_clean, 1);

    assert_eq!(
        read_worker_token_file(&h.state_dir, expected_tid.as_str()).unwrap(),
        None,
        "Codex worker token must be unlinked on retire"
    );

    let reap_deadline = Instant::now() + Duration::from_secs(3);
    while process_alive(grandchild_pid) && Instant::now() < reap_deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        !process_alive(grandchild_pid),
        "Codex SIGTERM-ignoring grandchild must be reaped via process-group SIGKILL"
    );

    // Verify RPC sequence including initialize, initialized, thread/start, turn/start, turn/steer, turn/interrupt.
    let raw_rpc = rex_wave_fs::read_regular_string(&rpc_log, 128 * 1024).unwrap();
    let entries: Vec<serde_json::Value> = raw_rpc
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert!(
        entries[0]["cwd"]
            .as_str()
            .unwrap()
            .ends_with(".worktrees/rex-wave/w-codex/author")
    );
    assert_eq!(entries[0]["token"].as_str(), Some(token.as_str()));
    assert_eq!(
        entries[0]["socket"].as_str(),
        Some(h.socket_path.to_string_lossy().as_ref())
    );
    let methods: Vec<&str> = entries
        .iter()
        .filter_map(|e| e.get("method").and_then(|v| v.as_str()))
        .collect();
    assert!(methods.contains(&"initialize"));
    assert!(methods.contains(&"initialized"));
    assert!(methods.contains(&"account/read"));
    assert!(methods.contains(&"thread/start"));
    assert!(methods.contains(&"turn/start"));
    assert!(methods.contains(&"turn/steer"));
    assert!(methods.contains(&"turn/interrupt"));

    // Verify after= cursor paging on CodexFeed.
    let full_page = h
        .router
        .read
        .agents
        .page(&wave, &role, None, 200)
        .await
        .unwrap();
    assert!(full_page.events.len() >= 4);
    let tail_page = h
        .router
        .read
        .agents
        .page(&wave, &role, Some(HostSeq(2)), 200)
        .await
        .unwrap();
    assert!(tail_page.events.iter().all(|e| e.seq > HostSeq(2)));

    h.codex.shutdown().await;
}

#[tokio::test]
async fn codex_refusal_error_classification_and_mixed_runner_wave() {
    let script_dir = tempfile::tempdir().unwrap();

    let claude_script = r#"#!/usr/bin/env python3
import json, sys
def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
_ = json.loads(sys.stdin.readline())
emit({"type": "system", "subtype": "init", "session_id": "s_mix"})
emit({"type": "assistant", "message": {"content": [{"type": "text", "text": "Claude ready"}]}})
emit({"type": "result", "subtype": "success", "is_error": False, "session_id": "s_mix"})
"#;

    let codex_script = r#"#!/usr/bin/env python3
import json, sys
def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    msg = json.loads(raw)
    m = msg.get("method")
    rid = msg.get("id")
    p = msg.get("params") or {}
    if m == "initialize":
        send({"jsonrpc": "2.0", "id": rid, "result": {}})
    elif m == "account/read":
        send({"jsonrpc": "2.0", "id": rid, "result": {"account": {"type": "apiKey"}, "requiresOpenaiAuth": False}})
    elif m == "thread/start":
        send({"jsonrpc": "2.0", "id": rid, "result": {"thread": {"id": "cx_mix"}, "model": "gpt-6.1-sol"}})
    elif m == "turn/start":
        text = p["input"][0]["text"]
        send({"jsonrpc": "2.0", "id": rid, "result": {"turn": {"id": "t1", "status": "inProgress"}}})
        if "TRIGGER_REFUSAL" in text:
            send({
                "jsonrpc": "2.0",
                "method": "turn/completed",
                "params": {
                    "threadId": "cx_mix",
                    "turn": {
                        "id": "t1",
                        "status": "failed",
                        "error": {"message": "Blocked by safety rule", "codexErrorInfo": "cyberPolicy"}
                    }
                }
            })
        else:
            send({
                "jsonrpc": "2.0",
                "method": "turn/completed",
                "params": {
                    "threadId": "cx_mix",
                    "turn": {
                        "id": "t1",
                        "status": "failed",
                        "error": {"message": "Upstream server failure", "codexErrorInfo": "internalServerError"}
                    }
                }
            })
"#;

    let claude_bin = write_executable_script(script_dir.path(), "mock-claude-mix", claude_script);
    let codex_bin = write_executable_script(script_dir.path(), "mock-codex-mix", codex_script);
    let h = TestHarness::new(claude_bin, codex_bin).await;

    let wave = WaveSlug::parse("w-mix").unwrap();
    let r_cl = RoleName::parse("cl-worker").unwrap();
    let r_cx_ref = RoleName::parse("cx-refused").unwrap();
    let r_cx_err = RoleName::parse("cx-errored").unwrap();

    let spec = WaveSpec {
        slug: wave.clone(),
        project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
        goal: Body::parse("Verify mixed Claude + Codex wave routing and classification").unwrap(),
        defaults: AgentSpec {
            provider: ProviderId::parse("claude:claude-code").unwrap(),
            model: None,
            account: None,
            reasoning: None,
            service_tier: None,
            permission: PermissionMode::Full,
            workspace: Workspace::ProjectDefault,
        },
        output_dir: None,
        workers: vec![
            WorkerSpec {
                role: r_cl.clone(),
                brief: Body::parse("Say hello").unwrap(),
                agent: None,
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            },
            WorkerSpec {
                role: r_cx_ref.clone(),
                brief: Body::parse("TRIGGER_REFUSAL").unwrap(),
                agent: Some(AgentSpec {
                    provider: ProviderId::parse("codex:codex").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Full,
                    workspace: Workspace::ProjectDefault,
                }),
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            },
            WorkerSpec {
                role: r_cx_err.clone(),
                brief: Body::parse("TRIGGER_ERROR").unwrap(),
                agent: Some(AgentSpec {
                    provider: ProviderId::parse("codex:codex").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Full,
                    workspace: Workspace::ProjectDefault,
                }),
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            },
        ],
    };

    h.operator
        .call(WaveStart {
            spec,
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .unwrap();

    let snap = h
        .wait_wave(&wave, |s| {
            matches!(
                worker_phase(s, &r_cl),
                Some(WorkerPhase::Live {
                    activity: Activity::Idle,
                    ..
                })
            ) && matches!(
                worker_phase(s, &r_cx_ref),
                Some(WorkerPhase::Live {
                    activity: Activity::Refused { .. },
                    ..
                })
            ) && matches!(
                worker_phase(s, &r_cx_err),
                Some(WorkerPhase::Live {
                    activity: Activity::Errored { .. },
                    ..
                })
            )
        })
        .await;

    assert_eq!(snap.counts.idle, 1);
    assert_eq!(snap.counts.refused, 1);
    assert_eq!(snap.counts.errored, 1);

    // FeedRouter routes both Claude and Codex pages transparently.
    let cl_page = h
        .router
        .read
        .agents
        .page(&wave, &r_cl, None, 50)
        .await
        .unwrap();
    assert_eq!(cl_page.thread.runner_kind(), RunnerKind::Claude);
    assert!(cl_page.events.iter().any(|e| {
        matches!(&e.kind, AgentEventKind::AssistantText { text } if text == "Claude ready")
    }));
    let cx_page = h
        .router
        .read
        .agents
        .page(&wave, &r_cx_ref, None, 50)
        .await
        .unwrap();
    assert_eq!(cx_page.thread.runner_kind(), RunnerKind::Codex);
    assert!(matches!(cx_page.activity, Activity::Refused { .. }));

    // check_live reports missing process on unknown thread after simulated restart.
    let ghost = HostThreadId::parse("codex:w-ghost:worker:a1").unwrap();
    let probe = CodexHost(h.codex.clone()).check_live(&ghost).await.unwrap();
    assert!(matches!(
        probe,
        Some(rex_wave_api::wave::Observation::Activity {
            activity: Activity::Errored { .. },
            ..
        })
    ));

    h.claude.shutdown().await;
    h.codex.shutdown().await;
}

#[tokio::test]
async fn claude_second_turn_crash_without_result_reports_errored() {
    let script_dir = tempfile::tempdir().unwrap();
    let claude_script = r#"#!/usr/bin/env python3
import json, sys
def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
_ = json.loads(sys.stdin.readline())
emit({"type": "system", "subtype": "init", "session_id": "s_crash"})
emit({"type": "assistant", "message": {"content": [{"type": "text", "text": "Turn 1 ok"}]}})
emit({"type": "result", "subtype": "success", "is_error": False, "session_id": "s_crash"})
_ = json.loads(sys.stdin.readline())
sys.stderr.write("fatal panic on turn 2\n")
sys.stderr.flush()
sys.exit(2)
"#;

    let claude_bin = write_executable_script(script_dir.path(), "mock-claude-crash", claude_script);
    let noop_codex =
        write_executable_script(script_dir.path(), "mock-codex", "#!/bin/sh\nexit 1\n");
    let h = TestHarness::new(claude_bin, noop_codex).await;

    let wave = WaveSlug::parse("w-cl-crash").unwrap();
    let role = RoleName::parse("author").unwrap();
    let spec = WaveSpec {
        slug: wave.clone(),
        project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
        goal: Body::parse("Verify mid-turn crash after successful turn 1").unwrap(),
        defaults: AgentSpec {
            provider: ProviderId::parse("claude:claude-code").unwrap(),
            model: None,
            account: None,
            reasoning: None,
            service_tier: None,
            permission: PermissionMode::Full,
            workspace: Workspace::ProjectDefault,
        },
        output_dir: None,
        workers: vec![WorkerSpec {
            role: role.clone(),
            brief: Body::parse("Turn 1").unwrap(),
            agent: None,
            output_dir: None,
            writes: Vec::new(),
            return_schema: None,
        }],
    };

    h.operator
        .call(WaveStart {
            spec,
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .unwrap();

    h.wait_wave(&wave, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            })
        )
    })
    .await;

    h.operator
        .call(WaveTell {
            wave: wave.clone(),
            role: role.clone(),
            body: Body::parse("Turn 2 triggers crash").unwrap(),
            mode: SteerDeliveryMode::Auto,
        })
        .await
        .unwrap();

    let snap = h
        .wait_wave(&wave, |s| {
            matches!(
                worker_phase(s, &role),
                Some(WorkerPhase::Live {
                    activity: Activity::Errored { error },
                    ..
                }) if error.contains("fatal panic on turn 2")
            )
        })
        .await;
    assert_eq!(snap.counts.errored, 1);

    h.claude.shutdown().await;
}

#[tokio::test]
async fn validate_resolve_preserves_blocked_interaction_and_handshake_abort_cleans_up() {
    let script_dir = tempfile::tempdir().unwrap();
    let claude_script = r#"#!/usr/bin/env python3
import json, sys
def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
_ = json.loads(sys.stdin.readline())
emit({"type": "system", "subtype": "init", "session_id": "s_val"})
emit({"type": "control_request", "request_id": "p_val", "request": {"subtype": "can_use_tool", "tool_name": "Bash", "input": {"command": "echo hi"}}})
ctrl = json.loads(sys.stdin.readline())
assert ctrl["response"]["response"]["behavior"] == "allow"
emit({"type": "result", "subtype": "success", "is_error": False, "session_id": "s_val"})
"#;

    let hang_pid_file = script_dir.path().join("hang_codex.pid");
    let codex_hang_script = format!(
        r#"#!/usr/bin/env python3
import os, signal, subprocess, sys, time
gc = subprocess.Popen([sys.executable, "-c", "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(120)"])
with open({hang_pid:?}, "w") as f:
    f.write(str(gc.pid) + "\n")
signal.signal(signal.SIGTERM, signal.SIG_IGN)
while True:
    time.sleep(1)
"#,
        hang_pid = hang_pid_file.to_string_lossy()
    );

    let claude_bin = write_executable_script(script_dir.path(), "mock-claude-val", claude_script);
    let codex_bin =
        write_executable_script(script_dir.path(), "mock-codex-hang", &codex_hang_script);
    let h = TestHarness::new(claude_bin, codex_bin).await;

    // 1. Unsupported AllowForSession (when Claude offered no session permission_suggestions)
    // must be rejected pre-commit so a subsequent AllowOnce succeeds cleanly.
    let wave = WaveSlug::parse("w-val").unwrap();
    let role = RoleName::parse("author").unwrap();
    let spec = WaveSpec {
        slug: wave.clone(),
        project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
        goal: Body::parse("Verify pre-commit validate_resolve and handshake cancellation").unwrap(),
        defaults: AgentSpec {
            provider: ProviderId::parse("claude-code").unwrap(),
            model: None,
            account: None,
            reasoning: None,
            service_tier: None,
            permission: PermissionMode::AcceptEdits,
            workspace: Workspace::ProjectDefault,
        },
        output_dir: None,
        workers: vec![WorkerSpec {
            role: role.clone(),
            brief: Body::parse("Run echo").unwrap(),
            agent: None,
            output_dir: None,
            writes: Vec::new(),
            return_schema: None,
        }],
    };
    h.operator
        .call(WaveStart {
            spec,
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .unwrap();

    h.wait_wave(&wave, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Blocked { interaction },
                ..
            }) if interaction.id.as_str() == "p_val"
        )
    })
    .await;

    let bad_res = h
        .operator
        .call(WaveResolve {
            wave: wave.clone(),
            role: role.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("p_val").unwrap(),
            resolution: InteractionResolution::AllowForSession {
                granted_permissions: None,
            },
        })
        .await;
    assert!(bad_res.is_err(), "unsupported AllowForSession must fail");

    h.operator
        .call(WaveResolve {
            wave: wave.clone(),
            role: role.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("p_val").unwrap(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        })
        .await
        .expect("subsequent AllowOnce must succeed because interaction stayed open");

    h.wait_wave(&wave, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            })
        )
    })
    .await;

    // 2. Dropping Codex spawn mid-handshake must reap the SIGTERM-ignoring grandchild and unlink token.
    let abort_wave = WaveSlug::parse("w-abort").unwrap();
    let abort_role = RoleName::parse("hanging").unwrap();
    let abort_req = rex_wave_waves::SpawnRequest {
        wave: abort_wave.clone(),
        role: abort_role.clone(),
        attempt: 1,
        project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
        agent: AgentSpec {
            provider: ProviderId::parse("codex").unwrap(),
            model: None,
            account: None,
            reasoning: None,
            service_tier: None,
            permission: PermissionMode::Full,
            workspace: Workspace::ProjectDefault,
        },
        room: rex_wave_waves::wave_room(
            &abort_wave,
            &ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
        ),
        prompt: Body::parse("hang").unwrap(),
        title: "[w-abort] @hanging".into(),
        run_stamp: "w-abort:hanging:a1".into(),
        output_dir: None,
        writes: Vec::new(),
        return_schema: None,
    };

    let codex_host = CodexHost(h.codex.clone());
    let mut spawn_fut = Box::pin(codex_host.spawn(abort_req));
    let _ = tokio::time::timeout(Duration::from_millis(250), &mut spawn_fut).await;
    let hang_gc_pid = wait_for_pid_file(&hang_pid_file);
    assert!(process_alive(hang_gc_pid));
    drop(spawn_fut);

    let abort_tid = HostThreadId::parse("codex:w-abort:hanging:a1").unwrap();
    assert_eq!(
        read_worker_token_file(&h.state_dir, abort_tid.as_str()).unwrap(),
        None,
        "dropped Codex spawn must unlink worker token"
    );
    let kill_deadline = Instant::now() + Duration::from_secs(3);
    while process_alive(hang_gc_pid) && Instant::now() < kill_deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        !process_alive(hang_gc_pid),
        "dropped Codex spawn must SIGKILL process group"
    );

    // 3. Pre-existing non-worktree directory under .worktrees/ must be rejected, not reused.
    let rogue_wt = h.project_dir.join(".worktrees/rex-wave/w-rogue/author");
    std::fs::create_dir_all(&rogue_wt).unwrap();
    std::fs::write(rogue_wt.join(".git"), "not a real worktree\n").unwrap();
    let rogue_req = rex_wave_waves::SpawnRequest {
        wave: WaveSlug::parse("w-rogue").unwrap(),
        role: RoleName::parse("author").unwrap(),
        attempt: 1,
        project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
        agent: AgentSpec {
            provider: ProviderId::parse("claude-code").unwrap(),
            model: None,
            account: None,
            reasoning: None,
            service_tier: None,
            permission: PermissionMode::Full,
            workspace: Workspace::ManagedWorktree {
                base: BaseBranch::Default,
            },
        },
        room: rex_wave_waves::wave_room(
            &WaveSlug::parse("w-rogue").unwrap(),
            &ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
        ),
        prompt: Body::parse("hi").unwrap(),
        title: "[w-rogue] @author".into(),
        run_stamp: "w-rogue:author:a1".into(),
        output_dir: None,
        writes: Vec::new(),
        return_schema: None,
    };
    let rogue_err = ClaudeHost(h.claude.clone())
        .spawn(rogue_req)
        .await
        .unwrap_err();
    assert!(
        rogue_err
            .to_string()
            .contains("not a registered git worktree"),
        "unexpected rogue worktree error: {rogue_err}"
    );

    h.claude.shutdown().await;
    h.codex.shutdown().await;
}

#[tokio::test]
async fn round2_worktree_collision_hook_scrub_overlapping_approvals_and_wire_ordered_turns() {
    let script_dir = tempfile::tempdir().unwrap();
    let claude_log = script_dir.path().join("r2_claude.jsonl");
    let codex_log = script_dir.path().join("r2_codex.jsonl");

    // Claude mock: logs cwd, emits two overlapping can_use_tool control_requests on wave alpha-beta,
    // and completes immediately on wave alpha.
    let claude_script = format!(
        r#"#!/usr/bin/env python3
import json, os, sys

with open({claude_log:?}, "a") as f:
    f.write(json.dumps({{"cwd": os.getcwd(), "tid": os.environ.get("REX_WAVE_WORKER_THREAD_ID")}}) + "\n")

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

first = json.loads(sys.stdin.readline())
text = first["message"]["content"][0]["text"]
tid = os.environ.get("REX_WAVE_WORKER_THREAD_ID", "")

if "alpha-beta" in tid:
    emit({{"type": "system", "subtype": "init", "session_id": "s_ab"}})
    emit({{
        "type": "control_request",
        "request_id": "perm_a",
        "request": {{"subtype": "can_use_tool", "tool_name": "Bash", "input": {{"command": "echo a"}}}}
    }})
    emit({{
        "type": "control_request",
        "request_id": "perm_b",
        "request": {{"subtype": "can_use_tool", "tool_name": "Bash", "input": {{"command": "echo b"}}}}
    }})
    r1 = json.loads(sys.stdin.readline())
    assert r1["response"]["request_id"] == "perm_a"
    assert r1["response"]["response"]["behavior"] == "allow"
    r2 = json.loads(sys.stdin.readline())
    assert r2["response"]["request_id"] == "perm_b"
    assert r2["response"]["response"]["behavior"] == "allow"
    emit({{"type": "result", "subtype": "success", "is_error": False, "session_id": "s_ab"}})
else:
    emit({{"type": "system", "subtype": "init", "session_id": "s_a"}})
    emit({{"type": "result", "subtype": "success", "is_error": False, "session_id": "s_a"}})
"#,
        claude_log = claude_log.to_string_lossy()
    );

    // Codex mock:
    // - Turn 1: flushes turn/start response + turn/started + turn/completed in one batch!
    // - Turn 2: blocks on approval `gate_t2` until resolved, then emits turn/completed.
    // - Turn 3: dispatched automatically from QueueIfActive queue once Turn 2 completes!
    let codex_script = format!(
        r#"#!/usr/bin/env python3
import json, sys

def log_ev(obj):
    with open({codex_log:?}, "a") as f:
        f.write(json.dumps(obj) + "\n")

def send_batch(objs):
    sys.stdout.write("".join(json.dumps(o) + "\n" for o in objs))
    sys.stdout.flush()

turn_n = 0
for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    msg = json.loads(line)
    log_ev(msg)
    m = msg.get("method")
    rid = msg.get("id")
    p = msg.get("params") or {{}}

    if m == "initialize":
        send_batch([{{"jsonrpc": "2.0", "id": rid, "result": {{}}}}])
    elif m == "account/read":
        send_batch([{{"jsonrpc": "2.0", "id": rid, "result": {{"account": {{"type": "apiKey"}}, "requiresOpenaiAuth": False}}}}])
    elif m == "thread/start":
        send_batch([{{"jsonrpc": "2.0", "id": rid, "result": {{"thread": {{"id": "cx_r2"}}, "model": "gpt-6.1-sol"}}}}])
    elif m == "turn/start":
        turn_n += 1
        tid = f"t_{{turn_n}}"
        if turn_n == 1:
            send_batch([
                {{"jsonrpc": "2.0", "id": rid, "result": {{"turn": {{"id": tid, "status": "inProgress"}}}}}},
                {{"jsonrpc": "2.0", "method": "turn/started", "params": {{"threadId": "cx_r2", "turn": {{"id": tid, "status": "inProgress"}}}}}},
                {{"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": {{"threadId": "cx_r2", "turnId": tid, "itemId": "m1", "delta": "fast turn 1"}}}},
                {{"jsonrpc": "2.0", "method": "turn/completed", "params": {{"threadId": "cx_r2", "turn": {{"id": tid, "status": "completed"}}}}}},
            ])
        elif turn_n == 2:
            send_batch([
                {{"jsonrpc": "2.0", "id": rid, "result": {{"turn": {{"id": tid, "status": "inProgress"}}}}}},
                {{"jsonrpc": "2.0", "method": "turn/started", "params": {{"threadId": "cx_r2", "turn": {{"id": tid, "status": "inProgress"}}}}}},
                {{"jsonrpc": "2.0", "id": 777, "method": "item/commandExecution/requestApproval", "params": {{"threadId": "cx_r2", "turnId": tid, "itemId": "gate_t2", "command": "true"}}}},
            ])
        elif turn_n == 3:
            assert "queued turn 3" in p["input"][0]["text"]
            send_batch([
                {{"jsonrpc": "2.0", "id": rid, "result": {{"turn": {{"id": tid, "status": "inProgress"}}}}}},
                {{"jsonrpc": "2.0", "method": "turn/started", "params": {{"threadId": "cx_r2", "turn": {{"id": tid, "status": "inProgress"}}}}}},
                {{"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": {{"threadId": "cx_r2", "turnId": tid, "itemId": "m3", "delta": "ran queued turn 3"}}}},
                {{"jsonrpc": "2.0", "method": "turn/completed", "params": {{"threadId": "cx_r2", "turn": {{"id": tid, "status": "completed"}}}}}},
            ])
    elif m is None and rid == 777:
        send_batch([
            {{"jsonrpc": "2.0", "method": "turn/completed", "params": {{"threadId": "cx_r2", "turn": {{"id": "t_2", "status": "completed"}}}}}},
        ])
    elif m == "turn/steer":
        raise AssertionError("QueueIfActive must not emit turn/steer")
"#,
        codex_log = codex_log.to_string_lossy()
    );

    let claude_bin = write_executable_script(script_dir.path(), "mock-cl-r2", &claude_script);
    let codex_bin = write_executable_script(script_dir.path(), "mock-cx-r2", &codex_script);
    let h = TestHarness::new(claude_bin, codex_bin).await;

    // Install a malicious post-checkout hook in the project repo; core.hooksPath=/dev/null must suppress it.
    let hook_marker = script_dir.path().join("hook_executed");
    let hooks_dir = h.project_dir.join(".git/hooks");
    std::fs::create_dir_all(&hooks_dir).unwrap();
    write_executable_script(
        &hooks_dir,
        "post-checkout",
        &format!("#!/bin/sh\necho pwned > {}\n", hook_marker.display()),
    );

    // 1. Spawn colliding hyphenated (alpha-beta, gamma) and (alpha, beta-gamma) managed worktrees.
    let w_ab = WaveSlug::parse("alpha-beta").unwrap();
    let r_g = RoleName::parse("gamma").unwrap();
    let w_a = WaveSlug::parse("alpha").unwrap();
    let r_bg = RoleName::parse("beta-gamma").unwrap();

    for (w, r) in [(&w_ab, &r_g), (&w_a, &r_bg)] {
        h.operator
            .call(WaveStart {
                spec: WaveSpec {
                    slug: w.clone(),
                    project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
                    goal: Body::parse("Verify injective worktrees and hook suppression").unwrap(),
                    defaults: AgentSpec {
                        provider: ProviderId::parse("claude-code").unwrap(),
                        model: None,
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: PermissionMode::AcceptEdits,
                        workspace: Workspace::ManagedWorktree {
                            base: BaseBranch::Default,
                        },
                    },
                    output_dir: None,
                    workers: vec![WorkerSpec {
                        role: r.clone(),
                        brief: Body::parse("check").unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    }],
                },
                idem: None,
                config_defaults: Default::default(),
            })
            .await
            .unwrap();
    }

    assert!(
        !hook_marker.exists(),
        "managed worktree creation must disable repository git hooks"
    );

    // Wait for alpha/beta-gamma Idle and alpha-beta/gamma Blocked on perm_a first.
    h.wait_wave(&w_a, |s| {
        matches!(
            worker_phase(s, &r_bg),
            Some(WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            })
        )
    })
    .await;

    h.wait_wave(&w_ab, |s| {
        matches!(
            worker_phase(s, &r_g),
            Some(WorkerPhase::Live {
                activity: Activity::Blocked { interaction },
                ..
            }) if interaction.id.as_str() == "perm_a"
        )
    })
    .await;

    // Resolve perm_a -> must immediately surface queued perm_b!
    h.operator
        .call(WaveResolve {
            wave: w_ab.clone(),
            role: r_g.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("perm_a").unwrap(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        })
        .await
        .unwrap();

    h.wait_wave(&w_ab, |s| {
        matches!(
            worker_phase(s, &r_g),
            Some(WorkerPhase::Live {
                activity: Activity::Blocked { interaction },
                ..
            }) if interaction.id.as_str() == "perm_b"
        )
    })
    .await;

    h.operator
        .call(WaveResolve {
            wave: w_ab.clone(),
            role: r_g.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("perm_b").unwrap(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        })
        .await
        .unwrap();

    h.wait_wave(&w_ab, |s| {
        matches!(
            worker_phase(s, &r_g),
            Some(WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            })
        )
    })
    .await;

    let cl_lines: Vec<serde_json::Value> = rex_wave_fs::read_regular_string(&claude_log, 32 * 1024)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(cl_lines.len(), 2);
    let cwds: Vec<&str> = cl_lines
        .iter()
        .map(|v| v["cwd"].as_str().unwrap())
        .collect();
    assert!(
        cwds.iter()
            .any(|c| c.ends_with(".worktrees/rex-wave/alpha-beta/gamma"))
    );
    assert!(
        cwds.iter()
            .any(|c| c.ends_with(".worktrees/rex-wave/alpha/beta-gamma"))
    );
    assert_ne!(cwds[0], cwds[1]);

    // 2. Codex batched turn/start+turn/completed wire ordering + QueueIfActive queueing.
    let w_cx = WaveSlug::parse("codex-order").unwrap();
    let r_cx = RoleName::parse("worker").unwrap();
    h.operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: w_cx.clone(),
                project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
                goal: Body::parse("Verify wire-ordered turn state and QueueIfActive").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("codex").unwrap(),
                    model: Some(ModelId::parse("gpt-6.1-sol").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::AcceptEdits,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: r_cx.clone(),
                    brief: Body::parse("turn 1").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                }],
            },
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .unwrap();

    // Wait for batched Turn 1 to reach Idle.
    h.wait_wave(&w_cx, |s| {
        matches!(
            worker_phase(s, &r_cx),
            Some(WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            })
        )
    })
    .await;

    // SteerDeliveryMode::Start must succeed because active_turn_id was cleared in wire order!
    h.operator
        .call(WaveTell {
            wave: w_cx.clone(),
            role: r_cx.clone(),
            body: Body::parse("start turn 2").unwrap(),
            mode: SteerDeliveryMode::Start,
        })
        .await
        .expect("Start must succeed after batched turn/completed");

    // Wait until Turn 2 blocks on gate_t2.
    h.wait_wave(&w_cx, |s| {
        matches!(
            worker_phase(s, &r_cx),
            Some(WorkerPhase::Live {
                activity: Activity::Blocked { interaction },
                ..
            }) if interaction.id.as_str() == "gate_t2"
        )
    })
    .await;

    // Queue Turn 3 behind active Turn 2 using QueueIfActive.
    h.operator
        .call(WaveTell {
            wave: w_cx.clone(),
            role: r_cx.clone(),
            body: Body::parse("queued turn 3").unwrap(),
            mode: SteerDeliveryMode::QueueIfActive,
        })
        .await
        .unwrap();

    // Unblock Turn 2 -> Turn 2 completes -> queued Turn 3 auto-dispatches and completes!
    h.operator
        .call(WaveResolve {
            wave: w_cx.clone(),
            role: r_cx.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("gate_t2").unwrap(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        })
        .await
        .unwrap();

    h.wait_page(&w_cx, &r_cx, |p| {
        p.events.iter().any(|e| {
            matches!(&e.kind, AgentEventKind::AssistantText { text } if text == "ran queued turn 3")
        })
    })
    .await;

    h.wait_wave(&w_cx, |s| {
        matches!(
            worker_phase(s, &r_cx),
            Some(WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            })
        )
    })
    .await;

    h.claude.shutdown().await;
    h.codex.shutdown().await;
}

#[tokio::test]
async fn round3_steer_interrupt_resume_fencing_broken_pipe_approval_and_partial_worktree() {
    let script_dir = tempfile::tempdir().unwrap();
    let eof_marker = script_dir.path().join("resume_race_stdout_closed");

    // Claude mock covering all Round 2 findings:
    // 1) Steer interrupt -> emits `result { subtype: "error_during_execution", is_error: True }`
    //    and then receives the steered prompt cleanly without marking worker Errored!
    // 2) Resume `in_flight` race (`RESUME_RACE`): old process closes stdout after `result` and sleeps
    //    before exit so old reader lingers in `child.wait()`; resumed process blocks on `resumed_hold`
    //    so test can prove feed stays Working/Blocked and a third `Start` is rejected!
    // 3) Crash mid-approval (`stale_perm`) without `result`, then `--resume` emits `fresh_perm`!
    // 4) Broken pipe on approval (`broken_perm`): closes stdin immediately after `control_request`!
    // 5) QueueIfActive cancelled prompt recording when active turn errors!
    let claude_script = format!(
        r#"#!/usr/bin/env python3
import json, os, sys, time

eof_marker = {eof_marker:?}

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

resumed = "--resume" in sys.argv[1:]
line1 = sys.stdin.readline()
msg1 = json.loads(line1)
text1 = msg1["message"]["content"][0]["text"]

if "STEER_CASE" in text1:
    emit({{"type": "system", "subtype": "init", "session_id": "s_steer"}})
    emit({{"type": "assistant", "message": {{"content": [{{"type": "text", "text": "Working turn 1"}}]}}}})
    intr = json.loads(sys.stdin.readline())
    assert intr["type"] == "control_request"
    assert intr["request"]["subtype"] == "interrupt"
    emit({{"type": "result", "subtype": "error_during_execution", "is_error": True, "session_id": "s_steer"}})
    msg2 = json.loads(sys.stdin.readline())
    assert "steered replacement" in msg2["message"]["content"][0]["text"]
    emit({{"type": "assistant", "message": {{"content": [{{"type": "text", "text": "Completed steered turn"}}]}}}})
    emit({{"type": "result", "subtype": "success", "is_error": False, "session_id": "s_steer"}})
elif "RESUME_RACE" in text1 and not resumed:
    emit({{"type": "system", "subtype": "init", "session_id": "s_rrace"}})
    emit({{"type": "result", "subtype": "success", "is_error": False, "session_id": "s_rrace"}})
    os.close(0)
    os.close(1)
    with open(eof_marker, "w") as f:
        f.write("closed\n")
    time.sleep(0.18)
    sys.exit(0)
elif resumed and "s_rrace" in sys.argv[1:]:
    assert "resumed second turn" in text1
    emit({{"type": "system", "subtype": "init", "session_id": "s_rrace"}})
    emit({{
        "type": "control_request",
        "request_id": "resumed_hold",
        "request": {{"subtype": "can_use_tool", "tool_name": "Bash", "input": {{"command": "echo hold"}}}}
    }})
    ctrl = json.loads(sys.stdin.readline())
    assert ctrl["response"]["request_id"] == "resumed_hold"
    emit({{"type": "result", "subtype": "success", "is_error": False, "session_id": "s_rrace"}})
elif "CRASH_APPROVAL" in text1 and not resumed:
    emit({{"type": "system", "subtype": "init", "session_id": "s_crash_perm"}})
    emit({{
        "type": "control_request",
        "request_id": "stale_perm",
        "request": {{"subtype": "can_use_tool", "tool_name": "Bash", "input": {{"command": "echo stale"}}}}
    }})
    sys.exit(1)
elif resumed and "s_crash_perm" in sys.argv[1:]:
    assert "recover after crash" in text1
    emit({{"type": "system", "subtype": "init", "session_id": "s_crash_perm"}})
    emit({{
        "type": "control_request",
        "request_id": "fresh_perm",
        "request": {{"subtype": "can_use_tool", "tool_name": "Bash", "input": {{"command": "echo fresh"}}}}
    }})
    ctrl = json.loads(sys.stdin.readline())
    assert ctrl["response"]["request_id"] == "fresh_perm"
    emit({{"type": "assistant", "message": {{"content": [{{"type": "text", "text": "Recovered fresh approval"}}]}}}})
    emit({{"type": "result", "subtype": "success", "is_error": False, "session_id": "s_crash_perm"}})
elif "BROKEN_PIPE" in text1:
    emit({{"type": "system", "subtype": "init", "session_id": "s_bpipe"}})
    emit({{
        "type": "control_request",
        "request_id": "broken_perm",
        "request": {{"subtype": "can_use_tool", "tool_name": "Bash", "input": {{"command": "echo bp"}}}}
    }})
    os.close(0)
    time.sleep(30)
elif "QUEUE_ERR" in text1:
    emit({{"type": "system", "subtype": "init", "session_id": "s_qerr"}})
    emit({{
        "type": "control_request",
        "request_id": "qerr_gate",
        "request": {{"subtype": "can_use_tool", "tool_name": "Bash", "input": {{"command": "echo gate"}}}}
    }})
    _ = json.loads(sys.stdin.readline())
    emit({{"type": "result", "subtype": "error_during_execution", "is_error": True, "error": "boom"}})
"#,
        eof_marker = eof_marker.to_string_lossy()
    );

    let codex_script = r#"#!/usr/bin/env python3
import json, sys

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    msg = json.loads(line)
    m = msg.get("method")
    rid = msg.get("id")
    if m == "initialize":
        send({"jsonrpc": "2.0", "id": rid, "result": {}})
    elif m == "account/read":
        send({"jsonrpc": "2.0", "id": rid, "result": {"account": {"type": "apiKey"}, "requiresOpenaiAuth": False}})
    elif m == "thread/start":
        send({"jsonrpc": "2.0", "id": rid, "result": {"thread": {"id": "cx_r3"}, "model": "gpt-6.1-sol"}})
    elif m == "turn/start":
        send({"jsonrpc": "2.0", "id": rid, "result": {"turn": {"id": "t1", "status": "inProgress"}}})
        send({"jsonrpc": "2.0", "method": "turn/started", "params": {"threadId": "cx_r3", "turn": {"id": "t1", "status": "inProgress"}}})
        send({
            "jsonrpc": "2.0",
            "id": 501,
            "method": "item/commandExecution/requestApproval",
            "params": {"threadId": "cx_r3", "turnId": "t1", "itemId": "cmd_amend", "command": "cargo test"}
        })
    elif m is None and rid == 501:
        send({
            "jsonrpc": "2.0",
            "id": 502,
            "method": "item/permissions/requestApproval",
            "params": {
                "threadId": "cx_r3",
                "turnId": "t1",
                "itemId": "perm_sub",
                "cwd": "/tmp",
                "startedAtMs": 1,
                "permissions": {"network": {"enabled": False}, "fileSystem": {"read": ["/tmp/ok"]}}
            }
        })
    elif m is None and rid == 502:
        send({"jsonrpc": "2.0", "method": "turn/completed", "params": {"threadId": "cx_r3", "turn": {"id": "t1", "status": "completed"}}})
"#;

    let claude_bin = write_executable_script(script_dir.path(), "mock-cl-r3", &claude_script);
    let codex_bin = write_executable_script(script_dir.path(), "mock-cx-r3", codex_script);
    let h = TestHarness::new(claude_bin, codex_bin).await;

    // 1. Opus Blocker: Claude Steer interrupt -> error_during_execution -> dispatches steered prompt cleanly!
    let w_steer = WaveSlug::parse("r3-steer").unwrap();
    let role = RoleName::parse("author").unwrap();
    h.operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: w_steer.clone(),
                project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
                goal: Body::parse("Verify Claude Steer interrupt recovery").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("claude-code").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Full,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("STEER_CASE").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                }],
            },
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .unwrap();

    h.wait_page(&w_steer, &role, |p| {
        p.events.iter().any(|e| {
            matches!(&e.kind, AgentEventKind::AssistantText { text } if text == "Working turn 1")
        })
    })
    .await;

    h.operator
        .call(WaveTell {
            wave: w_steer.clone(),
            role: role.clone(),
            body: Body::parse("steered replacement").unwrap(),
            mode: SteerDeliveryMode::SteerIfActive,
        })
        .await
        .unwrap();

    h.wait_page(&w_steer, &role, |p| {
        p.events.iter().any(|e| {
            matches!(&e.kind, AgentEventKind::AssistantText { text } if text == "Completed steered turn")
        })
    })
    .await;

    let steer_snap = h
        .wait_wave(&w_steer, |s| {
            matches!(
                worker_phase(s, &role),
                Some(WorkerPhase::Live {
                    activity: Activity::Idle,
                    ..
                })
            )
        })
        .await;
    assert_eq!(steer_snap.counts.errored, 0);

    // 2. Astra Blocker #1: Resume `in_flight` ownership transfer prevents duplicate Start while resumed turn runs!
    let w_rrace = WaveSlug::parse("r3-rrace").unwrap();
    h.operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: w_rrace.clone(),
                project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
                goal: Body::parse("Verify resume in_flight fencing").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("claude-code").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::AcceptEdits,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("RESUME_RACE").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                }],
            },
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .unwrap();

    let eof_deadline = Instant::now() + Duration::from_secs(5);
    while !eof_marker.exists() && Instant::now() < eof_deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(eof_marker.exists());

    let rrace_tid = HostThreadId::parse("claude:r3-rrace:author:a1").unwrap();
    let claude_host = ClaudeHost(h.claude.clone());
    claude_host
        .tell(
            &rrace_tid,
            &Body::parse("resumed second turn").unwrap(),
            SteerDeliveryMode::Start,
        )
        .await
        .unwrap();

    h.wait_wave(&w_rrace, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Blocked { interaction },
                ..
            }) if interaction.id.as_str() == "resumed_hold"
        )
    })
    .await;

    // Wait past the old process's 180ms sleep and prove `in_flight` stayed owned by generation 2!
    tokio::time::sleep(Duration::from_millis(220)).await;
    let third_start = claude_host
        .tell(
            &rrace_tid,
            &Body::parse("illegal overlapping start").unwrap(),
            SteerDeliveryMode::Start,
        )
        .await;
    assert!(
        third_start.is_err(),
        "concurrent Start must fail while resumed turn is active"
    );

    h.operator
        .call(WaveResolve {
            wave: w_rrace.clone(),
            role: role.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("resumed_hold").unwrap(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        })
        .await
        .unwrap();

    h.wait_wave(&w_rrace, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            })
        )
    })
    .await;

    // 3. Astra Blocker #2: Crashed Claude process clears `stale_perm` so `--resume` surfaces `fresh_perm`!
    let w_crash = WaveSlug::parse("r3-crash-perm").unwrap();
    h.operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: w_crash.clone(),
                project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
                goal: Body::parse("Verify stale approval cleared on crash").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("claude-code").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::AcceptEdits,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("CRASH_APPROVAL").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                }],
            },
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .unwrap();

    h.wait_wave(&w_crash, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Errored { .. },
                ..
            })
        )
    })
    .await;

    h.operator
        .call(WaveTell {
            wave: w_crash.clone(),
            role: role.clone(),
            body: Body::parse("recover after crash").unwrap(),
            mode: SteerDeliveryMode::Auto,
        })
        .await
        .unwrap();

    h.wait_wave(&w_crash, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Blocked { interaction },
                ..
            }) if interaction.id.as_str() == "fresh_perm"
        )
    })
    .await;

    h.operator
        .call(WaveResolve {
            wave: w_crash.clone(),
            role: role.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("fresh_perm").unwrap(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        })
        .await
        .unwrap();

    h.wait_wave(&w_crash, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            })
        )
    })
    .await;

    // 4. Astra Blocker #3: Broken pipe during approval delivery transitions worker to terminal Errored (never Working).
    let w_bp = WaveSlug::parse("r3-bpipe").unwrap();
    h.operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: w_bp.clone(),
                project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
                goal: Body::parse("Verify broken pipe approval delivery").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("claude-code").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::AcceptEdits,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("BROKEN_PIPE").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                }],
            },
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .unwrap();

    h.wait_wave(&w_bp, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Blocked { interaction },
                ..
            }) if interaction.id.as_str() == "broken_perm"
        )
    })
    .await;

    tokio::time::sleep(Duration::from_millis(60)).await;
    h.operator
        .call(WaveResolve {
            wave: w_bp.clone(),
            role: role.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("broken_perm").unwrap(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        })
        .await
        .unwrap();

    h.wait_wave(&w_bp, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Errored { .. },
                ..
            })
        )
    })
    .await;
    let bp_page = h
        .router
        .read
        .agents
        .page(&w_bp, &role, None, 50)
        .await
        .unwrap();
    assert!(matches!(bp_page.activity, Activity::Errored { .. }));

    // 5. Astra Blocker #4: Codex pre-commit validation rejects null execpolicy amendment & widened raw permissions.
    let w_cx = WaveSlug::parse("r3-cx-val").unwrap();
    h.operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: w_cx.clone(),
                project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
                goal: Body::parse("Verify Codex RequestAnswer schema and subset checks").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("codex").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::AcceptEdits,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("check approvals").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                }],
            },
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .unwrap();

    h.wait_wave(&w_cx, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Blocked { interaction },
                ..
            }) if interaction.id.as_str() == "cmd_amend"
        )
    })
    .await;

    let bad_amend = h
        .operator
        .call(WaveResolve {
            wave: w_cx.clone(),
            role: role.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("cmd_amend").unwrap(),
            resolution: InteractionResolution::RequestAnswer {
                value: serde_json::json!({"decision": {"acceptWithExecpolicyAmendment": null}}),
            },
        })
        .await;
    assert!(bad_amend.is_err());

    h.operator
        .call(WaveResolve {
            wave: w_cx.clone(),
            role: role.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("cmd_amend").unwrap(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        })
        .await
        .unwrap();

    h.wait_wave(&w_cx, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Blocked { interaction },
                ..
            }) if interaction.id.as_str() == "perm_sub"
        )
    })
    .await;

    let bad_raw_perm = h
        .operator
        .call(WaveResolve {
            wave: w_cx.clone(),
            role: role.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("perm_sub").unwrap(),
            resolution: InteractionResolution::RequestAnswer {
                value: serde_json::json!({"permissions": {"network": {"enabled": true}}, "scope": "turn"}),
            },
        })
        .await;
    assert!(bad_raw_perm.is_err());

    h.operator
        .call(WaveResolve {
            wave: w_cx.clone(),
            role: role.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("perm_sub").unwrap(),
            resolution: InteractionResolution::RequestAnswer {
                value: serde_json::json!({"permissions": {"fileSystem": {"read": ["/tmp/ok"]}}, "scope": "turn"}),
            },
        })
        .await
        .unwrap();

    h.wait_wave(&w_cx, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            })
        )
    })
    .await;

    // 6. Astra Should #5 & R3 #3: Unrelated manual worktree without creation-intent is rejected,
    //    whereas a worktree with `rex-wave`'s creation-intent marker heals cleanly on retry!
    let manual_wt = h.project_dir.join(".worktrees/rex-wave/r3-manual/author");
    std::fs::create_dir_all(manual_wt.parent().unwrap()).unwrap();
    let mut manual_cmd = StdCommand::new("git");
    for k in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_COMMON_DIR",
    ] {
        manual_cmd.env_remove(k);
    }
    assert!(
        manual_cmd
            .arg("-C")
            .arg(&h.project_dir)
            .args(["worktree", "add", "-b", "manual-investigation"])
            .arg(&manual_wt)
            .arg("HEAD")
            .output()
            .unwrap()
            .status
            .success()
    );
    let manual_reject = ClaudeHost(h.claude.clone())
        .spawn(rex_wave_waves::SpawnRequest {
            wave: WaveSlug::parse("r3-manual").unwrap(),
            role: role.clone(),
            attempt: 1,
            project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
            agent: AgentSpec {
                provider: ProviderId::parse("claude-code").unwrap(),
                model: None,
                account: None,
                reasoning: None,
                service_tier: None,
                permission: PermissionMode::AcceptEdits,
                workspace: Workspace::ManagedWorktree {
                    base: BaseBranch::Default,
                },
            },
            room: rex_wave_waves::wave_room(
                &WaveSlug::parse("r3-manual").unwrap(),
                &ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
            ),
            prompt: Body::parse("hi").unwrap(),
            title: "[r3-manual] @author".into(),
            run_stamp: "r3-manual:author:a1".into(),
            output_dir: None,
            writes: Vec::new(),
            return_schema: None,
        })
        .await;
    assert!(
        manual_reject.is_err(),
        "manual worktree without creation-intent marker must never be adopted"
    );

    let partial_wt = h.project_dir.join(".worktrees/rex-wave/r3-partial/author");
    std::fs::create_dir_all(partial_wt.parent().unwrap()).unwrap();
    let intents_dir = h.project_dir.join(".git/rex-wave-worktree-intents");
    std::fs::create_dir_all(&intents_dir).unwrap();
    std::fs::write(
        intents_dir.join("10--r3-partial_author.json"),
        r#"{"wave":"r3-partial","role":"author"}"#,
    )
    .unwrap();
    let mut wt_cmd = StdCommand::new("git");
    for k in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_COMMON_DIR",
    ] {
        wt_cmd.env_remove(k);
    }
    let wt_out = wt_cmd
        .arg("-C")
        .arg(&h.project_dir)
        .args(["worktree", "add", "-b", "rex-wave/r3-partial/author"])
        .arg(&partial_wt)
        .arg("HEAD")
        .output()
        .unwrap();
    assert!(wt_out.status.success());

    let w_pwt = WaveSlug::parse("r3-partial").unwrap();
    h.operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: w_pwt.clone(),
                project: ProjectId::parse(h.project_dir.to_string_lossy()).unwrap(),
                goal: Body::parse("Verify partial worktree healing and cancelled queue log")
                    .unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("claude-code").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::AcceptEdits,
                    workspace: Workspace::ManagedWorktree {
                        base: BaseBranch::Default,
                    },
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("QUEUE_ERR").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                }],
            },
            idem: None,
            config_defaults: Default::default(),
        })
        .await
        .unwrap();

    h.wait_wave(&w_pwt, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Blocked { interaction },
                ..
            }) if interaction.id.as_str() == "qerr_gate"
        )
    })
    .await;

    h.operator
        .call(WaveTell {
            wave: w_pwt.clone(),
            role: role.clone(),
            body: Body::parse("must record cancellation").unwrap(),
            mode: SteerDeliveryMode::QueueIfActive,
        })
        .await
        .unwrap();

    h.operator
        .call(WaveResolve {
            wave: w_pwt.clone(),
            role: role.clone(),
            interaction: rex_wave_api::HostInteractionId::parse("qerr_gate").unwrap(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        })
        .await
        .unwrap();

    h.wait_wave(&w_pwt, |s| {
        matches!(
            worker_phase(s, &role),
            Some(WorkerPhase::Live {
                activity: Activity::Errored { .. },
                ..
            })
        )
    })
    .await;

    let qerr_page = h
        .wait_page(&w_pwt, &role, |p| {
            p.events.iter().any(|e| {
                matches!(&e.kind, AgentEventKind::Other { bb_type } if bb_type.contains("cancelled_queued_prompt (turn_errored): must record cancellation"))
            })
        })
        .await;
    assert!(matches!(qerr_page.activity, Activity::Errored { .. }));

    h.claude.shutdown().await;
    h.codex.shutdown().await;
}
