#![allow(clippy::disallowed_methods)]

use futures_util::StreamExt;
use rex_wave_acp::{AcpBridge, AcpConfig, AcpFeed, AcpHost, AcpProviderSpec};
use rex_wave_api::read::{AgentEventKind, ThreadPage};
use rex_wave_api::room::Body;
use rex_wave_api::wave::{
    Activity, AgentSpec, BaseBranch, InteractionKind, InteractionResolution, Observation,
    PermissionMode, SteerDeliveryMode, WaveFinish, WaveOutcome, WaveResolve, WaveRetireWorker,
    WaveSpec, WaveStart, WaveTell, WaveView, WorkerPhase, WorkerSpec, Workspace,
};
use rex_wave_api::*;
use rex_wave_board::Board;
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    AgentFeed, FeedRouter, Hub, LocalClient, ReadSide, Registry, Router, SidebarProjection,
    spawn_reactor,
};
use rex_wave_store::Journal;
use rex_wave_transport::{read_worker_token_file, worker_token_path};
use rex_wave_waves::{AgentHost, HostRouter, WaveConductor, Waves};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::Arc;
use std::time::{Duration, Instant};

const MOCK_ACP_SCRIPT: &str = r#"#!/usr/bin/env python3
import json
import os
import signal
import subprocess
import sys
import time

LOG_PATH = os.environ.get("MOCK_ACP_LOG", "")
GRANDCHILD_PID_PATH = os.environ.get("MOCK_ACP_GRANDCHILD_PID", "")
MODE = os.environ.get("MOCK_ACP_MODE", "full_lifecycle")

def log_event(obj):
    if not LOG_PATH:
        return
    with open(LOG_PATH, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj) + "\n")

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

def spawn_stubborn_grandchild():
    if not GRANDCHILD_PID_PATH:
        return None
    # Child process that ignores SIGTERM so retire must escalate to SIGKILL on the process group.
    code = (
        "import signal, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "time.sleep(120)"
    )
    proc = subprocess.Popen([sys.executable, "-c", code])
    with open(GRANDCHILD_PID_PATH, "w", encoding="utf-8") as fh:
        fh.write(str(proc.pid))
    return proc

log_event({
    "event": "started",
    "argv": sys.argv[1:],
    "cwd": os.getcwd(),
    "pid": os.getpid(),
    "pgid": os.getpgrp(),
    "env": {
        "REX_WAVE_SOCKET": os.environ.get("REX_WAVE_SOCKET"),
        "REX_WAVE_TOKEN": os.environ.get("REX_WAVE_TOKEN"),
        "REX_WAVE_WORKER_THREAD_ID": os.environ.get("REX_WAVE_WORKER_THREAD_ID"),
        "T3CODE_THREAD_ID": os.environ.get("T3CODE_THREAD_ID"),
    },
})

session_id = "acp-sess-1"
prompt_count = 0
grandchild = None
pending_prompt_id = None

for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    msg = json.loads(line)
    log_event({"event": "rx", "msg": msg})
    method = msg.get("method")
    msg_id = msg.get("id")

    if method == "initialize":
        send({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": 1,
                "agentInfo": {"name": "mock-acp", "version": "1.0.0"},
                "agentCapabilities": {},
            },
        })
        continue

    if method == "session/new":
        grandchild = spawn_stubborn_grandchild()
        send({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "sessionId": session_id,
                "models": {
                    "currentModelId": "mock-model-1",
                    "availableModels": [{"modelId": "mock-model-1", "name": "Mock 1"}],
                },
            },
        })
        continue

    if method == "session/set_model":
        send({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "models": {"currentModelId": msg.get("params", {}).get("modelId", "mock-model-1")}
            },
        })
        continue

    if method == "session/cancel":
        log_event({"event": "cancelled"})
        if pending_prompt_id is not None:
            send({
                "jsonrpc": "2.0",
                "id": pending_prompt_id,
                "result": {"stopReason": "cancelled"},
            })
            pending_prompt_id = None
        continue

    if method == "session/prompt":
        prompt_count += 1
        prompt_blocks = msg.get("params", {}).get("prompt", [])
        prompt_text = " ".join(b.get("text", "") for b in prompt_blocks if isinstance(b, dict))

        if MODE == "steer_cancel":
            if prompt_count == 1:
                send({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": "starting long turn"},
                        },
                    },
                })
                pending_prompt_id = msg_id
                continue
            else:
                send({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": f"steered: {prompt_text}"},
                        },
                    },
                })
                send({
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {"stopReason": "end_turn"},
                })
                continue

        if MODE == "provider_error":
            send({
                "jsonrpc": "2.0",
                "id": msg_id,
                "error": {"code": -32000, "message": "upstream rate limit exceeded"},
            })
            continue

        if MODE == "crash_stderr":
            sys.stderr.write("fatal backend panic during turn\n")
            sys.stderr.flush()
            sys.exit(2)

        if MODE == "permission_variants":
            perm_id = 100 + prompt_count
            send({
                "jsonrpc": "2.0",
                "id": perm_id,
                "method": "session/request_permission",
                "params": {
                    "sessionId": session_id,
                    "toolCall": {"toolCallId": f"tc-{prompt_count}", "title": f"Action {prompt_count}", "kind": "edit"},
                    "options": [
                        {"optionId": "opt_once", "name": "Allow once", "kind": "allow_once"},
                        {"optionId": "opt_session", "name": "Allow always", "kind": "allow_always"},
                        {"optionId": "opt_reject", "name": "Reject", "kind": "reject_once"},
                    ],
                },
            })
            reply_line = sys.stdin.readline()
            if not reply_line:
                break
            reply_msg = json.loads(reply_line)
            log_event({"event": "perm_reply", "turn": prompt_count, "reply": reply_msg})
            send({
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {"stopReason": "end_turn"},
            })
            continue

        # Default MODE == "full_lifecycle"
        if prompt_count == 1:
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "inspecting repository"},
                    },
                },
            })
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "tc-1",
                        "title": "cargo test",
                        "kind": "execute",
                        "status": "in_progress",
                    },
                },
            })
            send({
                "jsonrpc": "2.0",
                "id": 77,
                "method": "session/request_permission",
                "params": {
                    "sessionId": session_id,
                    "toolCall": {
                        "toolCallId": "tc-1",
                        "title": "Run cargo test",
                        "kind": "execute",
                    },
                    "options": [
                        {"optionId": "allow_once", "name": "Allow once", "kind": "allow_once"},
                        {"optionId": "allow_always", "name": "Allow always", "kind": "allow_always"},
                        {"optionId": "reject_once", "name": "Reject", "kind": "reject_once"},
                    ],
                },
            })
            perm_line = sys.stdin.readline()
            if not perm_line:
                break
            perm_reply = json.loads(perm_line)
            log_event({"event": "perm_reply", "reply": perm_reply})
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "tc-1",
                        "status": "completed",
                        "rawOutput": {"exitCode": 0, "stdout": "all passed"},
                    },
                },
            })
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "usage_update",
                        "used": 2048,
                        "size": 200000,
                        "cost": {"amount": 0.015, "currency": "USD"},
                    },
                },
            })
            send({
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {"stopReason": "end_turn"},
            })
            continue
        else:
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "cannot comply with restricted request"},
                    },
                },
            })
            send({
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {"stopReason": "refusal", "message": "policy safeguard triggered"},
            })
            continue
"#;

fn write_mock_script(dir: &Path) -> PathBuf {
    let path = dir.join("mock-acp.py");
    std::fs::write(&path, MOCK_ACP_SCRIPT).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn init_temp_git_repo(dir: &Path) -> PathBuf {
    let repo = dir.join("demo-repo");
    std::fs::create_dir_all(&repo).unwrap();
    let run_git = |args: &[&str]| {
        let out = StdCommand::new("git")
            .arg("-C")
            .arg(&repo)
            .args(args)
            .env("GIT_AUTHOR_NAME", "Demo Tester")
            .env("GIT_AUTHOR_EMAIL", "demo@example.test")
            .env("GIT_COMMITTER_NAME", "Demo Tester")
            .env("GIT_COMMITTER_EMAIL", "demo@example.test")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run_git(&["init"]);
    std::fs::write(repo.join("README.md"), "# demo\n").unwrap();
    run_git(&["add", "README.md"]);
    run_git(&["commit", "-m", "init"]);
    repo
}

fn build_test_stack(
    dir: &Path,
    acp_cfg: AcpConfig,
) -> (Arc<Router>, Arc<AcpBridge>, tokio::task::JoinHandle<()>) {
    let journal = Journal::open(&dir.join("journal.sqlite3")).unwrap();
    let hub = Hub::new(1024);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let bridge = AcpBridge::new(acp_cfg);
    let feed: Arc<dyn AgentFeed> = Arc::new(FeedRouter::new([(
        RunnerKind::Acp,
        Arc::new(AcpFeed(bridge.clone())) as Arc<dyn AgentFeed>,
    )]));

    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Registry::<Waves>::new(journal.clone(), hub.clone()),
        board: Registry::<Board>::new(journal.clone(), hub.clone()),
        inbox: Registry::<rex_wave_notify::Inbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(rex_wave_runtime::Unavailable(Family::Mail)),
        presence: Arc::new(rex_wave_runtime::Unavailable(Family::Presence)),
        config: Arc::new(rex_wave_runtime::Unavailable(Family::Config)),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: feed,
        }),
    });

    bridge.attach_backend(router.clone());
    let host_router =
        HostRouter::new(RunnerKind::Acp).with_host(RunnerKind::Acp, AcpHost(bridge.clone()));
    let conductor = spawn_reactor(
        WaveConductor::new(host_router),
        LocalClient::new(
            router.clone(),
            Principal::System {
                part: SystemPart::WaveConductor,
            },
        ),
    );
    (router, bridge, conductor)
}

async fn wait_for_phase<F>(
    op: &LocalClient,
    wave: &WaveSlug,
    role: &RoleName,
    timeout: Duration,
    mut pred: F,
) -> WorkerPhase
where
    F: FnMut(&WorkerPhase) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
        let phase = snap
            .workers
            .iter()
            .find(|(r, _, _)| r == role)
            .map(|(_, _, p)| p.clone())
            .expect("worker present");
        if pred(&phase) {
            return phase;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for worker phase; last={phase:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn pid_alive(pid: i32) -> bool {
    let Some(p) = rustix::process::Pid::from_raw(pid) else {
        return false;
    };
    !matches!(
        rustix::process::test_kill_process(p),
        Err(rustix::io::Errno::SRCH)
    )
}

#[tokio::test]
async fn acp_full_lifecycle_streaming_permission_refusal_and_pgroup_reap() {
    let tmp = tempfile::tempdir().unwrap();
    let script = write_mock_script(tmp.path());
    let repo = init_temp_git_repo(tmp.path());
    let state_dir = tmp.path().join("state");
    let sock_path = state_dir.join("daemon.sock");
    let log_path = tmp.path().join("acp.log.jsonl");
    let gc_pid_path = tmp.path().join("grandchild.pid");

    let provider_spec = AcpProviderSpec::new(&script, ["acp"])
        .with_env("MOCK_ACP_LOG", log_path.to_string_lossy().into_owned())
        .with_env(
            "MOCK_ACP_GRANDCHILD_PID",
            gc_pid_path.to_string_lossy().into_owned(),
        )
        .with_env("MOCK_ACP_MODE", "full_lifecycle");

    let cfg = AcpConfig::with_env_overrides(None, None)
        .with_provider("antigravity", provider_spec)
        .with_state_dir(state_dir.clone())
        .with_socket_path(sock_path.clone())
        .with_rex_wave_bin(PathBuf::from("/usr/local/bin/rex-wave-test"))
        .with_retire_grace(Duration::from_millis(400));

    let (router, bridge, conductor) = build_test_stack(tmp.path(), cfg);
    let op = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );

    let wave = WaveSlug::parse("w-acp-life").unwrap();
    let role = RoleName::parse("author").unwrap();
    let project = ProjectId::parse(repo.to_string_lossy().into_owned()).unwrap();

    op.call(WaveStart {
        spec: WaveSpec {
            slug: wave.clone(),
            project,
            goal: Body::parse("Verify full ACP lifecycle").unwrap(),
            defaults: AgentSpec {
                provider: ProviderId::parse("acp:antigravity").unwrap(),
                model: Some(ModelId::parse("mock-model-1").unwrap()),
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
                brief: Body::parse("Run checks and report").unwrap(),
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

    // 1. Wait until inbound session/request_permission transitions worker to Blocked.
    let blocked_phase = wait_for_phase(&op, &wave, &role, Duration::from_secs(10), |p| {
        matches!(
            p,
            WorkerPhase::Live {
                activity: Activity::Blocked { .. },
                ..
            }
        )
    })
    .await;

    let (thread_id, interaction) = match blocked_phase {
        WorkerPhase::Live {
            thread,
            activity: Activity::Blocked { interaction },
        } => (thread, interaction),
        other => panic!("expected Blocked phase, got {other:?}"),
    };
    assert_eq!(thread_id.as_str(), "acp:w-acp-life:author:a1");
    assert_eq!(interaction.id.as_str(), "acp-perm-77");
    assert_eq!(interaction.kind, InteractionKind::Permission);
    assert_eq!(interaction.summary, "Run cargo test");

    // Verify mode-0600 worker token file exists before retire.
    let tok_path = worker_token_path(&state_dir, thread_id.as_str()).unwrap();
    let tok_meta = std::fs::symlink_metadata(&tok_path).unwrap();
    assert_eq!(tok_meta.permissions().mode() & 0o777, 0o600);
    let token = read_worker_token_file(&state_dir, thread_id.as_str())
        .unwrap()
        .expect("token provisioned");
    assert!(token.starts_with("rw_w_"));

    // Open live AgentFeed stream before resolving permission so we observe live frames too.
    let mut live_stream = bridge.live(&wave, &role).await.unwrap();

    // 2. Resolve permission via WaveResolve -> child finishes tool_call_update + usage + end_turn -> Idle.
    op.call(WaveResolve {
        wave: wave.clone(),
        role: role.clone(),
        interaction: interaction.id.clone(),
        resolution: InteractionResolution::AllowOnce {
            granted_permissions: None,
        },
    })
    .await
    .unwrap();

    let idle_phase = wait_for_phase(&op, &wave, &role, Duration::from_secs(10), |p| {
        matches!(
            p,
            WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            }
        )
    })
    .await;
    assert!(matches!(
        idle_phase,
        WorkerPhase::Live {
            activity: Activity::Idle,
            ..
        }
    ));

    // Verify AgentFeed::page returns monotonic HostSeq events and usage.
    let page = op
        .call(ThreadPage {
            wave: wave.clone(),
            role: role.clone(),
            after: None,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(page.thread, thread_id);
    assert_eq!(page.activity, Activity::Idle);
    assert!(page.events.windows(2).all(|w| w[0].seq < w[1].seq));
    assert!(page.events.iter().any(|e| matches!(
        &e.kind,
        AgentEventKind::AssistantText { text } if text == "inspecting repository"
    )));
    assert!(page.events.iter().any(|e| matches!(
        &e.kind,
        AgentEventKind::ToolCall { name, summary } if name == "execute" && summary == "cargo test"
    )));
    assert!(page.events.iter().any(|e| matches!(
        &e.kind,
        AgentEventKind::ToolResult { name, ok: true, .. } if name == "execute"
    )));
    let usage = page.usage.expect("usage recorded from usage_update");
    assert_eq!(usage.used_tokens, 2048);
    assert_eq!(usage.model_context_window, 200_000);
    assert_eq!(usage.cost_usd, Some(0.015));

    // Drain live stream events up through TurnEnded and confirm monotonic delivery.
    let mut seen_turn_ended = false;
    let mut last_live_seq = HostSeq(0);
    let drain_deadline = Instant::now() + Duration::from_secs(5);
    while !seen_turn_ended && Instant::now() < drain_deadline {
        let next = tokio::time::timeout(Duration::from_millis(500), live_stream.next()).await;
        if let Ok(Some(Ok(Frame::Agent { event, .. }))) = next {
            assert!(event.seq > last_live_seq, "live seq must increase");
            last_live_seq = event.seq;
            if matches!(event.kind, AgentEventKind::TurnEnded) {
                seen_turn_ended = true;
            }
        }
    }
    assert!(
        seen_turn_ended,
        "expected TurnEnded on live AgentFeed stream"
    );

    // 3. Send follow-up tell -> mock responds with stopReason: refusal -> Activity::Refused.
    op.call(WaveTell {
        wave: wave.clone(),
        role: role.clone(),
        body: Body::parse("Trigger policy refusal check").unwrap(),
        mode: SteerDeliveryMode::Auto,
    })
    .await
    .unwrap();

    let refused_phase = wait_for_phase(&op, &wave, &role, Duration::from_secs(10), |p| {
        matches!(
            p,
            WorkerPhase::Live {
                activity: Activity::Refused { .. },
                ..
            }
        )
    })
    .await;
    assert!(matches!(
        refused_phase,
        WorkerPhase::Live {
            activity: Activity::Refused { ref reason },
            ..
        } if reason.contains("refusal")
    ));

    // Confirm WaveFinish(Completed) refuses while worker is Refused.
    assert!(
        op.call(WaveFinish {
            wave: wave.clone(),
            outcome: WaveOutcome::Completed {
                summary: Body::parse("done").unwrap(),
            },
            allow_incomplete: false,
        })
        .await
        .is_err()
    );

    // Confirm stubborn grandchild subprocess is currently alive before retire.
    let gc_pid: i32 = std::fs::read_to_string(&gc_pid_path)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        pid_alive(gc_pid),
        "grandchild pid {gc_pid} should be alive before retire"
    );

    // 4. Retire worker -> token unlinked, entire process group (incl. stubborn grandchild) killed,
    // and phase transitions to Retired preserving the refusal reason.
    op.call(WaveRetireWorker {
        wave: wave.clone(),
        role: role.clone(),
        reason: Some("test complete".into()),
    })
    .await
    .unwrap();

    let retired_phase = wait_for_phase(&op, &wave, &role, Duration::from_secs(10), |p| {
        matches!(p, WorkerPhase::Retired { .. })
    })
    .await;
    assert!(matches!(
        retired_phase,
        WorkerPhase::Retired {
            thread: Some(ref t),
            refused: Some(_),
            errored: None,
        } if t == &thread_id
    ));

    // Worker token file must be unlinked.
    assert!(
        !tok_path.exists(),
        "worker token file must be unlinked on retire"
    );
    assert_eq!(
        read_worker_token_file(&state_dir, thread_id.as_str()).unwrap(),
        None
    );

    // Grandchild process ignoring SIGTERM must be dead after bounded grace + SIGKILL.
    assert!(
        !pid_alive(gc_pid),
        "grandchild pid {gc_pid} in child process group must be reaped on retire"
    );

    // Verify wire handshake & workspace details captured in mock log.
    let raw_log = std::fs::read_to_string(&log_path).unwrap();
    let entries: Vec<serde_json::Value> = raw_log
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    let started = entries
        .iter()
        .find(|e| e["event"] == "started")
        .expect("started log");
    assert_eq!(
        started["pid"], started["pgid"],
        "child must lead its own process group"
    );
    let expected_wt = rex_wave_fs::canonicalize_existing_prefix(
        &repo.join(".worktrees/rex-wave-w-acp-life-author"),
    )
    .unwrap();
    assert_eq!(
        PathBuf::from(started["cwd"].as_str().unwrap()),
        expected_wt,
        "ManagedWorktree must create isolated git worktree"
    );
    assert_eq!(started["env"]["REX_WAVE_TOKEN"], token);
    assert_eq!(
        started["env"]["REX_WAVE_SOCKET"],
        sock_path.to_string_lossy().as_ref()
    );
    assert_eq!(
        started["env"]["REX_WAVE_WORKER_THREAD_ID"],
        thread_id.as_str()
    );
    assert!(started["env"]["T3CODE_THREAD_ID"].is_null());

    let session_new = entries
        .iter()
        .find(|e| e["event"] == "rx" && e["msg"]["method"] == "session/new")
        .expect("session/new request logged");
    let mcp_servers = session_new["msg"]["params"]["mcpServers"]
        .as_array()
        .unwrap();
    assert_eq!(mcp_servers.len(), 1);
    assert_eq!(mcp_servers[0]["name"], "rex-wave");
    assert_eq!(mcp_servers[0]["command"], "/usr/local/bin/rex-wave-test");
    assert_eq!(
        mcp_servers[0]["args"],
        serde_json::json!([
            "--socket",
            sock_path.to_string_lossy().as_ref(),
            "mcp",
            "--stdio"
        ])
    );
    let mcp_env = mcp_servers[0]["env"].as_array().unwrap();
    assert!(
        mcp_env
            .iter()
            .any(|kv| kv["name"] == "REX_WAVE_TOKEN" && kv["value"] == token)
    );

    let perm_reply = entries
        .iter()
        .find(|e| e["event"] == "perm_reply")
        .expect("permission reply logged");
    assert_eq!(perm_reply["reply"]["id"], 77);
    assert_eq!(
        perm_reply["reply"]["result"]["outcome"],
        serde_json::json!({"outcome": "selected", "optionId": "allow_once"})
    );

    conductor.abort();
}

#[tokio::test]
async fn acp_steer_cancels_active_turn_and_dispatches_followup_prompt() {
    let tmp = tempfile::tempdir().unwrap();
    let script = write_mock_script(tmp.path());
    let project_dir = tmp.path().join("plain-ws");
    std::fs::create_dir_all(&project_dir).unwrap();
    let log_path = tmp.path().join("steer.log.jsonl");

    let provider_spec = AcpProviderSpec::new(&script, ["acp"])
        .with_env("MOCK_ACP_LOG", log_path.to_string_lossy().into_owned())
        .with_env("MOCK_ACP_MODE", "steer_cancel");

    let cfg = AcpConfig::with_env_overrides(None, None)
        .with_provider("opencode", provider_spec)
        .with_state_dir(tmp.path().join("state"))
        .with_retire_grace(Duration::from_millis(300));

    let (router, bridge, conductor) = build_test_stack(tmp.path(), cfg);
    let op = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );

    let wave = WaveSlug::parse("w-steer").unwrap();
    let role = RoleName::parse("coder").unwrap();
    let project = ProjectId::parse(project_dir.to_string_lossy().into_owned()).unwrap();

    op.call(WaveStart {
        spec: WaveSpec {
            slug: wave.clone(),
            project,
            goal: Body::parse("Verify steer cancellation").unwrap(),
            defaults: AgentSpec {
                provider: ProviderId::parse("acp:opencode").unwrap(),
                model: None,
                account: None,
                reasoning: None,
                service_tier: None,
                permission: PermissionMode::Auto,
                workspace: Workspace::ProjectDefault,
            },
            output_dir: None,
            workers: vec![WorkerSpec {
                role: role.clone(),
                brief: Body::parse("Wait for cancel").unwrap(),
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

    // Wait until initial turn has streamed its opening chunk and is still Working.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(page) = bridge.page(&wave, &role, None, 0).await
            && page.events.iter().any(|e| {
                matches!(
                    &e.kind,
                    AgentEventKind::AssistantText { text } if text == "starting long turn"
                )
            })
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for initial chunk"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Issue Steer while turn 1 is active -> sends session/cancel, then dispatches prompt 2.
    op.call(WaveTell {
        wave: wave.clone(),
        role: role.clone(),
        body: Body::parse("pivot to fix tests").unwrap(),
        mode: SteerDeliveryMode::Steer,
    })
    .await
    .unwrap();

    wait_for_phase(&op, &wave, &role, Duration::from_secs(10), |p| {
        matches!(
            p,
            WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            }
        )
    })
    .await;

    let page = bridge.page(&wave, &role, None, 0).await.unwrap();
    assert!(page.events.iter().any(|e| matches!(
        &e.kind,
        AgentEventKind::AssistantText { text } if text == "steered: pivot to fix tests"
    )));

    let raw_log = std::fs::read_to_string(&log_path).unwrap();
    assert!(
        raw_log.contains("\"event\": \"cancelled\"") || raw_log.contains("\"event\":\"cancelled\""),
        "expected session/cancel in mock log: {raw_log}"
    );

    op.call(WaveRetireWorker {
        wave: wave.clone(),
        role: role.clone(),
        reason: None,
    })
    .await
    .unwrap();
    wait_for_phase(&op, &wave, &role, Duration::from_secs(10), |p| {
        matches!(p, WorkerPhase::Retired { .. })
    })
    .await;

    conductor.abort();
}

#[tokio::test]
async fn acp_permission_allow_for_session_and_deny_plus_error_and_check_live() {
    let tmp = tempfile::tempdir().unwrap();
    let script = write_mock_script(tmp.path());
    let existing_ws = tmp.path().join("custom-ws");
    std::fs::create_dir_all(&existing_ws).unwrap();
    let log_path = tmp.path().join("perms.log.jsonl");

    let perm_spec = AcpProviderSpec::new(&script, ["acp"])
        .with_env("MOCK_ACP_LOG", log_path.to_string_lossy().into_owned())
        .with_env("MOCK_ACP_MODE", "permission_variants");
    let err_spec =
        AcpProviderSpec::new(&script, ["acp"]).with_env("MOCK_ACP_MODE", "provider_error");
    let crash_spec =
        AcpProviderSpec::new(&script, ["acp"]).with_env("MOCK_ACP_MODE", "crash_stderr");

    let cfg = AcpConfig::with_env_overrides(None, None)
        .with_provider("opencode", perm_spec)
        .with_provider("antigravity", err_spec)
        .with_provider("crasher", crash_spec)
        .with_state_dir(tmp.path().join("state"))
        .with_retire_grace(Duration::from_millis(300));

    let (router, bridge, conductor) = build_test_stack(tmp.path(), cfg);
    let op = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );

    let wave = WaveSlug::parse("w-variants").unwrap();
    let r_perm = RoleName::parse("perm").unwrap();
    let r_err = RoleName::parse("err").unwrap();
    let r_crash = RoleName::parse("crash").unwrap();
    let project = ProjectId::parse(tmp.path().to_string_lossy().into_owned()).unwrap();

    op.call(WaveStart {
        spec: WaveSpec {
            slug: wave.clone(),
            project,
            goal: Body::parse("Verify permission and error variants").unwrap(),
            defaults: AgentSpec {
                provider: ProviderId::parse("acp:opencode").unwrap(),
                model: None,
                account: None,
                reasoning: None,
                service_tier: None,
                permission: PermissionMode::AcceptEdits,
                workspace: Workspace::ExistingPath {
                    path: existing_ws.clone(),
                },
            },
            output_dir: None,
            workers: vec![
                WorkerSpec {
                    role: r_perm.clone(),
                    brief: Body::parse("turn 1").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                },
                WorkerSpec {
                    role: r_err.clone(),
                    brief: Body::parse("trigger error").unwrap(),
                    agent: Some(AgentSpec {
                        provider: ProviderId::parse("acp:antigravity").unwrap(),
                        model: None,
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: PermissionMode::AcceptEdits,
                        workspace: Workspace::ProjectDefault,
                    }),
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                },
                WorkerSpec {
                    role: r_crash.clone(),
                    brief: Body::parse("trigger crash").unwrap(),
                    agent: Some(AgentSpec {
                        provider: ProviderId::parse("acp:crasher").unwrap(),
                        model: None,
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: PermissionMode::AcceptEdits,
                        workspace: Workspace::ProjectDefault,
                    }),
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                },
            ],
        },
        idem: None,
        config_defaults: Default::default(),
    })
    .await
    .unwrap();

    // Turn 1: AllowForSession.
    let b1 = wait_for_phase(&op, &wave, &r_perm, Duration::from_secs(10), |p| {
        matches!(
            p,
            WorkerPhase::Live {
                activity: Activity::Blocked { .. },
                ..
            }
        )
    })
    .await;
    let id1 = match b1 {
        WorkerPhase::Live {
            activity: Activity::Blocked { interaction },
            ..
        } => interaction.id,
        other => panic!("unexpected {other:?}"),
    };
    op.call(WaveResolve {
        wave: wave.clone(),
        role: r_perm.clone(),
        interaction: id1,
        resolution: InteractionResolution::AllowForSession {
            granted_permissions: None,
        },
    })
    .await
    .unwrap();
    wait_for_phase(&op, &wave, &r_perm, Duration::from_secs(10), |p| {
        matches!(
            p,
            WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            }
        )
    })
    .await;

    // Turn 2: Deny.
    op.call(WaveTell {
        wave: wave.clone(),
        role: r_perm.clone(),
        body: Body::parse("turn 2").unwrap(),
        mode: SteerDeliveryMode::Auto,
    })
    .await
    .unwrap();
    let b2 = wait_for_phase(&op, &wave, &r_perm, Duration::from_secs(10), |p| {
        matches!(
            p,
            WorkerPhase::Live {
                activity: Activity::Blocked { .. },
                ..
            }
        )
    })
    .await;
    let id2 = match b2 {
        WorkerPhase::Live {
            activity: Activity::Blocked { interaction },
            ..
        } => interaction.id,
        other => panic!("unexpected {other:?}"),
    };
    op.call(WaveResolve {
        wave: wave.clone(),
        role: r_perm.clone(),
        interaction: id2,
        resolution: InteractionResolution::Deny,
    })
    .await
    .unwrap();
    wait_for_phase(&op, &wave, &r_perm, Duration::from_secs(10), |p| {
        matches!(
            p,
            WorkerPhase::Live {
                activity: Activity::Idle,
                ..
            }
        )
    })
    .await;

    // Provider error & crash workers must report Activity::Errored.
    let err_phase = wait_for_phase(&op, &wave, &r_err, Duration::from_secs(10), |p| {
        matches!(
            p,
            WorkerPhase::Live {
                activity: Activity::Errored { .. },
                ..
            }
        )
    })
    .await;
    assert!(matches!(
        err_phase,
        WorkerPhase::Live {
            activity: Activity::Errored { ref error },
            ..
        } if error.contains("rate limit")
    ));

    let crash_phase = wait_for_phase(&op, &wave, &r_crash, Duration::from_secs(10), |p| {
        matches!(
            p,
            WorkerPhase::Live {
                activity: Activity::Errored { .. },
                ..
            }
        )
    })
    .await;
    assert!(matches!(
        crash_phase,
        WorkerPhase::Live {
            activity: Activity::Errored { ref error },
            ..
        } if error.contains("fatal backend panic")
    ));

    // Verify AllowForSession -> opt_session and Deny -> opt_reject.
    let raw_log = std::fs::read_to_string(&log_path).unwrap();
    let entries: Vec<serde_json::Value> = raw_log
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let reply1 = entries
        .iter()
        .find(|e| e["event"] == "perm_reply" && e["turn"] == 1)
        .unwrap();
    assert_eq!(
        reply1["reply"]["result"]["outcome"],
        serde_json::json!({"outcome": "selected", "optionId": "opt_session"})
    );
    let reply2 = entries
        .iter()
        .find(|e| e["event"] == "perm_reply" && e["turn"] == 2)
        .unwrap();
    assert_eq!(
        reply2["reply"]["result"]["outcome"],
        serde_json::json!({"outcome": "selected", "optionId": "opt_reject"})
    );

    // Fresh bridge simulating daemon restart reports lost ACP process via check_live.
    let restarted = AcpHost(AcpBridge::new(AcpConfig::with_env_overrides(None, None)));
    let lost_tid = HostThreadId::parse("acp:w-variants:perm:a1").unwrap();
    let probe = restarted.check_live(&lost_tid).await.unwrap();
    assert!(matches!(
        probe,
        Some(Observation::Activity {
            activity: Activity::Errored { ref error },
            ..
        }) if error.contains("restart")
    ));

    bridge.shutdown_all().await;
    conductor.abort();
}
