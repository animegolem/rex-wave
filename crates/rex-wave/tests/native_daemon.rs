#![allow(clippy::disallowed_methods)]

mod common;

use common::{fresh_dir, rex_wave_at};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const DAEMON_MOCK_CLAUDE: &str = r##"#!/usr/bin/env python3
import json, os, subprocess, sys, time

REX_BIN = os.environ["MOCK_REX_WAVE_BIN"]
PROJECT = os.environ["MOCK_PROJECT_ID"]

assert os.environ.get("REX_WAVE_BB_TOKEN") is None, "daemon BB token leaked"
assert os.environ.get("REX_WAVE_T3_TOKEN") is None, "daemon T3 token leaked"
assert os.environ.get("REX_WAVE_OPERATOR") is None, "daemon operator leaked"
assert os.environ.get("CLAUDECODE") is None, "CLAUDECODE leaked"
assert "--mcp-config" in sys.argv[1:], f"missing --mcp-config: {sys.argv}"

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

_ = json.loads(sys.stdin.readline())
emit({"type": "system", "subtype": "init", "session_id": "daemon_cl_1", "model": "claude-opus-5-5"})
res = subprocess.run(
    [REX_BIN, "post", f"#{PROJECT}/wave-native-e2e", "hello from native claude worker"],
    capture_output=True,
    text=True,
)
assert res.returncode == 0, f"claude worker post failed: {res.stderr}"
emit({"type": "assistant", "message": {"content": [{"type": "text", "text": "Claude posted"}]}})
emit({"type": "result", "subtype": "success", "is_error": False, "session_id": "daemon_cl_1"})
for _ in sys.stdin:
    pass
"##;

const DAEMON_MOCK_CODEX: &str = r##"#!/usr/bin/env python3
import json, os, subprocess, sys

REX_BIN = os.environ["MOCK_REX_WAVE_BIN"]
PROJECT = os.environ["MOCK_PROJECT_ID"]

assert len(sys.argv) >= 2 and sys.argv[1] == "app-server", f"unexpected argv: {sys.argv}"
assert os.environ.get("REX_WAVE_BB_TOKEN") is None, "daemon BB token leaked"
assert os.environ.get("REX_WAVE_T3_TOKEN") is None, "daemon T3 token leaked"
assert os.environ.get("REX_WAVE_OPERATOR") is None, "daemon operator leaked"

# Parse `-c mcp_servers.rex-wave.*` overrides and verify Codex MCP env filtering works end-to-end.
mcp_cfg = {}
argv = sys.argv[2:]
i = 0
while i < len(argv):
    if argv[i] == "-c" and i + 1 < len(argv):
        kv = argv[i + 1]
        if "=" in kv:
            k, v = kv.split("=", 1)
            mcp_cfg[k] = json.loads(v)
        i += 2
    else:
        i += 1

mcp_cmd = mcp_cfg["mcp_servers.rex-wave.command"]
mcp_args = mcp_cfg["mcp_servers.rex-wave.args"]
mcp_env_vars = mcp_cfg["mcp_servers.rex-wave.env_vars"]
assert "REX_WAVE_TOKEN" in mcp_env_vars, f"missing REX_WAVE_TOKEN in env_vars: {mcp_env_vars}"

def call_mcp_with_filtered_env():
    # Mirror real Codex 0.172.0 MCP subprocess env filtering: pass ONLY base OS env + listed env_vars.
    filtered_env = {}
    for base_key in ("PATH", "HOME", "TMPDIR", "SYSTEMROOT"):
        if base_key in os.environ:
            filtered_env[base_key] = os.environ[base_key]
    for allowed_key in mcp_env_vars:
        if allowed_key in os.environ:
            filtered_env[allowed_key] = os.environ[allowed_key]

    proc = subprocess.Popen(
        [mcp_cmd, *mcp_args],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=filtered_env,
        text=True,
    )
    reqs = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "codex-mcp-check", "version": "1.0"},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "room_post",
                "arguments": {
                    "room": {
                        "ref": "channel",
                        "scope": {"scope": "project", "project": PROJECT},
                        "name": "wave-native-e2e",
                    },
                    "body": "hello from native codex worker",
                },
            },
        },
    ]
    out, err = proc.communicate("".join(json.dumps(r) + "\n" for r in reqs), timeout=10)
    assert proc.returncode == 0, f"mcp --stdio exited {proc.returncode}: {err}"
    replies = [json.loads(l) for l in out.splitlines() if l.strip()]
    tool_reply = next(r for r in replies if r.get("id") == 2)
    assert not tool_reply["result"].get("isError", False), f"mcp tool failed: {tool_reply}"

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
        send({"jsonrpc": "2.0", "id": rid, "result": {"userAgent": "mock-codex"}})
    elif m == "account/read":
        send({"jsonrpc": "2.0", "id": rid, "result": {"account": {"type": "chatgpt", "email": "ops@example.com"}, "requiresOpenaiAuth": True}})
    elif m == "thread/start":
        send({"jsonrpc": "2.0", "id": rid, "result": {"thread": {"id": "daemon_cx_1"}, "model": "gpt-6.1-sol"}})
    elif m == "turn/start":
        send({"jsonrpc": "2.0", "id": rid, "result": {"turn": {"id": "t1", "status": "inProgress"}}})
        call_mcp_with_filtered_env()
        send({"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": {"threadId": "daemon_cx_1", "turnId": "t1", "itemId": "am1", "delta": "Codex posted"}})
        send({"jsonrpc": "2.0", "method": "turn/completed", "params": {"threadId": "daemon_cx_1", "turn": {"id": "t1", "status": "completed"}}})
"##;

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_for_socket(sock: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if std::os::unix::net::UnixStream::connect(sock).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("daemon socket {} never became connectable", sock.display());
}

#[test]
fn daemon_runner_native_routes_claude_and_codex_authenticates_and_cleans_up_on_sigterm() {
    let dir = fresh_dir("native-e2e");
    let sock = dir.0.join("daemon.sock");
    let sandbox = dir.0.join("sandbox");
    let project_dir = dir.0.join("proj");
    std::fs::create_dir_all(&sandbox).unwrap();
    std::fs::create_dir_all(&project_dir).unwrap();
    std::fs::write(sandbox.join(".rex-wave-sandbox"), b"ok\n").unwrap();

    let claude_bin = dir.0.join("mock-claude.py");
    std::fs::write(&claude_bin, DAEMON_MOCK_CLAUDE).unwrap();
    std::fs::set_permissions(&claude_bin, std::fs::Permissions::from_mode(0o755)).unwrap();

    let codex_bin = dir.0.join("mock-codex.py");
    std::fs::write(&codex_bin, DAEMON_MOCK_CODEX).unwrap();
    std::fs::set_permissions(&codex_bin, std::fs::Permissions::from_mode(0o755)).unwrap();

    let project_str = project_dir.to_string_lossy().into_owned();

    let mut daemon_cmd = rex_wave_at(&sock);
    daemon_cmd
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "daemon",
            "--runner",
            "native",
            "--mail-sandbox",
            sandbox.to_str().unwrap(),
            "--wake-bell",
            "127.0.0.1:0",
        ])
        .env("REX_WAVE_BB_TOKEN", "secret-bb-token")
        .env("REX_WAVE_T3_TOKEN", "secret-t3-token")
        .env("REX_WAVE_OPERATOR", "ops")
        .env("CLAUDECODE", "1")
        .env("MOCK_REX_WAVE_BIN", env!("CARGO_BIN_EXE_rex-wave"))
        .env("MOCK_PROJECT_ID", &project_str)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    let mut daemon = KillOnDrop(daemon_cmd.spawn().expect("spawn daemon"));
    wait_for_socket(&sock, Duration::from_secs(10));

    // Configure both binaries dynamically through `rex-wave config set`.
    for (key, bin) in [
        ("harnesses.claude.binary", &claude_bin),
        ("harnesses.codex.binary", &codex_bin),
    ] {
        let out = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "config",
                "set",
                key,
                bin.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "config set {key} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    let start = rex_wave_at(&sock)
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "--json",
            "wave",
            "start",
            "native-e2e",
            "--project",
            &project_str,
            "--goal",
            "Verify native Claude + Codex daemon routing",
            "--worker",
            "builder=claude-code",
            "--worker",
            "reviewer=codex",
            "--workspace",
            "project-default",
        ])
        .output()
        .unwrap();
    assert!(
        start.status.success(),
        "wave start failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let st = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "--json",
                "wave",
                "status",
                "native-e2e",
            ])
            .output()
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&st.stdout).unwrap();
        if json["value"]["counts"]["idle"] == 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for both native workers Idle; status={json}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    let read_out = rex_wave_at(&sock)
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "read",
            &format!("#{project_str}/wave-native-e2e"),
        ])
        .output()
        .unwrap();
    let room_text = String::from_utf8_lossy(&read_out.stdout);
    assert!(room_text.contains("hello from native claude worker"));
    assert!(room_text.contains("hello from native codex worker"));

    let tokens_dir = dir.0.join("worker-tokens");
    let kill_status = Command::new("kill")
        .args(["-TERM", &daemon.0.id().to_string()])
        .status()
        .unwrap();
    assert!(kill_status.success());
    let exit_deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(st) = daemon.0.try_wait().unwrap() {
            break st;
        }
        assert!(
            Instant::now() < exit_deadline,
            "daemon did not exit on SIGTERM"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert!(status.success(), "daemon SIGTERM exit status={status}");
    let remaining_tokens = std::fs::read_dir(&tokens_dir)
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(
        remaining_tokens, 0,
        "SIGTERM must unlink all Claude and Codex worker tokens"
    );
}
