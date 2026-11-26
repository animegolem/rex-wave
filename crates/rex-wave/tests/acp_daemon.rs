#![allow(clippy::disallowed_methods)]

mod common;

use common::{fresh_dir, rex_wave_at};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};

const DAEMON_MOCK_ACP: &str = r##"#!/usr/bin/env python3
import json
import os
import subprocess
import sys

REX_BIN = os.environ["MOCK_REX_WAVE_BIN"]

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

session_id = "daemon-acp-sess-1"

for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    msg = json.loads(line)
    method = msg.get("method")
    msg_id = msg.get("id")

    if method == "initialize":
        send({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"protocolVersion": 1, "agentCapabilities": {}},
        })
    elif method == "session/new":
        # Verify mcpServers contains rex-wave stdio entry pointing at daemon socket.
        servers = msg.get("params", {}).get("mcpServers", [])
        assert len(servers) == 1 and servers[0]["name"] == "rex-wave", f"unexpected mcpServers: {servers}"
        send({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"sessionId": session_id},
        })
    elif method == "session/prompt":
        # Post back to the wave room using inherited REX_WAVE_SOCKET + REX_WAVE_TOKEN.
        project = os.environ["MOCK_PROJECT_ID"]
        res = subprocess.run(
            [REX_BIN, "post", f"#{project}/wave-acp-e2e", "hello from native acp worker"],
            capture_output=True,
            text=True,
        )
        assert res.returncode == 0, f"worker CLI post failed: {res.stderr}"
        send({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "posted to wave room"},
                },
            },
        })
        send({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"stopReason": "end_turn"},
        })
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
fn daemon_runner_acp_spawns_authenticates_worker_and_retires() {
    let dir = fresh_dir("acp-e2e");
    let sock = dir.0.join("daemon.sock");
    let sandbox = dir.0.join("sandbox");
    let project_dir = dir.0.join("proj");
    std::fs::create_dir_all(&sandbox).unwrap();
    std::fs::create_dir_all(&project_dir).unwrap();
    std::fs::write(sandbox.join(".rex-wave-sandbox"), b"ok\n").unwrap();

    let mock_bin = dir.0.join("mock-agy.py");
    std::fs::write(&mock_bin, DAEMON_MOCK_ACP).unwrap();
    std::fs::set_permissions(&mock_bin, std::fs::Permissions::from_mode(0o755)).unwrap();

    let project_str = project_dir.to_string_lossy().into_owned();

    let mut daemon_cmd = rex_wave_at(&sock);
    daemon_cmd
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "daemon",
            "--runner",
            "acp",
            "--mail-sandbox",
            sandbox.to_str().unwrap(),
            "--wake-bell",
            "127.0.0.1:0",
        ])
        .env("REX_WAVE_ACP_AGY_BIN", &mock_bin)
        .env("MOCK_REX_WAVE_BIN", env!("CARGO_BIN_EXE_rex-wave"))
        .env("MOCK_PROJECT_ID", &project_str)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    let _daemon = KillOnDrop(daemon_cmd.spawn().expect("spawn daemon"));
    wait_for_socket(&sock, Duration::from_secs(10));

    let start = rex_wave_at(&sock)
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "--json",
            "wave",
            "start",
            "acp-e2e",
            "--project",
            &project_str,
            "--goal",
            "Verify daemon ACP wiring",
            "--worker",
            "author=acp:antigravity",
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

    // Wait for worker to reach Idle.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let st = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "--json",
                "wave",
                "status",
                "acp-e2e",
            ])
            .output()
            .unwrap();
        assert!(st.status.success());
        let json: serde_json::Value = serde_json::from_slice(&st.stdout).unwrap();
        let phase = &json["value"]["workers"][0][2];
        if phase["phase"] == "live" && phase["activity"]["activity"] == "idle" {
            assert_eq!(phase["thread"], "acp:acp-e2e:author:a1");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for ACP worker Idle; status={json}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // Verify FeedRouter serves AcpFeed::page through the live daemon UDS.
    let tp = rex_wave_at(&sock)
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "--json",
            "call",
            "thread_page",
            r#"{"wave":"acp-e2e","role":"author"}"#,
        ])
        .output()
        .unwrap();
    assert!(
        tp.status.success(),
        "thread_page failed: {}",
        String::from_utf8_lossy(&tp.stderr)
    );
    let tp_json: serde_json::Value = serde_json::from_slice(&tp.stdout).unwrap();
    assert_eq!(tp_json["value"]["thread"], "acp:acp-e2e:author:a1");
    let tp_events = tp_json["value"]["events"].as_array().unwrap();
    assert!(
        tp_events
            .iter()
            .any(|e| e["kind"]["kind"] == "assistant_text"
                && e["kind"]["text"] == "posted to wave room"),
        "expected streamed assistant_text in AcpFeed thread_page: {tp_json}"
    );

    // Verify the worker's own CLI post authenticated as Principal::Worker via REX_WAVE_SOCKET + REX_WAVE_TOKEN.
    let read = rex_wave_at(&sock)
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "--json",
            "read",
            &format!("#{project_str}/wave-acp-e2e"),
        ])
        .output()
        .unwrap();
    assert!(
        read.status.success(),
        "read wave room failed: {}",
        String::from_utf8_lossy(&read.stderr)
    );
    let page: serde_json::Value = serde_json::from_slice(&read.stdout).unwrap();
    let entries = page["value"]["entries"].as_array().unwrap();
    assert!(
        entries.iter().any(|e| {
            e["by"]["kind"] == "worker"
                && e["by"]["wave"] == "acp-e2e"
                && e["by"]["role"] == "author"
                && e["event"]["event"]["body"] == "hello from native acp worker"
        }),
        "expected authenticated worker post in wave room: {page}"
    );

    // Retire worker cleanly.
    let retire = rex_wave_at(&sock)
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "wave",
            "retire",
            "acp-e2e",
            "author",
        ])
        .output()
        .unwrap();
    assert!(
        retire.status.success(),
        "wave retire failed: {}",
        String::from_utf8_lossy(&retire.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let st = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "--json",
                "wave",
                "status",
                "acp-e2e",
            ])
            .output()
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&st.stdout).unwrap();
        if json["value"]["workers"][0][2]["phase"] == "retired" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for Retired; status={json}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn daemon_bb_mode_still_routes_explicit_acp_opencode_workers() {
    let dir = fresh_dir("acp-bb-mix");
    let sock = dir.0.join("daemon.sock");
    let sandbox = dir.0.join("sandbox");
    let project_dir = dir.0.join("proj");
    std::fs::create_dir_all(&sandbox).unwrap();
    std::fs::create_dir_all(&project_dir).unwrap();
    std::fs::write(sandbox.join(".rex-wave-sandbox"), b"ok\n").unwrap();

    let mock_bin = dir.0.join("mock-opencode.py");
    std::fs::write(&mock_bin, DAEMON_MOCK_ACP).unwrap();
    std::fs::set_permissions(&mock_bin, std::fs::Permissions::from_mode(0o755)).unwrap();

    let project_str = project_dir.to_string_lossy().into_owned();

    let mut daemon_cmd = rex_wave_at(&sock);
    daemon_cmd
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "daemon",
            "--runner",
            "bb",
            "--bb-url",
            "ws://127.0.0.1:1/disabled",
            "--mail-sandbox",
            sandbox.to_str().unwrap(),
            "--wake-bell",
            "127.0.0.1:0",
        ])
        .env("REX_WAVE_ACP_OPENCODE_BIN", &mock_bin)
        .env("MOCK_REX_WAVE_BIN", env!("CARGO_BIN_EXE_rex-wave"))
        .env("MOCK_PROJECT_ID", &project_str)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    let _daemon = KillOnDrop(daemon_cmd.spawn().expect("spawn daemon"));
    wait_for_socket(&sock, Duration::from_secs(10));

    let start = rex_wave_at(&sock)
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "--json",
            "wave",
            "start",
            "acp-e2e",
            "--project",
            &project_str,
            "--goal",
            "Verify always-registered ACP slot in BB mode",
            "--worker",
            "coder=acp:opencode",
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
                "acp-e2e",
            ])
            .output()
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&st.stdout).unwrap();
        let phase = &json["value"]["workers"][0][2];
        if phase["phase"] == "live" && phase["activity"]["activity"] == "idle" {
            assert_eq!(phase["thread"], "acp:acp-e2e:coder:a1");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for acp:opencode Idle in BB mode; status={json}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}
