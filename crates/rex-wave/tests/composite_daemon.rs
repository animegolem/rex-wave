use rex_wave_t3::transport::ws_accept;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};

mod common;
use common::{fresh_dir, rex_wave_at, t3_home};

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

#[derive(Default)]
struct MiniT3State {
    expected_token: String,
    launched: Vec<Value>,
}

fn encode_server_ws_text(text: &str) -> Vec<u8> {
    let payload = text.as_bytes();
    let mut frame = Vec::with_capacity(10 + payload.len());
    frame.push(0x81);
    if payload.len() < 126 {
        frame.push(payload.len() as u8);
    } else if payload.len() <= u16::MAX as usize {
        frame.push(126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        frame.push(127);
        frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    frame.extend_from_slice(payload);
    frame
}

async fn read_ws_frame<R: tokio::io::AsyncRead + Unpin>(reader: &mut R) -> Option<(u8, Vec<u8>)> {
    let mut hdr = [0u8; 2];
    reader.read_exact(&mut hdr).await.ok()?;
    let opcode = hdr[0] & 0x0f;
    let masked = (hdr[1] & 0x80) != 0;
    let len = match (hdr[1] & 0x7f) as usize {
        126 => {
            let mut b = [0u8; 2];
            reader.read_exact(&mut b).await.ok()?;
            u16::from_be_bytes(b) as usize
        }
        127 => {
            let mut b = [0u8; 8];
            reader.read_exact(&mut b).await.ok()?;
            u64::from_be_bytes(b) as usize
        }
        n => n,
    };
    let mask = if masked {
        let mut k = [0u8; 4];
        reader.read_exact(&mut k).await.ok()?;
        Some(k)
    } else {
        None
    };
    let mut payload = vec![0u8; len];
    if len > 0 {
        reader.read_exact(&mut payload).await.ok()?;
        if let Some(k) = mask {
            for (i, b) in payload.iter_mut().enumerate() {
                *b ^= k[i & 3];
            }
        }
    }
    Some((opcode, payload))
}

async fn read_http(stream: &mut TcpStream) -> Option<(String, String, HashMap<String, String>)> {
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 1024];
    let end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
    };
    let head = std::str::from_utf8(&buf[..end]).ok()?.to_owned();
    let mut lines = head.split("\r\n");
    let first = lines.next()?;
    let mut parts = first.split_whitespace();
    let method = parts.next()?.to_owned();
    let target = parts.next()?.to_owned();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
        }
    }
    Some((method, target, headers))
}

async fn write_json(stream: &mut TcpStream, status: u16, reason: &str, body: &Value) {
    let bytes = serde_json::to_vec(body).unwrap();
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        bytes.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(&bytes).await;
    let _ = stream.flush().await;
}

async fn spawn_mock_t3(
    expected_token: &str,
) -> (
    SocketAddr,
    Arc<Mutex<MiniT3State>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(Mutex::new(MiniT3State {
        expected_token: expected_token.to_owned(),
        launched: Vec::new(),
    }));
    let st = state.clone();
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let st = st.clone();
            tokio::spawn(async move {
                let Some((method, target, headers)) = read_http(&mut stream).await else {
                    return;
                };
                if headers
                    .get("upgrade")
                    .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
                {
                    let Some(key) = headers.get("sec-websocket-key") else {
                        return;
                    };
                    let accept = ws_accept(key);
                    let resp = format!(
                        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
                    );
                    if stream.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                    let (mut r, mut w) = stream.into_split();
                    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
                    let writer = tokio::spawn(async move {
                        while let Some(msg) = rx.recv().await {
                            if w.write_all(&encode_server_ws_text(&msg)).await.is_err() {
                                break;
                            }
                        }
                    });
                    while let Some((op, payload)) = read_ws_frame(&mut r).await {
                        if op == 0x8 {
                            break;
                        }
                        if op != 0x1 {
                            continue;
                        }
                        let Ok(val) = serde_json::from_slice::<Value>(&payload) else {
                            continue;
                        };
                        if val.get("_tag").and_then(Value::as_str) != Some("Request") {
                            continue;
                        }
                        let id = val["id"].as_str().unwrap_or("").to_owned();
                        let tag = val["tag"].as_str().unwrap_or("");
                        match tag {
                            "orchestration.subscribeShell" => {
                                let chunk = json!({
                                    "_tag": "Chunk",
                                    "requestId": id,
                                    "values": [{
                                        "kind": "snapshot",
                                        "snapshot": {
                                            "snapshotSequence": 1,
                                            "projects": [{
                                                "id": "proj_t3",
                                                "title": "t3-demo",
                                                "workspaceRoot": "/tmp/t3-demo"
                                            }],
                                            "threads": [],
                                            "archivedThreads": []
                                        }
                                    }]
                                });
                                let _ = tx.send(chunk.to_string());
                            }
                            "orchestration.launchThread" => {
                                let p = val["payload"].clone();
                                let tid = p["threadId"].as_str().unwrap_or("t3:unknown").to_owned();
                                st.lock().await.launched.push(p);
                                let exit = json!({
                                    "_tag": "Exit",
                                    "requestId": id,
                                    "exit": {
                                        "_tag": "Success",
                                        "value": { "threadId": tid }
                                    }
                                });
                                let _ = tx.send(exit.to_string());
                            }
                            _ => {
                                let exit = json!({
                                    "_tag": "Exit",
                                    "requestId": id,
                                    "exit": { "_tag": "Success", "value": {} }
                                });
                                let _ = tx.send(exit.to_string());
                            }
                        }
                    }
                    writer.abort();
                    return;
                }

                let expected = st.lock().await.expected_token.clone();
                let auth_ok = headers
                    .get("authorization")
                    .is_some_and(|v| v == &format!("Bearer {expected}"));
                if target != "/.well-known/t3/environment" && !auth_ok {
                    write_json(
                        &mut stream,
                        401,
                        "Unauthorized",
                        &json!({"error": "bad token"}),
                    )
                    .await;
                    return;
                }
                match (method.as_str(), target.as_str()) {
                    ("GET", "/.well-known/t3/environment") => {
                        write_json(&mut stream, 200, "OK", &json!({"ok": true})).await;
                    }
                    ("POST", "/api/auth/websocket-ticket") => {
                        write_json(
                            &mut stream,
                            200,
                            "OK",
                            &json!({"ticket": "tkt_1", "expiresAt": "2099-01-01T00:00:00Z"}),
                        )
                        .await;
                    }
                    ("GET", "/api/projects") => {
                        write_json(
                            &mut stream,
                            200,
                            "OK",
                            &json!({
                                "projects": [{
                                    "id": "proj_t3",
                                    "title": "t3-demo",
                                    "workspaceRoot": "/tmp/t3-demo"
                                }]
                            }),
                        )
                        .await;
                    }
                    _ => {
                        write_json(
                            &mut stream,
                            404,
                            "Not Found",
                            &json!({"error": "not found"}),
                        )
                        .await;
                    }
                }
            });
        }
    });
    (addr, state, task)
}

async fn http_get_json(addr: SocketAddr, path: &str) -> (u16, Value) {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let sep = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = std::str::from_utf8(&buf[..sep]).unwrap();
    let status: u16 = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let val = serde_json::from_slice(&buf[sep + 4..]).unwrap();
    (status, val)
}

#[tokio::test]
async fn daemon_refuses_non_loopback_http_and_runs_composite_t3_host_with_worker_token_minter() {
    let dir = fresh_dir("daemon");
    let sandbox = dir.0.join("mail-sandbox");
    let t3_home = t3_home(&dir.0);
    std::fs::create_dir_all(&sandbox).unwrap();
    std::fs::write(sandbox.join(".rex-wave-sandbox"), "test\n").unwrap();

    let bad_sock = dir.0.join("bad.sock");
    let bad_out = rex_wave_at(&bad_sock)
        .args([
            "--socket",
            bad_sock.to_str().unwrap(),
            "daemon",
            "--mail-sandbox",
            sandbox.to_str().unwrap(),
            "--runner",
            "bb",
            "--t3-home",
            t3_home.to_str().unwrap(),
            "--http",
            "0.0.0.0:0",
        ])
        .output()
        .unwrap();
    assert!(!bad_out.status.success());
    assert!(
        String::from_utf8_lossy(&bad_out.stderr).contains("loopback"),
        "stderr: {}",
        String::from_utf8_lossy(&bad_out.stderr)
    );

    let t3_token = "t3_secret_test_tok";
    let (t3_addr, t3_state, t3_task) = spawn_mock_t3(t3_token).await;
    let http_port = free_port().await;
    let bell_port = free_port().await;
    let unused_bb_port = free_port().await;
    let sock = dir.0.join("daemon.sock");
    let journal = dir.0.join("journal.sqlite3");

    let child = rex_wave_at(&sock)
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "daemon",
            "--journal",
            journal.to_str().unwrap(),
            "--mail-sandbox",
            sandbox.to_str().unwrap(),
            "--runner",
            "both",
            "--t3-home",
            t3_home.to_str().unwrap(),
            "--bb-url",
            &format!("ws://127.0.0.1:{unused_bb_port}/bridge"),
            "--t3-url",
            &format!("http://{t3_addr}"),
            "--t3-token",
            t3_token,
            "--wake-bell",
            &format!("127.0.0.1:{bell_port}"),
            "--http",
            &format!("127.0.0.1:{http_port}"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rex-wave daemon");
    let _guard = ChildGuard(child);

    let http_addr: SocketAddr = format!("127.0.0.1:{http_port}").parse().unwrap();
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            if sock.exists() && TcpStream::connect(http_addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("daemon UDS and loopback HTTP became ready");

    let (st, health) = http_get_json(http_addr, "/v1/health").await;
    assert_eq!(st, 200);
    assert_eq!(health["ok"], true);
    assert_eq!(health["principal"]["kind"], "operator");

    let sock_str = sock.to_str().unwrap().to_owned();
    let start_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&sock_str)
            .args([
                "--socket",
                &sock_str,
                "--json",
                "wave",
                "start",
                "mixed-w1",
                "--project",
                "proj_t3",
                "--goal",
                "Verify CompositeHost and T3 WorkerTokenMinter",
                "--worker",
                "gemini-w=antigravity/gemini-4-neon",
                "--workspace",
                "project-default",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        start_out.status.success(),
        "wave start failed: {}",
        String::from_utf8_lossy(&start_out.stderr)
    );

    let expected_tid = "t3:mixed-w1:gemini-w:a1";
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            let launched = t3_state.lock().await.launched.len();
            if launched >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("CompositeHost routed antigravity worker to T3Host");

    let token_file = rex_wave_t3::worker_token_path(&dir.0, expected_tid).unwrap();
    let meta = std::fs::symlink_metadata(&token_file).expect("worker token file provisioned");
    assert_eq!(meta.permissions().mode() & 0o777, 0o600);

    let sock_str2 = sock.to_str().unwrap().to_owned();
    let post_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&sock_str2)
            .env("T3CODE_THREAD_ID", expected_tid)
            .args([
                "--socket",
                &sock_str2,
                "--json",
                "post",
                "#proj_t3/wave-mixed-w1",
                "gemini-w reporting in via bash CLI",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        post_out.status.success(),
        "t3 worker bash post failed: {}",
        String::from_utf8_lossy(&post_out.stderr)
    );

    let (msg_st, msg_page) =
        http_get_json(http_addr, "/v1/rooms/%23proj_t3%2Fwave-mixed-w1/messages").await;
    assert_eq!(msg_st, 200);
    let entries = msg_page["entries"].as_array().unwrap();
    assert!(entries.iter().any(|e| {
        e["by"] == json!({"kind": "worker", "wave": "mixed-w1", "role": "gemini-w"})
            && e["event"]["event"]["body"].as_str() == Some("gemini-w reporting in via bash CLI")
    }));

    let (sb_st, sb) = http_get_json(http_addr, "/v1/sidebar").await;
    assert_eq!(sb_st, 200);
    assert!(
        sb["projects"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["project"] == "proj_t3" && p["name"] == "t3-demo")
    );

    t3_task.abort();
}
