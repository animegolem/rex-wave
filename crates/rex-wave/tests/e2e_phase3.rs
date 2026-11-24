use rex_wave_mailbridge::{GATE1_CASE_IDS, current_process_start};
use rex_wave_t3::transport::ws_accept;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{BufRead, BufReader as StdBufReader};
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc, oneshot};

static NONCE: AtomicU64 = AtomicU64::new(1);

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct DirGuard(PathBuf);
impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn snapshot_tree(root: &Path) -> Vec<(PathBuf, u64, SystemTime)> {
    fn walk(base: &Path, cur: &Path, out: &mut Vec<(PathBuf, u64, SystemTime)>) {
        let mut entries: Vec<_> = std::fs::read_dir(cur)
            .unwrap()
            .map(|e| e.unwrap())
            .collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            let meta = std::fs::symlink_metadata(&path).unwrap();
            let rel = path.strip_prefix(base).unwrap().to_path_buf();
            out.push((rel, meta.len(), meta.modified().unwrap()));
            if meta.file_type().is_dir() {
                walk(base, &path, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out
}

fn fresh_dir(label: &str) -> DirGuard {
    let n = NONCE.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("rex-wave-e2e3-{label}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    DirGuard(p)
}

fn bin_path() -> &'static str {
    env!("CARGO_BIN_EXE_rex-wave")
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

async fn read_mock_http(
    stream: &mut TcpStream,
) -> Option<(String, String, HashMap<String, String>)> {
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

async fn write_mock_json(stream: &mut TcpStream, status: u16, reason: &str, body: &Value) {
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
                let Some((method, target, headers)) = read_mock_http(&mut stream).await else {
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
                                                "id": "proj_e2e",
                                                "title": "phase3-e2e",
                                                "workspaceRoot": "/tmp/phase3-e2e"
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
                    write_mock_json(
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
                        write_mock_json(&mut stream, 200, "OK", &json!({"ok": true})).await;
                    }
                    ("POST", "/api/auth/websocket-ticket") => {
                        write_mock_json(
                            &mut stream,
                            200,
                            "OK",
                            &json!({"ticket": "tkt_e2e", "expiresAt": "2099-01-01T00:00:00Z"}),
                        )
                        .await;
                    }
                    ("GET", "/api/projects") => {
                        write_mock_json(
                            &mut stream,
                            200,
                            "OK",
                            &json!({
                                "projects": [{
                                    "id": "proj_e2e",
                                    "title": "phase3-e2e",
                                    "workspaceRoot": "/tmp/phase3-e2e"
                                }]
                            }),
                        )
                        .await;
                    }
                    _ => {
                        write_mock_json(
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

struct IsolatedDaemon {
    dir: DirGuard,
    sock: PathBuf,
    http_addr: SocketAddr,
    t3_addr: SocketAddr,
    t3_token: String,
    t3_state: Arc<Mutex<MiniT3State>>,
    t3_task: tokio::task::JoinHandle<()>,
    _child: ChildGuard,
}

impl Drop for IsolatedDaemon {
    fn drop(&mut self) {
        self.t3_task.abort();
    }
}

impl IsolatedDaemon {
    async fn spawn_daemon_process(
        root: &Path,
        t3_addr: SocketAddr,
        t3_token: &str,
    ) -> (PathBuf, SocketAddr, ChildGuard) {
        let sandbox = root.join("mail-sandbox");
        let cfg_dir = root.join("config");
        let t3_home = root.join("t3-home");
        std::fs::create_dir_all(&t3_home).unwrap();
        let bell_port = free_port().await;
        let unused_bb_port = free_port().await;
        let sock = root.join("daemon.sock");
        let journal = root.join("journal.sqlite3");

        let mut child = Command::new(bin_path())
            .env("XDG_CONFIG_HOME", &cfg_dir)
            .env_remove("REX_WAVE_TOKEN")
            .env_remove("T3CODE_THREAD_ID")
            .env_remove("REX_WAVE_T3_HOME")
            .env_remove("REX_WAVE_T3_URL")
            .env_remove("REX_WAVE_T3_TOKEN")
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
                "--bb-url",
                &format!("ws://127.0.0.1:{unused_bb_port}/bridge"),
                "--t3-home",
                t3_home.to_str().unwrap(),
                "--t3-url",
                &format!("http://{t3_addr}"),
                "--t3-token",
                t3_token,
                "--wake-bell",
                &format!("127.0.0.1:{bell_port}"),
                "--http",
                "127.0.0.1:0",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn isolated rex-wave daemon");

        let stderr = child.stderr.take().expect("daemon stderr");
        let (addr_tx, addr_rx) = oneshot::channel::<SocketAddr>();
        std::thread::spawn(move || {
            let mut tx = Some(addr_tx);
            let reader = StdBufReader::new(stderr);
            for line in reader.lines().map_while(Result::ok) {
                if let Some(raw) = line.split("http listening on ").nth(1)
                    && let Ok(parsed) = raw.trim().parse::<SocketAddr>()
                    && let Some(sender) = tx.take()
                {
                    let _ = sender.send(parsed);
                }
            }
        });

        let http_addr = tokio::time::timeout(Duration::from_secs(6), addr_rx)
            .await
            .expect("daemon reported bound ephemeral HTTP port")
            .expect("daemon exited before reporting HTTP port");

        tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                if sock.exists() && TcpStream::connect(http_addr).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("daemon UDS and ephemeral HTTP became ready");

        (sock, http_addr, ChildGuard(child))
    }

    async fn start(label: &str) -> Self {
        let dir = fresh_dir(label);
        let sandbox = dir.0.join("mail-sandbox");
        let cfg_dir = dir.0.join("config");
        std::fs::create_dir_all(&sandbox).unwrap();
        std::fs::create_dir_all(cfg_dir.join("rex-wave")).unwrap();
        std::fs::write(sandbox.join(".rex-wave-sandbox"), "e2e\n").unwrap();
        std::fs::write(cfg_dir.join("rex-wave/protected-roots"), "").unwrap();

        let t3_token = "t3_e2e_secret_tok".to_owned();
        let (t3_addr, t3_state, t3_task) = spawn_mock_t3(&t3_token).await;
        let (sock, http_addr, _child) =
            Self::spawn_daemon_process(&dir.0, t3_addr, &t3_token).await;

        Self {
            dir,
            sock,
            http_addr,
            t3_addr,
            t3_token,
            t3_state,
            t3_task,
            _child,
        }
    }

    async fn restart(&mut self) {
        let _ = self._child.0.kill();
        let _ = self._child.0.wait();
        let _ = std::fs::remove_file(&self.sock);
        let (sock, http_addr, child) =
            Self::spawn_daemon_process(&self.dir.0, self.t3_addr, &self.t3_token).await;
        self.sock = sock;
        self.http_addr = http_addr;
        self._child = child;
    }

    async fn cli(&self, args: &[&str], envs: &[(&str, &str)], remove_envs: &[&str]) -> Output {
        let sock = self.sock.to_str().unwrap().to_owned();
        let owned_args: Vec<String> = args.iter().map(|s| (*s).to_owned()).collect();
        let owned_envs: Vec<(String, String)> = envs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        let owned_rem: Vec<String> = remove_envs.iter().map(|s| (*s).to_owned()).collect();
        tokio::task::spawn_blocking(move || {
            let mut cmd = Command::new(bin_path());
            cmd.env_remove("REX_WAVE_TOKEN")
                .env_remove("T3CODE_THREAD_ID");
            for k in &owned_rem {
                cmd.env_remove(k);
            }
            for (k, v) in &owned_envs {
                cmd.env(k, v);
            }
            cmd.arg("--socket").arg(&sock).args(&owned_args);
            cmd.output().expect("execute rex-wave cli")
        })
        .await
        .unwrap()
    }

    async fn cli_json(&self, args: &[&str]) -> Value {
        let mut full = vec!["--json"];
        full.extend_from_slice(args);
        let out = self
            .cli(&full, &[], &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"])
            .await;
        assert!(
            out.status.success(),
            "cli {:?} failed ({:?}): {}",
            args,
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
            panic!(
                "invalid json from {:?}: {e}: {}",
                args,
                String::from_utf8_lossy(&out.stdout)
            )
        })
    }
}

struct HttpReply {
    status: u16,
    body: Vec<u8>,
}

impl HttpReply {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "invalid http json (status={}): {e}: {}",
                self.status,
                String::from_utf8_lossy(&self.body)
            )
        })
    }
}

async fn http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    extra_headers: &[(&str, &str)],
    body: Option<&str>,
) -> HttpReply {
    let mut raw = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\n");
    for (k, v) in extra_headers {
        raw.push_str(&format!("{k}: {v}\r\n"));
    }
    if let Some(b) = body {
        raw.push_str(&format!("Content-Length: {}\r\n\r\n{b}", b.len()));
    } else {
        raw.push_str("\r\n");
    }

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(raw.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let sep = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("HTTP response header boundary");
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
    HttpReply {
        status,
        body: buf[sep + 4..].to_vec(),
    }
}

struct SseEvent {
    id: Option<String>,
    event: Option<String>,
    data: Option<String>,
}

async fn open_sse(
    addr: SocketAddr,
    path: &str,
    extra_headers: &[(&str, &str)],
) -> BufReader<TcpStream> {
    let mut raw = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n");
    for (k, v) in extra_headers {
        raw.push_str(&format!("{k}: {v}\r\n"));
    }
    raw.push_str("\r\n");

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(raw.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();

    let mut reader = BufReader::new(stream);
    let mut status_line = String::new();
    reader.read_line(&mut status_line).await.unwrap();
    assert!(
        status_line.contains("200 OK"),
        "expected 200 OK for SSE, got: {status_line}"
    );
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        if line == "\r\n" || line == "\n" || line.is_empty() {
            break;
        }
    }
    reader
}

async fn next_sse_event(reader: &mut BufReader<TcpStream>) -> SseEvent {
    let mut id = None;
    let mut event = None;
    let mut data = None;
    loop {
        let mut line = String::new();
        let n = tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut line))
            .await
            .expect("SSE event arrived within 5s")
            .unwrap();
        assert!(n > 0, "SSE stream closed unexpectedly");
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            if id.is_some() || event.is_some() || data.is_some() {
                return SseEvent { id, event, data };
            }
            continue;
        }
        if trimmed.starts_with(':') {
            continue;
        }
        if let Some(v) = trimmed.strip_prefix("id:") {
            id = Some(v.trim().to_owned());
        } else if let Some(v) = trimmed.strip_prefix("event:") {
            event = Some(v.trim().to_owned());
        } else if let Some(v) = trimmed.strip_prefix("data:") {
            data = Some(v.trim().to_owned());
        }
    }
}

async fn next_committed_sse_event(reader: &mut BufReader<TcpStream>) -> (String, Value) {
    loop {
        let ev = next_sse_event(reader).await;
        assert_eq!(ev.event.as_deref(), Some("frame"));
        let payload: Value = serde_json::from_str(ev.data.as_deref().unwrap()).unwrap();
        if payload["frame"] == "committed" && payload["event"]["event"]["body"].is_string() {
            let id = ev.id.expect("committed SSE event carries cursor id");
            return (id, payload);
        }
    }
}

#[tokio::test]
async fn e2e_phase3_tasks_org_sync_http_sse_claude_presence_and_shadow_mail() {
    let daemon = IsolatedDaemon::start("core").await;

    let health = http_request(daemon.http_addr, "GET", "/v1/health", &[], None).await;
    assert_eq!(health.status, 200);
    assert_eq!(health.json()["principal"]["kind"], "operator");

    let start_reply = daemon
        .cli_json(&[
            "wave",
            "start",
            "e2e-w1",
            "--project",
            "proj_e2e",
            "--goal",
            "Phase 3 end-to-end integration",
            "--worker",
            "coder=antigravity/gemini-4-neon",
            "--workspace",
            "project-default",
        ])
        .await;
    assert_eq!(start_reply["reply"], "wave_start");

    let worker_tid = "t3:e2e-w1:coder:a1";
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            if !daemon.t3_state.lock().await.launched.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("CompositeHost routed antigravity worker to mock T3 server");

    let tok_file = rex_wave_t3::worker_token_path(&daemon.dir.0, worker_tid).unwrap();
    let tok_meta = std::fs::symlink_metadata(&tok_file).expect("worker token file created");
    assert_eq!(tok_meta.permissions().mode() & 0o777, 0o600);

    let posted_task = daemon
        .cli_json(&[
            "post",
            "#general",
            "Implement SSE resume path",
            "--task",
            "Ship SSE resume",
            "--priority",
            "high",
        ])
        .await;
    assert_eq!(posted_task["reply"], "room_post");
    let task_seq = posted_task["value"]["seq"].as_u64().unwrap();
    let task_org_id = posted_task["value"]["org_id"].as_str().unwrap().to_owned();
    assert!(
        task_org_id.starts_with("rw-"),
        "expected rw-* OrgId, got {task_org_id}"
    );

    let claim_out = daemon
        .cli(
            &[
                "--json",
                "tasks",
                "claim",
                "#general",
                &task_seq.to_string(),
                "--ttl",
                "10m",
            ],
            &[("T3CODE_THREAD_ID", worker_tid)],
            &["REX_WAVE_TOKEN"],
        )
        .await;
    assert!(
        claim_out.status.success(),
        "worker tasks claim failed: {}",
        String::from_utf8_lossy(&claim_out.stderr)
    );
    let claim_json: Value = serde_json::from_slice(&claim_out.stdout).unwrap();
    assert_eq!(claim_json["value"]["epoch"], 1);
    assert_eq!(claim_json["value"]["org_id"], task_org_id);

    let first_reply = daemon
        .cli_json(&[
            "reply",
            "#general",
            &task_seq.to_string(),
            "first reply landed",
        ])
        .await;
    let first_reply_seq = first_reply["value"]["seq"].as_u64().unwrap();
    assert!(first_reply_seq > task_seq);

    let stale_reply = daemon
        .cli(
            &[
                "reply",
                "#general",
                &task_seq.to_string(),
                "stale concurrent reply",
                "--after",
                &task_seq.to_string(),
            ],
            &[],
            &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
        )
        .await;
    assert_eq!(
        stale_reply.status.code(),
        Some(3),
        "stale --after reply must return exit code 3"
    );
    let stale_err = String::from_utf8_lossy(&stale_reply.stderr);
    assert!(
        stale_err.contains("newer_message") && stale_err.contains("first reply landed"),
        "stale conflict stderr must surface NewerMessage: {stale_err}"
    );

    let wait_out = daemon
        .cli(
            &[
                "--json",
                "wait",
                "#general",
                "--since",
                &task_seq.to_string(),
                "--re",
                &task_seq.to_string(),
                "--timeout",
                "2000",
            ],
            &[("T3CODE_THREAD_ID", worker_tid)],
            &["REX_WAVE_TOKEN"],
        )
        .await;
    assert!(wait_out.status.success());
    let wait_json: Value = serde_json::from_slice(&wait_out.stdout).unwrap();
    assert_eq!(wait_json["value"]["timed_out"], false);
    assert!(
        wait_json["value"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["event"]["event"]["body"].as_str() == Some("first reply landed")),
        "wait --re must wake immediately on pre-existing thread reply"
    );

    let done_out = daemon
        .cli(
            &[
                "--json",
                "tasks",
                "transition",
                "#general",
                &task_seq.to_string(),
                "done",
                "--epoch",
                "1",
                "--note",
                "Atomic completion note",
            ],
            &[("T3CODE_THREAD_ID", worker_tid)],
            &["REX_WAVE_TOKEN"],
        )
        .await;
    assert!(
        done_out.status.success(),
        "tasks transition done failed: {}",
        String::from_utf8_lossy(&done_out.stderr)
    );
    let done_json: Value = serde_json::from_slice(&done_out.stdout).unwrap();
    assert_eq!(done_json["value"]["status"], "done");

    let show_json = daemon
        .cli_json(&["tasks", "show", "#general", &task_seq.to_string()])
        .await;
    assert_eq!(show_json["task"]["status"], "done");
    assert!(
        show_json["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["event"]["event"]["body"].as_str() == Some("Atomic completion note")),
        "tasks transition --note must append atomically into the task thread"
    );

    let org_task = daemon
        .cli_json(&[
            "post",
            "#org-e2e",
            "Sync task body",
            "--task",
            "Audit org sync",
            "--priority",
            "high",
        ])
        .await;
    let org_seq = org_task["value"]["seq"].as_u64().unwrap();
    let org_id = org_task["value"]["org_id"].as_str().unwrap().to_owned();
    assert!(org_id.starts_with("rw-"));

    let org_file = daemon.dir.0.join("tasks.org");
    let org_path_str = org_file.to_str().unwrap();
    let export_json = daemon
        .cli_json(&["tasks", "org-sync", "#org-e2e", org_path_str, "--once"])
        .await;
    assert_eq!(export_json["outcome"], "written");
    assert_eq!(export_json["tasks_synced"], 1);

    let exported_text = std::fs::read_to_string(&org_file).unwrap();
    assert!(exported_text.contains(&format!(":ID: {org_id}")));
    assert!(exported_text.contains(&format!(":REX_SEQ: {org_seq}")));
    assert!(exported_text.contains(":REX_REV: 1"));

    let lock_path = daemon.dir.0.join(".#tasks.org");
    std::os::unix::fs::symlink("golem@host.99999", &lock_path).unwrap();
    let deferred_json = daemon
        .cli_json(&["tasks", "org-sync", "#org-e2e", org_path_str, "--once"])
        .await;
    assert_eq!(deferred_json["outcome"], "deferred_lock");
    assert_eq!(
        std::fs::read_to_string(&org_file).unwrap(),
        exported_text,
        "Clause B-4 lock symlink must defer all writes to tasks.org"
    );
    std::fs::remove_file(&lock_path).unwrap();

    let emacs_edited = exported_text.replace(
        "* TODO [#A] Audit org sync",
        "* WAIT [#A] Audit org sync (Emacs)",
    );
    assert_ne!(emacs_edited, exported_text);
    std::fs::write(&org_file, &emacs_edited).unwrap();

    let synced_json = daemon
        .cli_json(&["tasks", "org-sync", "#org-e2e", org_path_str, "--once"])
        .await;
    assert_eq!(synced_json["outcome"], "synced");
    assert_eq!(synced_json["wrote_back"], true);

    let room_tasks_after = daemon.cli_json(&["tasks", "list", "#org-e2e"]).await;
    let t_after = &room_tasks_after["value"]["tasks"][0];
    assert_eq!(t_after["title"], "Audit org sync (Emacs)");
    assert_eq!(t_after["status"], "review");
    let current_rev = t_after["rev"].as_u64().unwrap();
    assert!(current_rev > 1);

    let synced_on_disk = std::fs::read_to_string(&org_file).unwrap();
    assert!(synced_on_disk.contains(&format!(":REX_REV: {current_rev}")));

    let room_bump = daemon
        .cli_json(&[
            "tasks",
            "update",
            "#org-e2e",
            &org_seq.to_string(),
            "--title",
            "Room moved ahead",
        ])
        .await;
    let bumped_rev = room_bump["value"]["rev"].as_u64().unwrap();
    assert_eq!(bumped_rev, current_rev + 1);

    let stale_org = synced_on_disk.replace("Audit org sync (Emacs)", "Stale Emacs edit");
    std::fs::write(&org_file, &stale_org).unwrap();

    let conflict_out = daemon
        .cli(
            &[
                "--json",
                "tasks",
                "org-sync",
                "#org-e2e",
                org_path_str,
                "--once",
            ],
            &[],
            &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
        )
        .await;
    assert_eq!(
        conflict_out.status.code(),
        Some(3),
        "stale :REX_REV: must refuse loudly with exit code 3"
    );
    let conflict_json: Value = serde_json::from_slice(&conflict_out.stderr).unwrap();
    assert_eq!(conflict_json["outcome"], "conflict");
    assert_eq!(
        conflict_json["conflicts"][0]["kind"]["kind"],
        "revision_conflict"
    );
    assert_eq!(
        conflict_json["conflicts"][0]["kind"]["expected"],
        current_rev
    );
    assert_eq!(conflict_json["conflicts"][0]["kind"]["actual"], bumped_rev);
    assert_eq!(
        std::fs::read_to_string(&org_file).unwrap(),
        stale_org,
        "stale org-sync conflict must not overwrite local .org edits"
    );

    let sidecar_path = rex_wave_board::conflict_sidecar_path(&org_file);
    let mut watch_child = ChildGuard(
        Command::new(bin_path())
            .env_remove("REX_WAVE_TOKEN")
            .env_remove("T3CODE_THREAD_ID")
            .args([
                "--socket",
                daemon.sock.to_str().unwrap(),
                "--json",
                "tasks",
                "org-sync",
                "#org-e2e",
                org_path_str,
                "--watch",
                "--interval-ms",
                "60",
                "--conflict-sidecar",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn tasks org-sync --watch"),
    );
    let watch_stdout = watch_child.0.stdout.take().expect("watch stdout");
    let watch_stderr = watch_child.0.stderr.take().expect("watch stderr");
    let (stdout_tx, mut stdout_rx) = mpsc::unbounded_channel::<String>();
    let (stderr_tx, mut stderr_rx) = mpsc::unbounded_channel::<String>();
    std::thread::spawn(move || {
        for line in StdBufReader::new(watch_stdout)
            .lines()
            .map_while(Result::ok)
        {
            let _ = stdout_tx.send(line);
        }
    });
    std::thread::spawn(move || {
        for line in StdBufReader::new(watch_stderr)
            .lines()
            .map_while(Result::ok)
        {
            let _ = stderr_tx.send(line);
        }
    });

    let first_conflict_line = tokio::time::timeout(Duration::from_secs(4), stderr_rx.recv())
        .await
        .expect("watch emitted initial conflict within 4s")
        .expect("conflict line on stderr");
    let first_conflict_val: Value = serde_json::from_str(&first_conflict_line).unwrap();
    assert_eq!(first_conflict_val["outcome"], "conflict");
    assert!(sidecar_path.exists(), "watch must write conflict sidecar");
    let sidecar_mtime_1 = std::fs::metadata(&sidecar_path)
        .unwrap()
        .modified()
        .unwrap();

    tokio::time::sleep(Duration::from_millis(320)).await;
    assert!(
        stderr_rx.try_recv().is_err(),
        "watch must not spam duplicate conflicts across ticks while .org bytes are unchanged"
    );
    let sidecar_mtime_2 = std::fs::metadata(&sidecar_path)
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(
        sidecar_mtime_1, sidecar_mtime_2,
        "watch must not rewrite conflict sidecar while .org bytes are unchanged"
    );

    let resolved_org = stale_org
        .replace(
            &format!(":REX_REV: {current_rev}"),
            &format!(":REX_REV: {bumped_rev}"),
        )
        .replace("Stale Emacs edit", "Resolved via watch mode");
    std::fs::write(&org_file, &resolved_org).unwrap();
    let synced_line = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let line = stdout_rx.recv().await.expect("watch stdout line");
            let val: Value = serde_json::from_str(&line).unwrap();
            if val["outcome"] == "synced" {
                break val;
            }
        }
    })
    .await
    .expect("watch synced once .org file was edited");
    assert_eq!(synced_line["updated"].as_array().unwrap().len(), 1);
    drop(watch_child);

    let after_watch = daemon
        .cli_json(&["tasks", "show", "#org-e2e", &org_seq.to_string()])
        .await;
    assert_eq!(after_watch["task"]["title"], "Resolved via watch mode");

    let alice_tok = daemon.cli_json(&["token", "--seat", "alice"]).await["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let charlie_tok = daemon.cli_json(&["token", "--seat", "charlie"]).await["token"]
        .as_str()
        .unwrap()
        .to_owned();

    let dm_post = http_request(
        daemon.http_addr,
        "POST",
        "/v1/rooms/@bob/messages",
        &[
            ("Authorization", &format!("Bearer {alice_tok}")),
            ("Content-Type", "application/json"),
        ],
        Some(r#"{"body":"secret between alice and bob"}"#),
    )
    .await;
    assert_eq!(dm_post.status, 201);

    let charlie_dm_read = http_request(
        daemon.http_addr,
        "GET",
        "/v1/rooms/dm:@alice,@bob/messages",
        &[("Authorization", &format!("Bearer {charlie_tok}"))],
        None,
    )
    .await;
    assert_eq!(
        charlie_dm_read.status, 404,
        "unauthorized DM read must be masked as 404 Not Found"
    );

    let mut sse = open_sse(daemon.http_addr, "/v1/events?room=%23sse-live", &[]).await;
    let caught = next_sse_event(&mut sse).await;
    assert_eq!(caught.event.as_deref(), Some("frame"));
    let caught_payload: Value = serde_json::from_str(caught.data.as_deref().unwrap()).unwrap();
    assert_eq!(caught_payload["frame"], "caught_up");

    daemon.cli_json(&["post", "#sse-live", "sse-msg-1"]).await;
    let (cursor_1, ev1) = next_committed_sse_event(&mut sse).await;
    assert_eq!(ev1["event"]["event"]["body"], "sse-msg-1");
    drop(sse);

    daemon.cli_json(&["post", "#sse-live", "sse-msg-2"]).await;
    daemon.cli_json(&["post", "#sse-live", "sse-msg-3"]).await;

    let mut sse_resumed = open_sse(
        daemon.http_addr,
        "/v1/events?room=%23sse-live",
        &[("Last-Event-ID", &cursor_1)],
    )
    .await;
    let (cursor_2, ev2) = next_committed_sse_event(&mut sse_resumed).await;
    let (cursor_3, ev3) = next_committed_sse_event(&mut sse_resumed).await;
    assert_eq!(ev2["event"]["event"]["body"], "sse-msg-2");
    assert_eq!(ev3["event"]["event"]["body"], "sse-msg-3");
    assert_ne!(cursor_2, cursor_1);
    assert_ne!(cursor_3, cursor_2);
    drop(sse_resumed);

    let fake_home = daemon.dir.0.join("fake-home");
    let sessions_dir = fake_home.join(".claude/sessions");
    std::fs::create_dir_all(&sessions_dir).unwrap();
    let my_pid = std::process::id();
    let my_start = current_process_start().expect("current process UTC lstart");
    let bg_session = json!({
        "pid": my_pid,
        "procStart": my_start,
        "startedAt": 1_795_260_000_000u64,
        "sessionId": "sess-bg-1",
        "messagingSocketPath": "/tmp/cc-socks/bg.sock",
        "kind": "bg",
        "name": "Tally-5.2",
        "nameSource": "user"
    });
    std::fs::write(
        sessions_dir.join(format!("{my_pid}.json")),
        serde_json::to_vec(&bg_session).unwrap(),
    )
    .unwrap();

    daemon
        .cli_json(&["presence", "claim", "tally", "--alias", "tally-desk"])
        .await;

    let bind_bg = daemon
        .cli(
            &[
                "--json",
                "presence",
                "bind",
                "--seat",
                "tally",
                "--from-claude-session",
                "--pid",
                &my_pid.to_string(),
            ],
            &[("HOME", fake_home.to_str().unwrap())],
            &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID", "CLAUDE_PID"],
        )
        .await;
    assert!(
        bind_bg.status.success(),
        "bg Claude session bind failed: {}",
        String::from_utf8_lossy(&bind_bg.stderr)
    );
    let bind_bg_json: Value = serde_json::from_slice(&bind_bg.stdout).unwrap();
    assert_eq!(bind_bg_json["reply"], "presence_bind");
    assert_eq!(
        bind_bg_json["value"]["endpoint"]["transport"],
        "claude_session"
    );
    assert_eq!(bind_bg_json["value"]["endpoint"]["pid"], my_pid);
    assert_eq!(bind_bg_json["value"]["endpoint"]["session_id"], "sess-bg-1");
    assert_eq!(bind_bg_json["value"]["endpoint"]["kind"], "bg");

    let fork_path = daemon.dir.0.join("fork-session.json");
    std::fs::write(
        &fork_path,
        serde_json::to_vec(&json!({
            "pid": my_pid,
            "procStart": my_start,
            "sessionId": "sess-fork-1",
            "messagingSocketPath": "/tmp/cc-socks/fork.sock",
            "kind": "interactive",
            "name": "Tally-5.2 ⑂",
            "nameSource": "user"
        }))
        .unwrap(),
    )
    .unwrap();
    let fork_denied = daemon
        .cli(
            &[
                "presence",
                "bind",
                "--seat",
                "tally",
                "--from-claude-session",
                fork_path.to_str().unwrap(),
            ],
            &[],
            &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
        )
        .await;
    assert!(
        !fork_denied.status.success(),
        "forked Claude session must fail without --allow-fork"
    );
    let fork_err = String::from_utf8_lossy(&fork_denied.stderr);
    assert!(
        fork_err.contains("fork") || fork_err.contains("⑂") || fork_err.contains("allow-fork"),
        "expected fork refusal message, got: {fork_err}"
    );

    let synth_interchange = daemon.dir.0.join("synth-interchange");
    std::fs::create_dir_all(synth_interchange.join("mail")).unwrap();
    std::fs::create_dir_all(synth_interchange.join("state")).unwrap();
    let letter1_bytes = b"---\nfrom: crest\nto: tally\nsubject: Live shadow check\n---\nping\n";
    std::fs::write(synth_interchange.join("mail/01-to-tally.md"), letter1_bytes).unwrap();
    std::fs::write(
        synth_interchange.join("mail/02-to-genga.md"),
        "---\nfrom: crest\nto: genga\nsubject: Offline shadow check\n---\nping\n",
    )
    .unwrap();
    std::fs::write(synth_interchange.join("state/acks.jsonl"), "").unwrap();

    let tree_before = snapshot_tree(&synth_interchange);

    let outside_dir = daemon.dir.0.join("outside-link-test");
    std::fs::create_dir_all(&outside_dir).unwrap();
    std::os::unix::fs::symlink(&synth_interchange, outside_dir.join("link")).unwrap();
    let escape_dotdot = outside_dir.join("link/../synth-interchange/escaped.jsonl");
    let escape_direct = outside_dir.join("link/escaped.jsonl");
    for escaped in [&escape_dotdot, &escape_direct] {
        let refused = daemon
            .cli(
                &[
                    "mail",
                    "shadow",
                    "--interchange",
                    synth_interchange.to_str().unwrap(),
                    "--out",
                    escaped.to_str().unwrap(),
                    "--once",
                ],
                &[],
                &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
            )
            .await;
        assert!(
            !refused.status.success(),
            "mail shadow must refuse --out escaping into interchange via {}",
            escaped.display()
        );
    }
    assert!(
        !synth_interchange.join("escaped.jsonl").exists(),
        "refused shadow escape must never write into interchange"
    );

    let unreachable_sock = daemon.dir.0.join("no-such-daemon.sock");
    let unreachable_out = tokio::task::spawn_blocking({
        let sock = unreachable_sock.clone();
        let interchange = synth_interchange.clone();
        let out_file = daemon.dir.0.join("unreachable-shadow.jsonl");
        move || {
            Command::new(bin_path())
                .env_remove("REX_WAVE_TOKEN")
                .env_remove("T3CODE_THREAD_ID")
                .args([
                    "--socket",
                    sock.to_str().unwrap(),
                    "mail",
                    "shadow",
                    "--interchange",
                    interchange.to_str().unwrap(),
                    "--out",
                    out_file.to_str().unwrap(),
                    "--once",
                ])
                .output()
                .expect("run unreachable mail shadow")
        }
    })
    .await
    .unwrap();
    assert!(
        !unreachable_out.status.success(),
        "mail shadow without --offline must fail when daemon socket is unreachable"
    );

    let shadow_out_path = daemon.dir.0.join("shadow.jsonl");
    let shadow_summary = daemon
        .cli_json(&[
            "mail",
            "shadow",
            "--interchange",
            synth_interchange.to_str().unwrap(),
            "--out",
            shadow_out_path.to_str().unwrap(),
            "--once",
        ])
        .await;
    assert_eq!(shadow_summary["live_presence"], true);
    assert_eq!(shadow_summary["letters_scanned"], 2);
    assert_eq!(
        snapshot_tree(&synth_interchange),
        tree_before,
        "mail shadow must leave the entire interchange directory tree untouched"
    );

    let shadow_raw = std::fs::read_to_string(&shadow_out_path).unwrap();
    let sha1 = shadow_raw
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|v| v["decision"] == "ring" && v["seat"] == "tally")
        .and_then(|v| v["referent"]["sha256"].as_str().map(str::to_owned))
        .expect("shadow.jsonl must contain ring referent sha256 for tally");
    let alias_acks_path = daemon.dir.0.join("alias-acks.jsonl");
    std::fs::write(
        &alias_acks_path,
        format!("{{\"acked_by\":\"Tally-Desk\",\"letter\":\"mail/01-to-tally.md\",\"sha256\":\"{sha1}\"}}\n"),
    )
    .unwrap();

    let all_fixtures_path = daemon.dir.0.join("gate1-full.jsonl");
    let all_fixtures_text: String = GATE1_CASE_IDS
        .iter()
        .map(|id| format!(r#"{{"id":"{id}"}}"#))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&all_fixtures_path, &all_fixtures_text).unwrap();

    let score_summary = daemon
        .cli_json(&[
            "mail",
            "score",
            "--shadow",
            shadow_out_path.to_str().unwrap(),
            "--acks",
            alias_acks_path.to_str().unwrap(),
            "--fixtures",
            all_fixtures_path.to_str().unwrap(),
        ])
        .await;
    assert_eq!(score_summary["shadow_score"]["rings_emitted"], 1);
    assert_eq!(
        score_summary["shadow_score"]["ring_referents_confirmed_acked"], 1,
        "mail score must honor aliases embedded from live presence when matching acks"
    );
    assert_eq!(score_summary["shadow_score"]["unrung_alerts"], 1);
    assert_eq!(score_summary["shadow_score"]["unrung_referents_caught"], 1);
    assert_eq!(score_summary["fixtures"]["fixture_ids_registered"], true);
    assert_eq!(score_summary["fixtures"]["covered_cases"], 10);
    assert_eq!(score_summary["fixtures"]["missing_case_ids"], json!([]));
    assert_eq!(score_summary["fixtures"]["unknown_case_ids"], json!([]));
    assert_eq!(
        score_summary["fixtures"]["case_test_map"]["dry-run-marks-seen"],
        "gate1_case_04_dry_run_marks_seen"
    );

    let partial_fixtures_path = daemon.dir.0.join("gate1-partial.jsonl");
    std::fs::write(&partial_fixtures_path, r#"{"id":"genga-stale-six-days"}"#).unwrap();
    let partial_score = daemon
        .cli(
            &[
                "--json",
                "mail",
                "score",
                "--shadow",
                shadow_out_path.to_str().unwrap(),
                "--acks",
                alias_acks_path.to_str().unwrap(),
                "--fixtures",
                partial_fixtures_path.to_str().unwrap(),
            ],
            &[],
            &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
        )
        .await;
    assert_eq!(
        partial_score.status.code(),
        Some(1),
        "incomplete Gate 1 fixture file must exit non-zero"
    );
    let partial_json: Value = serde_json::from_slice(&partial_score.stdout).unwrap();
    assert_eq!(partial_json["fixtures"]["fixture_ids_registered"], false);
    assert_eq!(
        partial_json["fixtures"]["missing_case_ids"]
            .as_array()
            .unwrap()
            .len(),
        9
    );
}

#[tokio::test]
async fn e2e_phase3_presence_hold_raii_unbind_and_15s_mail_unrung_lifecycle() {
    let daemon = IsolatedDaemon::start("presence-grace").await;

    daemon.cli_json(&["presence", "claim", "sam"]).await;
    let sam_tok = daemon.cli_json(&["token", "--seat", "sam"]).await["token"]
        .as_str()
        .unwrap()
        .to_owned();

    let mut hold_child = Command::new(bin_path())
        .env_remove("REX_WAVE_TOKEN")
        .env_remove("T3CODE_THREAD_ID")
        .args([
            "--socket",
            daemon.sock.to_str().unwrap(),
            "--token",
            &sam_tok,
            "--json",
            "presence",
            "hold",
            "--seat",
            "sam",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn presence hold");

    let hold_stdout = hold_child.stdout.take().expect("hold stdout");
    let (bound_tx, bound_rx) = oneshot::channel::<Value>();
    std::thread::spawn(move || {
        let mut reader = StdBufReader::new(hold_stdout);
        let mut acc = String::new();
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap_or(0) > 0 {
            acc.push_str(&line);
            line.clear();
            if let Ok(val) = serde_json::from_str::<Value>(&acc) {
                let _ = bound_tx.send(val);
                break;
            }
        }
    });

    let bound_json = tokio::time::timeout(Duration::from_secs(5), bound_rx)
        .await
        .expect("presence hold bound within 5s")
        .expect("presence hold emitted valid bound JSON");
    assert_eq!(bound_json["reply"], "presence_bind");
    assert_eq!(bound_json["value"]["endpoint"]["transport"], "uds_session");
    let conn_id = bound_json["value"]["endpoint"]["conn_id"].as_u64().unwrap();
    assert!(
        conn_id > 0,
        "daemon must stamp positive server conn_id on UdsSession binding"
    );

    let live_before = daemon.cli_json(&["presence", "list", "--live-only"]).await;
    assert_eq!(live_before["value"]["seats"].as_array().unwrap().len(), 1);
    assert_eq!(live_before["value"]["seats"][0]["record"]["seat"], "sam");

    let _ = hold_child.kill();
    let _ = hold_child.wait();
    let unbind_instant = Instant::now();

    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let live = daemon.cli_json(&["presence", "list", "--live-only"]).await;
            if live["value"]["seats"].as_array().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("killing presence hold triggered RAII on_disconnect PresenceUnbind");

    let presence_log = daemon
        .cli_json(&[
            "call",
            "read",
            r#"{"stream":{"stream":"presence"},"after":0,"limit":100}"#,
        ])
        .await;
    assert!(
        presence_log["value"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| {
                e["by"] == json!({"kind": "system", "part": "presence_monitor"})
                    && e["event"]["event"]["type"] == "unbound"
                    && e["event"]["event"]["seat"] == "sam"
            }),
        "RAII disconnect must dispatch PresenceUnbind as System(PresenceMonitor)"
    );

    let msg = daemon
        .cli_json(&["post", "#general", "Banked letter for offline sam"])
        .await;
    let msg_seq = msg["value"]["seq"].as_u64().unwrap();
    let promoted = daemon
        .cli_json(&[
            "promote",
            "#general",
            &format!("{msg_seq}..{msg_seq}"),
            "--to",
            "sam",
            "--subject",
            "Offline check",
            "--topic",
            "e2e-grace",
        ])
        .await;
    let letter_path = promoted["value"]["path"].as_str().unwrap().to_owned();
    let letter_sha = promoted["value"]["sha256"].as_str().unwrap().to_owned();

    tokio::time::sleep(Duration::from_millis(500)).await;
    let inbox_during_grace = http_request(daemon.http_addr, "GET", "/v1/inbox", &[], None).await;
    assert_eq!(inbox_during_grace.status, 200);
    assert!(
        !inbox_during_grace.json()["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["draft"]["dedupe"] == "mail-unrung:sam"),
        "MailUnrung must stay suppressed during the 15 s reconnect grace window"
    );

    let raised_card = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let resp = http_request(daemon.http_addr, "GET", "/v1/inbox", &[], None).await;
            assert_eq!(resp.status, 200);
            let json = resp.json();
            if let Some(card) = json["items"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["draft"]["dedupe"] == "mail-unrung:sam")
            {
                break card.clone();
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("HeraldHandle grace timer flushed MailUnrung after 15 s with zero extra events");
    let grace_elapsed = unbind_instant.elapsed();
    assert!(
        grace_elapsed >= Duration::from_millis(14_500),
        "MailUnrung must wait for the 15 s reconnect grace window, fired after {grace_elapsed:?}"
    );
    assert_eq!(raised_card["draft"]["kind"]["kind"], "mail_unrung");
    assert_eq!(raised_card["draft"]["kind"]["unread_count"], 1);
    assert_eq!(raised_card["state"], "unread");
    assert!(
        raised_card["draft"]["actions"]
            .as_array()
            .unwrap()
            .is_empty(),
        "MailUnrung card must be purely informational with zero decision buttons"
    );

    let rebind_out = daemon
        .cli(
            &[
                "--token",
                &sam_tok,
                "--json",
                "presence",
                "bind",
                "--seat",
                "sam",
                "--self-watcher",
                "codex",
                "--ttl",
                "5m",
            ],
            &[],
            &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
        )
        .await;
    assert!(
        rebind_out.status.success(),
        "sam rebind failed: {}",
        String::from_utf8_lossy(&rebind_out.stderr)
    );

    let ack_payload = json!({
        "path": letter_path,
        "sha256": letter_sha,
    })
    .to_string();
    let ack_out = daemon
        .cli(
            &[
                "--token",
                &sam_tok,
                "--json",
                "call",
                "mail_ack",
                &ack_payload,
            ],
            &[],
            &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
        )
        .await;
    assert!(
        ack_out.status.success(),
        "sam mail_ack failed: {}",
        String::from_utf8_lossy(&ack_out.stderr)
    );

    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let resp = http_request(daemon.http_addr, "GET", "/v1/inbox?all=true", &[], None).await;
            assert_eq!(resp.status, 200);
            let json = resp.json();
            if json["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|n| n["draft"]["dedupe"] == "mail-unrung:sam" && n["state"] == "done")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("rebinding + mail_ack auto-resolved MailUnrung card to Done");

    let _ = Path::new(&daemon.sock);
}

#[tokio::test]
async fn e2e_phase3_presence_hold_restart_rebinds_until_replaced() {
    let mut daemon = IsolatedDaemon::start("hold-restart").await;
    daemon.cli_json(&["presence", "claim", "sam"]).await;
    let sam_tok = daemon.cli_json(&["token", "--seat", "sam"]).await["token"]
        .as_str()
        .unwrap()
        .to_owned();

    let mut hold_child = ChildGuard(
        Command::new(bin_path())
            .env_remove("REX_WAVE_TOKEN")
            .env_remove("T3CODE_THREAD_ID")
            .args([
                "--socket",
                daemon.sock.to_str().unwrap(),
                "--token",
                &sam_tok,
                "--json",
                "presence",
                "hold",
                "--seat",
                "sam",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn presence hold"),
    );
    let hold_stdout = hold_child.0.stdout.take().expect("hold stdout");
    let (bind_tx, mut bind_rx) = mpsc::unbounded_channel::<Value>();
    std::thread::spawn(move || {
        let mut reader = StdBufReader::new(hold_stdout);
        let mut acc = String::new();
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap_or(0) > 0 {
            acc.push_str(&line);
            line.clear();
            if let Ok(val) = serde_json::from_str::<Value>(&acc) {
                let _ = bind_tx.send(val);
                acc.clear();
            }
        }
    });

    let initial = tokio::time::timeout(Duration::from_secs(5), bind_rx.recv())
        .await
        .expect("initial hold bind within 5s")
        .expect("initial hold bind json");
    let inc_id = initial["value"]["incarnation_id"]
        .as_str()
        .unwrap()
        .to_owned();

    daemon.restart().await;
    let rebound = tokio::time::timeout(Duration::from_secs(6), bind_rx.recv())
        .await
        .expect("hold rebound cleanly after daemon restart")
        .expect("rebound json");
    assert_eq!(rebound["value"]["incarnation_id"], inc_id);
    assert_eq!(rebound["value"]["endpoint"]["transport"], "uds_session");

    let takeover_out = daemon
        .cli(
            &[
                "--token",
                &sam_tok,
                "--json",
                "presence",
                "bind",
                "--seat",
                "sam",
                "--self-watcher",
                "codex",
                "--takeover",
            ],
            &[],
            &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
        )
        .await;
    assert!(
        takeover_out.status.success(),
        "self-watcher takeover failed: {}",
        String::from_utf8_lossy(&takeover_out.stderr)
    );

    daemon.restart().await;

    let hold_exit = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Some(status) = hold_child.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("replaced presence hold must exit after reconnect conflict");
    assert!(
        !hold_exit.success(),
        "replaced presence hold must exit non-zero rather than stealing the seat"
    );

    let live_after = daemon.cli_json(&["presence", "list", "--live-only"]).await;
    let bindings = live_after["value"]["seats"][0]["bindings"]
        .as_array()
        .unwrap();
    let live_bindings: Vec<&Value> = bindings.iter().filter(|b| b["status"] == "live").collect();
    assert_eq!(live_bindings.len(), 1);
    assert_eq!(
        live_bindings[0]["endpoint"]["transport"], "self_watcher",
        "restarted hold must not overwrite the newer self_watcher binding"
    );
}

#[tokio::test]
async fn e2e_pr20_seed_interchange_horizon_golem_guard_and_resident_manifest() {
    let daemon = IsolatedDaemon::start("pr20-soak").await;

    // 1. Build a synthetic interchange dir with roster.json, sibling .watchers/mailwatch-routes.json,
    //    and historical + post-horizon letters plus golem/cc letters.
    let estate = daemon.dir.0.join("estate");
    let interchange = estate.join("interchange");
    std::fs::create_dir_all(interchange.join("postmaster/state")).unwrap();
    std::fs::create_dir_all(estate.join(".watchers")).unwrap();
    std::fs::create_dir_all(interchange.join("mail")).unwrap();
    std::fs::create_dir_all(interchange.join("state")).unwrap();

    std::fs::write(
        interchange.join("postmaster/state/roster.json"),
        serde_json::to_vec(&json!({
            "schema": "house-roster/2",
            "seats": {
                "rd-1": { "name": "tally", "box": "/tmp/interchange/to-tally" },
                "rd-2": { "name": "golem", "box": "/tmp/interchange/to-golem" },
                "rd-3": { "name": "auburn-cepheus", "box": "/tmp/interchange/to-muse" }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(
        estate.join(".watchers/mailwatch-routes.json"),
        serde_json::to_vec(&json!({
            "tally": { "aliases": ["tally", "tally_votp", "Tally-VampOTP-CTO"] },
            "auburn-cepheus": { "aliases": ["muse", "auburn-cephus"] }
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(interchange.join("state/acks.jsonl"), "").unwrap();

    // 2. Dry-run seed-interchange works offline and skips `golem`.
    let dry_seed = daemon
        .cli_json(&[
            "presence",
            "seed-interchange",
            "--interchange",
            interchange.to_str().unwrap(),
            "--dry-run",
        ])
        .await;
    assert_eq!(dry_seed["dry_run"], true);
    assert!(
        dry_seed["skipped_targets"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "golem")
    );

    // 3. Live seed-interchange applies durable aliases BEFORE any seat binds.
    let live_seed = daemon
        .cli_json(&[
            "presence",
            "seed-interchange",
            "--interchange",
            interchange.to_str().unwrap(),
        ])
        .await;
    assert_eq!(live_seed["dry_run"], false);
    assert!(live_seed["applied_count"].as_u64().unwrap() >= 3);

    // 4. Q3 structural guard: binding or claiming `golem` as Active is denied by the daemon.
    let golem_bind = daemon
        .cli(
            &[
                "presence",
                "bind",
                "--seat",
                "golem",
                "--self-watcher",
                "desktop",
            ],
            &[],
            &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
        )
        .await;
    assert!(!golem_bind.status.success(), "golem bind must be denied");

    let golem_claim = daemon
        .cli(
            &["presence", "claim", "golem", "--status", "active"],
            &[],
            &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
        )
        .await;
    assert!(
        !golem_claim.status.success(),
        "golem active claim must be denied"
    );

    // 5. Bind `tally` with explicit `--mail-horizon` (no `--alias` needed; seeded aliases remain!).
    let horizon_ms: u64 = 1_795_000_000_000;
    let tally_bind = daemon
        .cli_json(&[
            "presence",
            "bind",
            "--seat",
            "tally",
            "--self-watcher",
            "claude-hook",
            "--mail-horizon",
            &horizon_ms.to_string(),
        ])
        .await;
    assert_eq!(tally_bind["value"]["seat"], "tally");

    let resolved_alias = daemon
        .cli_json(&["presence", "resolve", "tally_votp"])
        .await;
    assert_eq!(resolved_alias["value"]["record"]["seat"], "tally");
    assert_eq!(
        resolved_alias["value"]["record"]["mail_horizon_at"],
        horizon_ms
    );

    // 6. Write pre-horizon letter to `tally_votp`, post-horizon letter to `tally_votp`,
    //    plus letters to `golem` and `cc`.
    std::fs::write(
        interchange.join("mail/1792431783570-old-to-tally-votp.md"),
        "---\nfrom: genga\nto: tally_votp\nsubject: Old history\n---\nold\n",
    )
    .unwrap();
    std::fs::write(
        interchange.join("mail/1795284326392-new-to-tally-votp.md"),
        "---\nfrom: genga\nto: tally_votp\nsubject: Post horizon\n---\nnew\n",
    )
    .unwrap();
    std::fs::write(
        interchange.join("mail/1795284326400-to-golem.md"),
        "---\nfrom: tally\nto: golem\nsubject: Operator note\n---\noperator\n",
    )
    .unwrap();
    std::fs::write(
        interchange.join("mail/1795284326401-to-cc.md"),
        "---\nfrom: tally\nto: cc\nsubject: Group note\n---\ncc\n",
    )
    .unwrap();

    let shadow_out = daemon.dir.0.join("pr20-shadow.jsonl");
    daemon
        .cli_json(&[
            "mail",
            "shadow",
            "--interchange",
            interchange.to_str().unwrap(),
            "--out",
            shadow_out.to_str().unwrap(),
            "--once",
        ])
        .await;
    let score = daemon
        .cli_json(&[
            "mail",
            "score",
            "--shadow",
            shadow_out.to_str().unwrap(),
            "--acks",
            interchange.join("state/acks.jsonl").to_str().unwrap(),
        ])
        .await;
    assert_eq!(score["shadow_score"]["skipped_pre_horizon"], 1);
    assert_eq!(score["shadow_score"]["rings_emitted"], 1);
    assert_eq!(score["shadow_score"]["skipped_operator_surface"], 1);
    assert_eq!(score["shadow_score"]["skipped_non_seat_target"], 1);
    assert_eq!(score["shadow_score"]["unrung_alerts"], 0);

    // 7. Resident manifest (`seat-in-a-card.org`) claim -> export-manifest -> import-manifest on a fresh daemon!
    let claimed = daemon
        .cli_json(&[
            "presence",
            "claim",
            "fathom",
            "--role",
            "card-resident",
            "--work-class",
            "class_b",
            "--avatar",
            "avatars/fathom.png",
            "--home-dir",
            "/Users/golem/agent-folder/resident_memories/fathom",
            "--start-cmd",
            "claude --seat fathom",
            "--resume-cmd",
            "claude --resume fathom",
            "--rex-target",
            "block:fathom-card",
        ])
        .await;
    assert_eq!(claimed["value"]["work_class"], "class_b");
    assert_eq!(claimed["value"]["rex_target"], "block:fathom-card");

    let manifest_path = daemon.dir.0.join("residents.json");
    let exported = daemon
        .cli_json(&[
            "presence",
            "export-manifest",
            manifest_path.to_str().unwrap(),
        ])
        .await;
    assert_eq!(exported["schema"], "rex-wave-resident-manifest/1");
    assert!(exported["seat_count"].as_u64().unwrap() >= 4);

    let daemon2 = IsolatedDaemon::start("pr20-import").await;
    let imported = daemon2
        .cli_json(&[
            "presence",
            "import-manifest",
            manifest_path.to_str().unwrap(),
        ])
        .await;
    assert_eq!(imported["imported_count"], exported["seat_count"]);

    let fathom_res = daemon2.cli_json(&["presence", "resolve", "fathom"]).await;
    assert_eq!(fathom_res["value"]["record"]["work_class"], "class_b");
    assert_eq!(
        fathom_res["value"]["record"]["avatar"],
        "avatars/fathom.png"
    );
    assert_eq!(
        fathom_res["value"]["record"]["home_dir"],
        "/Users/golem/agent-folder/resident_memories/fathom"
    );
    assert_eq!(
        fathom_res["value"]["record"]["start_cmd"],
        "claude --seat fathom"
    );
    assert_eq!(
        fathom_res["value"]["record"]["resume_cmd"],
        "claude --resume fathom"
    );
    assert_eq!(
        fathom_res["value"]["record"]["rex_target"],
        "block:fathom-card"
    );
}

#[tokio::test]
async fn e2e_pr21_operator_only_manifest_exec_fields_and_tally_1seat_soak() {
    let daemon = IsolatedDaemon::start("pr21-guard").await;

    // Mint a Seat token for `tally` and a Worker token for `w1/coder`.
    let tally_tok = daemon.cli_json(&["token", "--seat", "tally"]).await["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let worker_tok = daemon.cli_json(&["token", "--worker", "w1/coder"]).await["token"]
        .as_str()
        .unwrap()
        .to_owned();

    // Build a synthetic interchange directory + two manifests:
    //  - a 1-seat manifest for `tally` with NO exec fields (tests upfront operator check)
    //  - a manifest for `tally` WITH `start_cmd` / `resume_cmd`
    let estate = daemon.dir.0.join("estate");
    let interchange = estate.join("interchange");
    std::fs::create_dir_all(interchange.join("postmaster/state")).unwrap();
    std::fs::create_dir_all(estate.join(".watchers")).unwrap();
    std::fs::create_dir_all(interchange.join("mail")).unwrap();
    std::fs::create_dir_all(interchange.join("state")).unwrap();
    std::fs::write(
        interchange.join("postmaster/state/roster.json"),
        serde_json::to_vec(&json!({
            "schema": "house-roster/2",
            "seats": {
                "rd-1": { "name": "tally", "box": "/tmp/interchange/to-tally" },
                "rd-2": { "name": "golem", "box": "/tmp/interchange/to-golem" },
                "rd-3": { "name": "nodal", "box": "/tmp/interchange/to-nodal" }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(
        estate.join(".watchers/mailwatch-routes.json"),
        serde_json::to_vec(&json!({
            "tally": { "aliases": ["tally", "tally_votp"] }
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(interchange.join("state/acks.jsonl"), "").unwrap();

    let self_manifest = daemon.dir.0.join("tally-only.json");
    std::fs::write(
        &self_manifest,
        serde_json::to_vec_pretty(&json!({
            "schema": "rex-wave-resident-manifest/1",
            "seats": [{
                "seat": "tally",
                "role": "estate-desk"
            }]
        }))
        .unwrap(),
    )
    .unwrap();

    let exec_manifest = daemon.dir.0.join("tally-exec.json");
    std::fs::write(
        &exec_manifest,
        serde_json::to_vec_pretty(&json!({
            "schema": "rex-wave-resident-manifest/1",
            "seats": [{
                "seat": "tally",
                "role": "estate-desk",
                "start_cmd": "claude --name Tally-5.2",
                "resume_cmd": "claude --resume 59e9053e"
            }]
        }))
        .unwrap(),
    )
    .unwrap();

    let assert_denied = |label: &str, out: &Output| {
        assert!(
            !out.status.success(),
            "{label} must exit non-zero, stdout={}, stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let err_json: Value = serde_json::from_slice(&out.stderr).unwrap_or_else(|e| {
            panic!(
                "{label} stderr was not ApiError JSON ({e}): {}",
                String::from_utf8_lossy(&out.stderr)
            )
        });
        assert_eq!(
            err_json["error"], "denied",
            "{label} expected ApiError::Denied, got {err_json}"
        );
    };

    // 1. Both Seat (`tally`) and Worker (`w1/coder`) tokens are DENIED on:
    //    - `presence import-manifest` (even a 1-seat manifest for `tally` with no exec fields!)
    //    - `presence seed-interchange`
    //    - `presence claim tally --start-cmd ...` / `--resume-cmd ...`
    for (who, tok) in [
        ("seat:tally", tally_tok.as_str()),
        ("worker:w1/coder", worker_tok.as_str()),
    ] {
        let out_import = daemon
            .cli(
                &[
                    "--json",
                    "--token",
                    tok,
                    "presence",
                    "import-manifest",
                    self_manifest.to_str().unwrap(),
                ],
                &[],
                &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
            )
            .await;
        assert_denied(&format!("{who} import-manifest (self-only)"), &out_import);

        let out_seed = daemon
            .cli(
                &[
                    "--json",
                    "--token",
                    tok,
                    "presence",
                    "seed-interchange",
                    "--interchange",
                    interchange.to_str().unwrap(),
                ],
                &[],
                &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
            )
            .await;
        assert_denied(&format!("{who} seed-interchange"), &out_seed);

        let out_start = daemon
            .cli(
                &[
                    "--json",
                    "--token",
                    tok,
                    "presence",
                    "claim",
                    "tally",
                    "--start-cmd",
                    "sh -c evil",
                ],
                &[],
                &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
            )
            .await;
        assert_denied(&format!("{who} claim --start-cmd"), &out_start);

        let out_resume = daemon
            .cli(
                &[
                    "--json",
                    "--token",
                    tok,
                    "presence",
                    "claim",
                    "tally",
                    "--resume-cmd",
                    "sh -c evil",
                ],
                &[],
                &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
            )
            .await;
        assert_denied(&format!("{who} claim --resume-cmd"), &out_resume);

        // HTTP transport boundary also denies `start_cmd` for non-operator tokens
        // (http.rs masks `ApiError::Denied` as 404 `not_found`).
        let auth_hdr = format!("Bearer {tok}");
        let http_res = http_request(
            daemon.http_addr,
            "POST",
            "/v1/call/presence_claim",
            &[
                ("Authorization", &auth_hdr),
                ("Content-Type", "application/json"),
            ],
            Some(r#"{"seat":"tally","start_cmd":"sh -c evil"}"#),
        )
        .await;
        assert_eq!(
            http_res.status,
            404,
            "{who} HTTP presence_claim with start_cmd must be rejected (masked Denied -> 404), body={}",
            String::from_utf8_lossy(&http_res.body)
        );
        assert_eq!(http_res.json()["error"], "not_found");
    }

    // Verify no denied call created `tally`.
    let list_before = daemon.cli_json(&["presence", "list"]).await;
    assert_eq!(
        list_before["value"]["seats"].as_array().unwrap().len(),
        0,
        "denied non-operator calls must leave presence table empty"
    );

    // 2. Operator succeeds on `import-manifest` with `start_cmd`/`resume_cmd`,
    //    and HTTP `/v1/call/presence_claim` succeeds as Operator (proving the 404
    //    above was masked `Denied`, not a bad route).
    let op_import = daemon
        .cli_json(&[
            "presence",
            "import-manifest",
            exec_manifest.to_str().unwrap(),
        ])
        .await;
    assert_eq!(op_import["imported_count"], 1);
    let op_http = http_request(
        daemon.http_addr,
        "POST",
        "/v1/call/presence_claim",
        &[("Content-Type", "application/json")],
        Some(r#"{"seat":"tally","start_cmd":"claude --name Tally-5.2"}"#),
    )
    .await;
    assert_eq!(
        op_http.status,
        200,
        "operator HTTP presence_claim with start_cmd must succeed: {}",
        String::from_utf8_lossy(&op_http.body)
    );
    let resolved = daemon.cli_json(&["presence", "resolve", "tally"]).await;
    assert_eq!(
        resolved["value"]["record"]["start_cmd"],
        "claude --name Tally-5.2"
    );
    assert_eq!(
        resolved["value"]["record"]["resume_cmd"],
        "claude --resume 59e9053e"
    );

    // 3. `tally`'s seat token cannot clear operator-set `start_cmd`/`resume_cmd` via `--replace`,
    //    but CAN still update its own non-exec metadata while preserving them.
    let replace_denied = daemon
        .cli(
            &[
                "--json",
                "--token",
                &tally_tok,
                "presence",
                "claim",
                "tally",
                "--replace",
                "tally",
            ],
            &[],
            &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
        )
        .await;
    assert_denied("seat:tally --replace clearing exec fields", &replace_denied);

    let role_update = daemon
        .cli(
            &[
                "--json",
                "--token",
                &tally_tok,
                "presence",
                "claim",
                "tally",
                "--role",
                "desk-keeper",
            ],
            &[],
            &["REX_WAVE_TOKEN", "T3CODE_THREAD_ID"],
        )
        .await;
    assert!(
        role_update.status.success(),
        "seat updating its own non-exec role must succeed: {}",
        String::from_utf8_lossy(&role_update.stderr)
    );
    let after_role = daemon.cli_json(&["presence", "resolve", "tally"]).await;
    assert_eq!(
        after_role["value"]["record"]["role"]["value"],
        "desk-keeper"
    );
    assert_eq!(
        after_role["value"]["record"]["start_cmd"],
        "claude --name Tally-5.2"
    );
    assert_eq!(
        after_role["value"]["record"]["resume_cmd"],
        "claude --resume 59e9053e"
    );

    // 4. Smoke-test Tally's exact 4-step 1-seat live-soak flow on a clean isolated daemon.
    let soak = IsolatedDaemon::start("pr21-tally-soak").await;
    let soak_estate = soak.dir.0.join("estate");
    let soak_interchange = soak_estate.join("interchange");
    std::fs::create_dir_all(soak_interchange.join("postmaster/state")).unwrap();
    std::fs::create_dir_all(soak_estate.join(".watchers")).unwrap();
    std::fs::create_dir_all(soak_interchange.join("mail")).unwrap();
    std::fs::create_dir_all(soak_interchange.join("state")).unwrap();
    std::fs::write(
        soak_interchange.join("postmaster/state/roster.json"),
        serde_json::to_vec(&json!({
            "schema": "house-roster/2",
            "seats": {
                "rd-1": { "name": "tally", "box": "/tmp/interchange/to-tally" },
                "rd-2": { "name": "nodal", "box": "/tmp/interchange/to-nodal" },
                "rd-3": { "name": "golem", "box": "/tmp/interchange/to-golem" }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(
        soak_estate.join(".watchers/mailwatch-routes.json"),
        serde_json::to_vec(&json!({
            "tally": { "aliases": ["tally", "tally_votp"] }
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(soak_interchange.join("state/acks.jsonl"), "").unwrap();
    // Pre-bind historical letter to `tally` (must be skipped by auto mail horizon on bind).
    std::fs::write(
        soak_interchange.join("mail/1792431783570-pre-bind-to-tally.md"),
        "---\nfrom: crest\nto: tally\ndate: 2026-10-18 12:00 UTC\nsubject: Historical\n---\nold\n",
    )
    .unwrap();
    let tree_before_seed = snapshot_tree(&soak_interchange);

    // Step 2a: `presence seed-interchange --dry-run`
    let dry = soak
        .cli_json(&[
            "presence",
            "seed-interchange",
            "--interchange",
            soak_interchange.to_str().unwrap(),
            "--dry-run",
        ])
        .await;
    assert_eq!(dry["dry_run"], true);
    assert_eq!(snapshot_tree(&soak_interchange), tree_before_seed);

    // Step 2b: `presence seed-interchange` (apply as Operator)
    let applied = soak
        .cli_json(&[
            "presence",
            "seed-interchange",
            "--interchange",
            soak_interchange.to_str().unwrap(),
        ])
        .await;
    assert_eq!(applied["dry_run"], false);
    assert!(applied["applied_count"].as_u64().unwrap() >= 2);
    assert!(
        applied["skipped_targets"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "golem")
    );
    assert_eq!(snapshot_tree(&soak_interchange), tree_before_seed);

    // Step 3: Manual `presence bind --seat tally --from-claude-session <mock_session_json> --ttl 24h`
    let my_pid = std::process::id();
    let my_start = current_process_start().expect("current process start time");
    let session_json_path = soak.dir.0.join("tally-claude-session.json");
    let mock_sock = soak.dir.0.join("tally-claude.sock");
    std::fs::write(
        &session_json_path,
        serde_json::to_vec(&json!({
            "pid": my_pid,
            "procStart": my_start,
            "sessionId": "sess-tally-soak-1",
            "messagingSocketPath": mock_sock.to_str().unwrap(),
            "kind": "interactive",
            "name": "Tally-5.2",
            "nameSource": "user"
        }))
        .unwrap(),
    )
    .unwrap();

    let bind = soak
        .cli_json(&[
            "presence",
            "bind",
            "--seat",
            "tally",
            "--from-claude-session",
            session_json_path.to_str().unwrap(),
            "--ttl",
            "24h",
        ])
        .await;
    assert_eq!(bind["reply"], "presence_bind");
    assert_eq!(bind["value"]["seat"], "tally");
    assert_eq!(bind["value"]["endpoint"]["transport"], "claude_session");
    assert!(
        !mock_sock.exists(),
        "presence bind must never create or connect to messagingSocketPath"
    );

    let tally_rec = soak.cli_json(&["presence", "resolve", "tally_votp"]).await;
    assert_eq!(tally_rec["value"]["record"]["seat"], "tally");
    let horizon_ms = tally_rec["value"]["record"]["mail_horizon_at"]
        .as_u64()
        .expect("bind auto-sets mail_horizon_at");
    let live_list = soak.cli_json(&["presence", "list", "--live-only"]).await;
    let live_seats = live_list["value"]["seats"].as_array().unwrap();
    assert_eq!(live_seats.len(), 1);
    assert_eq!(live_seats[0]["record"]["seat"], "tally");

    // Step 4: `mail shadow --watch` writing outside `interchange/`
    let shadow_out = soak.dir.0.join("tally-soak-shadow.jsonl");
    let _watch_guard = ChildGuard(
        Command::new(bin_path())
            .env_remove("REX_WAVE_TOKEN")
            .env_remove("T3CODE_THREAD_ID")
            .args([
                "--socket",
                soak.sock.to_str().unwrap(),
                "mail",
                "shadow",
                "--interchange",
                soak_interchange.to_str().unwrap(),
                "--out",
                shadow_out.to_str().unwrap(),
                "--watch",
                "--interval-ms",
                "150",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn mail shadow --watch"),
    );

    // Wait for initial shadow pass to write the file (pre-bind letter skipped as pre-horizon).
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(&shadow_out)
                && text.contains("skip_pre_horizon_history")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .expect("mail shadow --watch wrote initial pass with skip_pre_horizon_history");
    assert_eq!(
        snapshot_tree(&soak_interchange),
        tree_before_seed,
        "mail shadow --watch must never mutate interchange"
    );

    // Drop a new post-bind letter addressed to Tally's alias (`tally_votp`).
    let post_id_ms = horizon_ms + 1_000;
    let new_letter_name = format!("{post_id_ms}-post-bind-to-tally-votp.md");
    std::fs::write(
        soak_interchange.join("mail").join(&new_letter_name),
        format!(
            "---\nfrom: crest\nto: tally_votp\nid: {post_id_ms}\nsubject: Live 1-seat soak ring\n---\nhello tally\n"
        ),
    )
    .unwrap();
    let tree_after_letter = snapshot_tree(&soak_interchange);

    // Wait for the running `--watch` loop to emit a Ring decision for `tally`.
    let ring_line = tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(&shadow_out) {
                for line in text.lines() {
                    if let Ok(v) = serde_json::from_str::<Value>(line)
                        && v["decision"] == "ring"
                        && v["seat"] == "tally"
                    {
                        return v;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("mail shadow --watch emitted ring for post-bind letter to tally_votp");

    assert_eq!(ring_line["channel"], "claude_messaging_socket");
    assert_eq!(
        ring_line["referent"]["path"],
        format!("mail/{new_letter_name}")
    );
    assert_eq!(
        snapshot_tree(&soak_interchange),
        tree_after_letter,
        "mail shadow --watch must remain structurally read-only on interchange"
    );
    assert!(
        !mock_sock.exists(),
        "mail shadow --watch must never touch or open messagingSocketPath"
    );
}

#[tokio::test]
async fn pr22_cli_seed_seats_tsv_and_mail_score_mailwatch_seen() {
    let daemon = IsolatedDaemon::start("pr22-cli-soak").await;
    let estate = daemon.dir.0.join("estate");
    let interchange = estate.join("interchange");
    std::fs::create_dir_all(interchange.join("state")).unwrap();
    std::fs::create_dir_all(interchange.join("mail")).unwrap();
    std::fs::create_dir_all(estate.join(".watchers/mailwatch-state")).unwrap();

    std::fs::write(
        interchange.join("state/bb-knock-routes.tsv"),
        "seat\tbb_thread_id\tnote\nfathom\tthr_abc\treview\n",
    )
    .unwrap();
    let seats_tsv = estate.join("seats.tsv");
    std::fs::write(&seats_tsv, "Chroma-Session\tchroma\t/tmp/chroma\n").unwrap();

    let dry_seed = daemon
        .cli_json(&[
            "presence",
            "seed-interchange",
            "--interchange",
            interchange.to_str().unwrap(),
            "--seats-tsv",
            seats_tsv.to_str().unwrap(),
            "--dry-run",
        ])
        .await;
    let seeded_seats: Vec<&str> = dry_seed["claims"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c["seat"].as_str())
        .collect();
    assert!(seeded_seats.contains(&"fathom"));
    assert!(seeded_seats.contains(&"chroma"));
    assert!(seeded_seats.contains(&"neon"));

    // Score a synthetic shadow log with `--mailwatch-seen` in both `--json` and text modes.
    let sha1 = "a".repeat(64);
    let sha2 = "b".repeat(64);
    let shadow_path = daemon.dir.0.join("shadow.jsonl");
    let acks_path = daemon.dir.0.join("acks.jsonl");
    let seen_path = estate.join(".watchers/mailwatch-state/seen.jsonl");
    std::fs::write(
        &shadow_path,
        format!(
            concat!(
                "{{\"meta\":\"aliases\",\"aliases\":{{\"tally_votp\":\"tally\"}}}}\n",
                "{{\"decision\":\"ring\",\"seat\":\"tally\",\"incarnation_id\":\"inc_1\",\"channel\":\"claude_messaging_socket\",\"referent\":{{\"path\":\"mail/1.md\",\"sha16\":\"{s1_16}\",\"sha256\":\"{sha1}\"}},\"time_basis\":\"letter_id_ms\"}}\n",
                "{{\"decision\":\"ring\",\"seat\":\"crest\",\"incarnation_id\":\"inc_2\",\"channel\":\"self_watcher_lease\",\"referent\":{{\"path\":\"mail/2.md\",\"sha16\":\"{s2_16}\",\"sha256\":\"{sha2}\"}},\"time_basis\":\"letter_id_ms\"}}\n"
            ),
            s1_16 = &sha1[..16],
            s2_16 = &sha2[..16],
            sha1 = sha1,
            sha2 = sha2,
        ),
    )
    .unwrap();
    std::fs::write(
        &acks_path,
        format!(r#"{{"acked_by":"tally","letter":"mail/1.md","sha256":"{sha1}"}}"#),
    )
    .unwrap();
    std::fs::write(
        &seen_path,
        format!(
            concat!(
                "{{\"key\":\"shared-tally_votp:{s1_16}\",\"outcome\":\"defer:missing\",\"attempts\":1,\"ts\":\"2026-11-21T19:00:00Z\"}}\n",
                "{{\"key\":\"shared-tally_votp:{s1_16}\",\"outcome\":\"rung\",\"attempts\":2,\"ts\":\"2026-11-21T19:01:00Z\"}}\n",
                "{{\"key\":\"shared-crest:{s2_16}\",\"outcome\":\"own-knocker\",\"attempts\":1,\"ts\":\"2026-11-21T19:02:00Z\"}}\n"
            ),
            s1_16 = &sha1[..16],
            s2_16 = &sha2[..16],
        ),
    )
    .unwrap();

    let score_json = daemon
        .cli_json(&[
            "mail",
            "score",
            "--shadow",
            shadow_path.to_str().unwrap(),
            "--acks",
            acks_path.to_str().unwrap(),
            "--mailwatch-seen",
            seen_path.to_str().unwrap(),
        ])
        .await;
    assert_eq!(score_json["mailwatch_seen"]["seen_entries"], 2);
    assert_eq!(score_json["mailwatch_seen"]["shadow_ring_referents"], 2);
    assert_eq!(score_json["mailwatch_seen"]["agreed_rung"], 1);
    assert_eq!(score_json["mailwatch_seen"]["own_knocker_in_mailwatch"], 1);
    assert_eq!(score_json["mailwatch_seen"]["absent_in_mailwatch"], 0);

    let score_text = daemon
        .cli(
            &[
                "mail",
                "score",
                "--shadow",
                shadow_path.to_str().unwrap(),
                "--acks",
                acks_path.to_str().unwrap(),
                "--mailwatch-seen",
                seen_path.to_str().unwrap(),
            ],
            &[],
            &[],
        )
        .await;
    assert!(score_text.status.success());
    let stdout = String::from_utf8_lossy(&score_text.stdout);
    assert!(
        stdout.contains(
            "mailwatch seen: entries=2 shadow_ring_referents=2 agreed_rung=1 own_knocker=1"
        ),
        "stdout={stdout}"
    );
}

/// Spawns `mail shadow --watch --offline` and yields its text pass summaries.
fn spawn_shadow_watch(
    interchange: &Path,
    out: &Path,
    sock: &Path,
) -> (ChildGuard, std::sync::mpsc::Receiver<String>) {
    let mut child = Command::new(bin_path())
        .env_remove("REX_WAVE_TOKEN")
        .env_remove("T3CODE_THREAD_ID")
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "mail",
            "shadow",
            "--offline",
            "--interchange",
            interchange.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            "--watch",
            "--interval-ms",
            "100",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn mail shadow --watch");
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in StdBufReader::new(stdout).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    (ChildGuard(child), rx)
}

fn next_appended(rx: &std::sync::mpsc::Receiver<String>) -> u64 {
    let line = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("mail shadow --watch pass summary");
    let tail = line
        .split("appended_decisions=")
        .nth(1)
        .unwrap_or_else(|| panic!("summary lacks appended_decisions: {line}"));
    tail.split_whitespace().next().unwrap().parse().unwrap()
}

fn decision_lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .filter(|v| v.get("decision").is_some())
        .collect()
}

#[test]
fn pr24_cli_shadow_watch_appends_transitions_across_restart_and_scores_two_seen_logs() {
    let dir = fresh_dir("pr24-watch");
    let interchange = dir.0.join("interchange");
    std::fs::create_dir_all(interchange.join("mail")).unwrap();
    std::fs::create_dir_all(interchange.join("state")).unwrap();
    std::fs::write(interchange.join("state/acks.jsonl"), "").unwrap();
    std::fs::write(
        interchange.join("mail/1795000000001-to-alpha.md"),
        "---\nfrom: ops\nto: alpha\nid: 1795000000001\nsubject: one\n---\none\n",
    )
    .unwrap();
    let out = dir.0.join("shadow-watch.jsonl");
    let sock = dir.0.join("unused.sock");
    let tree_before = snapshot_tree(&interchange);

    // Run 1: the offline letter banks once, then later passes append nothing.
    {
        let (_guard, rx) = spawn_shadow_watch(&interchange, &out, &sock);
        assert_eq!(next_appended(&rx), 1);
        assert_eq!(next_appended(&rx), 0);
        assert_eq!(next_appended(&rx), 0);
    }
    let after_run1 = decision_lines(&out);
    assert_eq!(after_run1.len(), 1);
    assert_eq!(after_run1[0]["decision"], "raise_unrung");
    assert!(after_run1[0]["observed_at_ms"].as_u64().is_some());

    // Run 2: restart over the same log hydrates it -> no baseline duplicates;
    // a new letter appends only its own transition.
    {
        let (_guard, rx) = spawn_shadow_watch(&interchange, &out, &sock);
        assert_eq!(next_appended(&rx), 0, "restart must not re-append baseline");
        assert_eq!(next_appended(&rx), 0);
        std::fs::write(
            interchange.join("mail/1795000000002-to-beta.md"),
            "---\nfrom: ops\nto: beta\nid: 1795000000002\nsubject: two\n---\ntwo\n",
        )
        .unwrap();
        let appended = (0..20)
            .map(|_| next_appended(&rx))
            .find(|n| *n > 0)
            .expect("new letter appended within 20 passes");
        assert_eq!(appended, 1);
    }
    let after_run2 = decision_lines(&out);
    assert_eq!(after_run2.len(), 2);
    assert_eq!(
        after_run2[0], after_run1[0],
        "existing lines are never rewritten"
    );
    assert_eq!(after_run2[1]["seat"], "beta");
    assert_eq!(after_run2[1]["count"], 1);
    assert_eq!(snapshot_tree(&interchange).len(), tree_before.len() + 1);

    // A symlinked `--out` is refused before anything is appended.
    let link = dir.0.join("link.jsonl");
    std::os::unix::fs::symlink(&out, &link).unwrap();
    let refused = Command::new(bin_path())
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "mail",
            "shadow",
            "--offline",
            "--interchange",
            interchange.to_str().unwrap(),
            "--out",
            link.to_str().unwrap(),
            "--watch",
        ])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("symlink"));

    // `--once` keeps the atomic snapshot overwrite (no watch stamps).
    let snap = dir.0.join("snapshot.jsonl");
    let once = Command::new(bin_path())
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "mail",
            "shadow",
            "--offline",
            "--interchange",
            interchange.to_str().unwrap(),
            "--out",
            snap.to_str().unwrap(),
            "--once",
        ])
        .output()
        .unwrap();
    assert!(once.status.success());
    let snap_lines = decision_lines(&snap);
    assert_eq!(snap_lines.len(), 2);
    assert!(snap_lines.iter().all(|v| v.get("observed_at_ms").is_none()));

    // `mail score` merges a mailwatch and a t3-doorbell seen.jsonl in one run.
    let (sha_a, sha_b) = ("c".repeat(64), "d".repeat(64));
    let ring = |seat: &str, sha: &str| {
        format!(
            "{{\"decision\":\"ring\",\"seat\":\"{seat}\",\"incarnation_id\":\"inc_1\",\"channel\":\"t3_queue_if_active\",\"referent\":{{\"path\":\"mail/x.md\",\"sha16\":\"{}\",\"sha256\":\"{sha}\"}},\"time_basis\":\"letter_id_ms\"}}\n",
            &sha[..16]
        )
    };
    let shadow = dir.0.join("rings.jsonl");
    std::fs::write(
        &shadow,
        format!(
            "{}{}{}",
            ring("alpha", &sha_a),
            ring("beta", &sha_b),
            ring("alpha", &sha_a)
        ),
    )
    .unwrap();
    let acks = dir.0.join("acks.jsonl");
    std::fs::write(&acks, "").unwrap();
    let mailwatch_seen = dir.0.join("mailwatch-seen.jsonl");
    std::fs::write(
        &mailwatch_seen,
        format!(
            "{{\"key\":\"shared-alpha:{}\",\"outcome\":\"rung\"}}\n",
            &sha_a[..16]
        ),
    )
    .unwrap();
    let doorbell_seen = dir.0.join("doorbell-seen.jsonl");
    std::fs::write(
        &doorbell_seen,
        format!(
            "{{\"sha16\":\"{}\",\"seat\":\"beta\",\"outcome\":\"outcome_unknown\"}}\n",
            &sha_b[..16]
        ),
    )
    .unwrap();
    let scored = Command::new(bin_path())
        .args([
            "--json",
            "mail",
            "score",
            "--shadow",
            shadow.to_str().unwrap(),
            "--acks",
            acks.to_str().unwrap(),
            "--mailwatch-seen",
            mailwatch_seen.to_str().unwrap(),
            "--mailwatch-seen",
            doorbell_seen.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        scored.status.success(),
        "{}",
        String::from_utf8_lossy(&scored.stderr)
    );
    let v: Value = serde_json::from_slice(&scored.stdout).unwrap();
    assert_eq!(v["mailwatch_seen"]["seen_entries"], 2);
    assert_eq!(v["mailwatch_seen"]["shadow_ring_referents"], 2);
    assert_eq!(v["mailwatch_seen"]["agreed_rung"], 2);
    assert_eq!(v["shadow_score"]["referents_rung"], 2);
    assert_eq!(v["shadow_score"]["rings_emitted"], 2);
}
