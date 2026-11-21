use futures_util::StreamExt;
use rex_wave_api::read::{AgentEventKind, Read, ThreadPageView};
use rex_wave_api::room::{Body, RoomEvent};
use rex_wave_api::wave::{
    Activity, AgentSpec, BaseBranch, InteractionKind, InteractionResolution, Observation,
    PermissionMode, SteerDeliveryMode, UserAnswer, WaveEvent, WaveResolve, WaveRetireWorker,
    WaveSpec, WaveStart, WaveTell, WaveView, WorkerPhase, WorkerSpec, Workspace,
};
use rex_wave_api::*;
use rex_wave_board::Board;
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    AgentFeed, BoxFut, Hub, LocalClient, ReadSide, Registry, Router, SidebarProjection,
    frame_channel, spawn_reactor,
};
use rex_wave_store::Journal;
use rex_wave_t3::transport::ws_accept;
use rex_wave_t3::{
    CompositeFeed, CompositeHost, RunnerTarget, T3Bridge, T3Config, T3Feed, T3Host,
    read_worker_token_file,
};
use rex_wave_waves::{AgentHost, HostError, SpawnRequest, WaveConductor, Waves, wave_room};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc, watch};

#[derive(Default)]
struct MockT3State {
    expected_bearer: String,
    reject_first_ticket_with_401: bool,
    ticket_401_fired: bool,
    tickets_issued: Vec<String>,
    ws_connections: usize,
    projects: Vec<Value>,
    project_mutations: Vec<Value>,
    launched_threads: Vec<Value>,
    dispatched_commands: Vec<Value>,
    force_stops: Vec<Value>,
    fail_thread_stop: bool,
    interrupts: Vec<String>,
    acks: Vec<String>,
    thread_projections: HashMap<String, Value>,
    shell_txs: Vec<(String, mpsc::UnboundedSender<String>)>,
    thread_sub_txs: HashMap<String, Vec<(String, mpsc::UnboundedSender<String>)>>,
    active_conn_killers: Vec<mpsc::UnboundedSender<()>>,
}

struct MockT3Server {
    addr: std::net::SocketAddr,
    state: Arc<Mutex<MockT3State>>,
    _task: tokio::task::JoinHandle<()>,
}

impl MockT3Server {
    async fn start(state: MockT3State) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(state));
        let st = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let st = st.clone();
                tokio::spawn(async move {
                    handle_mock_conn(stream, st).await;
                });
            }
        });
        MockT3Server {
            addr,
            state,
            _task: task,
        }
    }

    fn origin(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn push_shell_item(&self, item: Value) {
        let st = self.state.lock().await;
        for (req_id, tx) in &st.shell_txs {
            let chunk = json!({
                "_tag": "Chunk",
                "requestId": req_id,
                "values": [item],
            });
            let _ = tx.send(chunk.to_string());
        }
    }

    async fn push_thread_item(&self, thread_id: &str, item: Value) {
        let st = self.state.lock().await;
        if let Some(subs) = st.thread_sub_txs.get(thread_id) {
            for (req_id, tx) in subs {
                let chunk = json!({
                    "_tag": "Chunk",
                    "requestId": req_id,
                    "values": [item],
                });
                let _ = tx.send(chunk.to_string());
            }
        }
    }

    async fn drop_all_ws_connections(&self) {
        let mut st = self.state.lock().await;
        for killer in st.active_conn_killers.drain(..) {
            let _ = killer.send(());
        }
    }
}

async fn read_request(
    stream: &mut TcpStream,
) -> Option<(String, String, HashMap<String, String>, Vec<u8>)> {
    let mut buf = Vec::with_capacity(1024);
    let mut scratch = [0u8; 2048];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        let n = stream.read(&mut scratch).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&scratch[..n]);
    };
    let head = std::str::from_utf8(&buf[..header_end]).ok()?.to_owned();
    let mut body = buf[header_end + 4..].to_vec();
    let mut lines = head.split("\r\n");
    let req_line = lines.next()?;
    let mut parts = req_line.split_whitespace();
    let method = parts.next()?.to_owned();
    let target = parts.next()?.to_owned();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
        }
    }
    if let Some(len) = headers
        .get("content-length")
        .and_then(|v| v.parse::<usize>().ok())
    {
        while body.len() < len {
            let n = stream.read(&mut scratch).await.ok()?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&scratch[..n]);
        }
        body.truncate(len);
    }
    Some((method, target, headers, body))
}

async fn write_http_json(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &Value,
    chunked: bool,
) {
    let bytes = serde_json::to_vec(body).unwrap();
    if chunked {
        let head = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
        );
        let _ = stream.write_all(head.as_bytes()).await;
        let mid = bytes.len() / 2;
        for part in [&bytes[..mid], &bytes[mid..]] {
            if !part.is_empty() {
                let _ = stream
                    .write_all(format!("{:x}\r\n", part.len()).as_bytes())
                    .await;
                let _ = stream.write_all(part).await;
                let _ = stream.write_all(b"\r\n").await;
            }
        }
        let _ = stream.write_all(b"0\r\n\r\n").await;
    } else {
        let head = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            bytes.len()
        );
        let _ = stream.write_all(head.as_bytes()).await;
        let _ = stream.write_all(&bytes).await;
    }
    let _ = stream.flush().await;
}

fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(val) =
                u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""), 16)
        {
            out.push(val);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
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

async fn read_client_ws_frame<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> Option<(u8, Vec<u8>)> {
    let mut hdr = [0u8; 2];
    reader.read_exact(&mut hdr).await.ok()?;
    let opcode = hdr[0] & 0x0f;
    let masked = (hdr[1] & 0x80) != 0;
    let len7 = (hdr[1] & 0x7f) as usize;
    let len = match len7 {
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

async fn handle_mock_conn(mut stream: TcpStream, state: Arc<Mutex<MockT3State>>) {
    let Some((method, target, headers, body)) = read_request(&mut stream).await else {
        return;
    };

    if headers
        .get("upgrade")
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
    {
        let query = target.split_once('?').map(|(_, q)| q).unwrap_or("");
        let mut proto = None;
        let mut ticket = None;
        for kv in query.split('&') {
            if let Some((k, v)) = kv.split_once('=') {
                if k == "orchestrationProtocol" {
                    proto = Some(v.to_owned());
                } else if k == "wsTicket" {
                    ticket = Some(percent_decode(v));
                }
            }
        }
        let ticket_ok = {
            let st = state.lock().await;
            proto.as_deref() == Some("2")
                && ticket
                    .as_deref()
                    .is_some_and(|t| st.tickets_issued.iter().any(|issued| issued == t))
        };
        if !ticket_ok {
            write_http_json(
                &mut stream,
                401,
                "Unauthorized",
                &json!({"error": "invalid wsTicket"}),
                false,
            )
            .await;
            return;
        }
        let Some(ws_key) = headers.get("sec-websocket-key") else {
            return;
        };
        let accept = ws_accept(ws_key);
        let resp = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        if stream.write_all(resp.as_bytes()).await.is_err() {
            return;
        }
        let _ = stream.flush().await;
        serve_mock_ws(stream, state).await;
        return;
    }

    let proto_hdr = headers
        .get("x-t3-orchestration-protocol")
        .map(String::as_str);
    let auth_hdr = headers.get("authorization").map(String::as_str);
    {
        let mut st = state.lock().await;
        if proto_hdr != Some("2") {
            write_http_json(
                &mut stream,
                400,
                "Bad Request",
                &json!({"error": "missing x-t3-orchestration-protocol: 2"}),
                false,
            )
            .await;
            return;
        }
        if method == "POST"
            && target == "/api/auth/websocket-ticket"
            && st.reject_first_ticket_with_401
            && !st.ticket_401_fired
        {
            st.ticket_401_fired = true;
            write_http_json(
                &mut stream,
                401,
                "Unauthorized",
                &json!({"error": "expired bearer token"}),
                false,
            )
            .await;
            return;
        }
        let expected = format!("Bearer {}", st.expected_bearer);
        if !st.expected_bearer.is_empty() && auth_hdr != Some(expected.as_str()) {
            write_http_json(
                &mut stream,
                401,
                "Unauthorized",
                &json!({"error": "bad bearer"}),
                false,
            )
            .await;
            return;
        }
    }

    match (method.as_str(), target.as_str()) {
        ("POST", "/api/auth/websocket-ticket") => {
            let mut st = state.lock().await;
            let n = st.tickets_issued.len() + 1;
            let ticket = format!("wst_{n}+/=");
            st.tickets_issued.push(ticket.clone());
            write_http_json(
                &mut stream,
                200,
                "OK",
                &json!({"ticket": ticket, "expiresAt": "2026-11-21T15:00:00Z"}),
                false,
            )
            .await;
        }
        ("GET", "/api/projects") => {
            let st = state.lock().await;
            write_http_json(
                &mut stream,
                200,
                "OK",
                &json!({"projects": st.projects, "updatedAt": "2026-11-21T08:00:00Z"}),
                true,
            )
            .await;
        }
        ("POST", "/api/projects/mutate") => {
            let val: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
            let mut st = state.lock().await;
            st.project_mutations.push(val.clone());
            let created = json!({
                "id": val["projectId"].as_str().unwrap_or("proj-created"),
                "title": val["title"].as_str().unwrap_or("unnamed"),
                "workspaceRoot": val["workspaceRoot"].as_str().unwrap_or("/tmp"),
            });
            st.projects.push(created.clone());
            write_http_json(&mut stream, 200, "OK", &created, false).await;
        }
        ("GET", p) if p.starts_with("/api/orchestration/threads/") => {
            let raw_tid = p.trim_start_matches("/api/orchestration/threads/");
            let tid = percent_decode(raw_tid);
            let st = state.lock().await;
            if let Some(proj) = st.thread_projections.get(&tid) {
                write_http_json(
                    &mut stream,
                    200,
                    "OK",
                    &json!({"snapshotSequence": 10, "projection": proj}),
                    true,
                )
                .await;
            } else {
                write_http_json(
                    &mut stream,
                    404,
                    "Not Found",
                    &json!({"error": "thread not found"}),
                    false,
                )
                .await;
            }
        }
        _ => {
            write_http_json(
                &mut stream,
                404,
                "Not Found",
                &json!({"error": "unknown route"}),
                false,
            )
            .await;
        }
    }
}

async fn serve_mock_ws(stream: TcpStream, state: Arc<Mutex<MockT3State>>) {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    let (kill_tx, mut kill_rx) = mpsc::unbounded_channel::<()>();

    {
        let mut st = state.lock().await;
        st.ws_connections += 1;
        st.active_conn_killers.push(kill_tx);
    }

    let write_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if writer
                .write_all(&encode_server_ws_text(&msg))
                .await
                .is_err()
            {
                break;
            }
            let _ = writer.flush().await;
        }
    });

    loop {
        tokio::select! {
            biased;
            _ = kill_rx.recv() => break,
            frame = read_client_ws_frame(&mut reader) => {
                let Some((opcode, payload)) = frame else { break };
                if opcode == 0x8 {
                    break;
                }
                if opcode != 0x1 {
                    continue;
                }
                let Ok(val) = serde_json::from_slice::<Value>(&payload) else { continue };
                let tag = val.get("_tag").and_then(|v| v.as_str()).unwrap_or("");
                match tag {
                    "Ack" => {
                        if let Some(rid) = val.get("requestId").and_then(|v| v.as_str()) {
                            state.lock().await.acks.push(rid.to_owned());
                        }
                    }
                    "Interrupt" => {
                        if let Some(rid) = val.get("requestId").and_then(|v| v.as_str()) {
                            let mut st = state.lock().await;
                            st.interrupts.push(rid.to_owned());
                            st.shell_txs.retain(|(id, _)| id != rid);
                            for subs in st.thread_sub_txs.values_mut() {
                                subs.retain(|(id, _)| id != rid);
                            }
                        }
                    }
                    "Request" => {
                        let req_id = val["id"].as_str().unwrap_or("").to_owned();
                        let method = val["tag"].as_str().unwrap_or("").to_owned();
                        let req_payload = val.get("payload").cloned().unwrap_or(Value::Null);
                        handle_mock_rpc(&req_id, &method, req_payload, &state, &out_tx).await;
                    }
                    _ => {}
                }
            }
        }
    }

    write_task.abort();
}

async fn handle_mock_rpc(
    req_id: &str,
    method: &str,
    payload: Value,
    state: &Arc<Mutex<MockT3State>>,
    out_tx: &mpsc::UnboundedSender<String>,
) {
    let send_ok = |val: Value| {
        let frame = json!({
            "_tag": "Exit",
            "requestId": req_id,
            "exit": { "_tag": "Success", "value": val },
        });
        let _ = out_tx.send(frame.to_string());
    };
    let send_err = |msg: &str| {
        let frame = json!({
            "_tag": "Exit",
            "requestId": req_id,
            "exit": { "_tag": "Failure", "cause": [{ "_tag": "Fail", "error": { "message": msg } }] },
        });
        let _ = out_tx.send(frame.to_string());
    };

    match method {
        "server.getConfig" => {
            send_ok(json!({
                "providers": [
                    { "instanceId": "antigravity", "models": [{ "slug": "gemini-4-neon" }] },
                    { "instanceId": "claudeAgent", "models": [{ "slug": "claude-opus-5-5" }] },
                    { "instanceId": "codex", "models": [{ "slug": "gpt-6.1-sol" }] },
                ]
            }));
        }
        "orchestration.launchThread" => {
            let tid = payload["threadId"]
                .as_str()
                .unwrap_or("t3:unknown")
                .to_owned();
            let mut st = state.lock().await;
            let already = st.thread_projections.contains_key(&tid);
            st.launched_threads.push(payload.clone());
            if !already {
                let proj = json!({
                    "thread": { "id": tid, "archivedAt": null },
                    "runs": [{ "id": "run_1", "ordinal": 1, "status": "running", "startedAt": "2026-11-21T08:00:00Z" }],
                    "providerTurns": [],
                    "runtimeRequests": [],
                    "turnItems": [
                        {
                            "id": "item_0",
                            "runId": "run_1",
                            "ordinal": 0,
                            "type": "user_message",
                            "status": "completed",
                            "text": payload["initialMessage"]["text"].as_str().unwrap_or(""),
                            "updatedAt": "2026-11-21T08:00:00Z"
                        }
                    ]
                });
                st.thread_projections.insert(tid.clone(), proj);
            }
            send_ok(json!({
                "threadId": tid,
                "resumed": already,
                "sequence": 1
            }));
        }
        "orchestration.dispatchCommand" => {
            let mut st = state.lock().await;
            st.dispatched_commands.push(payload.clone());
            let cmd_type = payload["type"].as_str().unwrap_or("");
            if cmd_type == "thread.stop" && st.fail_thread_stop {
                send_err("provider unresponsive to stop");
                return;
            }
            if cmd_type == "thread.archive"
                && let Some(tid) = payload["threadId"].as_str()
                && let Some(proj) = st.thread_projections.get_mut(tid)
            {
                proj["thread"]["archivedAt"] = json!("2026-11-21T08:05:00Z");
            }
            send_ok(json!({ "sequence": st.dispatched_commands.len() }));
        }
        "orchestration.forceStopThread" => {
            let mut st = state.lock().await;
            st.force_stops.push(payload);
            send_ok(json!({ "outcome": "killed" }));
        }
        "orchestration.subscribeShell" => {
            let mut st = state.lock().await;
            st.shell_txs.push((req_id.to_owned(), out_tx.clone()));
            let snapshot = json!({
                "_tag": "Chunk",
                "requestId": req_id,
                "values": [{
                    "kind": "snapshot",
                    "snapshot": {
                        "schemaVersion": 2,
                        "snapshotSequence": 5,
                        "projects": st.projects,
                        "threads": [],
                        "archivedThreads": []
                    }
                }]
            });
            let _ = out_tx.send(snapshot.to_string());
        }
        "orchestration.subscribeThread" => {
            let tid = payload["threadId"].as_str().unwrap_or("").to_owned();
            let mut st = state.lock().await;
            st.thread_sub_txs
                .entry(tid.clone())
                .or_default()
                .push((req_id.to_owned(), out_tx.clone()));
            if let Some(proj) = st.thread_projections.get(&tid).cloned() {
                let snap = json!({
                    "_tag": "Chunk",
                    "requestId": req_id,
                    "values": [{
                        "kind": "snapshot",
                        "snapshotSequence": 10,
                        "projection": proj
                    }]
                });
                let _ = out_tx.send(snap.to_string());
            }
        }
        other => send_err(&format!("unknown RPC method {other}")),
    }
}

fn build_test_router(dir: &std::path::Path, agents: Arc<dyn AgentFeed>) -> Arc<Router> {
    let journal = Journal::open(&dir.join("journal.sqlite3")).unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Registry::<Waves>::new(journal.clone(), hub.clone()),
        board: Registry::<Board>::new(journal.clone(), hub.clone()),
        inbox: Arc::new(rex_wave_runtime::Unavailable(Family::Inbox)),
        mail: Arc::new(rex_wave_runtime::Unavailable(Family::Mail)),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents,
        }),
    })
}

async fn wait_until<F, Fut>(timeout: Duration, mut pred: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if pred().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition did not become true within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

#[tokio::test]
async fn full_t3_lifecycle_discovery_401_refresh_approval_mirror_and_retire() {
    let tmp = tempfile::tempdir().unwrap();
    let t3_home = tmp.path().join("t3-home");
    let bb_dir = tmp.path().join("bb-data");
    let state_dir = tmp.path().join("rex-state");
    let repo_dir = tmp.path().join("alpha-repo");
    std::fs::create_dir_all(t3_home.join("userdata")).unwrap();
    std::fs::create_dir_all(t3_home.join("bin")).unwrap();
    std::fs::create_dir_all(&bb_dir).unwrap();
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::create_dir_all(&repo_dir).unwrap();

    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_minted_2".into(),
        reject_first_ticket_with_401: true,
        fail_thread_stop: true,
        projects: vec![json!({
            "id": "uuid-existing",
            "title": "existing-proj",
            "workspaceRoot": "/tmp/other-repo"
        })],
        ..Default::default()
    })
    .await;

    std::fs::write(
        t3_home.join("userdata/server-runtime.json"),
        serde_json::to_vec(&json!({
            "version": 1,
            "pid": 99999,
            "host": "127.0.0.1",
            "port": server.addr.port(),
            "origin": server.origin(),
            "serverVersion": "0.1.11"
        }))
        .unwrap(),
    )
    .unwrap();

    let counter_file = tmp.path().join("mint-count");
    let fake_t3 = t3_home.join("bin/t3");
    let script = format!(
        "#!/bin/sh\n\
         if [ \"$1\" != \"auth\" ] || [ \"$2\" != \"session\" ] || [ \"$3\" != \"issue\" ]; then exit 2; fi\n\
         c=0\n\
         if [ -f \"{}\" ]; then c=$(cat \"{}\"); fi\n\
         c=$((c + 1))\n\
         echo \"$c\" > \"{}\"\n\
         echo \"tok_minted_$c\"\n",
        counter_file.display(),
        counter_file.display(),
        counter_file.display()
    );
    std::fs::write(&fake_t3, script).unwrap();
    std::fs::set_permissions(&fake_t3, std::fs::Permissions::from_mode(0o755)).unwrap();

    std::fs::write(
        bb_dir.join("projects.json"),
        serde_json::to_vec(&json!({
            "projects": [{
                "id": "proj_alpha",
                "name": "alpha-repo",
                "sources": [{
                    "type": "local_path",
                    "path": repo_dir.to_string_lossy(),
                    "isDefault": true
                }]
            }]
        }))
        .unwrap(),
    )
    .unwrap();

    let mut cfg = T3Config::from_home(t3_home);
    cfg.bb_data_dir = Some(bb_dir);
    cfg.state_dir = Some(state_dir.clone());
    cfg.reconnect_min = Duration::from_millis(15);
    cfg.reconnect_max = Duration::from_millis(60);

    let bridge = T3Bridge::new(cfg);
    let router = build_test_router(tmp.path(), bridge.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));

    let conductor = WaveConductor::new(T3Host(bridge.clone()));
    let conductor_task = spawn_reactor(
        conductor,
        LocalClient::new(
            router.clone(),
            Principal::System {
                part: SystemPart::WaveConductor,
            },
        ),
    );

    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;
    assert_eq!(std::fs::read_to_string(&counter_file).unwrap().trim(), "2");

    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let wave = WaveSlug::parse("auth-v2").unwrap();
    let role = RoleName::parse("gemini-rev").unwrap();
    let project = BbProjectId::parse("proj_alpha").unwrap();

    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse("Verify T3 Code host lifecycle").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("t3:antigravity").unwrap(),
                    model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                    reasoning: Some("xhigh".into()),
                    service_tier: None,
                    permission: PermissionMode::AcceptEdits,
                    workspace: Workspace::ManagedWorktree {
                        base: BaseBranch::Named {
                            name: "main".into(),
                        },
                    },
                },
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("Audit T3 wire integration").unwrap(),
                    agent: None,
                }],
            },
            idem: None,
        })
        .await
        .unwrap();

    let expected_tid = BbThreadId::parse("t3:auth-v2:gemini-rev:a1").unwrap();
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let expected_tid = expected_tid.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            snap.workers.iter().any(|(_, _, phase)| {
                matches!(phase, WorkerPhase::Live { thread, .. } if thread == &expected_tid)
            })
        }
    })
    .await;

    {
        let st = server.state.lock().await;
        assert_eq!(st.project_mutations.len(), 1);
        assert_eq!(st.project_mutations[0]["type"], "project.create");
        assert_eq!(st.launched_threads.len(), 1);
        let launch = &st.launched_threads[0];
        assert_eq!(launch["threadId"], "t3:auth-v2:gemini-rev:a1");
        assert_eq!(launch["commandId"], "rex-wave:spawn:auth-v2:gemini-rev:a1");
        assert_eq!(launch["reuseExistingThread"], true);
        assert_eq!(launch["creationSource"], "server");
        assert_eq!(launch["runtimeMode"], "auto-accept-edits");
        assert_eq!(
            launch["workspaceStrategy"],
            json!({"type": "worktree", "baseRef": "main"})
        );
        assert_eq!(launch["modelSelection"]["instanceId"], "antigravity");
        assert_eq!(launch["modelSelection"]["model"], "gemini-4-neon");
    }

    let token_path = rex_wave_t3::worker_token_path(&state_dir, expected_tid.as_str()).unwrap();
    let token_meta = std::fs::symlink_metadata(&token_path).unwrap();
    assert_eq!(token_meta.permissions().mode() & 0o777, 0o600);
    let initial_token = read_worker_token_file(&state_dir, expected_tid.as_str())
        .unwrap()
        .expect("worker token written on spawn");

    // Idempotent retry on T3Host::spawn reuses the same deterministic threadId and token.
    let retry_tid = T3Host(bridge.clone())
        .spawn(SpawnRequest {
            wave: wave.clone(),
            role: role.clone(),
            attempt: 1,
            project: project.clone(),
            agent: AgentSpec {
                provider: ProviderId::parse("t3:antigravity").unwrap(),
                model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                reasoning: None,
                service_tier: None,
                permission: PermissionMode::AcceptEdits,
                workspace: Workspace::ManagedWorktree {
                    base: BaseBranch::Named {
                        name: "main".into(),
                    },
                },
            },
            room: wave_room(&wave, &project),
            prompt: Body::parse("Audit T3 wire integration").unwrap(),
            title: "[auth-v2] @gemini-rev".into(),
        })
        .await
        .unwrap();
    assert_eq!(retry_tid, expected_tid);
    assert_eq!(
        read_worker_token_file(&state_dir, expected_tid.as_str()).unwrap(),
        Some(initial_token)
    );

    // Trigger Blocked (approval_request) -> resolve -> Working -> Idle with final-turn mirroring.
    {
        let mut st = server.state.lock().await;
        let proj = st
            .thread_projections
            .get_mut(expected_tid.as_str())
            .unwrap();
        proj["runtimeRequests"] = json!([{
            "id": "req_perm_1",
            "kind": "command",
            "status": "pending",
            "createdAt": "2026-11-21T08:01:00Z"
        }]);
        proj["turnItems"].as_array_mut().unwrap().push(json!({
            "id": "item_1",
            "runId": "run_1",
            "ordinal": 1,
            "type": "approval_request",
            "status": "waiting",
            "requestId": "req_perm_1",
            "requestKind": "command",
            "reason": "Run cargo test --workspace",
            "updatedAt": "2026-11-21T08:01:00Z"
        }));
    }
    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 6,
            "location": "active",
            "thread": {
                "id": expected_tid.as_str(),
                "status": "waiting",
                "pendingRuntimeRequest": {
                    "id": "req_perm_1",
                    "kind": "command",
                    "blocking": true,
                    "createdAt": "2026-11-21T08:01:00Z"
                },
                "archivedAt": null
            }
        }))
        .await;

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            snap.workers.iter().any(|(_, _, phase)| {
                matches!(
                    phase,
                    WorkerPhase::Live {
                        activity: Activity::Blocked { interaction },
                        ..
                    } if interaction.id.as_str() == "req_perm_1"
                        && interaction.kind == InteractionKind::Permission
                        && interaction.summary == "Run cargo test --workspace"
                )
            })
        }
    })
    .await;

    operator
        .call(WaveResolve {
            wave: wave.clone(),
            role: role.clone(),
            interaction: BbInteractionId::parse("req_perm_1").unwrap(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        })
        .await
        .unwrap();

    wait_until(Duration::from_secs(3), || async {
        let st = server.state.lock().await;
        st.dispatched_commands.iter().any(|c| {
            c["type"] == "runtime-request.respond"
                && c["requestId"] == "req_perm_1"
                && c["decision"] == "accept"
        })
    })
    .await;

    // Exercise steer + queue delivery intents via WaveTell.
    operator
        .call(WaveTell {
            wave: wave.clone(),
            role: role.clone(),
            body: Body::parse("check edge cases").unwrap(),
            mode: SteerDeliveryMode::SteerIfActive,
        })
        .await
        .unwrap();
    operator
        .call(WaveTell {
            wave: wave.clone(),
            role: role.clone(),
            body: Body::parse("follow up after run").unwrap(),
            mode: SteerDeliveryMode::QueueIfActive,
        })
        .await
        .unwrap();

    wait_until(Duration::from_secs(3), || async {
        let st = server.state.lock().await;
        let has_steer = st.dispatched_commands.iter().any(|c| {
            c["type"] == "message.dispatch"
                && c["text"] == "check edge cases"
                && c["deliveryIntent"] == "steer"
        });
        let has_queue = st.dispatched_commands.iter().any(|c| {
            c["type"] == "message.dispatch"
                && c["text"] == "follow up after run"
                && c.get("deliveryIntent").is_none()
        });
        has_steer && has_queue
    })
    .await;

    // Transition Working -> Blocked(Question) -> UserAnswer -> Idle with final-turn mirroring.
    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 7,
            "location": "active",
            "thread": {
                "id": expected_tid.as_str(),
                "status": "running",
                "pendingRuntimeRequest": null,
                "archivedAt": null
            }
        }))
        .await;

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            snap.workers.iter().any(|(_, _, phase)| {
                matches!(
                    phase,
                    WorkerPhase::Live {
                        activity: Activity::Working,
                        ..
                    }
                )
            })
        }
    })
    .await;

    {
        let mut st = server.state.lock().await;
        let proj = st
            .thread_projections
            .get_mut(expected_tid.as_str())
            .unwrap();
        proj["runtimeRequests"] = json!([{
            "id": "req_q_2",
            "kind": "user_input",
            "status": "pending",
            "createdAt": "2026-11-21T08:02:00Z"
        }]);
        proj["turnItems"].as_array_mut().unwrap().push(json!({
            "id": "item_2",
            "runId": "run_1",
            "ordinal": 2,
            "type": "user_input_request",
            "status": "waiting",
            "requestId": "req_q_2",
            "questions": [{
                "id": "target",
                "header": "Target",
                "question": "Which branch should I rebase on?",
                "options": [
                    { "label": "main", "description": "Default branch" },
                    { "label": "release", "description": "Release branch" }
                ],
                "multiSelect": false
            }],
            "updatedAt": "2026-11-21T08:02:00Z"
        }));
    }
    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 8,
            "location": "active",
            "thread": {
                "id": expected_tid.as_str(),
                "status": "waiting",
                "pendingRuntimeRequest": {
                    "id": "req_q_2",
                    "kind": "user_input",
                    "blocking": true,
                    "createdAt": "2026-11-21T08:02:00Z"
                },
                "archivedAt": null
            }
        }))
        .await;

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            snap.workers.iter().any(|(_, _, phase)| {
                matches!(
                    phase,
                    WorkerPhase::Live {
                        activity: Activity::Blocked { interaction },
                        ..
                    } if interaction.id.as_str() == "req_q_2"
                        && matches!(&interaction.kind, InteractionKind::Question { questions } if questions.len() == 1 && questions[0].id == "target")
                )
            })
        }
    })
    .await;

    let mut answers = BTreeMap::new();
    answers.insert(
        "target".into(),
        UserAnswer {
            selected: vec!["main".into()],
            free_text: None,
        },
    );
    operator
        .call(WaveResolve {
            wave: wave.clone(),
            role: role.clone(),
            interaction: BbInteractionId::parse("req_q_2").unwrap(),
            resolution: InteractionResolution::UserAnswer { answers },
        })
        .await
        .unwrap();

    wait_until(Duration::from_secs(3), || async {
        let st = server.state.lock().await;
        st.dispatched_commands.iter().any(|c| {
            c["type"] == "runtime-request.respond"
                && c["requestId"] == "req_q_2"
                && c["answers"]["target"] == "main"
        })
    })
    .await;

    // Complete run with an unposted assistant_message; verify idempotent mirroring into #wave-auth-v2.
    {
        let mut st = server.state.lock().await;
        let proj = st
            .thread_projections
            .get_mut(expected_tid.as_str())
            .unwrap();
        proj["runtimeRequests"] = json!([]);
        proj["runs"][0]["status"] = json!("completed");
        proj["turnItems"].as_array_mut().unwrap().push(json!({
            "id": "item_3",
            "messageId": "msg_final_1",
            "runId": "run_1",
            "ordinal": 3,
            "type": "assistant_message",
            "status": "completed",
            "streaming": false,
            "text": "Audit complete: all T3 wire checks passed.",
            "updatedAt": "2026-11-21T08:03:00Z"
        }));
    }
    for seq in [9u64, 10u64] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": expected_tid.as_str(),
                    "status": if seq == 9 { "completed" } else { "idle" },
                    "pendingRuntimeRequest": null,
                    "archivedAt": null
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            snap.workers.iter().any(|(_, _, phase)| {
                matches!(
                    phase,
                    WorkerPhase::Live {
                        activity: Activity::Idle,
                        ..
                    }
                )
            })
        }
    })
    .await;

    let room = wave_room(&wave, &project);
    let room_page = operator
        .call(Read {
            stream: StreamId::Room { room: room.clone() },
            after: Seq::ORIGIN,
            limit: Some(100),
        })
        .await
        .unwrap();
    let mirrored: Vec<&Envelope> = room_page
        .entries
        .iter()
        .filter(|e| {
            matches!(
                &e.event,
                Event::Room(RoomEvent::Posted { body, .. })
                    if body.as_str() == "Audit complete: all T3 wire checks passed."
            )
        })
        .collect();
    assert_eq!(mirrored.len(), 1, "final turn mirrored exactly once");
    assert_eq!(
        mirrored[0].by,
        Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        }
    );

    // Retire worker: thread.stop fails -> forceStopThread fallback -> thread.archive -> token unlinked -> Retired.
    operator
        .call(WaveRetireWorker {
            wave: wave.clone(),
            role: role.clone(),
            reason: Some("done".into()),
        })
        .await
        .unwrap();

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            snap.workers
                .iter()
                .any(|(_, _, phase)| matches!(phase, WorkerPhase::Retired { .. }))
        }
    })
    .await;

    {
        let st = server.state.lock().await;
        assert_eq!(st.force_stops.len(), 1);
        assert_eq!(st.force_stops[0]["threadId"], expected_tid.as_str());
        assert!(
            st.dispatched_commands.iter().any(|c| {
                c["type"] == "thread.archive" && c["threadId"] == expected_tid.as_str()
            })
        );
    }
    assert_eq!(
        read_worker_token_file(&state_dir, expected_tid.as_str()).unwrap(),
        None,
        "worker token unlinked on retire"
    );

    let _ = shutdown_tx.send(true);
    conductor_task.abort();
    bridge_task.abort();
}

#[tokio::test]
async fn t3_feed_page_live_interrupt_on_drop_and_project_names_updates() {
    let tmp = tempfile::tempdir().unwrap();
    let tid = BbThreadId::parse("t3:inspect-v1:worker:a1").unwrap();
    let initial_proj = json!({
        "thread": { "id": tid.as_str(), "archivedAt": null },
        "runs": [{ "id": "run_1", "ordinal": 1, "status": "running" }],
        "providerTurns": [{
            "id": "pt_1",
            "ordinal": 1,
            "tokenUsage": {
                "usedTokens": 64000,
                "maxTokens": 200000,
                "inputTokens": 48000,
                "cachedInputTokens": 10000,
                "outputTokens": 6000,
                "autoCompactThreshold": 160000,
                "updatedAt": "2026-11-21T08:10:00Z"
            }
        }],
        "runtimeRequests": [],
        "turnItems": [
            { "id": "i0", "ordinal": 0, "type": "user_message", "status": "completed", "text": "hello", "updatedAt": "2026-11-21T08:00:00Z" },
            { "id": "i1", "ordinal": 1, "type": "command_execution", "status": "completed", "input": "cargo test", "exitCode": 0, "completedAt": "2026-11-21T08:00:05Z" },
            { "id": "i2", "ordinal": 2, "type": "file_change", "status": "completed", "fileName": "src/lib.rs", "additions": 10, "deletions": 2, "completedAt": "2026-11-21T08:00:06Z" },
            { "id": "i3", "ordinal": 3, "type": "dynamic_tool", "status": "inProgress", "toolName": "read_file", "title": "Cargo.toml", "startedAt": "2026-11-21T08:00:07Z" },
            { "id": "i4", "ordinal": 4, "type": "approval_request", "status": "completed", "requestId": "req_a", "requestKind": "command", "reason": "Allow git status", "updatedAt": "2026-11-21T08:00:08Z" },
            { "id": "i5", "ordinal": 5, "type": "user_input_request", "status": "completed", "requestId": "req_q", "questions": [{"id": "q", "header": "H", "question": "Proceed?", "options": []}], "updatedAt": "2026-11-21T08:00:09Z" },
            { "id": "i6", "ordinal": 6, "type": "error", "status": "failed", "failure": {"message": "rate limited"}, "updatedAt": "2026-11-21T08:00:10Z" },
            { "id": "i7", "ordinal": 7, "type": "assistant_message", "status": "completed", "streaming": false, "text": "Done!", "completedAt": "2026-11-21T08:00:11Z" }
        ]
    });

    let mut projs = HashMap::new();
    projs.insert(tid.to_string(), initial_proj);
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_feed".into(),
        projects: vec![json!({
            "id": "uuid-proj-1",
            "title": "rex-wave",
            "workspaceRoot": "/tmp/rex-wave"
        })],
        thread_projections: projs,
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_feed").unwrap();
    cfg.reconnect_min = Duration::from_millis(15);
    cfg.reconnect_max = Duration::from_millis(60);
    let bridge = T3Bridge::new(cfg);
    let router = build_test_router(tmp.path(), bridge.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));

    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    let wave = WaveSlug::parse("inspect-v1").unwrap();
    let role = RoleName::parse("worker").unwrap();
    bridge.observe_wave_event(
        wave.clone(),
        WaveEvent::WorkerObserved {
            role: role.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: tid.clone(),
            },
        },
    );

    let feed = T3Feed(bridge.clone());
    let page1 = feed.page(&wave, &role, None, 5).await.unwrap();
    assert_eq!(page1.events.len(), 5);
    assert_eq!(page1.next, Some(BbSeq(4)));
    assert!(
        matches!(&page1.events[0].kind, AgentEventKind::UserMessage { text } if text == "hello")
    );
    assert!(
        matches!(&page1.events[1].kind, AgentEventKind::ToolResult { name, ok: true, .. } if name == "command_execution")
    );
    assert!(
        matches!(&page1.events[2].kind, AgentEventKind::ToolResult { name, ok: true, summary } if name == "file_change" && summary.contains("+10/-2"))
    );
    assert!(
        matches!(&page1.events[3].kind, AgentEventKind::ToolCall { name, .. } if name == "read_file")
    );
    assert!(
        matches!(&page1.events[4].kind, AgentEventKind::Other { bb_type } if bb_type.contains("approval_request"))
    );

    let usage = page1.usage.expect("providerTurns tokenUsage parsed");
    assert_eq!(usage.used_tokens, 64_000);
    assert_eq!(usage.model_context_window, 200_000);
    assert_eq!(usage.auto_compact_at_tokens, Some(160_000));
    assert_eq!(usage.cost_usd, None);

    let page2 = feed.page(&wave, &role, page1.next, 10).await.unwrap();
    assert_eq!(page2.events.len(), 3);
    assert!(
        matches!(&page2.events[0].kind, AgentEventKind::Other { bb_type } if bb_type.contains("user_input_request"))
    );
    assert!(
        matches!(&page2.events[1].kind, AgentEventKind::Other { bb_type } if bb_type.contains("rate limited"))
    );
    assert!(
        matches!(&page2.events[2].kind, AgentEventKind::AssistantText { text } if text == "Done!")
    );

    let mut live = feed.live(&wave, &role).await.unwrap();
    let mut got_snapshot_items = 0usize;
    while got_snapshot_items < 8 {
        let frame = tokio::time::timeout(Duration::from_secs(2), live.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(frame, Frame::Agent { .. }));
        got_snapshot_items += 1;
    }

    server
        .push_thread_item(
            tid.as_str(),
            json!({
                "kind": "event",
                "sequence": 11,
                "event": {
                    "type": "turn-item.updated",
                    "payload": {
                        "id": "i8",
                        "ordinal": 8,
                        "type": "assistant_message",
                        "status": "completed",
                        "streaming": false,
                        "text": "Live chunk arrived",
                        "updatedAt": "2026-11-21T08:11:00Z"
                    }
                }
            }),
        )
        .await;

    let live_frame = tokio::time::timeout(Duration::from_secs(2), live.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        live_frame,
        Frame::Agent { event, .. } if matches!(event.kind, AgentEventKind::AssistantText { ref text } if text == "Live chunk arrived")
    ));

    // Dropping the FrameStream must emit an Effect RPC `Interrupt` for `subscribeThread`.
    drop(live);
    wait_until(Duration::from_secs(2), || async {
        let st = server.state.lock().await;
        !st.interrupts.is_empty() && !st.acks.is_empty()
    })
    .await;

    let names_rx = feed.project_names().expect("project_names watch");
    wait_until(Duration::from_secs(2), || async {
        names_rx
            .borrow()
            .get(&BbProjectId::parse("uuid-proj-1").unwrap())
            .map(String::as_str)
            == Some("rex-wave")
    })
    .await;

    server
        .push_shell_item(json!({
            "kind": "project.updated",
            "sequence": 12,
            "project": { "id": "uuid-proj-2", "title": "new-repo", "workspaceRoot": "/tmp/new-repo" }
        }))
        .await;
    wait_until(Duration::from_secs(2), || async {
        names_rx
            .borrow()
            .get(&BbProjectId::parse("uuid-proj-2").unwrap())
            .map(String::as_str)
            == Some("new-repo")
    })
    .await;

    server
        .push_shell_item(json!({
            "kind": "project.removed",
            "sequence": 13,
            "projectId": "uuid-proj-1"
        }))
        .await;
    wait_until(Duration::from_secs(2), || async {
        !names_rx
            .borrow()
            .contains_key(&BbProjectId::parse("uuid-proj-1").unwrap())
    })
    .await;

    let _ = shutdown_tx.send(true);
    bridge_task.abort();
}

#[tokio::test]
async fn reconnect_after_ws_drop_mints_fresh_ticket_and_resumes_observations() {
    let tmp = tempfile::tempdir().unwrap();
    let tid = BbThreadId::parse("t3:reconn-v1:worker:a1").unwrap();
    let mut projs = HashMap::new();
    projs.insert(
        tid.to_string(),
        json!({
            "thread": { "id": tid.as_str(), "archivedAt": null },
            "runs": [{ "id": "run_1", "ordinal": 1, "status": "running" }],
            "providerTurns": [],
            "runtimeRequests": [],
            "turnItems": []
        }),
    );

    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_reconn".into(),
        thread_projections: projs,
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_reconn").unwrap();
    cfg.reconnect_min = Duration::from_millis(15);
    cfg.reconnect_max = Duration::from_millis(50);
    let bridge = T3Bridge::new(cfg);
    let router = build_test_router(tmp.path(), bridge.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));

    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let wave = WaveSlug::parse("reconn-v1").unwrap();
    let role = RoleName::parse("worker").unwrap();
    let project = BbProjectId::parse("uuid-p").unwrap();
    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project,
                goal: Body::parse("Verify WS reconnect").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("antigravity").unwrap(),
                    model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("run").unwrap(),
                    agent: None,
                }],
            },
            idem: None,
        })
        .await
        .unwrap();

    let sys = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::BbBridge,
        },
    );
    sys.call(rex_wave_api::wave::WaveObserve {
        wave: wave.clone(),
        role: role.clone(),
        observation: Observation::Spawned {
            attempt: 1,
            thread: tid.clone(),
        },
    })
    .await
    .unwrap();
    bridge.observe_wave_event(
        wave.clone(),
        WaveEvent::WorkerObserved {
            role: role.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: tid.clone(),
            },
        },
    );

    wait_until(Duration::from_secs(3), || async {
        server.state.lock().await.ws_connections >= 1
    })
    .await;
    let tickets_before = server.state.lock().await.tickets_issued.len();

    let mut live = bridge.live(&wave, &role).await.unwrap();
    wait_until(Duration::from_secs(2), || async {
        server
            .state
            .lock()
            .await
            .thread_sub_txs
            .get(tid.as_str())
            .is_some_and(|v| !v.is_empty())
    })
    .await;

    server.drop_all_ws_connections().await;

    wait_until(Duration::from_secs(3), || async {
        let st = server.state.lock().await;
        st.ws_connections >= 2
            && st.tickets_issued.len() > tickets_before
            && !st.shell_txs.is_empty()
    })
    .await;

    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 20,
            "location": "active",
            "thread": {
                "id": tid.as_str(),
                "status": "failed",
                "lastError": "upstream timeout after reconnect",
                "pendingRuntimeRequest": null,
                "archivedAt": null
            }
        }))
        .await;

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            snap.workers.iter().any(|(_, _, phase)| {
                matches!(
                    phase,
                    WorkerPhase::Live {
                        activity: Activity::Errored { error },
                        ..
                    } if error == "upstream timeout after reconnect"
                )
            })
        }
    })
    .await;

    server
        .push_thread_item(
            tid.as_str(),
            json!({
                "kind": "event",
                "sequence": 21,
                "event": {
                    "type": "turn-item.updated",
                    "payload": {
                        "id": "i99",
                        "ordinal": 99,
                        "type": "assistant_message",
                        "status": "completed",
                        "streaming": false,
                        "text": "resumed after reconnect",
                        "updatedAt": "2026-11-21T08:15:00Z"
                    }
                }
            }),
        )
        .await;

    let resumed = tokio::time::timeout(Duration::from_secs(2), live.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        resumed,
        Frame::Agent { event, .. } if matches!(event.kind, AgentEventKind::AssistantText { ref text } if text == "resumed after reconnect")
    ));

    let _ = shutdown_tx.send(true);
    bridge_task.abort();
}

#[derive(Clone, Default)]
struct RecordingBbHost {
    spawns: Arc<StdMutex<Vec<(String, String)>>>,
    tells: Arc<StdMutex<Vec<String>>>,
    resolves: Arc<StdMutex<Vec<String>>>,
    retires: Arc<StdMutex<Vec<String>>>,
}

impl AgentHost for RecordingBbHost {
    fn spawn(
        &self,
        req: SpawnRequest,
    ) -> impl Future<Output = Result<BbThreadId, HostError>> + Send {
        let spawns = self.spawns.clone();
        async move {
            let tid = format!("th_bb_{}_{}", req.wave, req.role);
            spawns
                .lock()
                .unwrap()
                .push((req.agent.provider.to_string(), tid.clone()));
            Ok(BbThreadId::parse(tid).unwrap())
        }
    }

    fn tell(
        &self,
        thread: &BbThreadId,
        _body: &Body,
        _mode: SteerDeliveryMode,
    ) -> impl Future<Output = Result<(), HostError>> + Send {
        let tells = self.tells.clone();
        let tid = thread.to_string();
        async move {
            tells.lock().unwrap().push(tid);
            Ok(())
        }
    }

    fn resolve(
        &self,
        thread: &BbThreadId,
        _interaction: &BbInteractionId,
        _resolution: &InteractionResolution,
    ) -> impl Future<Output = Result<(), HostError>> + Send {
        let resolves = self.resolves.clone();
        let tid = thread.to_string();
        async move {
            resolves.lock().unwrap().push(tid);
            Ok(())
        }
    }

    fn retire(&self, thread: &BbThreadId) -> impl Future<Output = Result<(), HostError>> + Send {
        let retires = self.retires.clone();
        let tid = thread.to_string();
        async move {
            retires.lock().unwrap().push(tid);
            Ok(())
        }
    }
}

struct MockBbFeed {
    names_tx: watch::Sender<Arc<BTreeMap<BbProjectId, String>>>,
    names_rx: watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>,
}

impl MockBbFeed {
    fn new() -> Self {
        let mut init = BTreeMap::new();
        init.insert(BbProjectId::parse("proj_bb_1").unwrap(), "bb-repo".into());
        let (names_tx, names_rx) = watch::channel(Arc::new(init));
        MockBbFeed { names_tx, names_rx }
    }
}

impl AgentFeed for MockBbFeed {
    fn page(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        _after: Option<BbSeq>,
        _limit: u32,
    ) -> BoxFut<'_, Result<ThreadPageView, ApiError>> {
        let tid = BbThreadId::parse(format!("th_bb_{wave}_{role}")).unwrap();
        Box::pin(async move {
            Ok(ThreadPageView {
                thread: tid,
                activity: Activity::Working,
                events: vec![],
                usage: None,
                pending: None,
                next: None,
            })
        })
    }

    fn live(
        &self,
        _wave: &WaveSlug,
        _role: &RoleName,
    ) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        Box::pin(async {
            let (_tx, stream) = frame_channel(8);
            Ok(stream)
        })
    }

    fn project_names(&self) -> Option<watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>> {
        Some(self.names_rx.clone())
    }
}

#[tokio::test]
async fn composite_host_and_feed_route_antigravity_to_t3_and_bb_prefix_to_bb() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_comp".into(),
        projects: vec![json!({
            "id": "uuid-mix",
            "title": "t3-mixed-repo",
            "workspaceRoot": "/tmp/mixed"
        })],
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_comp").unwrap();
    cfg.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg);
    let router = build_test_router(tmp.path(), bridge.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router, shutdown_rx));

    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    let bb_host = RecordingBbHost::default();
    let composite = CompositeHost::both(bb_host.clone(), T3Host(bridge.clone()), RunnerTarget::Bb);

    let wave = WaveSlug::parse("mix-v1").unwrap();
    let project = BbProjectId::parse("uuid-mix").unwrap();
    let mk_req = |role: &str, provider: &str, model: Option<&str>| SpawnRequest {
        wave: wave.clone(),
        role: RoleName::parse(role).unwrap(),
        attempt: 1,
        project: project.clone(),
        agent: AgentSpec {
            provider: ProviderId::parse(provider).unwrap(),
            model: model.map(|m| ModelId::parse(m).unwrap()),
            reasoning: None,
            service_tier: None,
            permission: PermissionMode::Full,
            workspace: Workspace::ProjectDefault,
        },
        room: wave_room(&wave, &project),
        prompt: Body::parse("work").unwrap(),
        title: format!("[mix-v1] @{role}"),
    };

    // Bare `antigravity` routes to T3Host even though default_runner is Bb.
    let ag_tid = composite
        .spawn(mk_req("gemini", "antigravity", None))
        .await
        .unwrap();
    assert_eq!(ag_tid.as_str(), "t3:mix-v1:gemini:a1");

    // Explicit `bb:claude-code` strips prefix and routes to BB.
    let bb_tid = composite
        .spawn(mk_req("claude", "bb:claude-code", Some("claude-opus-5-5")))
        .await
        .unwrap();
    assert_eq!(bb_tid.as_str(), "th_bb_mix-v1_claude");
    assert_eq!(
        bb_host.spawns.lock().unwrap().as_slice(),
        &[("claude-code".to_owned(), "th_bb_mix-v1_claude".to_owned())]
    );

    // Explicit `bb:antigravity` is structurally rejected.
    let blocked_bb_ag = composite
        .spawn(mk_req("bad", "bb:antigravity", Some("gemini-4-neon")))
        .await;
    assert!(matches!(blocked_bb_ag, Err(HostError::Rejected(_))));

    // BB-only CompositeHost never falls back bare `antigravity` onto BB.
    let bb_only: CompositeHost<RecordingBbHost, T3Host> =
        CompositeHost::new(Some(bb_host.clone()), None, RunnerTarget::Bb);
    assert!(matches!(
        bb_only
            .spawn(mk_req("bad2", "antigravity", Some("gemini-4-neon")))
            .await,
        Err(HostError::Rejected(_))
    ));

    // Thread-ID prefix routes tell/resolve/retire cleanly.
    composite
        .tell(
            &ag_tid,
            &Body::parse("t3 steer").unwrap(),
            SteerDeliveryMode::Auto,
        )
        .await
        .unwrap();
    composite
        .tell(
            &bb_tid,
            &Body::parse("bb steer").unwrap(),
            SteerDeliveryMode::Auto,
        )
        .await
        .unwrap();
    assert_eq!(
        bb_host.tells.lock().unwrap().as_slice(),
        &["th_bb_mix-v1_claude"]
    );
    assert!(
        server
            .state
            .lock()
            .await
            .dispatched_commands
            .iter()
            .any(|c| { c["type"] == "message.dispatch" && c["threadId"] == ag_tid.as_str() })
    );

    composite.retire(&bb_tid).await.unwrap();
    assert_eq!(
        bb_host.retires.lock().unwrap().as_slice(),
        &["th_bb_mix-v1_claude"]
    );

    let bb_feed = Arc::new(MockBbFeed::new());
    let t3_feed = Arc::new(T3Feed(bridge.clone()));
    let composite_feed = CompositeFeed::new(Some(bb_feed.clone()), Some(t3_feed.clone()));

    let t3_page = composite_feed
        .page(&wave, &RoleName::parse("gemini").unwrap(), None, 50)
        .await
        .unwrap();
    assert_eq!(t3_page.thread, ag_tid);

    let bb_page = composite_feed
        .page(&wave, &RoleName::parse("claude").unwrap(), None, 50)
        .await
        .unwrap();
    assert_eq!(bb_page.thread, bb_tid);

    let ag_clone = ag_tid.clone();
    let bb_clone = bb_tid.clone();
    let resolved_feed = CompositeFeed::new(Some(bb_feed.clone()), Some(t3_feed))
        .with_thread_resolver(Arc::new(move |_w, r| {
            let tid = if r.as_str() == "gemini" {
                Some(ag_clone.clone())
            } else {
                Some(bb_clone.clone())
            };
            Box::pin(async move { tid })
        }));
    assert_eq!(
        resolved_feed
            .page(&wave, &RoleName::parse("gemini").unwrap(), None, 50)
            .await
            .unwrap()
            .thread,
        ag_tid
    );
    assert_eq!(
        resolved_feed
            .page(&wave, &RoleName::parse("claude").unwrap(), None, 50)
            .await
            .unwrap()
            .thread,
        bb_tid
    );

    let merged_names = composite_feed.project_names().expect("merged watch");
    wait_until(Duration::from_secs(2), || async {
        let cur = merged_names.borrow();
        cur.get(&BbProjectId::parse("proj_bb_1").unwrap())
            .map(String::as_str)
            == Some("bb-repo")
            && cur
                .get(&BbProjectId::parse("uuid-mix").unwrap())
                .map(String::as_str)
                == Some("t3-mixed-repo")
    })
    .await;

    let mut next_bb = (**bb_feed.names_rx.borrow()).clone();
    next_bb.insert(BbProjectId::parse("proj_bb_2").unwrap(), "bb-two".into());
    let _ = bb_feed.names_tx.send(Arc::new(next_bb));
    wait_until(Duration::from_secs(2), || async {
        merged_names
            .borrow()
            .get(&BbProjectId::parse("proj_bb_2").unwrap())
            .map(String::as_str)
            == Some("bb-two")
    })
    .await;

    let _ = shutdown_tx.send(true);
    bridge_task.abort();
}

#[tokio::test]
async fn idle_mirror_skips_when_worker_already_posted_to_wave_room() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_nomir".into(),
        projects: vec![json!({
            "id": "uuid-nomir",
            "title": "nomir-repo",
            "workspaceRoot": "/tmp/nomir"
        })],
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_nomir").unwrap();
    cfg.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg);
    let router = build_test_router(tmp.path(), bridge.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    let conductor_task = spawn_reactor(
        WaveConductor::new(T3Host(bridge.clone())),
        LocalClient::new(
            router.clone(),
            Principal::System {
                part: SystemPart::WaveConductor,
            },
        ),
    );
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let wave = WaveSlug::parse("nomir-v1").unwrap();
    let role = RoleName::parse("writer").unwrap();
    let project = BbProjectId::parse("uuid-nomir").unwrap();
    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse("Avoid duplicate room posts").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("antigravity").unwrap(),
                    model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("Post your result directly").unwrap(),
                    agent: None,
                }],
            },
            idem: None,
        })
        .await
        .unwrap();

    let expected_tid = BbThreadId::parse("t3:nomir-v1:writer:a1").unwrap();
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let expected_tid = expected_tid.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            snap.workers.iter().any(|(_, _, phase)| {
                matches!(phase, WorkerPhase::Live { thread, .. } if thread == &expected_tid)
            })
        }
    })
    .await;

    let room = wave_room(&wave, &project);
    let worker_client = LocalClient::new(
        router.clone(),
        Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        },
    );
    worker_client
        .call(rex_wave_api::room::RoomPost {
            room: rex_wave_api::room::RoomRef::Id { id: room.clone() },
            body: Body::parse("Posted directly via rex-wave CLI").unwrap(),
            reply_to: None,
            idem: None,
        })
        .await
        .unwrap();

    {
        let mut st = server.state.lock().await;
        let proj = st
            .thread_projections
            .get_mut(expected_tid.as_str())
            .unwrap();
        proj["runs"][0]["status"] = json!("completed");
        proj["turnItems"].as_array_mut().unwrap().push(json!({
            "id": "item_done",
            "messageId": "msg_done",
            "runId": "run_1",
            "ordinal": 2,
            "type": "assistant_message",
            "status": "completed",
            "streaming": false,
            "text": "Internal assistant wrap-up that must not duplicate",
            "updatedAt": "2026-11-21T08:04:00Z"
        }));
    }
    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 30,
            "location": "active",
            "thread": {
                "id": expected_tid.as_str(),
                "status": "completed",
                "pendingRuntimeRequest": null,
                "archivedAt": null
            }
        }))
        .await;

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            snap.workers.iter().any(|(_, _, phase)| {
                matches!(
                    phase,
                    WorkerPhase::Live {
                        activity: Activity::Idle,
                        ..
                    }
                )
            })
        }
    })
    .await;

    let room_page = operator
        .call(Read {
            stream: StreamId::Room { room },
            after: Seq::ORIGIN,
            limit: Some(50),
        })
        .await
        .unwrap();
    let worker_posts: Vec<&Envelope> = room_page
        .entries
        .iter()
        .filter(|e| {
            e.by == Principal::Worker {
                wave: wave.clone(),
                role: role.clone(),
            } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
        })
        .collect();
    assert_eq!(
        worker_posts.len(),
        1,
        "must not mirror assistant_message after worker already posted"
    );

    let _ = shutdown_tx.send(true);
    conductor_task.abort();
    bridge_task.abort();
}
