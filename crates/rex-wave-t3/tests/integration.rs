#![allow(clippy::disallowed_methods)]
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
    launch_receipts: HashMap<String, String>,
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
                if proj.get("mockUnavailable").and_then(|v| v.as_bool()) == Some(true) {
                    write_http_json(
                        &mut stream,
                        503,
                        "Service Unavailable",
                        &json!({"error": "projection store busy"}),
                        false,
                    )
                    .await;
                } else {
                    write_http_json(
                        &mut stream,
                        200,
                        "OK",
                        &json!({"snapshotSequence": 10, "projection": proj}),
                        true,
                    )
                    .await;
                }
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
            let Some(command_id) = payload["commandId"].as_str().filter(|s| !s.is_empty()) else {
                send_err("launchThread missing commandId");
                return;
            };
            let Some(tid) = payload["threadId"].as_str().filter(|s| !s.is_empty()) else {
                send_err("launchThread missing threadId");
                return;
            };
            let tid = tid.to_owned();
            if payload["projectId"].as_str().is_none_or(str::is_empty)
                || payload["title"].as_str().is_none_or(str::is_empty)
                || payload["modelSelection"]["instanceId"]
                    .as_str()
                    .is_none_or(str::is_empty)
                || payload["modelSelection"]["model"]
                    .as_str()
                    .is_none_or(str::is_empty)
                || payload["runtimeMode"].as_str().is_none_or(str::is_empty)
                || payload["interactionMode"]
                    .as_str()
                    .is_none_or(str::is_empty)
                || !payload["workspaceStrategy"].is_object()
            {
                send_err("launchThread missing required v0.1.11 fields");
                return;
            }
            let mut st = state.lock().await;
            let has_receipt = st.launch_receipts.contains_key(command_id);
            let exists = st.thread_projections.contains_key(&tid);
            // Mirrors T3 v0.1.11 ThreadLaunchService.validateReusableThread.
            if payload["reuseExistingThread"].as_bool() == Some(true) && !has_receipt && !exists {
                send_err("ProjectionStoreThreadNotFoundError: cannot reuse nonexistent thread");
                return;
            }
            st.launched_threads.push(payload.clone());
            if has_receipt {
                send_ok(json!({
                    "threadId": tid,
                    "resumed": true,
                    "sequence": 1
                }));
                return;
            }
            st.launch_receipts
                .insert(command_id.to_owned(), tid.clone());
            if !exists {
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
                "resumed": false,
                "sequence": 1
            }));
        }
        "orchestration.dispatchCommand" => {
            let cmd_type = payload["type"].as_str().unwrap_or("");
            if payload["commandId"].as_str().is_none_or(str::is_empty)
                || payload["threadId"].as_str().is_none_or(str::is_empty)
            {
                send_err("dispatchCommand missing commandId or threadId");
                return;
            }
            if cmd_type == "message.dispatch" {
                let mode_type = payload["dispatchMode"]["type"].as_str();
                let valid_mode = matches!(
                    mode_type,
                    Some(
                        "queue_after_active"
                            | "start_immediately"
                            | "steer_active"
                            | "restart_active"
                    )
                );
                if !valid_mode
                    || payload["messageId"].as_str().is_none_or(str::is_empty)
                    || payload["createdBy"].as_str().is_none_or(str::is_empty)
                    || payload["creationSource"].as_str().is_none_or(str::is_empty)
                    || !payload["attachments"].is_array()
                {
                    send_err(
                        "message.dispatch missing required T3 v0.1.11 dispatchMode or envelope fields",
                    );
                    return;
                }
            } else if cmd_type == "runtime-request.respond"
                && payload["requestId"].as_str().is_none_or(str::is_empty)
            {
                send_err("runtime-request.respond missing requestId");
                return;
            }
            let mut st = state.lock().await;
            st.dispatched_commands.push(payload.clone());
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
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents,
        }))
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
                    account: None,
                    reasoning: Some("xhigh".into()),
                    service_tier: None,
                    permission: PermissionMode::AcceptEdits,
                    workspace: Workspace::ManagedWorktree {
                        base: BaseBranch::Named {
                            name: "main".into(),
                        },
                    },
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("Audit T3 wire integration").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
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
        assert!(launch.get("reuseExistingThread").is_none());
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
                account: None,
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
            run_stamp: "auth-v2:gemini-rev:attempt-1:seq-1".into(),
            output_dir: None,
            writes: Vec::new(),
            return_schema: None,
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
                && c["dispatchMode"]["type"] == "start_immediately"
                && c["deliveryIntent"] == "steer"
        });
        let has_queue = st.dispatched_commands.iter().any(|c| {
            c["type"] == "message.dispatch"
                && c["text"] == "follow up after run"
                && c["dispatchMode"]["type"] == "queue_after_active"
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
        .call(Read::stream(
            StreamId::Room { room: room.clone() },
            Seq::ORIGIN,
            Some(100),
        ))
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
    assert_eq!(page1.next, Some(rex_wave_t3::item_event_seq(4, true)));
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
    let mut last_seq = BbSeq(0);
    let mut got_snapshot_items = 0usize;
    while got_snapshot_items < 8 {
        let frame = tokio::time::timeout(Duration::from_secs(2), live.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Frame::Agent { event, .. } = frame else {
            panic!("expected Frame::Agent");
        };
        assert!(event.seq > last_seq);
        last_seq = event.seq;
        got_snapshot_items += 1;
    }

    // Command ToolCall -> ToolResult on the same ordinal must emit distinct monotonic seqs,
    // streaming assistant updates must be skipped until finalized, and Usage/TurnEnded must not collide.
    server
        .push_thread_item(
            tid.as_str(),
            json!({
                "kind": "event",
                "sequence": 11,
                "event": {
                    "type": "turn-item.updated",
                    "payload": {
                        "id": "i8_cmd",
                        "ordinal": 8,
                        "type": "command_execution",
                        "status": "inProgress",
                        "input": "git diff",
                        "startedAt": "2026-11-21T08:10:50Z"
                    }
                }
            }),
        )
        .await;
    server
        .push_thread_item(
            tid.as_str(),
            json!({
                "kind": "event",
                "sequence": 12,
                "event": {
                    "type": "turn-item.updated",
                    "payload": {
                        "id": "i8_cmd",
                        "ordinal": 8,
                        "type": "command_execution",
                        "status": "completed",
                        "input": "git diff",
                        "exitCode": 0,
                        "completedAt": "2026-11-21T08:10:52Z"
                    }
                }
            }),
        )
        .await;
    server
        .push_thread_item(
            tid.as_str(),
            json!({
                "kind": "event",
                "sequence": 13,
                "event": {
                    "type": "turn-item.updated",
                    "payload": {
                        "id": "i9_msg",
                        "ordinal": 9,
                        "type": "assistant_message",
                        "status": "inProgress",
                        "streaming": true,
                        "text": "Live chunk...",
                        "updatedAt": "2026-11-21T08:10:58Z"
                    }
                }
            }),
        )
        .await;
    server
        .push_thread_item(
            tid.as_str(),
            json!({
                "kind": "event",
                "sequence": 14,
                "event": {
                    "type": "turn-item.updated",
                    "payload": {
                        "id": "i9_msg",
                        "ordinal": 9,
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
    server
        .push_thread_item(
            tid.as_str(),
            json!({
                "kind": "event",
                "sequence": 15,
                "event": {
                    "type": "provider-turn.updated",
                    "payload": {
                        "tokenUsage": { "usedTokens": 68000, "maxTokens": 200000 }
                    }
                }
            }),
        )
        .await;
    server
        .push_thread_item(
            tid.as_str(),
            json!({
                "kind": "event",
                "sequence": 16,
                "event": {
                    "type": "run.updated",
                    "payload": { "status": "completed" }
                }
            }),
        )
        .await;

    let mut live_events = Vec::new();
    for _ in 0..5 {
        let frame = tokio::time::timeout(Duration::from_secs(2), live.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Frame::Agent { event, .. } = frame else {
            panic!("expected Frame::Agent");
        };
        assert!(event.seq > last_seq, "live seq must advance monotonically");
        last_seq = event.seq;
        live_events.push(event);
    }
    assert!(matches!(
        live_events[0].kind,
        AgentEventKind::ToolCall { .. }
    ));
    assert!(matches!(
        live_events[1].kind,
        AgentEventKind::ToolResult { ok: true, .. }
    ));
    assert!(matches!(
        live_events[2].kind,
        AgentEventKind::AssistantText { ref text } if text == "Live chunk arrived"
    ));
    assert!(matches!(live_events[3].kind, AgentEventKind::Usage { .. }));
    assert!(matches!(live_events[4].kind, AgentEventKind::TurnEnded));

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
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("run").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
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
    fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<BbThreadId, HostError>> {
        let spawns = self.spawns.clone();
        Box::pin(async move {
            let tid = format!("th_bb_{}_{}", req.wave, req.role);
            spawns
                .lock()
                .unwrap()
                .push((req.agent.provider.to_string(), tid.clone()));
            Ok(BbThreadId::parse(tid).unwrap())
        })
    }

    fn tell(
        &self,
        thread: &BbThreadId,
        _body: &Body,
        _mode: SteerDeliveryMode,
    ) -> BoxFut<'_, Result<(), HostError>> {
        let tells = self.tells.clone();
        let tid = thread.to_string();
        Box::pin(async move {
            tells.lock().unwrap().push(tid);
            Ok(())
        })
    }

    fn resolve(
        &self,
        thread: &BbThreadId,
        _interaction: &BbInteractionId,
        _resolution: &InteractionResolution,
    ) -> BoxFut<'_, Result<(), HostError>> {
        let resolves = self.resolves.clone();
        let tid = thread.to_string();
        Box::pin(async move {
            resolves.lock().unwrap().push(tid);
            Ok(())
        })
    }

    fn retire(&self, thread: &BbThreadId) -> BoxFut<'_, Result<(), HostError>> {
        let retires = self.retires.clone();
        let tid = thread.to_string();
        Box::pin(async move {
            retires.lock().unwrap().push(tid);
            Ok(())
        })
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
            account: None,
            reasoning: None,
            service_tier: None,
            permission: PermissionMode::Full,
            workspace: Workspace::ProjectDefault,
        },
        room: wave_room(&wave, &project),
        prompt: Body::parse("work").unwrap(),
        title: format!("[mix-v1] @{role}"),
        run_stamp: format!("mix-v1:{role}:attempt-1:seq-1"),
        output_dir: None,
        writes: Vec::new(),
        return_schema: None,
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
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("Post your result directly").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
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
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
            origin: Default::default(),
        })
        .await
        .unwrap();

    {
        let mut st = server.state.lock().await;
        let proj = st
            .thread_projections
            .get_mut(expected_tid.as_str())
            .unwrap();
        proj["runs"][0]["completedAt"] = json!(rex_wave_runtime::now().0);
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

    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 31,
            "location": "active",
            "thread": {
                "id": expected_tid.as_str(),
                "status": "completed",
                "pendingRuntimeRequest": null,
                "archivedAt": null
            }
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    let room_page = operator
        .call(Read::stream(
            StreamId::Room { room: room.clone() },
            Seq::ORIGIN,
            Some(50),
        ))
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
        "must not mirror assistant_message after worker already posted even across repeated thread.updated"
    );

    let run2_start = rex_wave_runtime::now().0.saturating_add(5);
    {
        let mut st = server.state.lock().await;
        let proj = st
            .thread_projections
            .get_mut(expected_tid.as_str())
            .unwrap();
        proj["runs"].as_array_mut().unwrap().push(json!({
            "id": "run_2",
            "ordinal": 2,
            "status": "completed",
            "startedAt": run2_start,
            "completedAt": run2_start + 10
        }));
        proj["turnItems"].as_array_mut().unwrap().push(json!({
            "id": "item_run2",
            "messageId": "msg_run2",
            "runId": "run_2",
            "ordinal": 3,
            "type": "assistant_message",
            "status": "completed",
            "streaming": false,
            "text": "Second run unposted reply",
            "updatedAt": "2026-11-21T08:05:00Z"
        }));
    }
    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 32,
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
        let room = room.clone();
        let wave = wave.clone();
        let role = role.clone();
        async move {
            let Ok(page) = operator
                .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
                .await
            else {
                return false;
            };
            page.entries
                .iter()
                .filter(|e| {
                    e.by == Principal::Worker {
                        wave: wave.clone(),
                        role: role.clone(),
                    } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                })
                .count()
                == 2
        }
    })
    .await;

    let _ = shutdown_tx.send(true);
    conductor_task.abort();
    bridge_task.abort();
}

#[tokio::test]
async fn item_only_projection_attributes_direct_post_to_matching_turn_only() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_item_only".into(),
        projects: vec![json!({
            "id": "uuid-item-only",
            "title": "item-only-repo",
            "workspaceRoot": "/tmp/item-only"
        })],
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_item_only").unwrap();
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
    let wave = WaveSlug::parse("item-only-v1").unwrap();
    let role = RoleName::parse("writer").unwrap();
    let upgrade_role = RoleName::parse("upgrader").unwrap();
    let refused_role = RoleName::parse("refuser").unwrap();
    let lagged_role = RoleName::parse("lagged").unwrap();
    let project = BbProjectId::parse("uuid-item-only").unwrap();
    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse("Attribute direct post to first item-only turn only").unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("antigravity").unwrap(),
                    model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![
                    WorkerSpec {
                        role: role.clone(),
                        brief: Body::parse("Post in turn 1, reply directly in turn 2").unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: upgrade_role.clone(),
                        brief: Body::parse("Preserve turn 1 across pre-upgrade turn 2 mirror")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: refused_role.clone(),
                        brief: Body::parse("Suppress refused draft across bridge restart").unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: lagged_role.clone(),
                        brief: Body::parse("Suppress refused draft on lagged notification")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                ],
            },
            idem: None,
        })
        .await
        .unwrap();

    let expected_tid = BbThreadId::parse("t3:item-only-v1:writer:a1").unwrap();
    let upgrade_tid = BbThreadId::parse("t3:item-only-v1:upgrader:a1").unwrap();
    let refused_tid = BbThreadId::parse("t3:item-only-v1:refuser:a1").unwrap();
    let lagged_tid = BbThreadId::parse("t3:item-only-v1:lagged:a1").unwrap();
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let expected_tid = expected_tid.clone();
        let upgrade_tid = upgrade_tid.clone();
        let refused_tid = refused_tid.clone();
        let lagged_tid = lagged_tid.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            let w1 = snap.workers.iter().any(|(_, _, phase)| {
                matches!(phase, WorkerPhase::Live { thread, .. } if thread == &expected_tid)
            });
            let w2 = snap.workers.iter().any(|(_, _, phase)| {
                matches!(phase, WorkerPhase::Live { thread, .. } if thread == &upgrade_tid)
            });
            let w3 = snap.workers.iter().any(|(_, _, phase)| {
                matches!(phase, WorkerPhase::Live { thread, .. } if thread == &refused_tid)
            });
            let w4 = snap.workers.iter().any(|(_, _, phase)| {
                matches!(phase, WorkerPhase::Live { thread, .. } if thread == &lagged_tid)
            });
            w1 && w2 && w3 && w4
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
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
            origin: Default::default(),
        })
        .await
        .unwrap();

    let upgrade_client = LocalClient::new(
        router.clone(),
        Principal::Worker {
            wave: wave.clone(),
            role: upgrade_role.clone(),
        },
    );
    let legacy_t2_key =
        rex_wave_waves::mirror_idem_key(&wave, &upgrade_role, &upgrade_tid, "final2").unwrap();
    upgrade_client
        .call(rex_wave_api::room::RoomPost {
            room: rex_wave_api::room::RoomRef::Id { id: room.clone() },
            body: Body::parse("Pre-upgrade turn 2").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: Some(legacy_t2_key),
            origin: Default::default(),
        })
        .await
        .unwrap();

    {
        let mut st = server.state.lock().await;
        let r_proj = st.thread_projections.get_mut(refused_tid.as_str()).unwrap();
        r_proj["runs"] = json!([]);
        r_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "refused_draft", "text": "Refused draft turn 1" }
        ]);
        r_proj["thread"]["status"] = json!("completed");
        r_proj["thread"]["stopReason"] = json!("safety");

        let l_proj = st.thread_projections.get_mut(lagged_tid.as_str()).unwrap();
        l_proj["runs"] = json!([]);
        l_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "lagged_refused_draft", "text": "Lagged refused draft turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "lagged_recovered_final", "text": "Lagged recovered reply turn 2" }
        ]);
        l_proj["thread"]["status"] = json!("completed");
        l_proj["thread"]["stopReason"] = Value::Null;
    }
    for (seq, tid) in [(28u64, refused_tid.as_str()), (29u64, lagged_tid.as_str())] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": "safety"
                }
            }))
            .await;
    }
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let refused_role = refused_role.clone();
        let lagged_role = lagged_role.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            let r_ok = snap.workers.iter().any(|(r, _, phase)| {
                r == &refused_role
                    && matches!(
                        phase,
                        WorkerPhase::Live {
                            activity: Activity::Refused { .. },
                            ..
                        }
                    )
            });
            let l_ok = snap.workers.iter().any(|(r, _, phase)| {
                r == &lagged_role
                    && matches!(
                        phase,
                        WorkerPhase::Live {
                            activity: Activity::Refused { .. },
                            ..
                        }
                    )
            });
            r_ok && l_ok
        }
    })
    .await;

    {
        let mut st = server.state.lock().await;
        let base = rex_wave_runtime::now().0;
        let proj = st
            .thread_projections
            .get_mut(expected_tid.as_str())
            .unwrap();
        proj["runs"] = json!([]);
        proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message" },
            { "ordinal": 1, "type": "assistant_message", "id": "final1", "text": "First wrap-up", "createdAt": base + 5 },
            { "ordinal": 2, "type": "user_message", "createdAt": base + 10 },
            { "ordinal": 3, "type": "assistant_message", "id": "final2", "text": "Second reply", "createdAt": base + 20 }
        ]);
        let u_proj = st.thread_projections.get_mut(upgrade_tid.as_str()).unwrap();
        u_proj["runs"] = json!([]);
        u_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "final1", "text": "Turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "final2", "text": "Pre-upgrade turn 2" },
            { "ordinal": 4, "type": "user_message", "text": "u3" },
            { "ordinal": 5, "type": "assistant_message", "id": "final3", "text": "Turn 3" }
        ]);
        let r_proj = st.thread_projections.get_mut(refused_tid.as_str()).unwrap();
        r_proj["runs"] = json!([]);
        r_proj["thread"]["stopReason"] = Value::Null;
        r_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "refused_draft", "text": "Refused draft turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "recovered_final", "text": "Recovered reply turn 2" }
        ]);
    }

    let _ = shutdown_tx.send(true);
    let _ = bridge_task.await;
    let mut cfg2 = T3Config::explicit(&server.origin(), "tok_item_only").unwrap();
    cfg2.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg2);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    for (seq, tid) in [
        (30u64, expected_tid.as_str()),
        (31u64, expected_tid.as_str()),
        (32u64, upgrade_tid.as_str()),
        (33u64, upgrade_tid.as_str()),
        (34u64, refused_tid.as_str()),
        (35u64, refused_tid.as_str()),
        (36u64, lagged_tid.as_str()),
        (37u64, lagged_tid.as_str()),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "pendingRuntimeRequest": null,
                    "archivedAt": null
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let room = room.clone();
        let wave = wave.clone();
        let role = role.clone();
        let upgrade_role = upgrade_role.clone();
        let refused_role = refused_role.clone();
        let lagged_role = lagged_role.clone();
        async move {
            let Ok(page) = operator
                .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
                .await
            else {
                return false;
            };
            let c1 = page
                .entries
                .iter()
                .filter(|e| {
                    e.by == Principal::Worker {
                        wave: wave.clone(),
                        role: role.clone(),
                    } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                })
                .count();
            let c2 = page
                .entries
                .iter()
                .filter(|e| {
                    e.by == Principal::Worker {
                        wave: wave.clone(),
                        role: upgrade_role.clone(),
                    } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                })
                .count();
            let c3 = page
                .entries
                .iter()
                .filter(|e| {
                    e.by == Principal::Worker {
                        wave: wave.clone(),
                        role: refused_role.clone(),
                    } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                })
                .count();
            let c4 = page
                .entries
                .iter()
                .filter(|e| {
                    e.by == Principal::Worker {
                        wave: wave.clone(),
                        role: lagged_role.clone(),
                    } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                })
                .count();
            c1 == 2 && c2 == 3 && c3 == 1 && c4 == 1
        }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let room_page = operator
        .call(Read::stream(
            StreamId::Room { room: room.clone() },
            Seq::ORIGIN,
            Some(50),
        ))
        .await
        .unwrap();
    let bodies: Vec<&str> = room_page
        .entries
        .iter()
        .filter(|e| {
            e.by == Principal::Worker {
                wave: wave.clone(),
                role: role.clone(),
            }
        })
        .filter_map(|e| match &e.event {
            Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        bodies,
        vec!["Posted directly via rex-wave CLI", "Second reply"]
    );
    let upgrade_bodies: Vec<&str> = room_page
        .entries
        .iter()
        .filter(|e| {
            e.by == Principal::Worker {
                wave: wave.clone(),
                role: upgrade_role.clone(),
            }
        })
        .filter_map(|e| match &e.event {
            Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        upgrade_bodies,
        vec!["Pre-upgrade turn 2", "Turn 1", "Turn 3"]
    );
    let refused_bodies: Vec<&str> = room_page
        .entries
        .iter()
        .filter(|e| {
            e.by == Principal::Worker {
                wave: wave.clone(),
                role: refused_role.clone(),
            }
        })
        .filter_map(|e| match &e.event {
            Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(refused_bodies, vec!["Recovered reply turn 2"]);
    let lagged_bodies: Vec<&str> = room_page
        .entries
        .iter()
        .filter(|e| {
            e.by == Principal::Worker {
                wave: wave.clone(),
                role: lagged_role.clone(),
            }
        })
        .filter_map(|e| match &e.event {
            Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(lagged_bodies, vec!["Lagged recovered reply turn 2"]);

    let _ = shutdown_tx.send(true);
    conductor_task.abort();
    bridge_task.abort();
}

#[tokio::test]
async fn t3_bridge_respects_bb_default_runner_on_requested_and_ignores_retiring_working_updates() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_iso".into(),
        projects: vec![json!({
            "id": "uuid-proj-iso",
            "title": "rex-wave",
            "workspaceRoot": "/tmp/rex-wave"
        })],
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_iso")
        .unwrap()
        .with_default_runner(rex_wave_api::RunnerKind::Bb);
    cfg.reconnect_min = Duration::from_millis(15);
    cfg.reconnect_max = Duration::from_millis(60);
    let bridge = T3Bridge::new(cfg);
    let router = build_test_router(tmp.path(), bridge.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let sys = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    let wave = WaveSlug::parse("dual-iso").unwrap();
    let project = BbProjectId::parse("uuid-proj-iso").unwrap();
    let bb_role = RoleName::parse("bbworker").unwrap();
    let t3_role = RoleName::parse("t3worker").unwrap();

    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse("test dual runner isolation").unwrap(),
                output_dir: None,
                defaults: AgentSpec {
                    provider: ProviderId::parse("pi").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Full,
                    workspace: Workspace::ProjectDefault,
                },
                workers: vec![
                    WorkerSpec {
                        role: bb_role.clone(),
                        agent: None,
                        brief: Body::parse("bb work").unwrap(),
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: t3_role.clone(),
                        agent: Some(AgentSpec {
                            provider: ProviderId::parse("antigravity").unwrap(),
                            model: None,
                            account: None,
                            reasoning: None,
                            service_tier: None,
                            permission: PermissionMode::Full,
                            workspace: Workspace::ProjectDefault,
                        }),
                        brief: Body::parse("t3 work").unwrap(),
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                ],
            },
            idem: None,
        })
        .await
        .unwrap();

    // 1. Bare `pi` worker is Requested with default_runner=Bb: a rogue `t3:dual-iso:bbworker:a1`
    //    shell update must NOT claim it.
    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 1,
            "location": "active",
            "thread": {
                "id": "t3:dual-iso:bbworker:a1",
                "status": "running",
                "pendingRuntimeRequest": null,
                "archivedAt": null
            }
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let snap1 = operator
        .call(WaveView { wave: wave.clone() })
        .await
        .unwrap();
    let bb_phase = &snap1
        .workers
        .iter()
        .find(|(r, _, _)| r == &bb_role)
        .unwrap()
        .2;
    assert!(
        matches!(bb_phase, WorkerPhase::Requested { attempt: 1 }),
        "T3Bridge must not claim a Requested worker routed to BB under --runner both: got {bb_phase:?}"
    );

    // 2. Spawn `t3worker`, retire it into `Retiring`, push a `running` shell update, and verify it
    //    remains `Retiring` until an `archivedAt` update arrives.
    let t3_tid = BbThreadId::parse("t3:dual-iso:t3worker:a1").unwrap();
    sys.call(rex_wave_api::wave::WaveObserve {
        wave: wave.clone(),
        role: t3_role.clone(),
        observation: Observation::Spawned {
            attempt: 1,
            thread: t3_tid.clone(),
        },
    })
    .await
    .unwrap();
    operator
        .call(WaveRetireWorker {
            wave: wave.clone(),
            role: t3_role.clone(),
            reason: Some("done".into()),
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 2,
            "location": "active",
            "thread": {
                "id": t3_tid.as_str(),
                "status": "running",
                "pendingRuntimeRequest": null,
                "archivedAt": null
            }
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let snap2 = operator
        .call(WaveView { wave: wave.clone() })
        .await
        .unwrap();
    let t3_phase_mid = &snap2
        .workers
        .iter()
        .find(|(r, _, _)| r == &t3_role)
        .unwrap()
        .2;
    assert!(
        matches!(t3_phase_mid, WorkerPhase::Retiring { .. }),
        "retiring T3 worker must stay Retiring on non-archive shell update"
    );

    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 3,
            "location": "archived",
            "thread": {
                "id": t3_tid.as_str(),
                "status": "completed",
                "pendingRuntimeRequest": null,
                "archivedAt": "2026-11-24T12:00:00Z"
            }
        }))
        .await;
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let t3_role = t3_role.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            snap.workers
                .iter()
                .any(|(r, _, p)| r == &t3_role && matches!(p, WorkerPhase::Retired { .. }))
        }
    })
    .await;

    let _ = shutdown_tx.send(true);
    bridge_task.abort();
}

#[tokio::test]
async fn t3_explicit_shell_item_count_and_projection_outage_suppress_refused_drafts_across_restart()
{
    let tmp = tempfile::tempdir().unwrap();
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_explicit_ic".into(),
        projects: vec![json!({
            "id": "uuid-explicit-ic",
            "title": "rex-wave",
            "workspaceRoot": "/tmp/rex-wave"
        })],
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_explicit_ic").unwrap();
    cfg.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg);
    let router = build_test_router(tmp.path(), bridge.clone());
    let host_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    let conductor_task = spawn_reactor(WaveConductor::new(T3Host(bridge.clone())), host_client);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let wave = WaveSlug::parse("explicit-ic-v1").unwrap();
    let project = BbProjectId::parse("uuid-explicit-ic").unwrap();
    let outage_role = RoleName::parse("outage").unwrap();
    let terminal_ic_role = RoleName::parse("termic").unwrap();
    let double_fail_role = RoleName::parse("dblfail").unwrap();

    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse("Verify explicit shell itemCount and HTTP 503 outage replay")
                    .unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("antigravity").unwrap(),
                    model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![
                    WorkerSpec {
                        role: outage_role.clone(),
                        brief: Body::parse("Refused during HTTP 503 outage").unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: terminal_ic_role.clone(),
                        brief: Body::parse(
                            "Refused current turn with itemCount while HTTP running",
                        )
                        .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: double_fail_role.clone(),
                        brief: Body::parse("Two consecutive refused turns with lagged itemCount")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                ],
            },
            idem: None,
        })
        .await
        .unwrap();

    let outage_tid = BbThreadId::parse("t3:explicit-ic-v1:outage:a1").unwrap();
    let terminal_ic_tid = BbThreadId::parse("t3:explicit-ic-v1:termic:a1").unwrap();
    let double_fail_tid = BbThreadId::parse("t3:explicit-ic-v1:dblfail:a1").unwrap();
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let outage_tid = outage_tid.clone();
        let terminal_ic_tid = terminal_ic_tid.clone();
        let double_fail_tid = double_fail_tid.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            let ok1 = snap.workers.iter().any(
                |(_, _, p)| matches!(p, WorkerPhase::Live { thread, .. } if thread == &outage_tid),
            );
            let ok2 = snap.workers.iter().any(|(_, _, p)| {
                matches!(p, WorkerPhase::Live { thread, .. } if thread == &terminal_ic_tid)
            });
            let ok3 = snap.workers.iter().any(|(_, _, p)| {
                matches!(p, WorkerPhase::Live { thread, .. } if thread == &double_fail_tid)
            });
            ok1 && ok2 && ok3
        }
    })
    .await;

    {
        let mut st = server.state.lock().await;
        let o_proj = st.thread_projections.get_mut(outage_tid.as_str()).unwrap();
        o_proj["runs"] = json!([]);
        o_proj["mockUnavailable"] = json!(true);
        o_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "outage_refused_draft", "text": "Refused draft turn 1" }
        ]);

        let t_proj = st
            .thread_projections
            .get_mut(terminal_ic_tid.as_str())
            .unwrap();
        t_proj["runs"] = json!([]);
        t_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "good_turn1", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" }
        ]);
        t_proj["thread"]["status"] = json!("running");
        t_proj["thread"]["stopReason"] = Value::Null;

        let d_proj = st
            .thread_projections
            .get_mut(double_fail_tid.as_str())
            .unwrap();
        d_proj["runs"] = json!([]);
        d_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "lagged_refused_t1", "text": "Lagged refused draft turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "refused_t2", "text": "Failed draft turn 2" }
        ]);
        d_proj["thread"]["status"] = json!("completed");
        d_proj["thread"]["stopReason"] = json!("safety");
    }

    for (seq, tid, ic) in [
        (10u64, outage_tid.as_str(), 2u64),
        (11u64, terminal_ic_tid.as_str(), 4u64),
        (12u64, double_fail_tid.as_str(), 2u64),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": "safety",
                    "itemCount": ic
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let outage_role = outage_role.clone();
        let terminal_ic_role = terminal_ic_role.clone();
        let double_fail_role = double_fail_role.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&outage_role, &terminal_ic_role, &double_fail_role]
                .into_iter()
                .all(|want| {
                    snap.workers.iter().any(|(r, _, p)| {
                        r == want
                            && matches!(
                                p,
                                WorkerPhase::Live {
                                    activity: Activity::Refused { .. },
                                    ..
                                }
                            )
                    })
                })
        }
    })
    .await;

    {
        let mut st = server.state.lock().await;
        let o_proj = st.thread_projections.get_mut(outage_tid.as_str()).unwrap();
        o_proj["mockUnavailable"] = json!(false);
        o_proj["thread"]["status"] = json!("completed");
        o_proj["thread"]["stopReason"] = Value::Null;
        o_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "outage_refused_draft", "text": "Refused draft turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "outage_recovered_final", "text": "Recovered reply turn 2" }
        ]);

        let t_proj = st
            .thread_projections
            .get_mut(terminal_ic_tid.as_str())
            .unwrap();
        t_proj["thread"]["status"] = json!("completed");
        t_proj["thread"]["stopReason"] = Value::Null;
        t_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "good_turn1", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" },
            { "ordinal": 4, "type": "user_message", "text": "u3" },
            { "ordinal": 5, "type": "assistant_message", "id": "good_turn3", "text": "Third recovered" }
        ]);

        let d_proj = st
            .thread_projections
            .get_mut(double_fail_tid.as_str())
            .unwrap();
        d_proj["thread"]["status"] = json!("completed");
        d_proj["thread"]["stopReason"] = Value::Null;
        d_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "lagged_refused_t1", "text": "Lagged refused draft turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "refused_t2", "text": "Failed draft turn 2" },
            { "ordinal": 4, "type": "user_message", "text": "u3" },
            { "ordinal": 5, "type": "assistant_message", "id": "recovered_t3", "text": "Third recovered" }
        ]);
    }

    let _ = shutdown_tx.send(true);
    let _ = bridge_task.await;
    let mut cfg2 = T3Config::explicit(&server.origin(), "tok_explicit_ic").unwrap();
    cfg2.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg2);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    for (seq, tid) in [
        (20u64, outage_tid.as_str()),
        (21u64, terminal_ic_tid.as_str()),
        (22u64, double_fail_tid.as_str()),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "pendingRuntimeRequest": null,
                    "archivedAt": null
                }
            }))
            .await;
    }

    let room = wave_room(&wave, &project);
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let room = room.clone();
        let wave = wave.clone();
        let outage_role = outage_role.clone();
        let terminal_ic_role = terminal_ic_role.clone();
        let double_fail_role = double_fail_role.clone();
        async move {
            let Ok(page) = operator
                .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
                .await
            else {
                return false;
            };
            let count_for = |r: &RoleName| {
                page.entries
                    .iter()
                    .filter(|e| {
                        e.by == Principal::Worker {
                            wave: wave.clone(),
                            role: r.clone(),
                        } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                    })
                    .count()
            };
            count_for(&outage_role) == 1
                && count_for(&terminal_ic_role) == 2
                && count_for(&double_fail_role) == 1
        }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let page = operator
        .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
        .await
        .unwrap();
    let bodies_for = |r: &RoleName| -> Vec<&str> {
        page.entries
            .iter()
            .filter(|e| {
                e.by == Principal::Worker {
                    wave: wave.clone(),
                    role: r.clone(),
                }
            })
            .filter_map(|e| match &e.event {
                Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(bodies_for(&outage_role), vec!["Recovered reply turn 2"]);
    assert_eq!(
        bodies_for(&terminal_ic_role),
        vec!["Successful turn 1", "Third recovered"]
    );
    assert_eq!(bodies_for(&double_fail_role), vec!["Third recovered"]);

    let _ = shutdown_tx.send(true);
    conductor_task.abort();
    bridge_task.abort();
}

#[tokio::test]
async fn t3_run_id_item_only_ahead_item_count_and_unattributed_outage_preserve_good_turns() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_r17".into(),
        projects: vec![json!({
            "id": "uuid-r17",
            "title": "rex-wave",
            "workspaceRoot": "/tmp/rex-wave"
        })],
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_r17").unwrap();
    cfg.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg);
    let router = build_test_router(tmp.path(), bridge.clone());
    let host_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    let conductor_task = spawn_reactor(WaveConductor::new(T3Host(bridge.clone())), host_client);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let wave = WaveSlug::parse("r17-edge-v1").unwrap();
    let project = BbProjectId::parse("uuid-r17").unwrap();
    let run_outage_role = RoleName::parse("runoutage").unwrap();
    let ahead_ic_role = RoleName::parse("aheadic").unwrap();
    let unattr_runs_role = RoleName::parse("unattrruns").unwrap();

    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse(
                    "Verify runId item-only outage, ahead itemCount, and unattributed runs outage",
                )
                .unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("antigravity").unwrap(),
                    model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![
                    WorkerSpec {
                        role: run_outage_role.clone(),
                        brief: Body::parse("Outage with latestRunId replayed as item-only")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: ahead_ic_role.clone(),
                        brief: Body::parse("Refused with itemCount ahead of trailing error item")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: unattr_runs_role.clone(),
                        brief: Body::parse(
                            "Unattributed outage followed by completed/failed/completed runs",
                        )
                        .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                ],
            },
            idem: None,
        })
        .await
        .unwrap();

    let run_outage_tid = BbThreadId::parse("t3:r17-edge-v1:runoutage:a1").unwrap();
    let ahead_ic_tid = BbThreadId::parse("t3:r17-edge-v1:aheadic:a1").unwrap();
    let unattr_runs_tid = BbThreadId::parse("t3:r17-edge-v1:unattrruns:a1").unwrap();
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let run_outage_tid = run_outage_tid.clone();
        let ahead_ic_tid = ahead_ic_tid.clone();
        let unattr_runs_tid = unattr_runs_tid.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&run_outage_tid, &ahead_ic_tid, &unattr_runs_tid]
                .into_iter()
                .all(|want_tid| {
                    snap.workers.iter().any(|(_, _, p)| {
                        matches!(p, WorkerPhase::Live { thread, .. } if thread == want_tid)
                    })
                })
        }
    })
    .await;

    {
        let mut st = server.state.lock().await;
        let o_proj = st
            .thread_projections
            .get_mut(run_outage_tid.as_str())
            .unwrap();
        o_proj["runs"] = json!([]);
        o_proj["mockUnavailable"] = json!(true);
        o_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "runId": "r-outage", "id": "outage_refused_draft", "text": "Refused draft turn 1" }
        ]);

        let a_proj = st
            .thread_projections
            .get_mut(ahead_ic_tid.as_str())
            .unwrap();
        a_proj["runs"] = json!([]);
        a_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "good_turn1", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" }
        ]);
        a_proj["thread"]["status"] = json!("completed");
        a_proj["thread"]["stopReason"] = Value::Null;

        let u_proj = st
            .thread_projections
            .get_mut(unattr_runs_tid.as_str())
            .unwrap();
        u_proj["runs"] = json!([]);
        u_proj["mockUnavailable"] = json!(true);
    }

    for (seq, tid, ic, run_id) in [
        (10u64, run_outage_tid.as_str(), None, Some("r-outage")),
        (11u64, ahead_ic_tid.as_str(), Some(5u64), None),
        (12u64, unattr_runs_tid.as_str(), None, None),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": "safety",
                    "itemCount": ic,
                    "latestRunId": run_id
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let run_outage_role = run_outage_role.clone();
        let ahead_ic_role = ahead_ic_role.clone();
        let unattr_runs_role = unattr_runs_role.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&run_outage_role, &ahead_ic_role, &unattr_runs_role]
                .into_iter()
                .all(|want| {
                    snap.workers.iter().any(|(r, _, p)| {
                        r == want
                            && matches!(
                                p,
                                WorkerPhase::Live {
                                    activity: Activity::Refused { .. },
                                    ..
                                }
                            )
                    })
                })
        }
    })
    .await;

    {
        let mut st = server.state.lock().await;
        let o_proj = st
            .thread_projections
            .get_mut(run_outage_tid.as_str())
            .unwrap();
        o_proj["mockUnavailable"] = json!(false);
        o_proj["thread"]["status"] = json!("completed");
        o_proj["thread"]["stopReason"] = Value::Null;
        o_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "runId": "r-outage", "id": "outage_refused_draft", "text": "Refused draft turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "outage_recovered_final", "text": "Recovered reply turn 2" }
        ]);

        let a_proj = st
            .thread_projections
            .get_mut(ahead_ic_tid.as_str())
            .unwrap();
        a_proj["thread"]["status"] = json!("completed");
        a_proj["thread"]["stopReason"] = Value::Null;
        a_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "good_turn1", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" },
            { "ordinal": 4, "type": "error", "message": "refused" },
            { "ordinal": 5, "type": "user_message", "text": "u3" },
            { "ordinal": 6, "type": "assistant_message", "id": "good_turn3", "text": "Third recovered" }
        ]);

        let u_proj = st
            .thread_projections
            .get_mut(unattr_runs_tid.as_str())
            .unwrap();
        u_proj["mockUnavailable"] = json!(false);
        u_proj["thread"]["status"] = json!("completed");
        u_proj["thread"]["stopReason"] = Value::Null;
        u_proj["runs"] = json!([
            { "id": "r1", "ordinal": 0, "status": "completed" },
            { "id": "r2", "ordinal": 1, "status": "failed" },
            { "id": "r3", "ordinal": 2, "status": "completed" }
        ]);
        u_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "runId": "r1", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "runId": "r1", "id": "unobs_t1", "text": "Successful unobserved first turn" },
            { "ordinal": 2, "type": "user_message", "runId": "r2", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "runId": "r2", "id": "failed_t2", "text": "Failed draft turn 2" },
            { "ordinal": 4, "type": "user_message", "runId": "r3", "text": "u3" },
            { "ordinal": 5, "type": "assistant_message", "runId": "r3", "id": "good_t3", "text": "Third recovered" }
        ]);
    }

    let _ = shutdown_tx.send(true);
    let _ = bridge_task.await;
    let mut cfg2 = T3Config::explicit(&server.origin(), "tok_r17").unwrap();
    cfg2.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg2);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    for (seq, tid) in [
        (20u64, run_outage_tid.as_str()),
        (21u64, ahead_ic_tid.as_str()),
        (22u64, unattr_runs_tid.as_str()),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "pendingRuntimeRequest": null,
                    "archivedAt": null
                }
            }))
            .await;
    }

    let room = wave_room(&wave, &project);
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let room = room.clone();
        let wave = wave.clone();
        let run_outage_role = run_outage_role.clone();
        let ahead_ic_role = ahead_ic_role.clone();
        let unattr_runs_role = unattr_runs_role.clone();
        async move {
            let Ok(page) = operator
                .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
                .await
            else {
                return false;
            };
            let count_for = |r: &RoleName| {
                page.entries
                    .iter()
                    .filter(|e| {
                        e.by == Principal::Worker {
                            wave: wave.clone(),
                            role: r.clone(),
                        } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                    })
                    .count()
            };
            count_for(&run_outage_role) == 1
                && count_for(&ahead_ic_role) == 2
                && count_for(&unattr_runs_role) == 2
        }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let page = operator
        .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
        .await
        .unwrap();
    let bodies_for = |r: &RoleName| -> Vec<&str> {
        page.entries
            .iter()
            .filter(|e| {
                e.by == Principal::Worker {
                    wave: wave.clone(),
                    role: r.clone(),
                }
            })
            .filter_map(|e| match &e.event {
                Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(bodies_for(&run_outage_role), vec!["Recovered reply turn 2"]);
    assert_eq!(
        bodies_for(&ahead_ic_role),
        vec!["Successful turn 1", "Third recovered"]
    );
    assert_eq!(
        bodies_for(&unattr_runs_role),
        vec!["Successful unobserved first turn", "Third recovered"]
    );

    let _ = shutdown_tx.send(true);
    conductor_task.abort();
    bridge_task.abort();
}

#[tokio::test]
async fn t3_ahead_hold_across_stale_completion_chained_refusals_and_run_metadata_upgrade() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_r18".into(),
        projects: vec![json!({
            "id": "uuid-r18",
            "title": "rex-wave",
            "workspaceRoot": "/tmp/rex-wave"
        })],
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_r18").unwrap();
    cfg.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg);
    let router = build_test_router(tmp.path(), bridge.clone());
    let host_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    let conductor_task = spawn_reactor(WaveConductor::new(T3Host(bridge.clone())), host_client);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let wave = WaveSlug::parse("r18-edge-v1").unwrap();
    let project = BbProjectId::parse("uuid-r18").unwrap();
    let stale_hold_role = RoleName::parse("stalehold").unwrap();
    let chained_fail_role = RoleName::parse("chainfail").unwrap();
    let mode_upgrade_role = RoleName::parse("modeup").unwrap();

    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse("Verify stale ahead hold, chained refusals, and runId dedupe")
                    .unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("antigravity").unwrap(),
                    model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![
                    WorkerSpec {
                        role: stale_hold_role.clone(),
                        brief: Body::parse("Hold open tail across stale completion").unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: chained_fail_role.clone(),
                        brief: Body::parse("Suppress first outage refusal before explicit second")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: mode_upgrade_role.clone(),
                        brief: Body::parse("Deduplicate item-only post when runs metadata arrives")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                ],
            },
            idem: None,
        })
        .await
        .unwrap();

    let stale_tid = BbThreadId::parse("t3:r18-edge-v1:stalehold:a1").unwrap();
    let chain_tid = BbThreadId::parse("t3:r18-edge-v1:chainfail:a1").unwrap();
    let mode_tid = BbThreadId::parse("t3:r18-edge-v1:modeup:a1").unwrap();
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let stale_tid = stale_tid.clone();
        let chain_tid = chain_tid.clone();
        let mode_tid = mode_tid.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&stale_tid, &chain_tid, &mode_tid]
                .into_iter()
                .all(|want_tid| {
                    snap.workers.iter().any(|(_, _, p)| {
                        matches!(p, WorkerPhase::Live { thread, .. } if thread == want_tid)
                    })
                })
        }
    })
    .await;

    {
        let mut st = server.state.lock().await;
        for tid in [stale_tid.as_str(), mode_tid.as_str()] {
            let proj = st.thread_projections.get_mut(tid).unwrap();
            proj["runs"] = json!([]);
            proj["turnItems"] = json!([
                { "ordinal": 0, "type": "user_message", "text": "u1" },
                { "ordinal": 1, "type": "assistant_message", "id": "good_turn1", "text": "Successful turn 1" },
                { "ordinal": 2, "type": "user_message", "text": "u2" },
                { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" }
            ]);
            proj["thread"]["status"] = json!("completed");
            proj["thread"]["stopReason"] = Value::Null;
        }
        let c_proj = st.thread_projections.get_mut(chain_tid.as_str()).unwrap();
        c_proj["runs"] = json!([]);
        c_proj["mockUnavailable"] = json!(true);
    }

    for (seq, tid, ic) in [
        (10u64, stale_tid.as_str(), Some(5u64)),
        (11u64, chain_tid.as_str(), None),
        (12u64, mode_tid.as_str(), Some(5u64)),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": "safety",
                    "itemCount": ic
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let stale_hold_role = stale_hold_role.clone();
        let chained_fail_role = chained_fail_role.clone();
        let mode_upgrade_role = mode_upgrade_role.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&stale_hold_role, &chained_fail_role, &mode_upgrade_role]
                .into_iter()
                .all(|want| {
                    snap.workers.iter().any(|(r, _, p)| {
                        r == want
                            && matches!(
                                p,
                                WorkerPhase::Live {
                                    activity: Activity::Refused { .. },
                                    ..
                                }
                            )
                    })
                })
        }
    })
    .await;

    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 13,
            "location": "active",
            "thread": {
                "id": stale_tid.as_str(),
                "status": "completed",
                "itemCount": 7
            }
        }))
        .await;

    {
        let mut st = server.state.lock().await;
        let c_proj = st.thread_projections.get_mut(chain_tid.as_str()).unwrap();
        c_proj["mockUnavailable"] = json!(false);
        c_proj["thread"]["status"] = json!("completed");
        c_proj["runs"] = json!([]);
        c_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft" },
            { "type": "user_message", "text": "u2" },
            { "type": "assistant_message", "text": "Second refused draft" }
        ]);
    }
    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 14,
            "location": "active",
            "thread": {
                "id": chain_tid.as_str(),
                "status": "completed",
                "stopReason": "safety",
                "itemCount": 4
            }
        }))
        .await;

    tokio::time::sleep(Duration::from_millis(150)).await;
    let room = wave_room(&wave, &project);
    let mid_page = operator
        .call(Read::stream(
            StreamId::Room { room: room.clone() },
            Seq::ORIGIN,
            Some(50),
        ))
        .await
        .unwrap();
    let mid_bodies_for = |r: &RoleName| -> Vec<&str> {
        mid_page
            .entries
            .iter()
            .filter(|e| {
                e.by == Principal::Worker {
                    wave: wave.clone(),
                    role: r.clone(),
                }
            })
            .filter_map(|e| match &e.event {
                Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(mid_bodies_for(&stale_hold_role), vec!["Successful turn 1"]);
    assert!(mid_bodies_for(&chained_fail_role).is_empty());

    {
        let mut st = server.state.lock().await;
        let s_proj = st.thread_projections.get_mut(stale_tid.as_str()).unwrap();
        s_proj["thread"]["status"] = json!("completed");
        s_proj["thread"]["stopReason"] = Value::Null;
        s_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "good_turn1", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" },
            { "ordinal": 4, "type": "error", "message": "refused" },
            { "ordinal": 5, "type": "user_message", "text": "u3" },
            { "ordinal": 6, "type": "assistant_message", "id": "good_turn3", "text": "Third recovered" }
        ]);

        let m_proj = st.thread_projections.get_mut(mode_tid.as_str()).unwrap();
        m_proj["thread"]["status"] = json!("completed");
        m_proj["thread"]["stopReason"] = Value::Null;
        m_proj["runs"] = json!([
            { "id": "r1", "ordinal": 0, "status": "completed" },
            { "id": "r2", "ordinal": 1, "status": "failed" },
            { "id": "r3", "ordinal": 2, "status": "completed" }
        ]);
        m_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "runId": "r1", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "runId": "r1", "id": "good_turn1", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "runId": "r2", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "runId": "r2", "id": "failed_turn2", "text": "Failed draft turn 2" },
            { "ordinal": 4, "type": "error", "runId": "r2", "message": "refused" },
            { "ordinal": 5, "type": "user_message", "runId": "r3", "text": "u3" },
            { "ordinal": 6, "type": "assistant_message", "runId": "r3", "id": "good_turn3", "text": "Third recovered" }
        ]);

        let c_proj = st.thread_projections.get_mut(chain_tid.as_str()).unwrap();
        c_proj["thread"]["status"] = json!("completed");
        c_proj["thread"]["stopReason"] = Value::Null;
        c_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft" },
            { "type": "user_message", "text": "u2" },
            { "type": "assistant_message", "text": "Second refused draft" },
            { "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Recovered third turn" }
        ]);
    }

    let _ = shutdown_tx.send(true);
    let _ = bridge_task.await;
    let mut cfg2 = T3Config::explicit(&server.origin(), "tok_r18").unwrap();
    cfg2.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg2);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    for (seq, tid) in [
        (20u64, stale_tid.as_str()),
        (21u64, chain_tid.as_str()),
        (22u64, mode_tid.as_str()),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "pendingRuntimeRequest": null,
                    "archivedAt": null
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let room = room.clone();
        let wave = wave.clone();
        let stale_hold_role = stale_hold_role.clone();
        let chained_fail_role = chained_fail_role.clone();
        let mode_upgrade_role = mode_upgrade_role.clone();
        async move {
            let Ok(page) = operator
                .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
                .await
            else {
                return false;
            };
            let count_for = |r: &RoleName| {
                page.entries
                    .iter()
                    .filter(|e| {
                        e.by == Principal::Worker {
                            wave: wave.clone(),
                            role: r.clone(),
                        } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                    })
                    .count()
            };
            count_for(&stale_hold_role) == 2
                && count_for(&chained_fail_role) == 1
                && count_for(&mode_upgrade_role) == 2
        }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let page = operator
        .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
        .await
        .unwrap();
    let bodies_for = |r: &RoleName| -> Vec<&str> {
        page.entries
            .iter()
            .filter(|e| {
                e.by == Principal::Worker {
                    wave: wave.clone(),
                    role: r.clone(),
                }
            })
            .filter_map(|e| match &e.event {
                Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(
        bodies_for(&stale_hold_role),
        vec!["Successful turn 1", "Third recovered"]
    );
    assert_eq!(bodies_for(&chained_fail_role), vec!["Recovered third turn"]);
    assert_eq!(
        bodies_for(&mode_upgrade_role),
        vec!["Successful turn 1", "Third recovered"]
    );

    let _ = shutdown_tx.send(true);
    conductor_task.abort();
    bridge_task.abort();
}

#[tokio::test]
async fn t3_chained_empty_projection_outage_run_id_item_only_recovery_and_legacy_o0_upgrade() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_r19".into(),
        projects: vec![json!({
            "id": "uuid-r19",
            "title": "rex-wave",
            "workspaceRoot": "/tmp/rex-wave"
        })],
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_r19").unwrap();
    cfg.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg);
    let router = build_test_router(tmp.path(), bridge.clone());
    let host_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    let conductor_task = spawn_reactor(WaveConductor::new(T3Host(bridge.clone())), host_client);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let wave = WaveSlug::parse("r19-edge-v1").unwrap();
    let project = BbProjectId::parse("uuid-r19").unwrap();
    let chain_empty_role = RoleName::parse("chainempty").unwrap();
    let run_ic_role = RoleName::parse("runic").unwrap();
    let legacy_o0_role = RoleName::parse("legacyo0").unwrap();

    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse(
                    "Verify empty-proj chained outage, runId+itemCount recovery, and legacy o0 upgrade",
                )
                .unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("antigravity").unwrap(),
                    model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![
                    WorkerSpec {
                        role: chain_empty_role.clone(),
                        brief: Body::parse("Outage refusal then empty-projection explicit refusal")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: run_ic_role.clone(),
                        brief: Body::parse("Refusal with latestRunId and itemCount on item-only")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: legacy_o0_role.clone(),
                        brief: Body::parse("Deduplicate pre-upgrade o0 post without item IDs")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                ],
            },
            idem: None,
        })
        .await
        .unwrap();

    let chain_tid = BbThreadId::parse("t3:r19-edge-v1:chainempty:a1").unwrap();
    let run_ic_tid = BbThreadId::parse("t3:r19-edge-v1:runic:a1").unwrap();
    let legacy_o0_tid = BbThreadId::parse("t3:r19-edge-v1:legacyo0:a1").unwrap();
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let chain_tid = chain_tid.clone();
        let run_ic_tid = run_ic_tid.clone();
        let legacy_o0_tid = legacy_o0_tid.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&chain_tid, &run_ic_tid, &legacy_o0_tid]
                .into_iter()
                .all(|want_tid| {
                    snap.workers.iter().any(|(_, _, p)| {
                        matches!(p, WorkerPhase::Live { thread, .. } if thread == want_tid)
                    })
                })
        }
    })
    .await;

    let room = wave_room(&wave, &project);
    let legacy_worker = LocalClient::new(
        router.clone(),
        Principal::Worker {
            wave: wave.clone(),
            role: legacy_o0_role.clone(),
        },
    );
    legacy_worker
        .call(rex_wave_api::room::RoomPost {
            room: rex_wave_api::room::RoomRef::Id { id: room.clone() },
            body: Body::parse("Successful turn 1").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: Some(
                rex_wave_waves::mirror_idem_key(&wave, &legacy_o0_role, &legacy_o0_tid, "o0")
                    .unwrap(),
            ),
            origin: Default::default(),
        })
        .await
        .unwrap();

    {
        let mut st = server.state.lock().await;
        let c_proj = st.thread_projections.get_mut(chain_tid.as_str()).unwrap();
        c_proj["runs"] = json!([]);
        c_proj["mockUnavailable"] = json!(true);

        let r_proj = st.thread_projections.get_mut(run_ic_tid.as_str()).unwrap();
        r_proj["runs"] = json!([]);
        r_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "good_turn1", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" }
        ]);
        r_proj["thread"]["status"] = json!("completed");
        r_proj["thread"]["stopReason"] = Value::Null;

        let l_proj = st
            .thread_projections
            .get_mut(legacy_o0_tid.as_str())
            .unwrap();
        l_proj["runs"] = json!([]);
        l_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" }
        ]);
        l_proj["thread"]["status"] = json!("completed");
        l_proj["thread"]["stopReason"] = Value::Null;
    }

    for (seq, tid, ic, run_id) in [
        (10u64, chain_tid.as_str(), None, None),
        (11u64, run_ic_tid.as_str(), Some(5u64), Some("r2")),
        (12u64, legacy_o0_tid.as_str(), Some(5u64), None),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": "safety",
                    "latestRunId": run_id,
                    "itemCount": ic
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let chain_empty_role = chain_empty_role.clone();
        let run_ic_role = run_ic_role.clone();
        let legacy_o0_role = legacy_o0_role.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&chain_empty_role, &run_ic_role, &legacy_o0_role]
                .into_iter()
                .all(|want| {
                    snap.workers.iter().any(|(r, _, p)| {
                        r == want
                            && matches!(
                                p,
                                WorkerPhase::Live {
                                    activity: Activity::Refused { .. },
                                    ..
                                }
                            )
                    })
                })
        }
    })
    .await;

    {
        let mut st = server.state.lock().await;
        let c_proj = st.thread_projections.get_mut(chain_tid.as_str()).unwrap();
        c_proj["mockUnavailable"] = json!(false);
        c_proj["thread"]["status"] = json!("completed");
        c_proj["runs"] = json!([]);
        c_proj["turnItems"] = json!([]);
    }
    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 14,
            "location": "active",
            "thread": {
                "id": chain_tid.as_str(),
                "status": "completed",
                "stopReason": "safety",
                "itemCount": 4
            }
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    {
        let mut st = server.state.lock().await;
        let c_proj = st.thread_projections.get_mut(chain_tid.as_str()).unwrap();
        c_proj["thread"]["status"] = json!("completed");
        c_proj["thread"]["stopReason"] = Value::Null;
        c_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft" },
            { "type": "user_message", "text": "u2" },
            { "type": "assistant_message", "text": "Second refused draft" },
            { "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Recovered third turn" }
        ]);

        let r_proj = st.thread_projections.get_mut(run_ic_tid.as_str()).unwrap();
        r_proj["thread"]["status"] = json!("completed");
        r_proj["thread"]["stopReason"] = Value::Null;
        r_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "good_turn1", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" },
            { "ordinal": 4, "type": "error", "message": "refused" },
            { "ordinal": 5, "type": "user_message", "text": "u3" },
            { "ordinal": 6, "type": "assistant_message", "id": "good_turn3", "text": "Third recovered" }
        ]);

        let l_proj = st
            .thread_projections
            .get_mut(legacy_o0_tid.as_str())
            .unwrap();
        l_proj["thread"]["status"] = json!("completed");
        l_proj["thread"]["stopReason"] = Value::Null;
        l_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" },
            { "ordinal": 4, "type": "error", "message": "refused" },
            { "ordinal": 5, "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Third recovered" }
        ]);
    }

    let _ = shutdown_tx.send(true);
    let _ = bridge_task.await;
    let mut cfg2 = T3Config::explicit(&server.origin(), "tok_r19").unwrap();
    cfg2.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg2);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    for (seq, tid, rid) in [
        (20u64, chain_tid.as_str(), None),
        (21u64, run_ic_tid.as_str(), Some("r3")),
        (22u64, legacy_o0_tid.as_str(), None),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "latestRunId": rid,
                    "pendingRuntimeRequest": null,
                    "archivedAt": null
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let room = room.clone();
        let wave = wave.clone();
        let chain_empty_role = chain_empty_role.clone();
        let run_ic_role = run_ic_role.clone();
        let legacy_o0_role = legacy_o0_role.clone();
        async move {
            let Ok(page) = operator
                .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
                .await
            else {
                return false;
            };
            let count_for = |r: &RoleName| {
                page.entries
                    .iter()
                    .filter(|e| {
                        e.by == Principal::Worker {
                            wave: wave.clone(),
                            role: r.clone(),
                        } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                    })
                    .count()
            };
            count_for(&chain_empty_role) == 1
                && count_for(&run_ic_role) == 2
                && count_for(&legacy_o0_role) == 2
        }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let page = operator
        .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
        .await
        .unwrap();
    let bodies_for = |r: &RoleName| -> Vec<&str> {
        page.entries
            .iter()
            .filter(|e| {
                e.by == Principal::Worker {
                    wave: wave.clone(),
                    role: r.clone(),
                }
            })
            .filter_map(|e| match &e.event {
                Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(bodies_for(&chain_empty_role), vec!["Recovered third turn"]);
    assert_eq!(
        bodies_for(&run_ic_role),
        vec!["Successful turn 1", "Third recovered"]
    );
    assert_eq!(
        bodies_for(&legacy_o0_role),
        vec!["Successful turn 1", "Third recovered"]
    );

    let _ = shutdown_tx.send(true);
    conductor_task.abort();
    bridge_task.abort();
}

#[tokio::test]
async fn t3_double_outage_partial_replay_run_id_only_item_projection_and_index_zero_o0_claim() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_r20".into(),
        projects: vec![json!({
            "id": "uuid-r20",
            "title": "rex-wave",
            "workspaceRoot": "/tmp/rex-wave"
        })],
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_r20").unwrap();
    cfg.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg);
    let router = build_test_router(tmp.path(), bridge.clone());
    let host_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    let conductor_task = spawn_reactor(WaveConductor::new(T3Host(bridge.clone())), host_client);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let wave = WaveSlug::parse("r20-edge-v1").unwrap();
    let project = BbProjectId::parse("uuid-r20").unwrap();
    let two_unattr_role = RoleName::parse("twounattr").unwrap();
    let run_only_role = RoleName::parse("runonly").unwrap();
    let idx_zero_o0_role = RoleName::parse("idxzeroo0").unwrap();

    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse(
                    "Verify 1-for-1 outage ticket resolution, runId-only item projection, and index-0 o0 claim",
                )
                .unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("antigravity").unwrap(),
                    model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![
                    WorkerSpec {
                        role: two_unattr_role.clone(),
                        brief: Body::parse("Two unattributed refusals then partial single-turn replay")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: run_only_role.clone(),
                        brief: Body::parse("Refusal with latestRunId only on item-only projection")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: idx_zero_o0_role.clone(),
                        brief: Body::parse("Index-zero o0 exact match claims seq before identical later turn")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                ],
            },
            idem: None,
        })
        .await
        .unwrap();

    let two_tid = BbThreadId::parse("t3:r20-edge-v1:twounattr:a1").unwrap();
    let run_only_tid = BbThreadId::parse("t3:r20-edge-v1:runonly:a1").unwrap();
    let idx_zero_tid = BbThreadId::parse("t3:r20-edge-v1:idxzeroo0:a1").unwrap();
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let two_tid = two_tid.clone();
        let run_only_tid = run_only_tid.clone();
        let idx_zero_tid = idx_zero_tid.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&two_tid, &run_only_tid, &idx_zero_tid]
                .into_iter()
                .all(|want_tid| {
                    snap.workers.iter().any(|(_, _, p)| {
                        matches!(p, WorkerPhase::Live { thread, .. } if thread == want_tid)
                    })
                })
        }
    })
    .await;

    let room = wave_room(&wave, &project);
    let idx_zero_worker = LocalClient::new(
        router.clone(),
        Principal::Worker {
            wave: wave.clone(),
            role: idx_zero_o0_role.clone(),
        },
    );
    idx_zero_worker
        .call(rex_wave_api::room::RoomPost {
            room: rex_wave_api::room::RoomRef::Id { id: room.clone() },
            body: Body::parse("Successful turn 1").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: Some(
                rex_wave_waves::mirror_idem_key(
                    &wave,
                    &idx_zero_role_alias(&idx_zero_o0_role),
                    &idx_zero_tid,
                    "o0",
                )
                .unwrap(),
            ),
            origin: Default::default(),
        })
        .await
        .unwrap();

    {
        let mut st = server.state.lock().await;
        let t_proj = st.thread_projections.get_mut(two_tid.as_str()).unwrap();
        t_proj["runs"] = json!([]);
        t_proj["mockUnavailable"] = json!(true);

        let r_proj = st
            .thread_projections
            .get_mut(run_only_tid.as_str())
            .unwrap();
        r_proj["runs"] = json!([]);
        r_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "good_turn1", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" }
        ]);
        r_proj["thread"]["status"] = json!("completed");
        r_proj["thread"]["stopReason"] = Value::Null;

        let z_proj = st
            .thread_projections
            .get_mut(idx_zero_tid.as_str())
            .unwrap();
        z_proj["runs"] = json!([]);
        z_proj["turnItems"] = json!([
            { "type": "assistant_message", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" }
        ]);
        z_proj["thread"]["status"] = json!("completed");
        z_proj["thread"]["stopReason"] = Value::Null;
    }

    for (seq, tid, ic, run_id) in [
        (10u64, two_tid.as_str(), None, None),
        (11u64, run_only_tid.as_str(), None, Some("r2")),
        (12u64, idx_zero_tid.as_str(), Some(4u64), None),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": "safety",
                    "latestRunId": run_id,
                    "itemCount": ic
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let two_unattr_role = two_unattr_role.clone();
        let run_only_role = run_only_role.clone();
        let idx_zero_o0_role = idx_zero_o0_role.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&two_unattr_role, &run_only_role, &idx_zero_o0_role]
                .into_iter()
                .all(|want| {
                    snap.workers.iter().any(|(r, _, p)| {
                        r == want
                            && matches!(
                                p,
                                WorkerPhase::Live {
                                    activity: Activity::Refused { .. },
                                    ..
                                }
                            )
                    })
                })
        }
    })
    .await;

    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 14,
            "location": "active",
            "thread": {
                "id": two_tid.as_str(),
                "status": "completed",
                "stopReason": "safety"
            }
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    {
        let mut st = server.state.lock().await;
        let t_proj = st.thread_projections.get_mut(two_tid.as_str()).unwrap();
        t_proj["mockUnavailable"] = json!(false);
        t_proj["thread"]["status"] = json!("completed");
        t_proj["thread"]["stopReason"] = Value::Null;
        t_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft" }
        ]);
    }
    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 15,
            "location": "active",
            "thread": {
                "id": two_tid.as_str(),
                "status": "completed"
            }
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    {
        let mut st = server.state.lock().await;
        let t_proj = st.thread_projections.get_mut(two_tid.as_str()).unwrap();
        t_proj["thread"]["status"] = json!("completed");
        t_proj["thread"]["stopReason"] = Value::Null;
        t_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft" },
            { "type": "user_message", "text": "u2" },
            { "type": "assistant_message", "text": "Second refused draft" },
            { "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Recovered third turn" }
        ]);

        let r_proj = st
            .thread_projections
            .get_mut(run_only_tid.as_str())
            .unwrap();
        r_proj["thread"]["status"] = json!("completed");
        r_proj["thread"]["stopReason"] = Value::Null;
        r_proj["turnItems"] = json!([
            { "ordinal": 0, "type": "user_message", "text": "u1" },
            { "ordinal": 1, "type": "assistant_message", "id": "good_turn1", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" },
            { "ordinal": 5, "type": "user_message", "text": "u3" },
            { "ordinal": 6, "type": "assistant_message", "id": "good_turn3", "text": "Third recovered" }
        ]);

        let z_proj = st
            .thread_projections
            .get_mut(idx_zero_tid.as_str())
            .unwrap();
        z_proj["thread"]["status"] = json!("completed");
        z_proj["thread"]["stopReason"] = Value::Null;
        z_proj["turnItems"] = json!([
            { "type": "assistant_message", "text": "Successful turn 1" },
            { "ordinal": 2, "type": "user_message", "text": "u2" },
            { "ordinal": 3, "type": "assistant_message", "id": "failed_turn2", "text": "Failed draft turn 2" },
            { "ordinal": 4, "type": "error", "message": "refused" },
            { "ordinal": 5, "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Successful turn 1" }
        ]);
    }

    let _ = shutdown_tx.send(true);
    let _ = bridge_task.await;
    let mut cfg2 = T3Config::explicit(&server.origin(), "tok_r20").unwrap();
    cfg2.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg2);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    for (seq, tid, rid) in [
        (20u64, two_tid.as_str(), None),
        (21u64, run_only_tid.as_str(), Some("r3")),
        (22u64, idx_zero_tid.as_str(), None),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "latestRunId": rid,
                    "pendingRuntimeRequest": null,
                    "archivedAt": null
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let room = room.clone();
        let wave = wave.clone();
        let two_unattr_role = two_unattr_role.clone();
        let run_only_role = run_only_role.clone();
        let idx_zero_o0_role = idx_zero_o0_role.clone();
        async move {
            let Ok(page) = operator
                .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
                .await
            else {
                return false;
            };
            let count_for = |r: &RoleName| {
                page.entries
                    .iter()
                    .filter(|e| {
                        e.by == Principal::Worker {
                            wave: wave.clone(),
                            role: r.clone(),
                        } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                    })
                    .count()
            };
            count_for(&two_unattr_role) == 1
                && count_for(&run_only_role) == 2
                && count_for(&idx_zero_o0_role) == 2
        }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let page = operator
        .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
        .await
        .unwrap();
    let bodies_for = |r: &RoleName| -> Vec<&str> {
        page.entries
            .iter()
            .filter(|e| {
                e.by == Principal::Worker {
                    wave: wave.clone(),
                    role: r.clone(),
                }
            })
            .filter_map(|e| match &e.event {
                Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(bodies_for(&two_unattr_role), vec!["Recovered third turn"]);
    assert_eq!(
        bodies_for(&run_only_role),
        vec!["Successful turn 1", "Third recovered"]
    );
    assert_eq!(
        bodies_for(&idx_zero_o0_role),
        vec!["Successful turn 1", "Successful turn 1"]
    );

    let _ = shutdown_tx.send(true);
    conductor_task.abort();
    bridge_task.abort();
}

fn idx_zero_role_alias(r: &RoleName) -> RoleName {
    r.clone()
}

#[tokio::test]
async fn t3_matched_run_preserves_other_run_ticket_and_single_ticket_budget_preserves_later_success()
 {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_r21".into(),
        projects: vec![json!({
            "id": "uuid-r21",
            "title": "rex-wave",
            "workspaceRoot": "/tmp/rex-wave"
        })],
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_r21").unwrap();
    cfg.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg);
    let router = build_test_router(tmp.path(), bridge.clone());
    let host_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    let conductor_task = spawn_reactor(WaveConductor::new(T3Host(bridge.clone())), host_client);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let wave = WaveSlug::parse("r21-edge-v1").unwrap();
    let project = BbProjectId::parse("uuid-r21").unwrap();
    let matched_run_role = RoleName::parse("matchedrun").unwrap();
    let budget_cap_role = RoleName::parse("budgetcap").unwrap();

    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse(
                    "Verify matched run preserves other run tickets and 1-ticket budget preserves multiple recoveries",
                )
                .unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("antigravity").unwrap(),
                    model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![
                    WorkerSpec {
                        role: matched_run_role.clone(),
                        brief: Body::parse("Explicit r1 match preserves pending r2 ticket").unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: budget_cap_role.clone(),
                        brief: Body::parse("Single remaining ticket suppresses one turn before two successes")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                ],
            },
            idem: None,
        })
        .await
        .unwrap();

    let matched_tid = BbThreadId::parse("t3:r21-edge-v1:matchedrun:a1").unwrap();
    let budget_tid = BbThreadId::parse("t3:r21-edge-v1:budgetcap:a1").unwrap();
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let matched_tid = matched_tid.clone();
        let budget_tid = budget_tid.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&matched_tid, &budget_tid].into_iter().all(|want_tid| {
                snap.workers.iter().any(
                    |(_, _, p)| matches!(p, WorkerPhase::Live { thread, .. } if thread == want_tid),
                )
            })
        }
    })
    .await;

    {
        let mut st = server.state.lock().await;
        for tid in [matched_tid.as_str(), budget_tid.as_str()] {
            let proj = st.thread_projections.get_mut(tid).unwrap();
            proj["runs"] = json!([]);
            proj["mockUnavailable"] = json!(true);
        }
    }

    for (seq, tid, run_id) in [
        (10u64, matched_tid.as_str(), Some("r1")),
        (11u64, budget_tid.as_str(), None),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": "safety",
                    "latestRunId": run_id
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let matched_run_role = matched_run_role.clone();
        let budget_cap_role = budget_cap_role.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&matched_run_role, &budget_cap_role]
                .into_iter()
                .all(|want| {
                    snap.workers.iter().any(|(r, _, p)| {
                        r == want
                            && matches!(
                                p,
                                WorkerPhase::Live {
                                    activity: Activity::Refused { .. },
                                    ..
                                }
                            )
                    })
                })
        }
    })
    .await;

    for (seq, tid, run_id) in [
        (14u64, matched_tid.as_str(), Some("r2")),
        (15u64, budget_tid.as_str(), None),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": "safety",
                    "latestRunId": run_id
                }
            }))
            .await;
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    {
        let mut st = server.state.lock().await;
        let m_proj = st.thread_projections.get_mut(matched_tid.as_str()).unwrap();
        m_proj["mockUnavailable"] = json!(false);
        m_proj["thread"]["status"] = json!("completed");
        m_proj["thread"]["stopReason"] = Value::Null;
        m_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft", "runId": "r1" }
        ]);

        let b_proj = st.thread_projections.get_mut(budget_tid.as_str()).unwrap();
        b_proj["mockUnavailable"] = json!(false);
        b_proj["thread"]["status"] = json!("completed");
        b_proj["thread"]["stopReason"] = Value::Null;
        b_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft" }
        ]);
    }

    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 16,
            "location": "active",
            "thread": {
                "id": matched_tid.as_str(),
                "status": "completed",
                "stopReason": "safety",
                "latestRunId": "r1"
            }
        }))
        .await;
    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 17,
            "location": "active",
            "thread": {
                "id": budget_tid.as_str(),
                "status": "completed"
            }
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    {
        let mut st = server.state.lock().await;
        let m_proj = st.thread_projections.get_mut(matched_tid.as_str()).unwrap();
        m_proj["thread"]["status"] = json!("completed");
        m_proj["thread"]["stopReason"] = Value::Null;
        m_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft", "runId": "r1" },
            { "type": "user_message", "text": "u2" },
            { "type": "assistant_message", "text": "Second refused draft" },
            { "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Recovered third turn" }
        ]);

        let b_proj = st.thread_projections.get_mut(budget_tid.as_str()).unwrap();
        b_proj["thread"]["status"] = json!("completed");
        b_proj["thread"]["stopReason"] = Value::Null;
        b_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft" },
            { "type": "user_message", "text": "u2" },
            { "type": "assistant_message", "text": "Second refused draft" },
            { "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Recovered third turn" },
            { "type": "user_message", "text": "u4" },
            { "type": "assistant_message", "text": "Fourth success" }
        ]);
    }

    let _ = shutdown_tx.send(true);
    let _ = bridge_task.await;
    let mut cfg2 = T3Config::explicit(&server.origin(), "tok_r21").unwrap();
    cfg2.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg2);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    for (seq, tid) in [(20u64, matched_tid.as_str()), (21u64, budget_tid.as_str())] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "pendingRuntimeRequest": null,
                    "archivedAt": null
                }
            }))
            .await;
    }

    let room = wave_room(&wave, &project);
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let room = room.clone();
        let wave = wave.clone();
        let matched_run_role = matched_run_role.clone();
        let budget_cap_role = budget_cap_role.clone();
        async move {
            let Ok(page) = operator
                .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
                .await
            else {
                return false;
            };
            let count_for = |r: &RoleName| {
                page.entries
                    .iter()
                    .filter(|e| {
                        e.by == Principal::Worker {
                            wave: wave.clone(),
                            role: r.clone(),
                        } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                    })
                    .count()
            };
            count_for(&matched_run_role) == 1 && count_for(&budget_cap_role) == 2
        }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let page = operator
        .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
        .await
        .unwrap();
    let bodies_for = |r: &RoleName| -> Vec<&str> {
        page.entries
            .iter()
            .filter(|e| {
                e.by == Principal::Worker {
                    wave: wave.clone(),
                    role: r.clone(),
                }
            })
            .filter_map(|e| match &e.event {
                Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(bodies_for(&matched_run_role), vec!["Recovered third turn"]);
    assert_eq!(
        bodies_for(&budget_cap_role),
        vec!["Recovered third turn", "Fourth success"]
    );

    let _ = shutdown_tx.send(true);
    conductor_task.abort();
    bridge_task.abort();
}

#[tokio::test]
async fn t3_causal_pre_ticket_bound_run_anchor_promotion_and_anonymous_to_run_refinement() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_r22".into(),
        projects: vec![json!({
            "id": "uuid-r22",
            "title": "rex-wave",
            "workspaceRoot": "/tmp/rex-wave"
        })],
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_r22").unwrap();
    cfg.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg);
    let router = build_test_router(tmp.path(), bridge.clone());
    let host_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    let conductor_task = spawn_reactor(WaveConductor::new(T3Host(bridge.clone())), host_client);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let wave = WaveSlug::parse("r22-edge-v1").unwrap();
    let project = BbProjectId::parse("uuid-r22").unwrap();
    let prior_good_role = RoleName::parse("priorgood").unwrap();
    let anchor_promo_role = RoleName::parse("anchorpromo").unwrap();
    let anon_refine_role = RoleName::parse("anonrefine").unwrap();

    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse(
                    "Verify causal pre-ticket bound, suppressed anchor promotion, and anonymous-to-run refinement",
                )
                .unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("antigravity").unwrap(),
                    model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![
                    WorkerSpec {
                        role: prior_good_role.clone(),
                        brief: Body::parse("Later run ticket never suppresses turn before earlier run")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: anchor_promo_role.clone(),
                        brief: Body::parse("Promote item anchors when already settled by runId")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: anon_refine_role.clone(),
                        brief: Body::parse("Resolve anonymous outage ticket when repeated with runId")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                ],
            },
            idem: None,
        })
        .await
        .unwrap();

    let prior_tid = BbThreadId::parse("t3:r22-edge-v1:priorgood:a1").unwrap();
    let promo_tid = BbThreadId::parse("t3:r22-edge-v1:anchorpromo:a1").unwrap();
    let anon_tid = BbThreadId::parse("t3:r22-edge-v1:anonrefine:a1").unwrap();
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let prior_tid = prior_tid.clone();
        let promo_tid = promo_tid.clone();
        let anon_tid = anon_tid.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&prior_tid, &promo_tid, &anon_tid]
                .into_iter()
                .all(|want_tid| {
                    snap.workers.iter().any(|(_, _, p)| {
                        matches!(p, WorkerPhase::Live { thread, .. } if thread == want_tid)
                    })
                })
        }
    })
    .await;

    {
        let mut st = server.state.lock().await;
        for tid in [prior_tid.as_str(), promo_tid.as_str(), anon_tid.as_str()] {
            let proj = st.thread_projections.get_mut(tid).unwrap();
            proj["runs"] = json!([]);
            proj["mockUnavailable"] = json!(true);
        }
    }

    for (seq, tid, run_id) in [
        (10u64, prior_tid.as_str(), Some("r1")),
        (11u64, promo_tid.as_str(), Some("r1")),
        (12u64, anon_tid.as_str(), None),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": "safety",
                    "latestRunId": run_id
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let prior_good_role = prior_good_role.clone();
        let anchor_promo_role = anchor_promo_role.clone();
        let anon_refine_role = anon_refine_role.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&prior_good_role, &anchor_promo_role, &anon_refine_role]
                .into_iter()
                .all(|want| {
                    snap.workers.iter().any(|(r, _, p)| {
                        r == want
                            && matches!(
                                p,
                                WorkerPhase::Live {
                                    activity: Activity::Refused { .. },
                                    ..
                                }
                            )
                    })
                })
        }
    })
    .await;

    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 14,
            "location": "active",
            "thread": {
                "id": prior_tid.as_str(),
                "status": "completed",
                "stopReason": "safety",
                "latestRunId": "r2"
            }
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    {
        let mut st = server.state.lock().await;
        let p_proj = st.thread_projections.get_mut(prior_tid.as_str()).unwrap();
        p_proj["mockUnavailable"] = json!(false);
        p_proj["thread"]["status"] = json!("completed");
        p_proj["thread"]["stopReason"] = Value::Null;
        p_proj["turnItems"] = json!([
            { "type": "user_message", "text": "prior" },
            { "type": "assistant_message", "text": "Earlier success" },
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft", "runId": "r1" }
        ]);

        let a_proj = st.thread_projections.get_mut(promo_tid.as_str()).unwrap();
        a_proj["mockUnavailable"] = json!(false);
        a_proj["thread"]["status"] = json!("completed");
        a_proj["thread"]["stopReason"] = Value::Null;
        a_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft", "runId": "r1" }
        ]);

        let n_proj = st.thread_projections.get_mut(anon_tid.as_str()).unwrap();
        n_proj["mockUnavailable"] = json!(false);
        n_proj["thread"]["status"] = json!("completed");
        n_proj["thread"]["stopReason"] = Value::Null;
        n_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft", "runId": "r1" }
        ]);
    }

    for (seq, tid) in [
        (16u64, prior_tid.as_str()),
        (17u64, promo_tid.as_str()),
        (18u64, anon_tid.as_str()),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": "safety",
                    "latestRunId": "r1"
                }
            }))
            .await;
    }
    tokio::time::sleep(Duration::from_millis(140)).await;

    {
        let mut st = server.state.lock().await;
        let p_proj = st.thread_projections.get_mut(prior_tid.as_str()).unwrap();
        p_proj["thread"]["status"] = json!("completed");
        p_proj["thread"]["stopReason"] = Value::Null;
        p_proj["turnItems"] = json!([
            { "type": "user_message", "text": "prior" },
            { "type": "assistant_message", "text": "Earlier success" },
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft", "runId": "r1" },
            { "type": "user_message", "text": "u2" },
            { "type": "assistant_message", "text": "Second refused draft" },
            { "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Recovered third turn" }
        ]);

        let a_proj = st.thread_projections.get_mut(promo_tid.as_str()).unwrap();
        a_proj["thread"]["status"] = json!("completed");
        a_proj["thread"]["stopReason"] = Value::Null;
        a_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft" },
            { "type": "user_message", "text": "u2" },
            { "type": "assistant_message", "text": "Second successful turn" },
            { "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Recovered third turn" }
        ]);

        let n_proj = st.thread_projections.get_mut(anon_tid.as_str()).unwrap();
        n_proj["thread"]["status"] = json!("completed");
        n_proj["thread"]["stopReason"] = Value::Null;
        n_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft", "runId": "r1" },
            { "type": "user_message", "text": "u2" },
            { "type": "assistant_message", "text": "Second successful turn" },
            { "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Recovered third turn" }
        ]);
    }

    let _ = shutdown_tx.send(true);
    let _ = bridge_task.await;
    let mut cfg2 = T3Config::explicit(&server.origin(), "tok_r22").unwrap();
    cfg2.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg2);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    for (seq, tid) in [
        (20u64, prior_tid.as_str()),
        (21u64, promo_tid.as_str()),
        (22u64, anon_tid.as_str()),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "pendingRuntimeRequest": null,
                    "archivedAt": null
                }
            }))
            .await;
    }

    let room = wave_room(&wave, &project);
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let room = room.clone();
        let wave = wave.clone();
        let prior_good_role = prior_good_role.clone();
        let anchor_promo_role = anchor_promo_role.clone();
        let anon_refine_role = anon_refine_role.clone();
        async move {
            let Ok(page) = operator
                .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
                .await
            else {
                return false;
            };
            let count_for = |r: &RoleName| {
                page.entries
                    .iter()
                    .filter(|e| {
                        e.by == Principal::Worker {
                            wave: wave.clone(),
                            role: r.clone(),
                        } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                    })
                    .count()
            };
            count_for(&prior_good_role) == 2
                && count_for(&anchor_promo_role) == 2
                && count_for(&anon_refine_role) == 2
        }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let page = operator
        .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
        .await
        .unwrap();
    let bodies_for = |r: &RoleName| -> Vec<&str> {
        page.entries
            .iter()
            .filter(|e| {
                e.by == Principal::Worker {
                    wave: wave.clone(),
                    role: r.clone(),
                }
            })
            .filter_map(|e| match &e.event {
                Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(
        bodies_for(&prior_good_role),
        vec!["Earlier success", "Recovered third turn"]
    );
    assert_eq!(
        bodies_for(&anchor_promo_role),
        vec!["Second successful turn", "Recovered third turn"]
    );
    assert_eq!(
        bodies_for(&anon_refine_role),
        vec!["Second successful turn", "Recovered third turn"]
    );

    let _ = shutdown_tx.send(true);
    conductor_task.abort();
    bridge_task.abort();
}

#[tokio::test]
async fn t3_ordinary_completion_causal_bound_prefixed_anon_refinement_and_failed_append_retry() {
    use rex_wave_store::{CommitHook, StoreError, TxContext};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FailOnceAnchorPromo(AtomicBool);
    impl CommitHook for FailOnceAnchorPromo {
        fn on_append(
            &self,
            _: &TxContext<'_, '_>,
            committed: &[Envelope],
        ) -> Result<(), StoreError> {
            for env in committed {
                if let Event::Wave(WaveEvent::WorkerObserved {
                    role,
                    observation: Observation::TurnsSuppressed { keys, .. },
                }) = &env.event
                    && role.as_str() == "anchorpromo"
                    && keys.iter().any(|k| k == "o1")
                    && !self.0.swap(true, Ordering::SeqCst)
                {
                    return Err(StoreError::Backend("injected one failed append".into()));
                }
            }
            Ok(())
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_r23".into(),
        projects: vec![json!({
            "id": "uuid-r23",
            "title": "rex-wave",
            "workspaceRoot": "/tmp/rex-wave"
        })],
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_r23").unwrap();
    cfg.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg);
    let journal = Journal::open_with_hook(
        &tmp.path().join("journal.sqlite3"),
        Some(Arc::new(FailOnceAnchorPromo(AtomicBool::new(false)))),
    )
    .unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Registry::<Waves>::new(journal.clone(), hub.clone()),
        board: Registry::<Board>::new(journal.clone(), hub.clone()),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: bridge.clone(),
        }))
    });
    let host_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    let conductor_task = spawn_reactor(WaveConductor::new(T3Host(bridge.clone())), host_client);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let wave = WaveSlug::parse("r23-edge-v1").unwrap();
    let project = BbProjectId::parse("uuid-r23").unwrap();
    let prior_good_role = RoleName::parse("priorgood").unwrap();
    let anchor_promo_role = RoleName::parse("anchorpromo").unwrap();
    let anon_refine_role = RoleName::parse("anonrefine").unwrap();

    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse(
                    "Verify ordinary completion causal bound, prefixed anon refinement, and failed append retry",
                )
                .unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("antigravity").unwrap(),
                    model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![
                    WorkerSpec {
                        role: prior_good_role.clone(),
                        brief: Body::parse("Ordinary completion preserves untagged success before r1")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: anchor_promo_role.clone(),
                        brief: Body::parse("Retry anchor promotion cleanly after transient store failure")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: anon_refine_role.clone(),
                        brief: Body::parse("Refining anonymous ticket with r1 preserves earlier success")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                ],
            },
            idem: None,
        })
        .await
        .unwrap();

    let prior_tid = BbThreadId::parse("t3:r23-edge-v1:priorgood:a1").unwrap();
    let promo_tid = BbThreadId::parse("t3:r23-edge-v1:anchorpromo:a1").unwrap();
    let anon_tid = BbThreadId::parse("t3:r23-edge-v1:anonrefine:a1").unwrap();
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let prior_tid = prior_tid.clone();
        let promo_tid = promo_tid.clone();
        let anon_tid = anon_tid.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&prior_tid, &promo_tid, &anon_tid]
                .into_iter()
                .all(|want_tid| {
                    snap.workers.iter().any(|(_, _, p)| {
                        matches!(p, WorkerPhase::Live { thread, .. } if thread == want_tid)
                    })
                })
        }
    })
    .await;

    {
        let mut st = server.state.lock().await;
        for tid in [prior_tid.as_str(), promo_tid.as_str(), anon_tid.as_str()] {
            let proj = st.thread_projections.get_mut(tid).unwrap();
            proj["runs"] = json!([]);
            proj["mockUnavailable"] = json!(true);
        }
    }

    for (seq, tid, run_id) in [
        (10u64, prior_tid.as_str(), Some("r1")),
        (11u64, promo_tid.as_str(), Some("r1")),
        (12u64, anon_tid.as_str(), None),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": "safety",
                    "latestRunId": run_id
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let prior_good_role = prior_good_role.clone();
        let anchor_promo_role = anchor_promo_role.clone();
        let anon_refine_role = anon_refine_role.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&prior_good_role, &anchor_promo_role, &anon_refine_role]
                .into_iter()
                .all(|want| {
                    snap.workers.iter().any(|(r, _, p)| {
                        r == want
                            && matches!(
                                p,
                                WorkerPhase::Live {
                                    activity: Activity::Refused { .. },
                                    ..
                                }
                            )
                    })
                })
        }
    })
    .await;

    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 14,
            "location": "active",
            "thread": {
                "id": prior_tid.as_str(),
                "status": "completed",
                "stopReason": "safety",
                "latestRunId": "r2"
            }
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    {
        let mut st = server.state.lock().await;
        let p_proj = st.thread_projections.get_mut(prior_tid.as_str()).unwrap();
        p_proj["mockUnavailable"] = json!(false);
        p_proj["thread"]["status"] = json!("completed");
        p_proj["thread"]["stopReason"] = Value::Null;
        p_proj["turnItems"] = json!([
            { "type": "user_message", "text": "prior" },
            { "type": "assistant_message", "text": "Earlier success" },
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft", "runId": "r1" }
        ]);

        let a_proj = st.thread_projections.get_mut(promo_tid.as_str()).unwrap();
        a_proj["mockUnavailable"] = json!(false);
        a_proj["thread"]["status"] = json!("completed");
        a_proj["thread"]["stopReason"] = Value::Null;
        a_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft", "runId": "r1" }
        ]);

        let n_proj = st.thread_projections.get_mut(anon_tid.as_str()).unwrap();
        n_proj["mockUnavailable"] = json!(false);
        n_proj["thread"]["status"] = json!("completed");
        n_proj["thread"]["stopReason"] = Value::Null;
        n_proj["turnItems"] = json!([
            { "type": "user_message", "text": "prior" },
            { "type": "assistant_message", "text": "Earlier success" },
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft", "runId": "r1" }
        ]);
    }

    for (seq, tid, stop_reason) in [
        (16u64, prior_tid.as_str(), Value::Null),
        (17u64, promo_tid.as_str(), json!("safety")),
        (18u64, anon_tid.as_str(), json!("safety")),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": stop_reason,
                    "latestRunId": "r1"
                }
            }))
            .await;
    }
    tokio::time::sleep(Duration::from_millis(140)).await;

    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 19,
            "location": "active",
            "thread": {
                "id": promo_tid.as_str(),
                "status": "completed",
                "stopReason": "safety",
                "latestRunId": "r1"
            }
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(140)).await;

    {
        let mut st = server.state.lock().await;
        let p_proj = st.thread_projections.get_mut(prior_tid.as_str()).unwrap();
        p_proj["thread"]["status"] = json!("completed");
        p_proj["thread"]["stopReason"] = Value::Null;
        p_proj["turnItems"] = json!([
            { "type": "user_message", "text": "prior" },
            { "type": "assistant_message", "text": "Earlier success" },
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft", "runId": "r1" },
            { "type": "user_message", "text": "u2" },
            { "type": "assistant_message", "text": "Second refused draft" },
            { "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Recovered third turn" }
        ]);

        let a_proj = st.thread_projections.get_mut(promo_tid.as_str()).unwrap();
        a_proj["thread"]["status"] = json!("completed");
        a_proj["thread"]["stopReason"] = Value::Null;
        a_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft" },
            { "type": "user_message", "text": "u2" },
            { "type": "assistant_message", "text": "Second successful turn" },
            { "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Recovered third turn" }
        ]);

        let n_proj = st.thread_projections.get_mut(anon_tid.as_str()).unwrap();
        n_proj["thread"]["status"] = json!("completed");
        n_proj["thread"]["stopReason"] = Value::Null;
        n_proj["turnItems"] = json!([
            { "type": "user_message", "text": "prior" },
            { "type": "assistant_message", "text": "Earlier success" },
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft", "runId": "r1" },
            { "type": "user_message", "text": "u2" },
            { "type": "assistant_message", "text": "Second successful turn" },
            { "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Recovered third turn" }
        ]);
    }

    let _ = shutdown_tx.send(true);
    let _ = bridge_task.await;
    let mut cfg2 = T3Config::explicit(&server.origin(), "tok_r23").unwrap();
    cfg2.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg2);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    for (seq, tid) in [
        (20u64, prior_tid.as_str()),
        (21u64, promo_tid.as_str()),
        (22u64, anon_tid.as_str()),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "pendingRuntimeRequest": null,
                    "archivedAt": null
                }
            }))
            .await;
    }

    let room = wave_room(&wave, &project);
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let room = room.clone();
        let wave = wave.clone();
        let prior_good_role = prior_good_role.clone();
        let anchor_promo_role = anchor_promo_role.clone();
        let anon_refine_role = anon_refine_role.clone();
        async move {
            let Ok(page) = operator
                .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
                .await
            else {
                return false;
            };
            let count_for = |r: &RoleName| {
                page.entries
                    .iter()
                    .filter(|e| {
                        e.by == Principal::Worker {
                            wave: wave.clone(),
                            role: r.clone(),
                        } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                    })
                    .count()
            };
            count_for(&prior_good_role) == 2
                && count_for(&anchor_promo_role) == 2
                && count_for(&anon_refine_role) == 3
        }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let page = operator
        .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
        .await
        .unwrap();
    let bodies_for = |r: &RoleName| -> Vec<&str> {
        page.entries
            .iter()
            .filter(|e| {
                e.by == Principal::Worker {
                    wave: wave.clone(),
                    role: r.clone(),
                }
            })
            .filter_map(|e| match &e.event {
                Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(
        bodies_for(&prior_good_role),
        vec!["Earlier success", "Recovered third turn"]
    );
    assert_eq!(
        bodies_for(&anchor_promo_role),
        vec!["Second successful turn", "Recovered third turn"]
    );
    assert_eq!(
        bodies_for(&anon_refine_role),
        vec![
            "Earlier success",
            "Second successful turn",
            "Recovered third turn"
        ]
    );

    let _ = shutdown_tx.send(true);
    conductor_task.abort();
    bridge_task.abort();
}

#[tokio::test]
async fn t3_single_refusal_append_retry_and_named_outage_repeat_before_and_after_refinement() {
    use rex_wave_store::{CommitHook, StoreError, TxContext};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FailOnceSingleRefusal {
        failed_single: AtomicBool,
        failed_anon_ticket: AtomicBool,
    }
    impl CommitHook for FailOnceSingleRefusal {
        fn on_append(
            &self,
            _: &TxContext<'_, '_>,
            committed: &[Envelope],
        ) -> Result<(), StoreError> {
            for env in committed {
                if let Event::Wave(WaveEvent::WorkerObserved {
                    role,
                    observation: Observation::TurnsSuppressed { keys, .. },
                }) = &env.event
                {
                    if role.as_str() == "singlefail"
                        && keys.iter().any(|k| k == "o1")
                        && !self.failed_single.swap(true, Ordering::SeqCst)
                    {
                        return Err(StoreError::Backend("injected single failed append".into()));
                    }
                    if role.as_str() == "beforerefine"
                        && keys.iter().any(|k| k == "fail_ticket:1:any")
                        && !self.failed_anon_ticket.swap(true, Ordering::SeqCst)
                    {
                        return Err(StoreError::Backend(
                            "injected outage anonymous ticket failed append".into(),
                        ));
                    }
                }
            }
            Ok(())
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let server = MockT3Server::start(MockT3State {
        expected_bearer: "tok_r24".into(),
        projects: vec![json!({
            "id": "uuid-r24",
            "title": "rex-wave",
            "workspaceRoot": "/tmp/rex-wave"
        })],
        ..Default::default()
    })
    .await;

    let mut cfg = T3Config::explicit(&server.origin(), "tok_r24").unwrap();
    cfg.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg);
    let journal = Journal::open_with_hook(
        &tmp.path().join("journal.sqlite3"),
        Some(Arc::new(FailOnceSingleRefusal {
            failed_single: AtomicBool::new(false),
            failed_anon_ticket: AtomicBool::new(false),
        })),
    )
    .unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Registry::<Waves>::new(journal.clone(), hub.clone()),
        board: Registry::<Board>::new(journal.clone(), hub.clone()),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: bridge.clone(),
        }))
    });
    let host_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    let conductor_task = spawn_reactor(WaveConductor::new(T3Host(bridge.clone())), host_client);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    let operator = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let wave = WaveSlug::parse("r24-edge-v1").unwrap();
    let project = BbProjectId::parse("uuid-r24").unwrap();
    let single_fail_role = RoleName::parse("singlefail").unwrap();
    let before_refine_role = RoleName::parse("beforerefine").unwrap();
    let after_refine_role = RoleName::parse("afterrefine").unwrap();

    operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave.clone(),
                project: project.clone(),
                goal: Body::parse(
                    "Verify single-notification append retry and named outage repeat before/after refinement",
                )
                .unwrap(),
                defaults: AgentSpec {
                    provider: ProviderId::parse("antigravity").unwrap(),
                    model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Auto,
                    workspace: Workspace::ProjectDefault,
                },
                output_dir: None,
                workers: vec![
                    WorkerSpec {
                        role: single_fail_role.clone(),
                        brief: Body::parse("Retry single failed suppression append without duplicate shell")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: before_refine_role.clone(),
                        brief: Body::parse("Named repeat during outage refines pending anonymous ticket")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: after_refine_role.clone(),
                        brief: Body::parse("Named repeat during later outage skips already-bound run")
                            .unwrap(),
                        agent: None,
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                ],
            },
            idem: None,
        })
        .await
        .unwrap();

    let single_tid = BbThreadId::parse("t3:r24-edge-v1:singlefail:a1").unwrap();
    let before_tid = BbThreadId::parse("t3:r24-edge-v1:beforerefine:a1").unwrap();
    let after_tid = BbThreadId::parse("t3:r24-edge-v1:afterrefine:a1").unwrap();
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let single_tid = single_tid.clone();
        let before_tid = before_tid.clone();
        let after_tid = after_tid.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&single_tid, &before_tid, &after_tid]
                .into_iter()
                .all(|want_tid| {
                    snap.workers.iter().any(|(_, _, p)| {
                        matches!(p, WorkerPhase::Live { thread, .. } if thread == want_tid)
                    })
                })
        }
    })
    .await;

    {
        let mut st = server.state.lock().await;
        for tid in [before_tid.as_str(), after_tid.as_str()] {
            let proj = st.thread_projections.get_mut(tid).unwrap();
            proj["runs"] = json!([]);
            proj["mockUnavailable"] = json!(true);
        }
        let s_proj = st.thread_projections.get_mut(single_tid.as_str()).unwrap();
        s_proj["runs"] = json!([]);
        s_proj["mockUnavailable"] = json!(false);
        s_proj["thread"]["status"] = json!("completed");
        s_proj["thread"]["stopReason"] = Value::Null;
        s_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft", "runId": "r1" }
        ]);
    }

    for (seq, tid) in [(10u64, before_tid.as_str()), (11u64, after_tid.as_str())] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": "safety",
                    "latestRunId": null
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let before_refine_role = before_refine_role.clone();
        let after_refine_role = after_refine_role.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            [&before_refine_role, &after_refine_role]
                .into_iter()
                .all(|want| {
                    snap.workers.iter().any(|(r, _, p)| {
                        r == want
                            && matches!(
                                p,
                                WorkerPhase::Live {
                                    activity: Activity::Refused { .. },
                                    ..
                                }
                            )
                    })
                })
        }
    })
    .await;

    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 14,
            "location": "active",
            "thread": {
                "id": before_tid.as_str(),
                "status": "completed",
                "stopReason": "safety",
                "latestRunId": "r1"
            }
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    {
        let mut st = server.state.lock().await;
        for tid in [before_tid.as_str(), after_tid.as_str()] {
            let proj = st.thread_projections.get_mut(tid).unwrap();
            proj["mockUnavailable"] = json!(false);
            proj["thread"]["status"] = json!("completed");
            proj["thread"]["stopReason"] = Value::Null;
            proj["turnItems"] = json!([
                { "type": "user_message", "text": "prior" },
                { "type": "assistant_message", "text": "Earlier success" },
                { "type": "user_message", "text": "u1" },
                { "type": "assistant_message", "text": "First refused draft", "runId": "r1" }
            ]);
        }
    }

    for (seq, tid) in [
        (16u64, single_tid.as_str()),
        (17u64, before_tid.as_str()),
        (18u64, after_tid.as_str()),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "stopReason": "safety",
                    "latestRunId": "r1"
                }
            }))
            .await;
    }

    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let wave = wave.clone();
        let single_fail_role = single_fail_role.clone();
        async move {
            let Ok(snap) = operator.call(WaveView { wave }).await else {
                return false;
            };
            snap.workers.iter().any(|(r, _, p)| {
                r == &single_fail_role
                    && matches!(
                        p,
                        WorkerPhase::Live {
                            activity: Activity::Refused { .. },
                            ..
                        }
                    )
            })
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    {
        let mut st = server.state.lock().await;
        st.thread_projections.get_mut(after_tid.as_str()).unwrap()["mockUnavailable"] = json!(true);
    }
    server
        .push_shell_item(json!({
            "kind": "thread.updated",
            "sequence": 19,
            "location": "active",
            "thread": {
                "id": after_tid.as_str(),
                "status": "completed",
                "stopReason": "safety",
                "latestRunId": "r1"
            }
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    {
        let mut st = server.state.lock().await;
        let s_proj = st.thread_projections.get_mut(single_tid.as_str()).unwrap();
        s_proj["thread"]["status"] = json!("completed");
        s_proj["thread"]["stopReason"] = Value::Null;
        s_proj["turnItems"] = json!([
            { "type": "user_message", "text": "u1" },
            { "type": "assistant_message", "text": "First refused draft" },
            { "type": "user_message", "text": "u2" },
            { "type": "assistant_message", "text": "Second successful turn" },
            { "type": "user_message", "text": "u3" },
            { "type": "assistant_message", "text": "Recovered third turn" }
        ]);

        for tid in [before_tid.as_str(), after_tid.as_str()] {
            let proj = st.thread_projections.get_mut(tid).unwrap();
            proj["mockUnavailable"] = json!(false);
            proj["thread"]["status"] = json!("completed");
            proj["thread"]["stopReason"] = Value::Null;
            proj["turnItems"] = json!([
                { "type": "user_message", "text": "prior" },
                { "type": "assistant_message", "text": "Earlier success" },
                { "type": "user_message", "text": "u1" },
                { "type": "assistant_message", "text": "First refused draft" },
                { "type": "user_message", "text": "u2" },
                { "type": "assistant_message", "text": "Second successful turn" },
                { "type": "user_message", "text": "u3" },
                { "type": "assistant_message", "text": "Recovered third turn" }
            ]);
        }
    }

    let _ = shutdown_tx.send(true);
    let _ = bridge_task.await;
    let mut cfg2 = T3Config::explicit(&server.origin(), "tok_r24").unwrap();
    cfg2.reconnect_min = Duration::from_millis(15);
    let bridge = T3Bridge::new(cfg2);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bridge_task = tokio::spawn(bridge.clone().run_until(router.clone(), shutdown_rx));
    wait_until(Duration::from_secs(3), || async { bridge.connected() }).await;

    for (seq, tid) in [
        (21u64, single_tid.as_str()),
        (22u64, before_tid.as_str()),
        (23u64, after_tid.as_str()),
    ] {
        server
            .push_shell_item(json!({
                "kind": "thread.updated",
                "sequence": seq,
                "location": "active",
                "thread": {
                    "id": tid,
                    "status": "completed",
                    "pendingRuntimeRequest": null,
                    "archivedAt": null
                }
            }))
            .await;
    }

    let room = wave_room(&wave, &project);
    wait_until(Duration::from_secs(3), || {
        let operator = operator.clone();
        let room = room.clone();
        let wave = wave.clone();
        let single_fail_role = single_fail_role.clone();
        let before_refine_role = before_refine_role.clone();
        let after_refine_role = after_refine_role.clone();
        async move {
            let Ok(page) = operator
                .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
                .await
            else {
                return false;
            };
            let count_for = |r: &RoleName| {
                page.entries
                    .iter()
                    .filter(|e| {
                        e.by == Principal::Worker {
                            wave: wave.clone(),
                            role: r.clone(),
                        } && matches!(e.event, Event::Room(RoomEvent::Posted { .. }))
                    })
                    .count()
            };
            count_for(&single_fail_role) == 2
                && count_for(&before_refine_role) == 3
                && count_for(&after_refine_role) == 3
        }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let page = operator
        .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
        .await
        .unwrap();
    let bodies_for = |r: &RoleName| -> Vec<&str> {
        page.entries
            .iter()
            .filter(|e| {
                e.by == Principal::Worker {
                    wave: wave.clone(),
                    role: r.clone(),
                }
            })
            .filter_map(|e| match &e.event {
                Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(
        bodies_for(&single_fail_role),
        vec!["Second successful turn", "Recovered third turn"]
    );
    assert_eq!(
        bodies_for(&before_refine_role),
        vec![
            "Earlier success",
            "Second successful turn",
            "Recovered third turn"
        ]
    );
    assert_eq!(
        bodies_for(&after_refine_role),
        vec![
            "Earlier success",
            "Second successful turn",
            "Recovered third turn"
        ]
    );

    let _ = shutdown_tx.send(true);
    conductor_task.abort();
    bridge_task.abort();
}
