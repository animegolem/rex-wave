#![allow(clippy::disallowed_methods)]
use rex_wave_api::board::{BoardId, BoardView, project_board};
use rex_wave_api::mail::{LetterPriority, MailAck, MailPromote, MailSchemaV1};
use rex_wave_api::notify::{InboxList, NotifyKind, NotifyState};
use rex_wave_api::read::{InboxBadge, Read, Wait};
use rex_wave_api::room::{Body, RoomEvent, RoomId, RoomJoin, RoomOrigin, RoomRef};
use rex_wave_api::stage::{Osc7501BlockKind, Osc7501State, derive_osc7501_status};
use rex_wave_api::wave::{
    Activity, AgentSpec, PermissionMode, WaveFinish, WaveOutcome, WaveSpec, WaveStart, WaveView,
    WorkerPhase, WorkerSpec, Workspace,
};
use rex_wave_api::*;
use rex_wave_transport::{Credential, Endpoint, RemoteClient};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::process::{Child, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};

mod common;
use common::{fresh_dir, rex_wave, rex_wave_at, t3_home};

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h0: u32 = 0x6745_2301;
    let mut h1: u32 = 0xEFCD_AB89;
    let mut h2: u32 = 0x98BA_DCFE;
    let mut h3: u32 = 0x1032_5476;
    let mut h4: u32 = 0xC3D2_E1F0;

    let bit_len = (data.len() as u64) * 8;
    let mut padded = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.chunks_exact(64) {
        let mut w = [0u32; 80];
        for (i, word) in chunk.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let (mut a, mut b, mut c, mut d, mut e) = (h0, h1, h2, h3, h4);
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    let mut out = [0u8; 20];
    out[0..4].copy_from_slice(&h0.to_be_bytes());
    out[4..8].copy_from_slice(&h1.to_be_bytes());
    out[8..12].copy_from_slice(&h2.to_be_bytes());
    out[12..16].copy_from_slice(&h3.to_be_bytes());
    out[16..20].copy_from_slice(&h4.to_be_bytes());
    out
}

fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 0x3F) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3F) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[((triple >> 6) & 0x3F) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(triple & 0x3F) as usize] as char
        } else {
            '='
        });
    }
    out
}

async fn accept_ws(mut stream: TcpStream, expected_token: &str) -> Option<(TcpStream, Vec<u8>)> {
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    let end = loop {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > 16 * 1024 {
            return None;
        }
    };
    let leftover = buf.split_off(end);
    let header_str = std::str::from_utf8(&buf).ok()?;
    let mut ws_key = None;
    let mut token = None;
    for line in header_str.split("\r\n").skip(1) {
        if let Some((name, val)) = line.split_once(':') {
            let name = name.trim();
            let val = val.trim();
            if name.eq_ignore_ascii_case("sec-websocket-key") {
                ws_key = Some(val.to_owned());
            } else if name.eq_ignore_ascii_case("x-bb-plugin-token") {
                token = Some(val.to_owned());
            }
        }
    }
    if token.as_deref() != Some(expected_token) {
        let _ = stream
            .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n")
            .await;
        return None;
    }
    let key = ws_key?;
    let mut combined = key.into_bytes();
    combined.extend_from_slice(WS_GUID.as_bytes());
    let accept = base64_encode(&sha1(&combined));
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream.write_all(resp.as_bytes()).await.ok()?;
    Some((stream, leftover))
}

struct WsServerReader {
    reader: tokio::io::ReadHalf<TcpStream>,
    buf: Vec<u8>,
    pos: usize,
}

impl WsServerReader {
    async fn read_exact_bytes(&mut self, out: &mut [u8]) -> Option<()> {
        let mut written = 0;
        while written < out.len() {
            if self.pos < self.buf.len() {
                let avail = self.buf.len() - self.pos;
                let take = avail.min(out.len() - written);
                out[written..written + take].copy_from_slice(&self.buf[self.pos..self.pos + take]);
                self.pos += take;
                written += take;
            } else {
                self.buf.clear();
                self.pos = 0;
                let mut tmp = [0u8; 2048];
                let n = self.reader.read(&mut tmp).await.ok()?;
                if n == 0 {
                    return None;
                }
                self.buf.extend_from_slice(&tmp[..n]);
            }
        }
        Some(())
    }

    async fn recv_json(&mut self) -> Option<Value> {
        loop {
            let mut hdr = [0u8; 2];
            self.read_exact_bytes(&mut hdr).await?;
            let opcode = hdr[0] & 0x0F;
            let masked = (hdr[1] & 0x80) != 0;
            let len7 = (hdr[1] & 0x7F) as usize;
            let payload_len = match len7 {
                126 => {
                    let mut ext = [0u8; 2];
                    self.read_exact_bytes(&mut ext).await?;
                    u16::from_be_bytes(ext) as usize
                }
                127 => {
                    let mut ext = [0u8; 8];
                    self.read_exact_bytes(&mut ext).await?;
                    u64::from_be_bytes(ext) as usize
                }
                n => n,
            };
            let mask_key = if masked {
                let mut k = [0u8; 4];
                self.read_exact_bytes(&mut k).await?;
                Some(k)
            } else {
                None
            };
            let mut payload = vec![0u8; payload_len];
            if payload_len > 0 {
                self.read_exact_bytes(&mut payload).await?;
                if let Some(k) = mask_key {
                    for (i, b) in payload.iter_mut().enumerate() {
                        *b ^= k[i & 3];
                    }
                }
            }
            if opcode == 0x8 {
                return None;
            }
            if opcode == 0x1 {
                return serde_json::from_slice(&payload).ok();
            }
        }
    }
}

async fn ws_send_json(
    writer: &mut tokio::io::WriteHalf<TcpStream>,
    val: &Value,
) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(val).expect("valid json");
    let mut frame = Vec::with_capacity(bytes.len() + 10);
    frame.push(0x81);
    if bytes.len() < 126 {
        frame.push(bytes.len() as u8);
    } else if bytes.len() <= u16::MAX as usize {
        frame.push(126);
        frame.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    } else {
        frame.push(127);
        frame.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    }
    frame.extend_from_slice(&bytes);
    writer.write_all(&frame).await?;
    writer.flush().await
}

#[derive(Default)]
struct MockBbState {
    connected: bool,
    kv: HashMap<String, String>,
    spawned: Vec<Value>,
    meta_by_thread: HashMap<String, Value>,
    sends: Vec<(String, String, String)>,
    resolves: Vec<(String, String, Value)>,
    retired: BTreeSet<String>,
    tool_results: HashMap<String, (bool, String)>,
}

fn free_loopback_addr() -> SocketAddr {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback probe");
    probe.local_addr().expect("local_addr")
}

async fn http_post(
    addr: SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, String) {
    let mut req = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nContent-Length: {}\r\nConnection: close\r\n",
        addr.port(),
        body.len()
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");

    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect loopback http");
    let _ = stream.write_all(req.as_bytes()).await;
    let _ = stream.write_all(body).await;
    let mut raw = Vec::new();
    let mut buf = [0u8; 1024];
    let header_end = loop {
        let n = stream.read(&mut buf).await.expect("read http headers");
        assert!(n > 0, "unexpected EOF reading HTTP headers");
        raw.extend_from_slice(&buf[..n]);
        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = std::str::from_utf8(&raw[..header_end]).expect("utf8 http headers");
    let status: u16 = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .expect("http status code");
    let content_len: usize = head
        .lines()
        .find_map(|line| {
            let (k, v) = line.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse().ok())
                .flatten()
        })
        .unwrap_or(0);
    while raw.len() < header_end + content_len {
        let n = stream.read(&mut buf).await.expect("read http body");
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&buf[..n]);
    }
    let resp_body =
        String::from_utf8(raw[header_end..header_end + content_len].to_vec()).expect("utf8 body");
    (status, resp_body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_stack_daemon_wave_mcp_codemode_inbox_mail_wakebell_and_tui_e2e() {
    let dir_guard = fresh_dir("e2e-full");
    let dir = dir_guard.0.clone();

    let sock_path = dir.join("d.sock");
    let sandbox_dir = dir.join("mail-sandbox");
    std::fs::create_dir_all(&sandbox_dir).expect("create sandbox dir");
    std::fs::write(sandbox_dir.join(".rex-wave-sandbox"), b"e2e-sandbox\n")
        .expect("write sandbox marker");
    // `rex_wave_at` gives the daemon an empty protected-roots file (nothing to
    // protect) and an empty T3 home, so the BB-only runner never sees live state.
    let t3_home = t3_home(&dir);

    let bb_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock bb");
    let bb_addr = bb_listener.local_addr().unwrap();
    let bb_token = "bb_tok_e2e";
    let bb_state = Arc::new(Mutex::new(MockBbState::default()));
    let (up_tx, up_rx) = mpsc::unbounded_channel::<Value>();

    let bb_server_state = bb_state.clone();
    let bb_server = tokio::spawn(async move {
        let mut up_rx = up_rx;
        while let Ok((tcp, _)) = bb_listener.accept().await {
            let Some((ws_tcp, leftover)) = accept_ws(tcp, bb_token).await else {
                continue;
            };
            let (r_half, mut w_half) = tokio::io::split(ws_tcp);
            let mut reader = WsServerReader {
                reader: r_half,
                buf: leftover,
                pos: 0,
            };
            if ws_send_json(
                &mut w_half,
                &json!({
                    "t": "hello",
                    "plugin_version": "0.1.0",
                    "sdk_version": "0.6.132",
                }),
            )
            .await
            .is_err()
            {
                continue;
            }
            bb_server_state.lock().await.connected = true;

            loop {
                tokio::select! {
                    Some(up) = up_rx.recv() => {
                        if ws_send_json(&mut w_half, &up).await.is_err() {
                            break;
                        }
                    }
                    maybe_down = reader.recv_json() => {
                        let Some(down) = maybe_down else { break };
                        let tag = down.get("t").and_then(Value::as_str).unwrap_or("");
                        let req_id = down.get("req_id").and_then(Value::as_u64).unwrap_or(0);
                        let mut st = bb_server_state.lock().await;
                        match tag {
                            "find_by_meta" => {
                                let m = &down["meta"];
                                let key = format!(
                                    "spawn:{}/{}/{}",
                                    m["waveId"].as_str().unwrap_or(""),
                                    m["role"].as_str().unwrap_or(""),
                                    m["attempt"].as_u64().unwrap_or(0),
                                );
                                let found = st.kv.get(&key).cloned();
                                drop(st);
                                let _ = ws_send_json(
                                    &mut w_half,
                                    &json!({ "t": "result", "req_id": req_id, "ok": true, "value": { "thread_id": found } }),
                                ).await;
                            }
                            "spawn" => {
                                let args = down["args"].clone();
                                let meta = args["pluginMetadata"]["rexWave"].clone();
                                let wave_id = meta["waveId"].as_str().unwrap().to_owned();
                                let role = meta["role"].as_str().unwrap().to_owned();
                                let attempt = meta["attempt"].as_u64().unwrap();
                                let tid = format!("th_{wave_id}_{role}");
                                st.kv.insert(format!("spawn:{wave_id}/{role}/{attempt}"), tid.clone());
                                st.meta_by_thread.insert(tid.clone(), meta);
                                st.spawned.push(args);
                                drop(st);
                                let _ = ws_send_json(
                                    &mut w_half,
                                    &json!({ "t": "result", "req_id": req_id, "ok": true, "value": { "thread_id": tid } }),
                                ).await;
                            }
                            "send" => {
                                let tid = down["thread_id"].as_str().unwrap().to_owned();
                                let mode = down["mode"].as_str().unwrap().to_owned();
                                let text = down["text"].as_str().unwrap().to_owned();
                                st.sends.push((tid, mode, text));
                                drop(st);
                                let _ = ws_send_json(
                                    &mut w_half,
                                    &json!({ "t": "result", "req_id": req_id, "ok": true, "value": { "delivery": "sent" } }),
                                ).await;
                            }
                            "resolve" => {
                                let tid = down["thread_id"].as_str().unwrap().to_owned();
                                let iid = down["interaction_id"].as_str().unwrap().to_owned();
                                let res = down["resolution"].clone();
                                st.resolves.push((tid, iid, res));
                                drop(st);
                                let _ = ws_send_json(
                                    &mut w_half,
                                    &json!({ "t": "result", "req_id": req_id, "ok": true, "value": null }),
                                ).await;
                            }
                            "retire" => {
                                let tid = down["thread_id"].as_str().unwrap().to_owned();
                                st.retired.insert(tid);
                                drop(st);
                                let _ = ws_send_json(
                                    &mut w_half,
                                    &json!({ "t": "result", "req_id": req_id, "ok": true, "value": null }),
                                ).await;
                            }
                            "events_page" => {
                                drop(st);
                                let _ = ws_send_json(
                                    &mut w_half,
                                    &json!({ "t": "result", "req_id": req_id, "ok": true, "value": [] }),
                                ).await;
                            }
                            "thread_context" => {
                                drop(st);
                                let _ = ws_send_json(
                                    &mut w_half,
                                    &json!({
                                        "t": "result",
                                        "req_id": req_id,
                                        "ok": true,
                                        "value": {
                                            "used_tokens": 18_000,
                                            "model_context_window": 200_000,
                                            "estimated": false,
                                            "categories": []
                                        }
                                    }),
                                ).await;
                            }
                            "tool_result" => {
                                let cid = down["call_id"].as_str().unwrap().to_owned();
                                let ok = down["ok"].as_bool().unwrap_or(false);
                                let text = down["text"].as_str().unwrap_or("").to_owned();
                                st.tool_results.insert(cid, (ok, text));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    });

    let sink_addr = free_loopback_addr();
    let daemon_child = rex_wave_at(&sock_path)
        .args([
            "--socket",
            sock_path.to_str().unwrap(),
            "daemon",
            "--mail-sandbox",
            sandbox_dir.to_str().unwrap(),
            "--bb-url",
            &format!("ws://{bb_addr}/api/v1/plugins/rex-wave/http/bridge"),
            "--bb-token",
            bb_token,
            "--runner",
            "bb",
            "--t3-home",
            t3_home.to_str().unwrap(),
            "--operator",
            "owner",
            "--wake-bell",
            &sink_addr.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rex-wave daemon");
    let _daemon_guard = ChildGuard(daemon_child);

    let operator = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if bb_state.lock().await.connected
                && TcpStream::connect(sink_addr).await.is_ok()
                && let Ok(c) = RemoteClient::connect(
                    Endpoint::Unix(sock_path.clone()),
                    Credential::PeerUid,
                    "operator-e2e",
                )
                .await
            {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("daemon UDS, mock BB bridge, and wake-bell sink came online");

    let mut sidebar_stream = operator
        .watch(Interest::Sidebar, Cursor::live())
        .await
        .expect("open live sidebar stream");

    let wave_slug = WaveSlug::parse("auth-v2").unwrap();
    let project_id = BbProjectId::parse("proj_e2e").unwrap();
    let builder_role = RoleName::parse("builder").unwrap();
    let verifier_role = RoleName::parse("verifier").unwrap();

    let default_agent = AgentSpec {
        provider: ProviderId::parse("codex").unwrap(),
        model: Some(ModelId::parse("gpt-6.1-sol").unwrap()),
        account: None,
        reasoning: Some("xhigh".into()),
        service_tier: None,
        permission: PermissionMode::AcceptEdits,
        workspace: Workspace::ProjectDefault,
    };
    let verifier_agent = AgentSpec {
        provider: ProviderId::parse("claude-code").unwrap(),
        model: Some(ModelId::parse("claude-opus-5-5").unwrap()),
        account: None,
        reasoning: Some("xhigh".into()),
        service_tier: None,
        permission: PermissionMode::AcceptEdits,
        workspace: Workspace::ProjectDefault,
    };

    let started = operator
        .call(WaveStart {
            spec: WaveSpec {
                slug: wave_slug.clone(),
                project: project_id.clone(),
                goal: Body::parse("Ship token rotation").unwrap(),
                defaults: default_agent.clone(),
                output_dir: None,
                workers: vec![
                    WorkerSpec {
                        role: builder_role.clone(),
                        brief: Body::parse("Implement refresh rotation").unwrap(),
                        agent: Some(default_agent),
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    },
                    WorkerSpec {
                        role: verifier_role.clone(),
                        brief: Body::parse("Verify token rotation invariants").unwrap(),
                        agent: Some(verifier_agent),
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
        .expect("wave_start succeeded");

    let wave_room = started.room.clone();
    let expected_board: BoardId = project_board(&project_id);
    assert_eq!(started.board, expected_board);

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let spawned_len = bb_state.lock().await.spawned.len();
            let snap = operator
                .call(WaveView {
                    wave: wave_slug.clone(),
                })
                .await
                .unwrap();
            let all_live = snap.workers.len() == 2
                && snap
                    .workers
                    .iter()
                    .all(|(_, _, p)| matches!(p, WorkerPhase::Live { .. }));
            let page = operator
                .call(Read::stream(
                    StreamId::Room {
                        room: wave_room.clone(),
                    },
                    Seq::ORIGIN,
                    Some(50),
                ))
                .await
                .unwrap();
            let posted_count = page
                .entries
                .iter()
                .filter(|e| matches!(e.event, Event::Room(RoomEvent::Posted { .. })))
                .count();
            if spawned_len == 2 && all_live && posted_count >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("WaveConductor created room, board, briefs, and 2 hidden BB worker threads");

    {
        let st = bb_state.lock().await;
        assert_eq!(st.spawned.len(), 2);
        for spawn_arg in &st.spawned {
            assert_eq!(spawn_arg["visibility"], "hidden");
            assert_eq!(spawn_arg["projectId"], "proj_e2e");
        }
    }
    let board_initial = operator
        .call(BoardView {
            board: expected_board.clone(),
            filter: None,
        })
        .await
        .expect("project Kanban board auto-created");
    assert_eq!(board_initial.board, expected_board);
    assert!(
        operator
            .call(Read::stream(
                StreamId::Room {
                    room: wave_room.clone(),
                },
                Seq::ORIGIN,
                Some(5),
            ))
            .await
            .unwrap()
            .entries
            .iter()
            .any(|e| matches!(
                &e.event,
                Event::Room(RoomEvent::Opened {
                    origin: RoomOrigin::Wave { wave },
                    ..
                }) if wave == &wave_slug
            )),
        "#wave-<slug> opened with RoomOrigin::Wave"
    );

    let (builder_meta, verifier_meta) = {
        let st = bb_state.lock().await;
        (
            st.meta_by_thread["th_auth-v2_builder"].clone(),
            st.meta_by_thread["th_auth-v2_verifier"].clone(),
        )
    };

    up_tx
        .send(json!({
            "t": "thread",
            "signal": "idle",
            "thread_id": "th_auth-v2_verifier",
            "project_id": "proj_e2e",
            "meta": verifier_meta,
            "error": null,
        }))
        .unwrap();

    let join_reply = operator
        .call(RoomJoin {
            room: RoomRef::Id {
                id: wave_room.clone(),
            },
            member: Some(Principal::Seat {
                seat: SeatName::parse("sam").unwrap(),
            }),
            handle: None,
        })
        .await
        .expect("operator invited @sam to wave room");
    let _ = join_reply;
    let room_after_invite = operator
        .call(Read::stream(
            StreamId::Room {
                room: wave_room.clone(),
            },
            Seq::ORIGIN,
            Some(50),
        ))
        .await
        .unwrap()
        .entries
        .last()
        .unwrap()
        .seq;

    let mut builder_wait = Wait::room(wave_room.clone(), room_after_invite, false);
    builder_wait.timeout_ms = Some(15_000);
    up_tx
        .send(json!({
            "t": "tool_call",
            "call_id": "call_wait_builder_1",
            "thread_id": "th_auth-v2_builder",
            "project_id": "proj_e2e",
            "meta": builder_meta,
            "tool": "wait",
            "args": serde_json::to_value(&builder_wait).unwrap(),
        }))
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snap = operator
                .call(WaveView {
                    wave: wave_slug.clone(),
                })
                .await
                .unwrap();
            let builder_parked = snap.workers.iter().any(|(r, _, p)| {
                r == &builder_role
                    && matches!(
                        p,
                        WorkerPhase::Live {
                            activity: Activity::Parked { .. },
                            ..
                        }
                    )
            });
            let verifier_idle = snap.workers.iter().any(|(r, _, p)| {
                r == &verifier_role
                    && matches!(
                        p,
                        WorkerPhase::Live {
                            activity: Activity::Idle,
                            ..
                        }
                    )
            });
            if builder_parked && verifier_idle {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("builder reached Parked and verifier reached Idle");

    let tok_output = rex_wave_at(&sock_path)
        .args([
            "--socket",
            sock_path.to_str().unwrap(),
            "token",
            "--seat",
            "sam",
        ])
        .output()
        .expect("mint seat token");
    assert!(tok_output.status.success());
    let sam_token = String::from_utf8(tok_output.stdout)
        .unwrap()
        .trim()
        .to_owned();
    assert!(sam_token.starts_with("rw_"));

    let mcp_addr = free_loopback_addr();
    let mcp_child = rex_wave_at(&sock_path)
        .args([
            "--socket",
            sock_path.to_str().unwrap(),
            "mcp",
            "--http",
            &mcp_addr.to_string(),
            "--token",
            &sam_token,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rex-wave mcp --http");
    let _mcp_guard = ChildGuard(mcp_child);

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if TcpStream::connect(mcp_addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("MCP HTTP server came online");

    let init_payload = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {},
    }))
    .unwrap();

    let (no_auth_code, _) = http_post(
        mcp_addr,
        "/mcp",
        &[("Content-Type", "application/json")],
        &init_payload,
    )
    .await;
    assert_eq!(
        no_auth_code, 401,
        "HTTP MCP rejects unauthenticated loopback requests without bearer token"
    );

    let (bad_auth_code, _) = http_post(
        mcp_addr,
        "/mcp",
        &[
            ("Content-Type", "application/json"),
            ("Authorization", "Bearer rw_wrong_token"),
        ],
        &init_payload,
    )
    .await;
    assert_eq!(
        bad_auth_code, 401,
        "HTTP MCP rejects mismatched bearer token"
    );

    let bearer_hdr = format!("Bearer {sam_token}");
    let run_code_script = r#"
        const board = "project:proj_e2e";
        const c1 = await rex.card_upsert({
            board,
            patch: {
                title: "Verify refresh rotation",
                lane: "todo",
                assignee: { assign: "to", who: "@auth-v2/verifier" },
                priority: "high"
            }
        });
        const c2 = await rex.card_upsert({
            board,
            patch: {
                title: "Audit rotation telemetry",
                lane: "doing",
                assignee: { assign: "to", who: "@owner" },
                priority: "normal"
            }
        });
        const waveRoom = {
            ref: "channel",
            scope: { scope: "project", project: "proj_e2e" },
            name: "wave-auth-v2"
        };
        const posted = await rex.room_post({
            room: waveRoom,
            body: "@verifier slice #1 ready for review"
        });
        return { who: rex.whoami(), c1: c1.no, c2: c2.no, postSeq: posted.seq };
    "#;
    let run_code_rpc = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "run_code",
            "arguments": { "code": run_code_script }
        }
    }))
    .unwrap();

    let (mcp_status, mcp_body) = http_post(
        mcp_addr,
        "/mcp",
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &bearer_hdr),
        ],
        &run_code_rpc,
    )
    .await;
    assert_eq!(mcp_status, 200);
    let mcp_json: Value = serde_json::from_str(&mcp_body).expect("mcp json");
    assert_eq!(
        mcp_json["result"]["isError"], false,
        "mcp error: {mcp_json}"
    );
    let outcome_text = mcp_json["result"]["content"][0]["text"]
        .as_str()
        .expect("run_code outcome text");
    let outcome_val: Value = serde_json::from_str(outcome_text).unwrap();
    assert_eq!(
        outcome_val["value"]["who"],
        json!({ "kind": "seat", "seat": "sam" })
    );
    assert_eq!(outcome_val["value"]["c1"], 1);
    assert_eq!(outcome_val["value"]["c2"], 2);
    let post_seq = Seq(outcome_val["value"]["postSeq"].as_u64().unwrap());

    let wait_reply_text = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let st = bb_state.lock().await;
            let got_wait = st.tool_results.get("call_wait_builder_1").cloned();
            let has_verifier_doorbell = st.sends.iter().any(|(tid, mode, txt)| {
                tid == "th_auth-v2_verifier"
                    && mode == "auto"
                    && txt.contains("@verifier slice #1 ready for review")
            });
            if let Some((ok, text)) = got_wait
                && has_verifier_doorbell
            {
                assert!(ok, "builder wait failed: {text}");
                break text;
            }
            drop(st);
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("Parked builder wait returned AND Idle verifier received send(mode:auto) doorbell");

    assert!(wait_reply_text.contains("@verifier slice #1 ready for review"));
    assert!(
        !bb_state
            .lock()
            .await
            .sends
            .iter()
            .any(|(tid, _, _)| tid == "th_auth-v2_builder"),
        "Parked worker received zero redundant threads.send doorbells"
    );

    let mut attempt = 0u32;
    let worker_code_json = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            attempt += 1;
            let cid = format!("call_worker_run_code_{attempt}");
            up_tx
                .send(json!({
                    "t": "tool_call",
                    "call_id": cid,
                    "thread_id": "th_auth-v2_verifier",
                    "project_id": "proj_e2e",
                    "meta": verifier_meta,
                    "tool": "run_code",
                    "args": {
                        "code": "const ib = await rex.inbox_list({}); const bv = await rex.board_view({ board: 'project:proj_e2e' }); return { me: rex.whoami(), unread: ib.unread, kinds: ib.items.map(i => i.draft.kind.kind), cards: bv.lanes.reduce((n, l) => n + l.cards.length, 0) };"
                    }
                }))
                .unwrap();
            let (ok, txt) = loop {
                if let Some(res) = bb_state.lock().await.tool_results.remove(&cid) {
                    break res;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            assert!(ok, "BB worker run_code failed: {txt}");
            let parsed: Value = serde_json::from_str(&txt).unwrap();
            let kinds = parsed["value"]["kinds"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            if kinds.iter().any(|k| k.as_str() == Some("mention"))
                && kinds.iter().any(|k| k.as_str() == Some("card_assigned"))
            {
                break parsed;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("BB wave worker executed run_code and observed Herald mention + card_assigned cards");

    assert_eq!(
        worker_code_json["value"]["me"],
        json!({ "kind": "worker", "wave": "auth-v2", "role": "verifier" })
    );
    assert_eq!(worker_code_json["value"]["cards"], 2);

    up_tx
        .send(json!({
            "t": "interaction",
            "thread_id": "th_auth-v2_verifier",
            "meta": verifier_meta,
            "interaction": {
                "id": "int_perm_42",
                "payload": {
                    "kind": "approval",
                    "reason": "Run database migration"
                }
            }
        }))
        .unwrap();

    let allow_cmd = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let inbox = operator
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            let snap = operator
                .call(WaveView {
                    wave: wave_slug.clone(),
                })
                .await
                .unwrap();
            let phases: Vec<WorkerPhase> = snap.workers.into_iter().map(|(_, _, p)| p).collect();
            let blocking_count = inbox
                .items
                .iter()
                .filter(|i| i.state == NotifyState::Unread && matches!(i.draft.kind, NotifyKind::Interaction { .. }))
                .count() as u32;
            let osc = derive_osc7501_status(
                &InboxBadge {
                    unread: inbox.unread,
                    blocking: blocking_count,
                },
                &phases,
            );
            if osc.state == Osc7501State::Blocked
                && osc.block_kind == Some(Osc7501BlockKind::Permission)
                && let Some(card) = inbox.items.into_iter().find(|i| {
                    i.state == NotifyState::Unread
                        && matches!(
                            &i.draft.kind,
                            NotifyKind::Interaction { interaction, .. } if interaction.as_str() == "int_perm_42"
                        )
                })
            {
                let action = card
                    .draft
                    .actions
                    .into_iter()
                    .find(|a| a.label == "Allow once")
                    .expect("Allow once action on permission card");
                break *action.run;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("Inbox raised approval card and derive_osc7501_status entered blocked:permission");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(Ok(Frame::Sidebar(snap))) =
                std::future::poll_fn(|cx| sidebar_stream.as_mut().poll_next(cx)).await
                && snap.osc7501.state == Osc7501State::Blocked
                && snap.osc7501.block_kind == Some(Osc7501BlockKind::Permission)
                && snap.inbox.blocking >= 1
            {
                break;
            }
        }
    })
    .await
    .expect("SidebarProjection emitted blocked:permission with blocking inbox badge");

    operator
        .call_wire(allow_cmd)
        .await
        .expect("execute embedded WireCommand from Inbox card");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if bb_state
                .lock()
                .await
                .resolves
                .iter()
                .any(|(tid, iid, res)| {
                    tid == "th_auth-v2_verifier"
                        && iid == "int_perm_42"
                        && res["decision"] == "allow_once"
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("WaveConductor forwarded AllowOnce resolution to BB");

    up_tx
        .send(json!({
            "t": "thread",
            "signal": "active",
            "thread_id": "th_auth-v2_verifier",
            "project_id": "proj_e2e",
            "meta": verifier_meta,
            "error": null,
        }))
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let inbox = operator
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            let still_blocking = inbox
                .items
                .iter()
                .filter(|i| {
                    i.state == NotifyState::Unread
                        && matches!(i.draft.kind, NotifyKind::Interaction { .. })
                })
                .count() as u32;
            let snap = operator
                .call(WaveView {
                    wave: wave_slug.clone(),
                })
                .await
                .unwrap();
            let phases: Vec<WorkerPhase> = snap.workers.into_iter().map(|(_, _, p)| p).collect();
            let osc = derive_osc7501_status(
                &InboxBadge {
                    unread: inbox.unread,
                    blocking: still_blocking,
                },
                &phases,
            );
            if still_blocking == 0 && osc.state == Osc7501State::Working && osc.block_kind.is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("derive_osc7501_status flipped from blocked:permission to working");

    let sam_seat = SeatName::parse("sam").unwrap();
    let letter = operator
        .call(MailPromote {
            room: wave_room.clone(),
            range: SeqRange::new(Seq(1), post_seq).unwrap(),
            to: vec![sam_seat.clone()],
            subject: "auth-v2 slice 1 ratified".into(),
            topic: "auth-v2-slice-1".into(),
            priority: LetterPriority::High,
        })
        .await
        .expect("promote_to_letter succeeded");

    assert!(
        letter.path.is_relative() && letter.path.starts_with("to-sam"),
        "unexpected letter path: {}",
        letter.path.display()
    );
    let letter_on_disk = sandbox_dir.join(&letter.path);
    let letter_disk =
        std::fs::read_to_string(&letter_on_disk).expect("read banked Markdown letter");
    assert!(letter_disk.contains("auth-v2 slice 1 ratified"));
    assert!(letter_disk.contains("@verifier slice #1 ready for review"));
    assert_eq!(letter.item.schema, MailSchemaV1);
    assert_eq!(letter.item.to, "sam");
    assert_eq!(letter.item.kind, "letter");
    assert_eq!(letter.item.state, "open");
    assert_eq!(letter.item.topic.as_deref(), Some("auth-v2-slice-1"));
    assert_eq!(letter.item.reference.sha256, letter.sha256);

    let sam_client = RemoteClient::connect(
        Endpoint::Unix(sock_path.clone()),
        Credential::Token {
            token: sam_token.clone(),
        },
        "sam-e2e",
    )
    .await
    .expect("connect as sam seat");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let sam_inbox = sam_client
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            if sam_inbox.items.iter().any(|i| {
                matches!(
                    &i.draft.kind,
                    NotifyKind::LetterBanked { letter: l } if l.sha256 == letter.sha256
                )
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("MailBridge::with_hub published Banked to live Hub for Herald");

    sam_client
        .call(MailAck {
            path: letter.path.clone(),
            sha256: letter.sha256.clone(),
        })
        .await
        .expect("MailAck verified exact on-disk SHA-256");

    let bell_payload = serde_json::to_vec(&json!({
        "bell_id": "nightly-rotation-audit",
        "seat": "sam",
        "prompt": "Audit refresh token rotation telemetry.",
        "fired_at": "2026-11-17T08:00",
    }))
    .unwrap();

    for _ in 0..3 {
        let (code, body) = http_post(
            sink_addr,
            "/sink",
            &[("Content-Type", "application/json")],
            &bell_payload,
        )
        .await;
        assert_eq!(code, 200, "sink failed: {body}");
    }

    let dm_room: RoomId = RoomRef::Dm {
        with: Principal::System {
            part: SystemPart::WakeBell,
        },
    }
    .resolve(sam_client.me())
    .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let page = sam_client
                .call(Read::stream(
                    StreamId::Room {
                        room: dm_room.clone(),
                    },
                    Seq::ORIGIN,
                    Some(20),
                ))
                .await
                .unwrap();
            let posts: Vec<_> = page
                .entries
                .iter()
                .filter(|e| matches!(e.event, Event::Room(RoomEvent::Posted { .. })))
                .collect();
            if !posts.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("BellCourier delivered live wake-bell DM");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let dm_after_settle = sam_client
        .call(Read::stream(
            StreamId::Room { room: dm_room },
            Seq::ORIGIN,
            Some(20),
        ))
        .await
        .unwrap();
    let dm_bodies: Vec<&str> = dm_after_settle
        .entries
        .iter()
        .filter_map(|e| match &e.event {
            Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        dm_bodies,
        vec!["Audit refresh token rotation telemetry."],
        "wake-bell retries deduplicated to exactly one DM"
    );

    operator
        .call(WaveFinish {
            wave: wave_slug,
            outcome: WaveOutcome::Completed {
                summary: Body::parse("Rotation verified end to end").unwrap(),
            },
            allow_incomplete: false,
        })
        .await
        .expect("finish wave");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let retired = bb_state.lock().await.retired.clone();
            if retired.contains("th_auth-v2_builder") && retired.contains("th_auth-v2_verifier") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("both BB worker threads retired cleanly");

    let self_test = rex_wave(&dir)
        .args(["tui", "--self-test"])
        .output()
        .expect("run rex-wave tui --self-test");
    assert!(
        self_test.status.success(),
        "tui --self-test failed: {}",
        String::from_utf8_lossy(&self_test.stderr)
    );
    let tui_out = String::from_utf8_lossy(&self_test.stdout);
    assert!(tui_out.contains("Org Tree"));
    assert!(tui_out.contains("Kitty z=-1 SDF RGBA"));
    assert!(tui_out.contains("U+10EEEE Context Bar"));
    assert!(tui_out.contains("OSC 7501 blocked->working->clear OK"));
    assert!(tui_out.contains("SELF-TEST PASSED"));

    bb_server.abort();
}
