use rex_wave::daemon::{mint_seat_token, mint_worker_token};
use rex_wave::http::{HttpConfig, bind_http, serve_http_listener};
use rex_wave_api::board::{Priority, Title};
use rex_wave_api::presence::{
    BindingStatus, LivenessMode, PresenceResolution, PresenceSnapshot, SeatBinding, SeatRecord,
    SeatStatus, TransportEndpoint,
};
use rex_wave_api::room::{
    Body, Labeled, Posted, RoomId, RoomMessageSummary, TaskLease, TaskLeaseReceipt, TaskListView,
    TaskStatus, TaskView,
};
use rex_wave_api::*;
use rex_wave_notify::Inbox;
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    BoxFut, DecideCtx, Hub, NoAgentFeed, ReadSide, Registry, Router, Service, SidebarProjection,
    Unavailable,
};
use rex_wave_store::Journal;
use rex_wave_transport::{Authenticator, StateDirAuthenticator};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

mod common;
use common::fresh_dir;

struct HttpReply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpReply {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "invalid json (status={}): {e}: {}",
                self.status,
                String::from_utf8_lossy(&self.body)
            )
        })
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

async fn send_raw(addr: SocketAddr, raw: &str) -> HttpReply {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(raw.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let sep = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("HTTP response header terminator");
    let head = std::str::from_utf8(&buf[..sep]).unwrap();
    let body = buf[sep + 4..].to_vec();

    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut headers = Vec::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_owned(), v.trim().to_owned()));
        }
    }
    HttpReply {
        status,
        headers,
        body,
    }
}

fn build_request(
    method: &str,
    path: &str,
    addr: SocketAddr,
    extra_headers: &[(&str, &str)],
    body: Option<&str>,
) -> String {
    let mut out = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\n");
    for (k, v) in extra_headers {
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    if let Some(b) = body {
        out.push_str(&format!("Content-Length: {}\r\n", b.len()));
        out.push_str("\r\n");
        out.push_str(b);
    } else {
        out.push_str("\r\n");
    }
    out
}

fn real_router() -> Arc<Router> {
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Arc::new(Unavailable(Family::Wave)),
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
    })
}

#[tokio::test]
async fn bind_http_refuses_non_loopback_addresses() {
    let v4_any: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let err = bind_http(v4_any).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    let v6_any: SocketAddr = "[::]:0".parse().unwrap();
    let err6 = bind_http(v6_any).await.unwrap_err();
    assert_eq!(err6.kind(), std::io::ErrorKind::InvalidInput);

    let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = bind_http(loopback).await.unwrap();
    assert!(listener.local_addr().unwrap().ip().is_loopback());
}

#[tokio::test]
async fn http_security_guards_reject_bad_host_origin_sec_fetch_site_and_csrf() {
    let dir = fresh_dir("guards");
    let op = SeatName::parse("owner").unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(StateDirAuthenticator::from_state_dir(
        op.clone(),
        None,
        &dir.0,
    ));
    let listener = bind_http("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_http_listener(
        listener,
        real_router(),
        auth,
        op,
        HttpConfig::default(),
    ));

    let r = send_raw(addr, "GET /v1/health HTTP/1.1\r\n\r\n").await;
    assert_eq!(r.status, 400);

    let r = send_raw(
        addr,
        "GET /v1/health HTTP/1.1\r\nHost: evil.example:7782\r\n\r\n",
    )
    .await;
    assert_eq!(r.status, 403);

    let r = send_raw(
        addr,
        &build_request(
            "GET",
            "/v1/health",
            addr,
            &[("Origin", "https://evil.example")],
            None,
        ),
    )
    .await;
    assert_eq!(r.status, 403);

    let r = send_raw(
        addr,
        &build_request(
            "GET",
            "/v1/health",
            addr,
            &[("Origin", "http://127.0.0.1:1")],
            None,
        ),
    )
    .await;
    assert_eq!(r.status, 403);

    let matching_origin = format!("http://{addr}");
    let r = send_raw(
        addr,
        &build_request(
            "GET",
            "/v1/health",
            addr,
            &[("Origin", &matching_origin)],
            None,
        ),
    )
    .await;
    assert_eq!(r.status, 200);
    assert!(r.header("Access-Control-Allow-Origin").is_none());

    for bad in ["cross-site", "same-site"] {
        let r = send_raw(
            addr,
            &build_request("GET", "/v1/health", addr, &[("Sec-Fetch-Site", bad)], None),
        )
        .await;
        assert_eq!(r.status, 403, "expected 403 for Sec-Fetch-Site: {bad}");
    }

    for good in ["same-origin", "none"] {
        let r = send_raw(
            addr,
            &build_request("GET", "/v1/health", addr, &[("Sec-Fetch-Site", good)], None),
        )
        .await;
        assert_eq!(r.status, 200, "expected 200 for Sec-Fetch-Site: {good}");
    }

    let r = send_raw(
        addr,
        &build_request(
            "POST",
            "/v1/rooms/%23general/messages",
            addr,
            &[("Content-Type", "text/plain")],
            Some(r#"{"body":"hi"}"#),
        ),
    )
    .await;
    assert_eq!(r.status, 415);

    let r = send_raw(
        addr,
        &build_request(
            "POST",
            "/v1/rooms/%23general/messages",
            addr,
            &[("X-Rex-Wave", "1")],
            Some(r#"{"body":"via custom header"}"#),
        ),
    )
    .await;
    assert_eq!(r.status, 201);

    let r = send_raw(
        addr,
        &build_request(
            "POST",
            "/v1/rooms/%23general/messages",
            addr,
            &[("Content-Type", "application/json; charset=utf-8")],
            Some(r#"{"body":"via json"}"#),
        ),
    )
    .await;
    assert_eq!(r.status, 201);

    let r = send_raw(
        addr,
        &build_request("OPTIONS", "/v1/health", addr, &[], None),
    )
    .await;
    assert_eq!(r.status, 405);
    assert!(r.header("Access-Control-Allow-Origin").is_none());

    server.abort();
}

#[tokio::test]
async fn http_auth_pins_unauthenticated_to_operator_and_returns_404_on_invisible_dm_and_inbox() {
    let dir = fresh_dir("auth404");
    let sock = dir.0.join("daemon.sock");
    let op = SeatName::parse("owner").unwrap();
    let alice_tok = mint_seat_token(&sock, &SeatName::parse("alice").unwrap()).unwrap();
    let bob_tok = mint_seat_token(&sock, &SeatName::parse("bob").unwrap()).unwrap();
    let charlie_tok = mint_seat_token(&sock, &SeatName::parse("charlie").unwrap()).unwrap();
    let worker_tok = mint_worker_token(
        &sock,
        &WaveSlug::parse("w1").unwrap(),
        &RoleName::parse("author").unwrap(),
        None,
    )
    .unwrap();

    let auth: Arc<dyn Authenticator> = Arc::new(StateDirAuthenticator::from_state_dir(
        op.clone(),
        None,
        &dir.0,
    ));
    let listener = bind_http("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_http_listener(
        listener,
        real_router(),
        auth,
        op,
        HttpConfig::default(),
    ));

    let r = send_raw(
        addr,
        &build_request(
            "GET",
            "/v1/health?seat=alice&as=charlie",
            addr,
            &[("X-Seat", "alice"), ("X-Principal", "seat:alice")],
            None,
        ),
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(
        r.json()["principal"],
        json!({"kind": "operator", "seat": "owner"})
    );

    let r = send_raw(
        addr,
        &build_request(
            "GET",
            "/v1/health",
            addr,
            &[("Authorization", "Bearer invalid_token")],
            None,
        ),
    )
    .await;
    assert_eq!(r.status, 401);

    let worker_auth = format!("Bearer {worker_tok}");
    let r = send_raw(
        addr,
        &build_request(
            "GET",
            "/v1/health",
            addr,
            &[("Authorization", &worker_auth)],
            None,
        ),
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(
        r.json()["principal"],
        json!({"kind": "worker", "wave": "w1", "role": "author"})
    );

    let alice_auth = format!("Bearer {alice_tok}");
    let bob_auth = format!("Bearer {bob_tok}");
    let charlie_auth = format!("Bearer {charlie_tok}");
    let r = send_raw(
        addr,
        &build_request(
            "POST",
            "/v1/rooms/@bob/messages",
            addr,
            &[
                ("Authorization", &alice_auth),
                ("Content-Type", "application/json"),
            ],
            Some(r#"{"body":"secret for bob"}"#),
        ),
    )
    .await;
    assert_eq!(r.status, 201);

    let r = send_raw(
        addr,
        &build_request(
            "GET",
            "/v1/rooms/dm:@alice,@bob/messages",
            addr,
            &[("Authorization", &charlie_auth)],
            None,
        ),
    )
    .await;
    assert_eq!(r.status, 404);

    let r = send_raw(
        addr,
        &build_request(
            "GET",
            "/v1/events?room=dm:@alice,@bob",
            addr,
            &[("Authorization", &charlie_auth)],
            None,
        ),
    )
    .await;
    assert_eq!(r.status, 404);

    let r = send_raw(
        addr,
        &build_request(
            "GET",
            "/v1/inbox?owner=@alice",
            addr,
            &[("Authorization", &charlie_auth)],
            None,
        ),
    )
    .await;
    assert_eq!(r.status, 404);

    let r = send_raw(
        addr,
        &build_request(
            "GET",
            "/v1/events?stream=inbox:@alice",
            addr,
            &[("Authorization", &charlie_auth)],
            None,
        ),
    )
    .await;
    assert_eq!(r.status, 404);

    let r = send_raw(
        addr,
        &build_request(
            "GET",
            "/v1/events?interest=everything",
            addr,
            &[("Authorization", &charlie_auth)],
            None,
        ),
    )
    .await;
    assert_eq!(r.status, 404);

    let r = send_raw(
        addr,
        &build_request(
            "POST",
            "/v1/call/read",
            addr,
            &[
                ("Authorization", &charlie_auth),
                ("Content-Type", "application/json"),
            ],
            Some(r#"{"stream":{"stream":"inbox","owner":{"kind":"seat","seat":"alice"}}}"#),
        ),
    )
    .await;
    assert_eq!(r.status, 404);

    let r = send_raw(
        addr,
        &build_request(
            "GET",
            "/v1/rooms/dm:@alice,@bob/messages",
            addr,
            &[("Authorization", &bob_auth)],
            None,
        ),
    )
    .await;
    assert_eq!(r.status, 200);
    let entries = r.json()["entries"].as_array().unwrap().clone();
    assert!(
        entries
            .iter()
            .any(|e| { e["event"]["event"]["body"].as_str() == Some("secret for bob") })
    );

    server.abort();
}

struct StubPhase3Rooms;

impl Service<RoomCommand> for StubPhase3Rooms {
    fn dispatch(
        &self,
        cmd: RoomCommand,
        ctx: DecideCtx,
    ) -> BoxFut<'_, Result<WireReply, ApiError>> {
        Box::pin(async move {
            let room_id = RoomId::Channel {
                scope: rex_wave_api::room::RoomScope::Global,
                name: ChannelName::parse("general").unwrap(),
            };
            let sample_task = || TaskView {
                room: room_id.clone(),
                seq: Seq(1),
                org_id: OrgId::parse("rw-task1").unwrap(),
                rev: 2,
                title: Title::parse("Ship HTTP").unwrap(),
                status: TaskStatus::Claimed,
                priority: Priority::High,
                due_ms: None,
                needs: vec![],
                labels: vec![Label::parse("area=http").unwrap()],
                lease: Some(TaskLease {
                    holder: ctx.by.clone(),
                    epoch: LeaseEpoch(1),
                    claimed_at: UnixMs(100),
                    expires_at: UnixMs(60_100),
                }),
                ready: false,
                lease_expired: false,
                created_by: ctx.by.clone(),
                created_at: UnixMs(50),
                updated_at: UnixMs(100),
            };
            match cmd {
                RoomCommand::RoomPost(p) => {
                    if let Some(after) = p.after
                        && after < Seq(5)
                    {
                        return Err(ApiError::conflict_with(
                            "newer message exists",
                            ConflictData::NewerMessage {
                                expected: after,
                                latest: Seq(5),
                                newer_message: Box::new(RoomMessageSummary {
                                    seq: Seq(5),
                                    at: UnixMs(999),
                                    by: ctx.by.clone(),
                                    body: Body::parse("concurrent post").unwrap(),
                                    re: p.re,
                                    labels: vec![],
                                }),
                            },
                        ));
                    }
                    Ok(WireReply::RoomPost(Posted {
                        room: room_id,
                        seq: Seq(6),
                        mentions: Default::default(),
                        re: p.re,
                        org_id: None,
                        delivery_notice: None,
                    }))
                }
                RoomCommand::RoomLabel(l) => Ok(WireReply::RoomLabel(Labeled {
                    room: room_id,
                    seq: l.seq,
                    labels: l.add,
                })),
                RoomCommand::RoomTasks(_) => Ok(WireReply::RoomTasks(TaskListView {
                    room: room_id.clone(),
                    head: Seq(6),
                    tasks: vec![sample_task()],
                    total_count: 1,
                    truncated: false,
                })),
                RoomCommand::TaskClaim(c) => Ok(WireReply::TaskClaim(TaskLeaseReceipt {
                    room: room_id,
                    task: c.task,
                    org_id: OrgId::parse("rw-task1").unwrap(),
                    rev: 2,
                    epoch: LeaseEpoch(1),
                    expires_at: UnixMs(60_100),
                })),
                RoomCommand::TaskHeartbeat(h) => Ok(WireReply::TaskHeartbeat(TaskLeaseReceipt {
                    room: room_id,
                    task: h.task,
                    org_id: OrgId::parse("rw-task1").unwrap(),
                    rev: 2,
                    epoch: h.epoch,
                    expires_at: UnixMs(120_100),
                })),
                RoomCommand::TaskTransition(_) => Ok(WireReply::TaskTransition(sample_task())),
                RoomCommand::TaskUpdate(_) => Ok(WireReply::TaskUpdate(sample_task())),
                _ => Err(ApiError::Unavailable {
                    family: Family::Room,
                }),
            }
        })
    }
}

struct StubPresence;

impl Service<PresenceCommand> for StubPresence {
    fn dispatch(
        &self,
        cmd: PresenceCommand,
        ctx: DecideCtx,
    ) -> BoxFut<'_, Result<WireReply, ApiError>> {
        Box::pin(async move {
            let rec = SeatRecord {
                seat: SeatName::parse("sam").unwrap(),
                status: SeatStatus::Active,
                status_provenance: None,
                aliases: vec![],
                role: None,
                projects: vec![],
                home_room: None,
                retired_at: None,
                mail_horizon_at: None,
                work_class: None,
                avatar: None,
                home_dir: None,
                start_cmd: None,
                resume_cmd: None,
                rex_target: None,
                claimed_by: ctx.by,
                claimed_at: UnixMs(10),
            };
            let binding = SeatBinding {
                seat: SeatName::parse("sam").unwrap(),
                incarnation_id: IncarnationId::parse("inc_1").unwrap(),
                epoch: 1,
                lane: None,
                endpoint: TransportEndpoint::SelfWatcher {
                    harness: "codex".into(),
                    thread_id: None,
                    pid: None,
                    proc_start: None,
                },
                liveness: LivenessMode::LeaseOnly,
                bound_at: UnixMs(10),
                expires_at: Some(UnixMs(900_010)),
                status: BindingStatus::Live,
            };
            match cmd {
                PresenceCommand::PresenceList(_) => Ok(WireReply::PresenceList(PresenceSnapshot {
                    head: Seq(1),
                    seats: vec![rex_wave_api::presence::SeatPresenceView {
                        record: rec,
                        bindings: vec![binding],
                    }],
                    down_transports: vec![],
                })),
                PresenceCommand::PresenceResolve(_) => {
                    Ok(WireReply::PresenceResolve(PresenceResolution {
                        record: rec,
                        live_bindings: vec![binding],
                        last_live_at: Some(UnixMs(10)),
                    }))
                }
                _ => Err(ApiError::Unavailable {
                    family: Family::Presence,
                }),
            }
        })
    }
}

#[tokio::test]
async fn http_routes_cover_409_conflict_tasks_labels_presence_inbox_sidebar_and_call() {
    let dir = fresh_dir("routes");
    let op = SeatName::parse("owner").unwrap();
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(128);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Arc::new(StubPhase3Rooms),
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Arc::new(StubPresence),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });
    let auth: Arc<dyn Authenticator> = Arc::new(StateDirAuthenticator::from_state_dir(
        op.clone(),
        None,
        &dir.0,
    ));
    let listener = bind_http("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_http_listener(
        listener,
        router,
        auth,
        op,
        HttpConfig::default(),
    ));

    assert_eq!(
        send_raw(addr, &build_request("GET", "/v1/sidebar", addr, &[], None))
            .await
            .status,
        200
    );
    assert_eq!(
        send_raw(addr, &build_request("GET", "/v1/inbox", addr, &[], None))
            .await
            .status,
        200
    );
    assert_eq!(
        send_raw(
            addr,
            &build_request(
                "POST",
                "/v1/inbox/ack",
                addr,
                &[("Content-Type", "application/json")],
                Some(r#"{"ids":[],"state":"seen"}"#),
            ),
        )
        .await
        .status,
        200
    );
    let pres = send_raw(addr, &build_request("GET", "/v1/presence", addr, &[], None)).await;
    assert_eq!(pres.status, 200);
    assert_eq!(pres.json()["seats"][0]["record"]["seat"], "sam");
    let pres_res = send_raw(
        addr,
        &build_request("GET", "/v1/presence?resolve=sam", addr, &[], None),
    )
    .await;
    assert_eq!(pres_res.status, 200);
    assert_eq!(pres_res.json()["record"]["seat"], "sam");

    let conflict = send_raw(
        addr,
        &build_request(
            "POST",
            "/v1/rooms/%23general/messages",
            addr,
            &[("Content-Type", "application/json")],
            Some(r#"{"body":"stale","after":2}"#),
        ),
    )
    .await;
    assert_eq!(conflict.status, 409);
    let cjson = conflict.json();
    assert_eq!(cjson["error"], "conflict");
    assert_eq!(cjson["data"]["kind"], "newer_message");
    assert_eq!(cjson["data"]["latest"], 5);
    assert_eq!(cjson["data"]["newer_message"]["body"], "concurrent post");

    let lbl = send_raw(
        addr,
        &build_request(
            "PATCH",
            "/v1/rooms/%23general/messages/1/labels",
            addr,
            &[("Content-Type", "application/json")],
            Some(r#"{"add":["status=ready"]}"#),
        ),
    )
    .await;
    assert_eq!(lbl.status, 200);
    assert_eq!(lbl.json()["labels"], json!(["status=ready"]));

    assert_eq!(
        send_raw(
            addr,
            &build_request("GET", "/v1/rooms/%23general/tasks?ready=1", addr, &[], None),
        )
        .await
        .status,
        200
    );
    let claimed = send_raw(
        addr,
        &build_request(
            "POST",
            "/v1/rooms/%23general/tasks/1/claim",
            addr,
            &[("X-Rex-Wave", "1")],
            Some("{}"),
        ),
    )
    .await;
    assert_eq!(claimed.status, 200);
    assert_eq!(claimed.json()["epoch"], 1);

    assert_eq!(
        send_raw(
            addr,
            &build_request(
                "POST",
                "/v1/rooms/%23general/tasks/1/heartbeat",
                addr,
                &[("Content-Type", "application/json")],
                Some(r#"{"epoch":1,"ttl_ms":60000}"#),
            ),
        )
        .await
        .status,
        200
    );
    assert_eq!(
        send_raw(
            addr,
            &build_request(
                "POST",
                "/v1/rooms/%23general/tasks/1/transition",
                addr,
                &[("Content-Type", "application/json")],
                Some(r#"{"status":"done","epoch":1}"#),
            ),
        )
        .await
        .status,
        200
    );
    assert_eq!(
        send_raw(
            addr,
            &build_request(
                "PATCH",
                "/v1/rooms/%23general/tasks/1",
                addr,
                &[("Content-Type", "application/json")],
                Some(r#"{"expected_rev":2,"title":"Ship HTTP v2"}"#),
            ),
        )
        .await
        .status,
        200
    );

    let call_res = send_raw(
        addr,
        &build_request(
            "POST",
            "/v1/call/room_post",
            addr,
            &[("Content-Type", "application/json")],
            Some(r#"{"room":{"ref":"channel","scope":{"scope":"global"},"name":"general"},"body":"via call"}"#),
        ),
    )
    .await;
    assert_eq!(call_res.status, 200);
    assert_eq!(call_res.json()["reply"], "room_post");

    let forged_uds_bind = send_raw(
        addr,
        &build_request(
            "POST",
            "/v1/call/presence_bind",
            addr,
            &[("Content-Type", "application/json")],
            Some(
                r#"{"seat":"sam","endpoint":{"transport":"uds_session","conn_id":42,"client":"forged"},"liveness":"socket_lifetime"}"#,
            ),
        ),
    )
    .await;
    assert_eq!(
        forged_uds_bind.status, 400,
        "HTTP /v1/call/presence_bind must refuse UdsSession endpoints"
    );
    let forged_msg = forged_uds_bind.json()["detail"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(
        forged_msg.contains("Unix socket connection"),
        "expected UdsSession guard refusal, got: {forged_msg}"
    );

    server.abort();
}

async fn read_sse_event(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> (Option<String>, Option<String>, Option<String>, bool) {
    let mut id = None;
    let mut event = None;
    let mut data = None;
    let mut is_keepalive = false;
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await.unwrap();
        assert!(n > 0, "unexpected EOF while reading SSE event");
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            if id.is_some() || event.is_some() || data.is_some() || is_keepalive {
                return (id, event, data, is_keepalive);
            }
            continue;
        }
        if trimmed.starts_with(':') {
            is_keepalive = true;
        } else if let Some(v) = trimmed.strip_prefix("id:") {
            id = Some(v.trim().to_owned());
        } else if let Some(v) = trimmed.strip_prefix("event:") {
            event = Some(v.trim().to_owned());
        } else if let Some(v) = trimmed.strip_prefix("data:") {
            data = Some(v.trim().to_owned());
        }
    }
}

#[tokio::test]
async fn http_sse_streams_keepalive_and_resumes_seamlessly_via_last_event_id() {
    let dir = fresh_dir("sse");
    let op = SeatName::parse("owner").unwrap();
    let router = real_router();
    let auth: Arc<dyn Authenticator> = Arc::new(StateDirAuthenticator::from_state_dir(
        op.clone(),
        None,
        &dir.0,
    ));
    let listener = bind_http("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_http_listener(
        listener,
        router,
        auth,
        op,
        HttpConfig {
            keepalive_interval: Duration::from_millis(60),
        },
    ));

    for msg in ["first", "second"] {
        let body = format!(r#"{{"body":"{msg}"}}"#);
        let r = send_raw(
            addr,
            &build_request(
                "POST",
                "/v1/rooms/%23general/messages",
                addr,
                &[("Content-Type", "application/json")],
                Some(&body),
            ),
        )
        .await;
        assert_eq!(r.status, 201);
    }

    let stream1 = TcpStream::connect(addr).await.unwrap();
    let (r1, mut w1) = stream1.into_split();
    w1.write_all(build_request("GET", "/v1/events?room=%23general", addr, &[], None).as_bytes())
        .await
        .unwrap();
    let mut br1 = BufReader::new(r1);
    let mut header_line = String::new();
    br1.read_line(&mut header_line).await.unwrap();
    assert!(header_line.starts_with("HTTP/1.1 200"));
    let mut saw_sse_ct = false;
    loop {
        header_line.clear();
        br1.read_line(&mut header_line).await.unwrap();
        if header_line.trim().is_empty() {
            break;
        }
        if header_line
            .to_ascii_lowercase()
            .starts_with("content-type: text/event-stream")
        {
            saw_sse_ct = true;
        }
    }
    assert!(saw_sse_ct, "expected text/event-stream content type");

    let mut first_post_cursor: Option<String> = None;
    let mut saw_keepalive = false;
    let mut caught_up = false;
    tokio::time::timeout(Duration::from_secs(3), async {
        while !(caught_up && saw_keepalive) {
            let (id, ev, data, keepalive) = read_sse_event(&mut br1).await;
            if keepalive {
                saw_keepalive = true;
                continue;
            }
            assert_eq!(ev.as_deref(), Some("frame"));
            let val: Value = serde_json::from_str(data.as_deref().unwrap()).unwrap();
            if val["frame"] == "committed"
                && val["event"]["event"]["body"].as_str() == Some("first")
            {
                first_post_cursor = id;
            }
            if val["frame"] == "caught_up" {
                caught_up = true;
            }
        }
    })
    .await
    .expect("received initial frames and keepalive");

    let resume_id = first_post_cursor.expect("captured cursor id after first post");
    drop(br1);
    drop(w1);

    let r3 = send_raw(
        addr,
        &build_request(
            "POST",
            "/v1/rooms/%23general/messages",
            addr,
            &[("Content-Type", "application/json")],
            Some(r#"{"body":"third"}"#),
        ),
    )
    .await;
    assert_eq!(r3.status, 201);

    let stream2 = TcpStream::connect(addr).await.unwrap();
    let (r2, mut w2) = stream2.into_split();
    w2.write_all(
        build_request(
            "GET",
            "/v1/events?room=%23general",
            addr,
            &[("Last-Event-ID", &resume_id)],
            None,
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut br2 = BufReader::new(r2);
    let mut hdr = String::new();
    loop {
        hdr.clear();
        br2.read_line(&mut hdr).await.unwrap();
        if hdr.trim().is_empty() {
            break;
        }
    }

    let mut resumed_bodies = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (_, _, data, keepalive) = read_sse_event(&mut br2).await;
            if keepalive {
                continue;
            }
            let val: Value = serde_json::from_str(data.as_deref().unwrap()).unwrap();
            if val["frame"] == "committed"
                && let Some(body) = val["event"]["event"]["body"].as_str()
            {
                resumed_bodies.push(body.to_owned());
            }
            if val["frame"] == "caught_up" {
                break;
            }
        }
    })
    .await
    .expect("resumed stream caught up");

    assert_eq!(
        resumed_bodies,
        vec!["second".to_owned(), "third".to_owned()]
    );

    server.abort();
}

#[tokio::test]
async fn http_disconnect_cancels_worker_wait_and_restores_working() {
    use rex_wave_api::wave::{
        Activity, AgentSpec, Observation, PermissionMode, WaveObserve, WaveSpec, WaveStart,
        WaveView, WorkerPhase, WorkerSpec, Workspace,
    };
    use rex_wave_runtime::LocalClient;
    use rex_wave_waves::Waves;

    let dir = fresh_dir("wait-cancel");
    let sock = dir.0.join("daemon.sock");
    let op = SeatName::parse("owner").unwrap();
    let wave = WaveSlug::parse("w1").unwrap();
    let role = RoleName::parse("author").unwrap();
    let worker_tok = mint_worker_token(&sock, &wave, &role, None).unwrap();

    let journal = Journal::in_memory().unwrap();
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

    let op_client = LocalClient::new(router.clone(), Principal::Operator { seat: op.clone() });
    let sys_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::T3Bridge,
        },
    );
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
                goal: Body::parse("disconnect cancel test").unwrap(),
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
                thread: BbThreadId::parse("t3:w1:author:a1").unwrap(),
            },
        })
        .await
        .unwrap();

    let auth: Arc<dyn Authenticator> = Arc::new(StateDirAuthenticator::from_state_dir(
        op.clone(),
        None,
        &dir.0,
    ));
    let listener = bind_http("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_http_listener(
        listener,
        router,
        auth,
        op,
        HttpConfig::default(),
    ));

    let mut conn = TcpStream::connect(addr).await.unwrap();
    let wait_body = r#"{"interest":{"interest":"room","room":{"ref":"channel","scope":{"scope":"global"},"name":"general"}},"after":{"positions":[],"from_head":true},"timeout_ms":30000}"#;
    let bearer = format!("Bearer {worker_tok}");
    let req = build_request(
        "POST",
        "/v1/call/wait",
        addr,
        &[
            ("Authorization", &bearer),
            ("Content-Type", "application/json"),
        ],
        Some(wait_body),
    );
    conn.write_all(req.as_bytes()).await.unwrap();
    conn.flush().await.unwrap();

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snap = op_client
                .call(WaveView { wave: wave.clone() })
                .await
                .unwrap();
            if snap.workers.iter().any(|(r, _, phase)| {
                r == &role
                    && matches!(
                        phase,
                        WorkerPhase::Live {
                            activity: Activity::Parked { .. },
                            ..
                        }
                    )
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("HTTP worker wait entered Parked");

    drop(conn);

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snap = op_client
                .call(WaveView { wave: wave.clone() })
                .await
                .unwrap();
            if snap.workers.iter().any(|(r, _, phase)| {
                r == &role
                    && matches!(
                        phase,
                        WorkerPhase::Live {
                            activity: Activity::Working,
                            ..
                        }
                    )
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("closing HTTP connection cancelled worker Wait and restored Working");

    server.abort();
}
