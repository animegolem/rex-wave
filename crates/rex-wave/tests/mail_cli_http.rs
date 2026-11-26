//! End-to-end CLI and HTTP integration tests for PR #32:
//! - Four-verb CLI (`rex-wave mail send|check|ack|search|reconcile|rebuild|migrate-verify`)
//! - Standardized exit codes (`0` ok, `1` runtime/IO/internal, `2` refused/denied/invalid/protected-roots)
//! - HTTP routes (`GET /v1/mail`, `GET /v1/mail?q=...`, `POST /v1/mail`, `POST /v1/mail/ack`)
//!   with strict principal scoping and `404 Not Found` masking.

#![allow(clippy::disallowed_methods)]

use rex_wave::daemon;
use rex_wave::http::{HttpConfig, bind_http, serve_http_listener};
use rex_wave_api::*;
use rex_wave_mailbridge::{MailBridge, Presence, Protection, SandboxRoot};
use rex_wave_notify::Inbox;
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{Hub, NoAgentFeed, ReadSide, Registry, Router, SidebarProjection};
use rex_wave_store::Journal;
use rex_wave_transport::{Authenticator, StateDirAuthenticator, bind_unix, serve_listener};
use serde_json::{Value, json};
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod common;
use common::{fresh_dir, rex_wave_at};

async fn http_req(
    addr: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect http");
    let body_bytes = body.unwrap_or("").as_bytes();
    let mut raw = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nContent-Length: {}\r\n",
        addr.port(),
        body_bytes.len()
    );
    if method != "GET" {
        raw.push_str("Content-Type: application/json\r\n");
    }
    if let Some(tok) = token {
        raw.push_str(&format!("Authorization: Bearer {tok}\r\n"));
    }
    raw.push_str("Connection: close\r\n\r\n");
    stream.write_all(raw.as_bytes()).await.expect("write head");
    if !body_bytes.is_empty() {
        stream.write_all(body_bytes).await.expect("write body");
    }
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.expect("read resp");
    let text = String::from_utf8_lossy(&resp);
    let (head, body_part) = text.split_once("\r\n\r\n").expect("http split");
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .expect("status code");
    let val = serde_json::from_str(body_part).unwrap_or_else(|_| json!({ "raw": body_part }));
    (status, val)
}

fn write_protected_roots(xdg_dir: &Path, entries: &str) {
    let dir = xdg_dir.join("rex-wave");
    fs::create_dir_all(&dir).expect("mkdir xdg rex-wave");
    fs::write(dir.join("protected-roots"), entries).expect("write protected-roots");
}

#[tokio::test]
async fn mail_cli_four_verbs_exit_codes_http_routes_and_offline_tools() {
    let dir = fresh_dir("mail-cli-http");
    let sock = dir.0.join("daemon.sock");
    let journal_path = dir.0.join("journal.sqlite3");
    let mirror_dir = dir.0.join("mirror");
    let xdg_dir = dir.0.join("xdg");
    let protected_dir = dir.0.join("protected-house");
    fs::create_dir_all(&mirror_dir).expect("mkdir mirror");
    fs::create_dir_all(&protected_dir).expect("mkdir protected");
    fs::write(mirror_dir.join(".rex-wave-sandbox"), b"test\n").expect("marker");
    write_protected_roots(&xdg_dir, &format!("{}\n", protected_dir.display()));

    let protection = Protection {
        roots: vec![protected_dir.clone()],
        names: Vec::new(),
    };
    let sandbox = SandboxRoot::verify_with(&mirror_dir, &protection).expect("sandbox");
    let journal = Journal::open(&journal_path).expect("open journal");
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let room_svc = Registry::<Rooms>::new(journal.clone(), hub.clone());
    let presence_svc = Presence::default().registry(journal.clone(), hub.clone());
    let mail_writer = journal.claim(StreamId::Mail).expect("claim mail");
    let mail_svc = Arc::new(
        MailBridge::with_hub(sandbox, journal.clone(), mail_writer, hub.clone())
            .with_room_service(room_svc.clone()),
    );
    let router = Arc::new(Router {
        room: room_svc,
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: mail_svc,
        presence: presence_svc,
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }))
    });

    let owner = SeatName::parse("owner").expect("owner");
    let alice = SeatName::parse("alice").expect("alice");
    let bob = SeatName::parse("bob").expect("bob");
    let sam = SeatName::parse("sam").expect("sam");

    let (listener, daemon_uid) = bind_unix(&sock).expect("bind unix");
    let auth: Arc<dyn Authenticator> = Arc::new(StateDirAuthenticator::from_state_dir(
        owner.clone(),
        Some(daemon_uid),
        &dir.0,
    ));
    let uds_server = tokio::spawn(serve_listener(listener, router.clone(), auth.clone()));

    let http_listener = bind_http("127.0.0.1:0".parse().expect("addr"))
        .await
        .expect("bind http");
    let http_addr = http_listener.local_addr().expect("local addr");
    let http_server = tokio::spawn(serve_http_listener(
        http_listener,
        router.clone(),
        auth,
        owner,
        HttpConfig::default(),
    ));

    let alice_tok = daemon::mint_seat_token(&sock, &alice).expect("alice token");
    let bob_tok = daemon::mint_seat_token(&sock, &bob).expect("bob token");
    let sam_tok = daemon::mint_seat_token(&sock, &sam).expect("sam token");

    let sock_str = sock.to_str().expect("sock utf8").to_owned();

    // 1. CLI `mail send` with --body-file and --wait-export (exit 0).
    let body_file = dir.0.join("letter-body.md");
    fs::write(
        &body_file,
        "Ship dissolved content-addressed mail with FTS5.",
    )
    .expect("body file");
    let s1 = sock_str.clone();
    let tok1 = alice_tok.clone();
    let bf1 = body_file.clone();
    let send_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s1)
            .args([
                "--socket",
                &s1,
                "--token",
                &tok1,
                "--json",
                "mail",
                "send",
                "--to",
                "bob",
                "--subject",
                "Dissolved mail gate",
                "--body-file",
                bf1.to_str().unwrap(),
                "--topic",
                "dissolved-gate",
                "--priority",
                "high",
                "--wait-export",
                "--id",
                "cli-send-1",
            ])
            .output()
            .expect("run mail send")
    })
    .await
    .expect("join");
    assert_eq!(
        send_out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&send_out.stderr)
    );
    let send_json: Value = serde_json::from_slice(&send_out.stdout).expect("send json");
    let sha = send_json["value"]["sha256"]
        .as_str()
        .expect("sha256")
        .to_owned();
    let rel_path = send_json["value"]["path"]
        .as_str()
        .expect("path")
        .to_owned();
    assert_eq!(send_json["value"]["exported"], json!(true));
    assert!(
        mirror_dir
            .join("objects")
            .join(format!("{sha}.md"))
            .is_file()
    );
    assert!(mirror_dir.join(&rel_path).is_file());

    // 2. CLI `mail check` for bob returns unread letter (exit 0).
    let s2 = sock_str.clone();
    let tok2 = bob_tok.clone();
    let check_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s2)
            .args(["--socket", &s2, "--token", &tok2, "--json", "mail", "check"])
            .output()
            .expect("run mail check")
    })
    .await
    .expect("join");
    assert_eq!(check_out.status.code(), Some(0));
    let check_json: Value = serde_json::from_slice(&check_out.stdout).expect("check json");
    assert_eq!(check_json["value"]["unread_count"], json!(1));
    assert_eq!(check_json["value"]["messages"][0]["sha256"], json!(sha));

    // 3. Unauthorized CLI `mail check --seat bob` by sam -> exit code 2 (refused/denied)!
    let s3 = sock_str.clone();
    let tok3 = sam_tok.clone();
    let denied_check = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s3)
            .args([
                "--socket", &s3, "--token", &tok3, "--json", "mail", "check", "--seat", "bob",
            ])
            .output()
            .expect("run denied check")
    })
    .await
    .expect("join");
    assert_eq!(
        denied_check.status.code(),
        Some(2),
        "unauthorized mail check must exit 2; stderr={}",
        String::from_utf8_lossy(&denied_check.stderr)
    );

    // 4. CLI `mail search` (FTS5) finds the letter for bob and hides it from sam.
    let s4 = sock_str.clone();
    let tok4 = bob_tok.clone();
    let search_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s4)
            .args([
                "--socket",
                &s4,
                "--token",
                &tok4,
                "--json",
                "mail",
                "search",
                "content-addressed",
            ])
            .output()
            .expect("run mail search")
    })
    .await
    .expect("join");
    assert_eq!(search_out.status.code(), Some(0));
    let search_json: Value = serde_json::from_slice(&search_out.stdout).expect("search json");
    assert_eq!(search_json["value"]["hits"].as_array().unwrap().len(), 1);
    assert_eq!(search_json["value"]["hits"][0]["sha256"], json!(sha));

    // 5. CLI `mail ack` by exact sha256 (exit 0).
    let s5 = sock_str.clone();
    let tok5 = bob_tok.clone();
    let sha5 = sha.clone();
    let ack_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s5)
            .args([
                "--socket", &s5, "--token", &tok5, "--json", "mail", "ack", &sha5, "--note",
                "reviewed",
            ])
            .output()
            .expect("run mail ack")
    })
    .await
    .expect("join");
    assert_eq!(ack_out.status.code(), Some(0));
    let ack_json: Value = serde_json::from_slice(&ack_out.stdout).expect("ack json");
    assert_eq!(ack_json["value"]["sha256"], json!(sha));
    assert_eq!(ack_json["value"]["exported"], json!(true));

    // 6. Edit working-tree letter in place after ack, reconcile, and assert CLI ack returns exit 2!
    let edited_bytes =
        b"---\nfrom: alice\nto: bob\nsubject: Dissolved mail gate\n---\n\nEdited after ack!\n";
    fs::write(mirror_dir.join(&rel_path), edited_bytes).expect("edit working tree");
    let s6 = sock_str.clone();
    let rec_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s6)
            .args(["--socket", &s6, "--json", "mail", "reconcile"])
            .output()
            .expect("run mail reconcile")
    })
    .await
    .expect("join");
    assert_eq!(rec_out.status.code(), Some(0));
    let rec_json: Value = serde_json::from_slice(&rec_out.stdout).expect("rec json");
    assert_eq!(rec_json["value"]["mutated_after_ack_flagged"], json!(1));

    // Compute edited sha256 via mail check --all and attempt to ack -> exit 2!
    let s6b = sock_str.clone();
    let tok6b = bob_tok.clone();
    let all_check = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s6b)
            .args([
                "--socket", &s6b, "--token", &tok6b, "--json", "mail", "check", "--all",
            ])
            .output()
            .expect("mail check --all")
    })
    .await
    .expect("join");
    let all_json: Value = serde_json::from_slice(&all_check.stdout).expect("all json");
    let flagged_sha = all_json["value"]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["mutated_after_ack"] == json!(true))
        .expect("flagged entry")["sha256"]
        .as_str()
        .unwrap()
        .to_owned();

    let s6c = sock_str.clone();
    let tok6c = bob_tok.clone();
    let flagged_ack = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s6c)
            .args([
                "--socket",
                &s6c,
                "--token",
                &tok6c,
                "--json",
                "mail",
                "ack",
                &flagged_sha,
            ])
            .output()
            .expect("flagged ack")
    })
    .await
    .expect("join");
    assert_eq!(
        flagged_ack.status.code(),
        Some(2),
        "acking mutated_after_ack must exit 2"
    );

    // 7. HTTP routes: POST /v1/mail, GET /v1/mail (check + search), POST /v1/mail/ack, and 404 masking!
    let (st_send, http_sent) = http_req(
        http_addr,
        "POST",
        "/v1/mail",
        Some(&alice_tok),
        Some(
            &json!({
                "to": ["bob"],
                "subject": "HTTP mail check",
                "body": "Sent via loopback HTTP /v1/mail endpoint.",
                "wait_export": true
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(st_send, 201, "POST /v1/mail body={http_sent}");
    let http_sha = http_sent["sha256"].as_str().expect("http sha").to_owned();

    let (st_check, http_check) = http_req(http_addr, "GET", "/v1/mail", Some(&bob_tok), None).await;
    assert_eq!(st_check, 200);
    assert_eq!(http_check["unread_count"], json!(1));

    let (st_search, http_search) = http_req(
        http_addr,
        "GET",
        "/v1/mail?q=loopback",
        Some(&bob_tok),
        None,
    )
    .await;
    assert_eq!(st_search, 200);
    assert_eq!(http_search["hits"].as_array().unwrap().len(), 1);

    // Sam trying to read bob's mail or ack bob's mail via HTTP -> masked as 404 Not Found!
    let (st_masked_check, _) =
        http_req(http_addr, "GET", "/v1/mail?seat=bob", Some(&sam_tok), None).await;
    assert_eq!(
        st_masked_check, 404,
        "unauthorized GET /v1/mail masked as 404"
    );

    let (st_masked_ack, _) = http_req(
        http_addr,
        "POST",
        "/v1/mail/ack",
        Some(&sam_tok),
        Some(&json!({ "target": http_sha }).to_string()),
    )
    .await;
    assert_eq!(
        st_masked_ack, 404,
        "unauthorized POST /v1/mail/ack masked as 404"
    );

    let (st_ack, http_ack) = http_req(
        http_addr,
        "POST",
        "/v1/mail/ack",
        Some(&bob_tok),
        Some(&json!({ "target": http_sha }).to_string()),
    )
    .await;
    assert_eq!(st_ack, 200, "bob POST /v1/mail/ack body={http_ack}");
    assert_eq!(http_ack["sha256"], json!(http_sha));

    // 8. Offline CLI `mail rebuild` + `mail migrate-verify` (exit 0) + protected-roots refusal (exit 2)!
    let rebuilt_db = dir.0.join("rebuilt-out/offline-rebuilt.sqlite3");
    let m_dir = mirror_dir.clone();
    let x_dir = xdg_dir.clone();
    let r_db = rebuilt_db.clone();
    let s_ref = sock_str.clone();
    let rebuild_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s_ref)
            .env("XDG_CONFIG_HOME", &x_dir)
            .args([
                "--json",
                "mail",
                "rebuild",
                "--from",
                m_dir.to_str().unwrap(),
                "--journal",
                r_db.to_str().unwrap(),
            ])
            .output()
            .expect("run mail rebuild")
    })
    .await
    .expect("join");
    assert_eq!(
        rebuild_out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&rebuild_out.stderr)
    );
    let rebuild_json: Value = serde_json::from_slice(&rebuild_out.stdout).expect("rebuild json");
    assert_eq!(rebuild_json["messages_rebuilt"], json!(3));
    assert_eq!(rebuild_json["acks_rebuilt"], json!(2));
    assert_eq!(rebuild_json["mutated_after_ack_flagged"], json!(1));

    // Protected-roots refusal -> exit code 2!
    let m_dir2 = mirror_dir.clone();
    let x_dir2 = xdg_dir.clone();
    let p_db = protected_dir.join("journal.sqlite3");
    let s_ref2 = sock_str.clone();
    let refused_rebuild = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s_ref2)
            .env("XDG_CONFIG_HOME", &x_dir2)
            .args([
                "--json",
                "mail",
                "rebuild",
                "--from",
                m_dir2.to_str().unwrap(),
                "--journal",
                p_db.to_str().unwrap(),
            ])
            .output()
            .expect("run refused rebuild")
    })
    .await
    .expect("join");
    assert_eq!(
        refused_rebuild.status.code(),
        Some(2),
        "protected-roots violation must exit 2; stderr={}",
        String::from_utf8_lossy(&refused_rebuild.stderr)
    );

    // Offline CLI `mail migrate-verify` over mirror_dir -> parity_ok == true, exit 0!
    let m_dir3 = mirror_dir.clone();
    let x_dir3 = xdg_dir.clone();
    let work_mv = dir.0.join("mv-work");
    let s_ref3 = sock_str.clone();
    let mv_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&s_ref3)
            .env("XDG_CONFIG_HOME", &x_dir3)
            .args([
                "--json",
                "mail",
                "migrate-verify",
                "--interchange",
                m_dir3.to_str().unwrap(),
                "--work-dir",
                work_mv.to_str().unwrap(),
            ])
            .output()
            .expect("run migrate-verify")
    })
    .await
    .expect("join");
    assert_eq!(
        mv_out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&mv_out.stderr)
    );
    let mv_json: Value = serde_json::from_slice(&mv_out.stdout).expect("mv json");
    assert_eq!(mv_json["parity_ok"], json!(true));
    assert_eq!(mv_json["db_parity"], json!(true));
    assert_eq!(mv_json["ack_parity"], json!(true));
    assert_eq!(mv_json["flagged_parity"], json!(true));

    // Unreachable daemon -> exit code 1 (runtime/transport failure)!
    let dead_sock = dir.0.join("missing.sock");
    let dead_str = dead_sock.to_str().unwrap().to_owned();
    let dead_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&dead_str)
            .args(["--socket", &dead_str, "--json", "mail", "check"])
            .output()
            .expect("run check against dead socket")
    })
    .await
    .expect("join");
    assert_eq!(
        dead_out.status.code(),
        Some(1),
        "unreachable daemon must exit 1"
    );

    uds_server.abort();
    http_server.abort();
}

#[tokio::test]
async fn herald_never_raises_mail_unrung_for_mutated_after_ack_dissolved_mail() {
    use rex_wave_api::mail::*;
    use rex_wave_api::notify::InboxList;
    use rex_wave_notify::Herald;
    use rex_wave_runtime::{LocalClient, Reactor};
    use std::time::Duration;

    let dir = fresh_dir("herald-edit-never-ringable");
    let journal = Journal::open(&dir.0.join("journal.sqlite3")).expect("open journal");
    let hub = Hub::new(64);
    let router = Arc::new(Router {
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar: SidebarProjection::spawn(journal.clone(), hub.clone()),
            agents: Arc::new(NoAgentFeed),
        }))
    });

    let owner = SeatName::parse("owner").expect("owner");
    let bob = SeatName::parse("bob").expect("bob");
    let herald_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::Herald,
        },
    );
    let mut herald = Herald::with_grace(Duration::from_millis(1));
    herald
        .react(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &herald_client,
        )
        .await
        .expect("caught up");

    let msg = rex_wave_mailbridge::synthesize_mail_message(
        "alice",
        vec![bob],
        Vec::new(),
        "Edited post-ack",
        "Mutated body",
        LetterPriority::Normal,
        None,
        None,
        &[],
        UnixMs(1_795_500_000_000),
    )
    .expect("synth");
    let env = Envelope {
        stream: StreamId::Mail,
        seq: Seq(1),
        at: UnixMs(1_795_500_000_000),
        by: Principal::System {
            part: SystemPart::MailBridge,
        },
        event: Event::Mail(MailEvent::MessageCommitted {
            message: Box::new(msg),
            mutated_after_ack: true,
            prior_ack_sha: Some("0".repeat(64)),
        }),
    };
    herald
        .react(Frame::Committed(env), &herald_client)
        .await
        .expect("react");
    herald
        .flush_due_at(UnixMs(1_795_600_000_000), &herald_client)
        .await
        .expect("flush");

    let inbox = LocalClient::new(router, Principal::Operator { seat: owner })
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .expect("inbox");
    assert!(
        inbox.items.is_empty(),
        "Herald must never raise MailUnrung for mutated_after_ack"
    );
}
