use rex_wave::config_file::{
    CONFIG_FILE_MAX_BYTES, TomlConfigHook, daemon_config_file_path_from, reconcile_startup_config,
    write_config_toml_atomic,
};
use rex_wave::daemon::{mint_seat_token, mint_worker_token};
use rex_wave::http::{HttpConfig, bind_http, serve_http_listener};
use rex_wave_api::config::{ConfigGet, ConfigSet, ConfigShow, RexConfig, RunnerRoute};
use rex_wave_api::read::Read;
use rex_wave_api::*;
use rex_wave_runtime::{
    ConfigDecider, ConfigService, Hub, LocalClient, NoAgentFeed, ReadSide, Registry, Router,
    SidebarProjection,
};
use rex_wave_store::Journal;
use rex_wave_transport::{
    Authenticator, Credential, Endpoint, RemoteClient, StateDirAuthenticator, bind_unix,
    serve_listener,
};
use serde_json::{Value, json};
use std::ffi::OsStr;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

mod common;
use common::{config_home, fresh_dir, rex_wave, rex_wave_at};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_loopback_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn make_sandbox(root: &Path) -> PathBuf {
    let sb = root.join("sb");
    std::fs::create_dir_all(&sb).unwrap();
    std::fs::write(sb.join(".rex-wave-sandbox"), b"test-sandbox\n").unwrap();
    sb
}

fn spawn_test_daemon(root: &Path, sock: &Path, extra_args: &[&str]) -> ChildGuard {
    let sb = make_sandbox(root);
    let bell_port = free_loopback_port();
    let mut cmd = rex_wave(root);
    cmd.args([
        "--socket",
        sock.to_str().unwrap(),
        "daemon",
        "--mail-sandbox",
        sb.to_str().unwrap(),
        "--runner",
        "bb",
        "--bb-url",
        "ws://127.0.0.1:1/disabled",
        "--wake-bell",
        &format!("127.0.0.1:{bell_port}"),
    ]);
    cmd.args(extra_args);
    cmd.stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());
    let child = cmd.spawn().expect("spawn rex-wave daemon");
    let deadline = Instant::now() + Duration::from_secs(6);
    while !sock.exists() {
        assert!(
            Instant::now() < deadline,
            "daemon socket {} did not appear",
            sock.display()
        );
        std::thread::sleep(Duration::from_millis(25));
    }
    ChildGuard(child)
}

struct HttpReply {
    status: u16,
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
}

async fn send_http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> HttpReply {
    let mut raw = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\n");
    for (k, v) in headers {
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
        .expect("http header separator");
    let head = std::str::from_utf8(&buf[..sep]).unwrap();
    let status: u16 = head
        .split("\r\n")
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

#[test]
fn cli_config_show_get_set_unset_preserves_toml_comments_and_0600_mode() {
    let dir = fresh_dir("cfg-cli");
    let sock = dir.0.join("daemon.sock");
    let cfg_dir = config_home(&dir.0).join("rex-wave");
    let cfg_path = cfg_dir.join("config.toml");

    std::fs::write(
        &cfg_path,
        "# Operator top comment\n\n[runners]\n# Keep per-provider note\nclaude = \"t3\" # Keep inline billing note\ncodex = \"bb\" # Unchanged inline note\n",
    )
    .unwrap();

    let couriers_dir = cfg_dir.join("couriers.d");
    std::fs::create_dir_all(&couriers_dir).unwrap();
    let exec_courier = couriers_dir.join("alpha-bell");
    std::fs::write(&exec_courier, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&exec_courier, std::fs::Permissions::from_mode(0o755)).unwrap();
    let plain_courier = couriers_dir.join("beta-disabled");
    std::fs::write(&plain_courier, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&plain_courier, std::fs::Permissions::from_mode(0o644)).unwrap();

    let _daemon = spawn_test_daemon(&dir.0, &sock, &["--config", cfg_path.to_str().unwrap()]);

    let show_out = rex_wave_at(&sock)
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "--json",
            "config",
            "show",
        ])
        .output()
        .unwrap();
    assert!(
        show_out.status.success(),
        "config show stderr: {}",
        String::from_utf8_lossy(&show_out.stderr)
    );
    let show_json: Value = serde_json::from_slice(&show_out.stdout).unwrap();
    assert_eq!(show_json["value"]["config"]["runners"]["claude"], "t3");
    assert_eq!(show_json["value"]["head"], 1);
    assert_eq!(
        show_json["value"]["config_path"],
        cfg_path.to_str().unwrap()
    );
    let runners = show_json["value"]["runners_health"].as_array().unwrap();
    let bb_health = runners
        .iter()
        .find(|r| r["runner"] == "bb")
        .expect("bb runner health entry");
    assert_eq!(bb_health["configured"], true);
    assert_eq!(
        bb_health["healthy"], false,
        "disconnected BB bridge on closed port must report healthy=false"
    );
    let couriers = show_json["value"]["couriers_health"].as_array().unwrap();
    assert_eq!(couriers.len(), 2);
    assert_eq!(couriers[0]["name"], "alpha-bell");
    assert_eq!(couriers[0]["executable"], true);
    assert_eq!(couriers[1]["name"], "beta-disabled");
    assert_eq!(couriers[1]["executable"], false);

    let get_out = rex_wave_at(&sock)
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "config",
            "get",
            "runners.claude",
        ])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    assert_eq!(String::from_utf8_lossy(&get_out.stdout).trim(), "t3");

    for (k, v) in [
        ("runners.default_runner", "t3"),
        ("harnesses.claude.model", "claude-opus-5-5"),
        ("mail.export_enabled", "true"),
    ] {
        let set_out = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "--json",
                "config",
                "set",
                k,
                v,
            ])
            .output()
            .unwrap();
        assert!(
            set_out.status.success(),
            "config set {k}={v} stderr: {}",
            String::from_utf8_lossy(&set_out.stderr)
        );
    }

    let (_f, meta) = rex_wave_fs::open_regular_nofollow(&cfg_path).unwrap();
    assert_eq!(
        meta.permissions().mode() & 0o777,
        0o600,
        "config.toml must be written with mode 0600"
    );
    let raw_toml = rex_wave_fs::read_regular_string(&cfg_path, CONFIG_FILE_MAX_BYTES).unwrap();
    assert!(
        raw_toml.contains("# Operator top comment"),
        "top comment lost:\n{raw_toml}"
    );
    assert!(
        raw_toml.contains("# Keep per-provider note"),
        "inline section comment lost:\n{raw_toml}"
    );
    assert!(
        raw_toml.contains("# Keep inline billing note"),
        "inline comment lost on key:\n{raw_toml}"
    );
    assert!(
        raw_toml.contains("# Unchanged inline note"),
        "inline comment lost on unchanged key:\n{raw_toml}"
    );
    assert!(raw_toml.contains("default_runner = \"t3\""));
    assert!(raw_toml.contains("model = \"claude-opus-5-5\""));
    assert!(raw_toml.contains("export_enabled = true"));

    let unset_out = rex_wave_at(&sock)
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "--json",
            "config",
            "unset",
            "runners.default_runner",
        ])
        .output()
        .unwrap();
    assert!(unset_out.status.success());
    let unset_json: Value = serde_json::from_slice(&unset_out.stdout).unwrap();
    assert!(unset_json["value"]["config"]["runners"]["default_runner"].is_null());

    let get_after = rex_wave_at(&sock)
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "config",
            "get",
            "runners.default_runner",
        ])
        .output()
        .unwrap();
    assert!(get_after.status.success());
    assert_eq!(
        String::from_utf8_lossy(&get_after.stdout).trim(),
        "(default)"
    );

    let after_toml = rex_wave_fs::read_regular_string(&cfg_path, CONFIG_FILE_MAX_BYTES).unwrap();
    assert!(after_toml.contains("# Operator top comment"));
    assert!(!after_toml.contains("default_runner"));
}

#[tokio::test]
async fn startup_reconcile_exports_when_sqlite_ahead_and_imports_when_hand_edited() {
    let dir = fresh_dir("cfg-rec");
    let state_dir = dir.0.join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let cfg_path = dir.0.join("config.toml");
    let journal_path = state_dir.join("journal.sqlite3");

    // Verify daemon_config_file_path_from isolation rules.
    assert_eq!(
        daemon_config_file_path_from(
            Some(Path::new("")),
            true,
            &state_dir,
            Some(OsStr::new("/xdg")),
            Some(OsStr::new("/home/u")),
        ),
        None
    );
    assert_eq!(
        daemon_config_file_path_from(
            Some(Path::new("/explicit/config.toml")),
            true,
            &state_dir,
            Some(OsStr::new("/xdg")),
            Some(OsStr::new("/home/u")),
        ),
        Some(PathBuf::from("/explicit/config.toml"))
    );
    assert_eq!(
        daemon_config_file_path_from(None, true, &state_dir, None, Some(OsStr::new("/home/u")),),
        Some(state_dir.join("config.toml"))
    );
    assert_eq!(
        daemon_config_file_path_from(None, false, &state_dir, None, Some(OsStr::new("/home/u")),),
        Some(PathBuf::from("/home/u/.config/rex-wave/config.toml"))
    );

    // Phase 1: commit initial config (seq=1) via ConfigService + TomlConfigHook.
    {
        let journal = Journal::open(&journal_path).unwrap();
        let hub = Hub::new(64);
        let hook = TomlConfigHook::new(
            Some(cfg_path.clone()),
            state_dir.clone(),
            RunnerRoute::Bb,
            true,
            false,
        );
        let svc = ConfigService::with_hook(journal.clone(), hub.clone(), Some(hook)).unwrap();
        let op = Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        };
        let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
        let read = Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        });
        let op_client = LocalClient::new(
            Arc::new(Router {
                config: svc,
                ..Router::unavailable_with(read)
            }),
            op,
        );
        op_client
            .call(ConfigSet {
                key: "runners.default_runner".into(),
                value: Some("bb".into()),
                idem: None,
            })
            .await
            .unwrap();
        assert!(
            rex_wave_fs::read_regular_string(&cfg_path, CONFIG_FILE_MAX_BYTES)
                .unwrap()
                .contains("default_runner = \"bb\"")
        );
    }

    // Simulate crash between SQLite commit (seq=2) and config.toml export:
    // reopen journal and commit directly via pure Registry<ConfigDecider> without TomlConfigHook.
    {
        let journal = Journal::open(&journal_path).unwrap();
        let hub = Hub::new(64);
        let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
        let read = Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar,
            agents: Arc::new(NoAgentFeed),
        });
        let raw_client = LocalClient::new(
            Arc::new(Router {
                config: Registry::<ConfigDecider>::new(journal, hub),
                ..Router::unavailable_with(read)
            }),
            Principal::Operator {
                seat: SeatName::parse("owner").unwrap(),
            },
        );
        raw_client
            .call(ConfigSet {
                key: "runners.default_runner".into(),
                value: Some("t3".into()),
                idem: None,
            })
            .await
            .unwrap();
        assert!(
            rex_wave_fs::read_regular_string(&cfg_path, CONFIG_FILE_MAX_BYTES)
                .unwrap()
                .contains("default_runner = \"bb\""),
            "stale config.toml before restart"
        );
    }

    // Restart: reconcile_startup_config detects SQLite ahead of unedited disk file and exports.
    {
        let journal = Journal::open(&journal_path).unwrap();
        let hub = Hub::new(64);
        let hook2 = TomlConfigHook::new(
            Some(cfg_path.clone()),
            state_dir.clone(),
            RunnerRoute::Bb,
            true,
            false,
        );
        let svc2 = ConfigService::with_hook(journal.clone(), hub, Some(hook2)).unwrap();
        let rec1 = reconcile_startup_config(&journal, &svc2, Some(&cfg_path), &state_dir)
            .await
            .unwrap();
        assert_eq!(rec1.runners.default_runner, Some(RunnerRoute::T3));
        let exported = rex_wave_fs::read_regular_string(&cfg_path, CONFIG_FILE_MAX_BYTES).unwrap();
        assert!(
            exported.contains("default_runner = \"t3\""),
            "expected crash-reconciled export to config.toml, got:\n{exported}"
        );
    }

    // Phase 2: hand-edit config.toml while stopped.
    let mut edited = RexConfig::default();
    edited.runners.default_runner = Some(RunnerRoute::Native);
    edited.harnesses.codex.reasoning = Some("xhigh".into());
    std::fs::write(
        &cfg_path,
        "# Hand-edited while stopped\n[runners]\ndefault_runner = \"native\"\n\n[harnesses.codex]\nreasoning = \"xhigh\"\n",
    )
    .unwrap();
    assert!(write_config_toml_atomic(&dir.0.join("unused.toml"), &edited).is_ok());

    {
        let journal = Journal::open(&journal_path).unwrap();
        let hub = Hub::new(64);
        let hook3 = TomlConfigHook::new(
            Some(cfg_path.clone()),
            state_dir.clone(),
            RunnerRoute::Bb,
            true,
            false,
        );
        let svc3 = ConfigService::with_hook(journal.clone(), hub, Some(hook3)).unwrap();
        let rec2 = reconcile_startup_config(&journal, &svc3, Some(&cfg_path), &state_dir)
            .await
            .unwrap();
        assert_eq!(rec2.runners.default_runner, Some(RunnerRoute::Native));
        assert_eq!(rec2.harnesses.codex.reasoning.as_deref(), Some("xhigh"));
        let (head, replayed) = rex_wave_runtime::replay_config_from_journal(&journal).unwrap();
        assert_eq!(head, Seq(3));
        assert_eq!(replayed.runners.default_runner, Some(RunnerRoute::Native));
    }
}

#[test]
fn protected_roots_rejects_mail_mirror_and_drop_dir_on_set_and_startup() {
    let dir = fresh_dir("cfg-prot");
    let sock = dir.0.join("daemon.sock");
    let guarded = dir.0.join("guarded-interchange");
    let safe_mirror = dir.0.join("safe-mirror");
    std::fs::create_dir_all(&guarded).unwrap();
    std::fs::create_dir_all(&safe_mirror).unwrap();

    let cfg_dir = config_home(&dir.0).join("rex-wave");
    let roots_file = cfg_dir.join("protected-roots");
    std::fs::write(&roots_file, format!("{}\n", guarded.display())).unwrap();
    // Global XDG config.toml exists, but `--socket` daemons must isolate to `<state_dir>/config.toml`.
    std::fs::write(
        cfg_dir.join("config.toml"),
        "[runners]\ncodex = \"native\"\n",
    )
    .unwrap();
    let cfg_path = dir.0.join("config.toml");

    {
        let _daemon = spawn_test_daemon(&dir.0, &sock, &[]);

        let iso_show = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "--json",
                "config",
                "show",
            ])
            .output()
            .unwrap();
        assert!(iso_show.status.success());
        let iso_json: Value = serde_json::from_slice(&iso_show.stdout).unwrap();
        assert_eq!(iso_json["value"]["config_path"], cfg_path.to_str().unwrap());
        assert!(
            iso_json["value"]["config"]["runners"]["codex"].is_null(),
            "--socket daemon must not import global XDG_CONFIG_HOME config.toml"
        );

        for key in ["mail.mirror_dir", "mail.drop_dir"] {
            let target = guarded.join("sub");
            let out = rex_wave_at(&sock)
                .args([
                    "--socket",
                    sock.to_str().unwrap(),
                    "config",
                    "set",
                    key,
                    target.to_str().unwrap(),
                ])
                .output()
                .unwrap();
            assert!(
                !out.status.success(),
                "{key} inside protected-roots must be rejected"
            );
            let stderr = String::from_utf8_lossy(&out.stderr);
            assert!(
                stderr.contains("protected-roots") || stderr.contains("denied"),
                "expected protected-roots error for {key}, got: {stderr}"
            );
        }

        let ok_out = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "config",
                "set",
                "mail.mirror_dir",
                safe_mirror.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(
            ok_out.status.success(),
            "safe mirror_dir failed: {}",
            String::from_utf8_lossy(&ok_out.stderr)
        );

        let uncreated_safe = dir.0.join("future-mirror/nested");
        let future_out = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "config",
                "set",
                "mail.drop_dir",
                uncreated_safe.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(
            future_out.status.success(),
            "not-yet-created safe drop_dir failed: {}",
            String::from_utf8_lossy(&future_out.stderr)
        );

        // Configure harness permission/workspace defaults and verify CLI `wave start`
        // applies them when `--permission` / `--workspace` are omitted, while explicit
        // CLI flags override them.
        for (k, v) in [
            ("harnesses.claude.permission", "full"),
            ("harnesses.claude.workspace", "project-default"),
        ] {
            let set_out = rex_wave_at(&sock)
                .args(["--socket", sock.to_str().unwrap(), "config", "set", k, v])
                .output()
                .unwrap();
            assert!(set_out.status.success(), "set {k}={v} failed");
        }
        let ws_dir = dir.0.join("proj");
        std::fs::create_dir_all(&ws_dir).unwrap();
        let w1 = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "--json",
                "wave",
                "start",
                "cfg-defaults",
                "--project",
                ws_dir.to_str().unwrap(),
                "--goal",
                "test harness defaults",
                "--worker",
                "author=claude",
            ])
            .output()
            .unwrap();
        assert!(
            w1.status.success(),
            "wave start failed: {}",
            String::from_utf8_lossy(&w1.stderr)
        );
        let w1_view = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "--json",
                "wave",
                "status",
                "cfg-defaults",
            ])
            .output()
            .unwrap();
        assert!(w1_view.status.success());
        let w1_json: Value = serde_json::from_slice(&w1_view.stdout).unwrap();
        let w1_agent = &w1_json["value"]["workers"][0][1];
        assert_eq!(w1_agent["permission"], "full");
        assert_eq!(w1_agent["workspace"]["workspace"], "project_default");

        let w2 = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "--json",
                "wave",
                "start",
                "cfg-override",
                "--project",
                ws_dir.to_str().unwrap(),
                "--goal",
                "test explicit flag override",
                "--worker",
                "author=claude",
                "--permission",
                "accept-edits",
                "--workspace",
                "managed-worktree",
            ])
            .output()
            .unwrap();
        assert!(w2.status.success());
        let w2_view = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "--json",
                "wave",
                "status",
                "cfg-override",
            ])
            .output()
            .unwrap();
        assert!(w2_view.status.success());
        let w2_json: Value = serde_json::from_slice(&w2_view.stdout).unwrap();
        let w2_agent = &w2_json["value"]["workers"][0][1];
        assert_eq!(w2_agent["permission"], "accept-edits");
        assert_eq!(w2_agent["workspace"]["workspace"], "managed_worktree");
    }

    // Tighten `protected-roots` to include `safe_mirror` (which is still committed in SQLite),
    // repair `config.toml` offline to remove `mirror_dir` and `drop_dir`, and verify daemon boots!
    let _ = std::fs::remove_file(&sock);
    std::fs::write(
        &roots_file,
        format!("{}\n{}\n", guarded.display(), safe_mirror.display()),
    )
    .unwrap();
    std::fs::write(&cfg_path, "[runners]\ndefault_runner = \"bb\"\n").unwrap();
    {
        let _repaired = spawn_test_daemon(&dir.0, &sock, &[]);
        let repaired_show = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "--json",
                "config",
                "show",
            ])
            .output()
            .unwrap();
        assert!(repaired_show.status.success());
        let rep_json: Value = serde_json::from_slice(&repaired_show.stdout).unwrap();
        assert!(rep_json["value"]["config"]["mail"]["mirror_dir"].is_null());
    }

    // Verify rendered `config.toml` byte-limit check rejects oversized writes BEFORE journal commit.
    let _ = std::fs::remove_file(&sock);
    let pad_len = (CONFIG_FILE_MAX_BYTES as usize) - 48;
    let near_limit = format!(
        "# {}\n[runners]\ndefault_runner = \"bb\"\n",
        "x".repeat(pad_len)
    );
    assert!(near_limit.len() < CONFIG_FILE_MAX_BYTES as usize);
    std::fs::write(&cfg_path, &near_limit).unwrap();
    {
        let _daemon = spawn_test_daemon(&dir.0, &sock, &[]);
        let over_out = rex_wave_at(&sock)
            .args([
                "--socket",
                sock.to_str().unwrap(),
                "config",
                "set",
                "harnesses.claude.model",
                "claude-opus-5-5-very-long-model-identifier-exceeding-32-bytes",
            ])
            .output()
            .unwrap();
        assert!(
            !over_out.status.success(),
            "oversized rendered config.toml must be rejected before commit"
        );
    }

    // Wait briefly for socket cleanup then inject a protected path into config.toml
    // and verify daemon startup refuses to boot.
    let _ = std::fs::remove_file(&sock);
    std::fs::write(
        &cfg_path,
        format!(
            "[mail]\nmirror_dir = \"{}\"\n",
            guarded.join("offline-evil").display()
        ),
    )
    .unwrap();
    let sb = make_sandbox(&dir.0);
    let bell_port = free_loopback_port();
    let out = rex_wave(&dir.0)
        .args([
            "--socket",
            sock.to_str().unwrap(),
            "daemon",
            "--mail-sandbox",
            sb.to_str().unwrap(),
            "--runner",
            "bb",
            "--bb-url",
            "ws://127.0.0.1:1/disabled",
            "--wake-bell",
            &format!("127.0.0.1:{bell_port}"),
        ])
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "daemon must refuse startup when config.toml targets a protected root"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("protected-roots"),
        "expected protected-roots startup failure, got: {stderr}"
    );
}

#[tokio::test]
async fn non_operator_denied_on_uds_and_masked_404_on_http_while_operator_round_trips() {
    let dir = fresh_dir("cfg-auth");
    let sock = dir.0.join("daemon.sock");
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(128);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let config_svc = ConfigService::new(journal.clone(), hub.clone()).unwrap();
    let wave_svc = rex_wave_waves::WaveService::new(
        Registry::<rex_wave_waves::Waves>::new(journal.clone(), hub.clone()),
        RunnerKind::Bb,
        Some(config_svc.subscribe()),
    );

    let router = Arc::new(Router {
        config: config_svc,
        wave: wave_svc,
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }))
    });

    let op = SeatName::parse("owner").unwrap();
    let seat_tok = mint_seat_token(&sock, &SeatName::parse("alice").unwrap()).unwrap();
    let worker_tok = mint_worker_token(
        &sock,
        &WaveSlug::parse("w1").unwrap(),
        &RoleName::parse("dev").unwrap(),
        None,
    )
    .unwrap();

    let (uds_listener, daemon_uid) = bind_unix(&sock).unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(StateDirAuthenticator::from_state_dir(
        op.clone(),
        Some(daemon_uid),
        &dir.0,
    ));
    let uds_task = tokio::spawn(serve_listener(uds_listener, router.clone(), auth.clone()));

    let http_listener = bind_http("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let http_task = tokio::spawn(serve_http_listener(
        http_listener,
        router.clone(),
        auth,
        op,
        HttpConfig::default(),
    ));

    // Operator succeeds on UDS + HTTP (`/v1/config` and `/api/config`).
    let op_client = RemoteClient::connect(Endpoint::Unix(sock.clone()), Credential::PeerUid, "op")
        .await
        .unwrap();
    let snap = op_client.call(ConfigShow {}).await.unwrap();
    assert_eq!(snap.head, Seq(0));

    let set_http = send_http(
        http_addr,
        "PATCH",
        "/api/config",
        &[("Content-Type", "application/json")],
        Some(&json!({"key": "runners.default_runner", "value": "t3"}).to_string()),
    )
    .await;
    assert_eq!(set_http.status, 200);
    assert_eq!(set_http.json()["config"]["runners"]["default_runner"], "t3");

    let get_http = send_http(
        http_addr,
        "GET",
        "/v1/config?key=runners.default_runner",
        &[],
        None,
    )
    .await;
    assert_eq!(get_http.status, 200);
    assert_eq!(get_http.json()["value"], "t3");

    // Verify Operator SSE stream `?stream=config` delivers committed config entry.
    {
        let mut sse = TcpStream::connect(http_addr).await.unwrap();
        let req = format!("GET /v1/events?stream=config HTTP/1.1\r\nHost: {http_addr}\r\n\r\n");
        sse.write_all(req.as_bytes()).await.unwrap();
        sse.flush().await.unwrap();
        let mut reader = BufReader::new(sse);
        let mut status_line = String::new();
        reader.read_line(&mut status_line).await.unwrap();
        assert!(status_line.contains("200 OK"), "status: {status_line}");
        let mut saw_config = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            let mut line = String::new();
            let n = tokio::time::timeout(Duration::from_millis(500), reader.read_line(&mut line))
                .await
                .unwrap()
                .unwrap();
            if n == 0 {
                break;
            }
            if line.starts_with("data:") && line.contains("\"family\":\"config\"") {
                saw_config = true;
                break;
            }
        }
        assert!(
            saw_config,
            "expected StreamId::Config SSE frame for operator"
        );
    }

    // Seat and Worker principals: UDS returns ApiError::Denied; HTTP returns 404 Not Found.
    for (label, tok) in [("seat", seat_tok), ("worker", worker_tok)] {
        let client = RemoteClient::connect(
            Endpoint::Unix(sock.clone()),
            Credential::Token { token: tok.clone() },
            label,
        )
        .await
        .unwrap();

        assert!(
            matches!(
                client.call(ConfigShow {}).await,
                Err(ApiError::Denied { .. })
            ),
            "{label} UDS ConfigShow must return Denied"
        );
        assert!(
            matches!(
                client
                    .call(ConfigGet {
                        key: "runners.default_runner".into(),
                    })
                    .await,
                Err(ApiError::Denied { .. })
            ),
            "{label} UDS ConfigGet must return Denied"
        );
        assert!(
            matches!(
                client
                    .call(ConfigSet {
                        key: "runners.default_runner".into(),
                        value: Some("bb".into()),
                        idem: None,
                    })
                    .await,
                Err(ApiError::Denied { .. })
            ),
            "{label} UDS ConfigSet must return Denied"
        );
        assert!(
            matches!(
                client
                    .call(Read::stream(StreamId::Config, Seq::ORIGIN, Some(10)))
                    .await,
                Err(ApiError::Denied { .. })
            ),
            "{label} UDS Read(StreamId::Config) must return Denied"
        );
        assert!(
            matches!(
                client
                    .watch(
                        Interest::Streams {
                            streams: vec![StreamId::Config],
                        },
                        Cursor::default(),
                    )
                    .await,
                Err(ApiError::Denied { .. })
            ),
            "{label} UDS watch(StreamId::Config) must return Denied"
        );

        let auth_hdr = format!("Bearer {tok}");
        for (method, path, body) in [
            ("GET", "/v1/config", None),
            ("GET", "/api/config", None),
            ("GET", "/v1/config?key=runners.default_runner", None),
            (
                "POST",
                "/v1/config",
                Some(r#"{"key":"runners.default_runner","value":"bb"}"#),
            ),
            (
                "PATCH",
                "/api/config",
                Some(r#"{"key":"runners.default_runner","value":"bb"}"#),
            ),
            ("POST", "/v1/config", Some("not valid json {{{")),
            ("PATCH", "/api/config", Some(r#"{"unknown_field":123}"#)),
            ("POST", "/v1/call/config_show", Some("{}")),
            (
                "POST",
                "/v1/call/config_set",
                Some(r#"{"key":"runners.default_runner","value":"bb"}"#),
            ),
            ("GET", "/v1/events?stream=config", None),
        ] {
            let mut hdrs = vec![("Authorization", auth_hdr.as_str())];
            if body.is_some() {
                hdrs.push(("Content-Type", "application/json"));
            }
            let r = send_http(http_addr, method, path, &hdrs, body).await;
            assert_eq!(
                r.status,
                404,
                "{label} HTTP {method} {path} must be masked as 404, got {} ({})",
                r.status,
                String::from_utf8_lossy(&r.body)
            );
        }
    }

    // Confirm local in-process Seat client also gets Denied on StreamId::Config.
    let seat_local = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: SeatName::parse("alice").unwrap(),
        },
    );
    assert!(matches!(
        seat_local.call(ConfigShow {}).await,
        Err(ApiError::Denied { .. })
    ));

    // Verify a live `Worker` principal invoking `wave add-worker` over UDS inherits
    // configured `[harnesses.claude]` defaults even though `ConfigShow` is denied to workers.
    for (k, v) in [
        ("harnesses.claude.permission", "full"),
        ("harnesses.claude.workspace", "project-default"),
        ("harnesses.claude.account", "team-claude"),
    ] {
        op_client
            .call(ConfigSet {
                key: k.into(),
                value: Some(v.into()),
                idem: None,
            })
            .await
            .unwrap();
    }
    let proj_dir = dir.0.join("w1-proj");
    std::fs::create_dir_all(&proj_dir).unwrap();
    op_client
        .call(rex_wave_api::wave::WaveStart {
            spec: rex_wave_api::wave::WaveSpec {
                slug: WaveSlug::parse("w1").unwrap(),
                project: ProjectId::parse(proj_dir.to_str().unwrap()).unwrap(),
                goal: rex_wave_api::room::Body::parse("test worker add defaults").unwrap(),
                output_dir: None,
                defaults: rex_wave_api::wave::AgentSpec {
                    provider: ProviderId::parse("claude").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: rex_wave_api::wave::PermissionMode::AcceptEdits,
                    workspace: rex_wave_api::wave::Workspace::ManagedWorktree {
                        base: rex_wave_api::wave::BaseBranch::Default,
                    },
                },
                workers: vec![rex_wave_api::wave::WorkerSpec {
                    role: RoleName::parse("dev").unwrap(),
                    brief: rex_wave_api::room::Body::parse("lead").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                }],
            },
            idem: None,
            config_defaults: rex_wave_api::wave::WaveConfigDefaults {
                permission: true,
                workspace: true,
                account_from_config: false,
            },
        })
        .await
        .unwrap();
    let sys_conductor = LocalClient::new(
        router,
        Principal::System {
            part: SystemPart::WaveConductor,
        },
    );
    sys_conductor
        .call(rex_wave_api::wave::WaveObserve {
            wave: WaveSlug::parse("w1").unwrap(),
            role: RoleName::parse("dev").unwrap(),
            observation: rex_wave_api::wave::Observation::Spawned {
                attempt: 1,
                thread: HostThreadId::parse("th_bb_dev").unwrap(),
            },
        })
        .await
        .unwrap();

    let worker_tok2 = mint_worker_token(
        &sock,
        &WaveSlug::parse("w1").unwrap(),
        &RoleName::parse("dev").unwrap(),
        None,
    )
    .unwrap();
    let sock_for_cli = sock.clone();
    let add_out = tokio::task::spawn_blocking(move || {
        rex_wave_at(&sock_for_cli)
            .args([
                "--socket",
                sock_for_cli.to_str().unwrap(),
                "--token",
                &worker_tok2,
                "--json",
                "wave",
                "add-worker",
                "w1",
                "helper=claude",
                "--brief",
                "assist",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        add_out.status.success(),
        "live worker wave add-worker failed: {}",
        String::from_utf8_lossy(&add_out.stderr)
    );
    let view = op_client
        .call(rex_wave_api::wave::WaveView {
            wave: WaveSlug::parse("w1").unwrap(),
        })
        .await
        .unwrap();
    let (_, helper_spec, _) = view
        .workers
        .iter()
        .find(|(r, _, _)| r.as_str() == "helper")
        .expect("helper worker added");
    assert_eq!(helper_spec.provider.as_str(), "t3:claudeAgent");
    assert_eq!(
        helper_spec.permission,
        rex_wave_api::wave::PermissionMode::Full
    );
    assert_eq!(
        helper_spec.workspace,
        rex_wave_api::wave::Workspace::ProjectDefault
    );
    assert_eq!(helper_spec.account.as_deref(), Some("team-claude"));

    uds_task.abort();
    http_task.abort();
}
