use rex_wave::daemon::{TokenFileAuthenticator, tokens_path_for};
use rex_wave_api::room::{Body, RoomMessageSummary};
use rex_wave_api::wave::{
    Activity, AgentSpec, Observation, PermissionMode, WaveObserve, WaveSpec, WaveStart, WaveView,
    WorkerPhase, WorkerSpec, Workspace,
};
use rex_wave_api::*;
use rex_wave_notify::Inbox;
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    BoxFut, DecideCtx, Hub, LocalClient, NoAgentFeed, ReadSide, Registry, Router, Service,
    SidebarProjection, Unavailable,
};
use rex_wave_store::Journal;
use rex_wave_transport::{Authenticator, bind_unix, serve_listener};
use rex_wave_waves::Waves;
use serde_json::Value;
use std::io::Write as _;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static NONCE: AtomicU64 = AtomicU64::new(1);

struct DirGuard(PathBuf);
impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn fresh_dir(label: &str) -> DirGuard {
    let n = NONCE.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("rex-wave-cli3-{label}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    DirGuard(p)
}

fn bin_path() -> &'static str {
    env!("CARGO_BIN_EXE_rex-wave")
}

/// Wraps `Registry<Rooms>` so `RoomPost { after: Some(Seq(1)), .. }` triggers a
/// structured `ConflictData::NewerMessage` even before PR-3B merges into `rex-wave-rooms`.
struct ConflictInjectingRooms {
    inner: Arc<dyn Service<RoomCommand>>,
}

impl Service<RoomCommand> for ConflictInjectingRooms {
    fn dispatch(
        &self,
        cmd: RoomCommand,
        ctx: DecideCtx,
    ) -> BoxFut<'_, Result<WireReply, ApiError>> {
        Box::pin(async move {
            if let RoomCommand::RoomPost(ref p) = cmd
                && let Some(expected) = p.after
            {
                return Err(ApiError::conflict_with(
                    "newer post landed in room",
                    ConflictData::NewerMessage {
                        expected,
                        latest: Seq(expected.0 + 1),
                        newer_message: Box::new(RoomMessageSummary {
                            seq: Seq(expected.0 + 1),
                            at: UnixMs(1234),
                            by: ctx.by.clone(),
                            body: Body::parse("landed first").unwrap(),
                            re: p.re,
                            labels: vec![],
                        }),
                    },
                ));
            }
            self.inner.dispatch(cmd, ctx).await
        })
    }
}

#[tokio::test]
async fn cli_post_reply_read_wait_conflict_and_t3_worker_token_pickup() {
    let dir = fresh_dir("verbs");
    let sock = dir.0.join("daemon.sock");
    let journal = Journal::open(&dir.0.join("journal.sqlite3")).unwrap();
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());

    let inner_rooms: Arc<dyn Service<RoomCommand>> =
        Registry::<Rooms>::new(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: Arc::new(ConflictInjectingRooms { inner: inner_rooms }),
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

    let (listener, daemon_uid) = bind_unix(&sock).unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(TokenFileAuthenticator {
        operator: SeatName::parse("owner").unwrap(),
        daemon_uid: Some(daemon_uid),
        tokens_path: tokens_path_for(&sock),
    });
    let server = tokio::spawn(serve_listener(listener, router.clone(), auth));
    let sock_str = sock.to_str().unwrap().to_owned();

    let s1 = sock_str.clone();
    let root_out = tokio::task::spawn_blocking(move || {
        let mut child = Command::new(bin_path())
            .args(["--socket", &s1, "--json", "post", "#general", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"root from stdin\n")
            .unwrap();
        child.wait_with_output().unwrap()
    })
    .await
    .unwrap();
    assert!(
        root_out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&root_out.stderr)
    );
    let root_json: Value = serde_json::from_slice(&root_out.stdout).unwrap();
    let root_seq = root_json["value"]["seq"].as_u64().unwrap();

    let s2 = sock_str.clone();
    let reply_out = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args([
                "--socket",
                &s2,
                "--json",
                "reply",
                "#general",
                &root_seq.to_string(),
                "reply before wait",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(reply_out.status.success());
    let reply_json: Value = serde_json::from_slice(&reply_out.stdout).unwrap();
    let reply_seq = reply_json["value"]["seq"].as_u64().unwrap();
    assert!(reply_seq > root_seq);

    let s3 = sock_str.clone();
    let read_out = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args([
                "--socket",
                &s3,
                "--json",
                "read",
                "#general",
                "--re",
                &root_seq.to_string(),
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(read_out.status.success());
    let read_json: Value = serde_json::from_slice(&read_out.stdout).unwrap();
    assert_eq!(read_json["reply"], "read");
    let entries = read_json["value"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);

    let s4 = sock_str.clone();
    let wait_hit = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args([
                "--socket",
                &s4,
                "--json",
                "wait",
                "#general",
                "--since",
                &root_seq.to_string(),
                "--re",
                &root_seq.to_string(),
                "--timeout",
                "2000",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(wait_hit.status.success());
    let hit_json: Value = serde_json::from_slice(&wait_hit.stdout).unwrap();
    assert_eq!(hit_json["value"]["timed_out"], false);
    assert_eq!(
        hit_json["value"]["entries"][0]["event"]["event"]["body"],
        "reply before wait"
    );

    let s5 = sock_str.clone();
    let wait_to = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args([
                "--socket",
                &s5,
                "--json",
                "wait",
                "#general",
                "--since",
                &reply_seq.to_string(),
                "--all",
                "--timeout",
                "40",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(wait_to.status.code(), Some(124));
    let to_json: Value = serde_json::from_slice(&wait_to.stdout).unwrap();
    assert_eq!(to_json["value"]["timed_out"], true);

    let s6 = sock_str.clone();
    let conflict_out = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args([
                "--socket",
                &s6,
                "post",
                "#general",
                "stale update",
                "--after",
                "1",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(conflict_out.status.code(), Some(3));
    let stderr = String::from_utf8_lossy(&conflict_out.stderr);
    assert!(stderr.contains("newer_message"), "stderr was: {stderr}");
    assert!(stderr.contains("landed first"), "stderr was: {stderr}");

    let op_client = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let sys_client = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::T3Bridge,
        },
    );
    let wave = WaveSlug::parse("w1").unwrap();
    let role = RoleName::parse("author").unwrap();
    let thread_id = "t3:w1:author:a1";
    let agent = AgentSpec {
        provider: ProviderId::parse("antigravity").unwrap(),
        model: None,
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
                goal: Body::parse("test t3 worker cli").unwrap(),
                defaults: agent.clone(),
                workers: vec![WorkerSpec {
                    role: role.clone(),
                    brief: Body::parse("work").unwrap(),
                    agent: Some(agent),
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
                thread: BbThreadId::parse(thread_id).unwrap(),
            },
        })
        .await
        .unwrap();

    let s7 = sock_str.clone();
    let mint_out = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args([
                "--socket",
                &s7,
                "token",
                "--worker",
                "w1/author",
                "--thread",
                thread_id,
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(mint_out.status.success());
    let tok_file = rex_wave_t3::worker_token_path(&dir.0, thread_id).unwrap();
    assert_eq!(
        std::fs::symlink_metadata(&tok_file)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let s8 = sock_str.clone();
    let worker_post = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .env_remove("REX_WAVE_TOKEN")
            .env("T3CODE_THREAD_ID", thread_id)
            .args([
                "--socket",
                &s8,
                "--json",
                "post",
                "#general",
                "from t3 worker",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        worker_post.status.success(),
        "worker post failed: {}",
        String::from_utf8_lossy(&worker_post.stderr)
    );
    let wp_json: Value = serde_json::from_slice(&worker_post.stdout).unwrap();
    let wp_seq = wp_json["value"]["seq"].as_u64().unwrap();

    let s9 = sock_str.clone();
    let check_read = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args([
                "--socket",
                &s9,
                "--json",
                "read",
                "#general",
                "--after",
                &(wp_seq - 1).to_string(),
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let cr_json: Value = serde_json::from_slice(&check_read.stdout).unwrap();
    assert_eq!(
        cr_json["value"]["entries"][0]["by"],
        serde_json::json!({ "kind": "worker", "wave": "w1", "role": "author" })
    );

    let s10 = sock_str.clone();
    let worker_wait = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .env_remove("REX_WAVE_TOKEN")
            .env("T3CODE_THREAD_ID", thread_id)
            .args([
                "--socket",
                &s10,
                "--json",
                "wait",
                "#general",
                "--since",
                &wp_seq.to_string(),
                "--all",
                "--timeout",
                "4000",
            ])
            .output()
            .unwrap()
    });

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snap = op_client
                .call(WaveView { wave: wave.clone() })
                .await
                .unwrap();
            let is_parked = snap.workers.iter().any(|(r, _, phase)| {
                r == &role
                    && matches!(
                        phase,
                        WorkerPhase::Live {
                            activity: Activity::Parked { .. },
                            ..
                        }
                    )
            });
            if is_parked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("T3 worker CLI wait entered Activity::Parked");

    let s11 = sock_str.clone();
    let wake_out = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["--socket", &s11, "post", "#general", "wake worker"])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(wake_out.status.success());

    let ww_out = worker_wait.await.unwrap();
    assert!(ww_out.status.success());

    let snap_after = op_client
        .call(WaveView { wave: wave.clone() })
        .await
        .unwrap();
    assert!(snap_after.workers.iter().any(|(r, _, phase)| {
        r == &role
            && matches!(
                phase,
                WorkerPhase::Live {
                    activity: Activity::Working,
                    ..
                }
            )
    }));

    rex_wave_t3::remove_worker_token_file(&dir.0, thread_id).unwrap();
    let s12 = sock_str.clone();
    let retired_post = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .env_remove("REX_WAVE_TOKEN")
            .env("T3CODE_THREAD_ID", thread_id)
            .args(["--socket", &s12, "post", "#general", "should fail closed"])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(!retired_post.status.success());

    server.abort();
}
