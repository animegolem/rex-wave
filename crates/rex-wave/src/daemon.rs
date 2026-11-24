//! Daemon composition root wiring `Journal`, `Hub`, `SidebarProjection`,
//! subsystem `Registry`s (`Rooms`, `Waves`, `Board`, `Inbox`, `Presence`),
//! `MailBridge`, reactors (`WaveConductor`, `Herald` with `HeraldHandle`
//! reconnect-grace timer, `BellCourier`), `CompositeHost` / `CompositeFeed`
//! (`BbBridge` + `T3Bridge`), the loopback `/sink` listener for `wake-bell`,
//! optional loopback `--http` HTTP/1.1 + SSE server, and UDS
//! `serve_listener_with_hooks` (`StateDirAuthenticator` + RAII `PresenceUnbind`)
//! with graceful `ctrl_c` shutdown.

use crate::http::{HttpConfig, bind_http, serve_http_listener};
use rex_wave_api::presence::{BindingStatus, PresenceList, PresenceUnbind, TransportEndpoint};
use rex_wave_api::wave::{Observation, WaveEvent};
use rex_wave_api::*;
use rex_wave_bb::{BbBridge, BbHost, BridgeConfig};
use rex_wave_board::Board;
use rex_wave_mailbridge::{BellCourier, MailBridge, Presence, SandboxRoot};
use rex_wave_notify::{Herald, HeraldHandle, Inbox};
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    AgentFeed, Hub, LocalClient, ReadSide, Registry, Router, SidebarProjection, spawn_reactor,
};
use rex_wave_store::Journal;
use rex_wave_t3::{
    CompositeFeed, CompositeHost, RunnerTarget, T3AuthSource, T3Bridge, T3Config, T3EndpointSource,
    T3Feed, T3Host, T3Origin, ThreadIdResolver, runtime_file_path, worker_tokens_dir,
};
use rex_wave_transport::{
    Authenticator, ConnectionHooks, Endpoint, PrincipalTokenTable, StateDirAuthenticator,
    WorkerTokenFile, bind_unix, is_safe_worker_token_id, read_private_file,
    serve_listener_with_hooks, write_worker_token_file,
};
use rex_wave_waves::{WaveConductor, Waves};
use std::collections::HashSet;
use std::io::{self, Read as _};
use std::net::SocketAddr;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

pub const DEFAULT_BB_URL: &str = "ws://127.0.0.1:38886/api/v1/plugins/rex-wave/http/bridge";

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerMode {
    Bb,
    T3,
    Both,
}

#[derive(clap::Args, Debug, Clone)]
pub struct DaemonArgs {
    /// Journal path (default: `<socket_dir>/journal.sqlite3`).
    #[arg(long)]
    pub journal: Option<PathBuf>,
    /// Non-production mail sandbox root (must contain `.rex-wave-sandbox`).
    #[arg(long)]
    pub mail_sandbox: PathBuf,
    /// BB plugin bridge WebSocket URL.
    #[arg(long, default_value = DEFAULT_BB_URL)]
    pub bb_url: String,
    /// BB plugin bridge bearer token.
    #[arg(long, env = "REX_WAVE_BB_TOKEN", default_value = "")]
    pub bb_token: String,
    /// Worker host runner backend (`bb`, `t3`, or `both`). Defaults to `both`
    /// when T3 is configured or a local T3 runtime exists, otherwise `bb`.
    #[arg(long, value_enum)]
    pub runner: Option<RunnerMode>,
    /// T3 Code base directory (default: `~/.t3`).
    #[arg(long, env = "REX_WAVE_T3_HOME")]
    pub t3_home: Option<PathBuf>,
    /// Explicit T3 Code HTTP/WS origin (overrides `<t3-home>/userdata/server-runtime.json`).
    #[arg(long, env = "REX_WAVE_T3_URL")]
    pub t3_url: Option<String>,
    /// Explicit T3 Code bearer token (overrides `t3 auth session issue`).
    #[arg(long, env = "REX_WAVE_T3_TOKEN")]
    pub t3_token: Option<String>,
    /// Operator seat name bound to peer-UID sessions and unauthenticated loopback HTTP.
    #[arg(long, default_value = "owner")]
    pub operator: String,
    /// Loopback HTTP listener for `wake-bell` `/sink` delivery.
    #[arg(long, default_value = "127.0.0.1:7781")]
    pub wake_bell: SocketAddr,
    /// Optional loopback HTTP/1.1 + SSE API listener (`127.0.0.1:<port>` or `[::1]:<port>`).
    /// Opt-in for single-user machines: unauthenticated loopback requests run as `Principal::Operator`
    /// without Unix peer-UID checks, while `Authorization: Bearer <token>` pins to a seat or worker.
    #[arg(long)]
    pub http: Option<SocketAddr>,
    /// `MailUnrung` reconnect grace window in milliseconds (tests shorten it).
    #[arg(long, default_value_t = rex_wave_notify::RECONNECT_GRACE_MS, hide = true)]
    pub herald_grace_ms: u64,
}

pub fn default_t3_home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .map(|h| PathBuf::from(h).join(".t3"))
}

pub fn resolve_runner_mode(args: &DaemonArgs) -> RunnerMode {
    if let Some(mode) = args.runner {
        return mode;
    }
    if args.t3_url.is_some() || args.t3_home.is_some() {
        return RunnerMode::Both;
    }
    if args.bb_url != DEFAULT_BB_URL {
        return RunnerMode::Bb;
    }
    if default_t3_home().is_some_and(|home| runtime_file_path(&home).is_file()) {
        RunnerMode::Both
    } else {
        RunnerMode::Bb
    }
}

pub fn build_t3_config(args: &DaemonArgs, state_dir: &Path) -> Result<T3Config, String> {
    let mut cfg = if let Some(ref url) = args.t3_url {
        let origin = T3Origin::parse(url).map_err(|e| format!("invalid --t3-url: {e}"))?;
        if let Some(ref tok) = args.t3_token {
            let mut c = T3Config::explicit(url, tok.clone())
                .map_err(|e| format!("invalid --t3-url: {e}"))?;
            if let Some(ref home) = args.t3_home {
                c = c.with_t3_home(home.clone());
            }
            c
        } else if let Some(ref home) = args.t3_home {
            let mut c = T3Config::from_home(home.clone());
            c.endpoint = T3EndpointSource::Explicit(origin);
            c
        } else {
            T3Config::explicit(url, "").map_err(|e| format!("invalid --t3-url: {e}"))?
        }
    } else {
        let home = args
            .t3_home
            .clone()
            .or_else(default_t3_home)
            .ok_or_else(|| "cannot resolve ~/.t3; pass --t3-home or --t3-url".to_owned())?;
        let mut c = T3Config::from_home(home);
        if let Some(ref tok) = args.t3_token
            && !tok.is_empty()
        {
            c.auth = T3AuthSource::ExplicitToken(tok.clone());
        }
        c
    };

    cfg = cfg
        .with_state_dir(state_dir.to_path_buf())
        .with_system_principal(Principal::System {
            part: SystemPart::T3Bridge,
        });
    Ok(cfg)
}

pub fn resolve_socket_path(override_path: Option<&Path>) -> PathBuf {
    match override_path {
        Some(p) => p.to_path_buf(),
        None => match Endpoint::default_unix() {
            Endpoint::Unix(p) => p,
            Endpoint::WebSocket(_) => std::env::temp_dir().join("rex-wave/daemon.sock"),
        },
    }
}

pub fn state_dir_for(sock_path: &Path) -> PathBuf {
    sock_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn tokens_path_for(sock_path: &Path) -> PathBuf {
    sock_path.with_file_name("tokens.json")
}

fn token_store_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct TokenStoreGuard {
    _in_proc: std::sync::MutexGuard<'static, ()>,
    _lock_file: std::fs::File,
}

fn lock_token_store(tokens_path: &Path) -> io::Result<TokenStoreGuard> {
    let in_proc = token_store_lock().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(parent) = tokens_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let lock_path = tokens_path.with_extension("json.lock");
    let lock_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&lock_path)?;
    lock_file.lock()?;
    Ok(TokenStoreGuard {
        _in_proc: in_proc,
        _lock_file: lock_file,
    })
}

fn random_token(prefix: &str) -> io::Result<String> {
    let mut entropy = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut entropy)?;
    let mut token = String::from(prefix);
    for byte in entropy {
        use std::fmt::Write as _;
        let _ = write!(token, "{byte:02x}");
    }
    Ok(token)
}

/// Mints a random 256-bit capability token for `principal` via [`PrincipalTokenTable`].
///
/// - When `thread_id` is `Some`, persists a mode-`0600` [`WorkerTokenFile`] under
///   `<state_dir>/worker-tokens/<thread_id>` (reusing an existing valid file for the
///   same principal idempotently) so unlinking that file revokes access immediately.
/// - Otherwise persists the token atomically to `<state_dir>/tokens.json` (`0600`)
///   under a cross-process `tokens.json.lock` guard.
pub fn mint_principal_token(
    sock_path: &Path,
    principal: &Principal,
    thread_id: Option<&str>,
) -> io::Result<String> {
    if principal.is_operator_or_system() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "cannot mint bearer token for Operator or System principals",
        ));
    }

    let prefix = match principal {
        Principal::Worker { .. } => "rw_w_",
        _ => "rw_",
    };

    let tokens_path = tokens_path_for(sock_path);
    let _guard = lock_token_store(&tokens_path)?;

    if let Some(tid) = thread_id {
        if !matches!(principal, Principal::Worker { .. }) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "thread-scoped worker token files only allow Principal::Worker",
            ));
        }
        if !is_safe_worker_token_id(tid) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker token thread id must be a single safe filename",
            ));
        }
        let state_dir = state_dir_for(sock_path);
        let dir = worker_tokens_dir(&state_dir);
        std::fs::create_dir_all(&dir)?;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        let path = dir.join(tid);
        if let Ok(Some(raw)) = read_private_file(&path, None, "worker token file")
            && let Ok(existing) = serde_json::from_slice::<WorkerTokenFile>(&raw)
            && existing.principal == *principal
            && !existing.token.is_empty()
        {
            return Ok(existing.token);
        }
        let token = random_token(prefix)?;
        write_worker_token_file(&dir, tid, &token, principal)?;
        return Ok(token);
    }

    let mut table = PrincipalTokenTable::load_tokens_file(&tokens_path, None)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    let token = random_token(prefix)?;
    table.remove_principal(principal);
    table
        .insert(token.clone(), principal.clone())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    table.save_tokens_file(&tokens_path)?;
    Ok(token)
}

/// Mints a random 256-bit capability token for `seat` and atomically writes it
/// into `<state_dir>/tokens.json` with `0600` permissions.
pub fn mint_seat_token(sock_path: &Path, seat: &SeatName) -> io::Result<String> {
    mint_principal_token(sock_path, &Principal::Seat { seat: seat.clone() }, None)
}

/// Mints a random 256-bit capability token for `Principal::Worker { wave, role }`.
/// When `thread_id` is `Some`, writes `<state_dir>/worker-tokens/<thread_id>` (`0600`);
/// otherwise stores the principal-scoped token in `<state_dir>/tokens.json` (`0600`).
pub fn mint_worker_token(
    sock_path: &Path,
    wave: &WaveSlug,
    role: &RoleName,
    thread_id: Option<&str>,
) -> io::Result<String> {
    mint_principal_token(
        sock_path,
        &Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        },
        thread_id,
    )
}

fn spawn_herald_grace_timer(
    handle: HeraldHandle,
    client: LocalClient,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        const BASE_BACKOFF_MS: u64 = 250;
        const MAX_BACKOFF_MS: u64 = 5_000;
        let mut err_streak: u32 = 0;
        loop {
            let now = rex_wave_runtime::now();
            match handle.flush_due_at(now, &client).await {
                Ok(()) => err_streak = 0,
                Err(err) => {
                    if err_streak == 0 {
                        eprintln!("rex-wave daemon: herald grace flush failed: {err}");
                    }
                    err_streak = err_streak.saturating_add(1);
                    let shift = err_streak.saturating_sub(1).min(5);
                    let backoff_ms = BASE_BACKOFF_MS
                        .saturating_mul(1u64 << shift)
                        .min(MAX_BACKOFF_MS);
                    tokio::select! {
                        () = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                        () = handle.notified() => {}
                    }
                    continue;
                }
            }
            match handle.next_grace_deadline(now) {
                Some(deadline) => {
                    let rem_ms = deadline.0.saturating_sub(now.0).saturating_add(5);
                    tokio::select! {
                        () = tokio::time::sleep(Duration::from_millis(rem_ms)) => {}
                        () = handle.notified() => {}
                    }
                }
                None => {
                    handle.notified().await;
                }
            }
        }
    })
}

enum UdsUnbindScope<'a> {
    AllOrphanedOnStart,
    SingleConn(u64),
    AllNotIn(&'a Mutex<HashSet<u64>>),
}

async fn unbind_uds_sessions(monitor: &LocalClient, scope: UdsUnbindScope<'_>) {
    let snap = match monitor
        .call(PresenceList {
            project: None,
            live_only: false,
            include_retired: true,
        })
        .await
    {
        Ok(snap) => snap,
        Err(err) => {
            eprintln!("rex-wave daemon: presence list during UDS unbind failed: {err}");
            return;
        }
    };
    let live_snapshot = match scope {
        UdsUnbindScope::AllNotIn(tracked) => {
            Some(tracked.lock().unwrap_or_else(|e| e.into_inner()).clone())
        }
        _ => None,
    };
    for seat_view in snap.seats {
        for binding in seat_view.bindings {
            if !matches!(
                binding.status,
                BindingStatus::Live | BindingStatus::TransportDown
            ) {
                continue;
            }
            let TransportEndpoint::UdsSession { conn_id, .. } = binding.endpoint else {
                continue;
            };
            let reason = match scope {
                UdsUnbindScope::AllOrphanedOnStart => {
                    format!("orphaned uds connection #{conn_id} swept on daemon start")
                }
                UdsUnbindScope::SingleConn(want) => {
                    if want != conn_id {
                        continue;
                    }
                    format!("uds connection #{want} disconnected")
                }
                UdsUnbindScope::AllNotIn(_) => {
                    if live_snapshot
                        .as_ref()
                        .is_some_and(|live| live.contains(&conn_id))
                    {
                        continue;
                    }
                    format!("uds connection #{conn_id} disconnected (overflow sweep)")
                }
            };
            if let Err(err) = monitor
                .call(PresenceUnbind {
                    seat: binding.seat.clone(),
                    incarnation_id: binding.incarnation_id.clone(),
                    reason: Some(reason),
                })
                .await
            {
                eprintln!(
                    "rex-wave daemon: presence unbind failed for {} ({}): {err}",
                    binding.seat, binding.incarnation_id
                );
            }
        }
    }
}

struct PresenceDisconnectHooks {
    tracked: Arc<Mutex<HashSet<u64>>>,
    overflow_sweep: Arc<AtomicBool>,
    disconnect_tx: tokio::sync::mpsc::Sender<u64>,
}

impl ConnectionHooks for PresenceDisconnectHooks {
    fn on_uds_presence_bind(&self, conn_id: u64) {
        self.tracked
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(conn_id);
    }

    fn on_disconnect(&self, conn_id: u64, _principal: &Principal) {
        let had_binding = self
            .tracked
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&conn_id);
        if had_binding && let Err(err) = self.disconnect_tx.try_send(conn_id) {
            self.overflow_sweep.store(true, Ordering::Release);
            eprintln!(
                "rex-wave daemon: disconnect queue full for conn #{conn_id} (scheduled overflow sweep): {err}"
            );
        }
    }
}

fn spawn_presence_disconnect_worker(
    monitor: LocalClient,
    mut rx: tokio::sync::mpsc::Receiver<u64>,
    tracked: Arc<Mutex<HashSet<u64>>>,
    overflow_sweep: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(conn_id) = rx.recv().await {
            unbind_uds_sessions(&monitor, UdsUnbindScope::SingleConn(conn_id)).await;
            if overflow_sweep.swap(false, Ordering::AcqRel) {
                unbind_uds_sessions(&monitor, UdsUnbindScope::AllNotIn(&tracked)).await;
            }
        }
    })
}

fn journal_thread_resolver(journal: Journal) -> ThreadIdResolver {
    Arc::new(move |wave: &WaveSlug, role: &RoleName| {
        let journal = journal.clone();
        let stream = StreamId::Wave { wave: wave.clone() };
        let role = role.clone();
        Box::pin(async move {
            let mut after = Seq::ORIGIN;
            let mut latest: Option<BbThreadId> = None;
            loop {
                let Ok(page) = journal.read(&stream, after, 512) else {
                    break;
                };
                if page.is_empty() {
                    break;
                }
                for env in page {
                    after = env.seq;
                    if let Event::Wave(WaveEvent::WorkerObserved {
                        role: obs_role,
                        observation: Observation::Spawned { thread, .. },
                    }) = env.event
                        && obs_role == role
                    {
                        latest = Some(thread);
                    }
                }
            }
            latest
        })
    })
}

pub async fn run(args: DaemonArgs, socket_override: Option<PathBuf>) -> Result<(), String> {
    let operator = SeatName::parse(&args.operator).map_err(|e| e.to_string())?;
    if args.herald_grace_ms == 0 {
        return Err("--herald-grace-ms must be positive".into());
    }
    let house =
        rex_wave_mailbridge::load_house_profile().map_err(|e| format!("house profile: {e}"))?;
    let presence = Presence::with_house_profile(house);
    let sock_path = resolve_socket_path(socket_override.as_deref());
    let state_dir = state_dir_for(&sock_path);
    let journal_path = args
        .journal
        .clone()
        .unwrap_or_else(|| sock_path.with_file_name("journal.sqlite3"));
    if let Some(parent) = journal_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create state dir: {e}"))?;
    }
    std::fs::create_dir_all(&state_dir).map_err(|e| format!("create socket dir: {e}"))?;

    let runner_mode = resolve_runner_mode(&args);
    let t3_cfg = match runner_mode {
        RunnerMode::T3 | RunnerMode::Both => Some(build_t3_config(&args, &state_dir)?),
        RunnerMode::Bb => None,
    };

    let http_listener = match args.http {
        Some(addr) => {
            let listener = bind_http(addr)
                .await
                .map_err(|e| format!("bind http {addr}: {e}"))?;
            if let Ok(local) = listener.local_addr() {
                eprintln!("rex-wave daemon: http listening on {local}");
            }
            Some(listener)
        }
        None => None,
    };

    let journal = Journal::open(&journal_path).map_err(|e| format!("open journal: {e}"))?;
    let hub = Hub::new(4096);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let sandbox =
        SandboxRoot::verify(&args.mail_sandbox).map_err(|e| format!("mail sandbox: {e}"))?;
    let mail_writer = journal
        .claim(StreamId::Mail)
        .map_err(|e| format!("claim mail stream: {e}"))?;

    let bb_bridge = match runner_mode {
        RunnerMode::Bb | RunnerMode::Both => Some(BbBridge::with_code_runner(
            BridgeConfig {
                url: args.bb_url.clone(),
                token: args.bb_token.clone(),
            },
            Arc::new(|client, args| {
                Box::pin(async move {
                    let res = rex_wave_mcp::McpServer::new(client)
                        .call_tool(rex_wave_mcp::RUN_CODE_TOOL, args)
                        .await;
                    let text = res
                        .content
                        .into_iter()
                        .find_map(|c| c.get("text").and_then(|t| t.as_str()).map(str::to_owned))
                        .unwrap_or_else(|| "null".into());
                    if res.is_error { Err(text) } else { Ok(text) }
                })
            }),
        )),
        RunnerMode::T3 => None,
    };

    let t3_bridge = t3_cfg.map(T3Bridge::new);

    let composite_feed: Arc<dyn AgentFeed> = Arc::new(
        CompositeFeed::new(
            bb_bridge.as_ref().map(|b| b.clone() as Arc<dyn AgentFeed>),
            t3_bridge
                .as_ref()
                .map(|t| Arc::new(T3Feed(t.clone())) as Arc<dyn AgentFeed>),
        )
        .with_thread_resolver(journal_thread_resolver(journal.clone())),
    );

    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Registry::<Waves>::new(journal.clone(), hub.clone()),
        board: Registry::<Board>::new(journal.clone(), hub.clone()),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(MailBridge::with_hub(
            sandbox.clone(),
            journal.clone(),
            mail_writer,
            hub.clone(),
        )),
        presence: presence.registry(journal.clone(), hub.clone()),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: composite_feed,
        }),
    });

    let default_target = match runner_mode {
        RunnerMode::T3 => RunnerTarget::T3,
        RunnerMode::Bb | RunnerMode::Both => RunnerTarget::Bb,
    };
    let composite_host = CompositeHost::new(
        bb_bridge.as_ref().map(|b| BbHost(b.clone())),
        t3_bridge.as_ref().map(|t| T3Host(t.clone())),
        default_target,
    );

    let system = |part| LocalClient::new(router.clone(), Principal::System { part });
    let conductor_task = spawn_reactor(
        WaveConductor::new(composite_host),
        system(SystemPart::WaveConductor),
    );
    let herald = Herald::with_grace(Duration::from_millis(args.herald_grace_ms))
        .with_operator_surface_seat(presence.house().operator_seat.clone());
    let herald_handle = herald.handle();
    let herald_client = system(SystemPart::Herald);
    let herald_task = spawn_reactor(herald, herald_client.clone());
    let herald_grace_task = spawn_herald_grace_timer(herald_handle, herald_client);
    let bell_task = spawn_reactor(BellCourier::new(sandbox), system(SystemPart::WakeBell));
    let bb_task = bb_bridge.map(|b| tokio::spawn(b.run(router.clone())));
    let t3_task = t3_bridge.map(|t| tokio::spawn(t.run(router.clone())));
    let sink_task = tokio::spawn(rex_wave_mailbridge::serve_wake_bell_sink(
        args.wake_bell,
        system(SystemPart::WakeBell),
    ));

    let presence_monitor = system(SystemPart::PresenceMonitor);
    unbind_uds_sessions(&presence_monitor, UdsUnbindScope::AllOrphanedOnStart).await;
    let tracked_uds_conns = Arc::new(Mutex::new(HashSet::new()));
    let overflow_sweep = Arc::new(AtomicBool::new(false));
    let (disconnect_tx, disconnect_rx) = tokio::sync::mpsc::channel(256);
    let presence_disconnect_task = spawn_presence_disconnect_worker(
        presence_monitor,
        disconnect_rx,
        tracked_uds_conns.clone(),
        overflow_sweep.clone(),
    );
    let hooks: Arc<dyn ConnectionHooks> = Arc::new(PresenceDisconnectHooks {
        tracked: tracked_uds_conns,
        overflow_sweep,
        disconnect_tx,
    });

    let (listener, daemon_uid) =
        bind_unix(&sock_path).map_err(|e| format!("bind {}: {e}", sock_path.display()))?;
    let auth: Arc<dyn Authenticator> = Arc::new(StateDirAuthenticator::from_state_dir(
        operator.clone(),
        Some(daemon_uid),
        &state_dir,
    ));

    let http_task = http_listener.map(|hl| {
        let router = router.clone();
        let auth = auth.clone();
        let op = operator.clone();
        tokio::spawn(async move {
            if let Err(err) = serve_http_listener(hl, router, auth, op, HttpConfig::default()).await
            {
                eprintln!("rex-wave daemon: http listener exited: {err}");
            }
        })
    });

    let serve_result = tokio::select! {
        res = serve_listener_with_hooks(listener, router, auth, hooks) => {
            res.map_err(|e| format!("uds serve: {e}"))
        }
        sig = tokio::signal::ctrl_c() => {
            sig.map_err(|e| format!("ctrl_c listener: {e}"))
        }
    };

    conductor_task.abort();
    herald_task.abort();
    herald_grace_task.abort();
    bell_task.abort();
    presence_disconnect_task.abort();
    if let Some(t) = bb_task {
        t.abort();
    }
    if let Some(t) = t3_task {
        t.abort();
    }
    if let Some(t) = http_task {
        t.abort();
    }
    sink_task.abort();
    let _ = std::fs::remove_file(&sock_path);

    serve_result
}
