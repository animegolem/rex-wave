//! Daemon composition root wiring `Journal`, `Hub`, `SidebarProjection`,
//! subsystem `Registry`s (`Rooms`, `Waves`, `Board`, `Inbox`, `Presence`),
//! `MailBridge`, reactors (`WaveConductor`, `Herald` with `HeraldHandle`
//! reconnect-grace timer, `BellCourier`), `HostRouter` / `FeedRouter`
//! (`BbBridge`, `T3Bridge`, `ClaudeBridge`, `CodexBridge`), the loopback
//! `/sink` listener for `wake-bell`, optional loopback `--http` HTTP/1.1 + SSE
//! server, and UDS `serve_listener_with_hooks` (`StateDirAuthenticator` + RAII
//! `PresenceUnbind`) with graceful `ctrl_c` shutdown.

use crate::config_file::{TomlConfigHook, daemon_config_file_path, reconcile_startup_config};
use crate::http::{HttpConfig, bind_http, serve_http_listener};
use rex_wave_acp::{AcpBridge, AcpConfig, AcpFeed, AcpHost};
use rex_wave_api::config::RunnerRoute;
use rex_wave_api::presence::{BindingStatus, PresenceList, PresenceUnbind, TransportEndpoint};
use rex_wave_api::wave::{Observation, WaveEvent};
use rex_wave_api::*;
use rex_wave_bb::{BbBridge, BbHost, BridgeConfig};
use rex_wave_board::Board;
use rex_wave_mailbridge::{BellCourier, MailBridge, Presence, SandboxRoot};
use rex_wave_native::{
    ClaudeBridge, ClaudeFeed, ClaudeHost, ClaudeRunnerConfig, CodexBridge, CodexFeed, CodexHost,
    CodexRunnerConfig,
};
use rex_wave_notify::{Herald, HeraldHandle, Inbox};
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    AgentFeed, ConfigService, FeedRouter, Hub, LocalClient, ReadSide, Registry, Router,
    SidebarProjection, ThreadIdResolver, spawn_reactor,
};
use rex_wave_store::Journal;
use rex_wave_t3::{
    T3AuthSource, T3Bridge, T3Config, T3EndpointSource, T3Feed, T3Host, T3Origin, runtime_file_path,
};
use rex_wave_transport::{
    Authenticator, ConnectionHooks, Endpoint, PrincipalTokenTable, StateDirAuthenticator,
    bind_unix_prepared, is_safe_worker_token_id, mint_random_token, read_private_file,
    serve_listener_with_hooks,
};
use rex_wave_waves::{HostRouter, WaveConductor, Waves};
use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const DEFAULT_BB_URL: &str = "ws://127.0.0.1:38886/api/v1/plugins/rex-wave/http/bridge";

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerMode {
    Bb,
    T3,
    Both,
    Acp,
    Claude,
    Codex,
    Native,
}

#[derive(clap::Args, Debug, Clone)]
pub struct DaemonArgs {
    /// Journal path (default: `<socket_dir>/journal.sqlite3`).
    #[arg(long)]
    pub journal: Option<PathBuf>,
    /// Operator configuration file path (default: `$XDG_CONFIG_HOME/rex-wave/config.toml`, `<state_dir>/config.toml` under `--socket`, or `~/.config/rex-wave/config.toml`; pass `--config ""` to disable file sync).
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Non-production mail sandbox root (must contain `.rex-wave-sandbox`).
    #[arg(long)]
    pub mail_sandbox: PathBuf,
    /// BB plugin bridge WebSocket URL.
    #[arg(long, default_value = DEFAULT_BB_URL)]
    pub bb_url: String,
    /// BB plugin bridge bearer token.
    #[arg(long, env = "REX_WAVE_BB_TOKEN", default_value = "")]
    pub bb_token: String,
    /// Worker host runner backend (`bb`, `t3`, `both`, `acp`, `claude`, `codex`, or `native`).
    /// Defaults to `both` when T3 is configured or a local T3 runtime exists, otherwise `bb`.
    /// Native runners (`acp:*`, `claude:*`, `codex:*`) are always registered alongside.
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
    #[arg(long, env = "REX_WAVE_OPERATOR", default_value = "owner")]
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
    #[arg(long, hide = true)]
    pub herald_grace_ms: Option<u64>,
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

fn lock_token_store(tokens_path: &Path) -> io::Result<rex_wave_transport::StateTokenLockGuard> {
    let state_dir = tokens_path.parent().unwrap_or_else(|| Path::new("."));
    rex_wave_transport::lock_state_dir_tokens(state_dir)
}

fn operator_seat_path(state_dir: &Path) -> PathBuf {
    state_dir.join("operator-seat")
}

fn persist_operator_seat(state_dir: &Path, operator: &SeatName) -> io::Result<()> {
    let path = operator_seat_path(state_dir);
    rex_wave_fs::write_atomic_path(&path, format!("{operator}\n").as_bytes(), 0o600, false)
}

/// Resolves the effective operator seat for `sock_path` using the same source
/// of truth as the daemon: `<state_dir>/operator-seat` when written by a daemon
/// on `sock_path`, then `REX_WAVE_OPERATOR`, then `"owner"`.
pub fn resolve_effective_operator(sock_path: &Path) -> SeatName {
    let env_val = std::env::var("REX_WAVE_OPERATOR").ok();
    resolve_effective_operator_with_env(sock_path, env_val.as_deref())
}

fn resolve_effective_operator_with_env(sock_path: &Path, env_val: Option<&str>) -> SeatName {
    let state_dir = state_dir_for(sock_path);
    if let Ok(Some(bytes)) =
        read_private_file(&operator_seat_path(&state_dir), None, "operator-seat")
        && let Ok(text) = std::str::from_utf8(&bytes)
        && let Ok(seat) = SeatName::parse(text.trim())
    {
        return seat;
    }
    if let Some(raw) = env_val
        && let Ok(seat) = SeatName::parse(raw.trim())
    {
        return seat;
    }
    SeatName::parse("owner").expect("valid default operator seat")
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
    let effective_operator = resolve_effective_operator(sock_path);
    mint_principal_token_for_operator(sock_path, principal, thread_id, &effective_operator)
}

fn mint_principal_token_for_operator(
    sock_path: &Path,
    principal: &Principal,
    thread_id: Option<&str>,
    effective_operator: &SeatName,
) -> io::Result<String> {
    if principal.is_operator_or_system() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "cannot mint bearer token for Operator or System principals",
        ));
    }
    if let Principal::Seat { seat } = principal
        && seat == effective_operator
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "cannot mint a Seat token for operator seat '{seat}'; the operator authenticates via PeerUid or an Operator entry"
            ),
        ));
    }

    let prefix = match principal {
        Principal::Worker { .. } => "rw_w_",
        _ => "rw_",
    };

    let tokens_path = tokens_path_for(sock_path);
    let guard = lock_token_store(&tokens_path)?;

    if let Some(tid) = thread_id {
        let Principal::Worker { wave, role } = principal else {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "thread-scoped worker token files only allow Principal::Worker",
            ));
        };
        if !is_safe_worker_token_id(tid) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker token thread id must be a single safe filename",
            ));
        }
        let parsed_tid = HostThreadId::parse(tid)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        let state_dir = state_dir_for(sock_path);
        let provisioned = rex_wave_transport::provision_worker_token_locked(
            &guard,
            &state_dir,
            wave,
            role,
            &parsed_tid,
        )?;
        return Ok(provisioned.token);
    }

    let mut table = PrincipalTokenTable::load_tokens_file(&tokens_path, None)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    let token = mint_random_token(prefix)?;
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
                    expected_epoch: Some(binding.epoch),
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

fn spawn_mail_reconcile_poller(mail: Arc<MailBridge>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        const POLL_INTERVAL: Duration = Duration::from_secs(5);
        const MAX_BACKOFF: Duration = Duration::from_secs(30);
        let mut err_streak: u32 = 0;
        loop {
            let sleep_dur = if err_streak == 0 {
                POLL_INTERVAL
            } else {
                let shift = err_streak.saturating_sub(1).min(4);
                Duration::from_secs(5u64.saturating_mul(1u64 << shift)).min(MAX_BACKOFF)
            };
            tokio::time::sleep(sleep_dur).await;
            match mail.reconcile().await {
                Ok(_) => err_streak = 0,
                Err(err) => {
                    if err_streak == 0 {
                        eprintln!("rex-wave daemon: periodic mail reconcile failed: {err}");
                    }
                    err_streak = err_streak.saturating_add(1);
                }
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
                    match env.event {
                        Event::Wave(WaveEvent::WorkerRequested { role: req_role, .. })
                            if req_role == role =>
                        {
                            latest = None;
                        }
                        Event::Wave(WaveEvent::WorkerObserved {
                            role: obs_role,
                            observation: Observation::Spawned { thread, .. },
                        }) if obs_role == role => {
                            latest = Some(thread);
                        }
                        _ => {}
                    }
                }
            }
            latest
        })
    })
}

pub async fn run(args: DaemonArgs, socket_override: Option<PathBuf>) -> Result<(), String> {
    let operator = SeatName::parse(&args.operator).map_err(|e| e.to_string())?;
    if args.herald_grace_ms == Some(0) {
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
    let sandbox =
        SandboxRoot::verify(&args.mail_sandbox).map_err(|e| format!("mail sandbox: {e}"))?;
    let mut prepared_sock = rex_wave_fs::validate_unix_socket_target(&sock_path)
        .map_err(|e| format!("bind {}: {e}", sock_path.display()))?;
    prepared_sock.arm_cleanup_on_drop();
    rex_wave_fs::create_missing_private_dir(&state_dir)
        .map_err(|e| format!("create socket dir {}: {e}", state_dir.display()))?;
    if let Some(parent) = journal_path.parent()
        && parent != state_dir.as_path()
    {
        rex_wave_fs::create_missing_private_dir(parent)
            .map_err(|e| format!("create state dir {}: {e}", parent.display()))?;
    }

    let runner_mode = resolve_runner_mode(&args);
    let default_target = match runner_mode {
        RunnerMode::T3 => RunnerKind::T3,
        RunnerMode::Acp => RunnerKind::Acp,
        RunnerMode::Claude | RunnerMode::Native => RunnerKind::Claude,
        RunnerMode::Codex => RunnerKind::Codex,
        RunnerMode::Bb | RunnerMode::Both => RunnerKind::Bb,
    };
    let t3_cfg = match runner_mode {
        RunnerMode::T3 | RunnerMode::Both => {
            Some(build_t3_config(&args, &state_dir)?.with_default_runner(default_target))
        }
        RunnerMode::Bb
        | RunnerMode::Acp
        | RunnerMode::Claude
        | RunnerMode::Codex
        | RunnerMode::Native => None,
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
    let config_path = daemon_config_file_path(
        args.config.as_deref(),
        socket_override.is_some(),
        &state_dir,
    );
    let config_hook = TomlConfigHook::new(
        config_path.clone(),
        state_dir.clone(),
        RunnerRoute::from_runner_kind(default_target),
        matches!(runner_mode, RunnerMode::Bb | RunnerMode::Both),
        matches!(runner_mode, RunnerMode::T3 | RunnerMode::Both),
    );
    let config_svc =
        ConfigService::with_hook(journal.clone(), hub.clone(), Some(config_hook.clone()))
            .map_err(|e| format!("init config service: {e}"))?;
    let reconciled_cfg =
        reconcile_startup_config(&journal, &config_svc, config_path.as_deref(), &state_dir).await?;

    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let mail_writer = journal
        .claim(StreamId::Mail)
        .map_err(|e| format!("claim mail stream: {e}"))?;
    let (listener, daemon_uid) = bind_unix_prepared(&sock_path, &prepared_sock)
        .map_err(|e| format!("bind {}: {e}", sock_path.display()))?;
    persist_operator_seat(&state_dir, &operator)
        .map_err(|e| format!("persist operator seat: {e}"))?;

    let bb_bridge = match runner_mode {
        RunnerMode::Bb | RunnerMode::Both => Some(
            BbBridge::with_code_runner(
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
            )
            .with_default_runner(default_target)
            .with_config_watch(config_svc.subscribe())
            .with_pin_store(state_dir.join("router-pins.json")),
        ),
        RunnerMode::T3
        | RunnerMode::Acp
        | RunnerMode::Claude
        | RunnerMode::Codex
        | RunnerMode::Native => None,
    };

    let t3_bridge = t3_cfg.map(|cfg| T3Bridge::new(cfg.with_config_watch(config_svc.subscribe())));
    let acp_bridge = AcpBridge::new(
        AcpConfig::from_env()
            .with_state_dir(state_dir.clone())
            .with_socket_path(sock_path.clone())
            .with_default_runner(default_target)
            .with_config_watch(config_svc.subscribe()),
    );
    let mut claude_cfg = ClaudeRunnerConfig::from_env()
        .with_state_dir(state_dir.clone())
        .with_socket_path(sock_path.clone())
        .with_config_watch(config_svc.subscribe());
    let mut codex_cfg = CodexRunnerConfig::from_env()
        .with_state_dir(state_dir.clone())
        .with_socket_path(sock_path.clone())
        .with_config_watch(config_svc.subscribe());
    if let Ok(exe) = std::env::current_exe() {
        claude_cfg = claude_cfg.with_rex_wave_bin(exe.clone());
        codex_cfg = codex_cfg.with_rex_wave_bin(exe);
    }
    let claude_bridge = ClaudeBridge::new(claude_cfg);
    let codex_bridge = CodexBridge::new(codex_cfg);
    config_hook.bind_runner_liveness(
        bb_bridge.as_ref().map(|b| {
            let b = b.clone();
            Arc::new(move || b.connected()) as _
        }),
        t3_bridge.as_ref().map(|t| {
            let t = t.clone();
            Arc::new(move || t.connected()) as _
        }),
    );
    let bb_conn_rx = bb_bridge.as_ref().map(|b| b.watch_connected());
    let t3_conn_rx = t3_bridge.as_ref().map(|t| t.watch_connected());

    let mut feed_entries: Vec<(RunnerKind, Arc<dyn AgentFeed>)> = Vec::new();
    feed_entries.push((RunnerKind::Acp, Arc::new(AcpFeed(acp_bridge.clone()))));
    if let Some(ref t) = t3_bridge {
        feed_entries.push((RunnerKind::T3, Arc::new(T3Feed(t.clone()))));
    }
    if let Some(ref b) = bb_bridge {
        feed_entries.push((RunnerKind::Bb, b.clone()));
    }
    feed_entries.push((
        RunnerKind::Claude,
        Arc::new(ClaudeFeed(claude_bridge.clone())),
    ));
    feed_entries.push((RunnerKind::Codex, Arc::new(CodexFeed(codex_bridge.clone()))));
    let composite_feed: Arc<dyn AgentFeed> = Arc::new(
        FeedRouter::new(feed_entries)
            .with_thread_resolver(journal_thread_resolver(journal.clone())),
    );

    let presence_svc = presence.registry(journal.clone(), hub.clone());
    config_hook.bind_presence(presence_svc.clone());

    let wave_svc = rex_wave_waves::WaveService::new(
        Registry::<Waves>::new(journal.clone(), hub.clone()),
        default_target,
        Some(config_svc.subscribe()),
    )
    .with_state_dir(state_dir.clone());

    let room_svc = Registry::<Rooms>::new(journal.clone(), hub.clone());
    let mail_bridge = Arc::new(
        MailBridge::open_with_hub(sandbox.clone(), journal.clone(), mail_writer, hub.clone())
            .map_err(|e| format!("init mail bridge: {e}"))?
            .with_room_service(room_svc.clone())
            .with_config_watch(config_svc.subscribe())
            .with_house_profile(presence.house().clone())
            .with_drop_quiet_ms(2_000),
    );
    match mail_bridge.reconcile().await {
        Ok(report) => {
            if report.files_ingested > 0
                || report.acks_ingested > 0
                || report.blobs_exported > 0
                || report.working_tree_exported > 0
                || report.acks_exported > 0
            {
                eprintln!(
                    "rex-wave daemon: startup mail reconcile (ingested={} acks_in={} blobs_out={} letters_out={} acks_out={} drop_consumed={} flagged={})",
                    report.files_ingested,
                    report.acks_ingested,
                    report.blobs_exported,
                    report.working_tree_exported,
                    report.acks_exported,
                    report.drop_files_consumed,
                    report.mutated_after_ack_flagged,
                );
            }
        }
        Err(ApiError::Denied { action, .. }) => {
            return Err(format!("startup mail reconcile refused: {action}"));
        }
        Err(err) => {
            eprintln!("rex-wave daemon: startup mail reconcile warning (will retry): {err}");
        }
    }

    let router = Arc::new(Router {
        room: room_svc,
        wave: wave_svc.clone(),
        board: Registry::<Board>::new(journal.clone(), hub.clone()),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: mail_bridge.clone(),
        presence: presence_svc,
        config: config_svc.clone(),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: composite_feed,
        }),
    });

    acp_bridge.attach_backend(router.clone());
    claude_bridge.attach_router(router.clone());
    codex_bridge.attach_router(router.clone());

    let host_router = Arc::new(
        HostRouter::new(default_target)
            .with_config_watch(config_svc.subscribe())
            .with_pin_store(state_dir.join("router-pins.json"))
            .with_host(RunnerKind::Acp, AcpHost(acp_bridge.clone()))
            .with_host(RunnerKind::Claude, ClaudeHost(claude_bridge.clone()))
            .with_host(RunnerKind::Codex, CodexHost(codex_bridge.clone()))
            .with_optional_host(
                RunnerKind::Bb,
                bb_bridge.as_ref().map(|b| BbHost(b.clone())),
            )
            .with_optional_host(
                RunnerKind::T3,
                t3_bridge.as_ref().map(|t| T3Host(t.clone())),
            ),
    );
    wave_svc.bind_host(host_router.clone());

    let system = |part| LocalClient::new(router.clone(), Principal::System { part });
    let conductor_task = spawn_reactor(
        WaveConductor::new(host_router),
        system(SystemPart::WaveConductor),
    );
    let effective_herald_grace_ms = args
        .herald_grace_ms
        .unwrap_or(reconciled_cfg.mail.reconnect_grace_ms);
    let herald = Herald::with_grace(Duration::from_millis(effective_herald_grace_ms))
        .with_operator_surface_seat(presence.house().operator_seat.clone());
    let herald_handle = herald.handle();
    let herald_client = system(SystemPart::Herald);
    let herald_task = spawn_reactor(herald, herald_client.clone());
    let herald_grace_task = spawn_herald_grace_timer(herald_handle, herald_client);
    let bell_task = spawn_reactor(BellCourier::new(sandbox), system(SystemPart::WakeBell));
    let mail_reconcile_task = spawn_mail_reconcile_poller(mail_bridge);
    let bb_task = bb_bridge.map(|b| tokio::spawn(b.run(router.clone())));
    let t3_task = t3_bridge.map(|t| tokio::spawn(t.run(router.clone())));
    let acp_task = tokio::spawn(acp_bridge.clone().run(router.clone()));
    let mut bridge_health_tasks = Vec::new();
    for (transport, label, rx_opt) in [
        (
            rex_wave_api::presence::TransportKind::BbThread,
            "BB",
            bb_conn_rx,
        ),
        (
            rex_wave_api::presence::TransportKind::T3Thread,
            "T3 Code",
            t3_conn_rx,
        ),
    ] {
        if let Some(mut rx) = rx_opt {
            let mon = system(SystemPart::PresenceMonitor);
            bridge_health_tasks.push(tokio::spawn(async move {
                let init_live = *rx.borrow_and_update();
                let _ = mon
                    .call(rex_wave_api::presence::PresenceTransportHealth {
                        transport,
                        down: !init_live,
                        detail: (!init_live).then(|| format!("{label} bridge disconnected")),
                    })
                    .await;
                while rx.changed().await.is_ok() {
                    let live = *rx.borrow_and_update();
                    let _ = mon
                        .call(rex_wave_api::presence::PresenceTransportHealth {
                            transport,
                            down: !live,
                            detail: (!live).then(|| format!("{label} bridge disconnected")),
                        })
                        .await;
                }
            }));
        }
    }
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

    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| format!("sigterm listener: {e}"))?;

    #[cfg(unix)]
    let serve_result = tokio::select! {
        res = serve_listener_with_hooks(listener, router, auth, hooks) => {
            res.map_err(|e| format!("uds serve: {e}"))
        }
        sig = tokio::signal::ctrl_c() => {
            sig.map_err(|e| format!("ctrl_c listener: {e}"))
        }
        _ = sigterm.recv() => {
            Ok(())
        }
    };

    #[cfg(not(unix))]
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
    mail_reconcile_task.abort();
    presence_disconnect_task.abort();
    for t in bridge_health_tasks {
        t.abort();
    }
    if let Some(t) = bb_task {
        t.abort();
    }
    if let Some(t) = t3_task {
        t.abort();
    }
    acp_task.abort();
    if let Some(t) = http_task {
        t.abort();
    }
    sink_task.abort();
    tokio::join!(
        acp_bridge.shutdown_all(),
        claude_bridge.shutdown(),
        codex_bridge.shutdown(),
    );
    let _ = prepared_sock.cleanup_bound();

    serve_result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    #[test]
    fn mint_seat_token_tracks_configured_operator_seat() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("state/daemon.sock");
        let owner = SeatName::parse("owner").unwrap();
        let ops = SeatName::parse("ops").unwrap();
        let alice = SeatName::parse("alice").unwrap();

        let eff_default = resolve_effective_operator_with_env(&sock, None);
        assert_eq!(eff_default, owner);
        assert!(
            mint_principal_token_for_operator(
                &sock,
                &Principal::Seat {
                    seat: owner.clone()
                },
                None,
                &eff_default,
            )
            .is_err()
        );
        assert!(
            mint_principal_token_for_operator(
                &sock,
                &Principal::Seat { seat: ops.clone() },
                None,
                &eff_default,
            )
            .is_ok()
        );

        let eff_env = resolve_effective_operator_with_env(&sock, Some("ops"));
        assert_eq!(eff_env, ops);

        let state_dir = state_dir_for(&sock);
        persist_operator_seat(&state_dir, &ops).unwrap();
        assert_eq!(resolve_effective_operator_with_env(&sock, None), ops);
        assert!(mint_seat_token(&sock, &ops).is_err());
        assert!(mint_seat_token(&sock, &owner).is_ok());
        assert!(mint_seat_token(&sock, &alice).is_ok());
    }

    #[tokio::test]
    async fn daemon_rejects_invalid_sandbox_and_live_socket_without_clobbering_state() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("state");
        std::fs::create_dir(&state_dir).unwrap();
        std::fs::set_permissions(&state_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let unmarked_sb = dir.path().join("unmarked");
        std::fs::create_dir(&unmarked_sb).unwrap();

        let sock = state_dir.join("daemon.sock");
        let journal = state_dir.join("journal.sqlite3");
        let bad_args = DaemonArgs {
            journal: Some(journal.clone()),
            config: None,
            mail_sandbox: unmarked_sb,
            bb_url: "ws://127.0.0.1:1/disabled".into(),
            bb_token: String::new(),
            runner: Some(RunnerMode::Bb),
            t3_home: None,
            t3_url: None,
            t3_token: None,
            operator: "ops".into(),
            wake_bell: "127.0.0.1:0".parse().unwrap(),
            http: None,
            herald_grace_ms: Some(100),
        };
        assert!(run(bad_args, Some(sock.clone())).await.is_err());
        assert!(!sock.exists(), "failed start must not leave socket file");
        assert!(
            !operator_seat_path(&state_dir).exists(),
            "failed start must not publish operator-seat"
        );
        assert!(!journal.exists());

        let (live_listener, _) = rex_wave_transport::bind_unix(&sock).unwrap();
        persist_operator_seat(&state_dir, &SeatName::parse("owner").unwrap()).unwrap();
        let valid_sb = dir.path().join("sb");
        std::fs::create_dir(&valid_sb).unwrap();
        std::fs::write(valid_sb.join(".rex-wave-sandbox"), b"ok\n").unwrap();
        let second_args = DaemonArgs {
            journal: Some(journal),
            config: None,
            mail_sandbox: valid_sb,
            bb_url: "ws://127.0.0.1:1/disabled".into(),
            bb_token: String::new(),
            runner: Some(RunnerMode::Bb),
            t3_home: None,
            t3_url: None,
            t3_token: None,
            operator: "intruder".into(),
            wake_bell: "127.0.0.1:0".parse().unwrap(),
            http: None,
            herald_grace_ms: Some(100),
        };
        let second_err = run(second_args, Some(sock.clone())).await.unwrap_err();
        assert!(
            second_err.contains("already listening"),
            "unexpected error: {second_err}"
        );
        assert!(
            std::os::unix::net::UnixStream::connect(&sock).is_ok(),
            "healthy listener must remain reachable after failed second start"
        );
        assert_eq!(
            resolve_effective_operator_with_env(&sock, None).as_str(),
            "owner"
        );
        drop(live_listener);

        let held_prepare = rex_wave_fs::validate_unix_socket_target(&sock).unwrap();
        let concurrent_err = rex_wave_fs::validate_unix_socket_target(&sock)
            .err()
            .expect("concurrent startup lock must reject second caller");
        assert_eq!(concurrent_err.kind(), std::io::ErrorKind::AddrInUse);
        drop(held_prepare);
    }

    #[tokio::test]
    async fn daemon_rejects_insecure_preexisting_socket_dir_before_opening_journal() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("insecure-state");
        std::fs::create_dir(&state_dir).unwrap();
        std::fs::set_permissions(&state_dir, std::fs::Permissions::from_mode(0o775)).unwrap();
        let sandbox_dir = dir.path().join("sb");
        std::fs::create_dir(&sandbox_dir).unwrap();
        std::fs::write(sandbox_dir.join(".rex-wave-sandbox"), b"ok\n").unwrap();

        let sock = state_dir.join("daemon.sock");
        let journal = state_dir.join("journal.sqlite3");
        let args = DaemonArgs {
            journal: Some(journal.clone()),
            config: None,
            mail_sandbox: sandbox_dir,
            bb_url: "ws://127.0.0.1:1/disabled".into(),
            bb_token: String::new(),
            runner: Some(RunnerMode::Bb),
            t3_home: None,
            t3_url: None,
            t3_token: None,
            operator: "owner".into(),
            wake_bell: "127.0.0.1:0".parse().unwrap(),
            http: None,
            herald_grace_ms: Some(100),
        };
        let err = run(args, Some(sock)).await.unwrap_err();
        assert!(
            err.contains("group- or world-writable"),
            "unexpected error: {err}"
        );
        assert!(
            !journal.exists(),
            "daemon must validate socket directory before opening journal"
        );
    }
}
