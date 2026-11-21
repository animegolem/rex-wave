//! Daemon composition root wiring `Journal`, `Hub`, `SidebarProjection`,
//! subsystem `Registry`s (`Rooms`, `Waves`, `Board`, `Inbox`), `MailBridge`,
//! reactors (`WaveConductor`, `Herald`, `BellCourier`), `CompositeHost` /
//! `CompositeFeed` (`BbBridge` + `T3Bridge`), the loopback `/sink` listener for
//! `wake-bell`, optional loopback `--http` HTTP/1.1 + SSE server, and UDS
//! `serve_listener` with graceful `ctrl_c` shutdown.

use crate::http::{HttpConfig, bind_http, serve_http_listener};
use rex_wave_api::wave::{Observation, WaveEvent};
use rex_wave_api::*;
use rex_wave_bb::{BbBridge, BbHost, BridgeConfig};
use rex_wave_board::Board;
use rex_wave_mailbridge::{BellCourier, MailBridge, SandboxRoot};
use rex_wave_notify::{Herald, Inbox};
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    AgentFeed, Hub, LocalClient, ReadSide, Registry, Router, SidebarProjection, spawn_reactor,
};
use rex_wave_store::Journal;
use rex_wave_t3::{
    CompositeFeed, CompositeHost, RunnerTarget, T3AuthSource, T3Bridge, T3Config, T3EndpointSource,
    T3Feed, T3Host, T3Origin, ThreadIdResolver, WorkerTokenMinter, read_worker_token_file,
    runtime_file_path, write_worker_token_file,
};
use rex_wave_transport::{Authenticator, Credential, Endpoint, bind_unix, serve_listener};
use rex_wave_waves::{HostError, WaveConductor, Waves};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{self, Read as _, Write as _};
use std::net::SocketAddr;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

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
    #[arg(long)]
    pub http: Option<SocketAddr>,
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

/// Entry stored in `tokens.json`. Untagged so existing `"<tok>": "sam"` files
/// stay backward-compatible while worker tokens store structured records and
/// `Operator`/`System` principals remain unrepresentable in the token store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TokenRecord {
    Seat(SeatName),
    Worker(WorkerTokenEntry),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerTokenEntry {
    pub wave: WaveSlug,
    pub role: RoleName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
}

fn token_store_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn read_token_map(tokens_path: &Path) -> io::Result<BTreeMap<String, TokenRecord>> {
    if !tokens_path.exists() {
        return Ok(BTreeMap::new());
    }
    let raw = std::fs::read(tokens_path)?;
    serde_json::from_slice(&raw).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn write_token_map_atomic(
    tokens_path: &Path,
    map: &BTreeMap<String, TokenRecord>,
    entropy: &[u8; 4],
) -> io::Result<()> {
    if let Some(parent) = tokens_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let encoded = serde_json::to_vec_pretty(map)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let tmp_path = tokens_path.with_extension(format!(
        "tmp.{}.{:02x}{:02x}{:02x}{:02x}",
        std::process::id(),
        entropy[0],
        entropy[1],
        entropy[2],
        entropy[3]
    ));
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp_path)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
    }
    std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o600))?;
    if let Err(e) = std::fs::rename(&tmp_path, tokens_path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e);
    }
    Ok(())
}

fn random_token(prefix: &str) -> io::Result<(String, [u8; 4])> {
    let mut entropy = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut entropy)?;
    let mut token = String::from(prefix);
    for byte in entropy {
        use std::fmt::Write as _;
        let _ = write!(token, "{byte:02x}");
    }
    Ok((token, [entropy[0], entropy[1], entropy[2], entropy[3]]))
}

/// Mints a random 256-bit capability token for `seat` and atomically writes it
/// into `<state_dir>/tokens.json` with `0600` permissions.
pub fn mint_seat_token(sock_path: &Path, seat: &SeatName) -> io::Result<String> {
    let _guard = token_store_lock().lock().unwrap_or_else(|e| e.into_inner());
    let tokens_path = tokens_path_for(sock_path);
    let mut map = read_token_map(&tokens_path)?;
    let (token, suffix) = random_token("rw_")?;

    map.retain(|_, existing| !matches!(existing, TokenRecord::Seat(s) if s == seat));
    map.insert(token.clone(), TokenRecord::Seat(seat.clone()));

    write_token_map_atomic(&tokens_path, &map, &suffix)?;
    Ok(token)
}

/// Mints (or idempotently reuses for a thread) a random 256-bit capability token
/// for `Principal::Worker { wave, role }` and persists it to `<state_dir>/tokens.json`
/// with mode `0600`. When `write_file` is true and `thread_id` is `Some`, also writes
/// `<state_dir>/worker-tokens/<thread_id>` with mode `0600`.
pub fn mint_worker_token(
    sock_path: &Path,
    wave: &WaveSlug,
    role: &RoleName,
    thread_id: Option<&str>,
    write_file: bool,
) -> io::Result<String> {
    let _guard = token_store_lock().lock().unwrap_or_else(|e| e.into_inner());
    let tokens_path = tokens_path_for(sock_path);
    let state_dir = state_dir_for(sock_path);
    let mut map = read_token_map(&tokens_path)?;

    let desired = WorkerTokenEntry {
        wave: wave.clone(),
        role: role.clone(),
        thread_id: thread_id.map(str::to_owned),
    };

    if let Some(tid) = thread_id
        && let Ok(Some(existing_tok)) = read_worker_token_file(&state_dir, tid)
        && map.get(&existing_tok) == Some(&TokenRecord::Worker(desired.clone()))
    {
        return Ok(existing_tok);
    }

    let (token, suffix) = random_token("rw_w_")?;
    map.retain(|_, existing| !matches!(existing, TokenRecord::Worker(w) if w == &desired));
    map.insert(token.clone(), TokenRecord::Worker(desired));

    write_token_map_atomic(&tokens_path, &map, &suffix)?;
    if write_file && let Some(tid) = thread_id {
        write_worker_token_file(&state_dir, tid, &token)?;
    }
    Ok(token)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Reloads `tokens.json` on token authentication so `rex-wave token --seat <seat>`,
/// `rex-wave token --worker <wave>/<role>`, and `T3Bridge` worker tokens take
/// effect without restarting a running daemon, while enforcing `0600` mode and
/// matching UID ownership.
pub struct TokenFileAuthenticator {
    pub operator: SeatName,
    pub daemon_uid: Option<u32>,
    pub tokens_path: PathBuf,
}

impl TokenFileAuthenticator {
    fn load_tokens(&self) -> Result<BTreeMap<String, TokenRecord>, ApiError> {
        let meta = match std::fs::symlink_metadata(&self.tokens_path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(e) => return Err(ApiError::Internal(format!("tokens metadata: {e}"))),
        };
        if meta.file_type().is_symlink() || !meta.is_file() {
            return Err(ApiError::Denied {
                principal: "anonymous".to_owned(),
                action: "token store must be a regular file".to_owned(),
            });
        }
        if let Some(expected_uid) = self.daemon_uid
            && meta.uid() != expected_uid
        {
            return Err(ApiError::Denied {
                principal: "anonymous".to_owned(),
                action: "token store owned by another uid".to_owned(),
            });
        }
        if meta.permissions().mode() & 0o077 != 0 {
            return Err(ApiError::Denied {
                principal: "anonymous".to_owned(),
                action: "token store must be mode 0600".to_owned(),
            });
        }
        let raw = std::fs::read(&self.tokens_path)
            .map_err(|e| ApiError::Internal(format!("read tokens: {e}")))?;
        serde_json::from_slice(&raw).map_err(|e| ApiError::Internal(format!("decode tokens: {e}")))
    }

    fn state_dir(&self) -> PathBuf {
        self.tokens_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    }
}

impl Authenticator for TokenFileAuthenticator {
    fn authenticate(
        &self,
        peer_uid: Option<u32>,
        credential: &Credential,
    ) -> Result<Principal, ApiError> {
        match credential {
            Credential::PeerUid => match (peer_uid, self.daemon_uid) {
                (Some(peer), Some(daemon)) if peer == daemon => Ok(Principal::Operator {
                    seat: self.operator.clone(),
                }),
                _ => Err(ApiError::Denied {
                    principal: "anonymous".into(),
                    action: "peer-uid hello".into(),
                }),
            },
            Credential::Token { token } => {
                let tokens = self.load_tokens()?;
                let mut matched: Option<TokenRecord> = None;
                for (stored, record) in &tokens {
                    if constant_time_eq(stored.as_bytes(), token.as_bytes()) {
                        matched = Some(record.clone());
                    }
                }
                match matched {
                    Some(TokenRecord::Seat(seat)) => Ok(Principal::Seat { seat }),
                    Some(TokenRecord::Worker(entry)) => {
                        if let Some(ref tid) = entry.thread_id {
                            let state_dir = self.state_dir();
                            let active = read_worker_token_file(&state_dir, tid)
                                .ok()
                                .flatten()
                                .is_some_and(|disk| {
                                    constant_time_eq(disk.as_bytes(), token.as_bytes())
                                });
                            if !active {
                                return Err(ApiError::Denied {
                                    principal: "anonymous".into(),
                                    action: "worker token revoked".into(),
                                });
                            }
                        }
                        Ok(Principal::Worker {
                            wave: entry.wave,
                            role: entry.role,
                        })
                    }
                    None => Err(ApiError::Denied {
                        principal: "anonymous".into(),
                        action: "invalid token".into(),
                    }),
                }
            }
        }
    }
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
        Some(addr) => Some(
            bind_http(addr)
                .await
                .map_err(|e| format!("bind http {addr}: {e}"))?,
        ),
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

    let t3_bridge = t3_cfg.map(|cfg| {
        let sock_for_minter = sock_path.clone();
        let minter: WorkerTokenMinter = Arc::new(move |wave, role, thread| {
            mint_worker_token(&sock_for_minter, wave, role, Some(thread.as_str()), false)
                .map_err(|e| HostError::Unreachable(format!("mint worker token: {e}")))
        });
        T3Bridge::with_token_minter(cfg, Some(minter))
    });

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
            sandbox,
            journal.clone(),
            mail_writer,
            hub.clone(),
        )),
        presence: Arc::new(rex_wave_runtime::Unavailable(Family::Presence)),
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
    let herald_task = spawn_reactor(Herald::new(), system(SystemPart::Herald));
    let bell_task = spawn_reactor(BellCourier, system(SystemPart::WakeBell));
    let bb_task = bb_bridge.map(|b| tokio::spawn(b.run(router.clone())));
    let t3_task = t3_bridge.map(|t| tokio::spawn(t.run(router.clone())));
    let sink_task = tokio::spawn(rex_wave_mailbridge::serve_wake_bell_sink(
        args.wake_bell,
        system(SystemPart::WakeBell),
    ));

    let (listener, daemon_uid) =
        bind_unix(&sock_path).map_err(|e| format!("bind {}: {e}", sock_path.display()))?;
    let auth: Arc<dyn Authenticator> = Arc::new(TokenFileAuthenticator {
        operator: operator.clone(),
        daemon_uid: Some(daemon_uid),
        tokens_path: tokens_path_for(&sock_path),
    });

    let http_task = http_listener.map(|hl| {
        let router = router.clone();
        let auth = auth.clone();
        let op = operator.clone();
        tokio::spawn(async move {
            let _ = serve_http_listener(hl, router, auth, op, HttpConfig::default()).await;
        })
    });

    let serve_result = tokio::select! {
        res = serve_listener(listener, router, auth) => {
            res.map_err(|e| format!("uds serve: {e}"))
        }
        sig = tokio::signal::ctrl_c() => {
            sig.map_err(|e| format!("ctrl_c listener: {e}"))
        }
    };

    conductor_task.abort();
    herald_task.abort();
    bell_task.abort();
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
