//! Daemon composition root wiring `Journal`, `Hub`, `SidebarProjection`,
//! subsystem `Registry`s (`Rooms`, `Waves`, `Board`, `Inbox`), `MailBridge`,
//! reactors (`WaveConductor`, `Herald`, `BellCourier`), `BbBridge`, the
//! loopback `/sink` listener for `wake-bell`, and UDS `serve_listener` with
//! graceful `ctrl_c` shutdown.

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
use rex_wave_transport::{
    Authenticator, Credential, Endpoint, TableAuthenticator, bind_unix, serve_listener,
};
use rex_wave_waves::{WaveConductor, Waves};
use std::collections::BTreeMap;
use std::io::{self, Read as _, Write as _};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(clap::Args, Debug, Clone)]
pub struct DaemonArgs {
    /// Journal path (default: `<socket_dir>/journal.sqlite3`).
    #[arg(long)]
    pub journal: Option<PathBuf>,
    /// Non-production mail sandbox root (must contain `.rex-wave-sandbox`).
    #[arg(long)]
    pub mail_sandbox: PathBuf,
    /// BB plugin bridge WebSocket URL.
    #[arg(
        long,
        default_value = "ws://127.0.0.1:38886/api/v1/plugins/rex-wave/http/bridge"
    )]
    pub bb_url: String,
    /// BB plugin bridge bearer token.
    #[arg(long, env = "REX_WAVE_BB_TOKEN", default_value = "")]
    pub bb_token: String,
    /// Operator seat name bound to peer-UID sessions.
    #[arg(long, default_value = "owner")]
    pub operator: String,
    /// Loopback HTTP listener for `wake-bell` `/sink` delivery.
    #[arg(long, default_value = "127.0.0.1:7781")]
    pub wake_bell: std::net::SocketAddr,
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

pub fn tokens_path_for(sock_path: &Path) -> PathBuf {
    sock_path.with_file_name("tokens.json")
}

/// Mints a random 256-bit capability token for `seat` and atomically writes it
/// into `<state_dir>/tokens.json` with `0600` permissions.
pub fn mint_seat_token(sock_path: &Path, seat: &SeatName) -> io::Result<String> {
    let tokens_path = tokens_path_for(sock_path);
    if let Some(parent) = tokens_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut map: BTreeMap<String, SeatName> = if tokens_path.exists() {
        let raw = std::fs::read(&tokens_path)?;
        serde_json::from_slice(&raw).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
    } else {
        BTreeMap::new()
    };

    let mut entropy = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut entropy)?;
    let mut token = String::from("rw_");
    for byte in entropy {
        use std::fmt::Write as _;
        let _ = write!(token, "{byte:02x}");
    }

    map.retain(|_, existing| existing != seat);
    map.insert(token.clone(), seat.clone());

    let encoded = serde_json::to_vec_pretty(&map)
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
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp_path)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
    }
    std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o600))?;
    std::fs::rename(&tmp_path, &tokens_path)?;
    Ok(token)
}

/// Reloads `tokens.json` on token authentication so `rex-wave token --seat <seat>`
/// takes effect without restarting a running daemon, while enforcing `0600` mode
/// and matching UID ownership.
pub struct TokenFileAuthenticator {
    pub operator: SeatName,
    pub daemon_uid: Option<u32>,
    pub tokens_path: PathBuf,
}

impl TokenFileAuthenticator {
    fn load_tokens(&self) -> Result<BTreeMap<String, SeatName>, ApiError> {
        let meta = match std::fs::symlink_metadata(&self.tokens_path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(e) => return Err(ApiError::Internal(format!("tokens metadata: {e}"))),
        };
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
}

impl Authenticator for TokenFileAuthenticator {
    fn authenticate(
        &self,
        peer_uid: Option<u32>,
        credential: &Credential,
    ) -> Result<Principal, ApiError> {
        let tokens = match credential {
            Credential::PeerUid => BTreeMap::new(),
            Credential::Token { .. } => self.load_tokens()?,
        };
        let table = TableAuthenticator {
            operator: self.operator.clone(),
            daemon_uid: self.daemon_uid,
            tokens,
        };
        table.authenticate(peer_uid, credential)
    }
}

pub async fn run(args: DaemonArgs, socket_override: Option<PathBuf>) -> Result<(), String> {
    let operator = SeatName::parse(&args.operator).map_err(|e| e.to_string())?;
    let sock_path = resolve_socket_path(socket_override.as_deref());
    let journal_path = args
        .journal
        .clone()
        .unwrap_or_else(|| sock_path.with_file_name("journal.sqlite3"));
    if let Some(parent) = journal_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create state dir: {e}"))?;
    }

    let journal = Journal::open(&journal_path).map_err(|e| format!("open journal: {e}"))?;
    let hub = Hub::new(4096);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let sandbox =
        SandboxRoot::verify(&args.mail_sandbox).map_err(|e| format!("mail sandbox: {e}"))?;
    let mail_writer = journal
        .claim(StreamId::Mail)
        .map_err(|e| format!("claim mail stream: {e}"))?;
    let bridge = BbBridge::with_code_runner(
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
            agents: bridge.clone() as Arc<dyn AgentFeed>,
        }),
    });

    let system = |part| LocalClient::new(router.clone(), Principal::System { part });
    let conductor_task = spawn_reactor(
        WaveConductor::new(BbHost(bridge.clone())),
        system(SystemPart::WaveConductor),
    );
    let herald_task = spawn_reactor(Herald::new(), system(SystemPart::Herald));
    let bell_task = spawn_reactor(BellCourier, system(SystemPart::WakeBell));
    let bb_task = tokio::spawn(bridge.run(router.clone()));
    let sink_task = tokio::spawn(rex_wave_mailbridge::serve_wake_bell_sink(
        args.wake_bell,
        system(SystemPart::WakeBell),
    ));

    let (listener, daemon_uid) =
        bind_unix(&sock_path).map_err(|e| format!("bind {}: {e}", sock_path.display()))?;
    let auth: Arc<dyn Authenticator> = Arc::new(TokenFileAuthenticator {
        operator,
        daemon_uid: Some(daemon_uid),
        tokens_path: tokens_path_for(&sock_path),
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
    bb_task.abort();
    sink_task.abort();
    let _ = std::fs::remove_file(&sock_path);

    serve_result
}
