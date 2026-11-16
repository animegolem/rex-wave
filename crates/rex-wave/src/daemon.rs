//! The composition root: the only file that names every subsystem.
//!
//! Reading this file top to bottom tells you the whole runtime topology:
//! journal -> hub -> registries (one per family) -> router -> reactors ->
//! bridges -> transports. Swapping Kanban for a remote app is one line here.

use rex_wave_api::*;
use rex_wave_bb::{BbBridge, BbHost, BridgeConfig};
use rex_wave_board::Board;
use rex_wave_mailbridge::{BellCourier, MailBridge, SandboxRoot};
use rex_wave_notify::{Herald, Inbox};
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{AgentFeed, Hub, LocalClient, ReadSide, Registry, Router, SidebarProjection, spawn_reactor};
use rex_wave_store::Journal;
use rex_wave_transport::{Authenticator, Endpoint, TableAuthenticator, bind_unix, serve_listener};
use rex_wave_waves::{WaveConductor, Waves};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(clap::Args, Debug)]
pub struct DaemonArgs {
    /// Journal path (default: $XDG_STATE_HOME/rex-wave/journal.sqlite3).
    #[arg(long)]
    pub journal: Option<PathBuf>,
    /// NON-PROD mail sandbox root (must contain `.rex-wave-sandbox`).
    #[arg(long)]
    pub mail_sandbox: PathBuf,
    /// BB plugin bridge URL.
    #[arg(long, default_value = "ws://127.0.0.1:38886/api/v1/plugins/rex-wave/http/bridge")]
    pub bb_url: String,
    #[arg(long, env = "REX_WAVE_BB_TOKEN")]
    pub bb_token: String,
    /// The operator's seat name.
    #[arg(long, default_value = "owner")]
    pub operator: String,
    /// Loopback address for the wake-bell sink.
    #[arg(long, default_value = "127.0.0.1:7781")]
    pub wake_bell: std::net::SocketAddr,
}

pub async fn run(args: DaemonArgs) {
    let Endpoint::Unix(sock_path) = Endpoint::default_unix() else { unreachable!() };
    let journal_path = args.journal.clone().unwrap_or_else(|| sock_path.with_file_name("journal.sqlite3"));
    if let Some(parent) = journal_path.parent() {
        std::fs::create_dir_all(parent).expect("state dir");
    }
    let journal = Journal::open(&journal_path).expect("journal");
    let hub = Hub::new(4096);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let sandbox = SandboxRoot::verify(&args.mail_sandbox).expect("mail sandbox must be non-prod");
    let bridge = BbBridge::new(BridgeConfig { url: args.bb_url.clone(), token: args.bb_token.clone() });

    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Registry::<Waves>::new(journal.clone(), hub.clone()),
        // Split-out Kanban = replace this one line with
        //   Arc::new(Remote { client: RemoteClient::connect(board_endpoint, ..).await?, family: Family::Board })
        board: Registry::<Board>::new(journal.clone(), hub.clone()),
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: Arc::new(MailBridge::new(sandbox, journal.clone(), journal.claim(StreamId::Mail).expect("mail stream"))),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: bridge.clone() as Arc<dyn AgentFeed>,
        }),
    });

    let system = |part| LocalClient::new(router.clone(), Principal::System { part });
    spawn_reactor(WaveConductor::new(BbHost(bridge.clone())), system(SystemPart::WaveConductor));
    spawn_reactor(Herald::new(), system(SystemPart::Herald));
    spawn_reactor(BellCourier, system(SystemPart::WakeBell));
    tokio::spawn(bridge.run(router.clone()));
    tokio::spawn(rex_wave_mailbridge::serve_wake_bell_sink(args.wake_bell, system(SystemPart::WakeBell)));

    let (listener, daemon_uid) = bind_unix(&sock_path).expect("bind unix socket");
    let auth: Arc<dyn Authenticator> = Arc::new(TableAuthenticator {
        operator: SeatName::parse(args.operator).expect("operator seat"),
        daemon_uid: Some(daemon_uid),
        tokens: Default::default(),
    });
    serve_listener(listener, router, auth).await.expect("serve");
}
