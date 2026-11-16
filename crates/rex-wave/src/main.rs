//! `rex-wave`, one binary, many thin clients of one daemon API.
//!
//! Every CLI verb parses its arguments into exactly one `WireCommand` and
//! sends it with `Client::call_wire`; `rex-wave call <tool> <json>` is the
//! same path with no sugar. The CLI holds no business logic.

mod daemon;

use clap::{Parser, Subcommand};
use rex_wave_api::*;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "rex-wave", version, about = "Waves, #channels, DMs, kanban and notifications for BB + Rex")]
struct Cli {
    /// Daemon socket (default: $XDG_STATE_HOME/rex-wave/daemon.sock).
    #[arg(long, global = true)]
    socket: Option<PathBuf>,
    /// Emit JSON replies instead of human text.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run the daemon (composition root).
    Daemon(daemon::DaemonArgs),
    /// Open the TUI (run it inside Rex for Kitty cards + OSC 7501).
    Tui,
    /// Serve MCP for one seat (token from `token mint`).
    Mcp {
        #[arg(long, conflicts_with = "http")]
        stdio: bool,
        #[arg(long)]
        http: Option<SocketAddr>,
        #[arg(long, env = "REX_WAVE_TOKEN")]
        token: String,
    },
    /// Mint a capability token for a seat (operator only).
    Token {
        #[arg(long)]
        seat: String,
    },
    /// Post to a room: `#general`, `#rex-wave/wave-auth-v2` (project/channel), or `@sol` (DM).
    Post { room: String, text: String },
    /// Block until something matches (mentions by default) and print the catch-up slice.
    Wait {
        room: Option<String>,
        #[arg(long, default_value_t = 0)]
        since: u64,
        #[arg(long)]
        all: bool,
        #[arg(long)]
        timeout: Option<u64>,
    },
    /// Stream frames for a room / inbox / agent until interrupted.
    Watch { target: String, #[arg(long, default_value_t = 0)] since: u64 },
    /// Waves on BB.
    #[command(subcommand)]
    Wave(WaveCmd),
    /// Kanban tasks.
    #[command(subcommand)]
    Task(TaskCmd),
    /// Ranked notifications.
    Inbox {
        #[arg(long)]
        all: bool,
    },
    /// Bank a room range as a sandbox canon letter.
    Promote {
        room: String,
        /// `40..44`
        range: String,
        #[arg(long)]
        to: Vec<String>,
        #[arg(long)]
        subject: String,
        #[arg(long)]
        topic: String,
    },
    /// Generic escape hatch: identical to an MCP `tools/call`.
    Call { tool: String, args: String },
    /// Emit generated catalogs (`--bb` writes bb-plugin/tools.generated.json content).
    Schema {
        #[arg(long)]
        bb: bool,
    },
}

#[derive(Subcommand, Debug)]
enum WaveCmd {
    /// `rex-wave wave start auth-v2 --project p_123 --goal-file goal.md
    ///    --worker author-jwt=codex/gpt-6.1-sol --worker verifier=claude-code/claude-opus-5-5`
    Start {
        slug: String,
        #[arg(long)]
        project: String,
        #[arg(long)]
        goal_file: PathBuf,
        /// `role=provider[/model]`, repeatable; brief from `--brief-dir/<role>.md`.
        #[arg(long = "worker")]
        workers: Vec<String>,
        #[arg(long)]
        brief_dir: Option<PathBuf>,
        #[arg(long, default_value = "accept-edits")]
        permission: String,
    },
    Add { wave: String, worker: String, #[arg(long)] brief_file: PathBuf },
    Tell { wave: String, role: String, text: String, #[arg(long, default_value = "auto")] mode: String },
    Approve { wave: String, role: String, interaction: String },
    Deny { wave: String, role: String, interaction: String },
    Retire { wave: String, role: String },
    Finish { wave: String, #[arg(long)] summary: String },
    Show { wave: String },
}

#[derive(Subcommand, Debug)]
enum TaskCmd {
    /// `rex-wave task add "Wire OAuth refresh" --board project:p_123 --lane todo --assign @wave-auth-v2/author-jwt`
    Add { title: String, #[arg(long)] board: String, #[arg(long)] lane: Option<String>, #[arg(long)] assign: Option<String> },
    Move { board: String, no: u32, lane: String },
    Done { board: String, no: u32 },
    Board { board: String },
}

/// Parse a CLI verb into the one command it means. Pure; all validation here
/// happens by parsing into branded types (errors surface as `ApiError::Invalid`).
fn to_command(cmd: &Cmd) -> Result<WireCommand, ApiError> {
    let _ = cmd;
    todo!("to_command: one arm per verb, e.g. Post -> RoomPost{{room: parse_room_ref(..), body: Body::parse(..)}}")
}

/// `#general` | `#<project>/<channel>` | `@seat` | `@wave/role` -> RoomRef.
fn parse_room_ref(s: &str) -> Result<room::RoomRef, ApiError> {
    let _ = s;
    todo!("parse_room_ref")
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let _ = (cli.socket.as_ref(), cli.json, to_command as fn(&Cmd) -> _, parse_room_ref as fn(&str) -> _);
    match cli.cmd {
        Cmd::Daemon(args) => daemon::run(args).await,
        Cmd::Tui => todo!("RemoteClient::connect(PeerUid) -> rex_wave_tui::run"),
        Cmd::Mcp { .. } => todo!("RemoteClient::connect(Token) -> McpServer::serve_stdio | serve_http"),
        Cmd::Schema { bb } => {
            let out = if bb { rex_wave_bb::bb_tool_catalog() } else { rex_wave_api::tool_catalog() };
            println!("{}", serde_json::to_string_pretty(&out).expect("json"));
        }
        other => {
            let _ = other;
            todo!("connect(PeerUid); call_wire(to_command(&other)?); print reply (json or human)")
        }
    }
}
