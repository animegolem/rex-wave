//! `rex-wave`: single binary housing the daemon composition root, loopback
//! HTTP/SSE server, 2-pane TUI, MCP server, and thin CLI verbs over
//! `rex_wave_api::Client`.

use clap::{Parser, Subcommand};
use rex_wave::daemon;
use rex_wave::{parse_principal_ref, parse_room_ref};
use rex_wave_api::{
    board::{
        Assign, Assignee, BoardFilter, BoardId, BoardView, CardArchive, CardNo, CardPatch,
        CardUpsert, LaneName, Priority, Title,
    },
    mail::{LetterPriority, MailPromote},
    notify::InboxList,
    presence::{
        LivenessMode, PresenceBind, PresenceClaim, PresenceHeartbeat, PresenceList,
        PresenceResolve, PresenceSetStatus, PresenceUnbind, SeatStatus, TransportEndpoint,
        VerificationKind,
    },
    read::{Read, Wait},
    room::{
        Body, RoomEvent, RoomLabel, RoomPost, RoomTasks, TaskClaim, TaskFilter, TaskHeartbeat,
        TaskSpec, TaskStatus, TaskTransition, TaskUpdate, TaskView,
    },
    wave::{
        AgentSpec, BaseBranch, InteractionResolution, PermissionMode, SteerDeliveryMode,
        WaveAddWorker, WaveFinish, WaveOutcome, WaveResolve, WaveRetireWorker, WaveSpec, WaveStart,
        WaveTell, WaveView, WorkerPhase, WorkerSpec, Workspace,
    },
    *,
};
use rex_wave_mcp::McpServer;
use rex_wave_transport::{Credential, Endpoint, RemoteClient};
use std::io::Read as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const EXIT_CONFLICT: u8 = 3;
const EXIT_TIMEOUT: u8 = 124;

#[derive(Parser, Debug)]
#[command(
    name = "rex-wave",
    version,
    about = "Waves, #channels, DMs, kanban, tasks, presence, and notifications for BB + T3 + Rex"
)]
pub struct Cli {
    /// Daemon Unix socket path (default: `$XDG_STATE_HOME/rex-wave/daemon.sock`).
    #[arg(long, global = true)]
    pub socket: Option<PathBuf>,
    /// Capability token for seat or worker authentication (falls back to `T3CODE_THREAD_ID` worker token, then Unix peer UID).
    #[arg(long, global = true, env = "REX_WAVE_TOKEN")]
    pub token: Option<String>,
    /// Emit machine-readable JSON replies instead of human summaries.
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Run the daemon composition root.
    Daemon(daemon::DaemonArgs),
    /// Open the two-pane Rex TUI (`--self-test` runs standalone against an in-process fixture).
    Tui {
        #[arg(long)]
        self_test: bool,
    },
    /// Serve MCP atomic tools + Code Mode (`run_code`) for one capability token (`--token` / `REX_WAVE_TOKEN`).
    Mcp {
        #[arg(long, conflicts_with = "http")]
        stdio: bool,
        #[arg(long)]
        http: Option<SocketAddr>,
    },
    /// Mint a capability token for a seat (`--seat sam`) or wave worker (`--worker wave/role`).
    Token(TokenArgs),
    /// Post to `#channel`, `#<project>/<channel>`, `@seat`, or `@<wave>/<role>` (`-` reads stdin).
    Post {
        room: String,
        text: String,
        #[arg(long = "re", alias = "reply-to")]
        re: Option<u64>,
        #[arg(long)]
        after: Option<u64>,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
        #[arg(long)]
        task: Option<String>,
        #[arg(long)]
        org_id: Option<String>,
        #[arg(long = "needs")]
        needs: Vec<u64>,
        #[arg(long)]
        priority: Option<String>,
        #[arg(long)]
        due: Option<u64>,
    },
    /// Reply to a thread root in a room (`-` reads stdin).
    Reply {
        room: String,
        root_seq: u64,
        text: String,
        #[arg(long)]
        after: Option<u64>,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
    /// Read messages from a room with optional thread (`--re`) and `--label` filters.
    Read {
        room: String,
        #[arg(long)]
        after: Option<u64>,
        #[arg(long = "re")]
        re: Option<u64>,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
    },
    /// Block until matching messages arrive after `--since` and return the catch-up slice (exit 124 on timeout).
    Wait {
        room: Option<String>,
        #[arg(long, default_value_t = 0)]
        since: u64,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long = "re")]
        re: Option<u64>,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long, conflicts_with = "all")]
        mentions_only: bool,
        #[arg(long, conflicts_with = "mentions_only")]
        all: bool,
        /// Timeout in milliseconds.
        #[arg(long)]
        timeout: Option<u64>,
    },
    /// Stream live frames for a room, `inbox`, `sidebar`, or `agent:<wave>/<role>`.
    Watch {
        target: String,
        #[arg(long, default_value_t = 0)]
        since: u64,
    },
    /// Launch and manage multi-provider waves on BB and T3 Code.
    #[command(subcommand)]
    Wave(WaveCmd),
    /// Manage room task-threads (`#room`) and Kanban boards (`--board`).
    #[command(name = "tasks", visible_alias = "task", subcommand)]
    Tasks(TaskCmd),
    /// Manage durable seat records and ephemeral session bindings.
    #[command(subcommand)]
    Presence(PresenceCmd),
    /// List ranked inbox notifications.
    Inbox {
        #[arg(long)]
        all: bool,
        #[arg(long)]
        limit: Option<u32>,
    },
    /// Bank a room message range (`40..44`) as a sandbox canon letter.
    Promote {
        room: String,
        range: String,
        #[arg(long)]
        to: Vec<String>,
        #[arg(long)]
        subject: String,
        #[arg(long)]
        topic: String,
        #[arg(long, default_value = "normal")]
        priority: String,
    },
    /// Send a raw tool call (`rex-wave call <tool> '<json>'`).
    Call {
        tool: String,
        #[arg(default_value = "{}")]
        args: String,
    },
    /// Emit generated JSON-Schema tool catalogs (`--bb` for `bb-plugin/tools.generated.json`).
    Schema {
        #[arg(long)]
        bb: bool,
    },
}

#[derive(clap::Args, Debug, Clone)]
pub struct TokenArgs {
    #[arg(long, conflicts_with = "worker")]
    pub seat: Option<String>,
    /// Mint a worker token (`<wave>/<role>`).
    #[arg(long, conflicts_with = "seat")]
    pub worker: Option<String>,
    /// Optional T3 thread ID; also writes `<state_dir>/worker-tokens/<thread_id>` with mode 0600.
    #[arg(long)]
    pub thread: Option<String>,
    #[command(subcommand)]
    pub sub: Option<TokenSubcmd>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum TokenSubcmd {
    /// Mint a new token for a seat or wave worker.
    Mint {
        #[arg(long = "seat", conflicts_with = "worker_flag")]
        seat_flag: Option<String>,
        #[arg(long = "worker", conflicts_with = "seat_flag")]
        worker_flag: Option<String>,
        #[arg(long)]
        thread: Option<String>,
        seat: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TokenTarget {
    Seat(SeatName),
    Worker {
        wave: WaveSlug,
        role: RoleName,
        thread_id: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum WaveCmd {
    /// Launch a wave (`--worker role=provider[/model]`).
    Start {
        slug: String,
        #[arg(long)]
        project: String,
        #[arg(long, conflicts_with = "goal")]
        goal_file: Option<PathBuf>,
        #[arg(long)]
        goal: Option<String>,
        #[arg(long = "worker")]
        workers: Vec<String>,
        #[arg(long)]
        brief_dir: Option<PathBuf>,
        #[arg(long, default_value = "accept-edits")]
        permission: String,
        #[arg(long, default_value = "managed-worktree")]
        workspace: String,
    },
    /// Add a worker (`role=provider[/model]`) to a running wave.
    #[command(name = "add-worker", visible_alias = "add")]
    AddWorker {
        wave: String,
        worker: String,
        #[arg(long, conflicts_with = "brief")]
        brief_file: Option<PathBuf>,
        #[arg(long)]
        brief: Option<String>,
        #[arg(long, default_value = "accept-edits")]
        permission: String,
        #[arg(long, default_value = "managed-worktree")]
        workspace: String,
    },
    /// Approve a worker's pending interaction.
    Approve {
        wave: String,
        role: String,
        interaction: String,
        #[arg(long)]
        session: bool,
    },
    /// Deny a worker's pending interaction.
    Deny {
        wave: String,
        role: String,
        interaction: String,
    },
    /// Steer or send a message to a worker's thread.
    #[command(name = "steer", visible_alias = "tell")]
    Steer {
        wave: String,
        role: String,
        text: String,
        #[arg(long, default_value = "auto")]
        mode: String,
    },
    /// Stop and archive a worker's thread.
    Retire {
        wave: String,
        role: String,
        #[arg(long)]
        reason: Option<String>,
    },
    /// Show wave roster and worker phases.
    #[command(name = "status", visible_alias = "show")]
    Status { wave: String },
    /// Close a wave with a completion summary.
    Finish {
        wave: String,
        #[arg(long)]
        summary: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum TaskCmd {
    /// Add a room task-thread (`tasks add #room "Title"`) or Kanban card (`task add "Title" --board <board>`).
    Add {
        /// Room (`#channel` / `@seat`) for a task-thread, or title when `--board` is set.
        first: String,
        /// Task title (for room task-threads).
        second: Option<String>,
        /// Kanban board id (legacy Kanban mode).
        #[arg(long)]
        board: Option<String>,
        #[arg(long)]
        lane: Option<String>,
        #[arg(long)]
        assign: Option<String>,
        #[arg(long)]
        priority: Option<String>,
        /// Task body text (`-` reads stdin; defaults to title).
        #[arg(long)]
        body: Option<String>,
        #[arg(long)]
        org_id: Option<String>,
        #[arg(long = "needs")]
        needs: Vec<u64>,
        #[arg(long)]
        due: Option<u64>,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
    /// Move a Kanban card to another lane.
    Move {
        board: String,
        no: u32,
        lane: String,
        #[arg(long)]
        expected: Option<u64>,
    },
    /// List room tasks (`tasks list #room`) or Kanban cards (`task list --board <board>`).
    #[command(name = "list", visible_alias = "board")]
    List {
        target: Option<String>,
        #[arg(long = "board")]
        board_flag: Option<String>,
        #[arg(long)]
        lane: Option<String>,
        #[arg(long)]
        assign: Option<String>,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        ready: bool,
        #[arg(long)]
        mine: bool,
        #[arg(long)]
        holder: Option<String>,
        #[arg(long)]
        label: Option<String>,
    },
    /// Show one room task-thread and its replies.
    Show { room: String, seq: u64 },
    /// Claim a room task-thread with a TTL lease.
    Claim {
        room: String,
        seq: u64,
        /// Lease duration (e.g. `60s`, `10m`, `5000ms`; bare integer is seconds).
        #[arg(long, conflicts_with = "ttl_ms")]
        ttl: Option<String>,
        #[arg(long)]
        ttl_ms: Option<u64>,
        #[arg(long)]
        expected_rev: Option<u64>,
        #[arg(long)]
        force: bool,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
    /// Renew an active task lease for a given fencing epoch.
    Heartbeat {
        room: String,
        seq: u64,
        #[arg(long)]
        epoch: u64,
        #[arg(long, conflicts_with = "ttl_ms")]
        ttl: Option<String>,
        #[arg(long)]
        ttl_ms: Option<u64>,
    },
    /// Transition a room task-thread to a new status (`open|claimed|review|blocked|done|cancelled`).
    Transition {
        room: String,
        seq: u64,
        status: String,
        #[arg(long)]
        epoch: Option<u64>,
        #[arg(long)]
        expected_rev: Option<u64>,
        #[arg(long)]
        note: Option<String>,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
    /// Mark a room task (`#room <seq>`) or Kanban card (`<board> <no>`) done.
    Done {
        target: String,
        no: u64,
        #[arg(long)]
        epoch: Option<u64>,
        #[arg(long)]
        expected_rev: Option<u64>,
        #[arg(long)]
        note: Option<String>,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
    /// Mark a room task blocked.
    Block {
        room: String,
        seq: u64,
        #[arg(long)]
        epoch: Option<u64>,
        #[arg(long)]
        expected_rev: Option<u64>,
        #[arg(long)]
        note: Option<String>,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
    /// Move a room task to review.
    Review {
        room: String,
        seq: u64,
        #[arg(long)]
        epoch: Option<u64>,
        #[arg(long)]
        expected_rev: Option<u64>,
        #[arg(long)]
        note: Option<String>,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
    /// Release a claimed room task back to open.
    Release {
        room: String,
        seq: u64,
        #[arg(long)]
        epoch: Option<u64>,
        #[arg(long)]
        expected_rev: Option<u64>,
        #[arg(long)]
        note: Option<String>,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
    /// Update room task metadata (title, dependencies, priority, due date).
    Update {
        room: String,
        seq: u64,
        #[arg(long)]
        expected_rev: Option<u64>,
        #[arg(long)]
        title: Option<String>,
        #[arg(long = "needs-add")]
        needs_add: Vec<u64>,
        #[arg(long = "needs-rm")]
        needs_rm: Vec<u64>,
        #[arg(long)]
        priority: Option<String>,
        #[arg(long)]
        due: Option<u64>,
        #[arg(long)]
        clear_due: bool,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
    /// Add or remove `key=value` labels on a room message or task.
    Label {
        room: String,
        seq: u64,
        #[arg(long = "add")]
        add: Vec<String>,
        #[arg(long = "rm", alias = "remove")]
        remove: Vec<String>,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
    /// Archive a Kanban card.
    Archive { board: String, no: u32 },
}

#[derive(Subcommand, Debug)]
pub enum PresenceCmd {
    /// Claim or update durable seat identity and metadata.
    Claim {
        seat: String,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        role: Option<String>,
        #[arg(long = "project")]
        projects: Vec<String>,
        #[arg(long = "alias")]
        aliases: Vec<String>,
        #[arg(long)]
        home_room: Option<String>,
        #[arg(long)]
        replace: Option<String>,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
    /// Set a durable seat's status (`active`, `dormant`, or `retired`).
    #[command(name = "set-status")]
    SetStatus {
        seat: String,
        status: String,
        #[arg(long)]
        reason: Option<String>,
    },
    /// Bind an ephemeral live session endpoint to a seat.
    Bind {
        #[arg(long)]
        seat: Option<String>,
        #[arg(long)]
        incarnation: Option<String>,
        #[arg(long)]
        lane: Option<String>,
        /// Bind from a Claude session JSON file (omit value to inspect `~/.claude/sessions/<pid>.json`).
        #[arg(long, num_args = 0..=1, default_missing_value = "")]
        from_claude_session: Option<String>,
        #[arg(long)]
        claude_pid: Option<u32>,
        #[arg(long)]
        bb_thread: Option<String>,
        #[arg(long)]
        t3_thread: Option<String>,
        #[arg(long)]
        self_watcher: Option<String>,
        #[arg(long)]
        thread: Option<String>,
        #[arg(long)]
        pid: Option<u32>,
        #[arg(long)]
        operator_toast: Option<String>,
        #[arg(long, conflicts_with = "ttl_ms")]
        ttl: Option<String>,
        #[arg(long)]
        ttl_ms: Option<u64>,
        #[arg(long)]
        takeover: bool,
        #[arg(long)]
        allow_fork: bool,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
    /// Bind a UDS socket-lifetime presence session and block until interrupted.
    Hold {
        #[arg(long)]
        seat: Option<String>,
        #[arg(long)]
        lane: Option<String>,
        #[arg(long)]
        incarnation: Option<String>,
        #[arg(long)]
        takeover: bool,
        #[arg(long, default_value = "cli-hold")]
        client: String,
    },
    /// Renew the TTL lease on an active presence binding.
    Heartbeat {
        #[arg(long)]
        seat: Option<String>,
        #[arg(long)]
        incarnation: String,
        #[arg(long, conflicts_with = "ttl_ms")]
        ttl: Option<String>,
        #[arg(long)]
        ttl_ms: Option<u64>,
    },
    /// Unbind an active seat incarnation.
    Unbind {
        #[arg(long)]
        seat: Option<String>,
        #[arg(long)]
        incarnation: String,
        #[arg(long)]
        reason: Option<String>,
    },
    /// List durable seats and their ephemeral bindings.
    List {
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        live_only: bool,
        #[arg(long)]
        include_retired: bool,
    },
    /// Resolve a seat name, alias, or project to its durable record and live bindings.
    Resolve {
        query: String,
        #[arg(long)]
        lane: Option<String>,
    },
}

#[derive(Debug)]
enum CliError {
    Api(ApiError),
    Msg(String),
}

impl CliError {
    fn exit_code(&self) -> ExitCode {
        match self {
            CliError::Api(ApiError::Conflict { .. }) => ExitCode::from(EXIT_CONFLICT),
            CliError::Api(ApiError::Timeout) => ExitCode::from(EXIT_TIMEOUT),
            _ => ExitCode::FAILURE,
        }
    }
}

impl From<ApiError> for CliError {
    fn from(e: ApiError) -> Self {
        CliError::Api(e)
    }
}

impl From<InvalidId> for CliError {
    fn from(e: InvalidId) -> Self {
        CliError::Api(ApiError::Invalid(e))
    }
}

impl From<String> for CliError {
    fn from(s: String) -> Self {
        CliError::Msg(s)
    }
}

fn invalid(what: &str, reason: impl Into<String>) -> ApiError {
    ApiError::Invalid(InvalidId {
        what: what.to_owned(),
        reason: reason.into(),
    })
}

fn map_board_val<T, E: std::fmt::Display>(what: &str, res: Result<T, E>) -> Result<T, ApiError> {
    res.map_err(|e| invalid(what, e.to_string()))
}

fn is_room_syntax(s: &str) -> bool {
    s.starts_with('#') || s.starts_with('@')
}

fn read_body_arg(raw: &str) -> Result<Body, ApiError> {
    if raw == "-" {
        let mut buf = String::new();
        let mut stdin = std::io::stdin().take((64 * 1024 + 64) as u64);
        stdin
            .read_to_string(&mut buf)
            .map_err(|e| invalid("body", format!("failed reading stdin: {e}")))?;
        let trimmed = buf
            .strip_suffix("\r\n")
            .or_else(|| buf.strip_suffix('\n'))
            .unwrap_or(&buf);
        let text = if trimmed.is_empty() {
            buf
        } else {
            trimmed.to_owned()
        };
        Body::parse(text).map_err(Into::into)
    } else {
        Body::parse(raw.to_owned()).map_err(Into::into)
    }
}

pub fn parse_duration_ms(raw: &str) -> Result<u64, ApiError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(invalid("ttl", "duration must not be empty"));
    }
    let (digits, mult) = if let Some(n) = trimmed.strip_suffix("ms") {
        (n, 1u64)
    } else if let Some(n) = trimmed.strip_suffix('s') {
        (n, 1_000u64)
    } else if let Some(n) = trimmed.strip_suffix('m') {
        (n, 60_000u64)
    } else if let Some(n) = trimmed.strip_suffix('h') {
        (n, 3_600_000u64)
    } else {
        (trimmed, 1_000u64)
    };
    let val = digits
        .trim()
        .parse::<u64>()
        .map_err(|_| invalid("ttl", format!("invalid duration '{raw}'")))?;
    val.checked_mul(mult)
        .ok_or_else(|| invalid("ttl", "duration overflow"))
}

fn resolve_ttl_ms(ttl: Option<&String>, ttl_ms: Option<u64>) -> Result<Option<u64>, ApiError> {
    if let Some(ms) = ttl_ms {
        return Ok(Some(ms));
    }
    match ttl {
        Some(raw) => Ok(Some(parse_duration_ms(raw)?)),
        None => Ok(None),
    }
}

fn parse_labels(raw: &[String]) -> Result<Vec<Label>, ApiError> {
    raw.iter()
        .map(|s| Label::parse(s.as_str()).map_err(Into::into))
        .collect()
}

fn parse_cursor_arg(raw: &str) -> Result<Cursor, ApiError> {
    serde_json::from_str(raw).map_err(|e| invalid("cursor", format!("invalid cursor JSON: {e}")))
}

fn parse_task_status(s: &str) -> Result<TaskStatus, ApiError> {
    match s.trim().to_ascii_lowercase().as_str() {
        "open" => Ok(TaskStatus::Open),
        "claimed" => Ok(TaskStatus::Claimed),
        "review" => Ok(TaskStatus::Review),
        "blocked" => Ok(TaskStatus::Blocked),
        "done" => Ok(TaskStatus::Done),
        "cancelled" | "canceled" => Ok(TaskStatus::Cancelled),
        _ => Err(invalid(
            "status",
            "expected open, claimed, review, blocked, done, or cancelled",
        )),
    }
}

fn parse_seat_status(s: &str) -> Result<SeatStatus, ApiError> {
    match s.trim().to_ascii_lowercase().as_str() {
        "active" => Ok(SeatStatus::Active),
        "dormant" => Ok(SeatStatus::Dormant),
        "retired" => Ok(SeatStatus::Retired),
        _ => Err(invalid("status", "expected active, dormant, or retired")),
    }
}

fn parse_seq_range(s: &str) -> Result<SeqRange, ApiError> {
    let (a, b) = if let Some((l, r)) = s.split_once("..=") {
        (l, r)
    } else if let Some((l, r)) = s.split_once("..") {
        (l, r)
    } else {
        (s, s)
    };
    let first: u64 = a
        .trim()
        .parse()
        .map_err(|_| invalid("range", "start seq must be an integer"))?;
    let last: u64 = b
        .trim()
        .parse()
        .map_err(|_| invalid("range", "end seq must be an integer"))?;
    Ok(SeqRange::new(Seq(first), Seq(last))?)
}

fn parse_permission(s: &str) -> Result<PermissionMode, ApiError> {
    match s {
        "accept-edits" => Ok(PermissionMode::AcceptEdits),
        "auto" => Ok(PermissionMode::Auto),
        "full" => Ok(PermissionMode::Full),
        _ => Err(invalid(
            "permission",
            "expected accept-edits, auto, or full",
        )),
    }
}

fn parse_workspace(s: &str) -> Result<Workspace, ApiError> {
    match s {
        "managed-worktree" => Ok(Workspace::ManagedWorktree {
            base: BaseBranch::Default,
        }),
        "project-default" => Ok(Workspace::ProjectDefault),
        other => {
            if let Some(branch) = other.strip_prefix("managed-worktree:") {
                return Ok(Workspace::ManagedWorktree {
                    base: BaseBranch::Named {
                        name: branch.to_owned(),
                    },
                });
            }
            let path = other.strip_prefix("existing:").unwrap_or(other);
            if path.is_empty() {
                return Err(invalid("workspace", "empty workspace path"));
            }
            Ok(Workspace::ExistingPath {
                path: PathBuf::from(path),
            })
        }
    }
}

fn parse_steer_mode(s: &str) -> Result<SteerDeliveryMode, ApiError> {
    match s {
        "auto" => Ok(SteerDeliveryMode::Auto),
        "start" => Ok(SteerDeliveryMode::Start),
        "steer" => Ok(SteerDeliveryMode::Steer),
        "steer-if-active" => Ok(SteerDeliveryMode::SteerIfActive),
        "queue-if-active" => Ok(SteerDeliveryMode::QueueIfActive),
        _ => Err(invalid(
            "mode",
            "expected auto, start, steer, steer-if-active, or queue-if-active",
        )),
    }
}

fn parse_card_priority(s: &str) -> Result<Priority, ApiError> {
    match s {
        "low" => Ok(Priority::Low),
        "normal" => Ok(Priority::Normal),
        "high" => Ok(Priority::High),
        "blocking" => Ok(Priority::Blocking),
        _ => Err(invalid(
            "priority",
            "expected low, normal, high, or blocking",
        )),
    }
}

fn parse_letter_priority(s: &str) -> Result<LetterPriority, ApiError> {
    match s {
        "low" => Ok(LetterPriority::Low),
        "normal" => Ok(LetterPriority::Normal),
        "high" => Ok(LetterPriority::High),
        "blocking" => Ok(LetterPriority::Blocking),
        _ => Err(invalid(
            "priority",
            "expected low, normal, high, or blocking",
        )),
    }
}

fn parse_worker_spec(
    spec_str: &str,
    brief_text: String,
    permission: PermissionMode,
    workspace: Workspace,
) -> Result<WorkerSpec, ApiError> {
    let (role_raw, rhs) = spec_str
        .split_once('=')
        .ok_or_else(|| invalid("worker", "expected <role>=<provider>[/<model>]"))?;
    let role = RoleName::parse(role_raw)?;
    let (prov_raw, model_opt) = match rhs.split_once('/') {
        Some((p, m)) if !m.is_empty() => (p, Some(ModelId::parse(m)?)),
        Some((p, _)) => (p, None),
        None => (rhs, None),
    };
    let provider = ProviderId::parse(prov_raw)?;
    let brief = Body::parse(brief_text)?;
    Ok(WorkerSpec {
        role,
        brief,
        agent: Some(AgentSpec {
            provider,
            model: model_opt,
            reasoning: None,
            service_tier: None,
            permission,
            workspace,
        }),
    })
}

fn read_text_source(
    inline: Option<&String>,
    file: Option<&PathBuf>,
    what: &str,
) -> Result<String, ApiError> {
    match (inline, file) {
        (Some(s), _) => Ok(s.clone()),
        (None, Some(path)) => std::fs::read_to_string(path)
            .map_err(|e| invalid(what, format!("failed reading {}: {e}", path.display()))),
        (None, None) => Err(invalid(what, format!("provide --{what} or --{what}-file"))),
    }
}

fn resolve_seat_arg(explicit: Option<&String>, me: &Principal) -> Result<SeatName, ApiError> {
    if let Some(raw) = explicit {
        return Ok(SeatName::parse(raw.strip_prefix('@').unwrap_or(raw))?);
    }
    match me {
        Principal::Operator { seat } | Principal::Seat { seat } => Ok(seat.clone()),
        _ => Err(invalid("seat", "provide --seat <seat>")),
    }
}

/// Resolves CLI credentials in precedence order:
/// 1. Explicit `--token` / `REX_WAVE_TOKEN`.
/// 2. `<state_dir>/worker-tokens/$T3CODE_THREAD_ID` when `T3CODE_THREAD_ID` is set.
/// 3. Unix `Credential::PeerUid` (failing closed if a `t3:` worker token file is missing or invalid).
pub fn resolve_cli_credential(
    sock_path: &Path,
    explicit_token: Option<&str>,
) -> Result<Credential, String> {
    if let Some(tok) = explicit_token.map(str::trim).filter(|t| !t.is_empty()) {
        return Ok(Credential::Token {
            token: tok.to_owned(),
        });
    }
    if let Ok(raw_tid) = std::env::var("T3CODE_THREAD_ID") {
        let tid = raw_tid.trim();
        if !tid.is_empty() {
            let state_dir = daemon::state_dir_for(sock_path);
            match rex_wave_t3::read_worker_token_file(&state_dir, tid) {
                Ok(Some(token)) => return Ok(Credential::Token { token }),
                Ok(None) if tid.starts_with(rex_wave_t3::T3_THREAD_PREFIX) => {
                    return Err(format!(
                        "T3 wave worker token missing or retired for thread {tid}; refusing PeerUid fallback"
                    ));
                }
                Ok(None) => {}
                Err(e) => {
                    return Err(format!(
                        "failed reading worker token for T3CODE_THREAD_ID={tid}: {e}"
                    ));
                }
            }
        }
    }
    Ok(Credential::PeerUid)
}

#[derive(serde::Deserialize)]
struct ClaudeSessionFile {
    pid: u32,
    #[serde(alias = "procStart", alias = "proc_start")]
    proc_start: String,
    #[serde(alias = "startedAt", alias = "started_at_ms", default)]
    started_at_ms: u64,
    #[serde(alias = "sessionId", alias = "session_id")]
    session_id: String,
    #[serde(alias = "messagingSocketPath", alias = "messaging_socket")]
    messaging_socket: PathBuf,
    kind: String,
    #[serde(alias = "nameSource", alias = "name_source", default)]
    name_source: Option<String>,
    #[serde(alias = "name", alias = "display_name", default)]
    display_name: Option<String>,
}

fn load_claude_session_endpoint(
    path_arg: &str,
    claude_pid: Option<u32>,
    allow_fork: bool,
) -> Result<TransportEndpoint, ApiError> {
    let path = if !path_arg.trim().is_empty() {
        PathBuf::from(path_arg.trim())
    } else {
        let pid = claude_pid
            .or_else(|| {
                std::env::var("CLAUDE_PID")
                    .ok()
                    .and_then(|s| s.trim().parse::<u32>().ok())
            })
            .ok_or_else(|| {
                invalid(
                    "from-claude-session",
                    "pass a session JSON path or --claude-pid <pid>",
                )
            })?;
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or_else(|| invalid("from-claude-session", "HOME is unset"))?;
        home.join(".claude/sessions").join(format!("{pid}.json"))
    };

    let raw = std::fs::read(&path).map_err(|e| {
        invalid(
            "from-claude-session",
            format!("failed reading {}: {e}", path.display()),
        )
    })?;
    let info: ClaudeSessionFile = serde_json::from_slice(&raw).map_err(|e| {
        invalid(
            "from-claude-session",
            format!("invalid Claude session JSON {}: {e}", path.display()),
        )
    })?;
    if !TransportEndpoint::is_allowed_claude_kind(&info.kind) {
        return Err(invalid(
            "from-claude-session",
            format!("unsupported Claude session kind '{}'", info.kind),
        ));
    }
    if !allow_fork
        && info
            .display_name
            .as_deref()
            .is_some_and(TransportEndpoint::is_fork_display_name)
    {
        return Err(invalid(
            "from-claude-session",
            "refusing to bind forked Claude session (⑂); pass --allow-fork to override",
        ));
    }
    Ok(TransportEndpoint::ClaudeSession {
        pid: info.pid,
        proc_start: info.proc_start,
        started_at_ms: info.started_at_ms,
        session_id: info.session_id,
        messaging_socket: info.messaging_socket,
        kind: info.kind,
        name_source: info.name_source,
        display_name: info.display_name,
    })
}

pub fn to_command_for(cmd: &Cmd, me: &Principal) -> Result<WireCommand, ApiError> {
    match cmd {
        Cmd::Post {
            room,
            text,
            re,
            after,
            labels,
            idem,
            task,
            org_id,
            needs,
            priority,
            due,
        } => {
            if task.is_none()
                && (org_id.is_some() || !needs.is_empty() || priority.is_some() || due.is_some())
            {
                return Err(invalid(
                    "task",
                    "--org-id, --needs, --priority, and --due require --task <title>",
                ));
            }
            let task_spec = match task {
                Some(title_raw) => Some(TaskSpec {
                    title: map_board_val("title", Title::parse(title_raw.clone()))?,
                    org_id: match org_id {
                        Some(raw) => Some(map_board_val("org_id", OrgId::parse(raw.clone()))?),
                        None => None,
                    },
                    needs: needs.iter().copied().map(Seq).collect(),
                    priority: match priority {
                        Some(p) => Some(parse_card_priority(p)?),
                        None => None,
                    },
                    due_ms: due.map(UnixMs),
                }),
                None => None,
            };
            Ok(WireCommand::RoomPost(RoomPost {
                room: parse_room_ref(room)?,
                body: read_body_arg(text)?,
                re: re.map(Seq),
                after: after.map(Seq),
                labels: parse_labels(labels)?,
                task: task_spec,
                idem: match idem {
                    Some(k) => Some(IdemKey::parse(k.clone())?),
                    None => None,
                },
            }))
        }
        Cmd::Reply {
            room,
            root_seq,
            text,
            after,
            labels,
            idem,
        } => Ok(WireCommand::RoomPost(RoomPost {
            room: parse_room_ref(room)?,
            body: read_body_arg(text)?,
            re: Some(Seq(*root_seq)),
            after: after.map(Seq),
            labels: parse_labels(labels)?,
            task: None,
            idem: match idem {
                Some(k) => Some(IdemKey::parse(k.clone())?),
                None => None,
            },
        })),
        Cmd::Read {
            room,
            after,
            re,
            labels,
            cursor,
            limit,
        } => {
            let room_id = parse_room_ref(room)?.resolve(me)?;
            let stream = StreamId::Room {
                room: room_id.clone(),
            };
            let parsed_labels = parse_labels(labels)?;
            let start_seq = match (after, cursor) {
                (Some(n), _) => Seq(*n),
                (None, Some(c)) => parse_cursor_arg(c)?.at(&stream),
                (None, None) => Seq::ORIGIN,
            };
            if re.is_some() || !parsed_labels.is_empty() {
                let mut wait = Wait::room_filtered(room_id, start_seq, re.map(Seq), parsed_labels);
                wait.timeout_ms = Some(0);
                Ok(WireCommand::Wait(wait))
            } else {
                Ok(WireCommand::Read(Read {
                    stream,
                    after: start_seq,
                    limit: *limit,
                }))
            }
        }
        Cmd::Wait {
            room,
            since,
            cursor,
            re,
            labels,
            mentions_only: _,
            all,
            timeout,
        } => {
            let parsed_labels = parse_labels(labels)?;
            let has_filter = re.is_some() || !parsed_labels.is_empty();
            let base_cursor = match cursor {
                Some(raw) => Some(parse_cursor_arg(raw)?),
                None => None,
            };

            let wait = match room {
                Some(r) => {
                    let room_id = parse_room_ref(r)?.resolve(me)?;
                    let stream = StreamId::Room {
                        room: room_id.clone(),
                    };
                    if has_filter {
                        let start_seq = if *since > 0 {
                            Seq(*since)
                        } else {
                            base_cursor.as_ref().map_or(Seq::ORIGIN, |c| c.at(&stream))
                        };
                        let mut w =
                            Wait::room_filtered(room_id, start_seq, re.map(Seq), parsed_labels);
                        w.timeout_ms = *timeout;
                        w
                    } else {
                        let mentions = !*all;
                        let mut w = Wait::room(room_id, Seq(*since), mentions);
                        if let Some(mut c) = base_cursor {
                            if mentions {
                                let seq = c.at(&stream).max(Seq(*since));
                                w.after = Cursor::live().with(stream, seq);
                            } else {
                                if *since > 0 {
                                    c.advance(&stream, Seq(*since));
                                }
                                w.after = c;
                            }
                        }
                        w.timeout_ms = *timeout;
                        w
                    }
                }
                None => {
                    if *since > 0 {
                        return Err(invalid("since", "--since requires a target room"));
                    }
                    if has_filter {
                        return Err(invalid("room", "--re and --label require a target room"));
                    }
                    Wait {
                        interest: if *all {
                            Interest::Everything
                        } else {
                            Interest::Mentions
                        },
                        after: base_cursor.unwrap_or_else(Cursor::live),
                        timeout_ms: *timeout,
                    }
                }
            };
            Ok(WireCommand::Wait(wait))
        }
        Cmd::Inbox { all, limit } => Ok(WireCommand::InboxList(InboxList {
            include_done: *all,
            limit: *limit,
        })),
        Cmd::Promote {
            room,
            range,
            to,
            subject,
            topic,
            priority,
        } => {
            if to.is_empty() {
                return Err(invalid("to", "specify at least one --to <seat>"));
            }
            let recipients: Vec<SeatName> = to
                .iter()
                .map(|s| SeatName::parse(s.strip_prefix('@').unwrap_or(s)))
                .collect::<Result<_, _>>()?;
            let room_id = parse_room_ref(room)?.resolve(me)?;
            Ok(WireCommand::MailPromote(MailPromote {
                room: room_id,
                range: parse_seq_range(range)?,
                to: recipients,
                subject: subject.clone(),
                topic: topic.clone(),
                priority: parse_letter_priority(priority)?,
            }))
        }
        Cmd::Call { tool, args } => {
            let parsed_args: serde_json::Value = serde_json::from_str(args)
                .map_err(|e| invalid("args", format!("invalid JSON: {e}")))?;
            let envelope = serde_json::json!({
                "cmd": tool,
                "args": parsed_args,
            });
            serde_json::from_value(envelope)
                .map_err(|e| invalid("call", format!("invalid command payload for `{tool}`: {e}")))
        }
        Cmd::Wave(wave_cmd) => wave_to_command(wave_cmd),
        Cmd::Tasks(task_cmd) => task_to_command(task_cmd, me),
        Cmd::Presence(pres_cmd) => presence_to_command(pres_cmd, me),
        Cmd::Daemon(_)
        | Cmd::Tui { .. }
        | Cmd::Mcp { .. }
        | Cmd::Token(_)
        | Cmd::Watch { .. }
        | Cmd::Schema { .. } => Err(invalid(
            "command",
            "handled locally rather than via single WireCommand",
        )),
    }
}

fn wave_to_command(cmd: &WaveCmd) -> Result<WireCommand, ApiError> {
    match cmd {
        WaveCmd::Start {
            slug,
            project,
            goal_file,
            goal,
            workers,
            brief_dir,
            permission,
            workspace,
        } => {
            let slug = WaveSlug::parse(slug.clone())?;
            let project = BbProjectId::parse(project.clone())?;
            let goal_raw = read_text_source(goal.as_ref(), goal_file.as_ref(), "goal")?;
            let goal_body = Body::parse(goal_raw.trim_end_matches('\n').to_owned())?;
            let perm = parse_permission(permission)?;
            let ws = parse_workspace(workspace)?;

            let mut parsed_workers = Vec::with_capacity(workers.len());
            for entry in workers {
                let role_part = entry
                    .split_once('=')
                    .map(|(r, _)| r)
                    .unwrap_or(entry.as_str());
                let brief_str = if let Some(dir) = brief_dir {
                    let candidate = dir.join(format!("{role_part}.md"));
                    if candidate.exists() {
                        std::fs::read_to_string(&candidate).map_err(|e| {
                            invalid(
                                "brief",
                                format!("failed reading {}: {e}", candidate.display()),
                            )
                        })?
                    } else {
                        goal_body.as_str().to_owned()
                    }
                } else {
                    goal_body.as_str().to_owned()
                };
                parsed_workers.push(parse_worker_spec(entry, brief_str, perm, ws.clone())?);
            }

            let default_provider = parsed_workers
                .first()
                .and_then(|w| w.agent.as_ref().map(|a| a.provider.clone()))
                .unwrap_or_else(|| ProviderId::parse("codex").expect("valid provider"));
            let defaults = AgentSpec {
                provider: default_provider,
                model: None,
                reasoning: None,
                service_tier: None,
                permission: perm,
                workspace: ws,
            };

            Ok(WireCommand::WaveStart(WaveStart {
                spec: WaveSpec {
                    slug,
                    project,
                    goal: goal_body,
                    defaults,
                    workers: parsed_workers,
                },
                idem: None,
            }))
        }
        WaveCmd::AddWorker {
            wave,
            worker,
            brief_file,
            brief,
            permission,
            workspace,
        } => {
            let wave = WaveSlug::parse(wave.clone())?;
            let perm = parse_permission(permission)?;
            let ws = parse_workspace(workspace)?;
            let brief_raw = read_text_source(brief.as_ref(), brief_file.as_ref(), "brief")?;
            let spec = parse_worker_spec(worker, brief_raw, perm, ws)?;
            Ok(WireCommand::WaveAddWorker(WaveAddWorker {
                wave,
                worker: spec,
            }))
        }
        WaveCmd::Approve {
            wave,
            role,
            interaction,
            session,
        } => Ok(WireCommand::WaveResolve(WaveResolve {
            wave: WaveSlug::parse(wave.clone())?,
            role: RoleName::parse(role.clone())?,
            interaction: BbInteractionId::parse(interaction.clone())?,
            resolution: if *session {
                InteractionResolution::AllowForSession {
                    granted_permissions: None,
                }
            } else {
                InteractionResolution::AllowOnce {
                    granted_permissions: None,
                }
            },
        })),
        WaveCmd::Deny {
            wave,
            role,
            interaction,
        } => Ok(WireCommand::WaveResolve(WaveResolve {
            wave: WaveSlug::parse(wave.clone())?,
            role: RoleName::parse(role.clone())?,
            interaction: BbInteractionId::parse(interaction.clone())?,
            resolution: InteractionResolution::Deny,
        })),
        WaveCmd::Steer {
            wave,
            role,
            text,
            mode,
        } => Ok(WireCommand::WaveTell(WaveTell {
            wave: WaveSlug::parse(wave.clone())?,
            role: RoleName::parse(role.clone())?,
            body: Body::parse(text.clone())?,
            mode: parse_steer_mode(mode)?,
        })),
        WaveCmd::Retire { wave, role, reason } => {
            Ok(WireCommand::WaveRetireWorker(WaveRetireWorker {
                wave: WaveSlug::parse(wave.clone())?,
                role: RoleName::parse(role.clone())?,
                reason: reason.clone(),
            }))
        }
        WaveCmd::Status { wave } => Ok(WireCommand::WaveView(WaveView {
            wave: WaveSlug::parse(wave.clone())?,
        })),
        WaveCmd::Finish { wave, summary } => Ok(WireCommand::WaveFinish(WaveFinish {
            wave: WaveSlug::parse(wave.clone())?,
            outcome: WaveOutcome::Completed {
                summary: Body::parse(summary.clone())?,
            },
        })),
    }
}

fn build_task_transition(
    room: &str,
    seq: u64,
    status: TaskStatus,
    epoch: Option<u64>,
    expected_rev: Option<u64>,
    note: Option<&String>,
    idem: Option<&String>,
) -> Result<WireCommand, ApiError> {
    Ok(WireCommand::TaskTransition(TaskTransition {
        room: parse_room_ref(room)?,
        task: Seq(seq),
        status,
        epoch: epoch.map(LeaseEpoch),
        expected_rev,
        note: match note {
            Some(raw) => Some(read_body_arg(raw)?),
            None => None,
        },
        idem: match idem {
            Some(k) => Some(IdemKey::parse(k.clone())?),
            None => None,
        },
    }))
}

fn task_to_command(cmd: &TaskCmd, me: &Principal) -> Result<WireCommand, ApiError> {
    match cmd {
        TaskCmd::Add {
            first,
            second,
            board,
            lane,
            assign,
            priority,
            body,
            org_id,
            needs,
            due,
            labels,
            idem,
        } => {
            if let Some(board_raw) = board {
                let assignee = match assign {
                    Some(who) => Some(Assign::To {
                        who: map_board_val("assignee", Assignee::parse(who.clone()))?,
                    }),
                    None => None,
                };
                let lane_parsed = match lane {
                    Some(l) => Some(map_board_val("lane", LaneName::parse(l.clone()))?),
                    None => None,
                };
                let prio_parsed = match priority {
                    Some(p) => Some(parse_card_priority(p)?),
                    None => None,
                };
                return Ok(WireCommand::CardUpsert(CardUpsert {
                    board: map_board_val("board", BoardId::parse(board_raw.clone()))?,
                    card: None,
                    expected: None,
                    patch: CardPatch {
                        title: Some(map_board_val("title", Title::parse(first.clone()))?),
                        lane: lane_parsed,
                        assignee,
                        priority: prio_parsed,
                        ..CardPatch::default()
                    },
                    idem: match idem {
                        Some(k) => Some(map_board_val(
                            "idem",
                            serde_json::from_value(serde_json::Value::String(k.clone())),
                        )?),
                        None => None,
                    },
                }));
            }

            let title_raw = second.as_ref().ok_or_else(|| {
                invalid(
                    "title",
                    "usage: `rex-wave tasks add <room> <title>` or `rex-wave task add <title> --board <board>`",
                )
            })?;
            let room_ref = parse_room_ref(first)?;
            let title = map_board_val("title", Title::parse(title_raw.clone()))?;
            let msg_body = match body {
                Some(raw) => read_body_arg(raw)?,
                None => Body::parse(title_raw.clone())?,
            };
            Ok(WireCommand::RoomPost(RoomPost {
                room: room_ref,
                body: msg_body,
                re: None,
                after: None,
                labels: parse_labels(labels)?,
                task: Some(TaskSpec {
                    title,
                    org_id: match org_id {
                        Some(raw) => Some(map_board_val("org_id", OrgId::parse(raw.clone()))?),
                        None => None,
                    },
                    needs: needs.iter().copied().map(Seq).collect(),
                    priority: match priority {
                        Some(p) => Some(parse_card_priority(p)?),
                        None => None,
                    },
                    due_ms: due.map(UnixMs),
                }),
                idem: match idem {
                    Some(k) => Some(IdemKey::parse(k.clone())?),
                    None => None,
                },
            }))
        }
        TaskCmd::Move {
            board,
            no,
            lane,
            expected,
        } => Ok(WireCommand::CardUpsert(CardUpsert {
            board: map_board_val("board", BoardId::parse(board.clone()))?,
            card: Some(CardNo(*no)),
            expected: expected.map(rex_wave_api::board::CardRevision),
            patch: CardPatch {
                lane: Some(map_board_val("lane", LaneName::parse(lane.clone()))?),
                ..CardPatch::default()
            },
            idem: None,
        })),
        TaskCmd::List {
            target,
            board_flag,
            lane,
            assign,
            status,
            ready,
            mine,
            holder,
            label,
        } => {
            if board_flag.is_none()
                && let Some(room_raw) = target
                && is_room_syntax(room_raw)
            {
                let status_val = match status {
                    Some(s) => Some(parse_task_status(s)?),
                    None => None,
                };
                let holder_val = if *mine {
                    Some(me.clone())
                } else if let Some(h) = holder {
                    Some(parse_principal_ref(h)?)
                } else {
                    None
                };
                let label_val = match label {
                    Some(l) => Some(Label::parse(l.clone())?),
                    None => None,
                };
                let filter = if status_val.is_some()
                    || *ready
                    || holder_val.is_some()
                    || label_val.is_some()
                {
                    Some(TaskFilter {
                        status: status_val,
                        ready_only: *ready,
                        holder: holder_val,
                        label: label_val,
                    })
                } else {
                    None
                };
                return Ok(WireCommand::RoomTasks(RoomTasks {
                    room: parse_room_ref(room_raw)?,
                    filter,
                }));
            }

            let chosen_board = match target.as_ref().or(board_flag.as_ref()) {
                Some(b) => map_board_val("board", BoardId::parse(b.clone()))?,
                None => match me {
                    Principal::Operator { seat } | Principal::Seat { seat } => {
                        rex_wave_api::board::seat_board(seat)
                    }
                    _ => map_board_val("board", BoardId::parse("seat:owner"))?,
                },
            };
            let filter = if lane.is_some() || assign.is_some() || label.is_some() {
                Some(BoardFilter {
                    assignee: match assign {
                        Some(a) => Some(map_board_val("assignee", Assignee::parse(a.clone()))?),
                        None => None,
                    },
                    label: label.clone(),
                    lane: match lane {
                        Some(l) => Some(map_board_val("lane", LaneName::parse(l.clone()))?),
                        None => None,
                    },
                })
            } else {
                None
            };
            Ok(WireCommand::BoardView(BoardView {
                board: chosen_board,
                filter,
            }))
        }
        TaskCmd::Show { room, .. } => Ok(WireCommand::RoomTasks(RoomTasks {
            room: parse_room_ref(room)?,
            filter: None,
        })),
        TaskCmd::Claim {
            room,
            seq,
            ttl,
            ttl_ms,
            expected_rev,
            force,
            idem,
        } => Ok(WireCommand::TaskClaim(TaskClaim {
            room: parse_room_ref(room)?,
            task: Seq(*seq),
            ttl_ms: resolve_ttl_ms(ttl.as_ref(), *ttl_ms)?,
            expected_rev: *expected_rev,
            force: *force,
            idem: match idem {
                Some(k) => Some(IdemKey::parse(k.clone())?),
                None => None,
            },
        })),
        TaskCmd::Heartbeat {
            room,
            seq,
            epoch,
            ttl,
            ttl_ms,
        } => Ok(WireCommand::TaskHeartbeat(TaskHeartbeat {
            room: parse_room_ref(room)?,
            task: Seq(*seq),
            epoch: LeaseEpoch(*epoch),
            ttl_ms: resolve_ttl_ms(ttl.as_ref(), *ttl_ms)?,
        })),
        TaskCmd::Transition {
            room,
            seq,
            status,
            epoch,
            expected_rev,
            note,
            idem,
        } => build_task_transition(
            room,
            *seq,
            parse_task_status(status)?,
            *epoch,
            *expected_rev,
            note.as_ref(),
            idem.as_ref(),
        ),
        TaskCmd::Done {
            target,
            no,
            epoch,
            expected_rev,
            note,
            idem,
        } => {
            if is_room_syntax(target) {
                build_task_transition(
                    target,
                    *no,
                    TaskStatus::Done,
                    *epoch,
                    *expected_rev,
                    note.as_ref(),
                    idem.as_ref(),
                )
            } else {
                let card_no = u32::try_from(*no)
                    .map_err(|_| invalid("card", "kanban card number out of u32 range"))?;
                Ok(WireCommand::CardUpsert(CardUpsert {
                    board: map_board_val("board", BoardId::parse(target.clone()))?,
                    card: Some(CardNo(card_no)),
                    expected: expected_rev.map(rex_wave_api::board::CardRevision),
                    patch: CardPatch {
                        lane: Some(map_board_val("lane", LaneName::parse("done"))?),
                        ..CardPatch::default()
                    },
                    idem: match idem {
                        Some(k) => Some(map_board_val(
                            "idem",
                            serde_json::from_value(serde_json::Value::String(k.clone())),
                        )?),
                        None => None,
                    },
                }))
            }
        }
        TaskCmd::Block {
            room,
            seq,
            epoch,
            expected_rev,
            note,
            idem,
        } => build_task_transition(
            room,
            *seq,
            TaskStatus::Blocked,
            *epoch,
            *expected_rev,
            note.as_ref(),
            idem.as_ref(),
        ),
        TaskCmd::Review {
            room,
            seq,
            epoch,
            expected_rev,
            note,
            idem,
        } => build_task_transition(
            room,
            *seq,
            TaskStatus::Review,
            *epoch,
            *expected_rev,
            note.as_ref(),
            idem.as_ref(),
        ),
        TaskCmd::Release {
            room,
            seq,
            epoch,
            expected_rev,
            note,
            idem,
        } => build_task_transition(
            room,
            *seq,
            TaskStatus::Open,
            *epoch,
            *expected_rev,
            note.as_ref(),
            idem.as_ref(),
        ),
        TaskCmd::Update {
            room,
            seq,
            expected_rev,
            title,
            needs_add,
            needs_rm,
            priority,
            due,
            clear_due,
            idem,
        } => Ok(WireCommand::TaskUpdate(TaskUpdate {
            room: parse_room_ref(room)?,
            task: Seq(*seq),
            expected_rev: *expected_rev,
            title: match title {
                Some(t) => Some(map_board_val("title", Title::parse(t.clone()))?),
                None => None,
            },
            needs_add: needs_add.iter().copied().map(Seq).collect(),
            needs_remove: needs_rm.iter().copied().map(Seq).collect(),
            priority: match priority {
                Some(p) => Some(parse_card_priority(p)?),
                None => None,
            },
            due_ms: due.map(UnixMs),
            clear_due: *clear_due,
            idem: match idem {
                Some(k) => Some(IdemKey::parse(k.clone())?),
                None => None,
            },
        })),
        TaskCmd::Label {
            room,
            seq,
            add,
            remove,
            idem,
        } => {
            if add.is_empty() && remove.is_empty() {
                return Err(invalid(
                    "label",
                    "provide at least one --add <k=v> or --rm <k=v>",
                ));
            }
            Ok(WireCommand::RoomLabel(RoomLabel {
                room: parse_room_ref(room)?,
                seq: Seq(*seq),
                add: parse_labels(add)?,
                remove: parse_labels(remove)?,
                idem: match idem {
                    Some(k) => Some(IdemKey::parse(k.clone())?),
                    None => None,
                },
            }))
        }
        TaskCmd::Archive { board, no } => Ok(WireCommand::CardArchive(CardArchive {
            board: map_board_val("board", BoardId::parse(board.clone()))?,
            card: CardNo(*no),
            expected: None,
        })),
    }
}

fn presence_to_command(cmd: &PresenceCmd, me: &Principal) -> Result<WireCommand, ApiError> {
    match cmd {
        PresenceCmd::Claim {
            seat,
            status,
            role,
            projects,
            aliases,
            home_room,
            replace,
            idem,
        } => Ok(WireCommand::PresenceClaim(PresenceClaim {
            seat: SeatName::parse(seat.strip_prefix('@').unwrap_or(seat))?,
            status: match status {
                Some(s) => Some(parse_seat_status(s)?),
                None => None,
            },
            role: role.clone(),
            projects: projects.clone(),
            aliases: aliases
                .iter()
                .map(|a| SeatName::parse(a.strip_prefix('@').unwrap_or(a)))
                .collect::<Result<_, _>>()?,
            home_room: match home_room {
                Some(r) => Some(parse_room_ref(r)?.resolve(me)?),
                None => None,
            },
            verified_how: Some(VerificationKind::SelfWord),
            replace: match replace {
                Some(old) => Some(SeatName::parse(old.strip_prefix('@').unwrap_or(old))?),
                None => None,
            },
            idem: match idem {
                Some(k) => Some(IdemKey::parse(k.clone())?),
                None => None,
            },
        })),
        PresenceCmd::SetStatus {
            seat,
            status,
            reason,
        } => Ok(WireCommand::PresenceSetStatus(PresenceSetStatus {
            seat: SeatName::parse(seat.strip_prefix('@').unwrap_or(seat))?,
            status: parse_seat_status(status)?,
            verified_how: Some(VerificationKind::SelfWord),
            reason: reason.clone(),
        })),
        PresenceCmd::Bind {
            seat,
            incarnation,
            lane,
            from_claude_session,
            claude_pid,
            bb_thread,
            t3_thread,
            self_watcher,
            thread,
            pid,
            operator_toast,
            ttl,
            ttl_ms,
            takeover,
            allow_fork,
            idem,
        } => {
            let mut chosen: Vec<(TransportEndpoint, LivenessMode)> = Vec::new();
            if let Some(session_path) = from_claude_session {
                let ep = load_claude_session_endpoint(session_path, *claude_pid, *allow_fork)?;
                chosen.push((ep, LivenessMode::ProcessAndLease));
            }
            if let Some(tid) = bb_thread {
                chosen.push((
                    TransportEndpoint::BbThread {
                        thread_id: BbThreadId::parse(tid.clone())?,
                    },
                    LivenessMode::LeaseOnly,
                ));
            }
            if let Some(tid) = t3_thread {
                chosen.push((
                    TransportEndpoint::T3Thread {
                        thread_id: T3ThreadId::parse(tid.clone())?,
                    },
                    LivenessMode::ProcessAndLease,
                ));
            }
            if let Some(harness) = self_watcher {
                chosen.push((
                    TransportEndpoint::SelfWatcher {
                        harness: harness.clone(),
                        thread_id: thread.clone(),
                        pid: *pid,
                        proc_start: None,
                    },
                    LivenessMode::LeaseOnly,
                ));
            }
            if let Some(command) = operator_toast {
                chosen.push((
                    TransportEndpoint::OperatorToast {
                        command: command.clone(),
                    },
                    LivenessMode::LeaseOnly,
                ));
            }
            if chosen.len() != 1 {
                return Err(invalid(
                    "endpoint",
                    "specify exactly one of --from-claude-session, --bb-thread, --t3-thread, --self-watcher, or --operator-toast",
                ));
            }
            let (endpoint, liveness) = chosen.remove(0);
            Ok(WireCommand::PresenceBind(PresenceBind {
                seat: resolve_seat_arg(seat.as_ref(), me)?,
                incarnation_id: match incarnation {
                    Some(raw) => Some(IncarnationId::parse(raw.clone())?),
                    None => None,
                },
                lane: lane.clone(),
                endpoint,
                liveness,
                ttl_ms: resolve_ttl_ms(ttl.as_ref(), *ttl_ms)?,
                takeover: *takeover,
                allow_fork: *allow_fork,
                idem: match idem {
                    Some(k) => Some(IdemKey::parse(k.clone())?),
                    None => None,
                },
            }))
        }
        PresenceCmd::Hold { .. } => Err(invalid(
            "presence hold",
            "handled interactively via long-lived UDS session",
        )),
        PresenceCmd::Heartbeat {
            seat,
            incarnation,
            ttl,
            ttl_ms,
        } => Ok(WireCommand::PresenceHeartbeat(PresenceHeartbeat {
            seat: resolve_seat_arg(seat.as_ref(), me)?,
            incarnation_id: IncarnationId::parse(incarnation.clone())?,
            ttl_ms: resolve_ttl_ms(ttl.as_ref(), *ttl_ms)?,
        })),
        PresenceCmd::Unbind {
            seat,
            incarnation,
            reason,
        } => Ok(WireCommand::PresenceUnbind(PresenceUnbind {
            seat: resolve_seat_arg(seat.as_ref(), me)?,
            incarnation_id: IncarnationId::parse(incarnation.clone())?,
            reason: reason.clone(),
        })),
        PresenceCmd::List {
            project,
            live_only,
            include_retired,
        } => Ok(WireCommand::PresenceList(PresenceList {
            project: project.clone(),
            live_only: *live_only,
            include_retired: *include_retired,
        })),
        PresenceCmd::Resolve { query, lane } => Ok(WireCommand::PresenceResolve(PresenceResolve {
            query: query.clone(),
            lane: lane.clone(),
        })),
    }
}

fn parse_watch_target(
    target: &str,
    since: u64,
    me: &Principal,
) -> Result<(Interest, Cursor), ApiError> {
    match target {
        "inbox" => Ok((Interest::MyInbox, Cursor::live())),
        "sidebar" => Ok((Interest::Sidebar, Cursor::live())),
        "everything" => Ok((Interest::Everything, Cursor::live())),
        other => {
            if let Some(rest) = other.strip_prefix("agent:") {
                let (w, r) = rest
                    .split_once('/')
                    .ok_or_else(|| invalid("watch", "expected agent:<wave>/<role>"))?;
                return Ok((
                    Interest::Agent {
                        wave: WaveSlug::parse(w)?,
                        role: RoleName::parse(r)?,
                    },
                    Cursor::live(),
                ));
            }
            let room_ref = parse_room_ref(other)?;
            let room_id = room_ref.resolve(me)?;
            let cursor = Cursor::default().with(StreamId::Room { room: room_id }, Seq(since));
            Ok((Interest::Room { room: room_ref }, cursor))
        }
    }
}

fn parse_worker_ref(raw: &str) -> Result<(WaveSlug, RoleName), ApiError> {
    let trimmed = raw.strip_prefix('@').unwrap_or(raw);
    let (w, r) = trimmed
        .split_once('/')
        .ok_or_else(|| invalid("worker", "expected <wave>/<role>"))?;
    Ok((WaveSlug::parse(w)?, RoleName::parse(r)?))
}

fn extract_token_target(args: &TokenArgs) -> Result<TokenTarget, ApiError> {
    match (&args.seat, &args.worker, &args.sub) {
        (Some(s), None, None) => Ok(TokenTarget::Seat(SeatName::parse(
            s.strip_prefix('@').unwrap_or(s),
        )?)),
        (None, Some(w), None) => {
            let (wave, role) = parse_worker_ref(w)?;
            Ok(TokenTarget::Worker {
                wave,
                role,
                thread_id: args.thread.clone(),
            })
        }
        (
            _,
            _,
            Some(TokenSubcmd::Mint {
                seat_flag,
                worker_flag,
                thread,
                seat,
            }),
        ) => {
            if let Some(w) = worker_flag.as_deref().or(args.worker.as_deref()) {
                let (wave, role) = parse_worker_ref(w)?;
                return Ok(TokenTarget::Worker {
                    wave,
                    role,
                    thread_id: thread.clone().or_else(|| args.thread.clone()),
                });
            }
            let raw = seat_flag
                .as_deref()
                .or(seat.as_deref())
                .or(args.seat.as_deref())
                .ok_or_else(|| {
                    invalid(
                        "seat",
                        "provide --seat <seat>, --worker <wave>/<role>, or `token mint <seat>`",
                    )
                })?;
            Ok(TokenTarget::Seat(SeatName::parse(
                raw.strip_prefix('@').unwrap_or(raw),
            )?))
        }
        _ => Err(invalid(
            "seat",
            "provide `--seat <seat>`, `--worker <wave>/<role>`, or `token mint <seat>`",
        )),
    }
}

fn format_envelope_line(env: &Envelope) -> String {
    match &env.event {
        Event::Room(RoomEvent::Posted {
            body, re, labels, ..
        }) => {
            let re_part = re.map(|r| format!(" re=#{}", r.0)).unwrap_or_default();
            let lbl_part = if labels.is_empty() {
                String::new()
            } else {
                let joined: Vec<&str> = labels.iter().map(Label::as_str).collect();
                format!(" [{}]", joined.join(","))
            };
            format!(
                "[seq={}] {}{}{}: {}",
                env.seq.0,
                env.by,
                re_part,
                lbl_part,
                body.as_str()
            )
        }
        Event::Room(RoomEvent::Labeled {
            target,
            added,
            removed,
            ..
        }) => {
            let add_s: Vec<&str> = added.iter().map(Label::as_str).collect();
            let rm_s: Vec<&str> = removed.iter().map(Label::as_str).collect();
            format!(
                "[seq={}] {} labeled #{} +[{}] -[{}]",
                env.seq.0,
                env.by,
                target.0,
                add_s.join(","),
                rm_s.join(",")
            )
        }
        Event::Room(RoomEvent::TaskClaimed { task, rev, lease }) => format!(
            "[seq={}] {} claimed task #{} (rev={}, epoch={})",
            env.seq.0, env.by, task.0, rev, lease.epoch.0
        ),
        Event::Room(RoomEvent::TaskTransitioned { task, rev, status }) => format!(
            "[seq={}] {} set task #{} -> {:?} (rev={})",
            env.seq.0, env.by, task.0, status, rev
        ),
        _ => format!("[seq={}] {}: {:?}", env.seq.0, env.by, env.event),
    }
}

fn format_task_line(task: &TaskView) -> String {
    let holder = task
        .lease
        .as_ref()
        .map(|l| format!(" holder={} epoch={}", l.holder, l.epoch.0))
        .unwrap_or_default();
    let ready = if task.ready { " ready" } else { "" };
    let labels = if task.labels.is_empty() {
        String::new()
    } else {
        let joined: Vec<&str> = task.labels.iter().map(Label::as_str).collect();
        format!(" [{}]", joined.join(","))
    };
    format!(
        "  #{} [{:?}] {} (id={}, rev={}){}{}{}",
        task.seq.0, task.status, task.title, task.org_id, task.rev, ready, holder, labels
    )
}

fn print_reply(reply: &WireReply, as_json: bool) {
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(reply).expect("reply json")
        );
        return;
    }
    match reply {
        WireReply::RoomPost(p) => {
            let mut extra = String::new();
            if let Some(re) = p.re {
                extra.push_str(&format!(" re=#{}", re.0));
            }
            if let Some(ref org_id) = p.org_id {
                extra.push_str(&format!(" org_id={org_id}"));
            }
            if let Some(ref notice) = p.delivery_notice {
                extra.push_str(&format!(" ({notice})"));
            }
            println!("posted seq={} in {:?}{extra}", p.seq.0, p.room);
        }
        WireReply::RoomLabel(l) => {
            let joined: Vec<&str> = l.labels.iter().map(Label::as_str).collect();
            println!(
                "labeled seq={} in {:?}: [{}]",
                l.seq.0,
                l.room,
                joined.join(",")
            );
        }
        WireReply::TaskClaim(r) | WireReply::TaskHeartbeat(r) => {
            println!(
                "task #{} leased (org_id={}, rev={}, epoch={}, expires_at={})",
                r.task.0, r.org_id, r.rev, r.epoch.0, r.expires_at.0
            );
        }
        WireReply::TaskTransition(t) | WireReply::TaskUpdate(t) => {
            println!("{}", format_task_line(t).trim_start());
        }
        WireReply::RoomTasks(list) => {
            println!(
                "tasks in {:?} ({} tasks, head={}):",
                list.room,
                list.tasks.len(),
                list.head.0
            );
            for task in &list.tasks {
                println!("{}", format_task_line(task));
            }
        }
        WireReply::PresenceClaim(rec) | WireReply::PresenceSetStatus(rec) => {
            println!("seat @{} ({:?})", rec.seat, rec.status);
        }
        WireReply::PresenceBind(b) | WireReply::PresenceHeartbeat(b) => {
            println!(
                "bound @{} incarnation={} epoch={} status={:?}",
                b.seat, b.incarnation_id, b.epoch, b.status
            );
        }
        WireReply::PresenceList(snap) => {
            println!(
                "presence ({} seats, head={}):",
                snap.seats.len(),
                snap.head.0
            );
            for entry in &snap.seats {
                let live = entry
                    .bindings
                    .iter()
                    .filter(|b| b.status == rex_wave_api::presence::BindingStatus::Live)
                    .count();
                println!(
                    "  @{} ({:?}) live_bindings={}",
                    entry.record.seat, entry.record.status, live
                );
            }
        }
        WireReply::PresenceResolve(res) => {
            println!(
                "resolved @{} ({:?}, live_bindings={})",
                res.record.seat,
                res.record.status,
                res.live_bindings.len()
            );
        }
        WireReply::WaveStart(w) => {
            println!(
                "started wave {} (room={:?}, board={})",
                w.wave, w.room, w.board
            )
        }
        WireReply::WaveView(snap) => {
            println!(
                "wave {} ({:?}) project={}:",
                snap.spec.slug, snap.status, snap.spec.project
            );
            for (role, spec, phase) in &snap.workers {
                let phase_str = match phase {
                    WorkerPhase::Requested { attempt } => format!("requested(attempt={attempt})"),
                    WorkerPhase::Live { thread, activity } => {
                        format!("live({thread}, {activity:?})")
                    }
                    WorkerPhase::Retiring { thread, .. } => format!("retiring({thread})"),
                    WorkerPhase::Retired { .. } => "retired".to_owned(),
                    WorkerPhase::Failed { error } => format!("failed({error})"),
                };
                println!("  - {role} [{}] -> {phase_str}", spec.provider);
            }
        }
        WireReply::BoardView(b) => {
            println!("board {} (head={}):", b.board, b.head);
            for lane in &b.lanes {
                println!("  [{}] ({})", lane.lane, lane.cards.len());
                for c in &lane.cards {
                    let who = c
                        .assignee
                        .as_ref()
                        .map(|a| format!(" {a}"))
                        .unwrap_or_default();
                    println!("    #{} {}{}", c.no.0, c.title, who);
                }
            }
        }
        WireReply::CardUpsert(c) => {
            println!("card #{} on {} (rev={})", c.no.0, c.board, c.revision.0)
        }
        WireReply::InboxList(page) => {
            println!("inbox ({} unread, head={}):", page.unread, page.head.0);
            for item in &page.items {
                println!(
                    "  [{}] ({:?}) {} - {}",
                    item.id.0, item.state, item.draft.title, item.draft.preview
                );
            }
        }
        WireReply::MailPromote(letter) => {
            println!(
                "banked {} (sha256={}, wake={})",
                letter.path.display(),
                letter.sha256,
                letter.wake_id.0
            )
        }
        WireReply::Read(page) | WireReply::Wait(page) => {
            if page.timed_out && page.entries.is_empty() {
                println!("timed out (0 new entries)");
            } else if page.entries.is_empty() {
                println!("0 entries");
            } else {
                for env in &page.entries {
                    println!("{}", format_envelope_line(env));
                }
            }
        }
        other => println!(
            "{}",
            serde_json::to_string_pretty(other).expect("reply json")
        ),
    }
}

fn print_error(err: &CliError, as_json: bool) {
    match err {
        CliError::Api(api_err) if as_json => {
            eprintln!(
                "{}",
                serde_json::to_string_pretty(api_err).expect("api error json")
            );
        }
        CliError::Api(ApiError::Conflict { detail, data }) => {
            eprintln!("error: conflict: {detail}");
            if let Some(ConflictData::NewerMessage {
                expected,
                latest,
                newer_message,
            }) = data
            {
                eprintln!(
                    "newer_message: seq={} (expected after={}, latest={}) {}: {}",
                    newer_message.seq.0,
                    expected.0,
                    latest.0,
                    newer_message.by,
                    newer_message.body.as_str()
                );
            } else if let Some(d) = data {
                eprintln!(
                    "conflict_data: {}",
                    serde_json::to_string(d).unwrap_or_default()
                );
            }
        }
        CliError::Api(api_err) => eprintln!("error: {api_err}"),
        CliError::Msg(msg) => eprintln!("error: {msg}"),
    }
}

async fn execute_task_show<C: Client>(
    client: &C,
    room: &str,
    seq: u64,
    as_json: bool,
) -> Result<ExitCode, CliError> {
    let room_ref = parse_room_ref(room)?;
    let room_id = room_ref.resolve(client.me())?;
    let list = client
        .call(RoomTasks {
            room: room_ref,
            filter: None,
        })
        .await?;
    let task = list
        .tasks
        .into_iter()
        .find(|t| t.seq == Seq(seq))
        .ok_or_else(|| ApiError::NotFound {
            what: format!("task #{seq} in {room}"),
        })?;

    let mut wait = Wait::room_filtered(room_id, Seq::ORIGIN, Some(Seq(seq)), Vec::new());
    wait.timeout_ms = Some(0);
    let replies = client.call(wait).await?.entries;

    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "task": task,
                "entries": replies,
            }))
            .expect("task show json")
        );
    } else {
        println!("{}", format_task_line(&task).trim_start());
        for env in &replies {
            println!("  {}", format_envelope_line(env));
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn execute_presence_hold<C: Client>(
    client: &C,
    seat: Option<&String>,
    lane: Option<&String>,
    incarnation: Option<&String>,
    takeover: bool,
    client_name: &str,
    as_json: bool,
) -> Result<ExitCode, CliError> {
    let seat_name = resolve_seat_arg(seat, client.me())?;
    let binding = client
        .call(PresenceBind {
            seat: seat_name.clone(),
            incarnation_id: match incarnation {
                Some(raw) => Some(IncarnationId::parse(raw.clone())?),
                None => None,
            },
            lane: lane.cloned(),
            endpoint: TransportEndpoint::UdsSession {
                conn_id: 0,
                client: client_name.to_owned(),
            },
            liveness: LivenessMode::SocketLifetime,
            ttl_ms: None,
            takeover,
            allow_fork: false,
            idem: None,
        })
        .await?;
    print_reply(&WireReply::PresenceBind(binding.clone()), as_json);
    let _ = tokio::signal::ctrl_c().await;
    let _ = client
        .call(PresenceUnbind {
            seat: seat_name,
            incarnation_id: binding.incarnation_id,
            reason: Some("hold interrupted".into()),
        })
        .await;
    Ok(ExitCode::SUCCESS)
}

async fn execute_remote<C: Client>(
    client: &C,
    cmd: &Cmd,
    as_json: bool,
) -> Result<ExitCode, CliError> {
    if let Cmd::Tasks(TaskCmd::Show { room, seq }) = cmd {
        return execute_task_show(client, room, *seq, as_json).await;
    }
    if let Cmd::Presence(PresenceCmd::Hold {
        seat,
        lane,
        incarnation,
        takeover,
        client: client_name,
    }) = cmd
    {
        return execute_presence_hold(
            client,
            seat.as_ref(),
            lane.as_ref(),
            incarnation.as_ref(),
            *takeover,
            client_name,
            as_json,
        )
        .await;
    }

    let wire = to_command_for(cmd, client.me())?;
    let mut reply = client.call_wire(wire).await?;

    if let Cmd::Read { limit, .. } = cmd
        && let WireReply::Wait(mut page) = reply
    {
        page.timed_out = false;
        if let Some(lim) = limit {
            let cap = (*lim).clamp(1, 1000) as usize;
            if page.entries.len() > cap {
                page.entries.truncate(cap);
                if let Some(last) = page.entries.last() {
                    page.cursor = Cursor::default().with(last.stream.clone(), last.seq);
                }
            }
        }
        reply = WireReply::Read(page);
    }

    let timed_out = matches!(cmd, Cmd::Wait { .. })
        && matches!(&reply, WireReply::Wait(p) if p.timed_out && p.entries.is_empty());

    print_reply(&reply, as_json);
    if timed_out {
        Ok(ExitCode::from(EXIT_TIMEOUT))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let sock_path = daemon::resolve_socket_path(cli.socket.as_deref());
    let endpoint = Endpoint::Unix(sock_path.clone());

    let outcome: Result<ExitCode, CliError> = match cli.cmd {
        Cmd::Daemon(args) => {
            let bb = args.bb_url.split('?').next().unwrap_or(&args.bb_url);
            let runner = daemon::resolve_runner_mode(&args);
            let http_str = args.http.map(|a| format!(", http={a}")).unwrap_or_default();
            eprintln!(
                "rex-wave daemon: starting (operator={}, runner={runner:?}, socket={}, bb={bb}{http_str})",
                args.operator,
                sock_path.display(),
            );
            let res = daemon::run(args, cli.socket).await;
            if res.is_ok() {
                eprintln!("rex-wave daemon: stopped");
            }
            res.map(|()| ExitCode::SUCCESS).map_err(CliError::Msg)
        }
        Cmd::Tui { self_test: true } => rex_wave_tui::demo::run_self_test_demo()
            .await
            .map(|_| ExitCode::SUCCESS)
            .map_err(|e| CliError::Msg(e.to_string())),
        Cmd::Tui { self_test: false } => {
            match resolve_cli_credential(&sock_path, cli.token.as_deref()) {
                Ok(cred) => match RemoteClient::connect(endpoint, cred, "tui").await {
                    Ok(client) => {
                        let connection = client.watch_connection();
                        rex_wave_tui::run_with_connection(client, connection)
                            .await
                            .map(|()| ExitCode::SUCCESS)
                            .map_err(|e| CliError::Msg(e.to_string()))
                    }
                    Err(e) => Err(CliError::Api(e)),
                },
                Err(e) => Err(CliError::Msg(e)),
            }
        }
        Cmd::Mcp { stdio: _, http } => match cli
            .token
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
        {
            Some(tok) => {
                let token = tok.to_owned();
                let http_token = token.clone();
                match RemoteClient::connect(endpoint, Credential::Token { token }, "mcp").await {
                    Ok(client) => {
                        let server = McpServer::new(client);
                        let res = match http {
                            Some(addr) => server.with_http_token(http_token).serve_http(addr).await,
                            None => server.serve_stdio().await,
                        };
                        res.map(|()| ExitCode::SUCCESS)
                            .map_err(|e| CliError::Msg(e.to_string()))
                    }
                    Err(e) => Err(CliError::Api(e)),
                }
            }
            None => Err(CliError::Msg(
                "mcp requires --token <tok> or REX_WAVE_TOKEN".to_owned(),
            )),
        },
        Cmd::Token(token_args) => match extract_token_target(&token_args) {
            Ok(TokenTarget::Seat(seat)) => match daemon::mint_seat_token(&sock_path, &seat) {
                Ok(tok) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({ "seat": seat.as_str(), "token": tok })
                        );
                    } else {
                        println!("{tok}");
                    }
                    Ok(ExitCode::SUCCESS)
                }
                Err(e) => Err(CliError::Msg(format!("mint token: {e}"))),
            },
            Ok(TokenTarget::Worker {
                wave,
                role,
                thread_id,
            }) => match daemon::mint_worker_token(
                &sock_path,
                &wave,
                &role,
                thread_id.as_deref(),
                true,
            ) {
                Ok(tok) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "wave": wave.as_str(),
                                "role": role.as_str(),
                                "thread_id": thread_id,
                                "token": tok,
                            })
                        );
                    } else {
                        println!("{tok}");
                    }
                    Ok(ExitCode::SUCCESS)
                }
                Err(e) => Err(CliError::Msg(format!("mint worker token: {e}"))),
            },
            Err(e) => Err(CliError::Api(e)),
        },
        Cmd::Schema { bb } => {
            let out = if bb {
                rex_wave_bb::bb_tool_catalog()
            } else {
                rex_wave_api::tool_catalog()
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&out).expect("schema json")
            );
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Watch { target, since } => {
            match resolve_cli_credential(&sock_path, cli.token.as_deref()) {
                Ok(cred) => match RemoteClient::connect(endpoint, cred, "cli-watch").await {
                    Ok(client) => match parse_watch_target(&target, since, client.me()) {
                        Ok((interest, cursor)) => match client.watch(interest, cursor).await {
                            Ok(mut stream) => loop {
                                tokio::select! {
                                    _ = tokio::signal::ctrl_c() => break Ok(ExitCode::SUCCESS),
                                    item = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)) => match item {
                                        Some(Ok(frame)) => {
                                            println!("{}", serde_json::to_string(&frame).expect("frame json"));
                                        }
                                        Some(Err(e)) => break Err(CliError::Api(e)),
                                        None => break Ok(ExitCode::SUCCESS),
                                    }
                                }
                            },
                            Err(e) => Err(CliError::Api(e)),
                        },
                        Err(e) => Err(CliError::Api(e)),
                    },
                    Err(e) => Err(CliError::Api(e)),
                },
                Err(e) => Err(CliError::Msg(e)),
            }
        }
        other => match resolve_cli_credential(&sock_path, cli.token.as_deref()) {
            Ok(cred) => match RemoteClient::connect(endpoint, cred, "cli").await {
                Ok(client) => execute_remote(&client, &other, cli.json).await,
                Err(e) => Err(CliError::Api(e)),
            },
            Err(e) => Err(CliError::Msg(e)),
        },
    };

    match outcome {
        Ok(code) => code,
        Err(err) => {
            print_error(&err, cli.json);
            err.exit_code()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use rex_wave_transport::Authenticator;
    use rex_wave_tui::demo::DemoClient;
    use std::os::unix::fs::PermissionsExt;

    fn parse_cli(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("valid CLI args")
    }

    fn to_command(cmd: &Cmd) -> Result<WireCommand, ApiError> {
        let default_me = Principal::Operator {
            seat: SeatName::parse("owner").expect("valid seat"),
        };
        to_command_for(cmd, &default_me)
    }

    #[test]
    fn cli_parses_every_architecture_verb_into_exact_wire_command() {
        let tmp = std::env::temp_dir().join(format!("rex-wave-goal-{}.md", std::process::id()));
        std::fs::write(&tmp, "Ship OAuth refresh token rotation").unwrap();

        let wave_cli = parse_cli(&[
            "rex-wave",
            "wave",
            "start",
            "auth-v2",
            "--project",
            "p_123",
            "--goal-file",
            tmp.to_str().unwrap(),
            "--worker",
            "author-jwt=codex/gpt-6.1-sol",
            "--worker",
            "verifier=claude-code/claude-opus-5-5",
            "--worker",
            "migrator=pi/openrouter/~anthropic/claude-sonnet-latest",
        ]);
        let WireCommand::WaveStart(ws) = to_command(&wave_cli.cmd).unwrap() else {
            panic!("expected WaveStart");
        };
        let _ = std::fs::remove_file(&tmp);
        assert_eq!(ws.spec.slug.as_str(), "auth-v2");
        assert_eq!(ws.spec.project.as_str(), "p_123");
        assert_eq!(ws.spec.workers.len(), 3);
        assert_eq!(
            ws.spec.workers[2].agent.as_ref().unwrap().provider.as_str(),
            "pi"
        );
        assert_eq!(
            ws.spec.workers[2]
                .agent
                .as_ref()
                .unwrap()
                .model
                .as_ref()
                .unwrap()
                .as_str(),
            "openrouter/~anthropic/claude-sonnet-latest"
        );

        let add_cli = parse_cli(&[
            "rex-wave",
            "wave",
            "add-worker",
            "auth-v2",
            "auditor=codex/gpt-6.1-sol",
            "--brief",
            "Audit JWT claims",
        ]);
        assert!(matches!(
            to_command(&add_cli.cmd).unwrap(),
            WireCommand::WaveAddWorker(_)
        ));

        let approve_cli = parse_cli(&[
            "rex-wave", "wave", "approve", "auth-v2", "migrator", "int_8f2",
        ]);
        assert!(matches!(
            to_command(&approve_cli.cmd).unwrap(),
            WireCommand::WaveResolve(WaveResolve {
                resolution: InteractionResolution::AllowOnce { .. },
                ..
            })
        ));

        let deny_cli = parse_cli(&["rex-wave", "wave", "deny", "auth-v2", "migrator", "int_8f2"]);
        assert!(matches!(
            to_command(&deny_cli.cmd).unwrap(),
            WireCommand::WaveResolve(WaveResolve {
                resolution: InteractionResolution::Deny,
                ..
            })
        ));

        let steer_cli = parse_cli(&[
            "rex-wave",
            "wave",
            "steer",
            "auth-v2",
            "migrator",
            "pause writes",
            "--mode",
            "steer-if-active",
        ]);
        assert!(matches!(
            to_command(&steer_cli.cmd).unwrap(),
            WireCommand::WaveTell(WaveTell {
                mode: SteerDeliveryMode::SteerIfActive,
                ..
            })
        ));

        let retire_cli = parse_cli(&["rex-wave", "wave", "retire", "auth-v2", "migrator"]);
        assert!(matches!(
            to_command(&retire_cli.cmd).unwrap(),
            WireCommand::WaveRetireWorker(_)
        ));

        let status_cli = parse_cli(&["rex-wave", "wave", "status", "auth-v2"]);
        assert!(matches!(
            to_command(&status_cli.cmd).unwrap(),
            WireCommand::WaveView(_)
        ));

        let post_chan = parse_cli(&[
            "rex-wave",
            "post",
            "#p_123/wave-auth-v2",
            "@verifier check #4",
            "--re",
            "2",
            "--after",
            "3",
            "--label",
            "status=ready",
            "--id",
            "k1",
        ]);
        let WireCommand::RoomPost(rp) = to_command(&post_chan.cmd).unwrap() else {
            panic!("expected RoomPost");
        };
        assert_eq!(rp.re, Some(Seq(2)));
        assert_eq!(rp.after, Some(Seq(3)));
        assert_eq!(rp.labels.len(), 1);
        assert_eq!(rp.idem.as_ref().unwrap().as_str(), "k1");

        let reply_cli = parse_cli(&[
            "rex-wave",
            "reply",
            "#general",
            "7",
            "looks good",
            "--after",
            "8",
            "--label",
            "review=lgtm",
        ]);
        let WireCommand::RoomPost(rep) = to_command(&reply_cli.cmd).unwrap() else {
            panic!("expected RoomPost for reply");
        };
        assert_eq!(rep.re, Some(Seq(7)));
        assert_eq!(rep.after, Some(Seq(8)));

        let read_filtered = parse_cli(&[
            "rex-wave",
            "read",
            "#general",
            "--after",
            "1",
            "--re",
            "2",
            "--label",
            "status=open",
        ]);
        assert!(matches!(
            to_command(&read_filtered.cmd).unwrap(),
            WireCommand::Wait(Wait {
                interest: Interest::RoomFiltered {
                    re: Some(Seq(2)),
                    ..
                },
                timeout_ms: Some(0),
                ..
            })
        ));

        let wait_filtered = parse_cli(&[
            "rex-wave",
            "wait",
            "#p_123/wave-auth-v2",
            "--since",
            "41",
            "--re",
            "10",
            "--label",
            "kind=review",
            "--timeout",
            "5000",
        ]);
        assert!(matches!(
            to_command(&wait_filtered.cmd).unwrap(),
            WireCommand::Wait(Wait {
                interest: Interest::RoomFiltered {
                    re: Some(Seq(10)),
                    ..
                },
                timeout_ms: Some(5000),
                ..
            })
        ));

        let read_cursor = serde_json::to_string(&Cursor::default().with(
            StreamId::Room {
                room: rex_wave_api::room::RoomId::Channel {
                    scope: rex_wave_api::room::RoomScope::Global,
                    name: ChannelName::parse("general").unwrap(),
                },
            },
            Seq(12),
        ))
        .unwrap();
        let wait_mentions_cursor =
            parse_cli(&["rex-wave", "wait", "#general", "--cursor", &read_cursor]);
        let WireCommand::Wait(wmc) = to_command(&wait_mentions_cursor.cmd).unwrap() else {
            panic!("expected Wait");
        };
        assert!(
            wmc.after.from_head(),
            "mentions wait with a read cursor must preserve from_head=true"
        );
        assert_eq!(
            wmc.after.at(&StreamId::Room {
                room: rex_wave_api::room::RoomId::Channel {
                    scope: rex_wave_api::room::RoomScope::Global,
                    name: ChannelName::parse("general").unwrap(),
                },
            }),
            Seq(12)
        );

        let task_add = parse_cli(&[
            "rex-wave",
            "task",
            "add",
            "Wire OAuth refresh",
            "--board",
            "project:p_123",
            "--lane",
            "todo",
            "--assign",
            "@auth-v2/author-jwt",
        ]);
        assert!(
            matches!(to_command(&task_add.cmd).unwrap(), WireCommand::CardUpsert(u) if u.card.is_none())
        );

        let room_task_add = parse_cli(&[
            "rex-wave",
            "tasks",
            "add",
            "#general",
            "Ship HTTP server",
            "--org-id",
            "rw-http1",
            "--needs",
            "4",
            "--priority",
            "high",
            "--label",
            "area=http",
        ]);
        let WireCommand::RoomPost(rt) = to_command(&room_task_add.cmd).unwrap() else {
            panic!("expected RoomPost task");
        };
        let spec = rt.task.expect("task spec");
        assert_eq!(spec.org_id.unwrap().as_str(), "rw-http1");
        assert_eq!(spec.needs, vec![Seq(4)]);

        let task_claim = parse_cli(&[
            "rex-wave",
            "tasks",
            "claim",
            "#general",
            "9",
            "--ttl",
            "90s",
            "--expected-rev",
            "2",
        ]);
        let WireCommand::TaskClaim(tc) = to_command(&task_claim.cmd).unwrap() else {
            panic!("expected TaskClaim");
        };
        assert_eq!(tc.task, Seq(9));
        assert_eq!(tc.ttl_ms, Some(90_000));
        assert_eq!(tc.expected_rev, Some(2));

        let task_done_room = parse_cli(&[
            "rex-wave", "tasks", "done", "#general", "9", "--epoch", "3", "--note", "merged",
        ]);
        assert!(matches!(
            to_command(&task_done_room.cmd).unwrap(),
            WireCommand::TaskTransition(TaskTransition {
                status: TaskStatus::Done,
                epoch: Some(LeaseEpoch(3)),
                ..
            })
        ));

        let task_move = parse_cli(&["rex-wave", "task", "move", "project:p_123", "1", "review"]);
        assert!(matches!(
            to_command(&task_move.cmd).unwrap(),
            WireCommand::CardUpsert(u) if u.card == Some(CardNo(1))
        ));

        let task_list = parse_cli(&["rex-wave", "task", "list", "--board", "project:p_123"]);
        assert!(matches!(
            to_command(&task_list.cmd).unwrap(),
            WireCommand::BoardView(_)
        ));

        let room_task_list = parse_cli(&["rex-wave", "tasks", "list", "#general", "--ready"]);
        assert!(matches!(
            to_command(&room_task_list.cmd).unwrap(),
            WireCommand::RoomTasks(RoomTasks {
                filter: Some(TaskFilter {
                    ready_only: true,
                    ..
                }),
                ..
            })
        ));

        let pres_claim = parse_cli(&[
            "rex-wave",
            "presence",
            "claim",
            "sam",
            "--role",
            "reviewer",
            "--project",
            "rex-wave",
            "--alias",
            "sammy",
        ]);
        assert!(matches!(
            to_command(&pres_claim.cmd).unwrap(),
            WireCommand::PresenceClaim(_)
        ));

        let pres_bind = parse_cli(&[
            "rex-wave",
            "presence",
            "bind",
            "--seat",
            "sam",
            "--self-watcher",
            "codex",
            "--ttl",
            "15m",
        ]);
        let WireCommand::PresenceBind(pb) = to_command(&pres_bind.cmd).unwrap() else {
            panic!("expected PresenceBind");
        };
        assert_eq!(pb.ttl_ms, Some(900_000));

        let promote_cli = parse_cli(&[
            "rex-wave",
            "promote",
            "#p_123/wave-auth-v2",
            "40..44",
            "--to",
            "owner",
            "--subject",
            "auth-v2 ruling",
            "--topic",
            "auth-v2-ruling",
        ]);
        let WireCommand::MailPromote(mp) = to_command(&promote_cli.cmd).unwrap() else {
            panic!("expected MailPromote");
        };
        assert_eq!(mp.range.first(), Seq(40));
        assert_eq!(mp.range.last(), Seq(44));

        let call_cli = parse_cli(&[
            "rex-wave",
            "call",
            "room_post",
            r#"{"room":{"ref":"dm","with":{"kind":"seat","seat":"alice"}},"body":"lgtm"}"#,
        ]);
        assert!(matches!(
            to_command(&call_cli.cmd).unwrap(),
            WireCommand::RoomPost(_)
        ));
    }

    #[tokio::test]
    async fn token_mint_and_demo_dispatch_work_end_to_end() {
        let dir = std::env::temp_dir().join(format!("rex-wave-tok-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("daemon.sock");

        let tok_cli = parse_cli(&["rex-wave", "token", "mint", "--seat", "sam"]);
        let Cmd::Token(targs) = tok_cli.cmd else {
            panic!("expected token");
        };
        let TokenTarget::Seat(seat) = extract_token_target(&targs).unwrap() else {
            panic!("expected seat target");
        };
        let minted = daemon::mint_seat_token(&sock, &seat).unwrap();
        assert!(minted.starts_with("rw_"));

        let tok_path = daemon::tokens_path_for(&sock);
        let mode = std::fs::metadata(&tok_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);

        let auth = daemon::TokenFileAuthenticator {
            operator: SeatName::parse("owner").unwrap(),
            daemon_uid: None,
            tokens_path: tok_path,
        };
        let principal = auth
            .authenticate(None, &Credential::Token { token: minted })
            .unwrap();
        assert_eq!(principal, Principal::Seat { seat });

        let wave = WaveSlug::parse("auth-v2").unwrap();
        let role = RoleName::parse("author").unwrap();
        let wtok =
            daemon::mint_worker_token(&sock, &wave, &role, Some("t3:auth-v2:author:a1"), true)
                .unwrap();
        assert!(wtok.starts_with("rw_w_"));
        let wprincipal = auth
            .authenticate(
                None,
                &Credential::Token {
                    token: wtok.clone(),
                },
            )
            .unwrap();
        assert_eq!(
            wprincipal,
            Principal::Worker {
                wave: wave.clone(),
                role: role.clone(),
            }
        );
        rex_wave_t3::remove_worker_token_file(&dir, "t3:auth-v2:author:a1").unwrap();
        assert!(
            auth.authenticate(None, &Credential::Token { token: wtok })
                .is_err(),
            "unlinked worker token file must revoke authentication"
        );

        let client = DemoClient::new();
        let status_cli = parse_cli(&["rex-wave", "wave", "status", "auth-v2"]);
        execute_remote(&client, &status_cli.cmd, true)
            .await
            .unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }
}
