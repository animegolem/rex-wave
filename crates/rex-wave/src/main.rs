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
    config::{ConfigGet, ConfigSet, ConfigShow, SETTINGS_FIELDS},
    mail::{LetterPriority, MailPromote},
    notify::InboxList,
    presence::{
        LivenessMode, PresenceBind, PresenceClaim, PresenceHeartbeat, PresenceList,
        PresenceResolve, PresenceSetStatus, PresenceUnbind, RESIDENT_MANIFEST_SCHEMA,
        ResidentManifest, SeatStatus, TransportEndpoint, VerificationKind, WorkClass,
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
use rex_wave_board::task_org::{
    OrgConflictKind, OrgSyncConflict, OrgSyncOptions, OrgSyncState, OrgToRoomOutcome,
    RoomToOrgOutcome, sync_org_to_room, sync_room_to_org,
};
use rex_wave_mailbridge::{
    AliasMap, IncarnationAffinity, Presence, PresenceState, ReadOnlyInterchange, RetryState,
    RingPolicy, ShadowWatchTracker, check_gate1_fixtures_with_profile,
    compare_shadow_with_mailwatch_seen, extract_shadow_aliases, load_claude_session_file,
    load_house_profile, parse_acks_jsonl, parse_claude_session_json, parse_mailwatch_seen_sources,
    run_shadow_pass, score_shadow_jsonl, seed_records_from_interchange_with_options,
};
use rex_wave_mcp::McpServer;
use rex_wave_runtime::{Decider, EntryMeta};
use rex_wave_transport::{Credential, Endpoint, RemoteClient};
use std::io::{Read as _, Write as _};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

const EXIT_CONFLICT: u8 = 3;
const EXIT_TIMEOUT: u8 = 124;

#[derive(Parser, Debug)]
#[command(
    name = "rex-wave",
    version,
    about = "Waves, #channels, DMs, kanban, tasks, presence, and notifications for BB + T3 + Rex"
)]
pub struct Cli {
    /// Daemon Unix socket path (default: `$XDG_STATE_HOME/rex-wave/daemon.sock`; on macOS, must share `/tmp`'s filesystem).
    #[arg(long, global = true, env = "REX_WAVE_SOCKET")]
    pub socket: Option<PathBuf>,
    /// Capability token for seat or worker authentication (falls back to `REX_WAVE_WORKER_THREAD_ID` / `T3CODE_THREAD_ID` worker token, then Unix peer UID).
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
    /// Run the read-only shadow mail reactor and Gate 2 shadow scorer.
    #[command(subcommand)]
    Mail(MailCmd),
    /// Inspect or update operator configuration (`~/.config/rex-wave/config.toml`).
    #[command(subcommand)]
    Config(ConfigCmd),
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
        #[arg(long)]
        account: Option<String>,
        #[arg(long)]
        output_dir: Option<PathBuf>,
        #[arg(long = "worker-output")]
        worker_outputs: Vec<String>,
        #[arg(long = "worker-writes")]
        worker_writes: Vec<String>,
        #[arg(long = "worker-schema")]
        worker_schemas: Vec<String>,
        #[arg(long)]
        permission: Option<String>,
        #[arg(long)]
        workspace: Option<String>,
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
        #[arg(long)]
        account: Option<String>,
        #[arg(long)]
        output_dir: Option<PathBuf>,
        #[arg(long = "writes")]
        writes: Vec<PathBuf>,
        #[arg(long = "return-schema")]
        return_schema: Option<String>,
        #[arg(long)]
        permission: Option<String>,
        #[arg(long)]
        workspace: Option<String>,
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
        #[arg(long)]
        allow_incomplete: bool,
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
        #[arg(long)]
        limit: Option<u32>,
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
    /// Reconcile a room's task-threads two-way with an Emacs `.org` file.
    #[command(name = "org-sync")]
    OrgSync {
        room: String,
        file: PathBuf,
        /// Run one reconciliation pass and exit (default when `--watch` is omitted).
        #[arg(long, conflicts_with = "watch")]
        once: bool,
        /// Keep reconciling on a loop until interrupted.
        #[arg(long, conflicts_with = "once")]
        watch: bool,
        /// Poll interval in milliseconds for `--watch` mode.
        #[arg(long, default_value_t = 1000)]
        interval_ms: u64,
        /// Write a `<file>.rex-conflict` sidecar on revision or dependency conflicts.
        #[arg(long)]
        conflict_sidecar: bool,
    },
    /// Archive a Kanban card.
    Archive { board: String, no: u32 },
}

#[derive(Subcommand, Debug)]
pub enum ConfigCmd {
    /// Show full operator configuration and live runner/courier health.
    Show,
    /// Read one dotted configuration key (e.g. `runners.default_runner`).
    Get { key: String },
    /// Set one dotted configuration key and persist to `config.toml`.
    Set {
        key: String,
        value: String,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
    /// Reset/clear one dotted configuration key back to its default.
    Unset {
        key: String,
        #[arg(long = "id", alias = "idem")]
        idem: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum MailCmd {
    /// Run the structurally read-only shadow mail reactor over an interchange folder.
    Shadow {
        /// Interchange root directory (opened read-only).
        #[arg(long)]
        interchange: PathBuf,
        /// Output JSONL file path (`-` writes stdout; refused inside `--interchange`).
        #[arg(long)]
        out: PathBuf,
        /// Run one read-only shadow pass and exit (default when `--watch` is omitted).
        #[arg(long, conflicts_with = "watch")]
        once: bool,
        /// Re-scan periodically until interrupted.
        #[arg(long, conflicts_with = "once")]
        watch: bool,
        /// Poll interval in milliseconds for `--watch` mode.
        #[arg(long, default_value_t = 5000)]
        interval_ms: u64,
        /// Use an empty offline presence table without querying `--socket`.
        #[arg(long)]
        offline: bool,
        /// Explicit read-only marker flag (shadow is always structurally read-only).
        #[arg(long = "shadow-read-only")]
        shadow_read_only: bool,
    },
    /// Score a shadow JSONL decision log against ground-truth `acks.jsonl`.
    Score {
        /// Shadow JSONL output file produced by `rex-wave mail shadow`.
        #[arg(long)]
        shadow: PathBuf,
        /// Ground-truth `acks.jsonl` file (opened read-only).
        #[arg(long)]
        acks: PathBuf,
        /// Optional Gate 1 fixture file (`cases.jsonl`) to check case coverage.
        #[arg(long)]
        fixtures: Option<PathBuf>,
        /// Ring watcher log (`seen.jsonl`, mailwatch or t3-doorbell format) to
        /// cross-check shadow rings against. Repeat to merge several logs.
        #[arg(long = "mailwatch-seen")]
        mailwatch_seen: Vec<PathBuf>,
    },
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
        /// Optional mail horizon watermark (Unix milliseconds).
        #[arg(long = "mail-horizon")]
        mail_horizon: Option<u64>,
        /// Resident work class (`class_a` or `class_b`, per `seat-in-a-card.org`).
        #[arg(long = "work-class")]
        work_class: Option<String>,
        /// Resident portrait/avatar asset path or URI.
        #[arg(long)]
        avatar: Option<String>,
        /// Resident home/working directory.
        #[arg(long = "home-dir")]
        home_dir: Option<String>,
        /// Command used to start a fresh resident session.
        #[arg(long = "start-cmd")]
        start_cmd: Option<String>,
        /// Command used to resume an existing resident session.
        #[arg(long = "resume-cmd")]
        resume_cmd: Option<String>,
        /// Stable Rex terminal target identifier.
        #[arg(long = "rex-target")]
        rex_target: Option<String>,
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
        /// Optional explicit mail horizon watermark override (Unix milliseconds).
        #[arg(long = "mail-horizon")]
        mail_horizon: Option<u64>,
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
        /// Optional explicit mail horizon watermark override (Unix milliseconds).
        #[arg(long = "mail-horizon")]
        mail_horizon: Option<u64>,
        #[arg(long, default_value = "cli-hold")]
        client: String,
    },
    /// Seed durable seat records and aliases from an interchange folder (read-only on interchange).
    #[command(name = "seed-interchange")]
    SeedInterchange {
        /// Interchange directory containing house roster, route tables, and ledger files.
        #[arg(long)]
        interchange: PathBuf,
        /// Optional external `seats.tsv` (`session_name\tseat\troom_path`) file to include.
        #[arg(long = "seats-tsv")]
        seats_tsv: Option<PathBuf>,
        /// Preview the computed seed claims without connecting to or mutating the daemon.
        #[arg(long)]
        dry_run: bool,
    },
    /// Export durable presence seat records to a human-editable resident manifest JSON file.
    #[command(name = "export-manifest")]
    ExportManifest {
        /// Destination JSON file path (`-` writes to stdout).
        path: PathBuf,
        #[arg(long)]
        include_retired: bool,
    },
    /// Import durable seat records and resident manifest fields from a manifest JSON file.
    #[command(name = "import-manifest")]
    ImportManifest {
        /// Source resident manifest JSON file path.
        path: PathBuf,
        /// Validate and preview the manifest without connecting to or mutating the daemon.
        #[arg(long)]
        dry_run: bool,
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
    ConflictValue {
        summary: String,
        payload: serde_json::Value,
    },
    Msg(String),
}

impl CliError {
    fn exit_code(&self) -> ExitCode {
        match self {
            CliError::Api(ApiError::Conflict { .. }) | CliError::ConflictValue { .. } => {
                ExitCode::from(EXIT_CONFLICT)
            }
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

fn ensure_cursor_from_head(cursor: Cursor) -> Cursor {
    if cursor.from_head() || cursor.snapshot().is_some() {
        return cursor;
    }
    cursor.with_from_head(true)
}

fn parse_task_status(s: &str) -> Result<TaskStatus, ApiError> {
    rex_wave_api::parse_enum("status", s)
}

fn parse_seat_status(s: &str) -> Result<SeatStatus, ApiError> {
    rex_wave_api::parse_enum("status", s)
}

fn parse_work_class(s: &str) -> Result<WorkClass, ApiError> {
    rex_wave_api::parse_enum("work_class", s)
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
    rex_wave_api::parse_enum("permission", s)
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
    rex_wave_api::parse_enum("mode", s)
}

fn parse_card_priority(s: &str) -> Result<Priority, ApiError> {
    rex_wave_api::parse_enum("priority", s)
}

fn parse_letter_priority(s: &str) -> Result<LetterPriority, ApiError> {
    rex_wave_api::parse_enum("priority", s)
}

fn parse_worker_spec(
    spec_str: &str,
    brief_text: String,
    permission: PermissionMode,
    workspace: Workspace,
    account: Option<String>,
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
            account,
            reasoning: None,
            service_tier: None,
            permission,
            workspace,
        }),
        output_dir: None,
        writes: Vec::new(),
        return_schema: None,
    })
}

fn parse_role_kv<'a>(raw: &'a str, flag: &str) -> Result<(RoleName, &'a str), ApiError> {
    let (role_raw, val) = raw
        .split_once('=')
        .ok_or_else(|| invalid(flag, format!("expected <role>=<value>, got '{raw}'")))?;
    let role = RoleName::parse(role_raw)?;
    Ok((role, val))
}

/// The largest file the CLI reads as command input (bodies, briefs,
/// manifests, shadow and acks logs).
const MAX_CLI_INPUT_BYTES: u64 = 64 * 1024 * 1024;
/// The largest `mail shadow --watch` log resumed.
const MAX_SHADOW_LOG_BYTES: u64 = 256 * 1024 * 1024;

fn read_cli_text(path: &Path) -> std::io::Result<String> {
    rex_wave_fs::read_input_string(path, MAX_CLI_INPUT_BYTES)
}

fn read_text_source(
    inline: Option<&String>,
    file: Option<&PathBuf>,
    what: &str,
) -> Result<String, ApiError> {
    match (inline, file) {
        (Some(s), _) => Ok(s.clone()),
        (None, Some(path)) => read_cli_text(path)
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
/// 2. `<state_dir>/worker-tokens/$REX_WAVE_WORKER_THREAD_ID` when `REX_WAVE_WORKER_THREAD_ID` is set (always fails closed on missing/invalid file).
/// 3. `<state_dir>/worker-tokens/$T3CODE_THREAD_ID` when `T3CODE_THREAD_ID` is set (fails closed for wave worker schemes `t3:`, `acp:`, `claude:`, `codex:`).
/// 4. Unix `Credential::PeerUid`.
pub fn resolve_cli_credential(
    sock_path: &Path,
    explicit_token: Option<&str>,
) -> Result<Credential, String> {
    if let Some(tok) = explicit_token.map(str::trim).filter(|t| !t.is_empty()) {
        return Ok(Credential::Token {
            token: tok.to_owned(),
        });
    }
    if let Some(raw_os) = std::env::var_os(rex_wave_transport::WORKER_THREAD_ENV) {
        let Some(raw_str) = raw_os.to_str() else {
            return Err(format!(
                "{} contains non-UTF-8 bytes; refusing PeerUid fallback",
                rex_wave_transport::WORKER_THREAD_ENV
            ));
        };
        let tid = raw_str.trim();
        if tid.is_empty() {
            return Err(format!(
                "{} is set to an empty value; refusing PeerUid fallback",
                rex_wave_transport::WORKER_THREAD_ENV
            ));
        }
        let state_dir = daemon::state_dir_for(sock_path);
        return match rex_wave_transport::read_worker_token_file(&state_dir, tid) {
            Ok(Some(token)) => Ok(Credential::Token { token }),
            Ok(None) => Err(format!(
                "wave worker token missing or retired for {}={tid}; refusing PeerUid fallback",
                rex_wave_transport::WORKER_THREAD_ENV
            )),
            Err(e) => Err(format!(
                "failed reading worker token for {}={tid}: {e}",
                rex_wave_transport::WORKER_THREAD_ENV
            )),
        };
    }
    if let Some(raw_os) = std::env::var_os(rex_wave_transport::T3_WORKER_THREAD_ENV) {
        let Some(raw_str) = raw_os.to_str() else {
            return Err(format!(
                "{} contains non-UTF-8 bytes; refusing PeerUid fallback",
                rex_wave_transport::T3_WORKER_THREAD_ENV
            ));
        };
        let tid = raw_str.trim();
        if !tid.is_empty() {
            let has_worker_prefix = RunnerKind::from_thread_str(tid) != RunnerKind::Bb;
            let state_dir = daemon::state_dir_for(sock_path);
            match rex_wave_transport::read_worker_token_file(&state_dir, tid) {
                Ok(Some(token)) => return Ok(Credential::Token { token }),
                Ok(None) if has_worker_prefix => {
                    return Err(format!(
                        "T3 wave worker token missing or retired for thread {tid}; refusing PeerUid fallback"
                    ));
                }
                Ok(None) => {}
                Err(e) if !has_worker_prefix && e.kind() == std::io::ErrorKind::InvalidInput => {}
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

fn load_claude_session_endpoint(
    path_arg: &str,
    pid_arg: Option<u32>,
    claude_pid_arg: Option<u32>,
    allow_fork: bool,
) -> Result<TransportEndpoint, ApiError> {
    if let (Some(a), Some(b)) = (pid_arg, claude_pid_arg)
        && a != b
    {
        return Err(invalid(
            "pid",
            format!("--pid ({a}) and --claude-pid ({b}) disagree"),
        ));
    }
    let session = if !path_arg.trim().is_empty() {
        if pid_arg.is_some() || claude_pid_arg.is_some() {
            return Err(invalid(
                "from-claude-session",
                "pass either a session JSON path or --pid/--claude-pid, not both",
            ));
        }
        let path = PathBuf::from(path_arg.trim());
        let raw = read_cli_text(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::InvalidInput {
                ApiError::Denied {
                    principal: "presence".into(),
                    action: format!("read non-regular session file {}: {e}", path.display()),
                }
            } else {
                invalid(
                    "from-claude-session",
                    format!("failed reading {}: {e}", path.display()),
                )
            }
        })?;
        parse_claude_session_json(&raw)?
    } else {
        let resolved_pid = pid_arg
            .or(claude_pid_arg)
            .or_else(|| {
                std::env::var("CLAUDE_PID")
                    .ok()
                    .and_then(|s| s.trim().parse::<u32>().ok())
            })
            .or_else(|| {
                let ppid = std::os::unix::process::parent_id();
                if ppid > 0 {
                    eprintln!(
                        "rex-wave presence bind: no --pid or CLAUDE_PID set; using parent PID {ppid}"
                    );
                    Some(ppid)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                invalid(
                    "from-claude-session",
                    "pass a session JSON path, --pid <pid>, or set CLAUDE_PID",
                )
            })?;
        load_claude_session_file(resolved_pid)?
    };

    session.into_endpoint(allow_fork)
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
                origin: Default::default(),
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
            origin: Default::default(),
        })),
        Cmd::Read {
            room,
            after,
            re,
            labels,
            cursor,
            limit,
        } => {
            let room_ref = parse_room_ref(room)?;
            let parsed_labels = parse_labels(labels)?;
            let parsed_cursor = match cursor {
                Some(c) => Some(parse_cursor_arg(c)?),
                None => None,
            };
            let start_seq = after.map_or(Seq::ORIGIN, Seq);
            if re.is_some() || !parsed_labels.is_empty() {
                let mut wait =
                    Wait::room_ref_filtered(room_ref, me, start_seq, re.map(Seq), parsed_labels)?;
                if after.is_none()
                    && let Some(c) = parsed_cursor
                {
                    wait.after = c.clone();
                    wait.since = None;
                    wait.cursor = Some(c);
                }
                wait.timeout_ms = Some(0);
                wait.limit = Some(limit.unwrap_or(200).clamp(1, 1000));
                Ok(WireCommand::Wait(wait))
            } else {
                Ok(WireCommand::Read(Read::room_ref(
                    room_ref,
                    me,
                    start_seq,
                    if after.is_some() { None } else { parsed_cursor },
                    *limit,
                )?))
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
                    let room_ref = parse_room_ref(r)?;
                    let room_id = room_ref.resolve(me)?;
                    let stream = StreamId::Room {
                        room: room_id.clone(),
                    };
                    let fallback_stream =
                        rex_wave_api::read::legacy_operator_dm_stream(&room_ref, me)?
                            .unwrap_or_else(|| stream.clone());
                    if has_filter {
                        if *since == 0
                            && let Some(c) = base_cursor
                        {
                            Wait {
                                interest: Interest::RoomFiltered {
                                    room: room_ref.clone(),
                                    re: re.map(Seq),
                                    labels: parsed_labels,
                                },
                                after: c.clone(),
                                room: Some(room_ref),
                                since: None,
                                cursor: Some(c),
                                timeout_ms: *timeout,
                                limit: None,
                            }
                        } else {
                            let mut w = Wait::room_ref_filtered(
                                room_ref,
                                me,
                                Seq(*since),
                                re.map(Seq),
                                parsed_labels,
                            )?;
                            if let Some(c) = base_cursor {
                                let mut wire_c = c.clone();
                                wire_c.advance(&fallback_stream, Seq(*since));
                                w.after = wire_c;
                                w.cursor = Some(c);
                            }
                            w.timeout_ms = *timeout;
                            w
                        }
                    } else {
                        let mentions = !*all;
                        let mut w = Wait::room_ref(room_ref, me, Seq(*since), mentions)?;
                        if let Some(c) = base_cursor {
                            let was_unscoped = !c.from_head() && c.snapshot().is_none();
                            let base_c = if mentions {
                                ensure_cursor_from_head(c)
                            } else {
                                c
                            };
                            let mut wire_c = base_c.clone();
                            if *since > 0 {
                                wire_c.advance(&fallback_stream, Seq(*since));
                            } else if was_unscoped {
                                w.since = Some(Seq::ORIGIN);
                                if mentions
                                    && !wire_c.lists(&stream)
                                    && !wire_c.lists(&fallback_stream)
                                {
                                    wire_c.advance(&fallback_stream, Seq::ORIGIN);
                                }
                            } else {
                                w.since = None;
                            }
                            w.after = wire_c;
                            w.cursor = Some(base_c);
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
                    let after = match base_cursor {
                        Some(c) if !*all => ensure_cursor_from_head(c),
                        Some(c) => c,
                        None => Cursor::live(),
                    };
                    Wait {
                        timeout_ms: *timeout,
                        ..Wait::new(
                            if *all {
                                Interest::Everything
                            } else {
                                Interest::Mentions
                            },
                            after,
                        )
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
        Cmd::Config(cfg_cmd) => config_to_command(cfg_cmd),
        Cmd::Daemon(_)
        | Cmd::Tui { .. }
        | Cmd::Mcp { .. }
        | Cmd::Token(_)
        | Cmd::Mail(_)
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
            account,
            output_dir,
            worker_outputs,
            worker_writes,
            worker_schemas,
            permission,
            workspace,
        } => {
            let slug = WaveSlug::parse(slug.clone())?;
            let project = BbProjectId::parse(project.clone())?;
            let goal_raw = read_text_source(goal.as_ref(), goal_file.as_ref(), "goal")?;
            let goal_body = Body::parse(goal_raw.trim_end_matches('\n').to_owned())?;
            let perm = parse_permission(permission.as_deref().unwrap_or("accept-edits"))?;
            let ws = parse_workspace(workspace.as_deref().unwrap_or("managed-worktree"))?;

            let mut parsed_workers = Vec::with_capacity(workers.len());
            for entry in workers {
                let role_part = entry
                    .split_once('=')
                    .map(|(r, _)| r)
                    .unwrap_or(entry.as_str());
                let brief_str = if let Some(dir) = brief_dir {
                    let candidate = dir.join(format!("{role_part}.md"));
                    if candidate.exists() {
                        read_cli_text(&candidate).map_err(|e| {
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
                parsed_workers.push(parse_worker_spec(
                    entry,
                    brief_str,
                    perm,
                    ws.clone(),
                    account.clone(),
                )?);
            }

            for raw in worker_outputs {
                let (role, path_str) = parse_role_kv(raw, "worker-output")?;
                let w = parsed_workers
                    .iter_mut()
                    .find(|w| w.role == role)
                    .ok_or_else(|| {
                        invalid(
                            "worker-output",
                            format!("unknown worker role @{role} (declare with --worker first)"),
                        )
                    })?;
                w.output_dir = Some(PathBuf::from(path_str));
            }
            for raw in worker_writes {
                let (role, path_str) = parse_role_kv(raw, "worker-writes")?;
                let w = parsed_workers
                    .iter_mut()
                    .find(|w| w.role == role)
                    .ok_or_else(|| {
                        invalid(
                            "worker-writes",
                            format!("unknown worker role @{role} (declare with --worker first)"),
                        )
                    })?;
                w.writes.push(PathBuf::from(path_str));
            }
            for raw in worker_schemas {
                let (role, json_str) = parse_role_kv(raw, "worker-schema")?;
                let schema: serde_json::Value = serde_json::from_str(json_str).map_err(|e| {
                    invalid(
                        "worker-schema",
                        format!("invalid JSON schema for @{role}: {e}"),
                    )
                })?;
                let w = parsed_workers
                    .iter_mut()
                    .find(|w| w.role == role)
                    .ok_or_else(|| {
                        invalid(
                            "worker-schema",
                            format!("unknown worker role @{role} (declare with --worker first)"),
                        )
                    })?;
                w.return_schema = Some(schema);
            }

            let default_provider = parsed_workers
                .first()
                .and_then(|w| w.agent.as_ref().map(|a| a.provider.clone()))
                .unwrap_or_else(|| ProviderId::parse("codex").expect("valid provider"));
            let defaults = AgentSpec {
                provider: default_provider,
                model: None,
                account: account.clone(),
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
                    output_dir: output_dir.clone(),
                    workers: parsed_workers,
                },
                idem: None,
                config_defaults: rex_wave_api::wave::WaveConfigDefaults {
                    permission: permission.is_none(),
                    workspace: workspace.is_none(),
                    ..Default::default()
                },
            }))
        }
        WaveCmd::AddWorker {
            wave,
            worker,
            brief_file,
            brief,
            account,
            output_dir,
            writes,
            return_schema,
            permission,
            workspace,
        } => {
            let wave = WaveSlug::parse(wave.clone())?;
            let perm = parse_permission(permission.as_deref().unwrap_or("accept-edits"))?;
            let ws = parse_workspace(workspace.as_deref().unwrap_or("managed-worktree"))?;
            let brief_raw = read_text_source(brief.as_ref(), brief_file.as_ref(), "brief")?;
            let mut spec = parse_worker_spec(worker, brief_raw, perm, ws, account.clone())?;
            spec.output_dir = output_dir.clone();
            spec.writes = writes.clone();
            spec.return_schema =
                match return_schema {
                    Some(raw) => Some(serde_json::from_str(raw).map_err(|e| {
                        invalid("return-schema", format!("invalid JSON schema: {e}"))
                    })?),
                    None => None,
                };
            Ok(WireCommand::WaveAddWorker(WaveAddWorker {
                wave,
                worker: spec,
                config_defaults: rex_wave_api::wave::WaveConfigDefaults {
                    permission: permission.is_none(),
                    workspace: workspace.is_none(),
                    ..Default::default()
                },
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
        WaveCmd::Finish {
            wave,
            summary,
            allow_incomplete,
        } => Ok(WireCommand::WaveFinish(WaveFinish {
            wave: WaveSlug::parse(wave.clone())?,
            outcome: WaveOutcome::Completed {
                summary: Body::parse(summary.clone())?,
            },
            allow_incomplete: *allow_incomplete,
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
                origin: Default::default(),
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
            limit,
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
                    limit: *limit,
                }));
            }

            if limit.is_some() {
                return Err(ApiError::Invalid(InvalidId {
                    what: "--limit".into(),
                    reason: "only supported for room task lists (board views return all cards)"
                        .into(),
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
            limit: None,
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
        TaskCmd::OrgSync { .. } => Err(invalid(
            "tasks org-sync",
            "handled locally via two-way org reconciler",
        )),
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
            mail_horizon,
            work_class,
            avatar,
            home_dir,
            start_cmd,
            resume_cmd,
            rex_target,
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
            mail_horizon_at: mail_horizon.map(UnixMs),
            work_class: match work_class {
                Some(wc) => Some(parse_work_class(wc)?),
                None => None,
            },
            avatar: avatar.clone(),
            home_dir: home_dir.clone(),
            start_cmd: start_cmd.clone(),
            resume_cmd: resume_cmd.clone(),
            rex_target: rex_target.clone(),
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
            mail_horizon,
            idem,
        } => {
            let mut chosen: Vec<(TransportEndpoint, LivenessMode)> = Vec::new();
            if let Some(session_path) = from_claude_session {
                let ep =
                    load_claude_session_endpoint(session_path, *pid, *claude_pid, *allow_fork)?;
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
                mail_horizon_at: mail_horizon.map(UnixMs),
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
        PresenceCmd::SeedInterchange { .. } => Err(invalid(
            "presence seed-interchange",
            "handled locally via client batch command",
        )),
        PresenceCmd::ExportManifest { .. } => Err(invalid(
            "presence export-manifest",
            "handled locally via client export command",
        )),
        PresenceCmd::ImportManifest { .. } => Err(invalid(
            "presence import-manifest",
            "handled locally via client import command",
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
            expected_epoch: None,
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

fn config_to_command(cmd: &ConfigCmd) -> Result<WireCommand, ApiError> {
    match cmd {
        ConfigCmd::Show => Ok(WireCommand::ConfigShow(ConfigShow {})),
        ConfigCmd::Get { key } => Ok(WireCommand::ConfigGet(ConfigGet { key: key.clone() })),
        ConfigCmd::Set { key, value, idem } => Ok(WireCommand::ConfigSet(ConfigSet {
            key: key.clone(),
            value: Some(value.clone()),
            idem: idem.as_deref().map(IdemKey::parse).transpose()?,
        })),
        ConfigCmd::Unset { key, idem } => Ok(WireCommand::ConfigSet(ConfigSet {
            key: key.clone(),
            value: None,
            idem: idem.as_deref().map(IdemKey::parse).transpose()?,
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
        "config" => Ok((
            Interest::Streams {
                streams: vec![StreamId::Config],
            },
            Cursor::default().with(StreamId::Config, Seq(since)),
        )),
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
            if list.truncated {
                println!(
                    "[showing {} of {} — raise --limit (max 1000) to see more]",
                    list.tasks.len(),
                    list.total_count
                );
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
        WireReply::ConfigShow(snap)
        | WireReply::ConfigSet(snap)
        | WireReply::ConfigSyncFile(snap) => {
            let path = snap
                .config_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "(in-memory)".to_owned());
            let eff = snap
                .effective_default_runner
                .map(|r| r.as_str())
                .unwrap_or("auto");
            println!(
                "config (head={}, file={}, effective_default={}):",
                snap.head.0, path, eff
            );
            if !snap.runners_health.is_empty() {
                let parts: Vec<String> = snap
                    .runners_health
                    .iter()
                    .map(|rh| {
                        let st = if rh.configured && rh.healthy {
                            "ok".to_owned()
                        } else if rh.configured {
                            rh.detail.clone().unwrap_or_else(|| "degraded".to_owned())
                        } else {
                            rh.detail.clone().unwrap_or_else(|| "off".to_owned())
                        };
                        format!("{}={st}", rh.runner.display_name())
                    })
                    .collect();
                println!("  runners: {}", parts.join("  "));
            }
            if !snap.couriers_health.is_empty() {
                let parts: Vec<String> = snap
                    .couriers_health
                    .iter()
                    .map(|c| {
                        format!(
                            "{}({}/{}/{}{})",
                            c.name,
                            c.rung,
                            c.unknown,
                            c.refused,
                            if c.degraded { ",degraded" } else { "" }
                        )
                    })
                    .collect();
                println!("  couriers: {}", parts.join("  "));
            }
            if let Some(ref perr) = snap.persist_error {
                println!("  persist_error: {perr}");
            }
            for spec in SETTINGS_FIELDS {
                let val = snap.config.get_key(spec.key).ok().flatten();
                println!("  {} = {}", spec.key, val.as_deref().unwrap_or("(default)"));
            }
        }
        WireReply::ConfigGet(v) => {
            if let Some(ref val) = v.value {
                println!("{val}");
            } else {
                println!("(default)");
            }
        }
        WireReply::WaveStart(w) => {
            println!(
                "started wave {} (room={:?}, board={})",
                w.wave, w.room, w.board
            )
        }
        WireReply::WaveView(snap) => {
            let c = &snap.counts;
            println!(
                "wave {} ({:?}) project={} [total={} working={} idle={} parked={} blocked={} refused={} errored={} failed={} retiring={} retired(clean={}, refused={}, errored={})]:",
                snap.spec.slug,
                snap.status,
                snap.spec.project,
                c.total,
                c.working,
                c.idle,
                c.parked,
                c.blocked,
                c.refused,
                c.errored,
                c.failed,
                c.retiring,
                c.retired_clean,
                c.retired_refused,
                c.retired_errored,
            );
            for (role, spec, phase) in &snap.workers {
                let phase_str = match phase {
                    WorkerPhase::Requested { attempt } => format!("requested(attempt={attempt})"),
                    WorkerPhase::Live { thread, activity } => match activity {
                        rex_wave_api::wave::Activity::Refused { reason } => {
                            format!("live({thread}, ⊘ refused: {reason})")
                        }
                        rex_wave_api::wave::Activity::Errored { error } => {
                            format!("live({thread}, ✕ errored: {error})")
                        }
                        other => format!("live({thread}, {other:?})"),
                    },
                    WorkerPhase::Retiring {
                        thread,
                        errored,
                        refused,
                    } => {
                        if let Some(reason) = refused {
                            format!("retiring({thread}, ⊘ refused: {reason})")
                        } else if let Some(err) = errored {
                            format!("retiring({thread}, ✕ errored: {err})")
                        } else {
                            format!("retiring({thread})")
                        }
                    }
                    WorkerPhase::Retired {
                        errored, refused, ..
                    } => {
                        if let Some(reason) = refused {
                            format!("retired(⊘ refused: {reason})")
                        } else if let Some(err) = errored {
                            format!("retired(✕ errored: {err})")
                        } else {
                            "retired(✓ clean)".to_owned()
                        }
                    }
                    WorkerPhase::Failed { error } => format!("failed(✕ {error})"),
                };
                let meta = snap.worker_meta.get(role);
                let req_model = meta
                    .and_then(|m| m.requested_model.as_ref())
                    .or(spec.model.as_ref())
                    .map(ModelId::as_str);
                let act_model = meta
                    .and_then(|m| m.actual_model.as_ref())
                    .map(ModelId::as_str);
                let model_part = match (req_model, act_model) {
                    (Some(r), Some(a)) if r != a => format!("/{r}->{a}"),
                    (Some(r), _) => format!("/{r}"),
                    (None, Some(a)) => format!("/actual={a}"),
                    (None, None) => String::new(),
                };
                let req_acct = meta
                    .and_then(|m| m.requested_account.as_deref())
                    .or(spec.account.as_deref());
                let act_acct = meta.and_then(|m| m.actual_account.as_deref());
                let acct_part =
                    if meta.is_some_and(rex_wave_api::wave::WorkerRuntimeMeta::account_mismatch) {
                        format!(
                            " acct={}->{} [ACCOUNT MISMATCH]",
                            req_acct.unwrap_or("?"),
                            act_acct.unwrap_or("?")
                        )
                    } else {
                        match (req_acct, act_acct) {
                            (Some(r), Some(_)) => format!(" acct={r}"),
                            (Some(r), None) => format!(" acct={r} (unconfirmed)"),
                            (None, Some(a)) => format!(" acct=actual:{a}"),
                            (None, None) => String::new(),
                        }
                    };
                println!(
                    "  - {role} [{}{model_part}{acct_part}] -> {phase_str}",
                    spec.provider
                );
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
            if page.truncated {
                println!(
                    "[showing {} of {} — raise --limit (max 1000) to see more]",
                    page.items.len(),
                    page.total_count
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
                if page.truncated {
                    let last_seq = page.entries.last().map(|e| e.seq.0).unwrap_or(0);
                    println!(
                        "[showing {} of {} — use --after {} / --cursor / --limit to page]",
                        page.entries.len(),
                        page.total_count,
                        last_seq
                    );
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
        CliError::ConflictValue { payload, .. } if as_json => {
            eprintln!(
                "{}",
                serde_json::to_string_pretty(payload).expect("conflict error json")
            );
        }
        CliError::ConflictValue { summary, .. } => {
            eprintln!("error: conflict: {summary}");
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

fn org_conflict_to_json(c: &OrgSyncConflict) -> serde_json::Value {
    let kind = match &c.kind {
        OrgConflictKind::RevisionConflict { expected, actual } => serde_json::json!({
            "kind": "revision_conflict",
            "expected": expected,
            "actual": actual,
        }),
        OrgConflictKind::LeaseFenced {
            expected,
            actual,
            holder,
        } => serde_json::json!({
            "kind": "lease_fenced",
            "expected": expected.0,
            "actual": actual.0,
            "holder": holder.as_ref().map(ToString::to_string),
        }),
        OrgConflictKind::UnmetNeeds { unmet } => serde_json::json!({
            "kind": "unmet_needs",
            "unmet": unmet.iter().map(|s| s.0).collect::<Vec<_>>(),
        }),
        OrgConflictKind::UnresolvedDependency { missing } => serde_json::json!({
            "kind": "unresolved_dependency",
            "missing": missing.as_str(),
        }),
        OrgConflictKind::DependencyCycle => serde_json::json!({
            "kind": "dependency_cycle",
        }),
        OrgConflictKind::Other => serde_json::json!({
            "kind": "other",
        }),
    };
    serde_json::json!({
        "path": c.path.to_string_lossy(),
        "org_id": c.org_id.as_str(),
        "seq": c.seq.map(|s| s.0),
        "kind": kind,
        "detail": c.detail,
    })
}

enum OrgSyncStep {
    OrgToRoom(OrgToRoomOutcome),
    RoomToOrg(RoomToOrgOutcome),
}

async fn run_org_sync_step<C: Client>(
    client: &C,
    room: &rex_wave_api::room::RoomRef,
    file: &Path,
    state: &mut OrgSyncState,
    options: &OrgSyncOptions,
) -> Result<OrgSyncStep, CliError> {
    let use_org_to_room = if !file.exists() {
        false
    } else if let Some(ref synced) = state.last_synced_bytes {
        rex_wave_board::task_org::read_org_file(file)
            .map(|cur| cur != *synced)
            .unwrap_or(true)
    } else {
        true
    };
    if use_org_to_room {
        let outcome = sync_org_to_room(client, room, file, state, options)
            .await
            .map_err(|e| CliError::Msg(e.to_string()))?;
        Ok(OrgSyncStep::OrgToRoom(outcome))
    } else {
        let outcome = sync_room_to_org(client, room, file, state)
            .await
            .map_err(|e| CliError::Msg(e.to_string()))?;
        Ok(OrgSyncStep::RoomToOrg(outcome))
    }
}

fn format_org_sync_step(step: &OrgSyncStep) -> (serde_json::Value, String, bool) {
    match step {
        OrgSyncStep::OrgToRoom(OrgToRoomOutcome::IgnoredEphemeral)
        | OrgSyncStep::RoomToOrg(RoomToOrgOutcome::IgnoredEphemeral) => (
            serde_json::json!({ "outcome": "ignored_ephemeral" }),
            "org-sync: ignored Emacs lock or autosave path".into(),
            false,
        ),
        OrgSyncStep::OrgToRoom(OrgToRoomOutcome::DeferredLock { lock_path })
        | OrgSyncStep::RoomToOrg(RoomToOrgOutcome::DeferredLock { lock_path }) => (
            serde_json::json!({
                "outcome": "deferred_lock",
                "lock_path": lock_path.to_string_lossy(),
            }),
            format!(
                "org-sync: deferred while Emacs lock {} exists",
                lock_path.display()
            ),
            false,
        ),
        OrgSyncStep::RoomToOrg(RoomToOrgOutcome::DeferredLocalEdits) => (
            serde_json::json!({ "outcome": "deferred_local_edits" }),
            "org-sync: deferred write-back due to local edits".into(),
            false,
        ),
        OrgSyncStep::RoomToOrg(RoomToOrgOutcome::Unchanged) => (
            serde_json::json!({ "outcome": "unchanged" }),
            "org-sync: unchanged".into(),
            false,
        ),
        OrgSyncStep::RoomToOrg(RoomToOrgOutcome::Written { tasks_synced }) => (
            serde_json::json!({
                "outcome": "written",
                "tasks_synced": tasks_synced,
            }),
            format!("org-sync: wrote {tasks_synced} task(s) from room to org"),
            false,
        ),
        OrgSyncStep::OrgToRoom(OrgToRoomOutcome::Synced {
            created,
            updated,
            transitioned,
            labeled,
            wrote_back,
        }) => (
            serde_json::json!({
                "outcome": "synced",
                "created": created.iter().map(|id| id.as_str()).collect::<Vec<_>>(),
                "updated": updated.iter().map(|id| id.as_str()).collect::<Vec<_>>(),
                "transitioned": transitioned.iter().map(|id| id.as_str()).collect::<Vec<_>>(),
                "labeled": labeled.iter().map(|id| id.as_str()).collect::<Vec<_>>(),
                "wrote_back": wrote_back,
            }),
            format!(
                "org-sync: created={} updated={} transitioned={} labeled={} wrote_back={}",
                created.len(),
                updated.len(),
                transitioned.len(),
                labeled.len(),
                wrote_back
            ),
            false,
        ),
        OrgSyncStep::OrgToRoom(OrgToRoomOutcome::Conflict {
            applied,
            conflicts,
            sidecar_path,
        }) => {
            let details: Vec<String> = conflicts
                .iter()
                .map(|c| format!("{} ({})", c.org_id, c.detail))
                .collect();
            let payload = serde_json::json!({
                "outcome": "conflict",
                "applied": applied.iter().map(|id| id.as_str()).collect::<Vec<_>>(),
                "conflicts": conflicts.iter().map(org_conflict_to_json).collect::<Vec<_>>(),
                "sidecar_path": sidecar_path.as_ref().map(|p| p.to_string_lossy().into_owned()),
            });
            (payload, details.join("; "), true)
        }
    }
}

async fn execute_tasks_org_sync<C: Client>(
    client: &C,
    room: &str,
    file: &Path,
    watch: bool,
    interval_ms: u64,
    conflict_sidecar: bool,
    as_json: bool,
) -> Result<ExitCode, CliError> {
    let room_ref = parse_room_ref(room)?;
    let options = OrgSyncOptions {
        write_conflict_sidecar: conflict_sidecar,
        notify_principal: None,
    };
    let mut state = OrgSyncState::default();

    if !watch {
        let step = run_org_sync_step(client, &room_ref, file, &mut state, &options).await?;
        let (payload, summary, is_conflict) = format_org_sync_step(&step);
        if is_conflict {
            return Err(CliError::ConflictValue { summary, payload });
        }
        if as_json {
            println!(
                "{}",
                serde_json::to_string_pretty(&payload).expect("org sync json")
            );
        } else {
            println!("{summary}");
        }
        return Ok(ExitCode::SUCCESS);
    }

    const MAX_WATCH_CONSECUTIVE_ERRORS: u32 = 20;
    let sleep_dur = Duration::from_millis(interval_ms.max(50));
    let mut consecutive_errors: u32 = 0;
    let mut last_conflict_bytes: Option<Vec<u8>> = None;
    loop {
        let skip_same_conflict = if let Some(ref prev_conflict) = last_conflict_bytes {
            match rex_wave_board::task_org::read_org_file(file) {
                Ok(cur) if cur == *prev_conflict => true,
                Ok(_) => {
                    last_conflict_bytes = None;
                    false
                }
                Err(_) => false,
            }
        } else {
            false
        };

        if !skip_same_conflict {
            match run_org_sync_step(client, &room_ref, file, &mut state, &options).await {
                Ok(step) => {
                    consecutive_errors = 0;
                    let (payload, summary, is_conflict) = format_org_sync_step(&step);
                    if is_conflict {
                        last_conflict_bytes = rex_wave_board::task_org::read_org_file(file).ok();
                        if as_json {
                            eprintln!(
                                "{}",
                                serde_json::to_string(&payload).expect("conflict json")
                            );
                        } else {
                            eprintln!("error: conflict: {summary}");
                        }
                    } else {
                        last_conflict_bytes = None;
                        if !matches!(step, OrgSyncStep::RoomToOrg(RoomToOrgOutcome::Unchanged)) {
                            if as_json {
                                println!(
                                    "{}",
                                    serde_json::to_string(&payload).expect("org sync json")
                                );
                            } else {
                                println!("{summary}");
                            }
                        }
                    }
                }
                Err(err) => {
                    consecutive_errors += 1;
                    if !as_json {
                        eprintln!(
                            "rex-wave tasks org-sync: watch error ({consecutive_errors}/{MAX_WATCH_CONSECUTIVE_ERRORS})"
                        );
                    }
                    print_error(&err, as_json);
                    if consecutive_errors >= MAX_WATCH_CONSECUTIVE_ERRORS {
                        return Err(err);
                    }
                }
            }
        }
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(ExitCode::SUCCESS),
            () = tokio::time::sleep(sleep_dur) => {}
        }
    }
}

fn format_outside_shadow_err(
    out: &Path,
    interchange_root: &Path,
    err: rex_wave_fs::OutsideError,
) -> CliError {
    use rex_wave_fs::OutsideError;
    CliError::Msg(match err {
        OutsideError::Inside(_, ref root) => format!(
            "refusing to write shadow output {} inside read-only interchange {}",
            out.display(),
            if root.as_os_str().is_empty() {
                interchange_root.display()
            } else {
                root.display()
            }
        ),
        OutsideError::Symlink(_) => format!(
            "refusing to write shadow output through symlink {}",
            out.display()
        ),
        OutsideError::ParentDir(_) => format!(
            "refusing shadow output {} containing '..' parent traversal",
            out.display()
        ),
        other => format!("shadow output {}: {other}", out.display()),
    })
}

/// Read-only verification that shadow output `out` resolves outside
/// `interchange_root` without creating or mutating any filesystem entries.
fn verify_shadow_out_target(out: &Path, interchange_root: &Path) -> Result<PathBuf, CliError> {
    rex_wave_fs::OutsidePath::verify(out, interchange_root)
        .map(|p| p.path().to_path_buf())
        .map_err(|e| format_outside_shadow_err(out, interchange_root, e))
}

fn write_file_atomic_outside(
    target: &Path,
    interchange_root: &Path,
    bytes: &[u8],
) -> Result<(), CliError> {
    rex_wave_fs::write_outside_atomic_confined(target, interchange_root, bytes, 0o644)
        .map(|_| ())
        .map_err(|e| format_outside_shadow_err(target, interchange_root, e))
}

struct ShadowWatchLogFile {
    out_arg: PathBuf,
    file: std::fs::File,
    id: rex_wave_fs::FileId,
}

/// Appends `bytes` to the retained `--watch` shadow log handle after
/// read-only verification that `out_arg` still resolves outside
/// `interchange_root` to the exact same regular file `(dev, ino)` opened at
/// startup.
fn append_shadow_delta(
    log: &mut ShadowWatchLogFile,
    interchange_root: &Path,
    bytes: &[u8],
) -> Result<(), CliError> {
    assert!(!bytes.is_empty(), "callers skip empty deltas");
    let cur_target = verify_shadow_out_target(&log.out_arg, interchange_root)?;
    rex_wave_fs::verify_same_regular_nofollow(&cur_target, log.id).map_err(|e| {
        CliError::Msg(format!(
            "refusing to append shadow output to {}: {e}",
            cur_target.display()
        ))
    })?;
    log.file
        .write_all(bytes)
        .and_then(|()| log.file.sync_data())
        .map_err(|e| CliError::Msg(format!("append {}: {e}", cur_target.display())))
}

/// Opens and retains the `--watch` append log handle (`O_NOFOLLOW`), hydrating
/// a [`ShadowWatchTracker`] from any existing bytes on that exact handle so a
/// restarted watcher appends only new transitions.
fn open_shadow_watch_log(
    out: &Path,
    interchange_root: &Path,
) -> Result<(ShadowWatchLogFile, ShadowWatchTracker, bool), CliError> {
    let (final_target, file, meta) =
        rex_wave_fs::open_outside_rw_append_confined(out, interchange_root, 0o644).map_err(
            |e| match e {
                rex_wave_fs::OutsideError::Inside(_, ref root) => CliError::Msg(format!(
                    "refusing shadow output {} inside read-only interchange {}",
                    out.display(),
                    root.display()
                )),
                rex_wave_fs::OutsideError::Symlink(_) => CliError::Msg(format!(
                    "refusing to write shadow output through symlink {}",
                    out.display()
                )),
                rex_wave_fs::OutsideError::ParentDir(_) => CliError::Msg(format!(
                    "refusing shadow output {} containing '..' parent traversal",
                    out.display()
                )),
                other => CliError::Msg(format!(
                    "refusing shadow --watch output {}: {other}",
                    out.display()
                )),
            },
        )?;
    let id = rex_wave_fs::file_id(&meta);
    let reader_copy = file.try_clone().map_err(|e| {
        CliError::Msg(format!(
            "clone shadow --watch output handle {}: {e}",
            final_target.display()
        ))
    })?;
    let raw_bytes =
        rex_wave_fs::read_open_bounded(&final_target, reader_copy, &meta, MAX_SHADOW_LOG_BYTES)
            .map_err(|e| {
                CliError::Msg(format!(
                    "refusing shadow --watch output {}: {e}",
                    final_target.display()
                ))
            })?;
    let existing = String::from_utf8(raw_bytes).map_err(|_| {
        CliError::Msg(format!(
            "refusing shadow --watch output {}: not UTF-8",
            final_target.display()
        ))
    })?;
    let tracker = ShadowWatchTracker::from_existing_jsonl(&existing).map_err(|e| {
        CliError::Msg(format!(
            "cannot resume shadow --watch log {}: {e}; move it aside or repair it",
            final_target.display()
        ))
    })?;
    let needs_newline = !existing.is_empty() && !existing.ends_with('\n');
    Ok((
        ShadowWatchLogFile {
            out_arg: out.to_path_buf(),
            file,
            id,
        },
        tracker,
        needs_newline,
    ))
}

async fn load_live_presence_state(
    sock_path: &Path,
    explicit_token: Option<&str>,
    offline: bool,
) -> Result<(PresenceState, bool), CliError> {
    let house = rex_wave_mailbridge::load_house_profile()?;
    if offline {
        return Ok((PresenceState::with_house_profile(house), false));
    }
    let cred = resolve_cli_credential(sock_path, explicit_token)?;
    let client = match RemoteClient::connect(
        Endpoint::Unix(sock_path.to_path_buf()),
        cred,
        "mail-shadow",
    )
    .await
    {
        Ok(c) => c,
        Err(ApiError::Transport(msg)) => {
            return Err(CliError::Msg(format!(
                "mail shadow: daemon socket {} unreachable ({msg}); start the daemon or pass --offline for empty presence",
                sock_path.display()
            )));
        }
        Err(e) => return Err(CliError::Api(e)),
    };
    let mut state = PresenceState::with_house_profile(house);
    let mut after = Seq::ORIGIN;
    loop {
        let page = client
            .call(Read::stream(StreamId::Presence, after, Some(1000)))
            .await?;
        if page.entries.is_empty() {
            break;
        }
        for env in page.entries {
            after = env.seq;
            if let Event::Presence(ev) = &env.event {
                let meta = EntryMeta {
                    seq: env.seq,
                    at: env.at,
                    by: env.by.clone(),
                };
                Presence::evolve(&mut state, ev, &meta);
            }
        }
    }
    Ok((state, true))
}

#[allow(clippy::too_many_arguments)]
async fn execute_mail_shadow(
    interchange: &Path,
    out: &Path,
    watch: bool,
    interval_ms: u64,
    offline: bool,
    sock_path: &Path,
    explicit_token: Option<&str>,
    as_json: bool,
) -> Result<ExitCode, CliError> {
    let ro = ReadOnlyInterchange::open(interchange)
        .map_err(|e| CliError::Msg(format!("open read-only interchange: {e}")))?;
    if out != Path::new("-") {
        verify_shadow_out_target(out, ro.root())?;
    }
    let policy = RingPolicy::default();
    let affinity = IncarnationAffinity::new();
    let retry_state = RetryState::new();
    let sleep_dur = Duration::from_millis(interval_ms.max(100));
    let to_stdout = out == Path::new("-");
    // `--watch` appends transitions (a fresh tracker for stdout); `--once`
    // keeps the atomic snapshot overwrite.
    let mut watch_log: Option<(Option<ShadowWatchLogFile>, ShadowWatchTracker, bool)> =
        match (watch, to_stdout) {
            (false, _) => None,
            (true, true) => Some((None, ShadowWatchTracker::new(), false)),
            (true, false) => {
                let (log_file, tracker, needs_newline) = open_shadow_watch_log(out, ro.root())?;
                Some((Some(log_file), tracker, needs_newline))
            }
        };

    loop {
        let (presence, live_presence) =
            load_live_presence_state(sock_path, explicit_token, offline).await?;
        let aliases = AliasMap::from_presence(&presence);
        let now = rex_wave_runtime::now();
        let report = run_shadow_pass(
            &ro,
            &presence,
            &affinity,
            &retry_state,
            &policy,
            &aliases,
            now,
        )
        .map_err(|e| CliError::Msg(format!("shadow pass: {e}")))?;
        let mut appended_decisions: Option<usize> = None;
        if let Some((log_file, tracker, needs_newline)) = watch_log.as_mut() {
            let delta = tracker
                .observe_pass(&report.decisions, &aliases, now)
                .map_err(|e| CliError::Msg(format!("shadow watch: {e}")))?;
            appended_decisions = Some(delta.decision_lines);
            match log_file {
                None => {
                    print!("{}", delta.jsonl);
                    let _ = std::io::stdout().flush();
                }
                Some(log_file) if !delta.jsonl.is_empty() => {
                    let mut bytes = Vec::with_capacity(delta.jsonl.len() + 1);
                    if *needs_newline {
                        bytes.push(b'\n');
                    }
                    bytes.extend_from_slice(delta.jsonl.as_bytes());
                    append_shadow_delta(log_file, ro.root(), &bytes)?;
                    *needs_newline = false;
                }
                Some(_) => {}
            }
        } else if to_stdout {
            print!("{}", report.to_jsonl_with_aliases(&aliases));
            let _ = std::io::stdout().flush();
        } else {
            let jsonl = report.to_jsonl_with_aliases(&aliases);
            write_file_atomic_outside(out, ro.root(), jsonl.as_bytes())?;
        }
        if !to_stdout {
            if as_json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "interchange": ro.root().to_string_lossy(),
                        "out": out.to_string_lossy(),
                        "live_presence": live_presence,
                        "letters_scanned": report.letters_scanned,
                        "acks_records": report.acks_report.records.len(),
                        "acks_skipped_lines": report.acks_report.skipped_lines.len(),
                        "decisions": report.decisions.len(),
                        "appended_decisions": appended_decisions,
                    }))
                    .expect("shadow summary json")
                );
            } else {
                let appended = appended_decisions
                    .map(|n| format!(" appended_decisions={n}"))
                    .unwrap_or_default();
                println!(
                    "mail shadow: scanned={} acks={} skipped_ack_lines={} decisions={}{} live_presence={} out={}",
                    report.letters_scanned,
                    report.acks_report.records.len(),
                    report.acks_report.skipped_lines.len(),
                    report.decisions.len(),
                    appended,
                    live_presence,
                    out.display()
                );
            }
        }
        if !watch {
            return Ok(ExitCode::SUCCESS);
        }
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(ExitCode::SUCCESS),
            () = tokio::time::sleep(sleep_dur) => {}
        }
    }
}

fn execute_mail_score(
    shadow_path: &Path,
    acks_path: &Path,
    fixtures_path: Option<&Path>,
    mailwatch_seen_paths: &[PathBuf],
    as_json: bool,
) -> Result<ExitCode, CliError> {
    let shadow_text = read_cli_text(shadow_path)
        .map_err(|e| CliError::Msg(format!("read shadow {}: {e}", shadow_path.display())))?;
    let acks_text = read_cli_text(acks_path)
        .map_err(|e| CliError::Msg(format!("read acks {}: {e}", acks_path.display())))?;
    let aliases = extract_shadow_aliases(&shadow_text).map_err(CliError::Msg)?;
    let acks_report = parse_acks_jsonl(&acks_text, &aliases);
    let ack_index = acks_report.index();
    let score = score_shadow_jsonl(&shadow_text, &ack_index, &aliases).map_err(CliError::Msg)?;

    let fixtures = match fixtures_path {
        Some(path) => {
            let house = load_house_profile()
                .map_err(|e| CliError::Msg(format!("load house profile: {e}")))?;
            let raw = read_cli_text(path)
                .map_err(|e| CliError::Msg(format!("read fixtures {}: {e}", path.display())))?;
            Some(check_gate1_fixtures_with_profile(&raw, &house).map_err(CliError::Msg)?)
        }
        None => None,
    };

    let mailwatch_seen = if mailwatch_seen_paths.is_empty() {
        None
    } else {
        let mut texts = Vec::with_capacity(mailwatch_seen_paths.len());
        for path in mailwatch_seen_paths {
            let raw = read_cli_text(path).map_err(|e| {
                CliError::Msg(format!("read mailwatch seen {}: {e}", path.display()))
            })?;
            texts.push((path.display().to_string(), raw));
        }
        let sources: Vec<(&str, &str)> = texts
            .iter()
            .map(|(label, raw)| (label.as_str(), raw.as_str()))
            .collect();
        let seen_idx = parse_mailwatch_seen_sources(&sources, &aliases).map_err(CliError::Msg)?;
        Some(
            compare_shadow_with_mailwatch_seen(&shadow_text, &seen_idx, &aliases)
                .map_err(CliError::Msg)?,
        )
    };

    if as_json {
        let mut root = serde_json::json!({
            "shadow_score": score.to_json_value(),
            "acks": {
                "total_lines": acks_report.total_lines,
                "blank_lines": acks_report.blank_lines,
                "non_ack_rows": acks_report.non_ack_rows,
                "records_parsed": acks_report.records.len(),
                "skipped_lines": acks_report.skipped_lines.len(),
            },
        });
        if let Some(ref fx) = fixtures
            && let Some(obj) = root.as_object_mut()
        {
            obj.insert("fixtures".to_owned(), fx.to_json_value());
        }
        if let Some(ref mw) = mailwatch_seen
            && let Some(obj) = root.as_object_mut()
        {
            obj.insert("mailwatch_seen".to_owned(), mw.to_json_value());
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&root).expect("score json")
        );
    } else {
        println!(
            "mail score: decisions={} rings={} referents_rung={} confirmed_acked={} pending_ack={} skipped_acked={} receipt_only={} dormant_or_retired={} pre_horizon={} operator_surface={} non_seat_target={} mutated_after_ack={} deferred={} unrung_alerts={} unrung_caught={} unrung_already_acked={} missing_referent={}",
            score.total_decisions,
            score.rings_emitted,
            score.referents_rung,
            score.ring_referents_confirmed_acked,
            score.ring_referents_pending_ack,
            score.skipped_already_acked,
            score.skipped_receipt_only,
            score.skipped_dormant_or_retired,
            score.skipped_pre_horizon,
            score.skipped_operator_surface,
            score.skipped_non_seat_target,
            score.mutated_after_ack_flags,
            score.deferred,
            score.unrung_alerts,
            score.unrung_referents_caught,
            score.unrung_referents_already_acked,
            score.missing_referent_refusals,
        );
        println!(
            "acks: lines={} blank={} non_ack={} records={} skipped={}",
            acks_report.total_lines,
            acks_report.blank_lines,
            acks_report.non_ack_rows,
            acks_report.records.len(),
            acks_report.skipped_lines.len(),
        );
        if let Some(ref mw) = mailwatch_seen {
            println!(
                "mailwatch seen: entries={} shadow_ring_referents={} agreed_rung={} own_knocker={} deferred_or_unrung={} other={} absent={}",
                mw.seen_entries,
                mw.shadow_ring_referents,
                mw.agreed_rung,
                mw.own_knocker_in_mailwatch,
                mw.deferred_or_unrung_in_mailwatch,
                mw.other_in_mailwatch,
                mw.absent_in_mailwatch,
            );
        }
        if let Some(ref fx) = fixtures {
            println!(
                "fixtures: {}/{} Gate 1 cases registered (fixture_ids_registered={}, missing={}, unknown={})",
                fx.covered_cases,
                fx.total_cases,
                fx.fixture_ids_registered(),
                fx.missing_case_ids.len(),
                fx.unknown_case_ids.len(),
            );
        }
    }
    if fixtures
        .as_ref()
        .is_some_and(|fx| !fx.fixture_ids_registered())
    {
        return Ok(ExitCode::from(1));
    }
    Ok(ExitCode::SUCCESS)
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
            limit: None,
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

#[allow(clippy::too_many_arguments)]
async fn execute_presence_hold(
    client: &RemoteClient,
    seat: Option<&String>,
    lane: Option<&String>,
    incarnation: Option<&String>,
    takeover: bool,
    mail_horizon: Option<u64>,
    client_name: &str,
    as_json: bool,
) -> Result<ExitCode, CliError> {
    let seat_name = resolve_seat_arg(seat, client.me())?;
    let mut binding = client
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
            mail_horizon_at: mail_horizon.map(UnixMs),
            idem: None,
        })
        .await?;
    print_reply(&WireReply::PresenceBind(binding.clone()), as_json);
    let _ = std::io::stdout().flush();

    let mut conn_rx = client.watch_connection();
    let _ = conn_rx.borrow_and_update();
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let _ = client
                    .call(PresenceUnbind {
                        seat: seat_name,
                        incarnation_id: binding.incarnation_id,
                        expected_epoch: Some(binding.epoch),
                        reason: Some("hold interrupted".into()),
                    })
                    .await;
                return Ok(ExitCode::SUCCESS);
            }
            changed = conn_rx.changed() => match changed {
                Ok(()) => {
                    if *conn_rx.borrow_and_update() {
                        binding = client
                            .call(PresenceBind {
                                seat: seat_name.clone(),
                                incarnation_id: Some(binding.incarnation_id.clone()),
                                lane: lane.cloned(),
                                endpoint: TransportEndpoint::UdsSession {
                                    conn_id: 0,
                                    client: client_name.to_owned(),
                                },
                                liveness: LivenessMode::SocketLifetime,
                                ttl_ms: None,
                                takeover: false,
                                allow_fork: false,
                                mail_horizon_at: None,
                                idem: None,
                            })
                            .await?;
                        print_reply(&WireReply::PresenceBind(binding.clone()), as_json);
                        let _ = std::io::stdout().flush();
                    }
                }
                Err(_) => {
                    return Err(CliError::Msg(
                        "presence hold: daemon connection closed permanently".into(),
                    ));
                }
            }
        }
    }
}

fn write_file_atomic_plain(target: &Path, bytes: &[u8]) -> Result<(), CliError> {
    if let Some(parent) = target.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .map_err(|e| CliError::Msg(format!("create {}: {e}", parent.display())))?;
    }
    rex_wave_fs::write_atomic_path(target, bytes, 0o644, false)
        .map_err(|e| CliError::Msg(format!("write {}: {e}", target.display())))
}

async fn execute_presence_seed_interchange(
    interchange: &Path,
    seats_tsv: Option<&Path>,
    dry_run: bool,
    endpoint: Endpoint,
    sock_path: &Path,
    explicit_token: Option<&str>,
    as_json: bool,
) -> Result<ExitCode, CliError> {
    let house = rex_wave_mailbridge::load_house_profile()?;
    let plan = seed_records_from_interchange_with_options(interchange, seats_tsv, &house)?;
    if dry_run {
        if as_json {
            let mut val = plan.to_json_value();
            if let Some(obj) = val.as_object_mut() {
                obj.insert("dry_run".to_owned(), serde_json::Value::Bool(true));
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&val).expect("seed plan json")
            );
        } else {
            println!(
                "presence seed-interchange (dry-run): {} seats from {} source(s), skipped={:?}",
                plan.claims.len(),
                plan.sources_read.len(),
                plan.skipped_targets
            );
            for c in &plan.claims {
                let aliases: Vec<&str> = c.aliases.iter().map(SeatName::as_str).collect();
                println!(
                    "  @{} aliases=[{}] verified_how={:?}",
                    c.seat,
                    aliases.join(", "),
                    c.verified_how
                );
            }
        }
        return Ok(ExitCode::SUCCESS);
    }

    let cred = resolve_cli_credential(sock_path, explicit_token)?;
    let client = RemoteClient::connect(endpoint, cred, "presence-seed").await?;
    if !client.me().is_operator() {
        return Err(CliError::Api(ApiError::Denied {
            principal: client.me().to_string(),
            action: "presence seed-interchange".to_owned(),
        }));
    }
    let existing = client
        .call(PresenceList {
            include_retired: true,
            ..PresenceList::default()
        })
        .await?;
    let existing_by_seat: std::collections::BTreeMap<SeatName, rex_wave_api::presence::SeatRecord> =
        existing
            .seats
            .into_iter()
            .map(|v| (v.record.seat.clone(), v.record))
            .collect();
    let mut existing_alias_owners: std::collections::BTreeMap<SeatName, SeatName> =
        std::collections::BTreeMap::new();
    for (seat, rec) in &existing_by_seat {
        existing_alias_owners.insert(seat.clone(), seat.clone());
        for alias in &rec.aliases {
            existing_alias_owners.insert(alias.value.clone(), seat.clone());
        }
    }

    let mut applied = Vec::new();
    let mut skipped_retired = Vec::new();
    let mut skipped_conflicts = Vec::new();
    for mut claim in plan.claims {
        if let Some(prev) = existing_by_seat.get(&claim.seat) {
            if prev.status == SeatStatus::Retired {
                skipped_retired.push(claim.seat.as_str().to_owned());
                continue;
            }
            // Preserve existing status and merge existing aliases so seeding is non-destructive.
            claim.status = Some(prev.status);
            let mut merged: std::collections::BTreeSet<SeatName> =
                prev.aliases.iter().map(|a| a.value.clone()).collect();
            for a in claim.aliases {
                if existing_alias_owners
                    .get(&a)
                    .is_none_or(|owner| owner == &claim.seat)
                {
                    merged.insert(a);
                }
            }
            claim.aliases = merged.into_iter().collect();
        } else {
            if let Some(owner) = existing_alias_owners.get(&claim.seat)
                && owner != &claim.seat
            {
                skipped_conflicts.push(format!("{} (alias of {})", claim.seat, owner));
                continue;
            }
            claim.aliases.retain(|a| {
                existing_alias_owners
                    .get(a)
                    .is_none_or(|owner| owner == &claim.seat)
            });
        }
        let rec = client.call(claim).await?;
        existing_alias_owners.insert(rec.seat.clone(), rec.seat.clone());
        for alias in &rec.aliases {
            existing_alias_owners.insert(alias.value.clone(), rec.seat.clone());
        }
        applied.push(rec);
    }

    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "dry_run": false,
                "sources_read": plan
                    .sources_read
                    .iter()
                    .map(|p| p.to_string_lossy().into_owned())
                    .collect::<Vec<_>>(),
                "skipped_targets": plan.skipped_targets,
                "skipped_retired": skipped_retired,
                "skipped_conflicts": skipped_conflicts,
                "applied_count": applied.len(),
                "seats": applied,
            }))
            .expect("seed applied json")
        );
    } else {
        println!(
            "presence seed-interchange: applied {} seat(s), skipped_targets={:?}, skipped_retired={:?}, skipped_conflicts={:?}",
            applied.len(),
            plan.skipped_targets,
            skipped_retired,
            skipped_conflicts
        );
    }
    Ok(ExitCode::SUCCESS)
}

async fn execute_presence_export_manifest(
    path: &Path,
    include_retired: bool,
    endpoint: Endpoint,
    sock_path: &Path,
    explicit_token: Option<&str>,
    as_json: bool,
) -> Result<ExitCode, CliError> {
    let cred = resolve_cli_credential(sock_path, explicit_token)?;
    let client = RemoteClient::connect(endpoint, cred, "presence-export-manifest").await?;
    let snap = client
        .call(PresenceList {
            include_retired,
            ..PresenceList::default()
        })
        .await?;
    let records: Vec<_> = snap.seats.iter().map(|v| &v.record).collect();
    let manifest = ResidentManifest::from_records(records);
    let mut pretty =
        serde_json::to_string_pretty(&manifest).expect("resident manifest json serialization");
    pretty.push('\n');

    if path == Path::new("-") {
        print!("{pretty}");
        let _ = std::io::stdout().flush();
    } else {
        write_file_atomic_plain(path, pretty.as_bytes())?;
        if as_json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "schema": manifest.schema,
                    "path": path.to_string_lossy(),
                    "seat_count": manifest.seats.len(),
                }))
                .expect("export summary json")
            );
        } else {
            println!(
                "presence export-manifest: wrote {} seat(s) to {}",
                manifest.seats.len(),
                path.display()
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn execute_presence_import_manifest(
    path: &Path,
    dry_run: bool,
    endpoint: Endpoint,
    sock_path: &Path,
    explicit_token: Option<&str>,
    as_json: bool,
) -> Result<ExitCode, CliError> {
    let raw = rex_wave_fs::read_regular_string(path, MAX_CLI_INPUT_BYTES).map_err(|e| {
        CliError::Msg(format!(
            "refusing to read manifest file {}: {e}",
            path.display()
        ))
    })?;
    let manifest: ResidentManifest = serde_json::from_str(&raw)
        .map_err(|e| CliError::Msg(format!("parse manifest {}: {e}", path.display())))?;
    if manifest.schema != RESIDENT_MANIFEST_SCHEMA {
        return Err(CliError::Msg(format!(
            "unsupported resident manifest schema '{}' (expected '{}')",
            manifest.schema, RESIDENT_MANIFEST_SCHEMA
        )));
    }
    let mut seen_seats = std::collections::BTreeSet::new();
    for entry in &manifest.seats {
        if !seen_seats.insert(entry.seat.clone()) {
            return Err(CliError::Msg(format!(
                "duplicate seat '{}' in manifest {}",
                entry.seat,
                path.display()
            )));
        }
    }

    if dry_run {
        if as_json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "dry_run": true,
                    "schema": manifest.schema,
                    "path": path.to_string_lossy(),
                    "seat_count": manifest.seats.len(),
                    "seats": manifest.seats,
                }))
                .expect("import dry-run json")
            );
        } else {
            println!(
                "presence import-manifest (dry-run): {} seat(s) validated from {}",
                manifest.seats.len(),
                path.display()
            );
        }
        return Ok(ExitCode::SUCCESS);
    }

    let cred = resolve_cli_credential(sock_path, explicit_token)?;
    let client = RemoteClient::connect(endpoint, cred, "presence-import-manifest").await?;
    if !client.me().is_operator() {
        return Err(CliError::Api(ApiError::Denied {
            principal: client.me().to_string(),
            action: "presence import-manifest".to_owned(),
        }));
    }
    let mut imported = Vec::with_capacity(manifest.seats.len());
    for entry in manifest.seats {
        let rec = client.call(entry.into_claim(None)).await?;
        imported.push(rec);
    }

    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "dry_run": false,
                "schema": manifest.schema,
                "path": path.to_string_lossy(),
                "imported_count": imported.len(),
                "seats": imported,
            }))
            .expect("import summary json")
        );
    } else {
        println!(
            "presence import-manifest: imported {} seat(s) from {}",
            imported.len(),
            path.display()
        );
    }
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
    if let Cmd::Tasks(TaskCmd::OrgSync {
        room,
        file,
        once: _,
        watch,
        interval_ms,
        conflict_sidecar,
    }) = cmd
    {
        return execute_tasks_org_sync(
            client,
            room,
            file,
            *watch,
            *interval_ms,
            *conflict_sidecar,
            as_json,
        )
        .await;
    }

    let wire = to_command_for(cmd, client.me())?;
    let read_user_cursor = match &wire {
        WireCommand::Wait(w) => w.cursor.clone(),
        _ => None,
    };
    let mut reply = client.call_wire(wire).await?;

    if let Cmd::Read { limit, .. } = cmd
        && let WireReply::Wait(mut page) = reply
    {
        page.timed_out = false;
        let cap = limit.unwrap_or(200).clamp(1, 1000) as usize;
        if page.entries.len() > cap {
            let total_count = page.total_count.max(page.entries.len());
            page.entries.truncate(cap);
            let mut retained = read_user_cursor.unwrap_or_default();
            for env in &page.entries {
                retained.advance(&env.stream, env.seq);
            }
            page.cursor = retained;
            page.total_count = total_count;
            page.truncated = true;
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
            }) => match daemon::mint_worker_token(&sock_path, &wave, &role, thread_id.as_deref()) {
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
        Cmd::Mail(MailCmd::Shadow {
            interchange,
            out,
            once: _,
            watch,
            interval_ms,
            offline,
            shadow_read_only: _,
        }) => {
            execute_mail_shadow(
                &interchange,
                &out,
                watch,
                interval_ms,
                offline,
                &sock_path,
                cli.token.as_deref(),
                cli.json,
            )
            .await
        }
        Cmd::Mail(MailCmd::Score {
            shadow,
            acks,
            fixtures,
            mailwatch_seen,
        }) => execute_mail_score(
            &shadow,
            &acks,
            fixtures.as_deref(),
            &mailwatch_seen,
            cli.json,
        ),
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
        Cmd::Presence(PresenceCmd::Hold {
            seat,
            lane,
            incarnation,
            takeover,
            mail_horizon,
            client: client_name,
        }) => match resolve_cli_credential(&sock_path, cli.token.as_deref()) {
            Ok(cred) => match RemoteClient::connect(endpoint, cred, &client_name).await {
                Ok(client) => {
                    execute_presence_hold(
                        &client,
                        seat.as_ref(),
                        lane.as_ref(),
                        incarnation.as_ref(),
                        takeover,
                        mail_horizon,
                        &client_name,
                        cli.json,
                    )
                    .await
                }
                Err(e) => Err(CliError::Api(e)),
            },
            Err(e) => Err(CliError::Msg(e)),
        },
        Cmd::Presence(PresenceCmd::SeedInterchange {
            interchange,
            seats_tsv,
            dry_run,
        }) => {
            execute_presence_seed_interchange(
                &interchange,
                seats_tsv.as_deref(),
                dry_run,
                endpoint,
                &sock_path,
                cli.token.as_deref(),
                cli.json,
            )
            .await
        }
        Cmd::Presence(PresenceCmd::ExportManifest {
            path,
            include_retired,
        }) => {
            execute_presence_export_manifest(
                &path,
                include_retired,
                endpoint,
                &sock_path,
                cli.token.as_deref(),
                cli.json,
            )
            .await
        }
        Cmd::Presence(PresenceCmd::ImportManifest { path, dry_run }) => {
            execute_presence_import_manifest(
                &path,
                dry_run,
                endpoint,
                &sock_path,
                cli.token.as_deref(),
                cli.json,
            )
            .await
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

        let honesty_wave = parse_cli(&[
            "rex-wave",
            "wave",
            "start",
            "honesty-1",
            "--project",
            "p_123",
            "--goal",
            "Audit outputs",
            "--account",
            "work-acct",
            "--output-dir",
            "runs/honesty-1",
            "--worker",
            "auditor=claude-code/claude-opus-5-5",
            "--worker-output",
            "auditor=runs/honesty-1/custom",
            "--worker-writes",
            "auditor=src/lib.rs",
            "--worker-schema",
            "auditor={\"type\":\"object\"}",
        ]);
        let WireCommand::WaveStart(hws) = to_command(&honesty_wave.cmd).unwrap() else {
            panic!("expected WaveStart");
        };
        assert_eq!(hws.spec.defaults.account.as_deref(), Some("work-acct"));
        assert_eq!(
            hws.spec.output_dir.as_deref(),
            Some(std::path::Path::new("runs/honesty-1"))
        );
        assert_eq!(
            hws.spec.workers[0].output_dir.as_deref(),
            Some(std::path::Path::new("runs/honesty-1/custom"))
        );
        assert_eq!(
            hws.spec.workers[0].writes,
            vec![PathBuf::from("src/lib.rs")]
        );
        assert_eq!(
            hws.spec.workers[0].return_schema,
            Some(serde_json::json!({"type": "object"}))
        );

        let finish_cli = parse_cli(&[
            "rex-wave",
            "wave",
            "finish",
            "honesty-1",
            "--summary",
            "partial",
            "--allow-incomplete",
        ]);
        assert!(matches!(
            to_command(&finish_cli.cmd).unwrap(),
            WireCommand::WaveFinish(WaveFinish {
                allow_incomplete: true,
                ..
            })
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

        let other_stream = StreamId::Room {
            room: rex_wave_api::room::RoomId::Channel {
                scope: rex_wave_api::room::RoomScope::Global,
                name: ChannelName::parse("ops").unwrap(),
            },
        };
        let chained_cursor = serde_json::to_string(
            &Cursor::live()
                .with(
                    StreamId::Room {
                        room: rex_wave_api::room::RoomId::Channel {
                            scope: rex_wave_api::room::RoomScope::Global,
                            name: ChannelName::parse("general").unwrap(),
                        },
                    },
                    Seq(12),
                )
                .with(other_stream.clone(), Seq(7)),
        )
        .unwrap();
        let wait_chained =
            parse_cli(&["rex-wave", "wait", "#general", "--cursor", &chained_cursor]);
        let WireCommand::Wait(wc) = to_command(&wait_chained.cmd).unwrap() else {
            panic!("expected Wait");
        };
        assert!(wc.after.from_head());
        assert_eq!(
            wc.after.at(&other_stream),
            Seq(7),
            "chaining a live mentions cursor must retain other stream positions"
        );

        let wait_no_room_read_cursor = parse_cli(&["rex-wave", "wait", "--cursor", &read_cursor]);
        let WireCommand::Wait(wnr) = to_command(&wait_no_room_read_cursor.cmd).unwrap() else {
            panic!("expected Wait");
        };
        assert!(
            wnr.after.from_head(),
            "no-room mentions wait with a read cursor must promote from_head=true"
        );
        assert_eq!(
            wnr.after.at(&StreamId::Room {
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

        let auth = rex_wave_transport::StateDirAuthenticator::from_state_dir(
            SeatName::parse("owner").unwrap(),
            None,
            &dir,
        );
        let principal = auth
            .authenticate(None, &Credential::Token { token: minted })
            .unwrap();
        assert_eq!(principal, Principal::Seat { seat });

        let wave = WaveSlug::parse("auth-v2").unwrap();
        let role = RoleName::parse("author").unwrap();
        let standalone_wtok = daemon::mint_worker_token(&sock, &wave, &role, None).unwrap();
        assert!(standalone_wtok.starts_with("rw_w_"));
        assert_eq!(
            auth.authenticate(
                None,
                &Credential::Token {
                    token: standalone_wtok,
                },
            )
            .unwrap(),
            Principal::Worker {
                wave: wave.clone(),
                role: role.clone(),
            }
        );

        let wtok =
            daemon::mint_worker_token(&sock, &wave, &role, Some("t3:auth-v2:author:a1")).unwrap();
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

        let org_sync_cli = parse_cli(&[
            "rex-wave",
            "tasks",
            "org-sync",
            "#general",
            "/tmp/tasks.org",
            "--once",
        ]);
        assert!(matches!(
            org_sync_cli.cmd,
            Cmd::Tasks(TaskCmd::OrgSync {
                once: true,
                watch: false,
                ..
            })
        ));

        let shadow_cli = parse_cli(&[
            "rex-wave",
            "mail",
            "shadow",
            "--offline",
            "--interchange",
            "/tmp/interchange",
            "--out",
            "/tmp/shadow.jsonl",
            "--once",
        ]);
        assert!(matches!(
            shadow_cli.cmd,
            Cmd::Mail(MailCmd::Shadow {
                offline: true,
                once: true,
                watch: false,
                ..
            })
        ));

        let score_cli = parse_cli(&[
            "rex-wave",
            "mail",
            "score",
            "--shadow",
            "/tmp/shadow.jsonl",
            "--acks",
            "/tmp/acks.jsonl",
            "--fixtures",
            "/tmp/cases.jsonl",
            "--json",
        ]);
        assert!(score_cli.json);
        assert!(matches!(
            score_cli.cmd,
            Cmd::Mail(MailCmd::Score {
                fixtures: Some(_),
                ..
            })
        ));

        let client = DemoClient::new();
        let status_cli = parse_cli(&["rex-wave", "wave", "status", "auth-v2"]);
        execute_remote(&client, &status_cli.cmd, true)
            .await
            .unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }
}
