//! `rex-wave`: single binary housing the daemon composition root, 2-pane TUI,
//! MCP server, and thin CLI verbs over `rex_wave_api::Client`.

mod daemon;

use clap::{Parser, Subcommand};
use rex_wave_api::{
    board::{
        Assign, Assignee, BoardFilter, BoardId, BoardView, CardArchive, CardNo, CardPatch,
        CardUpsert, LaneName, Priority, Title,
    },
    mail::{LetterPriority, MailPromote},
    notify::InboxList,
    read::Wait,
    room::{Body, RoomPost, RoomRef, RoomScope},
    wave::{
        AgentSpec, BaseBranch, InteractionResolution, PermissionMode, SteerDeliveryMode,
        WaveAddWorker, WaveFinish, WaveOutcome, WaveResolve, WaveRetireWorker, WaveSpec, WaveStart,
        WaveTell, WaveView, WorkerPhase, WorkerSpec, Workspace,
    },
    *,
};
use rex_wave_mcp::McpServer;
use rex_wave_transport::{Credential, Endpoint, RemoteClient};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser, Debug)]
#[command(
    name = "rex-wave",
    version,
    about = "Waves, #channels, DMs, kanban and notifications for BB + Rex"
)]
pub struct Cli {
    /// Daemon Unix socket path (default: `$XDG_STATE_HOME/rex-wave/daemon.sock`).
    #[arg(long, global = true)]
    pub socket: Option<PathBuf>,
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
    /// Serve MCP atomic tools + Code Mode (`run_code`) for one seat token.
    Mcp {
        #[arg(long, conflicts_with = "http")]
        stdio: bool,
        #[arg(long)]
        http: Option<SocketAddr>,
        #[arg(long, env = "REX_WAVE_TOKEN")]
        token: String,
    },
    /// Mint a capability token for a seat (`rex-wave token --seat sam` or `rex-wave token mint --seat sam`).
    Token(TokenArgs),
    /// Post to `#channel`, `#<project>/<channel>`, `@seat`, or `@<wave>/<role>`.
    Post {
        room: String,
        text: String,
        #[arg(long)]
        reply_to: Option<u64>,
        #[arg(long)]
        idem: Option<String>,
    },
    /// Block until matching messages arrive after `--since` and return the catch-up slice.
    Wait {
        room: Option<String>,
        #[arg(long, default_value_t = 0)]
        since: u64,
        #[arg(long)]
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
    /// Launch and manage multi-provider waves on BB.
    #[command(subcommand)]
    Wave(WaveCmd),
    /// Manage Kanban / org-todo tasks.
    #[command(subcommand)]
    Task(TaskCmd),
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
    #[arg(long)]
    pub seat: Option<String>,
    #[command(subcommand)]
    pub sub: Option<TokenSubcmd>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum TokenSubcmd {
    /// Mint a new token for a seat.
    Mint {
        #[arg(long = "seat")]
        seat_flag: Option<String>,
        seat: Option<String>,
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
    /// Approve a worker's pending BB interaction.
    Approve {
        wave: String,
        role: String,
        interaction: String,
        #[arg(long)]
        session: bool,
    },
    /// Deny a worker's pending BB interaction.
    Deny {
        wave: String,
        role: String,
        interaction: String,
    },
    /// Steer or send a message to a worker's BB thread.
    #[command(name = "steer", visible_alias = "tell")]
    Steer {
        wave: String,
        role: String,
        text: String,
        #[arg(long, default_value = "auto")]
        mode: String,
    },
    /// Stop and archive a worker's BB thread.
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
    /// Add a card to a Kanban board.
    Add {
        title: String,
        #[arg(long)]
        board: String,
        #[arg(long)]
        lane: Option<String>,
        #[arg(long)]
        assign: Option<String>,
        #[arg(long)]
        priority: Option<String>,
    },
    /// Move a card to another lane.
    Move {
        board: String,
        no: u32,
        lane: String,
        #[arg(long)]
        expected: Option<u64>,
    },
    /// List lanes and live cards on a board.
    #[command(name = "list", visible_alias = "board")]
    List {
        board: Option<String>,
        #[arg(long = "board")]
        board_flag: Option<String>,
        #[arg(long)]
        lane: Option<String>,
        #[arg(long)]
        assign: Option<String>,
        #[arg(long)]
        label: Option<String>,
    },
    /// Move a card directly to the `done` lane.
    Done { board: String, no: u32 },
    /// Archive a card.
    Archive { board: String, no: u32 },
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

pub fn parse_principal_ref(raw: &str) -> Result<Principal, ApiError> {
    let trimmed = raw.strip_prefix('@').unwrap_or(raw);
    if let Some((w, r)) = trimmed.split_once('/') {
        Ok(Principal::Worker {
            wave: WaveSlug::parse(w)?,
            role: RoleName::parse(r)?,
        })
    } else {
        Ok(Principal::Seat {
            seat: SeatName::parse(trimmed)?,
        })
    }
}

pub fn parse_room_ref(s: &str) -> Result<RoomRef, ApiError> {
    if let Some(rest) = s.strip_prefix('#') {
        if let Some((proj, chan)) = rest.split_once('/') {
            let proj_clean = proj.strip_prefix("proj:").unwrap_or(proj);
            return Ok(RoomRef::Channel {
                scope: RoomScope::Project {
                    project: BbProjectId::parse(proj_clean)?,
                },
                name: ChannelName::parse(chan)?,
            });
        }
        return Ok(RoomRef::Channel {
            scope: RoomScope::Global,
            name: ChannelName::parse(rest)?,
        });
    }
    if s.starts_with('@') {
        return Ok(RoomRef::Dm {
            with: parse_principal_ref(s)?,
        });
    }
    Err(invalid(
        "room",
        "expected #<channel>, #<project>/<channel>, @<seat>, or @<wave>/<role>",
    ))
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

pub fn to_command_for(cmd: &Cmd, me: &Principal) -> Result<WireCommand, ApiError> {
    match cmd {
        Cmd::Post {
            room,
            text,
            reply_to,
            idem,
        } => Ok(WireCommand::RoomPost(RoomPost {
            room: parse_room_ref(room)?,
            body: Body::parse(text.clone())?,
            reply_to: reply_to.map(Seq),
            idem: match idem {
                Some(k) => Some(IdemKey::parse(k.clone())?),
                None => None,
            },
        })),
        Cmd::Wait {
            room,
            since,
            all,
            timeout,
        } => {
            let wait = match room {
                Some(r) => {
                    let room_id = parse_room_ref(r)?.resolve(me)?;
                    let mut w = Wait::room(room_id, Seq(*since), !*all);
                    w.timeout_ms = *timeout;
                    w
                }
                None => {
                    if *since > 0 {
                        return Err(invalid("since", "--since requires a target room"));
                    }
                    Wait {
                        interest: if *all {
                            Interest::Everything
                        } else {
                            Interest::Mentions
                        },
                        after: Cursor::live(),
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
        Cmd::Task(task_cmd) => task_to_command(task_cmd, me),
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

fn task_to_command(cmd: &TaskCmd, me: &Principal) -> Result<WireCommand, ApiError> {
    match cmd {
        TaskCmd::Add {
            title,
            board,
            lane,
            assign,
            priority,
        } => {
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
            Ok(WireCommand::CardUpsert(CardUpsert {
                board: map_board_val("board", BoardId::parse(board.clone()))?,
                card: None,
                expected: None,
                patch: CardPatch {
                    title: Some(map_board_val("title", Title::parse(title.clone()))?),
                    lane: lane_parsed,
                    assignee,
                    priority: prio_parsed,
                    ..CardPatch::default()
                },
                idem: None,
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
            board,
            board_flag,
            lane,
            assign,
            label,
        } => {
            let chosen_board = match board.as_ref().or(board_flag.as_ref()) {
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
        TaskCmd::Done { board, no } => Ok(WireCommand::CardUpsert(CardUpsert {
            board: map_board_val("board", BoardId::parse(board.clone()))?,
            card: Some(CardNo(*no)),
            expected: None,
            patch: CardPatch {
                lane: Some(map_board_val("lane", LaneName::parse("done"))?),
                ..CardPatch::default()
            },
            idem: None,
        })),
        TaskCmd::Archive { board, no } => Ok(WireCommand::CardArchive(CardArchive {
            board: map_board_val("board", BoardId::parse(board.clone()))?,
            card: CardNo(*no),
            expected: None,
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

fn extract_token_seat(args: &TokenArgs) -> Result<SeatName, ApiError> {
    let raw = match (&args.seat, &args.sub) {
        (Some(s), None) => s.as_str(),
        (_, Some(TokenSubcmd::Mint { seat_flag, seat })) => seat_flag
            .as_deref()
            .or(seat.as_deref())
            .ok_or_else(|| invalid("seat", "provide --seat <seat> or `token mint <seat>`"))?,
        (None, None) => {
            return Err(invalid(
                "seat",
                "provide `--seat <seat>` or `token mint <seat>`",
            ));
        }
    };
    Ok(SeatName::parse(raw.strip_prefix('@').unwrap_or(raw))?)
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
        WireReply::RoomPost(p) => println!("posted seq={} in {:?}", p.seq.0, p.room),
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
                    WorkerPhase::Retiring { thread } => format!("retiring({thread})"),
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
            } else {
                for env in &page.entries {
                    println!("[seq={}] {}: {:?}", env.seq.0, env.by, env.event);
                }
            }
        }
        other => println!(
            "{}",
            serde_json::to_string_pretty(other).expect("reply json")
        ),
    }
}

pub async fn execute_remote<C: Client>(client: &C, cmd: &Cmd, as_json: bool) -> Result<(), String> {
    let wire = to_command_for(cmd, client.me()).map_err(|e| e.to_string())?;
    let reply = client.call_wire(wire).await.map_err(|e| e.to_string())?;
    print_reply(&reply, as_json);
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let sock_path = daemon::resolve_socket_path(cli.socket.as_deref());
    let endpoint = Endpoint::Unix(sock_path.clone());

    let outcome: Result<(), String> = match cli.cmd {
        Cmd::Daemon(args) => daemon::run(args, cli.socket).await,
        Cmd::Tui { self_test: true } => rex_wave_tui::demo::run_self_test_demo()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        Cmd::Tui { self_test: false } => {
            match RemoteClient::connect(endpoint, Credential::PeerUid, "tui").await {
                Ok(client) => rex_wave_tui::run(client).await.map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            }
        }
        Cmd::Mcp {
            stdio: _,
            http,
            token,
        } => {
            let http_token = token.clone();
            match RemoteClient::connect(endpoint, Credential::Token { token }, "mcp").await {
                Ok(client) => {
                    let server = McpServer::new(client);
                    let res = match http {
                        Some(addr) => server.with_http_token(http_token).serve_http(addr).await,
                        None => server.serve_stdio().await,
                    };
                    res.map_err(|e| e.to_string())
                }
                Err(e) => Err(e.to_string()),
            }
        }
        Cmd::Token(token_args) => match extract_token_seat(&token_args) {
            Ok(seat) => match daemon::mint_seat_token(&sock_path, &seat) {
                Ok(tok) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({ "seat": seat.as_str(), "token": tok })
                        );
                    } else {
                        println!("{tok}");
                    }
                    Ok(())
                }
                Err(e) => Err(format!("mint token: {e}")),
            },
            Err(e) => Err(e.to_string()),
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
            Ok(())
        }
        Cmd::Watch { target, since } => {
            match RemoteClient::connect(endpoint, Credential::PeerUid, "cli-watch").await {
                Ok(client) => match parse_watch_target(&target, since, client.me()) {
                    Ok((interest, cursor)) => match client.watch(interest, cursor).await {
                        Ok(mut stream) => loop {
                            tokio::select! {
                                _ = tokio::signal::ctrl_c() => break Ok(()),
                                item = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)) => match item {
                                    Some(Ok(frame)) => {
                                        println!("{}", serde_json::to_string(&frame).expect("frame json"));
                                    }
                                    Some(Err(e)) => break Err(e.to_string()),
                                    None => break Ok(()),
                                }
                            }
                        },
                        Err(e) => Err(e.to_string()),
                    },
                    Err(e) => Err(e.to_string()),
                },
                Err(e) => Err(e.to_string()),
            }
        }
        other => match RemoteClient::connect(endpoint, Credential::PeerUid, "cli").await {
            Ok(client) => execute_remote(&client, &other, cli.json).await,
            Err(e) => Err(e.to_string()),
        },
    };

    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
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
        ]);
        assert!(matches!(
            to_command(&post_chan.cmd).unwrap(),
            WireCommand::RoomPost(_)
        ));

        let post_dm = parse_cli(&["rex-wave", "post", "@sam", "can you take the migration?"]);
        assert!(matches!(
            to_command(&post_dm.cmd).unwrap(),
            WireCommand::RoomPost(_)
        ));

        let wait_cli = parse_cli(&["rex-wave", "wait", "#p_123/wave-auth-v2", "--since", "41"]);
        assert!(matches!(
            to_command(&wait_cli.cmd).unwrap(),
            WireCommand::Wait(_)
        ));

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
        let seat = extract_token_seat(&targs).unwrap();
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

        let client = DemoClient::new();
        let status_cli = parse_cli(&["rex-wave", "wave", "status", "auth-v2"]);
        execute_remote(&client, &status_cli.cmd, true)
            .await
            .unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }
}
