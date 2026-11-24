//! The single list of everything the daemon can be asked to do.
//!
//! Adding a command = adding one line to `commands!` below plus its arg/reply
//! structs in the owning domain module. The macro derives:
//!
//! * `WireCommand` / `WireReply`, the flat, serde-tagged wire enums
//!   (`{"cmd":"room_post","args":{...}}`). The serde tag **is** the MCP tool name
//!   and the `rex.<tool>()` Code Mode function name.
//! * `Family` and one `<Family>Command` enum per family, plus `Routed`, which
//!   the daemon router matches exhaustively, so a new family fails the build
//!   until it has an owner (per encode-lessons-in-structure).
//! * `impl Command for X` with the typed reply.
//! * `TOOLS`, the MCP / BB tool catalog, each entry carrying its JSON Schema.

use crate::board::*;
use crate::mail::*;
use crate::notify::*;
use crate::presence::*;
use crate::read::*;
use crate::room::*;
use crate::wave::*;
use crate::{Ack, ApiError, Principal};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Who may issue a command. Checked once, in the router, against the session principal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Audience {
    /// Any authenticated principal: operator, seat, worker, and the daemon's
    /// own parts (reactors post to rooms, unbind presence and raise
    /// notifications as `System`). Finer, per-resource policy (room
    /// membership, wave roster) lives in the owning decider.
    Member,
    /// Only the human operator (approvals, wave launch).
    Operator,
    /// Only daemon-internal parts (`Principal::System`).
    System,
}

impl Audience {
    /// Whether `by` may issue a command of this audience.
    pub fn allows(self, by: &Principal) -> bool {
        match self {
            Audience::Member => true,
            Audience::Operator => by.is_operator(),
            Audience::System => matches!(by, Principal::System { .. }),
        }
    }
}

/// The audience check the router runs once per command. Finer policy
/// belongs to deciders.
pub fn authorize(audience: Audience, by: &Principal) -> Result<(), ApiError> {
    if audience.allows(by) {
        Ok(())
    } else {
        Err(ApiError::Denied {
            principal: by.to_string(),
            action: format!("{audience:?} command"),
        })
    }
}

/// Catalog entry for MCP `tools/list`, BB `registerTool`, and `rex.*` in Code Mode.
#[derive(Clone, Copy, Debug)]
pub struct ToolMeta {
    pub name: &'static str,
    pub doc: &'static str,
    pub family: Family,
    pub audience: Audience,
    pub mutates: bool,
    /// JSON Schema of the command's `args`.
    pub input_schema: fn() -> serde_json::Value,
}

fn schema_of<T: JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("schemas serialize")
}

/// `[{name, description, inputSchema}]` for every tool a non-system caller
/// may use: MCP `tools/list` and `bb-plugin/tools.generated.json`.
pub fn tool_catalog() -> serde_json::Value {
    TOOLS
        .iter()
        .filter(|t| t.audience != Audience::System)
        .map(|t| serde_json::json!({ "name": t.name, "description": t.doc, "inputSchema": (t.input_schema)() }))
        .collect()
}

/// A typed request. Implemented only by the `commands!` expansion.
pub trait Command: Serialize + DeserializeOwned + JsonSchema + Send + Sync + 'static {
    type Reply: Serialize + DeserializeOwned + Send + 'static;
    const TOOL: &'static str;
    const FAMILY: Family;
    const AUDIENCE: Audience;
    const MUTATES: bool;
    fn into_wire(self) -> WireCommand;
    fn reply_from_wire(reply: WireReply) -> Result<Self::Reply, ApiError>;
}

macro_rules! commands {
    (
        $(
            $family:ident ( $fam_enum:ident ) {
                $( $cmd:ident -> $reply:ty : $tool:literal, $aud:ident, $mutates:literal, $doc:literal; )*
            }
        )*
    ) => {
        /// The subsystem that owns a command. One owner per family.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum Family { $( $family, )* }

        $(
            #[doc = concat!("Commands owned by the `", stringify!($family), "` family.")]
            #[derive(Clone, Debug)]
            pub enum $fam_enum { $( $cmd($cmd), )* }

            impl From<$fam_enum> for WireCommand {
                fn from(c: $fam_enum) -> WireCommand {
                    match c { $( $fam_enum::$cmd(x) => WireCommand::$cmd(x), )* }
                }
            }
        )*

        /// Exhaustively matched by the daemon router: one arm per family.
        #[derive(Clone, Debug)]
        pub enum Routed { $( $family($fam_enum), )* }

        /// Flat wire request. `{"cmd": "<tool>", "args": {...}}`.
        #[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
        #[serde(tag = "cmd", content = "args")]
        pub enum WireCommand { $( $( #[serde(rename = $tool)] $cmd($cmd), )* )* }

        /// Flat wire reply. Variant name matches the command that produced it.
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[serde(tag = "reply", content = "value")]
        pub enum WireReply { $( $( #[serde(rename = $tool)] $cmd($reply), )* )* }

        impl WireCommand {
            pub fn route(self) -> Routed {
                match self { $( $( WireCommand::$cmd(c) => Routed::$family($fam_enum::$cmd(c)), )* )* }
            }
            pub fn meta(&self) -> &'static ToolMeta {
                let name = match self { $( $( WireCommand::$cmd(_) => $tool, )* )* };
                TOOLS.iter().find(|t| t.name == name).expect("TOOLS is generated from the same list")
            }
        }

        $( $(
            impl Command for $cmd {
                type Reply = $reply;
                const TOOL: &'static str = $tool;
                const FAMILY: Family = Family::$family;
                const AUDIENCE: Audience = Audience::$aud;
                const MUTATES: bool = $mutates;
                fn into_wire(self) -> WireCommand { WireCommand::$cmd(self) }
                fn reply_from_wire(reply: WireReply) -> Result<$reply, ApiError> {
                    match reply {
                        WireReply::$cmd(v) => Ok(v),
                        other => Err(ApiError::ReplyMismatch { expected: $tool.into(), got: format!("{other:?}") }),
                    }
                }
            }
        )* )*

        /// The tool catalog, derived from the same list as the wire enums.
        pub const TOOLS: &[ToolMeta] = &[
            $( $( ToolMeta {
                name: $tool, doc: $doc, family: Family::$family,
                audience: Audience::$aud, mutates: $mutates,
                input_schema: schema_of::<$cmd>,
            }, )* )*
        ];
    };
}

commands! {
    Room(RoomCommand) {
        RoomEnsure     -> RoomInfo         : "room_ensure",     Member, true,  "Create-or-fetch a #channel or DM (idempotent: rooms are named, not numbered).";
        RoomPost       -> Posted           : "room_post",       Member, true,  "Post to a #channel or DM. @handles and @all are resolved against room members.";
        RoomJoin       -> Ack              : "room_join",       Member, true,  "Add a member (self, or others if you own the room).";
        RoomLeave      -> Ack              : "room_leave",      Member, true,  "Leave a room.";
        RoomLabel      -> Labeled          : "room_label",      Member, true,  "Add or remove validated key=value labels on a room message.";
        TaskClaim      -> TaskLeaseReceipt : "task_claim",      Member, true,  "Claim a room task thread with a TTL lease and monotonic fencing epoch.";
        TaskHeartbeat  -> TaskLeaseReceipt : "task_heartbeat",  Member, true,  "Renew an active task lease for the current holder and fencing epoch.";
        TaskTransition -> TaskView         : "task_transition", Member, true,  "Transition a room task thread's status with lease-epoch or revision CAS checks.";
        TaskUpdate     -> TaskView         : "task_update",     Member, true,  "Update a room task thread's title, dependencies, priority, or due timestamp.";
        RoomTasks      -> TaskListView     : "room_tasks",      Member, false, "List task threads in a room with readiness and lease-expiry state.";
    }
    Wave(WaveCommand) {
        WaveStart        -> WaveStarted : "wave_start",         Operator, true,  "Launch a wave on BB: creates #wave-<slug> under the BB project, a board, and spawns workers.";
        WaveAddWorker    -> Ack         : "wave_add_worker",    Member,   true,  "Add a worker (any BB provider/model) to a running wave.";
        WaveRetireWorker -> Ack         : "wave_retire_worker", Member,   true,  "Stop, then archive, a worker's BB thread.";
        WaveTell         -> Ack         : "wave_tell",          Member,   true,  "Send to one worker's BB thread with a BB delivery mode (auto, start, steer, steer-if-active, queue-if-active).";
        WaveResolve      -> Ack         : "wave_resolve",       Operator, true,  "Answer a worker's pending BB interaction (approval, question, plugin request).";
        WaveFinish       -> Ack         : "wave_finish",        Member,   true,  "Close a wave with an outcome; retires remaining workers.";
        WaveObserve      -> Ack         : "wave_observe",       System,   true,  "BB bridge reports a worker observation (spawned, activity, interaction).";
        WaveView         -> WaveSnapshot: "wave_view",          Member,   false, "Current wave roster and worker phases.";
    }
    Board(BoardCommand) {
        BoardEnsure -> BoardInfo     : "board_ensure", Member, true,  "Create-or-fetch a kanban board (project or personal).";
        CardUpsert  -> CardRef       : "card_upsert",  Member, true,  "Create a card (no `card`) or patch one; pass `expected` to fail instead of overwriting a concurrent edit.";
        CardArchive -> Ack           : "card_archive", Member, true,  "Archive a card.";
        BoardView   -> BoardSnapshot : "board_view",   Member, false, "Lanes and live cards of one board.";
    }
    Inbox(InboxCommand) {
        InboxList     -> InboxPage : "inbox_list",      Member,   false, "Your ranked notifications.";
        InboxAck      -> Ack       : "inbox_ack",       Member,   true,  "Mark notifications seen / done / dismissed.";
        InboxMarkRead -> Ack       : "inbox_mark_read", Member,   true,  "Advance your read cursor in a room.";
        SetArchived   -> Ack       : "set_archived",    Operator, true,  "Archive or unarchive a project, wave, or room in the sidebar tree.";
        InboxRaise    -> Ack       : "inbox_raise",     System,   true,  "Herald files a notification into one principal's inbox.";
    }
    Mail(MailCommand) {
        MailPromote  -> LetterRef : "promote_to_letter", Member, true, "Bank a room range as a canon letter in the NON-PROD sandbox (bank before announce).";
        MailAck      -> Ack       : "mail_ack",          Member, true, "ACK a sandbox letter by its exact sha256.";
        MailWakeBell -> Ack       : "mail_wake_bell",    System, true, "wake-bell Sink ingress: deliver a scheduled prompt into a seat's DM.";
    }
    Presence(PresenceCommand) {
        PresenceClaim           -> SeatRecord         : "presence_claim",            Member, true,  "Claim or update durable seat metadata (role, projects, aliases, status) with provenance.";
        PresenceSetStatus       -> SeatRecord         : "presence_set_status",       Member, true,  "Transition a seat between active, dormant, and retired with provenance.";
        PresenceBind            -> SeatBinding        : "presence_bind",             Member, true,  "Bind an ephemeral live session endpoint to a seat.";
        PresenceHeartbeat       -> SeatBinding        : "presence_heartbeat",        Member, true,  "Renew the TTL lease on an active seat binding.";
        PresenceUnbind          -> Ack                : "presence_unbind",           Member, true,  "Unbind an active seat session incarnation.";
        PresenceExpire          -> Ack                : "presence_expire",           System, true,  "PresenceMonitor expires a dead or lapsed seat binding.";
        PresenceTransportHealth -> Ack                : "presence_transport_health", System, true,  "Report transport provider health to trip or clear TransportDown status.";
        PresenceList            -> PresenceSnapshot   : "presence_list",             Member, false, "List durable seats and live session bindings.";
        PresenceResolve         -> PresenceResolution : "presence_resolve",          Member, false, "Resolve a seat name, alias, or project to its durable record and live bindings.";
    }
    Read(ReadCommand) {
        Read       -> Page                : "read",        Member, false, "Page one stream after a seq.";
        Wait       -> Page                : "wait",        Member, false, "Block until an interest matches after a cursor; returns the full catch-up slice.";
        Sidebar    -> SidebarTreeSnapshot : "sidebar",     Member, false, "Sidebar tree: inbox and kanban badges, projects > rooms > wave agents, channels, DMs.";
        ThreadPage -> ThreadPageView      : "thread_page", Member, false, "One wave agent's BB thread events and context usage (BB-owned, proxied).";
    }
}
