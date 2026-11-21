//! Durable seat identity (`SeatRecord` with provenance) split from ephemeral
//! session liveness (`SeatBinding`), served on `StreamId::Presence`.

use crate::id::{BbThreadId, IdemKey, IncarnationId, Principal, SeatName, Seq, T3ThreadId, UnixMs};
use crate::room::RoomId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Lifecycle state of a durable seat record. Letters to `Dormant` or `Retired`
/// seats never raise Operator inbox cards.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SeatStatus {
    #[default]
    Active,
    Dormant,
    Retired,
}

/// How an asserted field on [`SeatRecord`] was checked.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "how", rename_all = "snake_case")]
pub enum VerificationKind {
    SelfWord,
    WitnessedBy { seat: SeatName },
    MachineChecked,
}

/// Who asserted a durable seat metadata claim, how it was checked, and when.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct Provenance {
    pub asserted_by: Principal,
    pub verified_how: VerificationKind,
    pub at: UnixMs,
}

/// A durable metadata value paired with its provenance.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct Attested<T> {
    pub value: T,
    pub provenance: Provenance,
}

/// Durable identity and ownership metadata for a seat, preserved even when all
/// live session bindings are offline.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SeatRecord {
    pub seat: SeatName,
    #[serde(default)]
    pub status: SeatStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_provenance: Option<Provenance>,
    #[serde(default)]
    pub aliases: Vec<Attested<SeatName>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<Attested<String>>,
    #[serde(default)]
    pub projects: Vec<Attested<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_room: Option<Attested<RoomId>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_at: Option<UnixMs>,
    pub claimed_by: Principal,
    pub claimed_at: UnixMs,
}

/// Coarse transport family used for provider-level `TransportDown` circuit breaking.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TransportKind {
    UdsSession,
    ClaudeSession,
    BbThread,
    T3Thread,
    SelfWatcher,
    OperatorToast,
}

/// How one live seat incarnation can be reached.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "transport", rename_all = "snake_case")]
pub enum TransportEndpoint {
    UdsSession {
        conn_id: u64,
        client: String,
    },
    ClaudeSession {
        pid: u32,
        proc_start: String,
        started_at_ms: u64,
        session_id: String,
        messaging_socket: PathBuf,
        kind: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name_source: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        display_name: Option<String>,
    },
    BbThread {
        thread_id: BbThreadId,
    },
    T3Thread {
        thread_id: T3ThreadId,
    },
    SelfWatcher {
        harness: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pid: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        proc_start: Option<String>,
    },
    OperatorToast {
        command: String,
    },
}

impl TransportEndpoint {
    pub fn kind(&self) -> TransportKind {
        match self {
            TransportEndpoint::UdsSession { .. } => TransportKind::UdsSession,
            TransportEndpoint::ClaudeSession { .. } => TransportKind::ClaudeSession,
            TransportEndpoint::BbThread { .. } => TransportKind::BbThread,
            TransportEndpoint::T3Thread { .. } => TransportKind::T3Thread,
            TransportEndpoint::SelfWatcher { .. } => TransportKind::SelfWatcher,
            TransportEndpoint::OperatorToast { .. } => TransportKind::OperatorToast,
        }
    }

    pub fn is_allowed_claude_kind(kind: &str) -> bool {
        matches!(kind, "interactive" | "bg")
    }

    pub fn is_fork_display_name(name: &str) -> bool {
        name.contains('⑂')
    }

    pub fn is_derived_name_source(name_source: Option<&str>) -> bool {
        name_source == Some("derived")
    }
}

/// How liveness of a [`SeatBinding`] is tracked.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum LivenessMode {
    SocketLifetime,
    ProcessAndLease,
    LeaseOnly,
}

/// Current state of an ephemeral [`SeatBinding`].
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum BindingStatus {
    Live,
    TransportDown,
    Expired,
    Unbound,
}

/// Ephemeral session binding attached to a durable [`SeatRecord`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SeatBinding {
    pub seat: SeatName,
    pub incarnation_id: IncarnationId,
    pub epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane: Option<String>,
    pub endpoint: TransportEndpoint,
    pub liveness: LivenessMode,
    pub bound_at: UnixMs,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<UnixMs>,
    pub status: BindingStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PresenceClaim {
    pub seat: SeatName,
    #[serde(default)]
    pub status: Option<SeatStatus>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub projects: Vec<String>,
    #[serde(default)]
    pub aliases: Vec<SeatName>,
    #[serde(default)]
    pub home_room: Option<RoomId>,
    #[serde(default)]
    pub verified_how: Option<VerificationKind>,
    #[serde(default)]
    pub replace: Option<SeatName>,
    #[serde(default, alias = "id")]
    pub idem: Option<IdemKey>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PresenceSetStatus {
    pub seat: SeatName,
    pub status: SeatStatus,
    #[serde(default)]
    pub verified_how: Option<VerificationKind>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PresenceBind {
    pub seat: SeatName,
    #[serde(default)]
    pub incarnation_id: Option<IncarnationId>,
    #[serde(default)]
    pub lane: Option<String>,
    pub endpoint: TransportEndpoint,
    pub liveness: LivenessMode,
    #[serde(default)]
    pub ttl_ms: Option<u64>,
    #[serde(default)]
    pub takeover: bool,
    #[serde(default)]
    pub allow_fork: bool,
    #[serde(default, alias = "id")]
    pub idem: Option<IdemKey>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PresenceHeartbeat {
    pub seat: SeatName,
    pub incarnation_id: IncarnationId,
    #[serde(default)]
    pub ttl_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PresenceUnbind {
    pub seat: SeatName,
    pub incarnation_id: IncarnationId,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PresenceExpire {
    pub seat: SeatName,
    pub incarnation_id: IncarnationId,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PresenceTransportHealth {
    pub transport: TransportKind,
    pub down: bool,
    #[serde(default)]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct PresenceList {
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub live_only: bool,
    #[serde(default)]
    pub include_retired: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SeatPresenceView {
    pub record: SeatRecord,
    #[serde(default)]
    pub bindings: Vec<SeatBinding>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PresenceSnapshot {
    pub head: Seq,
    pub seats: Vec<SeatPresenceView>,
    #[serde(default)]
    pub down_transports: Vec<TransportKind>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PresenceResolve {
    pub query: String,
    #[serde(default)]
    pub lane: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PresenceResolution {
    pub record: SeatRecord,
    #[serde(default)]
    pub live_bindings: Vec<SeatBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_live_at: Option<UnixMs>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PresenceEvent {
    SeatClaimed {
        record: SeatRecord,
    },
    SeatStatusSet {
        seat: SeatName,
        status: SeatStatus,
        provenance: Provenance,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    Bound {
        binding: SeatBinding,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        superseded: Vec<IncarnationId>,
    },
    Heartbeated {
        seat: SeatName,
        incarnation_id: IncarnationId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expires_at: Option<UnixMs>,
    },
    Unbound {
        seat: SeatName,
        incarnation_id: IncarnationId,
        status: BindingStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    TransportHealthChanged {
        transport: TransportKind,
        down: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}
