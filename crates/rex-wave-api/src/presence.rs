//! Durable seat identity (`SeatRecord` with provenance) split from ephemeral
//! session liveness (`SeatBinding`), served on `StreamId::Presence`.

use crate::id::{BbThreadId, IdemKey, IncarnationId, Principal, SeatName, Seq, T3ThreadId, UnixMs};
use crate::room::RoomId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Reserved human operator surface seat name that must never become an `Active`
/// bound seat via hooks or receive mail rings / `MailUnrung` alerts.
pub const OPERATOR_SURFACE_SEAT: &str = "golem";

/// Reserved non-seat group/lane/pseudo-target names that appear in historical
/// interchange headers (`to:`/`cc:` or `to-cc/`) but are not bindable seats.
pub const NON_SEAT_TARGETS: &[&str] = &[
    "cc",
    "to-cc",
    "all-seats",
    "desk",
    "desktop",
    "codex",
    "claude",
    "unregistered-addressee",
];

/// Returns `true` when `seat` names the reserved human operator surface (`golem`).
pub fn is_operator_surface_seat(seat: &str) -> bool {
    seat.trim()
        .trim_start_matches('@')
        .eq_ignore_ascii_case(OPERATOR_SURFACE_SEAT)
}

/// Returns `true` when `seat` names a reserved non-seat group/lane pseudo-target.
pub fn is_non_seat_target(seat: &str) -> bool {
    let cleaned = seat.trim().trim_start_matches('@');
    NON_SEAT_TARGETS
        .iter()
        .any(|reserved| cleaned.eq_ignore_ascii_case(reserved))
}

/// Schema version tag for human-editable shared resident manifest JSON files
/// (`seat-in-a-card.org`).
pub const RESIDENT_MANIFEST_SCHEMA: &str = "rex-wave-resident-manifest/1";

/// Where a resident seat's primary terminal window lives (`seat-in-a-card.org`).
/// Changing `work_class` moves the window without restarting the underlying Rex terminal.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum WorkClass {
    ClassA,
    ClassB,
}

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
    /// First-live-bind mail horizon watermark: letters whose timestamp is strictly
    /// older than `mail_horizon_at` are treated as historical (`SkipPreHorizonHistory`)
    /// and never rung or included in `MailUnrung` alerts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mail_horizon_at: Option<UnixMs>,
    /// Resident work class (`class_a` main workspace vs `class_b` card-only) per `seat-in-a-card.org`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_class: Option<WorkClass>,
    /// Resident portrait/avatar asset path or URI (`seat-in-a-card.org`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,
    /// Resident home directory / working folder (`seat-in-a-card.org`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_dir: Option<String>,
    /// Command used to start a fresh resident session (`seat-in-a-card.org`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_cmd: Option<String>,
    /// Command used to resume an existing resident session (`seat-in-a-card.org`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_cmd: Option<String>,
    /// Stable Rex terminal target identifier (`seat-in-a-card.org`, `<house-terminal seat="...">`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rex_target: Option<String>,
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
    pub mail_horizon_at: Option<UnixMs>,
    #[serde(default)]
    pub work_class: Option<WorkClass>,
    #[serde(default)]
    pub avatar: Option<String>,
    #[serde(default)]
    pub home_dir: Option<String>,
    #[serde(default)]
    pub start_cmd: Option<String>,
    #[serde(default)]
    pub resume_cmd: Option<String>,
    #[serde(default)]
    pub rex_target: Option<String>,
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
    /// Optional explicit mail-horizon watermark override (`--mail-horizon <ms>`).
    /// When omitted on a seat's first live bind, the daemon stamps `ctx.at` automatically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mail_horizon_at: Option<UnixMs>,
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

/// One seat entry in a human-editable shared resident manifest (`seat-in-a-card.org`),
/// readable by Hearth (`<house-terminal seat="...">`), T3 Code's dock, and `rex-wave` Presence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ResidentManifestEntry {
    pub seat: SeatName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SeatStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<SeatName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub projects: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_room: Option<RoomId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mail_horizon_at: Option<UnixMs>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_class: Option<WorkClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_cmd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_cmd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rex_target: Option<String>,
}

impl ResidentManifestEntry {
    pub fn from_record(rec: &SeatRecord) -> Self {
        Self {
            seat: rec.seat.clone(),
            status: Some(rec.status),
            aliases: rec.aliases.iter().map(|a| a.value.clone()).collect(),
            role: rec.role.as_ref().map(|r| r.value.clone()),
            projects: rec.projects.iter().map(|p| p.value.clone()).collect(),
            home_room: rec.home_room.as_ref().map(|r| r.value.clone()),
            mail_horizon_at: rec.mail_horizon_at,
            work_class: rec.work_class,
            avatar: rec.avatar.clone(),
            home_dir: rec.home_dir.clone(),
            start_cmd: rec.start_cmd.clone(),
            resume_cmd: rec.resume_cmd.clone(),
            rex_target: rec.rex_target.clone(),
        }
    }

    pub fn into_claim(self, verified_how: Option<VerificationKind>) -> PresenceClaim {
        PresenceClaim {
            seat: self.seat,
            status: self.status,
            role: self.role,
            projects: self.projects,
            aliases: self.aliases,
            home_room: self.home_room,
            mail_horizon_at: self.mail_horizon_at,
            work_class: self.work_class,
            avatar: self.avatar,
            home_dir: self.home_dir,
            start_cmd: self.start_cmd,
            resume_cmd: self.resume_cmd,
            rex_target: self.rex_target,
            verified_how,
            replace: None,
            idem: None,
        }
    }
}

/// Shared human-editable resident manifest (`seat-in-a-card.org`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ResidentManifest {
    #[serde(default = "default_resident_manifest_schema")]
    pub schema: String,
    #[serde(default)]
    pub seats: Vec<ResidentManifestEntry>,
}

fn default_resident_manifest_schema() -> String {
    RESIDENT_MANIFEST_SCHEMA.to_owned()
}

impl ResidentManifest {
    pub fn from_records<'a>(records: impl IntoIterator<Item = &'a SeatRecord>) -> Self {
        let mut seats: Vec<ResidentManifestEntry> = records
            .into_iter()
            .map(ResidentManifestEntry::from_record)
            .collect();
        seats.sort_by(|a, b| a.seat.cmp(&b.seat));
        Self {
            schema: RESIDENT_MANIFEST_SCHEMA.to_owned(),
            seats,
        }
    }
}
