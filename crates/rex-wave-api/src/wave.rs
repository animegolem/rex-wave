//! Waves: a goal, a BB project, and a roster of workers on any BB provider.
//! Pure state machines live in `rex-wave-waves`; these are the shapes.

use crate::board::BoardId;
use crate::id::{BbInteractionId, BbProjectId, BbThreadId, IdemKey, ModelId, ProviderId, RoleName, Seq, WaveSlug};
use crate::room::{Body, RoomId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Where a worker's BB thread runs. Maps 1:1 onto verified BB spawn shapes
/// (#17): `ManagedWorktree` -> `{type:"host", workspace:{type:"managed-worktree", baseBranch}}`,
/// `ProjectDefault` -> `{type:"project-default"}`,
/// `ExistingPath` -> `{type:"provider", environmentProviderId:"git-worktree", inputs:{kind:"existing", path}}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "workspace", rename_all = "snake_case")]
pub enum Workspace {
    ManagedWorktree { base: BaseBranch },
    ProjectDefault,
    ExistingPath { path: PathBuf },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BaseBranch {
    Default,
    Named { name: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum PermissionMode {
    AcceptEdits,
    Auto,
    Full,
}

/// Which BB provider/model runs a worker. Any provider BB has configured.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentSpec {
    pub provider: ProviderId,
    #[serde(default)]
    pub model: Option<ModelId>,
    #[serde(default)]
    pub reasoning: Option<String>,
    #[serde(default)]
    pub service_tier: Option<String>,
    pub permission: PermissionMode,
    pub workspace: Workspace,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WorkerSpec {
    pub role: RoleName,
    /// The worker's brief: posted to `#wave-<slug>` as an @mention and sent as the spawn prompt.
    pub brief: Body,
    /// Overrides `WaveSpec::defaults` when present.
    #[serde(default)]
    pub agent: Option<AgentSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WaveSpec {
    pub slug: WaveSlug,
    pub project: BbProjectId,
    pub goal: Body,
    pub defaults: AgentSpec,
    pub workers: Vec<WorkerSpec>,
}

/// A worker's lifecycle. Transitions are pure (`rex_wave_waves::worker_step`).
///
/// ```text
/// Requested(n) --spawned--> Live{thread, activity} --retire--> Retiring --archived--> Retired
///      \--spawn failed--> Requested(n+1) (bounded) | Failed
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum WorkerPhase {
    Requested { attempt: u32 },
    Live { thread: BbThreadId, activity: Activity },
    Retiring { thread: BbThreadId },
    Retired { thread: Option<BbThreadId> },
    Failed { error: String },
}

impl WorkerPhase {
    /// Pure lifecycle step, shared by the wave decider and the sidebar
    /// projection so both fold observations identically. Stale observations
    /// (from an earlier attempt or an already-retired thread) leave the phase
    /// unchanged. Retrying a failed spawn is the decider's call: it emits a
    /// new `WorkerRequested`.
    pub fn observe(&self, observation: &Observation) -> WorkerPhase {
        match (self, observation) {
            (WorkerPhase::Requested { attempt }, Observation::Spawned { attempt: a, thread }) if a == attempt => {
                WorkerPhase::Live { thread: thread.clone(), activity: Activity::Working }
            }
            (WorkerPhase::Requested { attempt }, Observation::SpawnFailed { attempt: a, error }) if a == attempt => {
                WorkerPhase::Failed { error: error.clone() }
            }
            (WorkerPhase::Live { thread, .. }, Observation::Activity { activity }) => {
                WorkerPhase::Live { thread: thread.clone(), activity: activity.clone() }
            }
            (WorkerPhase::Live { thread, .. } | WorkerPhase::Retiring { thread }, Observation::Archived) => {
                WorkerPhase::Retired { thread: Some(thread.clone()) }
            }
            (WorkerPhase::Requested { .. }, Observation::Archived) => WorkerPhase::Retired { thread: None },
            (phase, _) => phase.clone(),
        }
    }

    /// The phase after `wave_retire_worker` or `wave_finish`.
    pub fn retiring(&self) -> WorkerPhase {
        match self {
            WorkerPhase::Live { thread, .. } => WorkerPhase::Retiring { thread: thread.clone() },
            WorkerPhase::Requested { .. } => WorkerPhase::Retired { thread: None },
            phase => phase.clone(),
        }
    }
}

/// What a live worker is doing, as observed from BB events. Modeling all four
/// non-working states explicitly is what makes wakeups correct (#12):
/// `Parked` wakes by `wait` returning; `Idle` wakes by `threads.send(auto)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "activity", rename_all = "snake_case")]
pub enum Activity {
    Working,
    /// Inside a `wait` tool call (zero-token park) on `room` since `since`;
    /// `room: None` is a mentions-only wait, which covers every room.
    Parked { room: Option<RoomId>, since: Seq },
    /// Turn ended; needs a doorbell to run again.
    Idle,
    Blocked { interaction: PendingInteraction },
    Errored { error: String },
}

/// A BB pending interaction, normalized.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PendingInteraction {
    pub id: BbInteractionId,
    pub kind: InteractionKind,
    pub summary: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InteractionKind {
    Permission,
    Question { questions: Vec<Question> },
    Extension,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Question {
    pub id: String,
    pub prompt: String,
    pub options: Vec<String>,
    pub multi: bool,
}

/// Mirrors `@get-bb/plugin-sdk@0.6.132` `PendingInteractionResolution`
/// variant for variant, so the bridge maps it to BB JSON without losing a case.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InteractionResolution {
    AllowOnce {
        #[serde(default)]
        granted_permissions: Option<GrantedPermissions>,
    },
    AllowForSession {
        #[serde(default)]
        granted_permissions: Option<GrantedPermissions>,
    },
    Deny,
    UserAnswer { answers: BTreeMap<String, UserAnswer> },
    PluginSubmitted {
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        detail: Option<String>,
        #[serde(default)]
        payload: Option<serde_json::Value>,
    },
    RequestAnswer { value: serde_json::Value },
}

/// BB `grantedPermissions`: `None` on a field means "no change to that grant".
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GrantedPermissions {
    #[serde(default)]
    pub file_system: Option<FileSystemGrant>,
    #[serde(default)]
    pub network: Option<NetworkGrant>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FileSystemGrant {
    pub read: Vec<String>,
    pub write: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct NetworkGrant {
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct UserAnswer {
    pub selected: Vec<String>,
    #[serde(default)]
    pub free_text: Option<String>,
}

/// How a `wave_tell` reaches the thread. Mirrors BB `ThreadSendArgs["mode"]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum SteerDeliveryMode {
    /// Wake if idle, queue if active. The doorbell default.
    Auto,
    /// Start a new turn; fails if one is running.
    Start,
    /// Interrupt the running turn.
    Steer,
    /// Add to the running turn, or start one if idle.
    SteerIfActive,
    /// Wait behind the running turn, or start one if idle.
    QueueIfActive,
}

/// What the wave conductor should do when a worker is @mentioned in `room`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakePlan {
    /// The worker will see it anyway: it is working, spawning, or parked in a
    /// `wait` that covers `room`.
    Covered,
    /// Ring the worker's BB thread with this mode.
    Doorbell(SteerDeliveryMode),
    /// Do not disturb: blocked on the operator, failing, or leaving.
    Suppress,
}

/// Pure wakeup policy. A worker parked in a `wait` on another room is mid-turn,
/// so it needs `steer-if-active`; an idle worker needs `auto` to start a turn.
pub fn plan_mention_wakeup(phase: &WorkerPhase, room: &RoomId) -> WakePlan {
    match phase {
        WorkerPhase::Requested { .. } => WakePlan::Covered,
        WorkerPhase::Live { activity, .. } => match activity {
            Activity::Working => WakePlan::Covered,
            Activity::Parked { room: None, .. } => WakePlan::Covered,
            Activity::Parked { room: Some(parked), .. } if parked == room => WakePlan::Covered,
            Activity::Parked { .. } => WakePlan::Doorbell(SteerDeliveryMode::SteerIfActive),
            Activity::Idle => WakePlan::Doorbell(SteerDeliveryMode::Auto),
            Activity::Blocked { .. } | Activity::Errored { .. } => WakePlan::Suppress,
        },
        WorkerPhase::Retiring { .. } | WorkerPhase::Retired { .. } | WorkerPhase::Failed { .. } => WakePlan::Suppress,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum WaveOutcome {
    Completed { summary: Body },
    Cancelled { reason: String },
    Failed { reason: String },
}

/// What the BB bridge observed about a worker. System audience only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "observed", rename_all = "snake_case")]
pub enum Observation {
    Spawned { attempt: u32, thread: BbThreadId },
    SpawnFailed { attempt: u32, error: String },
    Activity { activity: Activity },
    Archived,
}


#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct WaveStart {
    pub spec: WaveSpec,
    #[serde(default)]
    pub idem: Option<IdemKey>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct WaveAddWorker {
    pub wave: WaveSlug,
    pub worker: WorkerSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct WaveRetireWorker {
    pub wave: WaveSlug,
    pub role: RoleName,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct WaveTell {
    pub wave: WaveSlug,
    pub role: RoleName,
    pub body: Body,
    pub mode: SteerDeliveryMode,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct WaveResolve {
    pub wave: WaveSlug,
    pub role: RoleName,
    pub interaction: BbInteractionId,
    pub resolution: InteractionResolution,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct WaveFinish {
    pub wave: WaveSlug,
    pub outcome: WaveOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct WaveObserve {
    pub wave: WaveSlug,
    pub role: RoleName,
    pub observation: Observation,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct WaveView {
    pub wave: WaveSlug,
}


#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct WaveStarted {
    pub wave: WaveSlug,
    pub room: RoomId,
    pub board: BoardId,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct WaveSnapshot {
    pub spec: WaveSpec,
    pub status: WaveStatus,
    pub workers: Vec<(RoleName, AgentSpec, WorkerPhase)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum WaveStatus {
    Running,
    Finished { outcome: WaveOutcome },
}


#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WaveEvent {
    Started { spec: WaveSpec, room: RoomId, board: BoardId },
    WorkerRequested { role: RoleName, agent: AgentSpec, brief: Body, attempt: u32 },
    WorkerObserved { role: RoleName, observation: Observation },
    Told { role: RoleName, body: Body, mode: SteerDeliveryMode },
    InteractionResolved { role: RoleName, interaction: BbInteractionId, resolution: InteractionResolution },
    WorkerRetiring { role: RoleName, reason: Option<String> },
    Finished { outcome: WaveOutcome },
}
