//! Waves: a goal, a BB project, and a roster of workers on any BB provider.
//! Pure state machines live in `rex-wave-waves`; these are the shapes.

use crate::board::BoardId;
use crate::id::{
    BbInteractionId, BbProjectId, BbThreadId, IdemKey, ModelId, ProviderId, RoleName, Seq, WaveSlug,
};
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
    /// Optional explicit paying account / billing profile expected for this worker.
    #[serde(default)]
    pub account: Option<String>,
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
    /// Project-owned output directory for this worker; defaults to `<wave.output_dir>/<role>`.
    #[serde(default)]
    pub output_dir: Option<PathBuf>,
    /// Exclusive project-owned write targets declared by this worker (enforced one-writer per path).
    #[serde(default)]
    pub writes: Vec<PathBuf>,
    /// Optional JSON Schema describing the expected structured return payload.
    #[serde(default)]
    pub return_schema: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WaveSpec {
    pub slug: WaveSlug,
    pub project: BbProjectId,
    pub goal: Body,
    pub defaults: AgentSpec,
    pub workers: Vec<WorkerSpec>,
    /// Optional project-owned base directory for wave outputs (`<output_dir>/<role>/`).
    #[serde(default)]
    pub output_dir: Option<PathBuf>,
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
    Requested {
        attempt: u32,
    },
    Live {
        thread: BbThreadId,
        activity: Activity,
    },
    /// `errored` and `refused` carry live failure/refusal states across retirement
    /// so a failed or safety-refused worker never reads as a clean exit.
    Retiring {
        thread: BbThreadId,
        #[serde(default)]
        errored: Option<String>,
        #[serde(default)]
        refused: Option<String>,
    },
    Retired {
        thread: Option<BbThreadId>,
        #[serde(default)]
        errored: Option<String>,
        #[serde(default)]
        refused: Option<String>,
    },
    Failed {
        error: String,
    },
}

impl WorkerPhase {
    /// Pure lifecycle step, shared by the wave decider and the sidebar
    /// projection so both fold observations identically. Stale observations
    /// (from an earlier attempt or an already-retired thread) leave the phase
    /// unchanged. Retrying a failed spawn is the decider's call: it emits a
    /// new `WorkerRequested`.
    pub fn observe(&self, observation: &Observation) -> WorkerPhase {
        match (self, observation) {
            (WorkerPhase::Requested { attempt }, Observation::Spawned { attempt: a, thread })
                if a == attempt =>
            {
                WorkerPhase::Live {
                    thread: thread.clone(),
                    activity: Activity::Working,
                }
            }
            (
                WorkerPhase::Requested { attempt },
                Observation::SpawnFailed { attempt: a, error },
            ) if a == attempt => WorkerPhase::Failed {
                error: error.clone(),
            },
            (
                WorkerPhase::Live {
                    thread,
                    activity: cur_activity,
                },
                Observation::Activity {
                    activity,
                    thread: obs_thread,
                    ..
                },
            ) if obs_thread.as_ref().is_none_or(|t| t == thread) => {
                let next_activity = match (cur_activity, activity) {
                    (Activity::Refused { reason }, Activity::Errored { .. }) => Activity::Refused {
                        reason: reason.clone(),
                    },
                    (_, act) => act.clone(),
                };
                WorkerPhase::Live {
                    thread: thread.clone(),
                    activity: next_activity,
                }
            }
            (
                WorkerPhase::Live {
                    thread: live_thread,
                    activity: cur_activity,
                },
                Observation::TurnsSuppressed { thread, keys, .. },
            ) if live_thread == thread => {
                if let Some(outcome) = failure_activity_from_suppression_keys(keys) {
                    let next_activity = match (cur_activity, outcome) {
                        (Activity::Refused { reason }, Activity::Errored { .. }) => {
                            Activity::Refused {
                                reason: reason.clone(),
                            }
                        }
                        (_, act) => act,
                    };
                    WorkerPhase::Live {
                        thread: live_thread.clone(),
                        activity: next_activity,
                    }
                } else {
                    self.clone()
                }
            }
            (
                WorkerPhase::Retiring {
                    thread: live_thread,
                    errored,
                    refused,
                },
                Observation::TurnsSuppressed { thread, keys, .. },
            ) if live_thread == thread => match failure_activity_from_suppression_keys(keys) {
                Some(Activity::Refused { reason }) => WorkerPhase::Retiring {
                    thread: live_thread.clone(),
                    errored: errored.clone(),
                    refused: Some(reason),
                },
                Some(Activity::Errored { error }) if refused.is_none() => WorkerPhase::Retiring {
                    thread: live_thread.clone(),
                    errored: Some(error),
                    refused: None,
                },
                _ => self.clone(),
            },
            (
                WorkerPhase::Live { thread, activity },
                Observation::Archived {
                    thread: obs_thread, ..
                },
            ) if obs_thread.as_ref().is_none_or(|t| t == thread) => WorkerPhase::Retired {
                thread: Some(thread.clone()),
                errored: activity.error().map(str::to_owned),
                refused: activity.refusal().map(str::to_owned),
            },
            (
                WorkerPhase::Retiring {
                    thread,
                    errored,
                    refused,
                },
                Observation::Archived {
                    thread: obs_thread, ..
                },
            ) if obs_thread.as_ref().is_none_or(|t| t == thread) => WorkerPhase::Retired {
                thread: Some(thread.clone()),
                errored: errored.clone(),
                refused: refused.clone(),
            },
            (
                WorkerPhase::Requested { attempt },
                Observation::Archived {
                    thread: obs_thread,
                    attempt: obs_attempt,
                },
            ) if obs_attempt.is_none_or(|a| a == *attempt)
                && (obs_thread.is_none() || obs_attempt.is_some()) =>
            {
                WorkerPhase::Retired {
                    thread: obs_thread.clone(),
                    errored: None,
                    refused: None,
                }
            }
            (
                WorkerPhase::Retired {
                    thread: None,
                    errored,
                    refused,
                },
                Observation::Archived {
                    thread: Some(obs_thread),
                    attempt: Some(_),
                },
            ) => WorkerPhase::Retired {
                thread: Some(obs_thread.clone()),
                errored: errored.clone(),
                refused: refused.clone(),
            },
            (phase, _) => phase.clone(),
        }
    }

    /// The phase after `wave_retire_worker` or `wave_finish`.
    pub fn retiring(&self) -> WorkerPhase {
        match self {
            WorkerPhase::Live { thread, activity } => WorkerPhase::Retiring {
                thread: thread.clone(),
                errored: activity.error().map(str::to_owned),
                refused: activity.refusal().map(str::to_owned),
            },
            WorkerPhase::Requested { .. } => WorkerPhase::Retired {
                thread: None,
                errored: None,
                refused: None,
            },
            phase => phase.clone(),
        }
    }

    /// The error a worker ended (or is ending) on, whatever its phase.
    pub fn error(&self) -> Option<&str> {
        match self {
            WorkerPhase::Live { activity, .. } => activity.error(),
            WorkerPhase::Retiring { errored, .. } | WorkerPhase::Retired { errored, .. } => {
                errored.as_deref()
            }
            WorkerPhase::Failed { error } => Some(error),
            WorkerPhase::Requested { .. } => None,
        }
    }

    /// The safety refusal a worker ended (or is ending) on, whatever its phase.
    pub fn refusal(&self) -> Option<&str> {
        match self {
            WorkerPhase::Live { activity, .. } => activity.refusal(),
            WorkerPhase::Retiring { refused, .. } | WorkerPhase::Retired { refused, .. } => {
                refused.as_deref()
            }
            WorkerPhase::Failed { .. } | WorkerPhase::Requested { .. } => None,
        }
    }
}

/// What a live worker is doing, as observed from BB/T3 events. Modeling all
/// non-working states explicitly is what makes wakeups correct (#12):
/// `Parked` wakes by `wait` returning; `Idle` wakes by `threads.send(auto)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "activity", rename_all = "snake_case")]
pub enum Activity {
    Working,
    /// Inside a `wait` tool call (zero-token park) on `room` since `since`;
    /// `room: None` is a mentions-only wait, which covers every room.
    Parked {
        room: Option<RoomId>,
        since: Seq,
    },
    /// Turn ended; needs a doorbell to run again.
    Idle,
    Blocked {
        interaction: PendingInteraction,
    },
    /// Turn ended with a provider safety / policy / content-filter refusal.
    Refused {
        reason: String,
    },
    Errored {
        error: String,
    },
}

impl Activity {
    pub fn error(&self) -> Option<&str> {
        match self {
            Activity::Errored { error } => Some(error),
            _ => None,
        }
    }

    pub fn refusal(&self) -> Option<&str> {
        match self {
            Activity::Refused { reason } => Some(reason),
            _ => None,
        }
    }
}

/// Extracts the highest-precedence terminal failure or refusal [`Activity`]
/// encoded in `Observation::TurnsSuppressed` / `suppress_keys` (`fp:<tag>:<reason>`
/// or `fe:<tag>:<error>`), preserving safety refusals over generic errors.
pub fn failure_activity_from_suppression_keys(keys: &[String]) -> Option<Activity> {
    let mut out: Option<Activity> = None;
    for key in keys {
        if let Some(rest) = key.strip_prefix("fp:")
            && let Some((_, reason)) = rest.split_once(':')
        {
            out = Some(Activity::Refused {
                reason: reason.to_owned(),
            });
        } else if let Some(rest) = key.strip_prefix("fe:")
            && let Some((_, error)) = rest.split_once(':')
            && !matches!(out, Some(Activity::Refused { .. }))
        {
            out = Some(Activity::Errored {
                error: error.to_owned(),
            });
        }
    }
    out
}

/// Shared per-worker unsettled terminal failure/refusal tracker used by both
/// `Waves` and `SidebarProjection` so live, retiring, and archived phases stay
/// identical even when an intermediate `Working` turn precedes retirement or archive,
/// or when pre-spawn `TurnsSuppressed` keys arrive across multiple threads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsettledFailure {
    attempt: u32,
    active_thread: Option<BbThreadId>,
    failures_by_thread: BTreeMap<BbThreadId, Activity>,
    failure_seq_by_thread: BTreeMap<BbThreadId, u64>,
    seq_by_thread: BTreeMap<BbThreadId, u64>,
    last_activity_seq: u64,
    last_idle_seq: u64,
}

impl Default for UnsettledFailure {
    fn default() -> Self {
        Self::for_attempt(1)
    }
}

impl UnsettledFailure {
    pub fn for_attempt(attempt: u32) -> Self {
        Self {
            attempt: attempt.max(1),
            active_thread: None,
            failures_by_thread: BTreeMap::new(),
            failure_seq_by_thread: BTreeMap::new(),
            seq_by_thread: BTreeMap::new(),
            last_activity_seq: 0,
            last_idle_seq: 0,
        }
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn activity(&self) -> Option<&Activity> {
        self.active_thread
            .as_ref()
            .and_then(|t| self.failures_by_thread.get(t))
    }

    pub fn activity_for(&self, thread: &BbThreadId) -> Option<&Activity> {
        self.failures_by_thread.get(thread)
    }

    /// Returns any unresolved failure/refusal blocking ordinary `WaveFinish(Completed)`
    /// in `phase`. While `Requested` or `Retired { thread: None }`, any candidate
    /// thread failure recorded for this attempt defers completion until `Spawned`
    /// or `Archived` identifies the actual worker thread.
    pub fn blocker_for_phase(&self, phase: &WorkerPhase) -> Option<&Activity> {
        match phase {
            WorkerPhase::Requested { .. } | WorkerPhase::Retired { thread: None, .. } => {
                let mut first_err: Option<&Activity> = None;
                for act in self.failures_by_thread.values() {
                    match act {
                        Activity::Refused { .. } => return Some(act),
                        Activity::Errored { .. } if first_err.is_none() => {
                            first_err = Some(act);
                        }
                        _ => {}
                    }
                }
                first_err
            }
            WorkerPhase::Live { thread, .. }
            | WorkerPhase::Retiring { thread, .. }
            | WorkerPhase::Retired {
                thread: Some(thread),
                ..
            } => self.failures_by_thread.get(thread),
            WorkerPhase::Failed { .. } => None,
        }
    }

    fn fold_keys_for_thread(
        &mut self,
        thread: &BbThreadId,
        keys: &[String],
        clean_seq: Option<u64>,
    ) {
        let prev_fseq = self.failure_seq_by_thread.get(thread).copied().unwrap_or(0);
        let stale_outcome_seq = clean_seq.is_some_and(|s| s <= prev_fseq);
        for key in keys {
            if let Some(rest) = key.strip_prefix("fp:")
                && let Some((_, reason)) = rest.split_once(':')
            {
                let existing_refused = matches!(
                    self.failures_by_thread.get(thread),
                    Some(Activity::Refused { .. })
                );
                if !(existing_refused && stale_outcome_seq) {
                    self.failures_by_thread.insert(
                        thread.clone(),
                        Activity::Refused {
                            reason: reason.to_owned(),
                        },
                    );
                    if let Some(s) = clean_seq {
                        let slot = self
                            .failure_seq_by_thread
                            .entry(thread.clone())
                            .or_insert(0);
                        *slot = (*slot).max(s);
                    }
                }
            } else if let Some(rest) = key.strip_prefix("fe:")
                && let Some((_, error)) = rest.split_once(':')
            {
                let cur = self.failures_by_thread.get(thread);
                let blocked_by_refusal = matches!(cur, Some(Activity::Refused { .. }));
                let blocked_by_newer_err =
                    matches!(cur, Some(Activity::Errored { .. })) && stale_outcome_seq;
                if !blocked_by_refusal && !blocked_by_newer_err {
                    self.failures_by_thread.insert(
                        thread.clone(),
                        Activity::Errored {
                            error: error.to_owned(),
                        },
                    );
                    if let Some(s) = clean_seq {
                        let slot = self
                            .failure_seq_by_thread
                            .entry(thread.clone())
                            .or_insert(0);
                        *slot = (*slot).max(s);
                    }
                }
            } else if (key.starts_with("ft:") || key.starts_with("fail_ticket:"))
                && !self.failures_by_thread.contains_key(thread)
            {
                self.failures_by_thread.insert(
                    thread.clone(),
                    Activity::Errored {
                        error: "turn failed".into(),
                    },
                );
                if let Some(s) = clean_seq {
                    let slot = self
                        .failure_seq_by_thread
                        .entry(thread.clone())
                        .or_insert(0);
                    *slot = (*slot).max(s);
                }
            }
        }
    }

    pub fn observe(&mut self, phase: &WorkerPhase, obs: &Observation) -> WorkerPhase {
        match obs {
            Observation::Spawned { attempt, thread } => {
                if matches!(phase, WorkerPhase::Requested { attempt: a } if a == attempt) {
                    self.attempt = *attempt;
                    self.active_thread = Some(thread.clone());
                    self.last_activity_seq = self.seq_by_thread.get(thread).copied().unwrap_or(0);
                    self.last_idle_seq = 0;
                    self.seq_by_thread.clear();
                    let init_activity = self
                        .failures_by_thread
                        .get(thread)
                        .cloned()
                        .unwrap_or(Activity::Working);
                    return WorkerPhase::Live {
                        thread: thread.clone(),
                        activity: init_activity,
                    };
                } else if let WorkerPhase::Retired {
                    thread: None,
                    errored,
                    refused,
                } = phase
                    && *attempt == self.attempt
                {
                    self.active_thread = Some(thread.clone());
                    self.last_activity_seq = self.seq_by_thread.get(thread).copied().unwrap_or(0);
                    self.last_idle_seq = 0;
                    self.seq_by_thread.clear();
                    let mut eff_err = errored.clone();
                    let mut eff_ref = refused.clone();
                    match self.failures_by_thread.get(thread) {
                        Some(Activity::Refused { reason }) if eff_ref.is_none() => {
                            eff_ref = Some(reason.clone());
                            eff_err = None;
                        }
                        Some(Activity::Errored { error })
                            if eff_ref.is_none() && eff_err.is_none() =>
                        {
                            eff_err = Some(error.clone());
                        }
                        _ => {}
                    }
                    return WorkerPhase::Retiring {
                        thread: thread.clone(),
                        errored: eff_err,
                        refused: eff_ref,
                    };
                }
            }
            Observation::SpawnFailed { attempt, .. } => {
                if matches!(phase, WorkerPhase::Requested { attempt: a } if a == attempt) {
                    self.active_thread = None;
                    self.failures_by_thread.clear();
                    self.failure_seq_by_thread.clear();
                    self.seq_by_thread.clear();
                    self.last_activity_seq = 0;
                    self.last_idle_seq = 0;
                }
            }
            Observation::TurnsSuppressed {
                thread,
                host_seq,
                keys,
            } => {
                let matches_thread = match phase {
                    WorkerPhase::Live { thread: t, .. }
                    | WorkerPhase::Retiring { thread: t, .. } => t == thread,
                    WorkerPhase::Requested { .. } | WorkerPhase::Retired { thread: None, .. } => {
                        true
                    }
                    WorkerPhase::Retired {
                        thread: Some(_), ..
                    }
                    | WorkerPhase::Failed { .. } => false,
                };
                if matches_thread {
                    let unbound_phase = matches!(
                        phase,
                        WorkerPhase::Requested { .. } | WorkerPhase::Retired { thread: None, .. }
                    );
                    if !unbound_phase {
                        self.active_thread = Some(thread.clone());
                    }
                    let clean_seq = host_seq.filter(|&s| s > 0);
                    if !unbound_phase && clean_seq.is_some_and(|s| s <= self.last_idle_seq) {
                        return phase.clone();
                    }
                    let stale_for_live = matches!(phase, WorkerPhase::Live { .. })
                        && clean_seq.is_some_and(|s| s <= self.last_activity_seq);
                    self.fold_keys_for_thread(thread, keys, clean_seq);
                    if failure_activity_from_suppression_keys(keys).is_some()
                        && let Some(s) = clean_seq
                    {
                        if unbound_phase {
                            let slot = self.seq_by_thread.entry(thread.clone()).or_insert(0);
                            *slot = (*slot).max(s);
                        } else if matches!(phase, WorkerPhase::Live { .. }) && !stale_for_live {
                            self.last_activity_seq = self.last_activity_seq.max(s);
                        }
                    }
                    if stale_for_live {
                        return phase.clone();
                    }
                }
            }
            Observation::Activity {
                activity,
                thread,
                host_seq,
                suppress_keys,
                ..
            } => {
                if let WorkerPhase::Live {
                    thread: live_th, ..
                } = phase
                {
                    if thread.as_ref().is_some_and(|t| t != live_th) {
                        return phase.clone();
                    }
                    let clean_seq = host_seq.filter(|&s| s > 0);
                    if clean_seq.is_some_and(|s| s <= self.last_activity_seq) {
                        return phase.clone();
                    }
                    if let Some(s) = clean_seq {
                        self.last_activity_seq = s;
                        if matches!(activity, Activity::Idle) {
                            self.last_idle_seq = self.last_idle_seq.max(s);
                        }
                    }
                    self.active_thread = Some(live_th.clone());
                    self.fold_keys_for_thread(live_th, suppress_keys, clean_seq);
                    match activity {
                        Activity::Idle => {
                            self.failures_by_thread.remove(live_th);
                            self.failure_seq_by_thread.remove(live_th);
                        }
                        Activity::Refused { .. } => {
                            self.failures_by_thread
                                .insert(live_th.clone(), activity.clone());
                            if let Some(s) = clean_seq {
                                let slot = self
                                    .failure_seq_by_thread
                                    .entry(live_th.clone())
                                    .or_insert(0);
                                *slot = (*slot).max(s);
                            }
                        }
                        Activity::Errored { .. } => {
                            if !matches!(
                                self.failures_by_thread.get(live_th),
                                Some(Activity::Refused { .. })
                            ) {
                                self.failures_by_thread
                                    .insert(live_th.clone(), activity.clone());
                                if let Some(s) = clean_seq {
                                    let slot = self
                                        .failure_seq_by_thread
                                        .entry(live_th.clone())
                                        .or_insert(0);
                                    *slot = (*slot).max(s);
                                }
                            }
                        }
                        Activity::Working | Activity::Parked { .. } | Activity::Blocked { .. } => {}
                    }
                }
            }
            Observation::Archived {
                thread: obs_thread,
                attempt: obs_attempt,
            } => {
                if let WorkerPhase::Retired {
                    thread: None,
                    errored,
                    refused,
                } = phase
                {
                    let Some(obs_th) = obs_thread.as_ref() else {
                        return phase.clone();
                    };
                    if obs_attempt != &Some(self.attempt) {
                        return phase.clone();
                    }
                    self.active_thread = Some(obs_th.clone());
                    self.last_activity_seq = self.seq_by_thread.get(obs_th).copied().unwrap_or(0);
                    self.seq_by_thread.clear();
                    let mut eff_err = errored.clone();
                    let mut eff_ref = refused.clone();
                    match self.failures_by_thread.get(obs_th) {
                        Some(Activity::Refused { reason }) if eff_ref.is_none() => {
                            eff_ref = Some(reason.clone());
                            eff_err = None;
                        }
                        Some(Activity::Errored { error })
                            if eff_ref.is_none() && eff_err.is_none() =>
                        {
                            eff_err = Some(error.clone());
                        }
                        _ => {}
                    }
                    return WorkerPhase::Retired {
                        thread: Some(obs_th.clone()),
                        errored: eff_err,
                        refused: eff_ref,
                    };
                }
            }
            Observation::RuntimeMeta { .. } => {}
        }
        let mut next = phase.observe(obs);
        if let WorkerPhase::Requested { attempt } = &next {
            self.attempt = *attempt;
        }
        if let WorkerPhase::Live {
            ref thread,
            activity: Activity::Errored { .. },
        } = next
            && let Some(Activity::Refused { reason }) = self.failures_by_thread.get(thread)
        {
            next = WorkerPhase::Live {
                thread: thread.clone(),
                activity: Activity::Refused {
                    reason: reason.clone(),
                },
            };
        }
        if let Observation::Archived {
            thread: obs_thread, ..
        } = obs
            && let WorkerPhase::Retired {
                thread: ref ret_thread,
                ref mut errored,
                ref mut refused,
            } = next
        {
            if let Some(t) = obs_thread.as_ref().or(ret_thread.as_ref()) {
                self.active_thread = Some(t.clone());
            }
            let recorded = match obs_thread.as_ref().or(ret_thread.as_ref()) {
                Some(t) => self.failures_by_thread.get(t),
                None => self.activity(),
            };
            match recorded {
                Some(Activity::Refused { reason }) if refused.is_none() => {
                    *refused = Some(reason.clone());
                    *errored = None;
                }
                Some(Activity::Errored { error }) if refused.is_none() && errored.is_none() => {
                    *errored = Some(error.clone());
                }
                _ => {}
            }
        }
        next
    }

    pub fn retiring(&self, phase: &WorkerPhase) -> WorkerPhase {
        let mut next = phase.retiring();
        if let WorkerPhase::Retiring {
            ref thread,
            ref mut errored,
            ref mut refused,
        } = next
        {
            let recorded = self
                .failures_by_thread
                .get(thread)
                .or_else(|| self.activity());
            match recorded {
                Some(Activity::Refused { reason }) if refused.is_none() => {
                    *refused = Some(reason.clone());
                    *errored = None;
                }
                Some(Activity::Errored { error }) if refused.is_none() && errored.is_none() => {
                    *errored = Some(error.clone());
                }
                _ => {}
            }
        }
        next
    }
}

/// True when a provider stop/finish reason or status names a safety / policy refusal.
pub fn is_refusal_stop_reason(raw: &str) -> bool {
    let norm = raw.trim().to_ascii_lowercase().replace('-', "_");
    matches!(
        norm.as_str(),
        "refusal" | "refused" | "safety" | "content_filter" | "cyber_safeguard" | "policy"
    ) || norm.contains("refusal")
        || norm.contains("content_filter")
        || norm.contains("safeguard")
}

/// True when a provider error message or formatted category describes a safety refusal.
pub fn is_refusal_error_text(raw: &str) -> bool {
    let lower = raw.trim().to_ascii_lowercase();
    lower == "policy"
        || lower.starts_with("policy (")
        || lower.contains("refusal")
        || lower.contains("content_filter")
        || lower.contains("content filter")
        || lower.contains("cyber_safeguard")
        || lower.contains("cyber safeguard")
        || lower.contains("safety_refusal")
        || lower.contains("safety stop")
        || lower.contains("safety filter")
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
    UserAnswer {
        answers: BTreeMap<String, UserAnswer>,
    },
    PluginSubmitted {
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        detail: Option<String>,
        #[serde(default)]
        payload: Option<serde_json::Value>,
    },
    RequestAnswer {
        value: serde_json::Value,
    },
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
            Activity::Parked {
                room: Some(parked), ..
            } if parked == room => WakePlan::Covered,
            Activity::Parked { .. } => WakePlan::Doorbell(SteerDeliveryMode::SteerIfActive),
            Activity::Idle => WakePlan::Doorbell(SteerDeliveryMode::Auto),
            Activity::Blocked { .. } | Activity::Refused { .. } | Activity::Errored { .. } => {
                WakePlan::Suppress
            }
        },
        WorkerPhase::Retiring { .. } | WorkerPhase::Retired { .. } | WorkerPhase::Failed { .. } => {
            WakePlan::Suppress
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum WaveOutcome {
    Completed { summary: Body },
    Cancelled { reason: String },
    Failed { reason: String },
}

/// What the BB/T3 bridge observed about a worker. System audience only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "observed", rename_all = "snake_case")]
pub enum Observation {
    Spawned {
        attempt: u32,
        thread: BbThreadId,
    },
    SpawnFailed {
        attempt: u32,
        error: String,
    },
    Activity {
        activity: Activity,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<BbThreadId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host_seq: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        settled_seq: Option<u64>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        suppress_keys: Vec<String>,
    },
    RuntimeMeta {
        #[serde(default)]
        actual_model: Option<ModelId>,
        #[serde(default)]
        actual_account: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<BbThreadId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attempt: Option<u32>,
    },
    TurnsSuppressed {
        thread: BbThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host_seq: Option<u64>,
        #[serde(default)]
        keys: Vec<String>,
    },
    Archived {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<BbThreadId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attempt: Option<u32>,
    },
}

impl Observation {
    pub fn archived() -> Self {
        Observation::Archived {
            thread: None,
            attempt: None,
        }
    }

    pub fn archived_for(thread: BbThreadId, attempt: Option<u32>) -> Self {
        Observation::Archived {
            thread: Some(thread),
            attempt,
        }
    }

    pub fn activity(activity: Activity) -> Self {
        Observation::Activity {
            activity,
            thread: None,
            host_seq: None,
            settled_seq: None,
            suppress_keys: Vec::new(),
        }
    }

    pub fn activity_with_bounds(
        activity: Activity,
        thread: BbThreadId,
        settled_seq: Option<u64>,
        suppress_keys: Vec<String>,
    ) -> Self {
        Self::activity_with_seq_and_bounds(activity, thread, None, settled_seq, suppress_keys)
    }

    pub fn activity_with_seq_and_bounds(
        activity: Activity,
        thread: BbThreadId,
        host_seq: Option<u64>,
        settled_seq: Option<u64>,
        suppress_keys: Vec<String>,
    ) -> Self {
        Observation::Activity {
            activity,
            thread: Some(thread),
            host_seq: host_seq.filter(|&s| s > 0),
            settled_seq: settled_seq.filter(|&s| s > 0),
            suppress_keys,
        }
    }

    pub fn runtime_meta(
        actual_model: Option<ModelId>,
        actual_account: Option<String>,
        thread: Option<BbThreadId>,
        attempt: Option<u32>,
    ) -> Self {
        Observation::RuntimeMeta {
            actual_model,
            actual_account,
            thread,
            attempt,
        }
    }

    pub fn turns_suppressed(thread: BbThreadId, keys: Vec<String>) -> Self {
        Self::turns_suppressed_with_seq(thread, None, keys)
    }

    pub fn turns_suppressed_with_seq(
        thread: BbThreadId,
        host_seq: Option<u64>,
        keys: Vec<String>,
    ) -> Self {
        Observation::TurnsSuppressed {
            thread,
            host_seq: host_seq.filter(|&s| s > 0),
            keys,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WaveObserved {
    #[serde(default = "default_true")]
    pub applied: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed: Option<Observation>,
}

impl Default for WaveObserved {
    fn default() -> Self {
        Self::APPLIED
    }
}

impl WaveObserved {
    pub const APPLIED: Self = Self {
        applied: true,
        committed: None,
    };
    pub const NOOP: Self = Self {
        applied: false,
        committed: None,
    };
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
    /// Explicitly permit `WaveOutcome::Completed` even when workers were refused or errored.
    #[serde(default)]
    pub allow_incomplete: bool,
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

/// Aggregate worker status counts across a wave so safety refusals and errors
/// are always visible beside active and retired counts.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WaveCounts {
    pub total: usize,
    pub requested: usize,
    pub working: usize,
    pub parked: usize,
    pub idle: usize,
    pub blocked: usize,
    pub refused: usize,
    pub errored: usize,
    pub failed: usize,
    pub retiring: usize,
    pub retired_clean: usize,
    pub retired_refused: usize,
    pub retired_errored: usize,
}

impl WaveCounts {
    pub fn from_phases<'a>(phases: impl IntoIterator<Item = &'a WorkerPhase>) -> Self {
        let mut counts = WaveCounts::default();
        for phase in phases {
            counts.total += 1;
            match phase {
                WorkerPhase::Requested { .. } => counts.requested += 1,
                WorkerPhase::Live { activity, .. } => match activity {
                    Activity::Working => counts.working += 1,
                    Activity::Parked { .. } => counts.parked += 1,
                    Activity::Idle => counts.idle += 1,
                    Activity::Blocked { .. } => counts.blocked += 1,
                    Activity::Refused { .. } => counts.refused += 1,
                    Activity::Errored { .. } => counts.errored += 1,
                },
                WorkerPhase::Retiring { .. } => counts.retiring += 1,
                WorkerPhase::Retired {
                    errored, refused, ..
                } => {
                    if refused.is_some() {
                        counts.retired_refused += 1;
                    } else if errored.is_some() {
                        counts.retired_errored += 1;
                    } else {
                        counts.retired_clean += 1;
                    }
                }
                WorkerPhase::Failed { .. } => counts.failed += 1,
            }
        }
        counts
    }
}

/// Explicit requested vs. actual runtime provenance for a worker (model, paying account, run stamp).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WorkerRuntimeMeta {
    #[serde(default)]
    pub requested_model: Option<ModelId>,
    #[serde(default)]
    pub actual_model: Option<ModelId>,
    #[serde(default)]
    pub requested_account: Option<String>,
    #[serde(default)]
    pub actual_account: Option<String>,
    #[serde(default)]
    pub run_stamp: Option<String>,
}

impl WorkerRuntimeMeta {
    pub fn account_mismatch(&self) -> bool {
        match (
            self.requested_account.as_deref(),
            self.actual_account.as_deref(),
        ) {
            (Some(req), Some(act)) => req != act,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct WaveSnapshot {
    pub spec: WaveSpec,
    pub status: WaveStatus,
    pub workers: Vec<(RoleName, AgentSpec, WorkerPhase)>,
    #[serde(default)]
    pub counts: WaveCounts,
    #[serde(default)]
    pub worker_meta: BTreeMap<RoleName, WorkerRuntimeMeta>,
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
    Started {
        spec: WaveSpec,
        room: RoomId,
        board: BoardId,
    },
    WorkerRequested {
        role: RoleName,
        agent: AgentSpec,
        brief: Body,
        attempt: u32,
        #[serde(default)]
        output_dir: Option<PathBuf>,
        #[serde(default)]
        writes: Vec<PathBuf>,
        #[serde(default)]
        return_schema: Option<serde_json::Value>,
    },
    WorkerObserved {
        role: RoleName,
        observation: Observation,
    },
    Told {
        role: RoleName,
        body: Body,
        mode: SteerDeliveryMode,
    },
    InteractionResolved {
        role: RoleName,
        interaction: BbInteractionId,
        resolution: InteractionResolution,
    },
    WorkerRetiring {
        role: RoleName,
        reason: Option<String>,
    },
    Finished {
        outcome: WaveOutcome,
    },
}
