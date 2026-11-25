//! Wave decider (`Waves`), `AgentHost` port, and `WaveConductor` reactor.

pub mod router;
pub mod workspace;

pub use rex_wave_runtime::BoxFut;
pub use router::{HostRouter, RoutedProvider, route_provider, route_thread};
pub use workspace::{
    BbProjectLocation, ProjectWorkspaceLocation, default_bb_data_dir, expand_tilde,
    looks_like_workspace_path, lookup_bb_project_location, normalize_workspace_path_key,
    resolve_project_workspace_location,
};

use rex_wave_api::board::{BoardEnsure, BoardId, project_board};
use rex_wave_api::room::{
    Body, Mentions, PostOrigin, RoomEnsure, RoomEvent, RoomId, RoomJoin, RoomOrigin, RoomPost,
    RoomRef, RoomScope,
};
use rex_wave_api::wave::*;
use rex_wave_api::*;
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta, LocalClient, Reactor};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn wave_room(slug: &WaveSlug, project: &BbProjectId) -> RoomId {
    let name =
        ChannelName::parse(format!("wave-{slug}")).expect("wave slug is a valid channel suffix");
    RoomId::Channel {
        scope: RoomScope::Project {
            project: project.clone(),
        },
        name,
    }
}

pub const MAX_SPAWN_ATTEMPTS: u32 = 3;
pub const MAX_RETIRE_ATTEMPTS: u32 = 3;

/// Builds a collision-free idempotency key for final-turn room mirroring while
/// preserving the `wave:<wave>:turn:` prefix.
pub fn mirror_idem_key(
    wave: &WaveSlug,
    role: &RoleName,
    thread: &HostThreadId,
    anchor: &str,
) -> Result<IdemKey, InvalidId> {
    let raw = format!("wave:{wave}:turn:{role}:{thread}:{anchor}");
    if raw.len() <= 256 {
        return IdemKey::parse(raw);
    }
    let mut hasher = Sha256::new();
    hasher.update(thread.as_str().as_bytes());
    hasher.update(b":");
    hasher.update(anchor.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    IdemKey::parse(format!("wave:{wave}:turn:{role}:h{hex}"))
}

/// Pre-PR-27 BB turn-mirror idempotency key for inputs exceeding 256 bytes,
/// preserved solely to recognize already-journaled rows across upgrades.
pub fn legacy_truncated_mirror_idem_key(
    wave: &WaveSlug,
    role: &RoleName,
    thread: &HostThreadId,
    anchor: &str,
) -> Option<IdemKey> {
    let raw = format!("wave:{wave}:turn:{role}:{thread}:{anchor}");
    if raw.len() <= 256 {
        return None;
    }
    let mut end = 256usize;
    while !raw.is_char_boundary(end) {
        end -= 1;
    }
    IdemKey::parse(&raw[..end]).ok()
}

pub fn is_legacy_mirror_idem_key(idem: &IdemKey) -> bool {
    idem.as_str().starts_with("wave:") && idem.as_str().contains(":turn:")
}

/// Classifies whether `idem` matches an existing pre-upgrade BB turn-mirror row for
/// `(wave, role, thread)`. Returns `Some(true)` for an exact full start-anchor match
/// (`s<turn_start_seq>`), `Some(false)` for a 256-byte-truncated start or end key that
/// requires chronological turn-to-post reconciliation, or `None` when `idem` cannot
/// belong to this completed turn. Start-anchored keys (`s<N>`) never match without a
/// recovered `turn_start_seq`.
pub fn classify_legacy_bb_mirror_idem(
    idem: &IdemKey,
    wave: &WaveSlug,
    role: &RoleName,
    thread: &HostThreadId,
    turn_start_seq: Option<u64>,
    turn_end_seq: u64,
) -> Option<bool> {
    if let Some(s) = turn_start_seq {
        let start_anchor = format!("s{s}");
        if mirror_idem_key(wave, role, thread, &start_anchor)
            .ok()
            .as_ref()
            == Some(idem)
        {
            return Some(true);
        }
        if legacy_truncated_mirror_idem_key(wave, role, thread, &start_anchor).as_ref()
            == Some(idem)
        {
            return Some(false);
        }
    }

    let end_anchor = format!("e{turn_end_seq}");
    if legacy_truncated_mirror_idem_key(wave, role, thread, &end_anchor).as_ref() == Some(idem) {
        return Some(false);
    }
    None
}

pub fn worker_step(phase: &WorkerPhase, obs: &Observation) -> Result<WorkerPhase, String> {
    match (phase, obs) {
        (WorkerPhase::Requested { attempt }, Observation::Spawned { attempt: a, thread })
            if a == attempt =>
        {
            Ok(WorkerPhase::Live {
                thread: thread.clone(),
                activity: Activity::Working,
            })
        }
        (WorkerPhase::Requested { attempt }, Observation::SpawnFailed { attempt: a, error })
            if a == attempt =>
        {
            if *attempt < MAX_SPAWN_ATTEMPTS {
                Ok(WorkerPhase::Requested {
                    attempt: attempt + 1,
                })
            } else {
                Ok(WorkerPhase::Failed {
                    error: error.clone(),
                })
            }
        }
        (
            WorkerPhase::Live {
                thread,
                activity: Activity::Refused { reason },
            },
            Observation::Activity {
                activity: Activity::Errored { .. },
            },
        ) => Ok(WorkerPhase::Live {
            thread: thread.clone(),
            activity: Activity::Refused {
                reason: reason.clone(),
            },
        }),
        (WorkerPhase::Live { thread, .. }, Observation::Activity { activity }) => {
            Ok(WorkerPhase::Live {
                thread: thread.clone(),
                activity: activity.clone(),
            })
        }
        (
            WorkerPhase::Live { .. } | WorkerPhase::Retiring { .. } | WorkerPhase::Requested { .. },
            Observation::Archived,
        ) => Ok(phase.observe(obs)),
        _ => Err(format!(
            "observation {obs:?} does not apply in phase {phase:?}"
        )),
    }
}

pub fn doorbell_text(room: &RoomId, seq: Seq, from: &Principal, body: &Body) -> Body {
    let label = match room {
        RoomId::Channel { name, .. } => format!("#{name}"),
        RoomId::Direct { .. } => "dm".to_owned(),
    };
    let prefix = format!("[{label} seq={} from {from}] ", seq.0);
    let suffix = "  (reply with room_post; wait to park)";
    let max_body = (64 * 1024usize).saturating_sub(prefix.len() + suffix.len());
    let text = truncate_with_notice(body.as_str(), max_body);
    Body::parse(format!("{prefix}{text}{suffix}")).expect("doorbell fits within 64 KiB")
}

fn bounded_body(raw: String) -> Body {
    let trimmed = truncate_with_notice(&raw, 64 * 1024);
    Body::parse(trimmed).expect("non-empty bounded body")
}

/// Normalize a wave/worker-owned lexical path (`output_dir` or `writes` entry).
/// Rejects empty paths, `"."`-only paths, and any `..` parent traversal.
pub fn normalize_owned_path(path: &Path, what: &str) -> Result<PathBuf, ApiError> {
    rex_wave_fs::normalize_no_parent(path).map_err(|e| {
        ApiError::Invalid(InvalidId {
            what: what.into(),
            reason: e.to_string(),
        })
    })
}

/// Resolve a worker's effective output directory: explicit `worker_output_dir`
/// wins, otherwise `<wave_output_dir>/<role>` when `wave_output_dir` is set.
pub fn effective_worker_output_dir(
    wave_output_dir: Option<&Path>,
    role: &RoleName,
    worker_output_dir: Option<&Path>,
) -> Result<Option<PathBuf>, ApiError> {
    if let Some(dir) = worker_output_dir {
        return Ok(Some(normalize_owned_path(dir, "worker output_dir")?));
    }
    if let Some(base) = wave_output_dir {
        let norm = normalize_owned_path(base, "wave output_dir")?;
        return Ok(Some(norm.join(role.as_str())));
    }
    Ok(None)
}

/// Component-wise overlap check between two normalized paths.
/// Siblings (`out/w1` and `out/w2`) do NOT overlap; identical or ancestor/descendant paths DO.
pub fn paths_overlap(a: &Path, b: &Path) -> bool {
    a == b || a.starts_with(b) || b.starts_with(a)
}

fn validate_return_schema(schema: Option<&serde_json::Value>) -> Result<(), ApiError> {
    if let Some(val) = schema
        && !val.is_object()
    {
        return Err(ApiError::Invalid(InvalidId {
            what: "worker return_schema".into(),
            reason: "return_schema must be a JSON object".into(),
        }));
    }
    Ok(())
}

fn normalized_worker_ownership(
    wave_output_dir: Option<&Path>,
    worker: &WorkerSpec,
) -> Result<(Option<PathBuf>, Vec<PathBuf>), ApiError> {
    validate_return_schema(worker.return_schema.as_ref())?;
    let output_dir =
        effective_worker_output_dir(wave_output_dir, &worker.role, worker.output_dir.as_deref())?;
    let mut writes = Vec::with_capacity(worker.writes.len());
    for p in &worker.writes {
        writes.push(normalize_owned_path(p, "worker writes")?);
    }
    Ok((output_dir, writes))
}

fn owned_paths_of<'a>(output_dir: Option<&'a PathBuf>, writes: &'a [PathBuf]) -> Vec<&'a Path> {
    output_dir
        .into_iter()
        .map(PathBuf::as_path)
        .chain(writes.iter().map(PathBuf::as_path))
        .collect()
}

fn check_worker_path_conflicts(
    role_a: &RoleName,
    paths_a: &[&Path],
    role_b: &RoleName,
    paths_b: &[&Path],
) -> Result<(), ApiError> {
    for pa in paths_a {
        for pb in paths_b {
            if paths_overlap(pa, pb) {
                return Err(ApiError::conflict(format!(
                    "structural one-writer violation: @{role_a} ({}) overlaps with @{role_b} ({})",
                    pa.display(),
                    pb.display()
                )));
            }
        }
    }
    Ok(())
}

pub struct Waves;

#[derive(Clone, Debug)]
struct WorkerRecord {
    agent: AgentSpec,
    brief: Body,
    phase: WorkerPhase,
    resolved_interaction: Option<(BbInteractionId, InteractionResolution)>,
    output_dir: Option<PathBuf>,
    writes: Vec<PathBuf>,
    return_schema: Option<serde_json::Value>,
    meta: WorkerRuntimeMeta,
    suppressed_thread: Option<BbThreadId>,
    suppressed_turns: BTreeSet<String>,
}

#[derive(Clone, Debug, Default)]
pub struct WaveState {
    spec: Option<WaveSpec>,
    room: Option<RoomId>,
    board: Option<BoardId>,
    opener: Option<Principal>,
    started_at: UnixMs,
    workers: BTreeMap<RoleName, WorkerRecord>,
    status: Option<WaveStatus>,
    head: Seq,
}

impl WaveState {
    fn require_started<'a>(&'a self, wave: &WaveSlug) -> Result<&'a WaveSpec, ApiError> {
        self.spec.as_ref().ok_or_else(|| ApiError::NotFound {
            what: format!("wave {wave}"),
        })
    }

    fn is_live_member(&self, wave: &WaveSlug, who: &Principal) -> bool {
        matches!(
            who,
            Principal::Worker { wave: w, role }
                if w == wave && self.workers.get(role).is_some_and(|r| matches!(r.phase, WorkerPhase::Live { .. }))
        )
    }

    fn authorize_wave_mutation(
        &self,
        wave: &WaveSlug,
        who: &Principal,
        action: &str,
    ) -> Result<(), ApiError> {
        if who.is_operator_or_system() || self.is_live_member(wave, who) {
            Ok(())
        } else {
            Err(ApiError::Denied {
                principal: who.to_string(),
                action: format!("{action} in wave {wave}"),
            })
        }
    }
}

impl Decider for Waves {
    type State = WaveState;
    type Cmd = WaveCommand;
    type Event = WaveEvent;

    fn address(cmd: &WaveCommand, _ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        let wave = match cmd {
            WaveCommand::WaveStart(c) => &c.spec.slug,
            WaveCommand::WaveAddWorker(c) => &c.wave,
            WaveCommand::WaveRetireWorker(c) => &c.wave,
            WaveCommand::WaveTell(c) => &c.wave,
            WaveCommand::WaveResolve(c) => &c.wave,
            WaveCommand::WaveFinish(c) => &c.wave,
            WaveCommand::WaveObserve(c) => &c.wave,
            WaveCommand::WaveView(c) => &c.wave,
        };
        Ok(StreamId::Wave { wave: wave.clone() })
    }

    fn idem(cmd: &WaveCommand) -> Option<IdemKey> {
        match cmd {
            WaveCommand::WaveStart(c) => c.idem.clone(),
            _ => None,
        }
    }

    fn decide(
        state: &WaveState,
        cmd: &WaveCommand,
        ctx: &DecideCtx,
    ) -> Result<Vec<WaveEvent>, ApiError> {
        match cmd {
            WaveCommand::WaveStart(c) => {
                if let Some(base) = c.spec.output_dir.as_deref() {
                    normalize_owned_path(base, "wave output_dir")?;
                }
                let mut seen_roles = BTreeSet::new();
                let mut resolved_workers: Vec<(Option<PathBuf>, Vec<PathBuf>)> =
                    Vec::with_capacity(c.spec.workers.len());
                for w in &c.spec.workers {
                    if !seen_roles.insert(&w.role) {
                        return Err(ApiError::Invalid(InvalidId {
                            what: "worker role".into(),
                            reason: format!("duplicate role {}", w.role),
                        }));
                    }
                    let owned = normalized_worker_ownership(c.spec.output_dir.as_deref(), w)?;
                    resolved_workers.push(owned);
                }
                for (i, (out_a, writes_a)) in resolved_workers.iter().enumerate() {
                    let paths_a = owned_paths_of(out_a.as_ref(), writes_a);
                    for (j, (out_b, writes_b)) in resolved_workers.iter().enumerate().skip(i + 1) {
                        let paths_b = owned_paths_of(out_b.as_ref(), writes_b);
                        check_worker_path_conflicts(
                            &c.spec.workers[i].role,
                            &paths_a,
                            &c.spec.workers[j].role,
                            &paths_b,
                        )?;
                    }
                }
                if let Some(existing) = &state.spec {
                    if existing == &c.spec {
                        return Ok(vec![]);
                    }
                    return Err(ApiError::conflict(format!(
                        "wave {} already exists with a different spec",
                        c.spec.slug
                    )));
                }
                let room = wave_room(&c.spec.slug, &c.spec.project);
                let board = project_board(&c.spec.project);
                let mut events = Vec::with_capacity(1 + c.spec.workers.len());
                events.push(WaveEvent::Started {
                    spec: c.spec.clone(),
                    room,
                    board,
                });
                for (w, (output_dir, writes)) in c.spec.workers.iter().zip(resolved_workers) {
                    let mut agent = w.agent.clone().unwrap_or_else(|| c.spec.defaults.clone());
                    if agent.account.is_none() {
                        agent.account = c.spec.defaults.account.clone();
                    }
                    events.push(WaveEvent::WorkerRequested {
                        role: w.role.clone(),
                        agent,
                        brief: w.brief.clone(),
                        attempt: 1,
                        output_dir,
                        writes,
                        return_schema: w.return_schema.clone(),
                    });
                }
                Ok(events)
            }
            WaveCommand::WaveAddWorker(c) => {
                let spec = state.require_started(&c.wave)?;
                state.authorize_wave_mutation(&c.wave, ctx.by(), "add worker")?;
                if !matches!(state.status, Some(WaveStatus::Running)) {
                    return Err(ApiError::conflict(format!(
                        "wave {} is already finished",
                        c.wave
                    )));
                }
                if state.workers.contains_key(&c.worker.role) {
                    return Err(ApiError::conflict(format!(
                        "worker @{} already exists in wave {}",
                        c.worker.role, c.wave
                    )));
                }
                let (output_dir, writes) =
                    normalized_worker_ownership(spec.output_dir.as_deref(), &c.worker)?;
                let new_paths = owned_paths_of(output_dir.as_ref(), &writes);
                for (existing_role, existing_rec) in &state.workers {
                    let existing_paths =
                        owned_paths_of(existing_rec.output_dir.as_ref(), &existing_rec.writes);
                    check_worker_path_conflicts(
                        existing_role,
                        &existing_paths,
                        &c.worker.role,
                        &new_paths,
                    )?;
                }
                let mut agent = c
                    .worker
                    .agent
                    .clone()
                    .unwrap_or_else(|| spec.defaults.clone());
                if matches!(ctx.by(), Principal::Worker { .. })
                    && let Some(req_acct) = agent.account.as_deref()
                    && Some(req_acct) != spec.defaults.account.as_deref()
                {
                    return Err(ApiError::Denied {
                        principal: ctx.by().to_string(),
                        action: format!("override wave paying account in wave {}", c.wave),
                    });
                }
                if agent.account.is_none() {
                    agent.account = spec.defaults.account.clone();
                }
                Ok(vec![WaveEvent::WorkerRequested {
                    role: c.worker.role.clone(),
                    agent,
                    brief: c.worker.brief.clone(),
                    attempt: 1,
                    output_dir,
                    writes,
                    return_schema: c.worker.return_schema.clone(),
                }])
            }
            WaveCommand::WaveRetireWorker(c) => {
                state.require_started(&c.wave)?;
                state.authorize_wave_mutation(&c.wave, ctx.by(), "retire worker")?;
                let rec = state
                    .workers
                    .get(&c.role)
                    .ok_or_else(|| ApiError::NotFound {
                        what: format!("worker @{}/{}", c.wave, c.role),
                    })?;
                match rec.phase {
                    WorkerPhase::Live { .. } | WorkerPhase::Requested { .. } => {
                        Ok(vec![WaveEvent::WorkerRetiring {
                            role: c.role.clone(),
                            reason: c.reason.clone(),
                        }])
                    }
                    WorkerPhase::Retiring { .. }
                    | WorkerPhase::Retired { .. }
                    | WorkerPhase::Failed { .. } => Ok(vec![]),
                }
            }
            WaveCommand::WaveTell(c) => {
                state.require_started(&c.wave)?;
                if let Principal::Worker {
                    wave: caller_wave, ..
                } = ctx.by()
                    && (caller_wave != &c.wave || !state.is_live_member(&c.wave, ctx.by()))
                {
                    return Err(ApiError::Denied {
                        principal: ctx.by().to_string(),
                        action: format!("steer worker in wave {}", c.wave),
                    });
                }
                let rec = state
                    .workers
                    .get(&c.role)
                    .ok_or_else(|| ApiError::NotFound {
                        what: format!("worker @{}/{}", c.wave, c.role),
                    })?;
                if !matches!(rec.phase, WorkerPhase::Live { .. }) {
                    return Err(ApiError::conflict(format!(
                        "worker @{}/{} is not live ({:?})",
                        c.wave, c.role, rec.phase
                    )));
                }
                Ok(vec![WaveEvent::Told {
                    role: c.role.clone(),
                    body: c.body.clone(),
                    mode: c.mode,
                }])
            }
            WaveCommand::WaveResolve(c) => {
                state.require_started(&c.wave)?;
                let rec = state
                    .workers
                    .get(&c.role)
                    .ok_or_else(|| ApiError::NotFound {
                        what: format!("worker @{}/{}", c.wave, c.role),
                    })?;
                match &rec.phase {
                    WorkerPhase::Live {
                        activity: Activity::Blocked { interaction },
                        ..
                    } => {
                        if interaction.id != c.interaction {
                            return Err(ApiError::conflict(format!(
                                "worker @{}/{} is blocked on {}, not {}",
                                c.wave, c.role, interaction.id, c.interaction
                            )));
                        }
                        if let Some((prev_id, prev_res)) = &rec.resolved_interaction
                            && prev_id == &c.interaction
                        {
                            if prev_res == &c.resolution {
                                return Ok(vec![]);
                            }
                            return Err(ApiError::conflict(format!(
                                "interaction {} was already resolved for @{}/{}",
                                c.interaction, c.wave, c.role
                            )));
                        }
                        Ok(vec![WaveEvent::InteractionResolved {
                            role: c.role.clone(),
                            interaction: c.interaction.clone(),
                            resolution: c.resolution.clone(),
                        }])
                    }
                    other => Err(ApiError::conflict(format!(
                        "worker @{}/{} has no pending interaction ({other:?})",
                        c.wave, c.role
                    ))),
                }
            }
            WaveCommand::WaveFinish(c) => {
                state.require_started(&c.wave)?;
                state.authorize_wave_mutation(&c.wave, ctx.by(), "finish wave")?;
                if let Some(WaveStatus::Finished { outcome }) = &state.status {
                    if outcome == &c.outcome {
                        return Ok(vec![]);
                    }
                    return Err(ApiError::conflict(format!(
                        "wave {} is already finished",
                        c.wave
                    )));
                }
                if c.allow_incomplete && !ctx.by().is_operator() {
                    return Err(ApiError::Denied {
                        principal: ctx.by().to_string(),
                        action: format!("override incomplete finish gate in wave {}", c.wave),
                    });
                }
                if matches!(c.outcome, WaveOutcome::Completed { .. }) && !c.allow_incomplete {
                    let mut refused = Vec::new();
                    let mut errored = Vec::new();
                    for (role, rec) in &state.workers {
                        if rec.phase.refusal().is_some() {
                            refused.push(format!("@{role}"));
                        } else if rec.phase.error().is_some() {
                            errored.push(format!("@{role}"));
                        }
                    }
                    if !refused.is_empty() || !errored.is_empty() {
                        let mut parts = Vec::new();
                        if !refused.is_empty() {
                            parts.push(format!("refused: {}", refused.join(", ")));
                        }
                        if !errored.is_empty() {
                            parts.push(format!("errored: {}", errored.join(", ")));
                        }
                        let override_hint = if ctx.by().is_operator() {
                            "pass --allow-incomplete to override"
                        } else {
                            "an operator must pass --allow-incomplete to override"
                        };
                        return Err(ApiError::conflict(format!(
                            "cannot finish wave {} as completed with incomplete workers ({}); {override_hint}",
                            c.wave,
                            parts.join("; ")
                        )));
                    }
                }
                let mut events = vec![WaveEvent::Finished {
                    outcome: c.outcome.clone(),
                }];
                for (role, rec) in &state.workers {
                    if matches!(
                        rec.phase,
                        WorkerPhase::Live { .. } | WorkerPhase::Requested { .. }
                    ) {
                        events.push(WaveEvent::WorkerRetiring {
                            role: role.clone(),
                            reason: Some("wave finished".into()),
                        });
                    }
                }
                Ok(events)
            }
            WaveCommand::WaveObserve(c) => {
                if !matches!(ctx.by(), Principal::System { .. }) {
                    return Err(ApiError::Denied {
                        principal: ctx.by().to_string(),
                        action: "observe worker".into(),
                    });
                }
                state.require_started(&c.wave)?;
                let Some(rec) = state.workers.get(&c.role) else {
                    return Err(ApiError::NotFound {
                        what: format!("worker @{}/{}", c.wave, c.role),
                    });
                };
                if let Observation::RuntimeMeta {
                    actual_model,
                    actual_account,
                } = &c.observation
                {
                    if !matches!(
                        rec.phase,
                        WorkerPhase::Requested { .. }
                            | WorkerPhase::Live { .. }
                            | WorkerPhase::Retiring { .. }
                    ) {
                        return Ok(vec![]);
                    }
                    let model_changed = actual_model.is_some()
                        && rec.meta.actual_model.as_ref() != actual_model.as_ref();
                    let account_changed = actual_account.is_some()
                        && rec.meta.actual_account.as_ref() != actual_account.as_ref();
                    if !model_changed && !account_changed {
                        return Ok(vec![]);
                    }
                    return Ok(vec![WaveEvent::WorkerObserved {
                        role: c.role.clone(),
                        observation: c.observation.clone(),
                    }]);
                }
                if let Observation::TurnsSuppressed { thread, keys } = &c.observation {
                    let matches_thread = match &rec.phase {
                        WorkerPhase::Live { thread: t, .. }
                        | WorkerPhase::Retiring { thread: t, .. } => t == thread,
                        WorkerPhase::Requested { .. } => true,
                        WorkerPhase::Retired { .. } | WorkerPhase::Failed { .. } => false,
                    };
                    if !matches_thread {
                        return Ok(vec![]);
                    }
                    let thread_matches_existing = rec.suppressed_thread.as_ref() == Some(thread);
                    let mut clean: Vec<String> = Vec::new();
                    for key in keys {
                        let trimmed = key.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        let already_recorded =
                            thread_matches_existing && rec.suppressed_turns.contains(trimmed);
                        if !already_recorded && !clean.iter().any(|existing| existing == trimmed) {
                            clean.push(trimmed.to_owned());
                        }
                    }
                    if clean.is_empty() {
                        return Ok(vec![]);
                    }
                    return Ok(vec![WaveEvent::WorkerObserved {
                        role: c.role.clone(),
                        observation: Observation::TurnsSuppressed {
                            thread: thread.clone(),
                            keys: clean,
                        },
                    }]);
                }
                let Ok(next) = worker_step(&rec.phase, &c.observation) else {
                    return Ok(vec![]);
                };
                if next == rec.phase {
                    return Ok(vec![]);
                }
                let mut events = vec![WaveEvent::WorkerObserved {
                    role: c.role.clone(),
                    observation: c.observation.clone(),
                }];
                // Emit explicit retry `WorkerRequested` so SidebarProjection and WaveConductor stay in lockstep.
                if let WorkerPhase::Requested { attempt } = next {
                    events.push(WaveEvent::WorkerRequested {
                        role: c.role.clone(),
                        agent: rec.agent.clone(),
                        brief: rec.brief.clone(),
                        attempt,
                        output_dir: rec.output_dir.clone(),
                        writes: rec.writes.clone(),
                        return_schema: rec.return_schema.clone(),
                    });
                }
                Ok(events)
            }
            WaveCommand::WaveView(c) => {
                state.require_started(&c.wave)?;
                Ok(vec![])
            }
        }
    }

    fn evolve(state: &mut WaveState, event: &WaveEvent, meta: &EntryMeta) {
        state.head = meta.seq;
        match event {
            WaveEvent::Started { spec, room, board } => {
                state.spec = Some(spec.clone());
                state.room = Some(room.clone());
                state.board = Some(board.clone());
                state.opener = Some(meta.by.clone());
                state.started_at = meta.at;
                state.status = Some(WaveStatus::Running);
            }
            WaveEvent::WorkerRequested {
                role,
                agent,
                brief,
                attempt,
                output_dir,
                writes,
                return_schema,
            } => {
                let wave_slug = state.spec.as_ref().map_or("wave", |s| s.slug.as_str());
                let run_stamp = format!(
                    "{wave_slug}:{role}:attempt-{attempt}:at-{}",
                    state.started_at.0
                );
                state.workers.insert(
                    role.clone(),
                    WorkerRecord {
                        agent: agent.clone(),
                        brief: brief.clone(),
                        phase: WorkerPhase::Requested { attempt: *attempt },
                        resolved_interaction: None,
                        output_dir: output_dir.clone(),
                        writes: writes.clone(),
                        return_schema: return_schema.clone(),
                        meta: WorkerRuntimeMeta {
                            requested_model: agent.model.clone(),
                            actual_model: None,
                            requested_account: agent.account.clone(),
                            actual_account: None,
                            run_stamp: Some(run_stamp),
                        },
                        suppressed_thread: None,
                        suppressed_turns: BTreeSet::new(),
                    },
                );
            }
            WaveEvent::WorkerObserved { role, observation } => {
                if let Some(rec) = state.workers.get_mut(role) {
                    match observation {
                        Observation::RuntimeMeta {
                            actual_model,
                            actual_account,
                        } => {
                            if let Some(model) = actual_model {
                                rec.meta.actual_model = Some(model.clone());
                            }
                            if let Some(account) = actual_account {
                                rec.meta.actual_account = Some(account.clone());
                            }
                        }
                        Observation::TurnsSuppressed { thread, keys } => {
                            if rec.suppressed_thread.as_ref() != Some(thread) {
                                rec.suppressed_thread = Some(thread.clone());
                                rec.suppressed_turns.clear();
                            }
                            for key in keys {
                                if !key.is_empty() {
                                    rec.suppressed_turns.insert(key.clone());
                                }
                            }
                        }
                        other => {
                            if let Observation::Spawned { thread, .. } = other
                                && rec.suppressed_thread.as_ref() != Some(thread)
                            {
                                rec.suppressed_thread = Some(thread.clone());
                                rec.suppressed_turns.clear();
                            }
                            rec.phase = rec.phase.observe(other);
                            rec.resolved_interaction = None;
                        }
                    }
                }
            }
            WaveEvent::WorkerRetiring { role, .. } => {
                if let Some(rec) = state.workers.get_mut(role) {
                    rec.phase = rec.phase.retiring();
                    rec.resolved_interaction = None;
                }
            }
            WaveEvent::Finished { outcome } => {
                state.status = Some(WaveStatus::Finished {
                    outcome: outcome.clone(),
                });
                for rec in state.workers.values_mut() {
                    rec.phase = rec.phase.retiring();
                    rec.resolved_interaction = None;
                }
            }
            WaveEvent::InteractionResolved {
                role,
                interaction,
                resolution,
            } => {
                if let Some(rec) = state.workers.get_mut(role) {
                    rec.resolved_interaction = Some((interaction.clone(), resolution.clone()));
                }
            }
            WaveEvent::Told { .. } => {}
        }
    }

    fn reply(state: &WaveState, cmd: WaveCommand, _committed: &[Envelope]) -> WireReply {
        match cmd {
            WaveCommand::WaveStart(c) => {
                let room = state
                    .room
                    .clone()
                    .unwrap_or_else(|| wave_room(&c.spec.slug, &c.spec.project));
                let board = state
                    .board
                    .clone()
                    .unwrap_or_else(|| project_board(&c.spec.project));
                WireReply::WaveStart(WaveStarted {
                    wave: c.spec.slug,
                    room,
                    board,
                })
            }
            WaveCommand::WaveAddWorker(_) => WireReply::WaveAddWorker(Ack),
            WaveCommand::WaveRetireWorker(_) => WireReply::WaveRetireWorker(Ack),
            WaveCommand::WaveTell(_) => WireReply::WaveTell(Ack),
            WaveCommand::WaveResolve(_) => WireReply::WaveResolve(Ack),
            WaveCommand::WaveFinish(_) => WireReply::WaveFinish(Ack),
            WaveCommand::WaveObserve(_) => WireReply::WaveObserve(Ack),
            WaveCommand::WaveView(c) => {
                let spec = state.spec.clone().expect("validated in decide");
                let status = state.status.clone().unwrap_or(WaveStatus::Running);
                let workers: Vec<_> = state
                    .workers
                    .iter()
                    .map(|(role, rec)| (role.clone(), rec.agent.clone(), rec.phase.clone()))
                    .collect();
                let counts = WaveCounts::from_phases(state.workers.values().map(|rec| &rec.phase));
                let worker_meta = state
                    .workers
                    .iter()
                    .map(|(role, rec)| (role.clone(), rec.meta.clone()))
                    .collect();
                let _ = c;
                WireReply::WaveView(WaveSnapshot {
                    spec,
                    status,
                    workers,
                    counts,
                    worker_meta,
                })
            }
        }
    }

    fn wrap(event: WaveEvent) -> Event {
        Event::Wave(event)
    }

    fn unwrap(event: &Event) -> Option<&WaveEvent> {
        match event {
            Event::Wave(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HostError {
    #[error("host unreachable: {0}")]
    Unreachable(String),
    #[error("host rejected: {0}")]
    Rejected(String),
}

#[derive(Clone, Debug)]
pub struct SpawnRequest {
    pub wave: WaveSlug,
    pub role: RoleName,
    pub attempt: u32,
    pub project: BbProjectId,
    pub agent: AgentSpec,
    pub room: RoomId,
    pub prompt: Body,
    pub title: String,
    pub run_stamp: String,
    pub output_dir: Option<PathBuf>,
    pub writes: Vec<PathBuf>,
    pub return_schema: Option<serde_json::Value>,
}

pub trait AgentHost: Send + Sync + 'static {
    fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<HostThreadId, HostError>>;
    fn tell(
        &self,
        thread: &HostThreadId,
        body: &Body,
        mode: SteerDeliveryMode,
    ) -> BoxFut<'_, Result<(), HostError>>;
    fn resolve(
        &self,
        thread: &HostThreadId,
        interaction: &HostInteractionId,
        resolution: &InteractionResolution,
    ) -> BoxFut<'_, Result<(), HostError>>;
    fn retire(&self, thread: &HostThreadId) -> BoxFut<'_, Result<(), HostError>>;
    /// Probe whether a previously-`Live` host thread still exists after a reboot
    /// or reconnect. Returning `Ok(Some(obs))` submits a corrective `WaveObserve`
    /// (for example `Activity::Errored` when the host thread vanished after restart,
    /// or `Observation::Archived` when the thread was already archived).
    fn check_live(
        &self,
        _thread: &HostThreadId,
    ) -> BoxFut<'_, Result<Option<Observation>, HostError>> {
        Box::pin(async { Ok(None) })
    }
}

impl<H: AgentHost + ?Sized> AgentHost for Arc<H> {
    fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<HostThreadId, HostError>> {
        (**self).spawn(req)
    }

    fn tell(
        &self,
        thread: &HostThreadId,
        body: &Body,
        mode: SteerDeliveryMode,
    ) -> BoxFut<'_, Result<(), HostError>> {
        (**self).tell(thread, body, mode)
    }

    fn resolve(
        &self,
        thread: &HostThreadId,
        interaction: &HostInteractionId,
        resolution: &InteractionResolution,
    ) -> BoxFut<'_, Result<(), HostError>> {
        (**self).resolve(thread, interaction, resolution)
    }

    fn retire(&self, thread: &HostThreadId) -> BoxFut<'_, Result<(), HostError>> {
        (**self).retire(thread)
    }

    fn check_live(
        &self,
        thread: &HostThreadId,
    ) -> BoxFut<'_, Result<Option<Observation>, HostError>> {
        (**self).check_live(thread)
    }
}

impl<H: AgentHost + ?Sized> AgentHost for Box<H> {
    fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<HostThreadId, HostError>> {
        (**self).spawn(req)
    }

    fn tell(
        &self,
        thread: &HostThreadId,
        body: &Body,
        mode: SteerDeliveryMode,
    ) -> BoxFut<'_, Result<(), HostError>> {
        (**self).tell(thread, body, mode)
    }

    fn resolve(
        &self,
        thread: &HostThreadId,
        interaction: &HostInteractionId,
        resolution: &InteractionResolution,
    ) -> BoxFut<'_, Result<(), HostError>> {
        (**self).resolve(thread, interaction, resolution)
    }

    fn retire(&self, thread: &HostThreadId) -> BoxFut<'_, Result<(), HostError>> {
        (**self).retire(thread)
    }

    fn check_live(
        &self,
        thread: &HostThreadId,
    ) -> BoxFut<'_, Result<Option<Observation>, HostError>> {
        (**self).check_live(thread)
    }
}

pub struct WaveConductor<H: AgentHost = Arc<dyn AgentHost>> {
    host: H,
    view: BTreeMap<WaveSlug, WaveState>,
    room_waves: BTreeMap<RoomId, WaveSlug>,
    opened_rooms: BTreeSet<RoomId>,
    opened_boards: BTreeSet<BoardId>,
    room_members: BTreeMap<RoomId, BTreeSet<Principal>>,
    retire_attempts: BTreeMap<BbThreadId, u32>,
    probed_live_threads: BTreeSet<BbThreadId>,
    seen: Cursor,
    caught_up: bool,
    needs_reconcile: bool,
    working_mention_mode: Option<SteerDeliveryMode>,
}

impl<H: AgentHost> WaveConductor<H> {
    pub fn new(host: H) -> Self {
        Self::with_working_mention_mode(host, None)
    }

    pub fn with_working_mention_mode(
        host: H,
        working_mention_mode: Option<SteerDeliveryMode>,
    ) -> Self {
        WaveConductor {
            host,
            view: BTreeMap::new(),
            room_waves: BTreeMap::new(),
            opened_rooms: BTreeSet::new(),
            opened_boards: BTreeSet::new(),
            room_members: BTreeMap::new(),
            retire_attempts: BTreeMap::new(),
            probed_live_threads: BTreeSet::new(),
            seen: Cursor::default(),
            caught_up: false,
            needs_reconcile: false,
            working_mention_mode,
        }
    }

    fn fold(&mut self, env: &Envelope) {
        match (&env.stream, &env.event) {
            (StreamId::Wave { wave }, Event::Wave(ev)) => {
                if let WaveEvent::WorkerObserved {
                    role,
                    observation: Observation::Archived,
                } = ev
                    && let Some(
                        WorkerPhase::Live { thread, .. } | WorkerPhase::Retiring { thread, .. },
                    ) = self
                        .view
                        .get(wave)
                        .and_then(|st| st.workers.get(role))
                        .map(|r| &r.phase)
                {
                    let th = thread.clone();
                    self.retire_attempts.remove(&th);
                    self.probed_live_threads.remove(&th);
                }
                let st = self.view.entry(wave.clone()).or_default();
                Waves::evolve(
                    st,
                    ev,
                    &EntryMeta {
                        seq: env.seq,
                        at: env.at,
                        by: env.by.clone(),
                    },
                );
                if let Some(room) = &st.room {
                    self.room_waves.insert(room.clone(), wave.clone());
                }
            }
            (StreamId::Room { room }, Event::Room(ev)) => match ev {
                RoomEvent::Opened { .. } => {
                    self.opened_rooms.insert(room.clone());
                }
                RoomEvent::MemberJoined { member, .. } => {
                    self.room_members
                        .entry(room.clone())
                        .or_default()
                        .insert(member.clone());
                }
                RoomEvent::MemberLeft { member } => {
                    if let Some(members) = self.room_members.get_mut(room) {
                        members.remove(member);
                    }
                }
                RoomEvent::Posted { .. }
                | RoomEvent::TopicSet { .. }
                | RoomEvent::Labeled { .. }
                | RoomEvent::TaskClaimed { .. }
                | RoomEvent::TaskHeartbeat { .. }
                | RoomEvent::TaskTransitioned { .. }
                | RoomEvent::TaskUpdated { .. } => {}
            },
            (StreamId::Board { board }, Event::Board(_)) => {
                self.opened_boards.insert(board.clone());
            }
            _ => {}
        }
    }

    pub async fn handle_frame<C: Client>(
        &mut self,
        frame: Frame,
        client: &C,
    ) -> Result<(), ApiError> {
        match frame {
            Frame::CaughtUp { .. } => {
                self.caught_up = true;
                self.reconcile(client).await
            }
            Frame::Committed(env) => {
                if env.seq <= self.seen.at(&env.stream) {
                    if self.caught_up && self.needs_reconcile {
                        return self.reconcile(client).await;
                    }
                    return Ok(());
                }
                self.fold(&env);
                self.seen.advance(&env.stream, env.seq);
                if !self.caught_up {
                    return Ok(());
                }
                let was_dirty = self.needs_reconcile;
                let is_retire_event = matches!(
                    (&env.stream, &env.event),
                    (
                        StreamId::Wave { .. },
                        Event::Wave(WaveEvent::WorkerRetiring { .. })
                    )
                );
                let live_res = self.apply_live(&env, client).await;
                if live_res.is_err() && !is_retire_event {
                    self.needs_reconcile = true;
                } else if live_res.is_ok() && was_dirty {
                    self.reconcile(client).await?;
                }
                live_res
            }
            _ => Ok(()),
        }
    }

    async fn reconcile<C: Client>(&mut self, client: &C) -> Result<(), ApiError> {
        let mut first_err: Option<ApiError> = None;
        let mut any_retryable = false;
        let record_retryable =
            |slot: &mut Option<ApiError>, retryable: &mut bool, res: Result<(), ApiError>| {
                if let Err(e) = res {
                    *retryable = true;
                    if slot.is_none() {
                        *slot = Some(e);
                    }
                }
            };

        let slugs: Vec<WaveSlug> = self.view.keys().cloned().collect();
        for wave in slugs {
            let Some(st) = self.view.get(&wave).cloned() else {
                continue;
            };
            let Some(spec) = &st.spec else { continue };
            let room = st
                .room
                .clone()
                .unwrap_or_else(|| wave_room(&wave, &spec.project));
            let board = st
                .board
                .clone()
                .unwrap_or_else(|| project_board(&spec.project));

            if matches!(st.status, Some(WaveStatus::Running)) {
                if !self.opened_rooms.contains(&room) {
                    record_retryable(
                        &mut first_err,
                        &mut any_retryable,
                        self.ensure_wave_room(client, &wave, &room, &spec.goal)
                            .await,
                    );
                }
                if !self.opened_boards.contains(&board) {
                    record_retryable(
                        &mut first_err,
                        &mut any_retryable,
                        self.ensure_project_board(client, &board).await,
                    );
                }
                if let Some(opener) = &st.opener {
                    record_retryable(
                        &mut first_err,
                        &mut any_retryable,
                        self.ensure_member(client, &room, opener.clone(), None)
                            .await,
                    );
                }
                for role in st.workers.keys() {
                    let member = Principal::Worker {
                        wave: wave.clone(),
                        role: role.clone(),
                    };
                    let handle = Handle::parse(role.as_str()).ok();
                    record_retryable(
                        &mut first_err,
                        &mut any_retryable,
                        self.ensure_member(client, &room, member, handle).await,
                    );
                }
            }

            for (role, rec) in &st.workers {
                match &rec.phase {
                    WorkerPhase::Requested { attempt } => {
                        record_retryable(
                            &mut first_err,
                            &mut any_retryable,
                            self.spawn_worker(client, spec, &room, role, rec, *attempt)
                                .await,
                        );
                    }
                    WorkerPhase::Live { thread, activity } => {
                        if let Activity::Blocked { interaction } = activity
                            && let Some((resolved_id, resolution)) = &rec.resolved_interaction
                            && resolved_id == &interaction.id
                        {
                            record_retryable(
                                &mut first_err,
                                &mut any_retryable,
                                map_host_action(
                                    self.host.resolve(thread, resolved_id, resolution).await,
                                ),
                            );
                        }
                        let already_terminal_failure = matches!(
                            activity,
                            Activity::Errored { .. } | Activity::Refused { .. }
                        );
                        if !already_terminal_failure
                            && self.probed_live_threads.insert(thread.clone())
                        {
                            match self.host.check_live(thread).await {
                                Ok(Some(obs)) => {
                                    record_retryable(
                                        &mut first_err,
                                        &mut any_retryable,
                                        client
                                            .call(WaveObserve {
                                                wave: wave.clone(),
                                                role: role.clone(),
                                                observation: obs,
                                            })
                                            .await
                                            .map(|_| ()),
                                    );
                                }
                                Ok(None)
                                | Err(HostError::Rejected(_))
                                | Err(HostError::Unreachable(_)) => {}
                            }
                        }
                    }
                    WorkerPhase::Retiring { thread, .. } => {
                        if self.retire_attempts.get(thread).copied().unwrap_or(0)
                            < MAX_RETIRE_ATTEMPTS
                            && let Err(e) = self.retire_worker(client, &wave, role, thread).await
                        {
                            if self.retire_attempts.get(thread).copied().unwrap_or(0)
                                < MAX_RETIRE_ATTEMPTS
                            {
                                any_retryable = true;
                            }
                            if first_err.is_none() {
                                first_err = Some(e);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        self.needs_reconcile = any_retryable;
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn apply_live<C: Client>(&mut self, env: &Envelope, client: &C) -> Result<(), ApiError> {
        match (&env.stream, &env.event) {
            (StreamId::Wave { wave }, Event::Wave(ev)) => match ev {
                WaveEvent::Started { spec, room, board } => {
                    self.ensure_wave_room(client, wave, room, &spec.goal)
                        .await?;
                    self.ensure_project_board(client, board).await?;
                    self.ensure_member(client, room, env.by.clone(), None)
                        .await?;
                    for w in &spec.workers {
                        let member = Principal::Worker {
                            wave: wave.clone(),
                            role: w.role.clone(),
                        };
                        let handle = Handle::parse(w.role.as_str()).ok();
                        self.ensure_member(client, room, member, handle).await?;
                    }
                }
                WaveEvent::WorkerRequested { role, attempt, .. } => {
                    let Some(st) = self.view.get(wave).cloned() else {
                        return Ok(());
                    };
                    let Some(spec) = &st.spec else { return Ok(()) };
                    let Some(rec) = st.workers.get(role) else {
                        return Ok(());
                    };
                    if !matches!(rec.phase, WorkerPhase::Requested { attempt: a } if a == *attempt)
                    {
                        return Ok(());
                    }
                    let room = st
                        .room
                        .clone()
                        .unwrap_or_else(|| wave_room(wave, &spec.project));
                    self.spawn_worker(client, spec, &room, role, rec, *attempt)
                        .await?;
                }
                WaveEvent::Told { role, body, mode } => {
                    if let Some(thread) = self.live_thread(wave, role) {
                        map_host_action(self.host.tell(&thread, body, *mode).await)?;
                    }
                }
                WaveEvent::InteractionResolved {
                    role,
                    interaction,
                    resolution,
                } => {
                    if let Some(thread) = self.live_thread(wave, role) {
                        map_host_action(self.host.resolve(&thread, interaction, resolution).await)?;
                    }
                }
                WaveEvent::WorkerRetiring { role, .. } => {
                    if let Some(thread) = self.retiring_thread(wave, role) {
                        self.retire_worker(client, wave, role, &thread).await?;
                    }
                }
                WaveEvent::WorkerObserved { .. } | WaveEvent::Finished { .. } => {}
            },
            (
                StreamId::Room { room },
                Event::Room(RoomEvent::Posted {
                    body,
                    mentions,
                    idem,
                    origin,
                    ..
                }),
            ) => {
                // Briefs posted by WaveConductor itself are already delivered in `SpawnRequest::prompt`.
                if matches!(
                    env.by,
                    Principal::System {
                        part: SystemPart::WaveConductor
                    }
                ) {
                    return Ok(());
                }
                if *origin == PostOrigin::MirroredTurn
                    || idem.as_ref().is_some_and(is_legacy_mirror_idem_key)
                {
                    return Ok(());
                }
                self.route_post_wakeups(room, env.seq, &env.by, body, mentions)
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn ensure_wave_room<C: Client>(
        &mut self,
        client: &C,
        wave: &WaveSlug,
        room: &RoomId,
        goal: &Body,
    ) -> Result<(), ApiError> {
        client
            .call(RoomEnsure {
                room: RoomRef::Id { id: room.clone() },
                topic: Some(goal.as_str().to_owned()),
                origin: Some(RoomOrigin::Wave { wave: wave.clone() }),
            })
            .await?;
        self.opened_rooms.insert(room.clone());
        Ok(())
    }

    async fn ensure_project_board<C: Client>(
        &mut self,
        client: &C,
        board: &BoardId,
    ) -> Result<(), ApiError> {
        match client
            .call(BoardEnsure {
                board: board.clone(),
                lanes: None,
            })
            .await
        {
            Ok(_) => {
                self.opened_boards.insert(board.clone());
                Ok(())
            }
            Err(ApiError::Unavailable {
                family: Family::Board,
            }) => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn ensure_member<C: Client>(
        &mut self,
        client: &C,
        room: &RoomId,
        member: Principal,
        handle: Option<Handle>,
    ) -> Result<(), ApiError> {
        if self
            .room_members
            .get(room)
            .is_some_and(|m| m.contains(&member))
        {
            return Ok(());
        }
        match client
            .call(RoomJoin {
                room: RoomRef::Id { id: room.clone() },
                member: Some(member.clone()),
                handle,
            })
            .await
        {
            Ok(_) | Err(ApiError::Conflict { .. }) => {
                self.room_members
                    .entry(room.clone())
                    .or_default()
                    .insert(member);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn spawn_worker<C: Client>(
        &mut self,
        client: &C,
        spec: &WaveSpec,
        room: &RoomId,
        role: &RoleName,
        rec: &WorkerRecord,
        attempt: u32,
    ) -> Result<(), ApiError> {
        let wave = &spec.slug;
        let worker_principal = Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        };
        let handle = Handle::parse(role.as_str()).ok();
        self.ensure_member(client, room, worker_principal, handle)
            .await?;

        let idem = IdemKey::parse(format!("wave:{wave}:brief:{role}"))?;
        let mention_prefix = format!("@{role}");
        let brief_post = if rec.brief.as_str().starts_with(&mention_prefix) {
            rec.brief.clone()
        } else {
            bounded_body(format!("{mention_prefix} {}", rec.brief.as_str()))
        };
        client
            .call(RoomPost {
                idem: Some(idem),
                ..RoomPost::new(RoomRef::Id { id: room.clone() }, brief_post)
            })
            .await?;

        let run_stamp = rec
            .meta
            .run_stamp
            .clone()
            .unwrap_or_else(|| format!("{wave}:{role}:attempt-{attempt}"));
        let mut provenance = format!("[rex-wave provenance]\nrun_stamp: {run_stamp}\n");
        if let Some(dir) = &rec.output_dir {
            let anchor = if dir.is_relative() {
                " (relative to workspace root)"
            } else {
                ""
            };
            provenance.push_str(&format!(
                "output_dir: {}{anchor}\nrule: write deliverables under output_dir and tag them with run_stamp; never treat pre-existing files without matching run_stamp as completed work.\n",
                dir.display()
            ));
        } else {
            provenance.push_str(
                "rule: tag deliverables with run_stamp; never treat pre-existing files without matching run_stamp as completed work.\n",
            );
        }
        if !rec.writes.is_empty() {
            let list: Vec<String> = rec.writes.iter().map(|p| p.display().to_string()).collect();
            provenance.push_str(&format!("writes: {}\n", list.join(", ")));
        }
        if let Some(account) = &rec.agent.account {
            provenance.push_str(&format!("account: {account}\n"));
        }
        let mut prompt_text = format!(
            "Wave: {wave}\nRole: @{role}\nRoom: #wave-{wave} (project {})\nGoal: {}\n\n{provenance}\nAssignment:\n{}",
            spec.project,
            spec.goal.as_str(),
            rec.brief.as_str()
        );
        if let Some(schema) = &rec.return_schema
            && let Ok(compact) = serde_json::to_string(schema)
        {
            prompt_text.push_str(&format!("\n\nreturn_schema: {compact}"));
        }
        let prompt = bounded_body(prompt_text);
        let req = SpawnRequest {
            wave: wave.clone(),
            role: role.clone(),
            attempt,
            project: spec.project.clone(),
            agent: rec.agent.clone(),
            room: room.clone(),
            prompt,
            title: format!("[{wave}] @{role}"),
            run_stamp,
            output_dir: rec.output_dir.clone(),
            writes: rec.writes.clone(),
            return_schema: rec.return_schema.clone(),
        };

        let observation = match self.host.spawn(req).await {
            Ok(thread) => Observation::Spawned { attempt, thread },
            Err(HostError::Unreachable(msg)) => return Err(ApiError::Transport(msg)),
            Err(HostError::Rejected(error)) => Observation::SpawnFailed { attempt, error },
        };
        client
            .call(WaveObserve {
                wave: wave.clone(),
                role: role.clone(),
                observation,
            })
            .await?;
        Ok(())
    }

    async fn retire_worker<C: Client>(
        &mut self,
        client: &C,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
    ) -> Result<(), ApiError> {
        match self.host.retire(thread).await {
            Ok(()) => {}
            Err(HostError::Unreachable(msg)) => {
                self.needs_reconcile = true;
                return Err(ApiError::Transport(msg));
            }
            Err(HostError::Rejected(err)) => {
                let attempts = self.retire_attempts.entry(thread.clone()).or_insert(0);
                *attempts += 1;
                let attempt = *attempts;
                if attempt < MAX_RETIRE_ATTEMPTS {
                    self.needs_reconcile = true;
                } else if let Some(room) = self.view.get(wave).and_then(|st| st.room.clone()) {
                    let idem =
                        IdemKey::parse(format!("wave:{wave}:retire-failed:{role}:{thread}")).ok();
                    let body = bounded_body(format!(
                        "retire failed for @{role} ({thread}) after {attempt} attempts ({err})"
                    ));
                    let _ = client
                        .call(RoomPost {
                            idem,
                            ..RoomPost::new(RoomRef::Id { id: room }, body)
                        })
                        .await;
                }
                return Err(ApiError::Transport(format!("BB rejected action: {err}")));
            }
        }
        self.retire_attempts.remove(thread);
        client
            .call(WaveObserve {
                wave: wave.clone(),
                role: role.clone(),
                observation: Observation::Archived,
            })
            .await?;
        Ok(())
    }

    async fn route_post_wakeups(
        &self,
        room: &RoomId,
        seq: Seq,
        from: &Principal,
        body: &Body,
        mentions: &Mentions,
    ) -> Result<(), ApiError> {
        let mut targets: BTreeSet<(WaveSlug, RoleName)> = BTreeSet::new();

        for p in &mentions.principals {
            if let Principal::Worker { wave, role } = p {
                targets.insert((wave.clone(), role.clone()));
            }
        }
        if mentions.all
            && let Some(wave) = self.room_waves.get(room)
            && let Some(st) = self.view.get(wave)
        {
            for role in st.workers.keys() {
                targets.insert((wave.clone(), role.clone()));
            }
        }
        if let RoomId::Direct { pair } = room {
            let (a, b): (Principal, Principal) = pair.clone().into();
            for p in [a, b] {
                if let Principal::Worker { wave, role } = p {
                    targets.insert((wave, role));
                }
            }
        }

        for (wave, role) in targets {
            let worker_principal = Principal::Worker {
                wave: wave.clone(),
                role: role.clone(),
            };
            if from == &worker_principal {
                continue;
            }
            let Some(st) = self.view.get(&wave) else {
                continue;
            };
            let Some(rec) = st.workers.get(&role) else {
                continue;
            };
            let WorkerPhase::Live { thread, activity } = &rec.phase else {
                continue;
            };

            let mode = match plan_mention_wakeup(&rec.phase, room) {
                WakePlan::Doorbell(m) => Some(m),
                WakePlan::Covered if matches!(activity, Activity::Working) => {
                    self.working_mention_mode
                }
                WakePlan::Covered | WakePlan::Suppress => None,
            };
            if let Some(delivery_mode) = mode {
                let text = doorbell_text(room, seq, from, body);
                map_host_action(self.host.tell(thread, &text, delivery_mode).await)?;
            }
        }
        Ok(())
    }

    fn live_thread(&self, wave: &WaveSlug, role: &RoleName) -> Option<BbThreadId> {
        match &self.view.get(wave)?.workers.get(role)?.phase {
            WorkerPhase::Live { thread, .. } => Some(thread.clone()),
            _ => None,
        }
    }

    fn retiring_thread(&self, wave: &WaveSlug, role: &RoleName) -> Option<BbThreadId> {
        match &self.view.get(wave)?.workers.get(role)?.phase {
            WorkerPhase::Retiring { thread, .. } => Some(thread.clone()),
            _ => None,
        }
    }
}

fn map_host_action(res: Result<(), HostError>) -> Result<(), ApiError> {
    match res {
        Ok(()) => Ok(()),
        Err(HostError::Unreachable(msg)) => Err(ApiError::Transport(msg)),
        Err(HostError::Rejected(_)) => Ok(()),
    }
}

impl<H: AgentHost> Reactor for WaveConductor<H> {
    const NAME: &'static str = "wave-conductor";

    fn interest(&self) -> Interest {
        Interest::Everything
    }

    fn react(
        &mut self,
        frame: Frame,
        client: &LocalClient,
    ) -> impl Future<Output = Result<(), ApiError>> + Send {
        self.handle_frame(frame, client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rex_wave_api::room::{Posted, RoomInfo};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    fn block_on<F: Future>(fut: F) -> F::Output {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut fut = std::pin::pin!(fut);
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(out) => return out,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    fn sample_agent(provider: &str, model: &str, workspace: Workspace) -> AgentSpec {
        AgentSpec {
            provider: ProviderId::parse(provider).unwrap(),
            model: Some(ModelId::parse(model).unwrap()),
            account: None,
            reasoning: Some("xhigh".into()),
            service_tier: None,
            permission: PermissionMode::AcceptEdits,
            workspace,
        }
    }

    fn sample_spec() -> WaveSpec {
        WaveSpec {
            slug: WaveSlug::parse("auth-v2").unwrap(),
            project: BbProjectId::parse("proj_123").unwrap(),
            goal: Body::parse("Ship OAuth refresh").unwrap(),
            defaults: sample_agent(
                "codex",
                "gpt-6.1-sol",
                Workspace::ManagedWorktree {
                    base: BaseBranch::Default,
                },
            ),
            output_dir: None,
            workers: vec![
                WorkerSpec {
                    role: RoleName::parse("author").unwrap(),
                    brief: Body::parse("Own token refresh").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                },
                WorkerSpec {
                    role: RoleName::parse("verifier").unwrap(),
                    brief: Body::parse("Verify refresh edge cases").unwrap(),
                    agent: Some(sample_agent(
                        "claude-code",
                        "claude-opus-5-5",
                        Workspace::ProjectDefault,
                    )),
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                },
            ],
        }
    }

    fn apply_cmd(
        state: &mut WaveState,
        seq: &mut u64,
        by: Principal,
        cmd: WaveCommand,
    ) -> Result<(Vec<Envelope>, WireReply), ApiError> {
        let ctx = DecideCtx::for_test(by.clone(), UnixMs(1_000));
        let stream = Waves::address(&cmd, &ctx)?;
        let events = Waves::decide(state, &cmd, &ctx)?;
        let mut committed = Vec::new();
        for ev in events {
            *seq += 1;
            let meta = EntryMeta {
                seq: Seq(*seq),
                at: ctx.at(),
                by: by.clone(),
            };
            Waves::evolve(state, &ev, &meta);
            committed.push(Envelope {
                stream: stream.clone(),
                seq: meta.seq,
                at: meta.at,
                by: by.clone(),
                event: Event::Wave(ev),
            });
        }
        let reply = Waves::reply(state, cmd, &committed);
        Ok((committed, reply))
    }

    #[test]
    fn decider_lifecycle_swarm_add_worker_retries_and_permissions() {
        let mut state = WaveState::default();
        let mut seq = 0u64;
        let op = Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        };
        let sys = Principal::System {
            part: SystemPart::BbBridge,
        };
        let outsider_seat = Principal::Seat {
            seat: SeatName::parse("sam").unwrap(),
        };
        let spec = sample_spec();

        let (committed, reply) = apply_cmd(
            &mut state,
            &mut seq,
            op.clone(),
            WaveCommand::WaveStart(WaveStart {
                spec: spec.clone(),
                idem: None,
            }),
        )
        .unwrap();
        assert_eq!(committed.len(), 3);
        let WireReply::WaveStart(started) = reply else {
            panic!("expected WaveStarted")
        };
        assert_eq!(started.room, wave_room(&spec.slug, &spec.project));

        let (dup, _) = apply_cmd(
            &mut state,
            &mut seq,
            op.clone(),
            WaveCommand::WaveStart(WaveStart {
                spec: spec.clone(),
                idem: None,
            }),
        )
        .unwrap();
        assert!(dup.is_empty(), "identical WaveStart is idempotent");

        let mut conflicting = spec.clone();
        conflicting.goal = Body::parse("Different goal").unwrap();
        assert!(matches!(
            apply_cmd(
                &mut state,
                &mut seq,
                op.clone(),
                WaveCommand::WaveStart(WaveStart {
                    spec: conflicting,
                    idem: None
                })
            ),
            Err(ApiError::Conflict { .. })
        ));

        let author_role = RoleName::parse("author").unwrap();
        let author_principal = Principal::Worker {
            wave: spec.slug.clone(),
            role: author_role.clone(),
        };

        assert!(
            matches!(
                apply_cmd(
                    &mut state,
                    &mut seq,
                    author_principal.clone(),
                    WaveCommand::WaveAddWorker(WaveAddWorker {
                        wave: spec.slug.clone(),
                        worker: WorkerSpec {
                            role: RoleName::parse("helper").unwrap(),
                            brief: Body::parse("Assist").unwrap(),
                            agent: None,
                            output_dir: None,
                            writes: Vec::new(),
                            return_schema: None,
                        },
                    }),
                ),
                Err(ApiError::Denied { .. })
            ),
            "unspawned worker cannot add helpers yet"
        );

        apply_cmd(
            &mut state,
            &mut seq,
            sys.clone(),
            WaveCommand::WaveObserve(WaveObserve {
                wave: spec.slug.clone(),
                role: author_role.clone(),
                observation: Observation::Spawned {
                    attempt: 1,
                    thread: BbThreadId::parse("th_author").unwrap(),
                },
            }),
        )
        .unwrap();

        assert!(
            matches!(
                apply_cmd(
                    &mut state,
                    &mut seq,
                    outsider_seat,
                    WaveCommand::WaveAddWorker(WaveAddWorker {
                        wave: spec.slug.clone(),
                        worker: WorkerSpec {
                            role: RoleName::parse("helper").unwrap(),
                            brief: Body::parse("Assist").unwrap(),
                            agent: None,
                            output_dir: None,
                            writes: Vec::new(),
                            return_schema: None,
                        },
                    }),
                ),
                Err(ApiError::Denied { .. })
            ),
            "non-operator non-worker seat cannot add wave workers"
        );

        let helper_role = RoleName::parse("helper-pi").unwrap();
        let (added, _) = apply_cmd(
            &mut state,
            &mut seq,
            author_principal,
            WaveCommand::WaveAddWorker(WaveAddWorker {
                wave: spec.slug.clone(),
                worker: WorkerSpec {
                    role: helper_role.clone(),
                    brief: Body::parse("Check migration").unwrap(),
                    agent: Some(sample_agent(
                        "pi",
                        "openrouter/model",
                        Workspace::ExistingPath {
                            path: "/tmp/wt".into(),
                        },
                    )),
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                },
            }),
        )
        .unwrap();
        assert_eq!(
            added.len(),
            1,
            "active wave worker can add helper like AGY Swarm"
        );

        for attempt in 1..MAX_SPAWN_ATTEMPTS {
            let (obs_events, _) = apply_cmd(
                &mut state,
                &mut seq,
                sys.clone(),
                WaveCommand::WaveObserve(WaveObserve {
                    wave: spec.slug.clone(),
                    role: helper_role.clone(),
                    observation: Observation::SpawnFailed {
                        attempt,
                        error: "transient".into(),
                    },
                }),
            )
            .unwrap();
            assert_eq!(
                obs_events.len(),
                2,
                "failed attempt < MAX emits WorkerObserved + retry WorkerRequested"
            );
        }

        let (stale, _) = apply_cmd(
            &mut state,
            &mut seq,
            sys.clone(),
            WaveCommand::WaveObserve(WaveObserve {
                wave: spec.slug.clone(),
                role: helper_role.clone(),
                observation: Observation::SpawnFailed {
                    attempt: 1,
                    error: "stale".into(),
                },
            }),
        )
        .unwrap();
        assert!(
            stale.is_empty(),
            "stale attempt number is an idempotent no-op"
        );

        apply_cmd(
            &mut state,
            &mut seq,
            sys.clone(),
            WaveCommand::WaveObserve(WaveObserve {
                wave: spec.slug.clone(),
                role: helper_role.clone(),
                observation: Observation::SpawnFailed {
                    attempt: MAX_SPAWN_ATTEMPTS,
                    error: "fatal".into(),
                },
            }),
        )
        .unwrap();

        let (_, view_reply) = apply_cmd(
            &mut state,
            &mut seq,
            op.clone(),
            WaveCommand::WaveView(WaveView {
                wave: spec.slug.clone(),
            }),
        )
        .unwrap();
        let WireReply::WaveView(snap) = view_reply else {
            panic!("expected WaveSnapshot")
        };
        let helper_phase = &snap
            .workers
            .iter()
            .find(|(r, _, _)| r == &helper_role)
            .unwrap()
            .2;
        assert_eq!(
            helper_phase,
            &WorkerPhase::Failed {
                error: "fatal".into()
            }
        );
    }

    #[test]
    fn finishing_a_wave_keeps_each_workers_error_through_retirement() {
        let mut state = WaveState::default();
        let mut seq = 0u64;
        let op = Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        };
        let sys = Principal::System {
            part: SystemPart::BbBridge,
        };
        let spec = sample_spec();
        let author = RoleName::parse("author").unwrap();
        let verifier = RoleName::parse("verifier").unwrap();
        let observe = |state: &mut WaveState, seq: &mut u64, role: &RoleName, o| {
            apply_cmd(
                state,
                seq,
                sys.clone(),
                WaveCommand::WaveObserve(WaveObserve {
                    wave: spec.slug.clone(),
                    role: role.clone(),
                    observation: o,
                }),
            )
            .unwrap();
        };
        apply_cmd(
            &mut state,
            &mut seq,
            op.clone(),
            WaveCommand::WaveStart(WaveStart {
                spec: spec.clone(),
                idem: None,
            }),
        )
        .unwrap();
        for (role, thread) in [(&author, "th_author"), (&verifier, "th_verifier")] {
            observe(
                &mut state,
                &mut seq,
                role,
                Observation::Spawned {
                    attempt: 1,
                    thread: BbThreadId::parse(thread).unwrap(),
                },
            );
        }
        observe(
            &mut state,
            &mut seq,
            &author,
            Observation::Activity {
                activity: Activity::Errored {
                    error: "ServiceUnavailableError".into(),
                },
            },
        );
        observe(
            &mut state,
            &mut seq,
            &verifier,
            Observation::Activity {
                activity: Activity::Idle,
            },
        );
        let blocked_finish = apply_cmd(
            &mut state,
            &mut seq,
            op.clone(),
            WaveCommand::WaveFinish(WaveFinish {
                wave: spec.slug.clone(),
                outcome: WaveOutcome::Completed {
                    summary: Body::parse("done").unwrap(),
                },
                allow_incomplete: false,
            }),
        )
        .unwrap_err();
        assert!(
            matches!(blocked_finish, ApiError::Conflict { .. })
                && blocked_finish.to_string().contains("@author"),
            "WaveFinish(Completed) without allow_incomplete must reject errored worker: {blocked_finish:?}"
        );
        apply_cmd(
            &mut state,
            &mut seq,
            op.clone(),
            WaveCommand::WaveFinish(WaveFinish {
                wave: spec.slug.clone(),
                outcome: WaveOutcome::Completed {
                    summary: Body::parse("done").unwrap(),
                },
                allow_incomplete: true,
            }),
        )
        .unwrap();
        let phases = |state: &mut WaveState, seq: &mut u64| {
            let (_, reply) = apply_cmd(
                state,
                seq,
                op.clone(),
                WaveCommand::WaveView(WaveView {
                    wave: spec.slug.clone(),
                }),
            )
            .unwrap();
            let WireReply::WaveView(snap) = reply else {
                panic!("expected WaveSnapshot")
            };
            snap.workers
                .into_iter()
                .map(|(r, _, p)| (r.to_string(), p))
                .collect::<BTreeMap<_, _>>()
        };
        let retiring = phases(&mut state, &mut seq);
        assert_eq!(
            retiring["author"].error(),
            Some("ServiceUnavailableError"),
            "retiring keeps the error: {retiring:?}"
        );
        assert_eq!(retiring["verifier"].error(), None);

        observe(&mut state, &mut seq, &author, Observation::Archived);
        observe(&mut state, &mut seq, &verifier, Observation::Archived);
        let retired = phases(&mut state, &mut seq);
        assert_eq!(
            retired["author"],
            WorkerPhase::Retired {
                thread: Some(BbThreadId::parse("th_author").unwrap()),
                errored: Some("ServiceUnavailableError".into()),
                refused: None,
            }
        );
        assert_eq!(
            retired["verifier"],
            WorkerPhase::Retired {
                thread: Some(BbThreadId::parse("th_verifier").unwrap()),
                errored: None,
                refused: None,
            }
        );
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum HostCall {
        Spawn {
            wave: String,
            role: String,
            attempt: u32,
            provider: String,
            workspace: Workspace,
        },
        Tell {
            thread: String,
            mode: SteerDeliveryMode,
            text: String,
        },
        Resolve {
            thread: String,
            interaction: String,
            resolution: InteractionResolution,
        },
        Retire {
            thread: String,
        },
    }

    #[derive(Clone, Default)]
    struct FakeHost {
        calls: Arc<Mutex<Vec<HostCall>>>,
        spawn_requests: Arc<Mutex<Vec<SpawnRequest>>>,
        missing_threads: Arc<Mutex<BTreeSet<String>>>,
        unreachable_check_live: Arc<Mutex<bool>>,
        check_live_calls: Arc<Mutex<u32>>,
        unreachable_role: Arc<Mutex<Option<String>>>,
        rejected_role: Arc<Mutex<Option<String>>>,
        unreachable_retire: Arc<Mutex<bool>>,
        reject_retire: Arc<Mutex<bool>>,
        retire_attempts: Arc<Mutex<u32>>,
    }

    impl AgentHost for FakeHost {
        fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<BbThreadId, HostError>> {
            Box::pin(async move {
                if self.unreachable_role.lock().unwrap().as_deref() == Some(req.role.as_str()) {
                    return Err(HostError::Unreachable("transient disconnect".into()));
                }
                if self.rejected_role.lock().unwrap().as_deref() == Some(req.role.as_str()) {
                    return Err(HostError::Rejected(
                        "environment provider requires a machine".into(),
                    ));
                }
                self.calls.lock().unwrap().push(HostCall::Spawn {
                    wave: req.wave.to_string(),
                    role: req.role.to_string(),
                    attempt: req.attempt,
                    provider: req.agent.provider.to_string(),
                    workspace: req.agent.workspace.clone(),
                });
                let id = BbThreadId::parse(format!("th_{}_{}_{}", req.wave, req.role, req.attempt))
                    .unwrap();
                self.spawn_requests.lock().unwrap().push(req);
                Ok(id)
            })
        }

        fn tell(
            &self,
            thread: &BbThreadId,
            body: &Body,
            mode: SteerDeliveryMode,
        ) -> BoxFut<'_, Result<(), HostError>> {
            let thread = thread.to_string();
            let text = body.as_str().to_owned();
            Box::pin(async move {
                self.calls
                    .lock()
                    .unwrap()
                    .push(HostCall::Tell { thread, mode, text });
                Ok(())
            })
        }

        fn resolve(
            &self,
            thread: &BbThreadId,
            interaction: &BbInteractionId,
            resolution: &InteractionResolution,
        ) -> BoxFut<'_, Result<(), HostError>> {
            let thread = thread.to_string();
            let interaction = interaction.to_string();
            let resolution = resolution.clone();
            Box::pin(async move {
                self.calls.lock().unwrap().push(HostCall::Resolve {
                    thread,
                    interaction,
                    resolution,
                });
                Ok(())
            })
        }

        fn retire(&self, thread: &BbThreadId) -> BoxFut<'_, Result<(), HostError>> {
            let thread = thread.to_string();
            Box::pin(async move {
                *self.retire_attempts.lock().unwrap() += 1;
                if *self.unreachable_retire.lock().unwrap() {
                    return Err(HostError::Unreachable("BB bridge disconnected".into()));
                }
                if *self.reject_retire.lock().unwrap() {
                    return Err(HostError::Rejected("thread remained stopping".into()));
                }
                self.calls.lock().unwrap().push(HostCall::Retire { thread });
                Ok(())
            })
        }

        fn check_live(
            &self,
            thread: &BbThreadId,
        ) -> BoxFut<'_, Result<Option<Observation>, HostError>> {
            let thread = thread.to_string();
            Box::pin(async move {
                *self.check_live_calls.lock().unwrap() += 1;
                if *self.unreachable_check_live.lock().unwrap() {
                    return Err(HostError::Unreachable("host disconnected".into()));
                }
                if self.missing_threads.lock().unwrap().contains(&thread) {
                    return Ok(Some(Observation::Activity {
                        activity: Activity::Errored {
                            error: "host thread missing after restart".into(),
                        },
                    }));
                }
                Ok(None)
            })
        }
    }

    struct MiniBus {
        wave_states: BTreeMap<WaveSlug, WaveState>,
        room_members: BTreeMap<RoomId, BTreeMap<Handle, Principal>>,
        room_posts: BTreeMap<(RoomId, IdemKey), Seq>,
        boards: BTreeSet<BoardId>,
        seqs: BTreeMap<StreamId, u64>,
        queue: VecDeque<Envelope>,
        history: Vec<Envelope>,
    }

    impl MiniBus {
        fn new() -> Arc<Mutex<Self>> {
            Arc::new(Mutex::new(MiniBus {
                wave_states: BTreeMap::new(),
                room_members: BTreeMap::new(),
                room_posts: BTreeMap::new(),
                boards: BTreeSet::new(),
                seqs: BTreeMap::new(),
                queue: VecDeque::new(),
                history: Vec::new(),
            }))
        }

        fn push(&mut self, stream: StreamId, by: Principal, event: Event) -> Envelope {
            let seq = {
                let s = self.seqs.entry(stream.clone()).or_insert(0);
                *s += 1;
                Seq(*s)
            };
            let env = Envelope {
                stream,
                seq,
                at: UnixMs(100),
                by,
                event,
            };
            self.queue.push_back(env.clone());
            self.history.push(env.clone());
            env
        }
    }

    #[derive(Clone)]
    struct BusClient {
        me: Principal,
        bus: Arc<Mutex<MiniBus>>,
    }

    impl Client for BusClient {
        fn me(&self) -> &Principal {
            &self.me
        }

        fn call_wire(
            &self,
            cmd: WireCommand,
        ) -> impl Future<Output = Result<WireReply, ApiError>> + Send {
            let me = self.me.clone();
            let bus = self.bus.clone();
            async move {
                let mut g = bus.lock().unwrap();
                match cmd.route() {
                    Routed::Wave(wc) => {
                        let ctx = DecideCtx::for_test(me.clone(), UnixMs(100));
                        let stream = Waves::address(&wc, &ctx)?;
                        let StreamId::Wave { wave } = &stream else {
                            unreachable!()
                        };
                        let events = {
                            let st = g.wave_states.entry(wave.clone()).or_default();
                            Waves::decide(st, &wc, &ctx)?
                        };
                        let mut committed = Vec::new();
                        for ev in events {
                            let env = g.push(stream.clone(), me.clone(), Event::Wave(ev.clone()));
                            let meta = EntryMeta {
                                seq: env.seq,
                                at: env.at,
                                by: me.clone(),
                            };
                            Waves::evolve(g.wave_states.get_mut(wave).unwrap(), &ev, &meta);
                            committed.push(env);
                        }
                        let st = g.wave_states.get(wave).unwrap();
                        Ok(Waves::reply(st, wc, &committed))
                    }
                    Routed::Room(rc) => match rc {
                        RoomCommand::RoomEnsure(c) => {
                            let id = c.room.resolve(&me)?;
                            let origin = c.origin.clone().unwrap_or(RoomOrigin::Manual);
                            let stream = StreamId::Room { room: id.clone() };
                            if !g.seqs.contains_key(&stream) {
                                g.push(
                                    stream.clone(),
                                    me.clone(),
                                    Event::Room(RoomEvent::Opened {
                                        id: id.clone(),
                                        origin: origin.clone(),
                                        topic: c.topic.clone(),
                                    }),
                                );
                            }
                            let head = Seq(*g.seqs.get(&stream).unwrap_or(&0));
                            let members = g
                                .room_members
                                .get(&id)
                                .cloned()
                                .unwrap_or_default()
                                .into_iter()
                                .collect();
                            Ok(WireReply::RoomEnsure(RoomInfo {
                                id,
                                origin,
                                topic: c.topic,
                                head,
                                members,
                            }))
                        }
                        RoomCommand::RoomJoin(c) => {
                            let id = c.room.resolve(&me)?;
                            let member = c.member.unwrap_or_else(|| me.clone());
                            let handle = match c.handle {
                                Some(h) => h,
                                None => match &member {
                                    Principal::Operator { seat } | Principal::Seat { seat } => {
                                        Handle::parse(seat.as_str())?
                                    }
                                    Principal::Worker { role, .. } => Handle::parse(role.as_str())?,
                                    Principal::System { part } => {
                                        Handle::parse(format!("{part:?}").to_lowercase())?
                                    }
                                },
                            };
                            g.room_members
                                .entry(id.clone())
                                .or_default()
                                .insert(handle.clone(), member.clone());
                            g.push(
                                StreamId::Room { room: id },
                                me,
                                Event::Room(RoomEvent::MemberJoined { member, handle }),
                            );
                            Ok(WireReply::RoomJoin(Ack))
                        }
                        RoomCommand::RoomPost(c) => {
                            let id = c.room.resolve(&me)?;
                            if let Some(idem) = &c.idem
                                && let Some(&seq) = g.room_posts.get(&(id.clone(), idem.clone()))
                            {
                                return Ok(WireReply::RoomPost(Posted {
                                    room: id,
                                    seq,
                                    mentions: Mentions::default(),
                                    re: None,
                                    org_id: None,
                                    delivery_notice: None,
                                }));
                            }
                            let mut principals = Vec::new();
                            let mut all = false;
                            let map = g.room_members.get(&id).cloned().unwrap_or_default();
                            for tok in c.body.as_str().split_whitespace() {
                                if let Some(raw) = tok.strip_prefix('@') {
                                    let clean = raw.trim_end_matches(|ch: char| {
                                        !ch.is_ascii_alphanumeric() && ch != '-' && ch != '_'
                                    });
                                    if clean == "all" {
                                        all = true;
                                    } else if let Ok(h) = Handle::parse(clean)
                                        && let Some(p) = map.get(&h)
                                        && !principals.contains(p)
                                    {
                                        principals.push(p.clone());
                                    }
                                }
                            }
                            let mentions = Mentions { principals, all };
                            let env = g.push(
                                StreamId::Room { room: id.clone() },
                                me,
                                Event::Room(RoomEvent::Posted {
                                    body: c.body,
                                    mentions: mentions.clone(),
                                    re: c.re,
                                    after: c.after,
                                    labels: c.labels,
                                    task: c.task,
                                    idem: c.idem.clone(),
                                    origin: Default::default(),
                                }),
                            );
                            if let Some(idem) = c.idem {
                                g.room_posts.insert((id.clone(), idem), env.seq);
                            }
                            Ok(WireReply::RoomPost(Posted {
                                room: id,
                                seq: env.seq,
                                mentions,
                                re: c.re,
                                org_id: None,
                                delivery_notice: None,
                            }))
                        }
                        RoomCommand::RoomLeave(_) => Ok(WireReply::RoomLeave(Ack)),
                        _ => Err(ApiError::Unavailable {
                            family: Family::Room,
                        }),
                    },
                    Routed::Board(BoardCommand::BoardEnsure(c)) => {
                        g.boards.insert(c.board.clone());
                        g.push(
                            StreamId::Board {
                                board: c.board.clone(),
                            },
                            me,
                            Event::Board(rex_wave_api::board::BoardEvent::Opened {
                                board: c.board.clone(),
                                lanes: vec![],
                            }),
                        );
                        Ok(WireReply::BoardEnsure(rex_wave_api::board::BoardInfo {
                            board: c.board,
                            lanes: vec![],
                            head: 1,
                        }))
                    }
                    _ => Err(ApiError::Unavailable {
                        family: Family::Mail,
                    }),
                }
            }
        }

        async fn watch(&self, _interest: Interest, _from: Cursor) -> Result<FrameStream, ApiError> {
            Err::<FrameStream, ApiError>(ApiError::Unavailable {
                family: Family::Read,
            })
        }
    }

    fn drain_conductor(
        conductor: &mut WaveConductor<FakeHost>,
        sys_client: &BusClient,
        bus: &Arc<Mutex<MiniBus>>,
    ) {
        loop {
            let next = bus.lock().unwrap().queue.pop_front();
            let Some(env) = next else { break };
            block_on(conductor.handle_frame(Frame::Committed(env), sys_client)).unwrap();
        }
    }

    #[test]
    fn conductor_spawns_routes_dual_wakeups_resolves_and_retires() {
        let bus = MiniBus::new();
        let host = FakeHost::default();
        let mut conductor = WaveConductor::new(host.clone());
        let op = BusClient {
            me: Principal::Operator {
                seat: SeatName::parse("owner").unwrap(),
            },
            bus: bus.clone(),
        };
        let sys_conductor = BusClient {
            me: Principal::System {
                part: SystemPart::WaveConductor,
            },
            bus: bus.clone(),
        };
        let sys_bridge = BusClient {
            me: Principal::System {
                part: SystemPart::BbBridge,
            },
            bus: bus.clone(),
        };

        block_on(conductor.handle_frame(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &sys_conductor,
        ))
        .unwrap();

        let started = block_on(op.call(WaveStart {
            spec: sample_spec(),
            idem: None,
        }))
        .unwrap();
        drain_conductor(&mut conductor, &sys_conductor, &bus);

        assert!(
            bus.lock().unwrap().boards.contains(&started.board),
            "project board ensured"
        );
        assert_eq!(
            host.calls.lock().unwrap().len(),
            2,
            "both workers spawned without self-echoing brief doorbells"
        );

        let wave = WaveSlug::parse("auth-v2").unwrap();
        let author = RoleName::parse("author").unwrap();
        let verifier = RoleName::parse("verifier").unwrap();

        block_on(sys_bridge.call(WaveObserve {
            wave: wave.clone(),
            role: author.clone(),
            observation: Observation::Activity {
                activity: Activity::Parked {
                    room: Some(started.room.clone()),
                    since: Seq(1),
                },
            },
        }))
        .unwrap();
        block_on(sys_bridge.call(WaveObserve {
            wave: wave.clone(),
            role: verifier.clone(),
            observation: Observation::Activity {
                activity: Activity::Idle,
            },
        }))
        .unwrap();
        drain_conductor(&mut conductor, &sys_conductor, &bus);
        host.calls.lock().unwrap().clear();

        block_on(op.call(RoomPost::new(
            RoomRef::Id {
                id: started.room.clone(),
            },
            Body::parse("@all status check").unwrap(),
        )))
        .unwrap();
        drain_conductor(&mut conductor, &sys_conductor, &bus);

        let calls = host.calls.lock().unwrap().clone();
        assert_eq!(
            calls.len(),
            1,
            "Parked worker wakes via ReadSide; only Idle worker gets doorbell"
        );
        assert!(matches!(
            &calls[0],
            HostCall::Tell { thread, mode: SteerDeliveryMode::Auto, text }
                if thread == "th_auth-v2_verifier_1" && text.contains("#wave-auth-v2") && text.contains("status check")
        ));

        host.calls.lock().unwrap().clear();
        block_on(op.call(RoomPost::new(
            RoomRef::Dm {
                with: Principal::Worker {
                    wave: wave.clone(),
                    role: author.clone(),
                },
            },
            Body::parse("urgent DM").unwrap(),
        )))
        .unwrap();
        drain_conductor(&mut conductor, &sys_conductor, &bus);

        let dm_calls = host.calls.lock().unwrap().clone();
        assert_eq!(
            dm_calls.len(),
            1,
            "worker parked on wave room gets SteerIfActive doorbell for DM"
        );
        assert!(matches!(
            &dm_calls[0],
            HostCall::Tell { thread, mode: SteerDeliveryMode::SteerIfActive, .. } if thread == "th_auth-v2_author_1"
        ));

        host.calls.lock().unwrap().clear();
        let int_id = BbInteractionId::parse("int_42").unwrap();
        block_on(sys_bridge.call(WaveObserve {
            wave: wave.clone(),
            role: verifier.clone(),
            observation: Observation::Activity {
                activity: Activity::Blocked {
                    interaction: PendingInteraction {
                        id: int_id.clone(),
                        kind: InteractionKind::Permission,
                        summary: "Run git status".into(),
                    },
                },
            },
        }))
        .unwrap();
        block_on(op.call(WaveResolve {
            wave: wave.clone(),
            role: verifier.clone(),
            interaction: int_id.clone(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        }))
        .unwrap();
        drain_conductor(&mut conductor, &sys_conductor, &bus);
        assert_eq!(
            host.calls.lock().unwrap().as_slice(),
            &[HostCall::Resolve {
                thread: "th_auth-v2_verifier_1".into(),
                interaction: "int_42".into(),
                resolution: InteractionResolution::AllowOnce {
                    granted_permissions: None
                },
            }]
        );

        host.calls.lock().unwrap().clear();
        block_on(op.call(WaveFinish {
            wave: wave.clone(),
            outcome: WaveOutcome::Completed {
                summary: Body::parse("All green").unwrap(),
            },
            allow_incomplete: false,
        }))
        .unwrap();
        drain_conductor(&mut conductor, &sys_conductor, &bus);

        let retire_calls = host.calls.lock().unwrap().clone();
        assert_eq!(
            retire_calls,
            vec![
                HostCall::Retire {
                    thread: "th_auth-v2_author_1".into()
                },
                HostCall::Retire {
                    thread: "th_auth-v2_verifier_1".into()
                },
            ]
        );
        let snap = block_on(op.call(WaveView { wave })).unwrap();
        assert!(snap.workers.iter().all(|(_, _, p)| matches!(
            p,
            WorkerPhase::Retired {
                thread: Some(_),
                ..
            }
        )));
    }

    #[test]
    fn conductor_reconciles_crash_state_on_caught_up_without_replaying_old_doorbells() {
        let bus = MiniBus::new();
        let host = FakeHost::default();
        let mut conductor = WaveConductor::new(host.clone());
        let op = BusClient {
            me: Principal::Operator {
                seat: SeatName::parse("owner").unwrap(),
            },
            bus: bus.clone(),
        };
        let sys_conductor = BusClient {
            me: Principal::System {
                part: SystemPart::WaveConductor,
            },
            bus: bus.clone(),
        };
        let sys_bridge = BusClient {
            me: Principal::System {
                part: SystemPart::BbBridge,
            },
            bus: bus.clone(),
        };

        let started = block_on(op.call(WaveStart {
            spec: sample_spec(),
            idem: None,
        }))
        .unwrap();
        let wave = WaveSlug::parse("auth-v2").unwrap();
        let author = RoleName::parse("author").unwrap();
        let verifier = RoleName::parse("verifier").unwrap();

        block_on(sys_bridge.call(WaveObserve {
            wave: wave.clone(),
            role: author.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: BbThreadId::parse("th_author_old").unwrap(),
            },
        }))
        .unwrap();
        block_on(op.call(WaveRetireWorker {
            wave: wave.clone(),
            role: author,
            reason: Some("done".into()),
        }))
        .unwrap();
        block_on(op.call(RoomPost::new(
            RoomRef::Id { id: started.room },
            Body::parse("@all historical message").unwrap(),
        )))
        .unwrap();

        drain_conductor(&mut conductor, &sys_conductor, &bus);
        assert!(
            host.calls.lock().unwrap().is_empty(),
            "no side effects during pre-CaughtUp replay"
        );

        block_on(conductor.handle_frame(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &sys_conductor,
        ))
        .unwrap();
        drain_conductor(&mut conductor, &sys_conductor, &bus);

        let calls = host.calls.lock().unwrap().clone();
        assert_eq!(
            calls,
            vec![
                HostCall::Retire {
                    thread: "th_author_old".into()
                },
                HostCall::Spawn {
                    wave: "auth-v2".into(),
                    role: verifier.to_string(),
                    attempt: 1,
                    provider: "claude-code".into(),
                    workspace: Workspace::ProjectDefault,
                },
            ],
            "reconcile retires Retiring worker, spawns Requested worker, and skips historical doorbells"
        );
    }

    #[test]
    fn conductor_supports_dynamic_add_worker_working_queue_policy_and_tell_modes() {
        let bus = MiniBus::new();
        let host = FakeHost::default();
        let mut conductor = WaveConductor::with_working_mention_mode(
            host.clone(),
            Some(SteerDeliveryMode::QueueIfActive),
        );
        let op = BusClient {
            me: Principal::Operator {
                seat: SeatName::parse("owner").unwrap(),
            },
            bus: bus.clone(),
        };
        let sys_conductor = BusClient {
            me: Principal::System {
                part: SystemPart::WaveConductor,
            },
            bus: bus.clone(),
        };

        block_on(conductor.handle_frame(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &sys_conductor,
        ))
        .unwrap();
        let started = block_on(op.call(WaveStart {
            spec: sample_spec(),
            idem: None,
        }))
        .unwrap();
        drain_conductor(&mut conductor, &sys_conductor, &bus);
        host.calls.lock().unwrap().clear();

        let wave = WaveSlug::parse("auth-v2").unwrap();
        let author = RoleName::parse("author").unwrap();
        let author_client = BusClient {
            me: Principal::Worker {
                wave: wave.clone(),
                role: author.clone(),
            },
            bus: bus.clone(),
        };

        let helper_role = RoleName::parse("acp-helper").unwrap();
        block_on(author_client.call(WaveAddWorker {
            wave: wave.clone(),
            worker: WorkerSpec {
                role: helper_role.clone(),
                brief: Body::parse("Audit OAuth scopes").unwrap(),
                agent: Some(sample_agent(
                    "acp-opencode",
                    "opencode/gemini-4-carbon",
                    Workspace::ExistingPath {
                        path: "/tmp/wt".into(),
                    },
                )),
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            },
        }))
        .unwrap();
        drain_conductor(&mut conductor, &sys_conductor, &bus);

        assert_eq!(
            host.calls.lock().unwrap().as_slice(),
            &[HostCall::Spawn {
                wave: "auth-v2".into(),
                role: "acp-helper".into(),
                attempt: 1,
                provider: "acp-opencode".into(),
                workspace: Workspace::ExistingPath {
                    path: "/tmp/wt".into()
                },
            }]
        );
        host.calls.lock().unwrap().clear();

        block_on(op.call(RoomPost::new(
            RoomRef::Id { id: started.room },
            Body::parse("@acp-helper check token TTL").unwrap(),
        )))
        .unwrap();
        drain_conductor(&mut conductor, &sys_conductor, &bus);

        let queued = host.calls.lock().unwrap().clone();
        assert_eq!(queued.len(), 1);
        assert!(matches!(
            &queued[0],
            HostCall::Tell { thread, mode: SteerDeliveryMode::QueueIfActive, .. } if thread == "th_auth-v2_acp-helper_1"
        ));

        host.calls.lock().unwrap().clear();
        block_on(op.call(WaveTell {
            wave,
            role: helper_role,
            body: Body::parse("pivot to PKCE").unwrap(),
            mode: SteerDeliveryMode::Steer,
        }))
        .unwrap();
        drain_conductor(&mut conductor, &sys_conductor, &bus);

        assert_eq!(
            host.calls.lock().unwrap().as_slice(),
            &[HostCall::Tell {
                thread: "th_auth-v2_acp-helper_1".into(),
                mode: SteerDeliveryMode::Steer,
                text: "pivot to PKCE".into(),
            }]
        );
    }

    fn drain_conductor_allow_err(
        conductor: &mut WaveConductor<FakeHost>,
        sys_client: &BusClient,
        bus: &Arc<Mutex<MiniBus>>,
    ) {
        loop {
            let next = bus.lock().unwrap().queue.pop_front();
            let Some(env) = next else { break };
            let _ = block_on(conductor.handle_frame(Frame::Committed(env), sys_client));
        }
    }

    #[test]
    fn rejected_spawn_retries_post_the_brief_once() {
        let bus = MiniBus::new();
        let host = FakeHost::default();
        *host.rejected_role.lock().unwrap() = Some("author".into());
        let mut conductor = WaveConductor::new(host.clone());
        let op = BusClient {
            me: Principal::Operator {
                seat: SeatName::parse("owner").unwrap(),
            },
            bus: bus.clone(),
        };
        let sys_conductor = BusClient {
            me: Principal::System {
                part: SystemPart::WaveConductor,
            },
            bus: bus.clone(),
        };
        block_on(op.call(WaveStart {
            spec: sample_spec(),
            idem: None,
        }))
        .unwrap();
        let mut author_briefs = 0;
        let mut drain = |conductor: &mut WaveConductor<FakeHost>| loop {
            let next = bus.lock().unwrap().queue.pop_front();
            let Some(env) = next else { break };
            if let Event::Room(RoomEvent::Posted { body, .. }) = &env.event
                && body.as_str().starts_with("@author ")
            {
                author_briefs += 1;
            }
            let _ = block_on(conductor.handle_frame(Frame::Committed(env), &sys_conductor));
        };
        drain(&mut conductor);
        let _ = block_on(conductor.handle_frame(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &sys_conductor,
        ));
        drain(&mut conductor);

        let snap = block_on(op.call(WaveView {
            wave: WaveSlug::parse("auth-v2").unwrap(),
        }))
        .unwrap();
        let author = &snap
            .workers
            .iter()
            .find(|(r, _, _)| r.as_str() == "author")
            .unwrap()
            .2;
        assert!(
            matches!(author, WorkerPhase::Failed { .. }),
            "every attempt was rejected: {author:?}"
        );
        assert_eq!(
            author_briefs, 1,
            "brief posted once across {MAX_SPAWN_ATTEMPTS} attempts"
        );
    }

    #[test]
    fn conductor_continues_past_per_worker_errors_and_retries_rejected_retire() {
        let bus = MiniBus::new();
        let host = FakeHost::default();
        *host.unreachable_role.lock().unwrap() = Some("author".into());
        let mut conductor = WaveConductor::new(host.clone());
        let op = BusClient {
            me: Principal::Operator {
                seat: SeatName::parse("owner").unwrap(),
            },
            bus: bus.clone(),
        };
        let sys_conductor = BusClient {
            me: Principal::System {
                part: SystemPart::WaveConductor,
            },
            bus: bus.clone(),
        };

        let started = block_on(op.call(WaveStart {
            spec: sample_spec(),
            idem: None,
        }))
        .unwrap();
        drain_conductor_allow_err(&mut conductor, &sys_conductor, &bus);
        assert!(
            block_on(conductor.handle_frame(
                Frame::CaughtUp {
                    cursor: Cursor::default()
                },
                &sys_conductor
            ))
            .is_err()
        );
        drain_conductor_allow_err(&mut conductor, &sys_conductor, &bus);

        let wave = WaveSlug::parse("auth-v2").unwrap();
        let snap1 = block_on(op.call(WaveView { wave: wave.clone() })).unwrap();
        let author_p1 = &snap1
            .workers
            .iter()
            .find(|(r, _, _)| r.as_str() == "author")
            .unwrap()
            .2;
        let verifier_p1 = &snap1
            .workers
            .iter()
            .find(|(r, _, _)| r.as_str() == "verifier")
            .unwrap()
            .2;
        assert_eq!(
            author_p1,
            &WorkerPhase::Requested { attempt: 1 },
            "author stays Requested after transient error"
        );
        assert!(
            matches!(verifier_p1, WorkerPhase::Live { .. }),
            "reconcile continued past author failure to spawn verifier"
        );

        *host.unreachable_role.lock().unwrap() = None;
        *host.reject_retire.lock().unwrap() = true;
        block_on(op.call(WaveRetireWorker {
            wave: wave.clone(),
            role: RoleName::parse("verifier").unwrap(),
            reason: None,
        }))
        .unwrap();
        drain_conductor_allow_err(&mut conductor, &sys_conductor, &bus);

        let snap2 = block_on(op.call(WaveView { wave: wave.clone() })).unwrap();
        let verifier_p2 = &snap2
            .workers
            .iter()
            .find(|(r, _, _)| r.as_str() == "verifier")
            .unwrap()
            .2;
        assert!(
            matches!(verifier_p2, WorkerPhase::Retiring { .. }),
            "rejected retire stays Retiring instead of Archived"
        );

        *host.reject_retire.lock().unwrap() = false;
        block_on(op.call(RoomPost::new(
            RoomRef::Id {
                id: started.room.clone(),
            },
            Body::parse("trigger dirty reconcile").unwrap(),
        )))
        .unwrap();
        drain_conductor(&mut conductor, &sys_conductor, &bus);

        let snap3 = block_on(op.call(WaveView { wave: wave.clone() })).unwrap();
        let author_p3 = &snap3
            .workers
            .iter()
            .find(|(r, _, _)| r.as_str() == "author")
            .unwrap()
            .2;
        let verifier_p3 = &snap3
            .workers
            .iter()
            .find(|(r, _, _)| r.as_str() == "verifier")
            .unwrap()
            .2;
        assert!(
            matches!(author_p3, WorkerPhase::Live { .. }),
            "dirty reconcile spawned author on next frame"
        );
        assert!(
            matches!(
                verifier_p3,
                WorkerPhase::Retired {
                    thread: Some(_),
                    ..
                }
            ),
            "dirty reconcile completed retire"
        );

        *host.retire_attempts.lock().unwrap() = 0;
        *host.unreachable_retire.lock().unwrap() = true;
        block_on(op.call(WaveRetireWorker {
            wave,
            role: RoleName::parse("author").unwrap(),
            reason: None,
        }))
        .unwrap();
        drain_conductor_allow_err(&mut conductor, &sys_conductor, &bus);
        for n in 1..=4 {
            block_on(op.call(RoomPost::new(
                RoomRef::Id {
                    id: started.room.clone(),
                },
                Body::parse(format!("offline frame {n}")).unwrap(),
            )))
            .unwrap();
            drain_conductor_allow_err(&mut conductor, &sys_conductor, &bus);
        }
        assert_eq!(
            *host.retire_attempts.lock().unwrap(),
            5,
            "Unreachable retire keeps retrying beyond MAX_RETIRE_ATTEMPTS without burning the rejection cap"
        );
        let failure_idem =
            IdemKey::parse("wave:auth-v2:retire-failed:author:th_auth-v2_author_1").unwrap();
        assert!(
            !bus.lock()
                .unwrap()
                .room_posts
                .contains_key(&(started.room.clone(), failure_idem.clone())),
            "transient Unreachable must not post a permanent retire-failed notice"
        );

        *host.unreachable_retire.lock().unwrap() = false;
        *host.retire_attempts.lock().unwrap() = 0;
        *host.reject_retire.lock().unwrap() = true;
        for n in 1..=5 {
            block_on(op.call(RoomPost::new(
                RoomRef::Id {
                    id: started.room.clone(),
                },
                Body::parse(format!("chat frame {n}")).unwrap(),
            )))
            .unwrap();
            drain_conductor_allow_err(&mut conductor, &sys_conductor, &bus);
        }
        assert_eq!(
            *host.retire_attempts.lock().unwrap(),
            MAX_RETIRE_ATTEMPTS,
            "stuck Rejected retire stops retrying after MAX_RETIRE_ATTEMPTS so chat frames do not stall"
        );
        assert!(
            bus.lock()
                .unwrap()
                .room_posts
                .contains_key(&(started.room, failure_idem)),
            "exhausted retire posts diagnostic notice to wave room"
        );
    }

    #[test]
    fn refusal_survives_retirement_blocks_finish_without_flag_and_tracks_counts_and_runtime_meta() {
        let mut state = WaveState::default();
        let mut seq = 0;
        let op = Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        };
        let sys = Principal::System {
            part: SystemPart::BbBridge,
        };
        let mut spec = sample_spec();
        spec.defaults.account = Some("work".into());
        spec.output_dir = Some(PathBuf::from("artifacts/auth-v2"));
        apply_cmd(
            &mut state,
            &mut seq,
            op.clone(),
            WaveCommand::WaveStart(WaveStart {
                spec: spec.clone(),
                idem: None,
            }),
        )
        .unwrap();

        let author = RoleName::parse("author").unwrap();
        let verifier = RoleName::parse("verifier").unwrap();
        for (role, th) in [(&author, "th_a"), (&verifier, "th_v")] {
            apply_cmd(
                &mut state,
                &mut seq,
                sys.clone(),
                WaveCommand::WaveObserve(WaveObserve {
                    wave: spec.slug.clone(),
                    role: role.clone(),
                    observation: Observation::Spawned {
                        attempt: 1,
                        thread: BbThreadId::parse(th).unwrap(),
                    },
                }),
            )
            .unwrap();
        }

        // RuntimeMeta updates actual model/account and is idempotent on repeat.
        let (evs1, _) = apply_cmd(
            &mut state,
            &mut seq,
            sys.clone(),
            WaveCommand::WaveObserve(WaveObserve {
                wave: spec.slug.clone(),
                role: author.clone(),
                observation: Observation::RuntimeMeta {
                    actual_model: Some(ModelId::parse("gpt-6.1-sol-2026").unwrap()),
                    actual_account: Some("personal".into()),
                },
            }),
        )
        .unwrap();
        assert_eq!(evs1.len(), 1);
        let (evs2, _) = apply_cmd(
            &mut state,
            &mut seq,
            sys.clone(),
            WaveCommand::WaveObserve(WaveObserve {
                wave: spec.slug.clone(),
                role: author.clone(),
                observation: Observation::RuntimeMeta {
                    actual_model: Some(ModelId::parse("gpt-6.1-sol-2026").unwrap()),
                    actual_account: Some("personal".into()),
                },
            }),
        )
        .unwrap();
        assert!(evs2.is_empty(), "identical RuntimeMeta is idempotent");

        apply_cmd(
            &mut state,
            &mut seq,
            sys.clone(),
            WaveCommand::WaveObserve(WaveObserve {
                wave: spec.slug.clone(),
                role: author.clone(),
                observation: Observation::Activity {
                    activity: Activity::Refused {
                        reason: "cyber_safeguard: refusal".into(),
                    },
                },
            }),
        )
        .unwrap();
        apply_cmd(
            &mut state,
            &mut seq,
            sys.clone(),
            WaveCommand::WaveObserve(WaveObserve {
                wave: spec.slug.clone(),
                role: verifier.clone(),
                observation: Observation::Activity {
                    activity: Activity::Idle,
                },
            }),
        )
        .unwrap();

        let (_, reply) = apply_cmd(
            &mut state,
            &mut seq,
            op.clone(),
            WaveCommand::WaveView(WaveView {
                wave: spec.slug.clone(),
            }),
        )
        .unwrap();
        let WireReply::WaveView(snap) = reply else {
            panic!("expected WaveView");
        };
        assert_eq!(snap.counts.total, 2);
        assert_eq!(snap.counts.refused, 1);
        assert_eq!(snap.counts.idle, 1);
        let author_meta = &snap.worker_meta[&author];
        assert_eq!(
            author_meta.actual_model.as_ref().map(ModelId::as_str),
            Some("gpt-6.1-sol-2026")
        );
        assert!(author_meta.account_mismatch());
        assert_eq!(
            author_meta.run_stamp.as_deref(),
            Some("auth-v2:author:attempt-1:at-1000")
        );

        // A racing Activity::Errored must never overwrite Activity::Refused on the same attempt.
        let (dup_evs, _) = apply_cmd(
            &mut state,
            &mut seq,
            sys.clone(),
            WaveCommand::WaveObserve(WaveObserve {
                wave: spec.slug.clone(),
                role: author.clone(),
                observation: Observation::Activity {
                    activity: Activity::Errored {
                        error: "generic thread.failed".into(),
                    },
                },
            }),
        )
        .unwrap();
        assert!(
            dup_evs.is_empty(),
            "Errored must not overwrite Refused on the same live attempt"
        );

        // A Live worker cannot bypass allow_incomplete or override paying account.
        let worker_caller = Principal::Worker {
            wave: spec.slug.clone(),
            role: verifier.clone(),
        };
        let bypass_err = apply_cmd(
            &mut state,
            &mut seq,
            worker_caller.clone(),
            WaveCommand::WaveFinish(WaveFinish {
                wave: spec.slug.clone(),
                outcome: WaveOutcome::Completed {
                    summary: Body::parse("bypass").unwrap(),
                },
                allow_incomplete: true,
            }),
        )
        .unwrap_err();
        assert!(
            matches!(bypass_err, ApiError::Denied { .. }),
            "worker caller must be denied allow_incomplete override: {bypass_err:?}"
        );
        let acct_override_err = apply_cmd(
            &mut state,
            &mut seq,
            worker_caller,
            WaveCommand::WaveAddWorker(WaveAddWorker {
                wave: spec.slug.clone(),
                worker: WorkerSpec {
                    role: RoleName::parse("rogue").unwrap(),
                    brief: Body::parse("Switch billing").unwrap(),
                    agent: Some(AgentSpec {
                        provider: ProviderId::parse("codex").unwrap(),
                        model: None,
                        reasoning: None,
                        service_tier: None,
                        permission: PermissionMode::AcceptEdits,
                        workspace: Workspace::ProjectDefault,
                        account: Some("other-billing@example.com".into()),
                    }),
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                },
            }),
        )
        .unwrap_err();
        assert!(
            matches!(acct_override_err, ApiError::Denied { .. }),
            "worker caller must be denied overriding paying account: {acct_override_err:?}"
        );

        let err = apply_cmd(
            &mut state,
            &mut seq,
            op.clone(),
            WaveCommand::WaveFinish(WaveFinish {
                wave: spec.slug.clone(),
                outcome: WaveOutcome::Completed {
                    summary: Body::parse("ship").unwrap(),
                },
                allow_incomplete: false,
            }),
        )
        .unwrap_err();
        assert!(
            matches!(err, ApiError::Conflict { .. })
                && err.to_string().contains("refused")
                && err.to_string().contains("@author"),
            "WaveFinish must reject refused worker without --allow-incomplete: {err:?}"
        );

        // With allow_incomplete: true it succeeds and preserves refused through Retiring -> Retired.
        apply_cmd(
            &mut state,
            &mut seq,
            op.clone(),
            WaveCommand::WaveFinish(WaveFinish {
                wave: spec.slug.clone(),
                outcome: WaveOutcome::Completed {
                    summary: Body::parse("partial").unwrap(),
                },
                allow_incomplete: true,
            }),
        )
        .unwrap();
        for role in [&author, &verifier] {
            apply_cmd(
                &mut state,
                &mut seq,
                sys.clone(),
                WaveCommand::WaveObserve(WaveObserve {
                    wave: spec.slug.clone(),
                    role: role.clone(),
                    observation: Observation::Archived,
                }),
            )
            .unwrap();
        }
        let (_, reply2) = apply_cmd(
            &mut state,
            &mut seq,
            op.clone(),
            WaveCommand::WaveView(WaveView {
                wave: spec.slug.clone(),
            }),
        )
        .unwrap();
        let WireReply::WaveView(snap2) = reply2 else {
            panic!("expected WaveView");
        };
        assert_eq!(snap2.counts.retired_refused, 1);
        assert_eq!(snap2.counts.retired_clean, 1);
        let author_phase = &snap2
            .workers
            .iter()
            .find(|(r, _, _)| r == &author)
            .unwrap()
            .2;
        assert_eq!(author_phase.refusal(), Some("cyber_safeguard: refusal"));
    }

    #[test]
    fn structural_one_writer_enforces_path_safety_and_disjoint_ownership() {
        let op = Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        };

        // 1. Parent traversal `..` in wave output_dir is rejected.
        let mut bad_wave = sample_spec();
        bad_wave.output_dir = Some(PathBuf::from("../escape"));
        let mut st = WaveState::default();
        let mut seq = 0;
        assert!(matches!(
            apply_cmd(
                &mut st,
                &mut seq,
                op.clone(),
                WaveCommand::WaveStart(WaveStart {
                    spec: bad_wave,
                    idem: None,
                }),
            ),
            Err(ApiError::Invalid(_))
        ));

        // 2. Overlapping ancestor/descendant output dirs between workers are rejected.
        let mut overlap = sample_spec();
        overlap.workers[0].output_dir = Some(PathBuf::from("out"));
        overlap.workers[1].output_dir = Some(PathBuf::from("out/sub"));
        let overlap_err = apply_cmd(
            &mut st,
            &mut seq,
            op.clone(),
            WaveCommand::WaveStart(WaveStart {
                spec: overlap,
                idem: None,
            }),
        )
        .unwrap_err();
        assert!(
            matches!(overlap_err, ApiError::Conflict { .. })
                && overlap_err.to_string().contains("@author")
                && overlap_err.to_string().contains("@verifier"),
            "overlapping worker paths must return Conflict: {overlap_err:?}"
        );

        // 3. Sibling default output dirs (<base>/author vs <base>/verifier) succeed,
        //    and WaveAddWorker rejecting overlapping `writes` enforces one-writer dynamically.
        let mut good = sample_spec();
        good.output_dir = Some(PathBuf::from("reports/wave-1"));
        good.workers[0].writes = vec![PathBuf::from("src/auth.rs")];
        good.workers[1].writes = vec![PathBuf::from("tests/auth_test.rs")];
        apply_cmd(
            &mut st,
            &mut seq,
            op.clone(),
            WaveCommand::WaveStart(WaveStart {
                spec: good.clone(),
                idem: None,
            }),
        )
        .unwrap();

        let conflict_add = apply_cmd(
            &mut st,
            &mut seq,
            op,
            WaveCommand::WaveAddWorker(WaveAddWorker {
                wave: good.slug,
                worker: WorkerSpec {
                    role: RoleName::parse("reviewer").unwrap(),
                    brief: Body::parse("Edit auth too").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: vec![PathBuf::from("./src/auth.rs")],
                    return_schema: None,
                },
            }),
        )
        .unwrap_err();
        assert!(
            matches!(conflict_add, ApiError::Conflict { .. })
                && conflict_add.to_string().contains("@author")
                && conflict_add.to_string().contains("@reviewer"),
            "WaveAddWorker must reject normalized overlap: {conflict_add:?}"
        );
        // 4. Non-object return_schema (e.g. `42`) is rejected.
        let mut bad_schema = sample_spec();
        bad_schema.workers[0].return_schema = Some(serde_json::json!(42));
        let mut st_schema = WaveState::default();
        let mut seq_schema = 0;
        assert!(matches!(
            apply_cmd(
                &mut st_schema,
                &mut seq_schema,
                Principal::Operator {
                    seat: SeatName::parse("owner").unwrap(),
                },
                WaveCommand::WaveStart(WaveStart {
                    spec: bad_schema,
                    idem: None,
                }),
            ),
            Err(ApiError::Invalid(_))
        ));
    }

    #[test]
    fn conductor_injects_provenance_reconciles_missing_threads_and_marks_truncation() {
        let bus = MiniBus::new();
        let host = FakeHost::default();
        let mut conductor = WaveConductor::new(host.clone());
        let op = BusClient {
            me: Principal::Operator {
                seat: SeatName::parse("owner").unwrap(),
            },
            bus: bus.clone(),
        };
        let sys_conductor = BusClient {
            me: Principal::System {
                part: SystemPart::WaveConductor,
            },
            bus: bus.clone(),
        };

        let mut spec = sample_spec();
        spec.output_dir = Some(PathBuf::from("runs/auth-v2"));
        spec.workers[0].writes = vec![PathBuf::from("src/token.rs")];
        spec.workers[0].return_schema = Some(serde_json::json!({"type": "object"}));
        let wave = spec.slug.clone();

        block_on(conductor.handle_frame(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &sys_conductor,
        ))
        .unwrap();
        block_on(op.call(WaveStart { spec, idem: None })).unwrap();
        drain_conductor(&mut conductor, &sys_conductor, &bus);

        let reqs = host.spawn_requests.lock().unwrap().clone();
        let author_req = reqs.iter().find(|r| r.role.as_str() == "author").unwrap();
        assert_eq!(author_req.run_stamp, "auth-v2:author:attempt-1:at-100");
        assert_eq!(
            author_req.output_dir.as_deref(),
            Some(Path::new("runs/auth-v2/author"))
        );
        assert_eq!(author_req.writes, vec![PathBuf::from("src/token.rs")]);
        assert!(author_req.prompt.as_str().contains("[rex-wave provenance]"));
        assert!(
            author_req
                .prompt
                .as_str()
                .contains("run_stamp: auth-v2:author:attempt-1:at-100")
        );
        assert!(
            author_req
                .prompt
                .as_str()
                .contains("output_dir: runs/auth-v2/author (relative to workspace root)")
        );
        // Return_schema appears after Assignment so a large schema never clips the brief.
        let assign_pos = author_req.prompt.as_str().find("Assignment:").unwrap();
        let schema_pos = author_req.prompt.as_str().find("return_schema:").unwrap();
        assert!(schema_pos > assign_pos);

        // Unreachable check_live during reconcile must not fail or leave reconcile dirty.
        *host.unreachable_check_live.lock().unwrap() = true;
        let mut offline_restarted = WaveConductor::new(host.clone());
        let history = bus.lock().unwrap().history.clone();
        for env in &history {
            block_on(offline_restarted.handle_frame(Frame::Committed(env.clone()), &sys_conductor))
                .unwrap();
        }
        block_on(offline_restarted.handle_frame(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &sys_conductor,
        ))
        .unwrap();
        assert!(!offline_restarted.needs_reconcile);
        *host.unreachable_check_live.lock().unwrap() = false;

        // Simulate daemon restart: fresh conductor catches up, host reports author thread missing,
        // and each live thread is probed at most once even if reconcile runs again.
        *host.check_live_calls.lock().unwrap() = 0;
        host.missing_threads
            .lock()
            .unwrap()
            .insert("th_auth-v2_author_1".into());
        let mut restarted = WaveConductor::new(host.clone());
        for env in history {
            block_on(restarted.handle_frame(Frame::Committed(env), &sys_conductor)).unwrap();
        }
        block_on(restarted.handle_frame(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &sys_conductor,
        ))
        .unwrap();
        let probes_after_first = *host.check_live_calls.lock().unwrap();
        assert_eq!(probes_after_first, 2);
        block_on(restarted.handle_frame(
            Frame::CaughtUp {
                cursor: Cursor::default(),
            },
            &sys_conductor,
        ))
        .unwrap();
        assert_eq!(
            *host.check_live_calls.lock().unwrap(),
            probes_after_first,
            "reconcile must not re-probe already-probed live threads"
        );
        let snap = block_on(op.call(WaveView { wave })).unwrap();
        let author_phase = &snap
            .workers
            .iter()
            .find(|(r, _, _)| r.as_str() == "author")
            .unwrap()
            .2;
        assert!(
            author_phase
                .error()
                .is_some_and(|e| e.contains("missing after restart")),
            "reconcile must surface missing host thread instead of fake Live/Working: {author_phase:?}"
        );

        // Doorbell_text marks truncation explicitly instead of silently clipping.
        let huge = Body::parse("a".repeat(64 * 1024)).unwrap();
        let room = RoomId::Channel {
            scope: RoomScope::Global,
            name: ChannelName::parse("general").unwrap(),
        };
        let bell = doorbell_text(&room, Seq(7), &op.me, &huge);
        assert!(bell.as_str().len() <= 64 * 1024);
        assert!(
            bell.as_str().contains("[truncated:") && bell.as_str().contains("bytes shown]"),
            "doorbell must include explicit truncation marker: {}",
            &bell.as_str()[bell.as_str().len().saturating_sub(120)..]
        );
    }
}
