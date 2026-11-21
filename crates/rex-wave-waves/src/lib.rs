//! Wave decider (`Waves`), `AgentHost` port, and `WaveConductor` reactor.

use rex_wave_api::board::{BoardEnsure, BoardId, project_board};
use rex_wave_api::room::{
    Body, Mentions, RoomEnsure, RoomEvent, RoomId, RoomJoin, RoomOrigin, RoomPost, RoomRef,
    RoomScope,
};
use rex_wave_api::wave::*;
use rex_wave_api::*;
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta, LocalClient, Reactor};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

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
    let text = truncate_utf8(body.as_str(), max_body);
    Body::parse(format!("{prefix}{text}{suffix}")).expect("doorbell fits within 64 KiB")
}

fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

fn bounded_body(raw: String) -> Body {
    let trimmed = truncate_utf8(&raw, 64 * 1024);
    Body::parse(trimmed).expect("non-empty bounded body")
}

pub struct Waves;

#[derive(Clone, Debug)]
struct WorkerRecord {
    agent: AgentSpec,
    brief: Body,
    phase: WorkerPhase,
    resolved_interaction: Option<(BbInteractionId, InteractionResolution)>,
}

#[derive(Clone, Debug, Default)]
pub struct WaveState {
    spec: Option<WaveSpec>,
    room: Option<RoomId>,
    board: Option<BoardId>,
    opener: Option<Principal>,
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
        if matches!(who, Principal::Operator { .. } | Principal::System { .. })
            || self.is_live_member(wave, who)
        {
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
                if !matches!(ctx.by, Principal::Operator { .. }) {
                    return Err(ApiError::Denied {
                        principal: ctx.by.to_string(),
                        action: "start wave".into(),
                    });
                }
                let mut seen_roles = BTreeSet::new();
                for w in &c.spec.workers {
                    if !seen_roles.insert(&w.role) {
                        return Err(ApiError::Invalid(InvalidId {
                            what: "worker role".into(),
                            reason: format!("duplicate role {}", w.role),
                        }));
                    }
                }
                if let Some(existing) = &state.spec {
                    if existing == &c.spec {
                        return Ok(vec![]);
                    }
                    return Err(ApiError::Conflict {
                        detail: format!(
                            "wave {} already exists with a different spec",
                            c.spec.slug
                        ),
                    });
                }
                let room = wave_room(&c.spec.slug, &c.spec.project);
                let board = project_board(&c.spec.project);
                let mut events = Vec::with_capacity(1 + c.spec.workers.len());
                events.push(WaveEvent::Started {
                    spec: c.spec.clone(),
                    room,
                    board,
                });
                for w in &c.spec.workers {
                    let agent = w.agent.clone().unwrap_or_else(|| c.spec.defaults.clone());
                    events.push(WaveEvent::WorkerRequested {
                        role: w.role.clone(),
                        agent,
                        brief: w.brief.clone(),
                        attempt: 1,
                    });
                }
                Ok(events)
            }
            WaveCommand::WaveAddWorker(c) => {
                let spec = state.require_started(&c.wave)?;
                state.authorize_wave_mutation(&c.wave, &ctx.by, "add worker")?;
                if !matches!(state.status, Some(WaveStatus::Running)) {
                    return Err(ApiError::Conflict {
                        detail: format!("wave {} is already finished", c.wave),
                    });
                }
                if state.workers.contains_key(&c.worker.role) {
                    return Err(ApiError::Conflict {
                        detail: format!(
                            "worker @{} already exists in wave {}",
                            c.worker.role, c.wave
                        ),
                    });
                }
                let agent = c
                    .worker
                    .agent
                    .clone()
                    .unwrap_or_else(|| spec.defaults.clone());
                Ok(vec![WaveEvent::WorkerRequested {
                    role: c.worker.role.clone(),
                    agent,
                    brief: c.worker.brief.clone(),
                    attempt: 1,
                }])
            }
            WaveCommand::WaveRetireWorker(c) => {
                state.require_started(&c.wave)?;
                state.authorize_wave_mutation(&c.wave, &ctx.by, "retire worker")?;
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
                } = &ctx.by
                    && (caller_wave != &c.wave || !state.is_live_member(&c.wave, &ctx.by))
                {
                    return Err(ApiError::Denied {
                        principal: ctx.by.to_string(),
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
                    return Err(ApiError::Conflict {
                        detail: format!(
                            "worker @{}/{} is not live ({:?})",
                            c.wave, c.role, rec.phase
                        ),
                    });
                }
                Ok(vec![WaveEvent::Told {
                    role: c.role.clone(),
                    body: c.body.clone(),
                    mode: c.mode,
                }])
            }
            WaveCommand::WaveResolve(c) => {
                if !matches!(ctx.by, Principal::Operator { .. }) {
                    return Err(ApiError::Denied {
                        principal: ctx.by.to_string(),
                        action: "resolve interaction".into(),
                    });
                }
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
                            return Err(ApiError::Conflict {
                                detail: format!(
                                    "worker @{}/{} is blocked on {}, not {}",
                                    c.wave, c.role, interaction.id, c.interaction
                                ),
                            });
                        }
                        if let Some((prev_id, prev_res)) = &rec.resolved_interaction
                            && prev_id == &c.interaction
                        {
                            if prev_res == &c.resolution {
                                return Ok(vec![]);
                            }
                            return Err(ApiError::Conflict {
                                detail: format!(
                                    "interaction {} was already resolved for @{}/{}",
                                    c.interaction, c.wave, c.role
                                ),
                            });
                        }
                        Ok(vec![WaveEvent::InteractionResolved {
                            role: c.role.clone(),
                            interaction: c.interaction.clone(),
                            resolution: c.resolution.clone(),
                        }])
                    }
                    other => Err(ApiError::Conflict {
                        detail: format!(
                            "worker @{}/{} has no pending interaction ({other:?})",
                            c.wave, c.role
                        ),
                    }),
                }
            }
            WaveCommand::WaveFinish(c) => {
                state.require_started(&c.wave)?;
                state.authorize_wave_mutation(&c.wave, &ctx.by, "finish wave")?;
                if let Some(WaveStatus::Finished { outcome }) = &state.status {
                    if outcome == &c.outcome {
                        return Ok(vec![]);
                    }
                    return Err(ApiError::Conflict {
                        detail: format!("wave {} is already finished", c.wave),
                    });
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
                if !matches!(ctx.by, Principal::System { .. }) {
                    return Err(ApiError::Denied {
                        principal: ctx.by.to_string(),
                        action: "observe worker".into(),
                    });
                }
                state.require_started(&c.wave)?;
                let Some(rec) = state.workers.get(&c.role) else {
                    return Err(ApiError::NotFound {
                        what: format!("worker @{}/{}", c.wave, c.role),
                    });
                };
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
                state.status = Some(WaveStatus::Running);
            }
            WaveEvent::WorkerRequested {
                role,
                agent,
                brief,
                attempt,
            } => {
                state.workers.insert(
                    role.clone(),
                    WorkerRecord {
                        agent: agent.clone(),
                        brief: brief.clone(),
                        phase: WorkerPhase::Requested { attempt: *attempt },
                        resolved_interaction: None,
                    },
                );
            }
            WaveEvent::WorkerObserved { role, observation } => {
                if let Some(rec) = state.workers.get_mut(role) {
                    rec.phase = rec.phase.observe(observation);
                    rec.resolved_interaction = None;
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
                let workers = state
                    .workers
                    .iter()
                    .map(|(role, rec)| (role.clone(), rec.agent.clone(), rec.phase.clone()))
                    .collect();
                let _ = c;
                WireReply::WaveView(WaveSnapshot {
                    spec,
                    status,
                    workers,
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
}

pub trait AgentHost: Send + Sync + 'static {
    fn spawn(
        &self,
        req: SpawnRequest,
    ) -> impl Future<Output = Result<BbThreadId, HostError>> + Send;
    fn tell(
        &self,
        thread: &BbThreadId,
        body: &Body,
        mode: SteerDeliveryMode,
    ) -> impl Future<Output = Result<(), HostError>> + Send;
    fn resolve(
        &self,
        thread: &BbThreadId,
        interaction: &BbInteractionId,
        resolution: &InteractionResolution,
    ) -> impl Future<Output = Result<(), HostError>> + Send;
    fn retire(&self, thread: &BbThreadId) -> impl Future<Output = Result<(), HostError>> + Send;
}

pub struct WaveConductor<H: AgentHost> {
    host: H,
    view: BTreeMap<WaveSlug, WaveState>,
    room_waves: BTreeMap<RoomId, WaveSlug>,
    opened_rooms: BTreeSet<RoomId>,
    opened_boards: BTreeSet<BoardId>,
    room_members: BTreeMap<RoomId, BTreeSet<Principal>>,
    retire_attempts: BTreeMap<BbThreadId, u32>,
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
                RoomEvent::Posted { .. } | RoomEvent::TopicSet { .. } => {}
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
                    WorkerPhase::Live {
                        thread,
                        activity: Activity::Blocked { interaction },
                    } => {
                        if let Some((resolved_id, resolution)) = &rec.resolved_interaction
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
                if idem.as_ref().is_some_and(|k| {
                    k.as_str().starts_with("wave:") && k.as_str().contains(":turn:")
                }) {
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
                room: RoomRef::Id { id: room.clone() },
                body: brief_post,
                reply_to: None,
                idem: Some(idem),
            })
            .await?;

        let prompt = bounded_body(format!(
            "Wave: {wave}\nRole: @{role}\nRoom: #wave-{wave} (project {})\nGoal: {}\n\nAssignment:\n{}",
            spec.project,
            spec.goal.as_str(),
            rec.brief.as_str()
        ));
        let req = SpawnRequest {
            wave: wave.clone(),
            role: role.clone(),
            attempt,
            project: spec.project.clone(),
            agent: rec.agent.clone(),
            room: room.clone(),
            prompt,
            title: format!("[{wave}] @{role}"),
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
                            room: RoomRef::Id { id: room },
                            body,
                            reply_to: None,
                            idem,
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
            workers: vec![
                WorkerSpec {
                    role: RoleName::parse("author").unwrap(),
                    brief: Body::parse("Own token refresh").unwrap(),
                    agent: None,
                },
                WorkerSpec {
                    role: RoleName::parse("verifier").unwrap(),
                    brief: Body::parse("Verify refresh edge cases").unwrap(),
                    agent: Some(sample_agent(
                        "claude-code",
                        "claude-opus-5-5",
                        Workspace::ProjectDefault,
                    )),
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
        let ctx = DecideCtx {
            by: by.clone(),
            at: UnixMs(1_000),
        };
        let stream = Waves::address(&cmd, &ctx)?;
        let events = Waves::decide(state, &cmd, &ctx)?;
        let mut committed = Vec::new();
        for ev in events {
            *seq += 1;
            let meta = EntryMeta {
                seq: Seq(*seq),
                at: ctx.at,
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
        apply_cmd(
            &mut state,
            &mut seq,
            op.clone(),
            WaveCommand::WaveFinish(WaveFinish {
                wave: spec.slug.clone(),
                outcome: WaveOutcome::Completed {
                    summary: Body::parse("done").unwrap(),
                },
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
            }
        );
        assert_eq!(
            retired["verifier"],
            WorkerPhase::Retired {
                thread: Some(BbThreadId::parse("th_verifier").unwrap()),
                errored: None,
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
        unreachable_role: Arc<Mutex<Option<String>>>,
        rejected_role: Arc<Mutex<Option<String>>>,
        unreachable_retire: Arc<Mutex<bool>>,
        reject_retire: Arc<Mutex<bool>>,
        retire_attempts: Arc<Mutex<u32>>,
    }

    impl AgentHost for FakeHost {
        async fn spawn(&self, req: SpawnRequest) -> Result<BbThreadId, HostError> {
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
            Ok(BbThreadId::parse(format!("th_{}_{}_{}", req.wave, req.role, req.attempt)).unwrap())
        }

        async fn tell(
            &self,
            thread: &BbThreadId,
            body: &Body,
            mode: SteerDeliveryMode,
        ) -> Result<(), HostError> {
            self.calls.lock().unwrap().push(HostCall::Tell {
                thread: thread.to_string(),
                mode,
                text: body.as_str().to_owned(),
            });
            Ok(())
        }

        async fn resolve(
            &self,
            thread: &BbThreadId,
            interaction: &BbInteractionId,
            resolution: &InteractionResolution,
        ) -> Result<(), HostError> {
            self.calls.lock().unwrap().push(HostCall::Resolve {
                thread: thread.to_string(),
                interaction: interaction.to_string(),
                resolution: resolution.clone(),
            });
            Ok(())
        }

        async fn retire(&self, thread: &BbThreadId) -> Result<(), HostError> {
            *self.retire_attempts.lock().unwrap() += 1;
            if *self.unreachable_retire.lock().unwrap() {
                return Err(HostError::Unreachable("BB bridge disconnected".into()));
            }
            if *self.reject_retire.lock().unwrap() {
                return Err(HostError::Rejected("thread remained stopping".into()));
            }
            self.calls.lock().unwrap().push(HostCall::Retire {
                thread: thread.to_string(),
            });
            Ok(())
        }
    }

    struct MiniBus {
        wave_states: BTreeMap<WaveSlug, WaveState>,
        room_members: BTreeMap<RoomId, BTreeMap<Handle, Principal>>,
        room_posts: BTreeMap<(RoomId, IdemKey), Seq>,
        boards: BTreeSet<BoardId>,
        seqs: BTreeMap<StreamId, u64>,
        queue: VecDeque<Envelope>,
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
                        let ctx = DecideCtx {
                            by: me.clone(),
                            at: UnixMs(100),
                        };
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
                                    reply_to: c.reply_to,
                                    idem: c.idem.clone(),
                                }),
                            );
                            if let Some(idem) = c.idem {
                                g.room_posts.insert((id.clone(), idem), env.seq);
                            }
                            Ok(WireReply::RoomPost(Posted {
                                room: id,
                                seq: env.seq,
                                mentions,
                            }))
                        }
                        RoomCommand::RoomLeave(_) => Ok(WireReply::RoomLeave(Ack)),
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

        block_on(op.call(RoomPost {
            room: RoomRef::Id {
                id: started.room.clone(),
            },
            body: Body::parse("@all status check").unwrap(),
            reply_to: None,
            idem: None,
        }))
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
        block_on(op.call(RoomPost {
            room: RoomRef::Dm {
                with: Principal::Worker {
                    wave: wave.clone(),
                    role: author.clone(),
                },
            },
            body: Body::parse("urgent DM").unwrap(),
            reply_to: None,
            idem: None,
        }))
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
        block_on(op.call(RoomPost {
            room: RoomRef::Id { id: started.room },
            body: Body::parse("@all historical message").unwrap(),
            reply_to: None,
            idem: None,
        }))
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

        block_on(op.call(RoomPost {
            room: RoomRef::Id { id: started.room },
            body: Body::parse("@acp-helper check token TTL").unwrap(),
            reply_to: None,
            idem: None,
        }))
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
        block_on(op.call(RoomPost {
            room: RoomRef::Id {
                id: started.room.clone(),
            },
            body: Body::parse("trigger dirty reconcile").unwrap(),
            reply_to: None,
            idem: None,
        }))
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
            block_on(op.call(RoomPost {
                room: RoomRef::Id {
                    id: started.room.clone(),
                },
                body: Body::parse(format!("offline frame {n}")).unwrap(),
                reply_to: None,
                idem: None,
            }))
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
            block_on(op.call(RoomPost {
                room: RoomRef::Id {
                    id: started.room.clone(),
                },
                body: Body::parse(format!("chat frame {n}")).unwrap(),
                reply_to: None,
                idem: None,
            }))
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
}
