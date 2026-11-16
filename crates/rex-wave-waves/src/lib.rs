//! # rex-wave-waves
//!
//! Three things, all about one piece of knowledge, *a wave and its workers*:
//!
//! * [`Waves`], the pure decider. Owns roster, worker phases, outcome.
//! * [`AgentHost`], the port to whatever runs agent threads. Implemented by
//!   `rex-wave-bb` (BB, any provider). Tests use a fake.
//! * [`WaveConductor`], the reactor that turns wave events into effects:
//!   ensure `#wave-<slug>` under the BB project + the project board (via
//!   `Client`, never by importing rooms/board), post briefs, spawn/retire BB
//!   threads, forward tells and resolutions, and ring the **doorbell**: when a
//!   post in the wave room @mentions a worker whose activity is `Idle`, send
//!   it a BB `threads.send(mode: auto)` so its next turn starts. Workers that
//!   are `Parked` inside `wait` need nothing, `wait` returns on its own.
//!
//! The conductor is level-triggered: on boot it replays every wave stream
//! and reconciles (requested-but-unspawned workers get spawned, retiring
//! workers get archived). Every effect is idempotent:
//! spawns are keyed by `pluginMetadata {waveId, role, attempt}` and the host
//! looks up an existing thread before creating one; briefs are posted with
//! `IdemKey = "wave:<slug>:brief:<role>:<attempt>"`.

// Sketch phase: bodies are `todo!()`, so private state reads as dead code until Phase D.
#![allow(dead_code)]

use rex_wave_api::room::{Body, RoomId, RoomScope};
use rex_wave_api::wave::*;
use rex_wave_api::*;
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta, LocalClient, Reactor};
use std::collections::BTreeMap;
use std::future::Future;


/// The wave room's identity: `#wave-<slug>` under the wave's BB project.
pub fn wave_room(slug: &WaveSlug, project: &BbProjectId) -> RoomId {
    let name = ChannelName::parse(format!("wave-{slug}")).expect("wave slug is a valid channel suffix");
    RoomId::Channel { scope: RoomScope::Project { project: project.clone() }, name }
}

/// Max spawn attempts per worker before `Failed`.
pub const MAX_SPAWN_ATTEMPTS: u32 = 3;

/// Pure worker transition. `Err` = the observation does not apply in this phase.
pub fn worker_step(phase: &WorkerPhase, obs: &Observation) -> Result<WorkerPhase, String> {
    let _ = (phase, obs);
    // Requested{n} + Spawned{n,t}      -> Live{t, Working}
    // Requested{n} + SpawnFailed{n}    -> Requested{n+1} | Failed (n+1 > MAX)
    // Live{t,_}    + Activity{a}       -> Live{t,a}
    // Retiring{t}  + Archived          -> Retired{Some(t)}
    // stale attempt numbers            -> Err (idempotent no-op upstream)
    todo!("worker_step")
}

/// Pure: the text a doorbell delivers into an idle worker's BB thread.
pub fn doorbell_text(room: &RoomId, seq: Seq, from: &Principal, body: &Body) -> Body {
    let _ = (room, seq, from, body);
    todo!("doorbell_text: '[#wave-x seq=N from @a] <body>  (reply with room_post; wait to park)'")
}


pub struct Waves;

#[derive(Default)]
pub struct WaveState {
    spec: Option<WaveSpec>,
    room: Option<RoomId>,
    workers: BTreeMap<RoleName, (AgentSpec, WorkerPhase)>,
    status: Option<WaveStatus>,
    head: Seq,
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

    fn decide(state: &WaveState, cmd: &WaveCommand, ctx: &DecideCtx) -> Result<Vec<WaveEvent>, ApiError> {
        let _ = (state, cmd, ctx);
        // Start:  Conflict if spec exists and differs; [] if identical (idempotent);
        //         else [Started{room: wave_room(..), board: Project}, WorkerRequested{attempt:1}..]
        // AddWorker: role unique; caller must be operator or a live worker of this wave.
        // Retire: Live -> [WorkerRetiring]; already Retiring/Retired -> [].
        // Tell / Resolve: worker must be Live (Resolve: interaction must match Blocked).
        // Observe: worker_step(); Err -> [] (stale/duplicate observation is a no-op).
        // Finish: [Finished] + [WorkerRetiring] for every Live worker.
        todo!("Waves::decide")
    }

    fn evolve(state: &mut WaveState, event: &WaveEvent, meta: &EntryMeta) {
        let _ = (state, event, meta);
        todo!("Waves::evolve")
    }

    fn reply(state: &WaveState, cmd: WaveCommand, committed: &[Envelope]) -> WireReply {
        let _ = (state, cmd, committed);
        todo!("Waves::reply: WaveStarted | Ack | WaveSnapshot")
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

/// Everything needed to start one worker thread.
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

/// What runs agent threads. One implementation today: BB via `rex-wave-bb`.
pub trait AgentHost: Send + Sync + 'static {
    /// Idempotent per `(wave, role, attempt)`: returns the existing thread if one
    /// already carries that metadata.
    fn spawn(&self, req: SpawnRequest) -> impl Future<Output = Result<BbThreadId, HostError>> + Send;
    fn tell(&self, thread: &BbThreadId, body: &Body, mode: SteerDeliveryMode) -> impl Future<Output = Result<(), HostError>> + Send;
    fn resolve(&self, thread: &BbThreadId, interaction: &BbInteractionId, resolution: &InteractionResolution) -> impl Future<Output = Result<(), HostError>> + Send;
    /// Stop then archive (hidden BB threads keep runtimes loaded otherwise, #12).
    fn retire(&self, thread: &BbThreadId) -> impl Future<Output = Result<(), HostError>> + Send;
}


/// Turns wave events into BB effects and room/board setup. See crate docs.
pub struct WaveConductor<H: AgentHost> {
    host: H,
    /// Derived view of worker phases + room->wave map, folded from frames.
    view: BTreeMap<WaveSlug, WaveState>,
}

impl<H: AgentHost> WaveConductor<H> {
    pub fn new(host: H) -> Self {
        WaveConductor { host, view: BTreeMap::new() }
    }
}

impl<H: AgentHost> Reactor for WaveConductor<H> {
    const NAME: &'static str = "wave-conductor";

    fn interest(&self) -> Interest {
        Interest::Everything
    }

    fn react(&mut self, frame: Frame, client: &LocalClient) -> impl Future<Output = Result<(), ApiError>> + Send {
        let _ = (&self.host, &self.view, frame, client);
        async move {
            // Started          -> room_ensure(origin Wave) + board_ensure + room_join(roster, operator)
            // WorkerRequested  -> room_post(@role brief, idem) ; host.spawn ; wave_observe(Spawned|SpawnFailed)
            // Told             -> host.tell ; InteractionResolved -> host.resolve
            // WorkerRetiring   -> host.retire ; wave_observe(Archived)
            // Room Posted in a wave room mentioning an Idle worker (or @all) -> host.tell(Auto, doorbell_text)
            todo!("WaveConductor::react")
        }
    }
}
