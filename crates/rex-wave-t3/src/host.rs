//! `T3Bridge`, `T3Host` (`AgentHost`), `T3Feed` (`AgentFeed`), and live T3 shell reactor.

use crate::discovery::{T3Config, percent_encode_query, state_db_path};
use crate::projects::{
    T3ProjectInfo, load_t3_project_names_from_sqlite, parse_t3_projects_snapshot,
    project_names_map, resolve_or_ensure_t3_project,
};
use crate::protocol::{
    ExplicitFailurePlan, InspectedTurn, LiveSeqTracker, PendingTicket, ShellObservation,
    TerminalTurnState, TurnMirrorCheck, active_pending_interaction, active_unmatched_tickets,
    all_matching_run_ticket_ids, any_turn_inherently_suppressed, bind_ticket_anchor_keys,
    closed_turn_count_before_ahead_failure, collect_turn_candidates_in_range,
    default_model_from_server_config, derive_shell_observation, deterministic_thread_id,
    explicit_shell_suppression_keys, extract_explicit_stop_refusal, extract_refusal_reason,
    extract_runtime_meta_from_value, force_stop_thread_payload,
    inspect_closed_turns_before_ahead_failure, inspect_turns_in_order, is_t3_thread_id,
    launch_thread_payload, matching_run_ticket_ids_for_target, message_dispatch_payload,
    mirror_idem_key, next_failure_ticket_key, normalize_turn_item,
    parse_pending_interaction_from_projection, parse_provider_turns_usage, parse_t3_thread_id,
    plan_explicit_failure_turns, projection_has_ahead_suppression, projection_run_ticket_anchors,
    projection_slot_eligible_tickets, projection_slots_have_eligible_ticket,
    projection_turn_slot_count, resolve_ticket_ids, runtime_request_respond_payload,
    shell_explicit_turn_target, strip_t3_provider_prefix, terminal_suppressed_lookup_keys,
    thread_archive_payload, thread_stop_payload, ticket_causally_eligible_at_pos,
    unwrap_projection,
};
use crate::rpc::{StreamEvent, T3RpcClient};
use crate::worker_tokens::{WorkerTokenMinter, provision_worker_token, remove_worker_token_file};
use rex_wave_api::read::{Read, ThreadPageView};
use rex_wave_api::room::{Body, RoomEnsure, RoomEvent, RoomJoin, RoomOrigin, RoomPost, RoomRef};
use rex_wave_api::wave::{
    Activity, InteractionResolution, Observation, PendingInteraction, SteerDeliveryMode, WaveEvent,
    WaveObserve, WaveView, WorkerPhase,
};
use rex_wave_api::*;
pub use rex_wave_runtime::BridgeBackend;
use rex_wave_runtime::{AgentFeed, BoxFut, LocalClient, Reactor, Router, spawn_reactor};
use rex_wave_waves::{AgentHost, HostError, SpawnRequest};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{mpsc, oneshot, watch};

trait ErasedBackend: Send + Sync {
    fn call_as(&self, me: Principal, cmd: WireCommand) -> BoxFut<'_, Result<WireReply, ApiError>>;
}

impl<B: BridgeBackend> ErasedBackend for B {
    fn call_as(&self, me: Principal, cmd: WireCommand) -> BoxFut<'_, Result<WireReply, ApiError>> {
        let client = self.client(me);
        Box::pin(async move { client.call_wire(cmd).await })
    }
}

#[derive(Clone, Debug)]
struct TrackedWorker {
    attempt: u32,
    thread: Option<BbThreadId>,
    live: bool,
    activity: Activity,
    pending_interaction: Option<PendingInteraction>,
    shell_epoch: u64,
    mirror_room_watermark: Seq,
    settled_turn_keys: BTreeSet<String>,
    suppressed_turn_keys: BTreeSet<String>,
    claimed_direct_posts: BTreeSet<Seq>,
    hydrated_suppressions: bool,
}

type LiveWorkerRow = (WaveSlug, RoleName, BbThreadId, Activity, u64);
type MirrorStateSnapshot = (Seq, bool, bool, BTreeSet<Seq>);

enum StateMsg {
    BindThread {
        wave: WaveSlug,
        role: RoleName,
        attempt: u32,
        thread: BbThreadId,
    },
    FoldWave {
        wave: WaveSlug,
        event: WaveEvent,
    },
    LookupByThread {
        thread: BbThreadId,
        reply: oneshot::Sender<Option<(WaveSlug, RoleName, u32)>>,
    },
    LookupByRole {
        wave: WaveSlug,
        role: RoleName,
        reply: oneshot::Sender<Option<TrackedWorker>>,
    },
    ListLiveWorkers {
        reply: oneshot::Sender<Vec<LiveWorkerRow>>,
    },
    BumpShellEpoch {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
    },
    MirrorState {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        turn_keys: Vec<String>,
        reply: oneshot::Sender<MirrorStateSnapshot>,
    },
    AdvanceMirrorWatermark {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        seq: Seq,
        turn_keys: Vec<String>,
        mark_suppressed: bool,
        claimed_seqs: Vec<Seq>,
    },
    HydrationStatus {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        reply: oneshot::Sender<bool>,
    },
    SettledTurnKeys {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        reply: oneshot::Sender<BTreeSet<String>>,
    },
    ApplyHydratedSuppressions {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        turn_keys: Vec<String>,
    },
}

struct StateOwner {
    workers_by_role: HashMap<(WaveSlug, RoleName), TrackedWorker>,
    role_by_thread: HashMap<BbThreadId, (WaveSlug, RoleName)>,
}

impl StateOwner {
    fn spawn() -> mpsc::UnboundedSender<StateMsg> {
        let (tx, rx) = mpsc::unbounded_channel();
        let owner = StateOwner {
            workers_by_role: HashMap::new(),
            role_by_thread: HashMap::new(),
        };
        tokio::spawn(owner.run(rx));
        tx
    }

    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<StateMsg>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                StateMsg::BindThread {
                    wave,
                    role,
                    attempt,
                    thread,
                } => {
                    self.role_by_thread
                        .insert(thread.clone(), (wave.clone(), role.clone()));
                    let rec = self
                        .workers_by_role
                        .entry((wave, role))
                        .or_insert(TrackedWorker {
                            attempt,
                            thread: Some(thread.clone()),
                            live: true,
                            activity: Activity::Working,
                            pending_interaction: None,
                            shell_epoch: 0,
                            mirror_room_watermark: Seq::ORIGIN,
                            settled_turn_keys: BTreeSet::new(),
                            suppressed_turn_keys: BTreeSet::new(),
                            claimed_direct_posts: BTreeSet::new(),
                            hydrated_suppressions: false,
                        });
                    if rec.thread.as_ref() != Some(&thread) {
                        rec.shell_epoch = rec.shell_epoch.wrapping_add(1);
                        rec.mirror_room_watermark = Seq::ORIGIN;
                        rec.settled_turn_keys.clear();
                        rec.suppressed_turn_keys.clear();
                        rec.claimed_direct_posts.clear();
                        rec.hydrated_suppressions = false;
                    }
                    rec.attempt = attempt;
                    rec.thread = Some(thread);
                    rec.live = true;
                }
                StateMsg::FoldWave { wave, event } => {
                    self.fold_wave(wave, event);
                }
                StateMsg::LookupByThread { thread, reply } => {
                    let found = self.role_by_thread.get(&thread).and_then(|(w, r)| {
                        let rec = self.workers_by_role.get(&(w.clone(), r.clone()))?;
                        rec.live.then(|| (w.clone(), r.clone(), rec.attempt))
                    });
                    let _ = reply.send(found);
                }
                StateMsg::LookupByRole { wave, role, reply } => {
                    let _ = reply.send(self.workers_by_role.get(&(wave, role)).cloned());
                }
                StateMsg::ListLiveWorkers { reply } => {
                    let mut out = Vec::new();
                    for ((w, r), rec) in &self.workers_by_role {
                        if rec.live
                            && let Some(th) = &rec.thread
                            && is_t3_thread_id(th)
                        {
                            out.push((
                                w.clone(),
                                r.clone(),
                                th.clone(),
                                rec.activity.clone(),
                                rec.shell_epoch,
                            ));
                        }
                    }
                    let _ = reply.send(out);
                }
                StateMsg::BumpShellEpoch { wave, role, thread } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        rec.shell_epoch = rec.shell_epoch.wrapping_add(1);
                    }
                }
                StateMsg::MirrorState {
                    wave,
                    role,
                    thread,
                    turn_keys,
                    reply,
                } => {
                    let st = match self.workers_by_role.get(&(wave, role)) {
                        Some(r) if r.thread.as_ref() == Some(&thread) => {
                            let suppressed = turn_keys
                                .iter()
                                .any(|k| !k.is_empty() && r.suppressed_turn_keys.contains(k));
                            let settled = suppressed
                                || turn_keys
                                    .iter()
                                    .any(|k| !k.is_empty() && r.settled_turn_keys.contains(k));
                            (
                                r.mirror_room_watermark,
                                settled,
                                suppressed,
                                r.claimed_direct_posts.clone(),
                            )
                        }
                        _ => (Seq::ORIGIN, false, false, BTreeSet::new()),
                    };
                    let _ = reply.send(st);
                }
                StateMsg::AdvanceMirrorWatermark {
                    wave,
                    role,
                    thread,
                    seq,
                    turn_keys,
                    mark_suppressed,
                    claimed_seqs,
                } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        if rec.mirror_room_watermark < seq {
                            rec.mirror_room_watermark = seq;
                        }
                        for k in turn_keys {
                            if !k.is_empty() {
                                if mark_suppressed {
                                    rec.suppressed_turn_keys.insert(k.clone());
                                }
                                rec.settled_turn_keys.insert(k);
                            }
                        }
                        for s in claimed_seqs {
                            rec.claimed_direct_posts.insert(s);
                        }
                    }
                }
                StateMsg::HydrationStatus {
                    wave,
                    role,
                    thread,
                    reply,
                } => {
                    let status = match self.workers_by_role.get(&(wave, role)) {
                        Some(rec) if rec.thread.as_ref() == Some(&thread) => {
                            rec.hydrated_suppressions
                        }
                        _ => false,
                    };
                    let _ = reply.send(status);
                }
                StateMsg::SettledTurnKeys {
                    wave,
                    role,
                    thread,
                    reply,
                } => {
                    let keys = match self.workers_by_role.get(&(wave, role)) {
                        Some(rec) if rec.thread.as_ref() == Some(&thread) => {
                            rec.suppressed_turn_keys.clone()
                        }
                        _ => BTreeSet::new(),
                    };
                    let _ = reply.send(keys);
                }
                StateMsg::ApplyHydratedSuppressions {
                    wave,
                    role,
                    thread,
                    turn_keys,
                } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        for k in turn_keys {
                            if !k.is_empty() {
                                rec.suppressed_turn_keys.insert(k.clone());
                                rec.settled_turn_keys.insert(k);
                            }
                        }
                        rec.hydrated_suppressions = true;
                    }
                }
            }
        }
    }

    fn fold_wave(&mut self, wave: WaveSlug, event: WaveEvent) {
        match event {
            WaveEvent::Started { .. } => {}
            WaveEvent::WorkerRequested { role, attempt, .. } => {
                if let Some(prev) = self.workers_by_role.get(&(wave.clone(), role.clone())) {
                    if prev.attempt == attempt && prev.thread.is_some() {
                        return;
                    }
                    if let Some(ref th) = prev.thread {
                        self.role_by_thread.remove(th);
                    }
                }
                self.workers_by_role.insert(
                    (wave, role),
                    TrackedWorker {
                        attempt,
                        thread: None,
                        live: false,
                        activity: Activity::Working,
                        pending_interaction: None,
                        shell_epoch: 0,
                        mirror_room_watermark: Seq::ORIGIN,
                        settled_turn_keys: BTreeSet::new(),
                        suppressed_turn_keys: BTreeSet::new(),
                        claimed_direct_posts: BTreeSet::new(),
                        hydrated_suppressions: false,
                    },
                );
            }
            WaveEvent::WorkerObserved { role, observation } => match observation {
                Observation::Spawned { attempt, thread } => {
                    if !is_t3_thread_id(&thread) {
                        if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
                            if let Some(ref prev_th) = rec.thread {
                                self.role_by_thread.remove(prev_th);
                            }
                            rec.thread = None;
                            rec.live = false;
                            rec.pending_interaction = None;
                            rec.shell_epoch = rec.shell_epoch.wrapping_add(1);
                            rec.mirror_room_watermark = Seq::ORIGIN;
                            rec.settled_turn_keys.clear();
                            rec.suppressed_turn_keys.clear();
                            rec.claimed_direct_posts.clear();
                            rec.hydrated_suppressions = false;
                        }
                        return;
                    }
                    if let Some(prev) = self.workers_by_role.get(&(wave.clone(), role.clone()))
                        && let Some(ref prev_th) = prev.thread
                        && prev_th != &thread
                    {
                        self.role_by_thread.remove(prev_th);
                    }
                    self.role_by_thread
                        .insert(thread.clone(), (wave.clone(), role.clone()));
                    let rec = self
                        .workers_by_role
                        .entry((wave, role))
                        .or_insert(TrackedWorker {
                            attempt,
                            thread: Some(thread.clone()),
                            live: true,
                            activity: Activity::Working,
                            pending_interaction: None,
                            shell_epoch: 0,
                            mirror_room_watermark: Seq::ORIGIN,
                            settled_turn_keys: BTreeSet::new(),
                            suppressed_turn_keys: BTreeSet::new(),
                            claimed_direct_posts: BTreeSet::new(),
                            hydrated_suppressions: false,
                        });
                    if rec.thread.as_ref() != Some(&thread) {
                        rec.shell_epoch = rec.shell_epoch.wrapping_add(1);
                        rec.mirror_room_watermark = Seq::ORIGIN;
                        rec.settled_turn_keys.clear();
                        rec.suppressed_turn_keys.clear();
                        rec.claimed_direct_posts.clear();
                        rec.hydrated_suppressions = false;
                    }
                    rec.attempt = attempt;
                    rec.thread = Some(thread);
                    rec.live = true;
                    rec.activity = Activity::Working;
                    rec.pending_interaction = None;
                }
                Observation::Activity { activity } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
                        rec.pending_interaction = match &activity {
                            Activity::Blocked { interaction } => Some(interaction.clone()),
                            _ => None,
                        };
                        rec.activity = activity;
                    }
                }
                Observation::Archived => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
                        rec.live = false;
                        rec.pending_interaction = None;
                        if let Some(th) = &rec.thread {
                            self.role_by_thread.remove(th);
                        }
                    }
                }
                Observation::SpawnFailed { .. } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
                        rec.live = false;
                    }
                }
                Observation::RuntimeMeta { .. } => {}
                Observation::TurnsSuppressed { thread, keys } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        for k in keys {
                            if !k.is_empty() {
                                rec.suppressed_turn_keys.insert(k.clone());
                                rec.settled_turn_keys.insert(k);
                            }
                        }
                    }
                }
            },
            WaveEvent::WorkerRetiring { role, .. } => {
                if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
                    rec.live = false;
                }
            }
            WaveEvent::Finished { .. } => {
                for ((w, _), rec) in &mut self.workers_by_role {
                    if w == &wave {
                        rec.live = false;
                    }
                }
            }
            WaveEvent::Told { .. } | WaveEvent::InteractionResolved { .. } => {}
        }
    }
}

struct BridgeWaveWatcher {
    bridge: Arc<T3Bridge>,
}

impl Reactor for BridgeWaveWatcher {
    const NAME: &'static str = "t3-bridge-wave-index";

    fn interest(&self) -> Interest {
        Interest::Everything
    }

    async fn react(&mut self, frame: Frame, _client: &LocalClient) -> Result<(), ApiError> {
        match frame {
            Frame::Committed(Envelope {
                stream: StreamId::Wave { wave },
                event: Event::Wave(ev),
                ..
            }) => {
                let _ = self
                    .bridge
                    .state
                    .send(StateMsg::FoldWave { wave, event: ev });
            }
            Frame::CaughtUp { .. } => {
                let bridge = self.bridge.clone();
                tokio::spawn(async move {
                    bridge.reconcile_live_workers().await;
                });
            }
            _ => {}
        }
        Ok(())
    }
}

enum ThreadShellAction {
    Updated {
        location: Option<String>,
        shell: Value,
    },
    Removed,
}

/// Core bridge to a T3 Code 0.1.11 server. Exposes [`T3Host`] (`AgentHost`) and
/// [`T3Feed`] (`AgentFeed`), plus implements `AgentFeed` directly.
pub struct T3Bridge {
    cfg: T3Config,
    rpc: T3RpcClient,
    state: mpsc::UnboundedSender<StateMsg>,
    backend: OnceLock<Arc<dyn ErasedBackend>>,
    token_minter: Option<WorkerTokenMinter>,
    mirror_lock: tokio::sync::Mutex<()>,
    cmd_nonce: AtomicU64,
    project_names_tx: watch::Sender<Arc<BTreeMap<BbProjectId, String>>>,
    project_names_rx: watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>,
}

impl T3Bridge {
    pub fn new(cfg: T3Config) -> Arc<Self> {
        Self::with_token_minter(cfg, None)
    }

    pub fn with_token_minter(cfg: T3Config, token_minter: Option<WorkerTokenMinter>) -> Arc<Self> {
        let rpc = T3RpcClient::new(cfg.clone());
        let (project_names_tx, project_names_rx) = watch::channel(Arc::new(BTreeMap::new()));
        let bridge = Arc::new(T3Bridge {
            cfg,
            rpc,
            state: StateOwner::spawn(),
            backend: OnceLock::new(),
            token_minter,
            mirror_lock: tokio::sync::Mutex::new(()),
            cmd_nonce: AtomicU64::new(1),
            project_names_tx,
            project_names_rx,
        });
        bridge.seed_local_project_names();
        bridge
    }

    pub fn config(&self) -> &T3Config {
        &self.cfg
    }

    pub fn rpc(&self) -> &T3RpcClient {
        &self.rpc
    }

    pub fn connected(&self) -> bool {
        self.rpc.connected()
    }

    pub fn watch_connected(&self) -> watch::Receiver<bool> {
        self.rpc.watch_connected()
    }

    pub fn observe_wave_event(&self, wave: WaveSlug, event: WaveEvent) {
        let _ = self.state.send(StateMsg::FoldWave { wave, event });
    }

    fn next_nonce(&self) -> u64 {
        self.cmd_nonce.fetch_add(1, Ordering::Relaxed)
    }

    fn seed_local_project_names(&self) {
        let Some(t3_home) = self.cfg.t3_home.clone() else {
            return;
        };
        let db_path = state_db_path(&t3_home);
        let tx = self.project_names_tx.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn_blocking(move || {
                let loaded = load_t3_project_names_from_sqlite(&db_path);
                if loaded.is_empty() {
                    return;
                }
                tx.send_if_modified(|cur| {
                    let changed = loaded.iter().any(|(k, v)| cur.get(k) != Some(v));
                    if changed {
                        let mut next = (**cur).clone();
                        next.extend(loaded);
                        *cur = Arc::new(next);
                    }
                    changed
                });
            });
        }
    }

    fn replace_or_merge_project_names(&self, projects: &[T3ProjectInfo], replace: bool) {
        let fresh = project_names_map(projects);
        self.project_names_tx.send_if_modified(|cur| {
            let next = if replace {
                fresh
            } else {
                let mut merged = (**cur).clone();
                merged.extend(fresh);
                merged
            };
            if **cur != next {
                *cur = Arc::new(next);
                true
            } else {
                false
            }
        });
    }

    fn upsert_project_name(&self, project_id: &str, title: &str) {
        let title = title.trim();
        if title.is_empty() {
            return;
        }
        let Ok(pid) = BbProjectId::parse(project_id) else {
            return;
        };
        self.project_names_tx.send_if_modified(|cur| {
            if cur.get(&pid).map(String::as_str) == Some(title) {
                return false;
            }
            let mut next = (**cur).clone();
            next.insert(pid, title.to_owned());
            *cur = Arc::new(next);
            true
        });
    }

    fn remove_project_name(&self, project_id: &str) {
        let Ok(pid) = BbProjectId::parse(project_id) else {
            return;
        };
        self.project_names_tx.send_if_modified(|cur| {
            if !cur.contains_key(&pid) {
                return false;
            }
            let mut next = (**cur).clone();
            next.remove(&pid);
            *cur = Arc::new(next);
            true
        });
    }

    async fn refresh_http_projects(&self) {
        if let Ok(snap) = self.rpc.http_get_json("/api/projects").await {
            let projects = parse_t3_projects_snapshot(&snap);
            if !projects.is_empty() {
                self.replace_or_merge_project_names(&projects, false);
            }
        }
    }

    pub async fn run(self: Arc<Self>, router: Arc<Router>) {
        let sys = LocalClient::new(router.clone(), self.cfg.system_principal.clone());
        let _watcher = spawn_reactor(
            BridgeWaveWatcher {
                bridge: self.clone(),
            },
            sys,
        );
        let (_tx, rx) = watch::channel(false);
        self.run_until(router, rx).await;
    }

    pub async fn run_until<B: BridgeBackend>(
        self: Arc<Self>,
        backend: B,
        shutdown: watch::Receiver<bool>,
    ) {
        let _ = self.backend.set(Arc::new(backend));
        let shell_bridge = self.clone();
        let shell_shutdown = shutdown.clone();
        let shell_task = tokio::spawn(async move {
            shell_bridge.run_shell_subscription(shell_shutdown).await;
        });

        self.rpc.run_until(shutdown).await;
        shell_task.abort();
    }

    async fn run_shell_subscription(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut per_thread: HashMap<BbThreadId, mpsc::UnboundedSender<ThreadShellAction>> =
            HashMap::new();
        let mut last_seq: Option<u64> = None;

        loop {
            if *shutdown.borrow() {
                break;
            }
            let init_payload = match last_seq {
                Some(seq) => json!({ "afterSequence": seq }),
                None => json!({}),
            };
            let Ok(stream) = self
                .rpc
                .open_stream("orchestration.subscribeShell", init_payload)
                .await
            else {
                break;
            };
            let (mut ev_rx, lease) = stream.into_parts();

            let mut ended_by_shutdown = false;
            loop {
                let item = tokio::select! {
                    biased;
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            ended_by_shutdown = true;
                            break;
                        }
                        continue;
                    }
                    ev = ev_rx.recv() => ev,
                };
                let Some(ev) = item else { break };
                match ev {
                    StreamEvent::Reconnected => {
                        let b = self.clone();
                        tokio::spawn(async move {
                            b.refresh_http_projects().await;
                            b.reconcile_live_workers().await;
                        });
                    }
                    StreamEvent::Closed(_) => break,
                    StreamEvent::Values(values) => {
                        for val in values {
                            self.handle_shell_item(&val, &mut last_seq, &lease, &mut per_thread)
                                .await;
                        }
                    }
                }
            }
            drop(lease);
            if ended_by_shutdown || *shutdown.borrow() {
                break;
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                () = tokio::time::sleep(self.cfg.reconnect_min) => {}
            }
        }
    }

    async fn handle_shell_item(
        self: &Arc<Self>,
        item: &Value,
        last_seq: &mut Option<u64>,
        lease: &crate::rpc::RpcStreamLease,
        per_thread: &mut HashMap<BbThreadId, mpsc::UnboundedSender<ThreadShellAction>>,
    ) {
        let Some(kind) = item.get("kind").and_then(|v| v.as_str()) else {
            return;
        };
        match kind {
            "snapshot" => {
                if let Some(snap) = item.get("snapshot") {
                    if let Some(seq) = snap.get("snapshotSequence").and_then(|v| v.as_u64()) {
                        *last_seq = Some(seq);
                        lease.update_resume_payload(json!({ "afterSequence": seq }));
                    }
                    let projects = parse_t3_projects_snapshot(snap);
                    self.replace_or_merge_project_names(&projects, true);
                    if let Some(threads) = snap.get("threads").and_then(|v| v.as_array()) {
                        for th in threads {
                            self.enqueue_shell_thread(per_thread, Some("active"), th.clone());
                        }
                    }
                    if let Some(archived) = snap.get("archivedThreads").and_then(|v| v.as_array()) {
                        for th in archived {
                            self.enqueue_shell_thread(per_thread, Some("archive"), th.clone());
                        }
                    }
                }
            }
            "project.updated" => {
                if let Some(seq) = item.get("sequence").and_then(|v| v.as_u64()) {
                    *last_seq = Some(last_seq.map_or(seq, |p| p.max(seq)));
                    lease.update_resume_payload(json!({ "afterSequence": last_seq.unwrap() }));
                }
                if let Some(proj) = item.get("project")
                    && let Some(id) = proj.get("id").and_then(|v| v.as_str())
                    && let Some(title) = proj
                        .get("title")
                        .or_else(|| proj.get("name"))
                        .and_then(|v| v.as_str())
                {
                    self.upsert_project_name(id, title);
                }
            }
            "project.removed" => {
                if let Some(seq) = item.get("sequence").and_then(|v| v.as_u64()) {
                    *last_seq = Some(last_seq.map_or(seq, |p| p.max(seq)));
                    lease.update_resume_payload(json!({ "afterSequence": last_seq.unwrap() }));
                }
                if let Some(id) = item.get("projectId").and_then(|v| v.as_str()) {
                    self.remove_project_name(id);
                }
            }
            "thread.updated" => {
                if let Some(seq) = item.get("sequence").and_then(|v| v.as_u64()) {
                    *last_seq = Some(last_seq.map_or(seq, |p| p.max(seq)));
                    lease.update_resume_payload(json!({ "afterSequence": last_seq.unwrap() }));
                }
                let loc = item.get("location").and_then(|v| v.as_str());
                if let Some(th) = item.get("thread") {
                    self.enqueue_shell_thread(per_thread, loc, th.clone());
                }
            }
            "thread.removed" => {
                if let Some(seq) = item.get("sequence").and_then(|v| v.as_u64()) {
                    *last_seq = Some(last_seq.map_or(seq, |p| p.max(seq)));
                    lease.update_resume_payload(json!({ "afterSequence": last_seq.unwrap() }));
                }
                if let Some(id_raw) = item.get("threadId").and_then(|v| v.as_str())
                    && let Ok(tid) = BbThreadId::parse(id_raw)
                    && is_t3_thread_id(&tid)
                {
                    self.enqueue_thread_action(per_thread, tid, ThreadShellAction::Removed);
                }
            }
            _ => {}
        }
    }

    fn enqueue_shell_thread(
        self: &Arc<Self>,
        per_thread: &mut HashMap<BbThreadId, mpsc::UnboundedSender<ThreadShellAction>>,
        location: Option<&str>,
        shell: Value,
    ) {
        let Some(id_raw) = shell.get("id").and_then(|v| v.as_str()) else {
            return;
        };
        let Ok(tid) = BbThreadId::parse(id_raw) else {
            return;
        };
        if !is_t3_thread_id(&tid) {
            return;
        }
        self.enqueue_thread_action(
            per_thread,
            tid,
            ThreadShellAction::Updated {
                location: location.map(str::to_owned),
                shell,
            },
        );
    }

    /// Serializes shell updates per thread ID so rapid status transitions never race out of order.
    fn enqueue_thread_action(
        self: &Arc<Self>,
        per_thread: &mut HashMap<BbThreadId, mpsc::UnboundedSender<ThreadShellAction>>,
        thread: BbThreadId,
        action: ThreadShellAction,
    ) {
        let pending = match per_thread.get(&thread) {
            Some(tx) => match tx.send(action) {
                Ok(()) => return,
                Err(err) => err.0,
            },
            None => action,
        };
        per_thread.remove(&thread);
        let tx = self.ensure_thread_queue(per_thread, thread);
        let _ = tx.send(pending);
    }

    fn ensure_thread_queue(
        self: &Arc<Self>,
        per_thread: &mut HashMap<BbThreadId, mpsc::UnboundedSender<ThreadShellAction>>,
        thread: BbThreadId,
    ) -> mpsc::UnboundedSender<ThreadShellAction> {
        per_thread.retain(|_, tx| !tx.is_closed());
        if let Some(tx) = per_thread.get(&thread) {
            return tx.clone();
        }
        let (tx, mut rx) = mpsc::unbounded_channel::<ThreadShellAction>();
        let bridge = self.clone();
        let tid = thread.clone();
        tokio::spawn(async move {
            while let Some(action) = rx.recv().await {
                let done = match action {
                    ThreadShellAction::Updated { location, shell } => {
                        bridge
                            .process_thread_shell_update(&tid, location.as_deref(), &shell)
                            .await
                    }
                    ThreadShellAction::Removed => {
                        bridge.process_thread_removed(&tid).await;
                        true
                    }
                };
                if done {
                    break;
                }
            }
        });
        per_thread.insert(thread, tx.clone());
        tx
    }

    async fn resolve_worker_identity(
        &self,
        thread: &BbThreadId,
    ) -> Option<(WaveSlug, RoleName, u32, bool)> {
        let (tx, rx) = oneshot::channel();
        let _ = self.state.send(StateMsg::LookupByThread {
            thread: thread.clone(),
            reply: tx,
        });
        if let Ok(Some((wave, role, attempt))) = rx.await {
            return Some((wave, role, attempt, false));
        }

        let (wave, role, attempt) = parse_t3_thread_id(thread)?;
        let backend = self.backend.get()?;
        if let Ok(WireReply::WaveView(snap)) = backend
            .call_as(
                self.cfg.system_principal.clone(),
                WaveView { wave: wave.clone() }.into_wire(),
            )
            .await
        {
            let mut matched_live = false;
            let mut matched_retiring = false;
            for (r, spec, phase) in &snap.workers {
                if r != &role {
                    continue;
                }
                match phase {
                    WorkerPhase::Live { thread: t, .. } if t == thread => {
                        matched_live = true;
                    }
                    WorkerPhase::Requested { attempt: a }
                        if *a == attempt
                            && rex_wave_waves::route_provider(
                                &spec.provider,
                                self.cfg.default_runner,
                            )
                            .is_ok_and(|routed| routed.target == rex_wave_api::RunnerKind::T3) =>
                    {
                        matched_live = true;
                    }
                    WorkerPhase::Retiring { thread: t, .. } if t == thread => {
                        matched_retiring = true;
                    }
                    _ => {}
                }
            }
            if !matched_live && !matched_retiring {
                return None;
            }
            if matched_live {
                let _ = self.state.send(StateMsg::BindThread {
                    wave: wave.clone(),
                    role: role.clone(),
                    attempt,
                    thread: thread.clone(),
                });
            }
            return Some((wave, role, attempt, !matched_live));
        }

        None
    }

    async fn try_wave_observe(
        &self,
        wave: WaveSlug,
        role: RoleName,
        observation: Observation,
    ) -> bool {
        if let Some(backend) = self.backend.get() {
            let res = backend
                .call_as(
                    self.cfg.system_principal.clone(),
                    WaveObserve {
                        wave: wave.clone(),
                        role: role.clone(),
                        observation: observation.clone(),
                    }
                    .into_wire(),
                )
                .await;
            if !matches!(res, Ok(WireReply::WaveObserve(_))) {
                return false;
            }
        }
        self.observe_wave_event(wave, WaveEvent::WorkerObserved { role, observation });
        true
    }

    async fn call_wave_observe(&self, wave: WaveSlug, role: RoleName, observation: Observation) {
        let _ = self.try_wave_observe(wave, role, observation).await;
    }

    async fn fetch_thread_projection(&self, thread: &BbThreadId) -> Result<Value, HostError> {
        let encoded = percent_encode_query(thread.as_str());
        let path = format!("/api/orchestration/threads/{encoded}");
        self.rpc.http_get_json(&path).await
    }

    async fn process_thread_removed(&self, thread: &BbThreadId) {
        self.unlink_worker_token(thread);
        if let Some((wave, role, _, _)) = self.resolve_worker_identity(thread).await {
            let _ = self.state.send(StateMsg::BumpShellEpoch {
                wave: wave.clone(),
                role: role.clone(),
                thread: thread.clone(),
            });
            self.call_wave_observe(wave, role, Observation::Archived)
                .await;
        }
    }

    async fn current_shell_epoch(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
    ) -> Option<u64> {
        let (tx, rx) = oneshot::channel();
        self.state
            .send(StateMsg::LookupByRole {
                wave: wave.clone(),
                role: role.clone(),
                reply: tx,
            })
            .ok()?;
        let rec = rx.await.ok()??;
        (rec.live && rec.thread.as_ref() == Some(thread)).then_some(rec.shell_epoch)
    }

    async fn observe_runtime_meta_if_present(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        actual_model: Option<ModelId>,
        actual_account: Option<String>,
    ) {
        if actual_model.is_none() && actual_account.is_none() {
            return;
        }
        self.call_wave_observe(
            wave.clone(),
            role.clone(),
            Observation::RuntimeMeta {
                actual_model,
                actual_account,
            },
        )
        .await;
    }

    async fn process_thread_shell_update(
        &self,
        thread: &BbThreadId,
        location: Option<&str>,
        shell: &Value,
    ) -> bool {
        let Some(obs) = derive_shell_observation(shell, location) else {
            return false;
        };
        let Some((wave, role, attempt, retiring)) = self.resolve_worker_identity(thread).await
        else {
            return false;
        };
        let _ = self.state.send(StateMsg::BumpShellEpoch {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
        });
        if retiring {
            if matches!(obs, ShellObservation::Archived) {
                self.unlink_worker_token(thread);
                self.call_wave_observe(wave, role, Observation::Archived)
                    .await;
                return true;
            }
            return false;
        }
        let (shell_model, shell_account) = extract_runtime_meta_from_value(shell);

        match obs {
            ShellObservation::Archived => {
                self.unlink_worker_token(thread);
                self.call_wave_observe(wave, role, Observation::Archived)
                    .await;
                true
            }
            ShellObservation::Blocked {
                request_id,
                request_kind,
            } => {
                let projection = self.fetch_thread_projection(thread).await.ok();
                let interaction = projection
                    .as_ref()
                    .and_then(|proj| {
                        parse_pending_interaction_from_projection(
                            proj,
                            &request_id,
                            request_kind.as_deref(),
                        )
                    })
                    .or_else(|| {
                        parse_pending_interaction_from_projection(
                            &Value::Null,
                            &request_id,
                            request_kind.as_deref(),
                        )
                    });
                let Some(interaction) = interaction else {
                    return false;
                };
                self.call_wave_observe(
                    wave.clone(),
                    role.clone(),
                    Observation::Spawned {
                        attempt,
                        thread: thread.clone(),
                    },
                )
                .await;
                let (proj_model, proj_account) = projection
                    .as_ref()
                    .map(extract_runtime_meta_from_value)
                    .unwrap_or((None, None));
                self.observe_runtime_meta_if_present(
                    &wave,
                    &role,
                    proj_model.or(shell_model),
                    proj_account.or(shell_account),
                )
                .await;
                if let Some(proj) = projection.as_ref() {
                    self.mirror_turns_from_projection(
                        &wave,
                        &role,
                        thread,
                        Some(shell),
                        proj,
                        TerminalTurnState::InProgress,
                    )
                    .await;
                }
                self.call_wave_observe(
                    wave,
                    role,
                    Observation::Activity {
                        activity: Activity::Blocked { interaction },
                    },
                )
                .await;
                false
            }
            ShellObservation::Working => {
                self.call_wave_observe(
                    wave.clone(),
                    role.clone(),
                    Observation::Spawned {
                        attempt,
                        thread: thread.clone(),
                    },
                )
                .await;
                self.observe_runtime_meta_if_present(&wave, &role, shell_model, shell_account)
                    .await;
                self.call_wave_observe(
                    wave,
                    role,
                    Observation::Activity {
                        activity: Activity::Working,
                    },
                )
                .await;
                false
            }
            ShellObservation::Refused(reason) => {
                self.call_wave_observe(
                    wave.clone(),
                    role.clone(),
                    Observation::Spawned {
                        attempt,
                        thread: thread.clone(),
                    },
                )
                .await;
                let fetched_proj = self.fetch_thread_projection(thread).await.ok();
                let (proj_model, proj_account) = fetched_proj
                    .as_ref()
                    .map(extract_runtime_meta_from_value)
                    .unwrap_or((None, None));
                self.observe_runtime_meta_if_present(
                    &wave,
                    &role,
                    proj_model.or(shell_model),
                    proj_account.or(shell_account),
                )
                .await;
                self.handle_shell_failure_projection(
                    &wave,
                    &role,
                    thread,
                    shell,
                    fetched_proj.as_ref(),
                )
                .await;
                self.call_wave_observe(
                    wave,
                    role,
                    Observation::Activity {
                        activity: Activity::Refused { reason },
                    },
                )
                .await;
                false
            }
            ShellObservation::Errored(error) => {
                self.call_wave_observe(
                    wave.clone(),
                    role.clone(),
                    Observation::Spawned {
                        attempt,
                        thread: thread.clone(),
                    },
                )
                .await;
                let fetched_proj = self.fetch_thread_projection(thread).await.ok();
                let (proj_model, proj_account) = fetched_proj
                    .as_ref()
                    .map(extract_runtime_meta_from_value)
                    .unwrap_or((None, None));
                self.observe_runtime_meta_if_present(
                    &wave,
                    &role,
                    proj_model.or(shell_model),
                    proj_account.or(shell_account),
                )
                .await;
                self.handle_shell_failure_projection(
                    &wave,
                    &role,
                    thread,
                    shell,
                    fetched_proj.as_ref(),
                )
                .await;
                self.call_wave_observe(
                    wave,
                    role,
                    Observation::Activity {
                        activity: Activity::Errored { error },
                    },
                )
                .await;
                false
            }
            ShellObservation::Idle { mirror_candidate } => {
                self.call_wave_observe(
                    wave.clone(),
                    role.clone(),
                    Observation::Spawned {
                        attempt,
                        thread: thread.clone(),
                    },
                )
                .await;
                let fetched_proj = self.fetch_thread_projection(thread).await.ok();
                let (proj_model, proj_account) = fetched_proj
                    .as_ref()
                    .map(extract_runtime_meta_from_value)
                    .unwrap_or((None, None));
                self.observe_runtime_meta_if_present(
                    &wave,
                    &role,
                    proj_model.or(shell_model),
                    proj_account.or(shell_account),
                )
                .await;
                if !mirror_candidate {
                    self.handle_shell_failure_projection(
                        &wave,
                        &role,
                        thread,
                        shell,
                        fetched_proj.as_ref(),
                    )
                    .await;
                    self.call_wave_observe(
                        wave,
                        role,
                        Observation::Activity {
                            activity: Activity::Idle,
                        },
                    )
                    .await;
                    return false;
                }
                if let Some(proj) = fetched_proj.as_ref() {
                    let unwrapped = unwrap_projection(proj);
                    match derive_activity_from_projection(unwrapped) {
                        Some(activity @ (Activity::Refused { .. } | Activity::Errored { .. })) => {
                            self.mirror_turns_from_projection(
                                &wave,
                                &role,
                                thread,
                                Some(shell),
                                unwrapped,
                                TerminalTurnState::Suppressed,
                            )
                            .await;
                            self.call_wave_observe(wave, role, Observation::Activity { activity })
                                .await;
                            return false;
                        }
                        _ => {
                            let terminal_state = classify_projection_terminal_state(unwrapped);
                            self.mirror_turns_from_projection(
                                &wave,
                                &role,
                                thread,
                                Some(shell),
                                unwrapped,
                                terminal_state,
                            )
                            .await;
                        }
                    }
                }
                self.call_wave_observe(
                    wave,
                    role,
                    Observation::Activity {
                        activity: Activity::Idle,
                    },
                )
                .await;
                false
            }
        }
    }

    async fn record_suppressed_keys_locked(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        keys: Vec<String>,
    ) -> bool {
        if keys.is_empty() {
            return true;
        }
        if !self
            .try_wave_observe(
                wave.clone(),
                role.clone(),
                Observation::TurnsSuppressed {
                    thread: thread.clone(),
                    keys: keys.clone(),
                },
            )
            .await
        {
            return false;
        }
        let _ = self.state.send(StateMsg::AdvanceMirrorWatermark {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
            seq: Seq::ORIGIN,
            turn_keys: keys,
            mark_suppressed: true,
            claimed_seqs: Vec::new(),
        });
        true
    }

    async fn promote_suppressed_anchors_locked(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        suppression_keys: Vec<String>,
        run_ids: &[String],
    ) -> bool {
        let suppressed_set = self.settled_turn_keys_locked(wave, role, thread).await;
        let all_run_tids = all_matching_run_ticket_ids(&suppressed_set, run_ids);
        let unresolved_run_ids = matching_run_ticket_ids_for_target(&suppressed_set, run_ids);
        let bound_keys = bind_ticket_anchor_keys(&all_run_tids, &suppression_keys);
        let missing: Vec<String> = suppression_keys
            .into_iter()
            .chain(resolve_ticket_ids(&unresolved_run_ids))
            .chain(bound_keys)
            .filter(|k| !k.is_empty() && !suppressed_set.contains(k))
            .fold(Vec::new(), |mut acc, k| {
                if !acc.contains(&k) {
                    acc.push(k);
                }
                acc
            });
        if missing.is_empty() {
            return true;
        }
        self.record_suppressed_keys_locked(wave, role, thread, missing)
            .await
    }

    async fn settled_turn_keys_locked(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
    ) -> BTreeSet<String> {
        let _ = self
            .ensure_journal_suppressions_hydrated_locked(wave, role, thread)
            .await;
        let (sk_tx, sk_rx) = oneshot::channel();
        let _ = self.state.send(StateMsg::SettledTurnKeys {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
            reply: sk_tx,
        });
        sk_rx.await.unwrap_or_default()
    }

    async fn handle_shell_failure_projection(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        shell: &Value,
        fetched_proj: Option<&Value>,
    ) {
        let (explicit_run_id, explicit_item_count) = shell_explicit_turn_target(shell);
        let Some(proj) = fetched_proj else {
            let _mirror_guard = self.mirror_lock.lock().await;
            let settled_keys = self.settled_turn_keys_locked(wave, role, thread).await;
            let mut keys =
                explicit_shell_suppression_keys(explicit_run_id.as_deref(), explicit_item_count);
            if explicit_item_count.is_none()
                && let Some(ticket) =
                    next_failure_ticket_key(&settled_keys, explicit_run_id.as_deref())
            {
                keys.push(ticket);
            }
            let _ = self
                .record_suppressed_keys_locked(wave, role, thread, keys)
                .await;
            return;
        };

        let unwrapped = unwrap_projection(proj);
        let proj_terminal_state = classify_projection_terminal_state(unwrapped);
        if let Some(plan) = plan_explicit_failure_turns(
            unwrapped,
            proj_terminal_state,
            explicit_run_id.as_deref(),
            explicit_item_count,
        ) {
            let _mirror_guard = self.mirror_lock.lock().await;
            let settled_keys = self.settled_turn_keys_locked(wave, role, thread).await;
            let active_tickets = active_unmatched_tickets(Some(unwrapped), &settled_keys);
            let mut anchors = projection_run_ticket_anchors(unwrapped, &settled_keys);
            match plan {
                ExplicitFailurePlan::TargetAheadOfProjection { mut explicit_keys } => {
                    if explicit_item_count.is_none()
                        && let Some(ticket) =
                            next_failure_ticket_key(&settled_keys, explicit_run_id.as_deref())
                    {
                        explicit_keys.push(ticket);
                    }
                    if !self
                        .record_suppressed_keys_locked(wave, role, thread, explicit_keys)
                        .await
                    {
                        return;
                    }
                    let closed_end = closed_turn_count_before_ahead_failure(
                        unwrapped,
                        explicit_run_id.as_deref(),
                        explicit_item_count,
                    );
                    if projection_slots_have_eligible_ticket(
                        unwrapped,
                        closed_end,
                        &active_tickets,
                        &anchors,
                    ) {
                        return;
                    }
                    let checks = inspect_closed_turns_before_ahead_failure(
                        unwrapped,
                        explicit_run_id.as_deref(),
                        explicit_item_count,
                    );
                    self.execute_inspected_turns_locked(wave, role, thread, checks)
                        .await;
                }
                ExplicitFailurePlan::Matched {
                    target_pos,
                    target_run_ids,
                    target_lookup_keys,
                    mut turns,
                } => {
                    let all_direct_ids =
                        all_matching_run_ticket_ids(&settled_keys, &target_run_ids);
                    let unresolved_direct_ids =
                        matching_run_ticket_ids_for_target(&settled_keys, &target_run_ids);
                    for &tid in &all_direct_ids {
                        if !anchors.contains(&(target_pos, tid)) {
                            anchors.push((target_pos, tid));
                        }
                    }

                    let (wm_tx, wm_rx) = oneshot::channel();
                    let _ = self.state.send(StateMsg::MirrorState {
                        wave: wave.clone(),
                        role: role.clone(),
                        thread: thread.clone(),
                        turn_keys: target_lookup_keys,
                        reply: wm_tx,
                    });
                    let (_, target_already_settled, _, _) =
                        wm_rx
                            .await
                            .unwrap_or((Seq::ORIGIN, false, false, BTreeSet::new()));

                    let mut target_ticket_ids = all_direct_ids.clone();
                    let mut target_resolved_ids = unresolved_direct_ids;
                    let mut pre_tickets: Vec<PendingTicket> = active_tickets
                        .into_iter()
                        .filter(|t| !all_direct_ids.contains(&t.id))
                        .collect();

                    if explicit_run_id.is_some()
                        && all_direct_ids.is_empty()
                        && !target_already_settled
                        && let Some(idx) = pre_tickets.iter().rposition(|t| {
                            t.run_id.is_none()
                                && ticket_causally_eligible_at_pos(
                                    t,
                                    target_pos,
                                    &target_run_ids,
                                    &anchors,
                                )
                        })
                    {
                        let refined = pre_tickets.remove(idx);
                        target_ticket_ids.push(refined.id);
                        target_resolved_ids.push(refined.id);
                        if !anchors.contains(&(target_pos, refined.id)) {
                            anchors.push((target_pos, refined.id));
                        }
                    }

                    if target_pos > 0 && !pre_tickets.is_empty() {
                        let Some(consumed) = self
                            .suppress_turns_in_range_locked(
                                wave,
                                role,
                                thread,
                                unwrapped,
                                0,
                                target_pos,
                                None,
                                None,
                                false,
                                true,
                                &pre_tickets,
                                0,
                                &anchors,
                            )
                            .await
                        else {
                            return;
                        };
                        pre_tickets.retain(|t| !consumed.contains(&t.id));
                    }

                    if all_direct_ids.is_empty()
                        && target_resolved_ids.is_empty()
                        && !target_already_settled
                        && let Some(matched_ticket) = pre_tickets.iter().find(|t| {
                            ticket_causally_eligible_at_pos(
                                t,
                                target_pos,
                                &target_run_ids,
                                &anchors,
                            )
                        })
                    {
                        target_ticket_ids.push(matched_ticket.id);
                        target_resolved_ids.push(matched_ticket.id);
                    }

                    if (!target_resolved_ids.is_empty() || !target_ticket_ids.is_empty())
                        && let Some(target_turn) = turns.iter_mut().find(|t| t.pos == target_pos)
                    {
                        let mut extra = resolve_ticket_ids(&target_resolved_ids);
                        extra.extend(bind_ticket_anchor_keys(
                            &target_ticket_ids,
                            &target_turn.suppression_keys,
                        ));
                        for k in extra {
                            if !k.is_empty() {
                                if !target_turn.suppression_keys.contains(&k) {
                                    target_turn.suppression_keys.push(k.clone());
                                }
                                if let TurnMirrorCheck::Suppressed { turn_keys, .. } =
                                    &mut target_turn.check
                                    && !turn_keys.contains(&k)
                                {
                                    turn_keys.push(k);
                                }
                            }
                        }
                    }

                    self.execute_inspected_turns_locked(wave, role, thread, turns)
                        .await;
                }
            }
            return;
        }

        if projection_advanced_past_failure(unwrapped) {
            self.suppress_lagged_preterminal_failure(wave, role, thread, shell, unwrapped)
                .await;
        } else {
            self.mirror_turns_from_projection(
                wave,
                role,
                thread,
                Some(shell),
                unwrapped,
                TerminalTurnState::Suppressed,
            )
            .await;
        }
    }

    async fn ensure_journal_suppressions_hydrated_locked(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
    ) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self.state.send(StateMsg::HydrationStatus {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
            reply: tx,
        });
        let already_hydrated = rx.await.unwrap_or(false);
        if already_hydrated {
            return true;
        }
        let Some(backend) = self.backend.get() else {
            return false;
        };
        let sys = self.cfg.system_principal.clone();
        let mut cursor = Seq::ORIGIN;
        let mut active_thread: Option<BbThreadId> = None;
        let mut keys: Vec<String> = Vec::new();
        let mut reached_end = false;
        loop {
            let Ok(WireReply::Read(page)) = backend
                .call_as(
                    sys.clone(),
                    Read::stream(StreamId::Wave { wave: wave.clone() }, cursor, Some(500))
                        .into_wire(),
                )
                .await
            else {
                break;
            };
            let Some(last) = page.entries.last() else {
                reached_end = true;
                break;
            };
            if last.seq <= cursor {
                break;
            }
            cursor = last.seq;
            let count = page.entries.len();
            for env in &page.entries {
                if let Event::Wave(ev) = &env.event {
                    match ev {
                        WaveEvent::WorkerRequested { role: r, .. } if r == role => {
                            active_thread = None;
                            keys.clear();
                        }
                        WaveEvent::WorkerObserved {
                            role: r,
                            observation: Observation::Spawned { thread: th, .. },
                        } if r == role => {
                            if active_thread.as_ref() != Some(th) {
                                active_thread = Some(th.clone());
                                keys.clear();
                            }
                        }
                        WaveEvent::WorkerObserved {
                            role: r,
                            observation:
                                Observation::TurnsSuppressed {
                                    thread: th,
                                    keys: ks,
                                },
                        } if r == role => {
                            if active_thread.as_ref() != Some(th) {
                                active_thread = Some(th.clone());
                                keys.clear();
                            }
                            for k in ks {
                                if !k.is_empty() {
                                    keys.push(k.clone());
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            if count < 500 {
                reached_end = true;
                break;
            }
        }
        if reached_end {
            let matching = if active_thread.as_ref() == Some(thread) {
                keys
            } else {
                Vec::new()
            };
            let _ = self.state.send(StateMsg::ApplyHydratedSuppressions {
                wave: wave.clone(),
                role: role.clone(),
                thread: thread.clone(),
                turn_keys: matching,
            });
            return true;
        }
        false
    }

    #[allow(clippy::too_many_arguments)]
    async fn suppress_turns_in_range_locked(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        unwrapped_projection: &Value,
        start_pos: usize,
        end_pos: usize,
        explicit_run_id: Option<&str>,
        explicit_item_count: Option<usize>,
        check_terminal_suppression: bool,
        filter_clean_runs: bool,
        tickets: &[PendingTicket],
        extra_unattributed_budget: usize,
        anchors: &[(usize, usize)],
    ) -> Option<Vec<usize>> {
        if tickets.is_empty() && extra_unattributed_budget == 0 {
            return Some(Vec::new());
        }
        let is_unattributed = explicit_run_id.is_none() && explicit_item_count.is_none();
        let candidates = collect_turn_candidates_in_range(
            unwrapped_projection,
            start_pos,
            end_pos,
            explicit_run_id,
            explicit_item_count,
        );
        if candidates.is_empty() {
            return Some(Vec::new());
        }
        let Some(backend) = self.backend.get() else {
            return Some(Vec::new());
        };
        let sys = self.cfg.system_principal.clone();
        let Ok(WireReply::WaveView(snap)) = backend
            .call_as(sys.clone(), WaveView { wave: wave.clone() }.into_wire())
            .await
        else {
            return None;
        };
        let room = rex_wave_waves::wave_room(wave, &snap.spec.project);
        let worker_principal = Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        };
        let stream = StreamId::Room { room };

        let mut unsettled = Vec::new();
        for cand in candidates {
            if cand.suppression_keys.is_empty() {
                continue;
            }
            let (wm_tx, wm_rx) = oneshot::channel();
            let _ = self.state.send(StateMsg::MirrorState {
                wave: wave.clone(),
                role: role.clone(),
                thread: thread.clone(),
                turn_keys: cand.lookup_keys.clone(),
                reply: wm_tx,
            });
            let (watermark, already_settled, already_suppressed, claimed_direct_posts) = wm_rx
                .await
                .unwrap_or((Seq::ORIGIN, false, false, BTreeSet::new()));
            if already_suppressed {
                if !self
                    .promote_suppressed_anchors_locked(
                        wave,
                        role,
                        thread,
                        cand.suppression_keys,
                        &cand.run_ids,
                    )
                    .await
                {
                    return None;
                }
            } else if !already_settled {
                unsettled.push((cand, watermark, claimed_direct_posts));
            }
        }

        if is_unattributed && filter_clean_runs && !unsettled.is_empty() {
            let unsettled_terminal_suppressed = if check_terminal_suppression {
                let proj_term_state = classify_projection_terminal_state(unwrapped_projection);
                if let Some(term_lookup) =
                    terminal_suppressed_lookup_keys(unwrapped_projection, proj_term_state)
                {
                    let (wm_tx, wm_rx) = oneshot::channel();
                    let _ = self.state.send(StateMsg::MirrorState {
                        wave: wave.clone(),
                        role: role.clone(),
                        thread: thread.clone(),
                        turn_keys: term_lookup,
                        reply: wm_tx,
                    });
                    let (_, term_settled, _, _) =
                        wm_rx
                            .await
                            .unwrap_or((Seq::ORIGIN, false, false, BTreeSet::new()));
                    !term_settled
                } else {
                    false
                }
            } else {
                false
            };

            if unsettled_terminal_suppressed
                || unsettled.iter().any(|(c, _, _)| c.inherently_suppressed)
            {
                unsettled.retain(|(c, _, _)| c.inherently_suppressed);
            } else if !check_terminal_suppression {
                unsettled.retain(|(c, _, _)| !c.completed_cleanly);
            } else if unsettled.iter().all(|(c, _, _)| c.completed_cleanly)
                && any_turn_inherently_suppressed(unwrapped_projection)
            {
                unsettled.clear();
            } else if unsettled.iter().any(|(c, _, _)| !c.completed_cleanly)
                && unsettled.iter().any(|(c, _, _)| c.completed_cleanly)
            {
                unsettled.retain(|(c, _, _)| !c.completed_cleanly);
            }
        }

        let mut remaining_tickets: Vec<PendingTicket> = tickets.to_vec();
        let mut extra_remaining = extra_unattributed_budget;
        let mut consumed_ticket_ids: Vec<usize> = Vec::new();
        let mut newly_suppressed: Vec<String> = Vec::new();
        let mut pending_state_updates: Vec<(Seq, Vec<String>, bool, Vec<Seq>)> = Vec::new();

        for (cand, watermark, claimed_direct_posts) in unsettled {
            let compatible_ticket_pos = remaining_tickets
                .iter()
                .position(|t| ticket_causally_eligible_at_pos(t, cand.pos, &cand.run_ids, anchors));
            if compatible_ticket_pos.is_none() && extra_remaining == 0 {
                continue;
            }

            let mirror_match_spec = match &cand.check {
                TurnMirrorCheck::Unposted(c) => {
                    let mut exact_keys = Vec::new();
                    let mut legacy_keys = Vec::new();
                    for a in std::iter::once(&c.anchor).chain(c.alt_anchors.iter()) {
                        if let Ok(k) = mirror_idem_key(wave, role, thread, a) {
                            exact_keys.push(k);
                        }
                        if let Some(lk) =
                            rex_wave_waves::legacy_truncated_mirror_idem_key(wave, role, thread, a)
                        {
                            legacy_keys.push(lk);
                        }
                    }
                    for lba in &c.legacy_body_anchors {
                        if let Ok(k) = mirror_idem_key(wave, role, thread, lba)
                            && !legacy_keys.contains(&k)
                        {
                            legacy_keys.push(k);
                        }
                        if let Some(lk) = rex_wave_waves::legacy_truncated_mirror_idem_key(
                            wave, role, thread, lba,
                        ) && !legacy_keys.contains(&lk)
                        {
                            legacy_keys.push(lk);
                        }
                    }
                    let trimmed = truncate_with_notice(&c.text, 64 * 1024);
                    Some((exact_keys, legacy_keys, trimmed))
                }
                _ => None,
            };

            let can_claim_direct = cand.run_end_at > 0 || cand.next_turn_start_at.is_some();
            let mut cursor = watermark;
            let mut already_mirrored_in_room = false;
            let mut claimed_seqs = Vec::new();

            loop {
                let Ok(WireReply::Read(page)) = backend
                    .call_as(
                        sys.clone(),
                        Read::stream(stream.clone(), cursor, Some(500)).into_wire(),
                    )
                    .await
                else {
                    break;
                };
                let Some(last) = page.entries.last() else {
                    break;
                };
                if last.seq <= cursor {
                    break;
                }
                cursor = last.seq;
                for env in &page.entries {
                    if let Event::Room(RoomEvent::Posted {
                        body, idem, origin, ..
                    }) = &env.event
                    {
                        if let Some((ref exact_keys, ref legacy_keys, ref trimmed)) =
                            mirror_match_spec
                        {
                            if idem.as_ref().is_some_and(|k| exact_keys.contains(k)) {
                                claimed_seqs.push(env.seq);
                                already_mirrored_in_room = true;
                                break;
                            }
                            if env.by == worker_principal
                                && idem.as_ref().is_some_and(|k| legacy_keys.contains(k))
                                && body.as_str() == trimmed.as_str()
                                && !claimed_direct_posts.contains(&env.seq)
                            {
                                claimed_seqs.push(env.seq);
                                already_mirrored_in_room = true;
                                break;
                            }
                        }
                        let is_mirrored_row = *origin
                            == rex_wave_api::room::PostOrigin::MirroredTurn
                            || idem
                                .as_ref()
                                .is_some_and(rex_wave_waves::is_legacy_mirror_idem_key);
                        if can_claim_direct
                            && !is_mirrored_row
                            && env.by == worker_principal
                            && !claimed_direct_posts.contains(&env.seq)
                            && (cand.run_start_at == 0 || env.at.0 >= cand.run_start_at)
                        {
                            let post_after = (cand.run_end_at > 0 && env.at.0 > cand.run_end_at)
                                || cand.next_turn_start_at.is_some_and(|ns| env.at.0 >= ns);
                            if !post_after && (cand.tight_end_bound || claimed_seqs.is_empty()) {
                                claimed_seqs.push(env.seq);
                            }
                        }
                    }
                }
                if already_mirrored_in_room || page.entries.len() < 500 {
                    break;
                }
            }

            if !already_mirrored_in_room {
                let mut bound_extra = Vec::new();
                if let Some(pos) = compatible_ticket_pos {
                    let consumed = remaining_tickets.remove(pos);
                    consumed_ticket_ids.push(consumed.id);
                    bound_extra = bind_ticket_anchor_keys(&[consumed.id], &cand.suppression_keys);
                } else {
                    extra_remaining = extra_remaining.saturating_sub(1);
                }
                let mut turn_keys = cand.suppression_keys;
                for bk in bound_extra {
                    if !turn_keys.contains(&bk) {
                        turn_keys.push(bk);
                    }
                }
                for k in &turn_keys {
                    if !newly_suppressed.contains(k) {
                        newly_suppressed.push(k.clone());
                    }
                }
                pending_state_updates.push((watermark, turn_keys, true, claimed_seqs));
            } else {
                pending_state_updates.push((watermark, cand.suppression_keys, false, claimed_seqs));
            }
        }

        for rk in resolve_ticket_ids(&consumed_ticket_ids) {
            if !newly_suppressed.contains(&rk) {
                newly_suppressed.push(rk);
            }
        }
        if !self
            .record_suppressed_keys_locked(wave, role, thread, newly_suppressed)
            .await
        {
            return None;
        }
        for (seq, turn_keys, mark_suppressed, claimed_seqs) in pending_state_updates {
            let _ = self.state.send(StateMsg::AdvanceMirrorWatermark {
                wave: wave.clone(),
                role: role.clone(),
                thread: thread.clone(),
                seq,
                turn_keys,
                mark_suppressed,
                claimed_seqs,
            });
        }
        Some(consumed_ticket_ids)
    }

    async fn suppress_lagged_preterminal_failure(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        shell: &Value,
        unwrapped_projection: &Value,
    ) {
        let _mirror_guard = self.mirror_lock.lock().await;
        let settled_keys = self.settled_turn_keys_locked(wave, role, thread).await;
        let slot_count = projection_turn_slot_count(unwrapped_projection);
        if slot_count < 2 {
            return;
        }
        let active_tickets = active_unmatched_tickets(Some(unwrapped_projection), &settled_keys);
        let anchors = projection_run_ticket_anchors(unwrapped_projection, &settled_keys);
        let (explicit_run_id, explicit_item_count) = shell_explicit_turn_target(shell);
        let _ = self
            .suppress_turns_in_range_locked(
                wave,
                role,
                thread,
                unwrapped_projection,
                0,
                slot_count - 1,
                explicit_run_id.as_deref(),
                explicit_item_count,
                true,
                true,
                &active_tickets,
                1,
                &anchors,
            )
            .await;
    }

    async fn mirror_turns_from_projection(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        shell_hint: Option<&Value>,
        projection: &Value,
        terminal_state: TerminalTurnState,
    ) {
        let _mirror_guard = self.mirror_lock.lock().await;
        let settled_keys = self.settled_turn_keys_locked(wave, role, thread).await;
        let unwrapped = unwrap_projection(projection);
        let active_tickets = active_unmatched_tickets(Some(unwrapped), &settled_keys);
        let anchors = projection_run_ticket_anchors(unwrapped, &settled_keys);
        let (shell_run_id, shell_item_count) = shell_hint
            .map(shell_explicit_turn_target)
            .unwrap_or((None, None));
        let (ahead_hold, ahead_run_id, ahead_item_count) = projection_has_ahead_suppression(
            unwrapped,
            &settled_keys,
            shell_run_id.as_deref(),
            shell_item_count,
        );
        if ahead_hold {
            let closed_end = closed_turn_count_before_ahead_failure(
                unwrapped,
                ahead_run_id.as_deref(),
                ahead_item_count,
            );
            if projection_slots_have_eligible_ticket(
                unwrapped,
                closed_end,
                &active_tickets,
                &anchors,
            ) {
                return;
            }
            let checks = inspect_closed_turns_before_ahead_failure(
                unwrapped,
                ahead_run_id.as_deref(),
                ahead_item_count,
            );
            self.execute_inspected_turns_locked(wave, role, thread, checks)
                .await;
            return;
        }
        let slot_count = projection_turn_slot_count(unwrapped);
        if !active_tickets.is_empty() {
            let consumed_pre = if slot_count >= 2 {
                let Some(consumed) = self
                    .suppress_turns_in_range_locked(
                        wave,
                        role,
                        thread,
                        unwrapped,
                        0,
                        slot_count - 1,
                        None,
                        None,
                        true,
                        true,
                        &active_tickets,
                        0,
                        &anchors,
                    )
                    .await
                else {
                    return;
                };
                consumed
            } else {
                Vec::new()
            };
            let mut remaining_tickets: Vec<PendingTicket> = active_tickets
                .into_iter()
                .filter(|t| !consumed_pre.contains(&t.id))
                .collect();
            if slot_count >= 1 && terminal_state != TerminalTurnState::InProgress {
                let eligible_term = projection_slot_eligible_tickets(
                    unwrapped,
                    slot_count - 1,
                    &remaining_tickets,
                    &anchors,
                );
                if !eligible_term.is_empty() {
                    let Some(consumed_term) = self
                        .suppress_turns_in_range_locked(
                            wave,
                            role,
                            thread,
                            unwrapped,
                            slot_count - 1,
                            slot_count,
                            None,
                            None,
                            false,
                            false,
                            &eligible_term[..1],
                            0,
                            &anchors,
                        )
                        .await
                    else {
                        return;
                    };
                    remaining_tickets.retain(|t| !consumed_term.contains(&t.id));
                }
            }
            if projection_slots_have_eligible_ticket(
                unwrapped,
                slot_count,
                &remaining_tickets,
                &anchors,
            ) {
                return;
            }
        }
        let checks = inspect_turns_in_order(unwrapped, terminal_state);
        self.execute_inspected_turns_locked(wave, role, thread, checks)
            .await;
    }

    async fn execute_inspected_turns_locked(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        checks: Vec<InspectedTurn>,
    ) {
        if checks.is_empty() {
            return;
        }
        let Some(backend) = self.backend.get() else {
            return;
        };
        let sys = self.cfg.system_principal.clone();
        let Ok(WireReply::WaveView(snap)) = backend
            .call_as(sys.clone(), WaveView { wave: wave.clone() }.into_wire())
            .await
        else {
            return;
        };
        let room = rex_wave_waves::wave_room(wave, &snap.spec.project);
        let worker_principal = Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        };
        let stream = StreamId::Room { room: room.clone() };

        for inspected in checks {
            let check = inspected.check;
            let lookup_keys = inspected.lookup_keys;
            let promotion_keys = inspected.suppression_keys;
            let inspected_run_ids = inspected.run_ids;
            let next_run_start_at = inspected.next_turn_start_at;
            let has_later_turn = inspected.has_later_turn;
            let is_suppressed = matches!(check, TurnMirrorCheck::Suppressed { .. });
            let allow_watermark_advance =
                !has_later_turn && next_run_start_at.is_none() && !is_suppressed;
            let turn_keys: Vec<String> = match &check {
                TurnMirrorCheck::Ignore => continue,
                TurnMirrorCheck::Suppressed { turn_keys, .. } => turn_keys.clone(),
                TurnMirrorCheck::PostedViaTool { run_key, .. } => run_key.iter().cloned().collect(),
                TurnMirrorCheck::Unposted(c) => {
                    let mut keys = vec![c.anchor.clone()];
                    keys.extend(c.alt_anchors.iter().cloned());
                    if let Some(rk) = &c.run_key {
                        keys.push(rk.clone());
                    }
                    keys
                }
            };

            let (wm_tx, wm_rx) = oneshot::channel();
            let _ = self.state.send(StateMsg::MirrorState {
                wave: wave.clone(),
                role: role.clone(),
                thread: thread.clone(),
                turn_keys: lookup_keys,
                reply: wm_tx,
            });
            let (watermark, already_settled, already_suppressed, claimed_direct_posts) = wm_rx
                .await
                .unwrap_or((Seq::ORIGIN, false, false, BTreeSet::new()));
            if already_settled {
                if (already_suppressed || is_suppressed)
                    && !self
                        .promote_suppressed_anchors_locked(
                            wave,
                            role,
                            thread,
                            if is_suppressed {
                                turn_keys
                            } else {
                                promotion_keys
                            },
                            &inspected_run_ids,
                        )
                        .await
                {
                    return;
                }
                continue;
            }

            let mut cursor = watermark;
            let mut room_head = watermark;
            let mut safe_watermark = watermark;
            let mut watermark_pinned = !allow_watermark_advance;

            let settle_turn = |target_seq: Seq,
                               keys: Vec<String>,
                               mark_suppressed: bool,
                               claimed_seqs: Vec<Seq>| {
                let next_seq = if allow_watermark_advance {
                    target_seq
                } else {
                    watermark
                };
                let _ = self.state.send(StateMsg::AdvanceMirrorWatermark {
                    wave: wave.clone(),
                    role: role.clone(),
                    thread: thread.clone(),
                    seq: next_seq,
                    turn_keys: keys,
                    mark_suppressed,
                    claimed_seqs,
                });
            };

            let completed = match check {
                TurnMirrorCheck::Ignore => continue,
                TurnMirrorCheck::Suppressed {
                    run_start_at,
                    run_end_at,
                    tight_end_bound,
                    ..
                } => {
                    let can_claim_direct =
                        !has_later_turn || run_end_at > 0 || next_run_start_at.is_some();
                    let mut suppressed_claimed = Vec::new();
                    if can_claim_direct {
                        loop {
                            let Ok(WireReply::Read(page)) = backend
                                .call_as(
                                    sys.clone(),
                                    Read::stream(stream.clone(), cursor, Some(500)).into_wire(),
                                )
                                .await
                            else {
                                break;
                            };
                            let Some(last) = page.entries.last() else {
                                break;
                            };
                            if last.seq <= cursor {
                                break;
                            }
                            cursor = last.seq;
                            for env in &page.entries {
                                if env.by == worker_principal
                                    && let Event::Room(RoomEvent::Posted { idem, origin, .. }) =
                                        &env.event
                                {
                                    let is_mirrored_row = *origin
                                        == rex_wave_api::room::PostOrigin::MirroredTurn
                                        || idem
                                            .as_ref()
                                            .is_some_and(rex_wave_waves::is_legacy_mirror_idem_key);
                                    if !is_mirrored_row
                                        && !claimed_direct_posts.contains(&env.seq)
                                        && (run_start_at == 0 || env.at.0 >= run_start_at)
                                    {
                                        let post_after = (run_end_at > 0 && env.at.0 > run_end_at)
                                            || next_run_start_at.is_some_and(|ns| env.at.0 >= ns);
                                        if !post_after
                                            && (!has_later_turn
                                                || tight_end_bound
                                                || suppressed_claimed.is_empty())
                                        {
                                            suppressed_claimed.push(env.seq);
                                        }
                                    }
                                }
                            }
                            if page.entries.len() < 500 {
                                break;
                            }
                        }
                    }
                    if !turn_keys.is_empty()
                        && !self
                            .try_wave_observe(
                                wave.clone(),
                                role.clone(),
                                Observation::TurnsSuppressed {
                                    thread: thread.clone(),
                                    keys: turn_keys.clone(),
                                },
                            )
                            .await
                    {
                        return;
                    }
                    settle_turn(watermark, turn_keys, true, suppressed_claimed);
                    continue;
                }
                TurnMirrorCheck::PostedViaTool {
                    run_end_at,
                    tight_end_bound,
                    ..
                } => {
                    let mut tool_reached_end = false;
                    let mut tool_claimed = Vec::new();
                    loop {
                        let Ok(WireReply::Read(page)) = backend
                            .call_as(
                                sys.clone(),
                                Read::stream(stream.clone(), cursor, Some(500)).into_wire(),
                            )
                            .await
                        else {
                            return;
                        };
                        let Some(last) = page.entries.last() else {
                            tool_reached_end = true;
                            break;
                        };
                        if last.seq <= cursor {
                            break;
                        }
                        cursor = last.seq;
                        for env in &page.entries {
                            if env.by == worker_principal
                                && let Event::Room(RoomEvent::Posted { idem, origin, .. }) =
                                    &env.event
                            {
                                let is_mirrored_row = *origin
                                    == rex_wave_api::room::PostOrigin::MirroredTurn
                                    || idem
                                        .as_ref()
                                        .is_some_and(rex_wave_waves::is_legacy_mirror_idem_key);
                                if !is_mirrored_row {
                                    let post_after = (run_end_at > 0 && env.at.0 > run_end_at)
                                        || next_run_start_at.is_some_and(|ns| env.at.0 >= ns);
                                    if post_after {
                                        watermark_pinned = true;
                                    } else if !claimed_direct_posts.contains(&env.seq)
                                        && (tight_end_bound || tool_claimed.is_empty())
                                    {
                                        tool_claimed.push(env.seq);
                                    }
                                }
                            }
                            if !watermark_pinned {
                                safe_watermark = env.seq;
                            }
                        }
                        if page.entries.len() < 500 {
                            tool_reached_end = true;
                            break;
                        }
                    }
                    if tool_reached_end {
                        settle_turn(safe_watermark, turn_keys, false, tool_claimed);
                    }
                    continue;
                }
                TurnMirrorCheck::Unposted(c) => c,
            };

            let Ok(idem_key) = mirror_idem_key(wave, role, thread, &completed.anchor) else {
                settle_turn(watermark, turn_keys, false, Vec::new());
                continue;
            };

            let mut exact_idem_keys = vec![idem_key.clone()];
            let mut legacy_trunc_keys = Vec::new();
            if let Some(lk) = rex_wave_waves::legacy_truncated_mirror_idem_key(
                wave,
                role,
                thread,
                &completed.anchor,
            ) {
                legacy_trunc_keys.push(lk);
            }
            for alt in &completed.alt_anchors {
                if let Ok(ak) = mirror_idem_key(wave, role, thread, alt)
                    && !exact_idem_keys.contains(&ak)
                {
                    exact_idem_keys.push(ak);
                }
                if let Some(lk) =
                    rex_wave_waves::legacy_truncated_mirror_idem_key(wave, role, thread, alt)
                    && !legacy_trunc_keys.contains(&lk)
                {
                    legacy_trunc_keys.push(lk);
                }
            }
            for lba in &completed.legacy_body_anchors {
                if let Ok(bk) = mirror_idem_key(wave, role, thread, lba)
                    && !legacy_trunc_keys.contains(&bk)
                {
                    legacy_trunc_keys.push(bk);
                }
                if let Some(lk) =
                    rex_wave_waves::legacy_truncated_mirror_idem_key(wave, role, thread, lba)
                    && !legacy_trunc_keys.contains(&lk)
                {
                    legacy_trunc_keys.push(lk);
                }
            }

            let trimmed = truncate_with_notice(&completed.text, 64 * 1024);
            let mut already_posted = false;
            let mut reached_end = false;
            let mut claimed_seqs = Vec::new();
            let bounded_window = completed.tight_end_bound;
            loop {
                let Ok(WireReply::Read(page)) = backend
                    .call_as(
                        sys.clone(),
                        Read::stream(stream.clone(), cursor, Some(500)).into_wire(),
                    )
                    .await
                else {
                    return;
                };
                let Some(last) = page.entries.last() else {
                    reached_end = true;
                    break;
                };
                if last.seq <= cursor {
                    break;
                };
                cursor = last.seq;
                room_head = last.seq;
                let count = page.entries.len();
                for env in &page.entries {
                    if let Event::Room(RoomEvent::Posted {
                        body, idem, origin, ..
                    }) = &env.event
                    {
                        if idem.as_ref().is_some_and(|k| exact_idem_keys.contains(k)) {
                            if !watermark_pinned {
                                safe_watermark = env.seq;
                            }
                            claimed_seqs.push(env.seq);
                            already_posted = true;
                            break;
                        }
                        let is_mirrored_row = *origin
                            == rex_wave_api::room::PostOrigin::MirroredTurn
                            || idem
                                .as_ref()
                                .is_some_and(rex_wave_waves::is_legacy_mirror_idem_key);
                        if is_mirrored_row {
                            if env.by == worker_principal
                                && idem.as_ref().is_some_and(|k| legacy_trunc_keys.contains(k))
                                && body.as_str() == trimmed
                                && !claimed_direct_posts.contains(&env.seq)
                            {
                                if !watermark_pinned {
                                    safe_watermark = env.seq;
                                }
                                claimed_seqs.push(env.seq);
                                already_posted = true;
                                break;
                            }
                        } else if env.by == worker_principal
                            && !claimed_direct_posts.contains(&env.seq)
                            && (completed.run_start_at == 0 || env.at.0 >= completed.run_start_at)
                        {
                            let post_after_run = (completed.run_end_at > 0
                                && env.at.0 > completed.run_end_at)
                                || next_run_start_at.is_some_and(|ns| env.at.0 >= ns);
                            if post_after_run {
                                watermark_pinned = true;
                            } else {
                                if !watermark_pinned {
                                    safe_watermark = env.seq;
                                }
                                claimed_seqs.push(env.seq);
                                already_posted = true;
                                if !bounded_window {
                                    break;
                                }
                            }
                        }
                    }
                    if !watermark_pinned {
                        safe_watermark = env.seq;
                    }
                }
                if already_posted && !bounded_window {
                    break;
                }
                if count < 500 {
                    reached_end = true;
                    break;
                }
            }

            if !already_posted && !reached_end {
                return;
            }

            if already_posted {
                settle_turn(safe_watermark, turn_keys, false, claimed_seqs);
                continue;
            }

            let Ok(body) = Body::parse(trimmed) else {
                settle_turn(safe_watermark, turn_keys, false, Vec::new());
                continue;
            };
            let post_cmd =
                RoomPost::mirrored_turn(RoomRef::Id { id: room.clone() }, body, idem_key);
            let first_try = backend
                .call_as(worker_principal.clone(), post_cmd.clone().into_wire())
                .await;
            let post_res = match first_try {
                Err(ApiError::Denied { .. }) => {
                    let _ = backend
                        .call_as(
                            sys.clone(),
                            RoomEnsure {
                                room: RoomRef::Id { id: room.clone() },
                                topic: None,
                                origin: Some(RoomOrigin::Wave { wave: wave.clone() }),
                            }
                            .into_wire(),
                        )
                        .await;
                    let _ = backend
                        .call_as(
                            sys.clone(),
                            RoomJoin {
                                room: RoomRef::Id { id: room.clone() },
                                member: Some(worker_principal.clone()),
                                handle: Handle::parse(role.as_str()).ok(),
                            }
                            .into_wire(),
                        )
                        .await;
                    backend
                        .call_as(worker_principal.clone(), post_cmd.into_wire())
                        .await
                }
                other => other,
            };
            if let Ok(WireReply::RoomPost(posted)) = post_res {
                let posted_head =
                    if !watermark_pinned && posted.seq.0 == room_head.0.saturating_add(1) {
                        posted.seq
                    } else {
                        safe_watermark
                    };
                settle_turn(posted_head, turn_keys, false, vec![posted.seq]);
            } else {
                return;
            }
        }
    }

    async fn reconcile_live_workers(&self) {
        let (tx, rx) = oneshot::channel();
        if self
            .state
            .send(StateMsg::ListLiveWorkers { reply: tx })
            .is_err()
        {
            return;
        }
        let Ok(workers) = rx.await else { return };
        for (wave, role, thread, tracked_activity, shell_epoch) in workers {
            let raw_proj = match self.fetch_thread_projection(&thread).await {
                Ok(v) => v,
                Err(HostError::Rejected(msg)) => {
                    if let Some(obs) = classify_t3_missing_or_archived(&msg) {
                        let already_terminal_failure = matches!(
                            tracked_activity,
                            Activity::Errored { .. } | Activity::Refused { .. }
                        );
                        if !already_terminal_failure || matches!(obs, Observation::Archived) {
                            if matches!(obs, Observation::Archived) {
                                self.unlink_worker_token(&thread);
                            }
                            if matches!(obs, Observation::Archived)
                                || self.current_shell_epoch(&wave, &role, &thread).await
                                    == Some(shell_epoch)
                            {
                                self.call_wave_observe(wave, role, obs).await;
                            }
                        }
                    }
                    continue;
                }
                Err(HostError::Unreachable(_)) => continue,
            };
            let proj = unwrap_projection(&raw_proj);
            let thread_obj = proj.get("thread");
            if thread_obj
                .and_then(|t| t.get("archivedAt"))
                .is_some_and(|v| !v.is_null() && v.as_str().is_none_or(|s| !s.is_empty()))
            {
                self.unlink_worker_token(&thread);
                self.call_wave_observe(wave, role, Observation::Archived)
                    .await;
                continue;
            }

            if let Some(pending) = active_pending_interaction(proj) {
                let (proj_model, proj_account) = extract_runtime_meta_from_value(proj);
                self.observe_runtime_meta_if_present(&wave, &role, proj_model, proj_account)
                    .await;
                self.mirror_turns_from_projection(
                    &wave,
                    &role,
                    &thread,
                    None,
                    proj,
                    TerminalTurnState::InProgress,
                )
                .await;
                if self.current_shell_epoch(&wave, &role, &thread).await == Some(shell_epoch) {
                    self.call_wave_observe(
                        wave,
                        role,
                        Observation::Activity {
                            activity: Activity::Blocked {
                                interaction: pending,
                            },
                        },
                    )
                    .await;
                }
                continue;
            }

            let (proj_model, proj_account) = extract_runtime_meta_from_value(proj);
            self.observe_runtime_meta_if_present(&wave, &role, proj_model, proj_account)
                .await;
            let derived = derive_activity_from_projection(proj);
            if matches!(
                tracked_activity,
                Activity::Blocked { .. } | Activity::Parked { .. }
            ) && matches!(derived, Some(Activity::Working))
            {
                continue;
            }
            if let Some(activity) = derived {
                let terminal_state = classify_projection_terminal_state(proj);
                self.mirror_turns_from_projection(
                    &wave,
                    &role,
                    &thread,
                    None,
                    proj,
                    terminal_state,
                )
                .await;
                if self.current_shell_epoch(&wave, &role, &thread).await == Some(shell_epoch) {
                    self.call_wave_observe(wave, role, Observation::Activity { activity })
                        .await;
                }
            }
        }
    }

    fn unlink_worker_token(&self, thread: &BbThreadId) {
        if let Some(state_dir) = &self.cfg.state_dir {
            let _ = remove_worker_token_file(state_dir, thread.as_str());
        }
    }

    async fn resolve_model_for_spawn(
        &self,
        provider_instance: &str,
        explicit: Option<&ModelId>,
    ) -> Result<String, HostError> {
        if let Some(m) = explicit {
            return Ok(m.as_str().to_owned());
        }
        if let Some(def) = self.cfg.default_models.get(provider_instance) {
            return Ok(def.clone());
        }
        let config = self.rpc.call("server.getConfig", json!({})).await?;
        default_model_from_server_config(&config, provider_instance).ok_or_else(|| {
            HostError::Rejected(format!(
                "no model specified for T3 provider '{provider_instance}' and server.getConfig listed none"
            ))
        })
    }

    async fn spawn_worker_thread(&self, req: SpawnRequest) -> Result<BbThreadId, HostError> {
        let thread = deterministic_thread_id(&req.wave, &req.role, req.attempt)?;
        let provider_instance = strip_t3_provider_prefix(req.agent.provider.as_str()).to_owned();

        let resolved_project =
            resolve_or_ensure_t3_project(&self.rpc, &req.project, self.cfg.bb_data_dir.as_deref())
                .await?;
        self.replace_or_merge_project_names(&resolved_project.all_projects, false);
        if req.project.as_str() != resolved_project.t3_project_id {
            self.upsert_project_name(req.project.as_str(), &resolved_project.title);
        }

        let model = self
            .resolve_model_for_spawn(&provider_instance, req.agent.model.as_ref())
            .await?;

        // Provision the worker token before launch so T3's process sees it immediately on startup.
        let provisioned = if let Some(state_dir) = &self.cfg.state_dir {
            Some(provision_worker_token(
                state_dir,
                &req.wave,
                &req.role,
                &thread,
                self.token_minter.as_ref(),
            )?)
        } else {
            None
        };

        let payload = launch_thread_payload(
            &req,
            &thread,
            &resolved_project.t3_project_id,
            &provider_instance,
            &model,
        );
        let reply = match self.rpc.call("orchestration.launchThread", payload).await {
            Ok(v) => v,
            Err(err @ HostError::Rejected(_)) => {
                if let Some(prov) = provisioned
                    && prov.created
                    && let Some(state_dir) = &self.cfg.state_dir
                {
                    let _ = remove_worker_token_file(state_dir, thread.as_str());
                }
                return Err(err);
            }
            Err(err) => return Err(err),
        };

        let returned = reply
            .get("threadId")
            .and_then(|v| v.as_str())
            .map(BbThreadId::parse)
            .transpose()
            .map_err(|e| HostError::Rejected(e.to_string()))?
            .unwrap_or(thread);

        let (reply_model, reply_account) = extract_runtime_meta_from_value(&reply);
        let _ = self.state.send(StateMsg::BindThread {
            wave: req.wave.clone(),
            role: req.role.clone(),
            attempt: req.attempt,
            thread: returned.clone(),
        });
        self.observe_runtime_meta_if_present(&req.wave, &req.role, reply_model, reply_account)
            .await;
        Ok(returned)
    }

    async fn check_thread_live(
        &self,
        thread: &BbThreadId,
    ) -> Result<Option<Observation>, HostError> {
        match self.fetch_thread_projection(thread).await {
            Ok(raw_proj) => {
                let proj = unwrap_projection(&raw_proj);
                if proj
                    .get("thread")
                    .and_then(|t| t.get("archivedAt"))
                    .is_some_and(|v| !v.is_null() && v.as_str().is_none_or(|s| !s.is_empty()))
                {
                    self.unlink_worker_token(thread);
                    return Ok(Some(Observation::Archived));
                }
                if let Some(Activity::Refused { reason }) = derive_activity_from_projection(proj) {
                    if let Some((wave, role, _, false)) = self.resolve_worker_identity(thread).await
                    {
                        self.mirror_turns_from_projection(
                            &wave,
                            &role,
                            thread,
                            None,
                            proj,
                            TerminalTurnState::Suppressed,
                        )
                        .await;
                    }
                    return Ok(Some(Observation::Activity {
                        activity: Activity::Refused { reason },
                    }));
                }
                Ok(None)
            }
            Err(HostError::Rejected(msg)) => {
                let obs = classify_t3_missing_or_archived(&msg);
                if matches!(obs, Some(Observation::Archived)) {
                    self.unlink_worker_token(thread);
                }
                Ok(obs)
            }
            Err(HostError::Unreachable(msg)) => Err(HostError::Unreachable(msg)),
        }
    }

    async fn tell_worker_thread(
        &self,
        thread: &BbThreadId,
        body: &Body,
        mode: SteerDeliveryMode,
    ) -> Result<(), HostError> {
        let nonce = self.next_nonce();
        let command_id = format!("rex-wave:tell:{thread}:{nonce}");
        let message_id = format!("rex-wave:msg:{thread}:{nonce}");
        let payload = message_dispatch_payload(thread, body, mode, command_id, message_id);
        self.rpc
            .call("orchestration.dispatchCommand", payload)
            .await
            .map(|_| ())
    }

    async fn resolve_worker_interaction(
        &self,
        thread: &BbThreadId,
        interaction: &BbInteractionId,
        resolution: &InteractionResolution,
    ) -> Result<(), HostError> {
        let nonce = self.next_nonce();
        let command_id = format!("rex-wave:resolve:{thread}:{interaction}:{nonce}");
        let payload = runtime_request_respond_payload(thread, interaction, resolution, command_id)?;
        self.rpc
            .call("orchestration.dispatchCommand", payload)
            .await
            .map(|_| ())
    }

    async fn retire_worker_thread(&self, thread: &BbThreadId) -> Result<(), HostError> {
        let stop_nonce = self.next_nonce();
        let stop_payload =
            thread_stop_payload(thread, format!("rex-wave:stop:{thread}:{stop_nonce}"));
        if let Err(err) = self
            .rpc
            .call("orchestration.dispatchCommand", stop_payload)
            .await
        {
            match &err {
                HostError::Rejected(msg) if is_benign_stop_rejection(msg) => {}
                HostError::Rejected(_) => {
                    if let Err(force_err) = self
                        .rpc
                        .call(
                            "orchestration.forceStopThread",
                            force_stop_thread_payload(thread),
                        )
                        .await
                    {
                        match force_err {
                            HostError::Rejected(msg) if is_benign_stop_rejection(&msg) => {}
                            other => return Err(other),
                        }
                    }
                }
                HostError::Unreachable(_) => {
                    if !self.rpc.connected() {
                        return Err(err);
                    }
                    if let Err(force_err) = self
                        .rpc
                        .call(
                            "orchestration.forceStopThread",
                            force_stop_thread_payload(thread),
                        )
                        .await
                    {
                        match force_err {
                            HostError::Rejected(msg) if is_benign_stop_rejection(&msg) => {}
                            other => return Err(other),
                        }
                    }
                }
            }
        }

        let archive_nonce = self.next_nonce();
        let archive_payload =
            thread_archive_payload(thread, format!("rex-wave:archive:{thread}:{archive_nonce}"));
        match self
            .rpc
            .call("orchestration.dispatchCommand", archive_payload)
            .await
        {
            Ok(_) => {}
            Err(HostError::Rejected(msg)) if is_already_archived(&msg) => {}
            Err(e) => return Err(e),
        }

        self.unlink_worker_token(thread);
        if let Some((wave, role, _, _)) = self.resolve_worker_identity(thread).await {
            self.call_wave_observe(wave, role, Observation::Archived)
                .await;
        }
        Ok(())
    }

    async fn lookup_worker(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
    ) -> Result<TrackedWorker, ApiError> {
        let (tx, rx) = oneshot::channel();
        let _ = self.state.send(StateMsg::LookupByRole {
            wave: wave.clone(),
            role: role.clone(),
            reply: tx,
        });
        if let Ok(Some(rec)) = rx.await
            && let Some(ref th) = rec.thread
            && is_t3_thread_id(th)
        {
            return Ok(rec);
        }

        if let Some(backend) = self.backend.get()
            && let WireReply::WaveView(snap) = backend
                .call_as(
                    self.cfg.system_principal.clone(),
                    WaveView { wave: wave.clone() }.into_wire(),
                )
                .await?
        {
            for (r, _, phase) in snap.workers {
                if &r == role {
                    return match phase {
                        WorkerPhase::Live { thread, activity } if is_t3_thread_id(&thread) => {
                            let pending_interaction = match &activity {
                                Activity::Blocked { interaction } => Some(interaction.clone()),
                                _ => None,
                            };
                            let attempt =
                                parse_t3_thread_id(&thread).map(|(_, _, a)| a).unwrap_or(1);
                            let _ = self.state.send(StateMsg::BindThread {
                                wave: wave.clone(),
                                role: role.clone(),
                                attempt,
                                thread: thread.clone(),
                            });
                            Ok(TrackedWorker {
                                attempt,
                                thread: Some(thread),
                                live: true,
                                activity,
                                pending_interaction,
                                shell_epoch: 0,
                                mirror_room_watermark: Seq::ORIGIN,
                                settled_turn_keys: BTreeSet::new(),
                                suppressed_turn_keys: BTreeSet::new(),
                                claimed_direct_posts: BTreeSet::new(),
                                hydrated_suppressions: false,
                            })
                        }
                        WorkerPhase::Retiring { thread, .. }
                        | WorkerPhase::Retired {
                            thread: Some(thread),
                            ..
                        } if is_t3_thread_id(&thread) => {
                            let attempt =
                                parse_t3_thread_id(&thread).map(|(_, _, a)| a).unwrap_or(1);
                            Ok(TrackedWorker {
                                attempt,
                                thread: Some(thread),
                                live: false,
                                activity: Activity::Idle,
                                pending_interaction: None,
                                shell_epoch: 0,
                                mirror_room_watermark: Seq::ORIGIN,
                                settled_turn_keys: BTreeSet::new(),
                                suppressed_turn_keys: BTreeSet::new(),
                                claimed_direct_posts: BTreeSet::new(),
                                hydrated_suppressions: false,
                            })
                        }
                        _ => Err(ApiError::NotFound {
                            what: format!("T3 thread for @{wave}/{role}"),
                        }),
                    };
                }
            }
        }

        Err(ApiError::NotFound {
            what: format!("T3 worker @{wave}/{role}"),
        })
    }

    async fn fetch_thread_page(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        after: Option<BbSeq>,
        limit: u32,
    ) -> Result<ThreadPageView, ApiError> {
        let rec = self.lookup_worker(wave, role).await?;
        let thread = rec.thread.ok_or_else(|| ApiError::NotFound {
            what: format!("T3 thread for @{wave}/{role}"),
        })?;
        let raw_proj = self
            .fetch_thread_projection(&thread)
            .await
            .map_err(|e| ApiError::Transport(e.to_string()))?;
        let proj = unwrap_projection(&raw_proj);

        let mut events = Vec::new();
        if let Some(items) = proj.get("turnItems").and_then(|v| v.as_array()) {
            let mut ordered: Vec<(usize, &Value)> = items.iter().enumerate().collect();
            ordered.sort_by_key(|(idx, item)| {
                item.get("ordinal")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(*idx as u64)
            });
            for (idx, item) in ordered {
                if let Some(ev) = normalize_turn_item(item, idx as u64) {
                    if let Some(after_seq) = after
                        && ev.seq <= after_seq
                    {
                        continue;
                    }
                    events.push(ev);
                }
            }
        }
        if limit > 0 && events.len() > limit as usize {
            events.truncate(limit as usize);
        }

        let next = events.last().map(|e| e.seq);
        let usage = parse_provider_turns_usage(proj);
        let pending = active_pending_interaction(proj).or(rec.pending_interaction);
        let activity = if let Some(ref p) = pending {
            Activity::Blocked {
                interaction: p.clone(),
            }
        } else {
            derive_activity_from_projection(proj).unwrap_or(rec.activity)
        };

        Ok(ThreadPageView {
            thread,
            activity,
            events,
            usage,
            pending,
            next,
        })
    }

    async fn open_live_stream(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
    ) -> Result<FrameStream, ApiError> {
        let rec = self.lookup_worker(wave, role).await?;
        let thread = rec.thread.ok_or_else(|| ApiError::NotFound {
            what: format!("T3 thread for @{wave}/{role}"),
        })?;
        self.rpc
            .ensure_connected()
            .await
            .map_err(|e| ApiError::Transport(e.to_string()))?;

        let initial_payload = json!({ "threadId": thread.as_str() });
        let rpc_stream = self
            .rpc
            .open_stream("orchestration.subscribeThread", initial_payload)
            .await
            .map_err(|e| ApiError::Transport(e.to_string()))?;
        let (mut ev_rx, lease) = rpc_stream.into_parts();

        let (frame_tx, frame_rx) = mpsc::channel::<Result<Frame, ApiError>>(128);
        let (resume_tx, resume_rx) = mpsc::unbounded_channel::<Value>();
        let wave_owned = wave.clone();
        let role_owned = role.clone();
        let thread_str = thread.to_string();

        tokio::spawn(async move {
            let mut tracker = LiveSeqTracker::default();
            loop {
                tokio::select! {
                    biased;
                    () = frame_tx.closed() => break,
                    item = ev_rx.recv() => {
                        let Some(ev) = item else { break };
                        match ev {
                            StreamEvent::Reconnected => {}
                            StreamEvent::Closed(err) => {
                                if let Some(e) = err {
                                    let _ = frame_tx.send(Err(ApiError::Transport(e.to_string()))).await;
                                }
                                break;
                            }
                            StreamEvent::Values(values) => {
                                for val in values {
                                    let Some(kind) = val.get("kind").and_then(|v| v.as_str()) else {
                                        continue;
                                    };
                                    match kind {
                                        "snapshot" => {
                                            if let Some(seq) = val.get("snapshotSequence").and_then(|v| v.as_u64()) {
                                                let _ = resume_tx.send(json!({
                                                    "threadId": thread_str,
                                                    "afterSequence": seq,
                                                }));
                                            }
                                            let proj = unwrap_projection(&val);
                                            if let Some(items) = proj.get("turnItems").and_then(|v| v.as_array()) {
                                                let mut ordered: Vec<(usize, &Value)> = items.iter().enumerate().collect();
                                                ordered.sort_by_key(|(idx, it)| {
                                                    it.get("ordinal").and_then(|v| v.as_u64()).unwrap_or(*idx as u64)
                                                });
                                                for (idx, it) in ordered {
                                                    if let Some(agent_ev) = tracker.accept_turn_item(it, idx as u64)
                                                        && frame_tx.send(Ok(Frame::Agent {
                                                            wave: wave_owned.clone(),
                                                            role: role_owned.clone(),
                                                            event: agent_ev,
                                                        })).await.is_err()
                                                    {
                                                        return;
                                                    }
                                                }
                                            }
                                        }
                                        "event" => {
                                            let seq = val.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0);
                                            if seq > 0 {
                                                let _ = resume_tx.send(json!({
                                                    "threadId": thread_str,
                                                    "afterSequence": seq,
                                                }));
                                            }
                                            if let Some(domain_ev) = val.get("event")
                                                && let Some(agent_ev) = tracker.accept_domain_event(seq, domain_ev)
                                                && frame_tx.send(Ok(Frame::Agent {
                                                    wave: wave_owned.clone(),
                                                    role: role_owned.clone(),
                                                    event: agent_ev,
                                                })).await.is_err()
                                            {
                                                return;
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        // Hold `lease` directly inside the returned stream so dropping `FrameStream`
        // synchronously sends `Interrupt` to T3 on drop.
        let stream = futures_util::stream::unfold(
            (frame_rx, resume_rx, lease),
            |(mut frame_rx, mut resume_rx, mut lease)| async move {
                loop {
                    tokio::select! {
                        biased;
                        Some(payload) = resume_rx.recv() => {
                            lease.update_resume_payload(payload);
                        }
                        item = frame_rx.recv() => match item {
                            Some(frame) => {
                                while let Ok(payload) = resume_rx.try_recv() {
                                    lease.update_resume_payload(payload);
                                }
                                return Some((frame, (frame_rx, resume_rx, lease)));
                            }
                            None => {
                                lease.disarm();
                                return None;
                            }
                        }
                    }
                }
            },
        );
        Ok(Box::pin(stream))
    }
}

fn is_already_archived(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("already archived") || lower.contains("not found")
}

fn is_benign_stop_rejection(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("no active run")
        || lower.contains("not running")
        || lower.contains("already stopped")
        || lower.contains("not found")
}

fn classify_t3_missing_or_archived(msg: &str) -> Option<Observation> {
    let lower = msg.to_ascii_lowercase();
    // T3 HTTP errors include the request path (`GET /api/orchestration/threads/t3%3A<wave>%3A<role>... returned HTTP <status>: <body>`).
    // Inspect only the status+body suffix after `returned http ` when present so wave/role names
    // containing `archived` or `404` cannot false-match on a 401/403/400 response.
    let body = lower
        .find("returned http ")
        .map(|idx| &lower[idx..])
        .unwrap_or(lower.as_str());
    if body.contains("returned http 404")
        || body.contains("not found")
        || body.contains("unknown thread")
        || body.contains("no such thread")
        || body.contains("does not exist")
        || body.contains("thread deleted")
        || body.contains("thread_not_found")
    {
        Some(Observation::Activity {
            activity: Activity::Errored {
                error: format!("host thread missing after restart ({msg})"),
            },
        })
    } else if body.contains("returned http 410")
        || body.contains("already archived")
        || body.contains("thread archived")
        || body.contains("is archived")
    {
        Some(Observation::Archived)
    } else {
        None
    }
}

fn projection_advanced_past_failure(proj: &Value) -> bool {
    if projection_turn_slot_count(proj) < 2 {
        return false;
    }
    if classify_projection_terminal_state(proj) == TerminalTurnState::Suppressed {
        return false;
    }
    if proj
        .get("runs")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
    {
        return true;
    }
    if active_pending_interaction(proj).is_some() {
        return true;
    }
    matches!(
        proj.get("thread")
            .and_then(|t| derive_shell_observation(t, None)),
        Some(
            ShellObservation::Working
                | ShellObservation::Blocked { .. }
                | ShellObservation::Idle {
                    mirror_candidate: true,
                }
        )
    )
}

fn classify_projection_terminal_state(proj: &Value) -> TerminalTurnState {
    if active_pending_interaction(proj).is_some() {
        return TerminalTurnState::InProgress;
    }
    let thread_obs = proj
        .get("thread")
        .and_then(|t| derive_shell_observation(t, None));
    if let Some(runs) = proj
        .get("runs")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
        && let Some((_, latest)) = runs.iter().enumerate().max_by_key(|(idx, r)| {
            (
                r.get("ordinal")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(*idx as u64),
                *idx,
            )
        })
        && let Some(status) = latest.get("status").and_then(|v| v.as_str())
    {
        return match status {
            "preparing" | "queued" | "starting" | "running" | "waiting" => {
                TerminalTurnState::InProgress
            }
            "failed" | "interrupted" | "cancelled" | "rolled_back" => TerminalTurnState::Suppressed,
            other if rex_wave_api::wave::is_refusal_stop_reason(other) => {
                TerminalTurnState::Suppressed
            }
            "idle" | "completed" => {
                if extract_explicit_stop_refusal(latest).is_some()
                    || matches!(
                        thread_obs,
                        Some(
                            ShellObservation::Refused(_)
                                | ShellObservation::Errored(_)
                                | ShellObservation::Idle {
                                    mirror_candidate: false,
                                }
                        )
                    )
                {
                    TerminalTurnState::Suppressed
                } else {
                    TerminalTurnState::Completed
                }
            }
            _ => TerminalTurnState::InProgress,
        };
    }
    match thread_obs {
        Some(ShellObservation::Working | ShellObservation::Blocked { .. }) => {
            TerminalTurnState::InProgress
        }
        Some(
            ShellObservation::Refused(_)
            | ShellObservation::Errored(_)
            | ShellObservation::Idle {
                mirror_candidate: false,
            }
            | ShellObservation::Archived,
        ) => TerminalTurnState::Suppressed,
        Some(ShellObservation::Idle {
            mirror_candidate: true,
        })
        | None => TerminalTurnState::Completed,
    }
}

fn derive_activity_from_projection(proj: &Value) -> Option<Activity> {
    let thread_obs = proj
        .get("thread")
        .and_then(|t| derive_shell_observation(t, None));
    if let Some(runs) = proj.get("runs").and_then(|v| v.as_array())
        && let Some((_, latest)) = runs.iter().enumerate().max_by_key(|(idx, r)| {
            (
                r.get("ordinal")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(*idx as u64),
                *idx,
            )
        })
        && let Some(status) = latest.get("status").and_then(|v| v.as_str())
    {
        return match status {
            "preparing" | "queued" | "starting" | "running" | "waiting" => Some(Activity::Working),
            "failed" => {
                if let Some(reason) = extract_refusal_reason(latest) {
                    return Some(Activity::Refused { reason });
                }
                let err = latest
                    .get("error")
                    .or_else(|| latest.get("lastError"))
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or("T3 run failed")
                    .to_owned();
                Some(Activity::Errored { error: err })
            }
            "idle" | "completed" => {
                if let Some(reason) = extract_explicit_stop_refusal(latest) {
                    return Some(Activity::Refused { reason });
                }
                match thread_obs {
                    Some(ShellObservation::Refused(reason)) => Some(Activity::Refused { reason }),
                    Some(ShellObservation::Errored(error)) => Some(Activity::Errored { error }),
                    _ => Some(Activity::Idle),
                }
            }
            "interrupted" | "cancelled" | "rolled_back" => Some(Activity::Idle),
            other if rex_wave_api::wave::is_refusal_stop_reason(other) => Some(Activity::Refused {
                reason: extract_refusal_reason(latest).unwrap_or_else(|| other.to_owned()),
            }),
            _ => None,
        };
    }
    match thread_obs {
        Some(ShellObservation::Working) => Some(Activity::Working),
        Some(ShellObservation::Refused(reason)) => Some(Activity::Refused { reason }),
        Some(ShellObservation::Errored(error)) => Some(Activity::Errored { error }),
        Some(ShellObservation::Idle { .. }) => Some(Activity::Idle),
        _ => None,
    }
}

/// `rex_wave_waves::AgentHost` adapter backed by [`T3Bridge`].
#[derive(Clone)]
pub struct T3Host(pub Arc<T3Bridge>);

impl AgentHost for T3Host {
    fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<BbThreadId, HostError>> {
        let bridge = self.0.clone();
        Box::pin(async move { bridge.spawn_worker_thread(req).await })
    }

    fn tell(
        &self,
        thread: &BbThreadId,
        body: &Body,
        mode: SteerDeliveryMode,
    ) -> BoxFut<'_, Result<(), HostError>> {
        let bridge = self.0.clone();
        let thread = thread.clone();
        let body = body.clone();
        Box::pin(async move { bridge.tell_worker_thread(&thread, &body, mode).await })
    }

    fn resolve(
        &self,
        thread: &BbThreadId,
        interaction: &BbInteractionId,
        resolution: &InteractionResolution,
    ) -> BoxFut<'_, Result<(), HostError>> {
        let bridge = self.0.clone();
        let thread = thread.clone();
        let interaction = interaction.clone();
        let resolution = resolution.clone();
        Box::pin(async move {
            bridge
                .resolve_worker_interaction(&thread, &interaction, &resolution)
                .await
        })
    }

    fn retire(&self, thread: &BbThreadId) -> BoxFut<'_, Result<(), HostError>> {
        let bridge = self.0.clone();
        let thread = thread.clone();
        Box::pin(async move { bridge.retire_worker_thread(&thread).await })
    }

    fn check_live(
        &self,
        thread: &BbThreadId,
    ) -> BoxFut<'_, Result<Option<Observation>, HostError>> {
        let bridge = self.0.clone();
        let thread = thread.clone();
        Box::pin(async move { bridge.check_thread_live(&thread).await })
    }
}

/// `rex_wave_runtime::AgentFeed` adapter backed by [`T3Bridge`].
#[derive(Clone)]
pub struct T3Feed(pub Arc<T3Bridge>);

impl AgentFeed for T3Bridge {
    fn page(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        after: Option<BbSeq>,
        limit: u32,
    ) -> BoxFut<'_, Result<ThreadPageView, ApiError>> {
        let wave = wave.clone();
        let role = role.clone();
        Box::pin(async move { self.fetch_thread_page(&wave, &role, after, limit).await })
    }

    fn live(&self, wave: &WaveSlug, role: &RoleName) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        let wave = wave.clone();
        let role = role.clone();
        Box::pin(async move { self.open_live_stream(&wave, &role).await })
    }

    fn project_names(&self) -> Option<watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>> {
        Some(self.project_names_rx.clone())
    }
}

impl AgentFeed for T3Feed {
    fn page(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        after: Option<BbSeq>,
        limit: u32,
    ) -> BoxFut<'_, Result<ThreadPageView, ApiError>> {
        self.0.page(wave, role, after, limit)
    }

    fn live(&self, wave: &WaveSlug, role: &RoleName) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        self.0.live(wave, role)
    }

    fn project_names(&self) -> Option<watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>> {
        self.0.project_names()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_t3_missing_or_archived_ignores_wave_or_role_names_in_url_path() {
        let auth_err_with_archived_slug = "GET /api/orchestration/threads/t3%3Aarchived-404%3Aworker%3Aa1 returned HTTP 401: unauthorized";
        assert_eq!(
            classify_t3_missing_or_archived(auth_err_with_archived_slug),
            None
        );

        let not_found_err = "GET /api/orchestration/threads/t3%3Aw1%3Aworker%3Aa1 returned HTTP 404: thread not found";
        assert!(matches!(
            classify_t3_missing_or_archived(not_found_err),
            Some(Observation::Activity {
                activity: Activity::Errored { ref error }
            }) if error.contains("missing after restart")
        ));

        let gone_err =
            "GET /api/orchestration/threads/t3%3Aw1%3Aworker%3Aa1 returned HTTP 410: gone";
        assert_eq!(
            classify_t3_missing_or_archived(gone_err),
            Some(Observation::Archived)
        );
    }
}
