//! `T3Bridge`, `T3Host` (`AgentHost`), `T3Feed` (`AgentFeed`), and live T3 shell reactor.

use crate::discovery::{T3Config, percent_encode_query, state_db_path};
use crate::projects::{
    T3ProjectInfo, load_t3_project_names_from_sqlite, parse_t3_projects_snapshot,
    project_names_map, resolve_or_ensure_t3_project,
};
use crate::protocol::{
    LiveSeqTracker, ShellObservation, TurnMirrorCheck, active_pending_interaction,
    default_model_from_server_config, derive_shell_observation, deterministic_thread_id,
    extract_explicit_stop_refusal, extract_refusal_reason, extract_runtime_meta_from_value,
    force_stop_thread_payload, inspect_latest_completed_turn, is_t3_thread_id,
    launch_thread_payload, message_dispatch_payload, mirror_idem_key, normalize_turn_item,
    parse_pending_interaction_from_projection, parse_provider_turns_usage, parse_t3_thread_id,
    runtime_request_respond_payload, strip_t3_provider_prefix, thread_archive_payload,
    thread_stop_payload, unwrap_projection,
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
use std::collections::{BTreeMap, HashMap};
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
    mirror_room_watermark: Seq,
}

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
        reply: oneshot::Sender<Vec<(WaveSlug, RoleName, BbThreadId, Activity)>>,
    },
    MirrorWatermark {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        reply: oneshot::Sender<Seq>,
    },
    AdvanceMirrorWatermark {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        seq: Seq,
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
                            mirror_room_watermark: Seq::ORIGIN,
                        });
                    if rec.thread.as_ref() != Some(&thread) {
                        rec.mirror_room_watermark = Seq::ORIGIN;
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
                            out.push((w.clone(), r.clone(), th.clone(), rec.activity.clone()));
                        }
                    }
                    let _ = reply.send(out);
                }
                StateMsg::MirrorWatermark {
                    wave,
                    role,
                    thread,
                    reply,
                } => {
                    let wm = match self.workers_by_role.get(&(wave, role)) {
                        Some(r) if r.thread.as_ref() == Some(&thread) => r.mirror_room_watermark,
                        _ => Seq::ORIGIN,
                    };
                    let _ = reply.send(wm);
                }
                StateMsg::AdvanceMirrorWatermark {
                    wave,
                    role,
                    thread,
                    seq,
                } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                        && rec.mirror_room_watermark < seq
                    {
                        rec.mirror_room_watermark = seq;
                    }
                }
            }
        }
    }

    fn fold_wave(&mut self, wave: WaveSlug, event: WaveEvent) {
        match event {
            WaveEvent::Started { .. } => {}
            WaveEvent::WorkerRequested { role, attempt, .. } => {
                if let Some(prev) = self.workers_by_role.get(&(wave.clone(), role.clone()))
                    && let Some(ref th) = prev.thread
                {
                    self.role_by_thread.remove(th);
                }
                self.workers_by_role.insert(
                    (wave, role),
                    TrackedWorker {
                        attempt,
                        thread: None,
                        live: false,
                        activity: Activity::Working,
                        pending_interaction: None,
                        mirror_room_watermark: Seq::ORIGIN,
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
                            rec.mirror_room_watermark = Seq::ORIGIN;
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
                            mirror_room_watermark: Seq::ORIGIN,
                        });
                    if rec.thread.as_ref() != Some(&thread) {
                        rec.mirror_room_watermark = Seq::ORIGIN;
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

    async fn call_wave_observe(&self, wave: WaveSlug, role: RoleName, observation: Observation) {
        self.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: role.clone(),
                observation: observation.clone(),
            },
        );
        if let Some(backend) = self.backend.get() {
            let _ = backend
                .call_as(
                    self.cfg.system_principal.clone(),
                    WaveObserve {
                        wave,
                        role,
                        observation,
                    }
                    .into_wire(),
                )
                .await;
        }
    }

    async fn fetch_thread_projection(&self, thread: &BbThreadId) -> Result<Value, HostError> {
        let encoded = percent_encode_query(thread.as_str());
        let path = format!("/api/orchestration/threads/{encoded}");
        self.rpc.http_get_json(&path).await
    }

    async fn process_thread_removed(&self, thread: &BbThreadId) {
        self.unlink_worker_token(thread);
        if let Some((wave, role, _, _)) = self.resolve_worker_identity(thread).await {
            self.call_wave_observe(wave, role, Observation::Archived)
                .await;
        }
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
                self.observe_runtime_meta_if_present(&wave, &role, shell_model, shell_account)
                    .await;
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
                self.observe_runtime_meta_if_present(&wave, &role, shell_model, shell_account)
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
                self.observe_runtime_meta_if_present(&wave, &role, shell_model, shell_account)
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
                let fetched_proj = if mirror_candidate {
                    self.fetch_thread_projection(thread).await.ok()
                } else {
                    None
                };
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
                if let Some(proj) = fetched_proj.as_ref()
                    && let Some(Activity::Refused { reason }) =
                        derive_activity_from_projection(unwrap_projection(proj))
                {
                    self.call_wave_observe(
                        wave,
                        role,
                        Observation::Activity {
                            activity: Activity::Refused { reason },
                        },
                    )
                    .await;
                    return false;
                }
                if let Some(proj) = fetched_proj.as_ref() {
                    self.mirror_completed_turn_from_projection(&wave, &role, thread, proj)
                        .await;
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

    async fn mirror_completed_turn_from_projection(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        projection: &Value,
    ) {
        let check = inspect_latest_completed_turn(projection);
        if matches!(check, TurnMirrorCheck::Ignore) {
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

        let (wm_tx, wm_rx) = oneshot::channel();
        let _ = self.state.send(StateMsg::MirrorWatermark {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
            reply: wm_tx,
        });
        let watermark = wm_rx.await.unwrap_or(Seq::ORIGIN);

        let stream = StreamId::Room { room: room.clone() };
        let mut cursor = watermark;
        let mut room_head = watermark;
        let mut safe_watermark = watermark;
        let mut watermark_pinned = false;
        let completed = match check {
            TurnMirrorCheck::Ignore => return,
            TurnMirrorCheck::PostedViaTool { run_end_at } => {
                let mut tool_reached_end = false;
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
                            && let Event::Room(RoomEvent::Posted { origin, .. }) = &env.event
                            && *origin != rex_wave_api::room::PostOrigin::MirroredTurn
                            && run_end_at > 0
                            && env.at.0 > run_end_at
                        {
                            watermark_pinned = true;
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
                    let _ = self.state.send(StateMsg::AdvanceMirrorWatermark {
                        wave: wave.clone(),
                        role: role.clone(),
                        thread: thread.clone(),
                        seq: safe_watermark,
                    });
                }
                return;
            }
            TurnMirrorCheck::Unposted(c) => c,
        };

        // `WaveConductor` skips peer wakeups when idem starts with `wave:` and contains `:turn:`.
        let Ok(idem_key) = mirror_idem_key(wave, role, thread, &completed.anchor) else {
            return;
        };

        let mut already_posted = false;
        let mut reached_end = false;
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
            }
            cursor = last.seq;
            room_head = last.seq;
            let count = page.entries.len();
            for env in &page.entries {
                if let Event::Room(RoomEvent::Posted { idem, origin, .. }) = &env.event {
                    if idem.as_ref() == Some(&idem_key) {
                        if !watermark_pinned {
                            safe_watermark = env.seq;
                        }
                        already_posted = true;
                        break;
                    }
                    if *origin != rex_wave_api::room::PostOrigin::MirroredTurn
                        && env.by == worker_principal
                        && (completed.run_start_at == 0 || env.at.0 >= completed.run_start_at)
                    {
                        if completed.run_end_at > 0 && env.at.0 > completed.run_end_at {
                            watermark_pinned = true;
                        } else {
                            if !watermark_pinned {
                                safe_watermark = env.seq;
                            }
                            already_posted = true;
                            break;
                        }
                    }
                }
                if !watermark_pinned {
                    safe_watermark = env.seq;
                }
            }
            if already_posted {
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
            let _ = self.state.send(StateMsg::AdvanceMirrorWatermark {
                wave: wave.clone(),
                role: role.clone(),
                thread: thread.clone(),
                seq: safe_watermark,
            });
            return;
        }

        let trimmed = truncate_with_notice(&completed.text, 64 * 1024);
        let Ok(body) = Body::parse(trimmed) else {
            return;
        };
        let post_cmd = RoomPost::mirrored_turn(RoomRef::Id { id: room.clone() }, body, idem_key);
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
                        sys,
                        RoomJoin {
                            room: RoomRef::Id { id: room },
                            member: Some(worker_principal.clone()),
                            handle: Handle::parse(role.as_str()).ok(),
                        }
                        .into_wire(),
                    )
                    .await;
                backend
                    .call_as(worker_principal, post_cmd.into_wire())
                    .await
            }
            other => other,
        };
        if let Ok(WireReply::RoomPost(posted)) = post_res {
            let posted_head = if !watermark_pinned && posted.seq.0 == room_head.0.saturating_add(1)
            {
                posted.seq
            } else {
                safe_watermark
            };
            let _ = self.state.send(StateMsg::AdvanceMirrorWatermark {
                wave: wave.clone(),
                role: role.clone(),
                thread: thread.clone(),
                seq: posted_head,
            });
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
        for (wave, role, thread, tracked_activity) in workers {
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
                            self.call_wave_observe(wave, role, obs).await;
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
                if matches!(activity, Activity::Idle) {
                    self.mirror_completed_turn_from_projection(&wave, &role, &thread, proj)
                        .await;
                }
                self.call_wave_observe(wave, role, Observation::Activity { activity })
                    .await;
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
                                mirror_room_watermark: Seq::ORIGIN,
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
                                mirror_room_watermark: Seq::ORIGIN,
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

fn derive_activity_from_projection(proj: &Value) -> Option<Activity> {
    if let Some(runs) = proj.get("runs").and_then(|v| v.as_array())
        && let Some(latest) = runs
            .iter()
            .max_by_key(|r| r.get("ordinal").and_then(|v| v.as_u64()).unwrap_or(0))
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
                Some(Activity::Idle)
            }
            "interrupted" | "cancelled" | "rolled_back" => Some(Activity::Idle),
            other if rex_wave_api::wave::is_refusal_stop_reason(other) => Some(Activity::Refused {
                reason: extract_refusal_reason(latest).unwrap_or_else(|| other.to_owned()),
            }),
            _ => None,
        };
    }
    if let Some(thread) = proj.get("thread") {
        return match derive_shell_observation(thread, None) {
            Some(ShellObservation::Working) => Some(Activity::Working),
            Some(ShellObservation::Refused(reason)) => Some(Activity::Refused { reason }),
            Some(ShellObservation::Errored(error)) => Some(Activity::Errored { error }),
            Some(ShellObservation::Idle { .. }) => Some(Activity::Idle),
            _ => None,
        };
    }
    None
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
