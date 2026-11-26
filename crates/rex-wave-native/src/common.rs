//! Shared backend observation delivery, single-lock worker registry, bounded stdio I/O,
//! per-thread `AgentFeed` log, environment scrubbing, and worker token helpers.

use rex_wave_api::read::{AgentEvent, AgentEventKind, ThreadContextUsage, ThreadPageView};
use rex_wave_api::wave::{
    Activity, GrantedPermissions, Observation, PendingInteraction, WaveObserve, WaveView,
    WorkerPhase,
};
use rex_wave_api::{
    ApiError, Client, Command, Frame, FrameStream, HostSeq, HostThreadId, Principal, RoleName,
    WaveSlug, WireCommand, WireReply,
};
use rex_wave_runtime::{BoxFut, BridgeBackend, frame_channel};
use rex_wave_transport::{
    ProvisionedWorkerToken, T3_WORKER_THREAD_ENV, provision_worker_token_in_state_dir,
    remove_worker_token_file,
};
use rex_wave_waves::HostError;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::{broadcast, mpsc, oneshot};

const MAX_RETAINED_EVENTS: usize = 16_384;
const MAX_ARCHIVED_EVENTS: usize = 256;
pub const MAX_ARCHIVED_WORKERS: usize = 256;
pub const MAX_STDOUT_LINE_BYTES: usize = 8 * 1024 * 1024;
pub const STDIN_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const LIVE_BROADCAST_CAPACITY: usize = 256;
const LIVE_CHANNEL_CAPACITY: usize = 128;

/// Daemon-scoped credentials, nested CLI markers, and git hook/config overrides scrubbed
/// from native worker child processes and daemon-owned git commands.
pub const SCRUBBED_DAEMON_ENV: &[&str] = &[
    "REX_WAVE_BB_TOKEN",
    "REX_WAVE_T3_TOKEN",
    "REX_WAVE_T3_URL",
    "REX_WAVE_T3_HOME",
    "REX_WAVE_OPERATOR",
    "REX_WAVE_TOKEN",
    "REX_WAVE_SOCKET",
    "REX_WAVE_WORKER_THREAD_ID",
    "REX_WAVE_INTERCHANGE_DIR",
    "REX_WAVE_LIVE_INTERCHANGE_TEST_DIR",
    T3_WORKER_THREAD_ENV,
    "T3CODE_CLI_PATH",
    "T3CODE_DESKTOP_HOST_SCOPE",
    "T3_ACP_MCP_NODE",
    "T3_ACP_MCP_ENDPOINT",
    "T3_ACP_MCP_ENTRYPOINT",
    "T3_ACP_MCP_AUTHORIZATION",
    "CLAUDECODE",
    "CLAUDE_CODE_ENTRYPOINT",
    "CLAUDE_PID",
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_COMMON_DIR",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_PREFIX",
    "GIT_CEILING_DIRECTORIES",
    "GIT_CONFIG_PARAMETERS",
    "GIT_CONFIG_COUNT",
    "GIT_CONFIG_GLOBAL",
    "GIT_CONFIG_SYSTEM",
    "GIT_EXEC_PATH",
    "GIT_TEMPLATE_DIR",
];

pub fn scrub_daemon_env(cmd: &mut tokio::process::Command) {
    for key in SCRUBBED_DAEMON_ENV {
        cmd.env_remove(key);
    }
    for (k, _) in std::env::vars_os() {
        if let Some(s) = k.to_str()
            && (s.starts_with("GIT_CONFIG_KEY_") || s.starts_with("GIT_CONFIG_VALUE_"))
        {
            cmd.env_remove(s);
        }
    }
}

pub fn has_non_empty_granted_permissions(gp: Option<&GrantedPermissions>) -> bool {
    let Some(p) = gp else {
        return false;
    };
    p.file_system
        .as_ref()
        .is_some_and(|fs| !fs.read.is_empty() || !fs.write.is_empty())
        || p.network.as_ref().is_some_and(|n| n.enabled.is_some())
}

pub fn reject_non_empty_granted_permissions(
    gp: Option<&GrantedPermissions>,
    runner_label: &str,
) -> Result<(), HostError> {
    if has_non_empty_granted_permissions(gp) {
        return Err(HostError::Rejected(format!(
            "{runner_label} approval decision cannot enforce custom granted_permissions; omit granted_permissions or pass a protocol-specific RequestAnswer"
        )));
    }
    Ok(())
}

pub type StdinLine = (String, oneshot::Sender<Result<(), String>>);

pub fn spawn_stdin_writer(
    mut stdin: ChildStdin,
    label: &'static str,
) -> mpsc::UnboundedSender<StdinLine> {
    let (tx, mut rx) = mpsc::unbounded_channel::<StdinLine>();
    tokio::spawn(async move {
        while let Some((line, reply)) = rx.recv().await {
            let mut payload = line.into_bytes();
            payload.push(b'\n');
            let write_fut = async {
                stdin.write_all(&payload).await?;
                stdin.flush().await
            };
            let res = match tokio::time::timeout(STDIN_WRITE_TIMEOUT, write_fut).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e.to_string()),
                Err(_) => Err(format!(
                    "{label} stdin write timed out after {}ms",
                    STDIN_WRITE_TIMEOUT.as_millis()
                )),
            };
            let failed = res.is_err();
            let _ = reply.send(res);
            if failed {
                break;
            }
        }
    });
    tx
}

pub async fn write_stdin_line(
    tx: &mpsc::UnboundedSender<StdinLine>,
    line: String,
    label: &'static str,
) -> Result<(), String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send((line, reply_tx))
        .map_err(|_| format!("{label} stdin writer closed"))?;
    match tokio::time::timeout(STDIN_WRITE_TIMEOUT + Duration::from_secs(1), reply_rx).await {
        Ok(Ok(res)) => res,
        Ok(Err(_)) => Err(format!("{label} stdin writer dropped before flushing")),
        Err(_) => Err(format!("{label} stdin write timed out waiting for writer")),
    }
}

pub enum BoundedLine {
    Line(String),
    Oversized(usize),
}

/// Reads a single `\n`-delimited line up to `max_bytes` without unbounded allocation.
/// When a line exceeds `max_bytes`, drains through `\n` (or EOF) and returns `Oversized`.
pub async fn read_bounded_line(
    reader: &mut BufReader<ChildStdout>,
    scratch: &mut Vec<u8>,
    max_bytes: usize,
) -> io::Result<Option<BoundedLine>> {
    scratch.clear();
    let mut overflow_len: Option<usize> = None;

    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            if let Some(total) = overflow_len {
                return Ok(Some(BoundedLine::Oversized(total)));
            }
            if scratch.is_empty() {
                return Ok(None);
            }
            if scratch.last() == Some(&b'\r') {
                scratch.pop();
            }
            let s = String::from_utf8_lossy(scratch).into_owned();
            scratch.clear();
            return Ok(Some(BoundedLine::Line(s)));
        }

        if let Some(nl_pos) = chunk.iter().position(|&b| b == b'\n') {
            let line_part = &chunk[..nl_pos];
            if let Some(ref mut total) = overflow_len {
                *total = total.saturating_add(line_part.len());
                let out_total = *total;
                reader.consume(nl_pos + 1);
                scratch.clear();
                return Ok(Some(BoundedLine::Oversized(out_total)));
            }
            if scratch.len().saturating_add(line_part.len()) > max_bytes {
                let out_total = scratch.len().saturating_add(line_part.len());
                reader.consume(nl_pos + 1);
                scratch.clear();
                return Ok(Some(BoundedLine::Oversized(out_total)));
            }
            scratch.extend_from_slice(line_part);
            reader.consume(nl_pos + 1);
            if scratch.last() == Some(&b'\r') {
                scratch.pop();
            }
            let s = String::from_utf8_lossy(scratch).into_owned();
            scratch.clear();
            return Ok(Some(BoundedLine::Line(s)));
        }

        let n = chunk.len();
        if let Some(ref mut total) = overflow_len {
            *total = total.saturating_add(n);
        } else if scratch.len().saturating_add(n) > max_bytes {
            overflow_len = Some(scratch.len().saturating_add(n));
            scratch.clear();
        } else {
            scratch.extend_from_slice(chunk);
        }
        reader.consume(n);
    }
}

/// Single-lock registry for a native bridge's live and archived workers so
/// `by_thread`, `by_role`, and archived retention never acquire two locks in opposite order.
pub struct WorkerRegistry<W> {
    by_thread: HashMap<HostThreadId, Arc<W>>,
    by_role: HashMap<(WaveSlug, RoleName), (u32, Arc<W>)>,
    archived_order: VecDeque<HostThreadId>,
}

impl<W> Default for WorkerRegistry<W> {
    fn default() -> Self {
        Self {
            by_thread: HashMap::new(),
            by_role: HashMap::new(),
            archived_order: VecDeque::new(),
        }
    }
}

impl<W> WorkerRegistry<W> {
    pub fn get_thread(&self, thread: &HostThreadId) -> Option<Arc<W>> {
        self.by_thread.get(thread).cloned()
    }

    pub fn get_role(&self, wave: &WaveSlug, role: &RoleName) -> Option<Arc<W>> {
        self.by_role
            .get(&(wave.clone(), role.clone()))
            .map(|(_, w)| w.clone())
    }

    pub fn all_threads(&self) -> Vec<HostThreadId> {
        self.by_thread.keys().cloned().collect()
    }

    pub fn insert_live(
        &mut self,
        thread: HostThreadId,
        wave: WaveSlug,
        role: RoleName,
        attempt: u32,
        worker: Arc<W>,
    ) {
        self.archived_order.retain(|t| t != &thread);
        self.by_thread.insert(thread, worker.clone());
        let key = (wave, role);
        let replace = self
            .by_role
            .get(&key)
            .is_none_or(|(prev_attempt, _)| *prev_attempt <= attempt);
        if replace {
            self.by_role.insert(key, (attempt, worker));
        }
    }

    pub fn mark_archived<F>(&mut self, thread: &HostThreadId, mut key_of: F)
    where
        F: FnMut(&W) -> (WaveSlug, RoleName, u32),
    {
        if !self.by_thread.contains_key(thread) {
            return;
        }
        if !self.archived_order.iter().any(|t| t == thread) {
            self.archived_order.push_back(thread.clone());
        }
        while self.archived_order.len() > MAX_ARCHIVED_WORKERS {
            let Some(evict_tid) = self.archived_order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.by_thread.remove(&evict_tid) {
                let (w, r, a) = key_of(&evicted);
                if self
                    .by_role
                    .get(&(w.clone(), r.clone()))
                    .is_some_and(|(cur_a, cur_w)| *cur_a == a && Arc::ptr_eq(cur_w, &evicted))
                {
                    self.by_role.remove(&(w, r));
                }
            }
        }
    }
}

pub trait ErasedBackend: Send + Sync {
    fn call_as(&self, me: Principal, cmd: WireCommand) -> BoxFut<'_, Result<WireReply, ApiError>>;
}

impl<B: BridgeBackend> ErasedBackend for B {
    fn call_as(&self, me: Principal, cmd: WireCommand) -> BoxFut<'_, Result<WireReply, ApiError>> {
        let client = self.client(me);
        Box::pin(async move { client.call_wire(cmd).await })
    }
}

pub enum ObserverItem {
    Observe(Observation),
    Flush(oneshot::Sender<()>),
}

/// Spawns a serialized per-worker observation dispatcher so monotonic `HostSeq`
/// transitions and `Observation::Archived` always commit in causal order.
pub fn spawn_worker_observer(
    backend: Option<Arc<dyn ErasedBackend>>,
    system_principal: Principal,
    wave: WaveSlug,
    role: RoleName,
    thread: HostThreadId,
    attempt: u32,
) -> mpsc::UnboundedSender<ObserverItem> {
    let (tx, mut rx) = mpsc::unbounded_channel::<ObserverItem>();
    tokio::spawn(async move {
        while let Some(item) = rx.recv().await {
            match item {
                ObserverItem::Flush(reply) => {
                    let _ = reply.send(());
                }
                ObserverItem::Observe(obs) => {
                    let Some(ref backend) = backend else {
                        continue;
                    };
                    deliver_observation(
                        backend.as_ref(),
                        &system_principal,
                        &wave,
                        &role,
                        &thread,
                        attempt,
                        obs,
                    )
                    .await;
                }
            }
        }
    });
    tx
}

async fn deliver_observation(
    backend: &dyn ErasedBackend,
    system_principal: &Principal,
    wave: &WaveSlug,
    role: &RoleName,
    thread: &HostThreadId,
    attempt: u32,
    obs: Observation,
) {
    let is_activity = matches!(obs, Observation::Activity { .. });
    let mut delay_ms = 10u64;
    for retry in 0..40u32 {
        let res = backend
            .call_as(
                system_principal.clone(),
                WaveObserve {
                    wave: wave.clone(),
                    role: role.clone(),
                    observation: obs.clone(),
                }
                .into_wire(),
            )
            .await;

        match res {
            Ok(WireReply::WaveObserve(ack)) if ack.applied => return,
            Ok(WireReply::WaveObserve(_)) if is_activity => {
                // Fast child processes can emit Activity before WaveConductor commits Spawned.
                let still_requested = matches!(
                    backend
                        .call_as(
                            system_principal.clone(),
                            WaveView { wave: wave.clone() }.into_wire(),
                        )
                        .await,
                    Ok(WireReply::WaveView(snap))
                        if snap.workers.iter().any(|(r, _, phase)| {
                            r == role
                                && matches!(phase, WorkerPhase::Requested { attempt: a } if *a == attempt)
                        })
                );
                if !still_requested {
                    return;
                }
            }
            Ok(_) => return,
            Err(_) if retry < 5 => {}
            Err(_) => return,
        }

        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        delay_ms = (delay_ms * 2).min(100);
        let _ = thread;
    }
}

pub async fn flush_observer(tx: &mpsc::UnboundedSender<ObserverItem>) {
    let (reply_tx, reply_rx) = oneshot::channel();
    if tx.send(ObserverItem::Flush(reply_tx)).is_ok() {
        let _ = tokio::time::timeout(Duration::from_secs(3), reply_rx).await;
    }
}

/// In-memory per-thread event log backing `AgentFeed::page` and `AgentFeed::live`.
pub struct ThreadEventLog {
    pub thread: HostThreadId,
    pub wave: WaveSlug,
    pub role: RoleName,
    pub attempt: u32,
    events: Vec<AgentEvent>,
    next_event_seq: u64,
    next_host_seq: u64,
    pub usage: Option<ThreadContextUsage>,
    pub activity: Activity,
    pub pending: Option<PendingInteraction>,
    tx: broadcast::Sender<AgentEvent>,
}

impl ThreadEventLog {
    pub fn new(thread: HostThreadId, wave: WaveSlug, role: RoleName, attempt: u32) -> Self {
        let (tx, _) = broadcast::channel(LIVE_BROADCAST_CAPACITY);
        Self {
            thread,
            wave,
            role,
            attempt,
            events: Vec::new(),
            next_event_seq: 1,
            next_host_seq: 0,
            usage: None,
            activity: Activity::Working,
            pending: None,
            tx,
        }
    }

    pub fn push_event(&mut self, kind: AgentEventKind) -> AgentEvent {
        let seq = HostSeq(self.next_event_seq);
        self.next_event_seq = self.next_event_seq.saturating_add(1);
        let ev = AgentEvent {
            seq,
            at: rex_wave_runtime::now(),
            kind,
        };
        if self.events.len() >= MAX_RETAINED_EVENTS {
            let excess = self.events.len() - MAX_RETAINED_EVENTS + 1;
            self.events.drain(..excess);
        }
        self.events.push(ev.clone());
        let _ = self.tx.send(ev.clone());
        ev
    }

    pub fn trim_archived(&mut self) {
        self.pending = None;
        if self.events.len() > MAX_ARCHIVED_EVENTS {
            let excess = self.events.len() - MAX_ARCHIVED_EVENTS;
            self.events.drain(..excess);
        }
    }

    pub fn next_host_seq(&mut self) -> u64 {
        self.next_host_seq = self.next_host_seq.saturating_add(1);
        self.next_host_seq
    }

    pub fn last_event_seq(&self) -> Option<u64> {
        self.events.last().map(|e| e.seq.0)
    }

    pub fn page(&self, after: Option<HostSeq>, limit: u32) -> ThreadPageView {
        let mut out: Vec<AgentEvent> = self
            .events
            .iter()
            .filter(|ev| after.is_none_or(|a| ev.seq > a))
            .cloned()
            .collect();
        if limit > 0 && out.len() > limit as usize {
            out.truncate(limit as usize);
        }
        let next = out.last().map(|e| e.seq);
        ThreadPageView {
            thread: self.thread.clone(),
            activity: self.activity.clone(),
            events: out,
            usage: self.usage.clone(),
            pending: self.pending.clone(),
            next,
        }
    }

    pub fn subscribe_live(log: Arc<Mutex<Self>>) -> FrameStream {
        let (wave, role, backlog, mut rx) = {
            let guard = log.lock().unwrap_or_else(|e| e.into_inner());
            (
                guard.wave.clone(),
                guard.role.clone(),
                guard.events.clone(),
                guard.tx.subscribe(),
            )
        };
        let (frame_tx, stream) = frame_channel(LIVE_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            let mut last_seq = HostSeq(0);
            for ev in backlog {
                last_seq = ev.seq;
                if frame_tx
                    .send(Ok(Frame::Agent {
                        wave: wave.clone(),
                        role: role.clone(),
                        event: ev,
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            loop {
                tokio::select! {
                    biased;
                    () = frame_tx.closed() => break,
                    item = rx.recv() => match item {
                        Ok(ev) => {
                            if ev.seq <= last_seq {
                                continue;
                            }
                            last_seq = ev.seq;
                            if frame_tx
                                .send(Ok(Frame::Agent {
                                    wave: wave.clone(),
                                    role: role.clone(),
                                    event: ev,
                                }))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            let missed: Vec<AgentEvent> = {
                                let guard = log.lock().unwrap_or_else(|e| e.into_inner());
                                guard
                                    .events
                                    .iter()
                                    .filter(|e| e.seq > last_seq)
                                    .cloned()
                                    .collect()
                            };
                            for ev in missed {
                                last_seq = ev.seq;
                                if frame_tx
                                    .send(Ok(Frame::Agent {
                                        wave: wave.clone(),
                                        role: role.clone(),
                                        event: ev,
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        });
        stream
    }
}

pub fn format_thread_id(
    prefix: &str,
    wave: &WaveSlug,
    role: &RoleName,
    attempt: u32,
) -> Result<HostThreadId, HostError> {
    HostThreadId::parse(format!("{prefix}{wave}:{role}:a{attempt}"))
        .map_err(|e| HostError::Rejected(e.to_string()))
}

pub fn parse_prefixed_thread_id(
    prefix: &str,
    thread: &HostThreadId,
) -> Option<(WaveSlug, RoleName, u32)> {
    let rest = thread.as_str().strip_prefix(prefix)?;
    let mut parts = rest.split(':');
    let wave_raw = parts.next()?;
    let role_raw = parts.next()?;
    let attempt_raw = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let attempt: u32 = attempt_raw.strip_prefix('a')?.parse().ok()?;
    if attempt == 0 {
        return None;
    }
    Some((
        WaveSlug::parse(wave_raw).ok()?,
        RoleName::parse(role_raw).ok()?,
        attempt,
    ))
}

pub fn provision_native_worker_token(
    state_dir: Option<&Path>,
    wave: &WaveSlug,
    role: &RoleName,
    thread: &HostThreadId,
) -> Result<Option<ProvisionedWorkerToken>, HostError> {
    let Some(dir) = state_dir else {
        return Ok(None);
    };
    let tok = provision_worker_token_in_state_dir(dir, wave, role, thread)
        .map_err(|e| HostError::Unreachable(format!("provision worker token: {e}")))?;
    Ok(Some(tok))
}

pub fn revoke_native_worker_token(state_dir: Option<&Path>, thread: &HostThreadId) {
    if let Some(dir) = state_dir {
        let _ = remove_worker_token_file(dir, thread.as_str());
    }
}

pub fn truncate_summary(raw: &str, max_bytes: usize) -> String {
    let trimmed = raw.trim();
    if trimmed.len() <= max_bytes {
        return trimmed.to_owned();
    }
    let mut end = max_bytes.saturating_sub(3);
    while end > 0 && !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &trimmed[..end])
}
