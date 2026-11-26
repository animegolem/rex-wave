//! Shared backend observation delivery, per-thread `AgentFeed` log, and worker token helpers.

use rex_wave_api::read::{AgentEvent, AgentEventKind, ThreadContextUsage, ThreadPageView};
use rex_wave_api::wave::{
    Activity, Observation, PendingInteraction, WaveObserve, WaveView, WorkerPhase,
};
use rex_wave_api::{
    ApiError, Client, Command, Frame, FrameStream, HostSeq, HostThreadId, Principal, RoleName,
    WaveSlug, WireCommand, WireReply,
};
use rex_wave_runtime::{BoxFut, BridgeBackend, frame_channel};
use rex_wave_transport::{
    ProvisionedWorkerToken, provision_worker_token_in_state_dir, remove_worker_token_file,
};
use rex_wave_waves::HostError;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};

const MAX_RETAINED_EVENTS: usize = 8192;
const LIVE_BROADCAST_CAPACITY: usize = 256;
const LIVE_CHANNEL_CAPACITY: usize = 128;

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
