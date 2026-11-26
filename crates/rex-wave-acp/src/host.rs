//! `AcpBridge`, `AcpHost` (`AgentHost`), and `AcpFeed` (`AgentFeed`) over
//! child-process NDJSON JSON-RPC 2.0 stdio transport.

use crate::config::AcpConfig;
use crate::process::{
    kill_process_group_now, process_group_alive, terminate_and_reap_process_group,
};
use crate::protocol::{
    InboundMessage, ParsedPermissionRequest, PromptOutcome, classify_process_exit,
    classify_prompt_response, deterministic_thread_id, extract_session_id, extract_session_model,
    format_jsonrpc_error, initialize_request, is_acp_thread_id, jsonrpc_error_code,
    mcp_server_entry, method_not_found_response, normalize_session_update, parse_acp_thread_id,
    parse_inbound_line, parse_permission_request, permission_cancelled_response, rpc_id_matches,
    select_permission_response, session_cancel_notification, session_new_request,
    session_prompt_request, session_set_model_request, session_supports_models,
};
use crate::workspace::resolve_acp_workspace;
use rex_wave_api::Command as _;
use rex_wave_api::read::{AgentEvent, AgentEventKind, ThreadContextUsage, ThreadPageView};
use rex_wave_api::room::Body;
use rex_wave_api::wave::{
    Activity, InteractionResolution, Observation, PendingInteraction, SteerDeliveryMode,
    WaveObserve,
};
use rex_wave_api::*;
pub use rex_wave_runtime::BridgeBackend;
use rex_wave_runtime::{AgentFeed, BoxFut, Router, frame_channel};
use rex_wave_transport::{
    T3_WORKER_THREAD_ENV, WORKER_THREAD_ENV, provision_worker_token_in_state_dir,
    remove_worker_token_file,
};
use rex_wave_waves::{AgentHost, HostError, SpawnRequest};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

const MAX_FEED_EVENTS: usize = 4_096;
const MAX_LINE_BYTES: usize = 8 * 1024 * 1024;
const MAX_STDERR_BYTES: usize = 8 * 1024;
const READER_CHANNEL_CAP: usize = 512;
const FEED_BROADCAST_CAP: usize = 256;
const RETIRE_WRITE_TIMEOUT: Duration = Duration::from_millis(250);

trait ErasedBackend: Send + Sync {
    fn call_as(&self, me: Principal, cmd: WireCommand) -> BoxFut<'_, Result<WireReply, ApiError>>;
}

impl<B: BridgeBackend> ErasedBackend for B {
    fn call_as(&self, me: Principal, cmd: WireCommand) -> BoxFut<'_, Result<WireReply, ApiError>> {
        let client = self.client(me);
        Box::pin(async move { client.call_wire(cmd).await })
    }
}

#[derive(Default)]
struct StderrTail {
    buf: Vec<u8>,
}

impl StderrTail {
    fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
        if self.buf.len() > MAX_STDERR_BYTES {
            let excess = self.buf.len() - MAX_STDERR_BYTES;
            self.buf.drain(..excess);
        }
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.buf).into_owned()
    }
}

enum ReaderMsg {
    Line(String),
    Eof,
}

fn spawn_stdout_reader(
    stdout: tokio::process::ChildStdout,
) -> (mpsc::Receiver<ReaderMsg>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(READER_CHANNEL_CAP);
    let handle = tokio::spawn(async move {
        let mut reader = BufReader::new(stdout);
        let mut line_buf = Vec::new();
        loop {
            line_buf.clear();
            let mut limited = (&mut reader).take(MAX_LINE_BYTES as u64);
            match limited.read_until(b'\n', &mut line_buf).await {
                Ok(0) => {
                    let _ = tx.send(ReaderMsg::Eof).await;
                    break;
                }
                Ok(_) => {
                    if !line_buf.ends_with(b"\n") && line_buf.len() >= MAX_LINE_BYTES {
                        // Drain remainder of oversized line so framing stays aligned on '\n'.
                        let mut discard = Vec::new();
                        let _ = reader.read_until(b'\n', &mut discard).await;
                        continue;
                    }
                    let line = String::from_utf8_lossy(&line_buf).into_owned();
                    if tx.send(ReaderMsg::Line(line)).await.is_err() {
                        break;
                    }
                }
                Err(_) => {
                    let _ = tx.send(ReaderMsg::Eof).await;
                    break;
                }
            }
        }
    });
    (rx, handle)
}

fn spawn_stderr_drainer(
    mut stderr: tokio::process::ChildStderr,
) -> (Arc<Mutex<StderrTail>>, tokio::task::JoinHandle<()>) {
    let tail = Arc::new(Mutex::new(StderrTail::default()));
    let tail_bg = tail.clone();
    let handle = tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        loop {
            match stderr.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if let Ok(mut guard) = tail_bg.lock() {
                        guard.push(&buf[..n]);
                    }
                }
            }
        }
    });
    (tail, handle)
}

async fn write_json_line(stdin: &mut ChildStdin, payload: &Value) -> io::Result<()> {
    let mut encoded =
        serde_json::to_vec(payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    encoded.push(b'\n');
    stdin.write_all(&encoded).await?;
    stdin.flush().await
}

async fn wait_for_rpc_response(
    reader_rx: &mut mpsc::Receiver<ReaderMsg>,
    expected_id: u64,
    timeout: Duration,
    stderr_tail: &Arc<Mutex<StderrTail>>,
) -> Result<Result<Value, Value>, HostError> {
    let fut = async {
        while let Some(msg) = reader_rx.recv().await {
            match msg {
                ReaderMsg::Line(line) => {
                    if let Some(InboundMessage::Response { id, result, error }) =
                        parse_inbound_line(&line)
                        && rpc_id_matches(&id, expected_id)
                    {
                        if let Some(err) = error {
                            return Ok(Err(err));
                        }
                        return Ok(Ok(result.unwrap_or(Value::Null)));
                    }
                }
                ReaderMsg::Eof => {
                    let tail = stderr_tail.lock().map(|g| g.text()).unwrap_or_default();
                    let suffix = if tail.trim().is_empty() {
                        String::new()
                    } else {
                        format!(": {}", tail.trim())
                    };
                    return Err(HostError::Rejected(format!(
                        "ACP child exited before replying to request {expected_id}{suffix}"
                    )));
                }
            }
        }
        Err(HostError::Rejected(format!(
            "ACP reader closed before replying to request {expected_id}"
        )))
    };

    tokio::time::timeout(timeout, fut).await.map_err(|_| {
        HostError::Unreachable(format!(
            "ACP handshake timed out waiting for response to request {expected_id}"
        ))
    })?
}

struct FeedState {
    thread: HostThreadId,
    activity: Activity,
    pending: Option<PendingInteraction>,
    usage: Option<ThreadContextUsage>,
    events: VecDeque<AgentEvent>,
    next_seq: u64,
    closed: bool,
}

struct SessionFeed {
    state: Mutex<FeedState>,
    tx: broadcast::Sender<AgentEvent>,
    closed_tx: watch::Sender<bool>,
}

impl SessionFeed {
    fn new(thread: HostThreadId) -> Self {
        let (tx, _) = broadcast::channel(FEED_BROADCAST_CAP);
        let (closed_tx, _) = watch::channel(false);
        Self {
            state: Mutex::new(FeedState {
                thread,
                activity: Activity::Working,
                pending: None,
                usage: None,
                events: VecDeque::new(),
                next_seq: 1,
                closed: false,
            }),
            tx,
            closed_tx,
        }
    }

    fn push_event(&self, kind: AgentEventKind) -> AgentEvent {
        let at = rex_wave_runtime::now();
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let seq = HostSeq(guard.next_seq);
        guard.next_seq = guard.next_seq.saturating_add(1);
        if let AgentEventKind::Usage { ref usage } = kind {
            guard.usage = Some(usage.clone());
        }
        let ev = AgentEvent { seq, at, kind };
        if guard.events.len() >= MAX_FEED_EVENTS {
            guard.events.pop_front();
        }
        guard.events.push_back(ev.clone());
        let _ = self.tx.send(ev.clone());
        ev
    }

    fn set_activity(&self, activity: Activity) {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let effective = match (&guard.activity, activity) {
            (Activity::Refused { reason }, Activity::Errored { .. }) => Activity::Refused {
                reason: reason.clone(),
            },
            (_, next) => next,
        };
        guard.pending = match &effective {
            Activity::Blocked { interaction } => Some(interaction.clone()),
            _ => None,
        };
        guard.activity = effective;
    }

    fn mark_closed(&self) {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.closed = true;
        guard.pending = None;
        drop(guard);
        let _ = self.closed_tx.send(true);
    }

    fn events_after(&self, after: u64) -> Vec<AgentEvent> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .events
            .iter()
            .filter(|ev| ev.seq.0 > after)
            .cloned()
            .collect()
    }

    fn page(&self, after: Option<HostSeq>, limit: u32) -> ThreadPageView {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut events: Vec<AgentEvent> = guard
            .events
            .iter()
            .filter(|ev| after.is_none_or(|a| ev.seq > a))
            .cloned()
            .collect();
        if limit > 0 && events.len() > limit as usize {
            events.truncate(limit as usize);
        }
        let next = events.last().map(|e| e.seq);
        ThreadPageView {
            thread: guard.thread.clone(),
            activity: guard.activity.clone(),
            events,
            usage: guard.usage.clone(),
            pending: guard.pending.clone(),
            next,
        }
    }

    fn subscribe_live(self: &Arc<Self>, wave: WaveSlug, role: RoleName) -> FrameStream {
        let (existing, is_closed, mut rx, mut closed_rx) = {
            let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let rx = self.tx.subscribe();
            let closed_rx = self.closed_tx.subscribe();
            let existing: Vec<AgentEvent> = guard.events.iter().cloned().collect();
            (existing, guard.closed, rx, closed_rx)
        };
        let (frame_tx, stream) = frame_channel(128);
        let feed = self.clone();

        tokio::spawn(async move {
            let mut last_sent = 0u64;
            for ev in existing {
                last_sent = ev.seq.0;
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
            if is_closed {
                return;
            }
            loop {
                tokio::select! {
                    biased;
                    () = frame_tx.closed() => break,
                    item = rx.recv() => match item {
                        Ok(ev) => {
                            if ev.seq.0 > last_sent {
                                last_sent = ev.seq.0;
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
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            for ev in feed.events_after(last_sent) {
                                last_sent = ev.seq.0;
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
                    },
                    changed = closed_rx.changed() => {
                        if changed.is_err() || *closed_rx.borrow() {
                            for ev in feed.events_after(last_sent) {
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
                            break;
                        }
                    }
                }
            }
        });
        stream
    }
}

enum SessionCmd {
    Tell {
        body: String,
        mode: SteerDeliveryMode,
        reply: oneshot::Sender<Result<(), HostError>>,
    },
    Resolve {
        interaction: HostInteractionId,
        resolution: InteractionResolution,
        reply: oneshot::Sender<Result<(), HostError>>,
    },
    Retire {
        reply: oneshot::Sender<Result<(), HostError>>,
    },
}

struct SessionEntry {
    wave: WaveSlug,
    role: RoleName,
    attempt: u32,
    pgid: Option<u32>,
    cmd_tx: mpsc::UnboundedSender<SessionCmd>,
    feed: Arc<SessionFeed>,
    alive: AtomicBool,
    archived: AtomicBool,
    retire_lock: tokio::sync::Mutex<()>,
}

struct SessionActor {
    bridge: Arc<AcpBridge>,
    entry: Arc<SessionEntry>,
    thread: HostThreadId,
    session_id: String,
    child: Child,
    stdin: Option<ChildStdin>,
    reader_rx: mpsc::Receiver<ReaderMsg>,
    reader_handle: tokio::task::JoinHandle<()>,
    stderr_tail: Arc<Mutex<StderrTail>>,
    stderr_handle: tokio::task::JoinHandle<()>,
    cmd_rx: mpsc::UnboundedReceiver<SessionCmd>,
    next_rpc_id: u64,
    next_perm_seq: u64,
    active_prompt_id: Option<u64>,
    cancel_requested_for_active: bool,
    queued_prompts: VecDeque<String>,
    parked_permission: Option<ParsedPermissionRequest>,
    tool_names: HashMap<String, String>,
    activity_host_seq: AtomicU64,
    spawned_observed: bool,
    retiring: bool,
}

impl SessionActor {
    fn next_host_seq(&self) -> u64 {
        self.activity_host_seq.fetch_add(1, Ordering::Relaxed)
    }

    async fn ensure_spawned_observed(&mut self) {
        if self.spawned_observed {
            return;
        }
        self.spawned_observed = true;
        self.bridge
            .call_wave_observe(
                self.entry.wave.clone(),
                self.entry.role.clone(),
                Observation::Spawned {
                    attempt: self.entry.attempt,
                    thread: self.thread.clone(),
                },
            )
            .await;
    }

    async fn observe_activity(&mut self, activity: Activity) {
        self.entry.feed.set_activity(activity.clone());
        self.ensure_spawned_observed().await;
        let hseq = self.next_host_seq();
        self.bridge
            .call_wave_observe(
                self.entry.wave.clone(),
                self.entry.role.clone(),
                Observation::activity_with_seq_and_bounds(
                    activity,
                    self.thread.clone(),
                    Some(hseq),
                    None,
                    Vec::new(),
                ),
            )
            .await;
    }

    async fn dispatch_prompt(&mut self, text: String, emit_working: bool) -> Result<(), HostError> {
        let Some(ref mut stdin) = self.stdin else {
            return Err(HostError::Rejected("ACP worker stdin is closed".into()));
        };
        let rpc_id = self.next_rpc_id;
        self.next_rpc_id = self.next_rpc_id.saturating_add(1);
        let req = session_prompt_request(rpc_id, &self.session_id, &text);
        write_json_line(stdin, &req)
            .await
            .map_err(|e| HostError::Unreachable(format!("write ACP session/prompt: {e}")))?;

        self.active_prompt_id = Some(rpc_id);
        self.cancel_requested_for_active = false;
        self.entry
            .feed
            .push_event(AgentEventKind::UserMessage { text });
        self.entry.feed.push_event(AgentEventKind::TurnStarted);
        if emit_working {
            self.observe_activity(Activity::Working).await;
        } else {
            self.entry.feed.set_activity(Activity::Working);
        }
        Ok(())
    }

    async fn send_cancel_active(&mut self) {
        if let Some(parked) = self.parked_permission.take()
            && let Some(ref mut stdin) = self.stdin
        {
            let cancel_resp = permission_cancelled_response(parked.rpc_id);
            let _ =
                tokio::time::timeout(RETIRE_WRITE_TIMEOUT, write_json_line(stdin, &cancel_resp))
                    .await;
        }
        if !self.cancel_requested_for_active
            && let Some(ref mut stdin) = self.stdin
        {
            self.cancel_requested_for_active = true;
            let cancel = session_cancel_notification(&self.session_id);
            let _ =
                tokio::time::timeout(RETIRE_WRITE_TIMEOUT, write_json_line(stdin, &cancel)).await;
        }
    }

    async fn run(mut self, initial_prompt: String) {
        if let Err(err) = self.dispatch_prompt(initial_prompt, false).await {
            self.observe_activity(Activity::Errored {
                error: err.to_string(),
            })
            .await;
            self.cleanup_after_unexpected_exit().await;
            return;
        }

        loop {
            tokio::select! {
                biased;
                cmd = self.cmd_rx.recv() => {
                    let Some(cmd) = cmd else {
                        self.perform_retire(false).await;
                        break;
                    };
                    match cmd {
                        SessionCmd::Tell { body, mode, reply } => {
                            let res = self.handle_tell(body, mode).await;
                            let _ = reply.send(res);
                        }
                        SessionCmd::Resolve { interaction, resolution, reply } => {
                            let res = self.handle_resolve(interaction, resolution).await;
                            let _ = reply.send(res);
                        }
                        SessionCmd::Retire { reply } => {
                            self.perform_retire(true).await;
                            let _ = reply.send(Ok(()));
                            break;
                        }
                    }
                }
                msg = self.reader_rx.recv() => {
                    match msg {
                        Some(ReaderMsg::Line(line)) => {
                            self.handle_inbound_line(&line).await;
                        }
                        Some(ReaderMsg::Eof) | None => {
                            if !self.retiring {
                                self.handle_unexpected_eof().await;
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn handle_tell(
        &mut self,
        body: String,
        mode: SteerDeliveryMode,
    ) -> Result<(), HostError> {
        if !self.entry.alive.load(Ordering::Acquire) {
            return Err(HostError::Rejected(format!(
                "ACP worker {} is no longer running",
                self.thread
            )));
        }
        if self.active_prompt_id.is_none() {
            return self.dispatch_prompt(body, true).await;
        }
        match mode {
            SteerDeliveryMode::Start => Err(HostError::Rejected(format!(
                "ACP worker {} already has an active turn",
                self.thread
            ))),
            SteerDeliveryMode::Steer | SteerDeliveryMode::SteerIfActive => {
                self.send_cancel_active().await;
                self.queued_prompts.push_back(body);
                Ok(())
            }
            SteerDeliveryMode::Auto | SteerDeliveryMode::QueueIfActive => {
                self.queued_prompts.push_back(body);
                Ok(())
            }
        }
    }

    async fn handle_resolve(
        &mut self,
        interaction: HostInteractionId,
        resolution: InteractionResolution,
    ) -> Result<(), HostError> {
        let Some(ref parked) = self.parked_permission else {
            return Err(HostError::Rejected(format!(
                "ACP worker {} has no pending permission interaction",
                self.thread
            )));
        };
        if parked.interaction.id != interaction {
            return Err(HostError::Rejected(format!(
                "ACP worker {} is blocked on {}, not {interaction}",
                self.thread, parked.interaction.id
            )));
        }
        let response = select_permission_response(parked, &resolution)?;
        let Some(ref mut stdin) = self.stdin else {
            return Err(HostError::Rejected("ACP worker stdin is closed".into()));
        };
        write_json_line(stdin, &response)
            .await
            .map_err(|e| HostError::Unreachable(format!("write ACP permission reply: {e}")))?;

        self.parked_permission = None;
        self.observe_activity(Activity::Working).await;
        Ok(())
    }

    async fn handle_inbound_line(&mut self, line: &str) {
        let Some(msg) = parse_inbound_line(line) else {
            return;
        };
        match msg {
            InboundMessage::Notification { method, params } => {
                if method == "session/update"
                    && let Some(kind) = normalize_session_update(&params, &mut self.tool_names)
                {
                    self.entry.feed.push_event(kind);
                }
            }
            InboundMessage::Request { id, method, params } => {
                if method == "session/request_permission" {
                    self.next_perm_seq = self.next_perm_seq.saturating_add(1);
                    let parsed = parse_permission_request(id, &params, self.next_perm_seq);
                    let interaction = parsed.interaction.clone();
                    self.parked_permission = Some(parsed);
                    self.entry.feed.push_event(AgentEventKind::Other {
                        bb_type: format!("permission_request: {}", interaction.summary),
                    });
                    self.observe_activity(Activity::Blocked { interaction })
                        .await;
                } else if let Some(ref mut stdin) = self.stdin {
                    let reply = method_not_found_response(id, &method);
                    let _ = write_json_line(stdin, &reply).await;
                }
            }
            InboundMessage::Response { id, result, error } => {
                if let Some(active_id) = self.active_prompt_id
                    && rpc_id_matches(&id, active_id)
                {
                    self.active_prompt_id = None;
                    self.cancel_requested_for_active = false;
                    self.parked_permission = None;
                    let outcome = classify_prompt_response(result.as_ref(), error.as_ref());
                    match outcome {
                        PromptOutcome::Idle { usage } => {
                            if let Some(u) = usage {
                                self.entry
                                    .feed
                                    .push_event(AgentEventKind::Usage { usage: u });
                            }
                            self.entry.feed.push_event(AgentEventKind::TurnEnded);
                            if let Some(next_prompt) = self.queued_prompts.pop_front() {
                                if let Err(err) = self.dispatch_prompt(next_prompt, true).await {
                                    self.observe_activity(Activity::Errored {
                                        error: err.to_string(),
                                    })
                                    .await;
                                }
                            } else {
                                self.observe_activity(Activity::Idle).await;
                            }
                        }
                        PromptOutcome::Cancelled => {
                            self.entry.feed.push_event(AgentEventKind::TurnEnded);
                            if let Some(next_prompt) = self.queued_prompts.pop_front() {
                                if let Err(err) = self.dispatch_prompt(next_prompt, true).await {
                                    self.observe_activity(Activity::Errored {
                                        error: err.to_string(),
                                    })
                                    .await;
                                }
                            } else {
                                self.observe_activity(Activity::Idle).await;
                            }
                        }
                        PromptOutcome::Refused { reason, usage } => {
                            if let Some(u) = usage {
                                self.entry
                                    .feed
                                    .push_event(AgentEventKind::Usage { usage: u });
                            }
                            self.entry.feed.push_event(AgentEventKind::Other {
                                bb_type: format!("refusal: {reason}"),
                            });
                            self.entry.feed.push_event(AgentEventKind::TurnEnded);
                            self.queued_prompts.clear();
                            self.observe_activity(Activity::Refused { reason }).await;
                        }
                        PromptOutcome::Errored {
                            error: err_msg,
                            usage,
                        } => {
                            if let Some(u) = usage {
                                self.entry
                                    .feed
                                    .push_event(AgentEventKind::Usage { usage: u });
                            }
                            self.entry.feed.push_event(AgentEventKind::Other {
                                bb_type: format!("error: {err_msg}"),
                            });
                            self.entry.feed.push_event(AgentEventKind::TurnEnded);
                            self.queued_prompts.clear();
                            self.observe_activity(Activity::Errored { error: err_msg })
                                .await;
                        }
                    }
                }
            }
        }
    }

    async fn handle_unexpected_eof(&mut self) {
        let status = tokio::time::timeout(Duration::from_millis(500), self.child.wait())
            .await
            .ok()
            .and_then(Result::ok);
        let tail = self
            .stderr_tail
            .lock()
            .map(|g| g.text())
            .unwrap_or_default();
        self.entry.alive.store(false, Ordering::Release);
        self.stdin.take();
        let activity = classify_process_exit(status, &tail);
        if self.active_prompt_id.take().is_some() {
            self.entry.feed.push_event(AgentEventKind::TurnEnded);
        }
        self.observe_activity(activity).await;
        self.entry.feed.mark_closed();
        self.reader_handle.abort();
        self.stderr_handle.abort();
    }

    async fn cleanup_after_unexpected_exit(&mut self) {
        self.entry.alive.store(false, Ordering::Release);
        self.stdin.take();
        terminate_and_reap_process_group(
            &mut self.child,
            self.entry.pgid,
            self.bridge.cfg.retire_grace,
        )
        .await;
        self.entry.feed.mark_closed();
        self.reader_handle.abort();
        self.stderr_handle.abort();
    }

    async fn perform_retire(&mut self, emit_archived: bool) {
        self.retiring = true;
        self.bridge.unlink_worker_token(&self.thread);
        self.send_cancel_active().await;
        self.stdin.take();
        terminate_and_reap_process_group(
            &mut self.child,
            self.entry.pgid,
            self.bridge.cfg.retire_grace,
        )
        .await;
        self.entry.alive.store(false, Ordering::Release);
        self.entry.archived.store(true, Ordering::Release);
        self.entry.feed.mark_closed();
        self.reader_handle.abort();
        self.stderr_handle.abort();

        if emit_archived {
            self.bridge
                .call_wave_observe(
                    self.entry.wave.clone(),
                    self.entry.role.clone(),
                    Observation::archived_for(self.thread.clone(), Some(self.entry.attempt)),
                )
                .await;
        }
    }
}

/// Core native ACP bridge owning spawned child process groups, JSON-RPC 2.0
/// sessions, and per-worker live event feeds.
pub struct AcpBridge {
    cfg: AcpConfig,
    backend: OnceLock<Arc<dyn ErasedBackend>>,
    sessions: RwLock<HashMap<HostThreadId, Arc<SessionEntry>>>,
    sessions_by_role: RwLock<HashMap<(WaveSlug, RoleName), HostThreadId>>,
    project_names_tx: watch::Sender<Arc<BTreeMap<ProjectId, String>>>,
    project_names_rx: watch::Receiver<Arc<BTreeMap<ProjectId, String>>>,
}

impl Drop for AcpBridge {
    fn drop(&mut self) {
        if let Ok(guard) = self.sessions.read() {
            for entry in guard.values() {
                if entry.alive.load(Ordering::Acquire)
                    && let Some(pgid) = entry.pgid
                {
                    kill_process_group_now(pgid);
                }
            }
        }
    }
}

impl AcpBridge {
    pub fn new(cfg: AcpConfig) -> Arc<Self> {
        let (project_names_tx, project_names_rx) = watch::channel(Arc::new(BTreeMap::new()));
        Arc::new(Self {
            cfg,
            backend: OnceLock::new(),
            sessions: RwLock::new(HashMap::new()),
            sessions_by_role: RwLock::new(HashMap::new()),
            project_names_tx,
            project_names_rx,
        })
    }

    pub fn config(&self) -> &AcpConfig {
        &self.cfg
    }

    pub fn attach_backend<B: BridgeBackend>(&self, backend: B) {
        let _ = self.backend.set(Arc::new(backend));
    }

    pub async fn run(self: Arc<Self>, router: Arc<Router>) {
        let (_tx, rx) = watch::channel(false);
        self.run_until(router, rx).await;
    }

    pub async fn run_until<B: BridgeBackend>(
        self: Arc<Self>,
        backend: B,
        mut shutdown: watch::Receiver<bool>,
    ) {
        self.attach_backend(backend);
        while !*shutdown.borrow() {
            if shutdown.changed().await.is_err() {
                break;
            }
        }
        self.shutdown_all().await;
    }

    /// Gracefully retires and reaps every still-active ACP worker process group.
    pub async fn shutdown_all(&self) {
        let active: Vec<HostThreadId> = {
            let guard = self.sessions.read().unwrap_or_else(|e| e.into_inner());
            guard
                .iter()
                .filter(|(_, entry)| entry.alive.load(Ordering::Acquire))
                .map(|(tid, _)| tid.clone())
                .collect()
        };
        for tid in active {
            let _ = self.retire_worker_thread(&tid).await;
        }
    }

    fn upsert_project_name(&self, project: &ProjectId, title: &str) {
        let trimmed = title.trim();
        if trimmed.is_empty() {
            return;
        }
        self.project_names_tx.send_if_modified(|cur| {
            if cur.get(project).map(String::as_str) == Some(trimmed) {
                return false;
            }
            let mut next = (**cur).clone();
            next.insert(project.clone(), trimmed.to_owned());
            *cur = Arc::new(next);
            true
        });
    }

    fn unlink_worker_token(&self, thread: &HostThreadId) {
        if let Some(ref state_dir) = self.cfg.state_dir {
            let _ = remove_worker_token_file(state_dir, thread.as_str());
        }
    }

    async fn call_wave_observe(&self, wave: WaveSlug, role: RoleName, observation: Observation) {
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

    async fn spawn_worker_thread(
        self: &Arc<Self>,
        req: SpawnRequest,
    ) -> Result<HostThreadId, HostError> {
        let thread = deterministic_thread_id(&req.wave, &req.role, req.attempt)?;

        if let Some(existing) = self.get_session(&thread)
            && existing.alive.load(Ordering::Acquire)
        {
            return Ok(thread);
        }

        let (program, args, extra_env) = self
            .cfg
            .resolve_provider(req.agent.provider.as_str(), req.agent.permission)?;
        let (cwd, project_title) = resolve_acp_workspace(
            &req.project,
            &req.agent.workspace,
            &req.wave,
            &req.role,
            self.cfg.bb_data_dir.as_deref(),
        )
        .await?;
        self.upsert_project_name(&req.project, &project_title);

        let provisioned = if let Some(ref state_dir) = self.cfg.state_dir {
            Some(
                provision_worker_token_in_state_dir(state_dir, &req.wave, &req.role, &thread)
                    .map_err(|e| HostError::Unreachable(format!("provision worker token: {e}")))?,
            )
        } else {
            None
        };

        let mut cmd = Command::new(&program);
        cmd.args(&args)
            .current_dir(&cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .env_remove(T3_WORKER_THREAD_ENV)
            .env(WORKER_THREAD_ENV, thread.as_str());

        if let Some(ref sock) = self.cfg.socket_path {
            cmd.env("REX_WAVE_SOCKET", sock);
        }
        if let Some(ref prov) = provisioned {
            cmd.env("REX_WAVE_TOKEN", &prov.token);
        }
        for (k, v) in &extra_env {
            cmd.env(k, v);
        }

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                if let Some(ref prov) = provisioned
                    && prov.created
                {
                    self.unlink_worker_token(&thread);
                }
                return Err(HostError::Rejected(format!(
                    "failed to spawn ACP provider '{}': {e}",
                    program.display()
                )));
            }
        };

        let pgid = child.id();
        let mut stdin = child.stdin.take().ok_or_else(|| {
            if let Some(g) = pgid {
                kill_process_group_now(g);
            }
            HostError::Rejected("ACP child missing piped stdin".into())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            if let Some(g) = pgid {
                kill_process_group_now(g);
            }
            HostError::Rejected("ACP child missing piped stdout".into())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            if let Some(g) = pgid {
                kill_process_group_now(g);
            }
            HostError::Rejected("ACP child missing piped stderr".into())
        })?;

        let (mut reader_rx, reader_handle) = spawn_stdout_reader(stdout);
        let (stderr_tail, stderr_handle) = spawn_stderr_drainer(stderr);

        let handshake_res: Result<(String, Option<ModelId>, u64), HostError> = async {
            let mut next_id = 1u64;

            let init_id = next_id;
            next_id += 1;
            write_json_line(&mut stdin, &initialize_request(init_id))
                .await
                .map_err(|e| HostError::Unreachable(format!("write ACP initialize: {e}")))?;
            match wait_for_rpc_response(
                &mut reader_rx,
                init_id,
                self.cfg.handshake_timeout,
                &stderr_tail,
            )
            .await?
            {
                Ok(_) => {}
                Err(err) => {
                    return Err(HostError::Rejected(format!(
                        "ACP initialize rejected: {}",
                        format_jsonrpc_error(&err)
                    )));
                }
            }

            let mut mcp_servers = Vec::new();
            if let (Some(sock), Some(prov)) = (&self.cfg.socket_path, &provisioned) {
                mcp_servers.push(mcp_server_entry(
                    &self.cfg.rex_wave_bin,
                    sock,
                    &prov.token,
                    &thread,
                ));
            }

            let new_id = next_id;
            next_id += 1;
            write_json_line(&mut stdin, &session_new_request(new_id, &cwd, mcp_servers))
                .await
                .map_err(|e| HostError::Unreachable(format!("write ACP session/new: {e}")))?;
            let new_reply = match wait_for_rpc_response(
                &mut reader_rx,
                new_id,
                self.cfg.handshake_timeout,
                &stderr_tail,
            )
            .await?
            {
                Ok(val) => val,
                Err(err) => {
                    return Err(HostError::Rejected(format!(
                        "ACP session/new rejected: {}",
                        format_jsonrpc_error(&err)
                    )));
                }
            };

            let session_id = extract_session_id(&new_reply)?;
            let mut actual_model = extract_session_model(&new_reply);

            if let Some(ref requested_model) = req.agent.model
                && session_supports_models(&new_reply)
            {
                let set_id = next_id;
                next_id += 1;
                write_json_line(
                    &mut stdin,
                    &session_set_model_request(set_id, &session_id, requested_model.as_str()),
                )
                .await
                .map_err(|e| HostError::Unreachable(format!("write ACP session/set_model: {e}")))?;
                match wait_for_rpc_response(
                    &mut reader_rx,
                    set_id,
                    self.cfg.handshake_timeout,
                    &stderr_tail,
                )
                .await
                {
                    Ok(Ok(val)) => {
                        actual_model =
                            extract_session_model(&val).or_else(|| Some(requested_model.clone()));
                    }
                    Ok(Err(err)) if jsonrpc_error_code(&err) == Some(-32601) => {}
                    Ok(Err(err)) => {
                        return Err(HostError::Rejected(format!(
                            "ACP session/set_model rejected: {}",
                            format_jsonrpc_error(&err)
                        )));
                    }
                    Err(e) => return Err(e),
                }
            }

            Ok((session_id, actual_model, next_id))
        }
        .await;

        let (session_id, actual_model, next_rpc_id) = match handshake_res {
            Ok(tuple) => tuple,
            Err(err) => {
                drop(stdin);
                terminate_and_reap_process_group(&mut child, pgid, self.cfg.retire_grace).await;
                reader_handle.abort();
                stderr_handle.abort();
                if let Some(ref prov) = provisioned
                    && prov.created
                {
                    self.unlink_worker_token(&thread);
                }
                return Err(err);
            }
        };

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let feed = Arc::new(SessionFeed::new(thread.clone()));
        let entry = Arc::new(SessionEntry {
            wave: req.wave.clone(),
            role: req.role.clone(),
            attempt: req.attempt,
            pgid,
            cmd_tx,
            feed,
            alive: AtomicBool::new(true),
            archived: AtomicBool::new(false),
            retire_lock: tokio::sync::Mutex::new(()),
        });

        {
            let mut by_id = self.sessions.write().unwrap_or_else(|e| e.into_inner());
            by_id.insert(thread.clone(), entry.clone());
        }
        {
            let mut by_role = self
                .sessions_by_role
                .write()
                .unwrap_or_else(|e| e.into_inner());
            by_role.insert((req.wave.clone(), req.role.clone()), thread.clone());
        }

        if let Some(model) = actual_model {
            self.call_wave_observe(
                req.wave.clone(),
                req.role.clone(),
                Observation::runtime_meta(
                    Some(model),
                    None,
                    Some(thread.clone()),
                    Some(req.attempt),
                ),
            )
            .await;
        }

        let actor = SessionActor {
            bridge: self.clone(),
            entry,
            thread: thread.clone(),
            session_id,
            child,
            stdin: Some(stdin),
            reader_rx,
            reader_handle,
            stderr_tail,
            stderr_handle,
            cmd_rx,
            next_rpc_id,
            next_perm_seq: 0,
            active_prompt_id: None,
            cancel_requested_for_active: false,
            queued_prompts: VecDeque::new(),
            parked_permission: None,
            tool_names: HashMap::new(),
            activity_host_seq: AtomicU64::new(1),
            spawned_observed: false,
            retiring: false,
        };
        let prompt = req.prompt.as_str().to_owned();
        tokio::spawn(actor.run(prompt));

        Ok(thread)
    }

    fn get_session(&self, thread: &HostThreadId) -> Option<Arc<SessionEntry>> {
        self.sessions
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(thread)
            .cloned()
    }

    fn get_session_by_role(&self, wave: &WaveSlug, role: &RoleName) -> Option<Arc<SessionEntry>> {
        let tid = self
            .sessions_by_role
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(wave.clone(), role.clone()))
            .cloned()?;
        self.get_session(&tid)
    }

    async fn tell_worker_thread(
        &self,
        thread: &HostThreadId,
        body: &Body,
        mode: SteerDeliveryMode,
    ) -> Result<(), HostError> {
        let entry = self
            .get_session(thread)
            .ok_or_else(|| HostError::Rejected(format!("unknown ACP worker thread {thread}")))?;
        let (tx, rx) = oneshot::channel();
        entry
            .cmd_tx
            .send(SessionCmd::Tell {
                body: body.as_str().to_owned(),
                mode,
                reply: tx,
            })
            .map_err(|_| HostError::Rejected(format!("ACP worker {thread} already exited")))?;
        rx.await
            .map_err(|_| HostError::Rejected(format!("ACP worker {thread} actor stopped")))?
    }

    async fn resolve_worker_interaction(
        &self,
        thread: &HostThreadId,
        interaction: &HostInteractionId,
        resolution: &InteractionResolution,
    ) -> Result<(), HostError> {
        let entry = self
            .get_session(thread)
            .ok_or_else(|| HostError::Rejected(format!("unknown ACP worker thread {thread}")))?;
        let (tx, rx) = oneshot::channel();
        entry
            .cmd_tx
            .send(SessionCmd::Resolve {
                interaction: interaction.clone(),
                resolution: resolution.clone(),
                reply: tx,
            })
            .map_err(|_| HostError::Rejected(format!("ACP worker {thread} already exited")))?;
        rx.await
            .map_err(|_| HostError::Rejected(format!("ACP worker {thread} actor stopped")))?
    }

    async fn retire_worker_thread(&self, thread: &HostThreadId) -> Result<(), HostError> {
        if !is_acp_thread_id(thread) {
            return Err(HostError::Rejected(format!(
                "thread {thread} is not an ACP thread"
            )));
        }
        self.unlink_worker_token(thread);

        let Some(entry) = self.get_session(thread) else {
            if let Some((wave, role, attempt)) = parse_acp_thread_id(thread) {
                self.call_wave_observe(
                    wave,
                    role,
                    Observation::archived_for(thread.clone(), Some(attempt)),
                )
                .await;
            }
            return Ok(());
        };

        let _retire_guard = entry.retire_lock.lock().await;
        if entry.archived.load(Ordering::Acquire) {
            return Ok(());
        }

        let (tx, rx) = oneshot::channel();
        if entry.cmd_tx.send(SessionCmd::Retire { reply: tx }).is_ok() {
            let _ = rx.await;
            return Ok(());
        }

        // Actor already exited unexpectedly; ensure group is killed and Archived emitted.
        if let Some(pgid) = entry.pgid {
            kill_process_group_now(pgid);
            let deadline = std::time::Instant::now() + Duration::from_millis(300);
            while process_group_alive(pgid) && std::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
        entry.alive.store(false, Ordering::Release);
        entry.archived.store(true, Ordering::Release);
        entry.feed.mark_closed();
        self.call_wave_observe(
            entry.wave.clone(),
            entry.role.clone(),
            Observation::archived_for(thread.clone(), Some(entry.attempt)),
        )
        .await;
        Ok(())
    }

    async fn check_thread_live(
        &self,
        thread: &HostThreadId,
    ) -> Result<Option<Observation>, HostError> {
        if !is_acp_thread_id(thread) {
            return Ok(None);
        }
        let Some(entry) = self.get_session(thread) else {
            return Ok(Some(Observation::activity_with_bounds(
                Activity::Errored {
                    error: "ACP worker process lost after daemon restart".into(),
                },
                thread.clone(),
                None,
                Vec::new(),
            )));
        };
        if entry.archived.load(Ordering::Acquire) {
            return Ok(Some(Observation::archived_for(
                thread.clone(),
                Some(entry.attempt),
            )));
        }
        Ok(None)
    }
}

/// `rex_wave_waves::AgentHost` adapter backed by [`AcpBridge`].
#[derive(Clone)]
pub struct AcpHost(pub Arc<AcpBridge>);

impl AgentHost for AcpHost {
    fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<HostThreadId, HostError>> {
        let bridge = self.0.clone();
        Box::pin(async move { bridge.spawn_worker_thread(req).await })
    }

    fn tell(
        &self,
        thread: &HostThreadId,
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
        thread: &HostThreadId,
        interaction: &HostInteractionId,
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

    fn retire(&self, thread: &HostThreadId) -> BoxFut<'_, Result<(), HostError>> {
        let bridge = self.0.clone();
        let thread = thread.clone();
        Box::pin(async move { bridge.retire_worker_thread(&thread).await })
    }

    fn check_live(
        &self,
        thread: &HostThreadId,
    ) -> BoxFut<'_, Result<Option<Observation>, HostError>> {
        let bridge = self.0.clone();
        let thread = thread.clone();
        Box::pin(async move { bridge.check_thread_live(&thread).await })
    }
}

/// `rex_wave_runtime::AgentFeed` adapter backed by [`AcpBridge`].
#[derive(Clone)]
pub struct AcpFeed(pub Arc<AcpBridge>);

impl AgentFeed for AcpBridge {
    fn page(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        after: Option<HostSeq>,
        limit: u32,
    ) -> BoxFut<'_, Result<ThreadPageView, ApiError>> {
        let wave = wave.clone();
        let role = role.clone();
        Box::pin(async move {
            let entry =
                self.get_session_by_role(&wave, &role)
                    .ok_or_else(|| ApiError::NotFound {
                        what: format!("ACP worker @{wave}/{role}"),
                    })?;
            Ok(entry.feed.page(after, limit))
        })
    }

    fn live(&self, wave: &WaveSlug, role: &RoleName) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        let wave = wave.clone();
        let role = role.clone();
        Box::pin(async move {
            let entry =
                self.get_session_by_role(&wave, &role)
                    .ok_or_else(|| ApiError::NotFound {
                        what: format!("ACP worker @{wave}/{role}"),
                    })?;
            Ok(entry.feed.subscribe_live(wave, role))
        })
    }

    fn project_names(&self) -> Option<watch::Receiver<Arc<BTreeMap<ProjectId, String>>>> {
        Some(self.project_names_rx.clone())
    }
}

impl AgentFeed for AcpFeed {
    fn page(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        after: Option<HostSeq>,
        limit: u32,
    ) -> BoxFut<'_, Result<ThreadPageView, ApiError>> {
        self.0.page(wave, role, after, limit)
    }

    fn live(&self, wave: &WaveSlug, role: &RoleName) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        self.0.live(wave, role)
    }

    fn project_names(&self) -> Option<watch::Receiver<Arc<BTreeMap<ProjectId, String>>>> {
        self.0.project_names()
    }
}
