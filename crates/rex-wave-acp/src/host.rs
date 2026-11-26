//! `AcpBridge`, `AcpHost` (`AgentHost`), and `AcpFeed` (`AgentFeed`) over
//! child-process NDJSON JSON-RPC 2.0 stdio transport.

use crate::config::AcpConfig;
use crate::process::{
    kill_process_group_now, process_group_alive, terminate_and_reap_process_group,
};
use crate::protocol::{
    InboundMessage, ParsedPermissionRequest, PromptOutcome, classify_process_exit,
    classify_prompt_response, deterministic_thread_id, extract_model_config_option,
    extract_session_id, extract_session_model, format_jsonrpc_error, initialize_request,
    is_acp_thread_id, is_cancellation_jsonrpc_error, mcp_server_entry, method_not_found_response,
    model_is_listed_in_models_obj, normalize_session_update, parse_acp_thread_id,
    parse_inbound_line, parse_permission_request, permission_cancelled_response, rpc_id_matches,
    select_permission_response, session_cancel_notification, session_new_request,
    session_prompt_request, session_set_config_option_request, session_set_model_request,
    session_supports_models, validate_initialize_result, validate_selected_config_model,
    validate_selected_models_obj,
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
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

const MAX_FEED_EVENTS: usize = 16_384;
const MAX_ARCHIVED_FEED_EVENTS: usize = 256;
const MAX_ARCHIVED_SESSIONS: usize = 256;
const MAX_LINE_BYTES: usize = 8 * 1024 * 1024;
const MAX_STDERR_BYTES: usize = 8 * 1024;
const READER_CHANNEL_CAP: usize = 512;
const FEED_BROADCAST_CAP: usize = 256;
const RETIRE_WRITE_TIMEOUT: Duration = Duration::from_millis(250);
const PROMPT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Daemon-scoped credentials and harness ambient variables scrubbed from worker children.
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
    "CLAUDE_PID",
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_COMMON_DIR",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_PREFIX",
    "GIT_CEILING_DIRECTORIES",
];

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
    OversizedLine,
    Eof,
}

async fn drain_until_newline_bounded<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R) {
    loop {
        let Ok(buf) = reader.fill_buf().await else {
            break;
        };
        if buf.is_empty() {
            break;
        }
        if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            reader.consume(pos + 1);
            break;
        }
        let len = buf.len();
        reader.consume(len);
    }
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
                        drain_until_newline_bounded(&mut reader).await;
                        if tx.send(ReaderMsg::OversizedLine).await.is_err() {
                            break;
                        }
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

async fn write_json_line_bounded(
    stdin: &mut ChildStdin,
    payload: &Value,
    timeout: Duration,
) -> io::Result<()> {
    tokio::time::timeout(timeout, write_json_line(stdin, payload))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "ACP stdin write timed out"))?
}

async fn wait_for_rpc_response(
    stdin: &mut ChildStdin,
    reader_rx: &mut mpsc::Receiver<ReaderMsg>,
    expected_id: u64,
    timeout: Duration,
    stderr_tail: &Arc<Mutex<StderrTail>>,
) -> Result<Result<Value, Value>, HostError> {
    let fut = async {
        while let Some(msg) = reader_rx.recv().await {
            match msg {
                ReaderMsg::Line(line) => match parse_inbound_line(&line) {
                    Some(InboundMessage::Response { id, result, error })
                        if rpc_id_matches(&id, expected_id) =>
                    {
                        if let Some(err) = error {
                            return Ok(Err(err));
                        }
                        let Some(res) = result else {
                            return Err(HostError::Rejected(format!(
                                "malformed ACP JSON-RPC response for request {expected_id}: missing result and error"
                            )));
                        };
                        return Ok(Ok(res));
                    }
                    Some(InboundMessage::Request { id, method, .. }) => {
                        let reply = method_not_found_response(id, &method);
                        let _ = write_json_line_bounded(stdin, &reply, RETIRE_WRITE_TIMEOUT).await;
                    }
                    _ => {}
                },
                ReaderMsg::OversizedLine => {
                    return Err(HostError::Rejected(format!(
                        "ACP child emitted frame exceeding {MAX_LINE_BYTES} byte limit during request {expected_id}"
                    )));
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
        let live_ev = AgentEvent { seq, at, kind };
        if guard.events.len() >= MAX_FEED_EVENTS {
            guard.events.pop_front();
        }
        guard.events.push_back(live_ev.clone());
        let _ = self.tx.send(live_ev.clone());
        live_ev
    }

    fn current_activity(&self) -> Activity {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .activity
            .clone()
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
        while guard.events.len() > MAX_ARCHIVED_FEED_EVENTS {
            guard.events.pop_front();
        }
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

type PgidSlot = Arc<Mutex<Option<u32>>>;

fn take_pgid_slot(slot: &PgidSlot) -> Option<u32> {
    slot.lock().unwrap_or_else(|e| e.into_inner()).take()
}

struct KillPgidOnDrop {
    pgid: Option<u32>,
}

impl Drop for KillPgidOnDrop {
    fn drop(&mut self) {
        if let Some(g) = self.pgid.take() {
            kill_process_group_now(g);
        }
    }
}

struct SessionEntry {
    wave: WaveSlug,
    role: RoleName,
    attempt: u32,
    pgid_slot: PgidSlot,
    parked_snapshot: Mutex<Option<ParsedPermissionRequest>>,
    cmd_tx: mpsc::UnboundedSender<SessionCmd>,
    feed: Arc<SessionFeed>,
    alive: AtomicBool,
    archived: AtomicBool,
    retire_lock: tokio::sync::Mutex<()>,
}

impl SessionEntry {
    fn take_pgid(&self) -> Option<u32> {
        take_pgid_slot(&self.pgid_slot)
    }

    fn set_parked_snapshot(&self, parked: Option<ParsedPermissionRequest>) {
        *self
            .parked_snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = parked;
    }

    fn get_parked_snapshot(&self) -> Option<ParsedPermissionRequest> {
        self.parked_snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[derive(Default)]
struct SessionRegistry {
    by_id: HashMap<HostThreadId, Arc<SessionEntry>>,
    order: VecDeque<HostThreadId>,
    by_role: HashMap<(WaveSlug, RoleName), HostThreadId>,
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
        write_json_line_bounded(stdin, &req, PROMPT_WRITE_TIMEOUT)
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

    async fn send_cancel_active(&mut self, observe_unblocked: bool) {
        let mut had_parked = false;
        if let Some(parked) = self.parked_permission.take() {
            self.entry.set_parked_snapshot(None);
            had_parked = true;
            if let Some(ref mut stdin) = self.stdin {
                let cancel_resp = permission_cancelled_response(parked.rpc_id);
                let _ = write_json_line_bounded(stdin, &cancel_resp, RETIRE_WRITE_TIMEOUT).await;
            }
        }
        if !self.cancel_requested_for_active
            && let Some(ref mut stdin) = self.stdin
        {
            self.cancel_requested_for_active = true;
            let cancel = session_cancel_notification(&self.session_id);
            let _ = write_json_line_bounded(stdin, &cancel, RETIRE_WRITE_TIMEOUT).await;
        }
        if had_parked && observe_unblocked {
            self.observe_activity(Activity::Working).await;
        }
    }

    fn drain_queued_prompts_as_cancelled(&mut self, why: &str) {
        while let Some(queued) = self.queued_prompts.pop_front() {
            let preview = truncate_with_notice(queued.trim(), 160);
            self.entry.feed.push_event(AgentEventKind::Other {
                bb_type: format!("cancelled_queued_prompt ({why}): {preview}"),
            });
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
                        Some(ReaderMsg::OversizedLine) => {
                            self.handle_oversized_frame().await;
                            break;
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

    async fn handle_oversized_frame(&mut self) {
        let msg = format!("ACP child emitted frame exceeding {MAX_LINE_BYTES} byte limit");
        if self.active_prompt_id.take().is_some() {
            self.entry.feed.push_event(AgentEventKind::Other {
                bb_type: format!("error: {msg}"),
            });
            self.entry.feed.push_event(AgentEventKind::TurnEnded);
        }
        self.drain_queued_prompts_as_cancelled("oversized_frame");
        self.observe_activity(Activity::Errored { error: msg })
            .await;
        self.cleanup_after_unexpected_exit().await;
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
            if let Err(err) = self.dispatch_prompt(body, true).await {
                self.observe_activity(Activity::Errored {
                    error: err.to_string(),
                })
                .await;
                return Err(err);
            }
            return Ok(());
        }
        match mode {
            SteerDeliveryMode::Start => Err(HostError::Rejected(format!(
                "ACP worker {} already has an active turn",
                self.thread
            ))),
            SteerDeliveryMode::Steer | SteerDeliveryMode::SteerIfActive => {
                self.send_cancel_active(true).await;
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
        let Some(parked_clone) = self.parked_permission.clone() else {
            return Err(HostError::Rejected(format!(
                "ACP worker {} has no pending permission interaction",
                self.thread
            )));
        };
        if parked_clone.interaction.id != interaction {
            return Err(HostError::Rejected(format!(
                "ACP worker {} is blocked on {}, not {interaction}",
                self.thread, parked_clone.interaction.id
            )));
        }
        let response = match select_permission_response(&parked_clone, &resolution) {
            Ok(resp) => resp,
            Err(err) => {
                self.entry.feed.push_event(AgentEventKind::Other {
                    bb_type: format!("permission_resolution_rejected: {err}"),
                });
                self.observe_activity(Activity::Blocked {
                    interaction: parked_clone.interaction,
                })
                .await;
                return Err(err);
            }
        };
        let Some(ref mut stdin) = self.stdin else {
            return Err(HostError::Rejected("ACP worker stdin is closed".into()));
        };
        write_json_line_bounded(stdin, &response, PROMPT_WRITE_TIMEOUT)
            .await
            .map_err(|e| HostError::Unreachable(format!("write ACP permission reply: {e}")))?;

        self.parked_permission = None;
        self.entry.set_parked_snapshot(None);
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
                    if matches!(kind, AgentEventKind::UserMessage { .. })
                        && self.active_prompt_id.is_some()
                    {
                        return;
                    }
                    self.entry.feed.push_event(kind);
                }
            }
            InboundMessage::Request { id, method, params } => {
                if method == "session/request_permission" {
                    if self.cancel_requested_for_active || self.active_prompt_id.is_none() {
                        if let Some(ref mut stdin) = self.stdin {
                            let cancel_resp = permission_cancelled_response(id);
                            let _ =
                                write_json_line_bounded(stdin, &cancel_resp, RETIRE_WRITE_TIMEOUT)
                                    .await;
                        }
                    } else {
                        if let Some(prev) = self.parked_permission.take()
                            && let Some(ref mut stdin) = self.stdin
                        {
                            let cancel_resp = permission_cancelled_response(prev.rpc_id);
                            let _ =
                                write_json_line_bounded(stdin, &cancel_resp, RETIRE_WRITE_TIMEOUT)
                                    .await;
                        }
                        self.next_perm_seq = self.next_perm_seq.saturating_add(1);
                        let parsed = parse_permission_request(id, &params, self.next_perm_seq);
                        let interaction = parsed.interaction.clone();
                        self.entry.set_parked_snapshot(Some(parsed.clone()));
                        self.parked_permission = Some(parsed);
                        self.entry.feed.push_event(AgentEventKind::Other {
                            bb_type: format!("permission_request: {}", interaction.summary),
                        });
                        self.observe_activity(Activity::Blocked { interaction })
                            .await;
                    }
                } else if let Some(ref mut stdin) = self.stdin {
                    let reply = method_not_found_response(id, &method);
                    let _ = write_json_line_bounded(stdin, &reply, RETIRE_WRITE_TIMEOUT).await;
                }
            }
            InboundMessage::Response { id, result, error } => {
                if let Some(active_id) = self.active_prompt_id
                    && rpc_id_matches(&id, active_id)
                {
                    self.active_prompt_id = None;
                    let was_cancelled =
                        std::mem::replace(&mut self.cancel_requested_for_active, false);
                    self.parked_permission = None;
                    self.entry.set_parked_snapshot(None);
                    let raw_outcome = classify_prompt_response(result.as_ref(), error.as_ref());
                    let outcome = if !was_cancelled {
                        raw_outcome
                    } else {
                        match raw_outcome {
                            PromptOutcome::Refused { .. } => raw_outcome,
                            PromptOutcome::Idle { .. } | PromptOutcome::Cancelled => {
                                PromptOutcome::Cancelled
                            }
                            PromptOutcome::Errored {
                                error: err_msg,
                                usage,
                            } => {
                                if is_cancellation_jsonrpc_error(error.as_ref(), &err_msg) {
                                    PromptOutcome::Cancelled
                                } else {
                                    PromptOutcome::Errored {
                                        error: err_msg,
                                        usage,
                                    }
                                }
                            }
                        }
                    };
                    match outcome {
                        PromptOutcome::Idle { usage, stop_reason } => {
                            if let Some(u) = usage {
                                self.entry
                                    .feed
                                    .push_event(AgentEventKind::Usage { usage: u });
                            }
                            let low_sr = stop_reason.to_ascii_lowercase();
                            if matches!(low_sr.as_str(), "max_tokens" | "max_turn_requests") {
                                self.entry.feed.push_event(AgentEventKind::Other {
                                    bb_type: format!("stop_reason: {stop_reason}"),
                                });
                            }
                            self.entry.feed.push_event(AgentEventKind::TurnEnded);
                            if let Some(next_prompt) = self.queued_prompts.pop_front() {
                                if let Err(err) = self.dispatch_prompt(next_prompt, true).await {
                                    self.drain_queued_prompts_as_cancelled("dispatch_failed");
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
                                    self.drain_queued_prompts_as_cancelled("dispatch_failed");
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
                            self.drain_queued_prompts_as_cancelled("turn_refused");
                            self.entry.feed.push_event(AgentEventKind::TurnEnded);
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
                            self.drain_queued_prompts_as_cancelled("turn_errored");
                            self.entry.feed.push_event(AgentEventKind::TurnEnded);
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
        self.parked_permission = None;
        self.entry.set_parked_snapshot(None);
        self.entry.alive.store(false, Ordering::Release);
        self.stdin.take();
        self.bridge.unlink_worker_token(&self.thread);
        let pgid = self.bridge.claim_pgid(&self.thread, Some(&self.entry));
        terminate_and_reap_process_group(&mut self.child, pgid, self.bridge.cfg.retire_grace).await;
        let activity = classify_process_exit(status, &tail);
        if self.active_prompt_id.take().is_some() {
            self.entry.feed.push_event(AgentEventKind::TurnEnded);
        }
        self.drain_queued_prompts_as_cancelled("unexpected_eof");
        self.observe_activity(activity).await;
        self.entry.feed.mark_closed();
        self.reader_handle.abort();
        self.stderr_handle.abort();
    }

    async fn cleanup_after_unexpected_exit(&mut self) {
        self.parked_permission = None;
        self.entry.set_parked_snapshot(None);
        self.entry.alive.store(false, Ordering::Release);
        self.stdin.take();
        self.bridge.unlink_worker_token(&self.thread);
        let pgid = self.bridge.claim_pgid(&self.thread, Some(&self.entry));
        terminate_and_reap_process_group(&mut self.child, pgid, self.bridge.cfg.retire_grace).await;
        self.entry.feed.mark_closed();
        self.reader_handle.abort();
        self.stderr_handle.abort();
    }

    async fn perform_retire(&mut self, emit_archived: bool) {
        self.retiring = true;
        self.bridge.unlink_worker_token(&self.thread);
        self.send_cancel_active(false).await;
        self.stdin.take();
        let pgid = self.bridge.claim_pgid(&self.thread, Some(&self.entry));
        terminate_and_reap_process_group(&mut self.child, pgid, self.bridge.cfg.retire_grace).await;
        self.entry.alive.store(false, Ordering::Release);
        let first_archive = !self.entry.archived.swap(true, Ordering::AcqRel);
        self.entry.feed.mark_closed();
        self.reader_handle.abort();
        self.stderr_handle.abort();
        self.bridge.evict_excess_archived_sessions();

        if emit_archived && first_archive {
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

struct HandshakeCleanupGuard<'a> {
    bridge: &'a AcpBridge,
    thread: HostThreadId,
    pgid_slot: PgidSlot,
    armed: bool,
}

impl Drop for HandshakeCleanupGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.bridge.unregister_active_process(&self.thread);
            if let Some(g) = take_pgid_slot(&self.pgid_slot) {
                kill_process_group_now(g);
            }
            self.bridge.unlink_worker_token(&self.thread);
        }
    }
}

/// Core native ACP bridge owning spawned child process groups, JSON-RPC 2.0
/// sessions, and per-worker live event feeds.
pub struct AcpBridge {
    cfg: AcpConfig,
    backend: OnceLock<Arc<dyn ErasedBackend>>,
    registry: Mutex<SessionRegistry>,
    spawn_locks: Mutex<HashMap<HostThreadId, Arc<tokio::sync::Mutex<()>>>>,
    active_processes: Mutex<HashMap<HostThreadId, PgidSlot>>,
    project_names_tx: watch::Sender<Arc<BTreeMap<ProjectId, String>>>,
    project_names_rx: watch::Receiver<Arc<BTreeMap<ProjectId, String>>>,
}

impl Drop for AcpBridge {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active_processes.lock() {
            for (tid, slot) in active.drain() {
                if let Some(g) = take_pgid_slot(&slot) {
                    kill_process_group_now(g);
                }
                self.unlink_worker_token(&tid);
            }
        }
        if let Ok(reg) = self.registry.lock() {
            for (tid, entry) in &reg.by_id {
                if let Some(g) = entry.take_pgid() {
                    kill_process_group_now(g);
                }
                self.unlink_worker_token(tid);
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
            registry: Mutex::new(SessionRegistry::default()),
            spawn_locks: Mutex::new(HashMap::new()),
            active_processes: Mutex::new(HashMap::new()),
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

    /// Gracefully retires and reaps every ACP worker process group and token.
    pub async fn shutdown_all(&self) {
        let session_tids: Vec<HostThreadId> = {
            let reg = self.registry.lock().unwrap_or_else(|e| e.into_inner());
            reg.by_id
                .iter()
                .filter(|(_, entry)| !entry.archived.load(Ordering::Acquire))
                .map(|(tid, _)| tid.clone())
                .collect()
        };
        let futs = session_tids
            .iter()
            .map(|tid| self.retire_worker_thread(tid));
        let _ = futures_util::future::join_all(futs).await;

        let leftover: Vec<(HostThreadId, PgidSlot)> = {
            let mut active = self
                .active_processes
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            active.drain().collect()
        };
        for (tid, slot) in leftover {
            self.unlink_worker_token(&tid);
            if let Some(g) = take_pgid_slot(&slot) {
                kill_process_group_now(g);
                let deadline = std::time::Instant::now() + Duration::from_millis(300);
                while process_group_alive(g) && std::time::Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        }
    }

    fn register_active_process(&self, thread: &HostThreadId, slot: PgidSlot) {
        self.active_processes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(thread.clone(), slot);
    }

    fn unregister_active_process(&self, thread: &HostThreadId) {
        self.active_processes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(thread);
    }

    fn claim_pgid(&self, thread: &HostThreadId, entry: Option<&SessionEntry>) -> Option<u32> {
        self.unregister_active_process(thread);
        entry.and_then(SessionEntry::take_pgid)
    }

    fn evict_excess_archived_sessions(&self) {
        let mut evicted_tids = Vec::new();
        {
            let mut reg = self.registry.lock().unwrap_or_else(|e| e.into_inner());
            let mut archived_count = reg
                .by_id
                .values()
                .filter(|e| e.archived.load(Ordering::Acquire))
                .count();
            if archived_count <= MAX_ARCHIVED_SESSIONS {
                return;
            }
            let mut retained_order = VecDeque::with_capacity(reg.order.len());
            while let Some(tid) = reg.order.pop_front() {
                if archived_count > MAX_ARCHIVED_SESSIONS
                    && let Some(entry) = reg.by_id.get(&tid)
                    && entry.archived.load(Ordering::Acquire)
                {
                    let role_key = (entry.wave.clone(), entry.role.clone());
                    if reg.by_role.get(&role_key) == Some(&tid) {
                        reg.by_role.remove(&role_key);
                    }
                    reg.by_id.remove(&tid);
                    evicted_tids.push(tid);
                    archived_count -= 1;
                    continue;
                }
                retained_order.push_back(tid);
            }
            reg.order = retained_order;
        }
        if !evicted_tids.is_empty() {
            let mut locks = self.spawn_locks.lock().unwrap_or_else(|e| e.into_inner());
            for tid in evicted_tids {
                locks.remove(&tid);
            }
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
        let thread_lock = {
            let mut locks = self.spawn_locks.lock().unwrap_or_else(|e| e.into_inner());
            locks
                .entry(thread.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _spawn_guard = thread_lock.lock().await;

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

        let pgid_slot: PgidSlot = Arc::new(Mutex::new(None));
        self.register_active_process(&thread, pgid_slot.clone());
        let mut hs_guard = HandshakeCleanupGuard {
            bridge: self,
            thread: thread.clone(),
            pgid_slot: pgid_slot.clone(),
            armed: true,
        };

        let mut cmd = Command::new(&program);
        cmd.args(&args)
            .current_dir(&cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        for var in SCRUBBED_DAEMON_ENV {
            cmd.env_remove(var);
        }
        cmd.env("BELL_GUARD", "1")
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
                return Err(HostError::Rejected(format!(
                    "failed to spawn ACP provider '{}': {e}",
                    program.display()
                )));
            }
        };

        *pgid_slot.lock().unwrap_or_else(|e| e.into_inner()) = child.id();
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| HostError::Rejected("ACP child missing piped stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| HostError::Rejected("ACP child missing piped stdout".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| HostError::Rejected("ACP child missing piped stderr".into()))?;

        let (mut reader_rx, reader_handle) = spawn_stdout_reader(stdout);
        let (stderr_tail, stderr_handle) = spawn_stderr_drainer(stderr);

        let handshake_res: Result<(String, Option<ModelId>, u64), HostError> = async {
            let mut next_id = 1u64;

            let init_id = next_id;
            next_id += 1;
            write_json_line_bounded(&mut stdin, &initialize_request(init_id), PROMPT_WRITE_TIMEOUT)
                .await
                .map_err(|e| HostError::Unreachable(format!("write ACP initialize: {e}")))?;
            match wait_for_rpc_response(
                &mut stdin,
                &mut reader_rx,
                init_id,
                self.cfg.handshake_timeout,
                &stderr_tail,
            )
            .await?
            {
                Ok(init_val) => validate_initialize_result(&init_val)?,
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
            write_json_line_bounded(
                &mut stdin,
                &session_new_request(new_id, &cwd, mcp_servers),
                PROMPT_WRITE_TIMEOUT,
            )
            .await
            .map_err(|e| HostError::Unreachable(format!("write ACP session/new: {e}")))?;
            let new_reply = match wait_for_rpc_response(
                &mut stdin,
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

            if let Some(ref requested_model) = req.agent.model {
                let req_str = requested_model.as_str();
                if actual_model.as_ref().map(ModelId::as_str) != Some(req_str) {
                    if let Some(cfg_opt) = extract_model_config_option(&new_reply) {
                        if !cfg_opt.allowed_values.is_empty()
                            && !cfg_opt.allowed_values.iter().any(|v| v == req_str)
                        {
                            return Err(HostError::Rejected(format!(
                                "ACP provider does not advertise requested model '{req_str}' in configOptions"
                            )));
                        }
                        let set_id = next_id;
                        next_id += 1;
                        write_json_line_bounded(
                            &mut stdin,
                            &session_set_config_option_request(
                                set_id,
                                &session_id,
                                &cfg_opt.config_id,
                                req_str,
                            ),
                            PROMPT_WRITE_TIMEOUT,
                        )
                        .await
                        .map_err(|e| {
                            HostError::Unreachable(format!(
                                "write ACP session/set_config_option: {e}"
                            ))
                        })?;
                        match wait_for_rpc_response(
                            &mut stdin,
                            &mut reader_rx,
                            set_id,
                            self.cfg.handshake_timeout,
                            &stderr_tail,
                        )
                        .await?
                        {
                            Ok(val) => {
                                actual_model = Some(validate_selected_config_model(
                                    &val,
                                    &cfg_opt.config_id,
                                    req_str,
                                )?);
                            }
                            Err(err) => {
                                return Err(HostError::Rejected(format!(
                                    "ACP session/set_config_option rejected: {}",
                                    format_jsonrpc_error(&err)
                                )));
                            }
                        }
                    } else if session_supports_models(&new_reply) {
                        if !model_is_listed_in_models_obj(&new_reply, req_str) {
                            return Err(HostError::Rejected(format!(
                                "ACP provider does not advertise requested model '{req_str}' in availableModels"
                            )));
                        }
                        let set_id = next_id;
                        next_id += 1;
                        write_json_line_bounded(
                            &mut stdin,
                            &session_set_model_request(set_id, &session_id, req_str),
                            PROMPT_WRITE_TIMEOUT,
                        )
                        .await
                        .map_err(|e| {
                            HostError::Unreachable(format!("write ACP session/set_model: {e}"))
                        })?;
                        match wait_for_rpc_response(
                            &mut stdin,
                            &mut reader_rx,
                            set_id,
                            self.cfg.handshake_timeout,
                            &stderr_tail,
                        )
                        .await?
                        {
                            Ok(val) => {
                                actual_model = Some(validate_selected_models_obj(&val, req_str)?);
                            }
                            Err(err) => {
                                return Err(HostError::Rejected(format!(
                                    "ACP session/set_model rejected: {}",
                                    format_jsonrpc_error(&err)
                                )));
                            }
                        }
                    } else {
                        return Err(HostError::Rejected(format!(
                            "ACP provider does not support selecting requested model '{req_str}' (check worker model or harnesses.<provider>.model in config.toml)"
                        )));
                    }
                }
            }

            Ok((session_id, actual_model, next_id))
        }
        .await;

        let (session_id, actual_model, next_rpc_id) = match handshake_res {
            Ok(tuple) => tuple,
            Err(err) => {
                drop(stdin);
                let reap_pgid = take_pgid_slot(&pgid_slot);
                let mut kill_guard = KillPgidOnDrop { pgid: reap_pgid };
                terminate_and_reap_process_group(&mut child, reap_pgid, self.cfg.retire_grace)
                    .await;
                kill_guard.pgid = None;
                reader_handle.abort();
                stderr_handle.abort();
                self.unregister_active_process(&thread);
                self.unlink_worker_token(&thread);
                hs_guard.armed = false;
                return Err(err);
            }
        };

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

        // Synchronous, non-yielding handoff from `hs_guard` to the live `SessionActor`.
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let feed = Arc::new(SessionFeed::new(thread.clone()));
        let entry = Arc::new(SessionEntry {
            wave: req.wave.clone(),
            role: req.role.clone(),
            attempt: req.attempt,
            pgid_slot,
            parked_snapshot: Mutex::new(None),
            cmd_tx,
            feed,
            alive: AtomicBool::new(true),
            archived: AtomicBool::new(false),
            retire_lock: tokio::sync::Mutex::new(()),
        });

        {
            let mut reg = self.registry.lock().unwrap_or_else(|e| e.into_inner());
            if !reg.by_id.contains_key(&thread) {
                reg.order.push_back(thread.clone());
            }
            reg.by_id.insert(thread.clone(), entry.clone());
            let should_replace = reg
                .by_role
                .get(&(req.wave.clone(), req.role.clone()))
                .and_then(parse_acp_thread_id)
                .is_none_or(|(_, _, prev_attempt)| prev_attempt <= req.attempt);
            if should_replace {
                reg.by_role
                    .insert((req.wave.clone(), req.role.clone()), thread.clone());
            }
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
        hs_guard.armed = false;

        Ok(thread)
    }

    fn get_session(&self, thread: &HostThreadId) -> Option<Arc<SessionEntry>> {
        self.registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .by_id
            .get(thread)
            .cloned()
    }

    fn get_session_by_role(&self, wave: &WaveSlug, role: &RoleName) -> Option<Arc<SessionEntry>> {
        let reg = self.registry.lock().unwrap_or_else(|e| e.into_inner());
        let tid = reg.by_role.get(&(wave.clone(), role.clone()))?;
        reg.by_id.get(tid).cloned()
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

    fn validate_worker_interaction(
        &self,
        thread: &HostThreadId,
        interaction: &HostInteractionId,
        resolution: &InteractionResolution,
    ) -> Result<(), HostError> {
        let entry = self
            .get_session(thread)
            .ok_or_else(|| HostError::Rejected(format!("unknown ACP worker thread {thread}")))?;
        if !entry.alive.load(Ordering::Acquire) {
            return Err(HostError::Rejected(format!(
                "ACP worker {thread} already exited"
            )));
        }
        let Some(parked) = entry.get_parked_snapshot() else {
            return Err(HostError::Rejected(format!(
                "ACP worker {thread} has no pending permission interaction"
            )));
        };
        if &parked.interaction.id != interaction {
            return Err(HostError::Rejected(format!(
                "ACP worker {thread} is blocked on {}, not {interaction}",
                parked.interaction.id
            )));
        }
        let _ = select_permission_response(&parked, resolution)?;
        Ok(())
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
        let wait_deadline = self.cfg.retire_grace + Duration::from_millis(1500);
        if entry.cmd_tx.send(SessionCmd::Retire { reply: tx }).is_ok()
            && matches!(
                tokio::time::timeout(wait_deadline, rx).await,
                Ok(Ok(Ok(())))
            )
        {
            return Ok(());
        }

        // Actor already exited or is wedged writing to stdin; force-reap the group once independently.
        if let Some(pgid) = self.claim_pgid(thread, Some(&entry)) {
            kill_process_group_now(pgid);
            let deadline = std::time::Instant::now() + Duration::from_millis(300);
            while process_group_alive(pgid) && std::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
        entry.set_parked_snapshot(None);
        entry.alive.store(false, Ordering::Release);
        if !entry.archived.swap(true, Ordering::AcqRel) {
            entry.feed.mark_closed();
            self.evict_excess_archived_sessions();
            self.call_wave_observe(
                entry.wave.clone(),
                entry.role.clone(),
                Observation::archived_for(thread.clone(), Some(entry.attempt)),
            )
            .await;
        }
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
            self.unlink_worker_token(thread);
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
        if !entry.alive.load(Ordering::Acquire) {
            let terminal = match entry.feed.current_activity() {
                ref act @ (Activity::Refused { .. } | Activity::Errored { .. }) => act.clone(),
                _ => Activity::Errored {
                    error: "ACP worker process exited".into(),
                },
            };
            return Ok(Some(Observation::activity_with_bounds(
                terminal,
                thread.clone(),
                None,
                Vec::new(),
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

    fn validate_resolve(
        &self,
        thread: &HostThreadId,
        interaction: &HostInteractionId,
        resolution: &InteractionResolution,
    ) -> BoxFut<'_, Result<(), HostError>> {
        let res = self
            .0
            .validate_worker_interaction(thread, interaction, resolution);
        Box::pin(async move { res })
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
