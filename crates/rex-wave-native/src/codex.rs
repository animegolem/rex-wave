//! Native Codex `app-server` stdio JSON-RPC 2.0 runner (`CodexBridge`, `CodexHost`, `CodexFeed`).

use crate::common::{
    ErasedBackend, ObserverItem, ThreadEventLog, flush_observer, format_thread_id,
    parse_prefixed_thread_id, provision_native_worker_token, revoke_native_worker_token,
    spawn_worker_observer, truncate_summary,
};
use crate::process::{
    DEFAULT_RETIRE_GRACE, LiveChild, spawn_in_process_group, spawn_stderr_drainer,
    terminate_process_group,
};
use crate::workspace::resolve_worker_workspace;
use rex_wave_api::read::{
    AgentEventKind, ContextCategory, ContextCategoryKind, ThreadContextUsage, ThreadPageView,
};
use rex_wave_api::room::Body;
use rex_wave_api::wave::{
    Activity, InteractionKind, InteractionResolution, Observation, PendingInteraction,
    PermissionMode, Question, SteerDeliveryMode, is_refusal_error_text, is_refusal_stop_reason,
};
use rex_wave_api::{
    ApiError, CODEX_THREAD_PREFIX, FrameStream, HostInteractionId, HostSeq, HostThreadId, ModelId,
    Principal, RoleName, SystemPart, WaveSlug,
};
use rex_wave_runtime::{AgentFeed, BoxFut, BridgeBackend, Router};
use rex_wave_transport::WORKER_THREAD_ENV;
use rex_wave_waves::{AgentHost, HostError, SpawnRequest};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command};
use tokio::sync::{mpsc, oneshot};

pub const CODEX_BIN_ENV: &str = "REX_WAVE_CODEX_BIN";
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const INTERRUPT_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_STDERR_BYTES: usize = 4096;

#[derive(Clone, Debug)]
pub struct CodexRunnerConfig {
    pub bin: PathBuf,
    pub state_dir: Option<PathBuf>,
    pub socket_path: Option<PathBuf>,
    pub bb_data_dir: Option<PathBuf>,
    pub retire_grace: Duration,
    pub rpc_timeout: Duration,
    pub system_principal: Principal,
    pub extra_args: Vec<String>,
}

impl Default for CodexRunnerConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl CodexRunnerConfig {
    pub fn from_env() -> Self {
        let bin = std::env::var_os(CODEX_BIN_ENV)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("codex"));
        Self {
            bin,
            state_dir: None,
            socket_path: None,
            bb_data_dir: rex_wave_waves::default_bb_data_dir(),
            retire_grace: DEFAULT_RETIRE_GRACE,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
            system_principal: Principal::System {
                part: SystemPart::WaveConductor,
            },
            extra_args: Vec::new(),
        }
    }

    pub fn with_bin(mut self, bin: impl Into<PathBuf>) -> Self {
        self.bin = bin.into();
        self
    }

    pub fn with_state_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.state_dir = Some(dir.into());
        self
    }

    pub fn with_socket_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.socket_path = Some(path.into());
        self
    }

    pub fn with_bb_data_dir(mut self, dir: Option<PathBuf>) -> Self {
        self.bb_data_dir = dir;
        self
    }

    pub fn with_retire_grace(mut self, grace: Duration) -> Self {
        self.retire_grace = grace;
        self
    }

    pub fn with_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = timeout;
        self
    }

    pub fn with_system_principal(mut self, principal: Principal) -> Self {
        self.system_principal = principal;
        self
    }

    pub fn with_extra_args(mut self, args: Vec<String>) -> Self {
        self.extra_args = args;
        self
    }
}

pub fn codex_thread_id(
    wave: &WaveSlug,
    role: &RoleName,
    attempt: u32,
) -> Result<HostThreadId, HostError> {
    format_thread_id(CODEX_THREAD_PREFIX, wave, role, attempt)
}

pub fn parse_codex_thread_id(thread: &HostThreadId) -> Option<(WaveSlug, RoleName, u32)> {
    parse_prefixed_thread_id(CODEX_THREAD_PREFIX, thread)
}

pub fn codex_approval_and_sandbox(mode: PermissionMode) -> (&'static str, &'static str) {
    match mode {
        PermissionMode::AcceptEdits => ("on-request", "workspace-write"),
        PermissionMode::Auto => ("on-request", "workspace-write"),
        PermissionMode::Full => ("never", "danger-full-access"),
    }
}

fn is_codex_policy_error_info(raw: &str) -> bool {
    let lower = raw.trim().to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "cyberpolicy"
            | "biopolicy"
            | "misalignmentpolicyviolation"
            | "inferencehookdenied"
            | "policycapacityunavailable"
    ) || is_refusal_stop_reason(&lower)
}

pub fn parse_codex_token_usage(params: &Value) -> Option<ThreadContextUsage> {
    let tu = params.get("tokenUsage").unwrap_or(params);
    if !tu.is_object() {
        return None;
    }
    let breakdown = tu
        .get("total")
        .or_else(|| tu.get("last"))
        .filter(|v| v.is_object())
        .unwrap_or(tu);

    let input = breakdown
        .get("inputTokens")
        .or_else(|| breakdown.get("input_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let cached = breakdown
        .get("cachedInputTokens")
        .or_else(|| breakdown.get("cached_input_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let output = breakdown
        .get("outputTokens")
        .or_else(|| breakdown.get("output_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let reasoning = breakdown
        .get("reasoningOutputTokens")
        .or_else(|| breakdown.get("reasoning_output_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let total = breakdown
        .get("totalTokens")
        .or_else(|| breakdown.get("total_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or_else(|| {
            input
                .saturating_add(cached)
                .saturating_add(output)
                .saturating_add(reasoning)
        });

    let model_context_window = tu
        .get("modelContextWindow")
        .or_else(|| tu.get("model_context_window"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let mut categories = Vec::new();
    if input > 0 {
        categories.push(ContextCategory {
            id: "input".into(),
            label: "Input".into(),
            kind: ContextCategoryKind::Used,
            tokens: input,
        });
    }
    if cached > 0 {
        categories.push(ContextCategory {
            id: "cached_input".into(),
            label: "Cached input".into(),
            kind: ContextCategoryKind::Reserved,
            tokens: cached,
        });
    }
    if output > 0 {
        categories.push(ContextCategory {
            id: "output".into(),
            label: "Output".into(),
            kind: ContextCategoryKind::Used,
            tokens: output,
        });
    }
    if reasoning > 0 {
        categories.push(ContextCategory {
            id: "reasoning".into(),
            label: "Reasoning".into(),
            kind: ContextCategoryKind::Used,
            tokens: reasoning,
        });
    }
    if model_context_window > total {
        categories.push(ContextCategory {
            id: "free".into(),
            label: "Free".into(),
            kind: ContextCategoryKind::Free,
            tokens: model_context_window - total,
        });
    }

    Some(ThreadContextUsage {
        used_tokens: total,
        model_context_window,
        estimated: false,
        auto_compact_at_tokens: None,
        cost_usd: None,
        categories,
    })
}

pub fn parse_codex_approval_request(
    rpc_id: &Value,
    method: &str,
    params: &Value,
) -> Option<PendingInteraction> {
    let fallback_id = match rpc_id {
        Value::String(s) => format!("rpc-{s}"),
        Value::Number(n) => format!("rpc-{n}"),
        _ => "rpc-req".to_owned(),
    };
    let raw_id = params
        .get("approvalId")
        .or_else(|| params.get("itemId"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .unwrap_or(fallback_id);
    let id = HostInteractionId::parse(raw_id).ok()?;

    match method {
        "item/commandExecution/requestApproval" => {
            let cmd = params
                .get("command")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let reason = params
                .get("reason")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let summary = match (cmd, reason) {
                (Some(c), Some(r)) => format!("command `{}` ({})", truncate_summary(c, 120), r),
                (Some(c), None) => format!("command `{}`", truncate_summary(c, 160)),
                (None, Some(r)) => r.to_owned(),
                (None, None) => "Approve command execution".to_owned(),
            };
            Some(PendingInteraction {
                id,
                kind: InteractionKind::Permission,
                summary,
            })
        }
        "item/fileChange/requestApproval" => {
            let reason = params
                .get("reason")
                .or_else(|| params.get("grantRoot"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("Approve file changes")
                .to_owned();
            Some(PendingInteraction {
                id,
                kind: InteractionKind::Permission,
                summary: reason,
            })
        }
        "item/permissions/requestApproval" => {
            let reason = params
                .get("reason")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("Approve permission request")
                .to_owned();
            Some(PendingInteraction {
                id,
                kind: InteractionKind::Permission,
                summary: reason,
            })
        }
        "item/tool/requestUserInput" => {
            let mut questions = Vec::new();
            if let Some(arr) = params.get("questions").and_then(|v| v.as_array()) {
                for q in arr {
                    let Some(qid) = q.get("id").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    let prompt = q
                        .get("question")
                        .or_else(|| q.get("prompt"))
                        .or_else(|| q.get("header"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_owned();
                    let multi = q
                        .get("multiSelect")
                        .or_else(|| q.get("multi"))
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    let mut options = Vec::new();
                    if let Some(opts) = q.get("options").and_then(|v| v.as_array()) {
                        for o in opts {
                            if let Some(s) = o.as_str() {
                                options.push(s.to_owned());
                            } else if let Some(label) = o
                                .get("value")
                                .or_else(|| o.get("label"))
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty())
                            {
                                options.push(label.to_owned());
                            }
                        }
                    }
                    questions.push(Question {
                        id: qid.to_owned(),
                        prompt,
                        options,
                        multi,
                    });
                }
            }
            let summary = questions
                .first()
                .map(|q| q.prompt.clone())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "Question from Codex worker".to_owned());
            Some(PendingInteraction {
                id,
                kind: InteractionKind::Question { questions },
                summary,
            })
        }
        _ => None,
    }
}

pub fn encode_codex_approval_result(
    method: &str,
    resolution: &InteractionResolution,
) -> Result<Value, HostError> {
    match method {
        "item/commandExecution/requestApproval"
        | "item/fileChange/requestApproval"
        | "item/permissions/requestApproval" => match resolution {
            InteractionResolution::AllowOnce { .. } => Ok(json!({ "decision": "accept" })),
            InteractionResolution::AllowForSession { .. } => {
                Ok(json!({ "decision": "acceptForSession" }))
            }
            InteractionResolution::Deny => Ok(json!({ "decision": "decline" })),
            InteractionResolution::RequestAnswer { value } => Ok(value.clone()),
            _ => Err(HostError::Rejected(format!(
                "Codex {method} supports AllowOnce, AllowForSession, Deny, or RequestAnswer"
            ))),
        },
        "item/tool/requestUserInput" => match resolution {
            InteractionResolution::UserAnswer { answers } => {
                let mut map = Map::new();
                for (qid, ans) in answers {
                    let mut list = ans.selected.clone();
                    if let Some(ft) = ans
                        .free_text
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        list.push(ft.to_owned());
                    }
                    map.insert(qid.clone(), json!({ "answers": list }));
                }
                Ok(json!({ "answers": Value::Object(map) }))
            }
            InteractionResolution::RequestAnswer { value } => Ok(value.clone()),
            _ => Err(HostError::Rejected(
                "Codex item/tool/requestUserInput requires UserAnswer or RequestAnswer".into(),
            )),
        },
        other => Err(HostError::Rejected(format!(
            "unsupported Codex approval method {other}"
        ))),
    }
}

pub fn classify_codex_turn(params: &Value, prior_error: Option<&Value>) -> Activity {
    let turn = params.get("turn").unwrap_or(params);
    let status = turn
        .get("status")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .unwrap_or("completed");

    let err_obj = turn.get("error").filter(|v| !v.is_null()).or(prior_error);

    let err_msg = err_obj
        .and_then(|e| {
            e.get("message")
                .and_then(|v| v.as_str())
                .or_else(|| e.get("additionalDetails").and_then(|v| v.as_str()))
                .or_else(|| e.as_str())
        })
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let extra_details = err_obj
        .and_then(|e| e.get("additionalDetails"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let err_info_str = err_obj
        .and_then(|e| e.get("codexErrorInfo"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let has_misalignment = err_obj
        .and_then(|e| e.get("misalignment"))
        .is_some_and(|v| !v.is_null());

    if is_refusal_stop_reason(status)
        || has_misalignment
        || err_info_str.is_some_and(is_codex_policy_error_info)
        || err_msg.is_some_and(is_refusal_error_text)
        || extra_details.is_some_and(is_refusal_error_text)
    {
        let reason = match (err_info_str, err_msg) {
            (Some(code), Some(msg))
                if !msg
                    .to_ascii_lowercase()
                    .contains(&code.to_ascii_lowercase()) =>
            {
                format!("{code}: {msg}")
            }
            (_, Some(msg)) => msg.to_owned(),
            (Some(code), None) => code.to_owned(),
            (None, None) => status.to_owned(),
        };
        return Activity::Refused { reason };
    }

    if status == "failed" {
        let error = err_msg.unwrap_or("Codex turn failed").to_owned();
        return Activity::Errored { error };
    }

    Activity::Idle
}

type StdinLine = (String, oneshot::Sender<Result<(), String>>);

fn spawn_stdin_writer(mut stdin: ChildStdin) -> mpsc::UnboundedSender<StdinLine> {
    let (tx, mut rx) = mpsc::unbounded_channel::<StdinLine>();
    tokio::spawn(async move {
        while let Some((line, reply)) = rx.recv().await {
            let mut payload = line.into_bytes();
            payload.push(b'\n');
            let res = match stdin.write_all(&payload).await {
                Ok(()) => stdin.flush().await.map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
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

async fn write_stdin_line(
    tx: &mpsc::UnboundedSender<StdinLine>,
    line: String,
) -> Result<(), String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send((line, reply_tx))
        .map_err(|_| "Codex stdin writer closed".to_owned())?;
    reply_rx
        .await
        .map_err(|_| "Codex stdin writer dropped before flushing".to_owned())?
}

struct PendingCodexApproval {
    rpc_id: Value,
    method: String,
}

struct CodexWorkerState {
    thread: HostThreadId,
    attempt: u32,
    req: SpawnRequest,
    log: Arc<Mutex<ThreadEventLog>>,
    observer: mpsc::UnboundedSender<ObserverItem>,
    retiring: AtomicBool,
    retired: AtomicBool,
    next_rpc_id: AtomicU64,
    codex_thread_id: Mutex<Option<String>>,
    active_turn_id: Mutex<Option<String>>,
    started_turns: Mutex<HashSet<String>>,
    streamed_items: Mutex<HashSet<String>>,
    last_error: Mutex<Option<Value>>,
    stdin_tx: Mutex<Option<mpsc::UnboundedSender<StdinLine>>>,
    live_child: tokio::sync::Mutex<Option<LiveChild>>,
    pending_rpc: Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>,
    pending_approvals: Mutex<HashMap<HostInteractionId, PendingCodexApproval>>,
}

impl CodexWorkerState {
    fn emit_activity(&self, activity: Activity, include_settled: bool) {
        let (host_seq, settled_seq) = {
            let mut log = self.log.lock().unwrap_or_else(|e| e.into_inner());
            log.activity = activity.clone();
            if !matches!(activity, Activity::Blocked { .. }) {
                log.pending = None;
            }
            let hseq = log.next_host_seq();
            let sseq = include_settled.then(|| log.last_event_seq()).flatten();
            (hseq, sseq)
        };
        let _ = self.observer.send(ObserverItem::Observe(
            Observation::activity_with_seq_and_bounds(
                activity,
                self.thread.clone(),
                Some(host_seq),
                settled_seq,
                Vec::new(),
            ),
        ));
    }

    async fn rpc_call(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, HostError> {
        let id = self.next_rpc_id.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let mut pending = self.pending_rpc.lock().unwrap_or_else(|e| e.into_inner());
            pending.insert(id, reply_tx);
        }

        let payload = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        })
        .to_string();

        let stdin_tx = self
            .stdin_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| {
                self.pending_rpc
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&id);
                HostError::Unreachable(format!("Codex thread {} stdin closed", self.thread))
            })?;

        if let Err(err) = write_stdin_line(&stdin_tx, payload).await {
            self.pending_rpc
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id);
            return Err(HostError::Unreachable(format!(
                "send Codex RPC {method}: {err}"
            )));
        }

        match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(Ok(val))) => Ok(val),
            Ok(Ok(Err(rpc_err))) => Err(HostError::Rejected(format!(
                "Codex RPC {method} failed: {rpc_err}"
            ))),
            Ok(Err(_)) => Err(HostError::Unreachable(format!(
                "Codex process closed while awaiting {method}"
            ))),
            Err(_) => {
                self.pending_rpc
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&id);
                Err(HostError::Unreachable(format!(
                    "Codex RPC {method} timed out after {}ms",
                    timeout.as_millis()
                )))
            }
        }
    }

    async fn rpc_notify(&self, method: &str, params: Value) -> Result<(), HostError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        })
        .to_string();
        let stdin_tx = self
            .stdin_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| {
                HostError::Unreachable(format!("Codex thread {} stdin closed", self.thread))
            })?;
        write_stdin_line(&stdin_tx, payload)
            .await
            .map_err(|e| HostError::Unreachable(format!("send Codex notification {method}: {e}")))
    }

    async fn rpc_respond(&self, rpc_id: Value, result: Value) -> Result<(), HostError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": rpc_id,
            "result": result,
        })
        .to_string();
        let stdin_tx = self
            .stdin_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| {
                HostError::Unreachable(format!("Codex thread {} stdin closed", self.thread))
            })?;
        write_stdin_line(&stdin_tx, payload)
            .await
            .map_err(|e| HostError::Unreachable(format!("send Codex approval response: {e}")))
    }

    fn fail_all_pending_rpc(&self, reason: &str) {
        let mut pending = self.pending_rpc.lock().unwrap_or_else(|e| e.into_inner());
        for (_, tx) in pending.drain() {
            let _ = tx.send(Err(reason.to_owned()));
        }
    }
}

/// Core bridge managing native Codex `app-server` worker child processes.
pub struct CodexBridge {
    cfg: CodexRunnerConfig,
    backend: OnceLock<Arc<dyn ErasedBackend>>,
    workers_by_thread: Mutex<HashMap<HostThreadId, Arc<CodexWorkerState>>>,
    workers_by_role: Mutex<HashMap<(WaveSlug, RoleName), Arc<CodexWorkerState>>>,
}

impl CodexBridge {
    pub fn new(cfg: CodexRunnerConfig) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            backend: OnceLock::new(),
            workers_by_thread: Mutex::new(HashMap::new()),
            workers_by_role: Mutex::new(HashMap::new()),
        })
    }

    pub fn config(&self) -> &CodexRunnerConfig {
        &self.cfg
    }

    pub fn attach_backend<B: BridgeBackend>(&self, backend: B) {
        let _ = self.backend.set(Arc::new(backend));
    }

    pub fn attach_router(&self, router: Arc<Router>) {
        self.attach_backend(router);
    }

    pub async fn shutdown(&self) {
        let threads: Vec<HostThreadId> = {
            let map = self
                .workers_by_thread
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            map.keys().cloned().collect()
        };
        for tid in threads {
            let _ = self.retire_worker(&tid).await;
        }
    }

    async fn spawn_worker(&self, req: SpawnRequest) -> Result<HostThreadId, HostError> {
        let thread = codex_thread_id(&req.wave, &req.role, req.attempt)?;
        if let Some(existing) = self
            .workers_by_thread
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&thread)
            .cloned()
            && !existing.retiring.load(Ordering::Acquire)
            && !existing.retired.load(Ordering::Acquire)
        {
            return Ok(thread);
        }

        let cwd = resolve_worker_workspace(&req, self.cfg.bb_data_dir.as_deref()).await?;
        let provisioned = provision_native_worker_token(
            self.cfg.state_dir.as_deref(),
            &req.wave,
            &req.role,
            &thread,
        )?;
        let worker_token = provisioned.map(|p| p.token);

        let mut cmd = Command::new(&self.cfg.bin);
        cmd.arg("app-server")
            .args(&self.cfg.extra_args)
            .current_dir(&cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env(WORKER_THREAD_ENV, thread.as_str());

        if let Some(ref sock) = self.cfg.socket_path {
            cmd.env("REX_WAVE_SOCKET", sock);
        }
        if let Some(ref tok) = worker_token {
            cmd.env("REX_WAVE_TOKEN", tok);
        }

        let (mut child, pgid) = match spawn_in_process_group(&mut cmd) {
            Ok(pair) => pair,
            Err(err) => {
                revoke_native_worker_token(self.cfg.state_dir.as_deref(), &thread);
                return Err(err);
            }
        };

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| HostError::Unreachable("Codex child stdin pipe unavailable".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| HostError::Unreachable("Codex child stdout pipe unavailable".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| HostError::Unreachable("Codex child stderr pipe unavailable".into()))?;

        let log = Arc::new(Mutex::new(ThreadEventLog::new(
            thread.clone(),
            req.wave.clone(),
            req.role.clone(),
            req.attempt,
        )));
        let observer = spawn_worker_observer(
            self.backend.get().cloned(),
            self.cfg.system_principal.clone(),
            req.wave.clone(),
            req.role.clone(),
            thread.clone(),
            req.attempt,
        );

        let stdin_tx = spawn_stdin_writer(stdin);
        let stderr_task = spawn_stderr_drainer(stderr, MAX_STDERR_BYTES);

        let worker = Arc::new(CodexWorkerState {
            thread: thread.clone(),
            attempt: req.attempt,
            req: req.clone(),
            log,
            observer,
            retiring: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            next_rpc_id: AtomicU64::new(1),
            codex_thread_id: Mutex::new(None),
            active_turn_id: Mutex::new(None),
            started_turns: Mutex::new(HashSet::new()),
            streamed_items: Mutex::new(HashSet::new()),
            last_error: Mutex::new(None),
            stdin_tx: Mutex::new(Some(stdin_tx)),
            live_child: tokio::sync::Mutex::new(Some(LiveChild::new(child, pgid, 1))),
            pending_rpc: Mutex::new(HashMap::new()),
            pending_approvals: Mutex::new(HashMap::new()),
        });

        let reader_worker = worker.clone();
        tokio::spawn(async move {
            run_codex_stdout_reader(reader_worker, stdout, stderr_task).await;
        });

        let handshake_res = self.run_codex_handshake(&worker, &cwd, &req).await;
        if let Err(err) = handshake_res {
            worker.retiring.store(true, Ordering::Release);
            worker
                .stdin_tx
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            if let Some(mut live) = worker.live_child.lock().await.take() {
                terminate_process_group(&mut live, self.cfg.retire_grace).await;
            }
            revoke_native_worker_token(self.cfg.state_dir.as_deref(), &thread);
            return Err(err);
        }

        {
            let mut by_thread = self
                .workers_by_thread
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            by_thread.insert(thread.clone(), worker.clone());
        }
        {
            let mut by_role = self
                .workers_by_role
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            by_role.insert((req.wave.clone(), req.role.clone()), worker);
        }

        Ok(thread)
    }

    async fn run_codex_handshake(
        &self,
        worker: &Arc<CodexWorkerState>,
        cwd: &std::path::Path,
        req: &SpawnRequest,
    ) -> Result<(), HostError> {
        let init_res = worker
            .rpc_call(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "rex-wave",
                        "version": env!("CARGO_PKG_VERSION"),
                    }
                }),
                self.cfg.rpc_timeout,
            )
            .await?;

        worker.rpc_notify("initialized", json!({})).await?;

        let (approval_policy, sandbox) = codex_approval_and_sandbox(req.agent.permission);
        let mut thread_params = Map::new();
        thread_params.insert(
            "cwd".into(),
            Value::String(cwd.to_string_lossy().into_owned()),
        );
        thread_params.insert(
            "approvalPolicy".into(),
            Value::String(approval_policy.to_owned()),
        );
        thread_params.insert("sandbox".into(), Value::String(sandbox.to_owned()));
        if let Some(ref model) = req.agent.model {
            thread_params.insert("model".into(), Value::String(model.as_str().to_owned()));
        }
        if let Some(tier) = req.agent.service_tier.as_deref().map(str::trim)
            && !tier.is_empty()
        {
            thread_params.insert("serviceTier".into(), Value::String(tier.to_owned()));
        }

        let thread_res = worker
            .rpc_call(
                "thread/start",
                Value::Object(thread_params),
                self.cfg.rpc_timeout,
            )
            .await?;

        let codex_tid = thread_res
            .get("thread")
            .and_then(|t| t.get("id"))
            .or_else(|| thread_res.get("threadId"))
            .or_else(|| thread_res.get("id"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                HostError::Rejected("Codex thread/start reply missing thread.id".into())
            })?
            .to_owned();

        *worker
            .codex_thread_id
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(codex_tid.clone());

        let actual_model = thread_res
            .get("model")
            .or_else(|| thread_res.get("thread").and_then(|t| t.get("model")))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .and_then(|s| ModelId::parse(s).ok());
        let actual_account = thread_res
            .get("account")
            .or_else(|| thread_res.get("accountEmail"))
            .or_else(|| init_res.get("account"))
            .or_else(|| init_res.get("accountEmail"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        if actual_model.is_some() || actual_account.is_some() {
            let _ = worker
                .observer
                .send(ObserverItem::Observe(Observation::runtime_meta(
                    actual_model,
                    actual_account,
                    Some(worker.thread.clone()),
                    Some(worker.attempt),
                )));
        }

        let mut turn_params = Map::new();
        turn_params.insert("threadId".into(), Value::String(codex_tid));
        turn_params.insert(
            "input".into(),
            json!([{ "type": "text", "text": req.prompt.as_str() }]),
        );
        if let Some(effort) = req.agent.reasoning.as_deref().map(str::trim)
            && !effort.is_empty()
        {
            turn_params.insert("effort".into(), Value::String(effort.to_owned()));
        }
        if let Some(ref schema) = req.return_schema {
            turn_params.insert("outputSchema".into(), schema.clone());
        }

        {
            let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
            log.push_event(AgentEventKind::UserMessage {
                text: req.prompt.as_str().to_owned(),
            });
        }

        let turn_res = worker
            .rpc_call(
                "turn/start",
                Value::Object(turn_params),
                self.cfg.rpc_timeout,
            )
            .await?;

        if let Some(turn_id) = turn_res
            .get("turn")
            .and_then(|t| t.get("id"))
            .or_else(|| turn_res.get("turnId"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            *worker
                .active_turn_id
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(turn_id.to_owned());
            if worker
                .started_turns
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(turn_id.to_owned())
            {
                worker
                    .log
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push_event(AgentEventKind::TurnStarted);
            }
        }

        Ok(())
    }

    async fn tell_worker(
        &self,
        thread: &HostThreadId,
        body: &Body,
        mode: SteerDeliveryMode,
    ) -> Result<(), HostError> {
        let worker = {
            let map = self
                .workers_by_thread
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            map.get(thread).cloned()
        }
        .ok_or_else(|| HostError::Rejected(format!("unknown Codex thread {thread}")))?;

        if worker.retiring.load(Ordering::Acquire) || worker.retired.load(Ordering::Acquire) {
            return Err(HostError::Rejected(format!(
                "Codex thread {thread} is retired"
            )));
        }

        let codex_tid = worker
            .codex_thread_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| {
                HostError::Rejected(format!("Codex thread {thread} has no active session"))
            })?;

        let active_turn = worker
            .active_turn_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        let wants_steer = matches!(
            mode,
            SteerDeliveryMode::Steer | SteerDeliveryMode::SteerIfActive
        );

        if wants_steer && let Some(expected_turn_id) = active_turn {
            let steer_res = worker
                .rpc_call(
                    "turn/steer",
                    json!({
                        "threadId": codex_tid,
                        "expectedTurnId": expected_turn_id,
                        "input": [{ "type": "text", "text": body.as_str() }],
                    }),
                    self.cfg.rpc_timeout,
                )
                .await;
            match steer_res {
                Ok(_) => {
                    worker
                        .log
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push_event(AgentEventKind::UserMessage {
                            text: body.as_str().to_owned(),
                        });
                    return Ok(());
                }
                Err(HostError::Rejected(_)) if mode == SteerDeliveryMode::SteerIfActive => {}
                Err(err) => return Err(err),
            }
        }

        let mut params = Map::new();
        params.insert("threadId".into(), Value::String(codex_tid));
        params.insert(
            "input".into(),
            json!([{ "type": "text", "text": body.as_str() }]),
        );
        if let Some(effort) = worker.req.agent.reasoning.as_deref().map(str::trim)
            && !effort.is_empty()
        {
            params.insert("effort".into(), Value::String(effort.to_owned()));
        }

        {
            let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
            log.push_event(AgentEventKind::UserMessage {
                text: body.as_str().to_owned(),
            });
        }
        // Emit Working before turn/start so a fast turn/completed strictly follows it in host_seq.
        worker.emit_activity(Activity::Working, false);

        let turn_res = worker
            .rpc_call("turn/start", Value::Object(params), self.cfg.rpc_timeout)
            .await?;

        if let Some(turn_id) = turn_res
            .get("turn")
            .and_then(|t| t.get("id"))
            .or_else(|| turn_res.get("turnId"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            *worker
                .active_turn_id
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(turn_id.to_owned());
            if worker
                .started_turns
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(turn_id.to_owned())
            {
                worker
                    .log
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push_event(AgentEventKind::TurnStarted);
            }
        }

        Ok(())
    }

    async fn resolve_worker(
        &self,
        thread: &HostThreadId,
        interaction: &HostInteractionId,
        resolution: &InteractionResolution,
    ) -> Result<(), HostError> {
        let worker = {
            let map = self
                .workers_by_thread
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            map.get(thread).cloned()
        }
        .ok_or_else(|| HostError::Rejected(format!("unknown Codex thread {thread}")))?;

        if worker.retiring.load(Ordering::Acquire) || worker.retired.load(Ordering::Acquire) {
            return Err(HostError::Rejected(format!(
                "Codex thread {thread} is retired"
            )));
        }

        let (rpc_id, result) = {
            let approvals = worker
                .pending_approvals
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let pending = approvals.get(interaction).ok_or_else(|| {
                HostError::Rejected(format!(
                    "no pending Codex approval {interaction} on {thread}"
                ))
            })?;
            let encoded = encode_codex_approval_result(&pending.method, resolution)?;
            (pending.rpc_id.clone(), encoded)
        };

        worker
            .pending_approvals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(interaction);
        // Emit Working before sending approval response so turn/completed strictly follows it.
        worker.emit_activity(Activity::Working, false);
        worker.rpc_respond(rpc_id, result).await?;
        Ok(())
    }

    async fn retire_worker(&self, thread: &HostThreadId) -> Result<(), HostError> {
        revoke_native_worker_token(self.cfg.state_dir.as_deref(), thread);

        let worker = {
            let map = self
                .workers_by_thread
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            map.get(thread).cloned()
        };

        let Some(worker) = worker else {
            return Ok(());
        };

        worker.retiring.store(true, Ordering::Release);

        let codex_tid = worker
            .codex_thread_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let turn_id = worker
            .active_turn_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();

        if let (Some(ctid), Some(tid)) = (codex_tid, turn_id) {
            let _ = worker
                .rpc_call(
                    "turn/interrupt",
                    json!({ "threadId": ctid, "turnId": tid }),
                    INTERRUPT_TIMEOUT,
                )
                .await;
        }

        worker
            .pending_approvals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        worker.fail_all_pending_rpc("Codex worker retired");
        worker
            .stdin_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();

        if let Some(mut live) = worker.live_child.lock().await.take() {
            terminate_process_group(&mut live, self.cfg.retire_grace).await;
        }

        {
            let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
            log.pending = None;
        }

        if !worker.retired.swap(true, Ordering::AcqRel) {
            let _ = worker
                .observer
                .send(ObserverItem::Observe(Observation::archived_for(
                    worker.thread.clone(),
                    Some(worker.attempt),
                )));
            flush_observer(&worker.observer).await;
        }

        Ok(())
    }

    async fn check_worker_live(
        &self,
        thread: &HostThreadId,
    ) -> Result<Option<Observation>, HostError> {
        let worker = {
            let map = self
                .workers_by_thread
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            map.get(thread).cloned()
        };
        match worker {
            Some(w) if w.retired.load(Ordering::Acquire) => Ok(Some(Observation::archived_for(
                thread.clone(),
                Some(w.attempt),
            ))),
            Some(_) => Ok(None),
            None => {
                revoke_native_worker_token(self.cfg.state_dir.as_deref(), thread);
                Ok(Some(Observation::activity_with_bounds(
                    Activity::Errored {
                        error: "native Codex worker process missing after restart".into(),
                    },
                    thread.clone(),
                    None,
                    Vec::new(),
                )))
            }
        }
    }

    fn lookup_log(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
    ) -> Result<Arc<Mutex<ThreadEventLog>>, ApiError> {
        let map = self
            .workers_by_role
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        map.get(&(wave.clone(), role.clone()))
            .map(|w| w.log.clone())
            .ok_or_else(|| ApiError::NotFound {
                what: format!("Codex worker @{wave}/{role}"),
            })
    }
}

async fn run_codex_stdout_reader(
    worker: Arc<CodexWorkerState>,
    stdout: tokio::process::ChildStdout,
    stderr_task: tokio::task::JoinHandle<String>,
) {
    let mut lines = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(val) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        handle_codex_message(&worker, &val);
    }

    let stderr_tail = tokio::time::timeout(Duration::from_millis(500), stderr_task)
        .await
        .ok()
        .and_then(|r| r.ok())
        .unwrap_or_default();

    worker.fail_all_pending_rpc("Codex stdout closed");

    if worker.retiring.load(Ordering::Acquire) {
        return;
    }

    worker
        .stdin_tx
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take();

    let exit_status = if let Some(mut live) = worker.live_child.lock().await.take() {
        let st = tokio::time::timeout(Duration::from_secs(2), live.child.wait())
            .await
            .ok()
            .and_then(|r| r.ok());
        live.mark_reaped();
        st
    } else {
        None
    };

    if worker.retiring.load(Ordering::Acquire) {
        return;
    }

    let already_terminal = {
        let log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
        matches!(
            log.activity,
            Activity::Refused { .. } | Activity::Errored { .. } | Activity::Idle
        ) && worker
            .active_turn_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
    };
    if already_terminal {
        return;
    }

    let detail = match exit_status {
        Some(st) if !stderr_tail.is_empty() => {
            format!("Codex app-server exited with {st}: {stderr_tail}")
        }
        Some(st) => format!("Codex app-server exited with {st}"),
        None if !stderr_tail.is_empty() => {
            format!("Codex app-server closed unexpectedly: {stderr_tail}")
        }
        None => "Codex app-server closed unexpectedly".to_owned(),
    };
    let act = if is_refusal_error_text(&detail) {
        Activity::Refused { reason: detail }
    } else {
        Activity::Errored { error: detail }
    };
    worker.emit_activity(act, true);
}

fn handle_codex_message(worker: &Arc<CodexWorkerState>, val: &Value) {
    let has_method = val.get("method").and_then(|v| v.as_str()).is_some();
    let has_id = val.get("id").is_some_and(|v| !v.is_null());

    // Response to a client->server JSON-RPC request.
    if has_id && !has_method {
        if let Some(id_num) = val.get("id").and_then(|v| v.as_u64())
            && let Some(tx) = worker
                .pending_rpc
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id_num)
        {
            if let Some(err) = val.get("error").filter(|v| !v.is_null()) {
                let msg = err
                    .get("message")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
                    .unwrap_or_else(|| err.to_string());
                let _ = tx.send(Err(msg));
            } else {
                let res = val.get("result").cloned().unwrap_or(Value::Null);
                let _ = tx.send(Ok(res));
            }
        }
        return;
    }

    let Some(method) = val.get("method").and_then(|v| v.as_str()) else {
        return;
    };
    let params = val.get("params").unwrap_or(&Value::Null);

    // Server->client JSON-RPC request (approvals / user input).
    if has_id {
        let rpc_id = val.get("id").cloned().unwrap_or(Value::Null);
        if let Some(interaction) = parse_codex_approval_request(&rpc_id, method, params) {
            worker
                .pending_approvals
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(
                    interaction.id.clone(),
                    PendingCodexApproval {
                        rpc_id,
                        method: method.to_owned(),
                    },
                );
            {
                let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
                log.pending = Some(interaction.clone());
                log.push_event(AgentEventKind::Other {
                    bb_type: format!("approval_request: {}", interaction.summary),
                });
            }
            worker.emit_activity(Activity::Blocked { interaction }, false);
        }
        return;
    }

    // Server->client JSON-RPC notification.
    match method {
        "turn/started" => {
            let turn_id = params
                .get("turn")
                .and_then(|t| t.get("id"))
                .or_else(|| params.get("turnId"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty());
            if let Some(tid) = turn_id {
                *worker
                    .active_turn_id
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(tid.to_owned());
                if worker
                    .started_turns
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(tid.to_owned())
                {
                    worker
                        .log
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push_event(AgentEventKind::TurnStarted);
                }
            }
            let needs_working = {
                let log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
                !matches!(log.activity, Activity::Working)
            };
            if needs_working {
                worker.emit_activity(Activity::Working, false);
            }
        }
        "item/started" => {
            if let Some(item) = params.get("item")
                && let Some(ev_kind) = normalize_codex_item_started(item)
            {
                worker
                    .log
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push_event(ev_kind);
            }
        }
        "item/agentMessage/delta" => {
            if let Some(delta) = params.get("delta").and_then(|v| v.as_str())
                && !delta.is_empty()
            {
                if let Some(item_id) = params.get("itemId").and_then(|v| v.as_str()) {
                    worker
                        .streamed_items
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(item_id.to_owned());
                }
                worker
                    .log
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push_event(AgentEventKind::AssistantText {
                        text: delta.to_owned(),
                    });
            }
        }
        "item/completed" => {
            if let Some(item) = params.get("item") {
                let item_id = item.get("id").and_then(|v| v.as_str());
                let already_streamed = item_id.is_some_and(|id| {
                    worker
                        .streamed_items
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(id)
                });
                if let Some(ev_kind) = normalize_codex_item_completed(item, already_streamed) {
                    worker
                        .log
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push_event(ev_kind);
                }
            }
        }
        "thread/tokenUsage/updated" => {
            if let Some(usage) = parse_codex_token_usage(params) {
                let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
                log.usage = Some(usage.clone());
                log.push_event(AgentEventKind::Usage { usage });
            }
        }
        "error" => {
            let will_retry = params
                .get("willRetry")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !will_retry && let Some(err) = params.get("error").filter(|v| !v.is_null()) {
                *worker.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(err.clone());
                let msg = err
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Codex error");
                worker
                    .log
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push_event(AgentEventKind::Other {
                        bb_type: format!("error: {msg}"),
                    });
            }
        }
        "turn/completed" => {
            *worker
                .active_turn_id
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = None;
            let prior_err = worker
                .last_error
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            {
                let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
                log.pending = None;
                log.push_event(AgentEventKind::TurnEnded);
            }
            let activity = classify_codex_turn(params, prior_err.as_ref());
            worker.emit_activity(activity, true);
        }
        _ => {}
    }
}

fn normalize_codex_item_started(item: &Value) -> Option<AgentEventKind> {
    match item.get("type").and_then(|v| v.as_str())? {
        "commandExecution" => {
            let summary = item
                .get("command")
                .and_then(|v| v.as_str())
                .map(|s| truncate_summary(s, 240))
                .unwrap_or_default();
            Some(AgentEventKind::ToolCall {
                name: "command_execution".into(),
                summary,
            })
        }
        "fileChange" => {
            let summary = summarize_file_changes(item);
            Some(AgentEventKind::ToolCall {
                name: "file_change".into(),
                summary,
            })
        }
        "mcpToolCall" | "dynamicToolCall" => {
            let name = item
                .get("tool")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("tool")
                .to_owned();
            let summary = item
                .get("arguments")
                .filter(|v| !v.is_null())
                .map(|v| truncate_summary(&v.to_string(), 240))
                .unwrap_or_default();
            Some(AgentEventKind::ToolCall { name, summary })
        }
        "webSearch" => {
            let summary = item
                .get("query")
                .and_then(|v| v.as_str())
                .map(|s| truncate_summary(s, 240))
                .unwrap_or_default();
            Some(AgentEventKind::ToolCall {
                name: "web_search".into(),
                summary,
            })
        }
        _ => None,
    }
}

fn normalize_codex_item_completed(item: &Value, already_streamed: bool) -> Option<AgentEventKind> {
    let status = item
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("completed");
    match item.get("type").and_then(|v| v.as_str())? {
        "agentMessage" => {
            if already_streamed {
                return None;
            }
            let text = item
                .get("text")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())?;
            Some(AgentEventKind::AssistantText {
                text: text.to_owned(),
            })
        }
        "commandExecution" => {
            let exit_ok = item
                .get("exitCode")
                .and_then(|v| v.as_i64())
                .is_none_or(|c| c == 0);
            let ok = status == "completed" && exit_ok;
            let summary = item
                .get("aggregatedOutput")
                .or_else(|| item.get("command"))
                .and_then(|v| v.as_str())
                .map(|s| truncate_summary(s, 240))
                .unwrap_or_default();
            Some(AgentEventKind::ToolResult {
                name: "command_execution".into(),
                ok,
                summary,
            })
        }
        "fileChange" => {
            let ok = status == "completed";
            Some(AgentEventKind::ToolResult {
                name: "file_change".into(),
                ok,
                summary: summarize_file_changes(item),
            })
        }
        "mcpToolCall" => {
            let name = item
                .get("tool")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("mcp_tool")
                .to_owned();
            let ok = status == "completed" && item.get("error").is_none_or(|e| e.is_null());
            let summary = item
                .get("error")
                .filter(|v| !v.is_null())
                .or_else(|| item.get("result").filter(|v| !v.is_null()))
                .map(|v| truncate_summary(&v.to_string(), 240))
                .unwrap_or_default();
            Some(AgentEventKind::ToolResult { name, ok, summary })
        }
        "dynamicToolCall" => {
            let name = item
                .get("tool")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("dynamic_tool")
                .to_owned();
            let ok = item
                .get("success")
                .and_then(|v| v.as_bool())
                .unwrap_or(status == "completed");
            let summary = item
                .get("contentItems")
                .map(|v| truncate_summary(&v.to_string(), 240))
                .unwrap_or_default();
            Some(AgentEventKind::ToolResult { name, ok, summary })
        }
        "webSearch" => {
            let summary = item
                .get("query")
                .and_then(|v| v.as_str())
                .map(|s| truncate_summary(s, 240))
                .unwrap_or_default();
            Some(AgentEventKind::ToolResult {
                name: "web_search".into(),
                ok: true,
                summary,
            })
        }
        _ => None,
    }
}

fn summarize_file_changes(item: &Value) -> String {
    if let Some(arr) = item.get("changes").and_then(|v| v.as_array()) {
        let paths: Vec<&str> = arr
            .iter()
            .filter_map(|c| c.get("path").and_then(|v| v.as_str()))
            .collect();
        if !paths.is_empty() {
            return truncate_summary(&paths.join(", "), 240);
        }
    }
    String::new()
}

/// `rex_wave_waves::AgentHost` adapter backed by [`CodexBridge`].
#[derive(Clone)]
pub struct CodexHost(pub Arc<CodexBridge>);

impl AgentHost for CodexHost {
    fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<HostThreadId, HostError>> {
        let bridge = self.0.clone();
        Box::pin(async move { bridge.spawn_worker(req).await })
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
        Box::pin(async move { bridge.tell_worker(&thread, &body, mode).await })
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
                .resolve_worker(&thread, &interaction, &resolution)
                .await
        })
    }

    fn retire(&self, thread: &HostThreadId) -> BoxFut<'_, Result<(), HostError>> {
        let bridge = self.0.clone();
        let thread = thread.clone();
        Box::pin(async move { bridge.retire_worker(&thread).await })
    }

    fn check_live(
        &self,
        thread: &HostThreadId,
    ) -> BoxFut<'_, Result<Option<Observation>, HostError>> {
        let bridge = self.0.clone();
        let thread = thread.clone();
        Box::pin(async move { bridge.check_worker_live(&thread).await })
    }
}

/// `rex_wave_runtime::AgentFeed` adapter backed by [`CodexBridge`].
#[derive(Clone)]
pub struct CodexFeed(pub Arc<CodexBridge>);

impl AgentFeed for CodexBridge {
    fn page(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        after: Option<HostSeq>,
        limit: u32,
    ) -> BoxFut<'_, Result<ThreadPageView, ApiError>> {
        let res = self.lookup_log(wave, role).map(|log| {
            let guard = log.lock().unwrap_or_else(|e| e.into_inner());
            guard.page(after, limit)
        });
        Box::pin(async move { res })
    }

    fn live(&self, wave: &WaveSlug, role: &RoleName) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        let res = self
            .lookup_log(wave, role)
            .map(ThreadEventLog::subscribe_live);
        Box::pin(async move { res })
    }
}

impl AgentFeed for CodexFeed {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_approval_and_turn_classification_match_schema() {
        assert_eq!(
            codex_approval_and_sandbox(PermissionMode::AcceptEdits),
            ("on-request", "workspace-write")
        );
        assert_eq!(
            codex_approval_and_sandbox(PermissionMode::Full),
            ("never", "danger-full-access")
        );

        let cmd_req = parse_codex_approval_request(
            &json!(7),
            "item/commandExecution/requestApproval",
            &json!({"itemId": "it_1", "command": "cargo check"}),
        )
        .unwrap();
        assert_eq!(cmd_req.id.as_str(), "it_1");
        assert!(matches!(cmd_req.kind, InteractionKind::Permission));

        assert_eq!(
            encode_codex_approval_result(
                "item/commandExecution/requestApproval",
                &InteractionResolution::AllowOnce {
                    granted_permissions: None,
                },
            )
            .unwrap(),
            json!({"decision": "accept"})
        );
        assert_eq!(
            encode_codex_approval_result(
                "item/fileChange/requestApproval",
                &InteractionResolution::Deny,
            )
            .unwrap(),
            json!({"decision": "decline"})
        );

        assert!(matches!(
            classify_codex_turn(
                &json!({"turn": {"status": "failed", "error": {"message": "denied", "codexErrorInfo": "cyberPolicy"}}}),
                None
            ),
            Activity::Refused { .. }
        ));
        assert!(matches!(
            classify_codex_turn(
                &json!({"turn": {"status": "failed", "error": {"message": "timeout", "codexErrorInfo": "internalServerError"}}}),
                None
            ),
            Activity::Errored { ref error } if error == "timeout"
        ));
        assert_eq!(
            classify_codex_turn(&json!({"turn": {"status": "completed"}}), None),
            Activity::Idle
        );
    }

    #[test]
    fn codex_thread_id_roundtrips() {
        let w = WaveSlug::parse("wave-two").unwrap();
        let r = RoleName::parse("author").unwrap();
        let tid = codex_thread_id(&w, &r, 3).unwrap();
        assert_eq!(tid.as_str(), "codex:wave-two:author:a3");
        assert_eq!(parse_codex_thread_id(&tid), Some((w, r, 3)));
    }
}
