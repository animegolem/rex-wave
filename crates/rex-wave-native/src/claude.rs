//! Native Claude Code `stream-json` runner (`ClaudeBridge`, `ClaudeHost`, `ClaudeFeed`).

use crate::common::{
    BoundedLine, ErasedBackend, MAX_STDOUT_LINE_BYTES, ObserverItem, StdinLine, ThreadEventLog,
    WorkerRegistry, flush_observer, format_thread_id, parse_prefixed_thread_id,
    provision_native_worker_token, read_bounded_line, reject_non_empty_granted_permissions,
    revoke_native_worker_token, scrub_daemon_env, spawn_stdin_writer, spawn_worker_observer,
    truncate_summary, write_stdin_line,
};
use crate::process::{
    DEFAULT_RETIRE_GRACE, LiveChild, PgidSlot, claim_and_kill_pgid, new_pgid_slot,
    spawn_in_process_group, spawn_stderr_drainer, terminate_process_group,
};
use crate::workspace::resolve_worker_workspace;
use futures_util::future::join_all;
use rex_wave_api::config::RexConfig;
use rex_wave_api::read::{
    AgentEventKind, ContextCategory, ContextCategoryKind, ThreadContextUsage, ThreadPageView,
};
use rex_wave_api::room::Body;
use rex_wave_api::wave::{
    Activity, InteractionKind, InteractionResolution, Observation, PendingInteraction,
    PermissionMode, SteerDeliveryMode, is_refusal_error_text, is_refusal_stop_reason,
};
use rex_wave_api::{
    ApiError, CLAUDE_THREAD_PREFIX, FrameStream, HostInteractionId, HostSeq, HostThreadId, ModelId,
    Principal, RoleName, SystemPart, WaveSlug,
};
use rex_wave_runtime::{AgentFeed, BoxFut, BridgeBackend, Router};
use rex_wave_transport::WORKER_THREAD_ENV;
use rex_wave_waves::{AgentHost, HostError, SpawnRequest, expand_tilde};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;
use tokio::io::BufReader;
use tokio::process::Command;
use tokio::sync::{mpsc, watch};

pub const CLAUDE_BIN_ENV: &str = "REX_WAVE_CLAUDE_BIN";
const MAX_STDERR_BYTES: usize = 8192;

#[derive(Clone, Debug)]
pub struct ClaudeRunnerConfig {
    pub bin: PathBuf,
    pub state_dir: Option<PathBuf>,
    pub socket_path: Option<PathBuf>,
    pub mcp_config: Option<String>,
    pub rex_wave_bin: Option<PathBuf>,
    pub bb_data_dir: Option<PathBuf>,
    pub retire_grace: Duration,
    pub system_principal: Principal,
    pub extra_args: Vec<String>,
    pub config_rx: Option<watch::Receiver<RexConfig>>,
}

impl Default for ClaudeRunnerConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl ClaudeRunnerConfig {
    pub fn from_env() -> Self {
        let bin = std::env::var_os(CLAUDE_BIN_ENV)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("claude"));
        Self {
            bin,
            state_dir: None,
            socket_path: None,
            mcp_config: None,
            rex_wave_bin: None,
            bb_data_dir: rex_wave_waves::default_bb_data_dir(),
            retire_grace: DEFAULT_RETIRE_GRACE,
            system_principal: Principal::System {
                part: SystemPart::WaveConductor,
            },
            extra_args: Vec::new(),
            config_rx: None,
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

    pub fn with_mcp_config(mut self, mcp_config: impl Into<String>) -> Self {
        self.mcp_config = Some(mcp_config.into());
        self
    }

    pub fn with_rex_wave_bin(mut self, bin: impl Into<PathBuf>) -> Self {
        self.rex_wave_bin = Some(bin.into());
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

    pub fn with_system_principal(mut self, principal: Principal) -> Self {
        self.system_principal = principal;
        self
    }

    pub fn with_extra_args(mut self, args: Vec<String>) -> Self {
        self.extra_args = args;
        self
    }

    pub fn with_config_watch(mut self, rx: watch::Receiver<RexConfig>) -> Self {
        self.config_rx = Some(rx);
        self
    }

    pub fn effective_bin(&self) -> PathBuf {
        if let Some(ref rx) = self.config_rx
            && let Some(raw) = rx
                .borrow()
                .harnesses
                .claude
                .binary
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
        {
            return expand_tilde(raw);
        }
        self.bin.clone()
    }
}

pub fn claude_thread_id(
    wave: &WaveSlug,
    role: &RoleName,
    attempt: u32,
) -> Result<HostThreadId, HostError> {
    format_thread_id(CLAUDE_THREAD_PREFIX, wave, role, attempt)
}

pub fn parse_claude_thread_id(thread: &HostThreadId) -> Option<(WaveSlug, RoleName, u32)> {
    parse_prefixed_thread_id(CLAUDE_THREAD_PREFIX, thread)
}

pub fn claude_permission_mode(mode: PermissionMode) -> &'static str {
    match mode {
        PermissionMode::AcceptEdits => "acceptEdits",
        PermissionMode::Auto => "auto",
        PermissionMode::Full => "bypassPermissions",
    }
}

fn resolved_mcp_config(cfg: &ClaudeRunnerConfig) -> Option<String> {
    if let Some(ref explicit) = cfg.mcp_config
        && !explicit.trim().is_empty()
    {
        return Some(explicit.clone());
    }
    let (Some(bin), Some(sock)) = (&cfg.rex_wave_bin, &cfg.socket_path) else {
        return None;
    };
    let payload = json!({
        "mcpServers": {
            "rex-wave": {
                "command": bin.to_string_lossy(),
                "args": ["--socket", sock.to_string_lossy(), "mcp", "--stdio"],
            }
        }
    });
    serde_json::to_string(&payload).ok()
}

pub fn build_claude_args(
    cfg: &ClaudeRunnerConfig,
    req: &SpawnRequest,
    resume_session_id: Option<&str>,
) -> Vec<String> {
    let mut args = vec![
        "-p".to_owned(),
        "--input-format".to_owned(),
        "stream-json".to_owned(),
        "--output-format".to_owned(),
        "stream-json".to_owned(),
        "--verbose".to_owned(),
    ];

    if let Some(sid) = resume_session_id.map(str::trim).filter(|s| !s.is_empty()) {
        args.push("--resume".to_owned());
        args.push(sid.to_owned());
    }

    if let Some(ref model) = req.agent.model {
        args.push("--model".to_owned());
        args.push(model.as_str().to_owned());
    }

    if let Some(reasoning) = req.agent.reasoning.as_deref().map(str::trim)
        && !reasoning.is_empty()
    {
        args.push("--effort".to_owned());
        args.push(reasoning.to_owned());
    }

    args.push("--permission-mode".to_owned());
    args.push(claude_permission_mode(req.agent.permission).to_owned());

    if req.agent.permission != PermissionMode::Full {
        args.push("--permission-prompt-tool".to_owned());
        args.push("stdio".to_owned());
    }

    if let Some(mcp_cfg) = resolved_mcp_config(cfg) {
        args.push("--mcp-config".to_owned());
        args.push(mcp_cfg);
    }

    args.extend(cfg.extra_args.iter().cloned());
    args
}

pub fn encode_claude_user_turn(text: &str) -> String {
    json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": [
                { "type": "text", "text": text }
            ]
        }
    })
    .to_string()
}

fn encode_claude_interrupt_request(seq: u64) -> String {
    json!({
        "type": "control_request",
        "request_id": format!("rex-int-{seq}"),
        "request": {
            "subtype": "interrupt",
        }
    })
    .to_string()
}

fn is_explicit_session_suggestion(entry: &Value) -> bool {
    entry.is_object()
        && entry
            .get("destination")
            .and_then(Value::as_str)
            .is_some_and(|d| d.trim() == "session")
}

fn filter_session_permission_suggestions(permission_suggestions: Option<&Value>) -> Vec<Value> {
    let Some(arr) = permission_suggestions.and_then(Value::as_array) else {
        return Vec::new();
    };
    arr.iter()
        .filter(|entry| is_explicit_session_suggestion(entry))
        .cloned()
        .collect()
}

fn validate_claude_request_answer(value: &Value) -> Result<Value, HostError> {
    let Some(obj) = value.as_object() else {
        return Err(HostError::Rejected(
            "Claude stream-json RequestAnswer payload must be a JSON object".into(),
        ));
    };
    let Some(behavior) = obj.get("behavior").and_then(Value::as_str) else {
        return Err(HostError::Rejected(
            "Claude stream-json RequestAnswer requires string field 'behavior' ('allow' or 'deny')"
                .into(),
        ));
    };
    match behavior {
        "allow" => {
            if !obj.get("updatedInput").is_some_and(Value::is_object) {
                return Err(HostError::Rejected(
                    "Claude stream-json allow RequestAnswer requires object field 'updatedInput'"
                        .into(),
                ));
            }
            if let Some(up) = obj.get("updatedPermissions") {
                let Some(arr) = up.as_array() else {
                    return Err(HostError::Rejected(
                        "Claude stream-json RequestAnswer 'updatedPermissions' must be an array"
                            .into(),
                    ));
                };
                if arr
                    .iter()
                    .any(|entry| !is_explicit_session_suggestion(entry))
                {
                    return Err(HostError::Rejected(
                        "Claude stream-json RequestAnswer 'updatedPermissions' entries must explicitly target destination 'session'"
                            .into(),
                    ));
                }
            }
            Ok(value.clone())
        }
        "deny" => {
            if obj
                .get("message")
                .and_then(Value::as_str)
                .is_none_or(|m| m.trim().is_empty())
            {
                return Err(HostError::Rejected(
                    "Claude stream-json deny RequestAnswer requires non-empty string field 'message'"
                        .into(),
                ));
            }
            Ok(value.clone())
        }
        other => Err(HostError::Rejected(format!(
            "unsupported Claude stream-json RequestAnswer behavior '{other}'"
        ))),
    }
}

pub fn encode_claude_control_response(
    request_id: &str,
    resolution: &InteractionResolution,
    orig_input: &Value,
    permission_suggestions: Option<&Value>,
) -> Result<String, HostError> {
    let inner = match resolution {
        InteractionResolution::AllowOnce {
            granted_permissions,
        } => {
            reject_non_empty_granted_permissions(
                granted_permissions.as_ref(),
                "Claude stream-json",
            )?;
            let input_obj = if orig_input.is_object() {
                orig_input.clone()
            } else {
                json!({})
            };
            json!({
                "behavior": "allow",
                "updatedInput": input_obj,
            })
        }
        InteractionResolution::AllowForSession {
            granted_permissions,
        } => {
            reject_non_empty_granted_permissions(
                granted_permissions.as_ref(),
                "Claude stream-json",
            )?;
            let session_suggestions = filter_session_permission_suggestions(permission_suggestions);
            if session_suggestions.is_empty() {
                return Err(HostError::Rejected(
                    "Claude control_request did not offer session-scoped permission_suggestions; use AllowOnce or Deny"
                        .into(),
                ));
            }
            let input_obj = if orig_input.is_object() {
                orig_input.clone()
            } else {
                json!({})
            };
            let mut obj = Map::new();
            obj.insert("behavior".into(), Value::String("allow".into()));
            obj.insert("updatedInput".into(), input_obj);
            obj.insert(
                "updatedPermissions".into(),
                Value::Array(session_suggestions),
            );
            Value::Object(obj)
        }
        InteractionResolution::Deny => json!({
            "behavior": "deny",
            "message": "Denied by operator",
        }),
        InteractionResolution::RequestAnswer { value } => validate_claude_request_answer(value)?,
        InteractionResolution::UserAnswer { .. }
        | InteractionResolution::PluginSubmitted { .. } => {
            return Err(HostError::Rejected(
                "Claude stream-json control_response supports AllowOnce, AllowForSession, Deny, or RequestAnswer"
                    .into(),
            ));
        }
    };

    Ok(json!({
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": request_id,
            "response": inner,
        }
    })
    .to_string())
}

fn encode_claude_control_error(request_id: &str, message: &str) -> String {
    json!({
        "type": "control_response",
        "response": {
            "subtype": "error",
            "request_id": request_id,
            "error": message,
        }
    })
    .to_string()
}

fn select_claude_model_usage_entry<'a>(
    val: &'a Value,
    preferred_model: Option<&str>,
) -> Option<(&'a str, &'a Map<String, Value>)> {
    let map = val.get("modelUsage")?.as_object()?;
    if let Some(pref) = preferred_model.map(str::trim).filter(|s| !s.is_empty()) {
        if let Some((k, v)) = map.get_key_value(pref)
            && let Some(obj) = v.as_object()
        {
            return Some((k.as_str(), obj));
        }
        if let Some((k, v)) = map
            .iter()
            .find(|(k, _)| k.starts_with(pref) || pref.starts_with(k.as_str()))
            && let Some(obj) = v.as_object()
        {
            return Some((k.as_str(), obj));
        }
    }
    map.iter()
        .filter_map(|(k, v)| v.as_object().map(|obj| (k.as_str(), obj)))
        .max_by_key(|(_, obj)| {
            let ctx = obj
                .get("contextWindow")
                .or_else(|| obj.get("context_window"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let tok = obj
                .get("inputTokens")
                .or_else(|| obj.get("input_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .saturating_add(
                    obj.get("outputTokens")
                        .or_else(|| obj.get("output_tokens"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                );
            (ctx, tok)
        })
}

pub fn parse_claude_usage(val: &Value) -> Option<ThreadContextUsage> {
    parse_claude_usage_for_model(val, None)
}

pub fn parse_claude_usage_for_model(
    val: &Value,
    preferred_model: Option<&str>,
) -> Option<ThreadContextUsage> {
    let usage_obj = val.get("usage").and_then(|v| v.as_object());
    let model_usage_entry = select_claude_model_usage_entry(val, preferred_model).map(|(_, o)| o);

    if usage_obj.is_none() && model_usage_entry.is_none() {
        return None;
    }

    let get_u64 = |snake: &str, camel: &str| -> u64 {
        usage_obj
            .and_then(|u| u.get(snake).or_else(|| u.get(camel)))
            .or_else(|| model_usage_entry.and_then(|u| u.get(camel).or_else(|| u.get(snake))))
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    };

    let input = get_u64("input_tokens", "inputTokens");
    let cache_read = get_u64("cache_read_input_tokens", "cacheReadInputTokens");
    let cache_write = get_u64("cache_creation_input_tokens", "cacheCreationInputTokens");
    let output = get_u64("output_tokens", "outputTokens");

    let used_tokens = input
        .saturating_add(cache_read)
        .saturating_add(cache_write)
        .saturating_add(output);

    let model_context_window = model_usage_entry
        .and_then(|u| {
            u.get("contextWindow")
                .or_else(|| u.get("context_window"))
                .and_then(|v| v.as_u64())
        })
        .or_else(|| {
            val.get("context_window")
                .or_else(|| val.get("model_context_window"))
                .and_then(|v| v.as_u64())
        })
        .unwrap_or(0);

    let cost_usd = val
        .get("total_cost_usd")
        .or_else(|| val.get("cost_usd"))
        .and_then(|v| v.as_f64())
        .or_else(|| {
            model_usage_entry.and_then(|u| {
                u.get("costUSD")
                    .or_else(|| u.get("cost_usd"))
                    .and_then(|v| v.as_f64())
            })
        });

    let mut categories = Vec::new();
    if input > 0 {
        categories.push(ContextCategory {
            id: "input".into(),
            label: "Input".into(),
            kind: ContextCategoryKind::Used,
            tokens: input,
        });
    }
    if cache_read > 0 {
        categories.push(ContextCategory {
            id: "cache_read".into(),
            label: "Cache read".into(),
            kind: ContextCategoryKind::Reserved,
            tokens: cache_read,
        });
    }
    if cache_write > 0 {
        categories.push(ContextCategory {
            id: "cache_write".into(),
            label: "Cache write".into(),
            kind: ContextCategoryKind::Used,
            tokens: cache_write,
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
    if model_context_window > used_tokens {
        categories.push(ContextCategory {
            id: "free".into(),
            label: "Free".into(),
            kind: ContextCategoryKind::Free,
            tokens: model_context_window - used_tokens,
        });
    }

    Some(ThreadContextUsage {
        used_tokens,
        model_context_window,
        estimated: false,
        auto_compact_at_tokens: None,
        cost_usd,
        categories,
    })
}

fn merge_assistant_and_result_usage(
    mut occupancy: ThreadContextUsage,
    result_usage: &ThreadContextUsage,
) -> ThreadContextUsage {
    if result_usage.cost_usd.is_some() {
        occupancy.cost_usd = result_usage.cost_usd;
    }
    if occupancy.model_context_window == 0 && result_usage.model_context_window > 0 {
        occupancy.model_context_window = result_usage.model_context_window;
        occupancy.categories.retain(|c| c.id != "free");
        if occupancy.model_context_window > occupancy.used_tokens {
            occupancy.categories.push(ContextCategory {
                id: "free".into(),
                label: "Free".into(),
                kind: ContextCategoryKind::Free,
                tokens: occupancy.model_context_window - occupancy.used_tokens,
            });
        }
    }
    occupancy
}

pub fn extract_claude_runtime_meta(val: &Value) -> (Option<ModelId>, Option<String>) {
    extract_claude_runtime_meta_with_hint(val, None, true)
}

fn extract_claude_runtime_meta_with_hint(
    val: &Value,
    preferred_model: Option<&str>,
    allow_model_usage_fallback: bool,
) -> (Option<ModelId>, Option<String>) {
    let msg = val.get("message");
    let explicit_model = val
        .get("model")
        .and_then(|v| v.as_str())
        .or_else(|| msg.and_then(|m| m.get("model")).and_then(|v| v.as_str()))
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let model_raw = explicit_model.or_else(|| {
        allow_model_usage_fallback
            .then(|| select_claude_model_usage_entry(val, preferred_model).map(|(k, _)| k))
            .flatten()
    });
    let model = model_raw.and_then(|s| ModelId::parse(s).ok());

    let account = val
        .get("actual_account")
        .or_else(|| val.get("actualAccount"))
        .or_else(|| val.get("account_email"))
        .or_else(|| val.get("accountEmail"))
        .or_else(|| val.get("account"))
        .or_else(|| val.get("billing_account"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);

    (model, account)
}

pub fn classify_claude_result(val: &Value, prior_refusal: Option<&str>) -> Activity {
    let stop_reason = val
        .get("stop_reason")
        .or_else(|| val.get("stopReason"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let subtype = val
        .get("subtype")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let is_error = val
        .get("is_error")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let text_msg = val
        .get("error")
        .and_then(|v| v.as_str())
        .or_else(|| {
            val.get("errors")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_str())
        })
        .or_else(|| val.get("result").and_then(|v| v.as_str()))
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let refusal_reason = stop_reason
        .filter(|sr| is_refusal_stop_reason(sr))
        .or_else(|| subtype.filter(|st| is_refusal_stop_reason(st)))
        .or(prior_refusal);

    if let Some(sr) = refusal_reason {
        let reason = match text_msg {
            Some(m) if !m.to_ascii_lowercase().contains(&sr.to_ascii_lowercase()) => {
                format!("{sr}: {m}")
            }
            Some(m) => m.to_owned(),
            None => sr.to_owned(),
        };
        return Activity::Refused { reason };
    }

    if let Some(msg) = text_msg
        && (is_error || subtype.is_some_and(|s| s.starts_with("error")))
        && is_refusal_error_text(msg)
    {
        return Activity::Refused {
            reason: msg.to_owned(),
        };
    }

    if is_error || subtype.is_some_and(|s| s.starts_with("error")) {
        let error = text_msg
            .or(subtype)
            .unwrap_or("Claude turn failed")
            .to_owned();
        return Activity::Errored { error };
    }

    Activity::Idle
}

fn extract_tool_result_summary(content: Option<&Value>) -> String {
    let Some(val) = content else {
        return String::new();
    };
    if let Some(s) = val.as_str() {
        return truncate_summary(s, 240);
    }
    if let Some(arr) = val.as_array() {
        let mut parts = Vec::new();
        for item in arr {
            if let Some(t) = item.get("text").and_then(|v| v.as_str()) {
                parts.push(t);
            } else if let Some(s) = item.as_str() {
                parts.push(s);
            }
        }
        if !parts.is_empty() {
            return truncate_summary(&parts.join("\n"), 240);
        }
    }
    truncate_summary(&val.to_string(), 240)
}

#[derive(Clone)]
struct PendingClaudeControl {
    interaction: PendingInteraction,
    orig_input: Value,
    permission_suggestions: Option<Value>,
}

struct ClaudeTurnQueue {
    in_flight: bool,
    queued: VecDeque<String>,
}

struct ClaudeWorkerState {
    thread: HostThreadId,
    attempt: u32,
    req: SpawnRequest,
    cwd: PathBuf,
    state_dir: Option<PathBuf>,
    worker_token: Option<String>,
    log: Arc<Mutex<ThreadEventLog>>,
    observer: mpsc::UnboundedSender<ObserverItem>,
    retiring: AtomicBool,
    retired: AtomicBool,
    unresumable_dead: AtomicBool,
    turn_queue: Mutex<ClaudeTurnQueue>,
    generation: AtomicU64,
    interrupt_seq: AtomicU64,
    session_id: Mutex<Option<String>>,
    last_model: Mutex<Option<ModelId>>,
    last_account: Mutex<Option<String>>,
    stdin_tx: Mutex<Option<(u64, mpsc::UnboundedSender<StdinLine>)>>,
    live_child: tokio::sync::Mutex<Option<LiveChild>>,
    pending_controls: Mutex<VecDeque<PendingClaudeControl>>,
    tool_names: Mutex<HashMap<String, String>>,
    turn_lock: tokio::sync::Mutex<()>,
}

impl ClaudeWorkerState {
    fn preferred_model_hint(&self) -> Option<String> {
        self.last_model
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|m| m.as_str().to_owned())
            .or_else(|| self.req.agent.model.as_ref().map(|m| m.as_str().to_owned()))
    }

    fn emit_activity(&self, activity: Activity, include_settled: bool) {
        let (host_seq, settled_seq) = {
            let mut log = self.log.lock().unwrap_or_else(|e| e.into_inner());
            log.activity = activity.clone();
            match &activity {
                Activity::Blocked { interaction } => {
                    log.pending = Some(interaction.clone());
                }
                _ => {
                    log.pending = None;
                }
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

    fn emit_runtime_meta_if_changed(
        &self,
        actual_model: Option<ModelId>,
        actual_account: Option<String>,
    ) {
        let mut emit_model = None;
        if let Some(m) = actual_model {
            let mut slot = self.last_model.lock().unwrap_or_else(|e| e.into_inner());
            if slot.as_ref() != Some(&m) {
                *slot = Some(m.clone());
                emit_model = Some(m);
            }
        }
        let mut emit_account = None;
        if let Some(a) = actual_account {
            let mut slot = self.last_account.lock().unwrap_or_else(|e| e.into_inner());
            if slot.as_ref() != Some(&a) {
                *slot = Some(a.clone());
                emit_account = Some(a);
            }
        }
        if emit_model.is_some() || emit_account.is_some() {
            let _ = self
                .observer
                .send(ObserverItem::Observe(Observation::runtime_meta(
                    emit_model,
                    emit_account,
                    Some(self.thread.clone()),
                    Some(self.attempt),
                )));
        }
    }
}

struct ClaudeSpawnGuard {
    state_dir: Option<PathBuf>,
    thread: HostThreadId,
    worker: Arc<ClaudeWorkerState>,
    pgid_slot: PgidSlot,
    armed: bool,
}

impl ClaudeSpawnGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ClaudeSpawnGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.worker.retiring.store(true, Ordering::Release);
        self.worker
            .stdin_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        claim_and_kill_pgid(&self.pgid_slot);
        revoke_native_worker_token(self.state_dir.as_deref(), &self.thread);
    }
}

/// Core bridge managing native Claude `stream-json` worker child processes.
pub struct ClaudeBridge {
    cfg: ClaudeRunnerConfig,
    self_weak: Weak<Self>,
    backend: OnceLock<Arc<dyn ErasedBackend>>,
    registry: Mutex<WorkerRegistry<ClaudeWorkerState>>,
    spawn_locks: Mutex<HashMap<HostThreadId, Arc<tokio::sync::Mutex<()>>>>,
}

impl ClaudeBridge {
    pub fn new(cfg: ClaudeRunnerConfig) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            cfg,
            self_weak: weak.clone(),
            backend: OnceLock::new(),
            registry: Mutex::new(WorkerRegistry::default()),
            spawn_locks: Mutex::new(HashMap::new()),
        })
    }

    pub fn config(&self) -> &ClaudeRunnerConfig {
        &self.cfg
    }

    pub fn attach_backend<B: BridgeBackend>(&self, backend: B) {
        let _ = self.backend.set(Arc::new(backend));
    }

    pub fn attach_router(&self, router: Arc<Router>) {
        self.attach_backend(router);
    }

    fn thread_spawn_lock(&self, thread: &HostThreadId) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.spawn_locks.lock().unwrap_or_else(|e| e.into_inner());
        locks
            .entry(thread.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Gracefully terminates all live Claude worker process groups concurrently and revokes their tokens.
    pub async fn shutdown(&self) {
        let threads = self
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .all_threads();
        join_all(threads.iter().map(|tid| self.retire_worker(tid))).await;
    }

    fn spawn_process_for_worker(
        &self,
        worker: &Arc<ClaudeWorkerState>,
        resume_session_id: Option<&str>,
    ) -> Result<(LiveChild, PgidSlot, mpsc::UnboundedSender<StdinLine>), HostError> {
        let args = build_claude_args(&self.cfg, &worker.req, resume_session_id);
        let mut cmd = Command::new(self.cfg.effective_bin());
        scrub_daemon_env(&mut cmd);
        cmd.args(&args)
            .current_dir(&worker.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("BELL_GUARD", "1")
            .env(WORKER_THREAD_ENV, worker.thread.as_str());

        if let Some(ref sock) = self.cfg.socket_path {
            cmd.env("REX_WAVE_SOCKET", sock);
        }
        if let Some(ref tok) = worker.worker_token {
            cmd.env("REX_WAVE_TOKEN", tok);
        }

        let (mut child, pgid) = spawn_in_process_group(&mut cmd)?;
        let pgid_slot = new_pgid_slot(pgid);
        let generation = worker.generation.fetch_add(1, Ordering::AcqRel) + 1;
        let stdin_opt = child.stdin.take();
        let stdout_opt = child.stdout.take();
        let stderr_opt = child.stderr.take();
        let live = LiveChild::with_slot(child, pgid_slot.clone(), generation);

        let (Some(stdin), Some(stdout), Some(stderr)) = (stdin_opt, stdout_opt, stderr_opt) else {
            drop(live);
            return Err(HostError::Unreachable(
                "Claude child stdio pipes unavailable".into(),
            ));
        };

        let stdin_tx = spawn_stdin_writer(stdin, "Claude");
        let stderr_task = spawn_stderr_drainer(stderr, MAX_STDERR_BYTES);

        let reader_worker = worker.clone();
        let reader_pgid = pgid_slot.clone();
        let weak_bridge = self.self_weak.clone();
        tokio::spawn(async move {
            run_claude_stdout_reader(
                weak_bridge,
                reader_worker,
                stdout,
                stderr_task,
                generation,
                reader_pgid,
            )
            .await;
        });

        Ok((live, pgid_slot, stdin_tx))
    }

    async fn spawn_worker(&self, req: SpawnRequest) -> Result<HostThreadId, HostError> {
        let thread = claude_thread_id(&req.wave, &req.role, req.attempt)?;
        let spawn_lock = self.thread_spawn_lock(&thread);
        let _spawn_guard = spawn_lock.lock().await;

        if let Some(existing) = self
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_thread(&thread)
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

        let initial_prompt = req.prompt.as_str().to_owned();
        let worker = Arc::new(ClaudeWorkerState {
            thread: thread.clone(),
            attempt: req.attempt,
            req,
            cwd,
            state_dir: self.cfg.state_dir.clone(),
            worker_token,
            log,
            observer,
            retiring: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            unresumable_dead: AtomicBool::new(false),
            turn_queue: Mutex::new(ClaudeTurnQueue {
                in_flight: true,
                queued: VecDeque::new(),
            }),
            generation: AtomicU64::new(0),
            interrupt_seq: AtomicU64::new(0),
            session_id: Mutex::new(None),
            last_model: Mutex::new(None),
            last_account: Mutex::new(None),
            stdin_tx: Mutex::new(None),
            live_child: tokio::sync::Mutex::new(None),
            pending_controls: Mutex::new(VecDeque::new()),
            tool_names: Mutex::new(HashMap::new()),
            turn_lock: tokio::sync::Mutex::new(()),
        });

        let (mut live, pgid_slot, stdin_tx) = match self.spawn_process_for_worker(&worker, None) {
            Ok(triple) => triple,
            Err(err) => {
                revoke_native_worker_token(self.cfg.state_dir.as_deref(), &thread);
                return Err(err);
            }
        };

        let mut cleanup_guard = ClaudeSpawnGuard {
            state_dir: self.cfg.state_dir.clone(),
            thread: thread.clone(),
            worker: worker.clone(),
            pgid_slot,
            armed: true,
        };

        let generation = live.generation;
        {
            let mut slot = worker.stdin_tx.lock().unwrap_or_else(|e| e.into_inner());
            *slot = Some((generation, stdin_tx.clone()));
        }
        {
            let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
            log.push_event(AgentEventKind::UserMessage {
                text: initial_prompt.clone(),
            });
            log.push_event(AgentEventKind::TurnStarted);
        }

        if let Err(err) = write_stdin_line(
            &stdin_tx,
            encode_claude_user_turn(&initial_prompt),
            "Claude",
        )
        .await
        {
            worker.retiring.store(true, Ordering::Release);
            revoke_native_worker_token(self.cfg.state_dir.as_deref(), &thread);
            drop(stdin_tx);
            worker
                .stdin_tx
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            terminate_process_group(&mut live, self.cfg.retire_grace).await;
            cleanup_guard.disarm();
            return Err(HostError::Unreachable(format!(
                "write initial prompt to Claude stdin: {err}"
            )));
        }

        {
            let mut child_guard = worker.live_child.lock().await;
            *child_guard = Some(live);
        }

        {
            let mut reg = self.registry.lock().unwrap_or_else(|e| e.into_inner());
            reg.insert_live(
                thread.clone(),
                worker.req.wave.clone(),
                worker.req.role.clone(),
                worker.attempt,
                worker,
            );
        }
        cleanup_guard.disarm();

        Ok(thread)
    }

    async fn dispatch_prompt_now(
        &self,
        worker: &Arc<ClaudeWorkerState>,
        prompt_text: &str,
    ) -> Result<(), HostError> {
        let thread = &worker.thread;
        let prompt_line = encode_claude_user_turn(prompt_text);
        let existing_stdin = worker
            .stdin_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|(_, tx)| tx.clone());

        if existing_stdin.is_none()
            && worker
                .session_id
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_none()
        {
            worker
                .turn_queue
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .in_flight = false;
            return Err(HostError::Unreachable(format!(
                "Claude thread {thread} exited before reporting session_id; cannot resume"
            )));
        }

        {
            let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
            log.push_event(AgentEventKind::UserMessage {
                text: prompt_text.to_owned(),
            });
            log.push_event(AgentEventKind::TurnStarted);
        }
        worker.emit_activity(Activity::Working, false);

        let mut sent = false;
        if let Some(tx) = existing_stdin
            && write_stdin_line(&tx, prompt_line.clone(), "Claude")
                .await
                .is_ok()
        {
            sent = true;
        }

        if !sent {
            let session_id = match worker
                .session_id
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
            {
                Some(sid) => sid,
                None => {
                    let msg = format!(
                        "Claude thread {thread} exited before reporting session_id; cannot resume"
                    );
                    worker
                        .turn_queue
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .in_flight = false;
                    worker.emit_activity(Activity::Errored { error: msg.clone() }, false);
                    return Err(HostError::Unreachable(msg));
                }
            };

            if let Some(mut old_live) = worker.live_child.lock().await.take() {
                terminate_process_group(&mut old_live, Duration::from_millis(200)).await;
            }

            if worker.retiring.load(Ordering::Acquire) || worker.retired.load(Ordering::Acquire) {
                worker
                    .turn_queue
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .in_flight = false;
                return Err(HostError::Rejected(format!(
                    "Claude thread {thread} is retired"
                )));
            }

            let (mut new_live, _pgid_slot, new_stdin) =
                match self.spawn_process_for_worker(worker, Some(&session_id)) {
                    Ok(triple) => triple,
                    Err(err) => {
                        worker
                            .turn_queue
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .in_flight = false;
                        worker.emit_activity(
                            Activity::Errored {
                                error: err.to_string(),
                            },
                            false,
                        );
                        return Err(err);
                    }
                };
            let generation = new_live.generation;
            {
                let mut child_guard = worker.live_child.lock().await;
                if worker.retiring.load(Ordering::Acquire) || worker.retired.load(Ordering::Acquire)
                {
                    drop(child_guard);
                    drop(new_stdin);
                    terminate_process_group(&mut new_live, self.cfg.retire_grace).await;
                    worker
                        .turn_queue
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .in_flight = false;
                    return Err(HostError::Rejected(format!(
                        "Claude thread {thread} retired during respawn"
                    )));
                }
                {
                    let mut slot = worker.stdin_tx.lock().unwrap_or_else(|e| e.into_inner());
                    *slot = Some((generation, new_stdin.clone()));
                }
                *child_guard = Some(new_live);
            }
            if let Err(err) = write_stdin_line(&new_stdin, prompt_line, "Claude").await {
                let msg = format!("write resumed prompt to Claude stdin: {err}");
                worker
                    .turn_queue
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .in_flight = false;
                worker.emit_activity(Activity::Errored { error: msg.clone() }, false);
                drop(new_stdin);
                worker
                    .stdin_tx
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                if let Some(mut live) = worker.live_child.lock().await.take() {
                    terminate_process_group(&mut live, self.cfg.retire_grace).await;
                }
                return Err(HostError::Unreachable(msg));
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
        let worker = self
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_thread(thread)
            .ok_or_else(|| HostError::Rejected(format!("unknown Claude thread {thread}")))?;

        let _turn_guard = worker.turn_lock.lock().await;
        if worker.retiring.load(Ordering::Acquire) || worker.retired.load(Ordering::Acquire) {
            return Err(HostError::Rejected(format!(
                "Claude thread {thread} is retired"
            )));
        }

        let send_interrupt = {
            let mut tq = worker.turn_queue.lock().unwrap_or_else(|e| e.into_inner());
            if tq.in_flight {
                match mode {
                    SteerDeliveryMode::Start => {
                        return Err(HostError::Rejected(format!(
                            "Claude thread {thread} already has an active turn; use Steer or Auto"
                        )));
                    }
                    SteerDeliveryMode::Auto | SteerDeliveryMode::QueueIfActive => {
                        tq.queued.push_back(body.as_str().to_owned());
                        return Ok(());
                    }
                    SteerDeliveryMode::Steer | SteerDeliveryMode::SteerIfActive => {
                        tq.queued.push_back(body.as_str().to_owned());
                        true
                    }
                }
            } else {
                tq.in_flight = true;
                false
            }
        };

        if send_interrupt {
            let stdin_tx = worker
                .stdin_tx
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .map(|(_, tx)| tx.clone());
            if let Some(tx) = stdin_tx {
                let seq = worker.interrupt_seq.fetch_add(1, Ordering::Relaxed) + 1;
                let _ = write_stdin_line(&tx, encode_claude_interrupt_request(seq), "Claude").await;
            }
            return Ok(());
        }

        self.dispatch_prompt_now(&worker, body.as_str()).await
    }

    fn validate_resolve_worker(
        &self,
        thread: &HostThreadId,
        interaction: &HostInteractionId,
        resolution: &InteractionResolution,
    ) -> Result<(Arc<ClaudeWorkerState>, String), HostError> {
        let worker = self
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_thread(thread)
            .ok_or_else(|| HostError::Rejected(format!("unknown Claude thread {thread}")))?;

        if worker.retiring.load(Ordering::Acquire) || worker.retired.load(Ordering::Acquire) {
            return Err(HostError::Rejected(format!(
                "Claude thread {thread} is retired"
            )));
        }

        if worker
            .stdin_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
        {
            return Err(HostError::Unreachable(format!(
                "Claude thread {thread} stdin is closed"
            )));
        }

        let response_line = {
            let controls = worker
                .pending_controls
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let Some(front) = controls.front() else {
                return Err(HostError::Rejected(format!(
                    "no pending Claude control_request on {thread}"
                )));
            };
            if &front.interaction.id != interaction {
                return Err(HostError::Rejected(format!(
                    "Claude thread {thread} is blocked on {}, not {interaction}",
                    front.interaction.id
                )));
            }
            encode_claude_control_response(
                interaction.as_str(),
                resolution,
                &front.orig_input,
                front.permission_suggestions.as_ref(),
            )?
        };

        Ok((worker, response_line))
    }

    async fn resolve_worker(
        &self,
        thread: &HostThreadId,
        interaction: &HostInteractionId,
        resolution: &InteractionResolution,
    ) -> Result<(), HostError> {
        let worker = self
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_thread(thread)
            .ok_or_else(|| HostError::Rejected(format!("unknown Claude thread {thread}")))?;

        if worker.retiring.load(Ordering::Acquire) || worker.retired.load(Ordering::Acquire) {
            return Err(HostError::Rejected(format!(
                "Claude thread {thread} is retired"
            )));
        }

        let stdin_tx = worker
            .stdin_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|(_, tx)| tx.clone())
            .ok_or_else(|| {
                HostError::Unreachable(format!("Claude thread {thread} stdin is closed"))
            })?;

        let (response_line, next_blocked) = {
            let mut controls = worker
                .pending_controls
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let Some(front) = controls.front() else {
                return Err(HostError::Rejected(format!(
                    "no pending Claude control_request on {thread}"
                )));
            };
            if &front.interaction.id != interaction {
                return Err(HostError::Rejected(format!(
                    "Claude thread {thread} is blocked on {}, not {interaction}",
                    front.interaction.id
                )));
            }
            let encoded = encode_claude_control_response(
                interaction.as_str(),
                resolution,
                &front.orig_input,
                front.permission_suggestions.as_ref(),
            )?;
            controls.pop_front();
            let next = controls.front().map(|p| p.interaction.clone());
            (encoded, next)
        };

        // Emit next Blocked or Working before unblocking the child so subsequent observations strictly follow it.
        match next_blocked {
            Some(next_interaction) => {
                worker.emit_activity(
                    Activity::Blocked {
                        interaction: next_interaction,
                    },
                    false,
                );
            }
            None => {
                worker.emit_activity(Activity::Working, false);
            }
        }

        write_stdin_line(&stdin_tx, response_line, "Claude")
            .await
            .map_err(|e| HostError::Unreachable(format!("write Claude control_response: {e}")))?;

        Ok(())
    }

    async fn retire_worker(&self, thread: &HostThreadId) -> Result<(), HostError> {
        revoke_native_worker_token(self.cfg.state_dir.as_deref(), thread);

        let worker = self
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_thread(thread);

        let Some(worker) = worker else {
            self.spawn_locks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(thread);
            return Ok(());
        };

        worker.retiring.store(true, Ordering::Release);
        {
            let mut tq = worker.turn_queue.lock().unwrap_or_else(|e| e.into_inner());
            tq.in_flight = false;
            tq.queued.clear();
        }
        worker
            .pending_controls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
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
            log.trim_archived();
        }
        {
            let mut reg = self.registry.lock().unwrap_or_else(|e| e.into_inner());
            reg.mark_archived(thread, |w| {
                (w.req.wave.clone(), w.req.role.clone(), w.attempt)
            });
        }
        self.spawn_locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(thread);

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
        let worker = self
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_thread(thread);
        match worker {
            Some(w) if w.retired.load(Ordering::Acquire) => Ok(Some(Observation::archived_for(
                thread.clone(),
                Some(w.attempt),
            ))),
            Some(w) if w.unresumable_dead.load(Ordering::Acquire) => {
                Ok(Some(Observation::activity_with_bounds(
                    Activity::Errored {
                        error:
                            "native Claude worker exited before establishing a resumable session"
                                .into(),
                    },
                    thread.clone(),
                    None,
                    Vec::new(),
                )))
            }
            Some(_) => Ok(None),
            None => {
                revoke_native_worker_token(self.cfg.state_dir.as_deref(), thread);
                Ok(Some(Observation::activity_with_bounds(
                    Activity::Errored {
                        error: "native Claude worker process missing after restart".into(),
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
        self.registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_role(wave, role)
            .map(|w| w.log.clone())
            .ok_or_else(|| ApiError::NotFound {
                what: format!("Claude worker @{wave}/{role}"),
            })
    }
}

async fn run_claude_stdout_reader(
    weak_bridge: Weak<ClaudeBridge>,
    worker: Arc<ClaudeWorkerState>,
    stdout: tokio::process::ChildStdout,
    stderr_task: tokio::task::JoinHandle<String>,
    generation: u64,
    pgid_slot: PgidSlot,
) {
    let mut reader = BufReader::new(stdout);
    let mut scratch = Vec::with_capacity(4096);
    let mut prior_refusal: Option<String> = None;
    let mut last_assistant_usage: Option<ThreadContextUsage> = None;

    while let Ok(Some(item)) =
        read_bounded_line(&mut reader, &mut scratch, MAX_STDOUT_LINE_BYTES).await
    {
        if worker.generation.load(Ordering::Acquire) != generation {
            break;
        }
        match item {
            BoundedLine::Line(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let Ok(val) = serde_json::from_str::<Value>(trimmed) else {
                    continue;
                };
                handle_claude_line(
                    &weak_bridge,
                    &worker,
                    &val,
                    &mut prior_refusal,
                    &mut last_assistant_usage,
                    generation,
                );
            }
            BoundedLine::Oversized(total) => {
                worker
                    .log
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push_event(AgentEventKind::Other {
                        bb_type: format!("oversized_stdout_line_dropped: {total} bytes"),
                    });
            }
        }
    }

    let stderr_tail = tokio::time::timeout(Duration::from_millis(500), stderr_task)
        .await
        .ok()
        .and_then(|r| r.ok())
        .unwrap_or_default();

    if worker.retiring.load(Ordering::Acquire) {
        return;
    }

    // If a newer generation was already spawned (e.g. `--resume`), reap only our own
    // process group and never clobber the newer generation's turn or stdin state.
    if worker.generation.load(Ordering::Acquire) != generation {
        claim_and_kill_pgid(&pgid_slot);
        return;
    }

    {
        let mut slot = worker.stdin_tx.lock().unwrap_or_else(|e| e.into_inner());
        if slot.as_ref().is_some_and(|(g, _)| *g == generation) {
            slot.take();
        }
    }

    let exit_status = {
        let mut guard = worker.live_child.lock().await;
        if guard.as_ref().is_some_and(|l| l.generation == generation) {
            if let Some(mut live) = guard.take() {
                let st = tokio::time::timeout(Duration::from_secs(2), live.child.wait())
                    .await
                    .ok()
                    .and_then(|r| r.ok());
                live.mark_reaped();
                claim_and_kill_pgid(&pgid_slot);
                st
            } else {
                claim_and_kill_pgid(&pgid_slot);
                None
            }
        } else {
            claim_and_kill_pgid(&pgid_slot);
            None
        }
    };

    if worker.retiring.load(Ordering::Acquire)
        || worker.generation.load(Ordering::Acquire) != generation
    {
        return;
    }

    let can_resume = worker
        .session_id
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .is_some();
    if !can_resume {
        worker.unresumable_dead.store(true, Ordering::Release);
        revoke_native_worker_token(worker.state_dir.as_deref(), &worker.thread);
    }

    let (mid_turn, next_queued) = {
        let mut tq = worker.turn_queue.lock().unwrap_or_else(|e| e.into_inner());
        let was_in_flight = tq.in_flight;
        if exit_status.is_some_and(|s| s.success()) && can_resume && !tq.queued.is_empty() {
            let next = tq.queued.pop_front();
            tq.in_flight = next.is_some();
            (was_in_flight, next)
        } else {
            tq.in_flight = false;
            tq.queued.clear();
            (was_in_flight, None)
        }
    };

    if let Some(next_prompt) = next_queued
        && let Some(bridge) = weak_bridge.upgrade()
    {
        let w = worker.clone();
        tokio::spawn(async move {
            let _guard = w.turn_lock.lock().await;
            let _ = bridge.dispatch_prompt_now(&w, &next_prompt).await;
        });
        return;
    }

    match exit_status {
        Some(status) if status.success() => {
            if mid_turn {
                if let Some(reason) = prior_refusal.take() {
                    worker.emit_activity(Activity::Refused { reason }, true);
                } else {
                    worker.emit_activity(Activity::Idle, true);
                }
            }
        }
        Some(status) => {
            if mid_turn {
                let detail = if stderr_tail.is_empty() {
                    format!("Claude process exited with {status}")
                } else {
                    format!("Claude process exited with {status}: {stderr_tail}")
                };
                let act = if is_refusal_error_text(&detail) {
                    Activity::Refused { reason: detail }
                } else {
                    Activity::Errored { error: detail }
                };
                worker.emit_activity(act, true);
            }
        }
        None => {
            if mid_turn {
                let detail = if stderr_tail.is_empty() {
                    "Claude process terminated unexpectedly".to_owned()
                } else {
                    format!("Claude process terminated unexpectedly: {stderr_tail}")
                };
                worker.emit_activity(Activity::Errored { error: detail }, true);
            }
        }
    }
}

fn handle_claude_line(
    weak_bridge: &Weak<ClaudeBridge>,
    worker: &Arc<ClaudeWorkerState>,
    val: &Value,
    prior_refusal: &mut Option<String>,
    last_assistant_usage: &mut Option<ThreadContextUsage>,
    generation: u64,
) {
    if worker.generation.load(Ordering::Acquire) != generation {
        return;
    }

    if let Some(sid) = val
        .get("session_id")
        .or_else(|| val.get("sessionId"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        *worker.session_id.lock().unwrap_or_else(|e| e.into_inner()) = Some(sid.to_owned());
    }

    let Some(ty) = val.get("type").and_then(|v| v.as_str()) else {
        return;
    };

    match ty {
        "system" => {
            let hint = worker.preferred_model_hint();
            let (model, account) =
                extract_claude_runtime_meta_with_hint(val, hint.as_deref(), true);
            worker.emit_runtime_meta_if_changed(model, account);
        }
        "assistant" => {
            let hint = worker.preferred_model_hint();
            let (model, account) =
                extract_claude_runtime_meta_with_hint(val, hint.as_deref(), true);
            worker.emit_runtime_meta_if_changed(model, account);

            let msg = val.get("message").unwrap_or(val);
            if let Some(sr) = msg
                .get("stop_reason")
                .or_else(|| msg.get("stopReason"))
                .and_then(|v| v.as_str())
                && is_refusal_stop_reason(sr)
            {
                *prior_refusal = Some(sr.to_owned());
            }

            if let Some(content) = msg.get("content") {
                if let Some(text) = content.as_str() {
                    if !text.is_empty() {
                        worker
                            .log
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push_event(AgentEventKind::AssistantText {
                                text: text.to_owned(),
                            });
                    }
                } else if let Some(blocks) = content.as_array() {
                    for block in blocks {
                        match block.get("type").and_then(|v| v.as_str()) {
                            Some("text") => {
                                if let Some(text) = block.get("text").and_then(|v| v.as_str())
                                    && !text.is_empty()
                                {
                                    worker
                                        .log
                                        .lock()
                                        .unwrap_or_else(|e| e.into_inner())
                                        .push_event(AgentEventKind::AssistantText {
                                            text: text.to_owned(),
                                        });
                                }
                            }
                            Some("tool_use") => {
                                let name = block
                                    .get("name")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("tool")
                                    .to_owned();
                                if let Some(id) = block.get("id").and_then(|v| v.as_str()) {
                                    worker
                                        .tool_names
                                        .lock()
                                        .unwrap_or_else(|e| e.into_inner())
                                        .insert(id.to_owned(), name.clone());
                                }
                                let summary = block
                                    .get("input")
                                    .map(|v| truncate_summary(&v.to_string(), 240))
                                    .unwrap_or_default();
                                worker
                                    .log
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .push_event(AgentEventKind::ToolCall { name, summary });
                            }
                            _ => {}
                        }
                    }
                }
            }

            let hint_after = worker.preferred_model_hint();
            if let Some(usage) = parse_claude_usage_for_model(msg, hint_after.as_deref()) {
                *last_assistant_usage = Some(usage.clone());
                let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
                log.usage = Some(usage.clone());
                log.push_event(AgentEventKind::Usage { usage });
            }
        }
        "user" => {
            let msg = val.get("message").unwrap_or(val);
            if let Some(blocks) = msg.get("content").and_then(|v| v.as_array()) {
                for block in blocks {
                    if block.get("type").and_then(|v| v.as_str()) != Some("tool_result") {
                        continue;
                    }
                    let tool_name = block
                        .get("tool_use_id")
                        .and_then(|v| v.as_str())
                        .and_then(|id| {
                            worker
                                .tool_names
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .get(id)
                                .cloned()
                        })
                        .unwrap_or_else(|| "tool".to_owned());
                    let ok = !block
                        .get("is_error")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    let summary = extract_tool_result_summary(block.get("content"));
                    worker
                        .log
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push_event(AgentEventKind::ToolResult {
                            name: tool_name,
                            ok,
                            summary,
                        });
                }
            }
        }
        "control_cancel_request" => {
            let Some(req_id_raw) = val
                .get("request_id")
                .or_else(|| val.get("id"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            else {
                return;
            };
            let next_state = {
                let mut controls = worker
                    .pending_controls
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let was_front = controls
                    .front()
                    .is_some_and(|p| p.interaction.id.as_str() == req_id_raw);
                controls.retain(|p| p.interaction.id.as_str() != req_id_raw);
                was_front.then(|| controls.front().map(|p| p.interaction.clone()))
            };
            if let Some(maybe_next) = next_state {
                match maybe_next {
                    Some(next_interaction) => {
                        worker.emit_activity(
                            Activity::Blocked {
                                interaction: next_interaction,
                            },
                            false,
                        );
                    }
                    None => {
                        worker.emit_activity(Activity::Working, false);
                    }
                }
            }
        }
        "control_request" => {
            let Some(req_id_raw) = val
                .get("request_id")
                .or_else(|| val.get("id"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
            else {
                return;
            };
            let req_obj = val.get("request").unwrap_or(val);
            let subtype = req_obj.get("subtype").and_then(|v| v.as_str());
            if subtype != Some("can_use_tool") {
                if let Some(tx) = worker
                    .stdin_tx
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .map(|(_, tx)| tx.clone())
                {
                    let err_line = encode_claude_control_error(
                        req_id_raw,
                        "unsupported control_request subtype",
                    );
                    let (reply_tx, _) = tokio::sync::oneshot::channel();
                    let _ = tx.send((err_line, reply_tx));
                }
                return;
            }
            let Ok(interaction_id) = HostInteractionId::parse(req_id_raw) else {
                if let Some(tx) = worker
                    .stdin_tx
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .map(|(_, tx)| tx.clone())
                {
                    let err_line =
                        encode_claude_control_error(req_id_raw, "invalid control request_id");
                    let (reply_tx, _) = tokio::sync::oneshot::channel();
                    let _ = tx.send((err_line, reply_tx));
                }
                return;
            };
            let tool_name = req_obj
                .get("tool_name")
                .and_then(|v| v.as_str())
                .unwrap_or("tool");
            let orig_input = req_obj.get("input").cloned().unwrap_or_else(|| json!({}));
            let suggestions = req_obj.get("permission_suggestions").cloned();
            let reason = req_obj
                .get("decision_reason")
                .or_else(|| req_obj.get("blocked_path"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let summary = match reason {
                Some(r) => format!("{tool_name}: {r}"),
                None => format!(
                    "{tool_name}: {}",
                    truncate_summary(&orig_input.to_string(), 160)
                ),
            };
            let interaction = PendingInteraction {
                id: interaction_id,
                kind: InteractionKind::Permission,
                summary: summary.clone(),
            };
            let publish_now = {
                let mut controls = worker
                    .pending_controls
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let was_empty = controls.is_empty();
                controls.push_back(PendingClaudeControl {
                    interaction: interaction.clone(),
                    orig_input,
                    permission_suggestions: suggestions,
                });
                was_empty
            };
            {
                let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
                log.push_event(AgentEventKind::Other {
                    bb_type: format!("permission_request: {summary}"),
                });
            }
            if publish_now {
                worker.emit_activity(Activity::Blocked { interaction }, false);
            }
        }
        "result" => {
            worker
                .pending_controls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clear();

            let had_model = worker
                .last_model
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_some();
            let hint = worker.preferred_model_hint();
            let (model, account) =
                extract_claude_runtime_meta_with_hint(val, hint.as_deref(), !had_model);
            worker.emit_runtime_meta_if_changed(model, account);

            let hint_after = worker.preferred_model_hint();
            if let Some(res_usage) = parse_claude_usage_for_model(val, hint_after.as_deref()) {
                let effective = match last_assistant_usage.take() {
                    Some(occ) => merge_assistant_and_result_usage(occ, &res_usage),
                    None => res_usage,
                };
                let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
                log.usage = Some(effective.clone());
                log.push_event(AgentEventKind::Usage { usage: effective });
            } else {
                let _ = last_assistant_usage.take();
            }

            {
                let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
                log.pending = None;
                log.push_event(AgentEventKind::TurnEnded);
            }

            let activity = classify_claude_result(val, prior_refusal.as_deref());
            *prior_refusal = None;

            let next_queued = {
                let mut tq = worker.turn_queue.lock().unwrap_or_else(|e| e.into_inner());
                if !worker.retiring.load(Ordering::Acquire)
                    && matches!(activity, Activity::Idle)
                    && let Some(next) = tq.queued.pop_front()
                {
                    tq.in_flight = true;
                    Some(next)
                } else {
                    tq.in_flight = false;
                    if !matches!(activity, Activity::Idle) {
                        tq.queued.clear();
                    }
                    None
                }
            };

            if let Some(next_prompt) = next_queued
                && let Some(bridge) = weak_bridge.upgrade()
            {
                let w = worker.clone();
                tokio::spawn(async move {
                    let _guard = w.turn_lock.lock().await;
                    let _ = bridge.dispatch_prompt_now(&w, &next_prompt).await;
                });
                return;
            }

            worker.emit_activity(activity, true);
        }
        _ => {}
    }
}

/// `rex_wave_waves::AgentHost` adapter backed by [`ClaudeBridge`].
#[derive(Clone)]
pub struct ClaudeHost(pub Arc<ClaudeBridge>);

impl AgentHost for ClaudeHost {
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

    fn validate_resolve(
        &self,
        thread: &HostThreadId,
        interaction: &HostInteractionId,
        resolution: &InteractionResolution,
    ) -> BoxFut<'_, Result<(), HostError>> {
        let res = self
            .0
            .validate_resolve_worker(thread, interaction, resolution)
            .map(|_| ());
        Box::pin(async move { res })
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

/// `rex_wave_runtime::AgentFeed` adapter backed by [`ClaudeBridge`].
#[derive(Clone)]
pub struct ClaudeFeed(pub Arc<ClaudeBridge>);

impl AgentFeed for ClaudeBridge {
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

impl AgentFeed for ClaudeFeed {
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
    use rex_wave_api::wave::{AgentSpec, FileSystemGrant, GrantedPermissions, Workspace};
    use rex_wave_api::{ProjectId, ProviderId};

    fn sample_req(permission: PermissionMode) -> SpawnRequest {
        let wave = WaveSlug::parse("w1").unwrap();
        let project = ProjectId::parse("/workspace/demo").unwrap();
        SpawnRequest {
            wave: wave.clone(),
            role: RoleName::parse("author").unwrap(),
            attempt: 1,
            project: project.clone(),
            agent: AgentSpec {
                provider: ProviderId::parse("claude-code").unwrap(),
                model: Some(ModelId::parse("claude-opus-5-5").unwrap()),
                account: None,
                reasoning: Some("high".into()),
                service_tier: None,
                permission,
                workspace: Workspace::ProjectDefault,
            },
            room: rex_wave_waves::wave_room(&wave, &project),
            prompt: Body::parse("hello").unwrap(),
            title: "[w1] @author".into(),
            run_stamp: "w1:author:a1".into(),
            output_dir: None,
            writes: Vec::new(),
            return_schema: None,
        }
    }

    #[test]
    fn claude_args_map_permission_modes_and_resume() {
        let cfg = ClaudeRunnerConfig::from_env().with_mcp_config("/tmp/mcp.json");
        let args = build_claude_args(&cfg, &sample_req(PermissionMode::AcceptEdits), Some("s1"));
        assert!(
            args.windows(2)
                .any(|w| w == ["--permission-mode", "acceptEdits"])
        );
        assert!(
            args.windows(2)
                .any(|w| w == ["--permission-prompt-tool", "stdio"])
        );
        assert!(args.windows(2).any(|w| w == ["--resume", "s1"]));
        assert!(args.windows(2).any(|w| w == ["--effort", "high"]));
        assert!(
            args.windows(2)
                .any(|w| w == ["--mcp-config", "/tmp/mcp.json"])
        );

        let full_args = build_claude_args(&cfg, &sample_req(PermissionMode::Full), None);
        assert!(
            full_args
                .windows(2)
                .any(|w| w == ["--permission-mode", "bypassPermissions"])
        );
        assert!(!full_args.iter().any(|a| a == "--permission-prompt-tool"));
    }

    #[test]
    fn claude_control_response_enforces_session_scope_and_request_answer_shape() {
        let input = json!({"command": "cargo check"});
        assert!(
            encode_claude_control_response(
                "r1",
                &InteractionResolution::AllowForSession {
                    granted_permissions: None
                },
                &input,
                None,
            )
            .is_err(),
            "AllowForSession must fail when no session suggestions were offered"
        );

        let missing_dest = json!([{"type": "addRules"}]);
        assert!(
            encode_claude_control_response(
                "r1",
                &InteractionResolution::AllowForSession {
                    granted_permissions: None
                },
                &input,
                Some(&missing_dest),
            )
            .is_err(),
            "missing destination must not pass session filter"
        );

        let persistent_only = json!([{"type": "addRules", "destination": "userSettings"}]);
        assert!(
            encode_claude_control_response(
                "r1",
                &InteractionResolution::AllowForSession {
                    granted_permissions: None
                },
                &input,
                Some(&persistent_only),
            )
            .is_err(),
            "AllowForSession must not widen to userSettings"
        );

        assert!(
            encode_claude_control_response(
                "r1",
                &InteractionResolution::RequestAnswer { value: json!({}) },
                &input,
                None,
            )
            .is_err(),
            "empty RequestAnswer object must be rejected"
        );

        assert!(
            encode_claude_control_response(
                "r1",
                &InteractionResolution::RequestAnswer {
                    value: json!({
                        "behavior": "allow",
                        "updatedInput": {"command": "cargo check"},
                        "updatedPermissions": [{"type": "addRules", "destination": "userSettings"}]
                    })
                },
                &input,
                None,
            )
            .is_err(),
            "RequestAnswer must reject persistent updatedPermissions"
        );

        let mixed = json!([
            {"type": "addRules", "destination": "userSettings"},
            {"type": "addRules", "destination": "session"}
        ]);
        let ok_session: Value = serde_json::from_str(
            &encode_claude_control_response(
                "r1",
                &InteractionResolution::AllowForSession {
                    granted_permissions: None,
                },
                &input,
                Some(&mixed),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            ok_session["response"]["response"]["updatedPermissions"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        let custom_gp = GrantedPermissions {
            file_system: Some(FileSystemGrant {
                read: vec!["/etc".into()],
                write: Vec::new(),
            }),
            network: None,
        };
        assert!(
            encode_claude_control_response(
                "r1",
                &InteractionResolution::AllowOnce {
                    granted_permissions: Some(custom_gp),
                },
                &input,
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn claude_model_usage_prefers_primary_model_over_lexicographic_haiku() {
        let result_val = json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "usage": {"input_tokens": 5000, "output_tokens": 800},
            "modelUsage": {
                "claude-haiku-4-5": {"contextWindow": 200000, "inputTokens": 200, "outputTokens": 50},
                "claude-opus-5-5": {"contextWindow": 1000000, "inputTokens": 4800, "outputTokens": 750}
            }
        });
        let usage = parse_claude_usage_for_model(&result_val, Some("claude-opus-5-5")).unwrap();
        assert_eq!(usage.model_context_window, 1_000_000);
        let (meta_model, _) =
            extract_claude_runtime_meta_with_hint(&result_val, Some("claude-opus-5-5"), false);
        assert!(
            meta_model.is_none(),
            "result without explicit model must not flip already-known primary model"
        );
    }

    #[test]
    fn claude_result_classifies_refusal_error_and_idle() {
        assert!(matches!(
            classify_claude_result(
                &json!({"stop_reason": "refusal", "result": "blocked"}),
                None
            ),
            Activity::Refused { .. }
        ));
        assert!(matches!(
            classify_claude_result(
                &json!({"is_error": true, "result": "content_filter triggered"}),
                None
            ),
            Activity::Refused { .. }
        ));
        assert!(matches!(
            classify_claude_result(
                &json!({"is_error": true, "subtype": "error_during_execution", "error": "boom"}),
                None
            ),
            Activity::Errored { ref error } if error == "boom"
        ));
        assert_eq!(
            classify_claude_result(&json!({"subtype": "success", "is_error": false}), None),
            Activity::Idle
        );
    }

    #[test]
    fn claude_thread_id_roundtrips() {
        let w = WaveSlug::parse("wave-one").unwrap();
        let r = RoleName::parse("reviewer").unwrap();
        let tid = claude_thread_id(&w, &r, 2).unwrap();
        assert_eq!(tid.as_str(), "claude:wave-one:reviewer:a2");
        assert_eq!(parse_claude_thread_id(&tid), Some((w, r, 2)));
    }
}
