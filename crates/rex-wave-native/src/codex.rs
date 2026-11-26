//! Native Codex `app-server` stdio JSON-RPC 2.0 runner (`CodexBridge`, `CodexHost`, `CodexFeed`).

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
    Activity, GrantedPermissions, InteractionKind, InteractionResolution, Observation,
    PendingInteraction, PermissionMode, Question, SteerDeliveryMode, is_refusal_error_text,
    is_refusal_stop_reason,
};
use rex_wave_api::{
    ApiError, CODEX_THREAD_PREFIX, FrameStream, HostInteractionId, HostSeq, HostThreadId, ModelId,
    Principal, RoleName, SystemPart, WaveSlug,
};
use rex_wave_runtime::{AgentFeed, BoxFut, BridgeBackend, Router};
use rex_wave_transport::WORKER_THREAD_ENV;
use rex_wave_waves::{AgentHost, HostError, SpawnRequest, expand_tilde};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;
use tokio::io::BufReader;
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot, watch};

pub const CODEX_BIN_ENV: &str = "REX_WAVE_CODEX_BIN";
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const INTERRUPT_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_STDERR_BYTES: usize = 8192;

#[derive(Clone, Debug)]
pub struct CodexRunnerConfig {
    pub bin: PathBuf,
    pub state_dir: Option<PathBuf>,
    pub socket_path: Option<PathBuf>,
    pub rex_wave_bin: Option<PathBuf>,
    pub bb_data_dir: Option<PathBuf>,
    pub retire_grace: Duration,
    pub rpc_timeout: Duration,
    pub system_principal: Principal,
    pub extra_args: Vec<String>,
    pub config_rx: Option<watch::Receiver<RexConfig>>,
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
            rex_wave_bin: None,
            bb_data_dir: rex_wave_waves::default_bb_data_dir(),
            retire_grace: DEFAULT_RETIRE_GRACE,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
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

    pub fn with_config_watch(mut self, rx: watch::Receiver<RexConfig>) -> Self {
        self.config_rx = Some(rx);
        self
    }

    pub fn effective_bin(&self) -> PathBuf {
        if let Some(ref rx) = self.config_rx
            && let Some(raw) = rx
                .borrow()
                .harnesses
                .codex
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

pub fn build_codex_args(cfg: &CodexRunnerConfig) -> Vec<String> {
    let mut args = vec!["app-server".to_owned()];
    if let (Some(bin), Some(sock)) = (&cfg.rex_wave_bin, &cfg.socket_path)
        && let (Ok(cmd_lit), Ok(sock_lit)) = (
            serde_json::to_string(&bin.to_string_lossy()),
            serde_json::to_string(&sock.to_string_lossy()),
        )
    {
        args.push("-c".to_owned());
        args.push(format!("mcp_servers.rex-wave.command={cmd_lit}"));
        args.push("-c".to_owned());
        args.push(format!(
            "mcp_servers.rex-wave.args=[\"--socket\", {sock_lit}, \"mcp\", \"--stdio\"]"
        ));
        args.push("-c".to_owned());
        args.push(format!(
            "mcp_servers.rex-wave.env_vars=[\"REX_WAVE_TOKEN\", \"REX_WAVE_SOCKET\", \"{WORKER_THREAD_ENV}\", \"BELL_GUARD\"]"
        ));
    }
    args.extend(cfg.extra_args.iter().cloned());
    args
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

pub fn extract_codex_account_identity(account_read_res: &Value) -> Option<String> {
    let acct = account_read_res.get("account")?;
    if let Some(obj) = acct.as_object() {
        let acct_type = obj.get("type").and_then(Value::as_str).unwrap_or("");
        if acct_type == "chatgpt" {
            if let Some(email) = obj
                .get("email")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                return Some(email.to_owned());
            }
            if let Some(ws) = obj
                .get("workspaceName")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                return Some(ws.to_owned());
            }
        }
        return None;
    }
    None
}

/// Parses `ThreadTokenUsage` preferring `last` (current context window occupancy)
/// over cumulative `total`, and splits `cachedInputTokens` / `reasoningOutputTokens`
/// out of `inputTokens` / `outputTokens` so categories never double-count.
pub fn parse_codex_token_usage(params: &Value) -> Option<ThreadContextUsage> {
    let tu = params.get("tokenUsage").unwrap_or(params);
    if !tu.is_object() {
        return None;
    }
    let breakdown = tu
        .get("last")
        .or_else(|| tu.get("total"))
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

    let uncached_input = input.saturating_sub(cached);
    let visible_output = output.saturating_sub(reasoning);
    let total = breakdown
        .get("totalTokens")
        .or_else(|| breakdown.get("total_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or_else(|| {
            uncached_input
                .saturating_add(cached)
                .saturating_add(visible_output)
                .saturating_add(reasoning)
        });

    let model_context_window = tu
        .get("modelContextWindow")
        .or_else(|| tu.get("model_context_window"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let mut categories = Vec::new();
    if uncached_input > 0 {
        categories.push(ContextCategory {
            id: "input".into(),
            label: "Input".into(),
            kind: ContextCategoryKind::Used,
            tokens: uncached_input,
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
    if visible_output > 0 {
        categories.push(ContextCategory {
            id: "output".into(),
            label: "Output".into(),
            kind: ContextCategoryKind::Used,
            tokens: visible_output,
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

fn encode_codex_granted_profile(gp: &GrantedPermissions) -> Value {
    let mut out = Map::new();
    if let Some(ref fs) = gp.file_system
        && (!fs.read.is_empty() || !fs.write.is_empty())
    {
        out.insert(
            "fileSystem".into(),
            json!({
                "read": fs.read,
                "write": fs.write,
            }),
        );
    }
    if let Some(ref net) = gp.network
        && let Some(enabled) = net.enabled
    {
        out.insert("network".into(), json!({ "enabled": enabled }));
    }
    Value::Object(out)
}

fn collect_offered_fs_paths(fs_obj: &Map<String, Value>, mode: &str) -> HashSet<String> {
    let mut allowed = HashSet::new();
    if let Some(arr) = fs_obj.get(mode).and_then(Value::as_array) {
        for item in arr.iter().filter_map(Value::as_str) {
            allowed.insert(item.to_owned());
        }
    }
    if let Some(entries) = fs_obj.get("entries").and_then(Value::as_array) {
        for entry in entries {
            let access = entry.get("access").and_then(Value::as_str).unwrap_or("");
            let covers = (mode == "read" && matches!(access, "read" | "write" | "readWrite"))
                || (mode == "write" && matches!(access, "write" | "readWrite"));
            if covers
                && let Some(path_val) = entry.get("path")
                && let Some(s) = path_val
                    .as_str()
                    .or_else(|| path_val.get("path").and_then(Value::as_str))
            {
                allowed.insert(s.to_owned());
            }
        }
    }
    allowed
}

fn validate_codex_grant_subset(
    gp: &GrantedPermissions,
    requested_permissions: Option<&Value>,
) -> Result<(), HostError> {
    let Some(req_obj) = requested_permissions.and_then(Value::as_object) else {
        return Ok(());
    };
    if let Some(ref net) = gp.network
        && net.enabled == Some(true)
    {
        let req_net_enabled = req_obj
            .get("network")
            .and_then(Value::as_object)
            .and_then(|n| n.get("enabled"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !req_net_enabled {
            return Err(HostError::Rejected(
                "granted_permissions enables network access that was not requested by Codex".into(),
            ));
        }
    }
    if let Some(ref fs) = gp.file_system
        && (!fs.read.is_empty() || !fs.write.is_empty())
    {
        let Some(req_fs) = req_obj.get("fileSystem").and_then(Value::as_object) else {
            return Err(HostError::Rejected(
                "granted_permissions grants filesystem paths when Codex requested none".into(),
            ));
        };
        let offered_read = collect_offered_fs_paths(req_fs, "read");
        let offered_write = collect_offered_fs_paths(req_fs, "write");
        for r in &fs.read {
            if !offered_read.contains(r) && !offered_write.contains(r) {
                return Err(HostError::Rejected(format!(
                    "granted_permissions read path '{r}' was not requested by Codex"
                )));
            }
        }
        for w in &fs.write {
            if !offered_write.contains(w) {
                return Err(HostError::Rejected(format!(
                    "granted_permissions write path '{w}' was not requested by Codex"
                )));
            }
        }
    }
    Ok(())
}

fn validate_codex_fs_path_entry(path_val: &Value) -> Result<Option<String>, HostError> {
    let Some(pobj) = path_val.as_object() else {
        return Err(HostError::Rejected(
            "Codex fileSystem.entries[].path must be a JSON object".into(),
        ));
    };
    match pobj.get("type").and_then(Value::as_str) {
        Some("path") => {
            let Some(p) = pobj.get("path").and_then(Value::as_str) else {
                return Err(HostError::Rejected(
                    "Codex fileSystem path entry requires string field 'path'".into(),
                ));
            };
            Ok(Some(p.to_owned()))
        }
        Some("glob_pattern") => {
            if !pobj.get("pattern").is_some_and(Value::is_string) {
                return Err(HostError::Rejected(
                    "Codex fileSystem glob_pattern entry requires string field 'pattern'".into(),
                ));
            }
            Ok(None)
        }
        Some("special") => {
            let Some(vobj) = pobj.get("value").and_then(Value::as_object) else {
                return Err(HostError::Rejected(
                    "Codex fileSystem special entry requires object field 'value'".into(),
                ));
            };
            let kind = vobj.get("kind").and_then(Value::as_str).unwrap_or("");
            if !matches!(
                kind,
                "root" | "minimal" | "project_roots" | "tmpdir" | "slash_tmp" | "unknown"
            ) {
                return Err(HostError::Rejected(
                    "Codex fileSystem special entry has invalid 'kind'".into(),
                ));
            }
            if kind == "unknown" && !vobj.get("path").is_some_and(Value::is_string) {
                return Err(HostError::Rejected(
                    "Codex fileSystem unknown special entry requires string 'path'".into(),
                ));
            }
            Ok(None)
        }
        _ => Err(HostError::Rejected(
            "Codex fileSystem.entries[].path requires 'type' in path|glob_pattern|special".into(),
        )),
    }
}

fn validate_codex_raw_permissions_profile(
    perms_obj: &Map<String, Value>,
    requested_permissions: Option<&Value>,
) -> Result<(), HostError> {
    for k in perms_obj.keys() {
        if !matches!(k.as_str(), "fileSystem" | "network") {
            return Err(HostError::Rejected(format!(
                "unknown field '{k}' in Codex GrantedPermissionProfile"
            )));
        }
    }

    let mut granted_net_enabled = false;
    if let Some(net_val) = perms_obj.get("network")
        && !net_val.is_null()
    {
        let Some(net_obj) = net_val.as_object() else {
            return Err(HostError::Rejected(
                "Codex permissions.network must be an object or null".into(),
            ));
        };
        for k in net_obj.keys() {
            if k != "enabled" {
                return Err(HostError::Rejected(format!(
                    "unknown field '{k}' in Codex permissions.network"
                )));
            }
        }
        if let Some(en) = net_obj.get("enabled")
            && !en.is_null()
        {
            let Some(b) = en.as_bool() else {
                return Err(HostError::Rejected(
                    "Codex permissions.network.enabled must be a boolean or null".into(),
                ));
            };
            granted_net_enabled = b;
        }
    }

    let mut granted_read_paths: Vec<String> = Vec::new();
    let mut granted_write_paths: Vec<String> = Vec::new();
    let mut granted_structured_entries: Vec<(String, Value)> = Vec::new();
    let mut granted_glob_depth: Option<u64> = None;

    if let Some(fs_val) = perms_obj.get("fileSystem")
        && !fs_val.is_null()
    {
        let Some(fs_obj) = fs_val.as_object() else {
            return Err(HostError::Rejected(
                "Codex permissions.fileSystem must be an object or null".into(),
            ));
        };
        for k in fs_obj.keys() {
            if !matches!(
                k.as_str(),
                "read" | "write" | "entries" | "globScanMaxDepth"
            ) {
                return Err(HostError::Rejected(format!(
                    "unknown field '{k}' in Codex permissions.fileSystem"
                )));
            }
        }
        if let Some(depth_val) = fs_obj.get("globScanMaxDepth")
            && !depth_val.is_null()
        {
            let Some(d) = depth_val.as_u64().filter(|&n| n >= 1) else {
                return Err(HostError::Rejected(
                    "Codex permissions.fileSystem.globScanMaxDepth must be an integer >= 1".into(),
                ));
            };
            granted_glob_depth = Some(d);
        }
        for (key, target) in [
            ("read", &mut granted_read_paths),
            ("write", &mut granted_write_paths),
        ] {
            if let Some(arr_val) = fs_obj.get(key)
                && !arr_val.is_null()
            {
                let Some(arr) = arr_val.as_array() else {
                    return Err(HostError::Rejected(format!(
                        "Codex permissions.fileSystem.{key} must be a string array or null"
                    )));
                };
                for item in arr {
                    let Some(s) = item.as_str() else {
                        return Err(HostError::Rejected(format!(
                            "Codex permissions.fileSystem.{key} entries must be strings"
                        )));
                    };
                    target.push(s.to_owned());
                }
            }
        }
        if let Some(entries_val) = fs_obj.get("entries")
            && !entries_val.is_null()
        {
            let Some(entries) = entries_val.as_array() else {
                return Err(HostError::Rejected(
                    "Codex permissions.fileSystem.entries must be an array or null".into(),
                ));
            };
            for entry in entries {
                let Some(eobj) = entry.as_object() else {
                    return Err(HostError::Rejected(
                        "Codex permissions.fileSystem.entries items must be objects".into(),
                    ));
                };
                let Some(access) = eobj.get("access").and_then(Value::as_str) else {
                    return Err(HostError::Rejected(
                        "Codex permissions.fileSystem.entries item requires string 'access'".into(),
                    ));
                };
                if !matches!(access, "read" | "write" | "deny") {
                    return Err(HostError::Rejected(
                        "Codex permissions.fileSystem.entries access must be read|write|deny"
                            .into(),
                    ));
                }
                let Some(path_val) = eobj.get("path") else {
                    return Err(HostError::Rejected(
                        "Codex permissions.fileSystem.entries item requires 'path'".into(),
                    ));
                };
                let maybe_lit = validate_codex_fs_path_entry(path_val)?;
                match (access, maybe_lit) {
                    ("read", Some(p)) => granted_read_paths.push(p),
                    ("write", Some(p)) => granted_write_paths.push(p),
                    ("deny", _) => {}
                    (_, None) => {
                        granted_structured_entries.push((access.to_owned(), path_val.clone()))
                    }
                    _ => {}
                }
            }
        }
    }

    let has_any_grant = granted_net_enabled
        || !granted_read_paths.is_empty()
        || !granted_write_paths.is_empty()
        || !granted_structured_entries.is_empty()
        || granted_glob_depth.is_some();

    if !has_any_grant {
        return Ok(());
    }

    let Some(req_obj) = requested_permissions.and_then(Value::as_object) else {
        return Err(HostError::Rejected(
            "Codex RequestAnswer grants permissions when no permission profile was requested"
                .into(),
        ));
    };

    if granted_net_enabled {
        let req_net = req_obj
            .get("network")
            .and_then(Value::as_object)
            .and_then(|n| n.get("enabled"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !req_net {
            return Err(HostError::Rejected(
                "Codex RequestAnswer enables network access that was not requested".into(),
            ));
        }
    }

    if !granted_read_paths.is_empty()
        || !granted_write_paths.is_empty()
        || !granted_structured_entries.is_empty()
        || granted_glob_depth.is_some()
    {
        let Some(req_fs) = req_obj.get("fileSystem").and_then(Value::as_object) else {
            return Err(HostError::Rejected(
                "Codex RequestAnswer grants fileSystem permissions when none were requested".into(),
            ));
        };
        if let Some(depth) = granted_glob_depth {
            let req_depth = req_fs.get("globScanMaxDepth").and_then(Value::as_u64);
            if req_depth.is_none_or(|rd| depth > rd) {
                return Err(HostError::Rejected(
                    "Codex RequestAnswer globScanMaxDepth exceeds requested depth".into(),
                ));
            }
        }
        let offered_read = collect_offered_fs_paths(req_fs, "read");
        let offered_write = collect_offered_fs_paths(req_fs, "write");
        for r in &granted_read_paths {
            if !offered_read.contains(r) && !offered_write.contains(r) {
                return Err(HostError::Rejected(format!(
                    "Codex RequestAnswer read path '{r}' was not requested"
                )));
            }
        }
        for w in &granted_write_paths {
            if !offered_write.contains(w) {
                return Err(HostError::Rejected(format!(
                    "Codex RequestAnswer write path '{w}' was not requested"
                )));
            }
        }
        if !granted_structured_entries.is_empty() {
            let req_entries = req_fs
                .get("entries")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            for (acc, pval) in &granted_structured_entries {
                let covered = req_entries.iter().any(|re| {
                    let racc = re.get("access").and_then(Value::as_str).unwrap_or("");
                    let acc_ok = (acc == "read" && matches!(racc, "read" | "write" | "readWrite"))
                        || (acc == "write" && matches!(racc, "write" | "readWrite"));
                    acc_ok && re.get("path") == Some(pval)
                });
                if !covered {
                    return Err(HostError::Rejected(
                        "Codex RequestAnswer structured fileSystem entry was not requested".into(),
                    ));
                }
            }
        }
    }

    Ok(())
}

fn validate_codex_request_answer(
    method: &str,
    value: &Value,
    requested_permissions: Option<&Value>,
    question_ids: Option<&HashSet<String>>,
) -> Result<Value, HostError> {
    let Some(obj) = value.as_object() else {
        return Err(HostError::Rejected(format!(
            "Codex {method} RequestAnswer must be a JSON object"
        )));
    };
    match method {
        "item/commandExecution/requestApproval" => {
            if obj.len() != 1 || !obj.contains_key("decision") {
                return Err(HostError::Rejected(
                    "Codex item/commandExecution/requestApproval RequestAnswer must contain only 'decision'"
                        .into(),
                ));
            }
            let dec = &obj["decision"];
            let valid = if let Some(s) = dec.as_str() {
                matches!(s, "accept" | "acceptForSession" | "decline" | "cancel")
            } else if let Some(d) = dec.as_object() {
                if d.len() != 1 {
                    false
                } else if let Some(amend) = d.get("acceptWithExecpolicyAmendment") {
                    amend.as_object().is_some_and(|inner| {
                        inner.len() == 1
                            && inner
                                .get("execpolicy_amendment")
                                .and_then(Value::as_array)
                                .is_some_and(|arr| arr.iter().all(Value::is_string))
                    })
                } else if let Some(net) = d.get("applyNetworkPolicyAmendment") {
                    net.as_object().is_some_and(|inner| {
                        inner.len() == 1
                            && inner
                                .get("network_policy_amendment")
                                .and_then(Value::as_object)
                                .is_some_and(|npa| {
                                    npa.len() == 2
                                        && npa
                                            .get("action")
                                            .and_then(Value::as_str)
                                            .is_some_and(|a| matches!(a, "allow" | "deny"))
                                        && npa
                                            .get("host")
                                            .and_then(Value::as_str)
                                            .is_some_and(|h| !h.trim().is_empty())
                                })
                    })
                } else {
                    false
                }
            } else {
                false
            };
            if !valid {
                return Err(HostError::Rejected(
                    "invalid 'decision' in Codex item/commandExecution/requestApproval RequestAnswer"
                        .into(),
                ));
            }
            Ok(value.clone())
        }
        "item/fileChange/requestApproval" => {
            let valid = obj.len() == 1
                && obj
                    .get("decision")
                    .and_then(Value::as_str)
                    .is_some_and(|s| {
                        matches!(s, "accept" | "acceptForSession" | "decline" | "cancel")
                    });
            if !valid {
                return Err(HostError::Rejected(
                    "Codex item/fileChange/requestApproval RequestAnswer requires only 'decision' in accept|acceptForSession|decline|cancel"
                        .into(),
                ));
            }
            Ok(value.clone())
        }
        "item/permissions/requestApproval" => {
            for k in obj.keys() {
                if !matches!(k.as_str(), "permissions" | "scope" | "strictAutoReview") {
                    return Err(HostError::Rejected(format!(
                        "unknown field '{k}' in Codex PermissionsRequestApprovalResponse"
                    )));
                }
            }
            let Some(perms_obj) = obj.get("permissions").and_then(Value::as_object) else {
                return Err(HostError::Rejected(
                    "Codex item/permissions/requestApproval RequestAnswer requires object field 'permissions'"
                        .into(),
                ));
            };
            if let Some(scope) = obj.get("scope")
                && !scope
                    .as_str()
                    .is_some_and(|s| matches!(s, "turn" | "session"))
            {
                return Err(HostError::Rejected(
                    "Codex item/permissions/requestApproval RequestAnswer 'scope' must be 'turn' or 'session'"
                        .into(),
                ));
            }
            if let Some(strict) = obj.get("strictAutoReview")
                && !strict.is_null()
                && !strict.is_boolean()
            {
                return Err(HostError::Rejected(
                    "Codex item/permissions/requestApproval RequestAnswer 'strictAutoReview' must be boolean or null"
                        .into(),
                ));
            }
            validate_codex_raw_permissions_subset_or_profile(perms_obj, requested_permissions)?;
            Ok(value.clone())
        }
        "item/tool/requestUserInput" => {
            if obj.len() != 1 || !obj.contains_key("answers") {
                return Err(HostError::Rejected(
                    "Codex item/tool/requestUserInput RequestAnswer must contain only 'answers'"
                        .into(),
                ));
            }
            let Some(answers) = obj.get("answers").and_then(Value::as_object) else {
                return Err(HostError::Rejected(
                    "Codex item/tool/requestUserInput RequestAnswer requires object field 'answers'"
                        .into(),
                ));
            };
            for (qid, v) in answers {
                if let Some(allowed_qids) = question_ids
                    && !allowed_qids.contains(qid)
                {
                    return Err(HostError::Rejected(format!(
                        "unknown question id '{qid}' in Codex item/tool/requestUserInput RequestAnswer"
                    )));
                }
                let valid = v.as_object().is_some_and(|eobj| {
                    eobj.len() == 1
                        && eobj
                            .get("answers")
                            .and_then(Value::as_array)
                            .is_some_and(|arr| arr.iter().all(Value::is_string))
                });
                if !valid {
                    return Err(HostError::Rejected(
                        "Codex item/tool/requestUserInput RequestAnswer entries must be {\"answers\": [string, ...]}"
                            .into(),
                    ));
                }
            }
            Ok(value.clone())
        }
        other => Err(HostError::Rejected(format!(
            "unsupported Codex approval method {other}"
        ))),
    }
}

fn validate_codex_raw_permissions_subset_or_profile(
    perms_obj: &Map<String, Value>,
    requested_permissions: Option<&Value>,
) -> Result<(), HostError> {
    validate_codex_raw_permissions_profile(perms_obj, requested_permissions)
}

pub fn encode_codex_approval_result(
    method: &str,
    resolution: &InteractionResolution,
) -> Result<Value, HostError> {
    encode_codex_approval_result_full(method, resolution, None, None)
}

pub fn encode_codex_approval_result_with_request(
    method: &str,
    resolution: &InteractionResolution,
    requested_permissions: Option<&Value>,
) -> Result<Value, HostError> {
    encode_codex_approval_result_full(method, resolution, requested_permissions, None)
}

fn encode_codex_approval_result_full(
    method: &str,
    resolution: &InteractionResolution,
    requested_permissions: Option<&Value>,
    question_ids: Option<&HashSet<String>>,
) -> Result<Value, HostError> {
    match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            match resolution {
                InteractionResolution::AllowOnce {
                    granted_permissions,
                } => {
                    reject_non_empty_granted_permissions(granted_permissions.as_ref(), method)?;
                    Ok(json!({ "decision": "accept" }))
                }
                InteractionResolution::AllowForSession {
                    granted_permissions,
                } => {
                    reject_non_empty_granted_permissions(granted_permissions.as_ref(), method)?;
                    Ok(json!({ "decision": "acceptForSession" }))
                }
                InteractionResolution::Deny => Ok(json!({ "decision": "decline" })),
                InteractionResolution::RequestAnswer { value } => {
                    validate_codex_request_answer(method, value, requested_permissions, question_ids)
                }
                _ => Err(HostError::Rejected(format!(
                    "Codex {method} supports AllowOnce, AllowForSession, Deny, or RequestAnswer"
                ))),
            }
        }
        "item/permissions/requestApproval" => match resolution {
            InteractionResolution::AllowOnce {
                granted_permissions,
            } => {
                let perms = match granted_permissions {
                    Some(gp) => {
                        validate_codex_grant_subset(gp, requested_permissions)?;
                        encode_codex_granted_profile(gp)
                    }
                    None => requested_permissions
                        .filter(|v| v.is_object())
                        .cloned()
                        .ok_or_else(|| {
                            HostError::Rejected(
                                "Codex item/permissions/requestApproval requires granted_permissions or an offered permission profile"
                                    .into(),
                            )
                        })?,
                };
                Ok(json!({
                    "permissions": perms,
                    "scope": "turn",
                }))
            }
            InteractionResolution::AllowForSession {
                granted_permissions,
            } => {
                let perms = match granted_permissions {
                    Some(gp) => {
                        validate_codex_grant_subset(gp, requested_permissions)?;
                        encode_codex_granted_profile(gp)
                    }
                    None => requested_permissions
                        .filter(|v| v.is_object())
                        .cloned()
                        .ok_or_else(|| {
                            HostError::Rejected(
                                "Codex item/permissions/requestApproval requires granted_permissions or an offered permission profile"
                                    .into(),
                            )
                        })?,
                };
                Ok(json!({
                    "permissions": perms,
                    "scope": "session",
                }))
            }
            InteractionResolution::Deny => Ok(json!({ "permissions": {} })),
            InteractionResolution::RequestAnswer { value } => {
                validate_codex_request_answer(method, value, requested_permissions, question_ids)
            }
            _ => Err(HostError::Rejected(
                "Codex item/permissions/requestApproval supports AllowOnce, AllowForSession, Deny, or RequestAnswer"
                    .into(),
            )),
        },
        "item/tool/requestUserInput" => match resolution {
            InteractionResolution::UserAnswer { answers } => {
                let mut map = Map::new();
                for (qid, ans) in answers {
                    if let Some(allowed_qids) = question_ids
                        && !allowed_qids.contains(qid)
                    {
                        return Err(HostError::Rejected(format!(
                            "unknown question id '{qid}' in Codex item/tool/requestUserInput UserAnswer"
                        )));
                    }
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
            InteractionResolution::RequestAnswer { value } => {
                validate_codex_request_answer(method, value, requested_permissions, question_ids)
            }
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

#[derive(Clone)]
struct PendingCodexApproval {
    interaction: PendingInteraction,
    rpc_id: Value,
    method: String,
    requested_permissions: Option<Value>,
    question_ids: Option<HashSet<String>>,
}

fn push_cancelled_codex_prompts(
    log_lock: &Mutex<ThreadEventLog>,
    cancelled: Vec<String>,
    why: &str,
) {
    if cancelled.is_empty() {
        return;
    }
    let mut log = log_lock.lock().unwrap_or_else(|e| e.into_inner());
    for queued in cancelled {
        let preview = truncate_summary(queued.trim(), 160);
        log.push_event(AgentEventKind::Other {
            bb_type: format!("cancelled_queued_prompt ({why}): {preview}"),
        });
    }
}

/// Wire-ordered per-worker turn state updated synchronously from the stdout reader
/// so batched `turn/start` response + `turn/started` + `turn/completed` frames never
/// leave a completed turn marked active.
#[derive(Default)]
struct CodexTurnState {
    in_flight: bool,
    active_turn_id: Option<String>,
    started_turns: HashSet<String>,
    completed_turns: HashSet<String>,
    turn_start_rpc_ids: HashSet<u64>,
    queued_turns: VecDeque<String>,
}

struct CodexWorkerState {
    thread: HostThreadId,
    attempt: u32,
    req: SpawnRequest,
    state_dir: Option<PathBuf>,
    log: Arc<Mutex<ThreadEventLog>>,
    observer: mpsc::UnboundedSender<ObserverItem>,
    retiring: AtomicBool,
    retired: AtomicBool,
    process_dead: AtomicBool,
    next_rpc_id: AtomicU64,
    codex_thread_id: Mutex<Option<String>>,
    turn_state: Mutex<CodexTurnState>,
    streamed_items: Mutex<HashSet<String>>,
    last_error: Mutex<Option<Value>>,
    stdin_tx: Mutex<Option<mpsc::UnboundedSender<StdinLine>>>,
    live_child: tokio::sync::Mutex<Option<LiveChild>>,
    pending_rpc: Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>,
    pending_approvals: Mutex<VecDeque<PendingCodexApproval>>,
    turn_lock: tokio::sync::Mutex<()>,
}

impl CodexWorkerState {
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

    async fn rpc_call(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, HostError> {
        let id = self.next_rpc_id.fetch_add(1, Ordering::Relaxed);
        if method == "turn/start" {
            self.turn_state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .turn_start_rpc_ids
                .insert(id);
        }
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
                if method == "turn/start" {
                    self.turn_state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .turn_start_rpc_ids
                        .remove(&id);
                }
                self.pending_rpc
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&id);
                HostError::Unreachable(format!("Codex thread {} stdin closed", self.thread))
            })?;

        if let Err(err) = write_stdin_line(&stdin_tx, payload, "Codex").await {
            if method == "turn/start" {
                self.turn_state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .turn_start_rpc_ids
                    .remove(&id);
            }
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
                if method == "turn/start" {
                    self.turn_state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .turn_start_rpc_ids
                        .remove(&id);
                }
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
        write_stdin_line(&stdin_tx, payload, "Codex")
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
        write_stdin_line(&stdin_tx, payload, "Codex")
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

struct CodexSpawnGuard {
    state_dir: Option<PathBuf>,
    thread: HostThreadId,
    worker: Arc<CodexWorkerState>,
    pgid_slot: PgidSlot,
    armed: bool,
}

impl CodexSpawnGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CodexSpawnGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.worker.retiring.store(true, Ordering::Release);
        self.worker.fail_all_pending_rpc("Codex handshake aborted");
        self.worker
            .stdin_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        claim_and_kill_pgid(&self.pgid_slot);
        revoke_native_worker_token(self.state_dir.as_deref(), &self.thread);
    }
}

/// Core bridge managing native Codex `app-server` worker child processes.
pub struct CodexBridge {
    cfg: CodexRunnerConfig,
    self_weak: Weak<Self>,
    backend: OnceLock<Arc<dyn ErasedBackend>>,
    registry: Mutex<WorkerRegistry<CodexWorkerState>>,
    spawn_locks: Mutex<HashMap<HostThreadId, Arc<tokio::sync::Mutex<()>>>>,
}

impl CodexBridge {
    pub fn new(cfg: CodexRunnerConfig) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            cfg,
            self_weak: weak.clone(),
            backend: OnceLock::new(),
            registry: Mutex::new(WorkerRegistry::default()),
            spawn_locks: Mutex::new(HashMap::new()),
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

    fn thread_spawn_lock(&self, thread: &HostThreadId) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.spawn_locks.lock().unwrap_or_else(|e| e.into_inner());
        locks
            .entry(thread.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Gracefully terminates all live Codex worker process groups concurrently and revokes their tokens.
    pub async fn shutdown(&self) {
        let threads = self
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .all_threads();
        join_all(threads.iter().map(|tid| self.retire_worker(tid))).await;
    }

    async fn spawn_worker(&self, req: SpawnRequest) -> Result<HostThreadId, HostError> {
        let thread = codex_thread_id(&req.wave, &req.role, req.attempt)?;
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

        let args = build_codex_args(&self.cfg);
        let mut cmd = Command::new(self.cfg.effective_bin());
        scrub_daemon_env(&mut cmd);
        cmd.args(&args)
            .current_dir(&cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("BELL_GUARD", "1")
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
        let pgid_slot = new_pgid_slot(pgid);
        let stdin_opt = child.stdin.take();
        let stdout_opt = child.stdout.take();
        let stderr_opt = child.stderr.take();
        let live = LiveChild::with_slot(child, pgid_slot.clone(), 1);

        let (Some(stdin), Some(stdout), Some(stderr)) = (stdin_opt, stdout_opt, stderr_opt) else {
            drop(live);
            revoke_native_worker_token(self.cfg.state_dir.as_deref(), &thread);
            return Err(HostError::Unreachable(
                "Codex child stdio pipes unavailable".into(),
            ));
        };

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

        let stdin_tx = spawn_stdin_writer(stdin, "Codex");
        let stderr_task = spawn_stderr_drainer(stderr, MAX_STDERR_BYTES);

        let worker = Arc::new(CodexWorkerState {
            thread: thread.clone(),
            attempt: req.attempt,
            req: req.clone(),
            state_dir: self.cfg.state_dir.clone(),
            log,
            observer,
            retiring: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            process_dead: AtomicBool::new(false),
            next_rpc_id: AtomicU64::new(1),
            codex_thread_id: Mutex::new(None),
            turn_state: Mutex::new(CodexTurnState::default()),
            streamed_items: Mutex::new(HashSet::new()),
            last_error: Mutex::new(None),
            stdin_tx: Mutex::new(Some(stdin_tx)),
            live_child: tokio::sync::Mutex::new(Some(live)),
            pending_rpc: Mutex::new(HashMap::new()),
            pending_approvals: Mutex::new(VecDeque::new()),
            turn_lock: tokio::sync::Mutex::new(()),
        });

        let mut cleanup_guard = CodexSpawnGuard {
            state_dir: self.cfg.state_dir.clone(),
            thread: thread.clone(),
            worker: worker.clone(),
            pgid_slot: pgid_slot.clone(),
            armed: true,
        };

        let reader_worker = worker.clone();
        let weak_bridge = self.self_weak.clone();
        tokio::spawn(async move {
            run_codex_stdout_reader(weak_bridge, reader_worker, stdout, stderr_task, pgid_slot)
                .await;
        });

        let handshake_res = self.run_codex_handshake(&worker, &cwd, &req).await;
        if let Err(err) = handshake_res {
            worker.retiring.store(true, Ordering::Release);
            revoke_native_worker_token(self.cfg.state_dir.as_deref(), &thread);
            worker.fail_all_pending_rpc("Codex handshake failed");
            worker
                .stdin_tx
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            if let Some(mut live) = worker.live_child.lock().await.take() {
                terminate_process_group(&mut live, self.cfg.retire_grace).await;
            }
            cleanup_guard.disarm();
            return Err(err);
        }

        {
            let mut reg = self.registry.lock().unwrap_or_else(|e| e.into_inner());
            reg.insert_live(
                thread.clone(),
                req.wave.clone(),
                req.role.clone(),
                req.attempt,
                worker,
            );
        }
        cleanup_guard.disarm();

        Ok(thread)
    }

    async fn run_codex_handshake(
        &self,
        worker: &Arc<CodexWorkerState>,
        cwd: &std::path::Path,
        req: &SpawnRequest,
    ) -> Result<(), HostError> {
        worker
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

        // Probe account identity via `account/read` with a short bounded timeout so builds
        // that omit or ignore `account/read` never stall handshake.
        let actual_account = worker
            .rpc_call(
                "account/read",
                json!({ "refreshToken": false }),
                self.cfg.rpc_timeout.min(Duration::from_secs(2)),
            )
            .await
            .ok()
            .and_then(|res| extract_codex_account_identity(&res));

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
            let mut ts = worker.turn_state.lock().unwrap_or_else(|e| e.into_inner());
            ts.in_flight = true;
        }
        {
            let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
            log.push_event(AgentEventKind::UserMessage {
                text: req.prompt.as_str().to_owned(),
            });
        }

        if let Err(err) = worker
            .rpc_call(
                "turn/start",
                Value::Object(turn_params),
                self.cfg.rpc_timeout,
            )
            .await
        {
            let mut ts = worker.turn_state.lock().unwrap_or_else(|e| e.into_inner());
            ts.in_flight = false;
            ts.active_turn_id = None;
            return Err(err);
        }

        Ok(())
    }

    async fn start_codex_turn_now(
        &self,
        worker: &Arc<CodexWorkerState>,
        codex_tid: String,
        prompt_text: &str,
    ) -> Result<(), HostError> {
        if worker.retiring.load(Ordering::Acquire) || worker.retired.load(Ordering::Acquire) {
            let cancelled = {
                let mut ts = worker.turn_state.lock().unwrap_or_else(|e| e.into_inner());
                ts.in_flight = false;
                ts.active_turn_id = None;
                ts.queued_turns.drain(..).collect::<Vec<_>>()
            };
            push_cancelled_codex_prompts(&worker.log, cancelled, "retired");
            return Err(HostError::Rejected(format!(
                "Codex thread {} is retired",
                worker.thread
            )));
        }

        let mut params = Map::new();
        params.insert("threadId".into(), Value::String(codex_tid));
        params.insert(
            "input".into(),
            json!([{ "type": "text", "text": prompt_text }]),
        );
        if let Some(effort) = worker.req.agent.reasoning.as_deref().map(str::trim)
            && !effort.is_empty()
        {
            params.insert("effort".into(), Value::String(effort.to_owned()));
        }

        {
            let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
            log.push_event(AgentEventKind::UserMessage {
                text: prompt_text.to_owned(),
            });
        }
        // Emit Working before turn/start so a fast turn/completed strictly follows it in host_seq.
        worker.emit_activity(Activity::Working, false);

        if let Err(err) = worker
            .rpc_call("turn/start", Value::Object(params), self.cfg.rpc_timeout)
            .await
        {
            let cancelled = {
                let mut ts = worker.turn_state.lock().unwrap_or_else(|e| e.into_inner());
                ts.in_flight = false;
                ts.active_turn_id = None;
                ts.queued_turns.drain(..).collect::<Vec<_>>()
            };
            push_cancelled_codex_prompts(&worker.log, cancelled, "turn_start_failed");
            if !worker.retiring.load(Ordering::Acquire) {
                worker.emit_activity(
                    Activity::Errored {
                        error: err.to_string(),
                    },
                    false,
                );
            }
            return Err(err);
        }

        Ok(())
    }

    /// Delivers a prompt to `thread`. Note that Codex `app-server` provides native in-turn
    /// steering via `turn/steer`, so both `Steer` and `SteerIfActive` dispatch `turn/steer`
    /// into the running turn rather than aborting it via `turn/interrupt`.
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
            .ok_or_else(|| HostError::Rejected(format!("unknown Codex thread {thread}")))?;

        let _turn_guard = worker.turn_lock.lock().await;
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

        let steer_target = {
            let mut ts = worker.turn_state.lock().unwrap_or_else(|e| e.into_inner());
            if ts.in_flight {
                match mode {
                    SteerDeliveryMode::Start => {
                        return Err(HostError::Rejected(format!(
                            "Codex thread {thread} already has an active turn; use Steer or Auto"
                        )));
                    }
                    SteerDeliveryMode::Auto | SteerDeliveryMode::QueueIfActive => {
                        ts.queued_turns.push_back(body.as_str().to_owned());
                        return Ok(());
                    }
                    SteerDeliveryMode::Steer | SteerDeliveryMode::SteerIfActive => {
                        match ts.active_turn_id.clone() {
                            Some(tid) => Some(tid),
                            None => {
                                ts.queued_turns.push_back(body.as_str().to_owned());
                                return Ok(());
                            }
                        }
                    }
                }
            } else {
                ts.in_flight = true;
                None
            }
        };

        if let Some(expected_turn_id) = steer_target {
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
                Err(HostError::Rejected(_)) if mode == SteerDeliveryMode::SteerIfActive => {
                    let should_start = {
                        let mut ts = worker.turn_state.lock().unwrap_or_else(|e| e.into_inner());
                        if ts.in_flight {
                            ts.queued_turns.push_back(body.as_str().to_owned());
                            false
                        } else {
                            ts.in_flight = true;
                            true
                        }
                    };
                    if !should_start {
                        return Ok(());
                    }
                }
                Err(err) => return Err(err),
            }
        }

        self.start_codex_turn_now(&worker, codex_tid, body.as_str())
            .await
    }

    fn validate_resolve_worker(
        &self,
        thread: &HostThreadId,
        interaction: &HostInteractionId,
        resolution: &InteractionResolution,
    ) -> Result<(Arc<CodexWorkerState>, Value, Value), HostError> {
        let worker = self
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_thread(thread)
            .ok_or_else(|| HostError::Rejected(format!("unknown Codex thread {thread}")))?;

        if worker.retiring.load(Ordering::Acquire) || worker.retired.load(Ordering::Acquire) {
            return Err(HostError::Rejected(format!(
                "Codex thread {thread} is retired"
            )));
        }

        if worker
            .stdin_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
        {
            return Err(HostError::Unreachable(format!(
                "Codex thread {thread} stdin closed"
            )));
        }

        let (rpc_id, result) = {
            let approvals = worker
                .pending_approvals
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let Some(front) = approvals.front() else {
                return Err(HostError::Rejected(format!(
                    "no pending Codex approval on {thread}"
                )));
            };
            if &front.interaction.id != interaction {
                return Err(HostError::Rejected(format!(
                    "Codex thread {thread} is blocked on {}, not {interaction}",
                    front.interaction.id
                )));
            }
            let encoded = encode_codex_approval_result_full(
                &front.method,
                resolution,
                front.requested_permissions.as_ref(),
                front.question_ids.as_ref(),
            )?;
            (front.rpc_id.clone(), encoded)
        };

        Ok((worker, rpc_id, result))
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
            .ok_or_else(|| HostError::Rejected(format!("unknown Codex thread {thread}")))?;

        if worker.retiring.load(Ordering::Acquire) || worker.retired.load(Ordering::Acquire) {
            return Err(HostError::Rejected(format!(
                "Codex thread {thread} is retired"
            )));
        }

        if worker
            .stdin_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
        {
            return Err(HostError::Unreachable(format!(
                "Codex thread {thread} stdin closed"
            )));
        }

        let (rpc_id, result) = {
            let approvals = worker
                .pending_approvals
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let Some(front) = approvals.front() else {
                return Err(HostError::Rejected(format!(
                    "no pending Codex approval on {thread}"
                )));
            };
            if &front.interaction.id != interaction {
                return Err(HostError::Rejected(format!(
                    "Codex thread {thread} is blocked on {}, not {interaction}",
                    front.interaction.id
                )));
            }
            let encoded = encode_codex_approval_result_full(
                &front.method,
                resolution,
                front.requested_permissions.as_ref(),
                front.question_ids.as_ref(),
            )?;
            (front.rpc_id.clone(), encoded)
        };

        if let Err(err) = worker.rpc_respond(rpc_id.clone(), result).await {
            let msg = format!("deliver Codex approval {interaction}: {err}");
            worker.process_dead.store(true, Ordering::Release);
            worker
                .stdin_tx
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            worker
                .pending_approvals
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clear();
            worker.fail_all_pending_rpc("Codex stdin broken during approval delivery");
            let cancelled = {
                let mut ts = worker.turn_state.lock().unwrap_or_else(|e| e.into_inner());
                ts.in_flight = false;
                ts.active_turn_id = None;
                ts.queued_turns.drain(..).collect::<Vec<_>>()
            };
            push_cancelled_codex_prompts(&worker.log, cancelled, "approval_delivery_failed");
            revoke_native_worker_token(worker.state_dir.as_deref(), &worker.thread);
            if !worker.retiring.load(Ordering::Acquire) {
                worker.emit_activity(Activity::Errored { error: msg.clone() }, true);
                if let Some(mut live) = worker.live_child.lock().await.take() {
                    terminate_process_group(&mut live, self.cfg.retire_grace).await;
                }
            }
            return Err(HostError::Unreachable(msg));
        }

        let (popped, next_blocked) = {
            let mut approvals = worker
                .pending_approvals
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if approvals
                .front()
                .is_some_and(|f| &f.interaction.id == interaction && f.rpc_id == rpc_id)
            {
                approvals.pop_front();
                (true, approvals.front().map(|p| p.interaction.clone()))
            } else {
                (false, None)
            }
        };

        if popped {
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
                    let still_blocked_on_this = {
                        let log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
                        matches!(
                            &log.activity,
                            Activity::Blocked { interaction: cur } if &cur.id == interaction
                        )
                    };
                    if still_blocked_on_this {
                        worker.emit_activity(Activity::Working, false);
                    }
                }
            }
        }
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

        let codex_tid = worker
            .codex_thread_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let (turn_id, cancelled) = {
            let mut ts = worker.turn_state.lock().unwrap_or_else(|e| e.into_inner());
            ts.in_flight = false;
            let drained = ts.queued_turns.drain(..).collect::<Vec<_>>();
            (ts.active_turn_id.take(), drained)
        };
        push_cancelled_codex_prompts(&worker.log, cancelled, "retired");

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
            Some(w) if w.process_dead.load(Ordering::Acquire) => {
                Ok(Some(Observation::activity_with_bounds(
                    Activity::Errored {
                        error: "native Codex app-server process exited".into(),
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
        self.registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_role(wave, role)
            .map(|w| w.log.clone())
            .ok_or_else(|| ApiError::NotFound {
                what: format!("Codex worker @{wave}/{role}"),
            })
    }
}

async fn run_codex_stdout_reader(
    weak_bridge: Weak<CodexBridge>,
    worker: Arc<CodexWorkerState>,
    stdout: tokio::process::ChildStdout,
    stderr_task: tokio::task::JoinHandle<String>,
    pgid_slot: PgidSlot,
) {
    let mut reader = BufReader::new(stdout);
    let mut scratch = Vec::with_capacity(4096);

    while let Ok(Some(item)) =
        read_bounded_line(&mut reader, &mut scratch, MAX_STDOUT_LINE_BYTES).await
    {
        match item {
            BoundedLine::Line(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let Ok(val) = serde_json::from_str::<Value>(trimmed) else {
                    continue;
                };
                handle_codex_message(&weak_bridge, &worker, &val);
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

    worker.fail_all_pending_rpc("Codex stdout closed");

    if worker.retiring.load(Ordering::Acquire) {
        return;
    }

    worker.process_dead.store(true, Ordering::Release);
    worker
        .stdin_tx
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take();
    // Codex app-server has no post-exit session resume; revoke its bearer token and reap its group.
    revoke_native_worker_token(worker.state_dir.as_deref(), &worker.thread);

    let exit_status = if let Some(mut live) = worker.live_child.lock().await.take() {
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
    };

    if worker.retiring.load(Ordering::Acquire) {
        return;
    }

    worker
        .pending_approvals
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clear();

    let (had_in_flight_or_queued, cancelled_prompts) = {
        let mut ts = worker.turn_state.lock().unwrap_or_else(|e| e.into_inner());
        let active = ts.in_flight || ts.active_turn_id.is_some() || !ts.queued_turns.is_empty();
        ts.in_flight = false;
        ts.active_turn_id = None;
        let drained = ts.queued_turns.drain(..).collect::<Vec<_>>();
        (active, drained)
    };
    push_cancelled_codex_prompts(&worker.log, cancelled_prompts, "process_exit");

    let already_terminal = {
        let log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
        !had_in_flight_or_queued
            && matches!(
                log.activity,
                Activity::Refused { .. } | Activity::Errored { .. } | Activity::Idle
            )
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

fn record_turn_started_wire_ordered(
    worker: &Arc<CodexWorkerState>,
    turn_id: &str,
    status: Option<&str>,
) {
    let push_started = {
        let mut ts = worker.turn_state.lock().unwrap_or_else(|e| e.into_inner());
        if ts.completed_turns.contains(turn_id) {
            return;
        }
        let terminal = status.is_some_and(|s| matches!(s, "completed" | "failed" | "interrupted"));
        if !terminal {
            ts.in_flight = true;
            ts.active_turn_id = Some(turn_id.to_owned());
        }
        ts.started_turns.insert(turn_id.to_owned())
    };
    if push_started {
        worker
            .log
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_event(AgentEventKind::TurnStarted);
    }
}

fn handle_codex_message(
    weak_bridge: &Weak<CodexBridge>,
    worker: &Arc<CodexWorkerState>,
    val: &Value,
) {
    let has_method = val.get("method").and_then(|v| v.as_str()).is_some();
    let has_id = val.get("id").is_some_and(|v| !v.is_null());

    // Response to a client->server JSON-RPC request.
    if has_id && !has_method {
        if let Some(id_num) = val.get("id").and_then(|v| v.as_u64()) {
            let was_turn_start = worker
                .turn_state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .turn_start_rpc_ids
                .remove(&id_num);

            if was_turn_start
                && val.get("error").is_none_or(Value::is_null)
                && let Some(res) = val.get("result")
                && let Some(turn_id) = res
                    .get("turn")
                    .and_then(|t| t.get("id"))
                    .or_else(|| res.get("turnId"))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
            {
                let status = res
                    .get("turn")
                    .and_then(|t| t.get("status"))
                    .and_then(Value::as_str);
                record_turn_started_wire_ordered(worker, turn_id, status);
            }

            if let Some(tx) = worker
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
                } else if let Some(res) = val.get("result") {
                    let _ = tx.send(Ok(res.clone()));
                } else {
                    let _ = tx.send(Err("malformed JSON-RPC response missing result".into()));
                }
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
            let requested_permissions = (method == "item/permissions/requestApproval")
                .then(|| params.get("permissions").cloned())
                .flatten();
            let question_ids = match &interaction.kind {
                InteractionKind::Question { questions } => {
                    Some(questions.iter().map(|q| q.id.clone()).collect())
                }
                _ => None,
            };
            let publish_now = {
                let mut approvals = worker
                    .pending_approvals
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let was_empty = approvals.is_empty();
                approvals.push_back(PendingCodexApproval {
                    interaction: interaction.clone(),
                    rpc_id,
                    method: method.to_owned(),
                    requested_permissions,
                    question_ids,
                });
                was_empty
            };
            {
                let mut log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
                log.push_event(AgentEventKind::Other {
                    bb_type: format!("approval_request: {}", interaction.summary),
                });
            }
            if publish_now {
                worker.emit_activity(Activity::Blocked { interaction }, false);
            }
        } else if let Some(tx) = worker
            .stdin_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            let err_payload = json!({
                "jsonrpc": "2.0",
                "id": rpc_id,
                "error": {
                    "code": -32601,
                    "message": format!("unsupported server request method '{method}'"),
                }
            })
            .to_string();
            let (reply_tx, _) = oneshot::channel();
            let _ = tx.send((err_payload, reply_tx));
        }
        return;
    }

    // Server->client JSON-RPC notification.
    match method {
        "serverRequest/resolved" => {
            let Some(req_id) = params.get("requestId") else {
                return;
            };
            let next_state = {
                let mut approvals = worker
                    .pending_approvals
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let was_front = approvals.front().is_some_and(|p| &p.rpc_id == req_id);
                approvals.retain(|p| &p.rpc_id != req_id);
                was_front.then(|| approvals.front().map(|p| p.interaction.clone()))
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
        "turn/started" => {
            if let Some(tid) = params
                .get("turn")
                .and_then(|t| t.get("id"))
                .or_else(|| params.get("turnId"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
            {
                let status = params
                    .get("turn")
                    .and_then(|t| t.get("status"))
                    .and_then(Value::as_str);
                record_turn_started_wire_ordered(worker, tid, status);
            }
            let needs_working = {
                let log = worker.log.lock().unwrap_or_else(|e| e.into_inner());
                !matches!(log.activity, Activity::Working | Activity::Blocked { .. })
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
            worker
                .pending_approvals
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clear();

            let completed_tid = params
                .get("turn")
                .and_then(|t| t.get("id"))
                .or_else(|| params.get("turnId"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_owned);

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

            let (next_queued, cancelled_prompts) = {
                let mut ts = worker.turn_state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(ref tid) = completed_tid {
                    ts.completed_turns.insert(tid.clone());
                    if ts.active_turn_id.as_deref() == Some(tid.as_str()) {
                        ts.active_turn_id = None;
                    }
                } else {
                    ts.active_turn_id = None;
                }
                if !worker.retiring.load(Ordering::Acquire)
                    && matches!(activity, Activity::Idle)
                    && let Some(next) = ts.queued_turns.pop_front()
                {
                    ts.in_flight = true;
                    (Some(next), Vec::new())
                } else {
                    ts.in_flight = false;
                    let drained = if !matches!(activity, Activity::Idle) {
                        ts.queued_turns.drain(..).collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                    (None, drained)
                }
            };

            if !cancelled_prompts.is_empty() {
                let why = match &activity {
                    Activity::Refused { .. } => "turn_refused",
                    Activity::Errored { .. } => "turn_errored",
                    _ => "turn_ended",
                };
                push_cancelled_codex_prompts(&worker.log, cancelled_prompts, why);
            }

            if let Some(next_prompt) = next_queued
                && let Some(bridge) = weak_bridge.upgrade()
                && let Some(codex_tid) = worker
                    .codex_thread_id
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone()
            {
                let w = worker.clone();
                tokio::spawn(async move {
                    let _guard = w.turn_lock.lock().await;
                    if !w.retiring.load(Ordering::Acquire) && !w.retired.load(Ordering::Acquire) {
                        let _ = bridge
                            .start_codex_turn_now(&w, codex_tid, &next_prompt)
                            .await;
                    }
                });
                return;
            }

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
    use rex_wave_api::wave::{FileSystemGrant, NetworkGrant};

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
        assert!(
            encode_codex_approval_result(
                "item/commandExecution/requestApproval",
                &InteractionResolution::RequestAnswer { value: json!({}) },
            )
            .is_err(),
            "empty RequestAnswer object must be rejected"
        );
        assert!(
            encode_codex_approval_result(
                "item/commandExecution/requestApproval",
                &InteractionResolution::RequestAnswer {
                    value: json!({"decision": {"acceptWithExecpolicyAmendment": null}})
                },
            )
            .is_err(),
            "null acceptWithExecpolicyAmendment payload must be rejected"
        );
        assert!(
            encode_codex_approval_result(
                "item/commandExecution/requestApproval",
                &InteractionResolution::RequestAnswer {
                    value: json!({"decision": {"acceptWithExecpolicyAmendment": {"execpolicy_amendment": ["cargo", "test"]}}})
                },
            )
            .is_ok(),
            "valid acceptWithExecpolicyAmendment string array must pass"
        );
        assert!(
            encode_codex_approval_result_with_request(
                "item/permissions/requestApproval",
                &InteractionResolution::RequestAnswer {
                    value: json!({"permissions": {"network": {"enabled": true}}, "scope": "turn"})
                },
                Some(&json!({"network": {"enabled": false}})),
            )
            .is_err(),
            "raw RequestAnswer granting unrequested network must be rejected"
        );
        assert_eq!(
            encode_codex_approval_result(
                "item/fileChange/requestApproval",
                &InteractionResolution::Deny,
            )
            .unwrap(),
            json!({"decision": "decline"})
        );

        let offered_perms = json!({"network": {"enabled": true}});
        assert_eq!(
            encode_codex_approval_result_with_request(
                "item/permissions/requestApproval",
                &InteractionResolution::AllowForSession {
                    granted_permissions: None,
                },
                Some(&offered_perms),
            )
            .unwrap(),
            json!({"permissions": {"network": {"enabled": true}}, "scope": "session"})
        );
        // Explicitly empty granted_permissions must NOT widen back to offered_perms.
        assert_eq!(
            encode_codex_approval_result_with_request(
                "item/permissions/requestApproval",
                &InteractionResolution::AllowOnce {
                    granted_permissions: Some(GrantedPermissions {
                        file_system: Some(FileSystemGrant {
                            read: Vec::new(),
                            write: Vec::new(),
                        }),
                        network: None,
                    }),
                },
                Some(&offered_perms),
            )
            .unwrap(),
            json!({"permissions": {}, "scope": "turn"})
        );
        // Granting unrequested network permission must be rejected.
        assert!(
            encode_codex_approval_result_with_request(
                "item/permissions/requestApproval",
                &InteractionResolution::AllowOnce {
                    granted_permissions: Some(GrantedPermissions {
                        file_system: None,
                        network: Some(NetworkGrant {
                            enabled: Some(true),
                        }),
                    }),
                },
                Some(&json!({"network": {"enabled": false}})),
            )
            .is_err()
        );
        assert_eq!(
            encode_codex_approval_result_with_request(
                "item/permissions/requestApproval",
                &InteractionResolution::Deny,
                Some(&offered_perms),
            )
            .unwrap(),
            json!({"permissions": {}})
        );

        let cfg = CodexRunnerConfig::from_env()
            .with_socket_path("/tmp/rw.sock")
            .with_rex_wave_bin("/usr/local/bin/rex-wave");
        let args = build_codex_args(&cfg);
        assert_eq!(args.first().map(String::as_str), Some("app-server"));
        assert!(
            args.iter()
                .any(|a| a.starts_with("mcp_servers.rex-wave.command="))
        );
        assert!(args.iter().any(
            |a| a.starts_with("mcp_servers.rex-wave.env_vars=") && a.contains("REX_WAVE_TOKEN")
        ));

        // Token usage prefers `last` occupancy over cumulative `total` and avoids double-counting.
        let usage = parse_codex_token_usage(&json!({
            "tokenUsage": {
                "modelContextWindow": 200000,
                "total": {
                    "inputTokens": 250000,
                    "cachedInputTokens": 100000,
                    "outputTokens": 50000,
                    "reasoningOutputTokens": 10000,
                    "totalTokens": 300000
                },
                "last": {
                    "inputTokens": 10000,
                    "cachedInputTokens": 4000,
                    "outputTokens": 2000,
                    "reasoningOutputTokens": 500,
                    "totalTokens": 12000
                }
            }
        }))
        .unwrap();
        assert_eq!(usage.used_tokens, 12_000);
        let cat_sum: u64 = usage
            .categories
            .iter()
            .filter(|c| c.id != "free")
            .map(|c| c.tokens)
            .sum();
        assert_eq!(cat_sum, 12_000);

        assert_eq!(
            extract_codex_account_identity(&json!({
                "account": {"type": "chatgpt", "email": "dev@example.com", "planType": "pro"},
                "requiresOpenaiAuth": true
            })),
            Some("dev@example.com".into())
        );
        assert_eq!(
            extract_codex_account_identity(&json!({
                "account": {"type": "apiKey"},
                "requiresOpenaiAuth": true
            })),
            None
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
