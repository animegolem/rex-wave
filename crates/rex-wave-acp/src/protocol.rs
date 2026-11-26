//! Pure ACP JSON-RPC 2.0 wire builders, parsers, `AgentEvent` normalizers,
//! permission mapping, and refusal/error classification.

use rex_wave_api::read::{
    AgentEventKind, ContextCategory, ContextCategoryKind, ThreadContextUsage,
};
use rex_wave_api::wave::{
    Activity, InteractionKind, InteractionResolution, PendingInteraction, is_refusal_error_text,
    is_refusal_stop_reason,
};
use rex_wave_api::{
    ACP_THREAD_PREFIX, HostInteractionId, HostThreadId, ModelId, RoleName, RunnerKind, WaveSlug,
    truncate_with_notice,
};
use rex_wave_waves::HostError;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;
use std::process::ExitStatus;

pub const ACP_PROTOCOL_VERSION: u64 = 1;
const MAX_SUMMARY_BYTES: usize = 512;

pub fn is_acp_thread_id(thread: &HostThreadId) -> bool {
    thread.runner_kind() == RunnerKind::Acp
}

pub fn deterministic_thread_id(
    wave: &WaveSlug,
    role: &RoleName,
    attempt: u32,
) -> Result<HostThreadId, HostError> {
    let raw = format!("{ACP_THREAD_PREFIX}{wave}:{role}:a{attempt}");
    HostThreadId::parse(raw).map_err(|e| HostError::Rejected(e.to_string()))
}

pub fn parse_acp_thread_id(thread: &HostThreadId) -> Option<(WaveSlug, RoleName, u32)> {
    let rest = thread.as_str().strip_prefix(ACP_THREAD_PREFIX)?;
    let mut parts = rest.split(':');
    let wave_raw = parts.next()?;
    let role_raw = parts.next()?;
    let attempt_raw = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let attempt_str = attempt_raw.strip_prefix('a')?;
    let attempt: u32 = attempt_str.parse().ok()?;
    if attempt == 0 {
        return None;
    }
    let wave = WaveSlug::parse(wave_raw).ok()?;
    let role = RoleName::parse(role_raw).ok()?;
    Some((wave, role, attempt))
}

pub fn initialize_request(id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": {
            "protocolVersion": ACP_PROTOCOL_VERSION,
            "clientCapabilities": {
                "fs": {
                    "readTextFile": false,
                    "writeTextFile": false,
                },
                "terminal": false,
            },
            "clientInfo": {
                "name": "rex-wave",
                "version": env!("CARGO_PKG_VERSION"),
            },
        },
    })
}

pub fn mcp_server_entry(
    rex_wave_bin: &Path,
    socket_path: &Path,
    token: &str,
    thread: &HostThreadId,
) -> Value {
    let sock = socket_path.to_string_lossy().into_owned();
    json!({
        "name": "rex-wave",
        "command": rex_wave_bin.to_string_lossy().into_owned(),
        "args": ["--socket", sock.clone(), "mcp", "--stdio"],
        "env": [
            { "name": "REX_WAVE_TOKEN", "value": token },
            { "name": "REX_WAVE_SOCKET", "value": sock },
            { "name": "REX_WAVE_WORKER_THREAD_ID", "value": thread.as_str() },
        ],
    })
}

pub fn session_new_request(id: u64, cwd: &Path, mcp_servers: Vec<Value>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/new",
        "params": {
            "cwd": cwd.to_string_lossy().into_owned(),
            "mcpServers": mcp_servers,
        },
    })
}

pub fn session_set_model_request(id: u64, session_id: &str, model_id: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/set_model",
        "params": {
            "sessionId": session_id,
            "modelId": model_id,
        },
    })
}

pub fn session_set_config_option_request(
    id: u64,
    session_id: &str,
    config_id: &str,
    value: &str,
) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/set_config_option",
        "params": {
            "sessionId": session_id,
            "configId": config_id,
            "value": value,
        },
    })
}

pub fn session_prompt_request(id: u64, session_id: &str, text: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/prompt",
        "params": {
            "sessionId": session_id,
            "prompt": [
                {
                    "type": "text",
                    "text": text,
                }
            ],
        },
    })
}

pub fn session_cancel_notification(session_id: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {
            "sessionId": session_id,
        },
    })
}

pub fn permission_selected_response(rpc_id: Value, option_id: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": rpc_id,
        "result": {
            "outcome": {
                "outcome": "selected",
                "optionId": option_id,
            }
        },
    })
}

pub fn permission_cancelled_response(rpc_id: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": rpc_id,
        "result": {
            "outcome": {
                "outcome": "cancelled",
            }
        },
    })
}

pub fn method_not_found_response(rpc_id: Value, method: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": rpc_id,
        "error": {
            "code": -32601,
            "message": format!("method not found: {method}"),
        },
    })
}

#[derive(Clone, Debug, PartialEq)]
pub enum InboundMessage {
    Response {
        id: Value,
        result: Option<Value>,
        error: Option<Value>,
    },
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    Notification {
        method: String,
        params: Value,
    },
}

pub fn parse_inbound_line(line: &str) -> Option<InboundMessage> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    let val: Value = serde_json::from_str(trimmed).ok()?;
    let obj = val.as_object()?;
    let has_id = obj.get("id").is_some_and(|v| !v.is_null());
    let method = obj.get("method").and_then(Value::as_str).map(str::to_owned);
    match (has_id, method) {
        (true, Some(method)) => Some(InboundMessage::Request {
            id: obj.get("id").cloned().unwrap_or(Value::Null),
            method,
            params: obj.get("params").cloned().unwrap_or(Value::Null),
        }),
        (false, Some(method)) => Some(InboundMessage::Notification {
            method,
            params: obj.get("params").cloned().unwrap_or(Value::Null),
        }),
        (true, None) => Some(InboundMessage::Response {
            id: obj.get("id").cloned().unwrap_or(Value::Null),
            result: obj.get("result").cloned(),
            error: obj.get("error").cloned(),
        }),
        (false, None) => None,
    }
}

pub fn rpc_id_matches(id: &Value, expected: u64) -> bool {
    id.as_u64() == Some(expected)
        || id
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .is_some_and(|n| n == expected)
}

pub fn validate_initialize_result(result: &Value) -> Result<(), HostError> {
    let Some(obj) = result.as_object() else {
        return Err(HostError::Rejected(
            "ACP initialize reply must be a JSON object".into(),
        ));
    };
    let Some(ver) = obj
        .get("protocolVersion")
        .or_else(|| obj.get("protocol_version"))
    else {
        return Err(HostError::Rejected(
            "ACP initialize reply missing protocolVersion".into(),
        ));
    };
    let parsed_ver = ver
        .as_u64()
        .or_else(|| ver.as_str().and_then(|s| s.trim().parse::<u64>().ok()));
    if parsed_ver != Some(ACP_PROTOCOL_VERSION) {
        return Err(HostError::Rejected(format!(
            "unsupported ACP protocolVersion {ver}; expected {ACP_PROTOCOL_VERSION}"
        )));
    }
    Ok(())
}

pub fn extract_session_id(result: &Value) -> Result<String, HostError> {
    result
        .get("sessionId")
        .or_else(|| result.get("session_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| HostError::Rejected("ACP session/new reply missing sessionId".into()))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcpModelConfigOption {
    pub config_id: String,
    pub current_value: Option<String>,
    pub allowed_values: Vec<String>,
}

pub fn extract_model_config_option(result: &Value) -> Option<AcpModelConfigOption> {
    let arr = result
        .get("configOptions")
        .or_else(|| result.get("config_options"))
        .and_then(Value::as_array)?;
    for item in arr {
        let category = item
            .get("category")
            .and_then(Value::as_str)
            .map(|s| s.trim().to_ascii_lowercase());
        let id = item
            .get("id")
            .or_else(|| item.get("configId"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let is_model = category.as_deref() == Some("model")
            || id.is_some_and(|i| i.eq_ignore_ascii_case("model"));
        if !is_model {
            continue;
        }
        let config_id = id?.to_owned();
        let current_value = item
            .get("currentValue")
            .or_else(|| item.get("current_value"))
            .or_else(|| item.get("value"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        let mut allowed_values = Vec::new();
        if let Some(opts) = item.get("options").and_then(Value::as_array) {
            for opt in opts {
                if let Some(v) = opt
                    .get("value")
                    .or_else(|| opt.get("id"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    allowed_values.push(v.to_owned());
                } else if let Some(group_opts) = opt.get("options").and_then(Value::as_array) {
                    for gopt in group_opts {
                        if let Some(v) = gopt
                            .get("value")
                            .or_else(|| gopt.get("id"))
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                        {
                            allowed_values.push(v.to_owned());
                        }
                    }
                }
            }
        }
        return Some(AcpModelConfigOption {
            config_id,
            current_value,
            allowed_values,
        });
    }
    None
}

pub fn extract_session_model(result: &Value) -> Option<ModelId> {
    let raw = result
        .get("models")
        .and_then(|m| {
            m.get("currentModelId")
                .or_else(|| m.get("current_model_id"))
                .and_then(Value::as_str)
        })
        .or_else(|| result.get("model").and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .or_else(|| extract_model_config_option(result).and_then(|o| o.current_value))?;
    ModelId::parse(raw).ok()
}

pub fn session_supports_models(result: &Value) -> bool {
    result.get("models").is_some_and(Value::is_object)
}

pub fn model_is_listed_in_models_obj(result: &Value, requested: &str) -> bool {
    let Some(avail) = result
        .get("models")
        .and_then(|m| {
            m.get("availableModels")
                .or_else(|| m.get("available_models"))
        })
        .and_then(Value::as_array)
    else {
        return true;
    };
    if avail.is_empty() {
        return true;
    }
    avail.iter().any(|m| {
        m.get("modelId")
            .or_else(|| m.get("model_id"))
            .or_else(|| m.get("id"))
            .and_then(Value::as_str)
            .is_some_and(|id| id.trim() == requested)
    })
}

pub fn validate_selected_config_model(
    reply: &Value,
    config_id: &str,
    requested: &str,
) -> Result<ModelId, HostError> {
    let observed = reply
        .get("configOptions")
        .or_else(|| reply.get("config_options"))
        .and_then(Value::as_array)
        .and_then(|arr| {
            arr.iter().find(|item| {
                item.get("id")
                    .or_else(|| item.get("configId"))
                    .and_then(Value::as_str)
                    .is_some_and(|id| id.trim() == config_id)
            })
        })
        .and_then(|item| {
            item.get("currentValue")
                .or_else(|| item.get("current_value"))
                .or_else(|| item.get("value"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
        });

    if observed.as_deref() != Some(requested) {
        return Err(HostError::Rejected(format!(
            "ACP provider did not honor requested model '{requested}' for configId '{config_id}' in session/set_config_option reply (currentValue={observed:?})"
        )));
    }
    ModelId::parse(requested).map_err(|e| HostError::Rejected(e.to_string()))
}

pub fn validate_selected_models_obj(reply: &Value, requested: &str) -> Result<ModelId, HostError> {
    if !reply.is_object() {
        return Err(HostError::Rejected(
            "ACP session/set_model reply must be a JSON object".into(),
        ));
    }
    if let Some(actual) = extract_session_model(reply) {
        if actual.as_str() != requested {
            return Err(HostError::Rejected(format!(
                "ACP provider did not honor requested model '{requested}' in session/set_model reply (currentModelId='{}')",
                actual.as_str()
            )));
        }
        return Ok(actual);
    }
    ModelId::parse(requested).map_err(|e| HostError::Rejected(e.to_string()))
}

pub fn is_cancellation_jsonrpc_error(err: Option<&Value>, formatted_msg: &str) -> bool {
    if is_refusal_error_text(formatted_msg) {
        return false;
    }
    if err.and_then(jsonrpc_error_code) == Some(-32800) {
        return true;
    }
    let low = formatted_msg.trim().to_ascii_lowercase();
    matches!(
        low.as_str(),
        "cancelled"
            | "canceled"
            | "request cancelled"
            | "request canceled"
            | "prompt cancelled"
            | "prompt canceled"
            | "turn cancelled"
            | "turn canceled"
            | "session cancelled"
            | "session canceled"
    )
}

pub fn format_jsonrpc_error(err: &Value) -> String {
    let msg = err
        .get("message")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("ACP JSON-RPC error");
    let detail = err.get("data").and_then(|d| {
        if let Some(s) = d.as_str().map(str::trim).filter(|s| !s.is_empty()) {
            Some(s.to_owned())
        } else if let Some(obj) = d.as_object() {
            obj.get("message")
                .or_else(|| obj.get("reason"))
                .or_else(|| obj.get("error"))
                .or_else(|| obj.get("details"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
        } else {
            None
        }
    });
    match detail {
        Some(d) if !msg.to_ascii_lowercase().contains(&d.to_ascii_lowercase()) => {
            format!("{msg}: {d}")
        }
        _ => msg.to_owned(),
    }
}

pub fn jsonrpc_error_code(err: &Value) -> Option<i64> {
    err.get("code").and_then(Value::as_i64)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcpPermissionOption {
    pub option_id: String,
    pub kind: String,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedPermissionRequest {
    pub interaction: PendingInteraction,
    pub rpc_id: Value,
    pub options: Vec<AcpPermissionOption>,
}

fn build_interaction_id(rpc_id: &Value, fallback_seq: u64) -> HostInteractionId {
    let raw_suffix = match rpc_id {
        Value::Number(n) => n.to_string(),
        Value::String(s) if !s.trim().is_empty() => s
            .trim()
            .chars()
            .filter(|c| !c.is_control())
            .take(180)
            .collect(),
        _ => format!("seq-{fallback_seq}"),
    };
    let candidate = format!("acp-perm-{raw_suffix}");
    HostInteractionId::parse(candidate).unwrap_or_else(|_| {
        HostInteractionId::parse(format!("acp-perm-seq-{fallback_seq}"))
            .expect("valid fallback interaction id")
    })
}

pub fn parse_permission_request(
    rpc_id: Value,
    params: &Value,
    fallback_seq: u64,
) -> ParsedPermissionRequest {
    let id = build_interaction_id(&rpc_id, fallback_seq);
    let tool_call = params.get("toolCall").or_else(|| params.get("tool_call"));
    let summary = tool_call
        .and_then(|tc| {
            tc.get("title")
                .or_else(|| tc.get("name"))
                .or_else(|| tc.get("kind"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            params
                .get("reason")
                .or_else(|| params.get("prompt"))
                .or_else(|| params.get("message"))
                .or_else(|| params.get("title"))
                .and_then(Value::as_str)
        })
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| truncate_with_notice(s, MAX_SUMMARY_BYTES))
        .unwrap_or_else(|| "Permission approval requested".to_owned());

    let mut options = Vec::new();
    if let Some(arr) = params.get("options").and_then(Value::as_array) {
        for item in arr {
            let Some(option_id) = item
                .get("optionId")
                .or_else(|| item.get("option_id"))
                .or_else(|| item.get("id"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                continue;
            };
            let kind = item
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase();
            let name = item
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or(option_id)
                .trim()
                .to_owned();
            options.push(AcpPermissionOption {
                option_id: option_id.to_owned(),
                kind,
                name,
            });
        }
    }

    ParsedPermissionRequest {
        interaction: PendingInteraction {
            id,
            kind: InteractionKind::Permission,
            summary,
        },
        rpc_id,
        options,
    }
}

fn reject_custom_granted_permissions(
    granted: Option<&rex_wave_api::wave::GrantedPermissions>,
) -> Result<(), HostError> {
    if let Some(g) = granted
        && (g.file_system.is_some() || g.network.is_some())
    {
        return Err(HostError::Rejected(
            "ACP permission options cannot represent custom granted_permissions".into(),
        ));
    }
    Ok(())
}

pub fn select_permission_response(
    req: &ParsedPermissionRequest,
    resolution: &InteractionResolution,
) -> Result<Value, HostError> {
    let find_exact_kind = |want: &str| -> Option<String> {
        req.options
            .iter()
            .find(|o| o.kind == want)
            .map(|o| o.option_id.clone())
    };

    match resolution {
        InteractionResolution::AllowOnce {
            granted_permissions,
        } => {
            reject_custom_granted_permissions(granted_permissions.as_ref())?;
            let option_id = find_exact_kind("allow_once").ok_or_else(|| {
                HostError::Rejected(
                    "ACP permission request did not offer an allow_once option".into(),
                )
            })?;
            Ok(permission_selected_response(req.rpc_id.clone(), &option_id))
        }
        InteractionResolution::AllowForSession {
            granted_permissions,
        } => {
            reject_custom_granted_permissions(granted_permissions.as_ref())?;
            Err(HostError::Rejected(
                "ACP v1 permission options have no session-scoped grant (allow_always exceeds session lifetime); retry with allow_once"
                    .into(),
            ))
        }
        InteractionResolution::Deny => match find_exact_kind("reject_once") {
            Some(option_id) => Ok(permission_selected_response(req.rpc_id.clone(), &option_id)),
            None => Ok(permission_cancelled_response(req.rpc_id.clone())),
        },
        InteractionResolution::UserAnswer { .. }
        | InteractionResolution::RequestAnswer { .. }
        | InteractionResolution::PluginSubmitted { .. } => Err(HostError::Rejected(
            "ACP permission request only accepts allow_once, allow_for_session, or deny".into(),
        )),
    }
}

fn extract_content_text(val: &Value) -> Option<String> {
    if let Some(s) = val.as_str() {
        return (!s.is_empty()).then(|| s.to_owned());
    }
    if let Some(obj) = val.as_object() {
        if let Some(text) = obj.get("text").and_then(Value::as_str)
            && !text.is_empty()
        {
            return Some(text.to_owned());
        }
        if let Some(nested) = obj.get("content") {
            return extract_content_text(nested);
        }
        return None;
    }
    if let Some(arr) = val.as_array() {
        let mut parts = Vec::new();
        for item in arr {
            if let Some(text) = extract_content_text(item) {
                parts.push(text);
            }
        }
        if !parts.is_empty() {
            return Some(parts.join("\n"));
        }
    }
    None
}

fn summarize_tool_payload(update: &Value, prefer_output: bool) -> String {
    let mut candidates = Vec::new();
    if prefer_output {
        if let Some(raw_out) = update.get("rawOutput").or_else(|| update.get("raw_output")) {
            candidates.push(raw_out);
        }
        if let Some(content) = update.get("content") {
            candidates.push(content);
        }
    } else {
        if let Some(title) = update.get("title") {
            candidates.push(title);
        }
        if let Some(raw_in) = update.get("rawInput").or_else(|| update.get("raw_input")) {
            candidates.push(raw_in);
        }
        if let Some(content) = update.get("content") {
            candidates.push(content);
        }
    }
    if let Some(title) = update.get("title") {
        candidates.push(title);
    }

    for cand in candidates {
        if cand.is_null() {
            continue;
        }
        if let Some(text) = extract_content_text(cand) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                return truncate_with_notice(trimmed, MAX_SUMMARY_BYTES);
            }
        }
        if cand.is_object() || cand.is_array() {
            let rendered = cand.to_string();
            if rendered != "{}" && rendered != "[]" {
                return truncate_with_notice(&rendered, MAX_SUMMARY_BYTES);
            }
        }
    }
    String::new()
}

/// Parses ACP `usage_update` or token-usage payloads into [`ThreadContextUsage`].
pub fn parse_acp_usage(val: &Value) -> Option<ThreadContextUsage> {
    let obj = val
        .get("usage")
        .filter(|v| v.is_object())
        .or_else(|| val.get("tokenUsage").filter(|v| v.is_object()))
        .unwrap_or(val);
    if !obj.is_object() {
        return None;
    }

    let input_tokens = obj
        .get("inputTokens")
        .or_else(|| obj.get("input_tokens"))
        .and_then(Value::as_u64);
    let output_tokens = obj
        .get("outputTokens")
        .or_else(|| obj.get("output_tokens"))
        .and_then(Value::as_u64);
    let cached_tokens = obj
        .get("cachedInputTokens")
        .or_else(|| obj.get("cachedReadTokens"))
        .or_else(|| obj.get("cached_input_tokens"))
        .or_else(|| obj.get("cached_read_tokens"))
        .and_then(Value::as_u64);
    let reasoning_tokens = obj
        .get("reasoningTokens")
        .or_else(|| obj.get("reasoningOutputTokens"))
        .or_else(|| obj.get("thoughtTokens"))
        .or_else(|| obj.get("reasoning_tokens"))
        .and_then(Value::as_u64);

    let used_tokens = obj
        .get("used")
        .or_else(|| obj.get("usedTokens"))
        .or_else(|| obj.get("used_tokens"))
        .or_else(|| obj.get("totalTokens"))
        .or_else(|| obj.get("total_tokens"))
        .and_then(Value::as_u64)
        .or_else(|| {
            if input_tokens.is_some() || output_tokens.is_some() {
                Some(
                    input_tokens
                        .unwrap_or(0)
                        .saturating_add(output_tokens.unwrap_or(0))
                        .saturating_add(reasoning_tokens.unwrap_or(0)),
                )
            } else {
                None
            }
        })?;

    let model_context_window = obj
        .get("size")
        .or_else(|| obj.get("maxTokens"))
        .or_else(|| obj.get("max_tokens"))
        .or_else(|| obj.get("contextWindow"))
        .or_else(|| obj.get("context_window"))
        .or_else(|| obj.get("modelContextWindow"))
        .and_then(Value::as_u64)
        .unwrap_or(0);

    let auto_compact_at_tokens = obj
        .get("autoCompactThreshold")
        .or_else(|| obj.get("auto_compact_at_tokens"))
        .and_then(Value::as_u64);

    let cost_usd = obj
        .get("cost")
        .and_then(|c| {
            c.get("amount")
                .and_then(Value::as_f64)
                .or_else(|| c.as_f64())
        })
        .or_else(|| {
            obj.get("costUsd")
                .or_else(|| obj.get("cost_usd"))
                .and_then(Value::as_f64)
        });

    let mut categories = Vec::new();
    if let Some(tokens) = input_tokens {
        categories.push(ContextCategory {
            id: "input".into(),
            label: "Input".into(),
            kind: ContextCategoryKind::Used,
            tokens,
        });
    }
    if let Some(tokens) = cached_tokens {
        categories.push(ContextCategory {
            id: "cached_input".into(),
            label: "Cached input".into(),
            kind: ContextCategoryKind::Reserved,
            tokens,
        });
    }
    if let Some(tokens) = output_tokens {
        categories.push(ContextCategory {
            id: "output".into(),
            label: "Output".into(),
            kind: ContextCategoryKind::Used,
            tokens,
        });
    }
    if let Some(tokens) = reasoning_tokens {
        categories.push(ContextCategory {
            id: "reasoning".into(),
            label: "Reasoning".into(),
            kind: ContextCategoryKind::Used,
            tokens,
        });
    }
    if !categories.is_empty() && model_context_window > used_tokens {
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
        estimated: obj
            .get("estimated")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        auto_compact_at_tokens,
        cost_usd,
        categories,
    })
}

/// Normalizes one ACP `session/update` notification payload into an [`AgentEventKind`].
pub fn normalize_session_update(
    params: &Value,
    tool_names: &mut HashMap<String, String>,
) -> Option<AgentEventKind> {
    let update = params
        .get("update")
        .filter(|v| v.is_object())
        .unwrap_or(params);
    let kind = update
        .get("sessionUpdate")
        .or_else(|| update.get("session_update"))
        .or_else(|| update.get("type"))
        .and_then(Value::as_str)?;

    match kind {
        "agent_message_chunk" => {
            let text = update
                .get("content")
                .and_then(extract_content_text)
                .or_else(|| {
                    update
                        .get("text")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })?;
            if text.is_empty() {
                return None;
            }
            Some(AgentEventKind::AssistantText { text })
        }
        "user_message_chunk" => {
            let text = update
                .get("content")
                .and_then(extract_content_text)
                .or_else(|| {
                    update
                        .get("text")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })?;
            if text.is_empty() {
                return None;
            }
            Some(AgentEventKind::UserMessage { text })
        }
        "agent_thought_chunk" => {
            let preview = update
                .get("content")
                .and_then(extract_content_text)
                .or_else(|| {
                    update
                        .get("text")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .map(|t| truncate_with_notice(t.trim(), 160))
                .unwrap_or_default();
            let bb_type = if preview.is_empty() {
                "acp:thought".to_owned()
            } else {
                format!("acp:thought: {preview}")
            };
            Some(AgentEventKind::Other { bb_type })
        }
        "tool_call" | "tool_call_update" => {
            let call_id = update
                .get("toolCallId")
                .or_else(|| update.get("tool_call_id"))
                .or_else(|| update.get("id"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            let explicit_name = update
                .get("toolName")
                .or_else(|| update.get("tool_name"))
                .or_else(|| update.get("name"))
                .or_else(|| update.get("kind"))
                .or_else(|| update.get("title"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned);

            let name = match (&call_id, explicit_name) {
                (Some(cid), Some(n)) => {
                    tool_names.insert(cid.clone(), n.clone());
                    n
                }
                (Some(cid), None) => tool_names
                    .get(cid)
                    .cloned()
                    .unwrap_or_else(|| "tool_call".to_owned()),
                (None, Some(n)) => n,
                (None, None) => "tool_call".to_owned(),
            };

            let status = update
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("in_progress")
                .trim()
                .to_ascii_lowercase();
            let is_terminal = matches!(
                status.as_str(),
                "completed" | "failed" | "error" | "cancelled" | "canceled"
            );
            let summary = summarize_tool_payload(update, is_terminal);
            if is_terminal {
                Some(AgentEventKind::ToolResult {
                    name,
                    ok: status == "completed",
                    summary,
                })
            } else {
                Some(AgentEventKind::ToolCall { name, summary })
            }
        }
        "usage_update" => {
            let usage = parse_acp_usage(update)?;
            Some(AgentEventKind::Usage { usage })
        }
        other => Some(AgentEventKind::Other {
            bb_type: format!("acp:{other}"),
        }),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PromptOutcome {
    Idle {
        usage: Option<ThreadContextUsage>,
        stop_reason: String,
    },
    Cancelled,
    Refused {
        reason: String,
        usage: Option<ThreadContextUsage>,
    },
    Errored {
        error: String,
        usage: Option<ThreadContextUsage>,
    },
}

fn extract_refusal_from_value(obj: &Value) -> Option<String> {
    let candidates = [
        obj.get("stopReason").and_then(Value::as_str),
        obj.get("stop_reason").and_then(Value::as_str),
        obj.get("finishReason").and_then(Value::as_str),
        obj.get("finish_reason").and_then(Value::as_str),
        obj.get("errorCategory").and_then(Value::as_str),
        obj.get("providerCode").and_then(Value::as_str),
        obj.get("status").and_then(Value::as_str),
    ];
    let detail = obj
        .get("reason")
        .or_else(|| obj.get("message"))
        .or_else(|| obj.get("error"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());

    for sr in candidates.into_iter().flatten() {
        if is_refusal_stop_reason(sr) {
            return Some(match detail {
                Some(m) if !m.to_ascii_lowercase().contains(&sr.to_ascii_lowercase()) => {
                    format!("{sr}: {m}")
                }
                Some(m) => m.to_owned(),
                None => sr.to_owned(),
            });
        }
    }
    if let Some(m) = detail
        && is_refusal_error_text(m)
    {
        return Some(m.to_owned());
    }
    None
}

pub fn classify_prompt_response(result: Option<&Value>, error: Option<&Value>) -> PromptOutcome {
    if let Some(err) = error {
        let formatted = format_jsonrpc_error(err);
        if let Some(data) = err.get("data")
            && let Some(reason) = extract_refusal_from_value(data)
        {
            return PromptOutcome::Refused {
                reason,
                usage: None,
            };
        }
        if is_refusal_error_text(&formatted) {
            return PromptOutcome::Refused {
                reason: formatted,
                usage: None,
            };
        }
        return PromptOutcome::Errored {
            error: formatted,
            usage: None,
        };
    }

    let Some(res) = result.filter(|v| v.is_object()) else {
        return PromptOutcome::Errored {
            error: "malformed ACP session/prompt response: expected result object".into(),
            usage: None,
        };
    };
    let usage = parse_acp_usage(res).or_else(|| res.get("_meta").and_then(parse_acp_usage));

    let stop_reason = res
        .get("stopReason")
        .or_else(|| res.get("stop_reason"))
        .or_else(|| res.get("finishReason"))
        .or_else(|| res.get("finish_reason"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let Some(sr) = stop_reason else {
        return PromptOutcome::Errored {
            error: "malformed ACP session/prompt response: missing stopReason".into(),
            usage,
        };
    };

    let low = sr.to_ascii_lowercase();
    if is_refusal_stop_reason(sr) {
        let reason = extract_refusal_from_value(res).unwrap_or_else(|| sr.to_owned());
        return PromptOutcome::Refused { reason, usage };
    }
    if matches!(low.as_str(), "cancelled" | "canceled") {
        return PromptOutcome::Cancelled;
    }
    if matches!(low.as_str(), "error" | "failed") {
        let detail = res
            .get("error")
            .or_else(|| res.get("message"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("ACP prompt failed")
            .to_owned();
        if is_refusal_error_text(&detail) {
            return PromptOutcome::Refused {
                reason: detail,
                usage,
            };
        }
        return PromptOutcome::Errored {
            error: detail,
            usage,
        };
    }

    PromptOutcome::Idle {
        usage,
        stop_reason: sr.to_owned(),
    }
}

pub fn classify_process_exit(status: Option<ExitStatus>, stderr_tail: &str) -> Activity {
    let trimmed_err = stderr_tail.trim();
    if !trimmed_err.is_empty() && is_refusal_error_text(trimmed_err) {
        return Activity::Refused {
            reason: truncate_with_notice(trimmed_err, MAX_SUMMARY_BYTES),
        };
    }
    let exit_desc = match status {
        Some(s) => match s.code() {
            Some(code) => format!("ACP child process exited with status {code}"),
            None => "ACP child process terminated by signal".to_owned(),
        },
        None => "ACP child stdout closed unexpectedly".to_owned(),
    };
    let error = if trimmed_err.is_empty() {
        exit_desc
    } else {
        format!(
            "{exit_desc}: {}",
            truncate_with_notice(trimmed_err, MAX_SUMMARY_BYTES)
        )
    };
    Activity::Errored { error }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_id_roundtrips_and_validates() {
        let wave = WaveSlug::parse("sprint-1").unwrap();
        let role = RoleName::parse("author").unwrap();
        let tid = deterministic_thread_id(&wave, &role, 2).unwrap();
        assert_eq!(tid.as_str(), "acp:sprint-1:author:a2");
        assert!(is_acp_thread_id(&tid));
        assert_eq!(parse_acp_thread_id(&tid), Some((wave, role, 2)));

        let t3 = HostThreadId::parse("t3:sprint-1:author:a1").unwrap();
        assert!(!is_acp_thread_id(&t3));
        assert_eq!(parse_acp_thread_id(&t3), None);
    }

    #[test]
    fn normalizes_acp_session_updates_and_remembers_tool_names() {
        let mut names = HashMap::new();

        let chunk = json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": "hello world" }
            }
        });
        assert_eq!(
            normalize_session_update(&chunk, &mut names),
            Some(AgentEventKind::AssistantText {
                text: "hello world".into()
            })
        );

        let tc = json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "tool_call",
                "toolCallId": "call-1",
                "title": "cargo test",
                "kind": "execute",
                "status": "in_progress"
            }
        });
        assert_eq!(
            normalize_session_update(&tc, &mut names),
            Some(AgentEventKind::ToolCall {
                name: "execute".into(),
                summary: "cargo test".into(),
            })
        );

        let tcu = json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "tool_call_update",
                "toolCallId": "call-1",
                "status": "completed",
                "rawOutput": { "stdout": "ok" }
            }
        });
        assert_eq!(
            normalize_session_update(&tcu, &mut names),
            Some(AgentEventKind::ToolResult {
                name: "execute".into(),
                ok: true,
                summary: "{\"stdout\":\"ok\"}".into(),
            })
        );

        let usage = json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "usage_update",
                "used": 1200,
                "size": 200000,
                "cost": { "amount": 0.02, "currency": "USD" }
            }
        });
        assert!(matches!(
            normalize_session_update(&usage, &mut names),
            Some(AgentEventKind::Usage { usage }) if usage.used_tokens == 1200 && usage.model_context_window == 200000 && usage.cost_usd == Some(0.02)
        ));
    }

    #[test]
    fn maps_permission_request_and_resolutions() {
        let params = json!({
            "sessionId": "s1",
            "toolCall": {
                "toolCallId": "tc_1",
                "title": "Run shell command",
                "kind": "execute"
            },
            "options": [
                { "optionId": "opt_once", "name": "Allow once", "kind": "allow_once" },
                { "optionId": "opt_always", "name": "Allow always", "kind": "allow_always" },
                { "optionId": "opt_reject", "name": "Reject", "kind": "reject_once" }
            ]
        });
        let parsed = parse_permission_request(json!(9), &params, 1);
        assert_eq!(parsed.interaction.id.as_str(), "acp-perm-9");
        assert_eq!(parsed.interaction.kind, InteractionKind::Permission);
        assert_eq!(parsed.interaction.summary, "Run shell command");

        let allow_once = select_permission_response(
            &parsed,
            &InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        )
        .unwrap();
        assert_eq!(
            allow_once["result"]["outcome"],
            json!({ "outcome": "selected", "optionId": "opt_once" })
        );

        assert!(
            select_permission_response(
                &parsed,
                &InteractionResolution::AllowForSession {
                    granted_permissions: None,
                },
            )
            .is_err()
        );

        // If only allow_always is offered, neither AllowOnce nor AllowForSession may widen scope.
        let always_only = parse_permission_request(
            json!(10),
            &json!({
                "sessionId": "s1",
                "options": [
                    { "optionId": "opt_always", "name": "Allow always", "kind": "allow_always" }
                ]
            }),
            2,
        );
        assert!(
            select_permission_response(
                &always_only,
                &InteractionResolution::AllowOnce {
                    granted_permissions: None
                }
            )
            .is_err()
        );
        assert!(
            select_permission_response(
                &always_only,
                &InteractionResolution::AllowForSession {
                    granted_permissions: None
                }
            )
            .is_err()
        );
        assert!(
            select_permission_response(
                &parsed,
                &InteractionResolution::AllowOnce {
                    granted_permissions: Some(rex_wave_api::wave::GrantedPermissions {
                        file_system: Some(rex_wave_api::wave::FileSystemGrant {
                            read: vec!["/etc".into()],
                            write: Vec::new(),
                        }),
                        network: None,
                    })
                }
            )
            .is_err()
        );

        let deny = select_permission_response(&parsed, &InteractionResolution::Deny).unwrap();
        assert_eq!(
            deny["result"]["outcome"],
            json!({ "outcome": "selected", "optionId": "opt_reject" })
        );
    }

    #[test]
    fn classifies_refusal_error_cancelled_and_idle() {
        assert_eq!(
            classify_prompt_response(Some(&json!({ "stopReason": "end_turn" })), None),
            PromptOutcome::Idle {
                usage: None,
                stop_reason: "end_turn".into()
            }
        );
        assert!(matches!(
            classify_prompt_response(None, None),
            PromptOutcome::Errored { .. }
        ));
        assert_eq!(
            classify_prompt_response(Some(&json!({ "stopReason": "cancelled" })), None),
            PromptOutcome::Cancelled
        );
        assert!(matches!(
            classify_prompt_response(
                Some(&json!({ "stopReason": "refusal", "message": "safety policy" })),
                None
            ),
            PromptOutcome::Refused { ref reason, .. } if reason.contains("refusal")
        ));
        assert!(matches!(
            classify_prompt_response(
                None,
                Some(&json!({ "code": -32000, "message": "content_filter triggered" }))
            ),
            PromptOutcome::Refused { ref reason, .. } if reason.contains("content_filter")
        ));
        assert!(matches!(
            classify_prompt_response(
                None,
                Some(&json!({ "code": -32000, "message": "upstream timeout" }))
            ),
            PromptOutcome::Errored { ref error, .. } if error == "upstream timeout"
        ));

        // Strict cancellation JSON-RPC error classification does not swallow ordinary DB/network errors.
        assert!(!is_cancellation_jsonrpc_error(
            Some(&json!({ "code": -32603, "message": "upstream database connection interrupted" })),
            "upstream database connection interrupted"
        ));
        assert!(is_cancellation_jsonrpc_error(
            Some(&json!({ "code": -32800, "message": "Server error" })),
            "Server error"
        ));
        assert!(is_cancellation_jsonrpc_error(
            Some(&json!({ "code": -32000, "message": "Request cancelled" })),
            "Request cancelled"
        ));

        // Model reply validators require exact configId and object envelope.
        assert!(
            validate_selected_config_model(
                &json!({
                    "configOptions": [{
                        "id": "secondary-model",
                        "category": "model",
                        "currentValue": "chosen"
                    }]
                }),
                "worker-model",
                "chosen"
            )
            .is_err()
        );
        assert!(validate_selected_models_obj(&Value::Null, "chosen").is_err());
        assert_eq!(
            validate_selected_models_obj(&json!({}), "chosen")
                .unwrap()
                .as_str(),
            "chosen"
        );
    }
}
