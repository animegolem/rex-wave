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
        .filter(|s| !s.is_empty())?;
    ModelId::parse(raw).ok()
}

pub fn session_supports_models(result: &Value) -> bool {
    result.get("models").is_some_and(Value::is_object)
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

pub fn select_permission_response(
    req: &ParsedPermissionRequest,
    resolution: &InteractionResolution,
) -> Result<Value, HostError> {
    let pick = |preferred_kinds: &[&str], fallback_needle: &str| -> Option<String> {
        for want in preferred_kinds {
            if let Some(opt) = req.options.iter().find(|o| o.kind == *want) {
                return Some(opt.option_id.clone());
            }
        }
        req.options
            .iter()
            .find(|o| {
                o.kind.contains(fallback_needle)
                    || o.option_id.to_ascii_lowercase().contains(fallback_needle)
                    || o.name.to_ascii_lowercase().contains(fallback_needle)
            })
            .map(|o| o.option_id.clone())
    };

    match resolution {
        InteractionResolution::AllowOnce { .. } => {
            let option_id = pick(&["allow_once", "allow_always"], "allow")
                .unwrap_or_else(|| "allow_once".to_owned());
            Ok(permission_selected_response(req.rpc_id.clone(), &option_id))
        }
        InteractionResolution::AllowForSession { .. } => {
            let option_id = pick(&["allow_always", "allow_once"], "allow")
                .unwrap_or_else(|| "allow_always".to_owned());
            Ok(permission_selected_response(req.rpc_id.clone(), &option_id))
        }
        InteractionResolution::Deny => {
            match pick(&["reject_once", "reject_always"], "reject").or_else(|| pick(&[], "deny")) {
                Some(option_id) => Ok(permission_selected_response(req.rpc_id.clone(), &option_id)),
                None => Ok(permission_cancelled_response(req.rpc_id.clone())),
            }
        }
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

    let Some(res) = result else {
        return PromptOutcome::Idle { usage: None };
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

    if let Some(sr) = stop_reason {
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
    }

    PromptOutcome::Idle { usage }
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

        let allow_session = select_permission_response(
            &parsed,
            &InteractionResolution::AllowForSession {
                granted_permissions: None,
            },
        )
        .unwrap();
        assert_eq!(
            allow_session["result"]["outcome"],
            json!({ "outcome": "selected", "optionId": "opt_always" })
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
            PromptOutcome::Idle { usage: None }
        );
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
    }
}
