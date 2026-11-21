//! Pure T3 Code 0.1.11 wire codecs, normalization, and turn-mirror inspection.

use rex_wave_api::read::{
    AgentEvent, AgentEventKind, ContextCategory, ContextCategoryKind, ThreadContextUsage,
};
use rex_wave_api::room::Body;
use rex_wave_api::wave::{
    BaseBranch, InteractionKind, InteractionResolution, PendingInteraction, PermissionMode,
    Question, SteerDeliveryMode, Workspace,
};
use rex_wave_api::{BbInteractionId, BbSeq, BbThreadId, RoleName, UnixMs, WaveSlug};
use rex_wave_waves::{HostError, SpawnRequest};
use serde_json::{Map, Value, json};

pub const T3_THREAD_PREFIX: &str = "t3:";

pub fn is_t3_thread_id(thread: &BbThreadId) -> bool {
    thread.as_str().starts_with(T3_THREAD_PREFIX)
}

pub fn deterministic_thread_id(
    wave: &WaveSlug,
    role: &RoleName,
    attempt: u32,
) -> Result<BbThreadId, HostError> {
    let raw = format!("{T3_THREAD_PREFIX}{wave}:{role}:a{attempt}");
    BbThreadId::parse(raw).map_err(|e| HostError::Rejected(e.to_string()))
}

pub fn parse_t3_thread_id(thread: &BbThreadId) -> Option<(WaveSlug, RoleName, u32)> {
    let rest = thread.as_str().strip_prefix(T3_THREAD_PREFIX)?;
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

pub fn spawn_command_id(wave: &WaveSlug, role: &RoleName, attempt: u32) -> String {
    format!("rex-wave:spawn:{wave}:{role}:a{attempt}")
}

pub fn spawn_message_id(wave: &WaveSlug, role: &RoleName, attempt: u32) -> String {
    format!("rex-wave:prompt:{wave}:{role}:a{attempt}")
}

pub fn strip_t3_provider_prefix(provider: &str) -> &str {
    provider.strip_prefix(T3_THREAD_PREFIX).unwrap_or(provider)
}

pub fn workspace_strategy_json(workspace: &Workspace) -> Value {
    match workspace {
        Workspace::ProjectDefault => json!({ "type": "root" }),
        Workspace::ManagedWorktree { base } => {
            let base_ref = match base {
                BaseBranch::Default => "HEAD",
                BaseBranch::Named { name } => name.as_str(),
            };
            json!({
                "type": "worktree",
                "baseRef": base_ref,
            })
        }
        Workspace::ExistingPath { path } => json!({
            "type": "existing_worktree",
            "worktreePath": path.to_string_lossy(),
        }),
    }
}

pub fn runtime_mode_str(permission: PermissionMode) -> &'static str {
    match permission {
        PermissionMode::AcceptEdits => "auto-accept-edits",
        PermissionMode::Auto => "auto",
        PermissionMode::Full => "full-access",
    }
}

pub fn delivery_intent_str(mode: SteerDeliveryMode) -> Option<&'static str> {
    match mode {
        SteerDeliveryMode::Auto | SteerDeliveryMode::Start => Some("auto"),
        SteerDeliveryMode::Steer | SteerDeliveryMode::SteerIfActive => Some("steer"),
        SteerDeliveryMode::QueueIfActive => None,
    }
}

pub fn launch_thread_payload(
    req: &SpawnRequest,
    thread_id: &BbThreadId,
    t3_project_id: &str,
    provider_instance: &str,
    model: &str,
) -> Value {
    let mut model_sel = Map::new();
    model_sel.insert(
        "instanceId".into(),
        Value::String(provider_instance.to_owned()),
    );
    model_sel.insert("model".into(), Value::String(model.to_owned()));

    let mut options = Vec::new();
    if let Some(reasoning) = req.agent.reasoning.as_deref().map(str::trim)
        && !reasoning.is_empty()
    {
        options.push(json!({ "id": "reasoningEffort", "value": reasoning }));
    }
    if let Some(tier) = req.agent.service_tier.as_deref().map(str::trim)
        && !tier.is_empty()
    {
        options.push(json!({ "id": "serviceTier", "value": tier }));
    }
    if !options.is_empty() {
        model_sel.insert("options".into(), Value::Array(options));
    }

    json!({
        "commandId": spawn_command_id(&req.wave, &req.role, req.attempt),
        "creationSource": "server",
        "threadId": thread_id.as_str(),
        "reuseExistingThread": true,
        "projectId": t3_project_id,
        "title": req.title,
        "modelSelection": Value::Object(model_sel),
        "runtimeMode": runtime_mode_str(req.agent.permission),
        "interactionMode": "default",
        "workspaceStrategy": workspace_strategy_json(&req.agent.workspace),
        "initialMessage": {
            "messageId": spawn_message_id(&req.wave, &req.role, req.attempt),
            "text": req.prompt.as_str(),
            "attachments": [],
        },
    })
}

pub fn message_dispatch_payload(
    thread: &BbThreadId,
    body: &Body,
    mode: SteerDeliveryMode,
    command_id: String,
    message_id: String,
) -> Value {
    let mut obj = Map::new();
    obj.insert("type".into(), Value::String("message.dispatch".into()));
    obj.insert("commandId".into(), Value::String(command_id));
    obj.insert("threadId".into(), Value::String(thread.to_string()));
    obj.insert("messageId".into(), Value::String(message_id));
    obj.insert("text".into(), Value::String(body.as_str().to_owned()));
    obj.insert("attachments".into(), Value::Array(Vec::new()));
    obj.insert("createdBy".into(), Value::String("user".into()));
    obj.insert("creationSource".into(), Value::String("server".into()));
    if let Some(intent) = delivery_intent_str(mode) {
        obj.insert("deliveryIntent".into(), Value::String(intent.into()));
    }
    Value::Object(obj)
}

pub fn runtime_request_respond_payload(
    thread: &BbThreadId,
    interaction: &BbInteractionId,
    resolution: &InteractionResolution,
    command_id: String,
) -> Result<Value, HostError> {
    let mut obj = Map::new();
    obj.insert(
        "type".into(),
        Value::String("runtime-request.respond".into()),
    );
    obj.insert("commandId".into(), Value::String(command_id));
    obj.insert("threadId".into(), Value::String(thread.to_string()));
    obj.insert("requestId".into(), Value::String(interaction.to_string()));

    match resolution {
        InteractionResolution::AllowOnce { .. } => {
            obj.insert("decision".into(), Value::String("accept".into()));
        }
        InteractionResolution::AllowForSession { .. } => {
            obj.insert("decision".into(), Value::String("acceptForSession".into()));
        }
        InteractionResolution::Deny => {
            obj.insert("decision".into(), Value::String("decline".into()));
        }
        InteractionResolution::UserAnswer { answers } => {
            let mut map = Map::new();
            for (qid, ans) in answers {
                let free = ans
                    .free_text
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let val = match (ans.selected.as_slice(), free) {
                    ([], Some(ft)) => Value::String(ft.to_owned()),
                    ([], None) => Value::String(String::new()),
                    ([single], None) => Value::String(single.clone()),
                    ([single], Some(ft)) => Value::Array(vec![
                        Value::String(single.clone()),
                        Value::String(ft.to_owned()),
                    ]),
                    (many, None) => Value::Array(many.iter().cloned().map(Value::String).collect()),
                    (many, Some(ft)) => {
                        let mut arr: Vec<Value> = many.iter().cloned().map(Value::String).collect();
                        arr.push(Value::String(ft.to_owned()));
                        Value::Array(arr)
                    }
                };
                map.insert(qid.clone(), val);
            }
            obj.insert("answers".into(), Value::Object(map));
        }
        InteractionResolution::RequestAnswer { value } => {
            if value.is_object() {
                obj.insert("answers".into(), value.clone());
            } else {
                let text = value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string());
                obj.insert("feedback".into(), Value::String(text));
            }
        }
        InteractionResolution::PluginSubmitted { .. } => {
            return Err(HostError::Rejected(
                "T3 Code runtime-request.respond does not support plugin_submitted".into(),
            ));
        }
    }

    Ok(Value::Object(obj))
}

pub fn thread_stop_payload(thread: &BbThreadId, command_id: String) -> Value {
    json!({
        "type": "thread.stop",
        "commandId": command_id,
        "threadId": thread.as_str(),
    })
}

pub fn force_stop_thread_payload(thread: &BbThreadId) -> Value {
    json!({
        "threadId": thread.as_str(),
    })
}

pub fn thread_archive_payload(thread: &BbThreadId, command_id: String) -> Value {
    json!({
        "type": "thread.archive",
        "commandId": command_id,
        "threadId": thread.as_str(),
    })
}

/// Picks the first available model for `provider_instance` from a `server.getConfig` reply.
pub fn default_model_from_server_config(config: &Value, provider_instance: &str) -> Option<String> {
    let providers = config
        .get("providers")
        .and_then(|v| v.as_array())
        .or_else(|| config.as_array())?;
    for p in providers {
        let id = p
            .get("instanceId")
            .or_else(|| p.get("provider"))
            .or_else(|| p.get("id"))
            .and_then(|v| v.as_str());
        if id != Some(provider_instance) {
            continue;
        }
        if let Some(models) = p.get("models").and_then(|v| v.as_array()) {
            for m in models {
                if let Some(slug) = m
                    .get("slug")
                    .or_else(|| m.get("id"))
                    .or_else(|| m.get("model"))
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    return Some(slug.to_owned());
                }
                if let Some(slug) = m.as_str().map(str::trim).filter(|s| !s.is_empty()) {
                    return Some(slug.to_owned());
                }
            }
        }
    }
    None
}

/// High-level observation derived from a T3 `OrchestrationV2ThreadShell` update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShellObservation {
    Archived,
    Blocked {
        request_id: String,
        request_kind: Option<String>,
    },
    Working,
    Errored(String),
    Idle {
        mirror_candidate: bool,
    },
}

pub fn derive_shell_observation(shell: &Value, location: Option<&str>) -> Option<ShellObservation> {
    if location == Some("archive")
        || shell
            .get("archivedAt")
            .is_some_and(|v| !v.is_null() && v.as_str().is_none_or(|s| !s.is_empty()))
    {
        return Some(ShellObservation::Archived);
    }

    if let Some(pending) = shell.get("pendingRuntimeRequest").filter(|v| v.is_object()) {
        let blocking = pending
            .get("blocking")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if blocking
            && let Some(req_id) = pending
                .get("id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
        {
            let kind = pending
                .get("kind")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
            return Some(ShellObservation::Blocked {
                request_id: req_id.to_owned(),
                request_kind: kind,
            });
        }
    }

    let status = shell
        .get("status")
        .and_then(|v| v.as_str())
        .or_else(|| shell.get("activityRunStatus").and_then(|v| v.as_str()))?;

    match status {
        "preparing" | "queued" | "starting" | "running" | "waiting" => {
            Some(ShellObservation::Working)
        }
        "failed" => {
            let err = shell
                .get("lastError")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .unwrap_or("T3 thread failed")
                .to_owned();
            Some(ShellObservation::Errored(err))
        }
        "idle" | "completed" => Some(ShellObservation::Idle {
            mirror_candidate: true,
        }),
        "interrupted" | "cancelled" | "rolled_back" => Some(ShellObservation::Idle {
            mirror_candidate: false,
        }),
        _ => None,
    }
}

/// Unwraps either `{ snapshotSequence, projection }` or a bare projection object.
pub fn unwrap_projection(raw: &Value) -> &Value {
    raw.get("projection")
        .filter(|v| v.is_object())
        .unwrap_or(raw)
}

pub fn parse_pending_interaction_from_projection(
    projection: &Value,
    request_id: &str,
    fallback_kind: Option<&str>,
) -> Option<PendingInteraction> {
    let id = BbInteractionId::parse(request_id).ok()?;
    let proj = unwrap_projection(projection);

    if let Some(items) = proj.get("turnItems").and_then(|v| v.as_array()) {
        for item in items.iter().rev() {
            if item.get("requestId").and_then(|v| v.as_str()) != Some(request_id) {
                continue;
            }
            if let Some(parsed) = parse_pending_interaction_from_turn_item(item, &id) {
                return Some(parsed);
            }
        }
    }

    let kind_from_req = proj
        .get("runtimeRequests")
        .and_then(|v| v.as_array())
        .and_then(|reqs| {
            reqs.iter()
                .find(|r| r.get("id").and_then(|v| v.as_str()) == Some(request_id))
        })
        .and_then(|r| r.get("kind").and_then(|v| v.as_str()))
        .or(fallback_kind);

    let (kind, summary) = match kind_from_req {
        Some("user_input") => (
            InteractionKind::Question {
                questions: Vec::new(),
            },
            "Question from worker".to_owned(),
        ),
        Some(k @ ("command" | "file-read" | "file-change" | "mcp-elicitation" | "permission")) => (
            InteractionKind::Permission,
            format!("Permission approval requested ({k})"),
        ),
        Some(other) => (InteractionKind::Extension, other.to_owned()),
        None => (
            InteractionKind::Permission,
            "Permission approval requested".to_owned(),
        ),
    };
    Some(PendingInteraction { id, kind, summary })
}

fn parse_pending_interaction_from_turn_item(
    item: &Value,
    id: &BbInteractionId,
) -> Option<PendingInteraction> {
    match item.get("type").and_then(|v| v.as_str())? {
        "approval_request" => {
            let summary = item
                .get("reason")
                .or_else(|| item.get("prompt"))
                .or_else(|| item.get("title"))
                .or_else(|| item.get("appName"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("Permission approval requested")
                .to_owned();
            Some(PendingInteraction {
                id: id.clone(),
                kind: InteractionKind::Permission,
                summary,
            })
        }
        "user_input_request" => {
            let mut questions = Vec::new();
            if let Some(arr) = item.get("questions").and_then(|v| v.as_array()) {
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
                            } else if let Some(lbl) = o
                                .get("label")
                                .or_else(|| o.get("value"))
                                .and_then(|v| v.as_str())
                            {
                                options.push(lbl.to_owned());
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
                .or_else(|| {
                    item.get("title")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| "Question from worker".into());
            Some(PendingInteraction {
                id: id.clone(),
                kind: InteractionKind::Question { questions },
                summary,
            })
        }
        _ => None,
    }
}

/// Finds the active pending interaction from a projection's `runtimeRequests` + `turnItems`.
pub fn active_pending_interaction(projection: &Value) -> Option<PendingInteraction> {
    let proj = unwrap_projection(projection);
    if let Some(reqs) = proj.get("runtimeRequests").and_then(|v| v.as_array()) {
        for req in reqs.iter().rev() {
            if req.get("status").and_then(|v| v.as_str()) != Some("pending") {
                continue;
            }
            if let Some(req_id) = req.get("id").and_then(|v| v.as_str()) {
                let kind = req.get("kind").and_then(|v| v.as_str());
                return parse_pending_interaction_from_projection(proj, req_id, kind);
            }
        }
    }
    None
}

/// Normalizes one T3 `OrchestrationV2TurnItem` into a `rex_wave_api::read::AgentEvent`.
pub fn normalize_turn_item(item: &Value, fallback_ordinal: u64) -> Option<AgentEvent> {
    let ty = item.get("type").and_then(|v| v.as_str())?;
    let ordinal = item
        .get("ordinal")
        .and_then(|v| v.as_u64())
        .unwrap_or(fallback_ordinal);
    let seq = BbSeq(ordinal);
    let at = item
        .get("completedAt")
        .filter(|v| !v.is_null())
        .or_else(|| item.get("updatedAt").filter(|v| !v.is_null()))
        .or_else(|| item.get("startedAt").filter(|v| !v.is_null()))
        .map(parse_timestamp_ms)
        .unwrap_or(UnixMs(0));

    let status = item
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("inProgress");
    let is_terminal = matches!(
        status,
        "completed" | "failed" | "error" | "declined" | "cancelled" | "interrupted"
    ) || item.get("completedAt").is_some_and(|v| !v.is_null());

    let kind = match ty {
        "user_message" => {
            let text = item
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned();
            AgentEventKind::UserMessage { text }
        }
        "assistant_message" => {
            let text = item
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned();
            AgentEventKind::AssistantText { text }
        }
        "command_execution" => {
            let summary = item
                .get("input")
                .or_else(|| item.get("title"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned();
            if is_terminal {
                let output_failed = item
                    .get("outputIndicatesFailure")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let exit_ok = item
                    .get("exitCode")
                    .and_then(|v| v.as_i64())
                    .is_none_or(|code| code == 0);
                let ok = status == "completed" && !output_failed && exit_ok;
                AgentEventKind::ToolResult {
                    name: "command_execution".into(),
                    ok,
                    summary,
                }
            } else {
                AgentEventKind::ToolCall {
                    name: "command_execution".into(),
                    summary,
                }
            }
        }
        "file_change" => {
            let file_name = item
                .get("fileName")
                .or_else(|| item.get("title"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let adds = item.get("additions").and_then(|v| v.as_u64());
            let dels = item.get("deletions").and_then(|v| v.as_u64());
            let summary = match (adds, dels) {
                (Some(a), Some(d)) => format!("{file_name} (+{a}/-{d})"),
                _ => file_name.to_owned(),
            };
            if is_terminal {
                AgentEventKind::ToolResult {
                    name: "file_change".into(),
                    ok: status == "completed",
                    summary,
                }
            } else {
                AgentEventKind::ToolCall {
                    name: "file_change".into(),
                    summary,
                }
            }
        }
        "dynamic_tool" => {
            let name = item
                .get("toolName")
                .or_else(|| item.get("title"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("dynamic_tool")
                .to_owned();
            let summary = item
                .get("title")
                .or_else(|| item.get("viewedImagePath"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .or_else(|| {
                    item.get("input")
                        .filter(|v| !v.is_null())
                        .map(|v| truncate_utf8(&v.to_string(), 160).to_owned())
                })
                .unwrap_or_default();
            if is_terminal {
                AgentEventKind::ToolResult {
                    name,
                    ok: status == "completed",
                    summary,
                }
            } else {
                AgentEventKind::ToolCall { name, summary }
            }
        }
        "approval_request" => {
            let detail = item
                .get("reason")
                .or_else(|| item.get("prompt"))
                .or_else(|| item.get("title"))
                .or_else(|| item.get("requestKind"))
                .and_then(|v| v.as_str())
                .unwrap_or("permission");
            AgentEventKind::Other {
                bb_type: format!("approval_request: {detail}"),
            }
        }
        "user_input_request" => {
            let detail = item
                .get("questions")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|q| q.get("question").or_else(|| q.get("header")))
                .and_then(|v| v.as_str())
                .or_else(|| item.get("title").and_then(|v| v.as_str()))
                .unwrap_or("question");
            AgentEventKind::Other {
                bb_type: format!("user_input_request: {detail}"),
            }
        }
        "error" => {
            let msg = item
                .get("failure")
                .and_then(|f| f.get("message"))
                .and_then(|v| v.as_str())
                .or_else(|| item.get("message").and_then(|v| v.as_str()))
                .unwrap_or("T3 provider error");
            AgentEventKind::Other {
                bb_type: format!("error: {msg}"),
            }
        }
        other => AgentEventKind::Other {
            bb_type: format!("t3:{other}"),
        },
    };

    Some(AgentEvent { seq, at, kind })
}

pub fn parse_token_usage(usage: &Value) -> Option<ThreadContextUsage> {
    if usage.is_null() {
        return None;
    }
    let used_tokens = usage.get("usedTokens")?.as_u64()?;
    let model_context_window = usage.get("maxTokens").and_then(|v| v.as_u64()).unwrap_or(0);
    let auto_compact_at_tokens = usage.get("autoCompactThreshold").and_then(|v| v.as_u64());

    let mut categories = Vec::new();
    if let Some(tokens) = usage.get("inputTokens").and_then(|v| v.as_u64()) {
        categories.push(ContextCategory {
            id: "input".into(),
            label: "Input".into(),
            kind: ContextCategoryKind::Used,
            tokens,
        });
    }
    if let Some(tokens) = usage.get("cachedInputTokens").and_then(|v| v.as_u64()) {
        categories.push(ContextCategory {
            id: "cached_input".into(),
            label: "Cached input".into(),
            kind: ContextCategoryKind::Reserved,
            tokens,
        });
    }
    if let Some(tokens) = usage.get("outputTokens").and_then(|v| v.as_u64()) {
        categories.push(ContextCategory {
            id: "output".into(),
            label: "Output".into(),
            kind: ContextCategoryKind::Used,
            tokens,
        });
    }
    if let Some(tokens) = usage.get("reasoningOutputTokens").and_then(|v| v.as_u64()) {
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
        estimated: false,
        auto_compact_at_tokens,
        cost_usd: None,
        categories,
    })
}

pub fn parse_provider_turns_usage(projection: &Value) -> Option<ThreadContextUsage> {
    let proj = unwrap_projection(projection);
    let turns = proj.get("providerTurns")?.as_array()?;
    let mut best: Option<(u64, ThreadContextUsage)> = None;
    for (idx, turn) in turns.iter().enumerate() {
        let Some(tu) = turn.get("tokenUsage").filter(|v| v.is_object()) else {
            continue;
        };
        let Some(parsed) = parse_token_usage(tu) else {
            continue;
        };
        let ord = turn
            .get("ordinal")
            .and_then(|v| v.as_u64())
            .unwrap_or(idx as u64);
        if best.as_ref().is_none_or(|(prev, _)| ord >= *prev) {
            best = Some((ord, parsed));
        }
    }
    best.map(|(_, u)| u)
}

/// Normalizes a live `OrchestrationV2DomainEvent` from `orchestration.subscribeThread`.
pub fn normalize_domain_event(stream_seq: u64, event: &Value) -> Option<AgentEvent> {
    let ty = event.get("type").and_then(|v| v.as_str())?;
    let payload = event.get("payload").unwrap_or(&Value::Null);
    let at = event
        .get("occurredAt")
        .or_else(|| event.get("createdAt"))
        .or_else(|| payload.get("updatedAt"))
        .map(parse_timestamp_ms)
        .unwrap_or(UnixMs(0));

    match ty {
        "turn-item.updated" | "turn-item.created" => normalize_turn_item(payload, stream_seq),
        "run.created" => Some(AgentEvent {
            seq: BbSeq(stream_seq),
            at,
            kind: AgentEventKind::TurnStarted,
        }),
        "run.updated" => {
            let status = payload.get("status").and_then(|v| v.as_str())?;
            if matches!(
                status,
                "completed" | "failed" | "interrupted" | "cancelled" | "rolled_back"
            ) {
                Some(AgentEvent {
                    seq: BbSeq(stream_seq),
                    at,
                    kind: AgentEventKind::TurnEnded,
                })
            } else {
                None
            }
        }
        "provider-turn.updated" | "provider-turn.created" => {
            let tu = payload.get("tokenUsage")?;
            let usage = parse_token_usage(tu)?;
            Some(AgentEvent {
                seq: BbSeq(stream_seq),
                at,
                kind: AgentEventKind::Usage { usage },
            })
        }
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedTurn {
    pub anchor: String,
    pub run_start_at: u64,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnMirrorCheck {
    Ignore,
    PostedViaTool,
    Unposted(CompletedTurn),
}

pub fn inspect_latest_completed_turn(projection: &Value) -> TurnMirrorCheck {
    let proj = unwrap_projection(projection);
    let Some(items) = proj.get("turnItems").and_then(|v| v.as_array()) else {
        return TurnMirrorCheck::Ignore;
    };
    if items.is_empty() {
        return TurnMirrorCheck::Ignore;
    }

    let runs = proj.get("runs").and_then(|v| v.as_array());
    let latest_run = runs.and_then(|arr| {
        arr.iter()
            .max_by_key(|r| r.get("ordinal").and_then(|v| v.as_u64()).unwrap_or(0))
    });

    let (target_run_id, run_start_at) = if let Some(run) = latest_run {
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "completed" {
            return TurnMirrorCheck::Ignore;
        }
        let id = run.get("id").and_then(|v| v.as_str()).map(str::to_owned);
        let started = run
            .get("startedAt")
            .or_else(|| run.get("createdAt"))
            .map(parse_timestamp_ms)
            .map_or(0, |u| u.0);
        (id, started)
    } else {
        (None, 0)
    };

    let mut ordered: Vec<&Value> = items.iter().collect();
    ordered.sort_by_key(|it| it.get("ordinal").and_then(|v| v.as_u64()).unwrap_or(0));

    let slice: Vec<&Value> = if let Some(ref run_id) = target_run_id {
        let filtered: Vec<&Value> = ordered
            .iter()
            .copied()
            .filter(|it| it.get("runId").and_then(|v| v.as_str()) == Some(run_id.as_str()))
            .collect();
        if filtered.is_empty() {
            ordered
        } else {
            filtered
        }
    } else {
        let start = ordered
            .iter()
            .rposition(|it| it.get("type").and_then(|v| v.as_str()) == Some("user_message"))
            .unwrap_or(0);
        ordered[start..].to_vec()
    };

    for item in &slice {
        if item.get("type").and_then(|v| v.as_str()) != Some("dynamic_tool") {
            continue;
        }
        let status = item
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("completed");
        if matches!(status, "failed" | "error" | "cancelled" | "declined") {
            continue;
        }
        let tool = item.get("toolName").and_then(|v| v.as_str()).unwrap_or("");
        if matches!(tool, "rex_room_post" | "room_post") {
            let is_dm = item
                .get("input")
                .and_then(|a| a.get("room"))
                .and_then(|rm| rm.get("ref"))
                .and_then(|v| v.as_str())
                == Some("dm");
            if !is_dm {
                return TurnMirrorCheck::PostedViaTool;
            }
        }
    }

    let mut last_assistant: Option<(String, String)> = None;
    for item in &slice {
        if item.get("type").and_then(|v| v.as_str()) != Some("assistant_message") {
            continue;
        }
        if item
            .get("streaming")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            continue;
        }
        let Some(text) = item.get("text").and_then(|v| v.as_str()) else {
            continue;
        };
        if text.trim().is_empty() {
            continue;
        }
        let item_key = item
            .get("messageId")
            .or_else(|| item.get("id"))
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .unwrap_or_else(|| {
                format!(
                    "o{}",
                    item.get("ordinal").and_then(|v| v.as_u64()).unwrap_or(0)
                )
            });
        last_assistant = Some((item_key, text.to_owned()));
    }

    let Some((msg_key, text)) = last_assistant else {
        return TurnMirrorCheck::Ignore;
    };

    let anchor = match target_run_id {
        Some(run_id) => format!("r{run_id}:{msg_key}"),
        None => msg_key,
    };

    TurnMirrorCheck::Unposted(CompletedTurn {
        anchor,
        run_start_at,
        text,
    })
}

pub fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

pub fn parse_timestamp_ms(val: &Value) -> UnixMs {
    if let Some(n) = val.as_u64() {
        return UnixMs(n);
    }
    if let Some(s) = val.as_str()
        && let Some(ms) = parse_rfc3339_ms(s)
    {
        return UnixMs(ms);
    }
    UnixMs(0)
}

/// Parses an RFC 3339 / ISO-8601 timestamp (`YYYY-MM-DDTHH:MM:SS(.sss)?(Z|±HH:MM)`) into Unix ms.
pub fn parse_rfc3339_ms(raw: &str) -> Option<u64> {
    let s = raw.trim();
    let bytes = s.as_bytes();
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b't' | b' ')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year: i64 = s[0..4].parse().ok()?;
    let month: u32 = s[5..7].parse().ok()?;
    let day: u32 = s[8..10].parse().ok()?;
    let hour: u64 = s[11..13].parse().ok()?;
    let min: u64 = s[14..16].parse().ok()?;
    let sec: u64 = s[17..19].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 60 {
        return None;
    }

    let mut idx = 19usize;
    let mut millis: u64 = 0;
    if idx < bytes.len() && bytes[idx] == b'.' {
        idx += 1;
        let frac_start = idx;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        let frac = &s[frac_start..idx];
        if !frac.is_empty() {
            let mut scaled = 0u64;
            for (pos, b) in frac.bytes().take(3).enumerate() {
                let digit = (b - b'0') as u64;
                let factor = match pos {
                    0 => 100,
                    1 => 10,
                    _ => 1,
                };
                scaled += digit * factor;
            }
            millis = scaled;
        }
    }

    let tz = &s[idx..];
    let offset_secs: i64 = if tz == "Z" || tz == "z" || tz.is_empty() {
        0
    } else if tz.len() == 6
        && (tz.starts_with('+') || tz.starts_with('-'))
        && tz.as_bytes()[3] == b':'
    {
        let sign = if tz.starts_with('-') { -1i64 } else { 1i64 };
        let th: i64 = tz[1..3].parse().ok()?;
        let tm: i64 = tz[4..6].parse().ok()?;
        sign * (th * 3600 + tm * 60)
    } else {
        return None;
    };

    let days = days_from_civil(year, month, day);
    let secs = days * 86_400 + (hour * 3600 + min * 60 + sec) as i64 - offset_secs;
    if secs < 0 {
        return None;
    }
    Some((secs as u64).saturating_mul(1000).saturating_add(millis))
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let m = month as u64;
    let d = day as u64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    use rex_wave_api::wave::{AgentSpec, UserAnswer};
    use rex_wave_api::{BbProjectId, ModelId, ProviderId};
    use std::collections::BTreeMap;

    #[test]
    fn deterministic_thread_id_round_trips() {
        let wave = WaveSlug::parse("council-v1").unwrap();
        let role = RoleName::parse("gemini-review").unwrap();
        let tid = deterministic_thread_id(&wave, &role, 2).unwrap();
        assert_eq!(tid.as_str(), "t3:council-v1:gemini-review:a2");
        assert!(is_t3_thread_id(&tid));
        assert_eq!(parse_t3_thread_id(&tid), Some((wave, role, 2)));
        assert_eq!(
            parse_t3_thread_id(&BbThreadId::parse("th_bb_123").unwrap()),
            None
        );
    }

    #[test]
    fn maps_launch_tell_resolve_and_turn_items_exactly() {
        let wave = WaveSlug::parse("w1").unwrap();
        let role = RoleName::parse("reviewer").unwrap();
        let project = BbProjectId::parse("proj_abc").unwrap();
        let room = rex_wave_waves::wave_room(&wave, &project);
        let req = SpawnRequest {
            wave: wave.clone(),
            role: role.clone(),
            attempt: 1,
            project,
            agent: AgentSpec {
                provider: ProviderId::parse("t3:antigravity").unwrap(),
                model: Some(ModelId::parse("gemini-4-neon").unwrap()),
                reasoning: Some("xhigh".into()),
                service_tier: None,
                permission: PermissionMode::AcceptEdits,
                workspace: Workspace::ManagedWorktree {
                    base: BaseBranch::Named {
                        name: "main".into(),
                    },
                },
            },
            room,
            prompt: Body::parse("review PR").unwrap(),
            title: "[w1] @reviewer".into(),
        };
        let tid = deterministic_thread_id(&wave, &role, 1).unwrap();
        let payload =
            launch_thread_payload(&req, &tid, "uuid-proj-1", "antigravity", "gemini-4-neon");
        assert_eq!(payload["threadId"], "t3:w1:reviewer:a1");
        assert_eq!(payload["commandId"], "rex-wave:spawn:w1:reviewer:a1");
        assert_eq!(payload["reuseExistingThread"], true);
        assert_eq!(payload["creationSource"], "server");
        assert_eq!(payload["runtimeMode"], "auto-accept-edits");
        assert_eq!(
            payload["workspaceStrategy"],
            json!({"type": "worktree", "baseRef": "main"})
        );
        assert_eq!(payload["modelSelection"]["instanceId"], "antigravity");
        assert_eq!(payload["modelSelection"]["model"], "gemini-4-neon");

        assert_eq!(
            workspace_strategy_json(&Workspace::ProjectDefault),
            json!({"type": "root"})
        );
        assert_eq!(
            workspace_strategy_json(&Workspace::ManagedWorktree {
                base: BaseBranch::Default
            }),
            json!({"type": "worktree", "baseRef": "HEAD"})
        );
        assert_eq!(
            workspace_strategy_json(&Workspace::ExistingPath {
                path: "/tmp/wt".into()
            }),
            json!({"type": "existing_worktree", "worktreePath": "/tmp/wt"})
        );

        assert_eq!(runtime_mode_str(PermissionMode::Auto), "auto");
        assert_eq!(runtime_mode_str(PermissionMode::Full), "full-access");

        let tell_auto = message_dispatch_payload(
            &tid,
            &Body::parse("hi").unwrap(),
            SteerDeliveryMode::Auto,
            "c1".into(),
            "m1".into(),
        );
        assert_eq!(tell_auto["deliveryIntent"], "auto");
        let tell_steer = message_dispatch_payload(
            &tid,
            &Body::parse("stop").unwrap(),
            SteerDeliveryMode::SteerIfActive,
            "c2".into(),
            "m2".into(),
        );
        assert_eq!(tell_steer["deliveryIntent"], "steer");
        let tell_queue = message_dispatch_payload(
            &tid,
            &Body::parse("later").unwrap(),
            SteerDeliveryMode::QueueIfActive,
            "c3".into(),
            "m3".into(),
        );
        assert!(tell_queue.get("deliveryIntent").is_none());

        let iid = BbInteractionId::parse("req_1").unwrap();
        assert_eq!(
            runtime_request_respond_payload(
                &tid,
                &iid,
                &InteractionResolution::AllowOnce {
                    granted_permissions: None
                },
                "c4".into(),
            )
            .unwrap()["decision"],
            "accept"
        );
        assert_eq!(
            runtime_request_respond_payload(
                &tid,
                &iid,
                &InteractionResolution::AllowForSession {
                    granted_permissions: None
                },
                "c5".into(),
            )
            .unwrap()["decision"],
            "acceptForSession"
        );
        assert_eq!(
            runtime_request_respond_payload(&tid, &iid, &InteractionResolution::Deny, "c6".into())
                .unwrap()["decision"],
            "decline"
        );

        let mut answers = BTreeMap::new();
        answers.insert(
            "q1".into(),
            UserAnswer {
                selected: vec!["yes".into()],
                free_text: None,
            },
        );
        answers.insert(
            "q2".into(),
            UserAnswer {
                selected: vec!["a".into(), "b".into()],
                free_text: None,
            },
        );
        let ans_payload = runtime_request_respond_payload(
            &tid,
            &iid,
            &InteractionResolution::UserAnswer { answers },
            "c7".into(),
        )
        .unwrap();
        assert_eq!(ans_payload["answers"]["q1"], "yes");
        assert_eq!(ans_payload["answers"]["q2"], json!(["a", "b"]));

        assert_eq!(
            parse_rfc3339_ms("2026-11-21T00:00:00.500Z"),
            Some(1_795_219_200_500)
        );
    }
}
