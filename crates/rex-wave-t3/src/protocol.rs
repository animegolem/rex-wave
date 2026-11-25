//! Pure T3 Code 0.1.11 wire codecs, normalization, and turn-mirror inspection.

use rex_wave_api::read::{
    AgentEvent, AgentEventKind, ContextCategory, ContextCategoryKind, ThreadContextUsage,
};
use rex_wave_api::room::Body;
use rex_wave_api::wave::{
    BaseBranch, InteractionKind, InteractionResolution, PendingInteraction, PermissionMode,
    Question, SteerDeliveryMode, Workspace,
};
use rex_wave_api::{BbInteractionId, BbSeq, BbThreadId, RoleName, RunnerKind, UnixMs, WaveSlug};
use rex_wave_waves::{HostError, SpawnRequest};
use serde_json::{Map, Value, json};
use std::collections::HashSet;

pub use rex_wave_api::T3_THREAD_PREFIX;
pub use rex_wave_waves::mirror_idem_key;
pub const ITEM_SEQ_STRIDE: u64 = 1_000;

/// Maps `(turn_item_ordinal, is_terminal)` into a collision-free monotonic `BbSeq`.
/// In-progress updates use `+1`, terminal updates use `+2`, and live meta events
/// (`TurnStarted`, `TurnEnded`, `Usage`) allocate in `+3..+999`.
pub fn item_event_seq(ordinal: u64, is_terminal: bool) -> BbSeq {
    let base = ordinal.saturating_add(1).saturating_mul(ITEM_SEQ_STRIDE);
    BbSeq(base.saturating_add(if is_terminal { 2 } else { 1 }))
}

/// Allocates unique, causally ordered `BbSeq` values for a live `subscribeThread` stream.
#[derive(Default)]
pub struct LiveSeqTracker {
    emitted: HashSet<u64>,
    high_ordinal: Option<u64>,
    meta_offset: u64,
}

impl LiveSeqTracker {
    pub fn accept_turn_item(&mut self, item: &Value, fallback_ordinal: u64) -> Option<AgentEvent> {
        if item.get("type").and_then(|v| v.as_str()) == Some("assistant_message")
            && item
                .get("streaming")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        {
            return None;
        }
        let ordinal = item
            .get("ordinal")
            .and_then(|v| v.as_u64())
            .unwrap_or(fallback_ordinal);
        let ev = normalize_turn_item(item, fallback_ordinal)?;
        if !self.emitted.insert(ev.seq.0) {
            return None;
        }
        if self.high_ordinal.is_none_or(|prev| ordinal > prev) {
            self.high_ordinal = Some(ordinal);
            self.meta_offset = 0;
        }
        Some(ev)
    }

    pub fn next_meta_seq(&mut self) -> Option<BbSeq> {
        let base = match self.high_ordinal {
            Some(ord) => ord
                .saturating_add(1)
                .saturating_mul(ITEM_SEQ_STRIDE)
                .saturating_add(3),
            None => 1,
        };
        let limit = match self.high_ordinal {
            Some(_) => ITEM_SEQ_STRIDE - 3,
            None => ITEM_SEQ_STRIDE - 1,
        };
        if self.meta_offset >= limit {
            return None;
        }
        let seq = base.saturating_add(self.meta_offset);
        self.meta_offset += 1;
        self.emitted.insert(seq);
        Some(BbSeq(seq))
    }

    pub fn accept_domain_event(&mut self, stream_seq: u64, event: &Value) -> Option<AgentEvent> {
        let ty = event.get("type").and_then(|v| v.as_str())?;
        let payload = event.get("payload").unwrap_or(&Value::Null);
        let at = event
            .get("occurredAt")
            .or_else(|| event.get("createdAt"))
            .or_else(|| payload.get("updatedAt"))
            .map(parse_timestamp_ms)
            .unwrap_or(UnixMs(0));

        match ty {
            "turn-item.updated" | "turn-item.created" => self.accept_turn_item(payload, stream_seq),
            "run.created" => Some(AgentEvent {
                seq: self.next_meta_seq()?,
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
                        seq: self.next_meta_seq()?,
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
                    seq: self.next_meta_seq()?,
                    at,
                    kind: AgentEventKind::Usage { usage },
                })
            }
            _ => None,
        }
    }
}

pub fn is_t3_thread_id(thread: &BbThreadId) -> bool {
    thread.runner_kind() == RunnerKind::T3
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

/// Maps `SteerDeliveryMode` to T3 v0.1.11's required `dispatchMode` plus optional `deliveryIntent`.
pub fn message_dispatch_mode(mode: SteerDeliveryMode) -> (Value, Option<&'static str>) {
    match mode {
        SteerDeliveryMode::Auto | SteerDeliveryMode::QueueIfActive => {
            (json!({ "type": "queue_after_active" }), None)
        }
        SteerDeliveryMode::Start => (json!({ "type": "start_immediately" }), None),
        SteerDeliveryMode::Steer => (json!({ "type": "start_immediately" }), Some("restart")),
        SteerDeliveryMode::SteerIfActive => (json!({ "type": "start_immediately" }), Some("steer")),
    }
}

pub fn delivery_intent_str(mode: SteerDeliveryMode) -> Option<&'static str> {
    message_dispatch_mode(mode).1
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

    // Omit `reuseExistingThread`: T3 v0.1.11 rejects `reuseExistingThread: true`
    // for unknown thread IDs via `validateReusableThread`; idempotent retries
    // replay safely through T3's `commandId` receipt.
    json!({
        "commandId": spawn_command_id(&req.wave, &req.role, req.attempt),
        "creationSource": "server",
        "threadId": thread_id.as_str(),
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
    let (dispatch_mode, intent) = message_dispatch_mode(mode);
    let mut obj = Map::new();
    obj.insert("type".into(), Value::String("message.dispatch".into()));
    obj.insert("commandId".into(), Value::String(command_id));
    obj.insert("threadId".into(), Value::String(thread.to_string()));
    obj.insert("messageId".into(), Value::String(message_id));
    obj.insert("text".into(), Value::String(body.as_str().to_owned()));
    obj.insert("attachments".into(), Value::Array(Vec::new()));
    obj.insert("createdBy".into(), Value::String("user".into()));
    obj.insert("creationSource".into(), Value::String("server".into()));
    obj.insert("dispatchMode".into(), dispatch_mode);
    if let Some(intent) = intent {
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
                    ([], Some(ft)) | ([_], Some(ft)) => Value::String(ft.to_owned()),
                    ([], None) => Value::String(String::new()),
                    ([single], None) => Value::String(single.clone()),
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
    Refused(String),
    Errored(String),
    Idle {
        mirror_candidate: bool,
    },
}

/// Inspect only explicit stop/finish reason or provider error-code fields on a T3 object.
/// Safe on `idle`/`completed` shells where a stale `lastError` from an earlier run may linger.
pub fn extract_explicit_stop_refusal(obj: &Value) -> Option<String> {
    let candidates = [
        obj.get("stopReason").and_then(|v| v.as_str()),
        obj.get("stop_reason").and_then(|v| v.as_str()),
        obj.get("finishReason").and_then(|v| v.as_str()),
        obj.get("finish_reason").and_then(|v| v.as_str()),
        obj.get("status").and_then(|v| v.as_str()),
        obj.get("errorCategory").and_then(|v| v.as_str()),
        obj.get("providerCode").and_then(|v| v.as_str()),
    ];
    for sr in candidates.into_iter().flatten() {
        if rex_wave_api::wave::is_refusal_stop_reason(sr) {
            return Some(sr.to_owned());
        }
    }
    None
}

/// Inspect a failed T3 thread shell, run object, or error payload for safety/policy refusal signals.
pub fn extract_refusal_reason(obj: &Value) -> Option<String> {
    let msg = obj
        .get("lastError")
        .or_else(|| obj.get("error"))
        .or_else(|| obj.get("message"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if let Some(sr) = extract_explicit_stop_refusal(obj) {
        return Some(match msg {
            Some(m) if !m.to_ascii_lowercase().contains(&sr.to_ascii_lowercase()) => {
                format!("{sr}: {m}")
            }
            Some(m) => m.to_owned(),
            None => sr,
        });
    }
    if let Some(m) = msg
        && rex_wave_api::wave::is_refusal_error_text(m)
    {
        return Some(m.to_owned());
    }
    None
}

/// Extract host-reported runtime `(actual_model, actual_account)` from a T3 shell, launch reply,
/// or projection. Ignores launch-time `modelSelection` echoes so unconfirmed requests stay `None`.
pub fn extract_runtime_meta_from_value(
    raw: &Value,
) -> (Option<rex_wave_api::ModelId>, Option<String>) {
    let proj = unwrap_projection(raw);
    let thread = proj.get("thread").unwrap_or(proj);
    let latest_run = proj.get("runs").and_then(|v| v.as_array()).and_then(|arr| {
        arr.iter()
            .max_by_key(|r| r.get("ordinal").and_then(|v| v.as_u64()).unwrap_or(0))
    });

    let model_str = latest_run
        .and_then(|r| {
            r.get("actualModel")
                .or_else(|| r.get("model"))
                .and_then(|v| v.as_str())
        })
        .or_else(|| {
            thread
                .get("actualModel")
                .or_else(|| thread.get("model"))
                .and_then(|v| v.as_str())
        })
        .or_else(|| {
            proj.get("actualModel")
                .or_else(|| proj.get("model"))
                .and_then(|v| v.as_str())
        })
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let model = model_str.and_then(|s| rex_wave_api::ModelId::parse(s).ok());

    fn direct_account(obj: &Value) -> Option<&str> {
        obj.get("actualAccount")
            .or_else(|| obj.get("actual_account"))
            .or_else(|| obj.get("accountEmail"))
            .or_else(|| obj.get("accountName"))
            .or_else(|| obj.get("account_name"))
            .or_else(|| obj.get("account"))
            .or_else(|| obj.get("profile"))
            .and_then(|v| v.as_str())
    }
    let account = latest_run
        .and_then(direct_account)
        .or_else(|| direct_account(thread))
        .or_else(|| direct_account(proj))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);

    (model, account)
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

    let Some(status) = shell
        .get("status")
        .and_then(|v| v.as_str())
        .or_else(|| shell.get("activityRunStatus").and_then(|v| v.as_str()))
    else {
        return extract_explicit_stop_refusal(shell).map(ShellObservation::Refused);
    };

    match status {
        "preparing" | "queued" | "starting" | "running" | "waiting" => {
            Some(ShellObservation::Working)
        }
        "failed" => {
            if let Some(reason) = extract_refusal_reason(shell) {
                return Some(ShellObservation::Refused(reason));
            }
            let err = shell
                .get("lastError")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .unwrap_or("T3 thread failed")
                .to_owned();
            Some(ShellObservation::Errored(err))
        }
        "idle" | "completed" => {
            if let Some(reason) = extract_explicit_stop_refusal(shell) {
                return Some(ShellObservation::Refused(reason));
            }
            Some(ShellObservation::Idle {
                mirror_candidate: true,
            })
        }
        "interrupted" | "cancelled" | "rolled_back" => Some(ShellObservation::Idle {
            mirror_candidate: false,
        }),
        other if rex_wave_api::wave::is_refusal_stop_reason(other) => {
            Some(ShellObservation::Refused(
                extract_refusal_reason(shell).unwrap_or_else(|| other.to_owned()),
            ))
        }
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
                            } else if let Some(val_str) = o
                                .get("value")
                                .or_else(|| o.get("label"))
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty())
                            {
                                options.push(val_str.to_owned());
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
    let status = item
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("inProgress");
    let is_streaming = item
        .get("streaming")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let is_terminal = !is_streaming
        && (matches!(
            status,
            "completed" | "failed" | "error" | "declined" | "cancelled" | "interrupted"
        ) || item.get("completedAt").is_some_and(|v| !v.is_null()));
    let seq = item_event_seq(ordinal, is_terminal);
    let at = item
        .get("completedAt")
        .filter(|v| !v.is_null())
        .or_else(|| item.get("updatedAt").filter(|v| !v.is_null()))
        .or_else(|| item.get("startedAt").filter(|v| !v.is_null()))
        .map(parse_timestamp_ms)
        .unwrap_or(UnixMs(0));

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalTurnState {
    Completed,
    InProgress,
    Suppressed,
}

use std::collections::BTreeSet;

pub const FAILURE_TICKET_PREFIX: &str = "fail_ticket:";
pub const FAILURE_RESOLVED_PREFIX: &str = "fail_resolved:";

fn parse_failure_ticket(key: &str) -> Option<(usize, Option<&str>)> {
    let rest = key.strip_prefix(FAILURE_TICKET_PREFIX)?;
    let (id_str, kind) = rest.split_once(':')?;
    let id = id_str.parse::<usize>().ok()?;
    if kind == "any" {
        Some((id, None))
    } else if let Some(run_id) = kind.strip_prefix("run:") {
        (!run_id.is_empty()).then_some((id, Some(run_id)))
    } else {
        None
    }
}

pub fn next_failure_ticket_key(
    settled_keys: &BTreeSet<String>,
    run_id: Option<&str>,
) -> Option<String> {
    let clean_run = run_id.filter(|s| !s.is_empty());
    if let Some(want_run) = clean_run
        && settled_keys
            .iter()
            .any(|k| parse_failure_ticket(k).is_some_and(|(_, r)| r == Some(want_run)))
    {
        return None;
    }
    let max_id = settled_keys
        .iter()
        .filter_map(|k| parse_failure_ticket(k).map(|(id, _)| id))
        .max()
        .unwrap_or(0);
    let next_id = max_id + 1;
    Some(match clean_run {
        Some(rid) => format!("{FAILURE_TICKET_PREFIX}{next_id}:run:{rid}"),
        None => format!("{FAILURE_TICKET_PREFIX}{next_id}:any"),
    })
}

pub fn projection_contains_run_id(projection: &Value, want_run: &str) -> bool {
    if want_run.is_empty() {
        return false;
    }
    let (_, _, raw_turns) = slice_projection_turns(projection);
    raw_turns.iter().any(|t| {
        t.target_run_id == Some(want_run)
            || t.slice
                .iter()
                .any(|(_, _, it)| it.get("runId").and_then(|v| v.as_str()) == Some(want_run))
    })
}

pub fn active_unmatched_ticket_ids(
    projection: Option<&Value>,
    settled_keys: &BTreeSet<String>,
) -> Vec<usize> {
    let mut ids: Vec<usize> = settled_keys
        .iter()
        .filter_map(|k| {
            let (id, run_id) = parse_failure_ticket(k)?;
            let resolved_key = format!("{FAILURE_RESOLVED_PREFIX}{id}");
            if settled_keys.contains(&resolved_key) {
                return None;
            }
            if let (Some(proj), Some(rid)) = (projection, run_id)
                && projection_contains_run_id(proj, rid)
            {
                return None;
            }
            Some(id)
        })
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

pub fn resolve_ticket_keys(ticket_ids: &[usize], count: usize) -> Vec<String> {
    ticket_ids
        .iter()
        .take(count)
        .map(|id| format!("{FAILURE_RESOLVED_PREFIX}{id}"))
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedTurn {
    pub anchor: String,
    pub alt_anchors: Vec<String>,
    pub legacy_body_anchors: Vec<String>,
    pub run_key: Option<String>,
    pub run_start_at: u64,
    pub run_end_at: u64,
    pub tight_end_bound: bool,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnMirrorCheck {
    Ignore,
    Suppressed {
        turn_keys: Vec<String>,
        run_start_at: u64,
        run_end_at: u64,
        tight_end_bound: bool,
    },
    PostedViaTool {
        run_key: Option<String>,
        run_end_at: u64,
        tight_end_bound: bool,
    },
    Unposted(CompletedTurn),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectedTurn {
    pub check: TurnMirrorCheck,
    pub lookup_keys: Vec<String>,
    pub next_turn_start_at: Option<u64>,
    pub has_later_turn: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExplicitFailurePlan {
    TargetAheadOfProjection {
        explicit_keys: Vec<String>,
    },
    Matched {
        target_pos: usize,
        turns: Vec<InspectedTurn>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreterminalCandidate {
    pub check: TurnMirrorCheck,
    pub suppression_keys: Vec<String>,
    pub lookup_keys: Vec<String>,
    pub inherently_suppressed: bool,
    pub completed_cleanly: bool,
    pub run_start_at: u64,
    pub run_end_at: u64,
    pub tight_end_bound: bool,
    pub next_turn_start_at: Option<u64>,
}

type IndexedTurnItem<'a> = (usize, usize, &'a Value);

struct RawTurnSlice<'a> {
    pos: usize,
    has_later_turn: bool,
    target_run_id: Option<&'a str>,
    run_val: Option<&'a Value>,
    settle_run_key: Option<String>,
    suppressed_run_key: Option<String>,
    fallback_tool_key: String,
    run_start_at: u64,
    run_end_at: u64,
    tight_end_bound: bool,
    next_turn_start_at: Option<u64>,
    slice: Vec<IndexedTurnItem<'a>>,
}

fn push_unique_key(keys: &mut Vec<String>, candidate: String) {
    if !candidate.is_empty() && !keys.contains(&candidate) {
        keys.push(candidate);
    }
}

fn assistant_item_anchor(orig_idx: usize, item: &Value, target_run_id: Option<&str>) -> String {
    let item_key = item
        .get("messageId")
        .or_else(|| item.get("id"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            format!(
                "o{}",
                item.get("ordinal")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(orig_idx as u64)
            )
        });
    match target_run_id {
        Some(run_id) => format!("r{run_id}:{item_key}"),
        None => item_key,
    }
}

fn collect_suppressed_turn_keys(
    slice: &[IndexedTurnItem<'_>],
    target_run_id: Option<&str>,
    run_key: Option<String>,
) -> Vec<String> {
    let mut keys = Vec::new();
    for (orig_idx, _, item) in slice {
        let item_run_id = item
            .get("runId")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        if item.get("type").and_then(|v| v.as_str()) == Some("assistant_message") {
            push_unique_key(
                &mut keys,
                assistant_item_anchor(*orig_idx, item, target_run_id),
            );
            push_unique_key(&mut keys, assistant_item_anchor(*orig_idx, item, None));
            if let Some(irid) = item_run_id {
                push_unique_key(
                    &mut keys,
                    assistant_item_anchor(*orig_idx, item, Some(irid)),
                );
            }
        }
        if let Some(irid) = item_run_id {
            push_unique_key(&mut keys, format!("run:{irid}"));
        }
    }
    if let Some(rk) = run_key {
        push_unique_key(&mut keys, rk);
    }
    keys
}

fn collect_turn_lookup_keys(
    slice: &[IndexedTurnItem<'_>],
    target_run_id: Option<&str>,
    primary_key: Option<&str>,
    extra_keys: &[String],
) -> Vec<String> {
    let mut keys = Vec::new();
    for (orig_idx, _, item) in slice {
        let item_run_id = item
            .get("runId")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        if item.get("type").and_then(|v| v.as_str()) == Some("assistant_message") {
            push_unique_key(
                &mut keys,
                assistant_item_anchor(*orig_idx, item, target_run_id),
            );
            push_unique_key(&mut keys, assistant_item_anchor(*orig_idx, item, None));
            if let Some(irid) = item_run_id {
                push_unique_key(
                    &mut keys,
                    assistant_item_anchor(*orig_idx, item, Some(irid)),
                );
            }
        }
        if let Some(irid) = item_run_id {
            push_unique_key(&mut keys, format!("run:{irid}"));
        }
    }
    if let Some(pk) = primary_key {
        push_unique_key(&mut keys, pk.to_owned());
    }
    for (_, item_count, _) in slice {
        push_unique_key(&mut keys, format!("item_count:{item_count}"));
    }
    for extra in extra_keys {
        push_unique_key(&mut keys, extra.clone());
    }
    keys
}

fn error_item_is_retrying(item: &Value) -> bool {
    item.get("willRetry").and_then(|v| v.as_bool()) == Some(true)
        || item.get("retrying").and_then(|v| v.as_bool()) == Some(true)
        || item
            .get("failure")
            .and_then(|f| f.get("willRetry").or_else(|| f.get("retrying")))
            .and_then(|v| v.as_bool())
            == Some(true)
}

fn slice_ended_in_item_failure(slice: &[IndexedTurnItem<'_>]) -> bool {
    let last_asst_pos = slice.iter().rposition(|(_, _, it)| {
        it.get("type").and_then(|v| v.as_str()) == Some("assistant_message")
    });
    if let Some(pos) = last_asst_pos {
        let (_, _, asst) = slice[pos];
        if extract_explicit_stop_refusal(asst).is_some() {
            return true;
        }
        let status = asst
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("completed");
        if matches!(
            status,
            "failed" | "error" | "cancelled" | "declined" | "interrupted" | "rolled_back"
        ) {
            return true;
        }
    }
    let tail_start = last_asst_pos.map_or(0, |p| p + 1);
    slice[tail_start..].iter().any(|(_, _, it)| {
        it.get("type").and_then(|v| v.as_str()) == Some("error") && !error_item_is_retrying(it)
    })
}

fn inspect_turn_slice(
    slice: &[IndexedTurnItem<'_>],
    target_run_id: Option<&str>,
    run_key: Option<String>,
    fallback_tool_key: String,
    run_start_at: u64,
    run_end_at: u64,
    tight_end_bound: bool,
) -> TurnMirrorCheck {
    for (_, _, item) in slice {
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
                return TurnMirrorCheck::PostedViaTool {
                    run_key: run_key.or(Some(fallback_tool_key)),
                    run_end_at,
                    tight_end_bound,
                };
            }
        }
    }

    let mut last_assistant: Option<(String, Vec<String>, Vec<String>, String)> = None;
    for (orig_idx, _, item) in slice {
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
        let anchor = assistant_item_anchor(*orig_idx, item, None);
        let item_run_id = item
            .get("runId")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        let mut alt_anchors = Vec::new();
        if target_run_id.is_some() {
            push_unique_key(
                &mut alt_anchors,
                assistant_item_anchor(*orig_idx, item, target_run_id),
            );
        }
        if let Some(irid) = item_run_id {
            push_unique_key(
                &mut alt_anchors,
                assistant_item_anchor(*orig_idx, item, Some(irid)),
            );
        }
        alt_anchors.retain(|a| a != &anchor);

        let has_stable_id = item
            .get("messageId")
            .or_else(|| item.get("id"))
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty())
            || item.get("ordinal").and_then(|v| v.as_u64()).is_some();
        let mut legacy_body_anchors = Vec::new();
        if !has_stable_id {
            push_unique_key(&mut legacy_body_anchors, "o0".to_owned());
            if let Some(trid) = target_run_id {
                push_unique_key(&mut legacy_body_anchors, format!("r{trid}:o0"));
            }
            if let Some(irid) = item_run_id {
                push_unique_key(&mut legacy_body_anchors, format!("r{irid}:o0"));
            }
            legacy_body_anchors.retain(|a| a != &anchor && !alt_anchors.contains(a));
        }
        last_assistant = Some((anchor, alt_anchors, legacy_body_anchors, text.to_owned()));
    }

    let Some((anchor, alt_anchors, legacy_body_anchors, text)) = last_assistant else {
        return TurnMirrorCheck::Ignore;
    };

    TurnMirrorCheck::Unposted(CompletedTurn {
        anchor,
        alt_anchors,
        legacy_body_anchors,
        run_key,
        run_start_at,
        run_end_at,
        tight_end_bound,
        text,
    })
}

fn slice_projection_turns(projection: &Value) -> (usize, bool, Vec<RawTurnSlice<'_>>) {
    let proj = unwrap_projection(projection);
    let items_opt = proj.get("turnItems").and_then(|v| v.as_array());

    let mut raw_ordered: Vec<(usize, &Value)> = items_opt
        .map(|items| items.iter().enumerate().collect())
        .unwrap_or_default();
    raw_ordered.sort_by_key(|(idx, it)| {
        (
            it.get("ordinal")
                .and_then(|v| v.as_u64())
                .unwrap_or(*idx as u64),
            *idx,
        )
    });
    let ordered: Vec<IndexedTurnItem<'_>> = raw_ordered
        .into_iter()
        .enumerate()
        .map(|(sorted_pos, (orig_idx, it))| (orig_idx, sorted_pos + 1, it))
        .collect();
    let ordered_len = ordered.len();

    if let Some(runs_arr) = proj
        .get("runs")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
    {
        let mut ordered_runs: Vec<(usize, &Value)> = runs_arr.iter().enumerate().collect();
        ordered_runs.sort_by_key(|(idx, r)| {
            (
                r.get("ordinal")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(*idx as u64),
                *idx,
            )
        });
        let single_run = ordered_runs.len() == 1;
        let mut out = Vec::with_capacity(ordered_runs.len());
        for (pos, (idx, run)) in ordered_runs.iter().enumerate() {
            let has_later_turn = pos + 1 < ordered_runs.len();
            let target_run_id = run
                .get("id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty());
            let ord = run
                .get("ordinal")
                .and_then(|v| v.as_u64())
                .unwrap_or(*idx as u64);
            let run_key = Some(match target_run_id {
                Some(id) => format!("run:{id}"),
                None => format!("run_ord:{ord}"),
            });
            let run_start_at = run
                .get("startedAt")
                .or_else(|| run.get("createdAt"))
                .map(parse_timestamp_ms)
                .map_or(0, |u| u.0);
            let run_end_at = run
                .get("completedAt")
                .or_else(|| run.get("endedAt"))
                .map(parse_timestamp_ms)
                .map_or(0, |u| u.0);
            let immediate_next_start_at = ordered_runs.get(pos + 1).and_then(|(_, nr)| {
                nr.get("startedAt")
                    .or_else(|| nr.get("createdAt"))
                    .map(parse_timestamp_ms)
                    .map(|u| u.0)
                    .filter(|&ts| ts > 0)
            });
            let next_run_start_at = ordered_runs[pos + 1..]
                .iter()
                .filter_map(|(_, nr)| {
                    nr.get("startedAt")
                        .or_else(|| nr.get("createdAt"))
                        .or_else(|| nr.get("completedAt"))
                        .or_else(|| nr.get("endedAt"))
                        .map(parse_timestamp_ms)
                        .map(|u| u.0)
                        .filter(|&ts| ts > 0)
                })
                .min();
            let tight_end_bound = run_end_at > 0 || immediate_next_start_at.is_some();
            let slice: Vec<IndexedTurnItem<'_>> = if let Some(run_id) = target_run_id {
                let filtered: Vec<IndexedTurnItem<'_>> = ordered
                    .iter()
                    .copied()
                    .filter(|(_, _, it)| it.get("runId").and_then(|v| v.as_str()) == Some(run_id))
                    .collect();
                if filtered.is_empty() && single_run {
                    ordered.clone()
                } else {
                    filtered
                }
            } else if single_run {
                ordered.clone()
            } else {
                Vec::new()
            };
            out.push(RawTurnSlice {
                pos,
                has_later_turn,
                target_run_id,
                run_val: Some(run),
                settle_run_key: run_key.clone(),
                suppressed_run_key: run_key,
                fallback_tool_key: format!("run_pos:{pos}"),
                run_start_at,
                run_end_at,
                tight_end_bound,
                next_turn_start_at: next_run_start_at,
                slice,
            });
        }
        return (ordered_len, true, out);
    }

    if ordered.is_empty() {
        return (0, false, Vec::new());
    }

    let mut starts: Vec<usize> = ordered
        .iter()
        .enumerate()
        .filter_map(|(i, (_, _, it))| {
            (it.get("type").and_then(|v| v.as_str()) == Some("user_message")).then_some(i)
        })
        .collect();
    if starts.first().copied() != Some(0) {
        starts.insert(0, 0);
    }
    let mut out = Vec::with_capacity(starts.len());
    for (seg_idx, &start) in starts.iter().enumerate() {
        let end = starts.get(seg_idx + 1).copied().unwrap_or(ordered.len());
        let seg = &ordered[start..end];
        if seg.is_empty() {
            continue;
        }
        let has_later_turn = seg_idx + 1 < starts.len();
        let seg_start_at = seg
            .first()
            .filter(|(_, _, it)| it.get("type").and_then(|v| v.as_str()) == Some("user_message"))
            .and_then(|(_, _, it)| {
                it.get("startedAt")
                    .or_else(|| it.get("createdAt"))
                    .map(parse_timestamp_ms)
                    .map(|u| u.0)
                    .filter(|&ts| ts > 0)
            })
            .unwrap_or(0);
        let seg_end_at = seg
            .last()
            .filter(|(_, _, it)| it.get("type").and_then(|v| v.as_str()) != Some("user_message"))
            .and_then(|(_, _, it)| {
                it.get("completedAt")
                    .or_else(|| it.get("endedAt"))
                    .or_else(|| it.get("createdAt"))
                    .map(parse_timestamp_ms)
                    .map(|u| u.0)
                    .filter(|&ts| ts > 0)
            })
            .unwrap_or(0);
        let immediate_next_start_at = starts
            .get(seg_idx + 1)
            .and_then(|&next_idx| ordered.get(next_idx))
            .filter(|(_, _, it)| it.get("type").and_then(|v| v.as_str()) == Some("user_message"))
            .and_then(|(_, _, it)| {
                it.get("startedAt")
                    .or_else(|| it.get("createdAt"))
                    .map(parse_timestamp_ms)
                    .map(|u| u.0)
                    .filter(|&ts| ts > 0)
            });
        let next_seg_start_at = ordered[end..]
            .iter()
            .filter_map(|(_, _, it)| {
                it.get("startedAt")
                    .or_else(|| it.get("createdAt"))
                    .or_else(|| it.get("completedAt"))
                    .or_else(|| it.get("endedAt"))
                    .map(parse_timestamp_ms)
                    .map(|u| u.0)
                    .filter(|&ts| ts > 0)
            })
            .min();
        let tight_end_bound = seg_end_at > 0 || immediate_next_start_at.is_some();
        let has_non_user_item = seg
            .iter()
            .any(|(_, _, it)| it.get("type").and_then(|v| v.as_str()) != Some("user_message"));
        let seg_label = format!("seg:{seg_idx}");
        out.push(RawTurnSlice {
            pos: seg_idx,
            has_later_turn,
            target_run_id: None,
            run_val: None,
            settle_run_key: has_non_user_item.then(|| seg_label.clone()),
            suppressed_run_key: Some(seg_label.clone()),
            fallback_tool_key: seg_label,
            run_start_at: seg_start_at,
            run_end_at: seg_end_at,
            tight_end_bound,
            next_turn_start_at: next_seg_start_at,
            slice: seg.to_vec(),
        });
    }
    (ordered_len, false, out)
}

fn raw_turn_inherently_suppressed(raw: &RawTurnSlice<'_>) -> bool {
    if let Some(run) = raw.run_val {
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if matches!(
            status,
            "failed" | "interrupted" | "cancelled" | "rolled_back"
        ) || rex_wave_api::wave::is_refusal_stop_reason(status)
            || extract_refusal_reason(run).is_some()
            || extract_explicit_stop_refusal(run).is_some()
        {
            return true;
        }
    }
    !raw.slice.is_empty() && slice_ended_in_item_failure(&raw.slice)
}

fn evaluate_raw_turn(
    raw: &RawTurnSlice<'_>,
    is_terminal: bool,
    terminal_state: TerminalTurnState,
    extra_suppressed_keys: &[String],
    force_suppressed: bool,
) -> Option<InspectedTurn> {
    let suppressed = force_suppressed
        || (is_terminal && terminal_state == TerminalTurnState::Suppressed)
        || raw_turn_inherently_suppressed(raw);

    let check = if suppressed {
        let mut turn_keys = collect_suppressed_turn_keys(
            &raw.slice,
            raw.target_run_id,
            raw.suppressed_run_key.clone(),
        );
        for extra in extra_suppressed_keys {
            push_unique_key(&mut turn_keys, extra.clone());
        }
        TurnMirrorCheck::Suppressed {
            turn_keys,
            run_start_at: raw.run_start_at,
            run_end_at: raw.run_end_at,
            tight_end_bound: raw.tight_end_bound,
        }
    } else if let Some(run) = raw.run_val {
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if matches!(
            status,
            "preparing" | "queued" | "starting" | "running" | "waiting"
        ) || (is_terminal
            && terminal_state == TerminalTurnState::InProgress
            && !matches!(status, "completed" | "idle"))
        {
            TurnMirrorCheck::Ignore
        } else if matches!(status, "completed" | "idle") && !raw.slice.is_empty() {
            inspect_turn_slice(
                &raw.slice,
                raw.target_run_id,
                raw.settle_run_key.clone(),
                raw.fallback_tool_key.clone(),
                raw.run_start_at,
                raw.run_end_at,
                raw.tight_end_bound,
            )
        } else {
            TurnMirrorCheck::Ignore
        }
    } else if is_terminal && terminal_state == TerminalTurnState::InProgress {
        TurnMirrorCheck::Ignore
    } else {
        inspect_turn_slice(
            &raw.slice,
            None,
            raw.settle_run_key.clone(),
            raw.fallback_tool_key.clone(),
            raw.run_start_at,
            raw.run_end_at,
            raw.tight_end_bound,
        )
    };

    if matches!(check, TurnMirrorCheck::Ignore) {
        return None;
    }

    let check_keys: Vec<String> = match &check {
        TurnMirrorCheck::Ignore => Vec::new(),
        TurnMirrorCheck::Suppressed { turn_keys, .. } => turn_keys.clone(),
        TurnMirrorCheck::PostedViaTool { run_key, .. } => run_key.iter().cloned().collect(),
        TurnMirrorCheck::Unposted(c) => {
            let mut ks = vec![c.anchor.clone()];
            ks.extend(c.alt_anchors.iter().cloned());
            if let Some(rk) = &c.run_key {
                ks.push(rk.clone());
            }
            ks
        }
    };
    let lookup_keys = collect_turn_lookup_keys(
        &raw.slice,
        raw.target_run_id,
        raw.suppressed_run_key.as_deref(),
        &check_keys,
    );

    Some(InspectedTurn {
        check,
        lookup_keys,
        next_turn_start_at: raw.next_turn_start_at,
        has_later_turn: raw.has_later_turn,
    })
}

pub fn inspect_turns_in_order(
    projection: &Value,
    terminal_state: TerminalTurnState,
) -> Vec<InspectedTurn> {
    let (_, _, raw_turns) = slice_projection_turns(projection);
    let mut out = Vec::new();
    for raw in &raw_turns {
        let is_terminal = !raw.has_later_turn;
        if let Some(inspected) = evaluate_raw_turn(raw, is_terminal, terminal_state, &[], false) {
            out.push(inspected);
        }
    }
    out
}

pub fn projection_turn_slot_count(projection: &Value) -> usize {
    let (_, _, raw_turns) = slice_projection_turns(projection);
    raw_turns.len()
}

pub fn shell_explicit_turn_target(shell: &Value) -> (Option<String>, Option<usize>) {
    let run_id = shell
        .get("latestRunId")
        .or_else(|| shell.get("runId"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    let item_count = shell
        .get("itemCount")
        .and_then(|v| v.as_u64())
        .map(|n| n as usize)
        .filter(|&n| n > 0);
    (run_id, item_count)
}

pub fn explicit_shell_suppression_keys(
    explicit_run_id: Option<&str>,
    explicit_item_count: Option<usize>,
) -> Vec<String> {
    let mut keys = Vec::new();
    if let Some(run_id) = explicit_run_id
        && !run_id.is_empty()
    {
        push_unique_key(&mut keys, format!("run:{run_id}"));
    }
    if let Some(ic) = explicit_item_count
        && ic > 0
    {
        push_unique_key(&mut keys, format!("item_count:{ic}"));
    }
    keys
}

pub fn plan_explicit_failure_turns(
    projection: &Value,
    proj_terminal_state: TerminalTurnState,
    explicit_run_id: Option<&str>,
    explicit_item_count: Option<usize>,
) -> Option<ExplicitFailurePlan> {
    let explicit_keys = explicit_shell_suppression_keys(explicit_run_id, explicit_item_count);
    if explicit_keys.is_empty() {
        return None;
    }
    let (ordered_item_len, is_runs_mode, raw_turns) = slice_projection_turns(projection);
    if raw_turns.is_empty() {
        return Some(ExplicitFailurePlan::TargetAheadOfProjection { explicit_keys });
    }

    let target_pos = if is_runs_mode {
        if let Some(want_run) = explicit_run_id {
            let Some(pos) = raw_turns
                .iter()
                .position(|t| t.target_run_id == Some(want_run))
            else {
                return Some(ExplicitFailurePlan::TargetAheadOfProjection { explicit_keys });
            };
            pos
        } else if let Some(ic) = explicit_item_count {
            if ic > ordered_item_len {
                return Some(ExplicitFailurePlan::TargetAheadOfProjection { explicit_keys });
            }
            if let Some(pos) = raw_turns
                .iter()
                .position(|t| t.slice.iter().any(|(_, item_ic, _)| *item_ic == ic))
            {
                pos
            } else if raw_turns.len() == 1 {
                0
            } else {
                return Some(ExplicitFailurePlan::TargetAheadOfProjection { explicit_keys });
            }
        } else {
            return None;
        }
    } else {
        let by_run_id = explicit_run_id.and_then(|want_run| {
            raw_turns.iter().position(|t| {
                t.slice
                    .iter()
                    .any(|(_, _, it)| it.get("runId").and_then(|v| v.as_str()) == Some(want_run))
            })
        });
        if let Some(pos) = by_run_id {
            pos
        } else if let Some(ic) = explicit_item_count {
            if ic > ordered_item_len {
                return Some(ExplicitFailurePlan::TargetAheadOfProjection { explicit_keys });
            }
            let Some(pos) = raw_turns
                .iter()
                .position(|t| t.slice.iter().any(|(_, item_ic, _)| *item_ic == ic))
            else {
                return Some(ExplicitFailurePlan::TargetAheadOfProjection { explicit_keys });
            };
            pos
        } else {
            return Some(ExplicitFailurePlan::TargetAheadOfProjection { explicit_keys });
        }
    };

    let mut turns = Vec::new();
    for raw in &raw_turns {
        let is_terminal = !raw.has_later_turn;
        let inspected = if raw.pos == target_pos {
            evaluate_raw_turn(
                raw,
                is_terminal,
                TerminalTurnState::Suppressed,
                &explicit_keys,
                true,
            )
        } else if raw.pos < target_pos {
            evaluate_raw_turn(raw, false, TerminalTurnState::Completed, &[], false)
        } else if raw_turn_inherently_suppressed(raw)
            || (is_terminal && proj_terminal_state == TerminalTurnState::Suppressed)
        {
            evaluate_raw_turn(raw, is_terminal, TerminalTurnState::Suppressed, &[], true)
        } else {
            None
        };
        if let Some(inspected) = inspected {
            turns.push(inspected);
        }
    }
    Some(ExplicitFailurePlan::Matched { target_pos, turns })
}

pub fn closed_turn_count_before_ahead_failure(
    projection: &Value,
    explicit_run_id: Option<&str>,
    explicit_item_count: Option<usize>,
) -> usize {
    let (_, is_runs_mode, raw_turns) = slice_projection_turns(projection);
    if raw_turns.is_empty() {
        return 0;
    }
    if is_runs_mode && explicit_item_count.is_none() {
        if let Some(want_run) = explicit_run_id {
            let last_is_distinct_tagged_run = raw_turns.last().is_some_and(|last| {
                last.target_run_id.is_some_and(|rid| rid != want_run)
                    && !last.slice.is_empty()
                    && last.slice.iter().all(|(_, _, it)| {
                        it.get("runId").and_then(|v| v.as_str()) == last.target_run_id
                    })
            });
            if last_is_distinct_tagged_run {
                raw_turns.len()
            } else {
                raw_turns.len().saturating_sub(1)
            }
        } else {
            raw_turns.len().saturating_sub(1)
        }
    } else {
        raw_turns.len().saturating_sub(1)
    }
}

pub fn inspect_closed_turns_before_ahead_failure(
    projection: &Value,
    explicit_run_id: Option<&str>,
    explicit_item_count: Option<usize>,
) -> Vec<InspectedTurn> {
    let closed_end =
        closed_turn_count_before_ahead_failure(projection, explicit_run_id, explicit_item_count);
    if closed_end == 0 {
        return Vec::new();
    }
    let (_, _, raw_turns) = slice_projection_turns(projection);
    let mut out = Vec::new();
    for raw in &raw_turns[..closed_end] {
        if let Some(mut inspected) =
            evaluate_raw_turn(raw, false, TerminalTurnState::Completed, &[], false)
        {
            inspected.has_later_turn = true;
            out.push(inspected);
        }
    }
    out
}

pub fn any_turn_inherently_suppressed(projection: &Value) -> bool {
    let (_, _, raw_turns) = slice_projection_turns(projection);
    raw_turns.iter().any(raw_turn_inherently_suppressed)
}

pub fn projection_has_ahead_suppression(
    projection: &Value,
    settled_keys: &BTreeSet<String>,
    shell_run_id: Option<&str>,
    shell_item_count: Option<usize>,
) -> (bool, Option<String>, Option<usize>) {
    let (ordered_item_len, is_runs_mode, raw_turns) = slice_projection_turns(projection);
    let max_settled_ic = settled_keys
        .iter()
        .filter_map(|k| k.strip_prefix("item_count:"))
        .filter_map(|n| n.parse::<usize>().ok())
        .max();
    let ahead_ic = max_settled_ic
        .into_iter()
        .chain(shell_item_count)
        .max()
        .filter(|&ic| ic > ordered_item_len);

    let has_run_in_turns = |want: &str| {
        raw_turns.iter().any(|t| {
            t.target_run_id == Some(want)
                || t.slice
                    .iter()
                    .any(|(_, _, it)| it.get("runId").and_then(|v| v.as_str()) == Some(want))
        })
    };
    let ahead_run = if is_runs_mode {
        if let Some(sr) = shell_run_id.filter(|s| !s.is_empty()) {
            (!has_run_in_turns(sr)).then(|| sr.to_owned())
        } else {
            settled_keys
                .iter()
                .filter_map(|k| k.strip_prefix("run:"))
                .find(|want| !want.is_empty() && !has_run_in_turns(want))
                .map(str::to_owned)
        }
    } else {
        None
    };
    let hold = ahead_ic.is_some() || ahead_run.is_some();
    (hold, ahead_run, ahead_ic)
}

pub fn terminal_suppressed_lookup_keys(
    projection: &Value,
    terminal_state: TerminalTurnState,
) -> Option<Vec<String>> {
    let (_, _, raw_turns) = slice_projection_turns(projection);
    let last = raw_turns.last()?;
    if terminal_state != TerminalTurnState::Suppressed && !raw_turn_inherently_suppressed(last) {
        return None;
    }
    let supp = collect_suppressed_turn_keys(
        &last.slice,
        last.target_run_id,
        last.suppressed_run_key.clone(),
    );
    Some(collect_turn_lookup_keys(
        &last.slice,
        last.target_run_id,
        last.suppressed_run_key.as_deref(),
        &supp,
    ))
}

pub fn collect_turn_candidates_in_range(
    projection: &Value,
    start_pos: usize,
    end_pos: usize,
    explicit_run_id: Option<&str>,
    explicit_item_count: Option<usize>,
) -> Vec<PreterminalCandidate> {
    let (_, is_runs_mode, raw_turns) = slice_projection_turns(projection);
    let bound = end_pos.min(raw_turns.len());
    if start_pos >= bound {
        return Vec::new();
    }
    let matched_pos = if is_runs_mode {
        explicit_run_id.and_then(|want| {
            raw_turns[start_pos..bound]
                .iter()
                .find(|t| t.target_run_id == Some(want))
                .map(|t| t.pos)
        })
    } else {
        explicit_item_count.and_then(|ic| {
            raw_turns[start_pos..bound]
                .iter()
                .find(|t| t.slice.iter().any(|(_, item_ic, _)| *item_ic == ic))
                .map(|t| t.pos)
        })
    };

    let mut out = Vec::new();
    for raw in &raw_turns[start_pos..bound] {
        if matched_pos.is_some_and(|mp| mp != raw.pos) {
            continue;
        }
        let inherently_suppressed = raw_turn_inherently_suppressed(raw);
        let completed_cleanly = raw.run_val.is_some_and(|run| {
            let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
            matches!(status, "completed" | "idle") && !inherently_suppressed
        });
        let suppression_keys = collect_suppressed_turn_keys(
            &raw.slice,
            raw.target_run_id,
            raw.suppressed_run_key.clone(),
        );
        let check = if raw.slice.is_empty() {
            TurnMirrorCheck::Ignore
        } else {
            inspect_turn_slice(
                &raw.slice,
                raw.target_run_id,
                raw.settle_run_key.clone(),
                raw.fallback_tool_key.clone(),
                raw.run_start_at,
                raw.run_end_at,
                raw.tight_end_bound,
            )
        };
        let lookup_keys = collect_turn_lookup_keys(
            &raw.slice,
            raw.target_run_id,
            raw.suppressed_run_key.as_deref(),
            &suppression_keys,
        );
        out.push(PreterminalCandidate {
            check,
            suppression_keys,
            lookup_keys,
            inherently_suppressed,
            completed_cleanly,
            run_start_at: raw.run_start_at,
            run_end_at: raw.run_end_at,
            tight_end_bound: raw.tight_end_bound,
            next_turn_start_at: raw.next_turn_start_at,
        });
    }
    out
}

pub fn collect_turn_candidates_before(
    projection: &Value,
    end_pos: usize,
    explicit_run_id: Option<&str>,
    explicit_item_count: Option<usize>,
) -> Vec<PreterminalCandidate> {
    collect_turn_candidates_in_range(projection, 0, end_pos, explicit_run_id, explicit_item_count)
}

pub fn collect_preterminal_turn_candidates(
    projection: &Value,
    explicit_run_id: Option<&str>,
    explicit_item_count: Option<usize>,
) -> Vec<PreterminalCandidate> {
    let slot_count = projection_turn_slot_count(projection);
    if slot_count < 2 {
        return Vec::new();
    }
    collect_turn_candidates_before(
        projection,
        slot_count - 1,
        explicit_run_id,
        explicit_item_count,
    )
}

pub fn inspect_completed_turns_in_order(projection: &Value) -> Vec<(TurnMirrorCheck, Option<u64>)> {
    inspect_turns_in_order(projection, TerminalTurnState::Completed)
        .into_iter()
        .filter(|it| !matches!(it.check, TurnMirrorCheck::Suppressed { .. }))
        .map(|it| (it.check, it.next_turn_start_at))
        .collect()
}

pub fn inspect_latest_completed_turn(projection: &Value) -> TurnMirrorCheck {
    inspect_completed_turns_in_order(projection)
        .pop()
        .map_or(TurnMirrorCheck::Ignore, |(check, _)| check)
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

/// Parses an RFC 3339 / ISO-8601 timestamp into Unix ms (see [`UnixMs::parse_rfc3339`]).
pub fn parse_rfc3339_ms(raw: &str) -> Option<u64> {
    UnixMs::parse_rfc3339(raw).map(|ms| ms.0)
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
                account: None,
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
            run_stamp: "w1:reviewer:attempt-1:seq-1".into(),
            output_dir: None,
            writes: Vec::new(),
            return_schema: None,
        };
        let tid = deterministic_thread_id(&wave, &role, 1).unwrap();
        let payload =
            launch_thread_payload(&req, &tid, "uuid-proj-1", "antigravity", "gemini-4-neon");
        assert_eq!(payload["threadId"], "t3:w1:reviewer:a1");
        assert_eq!(payload["commandId"], "rex-wave:spawn:w1:reviewer:a1");
        assert!(payload.get("reuseExistingThread").is_none());
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
        assert_eq!(
            tell_auto["dispatchMode"],
            json!({"type": "queue_after_active"})
        );
        assert!(tell_auto.get("deliveryIntent").is_none());

        let tell_start = message_dispatch_payload(
            &tid,
            &Body::parse("begin").unwrap(),
            SteerDeliveryMode::Start,
            "c1b".into(),
            "m1b".into(),
        );
        assert_eq!(
            tell_start["dispatchMode"],
            json!({"type": "start_immediately"})
        );
        assert!(tell_start.get("deliveryIntent").is_none());

        let tell_steer_interrupt = message_dispatch_payload(
            &tid,
            &Body::parse("interrupt").unwrap(),
            SteerDeliveryMode::Steer,
            "c2a".into(),
            "m2a".into(),
        );
        assert_eq!(
            tell_steer_interrupt["dispatchMode"],
            json!({"type": "start_immediately"})
        );
        assert_eq!(tell_steer_interrupt["deliveryIntent"], "restart");

        let tell_steer = message_dispatch_payload(
            &tid,
            &Body::parse("stop").unwrap(),
            SteerDeliveryMode::SteerIfActive,
            "c2".into(),
            "m2".into(),
        );
        assert_eq!(
            tell_steer["dispatchMode"],
            json!({"type": "start_immediately"})
        );
        assert_eq!(tell_steer["deliveryIntent"], "steer");

        let tell_queue = message_dispatch_payload(
            &tid,
            &Body::parse("later").unwrap(),
            SteerDeliveryMode::QueueIfActive,
            "c3".into(),
            "m3".into(),
        );
        assert_eq!(
            tell_queue["dispatchMode"],
            json!({"type": "queue_after_active"})
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

    #[test]
    fn live_seq_tracker_orders_updates_meta_events_and_skips_streaming() {
        let mut tracker = LiveSeqTracker::default();
        let started = tracker
            .accept_domain_event(1, &json!({"type": "run.created", "payload": {}}))
            .unwrap();
        let call = tracker
            .accept_domain_event(
                2,
                &json!({
                    "type": "turn-item.updated",
                    "payload": {
                        "id": "c1",
                        "ordinal": 0,
                        "type": "command_execution",
                        "status": "inProgress",
                        "input": "cargo test"
                    }
                }),
            )
            .unwrap();
        assert!(
            tracker
                .accept_domain_event(
                    3,
                    &json!({
                        "type": "turn-item.updated",
                        "payload": {
                            "id": "c1",
                            "ordinal": 0,
                            "type": "command_execution",
                            "status": "inProgress",
                            "input": "cargo test"
                        }
                    }),
                )
                .is_none()
        );
        let result = tracker
            .accept_domain_event(
                4,
                &json!({
                    "type": "turn-item.updated",
                    "payload": {
                        "id": "c1",
                        "ordinal": 0,
                        "type": "command_execution",
                        "status": "completed",
                        "input": "cargo test",
                        "exitCode": 0
                    }
                }),
            )
            .unwrap();
        assert!(
            tracker
                .accept_domain_event(
                    5,
                    &json!({
                        "type": "turn-item.updated",
                        "payload": {
                            "id": "a1",
                            "ordinal": 1,
                            "type": "assistant_message",
                            "status": "inProgress",
                            "streaming": true,
                            "text": "part..."
                        }
                    }),
                )
                .is_none()
        );
        let final_msg = tracker
            .accept_domain_event(
                6,
                &json!({
                    "type": "turn-item.updated",
                    "payload": {
                        "id": "a1",
                        "ordinal": 1,
                        "type": "assistant_message",
                        "status": "completed",
                        "streaming": false,
                        "text": "final answer"
                    }
                }),
            )
            .unwrap();
        let usage = tracker
            .accept_domain_event(
                7,
                &json!({
                    "type": "provider-turn.updated",
                    "payload": { "tokenUsage": { "usedTokens": 1200, "maxTokens": 8000 } }
                }),
            )
            .unwrap();
        let ended = tracker
            .accept_domain_event(
                8,
                &json!({ "type": "run.updated", "payload": { "status": "completed" } }),
            )
            .unwrap();
        let next_turn = tracker
            .accept_domain_event(
                9,
                &json!({
                    "type": "turn-item.updated",
                    "payload": {
                        "id": "u2",
                        "ordinal": 2,
                        "type": "user_message",
                        "status": "completed",
                        "text": "turn 2"
                    }
                }),
            )
            .unwrap();

        let seqs = [
            started.seq,
            call.seq,
            result.seq,
            final_msg.seq,
            usage.seq,
            ended.seq,
            next_turn.seq,
        ];
        for pair in seqs.windows(2) {
            assert!(
                pair[0] < pair[1],
                "seqs must be strictly monotonic: {seqs:?}"
            );
        }
    }

    #[test]
    fn question_options_prefer_value_and_free_text_replaces_single_selection() {
        let proj = json!({
            "turnItems": [{
                "type": "user_input_request",
                "requestId": "req_opt",
                "questions": [{
                    "id": "q_env",
                    "header": "Env",
                    "question": "Which env?",
                    "options": [
                        { "label": "Production (us-east)", "value": "prod-east", "description": "live" },
                        { "label": "Staging", "description": "stage" }
                    ],
                    "multiSelect": false
                }]
            }]
        });
        let parsed = parse_pending_interaction_from_projection(&proj, "req_opt", None).unwrap();
        match parsed.kind {
            InteractionKind::Question { questions } => {
                assert_eq!(questions[0].options, vec!["prod-east", "Staging"]);
            }
            other => panic!("expected Question, got {other:?}"),
        }

        let tid = BbThreadId::parse("t3:w1:r1:a1").unwrap();
        let iid = BbInteractionId::parse("req_opt").unwrap();
        let mut answers = BTreeMap::new();
        answers.insert(
            "q_env".into(),
            UserAnswer {
                selected: vec!["other".into()],
                free_text: Some("custom-eu".into()),
            },
        );
        let payload = runtime_request_respond_payload(
            &tid,
            &iid,
            &InteractionResolution::UserAnswer { answers },
            "cmd_1".into(),
        )
        .unwrap();
        assert_eq!(payload["answers"]["q_env"], "custom-eu");
    }

    #[test]
    fn mirror_idem_key_hashes_long_anchors_without_collision() {
        let wave = WaveSlug::parse("a".repeat(48)).unwrap();
        let role = RoleName::parse("b".repeat(32)).unwrap();
        let thread = deterministic_thread_id(&wave, &role, 1).unwrap();
        let anchor_a = format!("run_{}_msg_1", "x".repeat(120));
        let anchor_b = format!("run_{}_msg_2", "x".repeat(120));
        let k1 = mirror_idem_key(&wave, &role, &thread, &anchor_a).unwrap();
        let k2 = mirror_idem_key(&wave, &role, &thread, &anchor_b).unwrap();
        assert_ne!(k1, k2);
        assert!(k1.as_str().starts_with("wave:"));
        assert!(k1.as_str().contains(":turn:"));
        assert!(k1.as_str().len() <= 256);
    }

    #[test]
    fn t3_refusal_stop_reasons_runtime_meta_and_stale_last_error_handling_work() {
        // Explicit stopReason on completed shell is Refused; launch-time modelSelection is NOT echoed as actual_*.
        let shell_refused = json!({
            "status": "completed",
            "stopReason": "safety",
            "modelSelection": {
                "model": "gemini-4-neon",
                "options": [{ "id": "account", "value": "work-acct" }]
            }
        });
        assert_eq!(
            derive_shell_observation(&shell_refused, None),
            Some(ShellObservation::Refused("safety".into()))
        );
        let (model, account) = extract_runtime_meta_from_value(&shell_refused);
        assert_eq!(model, None);
        assert_eq!(account, None);

        // Runtime-reported model & account on latest run are extracted.
        let proj_runtime = json!({
            "thread": { "modelSelection": { "model": "requested-model" } },
            "runs": [{ "ordinal": 1, "model": "gemini-4-neon", "accountEmail": "work-acct@example.com" }]
        });
        let (run_model, run_acct) = extract_runtime_meta_from_value(&proj_runtime);
        assert_eq!(
            run_model.as_ref().map(ModelId::as_str),
            Some("gemini-4-neon")
        );
        assert_eq!(run_acct.as_deref(), Some("work-acct@example.com"));

        // Stale lastError on a later clean idle/completed shell must NOT trigger Refused.
        let stale_idle = json!({
            "status": "idle",
            "lastError": "Request blocked by cyber_safeguard policy"
        });
        assert_eq!(
            derive_shell_observation(&stale_idle, None),
            Some(ShellObservation::Idle {
                mirror_candidate: true
            })
        );

        let failed_refusal = json!({
            "status": "failed",
            "lastError": "Request blocked by cyber_safeguard policy"
        });
        assert!(matches!(
            derive_shell_observation(&failed_refusal, None),
            Some(ShellObservation::Refused(ref r)) if r.contains("cyber_safeguard")
        ));
    }
}
