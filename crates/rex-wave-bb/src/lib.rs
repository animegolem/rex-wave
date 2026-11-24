//! BB bridge (`BbBridge` and `BbHost`): `AgentHost`, `AgentFeed`, and worker
//! tool execution over the private `/bridge` WebSocket.

mod ws;

use rex_wave_api::read::{
    AgentEvent, AgentEventKind, ContextCategory, ContextCategoryKind, Read, ThreadContextUsage,
    ThreadPageView,
};
use rex_wave_api::room::{
    Body, RoomEnsure, RoomEvent, RoomId, RoomJoin, RoomOrigin, RoomPost, RoomRef,
};
use rex_wave_api::wave::{
    Activity, BaseBranch, InteractionKind, InteractionResolution, Observation, PendingInteraction,
    PermissionMode, Question, SteerDeliveryMode, WaveEvent, WaveObserve, WaveView, WorkerPhase,
    Workspace,
};
use rex_wave_api::*;
use rex_wave_runtime::{
    AgentFeed, BoxFut, LocalClient, Reactor, Router, frame_channel, spawn_reactor,
};
use rex_wave_waves::{AgentHost, HostError, SpawnRequest};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

const CONNECT_WAIT_TIMEOUT: Duration = Duration::from_secs(3);
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

mod protocol {
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    pub type ReqId = u64;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "t", rename_all = "snake_case")]
    pub enum Up {
        Hello {
            plugin_version: String,
            sdk_version: String,
        },
        Thread {
            signal: ThreadSignal,
            thread_id: String,
            project_id: String,
            meta: Option<WaveMeta>,
            error: Option<String>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            stop_reason: Option<String>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            model: Option<String>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            account: Option<String>,
        },
        Interaction {
            thread_id: String,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            meta: Option<WaveMeta>,
            interaction: Value,
        },
        ThreadEvents {
            thread_id: String,
            sequence: u64,
        },
        ToolCall {
            call_id: String,
            thread_id: String,
            project_id: String,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            meta: Option<WaveMeta>,
            tool: String,
            args: Value,
        },
        Result {
            req_id: ReqId,
            ok: bool,
            value: Value,
        },
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ThreadSignal {
        Created,
        Active,
        Idle,
        Failed,
        Archived,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct WaveMeta {
        pub wave_id: String,
        pub role: String,
        pub attempt: u32,
        pub room_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub run_stamp: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub output_dir: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pub writes: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub account: Option<String>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum EventsOrder {
        Asc,
        Desc,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "t", rename_all = "snake_case")]
    pub enum Down {
        Spawn {
            req_id: ReqId,
            args: Value,
        },
        FindByMeta {
            req_id: ReqId,
            meta: WaveMeta,
        },
        Send {
            req_id: ReqId,
            thread_id: String,
            mode: String,
            text: String,
        },
        Resolve {
            req_id: ReqId,
            thread_id: String,
            interaction_id: String,
            resolution: Value,
        },
        Retire {
            req_id: ReqId,
            thread_id: String,
        },
        EventsPage {
            req_id: ReqId,
            thread_id: String,
            after_seq: Option<u64>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            before_seq: Option<u64>,
            limit: u32,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            order: Option<EventsOrder>,
        },
        ThreadContext {
            req_id: ReqId,
            thread_id: String,
        },
        WatchThread {
            thread_id: String,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            after_seq: Option<u64>,
        },
        UnwatchThread {
            thread_id: String,
        },
        ToolResult {
            call_id: String,
            ok: bool,
            text: String,
        },
        Push {
            title: String,
            body: String,
        },
    }

    impl Down {
        pub fn with_req_id(self, id: ReqId) -> Self {
            match self {
                Down::Spawn { args, .. } => Down::Spawn { req_id: id, args },
                Down::FindByMeta { meta, .. } => Down::FindByMeta { req_id: id, meta },
                Down::Send {
                    thread_id,
                    mode,
                    text,
                    ..
                } => Down::Send {
                    req_id: id,
                    thread_id,
                    mode,
                    text,
                },
                Down::Resolve {
                    thread_id,
                    interaction_id,
                    resolution,
                    ..
                } => Down::Resolve {
                    req_id: id,
                    thread_id,
                    interaction_id,
                    resolution,
                },
                Down::Retire { thread_id, .. } => Down::Retire {
                    req_id: id,
                    thread_id,
                },
                Down::EventsPage {
                    thread_id,
                    after_seq,
                    before_seq,
                    limit,
                    order,
                    ..
                } => Down::EventsPage {
                    req_id: id,
                    thread_id,
                    after_seq,
                    before_seq,
                    limit,
                    order,
                },
                Down::ThreadContext { thread_id, .. } => Down::ThreadContext {
                    req_id: id,
                    thread_id,
                },
                other => other,
            }
        }
    }
}

pub fn spawn_args(req: &SpawnRequest) -> serde_json::Value {
    let environment = match &req.agent.workspace {
        Workspace::ManagedWorktree { base } => {
            let base_branch = match base {
                BaseBranch::Default => serde_json::json!({ "kind": "default" }),
                BaseBranch::Named { name } => serde_json::json!({ "kind": "named", "name": name }),
            };
            serde_json::json!({
                "type": "host",
                "workspace": {
                    "type": "managed-worktree",
                    "baseBranch": base_branch,
                },
            })
        }
        Workspace::ExistingPath { path } => serde_json::json!({
            "type": "host",
            "workspace": {
                "type": "unmanaged",
                "path": path.to_string_lossy(),
            },
        }),
        Workspace::ProjectDefault => serde_json::json!({
            "type": "project-default",
        }),
    };

    let permission = match req.agent.permission {
        PermissionMode::AcceptEdits => "accept-edits",
        PermissionMode::Auto => "auto",
        PermissionMode::Full => "full",
    };

    let meta = protocol::WaveMeta {
        wave_id: req.wave.to_string(),
        role: req.role.to_string(),
        attempt: req.attempt,
        room_id: serde_json::to_string(&req.room).unwrap_or_default(),
        run_stamp: (!req.run_stamp.is_empty()).then(|| req.run_stamp.clone()),
        output_dir: req.output_dir.as_ref().map(|p| p.display().to_string()),
        writes: req.writes.iter().map(|p| p.display().to_string()).collect(),
        account: req.agent.account.clone(),
    };

    let mut obj = serde_json::Map::new();
    obj.insert(
        "projectId".into(),
        serde_json::Value::String(req.project.to_string()),
    );
    obj.insert("environment".into(), environment);
    obj.insert(
        "providerId".into(),
        serde_json::Value::String(req.agent.provider.to_string()),
    );
    if let Some(model) = &req.agent.model {
        obj.insert("model".into(), serde_json::Value::String(model.to_string()));
    }
    if let Some(reasoning) = &req.agent.reasoning {
        obj.insert(
            "reasoningLevel".into(),
            serde_json::Value::String(reasoning.clone()),
        );
    }
    if let Some(tier) = &req.agent.service_tier {
        obj.insert(
            "serviceTier".into(),
            serde_json::Value::String(tier.clone()),
        );
    }
    obj.insert(
        "permissionMode".into(),
        serde_json::Value::String(permission.into()),
    );
    obj.insert(
        "visibility".into(),
        serde_json::Value::String("hidden".into()),
    );
    obj.insert("title".into(), serde_json::Value::String(req.title.clone()));
    obj.insert(
        "prompt".into(),
        serde_json::Value::String(req.prompt.as_str().to_owned()),
    );
    obj.insert(
        "pluginMetadata".into(),
        serde_json::json!({
            "rexWave": serde_json::to_value(meta).expect("WaveMeta serializes"),
        }),
    );
    serde_json::Value::Object(obj)
}

pub fn resolution_json(r: &InteractionResolution) -> serde_json::Value {
    let grants_json = |grants: &Option<rex_wave_api::wave::GrantedPermissions>| match grants {
        None => serde_json::Value::Null,
        Some(g) => {
            let fs = match &g.file_system {
                None => serde_json::Value::Null,
                Some(f) => serde_json::json!({ "read": f.read, "write": f.write }),
            };
            let net = match &g.network {
                None => serde_json::Value::Null,
                Some(n) => serde_json::json!({ "enabled": n.enabled }),
            };
            serde_json::json!({ "fileSystem": fs, "network": net })
        }
    };

    match r {
        InteractionResolution::AllowOnce {
            granted_permissions,
        } => serde_json::json!({
            "decision": "allow_once",
            "grantedPermissions": grants_json(granted_permissions),
        }),
        InteractionResolution::AllowForSession {
            granted_permissions,
        } => serde_json::json!({
            "decision": "allow_for_session",
            "grantedPermissions": grants_json(granted_permissions),
        }),
        InteractionResolution::Deny => serde_json::json!({ "decision": "deny" }),
        InteractionResolution::UserAnswer { answers } => {
            let mut map = serde_json::Map::new();
            for (qid, ans) in answers {
                let mut entry = serde_json::Map::new();
                entry.insert("selected".into(), serde_json::json!(ans.selected));
                if let Some(ft) = &ans.free_text {
                    entry.insert("freeText".into(), serde_json::Value::String(ft.clone()));
                }
                map.insert(qid.clone(), serde_json::Value::Object(entry));
            }
            serde_json::json!({
                "kind": "user_answer",
                "answers": serde_json::Value::Object(map),
            })
        }
        InteractionResolution::PluginSubmitted {
            title,
            detail,
            payload,
        } => {
            let mut desc = serde_json::Map::new();
            if let Some(t) = title {
                desc.insert("title".into(), serde_json::Value::String(t.clone()));
            }
            if let Some(d) = detail {
                desc.insert("detail".into(), serde_json::Value::String(d.clone()));
            }
            if let Some(p) = payload {
                desc.insert("payload".into(), p.clone());
            }
            if desc.is_empty() {
                serde_json::json!({ "kind": "plugin_submitted" })
            } else {
                serde_json::json!({
                    "kind": "plugin_submitted",
                    "description": serde_json::Value::Object(desc),
                })
            }
        }
        InteractionResolution::RequestAnswer { value } => serde_json::json!({
            "kind": "request_answer",
            "value": value,
        }),
    }
}

pub fn send_mode(mode: SteerDeliveryMode) -> &'static str {
    match mode {
        SteerDeliveryMode::Auto => "auto",
        SteerDeliveryMode::Start => "start",
        SteerDeliveryMode::Steer => "steer",
        SteerDeliveryMode::SteerIfActive => "steer-if-active",
        SteerDeliveryMode::QueueIfActive => "queue-if-active",
    }
}

pub fn bb_tool_catalog() -> serde_json::Value {
    let mut entries: Vec<serde_json::Value> = TOOLS
        .iter()
        .filter(|t| t.audience == Audience::Member)
        .map(|t| serde_json::json!({ "name": t.name, "description": t.doc, "inputSchema": (t.input_schema)() }))
        .collect();
    entries.push(serde_json::json!({
        "name": "run_code",
        "description": "Code Mode: run JS in a fresh capped QuickJS sandbox with a frozen `rex` global exposing every tool.",
        "inputSchema": { "type": "object", "required": ["code"], "properties": { "code": { "type": "string" } } }
    }));
    serde_json::Value::Array(entries)
}

pub fn parse_pending_interaction(raw: &serde_json::Value) -> Option<PendingInteraction> {
    let id = BbInteractionId::parse(raw.get("id")?.as_str()?).ok()?;
    let payload = raw.get("payload");
    let kind_str = payload
        .and_then(|p| p.get("kind"))
        .and_then(|k| k.as_str())
        .unwrap_or("extension");

    let (kind, summary) = match kind_str {
        "approval" => {
            let reason = payload
                .and_then(|p| p.get("reason"))
                .and_then(|r| r.as_str());
            let cmd = payload
                .and_then(|p| p.get("subject"))
                .and_then(|s| {
                    s.get("command")
                        .or_else(|| s.get("tool"))
                        .or_else(|| s.get("title"))
                })
                .and_then(|v| v.as_str());
            let summary = reason
                .or(cmd)
                .unwrap_or("Permission approval requested")
                .to_owned();
            (InteractionKind::Permission, summary)
        }
        "user_question" => {
            let mut questions = Vec::new();
            if let Some(arr) = payload
                .and_then(|p| p.get("questions"))
                .and_then(|q| q.as_array())
            {
                for q in arr {
                    let Some(qid) = q.get("id").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    let prompt = q
                        .get("prompt")
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
                .unwrap_or_else(|| "Question from worker".into());
            (InteractionKind::Question { questions }, summary)
        }
        _ => {
            let summary = payload
                .and_then(|p| p.get("title"))
                .and_then(|v| v.as_str())
                .unwrap_or(kind_str)
                .to_owned();
            (InteractionKind::Extension, summary)
        }
    };

    Some(PendingInteraction { id, kind, summary })
}

pub fn parse_context_usage(raw: &serde_json::Value) -> Option<ThreadContextUsage> {
    if raw.is_null() {
        return None;
    }
    if let Some(used_tokens) = raw.get("used_tokens").and_then(|v| v.as_u64()) {
        let model_context_window = raw
            .get("model_context_window")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let estimated = raw
            .get("estimated")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let auto_compact_at_tokens = raw.get("auto_compact_at_tokens").and_then(|v| v.as_u64());
        let cost_usd = raw.get("cost_usd").and_then(|v| v.as_f64());
        let categories = parse_categories(raw.get("categories"));
        return Some(ThreadContextUsage {
            used_tokens,
            model_context_window,
            estimated,
            auto_compact_at_tokens,
            cost_usd,
            categories,
        });
    }
    let usage = raw.get("usage").unwrap_or(raw);
    if usage.is_null() {
        return None;
    }
    let used_tokens = usage.get("usedTokens")?.as_u64()?;
    let model_context_window = usage
        .get("modelContextWindow")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let estimated = usage
        .get("estimated")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let snapshot = usage.get("snapshot");
    let auto_compact_at_tokens = snapshot
        .and_then(|s| s.get("autoCompactAtTokens"))
        .and_then(|v| v.as_u64());
    let cost_usd = usage.get("cost").and_then(|c| {
        let currency = c.get("currency")?.as_str()?;
        if currency.eq_ignore_ascii_case("USD") {
            c.get("amount")?.as_f64()
        } else {
            None
        }
    });
    let categories = parse_categories(snapshot.and_then(|s| s.get("categories")));
    Some(ThreadContextUsage {
        used_tokens,
        model_context_window,
        estimated,
        auto_compact_at_tokens,
        cost_usd,
        categories,
    })
}

fn parse_categories(val: Option<&serde_json::Value>) -> Vec<ContextCategory> {
    let Some(arr) = val.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in arr {
        let Some(id) = item.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        let label = item.get("label").and_then(|v| v.as_str()).unwrap_or(id);
        let tokens = item.get("tokens").and_then(|v| v.as_u64()).unwrap_or(0);
        let kind = match item.get("kind").and_then(|v| v.as_str()) {
            Some("reserved") => ContextCategoryKind::Reserved,
            Some("deferred") => ContextCategoryKind::Deferred,
            Some("free") => ContextCategoryKind::Free,
            _ => ContextCategoryKind::Used,
        };
        out.push(ContextCategory {
            id: id.to_owned(),
            label: label.to_owned(),
            kind,
            tokens,
        });
    }
    out
}

pub fn normalize_agent_event(row: &serde_json::Value) -> Option<AgentEvent> {
    let seq = BbSeq(row.get("seq")?.as_u64()?);
    let at = UnixMs(row.get("createdAt").and_then(|v| v.as_u64()).unwrap_or(0));
    let ty = row
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let data = row.get("data").unwrap_or(&serde_json::Value::Null);

    let kind = match ty {
        "turn/started" | "client/turn/start" => AgentEventKind::TurnStarted,
        "turn/completed" => AgentEventKind::TurnEnded,
        "system/manager/user_message" => {
            let text = data
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned();
            AgentEventKind::UserMessage { text }
        }
        "item/agentMessage/delta" => {
            let text = data
                .get("delta")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned();
            AgentEventKind::AssistantText { text }
        }
        "item/started" | "item/completed" => {
            let completed = ty == "item/completed";
            let item = data.get("item").unwrap_or(&serde_json::Value::Null);
            let item_type = item
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            match item_type {
                "userMessage" => {
                    let text = extract_user_content(item.get("content"));
                    AgentEventKind::UserMessage { text }
                }
                "agentMessage" => {
                    let text = item
                        .get("text")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_owned();
                    AgentEventKind::AssistantText { text }
                }
                "commandExecution" | "toolCall" | "fileChange" | "fileRead" | "search"
                | "webSearch" | "webFetch" => {
                    let name = item
                        .get("tool")
                        .or_else(|| item.get("name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(item_type)
                        .to_owned();
                    let summary = item
                        .get("command")
                        .or_else(|| item.get("path"))
                        .or_else(|| item.get("url"))
                        .or_else(|| item.get("presentation").and_then(|p| p.get("title")))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_owned();
                    if completed {
                        let status = item
                            .get("status")
                            .and_then(|v| v.as_str())
                            .unwrap_or("completed");
                        AgentEventKind::ToolResult {
                            name,
                            ok: status == "completed",
                            summary,
                        }
                    } else {
                        AgentEventKind::ToolCall { name, summary }
                    }
                }
                _ => AgentEventKind::Other {
                    bb_type: format!("{ty}:{item_type}"),
                },
            }
        }
        "thread/contextWindowUsage/updated" => match parse_context_usage(data) {
            Some(usage) => AgentEventKind::Usage { usage },
            None => AgentEventKind::Other {
                bb_type: ty.to_owned(),
            },
        },
        _ => AgentEventKind::Other {
            bb_type: ty.to_owned(),
        },
    };

    Some(AgentEvent { seq, at, kind })
}

fn extract_user_content(content: Option<&serde_json::Value>) -> String {
    let Some(arr) = content.and_then(|v| v.as_array()) else {
        return String::new();
    };
    let mut parts = Vec::new();
    for block in arr {
        if let Some(t) = block.get("text").and_then(|v| v.as_str()) {
            parts.push(t);
        }
    }
    parts.join("\n")
}

fn extract_row_error_message(data: &serde_json::Value) -> Option<&str> {
    data.get("message")
        .and_then(|v| v.as_str())
        .or_else(|| {
            data.get("error").and_then(|e| {
                e.as_str()
                    .or_else(|| e.get("message").and_then(|m| m.as_str()))
            })
        })
        .or_else(|| data.get("reason").and_then(|v| v.as_str()))
        .or_else(|| data.get("detail").and_then(|v| v.as_str()))
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn is_retrying_provider_error(row: &serde_json::Value) -> bool {
    row.get("type").and_then(|v| v.as_str()) == Some("provider/error")
        && row
            .get("data")
            .and_then(|d| d.get("willRetry"))
            .and_then(|v| v.as_bool())
            == Some(true)
}

fn extract_refusal_from_row(row: &serde_json::Value) -> Option<String> {
    if is_retrying_provider_error(row) {
        return None;
    }
    let ty = row.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let data = row.get("data").unwrap_or(&serde_json::Value::Null);
    let stop_candidates = [
        data.get("stopReason").and_then(|v| v.as_str()),
        data.get("stop_reason").and_then(|v| v.as_str()),
        data.get("finishReason").and_then(|v| v.as_str()),
        data.get("finish_reason").and_then(|v| v.as_str()),
        data.get("status").and_then(|v| v.as_str()),
        data.get("errorInfo")
            .and_then(|e| e.get("providerCode"))
            .and_then(|v| v.as_str()),
        data.get("errorInfo")
            .and_then(|e| e.get("category"))
            .and_then(|v| v.as_str()),
        data.get("code").and_then(|v| v.as_str()),
    ];
    let msg = extract_row_error_message(data);

    match ty {
        "turn/completed" | "system/error" | "provider/error" => {
            for sr in stop_candidates.into_iter().flatten() {
                if rex_wave_api::wave::is_refusal_stop_reason(sr) {
                    return Some(match msg {
                        Some(m) if !m.to_ascii_lowercase().contains(&sr.to_ascii_lowercase()) => {
                            format!("{sr}: {m}")
                        }
                        Some(m) => m.to_owned(),
                        None => sr.to_owned(),
                    });
                }
            }
            if let Some(m) = msg
                && rex_wave_api::wave::is_refusal_error_text(m)
            {
                return Some(m.to_owned());
            }
            None
        }
        _ => None,
    }
}

/// Extract runtime-reported `(actual_model, actual_account)` from a BB event row.
/// Ignores outbound request echoes (`client/turn/start`, `turn/request`, `execution`, `sessionOptions`).
fn extract_runtime_meta_from_row(row: &serde_json::Value) -> (Option<ModelId>, Option<String>) {
    let ty = row.get("type").and_then(|v| v.as_str()).unwrap_or("");
    if matches!(ty, "client/turn/start" | "turn/request") {
        return (None, None);
    }
    let data = row.get("data").unwrap_or(&serde_json::Value::Null);
    let model_raw = data
        .get("fallbackModel")
        .or_else(|| {
            data.get("contextWindowUsage")
                .and_then(|c| c.get("snapshot"))
                .and_then(|s| s.get("model"))
        })
        .or_else(|| data.get("snapshot").and_then(|s| s.get("model")))
        .or_else(|| data.get("actualModel"))
        .or_else(|| data.get("actual_model"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let model = model_raw.and_then(|m| ModelId::parse(m).ok());

    let account = data
        .get("actualAccount")
        .or_else(|| data.get("actual_account"))
        .or_else(|| data.get("accountEmail"))
        .or_else(|| data.get("accountName"))
        .or_else(|| data.get("account_name"))
        .or_else(|| data.get("account"))
        .or_else(|| data.get("profile"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);

    (model, account)
}

fn extract_runtime_meta_from_rows(rows: &[serde_json::Value]) -> (Option<ModelId>, Option<String>) {
    let mut ordered: Vec<&serde_json::Value> = rows.iter().collect();
    ordered.sort_by_key(|r| r.get("seq").and_then(|v| v.as_u64()).unwrap_or(0));
    let mut latest_model = None;
    let mut latest_account = None;
    for row in ordered {
        let (m, a) = extract_runtime_meta_from_row(row);
        if m.is_some() {
            latest_model = m;
        }
        if a.is_some() {
            latest_account = a;
        }
    }
    (latest_model, latest_account)
}

fn activity_from_row(row: &serde_json::Value) -> Option<Activity> {
    if is_retrying_provider_error(row) {
        return None;
    }
    if let Some(reason) = extract_refusal_from_row(row) {
        return Some(Activity::Refused { reason });
    }
    let data = row.get("data").unwrap_or(&serde_json::Value::Null);
    match row.get("type").and_then(|v| v.as_str()) {
        Some("turn/started" | "client/turn/start") => Some(Activity::Working),
        Some("turn/completed") => {
            if data.get("status").and_then(|v| v.as_str()) == Some("failed") {
                let err = extract_row_error_message(data)
                    .unwrap_or("BB turn failed")
                    .to_owned();
                Some(Activity::Errored { error: err })
            } else {
                Some(Activity::Idle)
            }
        }
        Some("system/error" | "provider/error") => {
            let msg = extract_row_error_message(data)
                .unwrap_or("BB error")
                .to_owned();
            Some(Activity::Errored { error: msg })
        }
        _ => None,
    }
}

/// Derive the latest turn's activity from a set of BB thread event rows.
/// Uses the terminal `turn/completed` row's status (plus any non-retrying `provider/error`
/// in the same failed turn) so BB-shaped `turn/completed { status: "failed", error: { message } }`
/// never downgrades a refusal or failure to `Idle`, and transient `willRetry: true` errors
/// in a turn that completed normally never poison it as `Refused`.
fn derive_latest_turn_activity(rows: &[serde_json::Value]) -> Option<Activity> {
    if rows.is_empty() {
        return None;
    }
    let mut ordered: Vec<&serde_json::Value> = rows.iter().collect();
    ordered.sort_by_key(|r| r.get("seq").and_then(|v| v.as_u64()).unwrap_or(0));
    let start_idx = ordered
        .iter()
        .rposition(|r| {
            matches!(
                r.get("type").and_then(|v| v.as_str()),
                Some("turn/started" | "client/turn/start")
            )
        })
        .unwrap_or(0);
    let slice = &ordered[start_idx..];

    if let Some(tc_idx) = slice
        .iter()
        .rposition(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"))
    {
        let tc = slice[tc_idx];
        if let Some(reason) = extract_refusal_from_row(tc) {
            return Some(Activity::Refused { reason });
        }
        let data = tc.get("data").unwrap_or(&serde_json::Value::Null);
        if data.get("status").and_then(|v| v.as_str()) == Some("failed") {
            // Check preceding non-retrying provider/system error rows in this same failed turn
            // for structured policy/refusal metadata (`errorInfo.category == "policy"`, etc.).
            for row in slice[..tc_idx].iter().rev() {
                if let Some(reason) = extract_refusal_from_row(row) {
                    return Some(Activity::Refused { reason });
                }
            }
            let err = extract_row_error_message(data)
                .or_else(|| {
                    slice[..tc_idx].iter().rev().find_map(|r| {
                        if is_retrying_provider_error(r) {
                            return None;
                        }
                        if matches!(
                            r.get("type").and_then(|v| v.as_str()),
                            Some("provider/error" | "system/error")
                        ) {
                            extract_row_error_message(
                                r.get("data").unwrap_or(&serde_json::Value::Null),
                            )
                        } else {
                            None
                        }
                    })
                })
                .unwrap_or("BB turn failed")
                .to_owned();
            return Some(Activity::Errored { error: err });
        }
        return Some(Activity::Idle);
    }

    for row in slice.iter().rev() {
        if let Some(act) = activity_from_row(row) {
            return Some(act);
        }
    }
    None
}

fn refusal_from_latest_turn(rows: &[serde_json::Value]) -> Option<String> {
    match derive_latest_turn_activity(rows) {
        Some(Activity::Refused { reason }) => Some(reason),
        _ => None,
    }
}

fn classify_missing_or_archived_thread(msg: &str) -> Option<Observation> {
    let lower = msg.to_ascii_lowercase();
    if lower.contains("not found")
        || lower.contains("unknown thread")
        || lower.contains("no such thread")
        || lower.contains("does not exist")
        || lower.contains("thread deleted")
        || lower.contains("thread_not_found")
        || lower.contains("404")
    {
        Some(Observation::Activity {
            activity: Activity::Errored {
                error: format!("host thread missing after restart ({msg})"),
            },
        })
    } else if lower.contains("archived") {
        Some(Observation::Archived)
    } else {
        None
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CompletedTurn {
    anchor: String,
    turn_start_at: u64,
    text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TurnMirrorCheck {
    Ignore,
    PostedViaTool,
    Unposted(CompletedTurn),
}

const MAX_MIRROR_SCAN_PAGES: usize = 16;

fn truncate_body_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

fn inspect_latest_completed_turn(rows: &[serde_json::Value]) -> TurnMirrorCheck {
    if rows.is_empty() {
        return TurnMirrorCheck::Ignore;
    }
    let mut ordered: Vec<&serde_json::Value> = rows.iter().collect();
    ordered.sort_by_key(|r| r.get("seq").and_then(|v| v.as_u64()).unwrap_or(0));

    let Some(end_idx) = ordered
        .iter()
        .rposition(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"))
    else {
        return TurnMirrorCheck::Ignore;
    };
    let status = ordered[end_idx]
        .get("data")
        .and_then(|d| d.get("status"))
        .and_then(|v| v.as_str());
    if matches!(
        status,
        Some("failed" | "cancelled" | "canceled" | "interrupted" | "aborted")
    ) {
        return TurnMirrorCheck::Ignore;
    }
    let turn_end_seq = ordered[end_idx]
        .get("seq")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let upto_end = &ordered[..=end_idx];

    let start_idx = upto_end.iter().rposition(|r| {
        matches!(
            r.get("type").and_then(|v| v.as_str()),
            Some("turn/started" | "client/turn/start")
        )
    });
    let (turn_start_seq, turn_start_at, slice) = match start_idx {
        Some(idx) => {
            let seq = upto_end[idx]
                .get("seq")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let at = upto_end[idx]
                .get("createdAt")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            (Some(seq), at, &upto_end[idx..])
        }
        None => (None, 0, upto_end),
    };

    for row in slice {
        if row.get("type").and_then(|v| v.as_str()) != Some("item/completed") {
            continue;
        }
        let item = row.get("data").and_then(|d| d.get("item"));
        let Some(item) = item else { continue };
        if item.get("type").and_then(|v| v.as_str()) != Some("toolCall") {
            continue;
        }
        let status = item
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("completed");
        if matches!(status, "failed" | "error" | "cancelled" | "canceled") {
            continue;
        }
        let tool = item
            .get("tool")
            .or_else(|| item.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if matches!(tool, "rex_room_post" | "room_post") {
            let is_dm = item
                .get("arguments")
                .and_then(|a| a.get("room"))
                .and_then(|rm| rm.get("ref"))
                .and_then(|v| v.as_str())
                == Some("dm");
            if !is_dm {
                return TurnMirrorCheck::PostedViaTool;
            }
        }
    }

    let mut last_completed: Option<(u64, String)> = None;
    let mut trailing_deltas = String::new();
    let mut last_delta_seq = 0u64;
    for row in slice {
        let seq = row.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
        let ty = row.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let data = row.get("data").unwrap_or(&serde_json::Value::Null);
        match ty {
            "item/agentMessage/delta" => {
                if let Some(d) = data.get("delta").and_then(|v| v.as_str())
                    && !d.is_empty()
                {
                    trailing_deltas.push_str(d);
                    last_delta_seq = seq;
                }
            }
            "item/completed" => {
                let item = data.get("item").unwrap_or(&serde_json::Value::Null);
                if item.get("type").and_then(|v| v.as_str()) == Some("agentMessage") {
                    trailing_deltas.clear();
                    if let Some(t) = item.get("text").and_then(|v| v.as_str())
                        && !t.trim().is_empty()
                    {
                        last_completed = Some((seq, t.to_owned()));
                    }
                }
            }
            _ => {}
        }
    }

    let text = if !trailing_deltas.trim().is_empty() {
        let _ = last_delta_seq;
        trailing_deltas
    } else if let Some((_, t)) = last_completed {
        t
    } else {
        return TurnMirrorCheck::Ignore;
    };

    let anchor = match turn_start_seq {
        Some(s) => format!("s{s}"),
        None => format!("e{turn_end_seq}"),
    };

    TurnMirrorCheck::Unposted(CompletedTurn {
        anchor,
        turn_start_at,
        text,
    })
}

#[cfg(test)]
fn extract_unposted_final_turn(rows: &[serde_json::Value]) -> Option<CompletedTurn> {
    match inspect_latest_completed_turn(rows) {
        TurnMirrorCheck::Unposted(turn) => Some(turn),
        TurnMirrorCheck::Ignore | TurnMirrorCheck::PostedViaTool => None,
    }
}

fn default_bb_data_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("BB_DATA_DIR")
        && !dir.is_empty()
    {
        return Some(PathBuf::from(dir));
    }
    std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .map(|h| PathBuf::from(h).join(".bb"))
}

fn parse_project_rows(val: &serde_json::Value, out: &mut BTreeMap<BbProjectId, String>) {
    let arr = val
        .as_array()
        .or_else(|| val.get("projects").and_then(|v| v.as_array()));
    let Some(rows) = arr else { return };
    for row in rows {
        if let (Some(id_raw), Some(name_raw)) = (
            row.get("id").and_then(|v| v.as_str()),
            row.get("name").and_then(|v| v.as_str()),
        ) {
            let name = name_raw.trim();
            if !name.is_empty()
                && let Ok(id) = BbProjectId::parse(id_raw)
            {
                out.insert(id, name.to_owned());
            }
        }
    }
}

/// The largest BB `projects.json` cache read.
const MAX_PROJECT_CACHE_BYTES: u64 = 16 * 1024 * 1024;

pub fn load_bb_projects_from_dir(dir: &Path) -> BTreeMap<BbProjectId, String> {
    let mut map = BTreeMap::new();
    let json_cache = dir.join("projects.json");
    if let Ok(raw) = rex_wave_fs::read_regular_bounded(&json_cache, MAX_PROJECT_CACHE_BYTES)
        && let Ok(val) = serde_json::from_slice::<serde_json::Value>(&raw)
    {
        parse_project_rows(&val, &mut map);
    }

    let db_path = dir.join("bb.db");
    if db_path.is_file()
        && let Ok(output) = std::process::Command::new("sqlite3")
            .args([
                "-readonly",
                "-json",
                &format!("file:{}?mode=ro", db_path.display()),
                "SELECT id, name FROM projects",
            ])
            .output()
        && output.status.success()
        && let Ok(val) = serde_json::from_slice::<serde_json::Value>(&output.stdout)
    {
        parse_project_rows(&val, &mut map);
    }
    map
}

pub trait BridgeBackend: Send + Sync + 'static {
    type C: Client + Clone + 'static;
    fn client(&self, me: Principal) -> Self::C;
}

impl BridgeBackend for Arc<Router> {
    type C = LocalClient;
    fn client(&self, me: Principal) -> LocalClient {
        LocalClient::new(self.clone(), me)
    }
}

trait ErasedBackend: Send + Sync {
    fn call_as(&self, me: Principal, cmd: WireCommand) -> BoxFut<'_, Result<WireReply, ApiError>>;
}

impl<B: BridgeBackend> ErasedBackend for B {
    fn call_as(&self, me: Principal, cmd: WireCommand) -> BoxFut<'_, Result<WireReply, ApiError>> {
        let client = self.client(me);
        Box::pin(async move { client.call_wire(cmd).await })
    }
}

#[derive(Clone, Debug)]
pub struct BridgeConfig {
    pub url: String,
    pub token: String,
}

#[derive(Clone, Debug)]
struct TrackedWorker {
    attempt: u32,
    thread: Option<BbThreadId>,
    live: bool,
    activity: Activity,
    pending_interaction: Option<PendingInteraction>,
    last_seq: Option<u64>,
    mirror_room_watermark: Seq,
}

enum OwnerMsg {
    Connected {
        tx: mpsc::UnboundedSender<protocol::Down>,
        reply: oneshot::Sender<u64>,
    },
    Disconnected {
        conn_id: u64,
    },
    WaitConnected {
        reply: oneshot::Sender<()>,
    },
    Rpc {
        down: protocol::Down,
        started: oneshot::Sender<
            Result<(u64, oneshot::Receiver<Result<serde_json::Value, HostError>>), HostError>,
        >,
    },
    CancelRpc {
        req_id: u64,
    },
    Fire {
        down: protocol::Down,
        reply: oneshot::Sender<Result<(), HostError>>,
    },
    Result {
        req_id: u64,
        ok: bool,
        value: serde_json::Value,
    },
    BindThread {
        wave: WaveSlug,
        role: RoleName,
        attempt: u32,
        thread: BbThreadId,
    },
    FoldWave {
        wave: WaveSlug,
        event: WaveEvent,
    },
    LookupByThread {
        thread: BbThreadId,
        reply: oneshot::Sender<Option<(WaveSlug, RoleName, u32)>>,
    },
    LookupByRole {
        wave: WaveSlug,
        role: RoleName,
        reply: oneshot::Sender<Option<TrackedWorker>>,
    },
    ListLiveWorkers {
        reply: oneshot::Sender<Vec<(WaveSlug, RoleName, BbThreadId, Activity, Option<u64>)>>,
    },
    AdvanceSeq {
        thread: BbThreadId,
        seq: u64,
    },
    ThreadEvents {
        thread: BbThreadId,
        sequence: u64,
    },
    WatchOpen {
        thread: BbThreadId,
        after_seq: Option<u64>,
        reply: oneshot::Sender<(u64, mpsc::UnboundedReceiver<u64>)>,
    },
    WatchClose {
        thread: BbThreadId,
        lease_id: u64,
    },
    MirrorWatermark {
        wave: WaveSlug,
        role: RoleName,
        reply: oneshot::Sender<Seq>,
    },
    AdvanceMirrorWatermark {
        wave: WaveSlug,
        role: RoleName,
        seq: Seq,
    },
}

struct BridgeOwner {
    conn_id: u64,
    tx_down: Option<mpsc::UnboundedSender<protocol::Down>>,
    conn_waiters: Vec<oneshot::Sender<()>>,
    next_req_id: u64,
    next_lease_id: u64,
    pending: HashMap<u64, oneshot::Sender<Result<serde_json::Value, HostError>>>,
    workers_by_role: HashMap<(WaveSlug, RoleName), TrackedWorker>,
    role_by_thread: HashMap<BbThreadId, (WaveSlug, RoleName)>,
    watchers: HashMap<BbThreadId, Vec<(u64, mpsc::UnboundedSender<u64>)>>,
}

impl BridgeOwner {
    fn spawn() -> mpsc::UnboundedSender<OwnerMsg> {
        let (tx, rx) = mpsc::unbounded_channel();
        let owner = BridgeOwner {
            conn_id: 0,
            tx_down: None,
            conn_waiters: Vec::new(),
            next_req_id: 0,
            next_lease_id: 0,
            pending: HashMap::new(),
            workers_by_role: HashMap::new(),
            role_by_thread: HashMap::new(),
            watchers: HashMap::new(),
        };
        tokio::spawn(owner.run(rx));
        tx
    }

    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<OwnerMsg>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                OwnerMsg::Connected { tx, reply } => {
                    self.conn_id += 1;
                    let id = self.conn_id;
                    for (thread, subs) in &mut self.watchers {
                        subs.retain(|(_, s)| !s.is_closed());
                        if !subs.is_empty() {
                            let after_seq = self
                                .role_by_thread
                                .get(thread)
                                .and_then(|k| self.workers_by_role.get(k))
                                .and_then(|w| w.last_seq);
                            let _ = tx.send(protocol::Down::WatchThread {
                                thread_id: thread.to_string(),
                                after_seq,
                            });
                        }
                    }
                    self.watchers.retain(|_, subs| !subs.is_empty());
                    self.tx_down = Some(tx);
                    let _ = reply.send(id);
                    for w in self.conn_waiters.drain(..) {
                        let _ = w.send(());
                    }
                }
                OwnerMsg::Disconnected { conn_id } => {
                    if self.conn_id == conn_id {
                        self.tx_down = None;
                        for (_, waiter) in self.pending.drain() {
                            let _ = waiter
                                .send(Err(HostError::Unreachable("BB bridge disconnected".into())));
                        }
                    }
                }
                OwnerMsg::WaitConnected { reply } => {
                    if self.tx_down.is_some() {
                        let _ = reply.send(());
                    } else {
                        self.conn_waiters.push(reply);
                    }
                }
                OwnerMsg::Rpc { down, started } => {
                    let Some(tx) = &self.tx_down else {
                        let _ = started.send(Err(HostError::Unreachable(
                            "BB bridge is not connected".into(),
                        )));
                        continue;
                    };
                    self.next_req_id += 1;
                    let id = self.next_req_id;
                    let tagged = down.with_req_id(id);
                    if tx.send(tagged).is_err() {
                        self.tx_down = None;
                        let _ = started.send(Err(HostError::Unreachable(
                            "BB bridge write channel closed".into(),
                        )));
                    } else {
                        let (res_tx, res_rx) = oneshot::channel();
                        self.pending.insert(id, res_tx);
                        let _ = started.send(Ok((id, res_rx)));
                    }
                }
                OwnerMsg::CancelRpc { req_id } => {
                    self.pending.remove(&req_id);
                }
                OwnerMsg::Fire { down, reply } => {
                    let res = match &self.tx_down {
                        Some(tx) => tx.send(down).map_err(|_| {
                            HostError::Unreachable("BB bridge write channel closed".into())
                        }),
                        None => Err(HostError::Unreachable("BB bridge is not connected".into())),
                    };
                    let _ = reply.send(res);
                }
                OwnerMsg::Result { req_id, ok, value } => {
                    if let Some(waiter) = self.pending.remove(&req_id) {
                        if ok {
                            let _ = waiter.send(Ok(value));
                        } else {
                            let msg = value
                                .as_str()
                                .map(str::to_owned)
                                .unwrap_or_else(|| value.to_string());
                            let _ = waiter.send(Err(HostError::Rejected(msg)));
                        }
                    }
                }
                OwnerMsg::BindThread {
                    wave,
                    role,
                    attempt,
                    thread,
                } => {
                    self.role_by_thread
                        .insert(thread.clone(), (wave.clone(), role.clone()));
                    let entry = self
                        .workers_by_role
                        .entry((wave, role))
                        .or_insert(TrackedWorker {
                            attempt,
                            thread: Some(thread.clone()),
                            live: true,
                            activity: Activity::Working,
                            pending_interaction: None,
                            last_seq: None,
                            mirror_room_watermark: Seq::ORIGIN,
                        });
                    entry.attempt = attempt;
                    entry.thread = Some(thread);
                    entry.live = true;
                }
                OwnerMsg::FoldWave { wave, event } => {
                    self.fold_wave(wave, event);
                }
                OwnerMsg::LookupByThread { thread, reply } => {
                    let out = self.role_by_thread.get(&thread).and_then(|(w, r)| {
                        let rec = self.workers_by_role.get(&(w.clone(), r.clone()))?;
                        rec.live.then(|| (w.clone(), r.clone(), rec.attempt))
                    });
                    let _ = reply.send(out);
                }
                OwnerMsg::LookupByRole { wave, role, reply } => {
                    let _ = reply.send(self.workers_by_role.get(&(wave, role)).cloned());
                }
                OwnerMsg::ListLiveWorkers { reply } => {
                    let mut list = Vec::new();
                    for ((w, r), rec) in &self.workers_by_role {
                        if rec.live
                            && let Some(th) = &rec.thread
                        {
                            list.push((
                                w.clone(),
                                r.clone(),
                                th.clone(),
                                rec.activity.clone(),
                                rec.last_seq,
                            ));
                        }
                    }
                    let _ = reply.send(list);
                }
                OwnerMsg::AdvanceSeq { thread, seq } => {
                    if let Some(key) = self.role_by_thread.get(&thread)
                        && let Some(rec) = self.workers_by_role.get_mut(key)
                    {
                        rec.last_seq = Some(rec.last_seq.map_or(seq, |prev| prev.max(seq)));
                    }
                }
                OwnerMsg::ThreadEvents { thread, sequence } => {
                    if let Some(subs) = self.watchers.get_mut(&thread) {
                        subs.retain(|(_, tx)| tx.send(sequence).is_ok());
                        if subs.is_empty() {
                            self.watchers.remove(&thread);
                            if let Some(tx) = &self.tx_down {
                                let _ = tx.send(protocol::Down::UnwatchThread {
                                    thread_id: thread.to_string(),
                                });
                            }
                        }
                    }
                }
                OwnerMsg::WatchOpen {
                    thread,
                    after_seq,
                    reply,
                } => {
                    self.next_lease_id += 1;
                    let lease_id = self.next_lease_id;
                    let (seq_tx, seq_rx) = mpsc::unbounded_channel();
                    let entry = self.watchers.entry(thread.clone()).or_default();
                    let was_empty = entry.is_empty();
                    entry.push((lease_id, seq_tx));
                    if was_empty && let Some(tx) = &self.tx_down {
                        let _ = tx.send(protocol::Down::WatchThread {
                            thread_id: thread.to_string(),
                            after_seq,
                        });
                    }
                    let _ = reply.send((lease_id, seq_rx));
                }
                OwnerMsg::WatchClose { thread, lease_id } => {
                    if let Some(entry) = self.watchers.get_mut(&thread) {
                        entry.retain(|(id, _)| *id != lease_id);
                        if entry.is_empty() {
                            self.watchers.remove(&thread);
                            if let Some(tx) = &self.tx_down {
                                let _ = tx.send(protocol::Down::UnwatchThread {
                                    thread_id: thread.to_string(),
                                });
                            }
                        }
                    }
                }
                OwnerMsg::MirrorWatermark { wave, role, reply } => {
                    let watermark = self
                        .workers_by_role
                        .get(&(wave, role))
                        .map_or(Seq::ORIGIN, |w| w.mirror_room_watermark);
                    let _ = reply.send(watermark);
                }
                OwnerMsg::AdvanceMirrorWatermark { wave, role, seq } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.mirror_room_watermark < seq
                    {
                        rec.mirror_room_watermark = seq;
                    }
                }
            }
        }
    }

    fn fold_wave(&mut self, wave: WaveSlug, event: WaveEvent) {
        match event {
            WaveEvent::Started { .. } => {}
            WaveEvent::WorkerRequested { role, attempt, .. } => {
                self.workers_by_role.insert(
                    (wave, role),
                    TrackedWorker {
                        attempt,
                        thread: None,
                        live: false,
                        activity: Activity::Working,
                        pending_interaction: None,
                        last_seq: None,
                        mirror_room_watermark: Seq::ORIGIN,
                    },
                );
            }
            WaveEvent::WorkerObserved { role, observation } => match observation {
                Observation::Spawned { attempt, thread } => {
                    self.role_by_thread
                        .insert(thread.clone(), (wave.clone(), role.clone()));
                    let rec = self
                        .workers_by_role
                        .entry((wave, role))
                        .or_insert(TrackedWorker {
                            attempt,
                            thread: Some(thread.clone()),
                            live: true,
                            activity: Activity::Working,
                            pending_interaction: None,
                            last_seq: None,
                            mirror_room_watermark: Seq::ORIGIN,
                        });
                    rec.attempt = attempt;
                    rec.thread = Some(thread);
                    rec.live = true;
                    rec.activity = Activity::Working;
                    rec.pending_interaction = None;
                }
                Observation::Activity { activity } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
                        if matches!(rec.activity, Activity::Refused { .. })
                            && matches!(activity, Activity::Errored { .. })
                        {
                            return;
                        }
                        rec.pending_interaction = match &activity {
                            Activity::Blocked { interaction } => Some(interaction.clone()),
                            _ => None,
                        };
                        rec.activity = activity;
                    }
                }
                Observation::Archived => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
                        rec.live = false;
                        rec.pending_interaction = None;
                        if let Some(th) = &rec.thread {
                            self.role_by_thread.remove(th);
                        }
                    }
                }
                Observation::SpawnFailed { .. } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
                        rec.live = false;
                    }
                }
                Observation::RuntimeMeta { .. } => {}
            },
            WaveEvent::WorkerRetiring { role, .. } => {
                if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
                    rec.live = false;
                }
            }
            WaveEvent::Finished { .. } => {
                for ((w, _), rec) in &mut self.workers_by_role {
                    if w == &wave {
                        rec.live = false;
                    }
                }
            }
            WaveEvent::Told { .. } | WaveEvent::InteractionResolved { .. } => {}
        }
    }
}

struct WatchLeaseGuard {
    owner: mpsc::UnboundedSender<OwnerMsg>,
    thread: BbThreadId,
    lease_id: u64,
}

impl Drop for WatchLeaseGuard {
    fn drop(&mut self) {
        let _ = self.owner.send(OwnerMsg::WatchClose {
            thread: self.thread.clone(),
            lease_id: self.lease_id,
        });
    }
}

struct BridgeWaveWatcher {
    bridge: Arc<BbBridge>,
}

impl Reactor for BridgeWaveWatcher {
    const NAME: &'static str = "bb-bridge-wave-index";

    fn interest(&self) -> Interest {
        Interest::Everything
    }

    async fn react(&mut self, frame: Frame, _client: &LocalClient) -> Result<(), ApiError> {
        match frame {
            Frame::Committed(Envelope {
                stream: StreamId::Wave { wave },
                event: Event::Wave(ev),
                ..
            }) => {
                let _ = self
                    .bridge
                    .owner
                    .send(OwnerMsg::FoldWave { wave, event: ev });
            }
            Frame::CaughtUp { .. } => {
                let bridge = self.bridge.clone();
                tokio::spawn(async move {
                    bridge.reconcile_live_workers().await;
                });
            }
            _ => {}
        }
        Ok(())
    }
}

struct WaitFlightGuard {
    active: Arc<AtomicUsize>,
    aborted: Arc<AtomicBool>,
    completed: bool,
}

impl Drop for WaitFlightGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
        if !self.completed {
            self.aborted.store(true, Ordering::Relaxed);
        }
    }
}

#[derive(Clone)]
pub struct WorkerToolClient {
    bridge: Arc<BbBridge>,
    backend: Arc<dyn ErasedBackend>,
    me: Principal,
    wave: WaveSlug,
    role: RoleName,
    active_waits: Arc<AtomicUsize>,
    aborted_wait: Arc<AtomicBool>,
}

impl Client for WorkerToolClient {
    fn me(&self) -> &Principal {
        &self.me
    }

    fn call_wire(
        &self,
        cmd: WireCommand,
    ) -> impl Future<Output = Result<WireReply, ApiError>> + Send {
        let bridge = self.bridge.clone();
        let backend = self.backend.clone();
        let me = self.me.clone();
        let wave = self.wave.clone();
        let role = self.role.clone();
        let active_waits = self.active_waits.clone();
        let aborted_wait = self.aborted_wait.clone();
        async move {
            let parked_scope = match &cmd {
                WireCommand::Wait(w) => Some(parked_room_and_since(&me, w)),
                _ => None,
            };
            let mut wait_guard = None;
            if let Some((room, since)) = &parked_scope {
                active_waits.fetch_add(1, Ordering::Relaxed);
                wait_guard = Some(WaitFlightGuard {
                    active: active_waits.clone(),
                    aborted: aborted_wait,
                    completed: false,
                });
                bridge
                    .call_wave_observe(
                        wave.clone(),
                        role.clone(),
                        Observation::Activity {
                            activity: Activity::Parked {
                                room: room.clone(),
                                since: *since,
                            },
                        },
                    )
                    .await;
            }

            let outcome = backend.call_as(me, cmd).await;

            if let Some(mut guard) = wait_guard {
                guard.completed = true;
                drop(guard);
                if active_waits.load(Ordering::Relaxed) == 0
                    && let Ok(rec) = bridge.lookup_worker(&wave, &role).await
                    && matches!(rec.activity, Activity::Parked { .. })
                {
                    bridge
                        .call_wave_observe(
                            wave,
                            role,
                            Observation::Activity {
                                activity: Activity::Working,
                            },
                        )
                        .await;
                }
            }
            outcome
        }
    }

    async fn watch(&self, _interest: Interest, _from: Cursor) -> Result<FrameStream, ApiError> {
        Err(ApiError::Unavailable {
            family: Family::Read,
        })
    }
}

pub type CodeRunner = Arc<
    dyn Fn(WorkerToolClient, serde_json::Value) -> BoxFut<'static, Result<String, String>>
        + Send
        + Sync,
>;

pub struct BbBridge {
    cfg: BridgeConfig,
    data_dir: Option<PathBuf>,
    owner: mpsc::UnboundedSender<OwnerMsg>,
    backend: OnceLock<Arc<dyn ErasedBackend>>,
    code_runner: Option<CodeRunner>,
    project_names_tx: watch::Sender<Arc<BTreeMap<BbProjectId, String>>>,
    project_names_rx: watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>,
}

impl BbBridge {
    pub fn new(cfg: BridgeConfig) -> Arc<Self> {
        Self::with_options(cfg, default_bb_data_dir(), None)
    }

    #[cfg(test)]
    fn with_data_dir(cfg: BridgeConfig, data_dir: Option<PathBuf>) -> Arc<Self> {
        Self::with_options(cfg, data_dir, None)
    }

    pub fn with_code_runner(cfg: BridgeConfig, code_runner: CodeRunner) -> Arc<Self> {
        Self::with_options(cfg, default_bb_data_dir(), Some(code_runner))
    }

    fn with_options(
        cfg: BridgeConfig,
        data_dir: Option<PathBuf>,
        code_runner: Option<CodeRunner>,
    ) -> Arc<Self> {
        let (project_names_tx, project_names_rx) = watch::channel(Arc::new(BTreeMap::new()));
        let bridge = Arc::new(BbBridge {
            cfg,
            data_dir,
            owner: BridgeOwner::spawn(),
            backend: OnceLock::new(),
            code_runner,
            project_names_tx,
            project_names_rx,
        });
        bridge.refresh_local_project_names();
        bridge
    }

    fn refresh_local_project_names(&self) {
        let Some(dir) = self.data_dir.clone() else {
            return;
        };
        let tx = self.project_names_tx.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn_blocking(move || {
                let fresh = load_bb_projects_from_dir(&dir);
                if fresh.is_empty() {
                    return;
                }
                tx.send_if_modified(|cur| {
                    let changed = fresh.iter().any(|(k, v)| cur.get(k) != Some(v));
                    if changed {
                        let mut next = (**cur).clone();
                        next.extend(fresh);
                        *cur = Arc::new(next);
                    }
                    changed
                });
            });
        }
    }

    pub fn observe_wave_event(&self, wave: WaveSlug, event: WaveEvent) {
        let _ = self.owner.send(OwnerMsg::FoldWave { wave, event });
    }

    pub async fn run(self: Arc<Self>, router: Arc<Router>) {
        let sys = LocalClient::new(
            router.clone(),
            Principal::System {
                part: SystemPart::BbBridge,
            },
        );
        let _watcher = spawn_reactor(
            BridgeWaveWatcher {
                bridge: self.clone(),
            },
            sys,
        );
        let (_tx, rx) = watch::channel(false);
        self.run_until(router, rx).await;
    }

    pub async fn run_until<B: BridgeBackend>(
        self: Arc<Self>,
        backend: B,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let _ = self.backend.set(Arc::new(backend));
        let mut backoff = Duration::from_millis(25);
        loop {
            if *shutdown.borrow() {
                break;
            }
            let conn = tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                    continue;
                }
                res = ws::WsConn::connect(&self.cfg.url, &self.cfg.token) => res,
            };
            match conn {
                Ok(ws_conn) => {
                    backoff = Duration::from_millis(25);
                    self.serve_connection(ws_conn, &mut shutdown).await;
                }
                Err(_) => {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() { break; }
                        }
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(2));
                }
            }
        }
    }

    async fn serve_connection(
        self: &Arc<Self>,
        ws_conn: ws::WsConn<tokio::net::TcpStream>,
        shutdown: &mut watch::Receiver<bool>,
    ) {
        let (mut ws_reader, mut ws_writer) = ws_conn.into_split();
        let (down_tx, mut down_rx) = mpsc::unbounded_channel::<protocol::Down>();
        let (conn_tx, conn_rx) = oneshot::channel();
        if self
            .owner
            .send(OwnerMsg::Connected {
                tx: down_tx,
                reply: conn_tx,
            })
            .is_err()
        {
            return;
        }
        let Ok(conn_id) = conn_rx.await else { return };

        // Dedicated reader task keeps frame parsing out of `select!` so partial frames are never dropped.
        let (in_tx, mut in_rx) = mpsc::channel::<ws::WsInbound>(64);
        let reader_task = tokio::spawn(async move {
            while let Ok(Some(item)) = ws_reader.recv().await {
                if in_tx.send(item).await.is_err() {
                    break;
                }
            }
        });

        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                Some(down) = down_rx.recv() => {
                    let Ok(payload) = serde_json::to_string(&down) else { continue };
                    if ws_writer.send_text(&payload).await.is_err() {
                        break;
                    }
                }
                inbound = in_rx.recv() => match inbound {
                    Some(ws::WsInbound::Text(raw)) => {
                        if let Ok(up) = serde_json::from_str::<protocol::Up>(&raw) {
                            self.handle_up(up);
                        }
                    }
                    Some(ws::WsInbound::Ping(payload)) => {
                        if ws_writer.send_pong(&payload).await.is_err() {
                            break;
                        }
                    }
                    Some(ws::WsInbound::Close(reply)) => {
                        let _ = ws_writer.send_close_reply(&reply).await;
                        break;
                    }
                    None => break,
                }
            }
        }

        reader_task.abort();
        let _ = self.owner.send(OwnerMsg::Disconnected { conn_id });
    }

    fn handle_up(self: &Arc<Self>, up: protocol::Up) {
        match up {
            protocol::Up::Hello { .. } => {
                self.refresh_local_project_names();
                let bridge = self.clone();
                tokio::spawn(async move {
                    bridge.reconcile_live_workers().await;
                });
            }
            protocol::Up::Result { req_id, ok, value } => {
                let _ = self.owner.send(OwnerMsg::Result { req_id, ok, value });
            }
            protocol::Up::ThreadEvents {
                thread_id,
                sequence,
            } => {
                if let Ok(thread) = BbThreadId::parse(thread_id) {
                    let _ = self.owner.send(OwnerMsg::ThreadEvents { thread, sequence });
                }
            }
            protocol::Up::ToolCall {
                call_id,
                thread_id,
                meta,
                tool,
                args,
                ..
            } => {
                let bridge = self.clone();
                tokio::spawn(async move {
                    bridge
                        .on_tool_call(call_id, thread_id, meta, tool, args)
                        .await;
                });
            }
            signal @ (protocol::Up::Thread { .. } | protocol::Up::Interaction { .. }) => {
                let bridge = self.clone();
                tokio::spawn(async move {
                    bridge.on_thread_signal(signal).await;
                });
            }
        }
    }

    async fn ensure_connected(&self) -> Result<(), HostError> {
        let (tx, rx) = oneshot::channel();
        self.owner
            .send(OwnerMsg::WaitConnected { reply: tx })
            .map_err(|_| HostError::Unreachable("bridge stopped".into()))?;
        tokio::time::timeout(CONNECT_WAIT_TIMEOUT, rx)
            .await
            .map_err(|_| {
                HostError::Unreachable("timed out waiting for BB bridge connection".into())
            })?
            .map_err(|_| HostError::Unreachable("bridge stopped".into()))
    }

    async fn rpc(&self, down: protocol::Down) -> Result<serde_json::Value, HostError> {
        self.ensure_connected().await?;
        self.rpc_if_connected(down).await
    }

    /// Sends an RPC only if the bridge socket is already connected (0ms fail-fast otherwise).
    async fn rpc_if_connected(&self, down: protocol::Down) -> Result<serde_json::Value, HostError> {
        let (tx, rx) = oneshot::channel();
        self.owner
            .send(OwnerMsg::Rpc { down, started: tx })
            .map_err(|_| HostError::Unreachable("bridge stopped".into()))?;
        let (req_id, res_rx) = rx
            .await
            .map_err(|_| HostError::Unreachable("bridge dropped RPC".into()))??;
        match tokio::time::timeout(RPC_TIMEOUT, res_rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err(HostError::Unreachable("bridge dropped RPC response".into())),
            Err(_) => {
                let _ = self.owner.send(OwnerMsg::CancelRpc { req_id });
                Err(HostError::Unreachable("BB bridge RPC timed out".into()))
            }
        }
    }

    async fn fire(&self, down: protocol::Down) -> Result<(), HostError> {
        self.ensure_connected().await?;
        let (tx, rx) = oneshot::channel();
        self.owner
            .send(OwnerMsg::Fire { down, reply: tx })
            .map_err(|_| HostError::Unreachable("bridge stopped".into()))?;
        rx.await
            .map_err(|_| HostError::Unreachable("bridge dropped message".into()))?
    }

    async fn resolve_worker_identity(
        &self,
        thread: &BbThreadId,
        meta: Option<&protocol::WaveMeta>,
    ) -> Option<(WaveSlug, RoleName, u32)> {
        let (tx, rx) = oneshot::channel();
        let _ = self.owner.send(OwnerMsg::LookupByThread {
            thread: thread.clone(),
            reply: tx,
        });
        if let Ok(Some(found)) = rx.await {
            return Some(found);
        }

        let m = meta?;
        let wave = WaveSlug::parse(&m.wave_id).ok()?;
        let role = RoleName::parse(&m.role).ok()?;

        // Cross-check against authoritative WaveView so untrusted metadata cannot spoof a worker.
        if let Some(backend) = self.backend.get()
            && let Ok(WireReply::WaveView(snap)) = backend
                .call_as(
                    Principal::System {
                        part: SystemPart::BbBridge,
                    },
                    WaveView { wave: wave.clone() }.into_wire(),
                )
                .await
        {
            let valid = snap.workers.iter().any(|(r, _, phase)| {
                r == &role
                    && match phase {
                        WorkerPhase::Live { thread: t, .. }
                        | WorkerPhase::Retiring { thread: t, .. } => t == thread,
                        WorkerPhase::Requested { attempt } => *attempt == m.attempt,
                        _ => false,
                    }
            });
            if !valid {
                return None;
            }
            let _ = self.owner.send(OwnerMsg::BindThread {
                wave: wave.clone(),
                role: role.clone(),
                attempt: m.attempt,
                thread: thread.clone(),
            });
            return Some((wave, role, m.attempt));
        }

        None
    }

    async fn call_wave_observe(&self, wave: WaveSlug, role: RoleName, observation: Observation) {
        self.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: role.clone(),
                observation: observation.clone(),
            },
        );
        if let Some(backend) = self.backend.get() {
            let _ = backend
                .call_as(
                    Principal::System {
                        part: SystemPart::BbBridge,
                    },
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

    async fn observe_runtime_meta_if_present(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        actual_model: Option<ModelId>,
        actual_account: Option<String>,
    ) {
        if actual_model.is_none() && actual_account.is_none() {
            return;
        }
        self.call_wave_observe(
            wave.clone(),
            role.clone(),
            Observation::RuntimeMeta {
                actual_model,
                actual_account,
            },
        )
        .await;
    }

    async fn on_thread_signal(&self, up: protocol::Up) {
        match up {
            protocol::Up::Thread {
                signal,
                thread_id,
                meta,
                error,
                stop_reason,
                model,
                account,
                ..
            } => {
                let Ok(thread) = BbThreadId::parse(thread_id) else {
                    return;
                };
                let Some((wave, role, attempt)) =
                    self.resolve_worker_identity(&thread, meta.as_ref()).await
                else {
                    return;
                };
                let parsed_model = model
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .and_then(|m| ModelId::parse(m).ok());
                let parsed_account = account
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned);
                match signal {
                    protocol::ThreadSignal::Created => {
                        self.call_wave_observe(
                            wave.clone(),
                            role.clone(),
                            Observation::Spawned { attempt, thread },
                        )
                        .await;
                        self.observe_runtime_meta_if_present(
                            &wave,
                            &role,
                            parsed_model,
                            parsed_account,
                        )
                        .await;
                    }
                    protocol::ThreadSignal::Active => {
                        self.call_wave_observe(
                            wave.clone(),
                            role.clone(),
                            Observation::Spawned { attempt, thread },
                        )
                        .await;
                        self.observe_runtime_meta_if_present(
                            &wave,
                            &role,
                            parsed_model,
                            parsed_account,
                        )
                        .await;
                        self.call_wave_observe(
                            wave,
                            role,
                            Observation::Activity {
                                activity: Activity::Working,
                            },
                        )
                        .await;
                    }
                    protocol::ThreadSignal::Idle => {
                        self.call_wave_observe(
                            wave.clone(),
                            role.clone(),
                            Observation::Spawned {
                                attempt,
                                thread: thread.clone(),
                            },
                        )
                        .await;
                        let rows = self.fetch_recent_thread_rows(&thread).await;
                        let (row_model, row_account) = extract_runtime_meta_from_rows(&rows);
                        self.observe_runtime_meta_if_present(
                            &wave,
                            &role,
                            parsed_model.or(row_model),
                            parsed_account.or(row_account),
                        )
                        .await;
                        let refusal_reason = stop_reason
                            .as_deref()
                            .filter(|sr| rex_wave_api::wave::is_refusal_stop_reason(sr))
                            .map(str::to_owned)
                            .or_else(|| refusal_from_latest_turn(&rows));
                        if let Some(reason) = refusal_reason {
                            self.call_wave_observe(
                                wave,
                                role,
                                Observation::Activity {
                                    activity: Activity::Refused { reason },
                                },
                            )
                            .await;
                        } else {
                            self.mirror_completed_turn_from_rows(&wave, &role, &thread, &rows)
                                .await;
                            self.call_wave_observe(
                                wave,
                                role,
                                Observation::Activity {
                                    activity: Activity::Idle,
                                },
                            )
                            .await;
                        }
                    }
                    protocol::ThreadSignal::Failed => {
                        let msg = error.unwrap_or_else(|| "BB thread failed".into());
                        self.call_wave_observe(
                            wave.clone(),
                            role.clone(),
                            Observation::Spawned {
                                attempt,
                                thread: thread.clone(),
                            },
                        )
                        .await;
                        let mut final_model = parsed_model;
                        let mut final_account = parsed_account;
                        let is_direct_refusal = stop_reason
                            .as_deref()
                            .is_some_and(rex_wave_api::wave::is_refusal_stop_reason)
                            || rex_wave_api::wave::is_refusal_error_text(&msg);
                        let row_refusal = if !is_direct_refusal {
                            let rows = self.fetch_recent_thread_rows(&thread).await;
                            let (rm, ra) = extract_runtime_meta_from_rows(&rows);
                            final_model = final_model.or(rm);
                            final_account = final_account.or(ra);
                            refusal_from_latest_turn(&rows)
                        } else {
                            None
                        };
                        self.observe_runtime_meta_if_present(
                            &wave,
                            &role,
                            final_model,
                            final_account,
                        )
                        .await;
                        let activity = if is_direct_refusal {
                            let reason = match stop_reason.as_deref() {
                                Some(sr)
                                    if !msg
                                        .to_ascii_lowercase()
                                        .contains(&sr.to_ascii_lowercase()) =>
                                {
                                    format!("{sr}: {msg}")
                                }
                                _ => msg,
                            };
                            Activity::Refused { reason }
                        } else if let Some(reason) = row_refusal {
                            Activity::Refused { reason }
                        } else {
                            Activity::Errored { error: msg }
                        };
                        self.call_wave_observe(wave, role, Observation::Activity { activity })
                            .await;
                    }
                    protocol::ThreadSignal::Archived => {
                        self.call_wave_observe(wave, role, Observation::Archived)
                            .await;
                    }
                }
            }
            protocol::Up::Interaction {
                thread_id,
                meta,
                interaction,
            } => {
                let Ok(thread) = BbThreadId::parse(thread_id) else {
                    return;
                };
                let Some(pending) = parse_pending_interaction(&interaction) else {
                    return;
                };
                let Some((wave, role, attempt)) =
                    self.resolve_worker_identity(&thread, meta.as_ref()).await
                else {
                    return;
                };
                self.call_wave_observe(
                    wave.clone(),
                    role.clone(),
                    Observation::Spawned { attempt, thread },
                )
                .await;
                self.call_wave_observe(
                    wave,
                    role,
                    Observation::Activity {
                        activity: Activity::Blocked {
                            interaction: pending,
                        },
                    },
                )
                .await;
            }
            _ => {}
        }
    }

    async fn on_tool_call(
        self: &Arc<Self>,
        call_id: String,
        thread_raw: String,
        meta: Option<protocol::WaveMeta>,
        tool: String,
        args: serde_json::Value,
    ) {
        let send_reply = |ok: bool, text: String| async move {
            let _ = self
                .fire(protocol::Down::ToolResult { call_id, ok, text })
                .await;
        };

        let Ok(thread) = BbThreadId::parse(thread_raw) else {
            send_reply(false, "invalid BB thread id".into()).await;
            return;
        };
        let Some((wave, role, _)) = self.resolve_worker_identity(&thread, meta.as_ref()).await
        else {
            send_reply(false, format!("unrecognized wave worker thread {thread}")).await;
            return;
        };
        let Some(backend) = self.backend.get().cloned() else {
            send_reply(false, "rex-wave router unavailable".into()).await;
            return;
        };

        let active_waits = Arc::new(AtomicUsize::new(0));
        let aborted_wait = Arc::new(AtomicBool::new(false));
        let worker_client = WorkerToolClient {
            bridge: self.clone(),
            backend,
            me: Principal::Worker {
                wave: wave.clone(),
                role: role.clone(),
            },
            wave: wave.clone(),
            role: role.clone(),
            active_waits: active_waits.clone(),
            aborted_wait: aborted_wait.clone(),
        };

        if tool == "run_code" {
            let Some(runner) = self.code_runner.as_ref() else {
                send_reply(false, "run_code runner is not configured".into()).await;
                return;
            };
            let res = (runner)(worker_client, args).await;
            if aborted_wait.load(Ordering::Relaxed)
                && active_waits.load(Ordering::Relaxed) == 0
                && let Ok(rec) = self.lookup_worker(&wave, &role).await
                && matches!(rec.activity, Activity::Parked { .. })
            {
                self.call_wave_observe(
                    wave,
                    role,
                    Observation::Activity {
                        activity: Activity::Working,
                    },
                )
                .await;
            }
            match res {
                Ok(text) => send_reply(true, text).await,
                Err(err) => send_reply(false, err).await,
            }
            return;
        }

        let wire_json = serde_json::json!({ "cmd": tool, "args": args });
        let cmd: WireCommand = match serde_json::from_value(wire_json) {
            Ok(c) => c,
            Err(e) => {
                send_reply(false, format!("invalid arguments for {tool}: {e}")).await;
                return;
            }
        };

        match worker_client.call_wire(cmd).await {
            Ok(reply) => {
                let val = serde_json::to_value(&reply).unwrap_or(serde_json::Value::Null);
                let payload = val.get("value").unwrap_or(&val);
                let text = serde_json::to_string(payload).unwrap_or_else(|_| "null".into());
                send_reply(true, text).await;
            }
            Err(err) => {
                send_reply(false, err.to_string()).await;
            }
        }
    }

    async fn authoritative_activity(&self, wave: &WaveSlug, role: &RoleName) -> Option<Activity> {
        let backend = self.backend.get()?;
        let me = Principal::System {
            part: SystemPart::BbBridge,
        };
        let WireReply::WaveView(snap) = backend
            .call_as(me, WireCommand::WaveView(WaveView { wave: wave.clone() }))
            .await
            .ok()?
        else {
            return None;
        };
        snap.workers.into_iter().find_map(|(r, _, phase)| {
            if &r == role
                && let WorkerPhase::Live { activity, .. } = phase
            {
                Some(activity)
            } else {
                None
            }
        })
    }

    async fn fetch_recent_thread_rows(&self, thread: &BbThreadId) -> Vec<serde_json::Value> {
        let Ok(rows_val) = self
            .rpc(protocol::Down::EventsPage {
                req_id: 0,
                thread_id: thread.to_string(),
                after_seq: None,
                before_seq: None,
                limit: 200,
                order: Some(protocol::EventsOrder::Desc),
            })
            .await
        else {
            return Vec::new();
        };
        rows_val.as_array().cloned().unwrap_or_default()
    }

    async fn mirror_completed_turn_from_rows(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        rows: &[serde_json::Value],
    ) {
        let check = inspect_latest_completed_turn(rows);
        if matches!(check, TurnMirrorCheck::Ignore) {
            return;
        }
        let Some(backend) = self.backend.get() else {
            return;
        };
        let sys = Principal::System {
            part: SystemPart::BbBridge,
        };
        let Ok(WireReply::WaveView(snap)) = backend
            .call_as(sys.clone(), WaveView { wave: wave.clone() }.into_wire())
            .await
        else {
            return;
        };
        let room = rex_wave_waves::wave_room(wave, &snap.spec.project);
        let worker_principal = Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        };

        let (wm_tx, wm_rx) = oneshot::channel();
        let _ = self.owner.send(OwnerMsg::MirrorWatermark {
            wave: wave.clone(),
            role: role.clone(),
            reply: wm_tx,
        });
        let watermark = wm_rx.await.unwrap_or(Seq::ORIGIN);

        let stream = StreamId::Room { room: room.clone() };
        let mut cursor = watermark;
        let mut room_head = watermark;
        let completed = match check {
            TurnMirrorCheck::Ignore => return,
            TurnMirrorCheck::PostedViaTool => {
                for _ in 0..MAX_MIRROR_SCAN_PAGES {
                    let Ok(WireReply::Read(page)) = backend
                        .call_as(
                            sys.clone(),
                            Read {
                                stream: stream.clone(),
                                after: cursor,
                                limit: Some(500),
                            }
                            .into_wire(),
                        )
                        .await
                    else {
                        return;
                    };
                    let Some(last) = page.entries.last() else {
                        break;
                    };
                    cursor = last.seq;
                    room_head = last.seq;
                    if page.entries.len() < 500 {
                        break;
                    }
                }
                let _ = self.owner.send(OwnerMsg::AdvanceMirrorWatermark {
                    wave: wave.clone(),
                    role: role.clone(),
                    seq: room_head,
                });
                return;
            }
            TurnMirrorCheck::Unposted(c) => c,
        };

        let raw_idem = format!("wave:{wave}:turn:{role}:{thread}:{}", completed.anchor);
        let idem_str = truncate_body_utf8(&raw_idem, 256);
        let Ok(idem_key) = IdemKey::parse(idem_str) else {
            return;
        };

        let mut already_posted = false;
        let mut reached_end = false;
        for _ in 0..MAX_MIRROR_SCAN_PAGES {
            let Ok(WireReply::Read(page)) = backend
                .call_as(
                    sys.clone(),
                    Read {
                        stream: stream.clone(),
                        after: cursor,
                        limit: Some(500),
                    }
                    .into_wire(),
                )
                .await
            else {
                return;
            };
            let Some(last) = page.entries.last() else {
                reached_end = true;
                break;
            };
            cursor = last.seq;
            room_head = last.seq;
            let count = page.entries.len();
            for env in &page.entries {
                if let Event::Room(RoomEvent::Posted { idem, .. }) = &env.event {
                    if idem.as_ref() == Some(&idem_key) {
                        already_posted = true;
                        break;
                    }
                    if env.by == worker_principal
                        && (completed.turn_start_at == 0 || env.at.0 >= completed.turn_start_at)
                    {
                        already_posted = true;
                        break;
                    }
                }
            }
            if already_posted {
                break;
            }
            if count < 500 {
                reached_end = true;
                break;
            }
        }

        if already_posted || !reached_end {
            let _ = self.owner.send(OwnerMsg::AdvanceMirrorWatermark {
                wave: wave.clone(),
                role: role.clone(),
                seq: room_head,
            });
            return;
        }

        let trimmed = truncate_with_notice(&completed.text, 64 * 1024);
        let Ok(body) = Body::parse(trimmed) else {
            return;
        };
        let post_cmd = RoomPost {
            idem: Some(idem_key),
            ..RoomPost::new(RoomRef::Id { id: room.clone() }, body)
        };
        let first_try = backend
            .call_as(worker_principal.clone(), post_cmd.clone().into_wire())
            .await;
        let post_res = match first_try {
            Err(ApiError::Denied { .. }) => {
                let _ = backend
                    .call_as(
                        sys.clone(),
                        RoomEnsure {
                            room: RoomRef::Id { id: room.clone() },
                            topic: None,
                            origin: Some(RoomOrigin::Wave { wave: wave.clone() }),
                        }
                        .into_wire(),
                    )
                    .await;
                let _ = backend
                    .call_as(
                        sys,
                        RoomJoin {
                            room: RoomRef::Id { id: room },
                            member: Some(worker_principal.clone()),
                            handle: Handle::parse(role.as_str()).ok(),
                        }
                        .into_wire(),
                    )
                    .await;
                backend
                    .call_as(worker_principal, post_cmd.into_wire())
                    .await
            }
            other => other,
        };
        if let Ok(WireReply::RoomPost(posted)) = post_res {
            let _ = self.owner.send(OwnerMsg::AdvanceMirrorWatermark {
                wave: wave.clone(),
                role: role.clone(),
                seq: posted.seq,
            });
        }
    }

    async fn reconcile_live_workers(&self) {
        let (tx, rx) = oneshot::channel();
        if self
            .owner
            .send(OwnerMsg::ListLiveWorkers { reply: tx })
            .is_err()
        {
            return;
        }
        let Ok(workers) = rx.await else { return };
        for (wave, role, thread, tracked_activity, mut cursor_seq) in workers {
            let had_prev_cursor = cursor_seq.is_some();
            let mut collected_rows: Vec<serde_json::Value> = Vec::new();
            let mut thread_gone: Option<Observation> = None;
            let mut saw_new_turn_start = false;
            if cursor_seq.is_none() {
                let mut before_seq: Option<u64> = None;
                let mut pages = 0usize;
                'desc: loop {
                    match self
                        .rpc(protocol::Down::EventsPage {
                            req_id: 0,
                            thread_id: thread.to_string(),
                            after_seq: None,
                            before_seq,
                            limit: 200,
                            order: Some(protocol::EventsOrder::Desc),
                        })
                        .await
                    {
                        Ok(rows_val) => {
                            let Some(rows) = rows_val.as_array() else {
                                break;
                            };
                            if rows.is_empty() {
                                break;
                            }
                            pages += 1;
                            let count = rows.len();
                            let prev_before = before_seq;
                            let mut reached_turn_start = false;
                            for row in rows {
                                if let Some(s) = row.get("seq").and_then(|v| v.as_u64()) {
                                    cursor_seq = Some(cursor_seq.map_or(s, |cur| cur.max(s)));
                                    before_seq = Some(before_seq.map_or(s, |cur| cur.min(s)));
                                }
                                collected_rows.push(row.clone());
                                if matches!(
                                    row.get("type").and_then(|v| v.as_str()),
                                    Some("turn/started" | "client/turn/start")
                                ) {
                                    reached_turn_start = true;
                                }
                            }
                            if reached_turn_start
                                || count < 200
                                || before_seq == prev_before
                                || pages >= 4
                            {
                                break 'desc;
                            }
                        }
                        Err(HostError::Rejected(msg)) => {
                            thread_gone = classify_missing_or_archived_thread(&msg);
                            break;
                        }
                        Err(HostError::Unreachable(_)) => break,
                    }
                }
            } else {
                loop {
                    match self
                        .rpc(protocol::Down::EventsPage {
                            req_id: 0,
                            thread_id: thread.to_string(),
                            after_seq: cursor_seq,
                            before_seq: None,
                            limit: 200,
                            order: None,
                        })
                        .await
                    {
                        Ok(rows_val) => {
                            let Some(rows) = rows_val.as_array() else {
                                break;
                            };
                            if rows.is_empty() {
                                break;
                            }
                            let count = rows.len();
                            let prev = cursor_seq;
                            for row in rows {
                                if let Some(s) = row.get("seq").and_then(|v| v.as_u64()) {
                                    cursor_seq = Some(cursor_seq.map_or(s, |cur| cur.max(s)));
                                }
                                if matches!(
                                    row.get("type").and_then(|v| v.as_str()),
                                    Some("turn/started" | "client/turn/start")
                                ) {
                                    saw_new_turn_start = true;
                                }
                                collected_rows.push(row.clone());
                            }
                            if count < 200 || cursor_seq == prev {
                                break;
                            }
                        }
                        Err(HostError::Rejected(msg)) => {
                            thread_gone = classify_missing_or_archived_thread(&msg);
                            break;
                        }
                        Err(HostError::Unreachable(_)) => break,
                    }
                }
            }
            if let Some(seq) = cursor_seq {
                let _ = self.owner.send(OwnerMsg::AdvanceSeq {
                    thread: thread.clone(),
                    seq,
                });
            }
            let current_activity = self
                .authoritative_activity(&wave, &role)
                .await
                .unwrap_or(tracked_activity);
            if let Some(obs) = thread_gone {
                let already_terminal_failure = matches!(
                    current_activity,
                    Activity::Errored { .. } | Activity::Refused { .. }
                );
                if !already_terminal_failure || matches!(obs, Observation::Archived) {
                    self.call_wave_observe(wave, role, obs).await;
                }
                continue;
            }
            let (row_model, row_account) = extract_runtime_meta_from_rows(&collected_rows);
            self.observe_runtime_meta_if_present(&wave, &role, row_model, row_account)
                .await;
            let mut derived_activity = derive_latest_turn_activity(&collected_rows);
            if matches!(
                current_activity,
                Activity::Blocked { .. } | Activity::Parked { .. }
            ) && matches!(derived_activity, Some(Activity::Working))
            {
                derived_activity = None;
            }
            if matches!(
                current_activity,
                Activity::Refused { .. } | Activity::Errored { .. }
            ) && matches!(derived_activity, Some(Activity::Idle))
                && !(had_prev_cursor && saw_new_turn_start)
            {
                derived_activity = None;
            }
            if let Some(activity) = derived_activity {
                if matches!(activity, Activity::Idle) {
                    self.mirror_completed_turn_from_rows(&wave, &role, &thread, &collected_rows)
                        .await;
                }
                self.call_wave_observe(wave, role, Observation::Activity { activity })
                    .await;
            }
        }
    }

    async fn seed_tail_seq(&self, thread: &BbThreadId) -> Option<u64> {
        let rows_val = self
            .rpc(protocol::Down::EventsPage {
                req_id: 0,
                thread_id: thread.to_string(),
                after_seq: None,
                before_seq: None,
                limit: 1,
                order: Some(protocol::EventsOrder::Desc),
            })
            .await
            .ok()?;
        let seq = rows_val
            .as_array()?
            .first()
            .and_then(|r| r.get("seq"))
            .and_then(|v| v.as_u64());
        if let Some(seq) = seq {
            let _ = self.owner.send(OwnerMsg::AdvanceSeq {
                thread: thread.clone(),
                seq,
            });
        }
        seq
    }

    async fn lookup_worker(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
    ) -> Result<TrackedWorker, ApiError> {
        let (tx, rx) = oneshot::channel();
        let _ = self.owner.send(OwnerMsg::LookupByRole {
            wave: wave.clone(),
            role: role.clone(),
            reply: tx,
        });
        if let Ok(Some(rec)) = rx.await
            && rec.thread.is_some()
        {
            return Ok(rec);
        }

        if let Some(backend) = self.backend.get()
            && let WireReply::WaveView(snap) = backend
                .call_as(
                    Principal::System {
                        part: SystemPart::BbBridge,
                    },
                    WaveView { wave: wave.clone() }.into_wire(),
                )
                .await?
        {
            for (r, _, phase) in snap.workers {
                if &r == role {
                    return match phase {
                        WorkerPhase::Live { thread, activity } => {
                            let pending_interaction = match &activity {
                                Activity::Blocked { interaction } => Some(interaction.clone()),
                                _ => None,
                            };
                            let _ = self.owner.send(OwnerMsg::BindThread {
                                wave: wave.clone(),
                                role: role.clone(),
                                attempt: 1,
                                thread: thread.clone(),
                            });
                            Ok(TrackedWorker {
                                attempt: 1,
                                thread: Some(thread),
                                live: true,
                                activity,
                                pending_interaction,
                                last_seq: None,
                                mirror_room_watermark: Seq::ORIGIN,
                            })
                        }
                        WorkerPhase::Retiring { thread, .. }
                        | WorkerPhase::Retired {
                            thread: Some(thread),
                            ..
                        } => Ok(TrackedWorker {
                            attempt: 1,
                            thread: Some(thread),
                            live: false,
                            activity: Activity::Idle,
                            pending_interaction: None,
                            last_seq: None,
                            mirror_room_watermark: Seq::ORIGIN,
                        }),
                        _ => Err(ApiError::NotFound {
                            what: format!("live BB thread for @{wave}/{role}"),
                        }),
                    };
                }
            }
        }

        Err(ApiError::NotFound {
            what: format!("worker @{wave}/{role}"),
        })
    }

    async fn open_live_channel(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
    ) -> Result<mpsc::Receiver<Result<Frame, ApiError>>, ApiError> {
        let rec = self.lookup_worker(wave, role).await?;
        let thread = rec.thread.ok_or_else(|| ApiError::NotFound {
            what: format!("thread for @{wave}/{role}"),
        })?;
        let initial_seq = match rec.last_seq {
            Some(seq) => Some(seq),
            None => self.seed_tail_seq(&thread).await,
        };

        let (tx_open, rx_open) = oneshot::channel();
        self.owner
            .send(OwnerMsg::WatchOpen {
                thread: thread.clone(),
                after_seq: initial_seq,
                reply: tx_open,
            })
            .map_err(|_| ApiError::Internal("BB bridge stopped".into()))?;
        let (lease_id, mut seq_rx) = rx_open
            .await
            .map_err(|_| ApiError::Internal("BB bridge dropped watch".into()))?;
        let lease = WatchLeaseGuard {
            owner: self.owner.clone(),
            thread: thread.clone(),
            lease_id,
        };

        let (frame_tx, frame_rx) = mpsc::channel(128);
        let owner = self.owner.clone();
        let wave = wave.clone();
        let role = role.clone();

        tokio::spawn(async move {
            let _lease = lease;
            let mut cursor_seq = initial_seq;
            loop {
                tokio::select! {
                    biased;
                    () = frame_tx.closed() => break,
                    maybe_seq = seq_rx.recv() => {
                        let Some(target_seq) = maybe_seq else { break };
                        while cursor_seq.is_none_or(|c| c < target_seq) {
                            let (rpc_tx, rpc_rx) = oneshot::channel();
                            if owner.send(OwnerMsg::Rpc {
                                down: protocol::Down::EventsPage {
                                    req_id: 0,
                                    thread_id: thread.to_string(),
                                    after_seq: cursor_seq,
                                    before_seq: None,
                                    limit: 200,
                                    order: None,
                                },
                                started: rpc_tx,
                            }).is_err() {
                                return;
                            }
                            let Ok(Ok((req_id, res_rx))) = rpc_rx.await else { break };
                            let rows_val = match tokio::time::timeout(RPC_TIMEOUT, res_rx).await {
                                Ok(Ok(Ok(v))) => v,
                                Ok(_) => break,
                                Err(_) => {
                                    let _ = owner.send(OwnerMsg::CancelRpc { req_id });
                                    break;
                                }
                            };
                            let Some(rows) = rows_val.as_array() else { break };
                            if rows.is_empty() {
                                cursor_seq = Some(target_seq);
                                break;
                            }
                            for row in rows {
                                if let Some(ev) = normalize_agent_event(row) {
                                    cursor_seq = Some(cursor_seq.map_or(ev.seq.0, |prev| prev.max(ev.seq.0)));
                                    let _ = owner.send(OwnerMsg::AdvanceSeq { thread: thread.clone(), seq: ev.seq.0 });
                                    if frame_tx.send(Ok(Frame::Agent {
                                        wave: wave.clone(),
                                        role: role.clone(),
                                        event: ev,
                                    })).await.is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(frame_rx)
    }

    async fn spawn_worker_thread(&self, req: SpawnRequest) -> Result<BbThreadId, HostError> {
        let meta = protocol::WaveMeta {
            wave_id: req.wave.to_string(),
            role: req.role.to_string(),
            attempt: req.attempt,
            room_id: serde_json::to_string(&req.room).unwrap_or_default(),
            run_stamp: (!req.run_stamp.is_empty()).then(|| req.run_stamp.clone()),
            output_dir: req.output_dir.as_ref().map(|p| p.display().to_string()),
            writes: req.writes.iter().map(|p| p.display().to_string()).collect(),
            account: req.agent.account.clone(),
        };

        let existing = self
            .rpc(protocol::Down::FindByMeta { req_id: 0, meta })
            .await?;
        if let Some(id_str) = existing.get("thread_id").and_then(|v| v.as_str()) {
            let thread =
                BbThreadId::parse(id_str).map_err(|e| HostError::Rejected(e.to_string()))?;
            let _ = self.owner.send(OwnerMsg::BindThread {
                wave: req.wave,
                role: req.role,
                attempt: req.attempt,
                thread: thread.clone(),
            });
            return Ok(thread);
        }

        let created = self
            .rpc(protocol::Down::Spawn {
                req_id: 0,
                args: spawn_args(&req),
            })
            .await?;
        let id_str = created
            .get("thread_id")
            .or_else(|| created.get("id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                HostError::Rejected(format!("missing thread_id in spawn reply: {created}"))
            })?;
        let thread = BbThreadId::parse(id_str).map_err(|e| HostError::Rejected(e.to_string()))?;
        let actual_model = created
            .get("model")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .and_then(|m| ModelId::parse(m).ok());
        let actual_account = created
            .get("account")
            .or_else(|| created.get("accountEmail"))
            .or_else(|| created.get("profile"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        let _ = self.owner.send(OwnerMsg::BindThread {
            wave: req.wave.clone(),
            role: req.role.clone(),
            attempt: req.attempt,
            thread: thread.clone(),
        });
        self.observe_runtime_meta_if_present(&req.wave, &req.role, actual_model, actual_account)
            .await;
        Ok(thread)
    }

    async fn check_thread_live(
        &self,
        thread: &BbThreadId,
    ) -> Result<Option<Observation>, HostError> {
        match self
            .rpc_if_connected(protocol::Down::EventsPage {
                req_id: 0,
                thread_id: thread.to_string(),
                after_seq: None,
                before_seq: None,
                limit: 20,
                order: Some(protocol::EventsOrder::Desc),
            })
            .await
        {
            Ok(rows_val) => {
                if let Some(rows) = rows_val.as_array()
                    && let Some(act @ Activity::Refused { .. }) = derive_latest_turn_activity(rows)
                {
                    return Ok(Some(Observation::Activity { activity: act }));
                }
                Ok(None)
            }
            Err(HostError::Rejected(msg)) => Ok(classify_missing_or_archived_thread(&msg)),
            Err(HostError::Unreachable(msg)) => Err(HostError::Unreachable(msg)),
        }
    }
}

fn parked_room_and_since(
    worker: &Principal,
    w: &rex_wave_api::read::Wait,
) -> (Option<RoomId>, Seq) {
    match &w.interest {
        Interest::Room { room } | Interest::RoomFiltered { room, .. } => match room.resolve(worker)
        {
            Ok(id) => {
                let since = w.after.at(&StreamId::Room { room: id.clone() });
                (Some(id), since)
            }
            Err(_) => (None, Seq::ORIGIN),
        },
        Interest::Streams { streams } if streams.len() == 1 => match &streams[0] {
            StreamId::Room { room } => (Some(room.clone()), w.after.at(&streams[0])),
            _ => (None, Seq::ORIGIN),
        },
        _ => (None, Seq::ORIGIN),
    }
}

#[derive(Clone)]
pub struct BbHost(pub Arc<BbBridge>);

impl AgentHost for BbHost {
    fn spawn(
        &self,
        req: SpawnRequest,
    ) -> impl Future<Output = Result<BbThreadId, HostError>> + Send {
        let bridge = self.0.clone();
        async move { bridge.spawn_worker_thread(req).await }
    }

    fn tell(
        &self,
        thread: &BbThreadId,
        body: &Body,
        mode: SteerDeliveryMode,
    ) -> impl Future<Output = Result<(), HostError>> + Send {
        let bridge = self.0.clone();
        let down = protocol::Down::Send {
            req_id: 0,
            thread_id: thread.to_string(),
            mode: send_mode(mode).into(),
            text: body.as_str().into(),
        };
        async move { bridge.rpc(down).await.map(|_| ()) }
    }

    fn resolve(
        &self,
        thread: &BbThreadId,
        interaction: &BbInteractionId,
        resolution: &InteractionResolution,
    ) -> impl Future<Output = Result<(), HostError>> + Send {
        let bridge = self.0.clone();
        let down = protocol::Down::Resolve {
            req_id: 0,
            thread_id: thread.to_string(),
            interaction_id: interaction.to_string(),
            resolution: resolution_json(resolution),
        };
        async move { bridge.rpc(down).await.map(|_| ()) }
    }

    fn retire(&self, thread: &BbThreadId) -> impl Future<Output = Result<(), HostError>> + Send {
        let bridge = self.0.clone();
        let down = protocol::Down::Retire {
            req_id: 0,
            thread_id: thread.to_string(),
        };
        async move { bridge.rpc(down).await.map(|_| ()) }
    }

    fn check_live(
        &self,
        thread: &BbThreadId,
    ) -> impl Future<Output = Result<Option<Observation>, HostError>> + Send {
        let bridge = self.0.clone();
        let thread = thread.clone();
        async move { bridge.check_thread_live(&thread).await }
    }
}

impl AgentFeed for BbBridge {
    fn page(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        after: Option<BbSeq>,
        limit: u32,
    ) -> BoxFut<'_, Result<ThreadPageView, ApiError>> {
        let wave = wave.clone();
        let role = role.clone();
        Box::pin(async move {
            let rec = self.lookup_worker(&wave, &role).await?;
            let thread = rec.thread.ok_or_else(|| ApiError::NotFound {
                what: format!("thread for @{wave}/{role}"),
            })?;

            let rows_val = self
                .rpc(protocol::Down::EventsPage {
                    req_id: 0,
                    thread_id: thread.to_string(),
                    after_seq: after.map(|s| s.0),
                    before_seq: None,
                    limit,
                    order: None,
                })
                .await
                .map_err(|e| ApiError::Transport(e.to_string()))?;

            let ctx_val = self
                .rpc(protocol::Down::ThreadContext {
                    req_id: 0,
                    thread_id: thread.to_string(),
                })
                .await
                .map_err(|e| ApiError::Transport(e.to_string()))?;

            let mut events = Vec::new();
            if let Some(arr) = rows_val.as_array() {
                for row in arr {
                    if let Some(ev) = normalize_agent_event(row) {
                        events.push(ev);
                    }
                }
            }
            if let Some(last) = events.last() {
                let _ = self.owner.send(OwnerMsg::AdvanceSeq {
                    thread: thread.clone(),
                    seq: last.seq.0,
                });
            }
            let next = events.last().map(|e| e.seq);
            let usage = parse_context_usage(&ctx_val);

            Ok(ThreadPageView {
                thread,
                activity: rec.activity,
                events,
                usage,
                pending: rec.pending_interaction,
                next,
            })
        })
    }

    fn live(&self, wave: &WaveSlug, role: &RoleName) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        let wave = wave.clone();
        let role = role.clone();
        Box::pin(async move {
            let mut rx = self.open_live_channel(&wave, &role).await?;
            let (tx, stream) = frame_channel(128);
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        () = tx.closed() => break,
                        item = rx.recv() => match item {
                            Some(frame) => {
                                if tx.send(frame).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            });
            Ok(stream)
        })
    }

    fn project_names(&self) -> Option<watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>> {
        Some(self.project_names_rx.clone())
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use rex_wave_api::board::{BoardEnsure, BoardEvent, BoardId, BoardInfo};
    use rex_wave_api::read::{Page, ThreadPage, Wait};
    use rex_wave_api::room::{
        Mentions, Posted, RoomEvent, RoomInfo, RoomOrigin, RoomPost, RoomRef,
    };
    use rex_wave_api::wave::{
        AgentSpec, FileSystemGrant, GrantedPermissions, NetworkGrant, UserAnswer, WaveFinish,
        WaveOutcome, WaveResolve, WaveSpec, WaveStart, WorkerSpec,
    };
    use rex_wave_runtime::{DecideCtx, Decider, EntryMeta, authorize};
    use rex_wave_waves::{WaveConductor, WaveState, Waves};
    use std::collections::{BTreeMap, BTreeSet};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::Mutex;

    #[test]
    fn rfc6455_accept_vector_and_protocol_fixtures_round_trip() {
        assert_eq!(
            ws::ws_accept("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );

        let raw = include_str!("../../../bb-plugin/protocol.fixtures.json");
        let doc: serde_json::Value = serde_json::from_str(raw).unwrap();
        let ups: Vec<protocol::Up> = serde_json::from_value(doc["up"].clone()).unwrap();
        let downs: Vec<protocol::Down> = serde_json::from_value(doc["down"].clone()).unwrap();
        assert_eq!(ups.len(), 6);
        assert_eq!(downs.len(), 11);
        assert_eq!(serde_json::to_value(&ups).unwrap(), doc["up"]);
        assert_eq!(serde_json::to_value(&downs).unwrap(), doc["down"]);
    }

    #[tokio::test]
    async fn ws_auth_enforces_x_bb_plugin_token_and_concurrent_duplex_frames_never_desync() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let token = "tok_x_bb_check";

        let server = tokio::spawn(async move {
            let (bad_tcp, _) = listener.accept().await.unwrap();
            assert!(ws::WsConn::accept(bad_tcp, token).await.is_err());

            let (good_tcp, _) = listener.accept().await.unwrap();
            let conn = ws::WsConn::accept(good_tcp, token).await.unwrap();
            let (mut reader, mut writer) = conn.into_split();
            let big = "S".repeat(32 * 1024);
            let writer_task = tokio::spawn(async move {
                for i in 0..8 {
                    writer.send_text(&format!("srv:{i}:{big}")).await.unwrap();
                }
            });
            let mut got = 0usize;
            while got < 8 {
                if let Some(ws::WsInbound::Text(txt)) = reader.recv().await.unwrap() {
                    assert!(txt.starts_with("cli:") && txt.len() > 32 * 1024);
                    got += 1;
                }
            }
            writer_task.await.unwrap();

            let (frag_tcp, _) = listener.accept().await.unwrap();
            let frag_conn = ws::WsConn::accept(frag_tcp, token).await.unwrap();
            let (mut frag_reader, _frag_writer) = frag_conn.into_split();
            assert!(matches!(
                frag_reader.recv().await.unwrap(),
                Some(ws::WsInbound::Ping(ref p)) if p == b"hb"
            ));
            assert!(matches!(
                frag_reader.recv().await.unwrap(),
                Some(ws::WsInbound::Text(ref t)) if t == "hello world"
            ));
        });

        let mut raw_bad = TcpStream::connect(addr).await.unwrap();
        raw_bad
            .write_all(
                b"GET /bridge HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
                  Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\
                  Authorization: Bearer tok_x_bb_check\r\n\r\n",
            )
            .await
            .unwrap();
        let mut resp = vec![0u8; 64];
        let n = raw_bad.read(&mut resp).await.unwrap();
        assert!(
            std::str::from_utf8(&resp[..n])
                .unwrap()
                .starts_with("HTTP/1.1 401")
        );

        let good = ws::WsConn::connect(&format!("ws://{addr}/bridge"), token)
            .await
            .unwrap();
        let (mut reader, mut writer) = good.into_split();
        let big = "C".repeat(32 * 1024);
        let send_task = tokio::spawn(async move {
            for i in 0..8 {
                writer.send_text(&format!("cli:{i}:{big}")).await.unwrap();
            }
        });
        let mut recv_count = 0usize;
        while recv_count < 8 {
            if let Some(ws::WsInbound::Text(txt)) = reader.recv().await.unwrap() {
                assert!(txt.starts_with("srv:") && txt.len() > 32 * 1024);
                recv_count += 1;
            }
        }
        send_task.await.unwrap();

        let mut raw_frag = TcpStream::connect(addr).await.unwrap();
        raw_frag
            .write_all(
                b"GET /bridge HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
                  Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\
                  x-bb-plugin-token: tok_x_bb_check\r\n\r\n",
            )
            .await
            .unwrap();
        let mut hs_buf = vec![0u8; 256];
        let hs_n = raw_frag.read(&mut hs_buf).await.unwrap();
        assert!(
            std::str::from_utf8(&hs_buf[..hs_n])
                .unwrap()
                .starts_with("HTTP/1.1 101")
        );
        // Fragment 1 (`fin=0, opcode=1`, unmasked for loopback reader): "hello "
        // Interleaved Ping (`fin=1, opcode=9`): "hb"
        // Continuation (`fin=1, opcode=0`): "world"
        let frames: &[u8] = &[
            0x01, 0x06, b'h', b'e', b'l', b'l', b'o', b' ', 0x89, 0x02, b'h', b'b', 0x80, 0x05,
            b'w', b'o', b'r', b'l', b'd',
        ];
        raw_frag.write_all(frames).await.unwrap();

        server.await.unwrap();
    }

    #[test]
    fn spawn_args_and_resolutions_match_bb_sdk_0_6_132_shapes() {
        let room = rex_wave_waves::wave_room(
            &WaveSlug::parse("w1").unwrap(),
            &BbProjectId::parse("proj_1").unwrap(),
        );
        let mk_req = |provider: &str,
                      model: Option<&str>,
                      permission: PermissionMode,
                      workspace: Workspace| SpawnRequest {
            wave: WaveSlug::parse("w1").unwrap(),
            role: RoleName::parse("worker").unwrap(),
            attempt: 2,
            project: BbProjectId::parse("proj_1").unwrap(),
            agent: AgentSpec {
                provider: ProviderId::parse(provider).unwrap(),
                model: model.map(|m| ModelId::parse(m).unwrap()),
                account: None,
                reasoning: Some("xhigh".into()),
                service_tier: Some("fast".into()),
                permission,
                workspace,
            },
            room: room.clone(),
            prompt: Body::parse("ship it").unwrap(),
            title: "[w1] @worker".into(),
            run_stamp: "w1:worker:attempt-2:seq-1".into(),
            output_dir: None,
            writes: Vec::new(),
            return_schema: None,
        };

        let wt = spawn_args(&mk_req(
            "codex",
            Some("gpt-6.1-sol"),
            PermissionMode::AcceptEdits,
            Workspace::ManagedWorktree {
                base: BaseBranch::Named {
                    name: "feat/x".into(),
                },
            },
        ));
        assert_eq!(wt["environment"]["type"], "host");
        assert_eq!(wt["environment"]["workspace"]["type"], "managed-worktree");
        assert_eq!(
            wt["environment"]["workspace"]["baseBranch"],
            serde_json::json!({"kind": "named", "name": "feat/x"})
        );
        assert_eq!(wt["visibility"], "hidden");
        assert!(wt.get("input").is_none());
        assert_eq!(wt["pluginMetadata"]["rexWave"]["waveId"], "w1");
        assert_eq!(wt["pluginMetadata"]["rexWave"]["attempt"], 2);

        let wt_default = spawn_args(&mk_req(
            "codex",
            Some("gpt-6.1-sol"),
            PermissionMode::AcceptEdits,
            Workspace::ManagedWorktree {
                base: BaseBranch::Default,
            },
        ));
        assert_eq!(
            wt_default["environment"],
            serde_json::json!({
                "type": "host",
                "workspace": {
                    "type": "managed-worktree",
                    "baseBranch": { "kind": "default" },
                },
            })
        );

        let existing = spawn_args(&mk_req(
            "acp-opencode",
            Some("opencode/gemini-4-carbon"),
            PermissionMode::Auto,
            Workspace::ExistingPath {
                path: "/tmp/wt".into(),
            },
        ));
        assert_eq!(
            existing["environment"],
            serde_json::json!({
                "type": "host",
                "workspace": { "type": "unmanaged", "path": "/tmp/wt" },
            })
        );

        let checkout = spawn_args(&mk_req(
            "pi",
            None,
            PermissionMode::Full,
            Workspace::ProjectDefault,
        ));
        assert_eq!(
            checkout["environment"],
            serde_json::json!({ "type": "project-default" })
        );
        assert!(checkout.get("model").is_none());

        let allow = resolution_json(&InteractionResolution::AllowForSession {
            granted_permissions: Some(GrantedPermissions {
                file_system: Some(FileSystemGrant {
                    read: vec!["/a".into()],
                    write: vec!["/b".into()],
                }),
                network: Some(NetworkGrant {
                    enabled: Some(true),
                }),
            }),
        });
        assert_eq!(
            allow,
            serde_json::json!({
                "decision": "allow_for_session",
                "grantedPermissions": {
                    "fileSystem": { "read": ["/a"], "write": ["/b"] },
                    "network": { "enabled": true },
                }
            })
        );

        let mut answers = BTreeMap::new();
        answers.insert(
            "q1".into(),
            UserAnswer {
                selected: vec!["opt-a".into()],
                free_text: None,
            },
        );
        let ua = resolution_json(&InteractionResolution::UserAnswer { answers });
        assert_eq!(
            ua,
            serde_json::json!({"kind": "user_answer", "answers": {"q1": {"selected": ["opt-a"]}}})
        );
        assert_eq!(
            resolution_json(&InteractionResolution::RequestAnswer {
                value: serde_json::json!({"ok": 1})
            }),
            serde_json::json!({"kind": "request_answer", "value": {"ok": 1}})
        );
        assert_eq!(
            resolution_json(&InteractionResolution::Deny),
            serde_json::json!({"decision": "deny"})
        );
    }

    struct InMemState {
        waves: BTreeMap<WaveSlug, WaveState>,
        room_members: BTreeMap<RoomId, BTreeMap<Handle, Principal>>,
        room_posts: BTreeMap<(RoomId, IdemKey), Seq>,
        boards: BTreeSet<BoardId>,
        entries: BTreeMap<StreamId, Vec<Envelope>>,
        waiters: Vec<mpsc::UnboundedSender<Envelope>>,
    }

    #[derive(Clone)]
    struct LoopbackHarness {
        inner: Arc<Mutex<InMemState>>,
        frame_tx: mpsc::UnboundedSender<Envelope>,
        bridge: Arc<BbBridge>,
    }

    impl LoopbackHarness {
        fn new(bridge: Arc<BbBridge>) -> (Self, mpsc::UnboundedReceiver<Envelope>) {
            let (frame_tx, frame_rx) = mpsc::unbounded_channel();
            let inner = Arc::new(Mutex::new(InMemState {
                waves: BTreeMap::new(),
                room_members: BTreeMap::new(),
                room_posts: BTreeMap::new(),
                boards: BTreeSet::new(),
                entries: BTreeMap::new(),
                waiters: Vec::new(),
            }));
            (
                LoopbackHarness {
                    inner,
                    frame_tx,
                    bridge,
                },
                frame_rx,
            )
        }
    }

    #[derive(Clone)]
    struct HarnessClient {
        me: Principal,
        harness: LoopbackHarness,
    }

    impl BridgeBackend for LoopbackHarness {
        type C = HarnessClient;
        fn client(&self, me: Principal) -> HarnessClient {
            HarnessClient {
                me,
                harness: self.clone(),
            }
        }
    }

    impl InMemState {
        fn commit(
            &mut self,
            stream: StreamId,
            by: Principal,
            event: Event,
            tx: &mpsc::UnboundedSender<Envelope>,
        ) -> Envelope {
            let log = self.entries.entry(stream.clone()).or_default();
            let seq = Seq(log.len() as u64 + 1);
            let env = Envelope {
                stream,
                seq,
                at: UnixMs(1_000 + seq.0),
                by,
                event,
            };
            log.push(env.clone());
            let _ = tx.send(env.clone());
            self.waiters.retain(|w| w.send(env.clone()).is_ok());
            env
        }
    }

    impl Client for HarnessClient {
        fn me(&self) -> &Principal {
            &self.me
        }

        fn call_wire(
            &self,
            cmd: WireCommand,
        ) -> impl Future<Output = Result<WireReply, ApiError>> + Send {
            let me = self.me.clone();
            let harness = self.harness.clone();
            async move {
                authorize(cmd.meta().audience, &me)?;
                match cmd.route() {
                    Routed::Wave(wc) => {
                        let mut g = harness.inner.lock().await;
                        let ctx = DecideCtx::for_test(me.clone(), UnixMs(1_000));
                        let stream = Waves::address(&wc, &ctx)?;
                        let StreamId::Wave { wave } = &stream else {
                            unreachable!()
                        };
                        let events = {
                            let st = g.waves.entry(wave.clone()).or_default();
                            Waves::decide(st, &wc, &ctx)?
                        };
                        let mut committed = Vec::new();
                        for ev in events {
                            harness.bridge.observe_wave_event(wave.clone(), ev.clone());
                            let env = g.commit(
                                stream.clone(),
                                me.clone(),
                                Event::Wave(ev.clone()),
                                &harness.frame_tx,
                            );
                            let meta = EntryMeta {
                                seq: env.seq,
                                at: env.at,
                                by: me.clone(),
                            };
                            Waves::evolve(g.waves.get_mut(wave).unwrap(), &ev, &meta);
                            committed.push(env);
                        }
                        let st = g.waves.get(wave).unwrap();
                        Ok(Waves::reply(st, wc, &committed))
                    }
                    Routed::Room(rc) => {
                        let mut g = harness.inner.lock().await;
                        match rc {
                            RoomCommand::RoomEnsure(c) => {
                                let id = c.room.resolve(&me)?;
                                let origin = c.origin.unwrap_or(RoomOrigin::Manual);
                                let stream = StreamId::Room { room: id.clone() };
                                if !g.entries.contains_key(&stream) {
                                    g.commit(
                                        stream.clone(),
                                        me,
                                        Event::Room(RoomEvent::Opened {
                                            id: id.clone(),
                                            origin: origin.clone(),
                                            topic: c.topic.clone(),
                                        }),
                                        &harness.frame_tx,
                                    );
                                }
                                let head = Seq(g.entries.get(&stream).map_or(0, Vec::len) as u64);
                                let members = g
                                    .room_members
                                    .get(&id)
                                    .cloned()
                                    .unwrap_or_default()
                                    .into_iter()
                                    .collect();
                                Ok(WireReply::RoomEnsure(RoomInfo {
                                    id,
                                    origin,
                                    topic: c.topic,
                                    head,
                                    members,
                                }))
                            }
                            RoomCommand::RoomJoin(c) => {
                                let id = c.room.resolve(&me)?;
                                let member = c.member.unwrap_or_else(|| me.clone());
                                let handle = match c.handle {
                                    Some(h) => h,
                                    None => match &member {
                                        Principal::Operator { seat } | Principal::Seat { seat } => {
                                            Handle::parse(seat.as_str())?
                                        }
                                        Principal::Worker { role, .. } => {
                                            Handle::parse(role.as_str())?
                                        }
                                        Principal::System { part } => {
                                            Handle::parse(format!("{part:?}").to_lowercase())?
                                        }
                                    },
                                };
                                g.room_members
                                    .entry(id.clone())
                                    .or_default()
                                    .insert(handle.clone(), member.clone());
                                g.commit(
                                    StreamId::Room { room: id },
                                    me,
                                    Event::Room(RoomEvent::MemberJoined { member, handle }),
                                    &harness.frame_tx,
                                );
                                Ok(WireReply::RoomJoin(Ack))
                            }
                            RoomCommand::RoomPost(c) => {
                                let id = c.room.resolve(&me)?;
                                if let Some(idem) = &c.idem
                                    && let Some(&seq) =
                                        g.room_posts.get(&(id.clone(), idem.clone()))
                                {
                                    return Ok(WireReply::RoomPost(Posted {
                                        room: id,
                                        seq,
                                        mentions: Mentions::default(),
                                        re: None,
                                        org_id: None,
                                        delivery_notice: None,
                                    }));
                                }
                                let map = g.room_members.get(&id).cloned().unwrap_or_default();
                                let mut principals = Vec::new();
                                let mut all = false;
                                for tok in c.body.as_str().split_whitespace() {
                                    if let Some(raw) = tok.strip_prefix('@') {
                                        let clean = raw.trim_end_matches(|ch: char| {
                                            !ch.is_ascii_alphanumeric() && ch != '-' && ch != '_'
                                        });
                                        if clean == "all" {
                                            all = true;
                                        } else if let Ok(h) = Handle::parse(clean)
                                            && let Some(p) = map.get(&h)
                                            && !principals.contains(p)
                                        {
                                            principals.push(p.clone());
                                        }
                                    }
                                }
                                let mentions = Mentions { principals, all };
                                let env = g.commit(
                                    StreamId::Room { room: id.clone() },
                                    me,
                                    Event::Room(RoomEvent::Posted {
                                        body: c.body,
                                        mentions: mentions.clone(),
                                        re: c.re,
                                        after: c.after,
                                        labels: c.labels,
                                        task: c.task,
                                        idem: c.idem.clone(),
                                    }),
                                    &harness.frame_tx,
                                );
                                if let Some(idem) = c.idem {
                                    g.room_posts.insert((id.clone(), idem), env.seq);
                                }
                                Ok(WireReply::RoomPost(Posted {
                                    room: id,
                                    seq: env.seq,
                                    mentions,
                                    re: c.re,
                                    org_id: None,
                                    delivery_notice: None,
                                }))
                            }
                            RoomCommand::RoomLeave(_) => Ok(WireReply::RoomLeave(Ack)),
                            _ => Err(ApiError::Unavailable {
                                family: Family::Room,
                            }),
                        }
                    }
                    Routed::Board(BoardCommand::BoardEnsure(BoardEnsure { board, lanes })) => {
                        let mut g = harness.inner.lock().await;
                        g.boards.insert(board.clone());
                        g.commit(
                            StreamId::Board {
                                board: board.clone(),
                            },
                            me,
                            Event::Board(BoardEvent::Opened {
                                board: board.clone(),
                                lanes: lanes.clone().unwrap_or_default(),
                            }),
                            &harness.frame_tx,
                        );
                        Ok(WireReply::BoardEnsure(BoardInfo {
                            board,
                            lanes: lanes.unwrap_or_default(),
                            head: 1,
                        }))
                    }
                    Routed::Read(ReadCommand::Read(r)) => {
                        let g = harness.inner.lock().await;
                        let limit = r.limit.unwrap_or(200) as usize;
                        let entries: Vec<Envelope> = g
                            .entries
                            .get(&r.stream)
                            .into_iter()
                            .flatten()
                            .filter(|e| e.seq > r.after)
                            .take(limit)
                            .cloned()
                            .collect();
                        let total_count = entries.len();
                        let last = entries.last().map_or(r.after, |e| e.seq);
                        Ok(WireReply::Read(Page {
                            entries,
                            cursor: Cursor::default().with(r.stream, last),
                            timed_out: false,
                            total_count,
                            truncated: false,
                        }))
                    }
                    Routed::Read(ReadCommand::ThreadPage(t)) => {
                        let limit = t.limit.unwrap_or(100);
                        Ok(WireReply::ThreadPage(
                            harness
                                .bridge
                                .page(&t.wave, &t.role, t.after, limit)
                                .await?,
                        ))
                    }
                    Routed::Read(ReadCommand::Wait(w)) => {
                        let (wtx, mut wrx) = mpsc::unbounded_channel();
                        let target_room = match &w.interest {
                            Interest::Room { room } => Some(room.resolve(&me)?),
                            _ => None,
                        };
                        {
                            let mut g = harness.inner.lock().await;
                            if let Some(room_id) = &target_room {
                                let stream = StreamId::Room {
                                    room: room_id.clone(),
                                };
                                let since = w.after.at(&stream);
                                if let Some(log) = g.entries.get(&stream) {
                                    let catchup: Vec<Envelope> =
                                        log.iter().filter(|e| e.seq > since).cloned().collect();
                                    if !catchup.is_empty() {
                                        let total_count = catchup.len();
                                        let last = catchup.last().unwrap().seq;
                                        return Ok(WireReply::Wait(Page {
                                            entries: catchup,
                                            cursor: w.after.with(stream, last),
                                            timed_out: false,
                                            total_count,
                                            truncated: false,
                                        }));
                                    }
                                }
                            }
                            g.waiters.push(wtx);
                        }
                        let deadline = Duration::from_millis(w.timeout_ms.unwrap_or(5_000));
                        loop {
                            match tokio::time::timeout(deadline, wrx.recv()).await {
                                Ok(Some(env)) => {
                                    if let Some(room_id) = &target_room
                                        && env.stream
                                            == (StreamId::Room {
                                                room: room_id.clone(),
                                            })
                                    {
                                        let cursor =
                                            w.after.clone().with(env.stream.clone(), env.seq);
                                        return Ok(WireReply::Wait(Page {
                                            entries: vec![env],
                                            cursor,
                                            timed_out: false,
                                            total_count: 1,
                                            truncated: false,
                                        }));
                                    }
                                }
                                _ => {
                                    return Ok(WireReply::Wait(Page {
                                        entries: vec![],
                                        cursor: w.after,
                                        timed_out: true,
                                        total_count: 0,
                                        truncated: false,
                                    }));
                                }
                            }
                        }
                    }
                    _ => Err(ApiError::Unavailable {
                        family: Family::Mail,
                    }),
                }
            }
        }

        fn watch(
            &self,
            interest: Interest,
            _from: Cursor,
        ) -> impl Future<Output = Result<FrameStream, ApiError>> + Send {
            let bridge = self.harness.bridge.clone();
            async move {
                match interest {
                    Interest::Agent { wave, role }
                    | Interest::Stage {
                        target: StageTarget::Agent { wave, role },
                    } => bridge.live(&wave, &role).await,
                    _ => Err(ApiError::Unavailable {
                        family: Family::Read,
                    }),
                }
            }
        }
    }

    #[derive(Default)]
    struct MockPluginLedger {
        ops: Vec<String>,
        kv: HashMap<String, String>,
        runtime_active: BTreeSet<String>,
        fail_next_retire: bool,
        spawned_args: Vec<serde_json::Value>,
        sends: Vec<(String, String, String)>,
        resolves: Vec<(String, String, serde_json::Value)>,
        watched: BTreeSet<String>,
        tool_results: Vec<(String, bool, String)>,
        events_by_thread: HashMap<String, Vec<serde_json::Value>>,
        events_page_calls: Vec<(Option<u64>, Option<u64>, u32, Option<protocol::EventsOrder>)>,
    }

    #[tokio::test]
    async fn loopback_bb_plugin_e2e_spawn_resolve_doorbell_lease_cancel_reconnect_and_stop_before_archive()
     {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let token = "tok_secret_pr3";

        let ledger = Arc::new(Mutex::new(MockPluginLedger::default()));
        let (push_up_tx, push_up_rx) = mpsc::unbounded_channel::<protocol::Up>();
        let (reload_tx, reload_rx) = mpsc::unbounded_channel::<()>();
        let (unwatch_notify_tx, mut unwatch_notify_rx) = mpsc::unbounded_channel::<String>();

        let server_ledger = ledger.clone();
        let server_task = tokio::spawn(async move {
            let mut push_up_rx = push_up_rx;
            let mut reload_rx = reload_rx;
            while let Ok((tcp, _)) = listener.accept().await {
                let Ok(ws) = ws::WsConn::accept(tcp, token).await else {
                    continue;
                };
                let (mut ws_reader, mut ws_writer) = ws.into_split();
                if ws_writer
                    .send_text(
                        &serde_json::to_string(&protocol::Up::Hello {
                            plugin_version: "0.1.0".into(),
                            sdk_version: "0.6.132".into(),
                        })
                        .unwrap(),
                    )
                    .await
                    .is_err()
                {
                    continue;
                }

                loop {
                    tokio::select! {
                        Some(()) = reload_rx.recv() => {
                            let _ = ws_writer.send_close(1012, "plugin reload").await;
                            break;
                        }
                        Some(up) = push_up_rx.recv() => {
                            if ws_writer.send_text(&serde_json::to_string(&up).unwrap()).await.is_err() {
                                break;
                            }
                        }
                        inbound = ws_reader.recv() => {
                            let Ok(Some(ws::WsInbound::Text(raw))) = inbound else { break };
                            let down: protocol::Down = serde_json::from_str(&raw).unwrap();
                            let mut st = server_ledger.lock().await;
                            match down {
                                protocol::Down::FindByMeta { req_id, meta } => {
                                    let key = format!("spawn:{}/{}/{}", meta.wave_id, meta.role, meta.attempt);
                                    let found = st.kv.get(&key).cloned();
                                    let reply = protocol::Up::Result {
                                        req_id,
                                        ok: true,
                                        value: serde_json::json!({ "thread_id": found }),
                                    };
                                    ws_writer.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
                                }
                                protocol::Down::Spawn { req_id, args } => {
                                    st.spawned_args.push(args.clone());
                                    let meta: protocol::WaveMeta =
                                        serde_json::from_value(args["pluginMetadata"]["rexWave"].clone()).unwrap();
                                    let tid = format!("th_{}_{}", meta.wave_id, meta.role);
                                    let key = format!("spawn:{}/{}/{}", meta.wave_id, meta.role, meta.attempt);
                                    st.kv.insert(key, tid.clone());
                                    st.runtime_active.insert(tid.clone());
                                    st.ops.push(format!("spawn:{tid}"));
                                    st.events_by_thread.insert(
                                        tid.clone(),
                                        vec![
                                            serde_json::json!({
                                                "seq": 1,
                                                "createdAt": 1001,
                                                "type": "turn/started",
                                                "data": {}
                                            }),
                                            serde_json::json!({
                                                "seq": 2,
                                                "createdAt": 1002,
                                                "type": "item/completed",
                                                "data": { "item": { "type": "agentMessage", "text": "Checking slice" } }
                                            }),
                                        ],
                                    );
                                    let reply = protocol::Up::Result {
                                        req_id,
                                        ok: true,
                                        value: serde_json::json!({ "thread_id": tid }),
                                    };
                                    ws_writer.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
                                }
                                protocol::Down::Send { req_id, thread_id, mode, text } => {
                                    st.ops.push(format!("send:{thread_id}:{mode}"));
                                    st.sends.push((thread_id, mode, text));
                                    let reply = protocol::Up::Result {
                                        req_id,
                                        ok: true,
                                        value: serde_json::json!({ "delivery": "sent" }),
                                    };
                                    ws_writer.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
                                }
                                protocol::Down::Resolve { req_id, thread_id, interaction_id, resolution } => {
                                    st.ops.push(format!("resolve:{thread_id}:{interaction_id}"));
                                    st.resolves.push((thread_id, interaction_id, resolution));
                                    let reply = protocol::Up::Result { req_id, ok: true, value: serde_json::Value::Null };
                                    ws_writer.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
                                }
                                protocol::Down::Retire { req_id, thread_id } => {
                                    if st.fail_next_retire {
                                        st.fail_next_retire = false;
                                        let reply = protocol::Up::Result {
                                            req_id,
                                            ok: false,
                                            value: serde_json::Value::String("thread remained stopping".into()),
                                        };
                                        ws_writer.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
                                        continue;
                                    }
                                    st.runtime_active.remove(&thread_id);
                                    st.ops.push(format!("stop:{thread_id}"));
                                    assert!(!st.runtime_active.contains(&thread_id));
                                    st.ops.push(format!("archive:{thread_id}"));
                                    let reply = protocol::Up::Result { req_id, ok: true, value: serde_json::Value::Null };
                                    ws_writer.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
                                }
                                protocol::Down::EventsPage {
                                    req_id,
                                    thread_id,
                                    after_seq,
                                    before_seq,
                                    limit,
                                    order,
                                } => {
                                    st.events_page_calls.push((after_seq, before_seq, limit, order));
                                    let mut rows: Vec<serde_json::Value> = st
                                        .events_by_thread
                                        .get(&thread_id)
                                        .cloned()
                                        .unwrap_or_default()
                                        .into_iter()
                                        .filter(|r| {
                                            let s = r["seq"].as_u64().unwrap_or(0);
                                            after_seq.is_none_or(|a| s > a)
                                                && before_seq.is_none_or(|b| s < b)
                                        })
                                        .collect();
                                    if matches!(order, Some(protocol::EventsOrder::Desc)) {
                                        rows.reverse();
                                    }
                                    rows.truncate(limit as usize);
                                    let reply = protocol::Up::Result {
                                        req_id,
                                        ok: true,
                                        value: serde_json::Value::Array(rows),
                                    };
                                    ws_writer.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
                                }
                                protocol::Down::ThreadContext { req_id, .. } => {
                                    let reply = protocol::Up::Result {
                                        req_id,
                                        ok: true,
                                        value: serde_json::json!({
                                            "used_tokens": 24_000,
                                            "model_context_window": 200_000,
                                            "estimated": false,
                                            "auto_compact_at_tokens": 160_000,
                                            "cost_usd": 0.18,
                                            "categories": [
                                                { "id": "messages", "label": "Messages", "kind": "used", "tokens": 24_000 }
                                            ]
                                        }),
                                    };
                                    ws_writer.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
                                }
                                protocol::Down::WatchThread { thread_id, .. } => {
                                    st.ops.push(format!("watch:{thread_id}"));
                                    st.watched.insert(thread_id);
                                }
                                protocol::Down::UnwatchThread { thread_id } => {
                                    st.ops.push(format!("unwatch:{thread_id}"));
                                    st.watched.remove(&thread_id);
                                    let _ = unwatch_notify_tx.send(thread_id);
                                }
                                protocol::Down::ToolResult { call_id, ok, text } => {
                                    st.tool_results.push((call_id, ok, text));
                                }
                                protocol::Down::Push { title, body } => {
                                    st.ops.push(format!("push:{title}:{body}"));
                                }
                            }
                        }
                    }
                }
            }
        });

        let bridge = BbBridge::new(BridgeConfig {
            url: format!("ws://{addr}/api/v1/plugins/rex-wave/http/bridge"),
            token: token.into(),
        });
        let (harness, mut frame_rx) = LoopbackHarness::new(bridge.clone());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let bridge_runner = tokio::spawn({
            let bridge = bridge.clone();
            let harness = harness.clone();
            async move { bridge.run_until(harness, shutdown_rx).await }
        });

        let mut conductor = WaveConductor::new(BbHost(bridge.clone()));
        let sys_conductor = harness.client(Principal::System {
            part: SystemPart::WaveConductor,
        });
        conductor
            .handle_frame(
                Frame::CaughtUp {
                    cursor: Cursor::default(),
                },
                &sys_conductor,
            )
            .await
            .unwrap();

        let conductor_task = tokio::spawn(async move {
            while let Some(env) = frame_rx.recv().await {
                let _ = conductor
                    .handle_frame(Frame::Committed(env), &sys_conductor)
                    .await;
            }
        });

        let op = harness.client(Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        });
        let wave = WaveSlug::parse("auth").unwrap();
        let role = RoleName::parse("verifier").unwrap();
        let spec = WaveSpec {
            slug: wave.clone(),
            project: BbProjectId::parse("proj_demo_main").unwrap(),
            goal: Body::parse("Verify OAuth refresh").unwrap(),
            defaults: AgentSpec {
                provider: ProviderId::parse("codex").unwrap(),
                model: Some(ModelId::parse("gpt-6.1-sol").unwrap()),
                account: None,
                reasoning: Some("xhigh".into()),
                service_tier: None,
                permission: PermissionMode::AcceptEdits,
                workspace: Workspace::ManagedWorktree {
                    base: BaseBranch::Default,
                },
            },
            output_dir: None,
            workers: vec![WorkerSpec {
                role: role.clone(),
                brief: Body::parse("Audit token rotation").unwrap(),
                agent: None,
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            }],
        };

        let started = op.call(WaveStart { spec, idem: None }).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                if matches!(&snap.workers[0].2, WorkerPhase::Live { thread, .. } if thread.as_str() == "th_auth_verifier") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker spawned over real loopback RFC 6455 bridge");

        {
            let st = ledger.lock().await;
            assert_eq!(st.spawned_args.len(), 1);
            assert_eq!(st.spawned_args[0]["environment"]["type"], "host");
            assert_eq!(
                st.spawned_args[0]["environment"]["workspace"]["type"],
                "managed-worktree"
            );
            assert_eq!(
                st.spawned_args[0]["pluginMetadata"]["rexWave"]["role"],
                "verifier"
            );
        }

        let idempotent_tid = BbHost(bridge.clone())
            .spawn(SpawnRequest {
                wave: wave.clone(),
                role: role.clone(),
                attempt: 1,
                project: BbProjectId::parse("proj_demo_main").unwrap(),
                agent: AgentSpec {
                    provider: ProviderId::parse("codex").unwrap(),
                    model: Some(ModelId::parse("gpt-6.1-sol").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::AcceptEdits,
                    workspace: Workspace::ProjectDefault,
                },
                room: started.room.clone(),
                prompt: Body::parse("retry").unwrap(),
                title: "[auth] @verifier".into(),
                run_stamp: "auth:verifier:attempt-1:seq-1".into(),
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            })
            .await
            .unwrap();
        assert_eq!(idempotent_tid.as_str(), "th_auth_verifier");
        assert_eq!(
            ledger.lock().await.spawned_args.len(),
            1,
            "FindByMeta prevented duplicate spawn"
        );

        push_up_tx
            .send(protocol::Up::Interaction {
                thread_id: "th_auth_verifier".into(),
                meta: None,
                interaction: serde_json::json!({
                    "id": "int_8f2",
                    "payload": {
                        "kind": "approval",
                        "reason": "Run integration suite"
                    }
                }),
            })
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                if matches!(&snap.workers[0].2, WorkerPhase::Live { activity: Activity::Blocked { interaction }, .. } if interaction.id.as_str() == "int_8f2") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker entered Blocked state on interaction.pending");

        op.call(WaveResolve {
            wave: wave.clone(),
            role: role.clone(),
            interaction: BbInteractionId::parse("int_8f2").unwrap(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !ledger.lock().await.resolves.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("wave_resolve forwarded to BB interactions.resolve");
        assert_eq!(
            ledger.lock().await.resolves[0],
            (
                "th_auth_verifier".into(),
                "int_8f2".into(),
                serde_json::json!({ "decision": "allow_once", "grantedPermissions": null }),
            )
        );

        push_up_tx
            .send(protocol::Up::ToolCall {
                call_id: "call_wait_1".into(),
                thread_id: "th_auth_verifier".into(),
                project_id: "proj_demo_main".into(),
                meta: None,
                tool: "wait".into(),
                args: serde_json::to_value(Wait::room(started.room.clone(), Seq(4), false))
                    .unwrap(),
            })
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                if matches!(
                    &snap.workers[0].2,
                    WorkerPhase::Live {
                        activity: Activity::Parked { .. },
                        ..
                    }
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker entered Parked state during rex_wait");

        op.call(RoomPost {
            room: RoomRef::Id {
                id: started.room.clone(),
            },
            body: Body::parse("@verifier slice A ready").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if ledger
                    .lock()
                    .await
                    .tool_results
                    .iter()
                    .any(|(id, ok, _)| id == "call_wait_1" && *ok)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("rex_wait woke and returned catch-up slice");
        assert!(
            ledger.lock().await.sends.is_empty(),
            "Parked worker woke via ReadSide with zero doorbell sends"
        );

        push_up_tx
            .send(protocol::Up::Interaction {
                thread_id: "th_auth_verifier".into(),
                meta: None,
                interaction: serde_json::json!({
                    "id": "int_perm_2",
                    "payload": {
                        "kind": "approval",
                        "reason": "Run check"
                    }
                }),
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                if matches!(
                    &snap.workers[0].2,
                    WorkerPhase::Live {
                        activity: Activity::Blocked { interaction },
                        ..
                    } if interaction.id.as_str() == "int_perm_2"
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker entered Blocked state on int_perm_2");

        {
            let mut st = ledger.lock().await;
            st.events_page_calls.clear();
            let list = st.events_by_thread.get_mut("th_auth_verifier").unwrap();
            for seq in 3..=249u64 {
                list.push(serde_json::json!({
                    "seq": seq,
                    "createdAt": 1000 + seq,
                    "type": "turn/started",
                    "data": {}
                }));
            }
        }
        bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerRequested {
                role: role.clone(),
                agent: AgentSpec {
                    provider: ProviderId::parse("codex").unwrap(),
                    model: Some(ModelId::parse("gpt-6.1-sol").unwrap()),
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::AcceptEdits,
                    workspace: Workspace::ProjectDefault,
                },
                brief: Body::parse("verify").unwrap(),
                attempt: 1,
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            },
        );
        bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: role.clone(),
                observation: Observation::Spawned {
                    attempt: 1,
                    thread: BbThreadId::parse("th_auth_verifier").unwrap(),
                },
            },
        );
        bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: role.clone(),
                observation: Observation::Activity {
                    activity: Activity::Blocked {
                        interaction: PendingInteraction {
                            id: BbInteractionId::parse("int_perm_2").unwrap(),
                            kind: InteractionKind::Permission,
                            summary: "Run check".into(),
                        },
                    },
                },
            },
        );
        bridge.reconcile_live_workers().await;
        {
            let calls = &ledger.lock().await.events_page_calls;
            assert_eq!(
                calls.len(),
                1,
                "cold reconcile used a single descending tail query instead of scanning 249 rows"
            );
            assert_eq!(calls[0].3, Some(protocol::EventsOrder::Desc));
        }
        let still_blocked = op.call(WaveView { wave: wave.clone() }).await.unwrap();
        assert!(
            matches!(
                &still_blocked.workers[0].2,
                WorkerPhase::Live {
                    activity: Activity::Blocked { interaction },
                    ..
                } if interaction.id.as_str() == "int_perm_2"
            ),
            "trailing turn/started during reconcile must never overwrite Blocked"
        );

        {
            let mut st = ledger.lock().await;
            st.events_by_thread
                .get_mut("th_auth_verifier")
                .unwrap()
                .push(serde_json::json!({
                    "seq": 250,
                    "createdAt": 1250,
                    "type": "turn/completed",
                    "data": {}
                }));
        }
        reload_tx.send(()).unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                if matches!(
                    &snap.workers[0].2,
                    WorkerPhase::Live {
                        activity: Activity::Idle,
                        ..
                    }
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("reconnect cleared Blocked to Idle once turn/completed arrived");

        op.call(RoomPost {
            room: RoomRef::Id {
                id: started.room.clone(),
            },
            body: Body::parse("@verifier check #4").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !ledger.lock().await.sends.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("idle @mention rang send(auto) doorbell");
        {
            let st = ledger.lock().await;
            assert_eq!(st.sends[0].0, "th_auth_verifier");
            assert_eq!(st.sends[0].1, "auto");
            assert!(st.sends[0].2.contains("@verifier check #4"));
        }

        let page = op
            .call(ThreadPage {
                wave: wave.clone(),
                role: role.clone(),
                after: Some(BbSeq(248)),
                limit: Some(50),
            })
            .await
            .unwrap();
        assert_eq!(page.events.len(), 2);
        assert_eq!(page.usage.as_ref().unwrap().used_tokens, 24_000);
        assert_eq!(page.usage.as_ref().unwrap().cost_usd, Some(0.18));

        let mut live_rx = bridge.open_live_channel(&wave, &role).await.unwrap();
        let stage_stream = op
            .watch(
                Interest::Stage {
                    target: StageTarget::Agent {
                        wave: wave.clone(),
                        role: role.clone(),
                    },
                },
                Cursor::live(),
            )
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if ledger.lock().await.watched.contains("th_auth_verifier") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("WatchThread sent on AgentInspector lease open");

        {
            let mut st = ledger.lock().await;
            st.events_by_thread
                .get_mut("th_auth_verifier")
                .unwrap()
                .push(serde_json::json!({
                    "seq": 251,
                    "createdAt": 1251,
                    "type": "item/completed",
                    "data": { "item": { "type": "agentMessage", "text": "Slice #4 looks clean" } }
                }));
        }
        push_up_tx
            .send(protocol::Up::ThreadEvents {
                thread_id: "th_auth_verifier".into(),
                sequence: 251,
            })
            .unwrap();

        let live_frame = tokio::time::timeout(Duration::from_secs(2), live_rx.recv())
            .await
            .expect("received live agent frame")
            .unwrap()
            .unwrap();
        assert!(matches!(
            live_frame,
            Frame::Agent { event: AgentEvent { seq: BbSeq(251), kind: AgentEventKind::AssistantText { ref text }, .. }, .. }
                if text == "Slice #4 looks clean"
        ));

        drop(live_rx);
        drop(stage_stream);

        let unwatched = tokio::time::timeout(Duration::from_secs(2), unwatch_notify_rx.recv())
            .await
            .expect("dropping AgentInspector watch leases immediately sent UnwatchThread")
            .unwrap();
        assert_eq!(unwatched, "th_auth_verifier");

        ledger.lock().await.ops.clear();
        ledger.lock().await.fail_next_retire = true;
        op.call(WaveFinish {
            wave: wave.clone(),
            outcome: WaveOutcome::Completed {
                summary: Body::parse("Verified").unwrap(),
            },
            allow_incomplete: false,
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(40)).await;
        let mid_snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
        assert!(
            matches!(&mid_snap.workers[0].2, WorkerPhase::Retiring { .. }),
            "rejected retire leaves worker Retiring until retry succeeds"
        );

        op.call(RoomPost {
            room: RoomRef::Id { id: started.room },
            body: Body::parse("nudge dirty reconcile").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                if matches!(&snap.workers[0].2, WorkerPhase::Retired { thread: Some(t), .. } if t.as_str() == "th_auth_verifier") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker retired cleanly on retry");

        assert_eq!(
            ledger.lock().await.ops,
            vec!["stop:th_auth_verifier", "archive:th_auth_verifier"],
            "stop executed and deactivated runtime before archive"
        );

        let _ = shutdown_tx.send(true);
        let _ = bridge_runner.await;
        conductor_task.abort();
        server_task.abort();
    }

    #[test]
    fn extract_unposted_final_turn_picks_final_message_skips_room_posts_and_ignores_failures() {
        let unposted_rows = vec![
            serde_json::json!({ "seq": 10, "createdAt": 1000, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 11,
                "createdAt": 1010,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "Reading files first..." } }
            }),
            serde_json::json!({
                "seq": 12,
                "createdAt": 1020,
                "type": "item/completed",
                "data": { "item": { "type": "commandExecution", "command": "cargo test", "status": "completed" } }
            }),
            serde_json::json!({
                "seq": 13,
                "createdAt": 1030,
                "type": "item/agentMessage/delta",
                "data": { "delta": "partial" }
            }),
            serde_json::json!({
                "seq": 14,
                "createdAt": 1040,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "Final audit: 0 regressions." } }
            }),
            serde_json::json!({
                "seq": 15,
                "createdAt": 1050,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }),
            serde_json::json!({ "seq": 16, "createdAt": 1060, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 17,
                "createdAt": 1070,
                "type": "item/agentMessage/delta",
                "data": { "delta": "queued next turn..." }
            }),
        ];
        let mut desc = unposted_rows.clone();
        desc.reverse();
        let extracted = extract_unposted_final_turn(&desc).expect("final turn extracted");
        assert_eq!(extracted.anchor, "s10");
        assert_eq!(extracted.turn_start_at, 1000);
        assert_eq!(extracted.text, "Final audit: 0 regressions.");

        let posted_rows = vec![
            serde_json::json!({ "seq": 20, "createdAt": 2000, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 21,
                "createdAt": 2010,
                "type": "item/completed",
                "data": {
                    "item": {
                        "type": "toolCall",
                        "tool": "rex_room_post",
                        "status": "completed",
                        "arguments": { "room": { "ref": "channel", "name": "wave-w1" } }
                    }
                }
            }),
            serde_json::json!({
                "seq": 22,
                "createdAt": 2020,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "Posted the review to @dave." } }
            }),
            serde_json::json!({ "seq": 23, "createdAt": 2030, "type": "turn/completed", "data": {} }),
        ];
        assert_eq!(
            extract_unposted_final_turn(&posted_rows),
            None,
            "turn that already called rex_room_post to channel is not mirrored"
        );

        let failed_rows = vec![
            serde_json::json!({ "seq": 30, "createdAt": 3000, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 31,
                "createdAt": 3010,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "Halfway through..." } }
            }),
            serde_json::json!({
                "seq": 32,
                "createdAt": 3020,
                "type": "turn/completed",
                "data": { "status": "failed" }
            }),
        ];
        assert_eq!(extract_unposted_final_turn(&failed_rows), None);
    }

    #[test]
    fn local_bb_project_cache_loads_cleanly_with_offline_fallback() {
        let unique = format!(
            "rex-bb-cache-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).unwrap();

        assert!(
            load_bb_projects_from_dir(&dir).is_empty(),
            "missing cache files fall back cleanly to empty map"
        );

        std::fs::write(
            dir.join("projects.json"),
            r#"[{"id":"proj_demo_alpha","name":"demo-work"},{"id":"proj_demo_beta","name":"demo-house"}]"#,
        )
        .unwrap();
        let loaded = load_bb_projects_from_dir(&dir);
        assert_eq!(
            loaded
                .get(&BbProjectId::parse("proj_demo_alpha").unwrap())
                .map(String::as_str),
            Some("demo-work")
        );
        assert_eq!(
            loaded
                .get(&BbProjectId::parse("proj_demo_beta").unwrap())
                .map(String::as_str),
            Some("demo-house")
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn final_turn_mirroring_three_turn_sequence_and_project_names_work_end_to_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let token = "tok_mirror_e2e";

        let events_by_thread: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let spawned_envs: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let sends: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let (push_up_tx, mut push_up_rx) = mpsc::unbounded_channel::<protocol::Up>();

        let srv_events = events_by_thread.clone();
        let srv_envs = spawned_envs.clone();
        let srv_sends = sends.clone();
        let server_task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let ws = ws::WsConn::accept(tcp, token).await.unwrap();
            let (mut ws_reader, mut ws_writer) = ws.into_split();
            ws_writer
                .send_text(
                    &serde_json::to_string(&protocol::Up::Hello {
                        plugin_version: "0.1.0".into(),
                        sdk_version: "0.6.132".into(),
                    })
                    .unwrap(),
                )
                .await
                .unwrap();

            loop {
                tokio::select! {
                    Some(up) = push_up_rx.recv() => {
                        if ws_writer.send_text(&serde_json::to_string(&up).unwrap()).await.is_err() {
                            break;
                        }
                    }
                    inbound = ws_reader.recv() => {
                        let Ok(Some(ws::WsInbound::Text(raw))) = inbound else { break };
                        let down: protocol::Down = serde_json::from_str(&raw).unwrap();
                        match down {
                            protocol::Down::FindByMeta { req_id, .. } => {
                                let reply = protocol::Up::Result {
                                    req_id,
                                    ok: true,
                                    value: serde_json::json!({ "thread_id": null }),
                                };
                                let _ = ws_writer.send_text(&serde_json::to_string(&reply).unwrap()).await;
                            }
                            protocol::Down::Spawn { req_id, args } => {
                                srv_envs.lock().await.push(args["environment"].clone());
                                let role = args["pluginMetadata"]["rexWave"]["role"].as_str().unwrap();
                                let tid = format!("th_mirror_{role}");
                                let reply = protocol::Up::Result {
                                    req_id,
                                    ok: true,
                                    value: serde_json::json!({ "thread_id": tid }),
                                };
                                let _ = ws_writer.send_text(&serde_json::to_string(&reply).unwrap()).await;
                            }
                            protocol::Down::EventsPage { req_id, thread_id, order, .. } => {
                                let mut rows = srv_events
                                    .lock()
                                    .await
                                    .get(&thread_id)
                                    .cloned()
                                    .unwrap_or_default();
                                if order == Some(protocol::EventsOrder::Desc) {
                                    rows.reverse();
                                }
                                let reply = protocol::Up::Result {
                                    req_id,
                                    ok: true,
                                    value: serde_json::Value::Array(rows),
                                };
                                let _ = ws_writer.send_text(&serde_json::to_string(&reply).unwrap()).await;
                            }
                            protocol::Down::Send { req_id, thread_id, .. } => {
                                srv_sends.lock().await.push(thread_id);
                                let reply = protocol::Up::Result {
                                    req_id,
                                    ok: true,
                                    value: serde_json::json!({ "delivery": "sent" }),
                                };
                                let _ = ws_writer.send_text(&serde_json::to_string(&reply).unwrap()).await;
                            }
                            protocol::Down::ToolResult { .. } => {}
                            _ => {}
                        }
                    }
                }
            }
        });

        let tmp_dir = std::env::temp_dir().join(format!("rex-bb-mirror-{}", std::process::id()));
        std::fs::create_dir_all(&tmp_dir).unwrap();
        std::fs::write(
            tmp_dir.join("projects.json"),
            r#"[{"id":"proj_demo_alpha","name":"demo-work"}]"#,
        )
        .unwrap();

        let bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: format!("ws://{addr}/bridge"),
                token: token.into(),
            },
            Some(tmp_dir.clone()),
        );
        let (harness, mut frame_rx) = LoopbackHarness::new(bridge.clone());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let bridge_runner = tokio::spawn({
            let bridge = bridge.clone();
            let harness = harness.clone();
            async move { bridge.run_until(harness, shutdown_rx).await }
        });

        let sys_conductor = harness.client(Principal::System {
            part: SystemPart::WaveConductor,
        });
        let mut conductor = WaveConductor::new(BbHost(bridge.clone()));
        conductor
            .handle_frame(
                Frame::CaughtUp {
                    cursor: Cursor::default(),
                },
                &sys_conductor,
            )
            .await
            .unwrap();
        let conductor_task = tokio::spawn(async move {
            while let Some(env) = frame_rx.recv().await {
                let _ = conductor
                    .handle_frame(Frame::Committed(env), &sys_conductor)
                    .await;
            }
        });

        let op = harness.client(Principal::Operator {
            seat: SeatName::parse("dave").unwrap(),
        });
        let project = BbProjectId::parse("proj_demo_alpha").unwrap();
        let wave = WaveSlug::parse("demo-r2").unwrap();
        let role = RoleName::parse("council-claude").unwrap();
        let peer_role = RoleName::parse("council-gpt").unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let names = bridge.project_names().unwrap().borrow().clone();
                if names.get(&project).map(String::as_str) == Some("demo-work") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("local BB cache populated human project name asynchronously");

        let started = op
            .call(WaveStart {
                spec: WaveSpec {
                    slug: wave.clone(),
                    project: project.clone(),
                    goal: Body::parse("Review spec").unwrap(),
                    defaults: AgentSpec {
                        provider: ProviderId::parse("claude-code").unwrap(),
                        model: Some(ModelId::parse("claude-opus-5-5").unwrap()),
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: PermissionMode::Full,
                        workspace: Workspace::ManagedWorktree {
                            base: BaseBranch::Default,
                        },
                    },
                    output_dir: None,
                    workers: vec![
                        WorkerSpec {
                            role: role.clone(),
                            brief: Body::parse("Audit spec").unwrap(),
                            agent: None,
                            output_dir: None,
                            writes: Vec::new(),
                            return_schema: None,
                        },
                        WorkerSpec {
                            role: peer_role.clone(),
                            brief: Body::parse("Audit concurrency").unwrap(),
                            agent: None,
                            output_dir: None,
                            writes: Vec::new(),
                            return_schema: None,
                        },
                    ],
                },
                idem: None,
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                if snap
                    .workers
                    .iter()
                    .all(|(_, _, p)| matches!(p, WorkerPhase::Live { .. }))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("workers spawned");

        {
            let envs = spawned_envs.lock().await;
            assert_eq!(envs[0]["type"], "host");
            assert!(envs[0].get("hostId").is_none());
            assert_eq!(envs[0]["workspace"]["type"], "managed-worktree");
        }

        let peer_meta = protocol::WaveMeta {
            wave_id: wave.to_string(),
            role: peer_role.to_string(),
            attempt: 1,
            room_id: serde_json::to_string(&started.room).unwrap(),
            run_stamp: None,
            output_dir: None,
            writes: Vec::new(),
            account: None,
        };
        push_up_tx
            .send(protocol::Up::Thread {
                signal: protocol::ThreadSignal::Idle,
                thread_id: "th_mirror_council-gpt".into(),
                project_id: project.to_string(),
                meta: Some(peer_meta),
                error: None,
                stop_reason: None,
                model: None,
                account: None,
            })
            .unwrap();

        let meta = protocol::WaveMeta {
            wave_id: wave.to_string(),
            role: role.to_string(),
            attempt: 1,
            room_id: serde_json::to_string(&started.room).unwrap(),
            run_stamp: None,
            output_dir: None,
            writes: Vec::new(),
            account: None,
        };

        events_by_thread.lock().await.insert(
            "th_mirror_council-claude".into(),
            vec![
                serde_json::json!({ "seq": 1, "createdAt": 1_000, "type": "turn/started", "data": {} }),
                serde_json::json!({
                    "seq": 2,
                    "createdAt": 1_001,
                    "type": "item/completed",
                    "data": { "item": { "type": "agentMessage", "text": "Checking files..." } }
                }),
                serde_json::json!({
                    "seq": 3,
                    "createdAt": 1_002,
                    "type": "item/completed",
                    "data": { "item": { "type": "agentMessage", "text": "@dave Round 1 score: 8/10." } }
                }),
                serde_json::json!({ "seq": 4, "createdAt": 1_003, "type": "turn/completed", "data": { "status": "completed" } }),
            ],
        );
        for _ in 0..2 {
            push_up_tx
                .send(protocol::Up::Thread {
                    signal: protocol::ThreadSignal::Idle,
                    thread_id: "th_mirror_council-claude".into(),
                    project_id: project.to_string(),
                    meta: Some(meta.clone()),
                    error: None,
                    stop_reason: None,
                    model: None,
                    account: None,
                })
                .unwrap();
        }

        let worker_p = Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let page = op
                    .call(Read {
                        stream: StreamId::Room {
                            room: started.room.clone(),
                        },
                        after: Seq::ORIGIN,
                        limit: Some(50),
                    })
                    .await
                    .unwrap();
                if page.entries.iter().any(|e| e.by == worker_p) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Turn 1 mirrored into wave room");

        let worker_client = harness.client(worker_p.clone());
        let explicit_t2 = worker_client
            .call(RoomPost {
                room: RoomRef::Id {
                    id: started.room.clone(),
                },
                body: Body::parse("Explicit Turn 2 post").unwrap(),
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
                idem: None,
            })
            .await
            .unwrap();
        let t2_start_at = 1_000 + explicit_t2.seq.0;
        {
            let mut map = events_by_thread.lock().await;
            let list = map.get_mut("th_mirror_council-claude").unwrap();
            list.push(serde_json::json!({ "seq": 5, "createdAt": t2_start_at, "type": "turn/started", "data": {} }));
            list.push(serde_json::json!({
                "seq": 7,
                "createdAt": t2_start_at + 1,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "Ack: posted Turn 2." } }
            }));
            list.push(serde_json::json!({ "seq": 8, "createdAt": t2_start_at + 2, "type": "turn/completed", "data": { "status": "completed" } }));
        }
        push_up_tx
            .send(protocol::Up::Thread {
                signal: protocol::ThreadSignal::Idle,
                thread_id: "th_mirror_council-claude".into(),
                project_id: project.to_string(),
                meta: Some(meta.clone()),
                error: None,
                stop_reason: None,
                model: None,
                account: None,
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let fresh_bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: format!("ws://{addr}/bridge"),
                token: token.into(),
            },
            None,
        );
        let _ = fresh_bridge.backend.set(Arc::new(harness.clone()));
        let t3_rows = vec![
            serde_json::json!({ "seq": 9, "createdAt": t2_start_at + 10, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 10,
                "createdAt": t2_start_at + 11,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "@council-gpt @dave Turn 3 unposted addendum." } }
            }),
            serde_json::json!({ "seq": 11, "createdAt": t2_start_at + 12, "type": "turn/completed", "data": { "status": "completed" } }),
        ];
        sends.lock().await.clear();
        fresh_bridge
            .mirror_completed_turn_from_rows(
                &wave,
                &role,
                &BbThreadId::parse("th_mirror_council-claude").unwrap(),
                &t3_rows,
            )
            .await;

        let t3_posts = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let page = op
                    .call(Read {
                        stream: StreamId::Room {
                            room: started.room.clone(),
                        },
                        after: Seq::ORIGIN,
                        limit: Some(50),
                    })
                    .await
                    .unwrap();
                let posts: Vec<String> = page
                    .entries
                    .iter()
                    .filter(|e| e.by == worker_p)
                    .filter_map(|e| match &e.event {
                        Event::Room(RoomEvent::Posted { body, .. }) => {
                            Some(body.as_str().to_owned())
                        }
                        _ => None,
                    })
                    .collect();
                if posts.len() == 3 {
                    break posts;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Turn 3 unposted deliverable mirrored after Turn 2 tool post");

        assert_eq!(
            t3_posts,
            vec![
                "@dave Round 1 score: 8/10.".to_owned(),
                "Explicit Turn 2 post".to_owned(),
                "@council-gpt @dave Turn 3 unposted addendum.".to_owned(),
            ]
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            sends.lock().await.is_empty(),
            "mirrored fallback turn must not wake idle peer @council-gpt"
        );

        let _ = shutdown_tx.send(true);
        let _ = bridge_runner.await;
        conductor_task.abort();
        server_task.abort();
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn bb_refusal_stop_reasons_runtime_meta_and_spawn_provenance_work() {
        // S1: BB-shaped failed turn (provider/error policy + turn/completed status=failed with nested error.message)
        // must resolve to Refused, never Idle.
        let bb_refused_turn = vec![
            serde_json::json!({ "seq": 1, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 2,
                "type": "provider/error",
                "data": {
                    "message": "blocked by policy",
                    "errorInfo": { "category": "policy", "providerCode": "cyber_safeguard", "httpStatusCode": 400 }
                }
            }),
            serde_json::json!({
                "seq": 3,
                "type": "turn/completed",
                "data": { "status": "failed", "error": { "message": "turn failed" } }
            }),
        ];
        assert_eq!(
            derive_latest_turn_activity(&bb_refused_turn),
            Some(Activity::Refused {
                reason: "cyber_safeguard: blocked by policy".into()
            })
        );
        assert_eq!(
            refusal_from_latest_turn(&bb_refused_turn).as_deref(),
            Some("cyber_safeguard: blocked by policy")
        );

        // S1: BB-shaped generic failed turn (non-refusal) resolves to Errored with nested error.message.
        let bb_failed_turn = vec![
            serde_json::json!({ "seq": 1, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 2,
                "type": "turn/completed",
                "data": { "status": "failed", "error": { "message": "upstream timeout" } }
            }),
        ];
        assert_eq!(
            derive_latest_turn_activity(&bb_failed_turn),
            Some(Activity::Errored {
                error: "upstream timeout".into()
            })
        );

        // S8: a retrying provider/error (willRetry: true) in a turn that completes normally stays Idle.
        let retried_then_ok = vec![
            serde_json::json!({ "seq": 1, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 2,
                "type": "provider/error",
                "data": {
                    "message": "transient policy hiccup",
                    "willRetry": true,
                    "errorInfo": { "category": "policy", "providerCode": null, "httpStatusCode": null }
                }
            }),
            serde_json::json!({
                "seq": 3,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }),
        ];
        assert_eq!(
            derive_latest_turn_activity(&retried_then_ok),
            Some(Activity::Idle)
        );
        assert_eq!(refusal_from_latest_turn(&retried_then_ok), None);

        // S4: runtime model extraction reads actual BB runtime events (contextWindowUsage.snapshot.model
        // and provider/modelFallback), ignoring request echoes in client/turn/start.
        let runtime_rows = vec![
            serde_json::json!({
                "seq": 1,
                "type": "client/turn/start",
                "data": { "execution": { "model": "requested-echo", "account": "echo-acct" } }
            }),
            serde_json::json!({
                "seq": 2,
                "type": "thread/contextWindowUsage/updated",
                "data": { "contextWindowUsage": { "snapshot": { "model": "claude-opus-5-5" } } }
            }),
            serde_json::json!({
                "seq": 3,
                "type": "provider/modelFallback",
                "data": { "originalModel": "claude-opus-5-5", "fallbackModel": "claude-sonnet-5-0", "reason": "refusal", "message": "fallback" }
            }),
        ];
        let (model, account) = extract_runtime_meta_from_rows(&runtime_rows);
        assert_eq!(
            model.as_ref().map(ModelId::as_str),
            Some("claude-sonnet-5-0")
        );
        assert_eq!(account, None);

        // S10: narrow missing-thread matching ignores unrelated "missing required field" errors.
        assert_eq!(
            classify_missing_or_archived_thread("missing required field: threadId"),
            None
        );
        assert!(matches!(
            classify_missing_or_archived_thread("thread not found"),
            Some(Observation::Activity {
                activity: Activity::Errored { ref error }
            }) if error.contains("missing after restart")
        ));

        // S4: spawn_args keeps account in pluginMetadata.rexWave, not provider sessionOptions.
        let room = rex_wave_waves::wave_room(
            &WaveSlug::parse("w-prov").unwrap(),
            &BbProjectId::parse("proj_1").unwrap(),
        );
        let args = spawn_args(&SpawnRequest {
            wave: WaveSlug::parse("w-prov").unwrap(),
            role: RoleName::parse("auditor").unwrap(),
            attempt: 1,
            project: BbProjectId::parse("proj_1").unwrap(),
            agent: AgentSpec {
                provider: ProviderId::parse("claude-code").unwrap(),
                model: Some(ModelId::parse("claude-opus-5-5").unwrap()),
                account: Some("enterprise".into()),
                reasoning: None,
                service_tier: None,
                permission: PermissionMode::Full,
                workspace: Workspace::ProjectDefault,
            },
            room,
            prompt: Body::parse("audit").unwrap(),
            title: "[w-prov] @auditor".into(),
            run_stamp: "w-prov:auditor:attempt-1:at-1000".into(),
            output_dir: Some(PathBuf::from("runs/w-prov/auditor")),
            writes: vec![PathBuf::from("reports/a.md")],
            return_schema: None,
        });
        assert!(
            args.get("sessionOptions").is_none(),
            "account must not be injected as an undeclared BB provider sessionOption"
        );
        assert_eq!(
            args["pluginMetadata"]["rexWave"]["runStamp"],
            "w-prov:auditor:attempt-1:at-1000"
        );
        assert_eq!(
            args["pluginMetadata"]["rexWave"]["outputDir"],
            "runs/w-prov/auditor"
        );
        assert_eq!(args["pluginMetadata"]["rexWave"]["account"], "enterprise");
    }

    #[tokio::test]
    async fn bb_reconnect_reconcile_preserves_refused_and_flags_missing_threads() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let token = "tok_reconcile_honesty";
        let server_task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let ws = ws::WsConn::accept(tcp, token).await.unwrap();
            let (mut ws_reader, mut ws_writer) = ws.into_split();
            ws_writer
                .send_text(
                    &serde_json::to_string(&protocol::Up::Hello {
                        plugin_version: "0.1.0".into(),
                        sdk_version: "0.6.132".into(),
                    })
                    .unwrap(),
                )
                .await
                .unwrap();
            while let Ok(Some(ws::WsInbound::Text(raw))) = ws_reader.recv().await {
                let down: protocol::Down = serde_json::from_str(&raw).unwrap();
                if let protocol::Down::EventsPage {
                    req_id, thread_id, ..
                } = down
                {
                    let reply = if thread_id == "th_reconcile_refused" {
                        // Return newest-first BB-shaped failed refusal turn rows.
                        protocol::Up::Result {
                            req_id,
                            ok: true,
                            value: serde_json::json!([
                                {
                                    "seq": 3,
                                    "type": "turn/completed",
                                    "data": { "status": "failed", "error": { "message": "turn failed" } }
                                },
                                {
                                    "seq": 2,
                                    "type": "provider/error",
                                    "data": {
                                        "message": "policy refused",
                                        "errorInfo": { "category": "policy", "providerCode": "cyber_safeguard", "httpStatusCode": 400 }
                                    }
                                },
                                {
                                    "seq": 1,
                                    "type": "turn/started",
                                    "data": {}
                                }
                            ]),
                        }
                    } else {
                        protocol::Up::Result {
                            req_id,
                            ok: false,
                            value: serde_json::Value::String("thread not found".into()),
                        }
                    };
                    let _ = ws_writer
                        .send_text(&serde_json::to_string(&reply).unwrap())
                        .await;
                }
            }
        });

        let bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: format!("ws://{addr}/bridge"),
                token: token.into(),
            },
            None,
        );
        let (harness, _frame_rx) = LoopbackHarness::new(bridge.clone());
        let op = harness.client(Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        });
        let sys = harness.client(Principal::System {
            part: SystemPart::BbBridge,
        });

        let wave = WaveSlug::parse("reconcile-honesty").unwrap();
        let ref_role = RoleName::parse("refused-w").unwrap();
        let gone_role = RoleName::parse("gone-w").unwrap();
        let spec = WaveSpec {
            slug: wave.clone(),
            project: BbProjectId::parse("proj_1").unwrap(),
            goal: Body::parse("reconcile").unwrap(),
            output_dir: None,
            defaults: AgentSpec {
                provider: ProviderId::parse("claude-code").unwrap(),
                model: None,
                account: None,
                reasoning: None,
                service_tier: None,
                permission: PermissionMode::Full,
                workspace: Workspace::ProjectDefault,
            },
            workers: vec![
                WorkerSpec {
                    role: ref_role.clone(),
                    brief: Body::parse("a").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                },
                WorkerSpec {
                    role: gone_role.clone(),
                    brief: Body::parse("b").unwrap(),
                    agent: None,
                    output_dir: None,
                    writes: Vec::new(),
                    return_schema: None,
                },
            ],
        };
        op.call(WaveStart { spec, idem: None }).await.unwrap();
        sys.call(WaveObserve {
            wave: wave.clone(),
            role: ref_role.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: BbThreadId::parse("th_reconcile_refused").unwrap(),
            },
        })
        .await
        .unwrap();
        sys.call(WaveObserve {
            wave: wave.clone(),
            role: gone_role.clone(),
            observation: Observation::Spawned {
                attempt: 1,
                thread: BbThreadId::parse("th_reconcile_gone").unwrap(),
            },
        })
        .await
        .unwrap();

        // Simulate restart: bridge learns live workers from journal replay (cursor_seq == None).
        bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: ref_role.clone(),
                observation: Observation::Spawned {
                    attempt: 1,
                    thread: BbThreadId::parse("th_reconcile_refused").unwrap(),
                },
            },
        );
        bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: gone_role.clone(),
                observation: Observation::Spawned {
                    attempt: 1,
                    thread: BbThreadId::parse("th_reconcile_gone").unwrap(),
                },
            },
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let bridge_runner = tokio::spawn({
            let bridge = bridge.clone();
            let harness = harness.clone();
            async move { bridge.run_until(harness, shutdown_rx).await }
        });

        let snap = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let s = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                let ref_ok = s
                    .workers
                    .iter()
                    .find(|(r, _, _)| r == &ref_role)
                    .is_some_and(|(_, _, p)| p.refusal().is_some());
                let gone_ok = s
                    .workers
                    .iter()
                    .find(|(r, _, _)| r == &gone_role)
                    .is_some_and(|(_, _, p)| {
                        p.error()
                            .is_some_and(|e| e.contains("missing after restart"))
                    });
                if ref_ok && gone_ok {
                    break s;
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        })
        .await
        .expect("reconcile_live_workers must observe BB-shaped Refused and missing-thread Errored");

        assert_eq!(snap.counts.refused, 1);
        assert_eq!(snap.counts.errored, 1);
        assert_eq!(snap.counts.idle, 0);

        let _ = shutdown_tx.send(true);
        let _ = bridge_runner.await;
        server_task.abort();
    }
}
