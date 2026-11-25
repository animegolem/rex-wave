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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
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
        for row in slice.iter().rev() {
            if !is_retrying_provider_error(row)
                && let Some(reason) = extract_refusal_from_row(row)
            {
                return Some(Activity::Refused { reason });
            }
        }
        let data = tc.get("data").unwrap_or(&serde_json::Value::Null);
        if data.get("status").and_then(|v| v.as_str()) == Some("failed") {
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
    turn_start_seq: Option<u64>,
    turn_end_seq: u64,
    turn_start_at: u64,
    turn_end_at: u64,
    text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TurnMirrorCheck {
    Ignore,
    PostedViaTool {
        turn_start_at: u64,
        turn_end_at: u64,
        turn_end_seq: u64,
    },
    Unposted(CompletedTurn),
}

const MAX_TURN_BACKSCAN_PAGES: usize = 16_384;

fn sort_and_dedup_thread_rows(rows: &[serde_json::Value]) -> Vec<&serde_json::Value> {
    let mut ordered: Vec<&serde_json::Value> = rows.iter().collect();
    ordered.sort_by_key(|r| r.get("seq").and_then(|v| v.as_u64()).unwrap_or(0));
    ordered.dedup_by(|a, b| {
        let sa = a.get("seq").and_then(|v| v.as_u64());
        let sb = b.get("seq").and_then(|v| v.as_u64());
        sa.is_some() && sa == sb
    });
    ordered
}

fn is_failed_completion_status(status: Option<&str>) -> bool {
    matches!(
        status,
        Some("failed" | "cancelled" | "canceled" | "interrupted" | "aborted")
    )
}

fn is_nonmirrorable_completion_row(row: &serde_json::Value) -> bool {
    let status = row
        .get("data")
        .and_then(|d| d.get("status"))
        .and_then(|v| v.as_str());
    is_failed_completion_status(status) || extract_refusal_from_row(row).is_some()
}

fn turn_tail_end_exclusive(ordered: &[&serde_json::Value], end_idx: usize) -> usize {
    ordered[end_idx + 1..]
        .iter()
        .position(|r| {
            matches!(
                r.get("type").and_then(|v| v.as_str()),
                Some("turn/started" | "client/turn/start" | "turn/completed")
            )
        })
        .map_or(ordered.len(), |rel| end_idx + 1 + rel)
}

fn is_nonmirrorable_turn_window(turn_window: &[&serde_json::Value]) -> bool {
    let Some(end_idx) = turn_window
        .iter()
        .rposition(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"))
    else {
        return true;
    };
    if is_nonmirrorable_completion_row(turn_window[end_idx]) {
        return true;
    }
    let start_idx = turn_window[..=end_idx]
        .iter()
        .rposition(|r| {
            matches!(
                r.get("type").and_then(|v| v.as_str()),
                Some("turn/started" | "client/turn/start")
            )
        })
        .unwrap_or(0);
    turn_window[start_idx..]
        .iter()
        .any(|r| !is_retrying_provider_error(r) && extract_refusal_from_row(r).is_some())
}

fn is_turn_window_suppressed(
    turn_window: &[&serde_json::Value],
    suppressed_starts: &BTreeSet<u64>,
    suppressed_ends: &BTreeSet<u64>,
) -> bool {
    if is_nonmirrorable_turn_window(turn_window) {
        return true;
    }
    let Some(end_idx) = turn_window
        .iter()
        .rposition(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"))
    else {
        return true;
    };
    if let Some(end_seq) = turn_window[end_idx].get("seq").and_then(|v| v.as_u64())
        && suppressed_ends.contains(&end_seq)
    {
        return true;
    }
    if turn_window[..=end_idx].iter().any(|r| {
        matches!(
            r.get("type").and_then(|v| v.as_str()),
            Some("turn/started" | "client/turn/start")
        ) && r
            .get("seq")
            .and_then(|v| v.as_u64())
            .is_some_and(|s| suppressed_starts.contains(&s))
    }) {
        return true;
    }
    false
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BbTurnSegment {
    first_seq: u64,
    start_seqs: Vec<u64>,
    end_seq: Option<u64>,
    max_seq: u64,
}

fn extract_bb_turn_segments(rows: &[serde_json::Value]) -> Vec<BbTurnSegment> {
    let ordered = sort_and_dedup_thread_rows(rows);
    if ordered.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut win_start = 0usize;
    let mut saw_client_start = false;
    let mut saw_turn_started = false;
    let mut saw_body = false;

    let push_window = |slice: &[&serde_json::Value], acc: &mut Vec<BbTurnSegment>| {
        if slice.is_empty() {
            return;
        }
        let end_idx = slice
            .iter()
            .rposition(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"));
        let end_seq = end_idx
            .and_then(|ei| slice[ei].get("seq").and_then(|v| v.as_u64()))
            .filter(|&s| s > 0);
        let search_slice = match end_idx {
            Some(ei) => &slice[..=ei],
            None => slice,
        };
        let mut start_seqs: Vec<u64> = search_slice
            .iter()
            .filter(|r| {
                matches!(
                    r.get("type").and_then(|v| v.as_str()),
                    Some("turn/started" | "client/turn/start")
                )
            })
            .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()).filter(|&s| s > 0))
            .collect();
        start_seqs.sort_unstable();
        start_seqs.dedup();
        let first_seq = slice
            .iter()
            .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()).filter(|&s| s > 0))
            .min()
            .unwrap_or(0);
        let max_seq = slice
            .iter()
            .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()))
            .max()
            .unwrap_or(0);
        if max_seq > 0 && (!start_seqs.is_empty() || end_seq.is_some()) {
            acc.push(BbTurnSegment {
                first_seq: first_seq.max(1),
                start_seqs,
                end_seq,
                max_seq,
            });
        }
    };

    let mut idx = 0usize;
    while idx < ordered.len() {
        let ty = ordered[idx].get("type").and_then(|v| v.as_str());
        match ty {
            Some("client/turn/start") => {
                if idx > win_start && (saw_body || saw_client_start || saw_turn_started) {
                    push_window(&ordered[win_start..idx], &mut out);
                    win_start = idx;
                    saw_turn_started = false;
                    saw_body = false;
                }
                saw_client_start = true;
                idx += 1;
            }
            Some("turn/started") => {
                if idx > win_start && (saw_body || saw_turn_started) {
                    push_window(&ordered[win_start..idx], &mut out);
                    win_start = idx;
                    saw_client_start = false;
                    saw_body = false;
                }
                saw_turn_started = true;
                idx += 1;
            }
            Some("turn/completed") => {
                let tail_excl = turn_tail_end_exclusive(&ordered, idx);
                push_window(&ordered[win_start..tail_excl], &mut out);
                win_start = tail_excl;
                saw_client_start = false;
                saw_turn_started = false;
                saw_body = false;
                idx = tail_excl;
            }
            _ => {
                saw_body = true;
                idx += 1;
            }
        }
    }
    if win_start < ordered.len() {
        push_window(&ordered[win_start..], &mut out);
    }
    out
}

fn is_segment_already_suppressed(
    seg: &BbTurnSegment,
    suppressed_starts: &BTreeSet<u64>,
    suppressed_ends: &BTreeSet<u64>,
) -> bool {
    seg.end_seq.is_some_and(|e| suppressed_ends.contains(&e))
        || seg.start_seqs.iter().any(|s| suppressed_starts.contains(s))
}

fn has_unsuppressed_turn_after(
    rows: &[serde_json::Value],
    suppressed_starts: &BTreeSet<u64>,
    suppressed_ends: &BTreeSet<u64>,
    floor: Option<u64>,
) -> bool {
    let Some(fseq) = floor else {
        return false;
    };
    extract_bb_turn_segments(rows).iter().any(|seg| {
        !is_segment_already_suppressed(seg, suppressed_starts, suppressed_ends)
            && seg.first_seq > fseq
    })
}

fn rows_before_terminal_turn(rows: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let ordered = sort_and_dedup_thread_rows(rows);
    if ordered.is_empty() {
        return Vec::new();
    }
    let last_completed = ordered
        .iter()
        .rposition(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"));
    let cut_idx = match last_completed {
        Some(end_idx) => {
            if let Some(after_start_rel) = ordered[end_idx + 1..].iter().position(|r| {
                matches!(
                    r.get("type").and_then(|v| v.as_str()),
                    Some("turn/started" | "client/turn/start")
                )
            }) {
                end_idx + 1 + after_start_rel
            } else {
                let prev_end = ordered[..end_idx].iter().rposition(|r| {
                    r.get("type").and_then(|v| v.as_str()) == Some("turn/completed")
                });
                let search_from = prev_end.map_or(0, |i| i + 1);
                ordered[search_from..=end_idx]
                    .iter()
                    .rposition(|r| {
                        matches!(
                            r.get("type").and_then(|v| v.as_str()),
                            Some("turn/started" | "client/turn/start")
                        )
                    })
                    .map_or(search_from, |rel| search_from + rel)
            }
        }
        None => 0,
    };
    let end_exclusive = if cut_idx < ordered.len()
        && matches!(
            ordered[cut_idx].get("type").and_then(|v| v.as_str()),
            Some("turn/started" | "client/turn/start")
        ) {
        cut_idx + 1
    } else {
        cut_idx
    };
    ordered[..end_exclusive]
        .iter()
        .map(|r| (*r).clone())
        .collect()
}

fn strict_rows_before_terminal_turn(rows: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let mut prior = rows_before_terminal_turn(rows);
    if prior.last().is_some_and(|r| {
        matches!(
            r.get("type").and_then(|v| v.as_str()),
            Some("turn/started" | "client/turn/start")
        )
    }) {
        prior.pop();
    }
    prior
}

fn latest_nonfailed_turn_end_seq(rows: &[serde_json::Value]) -> Option<u64> {
    if rows.is_empty() {
        return None;
    }
    let ordered = sort_and_dedup_thread_rows(rows);
    let end_idx = ordered
        .iter()
        .rposition(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"))?;
    let turn_floor_idx = ordered[..end_idx]
        .iter()
        .rposition(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"))
        .map_or(0, |idx| idx + 1);
    let tail_excl = turn_tail_end_exclusive(&ordered, end_idx);
    if is_nonmirrorable_turn_window(&ordered[turn_floor_idx..tail_excl]) {
        return None;
    }
    Some(
        ordered[end_idx]
            .get("seq")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
    )
}

fn mirrorable_turn_completions_in_order(
    rows: &[serde_json::Value],
    suppressed_starts: &BTreeSet<u64>,
    suppressed_ends: &BTreeSet<u64>,
) -> Vec<(u64, Option<u64>)> {
    if rows.is_empty() {
        return Vec::new();
    }
    let ordered = sort_and_dedup_thread_rows(rows);
    let mut segments: Vec<(usize, usize)> = Vec::new();
    let mut seg_start = 0usize;
    for (idx, row) in ordered.iter().enumerate() {
        if row.get("type").and_then(|v| v.as_str()) == Some("turn/completed") {
            segments.push((seg_start, idx));
            seg_start = idx + 1;
        }
    }
    let trailing_start_at = ordered[seg_start..]
        .iter()
        .position(|r| {
            matches!(
                r.get("type").and_then(|v| v.as_str()),
                Some("turn/started" | "client/turn/start")
            )
        })
        .and_then(|rel| {
            ordered[seg_start + rel..]
                .iter()
                .filter_map(|r| r.get("createdAt").and_then(|v| v.as_u64()))
                .find(|&at| at > 0)
        });
    let mut out = Vec::new();
    for (seg_idx, &(s_idx, end_idx)) in segments.iter().enumerate() {
        let tail_excl = turn_tail_end_exclusive(&ordered, end_idx);
        if is_turn_window_suppressed(
            &ordered[s_idx..tail_excl],
            suppressed_starts,
            suppressed_ends,
        ) {
            continue;
        }
        let end_row = ordered[end_idx];
        let end_seq = end_row.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
        let next_start_at = segments
            .get(seg_idx + 1)
            .and_then(|&(ns, ne)| {
                let from = ordered[ns..=ne]
                    .iter()
                    .position(|r| {
                        matches!(
                            r.get("type").and_then(|v| v.as_str()),
                            Some("turn/started" | "client/turn/start")
                        )
                    })
                    .map_or(ns, |rel| ns + rel);
                ordered[from..=ne]
                    .iter()
                    .filter_map(|r| r.get("createdAt").and_then(|v| v.as_u64()))
                    .find(|&at| at > 0)
            })
            .or(trailing_start_at);
        out.push((end_seq, next_start_at));
    }
    out
}

fn latest_completed_turn_missing_start_boundary(rows: &[serde_json::Value]) -> Option<u64> {
    if rows.is_empty() {
        return None;
    }
    let ordered = sort_and_dedup_thread_rows(rows);
    let end_idx = ordered
        .iter()
        .rposition(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"))?;
    let prev_end_idx = ordered[..end_idx]
        .iter()
        .rposition(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"));
    if prev_end_idx.is_some() {
        return None;
    }
    let tail_excl = turn_tail_end_exclusive(&ordered, end_idx);
    if is_nonmirrorable_turn_window(&ordered[..tail_excl]) {
        return None;
    }
    let has_start = ordered[..=end_idx].iter().any(|r| {
        matches!(
            r.get("type").and_then(|v| v.as_str()),
            Some("turn/started" | "client/turn/start")
        )
    });
    if has_start {
        return None;
    }
    ordered
        .first()
        .and_then(|r| r.get("seq").and_then(|v| v.as_u64()))
        .filter(|&s| s > 1)
}

fn inspect_turn_window(turn_window: &[&serde_json::Value]) -> TurnMirrorCheck {
    let Some(end_idx) = turn_window
        .iter()
        .rposition(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"))
    else {
        return TurnMirrorCheck::Ignore;
    };
    if is_nonmirrorable_turn_window(turn_window) {
        return TurnMirrorCheck::Ignore;
    }
    let end_row = turn_window[end_idx];
    let body_window = &turn_window[..=end_idx];
    let turn_end_seq = end_row.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);

    let start_idx = body_window.iter().rposition(|r| {
        matches!(
            r.get("type").and_then(|v| v.as_str()),
            Some("turn/started" | "client/turn/start")
        )
    });
    let (turn_start_seq, turn_start_at, slice) = match start_idx {
        Some(idx) => {
            let seq = body_window[idx]
                .get("seq")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let at = body_window[idx]
                .get("createdAt")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            (Some(seq), at, &body_window[idx..])
        }
        None => (None, 0, body_window),
    };

    let turn_end_at = end_row
        .get("createdAt")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

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
                return TurnMirrorCheck::PostedViaTool {
                    turn_start_at,
                    turn_end_at,
                    turn_end_seq,
                };
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

    let anchor = if turn_end_seq > 0 {
        format!("e{turn_end_seq}")
    } else if let Some(s) = turn_start_seq {
        format!("s{s}")
    } else {
        "e0".to_owned()
    };

    TurnMirrorCheck::Unposted(CompletedTurn {
        anchor,
        turn_start_seq,
        turn_end_seq,
        turn_start_at,
        turn_end_at,
        text,
    })
}

fn inspect_latest_completed_turn(rows: &[serde_json::Value]) -> TurnMirrorCheck {
    if rows.is_empty() {
        return TurnMirrorCheck::Ignore;
    }
    let ordered = sort_and_dedup_thread_rows(rows);
    let Some(end_idx) = ordered
        .iter()
        .rposition(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"))
    else {
        return TurnMirrorCheck::Ignore;
    };
    let turn_floor_idx = ordered[..end_idx]
        .iter()
        .rposition(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"))
        .map_or(0, |idx| idx + 1);
    let tail_excl = turn_tail_end_exclusive(&ordered, end_idx);
    inspect_turn_window(&ordered[turn_floor_idx..tail_excl])
}

fn inspect_all_unposted_completed_turns(
    rows: &[serde_json::Value],
    through_end_seq: u64,
) -> Vec<(CompletedTurn, Option<u64>)> {
    let ordered = sort_and_dedup_thread_rows(rows);
    let mut segments: Vec<(usize, usize)> = Vec::new();
    let mut seg_start = 0usize;
    for (idx, row) in ordered.iter().enumerate() {
        if row.get("type").and_then(|v| v.as_str()) == Some("turn/completed") {
            let seq = row.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
            if through_end_seq > 0 && seq > through_end_seq {
                break;
            }
            segments.push((seg_start, idx));
            seg_start = idx + 1;
        }
    }
    let mut out = Vec::new();
    for (seg_idx, &(s, e)) in segments.iter().enumerate() {
        let tail_excl = turn_tail_end_exclusive(&ordered, e);
        if let TurnMirrorCheck::Unposted(turn) = inspect_turn_window(&ordered[s..tail_excl]) {
            let next_seg_start_at = segments.get(seg_idx + 1).and_then(|&(ns, ne)| {
                ordered[ns..=ne]
                    .iter()
                    .filter_map(|r| r.get("createdAt").and_then(|v| v.as_u64()))
                    .find(|&at| at > 0)
            });
            out.push((turn, next_seg_start_at));
        }
    }
    out
}

struct WorkerRoomPost {
    at: UnixMs,
    body: String,
    idem: Option<IdemKey>,
    is_mirrored: bool,
}

fn reconcile_shared_legacy_mirror(
    wave: &WaveSlug,
    role: &RoleName,
    thread: &BbThreadId,
    turns: &[(CompletedTurn, Option<u64>)],
    posts: &[WorkerRoomPost],
    target_end_seq: u64,
) -> bool {
    let Some(target_idx) = turns
        .iter()
        .rposition(|(t, _)| t.turn_end_seq == target_end_seq)
    else {
        return false;
    };
    let mut turn_matched = vec![false; turns.len()];
    let mut post_used = vec![false; posts.len()];

    for (ti, (turn, next_start_at)) in turns.iter().enumerate() {
        let exact_end = rex_wave_waves::mirror_idem_key(wave, role, thread, &turn.anchor).ok();
        let exact_start = turn.turn_start_seq.and_then(|s| {
            rex_wave_waves::mirror_idem_key(wave, role, thread, &format!("s{s}")).ok()
        });
        for (pi, post) in posts.iter().enumerate() {
            if post_used[pi] {
                continue;
            }
            let exact_idem_hit = post.idem.is_some()
                && (post.idem == exact_end || (exact_start.is_some() && post.idem == exact_start));
            let direct_hit = !post.is_mirrored
                && (turn.turn_start_at == 0 || post.at.0 >= turn.turn_start_at)
                && (turn.turn_end_at == 0 || post.at.0 <= turn.turn_end_at)
                && next_start_at.is_none_or(|ns| post.at.0 < ns);
            if exact_idem_hit || direct_hit {
                turn_matched[ti] = true;
                post_used[pi] = true;
                break;
            }
        }
    }
    if turn_matched[target_idx] {
        return true;
    }

    for (ti, (turn, _)) in turns.iter().enumerate() {
        if turn_matched[ti] {
            continue;
        }
        let trimmed = truncate_with_notice(&turn.text, 64 * 1024);
        let min_at = if turn.turn_start_at > 0 {
            turn.turn_start_at
        } else {
            turn.turn_end_at
        };
        for (pi, post) in posts.iter().enumerate() {
            if post_used[pi] || !post.is_mirrored {
                continue;
            }
            let Some(ref existing_idem) = post.idem else {
                continue;
            };
            if rex_wave_waves::classify_legacy_bb_mirror_idem(
                existing_idem,
                wave,
                role,
                thread,
                turn.turn_start_seq,
                turn.turn_end_seq,
            ) == Some(false)
                && post.body == trimmed
                && (min_at == 0 || post.at.0 >= min_at)
            {
                turn_matched[ti] = true;
                post_used[pi] = true;
                break;
            }
        }
    }
    turn_matched[target_idx]
}

#[cfg(test)]
fn extract_unposted_final_turn(rows: &[serde_json::Value]) -> Option<CompletedTurn> {
    match inspect_latest_completed_turn(rows) {
        TurnMirrorCheck::Unposted(turn) => Some(turn),
        TurnMirrorCheck::Ignore | TurnMirrorCheck::PostedViaTool { .. } => None,
    }
}

use rex_wave_waves::default_bb_data_dir;

fn is_bb_thread_id(thread: &BbThreadId) -> bool {
    thread.runner_kind() == RunnerKind::Bb
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

pub use rex_wave_runtime::BridgeBackend;

trait ErasedBackend: Send + Sync {
    fn call_as(&self, me: Principal, cmd: WireCommand) -> BoxFut<'_, Result<WireReply, ApiError>>;
    fn handles_worker_wait_parking(&self) -> bool;
    fn parked_scope(
        &self,
        worker: &Principal,
        wait: &rex_wave_api::read::Wait,
    ) -> Option<(Option<RoomId>, Seq)>;
}

impl<B: BridgeBackend> ErasedBackend for B {
    fn call_as(&self, me: Principal, cmd: WireCommand) -> BoxFut<'_, Result<WireReply, ApiError>> {
        let client = self.client(me);
        Box::pin(async move { client.call_wire(cmd).await })
    }
    fn handles_worker_wait_parking(&self) -> bool {
        BridgeBackend::handles_worker_wait_parking(self)
    }
    fn parked_scope(
        &self,
        worker: &Principal,
        wait: &rex_wave_api::read::Wait,
    ) -> Option<(Option<RoomId>, Seq)> {
        self.parked_room_and_since(worker, wait)
    }
}

#[derive(Clone, Debug)]
pub struct BridgeConfig {
    pub url: String,
    pub token: String,
}

#[derive(Clone, Debug)]
struct BbFailureTicket {
    id: u64,
    baseline_seq: Option<u64>,
    turns_started_after: usize,
    saved_in_journal: bool,
    activity: Option<Activity>,
    needs_activity_commit: bool,
}

fn format_bb_ticket_key(id: u64, baseline_seq: Option<u64>) -> String {
    match baseline_seq.filter(|&b| b > 0) {
        Some(b) => format!("ft:{id}:{b}"),
        None => format!("ft:{id}:any"),
    }
}

fn parse_bb_ticket_key(key: &str) -> Option<(u64, Option<u64>)> {
    let rest = key.strip_prefix("ft:")?;
    let (id_str, base_str) = rest.split_once(':')?;
    let id = id_str.parse::<u64>().ok().filter(|&n| n > 0)?;
    if base_str == "any" {
        Some((id, None))
    } else {
        let base = base_str.parse::<u64>().ok()?;
        Some((id, (base > 0).then_some(base)))
    }
}

fn parse_bb_ticket_resolved_key(key: &str) -> Option<(u64, u64)> {
    let rest = key.strip_prefix("fr:")?;
    let (id_str, seq_str) = rest.split_once(':')?;
    let id = id_str.parse::<u64>().ok().filter(|&n| n > 0)?;
    let seq = seq_str.parse::<u64>().ok()?;
    Some((id, seq))
}

#[derive(Clone, Debug, Default)]
struct HydratedSuppressionData {
    starts: BTreeSet<u64>,
    ends: BTreeSet<u64>,
    anchored_floor: Option<u64>,
    max_ticket_id: u64,
    resolved_ticket_ids: BTreeSet<u64>,
    open_tickets: Vec<BbFailureTicket>,
}

#[derive(Clone, Debug)]
struct FlushedSuppressionState {
    starts: BTreeSet<u64>,
    ends: BTreeSet<u64>,
    unsaved_failure: Option<(Activity, Option<u64>)>,
    anchored_failure_floor: Option<u64>,
    open_tickets: Vec<BbFailureTicket>,
}

#[derive(Clone, Debug)]
struct TrackedWorker {
    attempt: u32,
    thread: Option<BbThreadId>,
    live: bool,
    activity: Activity,
    pending_interaction: Option<PendingInteraction>,
    last_seq: Option<u64>,
    feed_seq: Option<u64>,
    pending_mirror_since: Option<u64>,
    mirror_room_watermark: Seq,
    last_mirrored_end_seq: u64,
    last_settled_turn_seq: u64,
    last_mirrored_anchor: Option<String>,
    claimed_direct_posts: BTreeSet<Seq>,
    suppressed_start_seqs: BTreeSet<u64>,
    suppressed_end_seqs: BTreeSet<u64>,
    unsaved_suppressed_starts: BTreeSet<u64>,
    unsaved_suppressed_ends: BTreeSet<u64>,
    unsaved_floor: Option<u64>,
    unsaved_failure: Option<(Activity, Option<u64>)>,
    anchored_failure_floor: Option<u64>,
    next_ticket_id: u64,
    resolved_ticket_ids: BTreeSet<u64>,
    open_tickets: Vec<BbFailureTicket>,
    hydrated_suppressions: bool,
}

impl TrackedWorker {
    fn new_empty(attempt: u32, thread: Option<BbThreadId>, live: bool, activity: Activity) -> Self {
        let pending_interaction = match &activity {
            Activity::Blocked { interaction } => Some(interaction.clone()),
            _ => None,
        };
        TrackedWorker {
            attempt,
            thread,
            live,
            activity,
            pending_interaction,
            last_seq: None,
            feed_seq: None,
            pending_mirror_since: None,
            mirror_room_watermark: Seq::ORIGIN,
            last_mirrored_end_seq: 0,
            last_settled_turn_seq: 0,
            last_mirrored_anchor: None,
            claimed_direct_posts: BTreeSet::new(),
            suppressed_start_seqs: BTreeSet::new(),
            suppressed_end_seqs: BTreeSet::new(),
            unsaved_suppressed_starts: BTreeSet::new(),
            unsaved_suppressed_ends: BTreeSet::new(),
            unsaved_floor: None,
            unsaved_failure: None,
            anchored_failure_floor: None,
            next_ticket_id: 1,
            resolved_ticket_ids: BTreeSet::new(),
            open_tickets: Vec::new(),
            hydrated_suppressions: false,
        }
    }

    fn reset_for_new_thread(&mut self) {
        self.last_seq = None;
        self.feed_seq = None;
        self.pending_mirror_since = None;
        self.mirror_room_watermark = Seq::ORIGIN;
        self.last_mirrored_end_seq = 0;
        self.last_settled_turn_seq = 0;
        self.last_mirrored_anchor = None;
        self.claimed_direct_posts.clear();
        self.suppressed_start_seqs.clear();
        self.suppressed_end_seqs.clear();
        self.unsaved_suppressed_starts.clear();
        self.unsaved_suppressed_ends.clear();
        self.unsaved_floor = None;
        self.unsaved_failure = None;
        self.anchored_failure_floor = None;
        self.next_ticket_id = 1;
        self.resolved_ticket_ids.clear();
        self.open_tickets.clear();
        self.hydrated_suppressions = false;
    }

    fn settled_seq_floor(&self) -> Option<u64> {
        let max_settled = [
            self.last_mirrored_end_seq,
            self.last_settled_turn_seq,
            self.anchored_failure_floor.unwrap_or(0),
            self.suppressed_end_seqs.last().copied().unwrap_or(0),
            self.unsaved_suppressed_ends.last().copied().unwrap_or(0),
            self.suppressed_start_seqs.last().copied().unwrap_or(0),
            self.unsaved_suppressed_starts.last().copied().unwrap_or(0),
        ]
        .into_iter()
        .max()
        .unwrap_or(0);
        (max_settled > 0).then_some(max_settled)
    }

    fn fold_suppression_key(&mut self, k: &str) {
        if let Some((id, base)) = parse_bb_ticket_key(k) {
            if id >= self.next_ticket_id {
                self.next_ticket_id = id + 1;
            }
            if !self.resolved_ticket_ids.contains(&id) {
                if let Some(existing) = self.open_tickets.iter_mut().find(|t| t.id == id) {
                    existing.saved_in_journal = true;
                    if existing.baseline_seq.is_none() {
                        existing.baseline_seq = base;
                    }
                } else {
                    self.open_tickets.push(BbFailureTicket {
                        id,
                        baseline_seq: base,
                        turns_started_after: 0,
                        saved_in_journal: true,
                        activity: None,
                        needs_activity_commit: false,
                    });
                    self.open_tickets.sort_by_key(|t| t.id);
                }
            }
        } else if let Some((id, res_seq)) = parse_bb_ticket_resolved_key(k) {
            if id >= self.next_ticket_id {
                self.next_ticket_id = id + 1;
            }
            self.resolved_ticket_ids.insert(id);
            self.open_tickets.retain(|t| t.id != id);
            self.anchored_failure_floor = Some(
                self.anchored_failure_floor
                    .map_or(res_seq, |a| a.max(res_seq)),
            );
            for rem in &mut self.open_tickets {
                if rem.id > id {
                    rem.baseline_seq = Some(rem.baseline_seq.map_or(res_seq, |b| b.max(res_seq)));
                }
            }
        } else if let Some(f) = k.strip_prefix('f').and_then(|n| n.parse::<u64>().ok()) {
            if self.unsaved_floor.is_some_and(|uf| uf <= f) {
                self.unsaved_floor = None;
            }
            self.anchored_failure_floor = Some(self.anchored_failure_floor.map_or(f, |a| a.max(f)));
        } else if let Some(s) = k
            .strip_prefix('s')
            .and_then(|n| n.parse::<u64>().ok())
            .filter(|&n| n > 0)
        {
            self.unsaved_suppressed_starts.remove(&s);
            self.suppressed_start_seqs.insert(s);
        } else if let Some(e) = k
            .strip_prefix('e')
            .and_then(|n| n.parse::<u64>().ok())
            .filter(|&n| n > 0)
        {
            self.unsaved_suppressed_ends.remove(&e);
            self.suppressed_end_seqs.insert(e);
        }
    }

    fn allocate_or_merge_ticket(
        &mut self,
        baseline_override: Option<Option<u64>>,
        activity: Activity,
        needs_activity_commit: bool,
    ) -> (u64, Option<u64>, bool) {
        let computed_base = baseline_override.unwrap_or_else(|| self.settled_seq_floor());
        if let Some(last) = self.open_tickets.last_mut()
            && last.turns_started_after == 0
        {
            if last.baseline_seq.is_none() {
                last.baseline_seq = computed_base;
            }
            let preserve_refusal = matches!(last.activity, Some(Activity::Refused { .. }))
                && matches!(activity, Activity::Errored { .. });
            if !preserve_refusal {
                last.activity = Some(activity);
            }
            last.needs_activity_commit |= needs_activity_commit;
            let fallback = self.last_mirrored_end_seq.saturating_add(1);
            self.pending_mirror_since = Some(
                self.pending_mirror_since
                    .map_or(fallback, |prev| prev.min(fallback)),
            );
            return (last.id, last.baseline_seq, last.saved_in_journal);
        }
        let id = self.next_ticket_id;
        self.next_ticket_id = self.next_ticket_id.saturating_add(1);
        self.open_tickets.push(BbFailureTicket {
            id,
            baseline_seq: computed_base,
            turns_started_after: 0,
            saved_in_journal: false,
            activity: Some(activity),
            needs_activity_commit,
        });
        let fallback = self.last_mirrored_end_seq.saturating_add(1);
        self.pending_mirror_since = Some(
            self.pending_mirror_since
                .map_or(fallback, |prev| prev.min(fallback)),
        );
        (id, computed_base, false)
    }
}

type MirrorStateReply = (Seq, bool, bool, BTreeSet<Seq>);

type PendingUnsavedReply = (
    BTreeSet<u64>,
    BTreeSet<u64>,
    Option<u64>,
    Option<(Activity, Option<u64>)>,
    Option<u64>,
    Vec<BbFailureTicket>,
);

type LiveWorkerSnapshot = (
    WaveSlug,
    RoleName,
    u32,
    BbThreadId,
    Activity,
    Option<u64>,
    Option<u64>,
    u64,
    BTreeSet<u64>,
    BTreeSet<u64>,
);

type MirrorCursorSnapshot = (
    Option<u64>,
    Option<u64>,
    u64,
    bool,
    BTreeSet<u64>,
    BTreeSet<u64>,
);

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
        reply: oneshot::Sender<Vec<LiveWorkerSnapshot>>,
    },
    WorkerMirrorCursor {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        reply: oneshot::Sender<MirrorCursorSnapshot>,
    },
    SuppressTurn {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        start_seqs: Vec<u64>,
        end_seq: Option<u64>,
        floor_seq: Option<u64>,
    },
    QueueUnsavedSuppressions {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        start_seqs: Vec<u64>,
        end_seq: Option<u64>,
        floor_seq: Option<u64>,
        failure: Option<(Activity, Option<u64>)>,
    },
    PendingUnsavedSuppressions {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        reply: oneshot::Sender<PendingUnsavedReply>,
    },
    AllocateOrMergeTicket {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        baseline_override: Option<Option<u64>>,
        activity: Activity,
        needs_activity_commit: bool,
        reply: oneshot::Sender<(u64, Option<u64>, bool)>,
    },
    MarkTicketSaved {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        ids: Vec<u64>,
    },
    ResolveTicket {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        id: u64,
        start_seqs: Vec<u64>,
        end_seq: Option<u64>,
        resolved_max_seq: u64,
        promote_unsaved_activity: Option<Activity>,
    },
    MarkTicketActivityCommitted {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
    },
    NoteTurnStartedAfterFailure {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
    },
    LookupFailureBaseline {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        reply: oneshot::Sender<(Option<u64>, Option<u64>, bool)>,
    },
    ClearUnsavedFailure {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        settled_seq: Option<u64>,
    },
    HydrationStatus {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        reply: oneshot::Sender<Option<(BTreeSet<u64>, BTreeSet<u64>)>>,
    },
    ApplyHydratedSuppressions {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        hydrated: HydratedSuppressionData,
        reply: oneshot::Sender<(BTreeSet<u64>, BTreeSet<u64>)>,
    },
    AdvanceSeq {
        thread: BbThreadId,
        seq: u64,
    },
    AdvanceFeedSeq {
        thread: BbThreadId,
        seq: u64,
    },
    SetPendingMirror {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        since_end_seq: Option<u64>,
        mark_unknown_dirty: bool,
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
    MirrorState {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        end_seq: u64,
        anchor: String,
        reply: oneshot::Sender<MirrorStateReply>,
    },
    AdvanceMirrorWatermark {
        wave: WaveSlug,
        role: RoleName,
        thread: BbThreadId,
        seq: Seq,
        end_seq: Option<u64>,
        anchor: Option<String>,
        claimed_seqs: Vec<Seq>,
        clear_pending: bool,
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
                                .and_then(|w| w.feed_seq.or(w.last_seq));
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
                    if !is_bb_thread_id(&thread) {
                        continue;
                    }
                    if let Some(prev) = self.workers_by_role.get(&(wave.clone(), role.clone()))
                        && let Some(ref prev_th) = prev.thread
                        && prev_th != &thread
                    {
                        self.role_by_thread.remove(prev_th);
                    }
                    self.role_by_thread
                        .insert(thread.clone(), (wave.clone(), role.clone()));
                    let entry = self.workers_by_role.entry((wave, role)).or_insert_with(|| {
                        TrackedWorker::new_empty(
                            attempt,
                            Some(thread.clone()),
                            true,
                            Activity::Working,
                        )
                    });
                    if entry.thread.as_ref() != Some(&thread) {
                        entry.reset_for_new_thread();
                    }
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
                        (rec.live && rec.thread.as_ref() == Some(&thread))
                            .then(|| (w.clone(), r.clone(), rec.attempt))
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
                            && is_bb_thread_id(th)
                        {
                            list.push((
                                w.clone(),
                                r.clone(),
                                rec.attempt,
                                th.clone(),
                                rec.activity.clone(),
                                rec.last_seq,
                                rec.pending_mirror_since,
                                rec.last_mirrored_end_seq,
                                rec.suppressed_start_seqs.clone(),
                                rec.suppressed_end_seqs.clone(),
                            ));
                        }
                    }
                    let _ = reply.send(list);
                }
                OwnerMsg::WorkerMirrorCursor {
                    wave,
                    role,
                    thread,
                    reply,
                } => {
                    let ever_connected = self.conn_id > 0;
                    let cur = match self.workers_by_role.get(&(wave, role)) {
                        Some(rec) if rec.thread.as_ref() == Some(&thread) => (
                            rec.last_seq,
                            rec.pending_mirror_since,
                            rec.last_mirrored_end_seq,
                            ever_connected,
                            rec.suppressed_start_seqs.clone(),
                            rec.suppressed_end_seqs.clone(),
                        ),
                        _ => (
                            None,
                            None,
                            0,
                            ever_connected,
                            BTreeSet::new(),
                            BTreeSet::new(),
                        ),
                    };
                    let _ = reply.send(cur);
                }
                OwnerMsg::SuppressTurn {
                    wave,
                    role,
                    thread,
                    start_seqs,
                    end_seq,
                    floor_seq,
                } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        for s in start_seqs {
                            if s > 0 {
                                rec.unsaved_suppressed_starts.remove(&s);
                                rec.suppressed_start_seqs.insert(s);
                            }
                        }
                        if let Some(e) = end_seq.filter(|&e| e > 0) {
                            rec.unsaved_suppressed_ends.remove(&e);
                            rec.suppressed_end_seqs.insert(e);
                        }
                        if let Some(f) = floor_seq {
                            if rec.unsaved_floor.is_some_and(|uf| uf <= f) {
                                rec.unsaved_floor = None;
                            }
                            rec.anchored_failure_floor =
                                Some(rec.anchored_failure_floor.map_or(f, |a| a.max(f)));
                        }
                    }
                }
                OwnerMsg::QueueUnsavedSuppressions {
                    wave,
                    role,
                    thread,
                    start_seqs,
                    end_seq,
                    floor_seq,
                    failure,
                } => {
                    if is_bb_thread_id(&thread)
                        && !self
                            .workers_by_role
                            .contains_key(&(wave.clone(), role.clone()))
                    {
                        self.workers_by_role.insert(
                            (wave.clone(), role.clone()),
                            TrackedWorker::new_empty(
                                1,
                                Some(thread.clone()),
                                false,
                                Activity::Working,
                            ),
                        );
                    }
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        for s in start_seqs {
                            if s > 0 && !rec.suppressed_start_seqs.contains(&s) {
                                rec.unsaved_suppressed_starts.insert(s);
                            }
                        }
                        if let Some(e) = end_seq.filter(|&e| e > 0)
                            && !rec.suppressed_end_seqs.contains(&e)
                        {
                            rec.unsaved_suppressed_ends.insert(e);
                        }
                        if let Some(f) = floor_seq {
                            rec.unsaved_floor =
                                Some(rec.unsaved_floor.map_or(f, |prev| prev.max(f)));
                            rec.anchored_failure_floor =
                                Some(rec.anchored_failure_floor.map_or(f, |a| a.max(f)));
                        }
                        if let Some((act, fseq)) = failure {
                            if let Some(b) = fseq {
                                rec.anchored_failure_floor =
                                    Some(rec.anchored_failure_floor.map_or(b, |a| a.max(b)));
                            }
                            let preserve_refusal =
                                matches!(rec.unsaved_failure, Some((Activity::Refused { .. }, _)))
                                    && matches!(act, Activity::Errored { .. });
                            if !preserve_refusal {
                                let merged_seq = match (
                                    rec.unsaved_failure.as_ref().and_then(|(_, s)| *s),
                                    fseq,
                                ) {
                                    (Some(a), Some(b)) => Some(a.max(b)),
                                    (None, b) => b,
                                    (a, None) => a,
                                };
                                rec.unsaved_failure = Some((act, merged_seq));
                            } else if let Some((_, ref mut prev_seq)) = rec.unsaved_failure
                                && let Some(b) = fseq
                            {
                                *prev_seq = Some(prev_seq.map_or(b, |a| a.max(b)));
                            }
                        }
                        let fallback = rec.last_mirrored_end_seq.saturating_add(1);
                        rec.pending_mirror_since = Some(
                            rec.pending_mirror_since
                                .map_or(fallback, |prev| prev.min(fallback)),
                        );
                    }
                }
                OwnerMsg::PendingUnsavedSuppressions {
                    wave,
                    role,
                    thread,
                    reply,
                } => {
                    let out = match self.workers_by_role.get(&(wave, role)) {
                        Some(rec) if rec.thread.as_ref() == Some(&thread) => (
                            rec.unsaved_suppressed_starts.clone(),
                            rec.unsaved_suppressed_ends.clone(),
                            rec.unsaved_floor,
                            rec.unsaved_failure.clone(),
                            rec.anchored_failure_floor,
                            rec.open_tickets.clone(),
                        ),
                        _ => (
                            BTreeSet::new(),
                            BTreeSet::new(),
                            None,
                            None,
                            None,
                            Vec::new(),
                        ),
                    };
                    let _ = reply.send(out);
                }
                OwnerMsg::AllocateOrMergeTicket {
                    wave,
                    role,
                    thread,
                    baseline_override,
                    activity,
                    needs_activity_commit,
                    reply,
                } => {
                    if is_bb_thread_id(&thread)
                        && !self
                            .workers_by_role
                            .contains_key(&(wave.clone(), role.clone()))
                    {
                        self.workers_by_role.insert(
                            (wave.clone(), role.clone()),
                            TrackedWorker::new_empty(
                                1,
                                Some(thread.clone()),
                                false,
                                Activity::Working,
                            ),
                        );
                    }
                    let out = if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        rec.allocate_or_merge_ticket(
                            baseline_override,
                            activity,
                            needs_activity_commit,
                        )
                    } else {
                        (1, baseline_override.flatten(), false)
                    };
                    let _ = reply.send(out);
                }
                OwnerMsg::MarkTicketSaved {
                    wave,
                    role,
                    thread,
                    ids,
                } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        for t in &mut rec.open_tickets {
                            if ids.contains(&t.id) {
                                t.saved_in_journal = true;
                            }
                        }
                    }
                }
                OwnerMsg::ResolveTicket {
                    wave,
                    role,
                    thread,
                    id,
                    start_seqs,
                    end_seq,
                    resolved_max_seq,
                    promote_unsaved_activity,
                } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        for s in start_seqs {
                            if s > 0 {
                                rec.unsaved_suppressed_starts.remove(&s);
                                rec.suppressed_start_seqs.insert(s);
                            }
                        }
                        if let Some(e) = end_seq.filter(|&e| e > 0) {
                            rec.unsaved_suppressed_ends.remove(&e);
                            rec.suppressed_end_seqs.insert(e);
                        }
                        rec.resolved_ticket_ids.insert(id);
                        rec.open_tickets.retain(|t| t.id != id);
                        rec.anchored_failure_floor = Some(
                            rec.anchored_failure_floor
                                .map_or(resolved_max_seq, |a| a.max(resolved_max_seq)),
                        );
                        for rem in &mut rec.open_tickets {
                            if rem.id > id {
                                rem.baseline_seq = Some(
                                    rem.baseline_seq
                                        .map_or(resolved_max_seq, |b| b.max(resolved_max_seq)),
                                );
                            }
                        }
                        if let Some(act) = promote_unsaved_activity {
                            let preserve_refusal =
                                matches!(rec.unsaved_failure, Some((Activity::Refused { .. }, _)))
                                    && matches!(act, Activity::Errored { .. });
                            if !preserve_refusal {
                                let merged_seq = Some(
                                    rec.unsaved_failure
                                        .as_ref()
                                        .and_then(|(_, s)| *s)
                                        .map_or(resolved_max_seq, |a| a.max(resolved_max_seq)),
                                );
                                rec.unsaved_failure = Some((act, merged_seq));
                            } else if let Some((_, ref mut prev_seq)) = rec.unsaved_failure {
                                *prev_seq = Some(
                                    prev_seq.map_or(resolved_max_seq, |a| a.max(resolved_max_seq)),
                                );
                            }
                        }
                    }
                }
                OwnerMsg::MarkTicketActivityCommitted { wave, role, thread } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        for u in &mut rec.open_tickets {
                            u.needs_activity_commit = false;
                        }
                    }
                }
                OwnerMsg::NoteTurnStartedAfterFailure { wave, role, thread } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        for u in &mut rec.open_tickets {
                            u.turns_started_after = u.turns_started_after.saturating_add(1);
                        }
                    }
                }
                OwnerMsg::LookupFailureBaseline {
                    wave,
                    role,
                    thread,
                    reply,
                } => {
                    let out = match self.workers_by_role.get(&(wave, role)) {
                        Some(rec) if rec.thread.as_ref() == Some(&thread) => (
                            rec.settled_seq_floor(),
                            rec.anchored_failure_floor,
                            rec.open_tickets.is_empty(),
                        ),
                        _ => (None, None, true),
                    };
                    let _ = reply.send(out);
                }
                OwnerMsg::ClearUnsavedFailure {
                    wave,
                    role,
                    thread,
                    settled_seq,
                } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        rec.unsaved_failure = None;
                        rec.unsaved_floor = None;
                        rec.anchored_failure_floor = None;
                        if let Some(seq) = settled_seq {
                            rec.last_settled_turn_seq = rec.last_settled_turn_seq.max(seq);
                        }
                    }
                }
                OwnerMsg::HydrationStatus {
                    wave,
                    role,
                    thread,
                    reply,
                } => {
                    let hydrated = self.workers_by_role.get(&(wave, role)).and_then(|rec| {
                        (rec.thread.as_ref() == Some(&thread) && rec.hydrated_suppressions).then(
                            || {
                                (
                                    rec.suppressed_start_seqs.clone(),
                                    rec.suppressed_end_seqs.clone(),
                                )
                            },
                        )
                    });
                    let _ = reply.send(hydrated);
                }
                OwnerMsg::ApplyHydratedSuppressions {
                    wave,
                    role,
                    thread,
                    hydrated,
                    reply,
                } => {
                    if is_bb_thread_id(&thread)
                        && !self
                            .workers_by_role
                            .contains_key(&(wave.clone(), role.clone()))
                    {
                        self.workers_by_role.insert(
                            (wave.clone(), role.clone()),
                            TrackedWorker::new_empty(
                                1,
                                Some(thread.clone()),
                                false,
                                Activity::Working,
                            ),
                        );
                    }
                    let out = if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        for s in &hydrated.starts {
                            rec.unsaved_suppressed_starts.remove(s);
                        }
                        for e in &hydrated.ends {
                            rec.unsaved_suppressed_ends.remove(e);
                        }
                        rec.suppressed_start_seqs.extend(hydrated.starts);
                        rec.suppressed_end_seqs.extend(hydrated.ends);
                        if let Some(f) = hydrated.anchored_floor {
                            if rec.unsaved_floor.is_some_and(|uf| uf <= f) {
                                rec.unsaved_floor = None;
                            }
                            rec.anchored_failure_floor =
                                Some(rec.anchored_failure_floor.map_or(f, |a| a.max(f)));
                        }
                        if hydrated.max_ticket_id >= rec.next_ticket_id {
                            rec.next_ticket_id = hydrated.max_ticket_id + 1;
                        }
                        for rid in hydrated.resolved_ticket_ids {
                            rec.resolved_ticket_ids.insert(rid);
                            rec.open_tickets.retain(|t| t.id != rid);
                        }
                        for ht in hydrated.open_tickets {
                            if rec.resolved_ticket_ids.contains(&ht.id) {
                                continue;
                            }
                            if let Some(existing) =
                                rec.open_tickets.iter_mut().find(|t| t.id == ht.id)
                            {
                                existing.saved_in_journal |= ht.saved_in_journal;
                                if existing.baseline_seq.is_none() {
                                    existing.baseline_seq = ht.baseline_seq;
                                }
                                existing.turns_started_after =
                                    existing.turns_started_after.max(ht.turns_started_after);
                            } else {
                                if ht.id >= rec.next_ticket_id {
                                    rec.next_ticket_id = ht.id + 1;
                                }
                                rec.open_tickets.push(ht);
                                rec.open_tickets.sort_by_key(|t| t.id);
                            }
                        }
                        rec.hydrated_suppressions = true;
                        (
                            rec.suppressed_start_seqs.clone(),
                            rec.suppressed_end_seqs.clone(),
                        )
                    } else {
                        (hydrated.starts, hydrated.ends)
                    };
                    let _ = reply.send(out);
                }
                OwnerMsg::AdvanceSeq { thread, seq } => {
                    if let Some(key) = self.role_by_thread.get(&thread)
                        && let Some(rec) = self.workers_by_role.get_mut(key)
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        rec.last_seq = Some(rec.last_seq.map_or(seq, |prev| prev.max(seq)));
                    }
                }
                OwnerMsg::AdvanceFeedSeq { thread, seq } => {
                    if let Some(key) = self.role_by_thread.get(&thread)
                        && let Some(rec) = self.workers_by_role.get_mut(key)
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        rec.feed_seq = Some(rec.feed_seq.map_or(seq, |prev| prev.max(seq)));
                    }
                }
                OwnerMsg::SetPendingMirror {
                    wave,
                    role,
                    thread,
                    since_end_seq,
                    mark_unknown_dirty,
                } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        if mark_unknown_dirty {
                            let fallback = rec.last_mirrored_end_seq.saturating_add(1);
                            rec.pending_mirror_since = Some(
                                rec.pending_mirror_since
                                    .map_or(fallback, |prev| prev.min(fallback)),
                            );
                        } else if let Some(new_s) = since_end_seq {
                            rec.pending_mirror_since = Some(
                                rec.pending_mirror_since
                                    .map_or(new_s, |prev| prev.min(new_s)),
                            );
                        } else {
                            rec.pending_mirror_since = None;
                        }
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
                OwnerMsg::MirrorState {
                    wave,
                    role,
                    thread,
                    end_seq,
                    anchor,
                    reply,
                } => {
                    let ever_connected = self.conn_id > 0;
                    let state = match self.workers_by_role.get(&(wave, role)) {
                        Some(w) if w.thread.as_ref() == Some(&thread) => {
                            let matched = (end_seq > 0 && w.last_mirrored_end_seq >= end_seq)
                                || (!anchor.is_empty()
                                    && w.last_mirrored_anchor.as_deref() == Some(anchor.as_str()));
                            (
                                w.mirror_room_watermark,
                                matched,
                                ever_connected,
                                w.claimed_direct_posts.clone(),
                            )
                        }
                        _ => (Seq::ORIGIN, false, ever_connected, BTreeSet::new()),
                    };
                    let _ = reply.send(state);
                }
                OwnerMsg::AdvanceMirrorWatermark {
                    wave,
                    role,
                    thread,
                    seq,
                    end_seq,
                    anchor,
                    claimed_seqs,
                    clear_pending,
                } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        if rec.mirror_room_watermark < seq {
                            rec.mirror_room_watermark = seq;
                        }
                        for s in claimed_seqs {
                            rec.claimed_direct_posts.insert(s);
                        }
                        if let Some(es) = end_seq {
                            if es > rec.last_mirrored_end_seq {
                                rec.last_mirrored_end_seq = es;
                            }
                            if es > rec.last_settled_turn_seq {
                                rec.last_settled_turn_seq = es;
                            }
                            if let Some(pend) = rec.pending_mirror_since
                                && pend <= es
                            {
                                rec.pending_mirror_since = if clear_pending {
                                    None
                                } else {
                                    Some(es.saturating_add(1))
                                };
                            }
                        }
                        if let Some(a) = anchor {
                            rec.last_mirrored_anchor = Some(a);
                        }
                        if clear_pending {
                            rec.pending_mirror_since = None;
                        }
                    }
                }
            }
        }
    }

    fn fold_wave(&mut self, wave: WaveSlug, event: WaveEvent) {
        match event {
            WaveEvent::Started { .. } => {}
            WaveEvent::WorkerRequested { role, attempt, .. } => {
                if let Some(prev) = self.workers_by_role.get(&(wave.clone(), role.clone())) {
                    if prev.attempt == attempt && prev.thread.is_some() {
                        return;
                    }
                    if let Some(ref prev_th) = prev.thread {
                        self.role_by_thread.remove(prev_th);
                    }
                }
                self.workers_by_role.insert(
                    (wave, role),
                    TrackedWorker::new_empty(attempt, None, false, Activity::Working),
                );
            }
            WaveEvent::WorkerObserved { role, observation } => match observation {
                Observation::Spawned { attempt, thread } => {
                    if !is_bb_thread_id(&thread) {
                        if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
                            if let Some(ref prev_th) = rec.thread {
                                self.role_by_thread.remove(prev_th);
                            }
                            rec.thread = None;
                            rec.live = false;
                            rec.pending_interaction = None;
                            rec.reset_for_new_thread();
                        }
                        return;
                    }
                    if let Some(prev) = self.workers_by_role.get(&(wave.clone(), role.clone()))
                        && let Some(ref prev_th) = prev.thread
                        && prev_th != &thread
                    {
                        self.role_by_thread.remove(prev_th);
                    }
                    self.role_by_thread
                        .insert(thread.clone(), (wave.clone(), role.clone()));
                    let rec = self.workers_by_role.entry((wave, role)).or_insert_with(|| {
                        TrackedWorker::new_empty(
                            attempt,
                            Some(thread.clone()),
                            true,
                            Activity::Working,
                        )
                    });
                    if rec.thread.as_ref() != Some(&thread) {
                        rec.reset_for_new_thread();
                    }
                    rec.attempt = attempt;
                    rec.thread = Some(thread);
                    rec.live = true;
                    rec.activity = Activity::Working;
                    rec.pending_interaction = None;
                }
                Observation::Activity { activity } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
                        if matches!(
                            activity,
                            Activity::Refused { .. } | Activity::Errored { .. }
                        ) {
                            rec.unsaved_failure = None;
                            for u in &mut rec.open_tickets {
                                if u.activity.as_ref() == Some(&activity)
                                    || (matches!(activity, Activity::Refused { .. })
                                        && matches!(u.activity, Some(Activity::Errored { .. })))
                                {
                                    u.needs_activity_commit = false;
                                }
                            }
                        } else if matches!(activity, Activity::Idle) {
                            rec.unsaved_failure = None;
                            rec.unsaved_floor = None;
                            rec.anchored_failure_floor = None;
                        } else if matches!(activity, Activity::Working) {
                            for u in &mut rec.open_tickets {
                                u.turns_started_after = u.turns_started_after.saturating_add(1);
                            }
                        }
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
                        rec.pending_mirror_since = None;
                        rec.pending_interaction = None;
                        rec.unsaved_failure = None;
                        rec.unsaved_floor = None;
                        rec.anchored_failure_floor = None;
                        rec.open_tickets.clear();
                        if let Some(th) = &rec.thread {
                            self.role_by_thread.remove(th);
                        }
                    }
                }
                Observation::SpawnFailed { .. } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
                        rec.live = false;
                        rec.pending_mirror_since = None;
                        rec.unsaved_failure = None;
                        rec.unsaved_floor = None;
                        rec.anchored_failure_floor = None;
                        rec.open_tickets.clear();
                    }
                }
                Observation::RuntimeMeta { .. } => {}
                Observation::TurnsSuppressed { thread, keys } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role))
                        && rec.thread.as_ref() == Some(&thread)
                    {
                        for k in keys {
                            rec.fold_suppression_key(&k);
                        }
                    }
                }
            },
            WaveEvent::WorkerRetiring { role, .. } => {
                if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
                    rec.live = false;
                    rec.pending_mirror_since = None;
                }
            }
            WaveEvent::Finished { .. } => {
                for ((w, _), rec) in &mut self.workers_by_role {
                    if w == &wave {
                        rec.live = false;
                        rec.pending_mirror_since = None;
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
                let _ = self.bridge.signal_tx.send(InboundSignal::Reconcile);
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Default)]
struct FallbackWaitTable {
    next_epoch: u64,
    entries: HashMap<(WaveSlug, RoleName), FallbackWaitEntry>,
}

struct FallbackWaitEntry {
    active: usize,
    epoch: u64,
    gate: Arc<tokio::sync::Mutex<()>>,
}

struct WaitFlightGuard {
    bridge: Arc<BbBridge>,
    key: (WaveSlug, RoleName),
    aborted: Arc<AtomicBool>,
    decremented: bool,
    spawned_restore: bool,
    completed: bool,
}

impl WaitFlightGuard {
    fn enter(
        bridge: Arc<BbBridge>,
        key: (WaveSlug, RoleName),
        aborted: Arc<AtomicBool>,
    ) -> (Self, u64, Arc<tokio::sync::Mutex<()>>) {
        let (epoch, gate) = bridge.increment_fallback_wait(&key);
        (
            WaitFlightGuard {
                bridge,
                key,
                aborted,
                decremented: false,
                spawned_restore: false,
                completed: false,
            },
            epoch,
            gate,
        )
    }

    async fn finish(mut self) {
        self.decremented = true;
        if self.bridge.decrement_fallback_wait(&self.key) == 0 {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                self.spawned_restore = true;
                let b = self.bridge.clone();
                let key = self.key.clone();
                let _ = handle
                    .spawn(async move {
                        b.restore_fallback_working_if_idle(&key).await;
                    })
                    .await;
            } else {
                self.bridge
                    .restore_fallback_working_if_idle(&self.key)
                    .await;
            }
        }
        self.completed = true;
    }
}

impl Drop for WaitFlightGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.aborted.store(true, Ordering::Relaxed);
        let should_restore = if !self.decremented {
            self.decremented = true;
            self.bridge.decrement_fallback_wait(&self.key) == 0
        } else {
            !self.spawned_restore
        };
        if should_restore && let Ok(handle) = tokio::runtime::Handle::try_current() {
            self.spawned_restore = true;
            let b = self.bridge.clone();
            let key = self.key.clone();
            handle.spawn(async move {
                b.restore_fallback_working_if_idle(&key).await;
            });
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
        let aborted_wait = self.aborted_wait.clone();
        async move {
            let is_blocking_wait = matches!(&cmd, WireCommand::Wait(w) if w.timeout_ms != Some(0));
            let router_handles_park = backend.handles_worker_wait_parking();
            let manual_parked_scope = match &cmd {
                WireCommand::Wait(w) if is_blocking_wait && !router_handles_park => {
                    backend.parked_scope(&me, w)
                }
                _ => None,
            };
            let mut wait_guard = None;
            if let Some((room, since)) = manual_parked_scope {
                let key = (wave.clone(), role.clone());
                let (guard, epoch, gate) =
                    WaitFlightGuard::enter(bridge.clone(), key.clone(), aborted_wait);
                wait_guard = Some(guard);
                let _guard_lock = gate.lock().await;
                if bridge.fallback_wait_is_live(&key, epoch, &gate) {
                    bridge
                        .call_wave_observe(
                            wave.clone(),
                            role.clone(),
                            Observation::Activity {
                                activity: Activity::Parked { room, since },
                            },
                        )
                        .await;
                }
            }

            let outcome = backend.call_as(me, cmd).await;

            if let Some(guard) = wait_guard {
                guard.finish().await;
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

struct QueuedTerminalSignalGuard {
    table: Arc<std::sync::Mutex<HashMap<BbThreadId, usize>>>,
    thread: BbThreadId,
}

impl Drop for QueuedTerminalSignalGuard {
    fn drop(&mut self) {
        let mut map = self.table.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cnt) = map.get_mut(&self.thread) {
            *cnt = cnt.saturating_sub(1);
            if *cnt == 0 {
                map.remove(&self.thread);
            }
        }
    }
}

enum InboundSignal {
    Reconcile,
    Up(protocol::Up, Option<QueuedTerminalSignalGuard>),
    #[cfg(test)]
    Flush(oneshot::Sender<()>),
}

pub struct BbBridge {
    cfg: BridgeConfig,
    default_runner: std::sync::Mutex<RunnerKind>,
    data_dir: Option<PathBuf>,
    owner: mpsc::UnboundedSender<OwnerMsg>,
    signal_tx: mpsc::UnboundedSender<InboundSignal>,
    queued_terminal_signals: Arc<std::sync::Mutex<HashMap<BbThreadId, usize>>>,
    backend: OnceLock<Arc<dyn ErasedBackend>>,
    code_runner: Option<CodeRunner>,
    fallback_active_waits: std::sync::Mutex<FallbackWaitTable>,
    reconcile_lock: tokio::sync::Mutex<()>,
    mirror_lock: tokio::sync::Mutex<()>,
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

    pub fn with_default_runner(self: Arc<Self>, runner: RunnerKind) -> Arc<Self> {
        *self
            .default_runner
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = runner;
        self
    }

    fn default_runner(&self) -> RunnerKind {
        *self
            .default_runner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn acquire_terminal_signal_guard(
        &self,
        up: &protocol::Up,
    ) -> Option<QueuedTerminalSignalGuard> {
        let protocol::Up::Thread {
            signal,
            thread_id,
            error,
            stop_reason,
            ..
        } = up
        else {
            return None;
        };
        let is_terminal_failure = matches!(signal, protocol::ThreadSignal::Failed)
            || (matches!(signal, protocol::ThreadSignal::Idle)
                && (stop_reason
                    .as_deref()
                    .is_some_and(rex_wave_api::wave::is_refusal_stop_reason)
                    || error
                        .as_deref()
                        .is_some_and(rex_wave_api::wave::is_refusal_error_text)));
        if !is_terminal_failure {
            return None;
        }
        let thread = BbThreadId::parse(thread_id.clone()).ok()?;
        let mut map = self
            .queued_terminal_signals
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *map.entry(thread.clone()).or_insert(0) += 1;
        Some(QueuedTerminalSignalGuard {
            table: self.queued_terminal_signals.clone(),
            thread,
        })
    }

    fn has_queued_terminal_signal(&self, thread: &BbThreadId) -> bool {
        self.queued_terminal_signals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(thread)
            .copied()
            .unwrap_or(0)
            > 0
    }

    fn with_options(
        cfg: BridgeConfig,
        data_dir: Option<PathBuf>,
        code_runner: Option<CodeRunner>,
    ) -> Arc<Self> {
        let (project_names_tx, project_names_rx) = watch::channel(Arc::new(BTreeMap::new()));
        let (signal_tx, mut signal_rx) = mpsc::unbounded_channel::<InboundSignal>();
        let bridge = Arc::new(BbBridge {
            cfg,
            default_runner: std::sync::Mutex::new(RunnerKind::Bb),
            data_dir,
            owner: BridgeOwner::spawn(),
            signal_tx,
            queued_terminal_signals: Arc::new(std::sync::Mutex::new(HashMap::new())),
            backend: OnceLock::new(),
            code_runner,
            fallback_active_waits: std::sync::Mutex::new(FallbackWaitTable::default()),
            reconcile_lock: tokio::sync::Mutex::new(()),
            mirror_lock: tokio::sync::Mutex::new(()),
            project_names_tx,
            project_names_rx,
        });
        bridge.refresh_local_project_names();
        let weak_bridge = Arc::downgrade(&bridge);
        tokio::spawn(async move {
            while let Some(sig) = signal_rx.recv().await {
                let Some(b) = weak_bridge.upgrade() else {
                    break;
                };
                match sig {
                    InboundSignal::Reconcile => {
                        #[cfg(test)]
                        let mut deferred_flushes: Vec<oneshot::Sender<()>> = Vec::new();
                        loop {
                            let mut drained_ups = Vec::new();
                            while let Ok(next) = signal_rx.try_recv() {
                                match next {
                                    InboundSignal::Reconcile => {}
                                    InboundSignal::Up(up, guard) => {
                                        drained_ups.push((up, guard));
                                    }
                                    #[cfg(test)]
                                    InboundSignal::Flush(done) => {
                                        deferred_flushes.push(done);
                                    }
                                }
                            }
                            if drained_ups.is_empty() {
                                break;
                            }
                            for (up, guard) in drained_ups {
                                b.on_thread_signal_with_guard(up, guard).await;
                            }
                        }
                        b.reconcile_live_workers().await;
                        #[cfg(test)]
                        for done in deferred_flushes {
                            let _ = done.send(());
                        }
                    }
                    InboundSignal::Up(up, guard) => {
                        b.on_thread_signal_with_guard(up, guard).await;
                    }
                    #[cfg(test)]
                    InboundSignal::Flush(done) => {
                        let _ = done.send(());
                    }
                }
            }
        });
        bridge
    }

    #[cfg(test)]
    async fn flush_signal_queue(&self) {
        let (tx, rx) = oneshot::channel();
        if self.signal_tx.send(InboundSignal::Flush(tx)).is_ok() {
            let _ = rx.await;
        }
    }

    fn increment_fallback_wait(
        &self,
        key: &(WaveSlug, RoleName),
    ) -> (u64, Arc<tokio::sync::Mutex<()>>) {
        let mut table = self
            .fallback_active_waits
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        table.next_epoch = table.next_epoch.wrapping_add(1);
        let assigned_epoch = table.next_epoch;
        let entry = table
            .entries
            .entry(key.clone())
            .or_insert_with(|| FallbackWaitEntry {
                active: 0,
                epoch: 0,
                gate: Arc::new(tokio::sync::Mutex::new(())),
            });
        entry.active += 1;
        entry.epoch = assigned_epoch;
        (entry.epoch, entry.gate.clone())
    }

    fn decrement_fallback_wait(&self, key: &(WaveSlug, RoleName)) -> usize {
        let mut table = self
            .fallback_active_waits
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(entry) = table.entries.get_mut(key) else {
            return 0;
        };
        entry.active = entry.active.saturating_sub(1);
        entry.active
    }

    fn fallback_wait_is_live(
        &self,
        key: &(WaveSlug, RoleName),
        expected_epoch: u64,
        gate: &Arc<tokio::sync::Mutex<()>>,
    ) -> bool {
        let table = self
            .fallback_active_waits
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        table.entries.get(key).is_some_and(|e| {
            e.active > 0 && e.epoch >= expected_epoch && Arc::ptr_eq(&e.gate, gate)
        })
    }

    fn snapshot_idle_fallback_wait(
        &self,
        key: &(WaveSlug, RoleName),
    ) -> Option<(u64, Arc<tokio::sync::Mutex<()>>)> {
        let table = self
            .fallback_active_waits
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let entry = table.entries.get(key)?;
        if entry.active > 0 {
            return None;
        }
        Some((entry.epoch, entry.gate.clone()))
    }

    fn fallback_idle_matches(
        &self,
        key: &(WaveSlug, RoleName),
        idle_epoch: u64,
        gate: &Arc<tokio::sync::Mutex<()>>,
    ) -> bool {
        let table = self
            .fallback_active_waits
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        table
            .entries
            .get(key)
            .is_some_and(|e| e.active == 0 && e.epoch == idle_epoch && Arc::ptr_eq(&e.gate, gate))
    }

    fn reclaim_idle_fallback_wait(
        &self,
        key: &(WaveSlug, RoleName),
        idle_epoch: u64,
        gate: &Arc<tokio::sync::Mutex<()>>,
    ) {
        let mut table = self
            .fallback_active_waits
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if table
            .entries
            .get(key)
            .is_some_and(|e| e.active == 0 && e.epoch == idle_epoch && Arc::ptr_eq(&e.gate, gate))
        {
            table.entries.remove(key);
        }
    }

    async fn restore_fallback_working_if_idle(&self, key: &(WaveSlug, RoleName)) {
        let Some((epoch, gate)) = self.snapshot_idle_fallback_wait(key) else {
            return;
        };
        let _guard = gate.lock().await;
        if !self.fallback_idle_matches(key, epoch, &gate) {
            return;
        }
        if let Ok(rec) = self.lookup_worker(&key.0, &key.1).await
            && matches!(rec.activity, Activity::Parked { .. })
            && self.fallback_idle_matches(key, epoch, &gate)
        {
            self.call_wave_observe(
                key.0.clone(),
                key.1.clone(),
                Observation::Activity {
                    activity: Activity::Working,
                },
            )
            .await;
        }
        self.reclaim_idle_fallback_wait(key, epoch, &gate);
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
                let _ = self.signal_tx.send(InboundSignal::Reconcile);
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
                let guard = self.acquire_terminal_signal_guard(&signal);
                let _ = self.signal_tx.send(InboundSignal::Up(signal, guard));
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
    ) -> Option<(WaveSlug, RoleName, u32, bool)> {
        if !is_bb_thread_id(thread) {
            return None;
        }
        let (tx, rx) = oneshot::channel();
        let _ = self.owner.send(OwnerMsg::LookupByThread {
            thread: thread.clone(),
            reply: tx,
        });
        if let Ok(Some((wave, role, attempt))) = rx.await {
            return Some((wave, role, attempt, false));
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
            let default_runner = self.default_runner();
            let mut matched_live = false;
            let mut matched_retiring = false;
            for (r, spec, phase) in &snap.workers {
                if r != &role {
                    continue;
                }
                match phase {
                    WorkerPhase::Live { thread: t, .. } if t == thread => {
                        matched_live = true;
                    }
                    WorkerPhase::Requested { attempt }
                        if *attempt == m.attempt
                            && rex_wave_waves::route_provider(&spec.provider, default_runner)
                                .is_ok_and(|routed| routed.target == RunnerKind::Bb) =>
                    {
                        matched_live = true;
                    }
                    WorkerPhase::Retiring { thread: t, .. } if t == thread => {
                        matched_retiring = true;
                    }
                    _ => {}
                }
            }
            if !matched_live && !matched_retiring {
                return None;
            }
            if matched_live {
                let _ = self.owner.send(OwnerMsg::BindThread {
                    wave: wave.clone(),
                    role: role.clone(),
                    attempt: m.attempt,
                    thread: thread.clone(),
                });
            }
            return Some((wave, role, m.attempt, !matched_live));
        }

        None
    }

    async fn try_wave_observe(
        &self,
        wave: WaveSlug,
        role: RoleName,
        observation: Observation,
    ) -> bool {
        if let Some(backend) = self.backend.get() {
            let res = backend
                .call_as(
                    Principal::System {
                        part: SystemPart::BbBridge,
                    },
                    WaveObserve {
                        wave: wave.clone(),
                        role: role.clone(),
                        observation: observation.clone(),
                    }
                    .into_wire(),
                )
                .await;
            if !matches!(res, Ok(WireReply::WaveObserve(_))) {
                return false;
            }
        }
        self.observe_wave_event(wave, WaveEvent::WorkerObserved { role, observation });
        true
    }

    async fn call_wave_observe(&self, wave: WaveSlug, role: RoleName, observation: Observation) {
        let _ = self.try_wave_observe(wave, role, observation).await;
    }

    async fn commit_bb_suppression_keys(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        keys: Vec<String>,
    ) -> bool {
        if keys.is_empty() {
            return true;
        }
        let mut committed = self
            .try_wave_observe(
                wave.clone(),
                role.clone(),
                Observation::TurnsSuppressed {
                    thread: thread.clone(),
                    keys: keys.clone(),
                },
            )
            .await;
        if !committed {
            tokio::task::yield_now().await;
            committed = self
                .try_wave_observe(
                    wave.clone(),
                    role.clone(),
                    Observation::TurnsSuppressed {
                        thread: thread.clone(),
                        keys,
                    },
                )
                .await;
        }
        committed
    }

    async fn record_suppressed_bb_segment(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        bounds: (Vec<u64>, Option<u64>, Option<u64>),
        failure: Option<(Activity, Option<u64>)>,
    ) -> bool {
        let (start_seqs, end_seq, floor_seq) = bounds;
        let mut keys = Vec::new();
        for &s in &start_seqs {
            if s > 0 {
                keys.push(format!("s{s}"));
            }
        }
        if let Some(e) = end_seq.filter(|&e| e > 0) {
            keys.push(format!("e{e}"));
        }
        if let Some(f) = floor_seq {
            keys.push(format!("f{f}"));
        }
        if !self
            .commit_bb_suppression_keys(wave, role, thread, keys)
            .await
        {
            let _ = self.owner.send(OwnerMsg::QueueUnsavedSuppressions {
                wave: wave.clone(),
                role: role.clone(),
                thread: thread.clone(),
                start_seqs,
                end_seq,
                floor_seq,
                failure,
            });
            return false;
        }
        let _ = self.owner.send(OwnerMsg::SuppressTurn {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
            start_seqs,
            end_seq,
            floor_seq,
        });
        true
    }

    async fn allocate_and_persist_bb_ticket(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        baseline_override: Option<Option<u64>>,
        activity: Activity,
        needs_activity_commit: bool,
    ) {
        let (tx, rx) = oneshot::channel();
        let _ = self.owner.send(OwnerMsg::AllocateOrMergeTicket {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
            baseline_override,
            activity,
            needs_activity_commit,
            reply: tx,
        });
        let Ok((id, base, already_saved)) = rx.await else {
            return;
        };
        if !already_saved {
            let key = format_bb_ticket_key(id, base);
            if self
                .commit_bb_suppression_keys(wave, role, thread, vec![key])
                .await
            {
                let _ = self.owner.send(OwnerMsg::MarkTicketSaved {
                    wave: wave.clone(),
                    role: role.clone(),
                    thread: thread.clone(),
                    ids: vec![id],
                });
            }
        }
    }

    async fn flush_and_hydrate_bb_suppressions(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
    ) -> Option<FlushedSuppressionState> {
        let (mut starts, mut ends) = self
            .ensure_bb_journal_suppressions_hydrated(wave, role, thread)
            .await?;
        let (u_tx, u_rx) = oneshot::channel();
        let _ = self.owner.send(OwnerMsg::PendingUnsavedSuppressions {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
            reply: u_tx,
        });
        let (
            unsaved_starts,
            unsaved_ends,
            unsaved_floor,
            unsaved_failure,
            mut anchored_failure_floor,
            mut open_tickets,
        ) = u_rx.await.ok()?;
        let mut keys = Vec::new();
        for &s in &unsaved_starts {
            if s > 0 && !starts.contains(&s) {
                keys.push(format!("s{s}"));
            }
        }
        for &e in &unsaved_ends {
            if e > 0 && !ends.contains(&e) {
                keys.push(format!("e{e}"));
            }
        }
        if let Some(f) = unsaved_floor {
            keys.push(format!("f{f}"));
        }
        let mut unsaved_ticket_ids = Vec::new();
        for t in &open_tickets {
            if !t.saved_in_journal {
                keys.push(format_bb_ticket_key(t.id, t.baseline_seq));
                unsaved_ticket_ids.push(t.id);
            }
        }
        if !keys.is_empty() {
            if !self
                .commit_bb_suppression_keys(wave, role, thread, keys)
                .await
            {
                let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                    wave: wave.clone(),
                    role: role.clone(),
                    thread: thread.clone(),
                    since_end_seq: None,
                    mark_unknown_dirty: true,
                });
                return None;
            }
            let start_vec: Vec<u64> = unsaved_starts.iter().copied().collect();
            for &s in &start_vec {
                starts.insert(s);
            }
            for &e in &unsaved_ends {
                let _ = self.owner.send(OwnerMsg::SuppressTurn {
                    wave: wave.clone(),
                    role: role.clone(),
                    thread: thread.clone(),
                    start_seqs: Vec::new(),
                    end_seq: Some(e),
                    floor_seq: None,
                });
                ends.insert(e);
            }
            if !start_vec.is_empty() || unsaved_floor.is_some() {
                let _ = self.owner.send(OwnerMsg::SuppressTurn {
                    wave: wave.clone(),
                    role: role.clone(),
                    thread: thread.clone(),
                    start_seqs: start_vec,
                    end_seq: None,
                    floor_seq: unsaved_floor,
                });
            }
            if let Some(f) = unsaved_floor {
                anchored_failure_floor = Some(anchored_failure_floor.map_or(f, |a| a.max(f)));
            }
            if !unsaved_ticket_ids.is_empty() {
                let _ = self.owner.send(OwnerMsg::MarkTicketSaved {
                    wave: wave.clone(),
                    role: role.clone(),
                    thread: thread.clone(),
                    ids: unsaved_ticket_ids.clone(),
                });
                for t in &mut open_tickets {
                    if unsaved_ticket_ids.contains(&t.id) {
                        t.saved_in_journal = true;
                    }
                }
            }
        }
        Some(FlushedSuppressionState {
            starts,
            ends,
            unsaved_failure,
            anchored_failure_floor,
            open_tickets,
        })
    }

    async fn resolve_unanchored_failures_in_rows(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        rows: &[serde_json::Value],
        flushed: &mut FlushedSuppressionState,
    ) -> Option<(Option<Activity>, Option<u64>, bool)> {
        if flushed.open_tickets.is_empty() {
            return Some((None, None, false));
        }
        let segments = extract_bb_turn_segments(rows);
        let total = flushed.open_tickets.len();
        let mut running_base = flushed.anchored_failure_floor;
        for idx in 0..total {
            let entry = &flushed.open_tickets[idx];
            let effective_base = match (entry.baseline_seq, running_base) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            let matched: Option<Result<BbTurnSegment, u64>> = match effective_base {
                Some(base) => segments
                    .iter()
                    .find(|seg| {
                        seg.max_seq > base
                            && !is_segment_already_suppressed(seg, &flushed.starts, &flushed.ends)
                    })
                    .cloned()
                    .map(Ok),
                None => {
                    let candidates: Vec<BbTurnSegment> = segments
                        .iter()
                        .filter(|seg| {
                            !is_segment_already_suppressed(seg, &flushed.starts, &flushed.ends)
                        })
                        .cloned()
                        .collect();
                    let remaining_after = total - 1 - idx;
                    let skip_tail = entry.turns_started_after.max(remaining_after);
                    if candidates.is_empty() {
                        None
                    } else if candidates.len() <= skip_tail {
                        Some(Err(0))
                    } else {
                        Some(Ok(candidates[candidates.len() - 1 - skip_tail].clone()))
                    }
                }
            };
            let Some(target) = matched else {
                let mut pending_act: Option<Activity> = None;
                for rem in &flushed.open_tickets[idx..] {
                    if rem.needs_activity_commit
                        && let Some(ref act) = rem.activity
                    {
                        let preserve_refusal =
                            matches!(pending_act, Some(Activity::Refused { .. }))
                                && matches!(act, Activity::Errored { .. });
                        if !preserve_refusal {
                            pending_act = Some(act.clone());
                        }
                    }
                }
                flushed.open_tickets.drain(0..idx);
                return Some((pending_act, effective_base, true));
            };
            let (seg_starts, seg_end, resolved_seq) = match target {
                Ok(seg) => (seg.start_seqs, seg.end_seq, seg.max_seq),
                Err(z) => (Vec::new(), None, z),
            };
            let mut res_keys = Vec::new();
            for &s in &seg_starts {
                if s > 0 && !flushed.starts.contains(&s) {
                    res_keys.push(format!("s{s}"));
                }
            }
            if let Some(e) = seg_end.filter(|&e| e > 0 && !flushed.ends.contains(&e)) {
                res_keys.push(format!("e{e}"));
            }
            res_keys.push(format!("f{resolved_seq}"));
            res_keys.push(format!("fr:{}:{resolved_seq}", entry.id));
            if !self
                .commit_bb_suppression_keys(wave, role, thread, res_keys)
                .await
            {
                flushed.open_tickets.drain(0..idx);
                let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                    wave: wave.clone(),
                    role: role.clone(),
                    thread: thread.clone(),
                    since_end_seq: None,
                    mark_unknown_dirty: true,
                });
                return None;
            }
            for &s in &seg_starts {
                if s > 0 {
                    flushed.starts.insert(s);
                }
            }
            if let Some(e) = seg_end.filter(|&e| e > 0) {
                flushed.ends.insert(e);
            }
            let promote = if entry.needs_activity_commit {
                entry.activity.clone()
            } else {
                None
            };
            if let Some(ref act) = promote {
                let preserve_refusal =
                    matches!(flushed.unsaved_failure, Some((Activity::Refused { .. }, _)))
                        && matches!(act, Activity::Errored { .. });
                if !preserve_refusal {
                    let merged_seq = Some(
                        flushed
                            .unsaved_failure
                            .as_ref()
                            .and_then(|(_, s)| *s)
                            .map_or(resolved_seq, |a| a.max(resolved_seq)),
                    );
                    flushed.unsaved_failure = Some((act.clone(), merged_seq));
                }
            }
            flushed.anchored_failure_floor = Some(
                flushed
                    .anchored_failure_floor
                    .map_or(resolved_seq, |a| a.max(resolved_seq)),
            );
            running_base = flushed.anchored_failure_floor;
            let _ = self.owner.send(OwnerMsg::ResolveTicket {
                wave: wave.clone(),
                role: role.clone(),
                thread: thread.clone(),
                id: entry.id,
                start_seqs: seg_starts,
                end_seq: seg_end,
                resolved_max_seq: resolved_seq,
                promote_unsaved_activity: promote,
            });
        }
        flushed.open_tickets.clear();
        Some((None, running_base, false))
    }

    async fn ensure_bb_journal_suppressions_hydrated(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
    ) -> Option<(BTreeSet<u64>, BTreeSet<u64>)> {
        let (tx, rx) = oneshot::channel();
        let _ = self.owner.send(OwnerMsg::HydrationStatus {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
            reply: tx,
        });
        if let Ok(Some(cached)) = rx.await {
            return Some(cached);
        }
        let Some(backend) = self.backend.get() else {
            return Some((BTreeSet::new(), BTreeSet::new()));
        };
        let sys = Principal::System {
            part: SystemPart::BbBridge,
        };
        let mut cursor = Seq::ORIGIN;
        let mut wave_project: Option<BbProjectId> = None;
        let mut active_thread: Option<BbThreadId> = None;
        let mut auth_activity = Activity::Working;
        let mut scratch =
            TrackedWorker::new_empty(1, Some(thread.clone()), true, Activity::Working);
        let mut reached_end = false;
        loop {
            let Ok(WireReply::Read(page)) = backend
                .call_as(
                    sys.clone(),
                    rex_wave_api::read::Read::stream(
                        StreamId::Wave { wave: wave.clone() },
                        cursor,
                        Some(500),
                    )
                    .into_wire(),
                )
                .await
            else {
                break;
            };
            let Some(last) = page.entries.last() else {
                reached_end = true;
                break;
            };
            if last.seq <= cursor {
                break;
            };
            cursor = last.seq;
            let count = page.entries.len();
            for env in &page.entries {
                if let Event::Wave(ev) = &env.event {
                    match ev {
                        WaveEvent::Started { spec, .. } => {
                            wave_project = Some(spec.project.clone());
                        }
                        WaveEvent::WorkerRequested { role: r, .. } if r == role => {
                            active_thread = None;
                            auth_activity = Activity::Working;
                            scratch.reset_for_new_thread();
                        }
                        WaveEvent::WorkerObserved {
                            role: r,
                            observation: Observation::Spawned { thread: th, .. },
                        } if r == role => {
                            if active_thread.as_ref() != Some(th) {
                                active_thread = Some(th.clone());
                                auth_activity = Activity::Working;
                                scratch.reset_for_new_thread();
                            }
                        }
                        WaveEvent::WorkerObserved {
                            role: r,
                            observation: Observation::Activity { activity },
                        } if r == role => {
                            if matches!(activity, Activity::Idle) {
                                scratch.anchored_failure_floor = None;
                            } else if matches!(activity, Activity::Working) {
                                for u in &mut scratch.open_tickets {
                                    u.turns_started_after = u.turns_started_after.saturating_add(1);
                                }
                            }
                            if !(matches!(auth_activity, Activity::Refused { .. })
                                && matches!(activity, Activity::Errored { .. }))
                            {
                                auth_activity = activity.clone();
                            }
                        }
                        WaveEvent::WorkerObserved {
                            role: r,
                            observation:
                                Observation::TurnsSuppressed {
                                    thread: th,
                                    keys: ks,
                                },
                        } if r == role => {
                            if active_thread.as_ref() != Some(th) {
                                active_thread = Some(th.clone());
                                scratch.reset_for_new_thread();
                            }
                            for k in ks {
                                scratch.fold_suppression_key(k);
                            }
                        }
                        _ => {}
                    }
                }
            }
            if count < 500 {
                reached_end = true;
                break;
            }
        }
        if !reached_end {
            return None;
        }
        let mut hydrated = if active_thread.as_ref() == Some(thread) {
            HydratedSuppressionData {
                starts: scratch.suppressed_start_seqs,
                ends: scratch.suppressed_end_seqs,
                anchored_floor: scratch.anchored_failure_floor,
                max_ticket_id: scratch.next_ticket_id.saturating_sub(1),
                resolved_ticket_ids: scratch.resolved_ticket_ids,
                open_tickets: scratch.open_tickets,
            }
        } else {
            HydratedSuppressionData::default()
        };
        if active_thread.as_ref() == Some(thread)
            && matches!(
                auth_activity,
                Activity::Errored { .. } | Activity::Refused { .. }
            )
            && hydrated.open_tickets.is_empty()
            && hydrated.anchored_floor.is_none()
            && hydrated.starts.is_empty()
            && hydrated.ends.is_empty()
            && let Some(proj) = wave_project
        {
            let room = rex_wave_waves::wave_room(wave, &proj);
            let worker_principal = Principal::Worker {
                wave: wave.clone(),
                role: role.clone(),
            };
            let idem_prefix = format!("wave:{wave}:turn:{role}:{thread}:e");
            let mut r_cur = Seq::ORIGIN;
            let mut max_mirrored_end: Option<u64> = None;
            loop {
                let Ok(WireReply::Read(r_page)) = backend
                    .call_as(
                        sys.clone(),
                        rex_wave_api::read::Read::stream(
                            StreamId::Room { room: room.clone() },
                            r_cur,
                            Some(500),
                        )
                        .into_wire(),
                    )
                    .await
                else {
                    return None;
                };
                let Some(r_last) = r_page.entries.last() else {
                    break;
                };
                if r_last.seq <= r_cur {
                    break;
                }
                r_cur = r_last.seq;
                let r_cnt = r_page.entries.len();
                for env in &r_page.entries {
                    if env.by == worker_principal
                        && let Event::Room(RoomEvent::Posted {
                            idem: Some(idem), ..
                        }) = &env.event
                        && let Some(es) = idem
                            .as_str()
                            .strip_prefix(&idem_prefix)
                            .and_then(|n| n.parse::<u64>().ok())
                            .filter(|&n| n > 0)
                    {
                        max_mirrored_end = Some(max_mirrored_end.map_or(es, |prev| prev.max(es)));
                    }
                }
                if r_cnt < 500 {
                    break;
                }
            }
            if max_mirrored_end.is_some() {
                let synth_id = hydrated.max_ticket_id.saturating_add(1);
                hydrated.max_ticket_id = synth_id;
                hydrated.open_tickets.push(BbFailureTicket {
                    id: synth_id,
                    baseline_seq: max_mirrored_end,
                    turns_started_after: 0,
                    saved_in_journal: false,
                    activity: Some(auth_activity),
                    needs_activity_commit: false,
                });
            }
        }
        let fallback_pair = (hydrated.starts.clone(), hydrated.ends.clone());
        let (ap_tx, ap_rx) = oneshot::channel();
        let _ = self.owner.send(OwnerMsg::ApplyHydratedSuppressions {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
            hydrated,
            reply: ap_tx,
        });
        Some(ap_rx.await.unwrap_or(fallback_pair))
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

    async fn handle_terminal_failure_signal(
        &self,
        target: (WaveSlug, RoleName, u32, BbThreadId),
        activity: Activity,
        rows_res: Result<Vec<serde_json::Value>, HostError>,
        runtime_meta: (Option<ModelId>, Option<String>),
        check_live_before_activity: bool,
    ) {
        let (wave, role, attempt, thread) = target;
        let (parsed_model, parsed_account) = runtime_meta;
        let (b_tx, b_rx) = oneshot::channel();
        let _ = self.owner.send(OwnerMsg::LookupFailureBaseline {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
            reply: b_tx,
        });
        let (owner_settled, owner_anchored, owner_tickets_empty) =
            b_rx.await.unwrap_or((None, None, true));

        if !self
            .try_wave_observe(
                wave.clone(),
                role.clone(),
                Observation::Spawned {
                    attempt,
                    thread: thread.clone(),
                },
            )
            .await
        {
            if let Ok(ref rows) = rows_res
                && owner_tickets_empty
            {
                let segments = extract_bb_turn_segments(rows);
                let settled_num = owner_settled.unwrap_or(0);
                if let Some(seg) = segments.iter().rfind(|s| s.max_seq > settled_num) {
                    let _ = self.owner.send(OwnerMsg::QueueUnsavedSuppressions {
                        wave,
                        role,
                        thread,
                        start_seqs: seg.start_seqs.clone(),
                        end_seq: seg.end_seq,
                        floor_seq: Some(seg.max_seq),
                        failure: Some((activity, Some(seg.max_seq))),
                    });
                    return;
                }
                let max_row_seq = rows
                    .iter()
                    .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()))
                    .max()
                    .unwrap_or(0);
                if let Some(af) = owner_anchored.filter(|&af| max_row_seq <= af) {
                    let _ = self.owner.send(OwnerMsg::QueueUnsavedSuppressions {
                        wave,
                        role,
                        thread,
                        start_seqs: Vec::new(),
                        end_seq: None,
                        floor_seq: Some(af),
                        failure: Some((activity, Some(af))),
                    });
                    return;
                }
            }
            let (tx, rx) = oneshot::channel();
            let _ = self.owner.send(OwnerMsg::AllocateOrMergeTicket {
                wave,
                role,
                thread,
                baseline_override: Some(owner_settled),
                activity,
                needs_activity_commit: true,
                reply: tx,
            });
            let _ = rx.await;
            return;
        }

        let (row_model, row_account) = rows_res
            .as_ref()
            .ok()
            .map(|r| extract_runtime_meta_from_rows(r))
            .unwrap_or((None, None));
        self.observe_runtime_meta_if_present(
            &wave,
            &role,
            parsed_model.or(row_model),
            parsed_account.or(row_account),
        )
        .await;

        let Ok(rows) = rows_res else {
            let can_observe_act = !check_live_before_activity
                || self.is_current_live_thread(&wave, &role, &thread).await;
            let committed = can_observe_act
                && self
                    .try_wave_observe(
                        wave.clone(),
                        role.clone(),
                        Observation::Activity {
                            activity: activity.clone(),
                        },
                    )
                    .await;
            self.allocate_and_persist_bb_ticket(&wave, &role, &thread, None, activity, !committed)
                .await;
            return;
        };

        let Some(mut flushed) = self
            .flush_and_hydrate_bb_suppressions(&wave, &role, &thread)
            .await
        else {
            let segments = extract_bb_turn_segments(&rows);
            let settled_num = owner_settled.unwrap_or(0);
            if owner_tickets_empty
                && let Some(seg) = segments.iter().rfind(|s| s.max_seq > settled_num)
            {
                let _ = self.owner.send(OwnerMsg::QueueUnsavedSuppressions {
                    wave,
                    role,
                    thread,
                    start_seqs: seg.start_seqs.clone(),
                    end_seq: seg.end_seq,
                    floor_seq: Some(seg.max_seq),
                    failure: Some((activity, Some(seg.max_seq))),
                });
            } else {
                let (tx, rx) = oneshot::channel();
                let _ = self.owner.send(OwnerMsg::AllocateOrMergeTicket {
                    wave,
                    role,
                    thread,
                    baseline_override: Some(owner_settled),
                    activity,
                    needs_activity_commit: true,
                    reply: tx,
                });
                let _ = rx.await;
            }
            return;
        };

        if !flushed.open_tickets.is_empty() {
            let tail_turns_after = flushed
                .open_tickets
                .last()
                .map_or(0, |t| t.turns_started_after);
            let strict_prior = strict_rows_before_terminal_turn(&rows);
            let Some((_, _, still_open_in_prior)) = self
                .resolve_unanchored_failures_in_rows(
                    &wave,
                    &role,
                    &thread,
                    &strict_prior,
                    &mut flushed,
                )
                .await
            else {
                self.allocate_and_persist_bb_ticket(&wave, &role, &thread, None, activity, true)
                    .await;
                return;
            };
            if still_open_in_prior {
                let Some((_, _, still_open_in_full)) = self
                    .resolve_unanchored_failures_in_rows(&wave, &role, &thread, &rows, &mut flushed)
                    .await
                else {
                    return;
                };
                if still_open_in_full {
                    let can_observe_act = !check_live_before_activity
                        || self.is_current_live_thread(&wave, &role, &thread).await;
                    let committed = can_observe_act
                        && self
                            .try_wave_observe(
                                wave.clone(),
                                role.clone(),
                                Observation::Activity {
                                    activity: activity.clone(),
                                },
                            )
                            .await;
                    self.allocate_and_persist_bb_ticket(
                        &wave, &role, &thread, None, activity, !committed,
                    )
                    .await;
                    return;
                }
                if tail_turns_after == 0 {
                    let prior_rows = rows_before_terminal_turn(&rows);
                    let mirrored_ok = self
                        .mirror_outstanding_turns_from_tail(&wave, &role, &thread, &prior_rows)
                        .await;
                    let fseq_now = flushed.anchored_failure_floor.unwrap_or(0);
                    let can_observe_act = !check_live_before_activity
                        || self.is_current_live_thread(&wave, &role, &thread).await;
                    if can_observe_act
                        && !self
                            .try_wave_observe(
                                wave.clone(),
                                role.clone(),
                                Observation::Activity {
                                    activity: activity.clone(),
                                },
                            )
                            .await
                    {
                        let _ = self.owner.send(OwnerMsg::QueueUnsavedSuppressions {
                            wave,
                            role,
                            thread,
                            start_seqs: Vec::new(),
                            end_seq: None,
                            floor_seq: Some(fseq_now),
                            failure: Some((activity, Some(fseq_now))),
                        });
                    } else if !mirrored_ok {
                        let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                            wave,
                            role,
                            thread,
                            since_end_seq: None,
                            mark_unknown_dirty: true,
                        });
                    }
                    return;
                }
            }
        }

        let (b2_tx, b2_rx) = oneshot::channel();
        let _ = self.owner.send(OwnerMsg::LookupFailureBaseline {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
            reply: b2_tx,
        });
        let (owner_settled_now, _, _) = b2_rx.await.unwrap_or((owner_settled, None, true));
        let settled_floor = [
            owner_settled_now.unwrap_or(0),
            flushed.anchored_failure_floor.unwrap_or(0),
            flushed.starts.last().copied().unwrap_or(0),
            flushed.ends.last().copied().unwrap_or(0),
        ]
        .into_iter()
        .max()
        .unwrap_or(0);

        let segments = extract_bb_turn_segments(&rows);
        if let Some(seg) = segments
            .iter()
            .rfind(|seg| {
                seg.max_seq > settled_floor
                    && !is_segment_already_suppressed(seg, &flushed.starts, &flushed.ends)
            })
            .cloned()
        {
            let failure = Some((activity.clone(), Some(seg.max_seq)));
            if !self
                .record_suppressed_bb_segment(
                    &wave,
                    &role,
                    &thread,
                    (seg.start_seqs, seg.end_seq, Some(seg.max_seq)),
                    failure.clone(),
                )
                .await
            {
                return;
            }
            let prior_rows = rows_before_terminal_turn(&rows);
            let mirrored_ok = self
                .mirror_outstanding_turns_from_tail(&wave, &role, &thread, &prior_rows)
                .await;
            let can_observe_act = !check_live_before_activity
                || self.is_current_live_thread(&wave, &role, &thread).await;
            if can_observe_act
                && !self
                    .try_wave_observe(
                        wave.clone(),
                        role.clone(),
                        Observation::Activity { activity },
                    )
                    .await
            {
                let _ = self.owner.send(OwnerMsg::QueueUnsavedSuppressions {
                    wave,
                    role,
                    thread,
                    start_seqs: Vec::new(),
                    end_seq: None,
                    floor_seq: Some(seg.max_seq),
                    failure,
                });
            } else if !mirrored_ok {
                let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                    wave,
                    role,
                    thread,
                    since_end_seq: None,
                    mark_unknown_dirty: true,
                });
            }
            return;
        }

        let max_row_seq = rows
            .iter()
            .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()))
            .max()
            .unwrap_or(0);
        if let Some(af) = flushed
            .anchored_failure_floor
            .filter(|&af| max_row_seq <= af)
        {
            if let Some(last_seg) = segments.last()
                && is_segment_already_suppressed(last_seg, &flushed.starts, &flushed.ends)
                && !self
                    .record_suppressed_bb_segment(
                        &wave,
                        &role,
                        &thread,
                        (
                            last_seg.start_seqs.clone(),
                            last_seg.end_seq,
                            Some(af.max(last_seg.max_seq)),
                        ),
                        Some((activity.clone(), Some(af.max(last_seg.max_seq)))),
                    )
                    .await
            {
                return;
            }
            let prior_rows = rows_before_terminal_turn(&rows);
            let _ = self
                .mirror_outstanding_turns_from_tail(&wave, &role, &thread, &prior_rows)
                .await;
            let can_observe_act = !check_live_before_activity
                || self.is_current_live_thread(&wave, &role, &thread).await;
            if can_observe_act
                && !self
                    .try_wave_observe(
                        wave.clone(),
                        role.clone(),
                        Observation::Activity {
                            activity: activity.clone(),
                        },
                    )
                    .await
            {
                let _ = self.owner.send(OwnerMsg::QueueUnsavedSuppressions {
                    wave,
                    role,
                    thread,
                    start_seqs: Vec::new(),
                    end_seq: None,
                    floor_seq: Some(af),
                    failure: Some((activity, Some(af))),
                });
            }
            return;
        }

        let can_observe_act =
            !check_live_before_activity || self.is_current_live_thread(&wave, &role, &thread).await;
        let committed = can_observe_act
            && self
                .try_wave_observe(
                    wave.clone(),
                    role.clone(),
                    Observation::Activity {
                        activity: activity.clone(),
                    },
                )
                .await;
        self.allocate_and_persist_bb_ticket(
            &wave,
            &role,
            &thread,
            Some((settled_floor > 0).then_some(settled_floor)),
            activity,
            !committed,
        )
        .await;
    }

    #[cfg(test)]
    async fn on_thread_signal(&self, up: protocol::Up) {
        let guard = self.acquire_terminal_signal_guard(&up);
        self.on_thread_signal_with_guard(up, guard).await;
    }

    async fn on_thread_signal_with_guard(
        &self,
        up: protocol::Up,
        queued_guard: Option<QueuedTerminalSignalGuard>,
    ) {
        let _reconcile_guard = self.reconcile_lock.lock().await;
        drop(queued_guard);
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
                let Some((wave, role, attempt, retiring)) =
                    self.resolve_worker_identity(&thread, meta.as_ref()).await
                else {
                    return;
                };
                if retiring {
                    if matches!(signal, protocol::ThreadSignal::Archived) {
                        self.call_wave_observe(wave, role, Observation::Archived)
                            .await;
                    }
                    return;
                }
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
                        let _ = self.owner.send(OwnerMsg::NoteTurnStartedAfterFailure {
                            wave: wave.clone(),
                            role: role.clone(),
                            thread: thread.clone(),
                        });
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
                        let direct_refusal = stop_reason
                            .as_deref()
                            .filter(|sr| rex_wave_api::wave::is_refusal_stop_reason(sr))
                            .map(str::to_owned);
                        if direct_refusal.is_none() && self.has_queued_terminal_signal(&thread) {
                            let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                                wave,
                                role,
                                thread,
                                since_end_seq: None,
                                mark_unknown_dirty: true,
                            });
                            return;
                        }
                        let rows_res = self.fetch_recent_terminal_turn_rows_checked(&thread).await;
                        let row_refusal = if direct_refusal.is_some() {
                            None
                        } else {
                            rows_res
                                .as_ref()
                                .ok()
                                .and_then(|rows| refusal_from_latest_turn(rows))
                        };
                        if let Some(reason) = direct_refusal.or(row_refusal) {
                            self.handle_terminal_failure_signal(
                                (wave, role, attempt, thread),
                                Activity::Refused { reason },
                                rows_res,
                                (parsed_model, parsed_account),
                                true,
                            )
                            .await;
                            return;
                        }
                        if self.has_queued_terminal_signal(&thread) {
                            let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                                wave,
                                role,
                                thread,
                                since_end_seq: None,
                                mark_unknown_dirty: true,
                            });
                            return;
                        }
                        if !self
                            .try_wave_observe(
                                wave.clone(),
                                role.clone(),
                                Observation::Spawned {
                                    attempt,
                                    thread: thread.clone(),
                                },
                            )
                            .await
                        {
                            let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                                wave,
                                role,
                                thread,
                                since_end_seq: None,
                                mark_unknown_dirty: true,
                            });
                            return;
                        }
                        let Ok(rows) = rows_res else {
                            let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                                wave,
                                role,
                                thread,
                                since_end_seq: None,
                                mark_unknown_dirty: true,
                            });
                            return;
                        };
                        let max_row_seq = rows
                            .iter()
                            .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()))
                            .max();
                        let fseq_now = max_row_seq.unwrap_or(0);
                        let (row_model, row_account) = extract_runtime_meta_from_rows(&rows);
                        self.observe_runtime_meta_if_present(
                            &wave,
                            &role,
                            parsed_model.or(row_model),
                            parsed_account.or(row_account),
                        )
                        .await;
                        let Some(mut flushed) = self
                            .flush_and_hydrate_bb_suppressions(&wave, &role, &thread)
                            .await
                        else {
                            return;
                        };
                        let mut working_rows = rows;
                        if !flushed.open_tickets.is_empty() {
                            let min_base = flushed
                                .open_tickets
                                .iter()
                                .map(|u| u.baseline_seq.unwrap_or(0))
                                .min()
                                .unwrap_or(0);
                            let min_row_seq = working_rows
                                .iter()
                                .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()))
                                .min()
                                .unwrap_or(0);
                            if min_row_seq > min_base.saturating_add(1) {
                                let (full_rows, incomplete) = self
                                    .fetch_all_thread_rows_through(
                                        &thread,
                                        &working_rows,
                                        false,
                                        true,
                                    )
                                    .await;
                                if incomplete {
                                    let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                                        wave,
                                        role,
                                        thread,
                                        since_end_seq: None,
                                        mark_unknown_dirty: true,
                                    });
                                    return;
                                }
                                working_rows = full_rows;
                            }
                            let Some((unresolved_act, unresolved_base, has_unresolved)) = self
                                .resolve_unanchored_failures_in_rows(
                                    &wave,
                                    &role,
                                    &thread,
                                    &working_rows,
                                    &mut flushed,
                                )
                                .await
                            else {
                                return;
                            };
                            if has_unresolved {
                                if let Some(safe_max) = unresolved_base.filter(|&b| b > 0) {
                                    let safe_rows: Vec<serde_json::Value> = working_rows
                                        .iter()
                                        .filter(|r| {
                                            r.get("seq").and_then(|v| v.as_u64()).unwrap_or(0)
                                                <= safe_max
                                        })
                                        .cloned()
                                        .collect();
                                    let _ = self
                                        .mirror_outstanding_turns_from_tail(
                                            &wave, &role, &thread, &safe_rows,
                                        )
                                        .await;
                                }
                                if let Some(act) = unresolved_act
                                    && self.is_current_live_thread(&wave, &role, &thread).await
                                    && self
                                        .try_wave_observe(
                                            wave.clone(),
                                            role.clone(),
                                            Observation::Activity { activity: act },
                                        )
                                        .await
                                {
                                    let _ =
                                        self.owner.send(OwnerMsg::MarkTicketActivityCommitted {
                                            wave: wave.clone(),
                                            role: role.clone(),
                                            thread: thread.clone(),
                                        });
                                }
                                let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                                    wave,
                                    role,
                                    thread,
                                    since_end_seq: None,
                                    mark_unknown_dirty: true,
                                });
                                return;
                            }
                        }
                        let auth_activity = self.authoritative_activity(&wave, &role).await;
                        let auth_terminal_failure = matches!(
                            auth_activity,
                            Some(Activity::Refused { .. } | Activity::Errored { .. })
                        );
                        let has_active_failure = flushed.anchored_failure_floor.is_some()
                            || flushed.unsaved_failure.is_some()
                            || auth_terminal_failure;
                        if has_active_failure {
                            let max_sup_seq = flushed
                                .starts
                                .last()
                                .copied()
                                .into_iter()
                                .chain(flushed.ends.last().copied())
                                .max();
                            let effective_floor = flushed
                                .anchored_failure_floor
                                .or_else(|| flushed.unsaved_failure.as_ref().and_then(|(_, s)| *s))
                                .or(max_sup_seq);
                            let mut recovered = has_unsuppressed_turn_after(
                                &working_rows,
                                &flushed.starts,
                                &flushed.ends,
                                effective_floor,
                            );
                            let needs_backfill = effective_floor.is_some_and(|fseq| {
                                working_rows.len() >= 200
                                    || working_rows
                                        .iter()
                                        .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()))
                                        .min()
                                        .is_some_and(|min_s| min_s > fseq)
                            });
                            if needs_backfill && let Some(fseq) = effective_floor {
                                let (full_rows, incomplete) = self
                                    .fetch_all_thread_rows_through(
                                        &thread,
                                        &working_rows,
                                        false,
                                        true,
                                    )
                                    .await;
                                if incomplete {
                                    let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                                        wave,
                                        role,
                                        thread,
                                        since_end_seq: None,
                                        mark_unknown_dirty: true,
                                    });
                                    return;
                                }
                                working_rows = full_rows;
                                recovered = has_unsuppressed_turn_after(
                                    &working_rows,
                                    &flushed.starts,
                                    &flushed.ends,
                                    Some(fseq),
                                );
                            }
                            if !recovered {
                                let segments = extract_bb_turn_segments(&working_rows);
                                let tail_ok = if effective_floor.is_none() {
                                    let (sup_starts, sup_end, sup_max) = match segments.last() {
                                        Some(seg) => {
                                            (seg.start_seqs.clone(), seg.end_seq, seg.max_seq)
                                        }
                                        None => (Vec::new(), None, fseq_now),
                                    };
                                    if !self
                                        .record_suppressed_bb_segment(
                                            &wave,
                                            &role,
                                            &thread,
                                            (sup_starts, sup_end, Some(sup_max)),
                                            flushed.unsaved_failure.clone(),
                                        )
                                        .await
                                    {
                                        return;
                                    }
                                    let prior_rows = rows_before_terminal_turn(&working_rows);
                                    self.mirror_outstanding_turns_from_tail(
                                        &wave,
                                        &role,
                                        &thread,
                                        &prior_rows,
                                    )
                                    .await
                                } else {
                                    if let Some(last_seg) = segments.last()
                                        && is_segment_already_suppressed(
                                            last_seg,
                                            &flushed.starts,
                                            &flushed.ends,
                                        )
                                        && !self
                                            .record_suppressed_bb_segment(
                                                &wave,
                                                &role,
                                                &thread,
                                                (
                                                    last_seg.start_seqs.clone(),
                                                    last_seg.end_seq,
                                                    Some(last_seg.max_seq),
                                                ),
                                                flushed.unsaved_failure.clone(),
                                            )
                                            .await
                                    {
                                        return;
                                    }
                                    self.mirror_outstanding_turns_from_tail(
                                        &wave,
                                        &role,
                                        &thread,
                                        &working_rows,
                                    )
                                    .await
                                };
                                if let Some((fail_act, _)) = flushed.unsaved_failure
                                    && self.is_current_live_thread(&wave, &role, &thread).await
                                    && !self
                                        .try_wave_observe(
                                            wave.clone(),
                                            role.clone(),
                                            Observation::Activity {
                                                activity: fail_act.clone(),
                                            },
                                        )
                                        .await
                                {
                                    let _ = self.owner.send(OwnerMsg::QueueUnsavedSuppressions {
                                        wave,
                                        role,
                                        thread,
                                        start_seqs: Vec::new(),
                                        end_seq: None,
                                        floor_seq: Some(effective_floor.unwrap_or(fseq_now)),
                                        failure: Some((
                                            fail_act,
                                            Some(effective_floor.unwrap_or(fseq_now)),
                                        )),
                                    });
                                    return;
                                }
                                if !tail_ok {
                                    let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                                        wave,
                                        role,
                                        thread,
                                        since_end_seq: None,
                                        mark_unknown_dirty: true,
                                    });
                                }
                                return;
                            }
                        }
                        if self.has_queued_terminal_signal(&thread) {
                            let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                                wave,
                                role,
                                thread,
                                since_end_seq: None,
                                mark_unknown_dirty: true,
                            });
                            return;
                        }
                        if self
                            .mirror_outstanding_turns_from_tail(
                                &wave,
                                &role,
                                &thread,
                                &working_rows,
                            )
                            .await
                            && !self.has_queued_terminal_signal(&thread)
                            && self.is_current_live_thread(&wave, &role, &thread).await
                        {
                            if self
                                .try_wave_observe(
                                    wave.clone(),
                                    role.clone(),
                                    Observation::Activity {
                                        activity: Activity::Idle,
                                    },
                                )
                                .await
                            {
                                let _ = self.owner.send(OwnerMsg::ClearUnsavedFailure {
                                    wave,
                                    role,
                                    thread,
                                    settled_seq: max_row_seq,
                                });
                            } else {
                                let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                                    wave,
                                    role,
                                    thread,
                                    since_end_seq: None,
                                    mark_unknown_dirty: true,
                                });
                            }
                        }
                    }
                    protocol::ThreadSignal::Failed => {
                        let msg = error.unwrap_or_else(|| "BB thread failed".into());
                        let is_direct_refusal = stop_reason
                            .as_deref()
                            .is_some_and(rex_wave_api::wave::is_refusal_stop_reason)
                            || rex_wave_api::wave::is_refusal_error_text(&msg);
                        let direct_failure_activity = || {
                            if is_direct_refusal {
                                let reason = match stop_reason.as_deref() {
                                    Some(sr)
                                        if !msg
                                            .to_ascii_lowercase()
                                            .contains(&sr.to_ascii_lowercase()) =>
                                    {
                                        format!("{sr}: {msg}")
                                    }
                                    _ => msg.clone(),
                                };
                                Activity::Refused { reason }
                            } else {
                                Activity::Errored { error: msg.clone() }
                            }
                        };
                        let rows_res = self.fetch_recent_terminal_turn_rows_checked(&thread).await;
                        let row_refusal = if is_direct_refusal {
                            None
                        } else {
                            rows_res
                                .as_ref()
                                .ok()
                                .and_then(|rows| refusal_from_latest_turn(rows))
                        };
                        let activity = if let Some(reason) = row_refusal {
                            Activity::Refused { reason }
                        } else {
                            direct_failure_activity()
                        };
                        self.handle_terminal_failure_signal(
                            (wave, role, attempt, thread),
                            activity,
                            rows_res,
                            (parsed_model, parsed_account),
                            false,
                        )
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
                let Some((wave, role, attempt, retiring)) =
                    self.resolve_worker_identity(&thread, meta.as_ref()).await
                else {
                    return;
                };
                if retiring {
                    return;
                }
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
        let Some((wave, role, _, _)) = self.resolve_worker_identity(&thread, meta.as_ref()).await
        else {
            send_reply(false, format!("unrecognized wave worker thread {thread}")).await;
            return;
        };
        let Some(backend) = self.backend.get().cloned() else {
            send_reply(false, "rex-wave router unavailable".into()).await;
            return;
        };

        let router_handles_park = backend.handles_worker_wait_parking();
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
            aborted_wait: aborted_wait.clone(),
        };

        if tool == "run_code" {
            let Some(runner) = self.code_runner.as_ref() else {
                send_reply(false, "run_code runner is not configured".into()).await;
                return;
            };
            let res = (runner)(worker_client, args).await;
            let key = (wave, role);
            if !router_handles_park && aborted_wait.load(Ordering::Relaxed) {
                self.restore_fallback_working_if_idle(&key).await;
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

    async fn fetch_recent_thread_rows_checked(
        &self,
        thread: &BbThreadId,
    ) -> Result<Vec<serde_json::Value>, HostError> {
        let rows_val = self
            .rpc(protocol::Down::EventsPage {
                req_id: 0,
                thread_id: thread.to_string(),
                after_seq: None,
                before_seq: None,
                limit: 200,
                order: Some(protocol::EventsOrder::Desc),
            })
            .await?;
        Ok(rows_val.as_array().cloned().unwrap_or_default())
    }

    async fn fetch_recent_terminal_turn_rows_checked(
        &self,
        thread: &BbThreadId,
    ) -> Result<Vec<serde_json::Value>, HostError> {
        let mut combined = self.fetch_recent_thread_rows_checked(thread).await?;
        let has_turn_marker = combined.iter().any(|r| {
            matches!(
                r.get("type").and_then(|v| v.as_str()),
                Some("turn/completed" | "turn/started" | "client/turn/start")
            )
        });
        if combined.len() < 200 || has_turn_marker {
            return Ok(combined);
        }
        let mut before_seq = combined
            .iter()
            .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()))
            .min();
        for page_idx in 0..MAX_TURN_BACKSCAN_PAGES {
            let Some(cur_before) = before_seq.filter(|&b| b > 1) else {
                break;
            };
            let rows_val = self
                .rpc(protocol::Down::EventsPage {
                    req_id: 0,
                    thread_id: thread.to_string(),
                    after_seq: None,
                    before_seq: Some(cur_before),
                    limit: 200,
                    order: Some(protocol::EventsOrder::Desc),
                })
                .await?;
            let Some(page) = rows_val.as_array() else {
                break;
            };
            if page.is_empty() {
                break;
            }
            let count = page.len();
            let mut next_before = cur_before;
            let mut saw_start = false;
            for row in page {
                if let Some(s) = row.get("seq").and_then(|v| v.as_u64())
                    && s < next_before
                {
                    next_before = s;
                }
                if matches!(
                    row.get("type").and_then(|v| v.as_str()),
                    Some("turn/started" | "client/turn/start")
                ) {
                    saw_start = true;
                }
                combined.push(row.clone());
            }
            if saw_start || count < 200 || next_before <= 1 || next_before == cur_before {
                break;
            }
            before_seq = Some(next_before);
            if page_idx % 8 == 7 {
                tokio::task::yield_now().await;
            }
        }
        Ok(combined)
    }

    #[cfg(test)]
    async fn fetch_recent_thread_rows(&self, thread: &BbThreadId) -> Vec<serde_json::Value> {
        self.fetch_recent_thread_rows_checked(thread)
            .await
            .unwrap_or_default()
    }

    async fn backfill_latest_turn_rows(
        &self,
        thread: &BbThreadId,
        initial_rows: &[serde_json::Value],
        ever_connected: bool,
    ) -> (Vec<serde_json::Value>, bool, bool) {
        let Some(mut before_seq) = latest_completed_turn_missing_start_boundary(initial_rows)
        else {
            let reached_origin = initial_rows
                .iter()
                .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()))
                .min()
                .is_some_and(|s| s <= 1);
            return (initial_rows.to_vec(), false, reached_origin);
        };
        let mut combined = initial_rows.to_vec();
        let mut complete = false;
        let mut reached_origin = false;
        for _ in 0..MAX_TURN_BACKSCAN_PAGES {
            let Ok(rows_val) = self
                .rpc(protocol::Down::EventsPage {
                    req_id: 0,
                    thread_id: thread.to_string(),
                    after_seq: None,
                    before_seq: Some(before_seq),
                    limit: 200,
                    order: Some(protocol::EventsOrder::Desc),
                })
                .await
            else {
                break;
            };
            let Some(page) = rows_val.as_array() else {
                break;
            };
            if page.is_empty() {
                complete = true;
                reached_origin = true;
                break;
            }
            let count = page.len();
            let prev_before = before_seq;
            let mut found_boundary = false;
            for row in page {
                if let Some(s) = row.get("seq").and_then(|v| v.as_u64())
                    && s < before_seq
                {
                    before_seq = s;
                }
                if matches!(
                    row.get("type").and_then(|v| v.as_str()),
                    Some("turn/started" | "client/turn/start" | "turn/completed")
                ) {
                    found_boundary = true;
                }
                combined.push(row.clone());
            }
            if count < 200 || before_seq <= 1 {
                complete = true;
                reached_origin = true;
                break;
            }
            if found_boundary {
                complete = true;
                break;
            }
            if before_seq == prev_before {
                break;
            }
        }
        let history_incomplete = ever_connected && !complete;
        (combined, history_incomplete, reached_origin)
    }

    async fn fetch_all_thread_rows_through(
        &self,
        thread: &BbThreadId,
        working_rows: &[serde_json::Value],
        already_reached_origin: bool,
        ever_connected: bool,
    ) -> (Vec<serde_json::Value>, bool) {
        if !ever_connected || already_reached_origin {
            return (working_rows.to_vec(), false);
        }
        let Some(mut before_seq) = working_rows
            .iter()
            .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()))
            .min()
            .filter(|&s| s > 1)
        else {
            return (working_rows.to_vec(), false);
        };
        let mut combined = working_rows.to_vec();
        let mut reached_origin = false;
        for _ in 0..MAX_TURN_BACKSCAN_PAGES {
            let Ok(rows_val) = self
                .rpc(protocol::Down::EventsPage {
                    req_id: 0,
                    thread_id: thread.to_string(),
                    after_seq: None,
                    before_seq: Some(before_seq),
                    limit: 200,
                    order: Some(protocol::EventsOrder::Desc),
                })
                .await
            else {
                break;
            };
            let Some(page) = rows_val.as_array() else {
                break;
            };
            if page.is_empty() {
                reached_origin = true;
                break;
            }
            let count = page.len();
            let prev_before = before_seq;
            for row in page {
                if let Some(s) = row.get("seq").and_then(|v| v.as_u64())
                    && s < before_seq
                {
                    before_seq = s;
                }
                combined.push(row.clone());
            }
            if count < 200 || before_seq <= 1 {
                reached_origin = true;
                break;
            }
            if before_seq == prev_before {
                break;
            }
        }
        (combined, !reached_origin)
    }

    async fn is_current_live_thread(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
    ) -> bool {
        let (tx, rx) = oneshot::channel();
        if self
            .owner
            .send(OwnerMsg::LookupByRole {
                wave: wave.clone(),
                role: role.clone(),
                reply: tx,
            })
            .is_err()
        {
            return false;
        }
        match rx.await {
            Ok(Some(rec)) => rec.live && rec.thread.as_ref() == Some(thread),
            Ok(None) => true,
            Err(_) => false,
        }
    }

    async fn mirror_outstanding_turns_from_tail(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        rows: &[serde_json::Value],
    ) -> bool {
        let Some(FlushedSuppressionState {
            starts: hyd_starts,
            ends: hyd_ends,
            ..
        }) = self
            .flush_and_hydrate_bb_suppressions(wave, role, thread)
            .await
        else {
            let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                wave: wave.clone(),
                role: role.clone(),
                thread: thread.clone(),
                since_end_seq: None,
                mark_unknown_dirty: true,
            });
            return false;
        };
        let (cur_tx, cur_rx) = oneshot::channel();
        let _ = self.owner.send(OwnerMsg::WorkerMirrorCursor {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
            reply: cur_tx,
        });
        let (
            _cursor_seq,
            pending_mirror_since,
            last_mirrored_end_seq,
            ever_connected,
            mut suppressed_start_seqs,
            mut suppressed_end_seqs,
        ) = cur_rx
            .await
            .unwrap_or((None, None, 0, false, BTreeSet::new(), BTreeSet::new()));
        suppressed_start_seqs.extend(hyd_starts);
        suppressed_end_seqs.extend(hyd_ends);
        let settled_floor = match (pending_mirror_since, last_mirrored_end_seq) {
            (Some(pend), last_m) if last_m > 0 => last_m.min(pend.saturating_sub(1)),
            (Some(pend), _) => pend.saturating_sub(1),
            (None, last_m) => last_m,
        };
        let floor_opt = (settled_floor > 0).then_some(settled_floor);

        let mut working = rows.to_vec();
        if ever_connected && (floor_opt.is_some() || working.len() >= 200) {
            let mut before_seq = working
                .iter()
                .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()))
                .min();
            for page_idx in 0..MAX_TURN_BACKSCAN_PAGES {
                let Some(cur_before) = before_seq else {
                    break;
                };
                if (settled_floor > 0 && cur_before <= settled_floor.saturating_add(1))
                    || cur_before <= 1
                {
                    break;
                }
                let Ok(rows_val) = self
                    .rpc(protocol::Down::EventsPage {
                        req_id: 0,
                        thread_id: thread.to_string(),
                        after_seq: None,
                        before_seq: Some(cur_before),
                        limit: 200,
                        order: Some(protocol::EventsOrder::Desc),
                    })
                    .await
                else {
                    let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                        wave: wave.clone(),
                        role: role.clone(),
                        thread: thread.clone(),
                        since_end_seq: Some(settled_floor.saturating_add(1)),
                        mark_unknown_dirty: false,
                    });
                    return false;
                };
                let Some(page) = rows_val.as_array() else {
                    break;
                };
                if page.is_empty() {
                    break;
                };
                let count = page.len();
                let mut next_before = cur_before;
                for row in page {
                    if let Some(s) = row.get("seq").and_then(|v| v.as_u64())
                        && s < next_before
                    {
                        next_before = s;
                    }
                    working.push(row.clone());
                }
                if count < 200 || next_before == cur_before {
                    break;
                }
                before_seq = Some(next_before);
                if page_idx % 8 == 7 {
                    tokio::task::yield_now().await;
                }
            }
        }

        let completions: Vec<(u64, Option<u64>)> = mirrorable_turn_completions_in_order(
            &working,
            &suppressed_start_seqs,
            &suppressed_end_seqs,
        )
        .into_iter()
        .filter(|&(end_seq, _)| floor_opt.is_none_or(|fl| end_seq > fl))
        .collect();
        if completions.is_empty() {
            let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                wave: wave.clone(),
                role: role.clone(),
                thread: thread.clone(),
                since_end_seq: None,
                mark_unknown_dirty: false,
            });
            return true;
        }

        let total = completions.len();
        for (idx, (target_end_seq, next_start_at)) in completions.into_iter().enumerate() {
            let is_last = idx + 1 == total;
            let prefix_rows: Vec<serde_json::Value> = if target_end_seq > 0 {
                working
                    .iter()
                    .filter(|r| {
                        r.get("seq").and_then(|v| v.as_u64()).unwrap_or(0) <= target_end_seq
                    })
                    .cloned()
                    .collect()
            } else {
                working.clone()
            };
            if !self
                .mirror_completed_turn_from_rows_inner(
                    wave,
                    role,
                    thread,
                    &prefix_rows,
                    next_start_at,
                    is_last,
                )
                .await
            {
                return false;
            }
        }
        true
    }

    #[cfg(test)]
    async fn mirror_completed_turn_from_rows(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        rows: &[serde_json::Value],
    ) -> bool {
        self.mirror_completed_turn_from_rows_inner(wave, role, thread, rows, None, true)
            .await
    }

    async fn mirror_completed_turn_from_rows_inner(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        thread: &BbThreadId,
        rows: &[serde_json::Value],
        next_turn_start_at: Option<u64>,
        clear_pending_on_settle: bool,
    ) -> bool {
        let _mirror_guard = self.mirror_lock.lock().await;
        let Some(end_seq_hint) = latest_nonfailed_turn_end_seq(rows) else {
            if clear_pending_on_settle {
                let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                    wave: wave.clone(),
                    role: role.clone(),
                    thread: thread.clone(),
                    since_end_seq: None,
                    mark_unknown_dirty: false,
                });
            }
            return true;
        };
        let allow_watermark_advance = clear_pending_on_settle && next_turn_start_at.is_none();
        let mark_pending = || {
            let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                wave: wave.clone(),
                role: role.clone(),
                thread: thread.clone(),
                since_end_seq: Some(end_seq_hint.max(1)),
                mark_unknown_dirty: false,
            });
            false
        };
        let settle_without_post = |seq: Seq,
                                   watermark_floor: Seq,
                                   end_seq: Option<u64>,
                                   anchor: Option<String>,
                                   claimed_seqs: Vec<Seq>| {
            let next_seq = if allow_watermark_advance {
                seq
            } else {
                watermark_floor
            };
            let _ = self.owner.send(OwnerMsg::AdvanceMirrorWatermark {
                wave: wave.clone(),
                role: role.clone(),
                thread: thread.clone(),
                seq: next_seq,
                end_seq,
                anchor,
                claimed_seqs,
                clear_pending: clear_pending_on_settle,
            });
            true
        };

        let Some(FlushedSuppressionState {
            starts: suppressed_starts,
            ends: suppressed_ends,
            ..
        }) = self
            .flush_and_hydrate_bb_suppressions(wave, role, thread)
            .await
        else {
            return mark_pending();
        };
        if end_seq_hint > 0
            && !mirrorable_turn_completions_in_order(rows, &suppressed_starts, &suppressed_ends)
                .iter()
                .any(|&(es, _)| es == end_seq_hint)
        {
            return settle_without_post(
                Seq::ORIGIN,
                Seq::ORIGIN,
                Some(end_seq_hint),
                None,
                Vec::new(),
            );
        }

        let Some(backend) = self.backend.get() else {
            return mark_pending();
        };
        let sys = Principal::System {
            part: SystemPart::BbBridge,
        };
        let snap = match backend
            .call_as(sys.clone(), WaveView { wave: wave.clone() }.into_wire())
            .await
        {
            Ok(WireReply::WaveView(snap)) => snap,
            Err(ApiError::NotFound { .. }) => {
                return settle_without_post(Seq::ORIGIN, Seq::ORIGIN, None, None, Vec::new());
            }
            _ => return mark_pending(),
        };
        let room = rex_wave_waves::wave_room(wave, &snap.spec.project);
        let worker_principal = Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        };

        let anchor_hint = if end_seq_hint > 0 {
            format!("e{end_seq_hint}")
        } else {
            String::new()
        };
        let (wm_tx, wm_rx) = oneshot::channel();
        let _ = self.owner.send(OwnerMsg::MirrorState {
            wave: wave.clone(),
            role: role.clone(),
            thread: thread.clone(),
            end_seq: end_seq_hint,
            anchor: anchor_hint.clone(),
            reply: wm_tx,
        });
        let (watermark, already_mirrored, ever_connected, claimed_direct_posts) = wm_rx
            .await
            .unwrap_or((Seq::ORIGIN, false, false, BTreeSet::new()));
        if already_mirrored {
            return settle_without_post(
                watermark,
                watermark,
                (end_seq_hint > 0).then_some(end_seq_hint),
                (!anchor_hint.is_empty()).then_some(anchor_hint),
                Vec::new(),
            );
        }

        let (working_rows, history_incomplete, reached_origin) = self
            .backfill_latest_turn_rows(thread, rows, ever_connected)
            .await;
        if history_incomplete {
            return mark_pending();
        }
        let check = inspect_latest_completed_turn(&working_rows);

        let stream = StreamId::Room { room: room.clone() };
        let mut cursor = watermark;
        let mut room_head = watermark;
        let mut safe_watermark = watermark;
        let mut watermark_pinned = !allow_watermark_advance;
        let mut effective_next_start_at = next_turn_start_at;
        let mut checked_live_tail_after_turn = false;
        let completed = match check {
            TurnMirrorCheck::Ignore => {
                return settle_without_post(
                    watermark,
                    watermark,
                    (end_seq_hint > 0).then_some(end_seq_hint),
                    (!anchor_hint.is_empty()).then_some(anchor_hint),
                    Vec::new(),
                );
            }
            TurnMirrorCheck::PostedViaTool {
                turn_start_at,
                turn_end_at,
                turn_end_seq,
            } => {
                let mut tool_reached_end = false;
                let mut tool_claimed = Vec::new();
                loop {
                    let Ok(WireReply::Read(page)) = backend
                        .call_as(
                            sys.clone(),
                            Read::stream(stream.clone(), cursor, Some(500)).into_wire(),
                        )
                        .await
                    else {
                        return mark_pending();
                    };
                    let Some(last) = page.entries.last() else {
                        tool_reached_end = true;
                        break;
                    };
                    if last.seq <= cursor {
                        break;
                    }
                    cursor = last.seq;
                    for env in &page.entries {
                        if env.by == worker_principal
                            && let Event::Room(RoomEvent::Posted { idem, origin, .. }) = &env.event
                        {
                            let is_mirrored_row = *origin
                                == rex_wave_api::room::PostOrigin::MirroredTurn
                                || idem
                                    .as_ref()
                                    .is_some_and(rex_wave_waves::is_legacy_mirror_idem_key);
                            if !is_mirrored_row {
                                if ever_connected
                                    && effective_next_start_at.is_none()
                                    && !checked_live_tail_after_turn
                                    && turn_end_seq > 0
                                    && (turn_start_at == 0 || env.at.0 >= turn_start_at)
                                {
                                    checked_live_tail_after_turn = true;
                                    let Ok(tail_val) = self
                                        .rpc(protocol::Down::EventsPage {
                                            req_id: 0,
                                            thread_id: thread.to_string(),
                                            after_seq: Some(turn_end_seq),
                                            before_seq: None,
                                            limit: 20,
                                            order: None,
                                        })
                                        .await
                                    else {
                                        return mark_pending();
                                    };
                                    if let Some(tail_rows) = tail_val.as_array() {
                                        let next_start = tail_rows
                                            .iter()
                                            .filter(|r| {
                                                r.get("seq").and_then(|v| v.as_u64())
                                                    > Some(turn_end_seq)
                                                    && matches!(
                                                        r.get("type").and_then(|v| v.as_str()),
                                                        Some("turn/started" | "client/turn/start")
                                                    )
                                            })
                                            .filter_map(|r| {
                                                r.get("createdAt").and_then(|v| v.as_u64())
                                            })
                                            .find(|&at| at > 0);
                                        if let Some(ns) = next_start {
                                            watermark_pinned = true;
                                            effective_next_start_at = Some(ns);
                                        }
                                    }
                                }
                                let post_after = (turn_end_at > 0 && env.at.0 > turn_end_at)
                                    || effective_next_start_at.is_some_and(|ns| env.at.0 >= ns);
                                if post_after {
                                    watermark_pinned = true;
                                } else if !claimed_direct_posts.contains(&env.seq)
                                    && (turn_end_at > 0
                                        || effective_next_start_at.is_some()
                                        || tool_claimed.is_empty())
                                {
                                    tool_claimed.push(env.seq);
                                }
                            }
                        }
                        if !watermark_pinned {
                            safe_watermark = env.seq;
                        }
                    }
                    if page.entries.len() < 500 {
                        tool_reached_end = true;
                        break;
                    }
                }
                if !tool_reached_end {
                    return mark_pending();
                }
                return settle_without_post(
                    safe_watermark,
                    watermark,
                    (end_seq_hint > 0).then_some(end_seq_hint),
                    (!anchor_hint.is_empty()).then_some(anchor_hint),
                    tool_claimed,
                );
            }
            TurnMirrorCheck::Unposted(c) => c,
        };

        let Ok(idem_key) = rex_wave_waves::mirror_idem_key(wave, role, thread, &completed.anchor)
        else {
            return settle_without_post(
                watermark,
                watermark,
                (completed.turn_end_seq > 0).then_some(completed.turn_end_seq),
                Some(completed.anchor),
                Vec::new(),
            );
        };
        let trimmed = truncate_with_notice(&completed.text, 64 * 1024);

        let mut already_posted = false;
        let mut needs_shared_legacy_reconcile = false;
        let mut reached_end = false;
        let mut claimed_seqs = Vec::new();
        loop {
            let Ok(WireReply::Read(page)) = backend
                .call_as(
                    sys.clone(),
                    Read::stream(stream.clone(), cursor, Some(500)).into_wire(),
                )
                .await
            else {
                return mark_pending();
            };
            let Some(last) = page.entries.last() else {
                reached_end = true;
                break;
            };
            if last.seq <= cursor {
                break;
            }
            cursor = last.seq;
            room_head = last.seq;
            let count = page.entries.len();
            for env in &page.entries {
                if let Event::Room(RoomEvent::Posted {
                    body, idem, origin, ..
                }) = &env.event
                {
                    if idem.as_ref() == Some(&idem_key) {
                        if !watermark_pinned {
                            safe_watermark = env.seq;
                        }
                        already_posted = true;
                        break;
                    }
                    let is_mirrored_row = *origin == rex_wave_api::room::PostOrigin::MirroredTurn
                        || idem
                            .as_ref()
                            .is_some_and(rex_wave_waves::is_legacy_mirror_idem_key);
                    if is_mirrored_row {
                        let legacy_min_at = if completed.turn_start_at > 0 {
                            completed.turn_start_at
                        } else {
                            completed.turn_end_at
                        };
                        if env.by == worker_principal
                            && let Some(existing_idem) = idem.as_ref()
                            && let Some(exact) = rex_wave_waves::classify_legacy_bb_mirror_idem(
                                existing_idem,
                                wave,
                                role,
                                thread,
                                completed.turn_start_seq,
                                completed.turn_end_seq,
                            )
                        {
                            if exact {
                                if !watermark_pinned {
                                    safe_watermark = env.seq;
                                }
                                already_posted = true;
                                break;
                            }
                            if body.as_str() == trimmed
                                && (legacy_min_at == 0 || env.at.0 >= legacy_min_at)
                            {
                                needs_shared_legacy_reconcile = true;
                            }
                        }
                    } else if env.by == worker_principal
                        && !claimed_direct_posts.contains(&env.seq)
                        && (completed.turn_start_at == 0 || env.at.0 >= completed.turn_start_at)
                    {
                        if ever_connected
                            && effective_next_start_at.is_none()
                            && !checked_live_tail_after_turn
                            && completed.turn_end_seq > 0
                        {
                            checked_live_tail_after_turn = true;
                            let Ok(tail_val) = self
                                .rpc(protocol::Down::EventsPage {
                                    req_id: 0,
                                    thread_id: thread.to_string(),
                                    after_seq: Some(completed.turn_end_seq),
                                    before_seq: None,
                                    limit: 20,
                                    order: None,
                                })
                                .await
                            else {
                                return mark_pending();
                            };
                            if let Some(tail_rows) = tail_val.as_array() {
                                let next_start = tail_rows
                                    .iter()
                                    .filter(|r| {
                                        r.get("seq").and_then(|v| v.as_u64())
                                            > Some(completed.turn_end_seq)
                                            && matches!(
                                                r.get("type").and_then(|v| v.as_str()),
                                                Some("turn/started" | "client/turn/start")
                                            )
                                    })
                                    .filter_map(|r| r.get("createdAt").and_then(|v| v.as_u64()))
                                    .find(|&at| at > 0);
                                if let Some(ns) = next_start {
                                    watermark_pinned = true;
                                    effective_next_start_at = Some(ns);
                                }
                            }
                        }
                        let bounded_window =
                            completed.turn_end_at > 0 || effective_next_start_at.is_some();
                        let post_after_turn = (completed.turn_end_at > 0
                            && env.at.0 > completed.turn_end_at)
                            || effective_next_start_at.is_some_and(|ns| env.at.0 >= ns);
                        if post_after_turn {
                            watermark_pinned = true;
                        } else {
                            if !watermark_pinned {
                                safe_watermark = env.seq;
                            }
                            claimed_seqs.push(env.seq);
                            already_posted = true;
                            if !bounded_window {
                                break;
                            }
                        }
                    }
                }
                if !watermark_pinned {
                    safe_watermark = env.seq;
                }
            }
            if already_posted && !(completed.turn_end_at > 0 || effective_next_start_at.is_some()) {
                break;
            }
            if count < 500 {
                reached_end = true;
                break;
            }
        }

        if !already_posted && !reached_end {
            return mark_pending();
        }

        if !already_posted && needs_shared_legacy_reconcile {
            let (full_thread_rows, full_incomplete) = self
                .fetch_all_thread_rows_through(
                    thread,
                    &working_rows,
                    reached_origin,
                    ever_connected,
                )
                .await;
            if full_incomplete {
                return mark_pending();
            }
            let mut all_worker_posts = Vec::new();
            let mut full_cursor = Seq::ORIGIN;
            let mut full_room_end = false;
            loop {
                let Ok(WireReply::Read(page)) = backend
                    .call_as(
                        sys.clone(),
                        Read::stream(stream.clone(), full_cursor, Some(500)).into_wire(),
                    )
                    .await
                else {
                    return mark_pending();
                };
                let Some(last) = page.entries.last() else {
                    full_room_end = true;
                    break;
                };
                if last.seq <= full_cursor {
                    break;
                }
                full_cursor = last.seq;
                if room_head < last.seq {
                    room_head = last.seq;
                }
                let count = page.entries.len();
                for env in &page.entries {
                    if env.by != worker_principal {
                        continue;
                    }
                    if let Event::Room(RoomEvent::Posted {
                        body, idem, origin, ..
                    }) = &env.event
                    {
                        let is_mirrored = *origin == rex_wave_api::room::PostOrigin::MirroredTurn
                            || idem
                                .as_ref()
                                .is_some_and(rex_wave_waves::is_legacy_mirror_idem_key);
                        all_worker_posts.push(WorkerRoomPost {
                            at: env.at,
                            body: body.as_str().to_owned(),
                            idem: idem.clone(),
                            is_mirrored,
                        });
                    }
                }
                if count < 500 {
                    full_room_end = true;
                    break;
                }
            }
            if !full_room_end {
                return mark_pending();
            }
            let all_turns =
                inspect_all_unposted_completed_turns(&full_thread_rows, completed.turn_end_seq);
            already_posted = reconcile_shared_legacy_mirror(
                wave,
                role,
                thread,
                &all_turns,
                &all_worker_posts,
                completed.turn_end_seq,
            );
        }

        if already_posted {
            return settle_without_post(
                safe_watermark,
                watermark,
                (completed.turn_end_seq > 0).then_some(completed.turn_end_seq),
                Some(completed.anchor),
                claimed_seqs,
            );
        }

        let Ok(body) = Body::parse(trimmed) else {
            return settle_without_post(
                safe_watermark,
                watermark,
                (completed.turn_end_seq > 0).then_some(completed.turn_end_seq),
                Some(completed.anchor),
                Vec::new(),
            );
        };
        if self.has_queued_terminal_signal(thread) {
            return mark_pending();
        }
        let post_cmd = RoomPost::mirrored_turn(RoomRef::Id { id: room.clone() }, body, idem_key);
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
                if self.has_queued_terminal_signal(thread) {
                    return mark_pending();
                }
                backend
                    .call_as(worker_principal, post_cmd.into_wire())
                    .await
            }
            other => other,
        };
        if let Ok(WireReply::RoomPost(posted)) = post_res {
            let posted_head = if !watermark_pinned && posted.seq.0 == room_head.0.saturating_add(1)
            {
                posted.seq
            } else {
                safe_watermark
            };
            return settle_without_post(
                posted_head,
                watermark,
                (completed.turn_end_seq > 0).then_some(completed.turn_end_seq),
                Some(completed.anchor),
                Vec::new(),
            );
        }
        mark_pending()
    }

    async fn reconcile_live_workers(&self) {
        let _reconcile_guard = self.reconcile_lock.lock().await;
        let (tx, rx) = oneshot::channel();
        if self
            .owner
            .send(OwnerMsg::ListLiveWorkers { reply: tx })
            .is_err()
        {
            return;
        }
        let Ok(workers) = rx.await else { return };
        'workers: for (
            wave,
            role,
            attempt,
            thread,
            tracked_activity,
            mut cursor_seq,
            pending_mirror_since,
            last_mirrored_end_seq,
            mut suppressed_start_seqs,
            mut suppressed_end_seqs,
        ) in workers
        {
            if self.has_queued_terminal_signal(&thread) {
                let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                    wave,
                    role,
                    thread,
                    since_end_seq: None,
                    mark_unknown_dirty: true,
                });
                continue 'workers;
            }
            if !self
                .try_wave_observe(
                    wave.clone(),
                    role.clone(),
                    Observation::Spawned {
                        attempt,
                        thread: thread.clone(),
                    },
                )
                .await
            {
                continue 'workers;
            }
            let Some(mut flushed) = self
                .flush_and_hydrate_bb_suppressions(&wave, &role, &thread)
                .await
            else {
                continue 'workers;
            };
            suppressed_start_seqs.extend(flushed.starts.iter().copied());
            suppressed_end_seqs.extend(flushed.ends.iter().copied());
            flushed.starts = suppressed_start_seqs.clone();
            flushed.ends = suppressed_end_seqs.clone();
            let min_unanchored_base = flushed
                .open_tickets
                .iter()
                .map(|u| u.baseline_seq.unwrap_or(0))
                .min();
            let initial_cursor = cursor_seq;
            let had_prev_cursor = initial_cursor.is_some();
            let mut forward_rows: Vec<serde_json::Value> = Vec::new();
            let mut backscan_rows: Vec<serde_json::Value> = Vec::new();
            let mut thread_gone: Option<Observation> = None;
            let mut saw_new_turn_start = false;
            let mut fetch_complete = true;
            if let Some(init_cur) = initial_cursor {
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
                                forward_rows.push(row.clone());
                            }
                            if count < 200 || cursor_seq == prev {
                                break;
                            }
                        }
                        Err(HostError::Rejected(msg)) => {
                            thread_gone = classify_missing_or_archived_thread(&msg);
                            fetch_complete = false;
                            break;
                        }
                        Err(HostError::Unreachable(_)) => {
                            fetch_complete = false;
                            break;
                        }
                    }
                }
                let forward_has_completion = forward_rows
                    .iter()
                    .any(|r| r.get("type").and_then(|v| v.as_str()) == Some("turn/completed"));
                let forward_starts_with_turn_started = forward_rows
                    .first()
                    .and_then(|r| r.get("type").and_then(|v| v.as_str()))
                    == Some("turn/started");
                let needs_pending_backscan =
                    pending_mirror_since.is_some_and(|pend| pend <= init_cur);
                let needs_unanchored_backscan =
                    min_unanchored_base.is_some_and(|base| base < init_cur);
                let needs_failure_boundary_backscan = (flushed.anchored_failure_floor.is_some()
                    || !suppressed_start_seqs.is_empty())
                    && !forward_rows.is_empty();
                let needs_turn_start_backscan = forward_has_completion && !saw_new_turn_start;
                if fetch_complete
                    && (needs_pending_backscan
                        || needs_unanchored_backscan
                        || needs_failure_boundary_backscan
                        || forward_starts_with_turn_started
                        || needs_turn_start_backscan)
                {
                    let mut before_seq = Some(init_cur.saturating_add(1));
                    let mut saw_turn_boundary = false;
                    let mut saw_completed_boundary = false;
                    for page_idx in 0..MAX_TURN_BACKSCAN_PAGES {
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
                                };
                                let count = rows.len();
                                let prev_before = before_seq;
                                for row in rows {
                                    if let Some(s) = row.get("seq").and_then(|v| v.as_u64()) {
                                        cursor_seq = Some(cursor_seq.map_or(s, |cur| cur.max(s)));
                                        before_seq = Some(before_seq.map_or(s, |cur| cur.min(s)));
                                    }
                                    let row_ty = row.get("type").and_then(|v| v.as_str());
                                    if row_ty == Some("turn/completed") {
                                        saw_completed_boundary = true;
                                        saw_turn_boundary = true;
                                    } else if matches!(
                                        row_ty,
                                        Some("turn/started" | "client/turn/start")
                                    ) {
                                        saw_turn_boundary = true;
                                    }
                                    backscan_rows.push(row.clone());
                                }
                                let covered_pending = pending_mirror_since
                                    .filter(|&p| p <= init_cur)
                                    .is_none_or(|pend| before_seq.is_some_and(|b| b <= pend));
                                let covered_unanchored = min_unanchored_base
                                    .filter(|&b| b < init_cur)
                                    .is_none_or(|base| {
                                        if base == 0 {
                                            count < 200 || before_seq.is_some_and(|b| b <= 1)
                                        } else {
                                            before_seq.is_some_and(|b| b <= base.saturating_add(1))
                                        }
                                    });
                                let covered_failure_boundary = !(needs_failure_boundary_backscan
                                    || forward_starts_with_turn_started)
                                    || saw_completed_boundary;
                                let covered_turn_start =
                                    !needs_turn_start_backscan || saw_turn_boundary;
                                if (covered_pending
                                    && covered_unanchored
                                    && covered_failure_boundary
                                    && covered_turn_start)
                                    || count < 200
                                    || before_seq.is_some_and(|b| b <= 1)
                                    || before_seq == prev_before
                                {
                                    break;
                                }
                                if page_idx % 8 == 7 {
                                    tokio::task::yield_now().await;
                                }
                            }
                            Err(HostError::Rejected(msg)) => {
                                thread_gone = classify_missing_or_archived_thread(&msg);
                                fetch_complete = false;
                                break;
                            }
                            Err(HostError::Unreachable(_)) => {
                                fetch_complete = false;
                                break;
                            }
                        }
                    }
                }
            } else {
                let base_settled = match (pending_mirror_since, last_mirrored_end_seq) {
                    (Some(pend), last_m) if last_m > 0 => last_m.min(pend.saturating_sub(1)),
                    (Some(pend), _) => pend.saturating_sub(1),
                    (None, last_m) => last_m,
                };
                let settled_floor = match min_unanchored_base {
                    Some(ub) => base_settled.min(ub),
                    None => base_settled,
                };
                let mut before_seq: Option<u64> = None;
                let mut reached_turn_start = false;
                'desc: for page_idx in 0..MAX_TURN_BACKSCAN_PAGES {
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
                            let count = rows.len();
                            let prev_before = before_seq;
                            for row in rows {
                                if let Some(s) = row.get("seq").and_then(|v| v.as_u64()) {
                                    cursor_seq = Some(cursor_seq.map_or(s, |cur| cur.max(s)));
                                    before_seq = Some(before_seq.map_or(s, |cur| cur.min(s)));
                                }
                                forward_rows.push(row.clone());
                                if matches!(
                                    row.get("type").and_then(|v| v.as_str()),
                                    Some("turn/started" | "client/turn/start")
                                ) {
                                    reached_turn_start = true;
                                }
                            }
                            let reached_settled_floor = settled_floor > 0
                                && reached_turn_start
                                && before_seq.is_some_and(|b| b <= settled_floor);
                            let reached_origin = count < 200
                                || before_seq.is_some_and(|b| b <= 1)
                                || before_seq == prev_before;
                            if reached_settled_floor || reached_origin {
                                break 'desc;
                            }
                            if page_idx % 8 == 7 {
                                tokio::task::yield_now().await;
                            }
                        }
                        Err(HostError::Rejected(msg)) => {
                            thread_gone = classify_missing_or_archived_thread(&msg);
                            fetch_complete = false;
                            break;
                        }
                        Err(HostError::Unreachable(_)) => {
                            fetch_complete = false;
                            break;
                        }
                    }
                }
            }
            if self.has_queued_terminal_signal(&thread) {
                let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                    wave,
                    role,
                    thread,
                    since_end_seq: None,
                    mark_unknown_dirty: true,
                });
                continue 'workers;
            }
            if !self.is_current_live_thread(&wave, &role, &thread).await {
                continue;
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
            if !fetch_complete {
                continue;
            }
            let mut collected_rows = Vec::with_capacity(backscan_rows.len() + forward_rows.len());
            collected_rows.extend(backscan_rows.iter().cloned());
            collected_rows.extend(forward_rows.iter().cloned());
            let max_collected_seq = collected_rows
                .iter()
                .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()))
                .max()
                .or(cursor_seq);
            let (row_model, row_account) = extract_runtime_meta_from_rows(&collected_rows);
            self.observe_runtime_meta_if_present(&wave, &role, row_model, row_account)
                .await;
            let Some((unresolved_act, unresolved_base, has_unresolved)) = self
                .resolve_unanchored_failures_in_rows(
                    &wave,
                    &role,
                    &thread,
                    &collected_rows,
                    &mut flushed,
                )
                .await
            else {
                continue 'workers;
            };
            suppressed_start_seqs = flushed.starts.clone();
            suppressed_end_seqs = flushed.ends.clone();
            if has_unresolved {
                if let Some(safe_max) = unresolved_base.filter(|&b| b > 0) {
                    let safe_rows: Vec<serde_json::Value> = collected_rows
                        .iter()
                        .filter(|r| r.get("seq").and_then(|v| v.as_u64()).unwrap_or(0) <= safe_max)
                        .cloned()
                        .collect();
                    let _ = self
                        .mirror_outstanding_turns_from_tail(&wave, &role, &thread, &safe_rows)
                        .await;
                    let _ = self.owner.send(OwnerMsg::AdvanceSeq {
                        thread: thread.clone(),
                        seq: safe_max,
                    });
                }
                if let Some(act) = unresolved_act
                    && self.is_current_live_thread(&wave, &role, &thread).await
                    && self
                        .try_wave_observe(
                            wave.clone(),
                            role.clone(),
                            Observation::Activity { activity: act },
                        )
                        .await
                {
                    let _ = self.owner.send(OwnerMsg::MarkTicketActivityCommitted {
                        wave: wave.clone(),
                        role: role.clone(),
                        thread: thread.clone(),
                    });
                }
                let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                    wave,
                    role,
                    thread,
                    since_end_seq: None,
                    mark_unknown_dirty: true,
                });
                continue 'workers;
            }
            let auth_terminal_failure = matches!(
                current_activity,
                Activity::Refused { .. } | Activity::Errored { .. }
            );
            let (
                active_unsaved_failure,
                suppress_historical_completions,
                anchor_floor_after,
                clear_failure_after,
            ) = match (flushed.anchored_failure_floor, flushed.unsaved_failure) {
                (Some(fseq), unsaved_opt) => {
                    if has_unsuppressed_turn_after(
                        &collected_rows,
                        &suppressed_start_seqs,
                        &suppressed_end_seqs,
                        Some(fseq),
                    ) {
                        (None, false, false, true)
                    } else {
                        (unsaved_opt, true, false, false)
                    }
                }
                (None, Some((act, Some(fseq)))) => {
                    if has_unsuppressed_turn_after(
                        &collected_rows,
                        &suppressed_start_seqs,
                        &suppressed_end_seqs,
                        Some(fseq),
                    ) {
                        (None, false, false, true)
                    } else {
                        (Some((act, Some(fseq))), true, true, false)
                    }
                }
                (None, Some((act, None))) => (
                    Some((act, Some(max_collected_seq.unwrap_or(0)))),
                    true,
                    true,
                    false,
                ),
                (None, None) => {
                    if auth_terminal_failure {
                        let max_sup = suppressed_start_seqs
                            .last()
                            .copied()
                            .into_iter()
                            .chain(suppressed_end_seqs.last().copied())
                            .max();
                        let recovered = match max_sup {
                            Some(fseq) => has_unsuppressed_turn_after(
                                &collected_rows,
                                &suppressed_start_seqs,
                                &suppressed_end_seqs,
                                Some(fseq),
                            ),
                            None => had_prev_cursor && saw_new_turn_start,
                        };
                        (None, !recovered, !recovered, recovered)
                    } else {
                        (None, false, false, false)
                    }
                }
            };
            let mut derived_activity =
                if !had_prev_cursor || !forward_rows.is_empty() || clear_failure_after {
                    derive_latest_turn_activity(&collected_rows)
                } else {
                    None
                };
            if matches!(
                current_activity,
                Activity::Blocked { .. } | Activity::Parked { .. }
            ) && matches!(derived_activity, Some(Activity::Working))
            {
                derived_activity = None;
            }
            let mirror_rows: std::borrow::Cow<'_, [serde_json::Value]> =
                if suppress_historical_completions {
                    let segments = extract_bb_turn_segments(&collected_rows);
                    if let Some(last_seg) = segments.last() {
                        let needs_upgrade = anchor_floor_after
                            || matches!(derived_activity, Some(Activity::Idle))
                            || is_segment_already_suppressed(
                                last_seg,
                                &suppressed_start_seqs,
                                &suppressed_end_seqs,
                            );
                        if needs_upgrade {
                            if !self
                                .record_suppressed_bb_segment(
                                    &wave,
                                    &role,
                                    &thread,
                                    (
                                        last_seg.start_seqs.clone(),
                                        last_seg.end_seq,
                                        Some(last_seg.max_seq),
                                    ),
                                    active_unsaved_failure.clone(),
                                )
                                .await
                            {
                                continue 'workers;
                            }
                            for &s in &last_seg.start_seqs {
                                suppressed_start_seqs.insert(s);
                            }
                            if let Some(e) = last_seg.end_seq {
                                suppressed_end_seqs.insert(e);
                            }
                        }
                    } else if anchor_floor_after
                        && !self
                            .record_suppressed_bb_segment(
                                &wave,
                                &role,
                                &thread,
                                (Vec::new(), None, Some(max_collected_seq.unwrap_or(0))),
                                active_unsaved_failure.clone(),
                            )
                            .await
                    {
                        continue 'workers;
                    }
                    if matches!(derived_activity, Some(Activity::Idle | Activity::Working)) {
                        derived_activity = None;
                    }
                    std::borrow::Cow::Owned(rows_before_terminal_turn(&collected_rows))
                } else {
                    std::borrow::Cow::Borrowed(&collected_rows)
                };
            if let Some((ref fail_act, _)) = active_unsaved_failure {
                derived_activity = Some(fail_act.clone());
            }

            let floor = match (pending_mirror_since, initial_cursor, last_mirrored_end_seq) {
                (Some(pend), Some(cur), _) => cur.min(pend.saturating_sub(1)),
                (Some(pend), None, last_m) => {
                    if last_m > 0 {
                        last_m.min(pend.saturating_sub(1))
                    } else {
                        pend.saturating_sub(1)
                    }
                }
                (None, Some(cur), last_m) => {
                    if last_m > 0 {
                        cur.min(last_m)
                    } else {
                        cur
                    }
                }
                (None, None, last_m) => last_m,
            };
            let completions: Vec<(u64, Option<u64>)> = mirrorable_turn_completions_in_order(
                &mirror_rows,
                &suppressed_start_seqs,
                &suppressed_end_seqs,
            )
            .into_iter()
            .filter(|&(end_seq, _)| end_seq > floor)
            .collect();
            let total = completions.len();
            for (idx, (target_end_seq, next_start_at)) in completions.into_iter().enumerate() {
                if self.has_queued_terminal_signal(&thread) {
                    let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                        wave,
                        role,
                        thread,
                        since_end_seq: None,
                        mark_unknown_dirty: true,
                    });
                    continue 'workers;
                }
                let is_last = idx + 1 == total;
                let prefix_rows: Vec<serde_json::Value> = if target_end_seq > 0 {
                    mirror_rows
                        .iter()
                        .filter(|r| {
                            r.get("seq").and_then(|v| v.as_u64()).unwrap_or(0) <= target_end_seq
                        })
                        .cloned()
                        .collect()
                } else {
                    mirror_rows.to_vec()
                };
                if !self
                    .mirror_completed_turn_from_rows_inner(
                        &wave,
                        &role,
                        &thread,
                        &prefix_rows,
                        next_start_at,
                        is_last,
                    )
                    .await
                {
                    continue 'workers;
                }
                if target_end_seq > 0 {
                    let _ = self.owner.send(OwnerMsg::AdvanceSeq {
                        thread: thread.clone(),
                        seq: target_end_seq,
                    });
                }
            }
            if self.has_queued_terminal_signal(&thread) {
                let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                    wave,
                    role,
                    thread,
                    since_end_seq: None,
                    mark_unknown_dirty: true,
                });
                continue 'workers;
            }
            if total == 0 && pending_mirror_since.is_some() {
                let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                    wave: wave.clone(),
                    role: role.clone(),
                    thread: thread.clone(),
                    since_end_seq: None,
                    mark_unknown_dirty: false,
                });
            }

            let is_clean_idle = matches!(derived_activity, Some(Activity::Idle));
            if let Some(activity) = derived_activity
                && self.is_current_live_thread(&wave, &role, &thread).await
                && !self
                    .try_wave_observe(
                        wave.clone(),
                        role.clone(),
                        Observation::Activity {
                            activity: activity.clone(),
                        },
                    )
                    .await
            {
                if matches!(
                    activity,
                    Activity::Refused { .. } | Activity::Errored { .. }
                ) {
                    let _ = self.owner.send(OwnerMsg::QueueUnsavedSuppressions {
                        wave: wave.clone(),
                        role: role.clone(),
                        thread: thread.clone(),
                        start_seqs: Vec::new(),
                        end_seq: None,
                        floor_seq: Some(cursor_seq.unwrap_or(0)),
                        failure: Some((activity, Some(cursor_seq.unwrap_or(0)))),
                    });
                } else {
                    let _ = self.owner.send(OwnerMsg::SetPendingMirror {
                        wave: wave.clone(),
                        role: role.clone(),
                        thread: thread.clone(),
                        since_end_seq: None,
                        mark_unknown_dirty: true,
                    });
                }
                continue 'workers;
            }
            if clear_failure_after || is_clean_idle {
                let settled_seq = if is_clean_idle {
                    max_collected_seq
                } else {
                    None
                };
                let _ = self.owner.send(OwnerMsg::ClearUnsavedFailure {
                    wave: wave.clone(),
                    role: role.clone(),
                    thread: thread.clone(),
                    settled_seq,
                });
            }
            if let Some(seq) = cursor_seq {
                let _ = self.owner.send(OwnerMsg::AdvanceSeq {
                    thread: thread.clone(),
                    seq,
                });
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
            let _ = self.owner.send(OwnerMsg::AdvanceFeedSeq {
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
            && let Some(ref th) = rec.thread
            && is_bb_thread_id(th)
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
                        WorkerPhase::Live { thread, activity } if is_bb_thread_id(&thread) => {
                            let _ = self.owner.send(OwnerMsg::BindThread {
                                wave: wave.clone(),
                                role: role.clone(),
                                attempt: 1,
                                thread: thread.clone(),
                            });
                            Ok(TrackedWorker::new_empty(1, Some(thread), true, activity))
                        }
                        WorkerPhase::Retiring { thread, .. }
                        | WorkerPhase::Retired {
                            thread: Some(thread),
                            ..
                        } if is_bb_thread_id(&thread) => Ok(TrackedWorker::new_empty(
                            1,
                            Some(thread),
                            false,
                            Activity::Idle,
                        )),
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
        let initial_seq = match rec.feed_seq.or(rec.last_seq) {
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
                                    let _ = owner.send(OwnerMsg::AdvanceFeedSeq { thread: thread.clone(), seq: ev.seq.0 });
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

#[derive(Clone)]
pub struct BbHost(pub Arc<BbBridge>);

impl AgentHost for BbHost {
    fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<BbThreadId, HostError>> {
        let bridge = self.0.clone();
        Box::pin(async move { bridge.spawn_worker_thread(req).await })
    }

    fn tell(
        &self,
        thread: &BbThreadId,
        body: &Body,
        mode: SteerDeliveryMode,
    ) -> BoxFut<'_, Result<(), HostError>> {
        let bridge = self.0.clone();
        let down = protocol::Down::Send {
            req_id: 0,
            thread_id: thread.to_string(),
            mode: send_mode(mode).into(),
            text: body.as_str().into(),
        };
        Box::pin(async move { bridge.rpc(down).await.map(|_| ()) })
    }

    fn resolve(
        &self,
        thread: &BbThreadId,
        interaction: &BbInteractionId,
        resolution: &InteractionResolution,
    ) -> BoxFut<'_, Result<(), HostError>> {
        let bridge = self.0.clone();
        let down = protocol::Down::Resolve {
            req_id: 0,
            thread_id: thread.to_string(),
            interaction_id: interaction.to_string(),
            resolution: resolution_json(resolution),
        };
        Box::pin(async move { bridge.rpc(down).await.map(|_| ()) })
    }

    fn retire(&self, thread: &BbThreadId) -> BoxFut<'_, Result<(), HostError>> {
        let bridge = self.0.clone();
        let down = protocol::Down::Retire {
            req_id: 0,
            thread_id: thread.to_string(),
        };
        Box::pin(async move { bridge.rpc(down).await.map(|_| ()) })
    }

    fn check_live(
        &self,
        thread: &BbThreadId,
    ) -> BoxFut<'_, Result<Option<Observation>, HostError>> {
        let bridge = self.0.clone();
        let thread = thread.clone();
        Box::pin(async move { bridge.check_thread_live(&thread).await })
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
                let _ = self.owner.send(OwnerMsg::AdvanceFeedSeq {
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
        fail_next_turns_suppressed: usize,
        fail_next_spawned_observe: usize,
        fail_next_wave_reads: usize,
        on_next_room_read: Option<protocol::Up>,
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
                fail_next_turns_suppressed: 0,
                fail_next_spawned_observe: 0,
                fail_next_wave_reads: 0,
                on_next_room_read: None,
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
                        if matches!(
                            &wc,
                            rex_wave_api::WaveCommand::WaveObserve(WaveObserve {
                                observation: Observation::TurnsSuppressed { .. },
                                ..
                            })
                        ) && g.fail_next_turns_suppressed > 0
                        {
                            g.fail_next_turns_suppressed -= 1;
                            return Err(ApiError::Internal(
                                "injected TurnsSuppressed write failure".into(),
                            ));
                        }
                        if matches!(
                            &wc,
                            rex_wave_api::WaveCommand::WaveObserve(WaveObserve {
                                observation: Observation::Spawned { .. },
                                ..
                            })
                        ) && g.fail_next_spawned_observe > 0
                        {
                            g.fail_next_spawned_observe -= 1;
                            return Err(ApiError::Internal(
                                "injected Spawned write failure".into(),
                            ));
                        }
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
                                        origin: Default::default(),
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
                        if matches!(r.stream, StreamId::Room { .. }) {
                            let up = harness.inner.lock().await.on_next_room_read.take();
                            if let Some(up) = up {
                                harness.bridge.handle_up(up);
                            }
                        }
                        let mut g = harness.inner.lock().await;
                        if matches!(r.stream, StreamId::Wave { .. }) && g.fail_next_wave_reads > 0 {
                            g.fail_next_wave_reads -= 1;
                            return Err(ApiError::Internal(
                                "injected wave stream read failure".into(),
                            ));
                        }
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
            origin: Default::default(),
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
                2,
                "cold reconcile paged descending through 249 rows to reach thread origin"
            );
            assert!(
                calls
                    .iter()
                    .all(|c| c.3 == Some(protocol::EventsOrder::Desc))
            );
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
            origin: Default::default(),
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
            Frame::Agent { event: AgentEvent { seq: HostSeq(251), kind: AgentEventKind::AssistantText { ref text }, .. }, .. }
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
            origin: Default::default(),
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
        assert_eq!(extracted.anchor, "e15");
        assert_eq!(extracted.turn_start_seq, Some(10));
        assert_eq!(extracted.turn_end_seq, 15);
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
                    .call(Read::stream(
                        StreamId::Room {
                            room: started.room.clone(),
                        },
                        Seq::ORIGIN,
                        Some(50),
                    ))
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
                origin: Default::default(),
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
                    .call(Read::stream(
                        StreamId::Room {
                            room: started.room.clone(),
                        },
                        Seq::ORIGIN,
                        Some(50),
                    ))
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
        // BB-shaped failed turn (provider/error policy + turn/completed status=failed with nested error.message)
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

        // BB-shaped generic failed turn (non-refusal) resolves to Errored with nested error.message.
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

        // A retrying provider/error (willRetry: true) in a turn that completes normally stays Idle.
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

        // Runtime model extraction reads actual BB runtime events (contextWindowUsage.snapshot.model
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

        // Narrow missing-thread matching ignores unrelated "missing required field" errors.
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

        // Spawn_args keeps account in pluginMetadata.rexWave, not provider sessionOptions.
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

    #[tokio::test]
    async fn cancelled_run_code_wait_does_not_mark_still_waiting_worker_working() {
        #[derive(Clone)]
        struct RouterModeHarness(LoopbackHarness);
        impl BridgeBackend for RouterModeHarness {
            type C = HarnessClient;
            fn client(&self, me: Principal) -> HarnessClient {
                self.0.client(me)
            }
            fn handles_worker_wait_parking(&self) -> bool {
                true
            }
        }

        for router_owns_parking in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let ws_srv = tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.unwrap();
                let ws = ws::WsConn::accept(tcp, "tok").await.unwrap();
                let (mut rd, mut wr) = ws.into_split();
                wr.send_text(
                    &serde_json::to_string(&protocol::Up::Hello {
                        plugin_version: "0.1.0".into(),
                        sdk_version: "0.6.132".into(),
                    })
                    .unwrap(),
                )
                .await
                .unwrap();
                while let Ok(Some(_)) = rd.recv().await {}
            });

            let room_slot: Arc<std::sync::OnceLock<RoomId>> = Arc::new(std::sync::OnceLock::new());
            let runner_room = room_slot.clone();
            let runner: CodeRunner = Arc::new(move |client, args| {
                let room = runner_room.get().unwrap().clone();
                Box::pin(async move {
                    let mode = args.get("mode").and_then(|v| v.as_str()).unwrap_or("hold");
                    let room_stream = StreamId::Room { room: room.clone() };
                    if mode == "repark_race" {
                        let c1 = client.clone();
                        let c2 = client;
                        let r1 = room.clone();
                        let r2 = room;
                        let rs1 = room_stream.clone();
                        let rs2 = room_stream;
                        let first = tokio::spawn(async move {
                            let w1 = rex_wave_api::read::Wait {
                                timeout_ms: Some(15),
                                ..rex_wave_api::read::Wait::new(
                                    Interest::Room {
                                        room: RoomRef::Id { id: r1 },
                                    },
                                    Cursor::default().with(rs1, Seq(1)),
                                )
                            };
                            let _ = c1.call(w1).await;
                        });
                        tokio::time::sleep(Duration::from_millis(15)).await;
                        tokio::task::yield_now().await;
                        let second = tokio::spawn(async move {
                            let w2 = rex_wave_api::read::Wait {
                                timeout_ms: Some(5_000),
                                ..rex_wave_api::read::Wait::new(
                                    Interest::Room {
                                        room: RoomRef::Id { id: r2 },
                                    },
                                    Cursor::default().with(rs2, Seq(1)),
                                )
                            };
                            let _ = c2.call(w2).await;
                        });
                        first.await.unwrap();
                        second.await.unwrap();
                        return Ok("reparked".into());
                    }
                    let w = rex_wave_api::read::Wait {
                        timeout_ms: Some(5_000),
                        ..rex_wave_api::read::Wait::new(
                            Interest::Room {
                                room: RoomRef::Id { id: room },
                            },
                            Cursor::default().with(room_stream, Seq(999)),
                        )
                    };
                    if mode == "abort" {
                        let fut = client.call(w);
                        let _ = tokio::time::timeout(Duration::from_millis(30), fut).await;
                        Err("script cancelled wait".into())
                    } else {
                        let _ = client.call(w).await;
                        Ok("done".into())
                    }
                })
            });

            let bridge = BbBridge::with_options(
                BridgeConfig {
                    url: format!("ws://{addr}/bridge"),
                    token: "tok".into(),
                },
                None,
                Some(runner),
            );
            let (harness, _frame_rx) = LoopbackHarness::new(bridge.clone());
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let bridge_runner = if router_owns_parking {
                let b = bridge.clone();
                let h = RouterModeHarness(harness.clone());
                tokio::spawn(async move { b.run_until(h, shutdown_rx).await })
            } else {
                let b = bridge.clone();
                let h = harness.clone();
                tokio::spawn(async move { b.run_until(h, shutdown_rx).await })
            };

            let op = harness.client(Principal::Operator {
                seat: SeatName::parse("owner").unwrap(),
            });
            let sys = harness.client(Principal::System {
                part: SystemPart::BbBridge,
            });
            let wave = WaveSlug::parse("w-cancel").unwrap();
            let role = RoleName::parse("coder").unwrap();
            let started = op
                .call(rex_wave_api::wave::WaveStart {
                    spec: rex_wave_api::wave::WaveSpec {
                        slug: wave.clone(),
                        project: BbProjectId::parse("proj_cancel").unwrap(),
                        goal: Body::parse("cancel guard").unwrap(),
                        output_dir: None,
                        defaults: rex_wave_api::wave::AgentSpec {
                            provider: ProviderId::parse("claude").unwrap(),
                            model: None,
                            account: None,
                            reasoning: None,
                            service_tier: None,
                            permission: PermissionMode::Full,
                            workspace: Workspace::ProjectDefault,
                        },
                        workers: vec![rex_wave_api::wave::WorkerSpec {
                            role: role.clone(),
                            brief: Body::parse("test").unwrap(),
                            agent: None,
                            output_dir: None,
                            writes: Vec::new(),
                            return_schema: None,
                        }],
                    },
                    idem: None,
                })
                .await
                .unwrap();
            let _ = room_slot.set(started.room.clone());

            let thread = BbThreadId::parse("th_cancel_1").unwrap();
            sys.call(WaveObserve {
                wave: wave.clone(),
                role: role.clone(),
                observation: Observation::Spawned {
                    attempt: 1,
                    thread: thread.clone(),
                },
            })
            .await
            .unwrap();
            bridge.observe_wave_event(
                wave.clone(),
                WaveEvent::WorkerObserved {
                    role: role.clone(),
                    observation: Observation::Spawned {
                        attempt: 1,
                        thread: thread.clone(),
                    },
                },
            );

            if router_owns_parking {
                let park_ev = Observation::Activity {
                    activity: Activity::Parked {
                        room: Some(started.room.clone()),
                        since: Seq::ORIGIN,
                    },
                };
                sys.call(WaveObserve {
                    wave: wave.clone(),
                    role: role.clone(),
                    observation: park_ev.clone(),
                })
                .await
                .unwrap();
                bridge.observe_wave_event(
                    wave.clone(),
                    WaveEvent::WorkerObserved {
                        role: role.clone(),
                        observation: park_ev,
                    },
                );
            }

            let b1 = bridge.clone();
            let hold_handle = tokio::spawn(async move {
                b1.on_tool_call(
                    "call_hold".into(),
                    "th_cancel_1".into(),
                    None,
                    "run_code".into(),
                    serde_json::json!({ "mode": "hold" }),
                )
                .await;
            });

            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let s = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                    if matches!(
                        &s.workers[0].2,
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
            .expect("hold call parked worker");

            bridge
                .on_tool_call(
                    "call_abort".into(),
                    "th_cancel_1".into(),
                    None,
                    "run_code".into(),
                    serde_json::json!({ "mode": "abort" }),
                )
                .await;

            tokio::time::sleep(Duration::from_millis(25)).await;
            let mid_snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
            assert!(
                matches!(
                    &mid_snap.workers[0].2,
                    WorkerPhase::Live {
                        activity: Activity::Parked { .. },
                        ..
                    }
                ),
                "cancelling one run_code wait (router_owns_parking={router_owns_parking}) must not overwrite sibling parked wait: {:?}",
                mid_snap.workers[0].2
            );

            op.call(RoomPost {
                room: RoomRef::Id {
                    id: started.room.clone(),
                },
                body: Body::parse("wake hold wait").unwrap(),
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
                idem: None,
                origin: Default::default(),
            })
            .await
            .unwrap();
            hold_handle.await.unwrap();
            if !router_owns_parking {
                let end_snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                assert!(
                    matches!(
                        &end_snap.workers[0].2,
                        WorkerPhase::Live {
                            activity: Activity::Working,
                            ..
                        }
                    ),
                    "finishing final fallback wait restores Working: {:?}",
                    end_snap.workers[0].2
                );

                let b_race = bridge.clone();
                let race_handle = tokio::spawn(async move {
                    b_race
                        .on_tool_call(
                            "call_repark".into(),
                            "th_cancel_1".into(),
                            None,
                            "run_code".into(),
                            serde_json::json!({ "mode": "repark_race" }),
                        )
                        .await;
                });
                tokio::time::sleep(Duration::from_millis(45)).await;
                let race_snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                assert!(
                    matches!(
                        &race_snap.workers[0].2,
                        WorkerPhase::Live {
                            activity: Activity::Parked { .. },
                            ..
                        }
                    ),
                    "new fallback wait parking during prior cleanup must stay Parked: {:?}",
                    race_snap.workers[0].2
                );
                op.call(RoomPost {
                    room: RoomRef::Id {
                        id: started.room.clone(),
                    },
                    body: Body::parse("wake repark wait").unwrap(),
                    re: None,
                    after: None,
                    labels: Vec::new(),
                    task: None,
                    idem: None,
                    origin: Default::default(),
                })
                .await
                .unwrap();
                race_handle.await.unwrap();
                assert!(
                    bridge
                        .fallback_active_waits
                        .lock()
                        .unwrap()
                        .entries
                        .is_empty(),
                    "completed fallback waits must reclaim per-worker gate entries"
                );

                // Obsolete gate queued across entry reclamation + recreation must never publish Working.
                let key = (wave.clone(), role.clone());
                let (e_old, g_old) = bridge.increment_fallback_wait(&key);
                assert_eq!(bridge.decrement_fallback_wait(&key), 0);
                let held_old = g_old.lock().await;
                let stale_cleanup = tokio::spawn({
                    let b = bridge.clone();
                    let k = key.clone();
                    async move {
                        b.restore_fallback_working_if_idle(&k).await;
                    }
                });
                tokio::task::yield_now().await;
                bridge.reclaim_idle_fallback_wait(&key, e_old, &g_old);

                let b_hold2 = bridge.clone();
                let hold2 = tokio::spawn(async move {
                    b_hold2
                        .on_tool_call(
                            "call_hold_after_recreate".into(),
                            "th_cancel_1".into(),
                            None,
                            "run_code".into(),
                            serde_json::json!({ "mode": "hold" }),
                        )
                        .await;
                });
                tokio::time::sleep(Duration::from_millis(25)).await;
                drop(held_old);
                stale_cleanup.await.unwrap();
                let after_stale = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                assert!(
                    matches!(
                        &after_stale.workers[0].2,
                        WorkerPhase::Live {
                            activity: Activity::Parked { .. },
                            ..
                        }
                    ),
                    "obsolete gate cleanup must not overwrite recreated active wait: {:?}",
                    after_stale.workers[0].2
                );
                op.call(RoomPost {
                    room: RoomRef::Id {
                        id: started.room.clone(),
                    },
                    body: Body::parse("wake recreated hold").unwrap(),
                    re: None,
                    after: None,
                    labels: Vec::new(),
                    task: None,
                    idem: None,
                    origin: Default::default(),
                })
                .await
                .unwrap();
                hold2.await.unwrap();
                assert!(
                    bridge
                        .fallback_active_waits
                        .lock()
                        .unwrap()
                        .entries
                        .is_empty()
                );

                // Cancelling a fallback wait after `Wait` returned while `Working` restoration is
                // in flight still completes `Working` and reclaims the map entry.
                #[derive(Clone)]
                struct SlowWorkingClient(HarnessClient);
                impl Client for SlowWorkingClient {
                    fn me(&self) -> &Principal {
                        self.0.me()
                    }
                    fn call_wire(
                        &self,
                        cmd: WireCommand,
                    ) -> impl Future<Output = Result<WireReply, ApiError>> + Send
                    {
                        let inner = self.0.clone();
                        let delay_working = matches!(
                            &cmd,
                            WireCommand::WaveObserve(WaveObserve {
                                observation: Observation::Activity {
                                    activity: Activity::Working,
                                },
                                ..
                            })
                        );
                        async move {
                            if delay_working {
                                tokio::time::sleep(Duration::from_millis(40)).await;
                            }
                            inner.call_wire(cmd).await
                        }
                    }
                    fn watch(
                        &self,
                        interest: Interest,
                        from: Cursor,
                    ) -> impl Future<Output = Result<FrameStream, ApiError>> + Send
                    {
                        self.0.watch(interest, from)
                    }
                }
                #[derive(Clone)]
                struct SlowWorkingBackend(LoopbackHarness);
                impl BridgeBackend for SlowWorkingBackend {
                    type C = SlowWorkingClient;
                    fn client(&self, me: Principal) -> SlowWorkingClient {
                        SlowWorkingClient(self.0.client(me))
                    }
                }

                let bridge_slow = BbBridge::with_options(
                    BridgeConfig {
                        url: "ws://127.0.0.1:1/disabled".into(),
                        token: "tok".into(),
                    },
                    None,
                    None,
                );
                let _ = bridge_slow
                    .backend
                    .set(Arc::new(SlowWorkingBackend(harness.clone())));
                let slow_aborted = Arc::new(AtomicBool::new(false));
                let direct_client = WorkerToolClient {
                    bridge: bridge_slow.clone(),
                    backend: bridge_slow.backend.get().unwrap().clone(),
                    me: Principal::Worker {
                        wave: wave.clone(),
                        role: role.clone(),
                    },
                    wave: wave.clone(),
                    role: role.clone(),
                    aborted_wait: slow_aborted.clone(),
                };
                let short_wait = rex_wave_api::read::Wait {
                    timeout_ms: Some(10),
                    ..rex_wave_api::read::Wait::new(
                        Interest::Room {
                            room: RoomRef::Id {
                                id: started.room.clone(),
                            },
                        },
                        Cursor::default().with(
                            StreamId::Room {
                                room: started.room.clone(),
                            },
                            Seq(999),
                        ),
                    )
                };
                let timed =
                    tokio::time::timeout(Duration::from_millis(25), direct_client.call(short_wait))
                        .await;
                assert!(timed.is_err(), "cancelled mid-restoration");
                assert!(
                    slow_aborted.load(Ordering::Relaxed),
                    "mid-restoration cancel must mark aborted_wait"
                );
                tokio::time::sleep(Duration::from_millis(55)).await;
                let restored_snap = op.call(WaveView { wave }).await.unwrap();
                assert!(
                    matches!(
                        &restored_snap.workers[0].2,
                        WorkerPhase::Live {
                            activity: Activity::Working,
                            ..
                        }
                    ),
                    "detached restoration must still transition worker back to Working: {:?}",
                    restored_snap.workers[0].2
                );
                assert!(
                    bridge_slow
                        .fallback_active_waits
                        .lock()
                        .unwrap()
                        .entries
                        .is_empty(),
                    "detached restoration must reclaim fallback map entry"
                );

                // 16 concurrent OS-thread registrations where 15 cancel must always leave the
                // sole survivor live (`fallback_wait_is_live == true`) with monotonic epochs.
                for survivor_idx in 0..16usize {
                    let barrier = std::sync::Barrier::new(16);
                    let regs: Vec<(u64, Arc<tokio::sync::Mutex<()>>)> = std::thread::scope(|s| {
                        let mut handles = Vec::with_capacity(16);
                        for _ in 0..16 {
                            let b_ref = &bridge_slow;
                            let k_ref = &key;
                            let bar_ref = &barrier;
                            handles.push(s.spawn(move || {
                                bar_ref.wait();
                                b_ref.increment_fallback_wait(k_ref)
                            }));
                        }
                        handles.into_iter().map(|h| h.join().unwrap()).collect()
                    });
                    for i in 0..16 {
                        if i != survivor_idx {
                            bridge_slow.decrement_fallback_wait(&key);
                        }
                    }
                    let (surv_epoch, surv_gate) = &regs[survivor_idx];
                    assert!(
                        bridge_slow.fallback_wait_is_live(&key, *surv_epoch, surv_gate),
                        "survivor {survivor_idx} (epoch={surv_epoch}) must remain live after 15 sibling cancellations"
                    );
                    assert_eq!(bridge_slow.decrement_fallback_wait(&key), 0);
                    bridge_slow.restore_fallback_working_if_idle(&key).await;
                }
            }
            let _ = shutdown_tx.send(true);
            let _ = bridge_runner.await;
            ws_srv.abort();
        }
    }

    #[tokio::test]
    async fn bb_bridge_ignores_non_bb_runner_threads_across_fold_lookup_and_reconcile() {
        let bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: "ws://127.0.0.1:9/unused".into(),
                token: String::new(),
            },
            None,
        );
        let (harness, _frame_rx) = LoopbackHarness::new(bridge.clone());
        let _ = bridge.backend.set(Arc::new(harness.clone()));

        let op = harness.client(Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        });
        let sys = harness.client(Principal::System {
            part: SystemPart::WaveConductor,
        });
        let wave = WaveSlug::parse("cross-host").unwrap();
        let project = BbProjectId::parse("proj_demo_1").unwrap();
        let make_worker = |role: &str, provider: &str| rex_wave_api::wave::WorkerSpec {
            role: RoleName::parse(role).unwrap(),
            brief: Body::parse(format!("@{role} go")).unwrap(),
            agent: Some(rex_wave_api::wave::AgentSpec {
                provider: ProviderId::parse(provider).unwrap(),
                model: None,
                account: None,
                reasoning: None,
                service_tier: None,
                permission: rex_wave_api::wave::PermissionMode::Full,
                workspace: rex_wave_api::wave::Workspace::ProjectDefault,
            }),
            output_dir: None,
            writes: Vec::new(),
            return_schema: None,
        };
        op.call(rex_wave_api::wave::WaveStart {
            spec: rex_wave_api::wave::WaveSpec {
                slug: wave.clone(),
                project,
                goal: Body::parse("mixed runner wave").unwrap(),
                output_dir: None,
                defaults: rex_wave_api::wave::AgentSpec {
                    provider: ProviderId::parse("pi").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: rex_wave_api::wave::PermissionMode::Full,
                    workspace: rex_wave_api::wave::Workspace::ProjectDefault,
                },
                workers: vec![
                    make_worker("t3role", "antigravity"),
                    make_worker("acprole", "acp:opencode"),
                    make_worker("clrole", "claude:claude-code"),
                    make_worker("cxrole", "codex:codex"),
                    make_worker("bbrole", "pi"),
                ],
            },
            idem: None,
        })
        .await
        .unwrap();

        for (role_str, tid_str) in [
            ("t3role", "t3:cross-host:t3role:a1"),
            ("acprole", "acp:cross-host:acprole:a1"),
            ("clrole", "claude:cross-host:clrole:a1"),
            ("cxrole", "codex:cross-host:cxrole:a1"),
            ("bbrole", "th_bb_live_1"),
        ] {
            let role = RoleName::parse(role_str).unwrap();
            let thread = BbThreadId::parse(tid_str).unwrap();
            let obs = Observation::Spawned {
                attempt: 1,
                thread: thread.clone(),
            };
            sys.call(WaveObserve {
                wave: wave.clone(),
                role: role.clone(),
                observation: obs.clone(),
            })
            .await
            .unwrap();
            bridge.observe_wave_event(
                wave.clone(),
                WaveEvent::WorkerObserved {
                    role,
                    observation: obs,
                },
            );
        }

        let (list_tx, list_rx) = oneshot::channel();
        bridge
            .owner
            .send(OwnerMsg::ListLiveWorkers { reply: list_tx })
            .unwrap();
        let live = list_rx.await.unwrap();
        assert_eq!(live.len(), 1, "only BB threads may enter ListLiveWorkers");
        assert_eq!(live[0].3.as_str(), "th_bb_live_1");

        for non_bb_role in ["t3role", "acprole", "clrole", "cxrole"] {
            let role = RoleName::parse(non_bb_role).unwrap();
            assert!(
                matches!(
                    bridge.lookup_worker(&wave, &role).await,
                    Err(ApiError::NotFound { .. })
                ),
                "BbBridge::lookup_worker must return NotFound for {non_bb_role}"
            );
        }

        let spoof_meta = protocol::WaveMeta {
            wave_id: wave.to_string(),
            role: "t3role".into(),
            attempt: 1,
            room_id: String::new(),
            run_stamp: None,
            output_dir: None,
            writes: Vec::new(),
            account: None,
        };
        assert_eq!(
            bridge
                .resolve_worker_identity(
                    &BbThreadId::parse("t3:cross-host:t3role:a1").unwrap(),
                    Some(&spoof_meta),
                )
                .await,
            None,
            "BbBridge must reject non-BB thread IDs even with matching WaveMeta"
        );

        // Retiring BB worker must accept terminal Archived without re-entering ListLiveWorkers on Active.
        let bbrole = RoleName::parse("bbrole").unwrap();
        op.call(rex_wave_api::wave::WaveRetireWorker {
            wave: wave.clone(),
            role: bbrole.clone(),
            reason: Some("done".into()),
        })
        .await
        .unwrap();
        bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerRetiring {
                role: bbrole.clone(),
                reason: Some("done".into()),
            },
        );
        let bb_meta = protocol::WaveMeta {
            wave_id: wave.to_string(),
            role: "bbrole".into(),
            attempt: 1,
            room_id: String::new(),
            run_stamp: None,
            output_dir: None,
            writes: Vec::new(),
            account: None,
        };
        bridge
            .on_thread_signal(protocol::Up::Thread {
                signal: protocol::ThreadSignal::Active,
                thread_id: "th_bb_live_1".into(),
                project_id: "proj_demo_1".into(),
                meta: Some(bb_meta.clone()),
                error: None,
                stop_reason: None,
                model: None,
                account: None,
            })
            .await;
        let (list_tx2, list_rx2) = oneshot::channel();
        bridge
            .owner
            .send(OwnerMsg::ListLiveWorkers { reply: list_tx2 })
            .unwrap();
        assert!(
            list_rx2.await.unwrap().is_empty(),
            "retiring worker must not re-enter ListLiveWorkers on Active signal"
        );
        bridge
            .on_thread_signal(protocol::Up::Thread {
                signal: protocol::ThreadSignal::Archived,
                thread_id: "th_bb_live_1".into(),
                project_id: "proj_demo_1".into(),
                meta: Some(bb_meta),
                error: None,
                stop_reason: None,
                model: None,
                account: None,
            })
            .await;
        let snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
        let bb_phase = &snap
            .workers
            .iter()
            .find(|(r, _, _)| r == &bbrole)
            .unwrap()
            .2;
        assert!(matches!(bb_phase, WorkerPhase::Retired { .. }));
    }

    #[tokio::test]
    async fn pre_upgrade_truncated_bb_mirror_turn_does_not_duplicate_across_retries() {
        let bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: "ws://127.0.0.1:9/unused".into(),
                token: String::new(),
            },
            None,
        );
        let (harness, _frame_rx) = LoopbackHarness::new(bridge.clone());
        let _ = bridge.backend.set(Arc::new(harness.clone()));
        let sys = harness.client(Principal::System {
            part: SystemPart::WaveConductor,
        });
        let owner = harness.client(Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        });
        let wave = WaveSlug::parse("long-mirror").unwrap();
        let proj = BbProjectId::parse("proj_mirror").unwrap();
        let role = RoleName::parse("writer").unwrap();
        let long_tid = BbThreadId::parse(format!("th_{}", "x".repeat(235))).unwrap();

        owner
            .call(rex_wave_api::wave::WaveStart {
                spec: rex_wave_api::wave::WaveSpec {
                    slug: wave.clone(),
                    project: proj.clone(),
                    goal: Body::parse("test upgrade mirror dedupe").unwrap(),
                    output_dir: None,
                    defaults: rex_wave_api::wave::AgentSpec {
                        provider: ProviderId::parse("pi").unwrap(),
                        model: None,
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: rex_wave_api::wave::PermissionMode::Full,
                        workspace: rex_wave_api::wave::Workspace::ProjectDefault,
                    },
                    workers: vec![rex_wave_api::wave::WorkerSpec {
                        role: role.clone(),
                        agent: None,
                        brief: Body::parse("write").unwrap(),
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    }],
                },
                idem: None,
            })
            .await
            .unwrap();
        let spawned = Observation::Spawned {
            attempt: 1,
            thread: long_tid.clone(),
        };
        sys.call(WaveObserve {
            wave: wave.clone(),
            role: role.clone(),
            observation: spawned.clone(),
        })
        .await
        .unwrap();
        bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: role.clone(),
                observation: spawned,
            },
        );

        let room = rex_wave_waves::wave_room(&wave, &proj);
        sys.call(RoomEnsure {
            room: RoomRef::Id { id: room.clone() },
            topic: None,
            origin: Some(RoomOrigin::Wave { wave: wave.clone() }),
        })
        .await
        .unwrap();
        let worker_principal = Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        };
        sys.call(RoomJoin {
            room: RoomRef::Id { id: room.clone() },
            member: Some(worker_principal.clone()),
            handle: Handle::parse("writer").ok(),
        })
        .await
        .unwrap();

        let anchor = "s999999999";
        let legacy_idem =
            rex_wave_waves::legacy_truncated_mirror_idem_key(&wave, &role, &long_tid, anchor)
                .expect("exceeds 256 bytes");
        let worker_client = harness.client(worker_principal);
        worker_client
            .call(RoomPost {
                room: RoomRef::Id { id: room.clone() },
                body: Body::parse("pre-upgrade mirrored turn").unwrap(),
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
                idem: Some(legacy_idem.clone()),
                origin: rex_wave_api::room::PostOrigin::Direct,
            })
            .await
            .unwrap();

        let rows = vec![
            serde_json::json!({
                "seq": 999_999_999u64,
                "createdAt": 900u64,
                "type": "turn/started",
                "data": {}
            }),
            serde_json::json!({
                "seq": 1_000_000_000u64,
                "createdAt": 950u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "pre-upgrade mirrored turn" } }
            }),
            serde_json::json!({
                "seq": 1_000_000_001u64,
                "createdAt": 990u64,
                "type": "turn/completed",
                "data": {}
            }),
        ];
        assert!(extract_unposted_final_turn(&rows).is_some());

        bridge
            .mirror_completed_turn_from_rows(&wave, &role, &long_tid, &rows)
            .await;
        bridge
            .mirror_completed_turn_from_rows(&wave, &role, &long_tid, &rows)
            .await;

        let page = owner
            .call(Read::stream(
                StreamId::Room { room: room.clone() },
                Seq::ORIGIN,
                Some(50),
            ))
            .await
            .unwrap();
        let posts: Vec<_> = page
            .entries
            .iter()
            .filter(|e| matches!(e.event, Event::Room(RoomEvent::Posted { .. })))
            .collect();
        assert_eq!(
            posts.len(),
            1,
            "pre-upgrade truncated mirror post must not duplicate on consecutive retries"
        );

        let cold_bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: "ws://127.0.0.1:9/unused".into(),
                token: String::new(),
            },
            None,
        );
        let _ = cold_bridge.backend.set(Arc::new(harness.clone()));
        cold_bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: role.clone(),
                observation: Observation::Spawned {
                    attempt: 1,
                    thread: long_tid.clone(),
                },
            },
        );
        assert_eq!(
            rex_wave_waves::legacy_truncated_mirror_idem_key(
                &wave,
                &role,
                &long_tid,
                "s1000000002",
            )
            .as_ref(),
            Some(&legacy_idem),
            "both anchors collide under legacy 256-byte truncation"
        );
        let second_turn_rows = vec![
            serde_json::json!({
                "seq": 1_000_000_002u64,
                "createdAt": 900u64,
                "type": "turn/started",
                "data": {}
            }),
            serde_json::json!({
                "seq": 1_000_000_003u64,
                "createdAt": 950u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "post-upgrade second turn" } }
            }),
            serde_json::json!({
                "seq": 1_000_000_004u64,
                "createdAt": 990u64,
                "type": "turn/completed",
                "data": {}
            }),
        ];
        assert!(extract_unposted_final_turn(&second_turn_rows).is_some());
        cold_bridge
            .mirror_completed_turn_from_rows(&wave, &role, &long_tid, &second_turn_rows)
            .await;

        let third_cold_bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: "ws://127.0.0.1:9/unused".into(),
                token: String::new(),
            },
            None,
        );
        let _ = third_cold_bridge.backend.set(Arc::new(harness.clone()));
        third_cold_bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: role.clone(),
                observation: Observation::Spawned {
                    attempt: 1,
                    thread: long_tid.clone(),
                },
            },
        );
        let third_turn_rows = vec![
            serde_json::json!({
                "seq": 1_000_000_005u64,
                "createdAt": 5_000u64,
                "type": "turn/started",
                "data": {}
            }),
            serde_json::json!({
                "seq": 1_000_000_006u64,
                "createdAt": 5_100u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "pre-upgrade mirrored turn" } }
            }),
            serde_json::json!({
                "seq": 1_000_000_007u64,
                "createdAt": 5_200u64,
                "type": "turn/completed",
                "data": {}
            }),
        ];
        assert!(extract_unposted_final_turn(&third_turn_rows).is_some());
        third_cold_bridge
            .mirror_completed_turn_from_rows(&wave, &role, &long_tid, &third_turn_rows)
            .await;

        let fourth_cold_bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: "ws://127.0.0.1:9/unused".into(),
                token: String::new(),
            },
            None,
        );
        let _ = fourth_cold_bridge.backend.set(Arc::new(harness.clone()));
        fourth_cold_bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: role.clone(),
                observation: Observation::Spawned {
                    attempt: 1,
                    thread: long_tid.clone(),
                },
            },
        );
        let full_page_turn4_rows = vec![
            serde_json::json!({
                "seq": 1_000_000_249u64,
                "createdAt": 8_000u64,
                "type": "turn/started",
                "data": {}
            }),
            serde_json::json!({
                "seq": 1_000_000_250u64,
                "createdAt": 8_100u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "pre-upgrade mirrored turn" } }
            }),
            serde_json::json!({
                "seq": 1_000_000_251u64,
                "createdAt": 8_200u64,
                "type": "turn/completed",
                "data": {}
            }),
        ];
        let partial_page_turn4_rows = vec![
            serde_json::json!({
                "seq": 1_000_000_250u64,
                "createdAt": 8_100u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "pre-upgrade mirrored turn" } }
            }),
            serde_json::json!({
                "seq": 1_000_000_251u64,
                "createdAt": 8_200u64,
                "type": "turn/completed",
                "data": {}
            }),
        ];
        let full_extracted =
            extract_unposted_final_turn(&full_page_turn4_rows).expect("unposted with start");
        let partial_extracted =
            extract_unposted_final_turn(&partial_page_turn4_rows).expect("unposted without start");
        assert_eq!(full_extracted.anchor, "e1000000251");
        assert_eq!(partial_extracted.anchor, "e1000000251");
        assert_eq!(partial_extracted.turn_start_at, 0);
        assert_eq!(partial_extracted.turn_end_at, 8_200);
        fourth_cold_bridge
            .mirror_completed_turn_from_rows(&wave, &role, &long_tid, &full_page_turn4_rows)
            .await;
        fourth_cold_bridge
            .mirror_completed_turn_from_rows(&wave, &role, &long_tid, &partial_page_turn4_rows)
            .await;

        let fifth_cold_bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: "ws://127.0.0.1:9/unused".into(),
                token: String::new(),
            },
            None,
        );
        let _ = fifth_cold_bridge.backend.set(Arc::new(harness.clone()));
        fifth_cold_bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: role.clone(),
                observation: Observation::Spawned {
                    attempt: 1,
                    thread: long_tid.clone(),
                },
            },
        );
        fifth_cold_bridge
            .mirror_completed_turn_from_rows(&wave, &role, &long_tid, &partial_page_turn4_rows)
            .await;

        let sixth_cold_bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: "ws://127.0.0.1:9/unused".into(),
                token: String::new(),
            },
            None,
        );
        let _ = sixth_cold_bridge.backend.set(Arc::new(harness.clone()));
        sixth_cold_bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: role.clone(),
                observation: Observation::Spawned {
                    attempt: 1,
                    thread: long_tid.clone(),
                },
            },
        );
        let missing_start_turn5_rows = vec![
            serde_json::json!({
                "seq": 1_000_000_500u64,
                "createdAt": 9_100u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "pre-upgrade mirrored turn" } }
            }),
            serde_json::json!({
                "seq": 1_000_000_501u64,
                "createdAt": 9_200u64,
                "type": "turn/completed",
                "data": {}
            }),
        ];
        sixth_cold_bridge
            .mirror_completed_turn_from_rows(&wave, &role, &long_tid, &missing_start_turn5_rows)
            .await;
        sixth_cold_bridge
            .mirror_completed_turn_from_rows(&wave, &role, &long_tid, &missing_start_turn5_rows)
            .await;

        let page2 = owner
            .call(Read::stream(StreamId::Room { room }, Seq::ORIGIN, Some(50)))
            .await
            .unwrap();
        let posts2: Vec<_> = page2
            .entries
            .iter()
            .filter(|e| matches!(e.event, Event::Room(RoomEvent::Posted { .. })))
            .collect();
        assert_eq!(
            posts2.len(),
            5,
            "full-page-then-partial-page retry must not duplicate, while genuinely new missing-start turn mirrors"
        );
    }

    #[tokio::test]
    async fn long_turn_backfills_start_seq_over_ws_for_delayed_legacy_s_keys() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let token = "tok_backfill_s_keys";

        let mut turn2_events = vec![
            serde_json::json!({ "seq": 10u64, "createdAt": 100u64, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 11u64,
                "createdAt": 150u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "Identical verdict." } }
            }),
            serde_json::json!({ "seq": 15u64, "createdAt": 200u64, "type": "turn/completed", "data": { "status": "completed" } }),
            serde_json::json!({ "seq": 20u64, "createdAt": 300u64, "type": "turn/started", "data": {} }),
        ];
        for s in 21u64..240u64 {
            turn2_events.push(serde_json::json!({
                "seq": s,
                "createdAt": 300u64 + (s - 20),
                "type": "item/agentMessage/delta",
                "data": { "delta": "." }
            }));
        }
        turn2_events.push(serde_json::json!({
            "seq": 240u64,
            "createdAt": 890u64,
            "type": "item/completed",
            "data": { "item": { "type": "agentMessage", "text": "Identical verdict." } }
        }));
        turn2_events.push(serde_json::json!({
            "seq": 241u64,
            "createdAt": 999u64,
            "type": "turn/completed",
            "data": { "status": "completed" }
        }));

        let mut single_long_turn_events = vec![serde_json::json!({
            "seq": 300u64,
            "createdAt": 100u64,
            "type": "turn/started",
            "data": {}
        })];
        for s in 301u64..524u64 {
            single_long_turn_events.push(serde_json::json!({
                "seq": s,
                "createdAt": 100u64 + (s - 300),
                "type": "item/agentMessage/delta",
                "data": { "delta": "." }
            }));
        }
        single_long_turn_events.push(serde_json::json!({
            "seq": 524u64,
            "createdAt": 890u64,
            "type": "item/completed",
            "data": { "item": { "type": "agentMessage", "text": "Long single turn." } }
        }));
        single_long_turn_events.push(serde_json::json!({
            "seq": 525u64,
            "createdAt": 999u64,
            "type": "turn/completed",
            "data": { "status": "completed" }
        }));

        let mut early_msg_events = vec![
            serde_json::json!({ "seq": 600u64, "createdAt": 100u64, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 601u64,
                "createdAt": 110u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "Early message before tool output." } }
            }),
        ];
        for s in 602u64..807u64 {
            early_msg_events.push(serde_json::json!({
                "seq": s,
                "createdAt": 110u64 + (s - 601),
                "type": "item/commandExecution/outputDelta",
                "data": { "delta": "log line\n" }
            }));
        }
        early_msg_events.push(serde_json::json!({
            "seq": 807u64,
            "createdAt": 999u64,
            "type": "turn/completed",
            "data": { "status": "completed" }
        }));

        let mut delta_disconnect_events = vec![
            serde_json::json!({ "seq": 900u64, "createdAt": 100u64, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 901u64,
                "createdAt": 101u64,
                "type": "item/agentMessage/delta",
                "data": { "delta": "Prefix " }
            }),
        ];
        let mut full_delta_body = String::from("Prefix ");
        for s in 902u64..1125u64 {
            delta_disconnect_events.push(serde_json::json!({
                "seq": s,
                "createdAt": 101u64 + (s - 901),
                "type": "item/agentMessage/delta",
                "data": { "delta": "x" }
            }));
            full_delta_body.push('x');
        }
        delta_disconnect_events.push(serde_json::json!({
            "seq": 1125u64,
            "createdAt": 990u64,
            "type": "item/agentMessage/delta",
            "data": { "delta": " suffix" }
        }));
        full_delta_body.push_str(" suffix");
        delta_disconnect_events.push(serde_json::json!({
            "seq": 1126u64,
            "createdAt": 999u64,
            "type": "turn/completed",
            "data": { "status": "completed" }
        }));

        let wave = WaveSlug::parse("ws-backfill").unwrap();
        let proj = BbProjectId::parse("proj_backfill").unwrap();
        let role_a = RoleName::parse("delayed-turn2").unwrap();
        let role_b = RoleName::parse("same-turn1").unwrap();
        let role_c = RoleName::parse("shared-s").unwrap();
        let role_d = RoleName::parse("end-trunc").unwrap();
        let role_e = RoleName::parse("early-msg").unwrap();
        let role_f = RoleName::parse("disc-delta").unwrap();

        let prefix_c_len = format!("wave:{wave}:turn:{role_c}:").len();
        let tid_c_fill = 256 - prefix_c_len - 2 - "th_c_".len();
        let tid_c = BbThreadId::parse(format!("th_c_{}", "z".repeat(tid_c_fill))).unwrap();
        let trunc_c_s10 =
            rex_wave_waves::legacy_truncated_mirror_idem_key(&wave, &role_c, &tid_c, "s10")
                .unwrap();
        let trunc_c_s20 =
            rex_wave_waves::legacy_truncated_mirror_idem_key(&wave, &role_c, &tid_c, "s20")
                .unwrap();
        assert_eq!(trunc_c_s10, trunc_c_s20);
        assert!(trunc_c_s10.as_str().ends_with(":s"));

        let prefix_d_len = format!("wave:{wave}:turn:{role_d}:").len();
        let tid_d_fill = 256 - prefix_d_len - 3 - "th_d_".len();
        let tid_d = BbThreadId::parse(format!("th_d_{}", "y".repeat(tid_d_fill))).unwrap();
        let trunc_d_e525 =
            rex_wave_waves::legacy_truncated_mirror_idem_key(&wave, &role_d, &tid_d, "e525")
                .unwrap();
        assert!(trunc_d_e525.as_str().ends_with(":e5"));

        let tid_a = BbThreadId::parse("th_backfill_delayed").unwrap();
        let tid_b = BbThreadId::parse("th_backfill_same").unwrap();
        let tid_e = BbThreadId::parse("th_backfill_early_msg").unwrap();
        let tid_f = BbThreadId::parse("th_backfill_disc_delta").unwrap();

        let tid_c_srv = tid_c.to_string();
        let tid_d_srv = tid_d.to_string();
        let drop_next_backfill = Arc::new(AtomicBool::new(false));
        let drop_srv = drop_next_backfill.clone();

        let server_task = tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    break;
                };
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
                        req_id,
                        thread_id,
                        before_seq,
                        limit,
                        order,
                        ..
                    } = down
                    {
                        if thread_id == "th_backfill_disc_delta"
                            && before_seq.is_some()
                            && drop_srv.swap(false, Ordering::SeqCst)
                        {
                            break;
                        }
                        let source = if thread_id == "th_backfill_delayed" || thread_id == tid_c_srv
                        {
                            &turn2_events
                        } else if thread_id == "th_backfill_early_msg" {
                            &early_msg_events
                        } else if thread_id == "th_backfill_disc_delta" {
                            &delta_disconnect_events
                        } else {
                            let _ = &tid_d_srv;
                            &single_long_turn_events
                        };
                        let mut filtered: Vec<serde_json::Value> = source
                            .iter()
                            .filter(|r| {
                                let seq = r.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
                                before_seq.is_none_or(|b| seq < b)
                            })
                            .cloned()
                            .collect();
                        if order == Some(protocol::EventsOrder::Desc) {
                            filtered.reverse();
                        }
                        filtered.truncate(limit as usize);
                        let reply = protocol::Up::Result {
                            req_id,
                            ok: true,
                            value: serde_json::Value::Array(filtered),
                        };
                        let _ = ws_writer
                            .send_text(&serde_json::to_string(&reply).unwrap())
                            .await;
                    }
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
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let bridge_runner = tokio::spawn({
            let bridge = bridge.clone();
            let harness = harness.clone();
            async move { bridge.run_until(harness, shutdown_rx).await }
        });
        let (wc_tx, wc_rx) = oneshot::channel();
        bridge
            .owner
            .send(OwnerMsg::WaitConnected { reply: wc_tx })
            .unwrap();
        wc_rx.await.unwrap();

        let sys = harness.client(Principal::System {
            part: SystemPart::WaveConductor,
        });
        let owner = harness.client(Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        });

        owner
            .call(WaveStart {
                spec: WaveSpec {
                    slug: wave.clone(),
                    project: proj.clone(),
                    goal: Body::parse("test ws start backfill").unwrap(),
                    output_dir: None,
                    defaults: AgentSpec {
                        provider: ProviderId::parse("pi").unwrap(),
                        model: None,
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: PermissionMode::Full,
                        workspace: Workspace::ProjectDefault,
                    },
                    workers: [&role_a, &role_b, &role_c, &role_d, &role_e, &role_f]
                        .into_iter()
                        .map(|r| WorkerSpec {
                            role: r.clone(),
                            agent: None,
                            brief: Body::parse("work").unwrap(),
                            output_dir: None,
                            writes: Vec::new(),
                            return_schema: None,
                        })
                        .collect(),
                },
                idem: None,
            })
            .await
            .unwrap();

        let room = rex_wave_waves::wave_room(&wave, &proj);
        sys.call(RoomEnsure {
            room: RoomRef::Id { id: room.clone() },
            topic: None,
            origin: Some(RoomOrigin::Wave { wave: wave.clone() }),
        })
        .await
        .unwrap();

        for (role, tid) in [
            (&role_a, &tid_a),
            (&role_b, &tid_b),
            (&role_c, &tid_c),
            (&role_d, &tid_d),
            (&role_e, &tid_e),
            (&role_f, &tid_f),
        ] {
            let obs = Observation::Spawned {
                attempt: 1,
                thread: tid.clone(),
            };
            sys.call(WaveObserve {
                wave: wave.clone(),
                role: role.clone(),
                observation: obs.clone(),
            })
            .await
            .unwrap();
            bridge.observe_wave_event(
                wave.clone(),
                WaveEvent::WorkerObserved {
                    role: role.clone(),
                    observation: obs,
                },
            );
            sys.call(RoomJoin {
                room: RoomRef::Id { id: room.clone() },
                member: Some(Principal::Worker {
                    wave: wave.clone(),
                    role: role.clone(),
                }),
                handle: Handle::parse(role.as_str()).ok(),
            })
            .await
            .unwrap();
        }

        let worker_p = |r: &RoleName| Principal::Worker {
            wave: wave.clone(),
            role: r.clone(),
        };

        harness
            .client(worker_p(&role_a))
            .call(RoomPost {
                room: RoomRef::Id { id: room.clone() },
                body: Body::parse("Identical verdict.").unwrap(),
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
                idem: Some(rex_wave_waves::mirror_idem_key(&wave, &role_a, &tid_a, "s10").unwrap()),
                origin: rex_wave_api::room::PostOrigin::Direct,
            })
            .await
            .unwrap();

        harness
            .client(worker_p(&role_b))
            .call(RoomPost {
                room: RoomRef::Id { id: room.clone() },
                body: Body::parse("Long single turn.").unwrap(),
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
                idem: Some(
                    rex_wave_waves::mirror_idem_key(&wave, &role_b, &tid_b, "s300").unwrap(),
                ),
                origin: rex_wave_api::room::PostOrigin::Direct,
            })
            .await
            .unwrap();

        harness
            .client(worker_p(&role_c))
            .call(RoomPost {
                room: RoomRef::Id { id: room.clone() },
                body: Body::parse("Identical verdict.").unwrap(),
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
                idem: Some(trunc_c_s10),
                origin: rex_wave_api::room::PostOrigin::Direct,
            })
            .await
            .unwrap();

        harness
            .client(worker_p(&role_d))
            .call(RoomPost {
                room: RoomRef::Id { id: room.clone() },
                body: Body::parse("Long single turn.").unwrap(),
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
                idem: Some(trunc_d_e525),
                origin: rex_wave_api::room::PostOrigin::Direct,
            })
            .await
            .unwrap();

        harness
            .client(worker_p(&role_f))
            .call(RoomPost {
                room: RoomRef::Id { id: room.clone() },
                body: Body::parse(full_delta_body).unwrap(),
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
                idem: Some(
                    rex_wave_waves::mirror_idem_key(&wave, &role_f, &tid_f, "s900").unwrap(),
                ),
                origin: rex_wave_api::room::PostOrigin::Direct,
            })
            .await
            .unwrap();

        for (role, tid) in [
            (&role_a, &tid_a),
            (&role_b, &tid_b),
            (&role_c, &tid_c),
            (&role_d, &tid_d),
            (&role_e, &tid_e),
        ] {
            let page1 = bridge.fetch_recent_thread_rows(tid).await;
            assert_eq!(page1.len(), 200);
            bridge
                .mirror_completed_turn_from_rows(&wave, role, tid, &page1)
                .await;
            bridge
                .mirror_completed_turn_from_rows(&wave, role, tid, &page1)
                .await;
        }

        let page1_f = bridge.fetch_recent_thread_rows(&tid_f).await;
        assert_eq!(page1_f.len(), 200);
        drop_next_backfill.store(true, Ordering::SeqCst);
        bridge
            .mirror_completed_turn_from_rows(&wave, &role_f, &tid_f, &page1_f)
            .await;

        let (wc_tx2, wc_rx2) = oneshot::channel();
        bridge
            .owner
            .send(OwnerMsg::WaitConnected { reply: wc_tx2 })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), wc_rx2)
            .await
            .unwrap()
            .unwrap();
        bridge
            .mirror_completed_turn_from_rows(&wave, &role_f, &tid_f, &page1_f)
            .await;

        let read_page = owner
            .call(Read::stream(
                StreamId::Room { room },
                Seq::ORIGIN,
                Some(100),
            ))
            .await
            .unwrap();
        let count_for = |r: &RoleName| -> usize {
            let p = worker_p(r);
            read_page
                .entries
                .iter()
                .filter(|e| e.by == p && matches!(e.event, Event::Room(RoomEvent::Posted { .. })))
                .count()
        };

        assert_eq!(count_for(&role_a), 2, "untruncated s10 vs backfilled s20");
        assert_eq!(count_for(&role_b), 1, "untruncated s300 deduped");
        assert_eq!(
            count_for(&role_c),
            2,
            "shared 256-byte ':s' truncation reconciled across T1 and T2"
        );
        assert_eq!(
            count_for(&role_d),
            1,
            "identifiable ':e5' legacy end-key still matches after s300 backfill"
        );
        assert_eq!(
            count_for(&role_e),
            1,
            "final agentMessage outside newest 200 events backfills before Ignore"
        );
        assert_eq!(
            count_for(&role_f),
            1,
            "disconnect during older-page backfill never publishes partial delta suffix"
        );

        let _ = shutdown_tx.send(true);
        let _ = bridge_runner.await;
        server_task.abort();
    }

    #[tokio::test]
    async fn reconnect_reconciles_unmirrored_turn_after_mid_backfill_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let token = "tok_reconnect_mirror";
        let drop_backfill_f = Arc::new(AtomicBool::new(false));
        let drop_backfill_g = Arc::new(AtomicBool::new(false));
        let drop_srv_f = drop_backfill_f.clone();
        let drop_srv_g = drop_backfill_g.clone();

        let full_delta_body = "HEAD-FROM-OLDER-PAGE::TAIL-IN-NEWEST-PAGE";
        let build_long_turn_events = || {
            let mut events = Vec::with_capacity(235);
            events.push(serde_json::json!({
                "seq": 905u64,
                "createdAt": 1_000u64,
                "type": "turn/started",
                "data": {}
            }));
            events.push(serde_json::json!({
                "seq": 906u64,
                "createdAt": 1_001u64,
                "type": "item/agentMessage/delta",
                "data": { "delta": "HEAD-FROM-OLDER-PAGE::" }
            }));
            for i in 0..218u64 {
                events.push(serde_json::json!({
                    "seq": 907 + i,
                    "createdAt": 1_002 + i,
                    "type": "tool/progress",
                    "data": { "step": i }
                }));
            }
            events.push(serde_json::json!({
                "seq": 1125u64,
                "createdAt": 1_225u64,
                "type": "item/agentMessage/delta",
                "data": { "delta": "TAIL-IN-NEWEST-PAGE" }
            }));
            events.push(serde_json::json!({
                "seq": 1126u64,
                "createdAt": 1_226u64,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }));
            events
        };
        let mut events_f = build_long_turn_events();
        let mut events_g = build_long_turn_events();

        let server_task = tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let Ok(ws) = ws::WsConn::accept(tcp, token).await else {
                    continue;
                };
                let (mut ws_reader, mut ws_writer) = ws.into_split();
                let _ = ws_writer
                    .send_text(
                        &serde_json::to_string(&protocol::Up::Hello {
                            plugin_version: "0.1.0".into(),
                            sdk_version: "0.6.132".into(),
                        })
                        .unwrap(),
                    )
                    .await;
                while let Ok(Some(ws::WsInbound::Text(raw))) = ws_reader.recv().await {
                    let down: protocol::Down = serde_json::from_str(&raw).unwrap();
                    match down {
                        protocol::Down::EventsPage {
                            req_id,
                            thread_id,
                            after_seq,
                            before_seq,
                            limit,
                            order,
                        } => {
                            if thread_id == "th_reconnect_f"
                                && before_seq.is_some()
                                && drop_srv_f.swap(false, Ordering::SeqCst)
                            {
                                events_f.extend([
                                    serde_json::json!({
                                        "seq": 1127u64,
                                        "createdAt": 1_300u64,
                                        "type": "turn/started",
                                        "data": {}
                                    }),
                                    serde_json::json!({
                                        "seq": 1128u64,
                                        "createdAt": 1_301u64,
                                        "type": "item/agentMessage/delta",
                                        "data": { "delta": "second reply" }
                                    }),
                                    serde_json::json!({
                                        "seq": 1129u64,
                                        "createdAt": 1_302u64,
                                        "type": "turn/completed",
                                        "data": { "status": "completed" }
                                    }),
                                ]);
                                break;
                            }
                            if thread_id == "th_reconnect_g"
                                && before_seq.is_some()
                                && drop_srv_g.swap(false, Ordering::SeqCst)
                            {
                                events_g.extend([
                                    serde_json::json!({
                                        "seq": 1127u64,
                                        "createdAt": 1_300u64,
                                        "type": "turn/started",
                                        "data": {}
                                    }),
                                    serde_json::json!({
                                        "seq": 1128u64,
                                        "createdAt": 1_301u64,
                                        "type": "item/completed",
                                        "data": {
                                            "item": {
                                                "type": "agentMessage",
                                                "text": "second reply"
                                            }
                                        }
                                    }),
                                    serde_json::json!({
                                        "seq": 1129u64,
                                        "createdAt": 1_302u64,
                                        "type": "turn/completed",
                                        "data": { "status": "completed" }
                                    }),
                                ]);
                                break;
                            }
                            let source = if thread_id == "th_reconnect_f" {
                                &events_f
                            } else {
                                &events_g
                            };
                            let mut filtered: Vec<serde_json::Value> = source
                                .iter()
                                .filter(|r| {
                                    let seq = r.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
                                    after_seq.is_none_or(|a| seq > a)
                                        && before_seq.is_none_or(|b| seq < b)
                                })
                                .cloned()
                                .collect();
                            if order == Some(protocol::EventsOrder::Desc) {
                                filtered.reverse();
                            }
                            filtered.truncate(limit as usize);
                            let reply = protocol::Up::Result {
                                req_id,
                                ok: true,
                                value: serde_json::Value::Array(filtered),
                            };
                            let _ = ws_writer
                                .send_text(&serde_json::to_string(&reply).unwrap())
                                .await;
                        }
                        protocol::Down::ThreadContext { req_id, .. } => {
                            let reply = protocol::Up::Result {
                                req_id,
                                ok: true,
                                value: serde_json::json!({}),
                            };
                            let _ = ws_writer
                                .send_text(&serde_json::to_string(&reply).unwrap())
                                .await;
                        }
                        _ => {}
                    }
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
        let _ = bridge.backend.set(Arc::new(harness.clone()));
        let sys = harness.client(Principal::System {
            part: SystemPart::WaveConductor,
        });
        let owner = harness.client(Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        });
        let wave = WaveSlug::parse("reconnect-mirror").unwrap();
        let proj = BbProjectId::parse("proj_reconnect").unwrap();
        let role_f = RoleName::parse("reconcile-disc").unwrap();
        let role_g = RoleName::parse("signal-disc").unwrap();
        let tid_f = BbThreadId::parse("th_reconnect_f").unwrap();
        let tid_g = BbThreadId::parse("th_reconnect_g").unwrap();

        owner
            .call(WaveStart {
                spec: WaveSpec {
                    slug: wave.clone(),
                    project: proj.clone(),
                    goal: Body::parse("test reconnect mirror recovery").unwrap(),
                    output_dir: None,
                    defaults: AgentSpec {
                        provider: ProviderId::parse("pi").unwrap(),
                        model: None,
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: PermissionMode::Full,
                        workspace: Workspace::ProjectDefault,
                    },
                    workers: [&role_f, &role_g]
                        .into_iter()
                        .map(|r| WorkerSpec {
                            role: r.clone(),
                            agent: None,
                            brief: Body::parse("work").unwrap(),
                            output_dir: None,
                            writes: Vec::new(),
                            return_schema: None,
                        })
                        .collect(),
                },
                idem: None,
            })
            .await
            .unwrap();

        let room = rex_wave_waves::wave_room(&wave, &proj);
        sys.call(RoomEnsure {
            room: RoomRef::Id { id: room.clone() },
            topic: None,
            origin: Some(RoomOrigin::Wave { wave: wave.clone() }),
        })
        .await
        .unwrap();

        for (role, tid) in [(&role_f, &tid_f), (&role_g, &tid_g)] {
            let obs = Observation::Spawned {
                attempt: 1,
                thread: tid.clone(),
            };
            sys.call(WaveObserve {
                wave: wave.clone(),
                role: role.clone(),
                observation: obs.clone(),
            })
            .await
            .unwrap();
            bridge.observe_wave_event(
                wave.clone(),
                WaveEvent::WorkerObserved {
                    role: role.clone(),
                    observation: obs,
                },
            );
            sys.call(RoomJoin {
                room: RoomRef::Id { id: room.clone() },
                member: Some(Principal::Worker {
                    wave: wave.clone(),
                    role: role.clone(),
                }),
                handle: Handle::parse(role.as_str()).ok(),
            })
            .await
            .unwrap();
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let bridge_runner = tokio::spawn({
            let bridge = bridge.clone();
            let harness = harness.clone();
            async move { bridge.run_until(harness, shutdown_rx).await }
        });
        let (wc_tx, wc_rx) = oneshot::channel();
        bridge
            .owner
            .send(OwnerMsg::WaitConnected { reply: wc_tx })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), wc_rx)
            .await
            .unwrap()
            .unwrap();
        bridge.reconcile_live_workers().await;

        let reset_for_probe = |r: &RoleName| Principal::Worker {
            wave: wave.clone(),
            role: r.clone(),
        };

        let _ = shutdown_tx.send(true);
        let _ = bridge_runner.await;

        let bridge2 = BbBridge::with_data_dir(
            BridgeConfig {
                url: format!("ws://{addr}/bridge"),
                token: token.into(),
            },
            None,
        );
        let (harness2, _frame_rx2) = LoopbackHarness::new(bridge2.clone());
        let _ = bridge2.backend.set(Arc::new(harness2.clone()));
        let sys2 = harness2.client(Principal::System {
            part: SystemPart::WaveConductor,
        });
        let owner2 = harness2.client(Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        });
        owner2
            .call(WaveStart {
                spec: WaveSpec {
                    slug: wave.clone(),
                    project: proj.clone(),
                    goal: Body::parse("test reconnect mirror recovery").unwrap(),
                    output_dir: None,
                    defaults: AgentSpec {
                        provider: ProviderId::parse("pi").unwrap(),
                        model: None,
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: PermissionMode::Full,
                        workspace: Workspace::ProjectDefault,
                    },
                    workers: [&role_f, &role_g]
                        .into_iter()
                        .map(|r| WorkerSpec {
                            role: r.clone(),
                            agent: None,
                            brief: Body::parse("work").unwrap(),
                            output_dir: None,
                            writes: Vec::new(),
                            return_schema: None,
                        })
                        .collect(),
                },
                idem: None,
            })
            .await
            .unwrap();
        sys2.call(RoomEnsure {
            room: RoomRef::Id { id: room.clone() },
            topic: None,
            origin: Some(RoomOrigin::Wave { wave: wave.clone() }),
        })
        .await
        .unwrap();
        for (role, tid) in [(&role_f, &tid_f), (&role_g, &tid_g)] {
            let obs = Observation::Spawned {
                attempt: 1,
                thread: tid.clone(),
            };
            sys2.call(WaveObserve {
                wave: wave.clone(),
                role: role.clone(),
                observation: obs.clone(),
            })
            .await
            .unwrap();
            bridge2.observe_wave_event(
                wave.clone(),
                WaveEvent::WorkerObserved {
                    role: role.clone(),
                    observation: obs,
                },
            );
            sys2.call(RoomJoin {
                room: RoomRef::Id { id: room.clone() },
                member: Some(reset_for_probe(role)),
                handle: Handle::parse(role.as_str()).ok(),
            })
            .await
            .unwrap();
        }

        bridge2
            .owner
            .send(OwnerMsg::AdvanceSeq {
                thread: tid_f.clone(),
                seq: 925,
            })
            .unwrap();
        bridge2
            .owner
            .send(OwnerMsg::AdvanceSeq {
                thread: tid_g.clone(),
                seq: 1126,
            })
            .unwrap();
        drop_backfill_f.store(true, Ordering::SeqCst);

        let (shutdown_tx2, shutdown_rx2) = watch::channel(false);
        let bridge_runner2 = tokio::spawn({
            let b = bridge2.clone();
            let h = harness2.clone();
            async move { b.run_until(h, shutdown_rx2).await }
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            while drop_backfill_f.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let (wc_tx2, wc_rx2) = oneshot::channel();
        bridge2
            .owner
            .send(OwnerMsg::WaitConnected { reply: wc_tx2 })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), wc_rx2)
            .await
            .unwrap()
            .unwrap();

        let _ = AgentFeed::page(&*bridge2, &wave, &role_g, Some(BbSeq(1120)), 50)
            .await
            .unwrap();
        drop_backfill_g.store(true, Ordering::SeqCst);
        bridge2
            .on_thread_signal(protocol::Up::Thread {
                signal: protocol::ThreadSignal::Idle,
                thread_id: tid_g.to_string(),
                project_id: proj.to_string(),
                meta: None,
                error: None,
                stop_reason: None,
                model: None,
                account: None,
            })
            .await;
        assert!(!drop_backfill_g.load(Ordering::SeqCst));

        let (wc_tx3, wc_rx3) = oneshot::channel();
        bridge2
            .owner
            .send(OwnerMsg::WaitConnected { reply: wc_tx3 })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), wc_rx3)
            .await
            .unwrap()
            .unwrap();
        bridge2.reconcile_live_workers().await;

        let read_page = owner2
            .call(Read::stream(
                StreamId::Room { room },
                Seq::ORIGIN,
                Some(100),
            ))
            .await
            .unwrap();
        for (label, role) in [("reconcile-disc", &role_f), ("signal-disc", &role_g)] {
            let p = reset_for_probe(role);
            let bodies: Vec<&str> = read_page
                .entries
                .iter()
                .filter(|e| e.by == p)
                .filter_map(|e| match &e.event {
                    Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                    _ => None,
                })
                .collect();
            assert_eq!(
                bodies,
                vec![full_delta_body, "second reply"],
                "{label}: reconnect must publish all completed turns in sequence order"
            );
        }

        let _ = shutdown_tx2.send(true);
        let _ = bridge_runner2.await;
        server_task.abort();
    }

    #[tokio::test]
    async fn late_mirror_from_replaced_thread_never_suppresses_replacement_replies() {
        let bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: "ws://127.0.0.1:9/unused".into(),
                token: String::new(),
            },
            None,
        );
        let (harness, _frame_rx) = LoopbackHarness::new(bridge.clone());
        let _ = bridge.backend.set(Arc::new(harness.clone()));
        let sys = harness.client(Principal::System {
            part: SystemPart::WaveConductor,
        });
        let owner = harness.client(Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        });
        let wave = WaveSlug::parse("replaced-thread-mirror").unwrap();
        let proj = BbProjectId::parse("proj_replaced").unwrap();
        let role_a = RoleName::parse("writer").unwrap();
        let role_b = RoleName::parse("same-text").unwrap();
        let old_tid_a = BbThreadId::parse(format!("th_old_a_{}", "x".repeat(230))).unwrap();
        let new_tid_a = BbThreadId::parse("th_new_a").unwrap();
        let old_tid_b = BbThreadId::parse("th_old_b").unwrap();
        let new_tid_b = BbThreadId::parse("th_new_b").unwrap();

        owner
            .call(WaveStart {
                spec: WaveSpec {
                    slug: wave.clone(),
                    project: proj.clone(),
                    goal: Body::parse("test replacement thread mirror isolation").unwrap(),
                    output_dir: None,
                    defaults: AgentSpec {
                        provider: ProviderId::parse("pi").unwrap(),
                        model: None,
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: PermissionMode::Full,
                        workspace: Workspace::ProjectDefault,
                    },
                    workers: [&role_a, &role_b]
                        .into_iter()
                        .map(|r| WorkerSpec {
                            role: r.clone(),
                            agent: None,
                            brief: Body::parse("write").unwrap(),
                            output_dir: None,
                            writes: Vec::new(),
                            return_schema: None,
                        })
                        .collect(),
                },
                idem: None,
            })
            .await
            .unwrap();

        let room = rex_wave_waves::wave_room(&wave, &proj);
        sys.call(RoomEnsure {
            room: RoomRef::Id { id: room.clone() },
            topic: None,
            origin: Some(RoomOrigin::Wave { wave: wave.clone() }),
        })
        .await
        .unwrap();

        for (role, old_tid, new_tid) in [
            (&role_a, &old_tid_a, &new_tid_a),
            (&role_b, &old_tid_b, &new_tid_b),
        ] {
            let spawned1 = Observation::Spawned {
                attempt: 1,
                thread: old_tid.clone(),
            };
            sys.call(WaveObserve {
                wave: wave.clone(),
                role: role.clone(),
                observation: spawned1.clone(),
            })
            .await
            .unwrap();
            bridge.observe_wave_event(
                wave.clone(),
                WaveEvent::WorkerObserved {
                    role: role.clone(),
                    observation: spawned1,
                },
            );
            sys.call(RoomJoin {
                room: RoomRef::Id { id: room.clone() },
                member: Some(Principal::Worker {
                    wave: wave.clone(),
                    role: role.clone(),
                }),
                handle: Handle::parse(role.as_str()).ok(),
            })
            .await
            .unwrap();
            bridge.observe_wave_event(
                wave.clone(),
                WaveEvent::WorkerObserved {
                    role: role.clone(),
                    observation: Observation::Spawned {
                        attempt: 2,
                        thread: new_tid.clone(),
                    },
                },
            );
        }

        for (tid, start, body) in [
            (&old_tid_a, 701u64, "old reply"),
            (&new_tid_a, 1u64, "replacement reply"),
        ] {
            let rows = [
                serde_json::json!({ "seq": start, "type": "turn/started", "createdAt": 1_000 }),
                serde_json::json!({
                    "seq": start + 1,
                    "type": "item/completed",
                    "createdAt": 1_001,
                    "data": { "item": { "type": "agentMessage", "text": body } }
                }),
                serde_json::json!({
                    "seq": start + 2,
                    "type": "turn/completed",
                    "createdAt": 1_002,
                    "data": {}
                }),
            ];
            assert!(
                bridge
                    .mirror_completed_turn_from_rows(&wave, &role_a, tid, &rows)
                    .await
            );
        }

        for (tid, start) in [(&old_tid_b, 901u64), (&new_tid_b, 1u64)] {
            let rows = [
                serde_json::json!({ "seq": start, "type": "turn/started", "createdAt": 2_000 }),
                serde_json::json!({
                    "seq": start + 1,
                    "type": "item/completed",
                    "createdAt": 2_001,
                    "data": { "item": { "type": "agentMessage", "text": "Done." } }
                }),
                serde_json::json!({
                    "seq": start + 2,
                    "type": "turn/completed",
                    "createdAt": 2_002,
                    "data": {}
                }),
            ];
            assert!(
                bridge
                    .mirror_completed_turn_from_rows(&wave, &role_b, tid, &rows)
                    .await
            );
        }

        let locked = harness.inner.lock().await;
        let entries = &locked.entries[&StreamId::Room { room }];
        let pa = Principal::Worker {
            wave: wave.clone(),
            role: role_a.clone(),
        };
        let pb = Principal::Worker {
            wave: wave.clone(),
            role: role_b.clone(),
        };
        let bodies_a: Vec<&str> = entries
            .iter()
            .filter(|e| e.by == pa)
            .filter_map(|e| match &e.event {
                Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                _ => None,
            })
            .collect();
        let bodies_b: Vec<&str> = entries
            .iter()
            .filter(|e| e.by == pb)
            .filter_map(|e| match &e.event {
                Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(bodies_a, vec!["old reply", "replacement reply"]);
        assert_eq!(bodies_b, vec!["Done.", "Done."]);
    }

    #[tokio::test]
    async fn shared_legacy_key_survives_room_scan_past_8000_posts() {
        let bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: "ws://127.0.0.1:9/unused".into(),
                token: String::new(),
            },
            None,
        );
        let (harness, _frame_rx) = LoopbackHarness::new(bridge.clone());
        let _ = bridge.backend.set(Arc::new(harness.clone()));
        let sys = harness.client(Principal::System {
            part: SystemPart::WaveConductor,
        });
        let owner = harness.client(Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        });
        let wave = WaveSlug::parse("long-mirror-scan").unwrap();
        let proj = BbProjectId::parse("proj_mirror").unwrap();
        let role = RoleName::parse("writer").unwrap();
        let long_tid = BbThreadId::parse(format!("th_{}", "x".repeat(235))).unwrap();

        owner
            .call(WaveStart {
                spec: WaveSpec {
                    slug: wave.clone(),
                    project: proj.clone(),
                    goal: Body::parse("test legacy mirror dedupe across 8000+ room posts").unwrap(),
                    output_dir: None,
                    defaults: AgentSpec {
                        provider: ProviderId::parse("pi").unwrap(),
                        model: None,
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: PermissionMode::Full,
                        workspace: Workspace::ProjectDefault,
                    },
                    workers: vec![WorkerSpec {
                        role: role.clone(),
                        agent: None,
                        brief: Body::parse("write").unwrap(),
                        output_dir: None,
                        writes: Vec::new(),
                        return_schema: None,
                    }],
                },
                idem: None,
            })
            .await
            .unwrap();
        let spawned = Observation::Spawned {
            attempt: 1,
            thread: long_tid.clone(),
        };
        sys.call(WaveObserve {
            wave: wave.clone(),
            role: role.clone(),
            observation: spawned.clone(),
        })
        .await
        .unwrap();
        bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: role.clone(),
                observation: spawned.clone(),
            },
        );

        let room = rex_wave_waves::wave_room(&wave, &proj);
        sys.call(RoomEnsure {
            room: RoomRef::Id { id: room.clone() },
            topic: None,
            origin: Some(RoomOrigin::Wave { wave: wave.clone() }),
        })
        .await
        .unwrap();
        let worker_principal = Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        };
        sys.call(RoomJoin {
            room: RoomRef::Id { id: room.clone() },
            member: Some(worker_principal.clone()),
            handle: Handle::parse("writer").ok(),
        })
        .await
        .unwrap();

        let legacy_idem =
            rex_wave_waves::legacy_truncated_mirror_idem_key(&wave, &role, &long_tid, "s999999999")
                .expect("exceeds 256 bytes");
        harness
            .client(worker_principal.clone())
            .call(RoomPost {
                room: RoomRef::Id { id: room.clone() },
                body: Body::parse("pre-upgrade mirrored turn").unwrap(),
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
                idem: Some(legacy_idem),
                origin: rex_wave_api::room::PostOrigin::Direct,
            })
            .await
            .unwrap();

        for _ in 0..8_001 {
            owner
                .call(RoomPost {
                    room: RoomRef::Id { id: room.clone() },
                    body: Body::parse("unrelated room traffic").unwrap(),
                    re: None,
                    after: None,
                    labels: Vec::new(),
                    task: None,
                    idem: None,
                    origin: rex_wave_api::room::PostOrigin::Direct,
                })
                .await
                .unwrap();
        }

        let turn1_rows = vec![
            serde_json::json!({
                "seq": 999_999_999u64,
                "createdAt": 900u64,
                "type": "turn/started",
                "data": {}
            }),
            serde_json::json!({
                "seq": 1_000_000_000u64,
                "createdAt": 950u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "pre-upgrade mirrored turn" } }
            }),
            serde_json::json!({
                "seq": 1_000_000_001u64,
                "createdAt": 990u64,
                "type": "turn/completed",
                "data": {}
            }),
        ];
        assert!(
            bridge
                .mirror_completed_turn_from_rows(&wave, &role, &long_tid, &turn1_rows)
                .await
        );
        assert!(
            bridge
                .mirror_completed_turn_from_rows(&wave, &role, &long_tid, &turn1_rows)
                .await
        );

        let count_mirrored = || async {
            let locked = harness.inner.lock().await;
            locked.entries[&StreamId::Room { room: room.clone() }]
                .iter()
                .filter(|e| {
                    matches!(
                        &e.event,
                        Event::Room(RoomEvent::Posted { body, .. })
                            if body.as_str() == "pre-upgrade mirrored turn"
                    )
                })
                .count()
        };
        assert_eq!(
            count_mirrored().await,
            1,
            "shared legacy key followed by 8,001 room posts must reconcile without duplicating"
        );

        let mut both_turns_rows = turn1_rows.clone();
        both_turns_rows.extend([
            serde_json::json!({
                "seq": 1_000_000_010u64,
                "createdAt": 2_000u64,
                "type": "turn/started",
                "data": {}
            }),
            serde_json::json!({
                "seq": 1_000_000_011u64,
                "createdAt": 2_050u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "pre-upgrade mirrored turn" } }
            }),
            serde_json::json!({
                "seq": 1_000_000_012u64,
                "createdAt": 2_090u64,
                "type": "turn/completed",
                "data": {}
            }),
        ]);
        let cold_bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: "ws://127.0.0.1:9/unused".into(),
                token: String::new(),
            },
            None,
        );
        let _ = cold_bridge.backend.set(Arc::new(harness.clone()));
        cold_bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: role.clone(),
                observation: spawned,
            },
        );
        assert!(
            cold_bridge
                .mirror_completed_turn_from_rows(&wave, &role, &long_tid, &both_turns_rows)
                .await
        );
        assert!(
            bridge
                .mirror_completed_turn_from_rows(&wave, &role, &long_tid, &both_turns_rows)
                .await
        );
        assert_eq!(
            count_mirrored().await,
            2,
            "distinct second turn with identical text must still publish once past 8,001 room posts"
        );
    }

    #[tokio::test]
    async fn failed_final_turn_still_mirrors_earlier_successful_reply_on_restart() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let token = "tok_failed_tail_restart";

        let mut short_fail_events = vec![
            serde_json::json!({ "seq": 905u64, "createdAt": 100u64, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 906u64,
                "createdAt": 101u64,
                "type": "item/agentMessage/delta",
                "data": { "delta": "Prefix " }
            }),
        ];
        let mut full_delta_body = String::from("Prefix ");
        for s in 907u64..1125u64 {
            short_fail_events.push(serde_json::json!({
                "seq": s,
                "createdAt": 101u64 + (s - 906),
                "type": "item/agentMessage/delta",
                "data": { "delta": "x" }
            }));
            full_delta_body.push('x');
        }
        short_fail_events.push(serde_json::json!({
            "seq": 1125u64,
            "createdAt": 990u64,
            "type": "item/agentMessage/delta",
            "data": { "delta": " suffix" }
        }));
        full_delta_body.push_str(" suffix");
        short_fail_events.push(serde_json::json!({
            "seq": 1126u64,
            "createdAt": 999u64,
            "type": "turn/completed",
            "data": { "status": "completed" }
        }));
        let mut long_fail_events = short_fail_events.clone();
        let mut refused_page_events = short_fail_events.clone();
        refused_page_events.splice(
            0..0,
            [
                serde_json::json!({ "seq": 901u64, "createdAt": 80u64, "type": "turn/started", "data": {} }),
                serde_json::json!({
                    "seq": 902u64,
                    "createdAt": 81u64,
                    "type": "item/agentMessage/delta",
                    "data": { "delta": "First reply" }
                }),
                serde_json::json!({
                    "seq": 903u64,
                    "createdAt": 82u64,
                    "type": "turn/completed",
                    "data": { "status": "completed" }
                }),
            ],
        );
        let live_fail_events = {
            let mut v = short_fail_events.clone();
            v.push(serde_json::json!({
                "seq": 1127u64,
                "createdAt": 1_010u64,
                "type": "turn/started",
                "data": {}
            }));
            v.push(serde_json::json!({
                "seq": 1128u64,
                "createdAt": 1_011u64,
                "type": "item/agentMessage/delta",
                "data": { "delta": "failed turn partial" }
            }));
            v.push(serde_json::json!({
                "seq": 1129u64,
                "createdAt": 1_012u64,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }));
            v.push(serde_json::json!({
                "seq": 1130u64,
                "createdAt": 1_013u64,
                "type": "thread/contextWindowUsage/updated",
                "data": {}
            }));
            v
        };
        short_fail_events.push(serde_json::json!({
            "seq": 1127u64,
            "createdAt": 1_010u64,
            "type": "turn/started",
            "data": {}
        }));
        short_fail_events.push(serde_json::json!({
            "seq": 1128u64,
            "createdAt": 1_012u64,
            "type": "turn/completed",
            "data": { "status": "failed", "error": { "message": "upstream timeout" } }
        }));

        long_fail_events.push(serde_json::json!({
            "seq": 1127u64,
            "createdAt": 1_010u64,
            "type": "turn/started",
            "data": {}
        }));
        for s in 1128u64..1330u64 {
            long_fail_events.push(serde_json::json!({
                "seq": s,
                "createdAt": 1_010u64 + (s - 1127),
                "type": "item/commandExecution/outputDelta",
                "data": { "delta": "stderr\n" }
            }));
        }
        long_fail_events.push(serde_json::json!({
            "seq": 1330u64,
            "createdAt": 1_300u64,
            "type": "turn/completed",
            "data": { "status": "failed", "error": { "message": "long turn crashed" } }
        }));

        refused_page_events.push(serde_json::json!({
            "seq": 1127u64,
            "createdAt": 1_010u64,
            "type": "turn/started",
            "data": {}
        }));
        for s in 1128u64..1975u64 {
            refused_page_events.push(serde_json::json!({
                "seq": s,
                "createdAt": 1_010u64 + (s - 1127),
                "type": "item/commandExecution/outputDelta",
                "data": { "delta": "stderr\n" }
            }));
        }
        refused_page_events.push(serde_json::json!({
            "seq": 1975u64,
            "createdAt": 1_999u64,
            "type": "item/agentMessage/delta",
            "data": { "delta": "refused draft that must never publish" }
        }));
        refused_page_events.push(serde_json::json!({
            "seq": 1976u64,
            "createdAt": 2_000u64,
            "type": "turn/completed",
            "data": { "status": "completed" }
        }));
        refused_page_events.push(serde_json::json!({
            "seq": 1977u64,
            "createdAt": 2_001u64,
            "type": "provider/error",
            "data": {
                "willRetry": false,
                "message": "blocked by policy",
                "errorInfo": { "category": "policy" }
            }
        }));
        for s in 1978u64..2828u64 {
            refused_page_events.push(serde_json::json!({
                "seq": s,
                "createdAt": 2_001u64 + (s - 1977),
                "type": "thread/contextWindowUsage/updated",
                "data": {}
            }));
        }

        let wave = WaveSlug::parse("failed-tail-restart").unwrap();
        let proj = BbProjectId::parse("proj_failed_tail").unwrap();
        let role_short = RoleName::parse("short-fail").unwrap();
        let role_long = RoleName::parse("long-fail").unwrap();
        let role_refused = RoleName::parse("refused-page").unwrap();
        let role_live = RoleName::parse("live-fail").unwrap();
        let tid_short = BbThreadId::parse("th_short_fail").unwrap();
        let tid_long = BbThreadId::parse("th_long_fail").unwrap();
        let tid_refused = BbThreadId::parse("th_refused_page").unwrap();
        let tid_live = BbThreadId::parse("th_live_fail").unwrap();

        let mut events_map: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        events_map.insert(tid_short.to_string(), short_fail_events);
        events_map.insert(tid_long.to_string(), long_fail_events);
        events_map.insert(tid_refused.to_string(), refused_page_events);
        events_map.insert(tid_live.to_string(), live_fail_events);
        let events_map = Arc::new(events_map);

        let server_task = tokio::spawn({
            let events_map = events_map.clone();
            async move {
                loop {
                    let Ok((tcp, _)) = listener.accept().await else {
                        break;
                    };
                    let Ok(ws) = ws::WsConn::accept(tcp, token).await else {
                        continue;
                    };
                    let (mut ws_reader, mut ws_writer) = ws.into_split();
                    let hello = protocol::Up::Hello {
                        plugin_version: "0.1.0".into(),
                        sdk_version: "0.6.132".into(),
                    };
                    let _ = ws_writer
                        .send_text(&serde_json::to_string(&hello).unwrap())
                        .await;
                    while let Ok(Some(ws::WsInbound::Text(raw))) = ws_reader.recv().await {
                        let Ok(down) = serde_json::from_str::<protocol::Down>(&raw) else {
                            continue;
                        };
                        if let protocol::Down::EventsPage {
                            req_id,
                            thread_id,
                            after_seq,
                            before_seq,
                            limit,
                            order,
                        } = down
                        {
                            let mut rows: Vec<serde_json::Value> = events_map
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
                            let _ = ws_writer
                                .send_text(&serde_json::to_string(&reply).unwrap())
                                .await;
                        }
                    }
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
        let sys = harness.client(Principal::System {
            part: SystemPart::WaveConductor,
        });
        let owner = harness.client(Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        });

        owner
            .call(WaveStart {
                spec: WaveSpec {
                    slug: wave.clone(),
                    project: proj.clone(),
                    goal: Body::parse("settle earlier reply before failed final turn").unwrap(),
                    output_dir: None,
                    defaults: AgentSpec {
                        provider: ProviderId::parse("pi").unwrap(),
                        model: None,
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: PermissionMode::Full,
                        workspace: Workspace::ProjectDefault,
                    },
                    workers: [&role_short, &role_long, &role_refused, &role_live]
                        .into_iter()
                        .map(|r| WorkerSpec {
                            role: r.clone(),
                            agent: None,
                            brief: Body::parse("reply").unwrap(),
                            output_dir: None,
                            writes: Vec::new(),
                            return_schema: None,
                        })
                        .collect(),
                },
                idem: None,
            })
            .await
            .unwrap();

        let room = rex_wave_waves::wave_room(&wave, &proj);
        sys.call(RoomEnsure {
            room: RoomRef::Id { id: room.clone() },
            topic: None,
            origin: Some(RoomOrigin::Wave { wave: wave.clone() }),
        })
        .await
        .unwrap();

        for (role, tid) in [
            (&role_short, &tid_short),
            (&role_long, &tid_long),
            (&role_refused, &tid_refused),
        ] {
            let spawned = Observation::Spawned {
                attempt: 1,
                thread: tid.clone(),
            };
            sys.call(WaveObserve {
                wave: wave.clone(),
                role: role.clone(),
                observation: spawned.clone(),
            })
            .await
            .unwrap();
            bridge.observe_wave_event(
                wave.clone(),
                WaveEvent::WorkerObserved {
                    role: role.clone(),
                    observation: spawned,
                },
            );
            sys.call(RoomJoin {
                room: RoomRef::Id { id: room.clone() },
                member: Some(Principal::Worker {
                    wave: wave.clone(),
                    role: role.clone(),
                }),
                handle: Handle::parse(role.as_str()).ok(),
            })
            .await
            .unwrap();
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let bridge_runner = tokio::spawn({
            let bridge = bridge.clone();
            let harness = harness.clone();
            async move { bridge.run_until(harness, shutdown_rx).await }
        });
        let (wc_tx, wc_rx) = oneshot::channel();
        bridge
            .owner
            .send(OwnerMsg::WaitConnected { reply: wc_tx })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), wc_rx)
            .await
            .unwrap()
            .unwrap();

        bridge.reconcile_live_workers().await;
        bridge.reconcile_live_workers().await;

        let spawned_live = Observation::Spawned {
            attempt: 1,
            thread: tid_live.clone(),
        };
        sys.call(WaveObserve {
            wave: wave.clone(),
            role: role_live.clone(),
            observation: spawned_live.clone(),
        })
        .await
        .unwrap();
        bridge.observe_wave_event(
            wave.clone(),
            WaveEvent::WorkerObserved {
                role: role_live.clone(),
                observation: spawned_live,
            },
        );
        sys.call(RoomJoin {
            room: RoomRef::Id { id: room.clone() },
            member: Some(Principal::Worker {
                wave: wave.clone(),
                role: role_live.clone(),
            }),
            handle: Handle::parse(role_live.as_str()).ok(),
        })
        .await
        .unwrap();
        bridge
            .on_thread_signal(protocol::Up::Thread {
                signal: protocol::ThreadSignal::Failed,
                thread_id: tid_live.to_string(),
                project_id: proj.to_string(),
                meta: None,
                error: Some("upstream timeout".into()),
                stop_reason: None,
                model: None,
                account: None,
            })
            .await;
        bridge.reconcile_live_workers().await;

        let read_page = owner
            .call(Read::stream(
                StreamId::Room { room },
                Seq::ORIGIN,
                Some(100),
            ))
            .await
            .unwrap();
        let snap = owner.call(WaveView { wave: wave.clone() }).await.unwrap();
        for (label, role, expect_refused) in [
            ("short-fail", &role_short, false),
            ("long-fail", &role_long, false),
            ("refused-page", &role_refused, true),
            ("live-fail", &role_live, false),
        ] {
            let p = Principal::Worker {
                wave: wave.clone(),
                role: role.clone(),
            };
            let bodies: Vec<&str> = read_page
                .entries
                .iter()
                .filter(|e| e.by == p)
                .filter_map(|e| match &e.event {
                    Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                    _ => None,
                })
                .collect();
            assert_eq!(
                bodies,
                if expect_refused {
                    vec!["First reply", full_delta_body.as_str()]
                } else {
                    vec![full_delta_body.as_str()]
                },
                "{label}: earlier completed replies must publish in order even when final turn failed"
            );
            let terminal_ok =
                snap.workers
                    .iter()
                    .find(|(r, _, _)| r == role)
                    .is_some_and(|(_, _, phase)| {
                        if expect_refused {
                            phase.refusal().is_some()
                        } else {
                            phase.error().is_some()
                        }
                    });
            assert!(
                terminal_ok,
                "{label}: worker must report terminal failure or refusal for final turn"
            );
        }

        let _ = shutdown_tx.send(true);
        let _ = bridge_runner.await;
        server_task.abort();
    }

    #[tokio::test]
    async fn direct_post_during_second_turn_does_not_duplicate_when_first_turn_mirrors() {
        let bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: "ws://127.0.0.1:9/unused".into(),
                token: String::new(),
            },
            None,
        );
        let (harness, _frame_rx) = LoopbackHarness::new(bridge.clone());
        let _ = bridge.backend.set(Arc::new(harness.clone()));
        let sys = harness.client(Principal::System {
            part: SystemPart::WaveConductor,
        });
        let owner = harness.client(Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        });
        let wave = WaveSlug::parse("direct-second-turn").unwrap();
        let proj = BbProjectId::parse("proj_direct_second").unwrap();
        let role_batch = RoleName::parse("batch-w").unwrap();
        let role_stream = RoleName::parse("stream-w").unwrap();
        let role_two_snap = RoleName::parse("two-snap-w").unwrap();
        let tid_batch = BbThreadId::parse("th_batch_w").unwrap();
        let tid_stream = BbThreadId::parse("th_stream_w").unwrap();
        let tid_two_snap = BbThreadId::parse("th_two_snap_w").unwrap();

        owner
            .call(WaveStart {
                spec: WaveSpec {
                    slug: wave.clone(),
                    project: proj.clone(),
                    goal: Body::parse(
                        "preserve room watermark until all outstanding turns check direct posts",
                    )
                    .unwrap(),
                    output_dir: None,
                    defaults: AgentSpec {
                        provider: ProviderId::parse("pi").unwrap(),
                        model: None,
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: PermissionMode::Full,
                        workspace: Workspace::ProjectDefault,
                    },
                    workers: [&role_batch, &role_stream, &role_two_snap]
                        .into_iter()
                        .map(|r| WorkerSpec {
                            role: r.clone(),
                            agent: None,
                            brief: Body::parse("work").unwrap(),
                            output_dir: None,
                            writes: Vec::new(),
                            return_schema: None,
                        })
                        .collect(),
                },
                idem: None,
            })
            .await
            .unwrap();

        let room = rex_wave_waves::wave_room(&wave, &proj);
        sys.call(RoomEnsure {
            room: RoomRef::Id { id: room.clone() },
            topic: None,
            origin: Some(RoomOrigin::Wave { wave: wave.clone() }),
        })
        .await
        .unwrap();

        for (role, tid) in [
            (&role_batch, &tid_batch),
            (&role_stream, &tid_stream),
            (&role_two_snap, &tid_two_snap),
        ] {
            let spawned = Observation::Spawned {
                attempt: 1,
                thread: tid.clone(),
            };
            sys.call(WaveObserve {
                wave: wave.clone(),
                role: role.clone(),
                observation: spawned.clone(),
            })
            .await
            .unwrap();
            bridge.observe_wave_event(
                wave.clone(),
                WaveEvent::WorkerObserved {
                    role: role.clone(),
                    observation: spawned,
                },
            );
            sys.call(RoomJoin {
                room: RoomRef::Id { id: room.clone() },
                member: Some(Principal::Worker {
                    wave: wave.clone(),
                    role: role.clone(),
                }),
                handle: Handle::parse(role.as_str()).ok(),
            })
            .await
            .unwrap();
        }

        let p_batch = Principal::Worker {
            wave: wave.clone(),
            role: role_batch.clone(),
        };
        let direct_batch = harness
            .client(p_batch.clone())
            .call(RoomPost {
                room: RoomRef::Id { id: room.clone() },
                body: Body::parse("already sent via CLI").unwrap(),
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
                idem: None,
                origin: rex_wave_api::room::PostOrigin::Direct,
            })
            .await
            .unwrap();
        let start2_batch = {
            let locked = harness.inner.lock().await;
            locked.entries[&StreamId::Room { room: room.clone() }]
                .iter()
                .find(|e| e.seq == direct_batch.seq)
                .unwrap()
                .at
                .0
        };
        let batch_rows = vec![
            serde_json::json!({ "seq": 1u64, "createdAt": start2_batch - 5, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 2u64,
                "createdAt": start2_batch - 4,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "first" } }
            }),
            serde_json::json!({
                "seq": 3u64,
                "createdAt": start2_batch - 3,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }),
            serde_json::json!({ "seq": 4u64, "createdAt": start2_batch, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 5u64,
                "createdAt": start2_batch + 1,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "second" } }
            }),
            serde_json::json!({
                "seq": 6u64,
                "createdAt": start2_batch + 2,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }),
        ];
        assert!(
            bridge
                .mirror_outstanding_turns_from_tail(&wave, &role_batch, &tid_batch, &batch_rows)
                .await
        );

        let p_stream = Principal::Worker {
            wave: wave.clone(),
            role: role_stream.clone(),
        };
        let direct_stream = harness
            .client(p_stream.clone())
            .call(RoomPost {
                room: RoomRef::Id { id: room.clone() },
                body: Body::parse("already sent via CLI").unwrap(),
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
                idem: None,
                origin: rex_wave_api::room::PostOrigin::Direct,
            })
            .await
            .unwrap();
        let start2_stream = {
            let locked = harness.inner.lock().await;
            locked.entries[&StreamId::Room { room: room.clone() }]
                .iter()
                .find(|e| e.seq == direct_stream.seq)
                .unwrap()
                .at
                .0
        };
        let stream_pass1 = vec![
            serde_json::json!({ "seq": 1u64, "createdAt": start2_stream - 5, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 2u64,
                "createdAt": start2_stream - 4,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "first" } }
            }),
            serde_json::json!({
                "seq": 3u64,
                "createdAt": start2_stream - 3,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }),
            serde_json::json!({ "seq": 4u64, "createdAt": start2_stream, "type": "turn/started", "data": {} }),
        ];
        assert!(
            bridge
                .mirror_outstanding_turns_from_tail(&wave, &role_stream, &tid_stream, &stream_pass1)
                .await
        );
        let mut stream_pass2 = stream_pass1;
        stream_pass2.push(serde_json::json!({
            "seq": 5u64,
            "createdAt": start2_stream + 1,
            "type": "item/completed",
            "data": { "item": "unused" }
        }));
        let _ = stream_pass2.pop();
        stream_pass2.push(serde_json::json!({
            "seq": 5u64,
            "createdAt": start2_stream + 1,
            "type": "item/completed",
            "data": { "item": { "type": "agentMessage", "text": "second" } }
        }));
        stream_pass2.push(serde_json::json!({
            "seq": 6u64,
            "createdAt": start2_stream + 2,
            "type": "turn/completed",
            "data": { "status": "completed" }
        }));
        assert!(
            bridge
                .mirror_outstanding_turns_from_tail(&wave, &role_stream, &tid_stream, &stream_pass2)
                .await
        );

        let p_two_snap = Principal::Worker {
            wave: wave.clone(),
            role: role_two_snap.clone(),
        };
        let direct_two_snap = harness
            .client(p_two_snap.clone())
            .call(RoomPost {
                room: RoomRef::Id { id: room.clone() },
                body: Body::parse("already sent via CLI").unwrap(),
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
                idem: None,
                origin: rex_wave_api::room::PostOrigin::Direct,
            })
            .await
            .unwrap();
        let start2_two_snap = {
            let locked = harness.inner.lock().await;
            locked.entries[&StreamId::Room { room: room.clone() }]
                .iter()
                .find(|e| e.seq == direct_two_snap.seq)
                .unwrap()
                .at
                .0
        };
        let two_snap_full = vec![
            serde_json::json!({ "seq": 1u64, "createdAt": start2_two_snap - 5, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 2u64,
                "createdAt": start2_two_snap - 4,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "first" } }
            }),
            serde_json::json!({
                "seq": 3u64,
                "createdAt": start2_two_snap - 3,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }),
            serde_json::json!({ "seq": 4u64, "createdAt": start2_two_snap, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 5u64,
                "createdAt": start2_two_snap + 1,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "second" } }
            }),
            serde_json::json!({
                "seq": 6u64,
                "createdAt": start2_two_snap + 2,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }),
        ];
        assert!(
            bridge
                .mirror_outstanding_turns_from_tail(
                    &wave,
                    &role_two_snap,
                    &tid_two_snap,
                    &two_snap_full[..3]
                )
                .await
        );
        assert!(
            bridge
                .mirror_outstanding_turns_from_tail(
                    &wave,
                    &role_two_snap,
                    &tid_two_snap,
                    &two_snap_full
                )
                .await
        );

        let locked = harness.inner.lock().await;
        let entries = &locked.entries[&StreamId::Room { room }];
        for (label, p) in [
            ("batch", &p_batch),
            ("stream", &p_stream),
            ("two-snap", &p_two_snap),
        ] {
            let bodies: Vec<&str> = entries
                .iter()
                .filter(|e| &e.by == p)
                .filter_map(|e| match &e.event {
                    Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                    _ => None,
                })
                .collect();
            assert_eq!(
                bodies,
                vec!["already sent via CLI", "first"],
                "{label}: turn 2 direct post must suppress turn 2 mirror even after turn 1 mirrors"
            );
        }
    }

    async fn run_bb_suppression_fault_matrix_case(scenario: &str) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let token = "tok_bb_faults";
        let wave = WaveSlug::parse("bb-fault-inject").unwrap();
        let proj = BbProjectId::parse("proj_bb_fault").unwrap();
        let role_live = RoleName::parse("live-retry").unwrap();
        let role_cursor = RoleName::parse("cursor-unanch").unwrap();
        let role_same_idle = RoleName::parse("same-idle").unwrap();
        let role_cold = RoleName::parse("cold-hydrate").unwrap();
        let tid_live = BbThreadId::parse("th_live_retry").unwrap();
        let tid_cursor = BbThreadId::parse("th_cursor_unanch").unwrap();
        let tid_same_idle = BbThreadId::parse("th_same_idle").unwrap();
        let tid_cold = BbThreadId::parse("th_cold_hydrate").unwrap();

        let live_rows = vec![
            serde_json::json!({ "seq": 1u64, "createdAt": 100u64, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 2u64,
                "createdAt": 101u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "Earlier completed reply" } }
            }),
            serde_json::json!({
                "seq": 3u64,
                "createdAt": 102u64,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }),
            serde_json::json!({ "seq": 4u64, "createdAt": 110u64, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 5u64,
                "createdAt": 111u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "failed turn partial" } }
            }),
            serde_json::json!({
                "seq": 6u64,
                "createdAt": 112u64,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }),
        ];
        let cold_rows = vec![
            serde_json::json!({ "seq": 1u64, "createdAt": 200u64, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 2u64,
                "createdAt": 201u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "Suppressed cold draft" } }
            }),
            serde_json::json!({
                "seq": 3u64,
                "createdAt": 202u64,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }),
            serde_json::json!({ "seq": 4u64, "createdAt": 210u64, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 5u64,
                "createdAt": 211u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "Clean cold reply" } }
            }),
            serde_json::json!({
                "seq": 6u64,
                "createdAt": 212u64,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }),
        ];

        let server_rows: Arc<std::sync::Mutex<HashMap<String, Vec<serde_json::Value>>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let fail_events_page_once = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first_rpc = Arc::new(tokio::sync::Notify::new());
        let server_first_rpc = first_rpc.clone();
        let server_rows_srv = server_rows.clone();
        let fail_ep_srv = fail_events_page_once.clone();
        let server_task = tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    break;
                };
                let ws = ws::WsConn::accept(tcp, token).await.unwrap();
                let (mut ws_reader, mut ws_writer) = ws.into_split();
                let hello = protocol::Up::Hello {
                    plugin_version: "0.1.0".into(),
                    sdk_version: "0.6.132".into(),
                };
                let _ = ws_writer
                    .send_text(&serde_json::to_string(&hello).unwrap())
                    .await;
                while let Ok(Some(ws::WsInbound::Text(raw))) = ws_reader.recv().await {
                    let Ok(down) = serde_json::from_str::<protocol::Down>(&raw) else {
                        continue;
                    };
                    if let protocol::Down::EventsPage {
                        req_id,
                        thread_id,
                        after_seq,
                        before_seq,
                        limit,
                        order,
                    } = down
                    {
                        server_first_rpc.notify_one();
                        if fail_ep_srv.swap(false, std::sync::atomic::Ordering::SeqCst) {
                            let err_reply = protocol::Up::Result {
                                req_id,
                                ok: false,
                                value: serde_json::json!("transient RPC error"),
                            };
                            let _ = ws_writer
                                .send_text(&serde_json::to_string(&err_reply).unwrap())
                                .await;
                            continue;
                        }
                        let src = server_rows_srv
                            .lock()
                            .unwrap()
                            .get(&thread_id)
                            .cloned()
                            .unwrap_or_default();
                        let mut rows: Vec<serde_json::Value> = src
                            .into_iter()
                            .filter(|r| {
                                let s = r["seq"].as_u64().unwrap_or(0);
                                after_seq.is_none_or(|a| s > a) && before_seq.is_none_or(|b| s < b)
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
                        let _ = ws_writer
                            .send_text(&serde_json::to_string(&reply).unwrap())
                            .await;
                    }
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
        let sys = harness.client(Principal::System {
            part: SystemPart::WaveConductor,
        });
        let owner = harness.client(Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        });

        owner
            .call(WaveStart {
                spec: WaveSpec {
                    slug: wave.clone(),
                    project: proj.clone(),
                    goal: Body::parse("survive suppression write and hydration read failures")
                        .unwrap(),
                    output_dir: None,
                    defaults: AgentSpec {
                        provider: ProviderId::parse("pi").unwrap(),
                        model: None,
                        account: None,
                        reasoning: None,
                        service_tier: None,
                        permission: PermissionMode::Full,
                        workspace: Workspace::ProjectDefault,
                    },
                    workers: [&role_live, &role_cursor, &role_same_idle, &role_cold]
                        .into_iter()
                        .map(|r| WorkerSpec {
                            role: r.clone(),
                            agent: None,
                            brief: Body::parse("work").unwrap(),
                            output_dir: None,
                            writes: Vec::new(),
                            return_schema: None,
                        })
                        .collect(),
                },
                idem: None,
            })
            .await
            .unwrap();

        let room = rex_wave_waves::wave_room(&wave, &proj);
        sys.call(RoomEnsure {
            room: RoomRef::Id { id: room.clone() },
            topic: None,
            origin: Some(RoomOrigin::Wave { wave: wave.clone() }),
        })
        .await
        .unwrap();

        for (role, tid) in [
            (&role_live, &tid_live),
            (&role_cursor, &tid_cursor),
            (&role_same_idle, &tid_same_idle),
            (&role_cold, &tid_cold),
        ] {
            let spawned = Observation::Spawned {
                attempt: 1,
                thread: tid.clone(),
            };
            sys.call(WaveObserve {
                wave: wave.clone(),
                role: role.clone(),
                observation: spawned.clone(),
            })
            .await
            .unwrap();
            bridge.observe_wave_event(
                wave.clone(),
                WaveEvent::WorkerObserved {
                    role: role.clone(),
                    observation: spawned,
                },
            );
            sys.call(RoomJoin {
                room: RoomRef::Id { id: room.clone() },
                member: Some(Principal::Worker {
                    wave: wave.clone(),
                    role: role.clone(),
                }),
                handle: Handle::parse(role.as_str()).ok(),
            })
            .await
            .unwrap();
        }

        sys.call(WaveObserve {
            wave: wave.clone(),
            role: role_cold.clone(),
            observation: Observation::TurnsSuppressed {
                thread: tid_cold.clone(),
                keys: vec!["s1".into(), "e3".into()],
            },
        })
        .await
        .unwrap();

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let bridge_runner = tokio::spawn({
            let bridge = bridge.clone();
            let harness = harness.clone();
            async move { bridge.run_until(harness, shutdown_rx).await }
        });
        let (wc_tx, wc_rx) = oneshot::channel();
        bridge
            .owner
            .send(OwnerMsg::WaitConnected { reply: wc_tx })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), wc_rx)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), first_rpc.notified())
            .await
            .unwrap();
        bridge.flush_signal_queue().await;

        if scenario.starts_with("r28_") {
            let t = tid_cursor.clone();
            let failed = |msg: &str| protocol::Up::Thread {
                signal: protocol::ThreadSignal::Failed,
                thread_id: t.to_string(),
                project_id: proj.to_string(),
                meta: None,
                error: Some(msg.to_owned()),
                stop_reason: None,
                model: None,
                account: None,
            };
            let idle = || protocol::Up::Thread {
                signal: protocol::ThreadSignal::Idle,
                thread_id: t.to_string(),
                project_id: proj.to_string(),
                meta: None,
                error: None,
                stop_reason: None,
                model: None,
                account: None,
            };
            let publish = |rows: &[serde_json::Value]| {
                server_rows
                    .lock()
                    .unwrap()
                    .insert(t.to_string(), rows.to_vec());
            };
            let mut three = live_rows.clone();
            for (i, src_row) in live_rows[..3].iter().enumerate() {
                let mut row = src_row.clone();
                row["seq"] = serde_json::json!(7 + i);
                row["createdAt"] = serde_json::json!(120 + i);
                if i == 1 {
                    row["data"]["item"]["text"] = serde_json::json!("third reply");
                }
                three.push(row);
            }
            publish(&live_rows[..3]);
            bridge.reconcile_live_workers().await;
            if scenario != "r28_successful_stale_read" {
                publish(&live_rows);
            }
            let fail_page =
                || fail_events_page_once.store(true, std::sync::atomic::Ordering::SeqCst);
            match scenario {
                "r28_pending_write" => {
                    fail_page();
                    bridge.on_thread_signal(failed("second turn failed")).await;
                    harness.inner.lock().await.fail_next_turns_suppressed = 2;
                    bridge.on_thread_signal(idle()).await;
                    assert_eq!(harness.inner.lock().await.fail_next_turns_suppressed, 0);
                    publish(&three);
                    bridge.on_thread_signal(idle()).await;
                }
                "r28_successor_failed" => {
                    fail_page();
                    bridge.on_thread_signal(failed("second turn failed")).await;
                    publish(&three);
                    bridge.on_thread_signal(failed("third turn failed")).await;
                }
                "r28_duplicate_failure" => {
                    for msg in ["turn failed", "thread failed"] {
                        fail_page();
                        bridge.on_thread_signal(failed(msg)).await;
                    }
                    publish(&three);
                    bridge.on_thread_signal(idle()).await;
                }
                "r28_control" => {
                    bridge.on_thread_signal(failed("second turn failed")).await;
                    publish(&three);
                    bridge.on_thread_signal(idle()).await;
                }
                "r28_two_start_markers" => {
                    fail_page();
                    bridge.on_thread_signal(failed("second turn failed")).await;
                    let mut rows = live_rows[..4].to_vec();
                    rows[3]["type"] = serde_json::json!("client/turn/start");
                    publish(&rows);
                    bridge.reconcile_live_workers().await;
                    for item in &live_rows[3..6] {
                        let mut row = item.clone();
                        row["seq"] = serde_json::json!(row["seq"].as_u64().unwrap() + 1);
                        rows.push(row);
                    }
                    publish(&rows);
                    bridge.reconcile_live_workers().await;
                }
                "r28_restart" | "r28_restart_write_fault" => {
                    if scenario == "r28_restart_write_fault" {
                        harness.inner.lock().await.fail_next_turns_suppressed = 2;
                    }
                    fail_page();
                    bridge.on_thread_signal(failed("second turn failed")).await;
                    let _ = shutdown_tx.send(true);
                    let _ = bridge_runner.await;
                    publish(&three);
                    let cold = BbBridge::with_data_dir(
                        BridgeConfig {
                            url: format!("ws://{addr}/bridge"),
                            token: token.into(),
                        },
                        None,
                    );
                    let fresh = LoopbackHarness {
                        bridge: cold.clone(),
                        ..harness.clone()
                    };
                    let history = harness.inner.lock().await.entries
                        [&StreamId::Wave { wave: wave.clone() }]
                        .clone();
                    for env in history {
                        if let Event::Wave(ev) = env.event {
                            cold.observe_wave_event(wave.clone(), ev);
                        }
                    }
                    let (stop, go) = watch::channel(false);
                    let (tx, rx) = oneshot::channel();
                    cold.owner
                        .send(OwnerMsg::WaitConnected { reply: tx })
                        .unwrap();
                    let running = tokio::spawn(cold.clone().run_until(fresh, go));
                    tokio::time::timeout(Duration::from_secs(5), rx)
                        .await
                        .unwrap()
                        .unwrap();
                    cold.reconcile_live_workers().await;
                    cold.flush_signal_queue().await;
                    let _ = stop.send(true);
                    let _ = running.await;
                    server_task.abort();
                    let page = owner
                        .call(Read::stream(
                            StreamId::Room { room },
                            Seq::ORIGIN,
                            Some(100),
                        ))
                        .await
                        .unwrap();
                    let bodies: Vec<_> = page
                        .entries
                        .iter()
                        .filter_map(|e| match &e.event {
                            Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                            _ => None,
                        })
                        .collect();
                    let snap = owner.call(WaveView { wave: wave.clone() }).await.unwrap();
                    let phase = &snap
                        .workers
                        .iter()
                        .find(|(r, _, _)| r == &role_cursor)
                        .unwrap()
                        .2;
                    assert_eq!(
                        bodies,
                        vec!["Earlier completed reply", "third reply"],
                        "{scenario}"
                    );
                    assert!(
                        matches!(
                            phase,
                            WorkerPhase::Live {
                                activity: Activity::Idle,
                                ..
                            }
                        ),
                        "{scenario}: {phase:?}"
                    );
                    return;
                }
                "r28_post_fetch_guard" => {
                    harness.inner.lock().await.on_next_room_read =
                        Some(failed("second turn failed"));
                    bridge.handle_up(protocol::Up::Hello {
                        plugin_version: "0.1.0".into(),
                        sdk_version: "0.6.132".into(),
                    });
                    bridge.flush_signal_queue().await;
                    bridge.flush_signal_queue().await;
                    assert!(harness.inner.lock().await.on_next_room_read.is_none());
                }
                "r28_idle_queued" => {
                    bridge.handle_up(idle());
                    bridge.handle_up(failed("second turn failed"));
                    bridge.flush_signal_queue().await;
                }
                "r28_successful_stale_read" => {
                    bridge.on_thread_signal(failed("second turn failed")).await;
                    publish(&live_rows);
                    bridge.reconcile_live_workers().await;
                }
                _ => unreachable!(),
            }
            bridge.reconcile_live_workers().await;
            let page = owner
                .call(Read::stream(
                    StreamId::Room { room },
                    Seq::ORIGIN,
                    Some(100),
                ))
                .await
                .unwrap();
            let bodies: Vec<_> = page
                .entries
                .iter()
                .filter_map(|e| match &e.event {
                    Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                    _ => None,
                })
                .collect();
            let snap = owner.call(WaveView { wave: wave.clone() }).await.unwrap();
            let phase = &snap
                .workers
                .iter()
                .find(|(r, _, _)| r == &role_cursor)
                .unwrap()
                .2;
            let expect_recovered = matches!(
                scenario,
                "r28_control" | "r28_pending_write" | "r28_duplicate_failure"
            );
            let expected = if expect_recovered {
                vec!["Earlier completed reply", "third reply"]
            } else {
                vec!["Earlier completed reply"]
            };
            assert_eq!(bodies, expected, "{scenario}");
            if expect_recovered {
                assert!(
                    matches!(
                        phase,
                        WorkerPhase::Live {
                            activity: Activity::Idle,
                            ..
                        }
                    ),
                    "{scenario}: {phase:?}"
                );
            } else {
                assert!(phase.error().is_some(), "{scenario}: {phase:?}");
            }
            let _ = shutdown_tx.send(true);
            let _ = bridge_runner.await;
            server_task.abort();
            return;
        }

        server_rows
            .lock()
            .unwrap()
            .insert(tid_live.to_string(), live_rows.clone());
        harness.inner.lock().await.fail_next_turns_suppressed = 2;
        let failed_live = protocol::Up::Thread {
            signal: protocol::ThreadSignal::Failed,
            thread_id: tid_live.to_string(),
            project_id: proj.to_string(),
            meta: None,
            error: Some("upstream timeout".into()),
            stop_reason: None,
            model: None,
            account: None,
        };
        if scenario == "hello-first" || scenario == "failed-first" {
            let hello = protocol::Up::Hello {
                plugin_version: "0.1.0".into(),
                sdk_version: "0.6.132".into(),
            };
            if scenario == "hello-first" {
                bridge.handle_up(hello);
                bridge.handle_up(failed_live);
            } else {
                bridge.handle_up(failed_live);
                bridge.handle_up(hello);
            }
            bridge.flush_signal_queue().await;
        } else if scenario == "concurrent-join" {
            tokio::join!(
                bridge.on_thread_signal(failed_live),
                bridge.reconcile_live_workers(),
            );
        } else {
            bridge.on_thread_signal(failed_live).await;
            bridge.reconcile_live_workers().await;
        }
        bridge.reconcile_live_workers().await;

        server_rows
            .lock()
            .unwrap()
            .insert(tid_cursor.to_string(), live_rows[..3].to_vec());
        bridge.reconcile_live_workers().await;
        if scenario != "stale-retry" {
            server_rows
                .lock()
                .unwrap()
                .insert(tid_cursor.to_string(), live_rows.clone());
        }
        fail_events_page_once.store(
            scenario != "spawn-only",
            std::sync::atomic::Ordering::SeqCst,
        );
        harness.inner.lock().await.fail_next_spawned_observe = 1;
        bridge
            .on_thread_signal(protocol::Up::Thread {
                signal: protocol::ThreadSignal::Failed,
                thread_id: tid_cursor.to_string(),
                project_id: proj.to_string(),
                meta: None,
                error: Some("upstream timeout".into()),
                stop_reason: None,
                model: None,
                account: None,
            })
            .await;
        if scenario == "stale-retry" {
            bridge.reconcile_live_workers().await;
            server_rows
                .lock()
                .unwrap()
                .insert(tid_cursor.to_string(), live_rows.clone());
            harness.inner.lock().await.fail_next_turns_suppressed = 1;
            bridge.reconcile_live_workers().await;
        } else if scenario == "two-turn-retry" {
            let mut more = live_rows.clone();
            for (i, v) in live_rows[..3].iter().enumerate() {
                let mut row = v.clone();
                row["seq"] = serde_json::json!(i as u64 + 7);
                row["createdAt"] = serde_json::json!(i as u64 + 120);
                if i == 1 {
                    row["data"]["item"]["text"] = serde_json::json!("Clean third reply");
                }
                more.push(row);
            }
            server_rows
                .lock()
                .unwrap()
                .insert(tid_cursor.to_string(), more);
        }
        bridge.reconcile_live_workers().await;

        server_rows
            .lock()
            .unwrap()
            .insert(tid_same_idle.to_string(), live_rows.clone());
        harness.inner.lock().await.fail_next_turns_suppressed = 2;
        bridge
            .on_thread_signal(protocol::Up::Thread {
                signal: protocol::ThreadSignal::Failed,
                thread_id: tid_same_idle.to_string(),
                project_id: proj.to_string(),
                meta: None,
                error: Some("upstream timeout".into()),
                stop_reason: (scenario == "refusal").then(|| "refusal".to_owned()),
                model: None,
                account: None,
            })
            .await;
        bridge
            .on_thread_signal(protocol::Up::Thread {
                signal: protocol::ThreadSignal::Idle,
                thread_id: tid_same_idle.to_string(),
                project_id: proj.to_string(),
                meta: None,
                error: None,
                stop_reason: None,
                model: None,
                account: None,
            })
            .await;
        bridge.reconcile_live_workers().await;
        bridge
            .on_thread_signal(protocol::Up::Thread {
                signal: protocol::ThreadSignal::Idle,
                thread_id: tid_same_idle.to_string(),
                project_id: proj.to_string(),
                meta: None,
                error: None,
                stop_reason: None,
                model: None,
                account: None,
            })
            .await;

        let snap_mid = owner.call(WaveView { wave: wave.clone() }).await.unwrap();
        if scenario == "refusal" {
            assert!(snap_mid.workers.iter().any(|(r, _, p)| {
                r == &role_same_idle
                    && matches!(
                        p,
                        WorkerPhase::Live {
                            activity: Activity::Refused { .. },
                            ..
                        }
                    )
            }));
        } else {
            let same_idle_errored = snap_mid
                .workers
                .iter()
                .find(|(r, _, _)| r == &role_same_idle)
                .is_some_and(|(_, _, phase)| phase.error().is_some());
            assert!(same_idle_errored, "scenario={scenario}");
        }

        let mut recovered_rows = live_rows.clone();
        recovered_rows.extend([
            serde_json::json!({ "seq": 7u64, "createdAt": 120u64, "type": "turn/started", "data": {} }),
            serde_json::json!({
                "seq": 8u64,
                "createdAt": 121u64,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "Recovered third reply" } }
            }),
            serde_json::json!({
                "seq": 9u64,
                "createdAt": 122u64,
                "type": "turn/completed",
                "data": { "status": "completed" }
            }),
        ]);
        if scenario == "second-failure" {
            recovered_rows[7]["data"]["item"]["text"] = serde_json::json!("failed third draft");
        }
        server_rows
            .lock()
            .unwrap()
            .insert(tid_same_idle.to_string(), recovered_rows);
        if scenario == "second-failure" {
            fail_events_page_once.store(true, std::sync::atomic::Ordering::SeqCst);
            bridge
                .on_thread_signal(protocol::Up::Thread {
                    signal: protocol::ThreadSignal::Failed,
                    thread_id: tid_same_idle.to_string(),
                    project_id: proj.to_string(),
                    meta: None,
                    error: Some("second failure".into()),
                    stop_reason: None,
                    model: None,
                    account: None,
                })
                .await;
        }
        bridge
            .on_thread_signal(protocol::Up::Thread {
                signal: protocol::ThreadSignal::Idle,
                thread_id: tid_same_idle.to_string(),
                project_id: proj.to_string(),
                meta: None,
                error: None,
                stop_reason: None,
                model: None,
                account: None,
            })
            .await;

        let cold_bridge = BbBridge::with_data_dir(
            BridgeConfig {
                url: "ws://127.0.0.1:9/unused".into(),
                token: String::new(),
            },
            None,
        );
        let _ = cold_bridge.backend.set(Arc::new(harness.clone()));
        harness.inner.lock().await.fail_next_wave_reads = 1;
        assert!(
            !cold_bridge
                .mirror_outstanding_turns_from_tail(&wave, &role_cold, &tid_cold, &cold_rows)
                .await
        );
        assert!(
            cold_bridge
                .mirror_outstanding_turns_from_tail(&wave, &role_cold, &tid_cold, &cold_rows)
                .await
        );

        let read_page = owner
            .call(Read::stream(
                StreamId::Room { room },
                Seq::ORIGIN,
                Some(100),
            ))
            .await
            .unwrap();
        let snap = owner.call(WaveView { wave: wave.clone() }).await.unwrap();

        let bodies_for = |r: &RoleName| -> Vec<&str> {
            let p = Principal::Worker {
                wave: wave.clone(),
                role: r.clone(),
            };
            read_page
                .entries
                .iter()
                .filter(|e| e.by == p)
                .filter_map(|e| match &e.event {
                    Event::Room(RoomEvent::Posted { body, .. }) => Some(body.as_str()),
                    _ => None,
                })
                .collect()
        };
        assert_eq!(
            bodies_for(&role_live),
            vec!["Earlier completed reply"],
            "scenario={scenario}"
        );
        if scenario == "two-turn-retry" {
            assert_eq!(
                bodies_for(&role_cursor),
                vec!["Earlier completed reply", "Clean third reply"],
                "scenario={scenario}"
            );
        } else {
            assert_eq!(
                bodies_for(&role_cursor),
                vec!["Earlier completed reply"],
                "scenario={scenario}"
            );
        }
        if scenario == "second-failure" {
            assert_eq!(
                bodies_for(&role_same_idle),
                vec!["Earlier completed reply"],
                "scenario={scenario}"
            );
        } else {
            assert_eq!(
                bodies_for(&role_same_idle),
                vec!["Earlier completed reply", "Recovered third reply"],
                "scenario={scenario}"
            );
        }
        assert_eq!(
            bodies_for(&role_cold),
            vec!["Clean cold reply"],
            "scenario={scenario}"
        );

        let live_errored = snap
            .workers
            .iter()
            .find(|(r, _, _)| r == &role_live)
            .is_some_and(|(_, _, phase)| phase.error().is_some());
        assert!(live_errored, "scenario={scenario}");

        if scenario == "two-turn-retry" {
            let cursor_recovered = snap
                .workers
                .iter()
                .find(|(r, _, _)| r == &role_cursor)
                .is_some_and(|(_, _, phase)| {
                    matches!(
                        phase,
                        WorkerPhase::Live {
                            activity: Activity::Idle,
                            ..
                        }
                    )
                });
            assert!(cursor_recovered, "scenario={scenario}");
        } else {
            let cursor_errored = snap
                .workers
                .iter()
                .find(|(r, _, _)| r == &role_cursor)
                .is_some_and(|(_, _, phase)| phase.error().is_some());
            assert!(cursor_errored, "scenario={scenario}");
        }

        if scenario == "second-failure" {
            let same_idle_err = snap
                .workers
                .iter()
                .find(|(r, _, _)| r == &role_same_idle)
                .and_then(|(_, _, phase)| phase.error());
            assert_eq!(same_idle_err, Some("second failure"), "scenario={scenario}");
        } else {
            let same_idle_recovered = snap
                .workers
                .iter()
                .find(|(r, _, _)| r == &role_same_idle)
                .is_some_and(|(_, _, phase)| {
                    matches!(
                        phase,
                        WorkerPhase::Live {
                            activity: Activity::Idle,
                            ..
                        }
                    )
                });
            assert!(same_idle_recovered, "scenario={scenario}");
        }

        let _ = shutdown_tx.send(true);
        let _ = bridge_runner.await;
        server_task.abort();
    }

    #[tokio::test]
    async fn bb_suppression_write_and_hydration_read_failures_never_leak_suppressed_turns() {
        for scenario in [
            "concurrent-join",
            "hello-first",
            "failed-first",
            "spawn-only",
            "refusal",
            "stale-retry",
            "two-turn-retry",
            "second-failure",
            "r28_control",
            "r28_pending_write",
            "r28_successor_failed",
            "r28_duplicate_failure",
            "r28_two_start_markers",
            "r28_restart",
            "r28_restart_write_fault",
            "r28_post_fetch_guard",
            "r28_idle_queued",
            "r28_successful_stale_read",
        ] {
            run_bb_suppression_fault_matrix_case(scenario).await;
        }
    }
}
