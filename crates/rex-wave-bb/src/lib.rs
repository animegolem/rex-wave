//! BB bridge adapter (`BbBridge` / `BbAdapter` and `BbHost`): `AgentHost`,
//! `AgentFeed`, and worker tool execution over the private `/bridge` WebSocket.

mod ws;

use rex_wave_api::read::{
    AgentEvent, AgentEventKind, ContextCategory, ContextCategoryKind, ThreadContextUsage, ThreadPageView,
};
use rex_wave_api::room::{Body, RoomId};
use rex_wave_api::wave::{
    Activity, BaseBranch, InteractionKind, InteractionResolution, Observation, PendingInteraction, PermissionMode,
    Question, SteerDeliveryMode, WaveEvent, WaveObserve, WaveView, WorkerPhase, Workspace,
};
use rex_wave_api::*;
use rex_wave_runtime::{AgentFeed, BoxFut, LocalClient, Reactor, Router, frame_channel, spawn_reactor};
use rex_wave_waves::{AgentHost, HostError, SpawnRequest};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

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
            limit: u32,
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
                Down::Send { thread_id, mode, text, .. } => Down::Send { req_id: id, thread_id, mode, text },
                Down::Resolve { thread_id, interaction_id, resolution, .. } => {
                    Down::Resolve { req_id: id, thread_id, interaction_id, resolution }
                }
                Down::Retire { thread_id, .. } => Down::Retire { req_id: id, thread_id },
                Down::EventsPage { thread_id, after_seq, limit, .. } => {
                    Down::EventsPage { req_id: id, thread_id, after_seq, limit }
                }
                Down::ThreadContext { thread_id, .. } => Down::ThreadContext { req_id: id, thread_id },
                other => other,
            }
        }
    }
}

pub fn spawn_args(req: &SpawnRequest) -> serde_json::Value {
    let environment = match &req.agent.workspace {
        Workspace::ManagedWorktree { base } => {
            let branch = match base {
                BaseBranch::Default => serde_json::json!({ "kind": "default" }),
                BaseBranch::Named { name } => serde_json::json!({ "kind": "named", "name": name }),
            };
            serde_json::json!({
                "type": "provider",
                "environmentProviderId": "git-worktree",
                "inputs": { "branch": branch },
            })
        }
        Workspace::ExistingPath { path } => serde_json::json!({
            "type": "provider",
            "environmentProviderId": "git-worktree",
            "inputs": { "kind": "existing", "path": path.to_string_lossy() },
        }),
        Workspace::ProjectDefault => serde_json::json!({
            "type": "provider",
            "environmentProviderId": "project-checkout",
            "inputs": {},
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
    };

    let mut obj = serde_json::Map::new();
    obj.insert("projectId".into(), serde_json::Value::String(req.project.to_string()));
    obj.insert("environment".into(), environment);
    obj.insert("providerId".into(), serde_json::Value::String(req.agent.provider.to_string()));
    if let Some(model) = &req.agent.model {
        obj.insert("model".into(), serde_json::Value::String(model.to_string()));
    }
    if let Some(reasoning) = &req.agent.reasoning {
        obj.insert("reasoningLevel".into(), serde_json::Value::String(reasoning.clone()));
    }
    if let Some(tier) = &req.agent.service_tier {
        obj.insert("serviceTier".into(), serde_json::Value::String(tier.clone()));
    }
    obj.insert("permissionMode".into(), serde_json::Value::String(permission.into()));
    obj.insert("visibility".into(), serde_json::Value::String("hidden".into()));
    obj.insert("title".into(), serde_json::Value::String(req.title.clone()));
    obj.insert("prompt".into(), serde_json::Value::String(req.prompt.as_str().to_owned()));
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
        InteractionResolution::AllowOnce { granted_permissions } => serde_json::json!({
            "decision": "allow_once",
            "grantedPermissions": grants_json(granted_permissions),
        }),
        InteractionResolution::AllowForSession { granted_permissions } => serde_json::json!({
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
        InteractionResolution::PluginSubmitted { title, detail, payload } => {
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
    let kind_str = payload.and_then(|p| p.get("kind")).and_then(|k| k.as_str()).unwrap_or("extension");

    let (kind, summary) = match kind_str {
        "approval" => {
            let reason = payload.and_then(|p| p.get("reason")).and_then(|r| r.as_str());
            let cmd = payload
                .and_then(|p| p.get("subject"))
                .and_then(|s| s.get("command").or_else(|| s.get("tool")).or_else(|| s.get("title")))
                .and_then(|v| v.as_str());
            let summary = reason.or(cmd).unwrap_or("Permission approval requested").to_owned();
            (InteractionKind::Permission, summary)
        }
        "user_question" => {
            let mut questions = Vec::new();
            if let Some(arr) = payload.and_then(|p| p.get("questions")).and_then(|q| q.as_array()) {
                for q in arr {
                    let Some(qid) = q.get("id").and_then(|v| v.as_str()) else { continue };
                    let prompt = q.get("prompt").and_then(|v| v.as_str()).unwrap_or("").to_owned();
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
                            } else if let Some(lbl) = o.get("label").or_else(|| o.get("value")).and_then(|v| v.as_str()) {
                                options.push(lbl.to_owned());
                            }
                        }
                    }
                    questions.push(Question { id: qid.to_owned(), prompt, options, multi });
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
        let model_context_window = raw.get("model_context_window").and_then(|v| v.as_u64()).unwrap_or(0);
        let estimated = raw.get("estimated").and_then(|v| v.as_bool()).unwrap_or(false);
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
    let model_context_window = usage.get("modelContextWindow").and_then(|v| v.as_u64()).unwrap_or(0);
    let estimated = usage.get("estimated").and_then(|v| v.as_bool()).unwrap_or(false);
    let snapshot = usage.get("snapshot");
    let auto_compact_at_tokens = snapshot.and_then(|s| s.get("autoCompactAtTokens")).and_then(|v| v.as_u64());
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
        let Some(id) = item.get("id").and_then(|v| v.as_str()) else { continue };
        let label = item.get("label").and_then(|v| v.as_str()).unwrap_or(id);
        let tokens = item.get("tokens").and_then(|v| v.as_u64()).unwrap_or(0);
        let kind = match item.get("kind").and_then(|v| v.as_str()) {
            Some("reserved") => ContextCategoryKind::Reserved,
            Some("deferred") => ContextCategoryKind::Deferred,
            Some("free") => ContextCategoryKind::Free,
            _ => ContextCategoryKind::Used,
        };
        out.push(ContextCategory { id: id.to_owned(), label: label.to_owned(), kind, tokens });
    }
    out
}

pub fn normalize_agent_event(row: &serde_json::Value) -> Option<AgentEvent> {
    let seq = BbSeq(row.get("seq")?.as_u64()?);
    let at = UnixMs(row.get("createdAt").and_then(|v| v.as_u64()).unwrap_or(0));
    let ty = row.get("type").and_then(|v| v.as_str()).unwrap_or("unknown");
    let data = row.get("data").unwrap_or(&serde_json::Value::Null);

    let kind = match ty {
        "turn/started" | "client/turn/start" => AgentEventKind::TurnStarted,
        "turn/completed" => AgentEventKind::TurnEnded,
        "system/manager/user_message" => {
            let text = data.get("text").and_then(|v| v.as_str()).unwrap_or("").to_owned();
            AgentEventKind::UserMessage { text }
        }
        "item/agentMessage/delta" => {
            let text = data.get("delta").and_then(|v| v.as_str()).unwrap_or("").to_owned();
            AgentEventKind::AssistantText { text }
        }
        "item/started" | "item/completed" => {
            let completed = ty == "item/completed";
            let item = data.get("item").unwrap_or(&serde_json::Value::Null);
            let item_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("unknown");
            match item_type {
                "userMessage" => {
                    let text = extract_user_content(item.get("content"));
                    AgentEventKind::UserMessage { text }
                }
                "agentMessage" => {
                    let text = item.get("text").and_then(|v| v.as_str()).unwrap_or("").to_owned();
                    AgentEventKind::AssistantText { text }
                }
                "commandExecution" | "toolCall" | "fileChange" | "fileRead" | "search" | "webSearch" | "webFetch" => {
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
                        let status = item.get("status").and_then(|v| v.as_str()).unwrap_or("completed");
                        AgentEventKind::ToolResult { name, ok: status == "completed", summary }
                    } else {
                        AgentEventKind::ToolCall { name, summary }
                    }
                }
                _ => AgentEventKind::Other { bb_type: format!("{ty}:{item_type}") },
            }
        }
        "thread/contextWindowUsage/updated" => match parse_context_usage(data) {
            Some(usage) => AgentEventKind::Usage { usage },
            None => AgentEventKind::Other { bb_type: ty.to_owned() },
        },
        _ => AgentEventKind::Other { bb_type: ty.to_owned() },
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
}

enum OwnerMsg {
    Connected {
        tx: mpsc::UnboundedSender<protocol::Down>,
    },
    Disconnected {
        conn_id: u64,
    },
    WaitConnected {
        reply: oneshot::Sender<()>,
    },
    Rpc {
        down: protocol::Down,
        reply: oneshot::Sender<Result<serde_json::Value, HostError>>,
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
        reply: oneshot::Sender<Vec<(WaveSlug, RoleName, BbThreadId, Option<u64>)>>,
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
    fn spawn() -> (mpsc::UnboundedSender<OwnerMsg>, Arc<std::sync::atomic::AtomicU64>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let active_conn = Arc::new(std::sync::atomic::AtomicU64::new(0));
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
        tokio::spawn(owner.run(rx, active_conn.clone()));
        (tx, active_conn)
    }

    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<OwnerMsg>, active_conn: Arc<std::sync::atomic::AtomicU64>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                OwnerMsg::Connected { tx } => {
                    self.conn_id += 1;
                    active_conn.store(self.conn_id, std::sync::atomic::Ordering::SeqCst);
                    for (thread, subs) in &mut self.watchers {
                        subs.retain(|(_, s)| !s.is_closed());
                        if !subs.is_empty() {
                            let after_seq = self
                                .role_by_thread
                                .get(thread)
                                .and_then(|k| self.workers_by_role.get(k))
                                .and_then(|w| w.last_seq);
                            let _ = tx.send(protocol::Down::WatchThread { thread_id: thread.to_string(), after_seq });
                        }
                    }
                    self.watchers.retain(|_, subs| !subs.is_empty());
                    self.tx_down = Some(tx);
                    for w in self.conn_waiters.drain(..) {
                        let _ = w.send(());
                    }
                }
                OwnerMsg::Disconnected { conn_id } => {
                    if self.conn_id == conn_id {
                        self.tx_down = None;
                        for (_, waiter) in self.pending.drain() {
                            let _ = waiter.send(Err(HostError::Unreachable("BB bridge disconnected".into())));
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
                OwnerMsg::Rpc { down, reply } => {
                    let Some(tx) = &self.tx_down else {
                        let _ = reply.send(Err(HostError::Unreachable("BB bridge is not connected".into())));
                        continue;
                    };
                    self.next_req_id += 1;
                    let id = self.next_req_id;
                    let tagged = down.with_req_id(id);
                    if tx.send(tagged).is_err() {
                        self.tx_down = None;
                        let _ = reply.send(Err(HostError::Unreachable("BB bridge write channel closed".into())));
                    } else {
                        self.pending.insert(id, reply);
                    }
                }
                OwnerMsg::Fire { down, reply } => {
                    let res = match &self.tx_down {
                        Some(tx) => tx
                            .send(down)
                            .map_err(|_| HostError::Unreachable("BB bridge write channel closed".into())),
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
                OwnerMsg::BindThread { wave, role, attempt, thread } => {
                    self.role_by_thread.insert(thread.clone(), (wave.clone(), role.clone()));
                    let entry = self.workers_by_role.entry((wave, role)).or_insert(TrackedWorker {
                        attempt,
                        thread: Some(thread.clone()),
                        live: true,
                        activity: Activity::Working,
                        pending_interaction: None,
                        last_seq: None,
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
                            list.push((w.clone(), r.clone(), th.clone(), rec.last_seq));
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
                                let _ = tx.send(protocol::Down::UnwatchThread { thread_id: thread.to_string() });
                            }
                        }
                    }
                }
                OwnerMsg::WatchOpen { thread, after_seq, reply } => {
                    self.next_lease_id += 1;
                    let lease_id = self.next_lease_id;
                    let (seq_tx, seq_rx) = mpsc::unbounded_channel();
                    let entry = self.watchers.entry(thread.clone()).or_default();
                    let was_empty = entry.is_empty();
                    entry.push((lease_id, seq_tx));
                    if was_empty
                        && let Some(tx) = &self.tx_down
                    {
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
                                let _ = tx.send(protocol::Down::UnwatchThread { thread_id: thread.to_string() });
                            }
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
                self.workers_by_role.insert(
                    (wave, role),
                    TrackedWorker {
                        attempt,
                        thread: None,
                        live: false,
                        activity: Activity::Working,
                        pending_interaction: None,
                        last_seq: None,
                    },
                );
            }
            WaveEvent::WorkerObserved { role, observation } => match observation {
                Observation::Spawned { attempt, thread } => {
                    self.role_by_thread.insert(thread.clone(), (wave.clone(), role.clone()));
                    let rec = self.workers_by_role.entry((wave, role)).or_insert(TrackedWorker {
                        attempt,
                        thread: Some(thread.clone()),
                        live: true,
                        activity: Activity::Working,
                        pending_interaction: None,
                        last_seq: None,
                    });
                    rec.attempt = attempt;
                    rec.thread = Some(thread);
                    rec.live = true;
                    rec.activity = Activity::Working;
                    rec.pending_interaction = None;
                }
                Observation::Activity { activity } => {
                    if let Some(rec) = self.workers_by_role.get_mut(&(wave, role)) {
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

pub struct AgentSubscription {
    rx: mpsc::UnboundedReceiver<AgentEvent>,
    _lease: WatchLeaseGuard,
}

impl AgentSubscription {
    pub async fn recv(&mut self) -> Option<AgentEvent> {
        self.rx.recv().await
    }
}

struct BridgeWaveWatcher {
    owner: mpsc::UnboundedSender<OwnerMsg>,
}

impl Reactor for BridgeWaveWatcher {
    const NAME: &'static str = "bb-bridge-wave-index";

    fn interest(&self) -> Interest {
        Interest::Everything
    }

    async fn react(&mut self, frame: Frame, _client: &LocalClient) -> Result<(), ApiError> {
        if let Frame::Committed(Envelope { stream: StreamId::Wave { wave }, event: Event::Wave(ev), .. }) = frame {
            let _ = self.owner.send(OwnerMsg::FoldWave { wave, event: ev });
        }
        Ok(())
    }
}

pub struct BbBridge {
    cfg: BridgeConfig,
    owner: mpsc::UnboundedSender<OwnerMsg>,
    active_conn: Arc<std::sync::atomic::AtomicU64>,
    backend: OnceLock<Arc<dyn ErasedBackend>>,
}

pub type BbAdapter = BbBridge;

impl BbBridge {
    pub fn new(cfg: BridgeConfig) -> Arc<Self> {
        let (owner, active_conn) = BridgeOwner::spawn();
        Arc::new(BbBridge {
            cfg,
            owner,
            active_conn,
            backend: OnceLock::new(),
        })
    }

    pub fn observe_wave_event(&self, wave: WaveSlug, event: WaveEvent) {
        let _ = self.owner.send(OwnerMsg::FoldWave { wave, event });
    }

    pub async fn run(self: Arc<Self>, router: Arc<Router>) {
        let sys = LocalClient::new(router.clone(), Principal::System { part: SystemPart::BbBridge });
        let _watcher = spawn_reactor(BridgeWaveWatcher { owner: self.owner.clone() }, sys);
        let (_tx, rx) = watch::channel(false);
        self.run_until(router, rx).await;
    }

    pub async fn run_until<B: BridgeBackend>(self: Arc<Self>, backend: B, mut shutdown: watch::Receiver<bool>) {
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
        mut ws_conn: ws::WsConn<tokio::net::TcpStream>,
        shutdown: &mut watch::Receiver<bool>,
    ) {
        let (down_tx, mut down_rx) = mpsc::unbounded_channel::<protocol::Down>();
        let _ = self.owner.send(OwnerMsg::Connected { tx: down_tx });

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
                    if ws_conn.send_text(&payload).await.is_err() {
                        break;
                    }
                }
                inbound = ws_conn.recv_text() => match inbound {
                    Ok(Some(raw)) => {
                        if let Ok(up) = serde_json::from_str::<protocol::Up>(&raw) {
                            self.handle_up(up);
                        }
                    }
                    Ok(None) | Err(_) => break,
                }
            }
        }

        let conn_id = self.active_conn.load(std::sync::atomic::Ordering::SeqCst);
        let _ = self.owner.send(OwnerMsg::Disconnected { conn_id });
    }

    fn handle_up(self: &Arc<Self>, up: protocol::Up) {
        match up {
            protocol::Up::Hello { .. } => {
                let bridge = self.clone();
                tokio::spawn(async move {
                    bridge.reconcile_live_workers().await;
                });
            }
            protocol::Up::Result { req_id, ok, value } => {
                let _ = self.owner.send(OwnerMsg::Result { req_id, ok, value });
            }
            protocol::Up::ThreadEvents { thread_id, sequence } => {
                if let Ok(thread) = BbThreadId::parse(thread_id) {
                    let _ = self.owner.send(OwnerMsg::ThreadEvents { thread, sequence });
                }
            }
            protocol::Up::ToolCall { call_id, thread_id, meta, tool, args, .. } => {
                let bridge = self.clone();
                tokio::spawn(async move {
                    bridge.on_tool_call(call_id, thread_id, meta, tool, args).await;
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
        tokio::time::timeout(Duration::from_secs(3), rx)
            .await
            .map_err(|_| HostError::Unreachable("timed out waiting for BB bridge connection".into()))?
            .map_err(|_| HostError::Unreachable("bridge stopped".into()))
    }

    async fn rpc(&self, down: protocol::Down) -> Result<serde_json::Value, HostError> {
        self.ensure_connected().await?;
        let (tx, rx) = oneshot::channel();
        self.owner
            .send(OwnerMsg::Rpc { down, reply: tx })
            .map_err(|_| HostError::Unreachable("bridge stopped".into()))?;
        rx.await.map_err(|_| HostError::Unreachable("bridge dropped RPC".into()))?
    }

    async fn fire(&self, down: protocol::Down) -> Result<(), HostError> {
        self.ensure_connected().await?;
        let (tx, rx) = oneshot::channel();
        self.owner
            .send(OwnerMsg::Fire { down, reply: tx })
            .map_err(|_| HostError::Unreachable("bridge stopped".into()))?;
        rx.await.map_err(|_| HostError::Unreachable("bridge dropped message".into()))?
    }

    pub async fn push_notification(&self, title: impl Into<String>, body: impl Into<String>) -> Result<(), HostError> {
        self.fire(protocol::Down::Push { title: title.into(), body: body.into() }).await
    }

    async fn resolve_worker_identity(
        &self,
        thread: &BbThreadId,
        meta: Option<&protocol::WaveMeta>,
    ) -> Option<(WaveSlug, RoleName, u32)> {
        let (tx, rx) = oneshot::channel();
        let _ = self.owner.send(OwnerMsg::LookupByThread { thread: thread.clone(), reply: tx });
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
                    Principal::System { part: SystemPart::BbBridge },
                    WaveView { wave: wave.clone() }.into_wire(),
                )
                .await
        {
            let valid = snap.workers.iter().any(|(r, _, phase)| {
                r == &role
                    && match phase {
                        WorkerPhase::Live { thread: t, .. } | WorkerPhase::Retiring { thread: t } => t == thread,
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
        self.observe_wave_event(wave.clone(), WaveEvent::WorkerObserved { role: role.clone(), observation: observation.clone() });
        if let Some(backend) = self.backend.get() {
            let _ = backend
                .call_as(
                    Principal::System { part: SystemPart::BbBridge },
                    WaveObserve { wave, role, observation }.into_wire(),
                )
                .await;
        }
    }

    async fn on_thread_signal(&self, up: protocol::Up) {
        match up {
            protocol::Up::Thread { signal, thread_id, meta, error, .. } => {
                let Ok(thread) = BbThreadId::parse(thread_id) else { return };
                let Some((wave, role, attempt)) = self.resolve_worker_identity(&thread, meta.as_ref()).await else {
                    return;
                };
                match signal {
                    protocol::ThreadSignal::Created => {
                        self.call_wave_observe(wave, role, Observation::Spawned { attempt, thread }).await;
                    }
                    protocol::ThreadSignal::Active => {
                        self.call_wave_observe(wave.clone(), role.clone(), Observation::Spawned { attempt, thread }).await;
                        self.call_wave_observe(wave, role, Observation::Activity { activity: Activity::Working }).await;
                    }
                    protocol::ThreadSignal::Idle => {
                        self.call_wave_observe(wave.clone(), role.clone(), Observation::Spawned { attempt, thread }).await;
                        self.call_wave_observe(wave, role, Observation::Activity { activity: Activity::Idle }).await;
                    }
                    protocol::ThreadSignal::Failed => {
                        let msg = error.unwrap_or_else(|| "BB thread failed".into());
                        self.call_wave_observe(wave.clone(), role.clone(), Observation::Spawned { attempt, thread }).await;
                        self.call_wave_observe(
                            wave,
                            role,
                            Observation::Activity { activity: Activity::Errored { error: msg } },
                        )
                        .await;
                    }
                    protocol::ThreadSignal::Archived => {
                        self.call_wave_observe(wave, role, Observation::Archived).await;
                    }
                }
            }
            protocol::Up::Interaction { thread_id, meta, interaction } => {
                let Ok(thread) = BbThreadId::parse(thread_id) else { return };
                let Some(pending) = parse_pending_interaction(&interaction) else { return };
                let Some((wave, role, attempt)) = self.resolve_worker_identity(&thread, meta.as_ref()).await else {
                    return;
                };
                self.call_wave_observe(wave.clone(), role.clone(), Observation::Spawned { attempt, thread }).await;
                self.call_wave_observe(
                    wave,
                    role,
                    Observation::Activity { activity: Activity::Blocked { interaction: pending } },
                )
                .await;
            }
            _ => {}
        }
    }

    async fn on_tool_call(
        &self,
        call_id: String,
        thread_raw: String,
        meta: Option<protocol::WaveMeta>,
        tool: String,
        args: serde_json::Value,
    ) {
        let send_reply = |ok: bool, text: String| async move {
            let _ = self.fire(protocol::Down::ToolResult { call_id, ok, text }).await;
        };

        let Ok(thread) = BbThreadId::parse(thread_raw) else {
            send_reply(false, "invalid BB thread id".into()).await;
            return;
        };
        let Some((wave, role, _)) = self.resolve_worker_identity(&thread, meta.as_ref()).await else {
            send_reply(false, format!("unrecognized wave worker thread {thread}")).await;
            return;
        };
        let Some(backend) = self.backend.get().cloned() else {
            send_reply(false, "rex-wave router unavailable".into()).await;
            return;
        };

        let wire_json = serde_json::json!({ "cmd": tool, "args": args });
        let cmd: WireCommand = match serde_json::from_value(wire_json) {
            Ok(c) => c,
            Err(e) => {
                send_reply(false, format!("invalid arguments for {tool}: {e}")).await;
                return;
            }
        };

        let worker_principal = Principal::Worker { wave: wave.clone(), role: role.clone() };
        let parked_scope = match &cmd {
            WireCommand::Wait(w) => Some(parked_room_and_since(&worker_principal, w)),
            _ => None,
        };

        if let Some((room, since)) = &parked_scope {
            self.call_wave_observe(
                wave.clone(),
                role.clone(),
                Observation::Activity {
                    activity: Activity::Parked { room: room.clone(), since: *since },
                },
            )
            .await;
        }

        let outcome = backend.call_as(worker_principal, cmd).await;

        if parked_scope.is_some()
            && let Ok(rec) = self.lookup_worker(&wave, &role).await
            && matches!(rec.activity, Activity::Parked { .. })
        {
            self.call_wave_observe(wave, role, Observation::Activity { activity: Activity::Working }).await;
        }

        match outcome {
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

    async fn reconcile_live_workers(&self) {
        let (tx, rx) = oneshot::channel();
        if self.owner.send(OwnerMsg::ListLiveWorkers { reply: tx }).is_err() {
            return;
        }
        let Ok(workers) = rx.await else { return };
        for (wave, role, thread, after_seq) in workers {
            let Ok(rows_val) = self
                .rpc(protocol::Down::EventsPage {
                    req_id: 0,
                    thread_id: thread.to_string(),
                    after_seq,
                    limit: 200,
                })
                .await
            else {
                continue;
            };
            let Some(rows) = rows_val.as_array() else { continue };
            let mut max_seq = after_seq;
            let mut derived_activity: Option<Activity> = None;
            for row in rows {
                if let Some(s) = row.get("seq").and_then(|v| v.as_u64()) {
                    max_seq = Some(max_seq.map_or(s, |prev| prev.max(s)));
                }
                match row.get("type").and_then(|v| v.as_str()) {
                    Some("turn/started" | "client/turn/start") => derived_activity = Some(Activity::Working),
                    Some("turn/completed") => derived_activity = Some(Activity::Idle),
                    Some("system/error" | "provider/error") => {
                        let msg = row
                            .get("data")
                            .and_then(|d| d.get("message"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("BB error")
                            .to_owned();
                        derived_activity = Some(Activity::Errored { error: msg });
                    }
                    _ => {}
                }
            }
            if let Some(seq) = max_seq {
                let _ = self.owner.send(OwnerMsg::AdvanceSeq { thread: thread.clone(), seq });
            }
            if let Some(activity) = derived_activity {
                self.call_wave_observe(wave, role, Observation::Activity { activity }).await;
            }
        }
    }

    async fn lookup_worker(&self, wave: &WaveSlug, role: &RoleName) -> Result<TrackedWorker, ApiError> {
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
                    Principal::System { part: SystemPart::BbBridge },
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
                            })
                        }
                        WorkerPhase::Retiring { thread } | WorkerPhase::Retired { thread: Some(thread) } => Ok(TrackedWorker {
                            attempt: 1,
                            thread: Some(thread),
                            live: false,
                            activity: Activity::Idle,
                            pending_interaction: None,
                            last_seq: None,
                        }),
                        _ => Err(ApiError::NotFound { what: format!("live BB thread for @{wave}/{role}") }),
                    };
                }
            }
        }

        Err(ApiError::NotFound { what: format!("worker @{wave}/{role}") })
    }

    pub async fn subscribe_agent(self: &Arc<Self>, wave: &WaveSlug, role: &RoleName) -> Result<AgentSubscription, ApiError> {
        let rec = self.lookup_worker(wave, role).await?;
        let thread = rec.thread.ok_or_else(|| ApiError::NotFound { what: format!("thread for @{wave}/{role}") })?;
        let initial_seq = rec.last_seq;

        let (tx_open, rx_open) = oneshot::channel();
        self.owner
            .send(OwnerMsg::WatchOpen {
                thread: thread.clone(),
                after_seq: initial_seq,
                reply: tx_open,
            })
            .map_err(|_| ApiError::Internal("BB bridge stopped".into()))?;
        let (lease_id, mut seq_rx) = rx_open.await.map_err(|_| ApiError::Internal("BB bridge dropped watch".into()))?;

        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        let bridge = self.clone();
        let watched_thread = thread.clone();

        tokio::spawn(async move {
            let mut cursor_seq = initial_seq;
            loop {
                tokio::select! {
                    biased;
                    () = ev_tx.closed() => break,
                    maybe_seq = seq_rx.recv() => {
                        let Some(target_seq) = maybe_seq else { break };
                        while cursor_seq.is_none_or(|c| c < target_seq) {
                            let Ok(rows_val) = bridge.rpc(protocol::Down::EventsPage {
                                req_id: 0,
                                thread_id: watched_thread.to_string(),
                                after_seq: cursor_seq,
                                limit: 200,
                            }).await else {
                                break;
                            };
                            let Some(rows) = rows_val.as_array() else { break };
                            if rows.is_empty() {
                                cursor_seq = Some(target_seq);
                                break;
                            }
                            for row in rows {
                                if let Some(ev) = normalize_agent_event(row) {
                                    cursor_seq = Some(cursor_seq.map_or(ev.seq.0, |prev| prev.max(ev.seq.0)));
                                    let _ = bridge.owner.send(OwnerMsg::AdvanceSeq { thread: watched_thread.clone(), seq: ev.seq.0 });
                                    if ev_tx.send(ev).is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(AgentSubscription {
            rx: ev_rx,
            _lease: WatchLeaseGuard {
                owner: self.owner.clone(),
                thread,
                lease_id,
            },
        })
    }

    async fn spawn_worker_thread(&self, req: SpawnRequest) -> Result<BbThreadId, HostError> {
        let meta = protocol::WaveMeta {
            wave_id: req.wave.to_string(),
            role: req.role.to_string(),
            attempt: req.attempt,
            room_id: serde_json::to_string(&req.room).unwrap_or_default(),
        };

        let existing = self.rpc(protocol::Down::FindByMeta { req_id: 0, meta }).await?;
        if let Some(id_str) = existing.get("thread_id").and_then(|v| v.as_str()) {
            let thread = BbThreadId::parse(id_str).map_err(|e| HostError::Rejected(e.to_string()))?;
            let _ = self.owner.send(OwnerMsg::BindThread {
                wave: req.wave,
                role: req.role,
                attempt: req.attempt,
                thread: thread.clone(),
            });
            return Ok(thread);
        }

        let created = self.rpc(protocol::Down::Spawn { req_id: 0, args: spawn_args(&req) }).await?;
        let id_str = created
            .get("thread_id")
            .or_else(|| created.get("id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| HostError::Rejected(format!("missing thread_id in spawn reply: {created}")))?;
        let thread = BbThreadId::parse(id_str).map_err(|e| HostError::Rejected(e.to_string()))?;
        let _ = self.owner.send(OwnerMsg::BindThread {
            wave: req.wave,
            role: req.role,
            attempt: req.attempt,
            thread: thread.clone(),
        });
        Ok(thread)
    }
}

fn parked_room_and_since(worker: &Principal, w: &rex_wave_api::read::Wait) -> (Option<RoomId>, Seq) {
    match &w.interest {
        Interest::Room { room } => match room.resolve(worker) {
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
    fn spawn(&self, req: SpawnRequest) -> impl Future<Output = Result<BbThreadId, HostError>> + Send {
        let bridge = self.0.clone();
        async move { bridge.spawn_worker_thread(req).await }
    }

    fn tell(&self, thread: &BbThreadId, body: &Body, mode: SteerDeliveryMode) -> impl Future<Output = Result<(), HostError>> + Send {
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
        let down = protocol::Down::Retire { req_id: 0, thread_id: thread.to_string() };
        async move { bridge.rpc(down).await.map(|_| ()) }
    }
}

impl AgentHost for BbBridge {
    fn spawn(&self, req: SpawnRequest) -> impl Future<Output = Result<BbThreadId, HostError>> + Send {
        async move { self.spawn_worker_thread(req).await }
    }

    fn tell(&self, thread: &BbThreadId, body: &Body, mode: SteerDeliveryMode) -> impl Future<Output = Result<(), HostError>> + Send {
        let down = protocol::Down::Send {
            req_id: 0,
            thread_id: thread.to_string(),
            mode: send_mode(mode).into(),
            text: body.as_str().into(),
        };
        async move { self.rpc(down).await.map(|_| ()) }
    }

    fn resolve(
        &self,
        thread: &BbThreadId,
        interaction: &BbInteractionId,
        resolution: &InteractionResolution,
    ) -> impl Future<Output = Result<(), HostError>> + Send {
        let down = protocol::Down::Resolve {
            req_id: 0,
            thread_id: thread.to_string(),
            interaction_id: interaction.to_string(),
            resolution: resolution_json(resolution),
        };
        async move { self.rpc(down).await.map(|_| ()) }
    }

    fn retire(&self, thread: &BbThreadId) -> impl Future<Output = Result<(), HostError>> + Send {
        let down = protocol::Down::Retire { req_id: 0, thread_id: thread.to_string() };
        async move { self.rpc(down).await.map(|_| ()) }
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
            let thread = rec.thread.ok_or_else(|| ApiError::NotFound { what: format!("thread for @{wave}/{role}") })?;

            let rows_val = self
                .rpc(protocol::Down::EventsPage {
                    req_id: 0,
                    thread_id: thread.to_string(),
                    after_seq: after.map(|s| s.0),
                    limit,
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
                let _ = self.owner.send(OwnerMsg::AdvanceSeq { thread: thread.clone(), seq: last.seq.0 });
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
            let rec = self.lookup_worker(&wave, &role).await?;
            let thread = rec.thread.ok_or_else(|| ApiError::NotFound { what: format!("thread for @{wave}/{role}") })?;
            let initial_seq = rec.last_seq;

            let (tx_open, rx_open) = oneshot::channel();
            self.owner
                .send(OwnerMsg::WatchOpen {
                    thread: thread.clone(),
                    after_seq: initial_seq,
                    reply: tx_open,
                })
                .map_err(|_| ApiError::Internal("BB bridge stopped".into()))?;
            let (lease_id, mut seq_rx) = rx_open.await.map_err(|_| ApiError::Internal("BB bridge dropped watch".into()))?;
            let lease = WatchLeaseGuard {
                owner: self.owner.clone(),
                thread: thread.clone(),
                lease_id,
            };

            let (frame_tx, stream) = frame_channel(128);
            let owner = self.owner.clone();

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
                                        limit: 200,
                                    },
                                    reply: rpc_tx,
                                }).is_err() {
                                    return;
                                }
                                let Ok(Ok(rows_val)) = rpc_rx.await else { break };
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

            Ok(stream)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rex_wave_api::board::{BoardEnsure, BoardEvent, BoardId, BoardInfo};
    use rex_wave_api::read::{Page, ThreadPage, Wait};
    use rex_wave_api::room::{Mentions, Posted, RoomEvent, RoomInfo, RoomOrigin, RoomPost, RoomRef};
    use rex_wave_api::wave::{
        AgentSpec, FileSystemGrant, GrantedPermissions, NetworkGrant, UserAnswer, WaveFinish, WaveOutcome, WaveResolve,
        WaveSpec, WaveStart, WorkerSpec,
    };
    use rex_wave_runtime::{DecideCtx, Decider, EntryMeta, authorize};
    use rex_wave_waves::{WaveConductor, WaveState, Waves};
    use std::collections::{BTreeMap, BTreeSet};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    #[test]
    fn rfc6455_accept_vector_and_protocol_fixtures_round_trip() {
        assert_eq!(ws::ws_accept("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");

        let raw = include_str!("../../../bb-plugin/protocol.fixtures.json");
        let doc: serde_json::Value = serde_json::from_str(raw).unwrap();
        let ups: Vec<protocol::Up> = serde_json::from_value(doc["up"].clone()).unwrap();
        let downs: Vec<protocol::Down> = serde_json::from_value(doc["down"].clone()).unwrap();
        assert_eq!(ups.len(), 6);
        assert_eq!(downs.len(), 11);
        assert_eq!(serde_json::to_value(&ups).unwrap(), doc["up"]);
        assert_eq!(serde_json::to_value(&downs).unwrap(), doc["down"]);
    }

    #[test]
    fn spawn_args_and_resolutions_match_bb_sdk_0_6_132_shapes() {
        let room = rex_wave_waves::wave_room(&WaveSlug::parse("w1").unwrap(), &BbProjectId::parse("proj_1").unwrap());
        let mk_req = |provider: &str, model: Option<&str>, permission: PermissionMode, workspace: Workspace| SpawnRequest {
            wave: WaveSlug::parse("w1").unwrap(),
            role: RoleName::parse("worker").unwrap(),
            attempt: 2,
            project: BbProjectId::parse("proj_1").unwrap(),
            agent: AgentSpec {
                provider: ProviderId::parse(provider).unwrap(),
                model: model.map(|m| ModelId::parse(m).unwrap()),
                reasoning: Some("xhigh".into()),
                service_tier: Some("fast".into()),
                permission,
                workspace,
            },
            room: room.clone(),
            prompt: Body::parse("ship it").unwrap(),
            title: "[w1] @worker".into(),
        };

        let wt = spawn_args(&mk_req(
            "codex",
            Some("gpt-6.1-sol"),
            PermissionMode::AcceptEdits,
            Workspace::ManagedWorktree { base: BaseBranch::Named { name: "feat/x".into() } },
        ));
        assert_eq!(wt["environment"]["environmentProviderId"], "git-worktree");
        assert_eq!(wt["environment"]["inputs"]["branch"], serde_json::json!({"kind": "named", "name": "feat/x"}));
        assert_eq!(wt["visibility"], "hidden");
        assert!(wt.get("input").is_none());
        assert_eq!(wt["pluginMetadata"]["rexWave"]["waveId"], "w1");
        assert_eq!(wt["pluginMetadata"]["rexWave"]["attempt"], 2);

        let existing = spawn_args(&mk_req(
            "acp-opencode",
            Some("opencode/gemini-4-carbon"),
            PermissionMode::Auto,
            Workspace::ExistingPath { path: "/tmp/wt".into() },
        ));
        assert_eq!(existing["environment"]["environmentProviderId"], "git-worktree");
        assert_eq!(existing["environment"]["inputs"], serde_json::json!({"kind": "existing", "path": "/tmp/wt"}));

        let checkout = spawn_args(&mk_req("pi", None, PermissionMode::Full, Workspace::ProjectDefault));
        assert_eq!(checkout["environment"]["environmentProviderId"], "project-checkout");
        assert_eq!(checkout["environment"]["inputs"], serde_json::json!({}));
        assert!(checkout.get("model").is_none());

        let allow = resolution_json(&InteractionResolution::AllowForSession {
            granted_permissions: Some(GrantedPermissions {
                file_system: Some(FileSystemGrant { read: vec!["/a".into()], write: vec!["/b".into()] }),
                network: Some(NetworkGrant { enabled: Some(true) }),
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
            UserAnswer { selected: vec!["opt-a".into()], free_text: None },
        );
        let ua = resolution_json(&InteractionResolution::UserAnswer { answers });
        assert_eq!(ua, serde_json::json!({"kind": "user_answer", "answers": {"q1": {"selected": ["opt-a"]}}}));
        assert_eq!(
            resolution_json(&InteractionResolution::RequestAnswer { value: serde_json::json!({"ok": 1}) }),
            serde_json::json!({"kind": "request_answer", "value": {"ok": 1}})
        );
        assert_eq!(resolution_json(&InteractionResolution::Deny), serde_json::json!({"decision": "deny"}));
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
            (LoopbackHarness { inner, frame_tx, bridge }, frame_rx)
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
            HarnessClient { me, harness: self.clone() }
        }
    }

    impl InMemState {
        fn commit(&mut self, stream: StreamId, by: Principal, event: Event, tx: &mpsc::UnboundedSender<Envelope>) -> Envelope {
            let log = self.entries.entry(stream.clone()).or_default();
            let seq = Seq(log.len() as u64 + 1);
            let env = Envelope { stream, seq, at: UnixMs(1_000 + seq.0), by, event };
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

        fn call_wire(&self, cmd: WireCommand) -> impl Future<Output = Result<WireReply, ApiError>> + Send {
            let me = self.me.clone();
            let harness = self.harness.clone();
            async move {
                authorize(cmd.meta().audience, &me)?;
                match cmd.route() {
                    Routed::Wave(wc) => {
                        let mut g = harness.inner.lock().await;
                        let ctx = DecideCtx { by: me.clone(), at: UnixMs(1_000) };
                        let stream = Waves::address(&wc, &ctx)?;
                        let StreamId::Wave { wave } = &stream else { unreachable!() };
                        let events = {
                            let st = g.waves.entry(wave.clone()).or_default();
                            Waves::decide(st, &wc, &ctx)?
                        };
                        let mut committed = Vec::new();
                        for ev in events {
                            harness.bridge.observe_wave_event(wave.clone(), ev.clone());
                            let env = g.commit(stream.clone(), me.clone(), Event::Wave(ev.clone()), &harness.frame_tx);
                            let meta = EntryMeta { seq: env.seq, at: env.at, by: me.clone() };
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
                                        Event::Room(RoomEvent::Opened { id: id.clone(), origin: origin.clone(), topic: c.topic.clone() }),
                                        &harness.frame_tx,
                                    );
                                }
                                let head = Seq(g.entries.get(&stream).map_or(0, Vec::len) as u64);
                                let members = g.room_members.get(&id).cloned().unwrap_or_default().into_iter().collect();
                                Ok(WireReply::RoomEnsure(RoomInfo { id, origin, topic: c.topic, head, members }))
                            }
                            RoomCommand::RoomJoin(c) => {
                                let id = c.room.resolve(&me)?;
                                let member = c.member.unwrap_or_else(|| me.clone());
                                let handle = match c.handle {
                                    Some(h) => h,
                                    None => match &member {
                                        Principal::Operator { seat } | Principal::Seat { seat } => Handle::parse(seat.as_str())?,
                                        Principal::Worker { role, .. } => Handle::parse(role.as_str())?,
                                        Principal::System { part } => Handle::parse(format!("{part:?}").to_lowercase())?,
                                    },
                                };
                                g.room_members.entry(id.clone()).or_default().insert(handle.clone(), member.clone());
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
                                    && let Some(&seq) = g.room_posts.get(&(id.clone(), idem.clone()))
                                {
                                    return Ok(WireReply::RoomPost(Posted { room: id, seq, mentions: Mentions::default() }));
                                }
                                let map = g.room_members.get(&id).cloned().unwrap_or_default();
                                let mut principals = Vec::new();
                                let mut all = false;
                                for tok in c.body.as_str().split_whitespace() {
                                    if let Some(raw) = tok.strip_prefix('@') {
                                        let clean = raw.trim_end_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '-' && ch != '_');
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
                                        reply_to: c.reply_to,
                                        idem: c.idem.clone(),
                                    }),
                                    &harness.frame_tx,
                                );
                                if let Some(idem) = c.idem {
                                    g.room_posts.insert((id.clone(), idem), env.seq);
                                }
                                Ok(WireReply::RoomPost(Posted { room: id, seq: env.seq, mentions }))
                            }
                            RoomCommand::RoomLeave(_) => Ok(WireReply::RoomLeave(Ack)),
                        }
                    }
                    Routed::Board(BoardCommand::BoardEnsure(BoardEnsure { board, lanes })) => {
                        let mut g = harness.inner.lock().await;
                        g.boards.insert(board.clone());
                        g.commit(
                            StreamId::Board { board: board.clone() },
                            me,
                            Event::Board(BoardEvent::Opened { board: board.clone(), lanes: lanes.clone().unwrap_or_default() }),
                            &harness.frame_tx,
                        );
                        Ok(WireReply::BoardEnsure(BoardInfo { board, lanes: lanes.unwrap_or_default(), head: 1 }))
                    }
                    Routed::Read(ReadCommand::ThreadPage(t)) => {
                        let limit = t.limit.unwrap_or(100);
                        Ok(WireReply::ThreadPage(harness.bridge.page(&t.wave, &t.role, t.after, limit).await?))
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
                                let stream = StreamId::Room { room: room_id.clone() };
                                let since = w.after.at(&stream);
                                if let Some(log) = g.entries.get(&stream) {
                                    let catchup: Vec<Envelope> = log.iter().filter(|e| e.seq > since).cloned().collect();
                                    if !catchup.is_empty() {
                                        let last = catchup.last().unwrap().seq;
                                        return Ok(WireReply::Wait(Page {
                                            entries: catchup,
                                            cursor: w.after.with(stream, last),
                                            timed_out: false,
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
                                        && env.stream == (StreamId::Room { room: room_id.clone() })
                                    {
                                        let cursor = w.after.clone().with(env.stream.clone(), env.seq);
                                        return Ok(WireReply::Wait(Page {
                                            entries: vec![env],
                                            cursor,
                                            timed_out: false,
                                        }));
                                    }
                                }
                                _ => {
                                    return Ok(WireReply::Wait(Page {
                                        entries: vec![],
                                        cursor: w.after,
                                        timed_out: true,
                                    }));
                                }
                            }
                        }
                    }
                    _ => Err(ApiError::Unavailable { family: Family::Mail }),
                }
            }
        }

        fn watch(&self, interest: Interest, _from: Cursor) -> impl Future<Output = Result<FrameStream, ApiError>> + Send {
            let bridge = self.harness.bridge.clone();
            async move {
                match interest {
                    Interest::Agent { wave, role } | Interest::Stage { target: StageTarget::Agent { wave, role } } => {
                        bridge.live(&wave, &role).await
                    }
                    _ => Err(ApiError::Unavailable { family: Family::Read }),
                }
            }
        }
    }

    #[derive(Default)]
    struct MockPluginLedger {
        ops: Vec<String>,
        kv: HashMap<String, String>,
        spawned_args: Vec<serde_json::Value>,
        sends: Vec<(String, String, String)>,
        resolves: Vec<(String, String, serde_json::Value)>,
        watched: BTreeSet<String>,
        tool_results: Vec<(String, bool, String)>,
        events_by_thread: HashMap<String, Vec<serde_json::Value>>,
    }

    #[tokio::test]
    async fn loopback_bb_plugin_e2e_spawn_resolve_doorbell_lease_cancel_reconnect_and_stop_before_archive() {
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
                let Ok(mut ws) = ws::WsConn::accept(tcp, token).await else { continue };
                if ws
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
                            let _ = ws.send_close(1012, "plugin reload").await;
                            break;
                        }
                        Some(up) = push_up_rx.recv() => {
                            if ws.send_text(&serde_json::to_string(&up).unwrap()).await.is_err() {
                                break;
                            }
                        }
                        inbound = ws.recv_text() => {
                            let Ok(Some(raw)) = inbound else { break };
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
                                    ws.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
                                }
                                protocol::Down::Spawn { req_id, args } => {
                                    st.spawned_args.push(args.clone());
                                    let meta: protocol::WaveMeta =
                                        serde_json::from_value(args["pluginMetadata"]["rexWave"].clone()).unwrap();
                                    let tid = format!("th_{}_{}", meta.wave_id, meta.role);
                                    let key = format!("spawn:{}/{}/{}", meta.wave_id, meta.role, meta.attempt);
                                    st.kv.insert(key, tid.clone());
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
                                    ws.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
                                }
                                protocol::Down::Send { req_id, thread_id, mode, text } => {
                                    st.ops.push(format!("send:{thread_id}:{mode}"));
                                    st.sends.push((thread_id, mode, text));
                                    let reply = protocol::Up::Result {
                                        req_id,
                                        ok: true,
                                        value: serde_json::json!({ "delivery": "sent" }),
                                    };
                                    ws.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
                                }
                                protocol::Down::Resolve { req_id, thread_id, interaction_id, resolution } => {
                                    st.ops.push(format!("resolve:{thread_id}:{interaction_id}"));
                                    st.resolves.push((thread_id, interaction_id, resolution));
                                    let reply = protocol::Up::Result { req_id, ok: true, value: serde_json::Value::Null };
                                    ws.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
                                }
                                protocol::Down::Retire { req_id, thread_id } => {
                                    st.ops.push(format!("stop:{thread_id}"));
                                    st.ops.push(format!("archive:{thread_id}"));
                                    let reply = protocol::Up::Result { req_id, ok: true, value: serde_json::Value::Null };
                                    ws.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
                                }
                                protocol::Down::EventsPage { req_id, thread_id, after_seq, limit } => {
                                    let rows: Vec<serde_json::Value> = st
                                        .events_by_thread
                                        .get(&thread_id)
                                        .cloned()
                                        .unwrap_or_default()
                                        .into_iter()
                                        .filter(|r| after_seq.is_none_or(|a| r["seq"].as_u64().unwrap_or(0) > a))
                                        .take(limit as usize)
                                        .collect();
                                    let reply = protocol::Up::Result {
                                        req_id,
                                        ok: true,
                                        value: serde_json::Value::Array(rows),
                                    };
                                    ws.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
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
                                    ws.send_text(&serde_json::to_string(&reply).unwrap()).await.unwrap();
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
        let sys_conductor = harness.client(Principal::System { part: SystemPart::WaveConductor });
        conductor
            .handle_frame(Frame::CaughtUp { cursor: Cursor::default() }, &sys_conductor)
            .await
            .unwrap();

        let conductor_task = tokio::spawn(async move {
            while let Some(env) = frame_rx.recv().await {
                let _ = conductor.handle_frame(Frame::Committed(env), &sys_conductor).await;
            }
        });

        let op = harness.client(Principal::Operator { seat: SeatName::parse("owner").unwrap() });
        let wave = WaveSlug::parse("auth").unwrap();
        let role = RoleName::parse("verifier").unwrap();
        let spec = WaveSpec {
            slug: wave.clone(),
            project: BbProjectId::parse("proj_3vs7nkyeum").unwrap(),
            goal: Body::parse("Verify OAuth refresh").unwrap(),
            defaults: AgentSpec {
                provider: ProviderId::parse("codex").unwrap(),
                model: Some(ModelId::parse("gpt-6.1-sol").unwrap()),
                reasoning: Some("xhigh".into()),
                service_tier: None,
                permission: PermissionMode::AcceptEdits,
                workspace: Workspace::ManagedWorktree { base: BaseBranch::Default },
            },
            workers: vec![WorkerSpec {
                role: role.clone(),
                brief: Body::parse("Audit token rotation").unwrap(),
                agent: None,
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
            assert_eq!(st.spawned_args[0]["environment"]["environmentProviderId"], "git-worktree");
            assert_eq!(st.spawned_args[0]["pluginMetadata"]["rexWave"]["role"], "verifier");
        }

        let idempotent_tid = BbHost(bridge.clone())
            .spawn(SpawnRequest {
                wave: wave.clone(),
                role: role.clone(),
                attempt: 1,
                project: BbProjectId::parse("proj_3vs7nkyeum").unwrap(),
                agent: AgentSpec {
                    provider: ProviderId::parse("codex").unwrap(),
                    model: Some(ModelId::parse("gpt-6.1-sol").unwrap()),
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::AcceptEdits,
                    workspace: Workspace::ProjectDefault,
                },
                room: started.room.clone(),
                prompt: Body::parse("retry").unwrap(),
                title: "[auth] @verifier".into(),
            })
            .await
            .unwrap();
        assert_eq!(idempotent_tid.as_str(), "th_auth_verifier");
        assert_eq!(ledger.lock().await.spawned_args.len(), 1, "FindByMeta prevented duplicate spawn");

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
            resolution: InteractionResolution::AllowOnce { granted_permissions: None },
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
                project_id: "proj_3vs7nkyeum".into(),
                meta: None,
                tool: "wait".into(),
                args: serde_json::to_value(Wait::room(started.room.clone(), Seq(4), false)).unwrap(),
            })
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                if matches!(&snap.workers[0].2, WorkerPhase::Live { activity: Activity::Parked { .. }, .. }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker entered Parked state during rex_wait");

        op.call(RoomPost {
            room: RoomRef::Id { id: started.room.clone() },
            body: Body::parse("@verifier slice A ready").unwrap(),
            reply_to: None,
            idem: None,
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if ledger.lock().await.tool_results.iter().any(|(id, ok, _)| id == "call_wait_1" && *ok) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("rex_wait woke and returned catch-up slice");
        assert!(ledger.lock().await.sends.is_empty(), "Parked worker woke via ReadSide with zero doorbell sends");

        push_up_tx
            .send(protocol::Up::Thread {
                signal: protocol::ThreadSignal::Idle,
                thread_id: "th_auth_verifier".into(),
                project_id: "proj_3vs7nkyeum".into(),
                meta: None,
                error: None,
            })
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                if matches!(&snap.workers[0].2, WorkerPhase::Live { activity: Activity::Idle, .. }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker transitioned to Idle");

        op.call(RoomPost {
            room: RoomRef::Id { id: started.room.clone() },
            body: Body::parse("@verifier check #4").unwrap(),
            reply_to: None,
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
            .call(ThreadPage { wave: wave.clone(), role: role.clone(), after: None, limit: Some(50) })
            .await
            .unwrap();
        assert_eq!(page.events.len(), 2);
        assert_eq!(page.usage.as_ref().unwrap().used_tokens, 24_000);
        assert_eq!(page.usage.as_ref().unwrap().cost_usd, Some(0.18));

        let mut sub = bridge.subscribe_agent(&wave, &role).await.unwrap();
        let stage_stream = op
            .watch(
                Interest::Stage {
                    target: StageTarget::Agent { wave: wave.clone(), role: role.clone() },
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

        reload_tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;

        {
            let mut st = ledger.lock().await;
            st.events_by_thread.get_mut("th_auth_verifier").unwrap().push(serde_json::json!({
                "seq": 3,
                "createdAt": 1003,
                "type": "item/completed",
                "data": { "item": { "type": "agentMessage", "text": "Slice #4 looks clean" } }
            }));
        }
        push_up_tx
            .send(protocol::Up::ThreadEvents {
                thread_id: "th_auth_verifier".into(),
                sequence: 3,
            })
            .unwrap();

        let live_ev = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("received live agent event after reconnect")
            .unwrap();
        assert_eq!(live_ev.seq, BbSeq(3));
        assert_eq!(live_ev.kind, AgentEventKind::AssistantText { text: "Slice #4 looks clean".into() });

        drop(sub);
        drop(stage_stream);

        let unwatched = tokio::time::timeout(Duration::from_secs(2), unwatch_notify_rx.recv())
            .await
            .expect("dropping AgentInspector watch leases immediately sent UnwatchThread")
            .unwrap();
        assert_eq!(unwatched, "th_auth_verifier");

        ledger.lock().await.ops.clear();
        op.call(WaveFinish {
            wave: wave.clone(),
            outcome: WaveOutcome::Completed { summary: Body::parse("Verified").unwrap() },
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snap = op.call(WaveView { wave: wave.clone() }).await.unwrap();
                if matches!(&snap.workers[0].2, WorkerPhase::Retired { thread: Some(t) } if t.as_str() == "th_auth_verifier") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker retired cleanly");

        assert_eq!(
            ledger.lock().await.ops,
            vec!["stop:th_auth_verifier", "archive:th_auth_verifier"],
            "stop MUST execute strictly before archive"
        );

        let _ = shutdown_tx.send(true);
        let _ = bridge_runner.await;
        conductor_task.abort();
        server_task.abort();
    }
}
