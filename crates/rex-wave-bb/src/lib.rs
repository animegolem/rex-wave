//! # rex-wave-bb, the daemon's side of the BB bridge
//!
//! ```text
//!  rex-wave daemon ──dials──▶ ws://127.0.0.1:38886/api/v1/plugins/rex-wave/http/bridge  (auth: token)
//!        ▲   Down: spawn / find_by_meta / send / resolve / retire / events_page / watch_thread / tool_result / push
//!        └── Up:   hello / thread{created,active,idle,failed,archived} / interaction / thread_events / tool_call / result
//! ```
//!
//! One socket, two directions of RPC. The daemon dials BB (not the other way)
//! so BB never needs to know where the daemon lives, and a BB reload (`1012`)
//! is just a reconnect + `afterSeq` reconcile.
//!
//! [`BbBridge`] implements:
//! * `rex_wave_waves::AgentHost`, spawn/tell/resolve/retire worker threads on
//!   **any** BB provider (`claude-code`, `codex`, `acp-*`, `pi`), mapping
//!   `Workspace` to the verified BB environment shapes (#17: provider id is
//!   `"git-worktree"`, not the package name).
//! * `rex_wave_runtime::AgentFeed`, thread pages + live per-agent frames for
//!   the TUI Agent Inspector, held only while watched.
//!
//! and serves BB agent tool calls: an `Up::ToolCall{thread_id, tool, args}` is
//! resolved to `Principal::Worker{wave, role}` **from the thread's own
//! pluginMetadata as recorded in the wave stream** (never from args), then
//! executed as `WireCommand` through a `LocalClient` for that principal.
//!
//! The wire protocol is private (`mod protocol`). Its TypeScript mirror lives in
//! `bb-plugin/protocol.ts`; both sides round-trip the shared
//! `bb-plugin/protocol.fixtures.json` in their tests, so drift fails CI.

// Sketch phase: bodies are `todo!()`, so private state reads as dead code until Phase D.
#![allow(dead_code)]

use rex_wave_api::read::ThreadPageView;
use rex_wave_api::room::Body;
use rex_wave_api::wave::{InteractionResolution, SteerDeliveryMode};
use rex_wave_api::*;
use rex_wave_runtime::{AgentFeed, BoxFut, Router};
use rex_wave_waves::{AgentHost, HostError, SpawnRequest};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

mod protocol {
    //! Private wire types. Mirrored by `bb-plugin/protocol.ts`.
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    pub type ReqId = u64;

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(tag = "t", rename_all = "snake_case")]
    pub enum Up {
        Hello { plugin_version: String, sdk_version: String },
        Thread { signal: ThreadSignal, thread_id: String, project_id: String, meta: Option<WaveMeta>, error: Option<String> },
        Interaction { thread_id: String, interaction: Value },
        ThreadEvents { thread_id: String, sequence: u64 },
        ToolCall { call_id: String, thread_id: String, project_id: String, tool: String, args: Value },
        Result { req_id: ReqId, ok: bool, value: Value },
    }

    #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ThreadSignal {
        Created,
        Active,
        Idle,
        Failed,
        Archived,
    }

    /// The `pluginMetadata` the daemon seeds on spawn and reads back on every event.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct WaveMeta {
        pub wave_id: String,
        pub role: String,
        pub attempt: u32,
        pub room_id: String,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(tag = "t", rename_all = "snake_case")]
    pub enum Down {
        /// `args` is a ready-to-pass `ThreadSpawnArgs` (built by `spawn_args`).
        Spawn { req_id: ReqId, args: Value },
        FindByMeta { req_id: ReqId, meta: WaveMeta },
        Send { req_id: ReqId, thread_id: String, mode: String, text: String },
        Resolve { req_id: ReqId, thread_id: String, interaction_id: String, resolution: Value },
        Retire { req_id: ReqId, thread_id: String },
        EventsPage { req_id: ReqId, thread_id: String, after_seq: Option<u64>, limit: u32 },
        ThreadContext { req_id: ReqId, thread_id: String },
        WatchThread { thread_id: String },
        UnwatchThread { thread_id: String },
        ToolResult { call_id: String, ok: bool, text: String },
        Push { title: String, body: String },
    }
}

/// Pure: build BB `ThreadSpawnArgs` JSON for a request (prompt XOR input;
/// environment mapping per #17; `visibility: "hidden"`; `pluginMetadata`).
pub fn spawn_args(req: &SpawnRequest) -> serde_json::Value {
    let _ = req;
    todo!("spawn_args")
}

/// Pure: map a domain `InteractionResolution` to BB `PendingInteractionResolution` JSON.
pub fn resolution_json(r: &InteractionResolution) -> serde_json::Value {
    let _ = r;
    todo!("resolution_json: allow_once/allow_for_session(grantedPermissions) | deny | user_answer | plugin_submitted | request_answer")
}

/// Pure: BB send mode for a tell.
pub fn send_mode(mode: SteerDeliveryMode) -> &'static str {
    match mode {
        SteerDeliveryMode::Auto => "auto",
        SteerDeliveryMode::Start => "start",
        SteerDeliveryMode::Steer => "steer",
        SteerDeliveryMode::SteerIfActive => "steer-if-active",
        SteerDeliveryMode::QueueIfActive => "queue-if-active",
    }
}

/// The tool catalog for `bb-plugin/tools.generated.json`, derived from
/// `rex_wave_api::TOOLS` (Member-audience tools only) + `run_code`.
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

/// Bridge connection settings.
#[derive(Clone, Debug)]
pub struct BridgeConfig {
    /// `ws://127.0.0.1:38886/api/v1/plugins/rex-wave/http/bridge`
    pub url: String,
    /// BB plugin token (`auth: "token"`).
    pub token: String,
}

/// The live link: request/response correlation + thread->worker index.
pub struct BbBridge {
    cfg: BridgeConfig,
    /// Bound in `run` (the router itself holds this bridge as its AgentFeed).
    router: std::sync::OnceLock<Arc<Router>>,
    pending: Mutex<HashMap<u64, tokio::sync::oneshot::Sender<serde_json::Value>>>,
    /// Derived from wave streams (WorkerObserved Spawned): thread -> (wave, role).
    threads: Mutex<HashMap<BbThreadId, (WaveSlug, RoleName)>>,
}

impl BbBridge {
    pub fn new(cfg: BridgeConfig) -> Arc<Self> {
        Arc::new(BbBridge { cfg, router: std::sync::OnceLock::new(), pending: Mutex::new(HashMap::new()), threads: Mutex::new(HashMap::new()) })
    }

    /// Connect-and-serve loop with backoff: on (re)connect, reconcile every
    /// live worker via `events_page(afterSeq)` and re-issue `wave_observe`.
    pub async fn run(self: Arc<Self>, router: Arc<Router>) {
        let _ = self.router.set(router);
        let _ = (&self.cfg, &self.router, &self.pending, &self.threads);
        todo!("BbBridge::run")
    }

    async fn rpc(&self, down: protocol::Down) -> Result<serde_json::Value, HostError> {
        let _ = down;
        todo!("BbBridge::rpc: assign req_id, send, await Result")
    }

    /// Up::ToolCall -> principal from `threads` -> LocalClient::call_wire -> Down::ToolResult.
    async fn on_tool_call(&self, call_id: String, thread: BbThreadId, tool: String, args: serde_json::Value) {
        let _ = (call_id, thread, tool, args);
        todo!("on_tool_call")
    }

    /// Up::Thread / Up::Interaction -> `wave_observe` as System{BbBridge}.
    async fn on_thread_signal(&self, up: protocol::Up) {
        let _ = up;
        todo!("on_thread_signal")
    }
}

/// Cloneable `AgentHost` handle over the shared bridge (handed to `WaveConductor`).
#[derive(Clone)]
pub struct BbHost(pub Arc<BbBridge>);

impl AgentHost for BbHost {
    fn spawn(&self, req: SpawnRequest) -> impl Future<Output = Result<BbThreadId, HostError>> + Send {
        async move {
            // 1. FindByMeta{wave, role, attempt} -> Some(thread) => return it (idempotent)
            // 2. Spawn{args: spawn_args(&req)} -> ThreadResponse.id
            let _ = (req, protocol::Down::WatchThread { thread_id: String::new() });
            todo!("AgentHost::spawn")
        }
    }
    fn tell(&self, thread: &BbThreadId, body: &Body, mode: SteerDeliveryMode) -> impl Future<Output = Result<(), HostError>> + Send {
        let down = protocol::Down::Send { req_id: 0, thread_id: thread.to_string(), mode: send_mode(mode).into(), text: body.as_str().into() };
        async move { self.0.rpc(down).await.map(|_| ()) }
    }
    fn resolve(&self, thread: &BbThreadId, interaction: &BbInteractionId, resolution: &InteractionResolution) -> impl Future<Output = Result<(), HostError>> + Send {
        let down = protocol::Down::Resolve {
            req_id: 0,
            thread_id: thread.to_string(),
            interaction_id: interaction.to_string(),
            resolution: resolution_json(resolution),
        };
        async move { self.0.rpc(down).await.map(|_| ()) }
    }
    fn retire(&self, thread: &BbThreadId) -> impl Future<Output = Result<(), HostError>> + Send {
        let down = protocol::Down::Retire { req_id: 0, thread_id: thread.to_string() };
        async move { self.0.rpc(down).await.map(|_| ()) }
    }
}

impl AgentFeed for BbBridge {
    fn page(&self, wave: &WaveSlug, role: &RoleName, after: Option<BbSeq>, limit: u32) -> BoxFut<'_, Result<ThreadPageView, ApiError>> {
        let _ = (wave, role, after, limit);
        Box::pin(async move { todo!("AgentFeed::page: thread lookup -> EventsPage -> normalize to AgentEvent + ContextUsage") })
    }
    fn live(&self, wave: &WaveSlug, role: &RoleName) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        let _ = (wave, role);
        Box::pin(async move {
            todo!("AgentFeed::live: WatchThread; stream of Frame::Agent; on drop -> UnwatchThread (lease)")
        })
    }
}
