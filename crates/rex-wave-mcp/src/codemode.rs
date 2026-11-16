//! Code Mode: `run_code` with per-call isolation.
//!
//! Every invocation gets a **brand-new `AsyncRuntime` + `AsyncContext`**
//! (measured in #17: p50 0.55 ms, only ~12 µs over a shared runtime, and a
//! lower p99 because there is no cross-caller contention). Hard caps:
//! C-heap limit, stack limit, and an interrupt-handler deadline that cannot
//! be caught by JS. No `process`/`require`/`fetch`; `globalThis.rex` is frozen.
//!
//! Inside the sandbox:
//!
//! ```js
//! const tasks = await rex.board_view({ board: { board: "project", project: "p_123" } });
//! await rex.room_post({ room: { ref: "dm", with: { kind: "seat", seat: "astra" } },
//!                       body: `${tasks.lanes[1][1].length} in progress` });
//! const page = await rex.wait({ interest: { interest: "mentions" }, after: cur, timeout_ms: 30000 });
//! return page.entries.length;
//! ```
//!
//! `rex.<tool>(args)` is generated from `TOOLS` (audience-filtered for the
//! session) and resolves to the JSON reply value, or rejects with the
//! `ApiError` JSON. Host calls cross out of JS through a channel to the async
//! Rust side, which calls `Client::call_wire`, the identity is the session's.

use rex_wave_api::*;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Budget {
    pub deadline: Duration,
    pub heap_bytes: usize,
    pub stack_bytes: usize,
    pub max_host_calls: u32,
}

impl Default for Budget {
    fn default() -> Self {
        Budget { deadline: Duration::from_secs(30), heap_bytes: 8 << 20, stack_bytes: 512 << 10, max_host_calls: 256 }
    }
}

/// One host call made by a script (returned for transparency / audit).
#[derive(Clone, Debug, serde::Serialize)]
pub struct CallTrace {
    pub tool: &'static str,
    pub ok: bool,
    pub micros: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct CodeOutcome {
    pub value: serde_json::Value,
    pub logs: Vec<String>,
    pub calls: Vec<CallTrace>,
}

#[derive(Debug, thiserror::Error)]
pub enum CodeError {
    #[error("script exceeded its {0:?} deadline")]
    Deadline(Duration),
    #[error("script exceeded its memory cap")]
    OutOfMemory,
    #[error("script threw: {0}")]
    Thrown(String),
    #[error("too many host calls")]
    HostCallBudget,
}

/// Run one script in a fresh isolated runtime.
pub async fn run_code<C: Client>(client: &C, source: &str, budget: Budget) -> Result<CodeOutcome, CodeError> {
    let _ = (client, source, budget);
    // 1. rt = fresh_runtime(&budget); ctx = AsyncContext::full(&rt)
    // 2. install_rex(ctx, audience-filtered TOOLS, host-call channel); Object.freeze(globalThis.rex)
    // 3. wrap source in `(async () => { ... })()`; drive with deadline; pump host calls via client.call_wire
    // 4. JSON.stringify the resolved value -> CodeOutcome
    todo!("run_code")
}

/// Fresh runtime with heap/stack caps and an interrupt deadline.
#[allow(dead_code)]
fn fresh_runtime(budget: &Budget) -> rquickjs::Result<rquickjs::AsyncRuntime> {
    let _ = budget;
    todo!("fresh_runtime: AsyncRuntime::new; set_memory_limit; set_max_stack_size; set_interrupt_handler")
}

/// Define the frozen `rex` global with one async function per allowed tool.
#[allow(dead_code)]
fn install_rex(ctx: &rquickjs::Ctx<'_>, tools: &[&'static ToolMeta]) -> rquickjs::Result<()> {
    let _ = (ctx, tools);
    todo!("install_rex")
}
