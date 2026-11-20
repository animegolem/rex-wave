//! Code Mode (`run_code`) with per-call runtime isolation.
//!
//! Every invocation allocates a fresh `rquickjs::AsyncRuntime` and
//! `AsyncContext` bounded by heap, stack, host-call count, and wall-clock
//! interrupt caps. Ambient runtime globals are absent and `globalThis.rex` is
//! frozen.

use crate::{allowed_tools, parse_wire_command, to_camel_case, unwrap_wire_reply};
use rex_wave_api::*;
use rquickjs::prelude::Async;
use rquickjs::{AsyncContext, AsyncRuntime, CatchResultExt, CaughtError, Function, Promise};
use serde_json::{Value, json};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

const MAX_LOG_LINES: usize = 256;
const MAX_LOG_LINE_BYTES: usize = 4096;

const BOOTSTRAP_JS: &str = r#"(() => {
  const hostCall = globalThis.__rex_host_call;
  const hostLog = globalThis.__rex_log;
  const jsonStringify = JSON.stringify;
  const jsonParse = JSON.parse;
  const objectAssign = Object.assign;
  const objectFreeze = Object.freeze;
  const ErrorCtor = Error;
  const toolSpecs = jsonParse(globalThis.__rex_tools_json);
  const whoamiJson = globalThis.__rex_whoami_json;
  const userCode = globalThis.__rex_user_code;
  delete globalThis.__rex_host_call;
  delete globalThis.__rex_log;
  delete globalThis.__rex_tools_json;
  delete globalThis.__rex_whoami_json;
  delete globalThis.__rex_user_code;
  for (const ambient of ["process", "require", "fetch", "Deno", "Bun", "XMLHttpRequest", "WebSocket"]) {
    delete globalThis[ambient];
  }
  const formatArg = (v) => {
    if (typeof v === "string") return v;
    try {
      const s = jsonStringify(v);
      return s === undefined ? String(v) : s;
    } catch (_) {
      return String(v);
    }
  };
  const writeLog = (...args) => {
    hostLog(args.map(formatArg).join(" "));
  };
  Object.defineProperty(globalThis, "console", {
    value: objectFreeze({
      log: writeLog,
      info: writeLog,
      warn: writeLog,
      error: writeLog,
      debug: writeLog,
    }),
    writable: false,
    configurable: false,
    enumerable: true,
  });
  const rex = {
    whoami: () => objectFreeze(jsonParse(whoamiJson)),
  };
  for (const spec of toolSpecs) {
    const invoke = async (args) => {
      const encodedArgs = args === undefined ? "{}" : jsonStringify(args);
      const raw = await hostCall(spec.snake, encodedArgs === undefined ? "{}" : encodedArgs);
      const env = jsonParse(raw);
      if (!env.ok) {
        const err = new ErrorCtor(env.message);
        if (env.error && typeof env.error === "object") {
          objectAssign(err, env.error);
        }
        throw err;
      }
      return env.value;
    };
    rex[spec.snake] = invoke;
    if (spec.camel !== spec.snake) {
      rex[spec.camel] = invoke;
    }
  }
  objectFreeze(rex);
  Object.defineProperty(globalThis, "rex", {
    value: rex,
    writable: false,
    configurable: false,
    enumerable: true,
  });
  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
  return (async () => {
    const runner = new AsyncFunction(userCode);
    const out = await runner();
    if (out === undefined) return "null";
    const encoded = jsonStringify(out);
    return encoded === undefined ? "null" : encoded;
  })();
})()"#;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Budget {
    pub deadline: Duration,
    pub heap_bytes: usize,
    pub stack_bytes: usize,
    pub max_host_calls: u32,
}

impl Default for Budget {
    fn default() -> Self {
        Budget {
            deadline: Duration::from_millis(5_000),
            heap_bytes: 8 << 20,
            stack_bytes: 512 << 10,
            max_host_calls: 256,
        }
    }
}

/// One host call made by a script.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct CallTrace {
    pub tool: &'static str,
    pub ok: bool,
    pub micros: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct CodeOutcome {
    pub value: Value,
    pub logs: Vec<String>,
    pub calls: Vec<CallTrace>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
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

struct HostCallReq {
    tool: String,
    args_json: String,
    reply: oneshot::Sender<String>,
}

struct PendingCall<'a> {
    tool: &'static str,
    started: Instant,
    fut: Pin<Box<dyn Future<Output = Result<WireReply, ApiError>> + Send + 'a>>,
    reply: oneshot::Sender<String>,
}

#[derive(serde::Serialize)]
struct ToolSpec<'a> {
    snake: &'a str,
    camel: String,
}

/// Run one script in a fresh isolated QuickJS runtime.
pub async fn run_code<C: Client>(
    client: &C,
    source: &str,
    budget: Budget,
) -> Result<CodeOutcome, CodeError> {
    let start = Instant::now();
    let deadline_std = start + budget.deadline;
    let deadline_tokio = tokio::time::Instant::now() + budget.deadline;
    let interrupted = Arc::new(AtomicBool::new(false));

    let rt = fresh_runtime(&budget, interrupted.clone(), deadline_std).await?;
    let ctx = AsyncContext::full(&rt)
        .await
        .map_err(|e| classify_rquickjs_err(e, &interrupted, deadline_std, budget.deadline))?;

    let allowed = allowed_tools(client.me());
    let specs: Vec<ToolSpec<'_>> = allowed
        .iter()
        .map(|t| ToolSpec {
            snake: t.name,
            camel: to_camel_case(t.name),
        })
        .collect();
    let specs_json = serde_json::to_string(&specs).map_err(|e| CodeError::Thrown(e.to_string()))?;
    let whoami_json =
        serde_json::to_string(client.me()).map_err(|e| CodeError::Thrown(e.to_string()))?;
    let user_code = source.to_owned();

    let (call_tx, mut call_rx) = mpsc::unbounded_channel::<HostCallReq>();
    let (log_tx, mut log_rx) = mpsc::unbounded_channel::<String>();

    let interrupted_eval = interrupted.clone();
    let budget_deadline = budget.deadline;
    let eval_fut = ctx.async_with(async move |ctx| {
        install_bindings(&ctx, call_tx, log_tx, &specs_json, &whoami_json, &user_code)
            .catch(&ctx)
            .map_err(|e| {
                classify_caught_error(&ctx, e, &interrupted_eval, deadline_std, budget_deadline)
            })?;

        let promise: Promise<'_> = ctx.eval(BOOTSTRAP_JS.as_bytes()).catch(&ctx).map_err(|e| {
            classify_caught_error(&ctx, e, &interrupted_eval, deadline_std, budget_deadline)
        })?;

        promise
            .into_future::<String>()
            .await
            .catch(&ctx)
            .map_err(|e| {
                classify_caught_error(&ctx, e, &interrupted_eval, deadline_std, budget_deadline)
            })
    });

    let mut eval_pin = std::pin::pin!(eval_fut);
    let mut sleep_pin = std::pin::pin!(tokio::time::sleep_until(deadline_tokio));
    let mut in_flight: Vec<PendingCall<'_>> = Vec::new();
    let mut calls: Vec<CallTrace> = Vec::new();
    let mut started_calls: u32 = 0;

    let outcome_json = std::future::poll_fn(|cx| {
        loop {
            if Instant::now() >= deadline_std {
                interrupted.store(true, Ordering::Relaxed);
                return Poll::Ready(Err(CodeError::Deadline(budget.deadline)));
            }

            if let Poll::Ready(res) = eval_pin.as_mut().poll(cx) {
                return Poll::Ready(res);
            }

            let mut made_progress = false;

            while let Poll::Ready(Some(req)) = call_rx.poll_recv(cx) {
                made_progress = true;
                if started_calls >= budget.max_host_calls {
                    interrupted.store(true, Ordering::Relaxed);
                    return Poll::Ready(Err(CodeError::HostCallBudget));
                }
                started_calls += 1;

                let Some(meta) = allowed.iter().copied().find(|t| t.name == req.tool) else {
                    let denied = ApiError::Denied {
                        principal: client.me().to_string(),
                        action: format!("call {}", req.tool),
                    };
                    let _ = req
                        .reply
                        .send(encode_host_err(&denied.to_string(), Some(&denied)));
                    continue;
                };

                let parsed_args: Value = match serde_json::from_str(&req.args_json) {
                    Ok(v) => v,
                    Err(e) => {
                        calls.push(CallTrace {
                            tool: meta.name,
                            ok: false,
                            micros: 0,
                        });
                        let _ = req.reply.send(encode_host_err(
                            &format!("invalid tool arguments JSON: {e}"),
                            None,
                        ));
                        continue;
                    }
                };

                let cmd = match parse_wire_command(meta, parsed_args) {
                    Ok(c) => c,
                    Err(msg) => {
                        calls.push(CallTrace {
                            tool: meta.name,
                            ok: false,
                            micros: 0,
                        });
                        let _ = req.reply.send(encode_host_err(&msg, None));
                        continue;
                    }
                };

                in_flight.push(PendingCall {
                    tool: meta.name,
                    started: Instant::now(),
                    fut: Box::pin(client.call_wire(cmd)),
                    reply: req.reply,
                });
            }

            let mut idx = 0;
            while idx < in_flight.len() {
                if let Poll::Ready(res) = in_flight[idx].fut.as_mut().poll(cx) {
                    let finished = in_flight.swap_remove(idx);
                    made_progress = true;
                    let micros = finished.started.elapsed().as_micros() as u64;
                    let payload = match res {
                        Ok(wire_reply) => match unwrap_wire_reply(finished.tool, wire_reply) {
                            Ok(val) => {
                                calls.push(CallTrace {
                                    tool: finished.tool,
                                    ok: true,
                                    micros,
                                });
                                encode_host_ok(&val)
                            }
                            Err(err) => {
                                calls.push(CallTrace {
                                    tool: finished.tool,
                                    ok: false,
                                    micros,
                                });
                                encode_host_err(&err.to_string(), Some(&err))
                            }
                        },
                        Err(err) => {
                            calls.push(CallTrace {
                                tool: finished.tool,
                                ok: false,
                                micros,
                            });
                            encode_host_err(&err.to_string(), Some(&err))
                        }
                    };
                    let _ = finished.reply.send(payload);
                } else {
                    idx += 1;
                }
            }

            if sleep_pin.as_mut().poll(cx).is_ready() {
                interrupted.store(true, Ordering::Relaxed);
                return Poll::Ready(Err(CodeError::Deadline(budget.deadline)));
            }

            if !made_progress {
                return Poll::Pending;
            }
        }
    })
    .await;

    drop(in_flight);
    rt.set_interrupt_handler(None).await;

    let raw_json = outcome_json?;
    let value: Value = serde_json::from_str(&raw_json)
        .map_err(|e| CodeError::Thrown(format!("invalid return JSON: {e}")))?;

    let mut logs = Vec::new();
    while let Ok(line) = log_rx.try_recv() {
        logs.push(line);
    }

    Ok(CodeOutcome { value, logs, calls })
}

async fn fresh_runtime(
    budget: &Budget,
    interrupted: Arc<AtomicBool>,
    deadline: Instant,
) -> Result<AsyncRuntime, CodeError> {
    let rt = AsyncRuntime::new().map_err(|e| CodeError::Thrown(e.to_string()))?;
    rt.set_memory_limit(budget.heap_bytes).await;
    rt.set_max_stack_size(budget.stack_bytes).await;
    // QuickJS polls this callback periodically from its bytecode loop; returning true raises an uncatchable interrupt.
    rt.set_interrupt_handler(Some(Box::new(move || {
        interrupted.load(Ordering::Relaxed) || Instant::now() >= deadline
    })))
    .await;
    Ok(rt)
}

fn install_bindings<'js>(
    ctx: &rquickjs::Ctx<'js>,
    call_tx: mpsc::UnboundedSender<HostCallReq>,
    log_tx: mpsc::UnboundedSender<String>,
    specs_json: &str,
    whoami_json: &str,
    user_code: &str,
) -> rquickjs::Result<()> {
    let globals = ctx.globals();
    let host_fn = Function::new(
        ctx.clone(),
        Async(move |tool: String, args_json: String| {
            let tx = call_tx.clone();
            async move {
                let (reply, rx) = oneshot::channel();
                if tx
                    .send(HostCallReq {
                        tool,
                        args_json,
                        reply,
                    })
                    .is_err()
                {
                    return Err(rquickjs::Error::Exception);
                }
                rx.await.map_err(|_| rquickjs::Error::Exception)
            }
        }),
    )?;
    let logged_lines = Arc::new(AtomicUsize::new(0));
    let log_fn = Function::new(ctx.clone(), move |mut line: String| {
        if logged_lines.fetch_add(1, Ordering::Relaxed) >= MAX_LOG_LINES {
            return;
        }
        if line.len() > MAX_LOG_LINE_BYTES {
            let mut end = MAX_LOG_LINE_BYTES;
            while !line.is_char_boundary(end) {
                end -= 1;
            }
            line.truncate(end);
        }
        let _ = log_tx.send(line);
    })?;
    globals.set("__rex_host_call", host_fn)?;
    globals.set("__rex_log", log_fn)?;
    globals.set("__rex_tools_json", specs_json)?;
    globals.set("__rex_whoami_json", whoami_json)?;
    globals.set("__rex_user_code", user_code)?;
    Ok(())
}

fn encode_host_ok(value: &Value) -> String {
    serde_json::to_string(&json!({ "ok": true, "value": value }))
        .unwrap_or_else(|_| r#"{"ok":true,"value":null}"#.into())
}

fn encode_host_err(message: &str, api_err: Option<&ApiError>) -> String {
    let err_val = match api_err {
        Some(err) => api_error_json(err),
        None => json!({ "error": "invalid", "detail": message }),
    };
    serde_json::to_string(&json!({
        "ok": false,
        "message": message,
        "error": err_val,
    }))
    .unwrap_or_else(|_| r#"{"ok":false,"message":"host call failed"}"#.into())
}

fn api_error_json(err: &ApiError) -> Value {
    if let Ok(val) = serde_json::to_value(err) {
        return val;
    }
    match err {
        ApiError::Transport(detail) => json!({ "error": "transport", "detail": detail }),
        ApiError::Internal(detail) => json!({ "error": "internal", "detail": detail }),
        other => json!({ "error": "internal", "detail": other.to_string() }),
    }
}

fn classify_rquickjs_err(
    err: rquickjs::Error,
    interrupted: &AtomicBool,
    deadline_at: Instant,
    deadline: Duration,
) -> CodeError {
    if interrupted.load(Ordering::Relaxed) || Instant::now() >= deadline_at {
        return CodeError::Deadline(deadline);
    }
    if matches!(err, rquickjs::Error::Allocation) {
        return CodeError::OutOfMemory;
    }
    let msg = err.to_string();
    classify_message(&msg, deadline)
}

fn classify_caught_error<'js>(
    ctx: &rquickjs::Ctx<'js>,
    err: CaughtError<'js>,
    interrupted: &AtomicBool,
    deadline_at: Instant,
    deadline: Duration,
) -> CodeError {
    if interrupted.load(Ordering::Relaxed) || Instant::now() >= deadline_at {
        return CodeError::Deadline(deadline);
    }
    match err {
        CaughtError::Error(rquickjs::Error::Allocation) => CodeError::OutOfMemory,
        CaughtError::Error(other) => classify_message(&other.to_string(), deadline),
        CaughtError::Exception(ex) => {
            let mut text = ex.message().unwrap_or_default();
            if text.is_empty() {
                text = ex.to_string();
            }
            classify_message(&text, deadline)
        }
        CaughtError::Value(val) => {
            let text = val
                .as_string()
                .and_then(|s| s.to_string().ok())
                .or_else(|| {
                    ctx.json_stringify(val.clone())
                        .ok()
                        .flatten()
                        .and_then(|s| s.to_string().ok())
                })
                .unwrap_or_else(|| format!("{val:?}"));
            classify_message(&text, deadline)
        }
    }
}

fn classify_message(msg: &str, deadline: Duration) -> CodeError {
    // QuickJS C runtime uses fixed InternalError messages for JS_SetInterruptHandler and JS_SetMemoryLimit.
    let lower = msg.to_ascii_lowercase();
    if lower.contains("interrupted") {
        CodeError::Deadline(deadline)
    } else if lower.contains("out of memory") {
        CodeError::OutOfMemory
    } else {
        CodeError::Thrown(msg.to_owned())
    }
}
