//! MCP `2025-11-25` server over any [`rex_wave_api::Client`].
//!
//! Atomic tools are derived from [`rex_wave_api::TOOLS`] filtered by the
//! session principal's [`Audience`]. `run_code` runs an isolated QuickJS
//! async body against a frozen `globalThis.rex` binding for the same audience.
//! Both paths enter the daemon through [`Client::call_wire`].

pub mod codemode;
mod resources;
mod rpc;

use rex_wave_api::*;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;

pub const PROTOCOL_VERSION: &str = "2025-11-25";
pub const RUN_CODE_TOOL: &str = "run_code";
pub const MAX_RUN_CODE_TIMEOUT_MS: u64 = 30_000;

/// MCP `tools/list` entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDescriptor {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

/// MCP `tools/call` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolResult {
    pub content: Vec<Value>,
    #[serde(rename = "isError")]
    pub is_error: bool,
}

impl ToolResult {
    fn json(value: Value) -> Self {
        let text = serde_json::to_string(&value).unwrap_or_else(|_| "null".into());
        ToolResult {
            content: vec![json!({ "type": "text", "text": text })],
            is_error: false,
        }
    }

    fn error(message: impl Into<String>) -> Self {
        ToolResult {
            content: vec![json!({ "type": "text", "text": message.into() })],
            is_error: true,
        }
    }
}

/// JSON Schema for one tool's arguments, generated from its command struct.
pub fn args_schema(tool: &ToolMeta) -> Value {
    let mut schema = (tool.input_schema)();
    if let Value::Object(ref mut map) = schema
        && map.get("type").and_then(Value::as_str) == Some("object")
        && !map.contains_key("properties")
    {
        map.insert("properties".into(), Value::Object(Map::new()));
    }
    schema
}

pub struct McpServer<C: Client> {
    pub(crate) client: C,
    budget: codemode::Budget,
    pub(crate) http_token: Option<String>,
}

impl<C: Client> McpServer<C> {
    pub fn new(client: C) -> Self {
        McpServer {
            client,
            budget: codemode::Budget::default(),
            http_token: None,
        }
    }

    pub fn with_budget(client: C, budget: codemode::Budget) -> Self {
        McpServer {
            client,
            budget,
            http_token: None,
        }
    }

    pub fn with_http_token(mut self, token: impl Into<String>) -> Self {
        self.http_token = Some(token.into());
        self
    }

    /// Tools visible to this session: audience-filtered `TOOLS` plus `run_code`.
    pub fn tools(&self) -> Vec<ToolDescriptor> {
        let allowed = allowed_tools(self.client.me());
        let mut out: Vec<ToolDescriptor> = allowed
            .iter()
            .map(|tool| ToolDescriptor {
                name: tool.name.to_owned(),
                description: tool.doc.to_owned(),
                input_schema: args_schema(tool),
            })
            .collect();
        if session_allows(Audience::Member, self.client.me()) {
            out.push(run_code_descriptor(&allowed, &self.budget));
        }
        out
    }

    /// Dispatch one `tools/call`.
    pub async fn call_tool(&self, name: &str, arguments: Value) -> ToolResult {
        if name == RUN_CODE_TOOL {
            if !session_allows(Audience::Member, self.client.me()) {
                let denied = ApiError::Denied {
                    principal: self.client.me().to_string(),
                    action: format!("call {RUN_CODE_TOOL}"),
                };
                return ToolResult::error(denied.to_string());
            }
            let parsed: RunCodeArgs = match parse_run_code_args(arguments) {
                Ok(v) => v,
                Err(msg) => return ToolResult::error(msg),
            };
            let mut budget = self.budget.clone();
            budget.deadline = match parsed.timeout_ms {
                Some(ms) => Duration::from_millis(ms.clamp(1, MAX_RUN_CODE_TIMEOUT_MS)),
                None => budget
                    .deadline
                    .min(Duration::from_millis(MAX_RUN_CODE_TIMEOUT_MS)),
            };
            return match codemode::run_code(&self.client, &parsed.code, budget).await {
                Ok(outcome) => match serde_json::to_value(outcome) {
                    Ok(val) => ToolResult::json(val),
                    Err(e) => ToolResult::error(e.to_string()),
                },
                Err(err) => ToolResult::error(err.to_string()),
            };
        }

        let Some(meta) = TOOLS.iter().find(|t| t.name == name) else {
            return ToolResult::error(format!("unknown tool: {name}"));
        };
        if !session_allows(meta.audience, self.client.me()) {
            let denied = ApiError::Denied {
                principal: self.client.me().to_string(),
                action: format!("call {}", meta.name),
            };
            return ToolResult::error(denied.to_string());
        }

        let cmd = match parse_wire_command(meta, arguments) {
            Ok(c) => c,
            Err(msg) => return ToolResult::error(msg),
        };
        match self.client.call_wire(cmd).await {
            Ok(reply) => match unwrap_wire_reply(meta.name, reply) {
                Ok(val) => ToolResult::json(val),
                Err(err) => ToolResult::error(err.to_string()),
            },
            Err(err) => ToolResult::error(err.to_string()),
        }
    }
}

impl<C: Client + Clone + 'static> McpServer<C> {
    /// Serve MCP `2025-11-25` over any async duplex stream (newline JSON-RPC or `Content-Length` frames).
    pub async fn serve_stream<R, W>(self, reader: R, writer: W) -> io::Result<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        rpc::serve_stream(Arc::new(self), reader, writer).await
    }

    /// Serve MCP `2025-11-25` over process `stdin` and `stdout`.
    pub async fn serve_stdio(self) -> io::Result<()> {
        self.serve_stream(tokio::io::stdin(), tokio::io::stdout())
            .await
    }

    /// Serve MCP `2025-11-25` HTTP JSON-RPC over an already-bound loopback listener.
    pub async fn serve_http_listener(self, listener: TcpListener) -> io::Result<()> {
        if !listener.local_addr()?.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MCP HTTP server requires a loopback bind address",
            ));
        }
        rpc::serve_http_listener(Arc::new(self), listener).await
    }

    /// Serve MCP `2025-11-25` Streamable HTTP on a loopback socket.
    pub async fn serve_http(self, addr: SocketAddr) -> io::Result<()> {
        if !addr.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MCP HTTP server requires a loopback bind address",
            ));
        }
        let listener = TcpListener::bind(addr).await?;
        self.serve_http_listener(listener).await
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunCodeArgs {
    code: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

fn parse_run_code_args(arguments: Value) -> Result<RunCodeArgs, String> {
    serde_json::from_value(arguments).map_err(|e| format!("invalid run_code arguments: {e}"))
}

/// MCP sessions are members or the operator; the daemon's own parts never
/// reach tools through MCP, whatever the audience admits.
fn session_allows(audience: Audience, principal: &Principal) -> bool {
    !matches!(principal, Principal::System { .. }) && audience.allows(principal)
}

pub(crate) fn allowed_tools(principal: &Principal) -> Vec<&'static ToolMeta> {
    TOOLS
        .iter()
        .filter(|t| session_allows(t.audience, principal))
        .collect()
}

pub(crate) fn to_camel_case(snake: &str) -> String {
    let mut out = String::with_capacity(snake.len());
    let mut upper_next = false;
    for ch in snake.chars() {
        if ch == '_' {
            upper_next = true;
        } else if upper_next {
            out.extend(ch.to_uppercase());
            upper_next = false;
        } else {
            out.push(ch);
        }
    }
    out
}

pub(crate) fn parse_wire_command(
    tool: &'static ToolMeta,
    arguments: Value,
) -> Result<WireCommand, String> {
    let normalized = if arguments.is_null() {
        json!({})
    } else {
        arguments
    };
    serde_json::from_value::<WireCommand>(json!({
        "cmd": tool.name,
        "args": normalized,
    }))
    .map_err(|e| format!("invalid arguments for {}: {e}", tool.name))
}

pub(crate) fn unwrap_wire_reply(expected_tool: &str, reply: WireReply) -> Result<Value, ApiError> {
    let encoded = serde_json::to_value(reply).map_err(|e| ApiError::Internal(e.to_string()))?;
    let Value::Object(mut map) = encoded else {
        return Err(ApiError::Internal(
            "WireReply did not serialize as object".into(),
        ));
    };
    let got_tag = map.remove("reply").and_then(|v| match v {
        Value::String(s) => Some(s),
        _ => None,
    });
    if got_tag.as_deref() != Some(expected_tool) {
        return Err(ApiError::ReplyMismatch {
            expected: expected_tool.to_owned(),
            got: got_tag.unwrap_or_else(|| "<missing>".into()),
        });
    }
    Ok(map.remove("value").unwrap_or(Value::Null))
}

fn run_code_descriptor(allowed: &[&'static ToolMeta], budget: &codemode::Budget) -> ToolDescriptor {
    let default_ms = budget
        .deadline
        .as_millis()
        .min(u128::from(MAX_RUN_CODE_TIMEOUT_MS)) as u64;
    let heap_kib = budget.heap_bytes >> 10;
    let stack_kib = budget.stack_bytes >> 10;
    let ts_signatures = render_typescript_summary(allowed);
    let description = format!(
        "Execute an isolated QuickJS async script with top-level `await` and `return` ({heap_kib} KiB heap cap, {stack_kib} KiB stack cap, <=30s deadline). Frozen `globalThis.rex` exposes `whoami()` plus audience-filtered commands in both `snake_case` and `camelCase`:\n\n```ts\n{ts_signatures}\n```"
    );
    ToolDescriptor {
        name: RUN_CODE_TOOL.to_owned(),
        description,
        input_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["code"],
            "properties": {
                "code": {
                    "type": "string",
                    "description": "JavaScript async function body using `await rex.<tool>(args)` and `return`.",
                },
                "timeout_ms": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_RUN_CODE_TIMEOUT_MS,
                    "default": default_ms,
                    "description": "Hard execution deadline in milliseconds (clamped to 1..=30000).",
                }
            }
        }),
    }
}

fn render_typescript_summary(allowed: &[&'static ToolMeta]) -> String {
    let mut lines = Vec::with_capacity(allowed.len() + 3);
    lines.push("type Principal = { kind: \"operator\" | \"seat\"; seat: string } | { kind: \"worker\"; wave: string; role: string };".to_owned());
    lines.push("declare const rex: Readonly<{".to_owned());
    lines.push("  whoami(): Readonly<Principal>;".to_owned());
    for tool in allowed {
        let schema = args_schema(tool);
        let params_ts = render_params_ts(&schema);
        let camel = to_camel_case(tool.name);
        if camel == tool.name {
            lines.push(format!(
                "  {}(args{params_ts}): Promise<unknown>;",
                tool.name
            ));
        } else {
            lines.push(format!(
                "  {}(args{params_ts}): Promise<unknown>; // alias: rex.{camel}",
                tool.name
            ));
        }
    }
    lines.push("}>;".to_owned());
    lines.join("\n")
}

fn render_params_ts(schema: &Value) -> String {
    let defs = schema.get("$defs").and_then(Value::as_object);
    let Some(props) = schema.get("properties").and_then(Value::as_object) else {
        return "?: Record<string, never>".into();
    };
    if props.is_empty() {
        return "?: Record<string, never>".into();
    }
    let required: BTreeSet<&str> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|arr| arr.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();

    let mut fields = Vec::with_capacity(props.len());
    for (name, prop_schema) in props {
        let opt = if required.contains(name.as_str()) {
            ""
        } else {
            "?"
        };
        let ty = render_type_ts(prop_schema, defs, 0);
        fields.push(format!("{name}{opt}: {ty}"));
    }
    let prefix = if required.is_empty() { "?" } else { "" };
    format!("{prefix}: {{ {} }}", fields.join("; "))
}

fn render_type_ts(schema: &Value, defs: Option<&Map<String, Value>>, depth: usize) -> String {
    if depth > 2 {
        return "unknown".into();
    }
    if let Some(ref_str) = schema.get("$ref").and_then(Value::as_str) {
        let short = ref_str.rsplit('/').next().unwrap_or("unknown");
        if let Some(def_schema) = defs.and_then(|d| d.get(short))
            && def_schema.get("type").and_then(Value::as_str) == Some("string")
            && def_schema.get("enum").is_none()
        {
            return "string".into();
        }
        return short.to_owned();
    }
    if let Some(const_val) = schema.get("const") {
        return serde_json::to_string(const_val).unwrap_or_else(|_| "unknown".into());
    }
    if let Some(enum_vals) = schema.get("enum").and_then(Value::as_array) {
        let parts: Vec<String> = enum_vals
            .iter()
            .filter_map(|v| serde_json::to_string(v).ok())
            .collect();
        if !parts.is_empty() {
            return parts.join(" | ");
        }
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(variants) = schema.get(key).and_then(Value::as_array) {
            let mut parts = Vec::new();
            for v in variants {
                let rendered = render_type_ts(v, defs, depth + 1);
                if !parts.contains(&rendered) {
                    parts.push(rendered);
                }
            }
            if !parts.is_empty() {
                return parts.join(" | ");
            }
        }
    }
    match schema.get("type") {
        Some(Value::String(t)) => match t.as_str() {
            "string" => "string".into(),
            "integer" | "number" => "number".into(),
            "boolean" => "boolean".into(),
            "null" => "null".into(),
            "array" => {
                if let Some(prefix) = schema.get("prefixItems").and_then(Value::as_array) {
                    let elems: Vec<String> = prefix
                        .iter()
                        .map(|p| render_type_ts(p, defs, depth + 1))
                        .collect();
                    format!("[{}]", elems.join(", "))
                } else if let Some(items) = schema.get("items") {
                    let inner = render_type_ts(items, defs, depth + 1);
                    if inner.contains(' ') {
                        format!("Array<{inner}>")
                    } else {
                        format!("{inner}[]")
                    }
                } else {
                    "unknown[]".into()
                }
            }
            "object" => "Record<string, unknown>".into(),
            _ => "unknown".into(),
        },
        Some(Value::Array(types)) => {
            let parts: Vec<String> = types
                .iter()
                .filter_map(Value::as_str)
                .map(|t| match t {
                    "integer" => "number".to_owned(),
                    other => other.to_owned(),
                })
                .collect();
            if parts.is_empty() {
                "unknown".into()
            } else {
                parts.join(" | ")
            }
        }
        _ => "unknown".into(),
    }
}
