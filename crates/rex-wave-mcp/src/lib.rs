//! # rex-wave-mcp
//!
//! An MCP server over any `api::Client` (normally a `RemoteClient` bound to a
//! seat token). It exposes:
//!
//! * **atomic tools**, one per `rex_wave_api::TOOLS` entry the session's
//!   principal may call (Member audience; Operator tools only for operators).
//!   `tools/call {name, arguments}` becomes `WireCommand` by
//!   `{"cmd": name, "args": arguments}`, the serde tag *is* the tool name,
//!   so there is no mapping table to keep in sync;
//! * **`run_code`**, Code Mode: a JS snippet runs in a fresh, capped QuickJS
//!   runtime with a frozen `rex` global whose functions are the same tools
//!   (`await rex.room_post({...})`). See [`codemode`].
//!
//! Both paths end in `Client::call_wire`. There is one way to reach the daemon.

pub mod codemode;

use rex_wave_api::*;
use serde::Serialize;
use std::net::SocketAddr;

/// MCP `tools/list` entry.
#[derive(Clone, Debug, Serialize)]
pub struct ToolDescriptor {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: serde_json::Value,
}

/// MCP `tools/call` result.
#[derive(Clone, Debug, Serialize)]
pub struct ToolResult {
    pub content: Vec<serde_json::Value>,
    #[serde(rename = "isError")]
    pub is_error: bool,
}

/// JSON Schema for one tool's arguments, generated from its command struct.
pub fn args_schema(tool: &ToolMeta) -> serde_json::Value {
    (tool.input_schema)()
}

pub struct McpServer<C: Client> {
    client: C,
    budget: codemode::Budget,
}

impl<C: Client + Clone + 'static> McpServer<C> {
    pub fn new(client: C) -> Self {
        McpServer { client, budget: codemode::Budget::default() }
    }

    /// Tools visible to this session: filtered by audience vs `client.me()`, plus `run_code`.
    pub fn tools(&self) -> Vec<ToolDescriptor> {
        let _ = (&self.client, TOOLS);
        todo!("McpServer::tools")
    }

    /// Dispatch one `tools/call`.
    pub async fn call_tool(&self, name: &str, arguments: serde_json::Value) -> ToolResult {
        if name == "run_code" {
            let code = arguments.get("code").and_then(|v| v.as_str()).unwrap_or_default().to_owned();
            let outcome = codemode::run_code(&self.client, &code, self.budget.clone()).await;
            let _ = outcome;
            todo!("render CodeOutcome / CodeError as ToolResult")
        }
        let _ = arguments;
        // let cmd: WireCommand = serde_json::from_value(json!({"cmd": name, "args": arguments}))?;
        // self.client.call_wire(cmd).await -> ToolResult{text: json(reply)}
        todo!("call_tool: atomic")
    }

    /// MCP over stdio (JSON-RPC 2.0, newline-delimited), protocol `2025-11-25`.
    pub async fn serve_stdio(self) -> std::io::Result<()> {
        todo!("serve_stdio")
    }

    /// MCP Streamable HTTP on loopback; bearer = the same seat token.
    pub async fn serve_http(self, addr: SocketAddr) -> std::io::Result<()> {
        let _ = addr;
        todo!("serve_http")
    }
}
