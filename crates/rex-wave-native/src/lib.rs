//! Pure-Rust native stdio runners for Claude (`stream-json`) and Codex (`app-server`).

pub mod claude;
pub mod codex;
pub mod common;
pub mod process;
pub mod workspace;

pub use claude::{
    CLAUDE_BIN_ENV, ClaudeBridge, ClaudeFeed, ClaudeHost, ClaudeRunnerConfig, build_claude_args,
    classify_claude_result, claude_permission_mode, claude_thread_id,
    encode_claude_control_response, encode_claude_user_turn, parse_claude_thread_id,
    parse_claude_usage,
};
pub use codex::{
    CODEX_BIN_ENV, CodexBridge, CodexFeed, CodexHost, CodexRunnerConfig, classify_codex_turn,
    codex_approval_and_sandbox, codex_thread_id, encode_codex_approval_result,
    parse_codex_approval_request, parse_codex_thread_id, parse_codex_token_usage,
};
pub use process::{
    DEFAULT_RETIRE_GRACE, LiveChild, kill_process_group_now, process_group_alive,
    spawn_in_process_group, terminate_process_group,
};
pub use workspace::resolve_worker_workspace;
