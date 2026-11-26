//! Native pure-Rust ACP stdio runner (`AcpBridge`, `AcpHost`, `AcpFeed`).

pub mod config;
pub mod host;
pub mod process;
pub mod protocol;
pub mod workspace;

pub use config::{
    AcpConfig, AcpProviderSpec, DEFAULT_HANDSHAKE_TIMEOUT, DEFAULT_RETIRE_GRACE, ENV_ACP_AGY_BIN,
    ENV_ACP_OPENCODE_BIN, canonical_acp_provider,
};
pub use host::{AcpBridge, AcpFeed, AcpHost, SCRUBBED_DAEMON_ENV};
pub use protocol::{deterministic_thread_id, is_acp_thread_id, parse_acp_thread_id};
