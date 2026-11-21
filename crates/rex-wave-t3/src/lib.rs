//! T3 Code 0.1.11 host bridge (`T3Bridge`, `T3Host`, `T3Feed`) and multi-host
//! composition (`CompositeHost`, `CompositeFeed`).

pub mod composite;
pub mod discovery;
pub mod host;
pub mod projects;
pub mod protocol;
pub mod rpc;
pub mod transport;
pub mod worker_tokens;

pub use composite::{
    CompositeFeed, CompositeHost, RoutedProvider, RunnerTarget, ThreadIdResolver, route_provider,
    route_thread,
};
pub use discovery::{
    DEFAULT_CONNECT_TIMEOUT, DEFAULT_RECONNECT_MAX, DEFAULT_RECONNECT_MIN, DEFAULT_RPC_TIMEOUT,
    DEFAULT_TOKEN_LABEL, ORCHESTRATION_PROTOCOL_VERSION, ServerRuntimeInfo, T3AuthSource, T3Config,
    T3EndpointSource, T3Origin, default_bb_data_dir, default_t3_bin, issue_cli_session_token,
    runtime_file_path, state_db_path,
};
pub use host::{BridgeBackend, T3Bridge, T3Feed, T3Host};
pub use projects::{
    BbProjectLocation, ResolvedT3Project, T3ProjectInfo, load_t3_project_names_from_sqlite,
    lookup_bb_project_location, parse_t3_projects_snapshot, resolve_or_ensure_t3_project,
};
pub use protocol::{
    CompletedTurn, ITEM_SEQ_STRIDE, LiveSeqTracker, ShellObservation, T3_THREAD_PREFIX,
    TurnMirrorCheck, active_pending_interaction, delivery_intent_str, derive_shell_observation,
    deterministic_thread_id, force_stop_thread_payload, inspect_latest_completed_turn,
    is_t3_thread_id, item_event_seq, launch_thread_payload, message_dispatch_mode,
    message_dispatch_payload, mirror_idem_key, normalize_turn_item,
    parse_pending_interaction_from_projection, parse_provider_turns_usage, parse_rfc3339_ms,
    parse_t3_thread_id, parse_timestamp_ms, parse_token_usage, runtime_mode_str,
    runtime_request_respond_payload, spawn_command_id, spawn_message_id, strip_t3_provider_prefix,
    thread_archive_payload, thread_stop_payload, workspace_strategy_json,
};
pub use rpc::{
    RpcClientFrame, RpcExit, RpcServerFrame, RpcStream, RpcStreamLease, StreamEvent, T3RpcClient,
    decode_server_frames, format_effect_cause,
};
pub use worker_tokens::{
    ProvisionedWorkerToken, WORKER_TOKENS_DIR, WorkerTokenMinter, provision_worker_token,
    read_worker_token_file, remove_worker_token_file, worker_token_path, worker_tokens_dir,
    write_worker_token_record,
};
