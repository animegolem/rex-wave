//! # rex-wave-api, the one contract every frontend and every actor speaks
//!
//! TUI, CLI, MCP (atomic tools *and* `run_code`), the BB bridge, a future
//! desktop GUI, and even the daemon's own reactors drive the system through
//! exactly one trait: [`Client`]. It has two required methods:
//!
//! * [`Client::call_wire`], send one [`WireCommand`], get one [`WireReply`].
//! * [`Client::watch`], subscribe to an [`Interest`] from a [`Cursor`], get a
//!   stream of [`Frame`]s (committed journal entries or live BB agent events).
//!
//! [`Client::call`] is the typed convenience over `call_wire`; it is provided
//! once here, so there is no second path to the daemon.
//!
//! Every command, its reply type, its MCP tool name, its audience, and its
//! owning [`Family`] are declared in a single list in [`registry`]. The wire
//! enums, the routing enum, the typed `Command` impls, and the MCP/BB tool
//! catalog are *generated* from that list, there is no hand-synced copy.
//!
//! Identity is never an argument: the principal comes from the session the
//! `Client` was opened with (per boundary-discipline and the mcp-room
//! feasibility finding in grounding #11).

pub mod board;
pub mod client;
pub mod id;
pub mod mail;
pub mod notify;
pub mod presence;
pub mod read;
pub mod registry;
pub mod room;
pub mod stage;
pub mod stream;
pub mod wave;

pub use client::{ApiError, Client, ConflictData, FrameStream};
pub use id::*;
pub use read::{ArchiveTarget, SetArchived};
pub use registry::{
    Audience, BoardCommand, Command, Family, InboxCommand, MailCommand, PresenceCommand,
    ReadCommand, RoomCommand, Routed, TOOLS, ToolMeta, WaveCommand, WireCommand, WireReply,
    tool_catalog,
};
pub use stage::{FocusGeneration, StageTarget};
pub use stream::{Cursor, Envelope, Event, Frame, Interest, StreamId};

/// Empty success reply for commands whose only result is "it is now true".
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct Ack;

/// Bounds `s` to at most `max_bytes` UTF-8 bytes without ever silently dropping the tail.
/// When `s.len() > max_bytes`, returns a char-boundary prefix followed by an explicit
/// `\n… [truncated: N of M bytes shown]` marker that fits inside `max_bytes`.
pub fn truncate_with_notice(s: &str, max_bytes: usize) -> String {
    let total = s.len();
    if total <= max_bytes {
        return s.to_owned();
    }
    let mut kept = max_bytes;
    for _ in 0..4 {
        let marker = format!("\n… [truncated: {kept} of {total} bytes shown]");
        let budget = max_bytes.saturating_sub(marker.len());
        let mut next_kept = budget.min(total);
        while next_kept > 0 && !s.is_char_boundary(next_kept) {
            next_kept -= 1;
        }
        let final_marker = format!("\n… [truncated: {next_kept} of {total} bytes shown]");
        if next_kept + final_marker.len() <= max_bytes {
            let mut out = String::with_capacity(next_kept + final_marker.len());
            out.push_str(&s[..next_kept]);
            out.push_str(&final_marker);
            return out;
        }
        kept = next_kept;
    }
    let short = format!("[truncated: 0 of {total} bytes shown]");
    if short.len() <= max_bytes {
        return short;
    }
    let mut end = max_bytes.min(total);
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_owned()
}
