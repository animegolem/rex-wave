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
