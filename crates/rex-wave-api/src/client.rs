//! The `Client` trait: the only way into the daemon, for every frontend and
//! for the daemon's own reactors.

use crate::id::{InvalidId, Principal};
use crate::registry::{Command, Family, WireCommand, WireReply};
use crate::stream::{Cursor, Frame, Interest};
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;

/// A live subscription. Dropping it unsubscribes (RAII), which is what makes
/// on-hover stream switching in the TUI cheap: drop the old, open the new.
pub type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, ApiError>> + Send + 'static>>;

/// Every failure a caller can see. Serializable so it crosses every transport
/// unchanged; transport-specific detail is flattened into `Transport`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "snake_case")]
pub enum ApiError {
    #[error("invalid input: {0}")]
    Invalid(InvalidId),
    #[error("{principal} may not {action}")]
    Denied { principal: String, action: String },
    #[error("not found: {what}")]
    NotFound { what: String },
    /// Optimistic-concurrency or state-machine conflict (e.g. retire a worker
    /// that was never spawned). Carries the current state for the caller.
    #[error("conflict: {detail}")]
    Conflict { detail: String },
    /// The owning service for a family is down or (after a split) unreachable.
    #[error("{family:?} service unavailable")]
    Unavailable { family: Family },
    #[error("timed out")]
    Timeout,
    #[error("transport: {0}")]
    Transport(#[serde(with = "detail")] String),
    #[error("reply mismatch: expected {expected}, got {got}")]
    ReplyMismatch { expected: String, got: String },
    #[error("internal: {0}")]
    Internal(#[serde(with = "detail")] String),
}

/// An internally tagged enum cannot carry a bare string, so the `String` of
/// `Transport` and `Internal` goes on the wire as `{"detail": ...}`.
mod detail {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize)]
    struct Out<'a> {
        detail: &'a str,
    }

    #[derive(Deserialize)]
    struct In {
        detail: String,
    }

    pub fn serialize<S: Serializer>(detail: &str, s: S) -> Result<S::Ok, S::Error> {
        Out { detail }.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
        In::deserialize(d).map(|i| i.detail)
    }
}

impl From<InvalidId> for ApiError {
    fn from(e: InvalidId) -> Self {
        ApiError::Invalid(e)
    }
}

/// The daemon API. Implemented by:
/// * `rex_wave_runtime::LocalClient`, in-process (reactors, tests, embedded GUI).
/// * `rex_wave_transport::RemoteClient`, Unix socket / WebSocket.
///
/// The session principal is fixed when the client is constructed; there is
/// deliberately no way to pass `from` per call.
pub trait Client: Send + Sync {
    /// The principal every call on this client is attributed to.
    fn me(&self) -> &Principal;

    /// Send one command and await its reply. The only required call path.
    fn call_wire(
        &self,
        cmd: WireCommand,
    ) -> impl Future<Output = Result<WireReply, ApiError>> + Send;

    /// Subscribe from `from`. The daemon guarantees *no sleep-through*: it
    /// registers the live tap first, then replays the journal after `from`,
    /// then switches to live frames, de-duplicating by `(stream, seq)`.
    fn watch(
        &self,
        interest: Interest,
        from: Cursor,
    ) -> impl Future<Output = Result<FrameStream, ApiError>> + Send;

    /// Typed convenience over [`Client::call_wire`]. Provided once, here.
    fn call<C: Command>(&self, cmd: C) -> impl Future<Output = Result<C::Reply, ApiError>> + Send
    where
        Self: Sized,
    {
        async move { C::reply_from_wire(self.call_wire(cmd.into_wire()).await?) }
    }
}
