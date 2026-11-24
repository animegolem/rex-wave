//! # rex-wave-runtime, how every subsystem becomes an actor
//!
//! A subsystem writes **one pure type**, a [`Decider`]:
//!
//! ```text
//! decide(&State, &Cmd, &Ctx) -> Result<Vec<Event>>     // pure policy
//! evolve(&mut State, &Event, &EntryMeta)               // pure fold
//! reply(&State, Cmd, &[Envelope]) -> WireReply         // pure projection
//! ```
//!
//! and gets, from [`Registry<D>`]:
//! * one tokio task per stream (room, wave, board, inbox), spawned on demand,
//!   rehydrated by replaying its journal, passivated when idle;
//! * single-writer appends through the stream's unique `StreamWriter`;
//! * live publication to the [`Hub`] after commit (journal first, doorbell second);
//! * idempotent retries via `IdemKey`.
//!
//! Side effects (spawn a BB thread, write a sandbox letter, wake an idle
//! worker) never happen in a decider. They happen in a [`Reactor`]: a task that
//! watches frames through the same `Client` API every frontend uses, performs
//! the effect idempotently, and reports back with a command.
//!
//! The [`Router`] is the only place that knows which service owns which
//! family. It is built with a struct literal, so adding a family to the
//! registry fails the build until it gets an owner. Any slot can be a
//! [`Remote`] service, which is how Kanban splits out.
//!
//! No shared state sits behind a lock here: the registry, each stream, and the
//! sidebar projection are actors that own their maps, and the stage slot is a
//! `watch` channel.

mod actor;
mod feed_router;
mod hub;
mod reactor;
mod read;
mod router;
mod sidebar;

pub use actor::Registry;
pub use feed_router::{FeedRouter, ThreadIdResolver, fallback_parked_room_and_since};
pub use hub::Hub;
pub use reactor::{Reactor, spawn_reactor};
pub use read::{
    AgentFeed, NoAgentFeed, ReadSide, can_read, can_read_visible, frame_channel,
    labeled_event_matches, labels_match, mask_unauthorized_as_not_found,
};
pub use router::{BridgeBackend, LocalClient, Remote, Router, Session, Unavailable, authorize};
pub use sidebar::SidebarProjection;

use rex_wave_api::{
    ApiError, Envelope, Event, IdemKey, Principal, Seq, StreamId, UnixMs, WireReply,
};
use std::future::Future;
use std::pin::Pin;

pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Context the router stamps on every command. Never caller-supplied:
/// only the router (and `test-support` in tests) can build one.
#[derive(Clone, Debug)]
pub struct DecideCtx {
    by: Principal,
    at: UnixMs,
}

impl DecideCtx {
    pub(crate) fn new(by: Principal, at: UnixMs) -> Self {
        Self { by, at }
    }

    /// A context for driving a decider directly in a test. Compiled out of
    /// non-test builds unless the `test-support` dev feature is enabled.
    #[cfg(any(test, feature = "test-support"))]
    pub fn for_test(by: Principal, at: UnixMs) -> Self {
        Self { by, at }
    }

    /// Who issued the command.
    pub fn by(&self) -> &Principal {
        &self.by
    }

    /// When the router received it.
    pub fn at(&self) -> UnixMs {
        self.at
    }
}

/// Journal metadata visible to `evolve`.
#[derive(Clone, Debug)]
pub struct EntryMeta {
    pub seq: Seq,
    pub at: UnixMs,
    pub by: Principal,
}

/// The one thing a subsystem implements. All methods are pure.
pub trait Decider: Send + Sync + 'static {
    type State: Default + Send + 'static;
    type Cmd: Send + 'static;
    type Event: Clone + Send + 'static;

    /// Which actor (stream) a command addresses. May use `ctx.by` (DMs, inboxes).
    fn address(cmd: &Self::Cmd, ctx: &DecideCtx) -> Result<StreamId, ApiError>;
    /// Idempotency key carried by the command, if any.
    fn idem(cmd: &Self::Cmd) -> Option<IdemKey>;
    /// Policy: validate against current state and emit events. No I/O.
    /// Returning no events is how read-only commands (views) work.
    fn decide(
        state: &Self::State,
        cmd: &Self::Cmd,
        ctx: &DecideCtx,
    ) -> Result<Vec<Self::Event>, ApiError>;
    /// Fold one committed event into state. Must be total.
    fn evolve(state: &mut Self::State, event: &Self::Event, meta: &EntryMeta);
    /// Build the reply from post-commit state and the committed envelopes
    /// (or the earlier ones, for an idempotent retry).
    fn reply(state: &Self::State, cmd: Self::Cmd, committed: &[Envelope]) -> WireReply;

    fn wrap(event: Self::Event) -> Event;
    fn unwrap(event: &Event) -> Option<&Self::Event>;
}

/// A typed, dyn-compatible owner of one command family. Local registries and
/// remote forwarders both implement it.
pub trait Service<C>: Send + Sync + 'static {
    fn dispatch(&self, cmd: C, ctx: DecideCtx) -> BoxFut<'_, Result<WireReply, ApiError>>;
}

pub fn now() -> UnixMs {
    UnixMs::now_wall()
}

fn store_error(e: rex_wave_store::StoreError) -> ApiError {
    match e {
        rex_wave_store::StoreError::HeadMoved { .. } => ApiError::conflict(e.to_string()),
        other => ApiError::Internal(other.to_string()),
    }
}
