//! # rex-wave-notify
//!
//! * [`Inbox`], decider for one principal's inbox stream. Sole owner of
//!   notifications and per-room read cursors for that principal.
//! * [`classify`] / [`rank`], pure functions from a committed envelope to the
//!   notifications it implies. All notification policy lives here.
//! * [`Herald`], reactor: watches everything, runs `classify`, files
//!   `inbox_raise` commands. Dedupe keys make re-raises (replays, repeated BB
//!   events) update a card instead of duplicating it.
//!
//! Sinks are not this crate's business: the TUI renders `Interest::MyInbox`
//! as z=-1 cards and derives OSC 7501 from it; the BB bridge may forward
//! blocking items to BB `push-notifications`. All read the same stream.

// Sketch phase: bodies are `todo!()`, so private state reads as dead code until Phase D.
#![allow(dead_code)]

use rex_wave_api::notify::*;
use rex_wave_api::*;
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta, LocalClient, Reactor};
use std::collections::BTreeMap;
use std::future::Future;

/// Pure: which notifications does this entry imply, and for whom?
/// Inputs beyond the envelope (room membership, worker roster) come from the
/// caller's derived view, passed in explicitly, no hidden lookups.
pub fn classify(entry: &Envelope, view: &HeraldView) -> Vec<(Principal, NotificationDraft)> {
    let _ = (entry, view);
    // Room Posted with mentions          -> Mention for each mentioned principal (+ members on @all)
    // Room Posted in a Direct room       -> DirectMessage for the other party
    // Wave WorkerObserved Blocked{..}    -> Interaction for the operator, actions:
    //                                       [a] wave_resolve AllowOnce, [d] wave_resolve Deny
    // Wave WorkerObserved Errored        -> WorkerStuck
    // Wave Finished                      -> WaveFinished
    // Board TaskPatched assignee=To{who} -> TaskAssigned for `who`
    // Mail Projected / Banked / BellRang -> Mail / LetterBanked / Reminder
    todo!("classify")
}

/// Pure ranking: interactions > blocking mail > DMs > mentions > the rest; decays with age.
pub fn rank(kind: &NotifyKind, age_ms: u64) -> Rank {
    let _ = (kind, age_ms);
    todo!("rank")
}

/// The Herald's derived view: room members and wave rosters, folded from frames.
#[derive(Default)]
pub struct HeraldView {
    members: BTreeMap<room::RoomId, Vec<Principal>>,
}


pub struct Inbox;

#[derive(Default)]
pub struct InboxState {
    items: BTreeMap<NotificationId, Notification>,
    by_dedupe: BTreeMap<String, NotificationId>,
    cursors: BTreeMap<room::RoomId, Seq>,
    next_id: u64,
    head: Seq,
}

impl Decider for Inbox {
    type State = InboxState;
    type Cmd = InboxCommand;
    type Event = InboxEvent;

    /// `inbox_raise` addresses `to`; everything else addresses the caller's own inbox.
    fn address(cmd: &InboxCommand, ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        let owner = match cmd {
            InboxCommand::InboxRaise(c) => c.to.clone(),
            _ => ctx.by.clone(),
        };
        Ok(StreamId::Inbox { owner })
    }

    fn idem(_cmd: &InboxCommand) -> Option<IdemKey> {
        None
    }

    fn decide(state: &InboxState, cmd: &InboxCommand, ctx: &DecideCtx) -> Result<Vec<InboxEvent>, ApiError> {
        let _ = (state, cmd, ctx);
        // Raise: dedupe hit -> Updated (or [] if identical) ; else Raised{next_id, rank()}
        // Ack:   StateSet for ids that exist and differ ([] otherwise: idempotent)
        // MarkRead: CursorAdvanced only if upto > current (monotone)
        // List:  []
        todo!("Inbox::decide")
    }

    fn evolve(state: &mut InboxState, event: &InboxEvent, meta: &EntryMeta) {
        let _ = (state, event, meta);
        todo!("Inbox::evolve")
    }

    fn reply(state: &InboxState, cmd: InboxCommand, committed: &[Envelope]) -> WireReply {
        let _ = (state, cmd, committed);
        todo!("Inbox::reply: InboxPage | Ack")
    }

    fn wrap(event: InboxEvent) -> Event {
        Event::Inbox(event)
    }
    fn unwrap(event: &Event) -> Option<&InboxEvent> {
        match event {
            Event::Inbox(e) => Some(e),
            _ => None,
        }
    }
}


pub struct Herald {
    view: HeraldView,
}

impl Herald {
    pub fn new() -> Self {
        Herald { view: HeraldView::default() }
    }
}

impl Default for Herald {
    fn default() -> Self {
        Self::new()
    }
}

impl Reactor for Herald {
    const NAME: &'static str = "herald";
    fn interest(&self) -> Interest {
        Interest::Everything
    }
    fn react(&mut self, frame: Frame, client: &LocalClient) -> impl Future<Output = Result<(), ApiError>> + Send {
        let _ = (&self.view, frame, client);
        async move { todo!("Herald::react: fold view; for each classify() result -> inbox_raise") }
    }
}
