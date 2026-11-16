//! # rex-wave-rooms
//!
//! Public surface: [`Rooms`] (a `Decider`) and [`resolve_mentions`]. Nothing
//! else. State fields are private, so no other crate can read or write a
//! room except through commands.
//!
//! Policy owned here (and only here):
//! * channels under `Global`/`Project` scope auto-join the poster on first post;
//! * `#wave-<slug>` rooms (`RoomOrigin::Wave`) accept posts only from members
//!   (the roster, joined by the wave conductor) and the operator;
//! * DM rooms open implicitly on first post and only ever have the pair as members;
//! * `@handle` resolves against this room's members; `@all` sets `Mentions::all`.

use rex_wave_api::room::*;
use rex_wave_api::*;
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta};
use std::collections::BTreeMap;

/// The room decider. Stateless marker; state lives in [`RoomState`].
pub struct Rooms;

/// One room's folded state. Fields are private to this crate.
#[derive(Default)]
pub struct RoomState {
    opened: Option<(RoomId, RoomOrigin)>,
    topic: Option<String>,
    members: BTreeMap<Handle, Principal>,
    head: Seq,
}

/// Pure: scan `body` for `@handle` / `@all` tokens and resolve against members.
/// Unknown handles are left as text (no error: chat is weather).
pub fn resolve_mentions(body: &Body, members: &BTreeMap<Handle, Principal>) -> Mentions {
    let _ = (body, members);
    todo!("resolve_mentions: tokenize on '@', strip trailing punctuation, map handles")
}

fn default_handle(p: &Principal) -> Result<Handle, ApiError> {
    let raw = match p {
        Principal::Operator { seat } | Principal::Seat { seat } => seat.as_str().to_owned(),
        Principal::Worker { role, .. } => role.as_str().to_owned(),
        Principal::System { part } => format!("{part:?}").to_lowercase(),
    };
    Ok(Handle::parse(raw)?)
}

impl Decider for Rooms {
    type State = RoomState;
    type Cmd = RoomCommand;
    type Event = RoomEvent;

    fn address(cmd: &RoomCommand, ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        let r = match cmd {
            RoomCommand::RoomEnsure(c) => &c.room,
            RoomCommand::RoomPost(c) => &c.room,
            RoomCommand::RoomJoin(c) => &c.room,
            RoomCommand::RoomLeave(c) => &c.room,
        };
        Ok(StreamId::Room { room: r.resolve(&ctx.by)? })
    }

    fn idem(cmd: &RoomCommand) -> Option<IdemKey> {
        match cmd {
            RoomCommand::RoomPost(c) => c.idem.clone(),
            _ => None,
        }
    }

    fn decide(state: &RoomState, cmd: &RoomCommand, ctx: &DecideCtx) -> Result<Vec<RoomEvent>, ApiError> {
        let _ = (state, cmd, ctx, default_handle as fn(&Principal) -> Result<Handle, ApiError>);
        // TODO per command:
        //  Ensure: if !opened -> [Opened, MemberJoined(by)]; Wave origin requires System{WaveConductor}.
        //  Post:   implicit open for DM/channel; membership policy; resolve_mentions; [MemberJoined?, Posted].
        //  Join:   self-join always for channels; joining others requires opener/System; handle uniqueness.
        //  Leave:  only if member; DMs cannot be left (they are identity, not membership).
        todo!("Rooms::decide")
    }

    fn evolve(state: &mut RoomState, event: &RoomEvent, meta: &EntryMeta) {
        state.head = meta.seq;
        match event {
            RoomEvent::Opened { id, origin, topic } => {
                state.opened = Some((id.clone(), origin.clone()));
                state.topic = topic.clone();
            }
            RoomEvent::MemberJoined { member, handle } => {
                state.members.insert(handle.clone(), member.clone());
            }
            RoomEvent::MemberLeft { member } => state.members.retain(|_, p| p != member),
            RoomEvent::Posted { .. } => {}
            RoomEvent::TopicSet { topic } => state.topic = Some(topic.clone()),
        }
    }

    fn reply(state: &RoomState, cmd: RoomCommand, committed: &[Envelope]) -> WireReply {
        let _ = (state, cmd, committed);
        todo!("Rooms::reply: RoomInfo | Posted{{seq from committed Posted}} | Ack")
    }

    fn wrap(event: RoomEvent) -> Event {
        Event::Room(event)
    }
    fn unwrap(event: &Event) -> Option<&RoomEvent> {
        match event {
            Event::Room(e) => Some(e),
            _ => None,
        }
    }
}
