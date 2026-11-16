//! # rex-wave-board
//!
//! Adapter turning `rex-kanban::BoardState` into a rex-wave [`Decider`].
//!
//! **Splittability contract:** the model, `CardRevision` CAS, and `.org` codec
//! live in `rex-kanban` (which imports no rex-wave crate at all), and this
//! crate's `Cargo.toml` adds only `rex-wave-api` and `rex-wave-runtime`. Waves
//! create cards by sending `card_upsert` through `Client`, just like the TUI.
//! To run Kanban as its own app, serve `Registry<Board>` behind
//! `rex-wave-transport::serve` in a second binary and swap the `board:` field
//! in `daemon.rs` to `Remote`.

#![allow(dead_code)]

use rex_wave_api::board::*;
use rex_wave_api::*;
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta};

pub struct Board;

#[derive(Default)]
pub struct BoardActorState {
    inner: rex_kanban::BoardState,
    head: Seq,
}

pub fn default_lanes() -> Vec<LaneName> {
    ["todo", "doing", "review", "done"].into_iter().map(|l| LaneName::parse(l).expect("static lane")).collect()
}

impl Decider for Board {
    type State = BoardActorState;
    type Cmd = BoardCommand;
    type Event = BoardEvent;

    fn address(cmd: &BoardCommand, _ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        let b = match cmd {
            BoardCommand::BoardEnsure(c) => &c.board,
            BoardCommand::CardUpsert(c) => &c.board,
            BoardCommand::CardArchive(c) => &c.board,
            BoardCommand::BoardView(c) => &c.board,
        };
        Ok(StreamId::Board { board: b.clone() })
    }

    fn idem(cmd: &BoardCommand) -> Option<IdemKey> {
        match cmd {
            BoardCommand::CardUpsert(c) => c.idem.as_ref().and_then(|k| IdemKey::parse(k.as_str()).ok()),
            _ => None,
        }
    }

    fn decide(state: &BoardActorState, cmd: &BoardCommand, ctx: &DecideCtx) -> Result<Vec<BoardEvent>, ApiError> {
        let _ = (&state.inner, state.head, cmd, ctx);
        todo!("Board::decide: delegate to rex_kanban::BoardState, mapping KanbanError::RevisionConflict -> ApiError::Conflict")
    }

    fn evolve(state: &mut BoardActorState, event: &BoardEvent, meta: &EntryMeta) {
        state.head = meta.seq;
        state.inner.evolve(event);
    }

    fn reply(state: &BoardActorState, cmd: BoardCommand, committed: &[Envelope]) -> WireReply {
        let _ = (state, cmd, committed);
        todo!("Board::reply: BoardInfo | CardRef | Ack | BoardSnapshot")
    }

    fn wrap(event: BoardEvent) -> Event {
        Event::Board(event)
    }

    fn unwrap(event: &Event) -> Option<&BoardEvent> {
        match event {
            Event::Board(e) => Some(e),
            _ => None,
        }
    }
}
