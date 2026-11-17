//! Kanban and Org-todo [`Decider`] backed by standalone `rex-kanban`.

use rex_kanban::{KanbanError, Lane};
use rex_wave_api::board::*;
use rex_wave_api::*;
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta};

pub struct Board;

#[derive(Default)]
pub struct BoardActorState {
    inner: rex_kanban::BoardState,
    head: Seq,
}

impl BoardActorState {
    pub fn inner(&self) -> &rex_kanban::BoardState {
        &self.inner
    }

    pub fn head(&self) -> Seq {
        self.head
    }
}

pub fn default_lanes() -> Vec<LaneName> {
    Lane::default_board_lanes()
}

/// Build a `CardUpsert` command that moves `card` to `lane`, optionally ranking
/// it at `position` and attaching `links_add`.
pub fn task_move(
    board: BoardId,
    card: CardNo,
    expected: Option<CardRevision>,
    lane: LaneName,
    position: Option<u32>,
    links_add: Vec<Link>,
    idem: Option<rex_kanban::IdemKey>,
) -> CardUpsert {
    CardUpsert {
        board,
        card: Some(card),
        expected,
        patch: CardPatch {
            lane: Some(lane),
            position,
            links_add,
            ..CardPatch::default()
        },
        idem,
    }
}

fn map_kanban_error(err: KanbanError) -> ApiError {
    match err {
        KanbanError::NoSuchBoard(_) | KanbanError::NoSuchCard(_) | KanbanError::NoSuchLane(_) => {
            ApiError::NotFound {
                what: err.to_string(),
            }
        }
        KanbanError::RevisionConflict { .. } => ApiError::Conflict {
            detail: err.to_string(),
        },
        KanbanError::MissingTitle => ApiError::Invalid(InvalidId {
            what: "title".into(),
            reason: err.to_string(),
        }),
    }
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
            BoardCommand::CardUpsert(c) => c
                .idem
                .as_ref()
                .and_then(|k| IdemKey::parse(k.as_str()).ok()),
            _ => None,
        }
    }

    fn decide(
        state: &BoardActorState,
        cmd: &BoardCommand,
        _ctx: &DecideCtx,
    ) -> Result<Vec<BoardEvent>, ApiError> {
        match cmd {
            BoardCommand::BoardEnsure(c) => state.inner.decide_ensure(c).map_err(map_kanban_error),
            BoardCommand::CardUpsert(c) => state.inner.decide_upsert(c).map_err(map_kanban_error),
            BoardCommand::CardArchive(c) => state.inner.decide_archive(c).map_err(map_kanban_error),
            BoardCommand::BoardView(c) => {
                if state.inner.board.as_ref() != Some(&c.board) {
                    return Err(map_kanban_error(KanbanError::NoSuchBoard(c.board.clone())));
                }
                Ok(Vec::new())
            }
        }
    }

    fn evolve(state: &mut BoardActorState, event: &BoardEvent, meta: &EntryMeta) {
        state.head = meta.seq;
        state.inner.evolve(event);
    }

    fn reply(state: &BoardActorState, cmd: BoardCommand, committed: &[Envelope]) -> WireReply {
        match cmd {
            BoardCommand::BoardEnsure(c) => WireReply::BoardEnsure(BoardInfo {
                board: c.board,
                lanes: state.inner.lanes.clone(),
                head: state.head.0,
            }),
            BoardCommand::CardUpsert(c) => {
                let (no, revision) = committed
                    .iter()
                    .rev()
                    .find_map(|env| match &env.event {
                        Event::Board(BoardEvent::CardCreated { card }) => {
                            Some((card.no, card.revision))
                        }
                        Event::Board(BoardEvent::CardPatched { no, revision, .. }) => {
                            Some((*no, *revision))
                        }
                        _ => None,
                    })
                    .unwrap_or_else(|| {
                        let no = c.card.expect("noop patch verified card id");
                        (no, state.inner.cards[&no].revision)
                    });
                WireReply::CardUpsert(CardRef {
                    board: c.board,
                    no,
                    revision,
                })
            }
            BoardCommand::CardArchive(_) => WireReply::CardArchive(Ack),
            BoardCommand::BoardView(c) => {
                let snapshot = state
                    .inner
                    .snapshot(state.head.0, c.filter.as_ref())
                    .expect("Board::decide verified board is open");
                WireReply::BoardView(snapshot)
            }
        }
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
