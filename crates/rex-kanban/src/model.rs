use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[error("invalid {what}: {reason}")]
pub struct InvalidValue {
    pub what: String,
    pub reason: String,
}

macro_rules! checked_str {
    ($(#[$m:meta])* $name:ident, $what:literal, |$s:ident| $check:expr) => {
        $(#[$m])*
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn parse(raw: impl Into<String>) -> Result<Self, InvalidValue> {
                let $s: String = raw.into();
                let verdict: Result<(), &'static str> = $check;
                verdict.map_err(|reason| InvalidValue { what: $what.into(), reason: reason.into() })?;
                Ok(Self($s))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl TryFrom<String> for $name {
            type Error = InvalidValue;
            fn try_from(s: String) -> Result<Self, InvalidValue> {
                Self::parse(s)
            }
        }
        impl From<$name> for String {
            fn from(v: $name) -> String {
                v.0
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl JsonSchema for $name {
            fn inline_schema() -> bool {
                true
            }
            fn schema_name() -> Cow<'static, str> {
                stringify!($name).into()
            }
            fn json_schema(_: &mut SchemaGenerator) -> Schema {
                json_schema!({ "type": "string", "description": $what })
            }
        }
    };
}

fn printable(s: &str, max: usize) -> Result<(), &'static str> {
    if s.is_empty() || s.chars().count() > max || s.chars().any(char::is_control) {
        return Err("empty, too long, or contains control characters");
    }
    Ok(())
}

checked_str!(
    /// Opaque board key chosen by the host (`project:p_123`, `seat:sol`).
    BoardId, "board id", |s| printable(&s, 256));
checked_str!(
    /// A lane (`todo`, `doing`, `review`, `done`, or custom): `[a-z0-9_-]{1,32}`.
    LaneName, "lane", |s| if !s.is_empty() && s.len() <= 32 && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_') { Ok(()) } else { Err("need [a-z0-9_-]{1,32}") });
checked_str!(
    /// Card title: one line, 1..=200 chars.
    Title, "title", |s| if s.contains('\n') { Err("must be one line") } else { printable(&s, 200) });
checked_str!(
    /// Who a card is assigned to, as the host renders them (`@sol`, `@auth-v2/verifier`).
    Assignee, "assignee", |s| printable(&s, 128));
checked_str!(
    /// Client-chosen key that makes a create or patch safe to retry.
    IdemKey, "idempotency key", |s| printable(&s, 256));

/// Per-board card number, assigned by the board in creation order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct CardNo(pub u32);

/// Bumped on every change to one card. A patch or archive that names an
/// `expected` revision fails with [`KanbanError::RevisionConflict`] instead
/// of overwriting a concurrent edit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct CardRevision(pub u64);

impl CardRevision {
    pub fn next(self) -> Self {
        CardRevision(self.0 + 1)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low,
    Normal,
    High,
    Blocking,
}

/// A reference into some other system. The board stores it and never
/// dereferences it; the host decides what `namespace` means.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExternalLink {
    pub namespace: String,
    pub key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "link", rename_all = "snake_case")]
pub enum Link {
    External(ExternalLink),
    /// An org-mode heading (org-todo panel): file plus outline path.
    Org { file: PathBuf, outline: Vec<String> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "assign", rename_all = "snake_case")]
pub enum Assign {
    To { who: Assignee },
    Clear,
}

/// A sparse patch. `None` leaves a field unchanged.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CardPatch {
    #[serde(default)]
    pub title: Option<Title>,
    #[serde(default)]
    pub lane: Option<LaneName>,
    /// Position within the lane (0 = top). Applied after `lane`.
    #[serde(default)]
    pub position: Option<u32>,
    #[serde(default)]
    pub assignee: Option<Assign>,
    #[serde(default)]
    pub priority: Option<Priority>,
    /// Unix milliseconds.
    #[serde(default)]
    pub due_ms: Option<u64>,
    #[serde(default)]
    pub labels_add: Vec<String>,
    #[serde(default)]
    pub labels_remove: Vec<String>,
    #[serde(default)]
    pub links_add: Vec<Link>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Card {
    pub no: CardNo,
    pub revision: CardRevision,
    pub title: Title,
    pub lane: LaneName,
    pub assignee: Option<Assignee>,
    pub priority: Priority,
    pub due_ms: Option<u64>,
    pub labels: Vec<String>,
    pub links: Vec<Link>,
}


#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct BoardEnsure {
    pub board: BoardId,
    /// Only used on first creation; default `todo, doing, review, done`.
    #[serde(default)]
    pub lanes: Option<Vec<LaneName>>,
}

/// Create a card (`card: None`, `patch.title` required) or patch one.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct CardUpsert {
    pub board: BoardId,
    #[serde(default)]
    pub card: Option<CardNo>,
    #[serde(default)]
    pub expected: Option<CardRevision>,
    pub patch: CardPatch,
    #[serde(default)]
    pub idem: Option<IdemKey>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct CardArchive {
    pub board: BoardId,
    pub card: CardNo,
    #[serde(default)]
    pub expected: Option<CardRevision>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct BoardView {
    pub board: BoardId,
    #[serde(default)]
    pub filter: Option<BoardFilter>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct BoardFilter {
    #[serde(default)]
    pub assignee: Option<Assignee>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub lane: Option<LaneName>,
}


#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct BoardInfo {
    pub board: BoardId,
    pub lanes: Vec<LaneName>,
    /// The host's position in the board's log (journal seq inside rex-wave).
    pub head: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CardRef {
    pub board: BoardId,
    pub no: CardNo,
    pub revision: CardRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct LaneCards {
    pub lane: LaneName,
    pub cards: Vec<Card>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct BoardSnapshot {
    pub board: BoardId,
    pub head: u64,
    /// Lanes in display order, each with cards in display order.
    pub lanes: Vec<LaneCards>,
}


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BoardEvent {
    Opened { board: BoardId, lanes: Vec<LaneName> },
    CardCreated { card: Card },
    CardPatched { no: CardNo, revision: CardRevision, patch: CardPatch },
    CardArchived { no: CardNo, revision: CardRevision },
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum KanbanError {
    #[error("board {0} does not exist")]
    NoSuchBoard(BoardId),
    #[error("card {0:?} does not exist")]
    NoSuchCard(CardNo),
    #[error("lane {0} does not exist")]
    NoSuchLane(LaneName),
    #[error("card {card:?} is at revision {actual:?}, not {expected:?}")]
    RevisionConflict { card: CardNo, expected: CardRevision, actual: CardRevision },
    #[error("a new card needs a title")]
    MissingTitle,
}

/// One board's state, folded from its events.
#[derive(Clone, Debug, Default)]
pub struct BoardState {
    pub board: Option<BoardId>,
    pub lanes: Vec<LaneName>,
    pub cards: BTreeMap<CardNo, Card>,
    /// Display order of live cards per lane.
    pub order: BTreeMap<LaneName, Vec<CardNo>>,
    pub archived: Vec<CardNo>,
}

impl BoardState {
    /// Policy for `CardUpsert`: CAS against `expected`, then emit
    /// `CardCreated` or `CardPatched`.
    pub fn decide_upsert(&self, cmd: &CardUpsert) -> Result<Vec<BoardEvent>, KanbanError> {
        let _ = cmd;
        todo!("BoardState::decide_upsert")
    }

    pub fn decide_archive(&self, cmd: &CardArchive) -> Result<Vec<BoardEvent>, KanbanError> {
        let _ = cmd;
        todo!("BoardState::decide_archive")
    }

    /// Total fold of one event.
    pub fn evolve(&mut self, event: &BoardEvent) {
        let _ = event;
        todo!("BoardState::evolve")
    }

    pub fn snapshot(&self, head: u64, filter: Option<&BoardFilter>) -> Option<BoardSnapshot> {
        let _ = (head, filter);
        todo!("BoardState::snapshot")
    }
}
