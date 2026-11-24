use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

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

fn opaque_bytes(s: &str, max: usize) -> Result<(), &'static str> {
    if s.is_empty() || s.len() > max || s.chars().any(char::is_control) {
        return Err("empty, too long, or contains control characters");
    }
    Ok(())
}

fn org_id_valid(s: &str) -> Result<(), &'static str> {
    if s.is_empty() || s.chars().count() > 128 {
        return Err("must be 1..=128 chars");
    }
    if s.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err("must not contain whitespace or control characters");
    }
    if s.contains(':') || s.contains('[') || s.contains(']') {
        return Err("must not contain ':', '[', or ']'");
    }
    Ok(())
}

fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn fnv1a_128(bytes: &[u8]) -> u128 {
    let mut hash: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    for &b in bytes {
        hash ^= u128::from(b);
        hash = hash.wrapping_mul(0x0000_0000_0100_0000_0000_0000_0000_013b);
    }
    hash
}

static ORG_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

checked_str!(
    /// Opaque board key chosen by the host (`project:p_123`, `seat:sam`).
    BoardId, "board id", |s| printable(&s, 256));
checked_str!(
    /// Globally unique Org `:ID:` compatible with `[[id:...]]`.
    OrgId, "org id", |s| org_id_valid(&s));

impl OrgId {
    pub fn mint() -> Self {
        let seq = ORG_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let mut h1 = RandomState::new().build_hasher();
        h1.write_u32(pid);
        h1.write_u64(seq);
        let mut h2 = RandomState::new().build_hasher();
        h2.write_u64(seq.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let hi = mix64(h1.finish());
        let lo = mix64(h2.finish()) & 0xffff_ffff;
        Self(format!("rw-{hi:016x}{lo:08x}"))
    }

    pub fn mint_deterministic(seed: &str) -> Self {
        let h = fnv1a_128(seed.as_bytes());
        let folded = mix64((h as u64) ^ ((h >> 64) as u64));
        Self(format!("rw-{folded:016x}"))
    }

    pub fn for_board_card_scope(scope: &str, card: CardNo) -> Self {
        Self::mint_deterministic(&format!("rex-wave:board:{scope}:card:{}", card.0))
    }

    pub fn for_board_card(board: &BoardId, card: CardNo) -> Self {
        Self::for_board_card_scope(board.as_str(), card)
    }

    pub fn is_rex_minted(&self) -> bool {
        self.0.starts_with("rw-")
    }
}
checked_str!(
    /// A lane (`todo`, `doing`, `review`, `done`, or custom): `[a-z0-9_-]{1,32}`.
    LaneName, "lane", |s| if !s.is_empty() && s.len() <= 32 && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_') { Ok(()) } else { Err("need [a-z0-9_-]{1,32}") });
checked_str!(
    /// Card title: one line, 1..=200 chars.
    Title, "title", |s| if s.contains('\n') { Err("must be one line") } else { printable(&s, 200) });
checked_str!(
    /// Who a card is assigned to, as the host renders them (`@sam`, `@auth-v2/verifier`).
    Assignee, "assignee", |s| printable(&s, 128));
checked_str!(
    /// Client-chosen key that makes a create or patch safe to retry.
    IdemKey, "idempotency key", |s| opaque_bytes(&s, 256));

/// Canonical Kanban / Org-todo workflow states.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Lane {
    Idea,
    Todo,
    Doing,
    Review,
    Blocked,
    Done,
}

impl Lane {
    pub const ALL: [Lane; 6] = [
        Lane::Idea,
        Lane::Todo,
        Lane::Doing,
        Lane::Review,
        Lane::Blocked,
        Lane::Done,
    ];
    pub const DEFAULT_BOARD: [Lane; 4] = [Lane::Todo, Lane::Doing, Lane::Review, Lane::Done];

    pub const fn as_str(self) -> &'static str {
        match self {
            Lane::Idea => "idea",
            Lane::Todo => "todo",
            Lane::Doing => "doing",
            Lane::Review => "review",
            Lane::Blocked => "blocked",
            Lane::Done => "done",
        }
    }

    pub const fn org_keyword(self) -> &'static str {
        match self {
            Lane::Idea => "IDEA",
            Lane::Todo => "TODO",
            Lane::Doing => "STRT",
            Lane::Review => "WAIT",
            Lane::Blocked => "BLKD",
            Lane::Done => "DONE",
        }
    }

    pub fn from_org_keyword(keyword: &str) -> Option<Self> {
        match keyword {
            "IDEA" => Some(Lane::Idea),
            "TODO" => Some(Lane::Todo),
            "STRT" => Some(Lane::Doing),
            "WAIT" => Some(Lane::Review),
            "BLKD" => Some(Lane::Blocked),
            "DONE" => Some(Lane::Done),
            _ => None,
        }
    }

    pub fn from_lane_name(name: &LaneName) -> Option<Self> {
        match name.as_str() {
            "idea" => Some(Lane::Idea),
            "todo" => Some(Lane::Todo),
            "doing" => Some(Lane::Doing),
            "review" => Some(Lane::Review),
            "blocked" => Some(Lane::Blocked),
            "done" => Some(Lane::Done),
            _ => None,
        }
    }

    pub fn lane_name(self) -> LaneName {
        LaneName(self.as_str().to_owned())
    }

    pub const fn is_done(self) -> bool {
        matches!(self, Lane::Done)
    }

    pub fn all_lane_names() -> Vec<LaneName> {
        Self::ALL.into_iter().map(Self::lane_name).collect()
    }

    pub fn default_board_lanes() -> Vec<LaneName> {
        Self::DEFAULT_BOARD
            .into_iter()
            .map(Self::lane_name)
            .collect()
    }
}

impl From<Lane> for LaneName {
    fn from(lane: Lane) -> Self {
        lane.lane_name()
    }
}

impl TryFrom<&LaneName> for Lane {
    type Error = InvalidValue;

    fn try_from(value: &LaneName) -> Result<Self, Self::Error> {
        Self::from_lane_name(value).ok_or_else(|| InvalidValue {
            what: "canonical lane".into(),
            reason: format!("unknown canonical lane `{value}`"),
        })
    }
}

impl fmt::Display for Lane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Org-mode priority cookies (`[#A]`, `[#B]`, `[#C]`).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub enum OrgPriority {
    A,
    B,
    C,
}

impl OrgPriority {
    pub const fn letter(self) -> char {
        match self {
            OrgPriority::A => 'A',
            OrgPriority::B => 'B',
            OrgPriority::C => 'C',
        }
    }

    pub const fn cookie(self) -> &'static str {
        match self {
            OrgPriority::A => "[#A]",
            OrgPriority::B => "[#B]",
            OrgPriority::C => "[#C]",
        }
    }

    pub fn parse_cookie(token: &str) -> Option<Self> {
        match token {
            "[#A]" => Some(OrgPriority::A),
            "[#B]" => Some(OrgPriority::B),
            "[#C]" => Some(OrgPriority::C),
            _ => None,
        }
    }

    pub const fn from_priority(priority: Priority) -> Option<Self> {
        match priority {
            Priority::Blocking | Priority::High => Some(OrgPriority::A),
            Priority::Normal => None,
            Priority::Low => Some(OrgPriority::C),
        }
    }
}

impl From<OrgPriority> for Priority {
    fn from(priority: OrgPriority) -> Self {
        match priority {
            OrgPriority::A => Priority::High,
            OrgPriority::B => Priority::Normal,
            OrgPriority::C => Priority::Low,
        }
    }
}

impl fmt::Display for OrgPriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.cookie())
    }
}

/// Per-board card number, assigned by the board in creation order.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct CardNo(pub u32);

/// Bumped on every change to one card. A patch or archive that names an
/// `expected` revision fails with [`KanbanError::RevisionConflict`] instead
/// of overwriting a concurrent edit.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(transparent)]
pub struct CardRevision(pub u64);

impl CardRevision {
    pub fn next(self) -> Self {
        CardRevision(self.0 + 1)
    }
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
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

impl ExternalLink {
    pub fn new(namespace: impl Into<String>, key: impl Into<String>) -> Result<Self, InvalidValue> {
        let namespace = namespace.into();
        let key = key.into();
        if namespace.is_empty() || namespace.chars().any(char::is_control) {
            return Err(InvalidValue {
                what: "external link namespace".into(),
                reason: "empty or contains control characters".into(),
            });
        }
        if key.is_empty() || key.chars().any(char::is_control) {
            return Err(InvalidValue {
                what: "external link key".into(),
                reason: "empty or contains control characters".into(),
            });
        }
        Ok(Self { namespace, key })
    }

    /// Parses opaque URIs such as `rex://room/general#4`, `letter://sha256`, or `wave:auth-v2/verifier`.
    pub fn parse_uri(raw: &str) -> Result<Self, InvalidValue> {
        if raw.is_empty() || raw.chars().any(char::is_control) {
            return Err(InvalidValue {
                what: "external link".into(),
                reason: "empty or contains control characters".into(),
            });
        }
        let Some((scheme, rest)) = raw.split_once(':') else {
            return Err(InvalidValue {
                what: "external link".into(),
                reason: "missing scheme separator `:`".into(),
            });
        };
        let valid_scheme = !scheme.is_empty()
            && scheme.as_bytes()[0].is_ascii_alphabetic()
            && scheme
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.' | b'_'));
        if !valid_scheme {
            return Err(InvalidValue {
                what: "external link".into(),
                reason: "invalid URI scheme".into(),
            });
        }
        if rest.is_empty() || rest == "//" {
            return Err(InvalidValue {
                what: "external link".into(),
                reason: "empty URI target".into(),
            });
        }
        Ok(Self {
            namespace: scheme.to_owned(),
            key: rest.to_owned(),
        })
    }

    pub fn to_uri(&self) -> String {
        format!("{}:{}", self.namespace, self.key)
    }

    pub fn scheme(&self) -> &str {
        &self.namespace
    }
}

impl fmt::Display for ExternalLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.namespace, self.key)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "link", rename_all = "snake_case")]
pub enum Link {
    External(ExternalLink),
    /// An org-mode heading (org-todo panel): file plus outline path.
    Org {
        file: PathBuf,
        outline: Vec<String>,
    },
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<OrgId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs_add: Vec<OrgId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs_remove: Vec<OrgId>,
}

impl CardPatch {
    pub fn is_noop(&self) -> bool {
        self.title.is_none()
            && self.lane.is_none()
            && self.position.is_none()
            && self.assignee.is_none()
            && self.priority.is_none()
            && self.due_ms.is_none()
            && self.labels_add.is_empty()
            && self.labels_remove.is_empty()
            && self.links_add.is_empty()
            && self.org_id.is_none()
            && self.needs_add.is_empty()
            && self.needs_remove.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Card {
    pub no: CardNo,
    pub revision: CardRevision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<OrgId>,
    pub title: Title,
    pub lane: LaneName,
    pub assignee: Option<Assignee>,
    pub priority: Priority,
    pub due_ms: Option<u64>,
    pub labels: Vec<String>,
    pub links: Vec<Link>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs: Vec<OrgId>,
}

/// Full Org-backed task representation preserving body notes and unknown `:PROPERTIES:` keys.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCard {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<OrgId>,
    pub id: Option<CardNo>,
    pub revision: Option<CardRevision>,
    pub lane: LaneName,
    pub priority: Option<OrgPriority>,
    pub blocking: bool,
    pub title: Title,
    pub tags: Vec<String>,
    pub assignee: Option<Assignee>,
    pub due_ms: Option<u64>,
    pub links: Vec<Link>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs: Vec<OrgId>,
    pub properties: Vec<(String, String)>,
    pub body: String,
}

impl TaskCard {
    pub fn new(lane: Lane, title: Title) -> Self {
        Self {
            org_id: None,
            id: None,
            revision: None,
            lane: lane.lane_name(),
            priority: None,
            blocking: false,
            title,
            tags: Vec::new(),
            assignee: None,
            due_ms: None,
            links: Vec::new(),
            needs: Vec::new(),
            properties: Vec::new(),
            body: String::new(),
        }
    }

    pub fn canonical_lane(&self) -> Option<Lane> {
        Lane::from_lane_name(&self.lane)
    }

    pub fn effective_priority(&self) -> Priority {
        if self.blocking {
            Priority::Blocking
        } else {
            self.priority.map_or(Priority::Normal, Priority::from)
        }
    }

    pub fn to_card(&self, fallback_no: CardNo) -> Card {
        Card {
            no: self.id.unwrap_or(fallback_no),
            revision: self.revision.unwrap_or(CardRevision(1)),
            org_id: self.org_id.clone(),
            title: self.title.clone(),
            lane: self.lane.clone(),
            assignee: self.assignee.clone(),
            priority: self.effective_priority(),
            due_ms: self.due_ms,
            labels: self.tags.clone(),
            links: self.links.clone(),
            needs: self.needs.clone(),
        }
    }
}

impl From<&Card> for TaskCard {
    fn from(card: &Card) -> Self {
        Self {
            org_id: card.org_id.clone(),
            id: Some(card.no),
            revision: Some(card.revision),
            lane: card.lane.clone(),
            priority: OrgPriority::from_priority(card.priority),
            blocking: card.priority == Priority::Blocking,
            title: card.title.clone(),
            tags: card.labels.clone(),
            assignee: card.assignee.clone(),
            due_ms: card.due_ms,
            links: card.links.clone(),
            needs: card.needs.clone(),
            properties: Vec::new(),
            body: String::new(),
        }
    }
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LaneCards {
    pub lane: LaneName,
    pub cards: Vec<Card>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BoardSnapshot {
    pub board: BoardId,
    pub head: u64,
    /// Lanes in display order, each with cards in display order.
    pub lanes: Vec<LaneCards>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BoardEvent {
    Opened {
        board: BoardId,
        lanes: Vec<LaneName>,
    },
    CardCreated {
        card: Card,
    },
    CardPatched {
        no: CardNo,
        revision: CardRevision,
        patch: CardPatch,
    },
    CardArchived {
        no: CardNo,
        revision: CardRevision,
    },
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
    RevisionConflict {
        card: CardNo,
        expected: CardRevision,
        actual: CardRevision,
    },
    #[error("a new card needs a title")]
    MissingTitle,
    #[error("org id `{0}` is already used by another card on this board")]
    DuplicateOrgId(OrgId),
}

fn dedup_vec<T: PartialEq + Clone>(items: impl IntoIterator<Item = T>) -> Vec<T> {
    let mut out = Vec::new();
    for item in items {
        if !out.contains(&item) {
            out.push(item);
        }
    }
    out
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
    pub fn open(board: BoardId, lanes: Vec<LaneName>) -> Self {
        let mut state = Self::default();
        state.evolve(&BoardEvent::Opened { board, lanes });
        state
    }

    pub fn decide_ensure(&self, cmd: &BoardEnsure) -> Result<Vec<BoardEvent>, KanbanError> {
        if let Some(existing) = &self.board {
            if existing != &cmd.board {
                return Err(KanbanError::NoSuchBoard(cmd.board.clone()));
            }
            return Ok(Vec::new());
        }
        let lanes = match &cmd.lanes {
            Some(requested) => {
                let unique = dedup_vec(requested.iter().cloned());
                if unique.is_empty() {
                    Lane::default_board_lanes()
                } else {
                    unique
                }
            }
            None => Lane::default_board_lanes(),
        };
        Ok(vec![BoardEvent::Opened {
            board: cmd.board.clone(),
            lanes,
        }])
    }

    /// Policy for `CardUpsert`: CAS against `expected`, then emit
    /// `CardCreated` or `CardPatched`.
    pub fn decide_upsert(&self, cmd: &CardUpsert) -> Result<Vec<BoardEvent>, KanbanError> {
        match cmd.card {
            None => {
                if let Some(existing) = &self.board
                    && existing != &cmd.board
                {
                    return Err(KanbanError::NoSuchBoard(cmd.board.clone()));
                }
                if let Some(expected) = cmd.expected
                    && expected != CardRevision(0)
                {
                    return Err(KanbanError::RevisionConflict {
                        card: CardNo(0),
                        expected,
                        actual: CardRevision(0),
                    });
                }
                let Some(title) = cmd.patch.title.clone() else {
                    return Err(KanbanError::MissingTitle);
                };

                let mut events = Vec::new();
                let effective_lanes = if self.board.is_some() {
                    self.lanes.clone()
                } else {
                    let lanes = Lane::default_board_lanes();
                    events.push(BoardEvent::Opened {
                        board: cmd.board.clone(),
                        lanes: lanes.clone(),
                    });
                    lanes
                };

                let lane = match &cmd.patch.lane {
                    Some(lane) => {
                        if !effective_lanes.contains(lane) {
                            return Err(KanbanError::NoSuchLane(lane.clone()));
                        }
                        lane.clone()
                    }
                    None => effective_lanes
                        .first()
                        .cloned()
                        .ok_or_else(|| KanbanError::NoSuchLane(Lane::Todo.lane_name()))?,
                };

                let next_no = CardNo(self.max_card_no().saturating_add(1));
                let assignee = match &cmd.patch.assignee {
                    Some(Assign::To { who }) => Some(who.clone()),
                    Some(Assign::Clear) | None => None,
                };
                let mut labels = dedup_vec(cmd.patch.labels_add.iter().cloned());
                if !cmd.patch.labels_remove.is_empty() {
                    labels.retain(|l| !cmd.patch.labels_remove.contains(l));
                }
                let links = dedup_vec(cmd.patch.links_add.iter().cloned());
                let mut needs = dedup_vec(cmd.patch.needs_add.iter().cloned());
                if !cmd.patch.needs_remove.is_empty() {
                    needs.retain(|n| !cmd.patch.needs_remove.contains(n));
                }
                let org_id = cmd
                    .patch
                    .org_id
                    .clone()
                    .unwrap_or_else(|| OrgId::for_board_card(&cmd.board, next_no));
                if self
                    .cards
                    .values()
                    .any(|c| c.org_id.as_ref() == Some(&org_id))
                {
                    return Err(KanbanError::DuplicateOrgId(org_id));
                }

                let card = Card {
                    no: next_no,
                    revision: CardRevision(1),
                    org_id: Some(org_id),
                    title,
                    lane: lane.clone(),
                    assignee,
                    priority: cmd.patch.priority.unwrap_or(Priority::Normal),
                    due_ms: cmd.patch.due_ms,
                    labels,
                    links,
                    needs,
                };
                let existing_in_lane = self.order.get(&lane).map_or(0, Vec::len);
                events.push(BoardEvent::CardCreated { card });
                if let Some(pos) = cmd.patch.position
                    && (pos as usize) < existing_in_lane
                {
                    events.push(BoardEvent::CardPatched {
                        no: next_no,
                        revision: CardRevision(1),
                        patch: CardPatch {
                            position: Some(pos),
                            ..CardPatch::default()
                        },
                    });
                }
                Ok(events)
            }
            Some(no) => {
                if self.board.as_ref() != Some(&cmd.board) {
                    return Err(KanbanError::NoSuchBoard(cmd.board.clone()));
                }
                let Some(card) = self.cards.get(&no) else {
                    return Err(KanbanError::NoSuchCard(no));
                };
                if let Some(expected) = cmd.expected
                    && expected != card.revision
                {
                    return Err(KanbanError::RevisionConflict {
                        card: no,
                        expected,
                        actual: card.revision,
                    });
                }
                if let Some(lane) = &cmd.patch.lane
                    && !self.lanes.contains(lane)
                {
                    return Err(KanbanError::NoSuchLane(lane.clone()));
                }
                if let Some(org_id) = &cmd.patch.org_id
                    && self
                        .cards
                        .iter()
                        .any(|(&other_no, c)| other_no != no && c.org_id.as_ref() == Some(org_id))
                {
                    return Err(KanbanError::DuplicateOrgId(org_id.clone()));
                }
                if cmd.patch.is_noop() {
                    return Ok(Vec::new());
                }
                Ok(vec![BoardEvent::CardPatched {
                    no,
                    revision: card.revision.next(),
                    patch: cmd.patch.clone(),
                }])
            }
        }
    }

    pub fn decide_archive(&self, cmd: &CardArchive) -> Result<Vec<BoardEvent>, KanbanError> {
        if self.board.as_ref() != Some(&cmd.board) {
            return Err(KanbanError::NoSuchBoard(cmd.board.clone()));
        }
        let Some(card) = self.cards.get(&cmd.card) else {
            if cmd.expected.is_none() && self.archived.contains(&cmd.card) {
                return Ok(Vec::new());
            }
            return Err(KanbanError::NoSuchCard(cmd.card));
        };
        if let Some(expected) = cmd.expected
            && expected != card.revision
        {
            return Err(KanbanError::RevisionConflict {
                card: cmd.card,
                expected,
                actual: card.revision,
            });
        }
        Ok(vec![BoardEvent::CardArchived {
            no: cmd.card,
            revision: card.revision.next(),
        }])
    }

    /// Total fold of one event.
    pub fn evolve(&mut self, event: &BoardEvent) {
        match event {
            BoardEvent::Opened { board, lanes } => {
                self.board = Some(board.clone());
                self.lanes = dedup_vec(lanes.iter().cloned());
                for lane in &self.lanes {
                    self.order.entry(lane.clone()).or_default();
                }
            }
            BoardEvent::CardCreated { card } => {
                if !self.lanes.contains(&card.lane) {
                    self.lanes.push(card.lane.clone());
                }
                for list in self.order.values_mut() {
                    list.retain(|&c| c != card.no);
                }
                self.order
                    .entry(card.lane.clone())
                    .or_default()
                    .push(card.no);
                self.cards.insert(card.no, card.clone());
            }
            BoardEvent::CardPatched {
                no,
                revision,
                patch,
            } => {
                let Some(card) = self.cards.get_mut(no) else {
                    return;
                };
                let old_lane = card.lane.clone();
                card.revision = *revision;
                if let Some(title) = &patch.title {
                    card.title = title.clone();
                }
                if let Some(assign) = &patch.assignee {
                    card.assignee = match assign {
                        Assign::To { who } => Some(who.clone()),
                        Assign::Clear => None,
                    };
                }
                if let Some(priority) = patch.priority {
                    card.priority = priority;
                }
                if let Some(due_ms) = patch.due_ms {
                    card.due_ms = Some(due_ms);
                }
                if !patch.labels_remove.is_empty() {
                    card.labels.retain(|l| !patch.labels_remove.contains(l));
                }
                for label in &patch.labels_add {
                    if !card.labels.contains(label) {
                        card.labels.push(label.clone());
                    }
                }
                for link in &patch.links_add {
                    if !card.links.contains(link) {
                        card.links.push(link.clone());
                    }
                }
                if let Some(org_id) = &patch.org_id {
                    card.org_id = Some(org_id.clone());
                }
                if !patch.needs_remove.is_empty() {
                    card.needs.retain(|n| !patch.needs_remove.contains(n));
                }
                for dep in &patch.needs_add {
                    if !card.needs.contains(dep) {
                        card.needs.push(dep.clone());
                    }
                }

                let target_lane = patch.lane.clone().unwrap_or_else(|| old_lane.clone());
                let lane_changed = target_lane != old_lane;
                if lane_changed {
                    if !self.lanes.contains(&target_lane) {
                        self.lanes.push(target_lane.clone());
                    }
                    card.lane = target_lane.clone();
                }
                if lane_changed || patch.position.is_some() {
                    if let Some(old_list) = self.order.get_mut(&old_lane) {
                        old_list.retain(|&c| c != *no);
                    }
                    let list = self.order.entry(target_lane).or_default();
                    list.retain(|&c| c != *no);
                    match patch.position {
                        Some(pos) => {
                            let idx = (pos as usize).min(list.len());
                            list.insert(idx, *no);
                        }
                        None => list.push(*no),
                    }
                }
            }
            BoardEvent::CardArchived { no, .. } => {
                if let Some(card) = self.cards.remove(no)
                    && let Some(list) = self.order.get_mut(&card.lane)
                {
                    list.retain(|&c| c != *no);
                }
                if !self.archived.contains(no) {
                    self.archived.push(*no);
                }
            }
        }
    }

    pub fn snapshot(&self, head: u64, filter: Option<&BoardFilter>) -> Option<BoardSnapshot> {
        let board = self.board.clone()?;
        let mut lanes = Vec::new();
        for lane in &self.lanes {
            if let Some(f) = filter
                && let Some(wanted_lane) = &f.lane
                && wanted_lane != lane
            {
                continue;
            }
            let mut cards = Vec::new();
            if let Some(order) = self.order.get(lane) {
                for no in order {
                    let Some(card) = self.cards.get(no) else {
                        continue;
                    };
                    if let Some(f) = filter {
                        if let Some(who) = &f.assignee
                            && card.assignee.as_ref() != Some(who)
                        {
                            continue;
                        }
                        if let Some(label) = &f.label
                            && !card.labels.contains(label)
                        {
                            continue;
                        }
                    }
                    cards.push(card.clone());
                }
            }
            lanes.push(LaneCards {
                lane: lane.clone(),
                cards,
            });
        }
        Some(BoardSnapshot { board, head, lanes })
    }

    fn max_card_no(&self) -> u32 {
        let live_max = self.cards.keys().next_back().map_or(0, |c| c.0);
        let arch_max = self.archived.iter().map(|c| c.0).max().unwrap_or(0);
        live_max.max(arch_max)
    }
}

/// Standalone embeddable in-memory Kanban engine with per-board event logs and `IdemKey` deduplication.
#[derive(Clone, Debug, Default)]
pub struct KanbanEngine {
    boards: BTreeMap<BoardId, ManagedBoard>,
}

#[derive(Clone, Debug, Default)]
struct ManagedBoard {
    state: BoardState,
    events: Vec<BoardEvent>,
    idem_receipts: BTreeMap<IdemKey, CardRef>,
}

impl KanbanEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ensure_board(&mut self, cmd: BoardEnsure) -> Result<BoardInfo, KanbanError> {
        let entry = self.boards.entry(cmd.board.clone()).or_default();
        let events = entry.state.decide_ensure(&cmd)?;
        entry.apply(events);
        Ok(BoardInfo {
            board: cmd.board,
            lanes: entry.state.lanes.clone(),
            head: entry.events.len() as u64,
        })
    }

    pub fn upsert_card(&mut self, cmd: CardUpsert) -> Result<CardRef, KanbanError> {
        if let Some(key) = &cmd.idem
            && let Some(existing) = self
                .boards
                .get(&cmd.board)
                .and_then(|b| b.idem_receipts.get(key))
        {
            return Ok(existing.clone());
        }
        if cmd.card.is_some() && !self.boards.contains_key(&cmd.board) {
            return Err(KanbanError::NoSuchBoard(cmd.board));
        }
        let entry = self.boards.entry(cmd.board.clone()).or_default();
        let events = entry.state.decide_upsert(&cmd)?;
        let receipt = receipt_from_events_or_state(&cmd, &events, &entry.state);
        entry.apply(events);
        let receipt = receipt.unwrap_or_else(|| {
            let no = cmd.card.expect("card validated");
            CardRef {
                board: cmd.board.clone(),
                no,
                revision: entry.state.cards[&no].revision,
            }
        });
        if let Some(key) = cmd.idem {
            entry.idem_receipts.insert(key, receipt.clone());
        }
        Ok(receipt)
    }

    pub fn move_card(
        &mut self,
        board: BoardId,
        card: CardNo,
        expected: Option<CardRevision>,
        lane: LaneName,
        position: Option<u32>,
        links_add: Vec<Link>,
    ) -> Result<CardRef, KanbanError> {
        self.upsert_card(CardUpsert {
            board,
            card: Some(card),
            expected,
            patch: CardPatch {
                lane: Some(lane),
                position,
                links_add,
                ..CardPatch::default()
            },
            idem: None,
        })
    }

    pub fn archive_card(&mut self, cmd: CardArchive) -> Result<(), KanbanError> {
        let Some(entry) = self.boards.get_mut(&cmd.board) else {
            return Err(KanbanError::NoSuchBoard(cmd.board));
        };
        let events = entry.state.decide_archive(&cmd)?;
        entry.apply(events);
        Ok(())
    }

    pub fn view_board(&self, cmd: BoardView) -> Result<BoardSnapshot, KanbanError> {
        let Some(entry) = self.boards.get(&cmd.board) else {
            return Err(KanbanError::NoSuchBoard(cmd.board));
        };
        entry
            .state
            .snapshot(entry.events.len() as u64, cmd.filter.as_ref())
            .ok_or(KanbanError::NoSuchBoard(cmd.board))
    }

    pub fn events(&self, board: &BoardId) -> Option<&[BoardEvent]> {
        self.boards.get(board).map(|b| b.events.as_slice())
    }
}

impl ManagedBoard {
    fn apply(&mut self, events: Vec<BoardEvent>) {
        for event in events {
            self.state.evolve(&event);
            self.events.push(event);
        }
    }
}

fn receipt_from_events_or_state(
    cmd: &CardUpsert,
    events: &[BoardEvent],
    state: &BoardState,
) -> Option<CardRef> {
    for event in events.iter().rev() {
        match event {
            BoardEvent::CardCreated { card } => {
                return Some(CardRef {
                    board: cmd.board.clone(),
                    no: card.no,
                    revision: card.revision,
                });
            }
            BoardEvent::CardPatched { no, revision, .. } => {
                return Some(CardRef {
                    board: cmd.board.clone(),
                    no: *no,
                    revision: *revision,
                });
            }
            BoardEvent::Opened { .. } | BoardEvent::CardArchived { .. } => {}
        }
    }
    let no = cmd.card?;
    let card = state.cards.get(&no)?;
    Some(CardRef {
        board: cmd.board.clone(),
        no,
        revision: card.revision,
    })
}
