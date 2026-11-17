//! Rooms: `#channels` (global or under a BB project) and 1:1 DMs.
//! A DM is a room whose identity is a normalized principal pair, so there is
//! exactly one DM per pair and one actor type for both.

use crate::id::{BbProjectId, ChannelName, Handle, IdemKey, InvalidId, Principal, Seq, WaveSlug};
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Where a channel lives in the sidebar tree.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum RoomScope {
    /// The `Channels` section (non-project).
    Global,
    /// Under `Projects > <project>`.
    Project { project: BbProjectId },
}

/// Canonical room identity (also the actor address). Natural keys: creating
/// `#wave-auth-v2` twice is the same room, so `room_ensure` is idempotent.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "room", rename_all = "snake_case")]
pub enum RoomId {
    Channel { scope: RoomScope, name: ChannelName },
    Direct { pair: PrincipalPair },
}

/// An unordered pair of distinct principals, stored normalized (lo < hi).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "(Principal, Principal)", into = "(Principal, Principal)")]
pub struct PrincipalPair {
    lo: Principal,
    hi: Principal,
}

impl PrincipalPair {
    pub fn new(a: Principal, b: Principal) -> Result<Self, InvalidId> {
        match a.cmp(&b) {
            std::cmp::Ordering::Less => Ok(Self { lo: a, hi: b }),
            std::cmp::Ordering::Greater => Ok(Self { lo: b, hi: a }),
            std::cmp::Ordering::Equal => Err(InvalidId {
                what: "dm".into(),
                reason: "cannot DM yourself".into(),
            }),
        }
    }
    pub fn contains(&self, p: &Principal) -> bool {
        &self.lo == p || &self.hi == p
    }
    pub fn other(&self, me: &Principal) -> Option<&Principal> {
        if &self.lo == me {
            Some(&self.hi)
        } else if &self.hi == me {
            Some(&self.lo)
        } else {
            None
        }
    }
}
impl JsonSchema for PrincipalPair {
    fn schema_name() -> Cow<'static, str> {
        "PrincipalPair".into()
    }
    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        <(Principal, Principal)>::json_schema(generator)
    }
}
impl TryFrom<(Principal, Principal)> for PrincipalPair {
    type Error = InvalidId;
    fn try_from((a, b): (Principal, Principal)) -> Result<Self, InvalidId> {
        Self::new(a, b)
    }
}
impl From<PrincipalPair> for (Principal, Principal) {
    fn from(p: PrincipalPair) -> Self {
        (p.lo, p.hi)
    }
}

/// How a caller *names* a room. Resolved to a [`RoomId`] server-side using the
/// session principal (a DM ref needs "me"). Parsed from `#general`,
/// `#proj:p_123/wave-auth-v2`, or `@sol` at the CLI/MCP boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "ref", rename_all = "snake_case")]
pub enum RoomRef {
    Id { id: RoomId },
    Channel { scope: RoomScope, name: ChannelName },
    Dm { with: Principal },
}

impl RoomRef {
    /// Pure resolution; the only place DM pairs are formed.
    pub fn resolve(&self, me: &Principal) -> Result<RoomId, InvalidId> {
        match self {
            RoomRef::Id { id } => Ok(id.clone()),
            RoomRef::Channel { scope, name } => Ok(RoomId::Channel {
                scope: scope.clone(),
                name: name.clone(),
            }),
            RoomRef::Dm { with } => Ok(RoomId::Direct {
                pair: PrincipalPair::new(me.clone(), with.clone())?,
            }),
        }
    }
}

/// Why a room exists (drives sidebar placement and wave-room policy).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "origin", rename_all = "snake_case")]
pub enum RoomOrigin {
    Manual,
    /// `#wave-<slug>`: created by the wave conductor, members = roster.
    Wave {
        wave: WaveSlug,
    },
}

/// A message body: 1..=64 KiB of text. Markdown-ish; never interpreted as commands.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Body(String);
impl JsonSchema for Body {
    fn inline_schema() -> bool {
        true
    }
    fn schema_name() -> Cow<'static, str> {
        "Body".into()
    }
    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({ "type": "string", "minLength": 1, "maxLength": 65536 })
    }
}
impl Body {
    pub fn parse(s: impl Into<String>) -> Result<Self, InvalidId> {
        let s = s.into();
        if s.is_empty() || s.len() > 64 * 1024 {
            return Err(InvalidId {
                what: "body".into(),
                reason: "1..=65536 bytes".into(),
            });
        }
        Ok(Body(s))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for Body {
    type Error = InvalidId;
    fn try_from(s: String) -> Result<Self, InvalidId> {
        Self::parse(s)
    }
}
impl From<Body> for String {
    fn from(b: Body) -> String {
        b.0
    }
}

/// Mentions as resolved by the room at post time (pure function of members + body).
/// `principals` lists every mentioned member with `@all` already expanded, so
/// deciding "does this post mention me" never needs a membership lookup.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Mentions {
    pub principals: Vec<Principal>,
    pub all: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RoomEnsure {
    pub room: RoomRef,
    #[serde(default)]
    pub topic: Option<String>,
    /// `Wave` origin may only be set by `System{WaveConductor}` (decider policy).
    #[serde(default)]
    pub origin: Option<RoomOrigin>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RoomPost {
    pub room: RoomRef,
    pub body: Body,
    #[serde(default)]
    pub reply_to: Option<Seq>,
    #[serde(default)]
    pub idem: Option<IdemKey>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RoomJoin {
    pub room: RoomRef,
    /// `None` = join yourself.
    #[serde(default)]
    pub member: Option<Principal>,
    /// Handle to be addressable by in this room; defaults to seat/role name.
    #[serde(default)]
    pub handle: Option<Handle>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RoomLeave {
    pub room: RoomRef,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RoomInfo {
    pub id: RoomId,
    pub origin: RoomOrigin,
    pub topic: Option<String>,
    pub head: Seq,
    pub members: Vec<(Handle, Principal)>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Posted {
    pub room: RoomId,
    pub seq: Seq,
    pub mentions: Mentions,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RoomEvent {
    Opened {
        id: RoomId,
        origin: RoomOrigin,
        topic: Option<String>,
    },
    MemberJoined {
        member: Principal,
        handle: Handle,
    },
    MemberLeft {
        member: Principal,
    },
    Posted {
        body: Body,
        mentions: Mentions,
        reply_to: Option<Seq>,
        idem: Option<IdemKey>,
    },
    TopicSet {
        topic: String,
    },
}
