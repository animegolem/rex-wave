//! Rooms: `#channels` (global or under a BB project) and 1:1 DMs.
//! A DM is a room whose identity is a normalized principal pair, so there is
//! exactly one DM per pair and one actor type for both.

use crate::board::{Priority, Title};
use crate::id::{
    BbProjectId, ChannelName, Handle, IdemKey, InvalidId, Label, LeaseEpoch, OrgId, Principal, Seq,
    UnixMs, WaveSlug,
};
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
    /// The other side of this pair as `viewer` sees it. `Operator { seat }` and
    /// `Seat { seat }` share the same `@<seat>` DM identity.
    pub fn other_for_viewer(&self, viewer: &Principal) -> Option<&Principal> {
        if self.lo.same_actor(viewer) && !self.hi.same_actor(viewer) {
            Some(&self.hi)
        } else if self.hi.same_actor(viewer) && !self.lo.same_actor(viewer) {
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
/// `#proj:p_123/wave-auth-v2`, or `@sam` at the CLI/MCP boundary.
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
                pair: PrincipalPair::new(me.as_dm_member(), with.as_dm_member())?,
            }),
        }
    }

    /// Parse the canonical spelling: `#<channel>`, `#<project>/<channel>`
    /// (the project may itself contain `/`, and an optional `proj:` prefix
    /// is dropped), `@<seat>`, or `@<wave>/<role>`. The inverse of
    /// [`RoomRef`]'s `Display` for channel and member-DM refs.
    pub fn parse_str(raw: &str) -> Result<Self, InvalidId> {
        if let Some(rest) = raw.strip_prefix('#') {
            let Some((project, name)) = rest.rsplit_once('/') else {
                return Ok(RoomRef::Channel {
                    scope: RoomScope::Global,
                    name: ChannelName::parse(rest)?,
                });
            };
            return Ok(RoomRef::Channel {
                scope: RoomScope::Project {
                    project: BbProjectId::parse(project.strip_prefix("proj:").unwrap_or(project))?,
                },
                name: ChannelName::parse(name)?,
            });
        }
        if raw.starts_with('@') {
            return Ok(RoomRef::Dm {
                with: Principal::parse_member_ref(raw)?,
            });
        }
        Err(InvalidId {
            what: "room".into(),
            reason: "expected #<channel>, #<project>/<channel>, @<seat>, or @<wave>/<role>".into(),
        })
    }
}

impl std::str::FromStr for RoomRef {
    type Err = InvalidId;
    fn from_str(raw: &str) -> Result<Self, InvalidId> {
        Self::parse_str(raw)
    }
}

fn fmt_channel(
    f: &mut std::fmt::Formatter<'_>,
    scope: &RoomScope,
    name: &ChannelName,
) -> std::fmt::Result {
    match scope {
        RoomScope::Global => write!(f, "#{}", name.as_str()),
        RoomScope::Project { project } => write!(f, "#{}/{}", project.as_str(), name.as_str()),
    }
}

/// `#<channel>`, `#<project>/<channel>`, `@<member>`; a resolved DM id shows
/// as `dm:<a>,<b>`.
impl std::fmt::Display for RoomRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoomRef::Channel { scope, name }
            | RoomRef::Id {
                id: RoomId::Channel { scope, name },
            } => fmt_channel(f, scope, name),
            RoomRef::Dm { with } => write!(f, "{with}"),
            RoomRef::Id {
                id: RoomId::Direct { pair },
            } => write!(f, "dm:{},{}", pair.lo, pair.hi),
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

/// Lifecycle state of a room task-thread.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Open,
    Claimed,
    Review,
    Blocked,
    Done,
    #[serde(alias = "canceled")]
    Cancelled,
}

impl TaskStatus {
    pub const fn org_keyword(self) -> &'static str {
        match self {
            TaskStatus::Open => "TODO",
            TaskStatus::Claimed => "STRT",
            TaskStatus::Review => "WAIT",
            TaskStatus::Blocked => "BLKD",
            TaskStatus::Done => "DONE",
            TaskStatus::Cancelled => "CNCL",
        }
    }

    pub fn from_org_keyword(keyword: &str) -> Option<Self> {
        match keyword {
            "TODO" | "IDEA" => Some(TaskStatus::Open),
            "STRT" => Some(TaskStatus::Claimed),
            "WAIT" => Some(TaskStatus::Review),
            "BLKD" => Some(TaskStatus::Blocked),
            "DONE" => Some(TaskStatus::Done),
            "CNCL" => Some(TaskStatus::Cancelled),
            _ => None,
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, TaskStatus::Done | TaskStatus::Cancelled)
    }
}

/// Specification for turning a room root post into a task thread.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskSpec {
    pub title: Title,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<OrgId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs: Vec<Seq>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<Priority>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub due_ms: Option<UnixMs>,
}

/// Active TTL lease on a claimed task thread.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskLease {
    pub holder: Principal,
    pub epoch: LeaseEpoch,
    pub claimed_at: UnixMs,
    pub expires_at: UnixMs,
}

/// Compact snapshot of a newer message returned inside [`crate::ConflictData::NewerMessage`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RoomMessageSummary {
    pub seq: Seq,
    pub at: UnixMs,
    pub by: Principal,
    pub body: Body,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub re: Option<Seq>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<Label>,
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

/// Provenance of a room post. Defaults to [`PostOrigin::Direct`] when omitted so
/// existing journal rows and wire payloads stay 100% compatible.
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
#[serde(rename_all = "snake_case")]
pub enum PostOrigin {
    #[default]
    Direct,
    /// Final assistant turn mirrored into a wave room by a host bridge.
    MirroredTurn,
}

impl PostOrigin {
    pub const fn is_direct(&self) -> bool {
        matches!(self, PostOrigin::Direct)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RoomPost {
    pub room: RoomRef,
    pub body: Body,
    #[serde(default, alias = "reply_to", skip_serializing_if = "Option::is_none")]
    pub re: Option<Seq>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Seq>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<Label>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskSpec>,
    #[serde(default, alias = "id", skip_serializing_if = "Option::is_none")]
    pub idem: Option<IdemKey>,
    #[serde(default, skip_serializing_if = "PostOrigin::is_direct")]
    #[schemars(skip)]
    pub origin: PostOrigin,
}

impl RoomPost {
    pub fn new(room: RoomRef, body: Body) -> Self {
        Self {
            room,
            body,
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
            origin: PostOrigin::Direct,
        }
    }

    pub fn mirrored_turn(room: RoomRef, body: Body, idem: IdemKey) -> Self {
        Self {
            idem: Some(idem),
            origin: PostOrigin::MirroredTurn,
            ..Self::new(room, body)
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RoomLabel {
    pub room: RoomRef,
    pub seq: Seq,
    #[serde(default)]
    pub add: Vec<Label>,
    #[serde(default)]
    pub remove: Vec<Label>,
    #[serde(default, alias = "id")]
    pub idem: Option<IdemKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Labeled {
    pub room: RoomId,
    pub seq: Seq,
    #[serde(default)]
    pub labels: Vec<Label>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct TaskClaim {
    pub room: RoomRef,
    pub task: Seq,
    #[serde(default)]
    pub ttl_ms: Option<u64>,
    #[serde(default)]
    pub expected_rev: Option<u64>,
    #[serde(default)]
    pub force: bool,
    #[serde(default, alias = "id")]
    pub idem: Option<IdemKey>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct TaskHeartbeat {
    pub room: RoomRef,
    pub task: Seq,
    pub epoch: LeaseEpoch,
    #[serde(default)]
    pub ttl_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct TaskTransition {
    pub room: RoomRef,
    pub task: Seq,
    pub status: TaskStatus,
    #[serde(default)]
    pub epoch: Option<LeaseEpoch>,
    #[serde(default)]
    pub expected_rev: Option<u64>,
    #[serde(default)]
    pub note: Option<Body>,
    #[serde(default, alias = "id")]
    pub idem: Option<IdemKey>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct TaskUpdate {
    pub room: RoomRef,
    pub task: Seq,
    #[serde(default)]
    pub expected_rev: Option<u64>,
    #[serde(default)]
    pub title: Option<Title>,
    #[serde(default)]
    pub needs_add: Vec<Seq>,
    #[serde(default)]
    pub needs_remove: Vec<Seq>,
    #[serde(default)]
    pub priority: Option<Priority>,
    #[serde(default)]
    pub due_ms: Option<UnixMs>,
    #[serde(default)]
    pub clear_due: bool,
    #[serde(default, alias = "id")]
    pub idem: Option<IdemKey>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskFilter {
    #[serde(default)]
    pub status: Option<TaskStatus>,
    #[serde(default)]
    pub ready_only: bool,
    #[serde(default)]
    pub holder: Option<Principal>,
    #[serde(default)]
    pub label: Option<Label>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RoomTasks {
    pub room: RoomRef,
    #[serde(default)]
    pub filter: Option<TaskFilter>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskLeaseReceipt {
    pub room: RoomId,
    pub task: Seq,
    pub org_id: OrgId,
    pub rev: u64,
    pub epoch: LeaseEpoch,
    pub expires_at: UnixMs,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskView {
    pub room: RoomId,
    pub seq: Seq,
    pub org_id: OrgId,
    pub rev: u64,
    pub title: Title,
    pub status: TaskStatus,
    pub priority: Priority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub due_ms: Option<UnixMs>,
    #[serde(default)]
    pub needs: Vec<Seq>,
    #[serde(default)]
    pub labels: Vec<Label>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<TaskLease>,
    pub ready: bool,
    pub lease_expired: bool,
    pub created_by: Principal,
    pub created_at: UnixMs,
    pub updated_at: UnixMs,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskListView {
    pub room: RoomId,
    pub head: Seq,
    pub tasks: Vec<TaskView>,
    #[serde(default)]
    pub total_count: usize,
    #[serde(default)]
    pub truncated: bool,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub re: Option<Seq>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<OrgId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_notice: Option<String>,
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
        #[serde(default, alias = "reply_to", skip_serializing_if = "Option::is_none")]
        re: Option<Seq>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after: Option<Seq>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        labels: Vec<Label>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task: Option<TaskSpec>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        idem: Option<IdemKey>,
        #[serde(default, skip_serializing_if = "PostOrigin::is_direct")]
        origin: PostOrigin,
    },
    TopicSet {
        topic: String,
    },
    Labeled {
        target: Seq,
        #[serde(default)]
        added: Vec<Label>,
        #[serde(default)]
        removed: Vec<Label>,
        #[serde(default)]
        before: Vec<Label>,
        #[serde(default)]
        after: Vec<Label>,
    },
    TaskClaimed {
        task: Seq,
        rev: u64,
        lease: TaskLease,
    },
    TaskHeartbeat {
        task: Seq,
        epoch: LeaseEpoch,
        expires_at: UnixMs,
    },
    TaskTransitioned {
        task: Seq,
        rev: u64,
        status: TaskStatus,
    },
    TaskUpdated {
        task: Seq,
        rev: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<Title>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        needs_add: Vec<Seq>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        needs_remove: Vec<Seq>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        priority: Option<Priority>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        due_ms: Option<UnixMs>,
        #[serde(default)]
        clear_due: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{RoleName, SeatName};

    #[test]
    fn room_ref_from_str_inverts_display() {
        let channels = ["general", "wave-alpha", "ops_1"];
        let projects = ["proj_demo_1", "/workspace/repo", "/tmp/work/sample-repo"];
        let mut refs = Vec::new();
        for name in channels {
            let name = ChannelName::parse(name).unwrap();
            refs.push(RoomRef::Channel {
                scope: RoomScope::Global,
                name: name.clone(),
            });
            for project in projects {
                refs.push(RoomRef::Channel {
                    scope: RoomScope::Project {
                        project: BbProjectId::parse(project).unwrap(),
                    },
                    name: name.clone(),
                });
            }
        }
        for seat in ["alice", "bob-2"] {
            refs.push(RoomRef::Dm {
                with: Principal::Seat {
                    seat: SeatName::parse(seat).unwrap(),
                },
            });
        }
        for (wave, role) in [("auth-v2", "author"), ("council-v1", "critic")] {
            refs.push(RoomRef::Dm {
                with: Principal::Worker {
                    wave: WaveSlug::parse(wave).unwrap(),
                    role: RoleName::parse(role).unwrap(),
                },
            });
        }
        for r in refs {
            let text = r.to_string();
            assert_eq!(text.parse::<RoomRef>().unwrap(), r, "{text}");
        }
    }

    #[test]
    fn room_ref_parses_path_projects_and_proj_prefix() {
        let expected = RoomRef::Channel {
            scope: RoomScope::Project {
                project: BbProjectId::parse("/workspace/repo").unwrap(),
            },
            name: ChannelName::parse("wave-alpha").unwrap(),
        };
        assert_eq!(
            RoomRef::parse_str("#/workspace/repo/wave-alpha").unwrap(),
            expected
        );
        assert_eq!(
            RoomRef::parse_str("#proj:/workspace/repo/wave-alpha").unwrap(),
            expected
        );
        for bad in ["general", "", "#", "@", "@a/b/c", "#proj/"] {
            assert!(RoomRef::parse_str(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn member_refs_never_name_operator_or_system() {
        assert!(matches!(
            Principal::parse_member_ref("@sam").unwrap(),
            Principal::Seat { .. }
        ));
        assert!(matches!(
            Principal::parse_member_ref("auth-v2/author").unwrap(),
            Principal::Worker { .. }
        ));
    }
}
