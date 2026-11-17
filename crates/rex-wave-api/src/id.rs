//! Branded identifiers. Every one is parsed once at the boundary
//! (`serde(try_from = "String")`) and trusted everywhere inside.

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt;

/// A boundary-validation failure. Carried to callers as `ApiError::Invalid`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[error("invalid {what}: {reason}")]
pub struct InvalidId {
    pub what: String,
    pub reason: String,
}

fn slug(s: &str, max: usize) -> Result<(), &'static str> {
    if s.is_empty() || s.len() > max {
        return Err("length out of range");
    }
    if !s
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        return Err("only [a-z0-9_-] allowed");
    }
    if s.starts_with('-') {
        return Err("must not start with '-'");
    }
    Ok(())
}

fn opaque(s: &str) -> Result<(), &'static str> {
    if s.is_empty() || s.len() > 256 || s.chars().any(char::is_control) {
        return Err("must be 1..=256 printable chars");
    }
    Ok(())
}

macro_rules! branded_str {
    ($(#[$m:meta])* $name:ident, $what:literal, |$s:ident| $check:expr) => {
        $(#[$m])*
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            /// Parse at the boundary. The only constructor.
            pub fn parse(raw: impl Into<String>) -> Result<Self, InvalidId> {
                let $s: String = raw.into();
                let verdict: Result<(), &'static str> = $check;
                verdict.map_err(|reason| InvalidId { what: $what.into(), reason: reason.into() })?;
                Ok(Self($s))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl TryFrom<String> for $name {
            type Error = InvalidId;
            fn try_from(s: String) -> Result<Self, InvalidId> {
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

branded_str!(
    /// A resident seat on the estate roster (`sol`, `astra`, `owner`, ...).
    SeatName, "seat", |s| slug(&s, 32));
branded_str!(
    /// A worker's role inside one wave (`author-jwt`, `verifier`). Unique per wave.
    RoleName, "role", |s| slug(&s, 32));
branded_str!(
    /// A wave's slug. The wave room is always `#wave-<slug>`.
    WaveSlug, "wave slug", |s| slug(&s, 48));
branded_str!(
    /// A channel name *without* the leading `#`. Display adds it.
    ChannelName, "channel", |s| slug(&s, 64));
branded_str!(
    /// BB project id (opaque, owned by BB).
    BbProjectId, "bb project id", |s| opaque(&s));
branded_str!(
    /// BB thread id (opaque, owned by BB).
    BbThreadId, "bb thread id", |s| opaque(&s));
branded_str!(
    /// BB pending-interaction id (opaque, owned by BB).
    BbInteractionId, "bb interaction id", |s| opaque(&s));
branded_str!(
    /// BB provider id: `claude-code`, `codex`, `acp-cursor`, `pi`, ... Validated
    /// against the live BB catalog at wave launch, not here.
    ProviderId, "provider", |s| opaque(&s));
branded_str!(
    /// BB model id within a provider (`claude-opus-5-5`, `gpt-6.1-sol`, `openrouter/...`).
    ModelId, "model", |s| opaque(&s));
branded_str!(
    /// Client-chosen idempotency key. Re-sending a command with the same key on
    /// the same stream returns the original receipt instead of appending again.
    IdemKey, "idempotency key", |s| opaque(&s));
branded_str!(
    /// Lower-case 64-hex sha256 of exact bytes (mail ACKs acknowledge nothing without it).
    Sha256Hex, "sha256", |s| if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) { Ok(()) } else { Err("need 64 lower-hex chars") });

/// Per-stream monotonically increasing position. `Seq(0)` means "before the first entry".
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
pub struct Seq(pub u64);

impl Seq {
    pub const ORIGIN: Seq = Seq(0);
    pub fn next(self) -> Seq {
        Seq(self.0 + 1)
    }
}

/// BB's own per-thread event sequence (foreign cursor; never mixed with [`Seq`]).
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
pub struct BbSeq(pub u64);

/// Wall-clock milliseconds since the Unix epoch, stamped by the daemon only.
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
pub struct UnixMs(pub u64);

/// Inclusive range of seqs inside one stream (used by `promote_to_letter`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "(u64, u64)", into = "(u64, u64)")]
pub struct SeqRange {
    first: Seq,
    last: Seq,
}

impl SeqRange {
    pub fn new(first: Seq, last: Seq) -> Result<Self, InvalidId> {
        if first.0 == 0 || last < first {
            return Err(InvalidId {
                what: "seq range".into(),
                reason: "need 1 <= first <= last".into(),
            });
        }
        Ok(Self { first, last })
    }
    pub fn first(&self) -> Seq {
        self.first
    }
    pub fn last(&self) -> Seq {
        self.last
    }
}
impl JsonSchema for SeqRange {
    fn schema_name() -> Cow<'static, str> {
        "SeqRange".into()
    }
    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        <(u64, u64)>::json_schema(generator)
    }
}
impl TryFrom<(u64, u64)> for SeqRange {
    type Error = InvalidId;
    fn try_from((a, b): (u64, u64)) -> Result<Self, InvalidId> {
        Self::new(Seq(a), Seq(b))
    }
}
impl From<SeqRange> for (u64, u64) {
    fn from(r: SeqRange) -> (u64, u64) {
        (r.first.0, r.last.0)
    }
}

/// Who did something. Bound at session open by the transport; never parsed
/// from command arguments.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Principal {
    /// The human operator at the TUI / CLI (authenticated by Unix peer uid).
    Operator { seat: SeatName },
    /// A resident estate seat using a minted capability token (MCP stdio/HTTP).
    Seat { seat: SeatName },
    /// A BB thread spawned by a wave; resolved server-side from its BB thread id.
    Worker { wave: WaveSlug, role: RoleName },
    /// The daemon's own reactors and adapters.
    System { part: SystemPart },
}

/// The daemon-internal parts allowed to issue `Audience::System` commands.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SystemPart {
    WaveConductor,
    Herald,
    BbBridge,
    MailBridge,
    WakeBell,
}

impl fmt::Display for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Principal::Operator { seat } | Principal::Seat { seat } => write!(f, "@{seat}"),
            Principal::Worker { wave, role } => write!(f, "@{wave}/{role}"),
            Principal::System { part } => write!(f, "@system:{part:?}"),
        }
    }
}

branded_str!(
    /// Short `@handle` unique *within one room* (`verifier`, `sol`). Rooms
    /// resolve handles to principals at post time, so mention resolution never
    /// needs a cross-actor lookup.
    Handle, "handle", |s| slug(&s, 32));
