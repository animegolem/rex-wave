//! Operator configuration (`Family::Config`, `StreamId::Config`, and `~/.config/rex-wave/config.toml`).

use crate::id::{IdemKey, InvalidId, ModelId, ProviderId, RunnerKind, SeatName, Seq};
use crate::wave::PermissionMode;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

fn canonical_key_err(raw: &str) -> InvalidId {
    InvalidId {
        what: "config key".into(),
        reason: format!("unknown config key '{raw}'"),
    }
}

/// High-level runner route selectable globally or per provider in `[runners]`.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RunnerRoute {
    Bb,
    T3,
    Native,
}

impl RunnerRoute {
    pub const ALL: [RunnerRoute; 3] = [RunnerRoute::Bb, RunnerRoute::T3, RunnerRoute::Native];

    pub const fn as_str(self) -> &'static str {
        match self {
            RunnerRoute::Bb => "bb",
            RunnerRoute::T3 => "t3",
            RunnerRoute::Native => "native",
        }
    }

    pub const fn display_name(self) -> &'static str {
        match self {
            RunnerRoute::Bb => "BB",
            RunnerRoute::T3 => "T3",
            RunnerRoute::Native => "Native",
        }
    }

    pub const fn from_runner_kind(kind: RunnerKind) -> Self {
        match kind {
            RunnerKind::Bb => RunnerRoute::Bb,
            RunnerKind::T3 => RunnerRoute::T3,
            RunnerKind::Acp | RunnerKind::Claude | RunnerKind::Codex => RunnerRoute::Native,
        }
    }

    /// Maps this high-level route to the concrete [`RunnerKind`] for `provider`.
    pub fn resolve_for_provider(self, provider: &str) -> Result<RunnerKind, String> {
        let norm = provider.trim().to_ascii_lowercase();
        let is_agy = norm == "antigravity" || norm == "agy";
        match self {
            RunnerRoute::Bb => {
                if is_agy {
                    Err(
                        "provider 'antigravity' must route through T3 Code or Native ACP, never BB"
                            .into(),
                    )
                } else {
                    Ok(RunnerKind::Bb)
                }
            }
            RunnerRoute::T3 => Ok(RunnerKind::T3),
            RunnerRoute::Native => {
                if norm == "claude" || norm == "claude-code" || norm == "claudeagent" {
                    Ok(RunnerKind::Claude)
                } else if norm == "codex" {
                    Ok(RunnerKind::Codex)
                } else {
                    Ok(RunnerKind::Acp)
                }
            }
        }
    }
}

/// Default workspace checkout strategy for a harness profile in `[harnesses.<provider>]`.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum WorkspaceStrategy {
    ProjectDefault,
    ManagedWorktree,
}

impl WorkspaceStrategy {
    pub const fn as_str(self) -> &'static str {
        match self {
            WorkspaceStrategy::ProjectDefault => "project-default",
            WorkspaceStrategy::ManagedWorktree => "managed-worktree",
        }
    }
}

/// Canonical harness slots configured under `[runners]` and `[harnesses.<slot>]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HarnessProvider {
    Claude,
    Codex,
    Agy,
    Opencode,
}

impl HarnessProvider {
    pub const ALL: [HarnessProvider; 4] = [
        HarnessProvider::Claude,
        HarnessProvider::Codex,
        HarnessProvider::Agy,
        HarnessProvider::Opencode,
    ];

    pub const fn slug(self) -> &'static str {
        match self {
            HarnessProvider::Claude => "claude",
            HarnessProvider::Codex => "codex",
            HarnessProvider::Agy => "agy",
            HarnessProvider::Opencode => "opencode",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            HarnessProvider::Claude => "Claude",
            HarnessProvider::Codex => "Codex",
            HarnessProvider::Agy => "AGY",
            HarnessProvider::Opencode => "OpenCode",
        }
    }

    pub fn parse_slot(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "claude" | "claude-code" | "claudeagent" => Some(HarnessProvider::Claude),
            "codex" => Some(HarnessProvider::Codex),
            "agy" | "antigravity" => Some(HarnessProvider::Agy),
            "opencode" | "open-code" => Some(HarnessProvider::Opencode),
            _ => None,
        }
    }
}

/// `[runners]` section of `config.toml`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RunnersConfig {
    /// Global default runner when a provider has no override (`None` follows daemon `--runner` / environment detection).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_runner: Option<RunnerRoute>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude: Option<RunnerRoute>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codex: Option<RunnerRoute>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agy: Option<RunnerRoute>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opencode: Option<RunnerRoute>,
}

impl RunnersConfig {
    pub fn override_for(&self, p: HarnessProvider) -> Option<RunnerRoute> {
        match p {
            HarnessProvider::Claude => self.claude,
            HarnessProvider::Codex => self.codex,
            HarnessProvider::Agy => self.agy,
            HarnessProvider::Opencode => self.opencode,
        }
    }

    pub fn override_mut(&mut self, p: HarnessProvider) -> &mut Option<RunnerRoute> {
        match p {
            HarnessProvider::Claude => &mut self.claude,
            HarnessProvider::Codex => &mut self.codex,
            HarnessProvider::Agy => &mut self.agy,
            HarnessProvider::Opencode => &mut self.opencode,
        }
    }
}

/// Per-provider defaults under `[harnesses.<provider>]`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HarnessProfile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission: Option<PermissionMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<WorkspaceStrategy>,
}

impl HarnessProfile {
    pub fn is_empty(&self) -> bool {
        self.binary.is_none()
            && self.model.is_none()
            && self.reasoning.is_none()
            && self.account.is_none()
            && self.permission.is_none()
            && self.workspace.is_none()
    }
}

/// `[harnesses]` table of `config.toml`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HarnessesConfig {
    #[serde(default, skip_serializing_if = "HarnessProfile::is_empty")]
    pub claude: HarnessProfile,
    #[serde(default, skip_serializing_if = "HarnessProfile::is_empty")]
    pub codex: HarnessProfile,
    #[serde(default, skip_serializing_if = "HarnessProfile::is_empty")]
    pub agy: HarnessProfile,
    #[serde(default, skip_serializing_if = "HarnessProfile::is_empty")]
    pub opencode: HarnessProfile,
}

impl HarnessesConfig {
    pub fn profile(&self, p: HarnessProvider) -> &HarnessProfile {
        match p {
            HarnessProvider::Claude => &self.claude,
            HarnessProvider::Codex => &self.codex,
            HarnessProvider::Agy => &self.agy,
            HarnessProvider::Opencode => &self.opencode,
        }
    }

    pub fn profile_mut(&mut self, p: HarnessProvider) -> &mut HarnessProfile {
        match p {
            HarnessProvider::Claude => &mut self.claude,
            HarnessProvider::Codex => &mut self.codex,
            HarnessProvider::Agy => &mut self.agy,
            HarnessProvider::Opencode => &mut self.opencode,
        }
    }

    pub fn for_provider_id(&self, provider: &ProviderId) -> Option<&HarnessProfile> {
        let clean = RunnerKind::strip_provider_prefix(provider.as_str())
            .map_or_else(|| provider.as_str(), |(_, rest)| rest);
        HarnessProvider::parse_slot(clean).map(|slot| self.profile(slot))
    }
}

pub const DEFAULT_HUB_OWNER_SEAT: &str = "postmaster";
pub const DEFAULT_MAIL_RECONNECT_GRACE_MS: u64 = 15_000;
pub const MAX_MAIL_RECONNECT_GRACE_MS: u64 = 86_400_000;

fn default_hub_owner_seat() -> SeatName {
    SeatName::parse(DEFAULT_HUB_OWNER_SEAT).expect("valid default hub_owner_seat")
}

const fn default_reconnect_grace_ms() -> u64 {
    DEFAULT_MAIL_RECONNECT_GRACE_MS
}

/// `[mail]` section of `config.toml`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MailConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mirror_dir: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drop_dir: Option<PathBuf>,
    #[serde(default)]
    pub export_enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub couriers_dir: Option<PathBuf>,
    #[serde(default = "default_hub_owner_seat")]
    pub hub_owner_seat: SeatName,
    #[serde(default = "default_reconnect_grace_ms")]
    pub reconnect_grace_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toast_cmd: Option<String>,
}

impl Default for MailConfig {
    fn default() -> Self {
        Self {
            mirror_dir: None,
            drop_dir: None,
            export_enabled: false,
            couriers_dir: None,
            hub_owner_seat: default_hub_owner_seat(),
            reconnect_grace_ms: DEFAULT_MAIL_RECONNECT_GRACE_MS,
            toast_cmd: None,
        }
    }
}

/// Top-level operator configuration persisted in `StreamId::Config` and `~/.config/rex-wave/config.toml`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RexConfig {
    #[serde(default)]
    pub runners: RunnersConfig,
    #[serde(default)]
    pub harnesses: HarnessesConfig,
    #[serde(default)]
    pub mail: MailConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettingFieldKind {
    RunnerRoute { allow_bb: bool },
    Bool,
    Permission,
    Workspace,
    Text,
    Path,
    Number,
}

#[derive(Clone, Copy, Debug)]
pub struct SettingFieldSpec {
    pub section: &'static str,
    pub key: &'static str,
    pub label: &'static str,
    pub kind: SettingFieldKind,
    pub hint: &'static str,
}

pub static SETTINGS_FIELDS: &[SettingFieldSpec] = &[
    SettingFieldSpec {
        section: "Runners",
        key: "runners.default_runner",
        label: "Default runner",
        kind: SettingFieldKind::RunnerRoute { allow_bb: true },
        hint: "auto | bb | t3 | native",
    },
    SettingFieldSpec {
        section: "Runners",
        key: "runners.claude",
        label: "Claude route",
        kind: SettingFieldKind::RunnerRoute { allow_bb: true },
        hint: "auto | bb | t3 | native",
    },
    SettingFieldSpec {
        section: "Runners",
        key: "runners.codex",
        label: "Codex route",
        kind: SettingFieldKind::RunnerRoute { allow_bb: true },
        hint: "auto | bb | t3 | native",
    },
    SettingFieldSpec {
        section: "Runners",
        key: "runners.agy",
        label: "AGY route",
        kind: SettingFieldKind::RunnerRoute { allow_bb: false },
        hint: "auto | t3 | native",
    },
    SettingFieldSpec {
        section: "Runners",
        key: "runners.opencode",
        label: "OpenCode route",
        kind: SettingFieldKind::RunnerRoute { allow_bb: true },
        hint: "auto | bb | t3 | native",
    },
    SettingFieldSpec {
        section: "Harness · Claude",
        key: "harnesses.claude.binary",
        label: "Binary path",
        kind: SettingFieldKind::Text,
        hint: "executable name or path",
    },
    SettingFieldSpec {
        section: "Harness · Claude",
        key: "harnesses.claude.model",
        label: "Default model",
        kind: SettingFieldKind::Text,
        hint: "e.g. claude-opus-5-5",
    },
    SettingFieldSpec {
        section: "Harness · Claude",
        key: "harnesses.claude.reasoning",
        label: "Reasoning effort",
        kind: SettingFieldKind::Text,
        hint: "low | medium | high | xhigh | max",
    },
    SettingFieldSpec {
        section: "Harness · Claude",
        key: "harnesses.claude.account",
        label: "Billing account",
        kind: SettingFieldKind::Text,
        hint: "optional profile name",
    },
    SettingFieldSpec {
        section: "Harness · Claude",
        key: "harnesses.claude.permission",
        label: "Permission mode",
        kind: SettingFieldKind::Permission,
        hint: "default | accept-edits | auto | full",
    },
    SettingFieldSpec {
        section: "Harness · Claude",
        key: "harnesses.claude.workspace",
        label: "Workspace mode",
        kind: SettingFieldKind::Workspace,
        hint: "default | project-default | managed-worktree",
    },
    SettingFieldSpec {
        section: "Harness · Codex",
        key: "harnesses.codex.binary",
        label: "Binary path",
        kind: SettingFieldKind::Text,
        hint: "executable name or path",
    },
    SettingFieldSpec {
        section: "Harness · Codex",
        key: "harnesses.codex.model",
        label: "Default model",
        kind: SettingFieldKind::Text,
        hint: "e.g. gpt-6.2-codex",
    },
    SettingFieldSpec {
        section: "Harness · Codex",
        key: "harnesses.codex.reasoning",
        label: "Reasoning effort",
        kind: SettingFieldKind::Text,
        hint: "low | medium | high | xhigh",
    },
    SettingFieldSpec {
        section: "Harness · Codex",
        key: "harnesses.codex.account",
        label: "Billing account",
        kind: SettingFieldKind::Text,
        hint: "optional profile name",
    },
    SettingFieldSpec {
        section: "Harness · Codex",
        key: "harnesses.codex.permission",
        label: "Permission mode",
        kind: SettingFieldKind::Permission,
        hint: "default | accept-edits | auto | full",
    },
    SettingFieldSpec {
        section: "Harness · Codex",
        key: "harnesses.codex.workspace",
        label: "Workspace mode",
        kind: SettingFieldKind::Workspace,
        hint: "default | project-default | managed-worktree",
    },
    SettingFieldSpec {
        section: "Harness · AGY",
        key: "harnesses.agy.binary",
        label: "Binary path",
        kind: SettingFieldKind::Text,
        hint: "executable name or path",
    },
    SettingFieldSpec {
        section: "Harness · AGY",
        key: "harnesses.agy.model",
        label: "Default model",
        kind: SettingFieldKind::Text,
        hint: "e.g. gemini-4-neon-xhigh",
    },
    SettingFieldSpec {
        section: "Harness · AGY",
        key: "harnesses.agy.reasoning",
        label: "Reasoning effort",
        kind: SettingFieldKind::Text,
        hint: "low | medium | high | xhigh",
    },
    SettingFieldSpec {
        section: "Harness · AGY",
        key: "harnesses.agy.account",
        label: "Billing account",
        kind: SettingFieldKind::Text,
        hint: "optional profile name",
    },
    SettingFieldSpec {
        section: "Harness · AGY",
        key: "harnesses.agy.permission",
        label: "Permission mode",
        kind: SettingFieldKind::Permission,
        hint: "default | accept-edits | auto | full",
    },
    SettingFieldSpec {
        section: "Harness · AGY",
        key: "harnesses.agy.workspace",
        label: "Workspace mode",
        kind: SettingFieldKind::Workspace,
        hint: "default | project-default | managed-worktree",
    },
    SettingFieldSpec {
        section: "Harness · OpenCode",
        key: "harnesses.opencode.binary",
        label: "Binary path",
        kind: SettingFieldKind::Text,
        hint: "executable name or path",
    },
    SettingFieldSpec {
        section: "Harness · OpenCode",
        key: "harnesses.opencode.model",
        label: "Default model",
        kind: SettingFieldKind::Text,
        hint: "optional model id",
    },
    SettingFieldSpec {
        section: "Harness · OpenCode",
        key: "harnesses.opencode.reasoning",
        label: "Reasoning effort",
        kind: SettingFieldKind::Text,
        hint: "low | medium | high | xhigh",
    },
    SettingFieldSpec {
        section: "Harness · OpenCode",
        key: "harnesses.opencode.account",
        label: "Billing account",
        kind: SettingFieldKind::Text,
        hint: "optional profile name",
    },
    SettingFieldSpec {
        section: "Harness · OpenCode",
        key: "harnesses.opencode.permission",
        label: "Permission mode",
        kind: SettingFieldKind::Permission,
        hint: "default | accept-edits | auto | full",
    },
    SettingFieldSpec {
        section: "Harness · OpenCode",
        key: "harnesses.opencode.workspace",
        label: "Workspace mode",
        kind: SettingFieldKind::Workspace,
        hint: "default | project-default | managed-worktree",
    },
    SettingFieldSpec {
        section: "Mail & Couriers",
        key: "mail.export_enabled",
        label: "Markdown export",
        kind: SettingFieldKind::Bool,
        hint: "true | false",
    },
    SettingFieldSpec {
        section: "Mail & Couriers",
        key: "mail.mirror_dir",
        label: "Mirror directory",
        kind: SettingFieldKind::Path,
        hint: "guarded by protected-roots",
    },
    SettingFieldSpec {
        section: "Mail & Couriers",
        key: "mail.drop_dir",
        label: "Drop directory",
        kind: SettingFieldKind::Path,
        hint: "guarded by protected-roots",
    },
    SettingFieldSpec {
        section: "Mail & Couriers",
        key: "mail.couriers_dir",
        label: "Couriers directory",
        kind: SettingFieldKind::Path,
        hint: "default ~/.config/rex-wave/couriers.d",
    },
    SettingFieldSpec {
        section: "Mail & Couriers",
        key: "mail.hub_owner_seat",
        label: "Hub owner seat",
        kind: SettingFieldKind::Text,
        hint: "failure escalation target (e.g. postmaster)",
    },
    SettingFieldSpec {
        section: "Mail & Couriers",
        key: "mail.reconnect_grace_ms",
        label: "Reconnect grace (ms)",
        kind: SettingFieldKind::Number,
        hint: "1..=86400000 (default 15000)",
    },
    SettingFieldSpec {
        section: "Mail & Couriers",
        key: "mail.toast_cmd",
        label: "Toast command",
        kind: SettingFieldKind::Text,
        hint: "optional operator notification command",
    },
];

fn validate_printable(what: &str, raw: &str, max_bytes: usize) -> Result<String, InvalidId> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > max_bytes || trimmed.chars().any(char::is_control) {
        return Err(InvalidId {
            what: what.into(),
            reason: format!("must be 1..={max_bytes} printable non-control bytes"),
        });
    }
    Ok(trimmed.to_owned())
}

fn validate_reasoning(what: &str, raw: &str) -> Result<String, InvalidId> {
    let norm = raw.trim().to_ascii_lowercase();
    if norm.is_empty()
        || norm.len() > 32
        || norm.starts_with('-')
        || !norm
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_'))
    {
        return Err(InvalidId {
            what: what.into(),
            reason: "must be 1..=32 [a-z0-9_-] chars (e.g. low, medium, high, xhigh, max)".into(),
        });
    }
    Ok(norm)
}

fn validate_config_path(what: &str, p: &std::path::Path) -> Result<PathBuf, InvalidId> {
    let Some(raw) = p.to_str() else {
        return Err(InvalidId {
            what: what.into(),
            reason: "path must be valid UTF-8".into(),
        });
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > 1024 || trimmed.chars().any(char::is_control) {
        return Err(InvalidId {
            what: what.into(),
            reason: "path must be 1..=1024 printable chars".into(),
        });
    }
    let mut out = PathBuf::new();
    for comp in std::path::Path::new(trimmed).components() {
        match comp {
            std::path::Component::ParentDir => {
                return Err(InvalidId {
                    what: what.into(),
                    reason: format!("path `{trimmed}` must not contain `..`"),
                });
            }
            std::path::Component::CurDir => {}
            std::path::Component::RootDir
            | std::path::Component::Prefix(_)
            | std::path::Component::Normal(_) => {
                out.push(comp.as_os_str());
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(InvalidId {
            what: what.into(),
            reason: format!("path `{trimmed}` must not be empty or `.`"),
        });
    }
    Ok(out)
}

fn parse_runner_route_opt(
    what: &str,
    raw: Option<&str>,
    allow_bb: bool,
) -> Result<Option<RunnerRoute>, InvalidId> {
    let Some(s) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    if s.eq_ignore_ascii_case("auto") || s.eq_ignore_ascii_case("default") {
        return Ok(None);
    }
    let route: RunnerRoute = crate::parse_enum(what, s).map_err(|_| InvalidId {
        what: what.into(),
        reason: if allow_bb {
            "expected auto, bb, t3, or native".into()
        } else {
            "expected auto, t3, or native".into()
        },
    })?;
    if !allow_bb && route == RunnerRoute::Bb {
        return Err(InvalidId {
            what: what.into(),
            reason: "provider 'agy' ('antigravity') cannot route through 'bb'; choose auto, t3, or native".into(),
        });
    }
    Ok(Some(route))
}

fn parse_bool_str(what: &str, raw: Option<&str>) -> Result<bool, InvalidId> {
    let Some(s) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(false);
    };
    match s.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(InvalidId {
            what: what.into(),
            reason: "expected true or false".into(),
        }),
    }
}

/// Resolves canonical key spelling (normalizing `runners.antigravity` -> `runners.agy`, etc.).
pub fn canonicalize_config_key(raw: &str) -> Result<&'static str, InvalidId> {
    let norm = raw.trim().to_ascii_lowercase();
    let mapped = match norm.as_str() {
        "runners.default" => "runners.default_runner".to_owned(),
        "runners.antigravity" => "runners.agy".to_owned(),
        "runners.claude-code" | "runners.claudeagent" => "runners.claude".to_owned(),
        "runners.open-code" => "runners.opencode".to_owned(),
        other => {
            if let Some(rest) = other.strip_prefix("harnesses.antigravity.") {
                format!("harnesses.agy.{rest}")
            } else if let Some(rest) = other.strip_prefix("harnesses.claude-code.") {
                format!("harnesses.claude.{rest}")
            } else if let Some(rest) = other.strip_prefix("harnesses.open-code.") {
                format!("harnesses.opencode.{rest}")
            } else {
                other.to_owned()
            }
        }
    };
    SETTINGS_FIELDS
        .iter()
        .find(|f| f.key == mapped)
        .map(|f| f.key)
        .ok_or_else(|| canonical_key_err(raw.trim()))
}

impl RexConfig {
    /// Validates all fields and cross-field rules on a loaded or mutated [`RexConfig`].
    pub fn validate(&self) -> Result<(), InvalidId> {
        if self.runners.agy == Some(RunnerRoute::Bb) {
            return Err(InvalidId {
                what: "runners.agy".into(),
                reason:
                    "provider 'agy' ('antigravity') cannot route through 'bb'; choose t3 or native"
                        .into(),
            });
        }
        for p in HarnessProvider::ALL {
            let prof = self.harnesses.profile(p);
            let slug = p.slug();
            if let Some(ref bin) = prof.binary {
                validate_printable(&format!("harnesses.{slug}.binary"), bin, 512)?;
            }
            if let Some(ref r) = prof.reasoning {
                validate_reasoning(&format!("harnesses.{slug}.reasoning"), r)?;
            }
            if let Some(ref acct) = prof.account {
                validate_printable(&format!("harnesses.{slug}.account"), acct, 128)?;
            }
        }
        if let Some(ref p) = self.mail.mirror_dir {
            validate_config_path("mail.mirror_dir", p)?;
        }
        if let Some(ref p) = self.mail.drop_dir {
            validate_config_path("mail.drop_dir", p)?;
        }
        if let Some(ref p) = self.mail.couriers_dir {
            validate_config_path("mail.couriers_dir", p)?;
        }
        if self.mail.reconnect_grace_ms == 0
            || self.mail.reconnect_grace_ms > MAX_MAIL_RECONNECT_GRACE_MS
        {
            return Err(InvalidId {
                what: "mail.reconnect_grace_ms".into(),
                reason: format!(
                    "must be in 1..={MAX_MAIL_RECONNECT_GRACE_MS} ms (got {})",
                    self.mail.reconnect_grace_ms
                ),
            });
        }
        if let Some(ref cmd) = self.mail.toast_cmd {
            validate_printable("mail.toast_cmd", cmd, 1024)?;
        }
        Ok(())
    }

    /// Reads one dotted configuration key (`None` when an optional field is unset).
    pub fn get_key(&self, key: &str) -> Result<Option<String>, InvalidId> {
        let canon = canonicalize_config_key(key)?;
        let val = match canon {
            "runners.default_runner" => self.runners.default_runner.map(|r| r.as_str().to_owned()),
            "runners.claude" => self.runners.claude.map(|r| r.as_str().to_owned()),
            "runners.codex" => self.runners.codex.map(|r| r.as_str().to_owned()),
            "runners.agy" => self.runners.agy.map(|r| r.as_str().to_owned()),
            "runners.opencode" => self.runners.opencode.map(|r| r.as_str().to_owned()),
            "mail.export_enabled" => Some(self.mail.export_enabled.to_string()),
            "mail.mirror_dir" => self
                .mail
                .mirror_dir
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned()),
            "mail.drop_dir" => self
                .mail
                .drop_dir
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned()),
            "mail.couriers_dir" => self
                .mail
                .couriers_dir
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned()),
            "mail.hub_owner_seat" => Some(self.mail.hub_owner_seat.to_string()),
            "mail.reconnect_grace_ms" => Some(self.mail.reconnect_grace_ms.to_string()),
            "mail.toast_cmd" => self.mail.toast_cmd.clone(),
            other => {
                let rest = other
                    .strip_prefix("harnesses.")
                    .expect("canonical harness key");
                let (slot_str, field) = rest.split_once('.').expect("harness.<slot>.<field>");
                let slot = HarnessProvider::parse_slot(slot_str).expect("valid slot");
                let prof = self.harnesses.profile(slot);
                match field {
                    "binary" => prof.binary.clone(),
                    "model" => prof.model.as_ref().map(ToString::to_string),
                    "reasoning" => prof.reasoning.clone(),
                    "account" => prof.account.clone(),
                    "permission" => prof.permission.map(|p| match p {
                        PermissionMode::AcceptEdits => "accept-edits".to_owned(),
                        PermissionMode::Auto => "auto".to_owned(),
                        PermissionMode::Full => "full".to_owned(),
                    }),
                    "workspace" => prof.workspace.map(|w| w.as_str().to_owned()),
                    _ => unreachable!("canonicalize_config_key verified field"),
                }
            }
        };
        Ok(val)
    }

    /// Mutates one dotted configuration key (`value: None` or `Some("")` resets/clears to default).
    pub fn set_key(
        &mut self,
        key: &str,
        value: Option<&str>,
    ) -> Result<(&'static str, Option<String>), InvalidId> {
        let canon = canonicalize_config_key(key)?;
        match canon {
            "runners.default_runner" => {
                self.runners.default_runner = parse_runner_route_opt(canon, value, true)?;
            }
            "runners.claude" => {
                self.runners.claude = parse_runner_route_opt(canon, value, true)?;
            }
            "runners.codex" => {
                self.runners.codex = parse_runner_route_opt(canon, value, true)?;
            }
            "runners.agy" => {
                self.runners.agy = parse_runner_route_opt(canon, value, false)?;
            }
            "runners.opencode" => {
                self.runners.opencode = parse_runner_route_opt(canon, value, true)?;
            }
            "mail.export_enabled" => {
                self.mail.export_enabled = parse_bool_str(canon, value)?;
            }
            "mail.mirror_dir" => {
                self.mail.mirror_dir = match value.map(str::trim).filter(|s| !s.is_empty()) {
                    Some(s) => Some(validate_config_path(canon, std::path::Path::new(s))?),
                    None => None,
                };
            }
            "mail.drop_dir" => {
                self.mail.drop_dir = match value.map(str::trim).filter(|s| !s.is_empty()) {
                    Some(s) => Some(validate_config_path(canon, std::path::Path::new(s))?),
                    None => None,
                };
            }
            "mail.couriers_dir" => {
                self.mail.couriers_dir = match value.map(str::trim).filter(|s| !s.is_empty()) {
                    Some(s) => Some(validate_config_path(canon, std::path::Path::new(s))?),
                    None => None,
                };
            }
            "mail.hub_owner_seat" => {
                self.mail.hub_owner_seat = match value.map(str::trim).filter(|s| !s.is_empty()) {
                    Some(s) => SeatName::parse(s.strip_prefix('@').unwrap_or(s))?,
                    None => default_hub_owner_seat(),
                };
            }
            "mail.reconnect_grace_ms" => {
                self.mail.reconnect_grace_ms = match value.map(str::trim).filter(|s| !s.is_empty())
                {
                    Some(s) => {
                        let ms = s.parse::<u64>().map_err(|_| InvalidId {
                            what: canon.into(),
                            reason: "expected positive integer milliseconds".into(),
                        })?;
                        if ms == 0 || ms > MAX_MAIL_RECONNECT_GRACE_MS {
                            return Err(InvalidId {
                                what: canon.into(),
                                reason: format!("must be in 1..={MAX_MAIL_RECONNECT_GRACE_MS} ms"),
                            });
                        }
                        ms
                    }
                    None => DEFAULT_MAIL_RECONNECT_GRACE_MS,
                };
            }
            "mail.toast_cmd" => {
                self.mail.toast_cmd = match value.map(str::trim).filter(|s| !s.is_empty()) {
                    Some(s) => Some(validate_printable(canon, s, 1024)?),
                    None => None,
                };
            }
            other => {
                let rest = other
                    .strip_prefix("harnesses.")
                    .expect("canonical harness key");
                let (slot_str, field) = rest.split_once('.').expect("harness.<slot>.<field>");
                let slot = HarnessProvider::parse_slot(slot_str).expect("valid slot");
                let prof = self.harnesses.profile_mut(slot);
                match field {
                    "binary" => {
                        prof.binary = match value.map(str::trim).filter(|s| !s.is_empty()) {
                            Some(s) => Some(validate_printable(canon, s, 512)?),
                            None => None,
                        };
                    }
                    "model" => {
                        prof.model = match value.map(str::trim).filter(|s| !s.is_empty()) {
                            Some(s) => Some(ModelId::parse(s)?),
                            None => None,
                        };
                    }
                    "reasoning" => {
                        prof.reasoning = match value.map(str::trim).filter(|s| !s.is_empty()) {
                            Some(s) => Some(validate_reasoning(canon, s)?),
                            None => None,
                        };
                    }
                    "account" => {
                        prof.account = match value.map(str::trim).filter(|s| !s.is_empty()) {
                            Some(s) => Some(validate_printable(canon, s, 128)?),
                            None => None,
                        };
                    }
                    "permission" => {
                        prof.permission = match value.map(str::trim).filter(|s| !s.is_empty()) {
                            Some(s) if s.eq_ignore_ascii_case("default") => None,
                            Some(s) => {
                                Some(crate::parse_enum(canon, s).map_err(|_| InvalidId {
                                    what: canon.into(),
                                    reason: "expected default, accept-edits, auto, or full".into(),
                                })?)
                            }
                            None => None,
                        };
                    }
                    "workspace" => {
                        prof.workspace = match value.map(str::trim).filter(|s| !s.is_empty()) {
                            Some(s) if s.eq_ignore_ascii_case("default") => None,
                            Some(s) => Some(crate::parse_enum(canon, s).map_err(|_| {
                                InvalidId {
                                    what: canon.into(),
                                    reason:
                                        "expected default, project-default, or managed-worktree"
                                            .into(),
                                }
                            })?),
                            None => None,
                        };
                    }
                    _ => unreachable!("canonicalize_config_key verified field"),
                }
            }
        }
        self.validate()?;
        let norm_val = self.get_key(canon)?;
        Ok((canon, norm_val))
    }

    /// Returns the next cycled value (`Some(...)` or `None` for `"auto"` / `"default"`)
    /// when `key` is a discrete enum or boolean field, or `None` when `key` requires text input.
    pub fn next_cycled_value(&self, key: &str) -> Result<Option<Option<String>>, InvalidId> {
        let canon = canonicalize_config_key(key)?;
        let spec = SETTINGS_FIELDS
            .iter()
            .find(|f| f.key == canon)
            .expect("canonical key exists");
        let cur = self.get_key(canon)?;
        let next = match spec.kind {
            SettingFieldKind::RunnerRoute { allow_bb: true } => match cur.as_deref() {
                None => Some(Some("bb".to_owned())),
                Some("bb") => Some(Some("t3".to_owned())),
                Some("t3") => Some(Some("native".to_owned())),
                _ => Some(None),
            },
            SettingFieldKind::RunnerRoute { allow_bb: false } => match cur.as_deref() {
                None => Some(Some("t3".to_owned())),
                Some("t3") => Some(Some("native".to_owned())),
                _ => Some(None),
            },
            SettingFieldKind::Bool => match cur.as_deref() {
                Some("true") => Some(Some("false".to_owned())),
                _ => Some(Some("true".to_owned())),
            },
            SettingFieldKind::Permission => match cur.as_deref() {
                None => Some(Some("accept-edits".to_owned())),
                Some("accept-edits") => Some(Some("auto".to_owned())),
                Some("auto") => Some(Some("full".to_owned())),
                _ => Some(None),
            },
            SettingFieldKind::Workspace => match cur.as_deref() {
                None => Some(Some("project-default".to_owned())),
                Some("project-default") => Some(Some("managed-worktree".to_owned())),
                _ => Some(None),
            },
            SettingFieldKind::Text | SettingFieldKind::Path | SettingFieldKind::Number => None,
        };
        Ok(next)
    }
}

/// Live health badge for one runner backend in `ConfigSnapshot`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RunnerHealthStatus {
    pub runner: RunnerRoute,
    pub configured: bool,
    pub healthy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Live health and ring counters for one `couriers.d/` output adapter in `ConfigSnapshot`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CourierHealthStatus {
    pub name: String,
    pub executable: bool,
    pub rung: u64,
    pub unknown: u64,
    pub refused: u64,
    pub degraded: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct ConfigShow {}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ConfigGet {
    pub key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ConfigSet {
    pub key: String,
    /// New string value; `None` or `""` resets/clears the key to its default.
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub idem: Option<IdemKey>,
}

/// Internal daemon startup command recording an externally edited `config.toml` into `StreamId::Config`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ConfigSyncFile {
    pub config: RexConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConfigValueView {
    pub head: Seq,
    pub key: String,
    pub value: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConfigSnapshot {
    pub head: Seq,
    pub config: RexConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_default_runner: Option<RunnerRoute>,
    #[serde(default)]
    pub runners_health: Vec<RunnerHealthStatus>,
    #[serde(default)]
    pub couriers_health: Vec<CourierHealthStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConfigEvent {
    Updated {
        key: String,
        value: Option<String>,
        config: Box<RexConfig>,
    },
    SyncedFromFile {
        config: Box<RexConfig>,
    },
}

impl ConfigEvent {
    pub fn config(&self) -> &RexConfig {
        match self {
            ConfigEvent::Updated { config, .. } | ConfigEvent::SyncedFromFile { config } => config,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotted_keys_round_trip_and_reject_invalid_agy_bb_or_parent_traversal() {
        let mut cfg = RexConfig::default();
        assert_eq!(cfg.get_key("runners.default_runner").unwrap(), None);

        cfg.set_key("runners.default_runner", Some("t3")).unwrap();
        assert_eq!(
            cfg.get_key("runners.default_runner").unwrap(),
            Some("t3".into())
        );

        assert!(
            cfg.set_key("runners.agy", Some("bb")).is_err(),
            "agy must reject bb route"
        );
        cfg.set_key("runners.antigravity", Some("native")).unwrap();
        assert_eq!(cfg.get_key("runners.agy").unwrap(), Some("native".into()));

        cfg.set_key("harnesses.claude.model", Some("claude-opus-5-5"))
            .unwrap();
        cfg.set_key("harnesses.claude.permission", Some("auto"))
            .unwrap();
        cfg.set_key("harnesses.claude.workspace", Some("managed-worktree"))
            .unwrap();
        assert_eq!(
            cfg.get_key("harnesses.claude.model").unwrap(),
            Some("claude-opus-5-5".into())
        );
        assert_eq!(
            cfg.get_key("harnesses.claude.permission").unwrap(),
            Some("auto".into())
        );
        assert_eq!(
            cfg.get_key("harnesses.claude.workspace").unwrap(),
            Some("managed-worktree".into())
        );

        assert!(
            cfg.set_key("mail.mirror_dir", Some("../escape")).is_err(),
            "parent traversal must be rejected"
        );
        assert!(
            cfg.set_key("mail.reconnect_grace_ms", Some("0")).is_err(),
            "zero reconnect_grace_ms must be rejected"
        );
        cfg.set_key("mail.export_enabled", Some("true")).unwrap();
        assert_eq!(
            cfg.get_key("mail.export_enabled").unwrap(),
            Some("true".into())
        );

        // Clearing optional and scalar fields restores defaults cleanly.
        cfg.set_key("harnesses.claude.model", None).unwrap();
        assert_eq!(cfg.get_key("harnesses.claude.model").unwrap(), None);
        cfg.set_key("mail.reconnect_grace_ms", None).unwrap();
        assert_eq!(cfg.mail.reconnect_grace_ms, DEFAULT_MAIL_RECONNECT_GRACE_MS);
    }
}
