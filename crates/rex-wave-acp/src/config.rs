//! Configuration and provider binary/argument resolution for native ACP stdio runners.

use rex_wave_api::wave::PermissionMode;
use rex_wave_api::{ACP_THREAD_PREFIX, Principal, RunnerKind, SystemPart};
use rex_wave_waves::{HostError, default_bb_data_dir};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

pub const ENV_ACP_AGY_BIN: &str = "REX_WAVE_ACP_AGY_BIN";
pub const ENV_ACP_OPENCODE_BIN: &str = "REX_WAVE_ACP_OPENCODE_BIN";

pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_RETIRE_GRACE: Duration = Duration::from_secs(2);

/// Normalizes provider aliases (`acp:antigravity`, `agy`, `open-code`, ...) to
/// their canonical ACP provider key.
pub fn canonical_acp_provider(raw: &str) -> String {
    let trimmed = raw.trim();
    let stripped = trimmed
        .strip_prefix(ACP_THREAD_PREFIX)
        .or_else(|| trimmed.strip_prefix("acp-"))
        .unwrap_or(trimmed)
        .trim();
    match stripped.to_ascii_lowercase().as_str() {
        "antigravity" | "agy" => "antigravity".to_owned(),
        "opencode" | "open-code" => "opencode".to_owned(),
        other => other.to_owned(),
    }
}

/// Command line specification for spawning one ACP stdio provider server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcpProviderSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    /// Optional flag appended only when `PermissionMode::Full` is requested.
    pub full_permission_arg: Option<String>,
}

impl AcpProviderSpec {
    pub fn new(
        program: impl Into<PathBuf>,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            env: BTreeMap::new(),
            full_permission_arg: None,
        }
    }

    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    pub fn with_full_permission_arg(mut self, arg: impl Into<String>) -> Self {
        self.full_permission_arg = Some(arg.into());
        self
    }

    pub fn effective_args(&self, permission: PermissionMode) -> Vec<String> {
        let mut out = self.args.clone();
        if permission == PermissionMode::Full
            && let Some(ref flag) = self.full_permission_arg
            && !out.iter().any(|existing| existing == flag)
        {
            out.push(flag.clone());
        }
        out
    }
}

/// Configuration for [`crate::AcpBridge`].
#[derive(Clone, Debug)]
pub struct AcpConfig {
    pub providers: BTreeMap<String, AcpProviderSpec>,
    pub state_dir: Option<PathBuf>,
    pub socket_path: Option<PathBuf>,
    pub rex_wave_bin: PathBuf,
    pub bb_data_dir: Option<PathBuf>,
    pub system_principal: Principal,
    pub default_runner: RunnerKind,
    pub handshake_timeout: Duration,
    pub retire_grace: Duration,
}

impl Default for AcpConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl AcpConfig {
    pub fn from_env() -> Self {
        let agy_override = std::env::var_os(ENV_ACP_AGY_BIN).filter(|v| !v.is_empty());
        let opencode_override = std::env::var_os(ENV_ACP_OPENCODE_BIN).filter(|v| !v.is_empty());
        Self::with_env_overrides(
            agy_override.map(PathBuf::from),
            opencode_override.map(PathBuf::from),
        )
    }

    pub fn with_env_overrides(agy_bin: Option<PathBuf>, opencode_bin: Option<PathBuf>) -> Self {
        let mut providers = BTreeMap::new();

        // `agy acp` is the verified ACP stdio subcommand (`--acp` is rejected by `agy`).
        let mut agy_spec = match agy_bin {
            Some(bin) => AcpProviderSpec::new(bin, ["acp"]),
            None => AcpProviderSpec::new("agy", ["acp"])
                .with_full_permission_arg("--dangerously-skip-permissions"),
        };
        if agy_spec.program.as_os_str().is_empty() {
            agy_spec.program = PathBuf::from("agy");
        }
        providers.insert("antigravity".to_owned(), agy_spec);

        let opencode_program = opencode_bin
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| PathBuf::from("opencode"));
        providers.insert(
            "opencode".to_owned(),
            AcpProviderSpec::new(opencode_program, ["acp"]),
        );

        let rex_wave_bin = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("rex-wave"));

        Self {
            providers,
            state_dir: None,
            socket_path: None,
            rex_wave_bin,
            bb_data_dir: default_bb_data_dir(),
            system_principal: Principal::System {
                part: SystemPart::WaveConductor,
            },
            default_runner: RunnerKind::Acp,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            retire_grace: DEFAULT_RETIRE_GRACE,
        }
    }

    pub fn with_provider(mut self, name: &str, spec: AcpProviderSpec) -> Self {
        self.providers.insert(canonical_acp_provider(name), spec);
        self
    }

    pub fn with_state_dir(mut self, dir: PathBuf) -> Self {
        self.state_dir = Some(dir);
        self
    }

    pub fn with_socket_path(mut self, path: PathBuf) -> Self {
        self.socket_path = Some(path);
        self
    }

    pub fn with_rex_wave_bin(mut self, bin: PathBuf) -> Self {
        self.rex_wave_bin = bin;
        self
    }

    pub fn with_bb_data_dir(mut self, dir: Option<PathBuf>) -> Self {
        self.bb_data_dir = dir;
        self
    }

    pub fn with_system_principal(mut self, principal: Principal) -> Self {
        self.system_principal = principal;
        self
    }

    pub fn with_default_runner(mut self, runner: RunnerKind) -> Self {
        self.default_runner = runner;
        self
    }

    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    pub fn with_retire_grace(mut self, grace: Duration) -> Self {
        self.retire_grace = grace;
        self
    }

    pub fn resolve_provider(
        &self,
        raw_provider: &str,
        permission: PermissionMode,
    ) -> Result<(PathBuf, Vec<String>, BTreeMap<String, String>), HostError> {
        let key = canonical_acp_provider(raw_provider);
        let spec = self.providers.get(&key).ok_or_else(|| {
            HostError::Rejected(format!("unsupported ACP provider '{raw_provider}'"))
        })?;
        Ok((
            spec.program.clone(),
            spec.effective_args(permission),
            spec.env.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_provider_normalizes_aliases_and_prefixes() {
        for raw in ["antigravity", "AGY", "agy", "acp:antigravity", "acp-agy"] {
            assert_eq!(canonical_acp_provider(raw), "antigravity", "raw={raw}");
        }
        for raw in ["opencode", "open-code", "acp:opencode", "acp-open-code"] {
            assert_eq!(canonical_acp_provider(raw), "opencode", "raw={raw}");
        }
    }

    #[test]
    fn provider_resolution_honors_defaults_and_overrides() {
        let cfg = AcpConfig::with_env_overrides(None, None);
        let (prog, args, _) = cfg
            .resolve_provider("agy", PermissionMode::AcceptEdits)
            .unwrap();
        assert_eq!(prog, PathBuf::from("agy"));
        assert_eq!(args, vec!["acp"]);

        let (prog_full, args_full, _) = cfg
            .resolve_provider("antigravity", PermissionMode::Full)
            .unwrap();
        assert_eq!(prog_full, PathBuf::from("agy"));
        assert_eq!(args_full, vec!["acp", "--dangerously-skip-permissions"]);

        let (oc_prog, oc_args, _) = cfg
            .resolve_provider("open-code", PermissionMode::Full)
            .unwrap();
        assert_eq!(oc_prog, PathBuf::from("opencode"));
        assert_eq!(oc_args, vec!["acp"]);

        let overridden = AcpConfig::with_env_overrides(
            Some(PathBuf::from("/opt/bin/custom-agy")),
            Some(PathBuf::from("/opt/bin/custom-opencode")),
        );
        let (p1, a1, _) = overridden
            .resolve_provider("antigravity", PermissionMode::Full)
            .unwrap();
        assert_eq!(p1, PathBuf::from("/opt/bin/custom-agy"));
        assert_eq!(a1, vec!["acp"]);

        let (p2, a2, _) = overridden
            .resolve_provider("opencode", PermissionMode::AcceptEdits)
            .unwrap();
        assert_eq!(p2, PathBuf::from("/opt/bin/custom-opencode"));
        assert_eq!(a2, vec!["acp"]);

        assert!(
            overridden
                .resolve_provider("unknown", PermissionMode::Auto)
                .is_err()
        );
    }
}
