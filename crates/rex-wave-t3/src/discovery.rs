//! T3 Code server discovery (`server-runtime.json`) and bearer token bootstrap.

use rex_wave_api::{Principal, SystemPart};
use rex_wave_waves::HostError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_RECONNECT_MIN: Duration = Duration::from_millis(50);
pub const DEFAULT_RECONNECT_MAX: Duration = Duration::from_secs(2);
pub const DEFAULT_TOKEN_LABEL: &str = "rex-wave";
pub const ORCHESTRATION_PROTOCOL_VERSION: &str = "2";

/// Parsed HTTP origin for a T3 Code server (loopback or explicit host:port).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct T3Origin {
    pub host: String,
    pub port: u16,
    pub origin: String,
}

impl T3Origin {
    pub fn parse(raw: &str) -> Result<Self, HostError> {
        let trimmed = raw.trim().trim_end_matches('/');
        let rest = trimmed
            .strip_prefix("http://")
            .or_else(|| trimmed.strip_prefix("ws://"))
            .ok_or_else(|| {
                HostError::Rejected(format!(
                    "unsupported T3 URL scheme (expected http:// or ws://): {raw}"
                ))
            })?;
        let authority = rest.split('/').next().unwrap_or("");
        if authority.is_empty() {
            return Err(HostError::Rejected(format!(
                "missing host:port in T3 URL: {raw}"
            )));
        }

        let (host, port) = parse_authority(authority, raw)?;
        let origin = format!("http://{authority}");
        Ok(T3Origin { host, port, origin })
    }

    pub fn authority(&self) -> String {
        if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    pub fn ws_url_with_ticket(&self, ws_ticket: &str) -> String {
        let encoded = percent_encode_query(ws_ticket);
        format!(
            "ws://{}/ws?orchestrationProtocol={ORCHESTRATION_PROTOCOL_VERSION}&wsTicket={encoded}",
            self.authority()
        )
    }
}

fn parse_authority(authority: &str, raw: &str) -> Result<(String, u16), HostError> {
    if let Some(after_open) = authority.strip_prefix('[') {
        let (host, tail) = after_open
            .split_once(']')
            .ok_or_else(|| HostError::Rejected(format!("unclosed IPv6 host in T3 URL: {raw}")))?;
        if host.is_empty() {
            return Err(HostError::Rejected(format!("empty host in T3 URL: {raw}")));
        }
        let port = match tail.strip_prefix(':') {
            Some(p) => p
                .parse::<u16>()
                .map_err(|_| HostError::Rejected(format!("invalid port in T3 URL: {raw}")))?,
            None if tail.is_empty() => 80,
            None => {
                return Err(HostError::Rejected(format!(
                    "invalid authority in T3 URL: {raw}"
                )));
            }
        };
        return Ok((host.to_owned(), port));
    }

    match authority.rsplit_once(':') {
        Some((host, port_str)) if !host.is_empty() => {
            let port = port_str
                .parse::<u16>()
                .map_err(|_| HostError::Rejected(format!("invalid port in T3 URL: {raw}")))?;
            Ok((host.to_owned(), port))
        }
        None if !authority.is_empty() => Ok((authority.to_owned(), 80)),
        _ => Err(HostError::Rejected(format!(
            "invalid host:port in T3 URL: {raw}"
        ))),
    }
}

pub(crate) fn percent_encode_query(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        let unreserved = byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~');
        if unreserved {
            out.push(byte as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

/// Contents of `<t3_home>/userdata/server-runtime.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerRuntimeInfo {
    #[serde(default)]
    pub version: Option<u32>,
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub server_version: Option<String>,
    #[serde(default)]
    pub owner_id: Option<String>,
}

impl ServerRuntimeInfo {
    pub fn load(path: &Path) -> Result<Self, HostError> {
        let raw = std::fs::read(path).map_err(|e| {
            HostError::Unreachable(format!("read T3 runtime file {}: {e}", path.display()))
        })?;
        serde_json::from_slice(&raw).map_err(|e| {
            HostError::Unreachable(format!("parse T3 runtime file {}: {e}", path.display()))
        })
    }

    pub fn resolve_origin(&self) -> Result<T3Origin, HostError> {
        if let Some(origin) = self
            .origin
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return T3Origin::parse(origin);
        }
        let port = self.port.ok_or_else(|| {
            HostError::Unreachable("T3 server-runtime.json missing both origin and port".into())
        })?;
        let host = self
            .host
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("127.0.0.1");
        let url = if host.contains(':') && !host.starts_with('[') {
            format!("http://[{host}]:{port}")
        } else {
            format!("http://{host}:{port}")
        };
        T3Origin::parse(&url)
    }
}

pub fn runtime_file_path(t3_home: &Path) -> PathBuf {
    t3_home.join("userdata").join("server-runtime.json")
}

pub fn state_db_path(t3_home: &Path) -> PathBuf {
    t3_home.join("userdata").join("statev2.sqlite")
}

pub fn default_t3_bin(t3_home: &Path) -> PathBuf {
    let home_bin = t3_home.join("bin").join("t3");
    if home_bin.is_file() {
        return home_bin;
    }
    if let Some(home) = std::env::var_os("HOME").filter(|h| !h.is_empty()) {
        let local_bin = PathBuf::from(home).join(".local/bin/t3");
        if local_bin.is_file() {
            return local_bin;
        }
    }
    PathBuf::from("t3")
}

pub fn default_bb_data_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("BB_DATA_DIR")
        && !dir.is_empty()
    {
        return Some(PathBuf::from(dir));
    }
    std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .map(|h| PathBuf::from(h).join(".bb"))
}

#[derive(Clone, Debug)]
pub enum T3EndpointSource {
    Explicit(T3Origin),
    RuntimeFile(PathBuf),
}

impl T3EndpointSource {
    pub fn resolve(&self) -> Result<T3Origin, HostError> {
        match self {
            T3EndpointSource::Explicit(origin) => Ok(origin.clone()),
            T3EndpointSource::RuntimeFile(path) => ServerRuntimeInfo::load(path)?.resolve_origin(),
        }
    }
}

#[derive(Clone)]
pub enum T3AuthSource {
    ExplicitToken(String),
    CliSession {
        t3_bin: PathBuf,
        t3_home: PathBuf,
        label: String,
    },
}

impl std::fmt::Debug for T3AuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            T3AuthSource::ExplicitToken(_) => {
                f.debug_tuple("ExplicitToken").field(&"<redacted>").finish()
            }
            T3AuthSource::CliSession {
                t3_bin,
                t3_home,
                label,
            } => f
                .debug_struct("CliSession")
                .field("t3_bin", t3_bin)
                .field("t3_home", t3_home)
                .field("label", label)
                .finish(),
        }
    }
}

/// Configuration for [`crate::T3Bridge`].
#[derive(Clone, Debug)]
pub struct T3Config {
    pub endpoint: T3EndpointSource,
    pub auth: T3AuthSource,
    pub t3_home: Option<PathBuf>,
    pub bb_data_dir: Option<PathBuf>,
    pub state_dir: Option<PathBuf>,
    /// Principal used for `WaveObserve` and room operations.
    pub system_principal: Principal,
    pub default_models: BTreeMap<String, String>,
    pub rpc_timeout: Duration,
    pub connect_timeout: Duration,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
}

impl T3Config {
    pub fn from_home(t3_home: PathBuf) -> Self {
        let t3_bin = default_t3_bin(&t3_home);
        let runtime_file = runtime_file_path(&t3_home);
        T3Config {
            endpoint: T3EndpointSource::RuntimeFile(runtime_file),
            auth: T3AuthSource::CliSession {
                t3_bin,
                t3_home: t3_home.clone(),
                label: DEFAULT_TOKEN_LABEL.to_owned(),
            },
            t3_home: Some(t3_home),
            bb_data_dir: default_bb_data_dir(),
            state_dir: None,
            system_principal: Principal::System {
                part: SystemPart::T3Bridge,
            },
            default_models: BTreeMap::new(),
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            reconnect_min: DEFAULT_RECONNECT_MIN,
            reconnect_max: DEFAULT_RECONNECT_MAX,
        }
    }

    pub fn explicit(url: &str, token: impl Into<String>) -> Result<Self, HostError> {
        let origin = T3Origin::parse(url)?;
        Ok(T3Config {
            endpoint: T3EndpointSource::Explicit(origin),
            auth: T3AuthSource::ExplicitToken(token.into()),
            t3_home: None,
            bb_data_dir: None,
            state_dir: None,
            system_principal: Principal::System {
                part: SystemPart::T3Bridge,
            },
            default_models: BTreeMap::new(),
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            reconnect_min: DEFAULT_RECONNECT_MIN,
            reconnect_max: DEFAULT_RECONNECT_MAX,
        })
    }

    pub fn with_state_dir(mut self, dir: PathBuf) -> Self {
        self.state_dir = Some(dir);
        self
    }

    pub fn with_bb_data_dir(mut self, dir: PathBuf) -> Self {
        self.bb_data_dir = Some(dir);
        self
    }

    pub fn with_t3_home(mut self, dir: PathBuf) -> Self {
        self.t3_home = Some(dir);
        self
    }

    pub fn with_system_principal(mut self, principal: Principal) -> Self {
        self.system_principal = principal;
        self
    }

    pub fn with_default_model(
        mut self,
        provider: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        self.default_models.insert(provider.into(), model.into());
        self
    }
}

#[derive(Clone)]
pub(crate) struct TokenCache {
    source: T3AuthSource,
    cached: Arc<Mutex<Option<String>>>,
}

impl TokenCache {
    pub(crate) fn new(source: T3AuthSource) -> Self {
        let initial = match &source {
            T3AuthSource::ExplicitToken(tok) => Some(tok.clone()),
            T3AuthSource::CliSession { .. } => None,
        };
        TokenCache {
            source,
            cached: Arc::new(Mutex::new(initial)),
        }
    }

    pub(crate) fn can_refresh(&self) -> bool {
        matches!(self.source, T3AuthSource::CliSession { .. })
    }

    pub(crate) async fn invalidate(&self) {
        if self.can_refresh() {
            let mut guard = self.cached.lock().await;
            *guard = None;
        }
    }

    pub(crate) async fn bearer_token(&self, timeout: Duration) -> Result<String, HostError> {
        let mut guard = self.cached.lock().await;
        if let Some(tok) = guard.as_ref() {
            return Ok(tok.clone());
        }
        let minted = match &self.source {
            T3AuthSource::ExplicitToken(tok) => tok.clone(),
            T3AuthSource::CliSession {
                t3_bin,
                t3_home,
                label,
            } => {
                let bin = t3_bin.clone();
                let home = t3_home.clone();
                let lbl = label.clone();
                let handle =
                    tokio::task::spawn_blocking(move || issue_cli_session_token(&bin, &home, &lbl));
                tokio::time::timeout(timeout, handle)
                    .await
                    .map_err(|_| HostError::Unreachable("t3 auth session issue timed out".into()))?
                    .map_err(|e| {
                        HostError::Unreachable(format!("t3 auth session task failed: {e}"))
                    })??
            }
        };
        *guard = Some(minted.clone());
        Ok(minted)
    }
}

pub fn issue_cli_session_token(
    t3_bin: &Path,
    t3_home: &Path,
    label: &str,
) -> Result<String, HostError> {
    let output = Command::new(t3_bin)
        .arg("auth")
        .arg("session")
        .arg("issue")
        .arg("--base-dir")
        .arg(t3_home)
        .arg("--label")
        .arg(label)
        .arg("--token-only")
        .output()
        .map_err(|e| {
            HostError::Unreachable(format!("exec {} auth session issue: {e}", t3_bin.display()))
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(HostError::Unreachable(format!(
            "t3 auth session issue exited {}: {}",
            output.status,
            stderr.trim()
        )));
    }
    let token = String::from_utf8(output.stdout)
        .map_err(|_| HostError::Unreachable("t3 auth session issue returned non-UTF8".into()))?;
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return Err(HostError::Unreachable(
            "t3 auth session issue returned empty token".into(),
        ));
    }
    Ok(trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_origins_and_runtime_json() {
        let origin = T3Origin::parse("http://127.0.0.1:3773/").unwrap();
        assert_eq!(origin.host, "127.0.0.1");
        assert_eq!(origin.port, 3773);
        assert_eq!(origin.origin, "http://127.0.0.1:3773");
        assert_eq!(
            origin.ws_url_with_ticket("abc+/=1"),
            "ws://127.0.0.1:3773/ws?orchestrationProtocol=2&wsTicket=abc%2B%2F%3D1"
        );

        let info: ServerRuntimeInfo = serde_json::from_str(
            r#"{"version":1,"pid":123,"host":"127.0.0.1","port":4551,"origin":"http://127.0.0.1:4551"}"#,
        )
        .unwrap();
        let resolved = info.resolve_origin().unwrap();
        assert_eq!(resolved.port, 4551);
        assert_eq!(resolved.origin, "http://127.0.0.1:4551");

        let fallback_info: ServerRuntimeInfo = serde_json::from_str(r#"{"port":4552}"#).unwrap();
        assert_eq!(
            fallback_info.resolve_origin().unwrap().origin,
            "http://127.0.0.1:4552"
        );

        assert!(T3Origin::parse("https://example.com").is_err());

        let cfg = T3Config::explicit("http://127.0.0.1:3773", "super_secret_bearer").unwrap();
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("super_secret_bearer"));
        assert!(dbg.contains("<redacted>"));
    }
}
