//! # rex-wave-transport
//!
//! The only crate that knows bytes. Everything above it speaks `api::Client`.
//!
//! Wire: one multiplexed connection per client, frames are
//! `u32-BE length || JSON`. Calls and watches carry a client-chosen `id`, so
//! many watches (sidebar tree, stage) share one socket and a stage retarget or
//! drop is `Unwatch{old}` + `Watch{new}` with no reconnect.
//!
//! Auth happens once per connection, in `Hello`, and yields the session
//! [`Principal`]:
//! * Unix socket + peer uid == daemon uid -> `Operator{seat}` (TUI, CLI);
//! * `Token` minted by `rex-wave token --seat sam` -> `Seat{sam}` (MCP);
//! * the BB bridge runs in-process and never uses this crate.
//!
//! [`RemoteClient`] outlives its socket. When the connection drops (a daemon
//! restart unlinks and rebinds the socket), it redials with backoff, sends
//! `Hello` again, and re-opens every live watch from the last cursor it
//! delivered, so callers keep one `FrameStream` across the restart. Calls in
//! flight at the drop fail with the cause, since the daemon may or may not have
//! run them.

use futures_util::StreamExt;
use rex_wave_api::*;
use rex_wave_runtime::{Router, Session};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{AbortHandle, JoinSet};

const MAX_FRAME: usize = 16 * 1024 * 1024;
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
const WATCH_BUFFER: usize = 128;
const REDIAL_MIN: Duration = Duration::from_millis(50);
const REDIAL_MAX: Duration = Duration::from_secs(2);
const MAX_LOG_LINE: usize = 512;

fn log_daemon(args: std::fmt::Arguments<'_>) {
    let line = args.to_string();
    if line.len() <= MAX_LOG_LINE {
        eprintln!("rex-wave daemon: {line}");
    } else {
        let mut end = MAX_LOG_LINE;
        while !line.is_char_boundary(end) {
            end -= 1;
        }
        eprintln!("rex-wave daemon: {}...", &line[..end]);
    }
}

/// Daemon-side log line on stderr. Only the serving half logs: a client may be
/// drawing a full-screen TUI on the same terminal.
macro_rules! daemon_log {
    ($($arg:tt)*) => {
        log_daemon(format_args!($($arg)*))
    };
}

mod wire {
    use rex_wave_api::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(tag = "t", rename_all = "snake_case")]
    pub enum ClientFrame {
        Hello {
            credential: super::Credential,
            client: String,
        },
        Call {
            id: u64,
            cmd: WireCommand,
        },
        Watch {
            id: u64,
            interest: Interest,
            from: Cursor,
        },
        Unwatch {
            id: u64,
        },
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(tag = "t", rename_all = "snake_case")]
    pub enum ServerFrame {
        Welcome {
            result: Result<Principal, ApiError>,
            daemon: String,
        },
        Reply {
            id: u64,
            result: Result<WireReply, ApiError>,
        },
        WatchOpened {
            id: u64,
            result: Result<(), ApiError>,
        },
        Frame {
            id: u64,
            frame: Frame,
        },
        WatchEnded {
            id: u64,
            error: Option<ApiError>,
        },
    }

    impl ServerFrame {
        pub fn kind(&self) -> &'static str {
            match self {
                ServerFrame::Welcome { .. } => "welcome",
                ServerFrame::Reply { .. } => "reply",
                ServerFrame::WatchOpened { .. } => "watch_opened",
                ServerFrame::Frame { .. } => "frame",
                ServerFrame::WatchEnded { .. } => "watch_ended",
            }
        }

        /// The frame to send instead when this one cannot be encoded: the
        /// same answer slot, carrying `error`. A watch frame ends its watch.
        pub fn fallback(&self, error: ApiError) -> ServerFrame {
            match self {
                ServerFrame::Welcome { daemon, .. } => ServerFrame::Welcome {
                    result: Err(error),
                    daemon: daemon.clone(),
                },
                ServerFrame::Reply { id, .. } => ServerFrame::Reply {
                    id: *id,
                    result: Err(error),
                },
                ServerFrame::WatchOpened { id, .. } => ServerFrame::WatchOpened {
                    id: *id,
                    result: Err(error),
                },
                ServerFrame::Frame { id, .. } | ServerFrame::WatchEnded { id, .. } => {
                    ServerFrame::WatchEnded {
                        id: *id,
                        error: Some(error),
                    }
                }
            }
        }
    }
}

fn encode<T: Serialize>(msg: &T) -> io::Result<Vec<u8>> {
    let bytes =
        serde_json::to_vec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if bytes.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame exceeds 16 MiB",
        ));
    }
    Ok(bytes)
}

async fn write_bytes<W: AsyncWrite + Unpin>(w: &mut W, bytes: &[u8]) -> io::Result<()> {
    w.write_u32(bytes.len() as u32).await?;
    w.write_all(bytes).await?;
    w.flush().await
}

async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(w: &mut W, msg: &T) -> io::Result<()> {
    write_bytes(w, &encode(msg)?).await
}

/// Write `frame`, or its [`wire::ServerFrame::fallback`] if it cannot be
/// encoded, so one bad payload answers one request instead of closing the
/// connection. Errors only when the socket itself fails.
async fn write_server_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    conn: u64,
    frame: &wire::ServerFrame,
) -> io::Result<()> {
    let bytes = match encode(frame) {
        Ok(bytes) => bytes,
        Err(e) => {
            let kind = frame.kind();
            daemon_log!(
                "conn #{conn}: cannot encode {kind} frame ({e}); sending an internal error"
            );
            encode(&frame.fallback(ApiError::Internal(format!(
                "daemon could not encode {kind} frame: {e}"
            ))))?
        }
    };
    write_bytes(w, &bytes).await
}

async fn read_frame<R: AsyncRead + Unpin, T: DeserializeOwned>(r: &mut R) -> io::Result<Option<T>> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds 16 MiB",
        ));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    let msg =
        serde_json::from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(msg))
}

/// Aborts a spawned task when dropped, so a connection's helper tasks die with it.
struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Where the daemon listens.
#[derive(Clone, Debug)]
pub enum Endpoint {
    /// Default: `$XDG_STATE_HOME/rex-wave/daemon.sock` (mode 0600).
    Unix(PathBuf),
    /// Reserved for a browser / embedded GUI over loopback with a token.
    WebSocket(String),
}

impl Endpoint {
    pub fn default_unix() -> Self {
        let base = std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")))
            .unwrap_or_else(std::env::temp_dir);
        Endpoint::Unix(base.join("rex-wave/daemon.sock"))
    }
}

/// How a client proves who it is. Sent once, in `Hello`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Credential {
    /// Trust the Unix peer uid (operator on the same account).
    PeerUid,
    /// A capability token naming one seat, minted by the daemon.
    Token { token: String },
}

/// Maps a credential (plus the kernel-verified peer uid, on Unix) to a principal.
pub trait Authenticator: Send + Sync + 'static {
    fn authenticate(
        &self,
        peer_uid: Option<u32>,
        credential: &Credential,
    ) -> Result<Principal, ApiError>;
}

/// Standard authenticator: `PeerUid` succeeds when the peer's Unix uid equals
/// `daemon_uid`, and `Token` looks up a seat in a token map.
pub struct TableAuthenticator {
    pub operator: SeatName,
    pub daemon_uid: Option<u32>,
    pub tokens: BTreeMap<String, SeatName>,
}

impl Authenticator for TableAuthenticator {
    fn authenticate(
        &self,
        peer_uid: Option<u32>,
        credential: &Credential,
    ) -> Result<Principal, ApiError> {
        match credential {
            Credential::PeerUid => match (peer_uid, self.daemon_uid) {
                (Some(peer), Some(daemon)) if peer == daemon => Ok(Principal::Operator {
                    seat: self.operator.clone(),
                }),
                _ => Err(ApiError::Denied {
                    principal: "anonymous".into(),
                    action: "peer-uid hello".into(),
                }),
            },
            Credential::Token { token } => match self.tokens.get(token) {
                Some(seat) => Ok(Principal::Seat { seat: seat.clone() }),
                None => Err(ApiError::Denied {
                    principal: "anonymous".into(),
                    action: "invalid token".into(),
                }),
            },
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let max_len = a.len().max(b.len());
    let mut diff = a.len() ^ b.len();
    for i in 0..max_len {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= usize::from(x ^ y);
    }
    diff == 0
}

fn validate_token_principal(token: &str, principal: &Principal) -> Result<(), ApiError> {
    if token.is_empty() || token.chars().any(char::is_control) {
        return Err(ApiError::Invalid(InvalidId {
            what: "token".into(),
            reason: "must be non-empty without control characters".into(),
        }));
    }
    if matches!(principal, Principal::System { .. }) {
        return Err(ApiError::Denied {
            principal: "anonymous".into(),
            action: "bearer tokens may not authenticate as system".into(),
        });
    }
    Ok(())
}

/// The largest token store or worker token file read.
const MAX_TOKEN_FILE_BYTES: u64 = 4 * 1024 * 1024;

/// Read a private token file: a regular file (never through a symlink),
/// mode `0600`, owned by `expected_uid` when given, judged by the opened
/// handle. A missing file is `None`; any other refusal is `Denied`.
pub fn read_private_file(
    path: &Path,
    expected_uid: Option<u32>,
    what: &str,
) -> Result<Option<Vec<u8>>, ApiError> {
    match rex_wave_fs::read_private_regular(path, expected_uid, MAX_TOKEN_FILE_BYTES) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::InvalidInput | io::ErrorKind::PermissionDenied
            ) =>
        {
            Err(ApiError::Denied {
                principal: "anonymous".into(),
                action: format!("{what}: {e}"),
            })
        }
        Err(e) => Err(ApiError::Internal(format!("{what}: {e}"))),
    }
}

/// Refuse a directory of private files unless it is a real directory (not a
/// symlink), owned by `expected_uid` when given, with no group/other bits.
fn verify_private_dir(dir: &Path, expected_uid: Option<u32>) -> Result<bool, ApiError> {
    let denied = |why: &str| ApiError::Denied {
        principal: "anonymous".into(),
        action: why.into(),
    };
    let meta = match rex_wave_fs::dir_metadata_nofollow(dir) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) if e.kind() == io::ErrorKind::InvalidInput => {
            return Err(denied("worker-tokens path must be a real directory"));
        }
        Err(e) => return Err(ApiError::Internal(format!("worker-tokens metadata: {e}"))),
    };
    if let Some(uid) = expected_uid
        && meta.uid() != uid
    {
        return Err(denied("worker-tokens directory owned by another uid"));
    }
    if meta.permissions().mode() & 0o077 != 0 {
        return Err(denied("worker-tokens directory must be mode 0700"));
    }
    Ok(true)
}

/// Validates a single safe filename for `<state_dir>/worker-tokens/<id>`.
pub fn is_safe_worker_token_id(id: &str) -> bool {
    rex_wave_fs::SafeName::parse_plain(id).is_ok()
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum TokenMapValue {
    Seat(SeatName),
    Principal(Principal),
    LegacyWorker {
        wave: WaveSlug,
        role: RoleName,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread_id: Option<String>,
    },
}

impl TokenMapValue {
    fn from_principal(
        principal: &Principal,
        legacy_thread_id: Option<&str>,
    ) -> Result<Self, ApiError> {
        match principal {
            Principal::Seat { seat } => Ok(TokenMapValue::Seat(seat.clone())),
            Principal::Worker { wave, role } if legacy_thread_id.is_some() => {
                Ok(TokenMapValue::LegacyWorker {
                    wave: wave.clone(),
                    role: role.clone(),
                    thread_id: legacy_thread_id.map(str::to_owned),
                })
            }
            Principal::Operator { .. } | Principal::Worker { .. } => {
                Ok(TokenMapValue::Principal(principal.clone()))
            }
            Principal::System { .. } => Err(ApiError::Denied {
                principal: "anonymous".into(),
                action: "bearer tokens may not authenticate as system".into(),
            }),
        }
    }

    fn into_principal_and_thread(self) -> (Principal, Option<String>) {
        match self {
            TokenMapValue::Seat(seat) => (Principal::Seat { seat }, None),
            TokenMapValue::Principal(p) => (p, None),
            TokenMapValue::LegacyWorker {
                wave,
                role,
                thread_id,
            } => (Principal::Worker { wave, role }, thread_id),
        }
    }
}

fn verify_legacy_worker_thread_token(
    tokens_path: &Path,
    thread_id: &str,
    expected_token: &str,
    expected_principal: &Principal,
    expected_uid: Option<u32>,
) -> bool {
    if !is_safe_worker_token_id(thread_id) {
        return false;
    }
    let dir = tokens_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("worker-tokens");
    if !verify_private_dir(&dir, expected_uid).unwrap_or(false) {
        return false;
    }
    let file_path = dir.join(thread_id);
    let Ok(Some(bytes)) = read_private_file(&file_path, expected_uid, "legacy worker token") else {
        return false;
    };
    let Ok(raw) = String::from_utf8(bytes) else {
        return false;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed.starts_with('{') {
        let Ok(parsed) = serde_json::from_str::<WorkerTokenFile>(trimmed) else {
            return false;
        };
        parsed.principal == *expected_principal
            && constant_time_eq(parsed.token.as_bytes(), expected_token.as_bytes())
    } else {
        constant_time_eq(trimmed.as_bytes(), expected_token.as_bytes())
    }
}

/// On-disk JSON payload for one per-worker token file under `<state_dir>/worker-tokens/`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerTokenFile {
    pub token: String,
    pub principal: Principal,
}

/// Principal-scoped capability token table supporting `Seat`, `Worker`, and
/// `Operator` tokens while refusing `System` tokens.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PrincipalTokenTable {
    tokens: BTreeMap<String, Principal>,
    legacy_thread_ids: BTreeMap<String, String>,
}

impl PrincipalTokenTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        token: impl Into<String>,
        principal: Principal,
    ) -> Result<(), ApiError> {
        let token = token.into();
        validate_token_principal(&token, &principal)?;
        if let Some(existing) = self.tokens.get(&token)
            && existing != &principal
        {
            return Err(ApiError::conflict(
                "duplicate token bound to different principals",
            ));
        }
        self.tokens.insert(token, principal);
        Ok(())
    }

    pub fn insert_seat(
        &mut self,
        token: impl Into<String>,
        seat: SeatName,
    ) -> Result<(), ApiError> {
        self.insert(token, Principal::Seat { seat })
    }

    pub fn insert_worker(
        &mut self,
        token: impl Into<String>,
        wave: WaveSlug,
        role: RoleName,
    ) -> Result<(), ApiError> {
        self.insert(token, Principal::Worker { wave, role })
    }

    pub fn insert_operator(
        &mut self,
        token: impl Into<String>,
        seat: SeatName,
    ) -> Result<(), ApiError> {
        self.insert(token, Principal::Operator { seat })
    }

    pub fn extend_seats(&mut self, seats: BTreeMap<String, SeatName>) -> Result<(), ApiError> {
        for (token, seat) in seats {
            self.insert_seat(token, seat)?;
        }
        Ok(())
    }

    pub fn merge(&mut self, other: PrincipalTokenTable) -> Result<(), ApiError> {
        for (token, principal) in other.tokens {
            self.insert(token.clone(), principal)?;
            if let Some(tid) = other.legacy_thread_ids.get(&token) {
                self.legacy_thread_ids.insert(token, tid.clone());
            }
        }
        Ok(())
    }

    pub fn get(&self, candidate: &str) -> Option<&Principal> {
        let mut matched: Option<&Principal> = None;
        for (stored, principal) in &self.tokens {
            if constant_time_eq(stored.as_bytes(), candidate.as_bytes()) {
                matched = Some(principal);
            }
        }
        matched
    }

    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Principal)> {
        self.tokens.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn remove_principal(&mut self, principal: &Principal) {
        let legacy = &self.legacy_thread_ids;
        self.tokens
            .retain(|tok, existing| existing != principal || legacy.contains_key(tok));
    }

    /// Atomically persists the table to `tokens.json` with mode `0600`.
    /// Seats serialize as bare strings for backward compatibility while workers
    /// and operators serialize as tagged [`Principal`] objects; thread-scoped
    /// legacy worker entries retain their `thread_id`; `System` is refused.
    pub fn save_tokens_file(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut out: BTreeMap<&str, TokenMapValue> = BTreeMap::new();
        for (tok, principal) in &self.tokens {
            validate_token_principal(tok, principal)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
            let val = TokenMapValue::from_principal(
                principal,
                self.legacy_thread_ids.get(tok).map(String::as_str),
            )
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
            out.insert(tok.as_str(), val);
        }
        let encoded = serde_json::to_vec_pretty(&out)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        rex_wave_fs::write_atomic_path(path, &encoded, 0o600, false)?;
        Ok(())
    }

    /// Loads `tokens.json` in either legacy `{token: "seat"}`, legacy
    /// `{token: {wave, role, thread_id?}}`, or principal-scoped
    /// `{token: {kind: ...}}` form, enforcing mode `0600` and matching UID.
    /// Legacy worker entries carrying `thread_id` are accepted only while a
    /// matching private `<state_dir>/worker-tokens/<thread_id>` file exists.
    pub fn load_tokens_file(path: &Path, expected_uid: Option<u32>) -> Result<Self, ApiError> {
        let Some(raw) = read_private_file(path, expected_uid, "token store")? else {
            return Ok(Self::new());
        };
        let decoded: BTreeMap<String, TokenMapValue> = serde_json::from_slice(&raw)
            .map_err(|e| ApiError::Internal(format!("decode tokens: {e}")))?;
        let mut table = Self::new();
        for (token, entry) in decoded {
            let (principal, legacy_tid) = entry.into_principal_and_thread();
            if let Some(tid) = legacy_tid {
                if !verify_legacy_worker_thread_token(path, &tid, &token, &principal, expected_uid)
                {
                    continue;
                }
                table.insert(token.clone(), principal)?;
                table.legacy_thread_ids.insert(token, tid);
            } else {
                table.insert(token, principal)?;
            }
        }
        Ok(table)
    }

    /// Loads mode-`0600` per-worker token files from `<state_dir>/worker-tokens/`
    /// when present. Missing directory yields an empty table.
    pub fn load_worker_tokens_dir(dir: &Path, expected_uid: Option<u32>) -> Result<Self, ApiError> {
        if !verify_private_dir(dir, expected_uid)? {
            return Ok(Self::new());
        }
        let entries = std::fs::read_dir(dir)
            .map_err(|e| ApiError::Internal(format!("read worker-tokens dir: {e}")))?;
        let mut paths = Vec::new();
        for entry in entries {
            let entry =
                entry.map_err(|e| ApiError::Internal(format!("iterate worker-tokens dir: {e}")))?;
            let fname = entry.file_name();
            if fname.to_string_lossy().starts_with('.') {
                continue;
            }
            paths.push(entry.path());
        }
        paths.sort();

        let mut table = Self::new();
        for path in paths {
            let label = format!("worker token {}", path.display());
            let raw = match read_private_file(&path, expected_uid, &label) {
                Ok(Some(bytes)) => bytes,
                Ok(None) => continue,
                Err(e) => {
                    daemon_log!("skipping {label}: {e}");
                    continue;
                }
            };
            let file: WorkerTokenFile = match serde_json::from_slice(&raw) {
                Ok(parsed) => parsed,
                Err(e) => {
                    daemon_log!("skipping {label}: invalid WorkerTokenFile JSON: {e}");
                    continue;
                }
            };
            if !matches!(file.principal, Principal::Worker { .. }) {
                daemon_log!(
                    "skipping {label}: worker-tokens directory only allows Principal::Worker"
                );
                continue;
            }
            if let Err(e) = table.insert(file.token, file.principal) {
                daemon_log!("skipping {label}: {e}");
            }
        }
        Ok(table)
    }

    /// Loads both `tokens.json` and `<state_dir>/worker-tokens/` into one table.
    /// A malformed or non-`0700` `worker-tokens/` directory is logged and skipped
    /// so `tokens.json` seat handshakes remain available while worker tokens stay denied.
    pub fn load_state_dir(
        tokens_path: &Path,
        worker_tokens_dir: &Path,
        expected_uid: Option<u32>,
    ) -> Result<Self, ApiError> {
        let mut table = Self::load_tokens_file(tokens_path, expected_uid)?;
        match Self::load_worker_tokens_dir(worker_tokens_dir, expected_uid) {
            Ok(workers) => table.merge(workers)?,
            Err(e) => {
                daemon_log!(
                    "skipping worker-tokens directory {}: {e}",
                    worker_tokens_dir.display()
                );
            }
        }
        Ok(table)
    }
}

/// Atomically writes one mode-`0600` per-worker token file under `dir/<id>`.
pub fn write_worker_token_file(
    dir: &Path,
    id: &str,
    token: &str,
    principal: &Principal,
) -> io::Result<PathBuf> {
    if !is_safe_worker_token_id(id) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "worker token file id must be a single safe filename",
        ));
    }
    if !matches!(principal, Principal::Worker { .. }) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "worker token file only allows Principal::Worker",
        ));
    }
    validate_token_principal(token, principal)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    rex_wave_fs::ensure_private_dir(dir)?;

    let payload = WorkerTokenFile {
        token: token.to_owned(),
        principal: principal.clone(),
    };
    let encoded = serde_json::to_vec_pretty(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let dest = dir.join(id);
    rex_wave_fs::write_atomic_path(&dest, &encoded, 0o600, false)?;
    Ok(dest)
}

/// Authenticator backed by an in-memory [`PrincipalTokenTable`].
pub struct PrincipalAuthenticator {
    pub operator: SeatName,
    pub daemon_uid: Option<u32>,
    pub tokens: PrincipalTokenTable,
}

impl Authenticator for PrincipalAuthenticator {
    fn authenticate(
        &self,
        peer_uid: Option<u32>,
        credential: &Credential,
    ) -> Result<Principal, ApiError> {
        match credential {
            Credential::PeerUid => match (peer_uid, self.daemon_uid) {
                (Some(peer), Some(daemon)) if peer == daemon => Ok(Principal::Operator {
                    seat: self.operator.clone(),
                }),
                _ => Err(ApiError::Denied {
                    principal: "anonymous".into(),
                    action: "peer-uid hello".into(),
                }),
            },
            Credential::Token { token } => match self.tokens.get(token) {
                Some(principal)
                    if !matches!(principal, Principal::System { .. })
                        && !matches!(
                            principal,
                            Principal::Seat { seat } if seat == &self.operator
                        ) =>
                {
                    Ok(principal.clone())
                }
                _ => Err(ApiError::Denied {
                    principal: "anonymous".into(),
                    action: "invalid token".into(),
                }),
            },
        }
    }
}

/// Reloading authenticator reading both `<state_dir>/tokens.json` and
/// `<state_dir>/worker-tokens/` on each `Token` handshake.
pub struct StateDirAuthenticator {
    pub operator: SeatName,
    pub daemon_uid: Option<u32>,
    pub tokens_path: PathBuf,
    pub worker_tokens_dir: PathBuf,
}

impl StateDirAuthenticator {
    pub fn from_state_dir(operator: SeatName, daemon_uid: Option<u32>, state_dir: &Path) -> Self {
        Self {
            operator,
            daemon_uid,
            tokens_path: state_dir.join("tokens.json"),
            worker_tokens_dir: state_dir.join("worker-tokens"),
        }
    }
}

impl Authenticator for StateDirAuthenticator {
    fn authenticate(
        &self,
        peer_uid: Option<u32>,
        credential: &Credential,
    ) -> Result<Principal, ApiError> {
        let tokens = match credential {
            Credential::PeerUid => PrincipalTokenTable::new(),
            Credential::Token { .. } => PrincipalTokenTable::load_state_dir(
                &self.tokens_path,
                &self.worker_tokens_dir,
                self.daemon_uid,
            )?,
        };
        let auth = PrincipalAuthenticator {
            operator: self.operator.clone(),
            daemon_uid: self.daemon_uid,
            tokens,
        };
        auth.authenticate(peer_uid, credential)
    }
}

/// Where and how to (re)dial. Kept for the life of a [`RemoteClient`].
struct Dialer {
    endpoint: Endpoint,
    credential: Credential,
    client_name: String,
}

impl Dialer {
    /// Connect, send `Hello`, await `Welcome`.
    async fn dial(&self) -> Result<(Principal, OwnedReadHalf, OwnedWriteHalf), ApiError> {
        let stream = match &self.endpoint {
            Endpoint::Unix(path) => UnixStream::connect(path)
                .await
                .map_err(|e| ApiError::Transport(e.to_string()))?,
            Endpoint::WebSocket(_) => {
                return Err(ApiError::Transport(
                    "websocket endpoint is not enabled".into(),
                ));
            }
        };
        let (mut reader, mut writer) = stream.into_split();
        write_frame(
            &mut writer,
            &wire::ClientFrame::Hello {
                credential: self.credential.clone(),
                client: self.client_name.clone(),
            },
        )
        .await
        .map_err(|e| ApiError::Transport(e.to_string()))?;
        let welcome: wire::ServerFrame =
            tokio::time::timeout(HELLO_TIMEOUT, read_frame(&mut reader))
                .await
                .map_err(|_| ApiError::Timeout)?
                .map_err(|e| ApiError::Transport(e.to_string()))?
                .ok_or_else(|| ApiError::Transport("server closed before welcome".into()))?;
        match welcome {
            wire::ServerFrame::Welcome { result, .. } => Ok((result?, reader, writer)),
            other => Err(ApiError::Transport(format!(
                "expected welcome, got {other:?}"
            ))),
        }
    }
}

enum ClientReq {
    Call {
        cmd: WireCommand,
        reply: oneshot::Sender<Result<WireReply, ApiError>>,
    },
    Watch {
        interest: Interest,
        from: Cursor,
        reply: oneshot::Sender<Result<FrameStream, ApiError>>,
    },
    /// Sent by a dropped [`FrameStream`].
    Unwatch { id: u64 },
}

/// A client session that survives daemon restarts. Implements `api::Client`.
#[derive(Clone)]
pub struct RemoteClient {
    me: Principal,
    req_tx: mpsc::UnboundedSender<ClientReq>,
    connected: watch::Receiver<bool>,
    /// Why the session ended for good, once it has.
    closed: Arc<OnceLock<ApiError>>,
}

impl std::fmt::Debug for RemoteClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteClient")
            .field("me", &self.me)
            .field("connected", &self.connected())
            .finish()
    }
}

impl RemoteClient {
    /// Connect, send `Hello`, await `Welcome`. Bound to one principal for life:
    /// a redial that authenticates as anyone else ends the session.
    pub async fn connect(
        endpoint: Endpoint,
        credential: Credential,
        client_name: &str,
    ) -> Result<Self, ApiError> {
        let dialer = Arc::new(Dialer {
            endpoint,
            credential,
            client_name: client_name.to_owned(),
        });
        let (me, reader, writer) = dialer.dial().await?;
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        let (connected_tx, connected) = watch::channel(true);
        let closed = Arc::new(OnceLock::new());
        let session = Connection {
            dialer,
            me: me.clone(),
            req_rx,
            unwatch_tx: req_tx.downgrade(),
            connected: connected_tx,
            closed: closed.clone(),
            next_id: 1,
            watches: BTreeMap::new(),
            generations: Generations::default(),
        };
        tokio::spawn(session.run(reader, writer));
        Ok(RemoteClient {
            me,
            req_tx,
            connected,
            closed,
        })
    }

    /// Whether the socket is up right now. False while redialing.
    pub fn connected(&self) -> bool {
        *self.connected.borrow()
    }

    /// Follows [`RemoteClient::connected`], for status badges.
    pub fn watch_connection(&self) -> watch::Receiver<bool> {
        self.connected.clone()
    }

    fn request<T: Send>(
        &self,
        req: impl FnOnce(oneshot::Sender<Result<T, ApiError>>) -> ClientReq + Send,
    ) -> impl Future<Output = Result<T, ApiError>> + Send {
        let tx = self.req_tx.clone();
        let closed = self.closed.clone();
        async move {
            let gone = || {
                closed
                    .get()
                    .cloned()
                    .unwrap_or_else(|| ApiError::Transport("connection closed".into()))
            };
            let (reply, rx) = oneshot::channel();
            tx.send(req(reply)).map_err(|_| gone())?;
            rx.await.map_err(|_| gone())?
        }
    }
}

impl Client for RemoteClient {
    fn me(&self) -> &Principal {
        &self.me
    }

    fn call_wire(
        &self,
        cmd: WireCommand,
    ) -> impl Future<Output = Result<WireReply, ApiError>> + Send {
        self.request(|reply| ClientReq::Call { cmd, reply })
    }

    fn watch(
        &self,
        interest: Interest,
        from: Cursor,
    ) -> impl Future<Output = Result<FrameStream, ApiError>> + Send {
        self.request(|reply| ClientReq::Watch {
            interest,
            from,
            reply,
        })
    }
}

struct UnwatchGuard {
    id: u64,
    req_tx: mpsc::UnboundedSender<ClientReq>,
    armed: bool,
}

impl Drop for UnwatchGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.req_tx.send(ClientReq::Unwatch { id: self.id });
        }
    }
}

fn frame_stream(rx: mpsc::Receiver<Result<Frame, ApiError>>, guard: UnwatchGuard) -> FrameStream {
    Box::pin(futures_util::stream::unfold(
        (rx, guard),
        |(mut rx, mut guard)| async move {
            match rx.recv().await {
                Some(item) => Some((item, (rx, guard))),
                None => {
                    guard.armed = false;
                    None
                }
            }
        },
    ))
}

/// Maps each connection's stage generations into one increasing sequence.
/// The daemon numbers generations per session, so after a redial they restart
/// at 1, and a caller that drops frames older than its newest generation (the
/// TUI does) would drop every frame from the new session.
#[derive(Default)]
struct Generations {
    offset: u64,
    high: u64,
}

impl Generations {
    /// Called as each connection starts.
    fn rebase(&mut self) {
        self.offset = self.high;
    }

    fn map(&mut self, generation: FocusGeneration) -> FocusGeneration {
        let mapped = generation.0 + self.offset;
        self.high = self.high.max(mapped);
        FocusGeneration(mapped)
    }
}

/// A watch the caller still holds, with what it takes to re-open it.
struct LiveWatch {
    interest: Interest,
    /// Where to resume: the caller has every frame up to here.
    resume: Cursor,
    tx: mpsc::Sender<Result<Frame, ApiError>>,
    caught_up: bool,
    /// The generation the caller saw in `StageFocused`; a re-opened stage
    /// watch keeps it.
    stage: Option<FocusGeneration>,
}

impl LiveWatch {
    /// Record `frame` and return what the caller should see, or `None` for a
    /// frame the caller already has the meaning of (a re-opened watch's second
    /// `CaughtUp` or `StageFocused`).
    fn admit(&mut self, frame: Frame, generations: &mut Generations) -> Option<Frame> {
        match frame {
            Frame::StageFocused { generation, target } => {
                if self.stage.is_some() {
                    return None;
                }
                let generation = generations.map(generation);
                self.stage = Some(generation);
                Some(Frame::StageFocused { generation, target })
            }
            Frame::Stage { generation, inner } => {
                let generation = match self.stage {
                    Some(pinned) => pinned,
                    None => generations.map(generation),
                };
                let inner = self.track(*inner)?;
                Some(Frame::Stage {
                    generation,
                    inner: Box::new(inner),
                })
            }
            other => self.track(other),
        }
    }

    fn track(&mut self, frame: Frame) -> Option<Frame> {
        match &frame {
            Frame::Committed(env) => self.resume.advance(&env.stream, env.seq),
            Frame::CaughtUp { cursor } => {
                // `cursor` lists every stream that existed when this watch
                // started, so clear `from_head` before a redial can mistake a
                // stream created while down for one to skip.
                self.resume = pin_caught_up(cursor);
                if std::mem::replace(&mut self.caught_up, true) {
                    return None;
                }
            }
            _ => {}
        }
        Some(frame)
    }
}

fn pin_caught_up(cursor: &Cursor) -> Cursor {
    if !cursor.from_head() {
        return cursor.clone();
    }
    let mut value = serde_json::to_value(cursor).expect("Cursor serializes");
    if let Some(obj) = value.as_object_mut() {
        obj.insert("from_head".into(), serde_json::Value::Bool(false));
    }
    serde_json::from_value(value).expect("Cursor deserializes")
}

/// A watch sent to the daemon whose `WatchOpened` has not arrived.
struct Opening {
    reply: oneshot::Sender<Result<FrameStream, ApiError>>,
    watch: LiveWatch,
    stream: FrameStream,
}

enum ConnEvent {
    Read(io::Result<Option<wire::ServerFrame>>),
    WriteFailed(io::Error),
}

enum Redial {
    Connected(OwnedReadHalf, OwnedWriteHalf),
    Shutdown,
    Fatal(ApiError),
}

/// The task behind a [`RemoteClient`]: owns the socket, redials it, and keeps
/// watches open across redials.
struct Connection {
    dialer: Arc<Dialer>,
    me: Principal,
    req_rx: mpsc::UnboundedReceiver<ClientReq>,
    /// Weak, so this task ends once every client handle and stream is gone.
    unwatch_tx: mpsc::WeakUnboundedSender<ClientReq>,
    connected: watch::Sender<bool>,
    closed: Arc<OnceLock<ApiError>>,
    next_id: u64,
    watches: BTreeMap<u64, LiveWatch>,
    generations: Generations,
}

impl Connection {
    async fn run(mut self, mut reader: OwnedReadHalf, mut writer: OwnedWriteHalf) {
        let mut delay = REDIAL_MIN;
        loop {
            self.set_connected(true);
            let started = tokio::time::Instant::now();
            let Some(cause) = self.serve(reader, writer).await else {
                return;
            };
            if started.elapsed() >= REDIAL_MAX {
                delay = REDIAL_MIN;
            }
            self.set_connected(false);
            match self.redial(&cause, &mut delay).await {
                Redial::Connected(r, w) => (reader, writer) = (r, w),
                Redial::Shutdown => return,
                Redial::Fatal(e) => {
                    let _ = self.closed.set(e.clone());
                    for (_, w) in std::mem::take(&mut self.watches) {
                        let _ = w.tx.send(Err(e.clone())).await;
                    }
                    return;
                }
            }
        }
    }

    /// Notify `watch_connection` receivers only on a real change.
    fn set_connected(&self, up: bool) {
        self.connected
            .send_if_modified(|was| std::mem::replace(was, up) != up);
    }

    fn take_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Serve one socket until it drops (returning why) or every handle is
    /// gone (returning `None`).
    async fn serve(&mut self, reader: OwnedReadHalf, mut writer: OwnedWriteHalf) -> Option<String> {
        // `read_frame` is not cancel-safe: dropping it part-way through a frame
        // loses the bytes it has already taken off the socket, and every later
        // frame is then read from the wrong offset. Read on a task of its own
        // and select on the channel, whose `recv` can be dropped safely.
        let (ev_tx, mut ev_rx) = mpsc::channel(WATCH_BUFFER);
        let read_tx = ev_tx.clone();
        let mut reader = reader;
        let reader_task = tokio::spawn(async move {
            loop {
                let incoming = read_frame::<_, wire::ServerFrame>(&mut reader).await;
                let last = !matches!(incoming, Ok(Some(_)));
                if read_tx.send(ConnEvent::Read(incoming)).await.is_err() || last {
                    break;
                }
            }
        });
        let _reader = AbortOnDrop(reader_task.abort_handle());
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let writer_task = tokio::spawn(async move {
            while let Some(bytes) = out_rx.recv().await {
                if let Err(e) = write_bytes(&mut writer, &bytes).await {
                    let _ = ev_tx.send(ConnEvent::WriteFailed(e)).await;
                    break;
                }
            }
        });
        let _writer = AbortOnDrop(writer_task.abort_handle());

        self.generations.rebase();
        // A session holds at most one stage watch. If the socket died before
        // `WatchEnded` arrived for a superseded stage, drop the older one now
        // so two `Watch` frames cannot race to claim the new session's slot.
        if let Some(&latest_stage) = self
            .watches
            .iter()
            .rev()
            .find_map(|(id, w)| matches!(w.interest, Interest::Stage { .. }).then_some(id))
        {
            self.watches.retain(|&id, w| {
                id == latest_stage || !matches!(w.interest, Interest::Stage { .. })
            });
        }
        for (&id, w) in &self.watches {
            let reopen = wire::ClientFrame::Watch {
                id,
                interest: w.interest.clone(),
                from: w.resume.clone(),
            };
            if let Ok(bytes) = encode(&reopen) {
                let _ = out_tx.send(bytes);
            }
        }

        let mut calls: HashMap<u64, oneshot::Sender<Result<WireReply, ApiError>>> = HashMap::new();
        let mut opening: HashMap<u64, Opening> = HashMap::new();
        let cause = loop {
            tokio::select! {
                req = self.req_rx.recv() => match req {
                    Some(req) => self.submit(req, &out_tx, &mut calls, &mut opening),
                    None => return None,
                },
                ev = ev_rx.recv() => match ev {
                    Some(ConnEvent::Read(Ok(Some(frame)))) => {
                        self.receive(frame, &out_tx, &mut calls, &mut opening).await;
                    }
                    Some(ConnEvent::Read(Ok(None))) | None => {
                        break "daemon closed the connection".to_owned();
                    }
                    Some(ConnEvent::Read(Err(e))) => break format!("reading from daemon: {e}"),
                    Some(ConnEvent::WriteFailed(e)) => break format!("writing to daemon: {e}"),
                },
            }
        };
        for (_, reply) in calls {
            let _ = reply.send(Err(ApiError::Transport(cause.clone())));
        }
        for (_, o) in opening {
            let _ = o.reply.send(Err(ApiError::Transport(cause.clone())));
        }
        Some(cause)
    }

    fn submit(
        &mut self,
        req: ClientReq,
        out_tx: &mpsc::UnboundedSender<Vec<u8>>,
        calls: &mut HashMap<u64, oneshot::Sender<Result<WireReply, ApiError>>>,
        opening: &mut HashMap<u64, Opening>,
    ) {
        match req {
            ClientReq::Call { cmd, reply } => {
                let id = self.take_id();
                match encode(&wire::ClientFrame::Call { id, cmd }) {
                    Ok(bytes) => {
                        calls.insert(id, reply);
                        let _ = out_tx.send(bytes);
                    }
                    Err(e) => {
                        let _ = reply.send(Err(ApiError::Transport(format!("encoding call: {e}"))));
                    }
                }
            }
            ClientReq::Watch {
                interest,
                from,
                reply,
            } => {
                let id = self.take_id();
                let frame = wire::ClientFrame::Watch {
                    id,
                    interest: interest.clone(),
                    from: from.clone(),
                };
                let bytes = match encode(&frame) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        let _ =
                            reply.send(Err(ApiError::Transport(format!("encoding watch: {e}"))));
                        return;
                    }
                };
                // The requester holds a sender until it hears back, so this
                // upgrade only fails if it has already given up.
                let Some(req_tx) = self.unwatch_tx.upgrade() else {
                    return;
                };
                let (tx, rx) = mpsc::channel(WATCH_BUFFER);
                let guard = UnwatchGuard {
                    id,
                    req_tx,
                    armed: true,
                };
                opening.insert(
                    id,
                    Opening {
                        reply,
                        watch: LiveWatch {
                            interest,
                            resume: from,
                            tx,
                            caught_up: false,
                            stage: None,
                        },
                        stream: frame_stream(rx, guard),
                    },
                );
                let _ = out_tx.send(bytes);
            }
            ClientReq::Unwatch { id } => {
                if self.watches.remove(&id).is_some()
                    && let Ok(bytes) = encode(&wire::ClientFrame::Unwatch { id })
                {
                    let _ = out_tx.send(bytes);
                }
            }
        }
    }

    async fn receive(
        &mut self,
        frame: wire::ServerFrame,
        out_tx: &mpsc::UnboundedSender<Vec<u8>>,
        calls: &mut HashMap<u64, oneshot::Sender<Result<WireReply, ApiError>>>,
        opening: &mut HashMap<u64, Opening>,
    ) {
        match frame {
            wire::ServerFrame::Reply { id, result } => {
                if let Some(reply) = calls.remove(&id) {
                    let _ = reply.send(result);
                }
            }
            wire::ServerFrame::WatchOpened { id, result } => {
                if let Some(o) = opening.remove(&id) {
                    match result {
                        Ok(()) => {
                            self.watches.insert(id, o.watch);
                            let _ = o.reply.send(Ok(o.stream));
                        }
                        Err(e) => {
                            let _ = o.reply.send(Err(e));
                        }
                    }
                } else if let Err(e) = result
                    && let Some(w) = self.watches.remove(&id)
                {
                    // A re-opened watch the daemon now refuses.
                    let _ = w.tx.send(Err(e)).await;
                }
            }
            wire::ServerFrame::Frame { id, frame } => {
                let Some(w) = self.watches.get_mut(&id) else {
                    return;
                };
                let Some(frame) = w.admit(frame, &mut self.generations) else {
                    return;
                };
                if w.tx.send(Ok(frame)).await.is_err() {
                    self.watches.remove(&id);
                    if let Ok(bytes) = encode(&wire::ClientFrame::Unwatch { id }) {
                        let _ = out_tx.send(bytes);
                    }
                }
            }
            wire::ServerFrame::WatchEnded { id, error } => {
                if let Some(w) = self.watches.remove(&id)
                    && let Some(e) = error
                {
                    if let Ok(bytes) = encode(&wire::ClientFrame::Unwatch { id }) {
                        let _ = out_tx.send(bytes);
                    }
                    let _ = w.tx.send(Err(e)).await;
                }
            }
            wire::ServerFrame::Welcome { .. } => {}
        }
    }

    /// Redial with backoff until connected as the same principal.
    async fn redial(&mut self, cause: &str, delay: &mut Duration) -> Redial {
        let dialer = self.dialer.clone();
        loop {
            let wait = *delay;
            *delay = (*delay * 2).min(REDIAL_MAX);
            if self
                .while_down(tokio::time::sleep(wait), cause)
                .await
                .is_none()
            {
                return Redial::Shutdown;
            }
            match self.while_down(dialer.dial(), cause).await {
                None => return Redial::Shutdown,
                Some(Ok((me, reader, writer))) if me == self.me => {
                    return Redial::Connected(reader, writer);
                }
                Some(Ok((me, ..))) => {
                    return Redial::Fatal(ApiError::Transport(format!(
                        "daemon now authenticates this client as {me}, not {}",
                        self.me
                    )));
                }
                Some(Err(e @ ApiError::Denied { .. })) => return Redial::Fatal(e),
                Some(Err(_)) => {}
            }
        }
    }

    /// Drive `fut` while the socket is down, refusing calls and new watches
    /// with `cause`. `None` once every handle is gone.
    async fn while_down<F: Future>(&mut self, fut: F, cause: &str) -> Option<F::Output> {
        tokio::pin!(fut);
        let refusal = || ApiError::Transport(format!("reconnecting to daemon ({cause})"));
        loop {
            tokio::select! {
                out = &mut fut => return Some(out),
                req = self.req_rx.recv() => match req? {
                    ClientReq::Call { reply, .. } => {
                        let _ = reply.send(Err(refusal()));
                    }
                    ClientReq::Watch { reply, .. } => {
                        let _ = reply.send(Err(refusal()));
                    }
                    ClientReq::Unwatch { id } => {
                        self.watches.remove(&id);
                    }
                },
            }
        }
    }
}

/// Bind `endpoint`, recording the daemon process's own Unix uid from `geteuid()`.
pub fn bind_unix(path: &std::path::Path) -> io::Result<(UnixListener, u32)> {
    let prepared = rex_wave_fs::validate_unix_socket_target(path)?;
    bind_unix_prepared(path, &prepared)
}

/// Unlink any stale socket, bind inside the held parent directory, and apply
/// mode `0600` using a previously validated [`rex_wave_fs::PreparedUnixSocket`].
pub fn bind_unix_prepared(
    path: &std::path::Path,
    prepared: &rex_wave_fs::PreparedUnixSocket,
) -> io::Result<(UnixListener, u32)> {
    let std_listener = prepared.bind(path)?;
    std_listener.set_nonblocking(true)?;
    let listener = UnixListener::from_std(std_listener)?;
    Ok((listener, prepared.uid()))
}

/// Lifecycle callbacks fired around each authenticated UDS connection.
///
/// `on_disconnect` runs from an RAII drop guard, so it fires on normal EOF,
/// socket error, protocol violation, and server cancellation alike.
pub trait ConnectionHooks: Send + Sync + 'static {
    fn on_connect(&self, _conn_id: u64, _principal: &Principal, _client: &str) {}
    fn on_uds_presence_bind(&self, _conn_id: u64) {}
    fn on_disconnect(&self, _conn_id: u64, _principal: &Principal) {}
}

/// Default connection hook that does nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopHooks;

impl ConnectionHooks for NoopHooks {}

/// Closure-backed [`ConnectionHooks`] adapter.
pub struct FnConnectionHooks<C, D> {
    on_connect: C,
    on_disconnect: D,
}

impl<C, D> FnConnectionHooks<C, D>
where
    C: Fn(u64, &Principal, &str) + Send + Sync + 'static,
    D: Fn(u64, &Principal) + Send + Sync + 'static,
{
    pub fn new(on_connect: C, on_disconnect: D) -> Self {
        Self {
            on_connect,
            on_disconnect,
        }
    }
}

impl<C, D> ConnectionHooks for FnConnectionHooks<C, D>
where
    C: Fn(u64, &Principal, &str) + Send + Sync + 'static,
    D: Fn(u64, &Principal) + Send + Sync + 'static,
{
    fn on_connect(&self, conn_id: u64, principal: &Principal, client: &str) {
        (self.on_connect)(conn_id, principal, client);
    }

    fn on_disconnect(&self, conn_id: u64, principal: &Principal) {
        (self.on_disconnect)(conn_id, principal);
    }
}

/// Fires `hooks.on_disconnect` unconditionally when an authenticated connection ends.
struct DisconnectGuard {
    conn_id: u64,
    principal: Principal,
    hooks: Arc<dyn ConnectionHooks>,
}

impl Drop for DisconnectGuard {
    fn drop(&mut self) {
        self.hooks.on_disconnect(self.conn_id, &self.principal);
    }
}

/// Serve `router` on an already-bound `UnixListener` until cancelled, firing
/// `hooks` on every authenticated connect and RAII disconnect.
pub async fn serve_listener_with_hooks(
    listener: UnixListener,
    router: Arc<Router>,
    auth: Arc<dyn Authenticator>,
    hooks: Arc<dyn ConnectionHooks>,
) -> io::Result<()> {
    if let Ok(addr) = listener.local_addr()
        && let Some(path) = addr.as_pathname()
    {
        daemon_log!("listening on {}", path.display());
    }
    let mut conns = JoinSet::new();
    let mut next_conn = 0u64;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted.inspect_err(|e| daemon_log!("accept failed: {e}"))?;
                next_conn += 1;
                let conn = next_conn;
                let peer_uid = stream.peer_cred().ok().map(|c| c.uid());
                let router = router.clone();
                let auth = auth.clone();
                let hooks = hooks.clone();
                conns.spawn(async move {
                    if let Err(e) = handle_conn(conn, stream, peer_uid, router, auth, hooks).await {
                        daemon_log!("conn #{conn}: {e}");
                    }
                });
            }
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
}

/// Serve `router` on an already-bound `UnixListener` until cancelled.
/// Cancelling closes every connection, as a daemon exit would.
pub async fn serve_listener(
    listener: UnixListener,
    router: Arc<Router>,
    auth: Arc<dyn Authenticator>,
) -> io::Result<()> {
    serve_listener_with_hooks(listener, router, auth, Arc::new(NoopHooks)).await
}

/// Serve `router` on `endpoint` with connection lifecycle hooks until cancelled.
pub async fn serve_with_hooks(
    endpoint: Endpoint,
    router: Arc<Router>,
    auth: Arc<dyn Authenticator>,
    hooks: Arc<dyn ConnectionHooks>,
) -> io::Result<()> {
    match endpoint {
        Endpoint::Unix(path) => {
            let (listener, _) = bind_unix(&path)?;
            serve_listener_with_hooks(listener, router, auth, hooks).await
        }
        Endpoint::WebSocket(_) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "websocket endpoint is not enabled",
        )),
    }
}

/// Serve `router` on `endpoint` until cancelled. One task per connection, one
/// task per watch; replies complete in whatever order they finish.
pub async fn serve(
    endpoint: Endpoint,
    router: Arc<Router>,
    auth: Arc<dyn Authenticator>,
) -> io::Result<()> {
    serve_with_hooks(endpoint, router, auth, Arc::new(NoopHooks)).await
}

/// Errors that point at the daemon or what it fronts, as opposed to a caller
/// asking for something it may not have.
fn worth_logging(e: &ApiError) -> bool {
    matches!(e, ApiError::Internal(_) | ApiError::Transport(_))
}

async fn handle_conn(
    conn: u64,
    stream: UnixStream,
    peer_uid: Option<u32>,
    router: Arc<Router>,
    auth: Arc<dyn Authenticator>,
    hooks: Arc<dyn ConnectionHooks>,
) -> io::Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let first: Option<wire::ClientFrame> =
        tokio::time::timeout(HELLO_TIMEOUT, read_frame(&mut reader))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "hello timed out"))??;
    let Some(wire::ClientFrame::Hello { credential, client }) = first else {
        daemon_log!("conn #{conn}: closed without hello");
        return Ok(());
    };
    let welcome = auth.authenticate(peer_uid, &credential);
    write_server_frame(
        &mut writer,
        conn,
        &wire::ServerFrame::Welcome {
            result: welcome.clone(),
            daemon: env!("CARGO_PKG_VERSION").into(),
        },
    )
    .await?;
    let me = match welcome {
        Ok(me) => me,
        Err(e) => {
            daemon_log!("conn #{conn}: refused {client}: {e}");
            return Ok(());
        }
    };
    daemon_log!("conn #{conn}: {client} connected as {me}");
    hooks.on_connect(conn, &me, &client);
    let disconnect_guard = DisconnectGuard {
        conn_id: conn,
        principal: me.clone(),
        hooks,
    };

    let session = Session::new(me.clone());
    let (out_tx, mut out_rx) = mpsc::channel::<wire::ServerFrame>(WATCH_BUFFER * 2);
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            write_server_frame(&mut writer, conn, &frame).await?;
        }
        Ok::<(), io::Error>(())
    });
    let _writer = AbortOnDrop(writer_task.abort_handle());

    let mut watches: HashMap<u64, AbortOnDrop> = HashMap::new();
    let mut in_flight_calls: JoinSet<()> = JoinSet::new();
    let ended = loop {
        let frame = match read_frame::<_, wire::ClientFrame>(&mut reader).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break "client hung up".to_owned(),
            Err(e) => break format!("read failed: {e}"),
        };
        match frame {
            wire::ClientFrame::Hello { .. } => break "second hello".to_owned(),
            wire::ClientFrame::Call { id, mut cmd } => {
                while in_flight_calls.try_join_next().is_some() {}
                if let WireCommand::PresenceBind(ref mut bind) = cmd
                    && let rex_wave_api::presence::TransportEndpoint::UdsSession {
                        ref mut conn_id,
                        ..
                    } = bind.endpoint
                {
                    *conn_id = conn;
                    disconnect_guard.hooks.on_uds_presence_bind(conn);
                }
                let router = router.clone();
                let by = session.me().clone();
                let out_tx = out_tx.clone();
                let name = cmd.meta().name;
                in_flight_calls.spawn(async move {
                    let result = router.dispatch(&by, cmd).await;
                    if let Err(e) = &result
                        && worth_logging(e)
                    {
                        daemon_log!("conn #{conn}: {name} failed: {e}");
                    }
                    let _ = out_tx.send(wire::ServerFrame::Reply { id, result }).await;
                });
            }
            wire::ClientFrame::Watch { id, interest, from } => {
                let router = router.clone();
                let session = session.clone();
                let out_tx = out_tx.clone();
                let handle = tokio::spawn(async move {
                    let mut stream = match router.watch(&session, interest, from).await {
                        Ok(s) => {
                            if out_tx
                                .send(wire::ServerFrame::WatchOpened { id, result: Ok(()) })
                                .await
                                .is_err()
                            {
                                return;
                            }
                            s
                        }
                        Err(e) => {
                            if worth_logging(&e) {
                                daemon_log!("conn #{conn}: watch {id} refused: {e}");
                            }
                            let _ = out_tx
                                .send(wire::ServerFrame::WatchOpened { id, result: Err(e) })
                                .await;
                            return;
                        }
                    };
                    let mut end_err = None;
                    while let Some(item) = stream.next().await {
                        match item {
                            Ok(frame) => {
                                if out_tx
                                    .send(wire::ServerFrame::Frame { id, frame })
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Err(e) => {
                                if worth_logging(&e) {
                                    daemon_log!("conn #{conn}: watch {id} ended: {e}");
                                }
                                end_err = Some(e);
                                break;
                            }
                        }
                    }
                    let _ = out_tx
                        .send(wire::ServerFrame::WatchEnded { id, error: end_err })
                        .await;
                });
                watches.insert(id, AbortOnDrop(handle.abort_handle()));
            }
            wire::ClientFrame::Unwatch { id } => {
                watches.remove(&id);
            }
        }
    };

    drop(watches);
    // Allow short in-flight commands (such as MailBridge::promote or PresenceBind)
    // to finish cleanly before aborting stuck calls and firing RAII on_disconnect.
    const IN_FLIGHT_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);
    let _ = tokio::time::timeout(IN_FLIGHT_DRAIN_TIMEOUT, async {
        while in_flight_calls.join_next().await.is_some() {}
    })
    .await;
    in_flight_calls.abort_all();
    while in_flight_calls.join_next().await.is_some() {}
    drop(disconnect_guard);
    drop(out_tx);
    match writer_task.await {
        Ok(Err(e)) => daemon_log!("conn #{conn}: {me} disconnected ({ended}; write failed: {e})"),
        _ => daemon_log!("conn #{conn}: {me} disconnected ({ended})"),
    }
    Ok(())
}
