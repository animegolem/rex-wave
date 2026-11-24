//! Canonical mode-`0600` per-worker token storage under `<state_dir>/worker-tokens/<thread_id>`
//! and OS-CSPRNG capability token minting shared by the daemon, CLI, T3 bridge, and native runners.

use rex_wave_api::{HostThreadId, Principal, RoleName, WaveSlug};
use serde::{Deserialize, Serialize};
use std::io;
use std::path::{Path, PathBuf};

pub const WORKER_TOKENS_DIR: &str = "worker-tokens";
pub const WORKER_THREAD_ENV: &str = "REX_WAVE_WORKER_THREAD_ID";
pub const T3_WORKER_THREAD_ENV: &str = "T3CODE_THREAD_ID";

/// The largest worker token file read.
const MAX_WORKER_TOKEN_FILE_BYTES: u64 = 16 * 1024;

/// On-disk JSON payload for one per-worker token file under `<state_dir>/worker-tokens/`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerTokenFile {
    pub token: String,
    pub principal: Principal,
}

/// Validates a single safe filename for `<state_dir>/worker-tokens/<id>`.
pub fn is_safe_worker_token_id(id: &str) -> bool {
    rex_wave_fs::SafeName::parse_plain(id).is_ok()
}

pub fn worker_tokens_dir(state_dir: &Path) -> PathBuf {
    state_dir.join(WORKER_TOKENS_DIR)
}

pub fn worker_token_path(state_dir: &Path, thread_id: &str) -> io::Result<PathBuf> {
    rex_wave_fs::SafeName::parse_plain(thread_id).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid worker thread filename: {e}"),
        )
    })?;
    Ok(worker_tokens_dir(state_dir).join(thread_id))
}

/// Mints a random 256-bit hex capability token prefixed with `prefix` via the OS CSPRNG.
pub fn mint_random_token(prefix: &str) -> io::Result<String> {
    let mut entropy = [0u8; 32];
    getrandom::fill(&mut entropy)
        .map_err(|e| io::Error::other(format!("OS CSPRNG getrandom failed: {e}")))?;
    let mut token = String::with_capacity(prefix.len() + 64);
    token.push_str(prefix);
    for byte in entropy {
        use std::fmt::Write as _;
        let _ = write!(token, "{byte:02x}");
    }
    Ok(token)
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
    if token.is_empty() || token.chars().any(char::is_control) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "worker token must be non-empty without control characters",
        ));
    }
    if !matches!(principal, Principal::Worker { .. }) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "worker token file only allows Principal::Worker",
        ));
    }

    rex_wave_fs::ensure_private_dir(dir)?;

    let payload = WorkerTokenFile {
        token: token.to_owned(),
        principal: principal.clone(),
    };
    let mut encoded = serde_json::to_vec_pretty(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    encoded.push(b'\n');

    let dest = dir.join(id);
    rex_wave_fs::write_atomic_path(&dest, &encoded, 0o600, false)?;
    Ok(dest)
}

/// Atomically writes a structured [`WorkerTokenFile`] JSON record (`{token, principal}`)
/// under `<state_dir>/worker-tokens/<thread_id>`.
pub fn write_worker_token_record(
    state_dir: &Path,
    thread_id: &str,
    token: &str,
    principal: &Principal,
) -> io::Result<PathBuf> {
    write_worker_token_file(&worker_tokens_dir(state_dir), thread_id, token, principal)
}

/// Reads `<state_dir>/worker-tokens/<thread_id>`, refusing symlinks or non-`0600` files.
/// Accepts both structured [`WorkerTokenFile`] JSON (`{token, principal}`) and legacy
/// single-line plain token files.
pub fn read_worker_token_file(state_dir: &Path, thread_id: &str) -> io::Result<Option<String>> {
    let path = worker_token_path(state_dir, thread_id)?;
    let bytes = match rex_wave_fs::read_private_regular(&path, None, MAX_WORKER_TOKEN_FILE_BYTES) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("worker token file must be a regular mode-0600 file: {e}"),
            ));
        }
    };
    let raw =
        String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.starts_with('{') {
        let record: WorkerTokenFile = serde_json::from_str(trimmed)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let tok = record.token.trim();
        if tok.is_empty() || tok.chars().any(char::is_control) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "empty or invalid token in worker token JSON",
            ));
        }
        return Ok(Some(tok.to_owned()));
    }
    Ok(Some(trimmed.to_owned()))
}

/// Unlinks `<state_dir>/worker-tokens/<thread_id>`, ignoring missing files.
pub fn remove_worker_token_file(state_dir: &Path, thread_id: &str) -> io::Result<()> {
    let path = worker_token_path(state_dir, thread_id)?;
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Result of provisioning a worker token file on spawn.
#[derive(Clone, PartialEq, Eq)]
pub struct ProvisionedWorkerToken {
    pub path: PathBuf,
    pub token: String,
    pub created: bool,
}

impl std::fmt::Debug for ProvisionedWorkerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProvisionedWorkerToken")
            .field("path", &self.path)
            .field("token", &"<redacted>")
            .field("created", &self.created)
            .finish()
    }
}

/// Ensures `<state_dir>/worker-tokens/<thread_id>` exists with mode `0600` as a
/// structured [`WorkerTokenFile`] JSON record for `Principal::Worker { wave, role }`,
/// reusing an existing valid `0600` file for that same principal idempotently.
pub fn provision_worker_token_in_state_dir(
    state_dir: &Path,
    wave: &WaveSlug,
    role: &RoleName,
    thread: &HostThreadId,
) -> io::Result<ProvisionedWorkerToken> {
    let principal = Principal::Worker {
        wave: wave.clone(),
        role: role.clone(),
    };
    if let Ok(path) = worker_token_path(state_dir, thread.as_str())
        && let Ok(Some(existing)) = read_worker_token_file(state_dir, thread.as_str())
    {
        let raw = rex_wave_fs::read_private_regular(&path, None, MAX_WORKER_TOKEN_FILE_BYTES)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_default();
        let trimmed = raw.trim();
        if trimmed.starts_with('{') {
            if let Ok(rec) = serde_json::from_str::<WorkerTokenFile>(trimmed)
                && rec.principal == principal
            {
                return Ok(ProvisionedWorkerToken {
                    path,
                    token: existing,
                    created: false,
                });
            }
        } else {
            let upgraded =
                write_worker_token_record(state_dir, thread.as_str(), &existing, &principal)?;
            return Ok(ProvisionedWorkerToken {
                path: upgraded,
                token: existing,
                created: false,
            });
        }
    }

    let token = mint_random_token("rw_w_")?;
    let path = write_worker_token_record(state_dir, thread.as_str(), &token, &principal)?;
    Ok(ProvisionedWorkerToken {
        path,
        token,
        created: true,
    })
}
