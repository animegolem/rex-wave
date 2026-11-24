//! Per-worker mode-`0600` token files at `<state_dir>/worker-tokens/<thread_id>`.
//!
//! T3 injects `T3CODE_THREAD_ID=<thread_id>` into worker process environments;
//! storing the worker token in a mode-`0600` file keyed by thread ID lets bash
//! CLI calls authenticate as `Principal::Worker` without secrets in prompts.

use rex_wave_api::{BbThreadId, Principal, RoleName, WaveSlug};
use rex_wave_waves::HostError;
use serde::{Deserialize, Serialize};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const WORKER_TOKENS_DIR: &str = "worker-tokens";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerTokenRecord {
    token: String,
    principal: Principal,
}

pub type WorkerTokenMinter =
    Arc<dyn Fn(&WaveSlug, &RoleName, &BbThreadId) -> Result<String, HostError> + Send + Sync>;

/// The largest worker token file read.
const MAX_WORKER_TOKEN_FILE_BYTES: u64 = 16 * 1024;

fn validate_thread_filename(thread_id: &str) -> io::Result<()> {
    rex_wave_fs::SafeName::parse_plain(thread_id).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid worker thread filename: {e}"),
        )
    })?;
    Ok(())
}

pub fn worker_tokens_dir(state_dir: &Path) -> PathBuf {
    state_dir.join(WORKER_TOKENS_DIR)
}

pub fn worker_token_path(state_dir: &Path, thread_id: &str) -> io::Result<PathBuf> {
    validate_thread_filename(thread_id)?;
    Ok(worker_tokens_dir(state_dir).join(thread_id))
}

fn mint_random_worker_token() -> io::Result<String> {
    let mut entropy = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut entropy)?;
    let mut token = String::from("rw_w_");
    for byte in entropy {
        use std::fmt::Write as _;
        let _ = write!(token, "{byte:02x}");
    }
    Ok(token)
}

fn write_worker_token_bytes(
    state_dir: &Path,
    thread_id: &str,
    bytes: &[u8],
) -> io::Result<PathBuf> {
    let target = worker_token_path(state_dir, thread_id)?;
    let dir = worker_tokens_dir(state_dir);
    rex_wave_fs::ensure_private_dir(&dir)?;

    rex_wave_fs::write_atomic_path(&target, bytes, 0o600, false)?;
    Ok(target)
}

/// Atomically writes a structured `WorkerTokenFile` JSON record (`{token, principal}`)
/// readable by both `StateDirAuthenticator` and `read_worker_token_file`.
pub fn write_worker_token_record(
    state_dir: &Path,
    thread_id: &str,
    token: &str,
    principal: &Principal,
) -> io::Result<PathBuf> {
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
    let record = WorkerTokenRecord {
        token: token.to_owned(),
        principal: principal.clone(),
    };
    let mut bytes = serde_json::to_vec_pretty(&record)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    bytes.push(b'\n');
    write_worker_token_bytes(state_dir, thread_id, &bytes)
}

/// Reads `<state_dir>/worker-tokens/<thread_id>`, refusing symlinks or non-`0600` files.
/// Accepts both structured `WorkerTokenFile` JSON (`{token, principal}`) and legacy
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
        let record: WorkerTokenRecord = serde_json::from_str(trimmed)
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
/// structured `WorkerTokenFile` JSON record for `Principal::Worker { wave, role }`.
/// Without a custom minter, reuses an existing valid `0600` file for that same
/// principal so idempotent spawn retries do not rotate an active worker's token.
pub fn provision_worker_token(
    state_dir: &Path,
    wave: &WaveSlug,
    role: &RoleName,
    thread: &BbThreadId,
    minter: Option<&WorkerTokenMinter>,
) -> Result<ProvisionedWorkerToken, HostError> {
    let principal = Principal::Worker {
        wave: wave.clone(),
        role: role.clone(),
    };
    if let Some(m) = minter {
        let token = m(wave, role, thread)?;
        let path = write_worker_token_record(state_dir, thread.as_str(), &token, &principal)
            .map_err(|e| HostError::Unreachable(format!("write worker token: {e}")))?;
        return Ok(ProvisionedWorkerToken {
            path,
            token,
            created: true,
        });
    }

    if let Ok(path) = worker_token_path(state_dir, thread.as_str())
        && let Ok(Some(existing)) = read_worker_token_file(state_dir, thread.as_str())
    {
        let raw = rex_wave_fs::read_private_regular(&path, None, MAX_WORKER_TOKEN_FILE_BYTES)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_default();
        let trimmed = raw.trim();
        if trimmed.starts_with('{') {
            if let Ok(rec) = serde_json::from_str::<WorkerTokenRecord>(trimmed)
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
                write_worker_token_record(state_dir, thread.as_str(), &existing, &principal)
                    .map_err(|e| HostError::Unreachable(format!("upgrade worker token: {e}")))?;
            return Ok(ProvisionedWorkerToken {
                path: upgraded,
                token: existing,
                created: false,
            });
        }
    }

    let token = mint_random_worker_token()
        .map_err(|e| HostError::Unreachable(format!("mint worker token: {e}")))?;
    let path = write_worker_token_record(state_dir, thread.as_str(), &token, &principal)
        .map_err(|e| HostError::Unreachable(format!("write worker token: {e}")))?;
    Ok(ProvisionedWorkerToken {
        path,
        token,
        created: true,
    })
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn provisions_reads_reuses_and_unlinks_mode_0600_token() {
        let tmp = tempfile::tempdir().unwrap();
        let wave = WaveSlug::parse("w1").unwrap();
        let role = RoleName::parse("author").unwrap();
        let thread = BbThreadId::parse("t3:w1:author:a1").unwrap();

        let first = provision_worker_token(tmp.path(), &wave, &role, &thread, None).unwrap();
        assert!(first.created);
        assert!(first.token.starts_with("rw_w_"));
        let dbg = format!("{first:?}");
        assert!(!dbg.contains(&first.token));
        assert!(dbg.contains("<redacted>"));

        let meta = std::fs::symlink_metadata(&first.path).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        let dir_meta = std::fs::symlink_metadata(worker_tokens_dir(tmp.path())).unwrap();
        assert_eq!(dir_meta.permissions().mode() & 0o777, 0o700);

        let second = provision_worker_token(tmp.path(), &wave, &role, &thread, None).unwrap();
        assert!(!second.created);
        assert_eq!(second.token, first.token);

        assert_eq!(
            read_worker_token_file(tmp.path(), thread.as_str()).unwrap(),
            Some(first.token.clone())
        );

        let raw_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&first.path).unwrap()).unwrap();
        assert_eq!(raw_json["token"], first.token);
        assert_eq!(
            raw_json["principal"],
            serde_json::json!({ "kind": "worker", "wave": "w1", "role": "author" })
        );

        let other_role = RoleName::parse("reviewer").unwrap();
        let rotated =
            provision_worker_token(tmp.path(), &wave, &other_role, &thread, None).unwrap();
        assert!(rotated.created);
        assert_ne!(
            rotated.token, first.token,
            "different principal on same thread id must mint a fresh token"
        );

        remove_worker_token_file(tmp.path(), thread.as_str()).unwrap();
        remove_worker_token_file(tmp.path(), thread.as_str()).unwrap();
        assert_eq!(
            read_worker_token_file(tmp.path(), thread.as_str()).unwrap(),
            None
        );

        assert!(worker_token_path(tmp.path(), "../escape").is_err());
        assert!(worker_token_path(tmp.path(), ".hidden").is_err());
        assert!(worker_token_path(tmp.path(), "a..b").is_err());
        assert!(worker_token_path(tmp.path(), "a/b").is_err());
        assert!(
            write_worker_token_record(
                tmp.path(),
                "seat-bad",
                "tok_seat",
                &Principal::Seat {
                    seat: rex_wave_api::SeatName::parse("sam").unwrap(),
                },
            )
            .is_err()
        );
    }
}
