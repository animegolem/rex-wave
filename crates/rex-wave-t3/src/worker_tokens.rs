//! Per-worker mode-`0600` token files at `<state_dir>/worker-tokens/<thread_id>`.
//!
//! T3 injects `T3CODE_THREAD_ID=<thread_id>` into worker process environments;
//! storing the worker token in a mode-`0600` file keyed by thread ID lets bash
//! CLI calls authenticate as `Principal::Worker` without secrets in prompts.

use rex_wave_api::{BbThreadId, RoleName, WaveSlug};
use rex_wave_waves::HostError;
use std::io::{self, Read as _, Write as _};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const WORKER_TOKENS_DIR: &str = "worker-tokens";

pub type WorkerTokenMinter =
    Arc<dyn Fn(&WaveSlug, &RoleName, &BbThreadId) -> Result<String, HostError> + Send + Sync>;

fn validate_thread_filename(thread_id: &str) -> io::Result<()> {
    if thread_id.is_empty()
        || thread_id.contains('/')
        || thread_id.contains('\\')
        || thread_id.contains('\0')
        || thread_id == "."
        || thread_id == ".."
        || thread_id.contains("..")
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid worker thread filename: {thread_id}"),
        ));
    }
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

fn ensure_private_dir(dir: &Path) -> io::Result<()> {
    if !dir.exists() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)?;
    }
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
}

/// Atomically writes `token` to `<state_dir>/worker-tokens/<thread_id>` with mode `0600`.
pub fn write_worker_token_file(
    state_dir: &Path,
    thread_id: &str,
    token: &str,
) -> io::Result<PathBuf> {
    let target = worker_token_path(state_dir, thread_id)?;
    let dir = worker_tokens_dir(state_dir);
    ensure_private_dir(&dir)?;

    let mut suffix = [0u8; 4];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut suffix)?;
    let tmp = dir.join(format!(
        ".{thread_id}.tmp.{}.{:02x}{:02x}{:02x}{:02x}",
        std::process::id(),
        suffix[0],
        suffix[1],
        suffix[2],
        suffix[3],
    ));
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        file.write_all(token.as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    if let Err(e) = std::fs::rename(&tmp, &target) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(target)
}

/// Reads `<state_dir>/worker-tokens/<thread_id>`, refusing symlinks or non-`0600` files.
pub fn read_worker_token_file(state_dir: &Path, thread_id: &str) -> io::Result<Option<String>> {
    let path = worker_token_path(state_dir, thread_id)?;
    let meta = match std::fs::symlink_metadata(&path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if meta.file_type().is_symlink() || !meta.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "worker token path must be a regular file",
        ));
    }
    if meta.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "worker token file must be mode 0600",
        ));
    }
    let raw = std::fs::read_to_string(&path)?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionedWorkerToken {
    pub path: PathBuf,
    pub token: String,
    pub created: bool,
}

/// Ensures `<state_dir>/worker-tokens/<thread_id>` exists with mode `0600`.
/// Without a custom minter, reuses an existing valid `0600` file so idempotent
/// spawn retries do not rotate an active worker's token.
pub fn provision_worker_token(
    state_dir: &Path,
    wave: &WaveSlug,
    role: &RoleName,
    thread: &BbThreadId,
    minter: Option<&WorkerTokenMinter>,
) -> Result<ProvisionedWorkerToken, HostError> {
    if let Some(m) = minter {
        let token = m(wave, role, thread)?;
        let path = write_worker_token_file(state_dir, thread.as_str(), &token)
            .map_err(|e| HostError::Unreachable(format!("write worker token: {e}")))?;
        return Ok(ProvisionedWorkerToken {
            path,
            token,
            created: true,
        });
    }

    if let Ok(Some(existing)) = read_worker_token_file(state_dir, thread.as_str()) {
        let path = worker_token_path(state_dir, thread.as_str())
            .map_err(|e| HostError::Rejected(e.to_string()))?;
        return Ok(ProvisionedWorkerToken {
            path,
            token: existing,
            created: false,
        });
    }

    let token = mint_random_worker_token()
        .map_err(|e| HostError::Unreachable(format!("mint worker token: {e}")))?;
    let path = write_worker_token_file(state_dir, thread.as_str(), &token)
        .map_err(|e| HostError::Unreachable(format!("write worker token: {e}")))?;
    Ok(ProvisionedWorkerToken {
        path,
        token,
        created: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provisions_reads_reuses_and_unlinks_mode_0600_token() {
        let tmp = tempfile::tempdir().unwrap();
        let wave = WaveSlug::parse("w1").unwrap();
        let role = RoleName::parse("author").unwrap();
        let thread = BbThreadId::parse("t3:w1:author:a1").unwrap();

        let first = provision_worker_token(tmp.path(), &wave, &role, &thread, None).unwrap();
        assert!(first.created);
        assert!(first.token.starts_with("rw_w_"));

        let meta = std::fs::symlink_metadata(&first.path).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        let dir_meta = std::fs::symlink_metadata(worker_tokens_dir(tmp.path())).unwrap();
        assert_eq!(dir_meta.permissions().mode() & 0o777, 0o700);

        let second = provision_worker_token(tmp.path(), &wave, &role, &thread, None).unwrap();
        assert!(!second.created);
        assert_eq!(second.token, first.token);

        assert_eq!(
            read_worker_token_file(tmp.path(), thread.as_str()).unwrap(),
            Some(first.token)
        );

        remove_worker_token_file(tmp.path(), thread.as_str()).unwrap();
        remove_worker_token_file(tmp.path(), thread.as_str()).unwrap();
        assert_eq!(
            read_worker_token_file(tmp.path(), thread.as_str()).unwrap(),
            None
        );

        assert!(worker_token_path(tmp.path(), "../escape").is_err());
        assert!(worker_token_path(tmp.path(), "a/b").is_err());
    }
}
