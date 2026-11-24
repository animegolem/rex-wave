//! T3 worker-token adapter delegating mode-`0600` file storage and OS-CSPRNG
//! minting to [`rex_wave_transport::worker_tokens`].

use rex_wave_api::{BbThreadId, Principal, RoleName, WaveSlug};
use rex_wave_waves::HostError;
use std::path::Path;
use std::sync::Arc;

pub use rex_wave_transport::{
    ProvisionedWorkerToken, WORKER_TOKENS_DIR, provision_worker_token_in_state_dir,
    read_worker_token_file, remove_worker_token_file, worker_token_path, worker_tokens_dir,
    write_worker_token_record,
};

pub type WorkerTokenMinter =
    Arc<dyn Fn(&WaveSlug, &RoleName, &BbThreadId) -> Result<String, HostError> + Send + Sync>;

/// Ensures `<state_dir>/worker-tokens/<thread_id>` exists with mode `0600` as a
/// structured `WorkerTokenFile` JSON record for `Principal::Worker { wave, role }`.
pub fn provision_worker_token(
    state_dir: &Path,
    wave: &WaveSlug,
    role: &RoleName,
    thread: &BbThreadId,
    minter: Option<&WorkerTokenMinter>,
) -> Result<ProvisionedWorkerToken, HostError> {
    if let Some(m) = minter {
        let principal = Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        };
        let token = m(wave, role, thread)?;
        let path = write_worker_token_record(state_dir, thread.as_str(), &token, &principal)
            .map_err(|e| HostError::Unreachable(format!("write worker token: {e}")))?;
        return Ok(ProvisionedWorkerToken {
            path,
            token,
            created: true,
        });
    }

    provision_worker_token_in_state_dir(state_dir, wave, role, thread)
        .map_err(|e| HostError::Unreachable(format!("provision worker token: {e}")))
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
