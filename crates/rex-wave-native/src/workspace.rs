//! Workspace resolution for native runners (`ProjectDefault`, `ExistingPath`, `ManagedWorktree`).

use crate::common::scrub_daemon_env;
use rex_wave_api::wave::{BaseBranch, Workspace};
use rex_wave_api::{RoleName, WaveSlug};
use rex_wave_fs::EntryKind;
use rex_wave_waves::{HostError, SpawnRequest, expand_tilde, resolve_project_workspace_location};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tokio::process::Command;

static WORKTREE_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
const OWNER_MARKER_FILE: &str = "rex-wave-owner.json";

#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct WorktreeOwnerMarker {
    wave: String,
    role: String,
}

/// Builds a scrubbed `git` subprocess command rooted at `dir` with repository hooks disabled
/// (`core.hooksPath=/dev/null`) so daemon-owned worktree operations never leak daemon
/// credentials or execute untrusted repository hooks before the worker starts.
fn git_in(dir: &Path) -> Command {
    let mut cmd = Command::new("git");
    scrub_daemon_env(&mut cmd);
    cmd.args(["-c", "core.hooksPath=/dev/null"])
        .arg("-C")
        .arg(dir);
    cmd
}

fn ensure_real_dir(path: &Path, label: &str) -> Result<(), HostError> {
    if rex_wave_fs::entry_exists(path) {
        if rex_wave_fs::entry_kind(path).ok() != Some(EntryKind::Dir) {
            return Err(HostError::Rejected(format!(
                "{label} {} is not a regular non-symlink directory",
                path.display()
            )));
        }
        return Ok(());
    }
    std::fs::create_dir(path)
        .map_err(|e| HostError::Rejected(format!("create {label} {}: {e}", path.display())))
}

/// Resolves the working directory for `req`, provisioning `<project>/.worktrees/rex-wave/<wave>/<role>`
/// when `Workspace::ManagedWorktree` is requested inside a Git repository.
pub async fn resolve_worker_workspace(
    req: &SpawnRequest,
    bb_data_dir: Option<&Path>,
) -> Result<PathBuf, HostError> {
    let (project_root, _) = resolve_project_workspace_location(&req.project, bb_data_dir)?;
    if !project_root.is_dir() {
        return Err(HostError::Rejected(format!(
            "project workspace directory does not exist: {}",
            project_root.display()
        )));
    }

    match &req.agent.workspace {
        Workspace::ProjectDefault => Ok(project_root),
        Workspace::ExistingPath { path } => {
            let raw = path.to_string_lossy();
            let candidate = if path.is_absolute() || raw.starts_with("~/") {
                expand_tilde(&raw)
            } else {
                let rel = rex_wave_fs::normalize_no_parent(path)
                    .map_err(|e| HostError::Rejected(format!("invalid workspace path: {e}")))?;
                project_root.join(rel)
            };
            let resolved =
                rex_wave_fs::canonicalize_existing_prefix(&candidate).unwrap_or(candidate);
            if !resolved.is_dir() {
                return Err(HostError::Rejected(format!(
                    "existing workspace path does not exist: {}",
                    resolved.display()
                )));
            }
            Ok(resolved)
        }
        Workspace::ManagedWorktree { base } => {
            ensure_managed_worktree(&project_root, req, base).await
        }
    }
}

async fn ensure_managed_worktree(
    project_root: &Path,
    req: &SpawnRequest,
    base: &BaseBranch,
) -> Result<PathBuf, HostError> {
    if !is_git_work_tree(project_root).await {
        return Ok(project_root.to_path_buf());
    }

    let _wt_guard = WORKTREE_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let worktrees_parent = project_root.join(".worktrees");
    ensure_real_dir(&worktrees_parent, "worktrees directory")?;
    let rex_root = worktrees_parent.join("rex-wave");
    ensure_real_dir(&rex_root, "rex-wave worktrees directory")?;
    let wave_dir = rex_root.join(req.wave.as_str());
    ensure_real_dir(&wave_dir, "wave worktrees directory")?;

    let wt_path = wave_dir.join(req.role.as_str());
    if verify_or_heal_worktree_for(project_root, &wt_path, &req.wave, &req.role).await {
        let resolved = rex_wave_fs::canonicalize_existing_prefix(&wt_path).unwrap_or(wt_path);
        return Ok(resolved);
    }
    if rex_wave_fs::entry_exists(&wt_path) {
        return Err(HostError::Rejected(format!(
            "managed worktree path {} already exists and is not a registered git worktree owned by @{}/{}",
            wt_path.display(),
            req.wave,
            req.role
        )));
    }

    let base_ref = match base {
        BaseBranch::Default => "HEAD",
        BaseBranch::Named { name } => {
            let trimmed = name.trim();
            if trimmed.is_empty()
                || trimmed.starts_with('-')
                || trimmed.chars().any(|c| c.is_control() || c.is_whitespace())
            {
                return Err(HostError::Rejected(format!(
                    "invalid managed worktree base branch '{name}'"
                )));
            }
            trimmed
        }
    };

    let branch_name = format!("rex-wave/{}/{}", req.wave, req.role);
    let out = git_in(project_root)
        .args(["worktree", "add", "-b", &branch_name])
        .arg(&wt_path)
        .arg(base_ref)
        .output()
        .await
        .map_err(|e| HostError::Unreachable(format!("run git worktree add: {e}")))?;

    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_owned();
        if stderr.to_ascii_lowercase().contains("already exists") {
            let retry = git_in(project_root)
                .args(["worktree", "add", "--detach"])
                .arg(&wt_path)
                .arg(base_ref)
                .output()
                .await
                .map_err(|e| {
                    HostError::Unreachable(format!("run git worktree add --detach: {e}"))
                })?;
            if !retry.status.success() {
                let retry_err = String::from_utf8_lossy(&retry.stderr).trim().to_owned();
                return Err(HostError::Rejected(format!(
                    "git worktree add failed: {retry_err}"
                )));
            }
        } else {
            return Err(HostError::Rejected(format!(
                "git worktree add failed: {stderr}"
            )));
        }
    }

    write_owner_marker(&wt_path, &req.wave, &req.role).await?;
    let resolved = rex_wave_fs::canonicalize_existing_prefix(&wt_path).unwrap_or(wt_path);
    Ok(resolved)
}

async fn write_owner_marker(
    wt_path: &Path,
    wave: &WaveSlug,
    role: &RoleName,
) -> Result<(), HostError> {
    let Some(wt_git_dir) = git_rev_parse_path(wt_path, "--git-dir").await else {
        return Err(HostError::Rejected(format!(
            "failed resolving worktree git-dir for {}",
            wt_path.display()
        )));
    };
    let marker = WorktreeOwnerMarker {
        wave: wave.as_str().to_owned(),
        role: role.as_str().to_owned(),
    };
    let bytes = serde_json::to_vec(&marker)
        .map_err(|e| HostError::Rejected(format!("encode marker: {e}")))?;
    rex_wave_fs::write_atomic_path(&wt_git_dir.join(OWNER_MARKER_FILE), &bytes, 0o600, false)
        .map_err(|e| HostError::Rejected(format!("write worktree owner marker: {e}")))
}

async fn is_git_work_tree(dir: &Path) -> bool {
    let Ok(out) = git_in(dir)
        .args(["rev-parse", "--is-inside-work-tree"])
        .output()
        .await
    else {
        return false;
    };
    out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "true"
}

async fn git_rev_parse_path(dir: &Path, flag: &str) -> Option<PathBuf> {
    let out = git_in(dir).args(["rev-parse", flag]).output().await.ok()?;
    if !out.status.success() {
        return None;
    }
    let raw = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    if raw.is_empty() {
        return None;
    }
    let p = PathBuf::from(&raw);
    let joined = if p.is_absolute() { p } else { dir.join(p) };
    Some(rex_wave_fs::canonicalize_existing_prefix(&joined).unwrap_or(joined))
}

async fn verify_or_heal_worktree_for(
    project_root: &Path,
    wt_path: &Path,
    wave: &WaveSlug,
    role: &RoleName,
) -> bool {
    if rex_wave_fs::entry_kind(wt_path).ok() != Some(EntryKind::Dir) {
        return false;
    }
    if rex_wave_fs::entry_kind(&wt_path.join(".git")).ok() != Some(EntryKind::File) {
        return false;
    }
    let Ok(canon_proj) = rex_wave_fs::canonicalize_existing_prefix(project_root) else {
        return false;
    };
    let Ok(canon_wt) = rex_wave_fs::canonicalize_existing_prefix(wt_path) else {
        return false;
    };
    if canon_wt == canon_proj {
        return false;
    }
    let Some(wt_top) = git_rev_parse_path(wt_path, "--show-toplevel").await else {
        return false;
    };
    if wt_top != canon_wt {
        return false;
    }
    let Some(proj_common) = git_rev_parse_path(project_root, "--git-common-dir").await else {
        return false;
    };
    let Some(wt_common) = git_rev_parse_path(wt_path, "--git-common-dir").await else {
        return false;
    };
    if proj_common != wt_common {
        return false;
    }
    let Some(wt_git_dir) = git_rev_parse_path(wt_path, "--git-dir").await else {
        return false;
    };
    let Ok(out) = git_in(project_root)
        .args(["worktree", "list", "--porcelain"])
        .output()
        .await
    else {
        return false;
    };
    if !out.status.success() {
        return false;
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let registered_in_repo = stdout
        .lines()
        .filter_map(|line| line.strip_prefix("worktree "))
        .any(|registered| {
            let reg_path = Path::new(registered.trim());
            let Ok(canon_reg) = rex_wave_fs::canonicalize_existing_prefix(reg_path) else {
                return false;
            };
            canon_reg != canon_proj && canon_reg == canon_wt
        });
    if !registered_in_repo {
        return false;
    }

    let marker_path = wt_git_dir.join(OWNER_MARKER_FILE);
    if rex_wave_fs::entry_exists(&marker_path) {
        let Ok(marker_bytes) = rex_wave_fs::read_regular_bounded(&marker_path, 4096) else {
            return false;
        };
        let Ok(marker) = serde_json::from_slice::<WorktreeOwnerMarker>(&marker_bytes) else {
            return false;
        };
        return marker.wave == wave.as_str() && marker.role == role.as_str();
    }

    // Recover from an interrupted prior attempt that finished `git worktree add` at our
    // verified injective `<project>/.worktrees/rex-wave/<wave>/<role>` path before writing
    // `rex-wave-owner.json`.
    let expected_wt = canon_proj
        .join(".worktrees")
        .join("rex-wave")
        .join(wave.as_str())
        .join(role.as_str());
    if canon_wt != expected_wt {
        return false;
    }
    write_owner_marker(wt_path, wave, role).await.is_ok()
}
