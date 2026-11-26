//! Workspace resolution for native runners (`ProjectDefault`, `ExistingPath`, `ManagedWorktree`).

use rex_wave_api::wave::{BaseBranch, Workspace};
use rex_wave_waves::{HostError, SpawnRequest, expand_tilde, resolve_project_workspace_location};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tokio::process::Command;

static WORKTREE_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

const SCRUBBED_GIT_ENV: &[&str] = &[
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_COMMON_DIR",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_PREFIX",
    "GIT_CEILING_DIRECTORIES",
];

fn git_in(dir: &Path) -> Command {
    let mut cmd = Command::new("git");
    for key in SCRUBBED_GIT_ENV {
        cmd.env_remove(key);
    }
    cmd.arg("-C").arg(dir);
    cmd
}

/// Resolves the working directory for `req`, provisioning `<project>/.worktrees/rex-wave-<wave>-<role>`
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
    std::fs::create_dir_all(&worktrees_parent).map_err(|e| {
        HostError::Rejected(format!(
            "create worktrees parent {}: {e}",
            worktrees_parent.display()
        ))
    })?;

    let wt_path = worktrees_parent.join(format!("rex-wave-{}-{}", req.wave, req.role));
    if is_registered_worktree_for(project_root, &wt_path).await {
        let resolved = rex_wave_fs::canonicalize_existing_prefix(&wt_path).unwrap_or(wt_path);
        return Ok(resolved);
    }
    if rex_wave_fs::entry_exists(&wt_path) {
        return Err(HostError::Rejected(format!(
            "managed worktree path {} already exists and is not a registered git worktree of {}",
            wt_path.display(),
            project_root.display()
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

    if out.status.success() {
        let resolved = rex_wave_fs::canonicalize_existing_prefix(&wt_path).unwrap_or(wt_path);
        return Ok(resolved);
    }

    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_owned();
    if stderr.to_ascii_lowercase().contains("already exists") {
        let retry = git_in(project_root)
            .args(["worktree", "add", "--detach"])
            .arg(&wt_path)
            .arg(base_ref)
            .output()
            .await
            .map_err(|e| HostError::Unreachable(format!("run git worktree add --detach: {e}")))?;
        if retry.status.success() {
            let resolved = rex_wave_fs::canonicalize_existing_prefix(&wt_path).unwrap_or(wt_path);
            return Ok(resolved);
        }
        let retry_err = String::from_utf8_lossy(&retry.stderr).trim().to_owned();
        return Err(HostError::Rejected(format!(
            "git worktree add failed: {retry_err}"
        )));
    }

    Err(HostError::Rejected(format!(
        "git worktree add failed: {stderr}"
    )))
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

async fn is_registered_worktree_for(project_root: &Path, wt_path: &Path) -> bool {
    if rex_wave_fs::entry_kind(wt_path).ok() != Some(rex_wave_fs::EntryKind::Dir) {
        return false;
    }
    if rex_wave_fs::entry_kind(&wt_path.join(".git")).ok() != Some(rex_wave_fs::EntryKind::File) {
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
    stdout
        .lines()
        .filter_map(|line| line.strip_prefix("worktree "))
        .any(|registered| {
            let reg_path = Path::new(registered.trim());
            let Ok(canon_reg) = rex_wave_fs::canonicalize_existing_prefix(reg_path) else {
                return false;
            };
            canon_reg != canon_proj && canon_reg == canon_wt
        })
}
