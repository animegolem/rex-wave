//! Workspace resolution for native runners (`ProjectDefault`, `ExistingPath`, `ManagedWorktree`).

use rex_wave_api::wave::{BaseBranch, Workspace};
use rex_wave_waves::{HostError, SpawnRequest, expand_tilde, resolve_project_workspace_location};
use std::path::{Path, PathBuf};
use tokio::process::Command;

/// Resolves the working directory for `req`, provisioning `<project>/.worktrees/rex-wave-<wave>-<role>`
/// when `Workspace::ManagedWorktree` is requested.
pub async fn resolve_worker_workspace(
    req: &SpawnRequest,
    bb_data_dir: Option<&Path>,
) -> Result<PathBuf, HostError> {
    let (project_root, _) = resolve_project_workspace_location(&req.project, bb_data_dir)?;
    match &req.agent.workspace {
        Workspace::ProjectDefault => {
            if !project_root.is_dir() {
                return Err(HostError::Rejected(format!(
                    "project workspace directory does not exist: {}",
                    project_root.display()
                )));
            }
            Ok(project_root)
        }
        Workspace::ExistingPath { path } => {
            let expanded = expand_tilde(&path.to_string_lossy());
            let resolved = if expanded.is_absolute() {
                expanded
            } else {
                let rel = rex_wave_fs::normalize_no_parent(&expanded)
                    .map_err(|e| HostError::Rejected(format!("invalid workspace path: {e}")))?;
                project_root.join(rel)
            };
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
    if !project_root.is_dir() {
        return Err(HostError::Rejected(format!(
            "project root does not exist for worktree creation: {}",
            project_root.display()
        )));
    }
    let worktrees_parent = project_root.join(".worktrees");
    let wt_path = worktrees_parent.join(format!("rex-wave-{}-{}", req.wave, req.role));
    if wt_path.is_dir() && wt_path.join(".git").exists() {
        return Ok(wt_path);
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

    std::fs::create_dir_all(&worktrees_parent).map_err(|e| {
        HostError::Rejected(format!(
            "create worktrees parent {}: {e}",
            worktrees_parent.display()
        ))
    })?;

    let branch_name = format!("rex-wave/{}/{}", req.wave, req.role);
    let out = Command::new("git")
        .arg("-C")
        .arg(project_root)
        .args(["worktree", "add", "-b", &branch_name])
        .arg(&wt_path)
        .arg(base_ref)
        .output()
        .await
        .map_err(|e| HostError::Unreachable(format!("run git worktree add: {e}")))?;

    if out.status.success() {
        return Ok(wt_path);
    }

    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_owned();
    if stderr.to_ascii_lowercase().contains("already exists") {
        let retry = Command::new("git")
            .arg("-C")
            .arg(project_root)
            .args(["worktree", "add", "--detach"])
            .arg(&wt_path)
            .arg(base_ref)
            .output()
            .await
            .map_err(|e| HostError::Unreachable(format!("run git worktree add --detach: {e}")))?;
        if retry.status.success() {
            return Ok(wt_path);
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
