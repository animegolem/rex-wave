//! Workspace resolution and managed git worktree provisioning for ACP workers.

use rex_wave_api::wave::{BaseBranch, Workspace};
use rex_wave_api::{ProjectId, RoleName, WaveSlug};
use rex_wave_waves::{HostError, expand_tilde, resolve_project_workspace_location};
use std::path::{Path, PathBuf};
use tokio::process::Command;

/// Resolves the working directory for a wave worker (`ProjectDefault`,
/// `ExistingPath`, or `ManagedWorktree`). When `workspace` is `ManagedWorktree`
/// and the project root is a git work tree, provisions (or reuses) an isolated
/// worktree at `<project>/.worktrees/rex-wave-<wave>-<role>`.
pub async fn resolve_acp_workspace(
    project: &ProjectId,
    workspace: &Workspace,
    wave: &WaveSlug,
    role: &RoleName,
    bb_data_dir: Option<&Path>,
) -> Result<(PathBuf, String), HostError> {
    let (project_root, title) = resolve_project_workspace_location(project, bb_data_dir)?;
    if !project_root.is_dir() {
        return Err(HostError::Rejected(format!(
            "workspace project root does not exist: {}",
            project_root.display()
        )));
    }

    match workspace {
        Workspace::ProjectDefault => Ok((project_root, title)),
        Workspace::ExistingPath { path } => {
            let raw = path.to_string_lossy();
            let candidate = if path.is_absolute() || raw.starts_with("~/") {
                expand_tilde(&raw)
            } else {
                let rel = rex_wave_fs::normalize_no_parent(path).map_err(|e| {
                    HostError::Rejected(format!("invalid existing workspace path: {e}"))
                })?;
                project_root.join(rel)
            };
            let resolved =
                rex_wave_fs::canonicalize_existing_prefix(&candidate).unwrap_or(candidate);
            if !resolved.is_dir() {
                return Err(HostError::Rejected(format!(
                    "existing workspace path is not a directory: {}",
                    resolved.display()
                )));
            }
            Ok((resolved, title))
        }
        Workspace::ManagedWorktree { base } => {
            if !is_git_work_tree(&project_root).await {
                return Ok((project_root, title));
            }
            let worktrees_dir = project_root.join(".worktrees");
            std::fs::create_dir_all(&worktrees_dir).map_err(|e| {
                HostError::Rejected(format!(
                    "create worktrees directory {}: {e}",
                    worktrees_dir.display()
                ))
            })?;
            let wt_path = worktrees_dir.join(format!("rex-wave-{wave}-{role}"));
            if wt_path.is_dir() && is_git_work_tree(&wt_path).await {
                let resolved =
                    rex_wave_fs::canonicalize_existing_prefix(&wt_path).unwrap_or(wt_path);
                return Ok((resolved, title));
            }
            let base_ref = match base {
                BaseBranch::Default => "HEAD",
                BaseBranch::Named { name } => {
                    let trimmed = name.trim();
                    if trimmed.is_empty()
                        || trimmed.starts_with('-')
                        || trimmed.chars().any(char::is_control)
                    {
                        return Err(HostError::Rejected(format!(
                            "invalid git base branch ref: {name:?}"
                        )));
                    }
                    trimmed
                }
            };
            let out = Command::new("git")
                .arg("-C")
                .arg(&project_root)
                .args(["worktree", "add", "--detach"])
                .arg(&wt_path)
                .arg(base_ref)
                .output()
                .await
                .map_err(|e| {
                    HostError::Rejected(format!("failed running git worktree add: {e}"))
                })?;
            if !out.status.success() {
                let stderr = String::from_utf8_lossy(&out.stderr).trim().to_owned();
                return Err(HostError::Rejected(format!(
                    "git worktree add failed for {} (base {base_ref}): {stderr}",
                    wt_path.display()
                )));
            }
            let resolved = rex_wave_fs::canonicalize_existing_prefix(&wt_path).unwrap_or(wt_path);
            Ok((resolved, title))
        }
    }
}

async fn is_git_work_tree(dir: &Path) -> bool {
    let Ok(out) = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "--is-inside-work-tree"])
        .output()
        .await
    else {
        return false;
    };
    out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "true"
}
