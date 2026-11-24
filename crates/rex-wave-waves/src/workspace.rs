//! Host-agnostic project workspace path resolution (`ProjectWorkspaceLocation`,
//! `default_bb_data_dir`, `lookup_bb_project_location`, `normalize_workspace_path_key`)
//! shared by T3, BB, and native ACP/CLI runners.

use crate::HostError;
use rex_wave_api::ProjectId;
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// The largest BB `projects.json` cache read.
const MAX_PROJECT_CACHE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectWorkspaceLocation {
    pub name: Option<String>,
    pub workspace_root: PathBuf,
}

/// Compatibility alias for [`ProjectWorkspaceLocation`].
pub type BbProjectLocation = ProjectWorkspaceLocation;

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

pub fn looks_like_workspace_path(raw: &str) -> bool {
    raw.starts_with('/') || raw.starts_with("./") || raw.starts_with("../") || raw.starts_with("~/")
}

pub fn expand_tilde(raw: &str) -> PathBuf {
    if let Some(rest) = raw.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME").filter(|h| !h.is_empty())
    {
        return PathBuf::from(home).join(rest);
    }
    PathBuf::from(raw)
}

pub fn normalize_workspace_path_key(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if looks_like_workspace_path(trimmed) || Path::new(trimmed).is_absolute() {
        let expanded = expand_tilde(trimmed);
        if let Ok(real) = rex_wave_fs::canonicalize_existing_prefix(&expanded) {
            let s = real.to_string_lossy();
            let stripped = s.trim_end_matches('/');
            return if stripped.is_empty() {
                "/".to_owned()
            } else {
                stripped.to_owned()
            };
        }
    }
    let stripped = trimmed.trim_end_matches('/');
    if stripped.is_empty() && raw.starts_with('/') {
        "/".to_owned()
    } else {
        stripped.to_owned()
    }
}

/// Resolves a BB project ID (`proj_...`) to its local workspace root using
/// `<bb_data_dir>/projects.json` and read-only `<bb_data_dir>/bb.db`.
pub fn lookup_bb_project_location(
    bb_data_dir: &Path,
    project_id: &str,
) -> Option<ProjectWorkspaceLocation> {
    let json_path = bb_data_dir.join("projects.json");
    if let Ok(raw) = rex_wave_fs::read_regular_bounded(&json_path, MAX_PROJECT_CACHE_BYTES)
        && let Ok(val) = serde_json::from_slice::<Value>(&raw)
        && let Some(found) = lookup_bb_project_in_json(&val, project_id)
    {
        return Some(found);
    }

    let db_path = bb_data_dir.join("bb.db");
    if db_path.is_file()
        && let Some(found) = lookup_bb_project_in_sqlite(&db_path, project_id)
    {
        return Some(found);
    }

    None
}

/// Synchronously resolves a [`ProjectId`] (explicit filesystem path or BB `proj_...`
/// ID) to its local `(workspace_root, display_title)` pair.
pub fn resolve_project_workspace_location(
    project: &ProjectId,
    bb_data_dir: Option<&Path>,
) -> Result<(PathBuf, String), HostError> {
    let raw = project.as_str().trim();
    if looks_like_workspace_path(raw) {
        let expanded = expand_tilde(raw);
        let resolved = rex_wave_fs::canonicalize_existing_prefix(&expanded).unwrap_or(expanded);
        let title = resolved
            .file_name()
            .and_then(|n| n.to_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(raw)
            .to_owned();
        return Ok((resolved, title));
    }

    if let Some(dir) = bb_data_dir {
        if let Some(found) = lookup_bb_project_location(dir, raw) {
            let title = found.name.unwrap_or_else(|| {
                found
                    .workspace_root
                    .file_name()
                    .and_then(|n| n.to_str())
                    .filter(|s| !s.is_empty())
                    .unwrap_or(raw)
                    .to_owned()
            });
            return Ok((found.workspace_root, title));
        }
        return Err(HostError::Rejected(format!(
            "cannot resolve project '{raw}': not found in BB data dir"
        )));
    }

    Err(HostError::Rejected(format!(
        "cannot resolve project '{raw}': not a workspace path and no BB data dir configured"
    )))
}

fn lookup_bb_project_in_json(val: &Value, project_id: &str) -> Option<ProjectWorkspaceLocation> {
    let rows = val
        .as_array()
        .or_else(|| val.get("projects").and_then(|v| v.as_array()))?;
    for row in rows {
        if row.get("id").and_then(|v| v.as_str()) != Some(project_id) {
            continue;
        }
        let name = row
            .get("name")
            .or_else(|| row.get("title"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        let direct = row
            .get("workspaceRoot")
            .or_else(|| row.get("workspace_root"))
            .or_else(|| row.get("path"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if let Some(p) = direct {
            return Some(ProjectWorkspaceLocation {
                name,
                workspace_root: PathBuf::from(p),
            });
        }
        if let Some(sources) = row.get("sources").and_then(|v| v.as_array()) {
            let preferred = sources
                .iter()
                .filter(|s| {
                    s.get("type")
                        .and_then(|v| v.as_str())
                        .is_none_or(|t| t == "local_path")
                        && s.get("path")
                            .and_then(|v| v.as_str())
                            .is_some_and(|p| !p.trim().is_empty())
                })
                .max_by_key(|s| {
                    s.get("isDefault")
                        .or_else(|| s.get("is_default"))
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                });
            if let Some(src) = preferred
                && let Some(path) = src.get("path").and_then(|v| v.as_str())
            {
                return Some(ProjectWorkspaceLocation {
                    name,
                    workspace_root: PathBuf::from(path.trim()),
                });
            }
        }
    }
    None
}

fn lookup_bb_project_in_sqlite(
    db_path: &Path,
    project_id: &str,
) -> Option<ProjectWorkspaceLocation> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = Connection::open_with_flags(db_path, flags).ok()?;
    let _ = conn.busy_timeout(std::time::Duration::from_millis(5_000));

    let sql = "SELECT p.name, ps.path \
               FROM projects p \
               INNER JOIN project_sources ps \
                 ON ps.project_id = p.id AND ps.type = 'local_path' AND ps.path IS NOT NULL \
               WHERE p.id = ?1 \
               ORDER BY ps.is_default DESC, ps.created_at ASC \
               LIMIT 1";
    if let Ok(mut stmt) = conn.prepare(sql)
        && let Ok((name, path)) = stmt.query_row([project_id], |row| {
            let name: Option<String> = row.get(0)?;
            let path: String = row.get(1)?;
            Ok((name, path))
        })
        && !path.trim().is_empty()
    {
        return Some(ProjectWorkspaceLocation {
            name: name.filter(|s| !s.trim().is_empty()),
            workspace_root: PathBuf::from(path.trim()),
        });
    }

    let fallback_sql = "SELECT name, workspace_root FROM projects WHERE id = ?1 LIMIT 1";
    if let Ok(mut stmt) = conn.prepare(fallback_sql)
        && let Ok((name, path)) = stmt.query_row([project_id], |row| {
            let name: Option<String> = row.get(0)?;
            let path: Option<String> = row.get(1)?;
            Ok((name, path))
        })
        && let Some(p) = path.filter(|s| !s.trim().is_empty())
    {
        return Some(ProjectWorkspaceLocation {
            name: name.filter(|s| !s.trim().is_empty()),
            workspace_root: PathBuf::from(p.trim()),
        });
    }

    None
}
