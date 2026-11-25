//! Cross-host project resolution (T3 UUID, filesystem path, BB `proj_...`) and
//! read-only SQLite metadata helpers.

use crate::rpc::T3RpcClient;
use rex_wave_api::BbProjectId;
use rex_wave_waves::{
    HostError, expand_tilde, looks_like_workspace_path, normalize_workspace_path_key,
};
use rusqlite::{Connection, OpenFlags};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

pub use rex_wave_waves::{BbProjectLocation, ProjectWorkspaceLocation, lookup_bb_project_location};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct T3ProjectInfo {
    pub id: String,
    pub title: String,
    pub workspace_root: String,
}

pub fn parse_t3_projects_snapshot(val: &Value) -> Vec<T3ProjectInfo> {
    let arr = val
        .get("projects")
        .and_then(|v| v.as_array())
        .or_else(|| val.as_array());
    let Some(rows) = arr else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for row in rows {
        let Some(id) = row
            .get("id")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        let title = row
            .get("title")
            .or_else(|| row.get("name"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(id);
        let workspace_root = row
            .get("workspaceRoot")
            .or_else(|| row.get("workspace_root"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .unwrap_or("");
        out.push(T3ProjectInfo {
            id: id.to_owned(),
            title: title.to_owned(),
            workspace_root: workspace_root.to_owned(),
        });
    }
    out
}

pub fn project_names_map(projects: &[T3ProjectInfo]) -> BTreeMap<BbProjectId, String> {
    let mut out = BTreeMap::new();
    for p in projects {
        if let Ok(id) = BbProjectId::parse(&p.id) {
            out.insert(id, p.title.clone());
        }
    }
    out
}

/// Read-only fallback seeding T3 project names from `<t3_home>/userdata/statev2.sqlite`.
pub fn load_t3_project_names_from_sqlite(db_path: &Path) -> BTreeMap<BbProjectId, String> {
    let mut map = BTreeMap::new();
    if !db_path.is_file() {
        return map;
    }
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let Ok(conn) = Connection::open_with_flags(db_path, flags) else {
        return map;
    };
    // Pin a 5 s busy timeout explicitly so concurrent T3 WAL writes wait rather
    // than returning SQLITE_BUSY even if rusqlite's default changes.
    if conn
        .busy_timeout(std::time::Duration::from_millis(5_000))
        .is_err()
    {
        return map;
    }
    let Ok(mut stmt) =
        conn.prepare("SELECT project_id, title FROM projection_projects WHERE deleted_at IS NULL")
    else {
        return map;
    };
    let Ok(rows) = stmt.query_map([], |row| {
        let id: String = row.get(0)?;
        let title: String = row.get(1)?;
        Ok((id, title))
    }) else {
        return map;
    };
    for row in rows.flatten() {
        let title = row.1.trim();
        if !title.is_empty()
            && let Ok(pid) = BbProjectId::parse(&row.0)
        {
            map.insert(pid, title.to_owned());
        }
    }
    map
}

fn normalize_path_key(raw: &str) -> String {
    normalize_workspace_path_key(raw)
}

fn deterministic_project_uuid(workspace_root: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"rex-wave-t3-project:");
    hasher.update(workspace_root.as_bytes());
    let digest = hasher.finalize();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-4{:01x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        digest[0],
        digest[1],
        digest[2],
        digest[3],
        digest[4],
        digest[5],
        digest[6] & 0x0f,
        digest[7],
        (digest[8] & 0x3f) | 0x80,
        digest[9],
        digest[10],
        digest[11],
        digest[12],
        digest[13],
        digest[14],
        digest[15],
    )
}

fn looks_like_path(raw: &str) -> bool {
    looks_like_workspace_path(raw)
}

#[derive(Clone, Debug)]
pub struct ResolvedT3Project {
    pub t3_project_id: String,
    pub title: String,
    pub all_projects: Vec<T3ProjectInfo>,
}

/// Resolves `WaveSpec.project` (T3 UUID, filesystem path, or BB `proj_...`) into a
/// live T3 project ID, creating the T3 project via `POST /api/projects/mutate` when needed.
pub async fn resolve_or_ensure_t3_project(
    rpc: &T3RpcClient,
    project: &BbProjectId,
    bb_data_dir: Option<&Path>,
) -> Result<ResolvedT3Project, HostError> {
    let raw = project.as_str().trim();
    let snap = rpc.http_get_json("/api/projects").await?;
    let mut projects = parse_t3_projects_snapshot(&snap);

    if let Some(found) = projects.iter().find(|p| p.id == raw) {
        return Ok(ResolvedT3Project {
            t3_project_id: found.id.clone(),
            title: found.title.clone(),
            all_projects: projects,
        });
    }

    let (target_root, desired_title) = if looks_like_path(raw) {
        let expanded = expand_tilde(raw);
        let resolved = rex_wave_fs::canonicalize_existing_prefix(&expanded).unwrap_or(expanded);
        let root_str = resolved.to_string_lossy().into_owned();
        let title = resolved
            .file_name()
            .and_then(|n| n.to_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(raw)
            .to_owned();
        (root_str, title)
    } else if let Some(dir) = bb_data_dir {
        let dir_owned = dir.to_path_buf();
        let pid_owned = raw.to_owned();
        let loc =
            tokio::task::spawn_blocking(move || lookup_bb_project_location(&dir_owned, &pid_owned))
                .await
                .map_err(|e| {
                    HostError::Unreachable(format!("lookup BB project task failed: {e}"))
                })?;

        if let Some(found) = loc {
            let root_str = found.workspace_root.to_string_lossy().into_owned();
            let title = found.name.unwrap_or_else(|| {
                found
                    .workspace_root
                    .file_name()
                    .and_then(|n| n.to_str())
                    .filter(|s| !s.is_empty())
                    .unwrap_or(raw)
                    .to_owned()
            });
            (root_str, title)
        } else {
            return Err(HostError::Rejected(format!(
                "cannot resolve project '{raw}': not found in T3 /api/projects or BB data dir"
            )));
        }
    } else {
        return Err(HostError::Rejected(format!(
            "cannot resolve project '{raw}': not found in T3 /api/projects and no BB data dir configured"
        )));
    };

    let target_norm = normalize_path_key(&target_root);
    if let Some(found) = projects.iter().find(|p| {
        !p.workspace_root.is_empty() && normalize_path_key(&p.workspace_root) == target_norm
    }) {
        return Ok(ResolvedT3Project {
            t3_project_id: found.id.clone(),
            title: found.title.clone(),
            all_projects: projects,
        });
    }

    let new_id = deterministic_project_uuid(&target_norm);
    let command_id = format!("rex-wave:project-create:{new_id}");
    let mutation = json!({
        "type": "project.create",
        "commandId": command_id,
        "projectId": new_id,
        "title": desired_title,
        "workspaceRoot": target_root,
    });
    let created = rpc
        .http_post_json("/api/projects/mutate", &mutation)
        .await?;
    let created_id = created
        .get("id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(&new_id)
        .to_owned();
    let created_title = created
        .get("title")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(&desired_title)
        .to_owned();
    let created_root = created
        .get("workspaceRoot")
        .and_then(|v| v.as_str())
        .unwrap_or(&target_root)
        .to_owned();

    projects.push(T3ProjectInfo {
        id: created_id.clone(),
        title: created_title.clone(),
        workspace_root: created_root,
    });

    Ok(ResolvedT3Project {
        t3_project_id: created_id,
        title: created_title,
        all_projects: projects,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn loads_t3_sqlite_and_bb_project_sources() {
        let tmp = tempfile::tempdir().unwrap();

        let t3_db = tmp.path().join("statev2.sqlite");
        let conn = Connection::open(&t3_db).unwrap();
        conn.execute_batch(
            "CREATE TABLE projection_projects (
                project_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                workspace_root TEXT NOT NULL,
                deleted_at TEXT
            );
            INSERT INTO projection_projects VALUES ('uuid-1', 'rex-wave', '/tmp/rex-wave', NULL);
            INSERT INTO projection_projects VALUES ('uuid-del', 'gone', '/tmp/gone', '2026-01-01T00:00:00Z');",
        )
        .unwrap();
        let names = load_t3_project_names_from_sqlite(&t3_db);
        assert_eq!(names.len(), 1);
        assert_eq!(
            names
                .get(&BbProjectId::parse("uuid-1").unwrap())
                .map(String::as_str),
            Some("rex-wave")
        );

        let bb_dir = tmp.path().join("bb");
        std::fs::create_dir_all(&bb_dir).unwrap();
        let bb_db = bb_dir.join("bb.db");
        let bconn = Connection::open(&bb_db).unwrap();
        bconn
            .execute_batch(
                "CREATE TABLE projects (id TEXT PRIMARY KEY, name TEXT NOT NULL);
                 CREATE TABLE project_sources (
                     id TEXT PRIMARY KEY,
                     project_id TEXT NOT NULL,
                     type TEXT NOT NULL,
                     path TEXT,
                     is_default INTEGER DEFAULT 0 NOT NULL,
                     created_at INTEGER NOT NULL
                 );
                 INSERT INTO projects VALUES ('proj_alpha', 'alpha-repo');
                 INSERT INTO project_sources VALUES ('src_1', 'proj_alpha', 'local_path', '/tmp/alpha-secondary', 0, 1);
                 INSERT INTO project_sources VALUES ('src_2', 'proj_alpha', 'local_path', '/tmp/alpha-main', 1, 2);",
            )
            .unwrap();
        let loc = lookup_bb_project_location(&bb_dir, "proj_alpha").unwrap();
        assert_eq!(loc.name.as_deref(), Some("alpha-repo"));
        assert_eq!(loc.workspace_root, PathBuf::from("/tmp/alpha-main"));

        let repo_dir = tmp.path().join("SampleRepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        let abs_key = normalize_path_key(&repo_dir.to_string_lossy());
        let cwd = std::env::current_dir().unwrap();
        if let Ok(rel_from_cwd) = strip_or_rel(&cwd, &repo_dir) {
            assert_eq!(normalize_path_key(&rel_from_cwd), abs_key);
        }
        if rex_wave_fs::is_case_insensitive_dir(tmp.path()) {
            let upper = tmp.path().join("SAMPLEREPO");
            assert_eq!(normalize_path_key(&upper.to_string_lossy()), abs_key);
        }

        let real_parent = tmp.path().join("real-parent");
        let real_sub = real_parent.join("sub");
        let target_repo = real_parent.join("target-repo");
        let other_dir = tmp.path().join("other-dir");
        std::fs::create_dir_all(&real_sub).unwrap();
        std::fs::create_dir_all(&target_repo).unwrap();
        std::fs::create_dir_all(other_dir.join("target-repo")).unwrap();
        let link = other_dir.join("link");
        std::os::unix::fs::symlink(&real_sub, &link).unwrap();
        let via_link = format!("{}/../target-repo", link.to_string_lossy());
        assert_eq!(
            normalize_path_key(&via_link),
            normalize_path_key(&target_repo.to_string_lossy()),
            "symlink preceding .. must resolve link target before parent traversal"
        );
    }

    fn strip_or_rel(cwd: &Path, target: &Path) -> Result<String, ()> {
        let mut ups = Vec::new();
        let mut anc = cwd;
        while !target.starts_with(anc) {
            ups.push("..");
            anc = anc.parent().ok_or(())?;
        }
        let tail = target.strip_prefix(anc).map_err(|_| ())?;
        let mut out = if ups.is_empty() {
            PathBuf::from(".")
        } else {
            ups.into_iter().collect::<PathBuf>()
        };
        if !tail.as_os_str().is_empty() {
            out.push(tail);
        }
        Ok(out.to_string_lossy().into_owned())
    }
}
