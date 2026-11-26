//! `~/.config/rex-wave/config.toml` comment-preserving `toml_edit` persistence,
//! `protected-roots` validation, startup crash/hand-edit reconciliation, and
//! live runner + `couriers.d/` health reporting.

use rex_wave_api::config::{
    ConfigSnapshot, ConfigSyncFile, CourierHealthStatus, HarnessProvider, RexConfig,
    RunnerHealthStatus, RunnerRoute,
};
use rex_wave_api::presence::{PresenceList, TransportKind};
use rex_wave_api::*;
use rex_wave_fs::{EntryKind, ProtectedRootsGuard, Protection, load_protected_roots};
use rex_wave_runtime::{BoxFut, ConfigHook, ConfigService, DecideCtx, LocalClient, Service};
use rex_wave_store::Journal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use std::io;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use toml_edit::{DocumentMut, Item, Table};

pub const CONFIG_FILE_MAX_BYTES: u64 = 256 * 1024;
const CONFIG_SYNC_STATE_FILE: &str = "config-sync.json";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SyncMarker {
    seq: u64,
    #[serde(default)]
    sha256: Option<String>,
}

/// Resolves the operator `config.toml` path:
/// 1. explicit `--config <path>` when provided,
/// 2. `$XDG_CONFIG_HOME/rex-wave/config.toml` when absolute and non-empty,
/// 3. `$HOME/.config/rex-wave/config.toml` when absolute and non-empty.
pub fn config_file_path_from(
    explicit: Option<&Path>,
    xdg: Option<&OsStr>,
    home: Option<&OsStr>,
) -> Option<PathBuf> {
    if let Some(p) = explicit {
        if p.as_os_str().is_empty() {
            return None;
        }
        return Some(p.to_path_buf());
    }
    if let Some(x) = xdg.filter(|v| !v.is_empty()) {
        let p = Path::new(x);
        if p.is_absolute() {
            return Some(p.join("rex-wave/config.toml"));
        }
    }
    if let Some(h) = home.filter(|v| !v.is_empty()) {
        let p = Path::new(h);
        if p.is_absolute() {
            return Some(p.join(".config/rex-wave/config.toml"));
        }
    }
    None
}

pub fn daemon_config_file_path_from(
    explicit: Option<&Path>,
    has_socket_override: bool,
    state_dir: &Path,
    xdg: Option<&OsStr>,
    home: Option<&OsStr>,
) -> Option<PathBuf> {
    if let Some(p) = explicit {
        if p.as_os_str().is_empty() {
            return None;
        }
        return Some(p.to_path_buf());
    }
    if has_socket_override {
        return Some(state_dir.join("config.toml"));
    }
    if let Some(x) = xdg.filter(|v| !v.is_empty()) {
        let p = Path::new(x);
        if p.is_absolute() {
            return Some(p.join("rex-wave/config.toml"));
        }
    }
    if let Some(h) = home.filter(|v| !v.is_empty()) {
        let p = Path::new(h);
        if p.is_absolute() {
            return Some(p.join(".config/rex-wave/config.toml"));
        }
    }
    None
}

pub fn daemon_config_file_path(
    explicit: Option<&Path>,
    has_socket_override: bool,
    state_dir: &Path,
) -> Option<PathBuf> {
    let xdg = std::env::var_os("XDG_CONFIG_HOME");
    let home = std::env::var_os("HOME");
    daemon_config_file_path_from(
        explicit,
        has_socket_override,
        state_dir,
        xdg.as_deref(),
        home.as_deref(),
    )
}

pub fn default_config_file_path(explicit: Option<&Path>) -> Option<PathBuf> {
    let xdg = std::env::var_os("XDG_CONFIG_HOME");
    let home = std::env::var_os("HOME");
    config_file_path_from(explicit, xdg.as_deref(), home.as_deref())
}

pub fn effective_couriers_dir(cfg: &RexConfig, config_path: Option<&Path>) -> Option<PathBuf> {
    if let Some(ref d) = cfg.mail.couriers_dir {
        return Some(rex_wave_waves::expand_tilde(&d.to_string_lossy()));
    }
    config_path
        .and_then(Path::parent)
        .map(|p| p.join("couriers.d"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let digest = h.finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictRunnersToml {
    #[serde(default, alias = "default")]
    default_runner: Option<RunnerRoute>,
    #[serde(default, alias = "claude-code", alias = "claudeAgent")]
    claude: Option<RunnerRoute>,
    #[serde(default)]
    codex: Option<RunnerRoute>,
    #[serde(default, alias = "antigravity")]
    agy: Option<RunnerRoute>,
    #[serde(default, alias = "open-code")]
    opencode: Option<RunnerRoute>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictHarnessProfileToml {
    #[serde(default)]
    binary: Option<String>,
    #[serde(default)]
    model: Option<ModelId>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    account: Option<String>,
    #[serde(default)]
    permission: Option<rex_wave_api::wave::PermissionMode>,
    #[serde(default)]
    workspace: Option<rex_wave_api::config::WorkspaceStrategy>,
}

impl From<StrictHarnessProfileToml> for rex_wave_api::config::HarnessProfile {
    fn from(s: StrictHarnessProfileToml) -> Self {
        Self {
            binary: s.binary,
            model: s.model,
            reasoning: s.reasoning,
            account: s.account,
            permission: s.permission,
            workspace: s.workspace,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictHarnessesToml {
    #[serde(default, alias = "claude-code", alias = "claudeAgent")]
    claude: Option<StrictHarnessProfileToml>,
    #[serde(default)]
    codex: Option<StrictHarnessProfileToml>,
    #[serde(default, alias = "antigravity")]
    agy: Option<StrictHarnessProfileToml>,
    #[serde(default, alias = "open-code")]
    opencode: Option<StrictHarnessProfileToml>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictMailToml {
    #[serde(default)]
    mirror_dir: Option<PathBuf>,
    #[serde(default)]
    drop_dir: Option<PathBuf>,
    #[serde(default)]
    export_enabled: Option<bool>,
    #[serde(default)]
    couriers_dir: Option<PathBuf>,
    #[serde(default)]
    hub_owner_seat: Option<SeatName>,
    #[serde(default)]
    reconnect_grace_ms: Option<u64>,
    #[serde(default)]
    toast_cmd: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictRexConfigToml {
    #[serde(default)]
    runners: Option<StrictRunnersToml>,
    #[serde(default)]
    harnesses: Option<StrictHarnessesToml>,
    #[serde(default)]
    mail: Option<StrictMailToml>,
}

pub fn parse_config_toml(raw: &str) -> Result<RexConfig, String> {
    let parsed: StrictRexConfigToml =
        toml_edit::de::from_str(raw).map_err(|e| format!("invalid TOML: {e}"))?;
    let mut cfg = RexConfig::default();
    if let Some(r) = parsed.runners {
        cfg.runners.default_runner = r.default_runner;
        cfg.runners.claude = r.claude;
        cfg.runners.codex = r.codex;
        cfg.runners.agy = r.agy;
        cfg.runners.opencode = r.opencode;
    }
    if let Some(h) = parsed.harnesses {
        if let Some(p) = h.claude {
            cfg.harnesses.claude = p.into();
        }
        if let Some(p) = h.codex {
            cfg.harnesses.codex = p.into();
        }
        if let Some(p) = h.agy {
            cfg.harnesses.agy = p.into();
        }
        if let Some(p) = h.opencode {
            cfg.harnesses.opencode = p.into();
        }
    }
    if let Some(m) = parsed.mail {
        cfg.mail.mirror_dir = m.mirror_dir;
        cfg.mail.drop_dir = m.drop_dir;
        if let Some(b) = m.export_enabled {
            cfg.mail.export_enabled = b;
        }
        cfg.mail.couriers_dir = m.couriers_dir;
        if let Some(s) = m.hub_owner_seat {
            cfg.mail.hub_owner_seat = s;
        }
        if let Some(ms) = m.reconnect_grace_ms {
            cfg.mail.reconnect_grace_ms = ms;
        }
        cfg.mail.toast_cmd = m.toast_cmd;
    }
    cfg.validate().map_err(|e| e.to_string())?;
    Ok(cfg)
}

/// Reads `config.toml` safely (`O_NOFOLLOW`, bounded to 256 KiB), returning `Ok(None)` when absent.
pub fn load_config_toml(path: &Path) -> Result<Option<(RexConfig, String, String)>, String> {
    let bytes = match rex_wave_fs::read_regular_bounded(path, CONFIG_FILE_MAX_BYTES) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("read {}: {e}", path.display())),
    };
    let raw = std::str::from_utf8(&bytes)
        .map_err(|_| format!("{} is not valid UTF-8", path.display()))?
        .to_owned();
    let sha = sha256_hex(&bytes);
    let cfg = parse_config_toml(&raw).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(Some((cfg, sha, raw)))
}

fn set_or_remove_str(table: &mut Table, key: &str, val: Option<&str>) {
    match val {
        Some(v) => {
            if table.get(key).and_then(Item::as_str) == Some(v) {
                return;
            }
            let mut next_val = toml_edit::Value::from(v);
            if let Some(existing_val) = table.get(key).and_then(Item::as_value) {
                *next_val.decor_mut() = existing_val.decor().clone();
            }
            table[key] = Item::Value(next_val);
        }
        None => {
            table.remove(key);
        }
    }
}

fn set_bool_preserving_decor(table: &mut Table, key: &str, val: bool) {
    if table.get(key).and_then(Item::as_bool) == Some(val) {
        return;
    }
    let mut next_val = toml_edit::Value::from(val);
    if let Some(existing_val) = table.get(key).and_then(Item::as_value) {
        *next_val.decor_mut() = existing_val.decor().clone();
    }
    table[key] = Item::Value(next_val);
}

fn set_i64_preserving_decor(table: &mut Table, key: &str, val: i64) {
    if table.get(key).and_then(Item::as_integer) == Some(val) {
        return;
    }
    let mut next_val = toml_edit::Value::from(val);
    if let Some(existing_val) = table.get(key).and_then(Item::as_value) {
        *next_val.decor_mut() = existing_val.decor().clone();
    }
    table[key] = Item::Value(next_val);
}

fn ensure_subtable<'a>(parent: &'a mut Table, key: &str) -> &'a mut Table {
    if !parent.contains_key(key) || !parent[key].is_table() {
        let mut t = Table::new();
        t.set_implicit(true);
        parent.insert(key, Item::Table(t));
    }
    parent[key].as_table_mut().expect("table just inserted")
}

fn migrate_alias_key(table: &mut Table, canon: &str, aliases: &[&str]) {
    let mut migrated: Option<Item> = None;
    for alias in aliases {
        if let Some(item) = table.remove(alias)
            && migrated.is_none()
        {
            migrated = Some(item);
        }
    }
    if !table.contains_key(canon)
        && let Some(item) = migrated
    {
        table.insert(canon, item);
    }
}

fn migrate_alias_subtable(parent: &mut Table, canon: &str, aliases: &[&str]) {
    let mut migrated: Option<Item> = None;
    for alias in aliases {
        if let Some(item) = parent.remove(alias)
            && item.is_table()
            && migrated.is_none()
        {
            migrated = Some(item);
        }
    }
    if !parent.contains_key(canon)
        && let Some(item) = migrated
    {
        parent.insert(canon, item);
    }
}

/// Updates `existing_toml` in place via `toml_edit::DocumentMut` so operator comments,
/// inline value decorations, and unrelated formatting survive programmatic updates.
pub fn render_config_toml_preserving_comments(
    existing_toml: Option<&str>,
    cfg: &RexConfig,
) -> String {
    let mut doc: DocumentMut = existing_toml
        .and_then(|s| s.parse::<DocumentMut>().ok())
        .unwrap_or_else(|| {
            "# rex-wave operator configuration\n"
                .parse::<DocumentMut>()
                .expect("valid header")
        });

    {
        let runners = ensure_subtable(doc.as_table_mut(), "runners");
        runners.set_implicit(false);
        migrate_alias_key(runners, "default_runner", &["default"]);
        migrate_alias_key(runners, "claude", &["claude-code", "claudeAgent"]);
        migrate_alias_key(runners, "agy", &["antigravity"]);
        migrate_alias_key(runners, "opencode", &["open-code"]);
        set_or_remove_str(
            runners,
            "default_runner",
            cfg.runners.default_runner.map(RunnerRoute::as_str),
        );
        set_or_remove_str(
            runners,
            "claude",
            cfg.runners.claude.map(RunnerRoute::as_str),
        );
        set_or_remove_str(runners, "codex", cfg.runners.codex.map(RunnerRoute::as_str));
        set_or_remove_str(runners, "agy", cfg.runners.agy.map(RunnerRoute::as_str));
        set_or_remove_str(
            runners,
            "opencode",
            cfg.runners.opencode.map(RunnerRoute::as_str),
        );
    }

    {
        let harnesses = ensure_subtable(doc.as_table_mut(), "harnesses");
        migrate_alias_subtable(harnesses, "claude", &["claude-code", "claudeAgent"]);
        migrate_alias_subtable(harnesses, "agy", &["antigravity"]);
        migrate_alias_subtable(harnesses, "opencode", &["open-code"]);
        for slot in HarnessProvider::ALL {
            let prof = cfg.harnesses.profile(slot);
            if prof.is_empty() {
                harnesses.remove(slot.slug());
                continue;
            }
            let sub = ensure_subtable(harnesses, slot.slug());
            sub.set_implicit(false);
            set_or_remove_str(sub, "binary", prof.binary.as_deref());
            set_or_remove_str(sub, "model", prof.model.as_ref().map(|m| m.as_str()));
            set_or_remove_str(sub, "reasoning", prof.reasoning.as_deref());
            set_or_remove_str(sub, "account", prof.account.as_deref());
            let perm_str = prof.permission.map(|p| match p {
                rex_wave_api::wave::PermissionMode::AcceptEdits => "accept-edits",
                rex_wave_api::wave::PermissionMode::Auto => "auto",
                rex_wave_api::wave::PermissionMode::Full => "full",
            });
            set_or_remove_str(sub, "permission", perm_str);
            set_or_remove_str(sub, "workspace", prof.workspace.map(|w| w.as_str()));
        }
    }

    {
        let mail = ensure_subtable(doc.as_table_mut(), "mail");
        mail.set_implicit(false);
        set_bool_preserving_decor(mail, "export_enabled", cfg.mail.export_enabled);
        set_or_remove_str(
            mail,
            "mirror_dir",
            cfg.mail.mirror_dir.as_deref().and_then(Path::to_str),
        );
        set_or_remove_str(
            mail,
            "drop_dir",
            cfg.mail.drop_dir.as_deref().and_then(Path::to_str),
        );
        set_or_remove_str(
            mail,
            "couriers_dir",
            cfg.mail.couriers_dir.as_deref().and_then(Path::to_str),
        );
        set_or_remove_str(
            mail,
            "hub_owner_seat",
            Some(cfg.mail.hub_owner_seat.as_str()),
        );
        set_i64_preserving_decor(
            mail,
            "reconnect_grace_ms",
            cfg.mail.reconnect_grace_ms as i64,
        );
        set_or_remove_str(mail, "toast_cmd", cfg.mail.toast_cmd.as_deref());
    }

    let mut out = doc.to_string();
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

/// Validates `mail.mirror_dir` and `mail.drop_dir` against `protected-roots` so Gate 4
/// directories (such as live `interchange/`) can never be configured as write/mirror targets.
/// Allows not-yet-created directories outside protected roots while rejecting symlinks,
/// existing regular files, and any lexical or symlinked descendant of a protected root.
pub fn validate_config_protected_roots(
    cfg: &RexConfig,
    protection: &Protection,
) -> Result<(), ApiError> {
    let guard = ProtectedRootsGuard::resolve(protection).map_err(|e| ApiError::Denied {
        principal: "config".into(),
        action: format!("resolve protected-roots: {e}"),
    })?;
    for (field, maybe_path) in [
        ("mail.mirror_dir", cfg.mail.mirror_dir.as_deref()),
        ("mail.drop_dir", cfg.mail.drop_dir.as_deref()),
    ] {
        if let Some(path) = maybe_path {
            let expanded = rex_wave_waves::expand_tilde(&path.to_string_lossy());
            let abs = if expanded.is_absolute() {
                expanded
            } else {
                std::env::current_dir()
                    .unwrap_or_else(|_| PathBuf::from("/"))
                    .join(expanded)
            };
            let reject = |err: rex_wave_fs::SandboxError| ApiError::Denied {
                principal: "config".into(),
                action: format!("{field} rejected by protected-roots: {err}"),
            };
            if rex_wave_fs::entry_exists(&abs) {
                guard.verify_dir(&abs).map_err(reject)?;
            } else {
                let lex = rex_wave_fs::lexical_normalize(&abs).map_err(reject)?;
                guard.check(&lex, path).map_err(reject)?;
                let canon_prefix =
                    rex_wave_fs::canonicalize_existing_prefix(&abs).map_err(reject)?;
                guard.check(&canon_prefix, path).map_err(reject)?;
            }
        }
    }
    Ok(())
}

/// Writes `config.toml` atomically (`0600`, `O_NOFOLLOW`, `sync_parent = true`), creating
/// missing parent config directories (`~/.config/rex-wave`) with mode `0700`.
pub fn write_config_toml_atomic(path: &Path, cfg: &RexConfig) -> Result<String, String> {
    let existing = match rex_wave_fs::read_regular_bounded(path, CONFIG_FILE_MAX_BYTES) {
        Ok(b) => std::str::from_utf8(&b).ok().map(str::to_owned),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(format!("read existing {}: {e}", path.display())),
    };
    let rendered = render_config_toml_preserving_comments(existing.as_deref(), cfg);
    if rendered.len() as u64 > CONFIG_FILE_MAX_BYTES {
        return Err(format!(
            "rendered {} ({} bytes) exceeds {} byte limit",
            path.display(),
            rendered.len(),
            CONFIG_FILE_MAX_BYTES
        ));
    }
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && !parent.exists()
    {
        rex_wave_fs::create_missing_private_dir(parent)
            .map_err(|e| format!("create config dir {}: {e}", parent.display()))?;
    }
    rex_wave_fs::write_atomic_path(path, rendered.as_bytes(), 0o600, true)
        .map_err(|e| format!("write {}: {e}", path.display()))?;
    Ok(sha256_hex(rendered.as_bytes()))
}

fn load_sync_marker(state_dir: &Path) -> Option<SyncMarker> {
    let path = state_dir.join(CONFIG_SYNC_STATE_FILE);
    let bytes = rex_wave_fs::read_regular_bounded(&path, 4096).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn save_sync_marker_opt(state_dir: &Path, seq: Seq, sha256: Option<&str>) -> Result<(), String> {
    let path = state_dir.join(CONFIG_SYNC_STATE_FILE);
    let marker = SyncMarker {
        seq: seq.0,
        sha256: sha256.map(str::to_owned),
    };
    let bytes = serde_json::to_vec(&marker).map_err(|e| e.to_string())?;
    rex_wave_fs::write_atomic_path(&path, &bytes, 0o600, true)
        .map_err(|e| format!("write {}: {e}", path.display()))
}

fn save_sync_marker(state_dir: &Path, seq: Seq, sha256: &str) -> Result<(), String> {
    save_sync_marker_opt(state_dir, seq, Some(sha256))
}

/// Scans `couriers.d/` without following symlinks to report discovered courier adapters.
pub fn scan_couriers_dir(dir: &Path) -> Vec<CourierHealthStatus> {
    if rex_wave_fs::entry_kind(dir).ok() != Some(EntryKind::Dir) {
        return Vec::new();
    }
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in rd.flatten() {
        let name_os = entry.file_name();
        let Some(name) = name_os.to_str() else {
            continue;
        };
        if name.starts_with('.') || name.ends_with('~') {
            continue;
        }
        let full = entry.path();
        if rex_wave_fs::entry_kind(&full).ok() != Some(EntryKind::File) {
            continue;
        }
        let executable = rex_wave_fs::open_regular_nofollow(&full)
            .map(|(_f, m)| (m.permissions().mode() & 0o111) != 0)
            .unwrap_or(false);
        out.push(CourierHealthStatus {
            name: name.to_owned(),
            executable,
            rung: 0,
            unknown: 0,
            refused: 0,
            degraded: false,
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

pub type LivenessProbe = Arc<dyn Fn() -> bool + Send + Sync>;

pub struct TomlConfigHook {
    config_path: Option<PathBuf>,
    state_dir: PathBuf,
    daemon_default_runner: RunnerRoute,
    bb_configured: bool,
    t3_configured: bool,
    last_exported_sha: Arc<std::sync::Mutex<Option<Option<String>>>>,
    bb_live: OnceLock<LivenessProbe>,
    t3_live: OnceLock<LivenessProbe>,
    presence_svc: OnceLock<Arc<dyn Service<PresenceCommand>>>,
}

impl TomlConfigHook {
    pub fn new(
        config_path: Option<PathBuf>,
        state_dir: PathBuf,
        daemon_default_runner: RunnerRoute,
        bb_configured: bool,
        t3_configured: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            config_path,
            state_dir,
            daemon_default_runner,
            bb_configured,
            t3_configured,
            last_exported_sha: Arc::new(std::sync::Mutex::new(None)),
            bb_live: OnceLock::new(),
            t3_live: OnceLock::new(),
            presence_svc: OnceLock::new(),
        })
    }

    pub fn bind_presence(&self, svc: Arc<dyn Service<PresenceCommand>>) {
        let _ = self.presence_svc.set(svc);
    }

    pub fn bind_runner_liveness(
        &self,
        bb_live: Option<LivenessProbe>,
        t3_live: Option<LivenessProbe>,
    ) {
        if let Some(p) = bb_live {
            let _ = self.bb_live.set(p);
        }
        if let Some(p) = t3_live {
            let _ = self.t3_live.set(p);
        }
    }

    pub fn config_path(&self) -> Option<&Path> {
        self.config_path.as_deref()
    }
}

impl ConfigHook for TomlConfigHook {
    fn validate_candidate<'a>(
        &'a self,
        current: &'a RexConfig,
        next: &'a RexConfig,
        will_export: bool,
    ) -> BoxFut<'a, Result<(), ApiError>> {
        let cur = current.clone();
        let nxt = next.clone();
        let path_opt = self.config_path.clone();
        let state_dir = self.state_dir.clone();
        let last_exported_sha = self.last_exported_sha.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let protection = load_protected_roots().map_err(|e| ApiError::Denied {
                    principal: "config".into(),
                    action: format!("load protected-roots: {e}"),
                })?;
                validate_config_protected_roots(&nxt, &protection)?;

                if will_export && let Some(ref path) = path_opt {
                    let expected_opt = last_exported_sha
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .clone()
                        .or_else(|| load_sync_marker(&state_dir).map(|m| m.sha256));

                    let existing = match rex_wave_fs::read_regular_bounded(
                        path,
                        CONFIG_FILE_MAX_BYTES,
                    ) {
                        Ok(b) => {
                            let disk_sha = sha256_hex(&b);
                            if let Some(ref exp) = expected_opt
                                && exp.as_deref() != Some(disk_sha.as_str())
                            {
                                let disk_matches_known = std::str::from_utf8(&b)
                                    .ok()
                                    .and_then(|s| parse_config_toml(s).ok())
                                    .is_some_and(|d| d == cur || d == nxt);
                                if !disk_matches_known {
                                    return Err(ApiError::conflict(format!(
                                        "{} was modified externally while daemon is running; restart daemon to import external edits",
                                        path.display()
                                    )));
                                }
                            }
                            std::str::from_utf8(&b).ok().map(str::to_owned)
                        }
                        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
                        Err(e) => {
                            return Err(ApiError::Invalid(InvalidId {
                                what: "config.toml".into(),
                                reason: format!("read existing {}: {e}", path.display()),
                            }));
                        }
                    };
                    let rendered =
                        render_config_toml_preserving_comments(existing.as_deref(), &nxt);
                    if rendered.len() as u64 > CONFIG_FILE_MAX_BYTES {
                        return Err(ApiError::Invalid(InvalidId {
                            what: "config.toml".into(),
                            reason: format!(
                                "rendered config.toml ({} bytes) exceeds {} byte limit",
                                rendered.len(),
                                CONFIG_FILE_MAX_BYTES
                            ),
                        }));
                    }
                    let round_tripped = parse_config_toml(&rendered).map_err(|reason| {
                        ApiError::Invalid(InvalidId {
                            what: "config.toml".into(),
                            reason: format!("rendered TOML failed strict parse: {reason}"),
                        })
                    })?;
                    if round_tripped != nxt {
                        return Err(ApiError::Internal(
                            "rendered config.toml round-trip mismatch".into(),
                        ));
                    }
                }
                Ok::<(), ApiError>(())
            })
            .await
            .map_err(|e| ApiError::Internal(format!("config validate task join: {e}")))?
        })
    }

    fn on_committed(&self, head: Seq, config: &RexConfig) -> BoxFut<'_, Result<(), ApiError>> {
        let cfg = config.clone();
        let path_opt = self.config_path.clone();
        let state_dir = self.state_dir.clone();
        let last_exported_sha = self.last_exported_sha.clone();
        Box::pin(async move {
            if let Some(path) = path_opt {
                tokio::task::spawn_blocking(move || {
                    let sha = write_config_toml_atomic(&path, &cfg).map_err(ApiError::Internal)?;
                    *last_exported_sha.lock().unwrap_or_else(|e| e.into_inner()) =
                        Some(Some(sha.clone()));
                    save_sync_marker(&state_dir, head, &sha).map_err(ApiError::Internal)?;
                    Ok::<(), ApiError>(())
                })
                .await
                .map_err(|e| ApiError::Internal(format!("config sync task join: {e}")))??;
            }
            Ok(())
        })
    }

    fn augment_snapshot<'a>(&'a self, snap: &'a mut ConfigSnapshot) -> BoxFut<'a, ()> {
        Box::pin(async move {
            snap.config_path = self.config_path.clone();
            snap.effective_default_runner = Some(
                snap.config
                    .runners
                    .default_runner
                    .unwrap_or(self.daemon_default_runner),
            );

            let mut down_bb: Option<String> = None;
            let mut down_t3: Option<String> = None;
            if let Some(probe) = self.bb_live.get()
                && !probe()
            {
                down_bb = Some("disconnected".into());
            }
            if let Some(probe) = self.t3_live.get()
                && !probe()
            {
                down_t3 = Some("disconnected".into());
            }
            if let Some(presence) = self.presence_svc.get()
                && let Ok(WireReply::PresenceList(psnap)) = presence
                    .dispatch(
                        PresenceCommand::PresenceList(PresenceList {
                            project: None,
                            live_only: false,
                            include_retired: false,
                        }),
                        DecideCtx::for_system_part(SystemPart::ConfigSync),
                    )
                    .await
            {
                for kind in psnap.down_transports {
                    match kind {
                        TransportKind::BbThread if down_bb.is_none() => {
                            down_bb = Some("bridge offline".into());
                        }
                        TransportKind::T3Thread if down_t3.is_none() => {
                            down_t3 = Some("bridge offline".into());
                        }
                        _ => {}
                    }
                }
            }

            snap.runners_health = vec![
                RunnerHealthStatus {
                    runner: RunnerRoute::Bb,
                    configured: self.bb_configured,
                    healthy: self.bb_configured && down_bb.is_none(),
                    detail: if !self.bb_configured {
                        Some("disabled".into())
                    } else {
                        down_bb
                    },
                },
                RunnerHealthStatus {
                    runner: RunnerRoute::T3,
                    configured: self.t3_configured,
                    healthy: self.t3_configured && down_t3.is_none(),
                    detail: if !self.t3_configured {
                        Some("disabled".into())
                    } else {
                        down_t3
                    },
                },
                RunnerHealthStatus {
                    runner: RunnerRoute::Native,
                    configured: false,
                    healthy: false,
                    detail: Some("PRs #30/#31".into()),
                },
            ];

            if let Some(cdir) = effective_couriers_dir(&snap.config, self.config_path.as_deref()) {
                snap.couriers_health = scan_couriers_dir(&cdir);
            }
        })
    }
}

/// Reconciles `StreamId::Config` in `journal` with `config_path` at daemon startup:
/// - Selects the active startup candidate first (importing an offline-edited `config.toml`
///   even if `protected-roots` was tightened since the previous run, so offline file repairs work!).
/// - Validates the chosen candidate against `protected-roots` on EVERY return path (including
///   `--config ""` when file sync is disabled).
/// - Note: deleting `config.toml` while `StreamId::Config` has committed entries re-exports
///   the journaled configuration back to `config.toml` (use `config unset` or edit keys to clear).
pub async fn reconcile_startup_config(
    journal: &Journal,
    svc: &ConfigService,
    config_path: Option<&Path>,
    state_dir: &Path,
) -> Result<RexConfig, String> {
    let protection = load_protected_roots().map_err(|e| format!("load protected-roots: {e}"))?;
    let (journal_head, journal_cfg) = rex_wave_runtime::replay_config_from_journal(journal)
        .map_err(|e| format!("replay config stream: {e}"))?;

    let Some(path) = config_path else {
        validate_config_protected_roots(&journal_cfg, &protection).map_err(|e| e.to_string())?;
        return Ok(journal_cfg);
    };

    let marker = load_sync_marker(state_dir);
    let disk = load_config_toml(path)?;

    match (journal_head, disk, marker) {
        (Seq(0), None, _) => {
            validate_config_protected_roots(&journal_cfg, &protection)
                .map_err(|e| e.to_string())?;
            save_sync_marker_opt(state_dir, Seq(0), None)?;
            Ok(journal_cfg)
        }
        (Seq(0), Some((disk_cfg, disk_sha, _)), _) => {
            validate_config_protected_roots(&disk_cfg, &protection).map_err(|e| e.to_string())?;
            let reply = svc
                .dispatch(
                    ConfigCommand::ConfigSyncFile(ConfigSyncFile {
                        config: disk_cfg.clone(),
                    }),
                    DecideCtx::for_system_part(SystemPart::ConfigSync),
                )
                .await
                .map_err(|e| format!("sync initial config.toml: {e}"))?;
            if let WireReply::ConfigSyncFile(snap) = reply {
                save_sync_marker(state_dir, snap.head, &disk_sha)?;
            }
            Ok(svc.current())
        }
        (jhead, None, _) => {
            validate_config_protected_roots(&journal_cfg, &protection)
                .map_err(|e| e.to_string())?;
            let sha = write_config_toml_atomic(path, &journal_cfg)?;
            save_sync_marker(state_dir, jhead, &sha)?;
            Ok(journal_cfg)
        }
        (jhead, Some((disk_cfg, disk_sha, _)), maybe_marker) => {
            if disk_cfg == journal_cfg {
                validate_config_protected_roots(&journal_cfg, &protection)
                    .map_err(|e| e.to_string())?;
                save_sync_marker(state_dir, jhead, &disk_sha)?;
                return Ok(journal_cfg);
            }
            let file_was_unedited = maybe_marker
                .as_ref()
                .is_some_and(|m| m.sha256.as_deref() == Some(disk_sha.as_str()) && jhead.0 > m.seq);
            if file_was_unedited {
                // Crashed after SQLite commit and before config.toml export: finish the export.
                validate_config_protected_roots(&journal_cfg, &protection)
                    .map_err(|e| e.to_string())?;
                let sha = write_config_toml_atomic(path, &journal_cfg)?;
                save_sync_marker(state_dir, jhead, &sha)?;
                Ok(journal_cfg)
            } else {
                // Operator modified config.toml externally while stopped: validate and import disk_cfg.
                validate_config_protected_roots(&disk_cfg, &protection)
                    .map_err(|e| e.to_string())?;
                let reply = svc
                    .dispatch(
                        ConfigCommand::ConfigSyncFile(ConfigSyncFile { config: disk_cfg }),
                        DecideCtx::for_system_part(SystemPart::ConfigSync),
                    )
                    .await
                    .map_err(|e| format!("import modified config.toml: {e}"))?;
                if let WireReply::ConfigSyncFile(snap) = reply {
                    save_sync_marker(state_dir, snap.head, &disk_sha)?;
                }
                Ok(svc.current())
            }
        }
    }
}

/// Helper used by tests or LocalClient callers to query `ConfigShow` cleanly.
#[allow(dead_code)]
pub async fn fetch_config_snapshot(client: &LocalClient) -> Result<ConfigSnapshot, ApiError> {
    client.call(rex_wave_api::config::ConfigShow {}).await
}
