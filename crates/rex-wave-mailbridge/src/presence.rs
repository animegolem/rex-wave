//! `Family::Presence` decider (`StreamId::Presence`), explicit opt-in Claude
//! session hook parser, safe `ps`-backed process liveness verification,
//! `in_reply_to` incarnation affinity, and sender-first delivery notices.

use rex_wave_api::presence::*;
use rex_wave_api::*;
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Default lease TTL (10 minutes) for `ProcessAndLease` and `LeaseOnly` bindings
/// when `PresenceBind.ttl_ms` or `PresenceHeartbeat.ttl_ms` is omitted.
pub const DEFAULT_LEASE_TTL_MS: u64 = 600_000;

/// Threshold distinguishing real Unix-epoch timestamps from synthetic unit-test clocks.
const REAL_EPOCH_FLOOR_MS: u64 = 1_500_000_000_000;

/// Collapses ASCII whitespace runs so single-digit day padding (`Nov  6` vs
/// `Nov 6`) compares identically across `ps` and Claude session files.
pub fn normalize_proc_start(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn proc_start_matches(expected: &str, actual: &str) -> bool {
    let e = normalize_proc_start(expected);
    let a = normalize_proc_start(actual);
    !e.is_empty() && e == a
}

fn try_query_process_start_with_bin(bin: &str, pid: u32) -> std::io::Result<Option<String>> {
    if pid == 0 {
        return Ok(None);
    }
    let output = Command::new(bin)
        .args(["-p", &pid.to_string(), "-o", "lstart="])
        .env("TZ", "UTC")
        .env("LC_ALL", "C")
        .output()?;
    if !output.status.success() {
        return Ok(None);
    }
    let Ok(text) = String::from_utf8(output.stdout) else {
        return Ok(None);
    };
    let line = text.lines().map(str::trim).find(|l| !l.is_empty());
    Ok(line.map(str::to_owned))
}

/// Queries the OS process start time in UTC (`TZ=UTC LC_ALL=C ps -p <pid> -o lstart=`)
/// without `unsafe`, distinguishing probe execution failure (`Err`) from a dead
/// process (`Ok(None)`).
pub fn try_query_process_start(pid: u32) -> std::io::Result<Option<String>> {
    try_query_process_start_with_bin("ps", pid)
}

/// Queries the OS process start time in UTC (`TZ=UTC LC_ALL=C ps -p <pid> -o lstart=`)
/// without `unsafe`, matching Claude Code's UTC `procStart` field verbatim.
pub fn query_process_start(pid: u32) -> Option<String> {
    try_query_process_start(pid).ok().flatten()
}

fn is_process_alive_with_bin(bin: &str, pid: u32, expected_proc_start: &str) -> bool {
    if pid == 0 || expected_proc_start.trim().is_empty() {
        return false;
    }
    match try_query_process_start_with_bin(bin, pid) {
        Ok(Some(actual)) => proc_start_matches(expected_proc_start, &actual),
        Ok(None) => false,
        Err(_) => true,
    }
}

/// Returns `true` when `pid` is currently running and its UTC `lstart` matches
/// `expected_proc_start`. Fails closed (`true`) if `ps` itself cannot execute so
/// a live session is never silently superseded on probe failure.
pub fn is_process_alive(pid: u32, expected_proc_start: &str) -> bool {
    is_process_alive_with_bin("ps", pid, expected_proc_start)
}

fn probe_process_alive(pid: u32, expected_proc_start: &str) -> bool {
    if let Ok(handle) = tokio::runtime::Handle::try_current()
        && handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    {
        tokio::task::block_in_place(|| is_process_alive(pid, expected_proc_start))
    } else {
        is_process_alive(pid, expected_proc_start)
    }
}

/// Returns the current process's UTC `lstart`, useful for hooks and tests.
pub fn current_process_start() -> Option<String> {
    query_process_start(std::process::id())
}

/// Parsed `~/.claude/sessions/<pid>.json` payload loaded explicitly on opt-in
/// hook invocation (never scanned passively).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaudeSessionFile {
    pub pid: u32,
    pub proc_start: String,
    pub started_at_ms: u64,
    pub session_id: String,
    pub messaging_socket: PathBuf,
    pub kind: String,
    pub name: Option<String>,
    pub name_source: Option<String>,
    pub former_names: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub status: Option<String>,
}

fn invalid(what: &str, reason: impl Into<String>) -> ApiError {
    ApiError::Invalid(InvalidId {
        what: what.into(),
        reason: reason.into(),
    })
}

fn json_str<'a>(
    obj: &'a serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<&'a str> {
    keys.iter()
        .find_map(|k| obj.get(*k).and_then(|v| v.as_str()))
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// Parses one Claude Code session JSON document into [`ClaudeSessionFile`].
pub fn parse_claude_session_json(raw: &str) -> Result<ClaudeSessionFile, ApiError> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| invalid("claude_session", format!("invalid JSON: {e}")))?;
    let obj = value
        .as_object()
        .ok_or_else(|| invalid("claude_session", "session file must be a JSON object"))?;

    let pid_u64 = obj
        .get("pid")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| invalid("claude_session.pid", "missing integer pid"))?;
    let pid = u32::try_from(pid_u64)
        .ok()
        .filter(|&p| p > 0)
        .ok_or_else(|| invalid("claude_session.pid", "pid must be a positive u32"))?;

    let proc_start = json_str(obj, &["procStart", "proc_start"])
        .ok_or_else(|| invalid("claude_session.procStart", "missing procStart"))?
        .to_owned();

    let started_at_ms = obj
        .get("startedAt")
        .or_else(|| obj.get("started_at_ms"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let session_id = json_str(obj, &["sessionId", "session_id"])
        .ok_or_else(|| invalid("claude_session.sessionId", "missing sessionId"))?
        .to_owned();

    let messaging_socket = json_str(obj, &["messagingSocketPath", "messaging_socket"])
        .map(PathBuf::from)
        .ok_or_else(|| {
            invalid(
                "claude_session.messagingSocketPath",
                "missing messagingSocketPath",
            )
        })?;

    let kind = json_str(obj, &["kind"])
        .ok_or_else(|| invalid("claude_session.kind", "missing session kind"))?
        .to_owned();

    let name = json_str(obj, &["name", "display_name"]).map(str::to_owned);
    let name_source = json_str(obj, &["nameSource", "name_source"]).map(str::to_owned);
    let former_names = obj
        .get("formerNames")
        .or_else(|| obj.get("former_names"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::trim).filter(|s| !s.is_empty()))
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let cwd = json_str(obj, &["cwd"]).map(PathBuf::from);
    let status = json_str(obj, &["status"]).map(str::to_owned);

    Ok(ClaudeSessionFile {
        pid,
        proc_start,
        started_at_ms,
        session_id,
        messaging_socket,
        kind,
        name,
        name_source,
        former_names,
        cwd,
        status,
    })
}

/// Reads and parses `<sessions_dir>/<pid>.json` for an explicit hook call.
/// Never scans the directory.
pub fn load_claude_session_file_from(
    sessions_dir: &Path,
    pid: u32,
) -> Result<ClaudeSessionFile, ApiError> {
    if pid == 0 {
        return Err(invalid("pid", "pid must be positive"));
    }
    let path = sessions_dir.join(format!("{pid}.json"));
    let raw = read_text_nofollow(
        &path,
        MAX_SESSION_FILE_BYTES,
        "claude_session",
        "session file",
    )?
    .ok_or_else(|| ApiError::NotFound {
        what: format!("claude session file {}", path.display()),
    })?;
    let parsed = parse_claude_session_json(&raw)?;
    if parsed.pid != pid {
        return Err(invalid(
            "claude_session.pid",
            format!("file {pid}.json declared mismatched pid {}", parsed.pid),
        ));
    }
    Ok(parsed)
}

/// Reads and parses `$HOME/.claude/sessions/<pid>.json` on explicit
/// `--from-claude-session` hook invocation. Never scans `~/.claude/sessions/`.
pub fn load_claude_session_file(pid: u32) -> Result<ClaudeSessionFile, ApiError> {
    let home = std::env::var_os("HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| invalid("HOME", "HOME is not set"))?;
    load_claude_session_file_from(&home.join(".claude").join("sessions"), pid)
}

impl ClaudeSessionFile {
    /// Enforces Claude session binding guardrails:
    /// `kind` must be `"interactive"` or `"bg"`, and `⑂` forks are rejected
    /// unless `allow_fork` is explicitly set.
    pub fn validate_for_bind(&self, allow_fork: bool) -> Result<(), ApiError> {
        if !TransportEndpoint::is_allowed_claude_kind(&self.kind) {
            return Err(invalid(
                "claude_session.kind",
                format!(
                    "session kind '{}' is not allowed (only 'interactive' and 'bg' may bind)",
                    self.kind
                ),
            ));
        }
        if !allow_fork
            && self
                .name
                .as_deref()
                .is_some_and(TransportEndpoint::is_fork_display_name)
        {
            return Err(invalid(
                "claude_session.display_name",
                "fork session ('⑂') is refused unless allow_fork is explicitly set",
            ));
        }
        Ok(())
    }

    /// Converts a validated session file into a [`TransportEndpoint::ClaudeSession`].
    pub fn into_endpoint(&self, allow_fork: bool) -> Result<TransportEndpoint, ApiError> {
        self.validate_for_bind(allow_fork)?;
        Ok(TransportEndpoint::ClaudeSession {
            pid: self.pid,
            proc_start: self.proc_start.clone(),
            started_at_ms: self.started_at_ms,
            session_id: self.session_id.clone(),
            messaging_socket: self.messaging_socket.clone(),
            kind: self.kind.clone(),
            name_source: self.name_source.clone(),
            display_name: self.name.clone(),
        })
    }

    /// Returns `true` when the harness derived the session's display name
    /// (`nameSource == "derived"`). Seat identity is always passed explicitly to
    /// [`Self::into_bind_command`] and is never derived from a session display name.
    pub fn is_derived_name(&self) -> bool {
        TransportEndpoint::is_derived_name_source(self.name_source.as_deref())
    }

    /// Builds a [`PresenceBind`] command for an explicitly supplied `seat`.
    pub fn into_bind_command(
        &self,
        seat: SeatName,
        lane: Option<String>,
        ttl_ms: Option<u64>,
        takeover: bool,
        allow_fork: bool,
    ) -> Result<PresenceBind, ApiError> {
        let endpoint = self.into_endpoint(allow_fork)?;
        Ok(PresenceBind {
            seat,
            incarnation_id: None,
            lane,
            endpoint,
            liveness: LivenessMode::ProcessAndLease,
            ttl_ms,
            takeover,
            allow_fork,
            mail_horizon_at: None,
            idem: None,
        })
    }
}

/// Largest `house.json` the loader accepts. A house profile is a handful of
/// names; anything bigger is a mistake, not a configuration.
pub const HOUSE_PROFILE_MAX_BYTES: u64 = 64 * 1024;

/// Resolves the optional house profile path:
/// `$XDG_CONFIG_HOME/rex-wave/house.json` when `XDG_CONFIG_HOME` is absolute,
/// otherwise `$HOME/.config/rex-wave/house.json`. Relative `XDG_CONFIG_HOME`
/// values are ignored per the XDG base directory spec.
pub fn house_profile_path_from(
    xdg_config_home: Option<&std::ffi::OsStr>,
    home: Option<&std::ffi::OsStr>,
) -> Option<PathBuf> {
    let base = xdg_config_home
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| {
            home.filter(|v| !v.is_empty())
                .map(|h| PathBuf::from(h).join(".config"))
        })?;
    Some(base.join("rex-wave").join("house.json"))
}

/// Reads one house profile file. `NotFound` yields [`HouseProfile::default`];
/// a symlink, non-regular file, oversized file, malformed JSON, unknown field,
/// invalid seat name, or out-of-bounds list fails closed.
pub fn load_house_profile_from(path: &Path) -> Result<HouseProfile, ApiError> {
    let Some(raw) = read_text_nofollow(
        path,
        HOUSE_PROFILE_MAX_BYTES,
        "house_profile",
        "house profile",
    )?
    else {
        return Ok(HouseProfile::default());
    };
    let house: HouseProfile = serde_json::from_str(&raw)
        .map_err(|e| invalid("house_profile", format!("{}: {e}", path.display())))?;
    house
        .validate()
        .map_err(|e| invalid("house_profile", format!("{}: {e}", path.display())))?;
    Ok(house)
}

/// Loads the optional house profile from the environment-resolved path (see
/// [`house_profile_path_from`]). With no resolvable path or no file, returns
/// [`HouseProfile::default`].
pub fn load_house_profile() -> Result<HouseProfile, ApiError> {
    let xdg = std::env::var_os("XDG_CONFIG_HOME");
    let home = std::env::var_os("HOME");
    match house_profile_path_from(xdg.as_deref(), home.as_deref()) {
        Some(path) => load_house_profile_from(&path),
        None => Ok(HouseProfile::default()),
    }
}

/// Output of [`seed_records_from_interchange`]: durable [`PresenceClaim`]s ready
/// to apply (or preview under `--dry-run`) before seats run live `presence bind`.
#[derive(Clone, Debug)]
pub struct SeedInterchangePlan {
    pub claims: Vec<PresenceClaim>,
    pub sources_read: Vec<PathBuf>,
    pub skipped_targets: Vec<String>,
}

impl SeedInterchangePlan {
    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "sources_read": self
                .sources_read
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
            "skipped_targets": self.skipped_targets,
            "seat_count": self.claims.len(),
            "claims": self.claims,
        })
    }
}

/// The largest Claude session file read.
const MAX_SESSION_FILE_BYTES: u64 = 64 * 1024;
/// The largest interchange seed file read.
const MAX_SEED_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// Read a UTF-8 regular file of at most `max_bytes` through an `O_NOFOLLOW`
/// handle. Missing is `None`; a symlink or non-regular file is `Denied`; an
/// oversized or non-UTF-8 file is invalid `what`.
fn read_text_nofollow(
    path: &Path,
    max_bytes: u64,
    what: &str,
    label: &str,
) -> Result<Option<String>, ApiError> {
    match rex_wave_fs::read_regular_string(path, max_bytes) {
        Ok(raw) => Ok(Some(raw)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => Err(ApiError::Denied {
            principal: "presence".into(),
            action: format!("read non-regular {label} {}", path.display()),
        }),
        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => Err(invalid(what, e.to_string())),
        Err(e) => Err(ApiError::Internal(format!("read {}: {e}", path.display()))),
    }
}

fn read_regular_text_if_present(path: &Path) -> Result<Option<String>, ApiError> {
    read_text_nofollow(path, MAX_SEED_FILE_BYTES, "seed_file", "seed file")
}

fn read_regular_json_if_present(path: &Path) -> Result<Option<serde_json::Value>, ApiError> {
    let Some(raw) = read_regular_text_if_present(path)? else {
        return Ok(None);
    };
    let val = serde_json::from_str(&raw).map_err(|e| {
        invalid(
            "seed_json",
            format!("{}: invalid JSON: {e}", path.display()),
        )
    })?;
    Ok(Some(val))
}

fn resolve_local_or_parent(canon_dir: &Path, rel: &str) -> Option<PathBuf> {
    let local = canon_dir.join(rel);
    if rex_wave_fs::entry_exists(&local) {
        Some(local)
    } else {
        canon_dir
            .parent()
            .map(|p| p.join(rel))
            .filter(|p| rex_wave_fs::entry_exists(p))
    }
}

fn parse_seat_slug(raw: &str) -> Option<SeatName> {
    let norm = raw.trim().trim_start_matches('@').to_ascii_lowercase();
    if norm.is_empty() {
        return None;
    }
    SeatName::parse(norm).ok()
}

#[allow(clippy::too_many_arguments)]
fn register_seed_seat<'a>(
    house: &HouseProfile,
    raw_name: &str,
    machine_checked: bool,
    canonical_fold: &BTreeMap<String, SeatName>,
    skipped_targets: &mut BTreeSet<String>,
    machine_checked_seats: &mut BTreeSet<SeatName>,
    seat_aliases: &'a mut BTreeMap<SeatName, BTreeSet<SeatName>>,
) -> Option<(SeatName, &'a mut BTreeSet<SeatName>)> {
    if house.is_operator_seat(raw_name) || is_non_seat_target(raw_name) {
        skipped_targets.insert(raw_name.trim().to_ascii_lowercase());
        return None;
    }
    let raw_slug = parse_seat_slug(raw_name)?;
    let canon = canonical_fold
        .get(raw_slug.as_str())
        .cloned()
        .unwrap_or_else(|| raw_slug.clone());
    if machine_checked {
        machine_checked_seats.insert(canon.clone());
    }
    let set = seat_aliases.entry(canon.clone()).or_default();
    if raw_slug != canon {
        set.insert(raw_slug);
    }
    Some((canon, set))
}

/// Pure read-only helper that derives durable [`PresenceClaim`]s with aliases
/// and provenance from an interchange directory's `postmaster/state/roster.json`,
/// `.watchers/mailwatch-routes.json`, `tooling/t3-doorbell/targets.json`,
/// `state/bb-knock-routes.tsv`,
/// `state/codex-knock-routes.tsv`, `herdr-registry.tsv` (in `<dir>` or its
/// parent house root), `state/ledger.md`, and `house`'s canonical alias folds.
///
/// Never writes to disk and never emits `Active` claims for the house's
/// operator surface seat or non-seat pseudo-targets (`cc`, `desk`, etc.).
pub fn seed_records_from_interchange_with_profile(
    interchange_dir: &Path,
    house: &HouseProfile,
) -> Result<SeedInterchangePlan, ApiError> {
    seed_records_from_interchange_with_options(interchange_dir, None, house)
}

/// Extended variant of [`seed_records_from_interchange_with_profile`] that
/// also ingests an optional external `seats.tsv`
/// (`session_name\tseat\troom_path`) file when supplied.
pub fn seed_records_from_interchange_with_options(
    interchange_dir: &Path,
    seats_tsv: Option<&Path>,
    house: &HouseProfile,
) -> Result<SeedInterchangePlan, ApiError> {
    let interchange = rex_wave_fs::ConfinedDir::open(interchange_dir)
        .map_err(|e| invalid("interchange", format!("{}: {e}", interchange_dir.display())))?;
    let canon_dir = interchange.path().to_path_buf();

    let mut sources_read = Vec::new();
    let mut skipped_targets = BTreeSet::new();
    let mut seat_aliases: BTreeMap<SeatName, BTreeSet<SeatName>> = BTreeMap::new();
    let mut machine_checked_seats: BTreeSet<SeatName> = BTreeSet::new();

    // Reverse lookup for canonical pairs so roster/route entries under an alias
    // name fold into their canonical seat (e.g., `alice_old` -> `alice`).
    let mut canonical_fold: BTreeMap<String, SeatName> = BTreeMap::new();
    for (canon_seat, alias_list) in &house.aliases {
        canonical_fold.insert(canon_seat.as_str().to_owned(), canon_seat.clone());
        for alias_seat in alias_list {
            canonical_fold.insert(alias_seat.as_str().to_owned(), canon_seat.clone());
        }
    }

    // 1. Read `<interchange>/postmaster/state/roster.json` if present.
    let roster_path = canon_dir.join("postmaster/state/roster.json");
    if let Some(roster_val) = read_regular_json_if_present(&roster_path)? {
        sources_read.push(roster_path);
        if let Some(seats_obj) = roster_val.get("seats").and_then(|v| v.as_object()) {
            for entry in seats_obj.values() {
                let Some(raw_name) = entry.get("name").and_then(|v| v.as_str()) else {
                    continue;
                };
                let Some((canon, set)) = register_seed_seat(
                    house,
                    raw_name,
                    true,
                    &canonical_fold,
                    &mut skipped_targets,
                    &mut machine_checked_seats,
                    &mut seat_aliases,
                ) else {
                    continue;
                };
                // If `box` points to `.../interchange/to-<alias>`, register `<alias>` as an alias
                // when distinct from the canonical seat (e.g. `alice` -> `to-alice-desk`).
                if let Some(box_str) = entry.get("box").and_then(|v| v.as_str())
                    && let Some(last_seg) = box_str.trim_end_matches('/').rsplit('/').next()
                    && let Some(box_alias) = last_seg.strip_prefix("to-")
                    && let Some(alias_slug) = parse_seat_slug(box_alias)
                    && alias_slug != canon
                    && !house.is_operator_seat(alias_slug.as_str())
                    && !is_non_seat_target(alias_slug.as_str())
                {
                    set.insert(alias_slug);
                }
            }
        }
    }

    // 2. Read `.watchers/mailwatch-routes.json` and the T3 doorbell's
    //    `tooling/t3-doorbell/targets.json` under `<interchange>` or its parent
    //    house root. Both map `<seat>` -> `{.., "aliases": [..]}`.
    for rel in [
        ".watchers/mailwatch-routes.json",
        "tooling/t3-doorbell/targets.json",
    ] {
        let Some(routes_path) = resolve_local_or_parent(&canon_dir, rel) else {
            continue;
        };
        let Some(routes_val) = read_regular_json_if_present(&routes_path)? else {
            continue;
        };
        sources_read.push(routes_path);
        let Some(map) = routes_val.as_object() else {
            continue;
        };
        for (raw_key, entry) in map {
            // `_comment`, `_meta`, ... are annotations, never seats.
            if raw_key.starts_with('_') || !entry.is_object() {
                continue;
            }
            let Some((canon, set)) = register_seed_seat(
                house,
                raw_key,
                true,
                &canonical_fold,
                &mut skipped_targets,
                &mut machine_checked_seats,
                &mut seat_aliases,
            ) else {
                continue;
            };
            let Some(arr) = entry.get("aliases").and_then(|v| v.as_array()) else {
                continue;
            };
            for raw_alias in arr.iter().filter_map(|v| v.as_str()) {
                let cleaned = raw_alias
                    .trim()
                    .strip_prefix("to-")
                    .unwrap_or(raw_alias.trim());
                if let Some(alias_slug) = parse_seat_slug(cleaned)
                    && alias_slug != canon
                    && !house.is_operator_seat(alias_slug.as_str())
                    && !is_non_seat_target(alias_slug.as_str())
                {
                    set.insert(alias_slug);
                }
            }
        }
    }

    // 3. Read `<interchange>/state/bb-knock-routes.tsv` if present.
    let bb_routes_path = canon_dir.join("state/bb-knock-routes.tsv");
    if let Some(raw) = read_regular_text_if_present(&bb_routes_path)? {
        sources_read.push(bb_routes_path);
        for line in raw.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let mut cols = line.split('\t');
            let Some(raw_seat) = cols.next().map(str::trim).filter(|s| !s.is_empty()) else {
                continue;
            };
            let col1 = cols.next().map(str::trim).unwrap_or("");
            if raw_seat.eq_ignore_ascii_case("seat")
                && matches!(
                    col1.to_ascii_lowercase().as_str(),
                    "bb_thread_id" | "thread_id" | "thread"
                )
            {
                continue;
            }
            let _ = register_seed_seat(
                house,
                raw_seat,
                true,
                &canonical_fold,
                &mut skipped_targets,
                &mut machine_checked_seats,
                &mut seat_aliases,
            );
        }
    }

    // 4. Read `<interchange>/state/codex-knock-routes.tsv` if present.
    let codex_routes_path = canon_dir.join("state/codex-knock-routes.tsv");
    if let Some(raw) = read_regular_text_if_present(&codex_routes_path)? {
        sources_read.push(codex_routes_path);
        for line in raw.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let mut cols = line.split('\t');
            let Some(raw_seat) = cols.next().map(str::trim).filter(|s| !s.is_empty()) else {
                continue;
            };
            let col1 = cols.next().map(str::trim).unwrap_or("");
            if raw_seat.eq_ignore_ascii_case("seat")
                && matches!(
                    col1.to_ascii_lowercase().as_str(),
                    "hostid" | "host_id" | "threadid" | "thread_id"
                )
            {
                continue;
            }
            let _ = register_seed_seat(
                house,
                raw_seat,
                true,
                &canonical_fold,
                &mut skipped_targets,
                &mut machine_checked_seats,
                &mut seat_aliases,
            );
        }
    }

    // 5. Read `herdr-registry.tsv` under `<interchange>` or its parent house root.
    if let Some(herdr_path) = resolve_local_or_parent(&canon_dir, "herdr-registry.tsv")
        && let Some(raw) = read_regular_text_if_present(&herdr_path)?
    {
        sources_read.push(herdr_path);
        for line in raw.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let mut cols = line.split('\t');
            let Some(raw_seat) = cols.next().map(str::trim).filter(|s| !s.is_empty()) else {
                continue;
            };
            let col1 = cols.next().map(str::trim).unwrap_or("");
            if raw_seat.eq_ignore_ascii_case("seat")
                && matches!(
                    col1.to_ascii_lowercase().as_str(),
                    "target" | "pane" | "pane_id"
                )
            {
                continue;
            }
            if house.is_excluded_herdr_target(col1) {
                continue;
            }
            let _ = register_seed_seat(
                house,
                raw_seat,
                true,
                &canonical_fold,
                &mut skipped_targets,
                &mut machine_checked_seats,
                &mut seat_aliases,
            );
        }
    }

    // 6. Read `<interchange>/state/ledger.md` CLAIM rows if present.
    let ledger_path = canon_dir.join("state/ledger.md");
    if let Some(raw) = read_regular_text_if_present(&ledger_path)? {
        sources_read.push(ledger_path);
        for line in raw.lines() {
            let parts: Vec<&str> = line.split('|').map(str::trim).collect();
            if parts.len() < 3 || parts[1] != "CLAIM" {
                continue;
            }
            let raw_name = parts[2];
            if raw_name.is_empty() || raw_name.contains('/') {
                continue;
            }
            let _ = register_seed_seat(
                house,
                raw_name,
                false,
                &canonical_fold,
                &mut skipped_targets,
                &mut machine_checked_seats,
                &mut seat_aliases,
            );
        }
    }

    // 7. Read optional external `seats.tsv` (`session_name\tseat\troom_path`).
    //    When the caller explicitly supplies a path, surface `NotFound` rather
    //    than silently dropping a mistyped `--seats-tsv` flag.
    if let Some(seats_path) = seats_tsv {
        let Some(raw) = read_regular_text_if_present(seats_path)? else {
            return Err(ApiError::NotFound {
                what: format!("seats tsv {}", seats_path.display()),
            });
        };
        sources_read.push(seats_path.to_path_buf());
        for line in raw.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let mut cols = line.split('\t');
            let Some(raw_session) = cols.next().map(str::trim) else {
                continue;
            };
            let Some(raw_seat) = cols.next().map(str::trim).filter(|s| !s.is_empty()) else {
                continue;
            };
            if raw_session.eq_ignore_ascii_case("session_name")
                || raw_seat.eq_ignore_ascii_case("seat")
            {
                continue;
            }
            let Some((canon, set)) = register_seed_seat(
                house,
                raw_seat,
                true,
                &canonical_fold,
                &mut skipped_targets,
                &mut machine_checked_seats,
                &mut seat_aliases,
            ) else {
                continue;
            };
            let session_lower = raw_session.to_ascii_lowercase();
            let has_noise = session_lower.contains("fork")
                || session_lower.contains("attachment")
                || raw_session.contains('⑂');
            if !has_noise
                && let Some(alias_slug) = parse_seat_slug(raw_session)
                && alias_slug != canon
                && !house.is_operator_seat(alias_slug.as_str())
                && !is_non_seat_target(alias_slug.as_str())
            {
                set.insert(alias_slug);
            }
        }
    }

    // 8. Merge the house profile's canonical alias folds.
    for (canon, alias_list) in &house.aliases {
        let set = seat_aliases.entry(canon.clone()).or_default();
        for alias_slug in alias_list {
            if alias_slug != canon {
                set.insert(alias_slug.clone());
            }
        }
    }

    // 9. Remove any alias that collides with a canonical seat or an earlier seat's alias.
    let canonical_seats: BTreeSet<SeatName> = seat_aliases.keys().cloned().collect();
    let mut claimed_aliases: BTreeSet<SeatName> = BTreeSet::new();
    let witness = &house.witness_seat;
    let mut claims = Vec::with_capacity(seat_aliases.len());

    for (seat, raw_aliases) in seat_aliases {
        if house.is_operator_seat(seat.as_str()) || is_non_seat_target(seat.as_str()) {
            skipped_targets.insert(seat.as_str().to_owned());
            continue;
        }
        let mut final_aliases = Vec::new();
        for alias in raw_aliases {
            if alias == seat
                || canonical_seats.contains(&alias)
                || house.is_operator_seat(alias.as_str())
                || is_non_seat_target(alias.as_str())
            {
                continue;
            }
            if claimed_aliases.insert(alias.clone()) {
                final_aliases.push(alias);
            }
        }
        let verified_how = if machine_checked_seats.contains(&seat) {
            Some(VerificationKind::MachineChecked)
        } else {
            Some(VerificationKind::WitnessedBy {
                seat: witness.clone(),
            })
        };
        claims.push(PresenceClaim {
            seat,
            status: Some(SeatStatus::Active),
            role: None,
            projects: Vec::new(),
            aliases: final_aliases,
            home_room: None,
            mail_horizon_at: None,
            work_class: None,
            avatar: None,
            home_dir: None,
            start_cmd: None,
            resume_cmd: None,
            rex_target: None,
            verified_how,
            replace: None,
            idem: None,
        });
    }

    Ok(SeedInterchangePlan {
        claims,
        sources_read,
        skipped_targets: skipped_targets.into_iter().collect(),
    })
}

/// Tracks which seat incarnation authored each letter id so replies carrying
/// `in_reply_to` ring the originating session when multiple lanes are live.
#[derive(Clone, Debug, Default)]
pub struct IncarnationAffinity {
    by_letter_id: BTreeMap<String, (SeatName, IncarnationId)>,
}

impl IncarnationAffinity {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_authored(
        &mut self,
        letter_id: impl Into<String>,
        seat: SeatName,
        incarnation_id: IncarnationId,
    ) {
        let key = letter_id.into();
        let trimmed = key.trim();
        if !trimmed.is_empty() {
            self.by_letter_id
                .insert(trimmed.to_owned(), (seat, incarnation_id));
        }
    }

    pub fn lookup(&self, in_reply_to: &str) -> Option<(&SeatName, &IncarnationId)> {
        let trimmed = in_reply_to.trim();
        self.by_letter_id
            .get(trimmed)
            .map(|(seat, inc)| (seat, inc))
    }
}

/// Synchronous sender-first delivery verdict returned when banking or posting to a seat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryStatus {
    Live {
        binding: SeatBinding,
    },
    Dormant {
        seat: SeatName,
        notice: String,
    },
    Retired {
        seat: SeatName,
        notice: String,
    },
    Offline {
        seat: SeatName,
        since: Option<UnixMs>,
        notice: String,
    },
}

/// Evaluates whether a seat can be rung now or what synchronous notice the
/// sender should receive immediately after banking.
pub fn format_delivery_status(
    state: &PresenceState,
    seat: &SeatName,
    now: UnixMs,
) -> DeliveryStatus {
    if let Some(binding) = select_binding_for_delivery(
        state,
        seat,
        None,
        None,
        &IncarnationAffinity::default(),
        now,
    ) {
        return DeliveryStatus::Live {
            binding: binding.clone(),
        };
    }

    let status = state
        .seat(seat)
        .map_or(SeatStatus::Active, |record| record.status);
    match status {
        SeatStatus::Retired => DeliveryStatus::Retired {
            seat: seat.clone(),
            notice: format!("banked; {seat} is retired; no ring scheduled"),
        },
        SeatStatus::Dormant => DeliveryStatus::Dormant {
            seat: seat.clone(),
            notice: format!("banked; no live binding for {seat} (dormant); will ring on rebind"),
        },
        SeatStatus::Active => {
            let since = state.last_live_at(seat);
            let since_str = match since {
                Some(ts) => crate::unix_secs_to_iso8601_utc(ts.0 / 1_000),
                None => "never".to_owned(),
            };
            DeliveryStatus::Offline {
                seat: seat.clone(),
                since,
                notice: format!(
                    "banked; no live binding for {seat} since {since_str}; will ring on rebind"
                ),
            }
        }
    }
}

/// Returns `Some(notice)` for the sender when `seat` cannot be rung immediately,
/// or `None` when a live binding is reachable.
pub fn sender_delivery_notice(
    state: &PresenceState,
    seat: &SeatName,
    now: UnixMs,
) -> Option<String> {
    match format_delivery_status(state, seat, now) {
        DeliveryStatus::Live { .. } => None,
        DeliveryStatus::Dormant { notice, .. }
        | DeliveryStatus::Retired { notice, .. }
        | DeliveryStatus::Offline { notice, .. } => Some(notice),
    }
}

/// Chooses the target live binding for a delivery to `seat`, honoring
/// `in_reply_to` incarnation affinity first, then explicit `lane`, then the
/// default lane, then the highest binding epoch.
pub fn select_binding_for_delivery<'a>(
    state: &'a PresenceState,
    seat: &SeatName,
    lane: Option<&str>,
    in_reply_to: Option<&str>,
    affinity: &IncarnationAffinity,
    now: UnixMs,
) -> Option<&'a SeatBinding> {
    let live = state.live_bindings_for(seat, None, now);
    if live.is_empty() {
        return None;
    }

    if let Some(parent_id) = in_reply_to
        && let Some((aff_seat, aff_inc)) = affinity.lookup(parent_id)
        && aff_seat == seat
        && let Some(found) = live.iter().find(|b| &b.incarnation_id == aff_inc)
    {
        return Some(*found);
    }

    if let Some(want_lane) = lane {
        if let Some(found) = live
            .iter()
            .filter(|b| b.lane.as_deref() == Some(want_lane))
            .max_by_key(|b| (b.epoch, b.bound_at))
        {
            return Some(*found);
        }
        return None;
    }

    live.iter()
        .filter(|b| b.lane.is_none())
        .max_by_key(|b| (b.epoch, b.bound_at))
        .copied()
        .or_else(|| live.iter().max_by_key(|b| (b.epoch, b.bound_at)).copied())
}

fn process_identity(endpoint: &TransportEndpoint) -> Option<(u32, &str)> {
    match endpoint {
        TransportEndpoint::ClaudeSession {
            pid, proc_start, ..
        } => Some((*pid, proc_start.as_str())),
        TransportEndpoint::SelfWatcher {
            pid: Some(pid),
            proc_start: Some(proc_start),
            ..
        } => Some((*pid, proc_start.as_str())),
        _ => None,
    }
}

fn same_process_identity(a: &TransportEndpoint, b: &TransportEndpoint) -> bool {
    match (process_identity(a), process_identity(b)) {
        (Some((pa, sa)), Some((pb, sb))) => pa == pb && proc_start_matches(sa, sb),
        _ => false,
    }
}

fn is_reconnectable_binding(binding: &SeatBinding) -> bool {
    matches!(binding.liveness, LivenessMode::SocketLifetime)
        || matches!(
            binding.endpoint,
            TransportEndpoint::UdsSession { .. }
                | TransportEndpoint::BbThread { .. }
                | TransportEndpoint::T3Thread { .. }
        )
}

fn default_verification(by: &Principal, target_seat: &SeatName) -> VerificationKind {
    match by {
        Principal::System { .. } => VerificationKind::MachineChecked,
        Principal::Seat { seat } | Principal::Operator { seat } if seat == target_seat => {
            VerificationKind::SelfWord
        }
        Principal::Seat { seat } | Principal::Operator { seat } => {
            VerificationKind::WitnessedBy { seat: seat.clone() }
        }
        Principal::Worker { .. } => VerificationKind::SelfWord,
    }
}

fn make_provenance(
    by: &Principal,
    target_seat: &SeatName,
    explicit: Option<VerificationKind>,
    at: UnixMs,
) -> Provenance {
    Provenance {
        asserted_by: by.clone(),
        verified_how: explicit.unwrap_or_else(|| default_verification(by, target_seat)),
        at,
    }
}

fn can_manage_seat(by: &Principal, seat: &SeatName, _existing: Option<&SeatRecord>) -> bool {
    match by {
        Principal::Operator { .. } | Principal::System { .. } => true,
        Principal::Seat { seat: caller } => caller == seat,
        Principal::Worker { .. } => false,
    }
}

fn validate_endpoint(endpoint: &TransportEndpoint, allow_fork: bool) -> Result<(), ApiError> {
    match endpoint {
        TransportEndpoint::ClaudeSession {
            pid,
            proc_start,
            session_id,
            messaging_socket,
            kind,
            display_name,
            ..
        } => {
            if *pid == 0 {
                return Err(invalid("claude_session.pid", "pid must be positive"));
            }
            if proc_start.trim().is_empty() {
                return Err(invalid(
                    "claude_session.proc_start",
                    "proc_start must not be empty",
                ));
            }
            if session_id.trim().is_empty() {
                return Err(invalid(
                    "claude_session.session_id",
                    "session_id must not be empty",
                ));
            }
            if messaging_socket.as_os_str().is_empty() {
                return Err(invalid(
                    "claude_session.messaging_socket",
                    "messaging_socket must not be empty",
                ));
            }
            if !TransportEndpoint::is_allowed_claude_kind(kind) {
                return Err(invalid(
                    "claude_session.kind",
                    format!(
                        "session kind '{kind}' is not allowed (only 'interactive' and 'bg' may bind)"
                    ),
                ));
            }
            if !allow_fork
                && display_name
                    .as_deref()
                    .is_some_and(TransportEndpoint::is_fork_display_name)
            {
                return Err(invalid(
                    "claude_session.display_name",
                    "fork session ('⑂') is refused unless allow_fork is explicitly set",
                ));
            }
            Ok(())
        }
        TransportEndpoint::UdsSession { client, .. } => {
            if client.trim().is_empty() {
                return Err(invalid(
                    "uds_session.client",
                    "client name must not be empty",
                ));
            }
            Ok(())
        }
        TransportEndpoint::SelfWatcher { harness, .. } => {
            if harness.trim().is_empty() {
                return Err(invalid(
                    "self_watcher.harness",
                    "harness name must not be empty",
                ));
            }
            Ok(())
        }
        TransportEndpoint::OperatorToast { command } => {
            if command.trim().is_empty() {
                return Err(invalid(
                    "operator_toast.command",
                    "command must not be empty",
                ));
            }
            Ok(())
        }
        TransportEndpoint::BbThread { .. } | TransportEndpoint::T3Thread { .. } => Ok(()),
    }
}

fn normalize_lane(lane: &Option<String>) -> Result<Option<String>, ApiError> {
    let Some(raw) = lane else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > 64 || trimmed.chars().any(char::is_control) {
        return Err(invalid("lane", "must be 1..=64 printable characters"));
    }
    Ok(Some(trimmed.to_owned()))
}

fn validate_resident_field(what: &str, raw: &Option<String>) -> Result<Option<String>, ApiError> {
    let Some(val) = raw else {
        return Ok(None);
    };
    let trimmed = val.trim();
    if trimmed.is_empty() || trimmed.len() > 1024 || trimmed.chars().any(char::is_control) {
        return Err(invalid(what, "must be 1..=1024 printable characters"));
    }
    Ok(Some(trimmed.to_owned()))
}

fn binding_is_live_at(binding: &SeatBinding, now: UnixMs) -> bool {
    binding.status == BindingStatus::Live && binding.expires_at.is_none_or(|exp| exp > now)
}

/// Materialized projection of `StreamId::Presence`.
#[derive(Clone, Debug, Default)]
pub struct PresenceState {
    /// Read-only deployment config consulted by the decider; `evolve` never
    /// touches it and it is not derived from the journal.
    house: HouseProfile,
    head: Seq,
    latest_at: UnixMs,
    seats: BTreeMap<SeatName, SeatRecord>,
    bindings: BTreeMap<SeatName, BTreeMap<IncarnationId, SeatBinding>>,
    max_epoch: BTreeMap<SeatName, u64>,
    last_live_at: BTreeMap<SeatName, UnixMs>,
    last_drop: BTreeMap<SeatName, (UnixMs, bool)>,
    ever_bound: BTreeSet<SeatName>,
    down_transports: BTreeSet<TransportKind>,
    down_transport_details: BTreeMap<TransportKind, Option<String>>,
}

impl PresenceState {
    /// Empty state carrying an explicit [`HouseProfile`] (the decider's only
    /// source of operator-surface identity).
    pub fn with_house_profile(house: HouseProfile) -> Self {
        Self {
            house,
            ..Self::default()
        }
    }

    pub fn house(&self) -> &HouseProfile {
        &self.house
    }

    pub fn head(&self) -> Seq {
        self.head
    }

    pub fn latest_at(&self) -> UnixMs {
        self.latest_at
    }

    pub fn seat(&self, seat: &SeatName) -> Option<&SeatRecord> {
        self.seats.get(seat)
    }

    pub fn seats(&self) -> &BTreeMap<SeatName, SeatRecord> {
        &self.seats
    }

    pub fn bindings_for(&self, seat: &SeatName) -> Vec<&SeatBinding> {
        self.bindings
            .get(seat)
            .map(|m| m.values().collect())
            .unwrap_or_default()
    }

    pub fn live_bindings_for(
        &self,
        seat: &SeatName,
        lane: Option<&str>,
        now: UnixMs,
    ) -> Vec<&SeatBinding> {
        let Some(map) = self.bindings.get(seat) else {
            return Vec::new();
        };
        let mut out: Vec<&SeatBinding> = map
            .values()
            .filter(|b| {
                binding_is_live_at(b, now)
                    && !self.down_transports.contains(&b.endpoint.kind())
                    && lane.is_none_or(|want| b.lane.as_deref() == Some(want))
            })
            .collect();
        out.sort_by_key(|b| (std::cmp::Reverse(b.epoch), b.incarnation_id.clone()));
        out
    }

    pub fn last_live_at(&self, seat: &SeatName) -> Option<UnixMs> {
        self.last_live_at.get(seat).copied()
    }

    pub fn last_drop_info(&self, seat: &SeatName) -> Option<(UnixMs, bool)> {
        self.last_drop.get(seat).copied()
    }

    pub fn was_ever_bound(&self, seat: &SeatName) -> bool {
        self.ever_bound.contains(seat)
    }

    pub fn is_transport_down(&self, kind: TransportKind) -> bool {
        self.down_transports.contains(&kind)
    }

    pub fn down_transports(&self) -> Vec<TransportKind> {
        self.down_transports.iter().copied().collect()
    }

    /// Resolves a canonical seat name or registered alias (case-insensitive) to
    /// its unique canonical [`SeatName`].
    pub fn canonical_seat_for_alias(&self, raw: &str) -> Option<SeatName> {
        let needle = raw.trim().trim_start_matches('@').to_ascii_lowercase();
        if needle.is_empty() {
            return None;
        }
        if let Ok(exact) = SeatName::parse(&needle)
            && self.seats.contains_key(&exact)
        {
            return Some(exact);
        }
        let mut matches = Vec::new();
        for (seat, rec) in &self.seats {
            if rec
                .aliases
                .iter()
                .any(|a| a.value.as_str().eq_ignore_ascii_case(&needle))
            {
                matches.push(seat.clone());
            }
        }
        if matches.len() == 1 {
            matches.into_iter().next()
        } else {
            None
        }
    }

    /// Scans individual bindings for lease expiry or dead process identity without
    /// touching healthy sibling bindings of the same transport kind.
    pub fn expired_bindings<F>(&self, now: UnixMs, is_proc_alive: F) -> Vec<PresenceExpire>
    where
        F: Fn(u32, &str) -> bool,
    {
        let mut out = Vec::new();
        for (seat, map) in &self.bindings {
            for b in map.values() {
                if b.status != BindingStatus::Live {
                    continue;
                }
                if let Some(exp) = b.expires_at
                    && exp <= now
                {
                    out.push(PresenceExpire {
                        seat: seat.clone(),
                        incarnation_id: b.incarnation_id.clone(),
                        reason: format!("lease expired at {}", exp.0),
                    });
                    continue;
                }
                if let Some((pid, proc_start)) = process_identity(&b.endpoint)
                    && !is_proc_alive(pid, proc_start)
                {
                    out.push(PresenceExpire {
                        seat: seat.clone(),
                        incarnation_id: b.incarnation_id.clone(),
                        reason: format!("process pid={pid} no longer matches proc_start"),
                    });
                }
            }
        }
        out
    }

    fn effective_reply_now(&self) -> UnixMs {
        if self.latest_at.0 >= REAL_EPOCH_FLOOR_MS {
            self.latest_at.max(rex_wave_runtime::now())
        } else {
            self.latest_at
        }
    }

    fn effective_binding_view(&self, binding: &SeatBinding, now: UnixMs) -> SeatBinding {
        let mut out = binding.clone();
        if out.status == BindingStatus::Live {
            if self.down_transports.contains(&out.endpoint.kind()) {
                out.status = BindingStatus::TransportDown;
            } else if out.expires_at.is_some_and(|exp| exp <= now) {
                out.status = BindingStatus::Expired;
            }
        }
        out
    }

    pub fn snapshot_at(&self, query: &PresenceList, now: UnixMs) -> PresenceSnapshot {
        let project_needle = query
            .project
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());

        let mut seats = Vec::new();
        for (seat, record) in &self.seats {
            if !query.include_retired && record.status == SeatStatus::Retired {
                continue;
            }
            if let Some(proj) = project_needle
                && !record
                    .projects
                    .iter()
                    .any(|p| p.value.eq_ignore_ascii_case(proj))
            {
                continue;
            }
            let bindings: Vec<SeatBinding> = self
                .bindings
                .get(seat)
                .map(|m| {
                    m.values()
                        .map(|b| self.effective_binding_view(b, now))
                        .filter(|b| !query.live_only || b.status == BindingStatus::Live)
                        .collect()
                })
                .unwrap_or_default();
            if query.live_only && bindings.is_empty() {
                continue;
            }
            seats.push(SeatPresenceView {
                record: record.clone(),
                bindings,
            });
        }

        PresenceSnapshot {
            head: self.head,
            seats,
            down_transports: self.down_transports(),
        }
    }

    /// Resolves a seat name, alias, project, or role to a durable [`SeatRecord`]
    /// and its live bindings. Ambiguous role/project/alias matches return
    /// `ApiError::Conflict` listing every matching candidate.
    pub fn resolve_at(
        &self,
        query: &PresenceResolve,
        now: UnixMs,
    ) -> Result<PresenceResolution, ApiError> {
        let q = query.query.trim().trim_start_matches('@');
        if q.is_empty() {
            return Err(invalid("query", "presence resolve query must not be empty"));
        }
        let lane = normalize_lane(&query.lane)?;

        // 1. Exact canonical seat name (works even when all bindings are offline).
        if let Ok(seat_name) = SeatName::parse(q.to_ascii_lowercase())
            && let Some(record) = self.seats.get(&seat_name)
        {
            return Ok(self.build_resolution(record, lane.as_deref(), now));
        }

        // 2. Registered alias match across durable records.
        let alias_matches: Vec<&SeatRecord> = self
            .seats
            .values()
            .filter(|rec| {
                rec.aliases
                    .iter()
                    .any(|a| a.value.as_str().eq_ignore_ascii_case(q))
            })
            .collect();
        if alias_matches.len() == 1 {
            return Ok(self.build_resolution(alias_matches[0], lane.as_deref(), now));
        }
        if alias_matches.len() > 1 {
            let names: Vec<&str> = alias_matches.iter().map(|r| r.seat.as_str()).collect();
            return Err(ApiError::conflict(format!(
                "ambiguous alias '{q}' matches multiple seats: {}",
                names.join(", ")
            )));
        }

        // 3. Project or role ownership match (non-retired first, then retired fallback).
        let matches_project_or_role = |rec: &SeatRecord| {
            rec.projects.iter().any(|p| p.value.eq_ignore_ascii_case(q))
                || rec
                    .role
                    .as_ref()
                    .is_some_and(|r| r.value.eq_ignore_ascii_case(q))
        };

        let active_or_dormant: Vec<&SeatRecord> = self
            .seats
            .values()
            .filter(|rec| rec.status != SeatStatus::Retired && matches_project_or_role(rec))
            .collect();
        if active_or_dormant.len() == 1 {
            return Ok(self.build_resolution(active_or_dormant[0], lane.as_deref(), now));
        }
        if active_or_dormant.len() > 1 {
            let names: Vec<&str> = active_or_dormant.iter().map(|r| r.seat.as_str()).collect();
            return Err(ApiError::conflict(format!(
                "ambiguous project/role query '{q}' matches multiple seats: {}",
                names.join(", ")
            )));
        }

        let retired_matches: Vec<&SeatRecord> = self
            .seats
            .values()
            .filter(|rec| rec.status == SeatStatus::Retired && matches_project_or_role(rec))
            .collect();
        if retired_matches.len() == 1 {
            return Ok(self.build_resolution(retired_matches[0], lane.as_deref(), now));
        }
        if retired_matches.len() > 1 {
            let names: Vec<&str> = retired_matches.iter().map(|r| r.seat.as_str()).collect();
            return Err(ApiError::conflict(format!(
                "ambiguous project/role query '{q}' matches multiple retired seats: {}",
                names.join(", ")
            )));
        }

        Err(ApiError::NotFound {
            what: format!("presence seat, alias, project, or role '{q}'"),
        })
    }

    fn build_resolution(
        &self,
        record: &SeatRecord,
        lane: Option<&str>,
        now: UnixMs,
    ) -> PresenceResolution {
        let live_bindings = self
            .live_bindings_for(&record.seat, lane, now)
            .into_iter()
            .cloned()
            .collect();
        PresenceResolution {
            record: record.clone(),
            live_bindings,
            last_live_at: self.last_live_at(&record.seat),
        }
    }

    fn alias_owner(&self, candidate: &SeatName, exclude_seat: &SeatName) -> Option<SeatName> {
        for (seat, rec) in &self.seats {
            if seat == exclude_seat {
                continue;
            }
            if seat == candidate || rec.aliases.iter().any(|a| &a.value == candidate) {
                return Some(seat.clone());
            }
        }
        None
    }

    fn next_epoch(&self, seat: &SeatName) -> u64 {
        self.max_epoch.get(seat).copied().unwrap_or(0) + 1
    }
}

/// Pure `StreamId::Presence` decider wired into `rex_wave_runtime::Registry<Presence>`.
///
/// `Decider` is static, so the [`HouseProfile`] held here reaches `decide`
/// through the seeded [`PresenceState`] ([`Presence::registry`]); `decide`
/// itself never reads disk.
#[derive(Clone, Debug, Default)]
pub struct Presence {
    house: HouseProfile,
}

impl Presence {
    /// A decider configured with [`HouseProfile::default`].
    pub fn new() -> Self {
        Self::default()
    }

    /// A decider configured with an explicit, already-loaded [`HouseProfile`].
    pub fn with_house_profile(house: HouseProfile) -> Self {
        Self { house }
    }

    pub fn house(&self) -> &HouseProfile {
        &self.house
    }

    /// The empty state every Presence stream actor folds its journal into.
    pub fn initial_state(&self) -> PresenceState {
        PresenceState::with_house_profile(self.house.clone())
    }

    /// A Presence registry whose actors (including rehydrated ones) are seeded
    /// with this decider's [`HouseProfile`].
    pub fn registry(
        &self,
        journal: rex_wave_store::Journal,
        hub: rex_wave_runtime::Hub,
    ) -> std::sync::Arc<rex_wave_runtime::Registry<Presence>> {
        rex_wave_runtime::Registry::<Presence>::with_initial_state(
            journal,
            hub,
            self.initial_state(),
        )
    }

    /// Policy evaluation with an injectable process liveness probe for
    /// deterministic unit tests as well as real `ps`-backed checks.
    pub fn decide_with_probe<F>(
        state: &PresenceState,
        cmd: &PresenceCommand,
        ctx: &DecideCtx,
        is_proc_alive: F,
    ) -> Result<Vec<PresenceEvent>, ApiError>
    where
        F: Fn(u32, &str) -> bool,
    {
        match cmd {
            PresenceCommand::PresenceClaim(c) => Self::decide_claim(state, c, ctx),
            PresenceCommand::PresenceSetStatus(c) => Self::decide_set_status(state, c, ctx),
            PresenceCommand::PresenceBind(c) => Self::decide_bind(state, c, ctx, is_proc_alive),
            PresenceCommand::PresenceHeartbeat(c) => Self::decide_heartbeat(state, c, ctx),
            PresenceCommand::PresenceUnbind(c) => Self::decide_unbind(state, c, ctx),
            PresenceCommand::PresenceExpire(c) => Self::decide_expire(state, c),
            PresenceCommand::PresenceTransportHealth(c) => Self::decide_transport_health(state, c),
            PresenceCommand::PresenceList(_) => Ok(vec![]),
            PresenceCommand::PresenceResolve(q) => {
                state.resolve_at(q, ctx.at)?;
                Ok(vec![])
            }
        }
    }

    fn decide_claim(
        state: &PresenceState,
        c: &PresenceClaim,
        ctx: &DecideCtx,
    ) -> Result<Vec<PresenceEvent>, ApiError> {
        let existing = state.seats.get(&c.seat);
        let replacing_self = c.replace.as_ref() == Some(&c.seat);
        let replacing_other = c.replace.as_ref().filter(|old| *old != &c.seat);

        let status = c
            .status
            .or_else(|| existing.map(|r| r.status))
            .unwrap_or(SeatStatus::Active);

        // Structural Q3 guard: the house's operator surface seat is the human
        // operator and may never become an `Active` seat or be claimed as
        // another seat's alias.
        let house = &state.house;
        if house.is_operator_seat(c.seat.as_str()) && status == SeatStatus::Active {
            return Err(ApiError::Denied {
                principal: ctx.by.to_string(),
                action: format!(
                    "claim operator surface seat '{}' as active (the operator surface cannot become an active seat)",
                    c.seat
                ),
            });
        }
        if c.aliases
            .iter()
            .any(|alias| house.is_operator_seat(alias.as_str()))
        {
            return Err(ApiError::Denied {
                principal: ctx.by.to_string(),
                action: format!(
                    "claim reserved operator surface '{}' as an alias of '{}'",
                    house.operator_seat, c.seat
                ),
            });
        }

        // `start_cmd` and `resume_cmd` are operator-only command fields: no
        // seat, worker, or system principal may set or mutate them.
        if !matches!(ctx.by, Principal::Operator { .. })
            && (c.start_cmd.is_some() || c.resume_cmd.is_some())
        {
            return Err(ApiError::Denied {
                principal: ctx.by.to_string(),
                action: format!(
                    "set start_cmd/resume_cmd on seat '{}' (operator-only)",
                    c.seat
                ),
            });
        }

        if let Some(old_seat) = replacing_other {
            let Some(old_rec) = state.seats.get(old_seat) else {
                return Err(ApiError::NotFound {
                    what: format!("seat '{old_seat}' to replace"),
                });
            };
            if !can_manage_seat(&ctx.by, old_seat, Some(old_rec)) {
                return Err(ApiError::Denied {
                    principal: ctx.by.to_string(),
                    action: format!("replace seat '{old_seat}'"),
                });
            }
        }

        if let Some(rec) = existing {
            if !can_manage_seat(&ctx.by, &c.seat, Some(rec)) {
                if replacing_self {
                    return Err(ApiError::Denied {
                        principal: ctx.by.to_string(),
                        action: format!("replace seat '{}'", c.seat),
                    });
                }
                return Err(ApiError::conflict(format!(
                    "seat '{}' is already claimed by {}; pass replace='{}' as an authorized operator to replace explicitly",
                    c.seat, rec.claimed_by, c.seat
                )));
            }
            if replacing_self
                && !matches!(ctx.by, Principal::Operator { .. })
                && (rec.start_cmd.is_some() || rec.resume_cmd.is_some())
            {
                return Err(ApiError::Denied {
                    principal: ctx.by.to_string(),
                    action: format!(
                        "clear start_cmd/resume_cmd on seat '{}' via replace (operator-only)",
                        c.seat
                    ),
                });
            }
        } else {
            if !can_manage_seat(&ctx.by, &c.seat, None) {
                return Err(ApiError::Denied {
                    principal: ctx.by.to_string(),
                    action: format!("claim seat '{}'", c.seat),
                });
            }
            if let Some(owner) = state.alias_owner(&c.seat, &c.seat)
                && c.replace.as_ref() != Some(&owner)
            {
                return Err(ApiError::conflict(format!(
                    "seat name '{}' is already an alias of '{}'; pass replace='{owner}' to replace explicitly",
                    c.seat, owner
                )));
            }
        }

        for alias in &c.aliases {
            if alias == &c.seat {
                continue;
            }
            if let Some(owner) = state.alias_owner(alias, &c.seat)
                && c.replace.as_ref() != Some(&owner)
            {
                return Err(ApiError::conflict(format!(
                    "alias '{alias}' already belongs to seat '{owner}'"
                )));
            }
        }

        let prov = make_provenance(&ctx.by, &c.seat, c.verified_how.clone(), ctx.at);
        let mut events = Vec::new();

        if let Some(old_seat) = replacing_other
            && let Some(old_rec) = state.seats.get(old_seat)
            && old_rec.status != SeatStatus::Retired
        {
            events.push(PresenceEvent::SeatStatusSet {
                seat: old_seat.clone(),
                status: SeatStatus::Retired,
                provenance: prov.clone(),
                reason: Some(format!("replaced by {}", c.seat)),
            });
            if let Some(old_bindings) = state.bindings.get(old_seat) {
                for b in old_bindings.values() {
                    if matches!(b.status, BindingStatus::Live | BindingStatus::TransportDown) {
                        events.push(PresenceEvent::Unbound {
                            seat: old_seat.clone(),
                            incarnation_id: b.incarnation_id.clone(),
                            status: BindingStatus::Unbound,
                            reason: Some(format!("seat replaced by {}", c.seat)),
                        });
                    }
                }
            }
        }

        let status_provenance = if c.status.is_some() || existing.is_none() || replacing_self {
            Some(prov.clone())
        } else {
            existing.and_then(|r| r.status_provenance.clone())
        };

        let aliases = if !c.aliases.is_empty() || existing.is_none() || replacing_self {
            let mut dedup = BTreeSet::new();
            for a in &c.aliases {
                if a != &c.seat {
                    dedup.insert(a.clone());
                }
            }
            dedup
                .into_iter()
                .map(|value| Attested {
                    value,
                    provenance: prov.clone(),
                })
                .collect()
        } else {
            existing.map(|r| r.aliases.clone()).unwrap_or_default()
        };

        let role = match &c.role {
            Some(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    return Err(invalid("role", "role must not be blank"));
                }
                Some(Attested {
                    value: trimmed.to_owned(),
                    provenance: prov.clone(),
                })
            }
            None if replacing_self => None,
            None => existing.and_then(|r| r.role.clone()),
        };

        let projects = if !c.projects.is_empty() || existing.is_none() || replacing_self {
            let mut dedup = BTreeSet::new();
            for p in &c.projects {
                let trimmed = p.trim();
                if trimmed.is_empty() || trimmed.chars().any(char::is_control) {
                    return Err(invalid(
                        "projects",
                        "project names must be non-empty printable text",
                    ));
                }
                dedup.insert(trimmed.to_owned());
            }
            dedup
                .into_iter()
                .map(|value| Attested {
                    value,
                    provenance: prov.clone(),
                })
                .collect()
        } else {
            existing.map(|r| r.projects.clone()).unwrap_or_default()
        };

        let home_room = match &c.home_room {
            Some(room) => Some(Attested {
                value: room.clone(),
                provenance: prov.clone(),
            }),
            None if replacing_self => None,
            None => existing.and_then(|r| r.home_room.clone()),
        };

        let retired_at = if status == SeatStatus::Retired {
            existing.and_then(|r| r.retired_at).or(Some(ctx.at))
        } else {
            None
        };

        let mail_horizon_at = match c.mail_horizon_at {
            Some(h) => Some(h),
            None if replacing_self => None,
            None => existing.and_then(|r| r.mail_horizon_at),
        };
        let work_class = match c.work_class {
            Some(wc) => Some(wc),
            None if replacing_self => None,
            None => existing.and_then(|r| r.work_class),
        };
        let avatar = match validate_resident_field("avatar", &c.avatar)? {
            Some(v) => Some(v),
            None if replacing_self => None,
            None => existing.and_then(|r| r.avatar.clone()),
        };
        let home_dir = match validate_resident_field("home_dir", &c.home_dir)? {
            Some(v) => Some(v),
            None if replacing_self => None,
            None => existing.and_then(|r| r.home_dir.clone()),
        };
        let start_cmd = match validate_resident_field("start_cmd", &c.start_cmd)? {
            Some(v) => Some(v),
            None if replacing_self => None,
            None => existing.and_then(|r| r.start_cmd.clone()),
        };
        let resume_cmd = match validate_resident_field("resume_cmd", &c.resume_cmd)? {
            Some(v) => Some(v),
            None if replacing_self => None,
            None => existing.and_then(|r| r.resume_cmd.clone()),
        };
        let rex_target = match validate_resident_field("rex_target", &c.rex_target)? {
            Some(v) => Some(v),
            None if replacing_self => None,
            None => existing.and_then(|r| r.rex_target.clone()),
        };

        let can_reassign_owner = matches!(
            ctx.by,
            Principal::Operator { .. } | Principal::System { .. }
        );
        let (claimed_by, claimed_at) = match existing {
            Some(rec) if !(replacing_self && can_reassign_owner) => {
                (rec.claimed_by.clone(), rec.claimed_at)
            }
            _ => (ctx.by.clone(), ctx.at),
        };

        let record = SeatRecord {
            seat: c.seat.clone(),
            status,
            status_provenance,
            aliases,
            role,
            projects,
            home_room,
            retired_at,
            mail_horizon_at,
            work_class,
            avatar,
            home_dir,
            start_cmd,
            resume_cmd,
            rex_target,
            claimed_by,
            claimed_at,
        };
        events.push(PresenceEvent::SeatClaimed { record });
        Ok(events)
    }

    fn decide_set_status(
        state: &PresenceState,
        c: &PresenceSetStatus,
        ctx: &DecideCtx,
    ) -> Result<Vec<PresenceEvent>, ApiError> {
        if state.house.is_operator_seat(c.seat.as_str()) && c.status == SeatStatus::Active {
            return Err(ApiError::Denied {
                principal: ctx.by.to_string(),
                action: format!(
                    "set operator surface seat '{}' to active (the operator surface cannot become an active seat)",
                    c.seat
                ),
            });
        }
        let Some(rec) = state.seats.get(&c.seat) else {
            return Err(ApiError::NotFound {
                what: format!("seat '{}'", c.seat),
            });
        };
        if !can_manage_seat(&ctx.by, &c.seat, Some(rec)) {
            return Err(ApiError::Denied {
                principal: ctx.by.to_string(),
                action: format!("set status for seat '{}'", c.seat),
            });
        }
        let provenance = make_provenance(&ctx.by, &c.seat, c.verified_how.clone(), ctx.at);
        let mut events = vec![PresenceEvent::SeatStatusSet {
            seat: c.seat.clone(),
            status: c.status,
            provenance,
            reason: c.reason.clone(),
        }];
        if c.status == SeatStatus::Retired
            && let Some(bindings) = state.bindings.get(&c.seat)
        {
            for b in bindings.values() {
                if matches!(b.status, BindingStatus::Live | BindingStatus::TransportDown) {
                    events.push(PresenceEvent::Unbound {
                        seat: c.seat.clone(),
                        incarnation_id: b.incarnation_id.clone(),
                        status: BindingStatus::Unbound,
                        reason: Some("seat retired".into()),
                    });
                }
            }
        }
        Ok(events)
    }

    fn decide_bind<F>(
        state: &PresenceState,
        c: &PresenceBind,
        ctx: &DecideCtx,
        is_proc_alive: F,
    ) -> Result<Vec<PresenceEvent>, ApiError>
    where
        F: Fn(u32, &str) -> bool,
    {
        // Structural Q3 guard: the house's operator surface seat may never be
        // bound as an active seat by any hook or principal.
        if state.house.is_operator_seat(c.seat.as_str()) {
            return Err(ApiError::Denied {
                principal: ctx.by.to_string(),
                action: format!(
                    "bind operator surface seat '{}' (the operator surface cannot be bound as an active seat)",
                    c.seat
                ),
            });
        }
        validate_endpoint(&c.endpoint, c.allow_fork)?;
        let lane = normalize_lane(&c.lane)?;
        if c.ttl_ms == Some(0) {
            return Err(invalid("ttl_ms", "ttl_ms must be positive"));
        }

        let existing_seat = state.seats.get(&c.seat);
        if let Some(rec) = existing_seat {
            if !can_manage_seat(&ctx.by, &c.seat, Some(rec)) {
                return Err(ApiError::Denied {
                    principal: ctx.by.to_string(),
                    action: format!("bind seat '{}'", c.seat),
                });
            }
            if rec.status == SeatStatus::Retired {
                return Err(ApiError::conflict(format!(
                    "seat '{}' is retired; transition status to active or replace explicitly before binding",
                    c.seat
                )));
            }
        } else {
            if !can_manage_seat(&ctx.by, &c.seat, None) {
                return Err(ApiError::Denied {
                    principal: ctx.by.to_string(),
                    action: format!("bind seat '{}'", c.seat),
                });
            }
            if let Some(owner) = state.alias_owner(&c.seat, &c.seat) {
                return Err(ApiError::conflict(format!(
                    "seat name '{}' is already an alias of '{}'; replace explicitly before binding",
                    c.seat, owner
                )));
            }
        }

        let expires_at = match c.liveness {
            LivenessMode::SocketLifetime => {
                c.ttl_ms.map(|ttl| UnixMs(ctx.at.0.saturating_add(ttl)))
            }
            LivenessMode::ProcessAndLease | LivenessMode::LeaseOnly => {
                let ttl = c.ttl_ms.unwrap_or(DEFAULT_LEASE_TTL_MS);
                Some(UnixMs(ctx.at.0.saturating_add(ttl)))
            }
        };

        let mut superseded: Vec<IncarnationId> = Vec::new();
        let mut refresh_target: Option<(IncarnationId, u64)> = None;

        if let Some(existing_bindings) = state.bindings.get(&c.seat) {
            for b in existing_bindings.values() {
                if !matches!(b.status, BindingStatus::Live | BindingStatus::TransportDown) {
                    continue;
                }
                if b.lane != lane {
                    continue;
                }

                // Lease already elapsed -> dead, auto-superseded.
                if b.expires_at.is_some_and(|exp| exp <= ctx.at) {
                    superseded.push(b.incarnation_id.clone());
                    continue;
                }

                // Same live process (e.g. `/clear` rotating `sessionId` under same `(pid, procStart)`)
                // or explicit refresh of the same incarnation_id -> refresh freely without takeover.
                let same_proc = same_process_identity(&b.endpoint, &c.endpoint);
                let same_incarnation = c
                    .incarnation_id
                    .as_ref()
                    .is_some_and(|id| id == &b.incarnation_id);
                if same_proc || same_incarnation {
                    if c.incarnation_id.is_none() || same_incarnation {
                        refresh_target = Some((b.incarnation_id.clone(), b.epoch));
                    } else {
                        superseded.push(b.incarnation_id.clone());
                    }
                    continue;
                }

                // Different process: if the old `(pid, proc_start)` is dead (e.g. `--resume`),
                // automatically supersede it without requiring takeover.
                if let Some((old_pid, old_proc_start)) = process_identity(&b.endpoint)
                    && !is_proc_alive(old_pid, old_proc_start)
                {
                    superseded.push(b.incarnation_id.clone());
                    continue;
                }

                // Different live process / active socket on the same (seat, lane).
                if c.takeover {
                    superseded.push(b.incarnation_id.clone());
                } else {
                    return Err(ApiError::conflict_with(
                        format!(
                            "seat '{}' already has live binding '{}' on lane {}; pass takeover=true or bind a distinct lane",
                            c.seat,
                            b.incarnation_id,
                            lane.as_deref().unwrap_or("default")
                        ),
                        ConflictData::DuplicateLiveSession {
                            seat: c.seat.clone(),
                            active: b.incarnation_id.clone(),
                        },
                    ));
                }
            }
        }

        let (incarnation_id, epoch) = if let Some((existing_id, existing_epoch)) = refresh_target {
            (existing_id, existing_epoch)
        } else {
            let next_epoch = state.next_epoch(&c.seat);
            let id = match &c.incarnation_id {
                Some(explicit) => explicit.clone(),
                None => IncarnationId::parse(format!("inc-{}-{next_epoch}", c.seat))?,
            };
            (id, next_epoch)
        };

        let status = if state.down_transports.contains(&c.endpoint.kind()) {
            BindingStatus::TransportDown
        } else {
            BindingStatus::Live
        };

        let effective_horizon = c
            .mail_horizon_at
            .or_else(|| existing_seat.and_then(|r| r.mail_horizon_at))
            .or(Some(ctx.at));

        let mut events = Vec::new();
        if let Some(rec) = existing_seat {
            if rec.mail_horizon_at != effective_horizon {
                let mut updated = rec.clone();
                updated.mail_horizon_at = effective_horizon;
                events.push(PresenceEvent::SeatClaimed { record: updated });
            }
        } else {
            let prov = make_provenance(&ctx.by, &c.seat, None, ctx.at);
            events.push(PresenceEvent::SeatClaimed {
                record: SeatRecord {
                    seat: c.seat.clone(),
                    status: SeatStatus::Active,
                    status_provenance: Some(prov),
                    aliases: Vec::new(),
                    role: None,
                    projects: Vec::new(),
                    home_room: None,
                    retired_at: None,
                    mail_horizon_at: effective_horizon,
                    work_class: None,
                    avatar: None,
                    home_dir: None,
                    start_cmd: None,
                    resume_cmd: None,
                    rex_target: None,
                    claimed_by: ctx.by.clone(),
                    claimed_at: ctx.at,
                },
            });
        }

        let binding = SeatBinding {
            seat: c.seat.clone(),
            incarnation_id,
            epoch,
            lane,
            endpoint: c.endpoint.clone(),
            liveness: c.liveness,
            bound_at: ctx.at,
            expires_at,
            status,
        };
        events.push(PresenceEvent::Bound {
            binding,
            superseded,
        });
        Ok(events)
    }

    fn decide_heartbeat(
        state: &PresenceState,
        c: &PresenceHeartbeat,
        ctx: &DecideCtx,
    ) -> Result<Vec<PresenceEvent>, ApiError> {
        if c.ttl_ms == Some(0) {
            return Err(invalid("ttl_ms", "ttl_ms must be positive"));
        }
        let existing_seat = state.seats.get(&c.seat);
        if !can_manage_seat(&ctx.by, &c.seat, existing_seat) {
            return Err(ApiError::Denied {
                principal: ctx.by.to_string(),
                action: format!("heartbeat seat '{}'", c.seat),
            });
        }
        let Some(binding) = state
            .bindings
            .get(&c.seat)
            .and_then(|m| m.get(&c.incarnation_id))
        else {
            return Err(ApiError::NotFound {
                what: format!("binding '{}/{}'", c.seat, c.incarnation_id),
            });
        };
        if !matches!(
            binding.status,
            BindingStatus::Live | BindingStatus::TransportDown
        ) {
            return Err(ApiError::conflict(format!(
                "binding '{}/{}' is {:?} and cannot be heartbeated",
                c.seat, c.incarnation_id, binding.status
            )));
        }
        if binding.expires_at.is_some_and(|exp| exp <= ctx.at) {
            return Err(ApiError::conflict(format!(
                "binding '{}/{}' lease already expired; rebind",
                c.seat, c.incarnation_id
            )));
        }
        let expires_at = match binding.liveness {
            LivenessMode::SocketLifetime => c
                .ttl_ms
                .map(|ttl| UnixMs(ctx.at.0.saturating_add(ttl)))
                .or(binding.expires_at),
            LivenessMode::ProcessAndLease | LivenessMode::LeaseOnly => {
                let ttl = c.ttl_ms.unwrap_or(DEFAULT_LEASE_TTL_MS);
                Some(UnixMs(ctx.at.0.saturating_add(ttl)))
            }
        };
        Ok(vec![PresenceEvent::Heartbeated {
            seat: c.seat.clone(),
            incarnation_id: c.incarnation_id.clone(),
            expires_at,
        }])
    }

    fn decide_unbind(
        state: &PresenceState,
        c: &PresenceUnbind,
        ctx: &DecideCtx,
    ) -> Result<Vec<PresenceEvent>, ApiError> {
        let existing_seat = state.seats.get(&c.seat);
        if !can_manage_seat(&ctx.by, &c.seat, existing_seat) {
            return Err(ApiError::Denied {
                principal: ctx.by.to_string(),
                action: format!("unbind seat '{}'", c.seat),
            });
        }
        let Some(binding) = state
            .bindings
            .get(&c.seat)
            .and_then(|m| m.get(&c.incarnation_id))
        else {
            return Ok(vec![]);
        };
        if matches!(
            binding.status,
            BindingStatus::Unbound | BindingStatus::Expired
        ) {
            return Ok(vec![]);
        }
        Ok(vec![PresenceEvent::Unbound {
            seat: c.seat.clone(),
            incarnation_id: c.incarnation_id.clone(),
            status: BindingStatus::Unbound,
            reason: c.reason.clone(),
        }])
    }

    fn decide_expire(
        state: &PresenceState,
        c: &PresenceExpire,
    ) -> Result<Vec<PresenceEvent>, ApiError> {
        let Some(binding) = state
            .bindings
            .get(&c.seat)
            .and_then(|m| m.get(&c.incarnation_id))
        else {
            return Ok(vec![]);
        };
        if matches!(
            binding.status,
            BindingStatus::Unbound | BindingStatus::Expired
        ) {
            return Ok(vec![]);
        }
        Ok(vec![PresenceEvent::Unbound {
            seat: c.seat.clone(),
            incarnation_id: c.incarnation_id.clone(),
            status: BindingStatus::Expired,
            reason: Some(c.reason.clone()),
        }])
    }

    fn decide_transport_health(
        state: &PresenceState,
        c: &PresenceTransportHealth,
    ) -> Result<Vec<PresenceEvent>, ApiError> {
        let was_down = state.down_transports.contains(&c.transport);
        let prev_detail = state
            .down_transport_details
            .get(&c.transport)
            .and_then(|d| d.as_deref());
        if was_down == c.down && (!c.down || prev_detail == c.detail.as_deref()) {
            return Ok(vec![]);
        }
        Ok(vec![PresenceEvent::TransportHealthChanged {
            transport: c.transport,
            down: c.down,
            detail: c.detail.clone(),
        }])
    }
}

impl Decider for Presence {
    type State = PresenceState;
    type Cmd = PresenceCommand;
    type Event = PresenceEvent;

    fn address(_cmd: &PresenceCommand, _ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        Ok(StreamId::Presence)
    }

    fn idem(cmd: &PresenceCommand) -> Option<IdemKey> {
        match cmd {
            PresenceCommand::PresenceClaim(c) => c.idem.clone(),
            PresenceCommand::PresenceBind(c) => c.idem.clone(),
            _ => None,
        }
    }

    fn decide(
        state: &PresenceState,
        cmd: &PresenceCommand,
        ctx: &DecideCtx,
    ) -> Result<Vec<PresenceEvent>, ApiError> {
        Self::decide_with_probe(state, cmd, ctx, probe_process_alive)
    }

    fn evolve(state: &mut PresenceState, event: &PresenceEvent, meta: &EntryMeta) {
        state.head = meta.seq;
        if meta.at > state.latest_at {
            state.latest_at = meta.at;
        }
        match event {
            PresenceEvent::SeatClaimed { record } => {
                state.seats.insert(record.seat.clone(), record.clone());
            }
            PresenceEvent::SeatStatusSet {
                seat,
                status,
                provenance,
                ..
            } => {
                if let Some(rec) = state.seats.get_mut(seat) {
                    rec.status = *status;
                    rec.status_provenance = Some(provenance.clone());
                    rec.retired_at = if *status == SeatStatus::Retired {
                        Some(meta.at)
                    } else {
                        None
                    };
                }
            }
            PresenceEvent::Bound {
                binding,
                superseded,
            } => {
                let seat = binding.seat.clone();
                let entry = state.bindings.entry(seat.clone()).or_default();
                for old_id in superseded {
                    if let Some(old) = entry.get_mut(old_id) {
                        old.status = BindingStatus::Unbound;
                    }
                }
                let prev_max = state.max_epoch.get(&seat).copied().unwrap_or(0);
                state
                    .max_epoch
                    .insert(seat.clone(), prev_max.max(binding.epoch));
                entry.insert(binding.incarnation_id.clone(), binding.clone());
                state.ever_bound.insert(seat.clone());
                if binding.status == BindingStatus::Live {
                    state.last_live_at.insert(seat.clone(), meta.at);
                    state.last_drop.remove(&seat);
                }
            }
            PresenceEvent::Heartbeated {
                seat,
                incarnation_id,
                expires_at,
            } => {
                if let Some(binding) = state
                    .bindings
                    .get_mut(seat)
                    .and_then(|m| m.get_mut(incarnation_id))
                {
                    binding.expires_at = *expires_at;
                    if binding.status == BindingStatus::Live {
                        state.last_live_at.insert(seat.clone(), meta.at);
                    }
                }
            }
            PresenceEvent::Unbound {
                seat,
                incarnation_id,
                status,
                ..
            } => {
                let mut dropped_reconnectable = false;
                if let Some(binding) = state
                    .bindings
                    .get_mut(seat)
                    .and_then(|m| m.get_mut(incarnation_id))
                {
                    dropped_reconnectable =
                        *status == BindingStatus::Unbound && is_reconnectable_binding(binding);
                    binding.status = *status;
                }
                let still_live = state
                    .bindings
                    .get(seat)
                    .is_some_and(|m| m.values().any(|b| b.status == BindingStatus::Live));
                if !still_live {
                    state.last_live_at.entry(seat.clone()).or_insert(meta.at);
                    state
                        .last_drop
                        .insert(seat.clone(), (meta.at, dropped_reconnectable));
                }
            }
            PresenceEvent::TransportHealthChanged {
                transport,
                down,
                detail,
            } => {
                if *down {
                    state.down_transports.insert(*transport);
                    state
                        .down_transport_details
                        .insert(*transport, detail.clone());
                    for map in state.bindings.values_mut() {
                        for b in map.values_mut() {
                            if b.endpoint.kind() == *transport && b.status == BindingStatus::Live {
                                b.status = BindingStatus::TransportDown;
                            }
                        }
                    }
                } else {
                    state.down_transports.remove(transport);
                    state.down_transport_details.remove(transport);
                    for map in state.bindings.values_mut() {
                        for b in map.values_mut() {
                            if b.endpoint.kind() == *transport
                                && b.status == BindingStatus::TransportDown
                            {
                                b.status = BindingStatus::Live;
                            }
                        }
                    }
                }
            }
        }
    }

    fn reply(state: &PresenceState, cmd: PresenceCommand, committed: &[Envelope]) -> WireReply {
        match cmd {
            PresenceCommand::PresenceClaim(c) => {
                let record = committed
                    .iter()
                    .rev()
                    .find_map(|e| match &e.event {
                        Event::Presence(PresenceEvent::SeatClaimed { record }) => {
                            Some(record.clone())
                        }
                        _ => None,
                    })
                    .or_else(|| state.seats.get(&c.seat).cloned())
                    .expect("claimed seat exists in presence state");
                WireReply::PresenceClaim(record)
            }
            PresenceCommand::PresenceSetStatus(c) => {
                let record = state
                    .seats
                    .get(&c.seat)
                    .cloned()
                    .expect("seat exists after set_status");
                WireReply::PresenceSetStatus(record)
            }
            PresenceCommand::PresenceBind(c) => {
                let binding = committed
                    .iter()
                    .rev()
                    .find_map(|e| match &e.event {
                        Event::Presence(PresenceEvent::Bound { binding, .. }) => {
                            Some(binding.clone())
                        }
                        _ => None,
                    })
                    .or_else(|| {
                        c.incarnation_id
                            .as_ref()
                            .and_then(|id| state.bindings.get(&c.seat).and_then(|m| m.get(id)))
                            .cloned()
                    })
                    .expect("bound seat binding exists");
                WireReply::PresenceBind(binding)
            }
            PresenceCommand::PresenceHeartbeat(c) => {
                let binding = state
                    .bindings
                    .get(&c.seat)
                    .and_then(|m| m.get(&c.incarnation_id))
                    .cloned()
                    .expect("heartbeated binding exists");
                WireReply::PresenceHeartbeat(binding)
            }
            PresenceCommand::PresenceUnbind(_) => WireReply::PresenceUnbind(Ack),
            PresenceCommand::PresenceExpire(_) => WireReply::PresenceExpire(Ack),
            PresenceCommand::PresenceTransportHealth(_) => WireReply::PresenceTransportHealth(Ack),
            PresenceCommand::PresenceList(q) => {
                let now = state.effective_reply_now();
                WireReply::PresenceList(state.snapshot_at(&q, now))
            }
            PresenceCommand::PresenceResolve(q) => {
                let now = state.effective_reply_now();
                let res = state
                    .resolve_at(&q, now)
                    .expect("PresenceResolve validated in decide");
                WireReply::PresenceResolve(res)
            }
        }
    }

    fn wrap(event: PresenceEvent) -> Event {
        Event::Presence(event)
    }

    fn unwrap(event: &Event) -> Option<&PresenceEvent> {
        match event {
            Event::Presence(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_ps_binary_fails_closed_as_alive() {
        assert!(
            is_process_alive_with_bin(
                "/nonexistent/rex-wave-ps-binary",
                std::process::id(),
                "Thu Nov 14 00:00:00 2026",
            ),
            "probe spawn failure must fail closed so live sessions are never superseded without takeover"
        );
    }
}
