//! # rex-wave-mailbridge
//!
//! Non-production sandboxed canon mail service ([`SandboxRoot`], [`MailBridge`]),
//! `mailbox.item.schema.json` v1 projection ([`project`], [`wake_id`], [`bell_meta`]),
//! [`BellCourier`] scheduled wake delivery, `Family::Presence` decider
//! ([`presence::Presence`]), tolerant `acks.jsonl` parsing ([`acks`]),
//! structurally read-only shadow reactor ([`shadow`]), and Gate 2 scorer ([`score`]).

pub mod acks;
pub mod presence;
pub mod score;
pub mod shadow;

pub use acks::{
    AckIndex, AckRecord, AcksParseReport, AliasMap, NormalizedLetterPath, SkippedAckLine,
    letter_paths_match, normalize_letter_path, normalize_seat_token, parse_acks_jsonl,
};
pub use presence::{
    ClaudeSessionFile, DEFAULT_LEASE_TTL_MS, DeliveryStatus, IncarnationAffinity, Presence,
    PresenceState, current_process_start, format_delivery_status, is_process_alive,
    load_claude_session_file, load_claude_session_file_from, normalize_proc_start,
    parse_claude_session_json, proc_start_matches, query_process_start,
    select_binding_for_delivery, sender_delivery_notice, try_query_process_start,
};
pub use score::{
    FixtureCoverageReport, GATE1_CASE_IDS, ShadowScoreReport, check_gate1_fixtures,
    extract_shadow_aliases, score_shadow_against_acks, score_shadow_jsonl,
};
pub use shadow::{
    DEFAULT_DIGEST_THRESHOLD, DEFAULT_RECONNECT_GRACE_MS, DEFAULT_RETRY_DEADLINE_MS, DeferReason,
    LetterCandidate, ReadOnlyInterchange, RetryEntry, RetryState, RingDecision,
    RingDeliveryChannel, RingPolicy, RingReferent, ScannedLetter, ShadowPassReport,
    decide_mail_ring, decide_mail_rings_for_seat, run_shadow_pass,
};

use rex_wave_api::mail::*;
use rex_wave_api::*;
use rex_wave_runtime::{BoxFut, DecideCtx, Hub, LocalClient, Reactor, Service, can_read};
use rex_wave_store::{Appended, Journal, StreamWriter};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};

const MARKER_FILE: &str = ".rex-wave-sandbox";
const MAX_PROMOTE_SPAN: u64 = 1_000;
const MAX_HTTP_HEADER: usize = 16 * 1024;
const MAX_HTTP_BODY: usize = 128 * 1024;
const MAX_SINK_CONNS: usize = 32;

static TEMP_NONCE: AtomicU64 = AtomicU64::new(1);
static SANDBOX_ROOTS: OnceLock<RwLock<Vec<PathBuf>>> = OnceLock::new();

fn sandbox_registry() -> &'static RwLock<Vec<PathBuf>> {
    SANDBOX_ROOTS.get_or_init(|| RwLock::new(Vec::new()))
}

fn register_sandbox_root(path: &Path) {
    let mut guard = sandbox_registry()
        .write()
        .unwrap_or_else(|e| e.into_inner());
    if !guard.iter().any(|p| p == path) {
        guard.push(path.to_path_buf());
    }
}

fn regular_file_without_symlinks(root: &Path, segments: &[&str]) -> Option<PathBuf> {
    let mut cur = root.to_path_buf();
    for (idx, seg) in segments.iter().enumerate() {
        cur.push(seg);
        let meta = fs::symlink_metadata(&cur).ok()?;
        if meta.file_type().is_symlink() {
            return None;
        }
        if idx + 1 < segments.len() {
            if !meta.file_type().is_dir() {
                return None;
            }
        } else if !meta.file_type().is_file() {
            return None;
        }
    }
    Some(cur)
}

fn read_bell_prompt_from_root(root: &Path, digest16: &str) -> Option<String> {
    let fname = format!("{digest16}.json");
    let path = regular_file_without_symlinks(root, &["state", "bells", &fname])?;
    let raw = fs::read_to_string(&path).ok()?;
    let val: serde_json::Value = serde_json::from_str(&raw).ok()?;
    val.get("prompt")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
}

fn read_bell_prompt_any_sandbox(digest16: &str) -> Option<String> {
    let roots = sandbox_registry()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    for root in roots.iter().rev() {
        if let Some(prompt) = read_bell_prompt_from_root(root, digest16) {
            return Some(prompt);
        }
    }
    None
}

#[derive(Debug, thiserror::Error)]
pub enum SandboxError {
    #[error("{0} is, contains, or is under a protected folder")]
    Production(PathBuf),
    #[error("protected roots: {0}")]
    Config(String),
    #[error("{0} lacks the .rex-wave-sandbox marker")]
    Unmarked(PathBuf),
    #[error("io: {0}")]
    Io(String),
}

#[derive(Clone, Debug)]
pub struct SandboxRoot {
    root: PathBuf,
    policy: Policy,
}

/// The most entries, folders and names together, one deployment may protect.
pub const MAX_PROTECTED_ROOTS: usize = 64;

/// What a sandbox may never touch on this machine, as a deployment lists it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Protection {
    /// Absolute folders. A sandbox may not be one, contain one, or sit inside one.
    pub roots: Vec<PathBuf>,
    /// Folder names refused anywhere in a sandbox path.
    pub names: Vec<String>,
}

/// A `Protection` resolved once against the filesystem and the home folder,
/// so every later check sees the same answer.
#[derive(Clone, Debug)]
struct Policy {
    roots: Vec<PathBuf>,
    names: Vec<String>,
    homes: Vec<PathBuf>,
}

/// The most symlinks followed while resolving one protected folder.
const MAX_LINK_HOPS: usize = 40;

/// Resolve every symlink on the way to `path`, component by component, the
/// way the filesystem would, including links whose targets do not exist yet.
/// Components that do not exist are kept as written. A folder whose real
/// location cannot be established is an error, never a guess.
fn resolve_links(path: &Path) -> Result<PathBuf, SandboxError> {
    let unresolved = |at: &Path, why: &dyn std::fmt::Display| {
        SandboxError::Config(format!("cannot resolve `{}`: {why}", at.display()))
    };
    let mut out = PathBuf::new();
    let mut pending: Vec<std::ffi::OsString> = Vec::new();
    for comp in path.components().rev() {
        pending.push(comp.as_os_str().to_owned());
    }
    let mut hops = 0;
    while let Some(seg) = pending.pop() {
        match Path::new(&seg).components().next() {
            Some(Component::Prefix(_) | Component::RootDir) => {
                out.push(&seg);
                continue;
            }
            Some(Component::CurDir) | None => continue,
            Some(Component::ParentDir) => {
                out.pop();
                continue;
            }
            Some(Component::Normal(_)) => {}
        }
        let next = out.join(&seg);
        match fs::symlink_metadata(&next) {
            Ok(meta) if meta.file_type().is_symlink() => {
                hops += 1;
                if hops > MAX_LINK_HOPS {
                    return Err(unresolved(path, &"too many symlinks"));
                }
                let target = fs::read_link(&next).map_err(|e| unresolved(&next, &e))?;
                if target.is_absolute() {
                    out = PathBuf::new();
                }
                for comp in target.components().rev() {
                    pending.push(comp.as_os_str().to_owned());
                }
            }
            Ok(_) => out = next,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => out = next,
            Err(e) => return Err(unresolved(&next, &e)),
        }
    }
    Ok(out)
}

/// A protected folder must be absolute and free of `..`, because removing
/// `..` by spelling can name a different folder than the filesystem would.
fn checked_root(path: &Path) -> Result<PathBuf, SandboxError> {
    if !path.is_absolute() {
        return Err(SandboxError::Config(format!(
            "`{}` is not an absolute path",
            path.display()
        )));
    }
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(SandboxError::Config(format!(
            "`{}` contains `..`",
            path.display()
        )));
    }
    lexical_normalize(path)
}

fn checked_name(name: &str) -> Result<String, SandboxError> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\']) {
        return Err(SandboxError::Config(format!(
            "`{name}` is not a single folder name"
        )));
    }
    Ok(name.to_owned())
}

impl Policy {
    fn resolve(protection: &Protection) -> Result<Self, SandboxError> {
        if protection.roots.len() + protection.names.len() > MAX_PROTECTED_ROOTS {
            return Err(SandboxError::Config(format!(
                "more than {MAX_PROTECTED_ROOTS} protected entries"
            )));
        }
        // Hold each folder under both its given and its resolved name, so a
        // symlinked location cannot be used to step around it.
        let mut roots = BTreeSet::new();
        for root in &protection.roots {
            let given = checked_root(root)?;
            roots.insert(resolve_links(&given)?);
            roots.insert(given);
        }
        let mut names = BTreeSet::new();
        for name in &protection.names {
            names.insert(checked_name(name)?);
        }
        Ok(Policy {
            roots: roots.into_iter().collect(),
            names: names.into_iter().collect(),
            homes: home_dirs().into_iter().collect(),
        })
    }
}

fn lexical_normalize(path: &Path) -> Result<PathBuf, SandboxError> {
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| SandboxError::Io(e.to_string()))?
            .join(path)
    };
    let mut out = PathBuf::new();
    for comp in abs.components() {
        match comp {
            Component::Prefix(p) => out.push(p.as_os_str()),
            Component::RootDir => out.push(comp.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(seg) => out.push(seg),
        }
    }
    Ok(out)
}

fn home_dirs() -> BTreeSet<PathBuf> {
    let mut homes = BTreeSet::new();
    if let Some(h) = std::env::var_os("HOME") {
        let hp = PathBuf::from(h);
        if !hp.as_os_str().is_empty() {
            if let Ok(canon) = fs::canonicalize(&hp) {
                homes.insert(canon);
            }
            if let Ok(lex) = lexical_normalize(&hp) {
                homes.insert(lex);
            }
        }
    }
    homes
}

/// Parse a protected-roots file, one entry per line. Blank lines and lines
/// starting with `#` are skipped. `name: <folder name>` refuses that name
/// anywhere in a sandbox path. Every other line is a folder: an absolute
/// path, or one starting with `~/` for `home`, and never containing `..`.
pub fn parse_protected_roots(text: &str, home: Option<&Path>) -> Result<Protection, SandboxError> {
    let mut roots = BTreeSet::new();
    let mut names = BTreeSet::new();
    for (index, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let at_line = |e: SandboxError| match e {
            SandboxError::Config(detail) => {
                SandboxError::Config(format!("line {}: {detail}", index + 1))
            }
            other => other,
        };
        if let Some(name) = line.strip_prefix("name:") {
            names.insert(checked_name(name.trim()).map_err(at_line)?);
        } else {
            let path = match line.strip_prefix("~/") {
                Some(rest) => home
                    .ok_or_else(|| {
                        SandboxError::Config(format!(
                            "line {}: `~/` needs HOME to be set",
                            index + 1
                        ))
                    })?
                    .join(rest),
                None => PathBuf::from(line),
            };
            roots.insert(checked_root(&path).map_err(at_line)?);
        }
        if roots.len() + names.len() > MAX_PROTECTED_ROOTS {
            return Err(SandboxError::Config(format!(
                "more than {MAX_PROTECTED_ROOTS} protected entries"
            )));
        }
    }
    Ok(Protection {
        roots: roots.into_iter().collect(),
        names: names.into_iter().collect(),
    })
}

/// Where a deployment lists what it protects: `rex-wave/protected-roots`
/// under `xdg_config_home`, or under `home/.config`. A relative
/// `xdg_config_home` is ignored, as the XDG specification requires.
pub fn protected_roots_file(
    xdg_config_home: Option<&Path>,
    home: Option<&Path>,
) -> Result<PathBuf, SandboxError> {
    let base = match (xdg_config_home, home) {
        (Some(dir), _) if dir.is_absolute() => dir.to_path_buf(),
        (_, Some(home)) if home.is_absolute() => home.join(".config"),
        _ => {
            return Err(SandboxError::Config(
                "cannot locate the protected-roots file: set HOME or XDG_CONFIG_HOME to an absolute path".into(),
            ));
        }
    };
    Ok(base.join("rex-wave").join("protected-roots"))
}

/// Read a protected-roots file. A file that is missing, unreadable or
/// malformed is an error and never an empty list: running with nothing
/// protected takes an explicit, empty file.
pub fn load_protected_roots_from(
    file: &Path,
    home: Option<&Path>,
) -> Result<Protection, SandboxError> {
    let text = fs::read_to_string(file).map_err(|e| {
        let hint = if e.kind() == std::io::ErrorKind::NotFound {
            " (create it; it may be empty if there is nothing to protect)"
        } else {
            ""
        };
        SandboxError::Config(format!("{}: {e}{hint}", file.display()))
    })?;
    parse_protected_roots(&text, home)
        .map_err(|e| SandboxError::Config(format!("{}: {e}", file.display())))
}

/// Read this deployment's protected-roots file, located from the environment.
pub fn load_protected_roots() -> Result<Protection, SandboxError> {
    let env_path = |key: &str| {
        std::env::var_os(key)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    };
    let home = env_path("HOME");
    let file = protected_roots_file(env_path("XDG_CONFIG_HOME").as_deref(), home.as_deref())?;
    load_protected_roots_from(&file, home.as_deref())
}

fn check_not_production(
    candidate: &Path,
    reported: &Path,
    policy: &Policy,
) -> Result<(), SandboxError> {
    let refused = || Err(SandboxError::Production(reported.to_path_buf()));
    // The filesystem root and the home folder are never a sandbox.
    if candidate.parent().is_none() || policy.homes.iter().any(|home| home.starts_with(candidate)) {
        return refused();
    }
    if candidate.components().any(|c| {
        policy
            .names
            .iter()
            .any(|name| c.as_os_str() == name.as_str())
    }) {
        return refused();
    }
    for prod in &policy.roots {
        if candidate.starts_with(prod) || prod.starts_with(candidate) {
            return refused();
        }
    }
    Ok(())
}

impl SandboxRoot {
    /// Verify `path` against what this deployment's protected-roots file lists.
    pub fn verify(path: &Path) -> Result<Self, SandboxError> {
        Self::verify_with(path, &load_protected_roots()?)
    }

    /// Verify `path` against an explicit `Protection`.
    pub fn verify_with(path: &Path, protection: &Protection) -> Result<Self, SandboxError> {
        let forbidden = Policy::resolve(protection)?;
        let lex = lexical_normalize(path)?;
        check_not_production(&lex, path, &forbidden)?;

        // Judge a symlink by where it points, so the verdict is the same
        // whether or not its target exists on this machine.
        if let Ok(target) = fs::read_link(path) {
            let base = lex.parent().unwrap_or(Path::new("/"));
            let pointed = lexical_normalize(&base.join(target))?;
            check_not_production(&pointed, path, &forbidden)?;
        }

        let mut ancestor = lex.as_path();
        while let Some(parent) = ancestor.parent() {
            if let Ok(canon_anc) = fs::canonicalize(ancestor) {
                let resolved = match lex.strip_prefix(ancestor) {
                    Ok(suffix) if !suffix.as_os_str().is_empty() => canon_anc.join(suffix),
                    _ => canon_anc,
                };
                check_not_production(&resolved, path, &forbidden)?;
                break;
            }
            ancestor = parent;
        }

        let canon = fs::canonicalize(path).map_err(|e| SandboxError::Io(e.to_string()))?;
        check_not_production(&canon, path, &forbidden)?;

        let meta = fs::metadata(&canon).map_err(|e| SandboxError::Io(e.to_string()))?;
        if !meta.is_dir() {
            return Err(SandboxError::Io(format!(
                "{} is not a directory",
                canon.display()
            )));
        }

        let marker = canon.join(MARKER_FILE);
        match fs::symlink_metadata(&marker) {
            Ok(m) if m.file_type().is_file() => Ok(SandboxRoot {
                root: canon,
                policy: forbidden,
            }),
            _ => Err(SandboxError::Unmarked(canon)),
        }
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    fn ensure_dir(&self, segments: &[&str]) -> Result<PathBuf, ApiError> {
        let forbidden = &self.policy;
        let mut cur = self.root.clone();
        for &seg in segments {
            if seg.is_empty()
                || seg == "."
                || seg == ".."
                || seg.contains('/')
                || seg.contains('\\')
            {
                return Err(ApiError::Invalid(InvalidId {
                    what: "sandbox directory".into(),
                    reason: "invalid directory segment".into(),
                }));
            }
            cur = cur.join(seg);
            match fs::symlink_metadata(&cur) {
                Ok(m) if m.file_type().is_symlink() => {
                    return Err(ApiError::Denied {
                        principal: "mailbridge".into(),
                        action: format!("write through symlink {}", cur.display()),
                    });
                }
                Ok(m) if !m.file_type().is_dir() => {
                    return Err(ApiError::conflict(format!(
                        "{} exists and is not a directory",
                        cur.display()
                    )));
                }
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir(&cur).map_err(|ce| {
                        ApiError::Internal(format!("mkdir {}: {ce}", cur.display()))
                    })?;
                }
                Err(e) => return Err(ApiError::Internal(format!("stat {}: {e}", cur.display()))),
            }
            let canon = fs::canonicalize(&cur)
                .map_err(|e| ApiError::Internal(format!("canonicalize {}: {e}", cur.display())))?;
            if check_not_production(&canon, &cur, forbidden).is_err()
                || !canon.starts_with(&self.root)
            {
                return Err(ApiError::Denied {
                    principal: "mailbridge".into(),
                    action: format!("escape sandbox via {}", cur.display()),
                });
            }
            cur = canon;
        }
        Ok(cur)
    }

    fn confine(&self, raw: &Path) -> Result<(PathBuf, String, bool), SandboxError> {
        if raw.as_os_str().is_empty() || raw.components().any(|c| matches!(c, Component::ParentDir))
        {
            return Err(SandboxError::Io("invalid path outside sandbox".into()));
        }
        let forbidden = &self.policy;
        let joined = if raw.is_absolute() {
            raw.to_path_buf()
        } else {
            self.root.join(raw)
        };
        let lex = lexical_normalize(&joined)?;
        check_not_production(&lex, raw, forbidden)?;
        if !lex.starts_with(&self.root) || lex == self.root {
            return Err(SandboxError::Production(raw.to_path_buf()));
        }

        let mut walk = self.root.clone();
        if let Ok(rel_parts) = lex.strip_prefix(&self.root) {
            for comp in rel_parts.components() {
                walk.push(comp.as_os_str());
                if let Ok(sm) = fs::symlink_metadata(&walk)
                    && sm.file_type().is_symlink()
                {
                    return Err(SandboxError::Production(raw.to_path_buf()));
                }
            }
        }

        match fs::canonicalize(&lex) {
            Ok(canon) => {
                check_not_production(&canon, raw, forbidden)?;
                if !canon.starts_with(&self.root) || canon == self.root {
                    return Err(SandboxError::Production(raw.to_path_buf()));
                }
                let rel = relative_slash(&self.root, &canon)?;
                Ok((canon, rel, true))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Some(parent) = lex.parent()
                    && let Ok(canon_parent) = fs::canonicalize(parent)
                {
                    if !canon_parent.starts_with(&self.root) {
                        return Err(SandboxError::Production(raw.to_path_buf()));
                    }
                    if let Some(fname) = lex.file_name() {
                        check_not_production(&canon_parent.join(fname), raw, forbidden)?;
                    }
                }
                let rel = relative_slash(&self.root, &lex)?;
                Ok((lex, rel, false))
            }
            Err(e) => Err(SandboxError::Io(e.to_string())),
        }
    }
}

fn relative_slash(root: &Path, target: &Path) -> Result<String, SandboxError> {
    let stripped = target
        .strip_prefix(root)
        .map_err(|_| SandboxError::Production(target.to_path_buf()))?;
    let mut parts = Vec::new();
    for comp in stripped.components() {
        match comp {
            Component::Normal(seg) => {
                let s = seg
                    .to_str()
                    .ok_or_else(|| SandboxError::Io("non-utf8 path".into()))?;
                parts.push(s);
            }
            _ => return Err(SandboxError::Io("unexpected path component".into())),
        }
    }
    if parts.is_empty() {
        return Err(SandboxError::Io("empty relative path".into()));
    }
    Ok(parts.join("/"))
}

pub(crate) fn sha256_bytes(bytes: &[u8]) -> Sha256Hex {
    let mut h = Sha256::new();
    h.update(bytes);
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    Sha256Hex::parse(hex).expect("64 lower-hex sha256")
}

fn sha256_short16(input: &str) -> String {
    let mut h = Sha256::new();
    h.update(input.as_bytes());
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    hex[..16].to_owned()
}

pub fn wake_id(seat: &SeatName, mailbox: &str, file: &str, full_sha256: &Sha256Hex) -> WakeId {
    let mut h = Sha256::new();
    h.update(format!("wake/1\n{seat}\n{mailbox}/{file}\n{full_sha256}").as_bytes());
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    WakeId(hex[..16].to_owned())
}

pub(crate) fn unix_secs_to_iso8601_utc(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let hour = rem / 3_600;
    let minute = (rem % 3_600) / 60;
    let second = rem % 60;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };

    format!("{year:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn is_valid_address_segment(seg: &str) -> bool {
    let bytes = seg.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 40
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes.iter().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        })
}

fn is_valid_address(s: &str) -> bool {
    let mut count = 0usize;
    for seg in s.split('/') {
        count += 1;
        if count > 3 || !is_valid_address_segment(seg) {
            return false;
        }
    }
    count >= 1
}

fn is_valid_token(s: &str) -> bool {
    let bytes = s.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn is_valid_kind(s: &str) -> bool {
    let bytes = s.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 24
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
}

fn is_valid_bell_from(s: &str) -> bool {
    is_valid_address_segment(s)
}

fn is_valid_box(s: &str) -> bool {
    let Some(rest) = s.strip_prefix("to-") else {
        return false;
    };
    !rest.is_empty()
        && rest.len() <= 40
        && rest
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

fn is_valid_md_file(s: &str) -> bool {
    let Some(stem) = s.strip_suffix(".md") else {
        return false;
    };
    let bytes = stem.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 116
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn is_valid_ts(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 20
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T'
        && b[13] == b':'
        && b[16] == b':'
        && b[19] == b'Z'
        && b.iter()
            .enumerate()
            .all(|(i, c)| matches!(i, 4 | 7 | 10 | 13 | 16 | 19) || c.is_ascii_digit())
}

pub fn bell_meta(wake: &WakeRecord) -> BTreeMap<String, String> {
    let mut meta = BTreeMap::new();
    meta.insert("schema".into(), "1".into());
    let event = match wake.event.as_str() {
        "letter" | "changed" | "batch" | "backlog" | "selftest" | "wake" => wake.event.as_str(),
        _ => "wake",
    };
    meta.insert("event".into(), event.into());
    meta.insert(
        "ts".into(),
        if is_valid_ts(&wake.ts) {
            wake.ts.clone()
        } else {
            "1970-01-01T00:00:00Z".into()
        },
    );

    if matches!(event, "letter" | "changed") {
        if wake.wake_id.0.len() == 16
            && wake
                .wake_id
                .0
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            meta.insert("wake_id".into(), wake.wake_id.0.clone());
        }
        if is_valid_kind(&wake.item.kind) {
            meta.insert("kind".into(), wake.item.kind.clone());
        }
        let from_tok = wake.item.from.to_ascii_lowercase();
        meta.insert(
            "from".into(),
            if is_valid_bell_from(&from_tok) {
                from_tok
            } else {
                "?".into()
            },
        );

        let path_str = wake.item.reference.path.to_string_lossy();
        if let Some((box_part, file_part)) = path_str.rsplit_once('/') {
            let box_only = box_part.rsplit('/').next().unwrap_or(box_part);
            if is_valid_box(box_only) {
                meta.insert("box".into(), box_only.to_owned());
            }
            if is_valid_md_file(file_part) {
                meta.insert("file".into(), file_part.to_owned());
            }
        }
        meta.insert(
            "sha16".into(),
            wake.item.reference.sha256.as_str()[..16].to_owned(),
        );
        meta.insert(
            "topic".into(),
            match &wake.item.topic {
                Some(t) if is_valid_token(t) => t.clone(),
                _ => "?".into(),
            },
        );
        meta.insert(
            "round".into(),
            match wake.item.round {
                Some(r) if r <= 9_999 => r.to_string(),
                _ => "?".into(),
            },
        );
    } else if matches!(event, "batch" | "backlog") {
        meta.insert("count".into(), wake.count.max(1).to_string());
        if event == "backlog" {
            meta.insert("unknown".into(), wake.unknown.to_string());
        }
    }

    meta
}

fn principal_mail_address(p: &Principal) -> String {
    match p {
        Principal::Operator { seat } | Principal::Seat { seat } => seat.as_str().to_owned(),
        Principal::Worker { wave, role } => format!("{wave}/{role}"),
        Principal::System { part } => match part {
            SystemPart::WaveConductor => "system/wave_conductor".into(),
            SystemPart::Herald => "system/herald".into(),
            SystemPart::BbBridge => "system/bb_bridge".into(),
            SystemPart::T3Bridge => "system/t3_bridge".into(),
            SystemPart::MailBridge => "system/mail_bridge".into(),
            SystemPart::WakeBell => "system/wake_bell".into(),
            SystemPart::PresenceMonitor => "system/presence_monitor".into(),
        },
    }
}

fn priority_word(p: LetterPriority) -> &'static str {
    match p {
        LetterPriority::Low => "low",
        LetterPriority::Normal => "normal",
        LetterPriority::High => "high",
        LetterPriority::Blocking => "blocking",
    }
}

fn yaml_quote(raw: &str) -> String {
    let mut s = String::with_capacity(raw.len() + 2);
    s.push('"');
    for ch in raw.chars() {
        match ch {
            '\\' => s.push_str("\\\\"),
            '"' => s.push_str("\\\""),
            '\n' | '\r' => s.push(' '),
            c if c.is_control() => {}
            c => s.push(c),
        }
    }
    s.push('"');
    s
}

fn sanitize_topic_for_file(raw: &str) -> String {
    let mut out: String = raw
        .trim()
        .chars()
        .filter_map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                Some(c)
            } else if c.is_whitespace() {
                Some('-')
            } else {
                None
            }
        })
        .collect();
    while out.starts_with('.') || out.starts_with('-') {
        out.remove(0);
    }
    if out.is_empty() {
        out.push_str("letter");
    }
    out.truncate(64);
    out
}

fn room_provenance(room: &room::RoomId) -> String {
    match room {
        room::RoomId::Channel {
            scope: room::RoomScope::Global,
            name,
        } => format!("#{name}"),
        room::RoomId::Channel {
            scope: room::RoomScope::Project { project },
            name,
        } => format!("#{project}/{name}"),
        room::RoomId::Direct { pair } => {
            let (a, b): (Principal, Principal) = pair.clone().into();
            format!("dm:{a}<->{b}")
        }
    }
}

pub fn render_letter(req: &MailPromote, by: &Principal, entries: &[Envelope]) -> (String, Vec<u8>) {
    let topic = sanitize_topic_for_file(&req.topic);
    let round = 1u32;
    let file_name = format!("{topic}-r{round}.md");

    let from = principal_mail_address(by);
    let to_yaml = if req.to.len() == 1 {
        req.to[0].as_str().to_owned()
    } else {
        let joined: Vec<&str> = req.to.iter().map(SeatName::as_str).collect();
        format!("[{}]", joined.join(", "))
    };
    let room_label = room_provenance(&req.room);
    let subject_clean: String = req
        .subject
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .filter(|c| !c.is_control())
        .collect();

    let mut sorted: Vec<&Envelope> = entries.iter().collect();
    sorted.sort_by_key(|e| e.seq);

    let mut out = String::new();
    out.push_str("---\n");
    out.push_str(&format!("from: {from}\n"));
    out.push_str(&format!("to: {to_yaml}\n"));
    out.push_str(&format!("subject: {}\n", yaml_quote(subject_clean.trim())));
    out.push_str(&format!("topic: {topic}\n"));
    out.push_str(&format!("round: {round}\n"));
    out.push_str(&format!("priority: {}\n", priority_word(req.priority)));
    out.push_str(&format!("room: {}\n", yaml_quote(&room_label)));
    out.push_str(&format!(
        "range: [{}, {}]\n",
        req.range.first().0,
        req.range.last().0
    ));
    out.push_str("---\n\n");
    out.push_str(&format!("# {}\n\n", subject_clean.trim()));
    out.push_str(&format!(
        "Promoted from `{room_label}` (`seq {}..={}`).\n",
        req.range.first().0,
        req.range.last().0
    ));

    for env in sorted {
        if let Event::Room(room::RoomEvent::Posted { body, re, .. }) = &env.event {
            out.push('\n');
            match re {
                Some(r) => {
                    out.push_str(&format!("> **[#{} -> #{}] {}**:\n", env.seq.0, r.0, env.by))
                }
                None => out.push_str(&format!("> **[#{}] {}**:\n", env.seq.0, env.by)),
            }
            for line in body.as_str().lines() {
                if line.is_empty() {
                    out.push_str(">\n");
                } else {
                    out.push_str(&format!("> {line}\n"));
                }
            }
        }
    }

    (file_name, out.into_bytes())
}

#[derive(Default)]
struct Frontmatter {
    from: Option<String>,
    to: Option<String>,
    subject: Option<String>,
    topic: Option<String>,
    round: Option<u32>,
    priority: Option<String>,
    ts: Option<String>,
}

pub(crate) fn unquote_yaml(val: &str) -> String {
    let trimmed = val.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        let inner = &trimmed[1..trimmed.len() - 1];
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                if let Some(next) = chars.next() {
                    out.push(next);
                }
            } else {
                out.push(c);
            }
        }
        out
    } else if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        trimmed[1..trimmed.len() - 1].to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn parse_frontmatter(text: &str) -> Frontmatter {
    let mut fm = Frontmatter::default();
    let rest = if let Some(r) = text.strip_prefix("---\n") {
        r
    } else if let Some(r) = text.strip_prefix("---\r\n") {
        r
    } else {
        return fm;
    };

    for line in rest.lines() {
        let trimmed = line.trim_end_matches('\r');
        if trimmed == "---" {
            break;
        }
        let Some((k, v)) = trimmed.split_once(':') else {
            continue;
        };
        let key = k.trim();
        let val = unquote_yaml(v);
        if val.is_empty() {
            continue;
        }
        match key {
            "from" | "author" => {
                if fm.from.is_none() || key == "from" {
                    fm.from = Some(val);
                }
            }
            "to" => {
                let first = val
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .split(',')
                    .next()
                    .unwrap_or("")
                    .trim();
                if !first.is_empty() {
                    fm.to = Some(unquote_yaml(first));
                }
            }
            "subject" => fm.subject = Some(val),
            "topic" => fm.topic = Some(val),
            "round" => fm.round = val.parse::<u32>().ok(),
            "priority" => fm.priority = Some(val.to_ascii_lowercase()),
            "ts" => fm.ts = Some(val),
            _ => {}
        }
    }
    fm
}

fn truncate_label(raw: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in raw.chars().filter(|c| !c.is_control()).enumerate() {
        if idx >= max_chars {
            break;
        }
        out.push(ch);
    }
    out
}

fn is_acked_in_sandbox(
    root: &SandboxRoot,
    to_seat: &str,
    rel_path: &str,
    sha256: &Sha256Hex,
) -> bool {
    let Some(acks_path) = regular_file_without_symlinks(root.path(), &["state", "acks.jsonl"])
    else {
        return false;
    };
    let Ok(contents) = fs::read_to_string(&acks_path) else {
        return false;
    };
    let aliases = acks::AliasMap::empty();
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed)
            && ack_row_matches(&val, to_seat, rel_path, sha256, &aliases)
        {
            return true;
        }
    }
    false
}

fn ack_row_matches(
    val: &serde_json::Value,
    to_seat: &str,
    rel_path: &str,
    sha256: &Sha256Hex,
    aliases: &acks::AliasMap,
) -> bool {
    acks::ack_value_matches(val, to_seat, rel_path, sha256, aliases)
}

pub fn project(root: &SandboxRoot, path: &Path, bytes: &[u8]) -> Result<MailboxItem, SandboxError> {
    let (abs_path, rel_path, exists) = root.confine(path)?;
    let sha256 = sha256_bytes(bytes);
    let text = String::from_utf8_lossy(bytes);
    let fm = parse_frontmatter(&text);

    let box_seat = rel_path
        .split_once('/')
        .and_then(|(first, _)| first.strip_prefix("to-"))
        .filter(|s| is_valid_address(s))
        .map(str::to_owned);

    let to = box_seat
        .or_else(|| {
            fm.to
                .as_deref()
                .map(|s| s.trim_start_matches('@').to_ascii_lowercase())
                .filter(|s| is_valid_address(s))
        })
        .unwrap_or_else(|| "unknown".into());

    let from = fm
        .from
        .as_deref()
        .map(|s| s.trim_start_matches('@').to_ascii_lowercase())
        .filter(|s| is_valid_address(s))
        .unwrap_or_else(|| "unknown".into());

    let (ts, ts_basis) = if exists
        && let Ok(meta) = fs::metadata(&abs_path)
        && let Ok(modified) = meta.modified()
        && let Ok(dur) = modified.duration_since(UNIX_EPOCH)
    {
        (unix_secs_to_iso8601_utc(dur.as_secs()), "mtime".to_owned())
    } else if let Some(recorded) = fm.ts.filter(|t| is_valid_ts(t)) {
        (recorded, "recorded".to_owned())
    } else {
        ("1970-01-01T00:00:00Z".to_owned(), "recorded".to_owned())
    };

    let state = if is_acked_in_sandbox(root, &to, &rel_path, &sha256) {
        "acked".to_owned()
    } else {
        "open".to_owned()
    };

    let topic = fm.topic.filter(|t| is_valid_token(t));
    let round = fm.round.filter(|&r| r <= 9_999);
    let label = match (fm.priority.as_deref(), fm.subject.as_deref()) {
        (Some("blocking"), Some(subj)) if !subj.is_empty() => {
            Some(truncate_label(&format!("[blocking] {subj}"), 160))
        }
        (Some("blocking"), _) => Some("[blocking]".into()),
        (_, Some(subj)) if !subj.is_empty() => Some(truncate_label(subj, 160)),
        _ => None,
    };

    Ok(MailboxItem {
        schema: MailSchemaV1,
        id: format!("letter:{rel_path}"),
        from,
        to,
        kind: "letter".into(),
        state,
        ts,
        ts_basis,
        reference: MailRef {
            store: "letters".into(),
            path: PathBuf::from(&rel_path),
            sha256,
        },
        topic,
        round,
        label,
    })
}

fn fsync_dir(dir: &Path) -> Result<(), String> {
    let f = File::open(dir).map_err(|e| format!("open dir {}: {e}", dir.display()))?;
    f.sync_all()
        .map_err(|e| format!("fsync dir {}: {e}", dir.display()))
}

fn atomic_write_if_absent(dir: &Path, file_name: &str, bytes: &[u8]) -> Result<PathBuf, ApiError> {
    let dest = dir.join(file_name);
    match fs::symlink_metadata(&dest) {
        Ok(m) if m.file_type().is_symlink() => {
            return Err(ApiError::Denied {
                principal: "mailbridge".into(),
                action: format!("write through symlink {}", dest.display()),
            });
        }
        Ok(_) => {
            let existing = fs::read(&dest)
                .map_err(|e| ApiError::Internal(format!("read {}: {e}", dest.display())))?;
            if existing == bytes {
                return Ok(dest);
            }
            return Err(ApiError::conflict(format!(
                "canon file {} already exists with different bytes (BoxMonotone)",
                dest.display()
            )));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(ApiError::Internal(format!("stat {}: {e}", dest.display()))),
    }

    let nonce = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
    let tmp = dir.join(format!(".tmp.{}.{nonce}", std::process::id()));
    let write_res = (|| -> Result<(), String> {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .map_err(|e| format!("create temp {}: {e}", tmp.display()))?;
        f.write_all(bytes)
            .map_err(|e| format!("write temp {}: {e}", tmp.display()))?;
        f.sync_all()
            .map_err(|e| format!("fsync temp {}: {e}", tmp.display()))?;
        Ok(())
    })();

    if let Err(e) = write_res {
        let _ = fs::remove_file(&tmp);
        return Err(ApiError::Internal(e));
    }

    let link_res = fs::hard_link(&tmp, &dest);
    let _ = fs::remove_file(&tmp);

    match link_res {
        Ok(()) => {
            fsync_dir(dir).map_err(ApiError::Internal)?;
            Ok(dest)
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::symlink_metadata(&dest).is_ok_and(|m| m.file_type().is_symlink()) {
                return Err(ApiError::Denied {
                    principal: "mailbridge".into(),
                    action: format!("write through symlink {}", dest.display()),
                });
            }
            let existing = fs::read(&dest)
                .map_err(|re| ApiError::Internal(format!("read {}: {re}", dest.display())))?;
            if existing == bytes {
                Ok(dest)
            } else {
                Err(ApiError::conflict(format!(
                    "canon file {} already exists with different bytes (BoxMonotone)",
                    dest.display()
                )))
            }
        }
        Err(e) => Err(ApiError::Internal(format!("link {}: {e}", dest.display()))),
    }
}

fn canonical_bell_key(schedule_id: &str, mark: &str) -> String {
    let sched = schedule_id.trim();
    let m = mark.trim();
    if m.is_empty() || sched.ends_with(&format!("@{m}")) {
        sched.to_owned()
    } else {
        format!("{sched}@{m}")
    }
}

fn bell_digest16(seat: &SeatName, canonical_key: &str) -> String {
    sha256_short16(&format!("bell/1\n{seat}\n{canonical_key}"))
}

struct MailWriterState {
    writer: StreamWriter,
    seen_wakes: HashSet<WakeId>,
}

fn collect_seen_wakes(reader: &Journal) -> HashSet<WakeId> {
    let mut seen = HashSet::new();
    let mut after = Seq::ORIGIN;
    while let Ok(page) = reader.read(&StreamId::Mail, after, 512) {
        let Some(last) = page.last() else {
            break;
        };
        after = last.seq;
        for env in &page {
            match &env.event {
                Event::Mail(MailEvent::Banked { letter, .. }) => {
                    seen.insert(letter.wake_id.clone());
                }
                Event::Mail(MailEvent::Projected { item }) => {
                    let path_str = item.reference.path.to_string_lossy();
                    if let Ok(seat) = SeatName::parse(&item.to)
                        && let Some((box_name, file_name)) = path_str.rsplit_once('/')
                    {
                        seen.insert(wake_id(&seat, box_name, file_name, &item.reference.sha256));
                    }
                }
                _ => {}
            }
        }
    }
    seen
}

pub struct MailBridge {
    root: SandboxRoot,
    reader: Journal,
    state: Mutex<MailWriterState>,
    hub: Option<Hub>,
}

impl MailBridge {
    /// Without a [`Hub`], committed `Mail` entries persist for replay and `Read` only;
    /// live daemons pass a [`Hub`] via [`MailBridge::with_hub`].
    pub fn new(root: SandboxRoot, reader: Journal, writer: StreamWriter) -> Self {
        register_sandbox_root(root.path());
        let seen_wakes = collect_seen_wakes(&reader);
        MailBridge {
            root,
            reader,
            state: Mutex::new(MailWriterState { writer, seen_wakes }),
            hub: None,
        }
    }

    pub fn with_hub(root: SandboxRoot, reader: Journal, writer: StreamWriter, hub: Hub) -> Self {
        register_sandbox_root(root.path());
        let seen_wakes = collect_seen_wakes(&reader);
        MailBridge {
            root,
            reader,
            state: Mutex::new(MailWriterState { writer, seen_wakes }),
            hub: Some(hub),
        }
    }

    fn publish_fresh(&self, appended: &Appended) {
        if let (Appended::Fresh(committed), Some(hub)) = (appended, &self.hub) {
            hub.publish(committed);
        }
    }

    async fn promote(&self, req: MailPromote, ctx: DecideCtx) -> Result<LetterRef, ApiError> {
        if req.to.is_empty() {
            return Err(ApiError::Invalid(InvalidId {
                what: "to".into(),
                reason: "at least one recipient seat is required".into(),
            }));
        }
        let trimmed_subject = req.subject.trim();
        if trimmed_subject.is_empty() || req.subject.chars().any(char::is_control) {
            return Err(ApiError::Invalid(InvalidId {
                what: "subject".into(),
                reason: "must be non-empty without control characters".into(),
            }));
        }
        if !is_valid_token(req.topic.trim())
            || req.topic.trim().starts_with('.')
            || req.topic.trim().starts_with('-')
        {
            return Err(ApiError::Invalid(InvalidId {
                what: "topic".into(),
                reason: "must be a safe token ([A-Za-z0-9._-], 1..=64 chars)".into(),
            }));
        }

        let span_u64 = req.range.last().0 - req.range.first().0 + 1;
        if span_u64 > MAX_PROMOTE_SPAN {
            return Err(ApiError::Invalid(InvalidId {
                what: "range".into(),
                reason: format!("promotion range may span at most {MAX_PROMOTE_SPAN} seqs"),
            }));
        }

        let room_stream = StreamId::Room {
            room: req.room.clone(),
        };
        can_read(&ctx.by, &room_stream)?;

        let entries: Vec<Envelope> = self
            .reader
            .read(
                &room_stream,
                Seq(req.range.first().0 - 1),
                span_u64 as usize,
            )
            .map_err(|e| ApiError::Internal(e.to_string()))?
            .into_iter()
            .filter(|e| e.seq <= req.range.last())
            .collect();

        let has_posts = entries
            .iter()
            .any(|e| matches!(e.event, Event::Room(room::RoomEvent::Posted { .. })));
        if entries.is_empty() || !has_posts {
            return Err(ApiError::NotFound {
                what: format!(
                    "posted messages in range {}..={}",
                    req.range.first().0,
                    req.range.last().0
                ),
            });
        }

        let (file_name, bytes) = render_letter(&req, &ctx.by, &entries);
        let sha256 = sha256_bytes(&bytes);

        let mut seen_seats = BTreeSet::new();
        let mut unique_seats = Vec::new();
        for seat in &req.to {
            if seen_seats.insert(seat.clone()) {
                unique_seats.push(seat.clone());
            }
        }

        let mut banked_letters = Vec::with_capacity(unique_seats.len());
        for seat in &unique_seats {
            let box_name = format!("to-{seat}");
            let box_dir = self.root.ensure_dir(&[&box_name])?;
            let dest = atomic_write_if_absent(&box_dir, &file_name, &bytes)?;
            let item = project(&self.root, &dest, &bytes)
                .map_err(|e| ApiError::Internal(format!("project banked letter: {e}")))?;
            let wid = wake_id(seat, &box_name, &file_name, &sha256);
            banked_letters.push(LetterRef {
                path: item.reference.path.clone(),
                sha256: sha256.clone(),
                wake_id: wid,
                item,
            });
        }

        let seats_key: Vec<&str> = unique_seats.iter().map(SeatName::as_str).collect();
        let idem_hash = sha256_short16(&format!(
            "promote/1\n{:?}\n{}:{}\n{}\n{sha256}",
            req.room,
            req.range.first().0,
            req.range.last().0,
            seats_key.join(",")
        ));
        let idem = IdemKey::parse(format!("promote:{idem_hash}"))?;

        let events: Vec<Event> = banked_letters
            .iter()
            .map(|letter| {
                Event::Mail(MailEvent::Banked {
                    from_room: req.room.clone(),
                    range: (req.range.first(), req.range.last()),
                    letter: letter.clone(),
                })
            })
            .collect();

        let mut st = self.state.lock().await;
        let appended = st
            .writer
            .append(&ctx.by, ctx.at, Some(&idem), events)
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        for letter in &banked_letters {
            st.seen_wakes.insert(letter.wake_id.clone());
        }
        drop(st);
        self.publish_fresh(&appended);

        Ok(banked_letters
            .into_iter()
            .next()
            .expect("non-empty recipients"))
    }

    async fn ack(&self, req: MailAck, ctx: DecideCtx) -> Result<Ack, ApiError> {
        let (abs_path, rel_path, exists) = self.root.confine(&req.path).map_err(|e| match e {
            SandboxError::Production(_) => ApiError::Denied {
                principal: ctx.by.to_string(),
                action: format!("ack path outside sandbox ({})", req.path.display()),
            },
            other => ApiError::Invalid(InvalidId {
                what: "path".into(),
                reason: other.to_string(),
            }),
        })?;
        if !exists {
            return Err(ApiError::NotFound { what: rel_path });
        }

        let box_seat = rel_path
            .split_once('/')
            .and_then(|(first, _)| first.strip_prefix("to-"));

        let acker_seat = match (&ctx.by, box_seat) {
            (Principal::Seat { seat }, Some(target)) if seat.as_str() == target => {
                seat.as_str().to_owned()
            }
            (Principal::Seat { .. } | Principal::Worker { .. }, Some(target)) => {
                return Err(ApiError::Denied {
                    principal: ctx.by.to_string(),
                    action: format!("ack letter in to-{target}"),
                });
            }
            (Principal::Operator { seat }, Some(target))
                if seat.as_str() == target || target == "owner" =>
            {
                target.to_owned()
            }
            (Principal::Operator { .. }, Some(target)) => {
                return Err(ApiError::Denied {
                    principal: ctx.by.to_string(),
                    action: format!("ack letter in to-{target}"),
                });
            }
            (Principal::System { .. }, Some(target)) => target.to_owned(),
            (Principal::Operator { seat } | Principal::Seat { seat }, None) => {
                seat.as_str().to_owned()
            }
            _ => principal_mail_address(&ctx.by),
        };

        let bytes =
            fs::read(&abs_path).map_err(|e| ApiError::Internal(format!("read {rel_path}: {e}")))?;
        let on_disk_sha = sha256_bytes(&bytes);
        if on_disk_sha != req.sha256 {
            return Err(ApiError::conflict(format!(
                "sha256 mismatch for {rel_path}: expected {}, on-disk {on_disk_sha}",
                req.sha256
            )));
        }

        let ts = unix_secs_to_iso8601_utc(ctx.at.0 / 1_000);
        let ack_json = serde_json::json!({
            "schema": 1,
            "seat": acker_seat,
            "by": ctx.by.to_string(),
            "path": rel_path,
            "sha256_full": req.sha256.as_str(),
            "sha256": &req.sha256.as_str()[..16],
            "ts": ts,
        });

        let mut st = self.state.lock().await;
        let state_dir = self.root.ensure_dir(&["state"])?;
        let acks_file = state_dir.join("acks.jsonl");
        if let Ok(sm) = fs::symlink_metadata(&acks_file)
            && sm.file_type().is_symlink()
        {
            return Err(ApiError::Denied {
                principal: "mailbridge".into(),
                action: format!("write through symlink {}", acks_file.display()),
            });
        }

        if !is_acked_in_sandbox(&self.root, &acker_seat, &rel_path, &req.sha256) {
            let mut f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&acks_file)
                .map_err(|e| ApiError::Internal(format!("open acks.jsonl: {e}")))?;
            let mut line =
                serde_json::to_vec(&ack_json).map_err(|e| ApiError::Internal(e.to_string()))?;
            line.push(b'\n');
            f.write_all(&line)
                .map_err(|e| ApiError::Internal(format!("append acks.jsonl: {e}")))?;
            f.sync_all()
                .map_err(|e| ApiError::Internal(format!("fsync acks.jsonl: {e}")))?;
            fsync_dir(&state_dir).map_err(ApiError::Internal)?;
        }

        let idem_hash = sha256_short16(&format!("ack/1\n{acker_seat}\n{rel_path}\n{}", req.sha256));
        let idem = IdemKey::parse(format!("ack:{idem_hash}"))?;
        let appended = st
            .writer
            .append(
                &ctx.by,
                ctx.at,
                Some(&idem),
                vec![Event::Mail(MailEvent::Acked {
                    path: PathBuf::from(&rel_path),
                    sha256: req.sha256,
                })],
            )
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        drop(st);
        self.publish_fresh(&appended);
        Ok(Ack)
    }

    async fn wake_bell(&self, req: MailWakeBell, ctx: DecideCtx) -> Result<Ack, ApiError> {
        let sched = req.bell_id.trim();
        if sched.is_empty() || sched.chars().any(char::is_control) {
            return Err(ApiError::Invalid(InvalidId {
                what: "bell_id".into(),
                reason: "must be non-empty without control characters".into(),
            }));
        }
        if req.fired_at.chars().any(char::is_control) {
            return Err(ApiError::Invalid(InvalidId {
                what: "fired_at".into(),
                reason: "must not contain control characters".into(),
            }));
        }
        let body = room::Body::parse(&req.prompt)?;
        let canonical = canonical_bell_key(sched, &req.fired_at);
        let digest16 = bell_digest16(&req.seat, &canonical);
        let idem = IdemKey::parse(format!("bell:{digest16}"))?;

        let bell_doc = serde_json::json!({
            "bell_id": canonical,
            "schedule_id": sched,
            "mark": req.fired_at.trim(),
            "seat": req.seat.as_str(),
            "prompt": body.as_str(),
        });
        let mut bell_bytes =
            serde_json::to_vec_pretty(&bell_doc).map_err(|e| ApiError::Internal(e.to_string()))?;
        bell_bytes.push(b'\n');

        let mut st = self.state.lock().await;
        if st
            .writer
            .lookup(&idem)
            .map_err(|e| ApiError::Internal(e.to_string()))?
            .is_some()
        {
            return Ok(Ack);
        }

        let bells_dir = self.root.ensure_dir(&["state", "bells"])?;
        let bell_file = format!("{digest16}.json");
        if !bells_dir.join(&bell_file).exists() {
            atomic_write_if_absent(&bells_dir, &bell_file, &bell_bytes)?;
        }

        let appended = st
            .writer
            .append(
                &ctx.by,
                ctx.at,
                Some(&idem),
                vec![Event::Mail(MailEvent::BellRang {
                    bell_id: canonical,
                    seat: req.seat,
                })],
            )
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        drop(st);
        self.publish_fresh(&appended);
        Ok(Ack)
    }

    pub async fn rescan(&self) -> Result<u32, ApiError> {
        let entries =
            fs::read_dir(self.root.path()).map_err(|e| ApiError::Internal(e.to_string()))?;
        let mut boxes = Vec::new();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };
            let Some(seat_raw) = name_str.strip_prefix("to-") else {
                continue;
            };
            let Ok(seat) = SeatName::parse(seat_raw) else {
                continue;
            };
            if entry.file_type().is_ok_and(|ft| ft.is_dir()) {
                boxes.push((seat, name_str.to_owned(), entry.path()));
            }
        }
        boxes.sort_by(|a, b| a.1.cmp(&b.1));

        let mut projected_count = 0u32;
        let by = Principal::System {
            part: SystemPart::MailBridge,
        };

        for (seat, box_name, box_path) in boxes {
            let Ok(files) = fs::read_dir(&box_path) else {
                continue;
            };
            let mut md_files = Vec::new();
            for f in files.flatten() {
                let fname = f.file_name();
                let Some(fname_str) = fname.to_str() else {
                    continue;
                };
                if fname_str.starts_with('.') || !fname_str.ends_with(".md") {
                    continue;
                }
                if f.file_type().is_ok_and(|ft| ft.is_file()) {
                    md_files.push((fname_str.to_owned(), f.path()));
                }
            }
            md_files.sort_by(|a, b| a.0.cmp(&b.0));

            for (file_name, file_path) in md_files {
                let bytes = fs::read(&file_path).map_err(|e| ApiError::Internal(e.to_string()))?;
                let item = project(&self.root, &file_path, &bytes)
                    .map_err(|e| ApiError::Internal(e.to_string()))?;
                let wid = wake_id(&seat, &box_name, &file_name, &item.reference.sha256);
                let idem = IdemKey::parse(format!("proj:{}", wid.0))?;

                let mut st = self.state.lock().await;
                if st.seen_wakes.contains(&wid)
                    || st
                        .writer
                        .lookup(&idem)
                        .map_err(|e| ApiError::Internal(e.to_string()))?
                        .is_some()
                {
                    st.seen_wakes.insert(wid);
                    continue;
                }
                let at = UnixMs(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_or(0, |d| d.as_millis() as u64),
                );
                let appended = st
                    .writer
                    .append(
                        &by,
                        at,
                        Some(&idem),
                        vec![Event::Mail(MailEvent::Projected { item })],
                    )
                    .map_err(|e| ApiError::Internal(e.to_string()))?;
                st.seen_wakes.insert(wid);
                drop(st);
                if matches!(appended, Appended::Fresh(_)) {
                    projected_count += 1;
                }
                self.publish_fresh(&appended);
            }
        }
        Ok(projected_count)
    }
}

impl Service<MailCommand> for MailBridge {
    fn dispatch(
        &self,
        cmd: MailCommand,
        ctx: DecideCtx,
    ) -> BoxFut<'_, Result<WireReply, ApiError>> {
        Box::pin(async move {
            match cmd {
                MailCommand::MailPromote(req) => {
                    Ok(WireReply::MailPromote(self.promote(req, ctx).await?))
                }
                MailCommand::MailAck(a) => Ok(WireReply::MailAck(self.ack(a, ctx).await?)),
                MailCommand::MailWakeBell(b) => {
                    Ok(WireReply::MailWakeBell(self.wake_bell(b, ctx).await?))
                }
            }
        })
    }
}

fn is_loopback_host(raw: &str) -> bool {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix("[::1]") {
        return rest.is_empty()
            || rest
                .strip_prefix(':')
                .is_some_and(|p| p.parse::<u16>().is_ok());
    }
    let host = match trimmed.rsplit_once(':') {
        Some((h, port)) if port.parse::<u16>().is_ok() => h,
        _ => trimmed,
    };
    host.eq_ignore_ascii_case("localhost") || host == "127.0.0.1"
}

pub async fn serve_wake_bell_sink<C: Client + 'static>(
    addr: SocketAddr,
    client: C,
) -> std::io::Result<()> {
    if !addr.ip().is_loopback() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "wake-bell sink must bind a loopback address",
        ));
    }
    let listener = TcpListener::bind(addr).await?;
    let client = Arc::new(client);
    let sem = Arc::new(Semaphore::new(MAX_SINK_CONNS));
    loop {
        let (stream, _) = listener.accept().await?;
        let Ok(permit) = sem.clone().acquire_owned().await else {
            break;
        };
        let client = client.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _ = tokio::time::timeout(
                Duration::from_secs(10),
                handle_wake_bell_conn(stream, client.as_ref()),
            )
            .await;
        });
    }
    Ok(())
}

async fn handle_wake_bell_conn<C: Client>(
    mut stream: TcpStream,
    client: &C,
) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    let header_end = loop {
        if buf.len() > MAX_HTTP_HEADER {
            write_http_json(
                &mut stream,
                431,
                "Request Header Fields Too Large",
                &serde_json::json!({"ok": false, "code": "headers_too_large", "detail": "header limit exceeded"}),
            )
            .await?;
            return Ok(());
        }
        let n = match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut chunk)).await {
            Ok(Ok(0)) => return Ok(()),
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Ok(()),
        };
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
    };

    let header_str = String::from_utf8_lossy(&buf[..header_end]);
    let mut lines = header_str.split("\r\n");
    let Some(req_line) = lines.next() else {
        return Ok(());
    };
    let mut parts = req_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    if method != "POST" || path != "/sink" {
        write_http_json(
            &mut stream,
            404,
            "Not Found",
            &serde_json::json!({"ok": false, "code": "not_found", "detail": "expected POST /sink"}),
        )
        .await?;
        return Ok(());
    }

    let mut content_length = 0usize;
    let mut content_type_json = false;
    let mut host_ok = false;
    for line in lines {
        if let Some((name, val)) = line.split_once(':') {
            let header = name.trim();
            if header.eq_ignore_ascii_case("content-length")
                && let Ok(len) = val.trim().parse::<usize>()
            {
                content_length = len;
            } else if header.eq_ignore_ascii_case("content-type") {
                let media = val.split(';').next().unwrap_or("").trim();
                content_type_json = media.eq_ignore_ascii_case("application/json");
            } else if header.eq_ignore_ascii_case("host") {
                host_ok = is_loopback_host(val);
            }
        }
    }

    if content_length > MAX_HTTP_BODY {
        write_http_json(
            &mut stream,
            413,
            "Payload Too Large",
            &serde_json::json!({"ok": false, "code": "payload_too_large", "detail": "body limit exceeded"}),
        )
        .await?;
        return Ok(());
    }

    let body_offset = header_end + 4;
    while buf.len() < body_offset + content_length {
        let need = (body_offset + content_length - buf.len()).min(chunk.len());
        let n = match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut chunk[..need]))
            .await
        {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(e),
            Err(_) => break,
        };
        buf.extend_from_slice(&chunk[..n]);
    }

    if !host_ok {
        write_http_json(
            &mut stream,
            403,
            "Forbidden",
            &serde_json::json!({"ok": false, "code": "forbidden_host", "detail": "Host must be loopback"}),
        )
        .await?;
        return Ok(());
    }

    if !content_type_json {
        write_http_json(
            &mut stream,
            415,
            "Unsupported Media Type",
            &serde_json::json!({"ok": false, "code": "unsupported_media_type", "detail": "Content-Type must be application/json"}),
        )
        .await?;
        return Ok(());
    }

    if buf.len() < body_offset + content_length {
        write_http_json(
            &mut stream,
            400,
            "Bad Request",
            &serde_json::json!({"ok": false, "code": "truncated_body", "detail": "incomplete request body"}),
        )
        .await?;
        return Ok(());
    }

    let payload: MailWakeBell =
        match serde_json::from_slice(&buf[body_offset..body_offset + content_length]) {
            Ok(p) => p,
            Err(e) => {
                write_http_json(
                &mut stream,
                400,
                "Bad Request",
                &serde_json::json!({"ok": false, "code": "invalid_json", "detail": e.to_string()}),
            )
            .await?;
                return Ok(());
            }
        };

    match client.call(payload).await {
        Ok(Ack) => {
            write_http_json(&mut stream, 200, "OK", &serde_json::json!({"ok": true})).await?;
        }
        Err(e) => {
            write_http_json(
                &mut stream,
                422,
                "Unprocessable Entity",
                &serde_json::json!({"ok": false, "code": "dispatch_failed", "detail": e.to_string()}),
            )
            .await?;
        }
    }
    Ok(())
}

async fn write_http_json(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &serde_json::Value,
) -> std::io::Result<()> {
    let payload = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;
    stream.shutdown().await
}

async fn deliver_bell_dm(
    client: &LocalClient,
    seat: SeatName,
    bell_id: String,
) -> Result<(), ApiError> {
    let digest16 = bell_digest16(&seat, &bell_id);
    let idem = IdemKey::parse(format!("bell:{digest16}"))?;
    let prompt = read_bell_prompt_any_sandbox(&digest16).ok_or_else(|| ApiError::NotFound {
        what: format!("banked wake bell {bell_id} for @{seat}"),
    })?;
    let body = room::Body::parse(prompt)?;
    client
        .call(room::RoomPost {
            idem: Some(idem),
            ..room::RoomPost::new(
                room::RoomRef::Dm {
                    with: Principal::Seat { seat },
                },
                body,
            )
        })
        .await?;
    Ok(())
}

pub struct BellCourier;

impl Reactor for BellCourier {
    const NAME: &'static str = "bell-courier";

    fn interest(&self) -> Interest {
        Interest::Streams {
            streams: vec![StreamId::Mail],
        }
    }

    fn react(
        &mut self,
        frame: Frame,
        client: &LocalClient,
    ) -> impl Future<Output = Result<(), ApiError>> + Send {
        let client = client.clone();
        async move {
            match frame {
                Frame::Committed(Envelope {
                    event: Event::Mail(MailEvent::BellRang { bell_id, seat }),
                    ..
                }) => deliver_bell_dm(&client, seat, bell_id).await,
                _ => Ok(()),
            }
        }
    }
}
