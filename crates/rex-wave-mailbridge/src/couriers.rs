//! Executable `couriers.d/` output-adapter engine, multi-machine CRDT route sync,
//! and daemon [`CourierHub`] reactor conforming to `specs/mail/MailHub.tla` and
//! `specs/mail/MailMachines.tla`.
//!
//! Security & formal invariants enforced here:
//! - **Body-blind claims:** [`DeliveryClaim`] carries metadata referents only
//!   (`sha256`, `id`, `from`, `subject`, `path`, `priority`, `created_at_ms`),
//!   never raw letter bodies, and redacts any active `canary_token` from adapter
//!   diagnostics before persistence or escalation.
//! - **Fail-closed `protected-roots` & descriptor-bound execution:** every adapter
//!   in `couriers.d/` is verified with [`ProtectedRootsGuard`] (failing closed if
//!   guard loading or resolution fails) and staged from an `O_NOFOLLOW` descriptor
//!   into a private `0700` directory via [`rex_wave_fs::stage_verified_executable`]
//!   so pathname replacement between verification and `spawn` cannot alter execution.
//! - **Bounded pipe drain & process-group reaping:** adapters run in their own
//!   process group (`Command::process_group(0)`, zero `unsafe`) under a single
//!   wall-clock deadline covering `wait` and stdout/stderr pipe drain, with
//!   unconditional `SIGKILL` cleanup for lingering background descendants.
//! - **`OneMachineRings`:** [`select_binding_for_delivery_on_machine`] resolves one
//!   global winner across all machines and lanes before checking `local_machine_id`.
//! - **`NoDoubleRing` + `DegradedLoud` + Write-Ahead Claim:** a pre-dispatch
//!   `MailEvent::RingDispatched` claim is committed to `StreamId::Mail` before
//!   spawning any courier; unfinished claims on restart recover as `"unknown"`,
//!   which counts as rung on `(seat, sha256)` and escalates `AdapterDegraded` after
//!   `degraded_threshold` consecutive unknowns.
//! - **Canary-gated routes (`MailHub.tla`):** `BindingStatus::PendingCanary`
//!   bindings receive only a canary ring (with backoff retry on `"deferred"` or
//!   `"unknown"`) and are never eligible for normal mail until confirmed.
//! - **Incarnation-scoped `refused` + Durable Idempotent Follow-Up:** on the first
//!   `"refused"` outcome, the dead `(seat, incarnation_id, epoch)` is immediately
//!   fenced and expired with zero retries, `MailEvent::DeliveryFailed` is committed
//!   atomically with `RingAttempted`, and `PresenceExpire` + `MailSend` + `InboxRaise`
//!   are reconciled idempotently across restarts without ever interrupting `Operator`.

use crate::acks::normalize_seat_token;
use crate::dissolved::{CommittedMailEntry, DEFAULT_COURIER_DEGRADED_THRESHOLD, MailState};
use crate::presence::{
    IncarnationAffinity, Presence, PresenceState, select_binding_for_delivery,
    select_binding_for_delivery_on_machine,
};
use crate::sha256_short16;
use rex_wave_api::config::RexConfig;
use rex_wave_api::mail::{
    CourierOutcome, DeliveryFailureKind, LetterPriority, MailDeliveryFailure, MailEvent,
    MailFailureGroup, MailRecordRing, MailSend, RingRecordPhase,
};
use rex_wave_api::notify::{InboxRaise, NotificationDraft, NotifyKind};
use rex_wave_api::presence::{
    BindingStatus, HouseProfile, PresenceEvent, PresenceExpire, PresenceList, SeatBinding,
    TransportEndpoint,
};
use rex_wave_api::*;
use rex_wave_fs::{EntryKind, ProtectedRootsGuard, Protection, SafeName, load_protected_roots};
use rex_wave_runtime::{Decider, EntryMeta, LocalClient, Reactor};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, watch};

/// Wire schema version for body-blind [`DeliveryClaim`] payloads sent on courier stdin.
pub const COURIER_CLAIM_SCHEMA: u8 = 1;
/// Default consecutive `"unknown"` outcomes before marking a courier degraded (`MailMachines.tla` `K`).
pub const DEFAULT_DEGRADED_THRESHOLD: u32 = DEFAULT_COURIER_DEGRADED_THRESHOLD;
/// Default maximum `"deferred"` retries before loud `GiveUp` (`MailHub.tla` `MaxTries`).
pub const DEFAULT_MAX_DEFER_TRIES: u32 = rex_wave_api::config::DEFAULT_MAIL_MAX_DEFER_TRIES;
/// Default base backoff before retrying a `"deferred"` ring (30 seconds).
pub const DEFAULT_DEFER_BACKOFF_MS: u64 = rex_wave_api::config::DEFAULT_MAIL_DEFER_BACKOFF_MS;
/// Per-step cap on exponential `"deferred"` retry delay (5 minutes, unless base is larger).
const MAX_DEFER_STEP_DELAY_MS: u64 = 300_000;
/// Maximum letter referents batched in one courier ring claim.
pub const DEFAULT_MAX_REFERENTS_PER_RING: usize = 32;
/// Default wall-clock timeout enforced per courier script invocation.
pub const DEFAULT_COURIER_TIMEOUT_MS: u64 = 10_000;
/// Grace window between process-group `SIGTERM` and `SIGKILL` on timeout.
const KILL_GRACE_MS: u64 = 200;
/// Post-exit grace window for stdout/stderr pipes before reaping lingering descendants.
const PIPE_DRAIN_GRACE_MS: u64 = 100;
/// Maximum adapter binary/script size staged for execution (16 MiB).
const MAX_COURIER_EXECUTABLE_BYTES: u64 = 16 * 1024 * 1024;
/// Hard cap on stdout bytes read from a courier script.
pub const MAX_COURIER_STDOUT_BYTES: usize = 64 * 1024;
/// Hard cap on stderr bytes captured for failure diagnostics.
const MAX_COURIER_STDERR_BYTES: usize = 8 * 1024;
/// Sender address stamped on system delivery-failure notices committed by [`CourierHub`].
pub const COURIER_HUB_SENDER_ADDRESS: &str = "system/courier_hub";

static DISPATCH_NONCE: AtomicU64 = AtomicU64::new(1);

fn next_dispatch_id(machine_id: &str) -> String {
    let n = DISPATCH_NONCE.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let ts = rex_wave_runtime::now().0;
    format!("disp-{machine_id}-{pid}-{ts}-{n}")
}

/// Body-blind metadata referent for one letter in a [`DeliveryClaim`].
/// Never contains raw letter bodies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryReferent {
    pub sha256: Sha256Hex,
    pub id: String,
    pub from: String,
    pub subject: String,
    pub path: PathBuf,
    pub priority: LetterPriority,
    pub created_at_ms: u64,
}

impl DeliveryReferent {
    pub fn from_entry(entry: &CommittedMailEntry) -> Self {
        let m = &entry.message;
        Self {
            sha256: m.sha256.clone(),
            id: m.id.clone(),
            from: m.from.clone(),
            subject: m.subject.clone(),
            path: m.path.clone(),
            priority: m.priority,
            created_at_ms: m.created_at.0,
        }
    }
}

/// Body-blind JSON payload written to a `couriers.d/<adapter>` process's `stdin`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryClaim {
    pub schema: u8,
    pub seat: SeatName,
    pub machine_id: String,
    pub incarnation_id: IncarnationId,
    pub endpoint: TransportEndpoint,
    pub referents: Vec<DeliveryReferent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canary_token: Option<String>,
}

impl DeliveryClaim {
    /// Builds a body-blind mail delivery claim bounded to `max_referents`.
    pub fn for_mail(
        binding: &SeatBinding,
        machine_id: impl Into<String>,
        entries: &[&CommittedMailEntry],
        max_referents: usize,
    ) -> Self {
        let cap = max_referents.max(1);
        let referents = entries
            .iter()
            .take(cap)
            .map(|e| DeliveryReferent::from_entry(e))
            .collect();
        Self {
            schema: COURIER_CLAIM_SCHEMA,
            seat: binding.seat.clone(),
            machine_id: machine_id.into(),
            incarnation_id: binding.incarnation_id.clone(),
            endpoint: binding.endpoint.clone(),
            referents,
            canary_token: None,
        }
    }

    /// Builds a body-blind canary verification claim for a `PendingCanary` binding.
    pub fn for_canary(binding: &SeatBinding, machine_id: impl Into<String>) -> Self {
        Self {
            schema: COURIER_CLAIM_SCHEMA,
            seat: binding.seat.clone(),
            machine_id: machine_id.into(),
            incarnation_id: binding.incarnation_id.clone(),
            endpoint: binding.endpoint.clone(),
            referents: Vec::new(),
            canary_token: binding.canary_token.clone(),
        }
    }
}

/// Parsed reply returned by a `couriers.d/<adapter>` process on `stdout`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CourierReply {
    pub outcome: CourierOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// `true` when the failure occurred before spawning any child process, so normal
    /// mail retains push eligibility rather than burning `(seat, sha256)` (`Opus S2`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub pre_spawn: bool,
}

fn redact_raw_secret(text: String, secret: Option<&str>) -> String {
    match secret.map(str::trim).filter(|s| !s.is_empty()) {
        Some(tok) => text.replace(tok, "[REDACTED_CANARY]"),
        None => text,
    }
}

impl CourierReply {
    pub fn unknown(reason: impl Into<String>, detail: Option<String>) -> Self {
        Self {
            outcome: CourierOutcome::Unknown,
            reason: Some(reason.into()),
            detail,
            pre_spawn: false,
        }
    }

    pub fn pre_spawn_unknown(reason: impl Into<String>, detail: Option<String>) -> Self {
        Self {
            outcome: CourierOutcome::Unknown,
            reason: Some(reason.into()),
            detail,
            pre_spawn: true,
        }
    }

    fn redact_secret(mut self, secret: Option<&str>) -> Self {
        let Some(tok) = secret.map(str::trim).filter(|s| !s.is_empty()) else {
            return self;
        };
        if let Some(r) = self.reason.take() {
            self.reason = clean_opt_text(Some(r.replace(tok, "[REDACTED_CANARY]")), 128);
        }
        if let Some(d) = self.detail.take() {
            self.detail = clean_opt_text(Some(d.replace(tok, "[REDACTED_CANARY]")), 512);
        }
        self
    }
}

#[derive(Deserialize)]
struct RawCourierReply {
    outcome: CourierOutcome,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    detail: Option<String>,
}

fn clean_opt_text(raw: Option<String>, max_len: usize) -> Option<String> {
    let s = raw?.trim().to_owned();
    if s.is_empty() {
        return None;
    }
    let cleaned: String = s
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .take(max_len)
        .collect();
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

/// Parses courier `stdout` into a [`CourierReply`]. Malformed JSON, empty output,
/// or unrecognized outcome values fail safe to [`CourierOutcome::Unknown`] so an
/// ambiguous delivery is never double-rung.
pub fn parse_courier_stdout(raw: &[u8]) -> CourierReply {
    parse_courier_stdout_redacted(raw, None)
}

fn parse_courier_stdout_redacted(raw: &[u8], secret: Option<&str>) -> CourierReply {
    let Ok(text) = std::str::from_utf8(raw) else {
        return CourierReply::unknown(
            "malformed-stdout",
            Some("stdout was not valid UTF-8".into()),
        );
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return CourierReply::unknown("empty-stdout", Some("adapter produced empty stdout".into()));
    }
    let parsed = serde_json::from_str::<RawCourierReply>(trimmed).or_else(|first_err| {
        let last_line = trimmed
            .lines()
            .rev()
            .find(|l| !l.trim().is_empty())
            .unwrap_or(trimmed);
        serde_json::from_str::<RawCourierReply>(last_line.trim()).map_err(|_| first_err)
    });
    match parsed {
        Ok(reply) => CourierReply {
            outcome: reply.outcome,
            reason: clean_opt_text(reply.reason.map(|r| redact_raw_secret(r, secret)), 128),
            detail: clean_opt_text(reply.detail.map(|d| redact_raw_secret(d, secret)), 512),
            pre_spawn: false,
        },
        Err(err) => CourierReply::unknown(
            "malformed-stdout",
            clean_opt_text(
                Some(redact_raw_secret(
                    format!("invalid courier JSON reply: {err}"),
                    secret,
                )),
                512,
            ),
        ),
    }
}

/// Maps a [`TransportEndpoint`] to its `couriers.d/` executable adapter name.
///
/// [`TransportEndpoint::SelfWatcher`] (`own_knocker`) always returns `None` so
/// self-watching seats age unacked mail without invoking external push couriers.
pub fn resolve_adapter_name(endpoint: &TransportEndpoint) -> Option<&'static str> {
    match endpoint {
        TransportEndpoint::T3Thread { .. } => Some("t3-thread"),
        TransportEndpoint::ClaudeSession { .. } => Some("claude-session"),
        TransportEndpoint::BbThread { .. } => Some("bb-thread"),
        TransportEndpoint::OperatorToast { .. } => Some("macos-toast"),
        TransportEndpoint::SelfWatcher { .. } => None,
        TransportEndpoint::UdsSession { client, .. } => {
            let norm = client.trim().to_ascii_lowercase();
            if norm == "codex-app-server"
                || norm == "codex"
                || norm.starts_with("codex-app-server:")
                || norm.starts_with("codex:")
            {
                Some("codex-app-server")
            } else {
                None
            }
        }
    }
}

/// One discovered entry inside `couriers.d/`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveredCourier {
    pub name: String,
    pub path: PathBuf,
    pub executable: bool,
}

fn is_executable_file(path: &Path) -> Result<bool, std::io::Error> {
    let (_file, meta) = rex_wave_fs::open_regular_nofollow(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        Ok((meta.permissions().mode() & 0o111) != 0)
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        Ok(true)
    }
}

/// Discovers regular non-symlink entries in `dir`, rejecting directories or files
/// inside `protected-roots` when `guard` is provided.
pub fn discover_couriers_dir(
    dir: &Path,
    guard: Option<&ProtectedRootsGuard>,
) -> Vec<DiscoveredCourier> {
    if rex_wave_fs::entry_kind(dir).ok() != Some(EntryKind::Dir) {
        return Vec::new();
    }
    let verified_dir = match guard {
        Some(g) => match g.verify_dir(dir) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        },
        None => dir.to_path_buf(),
    };
    let Ok(rd) = std::fs::read_dir(&verified_dir) else {
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
        if SafeName::parse(name).is_err() {
            continue;
        }
        let full = entry.path();
        if rex_wave_fs::entry_kind(&full).ok() != Some(EntryKind::File) {
            continue;
        }
        if let Some(g) = guard
            && g.check(&full, &full).is_err()
        {
            continue;
        }
        let executable = is_executable_file(&full).unwrap_or(false);
        out.push(DiscoveredCourier {
            name: name.to_owned(),
            path: full,
            executable,
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Resolves one named executable adapter inside `couriers_dir` with descriptor-anchored
/// `O_DIRECTORY | O_NOFOLLOW` + `protected-roots` verification.
pub fn resolve_courier_executable(
    dir: &Path,
    adapter_name: &str,
    guard: Option<&ProtectedRootsGuard>,
) -> Result<Option<PathBuf>, String> {
    SafeName::parse(adapter_name)
        .map_err(|e| format!("invalid courier adapter name '{adapter_name}': {e}"))?;
    match rex_wave_fs::entry_kind(dir) {
        Ok(EntryKind::Dir) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Ok(_) => {
            return Err(format!(
                "couriers_dir {} is not a real directory",
                dir.display()
            ));
        }
        Err(e) => return Err(format!("inspect couriers_dir {}: {e}", dir.display())),
    }
    let (dir_file, verified_dir) = match guard {
        Some(g) => g
            .open_verified_dir(dir)
            .map_err(|e| format!("couriers_dir rejected by protected-roots: {e}"))?,
        None => {
            let f = rex_wave_fs::open_dir_nofollow(dir)
                .map_err(|e| format!("open couriers_dir {}: {e}", dir.display()))?;
            (f, dir.to_path_buf())
        }
    };
    let full = verified_dir.join(adapter_name);
    if let Some(g) = guard
        && let Err(e) = g.check(&full, &full)
    {
        return Err(format!("courier adapter rejected by protected-roots: {e}"));
    }
    match rex_wave_fs::open_regular_at(&dir_file, adapter_name) {
        Ok((_file, meta)) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                if (meta.permissions().mode() & 0o111) != 0 {
                    Ok(Some(full))
                } else {
                    Ok(None)
                }
            }
            #[cfg(not(unix))]
            {
                let _ = meta;
                Ok(Some(full))
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("open courier {}: {e}", full.display())),
    }
}

async fn read_bounded_into<R: AsyncRead + Unpin>(
    mut reader: R,
    dst: Arc<std::sync::Mutex<(Vec<u8>, bool)>>,
    limit: usize,
) {
    let mut buf = [0u8; 4096];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let mut guard = dst.lock().unwrap_or_else(|e| e.into_inner());
                let avail = limit.saturating_sub(guard.0.len());
                if avail > 0 {
                    let take = avail.min(n);
                    guard.0.extend_from_slice(&buf[..take]);
                    if n > take {
                        guard.1 = true;
                    }
                } else {
                    guard.1 = true;
                }
            }
            Err(_) => break,
        }
    }
}

#[cfg(unix)]
fn signal_process_group(raw_pid: u32, sig: rustix::process::Signal) {
    if let Ok(raw_i32) = i32::try_from(raw_pid)
        && let Some(pid) = rustix::process::Pid::from_raw(raw_i32)
    {
        let _ = rustix::process::kill_process_group(pid, sig);
    }
}

async fn reap_process_group_hard(child_pid: Option<u32>, grace_ms: u64) {
    #[cfg(unix)]
    if let Some(pid) = child_pid {
        // Post-wait group signaling reaps descendants that stayed in `child_pid`'s
        // process group. A grandchild that called `setsid()` leaves the group (leaving
        // only the negligible theoretical PGID-reuse window after leader wait), which is
        // already bounded on the caller side by `PIPE_DRAIN_GRACE_MS` + reader `abort()`.
        signal_process_group(pid, rustix::process::Signal::TERM);
        if grace_ms > 0 {
            tokio::time::sleep(Duration::from_millis(grace_ms)).await;
        }
        signal_process_group(pid, rustix::process::Signal::KILL);
    }
    #[cfg(not(unix))]
    {
        let _ = (child_pid, grace_ms);
    }
}

struct CourierSubprocessGuard {
    armed: bool,
    child_pid: Option<u32>,
    stdout_handle: tokio::task::AbortHandle,
    stderr_handle: tokio::task::AbortHandle,
}

impl Drop for CourierSubprocessGuard {
    fn drop(&mut self) {
        if self.armed {
            #[cfg(unix)]
            if let Some(pid) = self.child_pid {
                signal_process_group(pid, rustix::process::Signal::KILL);
            }
            self.stdout_handle.abort();
            self.stderr_handle.abort();
        }
    }
}

/// Spawns a verified `couriers.d/<adapter>` executable in its own process group,
/// binding execution to the `O_NOFOLLOW` descriptor via [`rex_wave_fs::stage_verified_executable`],
/// enforcing `ProtectedRootsGuard` fail-closed, bounding both `child.wait()` and pipe
/// drain under `timeout`, and redacting any active `canary_token` from diagnostics.
pub async fn invoke_courier_script(
    path: &Path,
    claim: &DeliveryClaim,
    timeout: Duration,
    guard: Option<&ProtectedRootsGuard>,
) -> CourierReply {
    let secret = claim.canary_token.as_deref();
    let owned_guard: ProtectedRootsGuard;
    let effective_guard: &ProtectedRootsGuard = match guard {
        Some(g) => g,
        None => match load_protected_roots().and_then(|p| ProtectedRootsGuard::resolve(&p)) {
            Ok(g) => {
                owned_guard = g;
                &owned_guard
            }
            Err(e) => {
                return CourierReply::pre_spawn_unknown(
                    "protected-roots-unavailable",
                    Some(format!("failed to load protected-roots: {e}")),
                )
                .redact_secret(secret);
            }
        },
    };
    invoke_courier_script_guarded(path, claim, timeout, effective_guard).await
}

async fn invoke_courier_script_guarded(
    path: &Path,
    claim: &DeliveryClaim,
    timeout: Duration,
    guard: &ProtectedRootsGuard,
) -> CourierReply {
    let secret = claim.canary_token.as_deref();
    if rex_wave_fs::entry_kind(path).ok() != Some(EntryKind::File) {
        return CourierReply::pre_spawn_unknown(
            "invalid-adapter",
            Some(format!("{} is not a regular file", path.display())),
        )
        .redact_secret(secret);
    }
    if let Err(e) = guard.check(path, path) {
        return CourierReply::pre_spawn_unknown(
            "protected-root",
            Some(format!("adapter rejected by protected-roots: {e}")),
        )
        .redact_secret(secret);
    }
    let Some(parent) = path.parent() else {
        return CourierReply::pre_spawn_unknown(
            "invalid-adapter",
            Some(format!("{} has no parent directory", path.display())),
        )
        .redact_secret(secret);
    };
    let Some(leaf) = path.file_name().and_then(|n| n.to_str()) else {
        return CourierReply::pre_spawn_unknown(
            "invalid-adapter",
            Some(format!("{} has invalid UTF-8 file name", path.display())),
        )
        .redact_secret(secret);
    };
    let (parent_dir_file, canon_parent) = match guard.open_verified_dir(parent) {
        Ok(pair) => pair,
        Err(e) => {
            return CourierReply::pre_spawn_unknown(
                "protected-root",
                Some(format!("adapter parent rejected by protected-roots: {e}")),
            )
            .redact_secret(secret);
        }
    };
    let canon_candidate = canon_parent.join(leaf);
    if let Err(e) = guard.check(&canon_candidate, path) {
        return CourierReply::pre_spawn_unknown(
            "protected-root",
            Some(format!("adapter rejected by protected-roots: {e}")),
        )
        .redact_secret(secret);
    }

    let staged = match rex_wave_fs::stage_verified_executable_at(
        &parent_dir_file,
        &canon_parent,
        leaf,
        MAX_COURIER_EXECUTABLE_BYTES,
    ) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            return CourierReply::pre_spawn_unknown("not-executable", Some(e.to_string()))
                .redact_secret(secret);
        }
        Err(e) => {
            return CourierReply::pre_spawn_unknown("open-failed", Some(e.to_string()))
                .redact_secret(secret);
        }
    };
    drop(parent_dir_file);

    let mut payload = match serde_json::to_vec(claim) {
        Ok(bytes) => bytes,
        Err(e) => {
            return CourierReply::pre_spawn_unknown("serialize-failed", Some(e.to_string()))
                .redact_secret(secret);
        }
    };
    payload.push(b'\n');

    let mut cmd = tokio::process::Command::new(staged.path());
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .env_remove("REX_WAVE_TOKEN")
        .env_remove("REX_WAVE_BB_TOKEN")
        .env_remove("REX_WAVE_T3_TOKEN")
        .env_remove("REX_WAVE_WORKER_THREAD_ID")
        .env("REX_WAVE_COURIER_SCHEMA", COURIER_CLAIM_SCHEMA.to_string())
        .env("REX_WAVE_COURIER_ORIGIN", &canon_candidate)
        .env("REX_WAVE_SEAT", claim.seat.as_str())
        .env("REX_WAVE_MACHINE_ID", &claim.machine_id)
        .env("REX_WAVE_INCARNATION_ID", claim.incarnation_id.as_str());
    #[cfg(unix)]
    {
        cmd.process_group(0);
    }

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return CourierReply::pre_spawn_unknown(
                "spawn-failed",
                Some(format!("spawn {}: {e}", canon_candidate.display())),
            )
            .redact_secret(secret);
        }
    };
    let child_pid = child.id();

    let mut stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let stdout_buf = Arc::new(std::sync::Mutex::new((Vec::new(), false)));
    let stderr_buf = Arc::new(std::sync::Mutex::new((Vec::new(), false)));
    let stdout_buf_task = stdout_buf.clone();
    let stderr_buf_task = stderr_buf.clone();

    let stdout_task = tokio::spawn(async move {
        if let Some(r) = stdout {
            read_bounded_into(r, stdout_buf_task, MAX_COURIER_STDOUT_BYTES).await;
        }
    });
    let stderr_task = tokio::spawn(async move {
        if let Some(r) = stderr {
            read_bounded_into(r, stderr_buf_task, MAX_COURIER_STDERR_BYTES).await;
        }
    });
    let mut subproc_guard = CourierSubprocessGuard {
        armed: true,
        child_pid,
        stdout_handle: stdout_task.abort_handle(),
        stderr_handle: stderr_task.abort_handle(),
    };

    let deadline = tokio::time::Instant::now() + timeout;
    let wait_fut = async {
        if let Some(mut s) = stdin.take() {
            let _ = s.write_all(&payload).await;
            let _ = s.shutdown().await;
            drop(s);
        }
        child.wait().await
    };

    let reply = match tokio::time::timeout_at(deadline, wait_fut).await {
        Ok(Ok(status)) => {
            // Bound stdout/stderr drain so a background descendant holding a pipe open
            // can never stall CourierHub past `PIPE_DRAIN_GRACE_MS` or `deadline`.
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let drain_budget = Duration::from_millis(PIPE_DRAIN_GRACE_MS).min(remaining);
            let drain_both = async {
                let _ = stdout_task.await;
                let _ = stderr_task.await;
            };
            if tokio::time::timeout(drain_budget, drain_both)
                .await
                .is_err()
            {
                reap_process_group_hard(child_pid, 50).await;
                subproc_guard.stdout_handle.abort();
                subproc_guard.stderr_handle.abort();
            }
            subproc_guard.armed = false;

            let (stdout_bytes, stdout_truncated) =
                stdout_buf.lock().unwrap_or_else(|e| e.into_inner()).clone();
            let (stderr_bytes, stderr_truncated) =
                stderr_buf.lock().unwrap_or_else(|e| e.into_inner()).clone();
            if !status.success() {
                let code_str = status
                    .code()
                    .map_or_else(|| "signal".to_owned(), |c| c.to_string());
                let detail = if secret.is_some() && (stderr_truncated || stdout_truncated) {
                    Some(
                        "[diagnostic omitted: output exceeded capture cap during canary ring]"
                            .into(),
                    )
                } else {
                    let stderr_text = String::from_utf8_lossy(&stderr_bytes).trim().to_owned();
                    if !stderr_text.is_empty() {
                        clean_opt_text(Some(redact_raw_secret(stderr_text, secret)), 512)
                    } else {
                        let stdout_text = String::from_utf8_lossy(&stdout_bytes).trim().to_owned();
                        clean_opt_text(Some(redact_raw_secret(stdout_text, secret)), 512)
                    }
                };
                CourierReply::unknown(format!("exit-{code_str}"), detail)
            } else {
                let mut parsed = parse_courier_stdout_redacted(&stdout_bytes, secret);
                if secret.is_some() && stdout_truncated {
                    if parsed.reason.is_some() {
                        parsed.reason = Some("redacted-cap".into());
                    }
                    if parsed.detail.is_some() {
                        parsed.detail = Some(
                            "[diagnostic omitted: output exceeded capture cap during canary ring]"
                                .into(),
                        );
                    }
                }
                parsed
            }
        }
        Ok(Err(e)) => {
            reap_process_group_hard(child_pid, 0).await;
            subproc_guard.stdout_handle.abort();
            subproc_guard.stderr_handle.abort();
            subproc_guard.armed = false;
            CourierReply::unknown("wait-failed", Some(e.to_string()))
        }
        Err(_) => {
            // Router-enforced timeout: SIGTERM -> grace -> unconditional SIGKILL on the
            // entire process group so surviving descendants are always reaped.
            #[cfg(unix)]
            if let Some(pid) = child_pid {
                signal_process_group(pid, rustix::process::Signal::TERM);
            }
            let _ = tokio::time::timeout(Duration::from_millis(KILL_GRACE_MS), child.wait()).await;
            #[cfg(unix)]
            if let Some(pid) = child_pid {
                signal_process_group(pid, rustix::process::Signal::KILL);
            }
            let _ = child.kill().await;
            let _ = tokio::time::timeout(Duration::from_millis(250), child.wait()).await;
            subproc_guard.stdout_handle.abort();
            subproc_guard.stderr_handle.abort();
            subproc_guard.armed = false;
            CourierReply::unknown(
                "timeout",
                Some(format!(
                    "adapter {} exceeded {}ms timeout",
                    canon_candidate.display(),
                    timeout.as_millis()
                )),
            )
        }
    };
    drop(staged);
    reply.redact_secret(secret)
}

/// One seat's machine-owned route claim synchronized across machines as a
/// commutative join-semilattice (`MailMachines.tla`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteClaim {
    pub seat: SeatName,
    pub machine_id: String,
    pub incarnation_id: IncarnationId,
    #[serde(default)]
    pub epoch: u64,
    pub endpoint: TransportEndpoint,
    pub claimed_at: UnixMs,
    pub confirmed: bool,
}

impl RouteClaim {
    /// Builds a [`RouteClaim`] from a [`SeatBinding`], falling back to `fallback_machine_id`
    /// when `binding.machine_id` is absent.
    pub fn from_binding(binding: &SeatBinding, fallback_machine_id: &str) -> Self {
        Self {
            seat: binding.seat.clone(),
            machine_id: binding
                .machine_id
                .clone()
                .unwrap_or_else(|| fallback_machine_id.to_owned()),
            incarnation_id: binding.incarnation_id.clone(),
            epoch: binding.epoch,
            endpoint: binding.endpoint.clone(),
            claimed_at: binding.bound_at,
            confirmed: binding.status == BindingStatus::Live,
        }
    }

    fn total_version_key(&self) -> (UnixMs, u64, &str, &IncarnationId, bool, String) {
        (
            self.claimed_at,
            self.epoch,
            self.machine_id.as_str(),
            &self.incarnation_id,
            self.confirmed,
            serde_json::to_string(&self.endpoint).unwrap_or_default(),
        )
    }
}

/// Per-incarnation versioned withdrawal tombstone recorded when a session disconnects or
/// expires so stale peer merges cannot resurrect that dropped incarnation while preserving
/// newer claims on other machines or incarnations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteWithdrawal {
    pub withdrawn_at: UnixMs,
    #[serde(default)]
    pub epoch: u64,
    pub machine_id: String,
    pub incarnation_id: IncarnationId,
}

impl RouteWithdrawal {
    fn version(&self) -> (UnixMs, u64) {
        (self.withdrawn_at, self.epoch)
    }

    fn dominates(&self, claim: &RouteClaim) -> bool {
        self.machine_id == claim.machine_id
            && self.incarnation_id == claim.incarnation_id
            && (claim.claimed_at, claim.epoch) <= self.version()
    }
}

/// Commutative, associative, idempotent Observed-Remove join-semilattice route table across
/// machines with total `(claimed_at, epoch, machine_id, incarnation_id, confirmed)` version
/// ordering and per-`(seat, machine_id, incarnation_id)` withdrawal tombstones.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteSyncTable {
    candidates: BTreeMap<SeatName, BTreeMap<(String, IncarnationId), RouteClaim>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    withdrawn: BTreeMap<(SeatName, String, IncarnationId), RouteWithdrawal>,
}

impl RouteSyncTable {
    pub fn new() -> Self {
        Self::default()
    }

    fn apply_withdrawal(&mut self, seat: SeatName, w: RouteWithdrawal) -> bool {
        let w_key = (seat.clone(), w.machine_id.clone(), w.incarnation_id.clone());
        let replace_w = match self.withdrawn.get(&w_key) {
            None => true,
            Some(existing) => w.version() > existing.version(),
        };
        let mut changed = false;
        if replace_w {
            self.withdrawn.insert(w_key.clone(), w);
            changed = true;
        }
        if let Some(active_w) = self.withdrawn.get(&w_key) {
            let sub_key = (active_w.machine_id.clone(), active_w.incarnation_id.clone());
            let mut remove_seat_map = false;
            if let Some(seat_map) = self.candidates.get_mut(&seat) {
                if seat_map
                    .get(&sub_key)
                    .is_some_and(|c| active_w.dominates(c))
                {
                    seat_map.remove(&sub_key);
                    changed = true;
                }
                remove_seat_map = seat_map.is_empty();
            }
            if remove_seat_map {
                self.candidates.remove(&seat);
            }
        }
        changed
    }

    /// Inserts or replaces a `(seat, machine_id, incarnation_id)` route claim when it is not
    /// dominated by a withdrawal tombstone for that incarnation and strictly beats any stored
    /// version of that incarnation.
    pub fn apply_claim(&mut self, claim: RouteClaim) -> bool {
        let w_key = (
            claim.seat.clone(),
            claim.machine_id.clone(),
            claim.incarnation_id.clone(),
        );
        if self
            .withdrawn
            .get(&w_key)
            .is_some_and(|w| w.dominates(&claim))
        {
            return false;
        }
        let sub_key = (claim.machine_id.clone(), claim.incarnation_id.clone());
        let seat_map = self.candidates.entry(claim.seat.clone()).or_default();
        match seat_map.get(&sub_key) {
            None => {
                seat_map.insert(sub_key, claim);
                true
            }
            Some(existing) => {
                if claim.total_version_key() > existing.total_version_key() {
                    seat_map.insert(sub_key, claim);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Withdraws all active local claims for `seat` owned by `local_machine_id` immediately
    /// when a local session disconnects (`UdsSession` / `presence hold` exit), recording
    /// per-incarnation withdrawal tombstones while leaving newer claims on other machines intact.
    pub fn drop_local(&mut self, seat: &SeatName, local_machine_id: &str) -> bool {
        let local_claims: Vec<RouteClaim> = self
            .candidates
            .get(seat)
            .map(|m| {
                m.values()
                    .filter(|c| c.machine_id == local_machine_id)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let mut changed = false;
        for c in local_claims {
            changed |= self.apply_withdrawal(
                seat.clone(),
                RouteWithdrawal {
                    withdrawn_at: c.claimed_at,
                    epoch: c.epoch,
                    machine_id: c.machine_id,
                    incarnation_id: c.incarnation_id,
                },
            );
        }
        changed
    }

    /// Withdraws a specific `(seat, machine_id, incarnation_id)` through `withdrawn_at`
    /// without affecting claims from other machines or newer incarnations.
    pub fn drop_local_at(
        &mut self,
        seat: &SeatName,
        local_machine_id: &str,
        incarnation_id: IncarnationId,
        withdrawn_at: UnixMs,
    ) -> bool {
        self.apply_withdrawal(
            seat.clone(),
            RouteWithdrawal {
                withdrawn_at,
                epoch: u64::MAX,
                machine_id: local_machine_id.to_owned(),
                incarnation_id,
            },
        )
    }

    /// Commutative, associative, idempotent semilattice merge from `peer`.
    pub fn merge_from(&mut self, peer: &RouteSyncTable) {
        for ((seat, _, _), w) in &peer.withdrawn {
            self.apply_withdrawal(seat.clone(), w.clone());
        }
        for seat_map in peer.candidates.values() {
            for claim in seat_map.values() {
                self.apply_claim(claim.clone());
            }
        }
    }

    pub fn get(&self, seat: &SeatName) -> Option<&RouteClaim> {
        self.candidates
            .get(seat)?
            .values()
            .max_by_key(|c| c.total_version_key())
    }

    pub fn owner_machine(&self, seat: &SeatName) -> Option<&str> {
        self.get(seat).map(|c| c.machine_id.as_str())
    }

    pub fn is_owned_by_machine(&self, seat: &SeatName, machine_id: &str) -> bool {
        self.get(seat)
            .is_some_and(|c| c.machine_id == machine_id && c.confirmed)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SeatName, &RouteClaim)> {
        self.candidates.iter().filter_map(|(seat, m)| {
            m.values()
                .max_by_key(|c| c.total_version_key())
                .map(|c| (seat, c))
        })
    }
}

/// Per-machine content-addressed mail, ack, and route-claim replica state supporting
/// commutative, associative, idempotent union sync (`MailMachines.tla`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MachineMailSyncState {
    pub machine_id: String,
    pub messages: BTreeSet<Sha256Hex>,
    pub acks: BTreeSet<(SeatName, Sha256Hex)>,
    pub routes: RouteSyncTable,
}

impl MachineMailSyncState {
    pub fn new(machine_id: impl Into<String>) -> Self {
        Self {
            machine_id: machine_id.into(),
            messages: BTreeSet::new(),
            acks: BTreeSet::new(),
            routes: RouteSyncTable::new(),
        }
    }

    /// Union-syncs `messages` and `acks` from `peer` and LWW-merges `routes`.
    pub fn sync_from(&mut self, peer: &MachineMailSyncState) {
        self.messages.extend(peer.messages.iter().cloned());
        self.acks.extend(peer.acks.iter().cloned());
        self.routes.merge_from(&peer.routes);
    }

    /// Returns `true` when this machine owns `seat`'s confirmed route AND has
    /// `sha256` unacked locally (`MailMachines.tla` `MayRing`).
    pub fn may_ring_locally(&self, seat: &SeatName, sha256: &Sha256Hex) -> bool {
        self.messages.contains(sha256)
            && !self.acks.contains(&(seat.clone(), sha256.clone()))
            && self.routes.is_owned_by_machine(seat, &self.machine_id)
    }
}

fn expand_home_prefix(path: &Path) -> PathBuf {
    let Some(raw) = path.to_str() else {
        return path.to_path_buf();
    };
    let trimmed = raw.trim();
    if trimmed == "~" {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home);
        }
    } else if let Some(rest) = trimmed.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(rest);
    }
    PathBuf::from(trimmed)
}

/// Resolves the effective `couriers.d/` directory for `cfg`, expanding `~/...`
/// identically across `⚙ Settings`, `ConfigHook`, and [`CourierHub`].
pub fn effective_couriers_dir_for(
    cfg: &RexConfig,
    config_path: Option<&Path>,
    fallback_dir: Option<&Path>,
) -> Option<PathBuf> {
    if let Some(ref custom) = cfg.mail.couriers_dir {
        return Some(expand_home_prefix(custom));
    }
    if let Some(cfg_path) = config_path
        && let Some(parent) = cfg_path.parent()
    {
        return Some(parent.join("couriers.d"));
    }
    fallback_dir.map(expand_home_prefix)
}

/// Configuration for the daemon [`CourierHub`] reactor.
#[derive(Clone, Debug)]
pub struct CourierHubConfig {
    pub local_machine_id: String,
    pub config_path: Option<PathBuf>,
    pub default_couriers_dir: Option<PathBuf>,
    pub house: HouseProfile,
    pub operator_seat: SeatName,
    pub timeout: Duration,
    pub defer_backoff: Duration,
    defer_backoff_overridden: bool,
    pub degraded_threshold: u32,
    pub max_defer_tries: u32,
    max_defer_tries_overridden: bool,
    pub max_referents_per_ring: usize,
    pub protection: Option<Protection>,
}

impl CourierHubConfig {
    pub fn new(
        local_machine_id: impl Into<String>,
        house: HouseProfile,
        operator_seat: SeatName,
    ) -> Self {
        Self {
            local_machine_id: local_machine_id.into(),
            config_path: None,
            default_couriers_dir: None,
            house,
            operator_seat,
            timeout: Duration::from_millis(DEFAULT_COURIER_TIMEOUT_MS),
            defer_backoff: Duration::from_millis(DEFAULT_DEFER_BACKOFF_MS),
            defer_backoff_overridden: false,
            degraded_threshold: DEFAULT_DEGRADED_THRESHOLD,
            max_defer_tries: DEFAULT_MAX_DEFER_TRIES,
            max_defer_tries_overridden: false,
            max_referents_per_ring: DEFAULT_MAX_REFERENTS_PER_RING,
            protection: None,
        }
    }

    pub fn with_config_path(mut self, path: Option<PathBuf>) -> Self {
        self.config_path = path;
        self
    }

    pub fn with_default_couriers_dir(mut self, dir: Option<PathBuf>) -> Self {
        self.default_couriers_dir = dir;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_defer_backoff(mut self, backoff: Duration) -> Self {
        self.defer_backoff = backoff.max(Duration::from_millis(1));
        self.defer_backoff_overridden = true;
        self
    }

    pub fn with_degraded_threshold(mut self, k: u32) -> Self {
        self.degraded_threshold = k.max(1);
        self
    }

    pub fn with_max_defer_tries(mut self, max_tries: u32) -> Self {
        self.max_defer_tries = max_tries.max(1);
        self.max_defer_tries_overridden = true;
        self
    }

    pub fn with_max_referents_per_ring(mut self, max_refs: usize) -> Self {
        self.max_referents_per_ring = max_refs.max(1);
        self
    }

    pub fn with_protection(mut self, protection: Protection) -> Self {
        self.protection = Some(protection);
        self
    }

    fn is_operator_target(&self, seat: &SeatName) -> bool {
        seat == &self.operator_seat || self.house.is_operator_seat(seat.as_str())
    }

    fn effective_defer_backoff_ms(&self, runtime_cfg: &RexConfig) -> u64 {
        if self.defer_backoff_overridden {
            u64::try_from(self.defer_backoff.as_millis())
                .unwrap_or(u64::MAX)
                .max(1)
        } else {
            runtime_cfg.mail.defer_backoff_ms.max(1)
        }
    }

    fn effective_max_defer_tries(&self, runtime_cfg: &RexConfig) -> u32 {
        if self.max_defer_tries_overridden {
            self.max_defer_tries.max(1)
        } else {
            runtime_cfg.mail.max_defer_tries.max(1)
        }
    }

    fn backoff_due_at(&self, runtime_cfg: &RexConfig, last_at: UnixMs, tries: u32) -> UnixMs {
        let base_ms = self.effective_defer_backoff_ms(runtime_cfg);
        let cap_ms = MAX_DEFER_STEP_DELAY_MS.max(base_ms);
        let shift = tries.saturating_sub(1).min(5);
        let delay = base_ms.saturating_mul(1u64 << shift).min(cap_ms);
        UnixMs(last_at.0.saturating_add(delay))
    }
}

struct CourierHubState {
    mail: MailState,
    presence: PresenceState,
    caught_up: bool,
    fenced_refused_epochs: BTreeSet<(SeatName, IncarnationId, u64)>,
    optimistic_canary_rung: BTreeSet<(SeatName, IncarnationId, u64)>,
    optimistic_canary_due: BTreeMap<(SeatName, IncarnationId, u64), UnixMs>,
    optimistic_mail_due: BTreeMap<(SeatName, Sha256Hex), UnixMs>,
    optimistic_no_route_due: BTreeMap<(SeatName, Sha256Hex), UnixMs>,
    reconciled_failure_seqs: BTreeSet<Seq>,
    affinity: IncarnationAffinity,
}

struct CourierHubInner {
    cfg: CourierHubConfig,
    config_rx: watch::Receiver<RexConfig>,
    state: Mutex<CourierHubState>,
    sweep_lock: Mutex<()>,
    next_timer_due_ms: AtomicU64,
}

/// Handle allowing the daemon periodic timer or tests to trigger a [`CourierHub`]
/// sweep (for deferred retries or newly added `couriers.d/` scripts).
#[derive(Clone)]
pub struct CourierHubHandle {
    inner: Arc<CourierHubInner>,
}

impl CourierHubHandle {
    pub async fn sweep(&self, client: &LocalClient) {
        self.inner.sweep(client).await;
    }
}

/// Daemon reactor dispatching body-blind [`DeliveryClaim`]s to executable
/// `couriers.d/` scripts for canary confirmation and mail delivery.
pub struct CourierHub {
    inner: Arc<CourierHubInner>,
}

impl CourierHub {
    pub fn new(cfg: CourierHubConfig, config_rx: watch::Receiver<RexConfig>) -> Self {
        let presence = PresenceState::with_house_profile(cfg.house.clone())
            .with_machine_id(cfg.local_machine_id.clone());
        let inner = Arc::new(CourierHubInner {
            cfg,
            config_rx,
            state: Mutex::new(CourierHubState {
                mail: MailState::default(),
                presence,
                caught_up: false,
                fenced_refused_epochs: BTreeSet::new(),
                optimistic_canary_rung: BTreeSet::new(),
                optimistic_canary_due: BTreeMap::new(),
                optimistic_mail_due: BTreeMap::new(),
                optimistic_no_route_due: BTreeMap::new(),
                reconciled_failure_seqs: BTreeSet::new(),
                affinity: IncarnationAffinity::default(),
            }),
            sweep_lock: Mutex::new(()),
            next_timer_due_ms: AtomicU64::new(0),
        });
        Self { inner }
    }

    pub fn handle(&self) -> CourierHubHandle {
        CourierHubHandle {
            inner: self.inner.clone(),
        }
    }
}

impl Reactor for CourierHub {
    const NAME: &'static str = "courier-hub";

    fn interest(&self) -> Interest {
        Interest::Streams {
            streams: vec![StreamId::Mail, StreamId::Presence, StreamId::Config],
        }
    }

    fn react(
        &mut self,
        frame: Frame,
        client: &LocalClient,
    ) -> impl Future<Output = Result<(), ApiError>> + Send {
        let inner = self.inner.clone();
        let client = client.clone();
        async move {
            match frame {
                Frame::CaughtUp { .. } => {
                    inner.state.lock().await.caught_up = true;
                    inner.recover_and_sweep_startup(&client).await;
                }
                Frame::Committed(env) => {
                    let (should_sweep, should_reconcile_failures) = {
                        let mut st = inner.state.lock().await;
                        let mut trigger_sweep = false;
                        let mut trigger_reconcile = false;
                        match &env.event {
                            Event::Mail(me) => {
                                st.mail.apply(&env);
                                match me {
                                    MailEvent::MessageCommitted { .. }
                                    | MailEvent::RecipientsAdded { .. }
                                    | MailEvent::Banked { .. }
                                    | MailEvent::Projected { .. } => {
                                        trigger_sweep = true;
                                    }
                                    MailEvent::DeliveryFailed { .. } => {
                                        trigger_reconcile = true;
                                    }
                                    // Self-authored RingDispatched/RingAttempted and Acked frames
                                    // never trigger immediate feedback sweeps (preventing tight
                                    // retry loops and redundant 1,653-letter scans).
                                    MailEvent::RingAttempted {
                                        courier,
                                        seat,
                                        incarnation_id,
                                        sha256s,
                                        is_canary,
                                        ..
                                    } => {
                                        if *is_canary {
                                            if let Some(inc) = incarnation_id {
                                                st.optimistic_canary_due.retain(|(s, i, _), _| {
                                                    !(s == seat && i == inc)
                                                });
                                            }
                                        } else if incarnation_id.is_none()
                                            && courier == "route-pending"
                                        {
                                            for sha in sha256s {
                                                st.optimistic_no_route_due
                                                    .remove(&(seat.clone(), sha.clone()));
                                            }
                                        } else {
                                            for sha in sha256s {
                                                let k = (seat.clone(), sha.clone());
                                                st.optimistic_mail_due.remove(&k);
                                                st.optimistic_no_route_due.remove(&k);
                                            }
                                        }
                                    }
                                    MailEvent::Acked {
                                        seat: Some(seat),
                                        sha256,
                                        ..
                                    } => {
                                        let k = (seat.clone(), sha256.clone());
                                        st.optimistic_mail_due.remove(&k);
                                        st.optimistic_no_route_due.remove(&k);
                                    }
                                    MailEvent::RingDispatched { .. }
                                    | MailEvent::DeliveryFailureReconciled { .. }
                                    | MailEvent::Acked { seat: None, .. }
                                    | MailEvent::PathHeadUpdated { .. }
                                    | MailEvent::BellRang { .. } => {}
                                }
                            }
                            Event::Presence(pe) => {
                                let meta = EntryMeta {
                                    seq: env.seq,
                                    at: env.at,
                                    by: env.by.clone(),
                                };
                                Presence::evolve(&mut st.presence, pe, &meta);
                                fold_presence_into_hub_state(&mut st, pe);
                                trigger_sweep = true;
                            }
                            Event::Config(_) => {
                                trigger_sweep = true;
                            }
                            _ => {}
                        }
                        (
                            st.caught_up && trigger_sweep,
                            st.caught_up && trigger_reconcile,
                        )
                    };
                    if should_sweep {
                        inner.sweep(&client).await;
                    } else if should_reconcile_failures {
                        inner.reconcile_pending_failures_locked(&client).await;
                    }
                }
                _ => {}
            }
            Ok(())
        }
    }
}

fn fold_presence_into_hub_state(st: &mut CourierHubState, pe: &PresenceEvent) {
    match pe {
        PresenceEvent::Bound {
            binding,
            superseded,
        } => {
            for old_inc in superseded {
                st.fenced_refused_epochs
                    .retain(|(s, inc, _)| !(s == &binding.seat && inc == old_inc));
                st.optimistic_canary_rung
                    .retain(|(s, inc, _)| !(s == &binding.seat && inc == old_inc));
                st.optimistic_canary_due
                    .retain(|(s, inc, _), _| !(s == &binding.seat && inc == old_inc));
                st.mail
                    .canary_defer_state
                    .remove(&(binding.seat.clone(), old_inc.clone()));
                st.mail
                    .canary_rung_incarnations
                    .remove(&(binding.seat.clone(), old_inc.clone()));
            }
            if binding.status == BindingStatus::Live {
                st.optimistic_no_route_due
                    .retain(|(s, _), _| s != &binding.seat);
            }
        }
        PresenceEvent::CanaryConfirmed {
            seat,
            incarnation_id,
            ..
        } => {
            st.optimistic_canary_rung
                .retain(|(s, inc, _)| !(s == seat && inc == incarnation_id));
            st.optimistic_canary_due
                .retain(|(s, inc, _), _| !(s == seat && inc == incarnation_id));
            st.optimistic_no_route_due.retain(|(s, _), _| s != seat);
            st.mail
                .canary_defer_state
                .remove(&(seat.clone(), incarnation_id.clone()));
            st.mail
                .canary_rung_incarnations
                .remove(&(seat.clone(), incarnation_id.clone()));
        }
        PresenceEvent::Unbound {
            seat,
            incarnation_id,
            ..
        } => {
            st.fenced_refused_epochs
                .retain(|(s, inc, _)| !(s == seat && inc == incarnation_id));
            st.optimistic_canary_rung
                .retain(|(s, inc, _)| !(s == seat && inc == incarnation_id));
            st.optimistic_canary_due
                .retain(|(s, inc, _), _| !(s == seat && inc == incarnation_id));
            st.mail
                .canary_defer_state
                .remove(&(seat.clone(), incarnation_id.clone()));
            st.mail
                .canary_rung_incarnations
                .remove(&(seat.clone(), incarnation_id.clone()));
        }
        _ => {}
    }
}

impl CourierHubInner {
    fn resolve_guard(&self) -> Result<ProtectedRootsGuard, String> {
        match &self.cfg.protection {
            Some(p) => ProtectedRootsGuard::resolve(p).map_err(|e| e.to_string()),
            None => load_protected_roots()
                .map_err(|e| e.to_string())
                .and_then(|p| ProtectedRootsGuard::resolve(&p).map_err(|e| e.to_string())),
        }
    }

    fn schedule_wake_at(self: &Arc<Self>, due_at: UnixMs, now: UnixMs, client: &LocalClient) {
        let prev = self.next_timer_due_ms.load(Ordering::Acquire);
        if prev != 0 && prev <= due_at.0 {
            return;
        }
        self.next_timer_due_ms.store(due_at.0, Ordering::Release);
        let wait_ms = due_at.0.saturating_sub(now.0).max(5);
        let me = self.clone();
        let client = client.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(wait_ms)).await;
            let _ = me.next_timer_due_ms.compare_exchange(
                due_at.0,
                0,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
            me.sweep(&client).await;
        });
    }

    async fn recover_and_sweep_startup(self: &Arc<Self>, client: &LocalClient) {
        {
            let _sweep_guard = self.sweep_lock.lock().await;
            let current_cfg = self.config_rx.borrow().clone();
            let hub_owner_seat = current_cfg.mail.hub_owner_seat.clone();
            let inflight: Vec<_> = {
                let st = self.state.lock().await;
                st.mail
                    .inflight_dispatches
                    .values()
                    .filter(|d| d.machine_id == self.cfg.local_machine_id)
                    .cloned()
                    .collect()
            };
            for disp in inflight {
                let _ = client
                    .call(MailRecordRing {
                        phase: RingRecordPhase::Complete,
                        dispatch_id: Some(disp.dispatch_id),
                        reconciled_failure_seq: None,
                        courier: disp.courier,
                        seat: disp.seat,
                        machine_id: disp.machine_id,
                        incarnation_id: disp.incarnation_id,
                        expected_epoch: disp.expected_epoch,
                        sha256s: disp.sha256s,
                        is_canary: disp.is_canary,
                        pre_spawn: false,
                        outcome: CourierOutcome::Unknown,
                        reason: Some("interrupted-on-restart".into()),
                        detail: Some(
                            "pre-dispatch claim recovered on CourierHub startup without re-ringing"
                                .into(),
                        ),
                        sender_seat: None,
                        hub_owner_seat: hub_owner_seat.clone(),
                        failure_kind: None,
                        failure_groups: Vec::new(),
                        degraded_threshold: Some(self.cfg.degraded_threshold),
                    })
                    .await;
            }
            self.reconcile_pending_failures(client).await;
        }
        self.sweep(client).await;
    }

    async fn reconcile_pending_failures_locked(&self, client: &LocalClient) {
        let _sweep_guard = self.sweep_lock.lock().await;
        self.reconcile_pending_failures(client).await;
    }

    async fn mark_failure_reconciled(&self, failure: &MailDeliveryFailure, client: &LocalClient) {
        self.state
            .lock()
            .await
            .reconciled_failure_seqs
            .insert(failure.seq);
        let _ = client
            .call(MailRecordRing {
                phase: RingRecordPhase::Reconciled,
                dispatch_id: None,
                reconciled_failure_seq: Some(failure.seq),
                courier: failure.courier.clone(),
                seat: failure.seat.clone(),
                machine_id: self.cfg.local_machine_id.clone(),
                incarnation_id: failure.incarnation_id.clone(),
                expected_epoch: failure.expected_epoch,
                sha256s: Vec::new(),
                is_canary: failure.is_canary,
                pre_spawn: false,
                outcome: CourierOutcome::Unknown,
                reason: None,
                detail: None,
                sender_seat: None,
                hub_owner_seat: failure.hub_owner_seat.clone(),
                failure_kind: None,
                failure_groups: Vec::new(),
                degraded_threshold: Some(self.cfg.degraded_threshold),
            })
            .await;
    }

    async fn reconcile_pending_failures(&self, client: &LocalClient) {
        let pending: Vec<MailDeliveryFailure> = {
            let st = self.state.lock().await;
            st.mail
                .unreconciled_failures
                .values()
                .filter(|f| !st.reconciled_failure_seqs.contains(&f.seq))
                .cloned()
                .collect()
        };
        for failure in &pending {
            if self.reconcile_one_failure(failure, client).await {
                self.mark_failure_reconciled(failure, client).await;
            }
        }
    }

    async fn reconcile_one_failure(
        &self,
        failure: &MailDeliveryFailure,
        client: &LocalClient,
    ) -> bool {
        let mut all_ok = true;

        // 1. Expire dead binding on Refused or canary GiveUp if still active.
        //    Canary failures pass `only_if_pending_canary: true` so a late canary reply
        //    can NEVER expire a route that was already confirmed Live (`Astra B2`).
        let expire_binding = failure.kind == DeliveryFailureKind::Refused
            || (failure.kind == DeliveryFailureKind::GiveUp
                && (failure.is_canary || failure.sha256s.is_empty()));
        let only_if_pending_canary = failure.is_canary
            || (failure.kind == DeliveryFailureKind::GiveUp && failure.sha256s.is_empty());
        if expire_binding && let Some(ref inc) = failure.incarnation_id {
            let needs_expire = match client
                .call(PresenceList {
                    project: None,
                    live_only: false,
                    include_retired: true,
                })
                .await
            {
                Ok(snap) => snap
                    .seats
                    .iter()
                    .find(|s| s.record.seat == failure.seat)
                    .and_then(|s| s.bindings.iter().find(|b| &b.incarnation_id == inc))
                    .is_some_and(|b| {
                        let status_ok = if only_if_pending_canary {
                            b.status == BindingStatus::PendingCanary
                        } else {
                            matches!(
                                b.status,
                                BindingStatus::Live
                                    | BindingStatus::PendingCanary
                                    | BindingStatus::TransportDown
                            )
                        };
                        status_ok && failure.expected_epoch.is_none_or(|ep| b.epoch == ep)
                    }),
                Err(_) => true,
            };
            if needs_expire
                && client
                    .call(PresenceExpire {
                        seat: failure.seat.clone(),
                        incarnation_id: inc.clone(),
                        expected_epoch: failure.expected_epoch,
                        lease_expires_at: None,
                        only_if_pending_canary,
                        reason: format!(
                            "courier {} {}: {}",
                            failure.courier,
                            failure.kind.as_str(),
                            failure.reason
                        ),
                    })
                    .await
                    .is_err()
            {
                all_ok = false;
            }
        }

        // 2. Determine non-Operator escalation recipients (`hub_owner_seat` + `sender_seat`).
        let mut recipients: Vec<SeatName> = Vec::new();
        if !self.cfg.is_operator_target(&failure.hub_owner_seat) {
            recipients.push(failure.hub_owner_seat.clone());
        }
        if matches!(
            failure.kind,
            DeliveryFailureKind::Refused | DeliveryFailureKind::GiveUp
        ) && let Some(ref sender) = failure.sender_seat
            && !self.cfg.is_operator_target(sender)
            && !recipients.contains(sender)
        {
            recipients.push(sender.clone());
        }
        if recipients.is_empty() {
            return all_ok;
        }

        let full_refs: Vec<String> = failure
            .sha256s
            .iter()
            .map(|s| s.as_str().to_owned())
            .collect();
        let short_list = if failure.sha256s.is_empty() {
            "none".to_owned()
        } else {
            failure
                .sha256s
                .iter()
                .map(|s| s.as_str()[..16].to_owned())
                .collect::<Vec<_>>()
                .join(",")
        };
        let detail_line = failure
            .detail
            .as_deref()
            .map(str::trim)
            .filter(|d| !d.is_empty())
            .map(|d| format!("\nDetail: {d}"))
            .unwrap_or_default();

        let (subject, title, body) = match failure.kind {
            DeliveryFailureKind::Refused => (
                format!(
                    "Delivery refused for @{} ({})",
                    failure.seat, failure.reason
                ),
                format!(
                    "Mail delivery refused for @{} ({})",
                    failure.seat, failure.reason
                ),
                format!(
                    "Courier `{}` refused delivery to `@{}` on machine `{}`.\n\
                     Reason: `{}`{detail_line}\n\
                     The dead session binding was immediately expired without retry.\n\
                     Letters: {short_list}",
                    failure.courier, failure.seat, self.cfg.local_machine_id, failure.reason
                ),
            ),
            DeliveryFailureKind::GiveUp => (
                format!(
                    "Delivery gave up for @{} ({})",
                    failure.seat, failure.reason
                ),
                format!(
                    "Mail delivery gave up for @{} ({})",
                    failure.seat, failure.reason
                ),
                format!(
                    "Courier `{}` exhausted retries for `@{}` on machine `{}`.\n\
                     Reason: `{}`{detail_line}\n\
                     Letters: {short_list}",
                    failure.courier, failure.seat, self.cfg.local_machine_id, failure.reason
                ),
            ),
            DeliveryFailureKind::AdapterDegraded => (
                format!(
                    "AdapterDegraded: courier `{}` on {}",
                    failure.courier, self.cfg.local_machine_id
                ),
                format!(
                    "AdapterDegraded: `{}` ({})",
                    failure.courier, failure.reason
                ),
                format!(
                    "Courier `{}` reached consecutive unknown threshold ({}) while ringing `@{}` on machine `{}`.\n\
                     Last reason: `{}`{detail_line}\n\
                     Previous unknown rings were counted as rung and not retried. Inspect `couriers.d/{}`.",
                    failure.courier,
                    self.cfg.degraded_threshold,
                    failure.seat,
                    self.cfg.local_machine_id,
                    failure.reason,
                    failure.courier
                ),
            ),
        };

        for recip in recipients {
            // Keyed deterministically by `(failure.seq, recip)` so distinct episodes (even with
            // empty `sha256s`) never collide while crash-recovery replays are 100% idempotent.
            let idem_sig = sha256_short16(&format!("courier_fail/2\n{}\n{}", failure.seq.0, recip));
            let Ok(idem) = IdemKey::parse(format!("cfail:{idem_sig}")) else {
                all_ok = false;
                continue;
            };
            let already_sent = self
                .state
                .lock()
                .await
                .mail
                .by_send_idem
                .contains_key(&idem);
            if !already_sent
                && client
                    .call(MailSend {
                        to: vec![recip.clone()],
                        cc: Vec::new(),
                        subject: subject.clone(),
                        body: body.clone(),
                        from: None,
                        priority: LetterPriority::High,
                        topic: Some("delivery-failure".into()),
                        in_reply_to: None,
                        refs: full_refs.clone(),
                        wait_export: false,
                        idem: Some(idem),
                    })
                    .await
                    .is_err()
            {
                all_ok = false;
            }

            let dedupe = format!(
                "courier_fail:{}:{}:{}:{}",
                failure.seq.0,
                failure.kind.as_str(),
                failure.courier,
                recip
            );
            let recip_principal = Principal::Seat {
                seat: recip.clone(),
            };
            if client
                .call(InboxRaise {
                    to: recip_principal,
                    draft: NotificationDraft {
                        kind: NotifyKind::MailDeliveryFailed {
                            failure_kind: failure.kind,
                            courier: failure.courier.clone(),
                            seat: failure.seat.clone(),
                            reason: failure.reason.clone(),
                        },
                        title: title.clone(),
                        preview: body.clone(),
                        actions: Vec::new(),
                        dedupe,
                    },
                })
                .await
                .is_err()
            {
                all_ok = false;
            }
        }

        all_ok
    }

    async fn authoritative_seat_bindings(
        &self,
        client: &LocalClient,
        seat: &SeatName,
    ) -> Option<Vec<SeatBinding>> {
        let snap = client
            .call(PresenceList {
                project: None,
                live_only: false,
                include_retired: false,
            })
            .await
            .ok()?;
        Some(
            snap.seats
                .into_iter()
                .find(|s| &s.record.seat == seat)
                .map(|s| s.bindings)
                .unwrap_or_default(),
        )
    }

    async fn is_binding_still_authoritative(
        &self,
        client: &LocalClient,
        expected: &SeatBinding,
        want_status: BindingStatus,
        now: UnixMs,
    ) -> bool {
        {
            let st = self.state.lock().await;
            if st.fenced_refused_epochs.contains(&(
                expected.seat.clone(),
                expected.incarnation_id.clone(),
                expected.epoch,
            )) {
                return false;
            }
        }
        let Some(bindings) = self
            .authoritative_seat_bindings(client, &expected.seat)
            .await
        else {
            return false;
        };
        let Some(live_b) = bindings
            .iter()
            .find(|b| b.incarnation_id == expected.incarnation_id)
        else {
            return false;
        };
        if live_b.epoch != expected.epoch
            || live_b.status != want_status
            || live_b.expires_at.is_some_and(|exp| exp <= now)
        {
            return false;
        }
        if want_status == BindingStatus::Live {
            // Re-verify global winner across ALL machines/lanes (`OneMachineRings`):
            let mut probe_state = PresenceState::with_house_profile(self.cfg.house.clone())
                .with_machine_id(self.cfg.local_machine_id.clone());
            for (idx, b) in bindings.into_iter().enumerate() {
                let meta = EntryMeta {
                    seq: Seq(idx as u64 + 1),
                    at: b.bound_at,
                    by: Principal::Seat {
                        seat: expected.seat.clone(),
                    },
                };
                Presence::evolve(
                    &mut probe_state,
                    &PresenceEvent::Bound {
                        binding: b,
                        superseded: Vec::new(),
                    },
                    &meta,
                );
            }
            let affinity = self.state.lock().await.affinity.clone();
            let Some(winner) = select_binding_for_delivery_on_machine(
                &probe_state,
                &expected.seat,
                None,
                None,
                &affinity,
                Some(&self.cfg.local_machine_id),
                now,
            ) else {
                return false;
            };
            if winner.incarnation_id != expected.incarnation_id || winner.epoch != expected.epoch {
                return false;
            }
        }
        true
    }

    async fn sweep(self: &Arc<Self>, client: &LocalClient) {
        let _sweep_guard = self.sweep_lock.lock().await;
        self.reconcile_pending_failures(client).await;

        let current_cfg = self.config_rx.borrow().clone();
        let Some(couriers_dir) = effective_couriers_dir_for(
            &current_cfg,
            self.cfg.config_path.as_deref(),
            self.cfg.default_couriers_dir.as_deref(),
        ) else {
            return;
        };
        // Fail closed when protected-roots cannot be loaded or resolved!
        let guard = match self.resolve_guard() {
            Ok(g) => g,
            Err(err) => {
                eprintln!(
                    "rex-wave courier-hub: refusing adapter execution because protected-roots failed: {err}"
                );
                return;
            }
        };
        let now = rex_wave_runtime::now();
        let hub_owner_seat = current_cfg.mail.hub_owner_seat.clone();
        let mut earliest_next_due: Option<UnixMs> = None;

        // Phase A: Canary rings for PendingCanary bindings owned by local_machine_id.
        let canary_candidates: Vec<SeatBinding> = {
            let st = self.state.lock().await;
            st.presence
                .pending_canary_bindings_for_machine(&self.cfg.local_machine_id, now)
                .into_iter()
                .cloned()
                .collect()
        };

        for binding in canary_candidates {
            let ckey = (binding.seat.clone(), binding.incarnation_id.clone());
            let ep_key = (
                binding.seat.clone(),
                binding.incarnation_id.clone(),
                binding.epoch,
            );
            let skip_or_due = {
                let st = self.state.lock().await;
                if st.fenced_refused_epochs.contains(&ep_key)
                    || st.optimistic_canary_rung.contains(&ep_key)
                    || st.mail.canary_rung_incarnations.contains(&ckey)
                {
                    Some(None)
                } else if let Some(&due) = st.optimistic_canary_due.get(&ep_key) {
                    if now < due { Some(Some(due)) } else { None }
                } else if let Some(rec) = st.mail.canary_defer_state.get(&ckey) {
                    let due = self
                        .cfg
                        .backoff_due_at(&current_cfg, rec.last_at, rec.tries);
                    if now < due { Some(Some(due)) } else { None }
                } else {
                    None
                }
            };
            if let Some(maybe_due) = skip_or_due {
                if let Some(due) = maybe_due {
                    earliest_next_due = Some(earliest_next_due.map_or(due, |cur| cur.min(due)));
                }
                continue;
            }

            let Some(adapter) = resolve_adapter_name(&binding.endpoint) else {
                continue;
            };
            let now_step = rex_wave_runtime::now();
            if !self
                .is_binding_still_authoritative(
                    client,
                    &binding,
                    BindingStatus::PendingCanary,
                    now_step,
                )
                .await
            {
                continue;
            }
            let script = match resolve_courier_executable(&couriers_dir, adapter, Some(&guard)) {
                Ok(Some(s)) => s,
                Ok(None) => {
                    let exists_on_disk = rex_wave_fs::entry_exists(&couriers_dir.join(adapter));
                    let (outcome, reason, detail) = if exists_on_disk {
                        (
                            CourierOutcome::Unknown,
                            "not-executable",
                            format!(
                                "courier adapter {} is not executable",
                                couriers_dir.join(adapter).display()
                            ),
                        )
                    } else {
                        (
                            CourierOutcome::Deferred,
                            "adapter-missing",
                            format!(
                                "courier adapter '{}' not found in {}",
                                adapter,
                                couriers_dir.display()
                            ),
                        )
                    };
                    if let Some(due) = self
                        .record_canary_pre_spawn_attempt(
                            &binding,
                            adapter,
                            outcome,
                            reason,
                            detail,
                            &current_cfg,
                            &hub_owner_seat,
                            client,
                        )
                        .await
                    {
                        earliest_next_due = Some(earliest_next_due.map_or(due, |cur| cur.min(due)));
                    }
                    continue;
                }
                Err(err) => {
                    if let Some(due) = self
                        .record_canary_pre_spawn_attempt(
                            &binding,
                            adapter,
                            CourierOutcome::Unknown,
                            "invalid-adapter",
                            err,
                            &current_cfg,
                            &hub_owner_seat,
                            client,
                        )
                        .await
                    {
                        earliest_next_due = Some(earliest_next_due.map_or(due, |cur| cur.min(due)));
                    }
                    continue;
                }
            };
            if let Some(due) = self
                .execute_canary_job(
                    binding,
                    adapter,
                    script,
                    &current_cfg,
                    &hub_owner_seat,
                    &guard,
                    client,
                )
                .await
            {
                earliest_next_due = Some(earliest_next_due.map_or(due, |cur| cur.min(due)));
            }
        }

        // Phase B: Normal mail delivery rings for Live bindings owned by local_machine_id.
        let couriers_dir_exists =
            rex_wave_fs::entry_kind(&couriers_dir).ok() == Some(EntryKind::Dir);
        let seats_with_mail: Vec<SeatName> = {
            let st = self.state.lock().await;
            let mut set = BTreeSet::new();
            for entry in st.mail.messages.values() {
                let is_courier_system_notice = entry.committed_by
                    == (Principal::System {
                        part: SystemPart::CourierHub,
                    })
                    && entry.message.from == COURIER_HUB_SENDER_ADDRESS;
                if entry.mutated_after_ack || entry.message.receipt_only || is_courier_system_notice
                {
                    continue;
                }
                for recip in entry.message.to.iter().chain(entry.message.cc.iter()) {
                    if self.cfg.is_operator_target(recip)
                        || rex_wave_api::presence::is_non_seat_target(recip.as_str())
                    {
                        continue;
                    }
                    set.insert(recip.clone());
                }
            }
            set.into_iter().collect()
        };

        for seat in seats_with_mail {
            let mut skipped_shas_in_sweep: BTreeSet<Sha256Hex> = BTreeSet::new();
            loop {
                let now_step = rex_wave_runtime::now();
                enum SeatSweepStep {
                    None,
                    NoRouteEntries(Vec<CommittedMailEntry>),
                    ReadyBatch {
                        binding: SeatBinding,
                        entries: Vec<CommittedMailEntry>,
                        has_more_batches: bool,
                    },
                }

                let step = {
                    let st = self.state.lock().await;
                    let global_winner = select_binding_for_delivery(
                        &st.presence,
                        &seat,
                        None,
                        None,
                        &st.affinity,
                        now_step,
                    )
                    .cloned();
                    match select_binding_for_delivery_on_machine(
                        &st.presence,
                        &seat,
                        None,
                        None,
                        &st.affinity,
                        Some(&self.cfg.local_machine_id),
                        now_step,
                    )
                    .cloned()
                    {
                        None => {
                            if global_winner.is_some() || !couriers_dir_exists {
                                SeatSweepStep::None
                            } else {
                                let horizon =
                                    st.presence.seat(&seat).and_then(|r| r.mail_horizon_at);
                                let ringable_shas = st.mail.ringable_for_seat(&seat);
                                let mut no_route_ready: Vec<CommittedMailEntry> = Vec::new();
                                for sha in ringable_shas {
                                    if skipped_shas_in_sweep.contains(&sha) {
                                        continue;
                                    }
                                    let k = (seat.clone(), sha.clone());
                                    if st.mail.rung_keys.contains(&k)
                                        || st.mail.has_any_failure_for_seat(&seat, &sha)
                                        || st.mail.has_inflight_for(&seat, &sha)
                                    {
                                        continue;
                                    }
                                    if let Some(&opt_due) = st.optimistic_no_route_due.get(&k)
                                        && now_step < opt_due
                                    {
                                        if opt_due < UnixMs(u64::MAX) {
                                            earliest_next_due = Some(
                                                earliest_next_due
                                                    .map_or(opt_due, |cur| cur.min(opt_due)),
                                            );
                                        }
                                        continue;
                                    }
                                    if let Some(rec) = st.mail.no_route_defer_state.get(&k) {
                                        let due = self.cfg.backoff_due_at(
                                            &current_cfg,
                                            rec.last_at,
                                            rec.tries,
                                        );
                                        if now_step < due {
                                            earliest_next_due = Some(
                                                earliest_next_due.map_or(due, |cur| cur.min(due)),
                                            );
                                            continue;
                                        }
                                    }
                                    let Some(entry) = st.mail.messages.get(&sha).cloned() else {
                                        continue;
                                    };
                                    let is_courier_system_notice = entry.committed_by
                                        == (Principal::System {
                                            part: SystemPart::CourierHub,
                                        })
                                        && entry.message.from == COURIER_HUB_SENDER_ADDRESS;
                                    if is_courier_system_notice
                                        || horizon.is_some_and(|h| entry.message.created_at < h)
                                    {
                                        continue;
                                    }
                                    no_route_ready.push(entry);
                                }
                                if no_route_ready.is_empty() {
                                    SeatSweepStep::None
                                } else {
                                    no_route_ready.sort_by_key(|e| (e.message.created_at, e.seq));
                                    SeatSweepStep::NoRouteEntries(no_route_ready)
                                }
                            }
                        }
                        Some(binding) => {
                            if st.fenced_refused_epochs.contains(&(
                                seat.clone(),
                                binding.incarnation_id.clone(),
                                binding.epoch,
                            )) {
                                SeatSweepStep::None
                            } else {
                                let horizon =
                                    st.presence.seat(&seat).and_then(|r| r.mail_horizon_at);
                                let ringable_shas = st.mail.ringable_for_seat(&seat);
                                let mut ready_entries: Vec<CommittedMailEntry> = Vec::new();
                                for sha in ringable_shas {
                                    if skipped_shas_in_sweep.contains(&sha) {
                                        continue;
                                    }
                                    let k = (seat.clone(), sha.clone());
                                    if st.mail.rung_keys.contains(&k)
                                        || st.mail.is_failed_for_binding(
                                            &seat,
                                            Some(&binding.incarnation_id),
                                            Some(binding.epoch),
                                            &sha,
                                        )
                                        || st.mail.has_inflight_for(&seat, &sha)
                                    {
                                        continue;
                                    }
                                    if let Some(&opt_due) = st.optimistic_mail_due.get(&k)
                                        && now_step < opt_due
                                    {
                                        if opt_due < UnixMs(u64::MAX) {
                                            earliest_next_due = Some(
                                                earliest_next_due
                                                    .map_or(opt_due, |cur| cur.min(opt_due)),
                                            );
                                        }
                                        continue;
                                    }
                                    if let Some(rec) = st.mail.defer_state.get(&k) {
                                        let due = self.cfg.backoff_due_at(
                                            &current_cfg,
                                            rec.last_at,
                                            rec.tries,
                                        );
                                        if now_step < due {
                                            earliest_next_due = Some(
                                                earliest_next_due.map_or(due, |cur| cur.min(due)),
                                            );
                                            continue;
                                        }
                                    }
                                    let Some(entry) = st.mail.messages.get(&sha).cloned() else {
                                        continue;
                                    };
                                    let is_courier_system_notice = entry.committed_by
                                        == (Principal::System {
                                            part: SystemPart::CourierHub,
                                        })
                                        && entry.message.from == COURIER_HUB_SENDER_ADDRESS;
                                    if is_courier_system_notice
                                        || horizon.is_some_and(|h| entry.message.created_at < h)
                                    {
                                        continue;
                                    }
                                    ready_entries.push(entry);
                                }
                                if ready_entries.is_empty() {
                                    SeatSweepStep::None
                                } else {
                                    ready_entries.sort_by_key(|e| (e.message.created_at, e.seq));
                                    let has_more_batches =
                                        ready_entries.len() > self.cfg.max_referents_per_ring;
                                    ready_entries.truncate(self.cfg.max_referents_per_ring);
                                    SeatSweepStep::ReadyBatch {
                                        binding,
                                        entries: ready_entries,
                                        has_more_batches,
                                    }
                                }
                            }
                        }
                    }
                };

                let (binding, entries, has_more_batches) = match step {
                    SeatSweepStep::None => break,
                    SeatSweepStep::NoRouteEntries(no_route_entries) => {
                        if let Some(due) = self
                            .record_no_route_defer_or_give_up(
                                &seat,
                                &no_route_entries,
                                &current_cfg,
                                &hub_owner_seat,
                                client,
                            )
                            .await
                        {
                            earliest_next_due =
                                Some(earliest_next_due.map_or(due, |cur| cur.min(due)));
                        }
                        break;
                    }
                    SeatSweepStep::ReadyBatch {
                        binding,
                        entries,
                        has_more_batches,
                    } => (binding, entries, has_more_batches),
                };

                let Some(adapter) = resolve_adapter_name(&binding.endpoint) else {
                    break;
                };
                let script = match resolve_courier_executable(&couriers_dir, adapter, Some(&guard))
                {
                    Ok(Some(s)) => s,
                    Ok(None) => {
                        if rex_wave_fs::entry_exists(&couriers_dir.join(adapter))
                            && let Some(due) = self
                                .record_mail_pre_spawn_attempt(
                                    &binding,
                                    adapter,
                                    &entries,
                                    CourierOutcome::Unknown,
                                    "not-executable",
                                    format!(
                                        "courier adapter {} is not executable",
                                        couriers_dir.join(adapter).display()
                                    ),
                                    &current_cfg,
                                    &hub_owner_seat,
                                    client,
                                )
                                .await
                        {
                            earliest_next_due =
                                Some(earliest_next_due.map_or(due, |cur| cur.min(due)));
                        }
                        break;
                    }
                    Err(err) => {
                        if let Some(due) = self
                            .record_mail_pre_spawn_attempt(
                                &binding,
                                adapter,
                                &entries,
                                CourierOutcome::Unknown,
                                "invalid-adapter",
                                err,
                                &current_cfg,
                                &hub_owner_seat,
                                client,
                            )
                            .await
                        {
                            earliest_next_due =
                                Some(earliest_next_due.map_or(due, |cur| cur.min(due)));
                        }
                        break;
                    }
                };

                if !self
                    .is_binding_still_authoritative(client, &binding, BindingStatus::Live, now_step)
                    .await
                {
                    break;
                }

                let (keep_going, maybe_due) = self
                    .execute_mail_batch(
                        binding,
                        adapter,
                        script,
                        entries,
                        &mut skipped_shas_in_sweep,
                        &current_cfg,
                        &hub_owner_seat,
                        &guard,
                        client,
                    )
                    .await;
                if let Some(due) = maybe_due {
                    earliest_next_due = Some(earliest_next_due.map_or(due, |cur| cur.min(due)));
                }
                if !keep_going || !has_more_batches {
                    break;
                }
            }
        }

        if let Some(due) = earliest_next_due {
            self.schedule_wake_at(due, rex_wave_runtime::now(), client);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_canary_pre_spawn_attempt(
        &self,
        binding: &SeatBinding,
        adapter: &str,
        outcome: CourierOutcome,
        reason: &str,
        detail: String,
        current_cfg: &RexConfig,
        hub_owner_seat: &SeatName,
        client: &LocalClient,
    ) -> Option<UnixMs> {
        let max_tries = self.cfg.effective_max_defer_tries(current_cfg);
        let prior_tries = self
            .state
            .lock()
            .await
            .mail
            .canary_defer_state
            .get(&(binding.seat.clone(), binding.incarnation_id.clone()))
            .map_or(0, |r| r.tries);
        let next_tries = prior_tries.saturating_add(1);
        let failure_kind = (next_tries >= max_tries).then_some(DeliveryFailureKind::GiveUp);
        let ep_key = (
            binding.seat.clone(),
            binding.incarnation_id.clone(),
            binding.epoch,
        );
        let now_done = rex_wave_runtime::now();
        let mut scheduled_due = None;
        {
            let mut st = self.state.lock().await;
            if failure_kind.is_some() {
                st.fenced_refused_epochs.insert(ep_key);
            } else {
                let due = self.cfg.backoff_due_at(current_cfg, now_done, next_tries);
                st.optimistic_canary_due.insert(ep_key, due);
                scheduled_due = Some(due);
            }
        }

        if let Ok(rec) = client
            .call(MailRecordRing {
                phase: RingRecordPhase::Complete,
                dispatch_id: None,
                reconciled_failure_seq: None,
                courier: adapter.to_owned(),
                seat: binding.seat.clone(),
                machine_id: self.cfg.local_machine_id.clone(),
                incarnation_id: Some(binding.incarnation_id.clone()),
                expected_epoch: Some(binding.epoch),
                sha256s: Vec::new(),
                is_canary: true,
                pre_spawn: true,
                outcome,
                reason: Some(reason.to_owned()),
                detail: Some(detail),
                sender_seat: None,
                hub_owner_seat: hub_owner_seat.clone(),
                failure_kind,
                failure_groups: Vec::new(),
                degraded_threshold: Some(self.cfg.degraded_threshold),
            })
            .await
        {
            for f in &rec.committed_failures {
                if self.reconcile_one_failure(f, client).await {
                    self.mark_failure_reconciled(f, client).await;
                }
            }
        }
        scheduled_due
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_mail_pre_spawn_attempt(
        &self,
        binding: &SeatBinding,
        adapter: &str,
        entries: &[CommittedMailEntry],
        outcome: CourierOutcome,
        reason: &str,
        detail: String,
        current_cfg: &RexConfig,
        hub_owner_seat: &SeatName,
        client: &LocalClient,
    ) -> Option<UnixMs> {
        let max_tries = self.cfg.effective_max_defer_tries(current_cfg);
        let seat = binding.seat.clone();
        let all_shas: Vec<Sha256Hex> = entries.iter().map(|e| e.message.sha256.clone()).collect();
        let sender_groups = group_entries_by_sender(entries);
        let primary_sender = sender_groups.first().and_then(|(s, _)| s.clone());

        let mut given_up_entries: Vec<CommittedMailEntry> = Vec::new();
        let mut earliest_due: Option<UnixMs> = None;
        let now_done = rex_wave_runtime::now();
        {
            let mut st = self.state.lock().await;
            for entry in entries {
                let k = (seat.clone(), entry.message.sha256.clone());
                let next_tries = st
                    .mail
                    .defer_state
                    .get(&k)
                    .map_or(0, |r| r.tries)
                    .saturating_add(1);
                if next_tries >= max_tries {
                    st.optimistic_mail_due.insert(k, UnixMs(u64::MAX));
                    st.mail.failed_incarnation_keys.insert((
                        seat.clone(),
                        binding.incarnation_id.clone(),
                        Some(binding.epoch),
                        entry.message.sha256.clone(),
                    ));
                    given_up_entries.push(entry.clone());
                } else {
                    let due = self.cfg.backoff_due_at(current_cfg, now_done, next_tries);
                    st.optimistic_mail_due.insert(k, due);
                    earliest_due = Some(earliest_due.map_or(due, |cur| cur.min(due)));
                }
            }
        }

        let (failure_kind, failure_groups) = if !given_up_entries.is_empty() {
            let groups = group_entries_by_sender(&given_up_entries)
                .into_iter()
                .map(|(s, shas)| MailFailureGroup {
                    sender_seat: s,
                    sha256s: shas,
                })
                .collect();
            (Some(DeliveryFailureKind::GiveUp), groups)
        } else {
            (None, Vec::new())
        };

        if let Ok(rec) = client
            .call(MailRecordRing {
                phase: RingRecordPhase::Complete,
                dispatch_id: None,
                reconciled_failure_seq: None,
                courier: adapter.to_owned(),
                seat,
                machine_id: self.cfg.local_machine_id.clone(),
                incarnation_id: Some(binding.incarnation_id.clone()),
                expected_epoch: Some(binding.epoch),
                sha256s: all_shas,
                is_canary: false,
                pre_spawn: true,
                outcome,
                reason: Some(reason.to_owned()),
                detail: Some(detail),
                sender_seat: primary_sender,
                hub_owner_seat: hub_owner_seat.clone(),
                failure_kind,
                failure_groups,
                degraded_threshold: Some(self.cfg.degraded_threshold),
            })
            .await
        {
            for f in &rec.committed_failures {
                if self.reconcile_one_failure(f, client).await {
                    self.mark_failure_reconciled(f, client).await;
                }
            }
        }
        earliest_due
    }

    async fn record_no_route_defer_or_give_up(
        &self,
        seat: &SeatName,
        entries: &[CommittedMailEntry],
        current_cfg: &RexConfig,
        hub_owner_seat: &SeatName,
        client: &LocalClient,
    ) -> Option<UnixMs> {
        let max_tries = self.cfg.effective_max_defer_tries(current_cfg);
        let all_shas: Vec<Sha256Hex> = entries.iter().map(|e| e.message.sha256.clone()).collect();
        let sender_groups = group_entries_by_sender(entries);
        let primary_sender = sender_groups.first().and_then(|(s, _)| s.clone());

        let mut given_up_entries: Vec<CommittedMailEntry> = Vec::new();
        let mut earliest_due: Option<UnixMs> = None;
        let now_done = rex_wave_runtime::now();
        {
            let mut st = self.state.lock().await;
            for entry in entries {
                let k = (seat.clone(), entry.message.sha256.clone());
                let next_tries = st
                    .mail
                    .no_route_defer_state
                    .get(&k)
                    .map_or(0, |r| r.tries)
                    .saturating_add(1);
                if next_tries >= max_tries {
                    st.optimistic_no_route_due
                        .insert(k.clone(), UnixMs(u64::MAX));
                    st.mail.failed_keys.insert(k);
                    given_up_entries.push(entry.clone());
                } else {
                    let due = self.cfg.backoff_due_at(current_cfg, now_done, next_tries);
                    st.optimistic_no_route_due.insert(k, due);
                    earliest_due = Some(earliest_due.map_or(due, |cur| cur.min(due)));
                }
            }
        }

        let (failure_kind, failure_groups) = if !given_up_entries.is_empty() {
            let groups = group_entries_by_sender(&given_up_entries)
                .into_iter()
                .map(|(s, shas)| MailFailureGroup {
                    sender_seat: s,
                    sha256s: shas,
                })
                .collect();
            (Some(DeliveryFailureKind::GiveUp), groups)
        } else {
            (None, Vec::new())
        };

        if let Ok(rec) = client
            .call(MailRecordRing {
                phase: RingRecordPhase::Complete,
                dispatch_id: None,
                reconciled_failure_seq: None,
                courier: "route-pending".to_owned(),
                seat: seat.clone(),
                machine_id: self.cfg.local_machine_id.clone(),
                incarnation_id: None,
                expected_epoch: None,
                sha256s: all_shas,
                is_canary: false,
                pre_spawn: true,
                outcome: CourierOutcome::Deferred,
                reason: Some("no-confirmed-route".into()),
                detail: Some(format!("no confirmed live route for @{seat}")),
                sender_seat: primary_sender,
                hub_owner_seat: hub_owner_seat.clone(),
                failure_kind,
                failure_groups,
                degraded_threshold: Some(self.cfg.degraded_threshold),
            })
            .await
        {
            for f in &rec.committed_failures {
                if self.reconcile_one_failure(f, client).await {
                    self.mark_failure_reconciled(f, client).await;
                }
            }
        }
        earliest_due
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_canary_job(
        &self,
        binding: SeatBinding,
        adapter: &'static str,
        script: PathBuf,
        current_cfg: &RexConfig,
        hub_owner_seat: &SeatName,
        guard: &ProtectedRootsGuard,
        client: &LocalClient,
    ) -> Option<UnixMs> {
        let dispatch_id = next_dispatch_id(&self.cfg.local_machine_id);
        if client
            .call(MailRecordRing {
                phase: RingRecordPhase::Begin,
                dispatch_id: Some(dispatch_id.clone()),
                reconciled_failure_seq: None,
                courier: adapter.to_owned(),
                seat: binding.seat.clone(),
                machine_id: self.cfg.local_machine_id.clone(),
                incarnation_id: Some(binding.incarnation_id.clone()),
                expected_epoch: Some(binding.epoch),
                sha256s: Vec::new(),
                is_canary: true,
                pre_spawn: false,
                outcome: CourierOutcome::Deferred,
                reason: None,
                detail: None,
                sender_seat: None,
                hub_owner_seat: hub_owner_seat.clone(),
                failure_kind: None,
                failure_groups: Vec::new(),
                degraded_threshold: Some(self.cfg.degraded_threshold),
            })
            .await
            .is_err()
        {
            return None;
        }

        let claim = DeliveryClaim::for_canary(&binding, &self.cfg.local_machine_id);
        let reply = invoke_courier_script_guarded(&script, &claim, self.cfg.timeout, guard).await;

        let now_done = rex_wave_runtime::now();
        // Re-check whether the binding is STILL PendingCanary at `binding.epoch` (`Astra B2`):
        // if the session confirmed its canary (promoting to Live) while the adapter was running,
        // a late canary failure MUST NOT fence or expire the confirmed Live route!
        let still_pending_canary = self
            .is_binding_still_authoritative(
                client,
                &binding,
                BindingStatus::PendingCanary,
                now_done,
            )
            .await;

        let max_tries = self.cfg.effective_max_defer_tries(current_cfg);
        let prior_tries = self
            .state
            .lock()
            .await
            .mail
            .canary_defer_state
            .get(&(binding.seat.clone(), binding.incarnation_id.clone()))
            .map_or(0, |r| r.tries);
        let next_tries = prior_tries.saturating_add(1);

        let failure_kind = if !still_pending_canary {
            None
        } else {
            match reply.outcome {
                CourierOutcome::Refused => Some(DeliveryFailureKind::Refused),
                CourierOutcome::Deferred | CourierOutcome::Unknown if next_tries >= max_tries => {
                    Some(DeliveryFailureKind::GiveUp)
                }
                _ => None,
            }
        };

        let ep_key = (
            binding.seat.clone(),
            binding.incarnation_id.clone(),
            binding.epoch,
        );
        let mut scheduled_due: Option<UnixMs> = None;
        if still_pending_canary {
            let mut st = self.state.lock().await;
            if failure_kind.is_some() {
                st.fenced_refused_epochs.insert(ep_key);
            } else {
                match reply.outcome {
                    CourierOutcome::Rung => {
                        st.optimistic_canary_rung.insert(ep_key);
                    }
                    CourierOutcome::Deferred | CourierOutcome::Unknown => {
                        let due = self.cfg.backoff_due_at(current_cfg, now_done, next_tries);
                        st.optimistic_canary_due.insert(ep_key, due);
                        scheduled_due = Some(due);
                    }
                    CourierOutcome::Refused => {}
                }
            }
        }

        let effective_outcome = if !still_pending_canary && reply.outcome == CourierOutcome::Refused
        {
            CourierOutcome::Deferred
        } else {
            reply.outcome
        };

        let recorded = client
            .call(MailRecordRing {
                phase: RingRecordPhase::Complete,
                dispatch_id: Some(dispatch_id),
                reconciled_failure_seq: None,
                courier: adapter.to_owned(),
                seat: binding.seat.clone(),
                machine_id: self.cfg.local_machine_id.clone(),
                incarnation_id: Some(binding.incarnation_id.clone()),
                expected_epoch: Some(binding.epoch),
                sha256s: Vec::new(),
                is_canary: true,
                pre_spawn: reply.pre_spawn,
                outcome: effective_outcome,
                reason: reply.reason.clone(),
                detail: reply.detail.clone(),
                sender_seat: None,
                hub_owner_seat: hub_owner_seat.clone(),
                failure_kind,
                failure_groups: Vec::new(),
                degraded_threshold: Some(self.cfg.degraded_threshold),
            })
            .await;

        if let Ok(rec) = recorded {
            for f in &rec.committed_failures {
                if self.reconcile_one_failure(f, client).await {
                    self.mark_failure_reconciled(f, client).await;
                }
            }
        }

        scheduled_due
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_mail_batch(
        &self,
        binding: SeatBinding,
        adapter: &'static str,
        script: PathBuf,
        candidate_entries: Vec<CommittedMailEntry>,
        skipped_shas_in_sweep: &mut BTreeSet<Sha256Hex>,
        current_cfg: &RexConfig,
        hub_owner_seat: &SeatName,
        guard: &ProtectedRootsGuard,
        client: &LocalClient,
    ) -> (bool, Option<UnixMs>) {
        let candidate_shas: Vec<Sha256Hex> = candidate_entries
            .iter()
            .map(|e| e.message.sha256.clone())
            .collect();
        let dispatch_id = next_dispatch_id(&self.cfg.local_machine_id);

        // Atomically re-validate unacked/unrung/unfailed status in MailBridge and persist
        // `MailEvent::RingDispatched` BEFORE invoking the external script!
        let begun = match client
            .call(MailRecordRing {
                phase: RingRecordPhase::Begin,
                dispatch_id: Some(dispatch_id.clone()),
                reconciled_failure_seq: None,
                courier: adapter.to_owned(),
                seat: binding.seat.clone(),
                machine_id: self.cfg.local_machine_id.clone(),
                incarnation_id: Some(binding.incarnation_id.clone()),
                expected_epoch: Some(binding.epoch),
                sha256s: candidate_shas.clone(),
                is_canary: false,
                pre_spawn: false,
                outcome: CourierOutcome::Deferred,
                reason: None,
                detail: None,
                sender_seat: None,
                hub_owner_seat: hub_owner_seat.clone(),
                failure_kind: None,
                failure_groups: Vec::new(),
                degraded_threshold: Some(self.cfg.degraded_threshold),
            })
            .await
        {
            Ok(b) => b,
            Err(_) => return (false, None),
        };

        let eligible_set: BTreeSet<&Sha256Hex> = begun.eligible_sha256s.iter().collect();
        for sha in &candidate_shas {
            if !eligible_set.contains(sha) {
                // Record candidates rejected by authoritative MailBridge (e.g. concurrent ACK)
                // so subsequent batch iterations in this sweep advance past them (`Astra B1`).
                skipped_shas_in_sweep.insert(sha.clone());
            }
        }
        if begun.eligible_sha256s.is_empty() {
            return (true, None);
        }

        let entries: Vec<CommittedMailEntry> = candidate_entries
            .into_iter()
            .filter(|e| eligible_set.contains(&e.message.sha256))
            .collect();
        let entry_refs: Vec<&CommittedMailEntry> = entries.iter().collect();
        let claim = DeliveryClaim::for_mail(
            &binding,
            &self.cfg.local_machine_id,
            &entry_refs,
            self.cfg.max_referents_per_ring,
        );
        let reply = invoke_courier_script_guarded(&script, &claim, self.cfg.timeout, guard).await;

        let max_tries = self.cfg.effective_max_defer_tries(current_cfg);
        let seat = binding.seat.clone();
        let all_shas: Vec<Sha256Hex> = entries.iter().map(|e| e.message.sha256.clone()).collect();
        let sender_groups = group_entries_by_sender(&entries);
        let primary_sender = sender_groups.first().and_then(|(s, _)| s.clone());

        let mut given_up_entries: Vec<CommittedMailEntry> = Vec::new();
        let mut earliest_due: Option<UnixMs> = None;
        let now_done = rex_wave_runtime::now();

        {
            let mut st = self.state.lock().await;
            st.mail.inflight_dispatches.remove(&dispatch_id);
            match reply.outcome {
                CourierOutcome::Rung => {
                    for sha in &all_shas {
                        st.optimistic_mail_due
                            .insert((seat.clone(), sha.clone()), UnixMs(u64::MAX));
                    }
                    for entry in &entries {
                        st.affinity.record_authored(
                            entry.message.id.clone(),
                            seat.clone(),
                            binding.incarnation_id.clone(),
                        );
                    }
                }
                CourierOutcome::Unknown if reply.pre_spawn => {
                    for entry in &entries {
                        let k = (seat.clone(), entry.message.sha256.clone());
                        let next_tries = st
                            .mail
                            .defer_state
                            .get(&k)
                            .map_or(0, |r| r.tries)
                            .saturating_add(1);
                        if next_tries >= max_tries {
                            st.optimistic_mail_due.insert(k, UnixMs(u64::MAX));
                            st.mail.failed_incarnation_keys.insert((
                                seat.clone(),
                                binding.incarnation_id.clone(),
                                Some(binding.epoch),
                                entry.message.sha256.clone(),
                            ));
                            given_up_entries.push(entry.clone());
                        } else {
                            let due = self.cfg.backoff_due_at(current_cfg, now_done, next_tries);
                            st.optimistic_mail_due.insert(k, due);
                            earliest_due = Some(earliest_due.map_or(due, |cur| cur.min(due)));
                        }
                    }
                }
                CourierOutcome::Unknown => {
                    for sha in &all_shas {
                        st.optimistic_mail_due
                            .insert((seat.clone(), sha.clone()), UnixMs(u64::MAX));
                    }
                    for entry in &entries {
                        st.affinity.record_authored(
                            entry.message.id.clone(),
                            seat.clone(),
                            binding.incarnation_id.clone(),
                        );
                    }
                }
                CourierOutcome::Refused => {
                    st.fenced_refused_epochs.insert((
                        seat.clone(),
                        binding.incarnation_id.clone(),
                        binding.epoch,
                    ));
                    for sha in &all_shas {
                        st.optimistic_mail_due
                            .insert((seat.clone(), sha.clone()), UnixMs(u64::MAX));
                        st.mail.failed_incarnation_keys.insert((
                            seat.clone(),
                            binding.incarnation_id.clone(),
                            Some(binding.epoch),
                            sha.clone(),
                        ));
                    }
                }
                CourierOutcome::Deferred => {
                    for entry in &entries {
                        let k = (seat.clone(), entry.message.sha256.clone());
                        let next_tries = st
                            .mail
                            .defer_state
                            .get(&k)
                            .map_or(0, |r| r.tries)
                            .saturating_add(1);
                        if next_tries >= max_tries {
                            st.optimistic_mail_due.insert(k, UnixMs(u64::MAX));
                            st.mail.failed_incarnation_keys.insert((
                                seat.clone(),
                                binding.incarnation_id.clone(),
                                Some(binding.epoch),
                                entry.message.sha256.clone(),
                            ));
                            given_up_entries.push(entry.clone());
                        } else {
                            let due = self.cfg.backoff_due_at(current_cfg, now_done, next_tries);
                            st.optimistic_mail_due.insert(k, due);
                            earliest_due = Some(earliest_due.map_or(due, |cur| cur.min(due)));
                        }
                    }
                }
            }
        }

        let (failure_kind, failure_groups, effective_reason) = match reply.outcome {
            CourierOutcome::Refused => {
                let r = reply.reason.clone().unwrap_or_else(|| "refused".to_owned());
                let groups = sender_groups
                    .iter()
                    .map(|(s, shas)| MailFailureGroup {
                        sender_seat: s.clone(),
                        sha256s: shas.clone(),
                    })
                    .collect();
                (Some(DeliveryFailureKind::Refused), groups, Some(r))
            }
            CourierOutcome::Deferred | CourierOutcome::Unknown if !given_up_entries.is_empty() => {
                let r = reply
                    .reason
                    .clone()
                    .unwrap_or_else(|| format!("max-tries-{max_tries}"));
                let groups = group_entries_by_sender(&given_up_entries)
                    .into_iter()
                    .map(|(s, shas)| MailFailureGroup {
                        sender_seat: s,
                        sha256s: shas,
                    })
                    .collect();
                (Some(DeliveryFailureKind::GiveUp), groups, Some(r))
            }
            _ => (None, Vec::new(), reply.reason.clone()),
        };

        let recorded = client
            .call(MailRecordRing {
                phase: RingRecordPhase::Complete,
                dispatch_id: Some(dispatch_id),
                reconciled_failure_seq: None,
                courier: adapter.to_owned(),
                seat: seat.clone(),
                machine_id: self.cfg.local_machine_id.clone(),
                incarnation_id: Some(binding.incarnation_id.clone()),
                expected_epoch: Some(binding.epoch),
                sha256s: all_shas,
                is_canary: false,
                pre_spawn: reply.pre_spawn,
                outcome: reply.outcome,
                reason: effective_reason,
                detail: reply.detail,
                sender_seat: primary_sender,
                hub_owner_seat: hub_owner_seat.clone(),
                failure_kind,
                failure_groups,
                degraded_threshold: Some(self.cfg.degraded_threshold),
            })
            .await;

        if let Ok(rec) = recorded {
            for f in &rec.committed_failures {
                if self.reconcile_one_failure(f, client).await {
                    self.mark_failure_reconciled(f, client).await;
                }
            }
        }

        let can_continue_batches = reply.outcome == CourierOutcome::Rung
            || (reply.outcome == CourierOutcome::Unknown && !reply.pre_spawn);
        (can_continue_batches, earliest_due)
    }
}

fn group_entries_by_sender(
    entries: &[CommittedMailEntry],
) -> Vec<(Option<SeatName>, Vec<Sha256Hex>)> {
    let mut map: BTreeMap<Option<SeatName>, Vec<Sha256Hex>> = BTreeMap::new();
    for entry in entries {
        let parsed = SeatName::parse(normalize_seat_token(&entry.message.from)).ok();
        map.entry(parsed)
            .or_default()
            .push(entry.message.sha256.clone());
    }
    map.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_courier_stdout_fails_safe_to_unknown() {
        assert_eq!(
            parse_courier_stdout(br#"{"outcome":"rung"}"#),
            CourierReply {
                outcome: CourierOutcome::Rung,
                reason: None,
                detail: None,
                pre_spawn: false,
            }
        );
        assert_eq!(
            parse_courier_stdout(
                br#"{"outcome":"refused","reason":"address-gone","detail":"thread archived"}"#
            ),
            CourierReply {
                outcome: CourierOutcome::Refused,
                reason: Some("address-gone".into()),
                detail: Some("thread archived".into()),
                pre_spawn: false,
            }
        );
        assert_eq!(
            parse_courier_stdout(b"not json at all").outcome,
            CourierOutcome::Unknown
        );
        assert_eq!(parse_courier_stdout(b"").outcome, CourierOutcome::Unknown);
        assert_eq!(
            parse_courier_stdout(br#"{"outcome":"bogus"}"#).outcome,
            CourierOutcome::Unknown
        );
    }

    #[test]
    fn canary_secret_is_redacted_from_reason_and_detail() {
        let raw = CourierReply {
            outcome: CourierOutcome::Unknown,
            reason: Some("bad token rwc_secret123".into()),
            detail: Some("stdin had canary_token=rwc_secret123 on failure".into()),
            pre_spawn: false,
        };
        let clean = raw.redact_secret(Some("rwc_secret123"));
        assert!(!clean.reason.as_deref().unwrap().contains("rwc_secret123"));
        assert!(!clean.detail.as_deref().unwrap().contains("rwc_secret123"));
        assert!(
            clean
                .detail
                .as_deref()
                .unwrap()
                .contains("[REDACTED_CANARY]")
        );

        // Also verify boundary-straddling canary token (>512 chars) is redacted BEFORE truncation (`Astra S12`)!
        let boundary_json = format!(
            r#"{{"outcome":"unknown","reason":"{}rwc_boundary_tok_99","detail":"{}rwc_boundary_tok_99"}}"#,
            "r".repeat(120),
            "d".repeat(505),
        );
        let parsed =
            parse_courier_stdout_redacted(boundary_json.as_bytes(), Some("rwc_boundary_tok_99"));
        assert!(
            !parsed
                .reason
                .as_deref()
                .unwrap_or_default()
                .contains("rwc_boundary"),
            "reason leaked truncated canary prefix: {:?}",
            parsed.reason
        );
        assert!(
            !parsed
                .detail
                .as_deref()
                .unwrap_or_default()
                .contains("rwc_boundary"),
            "detail leaked truncated canary prefix: {:?}",
            parsed.detail
        );
    }

    #[test]
    fn self_watcher_never_maps_to_external_push_courier() {
        assert_eq!(
            resolve_adapter_name(&TransportEndpoint::SelfWatcher {
                harness: "custom-knocker".into(),
                thread_id: None,
                pid: None,
                proc_start: None,
            }),
            None
        );
        assert_eq!(
            resolve_adapter_name(&TransportEndpoint::T3Thread {
                thread_id: T3ThreadId::parse("thr-1").unwrap(),
            }),
            Some("t3-thread")
        );
        assert_eq!(
            resolve_adapter_name(&TransportEndpoint::BbThread {
                thread_id: BbThreadId::parse("bb-1").unwrap(),
            }),
            Some("bb-thread")
        );
        assert_eq!(
            resolve_adapter_name(&TransportEndpoint::UdsSession {
                conn_id: 1,
                client: "codex-app-server".into(),
            }),
            Some("codex-app-server")
        );
        assert_eq!(
            resolve_adapter_name(&TransportEndpoint::UdsSession {
                conn_id: 2,
                client: "codex:thr_abc123".into(),
            }),
            Some("codex-app-server")
        );
    }
}
