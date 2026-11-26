//! Executable `couriers.d/` output-adapter engine, multi-machine LWW route sync,
//! and daemon [`CourierHub`] reactor conforming to `specs/mail/MailHub.tla` and
//! `specs/mail/MailMachines.tla`.
//!
//! Security & formal invariants enforced here:
//! - **Body-blind claims:** [`DeliveryClaim`] carries metadata referents only
//!   (`sha256`, `id`, `from`, `subject`, `path`, `priority`, `created_at_ms`),
//!   never raw letter bodies.
//! - **No-symlink execution & `protected-roots`:** every adapter in `couriers.d/`
//!   is verified with `rex_wave_fs` `O_NOFOLLOW` regular-file checks and
//!   [`ProtectedRootsGuard`] before spawning.
//! - **Safe process-group timeouts:** adapters spawn in their own process group
//!   (`Command::process_group(0)`, zero `unsafe`) and are reaped with
//!   `SIGTERM` -> grace -> `SIGKILL` via `rustix::process::kill_process_group`.
//! - **`OneMachineRings`:** only bindings whose `machine_id` matches the local
//!   hub's `machine_id` are rung locally.
//! - **`NoDoubleRing` + `DegradedLoud`:** `"unknown"` outcomes count as rung on
//!   `(seat, sha256)` and are never retried; `K` consecutive `"unknown"` outcomes
//!   mark the courier degraded and escalate `AdapterDegraded` to `hub_owner_seat`.
//! - **Canary-gated routes (`MailHub.tla`):** `BindingStatus::PendingCanary`
//!   bindings receive only a canary ring and are never eligible for normal mail
//!   until confirmed via `PresenceConfirmCanary`.
//! - **First-occurrence loud `refused` + Operator attention rule:** on the first
//!   `"refused"` outcome (e.g. `address-gone`), the dead binding is immediately
//!   expired and `hub_owner_seat` + sender are notified loudly without ever
//!   raising an interrupting card on the human `Operator`.

use crate::acks::normalize_seat_token;
use crate::dissolved::{CommittedMailEntry, DEFAULT_COURIER_DEGRADED_THRESHOLD, MailState};
use crate::presence::{
    IncarnationAffinity, Presence, PresenceState, select_binding_for_delivery_on_machine,
};
use crate::sha256_short16;
use rex_wave_api::config::RexConfig;
use rex_wave_api::mail::{
    CourierOutcome, DeliveryFailureKind, LetterPriority, MailEvent, MailRecordRing, MailSend,
};
use rex_wave_api::notify::{InboxRaise, NotificationDraft, NotifyKind};
use rex_wave_api::presence::{
    BindingStatus, HouseProfile, PresenceEvent, PresenceExpire, SeatBinding, TransportEndpoint,
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
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, watch};

/// Wire schema version for body-blind [`DeliveryClaim`] payloads sent on courier stdin.
pub const COURIER_CLAIM_SCHEMA: u8 = 1;
/// Default consecutive `"unknown"` outcomes before marking a courier degraded (`MailMachines.tla` `K`).
pub const DEFAULT_DEGRADED_THRESHOLD: u32 = DEFAULT_COURIER_DEGRADED_THRESHOLD;
/// Default maximum `"deferred"` retries before loud `GiveUp` (`MailHub.tla` `MaxTries`).
pub const DEFAULT_MAX_DEFER_TRIES: u32 = 5;
/// Maximum letter referents batched in one courier ring claim.
pub const DEFAULT_MAX_REFERENTS_PER_RING: usize = 32;
/// Default wall-clock timeout enforced per courier script invocation.
pub const DEFAULT_COURIER_TIMEOUT_MS: u64 = 10_000;
/// Grace window between process-group `SIGTERM` and `SIGKILL` on timeout.
const KILL_GRACE_MS: u64 = 200;
/// Hard cap on stdout bytes read from a courier script.
pub const MAX_COURIER_STDOUT_BYTES: usize = 64 * 1024;
/// Hard cap on stderr bytes captured for failure diagnostics.
const MAX_COURIER_STDERR_BYTES: usize = 8 * 1024;
/// Sender address stamped on system delivery-failure notices committed by [`CourierHub`].
pub const COURIER_HUB_SENDER_ADDRESS: &str = "system/courier_hub";

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
}

impl CourierReply {
    pub fn unknown(reason: impl Into<String>, detail: Option<String>) -> Self {
        Self {
            outcome: CourierOutcome::Unknown,
            reason: Some(reason.into()),
            detail,
        }
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
            reason: clean_opt_text(reply.reason, 128),
            detail: clean_opt_text(reply.detail, 512),
        },
        Err(err) => CourierReply::unknown(
            "malformed-stdout",
            Some(format!("invalid courier JSON reply: {err}")),
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
            if norm == "codex-app-server" || norm == "codex" {
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

/// Resolves one named executable adapter inside `couriers_dir` with `O_NOFOLLOW`
/// and `protected-roots` verification.
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
    let verified_dir = match guard {
        Some(g) => g
            .verify_dir(dir)
            .map_err(|e| format!("couriers_dir rejected by protected-roots: {e}"))?,
        None => dir.to_path_buf(),
    };
    let full = verified_dir.join(adapter_name);
    match rex_wave_fs::entry_kind(&full) {
        Ok(EntryKind::File) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Ok(EntryKind::Symlink) => {
            return Err(format!(
                "refusing symlink courier adapter {}",
                full.display()
            ));
        }
        Ok(_) => {
            return Err(format!(
                "courier adapter {} is not a regular file",
                full.display()
            ));
        }
        Err(e) => return Err(format!("inspect courier {}: {e}", full.display())),
    }
    if let Some(g) = guard
        && let Err(e) = g.check(&full, &full)
    {
        return Err(format!("courier adapter rejected by protected-roots: {e}"));
    }
    match is_executable_file(&full) {
        Ok(true) => Ok(Some(full)),
        Ok(false) => Ok(None),
        Err(e) => Err(format!("open courier {}: {e}", full.display())),
    }
}

async fn read_bounded<R: AsyncRead + Unpin>(mut reader: R, limit: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                if out.len() < limit {
                    let take = (limit - out.len()).min(n);
                    out.extend_from_slice(&buf[..take]);
                }
            }
            Err(_) => break,
        }
    }
    out
}

#[cfg(unix)]
fn signal_process_group(raw_pid: u32, sig: rustix::process::Signal) {
    if let Ok(raw_i32) = i32::try_from(raw_pid)
        && let Some(pid) = rustix::process::Pid::from_raw(raw_i32)
    {
        let _ = rustix::process::kill_process_group(pid, sig);
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
/// writes the body-blind JSON [`DeliveryClaim`] to `stdin`, enforces `timeout`
/// with process-group `SIGTERM`/`SIGKILL` reaping, and parses the JSON outcome.
///
/// Non-zero exits, timeouts, spawn failures, and malformed stdout all fail safe
/// to [`CourierOutcome::Unknown`].
pub async fn invoke_courier_script(
    path: &Path,
    claim: &DeliveryClaim,
    timeout: Duration,
    guard: Option<&ProtectedRootsGuard>,
) -> CourierReply {
    if rex_wave_fs::entry_kind(path).ok() != Some(EntryKind::File) {
        return CourierReply::unknown(
            "invalid-adapter",
            Some(format!("{} is not a regular file", path.display())),
        );
    }
    let exec_path = if let Some(g) = guard {
        if let Err(e) = g.check(path, path) {
            return CourierReply::unknown(
                "protected-root",
                Some(format!("adapter rejected by protected-roots: {e}")),
            );
        }
        let Some(parent) = path.parent() else {
            return CourierReply::unknown(
                "invalid-adapter",
                Some(format!("{} has no parent directory", path.display())),
            );
        };
        let Some(leaf) = path.file_name() else {
            return CourierReply::unknown(
                "invalid-adapter",
                Some(format!("{} has no file name", path.display())),
            );
        };
        match g.verify_dir(parent) {
            Ok(canon_parent) => {
                let candidate = canon_parent.join(leaf);
                if let Err(e) = g.check(&candidate, path) {
                    return CourierReply::unknown(
                        "protected-root",
                        Some(format!("adapter rejected by protected-roots: {e}")),
                    );
                }
                candidate
            }
            Err(e) => {
                return CourierReply::unknown(
                    "protected-root",
                    Some(format!("adapter parent rejected by protected-roots: {e}")),
                );
            }
        }
    } else {
        path.to_path_buf()
    };
    match is_executable_file(&exec_path) {
        Ok(true) => {}
        Ok(false) => {
            return CourierReply::unknown(
                "not-executable",
                Some(format!("{} is not executable", exec_path.display())),
            );
        }
        Err(e) => {
            return CourierReply::unknown("open-failed", Some(e.to_string()));
        }
    }

    let mut payload = match serde_json::to_vec(claim) {
        Ok(bytes) => bytes,
        Err(e) => return CourierReply::unknown("serialize-failed", Some(e.to_string())),
    };
    payload.push(b'\n');

    let mut cmd = tokio::process::Command::new(&exec_path);
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .env_remove("REX_WAVE_TOKEN")
        .env_remove("REX_WAVE_BB_TOKEN")
        .env_remove("REX_WAVE_T3_TOKEN")
        .env_remove("REX_WAVE_WORKER_THREAD_ID")
        .env("REX_WAVE_COURIER_SCHEMA", COURIER_CLAIM_SCHEMA.to_string())
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
            return CourierReply::unknown(
                "spawn-failed",
                Some(format!("spawn {}: {e}", exec_path.display())),
            );
        }
    };
    let child_pid = child.id();

    let mut stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let stdout_task = tokio::spawn(async move {
        match stdout {
            Some(r) => read_bounded(r, MAX_COURIER_STDOUT_BYTES).await,
            None => Vec::new(),
        }
    });
    let stderr_task = tokio::spawn(async move {
        match stderr {
            Some(r) => read_bounded(r, MAX_COURIER_STDERR_BYTES).await,
            None => Vec::new(),
        }
    });
    let mut subproc_guard = CourierSubprocessGuard {
        armed: true,
        child_pid,
        stdout_handle: stdout_task.abort_handle(),
        stderr_handle: stderr_task.abort_handle(),
    };

    let wait_fut = async {
        if let Some(mut s) = stdin.take() {
            let _ = s.write_all(&payload).await;
            let _ = s.shutdown().await;
            drop(s);
        }
        child.wait().await
    };

    match tokio::time::timeout(timeout, wait_fut).await {
        Ok(Ok(status)) => {
            subproc_guard.armed = false;
            let stdout_bytes = stdout_task.await.unwrap_or_default();
            let stderr_bytes = stderr_task.await.unwrap_or_default();
            if !status.success() {
                let code_str = status
                    .code()
                    .map_or_else(|| "signal".to_owned(), |c| c.to_string());
                let stderr_text = String::from_utf8_lossy(&stderr_bytes).trim().to_owned();
                let detail = if !stderr_text.is_empty() {
                    clean_opt_text(Some(stderr_text), 512)
                } else {
                    let stdout_text = String::from_utf8_lossy(&stdout_bytes).trim().to_owned();
                    clean_opt_text(Some(stdout_text), 512)
                };
                return CourierReply::unknown(format!("exit-{code_str}"), detail);
            }
            parse_courier_stdout(&stdout_bytes)
        }
        Ok(Err(e)) => {
            subproc_guard.armed = false;
            stdout_task.abort();
            stderr_task.abort();
            CourierReply::unknown("wait-failed", Some(e.to_string()))
        }
        Err(_) => {
            // Router-enforced timeout: reap entire process group cleanly.
            #[cfg(unix)]
            if let Some(pid) = child_pid {
                signal_process_group(pid, rustix::process::Signal::TERM);
            }
            let exited_gracefully =
                tokio::time::timeout(Duration::from_millis(KILL_GRACE_MS), child.wait())
                    .await
                    .is_ok();
            if !exited_gracefully {
                #[cfg(unix)]
                if let Some(pid) = child_pid {
                    signal_process_group(pid, rustix::process::Signal::KILL);
                }
                let _ = child.kill().await;
                let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
            }
            subproc_guard.armed = false;
            stdout_task.abort();
            stderr_task.abort();
            CourierReply::unknown(
                "timeout",
                Some(format!(
                    "adapter {} exceeded {}ms timeout",
                    exec_path.display(),
                    timeout.as_millis()
                )),
            )
        }
    }
}

/// One seat's machine-owned route claim synchronized across machines via
/// `(claimed_at, machine_id)` last-writer-wins ordering (`MailMachines.tla`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteClaim {
    pub seat: SeatName,
    pub machine_id: String,
    pub incarnation_id: IncarnationId,
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
            endpoint: binding.endpoint.clone(),
            claimed_at: binding.bound_at,
            confirmed: binding.status == BindingStatus::Live,
        }
    }

    fn lww_key(&self) -> (UnixMs, &str) {
        (self.claimed_at, self.machine_id.as_str())
    }
}

/// Deterministic `(claimed_at, machine_id)` LWW route table across machines.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteSyncTable {
    claims: BTreeMap<SeatName, RouteClaim>,
}

impl RouteSyncTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or replaces a seat's route claim if `(claimed_at, machine_id)` wins.
    /// Returns `true` when the table changed.
    pub fn apply_claim(&mut self, claim: RouteClaim) -> bool {
        match self.claims.get(&claim.seat) {
            None => {
                self.claims.insert(claim.seat.clone(), claim);
                true
            }
            Some(existing) => {
                let cmp = claim.lww_key().cmp(&existing.lww_key());
                if cmp.is_gt()
                    || (cmp.is_eq()
                        && (claim.confirmed && !existing.confirmed
                            || claim.incarnation_id != existing.incarnation_id))
                {
                    self.claims.insert(claim.seat.clone(), claim);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Removes a claim immediately when a local session disconnects (`UdsSession`
    /// / `presence hold` exit) without waiting for peer sync.
    pub fn drop_local(&mut self, seat: &SeatName, local_machine_id: &str) -> bool {
        if self
            .claims
            .get(seat)
            .is_some_and(|c| c.machine_id == local_machine_id)
        {
            self.claims.remove(seat);
            true
        } else {
            false
        }
    }

    /// Merges all route claims from `peer` using `(claimed_at, machine_id)` LWW ordering.
    pub fn merge_from(&mut self, peer: &RouteSyncTable) {
        for claim in peer.claims.values() {
            self.apply_claim(claim.clone());
        }
    }

    pub fn get(&self, seat: &SeatName) -> Option<&RouteClaim> {
        self.claims.get(seat)
    }

    pub fn owner_machine(&self, seat: &SeatName) -> Option<&str> {
        self.claims.get(seat).map(|c| c.machine_id.as_str())
    }

    pub fn is_owned_by_machine(&self, seat: &SeatName, machine_id: &str) -> bool {
        self.claims
            .get(seat)
            .is_some_and(|c| c.machine_id == machine_id && c.confirmed)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SeatName, &RouteClaim)> {
        self.claims.iter()
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

/// Resolves the effective `couriers.d/` directory for `cfg`.
pub fn effective_couriers_dir_for(
    cfg: &RexConfig,
    config_path: Option<&Path>,
    fallback_dir: Option<&Path>,
) -> Option<PathBuf> {
    if let Some(ref custom) = cfg.mail.couriers_dir {
        return Some(custom.clone());
    }
    if let Some(cfg_path) = config_path
        && let Some(parent) = cfg_path.parent()
    {
        return Some(parent.join("couriers.d"));
    }
    fallback_dir.map(Path::to_path_buf)
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
    pub degraded_threshold: u32,
    pub max_defer_tries: u32,
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
            degraded_threshold: DEFAULT_DEGRADED_THRESHOLD,
            max_defer_tries: DEFAULT_MAX_DEFER_TRIES,
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

    pub fn with_degraded_threshold(mut self, k: u32) -> Self {
        self.degraded_threshold = k.max(1);
        self
    }

    pub fn with_max_defer_tries(mut self, max_tries: u32) -> Self {
        self.max_defer_tries = max_tries.max(1);
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
}

struct CourierHubState {
    mail: MailState,
    presence: PresenceState,
    caught_up: bool,
    canary_dispatched: BTreeSet<(SeatName, IncarnationId, u64)>,
    in_flight_mail: BTreeSet<(SeatName, Sha256Hex)>,
    defer_tries: BTreeMap<(SeatName, Sha256Hex), u32>,
    affinity: IncarnationAffinity,
}

struct CourierHubInner {
    cfg: CourierHubConfig,
    config_rx: watch::Receiver<RexConfig>,
    state: Mutex<CourierHubState>,
    sweep_lock: Mutex<()>,
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
                canary_dispatched: BTreeSet::new(),
                in_flight_mail: BTreeSet::new(),
                defer_tries: BTreeMap::new(),
                affinity: IncarnationAffinity::default(),
            }),
            sweep_lock: Mutex::new(()),
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
                    inner.sweep(&client).await;
                }
                Frame::Committed(env) => {
                    let should_sweep = {
                        let mut st = inner.state.lock().await;
                        match &env.event {
                            Event::Mail(me) => {
                                st.mail.apply(&env);
                                if let MailEvent::Acked {
                                    seat: Some(seat),
                                    sha256,
                                    ..
                                } = me
                                {
                                    st.defer_tries.remove(&(seat.clone(), sha256.clone()));
                                }
                            }
                            Event::Presence(pe) => {
                                let meta = EntryMeta {
                                    seq: env.seq,
                                    at: env.at,
                                    by: env.by.clone(),
                                };
                                Presence::evolve(&mut st.presence, pe, &meta);
                                if let PresenceEvent::Unbound {
                                    seat,
                                    incarnation_id,
                                    ..
                                } = pe
                                {
                                    st.canary_dispatched.retain(|(s, inc, _)| {
                                        !(s == seat && inc == incarnation_id)
                                    });
                                }
                            }
                            Event::Config(_) => {}
                            _ => {}
                        }
                        st.caught_up
                    };
                    if should_sweep {
                        inner.sweep(&client).await;
                    }
                }
                _ => {}
            }
            Ok(())
        }
    }
}

struct PlannedCanary {
    binding: SeatBinding,
    adapter: &'static str,
    script: PathBuf,
    hub_owner_seat: SeatName,
}

struct PlannedMailRing {
    binding: SeatBinding,
    adapter: &'static str,
    script: PathBuf,
    entries: Vec<CommittedMailEntry>,
    hub_owner_seat: SeatName,
}

impl CourierHubInner {
    fn resolve_guard(&self) -> Option<ProtectedRootsGuard> {
        match &self.cfg.protection {
            Some(p) => ProtectedRootsGuard::resolve(p).ok(),
            None => load_protected_roots()
                .ok()
                .and_then(|p| ProtectedRootsGuard::resolve(&p).ok()),
        }
    }

    async fn sweep(&self, client: &LocalClient) {
        let _sweep_guard = self.sweep_lock.lock().await;
        let current_cfg = self.config_rx.borrow().clone();
        let Some(couriers_dir) = effective_couriers_dir_for(
            &current_cfg,
            self.cfg.config_path.as_deref(),
            self.cfg.default_couriers_dir.as_deref(),
        ) else {
            return;
        };
        let guard = self.resolve_guard();
        let now = rex_wave_runtime::now();
        let hub_owner_seat = current_cfg.mail.hub_owner_seat.clone();

        // Phase A: Canary rings for PendingCanary bindings owned by local_machine_id.
        let planned_canaries = {
            let mut st = self.state.lock().await;
            let candidates: Vec<SeatBinding> = st
                .presence
                .pending_canary_bindings_for_machine(&self.cfg.local_machine_id, now)
                .into_iter()
                .cloned()
                .collect();
            let mut planned = Vec::new();
            for binding in candidates {
                let key = (
                    binding.seat.clone(),
                    binding.incarnation_id.clone(),
                    binding.epoch,
                );
                if st.canary_dispatched.contains(&key) {
                    continue;
                }
                let Some(adapter) = resolve_adapter_name(&binding.endpoint) else {
                    continue;
                };
                let Ok(Some(script)) =
                    resolve_courier_executable(&couriers_dir, adapter, guard.as_ref())
                else {
                    continue;
                };
                st.canary_dispatched.insert(key);
                planned.push(PlannedCanary {
                    binding,
                    adapter,
                    script,
                    hub_owner_seat: hub_owner_seat.clone(),
                });
            }
            planned
        };

        for job in planned_canaries {
            self.execute_canary_job(job, guard.as_ref(), client).await;
        }

        // Phase B: Normal mail delivery rings for Live bindings owned by local_machine_id.
        let planned_mail = {
            let mut st = self.state.lock().await;
            let mut seats_with_mail: BTreeSet<SeatName> = BTreeSet::new();
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
                    seats_with_mail.insert(recip.clone());
                }
            }

            let mut planned = Vec::new();
            for seat in seats_with_mail {
                let Some(binding) = select_binding_for_delivery_on_machine(
                    &st.presence,
                    &seat,
                    None,
                    None,
                    &st.affinity,
                    Some(&self.cfg.local_machine_id),
                    now,
                )
                .cloned() else {
                    continue;
                };
                let Some(adapter) = resolve_adapter_name(&binding.endpoint) else {
                    // SelfWatcher / plain UdsSession: age unacked mail without external push couriers.
                    continue;
                };
                let Ok(Some(script)) =
                    resolve_courier_executable(&couriers_dir, adapter, guard.as_ref())
                else {
                    continue;
                };

                let horizon = st.presence.seat(&seat).and_then(|r| r.mail_horizon_at);
                let ringable_shas = st.mail.ringable_for_seat(&seat);
                let mut entries: Vec<CommittedMailEntry> = ringable_shas
                    .into_iter()
                    .filter(|sha| {
                        let k = (seat.clone(), sha.clone());
                        !st.mail.rung_keys.contains(&k)
                            && !st.mail.failed_keys.contains(&k)
                            && !st.in_flight_mail.contains(&k)
                    })
                    .filter_map(|sha| st.mail.messages.get(&sha).cloned())
                    .filter(|entry| {
                        let is_courier_system_notice = entry.committed_by
                            == (Principal::System {
                                part: SystemPart::CourierHub,
                            })
                            && entry.message.from == COURIER_HUB_SENDER_ADDRESS;
                        !is_courier_system_notice
                            && horizon.is_none_or(|h| entry.message.created_at >= h)
                    })
                    .collect();

                if entries.is_empty() {
                    continue;
                }
                entries.sort_by_key(|e| (e.message.created_at, e.seq));
                entries.truncate(self.cfg.max_referents_per_ring);
                for entry in &entries {
                    st.in_flight_mail
                        .insert((seat.clone(), entry.message.sha256.clone()));
                }
                planned.push(PlannedMailRing {
                    binding,
                    adapter,
                    script,
                    entries,
                    hub_owner_seat: hub_owner_seat.clone(),
                });
            }
            planned
        };

        for job in planned_mail {
            self.execute_mail_job(job, guard.as_ref(), client).await;
        }
    }

    async fn execute_canary_job(
        &self,
        job: PlannedCanary,
        guard: Option<&ProtectedRootsGuard>,
        client: &LocalClient,
    ) {
        let claim = DeliveryClaim::for_canary(&job.binding, &self.cfg.local_machine_id);
        let reply = invoke_courier_script(&job.script, &claim, self.cfg.timeout, guard).await;

        let failure_kind =
            (reply.outcome == CourierOutcome::Refused).then_some(DeliveryFailureKind::Refused);
        let recorded = client
            .call(MailRecordRing {
                courier: job.adapter.to_owned(),
                seat: job.binding.seat.clone(),
                machine_id: self.cfg.local_machine_id.clone(),
                incarnation_id: Some(job.binding.incarnation_id.clone()),
                sha256s: Vec::new(),
                is_canary: true,
                outcome: reply.outcome,
                reason: reply.reason.clone(),
                detail: reply.detail.clone(),
                sender_seat: None,
                hub_owner_seat: job.hub_owner_seat.clone(),
                failure_kind,
            })
            .await;

        if reply.outcome == CourierOutcome::Refused {
            let reason = reply.reason.clone().unwrap_or_else(|| "refused".to_owned());
            let _ = client
                .call(PresenceExpire {
                    seat: job.binding.seat.clone(),
                    incarnation_id: job.binding.incarnation_id.clone(),
                    expected_epoch: Some(job.binding.epoch),
                    lease_expires_at: None,
                    reason: format!("canary refused by {}: {}", job.adapter, reason),
                })
                .await;
            self.escalate_failure(
                client,
                DeliveryFailureKind::Refused,
                job.adapter,
                &job.binding.seat,
                &reason,
                reply.detail.as_deref(),
                &[],
                None,
                &job.hub_owner_seat,
            )
            .await;
        } else if let Ok(rec) = recorded
            && rec.degraded_newly_triggered
        {
            let reason = reply
                .reason
                .clone()
                .unwrap_or_else(|| "consecutive-unknown".to_owned());
            self.escalate_failure(
                client,
                DeliveryFailureKind::AdapterDegraded,
                job.adapter,
                &job.binding.seat,
                &reason,
                reply.detail.as_deref(),
                &[],
                None,
                &job.hub_owner_seat,
            )
            .await;
        }
    }

    async fn execute_mail_job(
        &self,
        job: PlannedMailRing,
        guard: Option<&ProtectedRootsGuard>,
        client: &LocalClient,
    ) {
        let entry_refs: Vec<&CommittedMailEntry> = job.entries.iter().collect();
        let claim = DeliveryClaim::for_mail(
            &job.binding,
            &self.cfg.local_machine_id,
            &entry_refs,
            self.cfg.max_referents_per_ring,
        );
        let reply = invoke_courier_script(&job.script, &claim, self.cfg.timeout, guard).await;

        let seat = job.binding.seat.clone();
        let all_shas: Vec<Sha256Hex> = job
            .entries
            .iter()
            .map(|e| e.message.sha256.clone())
            .collect();

        // Release in-flight marks and update optimistic local state immediately so
        // rapid frames never double-ring before StreamId::Mail loops back.
        let mut given_up_entries: Vec<CommittedMailEntry> = Vec::new();
        {
            let mut st = self.state.lock().await;
            for sha in &all_shas {
                st.in_flight_mail.remove(&(seat.clone(), sha.clone()));
            }
            match reply.outcome {
                CourierOutcome::Rung | CourierOutcome::Unknown => {
                    for sha in &all_shas {
                        st.defer_tries.remove(&(seat.clone(), sha.clone()));
                        st.mail.rung_keys.insert((seat.clone(), sha.clone()));
                    }
                    for entry in &job.entries {
                        st.affinity.record_authored(
                            entry.message.id.clone(),
                            seat.clone(),
                            job.binding.incarnation_id.clone(),
                        );
                    }
                }
                CourierOutcome::Refused => {
                    for sha in &all_shas {
                        st.defer_tries.remove(&(seat.clone(), sha.clone()));
                        st.mail.failed_keys.insert((seat.clone(), sha.clone()));
                    }
                }
                CourierOutcome::Deferred => {
                    for entry in &job.entries {
                        let key = (seat.clone(), entry.message.sha256.clone());
                        let tries = st.defer_tries.entry(key.clone()).or_insert(0);
                        *tries = tries.saturating_add(1);
                        if *tries >= self.cfg.max_defer_tries {
                            st.defer_tries.remove(&key);
                            st.mail.failed_keys.insert(key);
                            given_up_entries.push(entry.clone());
                        }
                    }
                }
            }
        }

        let sender_groups = group_entries_by_sender(&job.entries);
        let primary_sender = sender_groups.first().and_then(|(s, _)| s.clone());

        match reply.outcome {
            CourierOutcome::Rung => {
                let _ = client
                    .call(MailRecordRing {
                        courier: job.adapter.to_owned(),
                        seat,
                        machine_id: self.cfg.local_machine_id.clone(),
                        incarnation_id: Some(job.binding.incarnation_id.clone()),
                        sha256s: all_shas,
                        is_canary: false,
                        outcome: CourierOutcome::Rung,
                        reason: reply.reason,
                        detail: reply.detail,
                        sender_seat: primary_sender,
                        hub_owner_seat: job.hub_owner_seat,
                        failure_kind: None,
                    })
                    .await;
            }
            CourierOutcome::Unknown => {
                let recorded = client
                    .call(MailRecordRing {
                        courier: job.adapter.to_owned(),
                        seat: seat.clone(),
                        machine_id: self.cfg.local_machine_id.clone(),
                        incarnation_id: Some(job.binding.incarnation_id.clone()),
                        sha256s: all_shas.clone(),
                        is_canary: false,
                        outcome: CourierOutcome::Unknown,
                        reason: reply.reason.clone(),
                        detail: reply.detail.clone(),
                        sender_seat: primary_sender,
                        hub_owner_seat: job.hub_owner_seat.clone(),
                        failure_kind: None,
                    })
                    .await;
                if let Ok(rec) = recorded
                    && rec.degraded_newly_triggered
                {
                    let reason = reply
                        .reason
                        .clone()
                        .unwrap_or_else(|| "consecutive-unknown".to_owned());
                    self.escalate_failure(
                        client,
                        DeliveryFailureKind::AdapterDegraded,
                        job.adapter,
                        &seat,
                        &reason,
                        reply.detail.as_deref(),
                        &all_shas,
                        None,
                        &job.hub_owner_seat,
                    )
                    .await;
                }
            }
            CourierOutcome::Refused => {
                let reason = reply.reason.clone().unwrap_or_else(|| "refused".to_owned());
                // Record failure FIRST so HeraldView clears pending_mail before the
                // dead binding expires and never raises MailUnrung to Operator.
                for (sender_seat, shas) in &sender_groups {
                    let _ = client
                        .call(MailRecordRing {
                            courier: job.adapter.to_owned(),
                            seat: seat.clone(),
                            machine_id: self.cfg.local_machine_id.clone(),
                            incarnation_id: Some(job.binding.incarnation_id.clone()),
                            sha256s: shas.clone(),
                            is_canary: false,
                            outcome: CourierOutcome::Refused,
                            reason: Some(reason.clone()),
                            detail: reply.detail.clone(),
                            sender_seat: sender_seat.clone(),
                            hub_owner_seat: job.hub_owner_seat.clone(),
                            failure_kind: Some(DeliveryFailureKind::Refused),
                        })
                        .await;
                }
                // Immediately retire the dead binding on first refusal (zero retries).
                let _ = client
                    .call(PresenceExpire {
                        seat: seat.clone(),
                        incarnation_id: job.binding.incarnation_id.clone(),
                        expected_epoch: Some(job.binding.epoch),
                        lease_expires_at: None,
                        reason: format!("courier {} refused: {}", job.adapter, reason),
                    })
                    .await;
                // Escalate loudly to hub_owner_seat and every distinct sender (NEVER Operator).
                for (sender_seat, shas) in &sender_groups {
                    self.escalate_failure(
                        client,
                        DeliveryFailureKind::Refused,
                        job.adapter,
                        &seat,
                        &reason,
                        reply.detail.as_deref(),
                        shas,
                        sender_seat.as_ref(),
                        &job.hub_owner_seat,
                    )
                    .await;
                }
            }
            CourierOutcome::Deferred => {
                if given_up_entries.is_empty() {
                    let _ = client
                        .call(MailRecordRing {
                            courier: job.adapter.to_owned(),
                            seat,
                            machine_id: self.cfg.local_machine_id.clone(),
                            incarnation_id: Some(job.binding.incarnation_id.clone()),
                            sha256s: all_shas,
                            is_canary: false,
                            outcome: CourierOutcome::Deferred,
                            reason: reply.reason,
                            detail: reply.detail,
                            sender_seat: primary_sender,
                            hub_owner_seat: job.hub_owner_seat,
                            failure_kind: None,
                        })
                        .await;
                } else {
                    let reason = reply
                        .reason
                        .clone()
                        .unwrap_or_else(|| format!("max-tries-{}", self.cfg.max_defer_tries));
                    let giveup_groups = group_entries_by_sender(&given_up_entries);
                    for (sender_seat, shas) in &giveup_groups {
                        let _ = client
                            .call(MailRecordRing {
                                courier: job.adapter.to_owned(),
                                seat: seat.clone(),
                                machine_id: self.cfg.local_machine_id.clone(),
                                incarnation_id: Some(job.binding.incarnation_id.clone()),
                                sha256s: shas.clone(),
                                is_canary: false,
                                outcome: CourierOutcome::Deferred,
                                reason: Some(reason.clone()),
                                detail: reply.detail.clone(),
                                sender_seat: sender_seat.clone(),
                                hub_owner_seat: job.hub_owner_seat.clone(),
                                failure_kind: Some(DeliveryFailureKind::GiveUp),
                            })
                            .await;
                        self.escalate_failure(
                            client,
                            DeliveryFailureKind::GiveUp,
                            job.adapter,
                            &seat,
                            &reason,
                            reply.detail.as_deref(),
                            shas,
                            sender_seat.as_ref(),
                            &job.hub_owner_seat,
                        )
                        .await;
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn escalate_failure(
        &self,
        client: &LocalClient,
        kind: DeliveryFailureKind,
        courier: &str,
        target_seat: &SeatName,
        reason: &str,
        detail: Option<&str>,
        sha256s: &[Sha256Hex],
        sender_seat: Option<&SeatName>,
        hub_owner_seat: &SeatName,
    ) {
        let mut recipients: Vec<SeatName> = Vec::new();
        // CRITICAL OPERATOR ATTENTION RULE: never deliver failure notices or
        // interrupting inbox cards to the human operator seat.
        if !self.cfg.is_operator_target(hub_owner_seat) {
            recipients.push(hub_owner_seat.clone());
        }
        if matches!(
            kind,
            DeliveryFailureKind::Refused | DeliveryFailureKind::GiveUp
        ) && let Some(sender) = sender_seat
            && !self.cfg.is_operator_target(sender)
            && !recipients.contains(sender)
        {
            recipients.push(sender.clone());
        }
        if recipients.is_empty() {
            return;
        }

        let refs: Vec<String> = sha256s
            .iter()
            .map(|s| s.as_str()[..16].to_owned())
            .collect();
        let sha_key = if refs.is_empty() {
            "none".to_owned()
        } else {
            refs.join(",")
        };
        let detail_line = detail
            .map(str::trim)
            .filter(|d| !d.is_empty())
            .map(|d| format!("\nDetail: {d}"))
            .unwrap_or_default();

        let (subject, title, body) = match kind {
            DeliveryFailureKind::Refused => (
                format!("Delivery refused for @{target_seat} ({reason})"),
                format!("Mail delivery refused for @{target_seat} ({reason})"),
                format!(
                    "Courier `{courier}` refused delivery to `@{target_seat}` on machine `{}`.\n\
                     Reason: `{reason}`{detail_line}\n\
                     The dead session binding was immediately expired without retry.\n\
                     Letters: {sha_key}",
                    self.cfg.local_machine_id
                ),
            ),
            DeliveryFailureKind::GiveUp => (
                format!("Delivery gave up for @{target_seat} ({reason})"),
                format!("Mail delivery gave up for @{target_seat} ({reason})"),
                format!(
                    "Courier `{courier}` exhausted retries for `@{target_seat}` on machine `{}`.\n\
                     Reason: `{reason}`{detail_line}\n\
                     Letters: {sha_key}",
                    self.cfg.local_machine_id
                ),
            ),
            DeliveryFailureKind::AdapterDegraded => (
                format!(
                    "AdapterDegraded: courier `{courier}` on {}",
                    self.cfg.local_machine_id
                ),
                format!("AdapterDegraded: `{courier}` ({reason})"),
                format!(
                    "Courier `{courier}` reached consecutive unknown threshold ({}) while ringing `@{target_seat}` on machine `{}`.\n\
                     Last reason: `{reason}`{detail_line}\n\
                     Previous unknown rings were counted as rung and not retried. Inspect `couriers.d/{courier}`.",
                    self.cfg.degraded_threshold, self.cfg.local_machine_id
                ),
            ),
        };

        for recip in recipients {
            let idem_sig = sha256_short16(&format!(
                "courier_fail/1\n{}\n{}\n{}\n{}\n{}\n{}",
                kind.as_str(),
                courier,
                target_seat,
                recip,
                reason,
                sha_key
            ));
            let idem = IdemKey::parse(format!("cfail:{idem_sig}")).ok();
            let _ = client
                .call(MailSend {
                    to: vec![recip.clone()],
                    cc: Vec::new(),
                    subject: subject.clone(),
                    body: body.clone(),
                    from: None,
                    priority: LetterPriority::High,
                    topic: Some("delivery-failure".into()),
                    in_reply_to: None,
                    refs: refs.clone(),
                    wait_export: false,
                    idem,
                })
                .await;

            let dedupe = format!(
                "courier_fail:{}:{}:{}:{}:{}",
                kind.as_str(),
                courier,
                target_seat,
                recip,
                idem_sig
            );
            let _ = client
                .call(InboxRaise {
                    to: Principal::Seat { seat: recip },
                    draft: NotificationDraft {
                        kind: NotifyKind::MailDeliveryFailed {
                            failure_kind: kind,
                            courier: courier.to_owned(),
                            seat: target_seat.clone(),
                            reason: reason.to_owned(),
                        },
                        title: title.clone(),
                        preview: body.clone(),
                        actions: Vec::new(),
                        dedupe,
                    },
                })
                .await;
        }
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
    }
}
