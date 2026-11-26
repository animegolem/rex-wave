//! Safe process-group spawn and cancellation-safe single-use SIGTERM -> SIGKILL reaping via `rustix`.

use rex_wave_waves::HostError;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStderr, Command};

pub const DEFAULT_RETIRE_GRACE: Duration = Duration::from_secs(2);
const POLL_STEP: Duration = Duration::from_millis(20);
const POST_KILL_WAIT: Duration = Duration::from_millis(500);

/// Single-use process-group ownership slot shared across the worker state,
/// active `LiveChild`, and any cancellation/drop guard so at most one cleanup
/// path can claim and signal the numeric PGID.
pub type PgidSlot = Arc<Mutex<Option<i32>>>;

pub fn new_pgid_slot(pgid: Option<i32>) -> PgidSlot {
    Arc::new(Mutex::new(pgid))
}

pub fn claim_pgid_slot(slot: &PgidSlot) -> Option<i32> {
    slot.lock().unwrap_or_else(|e| e.into_inner()).take()
}

pub fn claim_and_kill_pgid(slot: &PgidSlot) {
    if let Some(pgid) = claim_pgid_slot(slot) {
        kill_process_group_now(Some(pgid));
    }
}

/// Ensures a claimed PGID still receives `SIGKILL` if `terminate_process_group`
/// is cancelled mid-await during its grace window.
struct ForceKillOnDrop(Option<i32>);

impl ForceKillOnDrop {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for ForceKillOnDrop {
    fn drop(&mut self) {
        if let Some(pgid) = self.0.take() {
            kill_process_group_now(Some(pgid));
        }
    }
}

/// Tracks a spawned child process and its shared single-use [`PgidSlot`],
/// sending `SIGKILL` to the process group on drop if no caller claimed it yet.
pub struct LiveChild {
    pub child: Child,
    pub pgid_slot: PgidSlot,
    pub generation: u64,
    reaped: bool,
}

impl LiveChild {
    pub fn new(child: Child, pgid: Option<i32>, generation: u64) -> Self {
        Self::with_slot(child, new_pgid_slot(pgid), generation)
    }

    pub fn with_slot(child: Child, pgid_slot: PgidSlot, generation: u64) -> Self {
        Self {
            child,
            pgid_slot,
            generation,
            reaped: false,
        }
    }

    pub fn claim_pgid(&self) -> Option<i32> {
        claim_pgid_slot(&self.pgid_slot)
    }

    pub fn mark_reaped(&mut self) {
        self.reaped = true;
    }
}

impl Drop for LiveChild {
    fn drop(&mut self) {
        if let Some(pgid) = self.claim_pgid() {
            kill_process_group_now(Some(pgid));
        }
        if !self.reaped {
            let _ = self.child.start_kill();
        }
    }
}

fn classify_spawn_io_error(err: io::Error) -> HostError {
    let permanent_os = matches!(
        err.raw_os_error(),
        Some(libc_code)
            if libc_code == rustix::io::Errno::NOENT.raw_os_error()
                || libc_code == rustix::io::Errno::ACCESS.raw_os_error()
                || libc_code == rustix::io::Errno::NOTDIR.raw_os_error()
                || libc_code == rustix::io::Errno::NOEXEC.raw_os_error()
    );
    if permanent_os
        || matches!(
            err.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied | io::ErrorKind::InvalidInput
        )
    {
        HostError::Rejected(format!("spawn native worker process: {err}"))
    } else {
        HostError::Unreachable(format!("spawn native worker process: {err}"))
    }
}

/// Spawns `cmd` in its own process group (`pgid == pid`) so `retire` can reap
/// the worker together with any grandchildren it spawned.
pub fn spawn_in_process_group(cmd: &mut Command) -> Result<(Child, Option<i32>), HostError> {
    #[cfg(unix)]
    cmd.process_group(0);
    cmd.kill_on_drop(true);
    let child = cmd.spawn().map_err(classify_spawn_io_error)?;
    let pgid = child.id().and_then(|pid| i32::try_from(pid).ok());
    Ok((child, pgid))
}

#[cfg(unix)]
pub fn signal_process_group(pgid: i32, sig: rustix::process::Signal) -> bool {
    let Some(pid) = rustix::process::Pid::from_raw(pgid) else {
        return false;
    };
    match rustix::process::kill_process_group(pid, sig) {
        Ok(()) => true,
        Err(rustix::io::Errno::SRCH) => false,
        Err(_) => true,
    }
}

#[cfg(unix)]
pub fn process_group_alive(pgid: i32) -> bool {
    let Some(pid) = rustix::process::Pid::from_raw(pgid) else {
        return false;
    };
    !matches!(
        rustix::process::test_kill_process_group(pid),
        Err(rustix::io::Errno::SRCH)
    )
}

#[cfg(not(unix))]
pub fn process_group_alive(_pgid: i32) -> bool {
    false
}

pub fn kill_process_group_now(pgid: Option<i32>) {
    #[cfg(unix)]
    if let Some(id) = pgid {
        let _ = signal_process_group(id, rustix::process::Signal::KILL);
    }
    #[cfg(not(unix))]
    let _ = pgid;
}

/// Claims `live`'s single-use PGID, terminates the process group
/// (`SIGTERM` -> `grace` -> `SIGKILL`), and reaps `live.child`.
/// If cancelled mid-grace, `ForceKillOnDrop` immediately sends `SIGKILL`.
pub async fn terminate_process_group(live: &mut LiveChild, grace: Duration) {
    let deadline = Instant::now() + grace;
    let claimed_pgid = live.claim_pgid();
    let mut kill_guard = ForceKillOnDrop(claimed_pgid);

    #[cfg(unix)]
    if let Some(pgid) = claimed_pgid {
        let _ = signal_process_group(pgid, rustix::process::Signal::TERM);
    }

    let leader_exited = match tokio::time::timeout(grace, live.child.wait()).await {
        Ok(_) => {
            live.mark_reaped();
            true
        }
        Err(_) => false,
    };

    if !leader_exited {
        kill_process_group_now(claimed_pgid);
        let _ = live.child.start_kill();
        let _ = tokio::time::timeout(Duration::from_secs(2), live.child.wait()).await;
        live.mark_reaped();
    }

    #[cfg(unix)]
    if let Some(pgid) = claimed_pgid {
        while process_group_alive(pgid) && Instant::now() < deadline {
            tokio::time::sleep(POLL_STEP).await;
        }
        if process_group_alive(pgid) {
            let _ = signal_process_group(pgid, rustix::process::Signal::KILL);
            let kill_deadline = Instant::now() + POST_KILL_WAIT;
            while process_group_alive(pgid) && Instant::now() < kill_deadline {
                tokio::time::sleep(POLL_STEP).await;
            }
        }
    }

    kill_guard.disarm();
}

/// Drains `stderr` in a background task and returns up to the last `max_bytes` UTF-8 tail.
pub fn spawn_stderr_drainer(
    mut stderr: ChildStderr,
    max_bytes: usize,
) -> tokio::task::JoinHandle<String> {
    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        let mut tail = Vec::new();
        loop {
            match stderr.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    tail.extend_from_slice(&buf[..n]);
                    if tail.len() > max_bytes {
                        let excess = tail.len() - max_bytes;
                        tail.drain(..excess);
                    }
                }
            }
        }
        String::from_utf8_lossy(&tail).trim().to_owned()
    })
}
