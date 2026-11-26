//! Safe process-group spawn and bounded SIGTERM -> SIGKILL reaping via `rustix`.

use rex_wave_waves::HostError;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStderr, Command};

pub const DEFAULT_RETIRE_GRACE: Duration = Duration::from_secs(2);
const POLL_STEP: Duration = Duration::from_millis(20);
const POST_KILL_WAIT: Duration = Duration::from_millis(500);

/// Tracks a spawned child process and its process-group ID, sending `SIGKILL`
/// to the process group on drop if the worker was never explicitly reaped.
pub struct LiveChild {
    pub child: Child,
    pub pgid: Option<i32>,
    pub generation: u64,
    reaped: bool,
}

impl LiveChild {
    pub fn new(child: Child, pgid: Option<i32>, generation: u64) -> Self {
        Self {
            child,
            pgid,
            generation,
            reaped: false,
        }
    }

    pub fn mark_reaped(&mut self) {
        self.reaped = true;
    }
}

impl Drop for LiveChild {
    fn drop(&mut self) {
        if !self.reaped {
            kill_process_group_now(self.pgid);
            let _ = self.child.start_kill();
        }
    }
}

/// Spawns `cmd` in its own process group (`pgid == pid`) so `retire` can reap
/// the worker together with any grandchildren it spawned.
pub fn spawn_in_process_group(cmd: &mut Command) -> Result<(Child, Option<i32>), HostError> {
    #[cfg(unix)]
    cmd.process_group(0);
    cmd.kill_on_drop(true);
    let child = cmd
        .spawn()
        .map_err(|e| HostError::Unreachable(format!("spawn native worker process: {e}")))?;
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

/// Terminates the process group (`SIGTERM` -> `grace` -> `SIGKILL`) and reaps `live.child`.
pub async fn terminate_process_group(live: &mut LiveChild, grace: Duration) {
    let deadline = Instant::now() + grace;

    #[cfg(unix)]
    if let Some(pgid) = live.pgid {
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
        kill_process_group_now(live.pgid);
        let _ = live.child.start_kill();
        let _ = tokio::time::timeout(Duration::from_secs(2), live.child.wait()).await;
        live.mark_reaped();
    }

    #[cfg(unix)]
    if let Some(pgid) = live.pgid {
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
