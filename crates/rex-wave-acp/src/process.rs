//! Safe process-group signaling and bounded reaping built over `rustix::process`.

use rustix::io::Errno;
use rustix::process::{Pid, Signal, kill_process_group, test_kill_process_group};
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(25);
const POST_KILL_WAIT: Duration = Duration::from_millis(300);

fn raw_to_pid(pgid: u32) -> Option<Pid> {
    let raw = i32::try_from(pgid).ok()?;
    if raw <= 0 {
        return None;
    }
    Pid::from_raw(raw)
}

pub fn signal_process_group(pgid: u32, sig: Signal) {
    if let Some(pid) = raw_to_pid(pgid) {
        let _ = kill_process_group(pid, sig);
    }
}

pub fn process_group_alive(pgid: u32) -> bool {
    let Some(pid) = raw_to_pid(pgid) else {
        return false;
    };
    !matches!(test_kill_process_group(pid), Err(Errno::SRCH))
}

pub fn kill_process_group_now(pgid: u32) {
    signal_process_group(pgid, Signal::KILL);
}

/// Sends `SIGTERM` to `pgid`, waits up to `grace` for both the direct child and
/// any grandchildren in `pgid` to exit, then escalates to `SIGKILL` on the
/// entire process group and reaps the direct child.
pub async fn terminate_and_reap_process_group(
    child: &mut tokio::process::Child,
    pgid: Option<u32>,
    grace: Duration,
) {
    let deadline = Instant::now() + grace;
    if let Some(g) = pgid {
        signal_process_group(g, Signal::TERM);
    }

    let child_reaped = tokio::time::timeout(grace, child.wait()).await.is_ok();

    if !child_reaped {
        if let Some(g) = pgid {
            signal_process_group(g, Signal::KILL);
        } else {
            let _ = child.kill().await;
        }
        let _ = child.wait().await;
    }

    if let Some(g) = pgid {
        while process_group_alive(g) {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let step = (deadline - now).min(POLL_INTERVAL);
            tokio::time::sleep(step).await;
        }
        if process_group_alive(g) {
            signal_process_group(g, Signal::KILL);
            let kill_deadline = Instant::now() + POST_KILL_WAIT;
            while process_group_alive(g) && Instant::now() < kill_deadline {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
    }
}
