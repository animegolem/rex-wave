//! Shared hermetic fixtures for `rex-wave` integration tests: short Unix socket
//! directories and subprocess environments isolated from the developer's live
//! house (`~/.t3`, `~/.bb`, `~/.config/rex-wave`, ambient tokens).
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static NONCE: AtomicU64 = AtomicU64::new(1);

/// macOS `sockaddr_un.sun_path` holds 104 bytes; keep every test directory
/// short enough that `<dir>/<name>.sock` always fits, whatever `TMPDIR` says.
pub const MAX_TEST_DIR_LEN: usize = 64;

/// Ambient variables that would point a spawned `rex-wave` at live state or
/// impersonate a seat/worker.
pub const SCRUBBED_ENV: &[&str] = &[
    "T3CODE_THREAD_ID",
    "REX_WAVE_TOKEN",
    "REX_WAVE_T3_HOME",
    "REX_WAVE_T3_URL",
    "REX_WAVE_T3_TOKEN",
    "REX_WAVE_BB_TOKEN",
    "REX_WAVE_INTERCHANGE_DIR",
    "CLAUDE_PID",
];

/// A fresh directory under `/tmp/rw-...` (never `$TMPDIR`), removed on drop.
pub struct DirGuard(pub PathBuf);

impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn fresh_dir(label: &str) -> DirGuard {
    let n = NONCE.fetch_add(1, Ordering::Relaxed);
    let short: String = label
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .take(12)
        .collect();
    let p = PathBuf::from(format!("/tmp/rw-{short}-{}-{n}", std::process::id()));
    assert!(
        p.as_os_str().len() < MAX_TEST_DIR_LEN,
        "test dir {} must stay under {MAX_TEST_DIR_LEN} bytes",
        p.display()
    );
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    DirGuard(p)
}

/// `<root>/config`, holding an empty `rex-wave/protected-roots` (the daemon
/// refuses to start without one).
pub fn config_home(root: &Path) -> PathBuf {
    let cfg = root.join("config");
    std::fs::create_dir_all(cfg.join("rex-wave")).unwrap();
    let roots = cfg.join("rex-wave/protected-roots");
    if !roots.exists() {
        std::fs::write(&roots, "").unwrap();
    }
    cfg
}

/// `<root>/t3-home`, an empty T3 Code home for `--t3-home`.
pub fn t3_home(root: &Path) -> PathBuf {
    let home = root.join("t3-home");
    std::fs::create_dir_all(&home).unwrap();
    home
}

/// Applies the hermetic environment rooted at `root` to `cmd`.
pub fn hermetic<'a>(cmd: &'a mut Command, root: &Path) -> &'a mut Command {
    for key in SCRUBBED_ENV {
        cmd.env_remove(key);
    }
    let home = root.join("home");
    let state = root.join("xdg-state");
    let bb = root.join("bb-data");
    for d in [&home, &state, &bb] {
        std::fs::create_dir_all(d).unwrap();
    }
    cmd.env("HOME", &home)
        .env("XDG_CONFIG_HOME", config_home(root))
        .env("XDG_STATE_HOME", &state)
        .env("BB_DATA_DIR", &bb)
}

/// A `rex-wave` subprocess whose environment is isolated under `root`.
pub fn rex_wave(root: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rex-wave"));
    hermetic(&mut cmd, root);
    cmd
}

/// A hermetic `rex-wave` subprocess for a daemon socket that sits directly in
/// its test root (`<root>/<name>.sock`).
pub fn rex_wave_at(sock: impl AsRef<Path>) -> Command {
    let root = sock
        .as_ref()
        .parent()
        .expect("test socket lives inside its test root");
    rex_wave(root)
}

/// Writes `<root>/config/rex-wave/house.json`, the optional house profile a
/// hermetic `rex-wave` subprocess rooted at `root` loads.
pub fn write_house_profile(root: &Path, profile: &serde_json::Value) {
    let path = config_home(root).join("rex-wave/house.json");
    std::fs::write(&path, serde_json::to_vec_pretty(profile).unwrap()).unwrap();
}
