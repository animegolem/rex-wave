//! Shared hermetic fixtures for `rex-wave` integration tests: short Unix socket
//! directories and subprocess environments isolated from the developer's live
//! house (`~/.t3`, `~/.bb`, `~/.config/rex-wave`, ambient tokens).
#![allow(dead_code)]
#![allow(clippy::disallowed_methods)]

use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::{Builder, TempDir};

/// macOS `sockaddr_un.sun_path` holds 104 bytes; keep every test directory
/// short enough that `<dir>/<name>.sock` always fits, whatever `TMPDIR` says.
pub const MAX_TEST_DIR_LEN: usize = 64;

/// Ambient variables that would point a spawned `rex-wave` at live state or
/// impersonate a seat/worker.
pub const SCRUBBED_ENV: &[&str] = &[
    "T3CODE_THREAD_ID",
    "REX_WAVE_WORKER_THREAD_ID",
    "REX_WAVE_TOKEN",
    "REX_WAVE_T3_HOME",
    "REX_WAVE_T3_URL",
    "REX_WAVE_T3_TOKEN",
    "REX_WAVE_BB_TOKEN",
    "REX_WAVE_INTERCHANGE_DIR",
    "REX_WAVE_LIVE_INTERCHANGE_TEST_DIR",
    "CLAUDE_PID",
];

/// A private `O_EXCL` (`0700`) temporary directory short enough for Unix
/// domain sockets on macOS, removed on drop via [`TempDir`].
pub struct DirGuard(pub PathBuf, TempDir);

pub fn fresh_dir(label: &str) -> DirGuard {
    let short: String = label
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .take(10)
        .collect();
    let ambient = std::env::temp_dir();
    let base = if ambient.as_os_str().len() < 36 && ambient.is_dir() {
        ambient
    } else {
        PathBuf::from("/tmp")
    };
    let prefix = format!("rw-{short}-");
    let mut builder = Builder::new();
    builder.prefix(&prefix);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(std::fs::Permissions::from_mode(0o700));
    }
    let guard = builder
        .tempdir_in(&base)
        .expect("create private short test temp dir");
    let p = guard.path().to_path_buf();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::symlink_metadata(&p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "test dir {} must be 0700", p.display());
    }
    assert!(
        p.as_os_str().len() < MAX_TEST_DIR_LEN,
        "test dir {} must stay under {MAX_TEST_DIR_LEN} bytes",
        p.display()
    );
    DirGuard(p, guard)
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
