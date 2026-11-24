use crate::name::{RelPath, SafeName};
use std::fs::{self, File, Metadata, OpenOptions, Permissions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Flags for every handle this crate opens by name: never follow a symlink
/// at the leaf, and never block on opening a FIFO a hostile peer planted.
const NOFOLLOW: i32 = libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC;

/// How many temp names one atomic write tries before giving up.
const MAX_TEMP_ATTEMPTS: u32 = 64;

static TEMP_NONCE: AtomicU64 = AtomicU64::new(1);

/// True when `err` is the refusal an `O_NOFOLLOW` open returns for a
/// symlink leaf (`ELOOP`; `EMLINK` on some BSDs).
fn is_symlink_refusal(err: &io::Error) -> bool {
    matches!(err.raw_os_error(), Some(code) if code == libc::ELOOP || code == libc::EMLINK)
}

fn opened(path: &Path, err: io::Error) -> io::Error {
    if is_symlink_refusal(&err) {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("refusing to follow symlink {}", path.display()),
        )
    } else {
        err
    }
}

fn not_regular(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{} is not a regular file", path.display()),
    )
}

/// Open `path` read-only without following a symlink at the leaf, then
/// judge the opened handle (`fstat`), so nothing can swap the file between
/// the check and the read.
pub fn open_regular_nofollow(path: &Path) -> io::Result<(File, Metadata)> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(NOFOLLOW)
        .open(path)
        .map_err(|e| opened(path, e))?;
    let meta = file.metadata()?;
    if !meta.is_file() {
        return Err(not_regular(path));
    }
    Ok((file, meta))
}

/// Read a regular file of at most `max_bytes` through an `O_NOFOLLOW`
/// handle. A larger file is `InvalidData`, never a silent truncation.
pub fn read_regular_bounded(path: &Path, max_bytes: u64) -> io::Result<Vec<u8>> {
    let (file, meta) = open_regular_nofollow(path)?;
    read_open_bounded(path, file, &meta, max_bytes)
}

/// [`read_regular_bounded`] for a secret: also refuses (`PermissionDenied`)
/// a file whose handle shows any group or other permission bit, or another
/// owner than `expected_uid`.
pub fn read_private_regular(
    path: &Path,
    expected_uid: Option<u32>,
    max_bytes: u64,
) -> io::Result<Vec<u8>> {
    let (file, meta) = open_regular_nofollow(path)?;
    if let Some(uid) = expected_uid
        && meta.uid() != uid
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{} is owned by another uid", path.display()),
        ));
    }
    if meta.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{} must be mode 0600", path.display()),
        ));
    }
    read_open_bounded(path, file, &meta, max_bytes)
}

/// `(dev, ino)` identity of an opened file handle.
pub type FileId = (u64, u64);

pub fn file_id(meta: &Metadata) -> FileId {
    (meta.dev(), meta.ino())
}

/// Read at most `max_bytes` from an already-opened handle.
pub fn read_open_bounded(
    path: &Path,
    mut file: File,
    meta: &Metadata,
    max_bytes: u64,
) -> io::Result<Vec<u8>> {
    let too_big = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} is larger than {max_bytes} bytes", path.display()),
        )
    };
    if meta.is_file() && meta.len() > max_bytes {
        return Err(too_big());
    }
    let cap = if meta.is_file() {
        usize::try_from(meta.len().min(max_bytes)).unwrap_or(0)
    } else {
        0
    };
    let mut buf = Vec::with_capacity(cap);
    if max_bytes == u64::MAX {
        file.read_to_end(&mut buf)?;
    } else {
        file.take(max_bytes + 1).read_to_end(&mut buf)?;
        if buf.len() as u64 > max_bytes {
            return Err(too_big());
        }
    }
    Ok(buf)
}

/// [`read_regular_bounded`], decoded as UTF-8 (`InvalidData` otherwise).
pub fn read_regular_string(path: &Path, max_bytes: u64) -> io::Result<String> {
    let bytes = read_regular_bounded(path, max_bytes)?;
    String::from_utf8(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} is not UTF-8", path.display()),
        )
    })
}

/// Bounded read for an operator-chosen CLI input path: follows symlinks and
/// accepts pipes, process substitution, and `/dev/stdin`, while refusing
/// directories and inputs larger than `max_bytes`.
pub fn read_input_bounded(path: &Path, max_bytes: u64) -> io::Result<Vec<u8>> {
    let file = File::open(path)?;
    let meta = file.metadata()?;
    if meta.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is a directory", path.display()),
        ));
    }
    read_open_bounded(path, file, &meta, max_bytes)
}

/// [`read_input_bounded`], decoded as UTF-8 (`InvalidData` otherwise).
pub fn read_input_string(path: &Path, max_bytes: u64) -> io::Result<String> {
    let bytes = read_input_bounded(path, max_bytes)?;
    String::from_utf8(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} is not UTF-8", path.display()),
        )
    })
}

/// Open (creating with `mode` when absent) `path` for appending without
/// following a symlink at the leaf, and refuse a handle that is not a
/// regular file.
pub fn open_append_nofollow(path: &Path, mode: u32) -> io::Result<(File, Metadata)> {
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .mode(mode)
        .custom_flags(NOFOLLOW)
        .open(path)
        .map_err(|e| opened(path, e))?;
    let meta = file.metadata()?;
    if !meta.is_file() {
        return Err(not_regular(path));
    }
    Ok((file, meta))
}

/// Open (creating with `mode` when absent) `path` for both initial bounded
/// reading and subsequent appending without following a symlink at the leaf.
pub fn open_rw_append_nofollow(path: &Path, mode: u32) -> io::Result<(File, Metadata)> {
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .mode(mode)
        .custom_flags(NOFOLLOW)
        .open(path)
        .map_err(|e| opened(path, e))?;
    let meta = file.metadata()?;
    if !meta.is_file() {
        return Err(not_regular(path));
    }
    Ok((file, meta))
}

/// Verify that `path` still names the exact regular file `expected` without
/// following a symlink at `path`.
pub fn verify_same_regular_nofollow(path: &Path, expected: FileId) -> io::Result<()> {
    let (_file, meta) = open_regular_nofollow(path)?;
    if file_id(&meta) != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} was replaced underneath watcher", path.display()),
        ));
    }
    Ok(())
}

/// Open (creating if absent) a private `0600` advisory lock file without
/// following a symlink at `path`.
pub fn open_lock_nofollow(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(NOFOLLOW)
        .open(path)
        .map_err(|e| opened(path, e))?;
    let meta = file.metadata()?;
    if !meta.is_file() {
        return Err(not_regular(path));
    }
    file.set_permissions(Permissions::from_mode(0o600))?;
    Ok(file)
}

/// Create a fresh, exclusive temp file beside `path` (never following a
/// symlink), fill it with `bytes` at `mode`, and sync it. Returns its path.
fn write_temp_sibling(path: &Path, bytes: &[u8], mode: u32) -> io::Result<PathBuf> {
    let parent = parent_of(path)?;
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} has no file name", path.display()),
        )
    })?;
    for _ in 0..MAX_TEMP_ATTEMPTS {
        let nonce = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        let mut tmp_name = std::ffi::OsString::from(".");
        tmp_name.push(name);
        tmp_name.push(format!(".tmp.{}.{nonce}", std::process::id()));
        let tmp = parent.join(tmp_name);
        let mut file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .custom_flags(NOFOLLOW)
            .open(&tmp)
        {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(opened(&tmp, e)),
        };
        // A private mode is set exactly; a shared one keeps the umask's say.
        let private = mode & 0o077 == 0;
        let filled = (if private {
            file.set_permissions(Permissions::from_mode(mode))
        } else {
            Ok(())
        })
        .and_then(|()| file.write_all(bytes))
        .and_then(|()| file.sync_all());
        if let Err(e) = filled {
            drop(file);
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        return Ok(tmp);
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "no free temp name beside {} after {MAX_TEMP_ATTEMPTS} tries",
            path.display()
        ),
    ))
}

fn parent_of(path: &Path) -> io::Result<&Path> {
    match path.parent() {
        Some(p) if p.as_os_str().is_empty() => Ok(Path::new(".")),
        Some(p) => Ok(p),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} has no parent directory", path.display()),
        )),
    }
}

/// `fsync` a directory so a rename or link inside it survives a crash.
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn refuse_symlink_leaf(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("refusing to replace symlink {}", path.display()),
        )),
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Replace `path` with `bytes` atomically: exclusive temp sibling, `mode`,
/// `fsync`, `rename`, and (when `sync_dir`) `fsync` of the parent. Refuses a
/// symlink at `path`. `rename` never writes through a link, so a link
/// planted after that check is replaced, not followed.
pub fn write_atomic_path(path: &Path, bytes: &[u8], mode: u32, sync_dir: bool) -> io::Result<()> {
    refuse_symlink_leaf(path)?;
    let tmp = write_temp_sibling(path, bytes, mode)?;
    if let Err(e) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    if sync_dir {
        self::sync_dir(parent_of(path)?)?;
    }
    Ok(())
}

/// Create `path` holding `bytes` only if nothing is there: temp sibling,
/// then `link`, which fails on any existing entry (a symlink included)
/// instead of replacing it. Returns false when `path` already existed.
pub fn write_atomic_if_absent_path(
    path: &Path,
    bytes: &[u8],
    mode: u32,
    sync_dir: bool,
) -> io::Result<bool> {
    let tmp = write_temp_sibling(path, bytes, mode)?;
    let linked = fs::hard_link(&tmp, path);
    let _ = fs::remove_file(&tmp);
    match linked {
        Ok(()) => {
            if sync_dir {
                self::sync_dir(parent_of(path)?)?;
            }
            Ok(true)
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e),
    }
}

/// What sits at `path` itself, a symlink never followed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryKind {
    Missing,
    File,
    Dir,
    Symlink,
    Other,
}

/// Classify the entry at `path` without following a symlink there. Use it
/// for presence checks (lock files are dangling symlinks); never to decide
/// whether a later open is safe — open the handle and judge that instead.
pub fn entry_kind(path: &Path) -> io::Result<EntryKind> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            let ft = meta.file_type();
            Ok(if ft.is_symlink() {
                EntryKind::Symlink
            } else if ft.is_file() {
                EntryKind::File
            } else if ft.is_dir() {
                EntryKind::Dir
            } else {
                EntryKind::Other
            })
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(EntryKind::Missing),
        Err(e) => Err(e),
    }
}

/// True when anything — a dangling symlink included — exists at `path`.
pub fn entry_exists(path: &Path) -> bool {
    !matches!(entry_kind(path), Ok(EntryKind::Missing) | Err(_))
}

/// Open the directory at `path` without following a symlink there and
/// return its metadata from the handle: for permission and owner checks.
pub fn dir_metadata_nofollow(path: &Path) -> io::Result<Metadata> {
    let not_dir = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is not a real directory", path.display()),
        )
    };
    // A symlink or non-directory fails as ELOOP, EMLINK or ENOTDIR
    // depending on the platform; all mean the same thing here.
    let dir = OpenOptions::new()
        .read(true)
        .custom_flags(NOFOLLOW | libc::O_DIRECTORY)
        .open(path)
        .map_err(|e| {
            if is_symlink_refusal(&e) || e.raw_os_error() == Some(libc::ENOTDIR) {
                not_dir()
            } else {
                e
            }
        })?;
    let meta = dir.metadata()?;
    if !meta.is_dir() {
        return Err(not_dir());
    }
    Ok(meta)
}

/// Create `dir` (and missing parents) with mode `0700` when absent, then open
/// `dir` with `O_DIRECTORY | O_NOFOLLOW` and `fchmod` the opened handle to
/// `0700` so a symlink at `dir` is refused rather than followed.
pub fn ensure_private_dir(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt as _;
    if entry_kind(dir)? == EntryKind::Missing {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)?;
    }
    let not_dir = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is not a real directory", dir.display()),
        )
    };
    let handle = OpenOptions::new()
        .read(true)
        .custom_flags(NOFOLLOW | libc::O_DIRECTORY)
        .open(dir)
        .map_err(|e| {
            if is_symlink_refusal(&e) || e.raw_os_error() == Some(libc::ENOTDIR) {
                not_dir()
            } else {
                e
            }
        })?;
    if !handle.metadata()?.is_dir() {
        return Err(not_dir());
    }
    handle.set_permissions(Permissions::from_mode(0o700))
}

/// The owner uid of the Unix socket at `path`, refusing anything else
/// (sockets cannot be opened, so this is the one `lstat`-based judgment).
pub fn socket_owner_uid(path: &Path) -> io::Result<u32> {
    use std::os::unix::fs::FileTypeExt as _;
    let meta = fs::symlink_metadata(path)?;
    if !meta.file_type().is_socket() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is not a socket", path.display()),
        ));
    }
    Ok(meta.uid())
}

/// Derive the current process's effective Unix uid directly via `geteuid()`,
/// independent of any replaceable filesystem entry or `TMPDIR`.
pub fn current_process_uid() -> io::Result<u32> {
    Ok(rustix::process::geteuid().as_raw())
}

/// Create `path` (and any missing ancestors) with mode `0700` when `path` does
/// not exist yet, without altering permissions of any pre-existing directory.
pub fn create_missing_private_dir(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt as _;
    if entry_kind(path)? == EntryKind::Missing {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)?;
    }
    Ok(())
}

/// Descriptor-pinned parent directory and exclusive startup lock for safe
/// Unix-domain socket setup.
pub struct PreparedUnixSocket {
    parent_dir: File,
    _lock_file: File,
    file_name: std::ffi::OsString,
    uid: u32,
}

impl PreparedUnixSocket {
    pub fn uid(&self) -> u32 {
        self.uid
    }

    /// Verify via `statat(AT_SYMLINK_NOFOLLOW)` on the held parent directory
    /// that the bound entry is a socket owned by `self.uid` and tighten its
    /// mode to `0600` without following symlinks.
    pub fn finalize_permissions(&self, display_path: &Path) -> io::Result<()> {
        use rustix::fs::{AtFlags, FileType, Mode, chmodat, statat};
        let st = statat(&self.parent_dir, &self.file_name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(io::Error::from)?;
        if FileType::from_raw_mode(st.st_mode) != FileType::Socket {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} is not a socket", display_path.display()),
            ));
        }
        if st.st_uid != self.uid {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("{} is owned by another uid", display_path.display()),
            ));
        }
        match chmodat(
            &self.parent_dir,
            &self.file_name,
            Mode::from_raw_mode(0o600),
            AtFlags::SYMLINK_NOFOLLOW,
        ) {
            Ok(()) => Ok(()),
            Err(e)
                if e == rustix::io::Errno::OPNOTSUPP
                    || e == rustix::io::Errno::NOSYS
                    || e == rustix::io::Errno::INVAL =>
            {
                // Parent directory is already verified owner-only and non-group/world-writable.
                chmodat(
                    &self.parent_dir,
                    &self.file_name,
                    Mode::from_raw_mode(0o600),
                    AtFlags::empty(),
                )
                .map_err(io::Error::from)
            }
            Err(e) => Err(io::Error::from(e)),
        }
    }
    pub fn unlink_stale(&self, display_path: &Path) -> io::Result<()> {
        use rustix::fs::{AtFlags, FileType, statat, unlinkat};

        match statat(&self.parent_dir, &self.file_name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(st) => {
                if FileType::from_raw_mode(st.st_mode) != FileType::Socket {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "refusing to unlink non-socket entry at {}",
                            display_path.display()
                        ),
                    ));
                }
                if st.st_uid != self.uid {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "refusing to unlink socket {} owned by another uid",
                            display_path.display()
                        ),
                    ));
                }
                if probe_socket_is_stale(&self.parent_dir, &self.file_name, self.uid, display_path)?
                {
                    unlinkat(&self.parent_dir, &self.file_name, AtFlags::empty())
                        .map_err(io::Error::from)?;
                }
            }
            Err(rustix::io::Errno::NOENT) => {}
            Err(e) => return Err(io::Error::from(e)),
        }
        Ok(())
    }
}

enum HeldProbeOutcome {
    Listening,
    Stale,
    Gone,
}

fn open_verified_sticky_tmp_root(uid: u32) -> io::Result<(File, PathBuf)> {
    use rustix::fs::{Mode, OFlags, openat};
    use std::path::Component;

    let tmp_root = fs::canonicalize("/tmp")?;
    let mut cur = OpenOptions::new()
        .read(true)
        .custom_flags(NOFOLLOW | libc::O_DIRECTORY)
        .open("/")?;
    let root_meta = cur.metadata()?;
    if !root_meta.is_dir()
        || (root_meta.uid() != 0 && root_meta.uid() != uid)
        || (root_meta.permissions().mode() & 0o022 != 0)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "root directory / failed ownership or mode verification",
        ));
    }

    let mut built = PathBuf::from("/");
    for comp in tmp_root.components() {
        match comp {
            Component::RootDir => {}
            Component::Normal(seg) => {
                let next_fd = openat(
                    &cur,
                    seg,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(io::Error::from)?;
                let next = File::from(next_fd);
                let meta = next.metadata()?;
                let mode = meta.permissions().mode();
                let owner_ok = meta.uid() == 0 || meta.uid() == uid;
                let mode_ok = (mode & 0o022 == 0) || (mode & 0o1000 != 0);
                if !meta.is_dir() || !owner_ok || !mode_ok {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "temporary ancestor {} failed ownership or sticky-mode verification",
                            built.join(seg).display()
                        ),
                    ));
                }
                built.push(seg);
                cur = next;
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "unexpected component in canonical /tmp: {}",
                        tmp_root.display()
                    ),
                ));
            }
        }
    }
    Ok((cur, built))
}

#[allow(clippy::unnecessary_cast)]
fn stat_file_id(st: &rustix::fs::Stat) -> (u64, u64) {
    (st.st_dev as u64, st.st_ino as u64)
}

fn connect_held_socket(
    parent_dir: &File,
    file_name: &std::ffi::OsStr,
    uid: u32,
    expected_id: (u64, u64),
    display_path: &Path,
) -> io::Result<HeldProbeOutcome> {
    use rustix::fs::{AtFlags, Mode, OFlags, linkat, mkdirat, openat, statat};

    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd as _;
        let fd_dir = PathBuf::from(format!("/proc/self/fd/{}", parent_dir.as_raw_fd()));
        if let (Ok(parent_meta), Ok(fd_meta)) = (parent_dir.metadata(), fs::metadata(&fd_dir))
            && (parent_meta.dev(), parent_meta.ino()) == (fd_meta.dev(), fd_meta.ino())
        {
            let target = fd_dir.join(file_name);
            return match std::os::unix::net::UnixStream::connect(&target) {
                Ok(_) => Ok(HeldProbeOutcome::Listening),
                Err(e)
                    if e.kind() == io::ErrorKind::ConnectionRefused
                        || e.raw_os_error() == Some(libc::ECONNREFUSED) =>
                {
                    Ok(HeldProbeOutcome::Stale)
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(HeldProbeOutcome::Gone),
                Err(e) => Err(io::Error::new(
                    e.kind(),
                    format!(
                        "refusing to unlink socket {} after inconclusive liveness probe: {e}",
                        display_path.display()
                    ),
                )),
            };
        }
    }

    let (tmp_dir, tmp_path) = open_verified_sticky_tmp_root(uid).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "refusing to unlink socket {}: cannot anchor probe staging root ({e})",
                display_path.display()
            ),
        )
    })?;

    let pid = std::process::id();
    let mut created_stage: Option<String> = None;
    for _ in 0..MAX_TEMP_ATTEMPTS {
        let n = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        let candidate = format!(".rw-sp-{pid}-{n:x}");
        match mkdirat(&tmp_dir, &candidate, Mode::from_raw_mode(0o700)) {
            Ok(()) => {
                created_stage = Some(candidate);
                break;
            }
            Err(rustix::io::Errno::EXIST) => continue,
            Err(e) => return Err(io::Error::from(e)),
        }
    }
    let stage_name = created_stage.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "exhausted temporary socket-probe staging names",
        )
    })?;

    struct StageGuard<'a> {
        tmp_dir: &'a File,
        stage_name: String,
        stage_dir: Option<File>,
    }
    impl Drop for StageGuard<'_> {
        fn drop(&mut self) {
            if let Some(ref sd) = self.stage_dir {
                let _ = rustix::fs::unlinkat(sd, "s", rustix::fs::AtFlags::empty());
            }
            let _ = rustix::fs::unlinkat(
                self.tmp_dir,
                &self.stage_name,
                rustix::fs::AtFlags::REMOVEDIR,
            );
        }
    }

    let mut guard = StageGuard {
        tmp_dir: &tmp_dir,
        stage_name: stage_name.clone(),
        stage_dir: None,
    };

    let stage_fd = openat(
        &tmp_dir,
        &stage_name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    let stage_dir = File::from(stage_fd);
    let stage_meta = stage_dir.metadata()?;
    if !stage_meta.is_dir()
        || stage_meta.uid() != uid
        || (stage_meta.permissions().mode() & 0o777 != 0o700)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "socket-probe staging directory failed ownership or 0700 mode check",
        ));
    }
    guard.stage_dir = Some(stage_dir);
    let stage_dir_ref = guard.stage_dir.as_ref().expect("stage_dir initialized");

    match linkat(parent_dir, file_name, stage_dir_ref, "s", AtFlags::empty()) {
        Ok(()) => {}
        Err(rustix::io::Errno::NOENT) => return Ok(HeldProbeOutcome::Gone),
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "refusing to unlink socket {}: cannot anchor liveness probe to held directory ({e})",
                    display_path.display()
                ),
            ));
        }
    }

    let linked_st =
        statat(stage_dir_ref, "s", AtFlags::SYMLINK_NOFOLLOW).map_err(io::Error::from)?;
    if stat_file_id(&linked_st) != expected_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "refusing to unlink socket {}: socket identity changed while staging probe",
                display_path.display()
            ),
        ));
    }

    let probe_path = tmp_path.join(&stage_name).join("s");
    match std::os::unix::net::UnixStream::connect(&probe_path) {
        Ok(_) => Ok(HeldProbeOutcome::Listening),
        Err(e)
            if e.kind() == io::ErrorKind::ConnectionRefused
                || e.raw_os_error() == Some(libc::ECONNREFUSED) =>
        {
            Ok(HeldProbeOutcome::Stale)
        }
        Err(e) => Err(io::Error::new(
            e.kind(),
            format!(
                "refusing to unlink socket {} after inconclusive liveness probe: {e}",
                display_path.display()
            ),
        )),
    }
}

/// Returns `Ok(true)` when a dead stale socket still sits at `(parent_dir, file_name)`,
/// `Ok(false)` when the entry disappeared concurrently (`NOENT` in `parent_dir`),
/// or `Err` when a live daemon is listening on `(parent_dir, file_name)` or the
/// probe cannot be anchored to `parent_dir`.
fn probe_socket_is_stale(
    parent_dir: &File,
    file_name: &std::ffi::OsStr,
    uid: u32,
    display_path: &Path,
) -> io::Result<bool> {
    use rustix::fs::{AtFlags, FileType, statat};

    let st_before = match statat(parent_dir, file_name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(st) => st,
        Err(rustix::io::Errno::NOENT) => return Ok(false),
        Err(e) => return Err(io::Error::from(e)),
    };
    if FileType::from_raw_mode(st_before.st_mode) != FileType::Socket {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "refusing to unlink non-socket entry at {}",
                display_path.display()
            ),
        ));
    }
    if st_before.st_uid != uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "refusing to unlink socket {} owned by another uid",
                display_path.display()
            ),
        ));
    }
    let expected_id = stat_file_id(&st_before);

    match connect_held_socket(parent_dir, file_name, uid, expected_id, display_path)? {
        HeldProbeOutcome::Listening => Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            format!(
                "another daemon is already listening on {}",
                display_path.display()
            ),
        )),
        HeldProbeOutcome::Gone => Ok(false),
        HeldProbeOutcome::Stale => match statat(parent_dir, file_name, AtFlags::SYMLINK_NOFOLLOW) {
            Err(rustix::io::Errno::NOENT) => Ok(false),
            Err(e) => Err(io::Error::from(e)),
            Ok(st_after) => {
                if FileType::from_raw_mode(st_after.st_mode) != FileType::Socket
                    || stat_file_id(&st_after) != expected_id
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "refusing to unlink socket {}: socket identity changed during probe",
                            display_path.display()
                        ),
                    ));
                }
                Ok(true)
            }
        },
    }
}

/// Validate `path` and its parent directory for a Unix-domain socket without
/// unlinking or binding yet:
/// 1. Derives the process UID via `geteuid()`.
/// 2. Creates missing parent directories with mode `0700`, opens the parent
///    directory with `O_DIRECTORY | O_NOFOLLOW`, and requires it to be owned
///    by this UID and not writable by group or others (`mode & 0o022 == 0`).
/// 3. Acquires a non-blocking exclusive flock on `<socket>.lock` inside the
///    verified parent directory, held inside [`PreparedUnixSocket`] through
///    probe, unlink, and `bind` so concurrent starts cannot race.
/// 4. Refuses non-socket entries, foreign-UID sockets, live listening
///    endpoints (`AddrInUse`), and inconclusive liveness probes.
pub fn validate_unix_socket_target(path: &Path) -> io::Result<PreparedUnixSocket> {
    use rustix::fs::{AtFlags, FileType, Mode, OFlags, openat, statat};
    use std::os::unix::fs::DirBuilderExt as _;

    let uid = current_process_uid()?;
    let file_name = path
        .file_name()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} has no socket file name", path.display()),
            )
        })?
        .to_os_string();
    let parent = parent_of(path)?;
    if entry_kind(parent)? == EntryKind::Missing {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)?;
    }
    let parent_dir = OpenOptions::new()
        .read(true)
        .custom_flags(NOFOLLOW | libc::O_DIRECTORY)
        .open(parent)
        .map_err(|e| {
            if is_symlink_refusal(&e) || e.raw_os_error() == Some(libc::ENOTDIR) {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "socket parent {} must be a real directory",
                        parent.display()
                    ),
                )
            } else {
                e
            }
        })?;
    let parent_meta = parent_dir.metadata()?;
    if !parent_meta.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("socket parent {} is not a directory", parent.display()),
        ));
    }
    if parent_meta.uid() != uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "socket parent {} is owned by uid {}, expected {}",
                parent.display(),
                parent_meta.uid(),
                uid
            ),
        ));
    }
    if parent_meta.permissions().mode() & 0o022 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "socket parent {} must not be group- or world-writable (mode {:o})",
                parent.display(),
                parent_meta.permissions().mode() & 0o7777
            ),
        ));
    }

    let mut lock_name = file_name.clone();
    lock_name.push(".lock");
    let lock_fd = openat(
        &parent_dir,
        &lock_name,
        OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o600),
    )
    .map_err(|e| {
        if e == rustix::io::Errno::LOOP || e == rustix::io::Errno::MLINK {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("refusing symlinked socket lock in {}", parent.display()),
            )
        } else {
            io::Error::from(e)
        }
    })?;
    let lock_file = File::from(lock_fd);
    if !lock_file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("socket lock in {} is not a regular file", parent.display()),
        ));
    }
    match lock_file.try_lock() {
        Ok(()) => {}
        Err(fs::TryLockError::WouldBlock) => {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!(
                    "another daemon is starting or listening on {}",
                    path.display()
                ),
            ));
        }
        Err(fs::TryLockError::Error(e)) => return Err(e),
    }

    match statat(&parent_dir, &file_name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(st) => {
            if FileType::from_raw_mode(st.st_mode) != FileType::Socket {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("refusing to unlink non-socket entry at {}", path.display()),
                ));
            }
            if st.st_uid != uid {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "refusing to unlink socket {} owned by another uid",
                        path.display()
                    ),
                ));
            }
            probe_socket_is_stale(&parent_dir, &file_name, uid, path)?;
        }
        Err(rustix::io::Errno::NOENT) => {}
        Err(e) => return Err(io::Error::from(e)),
    }

    Ok(PreparedUnixSocket {
        parent_dir,
        _lock_file: lock_file,
        file_name,
        uid,
    })
}

/// Validate `path` and unlink any dead stale socket owned by this UID before
/// `UnixListener::bind`.
pub fn prepare_unix_socket(path: &Path) -> io::Result<PreparedUnixSocket> {
    let prepared = validate_unix_socket_target(path)?;
    prepared.unlink_stale(path)?;
    Ok(prepared)
}

/// The real, symlink-free absolute location of an existing `path`.
pub(crate) fn real_path(path: &Path) -> io::Result<PathBuf> {
    fs::canonicalize(path)
}

/// A directory verified at open to be a real directory (not a symlink),
/// held by its symlink-free absolute path. Every lookup beneath it takes
/// validated [`SafeName`]/[`RelPath`] segments and refuses a symlink at
/// every step.
///
/// What is and is not race-free: each leaf is opened `O_NOFOLLOW` and judged
/// by its handle, so a swapped leaf is refused. Intermediate directories are
/// checked with `lstat` before use; safe std has no `openat`, so a peer that
/// can write inside the root could swap one between the check and the
/// open. The root is assumed writable only by its owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfinedDir {
    root: PathBuf,
}

impl ConfinedDir {
    /// Hold an existing directory. `root` may be reached through symlinks;
    /// the held path is its real location.
    pub fn open(root: &Path) -> io::Result<Self> {
        let real = real_path(root)?;
        dir_metadata_nofollow(&real)?;
        Ok(Self { root: real })
    }

    /// Create `root` (and missing parents) with `mode`, then hold it.
    pub fn create(root: &Path, mode: u32) -> io::Result<Self> {
        use std::os::unix::fs::DirBuilderExt as _;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(mode)
            .create(root)?;
        Self::open(root)
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    /// Walk `segments` beneath the root, refusing a symlink or non-directory
    /// at each step, creating missing ones with `create` when given.
    fn walk(&self, segments: &[SafeName], create: Option<u32>) -> io::Result<PathBuf> {
        use std::os::unix::fs::DirBuilderExt as _;
        let mut cur = self.root.clone();
        for seg in segments {
            cur.push(seg.as_str());
            match entry_kind(&cur)? {
                EntryKind::Dir => {}
                EntryKind::Missing if create.is_some() => {
                    let mode = create.expect("checked by guard");
                    match fs::DirBuilder::new().mode(mode).create(&cur) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                            dir_metadata_nofollow(&cur)?;
                        }
                        Err(e) => return Err(e),
                    }
                }
                EntryKind::Missing => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("{} does not exist", cur.display()),
                    ));
                }
                EntryKind::Symlink => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("refusing to follow symlink {}", cur.display()),
                    ));
                }
                EntryKind::File | EntryKind::Other => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotADirectory,
                        format!("{} is not a directory", cur.display()),
                    ));
                }
            }
        }
        Ok(cur)
    }

    /// The path `rel` names beneath the root, with every parent checked.
    pub fn resolve(&self, rel: &RelPath) -> io::Result<PathBuf> {
        Ok(self
            .walk(rel.parent_segments(), None)?
            .join(rel.file_name().as_str()))
    }

    pub fn open_regular(&self, rel: &RelPath) -> io::Result<(File, Metadata)> {
        open_regular_nofollow(&self.resolve(rel)?)
    }

    pub fn read_regular(&self, rel: &RelPath, max_bytes: u64) -> io::Result<Vec<u8>> {
        read_regular_bounded(&self.resolve(rel)?, max_bytes)
    }

    pub fn read_string(&self, rel: &RelPath, max_bytes: u64) -> io::Result<String> {
        read_regular_string(&self.resolve(rel)?, max_bytes)
    }

    pub fn write_atomic(
        &self,
        rel: &RelPath,
        bytes: &[u8],
        mode: u32,
        sync_dir: bool,
    ) -> io::Result<()> {
        write_atomic_path(&self.resolve(rel)?, bytes, mode, sync_dir)
    }

    pub fn write_atomic_if_absent(
        &self,
        rel: &RelPath,
        bytes: &[u8],
        mode: u32,
        sync_dir: bool,
    ) -> io::Result<bool> {
        write_atomic_if_absent_path(&self.resolve(rel)?, bytes, mode, sync_dir)
    }

    /// Ensure every directory in `rel` exists beneath the root (creating
    /// missing ones with `mode`), refusing symlinks; returns its path.
    pub fn ensure_subdir(&self, rel: &RelPath, mode: u32) -> io::Result<PathBuf> {
        self.walk(rel.segments(), Some(mode))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn tmp() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn bounded_read_refuses_symlink_oversize_and_non_regular() {
        let dir = tmp();
        let file = dir.path().join("f");
        fs::write(&file, b"hello").unwrap();
        assert_eq!(read_regular_bounded(&file, 5).unwrap(), b"hello");
        let big = read_regular_bounded(&file, 4).unwrap_err();
        assert_eq!(big.kind(), io::ErrorKind::InvalidData);

        let link = dir.path().join("l");
        symlink(&file, &link).unwrap();
        let err = read_regular_bounded(&link, 64).unwrap_err();
        assert!(err.to_string().contains("symlink"), "{err}");

        let err = read_regular_bounded(dir.path(), 64).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        fs::write(&file, [0xff, 0xfe]).unwrap();
        assert_eq!(
            read_regular_string(&file, 64).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn fifo_is_refused_without_blocking() {
        let dir = tmp();
        let fifo = dir.path().join("p");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(status.success());
        assert_eq!(
            read_regular_bounded(&fifo, 64).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn append_refuses_symlink_and_creates_with_mode() {
        let dir = tmp();
        let log = dir.path().join("log");
        let (mut f, meta) = open_append_nofollow(&log, 0o600).unwrap();
        f.write_all(b"a").unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);

        let victim = dir.path().join("victim");
        fs::write(&victim, b"keep").unwrap();
        let link = dir.path().join("link");
        symlink(&victim, &link).unwrap();
        assert!(open_append_nofollow(&link, 0o600).is_err());
        assert_eq!(fs::read(&victim).unwrap(), b"keep");
    }

    #[test]
    fn atomic_writes_replace_or_refuse() {
        let dir = tmp();
        let target = dir.path().join("t.json");
        write_atomic_path(&target, b"one", 0o600, true).unwrap();
        write_atomic_path(&target, b"two", 0o600, true).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"two");
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o600
        );

        assert!(!write_atomic_if_absent_path(&target, b"three", 0o644, true).unwrap());
        assert_eq!(fs::read(&target).unwrap(), b"two");
        let fresh = dir.path().join("fresh");
        assert!(write_atomic_if_absent_path(&fresh, b"x", 0o644, true).unwrap());

        let victim = dir.path().join("victim");
        fs::write(&victim, b"keep").unwrap();
        let link = dir.path().join("link");
        symlink(&victim, &link).unwrap();
        assert!(write_atomic_path(&link, b"evil", 0o600, false).is_err());
        assert!(!write_atomic_if_absent_path(&link, b"evil", 0o600, false).unwrap());
        assert_eq!(fs::read(&victim).unwrap(), b"keep");

        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty());
    }

    #[test]
    fn confined_dir_refuses_symlinked_segments() {
        let dir = tmp();
        let outside = tmp();
        let root = ConfinedDir::create(&dir.path().join("root"), 0o700).unwrap();
        let rel = RelPath::parse("state/bells/a.json").unwrap();
        root.ensure_subdir(&RelPath::parse("state/bells").unwrap(), 0o700)
            .unwrap();
        root.write_atomic(&rel, b"{}", 0o600, true).unwrap();
        assert_eq!(root.read_string(&rel, 16).unwrap(), "{}");
        assert!(
            root.write_atomic_if_absent(&rel, b"x", 0o600, false)
                .is_ok()
        );

        symlink(outside.path(), root.path().join("escape")).unwrap();
        let through = RelPath::parse("escape/x").unwrap();
        assert!(root.write_atomic(&through, b"x", 0o600, false).is_err());
        assert!(root.ensure_subdir(&through, 0o700).is_err());
        assert!(fs::read_dir(outside.path()).unwrap().next().is_none());

        let missing = RelPath::parse("nope/x").unwrap();
        assert_eq!(
            root.read_regular(&missing, 1).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }

    #[test]
    fn entry_kind_sees_dangling_symlinks() {
        let dir = tmp();
        let lock = dir.path().join(".#lock");
        symlink("user@host.1:2", &lock).unwrap();
        assert_eq!(entry_kind(&lock).unwrap(), EntryKind::Symlink);
        assert!(entry_exists(&lock));
        assert!(!entry_exists(&dir.path().join("none")));
        assert!(dir_metadata_nofollow(dir.path()).is_ok());
        let dlink = dir.path().join("dl");
        symlink(dir.path(), &dlink).unwrap();
        let plain = dir.path().join("plain");
        fs::write(&plain, b"").unwrap();
        for not_dir in [&dlink, &plain] {
            assert_eq!(
                dir_metadata_nofollow(not_dir).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                ensure_private_dir(not_dir).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
        let victim_dir = dir.path().join("victim-dir");
        fs::create_dir(&victim_dir).unwrap();
        fs::set_permissions(&victim_dir, Permissions::from_mode(0o755)).unwrap();
        let victim_link = dir.path().join("victim-link");
        symlink(&victim_dir, &victim_link).unwrap();
        assert!(ensure_private_dir(&victim_link).is_err());
        assert_eq!(
            fs::metadata(&victim_dir).unwrap().permissions().mode() & 0o777,
            0o755,
            "ensure_private_dir must never chmod through a symlink"
        );
    }

    #[test]
    fn diverged_display_path_refuses_to_unlink_held_socket() {
        use std::os::unix::net::UnixListener;

        let dir = tmp();
        let held_dir = dir.path().join("held");
        let decoy_dir = dir.path().join("decoy");
        ensure_private_dir(&held_dir).unwrap();
        ensure_private_dir(&decoy_dir).unwrap();

        let held_sock = held_dir.join("daemon.sock");
        let decoy_sock = decoy_dir.join("daemon.sock");

        let prepared = validate_unix_socket_target(&held_sock).unwrap();
        let live_listener = UnixListener::bind(&held_sock).unwrap();

        {
            let _dead_listener = UnixListener::bind(&decoy_sock).unwrap();
        }
        assert_eq!(entry_kind(&decoy_sock).unwrap(), EntryKind::Other);

        let err = prepared.unlink_stale(&decoy_sock).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse);
        assert_eq!(
            entry_kind(&held_sock).unwrap(),
            EntryKind::Other,
            "diverged dead socket path must never unlink live socket in held directory"
        );
        drop(live_listener);

        let outer = dir.path().join("outer");
        let outer_sub = outer.join("sub");
        ensure_private_dir(&outer).unwrap();
        ensure_private_dir(&outer_sub).unwrap();
        let orig_sock_path = outer_sub.join("daemon.sock");
        let prepared_moved = validate_unix_socket_target(&orig_sock_path).unwrap();
        let moved_dir = dir.path().join("outer_moved");
        let moved_sock_path = moved_dir.join("sub").join("daemon.sock");
        let live_in_moved = UnixListener::bind(&orig_sock_path).unwrap();

        fs::rename(&outer, &moved_dir).unwrap();
        ensure_private_dir(&outer).unwrap();
        ensure_private_dir(&outer_sub).unwrap();
        {
            let _dead_at_orig_path = UnixListener::bind(&orig_sock_path).unwrap();
        }
        assert_eq!(entry_kind(&orig_sock_path).unwrap(), EntryKind::Other);

        let err_moved = prepared_moved.unlink_stale(&orig_sock_path).unwrap_err();
        assert_eq!(
            err_moved.kind(),
            io::ErrorKind::AddrInUse,
            "probe anchored to held directory must detect live listener even after ancestor rename"
        );
        assert_eq!(entry_kind(&moved_sock_path).unwrap(), EntryKind::Other);

        drop(live_in_moved);
        prepared_moved.unlink_stale(&orig_sock_path).unwrap();
        assert_eq!(entry_kind(&moved_sock_path).unwrap(), EntryKind::Missing);
        assert_eq!(
            entry_kind(&orig_sock_path).unwrap(),
            EntryKind::Other,
            "unlink_stale must only remove stale socket in held directory"
        );
    }
}
