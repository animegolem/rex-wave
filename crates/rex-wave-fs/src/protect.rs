use crate::file::{EntryKind, entry_kind, read_regular_string, real_path};
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

/// The most entries, folders and names together, one deployment may protect.
pub const MAX_PROTECTED_ROOTS: usize = 64;

/// The largest protected-roots file read, in bytes.
pub const MAX_PROTECTED_ROOTS_FILE_BYTES: u64 = 64 * 1024;

/// The most symlinks followed while resolving one protected folder.
const MAX_LINK_HOPS: usize = 40;

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

/// What a sandbox may never touch on this machine, as a deployment lists it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Protection {
    /// Absolute folders. A sandbox may not be one, contain one, or sit inside one.
    pub roots: Vec<PathBuf>,
    /// Folder names refused anywhere in a sandbox path.
    pub names: Vec<String>,
}

/// A [`Protection`] resolved once against the filesystem and the home
/// folder, so every later check sees the same answer.
#[derive(Clone, Debug)]
pub struct ProtectedRootsGuard {
    roots: Vec<PathBuf>,
    names: Vec<String>,
    homes: Vec<PathBuf>,
}

/// Resolve every symlink on the way to `path`, component by component, the
/// way the filesystem would, including links whose targets do not exist yet.
/// Components that do not exist are kept as written. A folder whose real
/// location cannot be established is an error, never a guess.
pub fn resolve_links(path: &Path) -> Result<PathBuf, SandboxError> {
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
        match entry_kind(&next) {
            Ok(EntryKind::Symlink) => {
                hops += 1;
                if hops > MAX_LINK_HOPS {
                    return Err(unresolved(path, &"too many symlinks"));
                }
                let target = std::fs::read_link(&next).map_err(|e| unresolved(&next, &e))?;
                if target.is_absolute() {
                    out = PathBuf::new();
                }
                for comp in target.components().rev() {
                    pending.push(comp.as_os_str().to_owned());
                }
            }
            Ok(_) => out = next,
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
    crate::SafeName::parse(name)
        .map(|n| n.as_str().to_owned())
        .map_err(|_| SandboxError::Config(format!("`{name}` is not a single folder name")))
}

/// Absolute form of `path` with `.` dropped and `..` applied by spelling.
pub fn lexical_normalize(path: &Path) -> Result<PathBuf, SandboxError> {
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
            if let Ok(canon) = real_path(&hp) {
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
    let text = read_regular_string(file, MAX_PROTECTED_ROOTS_FILE_BYTES).map_err(|e| {
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

impl ProtectedRootsGuard {
    pub fn resolve(protection: &Protection) -> Result<Self, SandboxError> {
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
        Ok(Self {
            roots: roots.into_iter().collect(),
            names: names.into_iter().collect(),
            homes: home_dirs().into_iter().collect(),
        })
    }

    /// Refuse `candidate` (reported as `reported`) when it is the filesystem
    /// root or a home folder, carries a protected name, or is, contains or
    /// sits under a protected folder.
    pub fn check(&self, candidate: &Path, reported: &Path) -> Result<(), SandboxError> {
        let refused = || Err(SandboxError::Production(reported.to_path_buf()));
        if candidate.parent().is_none() || self.homes.iter().any(|home| home.starts_with(candidate))
        {
            return refused();
        }
        if candidate
            .components()
            .any(|c| self.names.iter().any(|name| c.as_os_str() == name.as_str()))
        {
            return refused();
        }
        for prod in &self.roots {
            if candidate.starts_with(prod) || prod.starts_with(candidate) {
                return refused();
            }
        }
        Ok(())
    }

    /// Judge an existing directory by its spelling, the target of a symlink
    /// at it, its deepest existing ancestor and its real location, and
    /// return that real location.
    pub fn verify_dir(&self, path: &Path) -> Result<PathBuf, SandboxError> {
        let lex = lexical_normalize(path)?;
        self.check(&lex, path)?;

        // Judge a symlink by where it points, so the verdict is the same
        // whether or not its target exists on this machine.
        if let Ok(target) = std::fs::read_link(path) {
            let base = lex.parent().unwrap_or(Path::new("/"));
            let pointed = lexical_normalize(&base.join(target))?;
            self.check(&pointed, path)?;
        }

        let mut ancestor = lex.as_path();
        while let Some(parent) = ancestor.parent() {
            if let Ok(canon_anc) = real_path(ancestor) {
                let resolved = match lex.strip_prefix(ancestor) {
                    Ok(suffix) if !suffix.as_os_str().is_empty() => canon_anc.join(suffix),
                    _ => canon_anc,
                };
                self.check(&resolved, path)?;
                break;
            }
            ancestor = parent;
        }

        let canon = real_path(path).map_err(|e| SandboxError::Io(e.to_string()))?;
        self.check(&canon, path)?;
        let meta = std::fs::metadata(&canon).map_err(|e| SandboxError::Io(e.to_string()))?;
        if !meta.is_dir() {
            return Err(SandboxError::Io(format!(
                "{} is not a directory",
                canon.display()
            )));
        }
        Ok(canon)
    }

    /// Place `raw` (absolute, or relative to `root`) strictly beneath the
    /// real directory `root`, refusing `..`, any symlink on the way, and
    /// protected locations. Returns `(path, slash-joined relative path,
    /// exists)`.
    pub fn confine(
        &self,
        root: &Path,
        raw: &Path,
    ) -> Result<(PathBuf, String, bool), SandboxError> {
        if raw.as_os_str().is_empty() || raw.components().any(|c| matches!(c, Component::ParentDir))
        {
            return Err(SandboxError::Io("invalid path outside sandbox".into()));
        }
        let joined = if raw.is_absolute() {
            raw.to_path_buf()
        } else {
            root.join(raw)
        };
        let lex = lexical_normalize(&joined)?;
        self.check(&lex, raw)?;
        if !lex.starts_with(root) || lex == root {
            return Err(SandboxError::Production(raw.to_path_buf()));
        }

        let mut walk = root.to_path_buf();
        if let Ok(rel_parts) = lex.strip_prefix(root) {
            for comp in rel_parts.components() {
                walk.push(comp.as_os_str());
                if matches!(entry_kind(&walk), Ok(EntryKind::Symlink)) {
                    return Err(SandboxError::Production(raw.to_path_buf()));
                }
            }
        }

        match real_path(&lex) {
            Ok(canon) => {
                self.check(&canon, raw)?;
                if !canon.starts_with(root) || canon == root {
                    return Err(SandboxError::Production(raw.to_path_buf()));
                }
                let rel = relative_slash(root, &canon)?;
                Ok((canon, rel, true))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Some(parent) = lex.parent()
                    && let Ok(canon_parent) = real_path(parent)
                {
                    if !canon_parent.starts_with(root) {
                        return Err(SandboxError::Production(raw.to_path_buf()));
                    }
                    if let Some(fname) = lex.file_name() {
                        self.check(&canon_parent.join(fname), raw)?;
                    }
                }
                let rel = relative_slash(root, &lex)?;
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

/// An output file location verified to lie outside one protected folder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutsidePath {
    path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum OutsideError {
    #[error("{0} contains '..' parent traversal")]
    ParentDir(PathBuf),
    #[error("{0} is a symlink")]
    Symlink(PathBuf),
    #[error("{0} is inside {1}")]
    Inside(PathBuf, PathBuf),
    #[error("{0} has no file name")]
    NoFileName(PathBuf),
    #[error("{0}")]
    Io(String),
}

/// Canonicalize the deepest existing prefix of `path` (resolving both
/// symlinks and filesystem case on case-insensitive volumes such as macOS
/// APFS), then re-attach any not-yet-existing trailing components.
pub fn canonicalize_existing_prefix(path: &Path) -> Result<PathBuf, SandboxError> {
    let lex = lexical_normalize(path)?;
    let resolved = resolve_links(&lex)?;
    for base in [&resolved, &lex] {
        let mut anc = base.as_path();
        loop {
            if let Ok(canon_anc) = real_path(anc) {
                return Ok(match base.strip_prefix(anc) {
                    Ok(suffix) if !suffix.as_os_str().is_empty() => canon_anc.join(suffix),
                    _ => canon_anc,
                });
            }
            let Some(parent) = anc.parent() else {
                break;
            };
            anc = parent;
        }
    }
    Ok(resolved)
}

fn flip_ascii_case_first(s: &str) -> Option<String> {
    let mut out = String::with_capacity(s.len());
    let mut flipped = false;
    for ch in s.chars() {
        if !flipped && ch.is_ascii_alphabetic() {
            out.push(if ch.is_ascii_lowercase() {
                ch.to_ascii_uppercase()
            } else {
                ch.to_ascii_lowercase()
            });
            flipped = true;
        } else {
            out.push(ch);
        }
    }
    flipped.then_some(out)
}

/// Probe whether `existing_dir` resides on a case-insensitive filesystem by
/// toggling the ASCII case of one existing path component and checking whether
/// it resolves to the same `(dev, ino)` inode.
fn is_case_insensitive_volume(existing_dir: &Path) -> bool {
    use std::os::unix::fs::MetadataExt as _;
    let Ok(orig) = std::fs::metadata(existing_dir) else {
        return false;
    };
    let mut cur = existing_dir;
    while let Some(parent) = cur.parent() {
        if let Some(name) = cur.file_name().and_then(|n| n.to_str())
            && let Some(toggled_name) = flip_ascii_case_first(name)
        {
            let suffix = existing_dir.strip_prefix(cur).unwrap_or(Path::new(""));
            let mut probe = parent.join(toggled_name);
            if !suffix.as_os_str().is_empty() {
                probe = probe.join(suffix);
            }
            return std::fs::metadata(&probe)
                .is_ok_and(|m| (m.dev(), m.ino()) == (orig.dev(), orig.ino()));
        }
        cur = parent;
    }
    false
}

fn starts_with_ascii_ci(path: &Path, root: &Path) -> bool {
    let mut p_comps = path.components();
    for r in root.components() {
        let Some(p) = p_comps.next() else {
            return false;
        };
        let eq = match (p.as_os_str().to_str(), r.as_os_str().to_str()) {
            (Some(ps), Some(rs)) => ps.eq_ignore_ascii_case(rs),
            _ => p.as_os_str() == r.as_os_str(),
        };
        if !eq {
            return false;
        }
    }
    true
}

impl OutsidePath {
    /// Verify that `out` (absolute, or relative to the current directory)
    /// names a file outside `protected_root`, judged by spelling, symlink
    /// resolution, and `canonicalize` of the deepest existing ancestor (which
    /// normalizes case on case-insensitive volumes such as macOS APFS). A
    /// symlink at `out` itself is refused.
    pub fn verify(out: &Path, protected_root: &Path) -> Result<Self, OutsideError> {
        if out.components().any(|c| matches!(c, Component::ParentDir)) {
            return Err(OutsideError::ParentDir(out.to_path_buf()));
        }
        let abs = lexical_normalize(out).map_err(|e| OutsideError::Io(e.to_string()))?;
        let file_name = abs
            .file_name()
            .ok_or_else(|| OutsideError::NoFileName(out.to_path_buf()))?
            .to_os_string();
        if matches!(entry_kind(&abs), Ok(EntryKind::Symlink)) {
            return Err(OutsideError::Symlink(out.to_path_buf()));
        }
        let parent = abs.parent().unwrap_or(Path::new("/"));
        let real_parent = resolve_links(parent).map_err(|e| OutsideError::Io(e.to_string()))?;
        let real = real_parent.join(&file_name);
        if matches!(entry_kind(&real), Ok(EntryKind::Symlink)) {
            return Err(OutsideError::Symlink(out.to_path_buf()));
        }
        let canon =
            canonicalize_existing_prefix(&abs).map_err(|e| OutsideError::Io(e.to_string()))?;
        if matches!(entry_kind(&canon), Ok(EntryKind::Symlink)) {
            return Err(OutsideError::Symlink(out.to_path_buf()));
        }

        let protected = [
            lexical_normalize(protected_root).map_err(|e| OutsideError::Io(e.to_string()))?,
            resolve_links(protected_root).map_err(|e| OutsideError::Io(e.to_string()))?,
            canonicalize_existing_prefix(protected_root)
                .map_err(|e| OutsideError::Io(e.to_string()))?,
        ];
        let mut deepest_existing = canon.as_path();
        while !deepest_existing.exists() {
            let Some(p) = deepest_existing.parent() else {
                break;
            };
            deepest_existing = p;
        }
        let ci = is_case_insensitive_volume(deepest_existing);

        for cand in [&abs, &real, &canon] {
            for root in &protected {
                if cand.starts_with(root) || (ci && starts_with_ascii_ci(cand, root)) {
                    return Err(OutsideError::Inside(out.to_path_buf(), root.clone()));
                }
            }
        }
        Ok(Self { path: canon })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn outside_path_refuses_inside_symlink_and_dotdot() {
        let dir = tempfile::tempdir().unwrap();
        let base = real_path(dir.path()).unwrap();
        let protected = base.join("interchange");
        std::fs::create_dir_all(protected.join("mail")).unwrap();
        let free = base.join("out");
        std::fs::create_dir_all(&free).unwrap();

        let ok = OutsidePath::verify(&free.join("shadow.jsonl"), &protected).unwrap();
        assert_eq!(ok.path(), free.join("shadow.jsonl"));
        let not_yet = OutsidePath::verify(&free.join("new/dir/x.jsonl"), &protected).unwrap();
        assert_eq!(not_yet.path(), free.join("new/dir/x.jsonl"));

        assert!(matches!(
            OutsidePath::verify(&protected.join("mail/x"), &protected),
            Err(OutsideError::Inside(..))
        ));
        assert!(matches!(
            OutsidePath::verify(&free.join("../interchange/x"), &protected),
            Err(OutsideError::ParentDir(_))
        ));

        let hop = base.join("hop");
        symlink(&protected, &hop).unwrap();
        assert!(matches!(
            OutsidePath::verify(&hop.join("later/x"), &protected),
            Err(OutsideError::Inside(..))
        ));
        let leaf = free.join("leaf");
        symlink(protected.join("acks.jsonl"), &leaf).unwrap();
        assert!(matches!(
            OutsidePath::verify(&leaf, &protected),
            Err(OutsideError::Symlink(_))
        ));

        if is_case_insensitive_volume(&base) {
            let house = base.join("House");
            let inter = house.join("interchange");
            std::fs::create_dir_all(&inter).unwrap();
            let upper_direct = base.join("HOUSE").join("INTERCHANGE").join("shadow.jsonl");
            assert!(matches!(
                OutsidePath::verify(&upper_direct, &inter),
                Err(OutsideError::Inside(..))
            ));
            let upper_sub = base
                .join("HOUSE")
                .join("INTERCHANGE")
                .join("not-yet")
                .join("shadow.jsonl");
            assert!(matches!(
                OutsidePath::verify(&upper_sub, &inter),
                Err(OutsideError::Inside(..))
            ));
        }
    }

    #[test]
    fn guard_refuses_home_root_names_and_nested_roots() {
        let dir = tempfile::tempdir().unwrap();
        let base = real_path(dir.path()).unwrap();
        let prot = base.join("prod");
        let guard = ProtectedRootsGuard::resolve(&Protection {
            roots: vec![prot.clone()],
            names: vec!["secret".into()],
        })
        .unwrap();
        assert!(guard.check(Path::new("/"), Path::new("/")).is_err());
        assert!(guard.check(&prot.join("x"), &prot).is_err());
        assert!(guard.check(&base, &base).is_err());
        assert!(guard.check(&base.join("a/secret/b"), &base).is_err());
        assert!(guard.check(&base.join("sandbox"), &base).is_ok());
    }
}
