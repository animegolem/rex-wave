use std::fmt;
use std::path::{Component, Path, PathBuf};

/// The longest single path segment a [`SafeName`] admits, in bytes.
pub const MAX_NAME_BYTES: usize = 128;

/// The most segments a [`RelPath`] admits.
pub const MAX_REL_SEGMENTS: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    #[error("name is empty")]
    Empty,
    #[error("name is longer than {MAX_NAME_BYTES} bytes")]
    TooLong,
    #[error("name `{0}` is `.` or `..`")]
    Dot(String),
    #[error("name `{0}` contains a separator, NUL or control character")]
    BadChar(String),
    #[error("name `{0}` starts with `.` or contains `..`")]
    Hidden(String),
    #[error("path has more than {MAX_REL_SEGMENTS} segments")]
    TooDeep,
    #[error("path `{0}` must not contain `..`")]
    ParentDir(String),
    #[error("path `{0}` must not be empty or `.`")]
    EmptyPath(String),
}

/// One validated path segment: 1..=[`MAX_NAME_BYTES`] bytes, never `.` or
/// `..`, and free of `/`, `\`, NUL and control characters. Joining a
/// `SafeName` onto a directory always names a direct child of it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SafeName(String);

impl SafeName {
    pub fn parse(raw: &str) -> Result<Self, NameError> {
        if raw.is_empty() {
            return Err(NameError::Empty);
        }
        if raw.len() > MAX_NAME_BYTES {
            return Err(NameError::TooLong);
        }
        if raw == "." || raw == ".." {
            return Err(NameError::Dot(raw.to_owned()));
        }
        if raw.chars().any(|c| c == '/' || c == '\\' || c.is_control()) {
            return Err(NameError::BadChar(raw.to_owned()));
        }
        Ok(Self(raw.to_owned()))
    }

    /// [`SafeName::parse`], also refusing a leading `.` and any `..`, for
    /// names that come from another system (thread ids) and must never
    /// produce a hidden file or anything a path parser could misread.
    pub fn parse_plain(raw: &str) -> Result<Self, NameError> {
        let name = Self::parse(raw)?;
        if raw.starts_with('.') || raw.contains("..") {
            return Err(NameError::Hidden(raw.to_owned()));
        }
        Ok(name)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SafeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<Path> for SafeName {
    fn as_ref(&self) -> &Path {
        Path::new(&self.0)
    }
}

/// A validated relative path of 1..=[`MAX_REL_SEGMENTS`] [`SafeName`]s.
/// Joining it onto a directory always names something beneath it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelPath(Vec<SafeName>);

impl RelPath {
    /// Parse `a/b/c`. Empty segments, `.` and `..` are refused rather than
    /// normalized away, so a spelling never names something other than
    /// what it says.
    pub fn parse(raw: &str) -> Result<Self, NameError> {
        let mut segments = Vec::new();
        for seg in raw.split('/') {
            if segments.len() == MAX_REL_SEGMENTS {
                return Err(NameError::TooDeep);
            }
            segments.push(SafeName::parse(seg)?);
        }
        Ok(Self(segments))
    }

    pub fn from_names(names: impl IntoIterator<Item = SafeName>) -> Result<Self, NameError> {
        let segments: Vec<SafeName> = names.into_iter().collect();
        if segments.is_empty() {
            return Err(NameError::Empty);
        }
        if segments.len() > MAX_REL_SEGMENTS {
            return Err(NameError::TooDeep);
        }
        Ok(Self(segments))
    }

    pub fn segments(&self) -> &[SafeName] {
        &self.0
    }

    /// Every segment but the last.
    pub fn parent_segments(&self) -> &[SafeName] {
        &self.0[..self.0.len() - 1]
    }

    pub fn file_name(&self) -> &SafeName {
        self.0.last().expect("RelPath is never empty")
    }

    pub fn to_path_buf(&self) -> PathBuf {
        self.0.iter().collect()
    }
}

impl fmt::Display for RelPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, seg) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("/")?;
            }
            f.write_str(seg.as_str())?;
        }
        Ok(())
    }
}

/// Drop `.` components from `path` and refuse it when it is empty, only
/// `.`, or contains `..` anywhere. Absolute paths stay absolute. No
/// filesystem access: this judges the spelling alone.
pub fn normalize_no_parent(path: &Path) -> Result<PathBuf, NameError> {
    let shown = || path.display().to_string();
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::ParentDir => return Err(NameError::ParentDir(shown())),
            Component::CurDir => {}
            Component::RootDir | Component::Prefix(_) | Component::Normal(_) => {
                out.push(comp.as_os_str());
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(NameError::EmptyPath(shown()));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_name_refuses_every_escape() {
        for bad in ["", ".", "..", "a/b", "a\\b", "a\0b", "a\nb", "a\u{7f}b"] {
            assert!(SafeName::parse(bad).is_err(), "{bad:?}");
        }
        assert!(SafeName::parse(&"x".repeat(MAX_NAME_BYTES)).is_ok());
        assert_eq!(
            SafeName::parse(&"x".repeat(MAX_NAME_BYTES + 1)),
            Err(NameError::TooLong)
        );
        for ok in ["a", ".hidden", "a..b", "thread-1.json"] {
            assert!(SafeName::parse(ok).is_ok(), "{ok:?}");
        }
    }

    #[test]
    fn plain_name_also_refuses_hidden_and_double_dot() {
        for bad in [".hidden", "a..b", "../x", "a/b", ""] {
            assert!(SafeName::parse_plain(bad).is_err(), "{bad:?}");
        }
        assert!(SafeName::parse_plain("thread_01.x").is_ok());
    }

    #[test]
    fn rel_path_round_trips_and_refuses_spelling_tricks() {
        let rel = RelPath::parse("state/bells/abc.json").unwrap();
        assert_eq!(rel.to_string(), "state/bells/abc.json");
        assert_eq!(rel.file_name().as_str(), "abc.json");
        assert_eq!(rel.parent_segments().len(), 2);
        for bad in ["", "/abs", "a//b", "a/./b", "a/../b", "a/"] {
            assert!(RelPath::parse(bad).is_err(), "{bad:?}");
        }
        let deep = vec!["d"; MAX_REL_SEGMENTS + 1].join("/");
        assert_eq!(RelPath::parse(&deep), Err(NameError::TooDeep));
    }

    #[test]
    fn normalize_no_parent_judges_spelling() {
        assert_eq!(
            normalize_no_parent(Path::new("./out/./a")).unwrap(),
            PathBuf::from("out/a")
        );
        assert_eq!(
            normalize_no_parent(Path::new("/abs/x")).unwrap(),
            PathBuf::from("/abs/x")
        );
        assert!(normalize_no_parent(Path::new("")).is_err());
        assert!(normalize_no_parent(Path::new(".")).is_err());
        assert!(normalize_no_parent(Path::new("a/../b")).is_err());
    }
}
