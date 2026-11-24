//! # rex-wave-fs
//!
//! The one owner of `std::fs` primitives in the workspace (the root
//! `clippy.toml` refuses them everywhere else):
//!
//! - [`SafeName`] / [`RelPath`]: validated path segments, so a joined name
//!   always stays beneath its directory.
//! - [`open_regular_nofollow`], [`read_regular_bounded`],
//!   [`read_regular_string`], [`open_append_nofollow`]: `O_NOFOLLOW` opens
//!   judged by the handle (`fstat`), never by a separate `lstat`, so no
//!   check-then-open window exists at the leaf. Reads are always bounded.
//! - [`write_atomic_path`], [`write_atomic_if_absent_path`]: exclusive temp
//!   sibling, `fsync`, then `rename` or `link`.
//! - [`ConfinedDir`]: the same operations beneath a held directory.
//! - [`Protection`], [`ProtectedRootsGuard`], [`OutsidePath`]: the
//!   deployment's protected folders and the checks against them.
#![allow(clippy::disallowed_methods)]

mod file;
mod name;
mod protect;

pub use file::{
    ConfinedDir, EntryKind, dir_metadata_nofollow, ensure_private_dir, entry_exists, entry_kind,
    open_append_nofollow, open_regular_nofollow, read_private_regular, read_regular_bounded,
    read_regular_string, socket_owner_uid, sync_dir, write_atomic_if_absent_path,
    write_atomic_path,
};
pub use name::{
    MAX_NAME_BYTES, MAX_REL_SEGMENTS, NameError, RelPath, SafeName, normalize_no_parent,
};
pub use protect::{
    MAX_PROTECTED_ROOTS, MAX_PROTECTED_ROOTS_FILE_BYTES, OutsideError, OutsidePath,
    ProtectedRootsGuard, Protection, SandboxError, lexical_normalize, load_protected_roots,
    load_protected_roots_from, parse_protected_roots, protected_roots_file, resolve_links,
};
