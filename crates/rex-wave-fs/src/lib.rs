//! # rex-wave-fs
//!
//! Workspace owner of the `std::fs` primitives banned by the root
//! `clippy.toml` (`symlink_metadata`, `read`, `read_to_string`, `rename`,
//! `hard_link`, `canonicalize`), plus descriptor-checked directory and lock
//! helpers:
//!
//! - [`SafeName`] / [`RelPath`]: validated path segments, so a joined name
//!   always stays beneath its directory.
//! - [`open_regular_nofollow`], [`read_regular_bounded`],
//!   [`read_regular_string`], [`open_append_nofollow`],
//!   [`open_rw_append_nofollow`]: `O_NOFOLLOW` opens judged by the handle
//!   (`fstat`), never by a separate `lstat`, so no check-then-open window
//!   exists at the leaf. Reads are always bounded.
//! - [`read_input_bounded`], [`read_input_string`]: bounded reads for
//!   operator-chosen CLI input paths (allowing symlinks, pipes, and process
//!   substitution while refusing directories and oversized inputs).
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
    ConfinedDir, EntryKind, FileId, PreparedUnixSocket, claim_regular_at, claim_regular_sibling,
    create_missing_private_dir, current_process_uid, dir_metadata_nofollow, ensure_private_dir,
    entry_exists, entry_kind, file_id, lock_exclusive_blocking, open_append_nofollow,
    open_dir_nofollow, open_lock_at, open_lock_nofollow, open_regular_at, open_regular_nofollow,
    open_rw_append_at, open_rw_append_nofollow, open_sub_dir_at, prepare_unix_socket,
    read_claimed_at_if_unchanged, read_input_bounded, read_input_string, read_open_bounded,
    read_private_regular, read_regular_bounded, read_regular_string, socket_owner_uid, sync_dir,
    unlink_regular_at, validate_unix_socket_target, verify_same_regular_nofollow, write_atomic_at,
    write_atomic_if_absent_at, write_atomic_if_absent_path, write_atomic_path,
};
pub use name::{
    MAX_NAME_BYTES, MAX_REL_SEGMENTS, NameError, RelPath, SafeName, normalize_no_parent,
};
pub use protect::{
    MAX_PROTECTED_ROOTS, MAX_PROTECTED_ROOTS_FILE_BYTES, OutsideError, OutsidePath,
    ProtectedRootsGuard, Protection, SandboxError, canonicalize_existing_prefix,
    is_case_insensitive_dir, lexical_normalize, load_protected_roots, load_protected_roots_from,
    open_outside_rw_append_confined, parse_protected_roots, protected_roots_file, resolve_links,
    resolve_path_preserving_symlinks, write_outside_atomic_confined,
};
