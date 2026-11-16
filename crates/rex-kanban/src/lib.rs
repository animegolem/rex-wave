//! # rex-kanban, the board, with no knowledge of rooms, waves, BB, or mail
//!
//! This crate owns the kanban / org-todo domain: boards, lanes, cards, the
//! per-card [`CardRevision`] used for compare-and-swap edits, and the `.org`
//! codec. Cross-references to the rest of the estate are opaque
//! [`ExternalLink`]s, so the board can run inside the rex-wave daemon (as the
//! `Board` decider in `rex-wave-board`) or as its own app without pulling in
//! any room or wave code.
//!
//! `rex-wave-api` lists these command types in its `commands!` registry, so
//! the wire names, MCP tools, and BB catalog entries for the board come from
//! the same single list as everything else.

pub mod model;
pub mod org;

pub use model::*;
