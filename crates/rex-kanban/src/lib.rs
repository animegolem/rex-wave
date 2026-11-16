//! Standalone Kanban and Org-todo domain model, `CardRevision` compare-and-swap,
//! embeddable [`KanbanEngine`], and bidirectional `.org` codec.

pub mod model;
pub mod org;

pub use model::*;
pub use org::{OrgDocument, OrgError};
