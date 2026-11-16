//! Org-mode codec for the org-todo panel:
//! `* TODO [#A] Title :label:` headings with a `:PROPERTIES:` drawer holding
//! `:CARD:`, `:REVISION:`, `:ASSIGNEE:`, and `:LINKS:`. Rendering then parsing
//! must round-trip without loss so the board and the file can sync both ways.

use crate::model::{BoardSnapshot, Card};

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OrgError {
    #[error("malformed heading at line {line}: {reason}")]
    Heading { line: usize, reason: String },
    #[error("bad property at line {line}: {reason}")]
    Property { line: usize, reason: String },
}

pub fn render(snapshot: &BoardSnapshot) -> String {
    let _ = snapshot;
    todo!("org::render")
}

pub fn parse(text: &str) -> Result<Vec<Card>, OrgError> {
    let _ = text;
    todo!("org::parse")
}
