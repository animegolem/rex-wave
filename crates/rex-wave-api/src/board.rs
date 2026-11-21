//! Kanban / org-todo. The model lives in the standalone `rex-kanban` crate;
//! this module re-exports it for the `commands!` list and adds the only
//! rex-wave-specific knowledge: how projects, seats and waves become opaque board keys,
//! assignees, and links.

pub use rex_kanban::{
    Assign, Assignee, BoardEnsure, BoardEvent, BoardFilter, BoardId, BoardInfo, BoardSnapshot,
    BoardView, Card, CardArchive, CardNo, CardPatch, CardRef, CardRevision, CardUpsert,
    ExternalLink, LaneCards, LaneName, Link, OrgId, Priority, Title,
};

use crate::id::{BbProjectId, Principal, RoleName, SeatName, Seq, WaveSlug};
use crate::mail::LetterRef;
use crate::room::RoomId;

/// The board under a BB project (wave cards land here, labeled by wave).
pub fn project_board(project: &BbProjectId) -> BoardId {
    BoardId::parse(format!("project:{project}")).expect("project ids are printable and short")
}

/// A seat's personal todo board.
pub fn seat_board(seat: &SeatName) -> BoardId {
    BoardId::parse(format!("seat:{seat}")).expect("seat names are slugs")
}

pub fn assignee(principal: &Principal) -> Assignee {
    Assignee::parse(principal.to_string()).expect("principal display is short and printable")
}

pub fn message_link(room: &RoomId, seq: Seq) -> Link {
    Link::External(ExternalLink {
        namespace: "rex-wave.message".into(),
        key: serde_json::json!({ "room": room, "seq": seq }).to_string(),
    })
}

pub fn worker_link(wave: &WaveSlug, role: &RoleName) -> Link {
    Link::External(ExternalLink {
        namespace: "rex-wave.worker".into(),
        key: format!("{wave}/{role}"),
    })
}

pub fn letter_link(letter: &LetterRef) -> Link {
    Link::External(ExternalLink {
        namespace: "rex-wave.letter".into(),
        key: letter.sha256.to_string(),
    })
}
