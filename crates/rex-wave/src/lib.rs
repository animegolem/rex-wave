//! Library exports for `rex-wave` daemon composition, loopback HTTP/SSE server,
//! and shared room/principal boundary parsers.

pub mod daemon;
pub mod http;

use rex_wave_api::room::{RoomRef, RoomScope};
use rex_wave_api::*;

fn invalid(what: &str, reason: impl Into<String>) -> ApiError {
    ApiError::Invalid(InvalidId {
        what: what.to_owned(),
        reason: reason.into(),
    })
}

pub fn parse_principal_ref(raw: &str) -> Result<Principal, ApiError> {
    let trimmed = raw.strip_prefix('@').unwrap_or(raw);
    if let Some((w, r)) = trimmed.split_once('/') {
        Ok(Principal::Worker {
            wave: WaveSlug::parse(w)?,
            role: RoleName::parse(r)?,
        })
    } else {
        Ok(Principal::Seat {
            seat: SeatName::parse(trimmed)?,
        })
    }
}

pub fn parse_room_ref(s: &str) -> Result<RoomRef, ApiError> {
    if let Some(rest) = s.strip_prefix('#') {
        if let Some((proj, chan)) = rest.split_once('/') {
            let proj_clean = proj.strip_prefix("proj:").unwrap_or(proj);
            return Ok(RoomRef::Channel {
                scope: RoomScope::Project {
                    project: BbProjectId::parse(proj_clean)?,
                },
                name: ChannelName::parse(chan)?,
            });
        }
        return Ok(RoomRef::Channel {
            scope: RoomScope::Global,
            name: ChannelName::parse(rest)?,
        });
    }
    if s.starts_with('@') {
        return Ok(RoomRef::Dm {
            with: parse_principal_ref(s)?,
        });
    }
    Err(invalid(
        "room",
        "expected #<channel>, #<project>/<channel>, @<seat>, or @<wave>/<role>",
    ))
}
