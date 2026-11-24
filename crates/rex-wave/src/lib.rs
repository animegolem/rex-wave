//! Library exports for `rex-wave` daemon composition, loopback HTTP/SSE server,
//! and shared room/principal boundary parsers.

pub mod daemon;
pub mod http;

use rex_wave_api::room::RoomRef;
use rex_wave_api::*;

pub fn parse_principal_ref(raw: &str) -> Result<Principal, ApiError> {
    Ok(Principal::parse_member_ref(raw)?)
}

pub fn parse_room_ref(s: &str) -> Result<RoomRef, ApiError> {
    Ok(RoomRef::parse_str(s)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rex_wave_api::room::RoomScope;

    #[test]
    fn parse_room_ref_supports_filesystem_path_project_ids() {
        let parsed =
            parse_room_ref("#/tmp/work/sample-repo/wave-alpha").expect("path project room");
        assert_eq!(
            parsed,
            RoomRef::Channel {
                scope: RoomScope::Project {
                    project: BbProjectId::parse("/tmp/work/sample-repo").unwrap(),
                },
                name: ChannelName::parse("wave-alpha").unwrap(),
            }
        );
    }
}
