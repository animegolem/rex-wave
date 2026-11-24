use futures_core::Stream;
use rex_wave_api::room::{RoomId, RoomRef, RoomScope};
use rex_wave_api::*;
use serde_json::{Value, json};
use std::collections::VecDeque;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

pub(crate) const SIDEBAR_URI: &str = "rex://sidebar";
const DEFAULT_ROOM_READ_LIMIT: usize = 200;
const ROOM_SCAN_BATCH: u32 = 500;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResourceTarget {
    Sidebar,
    Room(RoomRef),
    LegacyOperatorDm(Principal),
}

impl ResourceTarget {
    fn into_room_ref(self, viewer: &Principal) -> Result<RoomRef, InvalidId> {
        match self {
            ResourceTarget::Sidebar => Err(InvalidId {
                what: "room".into(),
                reason: "sidebar is not a room".into(),
            }),
            ResourceTarget::Room(r) => Ok(r),
            ResourceTarget::LegacyOperatorDm(other) => {
                let me_op = match viewer {
                    Principal::Operator { seat } | Principal::Seat { seat } => {
                        Principal::Operator { seat: seat.clone() }
                    }
                    other_me => other_me.clone(),
                };
                let pair = match &other {
                    Principal::Operator { .. } => {
                        rex_wave_api::room::PrincipalPair::new(viewer.clone(), other)?
                    }
                    _ => rex_wave_api::room::PrincipalPair::new(me_op, other)?,
                };
                Ok(RoomRef::Id {
                    id: RoomId::Direct { pair },
                })
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ResourceError {
    #[error("{0}")]
    InvalidUri(String),
    #[error(transparent)]
    Api(#[from] ApiError),
}

pub(crate) fn parse_uri(uri: &str) -> Result<ResourceTarget, ResourceError> {
    if uri == SIDEBAR_URI {
        return Ok(ResourceTarget::Sidebar);
    }
    let Some(rest) = uri.strip_prefix("rex://room/") else {
        return Err(ResourceError::InvalidUri(format!(
            "unsupported resource uri: {uri}"
        )));
    };
    if rest.is_empty() {
        return Err(ResourceError::InvalidUri(
            "room resource uri requires a room path".into(),
        ));
    }

    let raw_parts: Vec<&str> = rest.split('/').collect();
    if raw_parts.iter().any(|p| p.is_empty()) {
        return Err(ResourceError::InvalidUri(format!(
            "empty path segment in resource uri: {uri}"
        )));
    }
    let parts: Vec<String> = raw_parts
        .into_iter()
        .map(percent_decode)
        .collect::<Result<_, _>>()
        .map_err(ResourceError::InvalidUri)?;

    let api = |e: InvalidId| ResourceError::Api(e.into());
    let unsupported = || ResourceError::InvalidUri(format!("unsupported room resource uri: {uri}"));
    // Every segment but a project id is one name; a decoded `/` would
    // change which room the canonical spelling names.
    let one_name = |seg: &String| {
        if seg.contains('/') {
            Err(unsupported())
        } else {
            Ok(())
        }
    };
    let canonical = match parts.as_slice() {
        [name] => one_name(name).map(|()| format!("#{name}"))?,
        [kind, name] if kind == "channel" => one_name(name).map(|()| format!("#{name}"))?,
        [kind, seat] if kind == "dm" => one_name(seat).map(|()| format!("@{seat}"))?,
        [kind, sub, seat] if kind == "dm" && sub == "seat" => {
            one_name(seat).map(|()| format!("@{seat}"))?
        }
        [kind, sub, seat] if kind == "dm" && sub == "operator" => {
            return Ok(ResourceTarget::LegacyOperatorDm(Principal::Operator {
                seat: SeatName::parse(seat).map_err(api)?,
            }));
        }
        [kind, sub, seat] if kind == "dm" && sub == "legacy-seat" => {
            return Ok(ResourceTarget::LegacyOperatorDm(Principal::Seat {
                seat: SeatName::parse(seat).map_err(api)?,
            }));
        }
        [kind, sub, wave, role] if kind == "dm" && sub == "legacy-worker" => {
            return Ok(ResourceTarget::LegacyOperatorDm(Principal::Worker {
                wave: WaveSlug::parse(wave).map_err(api)?,
                role: RoleName::parse(role).map_err(api)?,
            }));
        }
        [kind, sub, wave, role] if kind == "dm" && sub == "worker" => {
            one_name(wave)?;
            one_name(role).map(|()| format!("@{wave}/{role}"))?
        }
        [kind, project, name] if kind == "project" => {
            one_name(name)?;
            return Ok(ResourceTarget::Room(RoomRef::Channel {
                scope: RoomScope::Project {
                    project: BbProjectId::parse(project.clone()).map_err(api)?,
                },
                name: ChannelName::parse(name.clone()).map_err(api)?,
            }));
        }
        [project, name] if project != "project" => {
            one_name(name).map(|()| format!("#{project}/{name}"))?
        }
        _ => return Err(unsupported()),
    };
    let room_ref = RoomRef::parse_str(&canonical).map_err(api)?;

    Ok(ResourceTarget::Room(room_ref))
}

pub(crate) fn room_uri(room: &RoomId, viewer: &Principal) -> Option<(String, String)> {
    match room {
        RoomId::Channel {
            scope: RoomScope::Global,
            name,
        } => Some((format!("rex://room/channel/{name}"), format!("#{name}"))),
        RoomId::Channel {
            scope: RoomScope::Project { project },
            name,
        } => {
            let encoded = percent_encode_segment(project.as_str());
            Some((
                format!("rex://room/project/{encoded}/{name}"),
                format!("#{project}/{name}"),
            ))
        }
        RoomId::Direct { pair } => {
            let other = pair.other_for_viewer(viewer)?;
            let (a, b): (Principal, Principal) = pair.clone().into();
            let has_legacy_op = a.is_operator() || b.is_operator();
            let uri = match other {
                Principal::Seat { seat } if has_legacy_op => {
                    format!("rex://room/dm/legacy-seat/{seat}")
                }
                Principal::Seat { seat } => format!("rex://room/dm/seat/{seat}"),
                Principal::Operator { seat } => format!("rex://room/dm/operator/{seat}"),
                Principal::Worker { wave, role } if has_legacy_op => {
                    format!("rex://room/dm/legacy-worker/{wave}/{role}")
                }
                Principal::Worker { wave, role } => format!("rex://room/dm/worker/{wave}/{role}"),
                Principal::System { .. } => return None,
            };
            Some((uri, format!("DM with {other}")))
        }
    }
}

pub(crate) async fn list_resources<C: Client>(client: &C) -> Result<Value, ApiError> {
    let mut resources = vec![json!({
        "uri": SIDEBAR_URI,
        "name": "Sidebar",
        "description": "Live sidebar tree: inbox badge, kanban boards, projects, wave rooms, channels, and direct messages.",
        "mimeType": "application/json",
    })];

    match client.call(read::Sidebar {}).await {
        Ok(snap) => {
            let viewer = client.me();
            for leaf in &snap.channels {
                if let Some((uri, name)) = room_uri(&leaf.room, viewer) {
                    resources.push(room_descriptor(uri, name, "Global channel stream."));
                }
            }
            for proj in &snap.projects {
                for leaf in &proj.channels {
                    if let Some((uri, name)) = room_uri(&leaf.room, viewer) {
                        resources.push(room_descriptor(uri, name, "Project channel stream."));
                    }
                }
                for wave in &proj.waves {
                    if let Some((uri, name)) = room_uri(&wave.room, viewer) {
                        resources.push(room_descriptor(uri, name, "Wave room stream."));
                    }
                }
            }
            for dm in &snap.direct_messages {
                if let Some((uri, name)) = room_uri(&dm.room, viewer) {
                    resources.push(room_descriptor(uri, name, "Direct message room stream."));
                }
            }
        }
        Err(ApiError::Unavailable { .. }) => {}
        Err(err) => return Err(err),
    }

    Ok(json!({ "resources": resources }))
}

pub(crate) fn resource_templates() -> Value {
    json!({
        "resourceTemplates": [
            {
                "uriTemplate": "rex://room/channel/{name}",
                "name": "Global channel",
                "description": "Journal entries for a global #channel.",
                "mimeType": "application/json",
            },
            {
                "uriTemplate": "rex://room/project/{project}/{name}",
                "name": "Project or wave channel",
                "description": "Journal entries for a project #channel or #wave-<slug> room.",
                "mimeType": "application/json",
            },
            {
                "uriTemplate": "rex://room/dm/seat/{seat}",
                "name": "Direct message room",
                "description": "Journal entries for a 1:1 DM with a seat.",
                "mimeType": "application/json",
            },
        ]
    })
}

pub(crate) async fn read_resource<C: Client>(
    client: &C,
    uri: &str,
) -> Result<Value, ResourceError> {
    let text = match parse_uri(uri)? {
        ResourceTarget::Sidebar => {
            let snap = client.call(read::Sidebar {}).await?;
            serde_json::to_string(&snap).map_err(|e| ApiError::Internal(e.to_string()))?
        }
        other_target => {
            let room_ref = other_target
                .into_room_ref(client.me())
                .map_err(ApiError::from)?;
            let room = room_ref.resolve(client.me()).map_err(ApiError::from)?;
            let page = read_room_tail(client, StreamId::Room { room }).await?;
            serde_json::to_string(&page).map_err(|e| ApiError::Internal(e.to_string()))?
        }
    };

    Ok(json!({
        "contents": [
            {
                "uri": uri,
                "mimeType": "application/json",
                "text": text,
            }
        ]
    }))
}

pub(crate) async fn subscribe_resource<C: Client>(
    client: &C,
    uri: String,
    out_tx: mpsc::UnboundedSender<Value>,
) -> Result<AbortHandle, ResourceError> {
    match parse_uri(&uri)? {
        ResourceTarget::Sidebar => {
            // ReadSide::watch_sidebar emits the current snapshot immediately on open; capture
            // the subscribe-time snapshot first so only real subsequent transitions notify.
            let mut last_seen = client.call(read::Sidebar {}).await.ok();
            let mut stream = client.watch(Interest::Sidebar, Cursor::live()).await?;
            let handle = tokio::spawn(async move {
                while let Some(item) = next_frame(&mut stream).await {
                    match item {
                        Ok(Frame::Sidebar(snap)) => {
                            if last_seen.as_ref() == Some(snap.as_ref()) {
                                continue;
                            }
                            last_seen = Some(*snap);
                            if out_tx.send(updated_notification(&uri)).is_err() {
                                break;
                            }
                        }
                        Ok(Frame::Committed(_)) => {
                            if out_tx.send(updated_notification(&uri)).is_err() {
                                break;
                            }
                        }
                        Ok(
                            Frame::CaughtUp { .. }
                            | Frame::Agent { .. }
                            | Frame::StageFocused { .. }
                            | Frame::Stage { .. },
                        ) => {}
                        Err(_) => break,
                    }
                }
            });
            Ok(handle.abort_handle())
        }
        other_target => {
            let room_ref = other_target
                .into_room_ref(client.me())
                .map_err(ApiError::from)?;
            room_ref.resolve(client.me()).map_err(ApiError::from)?;
            let mut stream = client
                .watch(Interest::Room { room: room_ref }, Cursor::live())
                .await?;
            let handle = tokio::spawn(async move {
                while let Some(item) = next_frame(&mut stream).await {
                    match item {
                        Ok(Frame::Committed(_)) => {
                            if out_tx.send(updated_notification(&uri)).is_err() {
                                break;
                            }
                        }
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
            });
            Ok(handle.abort_handle())
        }
    }
}

async fn read_room_tail<C: Client>(client: &C, stream: StreamId) -> Result<read::Page, ApiError> {
    let mut after = Seq::ORIGIN;
    let mut tail = VecDeque::with_capacity(DEFAULT_ROOM_READ_LIMIT);
    let mut cursor;

    loop {
        let page = client
            .call(read::Read {
                stream: stream.clone(),
                after,
                limit: Some(ROOM_SCAN_BATCH),
            })
            .await?;
        cursor = page.cursor;
        let count = page.entries.len();
        let last_seq = page.entries.last().map_or(after, |e| e.seq);
        for env in page.entries {
            if tail.len() == DEFAULT_ROOM_READ_LIMIT {
                tail.pop_front();
            }
            tail.push_back(env);
        }
        if count < ROOM_SCAN_BATCH as usize || last_seq <= after {
            break;
        }
        after = last_seq;
    }

    let entries: Vec<_> = tail.into_iter().collect();
    let total_count = (cursor.at_compatible(&stream).0 as usize).max(entries.len());
    let truncated = entries.len() < total_count;
    Ok(read::Page {
        entries,
        cursor,
        timed_out: false,
        total_count,
        truncated,
    })
}

fn room_descriptor(uri: String, name: String, description: &str) -> Value {
    json!({
        "uri": uri,
        "name": name,
        "description": description,
        "mimeType": "application/json",
    })
}

fn updated_notification(uri: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "notifications/resources/updated",
        "params": { "uri": uri },
    })
}

async fn next_frame(stream: &mut FrameStream) -> Option<Result<Frame, ApiError>> {
    std::future::poll_fn(|cx| Stream::poll_next(stream.as_mut(), cx)).await
}

fn percent_encode_segment(raw: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(raw.len());
    for &b in raw.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
    }
    out
}

fn percent_decode(seg: &str) -> Result<String, String> {
    let bytes = seg.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(format!("incomplete percent-encoding in '{seg}'"));
            }
            let hi = hex_val(bytes[i + 1])
                .ok_or_else(|| format!("invalid percent-encoding in '{seg}'"))?;
            let lo = hex_val(bytes[i + 2])
                .ok_or_else(|| format!("invalid percent-encoding in '{seg}'"))?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|e| format!("non-utf8 segment '{seg}': {e}"))
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rex_wave_api::room::PrincipalPair;

    #[test]
    fn room_uri_and_parse_uri_preserve_proj_prefixed_ids_and_operator_dms() {
        let operator = Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        };
        let alice = Principal::Seat {
            seat: SeatName::parse("alice").unwrap(),
        };
        let dm = RoomId::Direct {
            pair: PrincipalPair::new(operator.as_dm_member(), alice.clone()).unwrap(),
        };
        let (dm_uri, _) = room_uri(&dm, &operator).expect("operator must see seat-keyed DM uri");
        assert_eq!(dm_uri, "rex://room/dm/seat/alice");
        assert_eq!(
            parse_uri(&dm_uri).unwrap(),
            ResourceTarget::Room(RoomRef::Dm { with: alice })
        );

        let proj = BbProjectId::parse("proj:alpha/sub").unwrap();
        let chan = RoomId::Channel {
            scope: RoomScope::Project {
                project: proj.clone(),
            },
            name: ChannelName::parse("general").unwrap(),
        };
        let (chan_uri, _) = room_uri(&chan, &operator).unwrap();
        assert_eq!(
            parse_uri(&chan_uri).unwrap(),
            ResourceTarget::Room(RoomRef::Channel {
                scope: RoomScope::Project { project: proj },
                name: ChannelName::parse("general").unwrap(),
            })
        );
    }
}
