//! # rex-wave-rooms
//!
//! Public surface: [`Rooms`], [`resolve_mentions`], [`RoomMessage`], and [`room_history`].
//! State fields remain private so rooms are mutated exclusively through commands.

use rex_wave_api::read::Read;
use rex_wave_api::room::*;
use rex_wave_api::*;
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta};
use std::collections::{BTreeMap, BTreeSet};

pub struct Rooms;

#[derive(Default)]
pub struct RoomState {
    opened: Option<(RoomId, RoomOrigin)>,
    opener: Option<Principal>,
    topic: Option<String>,
    members: BTreeMap<Handle, Principal>,
    posts: BTreeSet<Seq>,
    head: Seq,
}

impl RoomState {
    fn has_member(&self, who: &Principal) -> bool {
        self.members.values().any(|p| p == who)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoomMessage {
    pub room: RoomId,
    pub seq: Seq,
    pub at: UnixMs,
    pub by: Principal,
    pub body: Body,
    pub mentions: Mentions,
    pub reply_to: Option<Seq>,
}

pub fn extract_messages(entries: &[Envelope]) -> Vec<RoomMessage> {
    let mut out = Vec::new();
    for env in entries {
        if let (StreamId::Room { room }, Event::Room(RoomEvent::Posted { body, mentions, reply_to, .. })) = (&env.stream, &env.event) {
            out.push(RoomMessage {
                room: room.clone(),
                seq: env.seq,
                at: env.at,
                by: env.by.clone(),
                body: body.clone(),
                mentions: mentions.clone(),
                reply_to: *reply_to,
            });
        }
    }
    out
}

pub async fn room_history<C: Client>(
    client: &C,
    room: RoomRef,
    after: Seq,
    limit: Option<u32>,
) -> Result<Vec<RoomMessage>, ApiError> {
    let id = room.resolve(client.me())?;
    let want = limit.unwrap_or(200).clamp(1, 1000) as usize;
    let stream = StreamId::Room { room: id };
    let mut cursor = after;
    let mut messages = Vec::new();

    loop {
        let remaining = want.saturating_sub(messages.len());
        if remaining == 0 {
            break;
        }
        let batch = remaining.clamp(64, 1000) as u32;
        let page = client.call(Read { stream: stream.clone(), after: cursor, limit: Some(batch) }).await?;
        let Some(last) = page.entries.last() else {
            break;
        };
        cursor = last.seq;
        let fetched = page.entries.len();
        for msg in extract_messages(&page.entries) {
            messages.push(msg);
            if messages.len() == want {
                return Ok(messages);
            }
        }
        if fetched < batch as usize {
            break;
        }
    }
    Ok(messages)
}

fn is_mention_prev(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'/')
}

fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'-'
}

pub fn resolve_mentions(body: &Body, members: &BTreeMap<Handle, Principal>) -> Mentions {
    let text = body.as_str();
    let bytes = text.as_bytes();
    let mut resolved = BTreeSet::new();
    let mut all = false;
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'@' && (i == 0 || !is_mention_prev(bytes[i - 1])) {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && is_token_byte(bytes[end]) {
                end += 1;
            }
            if end > start {
                let seg1 = &text[start..end];
                if end < bytes.len() && bytes[end] == b'/' {
                    let role_start = end + 1;
                    let mut role_end = role_start;
                    while role_end < bytes.len() && is_token_byte(bytes[role_end]) {
                        role_end += 1;
                    }
                    if role_end > role_start {
                        let seg2 = &text[role_start..role_end];
                        if let Some(worker) = members.values().find(|p| {
                            matches!(p, Principal::Worker { wave, role } if wave.as_str() == seg1 && role.as_str() == seg2)
                        }) {
                            resolved.insert(worker.clone());
                            i = role_end;
                            continue;
                        }
                    }
                }
                if seg1 == "all" {
                    all = true;
                } else if let Ok(handle) = Handle::parse(seg1)
                    && let Some(principal) = members.get(&handle)
                {
                    resolved.insert(principal.clone());
                }
                i = end;
                continue;
            }
        }
        i += 1;
    }

    if all {
        for member in members.values() {
            if !matches!(member, Principal::System { .. }) {
                resolved.insert(member.clone());
            }
        }
    }

    Mentions { principals: resolved.into_iter().collect(), all }
}

fn system_slug(part: SystemPart) -> &'static str {
    match part {
        SystemPart::WaveConductor => "wave_conductor",
        SystemPart::Herald => "herald",
        SystemPart::BbBridge => "bb_bridge",
        SystemPart::MailBridge => "mail_bridge",
        SystemPart::WakeBell => "wake_bell",
    }
}

fn sanitize_handle_prefix(raw: &str, max_len: usize) -> String {
    let mut s: String = raw
        .bytes()
        .map(|b| {
            let lower = b.to_ascii_lowercase();
            if lower.is_ascii_lowercase() || lower.is_ascii_digit() || lower == b'-' || lower == b'_' {
                lower as char
            } else {
                '_'
            }
        })
        .collect();
    while s.starts_with('-') {
        s.remove(0);
    }
    if s.is_empty() {
        s.push('u');
    }
    s.truncate(max_len);
    s
}

fn default_handle(p: &Principal) -> Result<Handle, ApiError> {
    let raw = match p {
        Principal::Operator { seat } | Principal::Seat { seat } => seat.as_str().to_owned(),
        Principal::Worker { role, .. } => role.as_str().to_owned(),
        Principal::System { part } => system_slug(*part).to_owned(),
    };
    Ok(Handle::parse(raw)?)
}

fn unique_handle(p: &Principal, members: &BTreeMap<Handle, Principal>) -> Result<Handle, ApiError> {
    let primary = default_handle(p)?;
    if members.get(&primary).is_none_or(|existing| existing == p) {
        return Ok(primary);
    }

    let qualified_raw = match p {
        Principal::Worker { wave, role } => format!("{wave}_{role}"),
        Principal::Operator { seat } => format!("op_{seat}"),
        Principal::Seat { seat } => format!("seat_{seat}"),
        Principal::System { part } => format!("sys_{}", system_slug(*part)),
    };
    let qualified = Handle::parse(sanitize_handle_prefix(&qualified_raw, 32))?;
    if members.get(&qualified).is_none_or(|existing| existing == p) {
        return Ok(qualified);
    }

    let base = sanitize_handle_prefix(primary.as_str(), 24);
    for n in 2u32.. {
        let candidate = Handle::parse(format!("{base}_{n}"))?;
        if members.get(&candidate).is_none_or(|existing| existing == p) {
            return Ok(candidate);
        }
    }
    unreachable!("u32 handle suffixes exceed room member count")
}

fn ensure_dm_access(pair: &PrincipalPair, by: &Principal) -> Result<(), ApiError> {
    if pair.contains(by) || matches!(by, Principal::Operator { .. } | Principal::System { .. }) {
        Ok(())
    } else {
        Err(ApiError::Denied { principal: by.to_string(), action: "access direct message room".into() })
    }
}

fn open_dm_events(id: RoomId, pair: &PrincipalPair, topic: Option<String>, members: &mut BTreeMap<Handle, Principal>) -> Result<Vec<RoomEvent>, ApiError> {
    let mut events = vec![RoomEvent::Opened { id, origin: RoomOrigin::Manual, topic }];
    let (a, b): (Principal, Principal) = pair.clone().into();
    for member in [a, b] {
        if !members.values().any(|p| p == &member) {
            let handle = unique_handle(&member, members)?;
            members.insert(handle.clone(), member.clone());
            events.push(RoomEvent::MemberJoined { member, handle });
        }
    }
    Ok(events)
}

impl Decider for Rooms {
    type State = RoomState;
    type Cmd = RoomCommand;
    type Event = RoomEvent;

    fn address(cmd: &RoomCommand, ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        let r = match cmd {
            RoomCommand::RoomEnsure(c) => &c.room,
            RoomCommand::RoomPost(c) => &c.room,
            RoomCommand::RoomJoin(c) => &c.room,
            RoomCommand::RoomLeave(c) => &c.room,
        };
        Ok(StreamId::Room { room: r.resolve(&ctx.by)? })
    }

    fn idem(cmd: &RoomCommand) -> Option<IdemKey> {
        match cmd {
            RoomCommand::RoomPost(c) => c.idem.clone(),
            _ => None,
        }
    }

    fn decide(state: &RoomState, cmd: &RoomCommand, ctx: &DecideCtx) -> Result<Vec<RoomEvent>, ApiError> {
        match cmd {
            RoomCommand::RoomEnsure(c) => {
                let id = c.room.resolve(&ctx.by)?;
                if matches!(c.origin, Some(RoomOrigin::Wave { .. })) {
                    if ctx.by != (Principal::System { part: SystemPart::WaveConductor }) {
                        return Err(ApiError::Denied { principal: ctx.by.to_string(), action: "create wave room".into() });
                    }
                    if matches!(id, RoomId::Direct { .. }) {
                        return Err(ApiError::Invalid(InvalidId {
                            what: "room origin".into(),
                            reason: "direct message rooms cannot have wave origin".into(),
                        }));
                    }
                }
                if let RoomId::Direct { pair } = &id {
                    ensure_dm_access(pair, &ctx.by)?;
                }

                if let Some((_, existing_origin)) = &state.opened {
                    if let Some(req_origin) = &c.origin
                        && req_origin != existing_origin
                    {
                        return Err(ApiError::Conflict { detail: format!("room origin already fixed as {existing_origin:?}") });
                    }
                    if let Some(topic) = &c.topic
                        && state.topic.as_ref() != Some(topic)
                    {
                        return Ok(vec![RoomEvent::TopicSet { topic: topic.clone() }]);
                    }
                    return Ok(vec![]);
                }

                let mut members = state.members.clone();
                match &id {
                    RoomId::Direct { pair } => open_dm_events(id.clone(), pair, c.topic.clone(), &mut members),
                    RoomId::Channel { .. } => {
                        let origin = c.origin.clone().unwrap_or(RoomOrigin::Manual);
                        let mut events = vec![RoomEvent::Opened { id, origin, topic: c.topic.clone() }];
                        if !matches!(ctx.by, Principal::System { .. }) {
                            let handle = unique_handle(&ctx.by, &members)?;
                            events.push(RoomEvent::MemberJoined { member: ctx.by.clone(), handle });
                        }
                        Ok(events)
                    }
                }
            }

            RoomCommand::RoomJoin(c) => {
                let id = c.room.resolve(&ctx.by)?;
                if matches!(id, RoomId::Direct { .. }) {
                    return Err(ApiError::Conflict { detail: "direct message membership is fixed to the pair".into() });
                }

                let target = c.member.clone().unwrap_or_else(|| ctx.by.clone());
                let origin = state.opened.as_ref().map_or(&RoomOrigin::Manual, |(_, o)| o);

                if target == ctx.by {
                    if let RoomOrigin::Wave { wave } = origin {
                        let allowed = matches!(ctx.by, Principal::Operator { .. } | Principal::System { .. })
                            || matches!(&ctx.by, Principal::Worker { wave: w, .. } if w == wave)
                            || state.opener.as_ref() == Some(&ctx.by)
                            || state.has_member(&ctx.by);
                        if !allowed {
                            return Err(ApiError::Denied {
                                principal: ctx.by.to_string(),
                                action: format!("self-join wave room #wave-{wave}"),
                            });
                        }
                    }
                } else {
                    let can_invite = matches!(ctx.by, Principal::Operator { .. } | Principal::System { .. })
                        || state.opener.as_ref() == Some(&ctx.by);
                    if !can_invite {
                        return Err(ApiError::Denied {
                            principal: ctx.by.to_string(),
                            action: "join another principal to room".into(),
                        });
                    }
                }

                let mut events = Vec::new();
                if state.opened.is_none() {
                    events.push(RoomEvent::Opened { id, origin: RoomOrigin::Manual, topic: None });
                }

                match &c.handle {
                    Some(handle) => {
                        if state.members.get(handle) == Some(&target) {
                            return Ok(vec![]);
                        }
                        if state.members.contains_key(handle) {
                            return Err(ApiError::Conflict { detail: format!("handle @{handle} is already in use in this room") });
                        }
                        events.push(RoomEvent::MemberJoined { member: target, handle: handle.clone() });
                    }
                    None => {
                        if state.has_member(&target) {
                            return Ok(vec![]);
                        }
                        let handle = unique_handle(&target, &state.members)?;
                        events.push(RoomEvent::MemberJoined { member: target, handle });
                    }
                }
                Ok(events)
            }

            RoomCommand::RoomLeave(c) => {
                let id = c.room.resolve(&ctx.by)?;
                if matches!(id, RoomId::Direct { .. }) {
                    return Err(ApiError::Conflict { detail: "cannot leave a direct message room".into() });
                }
                if !state.has_member(&ctx.by) {
                    return Err(ApiError::Conflict { detail: format!("{} is not a member of room", ctx.by) });
                }
                Ok(vec![RoomEvent::MemberLeft { member: ctx.by.clone() }])
            }

            RoomCommand::RoomPost(c) => {
                let id = c.room.resolve(&ctx.by)?;
                if let Some(reply_seq) = c.reply_to
                    && !state.posts.contains(&reply_seq)
                {
                    return Err(ApiError::NotFound { what: format!("message seq {} in room", reply_seq.0) });
                }

                let mut events = Vec::new();
                let mut members = state.members.clone();

                match &id {
                    RoomId::Direct { pair } => {
                        ensure_dm_access(pair, &ctx.by)?;
                        if state.opened.is_none() {
                            events.extend(open_dm_events(id.clone(), pair, None, &mut members)?);
                        }
                    }
                    RoomId::Channel { .. } => match state.opened.as_ref().map(|(_, o)| o) {
                        Some(RoomOrigin::Wave { wave }) => {
                            let allowed = state.has_member(&ctx.by)
                                || matches!(ctx.by, Principal::Operator { .. } | Principal::System { .. });
                            if !allowed {
                                return Err(ApiError::Denied {
                                    principal: ctx.by.to_string(),
                                    action: format!("post to wave room #wave-{wave} without membership"),
                                });
                            }
                            if !members.values().any(|p| p == &ctx.by) && !matches!(ctx.by, Principal::System { .. }) {
                                let handle = unique_handle(&ctx.by, &members)?;
                                members.insert(handle.clone(), ctx.by.clone());
                                events.push(RoomEvent::MemberJoined { member: ctx.by.clone(), handle });
                            }
                        }
                        Some(RoomOrigin::Manual) => {
                            if !members.values().any(|p| p == &ctx.by) && !matches!(ctx.by, Principal::System { .. }) {
                                let handle = unique_handle(&ctx.by, &members)?;
                                members.insert(handle.clone(), ctx.by.clone());
                                events.push(RoomEvent::MemberJoined { member: ctx.by.clone(), handle });
                            }
                        }
                        None => {
                            events.push(RoomEvent::Opened { id: id.clone(), origin: RoomOrigin::Manual, topic: None });
                            if !matches!(ctx.by, Principal::System { .. }) {
                                let handle = unique_handle(&ctx.by, &members)?;
                                members.insert(handle.clone(), ctx.by.clone());
                                events.push(RoomEvent::MemberJoined { member: ctx.by.clone(), handle });
                            }
                        }
                    },
                }

                let mentions = resolve_mentions(&c.body, &members);
                events.push(RoomEvent::Posted {
                    body: c.body.clone(),
                    mentions,
                    reply_to: c.reply_to,
                    idem: c.idem.clone(),
                });
                Ok(events)
            }
        }
    }

    fn evolve(state: &mut RoomState, event: &RoomEvent, meta: &EntryMeta) {
        state.head = meta.seq;
        match event {
            RoomEvent::Opened { id, origin, topic } => {
                state.opened = Some((id.clone(), origin.clone()));
                state.opener = Some(meta.by.clone());
                state.topic = topic.clone();
            }
            RoomEvent::MemberJoined { member, handle } => {
                state.members.retain(|_, p| p != member);
                state.members.insert(handle.clone(), member.clone());
            }
            RoomEvent::MemberLeft { member } => state.members.retain(|_, p| p != member),
            RoomEvent::Posted { .. } => {
                state.posts.insert(meta.seq);
            }
            RoomEvent::TopicSet { topic } => state.topic = Some(topic.clone()),
        }
    }

    fn reply(state: &RoomState, cmd: RoomCommand, committed: &[Envelope]) -> WireReply {
        match cmd {
            RoomCommand::RoomEnsure(_) => {
                let (id, origin) = state.opened.clone().expect("RoomEnsure guarantees opened state");
                WireReply::RoomEnsure(RoomInfo {
                    id,
                    origin,
                    topic: state.topic.clone(),
                    head: state.head,
                    members: state.members.iter().map(|(h, p)| (h.clone(), p.clone())).collect(),
                })
            }
            RoomCommand::RoomPost(_) => {
                let env = committed
                    .iter()
                    .rev()
                    .find(|e| matches!(e.event, Event::Room(RoomEvent::Posted { .. })))
                    .expect("RoomPost commits or replays a Posted event");
                let StreamId::Room { room } = &env.stream else { unreachable!() };
                let Event::Room(RoomEvent::Posted { mentions, .. }) = &env.event else { unreachable!() };
                WireReply::RoomPost(Posted { room: room.clone(), seq: env.seq, mentions: mentions.clone() })
            }
            RoomCommand::RoomJoin(_) => WireReply::RoomJoin(Ack),
            RoomCommand::RoomLeave(_) => WireReply::RoomLeave(Ack),
        }
    }

    fn wrap(event: RoomEvent) -> Event {
        Event::Room(event)
    }

    fn unwrap(event: &Event) -> Option<&RoomEvent> {
        match event {
            Event::Room(e) => Some(e),
            _ => None,
        }
    }
}
