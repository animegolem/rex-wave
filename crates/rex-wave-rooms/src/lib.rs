//! # rex-wave-rooms
//!
//! Public surface: [`Rooms`], [`RoomState`], [`resolve_mentions`], [`RoomMessage`],
//! and [`room_history`]. State fields remain private so rooms are mutated
//! exclusively through commands.

use rex_wave_api::board::{Priority, Title};
use rex_wave_api::read::Read;
use rex_wave_api::room::*;
use rex_wave_api::*;
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta};
use std::collections::{BTreeMap, BTreeSet};

const DEFAULT_TASK_LEASE_TTL_MS: u64 = 300_000;

pub struct Rooms;

#[derive(Clone, Debug)]
struct PostRecord {
    seq: Seq,
    at: UnixMs,
    by: Principal,
    body: Body,
    re: Option<Seq>,
    labels: BTreeSet<Label>,
}

impl PostRecord {
    fn to_summary(&self) -> RoomMessageSummary {
        RoomMessageSummary {
            seq: self.seq,
            at: self.at,
            by: self.by.clone(),
            body: self.body.clone(),
            re: self.re,
            labels: self.labels.iter().cloned().collect(),
        }
    }
}

#[derive(Clone, Debug)]
struct TaskRecord {
    seq: Seq,
    org_id: OrgId,
    rev: u64,
    title: Title,
    status: TaskStatus,
    priority: Priority,
    due_ms: Option<UnixMs>,
    needs: Vec<Seq>,
    lease: Option<TaskLease>,
    last_epoch: LeaseEpoch,
    created_by: Principal,
    created_at: UnixMs,
    updated_at: UnixMs,
}

#[derive(Default)]
pub struct RoomState {
    opened: Option<(RoomId, RoomOrigin)>,
    opener: Option<Principal>,
    topic: Option<String>,
    members: BTreeMap<Handle, Principal>,
    posts: BTreeMap<Seq, PostRecord>,
    last_post_seq: Option<Seq>,
    thread_tails: BTreeMap<Seq, Seq>,
    tasks: BTreeMap<Seq, TaskRecord>,
    org_ids: BTreeMap<OrgId, Seq>,
    head: Seq,
}

impl RoomState {
    fn has_member(&self, who: &Principal) -> bool {
        self.members.values().any(|p| p == who)
    }

    /// Project a single task thread at timestamp `at` (evaluating lease expiry
    /// and dependency readiness purely from `at`).
    pub fn task_view_at(&self, seq: Seq, at: UnixMs) -> Option<TaskView> {
        let (room, _) = self.opened.as_ref()?;
        let rec = self.tasks.get(&seq)?;
        let labels = self
            .posts
            .get(&seq)
            .map(|p| p.labels.iter().cloned().collect())
            .unwrap_or_default();
        let lease_expired = rec.status == TaskStatus::Claimed
            && rec.lease.as_ref().is_some_and(|l| l.expires_at <= at);
        let all_needs_done = rec.needs.iter().all(|dep| {
            self.tasks
                .get(dep)
                .is_some_and(|t| t.status == TaskStatus::Done)
        });
        let unheld_or_expired = rec.status == TaskStatus::Open
            || (rec.status == TaskStatus::Claimed && (rec.lease.is_none() || lease_expired));
        Some(TaskView {
            room: room.clone(),
            seq: rec.seq,
            org_id: rec.org_id.clone(),
            rev: rec.rev,
            title: rec.title.clone(),
            status: rec.status,
            priority: rec.priority,
            due_ms: rec.due_ms,
            needs: rec.needs.clone(),
            labels,
            lease: rec.lease.clone(),
            ready: all_needs_done && unheld_or_expired,
            lease_expired,
            created_by: rec.created_by.clone(),
            created_at: rec.created_at,
            updated_at: rec.updated_at,
        })
    }

    /// Project the filtered task list of an opened room at timestamp `at`.
    pub fn task_list_at(&self, filter: Option<&TaskFilter>, at: UnixMs) -> Option<TaskListView> {
        self.task_list_page_at(filter, None, at)
    }

    /// Project the filtered, optionally capped task list of an opened room at
    /// timestamp `at`, reporting the full matching `total_count` and whether
    /// `limit` truncated the returned slice.
    pub fn task_list_page_at(
        &self,
        filter: Option<&TaskFilter>,
        limit: Option<u32>,
        at: UnixMs,
    ) -> Option<TaskListView> {
        let (room, _) = self.opened.as_ref()?;
        let mut tasks = Vec::new();
        for &seq in self.tasks.keys() {
            let Some(view) = self.task_view_at(seq, at) else {
                continue;
            };
            if let Some(f) = filter {
                if let Some(want_status) = f.status
                    && view.status != want_status
                {
                    continue;
                }
                if f.ready_only && !view.ready {
                    continue;
                }
                if let Some(want_holder) = &f.holder {
                    let held = view
                        .lease
                        .as_ref()
                        .is_some_and(|l| &l.holder == want_holder && !view.lease_expired);
                    if !held {
                        continue;
                    }
                }
                if let Some(want_label) = &f.label
                    && !view.labels.contains(want_label)
                {
                    continue;
                }
            }
            tasks.push(view);
        }
        let total_count = tasks.len();
        if let Some(cap) = limit {
            tasks.truncate(cap.clamp(1, 1000) as usize);
        }
        let truncated = tasks.len() < total_count;
        Some(TaskListView {
            room: room.clone(),
            head: self.head,
            tasks,
            total_count,
            truncated,
        })
    }

    fn creates_cycle(&self, task: Seq, candidate_needs: &[Seq]) -> bool {
        let mut visited = BTreeSet::new();
        let mut stack: Vec<Seq> = candidate_needs.to_vec();
        while let Some(node) = stack.pop() {
            if node == task {
                return true;
            }
            if !visited.insert(node) {
                continue;
            }
            if let Some(rec) = self.tasks.get(&node) {
                stack.extend(rec.needs.iter().copied());
            }
        }
        false
    }

    fn unmet_needs(&self, task: &TaskRecord) -> Vec<Seq> {
        task.needs
            .iter()
            .copied()
            .filter(|dep| {
                self.tasks
                    .get(dep)
                    .is_none_or(|t| t.status != TaskStatus::Done)
            })
            .collect()
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

fn extract_messages(entries: &[Envelope]) -> Vec<RoomMessage> {
    let mut out = Vec::new();
    for env in entries {
        if let (
            StreamId::Room { room },
            Event::Room(RoomEvent::Posted {
                body, mentions, re, ..
            }),
        ) = (&env.stream, &env.event)
        {
            out.push(RoomMessage {
                room: room.clone(),
                seq: env.seq,
                at: env.at,
                by: env.by.clone(),
                body: body.clone(),
                mentions: mentions.clone(),
                reply_to: *re,
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
    let want = limit.unwrap_or(200).clamp(1, 1000) as usize;
    let mut cursor = after;
    let mut messages = Vec::new();

    loop {
        let remaining = want.saturating_sub(messages.len());
        if remaining == 0 {
            break;
        }
        let batch = remaining.clamp(64, 1000) as u32;
        let page = client
            .call(Read::room_ref(
                room.clone(),
                client.me(),
                cursor,
                None,
                Some(batch),
            )?)
            .await?;
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

fn backtick_run(bytes: &[u8], at: usize) -> usize {
    bytes[at..].iter().take_while(|&&b| b == b'`').count()
}

/// Where the code span or fenced block opening at `open` ends: just past the
/// next backtick run of the same length (CommonMark's closing rule), or `None`
/// when it never closes and the backticks are literal.
fn code_span_end(bytes: &[u8], open: usize) -> Option<usize> {
    let n = backtick_run(bytes, open);
    let mut i = open + n;
    while i < bytes.len() {
        if bytes[i] == b'`' {
            let run = backtick_run(bytes, i);
            if run == n {
                return Some(i + run);
            }
            i += run;
        } else {
            i += 1;
        }
    }
    None
}

/// Mentions inside inline code or fenced blocks are quoted, not addressed.
pub fn resolve_mentions(body: &Body, members: &BTreeMap<Handle, Principal>) -> Mentions {
    let text = body.as_str();
    let bytes = text.as_bytes();
    let mut resolved = BTreeSet::new();
    let mut all = false;
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'`' {
            i = code_span_end(bytes, i).unwrap_or(i + backtick_run(bytes, i));
            continue;
        }
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

    Mentions {
        principals: resolved.into_iter().collect(),
        all,
    }
}

fn system_slug(part: SystemPart) -> &'static str {
    match part {
        SystemPart::WaveConductor => "wave_conductor",
        SystemPart::Herald => "herald",
        SystemPart::BbBridge => "bb_bridge",
        SystemPart::T3Bridge => "t3_bridge",
        SystemPart::MailBridge => "mail_bridge",
        SystemPart::WakeBell => "wake_bell",
        SystemPart::PresenceMonitor => "presence_monitor",
    }
}

fn principal_seed(p: &Principal) -> String {
    match p {
        Principal::Operator { seat } => format!("op:{seat}"),
        Principal::Seat { seat } => format!("seat:{seat}"),
        Principal::Worker { wave, role } => format!("worker:{wave}/{role}"),
        Principal::System { part } => format!("sys:{}", system_slug(*part)),
    }
}

fn room_id_seed(id: &RoomId) -> String {
    match id {
        RoomId::Channel {
            scope: RoomScope::Global,
            name,
        } => format!("channel:global:{name}"),
        RoomId::Channel {
            scope: RoomScope::Project { project },
            name,
        } => format!("channel:project:{project}:{name}"),
        RoomId::Direct { pair } => {
            let (lo, hi): (Principal, Principal) = pair.clone().into();
            format!("dm:{}:{}", principal_seed(&lo), principal_seed(&hi))
        }
    }
}

fn mint_task_org_id(room_id: &RoomId, seq: Seq, existing: &BTreeMap<OrgId, Seq>) -> OrgId {
    let room_key = room_id_seed(room_id);
    for attempt in 0u64.. {
        let seed = if attempt == 0 {
            format!("rex-wave:room:{room_key}:task:{}", seq.0)
        } else {
            format!("rex-wave:room:{room_key}:task:{}:{attempt}", seq.0)
        };
        let candidate = OrgId::mint_deterministic(&seed);
        if !existing.contains_key(&candidate) {
            return candidate;
        }
    }
    unreachable!("deterministic OrgId namespace exhausted")
}

fn sanitize_handle_prefix(raw: &str, max_len: usize) -> String {
    let mut s: String = raw
        .bytes()
        .map(|b| {
            let lower = b.to_ascii_lowercase();
            if lower.is_ascii_lowercase()
                || lower.is_ascii_digit()
                || lower == b'-'
                || lower == b'_'
            {
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
    if pair.contains(by) || by.is_operator_or_system() {
        Ok(())
    } else {
        Err(ApiError::Denied {
            principal: by.to_string(),
            action: "access direct message room".into(),
        })
    }
}

fn ensure_wave_name_policy(
    id: &RoomId,
    origin: &RoomOrigin,
    by: &Principal,
) -> Result<(), ApiError> {
    if let RoomId::Channel { name, .. } = id
        && name.as_str().starts_with("wave-")
        && !matches!(origin, RoomOrigin::Wave { .. })
    {
        return Err(ApiError::Denied {
            principal: by.to_string(),
            action: format!("open reserved wave channel #{name} without wave origin"),
        });
    }
    Ok(())
}

fn ensure_existing_room_write_access(
    state: &RoomState,
    id: &RoomId,
    by: &Principal,
    action: &str,
) -> Result<(), ApiError> {
    if let RoomId::Direct { pair } = id {
        ensure_dm_access(pair, by)?;
    }
    let Some((_, origin)) = &state.opened else {
        return Err(ApiError::NotFound {
            what: format!("room {id:?}"),
        });
    };
    if let RoomOrigin::Wave { wave } = origin {
        let allowed = state.has_member(by) || by.is_operator_or_system();
        if !allowed {
            return Err(ApiError::Denied {
                principal: by.to_string(),
                action: format!("{action} in wave room #wave-{wave} without membership"),
            });
        }
    }
    Ok(())
}

fn resolve_mentions_for_room(
    id: &RoomId,
    by: &Principal,
    body: &Body,
    members: &BTreeMap<Handle, Principal>,
) -> Mentions {
    let mut mentions = resolve_mentions(body, members);
    if matches!(by, Principal::System { .. }) && matches!(id, RoomId::Channel { .. }) {
        mentions.principals.retain(|p| !p.is_operator());
    }
    mentions
}

fn validate_ttl(ttl_ms: Option<u64>) -> Result<u64, ApiError> {
    match ttl_ms {
        Some(0) => Err(ApiError::Invalid(InvalidId {
            what: "ttl_ms".into(),
            reason: "must be >= 1 ms".into(),
        })),
        Some(ms) => Ok(ms),
        None => Ok(DEFAULT_TASK_LEASE_TTL_MS),
    }
}

fn open_dm_events(
    id: RoomId,
    pair: &PrincipalPair,
    topic: Option<String>,
    members: &mut BTreeMap<Handle, Principal>,
) -> Result<Vec<RoomEvent>, ApiError> {
    let mut events = vec![RoomEvent::Opened {
        id,
        origin: RoomOrigin::Manual,
        topic,
    }];
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
            RoomCommand::RoomLabel(c) => &c.room,
            RoomCommand::TaskClaim(c) => &c.room,
            RoomCommand::TaskHeartbeat(c) => &c.room,
            RoomCommand::TaskTransition(c) => &c.room,
            RoomCommand::TaskUpdate(c) => &c.room,
            RoomCommand::RoomTasks(c) => &c.room,
        };
        Ok(StreamId::Room {
            room: r.resolve(ctx.by())?,
        })
    }

    fn idem(cmd: &RoomCommand) -> Option<IdemKey> {
        cmd.effective_idem()
    }

    fn decide(
        state: &RoomState,
        cmd: &RoomCommand,
        ctx: &DecideCtx,
    ) -> Result<Vec<RoomEvent>, ApiError> {
        match cmd {
            RoomCommand::RoomEnsure(c) => {
                let id = c.room.resolve(ctx.by())?;
                if matches!(c.origin, Some(RoomOrigin::Wave { .. })) {
                    if *ctx.by()
                        != (Principal::System {
                            part: SystemPart::WaveConductor,
                        })
                    {
                        return Err(ApiError::Denied {
                            principal: ctx.by().to_string(),
                            action: "create wave room".into(),
                        });
                    }
                    if matches!(id, RoomId::Direct { .. }) {
                        return Err(ApiError::Invalid(InvalidId {
                            what: "room origin".into(),
                            reason: "direct message rooms cannot have wave origin".into(),
                        }));
                    }
                }
                if let RoomId::Direct { pair } = &id {
                    ensure_dm_access(pair, ctx.by())?;
                }

                if let Some((_, existing_origin)) = &state.opened {
                    if let Some(req_origin) = &c.origin
                        && req_origin != existing_origin
                    {
                        return Err(ApiError::conflict(format!(
                            "room origin already fixed as {existing_origin:?}"
                        )));
                    }
                    if let Some(topic) = &c.topic
                        && state.topic.as_ref() != Some(topic)
                    {
                        return Ok(vec![RoomEvent::TopicSet {
                            topic: topic.clone(),
                        }]);
                    }
                    return Ok(vec![]);
                }

                let mut members = state.members.clone();
                match &id {
                    RoomId::Direct { pair } => {
                        open_dm_events(id.clone(), pair, c.topic.clone(), &mut members)
                    }
                    RoomId::Channel { .. } => {
                        let origin = c.origin.clone().unwrap_or(RoomOrigin::Manual);
                        ensure_wave_name_policy(&id, &origin, ctx.by())?;
                        let mut events = vec![RoomEvent::Opened {
                            id,
                            origin,
                            topic: c.topic.clone(),
                        }];
                        if !matches!(ctx.by(), Principal::System { .. }) {
                            let handle = unique_handle(ctx.by(), &members)?;
                            events.push(RoomEvent::MemberJoined {
                                member: ctx.by().clone(),
                                handle,
                            });
                        }
                        Ok(events)
                    }
                }
            }

            RoomCommand::RoomJoin(c) => {
                let id = c.room.resolve(ctx.by())?;
                if matches!(id, RoomId::Direct { .. }) {
                    return Err(ApiError::conflict(
                        "direct message membership is fixed to the pair",
                    ));
                }

                let target = c.member.clone().unwrap_or_else(|| ctx.by().clone());
                let origin = state
                    .opened
                    .as_ref()
                    .map_or(&RoomOrigin::Manual, |(_, o)| o);

                if target == *ctx.by() {
                    if let RoomOrigin::Wave { wave } = origin {
                        let allowed = ctx.by().is_operator_or_system()
                            || matches!(ctx.by(), Principal::Worker { wave: w, .. } if w == wave)
                            || state.opener.as_ref() == Some(ctx.by())
                            || state.has_member(ctx.by());
                        if !allowed {
                            return Err(ApiError::Denied {
                                principal: ctx.by().to_string(),
                                action: format!("self-join wave room #wave-{wave}"),
                            });
                        }
                    }
                } else {
                    let can_invite =
                        ctx.by().is_operator_or_system() || state.opener.as_ref() == Some(ctx.by());
                    if !can_invite {
                        return Err(ApiError::Denied {
                            principal: ctx.by().to_string(),
                            action: "join another principal to room".into(),
                        });
                    }
                }

                let mut events = Vec::new();
                if state.opened.is_none() {
                    ensure_wave_name_policy(&id, &RoomOrigin::Manual, ctx.by())?;
                    events.push(RoomEvent::Opened {
                        id,
                        origin: RoomOrigin::Manual,
                        topic: None,
                    });
                }

                match &c.handle {
                    Some(handle) => {
                        if state.members.get(handle) == Some(&target) {
                            return Ok(vec![]);
                        }
                        if state.members.contains_key(handle) {
                            return Err(ApiError::conflict(format!(
                                "handle @{handle} is already in use in this room"
                            )));
                        }
                        events.push(RoomEvent::MemberJoined {
                            member: target,
                            handle: handle.clone(),
                        });
                    }
                    None => {
                        if state.has_member(&target) {
                            return Ok(vec![]);
                        }
                        let handle = unique_handle(&target, &state.members)?;
                        events.push(RoomEvent::MemberJoined {
                            member: target,
                            handle,
                        });
                    }
                }
                Ok(events)
            }

            RoomCommand::RoomLeave(c) => {
                let id = c.room.resolve(ctx.by())?;
                if matches!(id, RoomId::Direct { .. }) {
                    return Err(ApiError::conflict("cannot leave a direct message room"));
                }
                if !state.has_member(ctx.by()) {
                    return Err(ApiError::conflict(format!(
                        "{} is not a member of room",
                        ctx.by()
                    )));
                }
                Ok(vec![RoomEvent::MemberLeft {
                    member: ctx.by().clone(),
                }])
            }

            RoomCommand::RoomPost(c) => {
                let id = c.room.resolve(ctx.by())?;

                // Authorize room access before inspecting `re` or `after` so unauthorized
                // callers cannot probe message existence or read newer bodies via ConflictData.
                let mut events = Vec::new();
                let mut members = state.members.clone();
                match &id {
                    RoomId::Direct { pair } => {
                        ensure_dm_access(pair, ctx.by())?;
                        if state.opened.is_none() {
                            events.extend(open_dm_events(id.clone(), pair, None, &mut members)?);
                        }
                    }
                    RoomId::Channel { .. } => match state.opened.as_ref().map(|(_, o)| o) {
                        Some(RoomOrigin::Wave { wave }) => {
                            let allowed =
                                state.has_member(ctx.by()) || ctx.by().is_operator_or_system();
                            if !allowed {
                                return Err(ApiError::Denied {
                                    principal: ctx.by().to_string(),
                                    action: format!(
                                        "post to wave room #wave-{wave} without membership"
                                    ),
                                });
                            }
                            if !members.values().any(|p| p == ctx.by())
                                && !matches!(ctx.by(), Principal::System { .. })
                            {
                                let handle = unique_handle(ctx.by(), &members)?;
                                members.insert(handle.clone(), ctx.by().clone());
                                events.push(RoomEvent::MemberJoined {
                                    member: ctx.by().clone(),
                                    handle,
                                });
                            }
                        }
                        Some(RoomOrigin::Manual) => {
                            if !members.values().any(|p| p == ctx.by())
                                && !matches!(ctx.by(), Principal::System { .. })
                            {
                                let handle = unique_handle(ctx.by(), &members)?;
                                members.insert(handle.clone(), ctx.by().clone());
                                events.push(RoomEvent::MemberJoined {
                                    member: ctx.by().clone(),
                                    handle,
                                });
                            }
                        }
                        None => {
                            ensure_wave_name_policy(&id, &RoomOrigin::Manual, ctx.by())?;
                            events.push(RoomEvent::Opened {
                                id: id.clone(),
                                origin: RoomOrigin::Manual,
                                topic: None,
                            });
                            if !matches!(ctx.by(), Principal::System { .. }) {
                                let handle = unique_handle(ctx.by(), &members)?;
                                members.insert(handle.clone(), ctx.by().clone());
                                events.push(RoomEvent::MemberJoined {
                                    member: ctx.by().clone(),
                                    handle,
                                });
                            }
                        }
                    },
                }

                if c.task.is_some() && c.re.is_some() {
                    return Err(ApiError::Invalid(InvalidId {
                        what: "task".into(),
                        reason: "a task must be a thread root (re must be omitted)".into(),
                    }));
                }

                let canonical_re = match c.re {
                    Some(reply_seq) => {
                        let Some(target) = state.posts.get(&reply_seq) else {
                            return Err(ApiError::NotFound {
                                what: format!("message seq {} in room", reply_seq.0),
                            });
                        };
                        Some(target.re.unwrap_or(target.seq))
                    }
                    None => None,
                };

                if let Some(expected_after) = c.after {
                    let latest_seq = match canonical_re {
                        Some(root) => state.thread_tails.get(&root).copied().unwrap_or(root),
                        None => state.last_post_seq.unwrap_or(Seq::ORIGIN),
                    };
                    if latest_seq > expected_after {
                        let newer = &state.posts[&latest_seq];
                        return Err(ApiError::conflict_with(
                            format!(
                                "newer message seq {} arrived after expected seq {}",
                                latest_seq.0, expected_after.0
                            ),
                            ConflictData::NewerMessage {
                                expected: expected_after,
                                latest: latest_seq,
                                newer_message: Box::new(newer.to_summary()),
                            },
                        ));
                    }
                }

                let post_seq = Seq(state.head.0 + events.len() as u64 + 1);
                let normalized_task = match &c.task {
                    Some(spec) => {
                        let mut needs = Vec::with_capacity(spec.needs.len());
                        for &dep in &spec.needs {
                            if dep == post_seq {
                                return Err(ApiError::Invalid(InvalidId {
                                    what: "needs".into(),
                                    reason: format!("task seq {} cannot depend on itself", dep.0),
                                }));
                            }
                            if !state.tasks.contains_key(&dep) {
                                if state.posts.contains_key(&dep) {
                                    return Err(ApiError::Invalid(InvalidId {
                                        what: "needs".into(),
                                        reason: format!(
                                            "seq {} is a message, not a task root in this room",
                                            dep.0
                                        ),
                                    }));
                                }
                                return Err(ApiError::NotFound {
                                    what: format!("task seq {} in room", dep.0),
                                });
                            }
                            if !needs.contains(&dep) {
                                needs.push(dep);
                            }
                        }
                        let org_id = match &spec.org_id {
                            Some(oid) => {
                                if state.org_ids.contains_key(oid) {
                                    return Err(ApiError::conflict(format!(
                                        "org id `{oid}` is already used by another task in this room"
                                    )));
                                }
                                oid.clone()
                            }
                            None => mint_task_org_id(&id, post_seq, &state.org_ids),
                        };
                        Some(TaskSpec {
                            title: spec.title.clone(),
                            org_id: Some(org_id),
                            needs,
                            priority: spec.priority,
                            due_ms: spec.due_ms,
                        })
                    }
                    None => None,
                };

                let labels: Vec<Label> = c
                    .labels
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let mentions = resolve_mentions_for_room(&id, ctx.by(), &c.body, &members);
                events.push(RoomEvent::Posted {
                    body: c.body.clone(),
                    mentions,
                    re: canonical_re,
                    after: c.after,
                    labels,
                    task: normalized_task,
                    idem: c.idem.clone(),
                    origin: c.origin,
                });
                Ok(events)
            }

            RoomCommand::RoomLabel(c) => {
                let id = c.room.resolve(ctx.by())?;
                ensure_existing_room_write_access(state, &id, ctx.by(), "label message")?;
                let Some(post) = state.posts.get(&c.seq) else {
                    return Err(ApiError::NotFound {
                        what: format!("message seq {} in room", c.seq.0),
                    });
                };
                let before_set = &post.labels;
                let mut after_set = before_set.clone();
                for lbl in &c.remove {
                    after_set.remove(lbl);
                }
                for lbl in &c.add {
                    after_set.insert(lbl.clone());
                }
                let added: Vec<Label> = after_set.difference(before_set).cloned().collect();
                let removed: Vec<Label> = before_set.difference(&after_set).cloned().collect();
                if added.is_empty() && removed.is_empty() {
                    return Ok(vec![]);
                }
                Ok(vec![RoomEvent::Labeled {
                    target: c.seq,
                    added,
                    removed,
                    before: before_set.iter().cloned().collect(),
                    after: after_set.into_iter().collect(),
                }])
            }

            RoomCommand::TaskClaim(c) => {
                let id = c.room.resolve(ctx.by())?;
                ensure_existing_room_write_access(state, &id, ctx.by(), "claim task")?;
                if c.force && !ctx.by().is_operator() {
                    return Err(ApiError::Denied {
                        principal: ctx.by().to_string(),
                        action: format!("force-claim task seq {}", c.task.0),
                    });
                }
                let ttl_ms = validate_ttl(c.ttl_ms)?;
                let Some(task) = state.tasks.get(&c.task) else {
                    return Err(ApiError::NotFound {
                        what: format!("task seq {} in room", c.task.0),
                    });
                };
                if let Some(expected) = c.expected_rev
                    && expected != task.rev
                {
                    return Err(ApiError::conflict_with(
                        format!(
                            "task seq {} is at rev {}, expected {expected}",
                            c.task.0, task.rev
                        ),
                        ConflictData::RevisionConflict {
                            expected,
                            actual: task.rev,
                        },
                    ));
                }

                match task.status {
                    TaskStatus::Open | TaskStatus::Review | TaskStatus::Blocked => {}
                    TaskStatus::Claimed => {
                        if let Some(lease) = &task.lease
                            && lease.expires_at > ctx.at()
                        {
                            return Err(ApiError::conflict_with(
                                format!(
                                    "task seq {} is actively claimed by {} at epoch {}",
                                    c.task.0, lease.holder, lease.epoch.0
                                ),
                                ConflictData::LeaseFenced {
                                    expected: LeaseEpoch(0),
                                    actual: lease.epoch,
                                    holder: Some(lease.holder.clone()),
                                },
                            ));
                        }
                    }
                    TaskStatus::Done | TaskStatus::Cancelled => {
                        return Err(ApiError::conflict(format!(
                            "cannot claim terminal task seq {} in status {:?}",
                            c.task.0, task.status
                        )));
                    }
                }

                let unmet = state.unmet_needs(task);
                let force_allowed = c.force && ctx.by().is_operator();
                if !unmet.is_empty() && !force_allowed {
                    return Err(ApiError::conflict_with(
                        format!("task seq {} has unmet dependencies: {:?}", c.task.0, unmet),
                        ConflictData::UnmetNeeds { unmet },
                    ));
                }

                let lease = TaskLease {
                    holder: ctx.by().clone(),
                    epoch: task.last_epoch.next(),
                    claimed_at: ctx.at(),
                    expires_at: UnixMs(ctx.at().0.saturating_add(ttl_ms)),
                };
                Ok(vec![RoomEvent::TaskClaimed {
                    task: c.task,
                    rev: task.rev + 1,
                    lease,
                }])
            }

            RoomCommand::TaskHeartbeat(c) => {
                let id = c.room.resolve(ctx.by())?;
                ensure_existing_room_write_access(state, &id, ctx.by(), "heartbeat task")?;
                let ttl_ms = validate_ttl(c.ttl_ms)?;
                let Some(task) = state.tasks.get(&c.task) else {
                    return Err(ApiError::NotFound {
                        what: format!("task seq {} in room", c.task.0),
                    });
                };
                let valid_lease = task.status == TaskStatus::Claimed
                    && task.lease.as_ref().is_some_and(|l| {
                        l.holder == *ctx.by() && l.epoch == c.epoch && l.expires_at > ctx.at()
                    });
                if !valid_lease {
                    let (actual, holder) = match &task.lease {
                        Some(l) => (l.epoch, Some(l.holder.clone())),
                        None => (task.last_epoch, None),
                    };
                    return Err(ApiError::conflict_with(
                        format!(
                            "task seq {} lease fenced for {} at epoch {} (current epoch {})",
                            c.task.0,
                            ctx.by(),
                            c.epoch.0,
                            actual.0
                        ),
                        ConflictData::LeaseFenced {
                            expected: c.epoch,
                            actual,
                            holder,
                        },
                    ));
                }
                Ok(vec![RoomEvent::TaskHeartbeat {
                    task: c.task,
                    epoch: c.epoch,
                    expires_at: UnixMs(ctx.at().0.saturating_add(ttl_ms)),
                }])
            }

            RoomCommand::TaskTransition(c) => {
                let id = c.room.resolve(ctx.by())?;
                ensure_existing_room_write_access(state, &id, ctx.by(), "transition task")?;
                if c.status == TaskStatus::Claimed {
                    return Err(ApiError::Invalid(InvalidId {
                        what: "status".into(),
                        reason: "use task_claim to claim a task with a TTL lease".into(),
                    }));
                }
                let Some(task) = state.tasks.get(&c.task) else {
                    return Err(ApiError::NotFound {
                        what: format!("task seq {} in room", c.task.0),
                    });
                };
                if let Some(expected) = c.expected_rev
                    && expected != task.rev
                {
                    return Err(ApiError::conflict_with(
                        format!(
                            "task seq {} is at rev {}, expected {expected}",
                            c.task.0, task.rev
                        ),
                        ConflictData::RevisionConflict {
                            expected,
                            actual: task.rev,
                        },
                    ));
                }

                let is_operator = ctx.by().is_operator();
                let active_lease = task
                    .lease
                    .as_ref()
                    .filter(|l| task.status == TaskStatus::Claimed && l.expires_at > ctx.at());

                if let Some(req_epoch) = c.epoch {
                    let matches_active = active_lease.is_some_and(|l| {
                        l.epoch == req_epoch && (is_operator || l.holder == *ctx.by())
                    });
                    if !matches_active {
                        let (actual, holder) = match &task.lease {
                            Some(l) => (l.epoch, Some(l.holder.clone())),
                            None => (task.last_epoch, None),
                        };
                        return Err(ApiError::conflict_with(
                            format!(
                                "task seq {} lease fenced at epoch {} (current epoch {})",
                                c.task.0, req_epoch.0, actual.0
                            ),
                            ConflictData::LeaseFenced {
                                expected: req_epoch,
                                actual,
                                holder,
                            },
                        ));
                    }
                } else if !is_operator && let Some(lease) = active_lease {
                    return Err(ApiError::conflict_with(
                        format!(
                            "task seq {} is actively claimed by {} at epoch {}; matching epoch required",
                            c.task.0, lease.holder, lease.epoch.0
                        ),
                        ConflictData::LeaseFenced {
                            expected: LeaseEpoch(0),
                            actual: lease.epoch,
                            holder: Some(lease.holder.clone()),
                        },
                    ));
                }

                if task.status == c.status && c.note.is_none() {
                    return Ok(vec![]);
                }

                let mut events = vec![RoomEvent::TaskTransitioned {
                    task: c.task,
                    rev: task.rev + 1,
                    status: c.status,
                }];
                if let Some(note) = &c.note {
                    let mentions = resolve_mentions_for_room(&id, ctx.by(), note, &state.members);
                    events.push(RoomEvent::Posted {
                        body: note.clone(),
                        mentions,
                        re: Some(c.task),
                        after: None,
                        labels: Vec::new(),
                        task: None,
                        idem: None,
                        origin: PostOrigin::Direct,
                    });
                }
                Ok(events)
            }

            RoomCommand::TaskUpdate(c) => {
                let id = c.room.resolve(ctx.by())?;
                ensure_existing_room_write_access(state, &id, ctx.by(), "update task")?;
                if c.due_ms.is_some() && c.clear_due {
                    return Err(ApiError::Invalid(InvalidId {
                        what: "due_ms".into(),
                        reason: "cannot set both due_ms and clear_due".into(),
                    }));
                }
                let Some(task) = state.tasks.get(&c.task) else {
                    return Err(ApiError::NotFound {
                        what: format!("task seq {} in room", c.task.0),
                    });
                };
                if let Some(expected) = c.expected_rev
                    && expected != task.rev
                {
                    return Err(ApiError::conflict_with(
                        format!(
                            "task seq {} is at rev {}, expected {expected}",
                            c.task.0, task.rev
                        ),
                        ConflictData::RevisionConflict {
                            expected,
                            actual: task.rev,
                        },
                    ));
                }

                for &dep in &c.needs_add {
                    if dep == c.task {
                        return Err(ApiError::Invalid(InvalidId {
                            what: "needs_add".into(),
                            reason: format!("task seq {} cannot depend on itself", c.task.0),
                        }));
                    }
                    if !state.tasks.contains_key(&dep) {
                        if state.posts.contains_key(&dep) {
                            return Err(ApiError::Invalid(InvalidId {
                                what: "needs_add".into(),
                                reason: format!(
                                    "seq {} is a message, not a task root in this room",
                                    dep.0
                                ),
                            }));
                        }
                        return Err(ApiError::NotFound {
                            what: format!("task seq {} in room", dep.0),
                        });
                    }
                }

                let mut new_needs = task.needs.clone();
                if !c.needs_remove.is_empty() {
                    new_needs.retain(|d| !c.needs_remove.contains(d));
                }
                for &dep in &c.needs_add {
                    if !new_needs.contains(&dep) {
                        new_needs.push(dep);
                    }
                }

                if state.creates_cycle(c.task, &new_needs) {
                    return Err(ApiError::Invalid(InvalidId {
                        what: "needs".into(),
                        reason: format!(
                            "dependency cycle detected involving task seq {}",
                            c.task.0
                        ),
                    }));
                }

                let eff_title = c.title.as_ref().filter(|t| *t != &task.title).cloned();
                let eff_priority = c.priority.filter(|p| *p != task.priority);
                let eff_clear_due = c.clear_due && task.due_ms.is_some();
                let eff_due_ms = c.due_ms.filter(|d| Some(*d) != task.due_ms);
                let eff_needs_remove: Vec<Seq> = task
                    .needs
                    .iter()
                    .copied()
                    .filter(|d| !new_needs.contains(d))
                    .collect();
                let eff_needs_add: Vec<Seq> = new_needs
                    .iter()
                    .copied()
                    .filter(|d| !task.needs.contains(d))
                    .collect();

                if eff_title.is_none()
                    && eff_priority.is_none()
                    && !eff_clear_due
                    && eff_due_ms.is_none()
                    && eff_needs_add.is_empty()
                    && eff_needs_remove.is_empty()
                {
                    return Ok(vec![]);
                }

                Ok(vec![RoomEvent::TaskUpdated {
                    task: c.task,
                    rev: task.rev + 1,
                    title: eff_title,
                    needs_add: eff_needs_add,
                    needs_remove: eff_needs_remove,
                    priority: eff_priority,
                    due_ms: eff_due_ms,
                    clear_due: eff_clear_due,
                }])
            }

            RoomCommand::RoomTasks(c) => {
                let id = c.room.resolve(ctx.by())?;
                if let RoomId::Direct { pair } = &id {
                    ensure_dm_access(pair, ctx.by())?;
                }
                if state.opened.is_none() {
                    return Err(ApiError::NotFound {
                        what: format!("room {id:?}"),
                    });
                }
                Ok(vec![])
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
            RoomEvent::Posted {
                body,
                re,
                labels,
                task,
                ..
            } => {
                let root = re.and_then(|parent| {
                    state
                        .posts
                        .get(&parent)
                        .map(|p| p.re.unwrap_or(p.seq))
                        .or(Some(parent))
                });
                state.posts.insert(
                    meta.seq,
                    PostRecord {
                        seq: meta.seq,
                        at: meta.at,
                        by: meta.by.clone(),
                        body: body.clone(),
                        re: root,
                        labels: labels.iter().cloned().collect(),
                    },
                );
                state.last_post_seq = Some(meta.seq);
                match root {
                    Some(thread_root) => {
                        state.thread_tails.insert(thread_root, meta.seq);
                    }
                    None => {
                        state.thread_tails.insert(meta.seq, meta.seq);
                    }
                }
                if let Some(spec) = task {
                    let org_id = spec.org_id.clone().unwrap_or_else(|| {
                        OrgId::mint_deterministic(&format!("room-task:{}", meta.seq.0))
                    });
                    state.org_ids.insert(org_id.clone(), meta.seq);
                    state.tasks.insert(
                        meta.seq,
                        TaskRecord {
                            seq: meta.seq,
                            org_id,
                            rev: 1,
                            title: spec.title.clone(),
                            status: TaskStatus::Open,
                            priority: spec.priority.unwrap_or(Priority::Normal),
                            due_ms: spec.due_ms,
                            needs: spec.needs.clone(),
                            lease: None,
                            last_epoch: LeaseEpoch(0),
                            created_by: meta.by.clone(),
                            created_at: meta.at,
                            updated_at: meta.at,
                        },
                    );
                }
            }
            RoomEvent::TopicSet { topic } => state.topic = Some(topic.clone()),
            RoomEvent::Labeled { target, after, .. } => {
                if let Some(post) = state.posts.get_mut(target) {
                    post.labels = after.iter().cloned().collect();
                }
                if let Some(task) = state.tasks.get_mut(target) {
                    task.updated_at = meta.at;
                }
            }
            RoomEvent::TaskClaimed { task, rev, lease } => {
                if let Some(rec) = state.tasks.get_mut(task) {
                    rec.status = TaskStatus::Claimed;
                    rec.rev = *rev;
                    rec.last_epoch = rec.last_epoch.max(lease.epoch);
                    rec.lease = Some(lease.clone());
                    rec.updated_at = meta.at;
                }
            }
            RoomEvent::TaskHeartbeat {
                task,
                epoch,
                expires_at,
            } => {
                if let Some(rec) = state.tasks.get_mut(task)
                    && let Some(lease) = &mut rec.lease
                    && lease.epoch == *epoch
                {
                    lease.expires_at = *expires_at;
                    rec.updated_at = meta.at;
                }
            }
            RoomEvent::TaskTransitioned { task, rev, status } => {
                if let Some(rec) = state.tasks.get_mut(task) {
                    rec.status = *status;
                    rec.rev = *rev;
                    if *status != TaskStatus::Claimed {
                        rec.lease = None;
                    }
                    rec.updated_at = meta.at;
                }
            }
            RoomEvent::TaskUpdated {
                task,
                rev,
                title,
                needs_add,
                needs_remove,
                priority,
                due_ms,
                clear_due,
            } => {
                if let Some(rec) = state.tasks.get_mut(task) {
                    rec.rev = *rev;
                    if let Some(t) = title {
                        rec.title = t.clone();
                    }
                    if !needs_remove.is_empty() {
                        rec.needs.retain(|d| !needs_remove.contains(d));
                    }
                    for dep in needs_add {
                        if !rec.needs.contains(dep) {
                            rec.needs.push(*dep);
                        }
                    }
                    if let Some(p) = priority {
                        rec.priority = *p;
                    }
                    if *clear_due {
                        rec.due_ms = None;
                    } else if let Some(due) = due_ms {
                        rec.due_ms = Some(*due);
                    }
                    rec.updated_at = meta.at;
                }
            }
        }
    }

    fn reply(state: &RoomState, cmd: RoomCommand, committed: &[Envelope]) -> WireReply {
        match cmd {
            RoomCommand::RoomEnsure(_) => {
                let (id, origin) = state
                    .opened
                    .clone()
                    .expect("RoomEnsure guarantees opened state");
                WireReply::RoomEnsure(RoomInfo {
                    id,
                    origin,
                    topic: state.topic.clone(),
                    head: state.head,
                    members: state
                        .members
                        .iter()
                        .map(|(h, p)| (h.clone(), p.clone()))
                        .collect(),
                })
            }
            RoomCommand::RoomPost(_) => {
                let env = committed
                    .iter()
                    .rev()
                    .find(|e| matches!(e.event, Event::Room(RoomEvent::Posted { .. })))
                    .expect("RoomPost commits or replays a Posted event");
                let StreamId::Room { room } = &env.stream else {
                    unreachable!()
                };
                let Event::Room(RoomEvent::Posted {
                    mentions, re, task, ..
                }) = &env.event
                else {
                    unreachable!()
                };
                WireReply::RoomPost(Posted {
                    room: room.clone(),
                    seq: env.seq,
                    mentions: mentions.clone(),
                    re: *re,
                    org_id: task.as_ref().and_then(|t| t.org_id.clone()),
                    delivery_notice: None,
                })
            }
            RoomCommand::RoomJoin(_) => WireReply::RoomJoin(Ack),
            RoomCommand::RoomLeave(_) => WireReply::RoomLeave(Ack),
            RoomCommand::RoomLabel(c) => {
                if let Some(env) = committed
                    .iter()
                    .rev()
                    .find(|e| matches!(e.event, Event::Room(RoomEvent::Labeled { .. })))
                {
                    let StreamId::Room { room } = &env.stream else {
                        unreachable!()
                    };
                    let Event::Room(RoomEvent::Labeled { target, after, .. }) = &env.event else {
                        unreachable!()
                    };
                    return WireReply::RoomLabel(Labeled {
                        room: room.clone(),
                        seq: *target,
                        labels: after.clone(),
                    });
                }
                let (room, _) = state
                    .opened
                    .as_ref()
                    .expect("RoomLabel verified room is open");
                let labels = state
                    .posts
                    .get(&c.seq)
                    .map(|p| p.labels.iter().cloned().collect())
                    .unwrap_or_default();
                WireReply::RoomLabel(Labeled {
                    room: room.clone(),
                    seq: c.seq,
                    labels,
                })
            }
            RoomCommand::TaskClaim(_) => {
                let env = committed
                    .iter()
                    .rev()
                    .find(|e| matches!(e.event, Event::Room(RoomEvent::TaskClaimed { .. })))
                    .expect("TaskClaim commits or replays a TaskClaimed event");
                let StreamId::Room { room } = &env.stream else {
                    unreachable!()
                };
                let Event::Room(RoomEvent::TaskClaimed { task, rev, lease }) = &env.event else {
                    unreachable!()
                };
                let org_id = state.tasks[task].org_id.clone();
                WireReply::TaskClaim(TaskLeaseReceipt {
                    room: room.clone(),
                    task: *task,
                    org_id,
                    rev: *rev,
                    epoch: lease.epoch,
                    expires_at: lease.expires_at,
                })
            }
            RoomCommand::TaskHeartbeat(_) => {
                let env = committed
                    .iter()
                    .rev()
                    .find(|e| matches!(e.event, Event::Room(RoomEvent::TaskHeartbeat { .. })))
                    .expect("TaskHeartbeat commits a TaskHeartbeat event");
                let StreamId::Room { room } = &env.stream else {
                    unreachable!()
                };
                let Event::Room(RoomEvent::TaskHeartbeat {
                    task,
                    epoch,
                    expires_at,
                }) = &env.event
                else {
                    unreachable!()
                };
                let rec = &state.tasks[task];
                WireReply::TaskHeartbeat(TaskLeaseReceipt {
                    room: room.clone(),
                    task: *task,
                    org_id: rec.org_id.clone(),
                    rev: rec.rev,
                    epoch: *epoch,
                    expires_at: *expires_at,
                })
            }
            RoomCommand::TaskTransition(c) => {
                let task_seq = committed
                    .iter()
                    .find_map(|e| match &e.event {
                        Event::Room(RoomEvent::TaskTransitioned { task, .. }) => Some(*task),
                        _ => None,
                    })
                    .unwrap_or(c.task);
                let at = committed
                    .last()
                    .map(|e| e.at)
                    .unwrap_or_else(rex_wave_runtime::now);
                WireReply::TaskTransition(
                    state
                        .task_view_at(task_seq, at)
                        .expect("TaskTransition verified task exists"),
                )
            }
            RoomCommand::TaskUpdate(c) => {
                let task_seq = committed
                    .iter()
                    .find_map(|e| match &e.event {
                        Event::Room(RoomEvent::TaskUpdated { task, .. }) => Some(*task),
                        _ => None,
                    })
                    .unwrap_or(c.task);
                let at = committed
                    .last()
                    .map(|e| e.at)
                    .unwrap_or_else(rex_wave_runtime::now);
                WireReply::TaskUpdate(
                    state
                        .task_view_at(task_seq, at)
                        .expect("TaskUpdate verified task exists"),
                )
            }
            RoomCommand::RoomTasks(c) => WireReply::RoomTasks(
                state
                    .task_list_page_at(c.filter.as_ref(), c.limit, rex_wave_runtime::now())
                    .expect("RoomTasks verified room is open"),
            ),
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
