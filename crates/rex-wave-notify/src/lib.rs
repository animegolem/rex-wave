//! # rex-wave-notify
//!
//! Per-principal [`Inbox`] decider, pure [`classify`] and [`rank`] notification
//! policy, and the [`Herald`] reactor.

use rex_wave_api::notify::*;
use rex_wave_api::*;
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta, LocalClient, Reactor};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

const DECAY_STEP_MS: u64 = 60_000;
const MAX_DECAY: u16 = 900;

fn snippet(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    let mut out = String::new();
    for (idx, ch) in trimmed.chars().enumerate() {
        if idx >= max_chars {
            out.push_str("...");
            break;
        }
        out.push(if ch == '\n' || ch == '\r' { ' ' } else { ch });
    }
    out
}

fn room_slug(room: &room::RoomId) -> String {
    match room {
        room::RoomId::Channel { scope: room::RoomScope::Global, name } => format!("global:{name}"),
        room::RoomId::Channel { scope: room::RoomScope::Project { project }, name } => format!("project:{project}:{name}"),
        room::RoomId::Direct { pair } => {
            let (a, b): (Principal, Principal) = pair.clone().into();
            format!("dm:{a}:{b}")
        }
    }
}

fn room_label(room: &room::RoomId) -> String {
    match room {
        room::RoomId::Channel { scope: room::RoomScope::Global, name } => format!("#{name}"),
        room::RoomId::Channel { scope: room::RoomScope::Project { project }, name } => format!("#{project}/{name}"),
        room::RoomId::Direct { pair } => {
            let (a, b): (Principal, Principal) = pair.clone().into();
            format!("DM({a},{b})")
        }
    }
}

fn mark_read_action(room: &room::RoomId, upto: Seq) -> NotifyAction {
    NotifyAction {
        label: "Mark read".into(),
        key: Some('r'),
        run: Box::new(WireCommand::InboxMarkRead(InboxMarkRead {
            room: room.clone(),
            upto,
        })),
    }
}

fn interaction_actions(
    wave: &WaveSlug,
    role: &RoleName,
    interaction: &wave::PendingInteraction,
) -> Vec<NotifyAction> {
    let deny = NotifyAction {
        label: "Deny".into(),
        key: Some('d'),
        run: Box::new(WireCommand::WaveResolve(wave::WaveResolve {
            wave: wave.clone(),
            role: role.clone(),
            interaction: interaction.id.clone(),
            resolution: wave::InteractionResolution::Deny,
        })),
    };
    match &interaction.kind {
        wave::InteractionKind::Permission => vec![
            NotifyAction {
                label: "Allow once".into(),
                key: Some('a'),
                run: Box::new(WireCommand::WaveResolve(wave::WaveResolve {
                    wave: wave.clone(),
                    role: role.clone(),
                    interaction: interaction.id.clone(),
                    resolution: wave::InteractionResolution::AllowOnce { granted_permissions: None },
                })),
            },
            NotifyAction {
                label: "Allow for session".into(),
                key: Some('s'),
                run: Box::new(WireCommand::WaveResolve(wave::WaveResolve {
                    wave: wave.clone(),
                    role: role.clone(),
                    interaction: interaction.id.clone(),
                    resolution: wave::InteractionResolution::AllowForSession { granted_permissions: None },
                })),
            },
            deny,
        ],
        wave::InteractionKind::Question { .. } | wave::InteractionKind::Extension => vec![deny],
    }
}

fn is_blocking_mail(item: &mail::MailboxItem) -> bool {
    matches!(item.kind.as_str(), "blocker" | "approval")
        || item.label.as_deref().is_some_and(|l| l.starts_with("[blocking]"))
}

pub fn rank(kind: &NotifyKind, age_ms: u64) -> Rank {
    let base: u16 = match kind {
        NotifyKind::Interaction { .. } => 9_000,
        NotifyKind::Mail { item } if is_blocking_mail(item) => 8_000,
        NotifyKind::LetterBanked { letter } if is_blocking_mail(&letter.item) => 8_000,
        NotifyKind::DirectMessage { .. } => 7_000,
        NotifyKind::Mention { .. } => 6_000,
        NotifyKind::WorkerStuck { .. } => 5_000,
        NotifyKind::LetterBanked { .. } | NotifyKind::Mail { .. } => 4_500,
        NotifyKind::Reminder { .. } => 4_000,
        NotifyKind::CardAssigned { .. } => 3_500,
        NotifyKind::WaveFinished { .. } => 3_000,
    };
    let decay = (age_ms / DECAY_STEP_MS).min(u64::from(MAX_DECAY)) as u16;
    Rank(base.saturating_sub(decay))
}

#[derive(Default)]
pub struct HeraldView {
    members: BTreeMap<room::RoomId, BTreeSet<Principal>>,
    wave_owners: BTreeMap<WaveSlug, Principal>,
    operators: BTreeSet<Principal>,
    operator_seats: BTreeSet<SeatName>,
    resident_seats: BTreeSet<SeatName>,
    blocked: BTreeMap<(WaveSlug, RoleName), (BbInteractionId, String)>,
    cleared_interactions: Vec<(WaveSlug, RoleName, BbInteractionId, String)>,
}

impl HeraldView {
    pub fn observe(&mut self, entry: &Envelope) {
        self.cleared_interactions.clear();
        self.record_principal(&entry.by);

        match (&entry.stream, &entry.event) {
            (StreamId::Room { room }, Event::Room(ev)) => {
                if let room::RoomId::Direct { pair } = room {
                    let (a, b): (Principal, Principal) = pair.clone().into();
                    self.record_principal(&a);
                    self.record_principal(&b);
                    let set = self.members.entry(room.clone()).or_default();
                    set.insert(a);
                    set.insert(b);
                }
                match ev {
                    room::RoomEvent::MemberJoined { member, .. } => {
                        self.record_principal(member);
                        self.members.entry(room.clone()).or_default().insert(member.clone());
                    }
                    room::RoomEvent::MemberLeft { member } => {
                        if let Some(set) = self.members.get_mut(room) {
                            set.remove(member);
                        }
                    }
                    room::RoomEvent::Opened { .. }
                    | room::RoomEvent::Posted { .. }
                    | room::RoomEvent::TopicSet { .. } => {}
                }
            }
            (StreamId::Wave { wave }, Event::Wave(ev)) => match ev {
                wave::WaveEvent::Started { .. } => {
                    if matches!(entry.by, Principal::Operator { .. }) {
                        self.wave_owners.insert(wave.clone(), entry.by.clone());
                    }
                }
                wave::WaveEvent::WorkerObserved { role, observation } => match observation {
                    wave::Observation::Activity {
                        activity: wave::Activity::Blocked { interaction },
                    } => {
                        let key = (wave.clone(), role.clone());
                        if let Some((old_id, old_summary)) = self.blocked.remove(&key)
                            && old_id != interaction.id
                        {
                            self.cleared_interactions
                                .push((wave.clone(), role.clone(), old_id, old_summary));
                        }
                        self.blocked.insert(key, (interaction.id.clone(), interaction.summary.clone()));
                    }
                    _ => {
                        if let Some((old_id, old_summary)) =
                            self.blocked.remove(&(wave.clone(), role.clone()))
                        {
                            self.cleared_interactions
                                .push((wave.clone(), role.clone(), old_id, old_summary));
                        }
                    }
                },
                wave::WaveEvent::InteractionResolved { role, interaction, .. } => {
                    let key = (wave.clone(), role.clone());
                    let summary = match self.blocked.get(&key) {
                        Some((id, s)) if id == interaction => {
                            let s = s.clone();
                            self.blocked.remove(&key);
                            s
                        }
                        _ => "Resolved".into(),
                    };
                    self.cleared_interactions
                        .push((wave.clone(), role.clone(), interaction.clone(), summary));
                }
                wave::WaveEvent::WorkerRetiring { role, .. } => {
                    if let Some((old_id, old_summary)) =
                        self.blocked.remove(&(wave.clone(), role.clone()))
                    {
                        self.cleared_interactions
                            .push((wave.clone(), role.clone(), old_id, old_summary));
                    }
                }
                wave::WaveEvent::Finished { .. } => {
                    let keys: Vec<(WaveSlug, RoleName)> = self
                        .blocked
                        .keys()
                        .filter(|(w, _)| w == wave)
                        .cloned()
                        .collect();
                    for key in keys {
                        if let Some((old_id, old_summary)) = self.blocked.remove(&key) {
                            self.cleared_interactions
                                .push((key.0, key.1, old_id, old_summary));
                        }
                    }
                }
                wave::WaveEvent::WorkerRequested { .. } | wave::WaveEvent::Told { .. } => {}
            },
            _ => {}
        }
    }

    fn record_principal(&mut self, p: &Principal) {
        match p {
            Principal::Operator { seat } => {
                self.operator_seats.insert(seat.clone());
                self.operators.insert(p.clone());
            }
            Principal::Seat { seat } => {
                self.resident_seats.insert(seat.clone());
            }
            Principal::Worker { .. } | Principal::System { .. } => {}
        }
    }

    fn members_of(&self, room: &room::RoomId) -> Option<&BTreeSet<Principal>> {
        self.members.get(room)
    }

    fn operators_for_wave(&self, wave: &WaveSlug) -> Vec<Principal> {
        if let Some(owner) = self.wave_owners.get(wave) {
            return vec![owner.clone()];
        }
        if !self.operators.is_empty() {
            return self.operators.iter().cloned().collect();
        }
        vec![Principal::Operator {
            seat: SeatName::parse("owner").expect("default operator seat is valid"),
        }]
    }

    fn principals_for_seat(&self, seat: &SeatName) -> Vec<Principal> {
        let mut out = Vec::new();
        if self.operator_seats.contains(seat) {
            out.push(Principal::Operator { seat: seat.clone() });
        }
        if self.resident_seats.contains(seat) {
            out.push(Principal::Seat { seat: seat.clone() });
        }
        if out.is_empty() {
            if seat.as_str() == "owner" {
                out.push(Principal::Operator { seat: seat.clone() });
            } else {
                out.push(Principal::Seat { seat: seat.clone() });
            }
        }
        out
    }

    fn principal_for_assignee(&self, raw: &str) -> Option<Principal> {
        let trimmed = raw.trim().trim_start_matches('@');
        if let Some((w, r)) = trimmed.split_once('/')
            && let (Ok(wave), Ok(role)) = (WaveSlug::parse(w), RoleName::parse(r))
        {
            return Some(Principal::Worker { wave, role });
        }
        let seat = SeatName::parse(trimmed).ok()?;
        self.principals_for_seat(&seat).into_iter().next()
    }
}

pub fn classify(entry: &Envelope, view: &HeraldView) -> Vec<(Principal, NotificationDraft)> {
    let mut out = Vec::new();

    for (wave, role, interaction_id, summary) in &view.cleared_interactions {
        for op in view.operators_for_wave(wave) {
            out.push((
                op,
                NotificationDraft {
                    kind: NotifyKind::Interaction {
                        wave: wave.clone(),
                        role: role.clone(),
                        interaction: interaction_id.clone(),
                    },
                    title: format!("@{wave}/{role} interaction resolved"),
                    preview: snippet(summary, 160),
                    actions: Vec::new(),
                    dedupe: format!("interaction:{wave}:{role}:{interaction_id}"),
                },
            ));
        }
    }

    match (&entry.stream, &entry.event) {
        (StreamId::Room { room }, Event::Room(room::RoomEvent::Posted { body, mentions, .. })) => match room {
            room::RoomId::Direct { pair } => {
                if !matches!(entry.by, Principal::System { part: SystemPart::WakeBell }) {
                    let (a, b): (Principal, Principal) = pair.clone().into();
                    for recipient in [a, b] {
                        if recipient != entry.by && !matches!(recipient, Principal::System { .. }) {
                            out.push((
                                recipient,
                                NotificationDraft {
                                    kind: NotifyKind::DirectMessage { room: room.clone(), seq: entry.seq },
                                    title: format!("Direct message from {}", entry.by),
                                    preview: snippet(body.as_str(), 160),
                                    actions: vec![mark_read_action(room, entry.seq)],
                                    dedupe: format!("dm:{}:{}", room_slug(room), entry.seq.0),
                                },
                            ));
                        }
                    }
                }
            }
            room::RoomId::Channel { .. } => {
                let mut recipients: BTreeSet<Principal> = mentions.principals.iter().cloned().collect();
                if mentions.all
                    && let Some(room_members) = view.members_of(room)
                {
                    recipients.extend(room_members.iter().cloned());
                }
                recipients.retain(|p| p != &entry.by && !matches!(p, Principal::System { .. }));
                for recipient in recipients {
                    out.push((
                        recipient,
                        NotificationDraft {
                            kind: NotifyKind::Mention { room: room.clone(), seq: entry.seq },
                            title: format!("Mention from {} in {}", entry.by, room_label(room)),
                            preview: snippet(body.as_str(), 160),
                            actions: vec![mark_read_action(room, entry.seq)],
                            dedupe: format!("mention:{}:{}", room_slug(room), entry.seq.0),
                        },
                    ));
                }
            }
        },

        (StreamId::Wave { wave }, Event::Wave(ev)) => match ev {
            wave::WaveEvent::WorkerObserved { role, observation } => match observation {
                wave::Observation::Activity { activity: wave::Activity::Blocked { interaction } } => {
                    let actions = interaction_actions(wave, role, interaction);
                    for op in view.operators_for_wave(wave) {
                        out.push((
                            op,
                            NotificationDraft {
                                kind: NotifyKind::Interaction {
                                    wave: wave.clone(),
                                    role: role.clone(),
                                    interaction: interaction.id.clone(),
                                },
                                title: format!("@{wave}/{role} waiting on interaction"),
                                preview: snippet(&interaction.summary, 160),
                                actions: actions.clone(),
                                dedupe: format!("interaction:{wave}:{role}:{}", interaction.id),
                            },
                        ));
                    }
                }
                wave::Observation::Activity { activity: wave::Activity::Errored { error } }
                | wave::Observation::SpawnFailed { error, .. } => {
                    for op in view.operators_for_wave(wave) {
                        out.push((
                            op,
                            NotificationDraft {
                                kind: NotifyKind::WorkerStuck {
                                    wave: wave.clone(),
                                    role: role.clone(),
                                    why: error.clone(),
                                },
                                title: format!("@{wave}/{role} stuck"),
                                preview: snippet(error, 160),
                                actions: Vec::new(),
                                dedupe: format!("worker-stuck:{wave}:{role}:{}", entry.seq.0),
                            },
                        ));
                    }
                }
                wave::Observation::Spawned { .. }
                | wave::Observation::Activity { .. }
                | wave::Observation::Archived => {}
            },
            wave::WaveEvent::Finished { outcome } => {
                let detail = match outcome {
                    wave::WaveOutcome::Completed { summary } => summary.as_str(),
                    wave::WaveOutcome::Cancelled { reason } | wave::WaveOutcome::Failed { reason } => reason.as_str(),
                };
                for op in view.operators_for_wave(wave) {
                    out.push((
                        op,
                        NotificationDraft {
                            kind: NotifyKind::WaveFinished { wave: wave.clone() },
                            title: format!("Wave {wave} finished"),
                            preview: snippet(detail, 160),
                            actions: Vec::new(),
                            dedupe: format!("wave-finished:{wave}"),
                        },
                    ));
                }
            }
            wave::WaveEvent::Started { .. }
            | wave::WaveEvent::WorkerRequested { .. }
            | wave::WaveEvent::Told { .. }
            | wave::WaveEvent::InteractionResolved { .. }
            | wave::WaveEvent::WorkerRetiring { .. } => {}
        },

        (StreamId::Board { board }, Event::Board(ev)) => match ev {
            board::BoardEvent::CardCreated { card } => {
                if let Some(assignee) = &card.assignee
                    && let Some(to) = view.principal_for_assignee(assignee.as_str())
                    && to != entry.by
                {
                    out.push((
                        to,
                        NotificationDraft {
                            kind: NotifyKind::CardAssigned {
                                card: board::CardRef {
                                    board: board.clone(),
                                    no: card.no,
                                    revision: card.revision,
                                },
                            },
                            title: format!("Card #{} assigned in {board}", card.no.0),
                            preview: snippet(card.title.as_str(), 160),
                            actions: Vec::new(),
                            dedupe: format!("card-assigned:{board}:{}:{}", card.no.0, card.revision.0),
                        },
                    ));
                }
            }
            board::BoardEvent::CardPatched { no, revision, patch } => {
                if let Some(board::Assign::To { who }) = &patch.assignee
                    && let Some(to) = view.principal_for_assignee(who.as_str())
                    && to != entry.by
                {
                    let preview = patch
                        .title
                        .as_ref()
                        .map_or_else(|| format!("Card #{}", no.0), |t| snippet(t.as_str(), 160));
                    out.push((
                        to,
                        NotificationDraft {
                            kind: NotifyKind::CardAssigned {
                                card: board::CardRef { board: board.clone(), no: *no, revision: *revision },
                            },
                            title: format!("Card #{} assigned in {board}", no.0),
                            preview,
                            actions: Vec::new(),
                            dedupe: format!("card-assigned:{board}:{}:{}", no.0, revision.0),
                        },
                    ));
                }
            }
            board::BoardEvent::Opened { .. } | board::BoardEvent::CardArchived { .. } => {}
        },

        (StreamId::Mail, Event::Mail(ev)) => match ev {
            mail::MailEvent::Banked { letter, .. } => {
                if let Ok(seat) = SeatName::parse(&letter.item.to) {
                    let ack_action = NotifyAction {
                        label: "Acknowledge letter".into(),
                        key: Some('k'),
                        run: Box::new(WireCommand::MailAck(mail::MailAck {
                            path: letter.path.clone(),
                            sha256: letter.sha256.clone(),
                        })),
                    };
                    let preview = letter
                        .item
                        .label
                        .as_deref()
                        .map_or_else(|| letter.path.display().to_string(), |l| snippet(l, 160));
                    for to in view.principals_for_seat(&seat) {
                        out.push((
                            to,
                            NotificationDraft {
                                kind: NotifyKind::LetterBanked { letter: letter.clone() },
                                title: format!("Letter banked from {}", letter.item.from),
                                preview: preview.clone(),
                                actions: vec![ack_action.clone()],
                                dedupe: format!("letter-banked:{}", letter.wake_id.0),
                            },
                        ));
                    }
                }
            }
            mail::MailEvent::Projected { item } => {
                if item.state != "acked"
                    && let Ok(seat) = SeatName::parse(&item.to)
                {
                    let ack_action = NotifyAction {
                        label: "Acknowledge letter".into(),
                        key: Some('k'),
                        run: Box::new(WireCommand::MailAck(mail::MailAck {
                            path: item.reference.path.clone(),
                            sha256: item.reference.sha256.clone(),
                        })),
                    };
                    let preview = item.label.as_deref().map_or_else(|| item.id.clone(), |l| snippet(l, 160));
                    for to in view.principals_for_seat(&seat) {
                        out.push((
                            to,
                            NotificationDraft {
                                kind: NotifyKind::Mail { item: item.clone() },
                                title: format!("Mail from {}", item.from),
                                preview: preview.clone(),
                                actions: vec![ack_action.clone()],
                                dedupe: format!("mail:{}:{}", item.id, item.reference.sha256),
                            },
                        ));
                    }
                }
            }
            mail::MailEvent::BellRang { bell_id, seat } => {
                for to in view.principals_for_seat(seat) {
                    out.push((
                        to,
                        NotificationDraft {
                            kind: NotifyKind::Reminder { bell_id: bell_id.clone() },
                            title: format!("Wake bell {bell_id}"),
                            preview: format!("Scheduled wake bell for @{seat}"),
                            actions: Vec::new(),
                            dedupe: format!("bell:{seat}:{bell_id}"),
                        },
                    ));
                }
            }
            mail::MailEvent::Acked { .. } => {}
        },

        _ => {}
    }

    out
}

fn kinds_equal(a: &NotifyKind, b: &NotifyKind) -> bool {
    match (a, b) {
        (NotifyKind::Mention { room: r1, seq: s1 }, NotifyKind::Mention { room: r2, seq: s2 }) => r1 == r2 && s1 == s2,
        (NotifyKind::DirectMessage { room: r1, seq: s1 }, NotifyKind::DirectMessage { room: r2, seq: s2 }) => r1 == r2 && s1 == s2,
        (
            NotifyKind::Interaction { wave: w1, role: r1, interaction: i1 },
            NotifyKind::Interaction { wave: w2, role: r2, interaction: i2 },
        ) => w1 == w2 && r1 == r2 && i1 == i2,
        (NotifyKind::WorkerStuck { wave: w1, role: r1, why: y1 }, NotifyKind::WorkerStuck { wave: w2, role: r2, why: y2 }) => {
            w1 == w2 && r1 == r2 && y1 == y2
        }
        (NotifyKind::WaveFinished { wave: w1 }, NotifyKind::WaveFinished { wave: w2 }) => w1 == w2,
        (NotifyKind::CardAssigned { card: c1 }, NotifyKind::CardAssigned { card: c2 }) => {
            c1.board == c2.board && c1.no == c2.no && c1.revision == c2.revision
        }
        (NotifyKind::Mail { item: m1 }, NotifyKind::Mail { item: m2 }) => m1 == m2,
        (NotifyKind::LetterBanked { letter: l1 }, NotifyKind::LetterBanked { letter: l2 }) => l1 == l2,
        (NotifyKind::Reminder { bell_id: b1 }, NotifyKind::Reminder { bell_id: b2 }) => b1 == b2,
        _ => false,
    }
}

fn actions_equal(a: &[NotifyAction], b: &[NotifyAction]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b.iter()).all(|(x, y)| {
            x.label == y.label && x.key == y.key && format!("{:?}", x.run) == format!("{:?}", y.run)
        })
}

fn drafts_equal(a: &NotificationDraft, b: &NotificationDraft) -> bool {
    a.dedupe == b.dedupe
        && a.title == b.title
        && a.preview == b.preview
        && kinds_equal(&a.kind, &b.kind)
        && actions_equal(&a.actions, &b.actions)
}

pub struct Inbox;

#[derive(Default)]
pub struct InboxState {
    items: BTreeMap<NotificationId, Notification>,
    by_dedupe: BTreeMap<String, NotificationId>,
    cursors: BTreeMap<room::RoomId, Seq>,
    next_id: u64,
    latest_at: UnixMs,
    head: Seq,
}

impl Decider for Inbox {
    type State = InboxState;
    type Cmd = InboxCommand;
    type Event = InboxEvent;

    fn address(cmd: &InboxCommand, ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        let owner = match cmd {
            InboxCommand::InboxRaise(c) => c.to.clone(),
            _ => ctx.by.clone(),
        };
        Ok(StreamId::Inbox { owner })
    }

    fn idem(_cmd: &InboxCommand) -> Option<IdemKey> {
        None
    }

    fn decide(state: &InboxState, cmd: &InboxCommand, _ctx: &DecideCtx) -> Result<Vec<InboxEvent>, ApiError> {
        match cmd {
            InboxCommand::InboxRaise(c) => {
                let is_resolved_interaction = matches!(c.draft.kind, NotifyKind::Interaction { .. })
                    && c.draft.actions.is_empty();
                let fresh_rank = rank(&c.draft.kind, 0);
                if let Some(&existing_id) = state.by_dedupe.get(&c.draft.dedupe)
                    && let Some(existing) = state.items.get(&existing_id)
                {
                    let mut events = Vec::new();
                    if !drafts_equal(&existing.draft, &c.draft) {
                        events.push(InboxEvent::Updated {
                            id: existing_id,
                            rank: fresh_rank,
                            draft: c.draft.clone(),
                        });
                    }
                    if is_resolved_interaction && existing.state != NotifyState::Done {
                        events.push(InboxEvent::StateSet {
                            ids: vec![existing_id],
                            state: NotifyState::Done,
                        });
                    }
                    return Ok(events);
                }
                if is_resolved_interaction {
                    return Ok(vec![]);
                }
                let id = NotificationId(state.next_id + 1);
                Ok(vec![InboxEvent::Raised { id, rank: fresh_rank, draft: c.draft.clone() }])
            }

            InboxCommand::InboxAck(c) => {
                let mut seen = BTreeSet::new();
                let mut changed = Vec::new();
                for id in &c.ids {
                    if seen.insert(*id)
                        && let Some(existing) = state.items.get(id)
                        && existing.state != c.state
                    {
                        changed.push(*id);
                    }
                }
                if changed.is_empty() {
                    Ok(vec![])
                } else {
                    Ok(vec![InboxEvent::StateSet { ids: changed, state: c.state }])
                }
            }

            InboxCommand::InboxMarkRead(c) => {
                let mut events = Vec::new();
                let current = state.cursors.get(&c.room).copied().unwrap_or(Seq::ORIGIN);
                if c.upto > current {
                    events.push(InboxEvent::CursorAdvanced { room: c.room.clone(), upto: c.upto });
                }
                let seen_ids: Vec<NotificationId> = state
                    .items
                    .values()
                    .filter(|n| {
                        n.state == NotifyState::Unread
                            && matches!(
                                &n.draft.kind,
                                NotifyKind::Mention { room, seq }
                                    | NotifyKind::DirectMessage { room, seq }
                                    if room == &c.room && *seq <= c.upto
                            )
                    })
                    .map(|n| n.id)
                    .collect();
                if !seen_ids.is_empty() {
                    events.push(InboxEvent::StateSet {
                        ids: seen_ids,
                        state: NotifyState::Seen,
                    });
                }
                Ok(events)
            }

            InboxCommand::InboxList(_) => Ok(vec![]),
        }
    }

    fn evolve(state: &mut InboxState, event: &InboxEvent, meta: &EntryMeta) {
        state.head = meta.seq;
        if meta.at > state.latest_at {
            state.latest_at = meta.at;
        }
        match event {
            InboxEvent::Raised { id, rank, draft } => {
                state.next_id = state.next_id.max(id.0);
                state.by_dedupe.insert(draft.dedupe.clone(), *id);
                state.items.insert(
                    *id,
                    Notification {
                        id: *id,
                        at: meta.at,
                        rank: *rank,
                        state: NotifyState::Unread,
                        draft: draft.clone(),
                    },
                );
            }
            InboxEvent::Updated { id, rank, draft } => {
                let (prev_state, prev_at) = state
                    .items
                    .get(id)
                    .map_or((NotifyState::Unread, meta.at), |n| (n.state, n.at));
                if let Some(old) = state.items.get(id)
                    && old.draft.dedupe != draft.dedupe
                {
                    state.by_dedupe.remove(&old.draft.dedupe);
                }
                state.by_dedupe.insert(draft.dedupe.clone(), *id);
                state.items.insert(
                    *id,
                    Notification {
                        id: *id,
                        at: prev_at,
                        rank: *rank,
                        state: prev_state,
                        draft: draft.clone(),
                    },
                );
            }
            InboxEvent::StateSet { ids, state: next_state } => {
                for id in ids {
                    if let Some(item) = state.items.get_mut(id) {
                        item.state = *next_state;
                    }
                }
            }
            InboxEvent::CursorAdvanced { room, upto } => {
                let cur = state.cursors.entry(room.clone()).or_default();
                if *cur < *upto {
                    *cur = *upto;
                }
            }
        }
    }

    fn reply(state: &InboxState, cmd: InboxCommand, _committed: &[Envelope]) -> WireReply {
        match cmd {
            InboxCommand::InboxList(q) => {
                let unread = state.items.values().filter(|n| n.state == NotifyState::Unread).count() as u32;
                let limit = q.limit.unwrap_or(200).clamp(1, 1000) as usize;
                let mut items: Vec<Notification> = state
                    .items
                    .values()
                    .filter(|n| {
                        q.include_done || !matches!(n.state, NotifyState::Done | NotifyState::Dismissed)
                    })
                    .map(|n| {
                        let age_ms = state.latest_at.0.saturating_sub(n.at.0);
                        let mut projected = n.clone();
                        projected.rank = rank(&n.draft.kind, age_ms);
                        projected
                    })
                    .collect();
                items.sort_by(|a, b| (b.rank, b.at, b.id).cmp(&(a.rank, a.at, a.id)));
                items.truncate(limit);
                let cursors = state.cursors.iter().map(|(r, s)| (r.clone(), *s)).collect();
                WireReply::InboxList(InboxPage { head: state.head, items, unread, cursors })
            }
            InboxCommand::InboxAck(_) => WireReply::InboxAck(Ack),
            InboxCommand::InboxMarkRead(_) => WireReply::InboxMarkRead(Ack),
            InboxCommand::InboxRaise(_) => WireReply::InboxRaise(Ack),
        }
    }

    fn wrap(event: InboxEvent) -> Event {
        Event::Inbox(event)
    }

    fn unwrap(event: &Event) -> Option<&InboxEvent> {
        match event {
            Event::Inbox(e) => Some(e),
            _ => None,
        }
    }
}

pub struct Herald {
    view: HeraldView,
}

impl Herald {
    pub fn new() -> Self {
        Herald { view: HeraldView::default() }
    }
}

impl Default for Herald {
    fn default() -> Self {
        Self::new()
    }
}

impl Reactor for Herald {
    const NAME: &'static str = "herald";

    fn interest(&self) -> Interest {
        Interest::Everything
    }

    fn react(&mut self, frame: Frame, client: &LocalClient) -> impl Future<Output = Result<(), ApiError>> + Send {
        let raises = match frame {
            Frame::Committed(env) => {
                self.view.observe(&env);
                classify(&env, &self.view)
            }
            _ => Vec::new(),
        };
        let client = client.clone();
        async move {
            for (to, draft) in raises {
                client.call(InboxRaise { to, draft }).await?;
            }
            Ok(())
        }
    }
}
