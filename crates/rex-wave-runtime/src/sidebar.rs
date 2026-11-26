use crate::hub::Hub;
use rex_wave_api::board::{Assign, BoardEvent, BoardId, CardNo, LaneName, assignee};
use rex_wave_api::notify::{InboxEvent, NotificationId, NotifyKind, NotifyState};
use rex_wave_api::read::*;
use rex_wave_api::room::{Mentions, RoomEvent, RoomId, RoomOrigin, RoomScope};
use rex_wave_api::stage::derive_osc7501_status;
use rex_wave_api::wave::{AgentSpec, UnsettledFailure, WaveEvent, WaveOutcome, WorkerPhase};
use rex_wave_api::{
    BbProjectId, Cursor, Envelope, Event, Principal, RoleName, Seq, StreamId, WaveSlug,
};
use rex_wave_store::Journal;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::{
    broadcast::error::{RecvError, TryRecvError},
    watch,
};

const REPLAY_PAGE: usize = 512;
const MAX_COALESCED_BURST: usize = 256;

/// Derived, rebuildable projection of the journal for `Sidebar` and
/// `Interest::Sidebar`.
///
/// One task owns the mutable world and publishes `Arc<World>` snapshots onto a
/// `watch` channel. Readers borrow the latest `Arc` without locking, and
/// `Interest::Sidebar` watches the channel directly so bursts of journal
/// commits coalesce into one sidebar frame.
pub struct SidebarProjection {
    world: watch::Receiver<Arc<World>>,
}

impl SidebarProjection {
    /// Tap the hub first, replay every stream in `journal`, then spawn the
    /// live fold task. Anything committed during startup is either in the
    /// replay or in `tap`; `World::apply` ignores seqs it has already folded.
    pub fn spawn(journal: Journal, hub: Hub) -> Arc<Self> {
        let mut tap = hub.tap();
        let mut initial = World::default();
        catch_up(&journal, &mut initial);
        let mut current = Arc::new(initial);
        let (tx, world_rx) = watch::channel(current.clone());
        tokio::spawn(async move {
            loop {
                match tap.recv().await {
                    Ok(env) => {
                        let world = Arc::make_mut(&mut current);
                        let mut dirty = world.apply(&env);
                        let mut drained = 0usize;
                        while drained < MAX_COALESCED_BURST {
                            match tap.try_recv() {
                                Ok(next_env) => {
                                    dirty |= world.apply(&next_env);
                                    drained += 1;
                                }
                                Err(TryRecvError::Lagged(_)) => {
                                    dirty |= catch_up(&journal, world);
                                    break;
                                }
                                Err(TryRecvError::Empty | TryRecvError::Closed) => break,
                            }
                        }
                        if dirty && tx.send(current.clone()).is_err() {
                            break;
                        }
                    }
                    Err(RecvError::Lagged(_)) => {
                        if catch_up(&journal, Arc::make_mut(&mut current))
                            && tx.send(current.clone()).is_err()
                        {
                            break;
                        }
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });
        Arc::new(SidebarProjection { world: world_rx })
    }

    pub fn snapshot(&self, viewer: &Principal) -> SidebarTreeSnapshot {
        self.world.borrow().snapshot(viewer, &BTreeMap::new())
    }

    pub fn snapshot_with_names(
        &self,
        viewer: &Principal,
        project_names: &BTreeMap<BbProjectId, String>,
    ) -> SidebarTreeSnapshot {
        self.world.borrow().snapshot(viewer, project_names)
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<Arc<World>> {
        self.world.clone()
    }

    pub(crate) fn is_member(&self, room: &RoomId, who: &Principal) -> bool {
        self.world.borrow().is_member(room, who)
    }

    pub fn is_member_caught_up(&self, journal: &Journal, room: &RoomId, who: &Principal) -> bool {
        let stream = StreamId::Room { room: room.clone() };
        {
            let world = self.world.borrow();
            if let Ok(head) = journal.head(&stream)
                && world.cursor.at(&stream) >= head
            {
                return world.is_member(room, who);
            }
        }
        is_room_member_in_journal(journal, room, who)
    }
}

pub fn is_room_member_in_journal(journal: &Journal, room: &RoomId, who: &Principal) -> bool {
    if let RoomId::Direct { pair } = room {
        return pair.other_for_viewer(who).is_some();
    }
    let stream = StreamId::Room { room: room.clone() };
    let mut after = Seq::ORIGIN;
    let mut member = false;
    while let Ok(page) = journal.read(&stream, after, REPLAY_PAGE) {
        if page.is_empty() {
            break;
        }
        for env in &page {
            after = env.seq;
            match &env.event {
                Event::Room(RoomEvent::MemberJoined { member: m, .. }) if m == who => {
                    member = true;
                }
                Event::Room(RoomEvent::MemberLeft { member: m }) if m == who => {
                    member = false;
                }
                _ => {}
            }
        }
    }
    member
}

fn catch_up(journal: &Journal, world: &mut World) -> bool {
    let Ok(streams) = journal.streams() else {
        return false;
    };
    let mut changed = false;
    for (stream, head) in streams {
        while world.cursor.at(&stream) < head {
            let after = world.cursor.at(&stream);
            let Ok(page) = journal.read(&stream, after, REPLAY_PAGE) else {
                break;
            };
            if page.is_empty() {
                break;
            }
            for e in &page {
                changed |= world.apply(e);
            }
        }
    }
    changed
}

#[derive(Clone, Default)]
pub(crate) struct World {
    rooms: BTreeMap<RoomId, Arc<RoomEntry>>,
    waves: BTreeMap<WaveSlug, Arc<WaveEntry>>,
    boards: BTreeMap<BoardId, Arc<BoardEntry>>,
    inboxes: BTreeMap<Principal, Arc<InboxEntry>>,
    archived_projects: BTreeSet<BbProjectId>,
    archived_waves: BTreeSet<WaveSlug>,
    archived_rooms: BTreeSet<RoomId>,
    mail_failed_deliveries: u32,
    courier_unknown_streak: BTreeMap<String, u32>,
    degraded_couriers: BTreeSet<String>,
    cursor: Cursor,
}

#[derive(Clone, Default)]
struct RoomEntry {
    origin: Option<RoomOrigin>,
    members: BTreeSet<Principal>,
    posts: Arc<Vec<PostMeta>>,
}

#[derive(Clone)]
struct PostMeta {
    seq: Seq,
    by: Principal,
    mentions: Mentions,
}

#[derive(Clone)]
struct WaveEntry {
    project: BbProjectId,
    room: RoomId,
    finished: Option<WaveOutcome>,
    workers: BTreeMap<RoleName, (AgentSpec, WorkerPhase, UnsettledFailure)>,
}

#[derive(Clone, Default)]
struct BoardEntry {
    done_lanes: BTreeSet<LaneName>,
    cards: BTreeMap<CardNo, (LaneName, Option<String>)>,
}

#[derive(Clone, Default)]
struct InboxEntry {
    items: BTreeMap<NotificationId, (NotifyState, bool)>,
    cursors: BTreeMap<RoomId, Seq>,
}

impl World {
    fn apply(&mut self, env: &Envelope) -> bool {
        if env.seq <= self.cursor.at(&env.stream) {
            return false;
        }
        self.cursor.advance(&env.stream, env.seq);
        match (&env.stream, &env.event) {
            (StreamId::Room { room }, Event::Room(ev)) => {
                let r = Arc::make_mut(self.rooms.entry(room.clone()).or_default());
                if let RoomId::Direct { pair } = room {
                    let (a, b): (Principal, Principal) = pair.clone().into();
                    r.members.insert(a);
                    r.members.insert(b);
                }
                match ev {
                    RoomEvent::Opened { origin, .. } => r.origin = Some(origin.clone()),
                    RoomEvent::MemberJoined { member, .. } => {
                        r.members.insert(member.clone());
                    }
                    RoomEvent::MemberLeft { member } => {
                        r.members.remove(member);
                    }
                    RoomEvent::Posted { mentions, .. } => {
                        Arc::make_mut(&mut r.posts).push(PostMeta {
                            seq: env.seq,
                            by: env.by.clone(),
                            mentions: mentions.clone(),
                        });
                    }
                    RoomEvent::TopicSet { .. }
                    | RoomEvent::Labeled { .. }
                    | RoomEvent::TaskClaimed { .. }
                    | RoomEvent::TaskHeartbeat { .. }
                    | RoomEvent::TaskTransitioned { .. }
                    | RoomEvent::TaskUpdated { .. } => {}
                }
            }
            (StreamId::Wave { wave }, Event::Wave(ev)) => match ev {
                WaveEvent::Started { spec, room, .. } => {
                    self.waves.insert(
                        wave.clone(),
                        Arc::new(WaveEntry {
                            project: spec.project.clone(),
                            room: room.clone(),
                            finished: None,
                            workers: BTreeMap::new(),
                        }),
                    );
                }
                WaveEvent::WorkerRequested {
                    role,
                    agent,
                    attempt,
                    ..
                } => {
                    if let Some(w) = self.waves.get_mut(wave) {
                        Arc::make_mut(w).workers.insert(
                            role.clone(),
                            (
                                agent.clone(),
                                WorkerPhase::Requested { attempt: *attempt },
                                UnsettledFailure::for_attempt(*attempt),
                            ),
                        );
                    }
                }
                WaveEvent::WorkerObserved { role, observation } => {
                    if let Some(w) = self.waves.get_mut(wave)
                        && let Some((_, phase, unsettled)) = Arc::make_mut(w).workers.get_mut(role)
                    {
                        *phase = unsettled.observe(phase, observation);
                    }
                }
                WaveEvent::WorkerRetiring { role, .. } => {
                    if let Some(w) = self.waves.get_mut(wave)
                        && let Some((_, phase, unsettled)) = Arc::make_mut(w).workers.get_mut(role)
                    {
                        *phase = unsettled.retiring(phase);
                    }
                }
                WaveEvent::Finished { outcome } => {
                    if let Some(w) = self.waves.get_mut(wave) {
                        let w = Arc::make_mut(w);
                        w.finished = Some(outcome.clone());
                        for (_, phase, unsettled) in w.workers.values_mut() {
                            *phase = unsettled.retiring(phase);
                        }
                    }
                }
                WaveEvent::Told { .. } | WaveEvent::InteractionResolved { .. } => {}
            },
            (StreamId::Board { board }, Event::Board(ev)) => {
                let b = Arc::make_mut(self.boards.entry(board.clone()).or_default());
                match ev {
                    BoardEvent::Opened { lanes, .. } => {
                        for l in lanes {
                            if l.as_str() == "done" {
                                b.done_lanes.insert(l.clone());
                            }
                        }
                    }
                    BoardEvent::CardCreated { card } => {
                        if card.lane.as_str() == "done" {
                            b.done_lanes.insert(card.lane.clone());
                        }
                        b.cards.insert(
                            card.no,
                            (
                                card.lane.clone(),
                                card.assignee.as_ref().map(|a| a.as_str().to_owned()),
                            ),
                        );
                    }
                    BoardEvent::CardPatched { no, patch, .. } => {
                        if let Some((lane, who)) = b.cards.get_mut(no) {
                            if let Some(l) = &patch.lane {
                                if l.as_str() == "done" {
                                    b.done_lanes.insert(l.clone());
                                }
                                *lane = l.clone();
                            }
                            if let Some(a) = &patch.assignee {
                                *who = match a {
                                    Assign::To { who } => Some(who.as_str().to_owned()),
                                    Assign::Clear => None,
                                };
                            }
                        }
                    }
                    BoardEvent::CardArchived { no, .. } => {
                        b.cards.remove(no);
                    }
                }
            }
            (StreamId::Inbox { owner }, Event::Inbox(ev)) => {
                let inbox = Arc::make_mut(self.inboxes.entry(owner.clone()).or_default());
                match ev {
                    InboxEvent::Raised { id, draft, .. } => {
                        inbox.items.insert(
                            *id,
                            (
                                NotifyState::Unread,
                                matches!(draft.kind, NotifyKind::Interaction { .. }),
                            ),
                        );
                    }
                    InboxEvent::Updated { id, draft, .. } => {
                        let state = inbox.items.get(id).map_or(NotifyState::Unread, |(s, _)| *s);
                        inbox.items.insert(
                            *id,
                            (state, matches!(draft.kind, NotifyKind::Interaction { .. })),
                        );
                    }
                    InboxEvent::StateSet { ids, state } => {
                        for id in ids {
                            if let Some((s, _)) = inbox.items.get_mut(id) {
                                *s = *state;
                            }
                        }
                    }
                    InboxEvent::CursorAdvanced { room, upto } => {
                        let cur = inbox.cursors.entry(room.clone()).or_default();
                        if *cur < *upto {
                            *cur = *upto;
                        }
                    }
                    InboxEvent::ArchivedSet { target, archived } => match target {
                        ArchiveTarget::Project { project } => {
                            if *archived {
                                self.archived_projects.insert(project.clone());
                            } else {
                                self.archived_projects.remove(project);
                            }
                        }
                        ArchiveTarget::Wave { wave } => {
                            if *archived {
                                self.archived_waves.insert(wave.clone());
                            } else {
                                self.archived_waves.remove(wave);
                            }
                        }
                        ArchiveTarget::Room { room } => {
                            if *archived {
                                self.archived_rooms.insert(room.clone());
                            } else {
                                self.archived_rooms.remove(room);
                            }
                        }
                    },
                }
            }
            (StreamId::Mail, Event::Mail(ev)) => match ev {
                rex_wave_api::mail::MailEvent::RingAttempted {
                    courier, outcome, ..
                } => match outcome {
                    rex_wave_api::mail::CourierOutcome::Rung => {
                        self.courier_unknown_streak.insert(courier.clone(), 0);
                        self.degraded_couriers.remove(courier);
                    }
                    rex_wave_api::mail::CourierOutcome::Unknown => {
                        let streak = self
                            .courier_unknown_streak
                            .entry(courier.clone())
                            .or_insert(0);
                        *streak = streak.saturating_add(1);
                        if *streak >= 3 {
                            self.degraded_couriers.insert(courier.clone());
                        }
                    }
                    rex_wave_api::mail::CourierOutcome::Deferred
                    | rex_wave_api::mail::CourierOutcome::Refused => {}
                },
                rex_wave_api::mail::MailEvent::DeliveryFailed { kind, courier, .. } => {
                    self.mail_failed_deliveries = self.mail_failed_deliveries.saturating_add(1);
                    if *kind == rex_wave_api::mail::DeliveryFailureKind::AdapterDegraded {
                        self.degraded_couriers.insert(courier.clone());
                    }
                }
                _ => {}
            },
            _ => {}
        }
        true
    }

    fn is_member(&self, room: &RoomId, who: &Principal) -> bool {
        match room {
            RoomId::Direct { pair } => pair.other_for_viewer(who).is_some(),
            RoomId::Channel { .. } => self
                .rooms
                .get(room)
                .is_some_and(|r| r.members.contains(who)),
        }
    }

    fn unread_for(&self, room: &RoomId, viewer: &Principal, upto: Seq) -> (u32, u32) {
        let Some(r) = self.rooms.get(room) else {
            return (0, 0);
        };
        let in_room = self.is_member(room, viewer);
        let mut unread = 0u32;
        let mut mentions = 0u32;
        for p in r.posts.iter() {
            if p.seq > upto && !viewer.same_actor(&p.by) {
                unread += 1;
                let system_to_operator =
                    matches!(p.by, Principal::System { .. }) && viewer.is_operator();
                if !system_to_operator
                    && (p.mentions.principals.contains(viewer) || (p.mentions.all && in_room))
                {
                    mentions += 1;
                }
            }
        }
        (unread, mentions)
    }

    pub(crate) fn snapshot(
        &self,
        viewer: &Principal,
        project_names: &BTreeMap<BbProjectId, String>,
    ) -> SidebarTreeSnapshot {
        let viewer_inbox = self.inboxes.get(viewer);
        let upto = |room: &RoomId| {
            viewer_inbox
                .and_then(|i| i.cursors.get(room))
                .copied()
                .unwrap_or(Seq::ORIGIN)
        };

        let mut inbox = InboxBadge::default();
        if let Some(i) = viewer_inbox {
            for &(state, blocking) in i.items.values() {
                if state == NotifyState::Unread {
                    inbox.unread += 1;
                    if blocking {
                        inbox.blocking += 1;
                    }
                }
            }
        }

        let me_str = assignee(viewer).as_str().to_owned();
        let kanban = self
            .boards
            .iter()
            .map(|(board, b)| {
                let mut open = 0u32;
                let mut mine = 0u32;
                for (lane, who) in b.cards.values() {
                    if !b.done_lanes.contains(lane) {
                        open += 1;
                        if who.as_deref() == Some(me_str.as_str()) {
                            mine += 1;
                        }
                    }
                }
                BoardBadge {
                    board: board.clone(),
                    open,
                    mine,
                }
            })
            .collect();

        let mut projects: BTreeMap<BbProjectId, ProjectBranch> = BTreeMap::new();
        let mut wave_rooms = BTreeSet::new();
        for (slug, w) in &self.waves {
            wave_rooms.insert(w.room.clone());
            let (unread, mentions) = self.unread_for(&w.room, viewer, upto(&w.room));
            let workers = w
                .workers
                .iter()
                .map(|(role, (spec, phase, _))| WaveWorkerLeaf {
                    role: role.clone(),
                    provider: spec.provider.clone(),
                    model: spec.model.clone(),
                    phase: phase.clone(),
                })
                .collect();
            let archived = self.archived_waves.contains(slug);
            projects
                .entry(w.project.clone())
                .or_insert_with(|| ProjectBranch {
                    project: w.project.clone(),
                    name: project_names.get(&w.project).cloned(),
                    archived: self.archived_projects.contains(&w.project),
                    channels: Vec::new(),
                    waves: Vec::new(),
                })
                .waves
                .push(WaveRoomBranch {
                    wave: slug.clone(),
                    room: w.room.clone(),
                    unread,
                    mentions,
                    finished: w.finished.clone(),
                    archived,
                    workers,
                });
        }

        let mut channels = Vec::new();
        let mut direct_messages = Vec::new();
        for (room, entry) in &self.rooms {
            if wave_rooms.contains(room) || matches!(entry.origin, Some(RoomOrigin::Wave { .. })) {
                continue;
            }
            let (unread, mentions) = self.unread_for(room, viewer, upto(room));
            let archived = self.archived_rooms.contains(room);
            match room {
                RoomId::Channel {
                    scope: RoomScope::Global,
                    ..
                } => {
                    if room.is_mail_channel()
                        && !viewer.is_operator_or_system()
                        && !self.is_member(room, viewer)
                    {
                        continue;
                    }
                    channels.push(ChannelLeaf {
                        room: room.clone(),
                        unread,
                        mentions,
                        archived,
                    });
                }
                RoomId::Channel {
                    scope: RoomScope::Project { project },
                    ..
                } => {
                    projects
                        .entry(project.clone())
                        .or_insert_with(|| ProjectBranch {
                            project: project.clone(),
                            name: project_names.get(project).cloned(),
                            archived: self.archived_projects.contains(project),
                            channels: Vec::new(),
                            waves: Vec::new(),
                        })
                        .channels
                        .push(ChannelLeaf {
                            room: room.clone(),
                            unread,
                            mentions,
                            archived,
                        });
                }
                RoomId::Direct { pair } => {
                    if let Some(other) = pair.other_for_viewer(viewer) {
                        direct_messages.push(DmLeaf {
                            room: room.clone(),
                            with: other.clone(),
                            unread,
                            archived,
                        });
                    }
                }
            }
        }

        let all_phases = self
            .waves
            .values()
            .flat_map(|w| w.workers.values().map(|(_, p, _)| p));
        let osc7501 = derive_osc7501_status(&inbox, all_phases);
        SidebarTreeSnapshot {
            inbox,
            kanban,
            projects: projects.into_values().collect(),
            channels,
            direct_messages,
            osc7501,
            mail_health: MailHealthBadge {
                failed_deliveries: self.mail_failed_deliveries,
                degraded_couriers: self.degraded_couriers.len() as u32,
            },
            cursor: self.cursor.clone(),
        }
    }
}
