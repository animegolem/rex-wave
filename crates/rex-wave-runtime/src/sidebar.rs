use crate::hub::Hub;
use rex_wave_api::board::{Assign, BoardEvent, BoardId, CardNo, LaneName, assignee};
use rex_wave_api::notify::{InboxEvent, NotificationId, NotifyKind, NotifyState};
use rex_wave_api::read::*;
use rex_wave_api::room::{Mentions, RoomEvent, RoomId, RoomOrigin, RoomScope};
use rex_wave_api::stage::derive_osc7501_status;
use rex_wave_api::wave::{AgentSpec, WaveEvent, WaveOutcome, WorkerPhase};
use rex_wave_api::{
    BbProjectId, Cursor, Envelope, Event, Principal, RoleName, Seq, StreamId, WaveSlug,
};
use rex_wave_store::Journal;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::{broadcast::error::RecvError, watch};

const REPLAY_PAGE: usize = 512;

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
        let mut world = World::default();
        catch_up(&journal, &mut world);
        let (tx, world_rx) = watch::channel(Arc::new(world.clone()));
        tokio::spawn(async move {
            loop {
                match tap.recv().await {
                    Ok(env) => {
                        if world.apply(&env) && tx.send(Arc::new(world.clone())).is_err() {
                            break;
                        }
                    }
                    Err(RecvError::Lagged(_)) => {
                        if catch_up(&journal, &mut world)
                            && tx.send(Arc::new(world.clone())).is_err()
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
    rooms: BTreeMap<RoomId, RoomEntry>,
    waves: BTreeMap<WaveSlug, WaveEntry>,
    boards: BTreeMap<BoardId, BoardEntry>,
    inboxes: BTreeMap<Principal, InboxEntry>,
    archived_projects: BTreeSet<BbProjectId>,
    archived_waves: BTreeSet<WaveSlug>,
    archived_rooms: BTreeSet<RoomId>,
    cursor: Cursor,
}

#[derive(Clone, Default)]
struct RoomEntry {
    origin: Option<RoomOrigin>,
    members: BTreeSet<Principal>,
    posts: Vec<PostMeta>,
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
    workers: BTreeMap<RoleName, (AgentSpec, WorkerPhase)>,
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
                let r = self.rooms.entry(room.clone()).or_default();
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
                        r.posts.push(PostMeta {
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
                        WaveEntry {
                            project: spec.project.clone(),
                            room: room.clone(),
                            finished: None,
                            workers: BTreeMap::new(),
                        },
                    );
                }
                WaveEvent::WorkerRequested {
                    role,
                    agent,
                    attempt,
                    ..
                } => {
                    if let Some(w) = self.waves.get_mut(wave) {
                        w.workers.insert(
                            role.clone(),
                            (agent.clone(), WorkerPhase::Requested { attempt: *attempt }),
                        );
                    }
                }
                WaveEvent::WorkerObserved { role, observation } => {
                    if let Some(w) = self.waves.get_mut(wave)
                        && let Some((_, phase)) = w.workers.get_mut(role)
                    {
                        *phase = phase.observe(observation);
                    }
                }
                WaveEvent::WorkerRetiring { role, .. } => {
                    if let Some(w) = self.waves.get_mut(wave)
                        && let Some((_, phase)) = w.workers.get_mut(role)
                    {
                        *phase = phase.retiring();
                    }
                }
                WaveEvent::Finished { outcome } => {
                    if let Some(w) = self.waves.get_mut(wave) {
                        w.finished = Some(outcome.clone());
                        for (_, phase) in w.workers.values_mut() {
                            *phase = phase.retiring();
                        }
                    }
                }
                WaveEvent::Told { .. } | WaveEvent::InteractionResolved { .. } => {}
            },
            (StreamId::Board { board }, Event::Board(ev)) => {
                let b = self.boards.entry(board.clone()).or_default();
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
                let inbox = self.inboxes.entry(owner.clone()).or_default();
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
            _ => {}
        }
        true
    }

    fn is_member(&self, room: &RoomId, who: &Principal) -> bool {
        match room {
            RoomId::Direct { pair } => pair.contains(who),
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
        for p in &r.posts {
            if p.seq > upto && &p.by != viewer {
                unread += 1;
                let system_to_operator = matches!(p.by, Principal::System { .. })
                    && matches!(viewer, Principal::Operator { .. });
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
                .map(|(role, (spec, phase))| WaveWorkerLeaf {
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
                    if let Some(other) = pair.other(viewer) {
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
            .flat_map(|w| w.workers.values().map(|(_, p)| p));
        let osc7501 = derive_osc7501_status(&inbox, all_phases);
        SidebarTreeSnapshot {
            inbox,
            kanban,
            projects: projects.into_values().collect(),
            channels,
            direct_messages,
            osc7501,
            cursor: self.cursor.clone(),
        }
    }
}
