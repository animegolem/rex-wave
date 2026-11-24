use crate::hub::Hub;
use crate::router::Session;
use crate::sidebar::SidebarProjection;
use crate::{BoxFut, DecideCtx, Service, store_error};
use futures_util::StreamExt;
use rex_wave_api::board::{BoardId, project_board};
use rex_wave_api::read::{Page, ThreadPageView};
use rex_wave_api::room::{RoomEvent, RoomId, RoomScope};
use rex_wave_api::wave::WaveEvent;
use rex_wave_api::*;
use rex_wave_store::Journal;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast::error::RecvError, mpsc, watch};

const REPLAY_PAGE: usize = 512;
const WATCH_BUFFER: usize = 128;

pub fn frame_channel(buffer: usize) -> (mpsc::Sender<Result<Frame, ApiError>>, FrameStream) {
    let (tx, rx) = mpsc::channel(buffer);
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    (tx, Box::pin(stream))
}

/// BB-owned agent data, proxied (implemented by `rex-wave-bb`). Never journaled:
/// BB is the single owner of thread events.
pub trait AgentFeed: Send + Sync + 'static {
    fn page(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        after: Option<BbSeq>,
        limit: u32,
    ) -> BoxFut<'_, Result<ThreadPageView, ApiError>>;
    /// Live frames for one agent. Lease semantics: BB-side watch is held only
    /// while the returned stream is alive.
    fn live(&self, wave: &WaveSlug, role: &RoleName) -> BoxFut<'_, Result<FrameStream, ApiError>>;
    /// Human BB project names (`proj_...` -> `"demo-work"`), loaded from the
    /// local BB metadata cache with graceful fallback to `None` when offline.
    fn project_names(&self) -> Option<watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>> {
        None
    }
}

/// Placeholder `AgentFeed` for tests and deployments without BB attached.
pub struct NoAgentFeed;

impl AgentFeed for NoAgentFeed {
    fn page(
        &self,
        _wave: &WaveSlug,
        _role: &RoleName,
        _after: Option<BbSeq>,
        _limit: u32,
    ) -> BoxFut<'_, Result<ThreadPageView, ApiError>> {
        Box::pin(async {
            Err(ApiError::Unavailable {
                family: Family::Wave,
            })
        })
    }
    fn live(
        &self,
        _wave: &WaveSlug,
        _role: &RoleName,
    ) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        Box::pin(async {
            Err(ApiError::Unavailable {
                family: Family::Wave,
            })
        })
    }
}

/// Stream-level read privacy: a principal's inbox is theirs alone, and a DM is
/// only for its two members.
pub fn can_read(viewer: &Principal, stream: &StreamId) -> Result<(), ApiError> {
    if viewer.is_operator_or_system() {
        return Ok(());
    }
    let allowed = match stream {
        StreamId::Inbox { owner } => owner == viewer,
        StreamId::Room {
            room: RoomId::Direct { pair },
        } => pair.contains(viewer),
        StreamId::Room { .. }
        | StreamId::Wave { .. }
        | StreamId::Board { .. }
        | StreamId::Mail
        | StreamId::Presence => true,
    };
    if allowed {
        Ok(())
    } else {
        Err(ApiError::Denied {
            principal: viewer.to_string(),
            action: format!("read {stream:?}"),
        })
    }
}

/// Maps `ApiError::Denied` to `ApiError::NotFound` so external/HTTP callers
/// cannot probe whether an invisible resource (DM or another principal's Inbox) exists.
pub fn mask_unauthorized_as_not_found(err: ApiError, resource: impl Into<String>) -> ApiError {
    match err {
        ApiError::Denied { .. } => ApiError::NotFound {
            what: resource.into(),
        },
        other => other,
    }
}

/// Checks stream visibility for external/HTTP callers, returning `ApiError::NotFound`
/// (HTTP 404) instead of `ApiError::Denied` (HTTP 403) when outside caller visibility.
pub fn can_read_visible(
    viewer: &Principal,
    stream: &StreamId,
    resource: impl Into<String>,
) -> Result<(), ApiError> {
    can_read(viewer, stream).map_err(|err| mask_unauthorized_as_not_found(err, resource))
}

/// True when every required `key=value` label is present in `actual`.
pub fn labels_match(required: &[Label], actual: &[Label]) -> bool {
    required.iter().all(|req| actual.contains(req))
}

/// True when `RoomEvent::Labeled` matches `required` either `before` OR `after` the mutation.
pub fn labeled_event_matches(required: &[Label], before: &[Label], after: &[Label]) -> bool {
    labels_match(required, before) || labels_match(required, after)
}

/// Serves `Family::Read` and every `watch`.
pub struct ReadSide {
    pub journal: Journal,
    pub hub: Hub,
    pub sidebar: Arc<SidebarProjection>,
    pub agents: Arc<dyn AgentFeed>,
}

impl ReadSide {
    /// Tap first, replay second, dedupe by `(stream, seq)`.
    pub fn watch(
        &self,
        session: Session,
        interest: Interest,
        from: Cursor,
    ) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        Box::pin(async move {
            let viewer = session.me().clone();
            match interest {
                Interest::Agent { wave, role } => self.agents.live(&wave, &role).await,
                Interest::Sidebar => Ok(self.watch_sidebar(viewer)),
                Interest::Stage { target } => self.watch_stage(session, target, from).await,
                other => self.watch_journal(viewer, other, from),
            }
        })
    }

    fn snapshot_sidebar(&self, viewer: &Principal) -> rex_wave_api::read::SidebarTreeSnapshot {
        match self.agents.project_names() {
            Some(rx) => {
                let names = rx.borrow().clone();
                self.sidebar.snapshot_with_names(viewer, &names)
            }
            None => self.sidebar.snapshot(viewer),
        }
    }

    fn watch_sidebar(&self, viewer: Principal) -> FrameStream {
        let mut rx = self.sidebar.subscribe();
        let mut names_rx = self.agents.project_names();
        let (tx, stream) = frame_channel(16);
        tokio::spawn(async move {
            let empty_names = Arc::new(BTreeMap::new());
            let mut last = rx.borrow_and_update().clone();
            let mut last_names = names_rx
                .as_mut()
                .map_or_else(|| empty_names.clone(), |r| r.borrow_and_update().clone());
            let mut sent = last.snapshot(&viewer, &last_names);
            if tx
                .send(Ok(Frame::Sidebar(Box::new(sent.clone()))))
                .await
                .is_err()
            {
                return;
            }
            loop {
                tokio::select! {
                    biased;
                    () = tx.closed() => break,
                    changed = rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        last = rx.borrow_and_update().clone();
                    }
                    Ok(()) = async {
                        match names_rx.as_mut() {
                            Some(r) => r.changed().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        if let Some(r) = names_rx.as_mut() {
                            last_names = r.borrow_and_update().clone();
                        }
                    }
                }
                let next = last.snapshot(&viewer, &last_names);
                if next != sent {
                    sent = next.clone();
                    if tx.send(Ok(Frame::Sidebar(Box::new(next)))).await.is_err() {
                        break;
                    }
                }
            }
        });
        stream
    }

    async fn watch_stage(
        &self,
        session: Session,
        target: StageTarget,
        from: Cursor,
    ) -> Result<FrameStream, ApiError> {
        // Claiming the stage slot before opening `inner` ends the old stage
        // watch even if the new target fails validation.
        let (generation, mut gen_rx) = session.focus();
        let viewer = session.me().clone();
        let inner_interest = match &target {
            StageTarget::Inbox => Interest::MyInbox,
            StageTarget::Kanban { board } => Interest::Streams {
                streams: vec![StreamId::Board {
                    board: board.clone(),
                }],
            },
            StageTarget::Room { room } => Interest::Room { room: room.clone() },
            StageTarget::Agent { wave, role } => Interest::Agent {
                wave: wave.clone(),
                role: role.clone(),
            },
        };
        let mut inner = match inner_interest {
            Interest::Agent { wave, role } => self.agents.live(&wave, &role).await?,
            other => self.watch_journal(viewer, other, from)?,
        };
        let (tx, stream) = frame_channel(WATCH_BUFFER);
        tokio::spawn(async move {
            if tx
                .send(Ok(Frame::StageFocused { generation, target }))
                .await
                .is_err()
            {
                return;
            }
            loop {
                tokio::select! {
                    biased;
                    () = tx.closed() => break,
                    changed = gen_rx.changed() => {
                        if changed.is_err() || *gen_rx.borrow() != generation {
                            break;
                        }
                    }
                    item = inner.next() => match item {
                        Some(Ok(frame)) => {
                            if tx.send(Ok(Frame::Stage { generation, inner: Box::new(frame) })).await.is_err() {
                                break;
                            }
                        }
                        Some(Err(e)) => {
                            // Deliver the error, but stop waiting on a full
                            // channel once this stage has been replaced.
                            let send = tx.send(Err(e));
                            tokio::pin!(send);
                            loop {
                                tokio::select! {
                                    biased;
                                    changed = gen_rx.changed() => {
                                        if changed.is_err() || *gen_rx.borrow() != generation {
                                            break;
                                        }
                                    }
                                    _ = &mut send => break,
                                }
                            }
                            break;
                        }
                        None => break,
                    },
                }
            }
        });
        Ok(stream)
    }

    fn watch_journal(
        &self,
        viewer: Principal,
        interest: Interest,
        from: Cursor,
    ) -> Result<FrameStream, ApiError> {
        let mut filter = Filter::build(&viewer, interest, self.sidebar.clone())?;
        let mut tap = self.hub.tap();
        let initial: BTreeMap<StreamId, Seq> = self
            .journal
            .streams()
            .map_err(store_error)?
            .into_iter()
            .collect();

        let journal = self.journal.clone();
        let (tx, stream) = frame_channel(WATCH_BUFFER);
        tokio::spawn(async move {
            let mut seen: BTreeMap<StreamId, Seq> = BTreeMap::new();
            let mut cursor = from.clone();

            let candidates: Vec<StreamId> = match &filter {
                Filter::Exact(set) => set.iter().cloned().collect(),
                Filter::RoomFiltered { stream, .. } => vec![stream.clone()],
                _ => initial.keys().cloned().collect(),
            };
            for s in candidates {
                let start = start_seq(&from, &initial, &s);
                filter.seed_stream(&journal, &s, start);
                seen.insert(s.clone(), start);
                cursor.advance(&s, start);
                if from.from_head() && !from.lists(&s) {
                    continue;
                }
                if !replay_stream(
                    &journal,
                    &s,
                    &viewer,
                    &mut filter,
                    &mut seen,
                    &mut cursor,
                    &tx,
                )
                .await
                {
                    return;
                }
            }
            if tx
                .send(Ok(Frame::CaughtUp {
                    cursor: cursor.clone(),
                }))
                .await
                .is_err()
            {
                return;
            }

            // and filling any gap from the journal.
            loop {
                tokio::select! {
                    biased;
                    () = tx.closed() => break,
                    item = tap.recv() => match item {
                        Ok(env) => {
                            filter.observe(&env);
                            if !filter.considers_stream(&env.stream) {
                                continue;
                            }
                            let cur = *seen.entry(env.stream.clone()).or_insert_with(|| start_seq(&from, &initial, &env.stream));
                            if env.seq <= cur {
                                continue;
                            }
                            if env.seq == cur.next() {
                                seen.insert(env.stream.clone(), env.seq);
                                cursor.advance(&env.stream, env.seq);
                                if filter.matches(&viewer, &env) && tx.send(Ok(Frame::Committed((*env).clone()))).await.is_err() {
                                    break;
                                }
                            } else if !replay_stream(&journal, &env.stream, &viewer, &mut filter, &mut seen, &mut cursor, &tx).await {
                                break;
                            }
                        }
                        Err(RecvError::Lagged(_)) => {
                            let Ok(streams) = journal.streams() else { break };
                            for (s, head) in streams {
                                let cur = *seen.entry(s.clone()).or_insert_with(|| start_seq(&from, &initial, &s));
                                filter.seed_stream(&journal, &s, cur);
                                if !filter.considers_stream(&s) {
                                    continue;
                                }
                                if cur < head && !replay_stream(&journal, &s, &viewer, &mut filter, &mut seen, &mut cursor, &tx).await {
                                    return;
                                }
                            }
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
            }
        });
        Ok(stream)
    }

    async fn wait_for(&self, viewer: Principal, req: read::Wait) -> Result<Page, ApiError> {
        let mut stream = self.watch_journal(viewer, req.interest, req.after.clone())?;
        let mut entries = Vec::new();
        let mut cursor = req.after;
        let mut caught_up = false;
        let deadline = req
            .timeout_ms
            .map(|ms| tokio::time::Instant::now() + Duration::from_millis(ms));

        loop {
            let next = match deadline {
                Some(until) if caught_up => {
                    match tokio::time::timeout_at(until, stream.next()).await {
                        Ok(item) => item,
                        Err(_) => {
                            let total_count = entries.len();
                            return Ok(Page {
                                entries,
                                cursor,
                                timed_out: true,
                                total_count,
                                truncated: false,
                            });
                        }
                    }
                }
                _ => stream.next().await,
            };
            match next {
                Some(Ok(Frame::Committed(env))) => {
                    cursor.advance(&env.stream, env.seq);
                    entries.push(env);
                    if caught_up {
                        tokio::task::yield_now().await;
                        while let Ok(Some(Ok(Frame::Committed(extra)))) =
                            tokio::time::timeout(Duration::ZERO, stream.next()).await
                        {
                            cursor.advance(&extra.stream, extra.seq);
                            entries.push(extra);
                        }
                        let total_count = entries.len();
                        return Ok(Page {
                            entries,
                            cursor,
                            timed_out: false,
                            total_count,
                            truncated: false,
                        });
                    }
                }
                Some(Ok(Frame::CaughtUp { cursor: c })) => {
                    cursor = c;
                    caught_up = true;
                    if !entries.is_empty() {
                        let total_count = entries.len();
                        return Ok(Page {
                            entries,
                            cursor,
                            timed_out: false,
                            total_count,
                            truncated: false,
                        });
                    }
                    if req.timeout_ms == Some(0) {
                        return Ok(Page {
                            entries,
                            cursor,
                            timed_out: true,
                            total_count: 0,
                            truncated: false,
                        });
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e),
                None => {
                    let total_count = entries.len();
                    return Ok(Page {
                        entries,
                        cursor,
                        timed_out: false,
                        total_count,
                        truncated: false,
                    });
                }
            }
        }
    }
}

fn start_seq(from: &Cursor, initial: &BTreeMap<StreamId, Seq>, stream: &StreamId) -> Seq {
    if from.lists(stream) {
        from.at(stream)
    } else if from.from_head() {
        initial.get(stream).copied().unwrap_or(Seq::ORIGIN)
    } else {
        Seq::ORIGIN
    }
}

async fn replay_stream(
    journal: &Journal,
    stream: &StreamId,
    viewer: &Principal,
    filter: &mut Filter,
    seen: &mut BTreeMap<StreamId, Seq>,
    cursor: &mut Cursor,
    tx: &mpsc::Sender<Result<Frame, ApiError>>,
) -> bool {
    loop {
        let after = seen.get(stream).copied().unwrap_or(Seq::ORIGIN);
        let page = match journal.read(stream, after, REPLAY_PAGE) {
            Ok(p) => p,
            Err(e) => {
                let _ = tx.send(Err(store_error(e))).await;
                return false;
            }
        };
        if page.is_empty() {
            return true;
        }
        for env in page {
            filter.observe(&env);
            seen.insert(stream.clone(), env.seq);
            cursor.advance(stream, env.seq);
            if filter.matches(viewer, &env) && tx.send(Ok(Frame::Committed(env))).await.is_err() {
                return false;
            }
        }
    }
}

enum Filter {
    Exact(BTreeSet<StreamId>),
    RoomFiltered {
        stream: StreamId,
        re: Option<Seq>,
        labels: Vec<Label>,
        thread_members: BTreeSet<Seq>,
        current_labels: BTreeMap<Seq, Vec<Label>>,
        seeded_upto: Seq,
    },
    Mentions {
        sidebar: Arc<SidebarProjection>,
        joined: BTreeMap<RoomId, BTreeSet<Principal>>,
    },
    Project {
        project: BbProjectId,
        board: BoardId,
        waves: BTreeSet<WaveSlug>,
    },
    Everything,
}

impl Filter {
    fn build(
        viewer: &Principal,
        interest: Interest,
        sidebar: Arc<SidebarProjection>,
    ) -> Result<Self, ApiError> {
        match interest {
            Interest::Streams { streams } => {
                for s in &streams {
                    can_read(viewer, s)?;
                }
                Ok(Filter::Exact(streams.into_iter().collect()))
            }
            Interest::Room { room } => {
                let s = StreamId::Room {
                    room: room.resolve(viewer)?,
                };
                can_read(viewer, &s)?;
                Ok(Filter::Exact(BTreeSet::from([s])))
            }
            Interest::RoomFiltered { room, re, labels } => {
                let s = StreamId::Room {
                    room: room.resolve(viewer)?,
                };
                can_read(viewer, &s)?;
                Ok(Filter::RoomFiltered {
                    stream: s,
                    re,
                    labels,
                    thread_members: BTreeSet::new(),
                    current_labels: BTreeMap::new(),
                    seeded_upto: Seq::ORIGIN,
                })
            }
            Interest::MyInbox => Ok(Filter::Exact(BTreeSet::from([StreamId::Inbox {
                owner: viewer.clone(),
            }]))),
            Interest::Mentions => Ok(Filter::Mentions {
                sidebar,
                joined: BTreeMap::new(),
            }),
            Interest::Project { project } => {
                let board = project_board(&project);
                Ok(Filter::Project {
                    project,
                    board,
                    waves: BTreeSet::new(),
                })
            }
            Interest::Everything => {
                if !viewer.is_operator_or_system() {
                    return Err(ApiError::Denied {
                        principal: viewer.to_string(),
                        action: "watch everything".into(),
                    });
                }
                Ok(Filter::Everything)
            }
            Interest::Agent { .. } | Interest::Sidebar | Interest::Stage { .. } => Err(
                ApiError::Internal("non-journal interest routed to watch_journal".into()),
            ),
        }
    }

    /// Learn stream-header facts (like which project owns a wave, or prior thread
    /// membership and labels before a mid-stream resume point).
    fn seed_stream(&mut self, journal: &Journal, stream: &StreamId, upto: Seq) {
        if let (Filter::Project { .. }, StreamId::Wave { .. }) = (&self, stream)
            && let Ok(first) = journal.read(stream, Seq::ORIGIN, 1)
        {
            for env in &first {
                self.observe(env);
            }
            return;
        }

        let seed_start = match self {
            Filter::RoomFiltered {
                stream: target,
                re,
                labels,
                seeded_upto,
                ..
            } if stream == target
                && (re.is_some() || !labels.is_empty())
                && upto > *seeded_upto =>
            {
                Some(*seeded_upto)
            }
            _ => None,
        };
        let Some(mut after) = seed_start else {
            return;
        };
        while after < upto {
            let Ok(page) = journal.read(stream, after, REPLAY_PAGE) else {
                return;
            };
            if page.is_empty() {
                break;
            }
            for env in &page {
                if env.seq > upto {
                    if let Filter::RoomFiltered { seeded_upto, .. } = self {
                        *seeded_upto = (*seeded_upto).max(upto);
                    }
                    return;
                }
                self.observe(env);
                after = env.seq;
            }
        }
        if let Filter::RoomFiltered { seeded_upto, .. } = self {
            *seeded_upto = (*seeded_upto).max(upto);
        }
    }

    fn observe(&mut self, env: &Envelope) {
        match (self, &env.stream, &env.event) {
            (
                Filter::RoomFiltered {
                    stream,
                    re,
                    thread_members,
                    current_labels,
                    seeded_upto,
                    ..
                },
                env_stream,
                Event::Room(RoomEvent::Posted {
                    re: post_re,
                    labels,
                    ..
                }),
            ) if env_stream == stream => {
                *seeded_upto = (*seeded_upto).max(env.seq);
                if let Some(root) = *re
                    && let Some(parent) = post_re
                    && (*parent == root || thread_members.contains(parent))
                {
                    thread_members.insert(env.seq);
                }
                current_labels.insert(env.seq, labels.clone());
            }
            (
                Filter::RoomFiltered {
                    stream,
                    current_labels,
                    seeded_upto,
                    ..
                },
                env_stream,
                Event::Room(RoomEvent::Labeled { target, after, .. }),
            ) if env_stream == stream => {
                *seeded_upto = (*seeded_upto).max(env.seq);
                current_labels.insert(*target, after.clone());
            }
            (
                Filter::RoomFiltered {
                    stream,
                    seeded_upto,
                    ..
                },
                env_stream,
                _,
            ) if env_stream == stream => {
                *seeded_upto = (*seeded_upto).max(env.seq);
            }
            (
                Filter::Project { project, waves, .. },
                StreamId::Wave { wave },
                Event::Wave(WaveEvent::Started { spec, .. }),
            ) if &spec.project == project => {
                waves.insert(wave.clone());
            }
            (
                Filter::Mentions { joined, .. },
                StreamId::Room { room },
                Event::Room(RoomEvent::MemberJoined { member, .. }),
            ) => {
                joined
                    .entry(room.clone())
                    .or_default()
                    .insert(member.clone());
            }
            (
                Filter::Mentions { joined, .. },
                StreamId::Room { room },
                Event::Room(RoomEvent::MemberLeft { member }),
            ) => {
                if let Some(m) = joined.get_mut(room) {
                    m.remove(member);
                }
            }
            _ => {}
        }
    }

    fn considers_stream(&self, stream: &StreamId) -> bool {
        match self {
            Filter::Exact(set) => set.contains(stream),
            Filter::RoomFiltered { stream: target, .. } => stream == target,
            Filter::Mentions { .. } => matches!(stream, StreamId::Room { .. }),
            Filter::Project {
                project,
                board,
                waves,
            } => match stream {
                StreamId::Room {
                    room:
                        RoomId::Channel {
                            scope: RoomScope::Project { project: p },
                            ..
                        },
                } => p == project,
                StreamId::Wave { wave } => waves.contains(wave),
                StreamId::Board { board: b } => b == board,
                _ => false,
            },
            Filter::Everything => true,
        }
    }

    fn matches(&self, viewer: &Principal, env: &Envelope) -> bool {
        if !self.considers_stream(&env.stream) || can_read(viewer, &env.stream).is_err() {
            return false;
        }
        match self {
            Filter::Exact(_) | Filter::Project { .. } | Filter::Everything => true,
            Filter::RoomFiltered {
                re,
                labels,
                thread_members,
                current_labels,
                ..
            } => match &env.event {
                Event::Room(RoomEvent::Posted {
                    labels: post_labels,
                    ..
                }) => {
                    let re_ok = match re {
                        Some(root) => env.seq == *root || thread_members.contains(&env.seq),
                        None => true,
                    };
                    let labels_ok = labels.is_empty() || labels_match(labels, post_labels);
                    re_ok && labels_ok
                }
                Event::Room(RoomEvent::Labeled {
                    target,
                    before,
                    after,
                    ..
                }) => {
                    let re_ok = match re {
                        Some(root) => *target == *root || thread_members.contains(target),
                        None => true,
                    };
                    let labels_ok =
                        labels.is_empty() || labeled_event_matches(labels, before, after);
                    re_ok && labels_ok
                }
                Event::Room(
                    RoomEvent::TaskClaimed { task, .. }
                    | RoomEvent::TaskHeartbeat { task, .. }
                    | RoomEvent::TaskTransitioned { task, .. }
                    | RoomEvent::TaskUpdated { task, .. },
                ) => {
                    let re_ok = re.is_none_or(|root| *task == root);
                    let labels_ok = labels.is_empty()
                        || current_labels
                            .get(task)
                            .is_some_and(|cur| labels_match(labels, cur));
                    re_ok && labels_ok
                }
                Event::Room(
                    RoomEvent::Opened { .. }
                    | RoomEvent::MemberJoined { .. }
                    | RoomEvent::MemberLeft { .. }
                    | RoomEvent::TopicSet { .. },
                ) => re.is_none() && labels.is_empty(),
                _ => false,
            },
            Filter::Mentions { sidebar, joined } => match (&env.stream, &env.event) {
                (
                    StreamId::Room {
                        room: RoomId::Direct { pair },
                    },
                    Event::Room(RoomEvent::Posted { .. }),
                ) => pair.other_for_viewer(viewer).is_some() && !viewer.same_actor(&env.by),
                (StreamId::Room { room }, Event::Room(RoomEvent::Posted { mentions, .. })) => {
                    if viewer.same_actor(&env.by) {
                        return false;
                    }
                    if mentions.principals.contains(viewer) {
                        return true;
                    }
                    let in_room = joined.get(room).is_some_and(|m| m.contains(viewer))
                        || sidebar.is_member(room, viewer);
                    mentions.all && in_room
                }
                _ => false,
            },
        }
    }
}

impl Service<ReadCommand> for ReadSide {
    fn dispatch(
        &self,
        cmd: ReadCommand,
        ctx: DecideCtx,
    ) -> BoxFut<'_, Result<WireReply, ApiError>> {
        Box::pin(async move {
            match cmd {
                ReadCommand::Read(r) => {
                    can_read(ctx.by(), &r.stream)?;
                    let limit = r.limit.unwrap_or(200).clamp(1, 1000) as usize;
                    let entries = self
                        .journal
                        .read(&r.stream, r.after, limit)
                        .map_err(store_error)?;
                    let head = self.journal.head(&r.stream).map_err(store_error)?;
                    let total_count =
                        (head.0.saturating_sub(r.after.0) as usize).max(entries.len());
                    let truncated = entries.len() < total_count;
                    let last = entries.last().map_or(r.after, |e| e.seq);
                    let cursor = Cursor::default().with(r.stream, last);
                    Ok(WireReply::Read(Page {
                        entries,
                        cursor,
                        timed_out: false,
                        total_count,
                        truncated,
                    }))
                }
                ReadCommand::Wait(w) => {
                    Ok(WireReply::Wait(self.wait_for(ctx.by().clone(), w).await?))
                }
                ReadCommand::Sidebar(_) => Ok(WireReply::Sidebar(self.snapshot_sidebar(ctx.by()))),
                ReadCommand::ThreadPage(t) => {
                    let limit = t.limit.unwrap_or(100).clamp(1, 500);
                    Ok(WireReply::ThreadPage(
                        self.agents.page(&t.wave, &t.role, t.after, limit).await?,
                    ))
                }
            }
        })
    }
}
