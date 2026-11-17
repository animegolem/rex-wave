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
use tokio::sync::{broadcast::error::RecvError, mpsc};

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
    if matches!(
        viewer,
        Principal::Operator { .. } | Principal::System { .. }
    ) {
        return Ok(());
    }
    let allowed = match stream {
        StreamId::Inbox { owner } => owner == viewer,
        StreamId::Room {
            room: RoomId::Direct { pair },
        } => pair.contains(viewer),
        StreamId::Room { .. } | StreamId::Wave { .. } | StreamId::Board { .. } | StreamId::Mail => {
            true
        }
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

    fn watch_sidebar(&self, viewer: Principal) -> FrameStream {
        let mut rx = self.sidebar.subscribe();
        let (tx, stream) = frame_channel(16);
        tokio::spawn(async move {
            let mut last = rx.borrow_and_update().clone();
            let mut sent = last.snapshot(&viewer);
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
                        let next = last.snapshot(&viewer);
                        if next != sent {
                            sent = next.clone();
                            if tx.send(Ok(Frame::Sidebar(Box::new(next)))).await.is_err() {
                                break;
                            }
                        }
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
                            let _ = tx.send(Err(e)).await;
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
                _ => initial.keys().cloned().collect(),
            };
            for s in candidates {
                filter.seed_stream(&journal, &s);
                let start = start_seq(&from, &initial, &s);
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
                                filter.seed_stream(&journal, &s);
                                if !filter.considers_stream(&s) {
                                    continue;
                                }
                                let cur = *seen.entry(s.clone()).or_insert_with(|| start_seq(&from, &initial, &s));
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
                            return Ok(Page {
                                entries,
                                cursor,
                                timed_out: true,
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
                        return Ok(Page {
                            entries,
                            cursor,
                            timed_out: false,
                        });
                    }
                }
                Some(Ok(Frame::CaughtUp { cursor: c })) => {
                    cursor = c;
                    caught_up = true;
                    if !entries.is_empty() {
                        return Ok(Page {
                            entries,
                            cursor,
                            timed_out: false,
                        });
                    }
                    if req.timeout_ms == Some(0) {
                        return Ok(Page {
                            entries,
                            cursor,
                            timed_out: true,
                        });
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e),
                None => {
                    return Ok(Page {
                        entries,
                        cursor,
                        timed_out: false,
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
                if !matches!(
                    viewer,
                    Principal::Operator { .. } | Principal::System { .. }
                ) {
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

    /// Learn stream-header facts (like which project owns a wave) even when
    /// `Cursor::live()` skips replaying the stream's body.
    fn seed_stream(&mut self, journal: &Journal, stream: &StreamId) {
        if let (Filter::Project { .. }, StreamId::Wave { .. }) = (&self, stream)
            && let Ok(first) = journal.read(stream, Seq::ORIGIN, 1)
        {
            for env in &first {
                self.observe(env);
            }
        }
    }

    fn observe(&mut self, env: &Envelope) {
        match (self, &env.stream, &env.event) {
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
            Filter::Mentions { sidebar, joined } => match (&env.stream, &env.event) {
                (
                    StreamId::Room {
                        room: RoomId::Direct { pair },
                    },
                    Event::Room(RoomEvent::Posted { .. }),
                ) => pair.contains(viewer) && &env.by != viewer,
                (StreamId::Room { room }, Event::Room(RoomEvent::Posted { mentions, .. })) => {
                    if &env.by == viewer {
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
                    can_read(&ctx.by, &r.stream)?;
                    let limit = r.limit.unwrap_or(200).clamp(1, 1000) as usize;
                    let entries = self
                        .journal
                        .read(&r.stream, r.after, limit)
                        .map_err(store_error)?;
                    let last = entries.last().map_or(r.after, |e| e.seq);
                    let cursor = Cursor::default().with(r.stream, last);
                    Ok(WireReply::Read(Page {
                        entries,
                        cursor,
                        timed_out: false,
                    }))
                }
                ReadCommand::Wait(w) => Ok(WireReply::Wait(self.wait_for(ctx.by, w).await?)),
                ReadCommand::Sidebar(_) => Ok(WireReply::Sidebar(self.sidebar.snapshot(&ctx.by))),
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
