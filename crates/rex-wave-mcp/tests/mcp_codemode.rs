use rex_wave_api::board::{
    Assign, BoardEnsure, BoardEvent, BoardId, BoardInfo, BoardSnapshot, BoardView, Card, CardNo,
    CardRef, CardRevision, CardUpsert, LaneCards, LaneName, Priority,
};
use rex_wave_api::room::{
    Body, Mentions, Posted, RoomEnsure, RoomEvent, RoomId, RoomInfo, RoomOrigin, RoomPost, RoomRef,
    RoomScope,
};
use rex_wave_api::wave::{WaveEvent, WaveSnapshot, WaveStarted, WaveStatus};
use rex_wave_api::*;
use rex_wave_mcp::codemode::{Budget, CodeError, run_code};
use rex_wave_mcp::{MAX_RUN_CODE_TIMEOUT_MS, McpServer, PROTOCOL_VERSION, RUN_CODE_TOOL};
use rex_wave_runtime::{
    DecideCtx, Decider, EntryMeta, Hub, LocalClient, NoAgentFeed, ReadSide, Registry, Router,
    SidebarProjection, Unavailable,
};
use rex_wave_store::Journal;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[derive(Default)]
struct TestRoomState {
    id: Option<RoomId>,
    origin: Option<RoomOrigin>,
    topic: Option<String>,
    head: Seq,
    members: Vec<(Handle, Principal)>,
    last_mentions: Mentions,
}

struct TestRooms;

impl Decider for TestRooms {
    type State = TestRoomState;
    type Cmd = RoomCommand;
    type Event = RoomEvent;

    fn address(cmd: &Self::Cmd, ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        let room_ref = match cmd {
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
            room: room_ref.resolve(&ctx.by)?,
        })
    }

    fn idem(cmd: &Self::Cmd) -> Option<IdemKey> {
        match cmd {
            RoomCommand::RoomPost(c) => c.idem.clone(),
            _ => None,
        }
    }

    fn decide(
        state: &Self::State,
        cmd: &Self::Cmd,
        ctx: &DecideCtx,
    ) -> Result<Vec<Self::Event>, ApiError> {
        match cmd {
            RoomCommand::RoomEnsure(RoomEnsure {
                room,
                topic,
                origin,
            }) => {
                if state.id.is_some() {
                    return Ok(Vec::new());
                }
                let id = room.resolve(&ctx.by)?;
                let mut events = vec![RoomEvent::Opened {
                    id,
                    origin: origin.clone().unwrap_or(RoomOrigin::Manual),
                    topic: topic.clone(),
                }];
                if let Some(h) = default_handle(&ctx.by) {
                    events.push(RoomEvent::MemberJoined {
                        member: ctx.by.clone(),
                        handle: h,
                    });
                }
                Ok(events)
            }
            RoomCommand::RoomJoin(j) => {
                let who = j.member.clone().unwrap_or_else(|| ctx.by.clone());
                if state.members.iter().any(|(_, p)| p == &who) {
                    return Ok(Vec::new());
                }
                let handle = match &j.handle {
                    Some(h) => h.clone(),
                    None => default_handle(&who)
                        .ok_or_else(|| ApiError::Internal("no handle for system".into()))?,
                };
                Ok(vec![RoomEvent::MemberJoined {
                    member: who,
                    handle,
                }])
            }
            RoomCommand::RoomLeave(_) => Ok(vec![RoomEvent::MemberLeft {
                member: ctx.by.clone(),
            }]),
            RoomCommand::RoomPost(RoomPost {
                room,
                body,
                re,
                after,
                labels,
                task,
                idem,
            }) => {
                let mut events = Vec::new();
                if state.id.is_none() {
                    let id = room.resolve(&ctx.by)?;
                    events.push(RoomEvent::Opened {
                        id,
                        origin: RoomOrigin::Manual,
                        topic: None,
                    });
                }
                let mut mentioned = Vec::new();
                let mut all = false;
                for token in body.as_str().split_whitespace() {
                    if let Some(tag) = token.strip_prefix('@') {
                        let clean = tag.trim_end_matches(|c: char| {
                            !c.is_ascii_alphanumeric() && c != '-' && c != '_'
                        });
                        if clean == "all" {
                            all = true;
                        } else if let Ok(seat) = SeatName::parse(clean) {
                            let p = Principal::Seat { seat };
                            if !mentioned.contains(&p) {
                                mentioned.push(p);
                            }
                        }
                    }
                }
                events.push(RoomEvent::Posted {
                    body: body.clone(),
                    mentions: Mentions {
                        principals: mentioned,
                        all,
                    },
                    re: *re,
                    after: *after,
                    labels: labels.clone(),
                    task: task.clone(),
                    idem: idem.clone(),
                });
                Ok(events)
            }
            _ => Err(ApiError::Unavailable {
                family: Family::Room,
            }),
        }
    }

    fn evolve(state: &mut Self::State, event: &Self::Event, meta: &EntryMeta) {
        state.head = meta.seq;
        match event {
            RoomEvent::Opened { id, origin, topic } => {
                state.id = Some(id.clone());
                state.origin = Some(origin.clone());
                state.topic = topic.clone();
            }
            RoomEvent::MemberJoined { member, handle } => {
                if !state.members.iter().any(|(_, p)| p == member) {
                    state.members.push((handle.clone(), member.clone()));
                }
            }
            RoomEvent::MemberLeft { member } => {
                state.members.retain(|(_, p)| p != member);
            }
            RoomEvent::Posted { mentions, .. } => {
                state.last_mentions = mentions.clone();
            }
            RoomEvent::TopicSet { topic } => {
                state.topic = Some(topic.clone());
            }
            RoomEvent::Labeled { .. }
            | RoomEvent::TaskClaimed { .. }
            | RoomEvent::TaskHeartbeat { .. }
            | RoomEvent::TaskTransitioned { .. }
            | RoomEvent::TaskUpdated { .. } => {}
        }
    }

    fn reply(state: &Self::State, cmd: Self::Cmd, _committed: &[Envelope]) -> WireReply {
        match cmd {
            RoomCommand::RoomEnsure(_) => WireReply::RoomEnsure(RoomInfo {
                id: state.id.clone().expect("room opened"),
                origin: state.origin.clone().unwrap_or(RoomOrigin::Manual),
                topic: state.topic.clone(),
                head: state.head,
                members: state.members.clone(),
            }),
            RoomCommand::RoomPost(_) => WireReply::RoomPost(Posted {
                room: state.id.clone().expect("room opened"),
                seq: state.head,
                mentions: state.last_mentions.clone(),
                re: None,
                org_id: None,
                delivery_notice: None,
            }),
            RoomCommand::RoomJoin(_) => WireReply::RoomJoin(Ack),
            RoomCommand::RoomLeave(_) => WireReply::RoomLeave(Ack),
            _ => unreachable!("rejected in decide"),
        }
    }

    fn wrap(event: Self::Event) -> Event {
        Event::Room(event)
    }

    fn unwrap(event: &Event) -> Option<&Self::Event> {
        match event {
            Event::Room(e) => Some(e),
            _ => None,
        }
    }
}

fn default_handle(principal: &Principal) -> Option<Handle> {
    match principal {
        Principal::Operator { seat } | Principal::Seat { seat } => {
            Handle::parse(seat.as_str()).ok()
        }
        Principal::Worker { role, .. } => Handle::parse(role.as_str()).ok(),
        Principal::System { .. } => None,
    }
}

#[derive(Default)]
struct TestBoardState {
    board: Option<BoardId>,
    lanes: Vec<LaneName>,
    cards: BTreeMap<CardNo, Card>,
    head: u64,
    last_card: Option<CardRef>,
}

struct TestBoards;

impl Decider for TestBoards {
    type State = TestBoardState;
    type Cmd = BoardCommand;
    type Event = BoardEvent;

    fn address(cmd: &Self::Cmd, _ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        let board = match cmd {
            BoardCommand::BoardEnsure(c) => c.board.clone(),
            BoardCommand::CardUpsert(c) => c.board.clone(),
            BoardCommand::CardArchive(c) => c.board.clone(),
            BoardCommand::BoardView(c) => c.board.clone(),
        };
        Ok(StreamId::Board { board })
    }

    fn idem(_cmd: &Self::Cmd) -> Option<IdemKey> {
        None
    }

    fn decide(
        state: &Self::State,
        cmd: &Self::Cmd,
        _ctx: &DecideCtx,
    ) -> Result<Vec<Self::Event>, ApiError> {
        match cmd {
            BoardCommand::BoardEnsure(BoardEnsure { board, lanes }) => {
                if state.board.is_some() {
                    return Ok(Vec::new());
                }
                let default_lanes = match lanes {
                    Some(l) if !l.is_empty() => l.clone(),
                    _ => ["todo", "doing", "review", "done"]
                        .into_iter()
                        .map(|s| LaneName::parse(s).expect("lane"))
                        .collect(),
                };
                Ok(vec![BoardEvent::Opened {
                    board: board.clone(),
                    lanes: default_lanes,
                }])
            }
            BoardCommand::CardUpsert(CardUpsert {
                board,
                card,
                expected,
                patch,
                ..
            }) => {
                let mut out = Vec::new();
                if state.board.is_none() {
                    let default_lanes = ["todo", "doing", "review", "done"]
                        .into_iter()
                        .map(|s| LaneName::parse(s).expect("lane"))
                        .collect();
                    out.push(BoardEvent::Opened {
                        board: board.clone(),
                        lanes: default_lanes,
                    });
                }
                match card {
                    None => {
                        let next_no = CardNo((state.cards.len() as u32) + 1);
                        let title = patch
                            .title
                            .clone()
                            .ok_or_else(|| ApiError::conflict("missing card title"))?;
                        let lane = patch
                            .lane
                            .clone()
                            .unwrap_or_else(|| LaneName::parse("todo").expect("todo"));
                        let assignee = match &patch.assignee {
                            Some(Assign::To { who }) => Some(who.clone()),
                            _ => None,
                        };
                        out.push(BoardEvent::CardCreated {
                            card: Card {
                                no: next_no,
                                revision: CardRevision(1),
                                org_id: patch.org_id.clone(),
                                title,
                                lane,
                                assignee,
                                priority: patch.priority.unwrap_or(Priority::Normal),
                                due_ms: patch.due_ms,
                                labels: patch.labels_add.clone(),
                                links: patch.links_add.clone(),
                                needs: patch.needs_add.clone(),
                            },
                        });
                        Ok(out)
                    }
                    Some(no) => {
                        let existing = state.cards.get(no).ok_or_else(|| ApiError::NotFound {
                            what: format!("card {}", no.0),
                        })?;
                        if let Some(exp) = expected
                            && &existing.revision != exp
                        {
                            return Err(ApiError::conflict(format!(
                                "expected rev {}, got {}",
                                exp.0, existing.revision.0
                            )));
                        }
                        out.push(BoardEvent::CardPatched {
                            no: *no,
                            revision: existing.revision.next(),
                            patch: patch.clone(),
                        });
                        Ok(out)
                    }
                }
            }
            BoardCommand::CardArchive(arch) => {
                let existing = state
                    .cards
                    .get(&arch.card)
                    .ok_or_else(|| ApiError::NotFound {
                        what: format!("card {}", arch.card.0),
                    })?;
                Ok(vec![BoardEvent::CardArchived {
                    no: arch.card,
                    revision: existing.revision.next(),
                }])
            }
            BoardCommand::BoardView(_) => {
                if state.board.is_none() {
                    return Err(ApiError::NotFound {
                        what: "board".into(),
                    });
                }
                Ok(Vec::new())
            }
        }
    }

    fn evolve(state: &mut Self::State, event: &Self::Event, meta: &EntryMeta) {
        state.head = meta.seq.0;
        match event {
            BoardEvent::Opened { board, lanes } => {
                state.board = Some(board.clone());
                state.lanes = lanes.clone();
            }
            BoardEvent::CardCreated { card } => {
                let board = state.board.clone().expect("board set");
                state.last_card = Some(CardRef {
                    board,
                    no: card.no,
                    revision: card.revision,
                });
                state.cards.insert(card.no, card.clone());
            }
            BoardEvent::CardPatched {
                no,
                revision,
                patch,
            } => {
                if let Some(c) = state.cards.get_mut(no) {
                    c.revision = *revision;
                    if let Some(t) = &patch.title {
                        c.title = t.clone();
                    }
                    if let Some(l) = &patch.lane {
                        c.lane = l.clone();
                    }
                    if let Some(a) = &patch.assignee {
                        c.assignee = match a {
                            Assign::To { who } => Some(who.clone()),
                            Assign::Clear => None,
                        };
                    }
                    let board = state.board.clone().expect("board set");
                    state.last_card = Some(CardRef {
                        board,
                        no: *no,
                        revision: *revision,
                    });
                }
            }
            BoardEvent::CardArchived { no, .. } => {
                state.cards.remove(no);
            }
        }
    }

    fn reply(state: &Self::State, cmd: Self::Cmd, _committed: &[Envelope]) -> WireReply {
        let board = state.board.clone().expect("board exists");
        match cmd {
            BoardCommand::BoardEnsure(_) => WireReply::BoardEnsure(BoardInfo {
                board,
                lanes: state.lanes.clone(),
                head: state.head,
            }),
            BoardCommand::CardUpsert(_) => {
                WireReply::CardUpsert(state.last_card.clone().expect("card ref"))
            }
            BoardCommand::CardArchive(_) => WireReply::CardArchive(Ack),
            BoardCommand::BoardView(BoardView { filter, .. }) => {
                let lanes = state
                    .lanes
                    .iter()
                    .map(|lane| {
                        let cards = state
                            .cards
                            .values()
                            .filter(|c| &c.lane == lane)
                            .filter(|c| match &filter {
                                Some(f) => f
                                    .assignee
                                    .as_ref()
                                    .is_none_or(|a| c.assignee.as_ref() == Some(a)),
                                None => true,
                            })
                            .cloned()
                            .collect();
                        LaneCards {
                            lane: lane.clone(),
                            cards,
                        }
                    })
                    .collect();
                WireReply::BoardView(BoardSnapshot {
                    board,
                    head: state.head,
                    lanes,
                })
            }
        }
    }

    fn wrap(event: Self::Event) -> Event {
        Event::Board(event)
    }

    fn unwrap(event: &Event) -> Option<&Self::Event> {
        match event {
            Event::Board(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Default)]
struct TestWaveState {
    spec: Option<wave::WaveSpec>,
    room: Option<RoomId>,
    board: Option<BoardId>,
}

struct TestWaves;

impl Decider for TestWaves {
    type State = TestWaveState;
    type Cmd = WaveCommand;
    type Event = WaveEvent;

    fn address(cmd: &Self::Cmd, _ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        let wave = match cmd {
            WaveCommand::WaveStart(c) => c.spec.slug.clone(),
            WaveCommand::WaveAddWorker(c) => c.wave.clone(),
            WaveCommand::WaveRetireWorker(c) => c.wave.clone(),
            WaveCommand::WaveTell(c) => c.wave.clone(),
            WaveCommand::WaveResolve(c) => c.wave.clone(),
            WaveCommand::WaveFinish(c) => c.wave.clone(),
            WaveCommand::WaveObserve(c) => c.wave.clone(),
            WaveCommand::WaveView(c) => c.wave.clone(),
        };
        Ok(StreamId::Wave { wave })
    }

    fn idem(_cmd: &Self::Cmd) -> Option<IdemKey> {
        None
    }

    fn decide(
        state: &Self::State,
        cmd: &Self::Cmd,
        _ctx: &DecideCtx,
    ) -> Result<Vec<Self::Event>, ApiError> {
        match cmd {
            WaveCommand::WaveStart(start) => {
                if state.spec.is_some() {
                    return Ok(Vec::new());
                }
                let room = RoomId::Channel {
                    scope: RoomScope::Project {
                        project: start.spec.project.clone(),
                    },
                    name: ChannelName::parse(format!("wave-{}", start.spec.slug))
                        .expect("wave channel"),
                };
                let board = rex_wave_api::board::project_board(&start.spec.project);
                Ok(vec![WaveEvent::Started {
                    spec: start.spec.clone(),
                    room,
                    board,
                }])
            }
            WaveCommand::WaveObserve(obs) => Ok(vec![WaveEvent::WorkerObserved {
                role: obs.role.clone(),
                observation: obs.observation.clone(),
            }]),
            _ => Ok(Vec::new()),
        }
    }

    fn evolve(state: &mut Self::State, event: &Self::Event, _meta: &EntryMeta) {
        if let WaveEvent::Started { spec, room, board } = event {
            state.spec = Some(spec.clone());
            state.room = Some(room.clone());
            state.board = Some(board.clone());
        }
    }

    fn reply(state: &Self::State, cmd: Self::Cmd, _committed: &[Envelope]) -> WireReply {
        match cmd {
            WaveCommand::WaveStart(start) => WireReply::WaveStart(WaveStarted {
                wave: start.spec.slug,
                room: state.room.clone().expect("wave room"),
                board: state.board.clone().expect("wave board"),
            }),
            WaveCommand::WaveView(_) => WireReply::WaveView(WaveSnapshot {
                spec: state.spec.clone().expect("spec"),
                status: WaveStatus::Running,
                workers: Vec::new(),
                counts: rex_wave_api::wave::WaveCounts::default(),
                worker_meta: std::collections::BTreeMap::new(),
            }),
            WaveCommand::WaveAddWorker(_) => WireReply::WaveAddWorker(Ack),
            WaveCommand::WaveRetireWorker(_) => WireReply::WaveRetireWorker(Ack),
            WaveCommand::WaveTell(_) => WireReply::WaveTell(Ack),
            WaveCommand::WaveResolve(_) => WireReply::WaveResolve(Ack),
            WaveCommand::WaveFinish(_) => WireReply::WaveFinish(Ack),
            WaveCommand::WaveObserve(_) => WireReply::WaveObserve(Ack),
        }
    }

    fn wrap(event: Self::Event) -> Event {
        Event::Wave(event)
    }

    fn unwrap(event: &Event) -> Option<&Self::Event> {
        match event {
            Event::Wave(e) => Some(e),
            _ => None,
        }
    }
}

struct TestHarness {
    _dir: tempfile::TempDir,
    journal: Journal,
    router: Arc<Router>,
}

impl TestHarness {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(&dir.path().join("journal.sqlite")).expect("open journal");
        let hub = Hub::new(256);
        let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
        let read = Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar,
            agents: Arc::new(NoAgentFeed),
        });
        let router = Arc::new(Router {
            room: Registry::<TestRooms>::new(journal.clone(), hub.clone()),
            wave: Registry::<TestWaves>::new(journal.clone(), hub.clone()),
            board: Registry::<TestBoards>::new(journal.clone(), hub),
            inbox: Arc::new(Unavailable(Family::Inbox)),
            mail: Arc::new(Unavailable(Family::Mail)),
            presence: Arc::new(Unavailable(Family::Presence)),
            read,
        });
        TestHarness {
            _dir: dir,
            journal,
            router,
        }
    }

    fn client(&self, me: Principal) -> LocalClient {
        LocalClient::new(self.router.clone(), me)
    }
}

fn seat_principal(name: &str) -> Principal {
    Principal::Seat {
        seat: SeatName::parse(name).expect("valid seat"),
    }
}

fn operator_principal(name: &str) -> Principal {
    Principal::Operator {
        seat: SeatName::parse(name).expect("valid operator seat"),
    }
}

fn worker_principal(wave: &str, role: &str) -> Principal {
    Principal::Worker {
        wave: WaveSlug::parse(wave).expect("valid wave"),
        role: RoleName::parse(role).expect("valid role"),
    }
}

fn parse_tool_result_json(res: &rex_wave_mcp::ToolResult) -> Value {
    let text = res.content[0]["text"].as_str().expect("text content");
    serde_json::from_str(text).expect("valid JSON in ToolResult")
}

#[tokio::test]
async fn audience_filtering_blocks_operator_and_system_tools_atomically_and_in_codemode() {
    let harness = TestHarness::new();
    let seat_srv = McpServer::new(harness.client(seat_principal("sam")));
    let worker_srv = McpServer::new(harness.client(worker_principal("auth-v2", "verifier")));
    let op_srv = McpServer::new(harness.client(operator_principal("owner")));

    let seat_tools: Vec<String> = seat_srv.tools().into_iter().map(|t| t.name).collect();
    let worker_tools: Vec<String> = worker_srv.tools().into_iter().map(|t| t.name).collect();
    let op_tools: Vec<String> = op_srv.tools().into_iter().map(|t| t.name).collect();

    assert_eq!(
        seat_tools, worker_tools,
        "Seat and Worker share Member tool visibility"
    );
    assert!(seat_tools.contains(&RUN_CODE_TOOL.to_string()));
    assert!(seat_tools.contains(&"room_post".to_string()));

    for sys_tool in ["wave_observe", "inbox_raise", "mail_wake_bell"] {
        assert!(
            !seat_tools.contains(&sys_tool.to_string()),
            "Seat must not list System tool {sys_tool}"
        );
        assert!(
            !op_tools.contains(&sys_tool.to_string()),
            "Operator must not list System tool {sys_tool}"
        );
    }
    for op_tool in ["wave_start", "wave_resolve"] {
        assert!(
            !seat_tools.contains(&op_tool.to_string()),
            "Member must not list Operator tool {op_tool}"
        );
        assert!(
            op_tools.contains(&op_tool.to_string()),
            "Operator must list {op_tool}"
        );
    }

    let seat_run_code_desc = &seat_srv
        .tools()
        .into_iter()
        .find(|t| t.name == RUN_CODE_TOOL)
        .unwrap()
        .description;
    assert!(!seat_run_code_desc.contains("wave_start("));
    assert!(!seat_run_code_desc.contains("wave_observe("));
    let op_run_code_desc = &op_srv
        .tools()
        .into_iter()
        .find(|t| t.name == RUN_CODE_TOOL)
        .unwrap()
        .description;
    assert!(op_run_code_desc.contains("wave_start("));
    assert!(!op_run_code_desc.contains("wave_observe("));

    let wave_start_args = json!({
        "spec": {
            "slug": "auth-v2",
            "project": "proj_1",
            "goal": "ship auth",
            "defaults": {
                "provider": "codex",
                "model": "gpt-6.1-sol",
                "permission": "accept-edits",
                "workspace": { "workspace": "project_default" }
            },
            "workers": []
        }
    });

    let seat_wave_start = seat_srv
        .call_tool("wave_start", wave_start_args.clone())
        .await;
    assert!(
        seat_wave_start.is_error,
        "Member atomic wave_start must be denied"
    );

    let op_wave_start = op_srv
        .call_tool("wave_start", wave_start_args.clone())
        .await;
    assert!(
        !op_wave_start.is_error,
        "Operator atomic wave_start must succeed"
    );

    let op_wave_observe = op_srv
        .call_tool(
            "wave_observe",
            json!({
                "wave": "auth-v2",
                "role": "verifier",
                "observation": { "observed": "archived" }
            }),
        )
        .await;
    assert!(
        op_wave_observe.is_error,
        "System-only wave_observe must be denied even to Operator"
    );

    let seat_code_probe = seat_srv
        .call_tool(
            RUN_CODE_TOOL,
            json!({
                "code": r#"
                    let startThrew = false;
                    try {
                        await rex.wave_start({});
                    } catch (_) {
                        startThrew = true;
                    }
                    return {
                        waveStartSnake: typeof rex.wave_start,
                        waveStartCamel: typeof rex.waveStart,
                        waveObserveSnake: typeof rex.wave_observe,
                        waveObserveCamel: typeof rex.waveObserve,
                        inboxRaise: typeof rex.inbox_raise,
                        mailWakeBell: typeof rex.mail_wake_bell,
                        startThrew,
                    };
                "#
            }),
        )
        .await;
    assert!(!seat_code_probe.is_error);
    let probe_out = parse_tool_result_json(&seat_code_probe);
    assert_eq!(
        probe_out["value"],
        json!({
            "waveStartSnake": "undefined",
            "waveStartCamel": "undefined",
            "waveObserveSnake": "undefined",
            "waveObserveCamel": "undefined",
            "inboxRaise": "undefined",
            "mailWakeBell": "undefined",
            "startThrew": true,
        })
    );
}

#[tokio::test]
async fn codemode_executes_multistep_host_composition_with_unforgeable_principal() {
    let harness = TestHarness::new();
    let sam_client = harness.client(seat_principal("sam"));
    let sam_srv = McpServer::new(sam_client);

    let script = r#"
        const me = rex.whoami();
        const board = "project:proj_1";
        await rex.board_ensure({ board });
        await rex.card_upsert({
            board,
            patch: {
                title: "Implement MCP server",
                lane: "doing",
                assignee: { assign: "to", who: "@sam" }
            }
        });
        await rex.card_upsert({
            board,
            patch: {
                title: "Build TUI inspector",
                lane: "todo",
                assignee: { assign: "to", who: "@alice" }
            }
        });
        const snap = await rex.board_view({ board });
        const mine = snap.lanes
            .flatMap(l => l.cards)
            .filter(c => c.assignee === "@sam");
        for (const c of mine) {
            await rex.cardUpsert({
                board,
                card: c.no,
                expected: c.revision,
                patch: { lane: "review" }
            });
        }
        const room = { ref: "channel", scope: { scope: "project", project: "proj_1" }, name: "wave-mcp" };
        await rex.roomEnsure({ room });
        const posted = await rex.room_post({
            room,
            body: `@alice moved ${mine.length} card to review`,
            from: { kind: "seat", seat: "alice" }
        });
        console.log("handoff_seq", posted.seq);
        return {
            caller: me,
            movedCards: mine.map(c => c.no),
            postSeq: posted.seq,
            mentioned: posted.mentions.principals,
        };
    "#;

    let res = sam_srv
        .call_tool(RUN_CODE_TOOL, json!({ "code": script }))
        .await;
    assert!(!res.is_error, "multi-step script failed: {:?}", res.content);
    let out = parse_tool_result_json(&res);

    assert_eq!(
        out["value"]["caller"],
        json!({ "kind": "seat", "seat": "sam" })
    );
    assert_eq!(out["value"]["movedCards"], json!([1]));
    assert_eq!(
        out["value"]["mentioned"],
        json!([{ "kind": "seat", "seat": "alice" }])
    );
    assert_eq!(out["logs"], json!(["handoff_seq 3"]));
    assert_eq!(out["calls"].as_array().unwrap().len(), 7);

    let room_stream = StreamId::Room {
        room: RoomId::Channel {
            scope: RoomScope::Project {
                project: BbProjectId::parse("proj_1").unwrap(),
            },
            name: ChannelName::parse("wave-mcp").unwrap(),
        },
    };
    let entries = harness
        .journal
        .read(&room_stream, Seq::ORIGIN, 10)
        .expect("read journal");
    let last = entries.last().expect("posted entry");
    assert_eq!(
        last.by,
        seat_principal("sam"),
        "principal must come from session, never from JS args"
    );
}

#[tokio::test]
async fn codemode_supports_concurrent_promise_all_host_calls() {
    let harness = TestHarness::new();
    let sam_client = harness.client(seat_principal("sam"));
    let alice_client = harness.client(seat_principal("alice"));

    let room = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("coord").unwrap(),
    };
    sam_client
        .call(RoomEnsure {
            room: room.clone(),
            topic: None,
            origin: None,
        })
        .await
        .unwrap();
    alice_client
        .call(rex_wave_api::room::RoomJoin {
            room: room.clone(),
            member: None,
            handle: None,
        })
        .await
        .unwrap();

    let waiter = tokio::spawn(async move {
        run_code(
            &alice_client,
            r#"
                const page = await rex.wait({
                    interest: { interest: "mentions" },
                    after: { positions: [], from_head: true },
                    timeout_ms: 3000
                });
                return page.entries.length;
            "#,
            Budget::default(),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let poster_out = run_code(
        &sam_client,
        r#"
            const [p1, p2] = await Promise.all([
                rex.room_post({ room: { ref: "channel", scope: { scope: "global" }, name: "coord" }, body: "progress note" }),
                rex.room_post({ room: { ref: "channel", scope: { scope: "global" }, name: "coord" }, body: "@alice ready" })
            ]);
            return [p1.seq, p2.seq];
        "#,
        Budget::default(),
    )
    .await
    .expect("concurrent Promise.all posts");
    assert_eq!(poster_out.calls.len(), 2);

    let woke = waiter.await.expect("join waiter").expect("waiter outcome");
    assert!(woke.value.as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn codemode_interrupts_infinite_loops_and_cancels_slow_waits_at_deadline() {
    let harness = TestHarness::new();
    let sam = harness.client(seat_principal("sam"));
    let fast_budget = Budget {
        deadline: Duration::from_millis(25),
        ..Budget::default()
    };

    let t0 = Instant::now();
    let err = run_code(&sam, "while (true) {}", fast_budget.clone())
        .await
        .expect_err("infinite loop must fail");
    assert_eq!(err, CodeError::Deadline(Duration::from_millis(25)));
    assert!(
        t0.elapsed() < Duration::from_millis(500),
        "deadline took too long: {:?}",
        t0.elapsed()
    );

    let err_caught = run_code(
        &sam,
        r#"try { while (true) {} } catch (_) { return "swallowed"; }"#,
        fast_budget.clone(),
    )
    .await
    .expect_err("try/catch must not swallow C interrupt");
    assert_eq!(err_caught, CodeError::Deadline(Duration::from_millis(25)));

    let err_microtask = run_code(
        &sam,
        "while (true) { await Promise.resolve(); }",
        fast_budget,
    )
    .await
    .expect_err("infinite microtask loop must fail");
    assert_eq!(
        err_microtask,
        CodeError::Deadline(Duration::from_millis(25))
    );

    let wait_budget = Budget {
        deadline: Duration::from_millis(40),
        ..Budget::default()
    };
    let t_wait = Instant::now();
    let err_wait = run_code(
        &sam,
        r#"
            return await rex.wait({
                interest: { interest: "mentions" },
                after: { positions: [], from_head: true },
                timeout_ms: 60000
            });
        "#,
        wait_budget,
    )
    .await
    .expect_err("parked host wait must cancel at script deadline");
    assert_eq!(err_wait, CodeError::Deadline(Duration::from_millis(40)));
    assert!(t_wait.elapsed() < Duration::from_millis(500));

    let srv = McpServer::new(sam);
    let clamped = srv
        .call_tool(
            RUN_CODE_TOOL,
            json!({ "code": "return 7 * 6;", "timeout_ms": 999_999 }),
        )
        .await;
    assert!(!clamped.is_error);
    assert_eq!(parse_tool_result_json(&clamped)["value"], json!(42));
}

#[tokio::test]
async fn codemode_enforces_heap_cap_stack_cap_and_host_call_budget() {
    let harness = TestHarness::new();
    let sam = harness.client(seat_principal("sam"));

    let oom = run_code(
        &sam,
        "const buf = new Uint8Array(64 * 1024 * 1024); return buf.length;",
        Budget::default(),
    )
    .await
    .expect_err("64 MiB allocation must exceed 8 MiB heap cap");
    assert_eq!(oom, CodeError::OutOfMemory);

    let stack = run_code(
        &sam,
        "function recurse(n) { return 1 + recurse(n + 1); } return recurse(0);",
        Budget::default(),
    )
    .await
    .expect_err("unbounded recursion must hit 512 KiB stack cap");
    assert!(
        matches!(stack, CodeError::Thrown(_)),
        "expected Thrown on stack overflow, got {stack:?}"
    );

    let call_cap = Budget {
        max_host_calls: 2,
        ..Budget::default()
    };
    let capped = run_code(
        &sam,
        r#"
            for (let i = 0; i < 5; i++) {
                try { await rex.sidebar({}); } catch (_) {}
            }
            return "escaped";
        "#,
        call_cap,
    )
    .await
    .expect_err("exceeding max_host_calls must abort script");
    assert_eq!(capped, CodeError::HostCallBudget);

    let log_flood = run_code(
        &sam,
        r#"
            const big = "x".repeat(16384);
            for (let i = 0; i < 600; i++) {
                console.log(big);
            }
            return "done";
        "#,
        Budget::default(),
    )
    .await
    .expect("bounded console.log flood");
    assert_eq!(log_flood.logs.len(), 256);
    assert_eq!(log_flood.logs[0].len(), 4096);
}

#[tokio::test]
async fn codemode_has_zero_ambient_authority_frozen_rex_and_per_call_isolation() {
    let harness = TestHarness::new();
    let sam = harness.client(seat_principal("sam"));

    let probe = run_code(
        &sam,
        r#"
            globalThis.leakedAcrossCalls = "secret";
            try { globalThis.rex = { hacked: true }; } catch (_) {}
            try { delete globalThis.rex; } catch (_) {}
            try { globalThis.rex.extra = 123; } catch (_) {}
            return {
                rexFrozen: Object.isFrozen(globalThis.rex),
                consoleFrozen: Object.isFrozen(globalThis.console),
                whoamiFrozen: Object.isFrozen(rex.whoami()),
                extraInjected: "extra" in globalThis.rex,
                hackedReplaced: "hacked" in globalThis.rex,
                typeofProcess: typeof process,
                typeofRequire: typeof require,
                typeofFetch: typeof fetch,
                typeofDeno: typeof Deno,
                typeofBun: typeof Bun,
                typeofXhr: typeof XMLHttpRequest,
                typeofWs: typeof WebSocket,
                typeofBridge: typeof __rex_host_call,
            };
        "#,
        Budget::default(),
    )
    .await
    .expect("sandbox probe");

    assert_eq!(
        probe.value,
        json!({
            "rexFrozen": true,
            "consoleFrozen": true,
            "whoamiFrozen": true,
            "extraInjected": false,
            "hackedReplaced": false,
            "typeofProcess": "undefined",
            "typeofRequire": "undefined",
            "typeofFetch": "undefined",
            "typeofDeno": "undefined",
            "typeofBun": "undefined",
            "typeofXhr": "undefined",
            "typeofWs": "undefined",
            "typeofBridge": "undefined",
        })
    );

    let second = run_code(
        &sam,
        "return typeof globalThis.leakedAcrossCalls;",
        Budget::default(),
    )
    .await
    .expect("second isolated call");
    assert_eq!(second.value, json!("undefined"));
}

#[tokio::test]
async fn mcp_duplex_stream_full_lifecycle_and_live_resource_subscriptions() {
    let harness = TestHarness::new();
    let sam_client = harness.client(seat_principal("sam"));
    let alice_client = harness.client(seat_principal("alice"));

    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_io);
    let (cli_r, mut cli_w) = tokio::io::split(client_io);
    let mut cli_lines = BufReader::new(cli_r).lines();

    let server_task =
        tokio::spawn(async move { McpServer::new(sam_client).serve_stream(srv_r, srv_w).await });

    async fn send_msg<W: tokio::io::AsyncWrite + Unpin>(w: &mut W, val: Value) {
        let mut line = serde_json::to_vec(&val).unwrap();
        line.push(b'\n');
        w.write_all(&line).await.unwrap();
        w.flush().await.unwrap();
    }

    async fn recv_msg<R: tokio::io::AsyncBufRead + Unpin>(
        lines: &mut tokio::io::Lines<R>,
    ) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(3), lines.next_line())
            .await
            .expect("timed out waiting for MCP message")
            .expect("read line io")
            .expect("unexpected EOF");
        serde_json::from_str(&line).expect("valid JSON-RPC line")
    }

    send_msg(
        &mut cli_w,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": { "name": "test-client", "version": "0.1.0" }
            }
        }),
    )
    .await;
    let init_resp = recv_msg(&mut cli_lines).await;
    assert_eq!(init_resp["id"], 1);
    assert_eq!(init_resp["result"]["protocolVersion"], PROTOCOL_VERSION);
    assert_eq!(
        init_resp["result"]["capabilities"]["resources"]["subscribe"],
        true
    );

    send_msg(
        &mut cli_w,
        json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }),
    )
    .await;
    send_msg(
        &mut cli_w,
        json!({ "jsonrpc": "2.0", "id": 2, "method": "ping" }),
    )
    .await;
    let ping_resp = recv_msg(&mut cli_lines).await;
    assert_eq!(
        ping_resp["id"], 2,
        "notifications/initialized must not emit a response before ping"
    );
    assert_eq!(ping_resp["result"], json!({}));

    send_msg(
        &mut cli_w,
        json!({ "jsonrpc": "2.0", "id": 3, "method": "tools/list" }),
    )
    .await;
    let list_resp = recv_msg(&mut cli_lines).await;
    let tools = list_resp["result"]["tools"]
        .as_array()
        .expect("tools array");
    let run_code_entry = tools
        .iter()
        .find(|t| t["name"] == RUN_CODE_TOOL)
        .expect("run_code listed");
    assert_eq!(
        run_code_entry["inputSchema"]["properties"]["timeout_ms"]["maximum"],
        json!(MAX_RUN_CODE_TIMEOUT_MS)
    );
    assert!(
        run_code_entry["description"]
            .as_str()
            .unwrap()
            .contains("declare const rex: Readonly<{")
    );

    send_msg(
        &mut cli_w,
        json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "room_post",
                "arguments": {
                    "room": { "ref": "channel", "scope": { "scope": "project", "project": "proj_1" }, "name": "wave-mcp" },
                    "body": "initial room seed"
                }
            }
        }),
    )
    .await;
    let call_resp = recv_msg(&mut cli_lines).await;
    assert_eq!(call_resp["id"], 4);
    assert_eq!(call_resp["result"]["isError"], false);

    tokio::time::sleep(Duration::from_millis(20)).await;

    send_msg(
        &mut cli_w,
        json!({ "jsonrpc": "2.0", "id": 5, "method": "resources/list" }),
    )
    .await;
    let res_list = recv_msg(&mut cli_lines).await;
    let res_uris: Vec<&str> = res_list["result"]["resources"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["uri"].as_str())
        .collect();
    let room_uri = "rex://room/project/proj_1/wave-mcp";
    assert!(res_uris.contains(&"rex://sidebar"));
    assert!(
        res_uris.contains(&room_uri),
        "expected {room_uri} in {res_uris:?}"
    );

    send_msg(
        &mut cli_w,
        json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "resources/subscribe",
            "params": { "uri": "rex://sidebar" }
        }),
    )
    .await;
    assert_eq!(recv_msg(&mut cli_lines).await["id"], 6);

    send_msg(
        &mut cli_w,
        json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "resources/subscribe",
            "params": { "uri": room_uri }
        }),
    )
    .await;
    assert_eq!(recv_msg(&mut cli_lines).await["id"], 7);

    alice_client
        .call(RoomPost {
            room: RoomRef::Channel {
                scope: RoomScope::Project {
                    project: BbProjectId::parse("proj_1").unwrap(),
                },
                name: ChannelName::parse("wave-mcp").unwrap(),
            },
            body: Body::parse("@sam live update arrived").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .expect("alice post");

    let n1 = recv_msg(&mut cli_lines).await;
    let n2 = recv_msg(&mut cli_lines).await;
    assert_eq!(n1["method"], "notifications/resources/updated");
    assert_eq!(n2["method"], "notifications/resources/updated");
    let mut updated_uris = vec![
        n1["params"]["uri"].as_str().unwrap().to_owned(),
        n2["params"]["uri"].as_str().unwrap().to_owned(),
    ];
    updated_uris.sort();
    assert_eq!(
        updated_uris,
        vec!["rex://room/project/proj_1/wave-mcp", "rex://sidebar"]
    );

    send_msg(
        &mut cli_w,
        json!({
            "jsonrpc": "2.0",
            "id": 8,
            "method": "resources/read",
            "params": { "uri": room_uri }
        }),
    )
    .await;
    let read_resp = recv_msg(&mut cli_lines).await;
    assert_eq!(read_resp["id"], 8);
    let page_json: Value =
        serde_json::from_str(read_resp["result"]["contents"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(page_json["entries"].as_array().unwrap().len(), 3);

    send_msg(
        &mut cli_w,
        json!({
            "jsonrpc": "2.0",
            "id": 9,
            "method": "resources/unsubscribe",
            "params": { "uri": room_uri }
        }),
    )
    .await;
    assert_eq!(recv_msg(&mut cli_lines).await["id"], 9);
    send_msg(
        &mut cli_w,
        json!({
            "jsonrpc": "2.0",
            "id": 10,
            "method": "resources/unsubscribe",
            "params": { "uri": "rex://sidebar" }
        }),
    )
    .await;
    assert_eq!(recv_msg(&mut cli_lines).await["id"], 10);

    alice_client
        .call(RoomPost {
            room: RoomRef::Channel {
                scope: RoomScope::Project {
                    project: BbProjectId::parse("proj_1").unwrap(),
                },
                name: ChannelName::parse("wave-mcp").unwrap(),
            },
            body: Body::parse("after unsubscribe").unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .expect("second alice post");

    send_msg(
        &mut cli_w,
        json!({ "jsonrpc": "2.0", "id": 11, "method": "ping" }),
    )
    .await;
    let after_unsub = recv_msg(&mut cli_lines).await;
    assert_eq!(
        after_unsub["id"], 11,
        "unsubscribed resources must not emit notifications"
    );

    cli_w.shutdown().await.unwrap();
    drop(cli_w);
    server_task
        .await
        .expect("server join")
        .expect("clean server exit");
}

#[tokio::test]
async fn mcp_supports_content_length_framing_and_loopback_http() {
    let harness = TestHarness::new();
    let sam = harness.client(seat_principal("sam"));

    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_io);
    let (mut cli_r, mut cli_w) = tokio::io::split(client_io);

    let srv_cl = sam.clone();
    let stream_task =
        tokio::spawn(async move { McpServer::new(srv_cl).serve_stream(srv_r, srv_w).await });

    let req_bytes = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": 77,
        "method": "tools/call",
        "params": {
            "name": "run_code",
            "arguments": { "code": "return rex.whoami().seat;" }
        }
    }))
    .unwrap();
    let header = format!("Content-Length: {}\r\n\r\n", req_bytes.len());
    cli_w.write_all(header.as_bytes()).await.unwrap();
    cli_w.write_all(&req_bytes).await.unwrap();
    cli_w.flush().await.unwrap();
    cli_w.shutdown().await.unwrap();
    drop(cli_w);

    let mut raw_resp = Vec::new();
    cli_r.read_to_end(&mut raw_resp).await.unwrap();
    let raw_str = std::str::from_utf8(&raw_resp).unwrap();
    let (hdr, body) = raw_str
        .split_once("\r\n\r\n")
        .expect("Content-Length response header");
    assert!(hdr.starts_with("Content-Length: "));
    let resp_json: Value = serde_json::from_str(body).unwrap();
    assert_eq!(resp_json["id"], 77);
    let code_res: Value =
        serde_json::from_str(resp_json["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(code_res["value"], json!("sam"));
    stream_task.await.unwrap().unwrap();

    let non_loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    let bind_err = McpServer::new(sam.clone())
        .serve_http(non_loopback)
        .await
        .unwrap_err();
    assert_eq!(bind_err.kind(), std::io::ErrorKind::InvalidInput);
    let unspec_listener = TcpListener::bind(non_loopback).await.unwrap();
    let listener_err = McpServer::new(sam.clone())
        .serve_http_listener(unspec_listener)
        .await
        .unwrap_err();
    assert_eq!(listener_err.kind(), std::io::ErrorKind::InvalidInput);

    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let http_task = tokio::spawn(async move {
        McpServer::new(sam)
            .with_http_token("cap-sam-token")
            .serve_http_listener(listener)
            .await
    });

    async fn raw_http(addr: SocketAddr, raw: &str) -> String {
        let mut conn = TcpStream::connect(addr).await.unwrap();
        conn.write_all(raw.as_bytes()).await.unwrap();
        let mut resp = Vec::new();
        conn.read_to_end(&mut resp).await.unwrap();
        String::from_utf8(resp).unwrap()
    }

    let body_bytes = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {}
    }))
    .unwrap();
    let body_str = std::str::from_utf8(&body_bytes).unwrap();

    let bad_origin = raw_http(
        addr,
        &format!(
            "POST /mcp HTTP/1.1\r\nHost: localhost\r\nOrigin: https://evil.example\r\nAuthorization: Bearer cap-sam-token\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body_str}",
            body_bytes.len()
        ),
    )
    .await;
    assert!(
        bad_origin.starts_with("HTTP/1.1 403 Forbidden\r\n"),
        "got: {bad_origin}"
    );

    let bad_host = raw_http(
        addr,
        &format!(
            "POST /mcp HTTP/1.1\r\nHost: evil.example\r\nAuthorization: Bearer cap-sam-token\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body_str}",
            body_bytes.len()
        ),
    )
    .await;
    assert!(
        bad_host.starts_with("HTTP/1.1 403 Forbidden\r\n"),
        "got: {bad_host}"
    );

    let missing_auth = raw_http(
        addr,
        &format!(
            "POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body_str}",
            body_bytes.len()
        ),
    )
    .await;
    assert!(
        missing_auth.starts_with("HTTP/1.1 401 Unauthorized\r\n"),
        "got: {missing_auth}"
    );

    let bad_ct = raw_http(
        addr,
        &format!(
            "POST /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer cap-sam-token\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body_str}",
            body_bytes.len()
        ),
    )
    .await;
    assert!(
        bad_ct.starts_with("HTTP/1.1 415 Unsupported Media Type\r\n"),
        "got: {bad_ct}"
    );

    let http_str = raw_http(
        addr,
        &format!(
            "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nOrigin: http://localhost:3000\r\nAuthorization: Bearer cap-sam-token\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\n\r\n{body_str}",
            addr.port(),
            body_bytes.len()
        ),
    )
    .await;
    assert!(
        http_str.starts_with("HTTP/1.1 200 OK\r\n"),
        "got: {http_str}"
    );
    let (_, json_body) = http_str.split_once("\r\n\r\n").unwrap();
    let parsed_http: Value = serde_json::from_str(json_body).unwrap();
    assert_eq!(parsed_http["result"]["protocolVersion"], PROTOCOL_VERSION);

    http_task.abort();
}

#[tokio::test]
async fn mcp_stream_reader_preserves_partial_frames_across_concurrent_tool_completion() {
    let harness = TestHarness::new();
    let sam = harness.client(seat_principal("sam"));

    let (client_io, server_io) = tokio::io::duplex(256 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_io);
    let (cli_r, mut cli_w) = tokio::io::split(client_io);
    let mut cli_lines = BufReader::new(cli_r).lines();

    let server_task =
        tokio::spawn(async move { McpServer::new(sam).serve_stream(srv_r, srv_w).await });

    let slow_call = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "wait",
            "arguments": {
                "interest": { "interest": "mentions" },
                "after": { "positions": [], "from_head": true },
                "timeout_ms": 35
            }
        }
    }))
    .unwrap();
    cli_w.write_all(&slow_call).await.unwrap();
    cli_w.write_all(b"\n").await.unwrap();
    cli_w.flush().await.unwrap();

    let large_body = "a".repeat(48 * 1024);
    let second_call = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "room_post",
            "arguments": {
                "room": { "ref": "channel", "scope": { "scope": "global" }, "name": "general" },
                "body": large_body
            }
        }
    }))
    .unwrap();
    let split_at = 24 * 1024;
    cli_w.write_all(&second_call[..split_at]).await.unwrap();
    cli_w.flush().await.unwrap();

    let first_line = tokio::time::timeout(Duration::from_secs(2), cli_lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let first_resp: Value = serde_json::from_str(&first_line).unwrap();
    assert_eq!(first_resp["id"], 1);

    cli_w.write_all(&second_call[split_at..]).await.unwrap();
    cli_w.write_all(b"\n").await.unwrap();
    cli_w.flush().await.unwrap();

    let second_line = tokio::time::timeout(Duration::from_secs(2), cli_lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let second_resp: Value = serde_json::from_str(&second_line).unwrap();
    assert_eq!(
        second_resp["id"], 2,
        "partial frame must survive concurrent rpc_tasks completion: {second_resp}"
    );
    assert_eq!(second_resp["result"]["isError"], false);

    cli_w.shutdown().await.unwrap();
    drop(cli_w);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn resources_read_returns_latest_200_entries_for_long_rooms() {
    let harness = TestHarness::new();
    let sam = harness.client(seat_principal("sam"));
    let room = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("busy").unwrap(),
    };
    for i in 1..=210u32 {
        sam.call(RoomPost {
            room: room.clone(),
            body: Body::parse(format!("entry-{i}")).unwrap(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None,
        })
        .await
        .unwrap();
    }

    let (client_io, server_io) = tokio::io::duplex(256 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_io);
    let (cli_r, mut cli_w) = tokio::io::split(client_io);
    let mut cli_lines = BufReader::new(cli_r).lines();

    let server_task =
        tokio::spawn(async move { McpServer::new(sam).serve_stream(srv_r, srv_w).await });

    let req = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "resources/read",
        "params": { "uri": "rex://room/channel/busy" }
    }))
    .unwrap();
    cli_w.write_all(&req).await.unwrap();
    cli_w.write_all(b"\n").await.unwrap();
    cli_w.flush().await.unwrap();

    let resp_line = tokio::time::timeout(Duration::from_secs(2), cli_lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let resp: Value = serde_json::from_str(&resp_line).unwrap();
    let page: Value =
        serde_json::from_str(resp["result"]["contents"][0]["text"].as_str().unwrap()).unwrap();
    let entries = page["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 200);
    assert_eq!(entries.first().unwrap()["seq"], json!(12));
    assert_eq!(entries.last().unwrap()["seq"], json!(211));
    assert_eq!(
        entries.last().unwrap()["event"]["event"]["body"],
        json!("entry-210")
    );

    cli_w.shutdown().await.unwrap();
    drop(cli_w);
    server_task.await.unwrap().unwrap();
}
