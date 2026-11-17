//! In-process fixture `Client` and headless `--self-test` verification harness.

use crate::{
    Effect, Model, Msg, ReplyTag, WatchSlot,
    input::{Action, Dir, HitMap},
    kitty::{
        CARD_APPROVAL_IMAGE, CONTEXT_BAR_IMAGE, CellPx, DIACRITICS, GraphicsMode, KittyCompositor,
        ProgramState, TermOut, clip, encode, program_status, render_backdrop,
    },
    sidebar::NodeKey,
    stage::{CONTEXT_BAR_COLS, StageTarget, plan_for, target_for},
    update, view,
};
use ratatui::{
    Terminal,
    backend::{Backend as _, CrosstermBackend, TestBackend},
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier},
};
use rex_wave_api::{
    board::{
        Assign, Assignee, BoardInfo, BoardSnapshot, Card, CardNo, CardRef, CardRevision, LaneCards,
        LaneName, Priority, Title, project_board,
    },
    mail::{LetterRef, MailRef, MailSchemaV1, MailboxItem, WakeId},
    notify::{
        InboxPage, Notification, NotificationDraft, NotificationId, NotifyAction, NotifyKind,
        NotifyState, Rank,
    },
    read::{
        AgentEvent, AgentEventKind, BoardBadge, ChannelLeaf, ContextCategory, ContextCategoryKind,
        DmLeaf, InboxBadge, Page, ProjectBranch, SidebarTreeSnapshot, ThreadContextUsage,
        ThreadPageView, WaveRoomBranch, WaveWorkerLeaf,
    },
    room::{
        Body, Mentions, Posted, PrincipalPair, RoomEvent, RoomId, RoomInfo, RoomOrigin, RoomScope,
    },
    stage::derive_osc7501_status,
    wave::{
        Activity, AgentSpec, BaseBranch, InteractionKind, InteractionResolution,
        PendingInteraction, PermissionMode, WaveSnapshot, WaveSpec, WaveStarted, WaveStatus,
        WorkerPhase, WorkerSpec, Workspace,
    },
    *,
};
use std::{
    collections::BTreeMap,
    future::Future,
    io,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::{mpsc, watch};

#[derive(Clone)]
pub struct DemoClient {
    me: Principal,
    inner: Arc<Mutex<DemoState>>,
    sidebar_tx: Arc<watch::Sender<SidebarTreeSnapshot>>,
}

struct DemoState {
    project: BbProjectId,
    wave: WaveSlug,
    wave_room: RoomId,
    general_room: RoomId,
    lounge_room: RoomId,
    sol_dm: RoomId,
    astra_dm: RoomId,
    board: BoardSnapshot,
    inbox: InboxPage,
    rooms: BTreeMap<RoomId, Vec<Envelope>>,
    workers: BTreeMap<RoleName, WorkerRecord>,
    generation: FocusGeneration,
}

#[derive(Clone)]
struct WorkerRecord {
    provider: ProviderId,
    model: Option<ModelId>,
    phase: WorkerPhase,
    page: ThreadPageView,
}

impl Default for DemoClient {
    fn default() -> Self {
        Self::new()
    }
}

impl DemoClient {
    pub fn new() -> Self {
        let owner = SeatName::parse("owner").expect("seat");
        let me = Principal::Operator {
            seat: owner.clone(),
        };
        let sol = Principal::Seat {
            seat: SeatName::parse("sol").expect("seat"),
        };
        let astra = Principal::Seat {
            seat: SeatName::parse("astra").expect("seat"),
        };
        let project = BbProjectId::parse("p_123").expect("project");
        let wave = WaveSlug::parse("auth-v2").expect("wave");

        let wave_room = RoomId::Channel {
            scope: RoomScope::Project {
                project: project.clone(),
            },
            name: ChannelName::parse("wave-auth-v2").expect("channel"),
        };
        let general_room = RoomId::Channel {
            scope: RoomScope::Project {
                project: project.clone(),
            },
            name: ChannelName::parse("general").expect("channel"),
        };
        let lounge_room = RoomId::Channel {
            scope: RoomScope::Global,
            name: ChannelName::parse("lounge").expect("channel"),
        };
        let sol_dm = RoomId::Direct {
            pair: PrincipalPair::new(me.clone(), sol.clone()).expect("pair"),
        };
        let astra_dm = RoomId::Direct {
            pair: PrincipalPair::new(me.clone(), astra.clone()).expect("pair"),
        };

        let author_role = RoleName::parse("author-jwt").expect("role");
        let migrator_role = RoleName::parse("migrator").expect("role");
        let interaction_id = BbInteractionId::parse("int_8f2").expect("interaction");
        let pending_interaction = PendingInteraction {
            id: interaction_id.clone(),
            kind: InteractionKind::Permission,
            summary: "Write migrations/004_oauth_refresh.sql in git-worktree".to_owned(),
        };

        let author_usage = ThreadContextUsage {
            used_tokens: 42_000,
            model_context_window: 200_000,
            estimated: false,
            auto_compact_at_tokens: Some(160_000),
            cost_usd: Some(0.38),
            categories: vec![
                ContextCategory {
                    id: "msgs".to_owned(),
                    label: "Messages".to_owned(),
                    kind: ContextCategoryKind::Used,
                    tokens: 26_000,
                },
                ContextCategory {
                    id: "tools".to_owned(),
                    label: "Tools".to_owned(),
                    kind: ContextCategoryKind::Used,
                    tokens: 16_000,
                },
                ContextCategory {
                    id: "sys".to_owned(),
                    label: "Reserved".to_owned(),
                    kind: ContextCategoryKind::Reserved,
                    tokens: 12_000,
                },
                ContextCategory {
                    id: "free".to_owned(),
                    label: "Free".to_owned(),
                    kind: ContextCategoryKind::Free,
                    tokens: 146_000,
                },
            ],
        };

        let migrator_usage = ThreadContextUsage {
            used_tokens: 128_000,
            model_context_window: 200_000,
            estimated: false,
            auto_compact_at_tokens: Some(160_000),
            cost_usd: Some(1.14),
            categories: vec![
                ContextCategory {
                    id: "msgs".to_owned(),
                    label: "Messages".to_owned(),
                    kind: ContextCategoryKind::Used,
                    tokens: 88_000,
                },
                ContextCategory {
                    id: "tools".to_owned(),
                    label: "Tools".to_owned(),
                    kind: ContextCategoryKind::Used,
                    tokens: 40_000,
                },
                ContextCategory {
                    id: "free".to_owned(),
                    label: "Free".to_owned(),
                    kind: ContextCategoryKind::Free,
                    tokens: 72_000,
                },
            ],
        };

        let mut workers = BTreeMap::new();
        workers.insert(
            author_role.clone(),
            WorkerRecord {
                provider: ProviderId::parse("codex").expect("provider"),
                model: Some(ModelId::parse("gpt-6.1-sol").expect("model")),
                phase: WorkerPhase::Live {
                    thread: BbThreadId::parse("thr_jwt_01").expect("thread"),
                    activity: Activity::Working,
                },
                page: ThreadPageView {
                    thread: BbThreadId::parse("thr_jwt_01").expect("thread"),
                    activity: Activity::Working,
                    events: vec![
                        AgentEvent {
                            seq: BbSeq(1),
                            at: UnixMs(1_700_000_100),
                            kind: AgentEventKind::TurnStarted,
                        },
                        AgentEvent {
                            seq: BbSeq(2),
                            at: UnixMs(1_700_000_120),
                            kind: AgentEventKind::UserMessage {
                                text: "Implement EdDSA refresh token rotation and post slice #2.".to_owned(),
                            },
                        },
                        AgentEvent {
                            seq: BbSeq(3),
                            at: UnixMs(1_700_000_250),
                            kind: AgentEventKind::ToolCall {
                                name: "cargo_test".to_owned(),
                                summary: "-p auth-jwt --offline".to_owned(),
                            },
                        },
                        AgentEvent {
                            seq: BbSeq(4),
                            at: UnixMs(1_700_000_610),
                            kind: AgentEventKind::ToolResult {
                                name: "cargo_test".to_owned(),
                                ok: true,
                                summary: "14 passed; 0 failed".to_owned(),
                            },
                        },
                        AgentEvent {
                            seq: BbSeq(5),
                            at: UnixMs(1_700_000_700),
                            kind: AgentEventKind::AssistantText {
                                text: "Replay-window checks are green; notifying @verifier in #wave-auth-v2.".to_owned(),
                            },
                        },
                    ],
                    usage: Some(author_usage),
                    pending: None,
                    next: Some(BbSeq(6)),
                },
            },
        );
        workers.insert(
            migrator_role.clone(),
            WorkerRecord {
                provider: ProviderId::parse("claude-code").expect("provider"),
                model: Some(ModelId::parse("claude-opus-5-5").expect("model")),
                phase: WorkerPhase::Live {
                    thread: BbThreadId::parse("thr_mig_02").expect("thread"),
                    activity: Activity::Blocked {
                        interaction: pending_interaction.clone(),
                    },
                },
                page: ThreadPageView {
                    thread: BbThreadId::parse("thr_mig_02").expect("thread"),
                    activity: Activity::Blocked {
                        interaction: pending_interaction.clone(),
                    },
                    events: vec![
                        AgentEvent {
                            seq: BbSeq(1),
                            at: UnixMs(1_700_000_150),
                            kind: AgentEventKind::TurnStarted,
                        },
                        AgentEvent {
                            seq: BbSeq(2),
                            at: UnixMs(1_700_000_320),
                            kind: AgentEventKind::ToolCall {
                                name: "write_file".to_owned(),
                                summary: "migrations/004_oauth_refresh.sql".to_owned(),
                            },
                        },
                    ],
                    usage: Some(migrator_usage),
                    pending: Some(pending_interaction),
                    next: Some(BbSeq(3)),
                },
            },
        );

        let approve_cmd = WireCommand::WaveResolve(wave::WaveResolve {
            wave: wave.clone(),
            role: migrator_role.clone(),
            interaction: interaction_id.clone(),
            resolution: InteractionResolution::AllowOnce {
                granted_permissions: None,
            },
        });
        let deny_cmd = WireCommand::WaveResolve(wave::WaveResolve {
            wave: wave.clone(),
            role: migrator_role.clone(),
            interaction: interaction_id.clone(),
            resolution: InteractionResolution::Deny,
        });

        let sha = Sha256Hex::parse("a".repeat(64)).expect("sha256");
        let mail_item = MailboxItem {
            schema: MailSchemaV1,
            id: "auth-v2-ruling-r1".to_owned(),
            from: "@sol".to_owned(),
            to: "@owner".to_owned(),
            kind: "letter".to_owned(),
            state: "unread".to_owned(),
            ts: "2026-11-16T10:20:00Z".to_owned(),
            ts_basis: "mtime".to_owned(),
            reference: MailRef {
                store: "sandbox".to_owned(),
                path: "to-owner/auth-v2-ruling-r1.md".into(),
                sha256: sha,
            },
            topic: Some("auth-v2-ruling".to_owned()),
            round: Some(1),
            label: Some("Ruling on EdDSA token expiry".to_owned()),
        };

        let inbox = InboxPage {
            head: Seq(3),
            unread: 3,
            cursors: Vec::new(),
            items: vec![
                Notification {
                    id: NotificationId(1),
                    at: UnixMs(1_700_000_800),
                    rank: Rank(950),
                    state: NotifyState::Unread,
                    draft: NotificationDraft {
                        kind: NotifyKind::Interaction {
                            wave: wave.clone(),
                            role: migrator_role.clone(),
                            interaction: interaction_id,
                        },
                        title: "@auth-v2/migrator requests file write permission".to_owned(),
                        preview: "Write migrations/004_oauth_refresh.sql inside git-worktree"
                            .to_owned(),
                        actions: vec![
                            NotifyAction {
                                label: "Approve".to_owned(),
                                key: Some('a'),
                                run: Box::new(approve_cmd),
                            },
                            NotifyAction {
                                label: "Deny".to_owned(),
                                key: Some('d'),
                                run: Box::new(deny_cmd),
                            },
                        ],
                        dedupe: "int:auth-v2:migrator:int_8f2".to_owned(),
                    },
                },
                Notification {
                    id: NotificationId(2),
                    at: UnixMs(1_700_000_720),
                    rank: Rank(720),
                    state: NotifyState::Unread,
                    draft: NotificationDraft {
                        kind: NotifyKind::Mention {
                            room: wave_room.clone(),
                            seq: Seq(2),
                        },
                        title: "@author-jwt mentioned @owner in #wave-auth-v2".to_owned(),
                        preview: "@owner @verifier JWT refresh rotation slice is ready (#2)"
                            .to_owned(),
                        actions: vec![NotifyAction {
                            label: "Dismiss".to_owned(),
                            key: Some('d'),
                            run: Box::new(WireCommand::InboxAck(notify::InboxAck {
                                ids: vec![NotificationId(2)],
                                state: NotifyState::Dismissed,
                            })),
                        }],
                        dedupe: "mention:wave-auth-v2:2".to_owned(),
                    },
                },
                Notification {
                    id: NotificationId(3),
                    at: UnixMs(1_700_000_500),
                    rank: Rank(610),
                    state: NotifyState::Unread,
                    draft: NotificationDraft {
                        kind: NotifyKind::Mail { item: mail_item },
                        title: "Sandbox Canon Letter · auth-v2-ruling (r1)".to_owned(),
                        preview: "From @sol: ratify 15m access token TTL + strict rotation"
                            .to_owned(),
                        actions: vec![NotifyAction {
                            label: "Dismiss".to_owned(),
                            key: Some('d'),
                            run: Box::new(WireCommand::InboxAck(notify::InboxAck {
                                ids: vec![NotificationId(3)],
                                state: NotifyState::Dismissed,
                            })),
                        }],
                        dedupe: "mail:auth-v2-ruling-r1".to_owned(),
                    },
                },
            ],
        };

        let board_id = project_board(&project);
        let board = BoardSnapshot {
            board: board_id,
            head: 4,
            lanes: vec![
                LaneCards {
                    lane: LaneName::parse("todo").expect("lane"),
                    cards: vec![
                        Card {
                            no: CardNo(1),
                            revision: CardRevision(1),
                            title: Title::parse("Wire OAuth refresh endpoint").expect("title"),
                            lane: LaneName::parse("todo").expect("lane"),
                            assignee: Some(
                                Assignee::parse("@auth-v2/author-jwt").expect("assignee"),
                            ),
                            priority: Priority::High,
                            due_ms: None,
                            labels: vec!["auth-v2".to_owned()],
                            links: Vec::new(),
                        },
                        Card {
                            no: CardNo(2),
                            revision: CardRevision(1),
                            title: Title::parse("Add SQLite WAL migration 004").expect("title"),
                            lane: LaneName::parse("todo").expect("lane"),
                            assignee: Some(Assignee::parse("@auth-v2/migrator").expect("assignee")),
                            priority: Priority::Blocking,
                            due_ms: None,
                            labels: vec!["db".to_owned()],
                            links: Vec::new(),
                        },
                    ],
                },
                LaneCards {
                    lane: LaneName::parse("doing").expect("lane"),
                    cards: vec![Card {
                        no: CardNo(3),
                        revision: CardRevision(2),
                        title: Title::parse("Rotate EdDSA signing keys").expect("title"),
                        lane: LaneName::parse("doing").expect("lane"),
                        assignee: Some(Assignee::parse("@auth-v2/author-jwt").expect("assignee")),
                        priority: Priority::Normal,
                        due_ms: None,
                        labels: Vec::new(),
                        links: Vec::new(),
                    }],
                },
                LaneCards {
                    lane: LaneName::parse("review").expect("lane"),
                    cards: vec![Card {
                        no: CardNo(4),
                        revision: CardRevision(1),
                        title: Title::parse("Audit token replay window").expect("title"),
                        lane: LaneName::parse("review").expect("lane"),
                        assignee: Some(Assignee::parse("@sol").expect("assignee")),
                        priority: Priority::Normal,
                        due_ms: None,
                        labels: Vec::new(),
                        links: Vec::new(),
                    }],
                },
                LaneCards {
                    lane: LaneName::parse("done").expect("lane"),
                    cards: Vec::new(),
                },
            ],
        };

        let mut rooms = BTreeMap::new();
        let author_principal = Principal::Worker {
            wave: wave.clone(),
            role: author_role,
        };
        rooms.insert(
            wave_room.clone(),
            vec![
                Envelope {
                    stream: StreamId::Room {
                        room: wave_room.clone(),
                    },
                    seq: Seq(1),
                    at: UnixMs(1_700_000_100),
                    by: me.clone(),
                    event: Event::Room(RoomEvent::Posted {
                        body: Body::parse("@all Launch wave auth-v2: ship refresh rotation with zero token replay.")
                            .expect("body"),
                        mentions: Mentions {
                            principals: Vec::new(),
                            all: true,
                        },
                        reply_to: None,
                        idem: None,
                    }),
                },
                Envelope {
                    stream: StreamId::Room {
                        room: wave_room.clone(),
                    },
                    seq: Seq(2),
                    at: UnixMs(1_700_000_720),
                    by: author_principal,
                    event: Event::Room(RoomEvent::Posted {
                        body: Body::parse("@owner @verifier JWT refresh rotation slice is ready (#2).")
                            .expect("body"),
                        mentions: Mentions {
                            principals: vec![me.clone()],
                            all: false,
                        },
                        reply_to: Some(Seq(1)),
                        idem: None,
                    }),
                },
            ],
        );
        rooms.insert(
            sol_dm.clone(),
            vec![Envelope {
                stream: StreamId::Room {
                    room: sol_dm.clone(),
                },
                seq: Seq(1),
                at: UnixMs(1_700_000_400),
                by: sol,
                event: Event::Room(RoomEvent::Posted {
                    body: Body::parse("@owner can you review the migration ruling when free?")
                        .expect("body"),
                    mentions: Mentions {
                        principals: vec![me.clone()],
                        all: false,
                    },
                    reply_to: None,
                    idem: None,
                }),
            }],
        );
        rooms.insert(general_room.clone(), Vec::new());
        rooms.insert(lounge_room.clone(), Vec::new());
        rooms.insert(astra_dm.clone(), Vec::new());

        let state = DemoState {
            project,
            wave,
            wave_room,
            general_room,
            lounge_room,
            sol_dm,
            astra_dm,
            board,
            inbox,
            rooms,
            workers,
            generation: FocusGeneration::default(),
        };
        let initial_snap = state.sidebar_snapshot(&me);
        let (sidebar_tx, _) = watch::channel(initial_snap);
        Self {
            me,
            inner: Arc::new(Mutex::new(state)),
            sidebar_tx: Arc::new(sidebar_tx),
        }
    }
}

impl DemoState {
    fn sidebar_snapshot(&self, me: &Principal) -> SidebarTreeSnapshot {
        let mut unread = 0u32;
        let mut blocking = 0u32;
        for item in &self.inbox.items {
            if item.state == NotifyState::Unread {
                unread += 1;
                if matches!(item.draft.kind, NotifyKind::Interaction { .. }) {
                    blocking += 1;
                }
            }
        }
        let inbox_badge = InboxBadge { unread, blocking };

        let open_cards: u32 = self
            .board
            .lanes
            .iter()
            .filter(|l| l.lane.as_str() != "done")
            .map(|l| l.cards.len() as u32)
            .sum();
        let kanban = vec![BoardBadge {
            board: self.board.board.clone(),
            open: open_cards,
            mine: 1,
        }];

        let workers: Vec<WaveWorkerLeaf> = self
            .workers
            .iter()
            .map(|(role, w)| WaveWorkerLeaf {
                role: role.clone(),
                provider: w.provider.clone(),
                model: w.model.clone(),
                phase: w.phase.clone(),
            })
            .collect();
        let osc7501 = derive_osc7501_status(&inbox_badge, workers.iter().map(|w| &w.phase));

        let sol = Principal::Seat {
            seat: SeatName::parse("sol").expect("seat"),
        };
        let astra = Principal::Seat {
            seat: SeatName::parse("astra").expect("seat"),
        };
        let _ = me;

        SidebarTreeSnapshot {
            inbox: inbox_badge,
            kanban,
            projects: vec![ProjectBranch {
                project: self.project.clone(),
                channels: vec![ChannelLeaf {
                    room: self.general_room.clone(),
                    unread: 1,
                    mentions: 0,
                }],
                waves: vec![WaveRoomBranch {
                    wave: self.wave.clone(),
                    room: self.wave_room.clone(),
                    unread: 1,
                    mentions: 1,
                    workers,
                }],
            }],
            channels: vec![ChannelLeaf {
                room: self.lounge_room.clone(),
                unread: 0,
                mentions: 0,
            }],
            direct_messages: vec![
                DmLeaf {
                    room: self.sol_dm.clone(),
                    with: sol,
                    unread: 1,
                },
                DmLeaf {
                    room: self.astra_dm.clone(),
                    with: astra,
                    unread: 0,
                },
            ],
            osc7501,
            cursor: Cursor::live(),
        }
    }
}

impl Client for DemoClient {
    fn me(&self) -> &Principal {
        &self.me
    }

    fn call_wire(
        &self,
        cmd: WireCommand,
    ) -> impl Future<Output = Result<WireReply, ApiError>> + Send {
        let me = self.me.clone();
        let inner = self.inner.clone();
        let sidebar_tx = self.sidebar_tx.clone();
        async move {
            let mut st = inner.lock().expect("demo lock");
            let reply = match cmd {
                WireCommand::Sidebar(_) => WireReply::Sidebar(st.sidebar_snapshot(&me)),
                WireCommand::InboxList(_) => WireReply::InboxList(st.inbox.clone()),
                WireCommand::InboxAck(ack) => {
                    for item in &mut st.inbox.items {
                        if ack.ids.contains(&item.id) {
                            item.state = ack.state;
                        }
                    }
                    st.inbox.unread = st
                        .inbox
                        .items
                        .iter()
                        .filter(|i| i.state == NotifyState::Unread)
                        .count() as u32;
                    let _ = sidebar_tx.send(st.sidebar_snapshot(&me));
                    WireReply::InboxAck(Ack)
                }
                WireCommand::InboxMarkRead(_) => WireReply::InboxMarkRead(Ack),
                WireCommand::BoardView(_) => WireReply::BoardView(st.board.clone()),
                WireCommand::BoardEnsure(b) => WireReply::BoardEnsure(BoardInfo {
                    board: b.board,
                    lanes: st.board.lanes.iter().map(|l| l.lane.clone()).collect(),
                    head: st.board.head,
                }),
                WireCommand::CardUpsert(up) => {
                    st.board.head += 1;
                    let card_ref = match up.card {
                        None => {
                            let next_no = st
                                .board
                                .lanes
                                .iter()
                                .flat_map(|l| l.cards.iter().map(|c| c.no.0))
                                .max()
                                .unwrap_or(0)
                                + 1;
                            let lane = up
                                .patch
                                .lane
                                .unwrap_or_else(|| LaneName::parse("todo").expect("lane"));
                            let title = up
                                .patch
                                .title
                                .unwrap_or_else(|| Title::parse("Untitled").expect("title"));
                            let assignee = match up.patch.assignee {
                                Some(Assign::To { who }) => Some(who),
                                _ => None,
                            };
                            let card = Card {
                                no: CardNo(next_no),
                                revision: CardRevision(1),
                                title,
                                lane: lane.clone(),
                                assignee,
                                priority: up.patch.priority.unwrap_or(Priority::Normal),
                                due_ms: up.patch.due_ms,
                                labels: up.patch.labels_add,
                                links: up.patch.links_add,
                            };
                            if let Some(target_lane) =
                                st.board.lanes.iter_mut().find(|l| l.lane == lane)
                            {
                                target_lane.cards.push(card);
                            }
                            CardRef {
                                board: up.board,
                                no: CardNo(next_no),
                                revision: CardRevision(1),
                            }
                        }
                        Some(no) => {
                            let mut rev = CardRevision(1);
                            let mut moved: Option<Card> = None;
                            for lane in &mut st.board.lanes {
                                if let Some(pos) = lane.cards.iter().position(|c| c.no == no) {
                                    let mut c = lane.cards.remove(pos);
                                    c.revision = c.revision.next();
                                    rev = c.revision;
                                    if let Some(l) = &up.patch.lane {
                                        c.lane = l.clone();
                                    }
                                    if let Some(t) = &up.patch.title {
                                        c.title = t.clone();
                                    }
                                    moved = Some(c);
                                    break;
                                }
                            }
                            if let Some(c) = moved
                                && let Some(target_lane) =
                                    st.board.lanes.iter_mut().find(|l| l.lane == c.lane)
                            {
                                target_lane.cards.push(c);
                            }
                            CardRef {
                                board: up.board,
                                no,
                                revision: rev,
                            }
                        }
                    };
                    let _ = sidebar_tx.send(st.sidebar_snapshot(&me));
                    WireReply::CardUpsert(card_ref)
                }
                WireCommand::CardArchive(ar) => {
                    for lane in &mut st.board.lanes {
                        lane.cards.retain(|c| c.no != ar.card);
                    }
                    let _ = sidebar_tx.send(st.sidebar_snapshot(&me));
                    WireReply::CardArchive(Ack)
                }
                WireCommand::Read(r) => {
                    let entries = match &r.stream {
                        StreamId::Room { room } => st
                            .rooms
                            .get(room)
                            .cloned()
                            .unwrap_or_default()
                            .into_iter()
                            .filter(|e| e.seq > r.after)
                            .collect(),
                        _ => Vec::new(),
                    };
                    let last = entries.last().map_or(r.after, |e| e.seq);
                    WireReply::Read(Page {
                        entries,
                        cursor: Cursor::default().with(r.stream, last),
                        timed_out: false,
                    })
                }
                WireCommand::Wait(_) => WireReply::Wait(Page {
                    entries: Vec::new(),
                    cursor: Cursor::live(),
                    timed_out: true,
                }),
                WireCommand::ThreadPage(tp) => {
                    let view = st
                        .workers
                        .get(&tp.role)
                        .map(|w| w.page.clone())
                        .ok_or_else(|| ApiError::NotFound {
                            what: format!("@{}/{}", tp.wave, tp.role),
                        })?;
                    WireReply::ThreadPage(view)
                }
                WireCommand::RoomPost(post) => {
                    let room_id = post.room.resolve(&me)?;
                    let entries = st.rooms.entry(room_id.clone()).or_default();
                    let next_seq = entries.last().map_or(Seq(1), |e| e.seq.next());
                    entries.push(Envelope {
                        stream: StreamId::Room {
                            room: room_id.clone(),
                        },
                        seq: next_seq,
                        at: UnixMs(1_700_000_900),
                        by: me.clone(),
                        event: Event::Room(RoomEvent::Posted {
                            body: post.body,
                            mentions: Mentions::default(),
                            reply_to: post.reply_to,
                            idem: post.idem,
                        }),
                    });
                    let _ = sidebar_tx.send(st.sidebar_snapshot(&me));
                    WireReply::RoomPost(Posted {
                        room: room_id,
                        seq: next_seq,
                        mentions: Mentions::default(),
                    })
                }
                WireCommand::RoomEnsure(re) => {
                    let id = re.room.resolve(&me)?;
                    WireReply::RoomEnsure(RoomInfo {
                        id,
                        origin: re.origin.unwrap_or(RoomOrigin::Manual),
                        topic: re.topic,
                        head: Seq(0),
                        members: Vec::new(),
                    })
                }
                WireCommand::RoomJoin(_) => WireReply::RoomJoin(Ack),
                WireCommand::RoomLeave(_) => WireReply::RoomLeave(Ack),
                WireCommand::WaveResolve(res) => {
                    if let Some(w) = st.workers.get_mut(&res.role) {
                        w.phase = WorkerPhase::Live {
                            thread: w.page.thread.clone(),
                            activity: Activity::Working,
                        };
                        w.page.activity = Activity::Working;
                        w.page.pending = None;
                    }
                    for item in &mut st.inbox.items {
                        if let NotifyKind::Interaction {
                            wave,
                            role,
                            interaction,
                        } = &item.draft.kind
                            && wave == &res.wave
                            && role == &res.role
                            && interaction == &res.interaction
                        {
                            item.state = NotifyState::Done;
                        }
                    }
                    st.inbox.unread = st
                        .inbox
                        .items
                        .iter()
                        .filter(|i| i.state == NotifyState::Unread)
                        .count() as u32;
                    let _ = sidebar_tx.send(st.sidebar_snapshot(&me));
                    WireReply::WaveResolve(Ack)
                }
                WireCommand::WaveTell(tell) => {
                    if let Some(w) = st.workers.get_mut(&tell.role) {
                        let next_seq = w
                            .page
                            .events
                            .last()
                            .map_or(BbSeq(1), |e| BbSeq(e.seq.0 + 1));
                        w.page.events.push(AgentEvent {
                            seq: next_seq,
                            at: UnixMs(1_700_000_950),
                            kind: AgentEventKind::UserMessage {
                                text: tell.body.as_str().to_owned(),
                            },
                        });
                        w.page.activity = Activity::Working;
                    }
                    WireReply::WaveTell(Ack)
                }
                WireCommand::WaveStart(ws) => WireReply::WaveStart(WaveStarted {
                    wave: ws.spec.slug.clone(),
                    room: RoomId::Channel {
                        scope: RoomScope::Project {
                            project: ws.spec.project.clone(),
                        },
                        name: ChannelName::parse(format!("wave-{}", ws.spec.slug)).expect("chan"),
                    },
                    board: project_board(&ws.spec.project),
                }),
                WireCommand::WaveAddWorker(_) => WireReply::WaveAddWorker(Ack),
                WireCommand::WaveRetireWorker(_) => WireReply::WaveRetireWorker(Ack),
                WireCommand::WaveFinish(_) => WireReply::WaveFinish(Ack),
                WireCommand::WaveObserve(_) => WireReply::WaveObserve(Ack),
                WireCommand::WaveView(wv) => WireReply::WaveView(WaveSnapshot {
                    spec: WaveSpec {
                        slug: wv.wave,
                        project: st.project.clone(),
                        goal: Body::parse("demo goal").expect("body"),
                        defaults: AgentSpec {
                            provider: ProviderId::parse("codex").expect("provider"),
                            model: None,
                            reasoning: None,
                            service_tier: None,
                            permission: PermissionMode::AcceptEdits,
                            workspace: Workspace::ManagedWorktree {
                                base: BaseBranch::Default,
                            },
                        },
                        workers: Vec::<WorkerSpec>::new(),
                    },
                    status: WaveStatus::Running,
                    workers: Vec::new(),
                }),
                WireCommand::MailPromote(mp) => {
                    let sha = Sha256Hex::parse("b".repeat(64)).expect("sha256");
                    let path = std::path::PathBuf::from(format!("to-owner/{}-r1.md", mp.topic));
                    let item = MailboxItem {
                        schema: MailSchemaV1,
                        id: format!("{}-r1", mp.topic),
                        from: me.to_string(),
                        to: "@owner".to_owned(),
                        kind: "letter".to_owned(),
                        state: "unread".to_owned(),
                        ts: "2026-11-16T10:30:00Z".to_owned(),
                        ts_basis: "mtime".to_owned(),
                        reference: MailRef {
                            store: "sandbox".to_owned(),
                            path: path.clone(),
                            sha256: sha.clone(),
                        },
                        topic: Some(mp.topic),
                        round: Some(1),
                        label: Some(mp.subject),
                    };
                    WireReply::MailPromote(LetterRef {
                        path,
                        sha256: sha,
                        wake_id: WakeId("0123456789abcdef".to_owned()),
                        item,
                    })
                }
                WireCommand::MailAck(_) => WireReply::MailAck(Ack),
                WireCommand::MailWakeBell(_) => WireReply::MailWakeBell(Ack),
                WireCommand::InboxRaise(_) => WireReply::InboxRaise(Ack),
            };
            Ok(reply)
        }
    }

    fn watch(
        &self,
        interest: Interest,
        _from: Cursor,
    ) -> impl Future<Output = Result<FrameStream, ApiError>> + Send {
        let inner = self.inner.clone();
        let sidebar_rx = self.sidebar_tx.subscribe();
        async move {
            match interest {
                Interest::Sidebar => {
                    let (tx, rx) = mpsc::channel(16);
                    let mut sub = sidebar_rx;
                    tokio::spawn(async move {
                        let initial = sub.borrow_and_update().clone();
                        if tx
                            .send(Ok(Frame::Sidebar(Box::new(initial))))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        while sub.changed().await.is_ok() {
                            let snap = sub.borrow_and_update().clone();
                            if tx.send(Ok(Frame::Sidebar(Box::new(snap)))).await.is_err() {
                                break;
                            }
                        }
                    });
                    Ok(channel_to_stream(rx))
                }
                Interest::Stage { target } => {
                    let generation = {
                        let mut st = inner.lock().expect("demo lock");
                        st.generation = st.generation.next();
                        st.generation
                    };
                    let (tx, rx) = mpsc::channel(16);
                    tokio::spawn(async move {
                        let _ = tx
                            .send(Ok(Frame::StageFocused { generation, target }))
                            .await;
                        tx.closed().await;
                    });
                    Ok(channel_to_stream(rx))
                }
                _ => {
                    let (tx, rx) = mpsc::channel(4);
                    tokio::spawn(async move {
                        let _ = tx
                            .send(Ok(Frame::CaughtUp {
                                cursor: Cursor::live(),
                            }))
                            .await;
                    });
                    Ok(channel_to_stream(rx))
                }
            }
        }
    }
}

struct ReceiverStreamAdapter {
    rx: mpsc::Receiver<Result<Frame, ApiError>>,
}

impl futures_core::Stream for ReceiverStreamAdapter {
    type Item = Result<Frame, ApiError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

fn channel_to_stream(rx: mpsc::Receiver<Result<Frame, ApiError>>) -> FrameStream {
    Box::pin(ReceiverStreamAdapter { rx })
}

pub struct SelfTestMetrics {
    pub warm_p50_us: u128,
    pub warm_p95_us: u128,
    pub cold_ansi_bytes: usize,
    pub warm_diff_bytes: usize,
    pub rgba_gen_us: u128,
    pub upload_b64_bytes: usize,
}

pub async fn run_self_test_demo() -> io::Result<SelfTestMetrics> {
    let client = DemoClient::new();
    run_self_test(&client).await
}

pub async fn run_self_test<C: Client>(client: &C) -> io::Result<SelfTestMetrics> {
    let width: u16 = 124;
    let height: u16 = 34;
    let cell = CellPx::default();

    let t_rgba = Instant::now();
    let card_w_px = 88 * u32::from(cell.w);
    let card_h_px = 4 * u32::from(cell.h);
    let approval_rgba = render_backdrop(crate::kitty::CardTone::Approval, card_w_px, card_h_px);
    let rgba_gen_us = t_rgba.elapsed().as_micros();
    assert_eq!(approval_rgba.len(), (card_w_px * card_h_px * 4) as usize);
    assert_eq!(
        approval_rgba[3], 0,
        "rounded corner (0,0) must have alpha 0"
    );
    let center_alpha =
        approval_rgba[(((card_h_px / 2) * card_w_px + (card_w_px / 2)) * 4 + 3) as usize];
    assert!(center_alpha >= 220, "card interior alpha must be >= 220");

    let upload_bytes = encode(
        &TermOut::Upload {
            image: CARD_APPROVAL_IMAGE,
            rgba_w: card_w_px,
            rgba_h: card_h_px,
        },
        cell,
        Some(&approval_rgba),
    );
    let upload_str = std::str::from_utf8(&upload_bytes).expect("utf8 kitty upload");
    for packet in upload_str.split("\x1b\\").filter(|s| !s.is_empty()) {
        let payload = packet.split_once(';').map(|(_, b)| b).unwrap_or("");
        assert!(
            payload.len() <= 4096,
            "Kitty base64 chunk exceeded 4096 bytes"
        );
    }

    let overview = match client
        .call_wire(WireCommand::Sidebar(read::Sidebar::default()))
        .await
        .map_err(|e| io::Error::other(e.to_string()))?
    {
        WireReply::Sidebar(s) => s,
        other => return Err(io::Error::other(format!("unexpected reply: {other:?}"))),
    };

    let mut model = Model::new(client.me().clone(), overview);
    let osc_initial = program_status(&model.sidebar_snapshot);
    assert_eq!(osc_initial.state, ProgramState::Blocked);

    retarget_sync(client, &mut model, StageTarget::Inbox).await?;

    let mut term = Terminal::new(TestBackend::new(width, height)).expect("test backend");
    let mut hits = HitMap::default();
    let mut cards = Vec::new();
    let mut compositor = KittyCompositor::new(cell);

    let pre = compositor.prepare_pre_draw(model.graphics, model.stage.active_usage_bar());
    assert!(pre.starts_with(b"\x1b[?2026h"));
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .expect("draw");
    let post = compositor.finish_post_draw(
        model.graphics,
        &cards,
        &program_status(&model.sidebar_snapshot),
    );
    assert!(post.ends_with(b"\x1b[?2026l"));
    assert!(
        !cards.is_empty(),
        "Inbox must place z=-1 notification cards"
    );

    let first_card = &cards[0];
    let first_clip = clip(first_card.card, first_card.viewport).expect("visible first card");
    assert_eq!(first_clip.top_clipped_rows, 0);
    assert_eq!(
        term.backend().buffer()[(first_card.card.x + 3, first_card.card.y)].bg,
        Color::Reset,
        "Rule A: z=-1 card interior cells must remain Color::Reset"
    );

    if let crate::stage::Stage::Inbox(inbox_view) = &mut model.stage {
        inbox_view.scroll = 2;
    }
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .expect("draw");
    let scrolled_post = compositor.finish_post_draw(
        model.graphics,
        &cards,
        &program_status(&model.sidebar_snapshot),
    );
    let scrolled_str = String::from_utf8_lossy(&scrolled_post);
    assert!(scrolled_str.contains("a=p,q=2,i=101,p=1"));
    assert!(
        scrolled_str.contains("y=48"),
        "2 clipped rows * 24px must crop y=48"
    );
    assert!(
        !scrolled_str.contains("a=t,"),
        "scroll crop must not re-upload RGBA"
    );

    if let crate::stage::Stage::Inbox(inbox_view) = &mut model.stage {
        inbox_view.scroll = 40;
    }
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .expect("draw");
    let offscreen_post = compositor.finish_post_draw(
        model.graphics,
        &cards,
        &program_status(&model.sidebar_snapshot),
    );
    assert!(
        String::from_utf8_lossy(&offscreen_post).contains("a=d,d=i,i=101,p=1"),
        "offscreen card must emit lowercase d=i placement delete"
    );
    if let crate::stage::Stage::Inbox(inbox_view) = &mut model.stage {
        inbox_view.scroll = 0;
    }

    let agent_target = StageTarget::Agent {
        wave: WaveSlug::parse("auth-v2").expect("wave"),
        role: RoleName::parse("author-jwt").expect("role"),
    };
    retarget_sync(client, &mut model, agent_target).await?;
    let stale = Frame::Stage {
        generation: FocusGeneration(0),
        inner: Box::new(Frame::Agent {
            wave: WaveSlug::parse("auth-v2").expect("wave"),
            role: RoleName::parse("author-jwt").expect("role"),
            event: AgentEvent {
                seq: BbSeq(999),
                at: UnixMs(0),
                kind: AgentEventKind::TurnEnded,
            },
        }),
    };
    let _ = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Stage,
            frame: stale,
        },
    );
    if let crate::stage::Stage::Agent(insp) = &model.stage {
        assert!(
            !insp.page.events.iter().any(|e| e.seq == BbSeq(999)),
            "stale FocusGeneration frame must be dropped"
        );
    }

    let _ = compositor.prepare_pre_draw(model.graphics, model.stage.active_usage_bar());
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .expect("draw");
    let buf = term.backend().buffer();
    let mut placeholder_cells = 0usize;
    for y in 0..height {
        for x in 0..width {
            let cell_ref = &buf[(x, y)];
            if cell_ref.symbol().starts_with('\u{10EEEE}') {
                placeholder_cells += 1;
                assert_eq!(cell_ref.fg, Color::Rgb(0, 0, CONTEXT_BAR_IMAGE.0 as u8));
                let chars: Vec<char> = cell_ref.symbol().chars().collect();
                assert_eq!(chars.len(), 3);
                assert_eq!(chars[1], DIACRITICS[0]);
            }
        }
    }
    assert_eq!(placeholder_cells, CONTEXT_BAR_COLS as usize);

    retarget_sync(client, &mut model, StageTarget::Kanban { board: None }).await?;
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .expect("draw");

    let wave_room = RoomId::Channel {
        scope: RoomScope::Project {
            project: BbProjectId::parse("p_123").expect("project"),
        },
        name: ChannelName::parse("wave-auth-v2").expect("chan"),
    };
    retarget_sync(client, &mut model, StageTarget::Room { room: wave_room }).await?;
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .expect("draw");

    retarget_sync(client, &mut model, StageTarget::Inbox).await?;
    let effects = update(&mut model, Msg::Action(Action::Accelerator('a')));
    for eff in effects {
        if let Effect::Call { tag, cmd } = eff {
            let res = client.call_wire(cmd).await;
            let _ = update(&mut model, Msg::Reply { tag, result: res });
        }
    }
    if let WireReply::Sidebar(fresh) = client
        .call_wire(WireCommand::Sidebar(read::Sidebar::default()))
        .await
        .map_err(|e| io::Error::other(e.to_string()))?
    {
        let _ = update(
            &mut model,
            Msg::Frame {
                watch: WatchSlot::Tree,
                frame: Frame::Sidebar(Box::new(fresh)),
            },
        );
    }
    assert_eq!(
        program_status(&model.sidebar_snapshot).state,
        ProgramState::Working
    );

    let mut sink = Vec::<u8>::with_capacity(16_384);
    let mut prev_buf = Buffer::empty(Rect::new(0, 0, width, height));
    let mut layout_us = Vec::with_capacity(200);
    let mut diff_us = Vec::with_capacity(200);
    let mut cold_ansi_bytes = 0usize;
    let mut warm_diff_bytes = 0usize;

    for i in 0..200 {
        if i % 2 == 1 {
            let _ = update(
                &mut model,
                Msg::Action(Action::Move(if i % 4 == 1 { Dir::Down } else { Dir::Up })),
            );
        }
        let t0 = Instant::now();
        term.draw(|f| view(&model, f, &mut hits, &mut cards))
            .expect("draw");
        layout_us.push(t0.elapsed().as_micros());

        let cur_buf = term.backend().buffer();
        sink.clear();
        let t1 = Instant::now();
        let diff = prev_buf.diff(cur_buf);
        {
            let mut ct = CrosstermBackend::new(&mut sink);
            ct.draw(diff.into_iter())?;
            ct.flush()?;
        }
        diff_us.push(t1.elapsed().as_micros());
        if i == 0 {
            cold_ansi_bytes = sink.len();
        } else if i == 1 {
            warm_diff_bytes = sink.len();
        }
        prev_buf = cur_buf.clone();
    }

    layout_us.sort_unstable();
    diff_us.sort_unstable();
    let warm_p50_us = layout_us[100] + diff_us[100];
    let warm_p95_us = layout_us[190] + diff_us[190];

    model.graphics = GraphicsMode::CellsOnly;
    model.sidebar.cursor = NodeKey::Room(RoomId::Channel {
        scope: RoomScope::Project {
            project: BbProjectId::parse("p_123").expect("project"),
        },
        name: ChannelName::parse("wave-auth-v2").expect("chan"),
    });
    let preview_target = target_for(&model.sidebar.cursor);
    retarget_sync(client, &mut model, preview_target).await?;
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .expect("draw");
    let ansi_preview = buffer_to_ansi(term.backend().buffer());

    let shutdown = compositor.shutdown_bytes();
    let shutdown_str = String::from_utf8_lossy(&shutdown);
    assert!(shutdown_str.contains("\x1b]7501;state=clear\x1b\\"));

    println!("=== REX-WAVE 2-PANE TUI SELF-TEST (124x34 ANSI PREVIEW) ===");
    print!("{ansi_preview}");
    println!("=== BENCHMARK & PROTOCOL INVARIANTS ===");
    println!(
        "1. Warm 2-Pane Frame (124x34, 200 iters): p50 = {warm_p50_us} us (layout {} us + diff/ANSI {} us), p95 = {warm_p95_us} us",
        layout_us[100], diff_us[100]
    );
    println!(
        "2. Wire Payload: cold full-frame = {cold_ansi_bytes} B | warm cursor diff = {warm_diff_bytes} B"
    );
    println!(
        "3. Kitty z=-1 SDF RGBA ({card_w_px}x{card_h_px} px): {rgba_gen_us} us | base64 a=t = {} B | Rule A Color::Reset + Rule B DECSET 2026 + Rule C y-crop/d=i OK",
        upload_bytes.len()
    );
    println!(
        "4. Agent Inspector U+10EEEE Context Bar: {placeholder_cells} cells | FocusGeneration stale-drop OK | OSC 7501 blocked->working->clear OK"
    );
    println!("SELF-TEST PASSED");

    Ok(SelfTestMetrics {
        warm_p50_us,
        warm_p95_us,
        cold_ansi_bytes,
        warm_diff_bytes,
        rgba_gen_us,
        upload_b64_bytes: upload_bytes.len(),
    })
}

async fn retarget_sync<C: Client>(
    client: &C,
    model: &mut Model,
    target: StageTarget,
) -> io::Result<()> {
    let plan = plan_for(&target, &model.sidebar_snapshot, &model.me);
    model.active_target = target.clone();
    model.stage = match &target {
        StageTarget::Summary { key } => crate::stage::Stage::Summary(key.clone()),
        other => crate::stage::Stage::Loading(other.clone()),
    };
    if let Some((interest, cursor)) = plan.watch {
        let mut stream = client
            .watch(interest, cursor)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        if let Some(Ok(first)) = poll_once_stream(&mut stream).await {
            let _ = update(
                model,
                Msg::Frame {
                    watch: WatchSlot::Stage,
                    frame: first,
                },
            );
        }
    }
    if let Some(cmd) = plan.initial {
        let res = client.call_wire(cmd).await;
        let seq = model.stage_req_seq;
        let _ = update(
            model,
            Msg::Reply {
                tag: ReplyTag::StageInitial(seq),
                result: res,
            },
        );
    }
    Ok(())
}

async fn poll_once_stream(stream: &mut FrameStream) -> Option<Result<Frame, ApiError>> {
    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

fn buffer_to_ansi(buf: &Buffer) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity((buf.area.width as usize) * (buf.area.height as usize) * 6);
    for y in 0..buf.area.height {
        let mut last_fg = Color::Reset;
        let mut last_bg = Color::Reset;
        let mut last_mod = Modifier::empty();
        for x in 0..buf.area.width {
            let c = &buf[(x, y)];
            if c.modifier != last_mod {
                out.push_str("\x1b[0m");
                last_fg = Color::Reset;
                last_bg = Color::Reset;
                if c.modifier.contains(Modifier::BOLD) {
                    out.push_str("\x1b[1m");
                }
                last_mod = c.modifier;
            }
            if c.fg != last_fg {
                match c.fg {
                    Color::Reset => out.push_str("\x1b[39m"),
                    Color::White => out.push_str("\x1b[97m"),
                    Color::Rgb(r, g, b) => {
                        let _ = write!(out, "\x1b[38;2;{r};{g};{b}m");
                    }
                    _ => out.push_str("\x1b[39m"),
                }
                last_fg = c.fg;
            }
            if c.bg != last_bg {
                match c.bg {
                    Color::Reset => out.push_str("\x1b[49m"),
                    Color::Rgb(r, g, b) => {
                        let _ = write!(out, "\x1b[48;2;{r};{g};{b}m");
                    }
                    _ => out.push_str("\x1b[49m"),
                }
                last_bg = c.bg;
            }
            out.push_str(c.symbol());
        }
        out.push_str("\x1b[0m\n");
    }
    out
}
