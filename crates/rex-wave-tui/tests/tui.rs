use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::{Terminal, backend::TestBackend, layout::Rect};
use rex_wave_api::{
    BbInteractionId, BbProjectId, BbSeq, ChannelName, Client, Cursor, Envelope, Event,
    FocusGeneration, Frame, Principal, RoleName, SeatName, Seq, StreamId, UnixMs, WaveSlug,
    WireCommand, WireReply,
    board::{BoardEvent, BoardId, Card, CardNo, CardRevision, LaneName, Priority, Title},
    notify::NotificationId,
    read::{AgentEvent, AgentEventKind, SidebarTreeSnapshot},
    room::{Body, Mentions, RoomEvent, RoomId, RoomScope},
    wave::{InteractionResolution, SteerDeliveryMode},
};
use rex_wave_tui::{
    Effect, Model, Msg, ReplyTag, WatchSlot,
    demo::{DemoClient, run_self_test_demo},
    input::{Action, FrameHitMap, Pane, key_action, mouse_action},
    kitty::{
        CARD_APPROVAL_IMAGE, CellPx, GraphicsMode, KittyCompositor, ProgramState, clip,
        program_status,
    },
    sidebar::{NodeKey, flatten},
    stage::{Stage, StageTarget},
    update, view,
};

fn press(model: &mut Model, pane: Pane, code: KeyCode) -> Vec<Effect> {
    let act = key_action(
        KeyEvent::new(code, KeyModifiers::NONE),
        pane,
        &model.sidebar.cursor,
    )
    .expect("mapped key");
    update(model, Msg::Action(act))
}

#[tokio::test]
async fn self_test_verifies_two_pane_render_kitty_and_osc7501() {
    let metrics = run_self_test_demo().await.expect("self test succeeds");
    assert!(metrics.cold_ansi_bytes > metrics.warm_diff_bytes);
    assert!(metrics.upload_b64_bytes > 0);
}

#[tokio::test]
async fn sidebar_org_tree_skips_divider_folds_and_falls_back_on_rebuild() {
    let client = DemoClient::new();
    let WireReply::Sidebar(overview) = client
        .call_wire(WireCommand::Sidebar(Default::default()))
        .await
        .expect("sidebar")
    else {
        panic!("expected sidebar reply");
    };

    let mut model = Model::new(client.me().clone(), overview.clone());
    let rows = flatten(&model.sidebar.roots, &model.sidebar.folds);
    assert_eq!(rows[0].key, NodeKey::Inbox);
    assert_eq!(rows[1].key, NodeKey::Kanban);
    assert_eq!(rows[2].key, NodeKey::Divider);
    assert_eq!(rows[3].key, NodeKey::ProjectsHeader);
    assert!(
        rows.iter()
            .any(|r| r.prefix.contains("├─ ") || r.prefix.contains("└─ ")),
        "org tree rows must render tree branch connectors"
    );

    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('j'));
    assert_eq!(model.sidebar.cursor, NodeKey::Kanban);
    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('j'));
    assert_eq!(model.sidebar.cursor, NodeKey::ProjectsHeader);

    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('G'));
    assert_eq!(model.sidebar.cursor, NodeKey::Settings);
    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('k'));
    assert!(matches!(model.sidebar.cursor, NodeKey::Dm(_)));
    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('g'));
    assert_eq!(model.sidebar.cursor, NodeKey::Inbox);

    let agent_key = NodeKey::Agent {
        wave: WaveSlug::parse("auth-v2").unwrap(),
        role: RoleName::parse("author-jwt").unwrap(),
    };
    model.sidebar.cursor = agent_key;
    let wave_room_key = NodeKey::Room(RoomId::Channel {
        scope: RoomScope::Project {
            project: BbProjectId::parse("p_123").unwrap(),
        },
        name: ChannelName::parse("wave-auth-v2").unwrap(),
    });
    let _ = update(
        &mut model,
        Msg::Action(Action::ToggleFold(wave_room_key.clone())),
    );
    assert_eq!(model.sidebar.cursor, wave_room_key);

    let empty = SidebarTreeSnapshot {
        inbox: overview.inbox,
        ..Default::default()
    };
    let _ = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Tree,
            frame: Frame::Sidebar(Box::new(empty)),
        },
    );
    assert_eq!(model.sidebar.cursor, NodeKey::ProjectsHeader);
}

#[test]
fn hover_debounces_while_activate_retargets_immediately_and_filters_stale_generations() {
    let me = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    let mut model = Model::new(me, SidebarTreeSnapshot::default());

    let hover_effects = update(&mut model, Msg::Action(Action::Hover(NodeKey::Kanban)));
    assert!(
        hover_effects
            .iter()
            .any(|e| matches!(e, Effect::DebounceHover(NodeKey::Kanban))),
        "hover must schedule a debounce rather than immediate stage watch churn"
    );
    assert!(
        !hover_effects
            .iter()
            .any(|e| matches!(e, Effect::Retarget(_)))
    );

    let settled = update(&mut model, Msg::HoverSettled(NodeKey::Kanban));
    assert!(
        settled
            .iter()
            .any(|e| matches!(e, Effect::Retarget(StageTarget::Kanban { board: None })))
    );

    let room = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("lounge").unwrap(),
    };
    let activated = update(
        &mut model,
        Msg::Action(Action::Activate(NodeKey::Room(room.clone()))),
    );
    assert!(
        activated
            .iter()
            .any(|e| matches!(e, Effect::Retarget(StageTarget::Room { room: r }) if r == &room))
    );

    let req_seq = model.stage_req_seq;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(req_seq),
            result: Ok(WireReply::Read(rex_wave_api::read::Page {
                entries: Vec::new(),
                cursor: Cursor::live(),
                timed_out: false,
                total_count: 0,
                truncated: false,
            })),
        },
    );
    let _ = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Stage,
            frame: Frame::StageFocused {
                generation: FocusGeneration(5),
                target: rex_wave_api::StageTarget::Room {
                    room: rex_wave_api::room::RoomRef::Id { id: room.clone() },
                },
            },
        },
    );

    let make_post = |seq: u64, text: &str| {
        Frame::Committed(Envelope {
            stream: StreamId::Room { room: room.clone() },
            seq: Seq(seq),
            at: UnixMs(100),
            by: Principal::Seat {
                seat: SeatName::parse("sam").unwrap(),
            },
            event: Event::Room(RoomEvent::Posted {
                body: Body::parse(text).unwrap(),
                mentions: Mentions::default(),
                re: None,
                after: None,
                labels: Vec::new(),
                task: None,
                idem: None,
                origin: Default::default(),
            }),
        })
    };

    let _ = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Stage,
            frame: Frame::Stage {
                generation: FocusGeneration(4),
                inner: Box::new(make_post(1, "stale frame")),
            },
        },
    );
    let _ = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Stage,
            frame: Frame::Stage {
                generation: FocusGeneration(5),
                inner: Box::new(make_post(2, "fresh frame")),
            },
        },
    );

    let Stage::Room(rv) = &model.stage else {
        panic!("expected room stage");
    };
    assert_eq!(rv.entries.len(), 1);
    assert_eq!(rv.entries[0].seq, Seq(2));
}

#[tokio::test]
async fn kanban_room_and_agent_stages_dispatch_typed_wire_commands() {
    let client = DemoClient::new();
    let WireReply::Sidebar(overview) = client
        .call_wire(WireCommand::Sidebar(Default::default()))
        .await
        .unwrap()
    else {
        panic!("sidebar");
    };
    let mut model = Model::new(client.me().clone(), overview);

    let _ = update(&mut model, Msg::Action(Action::Activate(NodeKey::Kanban)));
    let board_reply = client
        .call_wire(WireCommand::BoardView(rex_wave_api::board::BoardView {
            board: BoardId::parse("project:p_123").unwrap(),
            filter: None,
        }))
        .await;
    let req_seq = model.stage_req_seq;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(req_seq),
            result: board_reply,
        },
    );

    let move_fx = press(&mut model, Pane::Stage, KeyCode::Char('m'));
    assert!(move_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::CardUpsert(u), .. }
            if u.card == Some(CardNo(1)) && u.patch.lane == Some(LaneName::parse("doing").unwrap())
    )));

    let _ = press(&mut model, Pane::Stage, KeyCode::Char(']'));
    let _ = press(&mut model, Pane::Stage, KeyCode::Char('n'));
    assert_eq!(model.focus, Pane::Composer);
    for ch in "Ship PR5 TUI".chars() {
        let focus = model.focus;
        let _ = press(&mut model, focus, KeyCode::Char(ch));
    }
    let focus = model.focus;
    let add_fx = press(&mut model, focus, KeyCode::Enter);
    assert!(add_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::CardUpsert(u), .. }
            if u.card.is_none()
                && u.patch.title == Some(Title::parse("Ship PR5 TUI").unwrap())
                && u.patch.lane == Some(LaneName::parse("doing").unwrap())
    )));

    let live_card = Card {
        no: CardNo(99),
        revision: CardRevision(1),
        org_id: None,
        title: Title::parse("Live board fold").unwrap(),
        lane: LaneName::parse("review").unwrap(),
        assignee: None,
        priority: Priority::Normal,
        due_ms: None,
        labels: Vec::new(),
        links: Vec::new(),
        needs: Vec::new(),
    };
    let _ = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Stage,
            frame: Frame::Committed(Envelope {
                stream: StreamId::Board {
                    board: BoardId::parse("project:p_123").unwrap(),
                },
                seq: Seq(10),
                at: UnixMs(500),
                by: client.me().clone(),
                event: Event::Board(BoardEvent::CardCreated { card: live_card }),
            }),
        },
    );
    if let Stage::Kanban(kv) = &model.stage {
        assert!(
            kv.snapshot.lanes[2]
                .cards
                .iter()
                .any(|c| c.no == CardNo(99))
        );
    } else {
        panic!("expected kanban");
    }

    let wave_room = RoomId::Channel {
        scope: RoomScope::Project {
            project: BbProjectId::parse("p_123").unwrap(),
        },
        name: ChannelName::parse("wave-auth-v2").unwrap(),
    };
    let _ = update(
        &mut model,
        Msg::Action(Action::Activate(NodeKey::Room(wave_room.clone()))),
    );
    let read_reply = client
        .call_wire(WireCommand::Read(rex_wave_api::read::Read::stream(
            StreamId::Room {
                room: wave_room.clone(),
            },
            Seq::ORIGIN,
            Some(200),
        )))
        .await;
    let req_seq = model.stage_req_seq;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(req_seq),
            result: read_reply,
        },
    );

    let room_approve_fx = update(&mut model, Msg::Action(Action::Accelerator('a')));
    assert!(room_approve_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::WaveResolve(r), .. }
            if r.interaction == BbInteractionId::parse("int_8f2").unwrap()
                && matches!(r.resolution, InteractionResolution::AllowOnce { .. })
    )));

    let promote_fx = press(&mut model, Pane::Stage, KeyCode::Char('P'));
    assert!(promote_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::MailPromote(p), .. }
            if p.room == wave_room && p.range.first() == Seq(1) && p.range.last() == Seq(2)
    )));

    let _ = press(&mut model, Pane::Stage, KeyCode::Char('i'));
    for ch in "@verifier lgtm".chars() {
        let _ = update(&mut model, Msg::Action(Action::Input(ch)));
    }
    let post_fx = update(&mut model, Msg::Action(Action::Submit));
    assert!(post_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::RoomPost(rp), .. }
            if rp.body.as_str() == "@verifier lgtm"
    )));

    let mig_key = NodeKey::Agent {
        wave: WaveSlug::parse("auth-v2").unwrap(),
        role: RoleName::parse("migrator").unwrap(),
    };
    let _ = update(&mut model, Msg::Action(Action::Activate(mig_key)));
    let tp_reply = client
        .call_wire(WireCommand::ThreadPage(rex_wave_api::read::ThreadPage {
            wave: WaveSlug::parse("auth-v2").unwrap(),
            role: RoleName::parse("migrator").unwrap(),
            after: None,
            limit: Some(200),
        }))
        .await;
    let req_seq = model.stage_req_seq;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(req_seq),
            result: tp_reply,
        },
    );
    assert_eq!(
        model.sidebar.ctx_pct.get(&(
            WaveSlug::parse("auth-v2").unwrap(),
            RoleName::parse("migrator").unwrap()
        )),
        Some(&64)
    );

    let deny_fx = update(&mut model, Msg::Action(Action::Accelerator('d')));
    assert!(deny_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::WaveResolve(r), .. }
            if r.resolution == InteractionResolution::Deny
    )));

    let _ = update(&mut model, Msg::Action(Action::Compose));
    for ch in "Hold migration until backup finishes".chars() {
        let _ = update(&mut model, Msg::Action(Action::Input(ch)));
    }
    let steer_fx = update(&mut model, Msg::Action(Action::Submit));
    assert!(steer_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::WaveTell(t), .. }
            if t.mode == SteerDeliveryMode::Auto && t.body.as_str() == "Hold migration until backup finishes"
    )));

    let _ = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Stage,
            frame: Frame::Agent {
                wave: WaveSlug::parse("auth-v2").unwrap(),
                role: RoleName::parse("migrator").unwrap(),
                event: AgentEvent {
                    seq: BbSeq(99),
                    at: UnixMs(1_000),
                    kind: AgentEventKind::AssistantText {
                        text: "Acknowledged, pausing SQLite schema write.".to_owned(),
                    },
                },
            },
        },
    );
    if let Stage::Agent(insp) = &model.stage {
        assert!(insp.page.events.iter().any(|e| e.seq == BbSeq(99)));
    } else {
        panic!("expected agent inspector");
    }
}

#[tokio::test]
async fn graphics_cycle_mouse_hitmap_and_kitty_unplace_work_across_modes() {
    let client = DemoClient::new();
    let WireReply::Sidebar(overview) = client
        .call_wire(WireCommand::Sidebar(Default::default()))
        .await
        .unwrap()
    else {
        panic!("sidebar");
    };
    let mut model = Model::new(client.me().clone(), overview);
    let inbox_reply = client
        .call_wire(WireCommand::InboxList(rex_wave_api::notify::InboxList {
            include_done: false,
            limit: Some(100),
        }))
        .await;
    let req_seq = model.stage_req_seq;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(req_seq),
            result: inbox_reply,
        },
    );

    let mut term = Terminal::new(TestBackend::new(124, 34)).expect("backend");
    let mut hits = FrameHitMap::default();
    let mut cards = Vec::new();
    let mut compositor = KittyCompositor::new(CellPx::default());

    let _ = compositor.prepare_pre_draw(model.graphics, None);
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .expect("draw");
    let hybrid_out = compositor.finish_post_draw(
        model.graphics,
        &cards,
        &program_status(&model.sidebar_snapshot),
    );
    assert!(String::from_utf8_lossy(&hybrid_out).contains("a=p,q=2,i=101,p=1"));
    assert_eq!(cards[0].image, CARD_APPROVAL_IMAGE);
    assert!(clip(cards[0].card, Rect::new(0, 0, 10, 2)).is_none());

    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('v'));
    assert_eq!(model.graphics, GraphicsMode::BackdropOnly);
    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('v'));
    assert_eq!(model.graphics, GraphicsMode::PlaceholderOnly);

    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .expect("draw");
    let unplaced_out = compositor.finish_post_draw(
        model.graphics,
        &cards,
        &program_status(&model.sidebar_snapshot),
    );
    assert!(
        String::from_utf8_lossy(&unplaced_out).contains("a=d,d=i,i=101,p=1"),
        "switching out of backdrop mode must unplace active z=-1 cards"
    );

    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('v'));
    assert_eq!(model.graphics, GraphicsMode::CellsOnly);
    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('v'));
    assert_eq!(model.graphics, GraphicsMode::Hybrid);

    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .expect("draw");
    let mut clicked_action = None;
    for row in 0..34 {
        for col in 0..124 {
            let ev = MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: col,
                row,
                modifiers: KeyModifiers::NONE,
            };
            if let Some(Action::RunCardAction {
                id: NotificationId(1),
                index: 0,
            }) = mouse_action(ev, &hits)
            {
                clicked_action = Some(Action::RunCardAction {
                    id: NotificationId(1),
                    index: 0,
                });
                break;
            }
        }
    }
    let click = clicked_action.expect("FrameHitMap must expose clickable [a] Approve button");
    let click_fx = update(&mut model, Msg::Action(click));
    assert!(click_fx.iter().any(|e| matches!(
        e,
        Effect::Call {
            cmd: WireCommand::WaveResolve(_),
            ..
        }
    )));

    let quit_fx = press(&mut model, Pane::Sidebar, KeyCode::Char('q'));
    assert!(model.quit);
    assert!(quit_fx.iter().any(|e| matches!(
        e,
        Effect::Terminal(rex_wave_tui::kitty::TermOut::ProgramStatus(osc)) if osc.state == ProgramState::Clear
    )));
}

struct PanicAfterEndStream {
    ended: bool,
}

impl futures_core::Stream for PanicAfterEndStream {
    type Item = Result<Frame, rex_wave_api::ApiError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        assert!(!self.ended, "stream polled again after returning None");
        self.ended = true;
        std::task::Poll::Ready(None)
    }
}

#[tokio::test]
async fn room_paging_watch_eof_state_preservation_errors_and_inbox_scroll_hold() {
    let client = DemoClient::new();
    let wave_room = RoomId::Channel {
        scope: RoomScope::Project {
            project: BbProjectId::parse("p_123").unwrap(),
        },
        name: ChannelName::parse("wave-auth-v2").unwrap(),
    };
    client.seed_room_posts(&wave_room, 250);

    let WireReply::Sidebar(overview) = client
        .call_wire(WireCommand::Sidebar(Default::default()))
        .await
        .unwrap()
    else {
        panic!("sidebar");
    };
    let mut model = Model::new(client.me().clone(), overview.clone());

    let target = StageTarget::Room {
        room: wave_room.clone(),
    };
    let _ = update(
        &mut model,
        Msg::Action(Action::Activate(NodeKey::Room(wave_room.clone()))),
    );
    let req_seq = model.stage_req_seq;
    let (initial_res, _watch_res) =
        rex_wave_tui::load_stage_target(&client, &overview, &target).await;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(req_seq),
            result: initial_res.unwrap(),
        },
    );
    if let Stage::Room(rv) = &mut model.stage {
        assert_eq!(
            rv.entries.len(),
            250,
            "room > 200 entries pages forward to head"
        );
        assert_eq!(rv.scroll.pos, u16::MAX, "room opens scrolled to tail");
        assert_eq!(
            rv.cursor.at(&StreamId::Room {
                room: wave_room.clone()
            }),
            Seq(250)
        );
        rv.scroll.pos = 7;
        rv.status_banner = Some("Banked".to_owned());
    } else {
        panic!("expected room");
    }

    let refresh_fx = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::UserCommand,
            result: Ok(WireReply::RoomJoin(rex_wave_api::Ack)),
        },
    );
    for eff in refresh_fx {
        if let Effect::Call { tag, cmd } = eff {
            let res = client.call_wire(cmd).await;
            let _ = update(&mut model, Msg::Reply { tag, result: res });
        }
    }
    if let Stage::Room(rv) = &model.stage {
        assert_eq!(rv.scroll.pos, 7);
        assert_eq!(rv.status_banner.as_deref(), Some("Banked"));
    }

    let _ = update(&mut model, Msg::Action(Action::Activate(NodeKey::Kanban)));
    let k_seq = model.stage_req_seq;
    let board_res = client
        .call_wire(WireCommand::BoardView(rex_wave_api::board::BoardView {
            board: BoardId::parse("project:p_123").unwrap(),
            filter: None,
        }))
        .await;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(k_seq),
            result: board_res,
        },
    );
    let _ = press(&mut model, Pane::Stage, KeyCode::Char(']'));
    let _ = press(&mut model, Pane::Stage, KeyCode::Char(']'));
    let post_cmd_fx = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::UserCommand,
            result: Ok(WireReply::CardArchive(rex_wave_api::Ack)),
        },
    );
    for eff in post_cmd_fx {
        if let Effect::Call { tag, cmd } = eff {
            let res = client.call_wire(cmd).await;
            let _ = update(&mut model, Msg::Reply { tag, result: res });
        }
    }
    if let Stage::Kanban(kv) = &model.stage {
        assert_eq!(
            kv.cursor.0, 2,
            "Kanban lane cursor must survive UserCommand refresh"
        );
    } else {
        panic!("expected kanban");
    }

    let mut slot: Option<rex_wave_api::FrameStream> =
        Some(Box::pin(PanicAfterEndStream { ended: false }));
    let closed_msg = rex_wave_tui::poll_watch_slot(&mut slot, WatchSlot::Tree).await;
    assert!(slot.is_none(), "completed watch clears slot immediately");
    let _ = update(&mut model, closed_msg);
    assert_eq!(model.last_error.as_deref(), Some("daemon disconnected"));
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(5),
            rex_wave_tui::poll_watch_slot(&mut slot, WatchSlot::Tree),
        )
        .await
        .is_err()
    );

    let _ = update(
        &mut model,
        Msg::Action(Action::Activate(NodeKey::Agent {
            wave: WaveSlug::parse("auth-v2").unwrap(),
            role: RoleName::parse("missing").unwrap(),
        })),
    );
    let err_seq = model.stage_req_seq;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(err_seq),
            result: Err(rex_wave_api::ApiError::NotFound {
                what: "@auth-v2/missing".to_owned(),
            }),
        },
    );
    assert!(
        matches!(model.stage, Stage::Error { .. }),
        "failed StageInitial leaves Loading for Stage::Error"
    );

    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::UserCommand,
            result: Err(rex_wave_api::ApiError::conflict("revision mismatch")),
        },
    );
    assert!(
        model
            .last_error
            .as_deref()
            .unwrap()
            .contains("revision mismatch")
    );

    let _ = update(
        &mut model,
        Msg::Resize {
            cols: 120,
            rows: 24,
        },
    );
    let _ = update(&mut model, Msg::Action(Action::Activate(NodeKey::Inbox)));
    let i_seq = model.stage_req_seq;
    let mut many_items = Vec::new();
    for idx in 1..=12u64 {
        many_items.push(rex_wave_api::notify::Notification {
            id: NotificationId(idx),
            at: UnixMs(1_000 + idx),
            rank: rex_wave_api::notify::Rank(500),
            state: rex_wave_api::notify::NotifyState::Unread,
            draft: rex_wave_api::notify::NotificationDraft {
                kind: rex_wave_api::notify::NotifyKind::Reminder {
                    bell_id: format!("b{idx}"),
                },
                title: format!("Card {idx}"),
                preview: "body".to_owned(),
                actions: Vec::new(),
                dedupe: format!("d{idx}"),
            },
        });
    }
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(i_seq),
            result: Ok(WireReply::InboxList(rex_wave_api::notify::InboxPage {
                head: Seq(12),
                items: many_items,
                unread: 12,
                cursors: Vec::new(),
                total_count: 12,
                truncated: false,
            })),
        },
    );
    model.focus = Pane::Stage;
    for _ in 0..10 {
        let _ = press(&mut model, Pane::Stage, KeyCode::Char('j'));
    }
    let mut term = Terminal::new(TestBackend::new(120, 24)).expect("backend");
    let mut hits = FrameHitMap::default();
    let mut cards = Vec::new();
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .expect("draw");
    let selected_placement = cards
        .iter()
        .find(|c| c.placement == 11)
        .expect("selected 11th card (index 10) must be placed in viewport");
    assert_eq!(selected_placement.clip().unwrap().visible_rows, 4);
}

fn screen_text(term: &Terminal<TestBackend>) -> String {
    let buf = term.backend().buffer();
    let area = buf.area;
    let mut out = String::new();
    for y in 0..area.height {
        let mut line = String::new();
        for x in 0..area.width {
            line.push_str(buf[(x, y)].symbol());
        }
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out
}

#[test]
fn room_wraps_markdown_collapses_briefs_shortens_senders_and_opens_pager() {
    let me = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    let project = BbProjectId::parse("proj_alpha").unwrap();
    let wave = WaveSlug::parse("auth-v2").unwrap();
    let role = RoleName::parse("council-claude").unwrap();
    let wave_room = RoomId::Channel {
        scope: RoomScope::Project { project },
        name: ChannelName::parse("wave-auth-v2").unwrap(),
    };

    let mut model = Model::new(me, SidebarTreeSnapshot::default());
    let _ = update(
        &mut model,
        Msg::Action(Action::Activate(NodeKey::Room(wave_room.clone()))),
    );
    let req_seq = model.stage_req_seq;

    let brief_env = Envelope {
        stream: StreamId::Room {
            room: wave_room.clone(),
        },
        seq: Seq(1),
        at: UnixMs(100),
        by: Principal::System {
            part: rex_wave_api::SystemPart::WaveConductor,
        },
        event: Event::Room(RoomEvent::Posted {
            body: Body::parse(
                "@council-claude Review the JWT rotation design.\nHidden brief line 2.\nHidden brief line 3.",
            )
            .unwrap(),
            mentions: Mentions::default(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: Some(rex_wave_api::IdemKey::parse("wave:auth-v2:brief:council-claude:1").unwrap()), origin: Default::default(),
        }),
    };
    let review_env = Envelope {
        stream: StreamId::Room {
            room: wave_room.clone(),
        },
        seq: Seq(2),
        at: UnixMs(200),
        by: Principal::Worker {
            wave: wave.clone(),
            role: role.clone(),
        },
        event: Event::Room(RoomEvent::Posted {
            body: Body::parse(
                "# Council Verdict\n\nOverall **7/10** on `verify_jwt()`.\n\n```rust\nlet ttl = 900;\n```\n\nFinal paragraph survives multi-line wrap.",
            )
            .unwrap(),
            mentions: Mentions::default(),
            re: None,
            after: None,
            labels: Vec::new(),
            task: None,
            idem: None, origin: Default::default(),
        }),
    };

    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(req_seq),
            result: Ok(WireReply::Read(rex_wave_api::read::Page {
                entries: vec![brief_env, review_env],
                cursor: Cursor::live(),
                timed_out: false,
                total_count: 2,
                truncated: false,
            })),
        },
    );
    model.focus = Pane::Stage;

    let mut term = Terminal::new(TestBackend::new(124, 34)).unwrap();
    let mut hits = FrameHitMap::default();
    let mut cards = Vec::new();
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    let rendered = screen_text(&term);

    assert!(
        rendered.contains("brief for @council-claude [Enter to expand]"),
        "WaveConductor brief must collapse to 1-line summary, got:\n{rendered}"
    );
    assert!(
        !rendered.contains("Hidden brief line 2"),
        "collapsed brief body must stay hidden until expanded"
    );
    assert!(
        rendered.contains("#2 @council-claude") && !rendered.contains("@auth-v2/council-claude"),
        "sender inside #wave-auth-v2 must shorten to @council-claude, got:\n{rendered}"
    );
    assert!(rendered.contains("Council Verdict"));
    assert!(rendered.contains("7/10"));
    assert!(rendered.contains("let ttl = 900;"));
    assert!(
        rendered.contains("Final paragraph survives multi-line wrap."),
        "multi-line room post must render all wrapped lines instead of clipping to line 1"
    );

    // Select #2, then step up to #1 (brief) and expand in place with Enter; Enter again opens Pager; Space collapses.
    let _ = press(&mut model, Pane::Stage, KeyCode::Char('k'));
    let _ = press(&mut model, Pane::Stage, KeyCode::Char('k'));
    let _ = press(&mut model, Pane::Stage, KeyCode::Enter);
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    assert!(screen_text(&term).contains("Hidden brief line 2"));

    let _ = press(&mut model, Pane::Stage, KeyCode::Enter);
    assert_eq!(
        model.focus,
        Pane::Overlay,
        "Enter on an already-expanded brief opens the Pager"
    );
    let _ = press(&mut model, Pane::Overlay, KeyCode::Esc);

    let _ = press(&mut model, Pane::Stage, KeyCode::Char(' '));
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    assert!(!screen_text(&term).contains("Hidden brief line 2"));

    // Step down to #2 (council review) and open full-message Pager with Enter.
    let _ = press(&mut model, Pane::Stage, KeyCode::Char('j'));
    let _ = press(&mut model, Pane::Stage, KeyCode::Enter);
    assert_eq!(model.focus, Pane::Overlay);
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    let pager = screen_text(&term);
    assert!(pager.contains("#2 @council-claude"));
    assert!(pager.contains("Final paragraph survives multi-line wrap."));

    let _ = press(&mut model, Pane::Overlay, KeyCode::Esc);
    assert_eq!(model.focus, Pane::Stage);
    assert!(model.overlay.is_none());
}

#[test]
fn agent_inspector_wraps_turns_pins_latest_output_and_pages_turns() {
    let me = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    let wave = WaveSlug::parse("auth-v2").unwrap();
    let role = RoleName::parse("author-jwt").unwrap();
    let mut model = Model::new(me, SidebarTreeSnapshot::default());
    let _ = update(
        &mut model,
        Msg::Action(Action::Activate(NodeKey::Agent {
            wave: wave.clone(),
            role: role.clone(),
        })),
    );
    let seq = model.stage_req_seq;
    let tp = rex_wave_api::read::ThreadPageView {
        thread: rex_wave_api::BbThreadId::parse("thr_1").unwrap(),
        activity: rex_wave_api::wave::Activity::Idle,
        usage: None,
        pending: None,
        events: vec![
            AgentEvent {
                seq: BbSeq(1),
                at: UnixMs(10),
                kind: AgentEventKind::AssistantText {
                    text: "Earlier turn line 1\nEarlier turn line 2".to_owned(),
                },
            },
            AgentEvent {
                seq: BbSeq(2),
                at: UnixMs(20),
                kind: AgentEventKind::TurnEnded,
            },
            AgentEvent {
                seq: BbSeq(3),
                at: UnixMs(30),
                kind: AgentEventKind::AssistantText {
                    text: "# Final Summary\nAll **tests** pass on `main`.\nSecond wrapped line."
                        .to_owned(),
                },
            },
        ],
        next: None,
    };
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(seq),
            result: Ok(WireReply::ThreadPage(tp)),
        },
    );
    model.focus = Pane::Stage;

    let mut term = Terminal::new(TestBackend::new(124, 34)).unwrap();
    let mut hits = FrameHitMap::default();
    let mut cards = Vec::new();
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    let screen = screen_text(&term);
    assert!(screen.contains("TEXT ★ latest output"));
    assert!(screen.contains("Earlier turn line 2"));
    assert!(screen.contains("Second wrapped line."));

    let _ = press(&mut model, Pane::Stage, KeyCode::Char('k'));
    let _ = press(&mut model, Pane::Stage, KeyCode::Enter);
    assert_eq!(model.focus, Pane::Overlay);
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    assert!(screen_text(&term).contains("Turn · @auth-v2/author-jwt · #3"));
}

struct SingleSlotStream {
    rx: tokio::sync::mpsc::Receiver<Result<Frame, rex_wave_api::ApiError>>,
}

impl futures_core::Stream for SingleSlotStream {
    type Item = Result<Frame, rex_wave_api::ApiError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[derive(Clone)]
struct SingleSlotSlowClient {
    inner: DemoClient,
    stage_slot: std::sync::Arc<
        std::sync::Mutex<Option<tokio::sync::mpsc::Sender<Result<Frame, rex_wave_api::ApiError>>>>,
    >,
}

impl Client for SingleSlotSlowClient {
    fn me(&self) -> &Principal {
        self.inner.me()
    }

    async fn call_wire(&self, cmd: WireCommand) -> Result<WireReply, rex_wave_api::ApiError> {
        match &cmd {
            WireCommand::ThreadPage(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(160)).await;
                return Err(rex_wave_api::ApiError::Timeout);
            }
            WireCommand::Read(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(160)).await;
            }
            _ => {}
        }
        self.inner.call_wire(cmd).await
    }

    async fn watch(
        &self,
        interest: rex_wave_api::Interest,
        from: Cursor,
    ) -> Result<rex_wave_api::FrameStream, rex_wave_api::ApiError> {
        if let rex_wave_api::Interest::Stage { target } = interest {
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            let _ = tx.try_send(Ok(Frame::StageFocused {
                generation: FocusGeneration(1),
                target,
            }));
            // Single server stage slot: opening a new stage watch drops the previous sender.
            *self.stage_slot.lock().unwrap() = Some(tx);
            return Ok(Box::pin(SingleSlotStream { rx }));
        }
        self.inner.watch(interest, from).await
    }
}

#[tokio::test]
async fn async_stage_retarget_cancels_slow_room_without_stealing_single_stage_slot() {
    let client = SingleSlotSlowClient {
        inner: DemoClient::new(),
        stage_slot: std::sync::Arc::new(std::sync::Mutex::new(None)),
    };
    let WireReply::Sidebar(overview) = client
        .call_wire(WireCommand::Sidebar(Default::default()))
        .await
        .unwrap()
    else {
        panic!("sidebar");
    };
    let mut model = Model::new(client.me().clone(), overview);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let loader = rex_wave_tui::StageLoader::new(client.clone(), tx);

    // Leave a slow paged Room (seq A) for Kanban (seq B); loader must cancel the slow Room
    // before it opens a late watch that would evict Kanban's single server stage slot.
    let slow_room = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("lounge").unwrap(),
    };
    let fx = update(
        &mut model,
        Msg::Action(Action::Activate(NodeKey::Room(slow_room))),
    );
    let t0 = std::time::Instant::now();
    for eff in fx {
        if let Effect::Retarget(target) = eff {
            loader.retarget(model.stage_req_seq, model.sidebar_snapshot.clone(), target);
        }
    }
    assert!(
        t0.elapsed() < std::time::Duration::from_millis(30),
        "StageLoader::retarget must not block the UI loop"
    );

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let fx2 = update(&mut model, Msg::Action(Action::Activate(NodeKey::Kanban)));
    for eff in fx2 {
        if let Effect::Retarget(target) = eff {
            loader.retarget(model.stage_req_seq, model.sidebar_snapshot.clone(), target);
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(240)).await;
    let mut stage_watch = None;
    while let Ok(msg) = rx.try_recv() {
        match msg {
            Msg::StageWatchOpened {
                seq,
                watch: Ok(opt),
            } => {
                if seq == model.stage_req_seq {
                    stage_watch = opt.map(|w| w.0);
                }
            }
            other => {
                let _ = update(&mut model, other);
            }
        }
    }
    assert!(
        matches!(model.stage, Stage::Kanban(_)),
        "stale slow target must not clobber active Kanban stage"
    );
    let first_frame = rex_wave_tui::poll_watch_slot(&mut stage_watch, WatchSlot::Stage).await;
    assert!(
        matches!(
            first_frame,
            Msg::Frame {
                watch: WatchSlot::Stage,
                frame: Frame::StageFocused { .. }
            }
        ),
        "active Kanban stage watch must remain open on the server's single slot"
    );
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            rex_wave_tui::poll_watch_slot(&mut stage_watch, WatchSlot::Stage),
        )
        .await
        .is_err(),
        "active stage watch must stay parked rather than closing immediately"
    );

    // Verify human Stage Error label and connection status pill in top chrome.
    let _ = update(
        &mut model,
        Msg::Action(Action::Activate(NodeKey::Agent {
            wave: WaveSlug::parse("auth-v2").unwrap(),
            role: RoleName::parse("missing").unwrap(),
        })),
    );
    let err_seq = model.stage_req_seq;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(err_seq),
            result: Err(rex_wave_api::ApiError::NotFound {
                what: "worker".to_owned(),
            }),
        },
    );
    let mut term = Terminal::new(TestBackend::new(124, 24)).unwrap();
    let mut hits = FrameHitMap::default();
    let mut cards = Vec::new();
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    let screen = screen_text(&term);
    assert!(screen.contains("✕ @auth-v2/missing: not found: worker"));
    assert!(!screen.contains("WaveSlug"));
    assert!(screen.contains("● connected"));

    let _ = update(
        &mut model,
        Msg::WatchClosed {
            watch: WatchSlot::Tree,
            error: None,
        },
    );
    assert_eq!(
        model.connection,
        rex_wave_tui::ConnectionState::Disconnected
    );
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    assert!(screen_text(&term).contains("○ disconnected"));
}

#[tokio::test]
async fn inbox_keys_help_overlay_sidebar_resize_project_kanban_and_multiline_composer() {
    let client = DemoClient::new();
    let WireReply::Sidebar(mut overview) = client
        .call_wire(WireCommand::Sidebar(Default::default()))
        .await
        .unwrap()
    else {
        panic!("sidebar");
    };
    let proj_beta = BbProjectId::parse("proj_beta").unwrap();
    overview.projects.push(rex_wave_api::read::ProjectBranch {
        project: proj_beta.clone(),
        name: None,
        archived: false,
        channels: vec![rex_wave_api::read::ChannelLeaf {
            room: RoomId::Channel {
                scope: RoomScope::Project {
                    project: proj_beta.clone(),
                },
                name: ChannelName::parse("ops").unwrap(),
            },
            unread: 0,
            mentions: 0,
            archived: false,
        }],
        waves: Vec::new(),
    });

    let mut model = Model::new(client.me().clone(), overview);
    let room_alpha = RoomId::Channel {
        scope: RoomScope::Project {
            project: BbProjectId::parse("p_123").unwrap(),
        },
        name: ChannelName::parse("wave-auth-v2").unwrap(),
    };
    let page = rex_wave_api::notify::InboxPage {
        head: Seq(3),
        unread: 3,
        cursors: Vec::new(),
        total_count: 3,
        truncated: false,
        items: vec![
            rex_wave_api::notify::Notification {
                id: NotificationId(10),
                at: UnixMs(300),
                rank: rex_wave_api::notify::Rank(700),
                state: rex_wave_api::notify::NotifyState::Unread,
                draft: rex_wave_api::notify::NotificationDraft {
                    kind: rex_wave_api::notify::NotifyKind::Mention {
                        room: room_alpha.clone(),
                        seq: Seq(5),
                    },
                    title: "Mention 1".to_owned(),
                    preview: "hi".to_owned(),
                    actions: vec![rex_wave_api::notify::NotifyAction {
                        label: "Mark read".to_owned(),
                        key: Some('r'),
                        run: Box::new(WireCommand::InboxMarkRead(
                            rex_wave_api::notify::InboxMarkRead {
                                room: room_alpha.clone(),
                                upto: Seq(5),
                            },
                        )),
                    }],
                    dedupe: "m1".to_owned(),
                },
            },
            rex_wave_api::notify::Notification {
                id: NotificationId(11),
                at: UnixMs(200),
                rank: rex_wave_api::notify::Rank(690),
                state: rex_wave_api::notify::NotifyState::Unread,
                draft: rex_wave_api::notify::NotificationDraft {
                    kind: rex_wave_api::notify::NotifyKind::Mention {
                        room: room_alpha.clone(),
                        seq: Seq(8),
                    },
                    title: "Mention 2".to_owned(),
                    preview: "hi again".to_owned(),
                    actions: Vec::new(),
                    dedupe: "m2".to_owned(),
                },
            },
        ],
    };
    let req_seq = model.stage_req_seq;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(req_seq),
            result: Ok(WireReply::InboxList(page)),
        },
    );

    // [r] runs the card's Mark read action; [x] dismisses; [X] dismisses all MENTION cards + advances cursor.
    let r_fx = press(&mut model, Pane::Stage, KeyCode::Char('r'));
    assert!(r_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::InboxMarkRead(mr), .. } if mr.upto == Seq(5)
    )));
    let x_fx = press(&mut model, Pane::Stage, KeyCode::Char('x'));
    assert!(x_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::InboxAck(ack), .. }
            if ack.ids == vec![NotificationId(10)]
                && ack.state == rex_wave_api::notify::NotifyState::Dismissed
    )));
    let shift_x = key_action(
        KeyEvent::new(KeyCode::Char('X'), KeyModifiers::SHIFT),
        Pane::Stage,
        &model.sidebar.cursor,
    )
    .unwrap();
    let group_fx = update(&mut model, Msg::Action(shift_x));
    assert!(group_fx.iter().any(|e| matches!(
        e,
        Effect::CallBatch { cmds }
            if cmds.len() == 2
                && matches!(
                    &cmds[0],
                    WireCommand::InboxAck(ack)
                        if ack.ids == vec![NotificationId(10), NotificationId(11)]
                )
                && matches!(
                    &cmds[1],
                    WireCommand::InboxMarkRead(mr) if mr.upto == Seq(8)
                )
    )));

    // [?] opens Help Overlay showing Graphics mode, blocks margin clicks, and [v] cycles graphics.
    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('?'));
    assert_eq!(model.focus, Pane::Overlay);
    let mut term = Terminal::new(TestBackend::new(132, 38)).unwrap();
    let mut hits = FrameHitMap::default();
    let mut cards = Vec::new();
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    assert_eq!(
        mouse_action(
            MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: 0,
                row: 0,
                modifiers: KeyModifiers::NONE,
            },
            &hits,
        ),
        None,
        "clicks in the margin while an overlay is open must not hit underlying sidebar rows"
    );
    let help_screen = screen_text(&term);
    assert!(help_screen.contains("Hybrid z=-1 + U+10EEEE"));
    assert!(help_screen.contains("mark card read"));
    let _ = press(&mut model, Pane::Overlay, KeyCode::Char('v'));
    assert_eq!(model.graphics, GraphicsMode::BackdropOnly);
    let _ = press(&mut model, Pane::Overlay, KeyCode::Char('?'));
    assert_eq!(model.focus, Pane::Sidebar);

    // Sidebar badges shorten claude-code -> claude, strip #wave-, and < / > / \ + clamped drag resize.
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    let main_screen = screen_text(&term);
    assert!(
        main_screen.contains("[claude]") && !main_screen.contains("[claude-code]"),
        "sidebar must shorten claude-code badge, got:\n{main_screen}"
    );
    assert!(
        !main_screen.contains("#wave-auth-v2"),
        "sidebar wave branch must strip redundant #wave- prefix"
    );

    let _ = update(&mut model, Msg::Resize { cols: 90, rows: 34 });
    let _ = update(&mut model, Msg::Action(Action::ResizeBegin));
    let _ = update(&mut model, Msg::Action(Action::ResizeDrag(85)));
    let _ = update(&mut model, Msg::Action(Action::ResizeEnd));
    assert_eq!(model.sidebar_width, 66, "drag clamps to term_cols - 24");
    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('<'));
    assert_eq!(
        model.sidebar_width, 64,
        "< acts on the very first press after dragging to max width"
    );
    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('>'));
    assert_eq!(model.sidebar_width, 66);
    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('\\'));
    assert!(model.sidebar_hidden);
    assert_eq!(model.focus, Pane::Stage);
    let _ = press(&mut model, Pane::Stage, KeyCode::Char('\\'));
    assert!(!model.sidebar_hidden);

    // Project-scoped Kanban:
    // 1) Visiting proj_beta (which has no board in overview.kanban) falls back cleanly to board: None,
    //    while explicit [B] targets project:proj_beta.
    let _ = update(
        &mut model,
        Msg::HoverSettled(NodeKey::Project(proj_beta.clone())),
    );
    model.sidebar.cursor = NodeKey::Project(proj_beta.clone());
    let fallback_fx = update(&mut model, Msg::Action(Action::Activate(NodeKey::Kanban)));
    assert!(
        fallback_fx
            .iter()
            .any(|e| matches!(e, Effect::Retarget(StageTarget::Kanban { board: None })))
    );
    model.sidebar.cursor = NodeKey::Project(proj_beta.clone());
    let explicit_b = update(&mut model, Msg::Action(Action::OpenProjectBoard));
    assert!(explicit_b.iter().any(|e| matches!(
        e,
        Effect::Retarget(StageTarget::Kanban { board: Some(b) })
            if b == &BoardId::parse("project:proj_beta").unwrap()
    )));

    // 2) Once proj_beta has an ensured board in overview.kanban, stepping cursor [k] across p_123
    //    on the way up to Kanban does NOT clobber active_project.
    model
        .sidebar_snapshot
        .kanban
        .push(rex_wave_api::read::BoardBadge {
            board: BoardId::parse("project:proj_beta").unwrap(),
            open: 1,
            mine: 0,
        });
    model.sidebar.cursor = NodeKey::Project(proj_beta.clone());
    let _ = update(
        &mut model,
        Msg::HoverSettled(NodeKey::Project(proj_beta.clone())),
    );
    model.focus = Pane::Sidebar;
    while model.sidebar.cursor != NodeKey::Kanban {
        let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('k'));
    }
    let scoped_fx = update(&mut model, Msg::Action(Action::Activate(NodeKey::Kanban)));
    assert!(scoped_fx.iter().any(|e| matches!(
        e,
        Effect::Retarget(StageTarget::Kanban { board: Some(b) })
            if b == &BoardId::parse("project:proj_beta").unwrap()
    )));

    // Multiline composer with paste, Ctrl+W word delete, Shift+Enter newline, and cursor tracking.
    let _ = update(
        &mut model,
        Msg::Action(Action::Activate(NodeKey::Room(room_alpha.clone()))),
    );
    let r_seq = model.stage_req_seq;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(r_seq),
            result: Ok(WireReply::Read(rex_wave_api::read::Page {
                entries: Vec::new(),
                cursor: Cursor::live(),
                timed_out: false,
                total_count: 0,
                truncated: false,
            })),
        },
    );
    let _ = press(&mut model, Pane::Stage, KeyCode::Char('i'));
    assert_eq!(model.focus, Pane::Composer);
    let _ = update(
        &mut model,
        Msg::Action(Action::Paste("first line typo\r\nsecond line".to_owned())),
    );
    // Move up to line 1 end, Ctrl+W deletes "typo", then submit preserving newline.
    let up_act = key_action(
        KeyEvent::new(KeyCode::Up, KeyModifiers::NONE),
        Pane::Composer,
        &model.sidebar.cursor,
    )
    .unwrap();
    let _ = update(&mut model, Msg::Action(up_act));
    let end_act = key_action(
        KeyEvent::new(KeyCode::End, KeyModifiers::NONE),
        Pane::Composer,
        &model.sidebar.cursor,
    )
    .unwrap();
    let _ = update(&mut model, Msg::Action(end_act));
    let cw_act = key_action(
        KeyEvent::new(KeyCode::Char('w'), KeyModifiers::CONTROL),
        Pane::Composer,
        &model.sidebar.cursor,
    )
    .unwrap();
    let _ = update(&mut model, Msg::Action(cw_act));
    let _ = press(&mut model, Pane::Composer, KeyCode::Backspace);
    let submit_fx = press(&mut model, Pane::Composer, KeyCode::Enter);
    assert!(submit_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::RoomPost(rp), .. }
            if rp.body.as_str() == "first line\nsecond line"
    )));
}

#[test]
fn retired_errored_glyph_human_names_auto_fold_and_archive_round_trip() {
    let me = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    let sam = Principal::Seat {
        seat: SeatName::parse("sam").unwrap(),
    };
    let proj = BbProjectId::parse("proj_demo_alpha").unwrap();
    let active_wave = WaveSlug::parse("auth-v2").unwrap();
    let done_wave = WaveSlug::parse("release-v1").unwrap();
    let active_room = RoomId::Channel {
        scope: RoomScope::Project {
            project: proj.clone(),
        },
        name: ChannelName::parse("wave-auth-v2").unwrap(),
    };
    let done_room = RoomId::Channel {
        scope: RoomScope::Project {
            project: proj.clone(),
        },
        name: ChannelName::parse("wave-release-v1").unwrap(),
    };
    let proj_chan = RoomId::Channel {
        scope: RoomScope::Project {
            project: proj.clone(),
        },
        name: ChannelName::parse("general").unwrap(),
    };
    let dm_room = RoomId::Direct {
        pair: rex_wave_api::room::PrincipalPair::new(me.clone(), sam.clone()).unwrap(),
    };

    let mut overview = SidebarTreeSnapshot {
        projects: vec![rex_wave_api::read::ProjectBranch {
            project: proj.clone(),
            name: Some("demo-work".to_owned()),
            archived: false,
            channels: vec![rex_wave_api::read::ChannelLeaf {
                room: proj_chan.clone(),
                unread: 0,
                mentions: 0,
                archived: false,
            }],
            waves: vec![
                rex_wave_api::read::WaveRoomBranch {
                    wave: active_wave.clone(),
                    room: active_room.clone(),
                    unread: 0,
                    mentions: 0,
                    finished: None,
                    archived: false,
                    workers: vec![rex_wave_api::read::WaveWorkerLeaf {
                        role: RoleName::parse("builder").unwrap(),
                        provider: rex_wave_api::ProviderId::parse("codex").unwrap(),
                        model: None,
                        phase: rex_wave_api::wave::WorkerPhase::Live {
                            thread: rex_wave_api::BbThreadId::parse("thr_build").unwrap(),
                            activity: rex_wave_api::wave::Activity::Working,
                        },
                    }],
                },
                rex_wave_api::read::WaveRoomBranch {
                    wave: done_wave.clone(),
                    room: done_room.clone(),
                    unread: 0,
                    mentions: 0,
                    finished: Some(rex_wave_api::wave::WaveOutcome::Completed {
                        summary: Body::parse("done").unwrap(),
                    }),
                    archived: false,
                    workers: vec![
                        rex_wave_api::read::WaveWorkerLeaf {
                            role: RoleName::parse("clean-bot").unwrap(),
                            provider: rex_wave_api::ProviderId::parse("codex").unwrap(),
                            model: None,
                            phase: rex_wave_api::wave::WorkerPhase::Retired {
                                thread: Some(rex_wave_api::BbThreadId::parse("thr_clean").unwrap()),
                                errored: None,
                                refused: None,
                            },
                        },
                        rex_wave_api::read::WaveWorkerLeaf {
                            role: RoleName::parse("crash-bot").unwrap(),
                            provider: rex_wave_api::ProviderId::parse("claude-code").unwrap(),
                            model: None,
                            phase: rex_wave_api::wave::WorkerPhase::Retired {
                                thread: Some(rex_wave_api::BbThreadId::parse("thr_crash").unwrap()),
                                errored: Some("Insufficient balance (402)".to_owned()),
                                refused: None,
                            },
                        },
                        rex_wave_api::read::WaveWorkerLeaf {
                            role: RoleName::parse("refused-bot").unwrap(),
                            provider: rex_wave_api::ProviderId::parse("claude-code").unwrap(),
                            model: None,
                            phase: rex_wave_api::wave::WorkerPhase::Retired {
                                thread: Some(
                                    rex_wave_api::BbThreadId::parse("thr_refuse").unwrap(),
                                ),
                                errored: None,
                                refused: Some("cyber_safeguard".to_owned()),
                            },
                        },
                    ],
                },
            ],
        }],
        direct_messages: vec![rex_wave_api::read::DmLeaf {
            room: dm_room.clone(),
            with: sam,
            unread: 0,
            archived: false,
        }],
        ..Default::default()
    };

    let mut model = Model::new(me, overview.clone());
    let rows = flatten(&model.sidebar.roots, &model.sidebar.folds);
    assert!(
        rows.iter().any(|r| r.node.label == "demo-work"),
        "sidebar must render human BB project name"
    );
    assert!(
        !rows.iter().any(|r| r.node.label == "proj_demo_alpha"),
        "raw project id must not replace human project name in sidebar"
    );
    assert!(
        rows.iter().any(|r| r.node.label == "builder"),
        "running wave starts expanded"
    );
    assert!(
        !rows
            .iter()
            .any(|r| r.node.label == "clean-bot" || r.node.label == "crash-bot"),
        "finished wave must auto-fold on first sight"
    );

    let done_key = NodeKey::Room(done_room.clone());
    let _ = update(
        &mut model,
        Msg::Action(Action::ToggleFold(done_key.clone())),
    );
    let _ = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Tree,
            frame: Frame::Sidebar(Box::new(overview.clone())),
        },
    );
    let mut term = Terminal::new(TestBackend::new(132, 36)).unwrap();
    let mut hits = FrameHitMap::default();
    let mut cards = Vec::new();
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    let unfolded = screen_text(&term);
    assert!(unfolded.contains("✓ clean-bot [codex]"));
    assert!(unfolded.contains("✕ crash-bot [claude]"));
    assert!(unfolded.contains("⊘ refused-bot [claude]"));
    assert!(!unfolded.contains("✓ refused-bot"));

    let _ = update(
        &mut model,
        Msg::Action(Action::Activate(NodeKey::Project(proj.clone()))),
    );
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    assert!(screen_text(&term).contains("Project · demo-work"));

    let _ = update(&mut model, Msg::Action(Action::Activate(done_key.clone())));
    let room_seq = model.stage_req_seq;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(room_seq),
            result: Ok(WireReply::Read(rex_wave_api::read::Page {
                entries: Vec::new(),
                cursor: Cursor::live(),
                timed_out: false,
                total_count: 0,
                truncated: false,
            })),
        },
    );
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    assert!(screen_text(&term).contains("Room · #demo-work/wave-release-v1"));

    let _ = update(&mut model, Msg::Action(Action::Activate(NodeKey::Kanban)));
    let board_seq = model.stage_req_seq;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(board_seq),
            result: Ok(WireReply::BoardView(rex_wave_api::board::BoardSnapshot {
                board: rex_wave_api::board::project_board(&proj),
                head: 0,
                lanes: Vec::new(),
            })),
        },
    );
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    assert!(screen_text(&term).contains("Kanban · demo-work"));

    let crash_key = NodeKey::Agent {
        wave: done_wave.clone(),
        role: RoleName::parse("crash-bot").unwrap(),
    };
    let _ = update(&mut model, Msg::Action(Action::Activate(crash_key)));
    let crash_seq = model.stage_req_seq;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(crash_seq),
            result: Ok(WireReply::ThreadPage(rex_wave_api::read::ThreadPageView {
                thread: rex_wave_api::BbThreadId::parse("thr_crash").unwrap(),
                activity: rex_wave_api::wave::Activity::Idle,
                events: Vec::new(),
                usage: None,
                pending: None,
                next: None,
            })),
        },
    );
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    let crash_screen = screen_text(&term);
    assert!(crash_screen.contains("✕ RETIRED (ERRORED)"));
    assert!(crash_screen.contains("Insufficient balance (402)"));

    model.focus = Pane::Sidebar;
    model.sidebar.cursor = done_key.clone();
    let wave_arch_fx = press(&mut model, Pane::Sidebar, KeyCode::Char('z'));
    assert!(wave_arch_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::SetArchived(sa), .. }
            if sa.archived && sa.target == rex_wave_api::ArchiveTarget::Wave { wave: done_wave.clone() }
    )));

    model.sidebar.cursor = NodeKey::Project(proj.clone());
    let proj_arch_fx = press(&mut model, Pane::Sidebar, KeyCode::Char('z'));
    assert!(proj_arch_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::SetArchived(sa), .. }
            if sa.archived && sa.target == rex_wave_api::ArchiveTarget::Project { project: proj.clone() }
    )));

    model.sidebar.cursor = NodeKey::Room(proj_chan.clone());
    let chan_arch_fx = press(&mut model, Pane::Sidebar, KeyCode::Char('z'));
    assert!(chan_arch_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::SetArchived(sa), .. }
            if sa.archived && sa.target == rex_wave_api::ArchiveTarget::Room { room: proj_chan.clone() }
    )));

    model.sidebar.cursor = NodeKey::Dm(dm_room.clone());
    let dm_arch_fx = press(&mut model, Pane::Sidebar, KeyCode::Char('z'));
    assert!(dm_arch_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::SetArchived(sa), .. }
            if sa.archived && sa.target == rex_wave_api::ArchiveTarget::Room { room: dm_room.clone() }
    )));

    model.sidebar.cursor = done_key.clone();
    overview.projects[0].waves[1].archived = true;
    let hide_fx = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Tree,
            frame: Frame::Sidebar(Box::new(overview.clone())),
        },
    );
    assert_eq!(model.sidebar.cursor, NodeKey::Project(proj.clone()));
    assert!(
        hide_fx
            .iter()
            .any(|e| matches!(e, Effect::DebounceHover(NodeKey::Project(p)) if p == &proj))
    );
    let _ = update(
        &mut model,
        Msg::HoverSettled(NodeKey::Project(proj.clone())),
    );
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    let hidden_screen = screen_text(&term);
    assert!(!hidden_screen.contains("release-v1"));

    let _ = press(&mut model, Pane::Sidebar, KeyCode::Char('A'));
    assert!(model.sidebar.show_archived);
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    let shown_screen = screen_text(&term);
    assert!(shown_screen.contains("Org Tree [archived]"));
    assert!(shown_screen.contains("release-v1"));
    assert!(shown_screen.contains("[archived]"));

    model.sidebar.cursor = done_key;
    let unarchive_fx = press(&mut model, Pane::Sidebar, KeyCode::Char('z'));
    assert!(unarchive_fx.iter().any(|e| matches!(
        e,
        Effect::Call { cmd: WireCommand::SetArchived(sa), .. }
            if !sa.archived && sa.target == rex_wave_api::ArchiveTarget::Wave { wave: done_wave.clone() }
    )));

    let builder_key = NodeKey::Agent {
        wave: active_wave.clone(),
        role: RoleName::parse("builder").unwrap(),
    };
    model.sidebar.cursor = builder_key.clone();
    overview.projects[0].waves[0].finished = Some(rex_wave_api::wave::WaveOutcome::Completed {
        summary: Body::parse("finished while inspecting").unwrap(),
    });
    let finish_fx = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Tree,
            frame: Frame::Sidebar(Box::new(overview.clone())),
        },
    );
    assert_eq!(model.sidebar.cursor, builder_key);
    assert!(
        model
            .sidebar
            .folds
            .is_expanded(&NodeKey::Room(active_room.clone()))
    );
    assert!(
        !finish_fx
            .iter()
            .any(|e| matches!(e, Effect::DebounceHover(_)))
    );

    model.sidebar.cursor = NodeKey::Project(proj);
    let _ = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Tree,
            frame: Frame::Sidebar(Box::new(overview)),
        },
    );
    assert!(model.sidebar.folds.is_expanded(&NodeKey::Room(active_room)));
}

#[tokio::test]
async fn watch_connection_drives_reconnecting_badge_and_recovers_stage_on_reconnect() {
    let me = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    let mut model = Model::new(me, SidebarTreeSnapshot::default());
    model.has_connection_watch = true;
    let (conn_tx, conn_rx) = tokio::sync::watch::channel(true);
    let mut conn_slot = Some(conn_rx);

    let mut term = Terminal::new(TestBackend::new(124, 24)).unwrap();
    let mut hits = FrameHitMap::default();
    let mut cards = Vec::new();
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    assert!(screen_text(&term).contains("● connected"));

    conn_tx.send(false).unwrap();
    let drop_msg = rex_wave_tui::poll_connection(&mut conn_slot).await;
    assert!(matches!(drop_msg, Msg::ConnectionChanged(false)));
    let _ = update(&mut model, drop_msg);
    assert_eq!(
        model.connection,
        rex_wave_tui::ConnectionState::Reconnecting
    );

    let err_seq = model.stage_req_seq;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(err_seq),
            result: Err(rex_wave_api::ApiError::Transport(
                "reconnecting to daemon".to_owned(),
            )),
        },
    );
    assert!(matches!(model.stage, Stage::Error { .. }));

    let buffered_frame_fx = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Tree,
            frame: Frame::Sidebar(Box::default()),
        },
    );
    assert_eq!(
        model.connection,
        rex_wave_tui::ConnectionState::Reconnecting
    );
    assert!(matches!(model.stage, Stage::Error { .. }));
    assert!(
        !buffered_frame_fx
            .iter()
            .any(|e| matches!(e, Effect::Retarget(_)))
    );

    let buffered_reply_fx = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::Sidebar,
            result: Ok(WireReply::Sidebar(SidebarTreeSnapshot::default())),
        },
    );
    assert_eq!(
        model.connection,
        rex_wave_tui::ConnectionState::Reconnecting
    );
    assert!(matches!(model.stage, Stage::Error { .. }));
    assert!(
        !buffered_reply_fx
            .iter()
            .any(|e| matches!(e, Effect::Retarget(_)))
    );

    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    assert!(screen_text(&term).contains("○ reconnecting"));

    conn_tx.send(true).unwrap();
    let up_msg = rex_wave_tui::poll_connection(&mut conn_slot).await;
    assert!(matches!(up_msg, Msg::ConnectionChanged(true)));
    let recover_fx = update(&mut model, up_msg);
    assert_eq!(model.connection, rex_wave_tui::ConnectionState::Connected);
    assert!(matches!(model.stage, Stage::Loading(StageTarget::Inbox)));
    assert!(
        recover_fx
            .iter()
            .any(|e| matches!(e, Effect::EnsureTreeWatch))
    );
    assert!(recover_fx.iter().any(|e| matches!(
        e,
        Effect::Call {
            tag: ReplyTag::Sidebar,
            cmd: WireCommand::Sidebar(_),
        }
    )));
    assert!(
        recover_fx
            .iter()
            .any(|e| matches!(e, Effect::Retarget(StageTarget::Inbox)))
    );
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    assert!(screen_text(&term).contains("● connected"));

    drop(conn_tx);
    let closed_msg = rex_wave_tui::poll_connection(&mut conn_slot).await;
    assert!(conn_slot.is_none());
    assert!(matches!(closed_msg, Msg::ConnectionClosed));
    let _ = update(&mut model, closed_msg);
    assert_eq!(
        model.connection,
        rex_wave_tui::ConnectionState::Disconnected
    );
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(5),
            rex_wave_tui::poll_connection(&mut conn_slot),
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn settings_stage_supports_hotkey_cycling_inline_composer_reset_and_live_events() {
    use rex_wave_api::config::{ConfigEvent, RexConfig, RunnerRoute};
    use rex_wave_tui::input::{Action, Dir, Pane};
    use rex_wave_tui::{ComposerTarget, demo::DemoClient};

    let client = DemoClient::new();
    let overview = match client
        .call_wire(WireCommand::Sidebar(rex_wave_api::read::Sidebar::default()))
        .await
        .unwrap()
    {
        WireReply::Sidebar(s) => s,
        other => panic!("unexpected: {other:?}"),
    };
    let mut model = Model::new(client.me().clone(), overview);

    // Press `,` (`Action::OpenSettings`) to jump straight to Settings stage.
    let open_fx = update(&mut model, Msg::Action(Action::OpenSettings));
    assert_eq!(model.sidebar.cursor, NodeKey::Settings);
    assert_eq!(model.focus, Pane::Stage);
    assert!(
        open_fx
            .iter()
            .any(|e| matches!(e, Effect::Retarget(StageTarget::Settings)))
    );
    let seq = model.stage_req_seq;
    let (init_res, _) =
        rex_wave_tui::load_stage_target(&client, &model.sidebar_snapshot, &StageTarget::Settings)
            .await;
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(seq),
            result: init_res.unwrap(),
        },
    );
    assert!(matches!(model.stage, Stage::Settings(_)));

    // Render and verify health badges + couriers + settings rows appear.
    let mut term = Terminal::new(TestBackend::new(132, 38)).unwrap();
    let mut hits = FrameHitMap::default();
    let mut cards = Vec::new();
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    let scr = screen_text(&term);
    assert!(scr.contains("Settings · Operator Configuration"));
    assert!(scr.contains("BB"));
    assert!(scr.contains("T3"));
    assert!(scr.contains("Native"));
    assert!(scr.contains("demo-toast"));
    assert!(scr.contains("runners.default_runner"));

    // Cycle `runners.default_runner` via `l` (`Move(Dir::Right)`) -> `bb`, then `Space` -> `t3`.
    for act in [Action::Move(Dir::Right), Action::StageSpace] {
        for eff in update(&mut model, Msg::Action(act)) {
            if let Effect::Call { tag, cmd } = eff {
                let res = client.call_wire(cmd).await;
                let _ = update(&mut model, Msg::Reply { tag, result: res });
            }
        }
    }
    if let Stage::Settings(ref s) = model.stage {
        assert_eq!(
            s.snapshot.config.runners.default_runner,
            Some(RunnerRoute::T3)
        );
    } else {
        panic!("expected Stage::Settings");
    }

    // Move to `harnesses.claude.model` (index 6) and open inline composer via Enter.
    let _ = update(&mut model, Msg::Action(Action::SelectSetting(6)));
    let _ = update(&mut model, Msg::Action(Action::Open));
    assert_eq!(model.focus, Pane::Composer);
    assert_eq!(
        model.composer_target,
        Some(ComposerTarget::ConfigKey {
            key: "harnesses.claude.model".into(),
        })
    );
    let _ = update(
        &mut model,
        Msg::Action(Action::Paste("claude-opus-5-5".into())),
    );
    for eff in update(&mut model, Msg::Action(Action::Submit)) {
        if let Effect::Call { tag, cmd } = eff {
            let res = client.call_wire(cmd).await;
            let _ = update(&mut model, Msg::Reply { tag, result: res });
        }
    }
    assert_eq!(model.focus, Pane::Stage);
    if let Stage::Settings(ref s) = model.stage {
        assert_eq!(
            s.snapshot.config.get_key("harnesses.claude.model").unwrap(),
            Some("claude-opus-5-5".into())
        );
    }

    // Reopen composer with `i` -> pre-populated with current value; clear and empty-submit -> resets to None.
    let _ = update(&mut model, Msg::Action(Action::Compose));
    assert_eq!(model.focus, Pane::Composer);
    if let Stage::Settings(ref mut s) = model.stage {
        assert_eq!(s.composer.as_str(), "claude-opus-5-5");
        let _ = s.composer.take();
    }
    for eff in update(&mut model, Msg::Action(Action::Submit)) {
        if let Effect::Call { tag, cmd } = eff {
            let res = client.call_wire(cmd).await;
            let _ = update(&mut model, Msg::Reply { tag, result: res });
        }
    }
    if let Stage::Settings(ref s) = model.stage {
        assert_eq!(
            s.snapshot.config.get_key("harnesses.claude.model").unwrap(),
            None,
            "empty composer submit on ConfigKey must reset to None"
        );
    }

    // Live `Event::Config` frame updates SettingsView immediately.
    let mut external_cfg = RexConfig::default();
    external_cfg.mail.export_enabled = true;
    let live_env = Envelope {
        stream: StreamId::Config,
        seq: Seq(42),
        at: UnixMs(1_700_000_000_000),
        by: client.me().clone(),
        event: Event::Config(ConfigEvent::Updated {
            key: "mail.export_enabled".into(),
            value: Some("true".into()),
            config: Box::new(external_cfg),
        }),
    };
    let _ = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Stage,
            frame: Frame::Committed(live_env),
        },
    );
    if let Stage::Settings(ref s) = model.stage {
        assert_eq!(s.snapshot.head, Seq(42));
        assert!(s.snapshot.config.mail.export_enabled);
        assert_eq!(
            s.status_banner.as_deref(),
            Some("Saved mail.export_enabled = true")
        );
    }

    // Stale `Event::Config` (`seq <= head`) and stale `ConfigSet` reply (`head < current`) must be ignored.
    let mut stale_cfg = RexConfig::default();
    stale_cfg.runners.default_runner = Some(RunnerRoute::Bb);
    let stale_env = Envelope {
        stream: StreamId::Config,
        seq: Seq(10),
        at: UnixMs(1_700_000_000_001),
        by: client.me().clone(),
        event: Event::Config(ConfigEvent::Updated {
            key: "runners.default_runner".into(),
            value: Some("bb".into()),
            config: Box::new(stale_cfg.clone()),
        }),
    };
    let _ = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Stage,
            frame: Frame::Committed(stale_env),
        },
    );
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::UserCommand,
            result: Ok(WireReply::ConfigSet(rex_wave_api::config::ConfigSnapshot {
                head: Seq(10),
                config: stale_cfg,
                config_path: None,
                effective_default_runner: Some(RunnerRoute::Bb),
                runners_health: Vec::new(),
                couriers_health: Vec::new(),
                persist_error: None,
                mail_failures: Vec::new(),
            })),
        },
    );
    if let Stage::Settings(ref s) = model.stage {
        assert_eq!(s.snapshot.head, Seq(42));
        assert!(s.snapshot.config.mail.export_enabled);
        assert_eq!(s.snapshot.config.runners.default_runner, None);
    }

    // Restart-only keys annotate the banner with `(restart required)`.
    let mut grace_cfg = RexConfig::default();
    grace_cfg.mail.reconnect_grace_ms = 5000;
    let grace_env = Envelope {
        stream: StreamId::Config,
        seq: Seq(43),
        at: UnixMs(1_700_000_000_002),
        by: client.me().clone(),
        event: Event::Config(ConfigEvent::Updated {
            key: "mail.reconnect_grace_ms".into(),
            value: Some("5000".into()),
            config: Box::new(grace_cfg),
        }),
    };
    let _ = update(
        &mut model,
        Msg::Frame {
            watch: WatchSlot::Stage,
            frame: Frame::Committed(grace_env),
        },
    );
    if let Stage::Settings(ref s) = model.stage {
        assert_eq!(s.snapshot.head, Seq(43));
        assert_eq!(
            s.status_banner.as_deref(),
            Some("Saved mail.reconnect_grace_ms = 5000 (restart required)")
        );
    }

    // Failed ConfigKey submission preserves the typed draft and updates status_banner.
    model.composer_target = Some(ComposerTarget::ConfigKey {
        key: "mail.mirror_dir".into(),
    });
    model.focus = Pane::Composer;
    if let Stage::Settings(ref mut s) = model.stage {
        let _ = s.composer.take();
        s.composer.insert_str("relative/bad/path");
    }
    let _ = update(&mut model, Msg::Action(Action::Submit));
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::UserCommand,
            result: Err(rex_wave_api::ApiError::Invalid(rex_wave_api::InvalidId {
                what: "mail.mirror_dir".into(),
                reason: "must be absolute".into(),
            })),
        },
    );
    if let Stage::Settings(ref s) = model.stage {
        assert_eq!(s.composer.as_str(), "relative/bad/path");
        assert!(
            s.status_banner
                .as_deref()
                .is_some_and(|b| b.starts_with("Error:")),
            "unexpected status_banner: {:?}",
            s.status_banner
        );
    }

    // `h` (`Move(Dir::Left)`) returns focus to Sidebar.
    let _ = update(&mut model, Msg::Action(Action::Move(Dir::Left)));
    assert_eq!(model.focus, Pane::Sidebar);
}

#[tokio::test]
async fn passive_clickable_mail_failure_badge_opens_settings_without_interrupting_operator() {
    use rex_wave_api::IncarnationId;
    use rex_wave_api::config::{ConfigSnapshot, CourierHealthStatus, RexConfig};
    use rex_wave_api::mail::{DeliveryFailureKind, MailDeliveryFailure};
    use rex_wave_api::read::MailHealthBadge;
    use rex_wave_tui::demo::DemoClient;
    use rex_wave_tui::input::{Action, Target};

    let client = DemoClient::new();
    let mut overview = match client
        .call_wire(WireCommand::Sidebar(rex_wave_api::read::Sidebar::default()))
        .await
        .unwrap()
    {
        WireReply::Sidebar(s) => s,
        other => panic!("unexpected: {other:?}"),
    };
    let inbox_before = overview.inbox;
    let osc_before = overview.osc7501.clone();
    overview.mail_health = MailHealthBadge {
        failed_deliveries: 2,
        degraded_couriers: 1,
    };

    let mut model = Model::new(client.me().clone(), overview.clone());
    assert_eq!(model.sidebar_snapshot.inbox, inbox_before);
    assert_eq!(model.sidebar_snapshot.osc7501, osc_before);

    let mut term = Terminal::new(TestBackend::new(132, 38)).unwrap();
    let mut hits = FrameHitMap::default();
    let mut cards = Vec::new();
    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    let scr_before = screen_text(&term);
    assert!(
        scr_before.contains("Settings")
            && scr_before.contains("2 mail")
            && scr_before.contains("1 deg"),
        "Settings row must render passive mail failure + degraded badge:\n{scr_before}"
    );

    // Clicking the passive Settings row retargets directly to StageTarget::Settings.
    let settings_row_hit =
        (0..38u16).find(|&r| matches!(hits.hit(4, r), Some(Target::Node(NodeKey::Settings))));
    assert!(
        settings_row_hit.is_some(),
        "Settings row must be clickable in FrameHitMap"
    );
    let click_fx = update(&mut model, Msg::Action(Action::Activate(NodeKey::Settings)));
    assert!(
        click_fx
            .iter()
            .any(|e| matches!(e, Effect::Retarget(StageTarget::Settings)))
    );

    let seq = model.stage_req_seq;
    let snap_with_failures = ConfigSnapshot {
        head: Seq(7),
        config: RexConfig::default(),
        config_path: None,
        effective_default_runner: None,
        runners_health: Vec::new(),
        couriers_health: vec![CourierHealthStatus {
            name: "t3-thread".into(),
            executable: true,
            rung: 0,
            unknown: 3,
            refused: 1,
            degraded: true,
        }],
        persist_error: None,
        mail_failures: vec![
            MailDeliveryFailure {
                seq: Seq(5),
                at: UnixMs(1_795_261_554_000),
                kind: DeliveryFailureKind::Refused,
                courier: "t3-thread".into(),
                seat: SeatName::parse("bob").unwrap(),
                incarnation_id: Some(IncarnationId::parse("inc-bob-1").unwrap()),
                expected_epoch: Some(1),
                is_canary: false,
                sha256s: vec![],
                sender_seat: Some(SeatName::parse("alice").unwrap()),
                hub_owner_seat: SeatName::parse("postmaster").unwrap(),
                reason: "address-gone".into(),
                detail: Some("thread archived".into()),
            },
            MailDeliveryFailure {
                seq: Seq(6),
                at: UnixMs(1_795_261_555_000),
                kind: DeliveryFailureKind::AdapterDegraded,
                courier: "t3-thread".into(),
                seat: SeatName::parse("bob").unwrap(),
                incarnation_id: None,
                expected_epoch: None,
                is_canary: false,
                sha256s: vec![],
                sender_seat: None,
                hub_owner_seat: SeatName::parse("postmaster").unwrap(),
                reason: "consecutive-unknown".into(),
                detail: None,
            },
        ],
    };
    let _ = update(
        &mut model,
        Msg::Reply {
            tag: ReplyTag::StageInitial(seq),
            result: Ok(WireReply::ConfigShow(snap_with_failures)),
        },
    );

    term.draw(|f| view(&model, f, &mut hits, &mut cards))
        .unwrap();
    let scr_after = screen_text(&term);
    assert!(
        scr_after.contains("Mail Delivery Failures (2)"),
        "Settings stage must display Mail Delivery Failures section:\n{scr_after}"
    );
    assert!(scr_after.contains("address-gone"));
    assert!(scr_after.contains("adapter_degraded"));
    assert!(scr_after.contains("owner=@postmaster"));
}
