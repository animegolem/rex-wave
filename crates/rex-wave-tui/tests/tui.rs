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
                seat: SeatName::parse("sol").unwrap(),
            },
            event: Event::Room(RoomEvent::Posted {
                body: Body::parse(text).unwrap(),
                mentions: Mentions::default(),
                reply_to: None,
                idem: None,
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
        title: Title::parse("Live board fold").unwrap(),
        lane: LaneName::parse("review").unwrap(),
        assignee: None,
        priority: Priority::Normal,
        due_ms: None,
        labels: Vec::new(),
        links: Vec::new(),
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
        .call_wire(WireCommand::Read(rex_wave_api::read::Read {
            stream: StreamId::Room {
                room: wave_room.clone(),
            },
            after: Seq::ORIGIN,
            limit: Some(200),
        }))
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
        assert_eq!(rv.scroll, u16::MAX, "room opens scrolled to tail");
        assert_eq!(
            rv.cursor.at(&StreamId::Room {
                room: wave_room.clone()
            }),
            Seq(250)
        );
        rv.scroll = 7;
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
        assert_eq!(rv.scroll, 7);
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
            result: Err(rex_wave_api::ApiError::Conflict {
                detail: "revision mismatch".to_owned(),
            }),
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
