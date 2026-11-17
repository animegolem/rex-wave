use futures_util::StreamExt;
use rex_kanban::{ExternalLink, Lane};
use rex_wave_api::board::*;
use rex_wave_api::read::Sidebar;
use rex_wave_api::stage::StageTarget;
use rex_wave_api::*;
use rex_wave_board::{Board, default_lanes, task_move};
use rex_wave_runtime::*;
use rex_wave_store::Journal;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

fn build_router(journal: Journal, idle: Duration) -> Arc<Router> {
    let hub = Hub::new(256);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    Arc::new(Router {
        room: Arc::new(Unavailable(Family::Room)),
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Registry::<Board>::with_idle(journal.clone(), hub.clone(), idle),
        inbox: Arc::new(Unavailable(Family::Inbox)),
        mail: Arc::new(Unavailable(Family::Mail)),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    })
}

fn open_wal_router(path: &Path, idle: Duration) -> Arc<Router> {
    let journal = Journal::open(path).expect("open sqlite wal journal");
    build_router(journal, idle)
}

#[tokio::test]
async fn board_decider_cas_idem_moves_links_and_wal_passivation_rehydration() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("board-wal.sqlite3");

    let board = project_board(&BbProjectId::parse("proj_3vs7nkyeum").unwrap());
    let sol_principal = Principal::Operator { seat: SeatName::parse("sol").unwrap() };
    let worker_principal = Principal::Worker {
        wave: WaveSlug::parse("auth-v2").unwrap(),
        role: RoleName::parse("author-jwt").unwrap(),
    };

    {
        let router = open_wal_router(&db_path, Duration::from_millis(25));
        let sol = LocalClient::new(router.clone(), sol_principal.clone());
        let worker = LocalClient::new(router.clone(), worker_principal.clone());

        let missing = sol
            .call(BoardView { board: board.clone(), filter: None })
            .await
            .unwrap_err();
        assert!(matches!(missing, ApiError::NotFound { .. }));

        let info1 = sol
            .call(BoardEnsure { board: board.clone(), lanes: Some(Lane::all_lane_names()) })
            .await
            .unwrap();
        let info2 = sol
            .call(BoardEnsure { board: board.clone(), lanes: None })
            .await
            .unwrap();
        assert_eq!(info1.lanes, Lane::all_lane_names());
        assert_eq!(info1, info2, "board_ensure is idempotent once opened");
        assert_eq!(default_lanes().len(), 4);

        let create_idem = rex_kanban::IdemKey::parse("card-create-1").unwrap();
        let c1 = worker
            .call(CardUpsert {
                board: board.clone(),
                card: None,
                expected: None,
                patch: CardPatch {
                    title: Some(Title::parse("Wire OAuth refresh").unwrap()),
                    lane: Some(Lane::Todo.lane_name()),
                    assignee: Some(Assign::To { who: assignee(&worker_principal) }),
                    priority: Some(Priority::High),
                    labels_add: vec!["auth".into(), "wave".into()],
                    ..Default::default()
                },
                idem: Some(create_idem.clone()),
            })
            .await
            .unwrap();
        assert_eq!(c1.no, CardNo(1));
        assert_eq!(c1.revision, CardRevision(1));

        let c2 = sol
            .call(CardUpsert {
                board: board.clone(),
                card: None,
                expected: None,
                patch: CardPatch {
                    title: Some(Title::parse("Audit token scopes").unwrap()),
                    lane: Some(Lane::Todo.lane_name()),
                    position: Some(0),
                    assignee: Some(Assign::To { who: assignee(&sol_principal) }),
                    ..Default::default()
                },
                idem: None,
            })
            .await
            .unwrap();
        assert_eq!(c2.no, CardNo(2));

        let moved = worker
            .call(task_move(
                board.clone(),
                CardNo(1),
                Some(CardRevision(1)),
                Lane::Review.lane_name(),
                Some(0),
                vec![
                    worker_link(
                        &WaveSlug::parse("auth-v2").unwrap(),
                        &RoleName::parse("verifier").unwrap(),
                    ),
                    Link::External(ExternalLink::parse_uri("rex://rooms/wave-auth-v2#9").unwrap()),
                    Link::External(ExternalLink::parse_uri("letter://aabbccddeeff0011").unwrap()),
                ],
                Some(rex_kanban::IdemKey::parse("move-1-review").unwrap()),
            ))
            .await
            .unwrap();
        assert_eq!(moved.revision, CardRevision(2));

        let stale = sol
            .call(task_move(
                board.clone(),
                CardNo(1),
                Some(CardRevision(1)),
                Lane::Done.lane_name(),
                None,
                vec![],
                None,
            ))
            .await
            .unwrap_err();
        assert!(matches!(stale, ApiError::Conflict { .. }), "stale revision fails CAS");

        let replayed_create = worker
            .call(CardUpsert {
                board: board.clone(),
                card: None,
                expected: None,
                patch: CardPatch {
                    title: Some(Title::parse("Mutated retry body").unwrap()),
                    ..Default::default()
                },
                idem: Some(create_idem),
            })
            .await
            .unwrap();
        assert_eq!(
            replayed_create, c1,
            "idem retry after later patches still returns original CardRevision(1) receipt"
        );

        tokio::time::sleep(Duration::from_millis(60)).await;

        let c3 = sol
            .call(CardUpsert {
                board: board.clone(),
                card: None,
                expected: None,
                patch: CardPatch {
                    title: Some(Title::parse("After passivation").unwrap()),
                    lane: Some(Lane::Done.lane_name()),
                    ..Default::default()
                },
                idem: None,
            })
            .await
            .unwrap();
        assert_eq!(c3.no, CardNo(3), "rehydrated actor continues card numbering from WAL");
    }

    let restarted_router = open_wal_router(&db_path, Duration::from_secs(60));
    let sol = LocalClient::new(restarted_router.clone(), sol_principal);
    let worker = LocalClient::new(restarted_router, worker_principal);

    let snap = sol
        .call(BoardView { board: board.clone(), filter: None })
        .await
        .unwrap();
    let review_cards = &snap
        .lanes
        .iter()
        .find(|l| l.lane == Lane::Review.lane_name())
        .unwrap()
        .cards;
    assert_eq!(review_cards.len(), 1);
    assert_eq!(review_cards[0].no, CardNo(1));
    assert_eq!(review_cards[0].revision, CardRevision(2));
    assert_eq!(review_cards[0].links.len(), 3);

    let sidebar = worker.call(Sidebar::default()).await.unwrap();
    let badge = sidebar.kanban.iter().find(|b| b.board == board).unwrap();
    assert_eq!(badge.open, 2, "cards 1 and 2 are open; card 3 in done is excluded");
    assert_eq!(badge.mine, 1, "card 1 is assigned to worker");
}

#[tokio::test]
async fn implicit_board_open_stage_watch_and_org_import_export_round_trip() {
    let journal = Journal::in_memory().unwrap();
    let router = build_router(journal, Duration::from_secs(60));
    let sol = LocalClient::new(
        router,
        Principal::Operator { seat: SeatName::parse("sol").unwrap() },
    );

    let personal = seat_board(&SeatName::parse("sol").unwrap());
    let mut stage = sol
        .watch(
            Interest::Stage { target: StageTarget::Kanban { board: personal.clone() } },
            Cursor::default(),
        )
        .await
        .unwrap();
    assert!(matches!(
        stage.next().await,
        Some(Ok(Frame::StageFocused { generation: FocusGeneration(1), .. }))
    ));
    assert!(matches!(
        stage.next().await,
        Some(Ok(Frame::Stage { generation: FocusGeneration(1), inner }))
            if matches!(*inner, Frame::CaughtUp { .. })
    ));

    let auto_idem = rex_kanban::IdemKey::parse("implicit-open-1").unwrap();
    let r1 = sol
        .call(CardUpsert {
            board: personal.clone(),
            card: None,
            expected: None,
            patch: CardPatch {
                title: Some(Title::parse("Implicitly open personal board").unwrap()),
                priority: Some(Priority::Blocking),
                labels_add: vec!["personal".into()],
                links_add: vec![Link::External(ExternalLink::parse_uri("wave:pr2/owner").unwrap())],
                ..Default::default()
            },
            idem: Some(auto_idem.clone()),
        })
        .await
        .unwrap();
    assert_eq!(r1.no, CardNo(1));
    assert_eq!(r1.revision, CardRevision(1));

    let r1_retry = sol
        .call(CardUpsert {
            board: personal.clone(),
            card: None,
            expected: None,
            patch: CardPatch {
                title: Some(Title::parse("Duplicate retry").unwrap()),
                ..Default::default()
            },
            idem: Some(auto_idem),
        })
        .await
        .unwrap();
    assert_eq!(r1_retry, r1, "multi-event [Opened, CardCreated] batch replays idempotently");

    for _ in 0..2 {
        let frame = tokio::time::timeout(Duration::from_secs(1), stage.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(frame, Frame::Stage { generation: FocusGeneration(1), .. }));
    }

    let snap = sol
        .call(BoardView { board: personal.clone(), filter: None })
        .await
        .unwrap();
    let org_text = rex_kanban::org::render(&snap);
    assert!(org_text.contains("#+TITLE: seat:sol"));
    assert!(org_text.contains("* TODO [#A] Implicitly open personal board :personal:"));
    assert!(org_text.contains(":LINKS: wave:pr2/owner"));
    assert_eq!(rex_kanban::org::parse(&org_text).unwrap(), snap.lanes[0].cards);
}
