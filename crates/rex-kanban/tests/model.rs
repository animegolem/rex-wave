use rex_kanban::*;

fn apply_all(state: &mut BoardState, events: &[BoardEvent]) {
    for ev in events {
        state.evolve(ev);
    }
}

#[test]
fn validated_strings_lanes_priorities_and_external_links_behave_at_boundary() {
    assert!(BoardId::parse("project:p_123").is_ok());
    assert!(BoardId::parse("").is_err());
    assert!(LaneName::parse("doing").is_ok());
    assert!(LaneName::parse("Bad Lane").is_err());
    assert!(Title::parse("One line").is_ok());
    assert!(Title::parse("two\nlines").is_err());
    assert_eq!(CardRevision::default().next().next(), CardRevision(2));

    assert!(IdemKey::parse("a".repeat(256)).is_ok());
    assert!(IdemKey::parse("a".repeat(257)).is_err());
    assert!(IdemKey::parse("é".repeat(129)).is_err(), "byte-length cap matches wire IdemKey");

    let expected_pairs = [
        (Lane::Idea, "idea", "IDEA"),
        (Lane::Todo, "todo", "TODO"),
        (Lane::Doing, "doing", "STRT"),
        (Lane::Review, "review", "WAIT"),
        (Lane::Blocked, "blocked", "BLKD"),
        (Lane::Done, "done", "DONE"),
    ];
    for (lane, slug, org_kw) in expected_pairs {
        assert_eq!(lane.as_str(), slug);
        assert_eq!(lane.org_keyword(), org_kw);
        assert_eq!(Lane::from_org_keyword(org_kw), Some(lane));
        assert_eq!(Lane::from_lane_name(&lane.lane_name()), Some(lane));
        assert_eq!(Lane::try_from(&lane.lane_name()).unwrap(), lane);
    }
    assert_eq!(Lane::from_org_keyword("UNKNOWN"), None);

    for (prio, cookie, mapped) in [
        (OrgPriority::A, "[#A]", Priority::High),
        (OrgPriority::B, "[#B]", Priority::Normal),
        (OrgPriority::C, "[#C]", Priority::Low),
    ] {
        assert_eq!(prio.cookie(), cookie);
        assert_eq!(OrgPriority::parse_cookie(cookie), Some(prio));
        assert_eq!(Priority::from(prio), mapped);
    }
    assert_eq!(OrgPriority::parse_cookie("[#D]"), None);

    for raw in ["rex://rooms/general#42", "letter://9f86d081884c7d65", "wave:auth-v2/verifier"] {
        let parsed = ExternalLink::parse_uri(raw).unwrap();
        assert_eq!(parsed.to_uri(), raw);
        assert_eq!(parsed.to_string(), raw);
    }
    for bad in ["", "noscheme", "rex://", "wave:", "9bad:key", "rex://bad\nctrl"] {
        assert!(ExternalLink::parse_uri(bad).is_err(), "rejected malformed URI `{bad}`");
    }

    let upsert = CardUpsert {
        board: BoardId::parse("project:p_1").unwrap(),
        card: Some(CardNo(4)),
        expected: Some(CardRevision(2)),
        patch: CardPatch {
            lane: Some(LaneName::parse("review").unwrap()),
            links_add: vec![Link::External(ExternalLink {
                namespace: "rex-wave.worker".into(),
                key: "auth/verifier".into(),
            })],
            ..Default::default()
        },
        idem: Some(IdemKey::parse("idem-1").unwrap()),
    };
    let round: CardUpsert = serde_json::from_value(serde_json::to_value(&upsert).unwrap()).unwrap();
    assert_eq!(round.expected, Some(CardRevision(2)));
    assert_eq!(round.patch.links_add, upsert.patch.links_add);
}

#[test]
fn card_revision_cas_rejects_stale_writes_and_archives_cleanly() {
    let board = BoardId::parse("project:p_cas").unwrap();
    let mut state = BoardState::open(board.clone(), Lane::all_lane_names());

    let err = state
        .decide_upsert(&CardUpsert {
            board: board.clone(),
            card: None,
            expected: None,
            patch: CardPatch::default(),
            idem: None,
        })
        .unwrap_err();
    assert_eq!(err, KanbanError::MissingTitle);

    let created = state
        .decide_upsert(&CardUpsert {
            board: board.clone(),
            card: None,
            expected: None,
            patch: CardPatch {
                title: Some(Title::parse("Implement CAS").unwrap()),
                lane: Some(Lane::Todo.lane_name()),
                priority: Some(Priority::High),
                labels_add: vec!["core".into(), "core".into(), "cas".into()],
                ..Default::default()
            },
            idem: None,
        })
        .unwrap();
    apply_all(&mut state, &created);

    let c1 = &state.cards[&CardNo(1)];
    assert_eq!(c1.revision, CardRevision(1));
    assert_eq!(c1.labels, vec!["core", "cas"]);

    let patched = state
        .decide_upsert(&CardUpsert {
            board: board.clone(),
            card: Some(CardNo(1)),
            expected: Some(CardRevision(1)),
            patch: CardPatch {
                lane: Some(Lane::Doing.lane_name()),
                assignee: Some(Assign::To { who: Assignee::parse("@sol").unwrap() }),
                labels_remove: vec!["core".into()],
                labels_add: vec!["active".into()],
                ..Default::default()
            },
            idem: None,
        })
        .unwrap();
    apply_all(&mut state, &patched);
    assert_eq!(state.cards[&CardNo(1)].revision, CardRevision(2));
    assert_eq!(state.cards[&CardNo(1)].labels, vec!["cas", "active"]);

    let stale_patch = state
        .decide_upsert(&CardUpsert {
            board: board.clone(),
            card: Some(CardNo(1)),
            expected: Some(CardRevision(1)),
            patch: CardPatch { title: Some(Title::parse("Overwrite").unwrap()), ..Default::default() },
            idem: None,
        })
        .unwrap_err();
    assert_eq!(
        stale_patch,
        KanbanError::RevisionConflict {
            card: CardNo(1),
            expected: CardRevision(1),
            actual: CardRevision(2),
        }
    );

    let stale_archive = state
        .decide_archive(&CardArchive {
            board: board.clone(),
            card: CardNo(1),
            expected: Some(CardRevision(1)),
        })
        .unwrap_err();
    assert_eq!(
        stale_archive,
        KanbanError::RevisionConflict {
            card: CardNo(1),
            expected: CardRevision(1),
            actual: CardRevision(2),
        }
    );

    let archived = state
        .decide_archive(&CardArchive {
            board: board.clone(),
            card: CardNo(1),
            expected: Some(CardRevision(2)),
        })
        .unwrap();
    apply_all(&mut state, &archived);
    assert!(!state.cards.contains_key(&CardNo(1)));
    assert_eq!(
        state
            .decide_archive(&CardArchive { board: board.clone(), card: CardNo(1), expected: None })
            .unwrap(),
        vec![],
        "unconditional archive of an already-archived card is idempotent"
    );

    let second = state
        .decide_upsert(&CardUpsert {
            board: board.clone(),
            card: None,
            expected: None,
            patch: CardPatch { title: Some(Title::parse("Next card").unwrap()), ..Default::default() },
            idem: None,
        })
        .unwrap();
    apply_all(&mut state, &second);
    assert!(state.cards.contains_key(&CardNo(2)), "card numbers never reuse archived ids");
}

#[test]
fn deterministic_lane_ordering_and_replay_preserve_positions() {
    let board = BoardId::parse("project:p_rank").unwrap();
    let mut log = Vec::new();
    let mut state = BoardState::default();

    let open = state
        .decide_ensure(&BoardEnsure { board: board.clone(), lanes: Some(Lane::all_lane_names()) })
        .unwrap();
    apply_all(&mut state, &open);
    log.extend(open);

    for title in ["First", "Second", "Third"] {
        let evs = state
            .decide_upsert(&CardUpsert {
                board: board.clone(),
                card: None,
                expected: None,
                patch: CardPatch {
                    title: Some(Title::parse(title).unwrap()),
                    lane: Some(Lane::Todo.lane_name()),
                    ..Default::default()
                },
                idem: None,
            })
            .unwrap();
        apply_all(&mut state, &evs);
        log.extend(evs);
    }

    let top_create = state
        .decide_upsert(&CardUpsert {
            board: board.clone(),
            card: None,
            expected: None,
            patch: CardPatch {
                title: Some(Title::parse("Urgent Top").unwrap()),
                lane: Some(Lane::Todo.lane_name()),
                position: Some(0),
                ..Default::default()
            },
            idem: None,
        })
        .unwrap();
    apply_all(&mut state, &top_create);
    log.extend(top_create);

    assert_eq!(
        state.order[&Lane::Todo.lane_name()],
        vec![CardNo(4), CardNo(1), CardNo(2), CardNo(3)]
    );

    let reorder = state
        .decide_upsert(&CardUpsert {
            board: board.clone(),
            card: Some(CardNo(3)),
            expected: Some(CardRevision(1)),
            patch: CardPatch { position: Some(1), ..Default::default() },
            idem: None,
        })
        .unwrap();
    apply_all(&mut state, &reorder);
    log.extend(reorder);
    assert_eq!(
        state.order[&Lane::Todo.lane_name()],
        vec![CardNo(4), CardNo(3), CardNo(1), CardNo(2)]
    );

    let cross_move = state
        .decide_upsert(&CardUpsert {
            board: board.clone(),
            card: Some(CardNo(1)),
            expected: Some(CardRevision(1)),
            patch: CardPatch {
                lane: Some(Lane::Review.lane_name()),
                position: Some(0),
                assignee: Some(Assign::To { who: Assignee::parse("@astra").unwrap() }),
                ..Default::default()
            },
            idem: None,
        })
        .unwrap();
    apply_all(&mut state, &cross_move);
    log.extend(cross_move);

    let mut replayed = BoardState::default();
    apply_all(&mut replayed, &log);
    assert_eq!(
        replayed.snapshot(log.len() as u64, None),
        state.snapshot(log.len() as u64, None),
        "replaying event log reconstructs identical lane order and card state"
    );

    let filtered = state
        .snapshot(
            log.len() as u64,
            Some(&BoardFilter {
                assignee: Some(Assignee::parse("@astra").unwrap()),
                label: None,
                lane: Some(Lane::Review.lane_name()),
            }),
        )
        .unwrap();
    assert_eq!(filtered.lanes.len(), 1);
    assert_eq!(filtered.lanes[0].cards.len(), 1);
    assert_eq!(filtered.lanes[0].cards[0].no, CardNo(1));
}

#[test]
fn standalone_engine_handles_idem_moves_and_links() {
    let mut engine = KanbanEngine::new();
    let board = BoardId::parse("seat:sol").unwrap();

    let info = engine
        .ensure_board(BoardEnsure { board: board.clone(), lanes: Some(Lane::all_lane_names()) })
        .unwrap();
    assert_eq!(info.lanes.len(), 6);

    let idem = IdemKey::parse("create-1").unwrap();
    let r1 = engine
        .upsert_card(CardUpsert {
            board: board.clone(),
            card: None,
            expected: None,
            patch: CardPatch {
                title: Some(Title::parse("Draft RFC").unwrap()),
                lane: Some(Lane::Idea.lane_name()),
                priority: Some(Priority::High),
                labels_add: vec!["rfc".into()],
                ..Default::default()
            },
            idem: Some(idem.clone()),
        })
        .unwrap();
    assert_eq!(r1.no, CardNo(1));
    assert_eq!(r1.revision, CardRevision(1));

    let moved = engine
        .move_card(
            board.clone(),
            CardNo(1),
            Some(CardRevision(1)),
            Lane::Blocked.lane_name(),
            Some(0),
            vec![
                Link::External(ExternalLink::parse_uri("letter://0123456789abcdef").unwrap()),
                Link::External(ExternalLink::parse_uri("wave:auth-v2/author").unwrap()),
            ],
        )
        .unwrap();
    assert_eq!(moved.revision, CardRevision(2));

    let replay = engine
        .upsert_card(CardUpsert {
            board: board.clone(),
            card: None,
            expected: None,
            patch: CardPatch { title: Some(Title::parse("Different title on retry").unwrap()), ..Default::default() },
            idem: Some(idem),
        })
        .unwrap();
    assert_eq!(replay, r1, "idempotent retry returns original creation receipt");

    let snap = engine
        .view_board(BoardView { board: board.clone(), filter: None })
        .unwrap();
    let org_rendered = org::render(&snap);
    let parsed_cards = org::parse(&org_rendered).unwrap();
    assert_eq!(parsed_cards.len(), 1);
    assert_eq!(parsed_cards[0].links.len(), 2);
}
