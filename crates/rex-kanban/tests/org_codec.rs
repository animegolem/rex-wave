use rex_kanban::org::{OrgDocument, OrgError, parse, parse_document, render, render_document};
use rex_kanban::*;
use std::path::PathBuf;

#[test]
fn canonical_org_document_round_trips_byte_for_byte_preserving_unknown_properties_and_notes() {
    let canonical = "\
#+TITLE: project:p_123
#+STARTUP: overview

* IDEA [#B] Explore zero-copy framing :research:transport:
:PROPERTIES:
:ID: src-bush1951
:REX_CARD: 1
:REX_REV: 1
:EFFORT: 2h
:OWNER_NOTE: keep this exact custom value
:EMPTY_CUSTOM:
:END:
Initial sketch notes.

Second paragraph with *bold* emphasis and a sub-outline:
** Sub-bullet inside card body
- [ ] benchmark latency

* TODO [#A] Wire OAuth refresh :auth:backend:
:PROPERTIES:
:ID: 550e8400-e29b-41d4-a716-446655440000
:REX_CARD: 2
:REX_REV: 4
:REX_NEEDS: src-bush1951 rw-0123456789abcdef
:ASSIGNEE: @auth-v2/author-jwt
:DUE_MS: 1763280000000
:LINKS: rex://room/general#42 letter://deadbeefcafe0123 wave:auth-v2/verifier https://example.com/a%2Fb
:CATEGORY: security
:END:
Ensure refresh tokens rotate atomically.

* STRT Implement CAS checks :kanban:
:PROPERTIES:
:ID: rw-0123456789abcdef
:REX_CARD: 3
:REX_REV: 2
:ASSIGNEE: @sam
:END:

* WAIT [#C] Cross-model Sam review :review:
:PROPERTIES:
:ID: rw-fedcba9876543210
:REX_CARD: 4
:REX_REV: 1
:END:
Waiting on BB review thread.

* BLKD [#A] Provision staging credentials :infra:
:PROPERTIES:
:ID: emacs-custom-ops-99
:REX_CARD: 5
:REX_REV: 3
:PRIORITY: blocking
:LINKS: org+heading:docs/ops.org::Infra/Credentials\\x2FKeys
:TICKET: OPS-99
:END:

* DONE Ship Gate 1 foundation :milestone:
:PROPERTIES:
:ID: rw-1111222233334444
:REX_CARD: 6
:REX_REV: 5
:END:
";

    let doc: OrgDocument = parse_document(canonical).expect("valid canonical org");
    assert_eq!(doc.title.as_deref(), Some("project:p_123"));
    assert_eq!(doc.preamble, vec!["#+STARTUP: overview"]);
    assert_eq!(doc.tasks.len(), 6);

    assert_eq!(
        doc.tasks[0].org_id.as_ref().map(OrgId::as_str),
        Some("src-bush1951")
    );
    assert_eq!(doc.tasks[0].id, Some(CardNo(1)));
    assert_eq!(doc.tasks[0].canonical_lane(), Some(Lane::Idea));
    assert_eq!(doc.tasks[0].priority, Some(OrgPriority::B));
    assert_eq!(
        doc.tasks[0].properties,
        vec![
            ("EFFORT".to_owned(), "2h".to_owned()),
            (
                "OWNER_NOTE".to_owned(),
                "keep this exact custom value".to_owned()
            ),
            ("EMPTY_CUSTOM".to_owned(), String::new()),
        ]
    );
    assert!(doc.tasks[0].body.contains("** Sub-bullet inside card body"));

    assert_eq!(
        doc.tasks[1].org_id.as_ref().map(OrgId::as_str),
        Some("550e8400-e29b-41d4-a716-446655440000")
    );
    assert_eq!(
        doc.tasks[1].needs,
        vec![
            OrgId::parse("src-bush1951").unwrap(),
            OrgId::parse("rw-0123456789abcdef").unwrap(),
        ]
    );
    assert_eq!(doc.tasks[1].canonical_lane(), Some(Lane::Todo));
    assert_eq!(doc.tasks[1].links.len(), 4);
    assert_eq!(
        doc.tasks[1].links[3],
        Link::External(ExternalLink::parse_uri("https://example.com/a%2Fb").unwrap()),
        "percent-escaped URI targets stay opaque and match ExternalLink::parse_uri"
    );
    assert_eq!(doc.tasks[2].canonical_lane(), Some(Lane::Doing));
    assert_eq!(doc.tasks[3].canonical_lane(), Some(Lane::Review));
    assert_eq!(doc.tasks[4].canonical_lane(), Some(Lane::Blocked));
    assert!(doc.tasks[4].blocking);
    assert_eq!(
        doc.tasks[4].links,
        vec![Link::Org {
            file: PathBuf::from("docs/ops.org"),
            outline: vec!["Infra".into(), "Credentials/Keys".into()],
        }]
    );
    assert_eq!(doc.tasks[5].canonical_lane(), Some(Lane::Done));

    let rendered = render_document(&doc);
    assert_eq!(
        rendered, canonical,
        "parse -> render is byte-identical on canonical org docs"
    );

    let reparsed = parse_document(&rendered).expect("reparse succeeds");
    assert_eq!(reparsed, doc);
    assert_eq!(render_document(&reparsed), rendered);
}

#[test]
fn legacy_numeric_ids_upgrade_on_write_while_emacs_ids_stay_verbatim() {
    let legacy = "\
#+TITLE: project:legacy-board
#+STARTUP: overview

* TODO Legacy numeric card :legacy:
:PROPERTIES:
:ID: 1
:REV: 3
:OWNER_NOTE: keep verbatim
:END:
Body line preserved.

* STRT Emacs wiki note :wiki:
:PROPERTIES:
:ID: src-bush1951
:REX_NEEDS: [[id:rw-dep-1]] [[id:src-shannon1948][Shannon 1948]] plain-dep
:END:
** Subheading preserved
";

    let doc = parse_document(legacy).expect("legacy org parses cleanly");
    assert_eq!(doc.tasks.len(), 2);

    let upgraded_id = doc.tasks[0]
        .org_id
        .as_ref()
        .expect("legacy numeric id upgraded to rw-<hex>");
    assert!(upgraded_id.is_rex_minted());
    assert_eq!(doc.tasks[0].id, Some(CardNo(1)));
    assert_eq!(doc.tasks[0].revision, Some(CardRevision(3)));
    assert_eq!(
        doc.tasks[0].properties,
        vec![("OWNER_NOTE".to_owned(), "keep verbatim".to_owned())]
    );

    assert_eq!(
        doc.tasks[1].org_id.as_ref().map(OrgId::as_str),
        Some("src-bush1951")
    );
    assert_eq!(doc.tasks[1].id, None);
    assert_eq!(
        doc.tasks[1].needs,
        vec![
            OrgId::parse("rw-dep-1").unwrap(),
            OrgId::parse("src-shannon1948").unwrap(),
            OrgId::parse("plain-dep").unwrap(),
        ]
    );

    let rendered = render_document(&doc);
    assert!(rendered.contains(&format!(":ID: {upgraded_id}\n:REX_CARD: 1\n:REX_REV: 3\n")));
    assert!(
        rendered.contains(":ID: src-bush1951\n:REX_NEEDS: rw-dep-1 src-shannon1948 plain-dep\n")
    );
    assert!(!rendered.contains(":ID: 1\n"));
    assert!(!rendered.contains(":REV: 3\n"));
    assert!(rendered.contains(":OWNER_NOTE: keep verbatim\n"));
    assert!(rendered.contains("** Subheading preserved\n"));

    let reparsed = parse_document(&rendered).expect("upgraded doc reparses");
    assert_eq!(reparsed, doc);
    assert_eq!(render_document(&reparsed), rendered);
}

#[test]
fn board_snapshot_render_and_parse_round_trip_without_loss() {
    let board_id = BoardId::parse("project:rex-wave").unwrap();
    let card1_org_id = OrgId::parse("emacs-preserved-id-1").unwrap();
    let card2_org_id = OrgId::for_board_card(&board_id, CardNo(2));
    let card3_org_id = OrgId::for_board_card(&board_id, CardNo(3));

    let snapshot = BoardSnapshot {
        board: board_id.clone(),
        head: 12,
        lanes: vec![
            LaneCards {
                lane: Lane::Todo.lane_name(),
                cards: vec![Card {
                    no: CardNo(1),
                    revision: CardRevision(3),
                    org_id: Some(card1_org_id.clone()),
                    title: Title::parse("Handle UTF-8 café & colons: 1:2").unwrap(),
                    lane: Lane::Todo.lane_name(),
                    assignee: Some(Assignee::parse("@sam").unwrap()),
                    priority: Priority::Blocking,
                    due_ms: Some(1_800_000_000_000),
                    labels: vec!["utf8-café".into(), "space tag".into(), "colon:tag".into()],
                    links: vec![
                        Link::External(ExternalLink {
                            namespace: "rex-wave.message".into(),
                            key: "{\"room\": \"general\", \"seq\": 7}".into(),
                        }),
                        Link::External(ExternalLink::parse_uri("rex://wave/pr2").unwrap()),
                        Link::Org {
                            file: PathBuf::from("notes/design:v1.org"),
                            outline: vec!["Top".into(), "Sub/Leaf".into()],
                        },
                    ],
                    needs: vec![card2_org_id.clone()],
                }],
            },
            LaneCards {
                lane: Lane::Doing.lane_name(),
                cards: vec![Card {
                    no: CardNo(2),
                    revision: CardRevision(1),
                    org_id: None,
                    title: Title::parse("Normal task").unwrap(),
                    lane: Lane::Doing.lane_name(),
                    assignee: None,
                    priority: Priority::Normal,
                    due_ms: None,
                    labels: vec![],
                    links: vec![],
                    needs: vec![],
                }],
            },
            LaneCards {
                lane: LaneName::parse("triage").unwrap(),
                cards: vec![Card {
                    no: CardNo(3),
                    revision: CardRevision(2),
                    org_id: Some(card3_org_id.clone()),
                    title: Title::parse("Custom lane low priority").unwrap(),
                    lane: LaneName::parse("triage").unwrap(),
                    assignee: None,
                    priority: Priority::Low,
                    due_ms: None,
                    labels: vec!["custom".into()],
                    links: vec![],
                    needs: vec![],
                }],
            },
        ],
    };

    let org_text = render(&snapshot);
    assert!(org_text.contains(":ID: emacs-preserved-id-1\n:REX_CARD: 1\n:REX_REV: 3\n"));
    assert!(org_text.contains(&format!(":ID: {card2_org_id}\n:REX_CARD: 2\n:REX_REV: 1\n")));

    let parsed_cards = parse(&org_text).expect("snapshot org parses cleanly");
    let mut expected_cards: Vec<Card> = snapshot
        .lanes
        .iter()
        .flat_map(|l| l.cards.clone())
        .collect();
    expected_cards[1].org_id = Some(card2_org_id);
    assert_eq!(parsed_cards, expected_cards);

    let ast = parse_document(&org_text).unwrap();
    assert_eq!(
        render_document(&ast),
        org_text,
        "render -> parse -> render is byte-identical"
    );
}

#[test]
fn hand_written_org_without_ids_assigns_non_colliding_card_numbers() {
    let hand_written = "\
* TODO Unnumbered first
* STRT Explicit id one
:PROPERTIES:
:ID: 1
:REV: 5
:END:
";
    let cards = parse(hand_written).unwrap();
    assert_eq!(cards.len(), 2);
    assert_eq!(
        cards[0].no,
        CardNo(2),
        "synthetic id avoids colliding with explicit :ID: 1"
    );
    assert_eq!(cards[0].revision, CardRevision(1));
    assert_eq!(cards[1].no, CardNo(1));
    assert_eq!(cards[1].revision, CardRevision(5));
}

#[test]
fn malformed_headings_and_properties_report_accurate_line_errors() {
    assert!(matches!(
        parse_document("*"),
        Err(OrgError::Heading { line: 1, .. })
    ));
    assert!(matches!(
        parse_document("* TODO"),
        Err(OrgError::Heading { line: 1, .. })
    ));
    assert!(matches!(
        parse_document("* [#A] Missing keyword"),
        Err(OrgError::Heading { line: 1, .. })
    ));
    assert!(matches!(
        parse_document("* todo Lowercase keyword"),
        Err(OrgError::Heading { line: 1, .. })
    ));
    assert!(matches!(
        parse_document("* TODO [#Z] Bad cookie"),
        Err(OrgError::Heading { line: 1, .. })
    ));
    assert!(matches!(
        parse_document("* TODO :onlytags:"),
        Err(OrgError::Heading { line: 1, .. })
    ));

    let unclosed = "* TODO Task\n:PROPERTIES:\n:ID: 1\n* DONE Next\n";
    assert!(matches!(
        parse_document(unclosed),
        Err(OrgError::Property { line: 2, .. })
    ));

    let dup_id =
        "* TODO One\n:PROPERTIES:\n:ID: 7\n:END:\n\n* DONE Two\n:PROPERTIES:\n:ID: 7\n:END:\n";
    assert!(matches!(
        parse_document(dup_id),
        Err(OrgError::Property { line: 8, .. })
    ));

    let bad_rev = "* TODO One\n:PROPERTIES:\n:REV: nope\n:END:\n";
    assert!(matches!(
        parse_document(bad_rev),
        Err(OrgError::Property { line: 3, .. })
    ));

    let bad_link = "* TODO One\n:PROPERTIES:\n:LINKS: rex://\n:END:\n";
    assert!(matches!(
        parse_document(bad_link),
        Err(OrgError::Property { line: 3, .. })
    ));

    let dup_org_id = "* TODO One\n:PROPERTIES:\n:ID: src-dup\n:END:\n\n* DONE Two\n:PROPERTIES:\n:ID: src-dup\n:END:\n";
    assert!(matches!(
        parse_document(dup_org_id),
        Err(OrgError::Property { line: 8, .. })
    ));

    let bad_org_id = "* TODO One\n:PROPERTIES:\n:ID: bad:id\n:END:\n";
    assert!(matches!(
        parse_document(bad_org_id),
        Err(OrgError::Property { line: 3, .. })
    ));

    let bad_needs = "* TODO One\n:PROPERTIES:\n:REX_NEEDS: [[https://example.com]]\n:END:\n";
    assert!(matches!(
        parse_document(bad_needs),
        Err(OrgError::Property { line: 3, .. })
    ));
}
