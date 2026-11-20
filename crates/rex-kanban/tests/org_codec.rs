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
:ID: 1
:REV: 1
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
:ID: 2
:REV: 4
:ASSIGNEE: @auth-v2/author-jwt
:DUE_MS: 1763280000000
:LINKS: rex://room/general#42 letter://deadbeefcafe0123 wave:auth-v2/verifier https://example.com/a%2Fb
:CATEGORY: security
:END:
Ensure refresh tokens rotate atomically.

* STRT Implement CAS checks :kanban:
:PROPERTIES:
:ID: 3
:REV: 2
:ASSIGNEE: @sam
:END:

* WAIT [#C] Cross-model Sam review :review:
:PROPERTIES:
:ID: 4
:REV: 1
:END:
Waiting on BB review thread.

* BLKD [#A] Provision staging credentials :infra:
:PROPERTIES:
:ID: 5
:REV: 3
:PRIORITY: blocking
:LINKS: org+heading:docs/ops.org::Infra/Credentials\\x2FKeys
:TICKET: OPS-99
:END:

* DONE Ship Gate 1 foundation :milestone:
:PROPERTIES:
:ID: 6
:REV: 5
:END:
";

    let doc: OrgDocument = parse_document(canonical).expect("valid canonical org");
    assert_eq!(doc.title.as_deref(), Some("project:p_123"));
    assert_eq!(doc.preamble, vec!["#+STARTUP: overview"]);
    assert_eq!(doc.tasks.len(), 6);

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
fn board_snapshot_render_and_parse_round_trip_without_loss() {
    let snapshot = BoardSnapshot {
        board: BoardId::parse("project:rex-wave").unwrap(),
        head: 12,
        lanes: vec![
            LaneCards {
                lane: Lane::Todo.lane_name(),
                cards: vec![Card {
                    no: CardNo(1),
                    revision: CardRevision(3),
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
                }],
            },
            LaneCards {
                lane: Lane::Doing.lane_name(),
                cards: vec![Card {
                    no: CardNo(2),
                    revision: CardRevision(1),
                    title: Title::parse("Normal task").unwrap(),
                    lane: Lane::Doing.lane_name(),
                    assignee: None,
                    priority: Priority::Normal,
                    due_ms: None,
                    labels: vec![],
                    links: vec![],
                }],
            },
            LaneCards {
                lane: LaneName::parse("triage").unwrap(),
                cards: vec![Card {
                    no: CardNo(3),
                    revision: CardRevision(2),
                    title: Title::parse("Custom lane low priority").unwrap(),
                    lane: LaneName::parse("triage").unwrap(),
                    assignee: None,
                    priority: Priority::Low,
                    due_ms: None,
                    labels: vec!["custom".into()],
                    links: vec![],
                }],
            },
        ],
    };

    let org_text = render(&snapshot);
    let parsed_cards = parse(&org_text).expect("snapshot org parses cleanly");
    let expected_cards: Vec<Card> = snapshot
        .lanes
        .iter()
        .flat_map(|l| l.cards.clone())
        .collect();
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
}
