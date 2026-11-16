use rex_kanban::*;

#[test]
fn validated_strings_and_revisions_behave_at_boundary() {
    assert!(BoardId::parse("project:p_123").is_ok());
    assert!(BoardId::parse("").is_err());
    assert!(LaneName::parse("doing").is_ok());
    assert!(LaneName::parse("Bad Lane").is_err());
    assert!(Title::parse("One line").is_ok());
    assert!(Title::parse("two\nlines").is_err());
    assert_eq!(CardRevision::default().next().next(), CardRevision(2));

    let upsert = CardUpsert {
        board: BoardId::parse("project:p_1").unwrap(),
        card: Some(CardNo(4)),
        expected: Some(CardRevision(2)),
        patch: CardPatch {
            lane: Some(LaneName::parse("review").unwrap()),
            links_add: vec![Link::External(ExternalLink { namespace: "rex-wave.worker".into(), key: "auth/verifier".into() })],
            ..Default::default()
        },
        idem: Some(IdemKey::parse("idem-1").unwrap()),
    };
    let round: CardUpsert = serde_json::from_value(serde_json::to_value(&upsert).unwrap()).unwrap();
    assert_eq!(round.expected, Some(CardRevision(2)));
    assert_eq!(round.patch.links_add, upsert.patch.links_add);
}
