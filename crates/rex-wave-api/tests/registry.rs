use rex_wave_api::board::{CardPatch, CardRevision, CardUpsert, Priority, Title, project_board};
use rex_wave_api::read::{
    ContextCategory, ContextCategoryKind, InboxBadge, ThreadContextUsage, Wait,
};
use rex_wave_api::room::{Body, RoomId, RoomPost, RoomRef, RoomScope};
use rex_wave_api::stage::{Osc7501BlockKind, Osc7501State, StageTarget, derive_osc7501_status};
use rex_wave_api::wave::*;
use rex_wave_api::*;
use std::collections::BTreeMap;

#[test]
fn tool_names_are_unique_and_match_serde_tags() {
    let mut names: Vec<_> = TOOLS.iter().map(|t| t.name).collect();
    names.sort();
    let before = names.len();
    names.dedup();
    assert_eq!(before, names.len(), "duplicate tool name in commands! list");
}

#[test]
fn tool_catalog_emits_object_schemas_and_hides_system_commands() {
    let catalog = tool_catalog();
    let arr = catalog.as_array().expect("catalog is a JSON array");
    let expected = TOOLS
        .iter()
        .filter(|t| t.audience != Audience::System)
        .count();
    assert_eq!(arr.len(), expected);
    for entry in arr {
        let name = entry["name"].as_str().unwrap();
        assert_ne!(
            name, "wave_observe",
            "system commands stay out of external catalogs"
        );
        assert_eq!(
            entry["inputSchema"]["type"], "object",
            "tool {name} must have an object schema"
        );
    }
}

#[test]
fn mcp_style_call_parses_into_typed_command() {
    let json = serde_json::json!({
        "cmd": "room_post",
        "args": {
            "room": { "ref": "channel", "scope": { "scope": "global" }, "name": "general" },
            "body": "@verifier check #4"
        }
    });
    let cmd: WireCommand = serde_json::from_value(json).expect("parses");
    assert_eq!(cmd.meta().name, "room_post");
    assert!(matches!(
        cmd.route(),
        Routed::Room(RoomCommand::RoomPost(_))
    ));
}

#[test]
fn typed_and_wire_paths_agree_including_standalone_kanban_commands() {
    let post = RoomPost {
        room: RoomRef::Channel {
            scope: RoomScope::Global,
            name: ChannelName::parse("general").unwrap(),
        },
        body: Body::parse("hi").unwrap(),
        reply_to: None,
        idem: None,
    };
    let v = serde_json::to_value(post.into_wire()).unwrap();
    assert_eq!(v["cmd"], "room_post");
    assert_eq!(<RoomPost as Command>::FAMILY, Family::Room);

    let card = CardUpsert {
        board: project_board(&BbProjectId::parse("p_1").unwrap()),
        card: None,
        expected: Some(CardRevision(3)),
        patch: CardPatch {
            title: Some(Title::parse("Ship Gate 1").unwrap()),
            priority: Some(Priority::High),
            ..Default::default()
        },
        idem: None,
    };
    let wire = card.into_wire();
    assert_eq!(wire.meta().name, "card_upsert");
    assert!(matches!(
        wire.route(),
        Routed::Board(BoardCommand::CardUpsert(_))
    ));
}

#[test]
fn boundary_rejects_bad_ids() {
    assert!(ChannelName::parse("General!").is_err());
    assert!(serde_json::from_str::<WaveSlug>("\"-bad\"").is_err());
    assert!(serde_json::from_str::<Sha256Hex>("\"abc\"").is_err());
}

#[test]
fn steer_modes_interaction_resolutions_and_wakeup_policy_round_trip() {
    for (mode, expected) in [
        (SteerDeliveryMode::Auto, "\"auto\""),
        (SteerDeliveryMode::Start, "\"start\""),
        (SteerDeliveryMode::Steer, "\"steer\""),
        (SteerDeliveryMode::SteerIfActive, "\"steer-if-active\""),
        (SteerDeliveryMode::QueueIfActive, "\"queue-if-active\""),
    ] {
        assert_eq!(serde_json::to_string(&mode).unwrap(), expected);
        assert_eq!(
            serde_json::from_str::<SteerDeliveryMode>(expected).unwrap(),
            mode
        );
    }

    let resolutions = vec![
        InteractionResolution::AllowOnce {
            granted_permissions: Some(GrantedPermissions {
                file_system: Some(FileSystemGrant {
                    read: vec!["/repo".into()],
                    write: vec![],
                }),
                network: Some(NetworkGrant {
                    enabled: Some(false),
                }),
            }),
        },
        InteractionResolution::AllowForSession {
            granted_permissions: None,
        },
        InteractionResolution::Deny,
        InteractionResolution::UserAnswer {
            answers: BTreeMap::from([(
                "q1".into(),
                UserAnswer {
                    selected: vec!["yes".into()],
                    free_text: None,
                },
            )]),
        },
        InteractionResolution::RequestAnswer {
            value: serde_json::json!({"ok": true}),
        },
    ];
    for r in resolutions {
        let back: InteractionResolution =
            serde_json::from_value(serde_json::to_value(&r).unwrap()).unwrap();
        assert_eq!(back, r);
    }

    let r_a = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("a").unwrap(),
    };
    let r_b = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("b").unwrap(),
    };
    let t = BbThreadId::parse("th_1").unwrap();
    assert_eq!(
        plan_mention_wakeup(
            &WorkerPhase::Live {
                thread: t.clone(),
                activity: Activity::Parked {
                    room: Some(r_a.clone()),
                    since: Seq(4)
                }
            },
            &r_a
        ),
        WakePlan::Covered,
    );
    assert_eq!(
        plan_mention_wakeup(
            &WorkerPhase::Live {
                thread: t.clone(),
                activity: Activity::Parked {
                    room: Some(r_a),
                    since: Seq(4)
                }
            },
            &r_b
        ),
        WakePlan::Doorbell(SteerDeliveryMode::SteerIfActive),
    );
    assert_eq!(
        plan_mention_wakeup(
            &WorkerPhase::Live {
                thread: t,
                activity: Activity::Idle
            },
            &r_b
        ),
        WakePlan::Doorbell(SteerDeliveryMode::Auto),
    );
}

#[test]
fn osc7501_and_context_usage_and_wait_constructor_behave() {
    let t = BbThreadId::parse("th_1").unwrap();
    let working = WorkerPhase::Live {
        thread: t.clone(),
        activity: Activity::Working,
    };
    let retired = WorkerPhase::Retired {
        thread: Some(t.clone()),
    };
    let blocked = WorkerPhase::Live {
        thread: t,
        activity: Activity::Blocked {
            interaction: PendingInteraction {
                id: BbInteractionId::parse("ix_1").unwrap(),
                kind: InteractionKind::Question { questions: vec![] },
                summary: "pick one".into(),
            },
        },
    };
    let s = derive_osc7501_status(&InboxBadge::default(), [&working, &retired]);
    assert_eq!(s.state, Osc7501State::Working);
    assert_eq!(s.progress_pct, Some(50));

    let s = derive_osc7501_status(&InboxBadge::default(), [&working, &blocked]);
    assert_eq!(s.state, Osc7501State::Blocked);
    assert_eq!(s.block_kind, Some(Osc7501BlockKind::Question));

    let usage = ThreadContextUsage {
        used_tokens: 42_000,
        model_context_window: 200_000,
        estimated: false,
        auto_compact_at_tokens: Some(160_000),
        cost_usd: Some(0.18),
        categories: vec![ContextCategory {
            id: "msgs".into(),
            label: "Messages".into(),
            kind: ContextCategoryKind::Used,
            tokens: 42_000,
        }],
    };
    let back: ThreadContextUsage =
        serde_json::from_value(serde_json::to_value(&usage).unwrap()).unwrap();
    assert_eq!(back, usage);

    let room = RoomId::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("general").unwrap(),
    };
    let w = Wait::room(room.clone(), Seq(7), true);
    assert_eq!(w.interest, Interest::Mentions);
    assert!(w.after.from_head());
    assert_eq!(w.after.at(&StreamId::Room { room }), Seq(7));

    assert_eq!(FocusGeneration::default().next().next(), FocusGeneration(2));
    let stage = StageTarget::Inbox;
    let frame = Frame::StageFocused {
        generation: FocusGeneration(1),
        target: stage.clone(),
    };
    assert!(serde_json::to_value(frame).is_ok());
}
