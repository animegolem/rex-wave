use rex_wave_api::board::{CardPatch, CardRevision, CardUpsert, Priority, Title, project_board};
use rex_wave_api::notify::NotifyKind;
use rex_wave_api::presence::*;
use rex_wave_api::read::{
    ContextCategory, ContextCategoryKind, InboxBadge, ThreadContextUsage, Wait,
};
use rex_wave_api::room::{
    Body, Mentions, RoomEvent, RoomId, RoomLabel, RoomMessageSummary, RoomPost, RoomRef, RoomScope,
    RoomTasks, TaskClaim, TaskHeartbeat, TaskSpec, TaskStatus, TaskTransition, TaskUpdate,
};
use rex_wave_api::stage::{Osc7501BlockKind, Osc7501State, StageTarget, derive_osc7501_status};
use rex_wave_api::wave::*;
use rex_wave_api::*;
use std::collections::BTreeMap;
use std::path::PathBuf;

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
    let post = RoomPost::new(
        RoomRef::Channel {
            scope: RoomScope::Global,
            name: ChannelName::parse("general").unwrap(),
        },
        Body::parse("hi").unwrap(),
    );
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
        errored: None,
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

#[test]
fn every_api_error_variant_round_trips_through_json() {
    let every = [
        ApiError::Invalid(InvalidId {
            what: "seat".into(),
            reason: "length out of range".into(),
        }),
        ApiError::Denied {
            principal: "seat:sam".into(),
            action: "watch everything".into(),
        },
        ApiError::NotFound {
            what: "room #nope".into(),
        },
        ApiError::Conflict {
            detail: "worker never spawned".into(),
            data: None,
        },
        ApiError::Unavailable {
            family: Family::Wave,
        },
        ApiError::Timeout,
        ApiError::Transport("bb events page: connection refused".into()),
        ApiError::ReplyMismatch {
            expected: "room_post".into(),
            got: "ack".into(),
        },
        ApiError::Internal("journal locked".into()),
    ];
    for err in every {
        let json = serde_json::to_value(&err)
            .unwrap_or_else(|e| panic!("{err:?} must serialize, got {e}"));
        let back: ApiError = serde_json::from_value(json.clone())
            .unwrap_or_else(|e| panic!("{json} must deserialize, got {e}"));
        assert_eq!(back, err, "{json} round-trips");
    }

    assert_eq!(
        serde_json::to_value(ApiError::Transport("down".into())).unwrap(),
        serde_json::json!({ "error": "transport", "detail": "down" })
    );
    assert_eq!(
        serde_json::to_value(ApiError::Internal("boom".into())).unwrap(),
        serde_json::json!({ "error": "internal", "detail": "boom" })
    );
    assert_eq!(
        serde_json::to_value(ApiError::conflict("stale")).unwrap(),
        serde_json::json!({ "error": "conflict", "detail": "stale" })
    );
    let legacy_conflict: ApiError =
        serde_json::from_value(serde_json::json!({ "error": "conflict", "detail": "stale" }))
            .expect("legacy conflict without data deserializes");
    assert_eq!(legacy_conflict, ApiError::conflict("stale"));
}

#[test]
fn phase3_branded_types_conflict_data_and_legacy_room_events_round_trip() {
    let lbl = Label::parse("status=open").unwrap();
    assert_eq!(lbl.key(), "status");
    assert_eq!(lbl.value(), "open");
    assert!(Label::parse("tag.a-1_b=v/2:3").is_ok());
    assert!(Label::parse("no_equals").is_err());
    assert!(Label::parse("UPPER=val").is_err());
    assert!(Label::parse("key=has space").is_err());
    assert!(Label::parse("key=").is_err());
    assert!(Label::parse("=val").is_err());

    assert_eq!(LeaseEpoch::default().next().next(), LeaseEpoch(2));

    for part in [SystemPart::T3Bridge, SystemPart::PresenceMonitor] {
        let back: SystemPart = serde_json::from_value(serde_json::to_value(part).unwrap()).unwrap();
        assert_eq!(back, part);
    }

    let op = Principal::Operator {
        seat: SeatName::parse("golem").unwrap(),
    };
    let conflicts = [
        ConflictData::NewerMessage {
            expected: Seq(3),
            latest: Seq(4),
            newer_message: Box::new(RoomMessageSummary {
                seq: Seq(4),
                at: UnixMs(1000),
                by: op.clone(),
                body: Body::parse("landed first").unwrap(),
                re: Some(Seq(1)),
                labels: vec![lbl.clone()],
            }),
        },
        ConflictData::LeaseFenced {
            expected: LeaseEpoch(1),
            actual: LeaseEpoch(2),
            holder: Some(op.clone()),
        },
        ConflictData::UnmetNeeds {
            unmet: vec![Seq(2), Seq(5)],
        },
        ConflictData::RevisionConflict {
            expected: 3,
            actual: 4,
        },
        ConflictData::DuplicateLiveSession {
            seat: SeatName::parse("genga").unwrap(),
            active: IncarnationId::parse("inc-1").unwrap(),
        },
    ];
    for data in conflicts {
        let err = ApiError::conflict_with("structured conflict", data);
        let back: ApiError = serde_json::from_value(serde_json::to_value(&err).unwrap()).unwrap();
        assert_eq!(back, err);
    }

    let legacy_posted = serde_json::json!({
        "type": "posted",
        "body": "legacy message",
        "mentions": { "principals": [], "all": false },
        "reply_to": 7,
        "idem": "old-k1"
    });
    let ev: RoomEvent = serde_json::from_value(legacy_posted).expect("legacy Posted replays");
    match ev {
        RoomEvent::Posted {
            re,
            after,
            labels,
            task,
            idem,
            ..
        } => {
            assert_eq!(re, Some(Seq(7)));
            assert_eq!(after, None);
            assert!(labels.is_empty());
            assert_eq!(task, None);
            assert_eq!(idem, Some(IdemKey::parse("old-k1").unwrap()));
        }
        other => panic!("expected Posted, got {other:?}"),
    }

    let post_with_aliases: RoomPost = serde_json::from_value(serde_json::json!({
        "room": { "ref": "channel", "scope": { "scope": "global" }, "name": "general" },
        "body": "hello",
        "reply_to": 2,
        "id": "client-key-1",
        "after": 5,
        "labels": ["env=prod"],
        "task": {
            "title": "Ship Phase 3A",
            "org_id": "src-bush1951",
            "needs": [1]
        }
    }))
    .expect("aliases reply_to and id parse");
    assert_eq!(post_with_aliases.re, Some(Seq(2)));
    assert_eq!(post_with_aliases.after, Some(Seq(5)));
    assert_eq!(
        post_with_aliases.idem,
        Some(IdemKey::parse("client-key-1").unwrap())
    );
    assert_eq!(
        post_with_aliases.labels,
        vec![Label::parse("env=prod").unwrap()]
    );
    assert_eq!(
        post_with_aliases.task.as_ref().unwrap().org_id,
        Some(OrgId::parse("src-bush1951").unwrap())
    );

    assert_eq!(TaskStatus::Open.org_keyword(), "TODO");
    assert_eq!(TaskStatus::Claimed.org_keyword(), "STRT");
    assert_eq!(TaskStatus::Review.org_keyword(), "WAIT");
    assert_eq!(TaskStatus::Blocked.org_keyword(), "BLKD");
    assert_eq!(TaskStatus::Done.org_keyword(), "DONE");
    assert_eq!(TaskStatus::Cancelled.org_keyword(), "CNCL");
    assert!(TaskStatus::Done.is_terminal());
    assert!(TaskStatus::Cancelled.is_terminal());
    assert!(!TaskStatus::Claimed.is_terminal());

    let chan = RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("ops").unwrap(),
    };
    let cmds: Vec<WireCommand> = vec![
        RoomLabel {
            room: chan.clone(),
            seq: Seq(1),
            add: vec![lbl.clone()],
            remove: vec![],
            idem: None,
        }
        .into_wire(),
        TaskClaim {
            room: chan.clone(),
            task: Seq(1),
            ttl_ms: Some(60_000),
            expected_rev: Some(1),
            force: false,
            idem: None,
        }
        .into_wire(),
        TaskHeartbeat {
            room: chan.clone(),
            task: Seq(1),
            epoch: LeaseEpoch(1),
            ttl_ms: Some(60_000),
        }
        .into_wire(),
        TaskTransition {
            room: chan.clone(),
            task: Seq(1),
            status: TaskStatus::Done,
            epoch: Some(LeaseEpoch(1)),
            expected_rev: None,
            note: Some(Body::parse("verified").unwrap()),
            idem: None,
        }
        .into_wire(),
        TaskUpdate {
            room: chan.clone(),
            task: Seq(1),
            expected_rev: Some(2),
            title: Some(Title::parse("Updated").unwrap()),
            needs_add: vec![],
            needs_remove: vec![],
            priority: Some(Priority::High),
            due_ms: None,
            clear_due: false,
            idem: None,
        }
        .into_wire(),
        RoomTasks {
            room: chan,
            filter: None,
        }
        .into_wire(),
    ];
    for cmd in cmds {
        assert_eq!(cmd.meta().family, Family::Room);
        let wire: WireCommand =
            serde_json::from_value(serde_json::to_value(&cmd).unwrap()).unwrap();
        assert_eq!(wire.meta().name, cmd.meta().name);
    }

    let _ = Mentions::default();
    let _ = TaskSpec {
        title: Title::parse("Spec").unwrap(),
        org_id: None,
        needs: vec![],
        priority: None,
        due_ms: None,
    };
}

#[test]
fn presence_family_provenance_bindings_and_notify_kinds_round_trip() {
    let crest = SeatName::parse("crest").unwrap();
    let tally = SeatName::parse("tally").unwrap();
    let by_crest = Principal::Seat {
        seat: crest.clone(),
    };
    let prov_self = Provenance {
        asserted_by: by_crest.clone(),
        verified_how: VerificationKind::SelfWord,
        at: UnixMs(1_795_261_554_000),
    };
    let prov_witnessed = Provenance {
        asserted_by: Principal::Seat {
            seat: tally.clone(),
        },
        verified_how: VerificationKind::WitnessedBy {
            seat: tally.clone(),
        },
        at: UnixMs(1_795_261_555_000),
    };
    let prov_machine = Provenance {
        asserted_by: Principal::System {
            part: SystemPart::PresenceMonitor,
        },
        verified_how: VerificationKind::MachineChecked,
        at: UnixMs(1_795_261_556_000),
    };

    let record = SeatRecord {
        seat: crest.clone(),
        status: SeatStatus::Active,
        status_provenance: Some(prov_witnessed.clone()),
        aliases: vec![Attested {
            value: SeatName::parse("rex-owner").unwrap(),
            provenance: prov_witnessed.clone(),
        }],
        role: Some(Attested {
            value: "owner of rex-wave".into(),
            provenance: prov_self.clone(),
        }),
        projects: vec![Attested {
            value: "rex-wave".into(),
            provenance: prov_machine,
        }],
        home_room: None,
        retired_at: None,
        mail_horizon_at: Some(UnixMs(1_795_261_555_000)),
        work_class: Some(WorkClass::ClassA),
        avatar: Some("avatars/crest.png".into()),
        home_dir: Some("/Users/golem/agent-folder/resident_memories/crest".into()),
        start_cmd: Some("claude --seat crest".into()),
        resume_cmd: Some("claude --resume".into()),
        rex_target: Some("block:crest-main".into()),
        claimed_by: by_crest,
        claimed_at: UnixMs(1_795_261_554_000),
    };

    let endpoints = [
        TransportEndpoint::UdsSession {
            conn_id: 7,
            client: "rex-wave-cli".into(),
        },
        TransportEndpoint::ClaudeSession {
            pid: 8195,
            proc_start: "Sat Nov 21 05:30:00 2026".into(),
            started_at_ms: 1_795_260_600_000,
            session_id: "sess-tally-1".into(),
            messaging_socket: PathBuf::from("/tmp/cc-socks/8195.sock"),
            kind: "bg".into(),
            name_source: Some("user".into()),
            display_name: Some("Tally-5.2".into()),
        },
        TransportEndpoint::BbThread {
            thread_id: BbThreadId::parse("thr_123").unwrap(),
        },
        TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("t3:w1:author:a1").unwrap(),
        },
        TransportEndpoint::SelfWatcher {
            harness: "mail-wait.py".into(),
            thread_id: None,
            pid: Some(4242),
            proc_start: Some("Sat Nov 21 05:00:00 2026".into()),
        },
        TransportEndpoint::OperatorToast {
            command: "osascript".into(),
        },
    ];

    assert!(TransportEndpoint::is_allowed_claude_kind("interactive"));
    assert!(TransportEndpoint::is_allowed_claude_kind("bg"));
    assert!(!TransportEndpoint::is_allowed_claude_kind("subagent"));
    assert!(TransportEndpoint::is_fork_display_name("⑂ Tally-5.2"));
    assert!(!TransportEndpoint::is_fork_display_name("Tally-5.2"));
    assert!(TransportEndpoint::is_derived_name_source(Some("derived")));
    assert!(!TransportEndpoint::is_derived_name_source(Some("user")));

    for (idx, ep) in endpoints.into_iter().enumerate() {
        let binding = SeatBinding {
            seat: crest.clone(),
            incarnation_id: IncarnationId::parse(format!("inc-{idx}")).unwrap(),
            epoch: idx as u64 + 1,
            lane: None,
            endpoint: ep,
            liveness: LivenessMode::ProcessAndLease,
            bound_at: UnixMs(1_795_261_554_000),
            expires_at: Some(UnixMs(1_795_262_154_000)),
            status: BindingStatus::Live,
        };
        let ev = Event::Presence(PresenceEvent::Bound {
            binding: binding.clone(),
            superseded: vec![],
        });
        assert!(ev.fits(&StreamId::Presence));
        assert!(!ev.fits(&StreamId::Mail));
        let back: SeatBinding =
            serde_json::from_value(serde_json::to_value(&binding).unwrap()).unwrap();
        assert_eq!(back, binding);
    }

    let claim_ev = Event::Presence(PresenceEvent::SeatClaimed {
        record: record.clone(),
    });
    assert!(claim_ev.fits(&StreamId::Presence));
    let back_record: SeatRecord =
        serde_json::from_value(serde_json::to_value(&record).unwrap()).unwrap();
    assert_eq!(back_record, record);

    let list_cmd = PresenceList::default().into_wire();
    assert_eq!(list_cmd.meta().family, Family::Presence);
    assert!(matches!(
        list_cmd.route(),
        Routed::Presence(PresenceCommand::PresenceList(_))
    ));

    let notify_kinds = [
        NotifyKind::MailUnrung {
            seat: crest.clone(),
            unread_count: 3,
            oldest_at: UnixMs(1_795_261_000_000),
        },
        NotifyKind::TransportDown {
            transport: "claude_session".into(),
            detail: "/tmp/cc-socks missing".into(),
        },
        NotifyKind::OrgConflict {
            path: "boards/main.org".into(),
            org_id: OrgId::parse("src-bush1951").unwrap(),
            detail: "expected rev 2, actual 3".into(),
        },
    ];
    for kind in notify_kinds {
        let v = serde_json::to_value(&kind).unwrap();
        let _: NotifyKind = serde_json::from_value(v).unwrap();
    }

    let minimal_unrung: NotifyKind = serde_json::from_value(serde_json::json!({
        "kind": "mail_unrung",
        "seat": "crest"
    }))
    .expect("default fields on MailUnrung deserialize");
    assert!(matches!(
        minimal_unrung,
        NotifyKind::MailUnrung {
            unread_count: 0,
            oldest_at: UnixMs(0),
            ..
        }
    ));
}
