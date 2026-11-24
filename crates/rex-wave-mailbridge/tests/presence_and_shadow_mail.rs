use rex_wave_api::mail::{LetterPriority, MailPromote};
use rex_wave_api::presence::*;
use rex_wave_api::room::{Body, RoomPost, RoomRef, RoomScope};
use rex_wave_api::*;
use rex_wave_mailbridge::{
    AliasMap, ClaudeSessionFile, DeferReason, DeliveryStatus, GATE1_CASE_IDS, GATE1_CASE_TESTS,
    IncarnationAffinity, LetterCandidate, MailBridge, Presence, PresenceState, Protection,
    ReadOnlyInterchange, RetryEntry, RetryState, RingDecision, RingDeliveryChannel, RingPolicy,
    RingReferent, SandboxRoot, ShadowAction, TimeBasis, current_process_start, decide_mail_ring,
    decide_mail_rings_for_seat, extract_letter_timestamp, format_delivery_status,
    house_profile_path_from, is_process_alive, letter_paths_match, load_claude_session_file_from,
    load_house_profile_from, normalize_letter_path, parse_acks_jsonl, parse_claude_session_json,
    project, query_process_start, run_shadow_pass, score_shadow_against_acks, score_shadow_jsonl,
    seed_records_from_interchange_with_options, seed_records_from_interchange_with_profile,
    select_binding_for_delivery, sender_delivery_notice,
};
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    DecideCtx, Decider, EntryMeta, Hub, LocalClient, NoAgentFeed, ReadSide, Registry, Router,
    SidebarProjection, Unavailable,
};
use rex_wave_store::Journal;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

static DIR_COUNTER: AtomicU64 = AtomicU64::new(1);

struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    fn new(label: &str) -> Self {
        let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("rex-wave-pr16-{label}-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn seat(raw: &str) -> SeatName {
    SeatName::parse(raw).unwrap()
}

/// A non-default house profile: a named operator surface, a seat witness,
/// one permanently excluded pane, and canonical alias folds.
fn test_house() -> HouseProfile {
    let fold =
        |canon: &str, aliases: &[&str]| (seat(canon), aliases.iter().map(|a| seat(a)).collect());
    HouseProfile {
        operator_seat: seat("operator"),
        witness_seat: seat("carol"),
        excluded_herdr_targets: vec!["w9:p1S".to_owned()],
        aliases: [
            fold("carol", &["carol_old", "carol-legacy"]),
            fold("dave", &["dave_old", "dave-opus-5"]),
            fold("heidi", &["heidi-old", "heidi-typo", "maya"]),
            fold("ivan-supervisor", &["ivan", "ivan-sup"]),
            fold("lucas", &["lucas-rev"]),
            fold("niaj", &[]),
        ]
        .into_iter()
        .collect(),
        fixture_id_aliases: Default::default(),
    }
}

fn sha256_of(bytes: &[u8]) -> Sha256Hex {
    let mut h = Sha256::new();
    h.update(bytes);
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    Sha256Hex::parse(hex).unwrap()
}

fn apply_presence_with_probe<F: Fn(u32, &str) -> bool>(
    state: &mut PresenceState,
    seq: &mut u64,
    by: Principal,
    at: UnixMs,
    cmd: PresenceCommand,
    probe: F,
) -> Result<WireReply, ApiError> {
    let ctx = DecideCtx { by: by.clone(), at };
    let events = Presence::decide_with_probe(state, &cmd, &ctx, probe)?;
    let mut committed = Vec::new();
    for ev in events {
        *seq += 1;
        let meta = EntryMeta {
            seq: Seq(*seq),
            at,
            by: by.clone(),
        };
        Presence::evolve(state, &ev, &meta);
        committed.push(Envelope {
            stream: StreamId::Presence,
            seq: Seq(*seq),
            at,
            by: by.clone(),
            event: Event::Presence(ev),
        });
    }
    Ok(Presence::reply(state, cmd, &committed))
}

fn claude_endpoint(
    pid: u32,
    proc_start: &str,
    session_id: &str,
    kind: &str,
    name_source: Option<&str>,
    display_name: Option<&str>,
) -> TransportEndpoint {
    TransportEndpoint::ClaudeSession {
        pid,
        proc_start: proc_start.to_owned(),
        started_at_ms: 1_700_000_000_000,
        session_id: session_id.to_owned(),
        messaging_socket: PathBuf::from(format!("/tmp/cc-socks/{pid}.sock")),
        kind: kind.to_owned(),
        name_source: name_source.map(str::to_owned),
        display_name: display_name.map(str::to_owned),
    }
}

fn test_aliases() -> AliasMap {
    AliasMap::from_pairs([
        ("desk-v1", "carol"),
        ("desk_old", "carol"),
        ("cc-desk", "carol"),
        ("lucas-rev", "lucas"),
    ])
}

fn opt_in_interchange_dir() -> Option<PathBuf> {
    std::env::var_os("REX_WAVE_LIVE_INTERCHANGE_TEST_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn sample_candidate(rel_path: &str, seat: &str, body: &[u8], seen_at: UnixMs) -> LetterCandidate {
    let sha = sha256_of(body);
    let sha16 = sha.as_str()[..16].to_owned();
    LetterCandidate {
        rel_path: rel_path.to_owned(),
        sha256: sha,
        sha16,
        from: Some("carol".into()),
        to: SeatName::parse(seat).unwrap(),
        subject: Some("Test letter".into()),
        letter_id: None,
        in_reply_to: None,
        receipt_only: false,
        seen_at,
        time_basis: TimeBasis::LetterIdMs,
        date_only: false,
    }
}

fn empty_claim(seat: &str) -> PresenceClaim {
    PresenceClaim {
        seat: SeatName::parse(seat).unwrap(),
        status: None,
        role: None,
        projects: Vec::new(),
        aliases: Vec::new(),
        home_room: None,
        mail_horizon_at: None,
        work_class: None,
        avatar: None,
        home_dir: None,
        start_cmd: None,
        resume_cmd: None,
        rex_target: None,
        verified_how: None,
        replace: None,
        idem: None,
    }
}

#[tokio::test]
async fn presence_decider_round_trips_through_registry_with_provenance_and_idempotency() {
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(128);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());

    let router = Arc::new(Router {
        room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
        wave: Arc::new(Unavailable(Family::Wave)),
        board: Arc::new(Unavailable(Family::Board)),
        inbox: Arc::new(Unavailable(Family::Inbox)),
        mail: Arc::new(Unavailable(Family::Mail)),
        presence: Registry::<Presence>::new(journal.clone(), hub.clone()),
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });

    let owner = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        },
    );
    let eve = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: SeatName::parse("eve").unwrap(),
        },
    );
    let carol = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: SeatName::parse("carol").unwrap(),
        },
    );

    // Claim eve with Operator witness provenance on projects, then update role as eve itself.
    let claimed = owner
        .call(PresenceClaim {
            status: Some(SeatStatus::Active),
            projects: vec!["rex-wave".into()],
            aliases: vec![SeatName::parse("eve-lead").unwrap()],
            verified_how: Some(VerificationKind::WitnessedBy {
                seat: SeatName::parse("carol").unwrap(),
            }),
            idem: Some(IdemKey::parse("claim-eve-1").unwrap()),
            ..empty_claim("eve")
        })
        .await
        .unwrap();
    assert_eq!(claimed.status, SeatStatus::Active);
    assert_eq!(claimed.projects.len(), 1);
    assert_eq!(claimed.projects[0].value, "rex-wave");
    assert_eq!(
        claimed.projects[0].provenance.verified_how,
        VerificationKind::WitnessedBy {
            seat: SeatName::parse("carol").unwrap()
        }
    );

    // Idempotent retry returns the exact same SeatRecord without appending again.
    let retried = owner
        .call(PresenceClaim {
            status: Some(SeatStatus::Active),
            projects: vec!["rex-wave".into()],
            aliases: vec![SeatName::parse("eve-lead").unwrap()],
            idem: Some(IdemKey::parse("claim-eve-1").unwrap()),
            ..empty_claim("eve")
        })
        .await
        .unwrap();
    assert_eq!(retried, claimed);

    // Eve updates its own role; existing project provenance stays intact.
    let updated = eve
        .call(PresenceClaim {
            role: Some("rex-wave-owner".into()),
            ..empty_claim("eve")
        })
        .await
        .unwrap();
    assert_eq!(
        updated.role.as_ref().map(|r| r.value.as_str()),
        Some("rex-wave-owner")
    );
    assert_eq!(
        updated.role.as_ref().unwrap().provenance.verified_how,
        VerificationKind::SelfWord
    );
    assert_eq!(
        updated.projects[0].provenance.verified_how,
        VerificationKind::WitnessedBy {
            seat: SeatName::parse("carol").unwrap()
        },
        "untouched attested field retains its original provenance"
    );

    // Carol may not claim eve's seat without explicit replacement.
    let hijack = carol
        .call(PresenceClaim {
            role: Some("usurper".into()),
            ..empty_claim("eve")
        })
        .await
        .unwrap_err();
    assert!(
        matches!(hijack, ApiError::Conflict { .. }),
        "got {hijack:?}"
    );

    // Resolve by project works even while eve has zero live bindings.
    let resolved = carol
        .call(PresenceResolve {
            query: "rex-wave".into(),
            lane: None,
        })
        .await
        .unwrap();
    assert_eq!(resolved.record.seat.as_str(), "eve");
    assert!(resolved.live_bindings.is_empty());

    // Bind, heartbeat, retire -> auto-unbinds live bindings and rejects new binds.
    let bound = eve
        .call(PresenceBind {
            seat: SeatName::parse("eve").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: TransportEndpoint::SelfWatcher {
                harness: "codex".into(),
                thread_id: Some("thr-1".into()),
                pid: None,
                proc_start: None,
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(30_000),
            takeover: false,
            allow_fork: false,
            mail_horizon_at: None,
            idem: Some(IdemKey::parse("bind-eve-1").unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(bound.status, BindingStatus::Live);
    assert_eq!(bound.epoch, 1);

    let hb = eve
        .call(PresenceHeartbeat {
            seat: SeatName::parse("eve").unwrap(),
            incarnation_id: bound.incarnation_id.clone(),
            ttl_ms: Some(60_000),
        })
        .await
        .unwrap();
    assert!(hb.expires_at > bound.expires_at);

    let retired = owner
        .call(PresenceSetStatus {
            seat: SeatName::parse("eve").unwrap(),
            status: SeatStatus::Retired,
            verified_how: None,
            reason: Some("project handed off".into()),
        })
        .await
        .unwrap();
    assert_eq!(retired.status, SeatStatus::Retired);
    assert!(retired.retired_at.is_some());

    let list_default = carol.call(PresenceList::default()).await.unwrap();
    assert!(
        list_default.seats.is_empty(),
        "retired seats are hidden from PresenceList unless include_retired is true"
    );
    let list_all = carol
        .call(PresenceList {
            include_retired: true,
            ..PresenceList::default()
        })
        .await
        .unwrap();
    assert_eq!(list_all.seats.len(), 1);
    assert_eq!(list_all.seats[0].bindings[0].status, BindingStatus::Unbound);

    let bind_retired = eve
        .call(PresenceBind {
            seat: SeatName::parse("eve").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: TransportEndpoint::UdsSession {
                conn_id: 1,
                client: "cli".into(),
            },
            liveness: LivenessMode::SocketLifetime,
            ttl_ms: None,
            takeover: false,
            allow_fork: false,
            mail_horizon_at: None,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(bind_retired, ApiError::Conflict { .. }));
}

#[test]
fn claude_session_hook_parser_and_real_ps_process_liveness_enforce_carol_rules() {
    let dir = TempDirGuard::new("claude-sessions");
    let my_pid = std::process::id();
    let my_start = current_process_start().expect("ps reports current process UTC lstart");
    assert!(is_process_alive(my_pid, &my_start));
    assert!(!is_process_alive(my_pid, "Thu Jan  1 00:00:00 1970"));
    assert!(!is_process_alive(999_999_999, &my_start));

    // Background desk seat (`kind: "bg"`, like Carol's 8195.json) MUST be allowed.
    let bg_json = serde_json::json!({
        "pid": my_pid,
        "procStart": my_start,
        "startedAt": 1_794_550_979_000u64,
        "sessionId": "59e9053e-bbdd-4181-bf4f-3f7bf864d276",
        "messagingSocketPath": format!("/tmp/cc-socks/{my_pid}.sock"),
        "kind": "bg",
        "name": "Carol-5.2",
        "nameSource": "user",
        "formerNames": ["Carol-legacy"]
    });
    fs::write(
        dir.path().join(format!("{my_pid}.json")),
        serde_json::to_vec(&bg_json).unwrap(),
    )
    .unwrap();
    let loaded = load_claude_session_file_from(dir.path(), my_pid).unwrap();
    assert_eq!(loaded.kind, "bg");
    assert_eq!(loaded.former_names, vec!["Carol-legacy"]);
    assert!(!loaded.is_derived_name());
    let bind_cmd = loaded
        .into_bind_command(SeatName::parse("carol").unwrap(), None, None, false, false)
        .expect("kind='bg' is allowed to bind");
    assert!(matches!(
        bind_cmd.endpoint,
        TransportEndpoint::ClaudeSession { .. }
    ));

    // Disallowed kind (`subagent`) is rejected.
    let subagent = parse_claude_session_json(
        &serde_json::json!({
            "pid": my_pid,
            "procStart": my_start,
            "sessionId": "sub-1",
            "messagingSocketPath": "/tmp/cc-socks/sub.sock",
            "kind": "subagent",
            "name": "carol-8c",
            "nameSource": "derived"
        })
        .to_string(),
    )
    .unwrap();
    assert!(matches!(
        subagent.into_endpoint(false),
        Err(ApiError::Invalid(_))
    ));
    assert!(
        subagent.is_derived_name(),
        "nameSource='derived' is flagged as derived and never auto-derives a SeatName"
    );

    // Interactive session with `nameSource: "derived"` is flagged as derived,
    // though it may still bind when an explicit `--seat` is provided by its hook.
    let derived_interactive = parse_claude_session_json(
        &serde_json::json!({
            "pid": my_pid,
            "procStart": my_start,
            "sessionId": "sess-derived",
            "messagingSocketPath": "/tmp/cc-socks/85820.sock",
            "kind": "interactive",
            "name": "users-f0",
            "nameSource": "derived"
        })
        .to_string(),
    )
    .unwrap();
    assert!(derived_interactive.is_derived_name());
    assert!(derived_interactive.into_endpoint(false).is_ok());

    // Fork mark (`⑂`) is rejected unless `allow_fork == true`.
    let fork_session: ClaudeSessionFile = parse_claude_session_json(
        &serde_json::json!({
            "pid": my_pid,
            "procStart": my_start,
            "sessionId": "fork-1",
            "messagingSocketPath": "/tmp/cc-socks/10576.sock",
            "kind": "interactive",
            "name": "Carol-legacy ⑂",
            "nameSource": "user"
        })
        .to_string(),
    )
    .unwrap();
    assert!(matches!(
        fork_session.into_endpoint(false),
        Err(ApiError::Invalid(_))
    ));
    assert!(fork_session.into_endpoint(true).is_ok());
    assert!(!fork_session.is_derived_name());

    // Real process identity test: /clear refresh vs --resume dead-pid auto-supersede vs live duplicate.
    let mut state = PresenceState::default();
    let mut seq = 0u64;
    let carol_p = Principal::Seat {
        seat: SeatName::parse("carol").unwrap(),
    };

    // 1) Spawn a short-lived child process, bind it, then let it exit (`--resume` test).
    let mut child = Command::new("/bin/sleep").arg("60").spawn().unwrap();
    let child_pid = child.id();
    let child_start = query_process_start(child_pid).expect("query spawned child lstart");
    assert!(is_process_alive(child_pid, &child_start));

    let first_reply = apply_presence_with_probe(
        &mut state,
        &mut seq,
        carol_p.clone(),
        UnixMs(1_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: SeatName::parse("carol").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: claude_endpoint(
                child_pid,
                &child_start,
                "sess-old-1",
                "bg",
                Some("user"),
                Some("Carol-5.2"),
            ),
            liveness: LivenessMode::ProcessAndLease,
            ttl_ms: Some(600_000),
            takeover: false,
            allow_fork: false,
            mail_horizon_at: None,
            idem: None,
        }),
        is_process_alive,
    )
    .unwrap();
    let WireReply::PresenceBind(b1) = first_reply else {
        panic!("expected PresenceBind reply");
    };
    assert_eq!(b1.epoch, 1);

    // 2) Same live `(pid, proc_start)` refreshes freely across `/clear` (new `sessionId`)!
    let clear_reply = apply_presence_with_probe(
        &mut state,
        &mut seq,
        carol_p.clone(),
        UnixMs(2_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: SeatName::parse("carol").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: claude_endpoint(
                child_pid,
                &child_start,
                "sess-after-clear-2",
                "bg",
                Some("user"),
                Some("Carol-5.2"),
            ),
            liveness: LivenessMode::ProcessAndLease,
            ttl_ms: Some(600_000),
            takeover: false,
            allow_fork: false,
            mail_horizon_at: None,
            idem: None,
        }),
        is_process_alive,
    )
    .unwrap();
    let WireReply::PresenceBind(b_clear) = clear_reply else {
        panic!("expected PresenceBind reply");
    };
    assert_eq!(b_clear.incarnation_id, b1.incarnation_id);
    assert_eq!(b_clear.epoch, 1);
    assert!(matches!(
        &b_clear.endpoint,
        TransportEndpoint::ClaudeSession { session_id, .. } if session_id == "sess-after-clear-2"
    ));

    // 3) A DIFFERENT live process (our test runner `my_pid`) fails with DuplicateLiveSession!
    let dup_err = apply_presence_with_probe(
        &mut state,
        &mut seq,
        carol_p.clone(),
        UnixMs(3_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: SeatName::parse("carol").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: claude_endpoint(
                my_pid,
                &my_start,
                "sess-other-live",
                "interactive",
                Some("user"),
                Some("Carol-5.2"),
            ),
            liveness: LivenessMode::ProcessAndLease,
            ttl_ms: Some(600_000),
            takeover: false,
            allow_fork: false,
            mail_horizon_at: None,
            idem: None,
        }),
        is_process_alive,
    )
    .unwrap_err();
    assert!(matches!(
        dup_err,
        ApiError::Conflict {
            data: Some(ConflictData::DuplicateLiveSession { ref seat, ref active }),
            ..
        } if seat.as_str() == "carol" && active == &b1.incarnation_id
    ));

    // 4) Kill the old child process -> dead `(pid, proc_start)` auto-supersedes WITHOUT takeover (`--resume`)!
    let _ = child.kill();
    let _ = child.wait();
    assert!(!is_process_alive(child_pid, &child_start));

    let resume_reply = apply_presence_with_probe(
        &mut state,
        &mut seq,
        carol_p.clone(),
        UnixMs(4_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: SeatName::parse("carol").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: claude_endpoint(
                my_pid,
                &my_start,
                "sess-after-clear-2",
                "bg",
                Some("user"),
                Some("Carol-5.2"),
            ),
            liveness: LivenessMode::ProcessAndLease,
            ttl_ms: Some(600_000),
            takeover: false,
            allow_fork: false,
            mail_horizon_at: None,
            idem: None,
        }),
        is_process_alive,
    )
    .unwrap();
    let WireReply::PresenceBind(b_resumed) = resume_reply else {
        panic!("expected PresenceBind reply");
    };
    assert_eq!(b_resumed.epoch, 2);
    assert_ne!(b_resumed.incarnation_id, b1.incarnation_id);

    // 5) With `takeover: true`, a live process supersedes the active binding and bumps epoch.
    let mut second_child = Command::new("/bin/sleep").arg("60").spawn().unwrap();
    let second_pid = second_child.id();
    let second_start = query_process_start(second_pid).unwrap();
    let takeover_reply = apply_presence_with_probe(
        &mut state,
        &mut seq,
        carol_p,
        UnixMs(5_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: SeatName::parse("carol").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: claude_endpoint(
                second_pid,
                &second_start,
                "sess-takeover",
                "interactive",
                Some("user"),
                Some("Carol-5.2"),
            ),
            liveness: LivenessMode::ProcessAndLease,
            ttl_ms: Some(600_000),
            takeover: true,
            allow_fork: false,
            mail_horizon_at: None,
            idem: None,
        }),
        is_process_alive,
    )
    .unwrap();
    let _ = second_child.kill();
    let _ = second_child.wait();
    let WireReply::PresenceBind(b_takeover) = takeover_reply else {
        panic!("expected PresenceBind reply");
    };
    assert_eq!(b_takeover.epoch, 3);
    let carol_bindings = state.bindings_for(&SeatName::parse("carol").unwrap());
    assert_eq!(
        carol_bindings
            .iter()
            .filter(|b| b.status == BindingStatus::Live)
            .count(),
        1
    );
}

#[test]
fn gate1_all_10_mail_routing_cases_verified() {
    // Verify GATE1_CASE_TESTS maps every single GATE1_CASE_IDS entry 1:1 AND
    // every cited Rust integration test function actually exists in this file.
    assert_eq!(GATE1_CASE_TESTS.len(), GATE1_CASE_IDS.len());
    let expected_ids: BTreeSet<&str> = GATE1_CASE_IDS.iter().copied().collect();
    let mapped_ids: BTreeSet<&str> = GATE1_CASE_TESTS.iter().map(|(id, _)| *id).collect();
    assert_eq!(expected_ids, mapped_ids);
    let this_source = include_str!("presence_and_shadow_mail.rs");
    for &(case_id, test_fn) in GATE1_CASE_TESTS {
        assert!(
            this_source.contains(&format!("fn {test_fn}()")),
            "GATE1_CASE_TESTS entry '{case_id}' cites missing function '{test_fn}'"
        );
    }

    // When running with an opt-in live interchange directory, verify our 10-case
    // matrix covers 100% of `cases.jsonl` after applying any configured
    // `HouseProfile::fixture_id_aliases`.
    if let Some(root) = opt_in_interchange_dir() {
        let fixture_path = root.join("fixtures/mail-routing/cases.jsonl");
        if fixture_path.is_file() {
            let raw = fs::read_to_string(fixture_path).unwrap();
            let house = rex_wave_mailbridge::load_house_profile().unwrap();
            let report =
                rex_wave_mailbridge::check_gate1_fixtures_with_profile(&raw, &house).unwrap();
            assert!(
                report.fixture_ids_registered(),
                "expected Gate 1 case IDs to match cases.jsonl (missing={:?}, unknown={:?})",
                report.missing_case_ids,
                report.unknown_case_ids
            );
        }
    }
}

// Case 1: `seat-stale-six-days`
// Dead process expires -> RaiseUnrung + synchronous sender notice with exact timestamp.
#[test]
fn gate1_case_01_seat_stale_six_days() {
    let sys_p = Principal::System {
        part: SystemPart::PresenceMonitor,
    };
    let aliases = AliasMap::empty();
    let policy = RingPolicy::default();
    let empty_acks = parse_acks_jsonl("", &aliases).into_index();
    let empty_retry = RetryState::new();
    let empty_affinity = IncarnationAffinity::new();
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        let dave = SeatName::parse("dave").unwrap();
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            Principal::Seat { seat: dave.clone() },
            UnixMs(1_700_000_000_000),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: dave.clone(),
                incarnation_id: Some(IncarnationId::parse("inc-dave-dead").unwrap()),
                lane: None,
                endpoint: claude_endpoint(
                    4242,
                    "Thu Nov 14 00:00:00 2026",
                    "dead-sess",
                    "interactive",
                    Some("user"),
                    Some("Dave-Opus-5.5"),
                ),
                liveness: LivenessMode::ProcessAndLease,
                ttl_ms: Some(600_000),
                takeover: false,
                allow_fork: false,
                mail_horizon_at: None,
                idem: None,
            }),
            |_, _| false,
        )
        .unwrap();

        let expired = st.expired_bindings(UnixMs(1_700_000_010_000), |_, _| false);
        assert_eq!(expired.len(), 1);
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            sys_p.clone(),
            UnixMs(1_700_000_010_000),
            PresenceCommand::PresenceExpire(expired[0].clone()),
            |_, _| false,
        )
        .unwrap();

        let letter = sample_candidate(
            "mail/carol-to-dave.md",
            "dave",
            b"---\nto: dave\n---\nhello",
            UnixMs(1_700_000_020_000),
        );
        let decision = decide_mail_ring(
            &letter,
            &empty_acks,
            &st,
            &empty_affinity,
            &empty_retry,
            &policy,
            &aliases,
            UnixMs(1_700_000_020_000),
        );
        match decision {
            RingDecision::RaiseUnrung {
                seat,
                sender_notice,
                referents,
                ..
            } => {
                assert_eq!(seat, dave);
                assert_eq!(referents.len(), 1);
                assert_eq!(
                    sender_notice,
                    "banked; no live binding for dave since 2023-11-14T22:13:20Z; will ring on rebind"
                );
            }
            other => panic!("expected RaiseUnrung for seat-stale-six-days, got {other:?}"),
        }
    }
}

// Case 2: `carbon-copy-misroute`
// Durable SeatRecord answers project/role queries offline; ambiguous query fails with Conflict listing all candidates.
#[test]
fn gate1_case_02_carbon_copy_misroute() {
    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            owner_p.clone(),
            UnixMs(1_000),
            PresenceCommand::PresenceClaim(PresenceClaim {
                seat: SeatName::parse("eve").unwrap(),
                status: Some(SeatStatus::Active),
                role: Some("lead".into()),
                projects: vec!["rex-wave".into(), "shared-infra".into()],
                aliases: Vec::new(),
                home_room: None,
                mail_horizon_at: None,
                work_class: None,
                avatar: None,
                home_dir: None,
                start_cmd: None,
                resume_cmd: None,
                rex_target: None,
                verified_how: None,
                replace: None,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            owner_p.clone(),
            UnixMs(1_000),
            PresenceCommand::PresenceClaim(PresenceClaim {
                seat: SeatName::parse("carbon").unwrap(),
                status: Some(SeatStatus::Active),
                role: Some("lead".into()),
                projects: vec!["shared-infra".into()],
                aliases: Vec::new(),
                home_room: None,
                mail_horizon_at: None,
                work_class: None,
                avatar: None,
                home_dir: None,
                start_cmd: None,
                resume_cmd: None,
                rex_target: None,
                verified_how: None,
                replace: None,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();

        let unique = st
            .resolve_at(
                &PresenceResolve {
                    query: "rex-wave".into(),
                    lane: None,
                },
                UnixMs(2_000),
            )
            .unwrap();
        assert_eq!(unique.record.seat.as_str(), "eve");

        let ambig_project = st
            .resolve_at(
                &PresenceResolve {
                    query: "shared-infra".into(),
                    lane: None,
                },
                UnixMs(2_000),
            )
            .unwrap_err();
        match ambig_project {
            ApiError::Conflict { detail, .. } => {
                assert!(
                    detail.contains("carbon") && detail.contains("eve"),
                    "{detail}"
                );
            }
            other => panic!("expected Conflict listing all candidates, got {other:?}"),
        }

        let ambig_role = st
            .resolve_at(
                &PresenceResolve {
                    query: "lead".into(),
                    lane: None,
                },
                UnixMs(2_000),
            )
            .unwrap_err();
        assert!(matches!(ambig_role, ApiError::Conflict { .. }));
    }
}

// Case 3: `two-tables-one-name`
// Single Presence truth resolves `carbon` with its live binding cleanly.
#[test]
fn gate1_case_03_two_tables_one_name() {
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        let carbon = SeatName::parse("carbon").unwrap();
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            Principal::Seat {
                seat: carbon.clone(),
            },
            UnixMs(1_000),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: carbon.clone(),
                incarnation_id: None,
                lane: None,
                endpoint: TransportEndpoint::UdsSession {
                    conn_id: 99,
                    client: "rex-block".into(),
                },
                liveness: LivenessMode::SocketLifetime,
                ttl_ms: None,
                takeover: false,
                allow_fork: false,
                mail_horizon_at: None,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();
        let res = st
            .resolve_at(
                &PresenceResolve {
                    query: "carbon".into(),
                    lane: None,
                },
                UnixMs(1_500),
            )
            .unwrap();
        assert_eq!(res.live_bindings.len(), 1);
        assert_eq!(
            sender_delivery_notice(&st, &carbon, UnixMs(1_500)),
            None,
            "live binding needs no offline notice"
        );
    }
}

// Case 4: `dry-run-marks-seen`
// ReadOnlyInterchange + run_shadow_pass emits decisions without touching any file on disk.
#[test]
fn gate1_case_04_dry_run_marks_seen() {
    let aliases = AliasMap::empty();
    let policy = RingPolicy::default();
    let empty_retry = RetryState::new();
    let empty_affinity = IncarnationAffinity::new();
    {
        let dir = TempDirGuard::new("dry-run");
        fs::create_dir_all(dir.path().join("mail")).unwrap();
        fs::create_dir_all(dir.path().join("state")).unwrap();
        let letter_path = dir.path().join("mail/1795-carol-to-dave.md");
        let letter_bytes = b"---\nfrom: carol\nto: dave\nsubject: check\n---\nbody\n";
        fs::write(&letter_path, letter_bytes).unwrap();
        let acks_path = dir.path().join("state/acks.jsonl");
        fs::write(&acks_path, b"").unwrap();
        let acks_before = fs::read(&acks_path).unwrap();

        let ro = ReadOnlyInterchange::open(dir.path()).unwrap();
        let st = PresenceState::default();
        let report = run_shadow_pass(
            &ro,
            &st,
            &empty_affinity,
            &empty_retry,
            &policy,
            &aliases,
            UnixMs(10_000),
        )
        .unwrap();
        assert_eq!(report.letters_scanned, 1);
        assert!(report.decisions.iter().any(
            |d| matches!(d, RingDecision::RaiseUnrung { seat, .. } if seat.as_str() == "dave")
        ));
        assert_eq!(fs::read(&acks_path).unwrap(), acks_before);
        let state_entries: Vec<_> = fs::read_dir(dir.path().join("state"))
            .unwrap()
            .flatten()
            .collect();
        assert_eq!(
            state_entries.len(),
            1,
            "shadow dry run must create zero cursor or seen files"
        );
    }
}

// Case 5: `ring-types-into-a-live-turn`
// Rings always use non-intrusive queued/socket channels (never raw pane keystrokes).
#[test]
fn gate1_case_05_ring_types_into_a_live_turn() {
    let aliases = AliasMap::empty();
    let policy = RingPolicy::default();
    let empty_acks = parse_acks_jsonl("", &aliases).into_index();
    let empty_retry = RetryState::new();
    let empty_affinity = IncarnationAffinity::new();
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        let carol = SeatName::parse("carol").unwrap();
        let frank = SeatName::parse("frank").unwrap();
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            Principal::Seat {
                seat: carol.clone(),
            },
            UnixMs(1_000),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: carol.clone(),
                incarnation_id: None,
                lane: None,
                endpoint: claude_endpoint(
                    8195,
                    "Thu Nov 12 06:22:59 2026",
                    "59e9053e",
                    "bg",
                    Some("user"),
                    Some("Carol-5.2"),
                ),
                liveness: LivenessMode::ProcessAndLease,
                ttl_ms: Some(600_000),
                takeover: false,
                allow_fork: false,
                mail_horizon_at: None,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            Principal::Seat {
                seat: frank.clone(),
            },
            UnixMs(1_000),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: frank.clone(),
                incarnation_id: None,
                lane: None,
                endpoint: TransportEndpoint::BbThread {
                    thread_id: BbThreadId::parse("th_frank_1").unwrap(),
                },
                liveness: LivenessMode::SocketLifetime,
                ttl_ms: None,
                takeover: false,
                allow_fork: false,
                mail_horizon_at: None,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();

        let d_carol = decide_mail_ring(
            &sample_candidate("mail/a.md", "carol", b"hi carol", UnixMs(1_500)),
            &empty_acks,
            &st,
            &empty_affinity,
            &empty_retry,
            &policy,
            &aliases,
            UnixMs(1_500),
        );
        assert!(matches!(
            d_carol,
            RingDecision::Ring {
                channel: RingDeliveryChannel::ClaudeMessagingSocket { pid: 8195, .. },
                ..
            }
        ));

        let d_frank = decide_mail_ring(
            &sample_candidate("mail/b.md", "frank", b"hi frank", UnixMs(1_500)),
            &empty_acks,
            &st,
            &empty_affinity,
            &empty_retry,
            &policy,
            &aliases,
            UnixMs(1_500),
        );
        assert!(matches!(
            d_frank,
            RingDecision::Ring {
                channel: RingDeliveryChannel::BbQueueIfActive { .. },
                ..
            }
        ));
    }
}

// Case 6: `a-transport-dies`
// Per-binding expiry touches ONE seat only; provider TransportHealth flips ALL bindings of that TransportKind together.
#[test]
fn gate1_case_06_a_transport_dies() {
    let sys_p = Principal::System {
        part: SystemPart::PresenceMonitor,
    };
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        let quire = SeatName::parse("quire").unwrap();
        let trent = SeatName::parse("trent").unwrap();
        for (seat, tid) in [(&quire, "th_quire"), (&trent, "th_trent")] {
            apply_presence_with_probe(
                &mut st,
                &mut seq,
                Principal::Seat { seat: seat.clone() },
                UnixMs(1_000),
                PresenceCommand::PresenceBind(PresenceBind {
                    seat: seat.clone(),
                    incarnation_id: Some(IncarnationId::parse(format!("inc-{seat}")).unwrap()),
                    lane: None,
                    endpoint: TransportEndpoint::BbThread {
                        thread_id: BbThreadId::parse(tid).unwrap(),
                    },
                    liveness: LivenessMode::SocketLifetime,
                    ttl_ms: None,
                    takeover: false,
                    allow_fork: false,
                    mail_horizon_at: None,
                    idem: None,
                }),
                |_, _| true,
            )
            .unwrap();
        }

        // Provider-level circuit-breaker marks both BbThread bindings TransportDown together.
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            sys_p.clone(),
            UnixMs(2_000),
            PresenceCommand::PresenceTransportHealth(PresenceTransportHealth {
                transport: TransportKind::BbThread,
                down: true,
                detail: Some("bb bridge disconnected".into()),
            }),
            |_, _| true,
        )
        .unwrap();
        assert!(st.live_bindings_for(&quire, None, UnixMs(2_000)).is_empty());
        assert!(st.live_bindings_for(&trent, None, UnixMs(2_000)).is_empty());
        assert_eq!(
            st.bindings_for(&quire)[0].status,
            BindingStatus::TransportDown
        );
        assert_eq!(
            st.bindings_for(&trent)[0].status,
            BindingStatus::TransportDown
        );

        // Recovery restores both; then expiring ONLY quire leaves trent live.
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            sys_p.clone(),
            UnixMs(3_000),
            PresenceCommand::PresenceTransportHealth(PresenceTransportHealth {
                transport: TransportKind::BbThread,
                down: false,
                detail: None,
            }),
            |_, _| true,
        )
        .unwrap();
        assert_eq!(st.live_bindings_for(&quire, None, UnixMs(3_000)).len(), 1);
        assert_eq!(st.live_bindings_for(&trent, None, UnixMs(3_000)).len(), 1);

        apply_presence_with_probe(
            &mut st,
            &mut seq,
            sys_p.clone(),
            UnixMs(4_000),
            PresenceCommand::PresenceExpire(PresenceExpire {
                seat: quire.clone(),
                incarnation_id: IncarnationId::parse("inc-quire").unwrap(),
                reason: "single thread EOF".into(),
            }),
            |_, _| true,
        )
        .unwrap();
        assert!(st.live_bindings_for(&quire, None, UnixMs(4_000)).is_empty());
        assert_eq!(
            st.live_bindings_for(&trent, None, UnixMs(4_000)).len(),
            1,
            "per-binding expiry must not affect sibling bindings on the same transport"
        );
    }
}

// Case 7: `two-seats-one-name`
// Refuses claiming or replacing another seat without authorization; only an operator/system
// may replace another seat (retiring it and unbinding its live sessions).
#[test]
fn gate1_case_07_two_seats_one_name() {
    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        let peggy = SeatName::parse("peggy").unwrap();
        let peggy_p = Principal::Seat {
            seat: peggy.clone(),
        };
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            peggy_p.clone(),
            UnixMs(1_000),
            PresenceCommand::PresenceClaim(PresenceClaim {
                seat: peggy.clone(),
                status: Some(SeatStatus::Active),
                role: Some("audio".into()),
                projects: Vec::new(),
                aliases: Vec::new(),
                home_room: None,
                mail_horizon_at: None,
                work_class: None,
                avatar: None,
                home_dir: None,
                start_cmd: None,
                resume_cmd: None,
                rex_target: None,
                verified_how: None,
                replace: None,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            peggy_p,
            UnixMs(1_500),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: peggy.clone(),
                incarnation_id: None,
                lane: None,
                endpoint: TransportEndpoint::UdsSession {
                    conn_id: 77,
                    client: "peggy-cli".into(),
                },
                liveness: LivenessMode::SocketLifetime,
                ttl_ms: None,
                takeover: false,
                allow_fork: false,
                mail_horizon_at: None,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();

        let newcomer = Principal::Seat {
            seat: SeatName::parse("frank").unwrap(),
        };
        let worker = Principal::Worker {
            wave: WaveSlug::parse("w1").unwrap(),
            role: RoleName::parse("coder").unwrap(),
        };

        let collision = apply_presence_with_probe(
            &mut st,
            &mut seq,
            newcomer.clone(),
            UnixMs(2_000),
            PresenceCommand::PresenceClaim(PresenceClaim {
                seat: peggy.clone(),
                status: None,
                role: Some("emacs".into()),
                projects: Vec::new(),
                aliases: Vec::new(),
                home_room: None,
                mail_horizon_at: None,
                work_class: None,
                avatar: None,
                home_dir: None,
                start_cmd: None,
                resume_cmd: None,
                rex_target: None,
                verified_how: None,
                replace: None,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap_err();
        assert!(matches!(collision, ApiError::Conflict { .. }));

        // Neither a peer seat nor a Worker may hijack or retire another seat via `replace`.
        for unauthorized in [newcomer, worker] {
            let replace_same = apply_presence_with_probe(
                &mut st,
                &mut seq,
                unauthorized.clone(),
                UnixMs(2_500),
                PresenceCommand::PresenceClaim(PresenceClaim {
                    seat: peggy.clone(),
                    status: Some(SeatStatus::Active),
                    role: Some("hijack".into()),
                    projects: Vec::new(),
                    aliases: Vec::new(),
                    home_room: None,
                    mail_horizon_at: None,
                    work_class: None,
                    avatar: None,
                    home_dir: None,
                    start_cmd: None,
                    resume_cmd: None,
                    rex_target: None,
                    verified_how: None,
                    replace: Some(peggy.clone()),
                    idem: None,
                }),
                |_, _| true,
            )
            .unwrap_err();
            assert!(
                matches!(replace_same, ApiError::Denied { .. }),
                "expected Denied for replace=self by {unauthorized}, got {replace_same:?}"
            );

            let replace_other = apply_presence_with_probe(
                &mut st,
                &mut seq,
                unauthorized.clone(),
                UnixMs(2_600),
                PresenceCommand::PresenceClaim(PresenceClaim {
                    seat: SeatName::parse("frank").unwrap(),
                    status: Some(SeatStatus::Active),
                    role: Some("emacs".into()),
                    projects: Vec::new(),
                    aliases: Vec::new(),
                    home_room: None,
                    mail_horizon_at: None,
                    work_class: None,
                    avatar: None,
                    home_dir: None,
                    start_cmd: None,
                    resume_cmd: None,
                    rex_target: None,
                    verified_how: None,
                    replace: Some(peggy.clone()),
                    idem: None,
                }),
                |_, _| true,
            )
            .unwrap_err();
            assert!(
                matches!(replace_other, ApiError::Denied { .. }),
                "expected Denied for replace=other by {unauthorized}, got {replace_other:?}"
            );
        }
        assert_eq!(st.seat(&peggy).unwrap().status, SeatStatus::Active);
        assert_eq!(st.live_bindings_for(&peggy, None, UnixMs(2_700)).len(), 1);

        // Authorized operator replacement retires the replaced seat and unbinds its live sessions.
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            owner_p.clone(),
            UnixMs(3_000),
            PresenceCommand::PresenceClaim(PresenceClaim {
                seat: SeatName::parse("frank").unwrap(),
                status: Some(SeatStatus::Active),
                role: Some("emacs".into()),
                projects: Vec::new(),
                aliases: Vec::new(),
                home_room: None,
                mail_horizon_at: None,
                work_class: None,
                avatar: None,
                home_dir: None,
                start_cmd: None,
                resume_cmd: None,
                rex_target: None,
                verified_how: None,
                replace: Some(peggy.clone()),
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();
        assert_eq!(st.seat(&peggy).unwrap().status, SeatStatus::Retired);
        assert!(st.live_bindings_for(&peggy, None, UnixMs(3_000)).is_empty());
        assert_eq!(
            st.seat(&SeatName::parse("frank").unwrap()).unwrap().status,
            SeatStatus::Active
        );
    }
}

// Case 8: `session-names-drift`
// Keyed on `(pid, proc_start, session_id, socket)` + `in_reply_to` incarnation affinity.
#[test]
fn gate1_case_08_session_names_drift() {
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        let dave = SeatName::parse("dave").unwrap();
        let gp = Principal::Seat { seat: dave.clone() };
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            gp.clone(),
            UnixMs(1_000),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: dave.clone(),
                incarnation_id: Some(IncarnationId::parse("inc-dave-main").unwrap()),
                lane: None,
                endpoint: claude_endpoint(
                    85820,
                    "Thu Nov 19 05:52:37 2026",
                    "044c8872",
                    "interactive",
                    Some("derived"),
                    Some("users-c7"),
                ),
                liveness: LivenessMode::ProcessAndLease,
                ttl_ms: Some(600_000),
                takeover: false,
                allow_fork: false,
                mail_horizon_at: None,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();
        // Display name drifts `users-c7` -> `users-f0` on the same `(pid, proc_start)`: refreshes seamlessly.
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            gp.clone(),
            UnixMs(2_000),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: dave.clone(),
                incarnation_id: None,
                lane: None,
                endpoint: claude_endpoint(
                    85820,
                    "Thu Nov 19 05:52:37 2026",
                    "044c8872",
                    "interactive",
                    Some("derived"),
                    Some("users-f0"),
                ),
                liveness: LivenessMode::ProcessAndLease,
                ttl_ms: Some(600_000),
                takeover: false,
                allow_fork: false,
                mail_horizon_at: None,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();
        // Second live lane (`dave-2d`) bound on explicit lane `"2d"`.
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            gp,
            UnixMs(2_500),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: dave.clone(),
                incarnation_id: Some(IncarnationId::parse("inc-dave-2d").unwrap()),
                lane: Some("2d".into()),
                endpoint: claude_endpoint(
                    18604,
                    "Thu Nov 19 06:00:00 2026",
                    "2d-sess",
                    "interactive",
                    Some("user"),
                    Some("Dave-2D"),
                ),
                liveness: LivenessMode::ProcessAndLease,
                ttl_ms: Some(600_000),
                takeover: false,
                allow_fork: false,
                mail_horizon_at: None,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();

        let mut affinity = IncarnationAffinity::new();
        affinity.record_authored(
            "letter-2d-root",
            dave.clone(),
            IncarnationId::parse("inc-dave-2d").unwrap(),
        );
        let picked_reply = select_binding_for_delivery(
            &st,
            &dave,
            None,
            Some("letter-2d-root"),
            &affinity,
            UnixMs(3_000),
        )
        .unwrap();
        assert_eq!(picked_reply.incarnation_id.as_str(), "inc-dave-2d");

        let picked_default =
            select_binding_for_delivery(&st, &dave, None, None, &affinity, UnixMs(3_000)).unwrap();
        assert_eq!(picked_default.incarnation_id.as_str(), "inc-dave-main");
    }
}

// Case 9: `own-watcher-liveness`
// SelfWatcher holds a TTL lease; alive while renewed, expires deterministically when missed.
#[test]
fn gate1_case_09_own_watcher_liveness() {
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        let baton = SeatName::parse("baton").unwrap();
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            Principal::Seat {
                seat: baton.clone(),
            },
            UnixMs(10_000),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: baton.clone(),
                incarnation_id: Some(IncarnationId::parse("inc-baton-lease").unwrap()),
                lane: None,
                endpoint: TransportEndpoint::SelfWatcher {
                    harness: "codex-cron".into(),
                    thread_id: None,
                    pid: None,
                    proc_start: None,
                },
                liveness: LivenessMode::LeaseOnly,
                ttl_ms: Some(900_000),
                takeover: false,
                allow_fork: false,
                mail_horizon_at: None,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();
        assert!(matches!(
            format_delivery_status(&st, &baton, UnixMs(500_000)),
            DeliveryStatus::Live { .. }
        ));
        assert!(matches!(
            format_delivery_status(&st, &baton, UnixMs(910_000)),
            DeliveryStatus::Offline { .. }
        ));
    }
}

// Case 10: `bank-before-ring`
// Banking writes canon letter bytes to disk even when the recipient seat has ZERO live bindings.
#[test]
fn gate1_case_10_bank_before_ring() {
    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = TempDirGuard::new("bank-before-ring");
            fs::write(dir.path().join(".rex-wave-sandbox"), b"test\n").unwrap();
            let sandbox = SandboxRoot::verify_with(dir.path(), &Protection::default()).unwrap();
            let journal = Journal::in_memory().unwrap();
            let hub = Hub::new(64);
            let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
            let mail_writer = journal.claim(StreamId::Mail).unwrap();
            let router = Arc::new(Router {
                room: Registry::<Rooms>::new(journal.clone(), hub.clone()),
                wave: Arc::new(Unavailable(Family::Wave)),
                board: Arc::new(Unavailable(Family::Board)),
                inbox: Arc::new(Unavailable(Family::Inbox)),
                mail: Arc::new(MailBridge::with_hub(
                    sandbox.clone(),
                    journal.clone(),
                    mail_writer,
                    hub.clone(),
                )),
                presence: Registry::<Presence>::new(journal.clone(), hub.clone()),
                read: Arc::new(ReadSide {
                    journal,
                    hub,
                    sidebar,
                    agents: Arc::new(NoAgentFeed),
                }),
            });

            let owner = LocalClient::new(router.clone(), owner_p.clone());
            let room_ref = RoomRef::Channel {
                scope: RoomScope::Global,
                name: ChannelName::parse("lounge").unwrap(),
            };
            let posted = owner
                .call(RoomPost::new(
                    room_ref,
                    Body::parse("Bank this letter even when dave is offline.").unwrap(),
                ))
                .await
                .unwrap();

            // `dave` has NO presence binding, yet `MailPromote` banks the file durably first.
            let letter = owner
                .call(MailPromote {
                    room: posted.room,
                    range: SeqRange::new(posted.seq, posted.seq).unwrap(),
                    to: vec![SeatName::parse("dave").unwrap()],
                    subject: "Offline delivery".into(),
                    topic: "offline-bank".into(),
                    priority: LetterPriority::Normal,
                })
                .await
                .expect("banking succeeds unconditionally without a live binding");
            assert!(sandbox.path().join(&letter.path).is_file());
        });
    }
}

#[test]
fn gate1_all_6_friction_rows_edge_cases_and_sandbox_ack_matching() {
    let aliases = test_aliases();
    let policy = RingPolicy::default();
    let affinity = IncarnationAffinity::new();

    let sha_a = sha256_of(b"letter-a-bytes");
    let sha_b = sha256_of(b"letter-b-bytes");
    let sha_c = sha256_of(b"letter-c-bytes");
    let sha_d = sha256_of(b"letter-d-bytes");
    let sha_mut_old = sha256_of(b"original-acked-bytes");
    let sha_mut_new = sha256_of(b"amended-in-place-bytes");

    // Friction 1: Tolerant `acks.jsonl` parser handles 10+ historical schemas + non-JSON lines.
    let mut mixed_lines = vec![
        // 1. Canonical acked_by + 64-hex sha256 + to-cc/
        format!(
            r#"{{"ts":"2026-10-07T03:21:00Z","letter":"to-cc/a.md","sha256":"{}","acked_by":"Desk-V1","note":"read"}}"#,
            sha_a.as_str()
        ),
        // 2. seat + 16-hex sha256_16 + interchange/mail/...
        format!(
            r#"{{"ts":"2026-10-31T06:3xZ","seat":"desk_old","kind":"ack","letter":"interchange/mail/b.md","sha256_16":"{}"}}"#,
            &sha_b.as_str()[..16]
        ),
        // 3. by + sha16 + bare filename
        format!(
            r#"{{"ts":"2026-11-03T07:0x CST","by":"cc-desk","letter":"c.md","sha16":"{}"}}"#,
            &sha_c.as_str()[..16]
        ),
        // 4. file + sha256_full + sha256 short
        format!(
            r#"{{"ts":"2026-09-17T09:13:46+00:00","file":"interchange/to-lucas/d.md","sha256":"{}","sha256_full":"{}","by":"Lucas-rev"}}"#,
            &sha_d.as_str()[..16],
            sha_d.as_str()
        ),
        // 5. letters array of objects with seat
        format!(
            r#"{{"ts":"2026-10-31T01:50:00-05:00","kind":"ack","seat":"carol","letters":[{{"file":"interchange/mail/mut.md","sha256_16":"{}"}}]}}"#,
            &sha_mut_old.as_str()[..16]
        ),
        // 6. letters array of strings (path-only historical row)
        r#"{"ts":"2026-10-29T04:57:03+0000","seat":"carol","letters":["interchange/mail/old1.md","interchange/mail/old2.md"],"ack":"cc-read"}"#.to_owned(),
        // 7. items batch array
        r#"{"ts":"2026-10-31T08:05:00-05:00","kind":"ack-batch","seat":"carol","items":[{"letter":"reconcile.md","read":"done"}]}"#.to_owned(),
        // 8. non-ACK owner-ruling row (counted cleanly, not skipped as broken)
        r#"{"ts":"2026-10-30T11:06:00-05:00","kind":"owner-ruling","topic":"backups-over-everything","verbatim":"keep it clean"}"#.to_owned(),
        // 9. legacy sandbox schema (path + seat + sha256_full)
        format!(
            r#"{{"schema":1,"seat":"lucas","path":"to-lucas/mut.md","sha256_full":"{}"}}"#,
            sha_mut_old.as_str()
        ),
        // 10. Non-JSON plain-text checksum line
        "88c1baf5b74431e4b39142c0f60beb0d3ed2f8d7d0c54d6980657d0fbd82ae3a  to-cc/intro-r1.md".to_owned(),
    ];
    let acks_text = mixed_lines.join("\n");
    let report = parse_acks_jsonl(&acks_text, &aliases);
    assert_eq!(report.skipped_lines.len(), 1);
    assert_eq!(report.skipped_lines[0].line_number, 10);
    assert_eq!(report.non_ack_rows, 1);
    let idx = report.index();
    assert!(idx.is_acked_by_digest("carol", &sha_a, &aliases));
    assert!(idx.is_acked_by_digest("carol", &sha_b, &aliases));
    assert!(idx.is_acked_by_digest("carol", &sha_c, &aliases));
    assert!(idx.is_acked_by_digest("lucas", &sha_d, &aliases));
    assert!(!idx.is_acked_by_digest("dave", &sha_a, &aliases));

    // Also verify `project()` in lib.rs uses the tolerant `ack_row_matches` without hardcoded aliases.
    let sb_dir = TempDirGuard::new("sandbox-acks-compat");
    fs::write(sb_dir.path().join(".rex-wave-sandbox"), b"ok\n").unwrap();
    fs::create_dir_all(sb_dir.path().join("to-carol")).unwrap();
    fs::create_dir_all(sb_dir.path().join("state")).unwrap();
    let sb_letter = b"---\nfrom: dave\nto: carol\nsubject: Compat\n---\nbody\n";
    let sb_sha = sha256_of(sb_letter);
    let sb_letter_path = sb_dir.path().join("to-carol/compat-r1.md");
    fs::write(&sb_letter_path, sb_letter).unwrap();
    // Write a historical-format ACK row (`acked_by: @Carol`, `letter: interchange/to-carol/compat-r1.md`, 16-hex `sha16`) plus a non-JSON line.
    mixed_lines.push(format!(
        r#"{{"ts":"2026-11-21T06:00:00Z","acked_by":"@Carol","letter":"interchange/to-carol/compat-r1.md","sha16":"{}"}}"#,
        &sb_sha.as_str()[..16]
    ));
    fs::write(
        sb_dir.path().join("state/acks.jsonl"),
        mixed_lines.join("\n"),
    )
    .unwrap();
    let sandbox = SandboxRoot::verify_with(sb_dir.path(), &Protection::default()).unwrap();
    let item = project(
        &sandbox,
        &sandbox.path().join("to-carol/compat-r1.md"),
        sb_letter,
    )
    .unwrap();
    assert_eq!(
        item.state, "acked",
        "sandbox project() must recognize historical acks.jsonl schemas and ignore non-JSON lines"
    );

    // Prepare live bindings for `lucas` and `carol` for frictions 2..6.
    let mut st = PresenceState::default();
    let mut seq = 0;
    for (seat, conn_id) in [("lucas", 11u64), ("carol", 12u64)] {
        let s = SeatName::parse(seat).unwrap();
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            Principal::Seat { seat: s.clone() },
            UnixMs(1_000),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: s,
                incarnation_id: None,
                lane: None,
                endpoint: TransportEndpoint::UdsSession {
                    conn_id,
                    client: "seat".into(),
                },
                liveness: LivenessMode::SocketLifetime,
                ttl_ms: None,
                takeover: false,
                allow_fork: false,
                mail_horizon_at: None,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();
    }

    // Friction 2: `(seat, sha256)` dedupe across `mail/` and `to-lucas/` copies (acked AND unacked).
    let copy_mail = sample_candidate("mail/dup-r1.md", "lucas", b"same-content", UnixMs(2_000));
    let copy_box = sample_candidate(
        "to-lucas/dup-r1.md",
        "lucas",
        b"same-content",
        UnixMs(2_000),
    );
    let decisions_unacked = decide_mail_rings_for_seat(
        &SeatName::parse("lucas").unwrap(),
        &[copy_mail, copy_box],
        &idx,
        &st,
        &affinity,
        &RetryState::new(),
        &policy,
        &aliases,
        UnixMs(2_000),
    );
    assert_eq!(
        decisions_unacked.len(),
        1,
        "mailbox copy and root copy with identical SHA-256 ring at most once"
    );
    assert!(matches!(decisions_unacked[0], RingDecision::Ring { .. }));

    let acked_copy = sample_candidate("mail/d.md", "lucas", b"letter-d-bytes", UnixMs(2_000));
    let d_skip = decide_mail_ring(
        &acked_copy,
        &idx,
        &st,
        &affinity,
        &RetryState::new(),
        &policy,
        &aliases,
        UnixMs(2_000),
    );
    assert!(matches!(d_skip, RingDecision::SkipAlreadyAcked { .. }));

    // Friction 3: `FlagMutatedAfterAck` when an already-ACKed path changes SHA-256.
    let mutated = LetterCandidate {
        rel_path: "to-lucas/mut.md".into(),
        sha256: sha_mut_new.clone(),
        sha16: sha_mut_new.as_str()[..16].to_owned(),
        from: Some("dave".into()),
        to: SeatName::parse("lucas").unwrap(),
        subject: Some("Amended".into()),
        letter_id: None,
        in_reply_to: None,
        receipt_only: false,
        seen_at: UnixMs(2_000),
        time_basis: TimeBasis::LetterIdMs,
        date_only: false,
    };
    let d_mut = decide_mail_ring(
        &mutated,
        &idx,
        &st,
        &affinity,
        &RetryState::new(),
        &policy,
        &aliases,
        UnixMs(2_000),
    );
    match d_mut {
        RingDecision::FlagMutatedAfterAck { prior_ack_sha, .. } => {
            assert_eq!(prior_ack_sha, sha_mut_old.as_str());
        }
        other => panic!("expected FlagMutatedAfterAck, got {other:?}"),
    }

    // Friction 4: Wall-clock age-based retry expiry (`first_seen + deadline`), never pass count.
    let busy_letter = sample_candidate("mail/busy.md", "carol", b"busy-body", UnixMs(10_000));
    let mut retry_many_passes = RetryState::new();
    retry_many_passes.insert(
        SeatName::parse("carol").unwrap(),
        busy_letter.sha256.clone(),
        RetryEntry {
            first_seen_at: UnixMs(10_000),
            deadline_ms: 3_600_000,
            target_busy: true,
            pass_count: 500,
        },
    );
    let d_still_deferred = decide_mail_ring(
        &busy_letter,
        &idx,
        &st,
        &affinity,
        &retry_many_passes,
        &policy,
        &aliases,
        UnixMs(10_000 + 3_599_999),
    );
    assert!(
        matches!(
            d_still_deferred,
            RingDecision::Defer {
                reason: DeferReason::TargetBusy { pass_count: 500 },
                ..
            }
        ),
        "500 extra passes inside wall-clock window must NOT trigger early give-up"
    );

    let mut retry_one_pass_expired = RetryState::new();
    retry_one_pass_expired.insert(
        SeatName::parse("carol").unwrap(),
        busy_letter.sha256.clone(),
        RetryEntry {
            first_seen_at: UnixMs(10_000),
            deadline_ms: 3_600_000,
            target_busy: true,
            pass_count: 1,
        },
    );
    let d_expired = decide_mail_ring(
        &busy_letter,
        &idx,
        &st,
        &affinity,
        &retry_one_pass_expired,
        &policy,
        &aliases,
        UnixMs(10_000 + 3_600_000),
    );
    assert!(
        matches!(d_expired, RingDecision::RaiseUnrung { .. }),
        "wall-clock deadline expiry triggers RaiseUnrung even after 1 pass"
    );

    // Friction 5: `>3` queued letters coalesce into ONE `RingDigest` listing every `(path, sha16)`.
    let carol_seat = SeatName::parse("carol").unwrap();
    let burst: Vec<LetterCandidate> = (1..=4)
        .map(|i| {
            sample_candidate(
                &format!("mail/burst-{i}.md"),
                "carol",
                format!("burst-body-{i}").as_bytes(),
                UnixMs(20_000 + i),
            )
        })
        .collect();
    let three = decide_mail_rings_for_seat(
        &carol_seat,
        &burst[..3],
        &idx,
        &st,
        &affinity,
        &RetryState::new(),
        &policy,
        &aliases,
        UnixMs(25_000),
    );
    assert_eq!(three.len(), 3);
    assert!(three.iter().all(|d| matches!(d, RingDecision::Ring { .. })));

    let four = decide_mail_rings_for_seat(
        &carol_seat,
        &burst,
        &idx,
        &st,
        &affinity,
        &RetryState::new(),
        &policy,
        &aliases,
        UnixMs(25_000),
    );
    assert_eq!(four.len(), 1);
    match &four[0] {
        RingDecision::RingDigest { referents, .. } => {
            assert_eq!(referents.len(), 4);
            assert_eq!(referents[0].path(), "mail/burst-1.md");
            assert_eq!(referents[0].sha16().len(), 16);
        }
        other => panic!("expected RingDigest for 4 queued letters, got {other:?}"),
    }

    // Friction 6: Mandatory `(path, sha16)` referent + receipt-only skip.
    assert!(RingReferent::new("", sha_a.clone()).is_err());
    let empty_path_cand = sample_candidate("   ", "carol", b"no-path", UnixMs(25_000));
    assert!(matches!(
        decide_mail_ring(
            &empty_path_cand,
            &idx,
            &st,
            &affinity,
            &RetryState::new(),
            &policy,
            &aliases,
            UnixMs(25_000),
        ),
        RingDecision::RefuseMissingReferent { .. }
    ));

    let mut receipt_only = sample_candidate("mail/receipt.md", "carol", b"receipt", UnixMs(25_000));
    receipt_only.receipt_only = true;
    assert!(matches!(
        decide_mail_ring(
            &receipt_only,
            &idx,
            &st,
            &affinity,
            &RetryState::new(),
            &policy,
            &aliases,
            UnixMs(25_000),
        ),
        RingDecision::SkipReceiptOnly { .. }
    ));

    // Also verify ReadOnlyInterchange parses receipt-only frontmatter directly from disk.
    let ro_dir = TempDirGuard::new("receipt-only-fm");
    fs::create_dir_all(ro_dir.path().join("mail")).unwrap();
    fs::write(
        ro_dir.path().join("mail/pool-custody.md"),
        "---\nfrom: carol\nto: lucas\nstatus: \"receipt only; no return ACK requested\"\n---\nbody\n",
    )
    .unwrap();
    let ro = ReadOnlyInterchange::open(ro_dir.path()).unwrap();
    let scanned = ro.scan_letters(&aliases).unwrap();
    assert_eq!(scanned.len(), 1);
    assert!(scanned[0].receipt_only);
}

#[test]
fn gate2_shadow_scorer_and_opt_in_interchange_smoke() {
    let policy = RingPolicy::default();
    let affinity = IncarnationAffinity::new();

    // Deterministic synthetic Gate 2 test (always runs, including CI).
    let dir = TempDirGuard::new("gate2-synthetic");
    fs::create_dir_all(dir.path().join("mail")).unwrap();
    fs::create_dir_all(dir.path().join("state")).unwrap();

    let b_acked = b"---\nfrom: carol\nto: lucas\n---\nalready read\n";
    let b_live = b"---\nfrom: carol\nto: lucas\n---\nnew for live lucas\n";
    let b_missed = b"---\nfrom: carol\nto: dave\n---\nmissed for offline dave\n";
    let b_retired = b"---\nfrom: carol\nto: quire\n---\nto retired quire\n";
    fs::write(dir.path().join("mail/01-acked.md"), b_acked).unwrap();
    fs::write(dir.path().join("mail/02-live.md"), b_live).unwrap();
    fs::write(dir.path().join("mail/03-missed.md"), b_missed).unwrap();
    fs::write(dir.path().join("mail/04-retired.md"), b_retired).unwrap();

    let sha_acked = sha256_of(b_acked);
    // ACK recorded under an alias declared on lucas's durable SeatRecord.
    let acks_before_raw = format!(
        "{{\"ts\":\"2026-11-21T05:00:00Z\",\"acked_by\":\"lucas-review\",\"letter\":\"mail/01-acked.md\",\"sha256\":\"{}\"}}\n",
        sha_acked.as_str()
    );
    let acks_path = dir.path().join("state/acks.jsonl");
    fs::write(&acks_path, &acks_before_raw).unwrap();
    let acks_mtime_before = fs::metadata(&acks_path).unwrap().modified().unwrap();
    let acks_bytes_before = fs::read(&acks_path).unwrap();

    let mut st = PresenceState::default();
    let mut seq = 0;
    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    // lucas = Active + Live (with alias `lucas-review`), dave = Active + Offline, quire = Retired.
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p.clone(),
        UnixMs(900),
        PresenceCommand::PresenceClaim(PresenceClaim {
            seat: SeatName::parse("lucas").unwrap(),
            status: Some(SeatStatus::Active),
            role: None,
            projects: Vec::new(),
            aliases: vec![SeatName::parse("lucas-review").unwrap()],
            home_room: None,
            mail_horizon_at: None,
            work_class: None,
            avatar: None,
            home_dir: None,
            start_cmd: None,
            resume_cmd: None,
            rex_target: None,
            verified_how: None,
            replace: None,
            idem: None,
        }),
        |_, _| true,
    )
    .unwrap();
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        Principal::Seat {
            seat: SeatName::parse("lucas").unwrap(),
        },
        UnixMs(1_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: SeatName::parse("lucas").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: TransportEndpoint::UdsSession {
                conn_id: 1,
                client: "lucas".into(),
            },
            liveness: LivenessMode::SocketLifetime,
            ttl_ms: None,
            takeover: false,
            allow_fork: false,
            mail_horizon_at: None,
            idem: None,
        }),
        |_, _| true,
    )
    .unwrap();
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p.clone(),
        UnixMs(1_000),
        PresenceCommand::PresenceClaim(PresenceClaim {
            seat: SeatName::parse("dave").unwrap(),
            status: Some(SeatStatus::Active),
            role: None,
            projects: Vec::new(),
            aliases: Vec::new(),
            home_room: None,
            mail_horizon_at: None,
            work_class: None,
            avatar: None,
            home_dir: None,
            start_cmd: None,
            resume_cmd: None,
            rex_target: None,
            verified_how: None,
            replace: None,
            idem: None,
        }),
        |_, _| true,
    )
    .unwrap();
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p,
        UnixMs(1_000),
        PresenceCommand::PresenceClaim(PresenceClaim {
            seat: SeatName::parse("quire").unwrap(),
            status: Some(SeatStatus::Retired),
            role: None,
            projects: Vec::new(),
            aliases: Vec::new(),
            home_room: None,
            mail_horizon_at: None,
            work_class: None,
            avatar: None,
            home_dir: None,
            start_cmd: None,
            resume_cmd: None,
            rex_target: None,
            verified_how: None,
            replace: None,
            idem: None,
        }),
        |_, _| true,
    )
    .unwrap();

    let aliases = AliasMap::from_presence(&st);
    let ro = ReadOnlyInterchange::open(dir.path()).unwrap();
    let pass = run_shadow_pass(
        &ro,
        &st,
        &affinity,
        &RetryState::new(),
        &policy,
        &aliases,
        UnixMs(5_000),
    )
    .unwrap();

    // Read-only invariant verified against the private snapshot directory.
    assert_eq!(
        fs::metadata(&acks_path).unwrap().modified().unwrap(),
        acks_mtime_before
    );
    assert_eq!(fs::read(&acks_path).unwrap(), acks_bytes_before);
    assert_eq!(
        fs::read_dir(dir.path().join("state"))
            .unwrap()
            .flatten()
            .count(),
        1
    );

    let acks_before = pass.acks_report.index();
    let snap_score = score_shadow_against_acks(&pass.decisions, &acks_before, &aliases);
    assert_eq!(snap_score.rings_emitted, 1);
    assert_eq!(snap_score.ring_referents_confirmed_acked, 0);
    assert_eq!(snap_score.ring_referents_pending_ack, 1);
    assert_eq!(snap_score.skipped_already_acked, 1);
    assert_eq!(snap_score.skipped_dormant_or_retired, 1);
    assert_eq!(snap_score.unrung_alerts, 1);
    assert_eq!(snap_score.unrung_referents_caught, 1);
    assert_eq!(snap_score.unrung_referents_already_acked, 0);

    // JSONL output serializes every decision cleanly for `--shadow-read-only` logs.
    let jsonl = pass.to_jsonl();
    let parsed_kinds: Vec<String> = jsonl
        .lines()
        .map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            v["decision"].as_str().unwrap().to_owned()
        })
        .collect();
    assert_eq!(parsed_kinds.len(), pass.decisions.len());
    assert!(parsed_kinds.iter().any(|k| k == "ring"));
    assert!(parsed_kinds.iter().any(|k| k == "raise_unrung"));
    assert_eq!(
        rex_wave_mailbridge::score_shadow_jsonl(&jsonl, &acks_before, &aliases).unwrap(),
        snap_score,
        "JSONL shadow scorer must agree identically with in-memory decision scorer"
    );

    // Opt-in read-only smoke test against a real interchange directory when
    // `REX_WAVE_LIVE_INTERCHANGE_TEST_DIR` is set in the environment.
    if let Some(live_root) = opt_in_interchange_dir() {
        let live_acks_path = live_root.join("state/acks.jsonl");
        if live_acks_path.is_file() {
            let live_aliases = AliasMap::empty();
            let ro_live = ReadOnlyInterchange::open(&live_root).unwrap();
            let live_pass = run_shadow_pass(
                &ro_live,
                &PresenceState::default(),
                &affinity,
                &RetryState::new(),
                &policy,
                &live_aliases,
                UnixMs(1_795_262_000_000),
            )
            .unwrap();
            let live_idx = live_pass.acks_report.index();
            let live_score =
                score_shadow_against_acks(&live_pass.decisions, &live_idx, &live_aliases);
            assert!(!live_pass.acks_report.records.is_empty());
            assert_eq!(
                live_score.unrung_referents_already_acked, 0,
                "shadow pass must never emit RaiseUnrung for an already-ACKed (seat, sha256)"
            );
            assert_eq!(
                live_score.ring_referents_confirmed_acked, 0,
                "shadow pass must never re-ring an already-ACKed (seat, sha256)"
            );
        }
    }
}

#[test]
fn pr20_timestamp_precedence_letter_id_ms_recorded_date_and_mtime_in_shadow_and_project() {
    let dir = TempDirGuard::new("pr20-ts-precedence");
    fs::write(dir.path().join(".rex-wave-sandbox"), b"ok\n").unwrap();
    fs::create_dir_all(dir.path().join("mail")).unwrap();
    fs::create_dir_all(dir.path().join("to-lucas")).unwrap();
    fs::create_dir_all(dir.path().join("state")).unwrap();
    fs::write(dir.path().join("state/acks.jsonl"), b"").unwrap();

    // 1. Filename 13-digit epoch ms wins over recorded date and newer file mtime.
    let f_epoch_name = dir.path().join("mail/1792431783570-carol-to-lucas.md");
    let b_epoch_name =
        b"---\nfrom: carol\nto: lucas\ndate: 2026-11-20 12:00 CST\n---\nepoch filename\n";
    fs::write(&f_epoch_name, b_epoch_name).unwrap();

    // 2. Frontmatter `id` / `date_ms` wins over recorded date and newer file mtime.
    let f_id_ms = dir.path().join("to-lucas/id-ms-letter.md");
    let b_id_ms = b"---\nfrom: carol\nto: lucas\nid: 1795284326392-carol-reply\ndate: 2026-11-21 12:10 CST\n---\nid ms\n";
    fs::write(&f_id_ms, b_id_ms).unwrap();

    // 3. Recorded frontmatter `date` (`YYYY-MM-DD HH:MM CST`) wins over newer file mtime.
    let f_recorded_fm = dir.path().join("to-lucas/recorded-fm.md");
    let b_recorded_fm =
        b"---\nfrom: carol\nto: lucas\ndate: 2026-10-17 00:36 CDT\n---\nrecorded fm\n";
    fs::write(&f_recorded_fm, b_recorded_fm).unwrap();

    // 4. `YYYY-MM-DD-` filename prefix wins over newer file mtime when no frontmatter date.
    let f_ymd_name = dir.path().join("to-lucas/2026-09-15-history-note.md");
    let b_ymd_name = b"---\nfrom: carol\nto: lucas\n---\nymd filename\n";
    fs::write(&f_ymd_name, b_ymd_name).unwrap();

    // 5. Fallback to filesystem mtime only when no ID or recorded date exists.
    let f_mtime = dir.path().join("to-lucas/bare-note.md");
    let b_mtime = b"---\nfrom: carol\nto: lucas\n---\nmtime fallback\n";
    fs::write(&f_mtime, b_mtime).unwrap();

    let ro = ReadOnlyInterchange::open(dir.path()).unwrap();
    let scanned = ro.scan_letters(&AliasMap::empty()).unwrap();
    assert_eq!(scanned.len(), 5);
    let by_path: std::collections::BTreeMap<&str, _> =
        scanned.iter().map(|l| (l.rel_path.as_str(), l)).collect();

    assert_eq!(
        by_path["mail/1792431783570-carol-to-lucas.md"].seen_at,
        UnixMs(1_792_431_783_570)
    );
    assert_eq!(
        by_path["mail/1792431783570-carol-to-lucas.md"].time_basis,
        TimeBasis::LetterIdMs
    );

    assert_eq!(
        by_path["to-lucas/id-ms-letter.md"].seen_at,
        UnixMs(1_795_284_326_392)
    );
    assert_eq!(
        by_path["to-lucas/id-ms-letter.md"].time_basis,
        TimeBasis::LetterIdMs
    );

    // 2026-10-17 00:36 CDT == 2026-10-17T05:36:00Z
    let (expected_cdt_ms, expected_cdt_basis) = extract_letter_timestamp(
        "to-lucas/recorded-fm.md",
        None,
        None,
        Some("2026-10-17 00:36 CDT"),
        None,
        Some(UnixMs(1_799_999_999_999)),
    );
    assert_eq!(expected_cdt_basis, TimeBasis::RecordedDate);
    assert_eq!(by_path["to-lucas/recorded-fm.md"].seen_at, expected_cdt_ms);
    assert_eq!(
        by_path["to-lucas/recorded-fm.md"].time_basis,
        TimeBasis::RecordedDate
    );

    assert_eq!(
        by_path["to-lucas/2026-09-15-history-note.md"].time_basis,
        TimeBasis::RecordedDate
    );
    assert!(
        by_path["to-lucas/2026-09-15-history-note.md"].seen_at < expected_cdt_ms,
        "2026-09-15 filename date must sort older than 2026-10-17"
    );

    assert_eq!(
        by_path["to-lucas/bare-note.md"].time_basis,
        TimeBasis::FileModified
    );
    assert!(by_path["to-lucas/bare-note.md"].seen_at.0 > 1_700_000_000_000);

    // Shadow RaiseUnrung picks the historical oldest_at (2026-09-15), NOT today's mtime!
    let pass = run_shadow_pass(
        &ro,
        &PresenceState::default(),
        &IncarnationAffinity::new(),
        &RetryState::new(),
        &RingPolicy::default(),
        &AliasMap::empty(),
        UnixMs(1_795_300_000_000),
    )
    .unwrap();
    let unrung = pass
        .decisions
        .iter()
        .find(|d| matches!(d, RingDecision::RaiseUnrung { seat, .. } if seat.as_str() == "lucas"))
        .expect("lucas RaiseUnrung decision");
    match unrung {
        RingDecision::RaiseUnrung {
            oldest_at,
            time_basis,
            ..
        } => {
            assert_eq!(
                *oldest_at,
                by_path["to-lucas/2026-09-15-history-note.md"].seen_at
            );
            assert_eq!(*time_basis, TimeBasis::RecordedDate);
        }
        _ => unreachable!(),
    }
    let json = unrung.to_json_value();
    assert_eq!(json["time_basis"], "recorded_date");

    // Verify `lib.rs::project()` uses the exact same precedence!
    let sandbox = SandboxRoot::verify_with(dir.path(), &Protection::default()).unwrap();
    let item_epoch = project(
        &sandbox,
        &sandbox.path().join("to-lucas/id-ms-letter.md"),
        b_id_ms,
    )
    .unwrap();
    assert_eq!(item_epoch.ts, "2026-11-21T18:05:26Z");
    assert_eq!(item_epoch.ts_basis, "letter_id_ms");

    let item_recorded = project(
        &sandbox,
        &sandbox.path().join("to-lucas/recorded-fm.md"),
        b_recorded_fm,
    )
    .unwrap();
    assert_eq!(item_recorded.ts, "2026-10-17T05:36:00Z");
    assert_eq!(item_recorded.ts_basis, "recorded");

    let item_ymd = project(
        &sandbox,
        &sandbox.path().join("to-lucas/2026-09-15-history-note.md"),
        b_ymd_name,
    )
    .unwrap();
    assert_eq!(item_ymd.ts, "2026-09-15T00:00:00Z");
    assert_eq!(item_ymd.ts_basis, "recorded");

    let item_mtime = project(
        &sandbox,
        &sandbox.path().join("to-lucas/bare-note.md"),
        b_mtime,
    )
    .unwrap();
    assert_eq!(item_mtime.ts_basis, "mtime");
}

#[test]
fn pr20_q1_seed_records_from_interchange_covers_roster_routes_and_canonical_aliases() {
    let dir = TempDirGuard::new("pr20-q1-seed");
    let interchange = dir.path().join("interchange");
    fs::create_dir_all(interchange.join("postmaster/state")).unwrap();
    fs::create_dir_all(dir.path().join(".watchers")).unwrap();

    fs::write(
        interchange.join("postmaster/state/roster.json"),
        serde_json::to_vec(&serde_json::json!({
            "schema": "house-roster/2",
            "seats": {
                "rd-1": { "name": "carol", "box": "/tmp/interchange/to-carol" },
                "rd-2": { "name": "operator", "box": "/tmp/interchange/to-operator" },
                "rd-3": { "name": "heidi", "box": "/tmp/interchange/to-maya" },
                "rd-4": { "name": "dave", "box": "/tmp/interchange/to-dave" }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    fs::write(
        dir.path().join(".watchers/mailwatch-routes.json"),
        serde_json::to_vec(&serde_json::json!({
            "carol": { "aliases": ["carol", "carol_old", "Carol-legacy", "carol desk"] },
            "dave": { "aliases": ["dave", "dave_old", "users-c7"] },
            "heidi": { "aliases": ["maya", "heidi", "heidi-old"] },
            "cc": { "aliases": ["cc"] }
        }))
        .unwrap(),
    )
    .unwrap();

    let plan = seed_records_from_interchange_with_profile(&interchange, &test_house()).unwrap();
    assert_eq!(plan.sources_read.len(), 2);
    assert!(plan.skipped_targets.contains(&"operator".to_owned()));
    assert!(plan.skipped_targets.contains(&"cc".to_owned()));

    // Apply all seeded claims as Operator, then bind `carol` WITHOUT passing `--alias`.
    let mut st = PresenceState::default();
    let mut seq = 0;
    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    for claim in plan.claims {
        assert_ne!(claim.seat.as_str(), "operator");
        assert!(matches!(
            claim.verified_how,
            Some(VerificationKind::MachineChecked | VerificationKind::WitnessedBy { .. })
        ));
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            owner_p.clone(),
            UnixMs(1_000),
            PresenceCommand::PresenceClaim(claim),
            |_, _| true,
        )
        .unwrap();
    }

    // Binding as `carol` immediately covers `carol_old` via durable seeded aliases!
    let carol = SeatName::parse("carol").unwrap();
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        Principal::Seat {
            seat: carol.clone(),
        },
        UnixMs(2_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: carol.clone(),
            incarnation_id: None,
            lane: None,
            endpoint: TransportEndpoint::UdsSession {
                conn_id: 5,
                client: "carol-hook".into(),
            },
            liveness: LivenessMode::SocketLifetime,
            ttl_ms: None,
            takeover: false,
            allow_fork: false,
            mail_horizon_at: None,
            idem: None,
        }),
        |_, _| true,
    )
    .unwrap();

    let aliases = AliasMap::from_presence(&st);
    assert_eq!(aliases.canonicalize("carol_old"), "carol");
    assert_eq!(aliases.canonicalize("dave_old"), "dave");
    assert_eq!(aliases.canonicalize("heidi-old"), "heidi");
    assert_eq!(aliases.canonicalize("heidi-typo"), "heidi");
    assert_eq!(aliases.canonicalize("maya"), "heidi");

    let resolved = st
        .resolve_at(
            &PresenceResolve {
                query: "carol_old".into(),
                lane: None,
            },
            UnixMs(2_500),
        )
        .unwrap();
    assert_eq!(resolved.record.seat, carol);
    assert_eq!(resolved.live_bindings.len(), 1);
}

#[test]
fn pr20_q2_first_bind_mail_horizon_skips_pre_horizon_history_and_allows_override() {
    let mut st = PresenceState::default();
    let mut seq = 0;
    let carol = SeatName::parse("carol").unwrap();
    let carol_p = Principal::Seat {
        seat: carol.clone(),
    };
    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };

    // Seed `carol` first (mail_horizon_at starts None).
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p,
        UnixMs(1_000),
        PresenceCommand::PresenceClaim(PresenceClaim {
            status: Some(SeatStatus::Active),
            aliases: vec![SeatName::parse("carol_old").unwrap()],
            ..empty_claim("carol")
        }),
        |_, _| true,
    )
    .unwrap();
    assert_eq!(st.seat(&carol).unwrap().mail_horizon_at, None);

    // First live bind at t=10_000 stamps mail_horizon_at = Some(10_000).
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        carol_p.clone(),
        UnixMs(10_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: carol.clone(),
            incarnation_id: Some(IncarnationId::parse("inc-1").unwrap()),
            lane: None,
            endpoint: TransportEndpoint::UdsSession {
                conn_id: 1,
                client: "carol".into(),
            },
            liveness: LivenessMode::SocketLifetime,
            ttl_ms: None,
            takeover: false,
            allow_fork: false,
            mail_horizon_at: None,
            idem: None,
        }),
        |_, _| true,
    )
    .unwrap();
    assert_eq!(
        st.seat(&carol).unwrap().mail_horizon_at,
        Some(UnixMs(10_000))
    );

    // Subsequent rebind at t=20_000 WITHOUT override preserves the original horizon (10_000).
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        carol_p.clone(),
        UnixMs(20_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: carol.clone(),
            incarnation_id: Some(IncarnationId::parse("inc-2").unwrap()),
            lane: None,
            endpoint: TransportEndpoint::UdsSession {
                conn_id: 2,
                client: "carol".into(),
            },
            liveness: LivenessMode::SocketLifetime,
            ttl_ms: None,
            takeover: true,
            allow_fork: false,
            mail_horizon_at: None,
            idem: None,
        }),
        |_, _| true,
    )
    .unwrap();
    assert_eq!(
        st.seat(&carol).unwrap().mail_horizon_at,
        Some(UnixMs(10_000))
    );

    let aliases = AliasMap::from_presence(&st);
    let empty_acks = parse_acks_jsonl("", &aliases).into_index();
    let old_letter = sample_candidate("mail/old-hist.md", "carol", b"old", UnixMs(9_999));
    let exact_letter = sample_candidate("mail/at-horizon.md", "carol", b"at", UnixMs(10_000));
    let new_letter = sample_candidate("mail/new-live.md", "carol", b"new", UnixMs(15_000));

    let decisions = decide_mail_rings_for_seat(
        &carol,
        &[old_letter, exact_letter, new_letter],
        &empty_acks,
        &st,
        &IncarnationAffinity::new(),
        &RetryState::new(),
        &RingPolicy::default(),
        &aliases,
        UnixMs(20_500),
    );
    assert!(matches!(
        &decisions[0],
        ShadowAction::SkipPreHorizonHistory {
            letter_at: UnixMs(9_999),
            horizon_at: UnixMs(10_000),
            ..
        }
    ));
    let score = score_shadow_against_acks(&decisions, &empty_acks, &aliases);
    assert_eq!(score.skipped_pre_horizon, 1);
    assert_eq!(score.rings_emitted, 2);
    assert_eq!(score.unrung_alerts, 0);

    // Even when carol goes offline later, pre-horizon letters NEVER raise RaiseUnrung!
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        carol_p.clone(),
        UnixMs(25_000),
        PresenceCommand::PresenceUnbind(PresenceUnbind {
            seat: carol.clone(),
            incarnation_id: IncarnationId::parse("inc-2").unwrap(),
            reason: Some("closed".into()),
        }),
        |_, _| true,
    )
    .unwrap();
    let offline_old = decide_mail_ring(
        &sample_candidate("mail/hist-480.md", "carol", b"hist-480", UnixMs(5_000)),
        &empty_acks,
        &st,
        &IncarnationAffinity::new(),
        &RetryState::new(),
        &RingPolicy::default(),
        &aliases,
        UnixMs(60_000),
    );
    assert!(matches!(
        offline_old,
        ShadowAction::SkipPreHorizonHistory { .. }
    ));

    // Explicit `--mail-horizon` override moves the watermark cleanly.
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        carol_p,
        UnixMs(30_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: carol.clone(),
            incarnation_id: Some(IncarnationId::parse("inc-3").unwrap()),
            lane: None,
            endpoint: TransportEndpoint::UdsSession {
                conn_id: 3,
                client: "carol".into(),
            },
            liveness: LivenessMode::SocketLifetime,
            ttl_ms: None,
            takeover: true,
            allow_fork: false,
            mail_horizon_at: Some(UnixMs(25_000)),
            idem: None,
        }),
        |_, _| true,
    )
    .unwrap();
    assert_eq!(
        st.seat(&carol).unwrap().mail_horizon_at,
        Some(UnixMs(25_000))
    );
}

#[test]
fn pr20_q3_operator_structural_guard_and_non_seat_pseudo_target_classification() {
    let mut st = PresenceState::default();
    let mut seq = 0;
    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    let operator = SeatName::parse("operator").unwrap();

    // 1. PresenceBind for `operator` is structurally DENIED (even for Operator!).
    let bind_err = apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p.clone(),
        UnixMs(1_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: operator.clone(),
            incarnation_id: None,
            lane: None,
            endpoint: TransportEndpoint::OperatorToast {
                command: "mail-operator.sh".into(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(60_000),
            takeover: false,
            allow_fork: false,
            mail_horizon_at: None,
            idem: None,
        }),
        |_, _| true,
    )
    .unwrap_err();
    assert!(matches!(bind_err, ApiError::Denied { .. }), "{bind_err:?}");

    // 2. PresenceClaim setting `operator` to Active (explicit or default) is DENIED.
    let claim_active_err = apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p.clone(),
        UnixMs(1_000),
        PresenceCommand::PresenceClaim(empty_claim("operator")),
        |_, _| true,
    )
    .unwrap_err();
    assert!(
        matches!(claim_active_err, ApiError::Denied { .. }),
        "{claim_active_err:?}"
    );

    // 3. Claiming `operator` as an alias of another seat is DENIED.
    let alias_hijack_err = apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p.clone(),
        UnixMs(1_000),
        PresenceCommand::PresenceClaim(PresenceClaim {
            aliases: vec![operator.clone()],
            ..empty_claim("carol")
        }),
        |_, _| true,
    )
    .unwrap_err();
    assert!(
        matches!(alias_hijack_err, ApiError::Denied { .. }),
        "{alias_hijack_err:?}"
    );

    // 4. Dormant metadata on `operator` is allowed, but transitioning it to Active is DENIED.
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p.clone(),
        UnixMs(1_000),
        PresenceCommand::PresenceClaim(PresenceClaim {
            status: Some(SeatStatus::Dormant),
            ..empty_claim("operator")
        }),
        |_, _| true,
    )
    .unwrap();
    let promote_err = apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p,
        UnixMs(2_000),
        PresenceCommand::PresenceSetStatus(PresenceSetStatus {
            seat: operator.clone(),
            status: SeatStatus::Active,
            verified_how: None,
            reason: None,
        }),
        |_, _| true,
    )
    .unwrap_err();
    assert!(
        matches!(promote_err, ApiError::Denied { .. }),
        "{promote_err:?}"
    );

    // 5. Shadow classification: `operator` -> SkipOperatorSurface; non-seat targets -> SkipNonSeatTarget.
    let aliases = AliasMap::empty();
    let empty_acks = parse_acks_jsonl("", &aliases).into_index();
    let empty_st = PresenceState::default();
    let mut decisions = Vec::new();
    decisions.push(decide_mail_ring(
        &sample_candidate(
            "mail/to-operator.md",
            "operator",
            b"hi operator",
            UnixMs(1_000),
        ),
        &empty_acks,
        &empty_st,
        &IncarnationAffinity::new(),
        &RetryState::new(),
        &RingPolicy::default(),
        &aliases,
        UnixMs(2_000),
    ));
    for pseudo in [
        "cc",
        "to-cc",
        "all-seats",
        "desk",
        "desktop",
        "codex",
        "claude",
        "unregistered-addressee",
    ] {
        decisions.push(decide_mail_ring(
            &sample_candidate(
                &format!("mail/to-{pseudo}.md"),
                pseudo,
                format!("hi {pseudo}").as_bytes(),
                UnixMs(1_000),
            ),
            &empty_acks,
            &empty_st,
            &IncarnationAffinity::new(),
            &RetryState::new(),
            &RingPolicy::default(),
            &aliases,
            UnixMs(2_000),
        ));
    }

    assert!(matches!(
        decisions[0],
        ShadowAction::SkipOperatorSurface { .. }
    ));
    assert!(
        decisions[1..]
            .iter()
            .all(|d| matches!(d, ShadowAction::SkipNonSeatTarget { .. }))
    );

    let report = score_shadow_against_acks(&decisions, &empty_acks, &aliases);
    assert_eq!(report.skipped_operator_surface, 1);
    assert_eq!(report.skipped_non_seat_target, 8);
    assert_eq!(report.unrung_alerts, 0);
    assert_eq!(report.rings_emitted, 0);

    // JSONL round-trip agrees identically with in-memory score.
    let jsonl: String = decisions
        .iter()
        .map(|d| format!("{}\n", serde_json::to_string(&d.to_json_value()).unwrap()))
        .collect();
    assert_eq!(
        score_shadow_jsonl(&jsonl, &empty_acks, &aliases).unwrap(),
        report
    );
}

#[test]
fn pr20_resident_manifest_fields_claim_preserve_and_round_trip() {
    let mut st = PresenceState::default();
    let mut seq = 0;
    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    let carol = SeatName::parse("carol").unwrap();

    apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p.clone(),
        UnixMs(1_000),
        PresenceCommand::PresenceClaim(PresenceClaim {
            status: Some(SeatStatus::Active),
            role: Some("estate-desk".into()),
            projects: vec!["interchange".into()],
            aliases: vec![SeatName::parse("carol_old").unwrap()],
            mail_horizon_at: Some(UnixMs(1_795_284_326_000)),
            work_class: Some(WorkClass::ClassA),
            avatar: Some("avatars/carol.png".into()),
            home_dir: Some("/workspace/seats/carol".into()),
            start_cmd: Some("claude --name Carol-5.2".into()),
            resume_cmd: Some("claude --resume 59e9053e".into()),
            rex_target: Some("block:carol-desk".into()),
            ..empty_claim("carol")
        }),
        |_, _| true,
    )
    .unwrap();

    // Updating only `work_class` preserves all other resident manifest fields.
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p,
        UnixMs(2_000),
        PresenceCommand::PresenceClaim(PresenceClaim {
            work_class: Some(WorkClass::ClassB),
            ..empty_claim("carol")
        }),
        |_, _| true,
    )
    .unwrap();

    let rec = st.seat(&carol).unwrap();
    assert_eq!(rec.work_class, Some(WorkClass::ClassB));
    assert_eq!(rec.avatar.as_deref(), Some("avatars/carol.png"));
    assert_eq!(rec.home_dir.as_deref(), Some("/workspace/seats/carol"));
    assert_eq!(rec.start_cmd.as_deref(), Some("claude --name Carol-5.2"));
    assert_eq!(rec.resume_cmd.as_deref(), Some("claude --resume 59e9053e"));
    assert_eq!(rec.rex_target.as_deref(), Some("block:carol-desk"));
    assert_eq!(rec.mail_horizon_at, Some(UnixMs(1_795_284_326_000)));

    let manifest = ResidentManifest::from_records([rec]);
    assert_eq!(manifest.schema, RESIDENT_MANIFEST_SCHEMA);
    let json = serde_json::to_string_pretty(&manifest).unwrap();
    let parsed: ResidentManifest = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, manifest);
    assert_eq!(parsed.seats[0].work_class, Some(WorkClass::ClassB));

    // Omitting `status` in a hand-edited manifest preserves an existing Dormant status.
    let hand_edited: ResidentManifest = serde_json::from_value(serde_json::json!({
        "schema": RESIDENT_MANIFEST_SCHEMA,
        "seats": [{
            "seat": "nodal",
            "work_class": "class_b",
            "rex_target": "block:nodal-card"
        }]
    }))
    .unwrap();
    assert_eq!(hand_edited.seats[0].status, None);
    let mut st2 = PresenceState::default();
    let mut seq2 = 0;
    let owner2 = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    apply_presence_with_probe(
        &mut st2,
        &mut seq2,
        owner2.clone(),
        UnixMs(1_000),
        PresenceCommand::PresenceClaim(PresenceClaim {
            status: Some(SeatStatus::Dormant),
            ..empty_claim("nodal")
        }),
        |_, _| true,
    )
    .unwrap();
    apply_presence_with_probe(
        &mut st2,
        &mut seq2,
        owner2,
        UnixMs(2_000),
        PresenceCommand::PresenceClaim(hand_edited.seats[0].clone().into_claim(None)),
        |_, _| true,
    )
    .unwrap();
    let nodal = st2.seat(&SeatName::parse("nodal").unwrap()).unwrap();
    assert_eq!(nodal.status, SeatStatus::Dormant);
    assert_eq!(nodal.work_class, Some(WorkClass::ClassB));
}

#[test]
fn pr20_reply_prefix_and_date_only_letters_are_never_falsely_dropped_by_horizon() {
    let dir = TempDirGuard::new("pr20-reply-and-date-only");
    fs::write(dir.path().join(".rex-wave-sandbox"), b"ok\n").unwrap();
    fs::create_dir_all(dir.path().join("mail")).unwrap();
    fs::create_dir_all(dir.path().join("to-frank")).unwrap();
    fs::create_dir_all(dir.path().join("state")).unwrap();
    fs::write(dir.path().join("state/acks.jsonl"), b"").unwrap();

    // Parent was written at 2026-11-20T22:56:41Z (`1795215401193`).
    // Seat `frank` binds at 2026-11-21 01:00 CST (`2026-11-21T07:00:00Z` = `1795244400000`).
    // Carol replies at 2026-11-21 02:51 CST (`2026-11-21T08:51:00Z` = `1795251060000`) reusing
    // the parent's 13-digit prefix in filename and `id`, with `in_reply_to: 1795215401193`.
    let reply_bytes = b"---\nfrom: carol\nto: frank\nid: 1795215401193-carol-reply\nin_reply_to: 1795215401193\ndate: 2026-11-21 02:51 CST\nsubject: Post-bind reply\n---\nreply body\n";
    let reply_path = dir.path().join("mail/1795215401193-carol-reply.md");
    fs::write(&reply_path, reply_bytes).unwrap();

    // Date-only box letter written on the same local day (`2026-11-21`, resolves to 00:00 UTC
    // with `date_only = true`) AND a fuzzy-minute letter (`2026-11-21 11:1x CST`) must NOT be
    // skipped by a 2026-11-21 bind horizon, whereas an older historical date-only letter
    // (`2026-10-18`) MUST be skipped as `SkipPreHorizonHistory`.
    fs::write(
        dir.path().join("to-frank/2026-11-21-same-day.md"),
        b"---\nfrom: carol\nto: frank\ndate: 2026-11-21\n---\nsame-day date-only\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("to-frank/fuzzy-same-day.md"),
        b"---\nfrom: carol\nto: frank\ndate: 2026-11-21 11:1x CST\n---\nfuzzy same-day\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("to-frank/2026-10-18-old-history.md"),
        b"---\nfrom: carol\nto: frank\ndate: 2026-10-18\n---\nold history\n",
    )
    .unwrap();

    // Verify `lib.rs::project()` also uses the reply's own recorded date rather than the parent's id.
    let sandbox = SandboxRoot::verify_with(dir.path(), &Protection::default()).unwrap();
    let proj_reply = project(
        &sandbox,
        &sandbox.path().join("mail/1795215401193-carol-reply.md"),
        reply_bytes,
    )
    .unwrap();
    assert_eq!(proj_reply.ts, "2026-11-21T08:51:00Z");
    assert_eq!(proj_reply.ts_basis, "recorded");

    let mut st = PresenceState::default();
    let mut seq = 0;
    let frank = SeatName::parse("frank").unwrap();
    let bind_horizon = UnixMs(1_795_244_400_000); // 2026-11-21T07:00:00Z (01:00 CST)
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        Principal::Seat {
            seat: frank.clone(),
        },
        bind_horizon,
        PresenceCommand::PresenceBind(PresenceBind {
            seat: frank.clone(),
            incarnation_id: Some(IncarnationId::parse("inc-frank-1").unwrap()),
            lane: None,
            endpoint: TransportEndpoint::UdsSession {
                conn_id: 9,
                client: "frank".into(),
            },
            liveness: LivenessMode::SocketLifetime,
            ttl_ms: None,
            takeover: false,
            allow_fork: false,
            mail_horizon_at: None,
            idem: None,
        }),
        |_, _| true,
    )
    .unwrap();

    let ro = ReadOnlyInterchange::open(dir.path()).unwrap();
    let aliases = AliasMap::from_presence(&st);
    let pass = run_shadow_pass(
        &ro,
        &st,
        &IncarnationAffinity::new(),
        &RetryState::new(),
        &RingPolicy::default(),
        &aliases,
        UnixMs(1_795_260_000_000),
    )
    .unwrap();

    let mut rung_paths = BTreeSet::new();
    let mut skipped_pre_horizon_paths = BTreeSet::new();
    for d in &pass.decisions {
        match d {
            RingDecision::Ring { referent, .. } => {
                rung_paths.insert(referent.path().to_owned());
            }
            RingDecision::SkipPreHorizonHistory { referent, .. } => {
                skipped_pre_horizon_paths.insert(referent.path().to_owned());
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }
    assert!(
        rung_paths.contains("mail/1795215401193-carol-reply.md"),
        "post-bind reply reusing parent prefix must RING, got rung={rung_paths:?}, skipped={skipped_pre_horizon_paths:?}"
    );
    assert!(
        rung_paths.contains("to-frank/2026-11-21-same-day.md"),
        "same-day date-only letter must RING, got rung={rung_paths:?}, skipped={skipped_pre_horizon_paths:?}"
    );
    assert!(
        rung_paths.contains("to-frank/fuzzy-same-day.md"),
        "same-day fuzzy-time letter must RING, got rung={rung_paths:?}, skipped={skipped_pre_horizon_paths:?}"
    );
    assert_eq!(
        skipped_pre_horizon_paths,
        BTreeSet::from(["to-frank/2026-10-18-old-history.md".to_owned()])
    );
}

#[test]
fn pr21_start_and_resume_cmd_are_operator_only() {
    let mut st = PresenceState::default();
    let mut seq = 0;
    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    let carol_seat = SeatName::parse("carol").unwrap();
    let carol_p = Principal::Seat {
        seat: carol_seat.clone(),
    };
    let worker_p = Principal::Worker {
        wave: WaveSlug::parse("w1").unwrap(),
        role: RoleName::parse("coder").unwrap(),
    };
    let sys_p = Principal::System {
        part: SystemPart::MailBridge,
    };

    // 1. Non-operator principals cannot set `start_cmd` or `resume_cmd` on an unclaimed seat
    //    (including a seat claiming its own record).
    for caller in [carol_p.clone(), worker_p.clone(), sys_p.clone()] {
        let err_start = apply_presence_with_probe(
            &mut st,
            &mut seq,
            caller.clone(),
            UnixMs(1_000),
            PresenceCommand::PresenceClaim(PresenceClaim {
                start_cmd: Some("claude --name Carol".into()),
                ..empty_claim("carol")
            }),
            |_, _| true,
        )
        .unwrap_err();
        assert!(
            matches!(err_start, ApiError::Denied { .. }),
            "caller {caller} setting start_cmd must be Denied, got {err_start:?}"
        );

        let err_resume = apply_presence_with_probe(
            &mut st,
            &mut seq,
            caller.clone(),
            UnixMs(1_001),
            PresenceCommand::PresenceClaim(PresenceClaim {
                resume_cmd: Some("claude --resume abc".into()),
                ..empty_claim("carol")
            }),
            |_, _| true,
        )
        .unwrap_err();
        assert!(
            matches!(err_resume, ApiError::Denied { .. }),
            "caller {caller} setting resume_cmd must be Denied, got {err_resume:?}"
        );
    }
    assert!(
        st.seat(&carol_seat).is_none(),
        "denied claims must not create seat record"
    );

    // 2. Operator succeeds setting both `start_cmd` and `resume_cmd`.
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p.clone(),
        UnixMs(2_000),
        PresenceCommand::PresenceClaim(PresenceClaim {
            status: Some(SeatStatus::Active),
            start_cmd: Some("claude --name Carol".into()),
            resume_cmd: Some("claude --resume orig".into()),
            ..empty_claim("carol")
        }),
        |_, _| true,
    )
    .unwrap();
    let rec = st.seat(&carol_seat).unwrap();
    assert_eq!(rec.start_cmd.as_deref(), Some("claude --name Carol"));
    assert_eq!(rec.resume_cmd.as_deref(), Some("claude --resume orig"));

    // 3. Seat updating its own non-exec metadata succeeds and preserves operator-set commands.
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        carol_p.clone(),
        UnixMs(2_500),
        PresenceCommand::PresenceClaim(PresenceClaim {
            role: Some("estate-desk".into()),
            ..empty_claim("carol")
        }),
        |_, _| true,
    )
    .unwrap();
    let rec = st.seat(&carol_seat).unwrap();
    assert_eq!(
        rec.role.as_ref().map(|r| r.value.as_str()),
        Some("estate-desk")
    );
    assert_eq!(rec.start_cmd.as_deref(), Some("claude --name Carol"));
    assert_eq!(rec.resume_cmd.as_deref(), Some("claude --resume orig"));

    // 4. Non-operator principals cannot overwrite `start_cmd` or `resume_cmd` on an existing seat
    //    (even a Worker gets Denied rather than Conflict).
    for caller in [carol_p.clone(), worker_p.clone(), sys_p.clone()] {
        let err = apply_presence_with_probe(
            &mut st,
            &mut seq,
            caller.clone(),
            UnixMs(3_000),
            PresenceCommand::PresenceClaim(PresenceClaim {
                start_cmd: Some("sh -c pwn".into()),
                ..empty_claim("carol")
            }),
            |_, _| true,
        )
        .unwrap_err();
        assert!(
            matches!(err, ApiError::Denied { .. }),
            "caller {caller} overwriting start_cmd on existing seat must be Denied, got {err:?}"
        );
    }

    // 5. Non-operator `replacing_self` cannot clear existing `start_cmd`/`resume_cmd`.
    for caller in [carol_p.clone(), worker_p.clone(), sys_p.clone()] {
        let err = apply_presence_with_probe(
            &mut st,
            &mut seq,
            caller.clone(),
            UnixMs(3_500),
            PresenceCommand::PresenceClaim(PresenceClaim {
                replace: Some(carol_seat.clone()),
                ..empty_claim("carol")
            }),
            |_, _| true,
        )
        .unwrap_err();
        assert!(
            matches!(err, ApiError::Denied { .. }),
            "caller {caller} clearing start_cmd/resume_cmd via replace must be Denied, got {err:?}"
        );
    }
    let rec = st.seat(&carol_seat).unwrap();
    assert_eq!(rec.start_cmd.as_deref(), Some("claude --name Carol"));
    assert_eq!(rec.resume_cmd.as_deref(), Some("claude --resume orig"));

    // 6. A seat without `start_cmd`/`resume_cmd` may still self-replace its own metadata.
    let nodal_seat = SeatName::parse("nodal").unwrap();
    let nodal_p = Principal::Seat {
        seat: nodal_seat.clone(),
    };
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        nodal_p.clone(),
        UnixMs(3_600),
        PresenceCommand::PresenceClaim(PresenceClaim {
            role: Some("old-role".into()),
            ..empty_claim("nodal")
        }),
        |_, _| true,
    )
    .unwrap();
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        nodal_p,
        UnixMs(3_700),
        PresenceCommand::PresenceClaim(PresenceClaim {
            role: Some("new-role".into()),
            replace: Some(nodal_seat.clone()),
            ..empty_claim("nodal")
        }),
        |_, _| true,
    )
    .unwrap();
    assert_eq!(
        st.seat(&nodal_seat)
            .unwrap()
            .role
            .as_ref()
            .map(|r| r.value.as_str()),
        Some("new-role")
    );

    // 7. Operator can clear `start_cmd`/`resume_cmd` via explicit `replace`.
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p,
        UnixMs(4_000),
        PresenceCommand::PresenceClaim(PresenceClaim {
            replace: Some(carol_seat.clone()),
            ..empty_claim("carol")
        }),
        |_, _| true,
    )
    .unwrap();
    let rec = st.seat(&carol_seat).unwrap();
    assert_eq!(rec.start_cmd, None);
    assert_eq!(rec.resume_cmd, None);
}

#[test]
fn pr22_seed_records_ingests_all_doorbells_ledger_and_seats_tsv() {
    let dir = TempDirGuard::new("pr22-seed-all-sources");
    let house = dir.path();
    let interchange = house.join("interchange");
    fs::create_dir_all(interchange.join("postmaster/state")).unwrap();
    fs::create_dir_all(interchange.join("state")).unwrap();
    fs::create_dir_all(house.join(".watchers")).unwrap();

    fs::write(
        interchange.join("postmaster/state/roster.json"),
        serde_json::to_vec(&serde_json::json!({
            "schema": "house-roster/2",
            "seats": {
                "rd-1": { "name": "carol", "box": "/tmp/interchange/to-carol" },
                "rd-2": { "name": "operator", "box": "/tmp/interchange/to-operator" }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    fs::write(
        house.join(".watchers/mailwatch-routes.json"),
        serde_json::to_vec(&serde_json::json!({
            "eve": { "own_knocker": true, "aliases": ["eve"] },
            "desktop": { "aliases": ["desktop"] }
        }))
        .unwrap(),
    )
    .unwrap();
    fs::write(
        interchange.join("state/bb-knock-routes.tsv"),
        "# comment\nseat\tbb_thread_id\tnote\nfrank\tthr_bns4xpcfs9\tEmacs-FV review\n",
    )
    .unwrap();
    fs::write(
        interchange.join("state/codex-knock-routes.tsv"),
        "seat\thostId\tthreadId\tnote\nastra-emacs-fv\tlocal\t01a1b9c9\tEmacs Verify\n",
    )
    .unwrap();
    fs::write(
        house.join("herdr-registry.tsv"),
        concat!(
            "# seat\ttarget\tverified_date\tverified_how\n",
            "seat\ttarget\tdate\thow\n",
            "liam\trex:d5b20f/69f276\t2026-11-16\tself\n",
            "peggy\tw3:pN\t2026-10-13\tpane\n",
            "walter\tw4:p27\t2026-10-13\tpane\n",
            "sol_ext\tw5:p2\t2026-10-20\tpane\n",
            "excluded-seat\tw9:p1S\t2026-10-13\tPERMANENTLY EXCLUDED\n",
        ),
    )
    .unwrap();
    fs::write(
        interchange.join("state/ledger.md"),
        concat!(
            "# Interchange ledger\n",
            "2026-08-15 | CLAIM | mallory | local model\n",
            "2026-08-15 | CLAIM | Parallax | web seat\n",
            "2026-09-03 | CLAIM | liam | already machine-checked via herdr-registry\n",
            "2026-09-16 | CLAIM | to-cc/carol-introduction-r1.md | legacy file-path row\n",
            "2026-09-17 | ACK | to-cc/foo.md | not a claim\n",
        ),
    )
    .unwrap();

    let seats_tsv_path = house.join("seats.tsv");
    fs::write(
        &seats_tsv_path,
        concat!(
            "# external seats map\n",
            "ML-Papers\tliam\t/tmp/liam\n",
            "Pasted-text.txt attachment (fork) (fork)\tfrank\t/tmp/frank\n",
            "frank-fork\tfrank\t/tmp/frank\n",
            "Astra-Outside-Review\tfrank\t/tmp/frank\n",
            "cleanup-Color_Tool_K-Means\tchroma\t/tmp/chroma\n",
            "operator-session\toperator\t/tmp/operator\n",
        ),
    )
    .unwrap();

    // Base `seed_records_from_interchange` reads all 6 house files hermetically.
    let base_plan =
        seed_records_from_interchange_with_profile(&interchange, &test_house()).unwrap();
    assert_eq!(base_plan.sources_read.len(), 6);
    assert!(base_plan.skipped_targets.contains(&"operator".to_owned()));
    assert!(base_plan.skipped_targets.contains(&"desktop".to_owned()));

    let by_seat = |plan: &rex_wave_mailbridge::SeedInterchangePlan, name: &str| {
        plan.claims
            .iter()
            .find(|c| c.seat.as_str() == name)
            .cloned()
    };

    assert_eq!(
        by_seat(&base_plan, "frank").unwrap().verified_how,
        Some(VerificationKind::MachineChecked)
    );
    assert_eq!(
        by_seat(&base_plan, "astra-emacs-fv").unwrap().verified_how,
        Some(VerificationKind::MachineChecked)
    );
    assert_eq!(
        by_seat(&base_plan, "liam").unwrap().verified_how,
        Some(VerificationKind::MachineChecked),
        "liam is in herdr-registry.tsv so MachineChecked beats ledger.md WitnessedBy"
    );
    assert_eq!(
        by_seat(&base_plan, "peggy").unwrap().verified_how,
        Some(VerificationKind::MachineChecked)
    );
    assert_eq!(
        by_seat(&base_plan, "walter").unwrap().verified_how,
        Some(VerificationKind::MachineChecked)
    );
    assert_eq!(
        by_seat(&base_plan, "sol_ext").unwrap().verified_how,
        Some(VerificationKind::MachineChecked)
    );
    assert_eq!(
        by_seat(&base_plan, "eve").unwrap().verified_how,
        Some(VerificationKind::MachineChecked)
    );
    assert!(
        by_seat(&base_plan, "excluded-seat").is_none(),
        "w9:p1S row must be permanently excluded"
    );
    assert!(by_seat(&base_plan, "operator").is_none());
    assert!(by_seat(&base_plan, "desktop").is_none());
    assert!(
        base_plan
            .claims
            .iter()
            .all(|c| !c.seat.as_str().contains('/'))
    );

    let carol_seat = SeatName::parse("carol").unwrap();
    assert_eq!(
        by_seat(&base_plan, "mallory").unwrap().verified_how,
        Some(VerificationKind::WitnessedBy {
            seat: carol_seat.clone(),
        })
    );
    assert_eq!(
        by_seat(&base_plan, "parallax").unwrap().verified_how,
        Some(VerificationKind::WitnessedBy {
            seat: carol_seat.clone(),
        })
    );
    assert_eq!(
        by_seat(&base_plan, "niaj").unwrap().verified_how,
        Some(VerificationKind::WitnessedBy { seat: carol_seat })
    );
    assert!(by_seat(&base_plan, "chroma").is_none());

    // Extended `seed_records_from_interchange_with_options` includes `seats.tsv`.
    let ext_plan = seed_records_from_interchange_with_options(
        &interchange,
        Some(&seats_tsv_path),
        &test_house(),
    )
    .unwrap();
    assert_eq!(ext_plan.sources_read.len(), 7);
    let chroma = by_seat(&ext_plan, "chroma").expect("chroma seeded from seats.tsv");
    assert_eq!(chroma.verified_how, Some(VerificationKind::MachineChecked));
    assert!(
        chroma
            .aliases
            .iter()
            .any(|a| a.as_str() == "cleanup-color_tool_k-means")
    );
    let frank = by_seat(&ext_plan, "frank").unwrap();
    assert!(
        frank
            .aliases
            .iter()
            .any(|a| a.as_str() == "astra-outside-review")
    );
    assert!(
        !frank.aliases.iter().any(|a| a.as_str().contains("fork")),
        "fork/attachment session names must not be registered as aliases"
    );
    let liam = by_seat(&ext_plan, "liam").unwrap();
    assert!(liam.aliases.iter().any(|a| a.as_str() == "ml-papers"));

    // Explicit missing `seats.tsv` returns NotFound instead of silently dropping the flag.
    let missing_seats = house.join("does-not-exist-seats.tsv");
    let err_missing = seed_records_from_interchange_with_options(
        &interchange,
        Some(&missing_seats),
        &test_house(),
    )
    .unwrap_err();
    assert!(
        matches!(err_missing, ApiError::NotFound { .. }),
        "missing explicit seats_tsv must return NotFound, got {err_missing:?}"
    );

    // Symlinked seed sources must be refused.
    #[cfg(unix)]
    {
        let symlink_seats = house.join("seats-link.tsv");
        std::os::unix::fs::symlink(&seats_tsv_path, &symlink_seats).unwrap();
        let err = seed_records_from_interchange_with_options(
            &interchange,
            Some(&symlink_seats),
            &test_house(),
        )
        .unwrap_err();
        assert!(matches!(err, ApiError::Denied { .. }), "got {err:?}");
    }
}

#[test]
fn pr22_to_desktop_shared_lane_readme_skip_and_r3a_addressing() {
    let dir = TempDirGuard::new("pr22-shared-lane-r3a");
    let root = dir.path();
    fs::create_dir_all(root.join("mail")).unwrap();
    fs::create_dir_all(root.join("to-desktop")).unwrap();
    fs::create_dir_all(root.join("to-cc")).unwrap();
    fs::create_dir_all(root.join("to-carol")).unwrap();
    fs::create_dir_all(root.join("state")).unwrap();
    fs::write(root.join("state/acks.jsonl"), b"").unwrap();

    // Box doors and root README/chatter files must be skipped.
    fs::write(root.join("README.md"), b"root door\n").unwrap();
    fs::write(root.join("readme-extra.md"), b"root readme variant\n").unwrap();
    fs::write(root.join("chatter.md"), b"root chatter\n").unwrap();
    fs::write(root.join("to-carol/README.md"), b"carol door\n").unwrap();
    fs::write(root.join("to-desktop/README.md"), b"desktop door\n").unwrap();
    fs::write(root.join("mail/README.md"), b"mail door\n").unwrap();

    // `to-desktop/` is a shared lane: routes by `to:`/`cc:`, never emits `desktop`.
    fs::write(
        root.join("to-desktop/01-shared-review.md"),
        b"---\nfrom: eve\nto: carol (Review Lead desk)\ncc: astra (astra_votp), trent\n---\nbody\n",
    )
    .unwrap();

    // `to-cc/` remains a legacy non-seat box (`SkipNonSeatTarget` for `cc`).
    fs::write(
        root.join("to-cc/02-legacy-intake.md"),
        b"---\nfrom: eve\nto: carol\n---\nbody\n",
    )
    .unwrap();

    // `r3/r3a` addressing: semicolon `cc ` / `cc:`, slash splits, parenthetical glosses vs aliases,
    // and self-author exclusion on shared lanes.
    fs::write(
        root.join("mail/03-r3a-units.md"),
        b"---\nfrom: eve\nto: foo; cc Carol (desk)\ncc: \"Dave-Opus-5 (BOTH live claimants)\", sol (sol_ext / CodeLead-demo-worlds)\n---\nbody\n",
    )
    .unwrap();
    fs::write(
        root.join("mail/04-self-author.md"),
        b"---\nfrom: dave (session \"Dave-Opus-5.5\")\nto: dave (session users-c7); cc: carol, carol-watcher\n---\nbody\n",
    )
    .unwrap();
    fs::write(
        root.join("mail/05-paren-gloss.md"),
        b"---\nfrom: eve\nto: astra_votp (Astra), carol (CTO)\n---\nbody\n",
    )
    .unwrap();
    fs::write(
        root.join("to-carol/06-box-letter.md"),
        b"---\nfrom: carol\nto: carol\n---\nbody\n",
    )
    .unwrap();
    // File path containing `/` in `to:` must never spawn bogus seats from path segments,
    // while `carol / dave` resolves both known seats.
    fs::write(
        root.join("mail/07-slash-path-and-pair.md"),
        b"---\nfrom: eve\nto: carol (the desk), for the cross-read with seats/carol/reports/2026-10-15-republican-factory/verdict.md\ncc: carol / dave\n---\nbody\n",
    )
    .unwrap();
    // Multi-author `from:` + `author:` removes every authoring seat on a shared lane.
    fs::write(
        root.join("mail/08-multi-author.md"),
        b"---\nfrom: carol / dave\nauthor: lucas\nto: carol, dave, lucas, trent\n---\nbody\n",
    )
    .unwrap();

    // Build a presence-backed AliasMap mirroring a seeded house.
    let mut st = PresenceState::default();
    let mut seq = 0;
    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    for (seat, aliases) in [
        ("carol", vec!["carol_old"]),
        ("dave", vec!["dave-opus-5"]),
        ("astra_votp", vec![]),
        ("trent", vec![]),
        ("sol_ext", vec![]),
        ("carol-watcher", vec![]),
        ("lucas", vec![]),
    ] {
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            owner_p.clone(),
            UnixMs(1_000),
            PresenceCommand::PresenceClaim(PresenceClaim {
                aliases: aliases
                    .into_iter()
                    .map(|a| SeatName::parse(a).unwrap())
                    .collect(),
                ..empty_claim(seat)
            }),
            |_, _| true,
        )
        .unwrap();
    }
    let aliases = AliasMap::from_presence(&st);

    let ro = ReadOnlyInterchange::open(root).unwrap();
    let scanned = ro.scan_letters(&aliases).unwrap();
    let by_path: std::collections::BTreeMap<&str, Vec<&str>> = scanned
        .iter()
        .map(|l| {
            (
                l.rel_path.as_str(),
                l.recipients.iter().map(SeatName::as_str).collect(),
            )
        })
        .collect();

    // No README* or chatter.md file scanned anywhere.
    assert_eq!(scanned.len(), 8, "scanned={by_path:?}");
    assert!(
        by_path
            .keys()
            .all(|p| !p.to_ascii_lowercase().contains("readme") && !p.contains("chatter"))
    );

    // `to-desktop/` routed by frontmatter, zero `desktop` recipients.
    assert_eq!(
        by_path["to-desktop/01-shared-review.md"],
        vec!["astra", "astra_votp", "carol", "trent"]
    );
    assert!(
        scanned
            .iter()
            .all(|l| !l.recipients.iter().any(|r| r.as_str() == "desktop"))
    );

    // `to-cc/` still resolves to `cc` -> SkipNonSeatTarget.
    assert_eq!(by_path["to-cc/02-legacy-intake.md"], vec!["cc"]);

    // `r3/r3a` units: `foo`, `carol` (no `desk`), `dave` (from `Dave-Opus-5`), `sol` + `sol_ext`.
    assert_eq!(
        by_path["mail/03-r3a-units.md"],
        vec!["carol", "dave", "foo", "sol", "sol_ext"]
    );

    // Self-author `dave` excluded on shared lane, keeping `carol` and `carol-watcher`.
    assert_eq!(
        by_path["mail/04-self-author.md"],
        vec!["carol", "carol-watcher"]
    );

    // Parenthetical glosses `(Astra)` and `(CTO)` are ignored.
    assert_eq!(
        by_path["mail/05-paren-gloss.md"],
        vec!["astra_votp", "carol"]
    );

    // Box letter in `to-carol/` still targets `carol`.
    assert_eq!(by_path["to-carol/06-box-letter.md"], vec!["carol"]);

    // Slash path does not mint `reports` or `2026-10-15-republican-factory`, while `carol / dave` resolves both.
    assert_eq!(
        by_path["mail/07-slash-path-and-pair.md"],
        vec!["carol", "dave"]
    );

    // Multi-author `from: carol / dave` + `author: lucas` excludes all three, leaving `trent`.
    assert_eq!(by_path["mail/08-multi-author.md"], vec!["trent"]);

    // Offline (`AliasMap::empty()`) also ignores parenthetical glosses (`CTO`, `Astra`) and path segments.
    let scanned_offline = ro.scan_letters(&AliasMap::empty()).unwrap();
    let offline_by_path: std::collections::BTreeMap<&str, Vec<&str>> = scanned_offline
        .iter()
        .map(|l| {
            (
                l.rel_path.as_str(),
                l.recipients.iter().map(SeatName::as_str).collect(),
            )
        })
        .collect();
    assert_eq!(
        offline_by_path["mail/05-paren-gloss.md"],
        vec!["astra_votp", "carol"]
    );
    assert_eq!(
        offline_by_path["mail/07-slash-path-and-pair.md"],
        vec!["carol"]
    );
    assert_eq!(offline_by_path["mail/08-multi-author.md"], vec!["trent"]);

    // Verify `to-cc/02-legacy-intake.md` produces `SkipNonSeatTarget` in full shadow pass.
    let pass = run_shadow_pass(
        &ro,
        &st,
        &IncarnationAffinity::new(),
        &RetryState::new(),
        &RingPolicy::default(),
        &aliases,
        UnixMs(2_000),
    )
    .unwrap();
    let non_seat_targets: Vec<&str> = pass
        .decisions
        .iter()
        .filter_map(|d| match d {
            RingDecision::SkipNonSeatTarget { seat, .. } => Some(seat.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(non_seat_targets, vec!["cc"]);
}

#[test]
fn house_profile_loader_defaults_when_absent_and_applies_custom_profile_when_present() {
    let dir = TempDirGuard::new("house-profile");
    let root = dir.path();

    // Path resolution: absolute XDG wins, relative XDG is ignored, HOME falls back.
    let xdg = root.join("xdg");
    assert_eq!(
        house_profile_path_from(Some(xdg.as_os_str()), Some("/home/nobody".as_ref())),
        Some(xdg.join("rex-wave/house.json"))
    );
    assert_eq!(
        house_profile_path_from(Some("relative/xdg".as_ref()), Some("/home/nobody".as_ref())),
        Some(PathBuf::from("/home/nobody/.config/rex-wave/house.json"))
    );
    assert_eq!(
        house_profile_path_from(Some("".as_ref()), Some("/home/nobody".as_ref())),
        Some(PathBuf::from("/home/nobody/.config/rex-wave/house.json"))
    );
    assert_eq!(house_profile_path_from(None, None), None);

    // Absent: defaults.
    let path = xdg.join("rex-wave/house.json");
    let absent = load_house_profile_from(&path).unwrap();
    assert_eq!(absent, HouseProfile::default());
    assert_eq!(absent.operator_seat.as_str(), DEFAULT_OPERATOR_SURFACE_SEAT);
    assert_eq!(absent.witness_seat.as_str(), DEFAULT_WITNESS_SEAT);
    assert!(absent.aliases.is_empty() && absent.excluded_herdr_targets.is_empty());
    assert!(absent.is_operator_seat("@Operator"));
    assert!(is_default_operator_surface_seat(" operator "));

    // Present: every field applies.
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        br#"{
            "operator_seat": "bob",
            "witness_seat": "alice",
            "excluded_herdr_targets": ["w9:p1S"],
            "aliases": { "carol": ["carol_old", "carol-legacy"] }
        }"#,
    )
    .unwrap();
    let house = load_house_profile_from(&path).unwrap();
    assert_eq!(house, {
        let mut h = HouseProfile {
            operator_seat: seat("bob"),
            witness_seat: seat("alice"),
            excluded_herdr_targets: vec!["w9:p1S".to_owned()],
            ..HouseProfile::default()
        };
        h.aliases
            .insert(seat("carol"), vec![seat("carol_old"), seat("carol-legacy")]);
        h
    });
    assert!(house.is_operator_seat("@BOB") && !house.is_operator_seat("operator"));
    // Offline PresenceState (zero claimed SeatRecords) still folds house.json aliases.
    let offline_aliases =
        AliasMap::from_presence(&PresenceState::with_house_profile(house.clone()));
    assert_eq!(offline_aliases.canonicalize("carol_old"), "carol");
    assert!(offline_aliases.is_operator_surface("@bob"));

    // The loaded profile drives seeding: operator skipped, alias folded,
    // excluded pane dropped, unconfirmed claims witnessed by the profile's seat.
    let interchange = root.join("house/interchange");
    fs::create_dir_all(interchange.join("state")).unwrap();
    fs::write(
        interchange.join("state/ledger.md"),
        "2026-01-01 | CLAIM | bob | operator surface\n2026-01-01 | CLAIM | carol_old | legacy name\n2026-01-01 | CLAIM | dave | plain\n",
    )
    .unwrap();
    fs::write(
        root.join("house/herdr-registry.tsv"),
        "eve\tw9:p1S\t2026-01-01\tpane\nfrank\tw1:p1\t2026-01-01\tpane\n",
    )
    .unwrap();
    let plan = seed_records_from_interchange_with_profile(&interchange, &house).unwrap();
    let seats: Vec<&str> = plan.claims.iter().map(|c| c.seat.as_str()).collect();
    assert_eq!(
        seats,
        ["carol", "dave", "frank"],
        "claims: {:?}",
        plan.claims
    );
    assert!(plan.skipped_targets.contains(&"bob".to_owned()));
    let carol = &plan.claims[0];
    assert!(carol.aliases.iter().any(|a| a.as_str() == "carol_old"));
    assert_eq!(
        carol.verified_how,
        Some(VerificationKind::WitnessedBy {
            seat: seat("alice")
        })
    );

    // The decider enforces the profile's operator surface, not the default.
    let st = PresenceState::with_house_profile(house.clone());
    let ctx = DecideCtx {
        by: Principal::Operator {
            seat: seat("owner"),
        },
        at: UnixMs(1_000),
    };
    let claim = |name: &str| PresenceCommand::PresenceClaim(empty_claim(name));
    assert!(matches!(
        Presence::decide(&st, &claim("bob"), &ctx),
        Err(ApiError::Denied { .. })
    ));
    assert!(Presence::decide(&st, &claim("operator"), &ctx).is_ok());

    // Fail closed: malformed JSON, unknown field, invalid seat name, self-alias,
    // canonical-seat collision, or duplicate alias across two canonical seats.
    for bad in [
        &b"{ not json"[..],
        br#"{ "operator_seat": "bob", "surprise": 1 }"#,
        br#"{ "operator_seat": "Not A Seat" }"#,
        br#"{ "aliases": { "carol": ["bob"] }, "operator_seat": "bob" }"#,
        br#"{ "aliases": { "carol": ["carol"] } }"#,
        br#"{ "aliases": { "carol": ["dave"], "dave": [] } }"#,
        br#"{ "aliases": { "carol": ["shared_alias"], "dave": ["shared_alias"] } }"#,
    ] {
        fs::write(&path, bad).unwrap();
        let err = load_house_profile_from(&path).unwrap_err();
        assert!(matches!(err, ApiError::Invalid(_)), "got {err:?}");
    }

    // Symlinks are refused even when they point at a valid profile.
    #[cfg(unix)]
    {
        let real = root.join("real-house.json");
        fs::write(&real, br#"{ "operator_seat": "bob" }"#).unwrap();
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&real, &path).unwrap();
        let err = load_house_profile_from(&path).unwrap_err();
        assert!(matches!(err, ApiError::Denied { .. }), "got {err:?}");
    }
}

#[test]
fn letter_path_normalization_strips_any_house_root_prefix_but_not_interchange_dirs() {
    let canon = |raw: &str| normalize_letter_path(raw).map(|n| n.canonical);
    // Any house root ending at `/interchange/` folds away.
    for raw in [
        "~/mail-house/interchange/mail/x.md",
        "/abs/root/interchange/mail/x.md",
        "./interchange/mail/x.md",
        "interchange/mail/x.md",
        "mail/x.md",
    ] {
        assert_eq!(canon(raw).as_deref(), Some("mail/x.md"), "{raw}");
    }
    assert_eq!(
        canon("/abs/root/interchange/to-bob/x.md").as_deref(),
        Some("to-bob/x.md")
    );
    // A path already inside the interchange keeps its directories, even when a
    // nested folder happens to be named `interchange`.
    assert_eq!(
        canon("mail/interchange/x.md").as_deref(),
        Some("mail/interchange/x.md")
    );
    assert_eq!(
        canon("to-alice/interchange/x.md").as_deref(),
        Some("to-alice/interchange/x.md")
    );
    assert!(!letter_paths_match("mail/interchange/x.md", "to-bob/x.md"));
    assert_eq!(canon("///"), None);
}
