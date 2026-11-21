use rex_wave_api::mail::{LetterPriority, MailPromote};
use rex_wave_api::presence::*;
use rex_wave_api::room::{Body, RoomPost, RoomRef, RoomScope};
use rex_wave_api::*;
use rex_wave_mailbridge::{
    AliasMap, ClaudeSessionFile, DeferReason, DeliveryStatus, IncarnationAffinity, LetterCandidate,
    MailBridge, Presence, PresenceState, Protection, ReadOnlyInterchange, RetryEntry, RetryState,
    RingDecision, RingDeliveryChannel, RingPolicy, RingReferent, SandboxRoot,
    current_process_start, decide_mail_ring, decide_mail_rings_for_seat, format_delivery_status,
    is_process_alive, load_claude_session_file_from, parse_acks_jsonl, parse_claude_session_json,
    project, query_process_start, run_shadow_pass, score_shadow_against_acks,
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
        ("desk-v1", "tally"),
        ("desk_old", "tally"),
        ("cc-desk", "tally"),
        ("loupe-reviewer", "loupe"),
    ])
}

fn opt_in_interchange_dir() -> Option<PathBuf> {
    std::env::var_os("REX_WAVE_INTERCHANGE_DIR")
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
        from: Some("tally".into()),
        to: SeatName::parse(seat).unwrap(),
        subject: Some("Test letter".into()),
        letter_id: None,
        in_reply_to: None,
        receipt_only: false,
        seen_at,
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
    let crest = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: SeatName::parse("crest").unwrap(),
        },
    );
    let tally = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: SeatName::parse("tally").unwrap(),
        },
    );

    // Claim crest with Operator witness provenance on projects, then update role as crest itself.
    let claimed = owner
        .call(PresenceClaim {
            seat: SeatName::parse("crest").unwrap(),
            status: Some(SeatStatus::Active),
            role: None,
            projects: vec!["rex-wave".into()],
            aliases: vec![SeatName::parse("crest-lead").unwrap()],
            home_room: None,
            verified_how: Some(VerificationKind::WitnessedBy {
                seat: SeatName::parse("tally").unwrap(),
            }),
            replace: None,
            idem: Some(IdemKey::parse("claim-crest-1").unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(claimed.status, SeatStatus::Active);
    assert_eq!(claimed.projects.len(), 1);
    assert_eq!(claimed.projects[0].value, "rex-wave");
    assert_eq!(
        claimed.projects[0].provenance.verified_how,
        VerificationKind::WitnessedBy {
            seat: SeatName::parse("tally").unwrap()
        }
    );

    // Idempotent retry returns the exact same SeatRecord without appending again.
    let retried = owner
        .call(PresenceClaim {
            seat: SeatName::parse("crest").unwrap(),
            status: Some(SeatStatus::Active),
            role: None,
            projects: vec!["rex-wave".into()],
            aliases: vec![SeatName::parse("crest-lead").unwrap()],
            home_room: None,
            verified_how: None,
            replace: None,
            idem: Some(IdemKey::parse("claim-crest-1").unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(retried, claimed);

    // Crest updates its own role; existing project provenance stays intact.
    let updated = crest
        .call(PresenceClaim {
            seat: SeatName::parse("crest").unwrap(),
            status: None,
            role: Some("rex-wave-owner".into()),
            projects: Vec::new(),
            aliases: Vec::new(),
            home_room: None,
            verified_how: None,
            replace: None,
            idem: None,
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
            seat: SeatName::parse("tally").unwrap()
        },
        "untouched attested field retains its original provenance"
    );

    // Tally may not claim crest's seat without explicit replacement.
    let hijack = tally
        .call(PresenceClaim {
            seat: SeatName::parse("crest").unwrap(),
            status: None,
            role: Some("usurper".into()),
            projects: Vec::new(),
            aliases: Vec::new(),
            home_room: None,
            verified_how: None,
            replace: None,
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(hijack, ApiError::Conflict { .. }),
        "got {hijack:?}"
    );

    // Resolve by project works even while crest has zero live bindings.
    let resolved = tally
        .call(PresenceResolve {
            query: "rex-wave".into(),
            lane: None,
        })
        .await
        .unwrap();
    assert_eq!(resolved.record.seat.as_str(), "crest");
    assert!(resolved.live_bindings.is_empty());

    // Bind, heartbeat, retire -> auto-unbinds live bindings and rejects new binds.
    let bound = crest
        .call(PresenceBind {
            seat: SeatName::parse("crest").unwrap(),
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
            idem: Some(IdemKey::parse("bind-crest-1").unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(bound.status, BindingStatus::Live);
    assert_eq!(bound.epoch, 1);

    let hb = crest
        .call(PresenceHeartbeat {
            seat: SeatName::parse("crest").unwrap(),
            incarnation_id: bound.incarnation_id.clone(),
            ttl_ms: Some(60_000),
        })
        .await
        .unwrap();
    assert!(hb.expires_at > bound.expires_at);

    let retired = owner
        .call(PresenceSetStatus {
            seat: SeatName::parse("crest").unwrap(),
            status: SeatStatus::Retired,
            verified_how: None,
            reason: Some("project handed off".into()),
        })
        .await
        .unwrap();
    assert_eq!(retired.status, SeatStatus::Retired);
    assert!(retired.retired_at.is_some());

    let list_default = tally.call(PresenceList::default()).await.unwrap();
    assert!(
        list_default.seats.is_empty(),
        "retired seats are hidden from PresenceList unless include_retired is true"
    );
    let list_all = tally
        .call(PresenceList {
            include_retired: true,
            ..PresenceList::default()
        })
        .await
        .unwrap();
    assert_eq!(list_all.seats.len(), 1);
    assert_eq!(list_all.seats[0].bindings[0].status, BindingStatus::Unbound);

    let bind_retired = crest
        .call(PresenceBind {
            seat: SeatName::parse("crest").unwrap(),
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
            idem: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(bind_retired, ApiError::Conflict { .. }));
}

#[test]
fn claude_session_hook_parser_and_real_ps_process_liveness_enforce_tally_rules() {
    let dir = TempDirGuard::new("claude-sessions");
    let my_pid = std::process::id();
    let my_start = current_process_start().expect("ps reports current process UTC lstart");
    assert!(is_process_alive(my_pid, &my_start));
    assert!(!is_process_alive(my_pid, "Thu Jan  1 00:00:00 1970"));
    assert!(!is_process_alive(999_999_999, &my_start));

    // Background desk seat (`kind: "bg"`, like Tally's 8195.json) MUST be allowed.
    let bg_json = serde_json::json!({
        "pid": my_pid,
        "procStart": my_start,
        "startedAt": 1_794_550_979_000u64,
        "sessionId": "59e9053e-bbdd-4181-bf4f-3f7bf864d276",
        "messagingSocketPath": format!("/tmp/cc-socks/{my_pid}.sock"),
        "kind": "bg",
        "name": "Tally-5.2",
        "nameSource": "user",
        "formerNames": ["Tally-VampOTP-CTO"]
    });
    fs::write(
        dir.path().join(format!("{my_pid}.json")),
        serde_json::to_vec(&bg_json).unwrap(),
    )
    .unwrap();
    let loaded = load_claude_session_file_from(dir.path(), my_pid).unwrap();
    assert_eq!(loaded.kind, "bg");
    assert_eq!(loaded.former_names, vec!["Tally-VampOTP-CTO"]);
    assert!(!loaded.is_derived_name());
    let bind_cmd = loaded
        .into_bind_command(SeatName::parse("tally").unwrap(), None, None, false, false)
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
            "name": "tally-8c",
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
            "name": "Tally-VampOTP-CTO ⑂",
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
    let tally_p = Principal::Seat {
        seat: SeatName::parse("tally").unwrap(),
    };

    // 1) Spawn a short-lived child process, bind it, then let it exit (`--resume` test).
    let mut child = Command::new("/bin/sleep").arg("60").spawn().unwrap();
    let child_pid = child.id();
    let child_start = query_process_start(child_pid).expect("query spawned child lstart");
    assert!(is_process_alive(child_pid, &child_start));

    let first_reply = apply_presence_with_probe(
        &mut state,
        &mut seq,
        tally_p.clone(),
        UnixMs(1_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: SeatName::parse("tally").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: claude_endpoint(
                child_pid,
                &child_start,
                "sess-old-1",
                "bg",
                Some("user"),
                Some("Tally-5.2"),
            ),
            liveness: LivenessMode::ProcessAndLease,
            ttl_ms: Some(600_000),
            takeover: false,
            allow_fork: false,
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
        tally_p.clone(),
        UnixMs(2_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: SeatName::parse("tally").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: claude_endpoint(
                child_pid,
                &child_start,
                "sess-after-clear-2",
                "bg",
                Some("user"),
                Some("Tally-5.2"),
            ),
            liveness: LivenessMode::ProcessAndLease,
            ttl_ms: Some(600_000),
            takeover: false,
            allow_fork: false,
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
        tally_p.clone(),
        UnixMs(3_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: SeatName::parse("tally").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: claude_endpoint(
                my_pid,
                &my_start,
                "sess-other-live",
                "interactive",
                Some("user"),
                Some("Tally-5.2"),
            ),
            liveness: LivenessMode::ProcessAndLease,
            ttl_ms: Some(600_000),
            takeover: false,
            allow_fork: false,
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
        } if seat.as_str() == "tally" && active == &b1.incarnation_id
    ));

    // 4) Kill the old child process -> dead `(pid, proc_start)` auto-supersedes WITHOUT takeover (`--resume`)!
    let _ = child.kill();
    let _ = child.wait();
    assert!(!is_process_alive(child_pid, &child_start));

    let resume_reply = apply_presence_with_probe(
        &mut state,
        &mut seq,
        tally_p.clone(),
        UnixMs(4_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: SeatName::parse("tally").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: claude_endpoint(
                my_pid,
                &my_start,
                "sess-after-clear-2",
                "bg",
                Some("user"),
                Some("Tally-5.2"),
            ),
            liveness: LivenessMode::ProcessAndLease,
            ttl_ms: Some(600_000),
            takeover: false,
            allow_fork: false,
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
        tally_p,
        UnixMs(5_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: SeatName::parse("tally").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: claude_endpoint(
                second_pid,
                &second_start,
                "sess-takeover",
                "interactive",
                Some("user"),
                Some("Tally-5.2"),
            ),
            liveness: LivenessMode::ProcessAndLease,
            ttl_ms: Some(600_000),
            takeover: true,
            allow_fork: false,
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
    let tally_bindings = state.bindings_for(&SeatName::parse("tally").unwrap());
    assert_eq!(
        tally_bindings
            .iter()
            .filter(|b| b.status == BindingStatus::Live)
            .count(),
        1
    );
}

#[test]
fn gate1_all_10_mail_routing_cases_verified() {
    // When running on the estate Mac where Tally's fixture file exists, verify our
    // 10-case matrix covers 100% of the case IDs in `cases.jsonl`.
    let expected_ids: BTreeSet<&str> = [
        "genga-stale-six-days",
        "carbon-crest-misroute",
        "two-tables-one-name",
        "dry-run-marks-seen",
        "ring-types-into-a-live-turn",
        "a-transport-dies",
        "two-seats-one-name",
        "session-names-drift",
        "own-watcher-liveness",
        "bank-before-ring",
    ]
    .into_iter()
    .collect();

    if let Some(root) = opt_in_interchange_dir() {
        let fixture_path = root.join("fixtures/mail-routing/cases.jsonl");
        if fixture_path.is_file() {
            let raw = fs::read_to_string(fixture_path).unwrap();
            let disk_ids: BTreeSet<String> = raw
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| {
                    let v: serde_json::Value = serde_json::from_str(l).unwrap();
                    v["id"].as_str().unwrap().to_owned()
                })
                .collect();
            let expected_owned: BTreeSet<String> =
                expected_ids.iter().map(|s| (*s).to_owned()).collect();
            assert!(
                expected_owned.is_subset(&disk_ids),
                "expected Gate 1 case IDs missing from cases.jsonl: {:?}",
                expected_owned.difference(&disk_ids).collect::<Vec<_>>()
            );
        }
    }

    let owner_p = Principal::Operator {
        seat: SeatName::parse("owner").unwrap(),
    };
    let sys_p = Principal::System {
        part: SystemPart::PresenceMonitor,
    };
    let aliases = AliasMap::empty();
    let policy = RingPolicy::default();
    let empty_acks = parse_acks_jsonl("", &aliases).into_index();
    let empty_retry = RetryState::new();
    let empty_affinity = IncarnationAffinity::new();

    // Case 1: `genga-stale-six-days`
    // Dead process expires -> RaiseUnrung + synchronous sender notice with exact timestamp.
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        let genga = SeatName::parse("genga").unwrap();
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            Principal::Seat {
                seat: genga.clone(),
            },
            UnixMs(1_700_000_000_000),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: genga.clone(),
                incarnation_id: Some(IncarnationId::parse("inc-genga-dead").unwrap()),
                lane: None,
                endpoint: claude_endpoint(
                    4242,
                    "Thu Nov 14 00:00:00 2026",
                    "dead-sess",
                    "interactive",
                    Some("user"),
                    Some("Genga-Opus-5.5"),
                ),
                liveness: LivenessMode::ProcessAndLease,
                ttl_ms: Some(600_000),
                takeover: false,
                allow_fork: false,
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
            "mail/tally-to-genga.md",
            "genga",
            b"---\nto: genga\n---\nhello",
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
                assert_eq!(seat, genga);
                assert_eq!(referents.len(), 1);
                assert_eq!(
                    sender_notice,
                    "banked; no live binding for genga since 2023-11-14T22:13:20Z; will ring on rebind"
                );
            }
            other => panic!("expected RaiseUnrung for genga-stale-six-days, got {other:?}"),
        }
    }

    // Case 2: `carbon-crest-misroute`
    // Durable SeatRecord answers project/role queries offline; ambiguous query fails with Conflict listing all candidates.
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            owner_p.clone(),
            UnixMs(1_000),
            PresenceCommand::PresenceClaim(PresenceClaim {
                seat: SeatName::parse("crest").unwrap(),
                status: Some(SeatStatus::Active),
                role: Some("lead".into()),
                projects: vec!["rex-wave".into(), "shared-infra".into()],
                aliases: Vec::new(),
                home_room: None,
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
        assert_eq!(unique.record.seat.as_str(), "crest");

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
                    detail.contains("carbon") && detail.contains("crest"),
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

    // Case 3: `two-tables-one-name`
    // Single Presence truth resolves `carbon` with its live binding cleanly.
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

    // Case 4: `dry-run-marks-seen`
    // ReadOnlyInterchange + run_shadow_pass emits decisions without touching any file on disk.
    {
        let dir = TempDirGuard::new("dry-run");
        fs::create_dir_all(dir.path().join("mail")).unwrap();
        fs::create_dir_all(dir.path().join("state")).unwrap();
        let letter_path = dir.path().join("mail/1795-tally-to-genga.md");
        let letter_bytes = b"---\nfrom: tally\nto: genga\nsubject: check\n---\nbody\n";
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
            |d| matches!(d, RingDecision::RaiseUnrung { seat, .. } if seat.as_str() == "genga")
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

    // Case 5: `ring-types-into-a-live-turn`
    // Rings always use non-intrusive queued/socket channels (never raw pane keystrokes).
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        let tally = SeatName::parse("tally").unwrap();
        let fathom = SeatName::parse("fathom").unwrap();
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            Principal::Seat {
                seat: tally.clone(),
            },
            UnixMs(1_000),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: tally.clone(),
                incarnation_id: None,
                lane: None,
                endpoint: claude_endpoint(
                    8195,
                    "Thu Nov 12 06:22:59 2026",
                    "59e9053e",
                    "bg",
                    Some("user"),
                    Some("Tally-5.2"),
                ),
                liveness: LivenessMode::ProcessAndLease,
                ttl_ms: Some(600_000),
                takeover: false,
                allow_fork: false,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            Principal::Seat {
                seat: fathom.clone(),
            },
            UnixMs(1_000),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: fathom.clone(),
                incarnation_id: None,
                lane: None,
                endpoint: TransportEndpoint::BbThread {
                    thread_id: BbThreadId::parse("th_fathom_1").unwrap(),
                },
                liveness: LivenessMode::SocketLifetime,
                ttl_ms: None,
                takeover: false,
                allow_fork: false,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();

        let d_tally = decide_mail_ring(
            &sample_candidate("mail/a.md", "tally", b"hi tally", UnixMs(1_500)),
            &empty_acks,
            &st,
            &empty_affinity,
            &empty_retry,
            &policy,
            &aliases,
            UnixMs(1_500),
        );
        assert!(matches!(
            d_tally,
            RingDecision::Ring {
                channel: RingDeliveryChannel::ClaudeMessagingSocket { pid: 8195, .. },
                ..
            }
        ));

        let d_fathom = decide_mail_ring(
            &sample_candidate("mail/b.md", "fathom", b"hi fathom", UnixMs(1_500)),
            &empty_acks,
            &st,
            &empty_affinity,
            &empty_retry,
            &policy,
            &aliases,
            UnixMs(1_500),
        );
        assert!(matches!(
            d_fathom,
            RingDecision::Ring {
                channel: RingDeliveryChannel::BbQueueIfActive { .. },
                ..
            }
        ));
    }

    // Case 6: `a-transport-dies`
    // Per-binding expiry touches ONE seat only; provider TransportHealth flips ALL bindings of that TransportKind together.
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        let quire = SeatName::parse("quire").unwrap();
        let tangle = SeatName::parse("tangle").unwrap();
        for (seat, tid) in [(&quire, "th_quire"), (&tangle, "th_tangle")] {
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
        assert!(
            st.live_bindings_for(&tangle, None, UnixMs(2_000))
                .is_empty()
        );
        assert_eq!(
            st.bindings_for(&quire)[0].status,
            BindingStatus::TransportDown
        );
        assert_eq!(
            st.bindings_for(&tangle)[0].status,
            BindingStatus::TransportDown
        );

        // Recovery restores both; then expiring ONLY quire leaves tangle live.
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
        assert_eq!(st.live_bindings_for(&tangle, None, UnixMs(3_000)).len(), 1);

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
            st.live_bindings_for(&tangle, None, UnixMs(4_000)).len(),
            1,
            "per-binding expiry must not affect sibling bindings on the same transport"
        );
    }

    // Case 7: `two-seats-one-name`
    // Refuses claiming or replacing another seat without authorization; only an operator/system
    // may replace another seat (retiring it and unbinding its live sessions).
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        let plumb = SeatName::parse("plumb").unwrap();
        let plumb_p = Principal::Seat {
            seat: plumb.clone(),
        };
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            plumb_p.clone(),
            UnixMs(1_000),
            PresenceCommand::PresenceClaim(PresenceClaim {
                seat: plumb.clone(),
                status: Some(SeatStatus::Active),
                role: Some("audio".into()),
                projects: Vec::new(),
                aliases: Vec::new(),
                home_room: None,
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
            plumb_p,
            UnixMs(1_500),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: plumb.clone(),
                incarnation_id: None,
                lane: None,
                endpoint: TransportEndpoint::UdsSession {
                    conn_id: 77,
                    client: "plumb-cli".into(),
                },
                liveness: LivenessMode::SocketLifetime,
                ttl_ms: None,
                takeover: false,
                allow_fork: false,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();

        let newcomer = Principal::Seat {
            seat: SeatName::parse("fathom").unwrap(),
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
                seat: plumb.clone(),
                status: None,
                role: Some("emacs".into()),
                projects: Vec::new(),
                aliases: Vec::new(),
                home_room: None,
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
                    seat: plumb.clone(),
                    status: Some(SeatStatus::Active),
                    role: Some("hijack".into()),
                    projects: Vec::new(),
                    aliases: Vec::new(),
                    home_room: None,
                    verified_how: None,
                    replace: Some(plumb.clone()),
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
                    seat: SeatName::parse("fathom").unwrap(),
                    status: Some(SeatStatus::Active),
                    role: Some("emacs".into()),
                    projects: Vec::new(),
                    aliases: Vec::new(),
                    home_room: None,
                    verified_how: None,
                    replace: Some(plumb.clone()),
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
        assert_eq!(st.seat(&plumb).unwrap().status, SeatStatus::Active);
        assert_eq!(st.live_bindings_for(&plumb, None, UnixMs(2_700)).len(), 1);

        // Authorized operator replacement retires the replaced seat and unbinds its live sessions.
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            owner_p.clone(),
            UnixMs(3_000),
            PresenceCommand::PresenceClaim(PresenceClaim {
                seat: SeatName::parse("fathom").unwrap(),
                status: Some(SeatStatus::Active),
                role: Some("emacs".into()),
                projects: Vec::new(),
                aliases: Vec::new(),
                home_room: None,
                verified_how: None,
                replace: Some(plumb.clone()),
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();
        assert_eq!(st.seat(&plumb).unwrap().status, SeatStatus::Retired);
        assert!(st.live_bindings_for(&plumb, None, UnixMs(3_000)).is_empty());
        assert_eq!(
            st.seat(&SeatName::parse("fathom").unwrap()).unwrap().status,
            SeatStatus::Active
        );
    }

    // Case 8: `session-names-drift`
    // Keyed on `(pid, proc_start, session_id, socket)` + `in_reply_to` incarnation affinity.
    {
        let mut st = PresenceState::default();
        let mut seq = 0;
        let genga = SeatName::parse("genga").unwrap();
        let gp = Principal::Seat {
            seat: genga.clone(),
        };
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            gp.clone(),
            UnixMs(1_000),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: genga.clone(),
                incarnation_id: Some(IncarnationId::parse("inc-genga-main").unwrap()),
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
                seat: genga.clone(),
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
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();
        // Second live lane (`genga-2d`) bound on explicit lane `"2d"`.
        apply_presence_with_probe(
            &mut st,
            &mut seq,
            gp,
            UnixMs(2_500),
            PresenceCommand::PresenceBind(PresenceBind {
                seat: genga.clone(),
                incarnation_id: Some(IncarnationId::parse("inc-genga-2d").unwrap()),
                lane: Some("2d".into()),
                endpoint: claude_endpoint(
                    18604,
                    "Thu Nov 19 06:00:00 2026",
                    "2d-sess",
                    "interactive",
                    Some("user"),
                    Some("Genga-2D"),
                ),
                liveness: LivenessMode::ProcessAndLease,
                ttl_ms: Some(600_000),
                takeover: false,
                allow_fork: false,
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();

        let mut affinity = IncarnationAffinity::new();
        affinity.record_authored(
            "letter-2d-root",
            genga.clone(),
            IncarnationId::parse("inc-genga-2d").unwrap(),
        );
        let picked_reply = select_binding_for_delivery(
            &st,
            &genga,
            None,
            Some("letter-2d-root"),
            &affinity,
            UnixMs(3_000),
        )
        .unwrap();
        assert_eq!(picked_reply.incarnation_id.as_str(), "inc-genga-2d");

        let picked_default =
            select_binding_for_delivery(&st, &genga, None, None, &affinity, UnixMs(3_000)).unwrap();
        assert_eq!(picked_default.incarnation_id.as_str(), "inc-genga-main");
    }

    // Case 9: `own-watcher-liveness`
    // SelfWatcher holds a TTL lease; alive while renewed, expires deterministically when missed.
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

    // Case 10: `bank-before-ring`
    // Banking writes canon letter bytes to disk even when the recipient seat has ZERO live bindings.
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
                    Body::parse("Bank this letter even when genga is offline.").unwrap(),
                ))
                .await
                .unwrap();

            // `genga` has NO presence binding, yet `MailPromote` banks the file durably first.
            let letter = owner
                .call(MailPromote {
                    room: posted.room,
                    range: SeqRange::new(posted.seq, posted.seq).unwrap(),
                    to: vec![SeatName::parse("genga").unwrap()],
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
            r#"{{"ts":"2026-09-17T09:13:46+00:00","file":"interchange/to-loupe/d.md","sha256":"{}","sha256_full":"{}","by":"Loupe-Reviewer"}}"#,
            &sha_d.as_str()[..16],
            sha_d.as_str()
        ),
        // 5. letters array of objects with seat
        format!(
            r#"{{"ts":"2026-10-31T01:50:00-05:00","kind":"ack","seat":"tally","letters":[{{"file":"interchange/mail/mut.md","sha256_16":"{}"}}]}}"#,
            &sha_mut_old.as_str()[..16]
        ),
        // 6. letters array of strings (path-only historical row)
        r#"{"ts":"2026-10-29T04:57:03+0000","seat":"tally","letters":["interchange/mail/old1.md","interchange/mail/old2.md"],"ack":"cc-read"}"#.to_owned(),
        // 7. items batch array
        r#"{"ts":"2026-10-31T08:05:00-05:00","kind":"ack-batch","seat":"tally","items":[{"letter":"reconcile.md","read":"done"}]}"#.to_owned(),
        // 8. non-ACK owner-ruling row (counted cleanly, not skipped as broken)
        r#"{"ts":"2026-10-30T11:06:00-05:00","kind":"owner-ruling","topic":"backups-over-everything","verbatim":"keep it clean"}"#.to_owned(),
        // 9. legacy sandbox schema (path + seat + sha256_full)
        format!(
            r#"{{"schema":1,"seat":"loupe","path":"to-loupe/mut.md","sha256_full":"{}"}}"#,
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
    assert!(idx.is_acked_by_digest("tally", &sha_a, &aliases));
    assert!(idx.is_acked_by_digest("tally", &sha_b, &aliases));
    assert!(idx.is_acked_by_digest("tally", &sha_c, &aliases));
    assert!(idx.is_acked_by_digest("loupe", &sha_d, &aliases));
    assert!(!idx.is_acked_by_digest("genga", &sha_a, &aliases));

    // Also verify `project()` in lib.rs uses the tolerant `ack_row_matches` without hardcoded aliases.
    let sb_dir = TempDirGuard::new("sandbox-acks-compat");
    fs::write(sb_dir.path().join(".rex-wave-sandbox"), b"ok\n").unwrap();
    fs::create_dir_all(sb_dir.path().join("to-tally")).unwrap();
    fs::create_dir_all(sb_dir.path().join("state")).unwrap();
    let sb_letter = b"---\nfrom: genga\nto: tally\nsubject: Compat\n---\nbody\n";
    let sb_sha = sha256_of(sb_letter);
    let sb_letter_path = sb_dir.path().join("to-tally/compat-r1.md");
    fs::write(&sb_letter_path, sb_letter).unwrap();
    // Write a historical-format ACK row (`acked_by: @Tally`, `letter: interchange/to-tally/compat-r1.md`, 16-hex `sha16`) plus a non-JSON line.
    mixed_lines.push(format!(
        r#"{{"ts":"2026-11-21T06:00:00Z","acked_by":"@Tally","letter":"interchange/to-tally/compat-r1.md","sha16":"{}"}}"#,
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
        &sandbox.path().join("to-tally/compat-r1.md"),
        sb_letter,
    )
    .unwrap();
    assert_eq!(
        item.state, "acked",
        "sandbox project() must recognize historical acks.jsonl schemas and ignore non-JSON lines"
    );

    // Prepare live bindings for `loupe` and `tally` for frictions 2..6.
    let mut st = PresenceState::default();
    let mut seq = 0;
    for (seat, conn_id) in [("loupe", 11u64), ("tally", 12u64)] {
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
                idem: None,
            }),
            |_, _| true,
        )
        .unwrap();
    }

    // Friction 2: `(seat, sha256)` dedupe across `mail/` and `to-loupe/` copies (acked AND unacked).
    let copy_mail = sample_candidate("mail/dup-r1.md", "loupe", b"same-content", UnixMs(2_000));
    let copy_box = sample_candidate(
        "to-loupe/dup-r1.md",
        "loupe",
        b"same-content",
        UnixMs(2_000),
    );
    let decisions_unacked = decide_mail_rings_for_seat(
        &SeatName::parse("loupe").unwrap(),
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

    let acked_copy = sample_candidate("mail/d.md", "loupe", b"letter-d-bytes", UnixMs(2_000));
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
        rel_path: "to-loupe/mut.md".into(),
        sha256: sha_mut_new.clone(),
        sha16: sha_mut_new.as_str()[..16].to_owned(),
        from: Some("genga".into()),
        to: SeatName::parse("loupe").unwrap(),
        subject: Some("Amended".into()),
        letter_id: None,
        in_reply_to: None,
        receipt_only: false,
        seen_at: UnixMs(2_000),
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
    let busy_letter = sample_candidate("mail/busy.md", "tally", b"busy-body", UnixMs(10_000));
    let mut retry_many_passes = RetryState::new();
    retry_many_passes.insert(
        SeatName::parse("tally").unwrap(),
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
        SeatName::parse("tally").unwrap(),
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
    let tally_seat = SeatName::parse("tally").unwrap();
    let burst: Vec<LetterCandidate> = (1..=4)
        .map(|i| {
            sample_candidate(
                &format!("mail/burst-{i}.md"),
                "tally",
                format!("burst-body-{i}").as_bytes(),
                UnixMs(20_000 + i),
            )
        })
        .collect();
    let three = decide_mail_rings_for_seat(
        &tally_seat,
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
        &tally_seat,
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
    let empty_path_cand = sample_candidate("   ", "tally", b"no-path", UnixMs(25_000));
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

    let mut receipt_only = sample_candidate("mail/receipt.md", "tally", b"receipt", UnixMs(25_000));
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
        "---\nfrom: tally\nto: loupe\nstatus: \"receipt only; no return ACK requested\"\n---\nbody\n",
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

    let b_acked = b"---\nfrom: tally\nto: loupe\n---\nalready read\n";
    let b_live = b"---\nfrom: tally\nto: loupe\n---\nnew for live loupe\n";
    let b_missed = b"---\nfrom: tally\nto: genga\n---\nmissed for offline genga\n";
    let b_retired = b"---\nfrom: tally\nto: quire\n---\nto retired quire\n";
    fs::write(dir.path().join("mail/01-acked.md"), b_acked).unwrap();
    fs::write(dir.path().join("mail/02-live.md"), b_live).unwrap();
    fs::write(dir.path().join("mail/03-missed.md"), b_missed).unwrap();
    fs::write(dir.path().join("mail/04-retired.md"), b_retired).unwrap();

    let sha_acked = sha256_of(b_acked);
    // ACK recorded under an alias declared on loupe's durable SeatRecord.
    let acks_before_raw = format!(
        "{{\"ts\":\"2026-11-21T05:00:00Z\",\"acked_by\":\"loupe-review\",\"letter\":\"mail/01-acked.md\",\"sha256\":\"{}\"}}\n",
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
    // loupe = Active + Live (with alias `loupe-review`), genga = Active + Offline, quire = Retired.
    apply_presence_with_probe(
        &mut st,
        &mut seq,
        owner_p.clone(),
        UnixMs(900),
        PresenceCommand::PresenceClaim(PresenceClaim {
            seat: SeatName::parse("loupe").unwrap(),
            status: Some(SeatStatus::Active),
            role: None,
            projects: Vec::new(),
            aliases: vec![SeatName::parse("loupe-review").unwrap()],
            home_room: None,
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
            seat: SeatName::parse("loupe").unwrap(),
        },
        UnixMs(1_000),
        PresenceCommand::PresenceBind(PresenceBind {
            seat: SeatName::parse("loupe").unwrap(),
            incarnation_id: None,
            lane: None,
            endpoint: TransportEndpoint::UdsSession {
                conn_id: 1,
                client: "loupe".into(),
            },
            liveness: LivenessMode::SocketLifetime,
            ttl_ms: None,
            takeover: false,
            allow_fork: false,
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
            seat: SeatName::parse("genga").unwrap(),
            status: Some(SeatStatus::Active),
            role: None,
            projects: Vec::new(),
            aliases: Vec::new(),
            home_room: None,
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

    // Opt-in read-only smoke test against a real interchange directory when
    // `REX_WAVE_INTERCHANGE_DIR` is set in the environment.
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
