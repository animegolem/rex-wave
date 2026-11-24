//! `mail shadow --watch` append-only transitions, multi-pass scoring, ring
//! watcher `seen.jsonl` formats, and T3 doorbell target seeding.
#![allow(clippy::disallowed_methods)]

use rex_wave_api::presence::*;
use rex_wave_api::*;
use rex_wave_mailbridge::{
    AliasMap, IncarnationAffinity, Presence, PresenceState, ReadOnlyInterchange, RetryState,
    RingDecision, RingPolicy, RingReferent, ShadowWatchTracker, TimeBasis, WatchDecisionState,
    compare_shadow_with_mailwatch_seen, parse_acks_jsonl, parse_mailwatch_seen_jsonl,
    parse_mailwatch_seen_sources, run_shadow_pass, score_shadow_jsonl,
    seed_records_from_interchange_with_profile,
};
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static DIR_COUNTER: AtomicU64 = AtomicU64::new(1);

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("rex-wave-watch-{label}-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn sha256_of(bytes: &[u8]) -> Sha256Hex {
    let hex: String = Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Sha256Hex::parse(hex).unwrap()
}

fn seat(name: &str) -> SeatName {
    SeatName::parse(name).unwrap()
}

fn apply(state: &mut PresenceState, seq: &mut u64, by: Principal, cmd: PresenceCommand) {
    let at = UnixMs(1_000);
    let ctx = DecideCtx::for_test(by.clone(), at);
    let events = Presence::decide_with_probe(state, &cmd, &ctx, |_, _| true).unwrap();
    for ev in events {
        *seq += 1;
        let meta = EntryMeta {
            seq: Seq(*seq),
            at,
            by: by.clone(),
        };
        Presence::evolve(state, &ev, &meta);
    }
}

fn claim(name: &str, aliases: &[&str]) -> PresenceCommand {
    PresenceCommand::PresenceClaim(PresenceClaim {
        seat: seat(name),
        status: Some(SeatStatus::Active),
        role: None,
        projects: Vec::new(),
        aliases: aliases.iter().map(|a| seat(a)).collect(),
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
    })
}

/// `alpha` is Active + Live (alias `alpha-old`); `beta` is Active with no
/// binding, so its letters bank as `raise_unrung`.
fn alpha_live_beta_offline() -> PresenceState {
    let mut st = PresenceState::default();
    let mut seq = 0;
    let ops = Principal::Operator { seat: seat("ops") };
    apply(
        &mut st,
        &mut seq,
        ops.clone(),
        claim("alpha", &["alpha-old"]),
    );
    apply(&mut st, &mut seq, ops, claim("beta", &[]));
    apply(
        &mut st,
        &mut seq,
        Principal::Seat {
            seat: seat("alpha"),
        },
        PresenceCommand::PresenceBind(PresenceBind {
            seat: seat("alpha"),
            incarnation_id: None,
            lane: None,
            endpoint: TransportEndpoint::UdsSession {
                conn_id: 7,
                client: "alpha".into(),
            },
            liveness: LivenessMode::SocketLifetime,
            ttl_ms: None,
            takeover: false,
            allow_fork: false,
            // Letters in these fixtures carry `id:` ms values above zero.
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        }),
    );
    st
}

fn letter(dir: &Path, name: &str, to: &str, id_ms: u64) -> Sha256Hex {
    let body =
        format!("---\nfrom: ops\nto: {to}\nid: {id_ms}\nsubject: {name}\n---\n{name} body\n");
    fs::write(dir.join("mail").join(name), &body).unwrap();
    sha256_of(body.as_bytes())
}

struct Harness {
    dir: TempDir,
    presence: PresenceState,
    aliases: AliasMap,
}

impl Harness {
    fn new(label: &str) -> Self {
        let dir = TempDir::new(label);
        fs::create_dir_all(dir.path().join("mail")).unwrap();
        fs::create_dir_all(dir.path().join("state")).unwrap();
        fs::write(dir.path().join("state/acks.jsonl"), "").unwrap();
        let presence = alpha_live_beta_offline();
        let aliases = AliasMap::from_presence(&presence);
        Self {
            dir,
            presence,
            aliases,
        }
    }

    fn pass(&self, now: u64) -> Vec<RingDecision> {
        let ro = ReadOnlyInterchange::open(self.dir.path()).unwrap();
        run_shadow_pass(
            &ro,
            &self.presence,
            &IncarnationAffinity::new(),
            &RetryState::new(),
            &RingPolicy::default(),
            &self.aliases,
            UnixMs(now),
        )
        .unwrap()
        .decisions
    }

    fn ack(&self, by: &str, rel: &str, sha: &Sha256Hex) {
        let line = format!(
            "{{\"ts\":\"2026-11-24T00:00:00Z\",\"acked_by\":\"{by}\",\"letter\":\"{rel}\",\"sha256\":\"{sha}\"}}\n"
        );
        let path = self.dir.path().join("state/acks.jsonl");
        let mut raw = fs::read_to_string(&path).unwrap();
        raw.push_str(&line);
        fs::write(path, raw).unwrap();
    }

    fn acks_text(&self) -> String {
        fs::read_to_string(self.dir.path().join("state/acks.jsonl")).unwrap()
    }
}

fn lines(jsonl: &str) -> Vec<serde_json::Value> {
    jsonl
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn kinds(jsonl: &str) -> Vec<String> {
    lines(jsonl)
        .iter()
        .map(|v| {
            v["decision"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| format!("meta:{}", v["meta"].as_str().unwrap()))
        })
        .collect()
}

#[test]
fn skip_already_acked_json_carries_sha256_and_referent() {
    let sha = sha256_of(b"acked letter");
    let decision = RingDecision::SkipAlreadyAcked {
        seat: seat("alpha"),
        referent: RingReferent::new("mail/01-acked.md", sha.clone()).unwrap(),
        time_basis: TimeBasis::LetterIdMs,
    };
    let v = decision.to_json_value();
    assert_eq!(v["decision"], "skip_already_acked");
    assert_eq!(
        v["sha256"],
        sha.as_str(),
        "top-level sha256 kept for old readers"
    );
    assert_eq!(v["referent"]["sha256"], sha.as_str());
    assert_eq!(v["referent"]["sha16"], &sha.as_str()[..16]);
    assert_eq!(v["referent"]["path"], "mail/01-acked.md");
}

#[test]
fn watch_tracker_multi_pass_ring_ack_restart_scores_one_transient_ring() {
    let h = Harness::new("multi-pass");
    let rel1 = "1795000000001-to-alpha.md";
    let sha1 = letter(h.dir.path(), rel1, "alpha", 1_795_000_000_001);
    let sha_b = letter(
        h.dir.path(),
        "1795000000002-to-beta.md",
        "beta",
        1_795_000_000_002,
    );
    let mut log = String::new();
    let mut tracker = ShadowWatchTracker::new();

    // Pass 1: letter arrives -> alias header + ring (alpha) + raise_unrung (beta).
    let d1 = tracker
        .observe_pass(&h.pass(5_000), &h.aliases, UnixMs(5_000))
        .unwrap();
    assert_eq!(
        kinds(&d1.jsonl),
        vec!["meta:aliases", "ring", "raise_unrung"]
    );
    assert_eq!((d1.decision_lines, d1.alias_lines), (2, 1));
    for v in lines(&d1.jsonl) {
        assert_eq!(v["observed_at_ms"], 5_000, "every watch line is stamped");
    }
    assert_eq!(
        tracker.state_of(&seat("alpha"), &sha1),
        Some(&WatchDecisionState::Ring)
    );
    log.push_str(&d1.jsonl);

    // Pass 2: nothing changed (still un-acked) -> zero lines, zero duplicate rings.
    let d2 = tracker
        .observe_pass(&h.pass(6_000), &h.aliases, UnixMs(6_000))
        .unwrap();
    assert!(d2.jsonl.is_empty(), "unexpected delta: {}", d2.jsonl);
    assert_eq!(d2.decision_lines, 0);

    // Pass 3: alpha acks under its alias -> exactly one skip_already_acked line.
    h.ack("alpha-old", &format!("mail/{rel1}"), &sha1);
    let d3 = tracker
        .observe_pass(&h.pass(7_000), &h.aliases, UnixMs(7_000))
        .unwrap();
    assert_eq!(kinds(&d3.jsonl), vec!["skip_already_acked"]);
    let skip = &lines(&d3.jsonl)[0];
    assert_eq!(skip["referent"]["sha256"], sha1.as_str());
    assert_eq!(skip["sha256"], sha1.as_str());
    assert_eq!(skip["observed_at_ms"], 7_000);
    log.push_str(&d3.jsonl);

    // Pass 4: watcher restarts over the existing log -> no baseline duplicates.
    let mut restarted = ShadowWatchTracker::from_existing_jsonl(&log).unwrap();
    assert_eq!(restarted.tracked_referents(), 2);
    assert_eq!(
        restarted.state_of(&seat("alpha"), &sha1),
        Some(&WatchDecisionState::SkipAlreadyAcked)
    );
    let d4 = restarted
        .observe_pass(&h.pass(8_000), &h.aliases, UnixMs(8_000))
        .unwrap();
    assert!(
        d4.jsonl.is_empty(),
        "restart duplicated baseline: {}",
        d4.jsonl
    );
    log.push_str(&d4.jsonl);

    assert_eq!(kinds(&log).iter().filter(|k| *k == "ring").count(), 1);
    let acks = parse_acks_jsonl(&h.acks_text(), &h.aliases).index();
    let score = score_shadow_jsonl(&log, &acks, &h.aliases).unwrap();
    assert_eq!(
        score.rings_emitted, 1,
        "transient ring survives the later skip"
    );
    assert_eq!(score.referents_rung, 1);
    assert_eq!(score.ring_referents_confirmed_acked, 1);
    assert_eq!(score.ring_referents_pending_ack, 0);
    assert_eq!(score.skipped_already_acked, 1);
    assert_eq!(score.unrung_alerts, 1);
    assert_eq!(score.unrung_referents_caught, 1);
    assert_eq!(score.seats_with_unrung, vec![seat("beta")]);

    // Concatenating the same log twice (e.g. two `--out` files) never double-counts.
    let doubled = format!("{log}{log}");
    let doubled_score = score_shadow_jsonl(&doubled, &acks, &h.aliases).unwrap();
    assert_eq!(doubled_score.referents_rung, 1);
    assert_eq!(doubled_score.rings_emitted, 1);
    assert_eq!(doubled_score.unrung_referents_caught, 1);

    // Pass 5: a new letter for beta appends only that referent to raise_unrung.
    let sha_b2 = letter(
        h.dir.path(),
        "1795000000003-to-beta.md",
        "beta",
        1_795_000_000_003,
    );
    let d5 = restarted
        .observe_pass(&h.pass(9_000), &h.aliases, UnixMs(9_000))
        .unwrap();
    assert_eq!(kinds(&d5.jsonl), vec!["raise_unrung"]);
    let unrung = &lines(&d5.jsonl)[0];
    assert_eq!(unrung["count"], 1);
    assert_eq!(unrung["referents"][0]["sha256"], sha_b2.as_str());
    assert_ne!(sha_b, sha_b2);
}

#[test]
fn watch_tracker_narrows_ring_digest_to_new_referents() {
    let h = Harness::new("digest");
    let policy = RingPolicy::default();
    let first: Vec<Sha256Hex> = (0..=policy.digest_threshold as u64)
        .map(|i| {
            letter(
                h.dir.path(),
                &format!("17950000001{i:02}-a.md"),
                "alpha",
                1_795_000_000_100 + i,
            )
        })
        .collect();
    let mut tracker = ShadowWatchTracker::new();
    let d1 = tracker
        .observe_pass(&h.pass(5_000), &h.aliases, UnixMs(5_000))
        .unwrap();
    let digest = lines(&d1.jsonl)
        .into_iter()
        .find(|v| v["decision"] == "ring_digest")
        .unwrap();
    assert_eq!(digest["count"], first.len());

    let fresh = letter(
        h.dir.path(),
        "1795000000199-a.md",
        "alpha",
        1_795_000_000_199,
    );
    let d2 = tracker
        .observe_pass(&h.pass(6_000), &h.aliases, UnixMs(6_000))
        .unwrap();
    assert_eq!(kinds(&d2.jsonl), vec!["ring_digest"]);
    let narrowed = &lines(&d2.jsonl)[0];
    assert_eq!(narrowed["count"], 1);
    assert_eq!(narrowed["referents"][0]["sha256"], fresh.as_str());

    let log = format!("{}{}", d1.jsonl, d2.jsonl);
    let acks = parse_acks_jsonl("", &h.aliases).index();
    let score = score_shadow_jsonl(&log, &acks, &h.aliases).unwrap();
    assert_eq!(score.referents_rung, first.len() + 1);
    assert_eq!(score.rings_emitted, 2);
}

#[test]
fn watch_tracker_hydrates_legacy_lines_and_refuses_corrupt_logs() {
    let sha = sha256_of(b"legacy");
    // Pre-referent `skip_already_acked` shape (top-level sha256 only).
    let legacy = format!(
        "{{\"decision\":\"skip_already_acked\",\"seat\":\"alpha\",\"sha256\":\"{sha}\",\"time_basis\":\"letter_id_ms\"}}\n"
    );
    let mut tracker = ShadowWatchTracker::from_existing_jsonl(&legacy).unwrap();
    let decision = RingDecision::SkipAlreadyAcked {
        seat: seat("alpha"),
        referent: RingReferent::new("mail/x.md", sha.clone()).unwrap(),
        time_basis: TimeBasis::LetterIdMs,
    };
    let delta = tracker
        .observe_pass(&[decision], &AliasMap::empty(), UnixMs(1))
        .unwrap();
    assert!(delta.jsonl.is_empty());

    // Defer churn (`pass_count`) is not a transition; a new reason kind is.
    let defer = |reason: rex_wave_mailbridge::DeferReason| RingDecision::Defer {
        seat: seat("gamma"),
        referent: RingReferent::new("mail/y.md", sha.clone()).unwrap(),
        reason,
        deadline_at: UnixMs(99),
        time_basis: TimeBasis::LetterIdMs,
    };
    let busy = |n| rex_wave_mailbridge::DeferReason::TargetBusy { pass_count: n };
    let mut t = ShadowWatchTracker::new();
    let none = AliasMap::empty();
    assert_eq!(
        t.observe_pass(&[defer(busy(0))], &none, UnixMs(1))
            .unwrap()
            .decision_lines,
        1
    );
    assert_eq!(
        t.observe_pass(&[defer(busy(5))], &none, UnixMs(2))
            .unwrap()
            .decision_lines,
        0
    );
    let grace = rex_wave_mailbridge::DeferReason::ReconnectGrace {
        ready_at: UnixMs(50),
    };
    assert_eq!(
        t.observe_pass(&[defer(grace)], &none, UnixMs(3))
            .unwrap()
            .decision_lines,
        1
    );

    // Refusals have no referent; they dedupe on (seat, reason).
    let refuse = RingDecision::RefuseMissingReferent {
        seat: seat("gamma"),
        reason: "no path".into(),
        time_basis: TimeBasis::FileModified,
    };
    assert_eq!(
        t.observe_pass(std::slice::from_ref(&refuse), &none, UnixMs(4))
            .unwrap()
            .decision_lines,
        1
    );
    assert_eq!(
        t.observe_pass(&[refuse], &none, UnixMs(5))
            .unwrap()
            .decision_lines,
        0
    );

    assert!(ShadowWatchTracker::from_existing_jsonl("{\"decision\":\"ring\",\"seat\":").is_err());
    assert!(
        ShadowWatchTracker::from_existing_jsonl("{\"decision\":\"mystery\",\"seat\":\"alpha\"}")
            .is_err()
    );
}

#[test]
fn score_excludes_unrung_referents_that_later_rang() {
    let sha = sha256_of(b"banked then rung");
    let aliases = AliasMap::from_pairs([("beta-old", "beta")]);
    let referent = format!(
        "{{\"path\":\"mail/b.md\",\"sha16\":\"{}\",\"sha256\":\"{sha}\"}}",
        &sha.as_str()[..16]
    );
    let log = format!(
        concat!(
            "{{\"decision\":\"raise_unrung\",\"seat\":\"beta\",\"count\":1,\"oldest_at_ms\":1,\"sender_notice\":\"banked\",\"referents\":[{r}],\"time_basis\":\"letter_id_ms\",\"observed_at_ms\":1}}\n",
            "{{\"decision\":\"ring\",\"seat\":\"beta-old\",\"incarnation_id\":\"inc_1\",\"channel\":\"uds_frame_push\",\"referent\":{r},\"time_basis\":\"letter_id_ms\",\"observed_at_ms\":2}}\n",
        ),
        r = referent
    );
    let acks = parse_acks_jsonl("", &aliases).index();
    let score = score_shadow_jsonl(&log, &acks, &aliases).unwrap();
    assert_eq!(score.referents_rung, 1);
    assert_eq!(score.unrung_alerts, 1);
    assert_eq!(
        score.unrung_referents_caught, 0,
        "it rang later; not a miss"
    );
    assert_eq!(score.unrung_referents_already_acked, 0);
}

#[test]
fn seen_sources_merge_mailwatch_and_t3_doorbell_formats() {
    let mk = |tag: &str| sha256_of(tag.as_bytes());
    let (s_rung, s_unknown, s_failed, s_gave_up, s_seeded, s_absent) = (
        mk("rung"),
        mk("unknown"),
        mk("failed"),
        mk("gave-up"),
        mk("seeded"),
        mk("absent"),
    );
    let aliases = AliasMap::from_pairs([("alpha-t3", "alpha")]);
    let ring = |seat: &str, sha: &Sha256Hex, path: &str| {
        format!(
            "{{\"decision\":\"ring\",\"seat\":\"{seat}\",\"incarnation_id\":\"inc_1\",\"channel\":\"t3_queue_if_active\",\"referent\":{{\"path\":\"{path}\",\"sha16\":\"{}\",\"sha256\":\"{sha}\"}},\"time_basis\":\"letter_id_ms\"}}\n",
            &sha.as_str()[..16]
        )
    };
    let mut shadow = String::new();
    for (sha, path) in [
        (&s_rung, "mail/1.md"),
        (&s_unknown, "mail/2.md"),
        (&s_failed, "mail/3.md"),
        (&s_gave_up, "mail/4.md"),
        (&s_seeded, "mail/5.md"),
        (&s_absent, "mail/6.md"),
    ] {
        shadow.push_str(&ring("alpha", sha, path));
    }
    // The same referent rung again on a later watch pass is compared once.
    shadow.push_str(&ring("alpha-t3", &s_rung, "mail/1.md"));

    let h16 = |s: &Sha256Hex| s.as_str()[..16].to_owned();
    let mailwatch = format!(
        "{{\"key\":\"shared-alpha:{}\",\"outcome\":\"defer:missing\",\"attempts\":1}}\n",
        h16(&s_rung)
    );
    let doorbell = [
        (&s_rung, "rung"),
        (&s_unknown, "outcome_unknown"),
        (&s_failed, "failed:queue"),
        (&s_gave_up, "gave-up"),
        (&s_seeded, "seeded"),
    ]
    .iter()
    .map(|(sha, outcome)| {
        format!(
            "{{\"sha16\":\"{}\",\"seat\":\"alpha-t3\",\"outcome\":\"{outcome}\",\"ts\":\"2026-11-24T00:00:00Z\"}}\n",
            h16(sha)
        )
    })
    .collect::<String>();

    let idx = parse_mailwatch_seen_sources(
        &[
            ("mailwatch.jsonl", &mailwatch),
            ("doorbell.jsonl", &doorbell),
        ],
        &aliases,
    )
    .unwrap();
    assert_eq!(idx.len(), 6, "1 mailwatch key + 5 doorbell keys");
    assert_eq!(
        idx.outcome_for("alpha", &h16(&s_rung), &aliases),
        Some("rung"),
        "doorbell rung outranks mailwatch defer for the same canonical referent"
    );

    let cmp = compare_shadow_with_mailwatch_seen(&shadow, &idx, &aliases).unwrap();
    assert_eq!(cmp.seen_entries, 6);
    assert_eq!(cmp.shadow_ring_referents, 6);
    assert_eq!(cmp.agreed_rung, 2, "rung + outcome_unknown");
    assert_eq!(cmp.deferred_or_unrung_in_mailwatch, 2, "failed* + gave-up");
    assert_eq!(cmp.other_in_mailwatch, 1, "seeded");
    assert_eq!(cmp.absent_in_mailwatch, 1);
    assert_eq!(cmp.own_knocker_in_mailwatch, 0);

    // A single doorbell file parses on its own, too.
    assert_eq!(
        parse_mailwatch_seen_jsonl(&doorbell, &aliases)
            .unwrap()
            .len(),
        5
    );
    let err = parse_mailwatch_seen_sources(
        &[(
            "doorbell.jsonl",
            "{\"sha16\":\"nothex\",\"seat\":\"alpha\",\"outcome\":\"rung\"}",
        )],
        &aliases,
    )
    .unwrap_err();
    assert!(err.starts_with("doorbell.jsonl line 1"), "{err}");
    assert!(parse_mailwatch_seen_sources(&[("x", "{\"outcome\":\"rung\"}")], &aliases).is_err());
}

#[test]
fn seed_records_reads_t3_doorbell_targets_with_aliases() {
    let house = TempDir::new("doorbell-targets");
    let interchange = house.path().join("interchange");
    fs::create_dir_all(&interchange).unwrap();
    fs::create_dir_all(house.path().join("tooling/t3-doorbell")).unwrap();
    let targets = house.path().join("tooling/t3-doorbell/targets.json");
    fs::write(
        &targets,
        serde_json::to_vec(&serde_json::json!({
            "_comment": "seat -> T3 thread",
            "alpha": { "thread_id": "thr-a", "aliases": ["alpha-t3", "to-alpha-old"] },
            "gamma": { "thread_id": "thr-g" },
            "delta": "not an object"
        }))
        .unwrap(),
    )
    .unwrap();

    let plan =
        seed_records_from_interchange_with_profile(&interchange, &HouseProfile::default()).unwrap();
    let canon_targets = fs::canonicalize(&targets).unwrap();
    assert!(
        plan.sources_read
            .iter()
            .any(|p| fs::canonicalize(p).ok().as_ref() == Some(&canon_targets)),
        "sources_read: {:?}",
        plan.sources_read
    );
    let by_seat = |name: &str| plan.claims.iter().find(|c| c.seat.as_str() == name);
    let alpha = by_seat("alpha").expect("alpha seeded from targets.json");
    assert_eq!(alpha.verified_how, Some(VerificationKind::MachineChecked));
    let alpha_aliases: Vec<&str> = alpha.aliases.iter().map(SeatName::as_str).collect();
    assert!(alpha_aliases.contains(&"alpha-t3"), "{alpha_aliases:?}");
    assert!(alpha_aliases.contains(&"alpha-old"), "{alpha_aliases:?}");
    assert_eq!(
        by_seat("gamma").expect("gamma seeded").verified_how,
        Some(VerificationKind::MachineChecked)
    );
    assert!(by_seat("delta").is_none(), "non-object entries are skipped");
    assert!(
        plan.claims
            .iter()
            .all(|c| !c.seat.as_str().starts_with('_')),
        "`_` annotation keys are never seats"
    );
}

#[test]
fn defer_kind_hydrates_from_reason_kind_and_from_legacy_reason_lines() {
    use rex_wave_mailbridge::{DeferKind, DeferReason};
    let sha = rex_wave_api::Sha256Hex::parse("ab".repeat(32)).unwrap();
    let defer = |reason: DeferReason| RingDecision::Defer {
        seat: seat("gamma"),
        referent: RingReferent::new("mail/y.md", sha.clone()).unwrap(),
        reason,
        deadline_at: UnixMs(99),
        time_basis: TimeBasis::LetterIdMs,
    };
    let busy = |n| DeferReason::TargetBusy { pass_count: n };
    assert_eq!(busy(3).kind(), DeferKind::TargetBusy);
    let none = AliasMap::empty();

    let mut first = ShadowWatchTracker::new();
    let line = first
        .observe_pass(&[defer(busy(0))], &none, UnixMs(1))
        .unwrap()
        .jsonl;
    let mut parsed: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(parsed["reason_kind"], "TargetBusy");
    assert_eq!(parsed["reason"], "TargetBusy { pass_count: 0 }");

    let legacy = {
        parsed.as_object_mut().unwrap().remove("reason_kind");
        format!("{parsed}\n")
    };
    for log in [line.clone(), legacy] {
        let mut t = ShadowWatchTracker::from_existing_jsonl(&log).unwrap();
        let same_kind = t.observe_pass(&[defer(busy(7))], &none, UnixMs(2)).unwrap();
        assert_eq!(same_kind.decision_lines, 0, "{log}");
        let grace = DeferReason::ReconnectGrace {
            ready_at: UnixMs(50),
        };
        let new_kind = t.observe_pass(&[defer(grace)], &none, UnixMs(3)).unwrap();
        assert_eq!(new_kind.decision_lines, 1, "{log}");
    }

    let bogus = line.replace("\"TargetBusy\"", "\"Bogus\"");
    assert!(ShadowWatchTracker::from_existing_jsonl(&bogus).is_err());
}
