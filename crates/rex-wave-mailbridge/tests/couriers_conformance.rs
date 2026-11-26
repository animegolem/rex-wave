//! Conformance & end-to-end integration tests for PR #33:
//! - `proptest` state-machine conformance for `specs/mail/MailHub.tla`
//!   (`NoDoubleRing`, `RightSeat`, `LoudGiveUp`, `NoFalseGiveUp`, `Settles`,
//!   plus planted mutants `pinned_name`, `no_key`, `silent_giveup`).
//! - `proptest` state-machine conformance for `specs/mail/MailMachines.tla`
//!   (`OneMachineRings`, `NoDoubleRing`, `DegradedLoud`, `NoLostLetter`,
//!   `NoRingAfterAck`, union sync parity, plus planted mutants `retry_unknown`,
//!   `no_degrade`, `ring_any_machine`, `sync_overwrite`, `ring_after_ack`).
//! - End-to-end integration tests with real executable `0755` `couriers.d/` scripts:
//!   1. `PendingCanary -> canary ring -> confirm -> Live -> letter rung once -> acked (no re-ring)`.
//!   2. First-occurrence `refused: address-gone` -> immediate `Expired` binding, zero retries,
//!      loud escalation to `hub_owner_seat` + sender (`mail check` + inbox), zero Operator
//!      interrupting cards, and passive `SidebarTreeSnapshot.mail_health` / `courier_health_and_failures`.
//!   3. 3 consecutive `unknown` outcomes -> counted as rung (never double-rung), raises
//!      `AdapterDegraded` to `hub_owner_seat`, `degraded: true` in health snapshot, zero Operator cards,
//!      and clean `rung` resets `consecutive_unknown`.
//!   4. Multi-machine `machine_id` filtering (`OneMachineRings`), `(claimed_at, machine_id)` LWW
//!      route sync + union sync, `SelfWatcher` exclusion, router-enforced timeout process-group
//!      reaping, and reference `examples/couriers.d/` smoke verification.

use proptest::prelude::*;
use rex_wave_api::config::{CourierHealthStatus, RexConfig};
use rex_wave_api::mail::*;
use rex_wave_api::notify::{InboxList, NotifyKind};
use rex_wave_api::presence::*;
use rex_wave_api::*;
use rex_wave_fs::Protection;
use rex_wave_mailbridge::{
    CourierHub, CourierHubConfig, DeliveryClaim, MachineMailSyncState, MailBridge, MailState,
    Presence, PresenceState, RouteClaim, RouteSyncTable, SandboxRoot, invoke_courier_script,
    select_binding_for_delivery_on_machine,
};
use rex_wave_notify::{Herald, Inbox};
use rex_wave_rooms::Rooms;
use rex_wave_runtime::{
    DecideCtx, Decider, EntryMeta, Hub, LocalClient, NoAgentFeed, ReadSide, Registry, Router,
    SidebarProjection, spawn_reactor,
};
use rex_wave_store::Journal;
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::watch;

fn seat(s: &str) -> SeatName {
    SeatName::parse(s).expect("valid seat")
}

fn inc(s: &str) -> IncarnationId {
    IncarnationId::parse(s).expect("valid incarnation")
}

fn sha(n: u64) -> Sha256Hex {
    Sha256Hex::parse(format!("{n:064x}")).expect("valid sha256")
}

fn read_text(path: &Path) -> std::io::Result<String> {
    rex_wave_fs::read_regular_string(path, 1024 * 1024)
}

fn write_executable_script(dir: &Path, name: &str, content: &str) -> PathBuf {
    std::fs::create_dir_all(dir).expect("create couriers dir");
    let path = dir.join(name);
    std::fs::write(&path, content).expect("write courier script");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 courier script");
    path
}

// =============================================================================
// 1. `specs/mail/MailHub.tla` proptest conformance + planted mutants
// =============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MailHubMutant {
    None,
    PinnedName,
    NoKey,
    SilentGiveUp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LetterStatus {
    New,
    Rung,
    Acked,
    Refused,
    Dropped,
}

#[derive(Clone, Debug)]
enum MailHubOp {
    Start { seat_idx: usize, name_idx: usize },
    Fork { parent_slot: usize },
    Rename { sess_slot: usize, name_idx: usize },
    End { sess_slot: usize },
    Canary { seat_idx: usize },
    Deliver { letter_idx: usize },
    Ack { letter_idx: usize },
}

fn mailhub_op_strategy() -> impl Strategy<Value = MailHubOp> {
    prop_oneof![
        (0usize..2, 0usize..2)
            .prop_map(|(seat_idx, name_idx)| MailHubOp::Start { seat_idx, name_idx }),
        (0usize..8).prop_map(|parent_slot| MailHubOp::Fork { parent_slot }),
        (0usize..8, 0usize..2).prop_map(|(sess_slot, name_idx)| MailHubOp::Rename {
            sess_slot,
            name_idx,
        }),
        (0usize..8).prop_map(|sess_slot| MailHubOp::End { sess_slot }),
        (0usize..2).prop_map(|seat_idx| MailHubOp::Canary { seat_idx }),
        (0usize..3).prop_map(|letter_idx| MailHubOp::Deliver { letter_idx }),
        (0usize..3).prop_map(|letter_idx| MailHubOp::Ack { letter_idx }),
    ]
}

#[derive(Clone, Debug)]
struct MailHubSim {
    mutant: MailHubMutant,
    max_tries: u32,
    presence: PresenceState,
    seq: u64,
    clock: u64,
    next_sess: u64,
    seats: Vec<SeatName>,
    names: Vec<&'static str>,
    // Environment truth: session_id -> (seat_or_none_for_fork, display_name, live)
    sess_seat: BTreeMap<IncarnationId, Option<SeatName>>,
    sess_name: BTreeMap<IncarnationId, &'static str>,
    sess_live: BTreeSet<IncarnationId>,
    sess_order: Vec<IncarnationId>,
    // Track pinned name (only used by PinnedName mutant)
    pinned_name: BTreeMap<SeatName, &'static str>,
    // Per-letter hub state
    to: Vec<SeatName>,
    st: Vec<LetterStatus>,
    rings: Vec<u32>,
    ring_to: Vec<BTreeSet<IncarnationId>>,
    tries: Vec<u32>,
    loud: BTreeSet<usize>,
    false_giveup: BTreeSet<usize>,
    // MailState key ledger (`rung_keys`)
    rung_keys: BTreeSet<(SeatName, Sha256Hex)>,
}

impl MailHubSim {
    fn new(mutant: MailHubMutant) -> Self {
        let alice = seat("alice");
        let bob = seat("bob");
        Self {
            mutant,
            max_tries: 3,
            presence: PresenceState::default(),
            seq: 0,
            clock: 1_000,
            next_sess: 1,
            seats: vec![alice.clone(), bob.clone()],
            names: vec!["desk-alpha", "desk-beta"],
            sess_seat: BTreeMap::new(),
            sess_name: BTreeMap::new(),
            sess_live: BTreeSet::new(),
            sess_order: Vec::new(),
            pinned_name: BTreeMap::new(),
            to: vec![alice.clone(), bob, alice],
            st: vec![LetterStatus::New; 3],
            rings: vec![0; 3],
            ring_to: vec![BTreeSet::new(); 3],
            tries: vec![0; 3],
            loud: BTreeSet::new(),
            false_giveup: BTreeSet::new(),
            rung_keys: BTreeSet::new(),
        }
    }

    fn apply_presence(&mut self, by: Principal, cmd: PresenceCommand) -> bool {
        self.clock += 10;
        let ctx = DecideCtx::for_test(by.clone(), UnixMs(self.clock));
        let Ok(events) = Presence::decide_with_probe(&self.presence, &cmd, &ctx, |_, _| true)
        else {
            return false;
        };
        for ev in &events {
            self.seq += 1;
            let meta = EntryMeta {
                seq: Seq(self.seq),
                at: UnixMs(self.clock),
                by: by.clone(),
            };
            Presence::evolve(&mut self.presence, ev, &meta);
        }
        true
    }

    fn reachable(&self, s: &SeatName) -> bool {
        let now = UnixMs(self.clock);
        let Some(b) = select_binding_for_delivery_on_machine(
            &self.presence,
            s,
            None,
            None,
            &rex_wave_mailbridge::IncarnationAffinity::default(),
            Some("local"),
            now,
        ) else {
            return false;
        };
        self.sess_live.contains(&b.incarnation_id)
            && self.sess_seat.get(&b.incarnation_id) == Some(&Some(s.clone()))
    }

    fn target_for(&self, letter_idx: usize) -> Vec<IncarnationId> {
        let recipient = &self.to[letter_idx];
        if self.mutant == MailHubMutant::PinnedName {
            let Some(&pin) = self.pinned_name.get(recipient) else {
                return Vec::new();
            };
            return self
                .sess_live
                .iter()
                .filter(|id| self.sess_name.get(*id) == Some(&pin))
                .cloned()
                .collect();
        }
        let now = UnixMs(self.clock);
        match select_binding_for_delivery_on_machine(
            &self.presence,
            recipient,
            None,
            None,
            &rex_wave_mailbridge::IncarnationAffinity::default(),
            Some("local"),
            now,
        ) {
            Some(b) if self.sess_live.contains(&b.incarnation_id) => {
                vec![b.incarnation_id.clone()]
            }
            _ => Vec::new(),
        }
    }

    fn step(&mut self, op: MailHubOp) {
        match op {
            MailHubOp::Start { seat_idx, name_idx } => {
                let s = self.seats[seat_idx % self.seats.len()].clone();
                let n = self.names[name_idx % self.names.len()];
                let id = inc(&format!("sess-{}", self.next_sess));
                self.next_sess += 1;
                self.sess_seat.insert(id.clone(), Some(s.clone()));
                self.sess_name.insert(id.clone(), n);
                self.sess_live.insert(id.clone());
                self.sess_order.push(id.clone());
                self.pinned_name.entry(s.clone()).or_insert(n);
                // Bind with require_canary: true (starts PendingCanary in real Presence decider!)
                let _ = self.apply_presence(
                    Principal::Seat { seat: s.clone() },
                    PresenceCommand::PresenceBind(PresenceBind {
                        seat: s,
                        incarnation_id: Some(id.clone()),
                        machine_id: Some("local".into()),
                        lane: None,
                        endpoint: TransportEndpoint::T3Thread {
                            thread_id: T3ThreadId::parse(format!("thr-{}", id.as_str())).unwrap(),
                        },
                        liveness: LivenessMode::LeaseOnly,
                        ttl_ms: Some(3_600_000),
                        takeover: true,
                        allow_fork: false,
                        require_canary: true,
                        mail_horizon_at: Some(UnixMs(0)),
                        idem: None,
                    }),
                );
            }
            MailHubOp::Fork { parent_slot } => {
                if self.sess_order.is_empty() {
                    return;
                }
                let parent = self.sess_order[parent_slot % self.sess_order.len()].clone();
                if !self.sess_live.contains(&parent) {
                    return;
                }
                let inherited_name = self.sess_name[&parent];
                let child = inc(&format!("fork-{}", self.next_sess));
                self.next_sess += 1;
                // Fork has seatOf[child] = Fork (represented as None) and inherits parent's name!
                self.sess_seat.insert(child.clone(), None);
                self.sess_name.insert(child.clone(), inherited_name);
                self.sess_live.insert(child.clone());
                self.sess_order.push(child);
            }
            MailHubOp::Rename {
                sess_slot,
                name_idx,
            } => {
                if self.sess_order.is_empty() {
                    return;
                }
                let id = self.sess_order[sess_slot % self.sess_order.len()].clone();
                if self.sess_live.contains(&id) {
                    self.sess_name
                        .insert(id, self.names[name_idx % self.names.len()]);
                }
            }
            MailHubOp::End { sess_slot } => {
                if self.sess_order.is_empty() {
                    return;
                }
                let id = self.sess_order[sess_slot % self.sess_order.len()].clone();
                if self.sess_live.remove(&id)
                    && let Some(Some(s)) = self.sess_seat.get(&id).cloned()
                {
                    let _ = self.apply_presence(
                        Principal::System {
                            part: SystemPart::PresenceMonitor,
                        },
                        PresenceCommand::PresenceExpire(PresenceExpire {
                            seat: s,
                            incarnation_id: id,
                            expected_epoch: None,
                            lease_expires_at: None,
                            only_if_pending_canary: false,
                            reason: "session ended".into(),
                        }),
                    );
                }
            }
            MailHubOp::Canary { seat_idx } => {
                let s = self.seats[seat_idx % self.seats.len()].clone();
                let pending = self
                    .presence
                    .pending_canary_bindings_for_machine("local", UnixMs(self.clock))
                    .into_iter()
                    .find(|b| b.seat == s && self.sess_live.contains(&b.incarnation_id))
                    .cloned();
                if let Some(b) = pending
                    && let Some(tok) = b.canary_token
                {
                    let _ = self.apply_presence(
                        Principal::Seat { seat: s.clone() },
                        PresenceCommand::PresenceConfirmCanary(PresenceConfirmCanary {
                            seat: s,
                            incarnation_id: b.incarnation_id,
                            token: tok,
                        }),
                    );
                }
            }
            MailHubOp::Deliver { letter_idx } => {
                let l = letter_idx % self.to.len();
                let recipient = self.to[l].clone();
                let key = (recipient.clone(), sha(l as u64 + 1));
                let targets = self.target_for(l);

                let can_ring = (self.st[l] == LetterStatus::New && !self.rung_keys.contains(&key))
                    || (self.mutant == MailHubMutant::NoKey && self.st[l] == LetterStatus::Rung);
                if can_ring && self.rings[l] < 3 && !targets.is_empty() {
                    // Pick a target (preferring a fork if PinnedName mutant exposed one, to catch bug fast)
                    let chosen = targets
                        .iter()
                        .find(|t| self.sess_seat.get(*t) != Some(&Some(recipient.clone())))
                        .unwrap_or(&targets[0])
                        .clone();
                    self.ring_to[l].insert(chosen);
                    self.rings[l] += 1;
                    self.st[l] = LetterStatus::Rung;
                    if self.mutant != MailHubMutant::NoKey {
                        self.rung_keys.insert(key);
                    }
                    return;
                }
                if self.st[l] == LetterStatus::New && targets.is_empty() {
                    if self.tries[l] < self.max_tries {
                        self.tries[l] += 1;
                    } else {
                        if self.mutant == MailHubMutant::SilentGiveUp {
                            self.st[l] = LetterStatus::Dropped;
                        } else {
                            self.st[l] = LetterStatus::Refused;
                            self.loud.insert(l);
                        }
                        if self.reachable(&recipient) {
                            self.false_giveup.insert(l);
                        }
                    }
                }
            }
            MailHubOp::Ack { letter_idx } => {
                let l = letter_idx % self.to.len();
                if self.st[l] == LetterStatus::Rung {
                    self.st[l] = LetterStatus::Acked;
                }
            }
        }
    }

    fn settle_all(&mut self) {
        for l in 0..self.to.len() {
            for _ in 0..=self.max_tries + 1 {
                if matches!(
                    self.st[l],
                    LetterStatus::Acked | LetterStatus::Refused | LetterStatus::Dropped
                ) {
                    break;
                }
                if self.st[l] == LetterStatus::Rung {
                    self.step(MailHubOp::Ack { letter_idx: l });
                } else {
                    self.step(MailHubOp::Deliver { letter_idx: l });
                }
            }
        }
    }

    fn check_invariants(&self) -> Result<(), String> {
        for l in 0..self.to.len() {
            if self.rings[l] > 1 {
                return Err(format!(
                    "NoDoubleRing violated for letter {l}: rings={}",
                    self.rings[l]
                ));
            }
            for x in &self.ring_to[l] {
                if self.sess_seat.get(x) != Some(&Some(self.to[l].clone())) {
                    return Err(format!(
                        "RightSeat violated for letter {l}: rung session {x} belongs to {:?}, expected {}",
                        self.sess_seat.get(x),
                        self.to[l]
                    ));
                }
            }
            if matches!(self.st[l], LetterStatus::Refused | LetterStatus::Dropped)
                && !self.loud.contains(&l)
            {
                return Err(format!("LoudGiveUp violated for letter {l}"));
            }
        }
        if !self.false_giveup.is_empty() {
            return Err(format!("NoFalseGiveUp violated: {:?}", self.false_giveup));
        }
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn mailhub_tla_invariants_hold_for_production_presence_and_canary_routes(
        ops in prop::collection::vec(mailhub_op_strategy(), 1..80)
    ) {
        let mut sim = MailHubSim::new(MailHubMutant::None);
        for op in ops {
            sim.step(op);
            prop_assert_eq!(sim.check_invariants(), Ok(()));
        }
        sim.settle_all();
        prop_assert_eq!(sim.check_invariants(), Ok(()));
        for l in 0..sim.to.len() {
            prop_assert!(matches!(sim.st[l], LetterStatus::Acked | LetterStatus::Refused));
        }
    }
}

#[test]
fn mailhub_tla_planted_mutants_violate_their_target_invariants() {
    // 1. `pinned_name`: fork inherits parent's name -> violates RightSeat
    let mut pinned = MailHubSim::new(MailHubMutant::PinnedName);
    pinned.step(MailHubOp::Start {
        seat_idx: 0,
        name_idx: 0,
    });
    pinned.step(MailHubOp::Fork { parent_slot: 0 });
    pinned.step(MailHubOp::Deliver { letter_idx: 0 });
    let err = pinned.check_invariants().unwrap_err();
    assert!(
        err.contains("RightSeat"),
        "expected RightSeat violation, got: {err}"
    );

    // 2. `no_key`: ringing without (seat, sha256) key -> violates NoDoubleRing
    let mut no_key = MailHubSim::new(MailHubMutant::NoKey);
    no_key.step(MailHubOp::Start {
        seat_idx: 0,
        name_idx: 0,
    });
    no_key.step(MailHubOp::Canary { seat_idx: 0 });
    no_key.step(MailHubOp::Deliver { letter_idx: 0 });
    no_key.step(MailHubOp::Deliver { letter_idx: 0 });
    let err = no_key.check_invariants().unwrap_err();
    assert!(
        err.contains("NoDoubleRing"),
        "expected NoDoubleRing violation, got: {err}"
    );

    // 3. `silent_giveup`: drops after MaxTries without loud escalation -> violates LoudGiveUp
    let mut silent = MailHubSim::new(MailHubMutant::SilentGiveUp);
    for _ in 0..=silent.max_tries {
        silent.step(MailHubOp::Deliver { letter_idx: 0 });
    }
    let err = silent.check_invariants().unwrap_err();
    assert!(
        err.contains("LoudGiveUp"),
        "expected LoudGiveUp violation, got: {err}"
    );
}

// =============================================================================
// 2. `specs/mail/MailMachines.tla` proptest conformance + planted mutants
// =============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MailMachinesMutant {
    None,
    RingAnyMachine,
    RetryUnknown,
    NoDegrade,
    SyncOverwrite,
    RingAfterAck,
}

#[derive(Clone, Debug)]
enum MachineOp {
    Write {
        letter_idx: usize,
        machine_idx: usize,
    },
    Claim {
        seat_idx: usize,
        machine_idx: usize,
    },
    Break {
        seat_idx: usize,
    },
    Sync {
        from_idx: usize,
        to_idx: usize,
    },
    Ring {
        machine_idx: usize,
        letter_idx: usize,
    },
    Ack {
        letter_idx: usize,
    },
}

fn machine_op_strategy() -> impl Strategy<Value = MachineOp> {
    prop_oneof![
        (0usize..6, 0usize..3).prop_map(|(letter_idx, machine_idx)| MachineOp::Write {
            letter_idx,
            machine_idx,
        }),
        (0usize..2, 0usize..3).prop_map(|(seat_idx, machine_idx)| MachineOp::Claim {
            seat_idx,
            machine_idx,
        }),
        (0usize..2).prop_map(|seat_idx| MachineOp::Break { seat_idx }),
        (0usize..3, 0usize..3).prop_map(|(from_idx, to_idx)| MachineOp::Sync { from_idx, to_idx }),
        (0usize..3, 0usize..6).prop_map(|(machine_idx, letter_idx)| MachineOp::Ring {
            machine_idx,
            letter_idx,
        }),
        (0usize..6).prop_map(|letter_idx| MachineOp::Ack { letter_idx }),
    ]
}

struct MailMachinesCluster {
    mutant: MailMachinesMutant,
    k_threshold: u32,
    machines: Vec<MachineMailSyncState>,
    seats: Vec<SeatName>,
    to: Vec<SeatName>,
    written: BTreeSet<Sha256Hex>,
    home: BTreeMap<SeatName, String>,
    rung_by: BTreeMap<Sha256Hex, BTreeSet<String>>,
    rings: BTreeMap<Sha256Hex, u32>,
    broken: BTreeMap<SeatName, bool>,
    ghost_unk: BTreeMap<SeatName, u32>,
    ring_after_ack: BTreeSet<Sha256Hex>,
    // Production MailState folding RingAttempted / DeliveryFailed
    mail_state: MailState,
    seq: u64,
    clock: u64,
}

impl MailMachinesCluster {
    fn new(mutant: MailMachinesMutant) -> Self {
        let alice = seat("alice");
        let bob = seat("bob");
        Self {
            mutant,
            k_threshold: 3,
            machines: vec![
                MachineMailSyncState::new("m-alpha"),
                MachineMailSyncState::new("m-beta"),
                MachineMailSyncState::new("m-gamma"),
            ],
            seats: vec![alice.clone(), bob.clone()],
            to: vec![
                alice.clone(),
                alice.clone(),
                alice.clone(),
                alice,
                bob.clone(),
                bob,
            ],
            written: BTreeSet::new(),
            home: BTreeMap::new(),
            rung_by: BTreeMap::new(),
            rings: BTreeMap::new(),
            broken: BTreeMap::new(),
            ghost_unk: BTreeMap::new(),
            ring_after_ack: BTreeSet::new(),
            mail_state: MailState::default(),
            seq: 0,
            clock: 1_000,
        }
    }

    fn all_acks(&self) -> BTreeSet<(SeatName, Sha256Hex)> {
        let mut all = BTreeSet::new();
        for m in &self.machines {
            all.extend(m.acks.iter().cloned());
        }
        all
    }

    fn record_mail_event(&mut self, event: MailEvent) {
        self.seq += 1;
        self.clock += 5;
        let env = Envelope {
            stream: StreamId::Mail,
            seq: Seq(self.seq),
            at: UnixMs(self.clock),
            by: Principal::System {
                part: SystemPart::CourierHub,
            },
            event: Event::Mail(event),
        };
        self.mail_state.apply(&env);
    }

    fn step(&mut self, op: MachineOp) {
        match op {
            MachineOp::Write {
                letter_idx,
                machine_idx,
            } => {
                let l = letter_idx % self.to.len();
                let m = machine_idx % self.machines.len();
                let s = sha(l as u64 + 100);
                if self.written.insert(s.clone()) {
                    self.machines[m].messages.insert(s);
                }
            }
            MachineOp::Claim {
                seat_idx,
                machine_idx,
            } => {
                let s = self.seats[seat_idx % self.seats.len()].clone();
                if self.home.contains_key(&s) {
                    return;
                }
                let m_idx = machine_idx % self.machines.len();
                let m_id = self.machines[m_idx].machine_id.clone();
                self.clock += 10;
                let claim = RouteClaim {
                    seat: s.clone(),
                    machine_id: m_id.clone(),
                    incarnation_id: inc(&format!("inc-{s}-{m_id}")),
                    epoch: 1,
                    endpoint: TransportEndpoint::T3Thread {
                        thread_id: T3ThreadId::parse(format!("thr-{s}-{m_id}")).unwrap(),
                    },
                    claimed_at: UnixMs(self.clock),
                    confirmed: true,
                };
                self.home.insert(s, m_id);
                // Propagate the single authoritative home claim across machines.
                for machine in &mut self.machines {
                    machine.routes.apply_claim(claim.clone());
                }
            }
            MachineOp::Break { seat_idx } => {
                let s = self.seats[seat_idx % self.seats.len()].clone();
                self.broken.insert(s, true);
            }
            MachineOp::Sync { from_idx, to_idx } => {
                let a = from_idx % self.machines.len();
                let b = to_idx % self.machines.len();
                if a == b {
                    return;
                }
                let peer = self.machines[a].clone();
                if self.mutant == MailMachinesMutant::SyncOverwrite {
                    self.machines[b].messages = peer.messages;
                    self.machines[b].acks.extend(peer.acks);
                } else {
                    self.machines[b].sync_from(&peer);
                }
            }
            MachineOp::Ring {
                machine_idx,
                letter_idx,
            } => {
                let l = letter_idx % self.to.len();
                let m = machine_idx % self.machines.len();
                let m_id = self.machines[m].machine_id.clone();
                let digest = sha(l as u64 + 100);
                let recip = self.to[l].clone();

                let has_msg = self.machines[m].messages.contains(&digest);
                let acked_here = self.machines[m]
                    .acks
                    .contains(&(recip.clone(), digest.clone()));
                let Some(owner) = self.home.get(&recip).cloned() else {
                    return;
                };
                let ack_ok = self.mutant == MailMachinesMutant::RingAfterAck || !acked_here;
                let machine_ok = self.mutant == MailMachinesMutant::RingAnyMachine || owner == m_id;
                let already_rung = self
                    .mail_state
                    .rung_keys
                    .contains(&(recip.clone(), digest.clone()));
                let prev_rings = self.rings.get(&digest).copied().unwrap_or(0);
                let rung_ok = !already_rung
                    || (self.mutant == MailMachinesMutant::RetryUnknown && prev_rings < 2)
                    || (self.mutant == MailMachinesMutant::RingAnyMachine
                        && !self
                            .rung_by
                            .get(&digest)
                            .is_some_and(|set| set.contains(&m_id)));

                if !(has_msg && ack_ok && machine_ok && rung_ok) {
                    return;
                }

                let all_acks = self.all_acks();
                if all_acks.contains(&(recip.clone(), digest.clone())) {
                    self.ring_after_ack.insert(digest.clone());
                }
                self.rung_by
                    .entry(digest.clone())
                    .or_default()
                    .insert(m_id.clone());
                *self.rings.entry(digest.clone()).or_insert(0) += 1;

                let is_broken = self.broken.get(&recip).copied().unwrap_or(false);
                if is_broken {
                    *self.ghost_unk.entry(recip.clone()).or_insert(0) += 1;
                    let recorded_outcome = if self.mutant == MailMachinesMutant::NoDegrade {
                        // Mutant absorbs unknown silently as deferred (never degrades)
                        CourierOutcome::Deferred
                    } else {
                        CourierOutcome::Unknown
                    };
                    self.record_mail_event(MailEvent::RingAttempted {
                        dispatch_id: None,
                        courier: format!("courier-{recip}"),
                        seat: recip.clone(),
                        machine_id: m_id,
                        incarnation_id: None,
                        sha256s: vec![digest],
                        is_canary: false,
                        pre_spawn: false,
                        outcome: recorded_outcome,
                        reason: Some("broken".into()),
                        detail: None,
                        degraded_threshold: None,
                    });
                } else {
                    self.ghost_unk.insert(recip.clone(), 0);
                    self.record_mail_event(MailEvent::RingAttempted {
                        dispatch_id: None,
                        courier: format!("courier-{recip}"),
                        seat: recip,
                        machine_id: m_id,
                        incarnation_id: None,
                        sha256s: vec![digest],
                        is_canary: false,
                        pre_spawn: false,
                        outcome: CourierOutcome::Rung,
                        reason: None,
                        detail: None,
                        degraded_threshold: None,
                    });
                }
            }
            MachineOp::Ack { letter_idx } => {
                let l = letter_idx % self.to.len();
                let recip = self.to[l].clone();
                if self.broken.get(&recip).copied().unwrap_or(false) {
                    return;
                }
                let Some(owner) = self.home.get(&recip).cloned() else {
                    return;
                };
                let digest = sha(l as u64 + 100);
                let Some(owner_m) = self.machines.iter_mut().find(|m| m.machine_id == owner) else {
                    return;
                };
                let rung_any = self.rung_by.get(&digest).is_some_and(|set| !set.is_empty());
                if (rung_any || owner_m.messages.contains(&digest))
                    && !owner_m.acks.contains(&(recip.clone(), digest.clone()))
                {
                    owner_m.acks.insert((recip, digest));
                }
            }
        }
    }

    fn check_invariants(&self) -> Result<(), String> {
        // 1. OneMachineRings
        for (digest, ringers) in &self.rung_by {
            let l_idx =
                usize::try_from(u64::from_str_radix(digest.as_str(), 16).unwrap() - 100).unwrap();
            let recip = &self.to[l_idx];
            let owner = self.home.get(recip);
            for r in ringers {
                if owner != Some(r) {
                    return Err(format!(
                        "OneMachineRings violated: {digest} rung by {r}, owner={owner:?}"
                    ));
                }
            }
        }
        // 2. NoDoubleRing
        for (digest, &count) in &self.rings {
            if count > 1 {
                return Err(format!(
                    "NoDoubleRing violated: {digest} rung {count} times"
                ));
            }
        }
        // 3. DegradedLoud
        for s in &self.seats {
            let streak = self.ghost_unk.get(s).copied().unwrap_or(0);
            if streak >= self.k_threshold {
                let degraded = self
                    .mail_state
                    .courier_stats
                    .get(&format!("courier-{s}"))
                    .is_some_and(|st| st.degraded);
                if !degraded {
                    return Err(format!(
                        "DegradedLoud violated: seat {s} hit {streak} unknowns without degraded=true"
                    ));
                }
            }
        }
        // 4. NoLostLetter
        for digest in &self.written {
            if !self.machines.iter().any(|m| m.messages.contains(digest)) {
                return Err(format!(
                    "NoLostLetter violated: {digest} disappeared from all stores"
                ));
            }
        }
        // 5. NoRingAfterAck
        if !self.ring_after_ack.is_empty() {
            return Err(format!(
                "NoRingAfterAck violated: {:?}",
                self.ring_after_ack
            ));
        }
        Ok(())
    }

    fn sync_all_pairs(&mut self) {
        for a in 0..self.machines.len() {
            for b in 0..self.machines.len() {
                if a != b {
                    self.step(MachineOp::Sync {
                        from_idx: a,
                        to_idx: b,
                    });
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn mailmachines_tla_invariants_and_union_sync_convergence_hold(
        ops in prop::collection::vec(machine_op_strategy(), 1..100)
    ) {
        let mut cluster = MailMachinesCluster::new(MailMachinesMutant::None);
        for op in ops {
            cluster.step(op);
            prop_assert_eq!(cluster.check_invariants(), Ok(()));
        }
        cluster.sync_all_pairs();
        prop_assert_eq!(cluster.check_invariants(), Ok(()));
        // Converges: all machines agree on union of messages & acks.
        let m0 = &cluster.machines[0];
        for m in &cluster.machines[1..] {
            prop_assert_eq!(&m.messages, &m0.messages);
            prop_assert_eq!(&m.acks, &m0.acks);
            prop_assert_eq!(&m.routes, &m0.routes);
        }
    }
}

#[test]
fn mailmachines_tla_planted_mutants_violate_their_target_invariants() {
    // 1. retry_unknown violates NoDoubleRing
    let mut c = MailMachinesCluster::new(MailMachinesMutant::RetryUnknown);
    c.step(MachineOp::Claim {
        seat_idx: 0,
        machine_idx: 0,
    });
    c.step(MachineOp::Break { seat_idx: 0 });
    c.step(MachineOp::Write {
        letter_idx: 0,
        machine_idx: 0,
    });
    c.step(MachineOp::Ring {
        machine_idx: 0,
        letter_idx: 0,
    });
    c.step(MachineOp::Ring {
        machine_idx: 0,
        letter_idx: 0,
    });
    let err = c.check_invariants().unwrap_err();
    assert!(err.contains("NoDoubleRing"), "got: {err}");

    // 2. no_degrade violates DegradedLoud
    let mut c = MailMachinesCluster::new(MailMachinesMutant::NoDegrade);
    c.step(MachineOp::Claim {
        seat_idx: 0,
        machine_idx: 0,
    });
    c.step(MachineOp::Break { seat_idx: 0 });
    for l in 0..3 {
        c.step(MachineOp::Write {
            letter_idx: l,
            machine_idx: 0,
        });
        c.step(MachineOp::Ring {
            machine_idx: 0,
            letter_idx: l,
        });
    }
    let err = c.check_invariants().unwrap_err();
    assert!(err.contains("DegradedLoud"), "got: {err}");

    // 3. ring_any_machine violates OneMachineRings
    let mut c = MailMachinesCluster::new(MailMachinesMutant::RingAnyMachine);
    c.step(MachineOp::Claim {
        seat_idx: 0,
        machine_idx: 0,
    });
    c.step(MachineOp::Write {
        letter_idx: 0,
        machine_idx: 1,
    });
    c.step(MachineOp::Ring {
        machine_idx: 1,
        letter_idx: 0,
    });
    let err = c.check_invariants().unwrap_err();
    assert!(err.contains("OneMachineRings"), "got: {err}");

    // 4. sync_overwrite violates NoLostLetter
    let mut c = MailMachinesCluster::new(MailMachinesMutant::SyncOverwrite);
    c.step(MachineOp::Write {
        letter_idx: 0,
        machine_idx: 0,
    });
    c.step(MachineOp::Sync {
        from_idx: 1,
        to_idx: 0,
    });
    let err = c.check_invariants().unwrap_err();
    assert!(err.contains("NoLostLetter"), "got: {err}");

    // 5. ring_after_ack violates NoRingAfterAck
    let mut c = MailMachinesCluster::new(MailMachinesMutant::RingAfterAck);
    c.step(MachineOp::Claim {
        seat_idx: 0,
        machine_idx: 0,
    });
    c.step(MachineOp::Write {
        letter_idx: 0,
        machine_idx: 0,
    });
    c.step(MachineOp::Ack { letter_idx: 0 });
    c.step(MachineOp::Ring {
        machine_idx: 0,
        letter_idx: 0,
    });
    let err = c.check_invariants().unwrap_err();
    assert!(err.contains("NoRingAfterAck"), "got: {err}");
}

// =============================================================================
// 3. End-to-End Harness with Real Executable `couriers.d/` Scripts
// =============================================================================

struct E2eHarness {
    _tmp: TempDir,
    couriers_dir: PathBuf,
    router: Arc<Router>,
    mail_bridge: Arc<MailBridge>,
    sidebar: Arc<SidebarProjection>,
    courier_handle: rex_wave_mailbridge::CourierHubHandle,
    herald_task: tokio::task::JoinHandle<()>,
    courier_task: tokio::task::JoinHandle<()>,
}

impl E2eHarness {
    async fn new(local_machine_id: &str, timeout: Duration) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let sb_dir = tmp.path().join("sandbox");
        std::fs::create_dir_all(&sb_dir).expect("mkdir sandbox");
        std::fs::write(sb_dir.join(".rex-wave-sandbox"), b"ok\n").expect("marker");
        let couriers_dir = tmp.path().join("couriers.d");
        std::fs::create_dir_all(&couriers_dir).expect("mkdir couriers.d");

        let sandbox =
            SandboxRoot::verify_with(&sb_dir, &Protection::default()).expect("verify sandbox");
        let journal = Journal::in_memory().expect("in-memory journal");
        let hub = Hub::new(1024);
        let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());

        let mut cfg = RexConfig::default();
        cfg.mail.couriers_dir = Some(couriers_dir.clone());
        cfg.mail.hub_owner_seat = seat("postmaster");
        let (_cfg_tx, cfg_rx) = watch::channel(cfg);

        let house = HouseProfile::default();
        let presence = Presence::with_house_profile(house.clone())
            .with_machine_id(local_machine_id.to_owned());
        let presence_svc = presence.registry(journal.clone(), hub.clone());
        let room_svc = Registry::<Rooms>::new(journal.clone(), hub.clone());
        let mail_writer = journal.claim(StreamId::Mail).expect("claim mail stream");
        let mail_bridge = Arc::new(
            MailBridge::with_hub(sandbox, journal.clone(), mail_writer, hub.clone())
                .with_room_service(room_svc.clone())
                .with_config_watch(cfg_rx.clone())
                .with_house_profile(house.clone()),
        );

        let router = Arc::new(Router {
            room: room_svc,
            inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
            mail: mail_bridge.clone(),
            presence: presence_svc,
            read: Arc::new(ReadSide {
                journal,
                hub,
                sidebar: sidebar.clone(),
                agents: Arc::new(NoAgentFeed),
            }),
            ..Router::unavailable_with(Arc::new(ReadSide {
                journal: Journal::in_memory().expect("unused"),
                hub: Hub::new(8),
                sidebar: sidebar.clone(),
                agents: Arc::new(NoAgentFeed),
            }))
        });

        let herald = Herald::with_grace(Duration::from_millis(50))
            .with_operator_surface_seat(house.operator_seat.clone());
        let herald_client = LocalClient::new(
            router.clone(),
            Principal::System {
                part: SystemPart::Herald,
            },
        );
        let herald_task = spawn_reactor(herald, herald_client);

        let courier_cfg = CourierHubConfig::new(local_machine_id, house, seat("owner"))
            .with_default_couriers_dir(Some(couriers_dir.clone()))
            .with_timeout(timeout)
            .with_protection(Protection::default());
        let courier_hub = CourierHub::new(courier_cfg, cfg_rx);
        let courier_handle = courier_hub.handle();
        let courier_client = LocalClient::new(
            router.clone(),
            Principal::System {
                part: SystemPart::CourierHub,
            },
        );
        let courier_task = spawn_reactor(courier_hub, courier_client);

        // Give spawned reactors a moment to subscribe to the hub.
        tokio::time::sleep(Duration::from_millis(30)).await;

        Self {
            _tmp: tmp,
            couriers_dir,
            router,
            mail_bridge,
            sidebar,
            courier_handle,
            herald_task,
            courier_task,
        }
    }

    fn client(&self, p: Principal) -> LocalClient {
        LocalClient::new(self.router.clone(), p)
    }

    fn operator(&self) -> LocalClient {
        self.client(Principal::Operator {
            seat: seat("owner"),
        })
    }

    fn seat_client(&self, name: &str) -> LocalClient {
        self.client(Principal::Seat { seat: seat(name) })
    }

    async fn sweep_now(&self) {
        let sys = self.client(Principal::System {
            part: SystemPart::CourierHub,
        });
        self.courier_handle.sweep(&sys).await;
    }
}

impl Drop for E2eHarness {
    fn drop(&mut self) {
        self.herald_task.abort();
        self.courier_task.abort();
    }
}

async fn wait_for<F, Fut>(timeout: Duration, mut pred: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if pred().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for condition"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn read_full_http_request(stream: &mut tokio::net::TcpStream) -> String {
    use tokio::io::AsyncReadExt as _;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut expected_total: Option<usize> = None;
    loop {
        let Ok(Ok(n)) = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut chunk)).await
        else {
            break;
        };
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if expected_total.is_none()
            && let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n")
        {
            let headers = String::from_utf8_lossy(&buf[..pos]);
            let content_len = headers
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.trim()
                        .eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            expected_total = Some(pos + 4 + content_len);
        }
        if let Some(total) = expected_total
            && buf.len() >= total
        {
            break;
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

#[tokio::test]
async fn e2e_canary_lifecycle_gates_mail_until_confirmed_then_rings_once_body_blind() {
    let h = E2eHarness::new("node-a", Duration::from_secs(5)).await;
    let log_file = h._tmp.path().join("t3-thread.jsonl");
    write_executable_script(
        &h.couriers_dir,
        "t3-thread",
        &format!(
            "#!/usr/bin/env python3\nimport json, sys\nclaim = json.load(sys.stdin)\nwith open({log_path:?}, 'a', encoding='utf-8') as f:\n    f.write(json.dumps(claim) + '\\n')\nprint(json.dumps({{'outcome': 'rung'}}))\n",
            log_path = log_file.to_string_lossy()
        ),
    );

    let bob = h.seat_client("bob");
    let alice = h.seat_client("alice");

    // 1. Bind bob with --require-canary -> starts in PendingCanary with minted token.
    let bound = bob
        .call(PresenceBind {
            seat: seat("bob"),
            incarnation_id: Some(inc("inc-bob-canary")),
            machine_id: Some("node-a".into()),
            lane: None,
            endpoint: TransportEndpoint::T3Thread {
                thread_id: T3ThreadId::parse("thr-bob-1").unwrap(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: true,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .expect("bind pending canary");
    assert_eq!(bound.status, BindingStatus::PendingCanary);
    assert!(
        bound.canary_token.is_none(),
        "canary_token must be redacted from PresenceBind reply so only the target endpoint receives it"
    );
    let listed = bob
        .call(PresenceList {
            project: None,
            live_only: false,
            include_retired: true,
        })
        .await
        .unwrap();
    assert!(
        listed
            .seats
            .iter()
            .flat_map(|s| &s.bindings)
            .all(|b| b.canary_token.is_none()),
        "canary_token must be redacted from PresenceList snapshot"
    );
    let raw_presence_page = bob
        .call(rex_wave_api::read::Read {
            stream: StreamId::Presence,
            room: None,
            after: Seq::ORIGIN,
            cursor: None,
            limit: Some(50),
        })
        .await
        .unwrap();
    for env in &raw_presence_page.entries {
        if let Event::Presence(PresenceEvent::Bound { binding, .. }) = &env.event {
            assert!(
                binding.canary_token.is_none(),
                "canary_token must be redacted from non-System StreamId::Presence reads"
            );
        }
    }

    // 2. Send a letter from alice -> bob BEFORE canary is confirmed.
    let secret_body = "TOP-SECRET-BODY-NEVER-IN-COURIER-CLAIM";
    let sent = alice
        .call(MailSend {
            to: vec![seat("bob")],
            cc: vec![],
            subject: "Canary gated letter".into(),
            body: secret_body.into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("canary".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .expect("send letter");

    // Wait for canary ring to be logged.
    wait_for(Duration::from_secs(3), || {
        let p = log_file.clone();
        async move {
            read_text(&p)
                .map(|s| s.lines().count() >= 1)
                .unwrap_or(false)
        }
    })
    .await;
    h.sweep_now().await;

    let lines_before: Vec<serde_json::Value> = read_text(&log_file)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(
        lines_before.len(),
        1,
        "only the canary ring may fire while PendingCanary (normal mail must wait)"
    );
    let canary_token = lines_before[0]["canary_token"]
        .as_str()
        .expect("canary_token delivered out-of-band to courier endpoint")
        .to_owned();
    assert!(canary_token.starts_with("rwc_"));
    assert_eq!(lines_before[0]["referents"].as_array().unwrap().len(), 0);

    // 3. Wrong canary token is rejected; binding stays PendingCanary.
    let bad_confirm = bob
        .call(PresenceConfirmCanary {
            seat: seat("bob"),
            incarnation_id: inc("inc-bob-canary"),
            token: "rwc_wrong".into(),
        })
        .await;
    assert!(matches!(bad_confirm, Err(ApiError::Denied { .. })));

    // 4. Confirm exact canary token -> PendingCanary -> Live!
    let confirmed = bob
        .call(PresenceConfirmCanary {
            seat: seat("bob"),
            incarnation_id: inc("inc-bob-canary"),
            token: canary_token,
        })
        .await
        .expect("confirm canary");
    assert_eq!(confirmed.status, BindingStatus::Live);
    assert!(confirmed.canary_token.is_none());

    // 5. Normal letter is now rung ONCE with body-blind claim!
    wait_for(Duration::from_secs(3), || {
        let p = log_file.clone();
        async move {
            read_text(&p)
                .map(|s| s.lines().count() >= 2)
                .unwrap_or(false)
        }
    })
    .await;

    // 6. Ack the letter and sweep repeatedly -> MUST NOT re-ring!
    bob.call(MailAck {
        path: None,
        sha256: Some(sent.sha256.clone()),
        seat: Some(seat("bob")),
        note: Some("read".into()),
    })
    .await
    .expect("ack letter");
    h.sweep_now().await;
    h.sweep_now().await;

    let raw_log = read_text(&log_file).unwrap();
    assert!(
        !raw_log.contains(secret_body),
        "DeliveryClaim MUST be strictly body-blind!"
    );
    let claims: Vec<DeliveryClaim> = raw_log
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(
        claims.len(),
        2,
        "1 canary ring + 1 mail ring, never re-rung"
    );
    assert_eq!(claims[1].referents.len(), 1);
    assert_eq!(claims[1].referents[0].sha256, sent.sha256);
    assert_eq!(claims[1].canary_token, None);
}

#[tokio::test]
async fn e2e_first_occurrence_refused_address_gone_retires_binding_escalates_silently_for_operator()
{
    let h = E2eHarness::new("node-a", Duration::from_secs(5)).await;
    let count_file = h._tmp.path().join("refused.count");
    write_executable_script(
        &h.couriers_dir,
        "t3-thread",
        &format!(
            "#!/usr/bin/env python3\nimport json, sys\n_ = json.load(sys.stdin)\nwith open({cnt:?}, 'a', encoding='utf-8') as f:\n    f.write('1\\n')\nprint(json.dumps({{'outcome': 'refused', 'reason': 'address-gone', 'detail': 'thread archived'}}))\n",
            cnt = count_file.to_string_lossy()
        ),
    );

    let bob = h.seat_client("bob");
    let alice = h.seat_client("alice");
    let postmaster = h.seat_client("postmaster");
    let operator = h.operator();

    // Seed operator visibility with a room open so Herald tracks Operator.
    let _ = operator
        .call(rex_wave_api::room::RoomEnsure {
            room: rex_wave_api::room::RoomRef::Channel {
                scope: rex_wave_api::room::RoomScope::Global,
                name: ChannelName::parse("ops").unwrap(),
            },
            origin: None,
            topic: None,
        })
        .await
        .unwrap();

    bob.call(PresenceBind {
        seat: seat("bob"),
        incarnation_id: Some(inc("inc-bob-dead")),
        machine_id: Some("node-a".into()),
        lane: None,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-bob-gone").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: false,
        allow_fork: false,
        require_canary: false,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .expect("bind bob");

    alice
        .call(MailSend {
            to: vec![seat("bob")],
            cc: vec![],
            subject: "Ping dead thread".into(),
            body: "Hello bob".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("dead-route".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .expect("send letter");

    // Wait for dead binding to be expired on first refusal.
    wait_for(Duration::from_secs(3), || {
        let bob_c = h.seat_client("bob");
        async move {
            let snap = bob_c
                .call(PresenceList {
                    project: None,
                    live_only: false,
                    include_retired: true,
                })
                .await
                .unwrap();
            snap.seats
                .iter()
                .find(|s| s.record.seat == seat("bob"))
                .and_then(|s| s.bindings.first())
                .is_some_and(|b| b.status == BindingStatus::Expired)
        }
    })
    .await;

    // Repeated sweeps MUST NOT retry the refused delivery.
    h.sweep_now().await;
    h.sweep_now().await;
    let invocations = read_text(&count_file).unwrap().lines().count();
    assert_eq!(
        invocations, 1,
        "first-occurrence refused must have zero retries"
    );

    // Hub owner (`postmaster`) and sender (`alice`) received loud inbox notifications & mail notices.
    wait_for(Duration::from_secs(3), || {
        let pm = h.seat_client("postmaster");
        let al = h.seat_client("alice");
        async move {
            let pm_in = pm
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            let al_in = al
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            let has_pm = pm_in.items.iter().any(|i| {
                matches!(
                    &i.draft.kind,
                    NotifyKind::MailDeliveryFailed {
                        failure_kind: DeliveryFailureKind::Refused,
                        reason,
                        ..
                    } if reason == "address-gone"
                )
            });
            let has_al = al_in.items.iter().any(|i| {
                matches!(
                    &i.draft.kind,
                    NotifyKind::MailDeliveryFailed {
                        failure_kind: DeliveryFailureKind::Refused,
                        reason,
                        ..
                    } if reason == "address-gone"
                )
            });
            has_pm && has_al
        }
    })
    .await;

    // Sender `mail check` surfaces the refused delivery notice!
    let alice_check = alice
        .call(MailCheck {
            seat: Some(seat("alice")),
            include_all: true,
            limit: None,
        })
        .await
        .unwrap();
    assert!(
        alice_check
            .delivery_notices
            .iter()
            .any(|n| n.seat == seat("bob")
                && n.status == "refused"
                && n.notice.contains("address-gone")),
        "sender mail check must report refused address-gone: {:?}",
        alice_check.delivery_notices
    );
    let pm_check = postmaster
        .call(MailCheck {
            seat: Some(seat("postmaster")),
            include_all: true,
            limit: None,
        })
        .await
        .unwrap();
    assert!(
        pm_check
            .messages
            .iter()
            .any(|m| m.subject.contains("Delivery refused") && m.subject.contains("address-gone")),
        "hub_owner_seat must receive dissolved refusal notice letter: {:?}",
        pm_check.messages
    );

    // Wait beyond Herald's 50ms grace window and assert ZERO Operator interrupting cards!
    tokio::time::sleep(Duration::from_millis(120)).await;
    let op_inbox = operator
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .unwrap();
    assert!(
        op_inbox.items.is_empty() && op_inbox.unread == 0,
        "Operator MUST receive zero interrupting cards for address-gone refusal: {:?}",
        op_inbox.items
    );

    // Passive sidebar badge + courier health counters reflect the refusal without touching InboxBadge/OSC 7501.
    wait_for(Duration::from_secs(2), || {
        let sb = h.sidebar.clone();
        let op_p = Principal::Operator {
            seat: seat("owner"),
        };
        async move { sb.snapshot(&op_p).mail_health.failed_deliveries >= 1 }
    })
    .await;
    let sb_snap = h.sidebar.snapshot(&Principal::Operator {
        seat: seat("owner"),
    });
    assert_eq!(sb_snap.inbox.unread, 0);
    assert_eq!(sb_snap.inbox.blocking, 0);
    assert!(sb_snap.mail_health.failed_deliveries >= 1);

    let (health, failures) = h
        .mail_bridge
        .courier_health_and_failures(&[("t3-thread".into(), true)], 20)
        .await;
    assert_eq!(
        health,
        vec![CourierHealthStatus {
            name: "t3-thread".into(),
            executable: true,
            rung: 0,
            unknown: 0,
            refused: 1,
            degraded: false,
        }]
    );
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].kind, DeliveryFailureKind::Refused);
    assert_eq!(failures[0].reason, "address-gone");
}

#[tokio::test]
async fn e2e_three_consecutive_unknowns_mark_degraded_without_double_ring_or_operator_interrupt() {
    let h = E2eHarness::new("node-a", Duration::from_secs(5)).await;
    let count_file = h._tmp.path().join("unknown.count");
    let mode_file = h._tmp.path().join("adapter.mode");
    std::fs::write(&mode_file, "broken").unwrap();
    write_executable_script(
        &h.couriers_dir,
        "bb-thread",
        &format!(
            "#!/usr/bin/env python3\nimport json, pathlib, sys\nclaim = json.load(sys.stdin)\nwith open({cnt:?}, 'a', encoding='utf-8') as f:\n    f.write(claim['referents'][0]['sha256'] + '\\n')\nmode = pathlib.Path({mode:?}).read_text().strip()\nif mode == 'broken':\n    sys.stdout.write('NOT-JSON-GARBAGE\\n')\n    sys.exit(1)\nprint(json.dumps({{'outcome': 'rung'}}))\n",
            cnt = count_file.to_string_lossy(),
            mode = mode_file.to_string_lossy()
        ),
    );

    let bob = h.seat_client("bob");
    let alice = h.seat_client("alice");
    let operator = h.operator();

    bob.call(PresenceBind {
        seat: seat("bob"),
        incarnation_id: Some(inc("inc-bob-bb")),
        machine_id: Some("node-a".into()),
        lane: None,
        endpoint: TransportEndpoint::BbThread {
            thread_id: BbThreadId::parse("bb-thr-1").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: false,
        allow_fork: false,
        require_canary: false,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .expect("bind bob");

    // Send 3 separate letters one-by-one so each triggers a separate broken courier ring.
    for i in 1..=3 {
        alice
            .call(MailSend {
                to: vec![seat("bob")],
                cc: vec![],
                subject: format!("Unknown ring #{i}"),
                body: format!("Letter body #{i}"),
                from: None,
                priority: LetterPriority::Normal,
                topic: Some(format!("unk-{i}")),
                in_reply_to: None,
                refs: vec![],
                wait_export: false,
                idem: None,
            })
            .await
            .unwrap();
        wait_for(Duration::from_secs(3), || {
            let p = count_file.clone();
            async move {
                read_text(&p)
                    .map(|s| s.lines().count() >= i)
                    .unwrap_or(false)
            }
        })
        .await;
    }

    // Extra sweeps must NEVER re-ring any of the 3 letters (`unknown` counts as rung).
    h.sweep_now().await;
    h.sweep_now().await;
    let rung_shas: Vec<String> = read_text(&count_file)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(rung_shas.len(), 3, "unknown outcomes must never be retried");
    let unique: BTreeSet<&String> = rung_shas.iter().collect();
    assert_eq!(unique.len(), 3, "each letter rung exactly once");

    // Wait for AdapterDegraded escalation on postmaster + degraded=true in health & sidebar.
    wait_for(Duration::from_secs(3), || {
        let pm = h.seat_client("postmaster");
        async move {
            let page = pm
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            page.items.iter().any(|n| {
                matches!(
                    n.draft.kind,
                    NotifyKind::MailDeliveryFailed {
                        failure_kind: DeliveryFailureKind::AdapterDegraded,
                        ..
                    }
                )
            })
        }
    })
    .await;

    let (health, failures) = h
        .mail_bridge
        .courier_health_and_failures(&[("bb-thread".into(), true)], 20)
        .await;
    assert_eq!(health[0].unknown, 3);
    assert!(
        health[0].degraded,
        "3 consecutive unknowns must mark degraded=true"
    );
    assert!(
        failures
            .iter()
            .any(|f| f.kind == DeliveryFailureKind::AdapterDegraded && f.courier == "bb-thread")
    );

    let op_inbox = operator
        .call(InboxList {
            include_done: true,
            limit: None,
        })
        .await
        .unwrap();
    assert!(
        op_inbox.items.is_empty(),
        "Operator must receive zero cards for AdapterDegraded: {:?}",
        op_inbox.items
    );

    wait_for(Duration::from_secs(2), || {
        let sb = h.sidebar.clone();
        let op_p = Principal::Operator {
            seat: seat("owner"),
        };
        async move { sb.snapshot(&op_p).mail_health.degraded_couriers == 1 }
    })
    .await;

    // Recover adapter -> 4th letter rings cleanly and resets consecutive_unknown / degraded!
    std::fs::write(&mode_file, "ok").unwrap();
    alice
        .call(MailSend {
            to: vec![seat("bob")],
            cc: vec![],
            subject: "Recovered ring #4".into(),
            body: "Back online".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("recovered".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .unwrap();
    wait_for(Duration::from_secs(3), || {
        let mb = h.mail_bridge.clone();
        async move {
            let (health, _) = mb
                .courier_health_and_failures(&[("bb-thread".into(), true)], 20)
                .await;
            health.first().is_some_and(|c| c.rung == 1 && !c.degraded)
        }
    })
    .await;
}

#[tokio::test]
async fn e2e_multi_machine_filtering_self_watcher_exclusion_lww_sync_and_timeout_reaping() {
    let h = E2eHarness::new("machine-a", Duration::from_secs(5)).await;
    let ring_log = h._tmp.path().join("rings.log");
    write_executable_script(
        &h.couriers_dir,
        "t3-thread",
        &format!(
            "#!/usr/bin/env python3\nimport json, sys\nclaim = json.load(sys.stdin)\nwith open({log:?}, 'a', encoding='utf-8') as f:\n    f.write(claim['seat'] + ':' + claim['machine_id'] + '\\n')\nprint(json.dumps({{'outcome': 'rung'}}))\n",
            log = ring_log.to_string_lossy()
        ),
    );

    let bob = h.seat_client("bob");
    let carol = h.seat_client("carol");
    let alice = h.seat_client("alice");

    // 1. Bob binds on remote `machine-b` -> local `machine-a` hub MUST NOT ring bob (`OneMachineRings`).
    bob.call(PresenceBind {
        seat: seat("bob"),
        incarnation_id: Some(inc("inc-bob-remote")),
        machine_id: Some("machine-b".into()),
        lane: None,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-bob-remote").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: false,
        allow_fork: false,
        require_canary: false,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .unwrap();

    // 2. Carol binds as SelfWatcher (`own_knocker`) -> external push couriers MUST NOT be invoked.
    carol
        .call(PresenceBind {
            seat: seat("carol"),
            incarnation_id: Some(inc("inc-carol-self")),
            machine_id: Some("machine-a".into()),
            lane: None,
            endpoint: TransportEndpoint::SelfWatcher {
                harness: "custom-knocker".into(),
                thread_id: None,
                pid: None,
                proc_start: None,
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();

    alice
        .call(MailSend {
            to: vec![seat("bob"), seat("carol")],
            cc: vec![],
            subject: "Cross-machine + self-watcher check".into(),
            body: "Verify filtering".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("multi-machine".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .unwrap();
    h.sweep_now().await;
    assert!(
        !ring_log.exists(),
        "neither remote machine-b binding nor SelfWatcher may invoke local push couriers"
    );

    // 3. Bob migrates/takes over onto local `machine-a` -> local hub rings bob immediately!
    bob.call(PresenceBind {
        seat: seat("bob"),
        incarnation_id: Some(inc("inc-bob-local")),
        machine_id: Some("machine-a".into()),
        lane: None,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-bob-local").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: true,
        allow_fork: false,
        require_canary: false,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .unwrap();
    wait_for(Duration::from_secs(3), || {
        let p = ring_log.clone();
        async move {
            read_text(&p)
                .map(|s| s.contains("bob:machine-a"))
                .unwrap_or(false)
        }
    })
    .await;

    // 4. `(claimed_at, machine_id)` LWW route table + local UDS drop without waiting for sync.
    let mut table_a = RouteSyncTable::new();
    let mut table_b = RouteSyncTable::new();
    let claim_old = RouteClaim {
        seat: seat("bob"),
        machine_id: "machine-a".into(),
        incarnation_id: inc("inc-1"),
        epoch: 1,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-1").unwrap(),
        },
        claimed_at: UnixMs(100),
        confirmed: true,
    };
    let claim_tie = RouteClaim {
        seat: seat("bob"),
        machine_id: "machine-b".into(),
        incarnation_id: inc("inc-2"),
        epoch: 1,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-2").unwrap(),
        },
        claimed_at: UnixMs(100),
        confirmed: true,
    };
    let claim_newer = RouteClaim {
        seat: seat("bob"),
        machine_id: "machine-a".into(),
        incarnation_id: inc("inc-3"),
        epoch: 2,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-3").unwrap(),
        },
        claimed_at: UnixMs(200),
        confirmed: true,
    };
    table_a.apply_claim(claim_old);
    table_b.apply_claim(claim_tie);
    table_a.merge_from(&table_b);
    assert_eq!(
        table_a.owner_machine(&seat("bob")),
        Some("machine-b"),
        "equal claimed_at breaks tie deterministically by machine_id"
    );
    table_a.apply_claim(claim_newer);
    table_b.merge_from(&table_a);
    assert_eq!(
        table_b.owner_machine(&seat("bob")),
        Some("machine-a"),
        "newer claimed_at supersedes stale cross-machine route"
    );
    assert!(table_a.drop_local(&seat("bob"), "machine-a"));
    assert_eq!(
        table_a.owner_machine(&seat("bob")),
        Some("machine-b"),
        "dropping machine-a preserves machine-b's unwithdrawn live claim"
    );
    assert!(table_a.drop_local(&seat("bob"), "machine-b"));
    assert_eq!(table_a.owner_machine(&seat("bob")), None);

    // 5. Router-enforced timeout kills hanging process group cleanly and returns Unknown.
    let child_pid_file = h._tmp.path().join("hang-child.pid");
    let hang_script = write_executable_script(
        &h.couriers_dir,
        "hang-test",
        &format!(
            "#!/usr/bin/env sh\n\
             sleep 30 &\n\
             echo $! > {pidfile:?}\n\
             wait\n",
            pidfile = child_pid_file.to_string_lossy()
        ),
    );
    let dummy_claim = DeliveryClaim {
        schema: 1,
        seat: seat("bob"),
        machine_id: "machine-a".into(),
        incarnation_id: inc("inc-hang"),
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-hang").unwrap(),
        },
        referents: vec![],
        canary_token: None,
    };
    let reply =
        invoke_courier_script(&hang_script, &dummy_claim, Duration::from_millis(200), None).await;
    assert_eq!(reply.outcome, CourierOutcome::Unknown);
    assert_eq!(reply.reason.as_deref(), Some("timeout"));

    // Verify the spawned background sleep inside the process group was actually reaped!
    tokio::time::sleep(Duration::from_millis(100)).await;
    if let Ok(pid_txt) = read_text(&child_pid_file)
        && let Ok(pid) = pid_txt.trim().parse::<u32>()
    {
        let alive = std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .is_ok_and(|s| s.success());
        assert!(
            !alive,
            "timeout reaping must terminate background child in process group"
        );
    }
}

#[tokio::test]
async fn reference_courier_scripts_enforce_body_blind_dry_run_and_claude_safety_rules() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let examples_dir = manifest_dir.join("../../examples/couriers.d");

    for name in [
        "t3-thread",
        "claude-session",
        "codex-app-server",
        "bb-thread",
        "macos-toast",
    ] {
        let p = examples_dir.join(name);
        let meta = std::fs::metadata(&p).unwrap_or_else(|e| panic!("missing {}: {e}", p.display()));
        assert_ne!(
            meta.permissions().mode() & 0o111,
            0,
            "{name} must be executable (0755)"
        );
    }

    // Verify claude-session refuses fork marker `⑂` and dead PIDs cleanly.
    let claude_script = examples_dir.join("claude-session");
    let fork_claim = DeliveryClaim {
        schema: 1,
        seat: seat("bob"),
        machine_id: "node-a".into(),
        incarnation_id: inc("inc-fork"),
        endpoint: TransportEndpoint::ClaudeSession {
            pid: std::process::id(),
            proc_start: "now".into(),
            started_at_ms: 1,
            session_id: "sid-1".into(),
            messaging_socket: PathBuf::from("/tmp/unused.sock"),
            kind: "interactive".into(),
            name_source: Some("user".into()),
            display_name: Some("⑂ Bob-fork".into()),
        },
        referents: vec![],
        canary_token: None,
    };
    let reply =
        invoke_courier_script(&claude_script, &fork_claim, Duration::from_secs(3), None).await;
    assert_eq!(reply.outcome, CourierOutcome::Refused);
    assert_eq!(reply.reason.as_deref(), Some("fork-session"));

    // Verify claude-session refuses `name-shared` when two live sessions share a display name.
    let tmp = tempfile::tempdir().unwrap();
    let my_pid = std::process::id();
    std::fs::write(
        tmp.path().join(format!("{my_pid}.json")),
        serde_json::json!({
            "pid": my_pid,
            "sessionId": "sid-primary",
            "name": "SharedDesk"
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        tmp.path().join("999999.json"),
        serde_json::json!({
            "pid": my_pid,
            "sessionId": "sid-collision",
            "name": "SharedDesk"
        })
        .to_string(),
    )
    .unwrap();
    let shared_claim = DeliveryClaim {
        schema: 1,
        seat: seat("bob"),
        machine_id: "node-a".into(),
        incarnation_id: inc("inc-shared"),
        endpoint: TransportEndpoint::ClaudeSession {
            pid: my_pid,
            proc_start: "now".into(),
            started_at_ms: 1,
            session_id: "sid-primary".into(),
            messaging_socket: tmp.path().join("sock"),
            kind: "interactive".into(),
            name_source: Some("user".into()),
            display_name: Some("SharedDesk".into()),
        },
        referents: vec![],
        canary_token: None,
    };
    let mut cmd = tokio::process::Command::new(&claude_script);
    cmd.env("CLAUDE_SESSIONS_DIR", tmp.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    {
        use tokio::io::AsyncWriteExt as _;
        let mut stdin = child.stdin.take().unwrap();
        stdin
            .write_all(&serde_json::to_vec(&shared_claim).unwrap())
            .await
            .unwrap();
    }
    let out = child.wait_with_output().await.unwrap();
    let parsed = rex_wave_mailbridge::parse_courier_stdout(&out.stdout);
    assert_eq!(parsed.outcome, CourierOutcome::Refused);
    assert_eq!(parsed.reason.as_deref(), Some("name-shared"));

    // Verify BELL_GUARD=1 defers claude-session cleanly.
    let mut cmd2 = tokio::process::Command::new(&claude_script);
    cmd2.env("BELL_GUARD", "1")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    let mut child2 = cmd2.spawn().unwrap();
    {
        use tokio::io::AsyncWriteExt as _;
        let mut stdin = child2.stdin.take().unwrap();
        stdin
            .write_all(&serde_json::to_vec(&shared_claim).unwrap())
            .await
            .unwrap();
    }
    let out2 = child2.wait_with_output().await.unwrap();
    let parsed2 = rex_wave_mailbridge::parse_courier_stdout(&out2.stdout);
    assert_eq!(parsed2.outcome, CourierOutcome::Deferred);
    assert_eq!(parsed2.reason.as_deref(), Some("bell-guard"));
}

// =============================================================================
// 4. Round 2 Hardening Regressions (CRDT Semilattice, Pipe Drain, Crash Recovery,
//    Concurrent ACK / Refusal Fencing, Rebind Delivery, Deferred Backoff,
//    Canary Redaction, Multi-Machine Multi-Lane, & Reference Adapter Wire Contracts)
// =============================================================================

#[derive(Clone, Debug)]
enum RouteTableOp {
    Claim {
        seat_idx: usize,
        machine_idx: usize,
        inc_idx: usize,
        ts: u64,
        confirmed: bool,
    },
    Drop {
        seat_idx: usize,
        machine_idx: usize,
        inc_idx: usize,
        ts: u64,
    },
}

fn route_table_op_strategy() -> impl Strategy<Value = RouteTableOp> {
    prop_oneof![
        (0usize..2, 0usize..3, 0usize..4, 1u64..5, any::<bool>()).prop_map(
            |(seat_idx, machine_idx, inc_idx, ts, confirmed)| RouteTableOp::Claim {
                seat_idx,
                machine_idx,
                inc_idx,
                ts: ts * 100,
                confirmed,
            }
        ),
        (0usize..2, 0usize..3, 0usize..4, 1u64..5).prop_map(
            |(seat_idx, machine_idx, inc_idx, ts)| RouteTableOp::Drop {
                seat_idx,
                machine_idx,
                inc_idx,
                ts: ts * 100,
            }
        ),
    ]
}

fn build_route_table_from_ops(ops: &[RouteTableOp]) -> RouteSyncTable {
    let seats = [seat("alice"), seat("bob")];
    let machines = ["m-alpha", "m-beta", "m-gamma"];
    let mut table = RouteSyncTable::new();
    for op in ops {
        match op {
            RouteTableOp::Claim {
                seat_idx,
                machine_idx,
                inc_idx,
                ts,
                confirmed,
            } => {
                let s = seats[seat_idx % seats.len()].clone();
                let m = machines[machine_idx % machines.len()];
                let i = inc(&format!("inc-{inc_idx}"));
                table.apply_claim(RouteClaim {
                    seat: s,
                    machine_id: m.to_owned(),
                    incarnation_id: i,
                    epoch: *inc_idx as u64 + 1,
                    endpoint: TransportEndpoint::T3Thread {
                        thread_id: T3ThreadId::parse(format!("thr-{inc_idx}")).unwrap(),
                    },
                    claimed_at: UnixMs(*ts),
                    confirmed: *confirmed,
                });
            }
            RouteTableOp::Drop {
                seat_idx,
                machine_idx,
                inc_idx,
                ts,
            } => {
                let s = seats[seat_idx % seats.len()].clone();
                let m = machines[machine_idx % machines.len()];
                let i = inc(&format!("inc-{inc_idx}"));
                table.drop_local_at(&s, m, i, UnixMs(*ts));
            }
        }
    }
    table
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(192))]

    #[test]
    fn route_sync_table_is_commutative_associative_idempotent_semilattice_with_withdrawals(
        ops_a in prop::collection::vec(route_table_op_strategy(), 0..20),
        ops_b in prop::collection::vec(route_table_op_strategy(), 0..20),
        ops_c in prop::collection::vec(route_table_op_strategy(), 0..20),
    ) {
        let a = build_route_table_from_ops(&ops_a);
        let b = build_route_table_from_ops(&ops_b);
        let c = build_route_table_from_ops(&ops_c);

        // Idempotence: A ⊔ A == A
        let mut idem = a.clone();
        idem.merge_from(&a);
        prop_assert_eq!(&idem, &a);

        // Commutativity: A ⊔ B == B ⊔ A (even with equal (claimed_at, machine_id) & withdrawals!)
        let mut ab = a.clone();
        ab.merge_from(&b);
        let mut ba = b.clone();
        ba.merge_from(&a);
        prop_assert_eq!(&ab, &ba);

        // Associativity: (A ⊔ B) ⊔ C == A ⊔ (B ⊔ C)
        let mut abc_left = ab.clone();
        abc_left.merge_from(&c);
        let mut bc = b.clone();
        bc.merge_from(&c);
        let mut abc_right = a.clone();
        abc_right.merge_from(&bc);
        prop_assert_eq!(&abc_left, &abc_right);
    }
}

#[test]
fn route_sync_table_withdrawal_tombstone_prevents_stale_peer_resurrection() {
    let mut local = RouteSyncTable::new();
    let mut stale_peer = RouteSyncTable::new();
    let claim = RouteClaim {
        seat: seat("bob"),
        machine_id: "m-alpha".into(),
        incarnation_id: inc("inc-bob-1"),
        epoch: 1,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-bob-1").unwrap(),
        },
        claimed_at: UnixMs(1_000),
        confirmed: true,
    };
    local.apply_claim(claim.clone());
    stale_peer.apply_claim(claim);
    assert!(local.drop_local(&seat("bob"), "m-alpha"));
    assert_eq!(local.owner_machine(&seat("bob")), None);

    // Merging stale_peer into local MUST NOT resurrect the withdrawn claim!
    local.merge_from(&stale_peer);
    assert_eq!(local.owner_machine(&seat("bob")), None);

    // Merging local into stale_peer propagates the withdrawal!
    stale_peer.merge_from(&local);
    assert_eq!(stale_peer.owner_machine(&seat("bob")), None);
}

#[tokio::test]
async fn pipe_holding_grandchild_and_sigterm_ignoring_descendant_never_stall_courier_hub() {
    let tmp = tempfile::tempdir().unwrap();
    let guard = rex_wave_fs::ProtectedRootsGuard::resolve(&Protection::default()).unwrap();
    let dummy_claim = DeliveryClaim {
        schema: 1,
        seat: seat("bob"),
        machine_id: "node-a".into(),
        incarnation_id: inc("inc-pipe"),
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-pipe").unwrap(),
        },
        referents: vec![],
        canary_token: None,
    };

    // 1. Leader exits in milliseconds after spawning a background child that holds stdout/stderr open.
    let leak_pid_file = tmp.path().join("leak-grandchild.pid");
    let leak_script = write_executable_script(
        tmp.path(),
        "pipe-leak",
        &format!(
            "#!/usr/bin/env sh\n\
             sleep 30 &\n\
             echo $! > {pidfile:?}\n\
             printf '{{\"outcome\":\"rung\"}}\\n'\n\
             exit 0\n",
            pidfile = leak_pid_file.to_string_lossy()
        ),
    );
    let started = tokio::time::Instant::now();
    let reply = invoke_courier_script(
        &leak_script,
        &dummy_claim,
        Duration::from_secs(5),
        Some(&guard),
    )
    .await;
    assert_eq!(reply.outcome, CourierOutcome::Rung);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "pipe-leaking grandchild must be drained/reaped within grace, took {:?}",
        started.elapsed()
    );
    tokio::time::sleep(Duration::from_millis(80)).await;
    if let Ok(pid_txt) = read_text(&leak_pid_file)
        && let Ok(pid) = pid_txt.trim().parse::<u32>()
    {
        let alive = std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .is_ok_and(|s| s.success());
        assert!(!alive, "background pipe-holding grandchild must be reaped");
    }

    // 2. Timeout where leader exits promptly on SIGTERM while a grandchild ignores SIGTERM.
    let ign_pid_file = tmp.path().join("ign-grandchild.pid");
    let ign_script = write_executable_script(
        tmp.path(),
        "sigterm-ignore",
        &format!(
            "#!/usr/bin/env sh\n\
             (trap '' TERM; sleep 30) &\n\
             echo $! > {pidfile:?}\n\
             sleep 30\n",
            pidfile = ign_pid_file.to_string_lossy()
        ),
    );
    let reply2 = invoke_courier_script(
        &ign_script,
        &dummy_claim,
        Duration::from_millis(150),
        Some(&guard),
    )
    .await;
    assert_eq!(reply2.outcome, CourierOutcome::Unknown);
    assert_eq!(reply2.reason.as_deref(), Some("timeout"));
    tokio::time::sleep(Duration::from_millis(80)).await;
    if let Ok(pid_txt) = read_text(&ign_pid_file)
        && let Ok(pid) = pid_txt.trim().parse::<u32>()
    {
        let alive = std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .is_ok_and(|s| s.success());
        assert!(
            !alive,
            "SIGTERM-ignoring descendant must be killed by unconditional group SIGKILL"
        );
    }
}

#[tokio::test]
async fn e2e_refusal_fences_batch33_rebind_delivers_and_concurrent_ack_skips_ring() {
    let h = E2eHarness::new("node-a", Duration::from_secs(5)).await;
    let inv_log = h._tmp.path().join("invocations.jsonl");
    let mode_file = h._tmp.path().join("route.mode");
    std::fs::write(&mode_file, "refuse-dead").unwrap();

    write_executable_script(
        &h.couriers_dir,
        "t3-thread",
        &format!(
            r#"#!/usr/bin/env python3
import json, pathlib, sys, time
claim = json.load(sys.stdin)
mode = pathlib.Path({mode:?}).read_text().strip()
if claim['seat'] == 'carol':
    time.sleep(0.25)
with open({log:?}, 'a', encoding='utf-8') as f:
    f.write(json.dumps({{'seat': claim['seat'], 'inc': claim['incarnation_id'], 'n': len(claim['referents']), 'canary': bool(claim.get('canary_token'))}}) + '\n')
if mode == 'refuse-dead' and claim['incarnation_id'] == 'inc-bob-dead':
    print(json.dumps({{'outcome': 'refused', 'reason': 'address-gone', 'detail': 'dead thread'}}))
else:
    print(json.dumps({{'outcome': 'rung'}}))
"#,
            mode = mode_file.to_string_lossy(),
            log = inv_log.to_string_lossy()
        ),
    );

    let alice = h.seat_client("alice");
    let bob = h.seat_client("bob");
    let carol = h.seat_client("carol");
    let dave = h.seat_client("dave");

    // Part A: 33 letters (> max_referents_per_ring=32) on dead inc-bob-dead:
    // First batch of 32 is refused -> dead epoch fenced & expired -> 33rd is NOT rung on inc-bob-dead!
    // Then bob binds a new live session inc-bob-live-2 -> ALL 33 unacked letters ARE rung on inc-bob-live-2!
    for i in 1..=33 {
        let sender = if i % 2 == 0 { &alice } else { &carol };
        sender
            .call(MailSend {
                to: vec![seat("bob")],
                cc: vec![],
                subject: format!("Batch letter #{i}"),
                body: format!("Body #{i}"),
                from: None,
                priority: LetterPriority::Normal,
                topic: Some(format!("batch-{i}")),
                in_reply_to: None,
                refs: vec![],
                wait_export: false,
                idem: None,
            })
            .await
            .unwrap();
    }

    bob.call(PresenceBind {
        seat: seat("bob"),
        incarnation_id: Some(inc("inc-bob-dead")),
        machine_id: Some("node-a".into()),
        lane: None,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-bob-dead").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: false,
        allow_fork: false,
        require_canary: false,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .unwrap();

    wait_for(Duration::from_secs(3), || {
        let p = inv_log.clone();
        async move {
            read_text(&p)
                .map(|s| s.lines().count() >= 1)
                .unwrap_or(false)
        }
    })
    .await;
    h.sweep_now().await;

    let dead_calls: Vec<serde_json::Value> = read_text(&inv_log)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .filter(|v: &serde_json::Value| v["inc"] == "inc-bob-dead")
        .collect();
    assert_eq!(
        dead_calls.len(),
        1,
        "dead binding must be invoked ONCE for the first 32 letters and never again for the 33rd: {dead_calls:?}"
    );
    assert_eq!(dead_calls[0]["n"], 32);

    // Also verify courier_stats.refused incremented ONCE (even with 2 distinct senders alice & carol!),
    // and escalation MailSend.refs carry full 64-char sha256 strings!
    let (health, failures) = h
        .mail_bridge
        .courier_health_and_failures(&[("t3-thread".into(), true)], 20)
        .await;
    assert_eq!(
        health[0].refused, 1,
        "multi-sender refusal must increment refused counter once per script run"
    );
    assert_eq!(failures.len(), 2, "one DeliveryFailed per sender group");

    // Now bob rebinds a fresh healthy session inc-bob-live-2 -> all 33 letters deliver!
    bob.call(PresenceBind {
        seat: seat("bob"),
        incarnation_id: Some(inc("inc-bob-live-2")),
        machine_id: Some("node-a".into()),
        lane: None,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-bob-live-2").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: true,
        allow_fork: false,
        require_canary: false,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .unwrap();

    wait_for(Duration::from_secs(3), || {
        let p = inv_log.clone();
        async move {
            let total_live: u64 = read_text(&p)
                .unwrap_or_default()
                .lines()
                .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
                .filter(|v| v["inc"] == "inc-bob-live-2")
                .filter_map(|v| v["n"].as_u64())
                .sum();
            total_live == 33
        }
    })
    .await;

    // Part B: Concurrent ACK while a sibling seat's courier is busy (`NoRingAfterAck`!).
    carol
        .call(PresenceBind {
            seat: seat("carol"),
            incarnation_id: Some(inc("inc-carol-slow")),
            machine_id: Some("node-a".into()),
            lane: None,
            endpoint: TransportEndpoint::T3Thread {
                thread_id: T3ThreadId::parse("thr-carol").unwrap(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();
    dave.call(PresenceBind {
        seat: seat("dave"),
        incarnation_id: Some(inc("inc-dave-fast")),
        machine_id: Some("node-a".into()),
        lane: None,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-dave").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: false,
        allow_fork: false,
        require_canary: false,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .unwrap();

    // Send one multi-recipient letter to carol (processed first alphabetically, sleeps 250ms) and dave.
    let multi = alice
        .call(MailSend {
            to: vec![seat("carol"), seat("dave")],
            cc: vec![],
            subject: "Race ACK before dave rings".into(),
            body: "Test concurrent ack".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("ack-race".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .unwrap();

    // Immediately ACK as dave while carol's 250ms courier is running!
    tokio::time::sleep(Duration::from_millis(40)).await;
    dave.call(MailAck {
        path: None,
        sha256: Some(multi.sha256.clone()),
        seat: Some(seat("dave")),
        note: Some("fast ack".into()),
    })
    .await
    .unwrap();

    wait_for(Duration::from_secs(3), || {
        let p = inv_log.clone();
        async move {
            read_text(&p)
                .unwrap_or_default()
                .lines()
                .any(|l| l.contains("\"inc-carol-slow\""))
        }
    })
    .await;
    h.sweep_now().await;

    let dave_rings = read_text(&inv_log)
        .unwrap()
        .lines()
        .filter(|l| l.contains("\"inc-dave-fast\""))
        .count();
    assert_eq!(
        dave_rings, 0,
        "dave ACKed while carol's courier was running; dave must NEVER be rung after ACK"
    );
}

#[tokio::test]
async fn e2e_deferred_backoff_canary_retry_token_redaction_and_restart_crash_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let sb_dir = tmp.path().join("sandbox");
    std::fs::create_dir_all(&sb_dir).unwrap();
    std::fs::write(sb_dir.join(".rex-wave-sandbox"), b"ok\n").unwrap();
    let couriers_dir = tmp.path().join("couriers.d");
    std::fs::create_dir_all(&couriers_dir).unwrap();

    let journal_path = tmp.path().join("journal.sqlite3");
    let journal = Journal::open(&journal_path).unwrap();

    // Pre-seed an unfinished `RingDispatched` claim + an unreconciled `DeliveryFailed`
    // in the SQLite journal BEFORE starting CourierHub to prove crash recovery!
    let pre_hub = Hub::new(1024);
    let house = HouseProfile::default();
    let presence = Presence::with_house_profile(house.clone()).with_machine_id("node-a");
    let presence_svc = presence.registry(journal.clone(), pre_hub.clone());
    let room_svc = Registry::<Rooms>::new(journal.clone(), pre_hub.clone());
    let sandbox = SandboxRoot::verify_with(&sb_dir, &Protection::default()).unwrap();
    let mail_writer = journal.claim(StreamId::Mail).unwrap();

    let mut cfg = RexConfig::default();
    cfg.mail.couriers_dir = Some(couriers_dir.clone());
    cfg.mail.hub_owner_seat = seat("postmaster");
    cfg.mail.require_canary = true;
    let (_cfg_tx, cfg_rx) = watch::channel(cfg);
    let _presence_sync = presence.spawn_config_sync(cfg_rx.clone());

    let mail_bridge = Arc::new(
        MailBridge::with_hub(sandbox, journal.clone(), mail_writer, pre_hub.clone())
            .with_room_service(room_svc.clone())
            .with_config_watch(cfg_rx.clone())
            .with_house_profile(house.clone()),
    );
    let sidebar = SidebarProjection::spawn(journal.clone(), pre_hub.clone());
    let router = Arc::new(Router {
        room: room_svc,
        inbox: Registry::<Inbox>::new(journal.clone(), pre_hub.clone()),
        mail: mail_bridge.clone(),
        presence: presence_svc,
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub: pre_hub.clone(),
            sidebar: sidebar.clone(),
            agents: Arc::new(NoAgentFeed),
        }),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal: Journal::in_memory().unwrap(),
            hub: Hub::new(8),
            sidebar: sidebar.clone(),
            agents: Arc::new(NoAgentFeed),
        }))
    });

    let alice = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: seat("alice"),
        },
    );
    let carol = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: seat("carol"),
        },
    );
    let sys_courier = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::CourierHub,
        },
    );

    // Carol binds (policy `mail.require_canary = true` puts T3Thread in PendingCanary automatically!)
    let carol_bound = carol
        .call(PresenceBind {
            seat: seat("carol"),
            incarnation_id: Some(inc("inc-carol-policy")),
            machine_id: Some("node-a".into()),
            lane: None,
            endpoint: TransportEndpoint::T3Thread {
                thread_id: T3ThreadId::parse("thr-carol-policy").unwrap(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(
        carol_bound.status,
        BindingStatus::PendingCanary,
        "mail.require_canary=true policy must gate push-courier binds even without --require-canary"
    );

    // Send a letter to `dave` and record a pre-crash `RingDispatched` (no `RingAttempted`!)
    // plus a pre-crash `DeliveryFailed` on `erin` whose `PresenceExpire`/`MailSend`/`InboxRaise` never ran!
    let dave_client = LocalClient::new(router.clone(), Principal::Seat { seat: seat("dave") });
    let erin_client = LocalClient::new(router.clone(), Principal::Seat { seat: seat("erin") });
    // SelfWatcher bypasses push canary policy; let's bind dave & erin on OperatorToast (exempt) or confirm:
    dave_client
        .call(PresenceBind {
            seat: seat("dave"),
            incarnation_id: Some(inc("inc-dave-crash")),
            machine_id: Some("node-a".into()),
            lane: None,
            endpoint: TransportEndpoint::OperatorToast {
                command: "osascript".into(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();
    erin_client
        .call(PresenceBind {
            seat: seat("erin"),
            incarnation_id: Some(inc("inc-erin-crash")),
            machine_id: Some("node-a".into()),
            lane: None,
            endpoint: TransportEndpoint::OperatorToast {
                command: "osascript".into(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();

    let crash_letter = alice
        .call(MailSend {
            to: vec![seat("dave")],
            cc: vec![],
            subject: "In-flight before crash".into(),
            body: "Should recover as unknown and NOT ring twice".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("crash".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .unwrap();
    let erin_letter = alice
        .call(MailSend {
            to: vec![seat("erin")],
            cc: vec![],
            subject: "Refused right before crash".into(),
            body: "Escalation must reconcile on restart".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("crash-refused".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .unwrap();

    // Operator caller MUST be denied `MailRecordRing` (restricted to SystemPart::CourierHub).
    let op_denied = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: seat("owner"),
        },
    )
    .call(MailRecordRing {
        phase: RingRecordPhase::Begin,
        dispatch_id: Some("disp-bad".into()),
        reconciled_failure_seq: None,
        courier: "macos-toast".into(),
        seat: seat("dave"),
        machine_id: "node-a".into(),
        incarnation_id: Some(inc("inc-dave-crash")),
        expected_epoch: Some(1),
        sha256s: vec![crash_letter.sha256.clone()],
        is_canary: false,
        pre_spawn: false,
        outcome: CourierOutcome::Deferred,
        reason: None,
        detail: None,
        sender_seat: None,
        hub_owner_seat: seat("postmaster"),
        failure_kind: None,
        failure_groups: vec![],
        degraded_threshold: Some(2),
    })
    .await;
    assert!(matches!(op_denied, Err(ApiError::Denied { .. })));

    // Persist unfinished `Begin` on dave + committed `Refused` on erin (without running follow-ups!).
    sys_courier
        .call(MailRecordRing {
            phase: RingRecordPhase::Begin,
            dispatch_id: Some("disp-pre-crash-1".into()),
            reconciled_failure_seq: None,
            courier: "macos-toast".into(),
            seat: seat("dave"),
            machine_id: "node-a".into(),
            incarnation_id: Some(inc("inc-dave-crash")),
            expected_epoch: Some(1),
            sha256s: vec![crash_letter.sha256.clone()],
            is_canary: false,
            pre_spawn: false,
            outcome: CourierOutcome::Deferred,
            reason: None,
            detail: None,
            sender_seat: None,
            hub_owner_seat: seat("postmaster"),
            failure_kind: None,
            failure_groups: vec![],
            degraded_threshold: Some(2),
        })
        .await
        .unwrap();
    sys_courier
        .call(MailRecordRing {
            phase: RingRecordPhase::Complete,
            dispatch_id: None,
            reconciled_failure_seq: None,
            courier: "macos-toast".into(),
            seat: seat("erin"),
            machine_id: "node-a".into(),
            incarnation_id: Some(inc("inc-erin-crash")),
            expected_epoch: Some(1),
            sha256s: vec![erin_letter.sha256.clone()],
            is_canary: false,
            pre_spawn: false,
            outcome: CourierOutcome::Refused,
            reason: Some("address-gone".into()),
            detail: Some("crashed before escalate".into()),
            sender_seat: Some(seat("alice")),
            hub_owner_seat: seat("postmaster"),
            failure_kind: Some(DeliveryFailureKind::Refused),
            failure_groups: vec![],
            degraded_threshold: Some(2),
        })
        .await
        .unwrap();

    // Install scripts:
    // - `macos-toast` logs any invocation (must be 0 for dave's recovered letter & 0 for erin!).
    // - `t3-thread` tests canary token redaction on first try (exiting 1 while echoing token to stderr!),
    //   then returns `deferred` on second try, then `rung` on third try!
    let toast_log = tmp.path().join("toast.log");
    write_executable_script(
        &couriers_dir,
        "macos-toast",
        &format!(
            "#!/usr/bin/env python3\nimport json, sys\nc = json.load(sys.stdin)\nwith open({log:?}, 'a') as f:\n    f.write(c['seat'] + '\\n')\nprint(json.dumps({{'outcome':'rung'}}))\n",
            log = toast_log.to_string_lossy()
        ),
    );

    let t3_state = tmp.path().join("t3.step");
    let t3_canary_out = tmp.path().join("t3.canary");
    std::fs::write(&t3_state, "0").unwrap();
    write_executable_script(
        &couriers_dir,
        "t3-thread",
        &format!(
            r#"#!/usr/bin/env python3
import json, pathlib, sys
claim = json.load(sys.stdin)
p = pathlib.Path({st:?})
step = int(p.read_text().strip()) + 1
p.write_text(str(step))
tok = claim.get('canary_token') or ''
if tok:
    pathlib.Path({cout:?}).write_text(tok)
    if step == 1:
        sys.stderr.write('boom leaked token=' + tok + '\n')
        sys.exit(1)
    if step == 2:
        print(json.dumps({{'outcome': 'deferred', 'reason': 'busy'}}))
        sys.exit(0)
    print(json.dumps({{'outcome': 'rung'}}))
    sys.exit(0)
if step <= 5:
    print(json.dumps({{'outcome': 'deferred', 'reason': 'rate-limit'}}))
else:
    print(json.dumps({{'outcome': 'rung'}}))
"#,
            st = t3_state.to_string_lossy(),
            cout = t3_canary_out.to_string_lossy()
        ),
    );

    // Boot CourierHub with fast 80ms defer_backoff and custom degraded_threshold = 2!
    let courier_cfg = CourierHubConfig::new("node-a", house, seat("owner"))
        .with_default_couriers_dir(Some(couriers_dir))
        .with_defer_backoff(Duration::from_millis(80))
        .with_degraded_threshold(2)
        .with_max_defer_tries(4)
        .with_protection(Protection::default());
    let courier_hub = CourierHub::new(courier_cfg, cfg_rx);
    let courier_task = spawn_reactor(courier_hub, sys_courier.clone());

    // 1. Verify erin's pre-crash `DeliveryFailed` reconciled `PresenceExpire` + `MailSend` (full 64-hex refs) + `InboxRaise`,
    //    AND dave's pre-crash `RingDispatched` recovered as `Unknown` with ZERO duplicate script invocations!
    wait_for(Duration::from_secs(3), || {
        let al = LocalClient::new(router.clone(), Principal::Seat { seat: seat("alice") });
        async move {
            let inb = al
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            inb.items
                .iter()
                .any(|i| matches!(&i.draft.kind, NotifyKind::MailDeliveryFailed { seat: s, .. } if s == &seat("erin")))
        }
    })
    .await;
    assert!(
        !toast_log.exists(),
        "pre-crash RingDispatched on dave and Refused on erin must NEVER invoke macos-toast again"
    );
    let pm_mail = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: seat("postmaster"),
        },
    )
    .call(MailCheck {
        seat: Some(seat("postmaster")),
        include_all: true,
        limit: None,
    })
    .await
    .unwrap();
    let full_notice_sha = pm_mail
        .messages
        .iter()
        .find(|m| m.subject.contains("@erin"))
        .expect("reconciled refusal notice to postmaster")
        .sha256
        .clone();
    let raw_mail_stream = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: seat("owner"),
        },
    )
    .call(rex_wave_api::read::Read {
        stream: StreamId::Mail,
        room: None,
        after: Seq::ORIGIN,
        cursor: None,
        limit: Some(100),
    })
    .await
    .unwrap();
    let notice_msg = raw_mail_stream
        .entries
        .iter()
        .find_map(|e| match &e.event {
            Event::Mail(MailEvent::MessageCommitted { message, .. })
                if message.sha256 == full_notice_sha =>
            {
                Some(message)
            }
            _ => None,
        })
        .expect("committed escalation notice");
    assert_eq!(
        notice_msg.refs,
        vec![erin_letter.sha256.as_str().to_owned()],
        "escalation refs must hold full 64-character sha256 strings"
    );

    // 2. Wait for carol's canary to retry across `Unknown` (step 1) -> `Deferred` (step 2) -> `Rung` (step 3) via backoff timer!
    wait_for(Duration::from_secs(3), || {
        let p = t3_state.clone();
        async move {
            read_text(&p)
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok())
                .is_some_and(|n| n >= 3)
        }
    })
    .await;

    let actual_canary = read_text(&t3_canary_out).unwrap().trim().to_owned();
    assert!(actual_canary.starts_with("rwc_"));

    // Verify the canary token leaked to stderr on step 1 was REDACTED from StreamId::Mail!
    let raw_mail_after = LocalClient::new(
        router.clone(),
        Principal::Operator {
            seat: seat("owner"),
        },
    )
    .call(rex_wave_api::read::Read {
        stream: StreamId::Mail,
        room: None,
        after: Seq::ORIGIN,
        cursor: None,
        limit: Some(200),
    })
    .await
    .unwrap();
    let serialized_mail = serde_json::to_string(&raw_mail_after.entries).unwrap();
    assert!(
        !serialized_mail.contains(&actual_canary),
        "active canary_token leaked on adapter stderr MUST be redacted before persistence!"
    );
    assert!(serialized_mail.contains("[REDACTED_CANARY]"));

    // Confirm carol's canary -> Live, then verify refreshing the SAME endpoint with require_canary=true stays Live (Opus S7)!
    let confirmed_carol = carol
        .call(PresenceConfirmCanary {
            seat: seat("carol"),
            incarnation_id: inc("inc-carol-policy"),
            token: actual_canary,
        })
        .await
        .unwrap();
    assert_eq!(confirmed_carol.status, BindingStatus::Live);

    let refreshed_carol = carol
        .call(PresenceBind {
            seat: seat("carol"),
            incarnation_id: Some(inc("inc-carol-policy")),
            machine_id: Some("node-a".into()),
            lane: None,
            endpoint: TransportEndpoint::T3Thread {
                thread_id: T3ThreadId::parse("thr-carol-policy").unwrap(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: true,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(
        refreshed_carol.status,
        BindingStatus::Live,
        "refreshing an already-confirmed identical endpoint must preserve Live status"
    );
    assert_eq!(refreshed_carol.epoch, confirmed_carol.epoch);

    // Send a letter to carol -> defers twice with 80ms backoff (never tight-looping!) and rings on 3rd try!
    let send_started = tokio::time::Instant::now();
    alice
        .call(MailSend {
            to: vec![seat("carol")],
            cc: vec![],
            subject: "Deferred then rung".into(),
            body: "Tests backoff timer".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("backoff".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .unwrap();

    wait_for(Duration::from_secs(3), || {
        let p = t3_state.clone();
        async move {
            read_text(&p)
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok())
                .is_some_and(|n| n >= 6)
        }
    })
    .await;
    assert!(
        send_started.elapsed() >= Duration::from_millis(140),
        "two deferred retries with 80ms base backoff must wait for scheduled due times"
    );

    courier_task.abort();
}

#[tokio::test]
async fn e2e_two_machines_multi_lane_ring_strictly_one_owner_and_sidebar_tracks_per_seat_degradation()
 {
    let tmp = tempfile::tempdir().unwrap();
    let sb_dir = tmp.path().join("sandbox");
    std::fs::create_dir_all(&sb_dir).unwrap();
    std::fs::write(sb_dir.join(".rex-wave-sandbox"), b"ok\n").unwrap();
    let couriers_dir = tmp.path().join("couriers.d");
    std::fs::create_dir_all(&couriers_dir).unwrap();

    let ring_log = tmp.path().join("multi-hub-rings.log");
    write_executable_script(
        &couriers_dir,
        "t3-thread",
        &format!(
            r#"#!/usr/bin/env python3
import json, sys
claim = json.load(sys.stdin)
with open({log:?}, 'a', encoding='utf-8') as f:
    f.write(claim['machine_id'] + ':' + claim['incarnation_id'] + '\n')
if claim['seat'] == 'carol':
    sys.exit(1)
print(json.dumps({{'outcome': 'rung'}}))
"#,
            log = ring_log.to_string_lossy()
        ),
    );

    let sandbox = SandboxRoot::verify_with(&sb_dir, &Protection::default()).unwrap();
    let journal = Journal::in_memory().unwrap();
    let hub = Hub::new(1024);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());

    let mut cfg = RexConfig::default();
    cfg.mail.couriers_dir = Some(couriers_dir.clone());
    cfg.mail.hub_owner_seat = seat("postmaster");
    let (_cfg_tx, cfg_rx) = watch::channel(cfg);

    let house = HouseProfile::default();
    let presence = Presence::with_house_profile(house.clone()).with_machine_id("machine-a");
    let presence_svc = presence.registry(journal.clone(), hub.clone());
    let room_svc = Registry::<Rooms>::new(journal.clone(), hub.clone());
    let mail_writer = journal.claim(StreamId::Mail).unwrap();
    let mail_bridge = Arc::new(
        MailBridge::with_hub(sandbox, journal.clone(), mail_writer, hub.clone())
            .with_room_service(room_svc.clone())
            .with_config_watch(cfg_rx.clone())
            .with_house_profile(house.clone()),
    );
    let router = Arc::new(Router {
        room: room_svc,
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: mail_bridge.clone(),
        presence: presence_svc,
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar: sidebar.clone(),
            agents: Arc::new(NoAgentFeed),
        }),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal: Journal::in_memory().unwrap(),
            hub: Hub::new(8),
            sidebar: sidebar.clone(),
            agents: Arc::new(NoAgentFeed),
        }))
    });

    // Spawn TWO CourierHubs (`machine-a` and `machine-b`) watching the exact same stream!
    let sys_c = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::CourierHub,
        },
    );
    let hub_a = CourierHub::new(
        CourierHubConfig::new("machine-a", house.clone(), seat("owner"))
            .with_default_couriers_dir(Some(couriers_dir.clone()))
            .with_degraded_threshold(2)
            .with_protection(Protection::default()),
        cfg_rx.clone(),
    );
    let hub_b = CourierHub::new(
        CourierHubConfig::new("machine-b", house, seat("owner"))
            .with_default_couriers_dir(Some(couriers_dir.clone()))
            .with_degraded_threshold(2)
            .with_protection(Protection::default()),
        cfg_rx,
    );
    let handle_a = hub_a.handle();
    let handle_b = hub_b.handle();
    let task_a = spawn_reactor(hub_a, sys_c.clone());
    let task_b = spawn_reactor(hub_b, sys_c.clone());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let bob = LocalClient::new(router.clone(), Principal::Seat { seat: seat("bob") });
    let carol = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: seat("carol"),
        },
    );
    let alice = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: seat("alice"),
        },
    );

    // Bob binds TWO valid lanes: lane-a on machine-a, and lane-b on machine-b!
    bob.call(PresenceBind {
        seat: seat("bob"),
        incarnation_id: Some(inc("inc-bob-ma")),
        machine_id: Some("machine-a".into()),
        lane: Some("lane-a".into()),
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-ma").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: false,
        allow_fork: false,
        require_canary: false,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .unwrap();
    bob.call(PresenceBind {
        seat: seat("bob"),
        incarnation_id: Some(inc("inc-bob-mb")),
        machine_id: Some("machine-b".into()),
        lane: Some("lane-b".into()),
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-mb").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: false,
        allow_fork: false,
        require_canary: false,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .unwrap();

    alice
        .call(MailSend {
            to: vec![seat("bob")],
            cc: vec![],
            subject: "Multi-lane multi-machine test".into(),
            body: "Only one machine may ring".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("one-machine".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .unwrap();

    wait_for(Duration::from_secs(3), || {
        let p = ring_log.clone();
        async move {
            read_text(&p)
                .map(|s| s.lines().count() >= 1)
                .unwrap_or(false)
        }
    })
    .await;
    handle_a.sweep(&sys_c).await;
    handle_b.sweep(&sys_c).await;

    let bob_lines: Vec<String> = read_text(&ring_log)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(
        bob_lines,
        vec!["machine-b:inc-bob-mb".to_owned()],
        "strictly ONE machine (highest epoch global winner) may ring bob: {bob_lines:?}"
    );

    // Per-seat Sidebar degradation parity (`Astra S12`):
    // Degrade `t3-thread` on seat `carol` (threshold = 2), then ring cleanly on `bob`:
    // BOTH `MailState` AND `SidebarProjection` must remain degraded while `carol` is degraded!
    carol
        .call(PresenceBind {
            seat: seat("carol"),
            incarnation_id: Some(inc("inc-carol-ma")),
            machine_id: Some("machine-a".into()),
            lane: None,
            endpoint: TransportEndpoint::T3Thread {
                thread_id: T3ThreadId::parse("thr-carol").unwrap(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();

    for idx in 1..=2 {
        alice
            .call(MailSend {
                to: vec![seat("carol")],
                cc: vec![],
                subject: format!("Carol fail #{idx}"),
                body: "Triggers per-seat unknown".into(),
                from: None,
                priority: LetterPriority::Normal,
                topic: Some(format!("cf-{idx}")),
                in_reply_to: None,
                refs: vec![],
                wait_export: false,
                idem: None,
            })
            .await
            .unwrap();
        wait_for(Duration::from_secs(3), || {
            let p = ring_log.clone();
            let want = idx + 1;
            async move {
                read_text(&p)
                    .map(|s| s.lines().count() >= want)
                    .unwrap_or(false)
            }
        })
        .await;
    }

    // Now send a clean ring to `bob` (which returns `rung`!).
    alice
        .call(MailSend {
            to: vec![seat("bob")],
            cc: vec![],
            subject: "Bob clean ring while carol degraded".into(),
            body: "Sidebar must keep degraded_couriers == 1".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("bob-ok".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .unwrap();
    wait_for(Duration::from_secs(3), || {
        let p = ring_log.clone();
        async move {
            read_text(&p)
                .map(|s| s.lines().count() >= 4)
                .unwrap_or(false)
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (health, _) = mail_bridge
        .courier_health_and_failures(&[("t3-thread".into(), true)], 10)
        .await;
    assert!(health[0].degraded);
    let sb_snap = sidebar.snapshot(&Principal::Operator {
        seat: seat("owner"),
    });
    assert_eq!(
        sb_snap.mail_health.degraded_couriers, 1,
        "Sidebar must retain degraded_couriers=1 while carol remains degraded even after bob rings cleanly"
    );

    task_a.abort();
    task_b.abort();
}

#[tokio::test]
async fn reference_adapters_speak_real_t3_cli_bb_send_and_codex_jsonrpc_contracts() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let examples_dir = manifest_dir.join("../../examples/couriers.d");
    let tmp = tempfile::tempdir_in("/tmp").unwrap();

    // 1. `t3-thread`: verify positional `<thread> -` CLI contract + `--canary` confirmation flag!
    let t3_args_log = tmp.path().join("t3-cli.json");
    let fake_t3 = write_executable_script(
        tmp.path(),
        "fake-t3",
        &format!(
            r#"#!/usr/bin/env python3
import json, sys
stdin_body = sys.stdin.read()
with open({out:?}, 'w', encoding='utf-8') as f:
    json.dump({{'argv': sys.argv[1:], 'stdin': stdin_body}}, f)
print(json.dumps({{'ok': True}}))
"#,
            out = t3_args_log.to_string_lossy()
        ),
    );
    let t3_claim = DeliveryClaim {
        schema: 1,
        seat: seat("bob"),
        machine_id: "node-a".into(),
        incarnation_id: inc("inc-t3-1"),
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("mcp:thr-xyz").unwrap(),
        },
        referents: vec![],
        canary_token: Some("rwc_testcanary999".into()),
    };
    let mut c1 = tokio::process::Command::new(examples_dir.join("t3-thread"));
    c1.env("T3_BIN", &fake_t3)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    let mut ch1 = c1.spawn().unwrap();
    {
        use tokio::io::AsyncWriteExt as _;
        let mut s = ch1.stdin.take().unwrap();
        s.write_all(&serde_json::to_vec(&t3_claim).unwrap())
            .await
            .unwrap();
    }
    let out1 = ch1.wait_with_output().await.unwrap();
    assert_eq!(
        rex_wave_mailbridge::parse_courier_stdout(&out1.stdout).outcome,
        CourierOutcome::Rung
    );
    let captured_t3: serde_json::Value =
        serde_json::from_str(&read_text(&t3_args_log).unwrap()).unwrap();
    assert_eq!(
        captured_t3["argv"],
        serde_json::json!(["thread", "send", "--json", "--", "mcp:thr-xyz", "-"])
    );
    assert!(
        captured_t3["stdin"]
            .as_str()
            .unwrap()
            .contains("--canary rwc_testcanary999"),
        "canary nudge must use --canary (never colliding with global --token)"
    );

    // 2. `bb-thread`: verify `POST /api/v1/threads/<encoded>/send` with `input` + `mode: "auto"`!
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let bb_server = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt as _;
        let (mut stream, _) = listener.accept().await.unwrap();
        let req = read_full_http_request(&mut stream).await;
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 29\r\nConnection: close\r\n\r\n{\"ok\":true,\"delivery\":\"sent\"}",
            )
            .await
            .unwrap();
        let _ = stream.shutdown().await;
        req
    });
    let bb_claim = DeliveryClaim {
        schema: 1,
        seat: seat("bob"),
        machine_id: "node-a".into(),
        incarnation_id: inc("inc-bb-1"),
        endpoint: TransportEndpoint::BbThread {
            thread_id: BbThreadId::parse("thr_bb_42").unwrap(),
        },
        referents: vec![],
        canary_token: Some("rwc_bbcanary".into()),
    };
    let mut c2 = tokio::process::Command::new(examples_dir.join("bb-thread"));
    c2.env("BB_URL", format!("http://{addr}"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    let mut ch2 = c2.spawn().unwrap();
    {
        use tokio::io::AsyncWriteExt as _;
        let mut s = ch2.stdin.take().unwrap();
        s.write_all(&serde_json::to_vec(&bb_claim).unwrap())
            .await
            .unwrap();
    }
    let out2 = ch2.wait_with_output().await.unwrap();
    assert_eq!(
        rex_wave_mailbridge::parse_courier_stdout(&out2.stdout).outcome,
        CourierOutcome::Rung
    );
    let raw_http_req = bb_server.await.unwrap();
    assert!(
        raw_http_req.starts_with("POST /api/v1/threads/thr_bb_42/send HTTP/1.1"),
        "bb-thread must POST to /api/v1/threads/:id/send, got: {raw_http_req}"
    );
    let http_body = raw_http_req.split("\r\n\r\n").nth(1).unwrap();
    let parsed_bb_body: serde_json::Value = serde_json::from_str(http_body).unwrap();
    assert_eq!(parsed_bb_body["mode"], "auto");
    assert_eq!(parsed_bb_body["input"][0]["type"], "text");
    assert!(
        parsed_bb_body["input"][0]["text"]
            .as_str()
            .unwrap()
            .contains("--canary rwc_bbcanary")
    );

    // 3. `codex-app-server`: verify RFC 6455 WebSocket upgrade + masked JSON-RPC 2.0
    //    `initialize` -> `initialized` -> `turn/start` over Unix socket (`unix://...`)!
    let codex_sock = tmp.path().join("codex-app.sock");
    let uds_listener = tokio::net::UnixListener::bind(&codex_sock).unwrap();
    let codex_server = tokio::spawn(async move {
        use base64::Engine as _;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        fn sha1_rfc3174(data: &[u8]) -> [u8; 20] {
            let mut h0: u32 = 0x6745_2301;
            let mut h1: u32 = 0xEFCD_AB89;
            let mut h2: u32 = 0x98BA_DCFE;
            let mut h3: u32 = 0x1032_5476;
            let mut h4: u32 = 0xC3D2_E1F0;
            let bit_len = (data.len() as u64) * 8;
            let mut msg = data.to_vec();
            msg.push(0x80);
            while (msg.len() % 64) != 56 {
                msg.push(0);
            }
            msg.extend_from_slice(&bit_len.to_be_bytes());
            for chunk in msg.chunks_exact(64) {
                let mut w = [0u32; 80];
                for i in 0..16 {
                    w[i] = u32::from_be_bytes([
                        chunk[i * 4],
                        chunk[i * 4 + 1],
                        chunk[i * 4 + 2],
                        chunk[i * 4 + 3],
                    ]);
                }
                for i in 16..80 {
                    w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
                }
                let (mut a, mut b, mut c, mut d, mut e) = (h0, h1, h2, h3, h4);
                for (i, &wi) in w.iter().enumerate() {
                    let (f, k) = match i {
                        0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999),
                        20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                        40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                        _ => (b ^ c ^ d, 0xCA62_C1D6),
                    };
                    let temp = a
                        .rotate_left(5)
                        .wrapping_add(f)
                        .wrapping_add(e)
                        .wrapping_add(k)
                        .wrapping_add(wi);
                    e = d;
                    d = c;
                    c = b.rotate_left(30);
                    b = a;
                    a = temp;
                }
                h0 = h0.wrapping_add(a);
                h1 = h1.wrapping_add(b);
                h2 = h2.wrapping_add(c);
                h3 = h3.wrapping_add(d);
                h4 = h4.wrapping_add(e);
            }
            let mut out = [0u8; 20];
            out[0..4].copy_from_slice(&h0.to_be_bytes());
            out[4..8].copy_from_slice(&h1.to_be_bytes());
            out[8..12].copy_from_slice(&h2.to_be_bytes());
            out[12..16].copy_from_slice(&h3.to_be_bytes());
            out[16..20].copy_from_slice(&h4.to_be_bytes());
            out
        }

        let (mut stream, _) = uds_listener.accept().await.unwrap();
        let mut hdr_buf = Vec::new();
        let mut tmp_b = [0u8; 2048];
        while !hdr_buf.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = stream.read(&mut tmp_b).await.unwrap();
            assert!(n > 0, "unexpected EOF during WS handshake");
            hdr_buf.extend_from_slice(&tmp_b[..n]);
        }
        let hdr_end = hdr_buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let hdr_text = String::from_utf8_lossy(&hdr_buf[..hdr_end]).into_owned();
        assert!(
            hdr_text.starts_with("GET / HTTP/1.1\r\n"),
            "expected HTTP WS upgrade GET, got: {hdr_text}"
        );
        let ws_key = hdr_text
            .lines()
            .find_map(|l| {
                let (k, v) = l.split_once(':')?;
                k.trim()
                    .eq_ignore_ascii_case("sec-websocket-key")
                    .then(|| v.trim().to_owned())
            })
            .expect("Sec-WebSocket-Key header");
        let mut accept_input = ws_key.into_bytes();
        accept_input.extend_from_slice(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        let accept = base64::engine::general_purpose::STANDARD.encode(sha1_rfc3174(&accept_input));
        let upgrade_resp = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        stream.write_all(upgrade_resp.as_bytes()).await.unwrap();

        let mut leftover = hdr_buf[hdr_end..].to_vec();
        async fn read_ws_exact(
            s: &mut tokio::net::UnixStream,
            buf: &mut Vec<u8>,
            n: usize,
        ) -> Vec<u8> {
            use tokio::io::AsyncReadExt as _;
            let mut chunk = [0u8; 2048];
            while buf.len() < n {
                let r = s.read(&mut chunk).await.unwrap();
                assert!(r > 0, "unexpected WS EOF");
                buf.extend_from_slice(&chunk[..r]);
            }
            buf.drain(..n).collect()
        }
        async fn write_ws_text(s: &mut tokio::net::UnixStream, text: &str) {
            use tokio::io::AsyncWriteExt as _;
            let bytes = text.as_bytes();
            let mut frame = Vec::new();
            frame.push(0x81);
            if bytes.len() < 126 {
                frame.push(bytes.len() as u8);
            } else {
                frame.push(126);
                frame.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            }
            frame.extend_from_slice(bytes);
            s.write_all(&frame).await.unwrap();
        }

        // Send a server Ping frame first to verify client Pong handling!
        stream.write_all(&[0x89, 0x02, b'h', b'i']).await.unwrap();

        let mut received = Vec::new();
        let mut saw_pong = false;
        loop {
            let h2 = read_ws_exact(&mut stream, &mut leftover, 2).await;
            let opcode = h2[0] & 0x0F;
            let masked = (h2[1] & 0x80) != 0;
            assert!(masked, "RFC 6455 client frames MUST be masked");
            let mut len = (h2[1] & 0x7F) as usize;
            if len == 126 {
                let ext = read_ws_exact(&mut stream, &mut leftover, 2).await;
                len = u16::from_be_bytes([ext[0], ext[1]]) as usize;
            }
            let mask = read_ws_exact(&mut stream, &mut leftover, 4).await;
            let mut payload = read_ws_exact(&mut stream, &mut leftover, len).await;
            for (i, b) in payload.iter_mut().enumerate() {
                *b ^= mask[i & 3];
            }
            if opcode == 0xA {
                assert_eq!(&payload, b"hi");
                saw_pong = true;
                continue;
            }
            if opcode == 0x8 {
                break;
            }
            let text = String::from_utf8(payload).unwrap();
            let msg: serde_json::Value = serde_json::from_str(&text).unwrap();
            let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
            received.push(msg.clone());
            if method == "initialize" {
                write_ws_text(
                    &mut stream,
                    r#"{"jsonrpc":"2.0","id":1,"result":{"serverInfo":{"name":"mock"}}}"#,
                )
                .await;
            } else if method == "turn/start" {
                write_ws_text(
                    &mut stream,
                    r#"{"jsonrpc":"2.0","id":2,"result":{"turnId":"t-1"}}"#,
                )
                .await;
                break;
            }
        }
        assert!(saw_pong, "client must reply to WS Ping with masked Pong");
        received
    });
    let codex_claim = DeliveryClaim {
        schema: 1,
        seat: seat("bob"),
        machine_id: "node-a".into(),
        incarnation_id: inc("inc-codex-1"),
        endpoint: TransportEndpoint::UdsSession {
            conn_id: 7,
            client: "codex:codex-thr-77".into(),
        },
        referents: vec![],
        canary_token: None,
    };
    let mut c3 = tokio::process::Command::new(examples_dir.join("codex-app-server"));
    c3.env(
        "CODEX_APP_SERVER_SOCKET",
        format!("unix://{}", codex_sock.display()),
    )
    .stdin(std::process::Stdio::piped())
    .stdout(std::process::Stdio::piped());
    let mut ch3 = c3.spawn().unwrap();
    {
        use tokio::io::AsyncWriteExt as _;
        let mut s = ch3.stdin.take().unwrap();
        s.write_all(&serde_json::to_vec(&codex_claim).unwrap())
            .await
            .unwrap();
    }
    let out3 = ch3.wait_with_output().await.unwrap();
    assert_eq!(
        rex_wave_mailbridge::parse_courier_stdout(&out3.stdout).outcome,
        CourierOutcome::Rung,
        "codex-app-server stdout={:?} stderr={:?}",
        String::from_utf8_lossy(&out3.stdout),
        String::from_utf8_lossy(&out3.stderr),
    );
    let rpc_frames = codex_server.await.unwrap();
    assert_eq!(rpc_frames.len(), 3);
    assert_eq!(rpc_frames[0]["method"], "initialize");
    assert_eq!(rpc_frames[1]["method"], "initialized");
    assert_eq!(rpc_frames[2]["method"], "turn/start");
    assert_eq!(rpc_frames[2]["params"]["threadId"], "codex-thr-77");

    // 4. Tilde expansion parity (`Astra S13`): `~/...` in `mail.couriers_dir` resolves identically!
    let mut tilde_cfg = RexConfig::default();
    tilde_cfg.mail.couriers_dir = Some(PathBuf::from("~/custom-couriers-dir"));
    let resolved_tilde =
        rex_wave_mailbridge::effective_couriers_dir_for(&tilde_cfg, None, None).unwrap();
    assert!(
        resolved_tilde.is_absolute() && !resolved_tilde.to_string_lossy().starts_with('~'),
        "effective_couriers_dir_for must expand ~/ prefix: {}",
        resolved_tilde.display()
    );

    // 5. `stage_verified_executable` closes pathname TOCTOU swap window (`Astra B5`):
    // Stage an executable, replace its original pathname with a symlink and another file
    // BEFORE spawning `staged.path()`, and prove only the original verified code runs!
    let swap_dir = tmp.path().join("swap-couriers");
    write_executable_script(
        &swap_dir,
        "t3-thread",
        "#!/usr/bin/env sh\nprintf '{\"outcome\":\"rung\",\"detail\":\"original-verified\"}\\n'\n",
    );
    let staged =
        rex_wave_fs::stage_verified_executable(&swap_dir, "t3-thread", 1024 * 1024).unwrap();
    let staged_exec_path = staged.path().to_path_buf();
    std::fs::remove_file(swap_dir.join("t3-thread")).unwrap();
    let attacker_target = write_executable_script(
        tmp.path(),
        "evil-replacement",
        "#!/usr/bin/env sh\nprintf '{\"outcome\":\"rung\",\"detail\":\"SWAPPED-ATTACKER\"}\\n'\n",
    );
    std::os::unix::fs::symlink(&attacker_target, swap_dir.join("t3-thread")).unwrap();
    let out_staged = tokio::process::Command::new(&staged_exec_path)
        .output()
        .await
        .unwrap();
    let staged_reply = rex_wave_mailbridge::parse_courier_stdout(&out_staged.stdout);
    assert_eq!(staged_reply.detail.as_deref(), Some("original-verified"));
    drop(staged);
    assert!(
        !staged_exec_path.exists(),
        "staged executable cleaned up on drop"
    );

    // And a symlink at `couriers.d/t3-thread` is rejected outright by `stage_verified_executable`:
    assert!(rex_wave_fs::stage_verified_executable(&swap_dir, "t3-thread", 1024 * 1024).is_err());
}

// =============================================================================
// 5. Round 3 Regressions (Batch-32 ACK Stall, Late Canary Race, Ungated Refresh,
//    205+ Unreconciled Recovery, Pre-Spawn Eligibility & Bounded Retries,
//    Reused Incarnation Epoch, Per-Incarnation Withdrawal, HTTP 504/404,
//    Capture-Cap Canary Redaction, & Descriptor-Anchored Ancestor Swap)
// =============================================================================

#[tokio::test]
async fn round3_batch32_acked_advances_to_33rd_and_reused_incarnation_epoch_delivers() {
    let h = E2eHarness::new("node-a", Duration::from_secs(5)).await;
    let alice_started = h._tmp.path().join("alice.started");
    let alice_release = h._tmp.path().join("alice.release");
    let bob_log = h._tmp.path().join("bob-shas.log");
    let carol_mode = h._tmp.path().join("carol.mode");
    let carol_log = h._tmp.path().join("carol-epochs.log");
    std::fs::write(&carol_mode, "refuse").unwrap();

    write_executable_script(
        &h.couriers_dir,
        "t3-thread",
        &format!(
            r#"#!/usr/bin/env python3
import json, pathlib, sys, time
claim = json.load(sys.stdin)
seat = claim['seat']
if seat == 'alice':
    pathlib.Path({started:?}).write_text('1')
    deadline = time.time() + 4.0
    while not pathlib.Path({rel:?}).exists() and time.time() < deadline:
        time.sleep(0.01)
    print(json.dumps({{'outcome': 'rung'}}))
    sys.exit(0)
if seat == 'bob':
    with open({blog:?}, 'a', encoding='utf-8') as f:
        for r in claim['referents']:
            f.write(r['sha256'] + '\n')
    print(json.dumps({{'outcome': 'rung'}}))
    sys.exit(0)
if seat == 'carol':
    mode = pathlib.Path({cmode:?}).read_text().strip()
    if mode == 'refuse':
        print(json.dumps({{'outcome': 'refused', 'reason': 'address-gone'}}))
    else:
        with open({clog:?}, 'a', encoding='utf-8') as f:
            f.write(claim['incarnation_id'] + ':' + str(len(claim['referents'])) + '\n')
        print(json.dumps({{'outcome': 'rung'}}))
    sys.exit(0)
print(json.dumps({{'outcome': 'rung'}}))
"#,
            started = alice_started.to_string_lossy(),
            rel = alice_release.to_string_lossy(),
            blog = bob_log.to_string_lossy(),
            cmode = carol_mode.to_string_lossy(),
            clog = carol_log.to_string_lossy(),
        ),
    );

    let sender = h.seat_client("dave");
    let alice = h.seat_client("alice");
    let bob = h.seat_client("bob");
    let carol = h.seat_client("carol");

    // Pre-send 33 letters to bob while bob is unbound, then bind alice & bob and trigger ONE sweep
    // where alice runs first alphabetically, holding `sweep_lock` while CourierHub's reactor has NOT
    // yet consumed the ACKs we commit for bob's first 32 letters (`Astra B1`)!
    let mut bob_shas = Vec::new();
    for i in 1..=33 {
        let s = sender
            .call(MailSend {
                to: vec![seat("bob")],
                cc: vec![],
                subject: format!("Bob batch #{i:02}"),
                body: format!("Body #{i}"),
                from: None,
                priority: LetterPriority::Normal,
                topic: Some(format!("b33-{i}")),
                in_reply_to: None,
                refs: vec![],
                wait_export: false,
                idem: None,
            })
            .await
            .unwrap();
        bob_shas.push(s.sha256);
    }

    alice
        .call(PresenceBind {
            seat: seat("alice"),
            incarnation_id: Some(inc("inc-alice-1")),
            machine_id: Some("node-a".into()),
            lane: None,
            endpoint: TransportEndpoint::T3Thread {
                thread_id: T3ThreadId::parse("thr-alice").unwrap(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();
    bob.call(PresenceBind {
        seat: seat("bob"),
        incarnation_id: Some(inc("inc-bob-1")),
        machine_id: Some("node-a".into()),
        lane: None,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-bob").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: false,
        allow_fork: false,
        require_canary: false,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .unwrap();

    //Wait — bob's bind already triggered a sweep; let's test the exact stall with 33 fresh letters sent while alice's courier holds sweep_lock!
    wait_for(Duration::from_secs(3), || {
        let p = bob_log.clone();
        async move {
            read_text(&p)
                .map(|s| s.lines().count() >= 33)
                .unwrap_or(false)
        }
    })
    .await;
    std::fs::remove_file(&bob_log).unwrap();

    // Now send 33 NEW letters to bob FIRST while bob is temporarily unbound, OR send to alice first so alice grabs sweep_lock!
    // Look: send to alice -> CourierHub::react enters `sweep` and blocks inside `alice`!
    // Wait: if we send 33 letters to bob BEFORE alice, bob would sweep unless alice is already holding `sweep`!
    // Wait: if `CourierHub::react` is blocked in `alice`'s sweep, `st.mail` does NOT have bob's new letters yet;
    // instead, to make `st.mail` ALREADY have bob's 33 letters when `sweep` starts AND have the first 32 ACKed BEFORE `bob`'s batch runs:
    // Send 1 letter to `alice` AND 33 letters to `bob` while BOTH are unbound, then bind `alice` and `bob`?
    // Even simpler! Send a single multi-recipient letter or send 33 letters to `bob` + 1 letter to `alice` while `couriers_dir` is temporarily overridden or before binding,
    // wait: when `alice` binds first, `alice` has 1 waiting letter, so `alice`'s bind starts `sweep`? No, `bob` isn't bound yet!
    // If `bob` binds FIRST (with 0 letters), AND `alice` binds (with 0 letters), and then we send 1 letter `to: [alice, bob]` + 32 letters `to: [bob]`:
    // The FIRST `MailSend` (`to: [alice, bob]`) wakes `CourierHub::react`, which enters `alice`'s courier and blocks!
    // While `alice`'s courier is blocked, the 32 `MailSend`s are queued in Hub's broadcast channel behind `alice`'s sweep.
    // Once we release `alice`, `CourierHub::react` consumes ALL 32 `MessageCommitted` frames and on the first one enters a NEW `sweep`...
    // Better yet: make `bob` be `zed` (alphabetically AFTER `alice`), send 33 letters to `zed` while `zed` is bound on `machine-b` (so local `node-a` does NOT ring `zed`, while `st.mail` has all 33 letters!),
    // then send 1 letter to `alice` AND rebind `zed` to `node-a`!
    // Let's trace that!
    // 1. `zed` binds on `machine-b`. Send 33 letters to `zed` -> `st.mail` in `CourierHub` now has all 33 unacked letters for `zed`!
    let zed = h.seat_client("zed");
    zed.call(PresenceBind {
        seat: seat("zed"),
        incarnation_id: Some(inc("inc-zed-remote")),
        machine_id: Some("machine-b".into()),
        lane: None,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-zed-remote").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: false,
        allow_fork: false,
        require_canary: false,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .unwrap();

    let zed_log = h._tmp.path().join("zed-shas.log");
    write_executable_script(
        &h.couriers_dir,
        "t3-thread",
        &format!(
            r#"#!/usr/bin/env python3
import json, pathlib, sys, time
claim = json.load(sys.stdin)
seat = claim['seat']
if seat == 'alice':
    pathlib.Path({started:?}).write_text('1')
    deadline = time.time() + 4.0
    while not pathlib.Path({rel:?}).exists() and time.time() < deadline:
        time.sleep(0.01)
    print(json.dumps({{'outcome': 'rung'}}))
    sys.exit(0)
if seat == 'zed':
    with open({zlog:?}, 'a', encoding='utf-8') as f:
        for r in claim['referents']:
            f.write(r['sha256'] + '\n')
    print(json.dumps({{'outcome': 'rung'}}))
    sys.exit(0)
if seat == 'carol':
    mode = pathlib.Path({cmode:?}).read_text().strip()
    if mode == 'refuse':
        print(json.dumps({{'outcome': 'refused', 'reason': 'address-gone'}}))
    else:
        with open({clog:?}, 'a', encoding='utf-8') as f:
            f.write(claim['incarnation_id'] + ':' + str(len(claim['referents'])) + '\n')
        print(json.dumps({{'outcome': 'rung'}}))
    sys.exit(0)
print(json.dumps({{'outcome': 'rung'}}))
"#,
            started = alice_started.to_string_lossy(),
            rel = alice_release.to_string_lossy(),
            zlog = zed_log.to_string_lossy(),
            cmode = carol_mode.to_string_lossy(),
            clog = carol_log.to_string_lossy(),
        ),
    );

    let mut zed_shas = Vec::new();
    for i in 1..=33 {
        let s = sender
            .call(MailSend {
                to: vec![seat("zed")],
                cc: vec![],
                subject: format!("Zed batch #{i:02}"),
                body: format!("Zed body #{i}"),
                from: None,
                priority: LetterPriority::Normal,
                topic: Some(format!("z33-{i}")),
                in_reply_to: None,
                refs: vec![],
                wait_export: false,
                idem: None,
            })
            .await
            .unwrap();
        zed_shas.push(s.sha256);
    }
    h.sweep_now().await; // `st.mail` is 100% caught up with all 33 letters for `zed`!

    // Now move `zed` to `node-a` AND send 1 letter to `alice`? Even simpler: moving `zed` to `node-a` AFTER queueing `alice`!
    // Wait: if `alice` has 1 unrung letter (send to `alice` while `alice` is on `machine-b`, sweep so `st.mail` has it, then bind `zed` on `node-a`? No, if `alice` binds on `node-a` AFTER `zed` is already on `node-a`... wait: if `zed` binds on `node-a`, `zed` sweeps immediately!).
    // To have BOTH `alice` (1 letter) and `zed` (33 letters) become local in the SAME sweep:
    // Put BOTH `alice` and `zed` on `T3Thread`, mark `TransportKind::T3Thread` DOWN (`PresenceTransportHealth { down: true }`), bind `zed` on `node-a`, send 1 letter to `alice`, flush with `sweep_now()`, and THEN set `TransportKind::T3Thread` UP (`down: false`)!
    let mon = h.client(Principal::System {
        part: SystemPart::PresenceMonitor,
    });
    mon.call(PresenceTransportHealth {
        transport: TransportKind::T3Thread,
        down: true,
        detail: Some("pause".into()),
    })
    .await
    .unwrap();
    zed.call(PresenceBind {
        seat: seat("zed"),
        incarnation_id: Some(inc("inc-zed-local")),
        machine_id: Some("node-a".into()),
        lane: None,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-zed-local").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: true,
        allow_fork: false,
        require_canary: false,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .unwrap();
    sender
        .call(MailSend {
            to: vec![seat("alice")],
            cc: vec![],
            subject: "Hold sweep on alice".into(),
            body: "Gate".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("gate".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .unwrap();
    h.sweep_now().await;

    // Bring T3Thread UP -> ONE `sweep` starts! `alice` runs first and pauses on `alice_started`!
    mon.call(PresenceTransportHealth {
        transport: TransportKind::T3Thread,
        down: false,
        detail: None,
    })
    .await
    .unwrap();
    wait_for(Duration::from_secs(3), || {
        let p = alice_started.clone();
        async move { p.exists() }
    })
    .await;

    // While `CourierHub::react` is awaiting this `sweep` (so it CANNOT consume new `Acked` frames!),
    // ACK the first 32 of `zed`'s 33 letters in `MailBridge`!
    for sha_item in &zed_shas[..32] {
        zed.call(MailAck {
            path: None,
            sha256: Some(sha_item.clone()),
            seat: Some(seat("zed")),
            note: Some("pre-batch ack".into()),
        })
        .await
        .unwrap();
    }

    // Release `alice` -> the SAME `sweep` advances to `zed`, sees `Begin` return `[]` on the first 32,
    // skips them via `skipped_shas_in_sweep`, and immediately rings letter #33 without stalling (`Astra B1`)!
    std::fs::write(&alice_release, "go").unwrap();
    wait_for(Duration::from_secs(3), || {
        let p = zed_log.clone();
        async move {
            read_text(&p)
                .map(|s| s.lines().count() >= 1)
                .unwrap_or(false)
        }
    })
    .await;
    let rung_zed: Vec<String> = read_text(&zed_log)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(
        rung_zed,
        vec![zed_shas[32].as_str().to_owned()],
        "sweep must skip the 32 ACKed candidates and ring letter #33 without stalling"
    );

    // Part 2 (`Astra S9` + `Opus N3`): Reusing the SAME `incarnation_id` in a new epoch after `Refused` delivers!
    carol
        .call(PresenceBind {
            seat: seat("carol"),
            incarnation_id: Some(inc("inc-carol-hold")),
            machine_id: Some("node-a".into()),
            lane: None,
            endpoint: TransportEndpoint::T3Thread {
                thread_id: T3ThreadId::parse("thr-carol-dead").unwrap(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();
    sender
        .call(MailSend {
            to: vec![seat("carol")],
            cc: vec![],
            subject: "Reused incarnation ID test".into(),
            body: "Should deliver on epoch 2".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("reuse-inc".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .unwrap();

    // Wait for `inc-carol-hold` (epoch 1) to expire on refusal.
    wait_for(Duration::from_secs(3), || {
        let c = h.seat_client("carol");
        async move {
            c.call(PresenceList {
                project: None,
                live_only: false,
                include_retired: true,
            })
            .await
            .unwrap()
            .seats
            .iter()
            .find(|s| s.record.seat == seat("carol"))
            .and_then(|s| s.bindings.first())
            .is_some_and(|b| b.status == BindingStatus::Expired)
        }
    })
    .await;

    // Switch adapter to healthy and rebind the EXACT SAME `inc-carol-hold` -> gets `epoch = 2` and delivers!
    std::fs::write(&carol_mode, "ok").unwrap();
    let rebound_carol = carol
        .call(PresenceBind {
            seat: seat("carol"),
            incarnation_id: Some(inc("inc-carol-hold")),
            machine_id: Some("node-a".into()),
            lane: None,
            endpoint: TransportEndpoint::T3Thread {
                thread_id: T3ThreadId::parse("thr-carol-reconnected").unwrap(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();
    assert!(rebound_carol.epoch >= 2);

    wait_for(Duration::from_secs(3), || {
        let p = carol_log.clone();
        async move {
            read_text(&p)
                .map(|s| s.contains("inc-carol-hold:1"))
                .unwrap_or(false)
        }
    })
    .await;
}

#[tokio::test]
async fn round3_late_canary_refusal_never_expires_confirmed_live_route_and_ungated_rebind_requires_canary()
 {
    let h = E2eHarness::new("node-a", Duration::from_secs(5)).await;
    let canary_file = h._tmp.path().join("captured.canary");
    let release_canary = h._tmp.path().join("release.canary");
    let mail_log = h._tmp.path().join("mail-delivered.log");

    write_executable_script(
        &h.couriers_dir,
        "t3-thread",
        &format!(
            r#"#!/usr/bin/env python3
import json, pathlib, sys, time
claim = json.load(sys.stdin)
tok = claim.get('canary_token')
if tok and claim['seat'] == 'bob':
    pathlib.Path({cf:?}).write_text(tok)
    deadline = time.time() + 4.0
    while not pathlib.Path({rel:?}).exists() and time.time() < deadline:
        time.sleep(0.01)
    print(json.dumps({{'outcome': 'refused', 'reason': 'address-gone', 'detail': 'late stale refusal'}}))
    sys.exit(0)
if not tok and claim['seat'] == 'bob':
    with open({ml:?}, 'a', encoding='utf-8') as f:
        f.write(str(len(claim['referents'])) + '\n')
print(json.dumps({{'outcome': 'rung'}}))
"#,
            cf = canary_file.to_string_lossy(),
            rel = release_canary.to_string_lossy(),
            ml = mail_log.to_string_lossy(),
        ),
    );

    let bob = h.seat_client("bob");
    let carol = h.seat_client("carol");
    let alice = h.seat_client("alice");

    // 1 (`Astra B2`): Bind bob PendingCanary -> wait for canary script to capture token -> confirm Live
    //    WHILE canary script is still running -> release canary script with `refused` -> bob MUST remain Live!
    bob.call(PresenceBind {
        seat: seat("bob"),
        incarnation_id: Some(inc("inc-bob-race")),
        machine_id: Some("node-a".into()),
        lane: None,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-bob-race").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: false,
        allow_fork: false,
        require_canary: true,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .unwrap();

    wait_for(Duration::from_secs(3), || {
        let p = canary_file.clone();
        async move { p.exists() }
    })
    .await;
    let tok = read_text(&canary_file).unwrap().trim().to_owned();
    let confirmed = bob
        .call(PresenceConfirmCanary {
            seat: seat("bob"),
            incarnation_id: inc("inc-bob-race"),
            token: tok,
        })
        .await
        .unwrap();
    assert_eq!(confirmed.status, BindingStatus::Live);

    // Release the still-running canary adapter so it returns `refused` AFTER confirmation!
    std::fs::write(&release_canary, "1").unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    h.sweep_now().await;

    let snap = bob
        .call(PresenceList {
            project: None,
            live_only: false,
            include_retired: true,
        })
        .await
        .unwrap();
    let bob_b = snap
        .seats
        .iter()
        .find(|s| s.record.seat == seat("bob"))
        .and_then(|s| s.bindings.first())
        .unwrap();
    assert_eq!(
        bob_b.status,
        BindingStatus::Live,
        "late canary refusal MUST NOT expire an already-confirmed Live binding (`Astra B2`)"
    );

    // Prove bob's confirmed binding wasn't fenced either: normal mail rings cleanly!
    alice
        .call(MailSend {
            to: vec![seat("bob")],
            cc: vec![],
            subject: "After late canary race".into(),
            body: "Delivers!".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("race-ok".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .unwrap();
    wait_for(Duration::from_secs(3), || {
        let p = mail_log.clone();
        async move { read_text(&p).map(|s| s.trim() == "1").unwrap_or(false) }
    })
    .await;

    // 2 (`Astra B3`): Ungated Live binding MUST NOT satisfy a subsequent `require_canary: true` refresh,
    //    and changing `machine_id` on a confirmed binding MUST ALSO issue a fresh canary challenge!
    let ungated = carol
        .call(PresenceBind {
            seat: seat("carol"),
            incarnation_id: Some(inc("inc-carol-ug")),
            machine_id: Some("node-a".into()),
            lane: None,
            endpoint: TransportEndpoint::T3Thread {
                thread_id: T3ThreadId::parse("thr-carol-ug").unwrap(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(ungated.status, BindingStatus::Live);
    assert!(!ungated.canary_verified);

    let gated_refresh = carol
        .call(PresenceBind {
            seat: seat("carol"),
            incarnation_id: Some(inc("inc-carol-ug")),
            machine_id: Some("node-a".into()),
            lane: None,
            endpoint: TransportEndpoint::T3Thread {
                thread_id: T3ThreadId::parse("thr-carol-ug").unwrap(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: true,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(
        gated_refresh.status,
        BindingStatus::PendingCanary,
        "ungated binding must enter PendingCanary when refreshed with require_canary=true (`Astra B3`)"
    );

    // Also verify changing `machine_id` on bob's confirmed binding requires a new canary!
    let cross_machine_refresh = bob
        .call(PresenceBind {
            seat: seat("bob"),
            incarnation_id: Some(inc("inc-bob-race")),
            machine_id: Some("node-b".into()),
            lane: None,
            endpoint: TransportEndpoint::T3Thread {
                thread_id: T3ThreadId::parse("thr-bob-race").unwrap(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: true,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();
    assert_eq!(
        cross_machine_refresh.status,
        BindingStatus::PendingCanary,
        "refreshing a confirmed binding on a different machine_id must re-verify canary"
    );
}

#[tokio::test]
async fn round3_205_unreconciled_failures_survive_restart_and_pre_spawn_preserves_push_and_bounded_retries()
 {
    let tmp = tempfile::tempdir().unwrap();
    let sb_dir = tmp.path().join("sandbox");
    std::fs::create_dir_all(&sb_dir).unwrap();
    std::fs::write(sb_dir.join(".rex-wave-sandbox"), b"ok\n").unwrap();
    let couriers_dir = tmp.path().join("couriers.d");
    std::fs::create_dir_all(&couriers_dir).unwrap();

    let journal = Journal::open(&tmp.path().join("journal.sqlite3")).unwrap();
    let hub = Hub::new(2048);
    let house = HouseProfile::default();
    let presence = Presence::with_house_profile(house.clone()).with_machine_id("node-a");
    let presence_svc = presence.registry(journal.clone(), hub.clone());
    let room_svc = Registry::<Rooms>::new(journal.clone(), hub.clone());
    let sandbox = SandboxRoot::verify_with(&sb_dir, &Protection::default()).unwrap();
    let mail_writer = journal.claim(StreamId::Mail).unwrap();

    let mut cfg = RexConfig::default();
    cfg.mail.couriers_dir = Some(couriers_dir.clone());
    cfg.mail.hub_owner_seat = seat("postmaster");
    cfg.mail.defer_backoff_ms = 50;
    cfg.mail.max_defer_tries = 10;
    let (cfg_tx, cfg_rx) = watch::channel(cfg);

    let mail_bridge = Arc::new(
        MailBridge::with_hub(sandbox, journal.clone(), mail_writer, hub.clone())
            .with_room_service(room_svc.clone())
            .with_config_watch(cfg_rx.clone())
            .with_house_profile(house.clone()),
    );
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let router = Arc::new(Router {
        room: room_svc,
        inbox: Registry::<Inbox>::new(journal.clone(), hub.clone()),
        mail: mail_bridge.clone(),
        presence: presence_svc,
        read: Arc::new(ReadSide {
            journal: journal.clone(),
            hub: hub.clone(),
            sidebar: sidebar.clone(),
            agents: Arc::new(NoAgentFeed),
        }),
        ..Router::unavailable_with(Arc::new(ReadSide {
            journal: Journal::in_memory().unwrap(),
            hub: Hub::new(8),
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }))
    });

    let sys_courier = LocalClient::new(
        router.clone(),
        Principal::System {
            part: SystemPart::CourierHub,
        },
    );
    let victim1 = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: seat("victim1"),
        },
    );
    let alice = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: seat("alice"),
        },
    );

    // 1 (`Astra B6`): Bind `victim1`, commit failure #1 for `victim1`, then commit 204 MORE
    //    `DeliveryFailed` events BEFORE starting `CourierHub` (total = 205 > 200 display cap)!
    victim1
        .call(PresenceBind {
            seat: seat("victim1"),
            incarnation_id: Some(inc("inc-victim1")),
            machine_id: Some("node-a".into()),
            lane: None,
            endpoint: TransportEndpoint::OperatorToast {
                command: "osascript".into(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: false,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();

    sys_courier
        .call(MailRecordRing {
            phase: RingRecordPhase::Complete,
            dispatch_id: None,
            reconciled_failure_seq: None,
            courier: "macos-toast".into(),
            seat: seat("victim1"),
            machine_id: "node-a".into(),
            incarnation_id: Some(inc("inc-victim1")),
            expected_epoch: Some(1),
            sha256s: vec![],
            is_canary: false,
            pre_spawn: false,
            outcome: CourierOutcome::Refused,
            reason: Some("first-of-205".into()),
            detail: None,
            sender_seat: Some(seat("alice")),
            hub_owner_seat: seat("postmaster"),
            failure_kind: Some(DeliveryFailureKind::Refused),
            failure_groups: vec![],
            degraded_threshold: Some(1000),
        })
        .await
        .unwrap();

    for idx in 2..=205 {
        sys_courier
            .call(MailRecordRing {
                phase: RingRecordPhase::Complete,
                dispatch_id: None,
                reconciled_failure_seq: None,
                courier: "macos-toast".into(),
                seat: seat("filler"),
                machine_id: "node-a".into(),
                incarnation_id: None,
                expected_epoch: None,
                sha256s: vec![],
                is_canary: false,
                pre_spawn: false,
                outcome: CourierOutcome::Refused,
                reason: Some(format!("filler-{idx}")),
                detail: None,
                sender_seat: None,
                hub_owner_seat: seat("owner"),
                failure_kind: Some(DeliveryFailureKind::Refused),
                failure_groups: vec![],
                degraded_threshold: Some(1000),
            })
            .await
            .unwrap();
    }

    let pre_boot_mail = MailState::from_journal(&journal).unwrap();
    assert_eq!(pre_boot_mail.delivery_failures.len(), 200);
    assert_eq!(pre_boot_mail.unreconciled_failures.len(), 205);

    let courier_hub = CourierHub::new(
        CourierHubConfig::new("node-a", house, seat("owner"))
            .with_default_couriers_dir(Some(couriers_dir.clone()))
            .with_protection(Protection::default()),
        cfg_rx,
    );
    let courier_handle = courier_hub.handle();
    let courier_task = spawn_reactor(courier_hub, sys_courier.clone());

    wait_for(Duration::from_secs(10), || {
        let al = alice.clone();
        let v1 = victim1.clone();
        let j = journal.clone();
        async move {
            let inb = al
                .call(InboxList {
                    include_done: false,
                    limit: None,
                })
                .await
                .unwrap();
            let got_notice = inb.items.iter().any(|i| {
                matches!(
                    &i.draft.kind,
                    NotifyKind::MailDeliveryFailed { seat: s, reason, .. }
                        if s == &seat("victim1") && reason == "first-of-205"
                )
            });
            let snap = v1
                .call(PresenceList {
                    project: None,
                    live_only: false,
                    include_retired: true,
                })
                .await
                .unwrap();
            let expired = snap
                .seats
                .iter()
                .find(|s| s.record.seat == seat("victim1"))
                .and_then(|s| s.bindings.first())
                .is_some_and(|b| b.status == BindingStatus::Expired);
            if !(got_notice && expired) {
                return false;
            }
            MailState::from_journal(&j)
                .map(|m| m.unreconciled_failures.is_empty())
                .unwrap_or(false)
        }
    })
    .await;

    let replayed_mail = MailState::from_journal(&journal).unwrap();
    assert!(
        replayed_mail.unreconciled_failures.is_empty(),
        "all 205 failures must be durably marked DeliveryFailureReconciled"
    );

    // 2 (`Opus S2`): Non-executable (`0644`) adapter does NOT burn `(seat, sha256)` push eligibility!
    let t3_log = tmp.path().join("t3-delivered.log");
    let t3_script = couriers_dir.join("t3-thread");
    std::fs::write(
        &t3_script,
        format!(
            "#!/usr/bin/env python3\nimport json, sys\nc = json.load(sys.stdin)\nwith open({log:?}, 'a') as f:\n    f.write(str(len(c['referents'])) + '\\n')\nprint(json.dumps({{'outcome':'rung'}}))\n",
            log = t3_log.to_string_lossy()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&t3_script, std::fs::Permissions::from_mode(0o644)).unwrap();

    let bob = LocalClient::new(router.clone(), Principal::Seat { seat: seat("bob") });
    bob.call(PresenceBind {
        seat: seat("bob"),
        incarnation_id: Some(inc("inc-bob-chmod")),
        machine_id: Some("node-a".into()),
        lane: None,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-bob-chmod").unwrap(),
        },
        liveness: LivenessMode::LeaseOnly,
        ttl_ms: Some(300_000),
        takeover: false,
        allow_fork: false,
        require_canary: false,
        mail_horizon_at: Some(UnixMs(0)),
        idem: None,
    })
    .await
    .unwrap();

    alice
        .call(MailSend {
            to: vec![seat("bob")],
            cc: vec![],
            subject: "Chmod 0644 then 0755".into(),
            body: "Must still push once fixed!".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("chmod".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .unwrap();

    wait_for(Duration::from_secs(3), || {
        let mb = mail_bridge.clone();
        async move {
            let (h_list, _) = mb
                .courier_health_and_failures(&[("t3-thread".into(), false)], 10)
                .await;
            h_list
                .iter()
                .any(|c| c.name == "t3-thread" && c.unknown >= 1)
        }
    })
    .await;
    std::fs::set_permissions(&t3_script, std::fs::Permissions::from_mode(0o755)).unwrap();

    wait_for(Duration::from_secs(5), || {
        let p = t3_log.clone();
        async move { read_text(&p).map(|s| s.trim() == "1").unwrap_or(false) }
    })
    .await;

    cfg_tx.send_modify(|c| c.mail.max_defer_tries = 2);

    let bad_bb = couriers_dir.join("bb-thread");
    std::os::unix::fs::symlink("/bin/echo", &bad_bb).unwrap();
    let carol = LocalClient::new(
        router.clone(),
        Principal::Seat {
            seat: seat("carol"),
        },
    );
    carol
        .call(PresenceBind {
            seat: seat("carol"),
            incarnation_id: Some(inc("inc-carol-bad-canary")),
            machine_id: Some("node-a".into()),
            lane: None,
            endpoint: TransportEndpoint::BbThread {
                thread_id: BbThreadId::parse("bb-carol").unwrap(),
            },
            liveness: LivenessMode::LeaseOnly,
            ttl_ms: Some(300_000),
            takeover: false,
            allow_fork: false,
            require_canary: true,
            mail_horizon_at: Some(UnixMs(0)),
            idem: None,
        })
        .await
        .unwrap();

    // Also send a letter to `unrouted` (no binding at all) -> hits `GiveUp` after `max_defer_tries = 2`!
    alice
        .call(MailSend {
            to: vec![seat("unrouted")],
            cc: vec![],
            subject: "No route bounded retry".into(),
            body: "Should give up after 2 tries".into(),
            from: None,
            priority: LetterPriority::Normal,
            topic: Some("no-route".into()),
            in_reply_to: None,
            refs: vec![],
            wait_export: false,
            idem: None,
        })
        .await
        .unwrap();

    wait_for(Duration::from_secs(3), || {
        let mb = mail_bridge.clone();
        async move {
            let (_, failures) = mb.courier_health_and_failures(&[], 20).await;
            let carol_gave_up = failures
                .iter()
                .any(|f| f.seat == seat("carol") && f.kind == DeliveryFailureKind::GiveUp);
            let unrouted_gave_up = failures
                .iter()
                .any(|f| f.seat == seat("unrouted") && f.kind == DeliveryFailureKind::GiveUp);
            carol_gave_up && unrouted_gave_up
        }
    })
    .await;

    let _ = courier_handle;
    courier_task.abort();
}

#[tokio::test]
async fn round3_route_withdrawal_preserves_newer_machine_epoch_ordering_http504_and_ancestor_fd_staging()
 {
    // 1 (`Astra S10` + `Opus N1`): Withdrawing old machine A at t=120 keeps machine B's t=110 claim,
    //    and `inc-bob-10` (epoch 10) beats `inc-bob-9` (epoch 9) in the same millisecond!
    let mut table = RouteSyncTable::new();
    table.apply_claim(RouteClaim {
        seat: seat("bob"),
        machine_id: "m-a".into(),
        incarnation_id: inc("inc-a"),
        epoch: 1,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-a").unwrap(),
        },
        claimed_at: UnixMs(100),
        confirmed: true,
    });
    table.apply_claim(RouteClaim {
        seat: seat("bob"),
        machine_id: "m-b".into(),
        incarnation_id: inc("inc-b"),
        epoch: 2,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-b").unwrap(),
        },
        claimed_at: UnixMs(110),
        confirmed: true,
    });
    table.drop_local_at(&seat("bob"), "m-a", inc("inc-a"), UnixMs(120));
    assert_eq!(
        table.owner_machine(&seat("bob")),
        Some("m-b"),
        "withdrawing m-a's old incarnation at t=120 MUST NOT clear m-b's t=110 claim (`Astra S10`)"
    );

    let mut epoch_table = RouteSyncTable::new();
    epoch_table.apply_claim(RouteClaim {
        seat: seat("bob"),
        machine_id: "m-a".into(),
        incarnation_id: inc("inc-bob-9"),
        epoch: 9,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-9").unwrap(),
        },
        claimed_at: UnixMs(500),
        confirmed: true,
    });
    epoch_table.drop_local_at(&seat("bob"), "m-a", inc("inc-bob-9"), UnixMs(500));
    epoch_table.apply_claim(RouteClaim {
        seat: seat("bob"),
        machine_id: "m-a".into(),
        incarnation_id: inc("inc-bob-10"),
        epoch: 10,
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-10").unwrap(),
        },
        claimed_at: UnixMs(500),
        confirmed: true,
    });
    assert_eq!(
        epoch_table
            .get(&seat("bob"))
            .map(|c| c.incarnation_id.as_str()),
        Some("inc-bob-10"),
        "inc-bob-10 (epoch 10) must not be dominated by inc-bob-9's same-ms tombstone (`Opus N1`)"
    );

    // 2 (`Astra B5` + `S11`): HTTP 504 -> `unknown` (never `deferred`), generic BB 404 -> `unknown`,
    //    and thread-specific BB 404 -> `refused`!
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let examples_dir = manifest_dir.join("../../examples/couriers.d");

    async fn serve_single_http_status(status_line: &'static str, body: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt as _;
            let (mut s, _) = listener.accept().await.unwrap();
            let _ = read_full_http_request(&mut s).await;
            let resp = format!(
                "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = s.write_all(resp.as_bytes()).await;
            let _ = s.shutdown().await;
        });
        format!("http://{addr}")
    }

    let bb_claim = DeliveryClaim {
        schema: 1,
        seat: seat("bob"),
        machine_id: "node-a".into(),
        incarnation_id: inc("inc-bb"),
        endpoint: TransportEndpoint::BbThread {
            thread_id: BbThreadId::parse("thr_bb_1").unwrap(),
        },
        referents: vec![],
        canary_token: None,
    };
    let t3_claim = DeliveryClaim {
        schema: 1,
        seat: seat("bob"),
        machine_id: "node-a".into(),
        incarnation_id: inc("inc-t3"),
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr_t3_1").unwrap(),
        },
        referents: vec![],
        canary_token: None,
    };

    for (script_name, env_key, claim_ref, url) in [
        (
            "bb-thread",
            "BB_URL",
            &bb_claim,
            serve_single_http_status("504 Gateway Timeout", r#"{"error":"gateway timeout"}"#).await,
        ),
        (
            "t3-thread",
            "T3_URL",
            &t3_claim,
            serve_single_http_status("504 Gateway Timeout", r#"{"error":"gateway timeout"}"#).await,
        ),
        (
            "bb-thread",
            "BB_URL",
            &bb_claim,
            serve_single_http_status("404 Not Found", r#"{"error":"Not Found"}"#).await,
        ),
    ] {
        let mut cmd = tokio::process::Command::new(examples_dir.join(script_name));
        cmd.env(env_key, url)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped());
        let mut ch = cmd.spawn().unwrap();
        {
            use tokio::io::AsyncWriteExt as _;
            let mut stdin = ch.stdin.take().unwrap();
            stdin
                .write_all(&serde_json::to_vec(claim_ref).unwrap())
                .await
                .unwrap();
        }
        let out = ch.wait_with_output().await.unwrap();
        assert_eq!(
            rex_wave_mailbridge::parse_courier_stdout(&out.stdout).outcome,
            CourierOutcome::Unknown,
            "{script_name} must classify ambiguous 504 / generic 404 as unknown"
        );
    }

    let gone_url =
        serve_single_http_status("404 Not Found", r#"{"error":"thread thr_bb_1 not found"}"#).await;
    let mut gone_cmd = tokio::process::Command::new(examples_dir.join("bb-thread"));
    gone_cmd
        .env("BB_URL", gone_url)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    let mut gone_ch = gone_cmd.spawn().unwrap();
    {
        use tokio::io::AsyncWriteExt as _;
        let mut stdin = gone_ch.stdin.take().unwrap();
        stdin
            .write_all(&serde_json::to_vec(&bb_claim).unwrap())
            .await
            .unwrap();
    }
    let gone_out = gone_ch.wait_with_output().await.unwrap();
    assert_eq!(
        rex_wave_mailbridge::parse_courier_stdout(&gone_out.stdout).outcome,
        CourierOutcome::Refused
    );

    // 3 (`Astra S12`): Canary token straddling 8 KiB stderr capture cap omits diagnostic with zero leak!
    let tmp = tempfile::tempdir_in("/tmp").unwrap();
    let cap_script = write_executable_script(
        tmp.path(),
        "cap-leak",
        r#"#!/usr/bin/env python3
import json, sys
c = json.load(sys.stdin)
tok = c.get('canary_token') or ''
sys.stderr.write('x' * 8180 + tok + '\n')
sys.stderr.flush()
sys.exit(1)
"#,
    );
    let guard = rex_wave_fs::ProtectedRootsGuard::resolve(&Protection::default()).unwrap();
    let secret_tok = "rwc_secret_across_8k_boundary_abcdef";
    let cap_claim = DeliveryClaim {
        schema: 1,
        seat: seat("bob"),
        machine_id: "node-a".into(),
        incarnation_id: inc("inc-cap"),
        endpoint: TransportEndpoint::T3Thread {
            thread_id: T3ThreadId::parse("thr-cap").unwrap(),
        },
        referents: vec![],
        canary_token: Some(secret_tok.into()),
    };
    let cap_reply = invoke_courier_script(
        &cap_script,
        &cap_claim,
        Duration::from_secs(3),
        Some(&guard),
    )
    .await;
    assert_eq!(cap_reply.outcome, CourierOutcome::Unknown);
    assert!(
        !cap_reply
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("rwc_secret"),
        "truncated stderr at 8 KiB cap must never leak any prefix of canary_token: {:?}",
        cap_reply.detail
    );

    // 4 (`Astra B7`): Descriptor-anchored `open_verified_dir` + `stage_verified_executable_at`
    //    defeats ancestor rename/symlink swap to a protected root!
    let protected_dir = tmp.path().join("protected-interchange");
    let safe_parent = tmp.path().join("safe-holder");
    let safe_couriers = safe_parent.join("couriers.d");
    std::fs::create_dir_all(&protected_dir).unwrap();
    write_executable_script(
        &protected_dir,
        "t3-thread",
        "#!/usr/bin/env sh\nprintf '{\"outcome\":\"rung\",\"detail\":\"PROTECTED-EVIL\"}\\n'\n",
    );
    write_executable_script(
        &safe_couriers,
        "t3-thread",
        "#!/usr/bin/env sh\nprintf '{\"outcome\":\"rung\",\"detail\":\"SAFE-FD\"}\\n'\n",
    );

    let prot = Protection {
        roots: vec![protected_dir.clone()],
        names: vec![],
    };
    let strict_guard = rex_wave_fs::ProtectedRootsGuard::resolve(&prot).unwrap();
    assert!(
        strict_guard.open_verified_dir(&protected_dir).is_err(),
        "open_verified_dir must refuse protected root directly"
    );

    let (verified_fd, verified_path) = strict_guard.open_verified_dir(&safe_couriers).unwrap();
    let moved_aside = tmp.path().join("safe-holder-moved");
    {
        #[allow(clippy::disallowed_methods)]
        std::fs::rename(&safe_parent, &moved_aside).unwrap();
    }
    std::fs::create_dir_all(&safe_parent).unwrap();
    std::os::unix::fs::symlink(&protected_dir, safe_parent.join("couriers.d")).unwrap();

    let staged_fd = rex_wave_fs::stage_verified_executable_at(
        &verified_fd,
        &verified_path,
        "t3-thread",
        1024 * 1024,
    )
    .unwrap();
    let fd_out = tokio::process::Command::new(staged_fd.path())
        .output()
        .await
        .unwrap();
    assert_eq!(
        rex_wave_mailbridge::parse_courier_stdout(&fd_out.stdout)
            .detail
            .as_deref(),
        Some("SAFE-FD"),
        "stage_verified_executable_at must stage strictly from the held directory descriptor"
    );
    assert!(strict_guard.open_verified_dir(&safe_couriers).is_err());
}
