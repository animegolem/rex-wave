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
                        courier: format!("courier-{recip}"),
                        seat: recip.clone(),
                        machine_id: m_id,
                        incarnation_id: None,
                        sha256s: vec![digest],
                        is_canary: false,
                        outcome: recorded_outcome,
                        reason: Some("broken".into()),
                        detail: None,
                    });
                } else {
                    self.ghost_unk.insert(recip.clone(), 0);
                    self.record_mail_event(MailEvent::RingAttempted {
                        courier: format!("courier-{recip}"),
                        seat: recip,
                        machine_id: m_id,
                        incarnation_id: None,
                        sha256s: vec![digest],
                        is_canary: false,
                        outcome: CourierOutcome::Rung,
                        reason: None,
                        detail: None,
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
