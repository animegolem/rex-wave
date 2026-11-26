---
name: verify-state-machines
description: Verify and synchronize TLA+ state machines (specs/) and Rust conformance suites whenever Decider, Reactor, Presence, Wave, Room, Board, or Mail state transitions change.
---

# Verify State Machines (`specs/` + Conformance Suites)

Use this skill whenever editing any `Decider`, `Reactor`, host bridge, router worker parking, presence lease sweep, task lease, or mail delivery/storage state transition in `rex-wave`.

## Coverage Map

| Formal Spec / Conformance Suite | Production Rust / TS Targets | Core Invariants & Liveness |
|---|---|---|
| `specs/waves/WaveLifecycle.tla` + `crates/rex-wave-waves/tests/conformance.rs` | `crates/rex-wave-api/src/wave.rs`, `crates/rex-wave-waves/src/lib.rs`, `crates/rex-wave-runtime/src/router.rs`, `crates/rex-wave-bb/src/lib.rs`, `crates/rex-wave-t3/src/host.rs`, `bb-plugin/server.ts` | `StaleThreadNeverMutatesCurrentAttempt`, `MonotonicActivitySeq` (`L2`), `RefusalStickyOverErrored`, `SuppressedNeverLostOnPreSpawnCrossTalk`, `NoCompletedWaveWithFailedWorker`, `NoOrphanedHostThread` (`L1`), `RetiringEventuallySettles` (`L4`) |
| `specs/presence/PresenceLeases.tla` + `crates/rex-wave-mailbridge/tests/conformance.rs` | `crates/rex-wave-api/src/presence.rs`, `crates/rex-wave-mailbridge/src/presence.rs`, `crates/rex-wave/src/daemon.rs`, `crates/rex-wave/src/main.rs` | `StaleDisconnectNeverKillsReboundIncarnation` (`L3a`/`L3b`), `StaleExpireNeverKillsHeartbeatedBinding` (`L3c`), `AtMostOneLivePerLane`, `OperatorSurfaceNeverBound`, `DisconnectedSocketEventuallyUnbound` |
| `specs/mail/{MailHub,MailStore,MailMachines}.tla` + `crates/rex-wave-mailbridge/tests/conformance.rs` | `crates/rex-wave-mailbridge/src/{lib.rs,shadow.rs,acks.rs,score.rs}` | `NoDoubleRing`, `RightSeat`, `LoudGiveUp`, `NoFalseGiveUp`, `DurableSend`, `RebuildParity`, `EditNeverRingable`, `OneMachineRings`, `DegradedLoud`, `NoLostLetter`, `NoRingAfterAck`, `ShadowWatchTracker` hydration equivalence |
| `crates/rex-wave-rooms/tests/conformance.rs` | `crates/rex-wave-rooms/src/lib.rs` | Monotonic `LeaseEpoch` task fencing, expired-lease reclaim, DAG cycle & unmet-`needs` rejection, replay-from-journal state equivalence |
| `crates/rex-wave-board/tests/conformance.rs` | `crates/rex-wave-board/src/{lib.rs,task_org.rs}`, `crates/rex-kanban/src/{model.rs,org.rs}` | `CardRevision` fencing, deterministic `OrgId` preservation, idempotent two-way `.org` sync (`sync -> sync = Unchanged`) |
| `crates/rex-wave-notify/tests/conformance.rs` | `crates/rex-wave-notify/src/lib.rs` | `Herald` cold-replay vs live-delivery inbox equivalence (`observe_replay` defers transient `MailUnrung` grace evaluation until `CaughtUp` unless `StreamId::Inbox` already persisted the card), `MailUnrung` reconnect grace window, dedupe key monotonicity |

## Staged Spec-Sync Loop

1. **Model first (when adding or changing transitions):**
   - Update the corresponding `.tla` module and `.cfg` files under `specs/`.
   - Every new safety or liveness property **must** pair with:
     - an anti-vacuity reachability witness (`*-sat-Never*.cfg`) proving the guarded scenario is actually reachable under `Mutant = "none"`, and
     - a planted mutant (`Mutant = "<bug_slug>"`) proving TLC fails the exact property when the guard is omitted.
   - To inspect small-bound state graphs or counterexample traces visually, copy the target `.tla`/`.cfg` to a scratch directory under `/tmp` and run `tlc2.TLC -dump dot stategraph.dot` against `StatechartView`.

2. **Run the TLC proof-firewall gate:**
   ```sh
   ./scripts/model-check.sh
   ```
   All 5 base models must pass with `> 1` distinct states, all `*-sat-*` witnesses must be reached, and all planted mutants must fail their expected invariant or temporal property.

3. **Run the Rust conformance & workspace gate:**
   ```sh
   ./check
   ```

4. **Planted-break firewall check (mandatory before trusting a new invariant):**
   - Temporarily plant the bug in the real Rust production code (e.g., omit the `expected_epoch` check or late-`Spawned` transition).
   - Run `cargo test` on the corresponding conformance suite and verify it fails on the planted bug.
   - Revert the planted break and re-run `./check` to confirm green.
