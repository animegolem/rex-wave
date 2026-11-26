# TLA+ Formal State Machines (`specs/`)

Exhaustive model-checked TLA+ specifications for `rex-wave`'s concurrent state machines, paired with anti-vacuity reachability witnesses (`*-sat-*.cfg`) and planted-bug mutants so no invariant or temporal property passes vacuously.

Run all 5 models, 14 anti-vacuity witnesses, and 25 planted-bug mutants in parallel via:

```sh
./scripts/model-check.sh
```

## 1. Wave Worker Lifecycle & Fencing (`specs/waves/WaveLifecycle.tla`)

Models `Waves` (`crates/rex-wave-waves/src/lib.rs`), `WorkerPhase` / `Observation` (`crates/rex-wave-api/src/wave.rs`), `WaveConductor`, and `(attempt, thread)` fencing across `Router` (`crates/rex-wave-runtime/src/router.rs`), `BbBridge` (`crates/rex-wave-bb/src/lib.rs`), and `T3Bridge` (`crates/rex-wave-t3/src/host.rs`).

| Property | Kind | Meaning | Planted Mutant |
|---|---|---|---|
| `StaleThreadNeverMutatesCurrentAttempt` | Safety | Stale thread or attempt observations (`Activity`, `Archived`, `RuntimeMeta`, worker `Wait` park/unpark) never mutate the current attempt | `unfenced_thread_obs` |
| `MonotonicActivitySeq` | Safety | Out-of-order `Observation::Activity { host_seq }` never regresses newer settled/failure state | `unord_activity` (`L2`) |
| `RefusalStickyOverErrored` | Safety | `Activity::Refused` is sticky within an attempt and across retirement; `Errored` never downgrades it | `errored_over_refused` |
| `SuppressedNeverLostOnPreSpawnCrossTalk` | Safety | Pre-`Spawned` `TurnsSuppressed` keys are tracked per thread so delayed keys for another thread never clear thread A's keys | `single_slot_pre_spawn` |
| `NoCompletedWaveWithFailedWorker` | Safety | A wave never finishes `Completed` if any worker failed, errored, or refused | `complete_with_errored` |
| `NoOrphanedHostThread` | Liveness | Late `Spawned` arriving on `Retired { thread: None }` transitions to `Retiring { thread }` and retires the host thread | `drop_late_spawn` (`L1`) |
| `RetiringEventuallySettles` | Liveness | Transient `WaveObserve(Archived)` failure after `host.retire` keeps `needsReconcile` armed until `Retired` commits | `stalled_retire_observe` (`L4`) |

Anti-vacuity reachability witnesses (`NeverCompleted`, `NeverLateSpawnRetired`, `NeverRespawnedAttempt2`, `NeverRefusedWaveFailed`) all fail as expected under `Mutant = "none"`.

## 2. Presence Leases & Disconnect / Expiry Fencing (`specs/presence/PresenceLeases.tla`)

Models `Presence` (`crates/rex-wave-mailbridge/src/presence.rs`), `PresenceBind` / `PresenceUnbind` / `PresenceExpire` / `PresenceHeartbeat` (`crates/rex-wave-api/src/presence.rs`), and asynchronous UDS disconnect sweeps (`crates/rex-wave/src/daemon.rs`; `PresenceState::expired_bindings` + `PresenceExpire` provide the fenced lease-expiry decider seam ahead of background sweep wiring).

| Property | Kind | Meaning | Planted Mutant |
|---|---|---|---|
| `StaleDisconnectNeverKillsReboundIncarnation` | Safety | Rebinding an incarnation onto a new socket bumps `epoch`, and `PresenceUnbind` fences on `expected_epoch` | `no_epoch_bump_on_rebind` (`L3a`), `unfenced_unbind` (`L3b`) |
| `StaleExpireNeverKillsHeartbeatedBinding` | Safety | `PresenceExpire` fences on `(expected_epoch, lease_expires_at)` so concurrent heartbeats or rebinds survive queued sweeps | `unfenced_expire` (`L3c`) |
| `AtMostOneLivePerLane` | Safety | At most one `Live` incarnation exists per `(seat, lane)` | `allow_duplicate_lane` |
| `OperatorSurfaceNeverBound` | Safety | Reserved operator surface seat cannot bind as a ring target | `operator_bindable` |
| `DisconnectedSocketEventuallyUnbound` | Liveness | Any dropped socket binding either rebinds onto a live socket or transitions out of `Live` | — |

Anti-vacuity reachability witnesses (`NeverReboundAfterDisconnect`, `NeverHeartbeatedBeforeQueuedExpire`, `NeverSupersededOnTakeover`) all fail as expected under `Mutant = "none"`.

## 3. Mail Delivery, Storage, & Multi-Machine Couriers (`specs/mail/`)

### `MailHub.tla` (Canary-Confirmed Routes & Delivery Refusals)

| Property | Kind | Meaning | Planted Mutant |
|---|---|---|---|
| `NoDoubleRing` | Safety | At most one ring per `(seat, sha256)` | `no_key` |
| `RightSeat` | Safety | Rings target only a confirmed live session of the recipient seat, never a fork or another seat | `pinned_name` |
| `LoudGiveUp` | Safety | Exhausted retries transition to `refused` with a visible notice, never silently dropped | `silent_giveup` |
| `NoFalseGiveUp` | Safety | Never gives up on a seat with a live confirmed route | `pinned-deaf` (`pinned_name`) |
| `Settles` | Liveness | Every letter eventually reaches `acked` or `refused` | — |

### `MailStore.tla` (Content-Addressed SQLite Commit Point & Mirror Reconcile)

| Property | Kind | Meaning | Planted Mutant |
|---|---|---|---|
| `DurableSend` | Safety | Every acknowledged `send` is durable in SQLite, drop folder, or Markdown mirror | `success_before_commit` |
| `RebuildParity` | Safety | At rest, rebuilding from the Markdown mirror + ack ledger reproduces 100% of DB state, acks, and mutation flags | `ack_not_mirrored` |
| `EditNeverRingable` | Safety | Mutating an already-acked letter file flags the new hash instead of ringing it as new mail | `edit_reingests` |
| `ReachesRest` | Liveness | Startup reconcile exports rows left without files by crashes (`<>[]Quiescent`) | `no_reconcile` |

### `MailMachines.tla` (Courier Adapters, `AdapterDegraded`, & Union Sync)

| Property | Kind | Meaning | Planted Mutant |
|---|---|---|---|
| `OneMachineRings` | Safety | Only the machine owning the seat's route claim rings that seat | `ring_any_machine` |
| `NoDoubleRing` | Safety | An `unknown` courier outcome counts as rung and is never repeated | `retry_unknown` |
| `DegradedLoud` | Safety | `K` consecutive `unknown` courier outcomes raise `AdapterDegraded` | `no_degrade` |
| `NoLostLetter` | Safety | Cross-machine sync unions immutable message and ack sets without loss | `sync_overwrite` |
| `NoRingAfterAck` | Safety | Already-acked letters are never rung | `ring_after_ack` |
| `Converges` | Liveness | All machines' message and ack stores eventually converge (`<>[]`) | — |
