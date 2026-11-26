---------------------------- MODULE WaveLifecycle ----------------------------
(* Formal state machine for rex-wave worker lifecycle, host retirement,
   monotonic activity ordering, and (attempt, thread) observation fencing.

   Models crates/rex-wave-waves/src/lib.rs (Waves decider + WaveConductor),
   crates/rex-wave-api/src/wave.rs (WorkerPhase + Observation), and bridge /
   router thread fencing across crates/rex-wave-runtime/src/router.rs,
   crates/rex-wave-bb/src/lib.rs, and crates/rex-wave-t3/src/host.rs.

   Mutant plants known concurrency and fencing bugs; TLC must catch each one:
     "none"                      the production design;
     "drop_late_spawn"           (L1) late Spawned on Retired(thread=None) is
                                 dropped instead of transitioning to Retiring(t),
                                 orphaning live host threads;
     "unord_activity"            (L2) out-of-order Activity(host_seq) overwrites
                                 a newer settled/failure activity;
     "stalled_retire_observe"    (L4) transient failure committing Archived after
                                 host.retire clears needsReconcile, stalling Retiring;
     "errored_over_refused"      Errored overwrites sticky Refused within an attempt;
     "unfenced_thread_obs"       stale attempt-1 thread Activity/Archived/Park/Meta
                                 mutates attempt 2;
     "single_slot_pre_spawn"     pre-Spawned TurnsSuppressed keeps only the latest
                                 thread, losing thread A's suppressions when delayed
                                 thread B keys arrive before Spawned(A);
     "complete_with_errored"     wave finishes Completed despite an errored worker. *)
EXTENDS Naturals, FiniteSets

CONSTANTS Threads, MaxAttempt, MaxSeq, MaxFaults, Mutant, None

VARIABLES
  wavePhase,          \* "Live" | "Stopping" | "Completed" | "Failed"
  phase,              \* "Requested" | "Live" | "Retiring" | "Retired" | "Failed"
  attempt,            \* 1..MaxAttempt
  boundThread,        \* Threads \cup {None}
  activity,           \* "Working" | "Parked" | "Idle" | "Errored" | "Refused"
  errored,            \* BOOLEAN (sticky across retirement for wave outcome)
  refused,            \* BOOLEAN (sticky within attempt and across retirement)
  lastHostSeq,        \* 0..MaxSeq (monotonic per bound thread)
  runtimeModelThread, \* Threads \cup {None} (which thread last wrote RuntimeMeta)
  suppressedByThread, \* [Threads -> BOOLEAN] (per-thread TurnsSuppressed set)
  hostLive,           \* SUBSET Threads (threads currently running on host)
  externallyClosed,   \* SUBSET Threads (threads closed externally by host)
  threadAttempt,      \* [Threads -> 0..MaxAttempt]
  needsReconcile,     \* BOOLEAN (WaveConductor reconcile flag)
  faults,             \* 0..MaxFaults (bounded transient WaveObserve faults)
  staleMutated,       \* BOOLEAN (ghost: true if a stale thread/attempt mutated state)
  regressedSeq,       \* BOOLEAN (ghost: true if an older host_seq overwrote newer state)
  lostSuppression,    \* BOOLEAN (ghost: true if committed suppression for a thread disappeared)
  lateSpawnCaught     \* BOOLEAN (witness: Retired(None) -> Retiring(t) occurred)

vars == <<
  wavePhase, phase, attempt, boundThread, activity, errored, refused,
  lastHostSeq, runtimeModelThread, suppressedByThread,
  hostLive, externallyClosed, threadAttempt,
  needsReconcile, faults,
  staleMutated, regressedSeq, lostSuppression, lateSpawnCaught
>>

Phases == {"Requested", "Live", "Retiring", "Retired", "Failed"}
WavePhases == {"Live", "Stopping", "Completed", "Failed"}
Activities == {"Working", "Parked", "Idle", "Errored", "Refused"}

TypeOK ==
  /\ wavePhase \in WavePhases
  /\ phase \in Phases
  /\ attempt \in 1..MaxAttempt
  /\ boundThread \in Threads \cup {None}
  /\ activity \in Activities
  /\ errored \in BOOLEAN
  /\ refused \in BOOLEAN
  /\ lastHostSeq \in 0..MaxSeq
  /\ runtimeModelThread \in Threads \cup {None}
  /\ suppressedByThread \in [Threads -> BOOLEAN]
  /\ hostLive \subseteq Threads
  /\ externallyClosed \subseteq Threads
  /\ threadAttempt \in [Threads -> 0..MaxAttempt]
  /\ needsReconcile \in BOOLEAN
  /\ faults \in 0..MaxFaults
  /\ staleMutated \in BOOLEAN
  /\ regressedSeq \in BOOLEAN
  /\ lostSuppression \in BOOLEAN
  /\ lateSpawnCaught \in BOOLEAN

Init ==
  /\ wavePhase = "Live"
  /\ phase = "Requested"
  /\ attempt = 1
  /\ boundThread = None
  /\ activity = "Working"
  /\ errored = FALSE
  /\ refused = FALSE
  /\ lastHostSeq = 0
  /\ runtimeModelThread = None
  /\ suppressedByThread = [t \in Threads |-> FALSE]
  /\ hostLive = {}
  /\ externallyClosed = {}
  /\ threadAttempt = [t \in Threads |-> 0]
  /\ needsReconcile = TRUE
  /\ faults = 0
  /\ staleMutated = FALSE
  /\ regressedSeq = FALSE
  /\ lostSuppression = FALSE
  /\ lateSpawnCaught = FALSE

IsSettled ==
  \/ (phase = "Live" /\ activity \in {"Idle", "Errored", "Refused"})
  \/ phase \in {"Retired", "Failed"}

\* ---- Host & Bridge Environment ----

\* Host allocates a fresh thread t for the worker's in-flight attempt a.
HostSpawn(a, t) ==
  /\ a = attempt
  /\ phase = "Requested"
  /\ threadAttempt[t] = 0
  /\ ~\E t0 \in Threads : threadAttempt[t0] = a
  /\ hostLive' = hostLive \cup {t}
  /\ threadAttempt' = [threadAttempt EXCEPT ![t] = a]
  /\ UNCHANGED <<
       wavePhase, phase, attempt, boundThread, activity, errored, refused,
       lastHostSeq, runtimeModelThread, suppressedByThread,
       externallyClosed, needsReconcile, faults,
       staleMutated, regressedSeq, lostSuppression, lateSpawnCaught
     >>

\* Pre-spawn or live TurnsSuppressed recorded for thread t.
ObserveTurnsSuppressed(t) ==
  /\ threadAttempt[t] > 0
  /\ wavePhase \in {"Live", "Stopping"}
  /\ IF phase = "Live" THEN
       /\ boundThread = t
       /\ suppressedByThread' = [suppressedByThread EXCEPT ![t] = TRUE]
       /\ lostSuppression' = lostSuppression
     ELSE IF phase = "Requested" THEN
       IF Mutant = "single_slot_pre_spawn" THEN
         \* Buggy single-slot tracker clears any other pre-spawn thread's keys.
         /\ suppressedByThread' = [x \in Threads |-> x = t]
         /\ lostSuppression' = (lostSuppression \/ \E x \in Threads : x # t /\ suppressedByThread[x])
       ELSE
         /\ suppressedByThread' = [suppressedByThread EXCEPT ![t] = TRUE]
         /\ lostSuppression' = lostSuppression
     ELSE
       /\ UNCHANGED <<suppressedByThread, lostSuppression>>
  /\ UNCHANGED <<
       wavePhase, phase, attempt, boundThread, activity, errored, refused,
       lastHostSeq, runtimeModelThread,
       hostLive, externallyClosed, threadAttempt,
       needsReconcile, faults, staleMutated, regressedSeq, lateSpawnCaught
     >>

\* Bridge reports Observation::Spawned { attempt: a, thread: t }.
ObserveSpawned(a, t) ==
  /\ threadAttempt[t] = a
  /\ \/ /\ phase = "Requested"
        /\ a = attempt
        /\ phase' = "Live"
        /\ boundThread' = t
        /\ activity' = "Working"
        /\ lastHostSeq' = 0
        /\ runtimeModelThread' = None
        /\ needsReconcile' = needsReconcile
        /\ lateSpawnCaught' = lateSpawnCaught
     \/ \* L1: Spawned arrives after RetireRole/WaveStop moved Requested -> Retired(None).
        /\ phase = "Retired"
        /\ boundThread = None
        /\ a = attempt
        /\ IF Mutant = "drop_late_spawn" THEN
             /\ UNCHANGED <<phase, boundThread, activity, lastHostSeq, runtimeModelThread, needsReconcile, lateSpawnCaught>>
           ELSE
             /\ phase' = "Retiring"
             /\ boundThread' = t
             /\ activity' = activity
             /\ lastHostSeq' = 0
             /\ runtimeModelThread' = None
             /\ needsReconcile' = TRUE
             /\ lateSpawnCaught' = TRUE
  /\ UNCHANGED <<
       wavePhase, attempt, errored, refused, suppressedByThread,
       hostLive, externallyClosed, threadAttempt, faults,
       staleMutated, regressedSeq, lostSuppression
     >>

\* Host advances causal sequence on thread t and Bridge delivers Observation::Activity.
ObserveActivity(a, t, seq, act) ==
  /\ threadAttempt[t] = a
  /\ seq \in 1..MaxSeq
  /\ act \in Activities
  /\ wavePhase = "Live"
  /\ phase = "Live"
  /\ LET matchesCurrent == (phase = "Live" /\ boundThread = t /\ attempt = a)
         fencedAccept ==
           IF Mutant = "unfenced_thread_obs"
           THEN phase = "Live"
           ELSE matchesCurrent
         seqAccept ==
           IF Mutant = "unord_activity"
           THEN TRUE
           ELSE seq > lastHostSeq
         effectiveAct ==
           IF (activity = "Refused" \/ refused) /\ act = "Errored" /\ Mutant # "errored_over_refused"
           THEN "Refused"
           ELSE act
     IN /\ fencedAccept
        /\ seqAccept
        /\ staleMutated' = (staleMutated \/ ~matchesCurrent)
        /\ regressedSeq' = (regressedSeq \/ (matchesCurrent /\ seq < lastHostSeq /\ effectiveAct # activity))
        /\ lastHostSeq' = IF matchesCurrent /\ seq > lastHostSeq THEN seq ELSE lastHostSeq
        /\ activity' = effectiveAct
        /\ IF effectiveAct = "Idle" THEN
             /\ errored' = FALSE
             /\ refused' = FALSE
           ELSE
             /\ errored' = (errored \/ effectiveAct = "Errored")
             /\ refused' = (refused \/ effectiveAct = "Refused")
  /\ UNCHANGED <<
       wavePhase, phase, attempt, boundThread, runtimeModelThread, suppressedByThread,
       hostLive, externallyClosed, threadAttempt, needsReconcile, faults,
       lostSuppression, lateSpawnCaught
     >>

\* Router worker Wait auto-parking / unparking scoped to session's worker_thread.
RouterWaitParkOrUnpark(a, t, newAct) ==
  /\ threadAttempt[t] = a
  /\ newAct \in {"Parked", "Working"}
  /\ wavePhase = "Live"
  /\ phase = "Live"
  /\ activity \in {"Working", "Parked"}
  /\ LET matchesCurrent == (phase = "Live" /\ boundThread = t /\ attempt = a)
         accept ==
           IF Mutant = "unfenced_thread_obs"
           THEN phase = "Live"
           ELSE matchesCurrent
     IN /\ accept
        /\ activity' = newAct
        /\ staleMutated' = (staleMutated \/ ~matchesCurrent)
  /\ UNCHANGED <<
       wavePhase, phase, attempt, boundThread, errored, refused,
       lastHostSeq, runtimeModelThread, suppressedByThread,
       hostLive, externallyClosed, threadAttempt,
       needsReconcile, faults, regressedSeq, lostSuppression, lateSpawnCaught
     >>

\* Bridge delivers Observation::RuntimeMeta { thread, attempt }.
ObserveRuntimeMeta(a, t) ==
  /\ threadAttempt[t] = a
  /\ wavePhase = "Live"
  /\ phase = "Live"
  /\ LET matchesCurrent == (phase = "Live" /\ boundThread = t /\ attempt = a)
         accept ==
           IF Mutant = "unfenced_thread_obs"
           THEN phase = "Live"
           ELSE matchesCurrent
     IN /\ accept
        /\ runtimeModelThread' = t
        /\ staleMutated' = (staleMutated \/ ~matchesCurrent)
  /\ UNCHANGED <<
       wavePhase, phase, attempt, boundThread, activity, errored, refused,
       lastHostSeq, suppressedByThread, hostLive, externallyClosed, threadAttempt,
       needsReconcile, faults, regressedSeq, lostSuppression, lateSpawnCaught
     >>

\* Bridge delivers spontaneous Observation::Archived { thread, attempt } for an externally closed thread.
ObserveSpontaneousArchived(a, t) ==
  /\ threadAttempt[t] = a
  /\ t \in externallyClosed
  /\ LET matchesCurrent ==
           \/ (phase \in {"Live", "Retiring"} /\ boundThread = t /\ attempt = a)
           \/ (phase = "Requested" /\ attempt = a)
         accept ==
           IF Mutant = "unfenced_thread_obs"
           THEN phase \in {"Requested", "Live", "Retiring"}
           ELSE matchesCurrent
     IN /\ accept
        /\ phase' = "Retired"
        /\ boundThread' = IF phase = "Requested" THEN t ELSE boundThread
        /\ staleMutated' = (staleMutated \/ ~matchesCurrent)
  /\ UNCHANGED <<
       wavePhase, attempt, activity, errored, refused,
       lastHostSeq, runtimeModelThread, suppressedByThread,
       hostLive, externallyClosed, threadAttempt,
       needsReconcile, faults, regressedSeq, lostSuppression, lateSpawnCaught
     >>

\* Host externally closes/archives a live thread.
HostExternalClose(t) ==
  /\ t \in hostLive
  /\ hostLive' = hostLive \ {t}
  /\ externallyClosed' = externallyClosed \cup {t}
  /\ UNCHANGED <<
       wavePhase, phase, attempt, boundThread, activity, errored, refused,
       lastHostSeq, runtimeModelThread, suppressedByThread,
       threadAttempt,
       needsReconcile, faults, staleMutated, regressedSeq, lostSuppression, lateSpawnCaught
     >>

\* ---- Operator / Coordinator Commands ----

\* Operator retires the current worker role (e.g. WaveRetireRole or WaveTell respawn prep).
RetireRole ==
  /\ wavePhase = "Live"
  /\ phase \in {"Requested", "Live"}
  /\ IF phase = "Requested" THEN
       /\ phase' = "Retired"
       /\ boundThread' = None
       /\ needsReconcile' = needsReconcile
     ELSE
       /\ phase' = "Retiring"
       /\ boundThread' = boundThread
       /\ needsReconcile' = TRUE
  /\ errored' = (errored \/ activity = "Errored")
  /\ refused' = (refused \/ activity = "Refused")
  /\ UNCHANGED <<
       wavePhase, attempt, activity, lastHostSeq, runtimeModelThread,
       suppressedByThread, hostLive, externallyClosed, threadAttempt,
       faults, staleMutated, regressedSeq, lostSuppression, lateSpawnCaught
     >>

\* Host rejects an in-flight spawn attempt without creating a host thread, advancing to attempt + 1 or Failed.
ObserveSpawnFailed(a) ==
  /\ phase = "Requested"
  /\ a = attempt
  /\ ~\E t \in Threads : threadAttempt[t] = a
  /\ IF attempt < MaxAttempt THEN
       /\ phase' = "Requested"
       /\ attempt' = attempt + 1
       /\ needsReconcile' = TRUE
     ELSE
       /\ phase' = "Failed"
       /\ attempt' = attempt
       /\ needsReconcile' = FALSE
  /\ UNCHANGED <<
       wavePhase, boundThread, activity, errored, refused,
       lastHostSeq, runtimeModelThread, suppressedByThread,
       hostLive, externallyClosed, threadAttempt,
       faults, staleMutated, regressedSeq, lostSuppression, lateSpawnCaught
     >>

\* Respawn onto next attempt after a bound worker completed retirement (exercises cross-attempt stale thread signals).
RespawnNextAttempt ==
  /\ wavePhase = "Live"
  /\ phase = "Retired"
  /\ boundThread # None
  /\ boundThread \notin hostLive
  /\ attempt < MaxAttempt
  /\ phase' = "Requested"
  /\ attempt' = attempt + 1
  /\ boundThread' = None
  /\ activity' = "Working"
  /\ errored' = FALSE
  /\ refused' = FALSE
  /\ lastHostSeq' = 0
  /\ runtimeModelThread' = None
  /\ needsReconcile' = TRUE
  /\ UNCHANGED <<
       wavePhase, suppressedByThread, hostLive, externallyClosed, threadAttempt,
       faults, staleMutated, regressedSeq, lostSuppression, lateSpawnCaught
     >>

\* Operator stops the wave (WaveStop).
WaveStop ==
  /\ wavePhase = "Live"
  /\ IF phase = "Live" THEN
       /\ wavePhase' = "Stopping"
       /\ phase' = "Retiring"
       /\ errored' = (errored \/ activity = "Errored")
       /\ refused' = (refused \/ activity = "Refused")
       /\ needsReconcile' = TRUE
     ELSE IF phase = "Retiring" THEN
       /\ wavePhase' = "Stopping"
       /\ phase' = "Retiring"
       /\ UNCHANGED <<errored, refused>>
       /\ needsReconcile' = TRUE
     ELSE IF phase = "Requested" THEN
       /\ wavePhase' = "Failed"
       /\ phase' = "Retired"
       /\ boundThread' = None
       /\ UNCHANGED <<errored, refused, needsReconcile>>
     ELSE
       /\ wavePhase' = "Failed"
       /\ UNCHANGED <<phase, boundThread, errored, refused, needsReconcile>>
  /\ IF phase # "Requested" THEN UNCHANGED boundThread ELSE TRUE
  /\ UNCHANGED <<
       attempt, activity, lastHostSeq, runtimeModelThread, suppressedByThread,
       hostLive, externallyClosed, threadAttempt, faults,
       staleMutated, regressedSeq, lostSuppression, lateSpawnCaught
     >>

\* WaveComplete settles a Live wave once all workers are settled.
WaveComplete ==
  /\ wavePhase = "Live"
  /\ IsSettled
  /\ LET hasFailure ==
           \/ refused
           \/ activity = "Refused"
           \/ (Mutant # "complete_with_errored" /\ (errored \/ activity = "Errored" \/ phase = "Failed"))
     IN wavePhase' = IF hasFailure THEN "Failed" ELSE "Completed"
  /\ IF phase = "Live" THEN
       /\ phase' = "Retiring"
       /\ errored' = (errored \/ activity = "Errored")
       /\ refused' = (refused \/ activity = "Refused")
       /\ needsReconcile' = TRUE
     ELSE
       /\ UNCHANGED <<phase, errored, refused, needsReconcile>>
  /\ UNCHANGED <<
       attempt, boundThread, activity, lastHostSeq, runtimeModelThread,
       suppressedByThread, hostLive, externallyClosed, threadAttempt,
       faults, staleMutated, regressedSeq, lostSuppression, lateSpawnCaught
     >>

\* ---- WaveConductor Reactor ----

\* Conductor retires a Retiring worker on the host and commits Archived (or experiences a transient fault).
ConductorRetireStep ==
  /\ phase = "Retiring"
  /\ boundThread # None
  /\ needsReconcile
  /\ hostLive' = hostLive \ {boundThread}
  /\ \/ \* Clean commit of WaveObserve(Archived)
        /\ phase' = "Retired"
        /\ wavePhase' = IF wavePhase = "Stopping" THEN "Failed" ELSE wavePhase
        /\ needsReconcile' = FALSE
        /\ faults' = faults
     \/ \* Transient WaveObserve(Archived) failure after host.retire succeeded (L4)
        /\ faults < MaxFaults
        /\ phase' = "Retiring"
        /\ wavePhase' = wavePhase
        /\ faults' = faults + 1
        /\ needsReconcile' = IF Mutant = "stalled_retire_observe" THEN FALSE ELSE TRUE
  /\ UNCHANGED <<
       attempt, boundThread, activity, errored, refused,
       lastHostSeq, runtimeModelThread, suppressedByThread,
       externallyClosed, threadAttempt,
       staleMutated, regressedSeq, lostSuppression, lateSpawnCaught
     >>

\* Terminal stuttering guard so finite traces without infinite steps do not false-deadlock.
IdleDone ==
  /\ \/ (wavePhase \in {"Completed", "Failed"} /\ phase \in {"Retired", "Failed"} /\ hostLive = {})
     \/ (phase = "Retiring" /\ ~needsReconcile)
  /\ UNCHANGED vars

Next ==
  \/ \E a \in 1..MaxAttempt : ObserveSpawnFailed(a)
  \/ \E a \in 1..MaxAttempt, t \in Threads :
       \/ HostSpawn(a, t)
       \/ ObserveSpawned(a, t)
       \/ ObserveRuntimeMeta(a, t)
       \/ ObserveSpontaneousArchived(a, t)
       \/ \E nv \in {"Parked", "Working"} : RouterWaitParkOrUnpark(a, t, nv)
       \/ \E seq \in 1..MaxSeq, act \in Activities : ObserveActivity(a, t, seq, act)
  \/ \E t \in Threads : ObserveTurnsSuppressed(t) \/ HostExternalClose(t)
  \/ RetireRole
  \/ RespawnNextAttempt
  \/ WaveStop
  \/ WaveComplete
  \/ ConductorRetireStep
  \/ IdleDone

Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ WF_vars(ConductorRetireStep)
  /\ \A a \in 1..MaxAttempt, t \in Threads :
       /\ WF_vars(ObserveSpawned(a, t))
       /\ WF_vars(ObserveSpontaneousArchived(a, t))

\* Compact projection for TLC -dump dot state-graph inspection on small bounds.
StatechartView == <<wavePhase, phase, attempt, boundThread, activity, hostLive>>

\* ---- Safety Invariants ----

StaleThreadNeverMutatesCurrentAttempt ==
  /\ ~staleMutated
  /\ (phase = "Live" /\ runtimeModelThread # None => runtimeModelThread = boundThread)

MonotonicActivitySeq ==
  ~regressedSeq

RefusalStickyOverErrored ==
  /\ (activity = "Refused" => refused)
  /\ (refused /\ phase = "Live" => activity # "Errored")

SuppressedNeverLostOnPreSpawnCrossTalk ==
  ~lostSuppression

NoCompletedWaveWithFailedWorker ==
  wavePhase = "Completed" =>
    /\ phase # "Failed"
    /\ ~errored
    /\ ~refused
    /\ activity \notin {"Errored", "Refused"}

\* ---- Temporal Liveness Properties ----

\* Every host thread spawned for a retiring or retired worker is eventually retired on the host (catches L1).
NoOrphanedHostThread ==
  \A t \in Threads :
    []((t \in hostLive /\ threadAttempt[t] = attempt /\ phase \in {"Retiring", "Retired"})
       => <>(t \notin hostLive))

\* Every Retiring worker eventually reaches Retired (catches L4).
RetiringEventuallySettles ==
  [](phase = "Retiring" => <>(phase = "Retired"))

\* ---- Anti-Vacuity Reachability Witnesses (must FAIL under Mutant = "none") ----

NeverCompleted == wavePhase # "Completed"
NeverLateSpawnRetired == ~(lateSpawnCaught /\ phase = "Retired" /\ hostLive = {})
NeverRespawnedAttempt2 == ~(attempt = 2 /\ phase = "Live")
NeverRefusedWaveFailed == ~(wavePhase = "Failed" /\ refused)
=============================================================================
