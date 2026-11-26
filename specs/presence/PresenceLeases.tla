---------------------------- MODULE PresenceLeases ----------------------------
(* Formal state machine for rex-wave seat presence bindings, epoch fencing,
   asynchronous UDS disconnect sweeps, lease-expiry sweeps, lane exclusivity,
   and operator-surface seat refusal.

   Models crates/rex-wave-mailbridge/src/presence.rs (Presence decider),
   crates/rex-wave-api/src/presence.rs (PresenceBind / PresenceUnbind /
   PresenceExpire / PresenceHeartbeat), and crates/rex-wave/src/daemon.rs
   (unbind_uds_sessions + PresenceDisconnectHooks; PresenceState::expired_bindings
   and PresenceExpire provide the fenced lease-expiry decider seam ahead of
   background timer sweep wiring).

   Mutant plants known race and invariant bugs; TLC must catch each one:
     "none"                     the production design;
     "no_epoch_bump_on_rebind"  (L3a) refreshing an incarnation onto a new
                                endpoint keeps its old epoch, allowing a queued
                                disconnect unbind from the old socket to kill
                                the new binding;
     "unfenced_unbind"          (L3b) PresenceUnbind ignores expected_epoch;
     "unfenced_expire"          (L3c) PresenceExpire ignores expected_epoch /
                                lease_expires_at, killing a freshly heartbeated
                                or rebound incarnation;
     "allow_duplicate_lane"     PresenceBind skips lane exclusivity and allows
                                two live incarnations on the same lane;
     "operator_bindable"        PresenceBind allows the reserved operator
                                surface seat to bind as a ring target. *)
EXTENDS Integers, FiniteSets

CONSTANTS
  Seats, OperatorSeat, Incarnations, Conns, Lanes,
  MaxEpoch, MaxClock, Mutant, None

VARIABLES
  status,             \* [Incarnations -> {"Absent", "Live", "Expired", "Unbound", "Superseded"}]
  seatOf,             \* [Incarnations -> Seats \cup {OperatorSeat, None}]
  connOf,             \* [Incarnations -> Conns \cup {None}]
  laneOf,             \* [Incarnations -> Lanes \cup {None}]
  livenessOf,         \* [Incarnations -> {"Socket", "Heartbeat"}]
  epochOf,            \* [Incarnations -> 0..MaxEpoch]
  expiresAt,          \* [Incarnations -> 0..MaxClock]
  nextEpoch,          \* [Seats \cup {OperatorSeat} -> 1..(MaxEpoch + 1)]
  liveConns,          \* SUBSET Conns
  usedConns,          \* SUBSET Conns
  clock,              \* 0..MaxClock
  hbPending,          \* [Incarnations -> (-1)..MaxClock] (-1 means none)
  discQueue,          \* SUBSET (Incarnations \X Conns \X (1..MaxEpoch))
  expQueue,           \* SUBSET (Incarnations \X (1..MaxEpoch) \X (0..MaxClock))
  staleUnbindKilled,  \* BOOLEAN (ghost: stale disconnect sweep killed a live new-conn binding)
  staleExpireKilled,  \* BOOLEAN (ghost: stale expiry sweep killed a refreshed/heartbeated binding)
  reboundAfterDrop,   \* BOOLEAN (witness: disconnect -> rebind -> delayed sweep occurred)
  heartbeatedVsSweep, \* BOOLEAN (witness: pre-deadline heartbeat queued/applied vs stale sweep)
  supersededWitness   \* BOOLEAN (witness: lane takeover superseded an earlier incarnation)

vars == <<
  status, seatOf, connOf, laneOf, livenessOf, epochOf, expiresAt,
  nextEpoch, liveConns, usedConns, clock, hbPending, discQueue, expQueue,
  staleUnbindKilled, staleExpireKilled,
  reboundAfterDrop, heartbeatedVsSweep, supersededWitness
>>

AllSeats == Seats \cup {OperatorSeat}
Statuses == {"Absent", "Live", "Expired", "Unbound", "Superseded"}

TypeOK ==
  /\ status \in [Incarnations -> Statuses]
  /\ seatOf \in [Incarnations -> AllSeats \cup {None}]
  /\ connOf \in [Incarnations -> Conns \cup {None}]
  /\ laneOf \in [Incarnations -> Lanes \cup {None}]
  /\ livenessOf \in [Incarnations -> {"Socket", "Heartbeat"}]
  /\ epochOf \in [Incarnations -> 0..MaxEpoch]
  /\ expiresAt \in [Incarnations -> 0..MaxClock]
  /\ nextEpoch \in [AllSeats -> 1..(MaxEpoch + 1)]
  /\ liveConns \subseteq usedConns
  /\ usedConns \subseteq Conns
  /\ clock \in 0..MaxClock
  /\ hbPending \in [Incarnations -> (-1)..MaxClock]
  /\ discQueue \subseteq (Incarnations \X Conns \X (1..MaxEpoch))
  /\ expQueue \subseteq (Incarnations \X (1..MaxEpoch) \X (0..MaxClock))
  /\ staleUnbindKilled \in BOOLEAN
  /\ staleExpireKilled \in BOOLEAN
  /\ reboundAfterDrop \in BOOLEAN
  /\ heartbeatedVsSweep \in BOOLEAN
  /\ supersededWitness \in BOOLEAN

Init ==
  /\ status = [i \in Incarnations |-> "Absent"]
  /\ seatOf = [i \in Incarnations |-> None]
  /\ connOf = [i \in Incarnations |-> None]
  /\ laneOf = [i \in Incarnations |-> None]
  /\ livenessOf = [i \in Incarnations |-> "Socket"]
  /\ epochOf = [i \in Incarnations |-> 0]
  /\ expiresAt = [i \in Incarnations |-> 0]
  /\ nextEpoch = [s \in AllSeats |-> 1]
  /\ liveConns = {}
  /\ usedConns = {}
  /\ clock = 0
  /\ hbPending = [i \in Incarnations |-> -1]
  /\ discQueue = {}
  /\ expQueue = {}
  /\ staleUnbindKilled = FALSE
  /\ staleExpireKilled = FALSE
  /\ reboundAfterDrop = FALSE
  /\ heartbeatedVsSweep = FALSE
  /\ supersededWitness = FALSE

OtherLiveForSeat(s, selfInc) ==
  {j \in Incarnations : j # selfInc /\ seatOf[j] = s /\ status[j] = "Live"}

SameLaneLive(s, ln, selfInc) ==
  {j \in OtherLiveForSeat(s, selfInc) : laneOf[j] = ln}

\* ---- Transport Environment ----

OpenConn(c) ==
  /\ c \notin usedConns
  /\ usedConns' = usedConns \cup {c}
  /\ liveConns' = liveConns \cup {c}
  /\ UNCHANGED <<
       status, seatOf, connOf, laneOf, livenessOf, epochOf, expiresAt,
       nextEpoch, clock, hbPending, discQueue, expQueue,
       staleUnbindKilled, staleExpireKilled,
       reboundAfterDrop, heartbeatedVsSweep, supersededWitness
     >>

\* Socket c drops; PresenceDisconnectHooks snapshots any currently tracked bindings on c.
DisconnectConn(c) ==
  /\ c \in liveConns
  /\ liveConns' = liveConns \ {c}
  /\ LET affected ==
           {<<i, c, epochOf[i]>> :
             i \in {k \in Incarnations : status[k] = "Live" /\ connOf[k] = c}}
     IN discQueue' = discQueue \cup affected
  /\ UNCHANGED <<
       status, seatOf, connOf, laneOf, livenessOf, epochOf, expiresAt,
       nextEpoch, usedConns, clock, hbPending, expQueue,
       staleUnbindKilled, staleExpireKilled,
       reboundAfterDrop, heartbeatedVsSweep, supersededWitness
     >>

AdvanceClock ==
  /\ clock < MaxClock
  /\ clock' = clock + 1
  /\ UNCHANGED <<
       status, seatOf, connOf, laneOf, livenessOf, epochOf, expiresAt,
       nextEpoch, liveConns, usedConns, hbPending, discQueue, expQueue,
       staleUnbindKilled, staleExpireKilled,
       reboundAfterDrop, heartbeatedVsSweep, supersededWitness
     >>

\* ---- Presence Commands ----

\* Bind or refresh incarnation i on UDS socket c (SocketLifetime) or Heartbeat lease.
Bind(s, i, c, ln, mode, takeover) ==
  /\ (Mutant = "operator_bindable" \/ s # OperatorSeat)
  /\ (seatOf[i] = None \/ seatOf[i] = s)
  /\ c \in liveConns
  /\ mode \in {"Socket", "Heartbeat"}
  /\ takeover \in BOOLEAN
  /\ nextEpoch[s] <= MaxEpoch
  /\ LET sameLane == SameLaneLive(s, ln, i)
         superseded ==
           IF Mutant = "allow_duplicate_lane" THEN {}
           ELSE IF takeover THEN sameLane
           ELSE {}
         conflict ==
           /\ Mutant # "allow_duplicate_lane"
           /\ ~takeover
           /\ sameLane # {}
         wasLiveSameEndpoint ==
           /\ status[i] = "Live"
           /\ connOf[i] = c
           /\ laneOf[i] = ln
           /\ livenessOf[i] = mode
           /\ (livenessOf[i] = "Socket" \/ expiresAt[i] > clock)
         needBump ==
           IF Mutant = "no_epoch_bump_on_rebind"
           THEN status[i] # "Live"
           ELSE ~wasLiveSameEndpoint
         assignedEpoch ==
           IF needBump THEN nextEpoch[s] ELSE epochOf[i]
         newLeaseExp ==
           IF mode = "Heartbeat"
           THEN IF clock + 1 <= MaxClock THEN clock + 1 ELSE MaxClock
           ELSE 0
     IN /\ ~conflict
        /\ (needBump \/ livenessOf[i] # mode \/ expiresAt[i] # newLeaseExp \/ superseded # {})
        /\ status' = [j \in Incarnations |->
             IF j = i THEN "Live"
             ELSE IF j \in superseded THEN "Superseded"
             ELSE status[j]]
        /\ seatOf' = [seatOf EXCEPT ![i] = s]
        /\ connOf' = [connOf EXCEPT ![i] = c]
        /\ laneOf' = [laneOf EXCEPT ![i] = ln]
        /\ livenessOf' = [livenessOf EXCEPT ![i] = mode]
        /\ epochOf' = [epochOf EXCEPT ![i] = assignedEpoch]
        /\ expiresAt' = [expiresAt EXCEPT ![i] = newLeaseExp]
        /\ nextEpoch' = [nextEpoch EXCEPT ![s] = IF needBump THEN @ + 1 ELSE @]
        /\ supersededWitness' = (supersededWitness \/ superseded # {})
  /\ UNCHANGED <<
       liveConns, usedConns, clock, hbPending, discQueue, expQueue,
       staleUnbindKilled, staleExpireKilled, reboundAfterDrop, heartbeatedVsSweep
     >>

\* Router stamps ctx.at = clock on an incoming pre-deadline PresenceHeartbeat request.
QueueHeartbeat(i) ==
  /\ status[i] = "Live"
  /\ livenessOf[i] = "Heartbeat"
  /\ clock < expiresAt[i]
  /\ hbPending[i] = -1
  /\ hbPending' = [hbPending EXCEPT ![i] = clock]
  /\ UNCHANGED <<
       status, seatOf, connOf, laneOf, livenessOf, epochOf, expiresAt,
       nextEpoch, liveConns, usedConns, clock, discQueue, expQueue,
       staleUnbindKilled, staleExpireKilled,
       reboundAfterDrop, heartbeatedVsSweep, supersededWitness
     >>

\* Presence decider applies an in-flight heartbeat stamped at reqAt (< expiresAt[i]),
\* matching decide_heartbeat's `exp <= ctx.at()` rejection guard.
RunQueuedHeartbeat(i) ==
  /\ hbPending[i] >= 0
  /\ LET reqAt == hbPending[i]
         accepted ==
           /\ status[i] = "Live"
           /\ livenessOf[i] = "Heartbeat"
           /\ reqAt < expiresAt[i]
         extendedExp ==
           IF reqAt + 2 <= MaxClock THEN reqAt + 2 ELSE MaxClock
     IN /\ hbPending' = [hbPending EXCEPT ![i] = -1]
        /\ IF accepted THEN
             /\ expiresAt' = [expiresAt EXCEPT ![i] = extendedExp]
             /\ heartbeatedVsSweep' =
                  (heartbeatedVsSweep \/ \E e \in 1..MaxEpoch, dl \in 0..MaxClock : <<i, e, dl>> \in expQueue)
           ELSE
             /\ UNCHANGED <<expiresAt, heartbeatedVsSweep>>
  /\ UNCHANGED <<
       status, seatOf, connOf, laneOf, livenessOf, epochOf,
       nextEpoch, liveConns, usedConns, clock, discQueue, expQueue,
       staleUnbindKilled, staleExpireKilled, reboundAfterDrop, supersededWitness
     >>

\* Background disconnect worker processes a queued UDS disconnect entry <<i, droppedConn, expEpoch>>.
RunQueuedDisconnectUnbind(entry) ==
  /\ entry \in discQueue
  /\ LET i == entry[1]
         droppedConn == entry[2]
         expEpoch == entry[3]
         isLiveOnNewConn ==
           status[i] = "Live" /\ connOf[i] # droppedConn /\ connOf[i] \in liveConns
         epochMatches ==
           IF Mutant = "unfenced_unbind"
           THEN TRUE
           ELSE epochOf[i] = expEpoch
     IN /\ discQueue' = discQueue \ {entry}
        /\ reboundAfterDrop' = (reboundAfterDrop \/ isLiveOnNewConn)
        /\ IF status[i] = "Live" /\ epochMatches THEN
             /\ status' = [status EXCEPT ![i] = "Unbound"]
             /\ staleUnbindKilled' = (staleUnbindKilled \/ isLiveOnNewConn)
           ELSE
             /\ UNCHANGED <<status, staleUnbindKilled>>
  /\ UNCHANGED <<
       seatOf, connOf, laneOf, livenessOf, epochOf, expiresAt,
       nextEpoch, liveConns, usedConns, clock, hbPending, expQueue,
       staleExpireKilled, heartbeatedVsSweep, supersededWitness
     >>

\* Lease scanner observes an expired heartbeat binding and queues PresenceExpire.
QueueLeaseExpire(i) ==
  /\ status[i] = "Live"
  /\ livenessOf[i] = "Heartbeat"
  /\ expiresAt[i] <= clock
  /\ <<i, epochOf[i], expiresAt[i]>> \notin expQueue
  /\ expQueue' = expQueue \cup {<<i, epochOf[i], expiresAt[i]>>}
  /\ UNCHANGED <<
       status, seatOf, connOf, laneOf, livenessOf, epochOf, expiresAt,
       nextEpoch, liveConns, usedConns, clock, hbPending, discQueue,
       staleUnbindKilled, staleExpireKilled,
       reboundAfterDrop, heartbeatedVsSweep, supersededWitness
     >>

\* PresenceExpire executes a queued lease-expiry command <<i, expEpoch, expDeadline>>.
RunQueuedExpire(entry) ==
  /\ entry \in expQueue
  /\ LET i == entry[1]
         expEpoch == entry[2]
         expDeadline == entry[3]
         wasRefreshedSinceScan ==
           status[i] = "Live"
           /\ (epochOf[i] # expEpoch
               \/ livenessOf[i] # "Heartbeat"
               \/ expiresAt[i] > clock
               \/ expiresAt[i] # expDeadline)
         fencedAccept ==
           IF Mutant = "unfenced_expire"
           THEN status[i] = "Live"
           ELSE /\ status[i] = "Live"
                /\ livenessOf[i] = "Heartbeat"
                /\ epochOf[i] = expEpoch
                /\ expiresAt[i] = expDeadline
                /\ expiresAt[i] <= clock
     IN /\ expQueue' = expQueue \ {entry}
        /\ IF fencedAccept THEN
             /\ status' = [status EXCEPT ![i] = "Expired"]
             /\ staleExpireKilled' = (staleExpireKilled \/ wasRefreshedSinceScan)
           ELSE
             /\ UNCHANGED <<status, staleExpireKilled>>
  /\ UNCHANGED <<
       seatOf, connOf, laneOf, livenessOf, epochOf, expiresAt,
       nextEpoch, liveConns, usedConns, clock, hbPending, discQueue,
       staleUnbindKilled, reboundAfterDrop, heartbeatedVsSweep, supersededWitness
     >>

IdleStutter ==
  /\ \A i \in Incarnations : hbPending[i] = -1
  /\ discQueue = {}
  /\ expQueue = {}
  /\ clock = MaxClock
  /\ usedConns = Conns
  /\ UNCHANGED vars

Next ==
  \/ \E c \in Conns : OpenConn(c) \/ DisconnectConn(c)
  \/ AdvanceClock
  \/ \E s \in AllSeats, i \in Incarnations, c \in Conns,
        ln \in Lanes, m \in {"Socket", "Heartbeat"}, tk \in BOOLEAN :
       Bind(s, i, c, ln, m, tk)
  \/ \E i \in Incarnations : QueueHeartbeat(i) \/ RunQueuedHeartbeat(i) \/ QueueLeaseExpire(i)
  \/ \E entry \in discQueue : RunQueuedDisconnectUnbind(entry)
  \/ \E entry \in expQueue : RunQueuedExpire(entry)
  \/ IdleStutter

Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ \A i \in Incarnations :
       WF_vars(RunQueuedHeartbeat(i))
  /\ \A entry \in (Incarnations \X Conns \X (1..MaxEpoch)) :
       WF_vars(entry \in discQueue /\ RunQueuedDisconnectUnbind(entry))
  /\ \A entry \in (Incarnations \X (1..MaxEpoch) \X (0..MaxClock)) :
       WF_vars(entry \in expQueue /\ RunQueuedExpire(entry))

StatechartView == <<status, connOf, epochOf, liveConns, discQueue>>

\* ---- Safety Invariants ----

StaleDisconnectNeverKillsReboundIncarnation ==
  ~staleUnbindKilled

StaleExpireNeverKillsHeartbeatedBinding ==
  ~staleExpireKilled

AtMostOneLivePerLane ==
  \A s \in AllSeats, ln \in Lanes :
    Cardinality({i \in Incarnations : seatOf[i] = s /\ laneOf[i] = ln /\ status[i] = "Live"}) <= 1

OperatorSurfaceNeverBound ==
  \A i \in Incarnations : seatOf[i] = OperatorSeat => status[i] = "Absent"

\* ---- Temporal Liveness Properties ----

\* Any socket-lifetime binding whose connection dropped either rebinds onto a live socket or unbinds.
DisconnectedSocketEventuallyUnbound ==
  \A i \in Incarnations :
    []((status[i] = "Live" /\ connOf[i] \notin liveConns)
       => <>(status[i] # "Live" \/ connOf[i] \in liveConns))

\* ---- Anti-Vacuity Reachability Witnesses (must FAIL under Mutant = "none") ----

NeverReboundAfterDisconnect == ~(reboundAfterDrop /\ discQueue = {} /\ \E i \in Incarnations : status[i] = "Live")
NeverHeartbeatedBeforeQueuedExpire == ~(heartbeatedVsSweep /\ expQueue = {} /\ \E i \in Incarnations : status[i] = "Live")
NeverSupersededOnTakeover == ~supersededWitness
=============================================================================
