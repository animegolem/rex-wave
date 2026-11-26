---------------------------- MODULE MailHub ----------------------------
(* Mail hub delivery guarantees as a state machine.

   Sessions start, fork, rename, and end freely (the environment). A seat claims
   its route at every start; a canary rung there and acked from inside confirms
   it. A letter is rung only at its seat's confirmed, live route; it waits while
   there is none; after MaxTries waits it is refused loudly.

   Mutant plants a known-bad design; TLC must catch each one:
     "none"          the production design;
     "pinned_name"   the hub pins a route by session NAME at the seat's first
                     start (breaks when a session returns under a new name or
                     when a fork inherits its parent's name);
     "no_key"        a ring is retried without the (seat, sha256) key, so a rung
                     letter can be rung again;
     "silent_giveup" a give-up drops the letter without notifying anyone. *)
EXTENDS Naturals, FiniteSets

CONSTANTS Seats, Sessions, Names, Letters, To, MaxTries, Mutant, Fork, None

VARIABLES live, used, seatOf, name, route, confirmed, pinned,
          st, rings, ringTo, tries, loud, falseGiveUp

vars == <<live, used, seatOf, name, route, confirmed, pinned, st, rings, ringTo, tries, loud, falseGiveUp>>

TypeOK ==
  /\ live \subseteq used /\ used \subseteq Sessions
  /\ seatOf \in [Sessions -> Seats \cup {Fork, None}]
  /\ name \in [Sessions -> Names \cup {None}]
  /\ route \in [Seats -> Sessions \cup {None}]
  /\ confirmed \in [Seats -> BOOLEAN]
  /\ pinned \in [Seats -> Names \cup {None}]
  /\ st \in [Letters -> {"new", "rung", "acked", "refused", "dropped"}]
  /\ rings \in [Letters -> 0..3]
  /\ ringTo \in [Letters -> SUBSET Sessions]
  /\ tries \in [Letters -> 0..MaxTries]
  /\ loud \subseteq Letters /\ falseGiveUp \subseteq Letters

Init ==
  /\ live = {} /\ used = {}
  /\ seatOf = [x \in Sessions |-> None] /\ name = [x \in Sessions |-> None]
  /\ route = [s \in Seats |-> None] /\ confirmed = [s \in Seats |-> FALSE]
  /\ pinned = [s \in Seats |-> None]
  /\ st = [l \in Letters |-> "new"] /\ rings = [l \in Letters |-> 0]
  /\ ringTo = [l \in Letters |-> {}] /\ tries = [l \in Letters |-> 0]
  /\ loud = {} /\ falseGiveUp = {}

Mail == <<st, rings, ringTo, tries, loud, falseGiveUp>>

(* ---- the environment: sessions come and go ---- *)
Start(s, x, n) ==
  /\ x \notin used
  /\ used' = used \cup {x} /\ live' = live \cup {x}
  /\ seatOf' = [seatOf EXCEPT ![x] = s] /\ name' = [name EXCEPT ![x] = n]
  /\ route' = [route EXCEPT ![s] = x] /\ confirmed' = [confirmed EXCEPT ![s] = FALSE]
  /\ pinned' = IF pinned[s] = None THEN [pinned EXCEPT ![s] = n] ELSE pinned
  /\ UNCHANGED Mail

ForkS(x, y) ==
  /\ x \in live /\ y \notin used
  /\ used' = used \cup {y} /\ live' = live \cup {y}
  /\ seatOf' = [seatOf EXCEPT ![y] = Fork] /\ name' = [name EXCEPT ![y] = name[x]]
  /\ UNCHANGED <<route, confirmed, pinned>> /\ UNCHANGED Mail

Rename(x, n) ==
  /\ x \in live /\ name[x] # n
  /\ name' = [name EXCEPT ![x] = n]
  /\ UNCHANGED <<live, used, seatOf, route, confirmed, pinned>> /\ UNCHANGED Mail

End(x) ==
  /\ x \in live /\ live' = live \ {x}
  /\ UNCHANGED <<used, seatOf, name, route, confirmed, pinned>> /\ UNCHANGED Mail

Canary(s) ==
  /\ route[s] \in live /\ seatOf[route[s]] = s /\ ~confirmed[s]
  /\ confirmed' = [confirmed EXCEPT ![s] = TRUE]
  /\ UNCHANGED <<live, used, seatOf, name, route, pinned>> /\ UNCHANGED Mail

(* ---- the hub ---- *)
Target(l) ==
  IF Mutant = "pinned_name"
  THEN {x \in live : name[x] = pinned[To[l]]}
  ELSE IF confirmed[To[l]] /\ route[To[l]] \in live THEN {route[To[l]]} ELSE {}

Reachable(s) == confirmed[s] /\ route[s] \in live /\ seatOf[route[s]] = s

Ring(l) ==
  /\ (st[l] = "new" \/ (Mutant = "no_key" /\ st[l] = "rung"))
  /\ rings[l] < 3
  /\ \E x \in Target(l):
       /\ ringTo' = [ringTo EXCEPT ![l] = @ \cup {x}]
       /\ rings' = [rings EXCEPT ![l] = @ + 1]
       /\ st' = [st EXCEPT ![l] = "rung"]
  /\ UNCHANGED <<tries, loud, falseGiveUp>>

Defer(l) ==
  /\ st[l] = "new" /\ Target(l) = {} /\ tries[l] < MaxTries
  /\ tries' = [tries EXCEPT ![l] = @ + 1]
  /\ UNCHANGED <<st, rings, ringTo, loud, falseGiveUp>>

GiveUp(l) ==
  /\ st[l] = "new" /\ Target(l) = {} /\ tries[l] = MaxTries
  /\ IF Mutant = "silent_giveup"
     THEN st' = [st EXCEPT ![l] = "dropped"] /\ loud' = loud
     ELSE st' = [st EXCEPT ![l] = "refused"] /\ loud' = loud \cup {l}
  /\ falseGiveUp' = IF Reachable(To[l]) THEN falseGiveUp \cup {l} ELSE falseGiveUp
  /\ UNCHANGED <<rings, ringTo, tries>>

Ack(l) ==
  /\ st[l] = "rung" /\ st' = [st EXCEPT ![l] = "acked"]
  /\ UNCHANGED <<rings, ringTo, tries, loud, falseGiveUp>>

Hub == <<live, used, seatOf, name, route, confirmed, pinned>>

Env ==
  \/ \E s \in Seats, x \in Sessions, n \in Names : Start(s, x, n)
  \/ \E x, y \in Sessions : ForkS(x, y)
  \/ \E x \in Sessions, n \in Names : Rename(x, n)
  \/ \E x \in Sessions : End(x)
  \/ \E s \in Seats : Canary(s)

Deliver(l) == Ring(l) \/ Defer(l) \/ GiveUp(l) \/ Ack(l)

Next == Env \/ (\E l \in Letters : Deliver(l) /\ UNCHANGED Hub)

Spec == Init /\ [][Next]_vars
          /\ \A l \in Letters : SF_vars(Ring(l) /\ UNCHANGED Hub) /\ WF_vars(Defer(l) /\ UNCHANGED Hub)
                                /\ SF_vars(GiveUp(l) /\ UNCHANGED Hub) /\ WF_vars(Ack(l) /\ UNCHANGED Hub)

(* ---- the promises ---- *)
NoDoubleRing == \A l \in Letters : rings[l] <= 1
RightSeat    == \A l \in Letters : \A x \in ringTo[l] : seatOf[x] = To[l]
LoudGiveUp   == \A l \in Letters : st[l] \in {"refused", "dropped"} => l \in loud
NoFalseGiveUp == falseGiveUp = {}
Settles      == \A l \in Letters : <>(st[l] \in {"acked", "refused", "dropped"})

(* Non-vacuity: these must be REACHABLE in the design (checked as invariants that must FAIL). *)
NeverRung     == \A l \in Letters : st[l] # "rung"
NeverRefused  == \A l \in Letters : st[l] # "refused"
NeverForked   == \A x \in Sessions : seatOf[x] # Fork
=============================================================================
