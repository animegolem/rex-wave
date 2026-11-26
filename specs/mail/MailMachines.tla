---------------------------- MODULE MailMachines ----------------------------
(* Multi-machine courier adapters, AdapterDegraded alerts, and union store sync.

   Each machine runs its own hub; a seat's route claim names its machine, and
   only that machine's hub rings the seat. Message and ack stores cross machines
   by union sync (messages are immutable and content-addressed). A courier
   answers "rung" when healthy and "unknown" when its adapter is broken;
   "unknown" counts as rung (never repeated), and K unknowns in a row raise
   AdapterDegraded to the hub's owner. Seats may also read and ack a stored
   letter directly before the poll pass rings it.

   Mutant:
     "none"             the production design;
     "ring_any_machine" every hub rings any seat it has mail for, regardless of
                        which machine owns the seat's route claim;
     "retry_unknown"    an "unknown" courier outcome is retried instead of
                        counting as rung;
     "no_degrade"       unknowns are absorbed silently without raising AdapterDegraded;
     "sync_overwrite"   a sync overwrites the destination store instead of unioning;
     "ring_after_ack"   MayRing omits the ack ledger check and rings an already-acked letter. *)
EXTENDS Naturals, FiniteSets

CONSTANTS Machines, Seats, Letters, To, K, Mutant, None

VARIABLES home, store, acks, written, rungBy, rings, broken, unk, ghostUnk, degraded, ringAfterAck

vars == <<home, store, acks, written, rungBy, rings, broken, unk, ghostUnk, degraded, ringAfterAck>>

Init ==
  /\ home = [s \in Seats |-> None]
  /\ store = [mc \in Machines |-> {}] /\ acks = [mc \in Machines |-> {}]
  /\ written = {}
  /\ rungBy = [l \in Letters |-> {}] /\ rings = [l \in Letters |-> 0]
  /\ broken = [s \in Seats |-> FALSE]
  /\ unk = [s \in Seats |-> 0] /\ ghostUnk = [s \in Seats |-> 0]
  /\ degraded = {} /\ ringAfterAck = {}

AllAcks == UNION {acks[mc] : mc \in Machines}

Write(l, mc) ==
  /\ l \notin written
  /\ written' = written \cup {l} /\ store' = [store EXCEPT ![mc] = @ \cup {l}]
  /\ UNCHANGED <<home, acks, rungBy, rings, broken, unk, ghostUnk, degraded, ringAfterAck>>

Claim(s, mc) ==
  /\ home[s] = None /\ home' = [home EXCEPT ![s] = mc]
  /\ UNCHANGED <<store, acks, written, rungBy, rings, broken, unk, ghostUnk, degraded, ringAfterAck>>

Break(s) ==
  /\ ~broken[s] /\ broken' = [broken EXCEPT ![s] = TRUE]
  /\ UNCHANGED <<home, store, acks, written, rungBy, rings, unk, ghostUnk, degraded, ringAfterAck>>

Sync(a, b) ==
  /\ a # b
  /\ store' = [store EXCEPT ![b] = IF Mutant = "sync_overwrite" THEN store[a] ELSE @ \cup store[a]]
  /\ acks' = [acks EXCEPT ![b] = @ \cup acks[a]]
  /\ UNCHANGED <<home, written, rungBy, rings, broken, unk, ghostUnk, degraded, ringAfterAck>>

MayRing(mc, l) ==
  /\ l \in store[mc]
  /\ (Mutant = "ring_after_ack" \/ l \notin acks[mc])
  /\ home[To[l]] # None
  /\ (Mutant = "ring_any_machine" \/ home[To[l]] = mc)
  /\ (rungBy[l] = {} \/ (Mutant = "retry_unknown" /\ rings[l] < 2) \/ (Mutant = "ring_any_machine" /\ mc \notin rungBy[l]))

Ring(mc, l) ==
  /\ MayRing(mc, l)
  /\ LET s == To[l] IN
     /\ rungBy' = [rungBy EXCEPT ![l] = @ \cup {mc}]
     /\ rings' = [rings EXCEPT ![l] = @ + 1]
     /\ ringAfterAck' = IF l \in AllAcks THEN ringAfterAck \cup {l} ELSE ringAfterAck
     /\ IF broken[s]
        THEN
             /\ ghostUnk' = [ghostUnk EXCEPT ![s] = @ + 1]
             /\ unk' = [unk EXCEPT ![s] = IF Mutant = "no_degrade" THEN 0 ELSE @ + 1]
             /\ degraded' = IF Mutant # "no_degrade" /\ unk[s] + 1 >= K THEN degraded \cup {s} ELSE degraded
        ELSE
             /\ ghostUnk' = [ghostUnk EXCEPT ![s] = 0] /\ unk' = [unk EXCEPT ![s] = 0]
             /\ degraded' = degraded
  /\ UNCHANGED <<home, store, acks, written, broken>>

Ack(l) ==
  /\ ~broken[To[l]] /\ home[To[l]] # None
  /\ (rungBy[l] # {} \/ l \in store[home[To[l]]])
  /\ l \notin acks[home[To[l]]]
  /\ acks' = [acks EXCEPT ![home[To[l]]] = @ \cup {l}]
  /\ UNCHANGED <<home, store, written, rungBy, rings, broken, unk, ghostUnk, degraded, ringAfterAck>>

Next ==
  \/ \E l \in Letters, mc \in Machines : Write(l, mc) \/ Ring(mc, l)
  \/ \E s \in Seats, mc \in Machines : Claim(s, mc)
  \/ \E s \in Seats : Break(s)
  \/ \E a, b \in Machines : Sync(a, b)
  \/ \E l \in Letters : Ack(l)

Spec == Init /\ [][Next]_vars /\ \A a, b \in Machines : WF_vars(Sync(a, b))

(* ---- the promises ---- *)
OneMachineRings == \A l \in Letters : rungBy[l] \subseteq {home[To[l]]}
NoDoubleRing    == \A l \in Letters : rings[l] <= 1
DegradedLoud    == \A s \in Seats : ghostUnk[s] >= K => s \in degraded
NoLostLetter    == \A l \in written : \E mc \in Machines : l \in store[mc]
NoRingAfterAck  == ringAfterAck = {}
Converges       == <>[](\A a, b \in Machines : store[a] = store[b] /\ acks[a] = acks[b])

(* Non-vacuity: these must FAIL in the design *)
NeverDegraded == degraded = {}
NeverCrossMachine == ~(\E l \in Letters : \E mc \in Machines : l \in store[mc] /\ home[To[l]] # None /\ home[To[l]] # mc /\ rungBy[l] # {})
=============================================================================
