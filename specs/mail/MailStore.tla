---------------------------- MODULE MailStore ----------------------------
(* Content-addressed mail storage guarantees: messages immutable and named by
   content hash; single SQLite commit point with atomic Markdown mirror export
   after commit; drop-folder ingest whether the hub is up or down; startup
   reconcile; lossless rebuild from mirror + ack ledger; edited-after-ack files
   flagged and never re-rung as new mail.

   Messages are hashes. pathOf maps a hash to its human-readable Markdown letter
   path (`mail/<slug>.md`). That path holds at most ONE current content hash at a
   time (`mirrorPath`), while `blobStore` (`objects/<sha256>.md`) preserves every
   exported or ingested immutable content blob so an in-place edit after ack
   neither creates a multi-hash path nor loses the historical pre-edit content on
   database rebuild. The hub crashes at most MaxCrash times between any two steps
   (including between database commit and export).

   Mutant:
     "none"                  the production design;
     "success_before_commit" send reports success before committing durably;
     "no_reconcile"          a row is exported only right after the send that
                             committed it, leaving post-crash rows unexported;
     "edit_reingests"        an edited file is ingested as a new ringable letter;
     "ack_not_mirrored"      acks stay in the database without mirroring to the ledger. *)
EXTENDS Naturals, FiniteSets

CONSTANTS Msgs, pathOf, Edits, MaxCrash, Mutant

VARIABLES fresh, up, crashes, db, mirrorPath, blobStore, drop, acked, ledger, flagged, sent, pending, reconciled, sendable, dropable, editable

vars == <<fresh, up, crashes, db, mirrorPath, blobStore, drop, acked, ledger, flagged, sent, pending, reconciled, sendable, dropable, editable>>

ASSUME \A e \in Edits : e[1] # e[2] /\ pathOf[e[1]] = pathOf[e[2]]
Paths == {pathOf[m] : m \in Msgs}
NoMsg == "NONE"
EditTargets == {e[2] : e \in Edits}

PathHashes(mp) == {mp[p] : p \in {q \in Paths : mp[q] # NoMsg}}
MirrorAll == blobStore \cup PathHashes(mirrorPath)

Init ==
  /\ fresh = {} /\ up = TRUE /\ crashes = 0
  /\ db = {}
  /\ mirrorPath = [p \in Paths |-> NoMsg]
  /\ blobStore = {}
  /\ drop = {} /\ acked = {} /\ ledger = {} /\ flagged = {}
  /\ sent = {} /\ pending = {} /\ reconciled = TRUE
  /\ sendable = Msgs \ EditTargets
  /\ dropable = Msgs \ EditTargets
  /\ editable = Edits

IsEdit(m, ack) == \E m0 \in ack : m0 # m /\ pathOf[m0] = pathOf[m]

Send(m) ==
  /\ up /\ reconciled /\ m \in sendable
  /\ sendable' = sendable \ {m} /\ dropable' = dropable \ {m}
  /\ IF Mutant = "success_before_commit"
     THEN sent' = sent \cup {m} /\ pending' = pending \cup {m} /\ db' = db
     ELSE db' = db \cup {m} /\ sent' = sent \cup {m} /\ pending' = pending
  /\ fresh' = fresh \cup {m}
  /\ UNCHANGED <<up, crashes, mirrorPath, blobStore, drop, acked, ledger, flagged, reconciled, editable>>

Drop(m) ==
  /\ m \in dropable
  /\ dropable' = dropable \ {m} /\ sendable' = sendable \ {m}
  /\ drop' = drop \cup {m} /\ sent' = sent \cup {m}
  /\ UNCHANGED <<fresh, up, crashes, db, mirrorPath, blobStore, acked, ledger, flagged, pending, reconciled, editable>>

EditAfterAck(e) ==
  /\ e \in editable /\ e[1] \in ledger /\ mirrorPath[pathOf[e[1]]] = e[1]
  /\ editable' = editable \ {e}
  /\ mirrorPath' = [mirrorPath EXCEPT ![pathOf[e[1]]] = e[2]]
  /\ UNCHANGED <<fresh, up, crashes, db, blobStore, drop, acked, ledger, flagged, sent, pending, reconciled, sendable, dropable>>

Crash ==
  /\ up /\ crashes < MaxCrash
  /\ up' = FALSE /\ crashes' = crashes + 1 /\ pending' = {}
  /\ fresh' = {}
  /\ UNCHANGED <<db, mirrorPath, blobStore, drop, acked, ledger, flagged, sent, reconciled, sendable, dropable, editable>>

Restart ==
  /\ ~up /\ up' = TRUE /\ reconciled' = FALSE
  /\ UNCHANGED <<fresh, crashes, db, mirrorPath, blobStore, drop, acked, ledger, flagged, sent, pending, sendable, dropable, editable>>

Commit(m) ==
  /\ up /\ m \in pending
  /\ db' = db \cup {m} /\ pending' = pending \ {m} /\ fresh' = fresh \cup {m}
  /\ UNCHANGED <<up, crashes, mirrorPath, blobStore, drop, acked, ledger, flagged, sent, reconciled, sendable, dropable, editable>>

Export(m) ==
  /\ up /\ m \in db
  /\ (m \notin blobStore \/ mirrorPath[pathOf[m]] = NoMsg)
  /\ (Mutant # "no_reconcile" \/ m \in fresh)
  /\ blobStore' = blobStore \cup {m}
  /\ mirrorPath' =
       IF mirrorPath[pathOf[m]] = NoMsg
       THEN [mirrorPath EXCEPT ![pathOf[m]] = m]
       ELSE mirrorPath
  /\ UNCHANGED <<fresh, up, crashes, db, drop, acked, ledger, flagged, sent, pending, reconciled, sendable, dropable, editable>>

Ingest(m) ==
  /\ up /\ (m \in drop \/ (m \in PathHashes(mirrorPath) /\ m \notin db))
  /\ db' = db \cup {m} /\ drop' = drop \ {m}
  /\ flagged' = IF Mutant # "edit_reingests" /\ IsEdit(m, acked) THEN flagged \cup {m} ELSE flagged
  /\ UNCHANGED <<fresh, up, crashes, mirrorPath, blobStore, acked, ledger, sent, pending, reconciled, sendable, dropable, editable>>

Reconciled ==
  /\ up /\ ~reconciled
  /\ (Mutant = "no_reconcile" \/ db \subseteq blobStore)
  /\ PathHashes(mirrorPath) \subseteq db
  /\ blobStore \subseteq db
  /\ drop = {}
  /\ reconciled' = TRUE
  /\ UNCHANGED <<fresh, up, crashes, db, mirrorPath, blobStore, drop, acked, ledger, flagged, sent, pending, sendable, dropable, editable>>

Ringable == db \ (acked \cup flagged)

Ack(m) ==
  /\ up /\ m \in Ringable
  /\ acked' = acked \cup {m}
  /\ ledger' = IF Mutant = "ack_not_mirrored" THEN ledger ELSE ledger \cup {m}
  /\ UNCHANGED <<fresh, up, crashes, db, mirrorPath, blobStore, drop, flagged, sent, pending, reconciled, sendable, dropable, editable>>

Hub(m) == Commit(m) \/ Export(m) \/ Ingest(m) \/ Ack(m)

Next ==
  \/ \E m \in Msgs : Send(m) \/ Drop(m) \/ Hub(m)
  \/ \E e \in Edits : EditAfterAck(e)
  \/ Crash \/ Restart \/ Reconciled

Spec == Init /\ [][Next]_vars /\ WF_vars(Restart) /\ WF_vars(Reconciled)
          /\ \A m \in Msgs : WF_vars(Commit(m)) /\ WF_vars(Export(m)) /\ WF_vars(Ingest(m))

(* ---- the promises ---- *)
\* Structural type-shape conjunct enforced by construction on Export and EditAfterAck:
\* each working-tree path names at most one message hash, while blobStore retains all hashes.
SingleHashPerPath ==
  /\ mirrorPath \in [Paths -> Msgs \cup {NoMsg}]
  /\ \A p \in Paths : mirrorPath[p] # NoMsg => pathOf[mirrorPath[p]] = p

RebuiltDb    == MirrorAll \cup drop
RebuiltAcked == ledger
RebuiltFlags == {m \in MirrorAll : IsEdit(m, ledger)}

Quiescent ==
  /\ up /\ reconciled /\ pending = {} /\ drop = {}
  /\ db = blobStore
  /\ PathHashes(mirrorPath) \subseteq db
  /\ \A p \in Paths : (\E m \in db : pathOf[m] = p) => mirrorPath[p] # NoMsg

NoLostSend == \A m \in sent : m \in db \cup drop \cup MirrorAll \cup pending
DurableSend == \A m \in sent : m \in db \cup drop \cup MirrorAll
RebuildParity == SingleHashPerPath /\ (Quiescent => (db = RebuiltDb /\ acked = RebuiltAcked /\ flagged = RebuiltFlags))
EditNeverRingable == \A e \in Edits : e[2] \notin Ringable
ReachesRest == <>[]Quiescent

(* Non-vacuity: each must be reachable in the design (checked as invariants that must FAIL). *)
NeverFlagged   == flagged = {}
NeverRebuiltAfterCrash == ~(Quiescent /\ crashes > 0 /\ acked # {})
=============================================================================
