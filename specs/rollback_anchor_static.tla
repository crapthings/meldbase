-------------------- MODULE rollback_anchor_static --------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
Static crash-fault model for Meldbase rollback-anchor protocol v2.
This specification deliberately excludes live reconfiguration and Byzantine
members. A coordinate is [sequence |-> Nat, generation |-> Nat].
***************************************************************************)

CONSTANT Members, MaxSequence, MaxGeneration
ASSUME Cardinality(Members) % 2 = 1

Quorum == (Cardinality(Members) \div 2) + 1
Coord(s, g) == [sequence |-> s, generation |-> g]
Leq(a, b) == /\ a.sequence <= b.sequence
             /\ a.generation <= b.generation
Max(a, b) == IF Leq(a, b) THEN b ELSE a

VARIABLES db, replicas, acknowledged, pending, target, opened, rejected
vars == <<db, replicas, acknowledged, pending, target, opened, rejected>>

Init ==
  /\ db = Coord(0, 1)
  /\ replicas = [m \in Members |-> Coord(0, 1)]
  /\ acknowledged = Coord(0, 1)
  /\ pending = FALSE
  /\ target = Coord(0, 1)
  /\ opened = TRUE
  /\ rejected = FALSE

BeginLogical ==
  /\ opened /\ ~pending
  /\ db.sequence < MaxSequence /\ db.generation < MaxGeneration
  /\ target' = Coord(db.sequence + 1, db.generation + 1)
  /\ db' = target'
  /\ pending' = TRUE
  /\ UNCHANGED <<replicas, acknowledged, opened, rejected>>

BeginMaintenance ==
  /\ opened /\ ~pending
  /\ db.generation < MaxGeneration
  /\ target' = Coord(db.sequence, db.generation + 1)
  /\ db' = target'
  /\ pending' = TRUE
  /\ UNCHANGED <<replicas, acknowledged, opened, rejected>>

Deliver(m) ==
  /\ pending /\ m \in Members
  /\ replicas' = [replicas EXCEPT ![m] = Max(@, target)]
  /\ UNCHANGED <<db, acknowledged, pending, target, opened, rejected>>

AckCount == Cardinality({m \in Members : Leq(target, replicas[m])})

Acknowledge ==
  /\ pending /\ AckCount >= Quorum
  /\ acknowledged' = target
  /\ pending' = FALSE
  /\ UNCHANGED <<db, replicas, target, opened, rejected>>

Crash ==
  /\ pending
  /\ pending' = FALSE
  /\ opened' = FALSE
  /\ UNCHANGED <<db, replicas, acknowledged, target, rejected>>

(***************************************************************************
Rollback is an environment fault. Recovery must reject whenever any valid
read-quorum floor is ahead of the replacement database. The executable Go
model enumerates every read quorum and every recovery write quorum.
***************************************************************************)
InstallRollback(s, g) ==
  /\ ~opened /\ ~pending /\ g > s
  /\ db' = Coord(s, g)
  /\ UNCHANGED <<replicas, acknowledged, pending, target, opened, rejected>>

QuorumValues(Q) == {replicas[m] : m \in Q}
HasFloor(Q) == \E floor \in QuorumValues(Q) :
  \A value \in QuorumValues(Q) : Leq(value, floor)
ReadFloor(Q) == CHOOSE floor \in QuorumValues(Q) :
  \A value \in QuorumValues(Q) : Leq(value, floor)

RejectRecovery(Q) ==
  /\ ~opened /\ ~pending
  /\ Q \subseteq Members /\ Cardinality(Q) = Quorum
  /\ HasFloor(Q)
  /\ ~Leq(ReadFloor(Q), db)
  /\ rejected' = TRUE
  /\ opened' = FALSE
  /\ UNCHANGED <<db, replicas, acknowledged, pending, target>>

OpenEqual(Q) ==
  /\ ~opened /\ ~pending
  /\ Q \subseteq Members /\ Cardinality(Q) = Quorum
  /\ HasFloor(Q)
  /\ Leq(ReadFloor(Q), db) /\ Leq(db, ReadFloor(Q))
  /\ opened' = TRUE
  /\ rejected' = FALSE
  /\ UNCHANGED <<db, replicas, acknowledged, pending, target>>

RepairAndOpen(Q, W) ==
  /\ ~opened /\ ~pending
  /\ Q \subseteq Members /\ Cardinality(Q) = Quorum
  /\ W \subseteq Members /\ Cardinality(W) = Quorum
  /\ HasFloor(Q)
  /\ Leq(ReadFloor(Q), db) /\ ~Leq(db, ReadFloor(Q))
  /\ replicas' = [m \in Members |-> IF m \in W THEN Max(replicas[m], db)
                                           ELSE replicas[m]]
  /\ opened' = TRUE
  /\ rejected' = FALSE
  /\ UNCHANGED <<db, acknowledged, pending, target>>

Next ==
  \/ BeginLogical
  \/ BeginMaintenance
  \/ \E m \in Members : Deliver(m)
  \/ Acknowledge
  \/ Crash
  \/ \E s \in 0..db.sequence, g \in 1..db.generation : InstallRollback(s, g)
  \/ \E Q \in SUBSET Members : RejectRecovery(Q)
  \/ \E Q \in SUBSET Members : OpenEqual(Q)
  \/ \E Q \in SUBSET Members, W \in SUBSET Members : RepairAndOpen(Q, W)

TypeOK ==
  /\ db.generation > db.sequence
  /\ acknowledged.generation > acknowledged.sequence
  /\ \A m \in Members : replicas[m].generation > replicas[m].sequence

AcknowledgementSound == (opened \/ pending) => Leq(acknowledged, db)
EveryReadQuorumRetainsAcknowledgedFloor ==
  \A Q \in SUBSET Members :
    Cardinality(Q) = Quorum /\ HasFloor(Q) =>
      Leq(acknowledged, ReadFloor(Q))
OpenNeverBehindAcknowledged == opened => Leq(acknowledged, db)
RejectNeverOpens == rejected => ~opened

Spec == Init /\ [][Next]_vars

=============================================================================
