-------------------- MODULE rollback_anchor_static --------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
Static crash-fault model for Meldbase rollback-anchor protocol v2.
This specification deliberately excludes live reconfiguration and Byzantine
members. A coordinate is [sequence |-> Nat, generation |-> Nat].
***************************************************************************)

\* Sets the model members and upper bounds for commit sequences and generations.
CONSTANT Members, MaxSequence, MaxGeneration
\* Requires an odd number of members so every quorum is a strict majority.
ASSUME Cardinality(Members) % 2 = 1

\* Quorum is the minimum number of members needed for a majority.
Quorum == (Cardinality(Members) \div 2) + 1
\* Coord pairs a commit sequence with a physical storage generation.
Coord(s, g) == [sequence |-> s, generation |-> g]
\* Leq checks whether both coordinates of a are no greater than those of b.
Leq(a, b) == /\ a.sequence <= b.sequence
             /\ a.generation <= b.generation
\* Max selects b when it covers a, otherwise keeping a.
Max(a, b) == IF Leq(a, b) THEN b ELSE a

\* Tracks database and replica positions together with publication and recovery state.
VARIABLES db, replicas, acknowledged, pending, target, opened, rejected
\* vars groups all state variables for the temporal specification.
vars == <<db, replicas, acknowledged, pending, target, opened, rejected>>

\* Init starts the database and every replica at the same empty version.
Init ==
  /\ db = Coord(0, 1)
  /\ replicas = [m \in Members |-> Coord(0, 1)]
  /\ acknowledged = Coord(0, 1)
  /\ pending = FALSE
  /\ target = Coord(0, 1)
  /\ opened = TRUE
  /\ rejected = FALSE

\* BeginLogical starts a document commit that advances both sequence and generation.
BeginLogical ==
  /\ opened /\ ~pending
  /\ db.sequence < MaxSequence /\ db.generation < MaxGeneration
  /\ target' = Coord(db.sequence + 1, db.generation + 1)
  /\ db' = target'
  /\ pending' = TRUE
  /\ UNCHANGED <<replicas, acknowledged, opened, rejected>>

\* BeginMaintenance starts a storage update that advances only the generation.
BeginMaintenance ==
  /\ opened /\ ~pending
  /\ db.generation < MaxGeneration
  /\ target' = Coord(db.sequence, db.generation + 1)
  /\ db' = target'
  /\ pending' = TRUE
  /\ UNCHANGED <<replicas, acknowledged, opened, rejected>>

\* Deliver advances one replica toward the pending target without moving it backward.
Deliver(m) ==
  /\ pending /\ m \in Members
  /\ replicas' = [replicas EXCEPT ![m] = Max(@, target)]
  /\ UNCHANGED <<db, acknowledged, pending, target, opened, rejected>>

\* AckCount counts members whose stored positions cover the pending target.
AckCount == Cardinality({m \in Members : Leq(target, replicas[m])})

\* Acknowledge records a completed update after a majority covers its target.
Acknowledge ==
  /\ pending /\ AckCount >= Quorum
  /\ acknowledged' = target
  /\ pending' = FALSE
  /\ UNCHANGED <<db, replicas, target, opened, rejected>>

\* Crash interrupts a pending update and closes the database.
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
\* InstallRollback replaces a closed database with an earlier environment-supplied version.
InstallRollback(s, g) ==
  /\ ~opened /\ ~pending /\ g > s
  /\ db' = Coord(s, g)
  /\ UNCHANGED <<replicas, acknowledged, pending, target, opened, rejected>>

\* QuorumValues collects the stored positions of a selected read quorum.
QuorumValues(Q) == {replicas[m] : m \in Q}
\* HasFloor checks whether one quorum position covers every other position in that quorum.
HasFloor(Q) == \E floor \in QuorumValues(Q) :
  \A value \in QuorumValues(Q) : Leq(value, floor)
\* ReadFloor selects a quorum position that covers every position in that quorum.
ReadFloor(Q) == CHOOSE floor \in QuorumValues(Q) :
  \A value \in QuorumValues(Q) : Leq(value, floor)

\* RejectRecovery keeps the database closed when a read quorum is ahead of it.
RejectRecovery(Q) ==
  /\ ~opened /\ ~pending
  /\ Q \subseteq Members /\ Cardinality(Q) = Quorum
  /\ HasFloor(Q)
  /\ ~Leq(ReadFloor(Q), db)
  /\ rejected' = TRUE
  /\ opened' = FALSE
  /\ UNCHANGED <<db, replicas, acknowledged, pending, target>>

\* OpenEqual opens a database whose position equals the selected read quorum floor.
OpenEqual(Q) ==
  /\ ~opened /\ ~pending
  /\ Q \subseteq Members /\ Cardinality(Q) = Quorum
  /\ HasFloor(Q)
  /\ Leq(ReadFloor(Q), db) /\ Leq(db, ReadFloor(Q))
  /\ opened' = TRUE
  /\ rejected' = FALSE
  /\ UNCHANGED <<db, replicas, acknowledged, pending, target>>

\* RepairAndOpen advances a write quorum before opening a database ahead of the read quorum floor.
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

\* Next permits one publication, crash, rollback, or recovery transition.
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

\* TypeOK requires every stored generation to remain greater than its commit sequence.
TypeOK ==
  /\ db.generation > db.sequence
  /\ acknowledged.generation > acknowledged.sequence
  /\ \A m \in Members : replicas[m].generation > replicas[m].sequence

\* AcknowledgementSound requires an open or pending database to cover its last acknowledged position.
AcknowledgementSound == (opened \/ pending) => Leq(acknowledged, db)
\* EveryReadQuorumRetainsAcknowledgedFloor requires every defined read quorum floor to cover the acknowledged position.
EveryReadQuorumRetainsAcknowledgedFloor ==
  \A Q \in SUBSET Members :
    Cardinality(Q) = Quorum /\ HasFloor(Q) =>
      Leq(acknowledged, ReadFloor(Q))
\* OpenNeverBehindAcknowledged requires an open database to cover the last acknowledged position.
OpenNeverBehindAcknowledged == opened => Leq(acknowledged, db)
\* RejectNeverOpens requires rejected recovery to leave the database closed.
RejectNeverOpens == rejected => ~opened

\* Spec starts at Init and permits Next transitions or unchanged state.
Spec == Init /\ [][Next]_vars

=============================================================================
