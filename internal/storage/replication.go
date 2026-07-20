package storage

import (
	"errors"
	"math"
	"time"
)

// ApplyReplicationNoop advances a replica's logical Commit Log position for a
// source batch whose public projection is empty. The marker is private to the
// target's own Commit Log: it carries no user document or System-record value,
// but preserves the contiguous token contract required before a later public
// batch can be applied.
//
// Replication of private System-record contents is deliberately not implied by
// this method. A follower is read-only and must not serve source-side RPC or
// idempotency ownership; those records need a separately authenticated control
// plane if they are ever replicated.
func (f *File) ApplyReplicationNoop(transactionID [16]byte, committedAt time.Time) (uint64, error) {
	if f == nil || allZero(transactionID[:]) {
		return 0, ErrCorrupt
	}
	if committedAt.IsZero() {
		committedAt = time.Now()
	}
	var sequence uint64
	err := f.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		base := tx.BaseRoot()
		if base.CommitSequence == math.MaxUint64 {
			return DatabaseRoot{}, ErrCorrupt
		}
		sequence = tx.Sequence()
		commitLogRoot, oldest, err := tx.AppendCommitRetained(base.CommitLogRoot, base.OldestRetainedSequence, CommitBatch{
			Sequence: sequence, TransactionID: transactionID, CommittedAt: committedAt, CatalogRoot: base.CatalogRoot,
			Changes: []CommitChange{{CollectionID: math.MaxUint32, Operation: CommitCatalog, ChangedPaths: []string{"_replication.noop"}}},
		})
		if err != nil {
			return DatabaseRoot{}, err
		}
		base.CommitSequence = sequence
		base.CommitLogRoot = commitLogRoot
		base.OldestRetainedSequence = oldest
		return base, nil
	})
	if err != nil {
		return 0, err
	}
	if sequence == 0 {
		return 0, errors.New("meldbase storage v2: replication noop sequence missing")
	}
	return sequence, nil
}
