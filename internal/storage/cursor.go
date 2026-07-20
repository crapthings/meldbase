package storage

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"time"
)

var (
	ErrHistoryLost       = errors.New("meldbase storage: commit history is no longer retained")
	ErrHistoryPinned     = errors.New("meldbase storage: commit history is pinned by a live replay cursor")
	ErrNoDeliveredCommit = errors.New("meldbase storage: live stream has not delivered a commit")
	ErrCursorClosed      = errors.New("meldbase storage: commit cursor is closed")
)

// CommitCursor pins one immutable Commit Log root and replays a finite,
// gap-checked sequence range. A later live-stream layer tails new roots after
// this snapshot cursor reaches Through().
type CommitCursor struct {
	mu       sync.Mutex
	file     *File
	pinID    uint64
	rootPage uint64
	after    uint64
	through  uint64
	closed   bool
}

func (f *File) OpenCommitCursor(after uint64) (*CommitCursor, error) {
	if f == nil {
		return nil, ErrCorrupt
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return nil, errors.New("meldbase storage: file is closed")
	}
	root, err := f.databaseRootUnlocked()
	if err != nil {
		return nil, err
	}
	if after > root.CommitSequence {
		return nil, ErrCorrupt
	}
	if after < root.CommitSequence && (after == math.MaxUint64 || after+1 < root.OldestRetainedSequence) {
		return nil, ErrHistoryLost
	}
	f.nextPin++
	if f.nextPin == 0 {
		return nil, ErrCorrupt
	}
	pinID := f.nextPin
	f.readers[pinID] = readerPin{generation: f.meta.Generation, sequence: root.CommitSequence, rootPage: f.meta.RootPage}
	return &CommitCursor{file: f, pinID: pinID, rootPage: root.CommitLogRoot, after: after, through: root.CommitSequence}, nil
}

func (cursor *CommitCursor) Next() (CommitBatch, bool, error) {
	if cursor == nil {
		return CommitBatch{}, false, ErrCursorClosed
	}
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	if cursor.closed || cursor.file == nil {
		return CommitBatch{}, false, ErrCursorClosed
	}
	if cursor.after >= cursor.through {
		return CommitBatch{}, false, nil
	}
	sequence := cursor.after + 1
	cursor.file.mu.RLock()
	defer cursor.file.mu.RUnlock()
	if cursor.file.file == nil {
		return CommitBatch{}, false, errors.New("meldbase storage: file is closed")
	}
	batch, err := cursor.file.readCommitUnlocked(cursor.rootPage, sequence)
	if err != nil {
		return CommitBatch{}, false, err
	}
	cursor.after = sequence
	return batch, true, nil
}

func (cursor *CommitCursor) Through() uint64 {
	if cursor == nil {
		return 0
	}
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	return cursor.through
}

func (cursor *CommitCursor) Close() error {
	if cursor == nil {
		return nil
	}
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	if cursor.closed {
		return nil
	}
	cursor.closed = true
	if cursor.file != nil {
		cursor.file.mu.Lock()
		delete(cursor.file.readers, cursor.pinID)
		cursor.file.mu.Unlock()
	}
	cursor.file = nil
	return nil
}

// RetainCommitsFrom atomically drops logical Commit Log entries older than
// keepFrom and appends an auditable retention commit. Physical page reclamation
// remains a separate epoch-safe phase.
func (f *File) RetainCommitsFrom(keepFrom uint64, transactionID [16]byte, committedAt time.Time) (uint64, error) {
	if f == nil || allZero(transactionID[:]) {
		return 0, ErrCorrupt
	}
	if committedAt.IsZero() {
		committedAt = time.Now()
	}
	var sequence uint64
	err := f.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		sequence = tx.Sequence()
		base := tx.BaseRoot()
		if base.CommitSequence == 0 || base.OldestRetainedSequence == 0 || keepFrom < base.OldestRetainedSequence || keepFrom > base.CommitSequence+1 {
			return DatabaseRoot{}, ErrCorrupt
		}
		for _, pin := range tx.file.readers {
			if pin.replay && keepFrom > pin.sequence && keepFrom-pin.sequence > 1 {
				return DatabaseRoot{}, ErrHistoryPinned
			}
		}
		tree, err := tx.OpenTree(base.CommitLogRoot, TreeCommitLog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		for retained := base.OldestRetainedSequence; retained < keepFrom; retained++ {
			batch, err := tx.readCommitFromTree(tree, retained)
			if err != nil {
				return DatabaseRoot{}, err
			}
			removedBytes, err := commitBatchLogicalBytes(batch)
			if err != nil || removedBytes > tx.retainedCommitBytes {
				return DatabaseRoot{}, ErrCorrupt
			}
			for ordinal := uint32(0); ordinal <= uint32(len(batch.Changes)); ordinal++ {
				removed, err := tree.Delete(commitKey(retained, ordinal))
				if err != nil || !removed {
					return DatabaseRoot{}, ErrCorrupt
				}
			}
			tx.retainedCommitBytes -= removedBytes
		}
		retentionValue := make([]byte, 8)
		binary.LittleEndian.PutUint64(retentionValue, keepFrom)
		addedBytes, err := tx.appendCommitToTree(tree, CommitBatch{
			Sequence: tx.Sequence(), TransactionID: transactionID, CommittedAt: committedAt,
			CatalogRoot: base.CatalogRoot,
			Changes:     []CommitChange{{CollectionID: math.MaxUint32, Operation: CommitCatalog, ChangedPaths: []string{"_system.retention"}, After: retentionValue}},
		})
		if err != nil || addedBytes > math.MaxUint64-tx.retainedCommitBytes {
			if err == nil {
				err = ErrCorrupt
			}
			return DatabaseRoot{}, err
		}
		tx.retainedCommitBytes += addedBytes
		keepFrom, err = tx.pruneCommitTree(tree, keepFrom, tx.Sequence())
		if err != nil {
			return DatabaseRoot{}, err
		}
		commitLogRoot, err := tree.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		return DatabaseRoot{
			CommitSequence: tx.Sequence(), CatalogRoot: base.CatalogRoot, CommitLogRoot: commitLogRoot,
			FreeSpaceRoot: base.FreeSpaceRoot, OldestRetainedSequence: keepFrom,
			CatalogGeneration: base.CatalogGeneration, DocumentCount: base.DocumentCount, CollectionCount: base.CollectionCount,
		}, nil
	})
	return sequence, err
}
