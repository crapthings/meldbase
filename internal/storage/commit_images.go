package storage

import "errors"

// ResolvedCommitChange owns materialized before/after images. Empty images
// retain their operation-specific meaning (insert has no Before, delete has no
// After). ChangedPaths and image bytes never alias Commit Log or page-cache data.
type ResolvedCommitChange struct {
	CollectionID uint32
	DocumentID   [16]byte
	Operation    CommitOperation
	ChangedPaths []string
	Before       []byte
	After        []byte
}

// ResolveChange materializes a change returned by this finite cursor. The
// cursor's immutable root pin keeps referenced document versions valid until
// Close, even if logical retention advances concurrently.
func (cursor *CommitCursor) ResolveChange(change CommitChange) (ResolvedCommitChange, error) {
	if cursor == nil {
		return ResolvedCommitChange{}, ErrCursorClosed
	}
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	if cursor.closed || cursor.file == nil {
		return ResolvedCommitChange{}, ErrCursorClosed
	}
	cursor.file.mu.RLock()
	defer cursor.file.mu.RUnlock()
	if cursor.file.file == nil {
		return ResolvedCommitChange{}, errors.New("meldbase storage v2: file is closed")
	}
	return cursor.file.resolveCommitChangeUnlocked(change)
}

// ResolveChange materializes a change from the most recently delivered live
// batch. Calling Next again acknowledges that batch and may advance the replay
// retention lease, so callers must resolve all required images before Next.
func (stream *LiveCommitStream) ResolveChange(change CommitChange) (ResolvedCommitChange, error) {
	if stream == nil {
		return ResolvedCommitChange{}, ErrCursorClosed
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.file == nil {
		return ResolvedCommitChange{}, ErrCursorClosed
	}
	if stream.deliveredSequence == 0 {
		return ResolvedCommitChange{}, ErrNoDeliveredCommit
	}
	stream.file.mu.RLock()
	defer stream.file.mu.RUnlock()
	if stream.file.file == nil {
		return ResolvedCommitChange{}, errors.New("meldbase storage v2: file is closed")
	}
	if pin, exists := stream.file.readers[stream.pinID]; !exists || !pin.replay || pin.sequence != stream.deliveredSequence {
		return ResolvedCommitChange{}, ErrCorrupt
	}
	return stream.file.resolveCommitChangeUnlocked(change)
}

func (f *File) resolveCommitChangeUnlocked(change CommitChange) (ResolvedCommitChange, error) {
	resolved := ResolvedCommitChange{
		CollectionID: change.CollectionID, DocumentID: change.DocumentID, Operation: change.Operation,
		ChangedPaths: append([]string(nil), change.ChangedPaths...),
		Before:       append([]byte(nil), change.Before...), After: append([]byte(nil), change.After...),
	}
	var err error
	if change.BeforeRef != nil {
		resolved.Before, err = f.resolveDocumentVersionUnlocked(*change.BeforeRef)
		if err != nil {
			return ResolvedCommitChange{}, err
		}
	}
	if change.AfterRef != nil {
		resolved.After, err = f.resolveDocumentVersionUnlocked(*change.AfterRef)
		if err != nil {
			return ResolvedCommitChange{}, err
		}
	}
	return resolved, nil
}

func (f *File) resolveDocumentVersionUnlocked(reference DocumentVersionRef) ([]byte, error) {
	if reference.PrimaryRoot < 2 || allZero(reference.DocumentID[:]) {
		return nil, ErrCorrupt
	}
	value, exists, err := f.readDocumentUnlocked(reference.PrimaryRoot, reference.DocumentID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrCorrupt
	}
	return value, nil
}
