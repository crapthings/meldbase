package database

import (
	"errors"
	"fmt"
	"hash/maphash"

	storage "github.com/crapthings/meldbase/internal/storage"
)

// reactiveReplayView bridges one pinned Storage snapshot and its ordered
// Commit Log tail into the same immutable state transitions used by live
// process-local reactive views. It remains internal until Storage  becomes a
// supported public open path.
type reactiveReplayView struct {
	collection     string
	collectionID   uint32
	query          QuerySpec
	seed           maphash.Seed
	order          *reactiveCollectionOrder
	state          *reactiveViewState
	resourceLimits ResourceLimits
}

type replayOrderCheckpoint struct {
	token uint64
	next  uint64
	items map[DocumentID]replayOrderItem
}

type replayOrderItem struct {
	position uint64
	exists   bool
}

func newReactiveReplayView(snapshot *storage.ReadSnapshot, collection string, query QuerySpec, suppliedLimits ...ResourceLimits) (*reactiveReplayView, error) {
	if snapshot == nil || !collectionNamePattern.MatchString(collection) {
		return nil, ErrInvalidCollection
	}
	if len(suppliedLimits) > 1 {
		return nil, ErrCorrupt
	}
	limits, err := normalizeResourceLimits(ResourceLimits{})
	if err != nil {
		return nil, err
	}
	if len(suppliedLimits) == 1 {
		limits, err = normalizeResourceLimits(suppliedLimits[0])
		if err != nil {
			return nil, err
		}
	}
	token := snapshot.Sequence()
	meta, exists, err := snapshot.CollectionMeta(collection)
	if err != nil {
		return nil, replayCorrupt(err)
	}
	view := &reactiveReplayView{
		collection: collection, query: query, seed: maphash.MakeSeed(),
		order: &reactiveCollectionOrder{token: token, positions: make(map[DocumentID]uint64)}, resourceLimits: limits,
	}
	if !exists {
		view.state = &reactiveViewState{token: token, snapshot: QuerySnapshot{Token: token}}
		return view, nil
	}
	view.collectionID = meta.ID
	view.order.next = meta.NextDocumentPosition
	iterator, err := snapshot.OpenCollectionIterator(collection, nil, nil, 0)
	if err != nil {
		return nil, replayCorrupt(err)
	}
	defer iterator.Close()
	entries := make([]reactiveTreeEntry, 0)
	budget := newPredicateBudget(query, limits)
	var memberCount, memberBytes uint64
	var scanned uint64
	for iterator.Next() {
		record := iterator.Record()
		id := DocumentID(record.DocumentID)
		if record.InsertionPosition == 0 || record.InsertionPosition > meta.NextDocumentPosition {
			return nil, ErrCorrupt
		}
		if _, duplicate := view.order.positions[id]; duplicate {
			return nil, ErrCorrupt
		}
		document, err := decodeReplayDocument(record.Document, record.DocumentID)
		if err != nil {
			return nil, err
		}
		view.order.positions[id] = record.InsertionPosition
		scanned++
		matched, err := query.matchWithBudget(document, budget)
		if err != nil {
			return nil, err
		}
		if matched {
			size, err := canonicalDocumentSize(document)
			if err != nil {
				return nil, err
			}
			memberCount, memberBytes, err = admitReactiveViewMember(limits, memberCount, memberBytes, size)
			if err != nil {
				return nil, err
			}
			entries = append(entries, reactiveTreeEntry{id: id, member: reactiveMember{document: document, position: record.InsertionPosition, bytes: size}})
		}
	}
	if err := iterator.Err(); err != nil {
		return nil, replayCorrupt(err)
	}
	if scanned != meta.DocumentCount {
		return nil, ErrCorrupt
	}
	byID, ordered := buildReactiveTrees(view.seed, query, entries)
	view.state = &reactiveViewState{
		token: token, memberCount: memberCount, memberBytes: memberBytes, byID: byID, ordered: ordered,
		snapshot: QuerySnapshot{Token: token, Documents: materializeReactiveOrder(ordered, query.skip, query.limit)},
	}
	return view, nil
}

func (view *reactiveReplayView) Snapshot() QuerySnapshot {
	if view == nil || view.state == nil {
		return QuerySnapshot{}
	}
	return cloneSnapshot(view.state.snapshot)
}

// ApplyCommit resolves only this collection's images before acknowledging the
// stream batch. Unrelated commits still advance the internal sequence, while a
// delta is produced only when the materialized query result changes.
func (view *reactiveReplayView) ApplyCommit(stream *storage.LiveCommitStream, batch storage.CommitBatch) (QuerySnapshot, *sharedQueryDelta, error) {
	if view == nil || view.state == nil || stream == nil || batch.Sequence == 0 || batch.Sequence != view.state.token+1 {
		return QuerySnapshot{}, nil, ErrCorrupt
	}
	changes := make([]Change, 0)
	candidateCollectionID := view.collectionID
	seenDocuments := make(map[DocumentID]struct{})
	for _, change := range batch.Changes {
		if change.Operation == storage.CommitCatalog {
			if change.CollectionName != view.collection {
				continue
			}
			if candidateCollectionID != 0 && candidateCollectionID != change.CollectionID {
				return QuerySnapshot{}, nil, ErrCorrupt
			}
			candidateCollectionID = change.CollectionID
			continue
		}
		if candidateCollectionID == 0 || change.CollectionID != candidateCollectionID {
			continue
		}
		resolved, err := stream.ResolveChange(change)
		if err != nil {
			return QuerySnapshot{}, nil, replayCorrupt(err)
		}
		converted, err := convertReplayChange(resolved)
		if err != nil {
			return QuerySnapshot{}, nil, err
		}
		converted.Collection = view.collection
		if _, duplicate := seenDocuments[converted.DocumentID]; duplicate {
			return QuerySnapshot{}, nil, ErrCorrupt
		}
		seenDocuments[converted.DocumentID] = struct{}{}
		changes = append(changes, converted)
	}
	checkpoint := view.checkpointOrder(changes)
	if !view.order.apply(batch.Sequence, changes) {
		view.restoreOrder(checkpoint)
		return QuerySnapshot{}, nil, ErrCorrupt
	}
	previous := view.state
	next, valid, err := transitionReactiveViewState(previous, view.query, view.order, view.seed, batch.Sequence, changes, view.resourceLimits)
	if err != nil {
		view.restoreOrder(checkpoint)
		return QuerySnapshot{}, nil, err
	}
	if !valid {
		view.restoreOrder(checkpoint)
		return QuerySnapshot{}, nil, ErrCorrupt
	}
	if documentSlicesEqual(previous.snapshot.Documents, next.snapshot.Documents) {
		view.collectionID = candidateCollectionID
		view.state = next
		return cloneSnapshot(next.snapshot), nil, nil
	}
	delta, err := buildSharedQueryDelta(previous.snapshot.Documents, next.snapshot.Documents, next.snapshot.Token)
	if err != nil {
		view.restoreOrder(checkpoint)
		return QuerySnapshot{}, nil, err
	}
	view.collectionID = candidateCollectionID
	view.state = next
	return cloneSnapshot(next.snapshot), delta, nil
}

func (view *reactiveReplayView) checkpointOrder(changes []Change) replayOrderCheckpoint {
	checkpoint := replayOrderCheckpoint{items: make(map[DocumentID]replayOrderItem, len(changes))}
	view.order.mu.RLock()
	checkpoint.token, checkpoint.next = view.order.token, view.order.next
	for _, change := range changes {
		position, exists := view.order.positions[change.DocumentID]
		checkpoint.items[change.DocumentID] = replayOrderItem{position: position, exists: exists}
	}
	view.order.mu.RUnlock()
	return checkpoint
}

func (view *reactiveReplayView) restoreOrder(checkpoint replayOrderCheckpoint) {
	view.order.mu.Lock()
	view.order.token, view.order.next = checkpoint.token, checkpoint.next
	for id, item := range checkpoint.items {
		if item.exists {
			view.order.positions[id] = item.position
		} else {
			delete(view.order.positions, id)
		}
	}
	view.order.mu.Unlock()
}

func convertReplayChange(change storage.ResolvedCommitChange) (Change, error) {
	converted := Change{DocumentID: DocumentID(change.DocumentID), ChangedPaths: append([]string(nil), change.ChangedPaths...)}
	decodeBefore := func() error {
		document, err := decodeReplayDocument(change.Before, change.DocumentID)
		if err != nil {
			return err
		}
		converted.Before = &document
		return nil
	}
	decodeAfter := func() error {
		document, err := decodeReplayDocument(change.After, change.DocumentID)
		if err != nil {
			return err
		}
		size, err := canonicalDocumentSize(document)
		if err != nil {
			return err
		}
		converted.After = &document
		converted.afterCanonicalBytes = size
		converted.afterCanonicalBytesKnown = true
		return nil
	}
	switch change.Operation {
	case storage.CommitInsert:
		if len(change.Before) != 0 || len(change.After) == 0 {
			return Change{}, ErrCorrupt
		}
		converted.Operation = InsertOperation
		if err := decodeAfter(); err != nil {
			return Change{}, err
		}
	case storage.CommitUpdate:
		if len(change.Before) == 0 || len(change.After) == 0 {
			return Change{}, ErrCorrupt
		}
		converted.Operation = UpdateOperation
		if err := decodeBefore(); err != nil {
			return Change{}, err
		}
		if err := decodeAfter(); err != nil {
			return Change{}, err
		}
	case storage.CommitDelete:
		if len(change.Before) == 0 || len(change.After) != 0 {
			return Change{}, ErrCorrupt
		}
		converted.Operation = DeleteOperation
		if err := decodeBefore(); err != nil {
			return Change{}, err
		}
	default:
		return Change{}, ErrCorrupt
	}
	return converted, nil
}

func decodeReplayDocument(encoded []byte, expected [16]byte) (Document, error) {
	document, err := decodeStoredDocument(encoded)
	if err != nil {
		return nil, replayCorrupt(err)
	}
	id, exists := document.ID()
	if !exists || id != DocumentID(expected) {
		return nil, ErrCorrupt
	}
	return document, nil
}

func replayCorrupt(err error) error {
	if err == nil || errors.Is(err, ErrCorrupt) {
		return err
	}
	return fmt.Errorf("%w: storage store replay: %v", ErrCorrupt, err)
}
