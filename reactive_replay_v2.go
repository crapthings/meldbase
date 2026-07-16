package meldbase

import (
	"errors"
	"fmt"
	"hash/maphash"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

// v2ReactiveReplayView bridges one pinned Storage V2 snapshot and its ordered
// Commit Log tail into the same immutable state transitions used by live
// process-local reactive views. It remains internal until Storage V2 becomes a
// supported public open path.
type v2ReactiveReplayView struct {
	collection   string
	collectionID uint32
	query        QuerySpec
	seed         maphash.Seed
	order        *reactiveCollectionOrder
	state        *reactiveViewState
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

func newV2ReactiveReplayView(snapshot *storagev2.ReadSnapshot, collection string, query QuerySpec) (*v2ReactiveReplayView, error) {
	if snapshot == nil || !collectionNamePattern.MatchString(collection) {
		return nil, ErrInvalidCollection
	}
	token := snapshot.Sequence()
	meta, exists, err := snapshot.CollectionMeta(collection)
	if err != nil {
		return nil, replayCorrupt(err)
	}
	view := &v2ReactiveReplayView{
		collection: collection, query: query, seed: maphash.MakeSeed(),
		order: &reactiveCollectionOrder{token: token, positions: make(map[DocumentID]uint64)},
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
		document, err := decodeV2ReplayDocument(record.Document, record.DocumentID)
		if err != nil {
			return nil, err
		}
		view.order.positions[id] = record.InsertionPosition
		scanned++
		if query.Match(document) {
			entries = append(entries, reactiveTreeEntry{id: id, member: reactiveMember{document: document, position: record.InsertionPosition}})
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
		token: token, byID: byID, ordered: ordered,
		snapshot: QuerySnapshot{Token: token, Documents: materializeReactiveOrder(ordered, query.skip, query.limit)},
	}
	return view, nil
}

func (view *v2ReactiveReplayView) Snapshot() QuerySnapshot {
	if view == nil || view.state == nil {
		return QuerySnapshot{}
	}
	return cloneSnapshot(view.state.snapshot)
}

// ApplyCommit resolves only this collection's images before acknowledging the
// stream batch. Unrelated commits still advance the internal sequence, while a
// delta is produced only when the materialized query result changes.
func (view *v2ReactiveReplayView) ApplyCommit(stream *storagev2.LiveCommitStream, batch storagev2.CommitBatch) (QuerySnapshot, *sharedQueryDelta, error) {
	if view == nil || view.state == nil || stream == nil || batch.Sequence == 0 || batch.Sequence != view.state.token+1 {
		return QuerySnapshot{}, nil, ErrCorrupt
	}
	changes := make([]Change, 0)
	candidateCollectionID := view.collectionID
	seenDocuments := make(map[DocumentID]struct{})
	for _, change := range batch.Changes {
		if change.Operation == storagev2.CommitCatalog {
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
		converted, err := convertV2ReplayChange(resolved)
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
	next, valid := transitionReactiveViewState(previous, view.query, view.order, view.seed, batch.Sequence, changes)
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

func (view *v2ReactiveReplayView) checkpointOrder(changes []Change) replayOrderCheckpoint {
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

func (view *v2ReactiveReplayView) restoreOrder(checkpoint replayOrderCheckpoint) {
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

func convertV2ReplayChange(change storagev2.ResolvedCommitChange) (Change, error) {
	converted := Change{DocumentID: DocumentID(change.DocumentID)}
	decodeBefore := func() error {
		document, err := decodeV2ReplayDocument(change.Before, change.DocumentID)
		if err != nil {
			return err
		}
		converted.Before = &document
		return nil
	}
	decodeAfter := func() error {
		document, err := decodeV2ReplayDocument(change.After, change.DocumentID)
		if err != nil {
			return err
		}
		converted.After = &document
		return nil
	}
	switch change.Operation {
	case storagev2.CommitInsert:
		if len(change.Before) != 0 || len(change.After) == 0 {
			return Change{}, ErrCorrupt
		}
		converted.Operation = InsertOperation
		if err := decodeAfter(); err != nil {
			return Change{}, err
		}
	case storagev2.CommitUpdate:
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
	case storagev2.CommitDelete:
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

func decodeV2ReplayDocument(encoded []byte, expected [16]byte) (Document, error) {
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
	return fmt.Errorf("%w: storage v2 replay: %v", ErrCorrupt, err)
}
