package meldbase

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	storagefile "github.com/crapthings/meldbase/internal/storage"
	"github.com/crapthings/meldbase/internal/wal"
)

type durableStore struct {
	pages *storagefile.File
	log   *wal.Log
	path  string
}

func Open(path string) (*DB, error) {
	if path == "" {
		return nil, errors.New("meldbase: empty database path")
	}
	pages, blobs, meta, err := storagefile.OpenBlobs(path)
	if err != nil {
		return nil, mapStorageError(err)
	}
	collections, history, err := decodeCheckpointBlobs(blobs)
	if err != nil {
		pages.Close()
		return nil, fmt.Errorf("%w: snapshot: %v", ErrCorrupt, err)
	}
	if history != nil && ((meta.CheckpointToken > 0 && len(history) == 0) || (len(history) > 0 && history[len(history)-1].Token != meta.CheckpointToken)) {
		pages.Close()
		return nil, fmt.Errorf("%w: checkpoint history position", ErrCorrupt)
	}
	log, records, err := wal.Open(path+".wal", meta.CheckpointToken)
	if err != nil {
		pages.Close()
		return nil, mapStorageError(err)
	}
	db := &DB{collections: collections, watchers: make(map[uint64]*changeWatcher), token: meta.CheckpointToken, store: &durableStore{pages: pages, log: log, path: path}, databaseID: meta.DatabaseID, history: history, historyLimit: 1024}
	for _, record := range records {
		if record.Token != db.token+1 {
			db.store.close()
			return nil, fmt.Errorf("%w: WAL token gap", ErrCorrupt)
		}
		changes, err := decodeTransaction(record.Payload)
		if err != nil {
			db.store.close()
			return nil, fmt.Errorf("%w: WAL transaction", ErrCorrupt)
		}
		if err := db.applyRecovered(changes); err != nil {
			db.store.close()
			return nil, err
		}
		db.token = record.Token
		db.recordCommittedBatch(ChangeBatch{Token: record.Token, Changes: changes})
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		db.store.close()
		return nil, err
	}
	return db, nil
}

func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.fatalErr != nil {
		return db.fatalErr
	}
	if db.store == nil {
		return nil
	}
	blobs, err := encodeCheckpointBlobs(db.collections, db.history)
	if err != nil {
		return err
	}
	if err := db.store.pages.CheckpointBlobs(db.token, blobs); err != nil {
		db.fatalErr = fmt.Errorf("%w: %v", ErrDurability, err)
		return db.fatalErr
	}
	if err := db.store.log.Reset(db.token); err != nil {
		db.fatalErr = fmt.Errorf("%w: %v", ErrDurability, err)
		return db.fatalErr
	}
	return nil
}

func (db *DB) appendCommit(token uint64, changes []Change) error {
	if db.store == nil {
		return nil
	}
	payload, err := encodeTransaction(changes)
	if err != nil {
		return err
	}
	if err := db.store.log.Append(token, payload); err != nil {
		db.fatalErr = fmt.Errorf("%w: %v", ErrDurability, err)
		return db.fatalErr
	}
	return nil
}

func (db *DB) applyRecovered(changes []Change) error {
	if len(changes) == 0 {
		return ErrCorrupt
	}
	collection, operation := changes[0].Collection, changes[0].Operation
	for _, change := range changes {
		if change.Collection != collection || change.Operation != operation {
			return fmt.Errorf("%w: mixed transaction batch", ErrCorrupt)
		}
	}
	data := db.collections[collection]
	if data == nil {
		data = newCollectionData()
		db.collections[collection] = data
	}
	switch operation {
	case InsertOperation:
		for _, change := range changes {
			if _, exists := data.documents[change.DocumentID]; exists || change.After == nil {
				return fmt.Errorf("%w: invalid recovered insert", ErrCorrupt)
			}
			if err := data.validateIndexInsert(change.DocumentID, *change.After); err != nil {
				return fmt.Errorf("%w: recovered index insert", ErrCorrupt)
			}
			data.documents[change.DocumentID] = change.After.Clone()
			data.order = append(data.order, change.DocumentID)
			data.insertIndexes(change.DocumentID, *change.After)
		}
	case UpdateOperation:
		pending := make([]pendingUpdate, len(changes))
		for i, change := range changes {
			before, exists := data.documents[change.DocumentID]
			if !exists || change.After == nil {
				return fmt.Errorf("%w: invalid recovered update", ErrCorrupt)
			}
			pending[i] = pendingUpdate{id: change.DocumentID, before: before, after: *change.After}
		}
		if err := data.validateIndexUpdates(pending); err != nil {
			return fmt.Errorf("%w: recovered index update", ErrCorrupt)
		}
		for _, change := range pending {
			data.deleteIndexes(change.id, change.before)
		}
		for _, change := range pending {
			data.documents[change.id] = change.after.Clone()
			data.insertIndexes(change.id, change.after)
		}
	case DeleteOperation:
		for _, change := range changes {
			document, exists := data.documents[change.DocumentID]
			if !exists {
				return fmt.Errorf("%w: invalid recovered delete", ErrCorrupt)
			}
			data.deleteIndexes(change.DocumentID, document)
			delete(data.documents, change.DocumentID)
		}
		kept := data.order[:0]
		deleted := make(map[DocumentID]struct{}, len(changes))
		for _, change := range changes {
			deleted[change.DocumentID] = struct{}{}
		}
		for _, id := range data.order {
			if _, remove := deleted[id]; !remove {
				kept = append(kept, id)
			}
		}
		data.order = kept
	case CreateIndexOperation:
		if len(changes) != 1 || changes[0].Index == nil || data.indexes[changes[0].Index.Name] != nil {
			return fmt.Errorf("%w: invalid recovered index", ErrCorrupt)
		}
		state, err := buildIndex(*changes[0].Index, data)
		if err != nil {
			return fmt.Errorf("%w: recovered index", ErrCorrupt)
		}
		data.indexes[changes[0].Index.Name] = state
	default:
		return ErrCorrupt
	}
	return nil
}

func (s *durableStore) close() error {
	if s == nil {
		return nil
	}
	return errors.Join(s.log.Close(), s.pages.Close())
}
func mapStorageError(err error) error {
	if errors.Is(err, storagefile.ErrCorrupt) || errors.Is(err, wal.ErrCorrupt) {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	return err
}
func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
