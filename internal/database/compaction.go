package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	storage "github.com/crapthings/meldbase/internal/storage"
)

// Compact writes one current logical snapshot into a new, atomically
// published file. It never overwrites destination or mutates the source.
// Writes which commit after the source snapshot is pinned may continue and are
// intentionally absent from the destination. The compacted database receives a
// new identity and commit-log history, so callers must treat every old resume
// token as invalid.
func (db *DB) Compact(ctx context.Context, destination string) (resultErr error) {
	options := CompactionOptions{}
	if db != nil {
		options.StorageLimits = db.storageLimits
		options.ResourceLimits = db.resourceLimits
	}
	return db.CompactWithOptions(ctx, destination, options)
}

// CompactWithOptions is Compact with an explicit destination quota.
func (db *DB) CompactWithOptions(ctx context.Context, destination string, options CompactionOptions) (resultErr error) {
	if db == nil {
		return ErrCompactionUnsupported
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if destination == "" {
		return errors.New("meldbase: empty compaction destination")
	}
	resourceLimits, err := normalizeResourceLimits(options.ResourceLimits)
	if err != nil {
		return err
	}
	absoluteDestination, err := filepath.Abs(filepath.Clean(destination))
	if err != nil {
		return err
	}
	store, ok := db.durability.(*durableStore)
	if !ok || store == nil || store.file == nil {
		return ErrCompactionUnsupported
	}
	store.compactMu.Lock()
	defer store.compactMu.Unlock()

	db.metrics.compactionAttempts.Add(1)
	db.metrics.compactionActive.Add(1)
	started := time.Now()
	succeeded := false
	defer func() {
		db.metrics.compactionActive.Add(^uint64(0))
		db.metrics.compactionLastNanos.Store(uint64(time.Since(started)))
		if !succeeded {
			db.metrics.compactionFailed.Add(1)
		}
	}()

	// Only the snapshot admission is under db.mu. The storage snapshot pins its
	// own immutable root, so the potentially long copy and verification stages
	// must not stall ordinary writers.
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	if db.fatalErr != nil {
		db.mu.RUnlock()
		return db.fatalErr
	}
	builds, err := store.file.IndexBuilds()
	if err != nil {
		db.mu.RUnlock()
		return mapStorageError(err)
	}
	if len(builds) != 0 {
		db.mu.RUnlock()
		return fmt.Errorf("%w: %d durable index build(s) must finish or abort before compaction", ErrWriteConflict, len(builds))
	}
	if absoluteDestination == store.path {
		db.mu.RUnlock()
		return ErrCompactionDestinationExists
	}
	if _, err := os.Lstat(absoluteDestination); err == nil {
		db.mu.RUnlock()
		return ErrCompactionDestinationExists
	} else if !errors.Is(err, fs.ErrNotExist) {
		db.mu.RUnlock()
		return err
	}
	if info, err := os.Stat(store.path); err == nil {
		db.metrics.compactionInputBytes.Store(uint64(info.Size()))
	} else {
		db.mu.RUnlock()
		return err
	}
	source, err := store.file.OpenSnapshot()
	if err != nil {
		db.mu.RUnlock()
		return mapStorageError(err)
	}
	if source.Sequence() != db.token {
		db.mu.RUnlock()
		_ = source.Close()
		return ErrCorrupt
	}
	db.mu.RUnlock()
	defer source.Close()
	if store.testCompactionSnapshotHook != nil {
		store.testCompactionSnapshotHook()
	}

	directory := filepath.Dir(absoluteDestination)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(absoluteDestination)+".compact-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	defer os.Remove(temporaryPath)
	destinationFile, _, _, err := storage.OpenWithOptions(temporaryPath, storage.OpenOptions{MaxFileBytes: options.StorageLimits.MaxFileBytes})
	if err != nil {
		return mapStorageError(err)
	}
	closeDestination := func(current error) error {
		return errors.Join(current, mapStorageError(destinationFile.Close()))
	}
	if destinationFile.Meta().DatabaseID == db.databaseID {
		return closeDestination(ErrCorrupt)
	}
	if err := compactSnapshot(ctx, source, destinationFile, db, resourceLimits); err != nil {
		return closeDestination(err)
	}
	if _, err := destinationFile.Reachability(); err != nil {
		return closeDestination(mapStorageError(err))
	}
	destinationSnapshot, err := destinationFile.OpenSnapshot()
	if err != nil {
		return closeDestination(mapStorageError(err))
	}
	verifyErr := verifyCompactedSnapshots(ctx, source, destinationSnapshot)
	verifyErr = errors.Join(verifyErr, mapStorageError(destinationSnapshot.Close()))
	if verifyErr != nil {
		return closeDestination(verifyErr)
	}
	if err := closeDestination(nil); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	outputInfo, err := os.Stat(temporaryPath)
	if err != nil {
		return err
	}
	if err := publishNewFile(temporaryPath, absoluteDestination, publishFileOps{
		link: os.Link, remove: os.Remove, syncDirectory: syncDirectory,
	}); err != nil {
		if errors.Is(err, ErrDestinationExists) {
			return ErrCompactionDestinationExists
		}
		return err
	}
	_ = os.Remove(temporaryPath)
	_ = syncDirectory(directory)
	db.metrics.compactionOutputBytes.Store(uint64(outputInfo.Size()))
	db.metrics.compactionCompleted.Add(1)
	succeeded = true
	return nil
}

func compactSnapshot(ctx context.Context, source *storage.ReadSnapshot, destination *storage.File, db *DB, resourceLimits ResourceLimits) error {
	collections, err := source.Collections()
	if err != nil {
		return mapStorageError(err)
	}
	sort.Slice(collections, func(left, right int) bool { return collections[left].Name < collections[right].Name })
	for _, collection := range collections {
		if err := contextError(ctx); err != nil {
			return err
		}
		transactionID, err := newSnapshotTransactionID()
		if err != nil {
			return err
		}
		if _, err := destination.ApplyCreateCollection(storage.CreateCollectionTransaction{
			TransactionID: transactionID, Collection: collection.Name,
		}); err != nil {
			return mapStorageError(err)
		}
		iterator, err := source.OpenInsertionOrderIterator(collection.Name, nil, nil, 0)
		if err != nil {
			return mapStorageError(err)
		}
		pending := make([]storage.DocumentMutation, 0, snapshotDocumentBatchCount)
		pendingBytes := 0
		flush := func() error {
			if len(pending) == 0 {
				return nil
			}
			transactionID, err := newSnapshotTransactionID()
			if err != nil {
				return err
			}
			_, err = destination.ApplyDocumentTransaction(storage.DocumentTransaction{TransactionID: transactionID, Mutations: pending})
			pending = pending[:0]
			pendingBytes = 0
			return mapStorageError(err)
		}
		for iterator.Next() {
			if err := contextError(ctx); err != nil {
				_ = iterator.Close()
				return err
			}
			record := iterator.Record()
			if len(pending) > 0 && (len(pending) == snapshotDocumentBatchCount || pendingBytes+len(record.Document) > snapshotDocumentBatchBytes) {
				if err := flush(); err != nil {
					_ = iterator.Close()
					return err
				}
			}
			pending = append(pending, storage.DocumentMutation{
				Collection: collection.Name, DocumentID: record.DocumentID,
				Operation: storage.DocumentInsert, Document: append([]byte(nil), record.Document...),
			})
			pendingBytes += len(record.Document)
		}
		iteratorErr := mapStorageError(iterator.Err())
		closeErr := mapStorageError(iterator.Close())
		if err := errors.Join(iteratorErr, closeErr); err != nil {
			return err
		}
		if err := flush(); err != nil {
			return err
		}
		indexes, err := source.Indexes(collection.Name)
		if err != nil {
			return mapStorageError(err)
		}
		sort.Slice(indexes, func(left, right int) bool { return indexes[left].Name < indexes[right].Name })
		for _, index := range indexes {
			iterator, err := source.OpenIndexIterator(collection.Name, index.Name, nil, nil, 0)
			if err != nil {
				return mapStorageError(err)
			}
			budget := db.newIndexBuildBudget(resourceLimits)
			entries := make([]storage.IndexEntry, 0)
			for iterator.Next() {
				if err := contextError(ctx); err != nil {
					_ = iterator.Close()
					return err
				}
				entry := iterator.Entry()
				if err := budget.add(entry.Key); err != nil {
					_ = iterator.Close()
					return err
				}
				entries = append(entries, storage.IndexEntry{Key: append([]byte(nil), entry.Key...), DocumentID: entry.DocumentID})
			}
			iteratorErr := mapStorageError(iterator.Err())
			closeErr := mapStorageError(iterator.Close())
			if err := errors.Join(iteratorErr, closeErr); err != nil {
				return err
			}
			transactionID, err := newSnapshotTransactionID()
			if err != nil {
				return err
			}
			if _, err := destination.ApplyCreateIndex(storage.CreateIndexTransaction{
				TransactionID: transactionID, Collection: collection.Name, Name: index.Name,
				FieldPath: index.FieldPath, Fields: append([]storage.IndexField(nil), index.Fields...),
				Unique: index.Unique, Entries: entries,
			}); err != nil {
				return mapStorageError(err)
			}
		}
	}
	systemRecords, err := source.SystemRecords()
	if err != nil {
		return mapStorageError(err)
	}
	for _, record := range systemRecords {
		if err := contextError(ctx); err != nil {
			return err
		}
		transactionID, err := newSnapshotTransactionID()
		if err != nil {
			return err
		}
		result, err := destination.ApplySystemRecordTransaction(storage.SystemRecordTransaction{
			TransactionID: transactionID, Key: record.Key, NewValue: record.Value,
		})
		if err != nil {
			return mapStorageError(err)
		}
		if !result.Applied {
			return ErrCorrupt
		}
	}
	return nil
}

func verifyCompactedSnapshots(ctx context.Context, source, destination *storage.ReadSnapshot) error {
	expectedCollections, err := source.Collections()
	if err != nil {
		return mapStorageError(err)
	}
	actualCollections, err := destination.Collections()
	if err != nil {
		return mapStorageError(err)
	}
	sort.Slice(expectedCollections, func(left, right int) bool { return expectedCollections[left].Name < expectedCollections[right].Name })
	sort.Slice(actualCollections, func(left, right int) bool { return actualCollections[left].Name < actualCollections[right].Name })
	if len(expectedCollections) != len(actualCollections) {
		return fmt.Errorf("%w: compacted collection count", ErrCorrupt)
	}
	for index := range expectedCollections {
		expected, actual := expectedCollections[index], actualCollections[index]
		if expected.Name != actual.Name || expected.Meta.DocumentCount != actual.Meta.DocumentCount {
			return fmt.Errorf("%w: compacted collection metadata", ErrCorrupt)
		}
		if err := compareCompactedDocuments(ctx, source, destination, expected.Name); err != nil {
			return err
		}
		expectedIndexes, err := source.Indexes(expected.Name)
		if err != nil {
			return mapStorageError(err)
		}
		actualIndexes, err := destination.Indexes(actual.Name)
		if err != nil {
			return mapStorageError(err)
		}
		sort.Slice(expectedIndexes, func(left, right int) bool { return expectedIndexes[left].Name < expectedIndexes[right].Name })
		sort.Slice(actualIndexes, func(left, right int) bool { return actualIndexes[left].Name < actualIndexes[right].Name })
		if len(expectedIndexes) != len(actualIndexes) {
			return fmt.Errorf("%w: compacted index count", ErrCorrupt)
		}
		for indexPosition := range expectedIndexes {
			expectedIndex, actualIndex := expectedIndexes[indexPosition], actualIndexes[indexPosition]
			if expectedIndex.Name != actualIndex.Name || expectedIndex.FieldPath != actualIndex.FieldPath || expectedIndex.Unique != actualIndex.Unique {
				return fmt.Errorf("%w: compacted index definition", ErrCorrupt)
			}
			if err := compareCompactedIndex(ctx, source, destination, expected.Name, expectedIndex.Name); err != nil {
				return err
			}
		}
	}
	expectedSystem, err := source.SystemRecords()
	if err != nil {
		return mapStorageError(err)
	}
	actualSystem, err := destination.SystemRecords()
	if err != nil {
		return mapStorageError(err)
	}
	if len(expectedSystem) != len(actualSystem) {
		return fmt.Errorf("%w: compacted system record count", ErrCorrupt)
	}
	for index := range expectedSystem {
		if !bytes.Equal(expectedSystem[index].Key, actualSystem[index].Key) || !bytes.Equal(expectedSystem[index].Value, actualSystem[index].Value) {
			return fmt.Errorf("%w: compacted system record", ErrCorrupt)
		}
	}
	return nil
}

func compareCompactedIndex(ctx context.Context, source, destination *storage.ReadSnapshot, collection, name string) error {
	expected, err := source.OpenIndexIterator(collection, name, nil, nil, 0)
	if err != nil {
		return mapStorageError(err)
	}
	defer expected.Close()
	actual, err := destination.OpenIndexIterator(collection, name, nil, nil, 0)
	if err != nil {
		return mapStorageError(err)
	}
	defer actual.Close()
	for {
		if err := contextError(ctx); err != nil {
			return err
		}
		expectedNext, actualNext := expected.Next(), actual.Next()
		if expectedNext != actualNext {
			return fmt.Errorf("%w: compacted index entry count", ErrCorrupt)
		}
		if !expectedNext {
			return errors.Join(mapStorageError(expected.Err()), mapStorageError(actual.Err()))
		}
		expectedEntry, actualEntry := expected.Entry(), actual.Entry()
		if expectedEntry.DocumentID != actualEntry.DocumentID || !bytes.Equal(expectedEntry.Key, actualEntry.Key) {
			return fmt.Errorf("%w: compacted index entry", ErrCorrupt)
		}
	}
}

func compareCompactedDocuments(ctx context.Context, source, destination *storage.ReadSnapshot, collection string) error {
	expected, err := source.OpenInsertionOrderIterator(collection, nil, nil, 0)
	if err != nil {
		return mapStorageError(err)
	}
	defer expected.Close()
	actual, err := destination.OpenInsertionOrderIterator(collection, nil, nil, 0)
	if err != nil {
		return mapStorageError(err)
	}
	defer actual.Close()
	for {
		if err := contextError(ctx); err != nil {
			return err
		}
		expectedNext, actualNext := expected.Next(), actual.Next()
		if expectedNext != actualNext {
			return fmt.Errorf("%w: compacted document count", ErrCorrupt)
		}
		if !expectedNext {
			return errors.Join(mapStorageError(expected.Err()), mapStorageError(actual.Err()))
		}
		expectedRecord, actualRecord := expected.Record(), actual.Record()
		if expectedRecord.DocumentID != actualRecord.DocumentID || !bytes.Equal(expectedRecord.Document, actualRecord.Document) {
			return fmt.Errorf("%w: compacted document", ErrCorrupt)
		}
	}
}
