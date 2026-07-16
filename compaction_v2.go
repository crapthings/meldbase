package meldbase

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

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

// CompactToV2 writes the current logical V2 state into a new, atomically
// published V2 file. It never overwrites destination or mutates the source.
// The compacted database deliberately receives a new identity and commit-log
// history, so callers must treat every old resume token as invalid.
func (db *DB) CompactToV2(ctx context.Context, destination string) (resultErr error) {
	options := V2DestinationOptions{}
	if db != nil {
		options.StorageLimits = db.v2StorageLimits
		options.ResourceLimits = db.resourceLimits
	}
	return db.CompactToV2WithOptions(ctx, destination, options)
}

// CompactToV2WithOptions is CompactToV2 with an explicit destination quota.
func (db *DB) CompactToV2WithOptions(ctx context.Context, destination string, options V2DestinationOptions) (resultErr error) {
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
	store, ok := db.durability.(*v2DurableStore)
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

	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	if db.fatalErr != nil {
		return db.fatalErr
	}
	builds, err := store.file.IndexBuilds()
	if err != nil {
		return mapStorageV2Error(err)
	}
	if len(builds) != 0 {
		return fmt.Errorf("%w: %d durable index build(s) must finish or abort before compaction", ErrWriteConflict, len(builds))
	}
	if absoluteDestination == store.path {
		return ErrCompactionDestinationExists
	}
	if _, err := os.Lstat(absoluteDestination); err == nil {
		return ErrCompactionDestinationExists
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if info, err := os.Stat(store.path); err == nil {
		db.metrics.compactionInputBytes.Store(uint64(info.Size()))
	} else {
		return err
	}
	source, err := store.file.OpenSnapshot()
	if err != nil {
		return mapStorageV2Error(err)
	}
	defer source.Close()
	if source.Sequence() != db.token {
		return ErrCorrupt
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
	destinationFile, _, _, err := storagev2.OpenWithOptions(temporaryPath, storagev2.OpenOptions{MaxFileBytes: options.StorageLimits.MaxFileBytes})
	if err != nil {
		return mapStorageV2Error(err)
	}
	closeDestination := func(current error) error {
		return errors.Join(current, mapStorageV2Error(destinationFile.Close()))
	}
	if destinationFile.Meta().DatabaseID == db.databaseID {
		return closeDestination(ErrCorrupt)
	}
	if err := compactV2Snapshot(ctx, source, destinationFile, db, resourceLimits); err != nil {
		return closeDestination(err)
	}
	if _, err := destinationFile.Reachability(); err != nil {
		return closeDestination(mapStorageV2Error(err))
	}
	destinationSnapshot, err := destinationFile.OpenSnapshot()
	if err != nil {
		return closeDestination(mapStorageV2Error(err))
	}
	verifyErr := verifyCompactedV2Snapshots(ctx, source, destinationSnapshot)
	verifyErr = errors.Join(verifyErr, mapStorageV2Error(destinationSnapshot.Close()))
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
	if err := publishMigrationFile(temporaryPath, absoluteDestination, migrationPublishOps{
		link: os.Link, remove: os.Remove, syncDirectory: syncDirectory,
	}); err != nil {
		if errors.Is(err, ErrMigrationDestinationExists) {
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

func compactV2Snapshot(ctx context.Context, source *storagev2.ReadSnapshot, destination *storagev2.File, db *DB, resourceLimits ResourceLimits) error {
	collections, err := source.Collections()
	if err != nil {
		return mapStorageV2Error(err)
	}
	sort.Slice(collections, func(left, right int) bool { return collections[left].Name < collections[right].Name })
	for _, collection := range collections {
		if err := contextError(ctx); err != nil {
			return err
		}
		transactionID, err := newMigrationTransactionID()
		if err != nil {
			return err
		}
		if _, err := destination.ApplyCreateCollection(storagev2.CreateCollectionTransaction{
			TransactionID: transactionID, Collection: collection.Name,
		}); err != nil {
			return mapStorageV2Error(err)
		}
		iterator, err := source.OpenInsertionOrderIterator(collection.Name, nil, nil, 0)
		if err != nil {
			return mapStorageV2Error(err)
		}
		pending := make([]storagev2.DocumentMutation, 0, migrationDocumentBatchCount)
		pendingBytes := 0
		flush := func() error {
			if len(pending) == 0 {
				return nil
			}
			transactionID, err := newMigrationTransactionID()
			if err != nil {
				return err
			}
			_, err = destination.ApplyDocumentTransaction(storagev2.DocumentTransaction{TransactionID: transactionID, Mutations: pending})
			pending = pending[:0]
			pendingBytes = 0
			return mapStorageV2Error(err)
		}
		for iterator.Next() {
			if err := contextError(ctx); err != nil {
				_ = iterator.Close()
				return err
			}
			record := iterator.Record()
			if len(pending) > 0 && (len(pending) == migrationDocumentBatchCount || pendingBytes+len(record.Document) > migrationDocumentBatchBytes) {
				if err := flush(); err != nil {
					_ = iterator.Close()
					return err
				}
			}
			pending = append(pending, storagev2.DocumentMutation{
				Collection: collection.Name, DocumentID: record.DocumentID,
				Operation: storagev2.DocumentInsert, Document: append([]byte(nil), record.Document...),
			})
			pendingBytes += len(record.Document)
		}
		iteratorErr := mapStorageV2Error(iterator.Err())
		closeErr := mapStorageV2Error(iterator.Close())
		if err := errors.Join(iteratorErr, closeErr); err != nil {
			return err
		}
		if err := flush(); err != nil {
			return err
		}
		indexes, err := source.Indexes(collection.Name)
		if err != nil {
			return mapStorageV2Error(err)
		}
		sort.Slice(indexes, func(left, right int) bool { return indexes[left].Name < indexes[right].Name })
		for _, index := range indexes {
			iterator, err := source.OpenIndexIterator(collection.Name, index.Name, nil, nil, 0)
			if err != nil {
				return mapStorageV2Error(err)
			}
			budget := db.newIndexBuildBudget(resourceLimits)
			entries := make([]storagev2.IndexEntry, 0)
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
				entries = append(entries, storagev2.IndexEntry{Key: append([]byte(nil), entry.Key...), DocumentID: entry.DocumentID})
			}
			iteratorErr := mapStorageV2Error(iterator.Err())
			closeErr := mapStorageV2Error(iterator.Close())
			if err := errors.Join(iteratorErr, closeErr); err != nil {
				return err
			}
			transactionID, err := newMigrationTransactionID()
			if err != nil {
				return err
			}
			if _, err := destination.ApplyCreateIndex(storagev2.CreateIndexTransaction{
				TransactionID: transactionID, Collection: collection.Name, Name: index.Name,
				FieldPath: index.FieldPath, Fields: append([]storagev2.IndexField(nil), index.Fields...),
				Unique: index.Unique, Entries: entries,
			}); err != nil {
				return mapStorageV2Error(err)
			}
		}
	}
	systemRecords, err := source.SystemRecords()
	if err != nil {
		return mapStorageV2Error(err)
	}
	for _, record := range systemRecords {
		if err := contextError(ctx); err != nil {
			return err
		}
		transactionID, err := newMigrationTransactionID()
		if err != nil {
			return err
		}
		result, err := destination.ApplySystemRecordTransaction(storagev2.SystemRecordTransaction{
			TransactionID: transactionID, Key: record.Key, NewValue: record.Value,
		})
		if err != nil {
			return mapStorageV2Error(err)
		}
		if !result.Applied {
			return ErrCorrupt
		}
	}
	return nil
}

func verifyCompactedV2Snapshots(ctx context.Context, source, destination *storagev2.ReadSnapshot) error {
	expectedCollections, err := source.Collections()
	if err != nil {
		return mapStorageV2Error(err)
	}
	actualCollections, err := destination.Collections()
	if err != nil {
		return mapStorageV2Error(err)
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
			return mapStorageV2Error(err)
		}
		actualIndexes, err := destination.Indexes(actual.Name)
		if err != nil {
			return mapStorageV2Error(err)
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
		return mapStorageV2Error(err)
	}
	actualSystem, err := destination.SystemRecords()
	if err != nil {
		return mapStorageV2Error(err)
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

func compareCompactedIndex(ctx context.Context, source, destination *storagev2.ReadSnapshot, collection, name string) error {
	expected, err := source.OpenIndexIterator(collection, name, nil, nil, 0)
	if err != nil {
		return mapStorageV2Error(err)
	}
	defer expected.Close()
	actual, err := destination.OpenIndexIterator(collection, name, nil, nil, 0)
	if err != nil {
		return mapStorageV2Error(err)
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
			return errors.Join(mapStorageV2Error(expected.Err()), mapStorageV2Error(actual.Err()))
		}
		expectedEntry, actualEntry := expected.Entry(), actual.Entry()
		if expectedEntry.DocumentID != actualEntry.DocumentID || !bytes.Equal(expectedEntry.Key, actualEntry.Key) {
			return fmt.Errorf("%w: compacted index entry", ErrCorrupt)
		}
	}
}

func compareCompactedDocuments(ctx context.Context, source, destination *storagev2.ReadSnapshot, collection string) error {
	expected, err := source.OpenInsertionOrderIterator(collection, nil, nil, 0)
	if err != nil {
		return mapStorageV2Error(err)
	}
	defer expected.Close()
	actual, err := destination.OpenInsertionOrderIterator(collection, nil, nil, 0)
	if err != nil {
		return mapStorageV2Error(err)
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
			return errors.Join(mapStorageV2Error(expected.Err()), mapStorageV2Error(actual.Err()))
		}
		expectedRecord, actualRecord := expected.Record(), actual.Record()
		if expectedRecord.DocumentID != actualRecord.DocumentID || !bytes.Equal(expectedRecord.Document, actualRecord.Document) {
			return fmt.Errorf("%w: compacted document", ErrCorrupt)
		}
	}
}
