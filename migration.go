package meldbase

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

const (
	migrationDocumentBatchCount = 1024
	migrationDocumentBatchBytes = 8 * 1024 * 1024
)

// MigrateToV2 writes a consistent logical snapshot of an open V1 durable DB to
// a new V2 file. The source remains open and unchanged. The destination must not
// exist and is published only after V2 reopen and semantic verification.
// A successful migration deliberately has a new database identity, invalidating
// every V1 resume token rather than mapping it onto unrelated V2 commit history.
func (db *DB) MigrateToV2(ctx context.Context, destination string) error {
	options := V2DestinationOptions{}
	if db != nil {
		options.ResourceLimits = db.resourceLimits
	}
	return db.MigrateToV2WithOptions(ctx, destination, options)
}

// MigrateToV2WithOptions is MigrateToV2 with an explicit destination quota.
func (db *DB) MigrateToV2WithOptions(ctx context.Context, destination string, options V2DestinationOptions) error {
	if db == nil {
		return ErrMigrationUnsupported
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if destination == "" {
		return errors.New("meldbase: empty migration destination")
	}
	destination, err := filepath.Abs(filepath.Clean(destination))
	if err != nil {
		return err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	if db.fatalErr != nil {
		return db.fatalErr
	}
	if db.store == nil || db.durability != db.store {
		return ErrMigrationUnsupported
	}
	source, err := filepath.Abs(filepath.Clean(db.store.path))
	if err != nil {
		return err
	}
	if source == destination {
		return ErrMigrationDestinationExists
	}
	if _, err := os.Lstat(destination); err == nil {
		return ErrMigrationDestinationExists
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return db.migrateLockedToV2(ctx, destination, options)
}

func (db *DB) migrateLockedToV2(ctx context.Context, destination string, options V2DestinationOptions) error {
	resourceLimits, err := normalizeResourceLimits(options.ResourceLimits)
	if err != nil {
		return err
	}
	directory := filepath.Dir(destination)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(destination)+".migrate-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	defer os.Remove(temporaryPath)

	file, _, _, err := storagev2.OpenWithOptions(temporaryPath, storagev2.OpenOptions{MaxFileBytes: options.StorageLimits.MaxFileBytes})
	if err != nil {
		return mapStorageV2Error(err)
	}
	closeFile := func(current error) error {
		return errors.Join(current, mapStorageV2Error(file.Close()))
	}
	if err := db.writeV2SnapshotLocked(ctx, file, resourceLimits); err != nil {
		return closeFile(err)
	}
	if _, err := file.Reachability(); err != nil {
		return closeFile(mapStorageV2Error(err))
	}
	if err := closeFile(nil); err != nil {
		return err
	}

	verified, err := OpenV2WithOptions(temporaryPath, V2Options{StorageLimits: options.StorageLimits, ResourceLimits: resourceLimits})
	if err != nil {
		return err
	}
	if verified.databaseID == db.databaseID {
		_ = verified.Close()
		return fmt.Errorf("%w: migration reused database identity", ErrCorrupt)
	}
	if err := verifyV2MigrationLocked(db, verified); err != nil {
		_ = verified.Close()
		return err
	}
	if err := verified.Close(); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}

	if err := publishMigrationFile(temporaryPath, destination, migrationPublishOps{
		link: os.Link, remove: os.Remove, syncDirectory: syncDirectory,
	}); err != nil {
		return err
	}
	// The destination name is durable after publishMigrationFile. Cleanup of the
	// private hard link is best effort and cannot turn a committed migration into
	// a reported failure; a crash may leave only an ignorable hidden orphan.
	_ = os.Remove(temporaryPath)
	_ = syncDirectory(directory)
	return nil
}

type migrationPublishOps struct {
	link          func(string, string) error
	remove        func(string) error
	syncDirectory func(string) error
}

// publishMigrationFile has one commit point: successful directory sync after
// the no-overwrite link. Before it, errors remove our link; after it, the
// complete verified destination is committed and private-link cleanup is not
// allowed to change the result.
func publishMigrationFile(temporary, destination string, ops migrationPublishOps) error {
	if ops.link == nil || ops.remove == nil || ops.syncDirectory == nil {
		return ErrCorrupt
	}
	if err := ops.link(temporary, destination); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return ErrMigrationDestinationExists
		}
		return err
	}
	directory := filepath.Dir(destination)
	if err := ops.syncDirectory(directory); err != nil {
		if sameFile(temporary, destination) {
			_ = ops.remove(destination)
			_ = ops.syncDirectory(directory)
		}
		return err
	}
	return nil
}

func (db *DB) writeV2SnapshotLocked(ctx context.Context, file *storagev2.File, resourceLimits ResourceLimits) error {
	names := make([]string, 0, len(db.collections))
	for name := range db.collections {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := contextError(ctx); err != nil {
			return err
		}
		data := db.collections[name]
		if data == nil {
			return ErrCorrupt
		}
		transactionID, err := newMigrationTransactionID()
		if err != nil {
			return err
		}
		if _, err := file.ApplyCreateCollection(storagev2.CreateCollectionTransaction{
			TransactionID: transactionID, Collection: name,
		}); err != nil {
			return mapStorageV2Error(err)
		}
		if err := writeV2Documents(ctx, file, name, data); err != nil {
			return err
		}
		if err := writeV2Indexes(ctx, file, name, data, db, resourceLimits); err != nil {
			return err
		}
	}
	return nil
}

func writeV2Documents(ctx context.Context, file *storagev2.File, collection string, data *collectionData) error {
	seen := make(map[DocumentID]struct{}, len(data.order))
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
		if _, err := file.ApplyDocumentTransaction(storagev2.DocumentTransaction{
			TransactionID: transactionID, Mutations: pending,
		}); err != nil {
			return mapStorageV2Error(err)
		}
		pending = pending[:0]
		pendingBytes = 0
		return nil
	}
	for _, id := range data.order {
		if err := contextError(ctx); err != nil {
			return err
		}
		if _, duplicate := seen[id]; duplicate {
			return ErrCorrupt
		}
		seen[id] = struct{}{}
		document, exists := data.documents[id]
		if !exists {
			return ErrCorrupt
		}
		stored, err := encodeStoredDocument(document)
		if err != nil {
			return err
		}
		if len(pending) > 0 && (len(pending) == migrationDocumentBatchCount || pendingBytes+len(stored) > migrationDocumentBatchBytes) {
			if err := flush(); err != nil {
				return err
			}
		}
		pending = append(pending, storagev2.DocumentMutation{
			Collection: collection, DocumentID: [16]byte(id), Operation: storagev2.DocumentInsert, Document: stored,
		})
		pendingBytes += len(stored)
	}
	if len(seen) != len(data.documents) {
		return ErrCorrupt
	}
	return flush()
}

func writeV2Indexes(ctx context.Context, file *storagev2.File, collection string, data *collectionData, db *DB, resourceLimits ResourceLimits) error {
	names := make([]string, 0, len(data.indexes))
	for name := range data.indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := contextError(ctx); err != nil {
			return err
		}
		state := data.indexes[name]
		if state == nil || state.definition.Name != name || state.definition.Order != 1 {
			return ErrCorrupt
		}
		// Do not reserve from untrusted collection cardinality before the build
		// budget has admitted entries. Growth remains bounded by that budget.
		entries := make([]storagev2.IndexEntry, 0, min(len(data.order), 1024))
		budget := db.newIndexBuildBudget(resourceLimits)
		for _, id := range data.order {
			document := data.documents[id]
			value, exists := lookupInternal(document, state.definition.Field)
			if !exists {
				continue
			}
			key, err := encodeIndexKey(value)
			if err != nil {
				return ErrCorrupt
			}
			if err := budget.add(key); err != nil {
				return err
			}
			entries = append(entries, storagev2.IndexEntry{Key: key, DocumentID: [16]byte(id)})
		}
		transactionID, err := newMigrationTransactionID()
		if err != nil {
			return err
		}
		if _, err := file.ApplyCreateIndex(storagev2.CreateIndexTransaction{
			TransactionID: transactionID, Collection: collection, Name: name,
			FieldPath: state.definition.Field, Unique: state.definition.Unique, Entries: entries,
		}); err != nil {
			return mapStorageV2Error(err)
		}
	}
	return nil
}

func verifyV2MigrationLocked(source, destination *DB) error {
	if source == nil || destination == nil || len(source.collections) != len(destination.collections) {
		return fmt.Errorf("%w: migration collection count", ErrCorrupt)
	}
	query, err := CompileQuery(Filter{}, QueryOptions{})
	if err != nil {
		return err
	}
	for name, expected := range source.collections {
		actual := destination.collections[name]
		if expected == nil || actual == nil || len(expected.indexes) != len(actual.indexes) {
			return fmt.Errorf("%w: migration collection %s", ErrCorrupt, name)
		}
		destination.mu.RLock()
		documents, _, queryErr := destination.Collection(name).planStorageLocked(context.Background(), query)
		destination.mu.RUnlock()
		if queryErr != nil || len(documents) != len(expected.order) {
			return fmt.Errorf("%w: migration collection %s", ErrCorrupt, name)
		}
		for index, id := range expected.order {
			actualDocument := documents[index]
			actualID, actualOK := actualDocument.ID()
			if !actualOK || actualID != id {
				return fmt.Errorf("%w: migration document order", ErrCorrupt)
			}
			expectedDocument, expectedOK := expected.documents[id]
			if !expectedOK {
				return fmt.Errorf("%w: migration document identity", ErrCorrupt)
			}
			expectedBytes, expectedErr := encodeStoredDocument(expectedDocument)
			actualBytes, actualErr := encodeStoredDocument(actualDocument)
			if expectedErr != nil || actualErr != nil || !bytes.Equal(expectedBytes, actualBytes) {
				return fmt.Errorf("%w: migration document value", ErrCorrupt)
			}
		}
		for indexName, expectedIndex := range expected.indexes {
			actualIndex := actual.indexes[indexName]
			if expectedIndex == nil || actualIndex == nil || !equalIndexDefinitions(expectedIndex.definition, actualIndex.definition) {
				return fmt.Errorf("%w: migration index %s", ErrCorrupt, indexName)
			}
		}
	}
	return nil
}

func newMigrationTransactionID() ([16]byte, error) {
	var result [16]byte
	_, err := rand.Read(result[:])
	return result, err
}

func sameFile(left, right string) bool {
	leftInfo, leftErr := os.Stat(left)
	rightInfo, rightErr := os.Stat(right)
	return leftErr == nil && rightErr == nil && os.SameFile(leftInfo, rightInfo)
}
