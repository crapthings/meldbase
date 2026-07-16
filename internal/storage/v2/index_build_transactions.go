package v2

import (
	"bytes"
	"errors"
	"time"
)

const (
	MaxConcurrentIndexBuilds      = 64
	MaxIndexBuildBatchEntries     = 4096
	MaxIndexBuildBatchBytes       = 16 << 20
	MaxIndexBuildCatchUpCommits   = 1024
	MaxIndexBuildCatchUpMutations = 10_000
)

var (
	ErrIndexBuildExists      = errors.New("meldbase storage v2: index build exists")
	ErrIndexBuildNotFound    = errors.New("meldbase storage v2: index build not found")
	ErrIndexBuildState       = errors.New("meldbase storage v2: invalid index build state")
	ErrIndexBuildHistoryLost = errors.New("meldbase storage v2: index build commit history lost")
)

type BeginIndexBuildTransaction struct {
	BuildID    [16]byte
	Collection string
	Name       string
	FieldPath  string
	Fields     []IndexField
	Unique     bool
	CreatedAt  time.Time
}

type IndexBuildScanBatch struct {
	BuildID           [16]byte
	ExpectedScanAfter [16]byte
	ScanAfter         [16]byte
	Entries           []IndexEntry
	Complete          bool
	UpdatedAt         time.Time
}

type IndexBuildCatchUpMutation struct {
	Sequence   uint64
	DocumentID [16]byte
	Operation  CommitOperation
	BeforeKey  []byte
	AfterKey   []byte
}

type IndexBuildCatchUpBatch struct {
	BuildID                 [16]byte
	ExpectedAppliedSequence uint64
	ThroughSequence         uint64
	Mutations               []IndexBuildCatchUpMutation
	UpdatedAt               time.Time
}

type FinalizeIndexBuildTransaction struct {
	BuildID                 [16]byte
	TransactionID           [16]byte
	ExpectedAppliedSequence uint64
	CommittedAt             time.Time
}

type FailIndexBuildTransaction struct {
	BuildID   [16]byte
	Failure   IndexBuildFailure
	UpdatedAt time.Time
}

// FailIndexBuild durably stops automatic progress without publishing or
// deleting the private tree. Failed builds no longer pin Commit Log history;
// their source/shadow pages remain reachable until explicit abort.
func (f *File) FailIndexBuild(transaction FailIndexBuildTransaction) (IndexBuildMeta, error) {
	if f == nil || allZero(transaction.BuildID[:]) || transaction.Failure == IndexBuildFailureNone ||
		transaction.Failure > IndexBuildFailureInvalidIndex {
		return IndexBuildMeta{}, ErrCorrupt
	}
	if transaction.UpdatedAt.IsZero() {
		transaction.UpdatedAt = time.Now().UTC()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var result IndexBuildMeta
	err := f.updateUnlocked(false, func(tx *WriteTxn) (DatabaseRoot, error) {
		base := tx.BaseRoot()
		if base.IndexBuildCatalogRoot == 0 {
			return DatabaseRoot{}, ErrIndexBuildNotFound
		}
		builds, err := tx.OpenTree(base.IndexBuildCatalogRoot, TreeIndexBuildCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		encoded, exists, err := builds.Get(transaction.BuildID[:])
		if err != nil || !exists {
			if err != nil {
				return DatabaseRoot{}, err
			}
			return DatabaseRoot{}, ErrIndexBuildNotFound
		}
		meta, err := decodeIndexBuildMeta(transaction.BuildID[:], encoded)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if meta.Phase == IndexBuildFailed {
			return DatabaseRoot{}, ErrIndexBuildState
		}
		if transaction.UpdatedAt.Before(meta.UpdatedAt) {
			return DatabaseRoot{}, ErrIndexBuildState
		}
		meta.Phase = IndexBuildFailed
		meta.Failure = transaction.Failure
		meta.UpdatedAt = transaction.UpdatedAt.UTC()
		encoded, err = encodeIndexBuildMeta(meta)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := builds.Put(transaction.BuildID[:], encoded); err != nil {
			return DatabaseRoot{}, err
		}
		base.IndexBuildCatalogRoot, err = builds.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		tx.indexBuildCatalogChanged = true
		result = meta
		return base, nil
	})
	return result, err
}

// BeginIndexBuild protects the current CatalogRoot and one empty shadow
// Secondary root in a physical maintenance generation. It does not advance the
// logical commit sequence or publish an ordinary index definition.
func (f *File) BeginIndexBuild(transaction BeginIndexBuildTransaction) (IndexBuildMeta, error) {
	fields, validFields := normalizeIndexFields(transaction.FieldPath, transaction.Fields)
	if f == nil || allZero(transaction.BuildID[:]) || !validCollectionName(transaction.Collection) ||
		!validIndexName(transaction.Name) || !validFields {
		return IndexBuildMeta{}, ErrCorrupt
	}
	if transaction.CreatedAt.IsZero() {
		transaction.CreatedAt = time.Now().UTC()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var result IndexBuildMeta
	err := f.updateUnlocked(false, func(tx *WriteTxn) (DatabaseRoot, error) {
		base := tx.BaseRoot()
		if tx.Sequence() == 0 || base.CatalogRoot < 2 {
			return DatabaseRoot{}, ErrIndexBuildState
		}
		if err := tx.requireFeature(RequiredFeatureShadowIndexBuilds); err != nil {
			return DatabaseRoot{}, err
		}
		if compoundIndexFields(fields) {
			if err := tx.requireFeature(RequiredFeatureCompoundIndexes); err != nil {
				return DatabaseRoot{}, err
			}
		}
		catalog, err := tx.OpenTree(base.CatalogRoot, TreeCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		encodedCollection, exists, err := catalog.Get([]byte(transaction.Collection))
		if err != nil || !exists {
			if err != nil {
				return DatabaseRoot{}, err
			}
			return DatabaseRoot{}, ErrIndexBuildState
		}
		collection, err := decodeCollectionMeta(encodedCollection)
		if err != nil {
			return DatabaseRoot{}, err
		}
		indexes, err := tx.OpenTree(collection.IndexCatalogRoot, TreeIndexCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if _, exists, err := indexes.Get([]byte(transaction.Name)); err != nil || exists {
			if err != nil {
				return DatabaseRoot{}, err
			}
			return DatabaseRoot{}, ErrIndexExists
		}
		builds, err := tx.OpenTree(base.IndexBuildCatalogRoot, TreeIndexBuildCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if _, exists, err := builds.Get(transaction.BuildID[:]); err != nil || exists {
			if err != nil {
				return DatabaseRoot{}, err
			}
			return DatabaseRoot{}, ErrIndexBuildExists
		}
		pending, err := builds.Scan(nil, nil, MaxConcurrentIndexBuilds+1)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if len(pending) >= MaxConcurrentIndexBuilds {
			return DatabaseRoot{}, ErrIndexBuildState
		}
		for _, item := range pending {
			meta, err := decodeIndexBuildMeta(item.Key, item.Value)
			if err != nil {
				return DatabaseRoot{}, err
			}
			if meta.CollectionID == collection.ID && meta.Name == transaction.Name {
				return DatabaseRoot{}, ErrIndexBuildExists
			}
		}
		shadow, err := tx.NewSortedTreeBuilder(TreeSecondary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		shadowRoot, err := shadow.Finish()
		if err != nil {
			return DatabaseRoot{}, err
		}
		result = IndexBuildMeta{
			BuildID: transaction.BuildID, CollectionID: collection.ID, Collection: transaction.Collection,
			Name: transaction.Name, FieldPath: fields[0].Path, Fields: fields, Unique: transaction.Unique,
			Phase: IndexBuildScan, SourceSequence: tx.Sequence(), SourceCatalogRoot: base.CatalogRoot,
			ShadowRoot: shadowRoot, AppliedSequence: tx.Sequence(), CreatedAt: transaction.CreatedAt.UTC(), UpdatedAt: transaction.CreatedAt.UTC(),
		}
		encodedBuild, err := encodeIndexBuildMeta(result)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := builds.Put(transaction.BuildID[:], encodedBuild); err != nil {
			return DatabaseRoot{}, err
		}
		base.IndexBuildCatalogRoot, err = builds.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		tx.indexBuildCatalogChanged = true
		return base, nil
	})
	return result, err
}

// ApplyIndexBuildScanBatch adds one Primary-key-ordered, bounded batch to the
// private Secondary tree and advances its durable scan cursor. Source records
// are resolved through the build's protected CatalogRoot, never current state.
func (f *File) ApplyIndexBuildScanBatch(batch IndexBuildScanBatch) (IndexBuildMeta, error) {
	if f == nil || allZero(batch.BuildID[:]) || len(batch.Entries) > MaxIndexBuildBatchEntries {
		return IndexBuildMeta{}, ErrCorrupt
	}
	if batch.UpdatedAt.IsZero() {
		batch.UpdatedAt = time.Now().UTC()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var result IndexBuildMeta
	err := f.updateUnlocked(false, func(tx *WriteTxn) (DatabaseRoot, error) {
		base := tx.BaseRoot()
		if base.IndexBuildCatalogRoot == 0 || tx.requiredFeatures&RequiredFeatureShadowIndexBuilds == 0 {
			return DatabaseRoot{}, ErrIndexBuildNotFound
		}
		builds, err := tx.OpenTree(base.IndexBuildCatalogRoot, TreeIndexBuildCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		encoded, exists, err := builds.Get(batch.BuildID[:])
		if err != nil || !exists {
			if err != nil {
				return DatabaseRoot{}, err
			}
			return DatabaseRoot{}, ErrIndexBuildNotFound
		}
		meta, err := decodeIndexBuildMeta(batch.BuildID[:], encoded)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if meta.Phase != IndexBuildScan || meta.ScanAfter != batch.ExpectedScanAfter || batch.UpdatedAt.Before(meta.UpdatedAt) ||
			(!batch.Complete && (allZero(batch.ScanAfter[:]) || bytes.Compare(batch.ScanAfter[:], meta.ScanAfter[:]) <= 0)) ||
			(batch.Complete && bytes.Compare(batch.ScanAfter[:], meta.ScanAfter[:]) < 0) {
			return DatabaseRoot{}, ErrIndexBuildState
		}
		catalog, err := tx.OpenTree(meta.SourceCatalogRoot, TreeCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		encodedCollection, exists, err := catalog.Get([]byte(meta.Collection))
		if err != nil || !exists {
			return DatabaseRoot{}, ErrCorrupt
		}
		collection, err := decodeCollectionMeta(encodedCollection)
		if err != nil || collection.ID != meta.CollectionID {
			return DatabaseRoot{}, ErrCorrupt
		}
		primary, err := tx.OpenTree(collection.PrimaryRoot, TreePrimary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		order, err := tx.OpenTree(collection.OrderRoot, TreeOrder)
		if err != nil {
			return DatabaseRoot{}, err
		}
		shadow, err := tx.OpenTree(meta.ShadowRoot, TreeSecondary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		previousID := meta.ScanAfter
		batchBytes := uint64(0)
		for index := range batch.Entries {
			entry := &batch.Entries[index]
			if len(entry.Key) == 0 || len(entry.Key) > MaxSecondaryScalarKeyBytes || allZero(entry.DocumentID[:]) ||
				bytes.Compare(entry.DocumentID[:], previousID[:]) <= 0 ||
				(!allZero(batch.ScanAfter[:]) && bytes.Compare(entry.DocumentID[:], batch.ScanAfter[:]) > 0) {
				return DatabaseRoot{}, ErrCorrupt
			}
			stored, exists, err := primary.getBorrowed(entry.DocumentID[:])
			if err != nil || !exists {
				return DatabaseRoot{}, ErrCorrupt
			}
			position, _, err := decodeDocumentRecordDescriptor(stored)
			if err != nil || (entry.InsertionPosition != 0 && entry.InsertionPosition != position) {
				return DatabaseRoot{}, ErrCorrupt
			}
			owner, exists, err := order.Get(insertionPositionKey(position))
			if err != nil || !exists || !bytes.Equal(owner, entry.DocumentID[:]) {
				return DatabaseRoot{}, ErrCorrupt
			}
			key, err := secondaryKey(entry.Key, position, entry.DocumentID)
			if err != nil {
				return DatabaseRoot{}, err
			}
			entryBytes := uint64(len(key))
			if batchBytes > MaxIndexBuildBatchBytes || entryBytes > MaxIndexBuildBatchBytes-batchBytes {
				return DatabaseRoot{}, ErrIndexBuildState
			}
			if err := shadow.Put(key, []byte{0}); err != nil {
				return DatabaseRoot{}, err
			}
			batchBytes += entryBytes
			previousID = entry.DocumentID
		}
		if len(batch.Entries) > 0 && bytes.Compare(batch.ScanAfter[:], previousID[:]) < 0 {
			return DatabaseRoot{}, ErrIndexBuildState
		}
		if meta.EntryCount > ^uint64(0)-uint64(len(batch.Entries)) || meta.CanonicalBytes > ^uint64(0)-batchBytes {
			return DatabaseRoot{}, ErrCorrupt
		}
		meta.ShadowRoot, err = shadow.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		meta.ScanAfter = batch.ScanAfter
		meta.EntryCount += uint64(len(batch.Entries))
		meta.CanonicalBytes += batchBytes
		meta.UpdatedAt = batch.UpdatedAt.UTC()
		if batch.Complete {
			meta.Phase = IndexBuildCatchUp
			if meta.AppliedSequence == tx.Sequence() {
				meta.Phase = IndexBuildReady
			}
		}
		encoded, err = encodeIndexBuildMeta(meta)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := builds.Put(batch.BuildID[:], encoded); err != nil {
			return DatabaseRoot{}, err
		}
		base.IndexBuildCatalogRoot, err = builds.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		tx.indexBuildCatalogChanged = true
		result = meta
		return base, nil
	})
	return result, err
}

// ApplyIndexBuildCatchUpBatch applies caller-derived keys for every relevant
// Commit Log change in one contiguous sequence interval. The storage layer
// validates sequence continuity, operation/document identity and immutable
// Before/After document positions before mutating the private tree.
func (f *File) ApplyIndexBuildCatchUpBatch(batch IndexBuildCatchUpBatch) (IndexBuildMeta, error) {
	if f == nil || allZero(batch.BuildID[:]) || len(batch.Mutations) > MaxIndexBuildCatchUpMutations {
		return IndexBuildMeta{}, ErrCorrupt
	}
	if batch.UpdatedAt.IsZero() {
		batch.UpdatedAt = time.Now().UTC()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var result IndexBuildMeta
	err := f.updateUnlocked(false, func(tx *WriteTxn) (DatabaseRoot, error) {
		base := tx.BaseRoot()
		if base.IndexBuildCatalogRoot == 0 {
			return DatabaseRoot{}, ErrIndexBuildNotFound
		}
		builds, err := tx.OpenTree(base.IndexBuildCatalogRoot, TreeIndexBuildCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		encoded, exists, err := builds.Get(batch.BuildID[:])
		if err != nil || !exists {
			if err != nil {
				return DatabaseRoot{}, err
			}
			return DatabaseRoot{}, ErrIndexBuildNotFound
		}
		meta, err := decodeIndexBuildMeta(batch.BuildID[:], encoded)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if (meta.Phase != IndexBuildCatchUp && meta.Phase != IndexBuildReady) ||
			meta.AppliedSequence != batch.ExpectedAppliedSequence || batch.ThroughSequence <= meta.AppliedSequence ||
			batch.ThroughSequence > tx.Sequence() || batch.ThroughSequence-meta.AppliedSequence > MaxIndexBuildCatchUpCommits ||
			batch.UpdatedAt.Before(meta.UpdatedAt) {
			return DatabaseRoot{}, ErrIndexBuildState
		}
		if base.OldestRetainedSequence == 0 || meta.AppliedSequence+1 < base.OldestRetainedSequence {
			return DatabaseRoot{}, ErrIndexBuildHistoryLost
		}
		commitLog, err := tx.OpenTree(base.CommitLogRoot, TreeCommitLog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		shadow, err := tx.OpenTree(meta.ShadowRoot, TreeSecondary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		primaryTrees := make(map[uint64]*MutableTree)
		mutationIndex := 0
		throughCatalogRoot := uint64(0)
		for sequence := meta.AppliedSequence + 1; sequence <= batch.ThroughSequence; sequence++ {
			commit, err := tx.readCommitFromTree(commitLog, sequence)
			if err != nil {
				if sequence < base.OldestRetainedSequence {
					return DatabaseRoot{}, ErrIndexBuildHistoryLost
				}
				return DatabaseRoot{}, err
			}
			if commit.CatalogRoot < 2 {
				return DatabaseRoot{}, ErrCorrupt
			}
			throughCatalogRoot = commit.CatalogRoot
			for _, change := range commit.Changes {
				if change.CollectionID != meta.CollectionID || change.Operation == CommitCatalog {
					continue
				}
				if mutationIndex >= len(batch.Mutations) {
					return DatabaseRoot{}, ErrIndexBuildState
				}
				mutation := batch.Mutations[mutationIndex]
				mutationIndex++
				if mutation.Sequence != sequence || mutation.DocumentID != change.DocumentID || mutation.Operation != change.Operation ||
					len(mutation.BeforeKey) > MaxSecondaryScalarKeyBytes || len(mutation.AfterKey) > MaxSecondaryScalarKeyBytes {
					return DatabaseRoot{}, ErrIndexBuildState
				}
				if (change.Operation == CommitInsert && len(mutation.BeforeKey) != 0) ||
					(change.Operation == CommitDelete && len(mutation.AfterKey) != 0) {
					return DatabaseRoot{}, ErrIndexBuildState
				}
				beforePosition, afterPosition := uint64(0), uint64(0)
				if change.BeforeRef != nil {
					beforePosition, err = tx.indexBuildVersionPosition(primaryTrees, *change.BeforeRef, change.DocumentID)
					if err != nil {
						return DatabaseRoot{}, err
					}
				} else if change.Operation != CommitInsert {
					return DatabaseRoot{}, ErrCorrupt
				}
				if change.AfterRef != nil {
					afterPosition, err = tx.indexBuildVersionPosition(primaryTrees, *change.AfterRef, change.DocumentID)
					if err != nil {
						return DatabaseRoot{}, err
					}
				} else if change.Operation != CommitDelete {
					return DatabaseRoot{}, ErrCorrupt
				}
				if change.Operation == CommitUpdate && beforePosition != afterPosition {
					return DatabaseRoot{}, ErrCorrupt
				}
				if len(mutation.BeforeKey) > 0 {
					key, err := secondaryKey(mutation.BeforeKey, beforePosition, mutation.DocumentID)
					if err != nil {
						return DatabaseRoot{}, err
					}
					removed, err := shadow.Delete(key)
					if err != nil || !removed || meta.EntryCount == 0 || uint64(len(key)) > meta.CanonicalBytes {
						return DatabaseRoot{}, ErrIndexBuildState
					}
					meta.EntryCount--
					meta.CanonicalBytes -= uint64(len(key))
				}
				if len(mutation.AfterKey) > 0 {
					key, err := secondaryKey(mutation.AfterKey, afterPosition, mutation.DocumentID)
					if err != nil {
						return DatabaseRoot{}, err
					}
					if err := shadow.Put(key, []byte{0}); err != nil {
						return DatabaseRoot{}, err
					}
					if meta.EntryCount == ^uint64(0) || uint64(len(key)) > ^uint64(0)-meta.CanonicalBytes {
						return DatabaseRoot{}, ErrCorrupt
					}
					meta.EntryCount++
					meta.CanonicalBytes += uint64(len(key))
				}
			}
		}
		if mutationIndex != len(batch.Mutations) {
			return DatabaseRoot{}, ErrIndexBuildState
		}
		meta.ShadowRoot, err = shadow.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		meta.AppliedSequence = batch.ThroughSequence
		if err := tx.requireFeature(RequiredFeatureIndexBuildAppliedRoot); err != nil {
			return DatabaseRoot{}, err
		}
		meta.AppliedCatalogRoot = throughCatalogRoot
		meta.UpdatedAt = batch.UpdatedAt.UTC()
		meta.Phase = IndexBuildCatchUp
		if batch.ThroughSequence == tx.Sequence() {
			meta.Phase = IndexBuildReady
		}
		encoded, err = encodeIndexBuildMeta(meta)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := builds.Put(batch.BuildID[:], encoded); err != nil {
			return DatabaseRoot{}, err
		}
		base.IndexBuildCatalogRoot, err = builds.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		tx.indexBuildCatalogChanged = true
		result = meta
		return base, nil
	})
	return result, err
}

func (tx *WriteTxn) indexBuildVersionPosition(trees map[uint64]*MutableTree, reference DocumentVersionRef, documentID [16]byte) (uint64, error) {
	if tx == nil || reference.PrimaryRoot < 2 || reference.DocumentID != documentID {
		return 0, ErrCorrupt
	}
	tree := trees[reference.PrimaryRoot]
	if tree == nil {
		var err error
		tree, err = tx.OpenTree(reference.PrimaryRoot, TreePrimary)
		if err != nil {
			return 0, err
		}
		trees[reference.PrimaryRoot] = tree
	}
	stored, exists, err := tree.getBorrowed(documentID[:])
	if err != nil || !exists {
		return 0, ErrCorrupt
	}
	position, _, err := decodeDocumentRecordDescriptor(stored)
	return position, err
}

// FinalizeIndexBuild atomically moves an already-current shadow root into the
// ordinary IndexCatalog, removes its build record and appends one catalog
// Commit Log event. The Secondary tree is neither copied nor rebuilt.
func (f *File) FinalizeIndexBuild(transaction FinalizeIndexBuildTransaction) (uint64, error) {
	if f == nil || allZero(transaction.BuildID[:]) || allZero(transaction.TransactionID[:]) {
		return 0, ErrCorrupt
	}
	if transaction.CommittedAt.IsZero() {
		transaction.CommittedAt = time.Now().UTC()
	}
	var sequence uint64
	err := f.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		base := tx.BaseRoot()
		sequence = tx.Sequence()
		if base.IndexBuildCatalogRoot == 0 {
			return DatabaseRoot{}, ErrIndexBuildNotFound
		}
		builds, err := tx.OpenTree(base.IndexBuildCatalogRoot, TreeIndexBuildCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		encodedBuild, exists, err := builds.Get(transaction.BuildID[:])
		if err != nil || !exists {
			if err != nil {
				return DatabaseRoot{}, err
			}
			return DatabaseRoot{}, ErrIndexBuildNotFound
		}
		build, err := decodeIndexBuildMeta(transaction.BuildID[:], encodedBuild)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if build.Phase != IndexBuildReady || build.AppliedSequence != transaction.ExpectedAppliedSequence ||
			build.AppliedSequence != base.CommitSequence {
			return DatabaseRoot{}, ErrIndexBuildState
		}
		shadow, err := tx.OpenTree(build.ShadowRoot, TreeSecondary)
		if err != nil || shadow.root.count != build.EntryCount {
			return DatabaseRoot{}, ErrCorrupt
		}
		if build.Unique {
			if err := f.validateUniqueShadowUnlocked(build.ShadowRoot, build.EntryCount); err != nil {
				return DatabaseRoot{}, err
			}
		}
		catalog, err := tx.OpenTree(base.CatalogRoot, TreeCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		encodedCollection, exists, err := catalog.Get([]byte(build.Collection))
		if err != nil || !exists {
			return DatabaseRoot{}, ErrCorrupt
		}
		collection, err := decodeCollectionMeta(encodedCollection)
		if err != nil || collection.ID != build.CollectionID {
			return DatabaseRoot{}, ErrCorrupt
		}
		indexes, err := tx.OpenTree(collection.IndexCatalogRoot, TreeIndexCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if _, exists, err := indexes.Get([]byte(build.Name)); err != nil || exists {
			if err != nil {
				return DatabaseRoot{}, err
			}
			return DatabaseRoot{}, ErrIndexExists
		}
		index := IndexMeta{
			Name: build.Name, FieldPath: build.FieldPath, Fields: build.Fields, Unique: build.Unique, Root: build.ShadowRoot,
			EntryCount: build.EntryCount, CreatedSequence: tx.Sequence(), UpdatedSequence: tx.Sequence(),
		}
		encodedIndex, err := encodeIndexMeta(index)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := indexes.Put([]byte(build.Name), encodedIndex); err != nil {
			return DatabaseRoot{}, err
		}
		collection.IndexCatalogRoot, err = indexes.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		collection.UpdatedSequence = tx.Sequence()
		encodedCollection, err = encodeCollectionMeta(collection)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := catalog.Put([]byte(build.Collection), encodedCollection); err != nil {
			return DatabaseRoot{}, err
		}
		base.CatalogRoot, err = catalog.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		removed, err := builds.Delete(transaction.BuildID[:])
		if err != nil || !removed {
			return DatabaseRoot{}, ErrCorrupt
		}
		if builds.root.count == 0 {
			base.IndexBuildCatalogRoot = 0
		} else {
			base.IndexBuildCatalogRoot, err = builds.Flush()
			if err != nil {
				return DatabaseRoot{}, err
			}
		}
		tx.indexBuildCatalogChanged = true
		base.CommitLogRoot, base.OldestRetainedSequence, err = tx.AppendCommitRetained(
			base.CommitLogRoot, base.OldestRetainedSequence,
			CommitBatch{Sequence: tx.Sequence(), TransactionID: transaction.TransactionID, CommittedAt: transaction.CommittedAt,
				CatalogRoot: base.CatalogRoot, Changes: []CommitChange{{
					CollectionID: collection.ID, Operation: CommitCatalog,
					ChangedPaths: []string{"_indexes." + build.Name}, After: encodedIndex,
				}}},
		)
		if err != nil {
			return DatabaseRoot{}, err
		}
		base.CommitSequence = tx.Sequence()
		base.CatalogGeneration++
		return base, nil
	})
	return sequence, err
}

func (f *File) validateUniqueShadowUnlocked(root uint64, expectedCount uint64) error {
	iterator, err := newTreeIterator(f, root, TreeSecondary, nil, nil, 0)
	if err != nil {
		return err
	}
	var previous []byte
	count := uint64(0)
	for iterator.nextUnlocked() {
		scalar, _, _, err := secondaryKeyParts(iterator.Key())
		if err != nil || len(iterator.Value()) != 1 || iterator.Value()[0] != 0 {
			return ErrCorrupt
		}
		if previous != nil && bytes.Equal(previous, scalar) {
			return ErrUniqueConflict
		}
		previous = append(previous[:0], scalar...)
		count++
	}
	if iterator.err != nil {
		return iterator.err
	}
	if count != expectedCount {
		return ErrCorrupt
	}
	return nil
}

// IndexBuild returns one current build record without pinning it after return.
func (f *File) IndexBuild(buildID [16]byte) (IndexBuildMeta, bool, error) {
	if f == nil || allZero(buildID[:]) {
		return IndexBuildMeta{}, false, ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return IndexBuildMeta{}, false, errors.New("meldbase storage v2: file is closed")
	}
	if f.root.IndexBuildCatalogRoot == 0 {
		return IndexBuildMeta{}, false, nil
	}
	encoded, exists, err := f.treeGetUnlocked(f.root.IndexBuildCatalogRoot, TreeIndexBuildCatalog, buildID[:])
	if err != nil || !exists {
		return IndexBuildMeta{}, false, err
	}
	meta, err := decodeIndexBuildMeta(buildID[:], encoded)
	return meta, err == nil, err
}

func (f *File) IndexBuilds() ([]IndexBuildMeta, error) {
	if f == nil {
		return nil, ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return nil, errors.New("meldbase storage v2: file is closed")
	}
	if f.root.IndexBuildCatalogRoot == 0 {
		return nil, nil
	}
	iterator, err := newTreeIterator(f, f.root.IndexBuildCatalogRoot, TreeIndexBuildCatalog, nil, nil, MaxConcurrentIndexBuilds+1)
	if err != nil {
		return nil, err
	}
	items := make([]KeyValue, 0)
	for iterator.nextUnlocked() {
		items = append(items, KeyValue{Key: append([]byte(nil), iterator.Key()...), Value: append([]byte(nil), iterator.Value()...)})
	}
	if iterator.err != nil || len(items) > MaxConcurrentIndexBuilds {
		if iterator.err != nil {
			return nil, iterator.err
		}
		return nil, ErrCorrupt
	}
	result := make([]IndexBuildMeta, len(items))
	for index, item := range items {
		result[index], err = decodeIndexBuildMeta(item.Key, item.Value)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// OpenIndexBuildScanIterator streams the next protected source-Primary batch.
// Its reader pin protects the current DatabaseRoot, which in turn protects the
// build record, source CatalogRoot and shadow tree for the iterator lifetime.
func (f *File) OpenIndexBuildScanIterator(buildID [16]byte, limit int) (IndexBuildMeta, *DocumentIterator, error) {
	if f == nil || allZero(buildID[:]) || limit <= 0 || limit > MaxIndexBuildBatchEntries {
		return IndexBuildMeta{}, nil, ErrCorrupt
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return IndexBuildMeta{}, nil, errors.New("meldbase storage v2: file is closed")
	}
	if f.root.IndexBuildCatalogRoot == 0 {
		return IndexBuildMeta{}, nil, ErrIndexBuildNotFound
	}
	encoded, exists, err := f.treeGetUnlocked(f.root.IndexBuildCatalogRoot, TreeIndexBuildCatalog, buildID[:])
	if err != nil || !exists {
		if err != nil {
			return IndexBuildMeta{}, nil, err
		}
		return IndexBuildMeta{}, nil, ErrIndexBuildNotFound
	}
	build, err := decodeIndexBuildMeta(buildID[:], encoded)
	if err != nil {
		return IndexBuildMeta{}, nil, err
	}
	if build.Phase != IndexBuildScan {
		return IndexBuildMeta{}, nil, ErrIndexBuildState
	}
	encodedCollection, exists, err := f.treeGetUnlocked(build.SourceCatalogRoot, TreeCatalog, []byte(build.Collection))
	if err != nil || !exists {
		return IndexBuildMeta{}, nil, ErrCorrupt
	}
	collection, err := decodeCollectionMeta(encodedCollection)
	if err != nil || collection.ID != build.CollectionID {
		return IndexBuildMeta{}, nil, ErrCorrupt
	}
	var start []byte
	if !allZero(build.ScanAfter[:]) {
		next, ok := nextIndexBuildDocumentID(build.ScanAfter)
		if !ok {
			collection.PrimaryRoot = 0
		} else {
			start = next[:]
		}
	}
	tree, err := newTreeIterator(f, collection.PrimaryRoot, TreePrimary, start, nil, limit)
	if err != nil {
		return IndexBuildMeta{}, nil, err
	}
	pinID, err := f.addReaderPinUnlocked(f.root.CommitSequence, f.meta.RootPage, false)
	if err != nil {
		return IndexBuildMeta{}, nil, err
	}
	return build, &DocumentIterator{file: f, pinID: pinID, tree: tree}, nil
}

// OpenIndexBuildCatchUpSnapshot atomically reads the current build record and
// pins the current DatabaseRoot. Its Commit Log and every document-version root
// referenced by retained commits cannot be reclaimed until the snapshot closes.
func (f *File) OpenIndexBuildCatchUpSnapshot(buildID [16]byte) (IndexBuildMeta, *ReadSnapshot, error) {
	if f == nil || allZero(buildID[:]) {
		return IndexBuildMeta{}, nil, ErrCorrupt
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return IndexBuildMeta{}, nil, errors.New("meldbase storage v2: file is closed")
	}
	root, err := f.databaseRootUnlocked()
	if err != nil {
		return IndexBuildMeta{}, nil, err
	}
	if root.IndexBuildCatalogRoot == 0 {
		return IndexBuildMeta{}, nil, ErrIndexBuildNotFound
	}
	encoded, exists, err := f.treeGetUnlocked(root.IndexBuildCatalogRoot, TreeIndexBuildCatalog, buildID[:])
	if err != nil || !exists {
		if err != nil {
			return IndexBuildMeta{}, nil, err
		}
		return IndexBuildMeta{}, nil, ErrIndexBuildNotFound
	}
	build, err := decodeIndexBuildMeta(buildID[:], encoded)
	if err != nil {
		return IndexBuildMeta{}, nil, err
	}
	if build.Phase != IndexBuildCatchUp && build.Phase != IndexBuildReady {
		return IndexBuildMeta{}, nil, ErrIndexBuildState
	}
	if build.AppliedSequence > root.CommitSequence ||
		(build.AppliedSequence < root.CommitSequence && (root.OldestRetainedSequence == 0 || build.AppliedSequence+1 < root.OldestRetainedSequence)) {
		return IndexBuildMeta{}, nil, ErrIndexBuildHistoryLost
	}
	pinID, err := f.addReaderPinUnlocked(root.CommitSequence, f.meta.RootPage, false)
	if err != nil {
		return IndexBuildMeta{}, nil, err
	}
	return build, &ReadSnapshot{file: f, pinID: pinID, root: root}, nil
}

func nextIndexBuildDocumentID(current [16]byte) ([16]byte, bool) {
	next := current
	for index := len(next) - 1; index >= 0; index-- {
		next[index]++
		if next[index] != 0 {
			return next, true
		}
	}
	return [16]byte{}, false
}

// AbortIndexBuild removes only the protected build record. Its private pages
// become reclaimable under the normal dual-Meta epoch rules.
func (f *File) AbortIndexBuild(buildID [16]byte) error {
	if f == nil || allZero(buildID[:]) {
		return ErrCorrupt
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.updateUnlocked(false, func(tx *WriteTxn) (DatabaseRoot, error) {
		base := tx.BaseRoot()
		if base.IndexBuildCatalogRoot == 0 {
			return DatabaseRoot{}, ErrIndexBuildNotFound
		}
		builds, err := tx.OpenTree(base.IndexBuildCatalogRoot, TreeIndexBuildCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		encoded, exists, err := builds.Get(buildID[:])
		if err != nil || !exists {
			if err != nil {
				return DatabaseRoot{}, err
			}
			return DatabaseRoot{}, ErrIndexBuildNotFound
		}
		meta, err := decodeIndexBuildMeta(buildID[:], encoded)
		if err != nil || !bytes.Equal(meta.BuildID[:], buildID[:]) {
			return DatabaseRoot{}, ErrCorrupt
		}
		removed, err := builds.Delete(buildID[:])
		if err != nil || !removed {
			return DatabaseRoot{}, ErrCorrupt
		}
		if builds.root.count == 0 {
			base.IndexBuildCatalogRoot = 0
		} else {
			base.IndexBuildCatalogRoot, err = builds.Flush()
			if err != nil {
				return DatabaseRoot{}, err
			}
		}
		tx.indexBuildCatalogChanged = true
		return base, nil
	})
}
