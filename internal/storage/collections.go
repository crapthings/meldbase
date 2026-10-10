package storage

import (
	"errors"
	"math"
	"time"
)

// ErrCollectionExists reports an attempt to create a collection that already exists.
var ErrCollectionExists = errors.New("meldbase storage: collection exists")

// CreateCollectionTransaction describes the creation of an empty stored collection.
//
// Creation receives its own commit identity and replay event without a document mutation.
type CreateCollectionTransaction struct {
	TransactionID [16]byte
	CommittedAt   time.Time
	Collection    string
}

// ApplyCreateCollection commits an empty collection and its catalog change event together.
func (f *File) ApplyCreateCollection(transaction CreateCollectionTransaction) (uint64, error) {
	if f == nil || allZero(transaction.TransactionID[:]) || !validCollectionName(transaction.Collection) {
		return 0, ErrCorrupt
	}
	if transaction.CommittedAt.IsZero() {
		transaction.CommittedAt = time.Now()
	}
	var sequence uint64
	err := f.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		sequence = tx.Sequence()
		base := tx.BaseRoot()
		if base.CollectionCount == math.MaxUint32 {
			return DatabaseRoot{}, ErrCorrupt
		}
		catalog, err := tx.OpenTree(base.CatalogRoot, TreeCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if _, exists, err := catalog.Get([]byte(transaction.Collection)); err != nil || exists {
			if err != nil {
				return DatabaseRoot{}, err
			}
			return DatabaseRoot{}, ErrCollectionExists
		}
		primary, err := tx.OpenTree(0, TreePrimary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		primaryRoot, err := primary.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		order, err := tx.OpenTree(0, TreeOrder)
		if err != nil {
			return DatabaseRoot{}, err
		}
		orderRoot, err := order.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		indexes, err := tx.OpenTree(0, TreeIndexCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		indexRoot, err := indexes.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		meta := CollectionMeta{
			ID: uint32(base.CollectionCount + 1), PrimaryRoot: primaryRoot, OrderRoot: orderRoot, IndexCatalogRoot: indexRoot,
			CreatedSequence: tx.Sequence(), UpdatedSequence: tx.Sequence(),
		}
		encoded, err := encodeCollectionMeta(meta)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := catalog.Put([]byte(transaction.Collection), encoded); err != nil {
			return DatabaseRoot{}, err
		}
		catalogRoot, err := catalog.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		commitLogRoot, oldest, err := tx.AppendCommitRetained(base.CommitLogRoot, base.OldestRetainedSequence, CommitBatch{
			Sequence: tx.Sequence(), TransactionID: transaction.TransactionID, CommittedAt: transaction.CommittedAt,
			CatalogRoot: catalogRoot,
			Changes: []CommitChange{{
				CollectionID: meta.ID, CollectionName: transaction.Collection, Operation: CommitCatalog,
				ChangedPaths: []string{"_catalog"}, After: encoded,
			}},
		})
		if err != nil {
			return DatabaseRoot{}, err
		}
		return DatabaseRoot{
			CommitSequence: tx.Sequence(), CatalogRoot: catalogRoot, CommitLogRoot: commitLogRoot,
			FreeSpaceRoot: base.FreeSpaceRoot, OldestRetainedSequence: oldest,
			CatalogGeneration: base.CatalogGeneration + 1, DocumentCount: base.DocumentCount,
			CollectionCount: base.CollectionCount + 1,
		}, nil
	})
	return sequence, err
}
