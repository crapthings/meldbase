package meldbase

import (
	"context"
	"encoding/hex"
	"fmt"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

// V2VerificationReport is a schema-versioned receipt for a full, read-only V2
// protected-page graph and published-index semantic audit. ReclaimablePages is
// informational; verification never installs a free pool or publishes
// maintenance metadata.
type V2VerificationReport struct {
	SchemaVersion              int           `json:"schemaVersion"`
	Verified                   bool          `json:"verified"`
	Format                     StorageFormat `json:"format"`
	Revision                   uint16        `json:"revision"`
	DatabaseIDHex              string        `json:"databaseIdHex"`
	MetaGeneration             uint64        `json:"metaGeneration"`
	CommitSequence             uint64        `json:"commitSequence"`
	OldestRetainedSequence     uint64        `json:"oldestRetainedSequence"`
	RequiredFeatures           uint64        `json:"requiredFeatures"`
	OptionalFeatures           uint64        `json:"optionalFeatures"`
	ValidMetaSlots             int           `json:"validMetaSlots"`
	FileBytes                  uint64        `json:"fileBytes"`
	TrailingBytes              uint64        `json:"trailingBytes"`
	PhysicalPages              uint64        `json:"physicalPages"`
	CommittedPhysicalPages     uint64        `json:"committedPhysicalPages"`
	ReachablePages             uint64        `json:"reachablePages"`
	ReclaimablePages           uint64        `json:"reclaimablePages"`
	PersistentFreeSpace        bool          `json:"persistentFreeSpace"`
	FreeSpaceValid             bool          `json:"freeSpaceValid"`
	IndexContentsVerified      bool          `json:"indexContentsVerified"`
	IndexBuildContentsVerified bool          `json:"indexBuildContentsVerified"`
	SHA256                     string        `json:"sha256"`
}

// VerifyV2File performs an offline, non-mutating audit of an existing V2 file.
// It takes a non-blocking shared advisory lock, so an active writer fails with
// ErrDatabaseLocked. It never creates, truncates, repairs, reclaims, or advances
// the database. Meta inspection alone is cheaper; this method walks every page
// protected by both valid Meta roots, recomputes published and provable shadow
// Secondary keys from canonical Primary documents in both directions, and
// hashes the file. Legacy caught-up builds lacking an applied CatalogRoot remain
// readable but report IndexBuildContentsVerified=false.
func VerifyV2File(ctx context.Context, path string) (V2VerificationReport, error) {
	info, err := InspectStorageFormat(path)
	if err != nil {
		return V2VerificationReport{}, err
	}
	if info.Format != StorageFormatV2 {
		return V2VerificationReport{}, ErrVerificationUnsupported
	}
	if !info.ReaderCompatible {
		return V2VerificationReport{}, ErrUnsupportedFormat
	}
	verified, err := storagev2.VerifyPathContextWithIndexAudit(ctx, path, auditV2IndexKey)
	if err != nil {
		return V2VerificationReport{}, mapStorageV2Error(err)
	}
	meta := verified.Meta
	if meta.DatabaseID == ([16]byte{}) || verified.PhysicalPages < meta.PhysicalPageCount {
		return V2VerificationReport{}, fmt.Errorf("%w: invalid verification result", ErrCorrupt)
	}
	return V2VerificationReport{
		SchemaVersion: 3, Verified: true, Format: StorageFormatV2, Revision: storagev2.FormatVersion,
		DatabaseIDHex: hex.EncodeToString(meta.DatabaseID[:]), MetaGeneration: meta.Generation,
		CommitSequence: meta.CommitSequence, OldestRetainedSequence: meta.OldestRetainedSequence,
		RequiredFeatures: meta.RequiredFeatures, OptionalFeatures: meta.OptionalFeatures,
		ValidMetaSlots: verified.ValidMetaSlots, FileBytes: verified.FileBytes,
		TrailingBytes: verified.TrailingBytes, PhysicalPages: verified.PhysicalPages,
		CommittedPhysicalPages: meta.PhysicalPageCount, ReachablePages: verified.ReachablePages,
		ReclaimablePages: verified.ReclaimablePages, PersistentFreeSpace: verified.PersistentFreeSpace,
		FreeSpaceValid: verified.FreeSpaceValid, IndexContentsVerified: verified.SemanticIndexesVerified,
		IndexBuildContentsVerified: verified.SemanticIndexBuildsVerified,
		SHA256:                     hex.EncodeToString(verified.SHA256[:]),
	}, nil
}

func auditV2IndexKey(meta storagev2.IndexMeta, id [16]byte, encoded []byte) ([]byte, bool, error) {
	fields, err := publicV2IndexFields(meta.FieldPath, meta.Fields)
	if err != nil {
		return nil, false, err
	}
	definition := newIndexDefinition(meta.Name, fields, meta.Unique)
	return projectedIndexBuildKey(encoded, definition, DocumentID(id))
}
