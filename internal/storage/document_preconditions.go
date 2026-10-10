package storage

import (
	"crypto/sha256"
	"errors"
)

// validateDocumentPreconditions checks that document existence and contents still match the recorded reads.
func validateDocumentPreconditions(tx *WriteTxn, catalog *MutableTree, preconditions []DocumentPrecondition) error {
	if tx == nil || catalog == nil {
		return ErrCorrupt
	}
	seen := make(map[string]struct{}, len(preconditions))
	primary := make(map[string]*MutableTree)
	for _, precondition := range preconditions {
		if !validCollectionName(precondition.Collection) || allZero(precondition.DocumentID[:]) ||
			(!precondition.ExpectedExists && !allZero(precondition.ExpectedHash[:])) {
			return ErrCorrupt
		}
		identity := precondition.Collection + "\x00" + string(precondition.DocumentID[:])
		if _, duplicate := seen[identity]; duplicate {
			return ErrCorrupt
		}
		seen[identity] = struct{}{}
		tree := primary[precondition.Collection]
		if tree == nil {
			encoded, collectionExists, err := catalog.Get([]byte(precondition.Collection))
			if err != nil {
				return err
			}
			if !collectionExists {
				if precondition.ExpectedExists {
					return ErrDocumentConflict
				}
				continue
			}
			meta, err := decodeCollectionMeta(encoded)
			if err != nil {
				return err
			}
			tree, err = tx.OpenTree(meta.PrimaryRoot, TreePrimary)
			if err != nil {
				return err
			}
			primary[precondition.Collection] = tree
		}
		stored, exists, err := tree.Get(precondition.DocumentID[:])
		if err != nil {
			return err
		}
		if exists != precondition.ExpectedExists {
			return ErrDocumentConflict
		}
		if !exists {
			continue
		}
		_, document, err := tx.loadDocumentRecord(stored)
		if err != nil {
			return err
		}
		actual := sha256.Sum256(document)
		if !equalBytes(actual[:], precondition.ExpectedHash[:]) {
			return ErrDocumentConflict
		}
	}
	return nil
}

// validateCollectionPreconditions checks that collection identities and versions still match the recorded reads.
func validateCollectionPreconditions(catalog *MutableTree, preconditions []CollectionPrecondition) error {
	if catalog == nil {
		return ErrCorrupt
	}
	seen := make(map[string]struct{}, len(preconditions))
	for _, precondition := range preconditions {
		if !validCollectionName(precondition.Collection) ||
			(precondition.ExpectedExists && (precondition.ExpectedID == 0 || precondition.ExpectedUpdatedSequence == 0)) ||
			(!precondition.ExpectedExists && (precondition.ExpectedID != 0 || precondition.ExpectedUpdatedSequence != 0)) {
			return ErrCorrupt
		}
		if _, duplicate := seen[precondition.Collection]; duplicate {
			return ErrCorrupt
		}
		seen[precondition.Collection] = struct{}{}
		encoded, exists, err := catalog.Get([]byte(precondition.Collection))
		if err != nil {
			return err
		}
		if exists != precondition.ExpectedExists {
			return ErrDocumentConflict
		}
		if !exists {
			continue
		}
		meta, err := decodeCollectionMeta(encoded)
		if err != nil {
			return err
		}
		if meta.ID != precondition.ExpectedID || meta.UpdatedSequence != precondition.ExpectedUpdatedSequence {
			return ErrDocumentConflict
		}
	}
	return nil
}

// ValidateDocumentPreconditions checks recorded document reads against the current committed version.
func (f *File) ValidateDocumentPreconditions(preconditions []DocumentPrecondition) error {
	if f == nil {
		return ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return errors.New("meldbase storage: file is closed")
	}
	root, err := f.databaseRootUnlocked()
	if err != nil {
		return err
	}
	tx := &WriteTxn{
		file: f, generation: f.meta.Generation, sequence: f.meta.CommitSequence,
		nextPage: f.nextPage, byID: make(map[uint64][]byte),
	}
	catalog, err := tx.OpenTree(root.CatalogRoot, TreeCatalog)
	if err != nil {
		return err
	}
	return validateDocumentPreconditions(tx, catalog, preconditions)
}

// ValidateCollectionPreconditions checks recorded collection versions without committing changes.
//
// ApplyDocumentTransaction performs the same
// validation inside its atomic write transaction, which is the required commit
// path for phantom-safe callers.
func (f *File) ValidateCollectionPreconditions(preconditions []CollectionPrecondition) error {
	if f == nil {
		return ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return errors.New("meldbase storage: file is closed")
	}
	root, err := f.databaseRootUnlocked()
	if err != nil {
		return err
	}
	tx := &WriteTxn{
		file: f, generation: f.meta.Generation, sequence: f.meta.CommitSequence,
		nextPage: f.nextPage, byID: make(map[uint64][]byte),
	}
	catalog, err := tx.OpenTree(root.CatalogRoot, TreeCatalog)
	if err != nil {
		return err
	}
	return validateCollectionPreconditions(catalog, preconditions)
}
