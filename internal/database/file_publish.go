package database

import (
	"crypto/rand"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	// snapshotDocumentBatchCount limits documents in one snapshot copy batch.
	snapshotDocumentBatchCount = 1024
	// snapshotDocumentBatchBytes limits encoded bytes in one snapshot copy batch.
	snapshotDocumentBatchBytes = 8 * 1024 * 1024
)

// publishFileOps supplies filesystem operations for publishing a new file.
type publishFileOps struct {
	link          func(string, string) error
	remove        func(string) error
	syncDirectory func(string) error
}

// publishNewFile durably publishes a temporary file without overwriting an existing destination.
//
// Publication commits when the directory sync succeeds after the link.
func publishNewFile(temporary, destination string, ops publishFileOps) error {
	if ops.link == nil || ops.remove == nil || ops.syncDirectory == nil {
		return ErrCorrupt
	}
	if err := ops.link(temporary, destination); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return ErrDestinationExists
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

// newSnapshotTransactionID creates a random identifier for a snapshot write transaction.
func newSnapshotTransactionID() ([16]byte, error) {
	var id [16]byte
	_, err := rand.Read(id[:])
	return id, err
}

// sameFile reports whether two existing paths refer to the same file.
func sameFile(left, right string) bool {
	leftInfo, leftErr := os.Stat(left)
	rightInfo, rightErr := os.Stat(right)
	return leftErr == nil && rightErr == nil && os.SameFile(leftInfo, rightInfo)
}
