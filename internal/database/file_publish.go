package database

import (
	"crypto/rand"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	snapshotDocumentBatchCount = 1024
	snapshotDocumentBatchBytes = 8 * 1024 * 1024
)

type publishFileOps struct {
	link          func(string, string) error
	remove        func(string) error
	syncDirectory func(string) error
}

// publishNewFile has one commit point: a successful directory sync after the
// no-overwrite link. It is shared by backups, restores, and compaction.
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

func newSnapshotTransactionID() ([16]byte, error) {
	var id [16]byte
	_, err := rand.Read(id[:])
	return id, err
}

func sameFile(left, right string) bool {
	leftInfo, leftErr := os.Stat(left)
	rightInfo, rightErr := os.Stat(right)
	return leftErr == nil && rightErr == nil && os.SameFile(leftInfo, rightInfo)
}
