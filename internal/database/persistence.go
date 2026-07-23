package database

import (
	"context"
	"os"
)

// durabilityBackend is the private bridge between the in-memory mutation
// model and the current durable file format.
type durabilityBackend interface {
	appendDBCommit(ctx context.Context, db *DB, token uint64, changes []Change) error
	syncDB(db *DB) error
	closeDB(db *DB) error
}

type storageStatsBackend interface {
	storageDBStats() StorageStats
}

// Sync confirms the current durable state. Every successful write is already
// published and synced, so this is normally a cheap health check.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.fatalErr != nil {
		return db.fatalErr
	}
	if db.durability == nil {
		return nil
	}
	return db.durability.syncDB(db)
}

func (db *DB) appendCommit(ctx context.Context, token uint64, changes []Change) error {
	if db != nil && db.replicaReadOnly {
		return ErrReplicaReadOnly
	}
	if db.durability == nil {
		return nil
	}
	diagnostic := db.beginDiagnostic(DiagnosticCommit)
	if err := contextError(ctx); err != nil {
		db.finishCommitDiagnostic(diagnostic, len(changes), err)
		return err
	}
	err := db.durability.appendDBCommit(ctx, db, token, changes)
	db.finishCommitDiagnostic(diagnostic, len(changes), err)
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
