package database

import (
	"context"
	"os"
)

// durabilityBackend persists commits and manages the database storage lifecycle.
type durabilityBackend interface {
	appendDBCommit(ctx context.Context, db *DB, token uint64, changes []Change) error
	syncDB(db *DB) error
	closeDB(db *DB) error
}

// storageStatsBackend exposes storage health through the private durability bridge.
type storageStatsBackend interface {
	storageDBStats() StorageStats
}

// Sync confirms that the database's current state is durable.
//
// Successful writes are already synced, so this is normally a cheap health check.
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

// appendCommit persists a write through the durability backend and records its diagnostic outcome.
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

// syncDirectory flushes directory metadata to durable storage.
func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
