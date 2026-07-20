package meldbase

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// ArchiveV2Bootstrap binds an exact verified physical V2 snapshot to the
// durable database change feed that was pinned before that snapshot began.
//
// A receiver must persist and verify Backup, then drain and Ack every batch up
// through SnapshotToken without applying it (the snapshot already contains
// those effects). It can then apply and Ack later batches in order. This avoids
// the bootstrap/tail gap without inventing a second, weaker history contract.
type ArchiveV2Bootstrap struct {
	Backup          BackupV2Result
	CheckpointToken uint64
	SnapshotToken   uint64
}

// BeginV2Archive creates a durable database-wide checkpoint and a verified
// physical snapshot. The checkpoint is established first, so Commit Log
// retention preserves every token needed to bridge from CheckpointToken to the
// returned SnapshotToken, even while writes continue before the snapshot's
// short read barrier begins.
//
// This is deliberately a transport-neutral bootstrap primitive. It does not
// copy files to another machine, open a writable follower, or acknowledge a
// token on the caller's behalf. Those actions have different ownership and
// failure domains and must be explicit in the eventual wire protocol.
func (db *DB) BeginV2Archive(ctx context.Context, name, destination string, buffer int) (ArchiveV2Bootstrap, *DurableDatabaseChangeSubscription, error) {
	if db == nil {
		return ArchiveV2Bootstrap{}, nil, ErrBackupUnsupported
	}
	if !validPublicDurableConsumerName(name) || buffer <= 0 || buffer > 1024 {
		return ArchiveV2Bootstrap{}, nil, ErrInvalidDocument
	}
	if err := contextError(ctx); err != nil {
		return ArchiveV2Bootstrap{}, nil, err
	}
	if destination == "" {
		return ArchiveV2Bootstrap{}, nil, errors.New("meldbase: empty backup destination")
	}
	absoluteDestination, err := filepath.Abs(filepath.Clean(destination))
	if err != nil {
		return ArchiveV2Bootstrap{}, nil, err
	}
	store, ok := db.durability.(*v2DurableStore)
	if !ok || store == nil || store.file == nil {
		return ArchiveV2Bootstrap{}, nil, ErrBackupUnsupported
	}
	store.compactMu.Lock()
	defer store.compactMu.Unlock()

	// Check the destination before a zero-document database needs its private
	// System initialization generation. The backup path repeats this check under
	// db.mu because another process may race us after this point.
	if absoluteDestination == store.path {
		return ArchiveV2Bootstrap{}, nil, ErrBackupDestinationExists
	}
	if _, err := os.Lstat(absoluteDestination); err == nil {
		return ArchiveV2Bootstrap{}, nil, ErrBackupDestinationExists
	} else if !errors.Is(err, fs.ErrNotExist) {
		return ArchiveV2Bootstrap{}, nil, err
	}

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ArchiveV2Bootstrap{}, nil, ErrClosed
	}
	if db.fatalErr != nil {
		err := db.fatalErr
		db.mu.Unlock()
		return ArchiveV2Bootstrap{}, nil, err
	}
	checkpoint := db.token
	// On an empty source, creating the bootstrap consumer creates the private
	// System tree at sequence one. Guard that history-changing control commit
	// before opening a tail for a potentially revoked primary.
	if checkpoint == 0 {
		if err := db.validateV2PrimaryWriteFence(1); err != nil {
			db.mu.Unlock()
			return ArchiveV2Bootstrap{}, nil, err
		}
	}
	consumer, err := store.file.CreateDurableCommitConsumer(durableDatabaseConsumerKey(name), checkpoint)
	if err == nil {
		if sequence := store.file.Meta().CommitSequence; sequence != db.token {
			if sequence < db.token {
				err = ErrCorrupt
			} else {
				db.token = sequence
			}
		}
		checkpoint = db.token
	}
	db.mu.Unlock()
	if err != nil {
		if consumer != nil {
			_ = consumer.Close()
		}
		return ArchiveV2Bootstrap{}, nil, mapDurableConsumerError(err)
	}

	backup, backupErr := db.backupV2WithStoreLocked(ctx, absoluteDestination, store)
	if backupErr != nil {
		closeErr := consumer.Close()
		deleteErr := store.file.DeleteDurableCommitConsumer(durableDatabaseConsumerKey(name))
		return ArchiveV2Bootstrap{}, nil, errors.Join(backupErr, mapDurableConsumerError(closeErr), mapDurableConsumerError(deleteErr))
	}
	if backup.CommitSequence < checkpoint || backup.DatabaseIDHex == "" {
		_ = consumer.Close()
		_ = store.file.DeleteDurableCommitConsumer(durableDatabaseConsumerKey(name))
		return ArchiveV2Bootstrap{}, nil, ErrCorrupt
	}
	subscription, err := newDurableDatabaseChangeSubscription(ctx, store, consumer, buffer)
	if err != nil {
		_ = store.file.DeleteDurableCommitConsumer(durableDatabaseConsumerKey(name))
		return ArchiveV2Bootstrap{}, nil, err
	}
	return ArchiveV2Bootstrap{Backup: backup, CheckpointToken: checkpoint, SnapshotToken: backup.CommitSequence}, subscription, nil
}
