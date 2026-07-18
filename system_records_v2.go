package meldbase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/crapthings/meldbase/internal/storage/v2"
	"github.com/crapthings/meldbase/internal/systemrecord"
)

type dbSystemRecordBackend struct{ db *DB }

// MeldbaseSystemRecordBackend is internal plumbing for first-party packages
// such as server. Its return type lives under internal/, so it is deliberately
// unavailable as an application-facing generic key/value API.
func (db *DB) MeldbaseSystemRecordBackend() systemrecord.Backend {
	if db == nil {
		return nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if _, ok := db.durability.(*v2DurableStore); !ok || db.closed {
		return nil
	}
	return &dbSystemRecordBackend{db: db}
}

func (backend *dbSystemRecordBackend) CompareAndSwap(ctx context.Context, mutation systemrecord.Mutation) (systemrecord.Result, error) {
	if backend == nil || backend.db == nil || ctx == nil {
		return systemrecord.Result{}, ErrClosed
	}
	if err := contextError(ctx); err != nil {
		return systemrecord.Result{}, err
	}
	db := backend.db
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return systemrecord.Result{}, ErrClosed
	}
	if db.fatalErr != nil {
		err := db.fatalErr
		db.mu.Unlock()
		return systemrecord.Result{}, err
	}
	store, ok := db.durability.(*v2DurableStore)
	if !ok || store.file == nil {
		db.mu.Unlock()
		return systemrecord.Result{}, errors.New("meldbase: durable system records require storage V2")
	}
	db.metrics.v2CommitAttempts.Add(1)
	started := time.Now()
	result, err := store.file.ApplySystemRecordTransaction(v2.SystemRecordTransaction{
		TransactionID: mutation.TransactionID, Key: append([]byte(nil), mutation.Key...),
		ExpectedExists: mutation.ExpectedExists, ExpectedHash: mutation.ExpectedHash,
		NewValue: append([]byte(nil), mutation.NewValue...), Delete: mutation.Delete, Unconditional: mutation.Unconditional,
	})
	if err != nil {
		db.metrics.v2RejectedTransactions.Add(1)
		mapped := mapStorageV2Error(err)
		db.fatalErr = fmt.Errorf("%w: %v", ErrDurability, mapped)
		db.mu.Unlock()
		return systemrecord.Result{}, db.fatalErr
	}
	if !result.Applied {
		db.metrics.v2RejectedTransactions.Add(1)
		db.mu.Unlock()
		return systemrecord.Result{Current: append([]byte(nil), result.Current...)}, nil
	}
	want := db.token + 1
	if want == 0 || result.Sequence != want {
		db.metrics.v2RejectedTransactions.Add(1)
		db.fatalErr = fmt.Errorf("%w: V2 system commit sequence mismatch", ErrDurability)
		db.mu.Unlock()
		return systemrecord.Result{}, db.fatalErr
	}
	if store.rollbackAnchor != nil {
		if anchorErr := store.advanceRollbackAnchor(ctx, result.Sequence); anchorErr != nil {
			db.metrics.v2RejectedTransactions.Add(1)
			db.fatalErr = fmt.Errorf("%w: committed system sequence %d but %w", ErrDurability, result.Sequence, anchorErr)
			db.mu.Unlock()
			return systemrecord.Result{}, db.fatalErr
		}
	}
	db.token = result.Sequence
	elapsed := uint64(time.Since(started))
	db.metrics.v2CommittedTransactions.Add(1)
	db.metrics.v2CommitNanos.Add(elapsed)
	updateAtomicMax(&db.metrics.v2CommitMaxNanos, elapsed)
	batch := ChangeBatch{Token: result.Sequence}
	db.publish(batch)
	db.mu.Unlock()
	return systemrecord.Result{Applied: true}, nil
}

func (backend *dbSystemRecordBackend) Scan(ctx context.Context, start, end []byte, limit int) ([]systemrecord.KeyValue, error) {
	if backend == nil || backend.db == nil || ctx == nil {
		return nil, ErrClosed
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	db := backend.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	if db.fatalErr != nil {
		return nil, db.fatalErr
	}
	store, ok := db.durability.(*v2DurableStore)
	if !ok || store.file == nil {
		return nil, errors.New("meldbase: durable system records require storage V2")
	}
	snapshot, err := store.file.OpenSnapshot()
	if err != nil {
		return nil, mapStorageV2Error(err)
	}
	defer snapshot.Close()
	records, err := snapshot.ScanSystemRecords(start, end, limit)
	if err != nil {
		return nil, mapStorageV2Error(err)
	}
	result := make([]systemrecord.KeyValue, len(records))
	for index, record := range records {
		result[index] = systemrecord.KeyValue{Key: record.Key, Value: record.Value}
	}
	return result, nil
}
