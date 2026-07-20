package meldbase

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const rollbackAnchorRecordVersion uint32 = 2
const rollbackAnchorMaximumBytes int64 = 4096

type fileRollbackAnchorStore struct {
	path string
	gate chan struct{}
}

type rollbackAnchorRecord struct {
	Version               uint32 `json:"version"`
	DatabaseID            string `json:"databaseId"`
	MinimumCommitSequence uint64 `json:"minimumCommitSequence"`
	MinimumGeneration     uint64 `json:"minimumGeneration"`
	Checksum              string `json:"checksum"`
}

// NewFileRollbackAnchorStore returns a fail-closed, atomically replaced anchor
// file. The parent directory must already exist. For rollback protection, that
// directory must be backed by storage trusted independently from the database.
func NewFileRollbackAnchorStore(path string) (RollbackAnchorStore, error) {
	if path == "" {
		return nil, errors.New("meldbase: empty rollback anchor path")
	}
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	if filepath.Base(absolute) == "." || filepath.Base(absolute) == string(filepath.Separator) {
		return nil, errors.New("meldbase: invalid rollback anchor path")
	}
	info, err := os.Stat(filepath.Dir(absolute))
	if err != nil || !info.IsDir() {
		return nil, errors.Join(err, errors.New("meldbase: rollback anchor parent directory must exist"))
	}
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return &fileRollbackAnchorStore{path: absolute, gate: gate}, nil
}

func (store *fileRollbackAnchorStore) Load(ctx context.Context) (RollbackAnchor, bool, error) {
	if store == nil || store.path == "" || ctx == nil {
		return RollbackAnchor{}, false, ErrRollbackAnchor
	}
	if err := store.acquire(ctx); err != nil {
		return RollbackAnchor{}, false, err
	}
	defer store.release()
	return store.loadUnlocked(ctx)
}

func (store *fileRollbackAnchorStore) loadUnlocked(ctx context.Context) (RollbackAnchor, bool, error) {
	if err := ctx.Err(); err != nil {
		return RollbackAnchor{}, false, err
	}
	info, err := os.Lstat(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return RollbackAnchor{}, false, nil
	}
	if err != nil {
		return RollbackAnchor{}, false, fmt.Errorf("%w: %v", ErrRollbackAnchor, err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > rollbackAnchorMaximumBytes {
		return RollbackAnchor{}, false, fmt.Errorf("%w: anchor is not a bounded regular file", ErrRollbackAnchor)
	}
	file, err := os.Open(store.path)
	if err != nil {
		return RollbackAnchor{}, false, fmt.Errorf("%w: %v", ErrRollbackAnchor, err)
	}
	defer file.Close()
	if err := ctx.Err(); err != nil {
		return RollbackAnchor{}, false, err
	}
	decoder := json.NewDecoder(io.LimitReader(file, rollbackAnchorMaximumBytes+1))
	decoder.DisallowUnknownFields()
	var record rollbackAnchorRecord
	if err := decoder.Decode(&record); err != nil {
		return RollbackAnchor{}, false, fmt.Errorf("%w: invalid record: %v", ErrRollbackAnchor, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return RollbackAnchor{}, false, fmt.Errorf("%w: trailing record data", ErrRollbackAnchor)
	}
	anchor, err := decodeRollbackAnchorRecord(record)
	if err != nil {
		return RollbackAnchor{}, false, err
	}
	return anchor, true, nil
}

func (store *fileRollbackAnchorStore) Advance(ctx context.Context, anchor RollbackAnchor) error {
	if store == nil || store.path == "" || ctx == nil || !validRollbackAnchor(anchor) {
		return ErrRollbackAnchor
	}
	if err := store.acquire(ctx); err != nil {
		return err
	}
	defer store.release()

	lock, err := os.OpenFile(store.path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("%w: acquire update lock: %v", ErrRollbackAnchor, err)
	}
	defer lock.Close()
	info, err := lock.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("%w: invalid update lock", ErrRollbackAnchor)
	}
	for {
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return fmt.Errorf("%w: acquire update lock: %v", ErrRollbackAnchor, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)

	current, exists, err := store.loadUnlocked(ctx)
	if err != nil {
		return err
	}
	if exists && current.DatabaseID != anchor.DatabaseID {
		return fmt.Errorf("%w: identity change", ErrRollbackAnchor)
	}
	if exists && current.MinimumCommitSequence > anchor.MinimumCommitSequence {
		return fmt.Errorf("%w: sequence regression", ErrRollbackAnchor)
	}
	if exists && current.MinimumGeneration > anchor.MinimumGeneration {
		return fmt.Errorf("%w: generation regression", ErrRollbackAnchor)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	record := encodeRollbackAnchorRecord(anchor)
	raw, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	directory := filepath.Dir(store.path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(store.path)+".tmp-")
	if err != nil {
		return fmt.Errorf("%w: %v", ErrRollbackAnchor, err)
	}
	temporaryPath := temporary.Name()
	cleanup := func() { _ = temporary.Close(); _ = os.Remove(temporaryPath) }
	if err := temporary.Chmod(0o600); err != nil {
		cleanup()
		return fmt.Errorf("%w: %v", ErrRollbackAnchor, err)
	}
	written, writeErr := temporary.Write(raw)
	if writeErr == nil && written != len(raw) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = ctx.Err()
	}
	writeErr = errors.Join(writeErr, temporary.Sync(), temporary.Close())
	if writeErr != nil {
		cleanup()
		return fmt.Errorf("%w: %v", ErrRollbackAnchor, writeErr)
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		cleanup()
		return fmt.Errorf("%w: %v", ErrRollbackAnchor, err)
	}
	if err := syncDirectory(directory); err != nil {
		return fmt.Errorf("%w: %v", ErrRollbackAnchor, err)
	}
	return nil
}

func (store *fileRollbackAnchorStore) acquire(ctx context.Context) error {
	if store == nil || store.gate == nil || ctx == nil {
		return ErrRollbackAnchor
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-store.gate:
		return nil
	}
}

func (store *fileRollbackAnchorStore) release() {
	store.gate <- struct{}{}
}

func (*fileRollbackAnchorStore) RollbackAnchorStatus() RollbackAnchorStoreStatus {
	return RollbackAnchorStoreStatus{Replicas: 1, Quorum: 1}
}

func encodeRollbackAnchorRecord(anchor RollbackAnchor) rollbackAnchorRecord {
	return rollbackAnchorRecord{Version: rollbackAnchorRecordVersion, DatabaseID: hex.EncodeToString(anchor.DatabaseID[:]), MinimumCommitSequence: anchor.MinimumCommitSequence, MinimumGeneration: anchor.MinimumGeneration, Checksum: rollbackAnchorChecksum(anchor)}
}

func decodeRollbackAnchorRecord(record rollbackAnchorRecord) (RollbackAnchor, error) {
	if record.Version != rollbackAnchorRecordVersion || len(record.DatabaseID) != 32 || len(record.Checksum) != 64 {
		return RollbackAnchor{}, fmt.Errorf("%w: invalid record identity", ErrRollbackAnchor)
	}
	databaseID, err := hex.DecodeString(record.DatabaseID)
	if err != nil || len(databaseID) != 16 {
		return RollbackAnchor{}, fmt.Errorf("%w: invalid database identity", ErrRollbackAnchor)
	}
	anchor := RollbackAnchor{MinimumCommitSequence: record.MinimumCommitSequence, MinimumGeneration: record.MinimumGeneration}
	copy(anchor.DatabaseID[:], databaseID)
	if !validRollbackAnchor(anchor) || record.Checksum != rollbackAnchorChecksum(anchor) {
		return RollbackAnchor{}, fmt.Errorf("%w: checksum mismatch", ErrRollbackAnchor)
	}
	return anchor, nil
}

func rollbackAnchorChecksum(anchor RollbackAnchor) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("meldbase-rollback-anchor-store\x00"))
	_, _ = hash.Write(anchor.DatabaseID[:])
	var sequence [8]byte
	binary.BigEndian.PutUint64(sequence[:], anchor.MinimumCommitSequence)
	_, _ = hash.Write(sequence[:])
	binary.BigEndian.PutUint64(sequence[:], anchor.MinimumGeneration)
	_, _ = hash.Write(sequence[:])
	return hex.EncodeToString(hash.Sum(nil))
}

func zeroDatabaseID(value [16]byte) bool {
	var zero [16]byte
	return value == zero
}

func validRollbackAnchor(anchor RollbackAnchor) bool {
	// Commit sequence counts logical operations, while generation counts physical
	// Meta publications. A group commit can advance several logical sequences
	// in one generation, so they are independent monotonic coordinates rather
	// than a fixed-offset pair.
	return !zeroDatabaseID(anchor.DatabaseID) && anchor.MinimumGeneration > 0
}

func rollbackProtectionConfigured(protection RollbackProtection) bool {
	return !zeroDatabaseID(protection.ExpectedDatabaseID) || protection.MinimumCommitSequence > 0 || protection.MinimumGeneration > 0 || protection.AnchorStore != nil || protection.InitializeAnchor || protection.OperationTimeout != 0
}

func persistRollbackAnchor(ctx context.Context, store RollbackAnchorStore, anchor RollbackAnchor) (RollbackAnchor, error) {
	if ctx == nil || store == nil || !validRollbackAnchor(anchor) {
		return RollbackAnchor{}, ErrRollbackAnchor
	}
	if err := store.Advance(ctx, anchor); err != nil {
		return RollbackAnchor{}, fmt.Errorf("%w: %w", ErrRollbackAnchor, err)
	}
	retained, exists, err := store.Load(ctx)
	if err != nil || !exists || retained.DatabaseID != anchor.DatabaseID || retained.MinimumCommitSequence < anchor.MinimumCommitSequence || retained.MinimumGeneration < anchor.MinimumGeneration {
		return RollbackAnchor{}, errors.Join(fmt.Errorf("%w: advanced anchor did not read back monotonically", ErrRollbackAnchor), err)
	}
	return retained, nil
}
