package primarylease

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	fileRecordVersion      uint32 = 1
	fileRecordMaximumBytes int64  = 4096
)

type fileRecord struct {
	Version        uint32 `json:"version"`
	DatabaseID     string `json:"databaseId"`
	Owner          string `json:"owner"`
	Epoch          uint64 `json:"epoch"`
	CommitSequence uint64 `json:"commitSequence"`
	NotAfterMS     int64  `json:"notAfterMs"`
	Revoked        bool   `json:"revoked"`
	Checksum       string `json:"checksum"`
}

// FileStore is one durable, single-member LeaseStore rooted in a trusted
// existing directory. It is suitable as the local state of one independently
// operated quorum member, not as a quorum or leader-election service by
// itself. Each database identity has a separate atomically replaced record.
//
// The directory must be on storage appropriate for the controller's
// durability contract. FileStore writes use a per-record advisory flock,
// durable temporary file, atomic rename and directory fsync. A checksum makes
// torn, malformed or substituted record data fail closed.
type FileStore struct {
	directory string
	gate      sync.Mutex
}

// NewFileStore opens an existing controller-state directory. It never creates
// the directory implicitly, so deployment ownership and permissions remain
// explicit.
func NewFileStore(directory string) (*FileStore, error) {
	if directory == "" {
		return nil, ErrLeaseStore
	}
	abs, err := filepath.Abs(filepath.Clean(directory))
	if err != nil {
		return nil, errors.Join(ErrLeaseStore, err)
	}
	info, err := os.Stat(abs)
	if err != nil || !info.IsDir() {
		return nil, errors.Join(ErrLeaseStore, err)
	}
	return &FileStore{directory: abs}, nil
}

func (store *FileStore) LoadPrimaryLease(ctx context.Context, databaseID [16]byte) (LeaseRecord, bool, error) {
	if store == nil || store.directory == "" || ctx == nil || databaseID == [16]byte{} {
		return LeaseRecord{}, false, ErrLeaseStore
	}
	if err := ctx.Err(); err != nil {
		return LeaseRecord{}, false, err
	}
	store.gate.Lock()
	defer store.gate.Unlock()
	return store.loadUnlocked(ctx, databaseID)
}

func (store *FileStore) CompareAndSwapPrimaryLease(ctx context.Context, databaseID [16]byte, previous *LeaseRecord, next LeaseRecord) (bool, error) {
	if store == nil || store.directory == "" || ctx == nil || databaseID == [16]byte{} || !validLeaseRecord(next, databaseID) || (previous != nil && !validLeaseRecord(*previous, databaseID)) {
		return false, ErrLeaseStore
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	store.gate.Lock()
	defer store.gate.Unlock()
	path := store.recordPath(databaseID)
	lock, err := store.openAndLock(ctx, path+".lock")
	if err != nil {
		return false, err
	}
	defer func() {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
	}()
	current, exists, err := store.loadUnlocked(ctx, databaseID)
	if err != nil {
		return false, err
	}
	if previous == nil {
		if exists {
			return false, nil
		}
	} else if !exists || !leaseRecordEqual(current, *previous) {
		return false, nil
	}
	if err := store.persistUnlocked(ctx, path, next); err != nil {
		return false, err
	}
	return true, nil
}

func (store *FileStore) loadUnlocked(ctx context.Context, databaseID [16]byte) (LeaseRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return LeaseRecord{}, false, err
	}
	path := store.recordPath(databaseID)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return LeaseRecord{}, false, nil
	}
	if err != nil {
		return LeaseRecord{}, false, errors.Join(ErrLeaseStore, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() <= 0 || info.Size() > fileRecordMaximumBytes {
		return LeaseRecord{}, false, ErrLeaseStore
	}
	file, err := os.Open(path)
	if err != nil {
		return LeaseRecord{}, false, errors.Join(ErrLeaseStore, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, fileRecordMaximumBytes+1))
	decoder.DisallowUnknownFields()
	var wire fileRecord
	if err := decoder.Decode(&wire); err != nil {
		return LeaseRecord{}, false, ErrLeaseStore
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return LeaseRecord{}, false, ErrLeaseStore
	}
	record, err := decodeFileRecord(wire)
	if err != nil || record.DatabaseID != databaseID {
		return LeaseRecord{}, false, ErrLeaseStore
	}
	return record, true, nil
}

func (store *FileStore) persistUnlocked(ctx context.Context, path string, record LeaseRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(encodeFileRecord(record))
	if err != nil {
		return errors.Join(ErrLeaseStore, err)
	}
	raw = append(raw, '\n')
	temporary, err := os.CreateTemp(store.directory, "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return errors.Join(ErrLeaseStore, err)
	}
	temporaryPath := temporary.Name()
	cleanup := func() { _ = temporary.Close(); _ = os.Remove(temporaryPath) }
	if err := temporary.Chmod(0o600); err != nil {
		cleanup()
		return errors.Join(ErrLeaseStore, err)
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
		return errors.Join(ErrLeaseStore, writeErr)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		cleanup()
		return errors.Join(ErrLeaseStore, err)
	}
	if err := syncLeaseDirectory(store.directory); err != nil {
		return errors.Join(ErrLeaseStore, err)
	}
	return nil
}

func (store *FileStore) openAndLock(ctx context.Context, path string) (*os.File, error) {
	lock, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, errors.Join(ErrLeaseStore, err)
	}
	info, err := lock.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		_ = lock.Close()
		return nil, ErrLeaseStore
	}
	for {
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return lock, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = lock.Close()
			return nil, errors.Join(ErrLeaseStore, err)
		}
		select {
		case <-ctx.Done():
			_ = lock.Close()
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (store *FileStore) recordPath(databaseID [16]byte) string {
	return filepath.Join(store.directory, hex.EncodeToString(databaseID[:])+".lease")
}

func encodeFileRecord(record LeaseRecord) fileRecord {
	return fileRecord{
		Version: fileRecordVersion, DatabaseID: hex.EncodeToString(record.DatabaseID[:]), Owner: record.Owner,
		Epoch: record.Epoch, CommitSequence: record.CommitSequence, NotAfterMS: record.NotAfter.UTC().UnixMilli(),
		Revoked: record.Revoked, Checksum: fileRecordChecksum(record),
	}
}

func decodeFileRecord(wire fileRecord) (LeaseRecord, error) {
	if wire.Version != fileRecordVersion || len(wire.DatabaseID) != 32 || wire.DatabaseID != strings.ToLower(wire.DatabaseID) || len(wire.Checksum) != sha256.Size*2 {
		return LeaseRecord{}, ErrLeaseStore
	}
	encodedID, err := hex.DecodeString(wire.DatabaseID)
	if err != nil || len(encodedID) != 16 {
		return LeaseRecord{}, ErrLeaseStore
	}
	var record LeaseRecord
	copy(record.DatabaseID[:], encodedID)
	record.Owner, record.Epoch, record.CommitSequence = wire.Owner, wire.Epoch, wire.CommitSequence
	record.NotAfter, record.Revoked = time.UnixMilli(wire.NotAfterMS).UTC(), wire.Revoked
	if !validLeaseRecord(record, record.DatabaseID) || wire.Checksum != fileRecordChecksum(record) {
		return LeaseRecord{}, ErrLeaseStore
	}
	return record, nil
}

func fileRecordChecksum(record LeaseRecord) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("meldbase-primary-lease-file-v1\x00"))
	_, _ = hash.Write(record.DatabaseID[:])
	_, _ = hash.Write([]byte{byte(len(record.Owner))})
	_, _ = hash.Write([]byte(record.Owner))
	var integer [8]byte
	binary.BigEndian.PutUint64(integer[:], record.Epoch)
	_, _ = hash.Write(integer[:])
	binary.BigEndian.PutUint64(integer[:], record.CommitSequence)
	_, _ = hash.Write(integer[:])
	binary.BigEndian.PutUint64(integer[:], uint64(record.NotAfter.UTC().UnixMilli()))
	_, _ = hash.Write(integer[:])
	if record.Revoked {
		_, _ = hash.Write([]byte{1})
	} else {
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func syncLeaseDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

var _ LeaseStore = (*FileStore)(nil)

func (store *FileStore) String() string {
	if store == nil {
		return "primarylease.FileStore<nil>"
	}
	return "primarylease.FileStore"
}
