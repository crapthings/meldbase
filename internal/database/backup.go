package database

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	storage "github.com/crapthings/meldbase/internal/storage"
)

type BackupResult struct {
	Bytes          uint64 `json:"bytes"`
	Pages          uint64 `json:"pages"`
	CommitSequence uint64 `json:"commitSequence"`
	MetaGeneration uint64 `json:"metaGeneration"`
	DatabaseIDHex  string `json:"databaseIdHex"`
	SHA256         string `json:"sha256"`
}

// Backup writes an exact, verified physical copy to a new path. It preserves
// database identity and Commit Log history, so the result is a restore artifact
// rather than an independent writable fork. The source writer is blocked for
// the copy duration; readers remain available.
func (db *DB) Backup(ctx context.Context, destination string) (result BackupResult, resultErr error) {
	if db == nil {
		return result, ErrBackupUnsupported
	}
	if err := contextError(ctx); err != nil {
		return result, err
	}
	if destination == "" {
		return result, errors.New("meldbase: empty backup destination")
	}
	absoluteDestination, err := filepath.Abs(filepath.Clean(destination))
	if err != nil {
		return result, err
	}
	store, ok := db.durability.(*durableStore)
	if !ok || store == nil || store.file == nil {
		return result, ErrBackupUnsupported
	}
	store.compactMu.Lock()
	defer store.compactMu.Unlock()
	return db.backupWithStoreLocked(ctx, absoluteDestination, store)
}

// backupWithStoreLocked is the physical-copy half shared by ordinary
// backups and archive bootstrap. The caller holds store.compactMu, which also
// makes DB.Close wait until a newly created durable checkpoint has either been
// paired with its verified snapshot or removed on failure.
func (db *DB) backupWithStoreLocked(ctx context.Context, absoluteDestination string, store *durableStore) (result BackupResult, resultErr error) {
	if db == nil || store == nil || store.file == nil || absoluteDestination == "" {
		return result, ErrBackupUnsupported
	}
	db.metrics.backupAttempts.Add(1)
	db.metrics.backupActive.Add(1)
	started := time.Now()
	succeeded := false
	defer func() {
		db.metrics.backupActive.Add(^uint64(0))
		db.metrics.backupLastNanos.Store(uint64(time.Since(started)))
		if !succeeded {
			db.metrics.backupFailed.Add(1)
		}
	}()

	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return result, ErrClosed
	}
	if db.fatalErr != nil {
		return result, db.fatalErr
	}
	if absoluteDestination == store.path {
		return result, ErrBackupDestinationExists
	}
	if _, err := os.Lstat(absoluteDestination); err == nil {
		return result, ErrBackupDestinationExists
	} else if !errors.Is(err, fs.ErrNotExist) {
		return result, err
	}
	directory := filepath.Dir(absoluteDestination)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(absoluteDestination)+".backup-*")
	if err != nil {
		return result, err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	copyResult, copyErr := store.file.CopyPhysicalToContext(ctx, temporary)
	syncErr := temporary.Sync()
	closeErr := temporary.Close()
	if err := errors.Join(copyErr, syncErr, closeErr); err != nil {
		return result, mapStorageError(err)
	}
	if copyResult.Meta.CommitSequence != db.token || copyResult.Meta.DatabaseID != db.databaseID ||
		copyResult.Bytes < copyResult.Meta.PhysicalPageCount*storage.PageSize || copyResult.Bytes%storage.PageSize != 0 {
		return result, ErrCorrupt
	}
	readDigest, err := hashBackupFile(ctx, temporaryPath, copyResult.Bytes)
	if err != nil || readDigest != copyResult.SHA256 {
		return result, errors.Join(ErrCorrupt, err)
	}
	verifiedFile, verifiedMeta, err := storage.Open(temporaryPath)
	if err != nil {
		return result, mapStorageError(err)
	}
	if verifiedMeta != copyResult.Meta {
		_ = verifiedFile.Close()
		return result, ErrCorrupt
	}
	_, auditErr := verifiedFile.ReachabilityContext(ctx)
	auditErr = errors.Join(auditErr, verifiedFile.Close())
	if auditErr != nil {
		return result, mapStorageError(auditErr)
	}
	verified, err := Open(temporaryPath)
	if err != nil {
		return result, err
	}
	if verified.databaseID != db.databaseID || verified.token != db.token {
		_ = verified.Close()
		return result, ErrCorrupt
	}
	if err := verified.Close(); err != nil {
		return result, err
	}
	if err := contextError(ctx); err != nil {
		return result, err
	}
	if err := publishNewFile(temporaryPath, absoluteDestination, publishFileOps{
		link: os.Link, remove: os.Remove, syncDirectory: syncDirectory,
	}); err != nil {
		if errors.Is(err, ErrDestinationExists) {
			return result, ErrBackupDestinationExists
		}
		return result, err
	}
	_ = os.Remove(temporaryPath)
	_ = syncDirectory(directory)
	db.metrics.backupLastBytes.Store(copyResult.Bytes)
	db.metrics.backupCompleted.Add(1)
	succeeded = true
	return BackupResult{
		Bytes: copyResult.Bytes, Pages: copyResult.Bytes / storage.PageSize,
		CommitSequence: copyResult.Meta.CommitSequence, MetaGeneration: copyResult.Meta.Generation,
		DatabaseIDHex: hex.EncodeToString(copyResult.Meta.DatabaseID[:]), SHA256: hex.EncodeToString(copyResult.SHA256[:]),
	}, nil
}

func hashBackupFile(ctx context.Context, path string, expectedBytes uint64) ([32]byte, error) {
	var result [32]byte
	file, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer file.Close()
	hash := sha256.New()
	buffer := make([]byte, 1024*1024)
	var read uint64
	for read < expectedBytes {
		if err := contextError(ctx); err != nil {
			return result, err
		}
		length := min(uint64(len(buffer)), expectedBytes-read)
		count, err := io.ReadFull(file, buffer[:length])
		if err != nil {
			return result, err
		}
		_, _ = hash.Write(buffer[:count])
		read += uint64(count)
	}
	var extra [1]byte
	if count, err := file.Read(extra[:]); err != io.EOF || count != 0 {
		return result, ErrCorrupt
	}
	copy(result[:], hash.Sum(nil))
	return result, nil
}
