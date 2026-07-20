package meldbase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

// PhysicalBackupImportOptions bounds an untrusted physical-backup stream
// before it can consume local disk. Zero selects the normal V2 file limit.
// Deployments with a deliberately larger V2 database must set MaxBytes
// explicitly on the receiving side; a sender never chooses that authority.
type PhysicalBackupImportOptions struct {
	MaxBytes uint64
}

// ImportV2PhysicalBackup receives one exact BackupV2 artifact into a new local
// path. It writes a private temporary file, checks the claimed byte count and
// SHA-256 while streaming, runs the complete offline V2 graph/index verifier,
// then publishes with the same no-overwrite link-and-directory-sync commit
// point as backup and migration.
//
// source is intentionally transport-neutral. A WebSocket, HTTP response, QUIC
// stream, or removable-media reader may supply it, but transport cancellation
// must close or honor ctx itself: a generic io.Reader cannot be interrupted
// while blocked in Read. The destination is never opened as a writable DB by
// this function; callers normally open the successfully imported file through
// OpenV2Follower before applying a replication tail.
func ImportV2PhysicalBackup(ctx context.Context, source io.Reader, destination string, expected BackupV2Result, options PhysicalBackupImportOptions) (BackupV2Result, error) {
	if source == nil {
		return BackupV2Result{}, ErrCorrupt
	}
	if err := contextError(ctx); err != nil {
		return BackupV2Result{}, err
	}
	if destination == "" {
		return BackupV2Result{}, errors.New("meldbase: empty backup destination")
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = DefaultV2MaxFileBytes
	}
	if err := validatePhysicalBackupReceipt(expected, options.MaxBytes); err != nil {
		return BackupV2Result{}, err
	}
	destination, err := filepath.Abs(filepath.Clean(destination))
	if err != nil {
		return BackupV2Result{}, err
	}
	if _, err := os.Lstat(destination); err == nil {
		return BackupV2Result{}, ErrBackupDestinationExists
	} else if !errors.Is(err, fs.ErrNotExist) {
		return BackupV2Result{}, err
	}
	temporary, err := os.CreateTemp(filepath.Dir(destination), "."+filepath.Base(destination)+".import-*")
	if err != nil {
		return BackupV2Result{}, err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := copyAndVerifyPhysicalBackup(ctx, temporary, source, expected); err != nil {
		_ = temporary.Close()
		return BackupV2Result{}, err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return BackupV2Result{}, err
	}
	if err := temporary.Close(); err != nil {
		return BackupV2Result{}, err
	}
	verification, err := VerifyV2File(ctx, temporaryPath)
	if err != nil {
		return BackupV2Result{}, err
	}
	if !verification.Verified || verification.FileBytes != expected.Bytes || verification.PhysicalPages != expected.Pages ||
		verification.CommitSequence != expected.CommitSequence || verification.MetaGeneration != expected.MetaGeneration ||
		verification.DatabaseIDHex != expected.DatabaseIDHex || verification.SHA256 != expected.SHA256 ||
		!verification.IndexContentsVerified || !verification.IndexBuildContentsVerified {
		return BackupV2Result{}, ErrCorrupt
	}
	if err := contextError(ctx); err != nil {
		return BackupV2Result{}, err
	}
	if err := publishMigrationFile(temporaryPath, destination, migrationPublishOps{link: os.Link, remove: os.Remove, syncDirectory: syncDirectory}); err != nil {
		if errors.Is(err, ErrMigrationDestinationExists) {
			return BackupV2Result{}, ErrBackupDestinationExists
		}
		return BackupV2Result{}, err
	}
	_ = os.Remove(temporaryPath)
	return expected, nil
}

func validatePhysicalBackupReceipt(expected BackupV2Result, maxBytes uint64) error {
	if maxBytes < storagev2.PageSize || maxBytes%storagev2.PageSize != 0 || expected.Bytes < 2*storagev2.PageSize ||
		expected.Bytes > maxBytes || expected.Bytes%storagev2.PageSize != 0 || expected.Pages != expected.Bytes/storagev2.PageSize ||
		expected.MetaGeneration == 0 || len(expected.DatabaseIDHex) != 32 || stringsLower(expected.DatabaseIDHex) != expected.DatabaseIDHex ||
		len(expected.SHA256) != sha256.Size*2 || stringsLower(expected.SHA256) != expected.SHA256 {
		return ErrCorrupt
	}
	identity, err := hex.DecodeString(expected.DatabaseIDHex)
	if err != nil || len(identity) != 16 || allZeroBytes(identity) {
		return ErrCorrupt
	}
	digest, err := hex.DecodeString(expected.SHA256)
	if err != nil || len(digest) != sha256.Size {
		return ErrCorrupt
	}
	return nil
}

func copyAndVerifyPhysicalBackup(ctx context.Context, destination *os.File, source io.Reader, expected BackupV2Result) error {
	if destination == nil || source == nil {
		return ErrCorrupt
	}
	want, err := hex.DecodeString(expected.SHA256)
	if err != nil || len(want) != sha256.Size {
		return ErrCorrupt
	}
	hash := sha256.New()
	buffer := make([]byte, 1024*1024)
	for written := uint64(0); written < expected.Bytes; {
		if err := contextError(ctx); err != nil {
			return err
		}
		length := min(uint64(len(buffer)), expected.Bytes-written)
		count, err := io.ReadFull(source, buffer[:length])
		if err != nil {
			return fmt.Errorf("%w: physical backup truncated: %v", ErrCorrupt, err)
		}
		if count != len(buffer[:length]) {
			return ErrCorrupt
		}
		if _, err := destination.Write(buffer[:count]); err != nil {
			return err
		}
		_, _ = hash.Write(buffer[:count])
		written += uint64(count)
	}
	var extra [1]byte
	count, err := source.Read(extra[:])
	if count != 0 || (err != nil && !errors.Is(err, io.EOF)) || err == nil {
		return ErrCorrupt
	}
	if !bytes.Equal(hash.Sum(nil), want) {
		return ErrCorrupt
	}
	return nil
}

func stringsLower(value string) string {
	for index := range len(value) {
		if value[index] >= 'A' && value[index] <= 'F' {
			return ""
		}
	}
	return value
}

func allZeroBytes(value []byte) bool {
	var found byte
	for _, item := range value {
		found |= item
	}
	return found == 0
}
