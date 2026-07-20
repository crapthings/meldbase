package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
)

type PhysicalCopyResult struct {
	Meta   Meta
	Bytes  uint64
	SHA256 [32]byte
}

// CopyPhysicalToContext copies the complete page-aligned file while holding a
// storage read lock. Writers are blocked for the copy duration; readers remain
// available. The caller owns destination creation, fsync, verification and
// publication.
func (f *File) CopyPhysicalToContext(ctx context.Context, destination io.Writer) (PhysicalCopyResult, error) {
	if f == nil || destination == nil {
		return PhysicalCopyResult{}, ErrCorrupt
	}
	if err := contextErr(ctx); err != nil {
		return PhysicalCopyResult{}, err
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return PhysicalCopyResult{}, errors.New("meldbase storage: file is closed")
	}
	if f.fatalErr != nil {
		return PhysicalCopyResult{}, f.fatalErr
	}
	total := f.nextPage * PageSize
	buffer := make([]byte, 1024*1024)
	hash := sha256.New()
	for offset := uint64(0); offset < total; {
		if err := contextErr(ctx); err != nil {
			return PhysicalCopyResult{}, err
		}
		length := min(uint64(len(buffer)), total-offset)
		chunk := buffer[:length]
		if _, err := f.file.ReadAt(chunk, int64(offset)); err != nil {
			return PhysicalCopyResult{}, err
		}
		written, err := destination.Write(chunk)
		if err != nil {
			return PhysicalCopyResult{}, err
		}
		if written != len(chunk) {
			return PhysicalCopyResult{}, io.ErrShortWrite
		}
		_, _ = hash.Write(chunk)
		offset += length
	}
	result := PhysicalCopyResult{Meta: f.meta, Bytes: total}
	copy(result.SHA256[:], hash.Sum(nil))
	return result, nil
}
