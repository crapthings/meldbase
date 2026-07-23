package database

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

const (
	logicalArchiveFormat         = "meldbase-logical-archive"
	logicalArchiveVersion        = 1
	logicalArchiveMaxRecordBytes = 64 << 20
)

// LogicalArchiveResult is the portable, data-only archive receipt. SHA256
// covers every JSONL record before the final end record; the end record stores
// the same digest so an importer can reject truncation or alteration.
type LogicalArchiveResult struct {
	Format      string `json:"format"`
	Version     int    `json:"version"`
	Bytes       uint64 `json:"bytes"`
	Collections uint64 `json:"collections"`
	Documents   uint64 `json:"documents"`
	Indexes     uint64 `json:"indexes"`
	SHA256      string `json:"sha256"`
}

// LogicalArchiveImportOptions bounds an untrusted logical archive. Zero
// MaxBytes selects the normal storage-file limit; the receiver owns this cap.
type LogicalArchiveImportOptions struct{ MaxBytes uint64 }

type logicalArchiveHeader struct {
	Kind    string `json:"kind"`
	Format  string `json:"format"`
	Version int    `json:"version"`
}
type logicalArchiveCollectionRecord struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}
type logicalArchiveDocumentRecord struct {
	Kind       string          `json:"kind"`
	Collection string          `json:"collection"`
	Document   json.RawMessage `json:"document"`
}
type logicalArchiveIndexRecord struct {
	Kind       string       `json:"kind"`
	Collection string       `json:"collection"`
	Name       string       `json:"name"`
	Fields     []IndexField `json:"fields"`
	Unique     bool         `json:"unique"`
}
type logicalArchiveEndRecord struct {
	Kind        string `json:"kind"`
	Collections uint64 `json:"collections"`
	Documents   uint64 `json:"documents"`
	Indexes     uint64 `json:"indexes"`
	SHA256      string `json:"sha256"`
}

// ExportLogicalArchive writes a versioned JSON Lines snapshot that contains
// collections, typed documents and index definitions, but no pages, database
// identity or commit history. The source is pinned at one durable snapshot;
// writes committed after that point are deliberately absent.
func (db *DB) ExportLogicalArchive(ctx context.Context, destination string) (result LogicalArchiveResult, resultErr error) {
	if db == nil {
		return result, ErrLogicalArchiveUnsupported
	}
	if err := contextError(ctx); err != nil {
		return result, err
	}
	if destination == "" {
		return result, errors.New("meldbase: empty logical archive destination")
	}
	destination, err := filepath.Abs(filepath.Clean(destination))
	if err != nil {
		return result, err
	}
	store, ok := db.durability.(*durableStore)
	if !ok || store == nil || store.file == nil {
		return result, ErrLogicalArchiveUnsupported
	}
	store.compactMu.Lock()
	defer store.compactMu.Unlock()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return result, ErrClosed
	}
	if db.fatalErr != nil {
		err := db.fatalErr
		db.mu.RUnlock()
		return result, err
	}
	if destination == store.path {
		db.mu.RUnlock()
		return result, ErrLogicalArchiveDestinationExists
	}
	if _, err := os.Lstat(destination); err == nil {
		db.mu.RUnlock()
		return result, ErrLogicalArchiveDestinationExists
	} else if !errors.Is(err, fs.ErrNotExist) {
		db.mu.RUnlock()
		return result, err
	}
	snapshot, err := store.file.OpenSnapshot()
	if err == nil && snapshot.Sequence() != db.token {
		err = ErrCorrupt
	}
	db.mu.RUnlock()
	if err != nil {
		return result, mapStorageError(err)
	}
	defer func() {
		if closeErr := mapStorageError(snapshot.Close()); resultErr == nil && closeErr != nil {
			resultErr = closeErr
		}
	}()

	temporary, err := os.CreateTemp(filepath.Dir(destination), "."+filepath.Base(destination)+".logical-export-*")
	if err != nil {
		return result, err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	writer := newLogicalArchiveWriter(temporary)
	if err := writer.write(ctx, logicalArchiveHeader{Kind: "header", Format: logicalArchiveFormat, Version: logicalArchiveVersion}, true); err != nil {
		_ = temporary.Close()
		return result, err
	}
	collections, err := snapshot.Collections()
	if err != nil {
		_ = temporary.Close()
		return result, mapStorageError(err)
	}
	sort.Slice(collections, func(left, right int) bool { return collections[left].Name < collections[right].Name })
	for _, collection := range collections {
		if err := writer.write(ctx, logicalArchiveCollectionRecord{Kind: "collection", Name: collection.Name}, true); err != nil {
			_ = temporary.Close()
			return result, err
		}
		result.Collections++
	}
	for _, collection := range collections {
		iterator, err := snapshot.OpenInsertionOrderIterator(collection.Name, nil, nil, 0)
		if err != nil {
			_ = temporary.Close()
			return result, mapStorageError(err)
		}
		for iterator.Next() {
			if err := contextError(ctx); err != nil {
				_ = iterator.Close()
				_ = temporary.Close()
				return result, err
			}
			document, err := decodeStoredDocument(iterator.Record().Document)
			if err != nil {
				_ = iterator.Close()
				_ = temporary.Close()
				return result, ErrCorrupt
			}
			encoded, err := MarshalWireDocument(document)
			if err != nil {
				_ = iterator.Close()
				_ = temporary.Close()
				return result, err
			}
			if err := writer.write(ctx, logicalArchiveDocumentRecord{Kind: "document", Collection: collection.Name, Document: encoded}, true); err != nil {
				_ = iterator.Close()
				_ = temporary.Close()
				return result, err
			}
			result.Documents++
		}
		if err := errors.Join(iterator.Err(), iterator.Close()); err != nil {
			_ = temporary.Close()
			return result, mapStorageError(err)
		}
	}
	for _, collection := range collections {
		indexes, err := snapshot.Indexes(collection.Name)
		if err != nil {
			_ = temporary.Close()
			return result, mapStorageError(err)
		}
		sort.Slice(indexes, func(left, right int) bool { return indexes[left].Name < indexes[right].Name })
		for _, index := range indexes {
			fields, err := publicIndexFields(index.FieldPath, index.Fields)
			if err != nil {
				_ = temporary.Close()
				return result, ErrCorrupt
			}
			if err := writer.write(ctx, logicalArchiveIndexRecord{Kind: "index", Collection: collection.Name, Name: index.Name, Fields: fields, Unique: index.Unique}, true); err != nil {
				_ = temporary.Close()
				return result, err
			}
			result.Indexes++
		}
	}
	result.Format, result.Version = logicalArchiveFormat, logicalArchiveVersion
	result.SHA256 = hex.EncodeToString(writer.hash.Sum(nil))
	if err := writer.write(ctx, logicalArchiveEndRecord{Kind: "end", Collections: result.Collections, Documents: result.Documents, Indexes: result.Indexes, SHA256: result.SHA256}, false); err != nil {
		_ = temporary.Close()
		return result, err
	}
	if err := writer.flushAndSync(); err != nil {
		return result, err
	}
	info, err := os.Stat(temporaryPath)
	if err != nil {
		return result, err
	}
	result.Bytes = uint64(info.Size())
	if err := publishNewFile(temporaryPath, destination, publishFileOps{link: os.Link, remove: os.Remove, syncDirectory: syncDirectory}); err != nil {
		if errors.Is(err, ErrDestinationExists) {
			return result, ErrLogicalArchiveDestinationExists
		}
		return result, err
	}
	_ = os.Remove(temporaryPath)
	return result, nil
}

// ImportLogicalArchive validates and applies a portable archive into a private
// temporary database, verifies that database offline, then atomically publishes
// it at destination. A malformed archive never leaves a destination database.
func ImportLogicalArchive(ctx context.Context, source io.Reader, destination string, options LogicalArchiveImportOptions) (result LogicalArchiveResult, resultErr error) {
	if source == nil {
		return result, ErrCorrupt
	}
	if err := contextError(ctx); err != nil {
		return result, err
	}
	if destination == "" {
		return result, errors.New("meldbase: empty logical archive destination")
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = DefaultMaxFileBytes
	}
	if options.MaxBytes == 0 {
		return result, ErrInvalidResourceLimits
	}
	destination, err := filepath.Abs(filepath.Clean(destination))
	if err != nil {
		return result, err
	}
	if _, err := os.Lstat(destination); err == nil {
		return result, ErrLogicalArchiveDestinationExists
	} else if !errors.Is(err, fs.ErrNotExist) {
		return result, err
	}
	temporary, err := os.CreateTemp(filepath.Dir(destination), "."+filepath.Base(destination)+".logical-import-*")
	if err != nil {
		return result, err
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return result, err
	}
	if err := os.Remove(temporaryPath); err != nil {
		return result, err
	}
	defer os.Remove(temporaryPath)
	db, err := Open(temporaryPath)
	if err != nil {
		return result, err
	}
	defer func() {
		if db != nil {
			closeErr := db.Close()
			if resultErr == nil && closeErr != nil {
				resultErr = closeErr
			}
		}
	}()
	result, err = importLogicalArchiveInto(ctx, source, db, options.MaxBytes)
	if err != nil {
		return LogicalArchiveResult{}, err
	}
	if err := db.Close(); err != nil {
		return LogicalArchiveResult{}, err
	}
	// Make the deferred close a no-op and verify before the one publish point.
	db = nil
	verification, err := VerifyFile(ctx, temporaryPath)
	if err != nil || !verification.Verified || !verification.IndexContentsVerified || !verification.IndexBuildContentsVerified {
		return LogicalArchiveResult{}, errors.Join(err, ErrCorrupt)
	}
	if err := publishNewFile(temporaryPath, destination, publishFileOps{link: os.Link, remove: os.Remove, syncDirectory: syncDirectory}); err != nil {
		if errors.Is(err, ErrDestinationExists) {
			return LogicalArchiveResult{}, ErrLogicalArchiveDestinationExists
		}
		return LogicalArchiveResult{}, err
	}
	_ = os.Remove(temporaryPath)
	return result, nil
}

func importLogicalArchiveInto(ctx context.Context, source io.Reader, db *DB, maxBytes uint64) (LogicalArchiveResult, error) {
	limited := &logicalArchiveLimitReader{source: source, maximum: maxBytes}
	reader := bufio.NewReaderSize(limited, 128*1024)
	hash := sha256.New()
	phase := 0 // header, collections, documents, indexes, end
	collections := make(map[string]struct{})
	indexes := make(map[string]struct{})
	result := LogicalArchiveResult{Format: logicalArchiveFormat, Version: logicalArchiveVersion}
	for lineNumber := uint64(1); ; lineNumber++ {
		line, newline, err := readLogicalArchiveLine(reader)
		if err != nil {
			if errors.Is(err, io.EOF) && phase == 4 {
				break
			}
			return LogicalArchiveResult{}, logicalArchiveError(lineNumber, err)
		}
		if len(line) == 0 {
			return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("empty record"))
		}
		kind, err := logicalArchiveKind(line)
		if err != nil {
			return LogicalArchiveResult{}, logicalArchiveError(lineNumber, err)
		}
		if kind != "end" {
			hash.Write(line)
			if newline {
				hash.Write([]byte{'\n'})
			}
		}
		switch kind {
		case "header":
			if phase != 0 {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("header is not first"))
			}
			var header logicalArchiveHeader
			if err := strictJSON(line, &header); err != nil || header.Format != logicalArchiveFormat || header.Version != logicalArchiveVersion {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("unsupported logical archive header"))
			}
			phase = 1
		case "collection":
			if phase != 1 {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("collection must precede documents and indexes"))
			}
			var record logicalArchiveCollectionRecord
			if err := strictJSON(line, &record); err != nil || !collectionNamePattern.MatchString(record.Name) {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("invalid collection"))
			}
			if _, exists := collections[record.Name]; exists {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("duplicate collection"))
			}
			if err := db.CreateCollection(ctx, record.Name); err != nil {
				return LogicalArchiveResult{}, err
			}
			collections[record.Name] = struct{}{}
			result.Collections++
		case "document":
			if phase == 1 {
				phase = 2
			}
			if phase != 2 {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("documents must follow collections and precede indexes"))
			}
			var record logicalArchiveDocumentRecord
			if err := strictJSON(line, &record); err != nil || len(record.Document) == 0 {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("invalid document record"))
			}
			if _, exists := collections[record.Collection]; !exists {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("document references unknown collection"))
			}
			document, err := UnmarshalWireDocument(record.Document, logicalArchiveWireLimits())
			if err != nil {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, err)
			}
			if _, err := db.Collection(record.Collection).InsertOne(ctx, document); err != nil {
				return LogicalArchiveResult{}, err
			}
			result.Documents++
		case "index":
			if phase == 1 || phase == 2 {
				phase = 3
			}
			if phase != 3 {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("indexes must follow documents"))
			}
			var record logicalArchiveIndexRecord
			if err := strictJSON(line, &record); err != nil {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("invalid index record"))
			}
			if _, exists := collections[record.Collection]; !exists {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("index references unknown collection"))
			}
			if _, exists := indexes[record.Collection+"\x00"+record.Name]; exists {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("duplicate index"))
			}
			if err := db.Collection(record.Collection).CreateIndex(ctx, record.Name, record.Fields, IndexOptions{Unique: record.Unique}); err != nil {
				return LogicalArchiveResult{}, err
			}
			indexes[record.Collection+"\x00"+record.Name] = struct{}{}
			result.Indexes++
		case "end":
			if phase == 0 || phase == 4 {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("invalid end record"))
			}
			var end logicalArchiveEndRecord
			if err := strictJSON(line, &end); err != nil || end.Collections != result.Collections || end.Documents != result.Documents || end.Indexes != result.Indexes {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("logical archive counts do not match"))
			}
			result.SHA256 = hex.EncodeToString(hash.Sum(nil))
			if len(end.SHA256) != sha256.Size*2 || end.SHA256 != result.SHA256 {
				return LogicalArchiveResult{}, logicalArchiveError(lineNumber, errors.New("logical archive digest does not match"))
			}
			phase = 4
		default:
			return LogicalArchiveResult{}, logicalArchiveError(lineNumber, fmt.Errorf("unknown record kind %q", kind))
		}
	}
	if phase != 4 {
		return LogicalArchiveResult{}, errors.New("meldbase: logical archive is missing end record")
	}
	result.Bytes = limited.read
	return result, nil
}

type logicalArchiveWriter struct {
	file   *os.File
	writer *bufio.Writer
	hash   hash.Hash
}

func newLogicalArchiveWriter(file *os.File) *logicalArchiveWriter {
	return &logicalArchiveWriter{file: file, writer: bufio.NewWriterSize(file, 128*1024), hash: sha256.New()}
}

func (writer *logicalArchiveWriter) write(ctx context.Context, record any, includeHash bool) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(line) > logicalArchiveMaxRecordBytes {
		return fmt.Errorf("%w: logical archive record exceeds limit", ErrResourceLimit)
	}
	if includeHash {
		_, _ = writer.hash.Write(line)
		_, _ = writer.hash.Write([]byte{'\n'})
	}
	if _, err := writer.writer.Write(line); err != nil {
		return err
	}
	return writer.writer.WriteByte('\n')
}

func (writer *logicalArchiveWriter) flushAndSync() error {
	if err := writer.writer.Flush(); err != nil {
		return err
	}
	if err := writer.file.Sync(); err != nil {
		return err
	}
	return writer.file.Close()
}

type logicalArchiveLimitReader struct {
	source  io.Reader
	maximum uint64
	read    uint64
}

func (reader *logicalArchiveLimitReader) Read(target []byte) (int, error) {
	if reader.source == nil || reader.maximum == 0 {
		return 0, ErrResourceLimit
	}
	if reader.read >= reader.maximum {
		var extra [1]byte
		count, err := reader.source.Read(extra[:])
		if count > 0 {
			return 0, ErrResourceLimit
		}
		return 0, err
	}
	remaining := reader.maximum - reader.read
	if uint64(len(target)) > remaining {
		target = target[:remaining]
	}
	count, err := reader.source.Read(target)
	reader.read += uint64(count)
	return count, err
}

func readLogicalArchiveLine(reader *bufio.Reader) ([]byte, bool, error) {
	line, err := reader.ReadBytes('\n')
	if len(line) > logicalArchiveMaxRecordBytes+1 {
		return nil, false, fmt.Errorf("%w: logical archive record exceeds limit", ErrResourceLimit)
	}
	if errors.Is(err, io.EOF) {
		if len(line) == 0 {
			return nil, false, io.EOF
		}
		return line, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return line[:len(line)-1], true, nil
}

func logicalArchiveKind(line []byte) (string, error) {
	fields, err := rawObject(line)
	if err != nil {
		return "", err
	}
	returnValue, exists := fields["kind"]
	if !exists {
		return "", errors.New("record is missing kind")
	}
	var kind string
	if err := strictJSON(returnValue, &kind); err != nil || kind == "" {
		return "", errors.New("invalid record kind")
	}
	return kind, nil
}

func logicalArchiveWireLimits() QueryLimits {
	return QueryLimits{MaxWireBytes: logicalArchiveMaxRecordBytes, MaxDepth: 64, MaxNodes: 1 << 20, MaxArrayItems: 1 << 20, MaxValueBytes: logicalArchiveMaxRecordBytes, MaxSortFields: 4, MaxLimit: 10_000}
}

func logicalArchiveError(line uint64, err error) error {
	if err == nil {
		err = ErrCorrupt
	}
	return fmt.Errorf("%w: logical archive line %d: %v", ErrCorrupt, line, err)
}
