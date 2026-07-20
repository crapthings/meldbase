package v2

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"sync"
)

const (
	maxDurableCommitConsumers = 64
	maxDurableConsumerName    = 64
	durableConsumerHeaderSize = 16
)

var (
	durableConsumerKey   = []byte{0, 'm', 'e', 'l', 'd', 'b', 'a', 's', 'e', '.', 'c', 'o', 'n', 's', 'u', 'm', 'e', 'r', 's'}
	durableConsumerMagic = [8]byte{'M', 'E', 'L', 'D', 'C', 'O', 'N', '1'}

	// ErrDurableConsumerExists prevents an accidental reset of a durable
	// checkpoint. Callers must explicitly open the existing consumer instead.
	ErrDurableConsumerExists   = errors.New("meldbase storage v2: durable consumer already exists")
	ErrDurableConsumerNotFound = errors.New("meldbase storage v2: durable consumer not found")
)

type durableConsumerCheckpoint struct {
	name string
	ack  uint64
}

// DurableCommitConsumer is a pull-based, crash-resumable Commit Log consumer.
// Closing releases only its process-local replay pin; its named checkpoint
// remains durable and continues to cap retention until DeleteDurableCommitConsumer
// is called deliberately.
type DurableCommitConsumer struct {
	mu sync.Mutex
	// nextMu serializes only competing Next callers. It must not be held by a
	// blocking live-stream wait: Ack has to advance independently while the
	// subscription goroutine is idle, otherwise a one-at-a-time replication
	// session deadlocks after its first delivered batch.
	nextMu     sync.Mutex
	file       *File
	name       string
	stream     *LiveCommitStream
	checkpoint uint64
	delivered  uint64
	closed     bool
}

func validDurableConsumerName(name string) bool {
	if len(name) == 0 || len(name) > maxDurableConsumerName {
		return false
	}
	for index := range len(name) {
		value := name[index]
		if !((value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
			(value >= '0' && value <= '9') || value == '_' || value == '-') {
			return false
		}
	}
	return true
}

// CreateDurableCommitConsumer establishes a new named checkpoint at after.
// The selected position must already be recoverable from the current retained
// Commit Log. Creation changes only private control-plane state: it publishes a
// physical COW generation but does not advance the logical commit sequence or
// inject a synthetic change into the consumer's stream.
func (f *File) CreateDurableCommitConsumer(name string, after uint64) (*DurableCommitConsumer, error) {
	if f == nil || !validDurableConsumerName(name) {
		return nil, ErrCorrupt
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return nil, errors.New("meldbase storage v2: file is closed")
	}
	root, err := f.databaseRootUnlocked()
	if err != nil {
		return nil, err
	}
	if err := retainedConsumerPosition(root, after); err != nil {
		return nil, err
	}
	checkpoints, err := f.durableConsumerCheckpointsUnlocked(root.CatalogRoot)
	if err != nil {
		return nil, err
	}
	if _, exists := findDurableConsumer(checkpoints, name); exists {
		return nil, ErrDurableConsumerExists
	}
	if len(checkpoints) >= maxDurableCommitConsumers {
		return nil, ErrCorrupt
	}
	// A revision-3 empty database deliberately has no roots at logical sequence
	// zero, so it cannot host a private System tree in a zero-sequence physical
	// generation. Establishing the first durable consumer therefore creates one
	// private catalog commit and checkpoints past it; no user document history
	// exists to lose at this point. Later create/ack/delete control updates keep
	// their zero-sequence form.
	if root.CommitSequence == 0 {
		if after != 0 {
			return nil, ErrCorrupt
		}
		checkpoint, err := f.createInitialDurableConsumerUnlocked(name)
		if err != nil {
			return nil, err
		}
		return f.openDurableCommitConsumerUnlocked(name, checkpoint)
	}
	checkpoints = append(checkpoints, durableConsumerCheckpoint{name: name, ack: after})
	sort.Slice(checkpoints, func(left, right int) bool { return checkpoints[left].name < checkpoints[right].name })
	if err := f.replaceDurableConsumerCheckpointsUnlocked(checkpoints); err != nil {
		return nil, err
	}
	return f.openDurableCommitConsumerUnlocked(name, after)
}

// OpenDurableCommitConsumer resumes a named checkpoint. It fails explicitly if
// the checkpoint predates retained history; a caller must resynchronize rather
// than silently starting from a newer position.
func (f *File) OpenDurableCommitConsumer(name string) (*DurableCommitConsumer, error) {
	if f == nil || !validDurableConsumerName(name) {
		return nil, ErrCorrupt
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return nil, errors.New("meldbase storage v2: file is closed")
	}
	root, err := f.databaseRootUnlocked()
	if err != nil {
		return nil, err
	}
	checkpoints, err := f.durableConsumerCheckpointsUnlocked(root.CatalogRoot)
	if err != nil {
		return nil, err
	}
	checkpoint, exists := findDurableConsumer(checkpoints, name)
	if !exists {
		return nil, ErrDurableConsumerNotFound
	}
	if err := retainedConsumerPosition(root, checkpoint.ack); err != nil {
		return nil, err
	}
	return f.openDurableCommitConsumerUnlocked(name, checkpoint.ack)
}

func retainedConsumerPosition(root DatabaseRoot, after uint64) error {
	if after > root.CommitSequence {
		return ErrCorrupt
	}
	if after < root.CommitSequence && (after == math.MaxUint64 || after+1 < root.OldestRetainedSequence) {
		return ErrHistoryLost
	}
	return nil
}

func (f *File) openDurableCommitConsumerUnlocked(name string, after uint64) (*DurableCommitConsumer, error) {
	if f == nil || f.file == nil {
		return nil, errors.New("meldbase storage v2: file is closed")
	}
	pinID, err := f.addReaderPinUnlocked(after, f.meta.RootPage, true)
	if err != nil {
		return nil, err
	}
	stream := &LiveCommitStream{file: f, pinID: pinID, after: after, closed: make(chan struct{})}
	return &DurableCommitConsumer{file: f, name: name, stream: stream, checkpoint: after, delivered: after}, nil
}

// Next returns the next durable Commit Log batch. It does not advance the
// checkpoint; callers must Ack only after their own side effect is complete.
func (consumer *DurableCommitConsumer) Next(ctx context.Context) (CommitBatch, error) {
	if consumer == nil {
		return CommitBatch{}, ErrCursorClosed
	}
	consumer.nextMu.Lock()
	defer consumer.nextMu.Unlock()
	consumer.mu.Lock()
	if consumer.closed || consumer.stream == nil {
		consumer.mu.Unlock()
		return CommitBatch{}, ErrCursorClosed
	}
	stream := consumer.stream
	consumer.mu.Unlock()
	batch, err := stream.Next(ctx)
	if err != nil {
		return CommitBatch{}, err
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.closed || consumer.stream != stream {
		return CommitBatch{}, ErrCursorClosed
	}
	if batch.Sequence <= consumer.delivered {
		return CommitBatch{}, ErrCorrupt
	}
	consumer.delivered = batch.Sequence
	return batch, nil
}

// ResolveChange materializes one change from the batch most recently returned
// by Next. It has the same lifetime/ordering rule as LiveCommitStream: callers
// must resolve all required images before advancing the consumer again.
func (consumer *DurableCommitConsumer) ResolveChange(change CommitChange) (ResolvedCommitChange, error) {
	if consumer == nil {
		return ResolvedCommitChange{}, ErrCursorClosed
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.closed || consumer.stream == nil {
		return ResolvedCommitChange{}, ErrCursorClosed
	}
	return consumer.stream.ResolveChange(change)
}

// Ack durably advances this consumer through a delivered sequence. Ack is
// monotonic and idempotent; a stale process may acknowledge an older sequence
// but can never move the durable checkpoint backward. The control-plane update
// has no logical commit sequence, so it cannot create an acknowledgement loop.
func (consumer *DurableCommitConsumer) Ack(sequence uint64) error {
	if consumer == nil {
		return ErrCursorClosed
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.closed || consumer.file == nil {
		return ErrCursorClosed
	}
	if sequence < consumer.checkpoint || sequence > consumer.delivered {
		return ErrCorrupt
	}
	consumer.file.mu.Lock()
	defer consumer.file.mu.Unlock()
	if consumer.file.file == nil {
		return errors.New("meldbase storage v2: file is closed")
	}
	root, err := consumer.file.databaseRootUnlocked()
	if err != nil {
		return err
	}
	checkpoints, err := consumer.file.durableConsumerCheckpointsUnlocked(root.CatalogRoot)
	if err != nil {
		return err
	}
	checkpoint, exists := findDurableConsumer(checkpoints, consumer.name)
	if !exists {
		return ErrDurableConsumerNotFound
	}
	if checkpoint.ack >= sequence {
		consumer.checkpoint = checkpoint.ack
		return nil
	}
	for index := range checkpoints {
		if checkpoints[index].name == consumer.name {
			checkpoints[index].ack = sequence
			break
		}
	}
	if err := consumer.file.replaceDurableConsumerCheckpointsUnlocked(checkpoints); err != nil {
		return err
	}
	consumer.checkpoint = sequence
	return nil
}

func (f *File) createInitialDurableConsumerUnlocked(name string) (uint64, error) {
	if f == nil || f.file == nil || !validDurableConsumerName(name) || f.meta.CommitSequence != 0 {
		return 0, ErrCorrupt
	}
	var transactionID [16]byte
	if _, err := rand.Read(transactionID[:]); err != nil {
		return 0, err
	}
	checkpoint := uint64(0)
	err := f.updateUnlocked(true, func(tx *WriteTxn) (DatabaseRoot, error) {
		base := tx.BaseRoot()
		if base.CommitSequence != 0 || tx.Sequence() != 1 {
			return DatabaseRoot{}, ErrCorrupt
		}
		checkpoint = tx.Sequence()
		encoded, err := encodeDurableConsumerCheckpoints([]durableConsumerCheckpoint{{name: name, ack: checkpoint}})
		if err != nil {
			return DatabaseRoot{}, err
		}
		catalog, err := tx.OpenTree(base.CatalogRoot, TreeCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		_, applied, err := tx.applySystemRecordMutation(catalog, SystemRecordMutation{
			Key: durableConsumerKey, NewValue: encoded, Unconditional: true,
		})
		if err != nil || !applied {
			if err == nil {
				err = ErrCorrupt
			}
			return DatabaseRoot{}, err
		}
		catalogRoot, err := catalog.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		commitLogRoot, oldest, err := tx.AppendCommitRetained(base.CommitLogRoot, base.OldestRetainedSequence, CommitBatch{
			Sequence: tx.Sequence(), TransactionID: transactionID, CatalogRoot: catalogRoot,
			Changes: []CommitChange{{CollectionID: math.MaxUint32, Operation: CommitCatalog, ChangedPaths: []string{"_system.durable_consumers"}}},
		})
		if err != nil {
			return DatabaseRoot{}, err
		}
		return DatabaseRoot{
			CommitSequence: tx.Sequence(), CatalogRoot: catalogRoot, CommitLogRoot: commitLogRoot,
			FreeSpaceRoot: base.FreeSpaceRoot, OldestRetainedSequence: oldest,
			CatalogGeneration: base.CatalogGeneration, DocumentCount: base.DocumentCount, CollectionCount: base.CollectionCount,
			IndexBuildCatalogRoot: base.IndexBuildCatalogRoot,
		}, nil
	})
	return checkpoint, err
}

func (consumer *DurableCommitConsumer) Checkpoint() uint64 {
	if consumer == nil {
		return 0
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	return consumer.checkpoint
}

func (consumer *DurableCommitConsumer) Close() error {
	if consumer == nil {
		return nil
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.closed {
		return nil
	}
	consumer.closed = true
	if consumer.stream != nil {
		err := consumer.stream.Close()
		consumer.stream = nil
		consumer.file = nil
		return err
	}
	consumer.file = nil
	return nil
}

// DeleteDurableCommitConsumer explicitly removes a checkpoint and its durable
// retention pin. It does not close another process's active stream; that stream
// still owns its temporary pin until Close, but future acknowledgements fail.
func (f *File) DeleteDurableCommitConsumer(name string) error {
	if f == nil || !validDurableConsumerName(name) {
		return ErrCorrupt
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return errors.New("meldbase storage v2: file is closed")
	}
	root, err := f.databaseRootUnlocked()
	if err != nil {
		return err
	}
	checkpoints, err := f.durableConsumerCheckpointsUnlocked(root.CatalogRoot)
	if err != nil {
		return err
	}
	index := sort.Search(len(checkpoints), func(index int) bool { return checkpoints[index].name >= name })
	if index >= len(checkpoints) || checkpoints[index].name != name {
		return ErrDurableConsumerNotFound
	}
	checkpoints = append(checkpoints[:index], checkpoints[index+1:]...)
	return f.replaceDurableConsumerCheckpointsUnlocked(checkpoints)
}

func findDurableConsumer(checkpoints []durableConsumerCheckpoint, name string) (durableConsumerCheckpoint, bool) {
	index := sort.Search(len(checkpoints), func(index int) bool { return checkpoints[index].name >= name })
	if index < len(checkpoints) && checkpoints[index].name == name {
		return checkpoints[index], true
	}
	return durableConsumerCheckpoint{}, false
}

func (f *File) durableConsumerCheckpointsUnlocked(catalogRoot uint64) ([]durableConsumerCheckpoint, error) {
	value, exists, err := f.getSystemRecordUnlocked(catalogRoot, durableConsumerKey)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return decodeDurableConsumerCheckpoints(value)
}

// replaceDurableConsumerCheckpointsUnlocked persists only control-plane state:
// it advances the physical generation but keeps CommitSequence and Commit Log
// roots intact. The next business publication observes this directory before
// pruning history.
func (f *File) replaceDurableConsumerCheckpointsUnlocked(checkpoints []durableConsumerCheckpoint) error {
	encoded, err := encodeDurableConsumerCheckpoints(checkpoints)
	if err != nil {
		return err
	}
	return f.updateUnlocked(false, func(tx *WriteTxn) (DatabaseRoot, error) {
		base := tx.BaseRoot()
		catalog, err := tx.OpenTree(base.CatalogRoot, TreeCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		_, applied, err := tx.applySystemRecordMutation(catalog, SystemRecordMutation{
			Key: durableConsumerKey, NewValue: encoded, Unconditional: true,
		})
		if err != nil || !applied {
			if err == nil {
				err = ErrCorrupt
			}
			return DatabaseRoot{}, err
		}
		catalogRoot, err := catalog.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		return DatabaseRoot{
			CommitSequence: base.CommitSequence, CatalogRoot: catalogRoot, CommitLogRoot: base.CommitLogRoot,
			FreeSpaceRoot: base.FreeSpaceRoot, OldestRetainedSequence: base.OldestRetainedSequence,
			CatalogGeneration: base.CatalogGeneration, DocumentCount: base.DocumentCount, CollectionCount: base.CollectionCount,
			IndexBuildCatalogRoot: base.IndexBuildCatalogRoot,
		}, nil
	})
}

func encodeDurableConsumerCheckpoints(checkpoints []durableConsumerCheckpoint) ([]byte, error) {
	if len(checkpoints) > maxDurableCommitConsumers {
		return nil, ErrCorrupt
	}
	ordered := append([]durableConsumerCheckpoint(nil), checkpoints...)
	sort.Slice(ordered, func(left, right int) bool { return ordered[left].name < ordered[right].name })
	size := durableConsumerHeaderSize
	for index, checkpoint := range ordered {
		if !validDurableConsumerName(checkpoint.name) || (index > 0 && ordered[index-1].name == checkpoint.name) ||
			len(checkpoint.name) > math.MaxUint8 || size > math.MaxInt-(1+len(checkpoint.name)+8) {
			return nil, ErrCorrupt
		}
		size += 1 + len(checkpoint.name) + 8
	}
	result := make([]byte, size)
	copy(result[:8], durableConsumerMagic[:])
	binary.LittleEndian.PutUint16(result[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(result[10:12], uint16(len(ordered)))
	offset := durableConsumerHeaderSize
	for _, checkpoint := range ordered {
		result[offset] = byte(len(checkpoint.name))
		offset++
		copy(result[offset:], checkpoint.name)
		offset += len(checkpoint.name)
		binary.LittleEndian.PutUint64(result[offset:offset+8], checkpoint.ack)
		offset += 8
	}
	return result, nil
}

func decodeDurableConsumerCheckpoints(encoded []byte) ([]durableConsumerCheckpoint, error) {
	if len(encoded) < durableConsumerHeaderSize || !bytes.Equal(encoded[:8], durableConsumerMagic[:]) ||
		binary.LittleEndian.Uint16(encoded[8:10]) != FormatVersion || binary.LittleEndian.Uint16(encoded[12:14]) != 0 ||
		binary.LittleEndian.Uint16(encoded[14:16]) != 0 {
		return nil, ErrCorrupt
	}
	count := int(binary.LittleEndian.Uint16(encoded[10:12]))
	if count > maxDurableCommitConsumers {
		return nil, ErrCorrupt
	}
	result := make([]durableConsumerCheckpoint, count)
	offset := durableConsumerHeaderSize
	for index := range result {
		if offset >= len(encoded) {
			return nil, ErrCorrupt
		}
		nameLength := int(encoded[offset])
		offset++
		if nameLength == 0 || nameLength > maxDurableConsumerName || offset > len(encoded)-nameLength-8 {
			return nil, ErrCorrupt
		}
		name := string(encoded[offset : offset+nameLength])
		offset += nameLength
		if !validDurableConsumerName(name) || (index > 0 && result[index-1].name >= name) {
			return nil, ErrCorrupt
		}
		result[index] = durableConsumerCheckpoint{name: name, ack: binary.LittleEndian.Uint64(encoded[offset : offset+8])}
		offset += 8
	}
	if offset != len(encoded) {
		return nil, ErrCorrupt
	}
	return result, nil
}

func (tx *WriteTxn) durableConsumerRetentionFloor(catalogRoot uint64, oldest, latest uint64) (uint64, error) {
	if tx == nil || tx.file == nil || oldest == 0 || oldest > latest {
		return 0, ErrCorrupt
	}
	checkpoints, err := tx.file.durableConsumerCheckpointsUnlocked(catalogRoot)
	if err != nil || len(checkpoints) == 0 {
		return 0, err
	}
	floor := latest
	for _, checkpoint := range checkpoints {
		if checkpoint.ack >= latest {
			return 0, ErrCorrupt
		}
		protected := checkpoint.ack + 1
		if protected == 0 || protected < oldest {
			return 0, ErrCorrupt
		}
		if floor > protected {
			floor = protected
		}
	}
	return floor, nil
}
