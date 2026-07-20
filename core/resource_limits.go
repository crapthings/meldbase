package meldbase

import (
	"fmt"
	"math"
	"unicode/utf8"
)

const (
	DefaultMaxDocumentBytes      uint64 = 16 << 20
	DefaultMaxTransactionBytes   uint64 = 64 << 20
	DefaultMaxTransactionChanges uint64 = 10_000
	DefaultMaxIndexBuildEntries  uint64 = 1_000_000
	DefaultMaxIndexBuildBytes    uint64 = 256 << 20
	// Reactive views retain matching document versions for incremental ordering
	// and updates, not merely the page currently emitted to a subscriber.
	DefaultMaxReactiveViewDocuments uint64 = 10_000
	DefaultMaxReactiveViewBytes     uint64 = 64 << 20
)

// ResourceLimits bounds work admitted by write and index-maintenance APIs. Zero values
// select production defaults; limits cannot be disabled accidentally. Byte
// limits use the canonical typed binary representation, independent of Go heap
// layout, JSON spelling, storage generation, or transport compression.
type ResourceLimits struct {
	MaxDocumentBytes         uint64 `json:"maxDocumentBytes"`
	MaxTransactionBytes      uint64 `json:"maxTransactionBytes"`
	MaxTransactionChanges    uint64 `json:"maxTransactionChanges"`
	MaxIndexBuildEntries     uint64 `json:"maxIndexBuildEntries"`
	MaxIndexBuildBytes       uint64 `json:"maxIndexBuildBytes"`
	MaxReactiveViewDocuments uint64 `json:"maxReactiveViewDocuments"`
	MaxReactiveViewBytes     uint64 `json:"maxReactiveViewBytes"`
}

// DatabaseOptions configures an in-memory database.
type DatabaseOptions struct{ ResourceLimits ResourceLimits }

func normalizeResourceLimits(limits ResourceLimits) (ResourceLimits, error) {
	if limits.MaxDocumentBytes == 0 {
		limits.MaxDocumentBytes = DefaultMaxDocumentBytes
	}
	if limits.MaxTransactionBytes == 0 {
		limits.MaxTransactionBytes = DefaultMaxTransactionBytes
	}
	if limits.MaxTransactionChanges == 0 {
		limits.MaxTransactionChanges = DefaultMaxTransactionChanges
	}
	if limits.MaxIndexBuildEntries == 0 {
		limits.MaxIndexBuildEntries = DefaultMaxIndexBuildEntries
	}
	if limits.MaxIndexBuildBytes == 0 {
		limits.MaxIndexBuildBytes = DefaultMaxIndexBuildBytes
	}
	if limits.MaxReactiveViewDocuments == 0 {
		limits.MaxReactiveViewDocuments = DefaultMaxReactiveViewDocuments
	}
	if limits.MaxReactiveViewBytes == 0 {
		limits.MaxReactiveViewBytes = DefaultMaxReactiveViewBytes
	}
	if limits.MaxDocumentBytes > maxStoredDocumentBody {
		return ResourceLimits{}, fmt.Errorf("%w: MaxDocumentBytes exceeds the storage format maximum", ErrInvalidResourceLimits)
	}
	if limits.MaxTransactionBytes < limits.MaxDocumentBytes {
		return ResourceLimits{}, fmt.Errorf("%w: MaxTransactionBytes is smaller than MaxDocumentBytes", ErrInvalidResourceLimits)
	}
	if limits.MaxTransactionChanges > math.MaxUint32 {
		return ResourceLimits{}, fmt.Errorf("%w: MaxTransactionChanges exceeds the format maximum", ErrInvalidResourceLimits)
	}
	if limits.MaxIndexBuildEntries > math.MaxUint32 {
		return ResourceLimits{}, fmt.Errorf("%w: MaxIndexBuildEntries exceeds the format maximum", ErrInvalidResourceLimits)
	}
	return limits, nil
}

// admitReactiveViewMember accounts the immutable matching version retained by
// one shared reactive view. It uses canonical document bytes rather than Go
// heap estimates, making the contract stable across storage engines.
func admitReactiveViewMember(limits ResourceLimits, count, bytes, documentBytes uint64) (uint64, uint64, error) {
	if count >= limits.MaxReactiveViewDocuments {
		return 0, 0, fmt.Errorf("%w: reactive view documents exceed limit %d", ErrResourceLimit, limits.MaxReactiveViewDocuments)
	}
	if bytes > limits.MaxReactiveViewBytes || documentBytes > limits.MaxReactiveViewBytes-bytes {
		return 0, 0, fmt.Errorf("%w: reactive view bytes exceed limit %d", ErrResourceLimit, limits.MaxReactiveViewBytes)
	}
	return count + 1, bytes + documentBytes, nil
}

// indexBuildBudget accounts the exact eventual Secondary key bytes: encoded
// scalar key, insertion position, and document ID. It is intentionally a
// storage-independent logical contract rather than an estimate of Go heap use.
type indexBuildBudget struct {
	limits   ResourceLimits
	entries  uint64
	bytes    uint64
	onReject func()
	rejected bool
}

func (budget *indexBuildBudget) reset() {
	if budget == nil {
		return
	}
	budget.entries, budget.bytes, budget.rejected = 0, 0, false
}

func (db *DB) newIndexBuildBudget(limits ResourceLimits) *indexBuildBudget {
	budget := &indexBuildBudget{limits: limits}
	if db != nil {
		budget.onReject = func() { db.metrics.resourceLimitRejections.Add(1) }
	}
	return budget
}

func (budget *indexBuildBudget) add(key []byte) error {
	if budget == nil {
		return nil
	}
	const secondaryKeySuffixBytes = uint64(8 + 16)
	entryBytes := uint64(len(key)) + secondaryKeySuffixBytes
	if budget.entries >= budget.limits.MaxIndexBuildEntries {
		return budget.reject(fmt.Sprintf("index build entries exceed limit %d", budget.limits.MaxIndexBuildEntries))
	}
	if budget.bytes > budget.limits.MaxIndexBuildBytes || entryBytes > budget.limits.MaxIndexBuildBytes-budget.bytes {
		return budget.reject(fmt.Sprintf("index build bytes exceed limit %d", budget.limits.MaxIndexBuildBytes))
	}
	budget.entries++
	budget.bytes += entryBytes
	return nil
}

func (budget *indexBuildBudget) remove(key []byte) error {
	if budget == nil {
		return nil
	}
	entryBytes := uint64(len(key)) + 8 + 16
	if budget.entries == 0 || entryBytes > budget.bytes {
		return ErrCorrupt
	}
	budget.entries--
	budget.bytes -= entryBytes
	return nil
}

func (budget *indexBuildBudget) reject(reason string) error {
	if !budget.rejected && budget.onReject != nil {
		budget.rejected = true
		budget.onReject()
	}
	return fmt.Errorf("%w: %s", ErrResourceLimit, reason)
}

// ResourceLimits returns the immutable normalized limits selected at open.
func (db *DB) ResourceLimits() ResourceLimits {
	if db == nil {
		return ResourceLimits{}
	}
	return db.resourceLimits
}

func (db *DB) validateDocumentResource(document Document) (uint64, error) {
	size, err := canonicalDocumentSize(document)
	if err != nil {
		return 0, err
	}
	if size > db.resourceLimits.MaxDocumentBytes {
		db.metrics.resourceLimitRejections.Add(1)
		return 0, fmt.Errorf("%w: document bytes %d exceed limit %d", ErrResourceLimit, size, db.resourceLimits.MaxDocumentBytes)
	}
	return size, nil
}

func (db *DB) validateTransactionResource(changes []Change) error {
	return db.validateTransactionResourceExtra(changes, 0)
}

func (db *DB) validateTransactionResourceExtra(changes []Change, extraBytes uint64) error {
	if uint64(len(changes)) > db.resourceLimits.MaxTransactionChanges {
		db.metrics.resourceLimitRejections.Add(1)
		return fmt.Errorf("%w: transaction changes %d exceed limit %d", ErrResourceLimit, len(changes), db.resourceLimits.MaxTransactionChanges)
	}
	total := extraBytes
	if total > db.resourceLimits.MaxTransactionBytes {
		db.metrics.resourceLimitRejections.Add(1)
		return fmt.Errorf("%w: transaction bytes exceed limit %d", ErrResourceLimit, db.resourceLimits.MaxTransactionBytes)
	}
	for index := range changes {
		change := &changes[index]
		dispatchBytes, validDispatchBytes := changeDispatchBaseBytes(*change)
		for _, document := range []*Document{change.Before, change.After} {
			if document == nil {
				continue
			}
			size, err := db.validateDocumentResource(*document)
			if err != nil {
				return err
			}
			if total > db.resourceLimits.MaxTransactionBytes || size > db.resourceLimits.MaxTransactionBytes-total {
				db.metrics.resourceLimitRejections.Add(1)
				return fmt.Errorf("%w: transaction bytes exceed limit %d", ErrResourceLimit, db.resourceLimits.MaxTransactionBytes)
			}
			total += size
			if !validDispatchBytes || size > ^uint64(0)-dispatchBytes {
				validDispatchBytes = false
			} else {
				dispatchBytes += size
			}
			if document == change.After {
				change.afterCanonicalBytes = size
				change.afterCanonicalBytesKnown = true
			}
		}
		change.dispatchBytes = dispatchBytes
		change.dispatchBytesKnown = validDispatchBytes
	}
	return nil
}

func (db *DB) boundedMutationSelection(maxAffected int, one bool) (limit int, resourceBounded bool) {
	if one {
		return maxAffected, false
	}
	resource := db.resourceLimits.MaxTransactionChanges
	// Selection reads at most limit+1 to prove overflow.
	maxInt := uint64(^uint(0)>>1) - 1
	if resource > maxInt {
		resource = maxInt
	}
	limit = int(resource)
	if maxAffected > 0 && maxAffected <= limit {
		return maxAffected, false
	}
	return limit, true
}

// canonicalDocumentSize returns the exact size produced by
// encodeDocumentBinary without allocating the encoded byte slice.
func canonicalDocumentSize(document Document) (uint64, error) {
	return canonicalObjectSize(document, 0)
}

func canonicalObjectSize(document Document, depth int) (uint64, error) {
	if depth > 64 || uint64(len(document)) > math.MaxUint32 {
		return 0, ErrInvalidDocument
	}
	total := uint64(4)
	for key, value := range document {
		if err := validField(key); err != nil {
			return 0, err
		}
		if len(key) > math.MaxUint16 {
			return 0, ErrInvalidDocument
		}
		size, err := canonicalValueSize(value, depth+1)
		if err != nil {
			return 0, err
		}
		add := uint64(2 + len(key))
		if add > math.MaxUint64-total || size > math.MaxUint64-total-add {
			return 0, ErrInvalidDocument
		}
		total += add + size
	}
	return total, nil
}

func canonicalValueSize(value Value, depth int) (uint64, error) {
	if depth > 64 {
		return 0, ErrInvalidDocument
	}
	switch value.kind {
	case NullKind:
		return 1, nil
	case BoolKind:
		return 2, nil
	case Int64Kind, TimeKind:
		return 9, nil
	case Float64Kind:
		if math.IsNaN(value.f) || math.IsInf(value.f, 0) {
			return 0, ErrInvalidDocument
		}
		return 9, nil
	case IDKind:
		return 17, nil
	case StringKind:
		if !utf8.ValidString(value.s) || uint64(len(value.s)) > math.MaxUint32 {
			return 0, ErrInvalidDocument
		}
		return 5 + uint64(len(value.s)), nil
	case BinaryKind:
		if uint64(len(value.bin)) > math.MaxUint32 {
			return 0, ErrInvalidDocument
		}
		return 5 + uint64(len(value.bin)), nil
	case ObjectKind:
		size, err := canonicalObjectSize(value.obj, depth+1)
		return size + 1, err
	case ArrayKind:
		if uint64(len(value.arr)) > math.MaxUint32 {
			return 0, ErrInvalidDocument
		}
		total := uint64(5)
		for _, item := range value.arr {
			size, err := canonicalValueSize(item, depth+1)
			if err != nil {
				return 0, err
			}
			if size > math.MaxUint64-total {
				return 0, ErrInvalidDocument
			}
			total += size
		}
		return total, nil
	default:
		return 0, ErrInvalidDocument
	}
}
