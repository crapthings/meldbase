package database

import (
	"fmt"
	"math"
)

const (
	// DefaultMaxDocumentBytes sets the default canonical document byte limit.
	DefaultMaxDocumentBytes uint64 = 16 << 20
	// DefaultMaxTransactionBytes sets the default canonical transaction byte limit.
	DefaultMaxTransactionBytes uint64 = 64 << 20
	// DefaultMaxTransactionChanges sets the default transaction change count limit.
	DefaultMaxTransactionChanges uint64 = 10_000
	// DefaultMaxIndexBuildEntries sets the default index-build entry count limit.
	DefaultMaxIndexBuildEntries uint64 = 1_000_000
	// DefaultMaxIndexBuildBytes sets the default index-build byte limit.
	DefaultMaxIndexBuildBytes uint64 = 256 << 20
	// DefaultMaxReactiveViewDocuments sets the default number of document versions retained by a live query.
	//
	// Views retain matching versions for incremental ordering and updates,
	// not merely the page currently emitted to a subscriber.
	DefaultMaxReactiveViewDocuments uint64 = 10_000
	// DefaultMaxReactiveViewBytes sets the default retained live query byte limit.
	DefaultMaxReactiveViewBytes uint64 = 64 << 20
	// DefaultMaxQueryDocumentsExamined sets the default examined document limit per query.
	DefaultMaxQueryDocumentsExamined uint64 = 100_000
	// DefaultMaxQueryKeysExamined sets the default examined index key limit per query.
	DefaultMaxQueryKeysExamined uint64 = 100_000
	// DefaultMaxQueryCandidates sets the default retained query candidate limit.
	DefaultMaxQueryCandidates uint64 = 100_000
	// DefaultMaxQuerySortBytes sets the default retained sort data byte limit.
	DefaultMaxQuerySortBytes uint64 = 64 << 20
	// DefaultMaxQuerySkip sets the default query offset limit.
	DefaultMaxQuerySkip uint64 = 100_000
	// DefaultMaxQueryPredicateSteps sets the default number of filter evaluation steps per query.
	//
	// Steps include expression visits and data-dependent comparisons such as
	// scalar membership within stored arrays, bounding work beyond scan counts.
	DefaultMaxQueryPredicateSteps uint64 = 10_000_000
)

// ResourceLimits bounds work admitted by writes, index maintenance, and query execution.
//
// Zero values select production defaults; limits cannot be disabled accidentally. Byte
// limits use the canonical typed binary representation, independent of Go heap
// layout, JSON spelling, storage generation, or transport compression.
type ResourceLimits struct {
	MaxDocumentBytes          uint64 `json:"maxDocumentBytes"`
	MaxTransactionBytes       uint64 `json:"maxTransactionBytes"`
	MaxTransactionChanges     uint64 `json:"maxTransactionChanges"`
	MaxIndexBuildEntries      uint64 `json:"maxIndexBuildEntries"`
	MaxIndexBuildBytes        uint64 `json:"maxIndexBuildBytes"`
	MaxReactiveViewDocuments  uint64 `json:"maxReactiveViewDocuments"`
	MaxReactiveViewBytes      uint64 `json:"maxReactiveViewBytes"`
	MaxQueryDocumentsExamined uint64 `json:"maxQueryDocumentsExamined"`
	MaxQueryKeysExamined      uint64 `json:"maxQueryKeysExamined"`
	MaxQueryCandidates        uint64 `json:"maxQueryCandidates"`
	MaxQuerySortBytes         uint64 `json:"maxQuerySortBytes"`
	MaxQuerySkip              uint64 `json:"maxQuerySkip"`
	MaxQueryPredicateSteps    uint64 `json:"maxQueryPredicateSteps"`
}

// DatabaseOptions configures an in-memory database.
type DatabaseOptions struct{ ResourceLimits ResourceLimits }

// normalizeResourceLimits supplies defaults and rejects unsupported resource limit combinations.
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
	if limits.MaxQueryDocumentsExamined == 0 {
		limits.MaxQueryDocumentsExamined = DefaultMaxQueryDocumentsExamined
	}
	if limits.MaxQueryKeysExamined == 0 {
		limits.MaxQueryKeysExamined = DefaultMaxQueryKeysExamined
	}
	if limits.MaxQueryCandidates == 0 {
		limits.MaxQueryCandidates = DefaultMaxQueryCandidates
	}
	if limits.MaxQuerySortBytes == 0 {
		limits.MaxQuerySortBytes = DefaultMaxQuerySortBytes
	}
	if limits.MaxQuerySkip == 0 {
		limits.MaxQuerySkip = DefaultMaxQuerySkip
	}
	if limits.MaxQueryPredicateSteps == 0 {
		limits.MaxQueryPredicateSteps = DefaultMaxQueryPredicateSteps
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

// admitReactiveViewMember checks whether a live query can retain one more document and returns updated totals.
//
// It uses canonical document bytes rather than Go
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

// indexBuildBudget tracks index entry counts and encoded key bytes against build limits.
//
// Each key includes its encoded value, insertion position, and document ID.
// This is a
// storage-independent logical contract rather than an estimate of Go heap use.
type indexBuildBudget struct {
	limits   ResourceLimits
	entries  uint64
	bytes    uint64
	onReject func()
	rejected bool
}

// reset clears retained index entry counts, bytes, and rejection state.
func (budget *indexBuildBudget) reset() {
	if budget == nil {
		return
	}
	budget.entries, budget.bytes, budget.rejected = 0, 0, false
}

// newIndexBuildBudget creates index accounting that records the first resource rejection.
func (db *DB) newIndexBuildBudget(limits ResourceLimits) *indexBuildBudget {
	budget := &indexBuildBudget{limits: limits}
	if db != nil {
		budget.onReject = func() { db.metrics.resourceLimitRejections.Add(1) }
	}
	return budget
}

// add admits one index key and its position and document ID suffix within build limits.
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

// remove releases the entry and byte accounting for one index key.
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

// reject reports a resource limit error and records the first rejection.
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

// validateDocumentResource validates a document and checks its canonical encoded size limit.
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

// validateTransactionResource checks transaction limits and records change delivery sizes.
func (db *DB) validateTransactionResource(changes []Change) error {
	return db.validateTransactionResourceExtra(changes, 0)
}

// validateTransactionResourceExtra checks transaction limits including extra bytes and records change delivery sizes.
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

// boundedMutationSelection caps document selection by the caller's limit or the transaction change limit.
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
