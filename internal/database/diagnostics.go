package database

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrDiagnosticsActive reports that a diagnostics session is already active.
	ErrDiagnosticsActive = errors.New("meldbase: diagnostics are already active")
)

// DiagnosticKind identifies the operation recorded by a diagnostic event.
type DiagnosticKind string

const (
	// DiagnosticQuery marks a query diagnostic.
	DiagnosticQuery DiagnosticKind = "query"
	// DiagnosticCommit marks a commit diagnostic.
	DiagnosticCommit DiagnosticKind = "commit"
)

// DiagnosticOutcome identifies how a recorded operation ended.
type DiagnosticOutcome string

const (
	// DiagnosticSuccess marks a successful operation.
	DiagnosticSuccess DiagnosticOutcome = "success"
	// DiagnosticFailure marks a failed operation.
	DiagnosticFailure DiagnosticOutcome = "failure"
	// DiagnosticCanceled marks a canceled operation.
	DiagnosticCanceled DiagnosticOutcome = "canceled"
)

// DiagnosticsOptions configures bounded diagnostic recording and event selection.
//
// Defaults retain 256 events and record failures, queries taking at least 50ms,
// and commits taking at least 100ms.
// RecordAll is intended only for short development sessions. SampleEvery adds a
// deterministic one-in-N sample of otherwise fast successful operations.
type DiagnosticsOptions struct {
	Capacity            int
	SlowQueryThreshold  time.Duration
	SlowCommitThreshold time.Duration
	SampleEvery         uint64
	RecordAll           bool
	ExcludeFailures     bool
}

// DiagnosticEvent records one operation's timing, outcome, and work counters.
type DiagnosticEvent struct {
	Sequence              uint64            `json:"sequence"`
	CapturedAt            time.Time         `json:"capturedAt"`
	Kind                  DiagnosticKind    `json:"kind"`
	Outcome               DiagnosticOutcome `json:"outcome"`
	ErrorClass            string            `json:"errorClass,omitempty"`
	Stage                 string            `json:"stage,omitempty"`
	PlanReason            string            `json:"planReason,omitempty"`
	FallbackReason        string            `json:"fallbackReason,omitempty"`
	EarlyStopReason       string            `json:"earlyStopReason,omitempty"`
	EarlyStopScope        string            `json:"earlyStopScope,omitempty"`
	BudgetPressure        string            `json:"budgetPressure,omitempty"`
	BudgetExceeded        string            `json:"budgetExceeded,omitempty"`
	Duration              time.Duration     `json:"durationNanos"`
	DocumentsExamined     uint64            `json:"documentsExamined,omitempty"`
	DocumentsReturned     uint64            `json:"documentsReturned,omitempty"`
	KeysExamined          uint64            `json:"keysExamined,omitempty"`
	PredicateSteps        uint64            `json:"predicateSteps,omitempty"`
	CandidateIDs          uint64            `json:"candidateIds,omitempty"`
	UniqueCandidateIDs    uint64            `json:"uniqueCandidateIds,omitempty"`
	DuplicateCandidateIDs uint64            `json:"duplicateCandidateIds,omitempty"`
	CandidatesRetained    uint64            `json:"candidatesRetained,omitempty"`
	SortBytes             uint64            `json:"sortBytes,omitempty"`
	EarlyStopped          bool              `json:"earlyStopped,omitempty"`
	Changes               uint64            `json:"changes,omitempty"`
	Slow                  bool              `json:"slow"`
	Sampled               bool              `json:"sampled"`
}

// DiagnosticStats reports diagnostics activity and retained event counts.
type DiagnosticStats struct {
	Enabled         bool   `json:"enabled"`
	Capacity        uint64 `json:"capacity"`
	Retained        uint64 `json:"retained"`
	Recorded        uint64 `json:"recorded"`
	Overwritten     uint64 `json:"overwritten"`
	QueriesObserved uint64 `json:"queriesObserved"`
	CommitsObserved uint64 `json:"commitsObserved"`
}

// DiagnosticSnapshot captures a diagnostics session's counters and retained events.
type DiagnosticSnapshot struct {
	Session    uint64            `json:"session"`
	StartedAt  time.Time         `json:"startedAt"`
	CapturedAt time.Time         `json:"capturedAt"`
	Stats      DiagnosticStats   `json:"stats"`
	Events     []DiagnosticEvent `json:"events"`
	Truncated  bool              `json:"truncated"`
	HasMore    bool              `json:"hasMore"`
}

// Diagnostics owns a bounded ring of recorded operation events.
//
// Close disables timing and recording but keeps retained events readable.
type Diagnostics struct {
	db        *DB
	options   DiagnosticsOptions
	session   uint64
	startedAt time.Time
	closed    atomic.Bool

	queriesObserved atomic.Uint64
	commitsObserved atomic.Uint64

	mu           sync.Mutex
	events       []DiagnosticEvent
	head         int
	length       int
	nextSequence uint64
	overwritten  uint64
}

// diagnosticSpan records the start and sampling position of one observed operation.
type diagnosticSpan struct {
	recorder *Diagnostics
	kind     DiagnosticKind
	started  time.Time
	ordinal  uint64
}

// EnableDiagnostics starts the database's single bounded diagnostic recording session.
func (db *DB) EnableDiagnostics(options DiagnosticsOptions) (*Diagnostics, error) {
	if db == nil {
		return nil, ErrClosed
	}
	if options.Capacity == 0 {
		options.Capacity = 256
	}
	if options.Capacity < 1 || options.Capacity > 65_536 {
		return nil, errors.New("meldbase: diagnostics capacity must be between 1 and 65536")
	}
	if options.SlowQueryThreshold < 0 || options.SlowCommitThreshold < 0 {
		return nil, errors.New("meldbase: diagnostics thresholds cannot be negative")
	}
	if options.SlowQueryThreshold == 0 {
		options.SlowQueryThreshold = 50 * time.Millisecond
	}
	if options.SlowCommitThreshold == 0 {
		options.SlowCommitThreshold = 100 * time.Millisecond
	}

	diagnostics := &Diagnostics{
		db: db, options: options, session: db.diagnosticSession.Add(1), startedAt: time.Now(),
		events: make([]DiagnosticEvent, options.Capacity),
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	if !db.diagnostics.CompareAndSwap(nil, diagnostics) {
		return nil, ErrDiagnosticsActive
	}
	return diagnostics, nil
}

// beginDiagnostic starts an operation timer when diagnostics are enabled.
func (db *DB) beginDiagnostic(kind DiagnosticKind) diagnosticSpan {
	if db == nil {
		return diagnosticSpan{}
	}
	recorder := db.diagnostics.Load()
	if recorder == nil {
		return diagnosticSpan{}
	}
	return recorder.begin(kind)
}

// begin starts timing a known operation and advances its sampling counter.
func (d *Diagnostics) begin(kind DiagnosticKind) diagnosticSpan {
	if d.closed.Load() {
		return diagnosticSpan{}
	}
	var ordinal uint64
	switch kind {
	case DiagnosticQuery:
		ordinal = d.queriesObserved.Add(1)
	case DiagnosticCommit:
		ordinal = d.commitsObserved.Add(1)
	default:
		return diagnosticSpan{}
	}
	return diagnosticSpan{recorder: d, kind: kind, started: time.Now(), ordinal: ordinal}
}

// finishQueryDiagnostic forwards a query outcome to its diagnostic session.
func (db *DB) finishQueryDiagnostic(span diagnosticSpan, explain ExplainResult, returned int, operationErr error) {
	if span.recorder == nil {
		return
	}
	span.recorder.finishQuery(span, explain, returned, operationErr)
}

// finishQuery records safe query labels, work counters, and timing.
func (d *Diagnostics) finishQuery(span diagnosticSpan, explain ExplainResult, returned int, operationErr error) {
	event := finishDiagnosticEvent(span, operationErr)
	event.Stage = safeDiagnosticStage(explain.Stage)
	event.PlanReason = safeDiagnosticPlanReason(explain.PlanReason)
	event.FallbackReason = safeDiagnosticFallbackReason(explain.FallbackReason)
	event.EarlyStopReason = safeDiagnosticEarlyStopReason(explain.EarlyStopReason)
	event.EarlyStopScope = safeDiagnosticEarlyStopScope(explain.EarlyStopScope)
	event.BudgetPressure = safeDiagnosticBudgetReason(explain.Budget.Pressure)
	event.BudgetExceeded = safeDiagnosticBudgetReason(explain.Budget.Exceeded)
	event.EarlyStopped = explain.EarlyStopped
	if explain.DocumentsExamined > 0 {
		event.DocumentsExamined = uint64(explain.DocumentsExamined)
	}
	if explain.KeysExamined > 0 {
		event.KeysExamined = uint64(explain.KeysExamined)
	}
	if explain.Budget.PredicateStepsUsed > 0 {
		event.PredicateSteps = explain.Budget.PredicateStepsUsed
	}
	if explain.CandidateIDs > 0 {
		event.CandidateIDs = uint64(explain.CandidateIDs)
	}
	if explain.UniqueCandidateIDs > 0 {
		event.UniqueCandidateIDs = uint64(explain.UniqueCandidateIDs)
	}
	if explain.DuplicateCandidateIDs > 0 {
		event.DuplicateCandidateIDs = uint64(explain.DuplicateCandidateIDs)
	}
	event.CandidatesRetained = explain.CandidatesRetained
	event.SortBytes = explain.SortBytes
	if returned > 0 {
		event.DocumentsReturned = uint64(returned)
	}
	d.record(event, span.ordinal)
}

// finishCommitDiagnostic forwards a commit outcome to its diagnostic session.
func (db *DB) finishCommitDiagnostic(span diagnosticSpan, changes int, operationErr error) {
	if span.recorder == nil {
		return
	}
	span.recorder.finishCommit(span, changes, operationErr)
}

// finishCommit records a commit's timing, outcome, and change count.
func (d *Diagnostics) finishCommit(span diagnosticSpan, changes int, operationErr error) {
	event := finishDiagnosticEvent(span, operationErr)
	if changes > 0 {
		event.Changes = uint64(changes)
	}
	d.record(event, span.ordinal)
}

// finishDiagnosticEvent builds timing and outcome fields for a completed operation.
func finishDiagnosticEvent(span diagnosticSpan, operationErr error) DiagnosticEvent {
	finished := time.Now()
	outcome, class := classifyDiagnosticError(operationErr)
	return DiagnosticEvent{
		CapturedAt: finished, Kind: span.kind, Outcome: outcome,
		ErrorClass: class, Duration: finished.Sub(span.started),
	}
}

// record retains eligible events in the bounded ring and overwrites the oldest when full.
func (d *Diagnostics) record(event DiagnosticEvent, ordinal uint64) {
	if d == nil || d.closed.Load() {
		return
	}
	threshold := d.options.SlowQueryThreshold
	if event.Kind == DiagnosticCommit {
		threshold = d.options.SlowCommitThreshold
	}
	event.Slow = event.Duration >= threshold
	event.Sampled = d.options.SampleEvery > 0 && ordinal%d.options.SampleEvery == 0
	failed := event.Outcome != DiagnosticSuccess
	if !d.options.RecordAll && !event.Slow && !event.Sampled && (!failed || d.options.ExcludeFailures) {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed.Load() {
		return
	}
	d.nextSequence++
	event.Sequence = d.nextSequence
	index := (d.head + d.length) % len(d.events)
	d.events[index] = event
	if d.length < len(d.events) {
		d.length++
	} else {
		d.head = (d.head + 1) % len(d.events)
		d.overwritten++
	}
}

// Stats samples diagnostic activity and event retention counters.
func (d *Diagnostics) Stats() DiagnosticStats {
	if d == nil {
		return DiagnosticStats{}
	}
	d.mu.Lock()
	stats := DiagnosticStats{
		Enabled: !d.closed.Load(), Capacity: uint64(len(d.events)),
		Retained: uint64(d.length), Recorded: d.nextSequence, Overwritten: d.overwritten,
	}
	d.mu.Unlock()
	stats.QueriesObserved = d.queriesObserved.Load()
	stats.CommitsObserved = d.commitsObserved.Load()
	return stats
}

// Snapshot copies all events currently retained by the diagnostic session.
func (d *Diagnostics) Snapshot() DiagnosticSnapshot {
	return d.SnapshotAfter(0, 0)
}

// SnapshotAfter copies retained events newer than after in chronological order.
//
// A non-positive limit means the ring capacity; Truncated
// reports that after predates the oldest retained sequence; HasMore asks the
// caller to continue from the last returned sequence.
func (d *Diagnostics) SnapshotAfter(after uint64, limit int) DiagnosticSnapshot {
	if d == nil {
		return DiagnosticSnapshot{CapturedAt: time.Now()}
	}
	d.mu.Lock()
	if limit <= 0 || limit > len(d.events) {
		limit = len(d.events)
	}
	events := make([]DiagnosticEvent, 0, min(d.length, limit))
	truncated := false
	if d.length > 0 && after != 0 {
		oldest := d.events[d.head].Sequence
		truncated = after < oldest && oldest-after > 1
	}
	hasMore := false
	for index := 0; index < d.length; index++ {
		event := d.events[(d.head+index)%len(d.events)]
		if event.Sequence <= after {
			continue
		}
		if len(events) == limit {
			hasMore = true
			break
		}
		events = append(events, event)
	}
	stats := DiagnosticStats{
		Enabled: !d.closed.Load(), Capacity: uint64(len(d.events)),
		Retained: uint64(d.length), Recorded: d.nextSequence, Overwritten: d.overwritten,
	}
	d.mu.Unlock()
	stats.QueriesObserved = d.queriesObserved.Load()
	stats.CommitsObserved = d.commitsObserved.Load()
	return DiagnosticSnapshot{
		Session: d.session, StartedAt: d.startedAt, CapturedAt: time.Now(), Stats: stats, Events: events,
		Truncated: truncated, HasMore: hasMore,
	}
}

// DiagnosticSnapshotAfter copies newer events from the currently active diagnostic session.
//
// Long-lived admin handlers can follow a replaced session through this method.
func (db *DB) DiagnosticSnapshotAfter(after uint64, limit int) DiagnosticSnapshot {
	if db == nil {
		return DiagnosticSnapshot{CapturedAt: time.Now()}
	}
	diagnostics := db.diagnostics.Load()
	if diagnostics == nil {
		return DiagnosticSnapshot{CapturedAt: time.Now()}
	}
	return diagnostics.SnapshotAfter(after, limit)
}

// DiagnosticSnapshotAfter copies newer events through the admin diagnostic-source contract.
func (d *Diagnostics) DiagnosticSnapshotAfter(after uint64, limit int) DiagnosticSnapshot {
	return d.SnapshotAfter(after, limit)
}

// Close disables recording while leaving retained diagnostic events readable.
func (d *Diagnostics) Close() error {
	if d == nil {
		return nil
	}
	if !d.closed.CompareAndSwap(false, true) {
		return nil
	}
	if d.db != nil {
		d.db.diagnostics.CompareAndSwap(d, nil)
	}
	return nil
}
