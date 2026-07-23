package database

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrDiagnosticsActive = errors.New("meldbase: diagnostics are already active")
)

type DiagnosticKind string

const (
	DiagnosticQuery  DiagnosticKind = "query"
	DiagnosticCommit DiagnosticKind = "commit"
)

type DiagnosticOutcome string

const (
	DiagnosticSuccess  DiagnosticOutcome = "success"
	DiagnosticFailure  DiagnosticOutcome = "failure"
	DiagnosticCanceled DiagnosticOutcome = "canceled"
)

// DiagnosticsOptions controls opt-in detailed events. Defaults retain 256
// events and record failed, >=50ms queries and >=100ms durable commits. Setting
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

type DiagnosticEvent struct {
	Sequence          uint64            `json:"sequence"`
	CapturedAt        time.Time         `json:"capturedAt"`
	Kind              DiagnosticKind    `json:"kind"`
	Outcome           DiagnosticOutcome `json:"outcome"`
	ErrorClass        string            `json:"errorClass,omitempty"`
	Stage             string            `json:"stage,omitempty"`
	Duration          time.Duration     `json:"durationNanos"`
	DocumentsExamined uint64            `json:"documentsExamined,omitempty"`
	DocumentsReturned uint64            `json:"documentsReturned,omitempty"`
	Changes           uint64            `json:"changes,omitempty"`
	Slow              bool              `json:"slow"`
	Sampled           bool              `json:"sampled"`
}

type DiagnosticStats struct {
	Enabled         bool   `json:"enabled"`
	Capacity        uint64 `json:"capacity"`
	Retained        uint64 `json:"retained"`
	Recorded        uint64 `json:"recorded"`
	Overwritten     uint64 `json:"overwritten"`
	QueriesObserved uint64 `json:"queriesObserved"`
	CommitsObserved uint64 `json:"commitsObserved"`
}

type DiagnosticSnapshot struct {
	Session    uint64            `json:"session"`
	StartedAt  time.Time         `json:"startedAt"`
	CapturedAt time.Time         `json:"capturedAt"`
	Stats      DiagnosticStats   `json:"stats"`
	Events     []DiagnosticEvent `json:"events"`
	Truncated  bool              `json:"truncated"`
	HasMore    bool              `json:"hasMore"`
}

// Diagnostics owns a fixed-capacity event ring. Close disables future timing
// and recording but keeps the retained snapshot readable by its owner.
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

type diagnosticSpan struct {
	recorder *Diagnostics
	kind     DiagnosticKind
	started  time.Time
	ordinal  uint64
}

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

func (db *DB) finishQueryDiagnostic(span diagnosticSpan, explain ExplainResult, returned int, operationErr error) {
	if span.recorder == nil {
		return
	}
	span.recorder.finishQuery(span, explain, returned, operationErr)
}

func (d *Diagnostics) finishQuery(span diagnosticSpan, explain ExplainResult, returned int, operationErr error) {
	event := finishDiagnosticEvent(span, operationErr)
	event.Stage = safeDiagnosticStage(explain.Stage)
	if explain.DocumentsExamined > 0 {
		event.DocumentsExamined = uint64(explain.DocumentsExamined)
	}
	if returned > 0 {
		event.DocumentsReturned = uint64(returned)
	}
	d.record(event, span.ordinal)
}

func (db *DB) finishCommitDiagnostic(span diagnosticSpan, changes int, operationErr error) {
	if span.recorder == nil {
		return
	}
	span.recorder.finishCommit(span, changes, operationErr)
}

func (d *Diagnostics) finishCommit(span diagnosticSpan, changes int, operationErr error) {
	event := finishDiagnosticEvent(span, operationErr)
	if changes > 0 {
		event.Changes = uint64(changes)
	}
	d.record(event, span.ordinal)
}

func finishDiagnosticEvent(span diagnosticSpan, operationErr error) DiagnosticEvent {
	finished := time.Now()
	outcome, class := classifyDiagnosticError(operationErr)
	return DiagnosticEvent{
		CapturedAt: finished, Kind: span.kind, Outcome: outcome,
		ErrorClass: class, Duration: finished.Sub(span.started),
	}
}

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

func (d *Diagnostics) Snapshot() DiagnosticSnapshot {
	return d.SnapshotAfter(0, 0)
}

// SnapshotAfter returns retained events with sequence greater than after in
// chronological order. A non-positive limit means the ring capacity. Truncated
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

// DiagnosticSnapshotAfter reads the currently active diagnostic session. It
// allows long-lived admin handlers to follow a safely replaced session.
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

// DiagnosticSnapshotAfter lets a fixed Diagnostics handle also satisfy admin
// diagnostic-source contracts.
func (d *Diagnostics) DiagnosticSnapshotAfter(after uint64, limit int) DiagnosticSnapshot {
	return d.SnapshotAfter(after, limit)
}

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

func safeDiagnosticStage(stage string) string {
	switch stage {
	case "COLLSCAN", "IXSCAN", "ID_LOOKUP":
		return stage
	default:
		return "UNKNOWN"
	}
}

func classifyDiagnosticError(err error) (DiagnosticOutcome, string) {
	if err == nil {
		return DiagnosticSuccess, ""
	}
	if errors.Is(err, context.Canceled) {
		return DiagnosticCanceled, "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return DiagnosticCanceled, "deadline"
	}
	classes := []struct {
		err  error
		name string
	}{
		{ErrClosed, "closed"}, {ErrNotFound, "not_found"},
		{ErrDuplicateID, "duplicate_id"}, {ErrDuplicateKey, "duplicate_key"},
		{ErrMutationLimit, "mutation_limit"}, {ErrDurability, "durability"},
		{ErrCorrupt, "corrupt"}, {ErrInvalidDocument, "invalid_document"},
		{ErrInvalidFilter, "invalid_filter"}, {ErrInvalidUpdate, "invalid_update"},
		{ErrInvalidIndex, "invalid_index"},
	}
	for _, class := range classes {
		if errors.Is(err, class.err) {
			return DiagnosticFailure, class.name
		}
	}
	return DiagnosticFailure, "other"
}
