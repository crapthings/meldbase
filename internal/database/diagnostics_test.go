package database

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDiagnosticsAreOptInBoundedAndContainNoUserData(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "diagnostics.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.Stats().Diagnostics.Enabled {
		t.Fatal("diagnostics enabled by default")
	}

	diagnostics, err := db.EnableDiagnostics(DiagnosticsOptions{Capacity: 2, RecordAll: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.EnableDiagnostics(DiagnosticsOptions{}); !errors.Is(err, ErrDiagnosticsActive) {
		t.Fatalf("second diagnostics handle error=%v", err)
	}

	collection := db.Collection("secret_records")
	id, err := collection.InsertOne(context.Background(), Document{"private_field": String("private-value")})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		if _, err := collection.FindOne(context.Background(), Filter{"_id": id}); err != nil {
			t.Fatal(err)
		}
	}

	snapshot := diagnostics.Snapshot()
	if len(snapshot.Events) != 2 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if snapshot.Stats.Recorded != 3 || snapshot.Stats.Overwritten != 1 || snapshot.Events[0].Sequence != 2 || snapshot.Events[1].Sequence != 3 {
		t.Fatalf("bounded diagnostics stats=%+v events=%+v", snapshot.Stats, snapshot.Events)
	}
	if snapshot.Events[0].Kind != DiagnosticQuery || snapshot.Events[0].Stage != "ID_LOOKUP" || snapshot.Events[1].DocumentsReturned != 1 {
		t.Fatalf("query diagnostics=%+v", snapshot.Events)
	}
	wire, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret_records", "private_field", "private-value", id.String()} {
		if strings.Contains(string(wire), secret) {
			t.Fatalf("diagnostics leaked %q: %s", secret, wire)
		}
	}
	if stats := db.Stats().Diagnostics; !stats.Enabled || stats.Retained != 2 || stats.QueriesObserved != 2 || stats.CommitsObserved != 1 {
		t.Fatalf("DB diagnostic stats=%+v", stats)
	}

	if err := diagnostics.Close(); err != nil {
		t.Fatal(err)
	}
	if diagnostics.Snapshot().Stats.Enabled || db.Stats().Diagnostics.Enabled {
		t.Fatal("diagnostics remained enabled after close")
	}
	if err := diagnostics.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDiagnosticsSamplingThresholdsAndFailureClasses(t *testing.T) {
	db := New()
	defer db.Close()
	diagnostics, err := db.EnableDiagnostics(DiagnosticsOptions{
		Capacity: 8, SlowQueryThreshold: time.Hour, SlowCommitThreshold: time.Hour,
		SampleEvery: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		span := db.beginDiagnostic(DiagnosticQuery)
		db.finishQueryDiagnostic(span, ExplainResult{Stage: "IXSCAN", DocumentsExamined: 2}, 1, nil)
	}
	events := diagnostics.Snapshot().Events
	if len(events) != 1 || !events[0].Sampled || events[0].Slow || events[0].Outcome != DiagnosticSuccess {
		t.Fatalf("sampled events=%+v", events)
	}

	failure := db.beginDiagnostic(DiagnosticQuery)
	db.finishQueryDiagnostic(failure, ExplainResult{Stage: "user-controlled-stage"}, 0, fmtWrap(ErrInvalidFilter))
	events = diagnostics.Snapshot().Events
	last := events[len(events)-1]
	if last.Outcome != DiagnosticFailure || last.ErrorClass != "invalid_filter" || last.Stage != "UNKNOWN" {
		t.Fatalf("failure event=%+v", last)
	}

	canceled := db.beginDiagnostic(DiagnosticCommit)
	db.finishCommitDiagnostic(canceled, 1, context.DeadlineExceeded)
	last = diagnostics.Snapshot().Events[len(diagnostics.Snapshot().Events)-1]
	if last.Outcome != DiagnosticCanceled || last.ErrorClass != "deadline" {
		t.Fatalf("canceled event=%+v", last)
	}
}

func fmtWrap(err error) error {
	return errors.Join(errors.New("opaque detail that must not be retained"), err)
}

func TestDiagnosticsExcludeFailuresAndRecordSlowOperations(t *testing.T) {
	db := New()
	defer db.Close()
	diagnostics, err := db.EnableDiagnostics(DiagnosticsOptions{
		Capacity: 4, SlowQueryThreshold: time.Millisecond,
		SlowCommitThreshold: time.Millisecond, ExcludeFailures: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	failure := db.beginDiagnostic(DiagnosticQuery)
	db.finishQueryDiagnostic(failure, ExplainResult{Stage: "COLLSCAN"}, 0, ErrCorrupt)
	if got := diagnostics.Snapshot().Events; len(got) != 0 {
		t.Fatalf("excluded failure events=%+v", got)
	}

	slow := db.beginDiagnostic(DiagnosticQuery)
	slow.started = time.Now().Add(-2 * time.Millisecond)
	db.finishQueryDiagnostic(slow, ExplainResult{Stage: "COLLSCAN", DocumentsExamined: 7}, 2, nil)
	events := diagnostics.Snapshot().Events
	if len(events) != 1 || !events[0].Slow || events[0].DocumentsExamined != 7 || events[0].DocumentsReturned != 2 {
		t.Fatalf("slow event=%+v", events)
	}
}

func TestDiagnosticsConcurrentSnapshotAndClose(t *testing.T) {
	db := New()
	diagnostics, err := db.EnableDiagnostics(DiagnosticsOptions{Capacity: 64, RecordAll: true})
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 500; iteration++ {
				span := db.beginDiagnostic(DiagnosticQuery)
				db.finishQueryDiagnostic(span, ExplainResult{Stage: "COLLSCAN", DocumentsExamined: 1}, 1, nil)
				_ = diagnostics.Snapshot()
			}
		}()
	}
	time.Sleep(time.Millisecond)
	if err := diagnostics.Close(); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	snapshot := diagnostics.Snapshot()
	if snapshot.Stats.Enabled || snapshot.Stats.Retained > snapshot.Stats.Capacity || len(snapshot.Events) > 64 {
		t.Fatalf("closed concurrent snapshot=%+v", snapshot.Stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStreamingCursorRecordsExecutionLifecycle(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "diagnostics.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	for index := 0; index < 4; index++ {
		if _, err := collection.InsertOne(context.Background(), Document{"n": Int(int64(index))}); err != nil {
			t.Fatal(err)
		}
	}
	diagnostics, err := db.EnableDiagnostics(DiagnosticsOptions{Capacity: 4, RecordAll: true})
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := collection.Find(context.Background(), Filter{"n": map[string]any{"$gte": int64(0)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, exists, err := cursor.Next(context.Background()); err != nil || !exists {
		t.Fatalf("next exists=%t err=%v", exists, err)
	}
	if events := diagnostics.Snapshot().Events; len(events) != 0 {
		t.Fatalf("stream recorded before close: %+v", events)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	events := diagnostics.Snapshot().Events
	if len(events) != 1 || events[0].Stage != "COLLSCAN" || events[0].DocumentsExamined != 1 || events[0].DocumentsReturned != 1 {
		t.Fatalf("stream lifecycle event=%+v", events)
	}
}

func TestDiagnosticsConfigurationValidation(t *testing.T) {
	db := New()
	defer db.Close()
	for _, options := range []DiagnosticsOptions{
		{Capacity: -1}, {Capacity: 65_537}, {SlowQueryThreshold: -1}, {SlowCommitThreshold: -1},
	} {
		if _, err := db.EnableDiagnostics(options); err == nil {
			t.Fatalf("accepted options=%+v", options)
		}
	}
}

func TestDiagnosticSnapshotAfterReportsLossAndPagination(t *testing.T) {
	db := New()
	defer db.Close()
	diagnostics, err := db.EnableDiagnostics(DiagnosticsOptions{Capacity: 2, RecordAll: true})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 4; index++ {
		span := db.beginDiagnostic(DiagnosticQuery)
		db.finishQueryDiagnostic(span, ExplainResult{Stage: "COLLSCAN"}, index, nil)
	}

	page := diagnostics.SnapshotAfter(1, 1)
	if !page.Truncated || !page.HasMore || len(page.Events) != 1 || page.Events[0].Sequence != 3 {
		t.Fatalf("first page=%+v", page)
	}
	next := diagnostics.SnapshotAfter(page.Events[0].Sequence, 1)
	if next.Truncated || next.HasMore || len(next.Events) != 1 || next.Events[0].Sequence != 4 {
		t.Fatalf("next page=%+v", next)
	}
	empty := diagnostics.SnapshotAfter(4, 1)
	if empty.Truncated || empty.HasMore || len(empty.Events) != 0 {
		t.Fatalf("empty page=%+v", empty)
	}
}

func TestDiagnosticHooksDoNotAllocate(t *testing.T) {
	for _, mode := range []string{"disabled", "enabled_filtered", "enabled_record_all"} {
		t.Run(mode, func(t *testing.T) {
			db := New()
			defer db.Close()
			var diagnostics *Diagnostics
			var err error
			switch mode {
			case "enabled_filtered":
				diagnostics, err = db.EnableDiagnostics(DiagnosticsOptions{
					Capacity: 1, SlowQueryThreshold: time.Hour, SlowCommitThreshold: time.Hour,
					ExcludeFailures: true,
				})
			case "enabled_record_all":
				diagnostics, err = db.EnableDiagnostics(DiagnosticsOptions{Capacity: 1, RecordAll: true})
			}
			if err != nil {
				t.Fatal(err)
			}
			if diagnostics != nil {
				defer diagnostics.Close()
			}
			allocations := testing.AllocsPerRun(1_000, func() {
				span := db.beginDiagnostic(DiagnosticQuery)
				db.finishQueryDiagnostic(span, ExplainResult{Stage: "ID_LOOKUP", DocumentsExamined: 1}, 1, nil)
			})
			if allocations != 0 {
				t.Fatalf("diagnostic hook allocations=%f", allocations)
			}
		})
	}
}

// This gate is opt-in because scheduler noise makes nanosecond p99 assertions
// unsuitable for arbitrary shared CI hosts. Release runners enable it explicitly.
func TestDiagnosticsPerformanceBudget(t *testing.T) {
	if os.Getenv("MELDBASE_PERF_GATE") == "" {
		t.Skip("set MELDBASE_PERF_GATE=1 on a dedicated performance runner")
	}
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	id := DocumentID{1}
	if _, err := collection.InsertOne(context.Background(), Document{"_id": ID(id), "n": Int(1)}); err != nil {
		t.Fatal(err)
	}
	query, err := CompileQuery(Filter{"_id": id}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	previousGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previousGC)
	disabledRounds := make([]time.Duration, 7)
	enabledRounds := make([]time.Duration, 7)
	measureDisabled := func() time.Duration {
		warmDiagnosticBudgetQueries(t, collection, query, 2_000)
		return diagnosticQueryP99(t, collection, query, 10_000)
	}
	measureEnabled := func() time.Duration {
		diagnostics, err := db.EnableDiagnostics(DiagnosticsOptions{
			Capacity: 1, SlowQueryThreshold: time.Hour, SlowCommitThreshold: time.Hour,
			ExcludeFailures: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		warmDiagnosticBudgetQueries(t, collection, query, 2_000)
		result := diagnosticQueryP99(t, collection, query, 10_000)
		if err := diagnostics.Close(); err != nil {
			t.Fatal(err)
		}
		return result
	}
	for round := range disabledRounds {
		if round%2 == 0 {
			disabledRounds[round] = measureDisabled()
			enabledRounds[round] = measureEnabled()
		} else {
			enabledRounds[round] = measureEnabled()
			disabledRounds[round] = measureDisabled()
		}
	}
	sort.Slice(disabledRounds, func(left, right int) bool { return disabledRounds[left] < disabledRounds[right] })
	sort.Slice(enabledRounds, func(left, right int) bool { return enabledRounds[left] < enabledRounds[right] })
	disabledP99 := disabledRounds[len(disabledRounds)/2]
	enabledP99 := enabledRounds[len(enabledRounds)/2]
	if enabledP99 > disabledP99*135/100 {
		t.Fatalf("filtered diagnostics p99=%s disabled=%s overhead=%.1f%%", enabledP99, disabledP99, (float64(enabledP99)/float64(disabledP99)-1)*100)
	}
	t.Logf("filtered diagnostics p99=%s disabled=%s overhead=%.1f%%", enabledP99, disabledP99, (float64(enabledP99)/float64(disabledP99)-1)*100)
}

func warmDiagnosticBudgetQueries(t *testing.T, collection *Collection, query QuerySpec, count int) {
	t.Helper()
	for iteration := 0; iteration < count; iteration++ {
		if !runDiagnosticBudgetQuery(collection, query) {
			t.Fatal("diagnostic budget warmup query failed")
		}
	}
}

func diagnosticQueryP99(t *testing.T, collection *Collection, query QuerySpec, count int) time.Duration {
	t.Helper()
	latencies := make([]time.Duration, count)
	for iteration := range latencies {
		started := time.Now()
		if !runDiagnosticBudgetQuery(collection, query) {
			t.Fatal("budget query failed")
		}
		latencies[iteration] = time.Since(started)
	}
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	return latencies[(len(latencies)-1)*99/100]
}

func runDiagnosticBudgetQuery(collection *Collection, query QuerySpec) bool {
	cursor, err := collection.FindQuery(context.Background(), query)
	if err != nil {
		return false
	}
	documents, err := cursor.All(context.Background())
	return err == nil && len(documents) == 1
}

func TestDatabaseDiagnosticSourceFollowsReplacementSession(t *testing.T) {
	db := New()
	defer db.Close()
	first, err := db.EnableDiagnostics(DiagnosticsOptions{Capacity: 1, RecordAll: true})
	if err != nil {
		t.Fatal(err)
	}
	span := db.beginDiagnostic(DiagnosticQuery)
	db.finishQueryDiagnostic(span, ExplainResult{Stage: "COLLSCAN"}, 0, nil)
	firstSnapshot := db.DiagnosticSnapshotAfter(0, 1)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := db.EnableDiagnostics(DiagnosticsOptions{Capacity: 1, RecordAll: true})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	span = db.beginDiagnostic(DiagnosticQuery)
	db.finishQueryDiagnostic(span, ExplainResult{Stage: "ID_LOOKUP"}, 1, nil)
	secondSnapshot := db.DiagnosticSnapshotAfter(0, 1)
	if firstSnapshot.Session == secondSnapshot.Session || secondSnapshot.Session == 0 || len(secondSnapshot.Events) != 1 || secondSnapshot.Events[0].Sequence != 1 {
		t.Fatalf("first=%+v second=%+v", firstSnapshot, secondSnapshot)
	}
}
