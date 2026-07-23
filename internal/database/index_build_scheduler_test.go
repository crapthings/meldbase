package database

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestIndexBuildSchedulerCompletesDurableBuild(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "scheduler.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	for value := int64(0); value < 50; value++ {
		if _, err := items.InsertOne(context.Background(), Document{"value": Int(value)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := items.StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	scheduler, err := db.StartIndexBuildScheduler(context.Background(), IndexBuildSchedulerOptions{
		PollInterval: 10 * time.Millisecond, RunTimeout: time.Second, RunImmediately: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scheduler.Stop)
	waitForIndexBuildCondition(t, time.Second, func() bool {
		builds, err := db.IndexBuilds()
		return err == nil && len(builds) == 0 && scheduler.Stats().Completed == 1
	})
	stats := scheduler.Stats()
	if stats.Completed != 1 || stats.Runs == 0 || stats.Active != 0 {
		t.Fatalf("scheduler stats=%+v", stats)
	}
	explain, err := items.Explain(context.Background(), Filter{"value": int64(49)})
	if err != nil || explain.Stage != "IXSCAN" || explain.IndexName != "by_value" {
		t.Fatalf("explain=%+v err=%v", explain, err)
	}
}

func TestIndexBuildSchedulerMarksTerminalFailureAndSkipsIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failed-scheduler.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	for range 2 {
		if _, err := items.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
			t.Fatal(err)
		}
	}
	id, err := items.StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := db.StartIndexBuildScheduler(context.Background(), IndexBuildSchedulerOptions{
		PollInterval: 10 * time.Millisecond, RunTimeout: time.Second, RunImmediately: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForIndexBuildCondition(t, time.Second, func() bool {
		status, err := db.IndexBuild(id)
		return err == nil && status.Phase == IndexBuildPhaseFailed
	})
	runs := scheduler.Stats().Runs
	time.Sleep(40 * time.Millisecond)
	if stats := scheduler.Stats(); stats.MarkedFailed != 1 || stats.Runs != runs {
		t.Fatalf("failed scheduler stats=%+v initialRuns=%d", stats, runs)
	}
	scheduler.Stop()
	status, err := db.IndexBuild(id)
	if err != nil || status.Failure != IndexBuildFailureUniqueConflict {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	if stats := db.Stats().IndexBuilds; stats.Persistent != 1 || stats.PersistentFailed != 1 || stats.SchedulerFailures != 1 {
		t.Fatalf("persistent failure stats=%+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	status, err = db.IndexBuild(id)
	if err != nil || status.Phase != IndexBuildPhaseFailed || status.Failure != IndexBuildFailureUniqueConflict {
		t.Fatalf("reopened status=%+v err=%v", status, err)
	}
	if err := db.ResumeIndexBuild(context.Background(), id); !errors.Is(err, ErrIndexBuildFailed) {
		t.Fatalf("resume failed build=%v", err)
	}
}

func TestIndexBuildSchedulerStopYieldsWithoutFailingBuild(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "stop-scheduler.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	id, err := items.StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	store := db.durability.(*durableStore)
	store.testPersistentIndexBuildBatchHook = func(ctx context.Context, _ IndexBuildID) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-ctx.Done()
	}
	scheduler, err := db.StartIndexBuildScheduler(context.Background(), IndexBuildSchedulerOptions{
		PollInterval: 10 * time.Millisecond, RunTimeout: time.Hour, RunImmediately: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	scheduler.Stop()
	store.testPersistentIndexBuildBatchHook = nil
	status, err := db.IndexBuild(id)
	if err != nil || status.Phase != IndexBuildPhaseScan || status.Failure != IndexBuildFailureNone {
		t.Fatalf("stopped status=%+v err=%v", status, err)
	}
	if stats := scheduler.Stats(); stats.Active != 0 || stats.MarkedFailed != 0 || stats.Failed != 0 {
		t.Fatalf("stopped scheduler stats=%+v", stats)
	}
	second, err := db.StartIndexBuildScheduler(context.Background(), IndexBuildSchedulerOptions{PollInterval: time.Hour})
	if err != nil {
		t.Fatalf("restart scheduler=%v", err)
	}
	second.Stop()
}

func TestIndexBuildSchedulerRejectsInvalidAndOverlappingStarts(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "scheduler-options.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.StartIndexBuildScheduler(context.Background(), IndexBuildSchedulerOptions{MaxConcurrency: 9}); !errors.Is(err, ErrInvalidIndexBuildSchedulerOptions) {
		t.Fatalf("invalid options=%v", err)
	}
	first, err := db.StartIndexBuildScheduler(context.Background(), IndexBuildSchedulerOptions{PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.StartIndexBuildScheduler(context.Background(), IndexBuildSchedulerOptions{}); !errors.Is(err, ErrIndexBuildSchedulerRunning) {
		t.Fatalf("overlapping scheduler=%v", err)
	}
	first.Stop()
}

func TestIndexBuildSchedulerTimeSlicesRoundRobinWithoutStarvation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "scheduler-fairness.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"a": Int(1), "b": Int(2)}); err != nil {
		t.Fatal(err)
	}
	first, err := items.StartIndexBuild(context.Background(), "by_a", []IndexField{{Field: "a", Order: 1}}, IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := items.StartIndexBuild(context.Background(), "by_b", []IndexField{{Field: "b", Order: 1}}, IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan IndexBuildID, 8)
	store := db.durability.(*durableStore)
	store.testPersistentIndexBuildBatchHook = func(ctx context.Context, id IndexBuildID) {
		seen <- id
		<-ctx.Done()
	}
	scheduler, err := db.StartIndexBuildScheduler(context.Background(), IndexBuildSchedulerOptions{
		PollInterval: 10 * time.Millisecond, RunTimeout: 10 * time.Millisecond,
		MaxConcurrency: 1, RunImmediately: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	observed := make(map[IndexBuildID]bool)
	deadline := time.After(time.Second)
	for len(observed) < 2 {
		select {
		case id := <-seen:
			observed[id] = true
		case <-deadline:
			t.Fatalf("starved builds: first=%t second=%t stats=%+v", observed[first], observed[second], scheduler.Stats())
		}
	}
	waitForIndexBuildCondition(t, time.Second, func() bool { return scheduler.Stats().Yielded >= 2 })
	scheduler.Stop()
	store.testPersistentIndexBuildBatchHook = nil
	if !observed[first] || !observed[second] || scheduler.Stats().Yielded < 2 {
		t.Fatalf("fairness observed=%v stats=%+v", observed, scheduler.Stats())
	}
}

func waitForIndexBuildCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for index build condition")
}
