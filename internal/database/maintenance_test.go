package database

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestMaintenanceRunsOnlineAndStops(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "maintenance.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	id, err := collection.InsertOne(context.Background(), Document{"value": Int(0)})
	if err != nil {
		t.Fatal(err)
	}
	for revision := 1; revision <= 4; revision++ {
		if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": int64(revision)}}); err != nil {
			t.Fatal(err)
		}
	}
	maintenance, err := db.StartMaintenance(context.Background(), MaintenanceOptions{
		Interval: time.Hour, Timeout: 5 * time.Second, MaxAttempts: 1, RunImmediately: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	for maintenance.Stats().Completed == 0 {
		select {
		case <-deadline:
			t.Fatalf("maintenance stats=%+v", maintenance.Stats())
		case <-time.After(10 * time.Millisecond):
		}
	}
	maintenance.Stop()
	stats := maintenance.Stats()
	if stats.Runs != 1 || stats.Completed != 1 || stats.Conflicts != 0 || stats.Failed != 0 || stats.Active || stats.LastDuration <= 0 || stats.LastError != "" {
		t.Fatalf("maintenance stats=%+v", stats)
	}
	if storage := db.Stats().Storage; storage.PersistentFreeSpace || storage.ReusablePages == 0 {
		t.Fatalf("storage after maintenance=%+v", storage)
	}
}

func TestMaintenanceValidationAndDBCloseLifecycle(t *testing.T) {
	memory := New()
	defer memory.Close()
	if _, err := memory.StartMaintenance(context.Background(), MaintenanceOptions{}); !errors.Is(err, ErrReclamationUnsupported) {
		t.Fatalf("memory maintenance error=%v", err)
	}

	db, err := Open(filepath.Join(t.TempDir(), "maintenance-close.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	for _, options := range []MaintenanceOptions{
		{Interval: time.Millisecond, Timeout: time.Second},
		{Interval: time.Second, Timeout: time.Millisecond},
		{Interval: time.Second, Timeout: time.Second, MaxAttempts: 33},
	} {
		if _, err := db.StartMaintenance(context.Background(), options); !errors.Is(err, ErrInvalidReclamationOptions) {
			t.Fatalf("options=%+v error=%v", options, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.StartMaintenance(ctx, MaintenanceOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled maintenance error=%v", err)
	}
	maintenance, err := db.StartMaintenance(context.Background(), MaintenanceOptions{Interval: time.Hour, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-maintenance.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("DB close did not stop maintenance")
	}
	maintenance.Stop()
	if _, err := db.StartMaintenance(context.Background(), MaintenanceOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed DB maintenance error=%v", err)
	}
}
