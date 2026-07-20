package meldbase

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

const rollbackAnchorProcessChild = "MELDBASE_ROLLBACK_ANCHOR_PROCESS_CHILD"
const rollbackAnchorGroupProcessChild = "MELDBASE_ROLLBACK_ANCHOR_GROUP_PROCESS_CHILD"

type blockingRollbackAnchorStore struct {
	delegate RollbackAnchorStore
	marker   string
}

func (store *blockingRollbackAnchorStore) Load(ctx context.Context) (RollbackAnchor, bool, error) {
	return store.delegate.Load(ctx)
}

func (store *blockingRollbackAnchorStore) Advance(ctx context.Context, anchor RollbackAnchor) error {
	if anchor.MinimumCommitSequence == 0 {
		return store.delegate.Advance(ctx, anchor)
	}
	if err := os.WriteFile(store.marker, []byte("database-durable-anchor-pending\n"), 0o600); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Dir(store.marker)); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestV2RollbackAnchorProcessKillWindow(t *testing.T) {
	if childDirectory := os.Getenv(rollbackAnchorProcessChild); childDirectory != "" {
		runRollbackAnchorProcessChild(t, childDirectory)
		return
	}
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL validation is Unix-only")
	}
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "database.meld")
	anchorPath := filepath.Join(directory, "database.anchor")
	markerPath := filepath.Join(directory, "anchor-pending")
	anchor, err := NewFileRollbackAnchorStore(anchorPath)
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithOptions(databasePath, OpenOptions{RollbackProtection: V2RollbackProtection{AnchorStore: anchor, InitializeAnchor: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestV2RollbackAnchorProcessKillWindow$")
	command.Env = append(os.Environ(), rollbackAnchorProcessChild+"="+directory)
	var childOutput bytes.Buffer
	command.Stdout, command.Stderr = &childOutput, &childOutput
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(markerPath); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatalf("child did not reach anchor window: %s", childOutput.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("child exited successfully instead of being killed")
	}

	retained, exists, err := anchor.Load(context.Background())
	if err != nil || !exists || retained.MinimumCommitSequence != 0 || retained.MinimumGeneration != 1 {
		t.Fatalf("anchor changed before kill: retained=%+v exists=%t err=%v", retained, exists, err)
	}
	verification, err := VerifyV2File(context.Background(), databasePath)
	if err != nil || verification.CommitSequence != 1 || !verification.Verified {
		t.Fatalf("physical commit verification=%+v err=%v", verification, err)
	}
	reopened, err := OpenWithOptions(databasePath, OpenOptions{RollbackProtection: V2RollbackProtection{AnchorStore: anchor}})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	document, err := reopened.Collection("items").FindOne(context.Background(), Filter{})
	value, ok := document["value"].StringValue()
	if err != nil || !ok || value != "committed-before-kill" {
		t.Fatalf("document=%v value=%q err=%v", document, value, err)
	}
	retained, exists, err = anchor.Load(context.Background())
	if err != nil || !exists || retained.MinimumCommitSequence != 1 || retained.MinimumGeneration != 2 || retained.DatabaseID != reopened.DatabaseIdentity() {
		t.Fatalf("advanced anchor=%+v exists=%t err=%v", retained, exists, err)
	}
}

// TestV2RollbackAnchorGroupProcessKillWindow proves the external acknowledgement
// boundary for an opt-in coordinator group. The process dies after the final
// Meta makes both logical members durable, but before the external anchor is
// advanced. No caller can have received success at that point; reopen must
// retain the whole group and reconcile the anchor to the final coordinates.
func TestV2RollbackAnchorGroupProcessKillWindow(t *testing.T) {
	if childDirectory := os.Getenv(rollbackAnchorGroupProcessChild); childDirectory != "" {
		runRollbackAnchorGroupProcessChild(t, childDirectory)
		return
	}
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL validation is Unix-only")
	}
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "database.meld")
	anchorPath := filepath.Join(directory, "database.anchor")
	markerPath := filepath.Join(directory, "group-anchor-pending")
	anchor, err := NewFileRollbackAnchorStore(anchorPath)
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithOptions(databasePath, OpenOptions{RollbackProtection: V2RollbackProtection{AnchorStore: anchor, InitializeAnchor: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestV2RollbackAnchorGroupProcessKillWindow$")
	command.Env = append(os.Environ(), rollbackAnchorGroupProcessChild+"="+directory)
	var childOutput bytes.Buffer
	command.Stdout, command.Stderr = &childOutput, &childOutput
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(markerPath); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatalf("child did not reach group anchor window: %s", childOutput.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("child exited successfully instead of being killed")
	}

	retained, exists, err := anchor.Load(context.Background())
	if err != nil || !exists || retained.MinimumCommitSequence != 0 || retained.MinimumGeneration != 1 {
		t.Fatalf("anchor changed before group kill: retained=%+v exists=%t err=%v", retained, exists, err)
	}
	verification, err := VerifyV2File(context.Background(), databasePath)
	if err != nil || verification.CommitSequence != 2 || !verification.Verified {
		t.Fatalf("physical group verification=%+v err=%v", verification, err)
	}
	reopened, err := OpenWithOptions(databasePath, OpenOptions{RollbackProtection: V2RollbackProtection{AnchorStore: anchor}})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, value := range []string{"first", "second"} {
		document, err := reopened.Collection("items").FindOne(context.Background(), Filter{"value": String(value)})
		if err != nil {
			t.Fatalf("recovered group document %q=%v err=%v", value, document, err)
		}
	}
	retained, exists, err = anchor.Load(context.Background())
	if err != nil || !exists || retained.MinimumCommitSequence != 2 || retained.MinimumGeneration != 2 || retained.DatabaseID != reopened.DatabaseIdentity() {
		t.Fatalf("advanced group anchor=%+v exists=%t err=%v", retained, exists, err)
	}
}

func runRollbackAnchorProcessChild(t *testing.T, directory string) {
	t.Helper()
	anchor, err := NewFileRollbackAnchorStore(filepath.Join(directory, "database.anchor"))
	if err != nil {
		t.Fatal(err)
	}
	store := &blockingRollbackAnchorStore{delegate: anchor, marker: filepath.Join(directory, "anchor-pending")}
	db, err := OpenWithOptions(filepath.Join(directory, "database.meld"), OpenOptions{RollbackProtection: V2RollbackProtection{AnchorStore: store}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Collection("items").InsertOne(context.Background(), Document{"value": String("committed-before-kill")})
	t.Fatalf("commit unexpectedly returned: %v", err)
}

func runRollbackAnchorGroupProcessChild(t *testing.T, directory string) {
	t.Helper()
	anchor, err := NewFileRollbackAnchorStore(filepath.Join(directory, "database.anchor"))
	if err != nil {
		t.Fatal(err)
	}
	store := &blockingRollbackAnchorStore{delegate: anchor, marker: filepath.Join(directory, "group-anchor-pending")}
	db, err := OpenWithOptions(filepath.Join(directory, "database.meld"), OpenOptions{
		CommitCoordinator:  V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Second},
		RollbackProtection: V2RollbackProtection{AnchorStore: store},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coordinator := db.commitCoordinator
	entered, release := make(chan struct{}), make(chan struct{})
	coordinator.testBeforeCoalesce = func() {
		close(entered)
		<-release
	}
	results := make(chan error, 2)
	go func() {
		_, err := db.Collection("items").InsertOne(context.Background(), Document{"value": String("first")})
		results <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not begin group coalescing")
	}
	go func() {
		_, err := db.Collection("items").InsertOne(context.Background(), Document{"value": String("second")})
		results <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		coordinator.mu.Lock()
		pending := len(coordinator.queue)
		coordinator.mu.Unlock()
		if pending == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second group member was not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	// The anchor writes its parent-visible marker and blocks. A normal return
	// would mean the test process was not killed in the intended window.
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("group commit unexpectedly returned: %v", err)
		}
	}
	t.Fatal("group commit unexpectedly completed")
}
