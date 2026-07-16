package v2

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const sizedRetentionCommitBytes = uint64(commitHeaderBytes + 52 + len("items") + 100)

func TestRetentionBytePruneAndBusinessCommitShareCrashBoundary(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "retention-byte-crash-base.meld2")
	options := OpenOptions{CommitRetentionMaxCommits: 100, CommitRetentionMaxBytes: 400}
	base, _, _, err := OpenWithOptions(basePath, options)
	if err != nil {
		t.Fatal(err)
	}
	applySizedRetentionCommit(t, base, 1)
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	baseBytes, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, point := range []faultPoint{faultAfterPageWrite, faultBeforeDataSync, faultAfterDataSync, faultAfterMetaWrite, faultAfterMetaSync} {
		t.Run(fmt.Sprint(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "candidate.meld2")
			if err := os.WriteFile(path, baseBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			candidate, _, _, err := OpenWithOptions(path, options)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("retention byte publication crash")
			candidate.fault = func(current faultPoint) error {
				if current == point {
					return injected
				}
				return nil
			}
			if err := applySizedRetentionCommitError(candidate, 2); !errors.Is(err, injected) {
				t.Fatalf("apply error=%v", err)
			}
			if err := candidate.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, _, _, err := OpenWithOptions(path, options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			root, err := reopened.DatabaseRoot()
			if err != nil {
				t.Fatal(err)
			}
			if !((root.CommitSequence == 1 && root.OldestRetainedSequence == 1) ||
				(root.CommitSequence == 2 && root.OldestRetainedSequence == 2)) {
				t.Fatalf("split byte retention root=%+v", root)
			}
			if stats := reopened.StorageStats(); stats.RetainedCommitBytes != sizedRetentionCommitBytes {
				t.Fatalf("recovered byte stats=%+v", stats)
			}
			if _, err := reopened.Reachability(); err != nil {
				t.Fatalf("recovered graph: %v", err)
			}
		})
	}
}

func TestCommitRetentionByteBudgetAdvancesAndRebuildsOnOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retention-bytes.meld2")
	options := OpenOptions{CommitRetentionMaxCommits: 100, CommitRetentionMaxBytes: 400}
	file, _, _, err := OpenWithOptions(path, options)
	if err != nil {
		t.Fatal(err)
	}
	for sequence := byte(1); sequence <= 3; sequence++ {
		applySizedRetentionCommit(t, file, sequence)
	}
	stats := file.StorageStats()
	if stats.OldestRetainedSequence != 3 || stats.RetainedCommits != 1 ||
		stats.RetainedCommitBytes != sizedRetentionCommitBytes || stats.CommitRetentionMaxBytes != 400 ||
		stats.CommitRetentionByteOverage != 0 || stats.RetentionPrunedCommits != 2 || stats.RetentionPressure {
		t.Fatalf("byte retention stats=%+v", stats)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, _, err := OpenWithOptions(path, options)
	if err != nil {
		t.Fatal(err)
	}
	if stats := reopened.StorageStats(); stats.RetainedCommitBytes != sizedRetentionCommitBytes || stats.CommitRetentionMaxBytes != 400 {
		t.Fatalf("rebuilt byte stats=%+v", stats)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, _, err = OpenWithOptions(path, OpenOptions{CommitRetentionMaxCommits: 100, CommitRetentionMaxBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if stats := reopened.StorageStats(); !stats.RetentionPressure || stats.CommitRetentionByteOverage != sizedRetentionCommitBytes-100 {
		t.Fatalf("smaller reopened budget stats=%+v", stats)
	}
}

func TestReplayPinWinsOverRetentionByteBudget(t *testing.T) {
	file, _, _, err := OpenWithOptions(filepath.Join(t.TempDir(), "retention-byte-pin.meld2"), OpenOptions{
		CommitRetentionMaxCommits: 100, CommitRetentionMaxBytes: 400,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	applySizedRetentionCommit(t, file, 1)
	snapshot, stream, err := file.OpenSnapshotAndStreamAt(1)
	if err != nil {
		t.Fatal(err)
	}
	applySizedRetentionCommit(t, file, 2)
	applySizedRetentionCommit(t, file, 3)
	stats := file.StorageStats()
	if stats.OldestRetainedSequence != 2 || stats.RetainedCommits != 2 ||
		stats.RetainedCommitBytes != 2*sizedRetentionCommitBytes || stats.CommitRetentionByteOverage != 2*sizedRetentionCommitBytes-400 ||
		!stats.RetentionPressure || stats.RetentionPressureEvents != 1 {
		t.Fatalf("pinned byte stats=%+v", stats)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	applySizedRetentionCommit(t, file, 4)
	stats = file.StorageStats()
	if stats.OldestRetainedSequence != 4 || stats.RetainedCommits != 1 || stats.RetainedCommitBytes != sizedRetentionCommitBytes ||
		stats.CommitRetentionByteOverage != 0 || stats.RetentionPressure || stats.RetentionPressureEvents != 1 {
		t.Fatalf("released byte stats=%+v", stats)
	}
}

func TestRetentionPruneAndBusinessCommitShareCrashBoundary(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "retention-crash-base.meld2")
	base, _, _, err := OpenWithOptions(basePath, OpenOptions{CommitRetentionMaxCommits: 2})
	if err != nil {
		t.Fatal(err)
	}
	applyRetentionRevision(t, base, 1)
	applyRetentionRevision(t, base, 2)
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	baseBytes, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatal(err)
	}
	points := []faultPoint{faultAfterPageWrite, faultBeforeDataSync, faultAfterDataSync, faultAfterMetaWrite, faultAfterMetaSync}
	for _, point := range points {
		t.Run(fmt.Sprint(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "candidate.meld2")
			if err := os.WriteFile(path, baseBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			candidate, _, _, err := OpenWithOptions(path, OpenOptions{CommitRetentionMaxCommits: 2})
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("retention publication crash")
			candidate.fault = func(current faultPoint) error {
				if current == point {
					return injected
				}
				return nil
			}
			applyErr := applyRetentionRevisionError(candidate, 3)
			if !errors.Is(applyErr, injected) {
				t.Fatalf("apply error=%v", applyErr)
			}
			if err := candidate.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, _, _, err := OpenWithOptions(path, OpenOptions{CommitRetentionMaxCommits: 2})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			root, err := reopened.DatabaseRoot()
			if err != nil {
				t.Fatal(err)
			}
			if !((root.CommitSequence == 2 && root.OldestRetainedSequence == 1) ||
				(root.CommitSequence == 3 && root.OldestRetainedSequence == 2)) {
				t.Fatalf("split retention root=%+v", root)
			}
			if _, err := reopened.Reachability(); err != nil {
				t.Fatalf("recovered graph: %v", err)
			}
		})
	}
}

func TestCommitRetentionWindowAdvancesInsideBusinessPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retention-window.meld2")
	file, _, _, err := OpenWithOptions(path, OpenOptions{CommitRetentionMaxCommits: 3})
	if err != nil {
		t.Fatal(err)
	}
	for revision := byte(1); revision <= 6; revision++ {
		applyRetentionRevision(t, file, revision)
	}
	root, err := file.DatabaseRoot()
	if err != nil {
		t.Fatal(err)
	}
	stats := file.StorageStats()
	if root.CommitSequence != 6 || root.OldestRetainedSequence != 4 || stats.RetainedCommits != 3 ||
		stats.CommitRetentionMax != 3 || stats.CommitRetentionOverage != 0 || stats.RetentionPrunedCommits != 3 ||
		stats.RetentionPressure || stats.RetentionPressureEvents != 0 {
		t.Fatalf("root=%+v stats=%+v", root, stats)
	}
	if _, err := file.ReadCommit(root.CommitLogRoot, 3); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("pruned commit remained readable: %v", err)
	}
	if batch, err := file.ReadCommit(root.CommitLogRoot, 4); err != nil || batch.Sequence != 4 {
		t.Fatalf("oldest retained batch=%+v err=%v", batch, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, _, err := OpenWithOptions(path, OpenOptions{CommitRetentionMaxCommits: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if stats := reopened.StorageStats(); stats.RetainedCommits != 3 || stats.CommitRetentionMax != 3 || stats.CommitRetentionOverage != 0 {
		t.Fatalf("reopened stats=%+v", stats)
	}
}

func TestReplayPinWinsOverRetentionWindowAndPressureClears(t *testing.T) {
	file, _, _, err := OpenWithOptions(filepath.Join(t.TempDir(), "retention-pin.meld2"), OpenOptions{CommitRetentionMaxCommits: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	applyRetentionRevision(t, file, 1)
	applyRetentionRevision(t, file, 2)
	snapshot, stream, err := file.OpenSnapshotAndStreamAt(1)
	if err != nil {
		t.Fatal(err)
	}
	applyRetentionRevision(t, file, 3)
	applyRetentionRevision(t, file, 4)
	stats := file.StorageStats()
	if stats.OldestRetainedSequence != 2 || stats.RetainedCommits != 3 || stats.CommitRetentionOverage != 1 ||
		!stats.RetentionPressure || stats.RetentionPressureEvents != 1 || stats.ActiveReplayLeases != 1 {
		t.Fatalf("pinned stats=%+v", stats)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	applyRetentionRevision(t, file, 5)
	stats = file.StorageStats()
	if stats.OldestRetainedSequence != 4 || stats.RetainedCommits != 2 || stats.CommitRetentionOverage != 0 ||
		stats.RetentionPressure || stats.RetentionPressureEvents != 1 || stats.RetentionPrunedCommits != 3 {
		t.Fatalf("released stats=%+v", stats)
	}
}

func applyRetentionRevision(t *testing.T, file *File, revision byte) {
	t.Helper()
	if err := applyRetentionRevisionError(file, revision); err != nil {
		t.Fatalf("revision %d: %v", revision, err)
	}
}

func applyRetentionRevisionError(file *File, revision byte) error {
	operation := DocumentUpdate
	if revision == 1 {
		operation = DocumentInsert
	}
	transactionID := [16]byte{15: revision}
	_, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: transactionID,
		Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: [16]byte{15: 1}, Operation: operation,
			Document: []byte{'v', revision}, ChangedPaths: []string{"value"},
		}},
	})
	return err
}

func applySizedRetentionCommit(t *testing.T, file *File, sequence byte) {
	t.Helper()
	if err := applySizedRetentionCommitError(file, sequence); err != nil {
		t.Fatal(err)
	}
}

func applySizedRetentionCommitError(file *File, sequence byte) error {
	return file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		base := tx.BaseRoot()
		root, oldest, err := tx.AppendCommitRetained(base.CommitLogRoot, base.OldestRetainedSequence, CommitBatch{
			Sequence: tx.Sequence(), TransactionID: [16]byte{15: sequence}, CommittedAt: time.Unix(int64(sequence), 0),
			Changes: []CommitChange{{CollectionID: 1, CollectionName: "items", Operation: CommitCatalog, After: make([]byte, 100)}},
		})
		if err != nil {
			return DatabaseRoot{}, err
		}
		return DatabaseRoot{CommitSequence: tx.Sequence(), CommitLogRoot: root, OldestRetainedSequence: oldest}, nil
	})
}
