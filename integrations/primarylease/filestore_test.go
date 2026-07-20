package primarylease_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/crapthings/meldbase/integrations/primarylease"
)

func TestFileStoreDurableCASReopenAndNamespace(t *testing.T) {
	directory := t.TempDir()
	store, err := primarylease.NewFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	firstID, secondID := [16]byte{10: 1}, [16]byte{10: 2}
	first := primarylease.LeaseRecord{DatabaseID: firstID, Owner: "writer-a", Epoch: 1, CommitSequence: 7, NotAfter: now.Add(time.Minute)}
	if swapped, err := store.CompareAndSwapPrimaryLease(context.Background(), firstID, nil, first); err != nil || !swapped {
		t.Fatalf("first CAS swapped=%t err=%v", swapped, err)
	}
	if record, exists, err := store.LoadPrimaryLease(context.Background(), firstID); err != nil || !exists || record != first {
		t.Fatalf("first load record=%+v exists=%t err=%v", record, exists, err)
	}
	if record, exists, err := store.LoadPrimaryLease(context.Background(), secondID); err != nil || exists || record != (primarylease.LeaseRecord{}) {
		t.Fatalf("other namespace record=%+v exists=%t err=%v", record, exists, err)
	}
	second := primarylease.LeaseRecord{DatabaseID: secondID, Owner: "writer-b", Epoch: 1, CommitSequence: 3, NotAfter: now.Add(time.Minute)}
	if swapped, err := store.CompareAndSwapPrimaryLease(context.Background(), secondID, nil, second); err != nil || !swapped {
		t.Fatalf("second namespace CAS swapped=%t err=%v", swapped, err)
	}
	reopened, err := primarylease.NewFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if record, exists, err := reopened.LoadPrimaryLease(context.Background(), firstID); err != nil || !exists || record != first {
		t.Fatalf("reopened record=%+v exists=%t err=%v", record, exists, err)
	}
	path := filepath.Join(directory, hex.EncodeToString(firstID[:])+".lease")
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("record file mode=%v err=%v", info.Mode(), err)
	}
	if swapped, err := reopened.CompareAndSwapPrimaryLease(context.Background(), firstID, nil, first); err != nil || swapped {
		t.Fatalf("create over existing swapped=%t err=%v", swapped, err)
	}
	wrongPrevious := first
	wrongPrevious.Owner = "writer-z"
	if swapped, err := reopened.CompareAndSwapPrimaryLease(context.Background(), firstID, &wrongPrevious, first); err != nil || swapped {
		t.Fatalf("wrong previous swapped=%t err=%v", swapped, err)
	}
}

func TestFileStoreSerializesIndependentInstances(t *testing.T) {
	directory := t.TempDir()
	first, err := primarylease.NewFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	second, err := primarylease.NewFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{10: 3}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	records := []primarylease.LeaseRecord{
		{DatabaseID: databaseID, Owner: "writer-a", Epoch: 1, NotAfter: now.Add(time.Minute)},
		{DatabaseID: databaseID, Owner: "writer-b", Epoch: 1, NotAfter: now.Add(time.Minute)},
	}
	stores := []*primarylease.FileStore{first, second}
	start := make(chan struct{})
	results := make([]bool, len(stores))
	errorsByWriter := make([]error, len(stores))
	var writers sync.WaitGroup
	for index := range stores {
		writers.Add(1)
		go func(index int) {
			defer writers.Done()
			<-start
			results[index], errorsByWriter[index] = stores[index].CompareAndSwapPrimaryLease(context.Background(), databaseID, nil, records[index])
		}(index)
	}
	close(start)
	writers.Wait()
	succeeded := 0
	for index, err := range errorsByWriter {
		if err != nil {
			t.Fatalf("writer %d err=%v", index, err)
		}
		if results[index] {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("writers succeeded=%d results=%v", succeeded, results)
	}
	loaded, exists, err := first.LoadPrimaryLease(context.Background(), databaseID)
	if err != nil || !exists || (loaded != records[0] && loaded != records[1]) {
		t.Fatalf("serialized record=%+v exists=%t err=%v", loaded, exists, err)
	}
}

func TestFileStoreRejectsCorruptionPermissionsAndCancelledWrites(t *testing.T) {
	directory := t.TempDir()
	store, err := primarylease.NewFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{10: 4}
	record := primarylease.LeaseRecord{DatabaseID: databaseID, Owner: "writer-a", Epoch: 1, NotAfter: time.Now().UTC().Add(time.Minute)}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if swapped, err := store.CompareAndSwapPrimaryLease(cancelled, databaseID, nil, record); swapped || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled CAS swapped=%t err=%v", swapped, err)
	}
	if _, exists, err := store.LoadPrimaryLease(context.Background(), databaseID); err != nil || exists {
		t.Fatalf("cancelled CAS published record exists=%t err=%v", exists, err)
	}
	if swapped, err := store.CompareAndSwapPrimaryLease(context.Background(), databaseID, nil, record); err != nil || !swapped {
		t.Fatalf("seed CAS swapped=%t err=%v", swapped, err)
	}
	path := filepath.Join(directory, hex.EncodeToString(databaseID[:])+".lease")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 1
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.LoadPrimaryLease(context.Background(), databaseID); !errors.Is(err, primarylease.ErrLeaseStore) {
		t.Fatalf("corrupt record err=%v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.LoadPrimaryLease(context.Background(), databaseID); !errors.Is(err, primarylease.ErrLeaseStore) {
		t.Fatalf("public mode record err=%v", err)
	}
}

func TestFileStoreIgnoresUnpublishedTemporaryFiles(t *testing.T) {
	directory := t.TempDir()
	store, err := primarylease.NewFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{10: 5}
	if err := os.WriteFile(filepath.Join(directory, "."+hex.EncodeToString(databaseID[:])+".lease.tmp-interrupted"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if record, exists, err := store.LoadPrimaryLease(context.Background(), databaseID); err != nil || exists || record != (primarylease.LeaseRecord{}) {
		t.Fatalf("temporary file became published record=%+v exists=%t err=%v", record, exists, err)
	}
}

func TestAuthorityPersistsCertificateStateThroughFileQuorum(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	directories := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	replicas := make([]primarylease.QuorumReplica, 0, len(directories))
	for index, directory := range directories {
		store, err := primarylease.NewFileStore(directory)
		if err != nil {
			t.Fatal(err)
		}
		replicas = append(replicas, primarylease.QuorumReplica{MemberID: "member-" + string(rune('a'+index)), Store: store})
	}
	quorum, err := primarylease.NewQuorumStore(replicas)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	authority, err := primarylease.NewAuthority(primarylease.AuthorityOptions{Store: quorum, PrivateKey: privateKey, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{10: 6}
	grant, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-a", CommitSequence: 9})
	if err != nil || grant.Certificate == "" || grant.Record.Epoch != 1 {
		t.Fatalf("authority grant=%+v err=%v", grant, err)
	}
	reopenedReplicas := make([]primarylease.QuorumReplica, 0, len(directories))
	for index, directory := range directories {
		store, err := primarylease.NewFileStore(directory)
		if err != nil {
			t.Fatal(err)
		}
		reopenedReplicas = append(reopenedReplicas, primarylease.QuorumReplica{MemberID: "member-" + string(rune('a'+index)), Store: store})
	}
	reopenedQuorum, err := primarylease.NewQuorumStore(reopenedReplicas)
	if err != nil {
		t.Fatal(err)
	}
	if record, exists, err := reopenedQuorum.LoadPrimaryLease(context.Background(), databaseID); err != nil || !exists || record != grant.Record {
		t.Fatalf("reopened quorum record=%+v exists=%t err=%v", record, exists, err)
	}
}
