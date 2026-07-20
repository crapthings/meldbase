package meldbase

import (
	"errors"
	"sync"
	"testing"
)

func TestReplicationSourceLeaseIsSingleActiveAndReleasable(t *testing.T) {
	db := New()
	defer db.Close()
	lease, err := db.AcquireReplicationSourceLease("replica_a")
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, err := db.AcquireReplicationSourceLease("replica_a"); duplicate != nil || !errors.Is(err, ErrReplicaSourceActive) {
		t.Fatalf("duplicate lease=%v err=%v", duplicate, err)
	}
	if other, err := db.AcquireReplicationSourceLease("replica_b"); err != nil {
		t.Fatal(err)
	} else {
		other.Release()
	}
	lease.Release()
	lease.Release()
	again, err := db.AcquireReplicationSourceLease("replica_a")
	if err != nil {
		t.Fatal(err)
	}
	again.Release()
	if invalid, err := db.AcquireReplicationSourceLease("bad/name"); invalid != nil || !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("invalid lease=%v err=%v", invalid, err)
	}
}

func TestReplicationSourceLeaseConcurrentAcquireAdmitsExactlyOne(t *testing.T) {
	db := New()
	defer db.Close()
	const contenders = 32
	ready := make(chan struct{})
	release := make(chan struct{})
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(contenders)
	done.Add(contenders)
	results := make(chan error, contenders)
	for range contenders {
		go func() {
			defer done.Done()
			start.Done()
			<-ready
			lease, err := db.AcquireReplicationSourceLease("same_replica")
			if err == nil {
				results <- nil
				<-release
				lease.Release()
				return
			}
			results <- err
		}()
	}
	start.Wait()
	close(ready)
	successes, active := 0, 0
	var unexpected error
	for range contenders {
		err := <-results
		if err == nil {
			successes++
		} else if errors.Is(err, ErrReplicaSourceActive) {
			active++
		} else {
			unexpected = err
		}
	}
	close(release)
	done.Wait()
	if unexpected != nil {
		t.Fatalf("unexpected acquire error: %v", unexpected)
	}
	if successes != 1 || active != contenders-1 {
		t.Fatalf("successes=%d active=%d", successes, active)
	}
}
