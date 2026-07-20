package primarylease_test

import (
	"context"
	"errors"
	"math/bits"
	"testing"
	"time"

	"github.com/crapthings/meldbase/integrations/primarylease"
)

type failingLeaseStore struct{}

type identifiedLeaseStore struct {
	primarylease.LeaseStore
	identity string
}

func (store identifiedLeaseStore) PrimaryLeaseMemberIdentity() string { return store.identity }

type rotatingIdentifiedLeaseStore struct {
	primarylease.LeaseStore
	identities []string
}

func (store rotatingIdentifiedLeaseStore) PrimaryLeaseMemberIdentities() []string {
	return append([]string(nil), store.identities...)
}

func (failingLeaseStore) LoadPrimaryLease(context.Context, [16]byte) (primarylease.LeaseRecord, bool, error) {
	return primarylease.LeaseRecord{}, false, errors.New("member unavailable")
}

func (failingLeaseStore) CompareAndSwapPrimaryLease(context.Context, [16]byte, *primarylease.LeaseRecord, primarylease.LeaseRecord) (bool, error) {
	return false, errors.New("member unavailable")
}

func TestQuorumStoreRequiresExactMajorityAndRepairsFromPartialWrite(t *testing.T) {
	databaseID := [16]byte{9: 1}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	old := primarylease.LeaseRecord{DatabaseID: databaseID, Owner: "writer-a", Epoch: 1, CommitSequence: 4, NotAfter: now.Add(time.Minute)}
	partial := primarylease.LeaseRecord{DatabaseID: databaseID, Owner: "writer-b", Epoch: 2, CommitSequence: 4, NotAfter: now.Add(2 * time.Minute)}
	next := primarylease.LeaseRecord{DatabaseID: databaseID, Owner: "writer-c", Epoch: 2, CommitSequence: 4, NotAfter: now.Add(3 * time.Minute)}
	first, second, third := primarylease.NewMemoryStore(), primarylease.NewMemoryStore(), primarylease.NewMemoryStore()
	putLeaseRecord(t, first, databaseID, old)
	putLeaseRecord(t, second, databaseID, old)
	putLeaseRecord(t, third, databaseID, partial)
	store, err := primarylease.NewQuorumStore([]primarylease.QuorumReplica{{MemberID: "member-a", Store: first}, {MemberID: "member-b", Store: second}, {MemberID: "member-c", Store: third}})
	if err != nil {
		t.Fatal(err)
	}
	record, exists, err := store.LoadPrimaryLease(context.Background(), databaseID)
	if err != nil || !exists || record != old {
		t.Fatalf("partial write became quorum state: record=%+v exists=%t err=%v", record, exists, err)
	}
	swapped, err := store.CompareAndSwapPrimaryLease(context.Background(), databaseID, &old, next)
	if err != nil || !swapped {
		t.Fatalf("quorum repair CAS swapped=%t err=%v", swapped, err)
	}
	record, exists, err = store.LoadPrimaryLease(context.Background(), databaseID)
	if err != nil || !exists || record != next {
		t.Fatalf("repaired quorum record=%+v exists=%t err=%v", record, exists, err)
	}
}

func TestQuorumStoreFailsClosedOnCrossedPartialHistories(t *testing.T) {
	databaseID := [16]byte{9: 2}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	first, second, third := primarylease.NewMemoryStore(), primarylease.NewMemoryStore(), primarylease.NewMemoryStore()
	putLeaseRecord(t, first, databaseID, primarylease.LeaseRecord{DatabaseID: databaseID, Owner: "writer-a", Epoch: 2, CommitSequence: 4, NotAfter: now.Add(time.Minute)})
	putLeaseRecord(t, second, databaseID, primarylease.LeaseRecord{DatabaseID: databaseID, Owner: "writer-b", Epoch: 2, CommitSequence: 4, NotAfter: now.Add(time.Minute)})
	putLeaseRecord(t, third, databaseID, primarylease.LeaseRecord{DatabaseID: databaseID, Owner: "writer-c", Epoch: 1, CommitSequence: 4, NotAfter: now.Add(time.Minute)})
	store, err := primarylease.NewQuorumStore([]primarylease.QuorumReplica{{MemberID: "member-a", Store: first}, {MemberID: "member-b", Store: second}, {MemberID: "member-c", Store: third}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.LoadPrimaryLease(context.Background(), databaseID); !errors.Is(err, primarylease.ErrLeaseConflict) {
		t.Fatalf("crossed partial records err=%v", err)
	}
}

func TestQuorumStoreMaintainsAvailabilityOnlyAtMajority(t *testing.T) {
	databaseID := [16]byte{9: 3}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	first, second := primarylease.NewMemoryStore(), primarylease.NewMemoryStore()
	store, err := primarylease.NewQuorumStore([]primarylease.QuorumReplica{{MemberID: "member-a", Store: first}, {MemberID: "member-b", Store: second}, {MemberID: "member-c", Store: failingLeaseStore{}}})
	if err != nil {
		t.Fatal(err)
	}
	next := primarylease.LeaseRecord{DatabaseID: databaseID, Owner: "writer-a", Epoch: 1, NotAfter: now.Add(time.Minute)}
	if swapped, err := store.CompareAndSwapPrimaryLease(context.Background(), databaseID, nil, next); err != nil || !swapped {
		t.Fatalf("two-member write quorum swapped=%t err=%v", swapped, err)
	}
	if record, exists, err := store.LoadPrimaryLease(context.Background(), databaseID); err != nil || !exists || record != next {
		t.Fatalf("two-member read quorum record=%+v exists=%t err=%v", record, exists, err)
	}
	withoutQuorum, err := primarylease.NewQuorumStore([]primarylease.QuorumReplica{{MemberID: "member-a", Store: first}, {MemberID: "member-b", Store: failingLeaseStore{}}, {MemberID: "member-c", Store: failingLeaseStore{}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := withoutQuorum.LoadPrimaryLease(context.Background(), databaseID); !errors.Is(err, primarylease.ErrLeaseQuorum) {
		t.Fatalf("single member read err=%v", err)
	}
	if stats := store.Stats(); stats.Replicas != 3 || stats.Quorum != 2 {
		t.Fatalf("quorum stats=%+v", stats)
	}
	if stats := withoutQuorum.Stats(); stats.EndpointFailures == 0 || stats.QuorumFailures == 0 {
		t.Fatalf("no-quorum stats=%+v", stats)
	}
}

func TestQuorumStoreRejectsAliasedOrIncompleteMemberSets(t *testing.T) {
	member := primarylease.NewMemoryStore()
	if _, err := primarylease.NewQuorumStore([]primarylease.QuorumReplica{{MemberID: "member-a", Store: member}, {MemberID: "member-a", Store: member}, {MemberID: "member-c", Store: member}}); err == nil {
		t.Fatal("duplicate member IDs formed a quorum")
	}
	if _, err := primarylease.NewQuorumStore([]primarylease.QuorumReplica{{MemberID: "member-a", Store: member}, {MemberID: "member-b", Store: member}}); err == nil {
		t.Fatal("even member count formed a quorum")
	}
	aliased := identifiedLeaseStore{LeaseStore: member, identity: "same-verified-leaf"}
	if _, err := primarylease.NewQuorumStore([]primarylease.QuorumReplica{{MemberID: "member-a", Store: aliased}, {MemberID: "member-b", Store: aliased}, {MemberID: "member-c", Store: aliased}}); err == nil {
		t.Fatal("one verified member identity formed several quorum votes")
	}
}

func TestQuorumStoreRejectsOverlappingRotatingMemberIdentities(t *testing.T) {
	member := primarylease.NewMemoryStore()
	rotating := rotatingIdentifiedLeaseStore{LeaseStore: member, identities: []string{"leaf-old", "leaf-new"}}
	overlap := rotatingIdentifiedLeaseStore{LeaseStore: member, identities: []string{"leaf-new", "leaf-next"}}
	distinct := rotatingIdentifiedLeaseStore{LeaseStore: member, identities: []string{"leaf-third"}}
	if _, err := primarylease.NewQuorumStore([]primarylease.QuorumReplica{{MemberID: "member-a", Store: rotating}, {MemberID: "member-b", Store: overlap}, {MemberID: "member-c", Store: distinct}}); err == nil {
		t.Fatal("overlapping rotating identities formed several quorum votes")
	}
	duplicateWithinMember := rotatingIdentifiedLeaseStore{LeaseStore: member, identities: []string{"leaf-a", "leaf-a"}}
	if _, err := primarylease.NewQuorumStore([]primarylease.QuorumReplica{{MemberID: "member-a", Store: duplicateWithinMember}, {MemberID: "member-b", Store: distinct}, {MemberID: "member-c", Store: identifiedLeaseStore{LeaseStore: member, identity: "leaf-c"}}}); err == nil {
		t.Fatal("duplicate rotating identity was accepted")
	}
}

func TestQuorumCASModelPreventsTwoAcknowledgedSuccessors(t *testing.T) {
	for _, replicas := range []int{1, 3, 5} {
		quorum := replicas/2 + 1
		for firstMask := 0; firstMask < 1<<replicas; firstMask++ {
			for secondMask := 0; secondMask < 1<<replicas; secondMask++ {
				// Both candidates compare against the same predecessor. A member
				// that accepted firstMask no longer accepts secondMask; this is the
				// only property QuorumStore requires from each replica's CAS.
				firstAcknowledged := bits.OnesCount(uint(firstMask)) >= quorum
				secondAcknowledged := bits.OnesCount(uint(secondMask&^firstMask)) >= quorum
				if firstAcknowledged && secondAcknowledged {
					t.Fatalf("two successors acknowledged replicas=%d first=%b second=%b", replicas, firstMask, secondMask)
				}
			}
		}
	}
}

func putLeaseRecord(t *testing.T, store *primarylease.MemoryStore, databaseID [16]byte, record primarylease.LeaseRecord) {
	t.Helper()
	swapped, err := store.CompareAndSwapPrimaryLease(context.Background(), databaseID, nil, record)
	if err != nil || !swapped {
		t.Fatalf("put record swapped=%t err=%v", swapped, err)
	}
}
