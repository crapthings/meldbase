package primarylease

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

var (
	// ErrLeaseQuorum means too few configured independent members completed an
	// operation. It is deliberately distinct from a CAS conflict.
	ErrLeaseQuorum = errors.New("meldbase primary lease: quorum unavailable")
	// ErrLeaseConflict means a read quorum observed no exact majority record.
	// This can occur after interrupted or crossed partial writes; choosing a
	// plausible-looking highest epoch would be unsafe.
	ErrLeaseConflict = errors.New("meldbase primary lease: quorum state conflict")
)

// QuorumReplica binds one independent, statically identified controller member
// to a local LeaseStore adapter. Store typically represents an authenticated
// HTTPS/mTLS RPC client. MemberID must be stable configuration, not a value
// supplied by a request or endpoint response.
type QuorumReplica struct {
	MemberID string
	Store    LeaseStore
}

// quorumMemberIdentity is the legacy single-identity form optionally supplied
// by a transport adapter whose remote member identity is cryptographically
// bound (for example an expected mTLS leaf fingerprint).
type quorumMemberIdentity interface {
	PrimaryLeaseMemberIdentity() string
}

// quorumMemberIdentities is the rotation-aware form of quorumMemberIdentity.
// Every identity must be cryptographically bound to the same configured remote
// member. QuorumStore rejects any overlap between replicas: during a leaf
// certificate rotation, accepting the old and new leaves for one member must
// not let that member occupy two static votes.
type quorumMemberIdentities interface {
	PrimaryLeaseMemberIdentities() []string
}

// quorumFatalError lets a transport report an authentication, identity or wire
// contract failure that a majority must not silently mask as an ordinary slow
// member. It is intentionally private but structural, so adapters in other
// packages can implement it without a dependency cycle.
type quorumFatalError interface {
	PrimaryLeaseQuorumFatal() bool
}

// QuorumStore turns independent linearizable member stores into a fail-closed
// LeaseStore. A successful CAS is persisted on a strict majority. Reads accept
// only a record (or absence) held identically by a strict majority; they never
// choose a maximum from crossed or partial histories.
//
// This is the quorum layer, not a membership/election system. Deployments must
// still use separately operated failure domains and an authenticated adapter
// for every replica.
type QuorumStore struct {
	replicas []QuorumReplica
	metrics  quorumStoreMetrics
}

type quorumStoreMetrics struct {
	loads            atomic.Uint64
	compareAndSwaps  atomic.Uint64
	endpointFailures atomic.Uint64
	quorumFailures   atomic.Uint64
	conflicts        atomic.Uint64
}

type quorumLoadResult struct {
	record LeaseRecord
	exists bool
	err    error
}

// QuorumStats is a fixed, identity-free operational snapshot. It intentionally
// omits endpoint and database identities.
type QuorumStats struct {
	Replicas         uint64
	Quorum           uint64
	Loads            uint64
	CompareAndSwaps  uint64
	EndpointFailures uint64
	QuorumFailures   uint64
	Conflicts        uint64
}

// NewQuorumStore accepts one development member or an odd number of at least
// three members. Duplicate IDs are rejected so endpoint aliases cannot create
// a fake quorum.
func NewQuorumStore(replicas []QuorumReplica) (*QuorumStore, error) {
	if len(replicas) != 1 && (len(replicas) < 3 || len(replicas)%2 == 0) {
		return nil, ErrLeaseStore
	}
	seen := make(map[string]struct{}, len(replicas))
	seenIdentities := make(map[string]struct{}, len(replicas))
	configured := make([]QuorumReplica, len(replicas))
	for index, replica := range replicas {
		if !validOwner(replica.MemberID) || replica.Store == nil {
			return nil, ErrLeaseStore
		}
		if _, duplicate := seen[replica.MemberID]; duplicate {
			return nil, ErrLeaseStore
		}
		identities, identified := leaseMemberIdentities(replica.Store)
		if identified {
			if len(identities) == 0 {
				return nil, ErrLeaseStore
			}
			local := make(map[string]struct{}, len(identities))
			for _, identity := range identities {
				if identity == "" {
					return nil, ErrLeaseStore
				}
				if _, duplicate := local[identity]; duplicate {
					return nil, ErrLeaseStore
				}
				if _, duplicate := seenIdentities[identity]; duplicate {
					return nil, ErrLeaseStore
				}
				local[identity] = struct{}{}
			}
			for identity := range local {
				seenIdentities[identity] = struct{}{}
			}
		}
		seen[replica.MemberID] = struct{}{}
		configured[index] = replica
	}
	return &QuorumStore{replicas: configured}, nil
}

func leaseMemberIdentities(store LeaseStore) ([]string, bool) {
	if identities, ok := store.(quorumMemberIdentities); ok {
		return identities.PrimaryLeaseMemberIdentities(), true
	}
	if identity, ok := store.(quorumMemberIdentity); ok {
		return []string{identity.PrimaryLeaseMemberIdentity()}, true
	}
	return nil, false
}

func (store *QuorumStore) LoadPrimaryLease(ctx context.Context, databaseID [16]byte) (LeaseRecord, bool, error) {
	if store == nil || ctx == nil || databaseID == [16]byte{} {
		return LeaseRecord{}, false, ErrLeaseQuorum
	}
	store.metrics.loads.Add(1)
	operation, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan quorumLoadResult, len(store.replicas))
	for _, replica := range store.replicas {
		go func(replica QuorumReplica) {
			record, exists, err := replica.Store.LoadPrimaryLease(operation, databaseID)
			results <- quorumLoadResult{record: record, exists: exists, err: err}
		}(replica)
	}
	quorum := len(store.replicas)/2 + 1
	observed := make([]quorumLoadResult, 0, len(store.replicas))
	for received := 0; received < len(store.replicas); received++ {
		select {
		case <-ctx.Done():
			store.metrics.quorumFailures.Add(1)
			return LeaseRecord{}, false, errors.Join(ErrLeaseQuorum, ctx.Err())
		case result := <-results:
			if result.err != nil {
				if isQuorumFatal(result.err) {
					return LeaseRecord{}, false, result.err
				}
				store.metrics.endpointFailures.Add(1)
			} else if (result.exists && !validLeaseRecord(result.record, databaseID)) || (!result.exists && result.record != (LeaseRecord{})) {
				store.metrics.conflicts.Add(1)
				return LeaseRecord{}, false, ErrLeaseConflict
			} else {
				observed = append(observed, result)
				if record, exists, matched := majorityLeaseRecord(observed, quorum); matched {
					return record, exists, nil
				}
			}
			if len(observed)+len(store.replicas)-received-1 < quorum {
				store.metrics.quorumFailures.Add(1)
				return LeaseRecord{}, false, ErrLeaseQuorum
			}
		}
	}
	store.metrics.conflicts.Add(1)
	return LeaseRecord{}, false, ErrLeaseConflict
}

func (store *QuorumStore) CompareAndSwapPrimaryLease(ctx context.Context, databaseID [16]byte, previous *LeaseRecord, next LeaseRecord) (bool, error) {
	if store == nil || ctx == nil || databaseID == [16]byte{} || !validLeaseRecord(next, databaseID) || (previous != nil && !validLeaseRecord(*previous, databaseID)) {
		return false, ErrLeaseQuorum
	}
	store.metrics.compareAndSwaps.Add(1)
	type result struct {
		swapped bool
		err     error
	}
	operation, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan result, len(store.replicas))
	for _, replica := range store.replicas {
		go func(replica QuorumReplica) {
			swapped, err := replica.Store.CompareAndSwapPrimaryLease(operation, databaseID, previous, next)
			results <- result{swapped: swapped, err: err}
		}(replica)
	}
	quorum := len(store.replicas)/2 + 1
	successes, conflicts := 0, 0
	for received := 0; received < len(store.replicas); received++ {
		select {
		case <-ctx.Done():
			store.metrics.quorumFailures.Add(1)
			return false, errors.Join(ErrLeaseQuorum, ctx.Err())
		case result := <-results:
			if result.err != nil {
				if isQuorumFatal(result.err) {
					return false, result.err
				}
				store.metrics.endpointFailures.Add(1)
			} else if result.swapped {
				successes++
				if successes == quorum {
					return true, nil
				}
			} else {
				conflicts++
			}
			if successes+len(store.replicas)-received-1 < quorum {
				if conflicts > 0 {
					store.metrics.conflicts.Add(1)
					return false, nil
				}
				store.metrics.quorumFailures.Add(1)
				return false, ErrLeaseQuorum
			}
		}
	}
	store.metrics.quorumFailures.Add(1)
	return false, ErrLeaseQuorum
}

func isQuorumFatal(err error) bool {
	var fatal quorumFatalError
	return errors.As(err, &fatal) && fatal.PrimaryLeaseQuorumFatal()
}

// Stats reports no per-member identity so it is safe for aggregate telemetry.
func (store *QuorumStore) Stats() QuorumStats {
	if store == nil {
		return QuorumStats{}
	}
	return QuorumStats{
		Replicas: uint64(len(store.replicas)), Quorum: uint64(len(store.replicas)/2 + 1),
		Loads: store.metrics.loads.Load(), CompareAndSwaps: store.metrics.compareAndSwaps.Load(),
		EndpointFailures: store.metrics.endpointFailures.Load(), QuorumFailures: store.metrics.quorumFailures.Load(),
		Conflicts: store.metrics.conflicts.Load(),
	}
}

func majorityLeaseRecord(results []quorumLoadResult, quorum int) (LeaseRecord, bool, bool) {
	if quorum < 1 || len(results) < quorum {
		return LeaseRecord{}, false, false
	}
	for _, candidate := range results {
		matches := 0
		for _, observed := range results {
			if candidate.exists == observed.exists && (!candidate.exists || leaseRecordEqual(candidate.record, observed.record)) {
				matches++
			}
		}
		if matches >= quorum {
			return candidate.record, candidate.exists, true
		}
	}
	return LeaseRecord{}, false, false
}

var _ LeaseStore = (*QuorumStore)(nil)

func (store *QuorumStore) String() string {
	if store == nil {
		return "primarylease.QuorumStore<nil>"
	}
	return fmt.Sprintf("primarylease.QuorumStore{%d replicas, quorum %d}", len(store.replicas), len(store.replicas)/2+1)
}
