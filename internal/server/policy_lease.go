package server

import (
	"context"
	"errors"
	"sync"
	"unicode/utf8"
)

var ErrInvalidPolicyLease = errors.New("meldbase server: invalid query policy lease")

// QueryPolicyLease linearizes policy revocation against authorized output.
// Revoke first prevents new acquisitions and closes Done, then waits for every
// acquisition already encoding or enqueueing a response to finish. Frames
// already placed in the transport queue are considered authorized in flight.
// One lease may be shared by many subscriptions governed by the same version.
type QueryPolicyLease struct {
	mu      sync.Mutex
	version string
	active  uint64
	revoked bool
	done    chan struct{}
	drained chan struct{}
}

func NewQueryPolicyLease(version string) (*QueryPolicyLease, error) {
	if !validPolicyVersion(version) {
		return nil, ErrInvalidPolicyLease
	}
	return &QueryPolicyLease{version: version, done: make(chan struct{}), drained: make(chan struct{})}, nil
}

func (lease *QueryPolicyLease) Version() string {
	if lease == nil {
		return ""
	}
	return lease.version
}

func (lease *QueryPolicyLease) Done() <-chan struct{} {
	if lease == nil {
		return nil
	}
	return lease.done
}

func (lease *QueryPolicyLease) Valid() bool {
	if lease == nil {
		return false
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return !lease.revoked
}

// Revoke is idempotent. A canceled context stops waiting but does not undo the
// revocation; a later call may wait for the same lease to drain.
func (lease *QueryPolicyLease) Revoke(ctx context.Context) error {
	if lease == nil {
		return ErrInvalidPolicyLease
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lease.mu.Lock()
	if !lease.revoked {
		lease.revoked = true
		close(lease.done)
		if lease.active == 0 {
			close(lease.drained)
		}
	}
	drained := lease.drained
	lease.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (lease *QueryPolicyLease) acquire() bool {
	if lease == nil {
		return true
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.revoked {
		return false
	}
	lease.active++
	if lease.active == 0 {
		panic("query policy lease acquisition overflow")
	}
	return true
}

func (lease *QueryPolicyLease) release() {
	if lease == nil {
		return
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.active == 0 {
		panic("query policy lease released without acquisition")
	}
	lease.active--
	if lease.revoked && lease.active == 0 {
		close(lease.drained)
	}
}

func validPolicyVersion(version string) bool {
	return version != "" && len(version) <= 128 && utf8.ValidString(version)
}

func underQueryPolicyLease(lease *QueryPolicyLease, action func() error) (bool, error) {
	if action == nil || !lease.acquire() {
		return false, nil
	}
	defer lease.release()
	return true, action()
}

func underQueryPolicy(policy QueryPolicy, action func() error) (bool, error) {
	if action == nil || !policy.Lease.acquire() {
		return false, nil
	}
	defer policy.Lease.release()
	if !policy.additionalLease.acquire() {
		return false, nil
	}
	defer policy.additionalLease.release()
	return true, action()
}
