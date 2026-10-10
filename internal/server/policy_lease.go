package server

import (
	"context"
	"errors"
	"sync"
	"unicode/utf8"
)

// ErrInvalidPolicyLease reports an invalid policy version or missing lease where a lease is required.
var ErrInvalidPolicyLease = errors.New("meldbase server: invalid query policy lease")

// QueryPolicyLease coordinates policy revocation with authorized response delivery.
//
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

// NewQueryPolicyLease creates an active lease for a nonempty policy version.
func NewQueryPolicyLease(version string) (*QueryPolicyLease, error) {
	if !validPolicyVersion(version) {
		return nil, ErrInvalidPolicyLease
	}
	return &QueryPolicyLease{version: version, done: make(chan struct{}), drained: make(chan struct{})}, nil
}

// Version returns the policy version or an empty string for a nil lease.
func (lease *QueryPolicyLease) Version() string {
	if lease == nil {
		return ""
	}
	return lease.version
}

// Done returns the channel closed on revocation or nil for a nil lease.
func (lease *QueryPolicyLease) Done() <-chan struct{} {
	if lease == nil {
		return nil
	}
	return lease.done
}

// Valid reports whether a non-nil lease has not been revoked.
func (lease *QueryPolicyLease) Valid() bool {
	if lease == nil {
		return false
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return !lease.revoked
}

// Revoke blocks new authorized actions and waits for existing actions to finish.
//
// Repeated calls are safe. A canceled context stops waiting but does not undo
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

// acquire reserves an authorized action unless the lease is revoked, with a nil lease allowing the action.
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

// release finishes an acquired action and signals when a revoked lease has no active actions.
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

// validPolicyVersion checks that a policy version is nonempty valid UTF-8 within the byte limit.
func validPolicyVersion(version string) bool {
	return version != "" && len(version) <= 128 && utf8.ValidString(version)
}

// underQueryPolicyLease runs an action while holding a lease acquisition and reports whether it was authorized.
func underQueryPolicyLease(lease *QueryPolicyLease, action func() error) (bool, error) {
	if action == nil || !lease.acquire() {
		return false, nil
	}
	defer lease.release()
	return true, action()
}

// underQueryPolicy runs an action while holding acquisitions on both leases attached to the query policy.
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
