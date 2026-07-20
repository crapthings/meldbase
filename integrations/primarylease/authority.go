package primarylease

import (
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crapthings/meldbase"
)

const (
	defaultClockSkew  = time.Second
	maximumClockSkew  = time.Minute
	defaultCASRetries = 16
)

var (
	// ErrLeaseActive means another owner may still possess a locally valid
	// certificate. Callers must wait for RetryAfter rather than attempting to
	// shorten that previous lease.
	ErrLeaseActive = errors.New("meldbase primary lease: previous owner may still be active")
	// ErrLeaseSequence rejects a controller request that would certify a source
	// position older than the controller has already recorded.
	ErrLeaseSequence = errors.New("meldbase primary lease: commit sequence regressed")
	// ErrLeaseStore reports a malformed or unavailable controller state store.
	ErrLeaseStore = errors.New("meldbase primary lease: controller state store failure")
	// ErrLeasePromotionReadiness means a promotion lacks the external proof that
	// its follower is safe to become primary at the requested source position.
	ErrLeasePromotionReadiness = errors.New("meldbase primary lease: promotion readiness was not proven")
)

// LeaseRecord is the controller's durable compare-and-swap state for one
// database. It is deliberately distinct from a rollback anchor: Owner/Epoch
// move forward and a lease may expire, while a rollback anchor is a permanent
// lower bound on recoverable history.
type LeaseRecord struct {
	DatabaseID     [16]byte
	Owner          string
	Epoch          uint64
	CommitSequence uint64
	NotAfter       time.Time
	Revoked        bool
}

// LeaseStore is the minimal durable CAS required by Authority. Implement it
// with a quorum-backed transactional store in production. Load and
// CompareAndSwap must be linearizable for a database identity; a local file or
// MemoryStore is useful only for deterministic development/testing.
//
// previous is nil only when the record is expected not to exist. A false swap
// result is a concurrent state change, not a successful no-op.
type LeaseStore interface {
	LoadPrimaryLease(context.Context, [16]byte) (LeaseRecord, bool, error)
	CompareAndSwapPrimaryLease(context.Context, [16]byte, *LeaseRecord, LeaseRecord) (bool, error)
}

// AuthorityOptions configures a controller-side certificate issuer.
// MaxClockSkew is the maximum absolute offset allowed between a primary and
// the controller. A new owner cannot receive a currently valid certificate
// until the preceding certificate's expiry plus this window.
type AuthorityOptions struct {
	Store         LeaseStore
	PrivateKey    ed25519.PrivateKey
	LeaseDuration time.Duration
	MaxClockSkew  time.Duration
	Clock         func() time.Time
	CASRetries    int
}

// Authority issues signed certificates only after an external LeaseStore CAS
// makes the handoff durable. It has no HTTP listener, peer authentication or
// membership logic; those are deployment concerns around the store.
type Authority struct {
	store         LeaseStore
	privateKey    ed25519.PrivateKey
	leaseDuration time.Duration
	clockSkew     time.Duration
	now           func() time.Time
	casRetries    int
	metrics       authorityMetrics
}

type authorityMetrics struct {
	grantAttempts              atomic.Uint64
	granted                    atomic.Uint64
	handoffWaits               atomic.Uint64
	promotionAttempts          atomic.Uint64
	promotionReadinessRejected atomic.Uint64
	sequenceRejected           atomic.Uint64
	storeFailures              atomic.Uint64
	casConflicts               atomic.Uint64
	revokeAttempts             atomic.Uint64
	revoked                    atomic.Uint64
}

// AuthorityStats is a fixed-cardinality, identity-free process snapshot for a
// controller issuer. It deliberately excludes database IDs, owners, epochs,
// certificates, endpoints and error details. Read it from a sampler rather
// than from a controller request hot path.
type AuthorityStats struct {
	GrantAttempts              uint64
	Granted                    uint64
	HandoffWaits               uint64
	PromotionAttempts          uint64
	PromotionReadinessRejected uint64
	SequenceRejected           uint64
	StoreFailures              uint64
	CASConflicts               uint64
	RevokeAttempts             uint64
	Revoked                    uint64
}

// Grant is the controller response for a primary or follower-promotion
// request. RetryAfter is set only with ErrLeaseActive.
type Grant struct {
	Certificate string
	Record      LeaseRecord
	RetryAfter  time.Time
}

// GrantRequest identifies the owner and exact source position the controller
// is asked to certify. Owner must derive from authenticated controller-side
// identity, never an application request.
type GrantRequest struct {
	DatabaseID     [16]byte
	Owner          string
	CommitSequence uint64
}

// PromotionReadiness proves that a specific follower position is eligible for
// promotion against the controller state observed by the same Authority CAS
// attempt. A production implementation normally verifies durable replication
// receipt/ack evidence and any required application recovery policy. Owner and
// epoch fencing alone cannot prove that an asynchronous follower contains all
// writes from the former primary.
type PromotionReadiness interface {
	VerifyV2FollowerPromotion(context.Context, meldbase.FollowerPromotionRequest, LeaseRecord, bool) error
}

// PromotionReadinessFunc adapts a function to PromotionReadiness.
type PromotionReadinessFunc func(context.Context, meldbase.FollowerPromotionRequest, LeaseRecord, bool) error

func (function PromotionReadinessFunc) VerifyV2FollowerPromotion(ctx context.Context, request meldbase.FollowerPromotionRequest, record LeaseRecord, exists bool) error {
	if function == nil {
		return ErrLeasePromotionReadiness
	}
	return function(ctx, request, record, exists)
}

// NewAuthority validates controller-local configuration. It is intentionally
// impossible to construct an Authority with a default in-memory store: doing
// so would make an accidental single-process test controller look durable.
func NewAuthority(options AuthorityOptions) (*Authority, error) {
	if options.Store == nil || len(options.PrivateKey) != ed25519.PrivateKeySize {
		return nil, ErrLeaseStore
	}
	duration := options.LeaseDuration
	if duration == 0 {
		duration = defaultMaxLeaseDuration
	}
	if duration < time.Second || duration > hardMaxLeaseDuration {
		return nil, ErrCertificate
	}
	skew := options.MaxClockSkew
	if skew == 0 {
		skew = defaultClockSkew
	}
	if skew < 0 || skew > maximumClockSkew {
		return nil, ErrCertificate
	}
	now := options.Clock
	if now == nil {
		now = time.Now
	}
	retries := options.CASRetries
	if retries == 0 {
		retries = defaultCASRetries
	}
	if retries < 1 || retries > 128 {
		return nil, ErrLeaseStore
	}
	return &Authority{
		store:         options.Store,
		privateKey:    append(ed25519.PrivateKey(nil), options.PrivateKey...),
		leaseDuration: duration,
		clockSkew:     skew,
		now:           now,
		casRetries:    retries,
	}, nil
}

// Grant issues a new epoch. A renewal for the same owner may be granted before
// the old certificate expires; a different owner must wait until the old
// expiry plus MaxClockSkew. This explicit availability gap is the price of not
// allowing two clock-skewed processes to write concurrently.
func (authority *Authority) Grant(ctx context.Context, request GrantRequest) (Grant, error) {
	return authority.grant(ctx, request, nil)
}

func (authority *Authority) grant(ctx context.Context, request GrantRequest, readiness PromotionReadiness) (Grant, error) {
	if authority == nil || authority.store == nil {
		return Grant{}, ErrLeaseStore
	}
	authority.metrics.grantAttempts.Add(1)
	if err := authority.contextError(ctx); err != nil {
		return Grant{}, err
	}
	if request.DatabaseID == [16]byte{} || !validOwner(request.Owner) {
		return Grant{}, ErrCertificate
	}
	for attempt := 0; attempt < authority.casRetries; attempt++ {
		current, exists, err := authority.store.LoadPrimaryLease(ctx, request.DatabaseID)
		if err != nil {
			authority.metrics.storeFailures.Add(1)
			return Grant{}, errors.Join(ErrLeaseStore, err)
		}
		if exists && !validLeaseRecord(current, request.DatabaseID) {
			authority.metrics.storeFailures.Add(1)
			return Grant{}, ErrLeaseStore
		}
		now := authority.now().UTC().Truncate(time.Millisecond)
		if exists && request.CommitSequence < current.CommitSequence {
			authority.metrics.sequenceRejected.Add(1)
			return Grant{}, ErrLeaseSequence
		}
		// Promotion readiness is meaningful only after the old owner has passed
		// the same skew-safe handoff point as a different-owner grant. Apply it
		// even if a deployment accidentally reuses an Owner label for a new
		// process: identity spelling must not bypass write fencing.
		if exists && (current.Owner != request.Owner || current.Revoked || readiness != nil) {
			safeAfter := current.NotAfter.Add(authority.clockSkew)
			if now.Before(safeAfter) {
				authority.metrics.handoffWaits.Add(1)
				return Grant{Record: current, RetryAfter: safeAfter}, ErrLeaseActive
			}
		}
		if readiness != nil {
			promotionRequest := meldbase.FollowerPromotionRequest{DatabaseID: request.DatabaseID, CommitSequence: request.CommitSequence}
			if err := readiness.VerifyV2FollowerPromotion(ctx, promotionRequest, current, exists); err != nil {
				authority.metrics.promotionReadinessRejected.Add(1)
				return Grant{}, errors.Join(ErrLeasePromotionReadiness, err)
			}
		}
		epoch := uint64(1)
		if exists {
			if current.Epoch == ^uint64(0) {
				authority.metrics.storeFailures.Add(1)
				return Grant{}, ErrLeaseStore
			}
			epoch = current.Epoch + 1
		}
		next := LeaseRecord{
			DatabaseID: request.DatabaseID, Owner: request.Owner, Epoch: epoch,
			CommitSequence: request.CommitSequence, NotAfter: now.Add(authority.leaseDuration),
		}
		certificate, err := Sign(Certificate{
			DatabaseID: request.DatabaseID, Owner: request.Owner, Epoch: epoch,
			CommitSequence: request.CommitSequence, NotBefore: now, NotAfter: next.NotAfter,
		}, authority.privateKey)
		if err != nil {
			return Grant{}, err
		}
		var previous *LeaseRecord
		if exists {
			copy := current
			previous = &copy
		}
		swapped, err := authority.store.CompareAndSwapPrimaryLease(ctx, request.DatabaseID, previous, next)
		if err != nil {
			authority.metrics.storeFailures.Add(1)
			return Grant{}, errors.Join(ErrLeaseStore, err)
		}
		if swapped {
			authority.metrics.granted.Add(1)
			return Grant{Certificate: certificate, Record: next}, nil
		}
		authority.metrics.casConflicts.Add(1)
	}
	authority.metrics.storeFailures.Add(1)
	return Grant{}, ErrLeaseStore
}

// Revoke advances the durable controller epoch and removes the current owner.
// It does not shorten an already issued certificate: an unreachable old owner
// may still be using it. The next owner must respect the same handoff window
// in Grant. A reachable owner should receive Guard.Revoke through its local
// control agent as an optimization, not as the safety proof.
func (authority *Authority) Revoke(ctx context.Context, databaseID [16]byte, epoch uint64) (LeaseRecord, error) {
	if authority == nil || authority.store == nil {
		return LeaseRecord{}, ErrLeaseStore
	}
	authority.metrics.revokeAttempts.Add(1)
	if err := authority.contextError(ctx); err != nil {
		return LeaseRecord{}, err
	}
	if databaseID == [16]byte{} || epoch == 0 {
		return LeaseRecord{}, ErrCertificate
	}
	for attempt := 0; attempt < authority.casRetries; attempt++ {
		current, exists, err := authority.store.LoadPrimaryLease(ctx, databaseID)
		if err != nil {
			authority.metrics.storeFailures.Add(1)
			return LeaseRecord{}, errors.Join(ErrLeaseStore, err)
		}
		if !exists || !validLeaseRecord(current, databaseID) || current.Epoch != epoch || current.Epoch == ^uint64(0) {
			authority.metrics.storeFailures.Add(1)
			return LeaseRecord{}, ErrLeaseStore
		}
		next := current
		next.Owner, next.Epoch, next.Revoked = "", current.Epoch+1, true
		swapped, err := authority.store.CompareAndSwapPrimaryLease(ctx, databaseID, &current, next)
		if err != nil {
			authority.metrics.storeFailures.Add(1)
			return LeaseRecord{}, errors.Join(ErrLeaseStore, err)
		}
		if swapped {
			authority.metrics.revoked.Add(1)
			return next, nil
		}
		authority.metrics.casConflicts.Add(1)
	}
	authority.metrics.storeFailures.Add(1)
	return LeaseRecord{}, ErrLeaseStore
}

// Stats returns an O(1), allocation-free aggregate snapshot. It is separate
// from DBStats because a controller can manage many databases and is not part
// of the database commit path.
func (authority *Authority) Stats() AuthorityStats {
	if authority == nil {
		return AuthorityStats{}
	}
	return AuthorityStats{
		GrantAttempts: authority.metrics.grantAttempts.Load(), Granted: authority.metrics.granted.Load(),
		HandoffWaits: authority.metrics.handoffWaits.Load(), PromotionAttempts: authority.metrics.promotionAttempts.Load(),
		PromotionReadinessRejected: authority.metrics.promotionReadinessRejected.Load(), SequenceRejected: authority.metrics.sequenceRejected.Load(),
		StoreFailures: authority.metrics.storeFailures.Load(), CASConflicts: authority.metrics.casConflicts.Load(),
		RevokeAttempts: authority.metrics.revokeAttempts.Load(), Revoked: authority.metrics.revoked.Load(),
	}
}

func (authority *Authority) contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("meldbase primary lease: context is required")
	}
	return ctx.Err()
}

func validLeaseRecord(record LeaseRecord, databaseID [16]byte) bool {
	if record.DatabaseID != databaseID || record.Epoch == 0 || record.Epoch == ^uint64(0) || record.NotAfter.IsZero() {
		return false
	}
	if record.Revoked {
		return record.Owner == ""
	}
	return validOwner(record.Owner)
}

// ValidateLeaseRecord checks that record is a well-formed controller record
// for databaseID. Network adapters use it before handing a decoded value to a
// LeaseStore; it does not judge whether the lease is currently expired.
func ValidateLeaseRecord(record LeaseRecord, databaseID [16]byte) error {
	if !validLeaseRecord(record, databaseID) {
		return ErrLeaseStore
	}
	return nil
}

// PromotionAuthority adapts a controller Authority to
// meldbase.FollowerPromotionAuthority. Construct it with the authenticated
// target process owner identity; it never accepts that identity from a
// replication frame.
type PromotionAuthority struct {
	Authority *Authority
	Owner     string
	Readiness PromotionReadiness
}

func (authority PromotionAuthority) AuthorizeFollowerPromotion(ctx context.Context, request meldbase.FollowerPromotionRequest) (meldbase.FollowerPromotionFence, error) {
	if authority.Authority == nil || !validOwner(authority.Owner) {
		return meldbase.FollowerPromotionFence{}, ErrLeaseStore
	}
	authority.Authority.metrics.promotionAttempts.Add(1)
	if authority.Readiness == nil {
		authority.Authority.metrics.promotionReadinessRejected.Add(1)
		return meldbase.FollowerPromotionFence{}, ErrLeasePromotionReadiness
	}
	grant, err := authority.Authority.grant(ctx, GrantRequest{DatabaseID: request.DatabaseID, Owner: authority.Owner, CommitSequence: request.CommitSequence}, authority.Readiness)
	if err != nil {
		return meldbase.FollowerPromotionFence{}, err
	}
	return meldbase.FollowerPromotionFence{DatabaseID: request.DatabaseID, CommitSequence: request.CommitSequence, Epoch: grant.Certificate}, nil
}

// MemoryStore is a linearizable in-process LeaseStore for tests and local
// deterministic demos. It must not be used as a production quorum store.
type MemoryStore struct {
	mu      sync.Mutex
	records map[[16]byte]LeaseRecord
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{records: make(map[[16]byte]LeaseRecord)} }

func (store *MemoryStore) LoadPrimaryLease(ctx context.Context, databaseID [16]byte) (LeaseRecord, bool, error) {
	if err := memoryContextError(ctx); err != nil {
		return LeaseRecord{}, false, err
	}
	if store == nil {
		return LeaseRecord{}, false, ErrLeaseStore
	}
	store.mu.Lock()
	record, exists := store.records[databaseID]
	store.mu.Unlock()
	return record, exists, nil
}

func (store *MemoryStore) CompareAndSwapPrimaryLease(ctx context.Context, databaseID [16]byte, previous *LeaseRecord, next LeaseRecord) (bool, error) {
	if err := memoryContextError(ctx); err != nil {
		return false, err
	}
	if store == nil || !validLeaseRecord(next, databaseID) {
		return false, ErrLeaseStore
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.records[databaseID]
	if previous == nil {
		if exists {
			return false, nil
		}
	} else if !exists || !leaseRecordEqual(current, *previous) {
		return false, nil
	}
	store.records[databaseID] = next
	return true, nil
}

func leaseRecordEqual(left, right LeaseRecord) bool {
	return left.DatabaseID == right.DatabaseID && left.Owner == right.Owner && left.Epoch == right.Epoch && left.CommitSequence == right.CommitSequence && left.NotAfter.Equal(right.NotAfter) && left.Revoked == right.Revoked
}

func memoryContextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("meldbase primary lease: context is required")
	}
	return ctx.Err()
}
