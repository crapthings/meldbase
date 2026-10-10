package primarylease

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crapthings/meldbase"
)

const (
	// defaultRenewRequestTimeout limits how long a renewal request can take.
	defaultRenewRequestTimeout = 2 * time.Second
	// defaultRenewRetryInterval sets the wait after a failed renewal.
	defaultRenewRetryInterval = 500 * time.Millisecond
	// minimumRenewInterval sets the shortest allowed timeout or retry wait.
	minimumRenewInterval = 50 * time.Millisecond
	// maximumRenewInterval sets the longest allowed timeout or retry wait.
	maximumRenewInterval = time.Minute
)

// ErrRenewalConfiguration reports missing dependencies or invalid renewal settings.
var ErrRenewalConfiguration = errors.New("meldbase primary lease: invalid renewal agent configuration")

// RenewalClient requests a controller write lease for a database position.
//
// authorityhttp.Client implements it. Owner authentication stays in
// that transport and is never an argument to a renewal request.
type RenewalClient interface {
	Grant(context.Context, [16]byte, uint64) (Grant, error)
}

// RenewerOptions sets the database, write guard, controller client, and renewal timing.
//
// DB must be the
// same database that was opened with Guard as its PrimaryWriteFence. The
// API cannot infer that pointer equality across the Meldbase boundary, so this
// deployment wiring is explicit and checked at least for a configured fence.
//
// KeepLeaseOnStop is deliberately false by default: Run revokes the local
// guard when its supervisory context ends, so a stopped supervisor cannot leave
// a process writable until natural certificate expiry. Set it only when another
// supervisor takes over with the same process and guard lifecycle.
type RenewerOptions struct {
	DB              *meldbase.DB
	Guard           *Guard
	Client          RenewalClient
	RequestTimeout  time.Duration
	RetryInterval   time.Duration
	KeepLeaseOnStop bool
	Clock           func() time.Time
}

// Renewer keeps a primary's write lease current without holding the database writer during controller requests.
//
// It is not a
// leader-election, replication-ack or promotion mechanism: its commit
// sequence snapshot is a controller checkpoint and cannot establish follower
// completeness after a source failure.
type Renewer struct {
	db              *meldbase.DB
	guard           *Guard
	client          RenewalClient
	requestTimeout  time.Duration
	retryInterval   time.Duration
	keepLeaseOnStop bool
	now             func() time.Time
	renewMu         sync.Mutex
	metrics         renewerMetrics
}

// renewerMetrics tracks renewal outcomes and whether the renewal loop is running.
type renewerMetrics struct {
	attempts            atomic.Uint64
	succeeded           atomic.Uint64
	failed              atomic.Uint64
	installFailed       atomic.Uint64
	consecutiveFailures atomic.Uint64
	running             atomic.Bool
}

// RenewerStats reports renewal outcomes and whether the renewal loop is running.
//
// It intentionally contains no certificate, endpoint, owner or database ID.
type RenewerStats struct {
	Attempts            uint64
	Succeeded           uint64
	Failed              uint64
	InstallFailed       uint64
	ConsecutiveFailures uint64
	Running             bool
}

// NewRenewer creates a lease renewer after checking its dependencies and timing settings.
func NewRenewer(options RenewerOptions) (*Renewer, error) {
	if options.DB == nil || options.Guard == nil || options.Client == nil {
		return nil, ErrRenewalConfiguration
	}
	if stats := options.DB.Stats(); stats.Closed || stats.WritesDisabled || !stats.PrimaryWriteFence.Configured || !stats.PrimaryWriteFence.Enforced {
		return nil, ErrRenewalConfiguration
	}
	timeout := options.RequestTimeout
	if timeout == 0 {
		timeout = defaultRenewRequestTimeout
	}
	if timeout < minimumRenewInterval || timeout > maximumRenewInterval {
		return nil, ErrRenewalConfiguration
	}
	retry := options.RetryInterval
	if retry == 0 {
		retry = defaultRenewRetryInterval
	}
	if retry < minimumRenewInterval || retry > maximumRenewInterval {
		return nil, ErrRenewalConfiguration
	}
	now := options.Clock
	if now == nil {
		now = time.Now
	}
	return &Renewer{db: options.DB, guard: options.Guard, client: options.Client, requestTimeout: timeout, retryInterval: retry, keepLeaseOnStop: options.KeepLeaseOnStop, now: now}, nil
}

// Renew installs a fresh write lease from the controller for the current database position.
//
// The database writer is never held while client.Grant is in progress. A failed request leaves an existing lease in
// place until its normal local expiry; it does not extend authority and does
// not silently reopen an initially closed guard.
func (renewer *Renewer) Renew(ctx context.Context) error {
	if renewer == nil || renewer.db == nil || renewer.guard == nil || renewer.client == nil || ctx == nil {
		return ErrRenewalConfiguration
	}
	renewer.renewMu.Lock()
	defer renewer.renewMu.Unlock()
	renewer.metrics.attempts.Add(1)
	stats := renewer.db.Stats()
	if stats.Closed || stats.WritesDisabled || !stats.PrimaryWriteFence.Configured || !stats.PrimaryWriteFence.Enforced {
		renewer.recordFailure(false)
		return ErrRenewalConfiguration
	}
	request, cancel := context.WithTimeout(ctx, renewer.requestTimeout)
	grant, err := renewer.client.Grant(request, renewer.db.DatabaseID(), stats.CommitSequence)
	cancel()
	if err != nil {
		renewer.recordFailure(false)
		return err
	}
	if err := renewer.guard.Install(grant.Certificate); err != nil {
		renewer.recordFailure(true)
		return err
	}
	renewer.metrics.succeeded.Add(1)
	renewer.metrics.consecutiveFailures.Store(0)
	return nil
}

// Run keeps the local write lease renewed until the context ends.
//
// It renews immediately, then schedules the next request at one third of the
// current certificate duration before expiry. It retries failures at the
// bounded RetryInterval. On context completion it revokes the local guard by
// default, making deliberate supervisor shutdown fail closed immediately.
func (renewer *Renewer) Run(ctx context.Context) error {
	if renewer == nil || ctx == nil {
		return ErrRenewalConfiguration
	}
	if !renewer.metrics.running.CompareAndSwap(false, true) {
		return ErrRenewalConfiguration
	}
	defer func() {
		renewer.metrics.running.Store(false)
		if !renewer.keepLeaseOnStop && renewer.guard != nil {
			renewer.guard.Revoke()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := renewer.Renew(ctx); err != nil {
			if err := waitRenew(ctx, renewer.retryInterval); err != nil {
				return err
			}
			continue
		}
		if err := waitRenew(ctx, renewer.nextDelay()); err != nil {
			return err
		}
	}
}

// nextDelay schedules renewal before lease expiry or uses the retry wait when necessary.
func (renewer *Renewer) nextDelay() time.Duration {
	status := renewer.guard.LeaseStatus()
	if !status.Installed {
		return renewer.retryInterval
	}
	duration := status.NotAfter.Sub(status.NotBefore)
	lead := duration / 3
	if lead < minimumRenewInterval {
		lead = minimumRenewInterval
	}
	delay := status.NotAfter.Sub(renewer.now().UTC().Truncate(time.Millisecond)) - lead
	if delay <= 0 {
		return renewer.retryInterval
	}
	return delay
}

// waitRenew waits for the next renewal or returns when the context ends.
func waitRenew(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		delay = minimumRenewInterval
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// recordFailure counts a failed renewal and records whether certificate installation failed.
func (renewer *Renewer) recordFailure(install bool) {
	renewer.metrics.failed.Add(1)
	if install {
		renewer.metrics.installFailed.Add(1)
	}
	renewer.metrics.consecutiveFailures.Add(1)
}

// Stats returns an O(1), identity-free aggregate snapshot.
//
// It is designed for
// an external sampler rather than calls from the database write path.
func (renewer *Renewer) Stats() RenewerStats {
	if renewer == nil {
		return RenewerStats{}
	}
	return RenewerStats{
		Attempts: renewer.metrics.attempts.Load(), Succeeded: renewer.metrics.succeeded.Load(),
		Failed: renewer.metrics.failed.Load(), InstallFailed: renewer.metrics.installFailed.Load(),
		ConsecutiveFailures: renewer.metrics.consecutiveFailures.Load(), Running: renewer.metrics.running.Load(),
	}
}
