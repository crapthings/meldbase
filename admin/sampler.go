// Package admin provides database monitoring and operator HTTP endpoints.
//
// It is deliberately separate from the database package: creating a DB never
// starts a sampler, a goroutine, or a network listener.
package admin

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/crapthings/meldbase"
	meldserver "github.com/crapthings/meldbase/server"
)

var (
	// ErrClosed reports an attempt to subscribe to a closed sampler.
	ErrClosed = errors.New("meldbase admin: sampler closed")
	// ErrSubscriberLimit reports that the sampler has reached its subscriber limit.
	ErrSubscriberLimit = errors.New("meldbase admin: subscriber limit reached")
)

// StatsSource supplies database activity snapshots to the sampler.
//
// *meldbase.DB implements this interface.
type StatsSource interface {
	Stats() meldbase.DBStats
}

// ServerStatsSource supplies server activity snapshots to the sampler.
type ServerStatsSource interface {
	Stats() meldserver.ServerStats
}

// SamplerOptions sets the sampling interval, history size, subscriber limit, and optional server source.
type SamplerOptions struct {
	Interval       time.Duration
	HistorySize    int
	MaxSubscribers int
	Server         ServerStatsSource
}

// Rates reports activity rates between adjacent samples and cumulative cache hit ratios.
//
// Valid is false for the first sample and after a database reopen or counter
// reset. Ratios are cumulative for the current process session.
type Rates struct {
	Valid                      bool    `json:"valid"`
	WindowSeconds              float64 `json:"windowSeconds"`
	CommitsPerSecond           float64 `json:"commitsPerSecond"`
	ChangesPerSecond           float64 `json:"changesPerSecond"`
	QueriesPerSecond           float64 `json:"queriesPerSecond"`
	FailedQueriesPerSecond     float64 `json:"failedQueriesPerSecond"`
	DocumentsExaminedPerSecond float64 `json:"documentsExaminedPerSecond"`
	DocumentsReturnedPerSecond float64 `json:"documentsReturnedPerSecond"`
	KeysExaminedPerSecond      float64 `json:"keysExaminedPerSecond"`
	PredicateStepsPerSecond    float64 `json:"predicateStepsPerSecond"`
	PublishedChangesPerSecond  float64 `json:"publishedChangesPerSecond"`
	DeltaDeliveriesPerSecond   float64 `json:"deltaDeliveriesPerSecond"`
	WALBytesPerSecond          float64 `json:"walBytesPerSecond"`
	PageCacheHitRatio          float64 `json:"pageCacheHitRatio"`
	DocumentCacheHitRatio      float64 `json:"documentCacheHitRatio"`
	RPCRequestsPerSecond       float64 `json:"rpcRequestsPerSecond"`
	RPCFailuresPerSecond       float64 `json:"rpcFailuresPerSecond"`
	RPCBusyPerSecond           float64 `json:"rpcBusyPerSecond"`
	RPCRatesValid              bool    `json:"rpcRatesValid"`
}

// SamplerStatus reports sample counts, retained history, subscribers, and replaced deliveries.
type SamplerStatus struct {
	Samples           uint64 `json:"samples"`
	Subscribers       uint64 `json:"subscribers"`
	DroppedDeliveries uint64 `json:"droppedDeliveries"`
	HistorySamples    uint64 `json:"historySamples"`
}

// Sample groups database statistics, activity rates, and health at one sampling point.
type Sample struct {
	Sequence uint64                  `json:"sequence"`
	Stats    meldbase.DBStats        `json:"stats"`
	Rates    Rates                   `json:"rates"`
	Sampler  SamplerStatus           `json:"sampler"`
	Health   HealthStatus            `json:"health"`
	Server   *meldserver.ServerStats `json:"server,omitempty"`
}

// Sampler periodically collects database statistics and keeps a fixed-size history.
//
// Each
// subscriber has a single replaceable slot, so a slow consumer can never block
// sampling or database work.
type Sampler struct {
	source  StatsSource
	options SamplerOptions

	mu          sync.Mutex
	closed      bool
	sequence    uint64
	latest      Sample
	haveLatest  bool
	history     []Sample
	historyHead int
	historyLen  int
	nextSubID   uint64
	subscribers map[uint64]subscriber
	dropped     uint64

	cancel context.CancelFunc
	done   chan struct{}
}

// NewSampler validates settings, captures an initial sample, and starts periodic sampling.
func NewSampler(source StatsSource, options SamplerOptions) (*Sampler, error) {
	if source == nil {
		return nil, errors.New("meldbase admin: stats source is required")
	}
	if options.Interval == 0 {
		options.Interval = time.Second
	}
	if options.Interval < 10*time.Millisecond {
		return nil, errors.New("meldbase admin: sampling interval must be at least 10ms")
	}
	if options.HistorySize == 0 {
		options.HistorySize = 300
	}
	if options.HistorySize < 1 || options.HistorySize > 86_400 {
		return nil, errors.New("meldbase admin: history size must be between 1 and 86400")
	}
	if options.MaxSubscribers == 0 {
		options.MaxSubscribers = 64
	}
	if options.MaxSubscribers < 1 || options.MaxSubscribers > 4096 {
		return nil, errors.New("meldbase admin: max subscribers must be between 1 and 4096")
	}

	ctx, cancel := context.WithCancel(context.Background())
	sampler := &Sampler{
		source: source, options: options,
		history:     make([]Sample, options.HistorySize),
		subscribers: make(map[uint64]subscriber),
		cancel:      cancel, done: make(chan struct{}),
	}
	sampler.capture()
	go sampler.run(ctx)
	return sampler, nil
}

// run captures samples at the configured interval until cancellation.
func (s *Sampler) run(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(s.options.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.capture()
		}
	}
}

// capture records a statistics snapshot and offers it to subscribers without waiting for them.
func (s *Sampler) capture() {
	stats := s.source.Stats()
	var serverStats *meldserver.ServerStats
	if s.options.Server != nil {
		captured := s.options.Server.Stats()
		serverStats = &captured
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}

	s.sequence++
	sample := Sample{Sequence: s.sequence, Stats: stats, Server: serverStats}
	var previous *Sample
	if s.haveLatest {
		prior := s.latest
		previous = &prior
		sample.Rates = deriveRates(s.latest.Stats, stats)
		deriveServerRates(&sample.Rates, s.latest.Server, sample.Server)
	}
	sample.Sampler = SamplerStatus{
		Samples: s.sequence, Subscribers: uint64(len(s.subscribers)),
		DroppedDeliveries: s.dropped,
		HistorySamples:    uint64(min(s.historyLen+1, len(s.history))),
	}
	sample.Health = assessHealth(previous, sample)
	s.latest, s.haveLatest = sample, true
	s.history[(s.historyHead+s.historyLen)%len(s.history)] = sample
	if s.historyLen < len(s.history) {
		s.historyLen++
	} else {
		s.historyHead = (s.historyHead + 1) % len(s.history)
	}

	for _, subscriber := range s.subscribers {
		if offerLatest(subscriber.channel, sample) {
			s.dropped++
		}
	}
}

// offerLatest delivers the latest sample without blocking and reports whether an older sample was dropped.
//
// The channel has one slot. The consumer may receive between the first failed send and the
// stale-value drain, so both the drain and replacement send must remain
// non-blocking. The sampler lock guarantees that no other producer can send or
// close the channel while this function runs.
func offerLatest(channel chan Sample, sample Sample) (dropped bool) {
	select {
	case channel <- sample:
		return false
	default:
	}

	select {
	case <-channel:
		dropped = true
	default:
		// The consumer won the race and already drained the stale sample.
	}

	select {
	case channel <- sample:
	default:
		// There is one producer and a receive-only consumer, so the channel
		// cannot become full here. Keep this non-blocking as a defensive
		// invariant: observability must never stall its sampler.
	}
	return dropped
}

// deriveServerRates adds RPC rates when adjacent server snapshots have compatible counters and sessions.
func deriveServerRates(rates *Rates, previous, current *meldserver.ServerStats) {
	if rates == nil || !rates.Valid || previous == nil || current == nil || current.StartedAt != previous.StartedAt || rates.WindowSeconds <= 0 {
		return
	}
	delta := func(before, after uint64) float64 {
		if after < before {
			return -1
		}
		return float64(after-before) / rates.WindowSeconds
	}
	rates.RPCRequestsPerSecond = delta(previous.RPCRequests, current.RPCRequests)
	if rates.RPCRequestsPerSecond < 0 {
		rates.RPCRequestsPerSecond = 0
		return
	}
	previousFailures, previousOK := sumUintCounters(previous.RPCFailed, previous.RPCCanceled, previous.RPCRejected)
	currentFailures, currentOK := sumUintCounters(current.RPCFailed, current.RPCCanceled, current.RPCRejected)
	if !previousOK || !currentOK {
		return
	}
	rates.RPCFailuresPerSecond = delta(previousFailures, currentFailures)
	rates.RPCBusyPerSecond = delta(previous.RPCBusy, current.RPCBusy)
	if rates.RPCFailuresPerSecond < 0 || rates.RPCBusyPerSecond < 0 {
		rates.RPCFailuresPerSecond, rates.RPCBusyPerSecond = 0, 0
		return
	}
	rates.RPCRatesValid = true
}

// sumUintCounters adds unsigned counters and rejects overflow.
func sumUintCounters(values ...uint64) (uint64, bool) {
	var total uint64
	for _, value := range values {
		if ^uint64(0)-total < value {
			return 0, false
		}
		total += value
	}
	return total, true
}

// deriveRates calculates database activity rates and cache hit ratios from adjacent snapshots.
func deriveRates(previous, current meldbase.DBStats) Rates {
	window := current.CapturedAt.Sub(previous.CapturedAt).Seconds()
	if window <= 0 || current.StartedAt != previous.StartedAt {
		return Rates{}
	}
	delta := func(before, after uint64) (float64, bool) {
		if after < before {
			return 0, false
		}
		return float64(after-before) / window, true
	}

	commits, ok := delta(previous.Commits.Total, current.Commits.Total)
	if !ok {
		return Rates{}
	}
	rates := Rates{Valid: true, WindowSeconds: window, CommitsPerSecond: commits}
	rates.ChangesPerSecond, _ = delta(previous.Commits.Changes, current.Commits.Changes)
	rates.QueriesPerSecond, _ = delta(previous.Queries.Total, current.Queries.Total)
	rates.FailedQueriesPerSecond, _ = delta(previous.Queries.Failed, current.Queries.Failed)
	rates.DocumentsExaminedPerSecond, _ = delta(previous.Queries.DocumentsExamined, current.Queries.DocumentsExamined)
	rates.DocumentsReturnedPerSecond, _ = delta(previous.Queries.DocumentsReturned, current.Queries.DocumentsReturned)
	rates.KeysExaminedPerSecond, _ = delta(previous.Queries.KeysExamined, current.Queries.KeysExamined)
	rates.PredicateStepsPerSecond, _ = delta(previous.Queries.PredicateSteps, current.Queries.PredicateSteps)
	rates.PublishedChangesPerSecond, _ = delta(previous.Realtime.PublishedChanges, current.Realtime.PublishedChanges)
	rates.DeltaDeliveriesPerSecond, _ = delta(previous.Realtime.DeltaDeliveries, current.Realtime.DeltaDeliveries)
	rates.PageCacheHitRatio = hitRatio(current.Storage.PageCache.Hits, current.Storage.PageCache.Misses)
	rates.DocumentCacheHitRatio = hitRatio(current.Storage.DocumentCache.Hits, current.Storage.DocumentCache.Misses)
	return rates
}

// hitRatio returns the fraction of cache accesses that hit, or zero for an empty or overflowing total.
func hitRatio(hits, misses uint64) float64 {
	total := hits + misses
	if total < hits || total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

// Latest returns the most recent sample and whether a sample is available.
func (s *Sampler) Latest() (Sample, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest, s.haveLatest
}

// History returns a copy of the retained samples in oldest-first order.
func (s *Sampler) History() []Sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Sample, s.historyLen)
	for index := range result {
		result[index] = s.history[(s.historyHead+index)%len(s.history)]
	}
	return result
}

// Status returns the sampler's current counters and retained history size.
func (s *Sampler) Status() SamplerStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SamplerStatus{
		Samples: s.sequence, Subscribers: uint64(len(s.subscribers)),
		DroppedDeliveries: s.dropped, HistorySamples: uint64(s.historyLen),
	}
}

// Subscription exposes a sample stream that can be closed independently.
type Subscription struct {
	C       <-chan Sample
	sampler *Sampler
	id      uint64
}

// subscriber holds a replaceable sample slot and a close notification.
type subscriber struct {
	channel chan Sample
	done    chan struct{}
}

// Subscribe creates a bounded latest-sample stream that closes when its context ends.
func (s *Sampler) Subscribe(ctx context.Context) (*Subscription, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	if len(s.subscribers) >= s.options.MaxSubscribers {
		s.mu.Unlock()
		return nil, ErrSubscriberLimit
	}
	s.nextSubID++
	id := s.nextSubID
	channel := make(chan Sample, 1)
	done := make(chan struct{})
	if s.haveLatest {
		channel <- s.latest
	}
	s.subscribers[id] = subscriber{channel: channel, done: done}
	s.mu.Unlock()

	subscription := &Subscription{C: channel, sampler: s, id: id}
	go func() {
		select {
		case <-ctx.Done():
			subscription.Close()
		case <-done:
		case <-s.done:
		}
	}()
	return subscription, nil
}

// Close removes this subscription and closes its sample stream.
func (s *Subscription) Close() {
	if s == nil || s.sampler == nil {
		return
	}
	s.sampler.unsubscribe(s.id)
}

// unsubscribe removes a subscriber and closes its delivery and notification channels.
func (s *Sampler) unsubscribe(id uint64) {
	s.mu.Lock()
	if subscriber, ok := s.subscribers[id]; ok {
		delete(s.subscribers, id)
		close(subscriber.channel)
		close(subscriber.done)
	}
	s.mu.Unlock()
}

// Close closes subscriber streams and waits for periodic sampling to stop.
func (s *Sampler) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	for id, subscriber := range s.subscribers {
		delete(s.subscribers, id)
		close(subscriber.channel)
		close(subscriber.done)
	}
	s.mu.Unlock()
	s.cancel()
	<-s.done
	return nil
}
