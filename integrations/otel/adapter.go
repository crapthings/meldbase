// Package meldotel exports Meldbase's bounded admin snapshots through the
// stable OpenTelemetry Metrics API. It never calls DB.Stats directly and does
// not create an SDK, exporter, goroutine, network connection, or global state.
package meldotel

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crapthings/meldbase/admin"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/crapthings/meldbase/integrations/otel"

// SchemaVersion identifies the fixed aggregate instrument contract. It is
// independent of the admin JSON schema and OpenTelemetry API version.
const SchemaVersion uint32 = 8

type InstrumentKind string

const (
	InstrumentGauge   InstrumentKind = "gauge"
	InstrumentCounter InstrumentKind = "counter"
)

type InstrumentDescriptor struct {
	Name        string
	Description string
	Unit        string
	Kind        InstrumentKind
	Number      string
}

// Instruments returns a fresh, caller-owned copy of the fixed schema. It never
// contains application, collection, query, document, principal, or tenant data.
func Instruments() []InstrumentDescriptor {
	result := make([]InstrumentDescriptor, 0, len(intDescriptors)+len(floatDescriptors)+1)
	result = append(result, InstrumentDescriptor{
		Name: "meldbase.up", Description: "Whether the latest sampled database is open.",
		Unit: "1", Kind: InstrumentGauge, Number: "int64",
	})
	for _, descriptor := range intDescriptors {
		kind := InstrumentGauge
		if descriptor.kind == intCounter {
			kind = InstrumentCounter
		}
		result = append(result, InstrumentDescriptor{
			Name: descriptor.name, Description: descriptor.description, Unit: descriptor.unit,
			Kind: kind, Number: "int64",
		})
	}
	for _, descriptor := range floatDescriptors {
		kind := InstrumentGauge
		if descriptor.kind == floatCounter {
			kind = InstrumentCounter
		}
		result = append(result, InstrumentDescriptor{
			Name: descriptor.name, Description: descriptor.description, Unit: descriptor.unit,
			Kind: kind, Number: "float64",
		})
	}
	return result
}

// SampleSource is implemented by *admin.Sampler. The adapter deliberately
// consumes its latest immutable sample instead of entering a database lock from
// an OpenTelemetry collection callback.
type SampleSource interface {
	Latest() (admin.Sample, bool)
}

type Options struct {
	MeterProvider          metric.MeterProvider
	InstrumentationVersion string
}

type AdapterStats struct {
	Collections           uint64
	UnavailableSamples    uint64
	SaturatedMeasurements uint64
	Closed                bool
}

type intKind uint8

const (
	intGauge intKind = iota
	intCounter
)

type floatKind uint8

const (
	floatGauge floatKind = iota
	floatCounter
)

type intDescriptor struct {
	name, description, unit string
	kind                    intKind
	read                    func(admin.Sample) (uint64, bool)
}

type floatDescriptor struct {
	name, description, unit string
	kind                    floatKind
	read                    func(admin.Sample) (float64, bool)
}

type intObservation struct {
	descriptor intDescriptor
	instrument metric.Int64Observable
}

type floatObservation struct {
	descriptor floatDescriptor
	instrument metric.Float64Observable
}

type Adapter struct {
	source       SampleSource
	registration metric.Registration
	ints         []intObservation
	floats       []floatObservation
	closeOnce    sync.Once
	closeErr     error
	closed       atomic.Bool
	collections  atomic.Uint64
	unavailable  atomic.Uint64
	saturations  atomic.Uint64
}

func New(source SampleSource, options Options) (*Adapter, error) {
	if source == nil {
		return nil, errors.New("meldbase otel: sample source is required")
	}
	if options.MeterProvider == nil {
		return nil, errors.New("meldbase otel: MeterProvider is required")
	}
	meterOptions := []metric.MeterOption{}
	if options.InstrumentationVersion != "" {
		meterOptions = append(meterOptions, metric.WithInstrumentationVersion(options.InstrumentationVersion))
	}
	meter := options.MeterProvider.Meter(instrumentationName, meterOptions...)
	adapter := &Adapter{source: source}
	observables := make([]metric.Observable, 0, len(intDescriptors)+len(floatDescriptors)+1)

	up, err := meter.Int64ObservableGauge("meldbase.up", metric.WithDescription("Whether the latest sampled database is open."), metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	adapter.ints = append(adapter.ints, intObservation{descriptor: intDescriptor{name: "meldbase.up"}, instrument: up})
	observables = append(observables, up)
	for _, descriptor := range intDescriptors {
		var instrument metric.Int64Observable
		switch descriptor.kind {
		case intGauge:
			instrument, err = meter.Int64ObservableGauge(descriptor.name, metric.WithDescription(descriptor.description), metric.WithUnit(descriptor.unit))
		case intCounter:
			instrument, err = meter.Int64ObservableCounter(descriptor.name, metric.WithDescription(descriptor.description), metric.WithUnit(descriptor.unit))
		default:
			err = errors.New("meldbase otel: invalid integer instrument kind")
		}
		if err != nil {
			return nil, err
		}
		adapter.ints = append(adapter.ints, intObservation{descriptor: descriptor, instrument: instrument})
		observables = append(observables, instrument)
	}
	for _, descriptor := range floatDescriptors {
		var instrument metric.Float64Observable
		switch descriptor.kind {
		case floatGauge:
			instrument, err = meter.Float64ObservableGauge(descriptor.name, metric.WithDescription(descriptor.description), metric.WithUnit(descriptor.unit))
		case floatCounter:
			instrument, err = meter.Float64ObservableCounter(descriptor.name, metric.WithDescription(descriptor.description), metric.WithUnit(descriptor.unit))
		default:
			err = errors.New("meldbase otel: invalid floating-point instrument kind")
		}
		if err != nil {
			return nil, err
		}
		adapter.floats = append(adapter.floats, floatObservation{descriptor: descriptor, instrument: instrument})
		observables = append(observables, instrument)
	}
	adapter.registration, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		adapter.observe(otelSink{observer: observer})
		return nil
	}, observables...)
	if err != nil {
		return nil, err
	}
	return adapter, nil
}

func (adapter *Adapter) Close() error {
	if adapter == nil {
		return nil
	}
	adapter.closeOnce.Do(func() {
		adapter.closed.Store(true)
		if adapter.registration != nil {
			adapter.closeErr = adapter.registration.Unregister()
		}
	})
	return adapter.closeErr
}

func (adapter *Adapter) Stats() AdapterStats {
	if adapter == nil {
		return AdapterStats{Closed: true}
	}
	return AdapterStats{
		Collections: adapter.collections.Load(), UnavailableSamples: adapter.unavailable.Load(),
		SaturatedMeasurements: adapter.saturations.Load(), Closed: adapter.closed.Load(),
	}
}

type observationSink interface {
	Int64(string, metric.Int64Observable, int64)
	Float64(string, metric.Float64Observable, float64)
}

type otelSink struct{ observer metric.Observer }

func (sink otelSink) Int64(_ string, instrument metric.Int64Observable, value int64) {
	sink.observer.ObserveInt64(instrument, value)
}
func (sink otelSink) Float64(_ string, instrument metric.Float64Observable, value float64) {
	sink.observer.ObserveFloat64(instrument, value)
}

func (adapter *Adapter) observe(sink observationSink) {
	if adapter == nil || sink == nil || adapter.closed.Load() {
		return
	}
	adapter.collections.Add(1)
	sample, ok := adapter.source.Latest()
	valid := ok && sample.Version == admin.SchemaVersion
	up := uint64(0)
	if valid && !sample.Stats.Closed {
		up = 1
	}
	sink.Int64("meldbase.up", adapter.ints[0].instrument, int64(up))
	if !valid {
		adapter.unavailable.Add(1)
		return
	}
	for _, observation := range adapter.ints[1:] {
		value, available := observation.descriptor.read(sample)
		if !available {
			continue
		}
		sink.Int64(observation.descriptor.name, observation.instrument, adapter.toInt64(value))
	}
	for _, observation := range adapter.floats {
		value, available := observation.descriptor.read(sample)
		if available && value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
			sink.Float64(observation.descriptor.name, observation.instrument, value)
		}
	}
}

func (adapter *Adapter) toInt64(value uint64) int64 {
	if value > math.MaxInt64 {
		adapter.saturations.Add(1)
		return math.MaxInt64
	}
	return int64(value)
}

func available(value uint64) (uint64, bool) { return value, true }
func boolValue(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}
func serverValue(read func(admin.Sample) uint64) func(admin.Sample) (uint64, bool) {
	return func(sample admin.Sample) (uint64, bool) {
		if sample.Server == nil {
			return 0, false
		}
		return read(sample), true
	}
}
func healthValue(read func(admin.HealthStatus) admin.HealthLevel) func(admin.Sample) (uint64, bool) {
	return func(sample admin.Sample) (uint64, bool) { return read(sample.Health).Severity() }
}
func seconds(nanos uint64) float64 { return float64(nanos) / float64(time.Second) }
func durationSeconds(value time.Duration) float64 {
	if value <= 0 {
		return 0
	}
	return float64(value) / float64(time.Second)
}

var intDescriptors = []intDescriptor{
	{name: "meldbase.health.overall", description: "Derived overall health: 0 healthy, 1 degraded, 2 critical.", unit: "1", kind: intGauge, read: healthValue(func(h admin.HealthStatus) admin.HealthLevel { return h.Overall })},
	{name: "meldbase.health.database", description: "Derived database health: 0 healthy, 1 degraded, 2 critical.", unit: "1", kind: intGauge, read: healthValue(func(h admin.HealthStatus) admin.HealthLevel { return h.Database })},
	{name: "meldbase.health.durability", description: "Derived durability health: 0 healthy, 1 degraded, 2 critical.", unit: "1", kind: intGauge, read: healthValue(func(h admin.HealthStatus) admin.HealthLevel { return h.Durability })},
	{name: "meldbase.health.storage", description: "Derived storage health: 0 healthy, 1 degraded, 2 critical.", unit: "1", kind: intGauge, read: healthValue(func(h admin.HealthStatus) admin.HealthLevel { return h.Storage })},
	{name: "meldbase.health.realtime", description: "Derived realtime health: 0 healthy, 1 degraded, 2 critical.", unit: "1", kind: intGauge, read: healthValue(func(h admin.HealthStatus) admin.HealthLevel { return h.Realtime })},
	{name: "meldbase.health.telemetry", description: "Derived telemetry health: 0 healthy, 1 degraded, 2 critical.", unit: "1", kind: intGauge, read: healthValue(func(h admin.HealthStatus) admin.HealthLevel { return h.Telemetry })},
	{name: "meldbase.health.transport", description: "Derived transport health: 0 healthy, 1 degraded, 2 critical.", unit: "1", kind: intGauge, read: healthValue(func(h admin.HealthStatus) admin.HealthLevel { return h.Transport })},
	{name: "meldbase.database.write_disabled", description: "Whether a fail-stop durability error has disabled writes.", unit: "1", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(boolValue(s.Stats.WritesDisabled)) }},
	{name: "meldbase.recovery.performed", description: "Whether startup performed a bounded recovery action.", unit: "1", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(boolValue(s.Stats.Recovery.Recovered)) }},
	{name: "meldbase.recovery.fallback", description: "Whether startup selected an older valid Meta root.", unit: "1", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(boolValue(s.Stats.Recovery.FallbackToOlderRoot)) }},
	{name: "meldbase.recovery.meta_redundancy_degraded", description: "Whether redundant Meta roots were degraded at startup.", unit: "1", kind: intGauge, read: func(s admin.Sample) (uint64, bool) {
		return available(boolValue(s.Stats.Recovery.MetaRedundancyDegraded))
	}},
	{name: "meldbase.recovery.acceleration_degraded", description: "Whether startup discarded an optional acceleration structure.", unit: "1", kind: intGauge, read: func(s admin.Sample) (uint64, bool) {
		return available(boolValue(s.Stats.Recovery.AccelerationDegraded))
	}},
	{name: "meldbase.recovery.meta.checksum_valid", description: "Checksum-valid Meta slots observed at startup.", unit: "{slot}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(uint64(s.Stats.Recovery.ChecksumValidMetaSlots)) }},
	{name: "meldbase.recovery.meta.root_valid", description: "Root-valid Meta slots observed at startup.", unit: "{slot}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(uint64(s.Stats.Recovery.RootValidMetaSlots)) }},
	{name: "meldbase.recovery.main_tail_removed", description: "Provably incomplete main-file tail bytes removed at startup.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Recovery.MainTailBytesRemoved) }},
	{name: "meldbase.recovery.wal_replayed", description: "Complete V1 WAL records replayed at startup.", unit: "{record}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Recovery.WALRecordsReplayed) }},
	{name: "meldbase.recovery.wal_tail_removed", description: "Provably incomplete V1 WAL tail bytes removed at startup.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Recovery.WALTailBytesRemoved) }},
	{name: "meldbase.commit.sequence", description: "Current logical commit sequence.", unit: "{commit}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.CommitSequence) }},
	{name: "meldbase.storage.generation", description: "Current physical V2 publication generation.", unit: "{generation}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.Generation) }},
	{name: "meldbase.storage.rollback.protected", description: "Whether acknowledged V2 commits are gated by an external rollback anchor.", unit: "1", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(boolValue(s.Stats.Storage.RollbackProtected)) }},
	{name: "meldbase.storage.rollback.anchor.sequence", description: "Last rollback-anchor sequence durably read back in this process.", unit: "{commit}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.RollbackAnchorSequence) }},
	{name: "meldbase.storage.rollback.anchor.generation", description: "Last rollback-anchor generation durably read back in this process.", unit: "{generation}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.RollbackAnchorGeneration) }},
	{name: "meldbase.storage.rollback.anchor.lag", description: "Logical commits by which the database is ahead of its rollback anchor.", unit: "{commit}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) {
		if s.Stats.Storage.RollbackProtected && s.Stats.Storage.CommitSequence > s.Stats.Storage.RollbackAnchorSequence {
			return available(s.Stats.Storage.CommitSequence - s.Stats.Storage.RollbackAnchorSequence)
		}
		return available(0)
	}},
	{name: "meldbase.storage.rollback.anchor.generation_lag", description: "Physical generations by which the database is ahead of its rollback anchor.", unit: "{generation}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) {
		if s.Stats.Storage.RollbackProtected && s.Stats.Storage.Generation > s.Stats.Storage.RollbackAnchorGeneration {
			return available(s.Stats.Storage.Generation - s.Stats.Storage.RollbackAnchorGeneration)
		}
		return available(0)
	}},
	{name: "meldbase.storage.rollback.anchor.failure", description: "Rollback-anchor durable save or read-back failures.", unit: "{failure}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.RollbackAnchorFailures) }},
	{name: "meldbase.storage.rollback.anchor.replica", description: "Configured rollback-anchor replicas.", unit: "{replica}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.RollbackAnchorStore.Replicas) }},
	{name: "meldbase.storage.rollback.anchor.quorum", description: "Rollback-anchor replicas required for a majority.", unit: "{replica}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.RollbackAnchorStore.Quorum) }},
	{name: "meldbase.storage.rollback.anchor.store.load", description: "Rollback-anchor store load operations.", unit: "{operation}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.RollbackAnchorStore.Loads) }},
	{name: "meldbase.storage.rollback.anchor.store.advance", description: "Rollback-anchor store advance operations.", unit: "{operation}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.RollbackAnchorStore.Advances) }},
	{name: "meldbase.storage.rollback.anchor.endpoint.failure", description: "Rollback-anchor endpoint failures observed before an operation completed.", unit: "{failure}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) {
		return available(s.Stats.Storage.RollbackAnchorStore.EndpointFailures)
	}},
	{name: "meldbase.storage.rollback.anchor.quorum.failure", description: "Rollback-anchor operations that failed to reach quorum.", unit: "{failure}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) {
		return available(s.Stats.Storage.RollbackAnchorStore.QuorumFailures)
	}},
	{name: "meldbase.storage.rollback.anchor.conflict", description: "Rollback-anchor incomparable-history or identity conflicts.", unit: "{conflict}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.RollbackAnchorStore.Conflicts) }},
	{name: "meldbase.storage.rollback.anchor.authentication.failure", description: "Rollback-anchor authentication failures.", unit: "{failure}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) {
		return available(s.Stats.Storage.RollbackAnchorStore.AuthenticationFailures)
	}},
	{name: "meldbase.storage.rollback.anchor.protocol.failure", description: "Rollback-anchor protocol validation failures.", unit: "{failure}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) {
		return available(s.Stats.Storage.RollbackAnchorStore.ProtocolFailures)
	}},
	{name: "meldbase.storage.rollback.anchor.configuration.failure", description: "Rollback-anchor static configuration or member identity failures.", unit: "{failure}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) {
		return available(s.Stats.Storage.RollbackAnchorStore.ConfigurationFailures)
	}},
	{name: "meldbase.collection.count", description: "Current collections.", unit: "{collection}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Collections) }},
	{name: "meldbase.document.count", description: "Current documents.", unit: "{document}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Documents) }},
	{name: "meldbase.index.count", description: "Current secondary indexes.", unit: "{index}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Indexes) }},
	{name: "meldbase.index.build.active", description: "Index builds currently active.", unit: "{build}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.Active) }},
	{name: "meldbase.index.build.persistent", description: "Durable unfinished index builds.", unit: "{build}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.Persistent) }},
	{name: "meldbase.index.build.scanning", description: "Durable index builds scanning their source snapshot.", unit: "{build}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.Scanning) }},
	{name: "meldbase.index.build.catching_up", description: "Durable index builds replaying retained commits.", unit: "{build}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.CatchingUp) }},
	{name: "meldbase.index.build.ready", description: "Durable index builds ready for atomic publication.", unit: "{build}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.Ready) }},
	{name: "meldbase.index.build.failed_persistent", description: "Durable index builds stopped in a terminal failed state.", unit: "{build}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.PersistentFailed) }},
	{name: "meldbase.index.build.retention_lease.active", description: "Whether an unfinished durable index build currently pins Commit Log history.", unit: "1", kind: intGauge, read: func(s admin.Sample) (uint64, bool) {
		return available(boolValue(s.Stats.IndexBuilds.RetentionLeaseActive))
	}},
	{name: "meldbase.index.build.retention.pressure", description: "Whether a durable index-build watermark is preventing Commit Log retention from meeting its configured budget.", unit: "1", kind: intGauge, read: func(s admin.Sample) (uint64, bool) {
		return available(boolValue(s.Stats.IndexBuilds.RetentionPressure))
	}},
	{name: "meldbase.index.build.persistent_entry", description: "Entries in unfinished durable index shadows.", unit: "{entry}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.PersistentEntries) }},
	{name: "meldbase.index.build.persistent_bytes", description: "Canonical Secondary bytes in unfinished durable index shadows.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.PersistentBytes) }},
	{name: "meldbase.index.build.scheduler.run", description: "Background index-build scheduler time quanta started.", unit: "{run}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.SchedulerRuns) }},
	{name: "meldbase.index.build.scheduler.yield", description: "Background index-build scheduler time quanta yielded at their deadline.", unit: "{yield}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.SchedulerYields) }},
	{name: "meldbase.index.build.scheduler.failure", description: "Background index builds durably failed or encountered an unexpected failure.", unit: "{failure}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.SchedulerFailures) }},
	{name: "meldbase.index.build.attempt", description: "Index builds attempted.", unit: "{build}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.Attempts) }},
	{name: "meldbase.index.build.completed", description: "Index builds completed.", unit: "{build}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.Completed) }},
	{name: "meldbase.index.build.failed", description: "Index builds failed.", unit: "{build}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.Failed) }},
	{name: "meldbase.index.build.retry", description: "Optimistic index-build snapshot retries.", unit: "{retry}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.Retries) }},
	{name: "meldbase.index.build.conflict", description: "Index-build snapshot conflicts.", unit: "{conflict}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.Conflicts) }},
	{name: "meldbase.index.build.entry.last", description: "Entries admitted by the last index build.", unit: "{entry}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.LastEntries) }},
	{name: "meldbase.index.build.bytes.last", description: "Canonical Secondary bytes admitted by the last index build.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.IndexBuilds.LastBytes) }},
	{name: "meldbase.commit.count", description: "Committed logical transactions.", unit: "{commit}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Commits.Total) }},
	{name: "meldbase.commit.change.count", description: "Logical changes in committed transactions.", unit: "{change}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Commits.Changes) }},
	{name: "meldbase.write_transaction.active", description: "Public optimistic write transaction callbacks currently active.", unit: "{transaction}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Transactions.Active) }},
	{name: "meldbase.write_transaction.started", description: "Public optimistic write transaction callbacks started.", unit: "{transaction}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Transactions.Started) }},
	{name: "meldbase.write_transaction.committed", description: "Public optimistic write transactions committed.", unit: "{transaction}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Transactions.Committed) }},
	{name: "meldbase.write_transaction.noop", description: "Public optimistic write transactions completed without an effective change.", unit: "{transaction}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Transactions.Noops) }},
	{name: "meldbase.write_transaction.conflict", description: "Public optimistic write transactions rejected by point read-set conflicts.", unit: "{transaction}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Transactions.Conflicts) }},
	{name: "meldbase.write_transaction.aborted", description: "Public optimistic write transactions aborted for non-conflict reasons.", unit: "{transaction}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Transactions.Aborted) }},
	{name: "meldbase.resource.limit.rejection", description: "Operations rejected by resource admission limits.", unit: "{event}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Resources.Rejections) }},
	{name: "meldbase.resource.document.max", description: "Configured maximum canonical bytes per document.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Resources.Limits.MaxDocumentBytes) }},
	{name: "meldbase.resource.transaction.max", description: "Configured maximum canonical document bytes per transaction.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Resources.Limits.MaxTransactionBytes) }},
	{name: "meldbase.resource.transaction.change.max", description: "Configured maximum logical changes per transaction.", unit: "{change}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Resources.Limits.MaxTransactionChanges) }},
	{name: "meldbase.resource.index_build.entry.max", description: "Configured maximum entries per index build.", unit: "{entry}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Resources.Limits.MaxIndexBuildEntries) }},
	{name: "meldbase.resource.index_build.max", description: "Configured maximum canonical Secondary bytes per index build.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Resources.Limits.MaxIndexBuildBytes) }},
	{name: "meldbase.query.count", description: "Completed public queries.", unit: "{query}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Queries.Total) }},
	{name: "meldbase.query.failure.count", description: "Failed public queries.", unit: "{query}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Queries.Failed) }},
	{name: "meldbase.query.active", description: "Active lazy query cursors.", unit: "{query}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Queries.ActiveCursors) }},
	{name: "meldbase.query.document.examined", description: "Documents examined by completed queries.", unit: "{document}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Queries.DocumentsExamined) }},
	{name: "meldbase.query.document.returned", description: "Documents returned by completed queries.", unit: "{document}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Queries.DocumentsReturned) }},
	{name: "meldbase.realtime.view.count", description: "Current shared reactive views.", unit: "{view}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Realtime.SharedViews) }},
	{name: "meldbase.realtime.subscriber.count", description: "Current reactive query subscribers.", unit: "{subscriber}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Realtime.QuerySubscribers) }},
	{name: "meldbase.realtime.pending.batch", description: "Pending reactive commit batches.", unit: "{batch}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Realtime.PendingBatches) }},
	{name: "meldbase.realtime.pending.batch_capacity", description: "Fixed reactive pending-batch capacity.", unit: "{batch}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Realtime.PendingBatchCapacity) }},
	{name: "meldbase.realtime.pending.change_capacity", description: "Fixed reactive pending-change capacity.", unit: "{change}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Realtime.PendingChangeCapacity) }},
	{name: "meldbase.realtime.queue.overflow", description: "Reactive queue overflow fallbacks.", unit: "{event}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Realtime.QueueOverflows) }},
	{name: "meldbase.realtime.slow_consumer", description: "Disconnected slow business-data consumers.", unit: "{event}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Realtime.SlowConsumers) }},
	{name: "meldbase.realtime.incremental.batch", description: "Commit batches applied incrementally.", unit: "{batch}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Realtime.IncrementalBatches) }},
	{name: "meldbase.realtime.full_recompute", description: "Full reactive view recomputations.", unit: "{event}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Realtime.FullViewRecomputes) }},
	{name: "meldbase.realtime.delta.delivery", description: "Reactive delta deliveries.", unit: "{delivery}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Realtime.DeltaDeliveries) }},
	{name: "meldbase.wal.current.size", description: "Current V1 WAL length.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Durability.WALCurrentBytes) }},
	{name: "meldbase.wal.current.commit", description: "Current V1 WAL commits newer than checkpoint.", unit: "{commit}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Durability.WALCurrentCommits) }},
	{name: "meldbase.wal.append", description: "Successful V1 WAL appends.", unit: "{append}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Durability.WALAppends) }},
	{name: "meldbase.wal.payload", description: "Payload bytes appended to V1 WAL.", unit: "By", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Durability.WALPayloadBytes) }},
	{name: "meldbase.wal.append.failure", description: "Failed V1 WAL appends.", unit: "{event}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Durability.WALAppendFailures) }},
	{name: "meldbase.checkpoint.attempt", description: "V1 checkpoint attempts.", unit: "{checkpoint}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Durability.CheckpointAttempts) }},
	{name: "meldbase.checkpoint.completed", description: "Completed V1 checkpoints.", unit: "{checkpoint}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Durability.CheckpointsCompleted) }},
	{name: "meldbase.checkpoint.failure", description: "Failed V1 checkpoints.", unit: "{checkpoint}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Durability.CheckpointFailures) }},
	{name: "meldbase.checkpoint.automatic", description: "V1 checkpoints triggered automatically.", unit: "{checkpoint}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Durability.AutomaticCheckpoints) }},
	{name: "meldbase.storage.physical.page", description: "Current physical page high-water count.", unit: "{page}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.PhysicalPages) }},
	{name: "meldbase.storage.size", description: "Current physical file high-water bytes.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.StorageUsedBytes) }},
	{name: "meldbase.storage.max_size", description: "Configured physical file high-water quota.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.StorageMaxBytes) }},
	{name: "meldbase.storage.size_overage", description: "Existing physical bytes above the configured quota.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.StorageByteOverage) }},
	{name: "meldbase.storage.quota.exhausted", description: "Whether append capacity and reusable pages are exhausted.", unit: "1", kind: intGauge, read: func(s admin.Sample) (uint64, bool) {
		return available(boolValue(s.Stats.Storage.StorageQuotaExhausted))
	}},
	{name: "meldbase.storage.limit.rejection", description: "Transactions rejected before I/O by the physical storage quota.", unit: "{transaction}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.StorageLimitRejections) }},
	{name: "meldbase.storage.reusable.page", description: "Current reusable page count.", unit: "{page}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.ReusablePages) }},
	{name: "meldbase.storage.reader.active", description: "Current pinned storage readers.", unit: "{reader}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.ActiveReaders) }},
	{name: "meldbase.storage.replay_lease.active", description: "Current pinned replay leases.", unit: "{lease}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.ActiveReplayLeases) }},
	{name: "meldbase.storage.retained_commit", description: "Current logical commits retained for replay.", unit: "{commit}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.RetainedCommits) }},
	{name: "meldbase.storage.retention.max", description: "Configured normal Commit Log window.", unit: "{commit}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.CommitRetentionMax) }},
	{name: "meldbase.storage.retention.overage", description: "Retained commits above the configured window.", unit: "{commit}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.CommitRetentionOverage) }},
	{name: "meldbase.storage.retained_commit.size", description: "Canonical logical bytes currently retained in the Commit Log.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.RetainedCommitBytes) }},
	{name: "meldbase.storage.retention.max_size", description: "Configured normal Commit Log logical-byte budget.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.CommitRetentionMaxBytes) }},
	{name: "meldbase.storage.retention.size_overage", description: "Retained logical bytes above the configured budget.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.CommitRetentionByteOverage) }},
	{name: "meldbase.storage.retention.pressure", description: "Whether the configured count or byte retention budget is unsatisfied.", unit: "1", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(boolValue(s.Stats.Storage.RetentionPressure)) }},
	{name: "meldbase.storage.retention.pruned", description: "Commit Log entries pruned by successful publications.", unit: "{commit}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.RetentionPrunedCommits) }},
	{name: "meldbase.storage.retention.pressure_event", description: "Commits whose retention watermark was blocked by replay pins.", unit: "{event}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.RetentionPressureEvents) }},
	{name: "meldbase.storage.tree.split", description: "Published B+Tree splits.", unit: "{event}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.TreeSplits) }},
	{name: "meldbase.storage.tree.merge", description: "Published B+Tree sibling merges.", unit: "{event}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.TreeMerges) }},
	{name: "meldbase.storage.transaction.committed", description: "Committed V2 storage transactions.", unit: "{transaction}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.CommittedTransactions) }},
	{name: "meldbase.storage.transaction.rejected", description: "Rejected V2 storage transactions.", unit: "{transaction}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Storage.RejectedTransactions) }},
	{name: "meldbase.compaction.attempt", description: "Logical compaction attempts.", unit: "{compaction}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Compaction.Attempts) }},
	{name: "meldbase.compaction.failure", description: "Failed logical compactions.", unit: "{compaction}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Compaction.Failed) }},
	{name: "meldbase.reclamation.attempt", description: "Page reclamation attempts.", unit: "{reclamation}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Reclamation.Attempts) }},
	{name: "meldbase.reclamation.scan", description: "Complete page-graph scans, including optimistic retries.", unit: "{scan}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Reclamation.Scans) }},
	{name: "meldbase.reclamation.conflict", description: "Online reclamations discarded after commit conflicts.", unit: "{reclamation}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Reclamation.Conflicts) }},
	{name: "meldbase.reclamation.failure", description: "Failed page reclamations.", unit: "{reclamation}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Reclamation.Failed) }},
	{name: "meldbase.backup.active", description: "Current active physical V2 backups.", unit: "{backup}", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Backup.Active) }},
	{name: "meldbase.backup.attempt", description: "Physical V2 backup attempts.", unit: "{backup}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Backup.Attempts) }},
	{name: "meldbase.backup.completed", description: "Completed physical V2 backups.", unit: "{backup}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Backup.Completed) }},
	{name: "meldbase.backup.failure", description: "Failed physical V2 backups.", unit: "{backup}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Backup.Failed) }},
	{name: "meldbase.backup.last.size", description: "Bytes copied by the last physical V2 backup attempt.", unit: "By", kind: intGauge, read: func(s admin.Sample) (uint64, bool) { return available(s.Stats.Backup.LastBytes) }},
	{name: "meldbase.admin.delivery.dropped", description: "Stale admin subscriber samples replaced.", unit: "{sample}", kind: intCounter, read: func(s admin.Sample) (uint64, bool) { return available(s.Sampler.DroppedDeliveries) }},
	{name: "meldbase.server.connection.active", description: "Current authenticated realtime connections.", unit: "{connection}", kind: intGauge, read: serverValue(func(s admin.Sample) uint64 { return s.Server.ActiveConnections })},
	{name: "meldbase.rpc.request", description: "Valid RPC request envelopes.", unit: "{request}", kind: intCounter, read: serverValue(func(s admin.Sample) uint64 { return s.Server.RPCRequests })},
	{name: "meldbase.rpc.active", description: "Currently executing RPC handlers.", unit: "{call}", kind: intGauge, read: serverValue(func(s admin.Sample) uint64 { return s.Server.RPCActive })},
	{name: "meldbase.rpc.failure", description: "RPC application or internal failures.", unit: "{call}", kind: intCounter, read: serverValue(func(s admin.Sample) uint64 { return s.Server.RPCFailed })},
	{name: "meldbase.rpc.busy", description: "RPC calls rejected by concurrency budgets.", unit: "{call}", kind: intCounter, read: serverValue(func(s admin.Sample) uint64 { return s.Server.RPCBusy })},
	{name: "meldbase.worker.connected", description: "Current authenticated server workers.", unit: "{worker}", kind: intGauge, read: serverValue(func(s admin.Sample) uint64 { return s.Server.Worker.ConnectedWorkers })},
	{name: "meldbase.worker.protocol.failure", description: "Worker sessions closed for protocol failures.", unit: "{event}", kind: intCounter, read: serverValue(func(s admin.Sample) uint64 { return s.Server.Worker.ProtocolFailures })},
	{name: "meldbase.worker.policy.invalidation", description: "Durable worker publication policy invalidations.", unit: "{event}", kind: intCounter, read: serverValue(func(s admin.Sample) uint64 { return s.Server.Worker.PolicyInvalidations })},
}

var floatDescriptors = []floatDescriptor{
	{name: "meldbase.index.build.duration.last", description: "Duration of the last index build.", unit: "s", kind: floatGauge, read: func(s admin.Sample) (float64, bool) { return durationSeconds(s.Stats.IndexBuilds.LastDuration), true }},
	{name: "meldbase.index.build.duration.max", description: "Maximum index-build duration in this process session.", unit: "s", kind: floatGauge, read: func(s admin.Sample) (float64, bool) { return durationSeconds(s.Stats.IndexBuilds.MaxDuration), true }},
	{name: "meldbase.wal.append.time", description: "Accumulated V1 WAL append time.", unit: "s", kind: floatCounter, read: func(s admin.Sample) (float64, bool) { return seconds(s.Stats.Durability.WALAppendNanos), true }},
	{name: "meldbase.wal.append.max_duration", description: "Maximum V1 WAL append duration.", unit: "s", kind: floatGauge, read: func(s admin.Sample) (float64, bool) {
		return durationSeconds(s.Stats.Durability.WALAppendMaxLatency), true
	}},
	{name: "meldbase.checkpoint.time", description: "Accumulated successful V1 checkpoint time.", unit: "s", kind: floatCounter, read: func(s admin.Sample) (float64, bool) { return seconds(s.Stats.Durability.CheckpointNanos), true }},
	{name: "meldbase.checkpoint.max_duration", description: "Maximum successful V1 checkpoint duration.", unit: "s", kind: floatGauge, read: func(s admin.Sample) (float64, bool) {
		return durationSeconds(s.Stats.Durability.CheckpointMaxLatency), true
	}},
	{name: "meldbase.storage.commit.time", description: "Accumulated V2 storage commit time.", unit: "s", kind: floatCounter, read: func(s admin.Sample) (float64, bool) { return seconds(s.Stats.Storage.CommitNanos), true }},
	{name: "meldbase.storage.commit.max_duration", description: "Maximum V2 storage commit duration.", unit: "s", kind: floatGauge, read: func(s admin.Sample) (float64, bool) { return durationSeconds(s.Stats.Storage.CommitMaxLatency), true }},
	{name: "meldbase.storage.rollback.anchor.time", description: "Accumulated synchronous rollback-anchor update time.", unit: "s", kind: floatCounter, read: func(s admin.Sample) (float64, bool) { return seconds(s.Stats.Storage.RollbackAnchorNanos), true }},
	{name: "meldbase.storage.rollback.anchor.max_duration", description: "Maximum synchronous rollback-anchor update duration.", unit: "s", kind: floatGauge, read: func(s admin.Sample) (float64, bool) {
		return durationSeconds(s.Stats.Storage.RollbackAnchorMaxLatency), true
	}},
	{name: "meldbase.storage.rollback.anchor.timeout", description: "Configured deadline for each rollback-anchor interaction.", unit: "s", kind: floatGauge, read: func(s admin.Sample) (float64, bool) {
		return durationSeconds(s.Stats.Storage.RollbackAnchorTimeout), true
	}},
	{name: "meldbase.backup.last_duration", description: "Duration of the last physical V2 backup attempt.", unit: "s", kind: floatGauge, read: func(s admin.Sample) (float64, bool) { return durationSeconds(s.Stats.Backup.LastDuration), true }},
	{name: "meldbase.cache.page.hit_ratio", description: "Latest sampled V2 page-cache hit ratio.", unit: "1", kind: floatGauge, read: func(s admin.Sample) (float64, bool) { return s.Rates.PageCacheHitRatio, s.Rates.Valid }},
	{name: "meldbase.cache.document.hit_ratio", description: "Latest sampled decoded-document-cache hit ratio.", unit: "1", kind: floatGauge, read: func(s admin.Sample) (float64, bool) { return s.Rates.DocumentCacheHitRatio, s.Rates.Valid }},
	{name: "meldbase.rpc.time", description: "Accumulated RPC handler execution time.", unit: "s", kind: floatCounter, read: func(s admin.Sample) (float64, bool) {
		if s.Server == nil {
			return 0, false
		}
		return seconds(s.Server.RPCTotalNanos), true
	}},
	{name: "meldbase.rpc.max_duration", description: "Maximum RPC handler execution duration.", unit: "s", kind: floatGauge, read: func(s admin.Sample) (float64, bool) {
		if s.Server == nil {
			return 0, false
		}
		return durationSeconds(s.Server.RPCMaxLatency), true
	}},
}
