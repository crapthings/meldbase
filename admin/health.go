package admin

import (
	"github.com/crapthings/meldbase"
	meldserver "github.com/crapthings/meldbase/server"
)

type HealthLevel string

const (
	HealthUnavailable HealthLevel = "unavailable"
	HealthHealthy     HealthLevel = "healthy"
	HealthDegraded    HealthLevel = "degraded"
	HealthCritical    HealthLevel = "critical"
)

// HealthSignals is a fixed-cardinality explanation of the current assessment.
// Event signals describe increases during the latest sample window; state
// signals remain set while the underlying condition remains true.
type HealthSignals struct {
	DatabaseClosed               bool `json:"databaseClosed"`
	WritesDisabled               bool `json:"writesDisabled"`
	ReactiveQueuePressure        bool `json:"reactiveQueuePressure"`
	ReactiveQueueOverflow        bool `json:"reactiveQueueOverflow"`
	SlowConsumer                 bool `json:"slowConsumer"`
	PersistentFreeSpaceDiscarded bool `json:"persistentFreeSpaceDiscarded"`
	CommitRetentionPressure      bool `json:"commitRetentionPressure"`
	IndexBuildFailed             bool `json:"indexBuildFailed"`
	IndexBuildRetentionPressure  bool `json:"indexBuildRetentionPressure"`
	StorageQuotaExhausted        bool `json:"storageQuotaExhausted"`
	StorageLimitRejected         bool `json:"storageLimitRejected"`
	DurabilityFailure            bool `json:"durabilityFailure"`
	RollbackAnchorDegraded       bool `json:"rollbackAnchorDegraded"`
	TelemetryDeliveryDropped     bool `json:"telemetryDeliveryDropped"`
	TransportBusy                bool `json:"transportBusy"`
	RPCOutcomeUnknown            bool `json:"rpcOutcomeUnknown"`
	WorkerProtocolFailure        bool `json:"workerProtocolFailure"`
}

type HealthStatus struct {
	Overall    HealthLevel   `json:"overall"`
	Database   HealthLevel   `json:"database"`
	Durability HealthLevel   `json:"durability"`
	Storage    HealthLevel   `json:"storage"`
	Realtime   HealthLevel   `json:"realtime"`
	Telemetry  HealthLevel   `json:"telemetry"`
	Transport  HealthLevel   `json:"transport"`
	Signals    HealthSignals `json:"signals"`
}

// Severity maps stable health enums to aggregate metric values: healthy=0,
// degraded=1, critical=2. Unavailable is intentionally omitted by exporters.
func (level HealthLevel) Severity() (uint64, bool) {
	switch level {
	case HealthHealthy:
		return 0, true
	case HealthDegraded:
		return 1, true
	case HealthCritical:
		return 2, true
	default:
		return 0, false
	}
}

func assessHealth(previous *Sample, current Sample) HealthStatus {
	health := HealthStatus{
		Overall: HealthHealthy, Database: HealthHealthy, Durability: HealthHealthy,
		Storage: HealthHealthy, Realtime: HealthHealthy, Telemetry: HealthHealthy,
		Transport: HealthUnavailable,
	}
	health.Signals.DatabaseClosed = current.Stats.Closed
	health.Signals.WritesDisabled = current.Stats.WritesDisabled
	if health.Signals.DatabaseClosed {
		health.Database = HealthCritical
	}
	if health.Signals.WritesDisabled {
		health.Database = HealthCritical
		health.Durability = HealthCritical
	}
	if current.Stats.Storage.FreeSpaceLoadFailures > 0 && !current.Stats.Storage.PersistentFreeSpace {
		health.Signals.PersistentFreeSpaceDiscarded = true
		health.Storage = HealthDegraded
	}
	if current.Stats.Storage.RetentionPressure {
		health.Signals.CommitRetentionPressure = true
		health.Storage = HealthDegraded
	}
	if current.Stats.IndexBuilds.PersistentFailed > 0 {
		health.Signals.IndexBuildFailed = true
		health.Storage = HealthDegraded
	}
	if current.Stats.IndexBuilds.RetentionPressure {
		health.Signals.IndexBuildRetentionPressure = true
		health.Storage = HealthDegraded
	}
	if current.Stats.Storage.StorageQuotaExhausted {
		health.Signals.StorageQuotaExhausted = true
		health.Storage = HealthDegraded
	}

	pressure := reactivePressure(current.Stats.Realtime)
	if pressure >= 0.9 {
		health.Signals.ReactiveQueuePressure = true
		health.Realtime = HealthCritical
	} else if pressure >= 0.5 {
		health.Signals.ReactiveQueuePressure = true
		health.Realtime = HealthDegraded
	}

	if previous != nil && sameDatabaseSession(previous.Stats, current.Stats) {
		health.Signals.ReactiveQueueOverflow = increased(previous.Stats.Realtime.QueueOverflows, current.Stats.Realtime.QueueOverflows)
		health.Signals.SlowConsumer = increased(previous.Stats.Realtime.SlowConsumers, current.Stats.Realtime.SlowConsumers)
		health.Signals.DurabilityFailure = increased(previous.Stats.Durability.WALAppendFailures, current.Stats.Durability.WALAppendFailures) ||
			increased(previous.Stats.Durability.CheckpointFailures, current.Stats.Durability.CheckpointFailures) ||
			increased(previous.Stats.Storage.RollbackAnchorFailures, current.Stats.Storage.RollbackAnchorFailures)
		health.Signals.RollbackAnchorDegraded = increased(previous.Stats.Storage.RollbackAnchorStore.EndpointFailures, current.Stats.Storage.RollbackAnchorStore.EndpointFailures) ||
			increased(previous.Stats.Storage.RollbackAnchorStore.ConfigurationFailures, current.Stats.Storage.RollbackAnchorStore.ConfigurationFailures)
		health.Signals.StorageLimitRejected = increased(previous.Stats.Storage.StorageLimitRejections, current.Stats.Storage.StorageLimitRejections)
		if health.Signals.ReactiveQueueOverflow || health.Signals.SlowConsumer {
			health.Realtime = maxHealth(health.Realtime, HealthDegraded)
		}
		if health.Signals.DurabilityFailure {
			health.Durability = maxHealth(health.Durability, HealthDegraded)
		}
		if health.Signals.RollbackAnchorDegraded {
			health.Durability = maxHealth(health.Durability, HealthDegraded)
		}
		if health.Signals.StorageLimitRejected {
			health.Storage = maxHealth(health.Storage, HealthDegraded)
		}
	}
	if previous != nil && increased(previous.Sampler.DroppedDeliveries, current.Sampler.DroppedDeliveries) {
		health.Signals.TelemetryDeliveryDropped = true
		health.Telemetry = HealthDegraded
	}
	assessTransport(&health, previous, current)
	health.Overall = maxHealth(health.Database, health.Durability, health.Storage, health.Realtime, health.Telemetry, health.Transport)
	return health
}

func assessTransport(health *HealthStatus, previous *Sample, current Sample) {
	if health == nil || current.Server == nil {
		return
	}
	health.Transport = HealthHealthy
	if previous == nil || previous.Server == nil || !sameServerSession(previous.Server, current.Server) {
		return
	}
	health.Signals.TransportBusy = increased(previous.Server.RPCBusy, current.Server.RPCBusy) ||
		increased(previous.Server.Worker.CallsBusy, current.Server.Worker.CallsBusy) ||
		increased(previous.Server.Worker.PolicyBusy, current.Server.Worker.PolicyBusy)
	health.Signals.RPCOutcomeUnknown = increased(previous.Server.RPCIdempotencyUnknown, current.Server.RPCIdempotencyUnknown)
	health.Signals.WorkerProtocolFailure = increased(previous.Server.Worker.ProtocolFailures, current.Server.Worker.ProtocolFailures)
	if health.Signals.TransportBusy || health.Signals.RPCOutcomeUnknown || health.Signals.WorkerProtocolFailure {
		health.Transport = HealthDegraded
	}
}

func reactivePressure(stats meldbase.RealtimeStats) float64 {
	batch := ratio(stats.PendingBatches, stats.PendingBatchCapacity)
	changes := ratio(stats.PendingChanges, stats.PendingChangeCapacity)
	if changes > batch {
		return changes
	}
	return batch
}

func ratio(value, capacity uint64) float64 {
	if capacity == 0 {
		return 0
	}
	return float64(value) / float64(capacity)
}

func sameDatabaseSession(previous, current meldbase.DBStats) bool {
	return !previous.StartedAt.IsZero() && previous.StartedAt == current.StartedAt
}

func sameServerSession(previous, current *meldserver.ServerStats) bool {
	return previous != nil && current != nil && !previous.StartedAt.IsZero() && previous.StartedAt == current.StartedAt
}

func increased(previous, current uint64) bool { return current > previous }

func maxHealth(levels ...HealthLevel) HealthLevel {
	result := HealthHealthy
	severity := uint64(0)
	for _, level := range levels {
		value, available := level.Severity()
		if available && value > severity {
			result, severity = level, value
		}
	}
	return result
}
