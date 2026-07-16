package server

import (
	"sync/atomic"
	"time"
)

// ServerStats is a fixed-cardinality process-session snapshot. It deliberately
// contains no method, principal, tenant, argument, result or error text.
type ServerStats struct {
	CapturedAt               time.Time      `json:"capturedAt"`
	StartedAt                time.Time      `json:"startedAt"`
	ActiveConnections        uint64         `json:"activeConnections"`
	ConnectionsAccepted      uint64         `json:"connectionsAccepted"`
	RPCRequests              uint64         `json:"rpcRequests"`
	RPCActive                uint64         `json:"rpcActive"`
	RPCSucceeded             uint64         `json:"rpcSucceeded"`
	RPCFailed                uint64         `json:"rpcFailed"`
	RPCCanceled              uint64         `json:"rpcCanceled"`
	RPCRejected              uint64         `json:"rpcRejected"`
	RPCBusy                  uint64         `json:"rpcBusy"`
	RPCArguments             uint64         `json:"rpcArguments"`
	RPCRequestBytes          uint64         `json:"rpcRequestBytes"`
	RPCResultBytes           uint64         `json:"rpcResultBytes"`
	RPCTotalNanos            uint64         `json:"rpcTotalNanos"`
	RPCMaxLatency            time.Duration  `json:"rpcMaxLatencyNanos"`
	RPCIdempotencyClaims     uint64         `json:"rpcIdempotencyClaims"`
	RPCIdempotencyReplays    uint64         `json:"rpcIdempotencyReplays"`
	RPCIdempotencyConflicts  uint64         `json:"rpcIdempotencyConflicts"`
	RPCIdempotencyInProgress uint64         `json:"rpcIdempotencyInProgress"`
	RPCIdempotencyUnknown    uint64         `json:"rpcIdempotencyUnknown"`
	RPCIdempotencyFailures   uint64         `json:"rpcIdempotencyFailures"`
	RPCAtomicCommits         uint64         `json:"rpcAtomicCommits"`
	RPCAtomicRollbacks       uint64         `json:"rpcAtomicRollbacks"`
	RPCAtomicNoopCompletions uint64         `json:"rpcAtomicNoopCompletions"`
	Worker                   WorkerHubStats `json:"worker"`
}

type serverMetrics struct {
	activeConnections, connectionsAccepted            atomic.Uint64
	rpcRequests, rpcActive                            atomic.Uint64
	rpcSucceeded, rpcFailed, rpcCanceled              atomic.Uint64
	rpcRejected, rpcBusy                              atomic.Uint64
	rpcArguments, rpcRequestBytes                     atomic.Uint64
	rpcResultBytes, rpcTotalNanos                     atomic.Uint64
	rpcMaxNanos                                       atomic.Uint64
	rpcIdempotencyClaims, rpcIdempotencyReplays       atomic.Uint64
	rpcIdempotencyConflicts, rpcIdempotencyInProgress atomic.Uint64
	rpcIdempotencyUnknown, rpcIdempotencyFailures     atomic.Uint64
	rpcAtomicCommits, rpcAtomicRollbacks              atomic.Uint64
	rpcAtomicNoopCompletions                          atomic.Uint64
}

type rpcMetricSpan struct{ started time.Time }

func (h *Handler) Stats() ServerStats {
	now := time.Now()
	if h == nil {
		return ServerStats{CapturedAt: now}
	}
	workerStats := WorkerHubStats{}
	if hub, ok := h.config.RPCMethodResolver.(*WorkerHub); ok {
		workerStats = hub.Stats()
	} else if hub, ok := h.config.RPCTransactionalMethodResolver.(*WorkerHub); ok {
		workerStats = hub.Stats()
	} else if hub, ok := h.config.QueryPolicyResolver.(*WorkerHub); ok {
		workerStats = hub.Stats()
	}
	return ServerStats{
		CapturedAt: now, StartedAt: h.startedAt,
		ActiveConnections: h.metrics.activeConnections.Load(), ConnectionsAccepted: h.metrics.connectionsAccepted.Load(),
		RPCRequests: h.metrics.rpcRequests.Load(), RPCActive: h.metrics.rpcActive.Load(),
		RPCSucceeded: h.metrics.rpcSucceeded.Load(), RPCFailed: h.metrics.rpcFailed.Load(),
		RPCCanceled: h.metrics.rpcCanceled.Load(), RPCRejected: h.metrics.rpcRejected.Load(), RPCBusy: h.metrics.rpcBusy.Load(),
		RPCArguments: h.metrics.rpcArguments.Load(), RPCRequestBytes: h.metrics.rpcRequestBytes.Load(),
		RPCResultBytes: h.metrics.rpcResultBytes.Load(), RPCTotalNanos: h.metrics.rpcTotalNanos.Load(),
		RPCMaxLatency:        time.Duration(h.metrics.rpcMaxNanos.Load()),
		RPCIdempotencyClaims: h.metrics.rpcIdempotencyClaims.Load(), RPCIdempotencyReplays: h.metrics.rpcIdempotencyReplays.Load(),
		RPCIdempotencyConflicts: h.metrics.rpcIdempotencyConflicts.Load(), RPCIdempotencyInProgress: h.metrics.rpcIdempotencyInProgress.Load(),
		RPCIdempotencyUnknown: h.metrics.rpcIdempotencyUnknown.Load(), RPCIdempotencyFailures: h.metrics.rpcIdempotencyFailures.Load(),
		RPCAtomicCommits: h.metrics.rpcAtomicCommits.Load(), RPCAtomicRollbacks: h.metrics.rpcAtomicRollbacks.Load(),
		RPCAtomicNoopCompletions: h.metrics.rpcAtomicNoopCompletions.Load(),
		Worker:                   workerStats,
	}
}

func (h *Handler) beginRPC(arguments, requestBytes int) rpcMetricSpan {
	h.metrics.rpcActive.Add(1)
	h.metrics.rpcArguments.Add(uint64(arguments))
	h.metrics.rpcRequestBytes.Add(uint64(requestBytes))
	return rpcMetricSpan{started: time.Now()}
}

func (h *Handler) finishRPC(span rpcMetricSpan, outcome string, resultBytes int) {
	h.metrics.rpcActive.Add(^uint64(0))
	elapsed := uint64(time.Since(span.started))
	h.metrics.rpcTotalNanos.Add(elapsed)
	updateServerAtomicMax(&h.metrics.rpcMaxNanos, elapsed)
	switch outcome {
	case "success":
		h.metrics.rpcSucceeded.Add(1)
		h.metrics.rpcResultBytes.Add(uint64(resultBytes))
	case "canceled":
		h.metrics.rpcCanceled.Add(1)
	default:
		h.metrics.rpcFailed.Add(1)
	}
}

func updateServerAtomicMax(target *atomic.Uint64, value uint64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}
