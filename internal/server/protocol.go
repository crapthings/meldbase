package server

import (
	"mime"
	"net/http"
	"strings"
)

const (
	// ProtocolVersion is the current realtime/RPC envelope version. Existing
	// versions are immutable; incompatible grammar requires a new version.
	ProtocolVersion                   = 1
	protocolVersion                   = ProtocolVersion
	realtimeTicketCapabilityMediaType = "application/vnd.meldbase.realtime-ticket+json"
	workerCapabilityRequestHeader     = "Meldbase-Protocol"
	workerCapabilityRequestValue      = "capabilities-v1"
)

type protocolDescriptor struct {
	Versions     []int    `json:"versions"`
	Capabilities []string `json:"capabilities"`
}

// fixedProtocolErrorCodes is the engine/transport-owned v1 registry.
// MeldbaseError may additionally carry an application-owned namespaced code;
// those extensions are intentionally not presented as engine codes.
var fixedProtocolErrorCodes = []string{
	"cancelled", "database_unavailable", "delta_failed", "duplicate_key", "forbidden", "forbidden_field",
	"initial_snapshot_failed", "internal", "invalid_aggregate", "invalid_document", "invalid_document_envelope",
	"invalid_json", "invalid_mutation_envelope", "invalid_policy", "invalid_query", "invalid_query_envelope",
	"invalid_request", "invalid_rpc_argument", "invalid_rpc_envelope", "invalid_update", "missing_update",
	"mutation_limit_exceeded", "origin_forbidden", "outcome_unknown", "policy_expired", "preflight_forbidden",
	"request_too_large", "resource_limit_exceeded", "resume_failed", "rpc_busy",
	"rpc_canceled", "rpc_duplicate_request", "rpc_idempotency_conflict", "rpc_idempotency_required",
	"rpc_idempotency_unavailable", "rpc_in_progress", "rpc_not_found", "rpc_outcome_unknown",
	"rpc_result_invalid", "rpc_transaction_conflict", "rpc_transaction_requires_write", "snapshot_failed",
	"subscription_ended", "subscription_failed", "subscription_limit_or_duplicate", "unauthenticated", "unexpected_update",
	"unknown_mutation_action", "worker_busy",
}

func realtimeProtocolDescriptor(config Config) protocolDescriptor {
	capabilities := []string{"query.delta", "query.resume", "rpc", "rpc.cancel"}
	if config.RPCIdempotencyStore != nil {
		capabilities = append(capabilities, "rpc.idempotency")
	}
	if len(config.RPCTransactionalMethods) > 0 || config.RPCTransactionalMethodResolver != nil {
		capabilities = append(capabilities, "rpc.transactional")
	}
	return protocolDescriptor{Versions: []int{protocolVersion}, Capabilities: capabilities}
}

func workerProtocolDescriptor() protocolDescriptor {
	return protocolDescriptor{
		Versions: []int{protocolVersion},
		Capabilities: []string{
			"cancel", "publication.policy", "rpc", "rpc.transactional",
			"transaction.compiled_update", "transaction.invalidate_publication", "transaction.point_operations",
		},
	}
}

func requestsRealtimeCapabilities(request *http.Request) bool {
	if request == nil {
		return false
	}
	for _, item := range strings.Split(request.Header.Get("Accept"), ",") {
		mediaType, parameters, err := mime.ParseMediaType(strings.TrimSpace(item))
		if err == nil && mediaType == realtimeTicketCapabilityMediaType && parameters["capabilities"] == "1" {
			return true
		}
	}
	return false
}

func requestsWorkerCapabilities(request *http.Request) bool {
	return request != nil && request.Header.Get(workerCapabilityRequestHeader) == workerCapabilityRequestValue
}
