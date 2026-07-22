package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"

	"github.com/crapthings/meldbase/core"
)

var (
	rpcMethodNamePattern        = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,127}$`)
	rpcInternalErrorCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	rpcBusinessErrorCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}(?:\.[a-z][a-z0-9_]{0,31})+$`)
	rpcRequestIDPattern         = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	rpcIdempotencyKeyPattern    = regexp.MustCompile(`^[A-Za-z0-9_-]{22,128}$`)
)

const maxRPCErrorDataBytes = 64 << 10

// RPCMethod is a bounded, authenticated non-atomic application operation.
// Input and results use Meldbase's closed Value model, preserving Int64,
// Date, Binary and object semantics across Go and JavaScript. Any database
// write or external effect a handler reaches outside a WriteTransaction is not
// atomic with its RPC terminal result.
type RPCMethod func(context.Context, Actor, meldbase.Value) (meldbase.Value, error)

// RPCTransactionalMethod stages point writes against a short immutable
// snapshot. A successful result and all staged writes share one durable
// publication with the RPC idempotency terminal record after optimistic commit
// validation.
type RPCTransactionalMethod func(context.Context, Actor, meldbase.Value, *meldbase.WriteTransaction) (meldbase.Value, error)

// RPCMethodResolver resolves dynamic trusted-worker methods. It is consulted
// only after the immutable local registry misses.
type RPCMethodResolver interface {
	ResolveRPCMethod(string) (RPCMethod, bool)
}

// RPCTransactionalMethodResolver is the equivalent dynamic boundary for
// transaction-aware methods.
type RPCTransactionalMethodResolver interface {
	ResolveRPCTransactionalMethod(string) (RPCTransactionalMethod, bool)
}

// RPCAuthorizer is evaluated for every call before input is decoded or
// application code runs. Registration alone never grants call permission.
type RPCAuthorizer interface {
	AuthorizeRPC(context.Context, Actor, string) error
}

// MeldbaseError is an expected, application-owned RPC failure. Code is a
// namespaced stable identifier (for example "orders.already_paid"); Data is an
// optional safe document sent to the caller. Arbitrary handler errors are
// always classified as MeldbaseInternalError instead.
type MeldbaseError struct {
	Code string
	Data meldbase.Document
}

func (err *MeldbaseError) Error() string {
	if err == nil {
		return "meldbase error"
	}
	return "meldbase: " + err.Code
}

// MeldbaseInternalError is Meldbase-owned error state. Applications should
// return MeldbaseError for deliberate business outcomes and let all other
// failures be safely normalized by the server.
type MeldbaseInternalError struct {
	Code   string
	Status int
}

func (err *MeldbaseInternalError) Error() string {
	if err == nil {
		return "meldbase internal error"
	}
	return "meldbase internal: " + err.Code
}

type rpcCallEnvelope struct {
	Version        int              `json:"v"`
	Type           string           `json:"type"`
	RequestID      string           `json:"requestId"`
	IdempotencyKey *string          `json:"idempotencyKey,omitempty"`
	Method         string           `json:"method"`
	Input          *json.RawMessage `json:"input"`
}

type rpcOutcome struct {
	result json.RawMessage
	status int
	code   string
	kind   string
	data   json.RawMessage
}

func validRPCMethodName(name string) bool    { return rpcMethodNamePattern.MatchString(name) }
func validRPCIdempotencyKey(key string) bool { return rpcIdempotencyKeyPattern.MatchString(key) }

func (h *Handler) rpc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("cache-control", "no-store")
	actor, err := h.config.Authenticator.AuthenticateHTTP(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	body, err := readBounded(w, r, h.config.MaxBodyBytes)
	if err != nil {
		writeReadError(w, err)
		return
	}
	if err := meldbase.ValidateStrictJSON(body, h.config.MaxBodyBytes); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	envelope, err := decodeRPCCallEnvelope(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_rpc_envelope")
		return
	}
	h.metrics.rpcRequests.Add(1)
	method, transactionalMethod, standardExists, transactionalExists := h.resolveRPCMethod(envelope.Method)
	if !standardExists && !transactionalExists {
		h.metrics.rpcRejected.Add(1)
		writeRPCError(w, http.StatusNotFound, envelope.RequestID, "rpc_not_found")
		return
	}
	if h.config.RPCAuthorizer == nil || h.config.RPCAuthorizer.AuthorizeRPC(r.Context(), actor, envelope.Method) != nil {
		h.metrics.rpcRejected.Add(1)
		writeRPCError(w, http.StatusForbidden, envelope.RequestID, "forbidden")
		return
	}
	input, err := decodeRPCInput(*envelope.Input, h.config.QueryLimits)
	if err != nil {
		h.metrics.rpcRejected.Add(1)
		writeRPCError(w, http.StatusBadRequest, envelope.RequestID, "invalid_rpc_argument")
		return
	}
	select {
	case h.rpcSlots <- struct{}{}:
		defer func() { <-h.rpcSlots }()
	default:
		h.metrics.rpcBusy.Add(1)
		writeRPCError(w, http.StatusServiceUnavailable, envelope.RequestID, "rpc_busy")
		return
	}
	outcome := h.executeRPC(r.Context(), actor, method, transactionalMethod, envelope, input, len(body))
	if outcome.code != "" {
		writeJSON(w, outcome.status, rpcOutcomeErrorMessage(envelope.RequestID, outcome))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"v": protocolVersion, "type": "result", "requestId": envelope.RequestID, "result": outcome.result,
	})
}

func (h *Handler) resolveRPCMethod(name string) (RPCMethod, RPCTransactionalMethod, bool, bool) {
	method, standardExists := h.config.RPCMethods[name]
	transactionalMethod, transactionalExists := h.config.RPCTransactionalMethods[name]
	if standardExists || transactionalExists {
		return method, transactionalMethod, standardExists, transactionalExists
	}
	if h.config.RPCMethodResolver != nil {
		method, standardExists = h.config.RPCMethodResolver.ResolveRPCMethod(name)
	}
	if !standardExists && h.config.RPCTransactionalMethodResolver != nil {
		transactionalMethod, transactionalExists = h.config.RPCTransactionalMethodResolver.ResolveRPCTransactionalMethod(name)
	}
	return method, transactionalMethod, standardExists, transactionalExists
}

func (h *Handler) executeRPC(ctx context.Context, actor Actor, method RPCMethod, transactionalMethod RPCTransactionalMethod, envelope rpcCallEnvelope, input meldbase.Value, requestBytes int) rpcOutcome {
	var action *rpcIdempotencyAction
	if envelope.IdempotencyKey != nil {
		if h.config.RPCIdempotencyStore == nil {
			h.metrics.rpcRejected.Add(1)
			h.metrics.rpcIdempotencyFailures.Add(1)
			return rpcOutcome{status: http.StatusServiceUnavailable, code: "rpc_idempotency_unavailable"}
		}
		claim, err := newRPCIdempotencyClaim(actor, envelope, input, h.rpcSessionID, h.config.RPCIdempotencyRetention)
		if err != nil {
			h.metrics.rpcRejected.Add(1)
			h.metrics.rpcIdempotencyFailures.Add(1)
			return rpcOutcome{status: http.StatusServiceUnavailable, code: "rpc_idempotency_unavailable"}
		}
		decision, err := h.config.RPCIdempotencyStore.Claim(ctx, claim)
		if err != nil {
			h.metrics.rpcRejected.Add(1)
			h.metrics.rpcIdempotencyFailures.Add(1)
			if engineErrorCode(err, "") == "database_unavailable" {
				return rpcOutcome{status: http.StatusServiceUnavailable, code: "database_unavailable"}
			}
			return rpcOutcome{status: http.StatusServiceUnavailable, code: "rpc_idempotency_unavailable"}
		}
		if validateRPCIdempotencyDecision(decision, h.config.MaxRPCResultBytes) != nil {
			h.metrics.rpcRejected.Add(1)
			h.metrics.rpcIdempotencyFailures.Add(1)
			return rpcOutcome{status: http.StatusServiceUnavailable, code: "rpc_idempotency_unavailable"}
		}
		switch decision.Kind {
		case RPCIdempotencyExecute:
			h.metrics.rpcIdempotencyClaims.Add(1)
			action = &rpcIdempotencyAction{claim: claim, store: h.config.RPCIdempotencyStore}
		case RPCIdempotencyReplayResult:
			value, err := meldbase.UnmarshalWireValue(decision.Result, h.config.QueryLimits)
			if err != nil {
				h.metrics.rpcRejected.Add(1)
				h.metrics.rpcIdempotencyFailures.Add(1)
				return rpcOutcome{status: http.StatusServiceUnavailable, code: "rpc_idempotency_unavailable"}
			}
			canonical, err := meldbase.MarshalWireValue(value)
			if err != nil || !bytes.Equal(canonical, decision.Result) {
				h.metrics.rpcRejected.Add(1)
				h.metrics.rpcIdempotencyFailures.Add(1)
				return rpcOutcome{status: http.StatusServiceUnavailable, code: "rpc_idempotency_unavailable"}
			}
			h.metrics.rpcIdempotencyReplays.Add(1)
			return rpcOutcome{result: append(json.RawMessage(nil), decision.Result...)}
		case RPCIdempotencyReplayError:
			if decision.ErrorKind == "business" && len(decision.ErrorData) != 0 {
				value, err := meldbase.UnmarshalWireValue(decision.ErrorData, h.config.QueryLimits)
				if err != nil {
					h.metrics.rpcRejected.Add(1)
					h.metrics.rpcIdempotencyFailures.Add(1)
					return internalRPCOutcome(http.StatusServiceUnavailable, "rpc_idempotency_unavailable")
				}
				canonical, err := meldbase.MarshalWireValue(value)
				if err != nil || !bytes.Equal(canonical, decision.ErrorData) {
					h.metrics.rpcRejected.Add(1)
					h.metrics.rpcIdempotencyFailures.Add(1)
					return internalRPCOutcome(http.StatusServiceUnavailable, "rpc_idempotency_unavailable")
				}
				if _, ok := value.ObjectValue(); !ok {
					h.metrics.rpcRejected.Add(1)
					h.metrics.rpcIdempotencyFailures.Add(1)
					return internalRPCOutcome(http.StatusServiceUnavailable, "rpc_idempotency_unavailable")
				}
			}
			h.metrics.rpcIdempotencyReplays.Add(1)
			return rpcOutcome{status: decision.ErrorStatus, code: decision.ErrorCode, kind: decision.ErrorKind, data: append(json.RawMessage(nil), decision.ErrorData...)}
		default:
			if status, code, ok := idempotencyDecisionError(decision); ok {
				h.metrics.rpcRejected.Add(1)
				switch decision.Kind {
				case RPCIdempotencyConflict:
					h.metrics.rpcIdempotencyConflicts.Add(1)
				case RPCIdempotencyInProgress:
					h.metrics.rpcIdempotencyInProgress.Add(1)
				case RPCIdempotencyOutcomeUnknown:
					h.metrics.rpcIdempotencyUnknown.Add(1)
				}
				return rpcOutcome{status: status, code: code}
			}
			h.metrics.rpcRejected.Add(1)
			h.metrics.rpcIdempotencyFailures.Add(1)
			return rpcOutcome{status: http.StatusServiceUnavailable, code: "rpc_idempotency_unavailable"}
		}
	}
	if transactionalMethod != nil {
		if action == nil {
			h.metrics.rpcRejected.Add(1)
			return rpcOutcome{status: http.StatusBadRequest, code: "rpc_idempotency_required"}
		}
		return h.executeTransactionalRPC(ctx, actor, transactionalMethod, input, requestBytes, *action)
	}

	span := h.beginRPC(requestBytes)
	result, callErr := invokeRPCMethod(ctx, method, actor, input)
	if callErr != nil {
		h.metrics.rpcAtomicRollbacks.Add(1)
		metricOutcome := "failed"
		if errors.Is(callErr, context.Canceled) || errors.Is(callErr, context.DeadlineExceeded) {
			metricOutcome = "canceled"
		}
		h.finishRPC(span, metricOutcome, 0)
		outcome := classifyRPCOutcome(callErr)
		if action != nil {
			if metricOutcome == "canceled" {
				h.metrics.rpcIdempotencyUnknown.Add(1)
				h.markRPCUnknown(*action)
				return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
			}
			if !h.completeRPCIdempotency(*action, RPCIdempotencyCompletion{Claim: action.claim, ErrorCode: outcome.code, ErrorKind: outcome.kind, ErrorData: outcome.data, ErrorStatus: outcome.status}) {
				h.metrics.rpcIdempotencyUnknown.Add(1)
				h.metrics.rpcIdempotencyFailures.Add(1)
				return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
			}
		}
		return outcome
	}
	encoded, err := meldbase.MarshalWireValue(result)
	if err != nil || len(encoded) > h.config.MaxRPCResultBytes {
		h.finishRPC(span, "failed", 0)
		if action != nil && !h.completeRPCIdempotency(*action, RPCIdempotencyCompletion{
			Claim: action.claim, ErrorCode: "rpc_result_invalid", ErrorStatus: http.StatusInternalServerError,
		}) {
			h.metrics.rpcIdempotencyUnknown.Add(1)
			h.metrics.rpcIdempotencyFailures.Add(1)
			return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
		}
		return rpcOutcome{status: http.StatusInternalServerError, code: "rpc_result_invalid"}
	}
	h.finishRPC(span, "success", len(encoded))
	if action != nil && !h.completeRPCIdempotency(*action, RPCIdempotencyCompletion{Claim: action.claim, Result: encoded}) {
		h.metrics.rpcIdempotencyUnknown.Add(1)
		h.metrics.rpcIdempotencyFailures.Add(1)
		return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
	}
	return rpcOutcome{result: json.RawMessage(encoded)}
}

func (h *Handler) executeTransactionalRPC(ctx context.Context, actor Actor, method RPCTransactionalMethod, input meldbase.Value, requestBytes int, action rpcIdempotencyAction) rpcOutcome {
	store, ok := action.store.(*durableRPCIdempotencyStore)
	if !ok || store.db != h.config.DB {
		h.metrics.rpcRejected.Add(1)
		h.metrics.rpcIdempotencyFailures.Add(1)
		return rpcOutcome{status: http.StatusServiceUnavailable, code: "rpc_idempotency_unavailable"}
	}
	expected, err := store.atomicClaimMutation(action.claim)
	if err != nil {
		h.metrics.rpcRejected.Add(1)
		h.metrics.rpcIdempotencyFailures.Add(1)
		return rpcOutcome{status: http.StatusServiceUnavailable, code: "rpc_idempotency_unavailable"}
	}
	span := h.beginRPC(requestBytes)
	var encoded []byte
	var callErr error
	invalidResult := false
	cas, composite, commitErr := h.config.DB.MeldbaseSystemWrite(ctx, expected, func(tx *meldbase.WriteTransaction) ([]byte, error) {
		result, err := invokeRPCTransactionalMethod(ctx, method, actor, input, tx)
		if err != nil {
			callErr = err
			return nil, err
		}
		encoded, err = meldbase.MarshalWireValue(result)
		if err != nil || len(encoded) > h.config.MaxRPCResultBytes {
			invalidResult = true
			callErr = errors.New("invalid transactional RPC result")
			return nil, callErr
		}
		terminal, err := store.atomicCompletion(RPCIdempotencyCompletion{Claim: action.claim, Result: encoded})
		if err != nil {
			callErr = err
			return nil, err
		}
		return terminal.NewValue, nil
	})
	if callErr != nil {
		metricOutcome := "failed"
		if errors.Is(callErr, context.Canceled) || errors.Is(callErr, context.DeadlineExceeded) {
			metricOutcome = "canceled"
		}
		h.finishRPC(span, metricOutcome, 0)
		outcome := classifyRPCOutcome(callErr)
		if invalidResult {
			outcome = internalRPCOutcome(http.StatusInternalServerError, "rpc_result_invalid")
		}
		if metricOutcome == "canceled" {
			h.metrics.rpcIdempotencyUnknown.Add(1)
			h.markRPCUnknown(action)
			return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
		}
		if !h.completeRPCIdempotency(action, RPCIdempotencyCompletion{Claim: action.claim, ErrorCode: outcome.code, ErrorKind: outcome.kind, ErrorData: outcome.data, ErrorStatus: outcome.status}) {
			h.metrics.rpcIdempotencyUnknown.Add(1)
			h.metrics.rpcIdempotencyFailures.Add(1)
			return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
		}
		return outcome
	}
	if errors.Is(commitErr, meldbase.ErrWriteConflict) {
		h.finishRPC(span, "failed", 0)
		h.metrics.rpcAtomicRollbacks.Add(1)
		if !h.completeRPCIdempotency(action, RPCIdempotencyCompletion{
			Claim: action.claim, ErrorCode: "rpc_transaction_conflict", ErrorStatus: http.StatusConflict,
		}) {
			h.metrics.rpcIdempotencyUnknown.Add(1)
			h.metrics.rpcIdempotencyFailures.Add(1)
			return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
		}
		return rpcOutcome{status: http.StatusConflict, code: "rpc_transaction_conflict"}
	}
	if !composite && errors.Is(commitErr, meldbase.ErrInvalidDocument) {
		h.finishRPC(span, "failed", 0)
		h.metrics.rpcAtomicRollbacks.Add(1)
		if !h.completeRPCIdempotency(action, RPCIdempotencyCompletion{
			Claim: action.claim, ErrorCode: "rpc_transaction_requires_write", ErrorStatus: http.StatusBadRequest,
		}) {
			h.metrics.rpcIdempotencyUnknown.Add(1)
			h.metrics.rpcIdempotencyFailures.Add(1)
			return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
		}
		return rpcOutcome{status: http.StatusBadRequest, code: "rpc_transaction_requires_write"}
	}
	if commitErr != nil || (composite && !cas.Applied) {
		h.finishRPC(span, "failed", 0)
		h.metrics.rpcIdempotencyUnknown.Add(1)
		h.metrics.rpcIdempotencyFailures.Add(1)
		h.markRPCUnknown(action)
		return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
	}
	if !composite && !h.completeRPCIdempotency(action, RPCIdempotencyCompletion{Claim: action.claim, Result: encoded}) {
		h.finishRPC(span, "failed", 0)
		h.metrics.rpcIdempotencyUnknown.Add(1)
		h.metrics.rpcIdempotencyFailures.Add(1)
		return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
	}
	if composite {
		h.metrics.rpcAtomicCommits.Add(1)
	} else {
		h.metrics.rpcAtomicNoopCompletions.Add(1)
	}
	h.finishRPC(span, "success", len(encoded))
	return rpcOutcome{result: json.RawMessage(encoded)}
}

func (h *Handler) completeRPCIdempotency(action rpcIdempotencyAction, completion RPCIdempotencyCompletion) bool {
	if completion.ErrorCode != "" && completion.ErrorKind == "" {
		completion.ErrorKind = "internal"
	}
	ctx, cancel := context.WithTimeout(context.Background(), h.config.RPCIdempotencyCommitTimeout)
	defer cancel()
	if err := action.store.Complete(ctx, completion); err != nil {
		h.markRPCUnknown(action)
		return false
	}
	return true
}

func (h *Handler) markRPCUnknown(action rpcIdempotencyAction) {
	ctx, cancel := context.WithTimeout(context.Background(), h.config.RPCIdempotencyCommitTimeout)
	defer cancel()
	_ = action.store.MarkUnknown(ctx, action.claim)
}

func decodeRPCCallEnvelope(raw []byte) (rpcCallEnvelope, error) {
	var envelope rpcCallEnvelope
	if err := decodeStrict(raw, &envelope); err != nil || envelope.Version != protocolVersion || envelope.Type != "call" ||
		!rpcRequestIDPattern.MatchString(envelope.RequestID) || !validRPCMethodName(envelope.Method) ||
		envelope.Input == nil ||
		(envelope.IdempotencyKey != nil && !validRPCIdempotencyKey(*envelope.IdempotencyKey)) {
		return rpcCallEnvelope{}, errors.New("invalid RPC call envelope")
	}
	return envelope, nil
}

func decodeRPCInput(rawInput json.RawMessage, limits meldbase.QueryLimits) (meldbase.Value, error) {
	return meldbase.UnmarshalWireValue(rawInput, limits)
}

func internalRPCOutcome(status int, code string) rpcOutcome {
	return rpcOutcome{status: status, code: code, kind: "internal"}
}

func classifyRPCOutcome(err error) rpcOutcome {
	if errors.Is(err, meldbase.ErrCommitOutcomeUnknown) {
		return internalRPCOutcome(http.StatusConflict, "rpc_outcome_unknown")
	}
	if errors.Is(err, meldbase.ErrDurability) || errors.Is(err, meldbase.ErrClosed) ||
		errors.Is(err, meldbase.ErrWriteTransactionUnsupported) {
		return internalRPCOutcome(http.StatusServiceUnavailable, "database_unavailable")
	}
	if errors.Is(err, errRPCWorkerBusy) {
		return internalRPCOutcome(http.StatusServiceUnavailable, "worker_busy")
	}
	if errors.Is(err, meldbase.ErrResourceLimit) {
		return internalRPCOutcome(http.StatusRequestEntityTooLarge, "resource_limit_exceeded")
	}
	var business *MeldbaseError
	if errors.As(err, &business) && business != nil && rpcBusinessErrorCodePattern.MatchString(business.Code) {
		outcome := rpcOutcome{status: http.StatusBadRequest, code: business.Code, kind: "business"}
		if business.Data != nil {
			encoded, marshalErr := meldbase.MarshalWireValue(meldbase.Object(business.Data))
			if marshalErr != nil || len(encoded) > maxRPCErrorDataBytes {
				return internalRPCOutcome(http.StatusInternalServerError, "internal")
			}
			outcome.data = json.RawMessage(encoded)
		}
		return outcome
	}
	var internal *MeldbaseInternalError
	if errors.As(err, &internal) && internal != nil && rpcInternalErrorCodePattern.MatchString(internal.Code) {
		status := internal.Status
		if status < 400 || status > 599 {
			status = http.StatusInternalServerError
		}
		return internalRPCOutcome(status, internal.Code)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return internalRPCOutcome(http.StatusRequestTimeout, "rpc_canceled")
	}
	return internalRPCOutcome(http.StatusInternalServerError, "internal")
}

func validRPCBusinessErrorData(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	if len(data) > maxRPCErrorDataBytes {
		return false
	}
	value, err := meldbase.UnmarshalWireValue(data, meldbase.QueryLimits{
		MaxWireBytes: maxRPCErrorDataBytes, MaxDepth: 64, MaxNodes: 1 << 20,
		MaxArrayItems: 1 << 20, MaxValueBytes: maxRPCErrorDataBytes,
	})
	if err != nil {
		return false
	}
	canonical, err := meldbase.MarshalWireValue(value)
	if err != nil || !bytes.Equal(canonical, data) {
		return false
	}
	_, ok := value.ObjectValue()
	return ok
}

// classifyRPCError is kept internal for existing engine tests; public wire
// classification additionally carries the business/internal distinction.
func classifyRPCError(err error) (int, string) {
	outcome := classifyRPCOutcome(err)
	return outcome.status, outcome.code
}

func rpcResultMessage(requestID string, encoded json.RawMessage) map[string]any {
	return map[string]any{"v": protocolVersion, "type": "result", "requestId": requestID, "result": encoded}
}

func rpcErrorMessage(requestID, code string) map[string]any {
	return rpcOutcomeErrorMessage(requestID, internalRPCOutcome(http.StatusInternalServerError, code))
}

func rpcOutcomeErrorMessage(requestID string, outcome rpcOutcome) map[string]any {
	if outcome.kind == "" {
		outcome.kind = "internal"
	}
	errorBody := map[string]any{"kind": outcome.kind, "code": outcome.code}
	if len(outcome.data) != 0 {
		errorBody["data"] = json.RawMessage(outcome.data)
	}
	return map[string]any{"v": protocolVersion, "type": "error", "requestId": requestID, "error": errorBody}
}

func writeRPCError(w http.ResponseWriter, status int, requestID, code string) {
	writeJSON(w, status, rpcOutcomeErrorMessage(requestID, internalRPCOutcome(status, code)))
}

func (s *socketSession) startRPCCall(envelope rpcCallEnvelope, requestBytes int) {
	s.handler.metrics.rpcRequests.Add(1)
	method, transactionalMethod, standardExists, transactionalExists := s.handler.resolveRPCMethod(envelope.Method)
	if !standardExists && !transactionalExists {
		s.handler.metrics.rpcRejected.Add(1)
		s.enqueue(rpcErrorMessage(envelope.RequestID, "rpc_not_found"))
		return
	}
	if s.handler.config.RPCAuthorizer == nil || s.handler.config.RPCAuthorizer.AuthorizeRPC(s.ctx, s.actor, envelope.Method) != nil {
		s.handler.metrics.rpcRejected.Add(1)
		s.enqueue(rpcErrorMessage(envelope.RequestID, "forbidden"))
		return
	}
	input, err := decodeRPCInput(*envelope.Input, s.handler.config.QueryLimits)
	if err != nil {
		s.handler.metrics.rpcRejected.Add(1)
		s.enqueue(rpcErrorMessage(envelope.RequestID, "invalid_rpc_argument"))
		return
	}
	ctx, cancel := context.WithCancel(s.ctx)
	s.mu.Lock()
	_, subscriptionExists := s.byRequest[envelope.RequestID]
	_, rpcExists := s.rpcCalls[envelope.RequestID]
	if subscriptionExists || rpcExists {
		s.mu.Unlock()
		cancel()
		s.handler.metrics.rpcRejected.Add(1)
		s.enqueue(rpcErrorMessage(envelope.RequestID, "rpc_duplicate_request"))
		return
	}
	if len(s.rpcCalls) >= s.handler.config.MaxRPCPerConnection {
		s.mu.Unlock()
		cancel()
		s.handler.metrics.rpcBusy.Add(1)
		s.enqueue(rpcErrorMessage(envelope.RequestID, "rpc_busy"))
		return
	}
	s.rpcCalls[envelope.RequestID] = cancel
	s.mu.Unlock()
	select {
	case s.handler.rpcSlots <- struct{}{}:
	case <-ctx.Done():
		s.removeRPCCall(envelope.RequestID)
		cancel()
		return
	default:
		s.removeRPCCall(envelope.RequestID)
		cancel()
		s.handler.metrics.rpcBusy.Add(1)
		s.enqueue(rpcErrorMessage(envelope.RequestID, "rpc_busy"))
		return
	}
	go func() {
		cleaned := false
		cleanup := func() {
			if cleaned {
				return
			}
			cleaned = true
			s.removeRPCCall(envelope.RequestID)
			cancel()
			<-s.handler.rpcSlots
		}
		defer cleanup()
		outcome := s.handler.executeRPC(ctx, s.actor, method, transactionalMethod, envelope, input, requestBytes)
		cleanup()
		if outcome.code != "" {
			s.enqueue(rpcOutcomeErrorMessage(envelope.RequestID, outcome))
			return
		}
		s.enqueue(rpcResultMessage(envelope.RequestID, outcome.result))
	}()
}

func (s *socketSession) cancelRPCCall(requestID string) {
	s.mu.Lock()
	cancel := s.rpcCalls[requestID]
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *socketSession) removeRPCCall(requestID string) {
	s.mu.Lock()
	delete(s.rpcCalls, requestID)
	s.mu.Unlock()
}

func invokeRPCMethod(ctx context.Context, method RPCMethod, actor Actor, input meldbase.Value) (result meldbase.Value, err error) {
	defer func() {
		if recover() != nil {
			result, err = meldbase.Value{}, errors.New("RPC handler panic")
		}
	}()
	return method(ctx, actor, input)
}

func invokeRPCTransactionalMethod(ctx context.Context, method RPCTransactionalMethod, actor Actor, input meldbase.Value, tx *meldbase.WriteTransaction) (result meldbase.Value, err error) {
	defer func() {
		if recover() != nil {
			result, err = meldbase.Value{}, errors.New("RPC handler panic")
		}
	}()
	return method(ctx, actor, input, tx)
}
