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
	rpcMethodNamePattern     = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,127}$`)
	rpcErrorCodePattern      = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	rpcRequestIDPattern      = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	rpcIdempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{22,128}$`)
)

// RPCMethod is a bounded, authenticated data-only request handler. Arguments
// and results use Meldbase's closed Value model, preserving Int64, Date, Binary
// and object semantics across Go and JavaScript.
type RPCMethod func(context.Context, Principal, []meldbase.Value) (meldbase.Value, error)

// RPCTransactionalMethod stages point writes against a short immutable
// snapshot. A successful result and all staged writes share one durable
// publication with the RPC idempotency terminal record after optimistic commit
// validation.
type RPCTransactionalMethod func(context.Context, Principal, []meldbase.Value, *meldbase.WriteTransaction) (meldbase.Value, error)

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

// RPCAuthorizer is evaluated for every call before arguments are decoded or
// application code runs. Registration alone never grants call permission.
type RPCAuthorizer interface {
	AuthorizeRPC(context.Context, Principal, string) error
}

// RPCError exposes one stable, non-sensitive application error code. Arbitrary
// handler errors are returned as "internal" and their text never crosses the
// transport boundary.
type RPCError struct {
	Code string
}

type rpcCallEnvelope struct {
	Version        int                `json:"v"`
	Type           string             `json:"type"`
	RequestID      string             `json:"requestId"`
	IdempotencyKey *string            `json:"idempotencyKey,omitempty"`
	Method         string             `json:"method"`
	Arguments      *[]json.RawMessage `json:"arguments"`
}

type rpcOutcome struct {
	result json.RawMessage
	status int
	code   string
}

func (err *RPCError) Error() string {
	if err == nil {
		return "meldbase RPC error"
	}
	return "meldbase RPC: " + err.Code
}

func validRPCMethodName(name string) bool    { return rpcMethodNamePattern.MatchString(name) }
func validRPCIdempotencyKey(key string) bool { return rpcIdempotencyKeyPattern.MatchString(key) }

func (h *Handler) rpc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("cache-control", "no-store")
	principal, err := h.config.Authenticator.AuthenticateHTTP(r)
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
	envelope, err := decodeRPCCallEnvelope(body, h.config.MaxRPCArguments)
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
	if h.config.RPCAuthorizer == nil || h.config.RPCAuthorizer.AuthorizeRPC(r.Context(), principal, envelope.Method) != nil {
		h.metrics.rpcRejected.Add(1)
		writeRPCError(w, http.StatusForbidden, envelope.RequestID, "forbidden")
		return
	}
	arguments, err := decodeRPCArguments(*envelope.Arguments, h.config.QueryLimits)
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
	outcome := h.executeRPC(r.Context(), principal, method, transactionalMethod, envelope, arguments, len(body))
	if outcome.code != "" {
		writeRPCError(w, outcome.status, envelope.RequestID, outcome.code)
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

func (h *Handler) executeRPC(ctx context.Context, principal Principal, method RPCMethod, transactionalMethod RPCTransactionalMethod, envelope rpcCallEnvelope, arguments []meldbase.Value, requestBytes int) rpcOutcome {
	var action *rpcIdempotencyAction
	if envelope.IdempotencyKey != nil {
		if h.config.RPCIdempotencyStore == nil {
			h.metrics.rpcRejected.Add(1)
			h.metrics.rpcIdempotencyFailures.Add(1)
			return rpcOutcome{status: http.StatusServiceUnavailable, code: "rpc_idempotency_unavailable"}
		}
		claim, err := newRPCIdempotencyClaim(principal, envelope, arguments, h.rpcSessionID, h.config.RPCIdempotencyRetention)
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
			h.metrics.rpcIdempotencyReplays.Add(1)
			return rpcOutcome{status: decision.ErrorStatus, code: decision.ErrorCode}
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
		return h.executeTransactionalRPC(ctx, principal, transactionalMethod, arguments, requestBytes, *action)
	}

	span := h.beginRPC(len(arguments), requestBytes)
	result, callErr := invokeRPCMethod(ctx, method, principal, arguments)
	if callErr != nil {
		h.metrics.rpcAtomicRollbacks.Add(1)
		metricOutcome := "failed"
		if errors.Is(callErr, context.Canceled) || errors.Is(callErr, context.DeadlineExceeded) {
			metricOutcome = "canceled"
		}
		h.finishRPC(span, metricOutcome, 0)
		status, code := classifyRPCError(callErr)
		if action != nil {
			if metricOutcome == "canceled" {
				h.metrics.rpcIdempotencyUnknown.Add(1)
				h.markRPCUnknown(*action)
				return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
			}
			if !h.completeRPCIdempotency(*action, RPCIdempotencyCompletion{Claim: action.claim, ErrorCode: code, ErrorStatus: status}) {
				h.metrics.rpcIdempotencyUnknown.Add(1)
				h.metrics.rpcIdempotencyFailures.Add(1)
				return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
			}
		}
		return rpcOutcome{status: status, code: code}
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

func (h *Handler) executeTransactionalRPC(ctx context.Context, principal Principal, method RPCTransactionalMethod, arguments []meldbase.Value, requestBytes int, action rpcIdempotencyAction) rpcOutcome {
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
	span := h.beginRPC(len(arguments), requestBytes)
	var encoded []byte
	var callErr error
	invalidResult := false
	cas, composite, commitErr := h.config.DB.MeldbaseSystemWrite(ctx, expected, func(tx *meldbase.WriteTransaction) ([]byte, error) {
		result, err := invokeRPCTransactionalMethod(ctx, method, principal, arguments, tx)
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
		status, code := classifyRPCError(callErr)
		if invalidResult {
			status, code = http.StatusInternalServerError, "rpc_result_invalid"
		}
		if metricOutcome == "canceled" {
			h.metrics.rpcIdempotencyUnknown.Add(1)
			h.markRPCUnknown(action)
			return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
		}
		if !h.completeRPCIdempotency(action, RPCIdempotencyCompletion{Claim: action.claim, ErrorCode: code, ErrorStatus: status}) {
			h.metrics.rpcIdempotencyUnknown.Add(1)
			h.metrics.rpcIdempotencyFailures.Add(1)
			return rpcOutcome{status: http.StatusConflict, code: "rpc_outcome_unknown"}
		}
		return rpcOutcome{status: status, code: code}
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

func decodeRPCCallEnvelope(raw []byte, maxArguments int) (rpcCallEnvelope, error) {
	var envelope rpcCallEnvelope
	if err := decodeStrict(raw, &envelope); err != nil || envelope.Version != protocolVersion || envelope.Type != "call" ||
		!rpcRequestIDPattern.MatchString(envelope.RequestID) || !validRPCMethodName(envelope.Method) ||
		envelope.Arguments == nil || len(*envelope.Arguments) > maxArguments ||
		(envelope.IdempotencyKey != nil && !validRPCIdempotencyKey(*envelope.IdempotencyKey)) {
		return rpcCallEnvelope{}, errors.New("invalid RPC call envelope")
	}
	return envelope, nil
}

func decodeRPCArguments(rawArguments []json.RawMessage, limits meldbase.QueryLimits) ([]meldbase.Value, error) {
	arguments := make([]meldbase.Value, len(rawArguments))
	for index, raw := range rawArguments {
		value, err := meldbase.UnmarshalWireValue(raw, limits)
		if err != nil {
			return nil, err
		}
		arguments[index] = value
	}
	return arguments, nil
}

func classifyRPCError(err error) (int, string) {
	if errors.Is(err, meldbase.ErrCommitOutcomeUnknown) {
		return http.StatusConflict, "rpc_outcome_unknown"
	}
	if errors.Is(err, meldbase.ErrDurability) || errors.Is(err, meldbase.ErrClosed) ||
		errors.Is(err, meldbase.ErrWriteTransactionUnsupported) {
		return http.StatusServiceUnavailable, "database_unavailable"
	}
	if errors.Is(err, errRPCWorkerBusy) {
		return http.StatusServiceUnavailable, "worker_busy"
	}
	if errors.Is(err, meldbase.ErrResourceLimit) {
		return http.StatusRequestEntityTooLarge, "resource_limit_exceeded"
	}
	var exposed *RPCError
	if errors.As(err, &exposed) && exposed != nil && rpcErrorCodePattern.MatchString(exposed.Code) {
		return http.StatusBadRequest, exposed.Code
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusRequestTimeout, "rpc_canceled"
	}
	return http.StatusInternalServerError, "internal"
}

func rpcResultMessage(requestID string, encoded json.RawMessage) map[string]any {
	return map[string]any{"v": protocolVersion, "type": "result", "requestId": requestID, "result": encoded}
}

func rpcErrorMessage(requestID, code string) map[string]any {
	return map[string]any{"v": protocolVersion, "type": "error", "requestId": requestID, "error": map[string]any{"code": code}}
}

func writeRPCError(w http.ResponseWriter, status int, requestID, code string) {
	writeJSON(w, status, rpcErrorMessage(requestID, code))
}

func (s *socketSession) startRPCCall(envelope rpcCallEnvelope, requestBytes int) {
	s.handler.metrics.rpcRequests.Add(1)
	method, transactionalMethod, standardExists, transactionalExists := s.handler.resolveRPCMethod(envelope.Method)
	if !standardExists && !transactionalExists {
		s.handler.metrics.rpcRejected.Add(1)
		s.enqueue(rpcErrorMessage(envelope.RequestID, "rpc_not_found"))
		return
	}
	if s.handler.config.RPCAuthorizer == nil || s.handler.config.RPCAuthorizer.AuthorizeRPC(s.ctx, s.principal, envelope.Method) != nil {
		s.handler.metrics.rpcRejected.Add(1)
		s.enqueue(rpcErrorMessage(envelope.RequestID, "forbidden"))
		return
	}
	arguments, err := decodeRPCArguments(*envelope.Arguments, s.handler.config.QueryLimits)
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
		outcome := s.handler.executeRPC(ctx, s.principal, method, transactionalMethod, envelope, arguments, requestBytes)
		cleanup()
		if outcome.code != "" {
			s.enqueue(rpcErrorMessage(envelope.RequestID, outcome.code))
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

func invokeRPCMethod(ctx context.Context, method RPCMethod, principal Principal, arguments []meldbase.Value) (result meldbase.Value, err error) {
	defer func() {
		if recover() != nil {
			result, err = meldbase.Value{}, errors.New("RPC handler panic")
		}
	}()
	return method(ctx, principal, arguments)
}

func invokeRPCTransactionalMethod(ctx context.Context, method RPCTransactionalMethod, principal Principal, arguments []meldbase.Value, tx *meldbase.WriteTransaction) (result meldbase.Value, err error) {
	defer func() {
		if recover() != nil {
			result, err = meldbase.Value{}, errors.New("RPC handler panic")
		}
	}()
	return method(ctx, principal, arguments, tx)
}
