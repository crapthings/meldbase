package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/crapthings/meldbase"
)

// RPCIdempotencyDecisionKind identifies whether an RPC may execute or must use a previously recorded outcome.
type RPCIdempotencyDecisionKind uint8

const (
	// RPCIdempotencyExecute grants ownership of a new pending request.
	RPCIdempotencyExecute RPCIdempotencyDecisionKind = iota + 1
	// RPCIdempotencyReplayResult returns a previously recorded successful result.
	RPCIdempotencyReplayResult
	// RPCIdempotencyReplayError returns a previously recorded error.
	RPCIdempotencyReplayError
	// RPCIdempotencyInProgress reports that the same request is still pending in the current session.
	RPCIdempotencyInProgress
	// RPCIdempotencyOutcomeUnknown reports that the request cannot safely be executed again because its outcome is uncertain.
	RPCIdempotencyOutcomeUnknown
	// RPCIdempotencyConflict reports that the key was used for a different request.
	RPCIdempotencyConflict
)

// RPCIdempotencyClaim identifies a pending request and the session that owns it.
//
// The claim is persisted before application code starts. ScopeHash and KeyHash
// keep raw identities and caller keys out of stored record keys. SessionID and
// ClaimID are compare-and-set ownership tokens.
type RPCIdempotencyClaim struct {
	ScopeHash   [32]byte
	KeyHash     [32]byte
	Fingerprint [32]byte
	SessionID   [16]byte
	ClaimID     [16]byte
	ExpiresAt   time.Time
}

// RPCIdempotencyDecision carries the store's execution decision or recorded result or error.
type RPCIdempotencyDecision struct {
	Kind        RPCIdempotencyDecisionKind
	Result      []byte
	ErrorKind   string
	ErrorCode   string
	ErrorData   []byte
	ErrorStatus int
}

// RPCIdempotencyCompletion pairs a claim with the result or error to persist.
type RPCIdempotencyCompletion struct {
	Claim       RPCIdempotencyClaim
	Result      []byte
	ErrorKind   string
	ErrorCode   string
	ErrorData   []byte
	ErrorStatus int
}

// RPCIdempotencyStore records request ownership and outcomes so repeated calls cannot execute unsafely.
//
// Operations must be linearizable and durable. Claim must publish a new pending
// record before returning Execute. Complete and MarkUnknown are CAS
// transitions matching SessionID and ClaimID. Implementations must never turn a
// pending record owned by another session back into Execute.
type RPCIdempotencyStore interface {
	Claim(context.Context, RPCIdempotencyClaim) (RPCIdempotencyDecision, error)
	Complete(context.Context, RPCIdempotencyCompletion) error
	MarkUnknown(context.Context, RPCIdempotencyClaim) error
}

// RPCIdempotencyMaintenance removes expired terminal records without discarding pending requests.
type RPCIdempotencyMaintenance interface {
	// PruneExpired removes up to the requested number of expired records with known or unknown outcomes.
	//
	// Pending records are never removed by time alone.
	PruneExpired(context.Context, int) (int, error)
}

// DurableRPCIdempotencyStore combines durable request ownership with expired-record maintenance.
type DurableRPCIdempotencyStore interface {
	RPCIdempotencyStore
	RPCIdempotencyMaintenance
}

// rpcIdempotencyAction pairs an owned request claim with the store that records its outcome.
type rpcIdempotencyAction struct {
	claim RPCIdempotencyClaim
	store RPCIdempotencyStore
}

// newRPCIdempotencyClaim hashes the request scope and contents and creates ownership tokens for a pending request.
func newRPCIdempotencyClaim(actor Actor, envelope rpcCallEnvelope, input meldbase.Value, sessionID [16]byte, retention time.Duration) (RPCIdempotencyClaim, error) {
	if envelope.IdempotencyKey == nil || !validRPCIdempotencyKey(*envelope.IdempotencyKey) || allZero16(sessionID) || retention <= 0 {
		return RPCIdempotencyClaim{}, errors.New("invalid RPC idempotency claim")
	}
	if actor.ID == "" || len(actor.ID) > 4096 || len(actor.WorkspaceID) > 4096 ||
		!utf8.ValidString(actor.ID) || !utf8.ValidString(actor.WorkspaceID) {
		return RPCIdempotencyClaim{}, errors.New("RPC actor identity is not a stable UTF-8 scope")
	}
	claimID, err := randomToken16()
	if err != nil {
		return RPCIdempotencyClaim{}, err
	}
	scope := sha256.New()
	writeHashFrame(scope, []byte("meldbase-rpc-scope-v1"))
	writeHashFrame(scope, []byte(actor.WorkspaceID))
	writeHashFrame(scope, []byte(actor.ID))
	fingerprint := sha256.New()
	writeHashFrame(fingerprint, []byte("meldbase-rpc-request-v1"))
	writeHashFrame(fingerprint, []byte(envelope.Method))
	canonical, err := meldbase.MarshalWireValue(input)
	if err != nil {
		return RPCIdempotencyClaim{}, err
	}
	writeHashFrame(fingerprint, canonical)
	claim := RPCIdempotencyClaim{
		KeyHash: sha256.Sum256([]byte(*envelope.IdempotencyKey)), SessionID: sessionID, ClaimID: claimID,
		ExpiresAt: time.Now().UTC().Add(retention),
	}
	copy(claim.ScopeHash[:], scope.Sum(nil))
	copy(claim.Fingerprint[:], fingerprint.Sum(nil))
	return claim, nil
}

// validateRPCIdempotencyDecision checks that a decision carries only the outcome fields allowed by its kind.
func validateRPCIdempotencyDecision(decision RPCIdempotencyDecision, maxResultBytes int) error {
	switch decision.Kind {
	case RPCIdempotencyExecute, RPCIdempotencyInProgress, RPCIdempotencyOutcomeUnknown, RPCIdempotencyConflict:
		if len(decision.Result) != 0 || decision.ErrorKind != "" || decision.ErrorCode != "" || len(decision.ErrorData) != 0 || decision.ErrorStatus != 0 {
			return errors.New("invalid non-terminal idempotency decision")
		}
	case RPCIdempotencyReplayResult:
		if len(decision.Result) == 0 || len(decision.Result) > maxResultBytes || decision.ErrorKind != "" || decision.ErrorCode != "" || len(decision.ErrorData) != 0 || decision.ErrorStatus != 0 {
			return errors.New("invalid replay result")
		}
	case RPCIdempotencyReplayError:
		if len(decision.Result) != 0 || (decision.ErrorKind != "business" && decision.ErrorKind != "internal") || decision.ErrorStatus < 400 || decision.ErrorStatus > 599 ||
			(decision.ErrorKind == "business" && (!rpcBusinessErrorCodePattern.MatchString(decision.ErrorCode) || !validRPCBusinessErrorData(decision.ErrorData))) ||
			(decision.ErrorKind == "internal" && (!rpcInternalErrorCodePattern.MatchString(decision.ErrorCode) || len(decision.ErrorData) != 0)) {
			return errors.New("invalid replay error")
		}
	default:
		return errors.New("unknown idempotency decision")
	}
	return nil
}

// idempotencyDecisionError maps blocked execution decisions to HTTP conflict responses.
func idempotencyDecisionError(decision RPCIdempotencyDecision) (int, string, bool) {
	switch decision.Kind {
	case RPCIdempotencyInProgress:
		return http.StatusConflict, "rpc_in_progress", true
	case RPCIdempotencyOutcomeUnknown:
		return http.StatusConflict, "rpc_outcome_unknown", true
	case RPCIdempotencyConflict:
		return http.StatusConflict, "rpc_idempotency_conflict", true
	default:
		return 0, "", false
	}
}

// sameRPCFingerprint compares request hashes in constant time.
func sameRPCFingerprint(left, right [32]byte) bool {
	return subtle.ConstantTimeCompare(left[:], right[:]) == 1
}

// hashWriter accepts bytes used to build framed request hashes.
type hashWriter interface{ Write([]byte) (int, error) }

// writeHashFrame writes a byte length followed by its value to prevent ambiguous concatenated hashes.
func writeHashFrame(writer hashWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}

// allZero16 reports whether every byte of a token is zero.
func allZero16(value [16]byte) bool {
	var found byte
	for _, item := range value {
		found |= item
	}
	return found == 0
}

// randomToken16 generates a cryptographically random sixteen-byte ownership token.
func randomToken16() ([16]byte, error) {
	var token [16]byte
	_, err := rand.Read(token[:])
	return token, err
}
