package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/crapthings/meldbase/core"
	"github.com/crapthings/meldbase/internal/systemrecord"
)

const (
	rpcIdempotencyRecordVersion = 2
	rpcIdempotencyHeaderBytes   = 128
	rpcIdempotencyClaimAttempts = 16
)

const (
	idempotencyRecordPending uint8 = iota + 1
	idempotencyRecordResult
	idempotencyRecordError
	idempotencyRecordUnknown
)

var rpcIdempotencyRecordMagic = [8]byte{'M', 'E', 'L', 'D', 'R', 'P', 'C', '1'}
var rpcIdempotencyKeyPrefix = []byte("rpc.idempotency.v1\x00")

type durableRPCIdempotencyStore struct {
	backend systemrecord.Backend
	db      *meldbase.DB

	pruneMu    sync.Mutex
	pruneStart []byte
}

type rpcIdempotencyRecord struct {
	state       uint8
	fingerprint [32]byte
	sessionID   [16]byte
	claimID     [16]byte
	expiresAt   time.Time
	result      []byte
	errorKind   string
	errorCode   string
	errorData   []byte
	errorStatus int
}

// NewDurableRPCIdempotencyStore creates the built-in -backed store. Memory
// databases and V1 files are rejected rather than receiving a non-durable
// fallback.
func NewDurableRPCIdempotencyStore(db *meldbase.DB) (DurableRPCIdempotencyStore, error) {
	if db == nil {
		return nil, errors.New("meldbase server: idempotency database is required")
	}
	backend := db.MeldbaseSystemRecordBackend()
	if backend == nil {
		return nil, errors.New("meldbase server: durable RPC idempotency requires an open database")
	}
	return &durableRPCIdempotencyStore{backend: backend, db: db}, nil
}

func (store *durableRPCIdempotencyStore) PruneExpired(ctx context.Context, limit int) (int, error) {
	if store == nil || store.backend == nil || ctx == nil || limit <= 0 || limit > 256 {
		return 0, errors.New("RPC idempotency prune limit must be between 1 and 256")
	}
	store.pruneMu.Lock()
	defer store.pruneMu.Unlock()
	end := append([]byte(nil), rpcIdempotencyKeyPrefix...)
	end[len(end)-1]++
	start := append([]byte(nil), store.pruneStart...)
	if len(start) == 0 || bytes.Compare(start, rpcIdempotencyKeyPrefix) < 0 || bytes.Compare(start, end) >= 0 {
		start = append([]byte(nil), rpcIdempotencyKeyPrefix...)
	}
	defer func() { store.pruneStart = append(store.pruneStart[:0], start...) }()
	pruned, examined := 0, 0
	maxExamined := limit * 16
	if maxExamined > 4096 {
		maxExamined = 4096
	}
	now := time.Now().UTC()
	for pruned < limit && examined < maxExamined {
		pageLimit := min(256, maxExamined-examined)
		records, err := store.backend.Scan(ctx, start, end, pageLimit)
		if err != nil {
			return pruned, err
		}
		if len(records) == 0 {
			start = append(start[:0], rpcIdempotencyKeyPrefix...)
			break
		}
		for _, item := range records {
			examined++
			start = append(append([]byte(nil), item.Key...), 0)
			record, err := decodeRPCIdempotencyRecord(item.Value)
			if err != nil {
				return pruned, err
			}
			if record.state == idempotencyRecordPending || now.Before(record.expiresAt) {
				continue
			}
			transactionID, err := randomToken16()
			if err != nil {
				return pruned, err
			}
			result, err := store.backend.CompareAndSwap(ctx, systemrecord.Mutation{
				TransactionID: transactionID, Key: item.Key, ExpectedExists: true,
				ExpectedHash: sha256.Sum256(item.Value), Delete: true,
			})
			if err != nil {
				return pruned, err
			}
			if result.Applied {
				pruned++
				if pruned == limit {
					break
				}
			}
		}
		if len(records) < pageLimit {
			start = append(start[:0], rpcIdempotencyKeyPrefix...)
			break
		}
	}
	return pruned, nil
}

func (store *durableRPCIdempotencyStore) Claim(ctx context.Context, claim RPCIdempotencyClaim) (RPCIdempotencyDecision, error) {
	if store == nil || store.backend == nil || ctx == nil || allZero16(claim.SessionID) || allZero16(claim.ClaimID) || claim.ExpiresAt.IsZero() {
		return RPCIdempotencyDecision{}, errors.New("invalid durable idempotency claim")
	}
	key := rpcIdempotencyStorageKey(claim)
	pending := rpcIdempotencyRecord{
		state: idempotencyRecordPending, fingerprint: claim.Fingerprint, sessionID: claim.SessionID, claimID: claim.ClaimID, expiresAt: claim.ExpiresAt,
	}
	pendingBytes, err := encodeRPCIdempotencyRecord(pending)
	if err != nil {
		return RPCIdempotencyDecision{}, err
	}
	transactionID, err := randomToken16()
	if err != nil {
		return RPCIdempotencyDecision{}, err
	}
	result, err := store.backend.CompareAndSwap(ctx, systemrecord.Mutation{
		TransactionID: transactionID, Key: key, NewValue: pendingBytes,
	})
	if err != nil {
		return RPCIdempotencyDecision{}, err
	}
	if result.Applied {
		return RPCIdempotencyDecision{Kind: RPCIdempotencyExecute}, nil
	}
	currentBytes := result.Current
	for range rpcIdempotencyClaimAttempts {
		current, err := decodeRPCIdempotencyRecord(currentBytes)
		if err != nil {
			return RPCIdempotencyDecision{}, err
		}
		now := time.Now().UTC()
		if current.state != idempotencyRecordPending && !now.Before(current.expiresAt) {
			replaceID, err := randomToken16()
			if err != nil {
				return RPCIdempotencyDecision{}, err
			}
			replacement, err := store.backend.CompareAndSwap(ctx, systemrecord.Mutation{
				TransactionID: replaceID, Key: key, ExpectedExists: true,
				ExpectedHash: sha256.Sum256(currentBytes), NewValue: pendingBytes,
			})
			if err != nil {
				return RPCIdempotencyDecision{}, err
			}
			if replacement.Applied {
				return RPCIdempotencyDecision{Kind: RPCIdempotencyExecute}, nil
			}
			currentBytes = replacement.Current
			continue
		}
		if !sameRPCFingerprint(current.fingerprint, claim.Fingerprint) {
			return RPCIdempotencyDecision{Kind: RPCIdempotencyConflict}, nil
		}
		switch current.state {
		case idempotencyRecordPending:
			if current.sessionID == claim.SessionID {
				return RPCIdempotencyDecision{Kind: RPCIdempotencyInProgress}, nil
			}
			unknown := current
			unknown.state = idempotencyRecordUnknown
			unknown.expiresAt = claim.ExpiresAt
			unknownBytes, err := encodeRPCIdempotencyRecord(unknown)
			if err != nil {
				return RPCIdempotencyDecision{}, err
			}
			transitionID, err := randomToken16()
			if err != nil {
				return RPCIdempotencyDecision{}, err
			}
			transition, err := store.backend.CompareAndSwap(ctx, systemrecord.Mutation{
				TransactionID: transitionID, Key: key, ExpectedExists: true,
				ExpectedHash: sha256.Sum256(currentBytes), NewValue: unknownBytes,
			})
			if err != nil {
				return RPCIdempotencyDecision{}, err
			}
			if transition.Applied {
				return RPCIdempotencyDecision{Kind: RPCIdempotencyOutcomeUnknown}, nil
			}
			currentBytes = transition.Current
			continue
		case idempotencyRecordResult:
			return RPCIdempotencyDecision{Kind: RPCIdempotencyReplayResult, Result: append([]byte(nil), current.result...)}, nil
		case idempotencyRecordError:
			return RPCIdempotencyDecision{Kind: RPCIdempotencyReplayError, ErrorKind: current.errorKind, ErrorCode: current.errorCode, ErrorData: append([]byte(nil), current.errorData...), ErrorStatus: current.errorStatus}, nil
		case idempotencyRecordUnknown:
			return RPCIdempotencyDecision{Kind: RPCIdempotencyOutcomeUnknown}, nil
		default:
			return RPCIdempotencyDecision{}, errors.New("invalid durable idempotency state")
		}
	}
	return RPCIdempotencyDecision{}, errors.New("durable idempotency claim contention exceeded retry budget")
}

func (store *durableRPCIdempotencyStore) Complete(ctx context.Context, completion RPCIdempotencyCompletion) error {
	if store == nil || store.backend == nil || ctx == nil {
		return errors.New("invalid durable idempotency completion")
	}
	record, err := rpcIdempotencyCompletionRecord(completion)
	if err != nil {
		return err
	}
	return store.transition(ctx, completion.Claim, record)
}

func rpcIdempotencyCompletionRecord(completion RPCIdempotencyCompletion) (rpcIdempotencyRecord, error) {
	if completion.ErrorCode != "" && completion.ErrorKind == "" {
		completion.ErrorKind = "internal"
	}
	record := rpcIdempotencyRecord{
		fingerprint: completion.Claim.Fingerprint, sessionID: completion.Claim.SessionID,
		claimID: completion.Claim.ClaimID, expiresAt: completion.Claim.ExpiresAt,
	}
	if len(completion.Result) > 0 && completion.ErrorKind == "" && completion.ErrorCode == "" && len(completion.ErrorData) == 0 && completion.ErrorStatus == 0 {
		record.state, record.result = idempotencyRecordResult, append([]byte(nil), completion.Result...)
	} else if len(completion.Result) == 0 && (completion.ErrorKind == "business" || completion.ErrorKind == "internal") && completion.ErrorStatus >= 400 && completion.ErrorStatus <= 599 {
		if completion.ErrorKind == "business" && (!rpcBusinessErrorCodePattern.MatchString(completion.ErrorCode) || !validRPCBusinessErrorData(completion.ErrorData)) {
			return rpcIdempotencyRecord{}, errors.New("invalid durable business error")
		}
		if completion.ErrorKind == "internal" && (!rpcInternalErrorCodePattern.MatchString(completion.ErrorCode) || len(completion.ErrorData) != 0) {
			return rpcIdempotencyRecord{}, errors.New("invalid durable internal error")
		}
		record.state, record.errorKind, record.errorCode, record.errorData, record.errorStatus = idempotencyRecordError, completion.ErrorKind, completion.ErrorCode, append([]byte(nil), completion.ErrorData...), completion.ErrorStatus
	} else {
		return rpcIdempotencyRecord{}, errors.New("invalid durable idempotency terminal")
	}
	return record, nil
}

func (store *durableRPCIdempotencyStore) atomicCompletion(completion RPCIdempotencyCompletion) (systemrecord.Mutation, error) {
	if store == nil || store.db == nil {
		return systemrecord.Mutation{}, errors.New("invalid durable idempotency atomic completion")
	}
	mutation, err := store.atomicClaimMutation(completion.Claim)
	if err != nil {
		return systemrecord.Mutation{}, err
	}
	record, err := rpcIdempotencyCompletionRecord(completion)
	if err != nil {
		return systemrecord.Mutation{}, err
	}
	terminal, err := encodeRPCIdempotencyRecord(record)
	if err != nil {
		return systemrecord.Mutation{}, err
	}
	mutation.NewValue = terminal
	return mutation, nil
}

func (store *durableRPCIdempotencyStore) atomicClaimMutation(claim RPCIdempotencyClaim) (systemrecord.Mutation, error) {
	if store == nil || store.db == nil {
		return systemrecord.Mutation{}, errors.New("invalid durable idempotency atomic claim")
	}
	pending, err := encodeRPCIdempotencyRecord(rpcIdempotencyRecord{
		state: idempotencyRecordPending, fingerprint: claim.Fingerprint,
		sessionID: claim.SessionID, claimID: claim.ClaimID, expiresAt: claim.ExpiresAt,
	})
	if err != nil {
		return systemrecord.Mutation{}, err
	}
	return systemrecord.Mutation{
		Key: rpcIdempotencyStorageKey(claim), ExpectedExists: true,
		ExpectedHash: sha256.Sum256(pending),
	}, nil
}

func (store *durableRPCIdempotencyStore) MarkUnknown(ctx context.Context, claim RPCIdempotencyClaim) error {
	return store.transition(ctx, claim, rpcIdempotencyRecord{
		state: idempotencyRecordUnknown, fingerprint: claim.Fingerprint, sessionID: claim.SessionID, claimID: claim.ClaimID, expiresAt: claim.ExpiresAt,
	})
}

func (store *durableRPCIdempotencyStore) transition(ctx context.Context, claim RPCIdempotencyClaim, next rpcIdempotencyRecord) error {
	pending, err := encodeRPCIdempotencyRecord(rpcIdempotencyRecord{
		state: idempotencyRecordPending, fingerprint: claim.Fingerprint, sessionID: claim.SessionID, claimID: claim.ClaimID, expiresAt: claim.ExpiresAt,
	})
	if err != nil {
		return err
	}
	encoded, err := encodeRPCIdempotencyRecord(next)
	if err != nil {
		return err
	}
	transactionID, err := randomToken16()
	if err != nil {
		return err
	}
	result, err := store.backend.CompareAndSwap(ctx, systemrecord.Mutation{
		TransactionID: transactionID, Key: rpcIdempotencyStorageKey(claim), ExpectedExists: true,
		ExpectedHash: sha256.Sum256(pending), NewValue: encoded,
	})
	if err != nil {
		return err
	}
	if !result.Applied {
		return errors.New("durable idempotency claim ownership changed")
	}
	return nil
}

func rpcIdempotencyStorageKey(claim RPCIdempotencyClaim) []byte {
	key := make([]byte, len(rpcIdempotencyKeyPrefix)+64)
	copy(key, rpcIdempotencyKeyPrefix)
	copy(key[len(rpcIdempotencyKeyPrefix):len(rpcIdempotencyKeyPrefix)+32], claim.ScopeHash[:])
	copy(key[len(rpcIdempotencyKeyPrefix)+32:], claim.KeyHash[:])
	return key
}

func encodeRPCIdempotencyRecord(record rpcIdempotencyRecord) ([]byte, error) {
	if record.state < idempotencyRecordPending || record.state > idempotencyRecordUnknown || record.expiresAt.IsZero() ||
		allZero16(record.sessionID) || allZero16(record.claimID) {
		return nil, errors.New("invalid RPC idempotency record")
	}
	if record.state == idempotencyRecordResult {
		if len(record.result) == 0 || record.errorKind != "" || record.errorCode != "" || len(record.errorData) != 0 || record.errorStatus != 0 {
			return nil, errors.New("invalid RPC idempotency result")
		}
	} else if record.state == idempotencyRecordError {
		if len(record.result) != 0 || (record.errorKind != "business" && record.errorKind != "internal") || record.errorStatus < 400 || record.errorStatus > 599 ||
			(record.errorKind == "business" && (!rpcBusinessErrorCodePattern.MatchString(record.errorCode) || !validRPCBusinessErrorData(record.errorData))) ||
			(record.errorKind == "internal" && (!rpcInternalErrorCodePattern.MatchString(record.errorCode) || len(record.errorData) != 0)) {
			return nil, errors.New("invalid RPC idempotency error")
		}
	} else if len(record.result) != 0 || record.errorKind != "" || record.errorCode != "" || len(record.errorData) != 0 || record.errorStatus != 0 {
		return nil, errors.New("invalid RPC idempotency non-terminal")
	}
	if len(record.errorKind) > 8 || len(record.errorCode) > 64 || len(record.result) > 16<<20 || len(record.errorData) > maxRPCErrorDataBytes {
		return nil, errors.New("RPC idempotency terminal exceeds limit")
	}
	payload := record.result
	if record.state == idempotencyRecordError {
		payload = record.errorData
	}
	encoded := make([]byte, rpcIdempotencyHeaderBytes+len(record.errorKind)+len(record.errorCode)+len(payload))
	copy(encoded[:8], rpcIdempotencyRecordMagic[:])
	binary.LittleEndian.PutUint16(encoded[8:10], rpcIdempotencyRecordVersion)
	encoded[10] = record.state
	binary.LittleEndian.PutUint32(encoded[12:16], rpcIdempotencyHeaderBytes)
	binary.LittleEndian.PutUint64(encoded[16:24], uint64(record.expiresAt.UnixMilli()))
	copy(encoded[24:56], record.fingerprint[:])
	copy(encoded[56:72], record.sessionID[:])
	copy(encoded[72:88], record.claimID[:])
	binary.LittleEndian.PutUint16(encoded[88:90], uint16(record.errorStatus))
	encoded[90] = byte(len(record.errorKind))
	binary.LittleEndian.PutUint16(encoded[92:94], uint16(len(record.errorCode)))
	binary.LittleEndian.PutUint32(encoded[94:98], uint32(len(payload)))
	copy(encoded[rpcIdempotencyHeaderBytes:], record.errorKind)
	copy(encoded[rpcIdempotencyHeaderBytes+len(record.errorKind):], record.errorCode)
	copy(encoded[rpcIdempotencyHeaderBytes+len(record.errorKind)+len(record.errorCode):], payload)
	return encoded, nil
}

func decodeRPCIdempotencyRecord(encoded []byte) (rpcIdempotencyRecord, error) {
	if len(encoded) < rpcIdempotencyHeaderBytes || string(encoded[:8]) != string(rpcIdempotencyRecordMagic[:]) || encoded[11] != 0 ||
		binary.LittleEndian.Uint32(encoded[12:16]) != rpcIdempotencyHeaderBytes {
		return rpcIdempotencyRecord{}, errors.New("corrupt RPC idempotency record")
	}
	version := binary.LittleEndian.Uint16(encoded[8:10])
	if version != 1 && version != rpcIdempotencyRecordVersion {
		return rpcIdempotencyRecord{}, errors.New("corrupt RPC idempotency record")
	}
	var kindLength, codeLength, payloadLength int
	var errorKind string
	if version == 1 {
		if !allZeroBytes(encoded[96:rpcIdempotencyHeaderBytes]) {
			return rpcIdempotencyRecord{}, errors.New("corrupt RPC idempotency record")
		}
		codeLength = int(binary.LittleEndian.Uint16(encoded[90:92]))
		payloadLength = int(binary.LittleEndian.Uint32(encoded[92:96]))
	} else {
		if encoded[91] != 0 || !allZeroBytes(encoded[98:rpcIdempotencyHeaderBytes]) {
			return rpcIdempotencyRecord{}, errors.New("corrupt RPC idempotency record")
		}
		kindLength = int(encoded[90])
		codeLength = int(binary.LittleEndian.Uint16(encoded[92:94]))
		payloadLength = int(binary.LittleEndian.Uint32(encoded[94:98]))
	}
	if kindLength > 8 || codeLength > 64 || payloadLength > 16<<20 || rpcIdempotencyHeaderBytes+kindLength+codeLength+payloadLength != len(encoded) {
		return rpcIdempotencyRecord{}, errors.New("corrupt RPC idempotency terminal length")
	}
	expiresMillis := int64(binary.LittleEndian.Uint64(encoded[16:24]))
	if expiresMillis <= 0 {
		return rpcIdempotencyRecord{}, errors.New("corrupt RPC idempotency expiry")
	}
	payloadStart := rpcIdempotencyHeaderBytes + kindLength + codeLength
	if version == rpcIdempotencyRecordVersion && kindLength != 0 {
		errorKind = string(encoded[rpcIdempotencyHeaderBytes : rpcIdempotencyHeaderBytes+kindLength])
	}
	record := rpcIdempotencyRecord{
		state: encoded[10], expiresAt: time.UnixMilli(expiresMillis).UTC(), errorStatus: int(binary.LittleEndian.Uint16(encoded[88:90])),
		errorKind: errorKind,
		errorCode: string(encoded[rpcIdempotencyHeaderBytes+kindLength : payloadStart]),
	}
	payload := append([]byte(nil), encoded[payloadStart:]...)
	if record.state == idempotencyRecordError {
		if version == 1 {
			record.errorKind = "internal"
		}
		record.errorData = payload
	} else {
		record.result = payload
	}
	copy(record.fingerprint[:], encoded[24:56])
	copy(record.sessionID[:], encoded[56:72])
	copy(record.claimID[:], encoded[72:88])
	if _, err := encodeRPCIdempotencyRecord(record); err != nil {
		return rpcIdempotencyRecord{}, err
	}
	return record, nil
}

func allZeroBytes(value []byte) bool {
	var found byte
	for _, item := range value {
		found |= item
	}
	return found == 0
}
