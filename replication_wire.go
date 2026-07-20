package meldbase

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ReplicationProtocolVersion is intentionally separate from the browser
// realtime protocol. It transports durable database positions between trusted
// servers, not end-user query subscriptions.
const ReplicationProtocolVersion = 1

const (
	DefaultReplicationMaxFrameBytes = 96 << 20
	maxReplicationChanges           = 10_000
)

// ReplicationFrameLimits bounds one already-decompressed protocol frame. The
// default accommodates the configured 64 MiB canonical transaction limit plus
// JSON/base64 overhead, while still rejecting unbounded peer allocation.
type ReplicationFrameLimits struct{ MaxFrameBytes int }

// ReplicationFrame is a transport-neutral protocol envelope. A transport must
// authenticate both peers (for example with mTLS) before it accepts frames;
// DatabaseID binds every frame to one durable source identity.
type ReplicationFrame struct {
	Type       string
	DatabaseID [16]byte
	AfterToken uint64 // hello: receiver's durable position
	MaxBytes   int    // hello: receiver frame cap
	Batch      *DurableDatabaseChangeBatch
	AckToken   uint64
	Reason     string
}

const (
	ReplicationHelloFrame  = "hello"
	ReplicationBatchFrame  = "batch"
	ReplicationAckFrame    = "ack"
	ReplicationResyncFrame = "resync_required"
)

type replicationWireFrame struct {
	Version    int                     `json:"v"`
	Type       string                  `json:"type"`
	DatabaseID string                  `json:"databaseId"`
	After      *uint64                 `json:"afterToken,omitempty"`
	MaxBytes   *int                    `json:"maxBytes,omitempty"`
	Token      *uint64                 `json:"token,omitempty"`
	Txn        string                  `json:"transactionId,omitempty"`
	Committed  *int64                  `json:"committedAtMs,omitempty"`
	Changes    []replicationWireChange `json:"changes,omitempty"`
	Reason     string                  `json:"reason,omitempty"`
}

type replicationWireChange struct {
	Collection string           `json:"collection"`
	Operation  Operation        `json:"operation"`
	DocumentID string           `json:"documentId,omitempty"`
	Before     string           `json:"before,omitempty"`
	After      string           `json:"after,omitempty"`
	Index      *IndexDefinition `json:"index,omitempty"`
	Paths      []string         `json:"changedPaths,omitempty"`
}

// MarshalReplicationFrame returns a strict JSON frame with canonical document
// images encoded as base64 of the storage-independent typed document codec.
// It is suitable for WebSocket binary/text messages, QUIC streams or framed
// RPC, but it does not provide authentication or encryption itself.
func MarshalReplicationFrame(frame ReplicationFrame, limits ReplicationFrameLimits) ([]byte, error) {
	limits = normalizedReplicationFrameLimits(limits)
	wire, err := encodeReplicationFrame(frame)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("%w: encode", ErrReplicaProtocol)
	}
	if len(encoded) > limits.MaxFrameBytes {
		return nil, fmt.Errorf("%w: frame exceeds %d bytes", ErrReplicaProtocol, limits.MaxFrameBytes)
	}
	return encoded, nil
}

// UnmarshalReplicationFrame rejects unknown fields, duplicate JSON keys,
// malformed base64, invalid typed documents and non-canonical identities before
// a receiver reaches the follower state machine.
func UnmarshalReplicationFrame(data []byte, limits ReplicationFrameLimits) (ReplicationFrame, error) {
	limits = normalizedReplicationFrameLimits(limits)
	if len(data) == 0 || len(data) > limits.MaxFrameBytes {
		return ReplicationFrame{}, fmt.Errorf("%w: frame size", ErrReplicaProtocol)
	}
	if err := ValidateStrictJSON(data, limits.MaxFrameBytes); err != nil {
		return ReplicationFrame{}, fmt.Errorf("%w: %v", ErrReplicaProtocol, err)
	}
	var wire replicationWireFrame
	if err := strictJSON(data, &wire); err != nil {
		return ReplicationFrame{}, fmt.Errorf("%w: malformed envelope", ErrReplicaProtocol)
	}
	return decodeReplicationFrame(wire)
}

func normalizedReplicationFrameLimits(limits ReplicationFrameLimits) ReplicationFrameLimits {
	if limits.MaxFrameBytes == 0 {
		limits.MaxFrameBytes = DefaultReplicationMaxFrameBytes
	}
	if limits.MaxFrameBytes < 64<<10 || limits.MaxFrameBytes > DefaultReplicationMaxFrameBytes {
		// An invalid local configuration is treated the same as an unsafe frame:
		// no peer can enlarge allocation authority through a hello message.
		limits.MaxFrameBytes = 64 << 10
	}
	return limits
}

func encodeReplicationFrame(frame ReplicationFrame) (replicationWireFrame, error) {
	if frame.DatabaseID == [16]byte{} {
		return replicationWireFrame{}, fmt.Errorf("%w: zero database identity", ErrReplicaProtocol)
	}
	wire := replicationWireFrame{Version: ReplicationProtocolVersion, Type: frame.Type, DatabaseID: hex.EncodeToString(frame.DatabaseID[:])}
	switch frame.Type {
	case ReplicationHelloFrame:
		if frame.Batch != nil || frame.MaxBytes < 64<<10 || frame.MaxBytes > DefaultReplicationMaxFrameBytes {
			return replicationWireFrame{}, fmt.Errorf("%w: invalid hello", ErrReplicaProtocol)
		}
		after, maxBytes := frame.AfterToken, frame.MaxBytes
		wire.After, wire.MaxBytes = &after, &maxBytes
	case ReplicationAckFrame:
		if frame.Batch != nil || frame.Reason != "" {
			return replicationWireFrame{}, fmt.Errorf("%w: invalid ack", ErrReplicaProtocol)
		}
		token := frame.AckToken
		wire.Token = &token
	case ReplicationResyncFrame:
		if frame.Batch != nil || !validReplicationResyncReason(frame.Reason) {
			return replicationWireFrame{}, fmt.Errorf("%w: invalid resync", ErrReplicaProtocol)
		}
		wire.Reason = frame.Reason
	case ReplicationBatchFrame:
		if frame.Batch == nil {
			return replicationWireFrame{}, fmt.Errorf("%w: missing batch", ErrReplicaProtocol)
		}
		batch := frame.Batch
		if batch.Token == 0 || batch.TransactionID == [16]byte{} || batch.CommittedAt.IsZero() || len(batch.Changes) > maxReplicationChanges {
			return replicationWireFrame{}, fmt.Errorf("%w: invalid batch", ErrReplicaProtocol)
		}
		token, committed := batch.Token, batch.CommittedAt.UTC().UnixMilli()
		wire.Token, wire.Committed, wire.Txn = &token, &committed, hex.EncodeToString(batch.TransactionID[:])
		wire.Changes = make([]replicationWireChange, len(batch.Changes))
		for index, change := range batch.Changes {
			encoded, err := encodeReplicationChange(change)
			if err != nil {
				return replicationWireFrame{}, err
			}
			wire.Changes[index] = encoded
		}
	default:
		return replicationWireFrame{}, fmt.Errorf("%w: unknown type", ErrReplicaProtocol)
	}
	return wire, nil
}

func decodeReplicationFrame(wire replicationWireFrame) (ReplicationFrame, error) {
	if wire.Version != ReplicationProtocolVersion {
		return ReplicationFrame{}, fmt.Errorf("%w: unsupported version", ErrReplicaProtocol)
	}
	id, err := parseReplicationHex(wire.DatabaseID, 16)
	if err != nil {
		return ReplicationFrame{}, err
	}
	frame := ReplicationFrame{Type: wire.Type}
	copy(frame.DatabaseID[:], id)
	switch wire.Type {
	case ReplicationHelloFrame:
		if wire.After == nil || wire.MaxBytes == nil || wire.Token != nil || wire.Txn != "" || wire.Committed != nil || len(wire.Changes) != 0 || wire.Reason != "" || *wire.MaxBytes < 64<<10 || *wire.MaxBytes > DefaultReplicationMaxFrameBytes {
			return ReplicationFrame{}, fmt.Errorf("%w: invalid hello", ErrReplicaProtocol)
		}
		frame.AfterToken, frame.MaxBytes = *wire.After, *wire.MaxBytes
	case ReplicationAckFrame:
		if wire.Token == nil || wire.After != nil || wire.MaxBytes != nil || wire.Txn != "" || wire.Committed != nil || len(wire.Changes) != 0 || wire.Reason != "" {
			return ReplicationFrame{}, fmt.Errorf("%w: invalid ack", ErrReplicaProtocol)
		}
		frame.AckToken = *wire.Token
	case ReplicationResyncFrame:
		if wire.After != nil || wire.MaxBytes != nil || wire.Token != nil || wire.Txn != "" || wire.Committed != nil || len(wire.Changes) != 0 || !validReplicationResyncReason(wire.Reason) {
			return ReplicationFrame{}, fmt.Errorf("%w: invalid resync", ErrReplicaProtocol)
		}
		frame.Reason = wire.Reason
	case ReplicationBatchFrame:
		if wire.Token == nil || *wire.Token == 0 || wire.Committed == nil || *wire.Committed <= 0 || wire.After != nil || wire.MaxBytes != nil || wire.Reason != "" || len(wire.Changes) > maxReplicationChanges {
			return ReplicationFrame{}, fmt.Errorf("%w: invalid batch", ErrReplicaProtocol)
		}
		txn, err := parseReplicationHex(wire.Txn, 16)
		if err != nil {
			return ReplicationFrame{}, err
		}
		batch := &DurableDatabaseChangeBatch{Token: *wire.Token, CommittedAt: time.UnixMilli(*wire.Committed).UTC()}
		copy(batch.TransactionID[:], txn)
		batch.Changes = make([]Change, len(wire.Changes))
		for index, change := range wire.Changes {
			decoded, err := decodeReplicationChange(change)
			if err != nil {
				return ReplicationFrame{}, err
			}
			batch.Changes[index] = decoded
		}
		frame.Batch = batch
	default:
		return ReplicationFrame{}, fmt.Errorf("%w: unknown type", ErrReplicaProtocol)
	}
	return frame, nil
}

func encodeReplicationChange(change Change) (replicationWireChange, error) {
	if !collectionNamePattern.MatchString(change.Collection) {
		return replicationWireChange{}, fmt.Errorf("%w: invalid collection", ErrReplicaProtocol)
	}
	if !validReplicationPaths(change.ChangedPaths) {
		return replicationWireChange{}, fmt.Errorf("%w: invalid changed paths", ErrReplicaProtocol)
	}
	result := replicationWireChange{Collection: change.Collection, Operation: change.Operation, Paths: append([]string(nil), change.ChangedPaths...)}
	if change.Operation == CreateCollectionOperation {
		if change.DocumentID != (DocumentID{}) || change.Before != nil || change.After != nil || change.Index != nil || len(change.ChangedPaths) != 1 || change.ChangedPaths[0] != "_catalog" {
			return replicationWireChange{}, fmt.Errorf("%w: invalid collection event", ErrReplicaProtocol)
		}
		return result, nil
	}
	if change.Operation == CreateIndexOperation {
		if change.Index == nil || change.Before != nil || change.After != nil || len(change.ChangedPaths) != 1 || change.ChangedPaths[0] != "_indexes."+change.Index.Name {
			return replicationWireChange{}, fmt.Errorf("%w: invalid index event", ErrReplicaProtocol)
		}
		definition := cloneIndexDefinition(*change.Index)
		if _, err := validateIndexDefinition(definition.Name, definition.Fields, IndexOptions{Unique: definition.Unique}); err != nil {
			return replicationWireChange{}, fmt.Errorf("%w: invalid index", ErrReplicaProtocol)
		}
		result.Index = &definition
		return result, nil
	}
	if change.DocumentID.IsZero() {
		return replicationWireChange{}, fmt.Errorf("%w: zero document identity", ErrReplicaProtocol)
	}
	result.DocumentID = change.DocumentID.String()
	encode := func(document *Document) (string, error) {
		if document == nil {
			return "", nil
		}
		encoded, err := encodeStoredDocument(*document)
		if err != nil {
			return "", err
		}
		return base64.StdEncoding.EncodeToString(encoded), nil
	}
	var err error
	if result.Before, err = encode(change.Before); err != nil {
		return replicationWireChange{}, fmt.Errorf("%w: before document", ErrReplicaProtocol)
	}
	if result.After, err = encode(change.After); err != nil {
		return replicationWireChange{}, fmt.Errorf("%w: after document", ErrReplicaProtocol)
	}
	if !validReplicationDocumentShape(change.Operation, result.Before != "", result.After != "") {
		return replicationWireChange{}, fmt.Errorf("%w: invalid document event", ErrReplicaProtocol)
	}
	return result, nil
}

func decodeReplicationChange(wire replicationWireChange) (Change, error) {
	if !collectionNamePattern.MatchString(wire.Collection) {
		return Change{}, fmt.Errorf("%w: invalid collection", ErrReplicaProtocol)
	}
	if !validReplicationPaths(wire.Paths) {
		return Change{}, fmt.Errorf("%w: invalid changed paths", ErrReplicaProtocol)
	}
	result := Change{Collection: wire.Collection, Operation: wire.Operation, ChangedPaths: append([]string(nil), wire.Paths...)}
	if wire.Operation == CreateCollectionOperation {
		if wire.DocumentID != "" || wire.Before != "" || wire.After != "" || wire.Index != nil || len(wire.Paths) != 1 || wire.Paths[0] != "_catalog" {
			return Change{}, fmt.Errorf("%w: invalid collection event", ErrReplicaProtocol)
		}
		return result, nil
	}
	if wire.Operation == CreateIndexOperation {
		if wire.DocumentID != "" || wire.Before != "" || wire.After != "" || wire.Index == nil || len(wire.Paths) != 1 || wire.Paths[0] != "_indexes."+wire.Index.Name {
			return Change{}, fmt.Errorf("%w: invalid index event", ErrReplicaProtocol)
		}
		definition := cloneIndexDefinition(*wire.Index)
		if _, err := validateIndexDefinition(definition.Name, definition.Fields, IndexOptions{Unique: definition.Unique}); err != nil {
			return Change{}, fmt.Errorf("%w: invalid index", ErrReplicaProtocol)
		}
		result.Index = &definition
		return result, nil
	}
	id, err := ParseDocumentID(wire.DocumentID)
	if err != nil {
		return Change{}, fmt.Errorf("%w: document identity", ErrReplicaProtocol)
	}
	result.DocumentID = id
	decode := func(value string) (*Document, error) {
		if value == "" {
			return nil, nil
		}
		encoded, err := base64.StdEncoding.Strict().DecodeString(value)
		if err != nil || base64.StdEncoding.EncodeToString(encoded) != value {
			return nil, fmt.Errorf("%w: document base64", ErrReplicaProtocol)
		}
		document, err := decodeStoredDocument(encoded)
		if err != nil {
			return nil, fmt.Errorf("%w: document", ErrReplicaProtocol)
		}
		if documentID, exists := document.ID(); !exists || documentID != id {
			return nil, fmt.Errorf("%w: document identity", ErrReplicaProtocol)
		}
		return &document, nil
	}
	if result.Before, err = decode(wire.Before); err != nil {
		return Change{}, err
	}
	if result.After, err = decode(wire.After); err != nil {
		return Change{}, err
	}
	if !validReplicationDocumentShape(result.Operation, result.Before != nil, result.After != nil) {
		return Change{}, fmt.Errorf("%w: invalid document event", ErrReplicaProtocol)
	}
	return result, nil
}

func validReplicationDocumentShape(operation Operation, before, after bool) bool {
	switch operation {
	case InsertOperation:
		return !before && after
	case UpdateOperation:
		return before && after
	case DeleteOperation:
		return before && !after
	default:
		return false
	}
}

func validReplicationPaths(paths []string) bool {
	if len(paths) > maxReplicationChanges {
		return false
	}
	for index, path := range paths {
		if validatePath(path) != nil || (index > 0 && paths[index-1] >= path) {
			return false
		}
	}
	return true
}

func parseReplicationHex(value string, size int) ([]byte, error) {
	if len(value) != size*2 || strings.ToLower(value) != value {
		return nil, fmt.Errorf("%w: non-canonical identity", ErrReplicaProtocol)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != size || hex.EncodeToString(decoded) != value {
		return nil, fmt.Errorf("%w: malformed identity", ErrReplicaProtocol)
	}
	return decoded, nil
}

func validReplicationResyncReason(reason string) bool {
	return reason == "history_lost" || reason == "identity_mismatch" || reason == "snapshot_required" || reason == "protocol_error"
}
