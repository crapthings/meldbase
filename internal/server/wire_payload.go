package server

import (
	"encoding/json"
	"fmt"

	"github.com/crapthings/meldbase"
)

const (
	// realtimeEnvelopeBytes reserves response space for realtime IDs and resume tokens.
	//
	// Reserve bounded room before accumulating document bytes so
	// oversized snapshots and deltas fail before the final WebSocket marshal.
	realtimeEnvelopeBytes = 512
	// httpQueryEnvelopeBytes reserves bytes for the outer HTTP query response.
	httpQueryEnvelopeBytes = 64
)

// wirePayloadBudget tracks encoded item bytes within the space left after reserving the response envelope.
type wirePayloadBudget struct{ limit, used int }

// newWirePayloadBudget reserves envelope bytes and rejects a limit that leaves no room for items.
func newWirePayloadBudget(limit, reserved int) (*wirePayloadBudget, error) {
	if limit <= reserved {
		return nil, fmt.Errorf("%w: result budget %d cannot hold envelope", meldbase.ErrResourceLimit, limit)
	}
	return &wirePayloadBudget{limit: limit - reserved}, nil
}

// add charges an encoded item and its delimiter against the remaining payload budget.
func (budget *wirePayloadBudget) add(raw []byte) error {
	// One byte covers either the surrounding array delimiter or the comma before
	// this item. The check is overflow-safe because lengths are non-negative.
	if len(raw) > budget.limit-budget.used-1 {
		return fmt.Errorf("%w: encoded result exceeds %d bytes", meldbase.ErrResourceLimit, budget.limit)
	}
	budget.used += len(raw) + 1
	return nil
}

// encodeProjectedDocuments projects and encodes documents within a payload limit and envelope reservation.
func encodeProjectedDocuments(documents []meldbase.Document, policy QueryPolicy, limit, reserved int) ([]json.RawMessage, error) {
	budget, err := newWirePayloadBudget(limit, reserved)
	if err != nil {
		return nil, err
	}
	encoded := make([]json.RawMessage, len(documents))
	for index, document := range documents {
		raw, err := meldbase.MarshalWireDocument(project(document, policy))
		if err != nil {
			return nil, err
		}
		if err := budget.add(raw); err != nil {
			return nil, err
		}
		encoded[index] = raw
	}
	return encoded, nil
}

// encodeVisibleDocuments encodes already-visible documents within the realtime payload limit.
func encodeVisibleDocuments(documents []meldbase.Document, limit int) ([]json.RawMessage, error) {
	budget, err := newWirePayloadBudget(limit, realtimeEnvelopeBytes)
	if err != nil {
		return nil, err
	}
	encoded := make([]json.RawMessage, len(documents))
	for index, document := range documents {
		raw, err := meldbase.MarshalWireDocument(document)
		if err != nil {
			return nil, err
		}
		if err := budget.add(raw); err != nil {
			return nil, err
		}
		encoded[index] = raw
	}
	return encoded, nil
}

// encodeVisibleDelta encodes already-visible delta operations within the realtime payload limit.
func encodeVisibleDelta(delta meldbase.QueryDelta, limit int) ([]json.RawMessage, error) {
	budget, err := newWirePayloadBudget(limit, realtimeEnvelopeBytes)
	if err != nil {
		return nil, err
	}
	operations := make([]json.RawMessage, len(delta.Operations))
	for index, operation := range delta.Operations {
		wire := map[string]any{"op": string(operation.Kind), "id": operation.DocumentID.String()}
		if !operation.BeforeID.IsZero() {
			wire["before"] = operation.BeforeID.String()
		}
		if operation.Document != nil {
			raw, err := meldbase.MarshalWireDocument(operation.Document)
			if err != nil {
				return nil, err
			}
			wire["document"] = json.RawMessage(raw)
		}
		raw, err := json.Marshal(wire)
		if err != nil {
			return nil, err
		}
		if err := budget.add(raw); err != nil {
			return nil, err
		}
		operations[index] = raw
	}
	return operations, nil
}
