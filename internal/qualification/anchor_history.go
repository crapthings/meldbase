package qualification

import (
	"errors"
	"fmt"
)

const MaxAnchorHistoryOperations = 24

type AnchorHistoryOperationKind string

const (
	AnchorHistoryLoad    AnchorHistoryOperationKind = "load"
	AnchorHistoryAdvance AnchorHistoryOperationKind = "advance"
)

type AnchorHistoryOutcome string

const (
	AnchorHistorySucceeded AnchorHistoryOutcome = "succeeded"
	AnchorHistoryFailed    AnchorHistoryOutcome = "failed"
)

type AnchorHistoryValue struct {
	Exists         bool     `json:"exists"`
	DatabaseID     [16]byte `json:"databaseId"`
	CommitSequence uint64   `json:"commitSequence"`
	Generation     uint64   `json:"generation"`
}

type AnchorHistoryOperation struct {
	ID      string                     `json:"id"`
	Kind    AnchorHistoryOperationKind `json:"kind"`
	Outcome AnchorHistoryOutcome       `json:"outcome"`
	Invoke  uint64                     `json:"invoke"`
	Return  uint64                     `json:"return"`
	Value   AnchorHistoryValue         `json:"value"`
}

type AnchorHistory struct {
	Initial    AnchorHistoryValue       `json:"initial"`
	Operations []AnchorHistoryOperation `json:"operations"`
}

type AnchorHistoryStep struct {
	OperationID      string `json:"operationId"`
	AmbiguousApplied bool   `json:"ambiguousApplied"`
}

type AnchorHistoryCheck struct {
	Linearizable   bool                `json:"linearizable"`
	ExploredStates uint64              `json:"exploredStates"`
	Linearization  []AnchorHistoryStep `json:"linearization"`
	Violation      string              `json:"violation,omitempty"`
}

// CheckAnchorHistory checks a bounded monotonic-register history. Failed
// advances are ambiguous: each may linearize as either a durable transition or
// a no-op. Failed loads are observational no-ops. Invoke/Return are unique
// controller-assigned event ordinals, not wall-clock timestamps.
func CheckAnchorHistory(history AnchorHistory) (AnchorHistoryCheck, error) {
	if err := validateAnchorHistory(history); err != nil {
		return AnchorHistoryCheck{}, err
	}
	operations := history.Operations
	predecessors := make([]uint32, len(operations))
	for current := range operations {
		for previous := range operations {
			if operations[previous].Return < operations[current].Invoke {
				predecessors[current] |= 1 << previous
			}
		}
	}
	type searchKey struct {
		placed uint32
		value  AnchorHistoryValue
	}
	dead := make(map[searchKey]struct{})
	all := uint32(1<<len(operations)) - 1
	var explored uint64
	var search func(uint32, AnchorHistoryValue) ([]AnchorHistoryStep, bool)
	search = func(placed uint32, value AnchorHistoryValue) ([]AnchorHistoryStep, bool) {
		explored++
		if placed == all {
			return []AnchorHistoryStep{}, true
		}
		key := searchKey{placed: placed, value: value}
		if _, exists := dead[key]; exists {
			return nil, false
		}
		for index, operation := range operations {
			bit := uint32(1 << index)
			if placed&bit != 0 || predecessors[index]&^placed != 0 {
				continue
			}
			transitions := anchorHistoryTransitions(value, operation)
			for _, transition := range transitions {
				tail, ok := search(placed|bit, transition.value)
				if ok {
					step := AnchorHistoryStep{OperationID: operation.ID, AmbiguousApplied: transition.ambiguousApplied}
					return append([]AnchorHistoryStep{step}, tail...), true
				}
			}
		}
		dead[key] = struct{}{}
		return nil, false
	}
	linearization, ok := search(0, history.Initial)
	check := AnchorHistoryCheck{Linearizable: ok, ExploredStates: explored, Linearization: linearization}
	if !ok {
		check.Violation = "no monotonic-register linearization respects the observed results and real-time order"
	}
	return check, nil
}

type anchorHistoryTransition struct {
	value            AnchorHistoryValue
	ambiguousApplied bool
}

func anchorHistoryTransitions(current AnchorHistoryValue, operation AnchorHistoryOperation) []anchorHistoryTransition {
	switch operation.Kind {
	case AnchorHistoryLoad:
		if operation.Outcome == AnchorHistoryFailed {
			return []anchorHistoryTransition{{value: current}}
		}
		if current == operation.Value {
			return []anchorHistoryTransition{{value: current}}
		}
		return nil
	case AnchorHistoryAdvance:
		advanced, valid := advanceAnchorHistoryValue(current, operation.Value)
		if operation.Outcome == AnchorHistorySucceeded {
			if !valid {
				return nil
			}
			return []anchorHistoryTransition{{value: advanced}}
		}
		transitions := []anchorHistoryTransition{{value: current}}
		if valid && advanced != current {
			transitions = append(transitions, anchorHistoryTransition{value: advanced, ambiguousApplied: true})
		}
		return transitions
	default:
		return nil
	}
}

func advanceAnchorHistoryValue(current, target AnchorHistoryValue) (AnchorHistoryValue, bool) {
	if !target.Exists {
		return AnchorHistoryValue{}, false
	}
	if !current.Exists {
		return target, true
	}
	if current.DatabaseID != target.DatabaseID || current.CommitSequence > target.CommitSequence || current.Generation > target.Generation {
		return AnchorHistoryValue{}, false
	}
	return target, true
}

func validateAnchorHistory(history AnchorHistory) error {
	if !validAnchorHistoryValue(history.Initial) {
		return errors.New("anchor history has an invalid initial value")
	}
	if len(history.Operations) < 1 || len(history.Operations) > MaxAnchorHistoryOperations {
		return fmt.Errorf("anchor history requires 1..%d operations", MaxAnchorHistoryOperations)
	}
	identifiers := make(map[string]struct{}, len(history.Operations))
	events := make(map[uint64]struct{}, len(history.Operations)*2)
	for index, operation := range history.Operations {
		if operation.ID == "" || len(operation.ID) > 128 {
			return fmt.Errorf("anchor history operation %d has an invalid ID", index)
		}
		if _, exists := identifiers[operation.ID]; exists {
			return fmt.Errorf("anchor history operation %d duplicates ID %q", index, operation.ID)
		}
		identifiers[operation.ID] = struct{}{}
		if operation.Invoke == 0 || operation.Return <= operation.Invoke {
			return fmt.Errorf("anchor history operation %q has an invalid interval", operation.ID)
		}
		if _, exists := events[operation.Invoke]; exists {
			return fmt.Errorf("anchor history operation %q reuses an event ordinal", operation.ID)
		}
		events[operation.Invoke] = struct{}{}
		if _, exists := events[operation.Return]; exists {
			return fmt.Errorf("anchor history operation %q reuses an event ordinal", operation.ID)
		}
		events[operation.Return] = struct{}{}
		if operation.Outcome != AnchorHistorySucceeded && operation.Outcome != AnchorHistoryFailed {
			return fmt.Errorf("anchor history operation %q has an invalid outcome", operation.ID)
		}
		switch operation.Kind {
		case AnchorHistoryLoad:
			if operation.Outcome == AnchorHistoryFailed {
				if operation.Value != (AnchorHistoryValue{}) {
					return fmt.Errorf("failed anchor history load %q must not claim a value", operation.ID)
				}
			} else if !validAnchorHistoryValue(operation.Value) {
				return fmt.Errorf("anchor history load %q has an invalid value", operation.ID)
			}
		case AnchorHistoryAdvance:
			if !operation.Value.Exists || !validAnchorHistoryValue(operation.Value) {
				return fmt.Errorf("anchor history advance %q has an invalid target", operation.ID)
			}
		default:
			return fmt.Errorf("anchor history operation %q has an invalid kind", operation.ID)
		}
	}
	return nil
}

func validAnchorHistoryValue(value AnchorHistoryValue) bool {
	if !value.Exists {
		return value == (AnchorHistoryValue{})
	}
	return value.DatabaseID != ([16]byte{}) && value.Generation > value.CommitSequence
}
