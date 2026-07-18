package qualification

import "testing"

func TestAnchorHistoryCheckerAcceptsAmbiguousAdvanceNeededByLaterLoad(t *testing.T) {
	target := anchorHistoryTestValue(1, 2)
	history := AnchorHistory{Operations: []AnchorHistoryOperation{
		{ID: "advance", Kind: AnchorHistoryAdvance, Outcome: AnchorHistoryFailed, Invoke: 1, Return: 2, Value: target},
		{ID: "load", Kind: AnchorHistoryLoad, Outcome: AnchorHistorySucceeded, Invoke: 3, Return: 4, Value: target},
	}}
	check, err := CheckAnchorHistory(history)
	if err != nil || !check.Linearizable || len(check.Linearization) != 2 || !check.Linearization[0].AmbiguousApplied {
		t.Fatalf("check=%+v err=%v", check, err)
	}
}

func TestAnchorHistoryCheckerRejectsStaleRealTimeLoad(t *testing.T) {
	target := anchorHistoryTestValue(1, 2)
	history := AnchorHistory{Operations: []AnchorHistoryOperation{
		{ID: "advance", Kind: AnchorHistoryAdvance, Outcome: AnchorHistorySucceeded, Invoke: 1, Return: 2, Value: target},
		{ID: "load", Kind: AnchorHistoryLoad, Outcome: AnchorHistorySucceeded, Invoke: 3, Return: 4, Value: AnchorHistoryValue{}},
	}}
	check, err := CheckAnchorHistory(history)
	if err != nil || check.Linearizable || check.Violation == "" {
		t.Fatalf("check=%+v err=%v", check, err)
	}
}

func TestAnchorHistoryCheckerAllowsOverlappingLoadBeforeAdvance(t *testing.T) {
	history := AnchorHistory{Operations: []AnchorHistoryOperation{
		{ID: "advance", Kind: AnchorHistoryAdvance, Outcome: AnchorHistorySucceeded, Invoke: 1, Return: 4, Value: anchorHistoryTestValue(1, 2)},
		{ID: "overlap-load", Kind: AnchorHistoryLoad, Outcome: AnchorHistorySucceeded, Invoke: 2, Return: 3, Value: AnchorHistoryValue{}},
	}}
	check, err := CheckAnchorHistory(history)
	if err != nil || !check.Linearizable || len(check.Linearization) != 2 || check.Linearization[0].OperationID != "overlap-load" {
		t.Fatalf("check=%+v err=%v", check, err)
	}
}

func TestAnchorHistoryCheckerRejectsSuccessfulRealTimeRegression(t *testing.T) {
	history := AnchorHistory{Operations: []AnchorHistoryOperation{
		{ID: "higher", Kind: AnchorHistoryAdvance, Outcome: AnchorHistorySucceeded, Invoke: 1, Return: 2, Value: anchorHistoryTestValue(2, 3)},
		{ID: "lower", Kind: AnchorHistoryAdvance, Outcome: AnchorHistorySucceeded, Invoke: 3, Return: 4, Value: anchorHistoryTestValue(1, 2)},
	}}
	check, err := CheckAnchorHistory(history)
	if err != nil || check.Linearizable {
		t.Fatalf("check=%+v err=%v", check, err)
	}
}

func TestAnchorHistoryCheckerRejectsTwoSuccessfulCrossedWriters(t *testing.T) {
	history := AnchorHistory{Operations: []AnchorHistoryOperation{
		{ID: "logical", Kind: AnchorHistoryAdvance, Outcome: AnchorHistorySucceeded, Invoke: 1, Return: 4, Value: anchorHistoryTestValue(1, 2)},
		{ID: "maintenance", Kind: AnchorHistoryAdvance, Outcome: AnchorHistorySucceeded, Invoke: 2, Return: 3, Value: anchorHistoryTestValue(0, 3)},
	}}
	check, err := CheckAnchorHistory(history)
	if err != nil || check.Linearizable {
		t.Fatalf("check=%+v err=%v", check, err)
	}
}

func TestAnchorHistoryCheckerOrdersOverlappingComparableWriters(t *testing.T) {
	higher := anchorHistoryTestValue(2, 3)
	history := AnchorHistory{Operations: []AnchorHistoryOperation{
		{ID: "lower", Kind: AnchorHistoryAdvance, Outcome: AnchorHistorySucceeded, Invoke: 1, Return: 4, Value: anchorHistoryTestValue(1, 2)},
		{ID: "higher", Kind: AnchorHistoryAdvance, Outcome: AnchorHistorySucceeded, Invoke: 2, Return: 3, Value: higher},
		{ID: "read", Kind: AnchorHistoryLoad, Outcome: AnchorHistorySucceeded, Invoke: 5, Return: 6, Value: higher},
	}}
	check, err := CheckAnchorHistory(history)
	if err != nil || !check.Linearizable || len(check.Linearization) != 3 || check.Linearization[0].OperationID != "lower" {
		t.Fatalf("check=%+v err=%v", check, err)
	}
}

func TestAnchorHistoryCheckerRejectsMalformedInput(t *testing.T) {
	_, err := CheckAnchorHistory(AnchorHistory{Operations: []AnchorHistoryOperation{{
		ID: "bad", Kind: AnchorHistoryLoad, Outcome: AnchorHistorySucceeded, Invoke: 1, Return: 1,
	}}})
	if err == nil {
		t.Fatal("malformed history accepted")
	}
}

func anchorHistoryTestValue(sequence, generation uint64) AnchorHistoryValue {
	return AnchorHistoryValue{Exists: true, DatabaseID: [16]byte{1}, CommitSequence: sequence, Generation: generation}
}
