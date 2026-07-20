package anchorhttp

import (
	"fmt"
	"math/bits"
	"testing"
)

const (
	modelOperationActive uint8 = iota
	modelOperationSucceeded
	modelOperationConflicted
	modelOperationQuorumFailed
)

type concurrentModelState struct {
	replicas  [5]uint8
	delivered [2]uint8
	successes [2]uint8
	conflicts [2]uint8
	failures  [2]uint8
	status    [2]uint8
}

type concurrentModelScenario struct {
	name        string
	replicas    int
	coordinates [3]modelCoordinate
	crossed     bool
}

type concurrentModelCounts struct {
	visited          int
	terminal         int
	acknowledged     int
	ambiguousDurable int
}

func TestStaticQuorumExhaustiveConcurrentWriters(t *testing.T) {
	scenarios := []concurrentModelScenario{}
	for _, replicas := range []int{3, 5} {
		scenarios = append(scenarios,
			concurrentModelScenario{
				name: replicasName(replicas, "crossed"), replicas: replicas, crossed: true,
				coordinates: [3]modelCoordinate{{sequence: 0, generation: 1}, {sequence: 1, generation: 2}, {sequence: 0, generation: 3}},
			},
			concurrentModelScenario{
				name: replicasName(replicas, "comparable"), replicas: replicas,
				coordinates: [3]modelCoordinate{{sequence: 0, generation: 1}, {sequence: 1, generation: 2}, {sequence: 2, generation: 3}},
			},
		)
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			counts := exploreConcurrentModel(t, scenario)
			if counts.visited < 100 || counts.terminal == 0 || counts.acknowledged == 0 || counts.ambiguousDurable == 0 {
				t.Fatalf("concurrent model coverage=%+v", counts)
			}
			t.Logf("coverage=%+v", counts)
		})
	}
}

func replicasName(replicas int, suffix string) string {
	return fmt.Sprintf("replicas-%d-%s", replicas, suffix)
}

func exploreConcurrentModel(t *testing.T, scenario concurrentModelScenario) concurrentModelCounts {
	t.Helper()
	seen := make(map[concurrentModelState]struct{})
	counts := concurrentModelCounts{}
	var visit func(concurrentModelState)
	visit = func(state concurrentModelState) {
		if _, exists := seen[state]; exists {
			return
		}
		seen[state] = struct{}{}
		assertConcurrentModelSafety(t, scenario, state)

		allDelivered := uint8(1<<scenario.replicas - 1)
		if state.delivered[0] == allDelivered && state.delivered[1] == allDelivered {
			counts.terminal++
			for operation := 0; operation < 2; operation++ {
				if state.status[operation] == modelOperationActive {
					t.Fatalf("fully delivered operation remained active: %+v", state)
				}
				if state.status[operation] == modelOperationSucceeded {
					counts.acknowledged++
				} else if concurrentModelDurableCount(scenario, state, operation) >= scenario.replicas/2+1 {
					counts.ambiguousDurable++
				}
			}
			return
		}

		for operation := 0; operation < 2; operation++ {
			for member := 0; member < scenario.replicas; member++ {
				bit := uint8(1 << member)
				if state.delivered[operation]&bit != 0 {
					continue
				}
				next := state
				next.delivered[operation] |= bit
				if state.status[operation] != modelOperationActive {
					// Cancellation may drop an unsent request. It may also race a
					// request already executing at the member; explore both outcomes.
					visit(next)
					late := next
					applyConcurrentModelRequest(scenario, &late, operation, member, false)
					visit(late)
					continue
				}
				applyConcurrentModelRequest(scenario, &next, operation, member, true)
				visit(next)

				// The member may persist the same request while its response is
				// lost. This is an endpoint failure to the caller, not an abort.
				lost := state
				lost.delivered[operation] |= bit
				applyConcurrentModelRequest(scenario, &lost, operation, member, false)
				lost.failures[operation]++
				updateConcurrentModelStatus(scenario, &lost, operation)
				visit(lost)
			}
		}
	}
	visit(concurrentModelState{})
	counts.visited = len(seen)
	return counts
}

func applyConcurrentModelRequest(scenario concurrentModelScenario, state *concurrentModelState, operation, member int, countResponse bool) {
	current := scenario.coordinates[state.replicas[member]]
	targetIndex := uint8(operation + 1)
	target := scenario.coordinates[targetIndex]
	accepted := modelBeforeOrEqual(current, target)
	if accepted {
		state.replicas[member] = targetIndex
	}
	if !countResponse {
		return
	}
	if accepted {
		state.successes[operation]++
	} else {
		state.conflicts[operation]++
	}
	updateConcurrentModelStatus(scenario, state, operation)
}

func updateConcurrentModelStatus(scenario concurrentModelScenario, state *concurrentModelState, operation int) {
	if state.status[operation] != modelOperationActive {
		return
	}
	quorum := uint8(scenario.replicas/2 + 1)
	if state.successes[operation] >= quorum {
		state.status[operation] = modelOperationSucceeded
		return
	}
	remaining := uint8(scenario.replicas - bits.OnesCount8(state.delivered[operation]))
	if state.successes[operation]+remaining < quorum {
		if state.conflicts[operation] > 0 {
			state.status[operation] = modelOperationConflicted
		} else {
			state.status[operation] = modelOperationQuorumFailed
		}
	}
}

func concurrentModelDurableCount(scenario concurrentModelScenario, state concurrentModelState, operation int) int {
	target := scenario.coordinates[operation+1]
	count := 0
	for member := 0; member < scenario.replicas; member++ {
		if modelBeforeOrEqual(target, scenario.coordinates[state.replicas[member]]) {
			count++
		}
	}
	return count
}

func assertConcurrentModelSafety(t *testing.T, scenario concurrentModelScenario, state concurrentModelState) {
	t.Helper()
	if scenario.crossed && state.status[0] == modelOperationSucceeded && state.status[1] == modelOperationSucceeded {
		t.Fatalf("crossed writers both acknowledged: %+v", state)
	}
	acknowledged := scenario.coordinates[0]
	for operation := 0; operation < 2; operation++ {
		if state.status[operation] == modelOperationSucceeded {
			target := scenario.coordinates[operation+1]
			if modelBeforeOrEqual(acknowledged, target) {
				acknowledged = target
			}
		}
	}
	quorum := scenario.replicas/2 + 1
	for mask := 0; mask < 1<<scenario.replicas; mask++ {
		if bits.OnesCount(uint(mask)) != quorum {
			continue
		}
		floor, exists := concurrentModelReadFloor(scenario, state, mask)
		if exists && !modelBeforeOrEqual(acknowledged, floor) {
			t.Fatalf("post-ack read quorum fell behind acknowledged=%+v floor=%+v state=%+v mask=%b", acknowledged, floor, state, mask)
		}
	}
}

func concurrentModelReadFloor(scenario concurrentModelScenario, state concurrentModelState, mask int) (modelCoordinate, bool) {
	for candidateMember := 0; candidateMember < scenario.replicas; candidateMember++ {
		if mask&(1<<candidateMember) == 0 {
			continue
		}
		candidate := scenario.coordinates[state.replicas[candidateMember]]
		dominates := true
		for member := 0; member < scenario.replicas; member++ {
			if mask&(1<<member) != 0 && !modelBeforeOrEqual(scenario.coordinates[state.replicas[member]], candidate) {
				dominates = false
				break
			}
		}
		if dominates {
			return candidate, true
		}
	}
	return modelCoordinate{}, false
}
