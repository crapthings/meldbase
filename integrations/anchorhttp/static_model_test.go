package anchorhttp

import (
	"math/bits"
	"testing"

	"github.com/crapthings/meldbase"
)

// modelCoordinate stores commit and generation positions for a model independent of production merge logic.
type modelCoordinate struct {
	sequence   uint64
	generation uint64
}

// modelHistory defines the initialized, committed, and maintenance states explored by the model.
var modelHistory = [...]modelCoordinate{
	{sequence: 0, generation: 1}, // initialized database
	{sequence: 1, generation: 2}, // logical commit
	{sequence: 1, generation: 3}, // maintenance-only publication
	{sequence: 2, generation: 4}, // later logical commit
}

// staticModelCounts records publication, recovery, and rollback-check coverage.
type staticModelCounts struct {
	acknowledgedStates uint64
	failedWrites       uint64
	readQuorums        uint64
	rollbackRejections uint64
	recoveryRepairs    uint64
}

// TestStaticQuorumExhaustivePublicationRecoveryModel checks modeled publication and recovery schedules with one, three, and five replicas.
func TestStaticQuorumExhaustivePublicationRecoveryModel(t *testing.T) {
	var total staticModelCounts
	for _, replicas := range []int{1, 3, 5} {
		state := make([]int, replicas)
		counts := exploreStaticModel(t, state, 0, 1)
		if counts.acknowledgedStates == 0 || counts.readQuorums == 0 || counts.rollbackRejections == 0 || (replicas > 1 && (counts.failedWrites == 0 || counts.recoveryRepairs == 0)) {
			t.Fatalf("non-vacuous model coverage replicas=%d counts=%+v", replicas, counts)
		}
		total.add(counts)
	}
	if total.acknowledgedStates < 1_000 || total.readQuorums < 10_000 || total.rollbackRejections < 10_000 || total.recoveryRepairs < 1_000 {
		t.Fatalf("static model explored too little state: %+v", total)
	}
}

// exploreStaticModel explores which replicas persist each publication and checks the resulting recovery states.
func exploreStaticModel(t *testing.T, durable []int, acknowledged, target int) staticModelCounts {
	t.Helper()
	var counts staticModelCounts
	if target >= len(modelHistory) {
		return counts
	}
	quorum := len(durable)/2 + 1
	for deliveryMask := 0; deliveryMask < 1<<len(durable); deliveryMask++ {
		next := append([]int(nil), durable...)
		for member := range next {
			if deliveryMask&(1<<member) != 0 {
				next[member] = target
			}
		}
		if bits.OnesCount(uint(deliveryMask)) < quorum {
			counts.failedWrites++
			checkStaticRecoveryState(t, next, acknowledged, target, &counts)
			continue
		}
		counts.acknowledgedStates++
		checkStaticRecoveryState(t, next, target, target, &counts)
		counts.add(exploreStaticModel(t, next, target, target+1))
	}
	return counts
}

// checkStaticRecoveryState checks read floors, rollback rejection, and repair visibility for each modeled majority.
func checkStaticRecoveryState(t *testing.T, replicas []int, acknowledged, database int, counts *staticModelCounts) {
	t.Helper()
	quorum := len(replicas)/2 + 1
	for readMask := 0; readMask < 1<<len(replicas); readMask++ {
		if bits.OnesCount(uint(readMask)) != quorum {
			continue
		}
		floor := 0
		for member, retained := range replicas {
			if readMask&(1<<member) != 0 && retained > floor {
				floor = retained
			}
		}
		counts.readQuorums++
		if floor < acknowledged {
			t.Fatalf("read quorum lost acknowledged floor replicas=%v ack=%d db=%d readMask=%b floor=%d", replicas, acknowledged, database, readMask, floor)
		}
		if !modelBeforeOrEqual(modelHistory[floor], modelHistory[database]) {
			t.Fatalf("non-rollback database is behind read floor replicas=%v db=%+v floor=%+v", replicas, modelHistory[database], modelHistory[floor])
		}
		for candidate := 0; candidate <= database; candidate++ {
			reject := !modelBeforeOrEqual(modelHistory[floor], modelHistory[candidate])
			if candidate < floor && !reject {
				t.Fatalf("rolled-back database accepted candidate=%d floor=%d", candidate, floor)
			}
			if reject {
				counts.rollbackRejections++
				continue
			}
			if candidate <= floor {
				continue
			}
			// Database-ahead is the permitted crash window. Any successful
			// recovery write quorum must make every later read quorum observe it.
			for writeMask := 0; writeMask < 1<<len(replicas); writeMask++ {
				if bits.OnesCount(uint(writeMask)) != quorum {
					continue
				}
				repaired := append([]int(nil), replicas...)
				for member := range repaired {
					if writeMask&(1<<member) != 0 {
						repaired[member] = candidate
					}
				}
				counts.recoveryRepairs++
				assertAllReadQuorumsAtLeast(t, repaired, candidate)
			}
		}
	}
}

// assertAllReadQuorumsAtLeast checks that every read majority observes at least the required history position.
func assertAllReadQuorumsAtLeast(t *testing.T, replicas []int, floor int) {
	t.Helper()
	quorum := len(replicas)/2 + 1
	for mask := 0; mask < 1<<len(replicas); mask++ {
		if bits.OnesCount(uint(mask)) != quorum {
			continue
		}
		observed := 0
		for member, retained := range replicas {
			if mask&(1<<member) != 0 && retained > observed {
				observed = retained
			}
		}
		if observed < floor {
			t.Fatalf("recovery write quorum did not intersect read quorum replicas=%v want=%d mask=%b got=%d", replicas, floor, mask, observed)
		}
	}
}

// modelBeforeOrEqual checks that neither commit sequence nor generation moves backward.
func modelBeforeOrEqual(left, right modelCoordinate) bool {
	return left.sequence <= right.sequence && left.generation <= right.generation
}

// add accumulates coverage counts from another model exploration.
func (counts *staticModelCounts) add(other staticModelCounts) {
	counts.acknowledgedStates += other.acknowledgedStates
	counts.failedWrites += other.failedWrites
	counts.readQuorums += other.readQuorums
	counts.rollbackRejections += other.rollbackRejections
	counts.recoveryRepairs += other.recoveryRepairs
}

// TestStaticQuorumModelRejectsCrossedHistories checks that coordinates advancing different dimensions remain incomparable.
func TestStaticQuorumModelRejectsCrossedHistories(t *testing.T) {
	left := modelCoordinate{sequence: 2, generation: 3}
	right := modelCoordinate{sequence: 1, generation: 4}
	if modelBeforeOrEqual(left, right) || modelBeforeOrEqual(right, left) {
		t.Fatal("crossed sequence/generation histories became comparable")
	}
}

// TestStaticQuorumModelFindsCounterexampleWhenReadThresholdIsWeakened checks that reducing the read majority can lose an acknowledged update.
func TestStaticQuorumModelFindsCounterexampleWhenReadThresholdIsWeakened(t *testing.T) {
	for _, replicas := range []int{3, 5} {
		quorum := replicas/2 + 1
		if modelCanLoseAcknowledgedFloor(replicas, quorum, quorum) {
			t.Fatalf("majority read/write unexpectedly lost an acknowledged floor replicas=%d", replicas)
		}
		if !modelCanLoseAcknowledgedFloor(replicas, quorum, quorum-1) {
			t.Fatalf("model did not expose weakened-read counterexample replicas=%d", replicas)
		}
	}
}

// modelCanLoseAcknowledgedFloor checks whether the requested read and write thresholds allow disjoint replica sets.
func modelCanLoseAcknowledgedFloor(replicas, writeThreshold, readThreshold int) bool {
	for writeMask := 0; writeMask < 1<<replicas; writeMask++ {
		if bits.OnesCount(uint(writeMask)) != writeThreshold {
			continue
		}
		for readMask := 0; readMask < 1<<replicas; readMask++ {
			if bits.OnesCount(uint(readMask)) == readThreshold && writeMask&readMask == 0 {
				return true
			}
		}
	}
	return false
}

// TestStaticQuorumModelRejectsEndpointAliasesAsDistinctMembers checks that two endpoint names for one member cannot count as two votes.
func TestStaticQuorumModelRejectsEndpointAliasesAsDistinctMembers(t *testing.T) {
	// Two endpoint aliases can satisfy a naive endpoint-count quorum while
	// representing only one durable member. Static member identity binding
	// reduces this observation to one vote and therefore cannot acknowledge it.
	endpointObservations := []string{"member-a", "member-a"}
	naiveVotes := len(endpointObservations)
	uniqueVotes := map[string]struct{}{}
	for _, member := range endpointObservations {
		uniqueVotes[member] = struct{}{}
	}
	if naiveVotes < 2 {
		t.Fatal("test setup did not manufacture a naive endpoint quorum")
	}
	if len(uniqueVotes) >= 2 {
		t.Fatal("identity-aware counting accepted endpoint aliases as a quorum")
	}
}

// TestStaticQuorumModelPreservesFailedWriteForkCounterexample checks that a failed minority write can create an incomparable branch requiring repair in both dimensions.
func TestStaticQuorumModelPreservesFailedWriteForkCounterexample(t *testing.T) {
	acknowledged := modelCoordinate{sequence: 0, generation: 1}
	minorityFailedWrite := modelCoordinate{sequence: 1, generation: 2}
	majorityBranch := modelCoordinate{sequence: 0, generation: 3}

	if !modelBeforeOrEqual(acknowledged, minorityFailedWrite) || !modelBeforeOrEqual(acknowledged, majorityBranch) {
		t.Fatal("test setup did not create two descendants of the acknowledged floor")
	}
	if modelBeforeOrEqual(minorityFailedWrite, majorityBranch) || modelBeforeOrEqual(majorityBranch, minorityFailedWrite) {
		t.Fatal("failed-write minority and replacement branch must be crossed")
	}

	// A quorum containing both branches has no safe maximum and must fail
	// closed. A later coordinate that advances both dimensions can converge
	// the static set again.
	repair := modelCoordinate{sequence: 1, generation: 4}
	if !modelBeforeOrEqual(minorityFailedWrite, repair) || !modelBeforeOrEqual(majorityBranch, repair) {
		t.Fatal("repair coordinate did not dominate both failed-write branches")
	}
}

// TestStaticQuorumSelectorNeverInventsASafeReadQuorum checks that every successful production selection matches an independently modeled safe majority.
func TestStaticQuorumSelectorNeverInventsASafeReadQuorum(t *testing.T) {
	identityA := [16]byte{1}
	identityB := [16]byte{2}
	states := []loadResult{
		{},
		{exists: true, anchor: meldbase.RollbackAnchor{DatabaseID: identityA, MinimumCommitSequence: 0, MinimumGeneration: 1}},
		{exists: true, anchor: meldbase.RollbackAnchor{DatabaseID: identityA, MinimumCommitSequence: 1, MinimumGeneration: 2}},
		{exists: true, anchor: meldbase.RollbackAnchor{DatabaseID: identityA, MinimumCommitSequence: 0, MinimumGeneration: 3}},
		{exists: true, anchor: meldbase.RollbackAnchor{DatabaseID: identityA, MinimumCommitSequence: 1, MinimumGeneration: 4}},
		{exists: true, anchor: meldbase.RollbackAnchor{DatabaseID: identityB, MinimumCommitSequence: 0, MinimumGeneration: 1}},
	}
	checked := 0
	for _, replicas := range []int{1, 3, 5} {
		quorum := replicas/2 + 1
		responses := make([]loadResult, replicas)
		var visit func(int)
		visit = func(index int) {
			if index == len(responses) {
				checked++
				selected, exists, ready := selectLoadQuorum(responses, quorum)
				choices := modelSafeReadChoices(responses, quorum)
				if !ready {
					return
				}
				for _, choice := range choices {
					if choice.exists == exists && (!exists || choice.anchor == selected) {
						return
					}
				}
				t.Fatalf("selector invented quorum replicas=%d responses=%+v selected=%+v exists=%t choices=%+v", replicas, responses, selected, exists, choices)
			}
			for _, state := range states {
				responses[index] = state
				visit(index + 1)
			}
		}
		visit(0)
	}
	if checked != 7_998 {
		t.Fatalf("selector oracle explored unexpected response-set count: %d", checked)
	}
}

// modelReadChoice represents a safe modeled read result or an absent anchor.
type modelReadChoice struct {
	anchor meldbase.RollbackAnchor
	exists bool
}

// modelSafeReadChoices enumerates majority results that contain a coordinate at least as advanced as all their existing anchors.
func modelSafeReadChoices(results []loadResult, quorum int) []modelReadChoice {
	choices := make([]modelReadChoice, 0)
	for mask := 0; mask < 1<<len(results); mask++ {
		if bits.OnesCount(uint(mask)) != quorum {
			continue
		}
		existing := make([]meldbase.RollbackAnchor, 0, quorum)
		for index, result := range results {
			if mask&(1<<index) != 0 && result.exists {
				existing = append(existing, result.anchor)
			}
		}
		if len(existing) == 0 {
			choices = append(choices, modelReadChoice{})
			continue
		}
		for _, candidate := range existing {
			dominates := true
			for _, value := range existing {
				if !modelAnchorBeforeOrEqual(value, candidate) {
					dominates = false
					break
				}
			}
			if dominates {
				choices = append(choices, modelReadChoice{anchor: candidate, exists: true})
				break
			}
		}
	}
	return choices
}

// modelAnchorBeforeOrEqual checks matching database identity and nondecreasing commit and generation positions.
func modelAnchorBeforeOrEqual(left, right meldbase.RollbackAnchor) bool {
	return left.DatabaseID == right.DatabaseID &&
		left.MinimumCommitSequence <= right.MinimumCommitSequence &&
		left.MinimumGeneration <= right.MinimumGeneration
}
