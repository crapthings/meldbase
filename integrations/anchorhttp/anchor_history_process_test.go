package anchorhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crapthings/meldbase/core"
	"github.com/crapthings/meldbase/internal/qualification"
)

const anchorHistoryHelperEnvironment = "MELDBASE_ANCHOR_HISTORY_HELPER"

type anchorHistoryProcessResult struct {
	Outcome string `json:"outcome"`
	Error   string `json:"error,omitempty"`
}

func TestAnchorHistoryProcessHelper(t *testing.T) {
	if os.Getenv(anchorHistoryHelperEnvironment) != "1" {
		return
	}
	readyPath := os.Getenv("MELDBASE_ANCHOR_HISTORY_READY")
	gatePath := os.Getenv("MELDBASE_ANCHOR_HISTORY_GATE")
	resultPath := os.Getenv("MELDBASE_ANCHOR_HISTORY_RESULT")
	endpoints := strings.Split(os.Getenv("MELDBASE_ANCHOR_HISTORY_ENDPOINTS"), ",")
	members := strings.Split(os.Getenv("MELDBASE_ANCHOR_HISTORY_MEMBERS"), ",")
	sequence, sequenceErr := strconv.ParseUint(os.Getenv("MELDBASE_ANCHOR_HISTORY_SEQUENCE"), 10, 64)
	generation, generationErr := strconv.ParseUint(os.Getenv("MELDBASE_ANCHOR_HISTORY_GENERATION"), 10, 64)
	if readyPath == "" || gatePath == "" || resultPath == "" || len(endpoints) != len(members) || len(endpoints) != 3 || sequenceErr != nil || generationErr != nil {
		t.Fatal("invalid anchor history helper environment")
	}
	replicas := make([]Replica, len(endpoints))
	for index := range endpoints {
		replicas[index] = Replica{Endpoint: endpoints[index], MemberID: members[index]}
	}
	store, err := NewQuorumStore(QuorumOptions{
		ClusterID: testClusterID, Replicas: replicas, AnchorName: os.Getenv("MELDBASE_ANCHOR_HISTORY_NAME"),
		KeyID: testKeyID, SharedKey: testSharedKey, AllowInsecureHTTP: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readyPath, []byte("ready\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(gatePath); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for anchor history gate")
		}
		time.Sleep(time.Millisecond)
	}
	target := testAnchor(sequence, generation)
	advanceErr := store.Advance(context.Background(), target)
	result := anchorHistoryProcessResult{Outcome: "success"}
	if errors.Is(advanceErr, ErrConflict) {
		result.Outcome = "conflict"
	} else if advanceErr != nil {
		result.Outcome = "error"
		result.Error = advanceErr.Error()
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(resultPath, append(raw, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCrossedWritersProduceLinearizableMultiProcessHistory(t *testing.T) {
	nodes := newTestNodes(t, 3)
	for _, node := range nodes {
		defer node.close()
	}
	for iteration := 0; iteration < 10; iteration++ {
		directory := t.TempDir()
		gatePath := filepath.Join(directory, "go")
		name := fmt.Sprintf("process-history-%d", iteration)
		targets := [2]meldbase.RollbackAnchor{testAnchor(1, 2), testAnchor(0, 3)}
		var events atomic.Uint64
		commands := make([]*exec.Cmd, len(targets))
		outputs := make([]bytes.Buffer, len(targets))
		operations := make([]qualification.AnchorHistoryOperation, len(targets))
		resultPaths := make([]string, len(targets))
		readyPaths := make([]string, len(targets))
		endpoints := make([]string, len(nodes))
		members := make([]string, len(nodes))
		for index, node := range nodes {
			endpoints[index] = node.server.URL
			members[index] = node.memberID
		}
		for writer, target := range targets {
			readyPaths[writer] = filepath.Join(directory, fmt.Sprintf("ready-%d", writer))
			resultPaths[writer] = filepath.Join(directory, fmt.Sprintf("result-%d.json", writer))
			commands[writer] = exec.Command(os.Args[0], "-test.run=^TestAnchorHistoryProcessHelper$")
			commands[writer].Env = append(os.Environ(),
				anchorHistoryHelperEnvironment+"=1",
				"MELDBASE_ANCHOR_HISTORY_READY="+readyPaths[writer],
				"MELDBASE_ANCHOR_HISTORY_GATE="+gatePath,
				"MELDBASE_ANCHOR_HISTORY_RESULT="+resultPaths[writer],
				"MELDBASE_ANCHOR_HISTORY_ENDPOINTS="+strings.Join(endpoints, ","),
				"MELDBASE_ANCHOR_HISTORY_MEMBERS="+strings.Join(members, ","),
				"MELDBASE_ANCHOR_HISTORY_NAME="+name,
				"MELDBASE_ANCHOR_HISTORY_SEQUENCE="+strconv.FormatUint(target.MinimumCommitSequence, 10),
				"MELDBASE_ANCHOR_HISTORY_GENERATION="+strconv.FormatUint(target.MinimumGeneration, 10),
			)
			commands[writer].Stdout = &outputs[writer]
			commands[writer].Stderr = &outputs[writer]
			operations[writer] = qualification.AnchorHistoryOperation{
				ID: fmt.Sprintf("process-advance-%d", writer), Kind: qualification.AnchorHistoryAdvance,
				Invoke: events.Add(1), Value: qualificationAnchorValue(target),
			}
			if err := commands[writer].Start(); err != nil {
				t.Fatal(err)
			}
		}
		waitForHistoryHelpers(t, readyPaths)
		if err := os.WriteFile(gatePath, []byte("go\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		waitResults := make(chan struct {
			writer int
			err    error
		}, len(commands))
		for writer, command := range commands {
			go func(writer int, command *exec.Cmd) {
				waitResults <- struct {
					writer int
					err    error
				}{writer: writer, err: command.Wait()}
			}(writer, command)
		}
		outcomes := make([]anchorHistoryProcessResult, len(commands))
		for range commands {
			waited := <-waitResults
			operations[waited.writer].Return = events.Add(1)
			if waited.err != nil {
				t.Fatalf("helper %d failed: %v output=%s", waited.writer, waited.err, outputs[waited.writer].String())
			}
			raw, err := os.ReadFile(resultPaths[waited.writer])
			decodeErr := json.Unmarshal(raw, &outcomes[waited.writer])
			if err != nil || decodeErr != nil {
				t.Fatalf("helper %d result=%q readErr=%v decodeErr=%v", waited.writer, raw, err, decodeErr)
			}
			if outcomes[waited.writer].Outcome == "success" {
				operations[waited.writer].Outcome = qualification.AnchorHistorySucceeded
			} else if outcomes[waited.writer].Outcome == "conflict" {
				operations[waited.writer].Outcome = qualification.AnchorHistoryFailed
			} else {
				t.Fatalf("helper %d outcome=%+v", waited.writer, outcomes[waited.writer])
			}
		}
		if (outcomes[0].Outcome == "success") == (outcomes[1].Outcome == "success") {
			t.Fatalf("iteration %d outcomes=%+v", iteration, outcomes)
		}
		store := newTestQuorumNamed(t, nodes, name)
		loadInvoke := events.Add(1)
		retained, exists, err := store.Load(context.Background())
		loadReturn := events.Add(1)
		if err != nil || !exists {
			t.Fatalf("iteration %d final load retained=%+v exists=%t err=%v", iteration, retained, exists, err)
		}
		operations = append(operations, qualification.AnchorHistoryOperation{
			ID: "controller-final-load", Kind: qualification.AnchorHistoryLoad, Outcome: qualification.AnchorHistorySucceeded,
			Invoke: loadInvoke, Return: loadReturn, Value: qualificationAnchorValue(retained),
		})
		check, err := qualification.CheckAnchorHistory(qualification.AnchorHistory{Operations: operations})
		if err != nil || !check.Linearizable {
			t.Fatalf("iteration %d history=%+v check=%+v err=%v", iteration, operations, check, err)
		}
	}
}

func waitForHistoryHelpers(t *testing.T, paths []string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ready := 0
		for _, path := range paths {
			if _, err := os.Stat(path); err == nil {
				ready++
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
		if ready == len(paths) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for helper readiness: %v", paths)
		}
		time.Sleep(time.Millisecond)
	}
}
