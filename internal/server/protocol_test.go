package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"testing"

	"github.com/crapthings/meldbase/core"
)

type protocolV1Contract struct {
	FormatVersion        int    `json:"formatVersion"`
	ProtocolVersion      int    `json:"protocolVersion"`
	RealtimeTicketAccept string `json:"realtimeTicketAccept"`
	RealtimeCapabilities struct {
		Base        []string `json:"base"`
		Conditional []string `json:"conditional"`
	} `json:"realtimeCapabilities"`
	WorkerProtocol struct {
		CapabilityHeader string                  `json:"capabilityHeader"`
		Version          int                     `json:"version"`
		Capabilities     []string                `json:"capabilities"`
		NestedShapes     []protocolShapeContract `json:"nestedShapes"`
		HubFrames        []protocolFrameContract `json:"hubFrames"`
		WorkerFrames     []protocolFrameContract `json:"workerFrames"`
	} `json:"workerProtocol"`
	FixedErrorCodes []string                `json:"fixedErrorCodes"`
	ClientFrames    []protocolFrameContract `json:"clientFrames"`
	ServerFrames    []protocolFrameContract `json:"serverFrames"`
	NestedShapes    []protocolShapeContract `json:"nestedShapes"`
}

type protocolFrameContract struct {
	Type     string   `json:"type"`
	Required []string `json:"required"`
	Optional []string `json:"optional"`
}

type protocolShapeContract struct {
	Name     string   `json:"name"`
	Required []string `json:"required"`
	Optional []string `json:"optional"`
}

func TestProtocolDescriptorsAreCanonicalAndConfigurationHonest(t *testing.T) {
	contract := loadProtocolV1Contract(t)
	base := realtimeProtocolDescriptor(Config{})
	if !reflect.DeepEqual(base.Versions, []int{contract.ProtocolVersion}) || !reflect.DeepEqual(base.Capabilities, contract.RealtimeCapabilities.Base) {
		t.Fatalf("base realtime descriptor=%+v", base)
	}
	configured := realtimeProtocolDescriptor(Config{
		RPCIdempotencyStore: newMemoryIdempotencyStore(),
		RPCTransactionalMethods: map[string]RPCTransactionalMethod{
			"orders.create": func(context.Context, Actor, meldbase.Value, *meldbase.WriteTransaction) (meldbase.Value, error) {
				return meldbase.Null(), nil
			},
		},
	})
	wantConfigured := append(append([]string(nil), contract.RealtimeCapabilities.Base...), contract.RealtimeCapabilities.Conditional...)
	if !reflect.DeepEqual(configured.Capabilities, wantConfigured) {
		t.Fatalf("configured realtime descriptor=%+v", configured)
	}
	for name, descriptor := range map[string]protocolDescriptor{"base": base, "configured": configured} {
		for index := 1; index < len(descriptor.Capabilities); index++ {
			if descriptor.Capabilities[index-1] >= descriptor.Capabilities[index] {
				t.Fatalf("%s descriptor is not canonical: %+v", name, descriptor)
			}
		}
	}
}

func TestRealtimeProtocolV1FrameVocabularyMatchesSharedContract(t *testing.T) {
	contract := loadProtocolV1Contract(t)
	wantClient := []protocolFrameContract{
		{Type: "authenticate", Required: []string{"ticket", "type", "v"}},
		{Type: "call", Required: []string{"input", "method", "requestId", "type", "v"}, Optional: []string{"idempotencyKey"}},
		{Type: "cancel", Required: []string{"requestId", "type", "v"}},
		{Type: "ping", Required: []string{"type", "v"}},
		{Type: "subscribe", Required: []string{"collection", "query", "requestId", "type", "v"}, Optional: []string{"mode", "resumeToken"}},
		{Type: "unsubscribe", Required: []string{"subscriptionId", "type", "v"}},
	}
	wantServer := []protocolFrameContract{
		{Type: "authenticated", Required: []string{"type", "v"}},
		{Type: "delta", Required: []string{"fromToken", "operations", "requestId", "subscriptionId", "token", "type", "v"}},
		{Type: "error", Required: []string{"error", "requestId", "type", "v"}},
		{Type: "pong", Required: []string{"type", "v"}},
		{Type: "result", Required: []string{"requestId", "result", "type", "v"}},
		{Type: "resumed", Required: []string{"requestId", "subscriptionId", "token", "type", "v"}},
		{Type: "resync_required", Required: []string{"requestId", "type", "v"}},
		{Type: "snapshot", Required: []string{"documents", "requestId", "subscriptionId", "token", "type", "v"}},
	}
	wantNested := []protocolShapeContract{
		{Name: "delta.operation", Required: []string{"id", "op"}, Optional: []string{"before", "document"}},
		{Name: "error", Required: []string{"code", "kind"}, Optional: []string{"data"}},
	}
	if !equalProtocolFrames(contract.ClientFrames, wantClient) || !equalProtocolFrames(contract.ServerFrames, wantServer) || !equalProtocolShapes(contract.NestedShapes, wantNested) {
		t.Fatalf("realtime protocol v1 frame contract drifted: client=%+v server=%+v nested=%+v", contract.ClientFrames, contract.ServerFrames, contract.NestedShapes)
	}
}

func TestWorkerProtocolV1ContractIsCanonical(t *testing.T) {
	contract := loadProtocolV1Contract(t)
	workerContract := contract.WorkerProtocol
	if workerContract.Version != ProtocolVersion || workerContract.CapabilityHeader != workerCapabilityRequestValue {
		t.Fatalf("invalid worker protocol v1 contract metadata: %+v", contract)
	}
	worker := workerProtocolDescriptor()
	if !reflect.DeepEqual(worker.Versions, []int{ProtocolVersion}) || !reflect.DeepEqual(worker.Capabilities, workerContract.Capabilities) {
		t.Fatalf("worker descriptor=%+v", worker)
	}
	wantHub := []protocolFrameContract{
		{Type: "authorize_query", Required: []string{"actor", "callId", "collection", "query", "type", "v"}},
		{Type: "cancel", Required: []string{"callId", "type", "v"}},
		{Type: "invoke", Required: []string{"actor", "callId", "input", "method", "mode", "type", "v"}},
		{Type: "registered", Required: []string{"limits", "protocol", "sessionId", "type", "v"}},
		{Type: "tx_error", Required: []string{"callId", "error", "opId", "type", "v"}},
		{Type: "tx_result", Required: []string{"callId", "opId", "result", "type", "v"}},
	}
	wantWorker := []protocolFrameContract{
		{Type: "error", Required: []string{"callId", "error", "type", "v"}},
		{Type: "policy", Required: []string{"callId", "constraint", "type", "v"}},
		{Type: "policy_error", Required: []string{"callId", "error", "type", "v"}},
		{Type: "register", Required: []string{"methods", "readPolicies", "type", "v", "workerId"}},
		{Type: "result", Required: []string{"callId", "result", "type", "v"}},
		{Type: "tx_op", Required: []string{"callId", "collection", "opId", "operation", "type", "v"}, Optional: []string{"document", "id", "mutation"}},
	}
	wantNested := []protocolShapeContract{
		{Name: "actor", Required: []string{"id", "workspaceId"}},
		{Name: "error", Required: []string{"code", "kind"}, Optional: []string{"data"}},
	}
	if !equalProtocolFrames(workerContract.HubFrames, wantHub) || !equalProtocolFrames(workerContract.WorkerFrames, wantWorker) || !equalProtocolShapes(workerContract.NestedShapes, wantNested) {
		t.Fatalf("worker protocol v1 frame contract drifted: hub=%+v worker=%+v", workerContract.HubFrames, workerContract.WorkerFrames)
	}
	assertCanonicalStrings(t, "worker capabilities", workerContract.Capabilities)
	assertCanonicalFrames(t, "worker hub", workerContract.HubFrames)
	assertCanonicalFrames(t, "worker client", workerContract.WorkerFrames)
	for _, shape := range workerContract.NestedShapes {
		assertCanonicalStrings(t, "worker "+shape.Name+" required fields", shape.Required)
		assertCanonicalStrings(t, "worker "+shape.Name+" optional fields", shape.Optional)
	}
}

func equalProtocolFrames(left, right []protocolFrameContract) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Type != right[index].Type || !slices.Equal(left[index].Required, right[index].Required) || !slices.Equal(left[index].Optional, right[index].Optional) {
			return false
		}
	}
	return true
}

func equalProtocolShapes(left, right []protocolShapeContract) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Name != right[index].Name || !slices.Equal(left[index].Required, right[index].Required) || !slices.Equal(left[index].Optional, right[index].Optional) {
			return false
		}
	}
	return true
}

func loadProtocolV1Contract(t *testing.T) protocolV1Contract {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "protocol-v1-contract.json"))
	if err != nil {
		t.Fatal(err)
	}
	var contract protocolV1Contract
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatal(err)
	}
	if contract.FormatVersion != 1 || contract.ProtocolVersion != ProtocolVersion ||
		contract.RealtimeTicketAccept != realtimeTicketCapabilityMediaType+"; capabilities=1" {
		t.Fatalf("invalid protocol contract metadata: %+v", contract)
	}
	assertCanonicalStrings(t, "realtime base capabilities", contract.RealtimeCapabilities.Base)
	assertCanonicalStrings(t, "realtime conditional capabilities", contract.RealtimeCapabilities.Conditional)
	assertCanonicalStrings(t, "fixed error codes", contract.FixedErrorCodes)
	if !reflect.DeepEqual(contract.FixedErrorCodes, fixedProtocolErrorCodes) {
		t.Fatalf("fixed error code registry drifted: contract=%v implementation=%v", contract.FixedErrorCodes, fixedProtocolErrorCodes)
	}
	assertCanonicalFrames(t, "client", contract.ClientFrames)
	assertCanonicalFrames(t, "server", contract.ServerFrames)
	return contract
}

func assertCanonicalFrames(t *testing.T, name string, frames []protocolFrameContract) {
	t.Helper()
	frameTypes := make([]string, len(frames))
	for index, frame := range frames {
		frameTypes[index] = frame.Type
		assertCanonicalStrings(t, name+" "+frame.Type+" required fields", frame.Required)
		assertCanonicalStrings(t, name+" "+frame.Type+" optional fields", frame.Optional)
		for _, optional := range frame.Optional {
			if sort.SearchStrings(frame.Required, optional) < len(frame.Required) && frame.Required[sort.SearchStrings(frame.Required, optional)] == optional {
				t.Fatalf("%s frame %q field %q is both required and optional", name, frame.Type, optional)
			}
		}
	}
	assertCanonicalStrings(t, name+" frame types", frameTypes)
}

func assertCanonicalStrings(t *testing.T, name string, values []string) {
	t.Helper()
	for index, value := range values {
		if value == "" || (index > 0 && values[index-1] >= value) {
			t.Fatalf("%s are not non-empty, sorted and unique: %v", name, values)
		}
	}
}
