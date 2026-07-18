package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExecutePowerCycleObservesOffBeforeRestoringOn(t *testing.T) {
	const systemURI = "/redfish/v1/Systems/Node-1"
	var mutex sync.Mutex
	powerState := "On"
	actions := make([]string, 0, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		username, password, ok := request.BasicAuth()
		if !ok || username != "operator" || password != "secret" {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		mutex.Lock()
		defer mutex.Unlock()
		switch {
		case request.Method == http.MethodGet && request.URL.Path == systemURI:
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"@odata.id": systemURI, "Id": "Node-1", "UUID": "123e4567-e89b-12d3-a456-426614174000",
				"SerialNumber": "SERIAL-1", "Manufacturer": "Meld Labs", "Model": "Power Test", "PowerState": powerState,
				"Actions": map[string]any{"#ComputerSystem.Reset": map[string]any{
					"target":                            systemURI + "/Actions/ComputerSystem.Reset",
					"ResetType@Redfish.AllowableValues": []string{"ForceOff", "On"},
				}},
			})
		case request.Method == http.MethodPost && request.URL.Path == systemURI+"/Actions/ComputerSystem.Reset":
			var body struct {
				ResetType string `json:"ResetType"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			actions = append(actions, body.ResetType)
			if body.ResetType == "ForceOff" {
				powerState = "Off"
			} else if body.ResetType == "On" {
				powerState = "On"
			} else {
				http.Error(writer, "unsupported", http.StatusBadRequest)
				return
			}
			writer.Header().Set("Location", "/redfish/v1/TaskService/Tasks/1")
			writer.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	base, _ := url.Parse(server.URL)
	evidenceDir := t.TempDir()
	config := redfishConfig{BaseURL: base, SystemURI: systemURI, Username: "operator", Password: "secret", EvidenceDir: evidenceDir, PollInterval: time.Millisecond, RequestTimeout: time.Second, OperationTimeout: time.Second}
	system := computerSystem{ODataID: systemURI, UUID: "123e4567-e89b-12d3-a456-426614174000", SerialNumber: "SERIAL-1", Manufacturer: "Meld Labs", Model: "Power Test"}
	_, targetSHA, err := systemIdentity(config, system)
	if err != nil {
		t.Fatal(err)
	}
	request := validTestRequest(targetSHA)
	response, err := executePowerCycle(context.Background(), server.Client(), config, request)
	if err != nil {
		t.Fatal(err)
	}
	if !response.Success || response.TargetIdentitySHA256 != targetSHA || response.AcceptedAt.Before(request.RequestedAt) || !response.PowerLostAt.After(response.AcceptedAt) || !response.PowerRestoredAt.After(response.PowerLostAt) {
		t.Fatalf("response=%+v", response)
	}
	mutex.Lock()
	gotActions, finalState := append([]string(nil), actions...), powerState
	mutex.Unlock()
	if strings.Join(gotActions, ",") != "ForceOff,On" || finalState != "On" {
		t.Fatalf("actions=%v final=%s", gotActions, finalState)
	}
	evidencePath := filepath.Join(evidenceDir, "redfish-"+request.ControllerRunID+".json")
	raw, err := os.ReadFile(evidencePath)
	if err != nil || digest(raw) != response.HardwareEvidenceSHA256 {
		t.Fatalf("evidence digest error=%v", err)
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(response.HardwareEvidenceBase64)
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("embedded hardware evidence differs from retained log: %v", err)
	}
}

func TestExecutePowerCycleRejectsTargetBeforeForceOff(t *testing.T) {
	const systemURI = "/redfish/v1/Systems/Node-1"
	posts := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost {
			posts++
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"@odata.id": systemURI, "UUID": "uuid", "SerialNumber": "serial", "Manufacturer": "vendor", "Model": "model", "PowerState": "On",
		})
	}))
	defer server.Close()
	base, _ := url.Parse(server.URL)
	config := redfishConfig{BaseURL: base, SystemURI: systemURI, Username: "operator", Password: "secret", EvidenceDir: t.TempDir(), PollInterval: time.Millisecond, RequestTimeout: time.Second, OperationTimeout: time.Second}
	request := validTestRequest(strings.Repeat("f", 64))
	if _, err := executePowerCycle(context.Background(), server.Client(), config, request); err == nil || !strings.Contains(err.Error(), "target identity") {
		t.Fatalf("error=%v", err)
	}
	if posts != 0 {
		t.Fatalf("wrong target received %d reset actions", posts)
	}
	if _, err := os.Stat(filepath.Join(config.EvidenceDir, "redfish-"+request.ControllerRunID+".json")); err != nil {
		t.Fatalf("failed attempt evidence missing: %v", err)
	}
}

func TestExecutePowerCycleRestoresOnAfterPostOffFailure(t *testing.T) {
	const systemURI = "/redfish/v1/Systems/Node-1"
	powerState, onCalls := "On", 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"@odata.id": systemURI, "UUID": "uuid", "SerialNumber": "serial", "Manufacturer": "vendor", "Model": "model", "PowerState": powerState,
				"Actions": map[string]any{"#ComputerSystem.Reset": map[string]any{"target": systemURI + "/Actions/ComputerSystem.Reset", "ResetType@Redfish.AllowableValues": []string{"ForceOff", "On"}}},
			})
		case http.MethodPost:
			var body struct {
				ResetType string `json:"ResetType"`
			}
			_ = json.NewDecoder(request.Body).Decode(&body)
			if body.ResetType == "ForceOff" {
				powerState = "Off"
				writer.WriteHeader(http.StatusNoContent)
				return
			}
			onCalls++
			if onCalls == 1 {
				http.Error(writer, "temporary BMC failure", http.StatusServiceUnavailable)
				return
			}
			powerState = "On"
			writer.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()
	base, _ := url.Parse(server.URL)
	config := redfishConfig{BaseURL: base, SystemURI: systemURI, Username: "operator", Password: "secret", EvidenceDir: t.TempDir(), PollInterval: time.Millisecond, RequestTimeout: time.Second, OperationTimeout: time.Second}
	_, targetSHA, err := systemIdentity(config, computerSystem{ODataID: systemURI, UUID: "uuid", SerialNumber: "serial", Manufacturer: "vendor", Model: "model"})
	if err != nil {
		t.Fatal(err)
	}
	request := validTestRequest(targetSHA)
	if _, err := executePowerCycle(context.Background(), server.Client(), config, request); err == nil || !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("error=%v", err)
	}
	if powerState != "On" || onCalls != 2 {
		t.Fatalf("best-effort restoration state=%s calls=%d", powerState, onCalls)
	}
	if _, err := os.Stat(filepath.Join(config.EvidenceDir, "redfish-"+request.ControllerRunID+".json")); err != nil {
		t.Fatalf("failed-cycle evidence missing: %v", err)
	}
}

func TestExecutePowerCyclePreflightsEvidenceBeforeAnyRequest(t *testing.T) {
	directory := t.TempDir()
	request := validTestRequest(strings.Repeat("f", 64))
	if err := os.WriteFile(filepath.Join(directory, "redfish-"+request.ControllerRunID+".json"), []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	requests := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests++ }))
	defer server.Close()
	base, _ := url.Parse(server.URL)
	config := redfishConfig{BaseURL: base, SystemURI: "/redfish/v1/Systems/Node-1", EvidenceDir: directory, PollInterval: time.Millisecond, RequestTimeout: time.Second, OperationTimeout: time.Second}
	if _, err := executePowerCycle(context.Background(), server.Client(), config, request); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("error=%v", err)
	}
	if requests != 0 {
		t.Fatalf("preflight made %d BMC requests", requests)
	}
}

func TestLoadConfigRequiresTLSAndBoundedDurations(t *testing.T) {
	t.Setenv("MELDBASE_REDFISH_BASE_URL", "http://bmc.example")
	t.Setenv("MELDBASE_REDFISH_SYSTEM_URI", "/redfish/v1/Systems/1")
	t.Setenv("MELDBASE_REDFISH_USERNAME", "operator")
	t.Setenv("MELDBASE_REDFISH_PASSWORD", "secret")
	t.Setenv("MELDBASE_REDFISH_EVIDENCE_DIR", t.TempDir())
	if _, _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("error=%v", err)
	}
	t.Setenv("MELDBASE_REDFISH_BASE_URL", "https://bmc.example")
	t.Setenv("MELDBASE_REDFISH_OPERATION_TIMEOUT", "1s")
	if _, _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "OPERATION_TIMEOUT") {
		t.Fatalf("error=%v", err)
	}
}

func TestSystemURIIsOneCanonicalComputerSystem(t *testing.T) {
	if !validSystemURI("/redfish/v1/Systems/System.Embedded.1") {
		t.Fatal("valid system URI rejected")
	}
	for _, value := range []string{"/redfish/v1/Systems/", "/redfish/v1/Systems/1/Bios", "/redfish/v1/Systems/../Managers/1", "/redfish/v1/Systems/a%2Fb", "https://bmc/redfish/v1/Systems/1"} {
		if validSystemURI(value) {
			t.Fatalf("unsafe system URI %q accepted", value)
		}
	}
}

func TestEvidenceLocationRedactsAbsoluteOrTokenizedValues(t *testing.T) {
	response := &http.Response{Header: make(http.Header)}
	response.Header.Set("Location", "/redfish/v1/TaskService/Tasks/1")
	if got := redactedLocation(response); got != "/redfish/v1/TaskService/Tasks/1" {
		t.Fatalf("location=%q", got)
	}
	response.Header.Set("Location", "https://other.example/task?token=secret")
	if got := redactedLocation(response); !strings.HasPrefix(got, "sha256:") || strings.Contains(got, "secret") {
		t.Fatalf("redacted location=%q", got)
	}
}

func validTestRequest(target string) adapterRequest {
	return adapterRequest{SchemaVersion: 1, ControllerRunID: strings.Repeat("a", 32), TrialID: "power-01-01", Method: "redfish-computer-system-power-cycle", MarkerSHA256: strings.Repeat("b", 64), BootIDBefore: "boot-1", TargetIdentitySHA256: target, RequestedAt: time.Now().UTC().Add(-time.Second)}
}
