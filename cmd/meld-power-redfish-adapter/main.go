package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	protocolSchema = 1
	maximumJSON    = 1 << 20
)

type adapterRequest struct {
	SchemaVersion        uint32    `json:"schemaVersion"`
	ControllerRunID      string    `json:"controllerRunId"`
	TrialID              string    `json:"trialId"`
	Method               string    `json:"method"`
	MarkerSHA256         string    `json:"markerSha256"`
	BootIDBefore         string    `json:"bootIdBefore"`
	TargetIdentitySHA256 string    `json:"targetIdentitySha256"`
	RequestedAt          time.Time `json:"requestedAt"`
}

type adapterResponse struct {
	SchemaVersion          uint32    `json:"schemaVersion"`
	ControllerRunID        string    `json:"controllerRunId"`
	TrialID                string    `json:"trialId"`
	Method                 string    `json:"method"`
	OperationID            string    `json:"operationId"`
	TargetIdentitySHA256   string    `json:"targetIdentitySha256"`
	AcceptedAt             time.Time `json:"acceptedAt"`
	PowerLostAt            time.Time `json:"powerLostAt"`
	PowerRestoredAt        time.Time `json:"powerRestoredAt"`
	HardwareEvidenceSHA256 string    `json:"hardwareEvidenceSha256"`
	HardwareEvidenceBase64 string    `json:"hardwareEvidenceBase64"`
	Success                bool      `json:"success"`
}

type redfishConfig struct {
	BaseURL          *url.URL
	SystemURI        string
	Username         string
	Password         string
	EvidenceDir      string
	PollInterval     time.Duration
	RequestTimeout   time.Duration
	OperationTimeout time.Duration
}

type computerSystem struct {
	ODataID      string `json:"@odata.id"`
	ID           string `json:"Id"`
	UUID         string `json:"UUID"`
	SerialNumber string `json:"SerialNumber"`
	Manufacturer string `json:"Manufacturer"`
	Model        string `json:"Model"`
	PowerState   string `json:"PowerState"`
	Actions      map[string]struct {
		Target          string   `json:"target"`
		AllowableValues []string `json:"ResetType@Redfish.AllowableValues"`
	} `json:"Actions"`
}

type targetIdentity struct {
	SchemaVersion uint32 `json:"schemaVersion"`
	SystemURI     string `json:"systemUri"`
	UUID          string `json:"uuid"`
	SerialNumber  string `json:"serialNumber"`
	Manufacturer  string `json:"manufacturer"`
	Model         string `json:"model"`
}

type evidenceEntry struct {
	ObservedAt time.Time `json:"observedAt"`
	Operation  string    `json:"operation"`
	Path       string    `json:"path"`
	StatusCode int       `json:"statusCode,omitempty"`
	PowerState string    `json:"powerState,omitempty"`
	BodySHA256 string    `json:"bodySha256,omitempty"`
	RequestID  string    `json:"requestId,omitempty"`
	Location   string    `json:"location,omitempty"`
	Error      string    `json:"error,omitempty"`
}

type hardwareEvidence struct {
	SchemaVersion   uint32          `json:"schemaVersion"`
	ControllerRunID string          `json:"controllerRunId"`
	TrialID         string          `json:"trialId"`
	Target          targetIdentity  `json:"target"`
	TargetSHA256    string          `json:"targetSha256"`
	StartedAt       time.Time       `json:"startedAt"`
	FinishedAt      time.Time       `json:"finishedAt"`
	Entries         []evidenceEntry `json:"entries"`
	Successful      bool            `json:"successful"`
}

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "meld-power-redfish-adapter:", err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	config, client, err := loadConfig()
	if err != nil {
		return err
	}
	if len(args) == 1 && args[0] == "--identity" {
		ctx, cancel := context.WithTimeout(context.Background(), config.RequestTimeout)
		defer cancel()
		system, _, err := getSystem(ctx, client, config, nil)
		if err != nil {
			return err
		}
		identity, digest, err := systemIdentity(config, system)
		if err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(struct {
			Identity targetIdentity `json:"identity"`
			SHA256   string         `json:"sha256"`
		}{identity, digest})
	}
	if len(args) != 0 {
		return errors.New("usage: meld-power-redfish-adapter [--identity]")
	}
	var request adapterRequest
	if err := decodeStrict(stdin, &request); err != nil {
		return fmt.Errorf("adapter request: %w", err)
	}
	if err := validateRequest(request); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), config.OperationTimeout)
	defer cancel()
	response, err := executePowerCycle(ctx, client, config, request)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(response)
}

func loadConfig() (redfishConfig, *http.Client, error) {
	rawBase := os.Getenv("MELDBASE_REDFISH_BASE_URL")
	base, err := url.Parse(rawBase)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || base.Path != "" && base.Path != "/" {
		return redfishConfig{}, nil, errors.New("MELDBASE_REDFISH_BASE_URL must be an HTTPS origin without credentials, path, query or fragment")
	}
	systemURI := os.Getenv("MELDBASE_REDFISH_SYSTEM_URI")
	if !validSystemURI(systemURI) {
		return redfishConfig{}, nil, errors.New("MELDBASE_REDFISH_SYSTEM_URI must be one canonical /redfish/v1/Systems/<id> path")
	}
	username, password := os.Getenv("MELDBASE_REDFISH_USERNAME"), os.Getenv("MELDBASE_REDFISH_PASSWORD")
	if username == "" || password == "" || strings.ContainsAny(username+password, "\r\n") {
		return redfishConfig{}, nil, errors.New("Redfish username and password must be nonempty environment secrets without newlines")
	}
	evidenceDir, err := existingDirectory(os.Getenv("MELDBASE_REDFISH_EVIDENCE_DIR"))
	if err != nil {
		return redfishConfig{}, nil, fmt.Errorf("Redfish evidence directory: %w", err)
	}
	poll, err := durationEnv("MELDBASE_REDFISH_POLL_INTERVAL", 2*time.Second, 10*time.Millisecond, 30*time.Second)
	if err != nil {
		return redfishConfig{}, nil, err
	}
	requestTimeout, err := durationEnv("MELDBASE_REDFISH_REQUEST_TIMEOUT", 15*time.Second, time.Second, time.Minute)
	if err != nil {
		return redfishConfig{}, nil, err
	}
	operationTimeout, err := durationEnv("MELDBASE_REDFISH_OPERATION_TIMEOUT", 10*time.Minute, 30*time.Second, 30*time.Minute)
	if err != nil {
		return redfishConfig{}, nil, err
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return redfishConfig{}, nil, err
	}
	if caPath := os.Getenv("MELDBASE_REDFISH_CA_FILE"); caPath != "" {
		pem, err := os.ReadFile(caPath)
		if err != nil || !roots.AppendCertsFromPEM(pem) {
			return redfishConfig{}, nil, errors.New("MELDBASE_REDFISH_CA_FILE does not contain a trusted CA certificate")
		}
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, Proxy: http.ProxyFromEnvironment}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("Redfish redirects are disabled") }}
	return redfishConfig{base, systemURI, username, password, evidenceDir, poll, requestTimeout, operationTimeout}, client, nil
}

func executePowerCycle(ctx context.Context, client *http.Client, config redfishConfig, request adapterRequest) (response adapterResponse, resultErr error) {
	evidence := hardwareEvidence{SchemaVersion: 1, ControllerRunID: request.ControllerRunID, TrialID: request.TrialID, StartedAt: time.Now().UTC()}
	evidenceOutput := filepath.Join(config.EvidenceDir, "redfish-"+request.ControllerRunID+".json")
	if _, err := os.Lstat(evidenceOutput); err == nil {
		return response, errors.New("Redfish hardware evidence output already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return response, err
	}
	defer func() {
		if resultErr != nil {
			evidence.FinishedAt = time.Now().UTC()
			_, _, _ = writeEvidence(config.EvidenceDir, request.ControllerRunID, evidence)
		}
	}()
	system, _, err := getSystem(ctx, client, config, &evidence)
	if err != nil {
		return response, err
	}
	identity, targetSHA, err := systemIdentity(config, system)
	if err != nil || targetSHA != request.TargetIdentitySHA256 {
		return response, errors.New("Redfish target identity differs from the pre-approved request target")
	}
	evidence.Target, evidence.TargetSHA256 = identity, targetSHA
	if system.PowerState != "On" {
		return response, fmt.Errorf("Redfish target must initially be On, got %q", system.PowerState)
	}
	action, ok := system.Actions["#ComputerSystem.Reset"]
	if !ok || action.Target != config.SystemURI+"/Actions/ComputerSystem.Reset" || !validActionURI(action.Target) || !contains(action.AllowableValues, "ForceOff") {
		return response, errors.New("Redfish ComputerSystem does not advertise a safe ForceOff reset action")
	}
	onType := "On"
	if !contains(action.AllowableValues, onType) {
		onType = "ForceOn"
	}
	if !contains(action.AllowableValues, onType) {
		return response, errors.New("Redfish ComputerSystem does not advertise On or ForceOn restoration")
	}
	offIssued, restored := false, false
	defer func() {
		if offIssued && !restored {
			restoreCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			_, _, _ = postReset(restoreCtx, client, config, action.Target, onType, &evidence)
			_, _ = waitPowerState(restoreCtx, client, config, "On", &evidence)
		}
	}()
	offIssued = true
	acceptedAt, location, err := postReset(ctx, client, config, action.Target, "ForceOff", &evidence)
	if err != nil {
		return response, err
	}
	powerLostAt, err := waitPowerState(ctx, client, config, "Off", &evidence)
	if err != nil {
		return response, err
	}
	if _, _, err := postReset(ctx, client, config, action.Target, onType, &evidence); err != nil {
		return response, err
	}
	powerRestoredAt, err := waitPowerState(ctx, client, config, "On", &evidence)
	if err != nil {
		return response, err
	}
	restored, evidence.Successful, evidence.FinishedAt = true, true, time.Now().UTC()
	evidenceSHA, evidenceRaw, err := writeEvidence(config.EvidenceDir, request.ControllerRunID, evidence)
	if err != nil {
		return response, err
	}
	operationSeed := location
	if operationSeed == "" {
		operationSeed = request.ControllerRunID
	}
	operationHash := sha256.Sum256([]byte(operationSeed))
	return adapterResponse{protocolSchema, request.ControllerRunID, request.TrialID, request.Method, "redfish-" + hex.EncodeToString(operationHash[:8]), targetSHA, acceptedAt, powerLostAt, powerRestoredAt, evidenceSHA, base64.StdEncoding.EncodeToString(evidenceRaw), true}, nil
}

func getSystem(ctx context.Context, client *http.Client, config redfishConfig, evidence *hardwareEvidence) (computerSystem, []byte, error) {
	var system computerSystem
	response, raw, err := requestJSON(ctx, client, config, http.MethodGet, config.SystemURI, nil)
	entry := evidenceEntry{ObservedAt: time.Now().UTC(), Operation: "observe-system", Path: config.SystemURI}
	if err != nil {
		entry.Error = err.Error()
		if evidence != nil {
			if appendErr := appendEvidence(evidence, entry); appendErr != nil {
				return system, nil, appendErr
			}
		}
		return system, nil, err
	}
	entry.StatusCode, entry.BodySHA256, entry.RequestID, entry.Location = response.StatusCode, digest(raw), boundedHeader(response, "X-Request-ID"), redactedLocation(response)
	if response.StatusCode != http.StatusOK {
		entry.Error = "unexpected HTTP status"
		if evidence != nil {
			if appendErr := appendEvidence(evidence, entry); appendErr != nil {
				return system, raw, appendErr
			}
		}
		return system, raw, fmt.Errorf("Redfish system GET returned HTTP %d", response.StatusCode)
	}
	if err := decodeJSONBytes(raw, &system); err != nil {
		return system, raw, err
	}
	entry.PowerState = system.PowerState
	if evidence != nil {
		if appendErr := appendEvidence(evidence, entry); appendErr != nil {
			return system, raw, appendErr
		}
	}
	return system, raw, nil
}

func postReset(ctx context.Context, client *http.Client, config redfishConfig, path, resetType string, evidence *hardwareEvidence) (time.Time, string, error) {
	body, _ := json.Marshal(struct {
		ResetType string `json:"ResetType"`
	}{resetType})
	response, raw, err := requestJSON(ctx, client, config, http.MethodPost, path, body)
	entry := evidenceEntry{ObservedAt: time.Now().UTC(), Operation: "reset-" + resetType, Path: path}
	if err != nil {
		entry.Error = err.Error()
		if appendErr := appendEvidence(evidence, entry); appendErr != nil {
			return time.Time{}, "", appendErr
		}
		return time.Time{}, "", err
	}
	entry.StatusCode, entry.BodySHA256, entry.RequestID, entry.Location = response.StatusCode, digest(raw), boundedHeader(response, "X-Request-ID"), redactedLocation(response)
	if appendErr := appendEvidence(evidence, entry); appendErr != nil {
		return time.Time{}, "", appendErr
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusNoContent {
		return time.Time{}, "", fmt.Errorf("Redfish %s returned HTTP %d", resetType, response.StatusCode)
	}
	return entry.ObservedAt, entry.Location, nil
}

func waitPowerState(ctx context.Context, client *http.Client, config redfishConfig, want string, evidence *hardwareEvidence) (time.Time, error) {
	for {
		system, _, err := getSystem(ctx, client, config, evidence)
		if err == nil && system.PowerState == want {
			return evidence.Entries[len(evidence.Entries)-1].ObservedAt, nil
		}
		select {
		case <-ctx.Done():
			return time.Time{}, fmt.Errorf("timed out waiting for Redfish PowerState %s: %w", want, ctx.Err())
		case <-time.After(config.PollInterval):
		}
	}
}

func requestJSON(ctx context.Context, client *http.Client, config redfishConfig, method, path string, body []byte) (*http.Response, []byte, error) {
	requestCtx, cancel := context.WithTimeout(ctx, config.RequestTimeout)
	defer cancel()
	endpoint := config.BaseURL.ResolveReference(&url.URL{Path: path})
	request, err := http.NewRequestWithContext(requestCtx, method, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	request.SetBasicAuth(config.Username, config.Password)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("OData-Version", "4.0")
	request.Header.Set("User-Agent", "meldbase-redfish-power-adapter/1")
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, maximumJSON+1))
	if err != nil {
		return response, nil, err
	}
	if len(raw) > maximumJSON {
		return response, nil, errors.New("Redfish response exceeds 1 MiB")
	}
	return response, raw, nil
}

func systemIdentity(config redfishConfig, system computerSystem) (targetIdentity, string, error) {
	identity := targetIdentity{1, system.ODataID, system.UUID, system.SerialNumber, system.Manufacturer, system.Model}
	if identity.SystemURI != config.SystemURI || identity.UUID == "" || identity.SerialNumber == "" || identity.Manufacturer == "" || identity.Model == "" {
		return identity, "", errors.New("Redfish system lacks stable URI, UUID, serial, manufacturer or model identity")
	}
	raw, err := json.Marshal(identity)
	if err != nil {
		return identity, "", err
	}
	return identity, digest(raw), nil
}

func writeEvidence(directory, runID string, evidence hardwareEvidence) (string, []byte, error) {
	path := filepath.Join(directory, "redfish-"+runID+".json")
	raw, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return "", nil, err
	}
	raw = append(raw, '\n')
	if len(raw) > 512<<10 {
		return "", nil, errors.New("redacted Redfish hardware evidence exceeds 512 KiB")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", nil, err
	}
	written, writeErr := file.Write(raw)
	if writeErr == nil && written != len(raw) {
		writeErr = io.ErrShortWrite
	}
	writeErr = errors.Join(writeErr, file.Sync(), file.Close())
	dir, openErr := os.Open(directory)
	if openErr == nil {
		writeErr = errors.Join(writeErr, dir.Sync(), dir.Close())
	}
	if writeErr != nil {
		return "", nil, writeErr
	}
	return digest(raw), raw, nil
}

func appendEvidence(evidence *hardwareEvidence, entry evidenceEntry) error {
	if len(evidence.Entries) != 0 && entry.Operation == "observe-system" {
		previous := evidence.Entries[len(evidence.Entries)-1]
		if previous.Operation == entry.Operation && previous.Path == entry.Path && previous.StatusCode == entry.StatusCode && previous.PowerState == entry.PowerState && previous.Error == entry.Error {
			return nil
		}
	}
	if len(evidence.Entries) >= 512 {
		return errors.New("Redfish hardware evidence exceeded 512 distinct observations")
	}
	evidence.Entries = append(evidence.Entries, entry)
	return nil
}

func decodeStrict(reader io.Reader, output any) error {
	decoder := json.NewDecoder(io.LimitReader(reader, maximumJSON+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("expected exactly one JSON request")
	}
	return nil
}

func decodeJSONBytes(raw []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("Redfish response contains extra JSON")
	}
	return nil
}

func validateRequest(request adapterRequest) error {
	if request.SchemaVersion != protocolSchema || !hexString(request.ControllerRunID, 32) || request.TrialID == "" || request.Method != "redfish-computer-system-power-cycle" || !hexString(request.MarkerSHA256, 64) || request.BootIDBefore == "" || !hexString(request.TargetIdentitySHA256, 64) || request.RequestedAt.IsZero() {
		return errors.New("Redfish adapter request is incomplete or unsupported")
	}
	return nil
}

func validSystemURI(value string) bool {
	parsed, err := url.Parse(value)
	id := strings.TrimPrefix(value, "/redfish/v1/Systems/")
	return err == nil && !parsed.IsAbs() && parsed.RawQuery == "" && parsed.Fragment == "" && parsed.RawPath == "" && parsed.Path == value && strings.HasPrefix(value, "/redfish/v1/Systems/") && safeRedfishID(id)
}

func safeRedfishID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("._~-", character) {
			continue
		}
		return false
	}
	return true
}

func validActionURI(value string) bool {
	return strings.HasPrefix(value, "/redfish/v1/Systems/") && strings.HasSuffix(value, "/Actions/ComputerSystem.Reset") && !strings.Contains(value, "..")
}
func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
func digest(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func hexString(value string, length int) bool {
	if len(value) != length {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}
func boundedHeader(response *http.Response, name string) string {
	value := response.Header.Get(name)
	if len(value) > 256 || strings.ContainsAny(value, "\r\n") {
		return digest([]byte(value))
	}
	return value
}

func redactedLocation(response *http.Response) string {
	value := response.Header.Get("Location")
	parsed, err := url.Parse(value)
	if value == "" {
		return ""
	}
	if err == nil && !parsed.IsAbs() && strings.HasPrefix(parsed.Path, "/redfish/") && parsed.RawQuery == "" && parsed.Fragment == "" && parsed.RawPath == "" && len(value) <= 256 {
		return value
	}
	return "sha256:" + digest([]byte(value))
}

func existingDirectory(path string) (string, error) {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(absolute)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", errors.New("path must be an existing non-symlink directory")
	}
	return absolute, nil
}

func durationEnv(name string, fallback, minimum, maximum time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be between %s and %s", name, minimum, maximum)
	}
	return value, nil
}
