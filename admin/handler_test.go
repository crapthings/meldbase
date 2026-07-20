package admin

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/crapthings/meldbase"
)

const testAdminToken = "0123456789abcdef0123456789abcdef"

func newTestHandler(t *testing.T) (*Sampler, *Handler) {
	t.Helper()
	now := time.Now()
	source := &fakeSource{stats: meldbase.DBStats{StartedAt: now, CapturedAt: now}}
	sampler := newTestSampler(t, source, 4, 4)
	authorizer, err := NewBearerTokenAuthorizer(testAdminToken)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(HandlerOptions{
		Sampler: sampler, Authorize: authorizer,
		AllowedOrigins: []string{"https://admin.example"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return sampler, handler
}

func TestHandlerRequiresAuthenticationAndExactOrigin(t *testing.T) {
	_, handler := newTestHandler(t)

	request := httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || response.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("unauthenticated status=%d headers=%v", response.Code, response.Header())
	}
	if response.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("security headers=%v", response.Header())
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	request.Header.Set("Authorization", "Bearer "+testAdminToken)
	request.Header.Set("Origin", "https://evil.example")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("bad origin status=%d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	request.Header.Set("Authorization", "Bearer "+testAdminToken)
	request.Header.Set("Origin", "https://admin.example")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Access-Control-Allow-Origin") != "https://admin.example" {
		t.Fatalf("authorized status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	var wire map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &wire); err != nil {
		t.Fatal(err)
	}
	stats, ok := wire["stats"].(map[string]any)
	if !ok || stats["capturedAt"] == nil || stats["commitSequence"] == nil || stats["CapturedAt"] != nil {
		t.Fatalf("unstable stats JSON schema=%v", wire["stats"])
	}
	var sample Sample
	if err := json.Unmarshal(response.Body.Bytes(), &sample); err != nil || sample.Version != SchemaVersion || sample.Sequence != 1 {
		t.Fatalf("sample=%+v err=%v", sample, err)
	}
}

func TestHandlerAllowsDirectSameOriginDashboardRequests(t *testing.T) {
	_, handler := newTestHandler(t)
	request := httptest.NewRequest(http.MethodGet, "http://admin.local/v1/stats", nil)
	request.Host = "admin.local"
	request.Header.Set("Origin", "http://admin.local")
	request.Header.Set("Authorization", "Bearer "+testAdminToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("same-origin request status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestHandlerPreflightAndHistory(t *testing.T) {
	_, handler := newTestHandler(t)
	request := httptest.NewRequest(http.MethodOptions, "/v1/stats/stream", nil)
	request.Header.Set("Origin", "https://admin.example")
	request.Header.Set("Access-Control-Request-Method", http.MethodGet)
	request.Header.Set("Access-Control-Request-Headers", "authorization")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || response.Header().Get("Access-Control-Allow-Headers") != "Authorization" {
		t.Fatalf("preflight status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/stats/history", nil)
	request.Header.Set("Authorization", "Bearer "+testAdminToken)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("history status=%d body=%s", response.Code, response.Body.String())
	}
	var history struct {
		Version uint32   `json:"version"`
		Samples []Sample `json:"samples"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &history); err != nil || history.Version != SchemaVersion || len(history.Samples) != 1 {
		t.Fatalf("history=%+v err=%v", history, err)
	}
}

func TestHandlerStreamsInitialSample(t *testing.T) {
	_, handler := newTestHandler(t)
	server := httptest.NewServer(handler)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/v1/stats/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+testAdminToken)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("stream status=%d headers=%v", response.StatusCode, response.Header)
	}

	reader := bufio.NewReader(response.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				t.Fatal("stream ended before initial sample")
			}
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "data: ") {
			var sample Sample
			if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data: "))), &sample); err != nil {
				t.Fatal(err)
			}
			if sample.Version != SchemaVersion || sample.Sequence != 1 {
				t.Fatalf("stream sample=%+v", sample)
			}
			break
		}
	}
}

func TestAdminConfigurationFailsClosed(t *testing.T) {
	if _, err := NewBearerTokenAuthorizer("short"); err == nil {
		t.Fatal("accepted short bearer token")
	}
	if _, err := NewHandler(HandlerOptions{}); err == nil {
		t.Fatal("accepted missing sampler and authorizer")
	}

	now := time.Now()
	source := &fakeSource{stats: meldbase.DBStats{StartedAt: now, CapturedAt: now}}
	sampler := newTestSampler(t, source, 1, 1)
	if _, err := NewHandler(HandlerOptions{Sampler: sampler, Authorize: func(*http.Request) bool { return true }, AllowedOrigins: []string{"*"}}); err == nil {
		t.Fatal("accepted wildcard origin")
	}
}

func TestEmbeddedDashboardIsOptInAndContainsNoData(t *testing.T) {
	_, protected := newTestHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	protected.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("disabled dashboard status=%d", response.Code)
	}

	now := time.Now()
	source := &fakeSource{stats: meldbase.DBStats{StartedAt: now, CapturedAt: now}}
	sampler := newTestSampler(t, source, 1, 1)
	authorizer, err := NewBearerTokenAuthorizer(testAdminToken)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(HandlerOptions{Sampler: sampler, Authorize: authorizer, ServeDashboard: true})
	if err != nil {
		t.Fatal(err)
	}

	request = httptest.NewRequest(http.MethodGet, "/", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "Meldbase Observatory") ||
		!strings.Contains(response.Body.String(), "Operator Console") || !strings.Contains(response.Body.String(), "/assets/app.js") ||
		!strings.Contains(response.Body.String(), "id=\"root\"") {
		t.Fatalf("dashboard status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), testAdminToken) || !strings.Contains(response.Header().Get("Content-Security-Policy"), "connect-src 'self'") {
		t.Fatalf("unsafe dashboard response headers=%v", response.Header())
	}

	request = httptest.NewRequest(http.MethodGet, "/assets/app.js", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	script := response.Body.String()
	version := strconv.FormatUint(uint64(SchemaVersion), 10)
	if response.Code != http.StatusOK || !strings.HasPrefix(response.Header().Get("Content-Type"), "text/javascript") ||
		!strings.Contains(script, "Unsupported admin protocol") || !strings.Contains(script, "/v1/stats/stream") ||
		!strings.Contains(script, "/v1/diagnostics") || !strings.Contains(script, "Authorization") ||
		!strings.Contains(script, "rollbackAnchorGeneration") || !strings.Contains(script, "commitCoordinator") ||
		!strings.Contains(script, version) {
		t.Fatalf("dashboard script status=%d headers=%v", response.Code, response.Header())
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("dashboard enabled exposed stats without auth: %d", response.Code)
	}
}

func TestDiagnosticEndpointIsAuthenticatedBoundedAndIncremental(t *testing.T) {
	now := time.Now()
	source := &fakeSource{stats: meldbase.DBStats{StartedAt: now, CapturedAt: now}}
	sampler := newTestSampler(t, source, 1, 1)
	db := meldbase.New()
	t.Cleanup(func() { _ = db.Close() })
	diagnostics, err := db.EnableDiagnostics(meldbase.DiagnosticsOptions{Capacity: 4, RecordAll: true})
	if err != nil {
		t.Fatal(err)
	}
	collection := db.Collection("items")
	if _, err := collection.Find(context.Background(), meldbase.Filter{}); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Find(context.Background(), meldbase.Filter{}); err != nil {
		t.Fatal(err)
	}
	authorizer, err := NewBearerTokenAuthorizer(testAdminToken)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(HandlerOptions{Sampler: sampler, Authorize: authorizer, Diagnostics: diagnostics})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "/v1/diagnostics?after=1&limit=1", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated diagnostics status=%d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/diagnostics?after=1&limit=1", nil)
	request.Header.Set("Authorization", "Bearer "+testAdminToken)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("diagnostics status=%d body=%s", response.Code, response.Body.String())
	}
	var snapshot meldbase.DiagnosticSnapshot
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Events) != 1 || snapshot.Events[0].Sequence != 2 || snapshot.HasMore {
		t.Fatalf("diagnostic snapshot=%+v", snapshot)
	}

	for _, target := range []string{
		"/v1/diagnostics?after=invalid", "/v1/diagnostics?limit=0", "/v1/diagnostics?limit=257",
	} {
		request = httptest.NewRequest(http.MethodGet, target, nil)
		request.Header.Set("Authorization", "Bearer "+testAdminToken)
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("target=%s status=%d body=%s", target, response.Code, response.Body.String())
		}
	}
}

func TestPrometheusEndpointIsOptInAuthenticatedAndVersioned(t *testing.T) {
	now := time.Now()
	source := &fakeSource{stats: meldbase.DBStats{
		StartedAt: now, CapturedAt: now, CommitSequence: 7,
		Queries: meldbase.QueryStats{IndexScans: 3},
	}}
	sampler := newTestSampler(t, source, 1, 1)
	authorizer, err := NewBearerTokenAuthorizer(testAdminToken)
	if err != nil {
		t.Fatal(err)
	}
	disabled, err := NewHandler(HandlerOptions{Sampler: sampler, Authorize: authorizer})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Authorization", "Bearer "+testAdminToken)
	response := httptest.NewRecorder()
	disabled.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("disabled metrics status=%d", response.Code)
	}

	handler, err := NewHandler(HandlerOptions{Sampler: sampler, Authorize: authorizer, ServeMetrics: true})
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated metrics status=%d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Authorization", "Bearer "+testAdminToken)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != PrometheusContentType {
		t.Fatalf("metrics status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Content-Length") == "" {
		t.Fatalf("metrics cache/length headers=%v", response.Header())
	}
	if !strings.Contains(response.Body.String(), "meldbase_commit_sequence 7\n") || !strings.Contains(response.Body.String(), `meldbase_query_plan_total{stage="index_scan"} 3`+"\n") {
		t.Fatalf("metrics body missing current sampler state: %s", response.Body.String())
	}
	validatePrometheusText(t, response.Body.String())
}
