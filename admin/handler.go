package admin

import (
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/crapthings/meldbase"
)

type Authorizer func(*http.Request) bool

type DiagnosticSource interface {
	DiagnosticSnapshotAfter(after uint64, limit int) meldbase.DiagnosticSnapshot
}

type HandlerOptions struct {
	Sampler        *Sampler
	Authorize      Authorizer
	AllowedOrigins []string
	WriteTimeout   time.Duration
	ServeDashboard bool
	ServeMetrics   bool
	Diagnostics    DiagnosticSource
}

type Handler struct {
	sampler        *Sampler
	authorize      Authorizer
	allowedOrigins map[string]struct{}
	writeTimeout   time.Duration
	serveDashboard bool
	diagnostics    DiagnosticSource
	mux            *http.ServeMux
}

//go:embed dashboard/index.html
var dashboardHTML []byte

//go:embed dashboard/app.js
var dashboardJavaScript []byte

//go:embed dashboard/style.css
var dashboardCSS []byte

// NewBearerTokenAuthorizer creates a constant-time Authorization header check.
// Tokens are intentionally not accepted in URLs, where they leak into logs and
// browser history.
func NewBearerTokenAuthorizer(token string) (Authorizer, error) {
	if len(token) < 32 {
		return nil, errors.New("meldbase admin: bearer token must contain at least 32 bytes")
	}
	expected := []byte("Bearer " + token)
	return func(request *http.Request) bool {
		actual := []byte(request.Header.Get("Authorization"))
		return len(actual) == len(expected) && subtle.ConstantTimeCompare(actual, expected) == 1
	}, nil
}

func NewHandler(options HandlerOptions) (*Handler, error) {
	if options.Sampler == nil {
		return nil, errors.New("meldbase admin: sampler is required")
	}
	if options.Authorize == nil {
		return nil, errors.New("meldbase admin: authorizer is required")
	}
	if options.WriteTimeout == 0 {
		options.WriteTimeout = 5 * time.Second
	}
	if options.WriteTimeout < 100*time.Millisecond || options.WriteTimeout > time.Minute {
		return nil, errors.New("meldbase admin: write timeout must be between 100ms and one minute")
	}

	handler := &Handler{
		sampler: options.Sampler, authorize: options.Authorize,
		allowedOrigins: make(map[string]struct{}, len(options.AllowedOrigins)),
		writeTimeout:   options.WriteTimeout, serveDashboard: options.ServeDashboard,
		diagnostics: options.Diagnostics, mux: http.NewServeMux(),
	}
	for _, origin := range options.AllowedOrigins {
		parsed, err := url.Parse(origin)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, errors.New("meldbase admin: allowed origins must be exact http(s) origins")
		}
		handler.allowedOrigins[origin] = struct{}{}
	}
	handler.mux.HandleFunc("GET /v1/stats", handler.stats)
	handler.mux.HandleFunc("GET /v1/stats/history", handler.history)
	handler.mux.HandleFunc("GET /v1/stats/stream", handler.stream)
	if options.ServeMetrics {
		handler.mux.HandleFunc("GET /metrics", handler.prometheusMetrics)
	}
	if options.Diagnostics != nil {
		handler.mux.HandleFunc("GET /v1/diagnostics", handler.diagnosticEvents)
	}
	if options.ServeDashboard {
		handler.mux.HandleFunc("GET /", handler.dashboard)
		handler.mux.HandleFunc("GET /assets/app.js", handler.dashboardJavaScript)
		handler.mux.HandleFunc("GET /assets/style.css", handler.dashboardStyles)
	}
	return handler, nil
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	setSecurityHeaders(writer)
	if !h.originAllowed(request, request.Header.Get("Origin")) {
		writeAdminError(writer, http.StatusForbidden, "origin_forbidden")
		return
	}
	if origin := request.Header.Get("Origin"); origin != "" {
		writer.Header().Set("Access-Control-Allow-Origin", origin)
		writer.Header().Add("Vary", "Origin")
	}
	if h.serveDashboard && isDashboardPath(request.URL.Path) && request.Method == http.MethodGet {
		h.mux.ServeHTTP(writer, request)
		return
	}
	if request.Method == http.MethodOptions {
		h.preflight(writer, request)
		return
	}
	if !h.authorize(request) {
		writer.Header().Set("WWW-Authenticate", `Bearer realm="meldbase-admin"`)
		writeAdminError(writer, http.StatusUnauthorized, "unauthenticated")
		return
	}
	h.mux.ServeHTTP(writer, request)
}

func isDashboardPath(path string) bool {
	return path == "/" || path == "/assets/app.js" || path == "/assets/style.css"
}

func (h *Handler) preflight(writer http.ResponseWriter, request *http.Request) {
	if request.Header.Get("Origin") == "" || request.Header.Get("Access-Control-Request-Method") != http.MethodGet {
		writeAdminError(writer, http.StatusForbidden, "preflight_forbidden")
		return
	}
	for _, header := range strings.Split(request.Header.Get("Access-Control-Request-Headers"), ",") {
		header = strings.TrimSpace(header)
		if header != "" && !strings.EqualFold(header, "Authorization") {
			writeAdminError(writer, http.StatusForbidden, "preflight_forbidden")
			return
		}
	}
	writer.Header().Set("Access-Control-Allow-Methods", http.MethodGet)
	writer.Header().Set("Access-Control-Allow-Headers", "Authorization")
	writer.Header().Set("Access-Control-Max-Age", "600")
	writer.WriteHeader(http.StatusNoContent)
}

func (h *Handler) originAllowed(request *http.Request, origin string) bool {
	if origin == "" {
		return true
	}
	if _, ok := h.allowedOrigins[origin]; ok {
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host != request.Host {
		return false
	}
	scheme := "http"
	if request.TLS != nil {
		scheme = "https"
	}
	return parsed.Scheme == scheme
}

func (h *Handler) stats(writer http.ResponseWriter, _ *http.Request) {
	sample, ok := h.sampler.Latest()
	if !ok {
		writeAdminError(writer, http.StatusServiceUnavailable, "stats_unavailable")
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(writer).Encode(sample); err != nil {
		return
	}
}

func (h *Handler) history(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(writer).Encode(struct {
		Version uint32   `json:"version"`
		Samples []Sample `json:"samples"`
	}{Version: SchemaVersion, Samples: h.sampler.History()})
}

func (h *Handler) diagnosticEvents(writer http.ResponseWriter, request *http.Request) {
	after := uint64(0)
	limit := 100
	var err error
	if raw := request.URL.Query().Get("after"); raw != "" {
		after, err = strconv.ParseUint(raw, 10, 64)
		if err != nil {
			writeAdminError(writer, http.StatusBadRequest, "invalid_diagnostic_cursor")
			return
		}
	}
	if raw := request.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 256 {
			writeAdminError(writer, http.StatusBadRequest, "invalid_diagnostic_limit")
			return
		}
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(writer).Encode(h.diagnostics.DiagnosticSnapshotAfter(after, limit))
}

func (h *Handler) prometheusMetrics(writer http.ResponseWriter, _ *http.Request) {
	sample, ok := h.sampler.Latest()
	if !ok {
		writeAdminError(writer, http.StatusServiceUnavailable, "stats_unavailable")
		return
	}
	payload := MarshalPrometheus(sample)
	writer.Header().Set("Content-Type", PrometheusContentType)
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	_, _ = writer.Write(payload)
}

func (h *Handler) dashboard(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/" {
		http.NotFound(writer, request)
		return
	}
	setDashboardHeaders(writer)
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = writer.Write(dashboardHTML)
}

func (h *Handler) dashboardJavaScript(writer http.ResponseWriter, _ *http.Request) {
	setDashboardHeaders(writer)
	writer.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	_, _ = writer.Write(dashboardJavaScript)
}

func (h *Handler) dashboardStyles(writer http.ResponseWriter, _ *http.Request) {
	setDashboardHeaders(writer)
	writer.Header().Set("Content-Type", "text/css; charset=utf-8")
	_, _ = writer.Write(dashboardCSS)
}

func setDashboardHeaders(writer http.ResponseWriter) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
}

func (h *Handler) stream(writer http.ResponseWriter, request *http.Request) {
	flusher, ok := writer.(http.Flusher)
	if !ok {
		writeAdminError(writer, http.StatusInternalServerError, "streaming_unsupported")
		return
	}
	subscription, err := h.sampler.Subscribe(request.Context())
	if err != nil {
		writeAdminError(writer, http.StatusServiceUnavailable, "stream_unavailable")
		return
	}
	defer subscription.Close()

	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-store, no-transform")
	writer.Header().Set("Connection", "keep-alive")
	writer.WriteHeader(http.StatusOK)
	flusher.Flush()
	controller := http.NewResponseController(writer)

	for sample := range subscription.C {
		payload, err := json.Marshal(sample)
		if err != nil {
			return
		}
		if err := controller.SetWriteDeadline(time.Now().Add(h.writeTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return
		}
		if _, err := fmt.Fprintf(writer, "event: stats\ndata: %s\n\n", payload); err != nil {
			return
		}
		flusher.Flush()
	}
}

func setSecurityHeaders(writer http.ResponseWriter) {
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("Referrer-Policy", "no-referrer")
	writer.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	writer.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
}

func writeAdminError(writer http.ResponseWriter, status int, code string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(struct {
		Error string `json:"error"`
	}{Error: strings.TrimSpace(code)})
}
