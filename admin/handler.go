package admin

import (
	"context"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/crapthings/meldbase"
)

// Authorizer decides whether a request may access the operator API.
type Authorizer func(*http.Request) bool

// DiagnosticSource supplies bounded diagnostic snapshots after an event sequence.
type DiagnosticSource interface {
	DiagnosticSnapshotAfter(after uint64, limit int) meldbase.DiagnosticSnapshot
}

// IndexCatalogSource supplies published index definitions for the operator API.
//
// Its output is exposed only after operator authentication.
type IndexCatalogSource interface {
	IndexCatalog(context.Context) ([]meldbase.IndexCatalogEntry, error)
}

// HandlerOptions configures operator access, sampled data, and optional dashboard and diagnostic routes.
type HandlerOptions struct {
	Sampler        *Sampler
	Authorize      Authorizer
	AllowedOrigins []string
	WriteTimeout   time.Duration
	ServeDashboard bool
	ServeMetrics   bool
	Diagnostics    DiagnosticSource
	IndexCatalog   IndexCatalogSource
	AuditWriter    io.Writer
}

// Handler serves operator statistics and optional dashboard, metrics, and diagnostic endpoints.
type Handler struct {
	sampler        *Sampler
	authorize      Authorizer
	allowedOrigins map[string]struct{}
	writeTimeout   time.Duration
	serveDashboard bool
	diagnostics    DiagnosticSource
	indexCatalog   IndexCatalogSource
	mux            *http.ServeMux
	audit          *auditLogger
}

// dashboardHTML contains the bundled dashboard page.
//
//go:embed dashboard/index.html
var dashboardHTML []byte

// dashboardJavaScript contains the bundled dashboard script.
//
//go:embed dashboard/app.js
var dashboardJavaScript []byte

// dashboardCSS contains the bundled dashboard styles.
//
//go:embed dashboard/style.css
var dashboardCSS []byte

// NewBearerTokenAuthorizer creates an operator access check using constant-time bearer token comparisons.
//
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

// NewHandler validates operator settings and registers the enabled HTTP routes.
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
		diagnostics: options.Diagnostics, indexCatalog: options.IndexCatalog, mux: http.NewServeMux(),
		audit: newAuditLogger(options.AuditWriter),
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
	if options.IndexCatalog != nil {
		handler.mux.HandleFunc("GET /v1/indexes", handler.indexes)
	}
	if options.ServeDashboard {
		handler.mux.HandleFunc("GET /", handler.dashboard)
		handler.mux.HandleFunc("GET /assets/app.js", handler.dashboardJavaScript)
		handler.mux.HandleFunc("GET /assets/style.css", handler.dashboardStyles)
	}
	return handler, nil
}

// ServeHTTP routes operator requests through origin checks, authorization, and auditing.
//
// Dashboard pages and assets are public after the origin check; API requests
// require operator authorization. Preflight requests use their own validation.
func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	observed := &adminResponseStatusWriter{ResponseWriter: writer}
	defer h.audit.record(request, observed)
	writer = observed
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

// adminResponseStatusWriter records the first response status for request auditing.
type adminResponseStatusWriter struct {
	http.ResponseWriter
	status int
}

// WriteHeader records the first status and forwards the header write.
func (writer *adminResponseStatusWriter) WriteHeader(status int) {
	if writer.status == 0 {
		writer.status = status
	}
	writer.ResponseWriter.WriteHeader(status)
}

// Write records an implicit success status and forwards response bytes.
func (writer *adminResponseStatusWriter) Write(body []byte) (int, error) {
	if writer.status == 0 {
		writer.status = http.StatusOK
	}
	return writer.ResponseWriter.Write(body)
}

// Unwrap returns the underlying response writer.
func (writer *adminResponseStatusWriter) Unwrap() http.ResponseWriter { return writer.ResponseWriter }

// Flush records an implicit success status and flushes the underlying writer when supported.
func (writer *adminResponseStatusWriter) Flush() {
	if writer.status == 0 {
		writer.status = http.StatusOK
	}
	if flusher, ok := writer.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// isDashboardPath identifies the dashboard page and its two bundled assets.
func isDashboardPath(path string) bool {
	return path == "/" || path == "/assets/app.js" || path == "/assets/style.css"
}

// preflight permits cross-origin GET requests using only the authorization header.
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

// originAllowed accepts absent origins, configured origins, or origins matching the request host and scheme.
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

// stats sends the latest sample as JSON or reports that statistics are unavailable.
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

// history sends the retained sample history as JSON.
func (h *Handler) history(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(writer).Encode(struct {
		Samples []Sample `json:"samples"`
	}{Samples: h.sampler.History()})
}

// diagnosticEvents validates the cursor and limit and sends a bounded diagnostic snapshot.
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

// indexes sends published index definitions using a consistent ordered-field representation.
func (h *Handler) indexes(writer http.ResponseWriter, request *http.Request) {
	entries, err := h.indexCatalog.IndexCatalog(request.Context())
	if err != nil {
		writeAdminError(writer, http.StatusServiceUnavailable, "index_catalog_unavailable")
		return
	}
	type field struct {
		Path  string `json:"path"`
		Order int    `json:"order"`
	}
	type entry struct {
		Collection string  `json:"collection"`
		Name       string  `json:"name"`
		Fields     []field `json:"fields"`
		Unique     bool    `json:"unique"`
	}
	result := make([]entry, 0, len(entries))
	for _, index := range entries {
		fields := index.Definition.Fields
		if len(fields) == 0 && index.Definition.Field != "" {
			fields = []meldbase.IndexField{{Field: index.Definition.Field, Order: index.Definition.Order}}
		}
		item := entry{Collection: index.Collection, Name: index.Definition.Name, Unique: index.Definition.Unique, Fields: make([]field, len(fields))}
		for position, definition := range fields {
			item.Fields[position] = field{Path: definition.Field, Order: definition.Order}
		}
		result = append(result, item)
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(writer).Encode(struct {
		Indexes []entry `json:"indexes"`
	}{Indexes: result})
}

// prometheusMetrics sends the latest sample in Prometheus text format.
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

// dashboard serves the bundled HTML page at the root path.
func (h *Handler) dashboard(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/" {
		http.NotFound(writer, request)
		return
	}
	setDashboardHeaders(writer)
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = writer.Write(dashboardHTML)
}

// dashboardJavaScript serves the bundled dashboard script.
func (h *Handler) dashboardJavaScript(writer http.ResponseWriter, _ *http.Request) {
	setDashboardHeaders(writer)
	writer.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	_, _ = writer.Write(dashboardJavaScript)
}

// dashboardStyles serves the bundled dashboard stylesheet.
func (h *Handler) dashboardStyles(writer http.ResponseWriter, _ *http.Request) {
	setDashboardHeaders(writer)
	writer.Header().Set("Content-Type", "text/css; charset=utf-8")
	_, _ = writer.Write(dashboardCSS)
}

// setDashboardHeaders disables caching and restricts dashboard resources to the same origin.
func setDashboardHeaders(writer http.ResponseWriter) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
}

// stream delivers sampler updates as server-sent events with per-write deadlines when supported.
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

// setSecurityHeaders applies restrictive browser security headers to admin responses.
func setSecurityHeaders(writer http.ResponseWriter) {
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("Referrer-Policy", "no-referrer")
	writer.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	writer.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
}

// writeAdminError sends a JSON error code with caching disabled.
func writeAdminError(writer http.ResponseWriter, status int, code string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(struct {
		Error string `json:"error"`
	}{Error: strings.TrimSpace(code)})
}
