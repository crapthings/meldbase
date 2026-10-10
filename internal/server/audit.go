package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// httpAuditLogger records authentication, authorization, and rate-limit rejections.
//
// Events exclude credentials, query strings, bodies, client addresses, and
// concrete collection names.
type httpAuditLogger struct {
	writer io.Writer
	mu     sync.Mutex
}

// newHTTPAuditLogger creates a security rejection logger or returns nil when no writer is supplied.
func newHTTPAuditLogger(writer io.Writer) *httpAuditLogger {
	if writer == nil {
		return nil
	}
	return &httpAuditLogger{writer: writer}
}

// record writes a JSON audit event for an authentication, authorization, or rate-limit rejection.
func (logger *httpAuditLogger) record(request *http.Request, response *responseStatusWriter) {
	if logger == nil || request == nil || response == nil {
		return
	}
	status := response.status
	var outcome string
	switch status {
	case http.StatusUnauthorized:
		outcome = "unauthenticated"
	case http.StatusForbidden:
		outcome = "forbidden"
	case http.StatusTooManyRequests:
		outcome = "rate_limited"
	default:
		return
	}
	event := struct {
		SchemaVersion int       `json:"schemaVersion"`
		Timestamp     time.Time `json:"timestamp"`
		Kind          string    `json:"kind"`
		Method        string    `json:"method"`
		Route         string    `json:"route"`
		Status        int       `json:"status"`
		Outcome       string    `json:"outcome"`
	}{SchemaVersion: 1, Timestamp: time.Now().UTC(), Kind: "http_security_rejection", Method: request.Method, Route: auditRoute(request.URL.Path), Status: status, Outcome: outcome}
	encoded, err := json.Marshal(event)
	if err != nil {
		return
	}
	logger.mu.Lock()
	_, _ = logger.writer.Write(append(encoded, '\n'))
	logger.mu.Unlock()
}

// auditRoute replaces a collection name in a data API path with a fixed placeholder.
func auditRoute(path string) string {
	const collectionPrefix = "/v1/collections/"
	if !strings.HasPrefix(path, collectionPrefix) {
		return path
	}
	rest := strings.TrimPrefix(path, collectionPrefix)
	if _, suffix, found := strings.Cut(rest, "/"); found {
		return collectionPrefix + "{collection}/" + suffix
	}
	return collectionPrefix + "{collection}"
}
