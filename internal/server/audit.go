package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// httpAuditLogger emits only security rejections. It intentionally excludes
// credential material, query strings, bodies, client addresses, and concrete
// collection names from the retained event.
type httpAuditLogger struct {
	writer io.Writer
	mu     sync.Mutex
}

func newHTTPAuditLogger(writer io.Writer) *httpAuditLogger {
	if writer == nil {
		return nil
	}
	return &httpAuditLogger{writer: writer}
}

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
