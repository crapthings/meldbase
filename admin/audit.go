package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

// auditLogger writes operator request outcomes as line-delimited JSON records.
//
// It deliberately excludes query strings, headers, credentials, bodies, remote
// addresses, and response content.
type auditLogger struct {
	writer io.Writer
	mu     sync.Mutex
}

// newAuditLogger creates an audit logger when an output writer is supplied.
func newAuditLogger(writer io.Writer) *auditLogger {
	if writer == nil {
		return nil
	}
	return &auditLogger{writer: writer}
}

// record writes a serialized request outcome while excluding credentials and response content.
func (logger *auditLogger) record(request *http.Request, response *adminResponseStatusWriter) {
	if logger == nil || request == nil || response == nil {
		return
	}
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	outcome := "success"
	if status == http.StatusUnauthorized {
		outcome = "unauthenticated"
	} else if status == http.StatusForbidden {
		outcome = "forbidden"
	} else if status >= 400 {
		outcome = "failure"
	}
	event := struct {
		SchemaVersion int       `json:"schemaVersion"`
		Timestamp     time.Time `json:"timestamp"`
		Kind          string    `json:"kind"`
		Method        string    `json:"method"`
		Path          string    `json:"path"`
		Status        int       `json:"status"`
		Outcome       string    `json:"outcome"`
	}{SchemaVersion: 1, Timestamp: time.Now().UTC(), Kind: "admin_request", Method: request.Method, Path: request.URL.Path, Status: status, Outcome: outcome}
	encoded, err := json.Marshal(event)
	if err != nil {
		return
	}
	logger.mu.Lock()
	_, _ = logger.writer.Write(append(encoded, '\n'))
	logger.mu.Unlock()
}
