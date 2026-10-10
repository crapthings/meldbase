package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// newBufferServiceLogger creates a logger writing to the supplied buffer or fails the test.
func newBufferServiceLogger(t *testing.T, output *bytes.Buffer, level string) *serviceLogger {
	t.Helper()
	logger, err := newServiceLogger(output, level)
	if err != nil {
		t.Fatal(err)
	}
	return logger
}

// TestServiceLoggerFiltersStructuredEventsByLevel checks that log levels filter events and retained records contain the expected structured fields.
func TestServiceLoggerFiltersStructuredEventsByLevel(t *testing.T) {
	var output bytes.Buffer
	logger := newBufferServiceLogger(t, &output, "warn")
	logger.now = func() time.Time { return time.Date(2026, time.July, 24, 15, 0, 0, 0, time.UTC) }
	logger.info("server_started", map[string]any{"address": "127.0.0.1:8080"})
	logger.warn("server_started", map[string]any{"address": "127.0.0.1:8080"})
	logger.error("scheduled_backup_failed", map[string]any{"error": "disk full"})
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines=%q", lines)
	}
	for index, want := range []struct{ level, event string }{{"warn", "server_started"}, {"error", "scheduled_backup_failed"}} {
		var record struct {
			SchemaVersion int       `json:"schemaVersion"`
			Timestamp     time.Time `json:"timestamp"`
			Kind          string    `json:"kind"`
			Level         string    `json:"level"`
			Event         string    `json:"event"`
		}
		if err := json.Unmarshal([]byte(lines[index]), &record); err != nil || record.SchemaVersion != 1 || record.Kind != "service" || record.Level != want.level || record.Event != want.event || !record.Timestamp.Equal(logger.now()) {
			t.Fatalf("record=%+v error=%v", record, err)
		}
	}
	for _, value := range []string{"verbose", "INFO!", ""} {
		if _, err := parseServiceLogLevel(value); value == "" && err != nil || value != "" && err == nil {
			t.Fatalf("level %q error=%v", value, err)
		}
	}
}
