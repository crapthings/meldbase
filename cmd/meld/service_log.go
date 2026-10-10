package main

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
)

// serviceLogLevel ranks operational events by severity.
type serviceLogLevel uint8

const (
	// serviceLogDebug selects detailed diagnostic events.
	serviceLogDebug serviceLogLevel = iota
	// serviceLogInfo selects routine service lifecycle events.
	serviceLogInfo
	// serviceLogWarn selects events that need operator attention.
	serviceLogWarn
	// serviceLogError selects service failure events.
	serviceLogError
)

// parseServiceLogLevel parses a severity name and defaults an empty value to info.
func parseServiceLogLevel(value string) (serviceLogLevel, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return serviceLogDebug, nil
	case "info", "":
		return serviceLogInfo, nil
	case "warn", "warning":
		return serviceLogWarn, nil
	case "error":
		return serviceLogError, nil
	default:
		return 0, errors.New("--log-level must be debug, info, warn, or error")
	}
}

// String returns a severity name and maps unknown levels to error.
func (level serviceLogLevel) String() string {
	switch level {
	case serviceLogDebug:
		return "debug"
	case serviceLogInfo:
		return "info"
	case serviceLogWarn:
		return "warn"
	default:
		return "error"
	}
}

// synchronizedWriter prevents service and audit output writes from interleaving.
type synchronizedWriter struct {
	writer io.Writer
	mu     sync.Mutex
}

// Write serializes output writes or discards bytes when no writer is configured.
func (writer *synchronizedWriter) Write(value []byte) (int, error) {
	if writer == nil || writer.writer == nil {
		return len(value), nil
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.writer.Write(value)
}

// serviceLogger writes operational events as JSON lines using a minimum severity threshold.
type serviceLogger struct {
	writer io.Writer
	level  serviceLogLevel
	now    func() time.Time
}

// newServiceLogger creates a JSON event logger after validating its severity threshold.
func newServiceLogger(writer io.Writer, rawLevel string) (*serviceLogger, error) {
	level, err := parseServiceLogLevel(rawLevel)
	if err != nil {
		return nil, err
	}
	return &serviceLogger{writer: writer, level: level, now: time.Now}, nil
}

// info records an event at info severity when the threshold permits it.
func (logger *serviceLogger) info(event string, fields map[string]any) {
	logger.record(serviceLogInfo, event, fields)
}

// warn records an event at warning severity when the threshold permits it.
func (logger *serviceLogger) warn(event string, fields map[string]any) {
	logger.record(serviceLogWarn, event, fields)
}

// error records an event at error severity when the threshold permits it.
func (logger *serviceLogger) error(event string, fields map[string]any) {
	logger.record(serviceLogError, event, fields)
}

// record writes a timestamped JSON event when its severity meets the configured threshold.
func (logger *serviceLogger) record(level serviceLogLevel, event string, fields map[string]any) {
	if logger == nil || logger.writer == nil || level < logger.level {
		return
	}
	record := struct {
		SchemaVersion int            `json:"schemaVersion"`
		Timestamp     time.Time      `json:"timestamp"`
		Kind          string         `json:"kind"`
		Level         string         `json:"level"`
		Event         string         `json:"event"`
		Fields        map[string]any `json:"fields,omitempty"`
	}{SchemaVersion: 1, Timestamp: logger.now().UTC(), Kind: "service", Level: level.String(), Event: event, Fields: fields}
	encoded, err := json.Marshal(record)
	if err == nil {
		_, _ = logger.writer.Write(append(encoded, '\n'))
	}
}
