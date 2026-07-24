package main

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
)

type serviceLogLevel uint8

const (
	serviceLogDebug serviceLogLevel = iota
	serviceLogInfo
	serviceLogWarn
	serviceLogError
)

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

// synchronizedWriter makes records from service lifecycle and independently
// synchronized audit loggers atomic relative to one another.
type synchronizedWriter struct {
	writer io.Writer
	mu     sync.Mutex
}

func (writer *synchronizedWriter) Write(value []byte) (int, error) {
	if writer == nil || writer.writer == nil {
		return len(value), nil
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.writer.Write(value)
}

// serviceLogger emits deliberately small JSONL operational events. It has no
// request logger: request content, authorization headers, actors, workspaces,
// query values, and document values do not belong in general service logs.
type serviceLogger struct {
	writer io.Writer
	level  serviceLogLevel
	now    func() time.Time
}

func newServiceLogger(writer io.Writer, rawLevel string) (*serviceLogger, error) {
	level, err := parseServiceLogLevel(rawLevel)
	if err != nil {
		return nil, err
	}
	return &serviceLogger{writer: writer, level: level, now: time.Now}, nil
}

func (logger *serviceLogger) info(event string, fields map[string]any) {
	logger.record(serviceLogInfo, event, fields)
}
func (logger *serviceLogger) warn(event string, fields map[string]any) {
	logger.record(serviceLogWarn, event, fields)
}
func (logger *serviceLogger) error(event string, fields map[string]any) {
	logger.record(serviceLogError, event, fields)
}

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
