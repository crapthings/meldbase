package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/pprof"
	runtimetrace "runtime/trace"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrRuntimeCaptureBusy reports that another admin runtime capture is active.
	ErrRuntimeCaptureBusy = errors.New("meldbase admin: another runtime capture is active")
	// ErrRuntimeCaptureLimit reports that capture output exceeded its byte limit.
	ErrRuntimeCaptureLimit = errors.New("meldbase admin: runtime capture byte limit reached")
)

const (
	// defaultRuntimeCaptureDuration sets the requested duration when none is supplied.
	defaultRuntimeCaptureDuration = 10 * time.Second
	// defaultRuntimeCaptureMaxTime sets the default maximum capture duration.
	defaultRuntimeCaptureMaxTime = 60 * time.Second
	// defaultRuntimeCaptureMaxBytes sets the default capture output limit.
	defaultRuntimeCaptureMaxBytes = 64 << 20
	// minRuntimeCaptureDuration sets the shortest permitted timed capture.
	minRuntimeCaptureDuration = 100 * time.Millisecond
)

// RuntimeCaptureKind selects a CPU profile, heap snapshot, or runtime trace.
type RuntimeCaptureKind string

const (
	// RuntimeCPUProfile selects a timed CPU profile.
	RuntimeCPUProfile RuntimeCaptureKind = "cpu"
	// RuntimeHeapProfile selects an immediate heap snapshot.
	RuntimeHeapProfile RuntimeCaptureKind = "heap"
	// RuntimeTrace selects a timed Go runtime trace.
	RuntimeTrace RuntimeCaptureKind = "trace"
)

// RuntimeProfilerOptions sets duration and output limits for runtime captures.
type RuntimeProfilerOptions struct {
	MaxDuration time.Duration
	MaxBytes    int64
}

// RuntimeCaptureOptions selects the duration of one timed capture.
type RuntimeCaptureOptions struct {
	// Duration applies to CPU profiles and runtime traces. Zero selects ten
	// seconds. Heap snapshots are immediate and require zero.
	Duration time.Duration
}

// RuntimeCaptureResult reports capture timing, output size, and whether output was truncated.
type RuntimeCaptureResult struct {
	Kind      RuntimeCaptureKind
	StartedAt time.Time
	Duration  time.Duration
	Bytes     int64
	Truncated bool
}

// RuntimeProfilerStats reports capture activity, outcomes, and total output bytes.
type RuntimeProfilerStats struct {
	Active    uint64
	Attempts  uint64
	Completed uint64
	Failed    uint64
	Bytes     uint64
}

// RuntimeProfiler captures CPU profiles, heap snapshots, and runtime traces within configured limits.
//
// It does not start a listener or retain capture bytes. The caller owns
// authorization, the output writer, and its lifetime. CPU profiles and runtime
// traces add process-wide overhead; use them for short diagnostic windows.
type RuntimeProfiler struct {
	maxDuration time.Duration
	maxBytes    int64
	active      atomic.Uint64
	attempts    atomic.Uint64
	completed   atomic.Uint64
	failed      atomic.Uint64
	bytes       atomic.Uint64
}

// runtimeCaptureSlot allows only one admin runtime capture at a time across the process.
var runtimeCaptureSlot = make(chan struct{}, 1)

// NewRuntimeProfiler creates a profiler after validating duration and output limits.
func NewRuntimeProfiler(options RuntimeProfilerOptions) (*RuntimeProfiler, error) {
	if options.MaxDuration == 0 {
		options.MaxDuration = defaultRuntimeCaptureMaxTime
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = defaultRuntimeCaptureMaxBytes
	}
	if options.MaxDuration < minRuntimeCaptureDuration || options.MaxDuration > 5*time.Minute {
		return nil, errors.New("meldbase admin: runtime profile MaxDuration must be between 100ms and 5m")
	}
	if options.MaxBytes < 1024 || options.MaxBytes > 1<<30 {
		return nil, errors.New("meldbase admin: runtime profile MaxBytes must be between 1KiB and 1GiB")
	}
	return &RuntimeProfiler{maxDuration: options.MaxDuration, maxBytes: options.MaxBytes}, nil
}

// Stats returns the profiler's aggregate capture counters.
func (profiler *RuntimeProfiler) Stats() RuntimeProfilerStats {
	if profiler == nil {
		return RuntimeProfilerStats{}
	}
	return RuntimeProfilerStats{
		Active: profiler.active.Load(), Attempts: profiler.attempts.Load(),
		Completed: profiler.completed.Load(), Failed: profiler.failed.Load(), Bytes: profiler.bytes.Load(),
	}
}

// Capture writes a bounded runtime profile or trace and records its outcome.
func (profiler *RuntimeProfiler) Capture(ctx context.Context, writer io.Writer, kind RuntimeCaptureKind, options RuntimeCaptureOptions) (result RuntimeCaptureResult, resultErr error) {
	if profiler == nil || ctx == nil || writer == nil {
		return result, errors.New("meldbase admin: runtime profiler, context and writer are required")
	}
	duration, err := profiler.captureDuration(kind, options.Duration)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	profiler.attempts.Add(1)
	select {
	case runtimeCaptureSlot <- struct{}{}:
		defer func() { <-runtimeCaptureSlot }()
	default:
		profiler.failed.Add(1)
		return result, ErrRuntimeCaptureBusy
	}
	profiler.active.Add(1)
	defer profiler.active.Add(^uint64(0))
	result = RuntimeCaptureResult{Kind: kind, StartedAt: time.Now()}
	bounded := &captureWriter{writer: writer, remaining: profiler.maxBytes}
	defer func() {
		result.Duration = time.Since(result.StartedAt)
		result.Bytes = bounded.written
		result.Truncated = errors.Is(bounded.err, ErrRuntimeCaptureLimit)
		profiler.bytes.Add(uint64(max(result.Bytes, 0)))
		if resultErr == nil && bounded.err != nil {
			resultErr = bounded.err
		}
		if resultErr == nil {
			profiler.completed.Add(1)
		} else {
			profiler.failed.Add(1)
		}
	}()

	switch kind {
	case RuntimeHeapProfile:
		profile := pprof.Lookup("heap")
		if profile == nil {
			return result, errors.New("meldbase admin: heap profile unavailable")
		}
		resultErr = profile.WriteTo(bounded, 0)
	case RuntimeCPUProfile:
		if err := pprof.StartCPUProfile(bounded); err != nil {
			return result, fmt.Errorf("meldbase admin: start CPU profile: %w", err)
		}
		resultErr = waitRuntimeCapture(ctx, duration)
		pprof.StopCPUProfile()
	case RuntimeTrace:
		if err := runtimetrace.Start(bounded); err != nil {
			return result, fmt.Errorf("meldbase admin: start runtime trace: %w", err)
		}
		resultErr = waitRuntimeCapture(ctx, duration)
		runtimetrace.Stop()
	default:
		return result, errors.New("meldbase admin: unsupported runtime capture kind")
	}
	return result, resultErr
}

// captureDuration validates the capture kind and applies its duration defaults and limits.
func (profiler *RuntimeProfiler) captureDuration(kind RuntimeCaptureKind, requested time.Duration) (time.Duration, error) {
	switch kind {
	case RuntimeHeapProfile:
		if requested != 0 {
			return 0, errors.New("meldbase admin: heap capture duration must be zero")
		}
		return 0, nil
	case RuntimeCPUProfile, RuntimeTrace:
		if requested == 0 {
			requested = defaultRuntimeCaptureDuration
			if requested > profiler.maxDuration {
				requested = profiler.maxDuration
			}
		}
		if requested < minRuntimeCaptureDuration || requested > profiler.maxDuration {
			return 0, fmt.Errorf("meldbase admin: capture duration must be between 100ms and %s", profiler.maxDuration)
		}
		return requested, nil
	default:
		return 0, errors.New("meldbase admin: unsupported runtime capture kind")
	}
}

// waitRuntimeCapture waits for the capture duration or context cancellation.
func waitRuntimeCapture(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// captureWriter serializes output writes while enforcing a shared byte limit.
type captureWriter struct {
	mu        sync.Mutex
	writer    io.Writer
	remaining int64
	written   int64
	err       error
}

// Write forwards bytes within the remaining limit and preserves the first output error.
func (writer *captureWriter) Write(payload []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.err != nil {
		return 0, writer.err
	}
	limited := int64(len(payload)) > writer.remaining
	if limited {
		payload = payload[:writer.remaining]
	}
	written, err := writer.writer.Write(payload)
	writer.written += int64(written)
	writer.remaining -= int64(written)
	if err != nil {
		writer.err = err
		return written, err
	}
	if written != len(payload) {
		writer.err = io.ErrShortWrite
		return written, writer.err
	}
	if limited {
		writer.err = ErrRuntimeCaptureLimit
		return written, writer.err
	}
	return written, nil
}
