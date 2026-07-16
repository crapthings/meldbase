package admin

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestRuntimeProfilerHeapCaptureIsBoundedAndObservable(t *testing.T) {
	profiler, err := NewRuntimeProfiler(RuntimeProfilerOptions{MaxDuration: time.Second, MaxBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	result, err := profiler.Capture(context.Background(), &output, RuntimeHeapProfile, RuntimeCaptureOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != RuntimeHeapProfile || result.Bytes <= 0 || int64(output.Len()) != result.Bytes || result.Truncated {
		t.Fatalf("heap result=%+v bytes=%d", result, output.Len())
	}
	if stats := profiler.Stats(); stats.Active != 0 || stats.Attempts != 1 || stats.Completed != 1 || stats.Failed != 0 || stats.Bytes != uint64(result.Bytes) {
		t.Fatalf("heap stats=%+v", stats)
	}
}

func TestRuntimeProfilerEnforcesGlobalExclusionAndDuration(t *testing.T) {
	profiler, err := NewRuntimeProfiler(RuntimeProfilerOptions{MaxDuration: time.Second, MaxBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		var output bytes.Buffer
		_, captureErr := profiler.Capture(context.Background(), &output, RuntimeTrace, RuntimeCaptureOptions{Duration: 150 * time.Millisecond})
		done <- captureErr
	}()
	deadline := time.Now().Add(time.Second)
	for profiler.Stats().Active == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if profiler.Stats().Active != 1 {
		t.Fatal("runtime trace did not become active")
	}
	if _, err := profiler.Capture(context.Background(), &bytes.Buffer{}, RuntimeHeapProfile, RuntimeCaptureOptions{}); !errors.Is(err, ErrRuntimeCaptureBusy) {
		t.Fatalf("concurrent capture error=%v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if stats := profiler.Stats(); stats.Active != 0 || stats.Attempts != 2 || stats.Completed != 1 || stats.Failed != 1 {
		t.Fatalf("exclusion stats=%+v", stats)
	}
}

func TestRuntimeProfilerCancellationStopsCPUProfile(t *testing.T) {
	profiler, err := NewRuntimeProfiler(RuntimeProfilerOptions{MaxDuration: time.Second, MaxBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(120*time.Millisecond, cancel)
	var output bytes.Buffer
	result, err := profiler.Capture(ctx, &output, RuntimeCPUProfile, RuntimeCaptureOptions{Duration: time.Second})
	if !errors.Is(err, context.Canceled) || result.Bytes == 0 || result.Duration >= time.Second {
		t.Fatalf("canceled CPU result=%+v err=%v bytes=%d", result, err, output.Len())
	}
	if stats := profiler.Stats(); stats.Active != 0 || stats.Failed != 1 || stats.Completed != 0 {
		t.Fatalf("canceled stats=%+v", stats)
	}
}

func TestRuntimeProfilerValidatesPolicyAndCapture(t *testing.T) {
	for _, options := range []RuntimeProfilerOptions{
		{MaxDuration: time.Millisecond}, {MaxDuration: 6 * time.Minute}, {MaxBytes: 1}, {MaxBytes: (1 << 30) + 1},
	} {
		if _, err := NewRuntimeProfiler(options); err == nil {
			t.Fatalf("invalid options accepted: %+v", options)
		}
	}
	profiler, err := NewRuntimeProfiler(RuntimeProfilerOptions{MaxDuration: time.Second, MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if duration, err := profiler.captureDuration(RuntimeCPUProfile, 0); err != nil || duration != time.Second {
		t.Fatalf("bounded default duration=%s err=%v", duration, err)
	}
	if _, err := profiler.Capture(nil, &bytes.Buffer{}, RuntimeHeapProfile, RuntimeCaptureOptions{}); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, err := profiler.Capture(context.Background(), &bytes.Buffer{}, RuntimeCaptureKind("unknown"), RuntimeCaptureOptions{}); err == nil {
		t.Fatal("unknown capture accepted")
	}
	if _, err := profiler.Capture(context.Background(), &bytes.Buffer{}, RuntimeTrace, RuntimeCaptureOptions{Duration: time.Millisecond}); err == nil {
		t.Fatal("short trace accepted")
	}
	if _, err := profiler.Capture(context.Background(), &bytes.Buffer{}, RuntimeHeapProfile, RuntimeCaptureOptions{Duration: time.Second}); err == nil {
		t.Fatal("heap duration accepted")
	}
	result, err := profiler.Capture(context.Background(), &bytes.Buffer{}, RuntimeHeapProfile, RuntimeCaptureOptions{})
	if !errors.Is(err, ErrRuntimeCaptureLimit) || !result.Truncated || result.Bytes != 1024 {
		t.Fatalf("bounded heap result=%+v err=%v", result, err)
	}
}
