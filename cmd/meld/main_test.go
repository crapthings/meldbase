package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestDemoExercisesDurabilityIndexUpdateAndReactiveQuery(t *testing.T) {
	var stdout, stderr bytes.Buffer
	path := filepath.Join(t.TempDir(), "demo.meld")
	if err := run([]string{"demo", "--db", path}, &stdout, &stderr); err != nil {
		t.Fatalf("demo error = %v stderr=%s", err, stderr.String())
	}
	for _, expected := range []string{
		"Inserted user:", "Created index: users_email", "Found users: 1",
		"Received snapshot: 1 document", "Received snapshot: 2 documents",
		"Database reopened successfully", "Recovery check passed",
	} {
		if !strings.Contains(stdout.String(), expected) {
			t.Fatalf("demo output missing %q:\n%s", expected, stdout.String())
		}
	}
}

func TestServeRequiresExplicitUnsafeDevelopmentMode(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{"serve", "--db", filepath.Join(t.TempDir(), "data.meld")}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "--dev-no-auth") {
		t.Fatalf("serve error = %v", err)
	}
}

func TestDefaultRealtimeURL(t *testing.T) {
	if got := defaultRealtimeURL(":8080"); got != "ws://localhost:8080/v1/realtime" {
		t.Fatalf("url = %q", got)
	}
}
