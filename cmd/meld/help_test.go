package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestRunTopLevelHelpIsSuccessful checks that each supported help spelling prints output successfully.
func TestRunTopLevelHelpIsSuccessful(t *testing.T) {
	for _, argument := range []string{"--help", "-h", "help"} {
		t.Run(argument, func(t *testing.T) {
			var output bytes.Buffer
			if err := run([]string{argument}, &output, &bytes.Buffer{}); err != nil {
				t.Fatalf("run(%q)=%v", argument, err)
			}
			if output.Len() == 0 {
				t.Fatalf("run(%q) wrote no help", argument)
			}
		})
	}
}

// TestTopLevelHelpPreservesStableOperationalCommands checks that help lists stable operational commands and omits experimental destructive commands.
func TestTopLevelHelpPreservesStableOperationalCommands(t *testing.T) {
	var output bytes.Buffer
	if err := run([]string{"help"}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"access-policy", "init", "serve", "inspect", "verify", "backup", "restore", "export", "import", "index-build"} {
		if !strings.Contains(output.String(), "  "+command) {
			t.Fatalf("stable command %q missing from help:\n%s", command, output.String())
		}
	}
	if strings.Contains(output.String(), "destructive-enospc-check") {
		t.Fatalf("experimental destructive command leaked into stable help:\n%s", output.String())
	}
}

// TestVersionCommandReportsBuildIdentity checks that version output includes the build version and commit and rejects extra arguments.
func TestVersionCommandReportsBuildIdentity(t *testing.T) {
	for _, argument := range []string{"version", "--version"} {
		t.Run(argument, func(t *testing.T) {
			var output bytes.Buffer
			if err := run([]string{argument}, &output, &bytes.Buffer{}); err != nil {
				t.Fatalf("run(%q)=%v", argument, err)
			}
			if got, want := output.String(), "meld "+buildVersion+" ("+buildCommit+")\n"; got != want {
				t.Fatalf("version output=%q want %q", got, want)
			}
		})
	}
	if err := run([]string{"version", "extra"}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("version accepted an unexpected argument")
	}
}
