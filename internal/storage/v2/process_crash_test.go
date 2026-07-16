package v2

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

const v2CrashHelperExitCode = 91

func TestV2AbruptProcessExitPublicationMatrix(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "process-crash-base.meld2")
	file, _, err := Open(basePath)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{15: 1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("old"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	base, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatal(err)
	}

	for _, scenario := range []struct {
		name     string
		point    faultPoint
		allowOld bool
		allowNew bool
	}{
		{name: "after-first-page-write", point: faultAfterPageWrite, allowOld: true},
		{name: "before-data-sync", point: faultBeforeDataSync, allowOld: true},
		{name: "after-data-sync", point: faultAfterDataSync, allowOld: true},
		// A normal process exit may leave the unsynced Meta write in the kernel
		// page cache. Either complete generation is valid; mixed state is not.
		{name: "after-meta-write", point: faultAfterMetaWrite, allowOld: true, allowNew: true},
		{name: "after-meta-sync", point: faultAfterMetaSync, allowNew: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "crash.meld2")
			if err := os.WriteFile(path, base, 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestV2CrashProcessHelper$", "-test.count=1")
			command.Env = append(os.Environ(),
				"MELDBASE_V2_CRASH_HELPER=1",
				"MELDBASE_V2_CRASH_PATH="+path,
				"MELDBASE_V2_CRASH_POINT="+strconv.Itoa(int(scenario.point)),
			)
			err := command.Run()
			var exitErr *exec.ExitError
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != v2CrashHelperExitCode {
				t.Fatalf("helper error=%v", err)
			}

			reopened, meta, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			value, exists, err := reopened.GetDocument("items", id)
			if err != nil || !exists {
				t.Fatalf("value=%q exists=%t err=%v", value, exists, err)
			}
			switch string(value) {
			case "old":
				if !scenario.allowOld || meta.CommitSequence != 1 {
					t.Fatalf("unexpected old generation meta=%+v", meta)
				}
			case "new":
				if !scenario.allowNew || meta.CommitSequence != 2 {
					t.Fatalf("unexpected new generation meta=%+v", meta)
				}
			default:
				t.Fatalf("mixed/unknown value=%q meta=%+v", value, meta)
			}
			if _, err := reopened.Reachability(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestV2CrashProcessHelper(t *testing.T) {
	if os.Getenv("MELDBASE_V2_CRASH_HELPER") != "1" {
		return
	}
	pointValue, err := strconv.Atoi(os.Getenv("MELDBASE_V2_CRASH_POINT"))
	if err != nil {
		t.Fatal(err)
	}
	file, _, err := Open(os.Getenv("MELDBASE_V2_CRASH_PATH"))
	if err != nil {
		t.Fatal(err)
	}
	point := faultPoint(pointValue)
	file.fault = func(current faultPoint) error {
		if current == point {
			os.Exit(v2CrashHelperExitCode)
		}
		return nil
	}
	id := [16]byte{15: 1}
	_, err = file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{2}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("new"),
	}}})
	t.Fatalf("configured crash point was not reached: %v", err)
}
