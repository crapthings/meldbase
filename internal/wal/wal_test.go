package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReplayCheckpointFilteringAndReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.wal")
	log, records, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatal("new WAL not empty")
	}
	if err := log.Append(1, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := log.Append(2, []byte("two")); err != nil {
		t.Fatal(err)
	}
	log.Close()
	log, records, err = Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Token != 2 || string(records[0].Payload) != "two" {
		t.Fatalf("records = %+v", records)
	}
	if err := log.Reset(2); err != nil {
		t.Fatal(err)
	}
	if err := log.Append(3, []byte("three")); err != nil {
		t.Fatal(err)
	}
	log.Close()
	_, records, err = Open(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Token != 3 {
		t.Fatalf("reset records = %+v", records)
	}
}

func TestPartialTailIsDiscardedButChecksumCorruptionFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.wal")
	log, _, _ := Open(path, 0)
	if err := log.Append(1, []byte("committed")); err != nil {
		t.Fatal(err)
	}
	log.Close()
	raw, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	raw.Write([]byte("MELD"))
	raw.Close()
	log, records, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d", len(records))
	}
	log.Close()
	raw, _ = os.OpenFile(path, os.O_RDWR, 0)
	raw.WriteAt([]byte{0xff}, headerSize+1)
	raw.Close()
	if _, _, err := Open(path, 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corruption error = %v", err)
	}
}

func TestTokensMustIncrease(t *testing.T) {
	log, _, err := Open(filepath.Join(t.TempDir(), "data.wal"), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if err := log.Append(4, nil); err == nil {
		t.Fatal("accepted duplicate token")
	}
	if err := log.Append(5, nil); err != nil {
		t.Fatal(err)
	}
	if err := log.Reset(4); err == nil {
		t.Fatal("reset behind WAL")
	}
}
