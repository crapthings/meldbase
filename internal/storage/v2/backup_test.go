package v2

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
)

type cancelAfterFirstWrite struct {
	cancel context.CancelFunc
	writes int
}

func (writer *cancelAfterFirstWrite) Write(value []byte) (int, error) {
	writer.writes++
	if writer.writes == 1 {
		writer.cancel()
	}
	return len(value), nil
}

type shortBackupWriter struct{}

func (shortBackupWriter) Write(value []byte) (int, error) { return len(value) - 1, nil }

func TestCopyPhysicalToContextCancelsBetweenBoundedChunksAndRejectsShortWrite(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "copy-source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	mutations := make([]DocumentMutation, 128)
	for index := range mutations {
		id := [16]byte{0xa1, 14: byte(index >> 8), 15: byte(index)}
		mutations[index] = DocumentMutation{
			Collection: "items", DocumentID: id, Operation: DocumentInsert,
			Document: bytes.Repeat([]byte{byte(index + 1)}, inlineDocumentLimit+1024),
		}
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: mutations}); err != nil {
		t.Fatal(err)
	}
	if file.Meta().PhysicalPageCount*PageSize <= 1024*1024 {
		t.Fatalf("copy fixture is only %d pages", file.Meta().PhysicalPageCount)
	}
	ctx, cancel := context.WithCancel(context.Background())
	writer := &cancelAfterFirstWrite{cancel: cancel}
	if _, err := file.CopyPhysicalToContext(ctx, writer); !errors.Is(err, context.Canceled) || writer.writes != 1 {
		t.Fatalf("cancel copy writes=%d err=%v", writer.writes, err)
	}
	if _, err := file.CopyPhysicalToContext(context.Background(), shortBackupWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short copy error=%v", err)
	}
}
