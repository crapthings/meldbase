package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDurableCommitConsumerPersistsCheckpointAndCapsRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durable-consumer.meld2")
	file, _, _, err := OpenWithOptions(path, OpenOptions{CommitRetentionMaxCommits: 2})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := file.CreateDurableCommitConsumer("archive", 0)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if meta := file.Meta(); meta.CommitSequence != 1 {
		_ = consumer.Close()
		_ = file.Close()
		t.Fatalf("initial consumer setup did not create its private checkpoint commit: %+v", meta)
	}
	for sequence := 2; sequence <= 5; sequence++ {
		applyDurableConsumerDocument(t, file, sequence)
	}
	if root, err := file.DatabaseRoot(); err != nil || root.OldestRetainedSequence != 2 {
		_ = consumer.Close()
		_ = file.Close()
		t.Fatalf("durable consumer did not pin initial history: root=%+v err=%v", root, err)
	}
	for sequence := uint64(2); sequence <= 5; sequence++ {
		batch, err := consumer.Next(context.Background())
		if err != nil || batch.Sequence != sequence {
			_ = consumer.Close()
			_ = file.Close()
			t.Fatalf("sequence=%d batch=%+v err=%v", sequence, batch, err)
		}
	}
	if err := consumer.Ack(5); err != nil {
		_ = consumer.Close()
		_ = file.Close()
		t.Fatal(err)
	}
	if consumer.Checkpoint() != 5 || file.Meta().CommitSequence != 5 {
		_ = consumer.Close()
		_ = file.Close()
		t.Fatalf("checkpoint=%d meta=%+v", consumer.Checkpoint(), file.Meta())
	}
	if err := consumer.Close(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	file, _, _, err = OpenWithOptions(path, OpenOptions{CommitRetentionMaxCommits: 2})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err = file.OpenDurableCommitConsumer("archive")
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if consumer.Checkpoint() != 5 {
		_ = consumer.Close()
		_ = file.Close()
		t.Fatalf("reopened checkpoint=%d", consumer.Checkpoint())
	}
	applyDurableConsumerDocument(t, file, 6)
	if root, err := file.DatabaseRoot(); err != nil || root.OldestRetainedSequence != 5 {
		_ = consumer.Close()
		_ = file.Close()
		t.Fatalf("ack checkpoint did not bound retention: root=%+v err=%v", root, err)
	}
	batch, err := consumer.Next(context.Background())
	if err != nil || batch.Sequence != 6 {
		_ = consumer.Close()
		_ = file.Close()
		t.Fatalf("resumed batch=%+v err=%v", batch, err)
	}
	if err := consumer.Ack(6); err != nil {
		_ = consumer.Close()
		_ = file.Close()
		t.Fatal(err)
	}
	if err := consumer.Close(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableCommitConsumerRejectsUnsafePositionsAndAckSkips(t *testing.T) {
	file, _, _, err := OpenWithOptions(filepath.Join(t.TempDir(), "durable-consumer-errors.meld2"), OpenOptions{CommitRetentionMaxCommits: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for sequence := 1; sequence <= 4; sequence++ {
		applyDurableConsumerDocument(t, file, sequence)
	}
	if _, err := file.CreateDurableCommitConsumer("too-old", 0); !errors.Is(err, ErrHistoryLost) {
		t.Fatalf("too-old create error=%v", err)
	}
	consumer, err := file.CreateDurableCommitConsumer("worker", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if err := consumer.Ack(3); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ack before delivery error=%v", err)
	}
	batch, err := consumer.Next(context.Background())
	if err != nil || batch.Sequence != 3 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	if err := consumer.Ack(3); err != nil {
		t.Fatal(err)
	}
	if err := file.DeleteDurableCommitConsumer("worker"); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Ack(3); !errors.Is(err, ErrDurableConsumerNotFound) {
		t.Fatalf("deleted consumer ack error=%v", err)
	}
}

func TestDurableCommitConsumerAckDoesNotWaitForNextBlockingRead(t *testing.T) {
	file, _, _, err := OpenWithOptions(filepath.Join(t.TempDir(), "durable-consumer-ack-concurrency.meld2"), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	applyDurableConsumerDocument(t, file, 1)
	consumer, err := file.CreateDurableCommitConsumer("peer", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	applyDurableConsumerDocument(t, file, 2)
	batch, err := consumer.Next(context.Background())
	if err != nil || batch.Sequence != 2 {
		t.Fatalf("first batch=%+v err=%v", batch, err)
	}
	// Model the subscription pump: it has immediately started waiting for the
	// next batch while another goroutine persists the ACK for the delivered one.
	waitContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	nextDone := make(chan error, 1)
	go func() {
		_, err := consumer.Next(waitContext)
		nextDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
	ackDone := make(chan error, 1)
	go func() { ackDone <- consumer.Ack(2) }()
	select {
	case err := <-ackDone:
		if err != nil {
			t.Fatalf("ack err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ack was blocked by an idle Next call")
	}
	cancel()
	if err := <-nextDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked next err=%v", err)
	}
}

func TestDurableCommitConsumerAckFaultRecoversOldOrNewCheckpoint(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "durable-consumer-ack-base.meld2")
	base, _, err := Open(basePath)
	if err != nil {
		t.Fatal(err)
	}
	applyDurableConsumerDocument(t, base, 1)
	consumer, err := base.CreateDurableCommitConsumer("archive", 1)
	if err != nil {
		_ = base.Close()
		t.Fatal(err)
	}
	applyDurableConsumerDocument(t, base, 2)
	if _, err := consumer.Next(context.Background()); err != nil {
		_ = consumer.Close()
		_ = base.Close()
		t.Fatal(err)
	}
	if err := consumer.Close(); err != nil {
		_ = base.Close()
		t.Fatal(err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	bytes, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, point := range []faultPoint{faultAfterPageWrite, faultBeforeDataSync, faultAfterDataSync, faultAfterMetaWrite, faultAfterMetaSync} {
		t.Run(fmt.Sprint(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "candidate.meld2")
			if err := os.WriteFile(path, bytes, 0o600); err != nil {
				t.Fatal(err)
			}
			candidate, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			consumer, err := candidate.OpenDurableCommitConsumer("archive")
			if err != nil {
				_ = candidate.Close()
				t.Fatal(err)
			}
			batch, err := consumer.Next(context.Background())
			if err != nil || batch.Sequence != 2 {
				_ = consumer.Close()
				_ = candidate.Close()
				t.Fatalf("batch=%+v err=%v", batch, err)
			}
			injected := errors.New("durable checkpoint fault")
			candidate.fault = func(current faultPoint) error {
				if current == point {
					return injected
				}
				return nil
			}
			if err := consumer.Ack(2); !errors.Is(err, injected) {
				_ = consumer.Close()
				_ = candidate.Close()
				t.Fatalf("ack error=%v", err)
			}
			if err := consumer.Close(); err != nil {
				_ = candidate.Close()
				t.Fatal(err)
			}
			if err := candidate.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, meta, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if meta.CommitSequence != 2 {
				t.Fatalf("ack control update changed logical sequence: %+v", meta)
			}
			consumer, err = reopened.OpenDurableCommitConsumer("archive")
			if err != nil {
				t.Fatal(err)
			}
			checkpoint := consumer.Checkpoint()
			if err := consumer.Close(); err != nil || (checkpoint != 1 && checkpoint != 2) {
				t.Fatalf("recovered checkpoint=%d close=%v", checkpoint, err)
			}
			if _, err := reopened.Reachability(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func applyDurableConsumerDocument(t *testing.T, file *File, sequence int) {
	t.Helper()
	id := [16]byte{byte(sequence), 1}
	got, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: [16]byte{byte(sequence), 2},
		Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte{byte(sequence)},
		}},
	})
	if err != nil || got != uint64(sequence) {
		t.Fatalf("sequence=%d got=%d err=%v", sequence, got, err)
	}
}
