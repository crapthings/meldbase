package database

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestReplicationSourceSessionBindsHelloInFlightAndDurableAck(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	subscription, err := db.CreateDurableDatabaseChanges(context.Background(), "peer", db.Stats().CommitSequence, 2)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewReplicationSourceSession(db, subscription, ReplicationFrameLimits{})
	if err != nil {
		subscription.Close()
		t.Fatal(err)
	}
	defer session.Close()
	identity := db.databaseID
	resync, err := session.AcceptHello(ReplicationFrame{Type: ReplicationHelloFrame, DatabaseID: [16]byte{9}, AfterToken: 1, MaxBytes: 1 << 20})
	if err != nil || resync == nil || resync.Type != ReplicationResyncFrame || resync.Reason != "identity_mismatch" {
		t.Fatalf("identity hello resync=%+v err=%v", resync, err)
	}
	session.Close()

	// A resync response is terminal for that connection but does not discard the
	// named checkpoint. A freshly opened authenticated connection can resume it.
	subscription, err = db.OpenDurableDatabaseChanges(context.Background(), "peer", 2)
	if err != nil {
		t.Fatal(err)
	}
	session, err = NewReplicationSourceSession(db, subscription, ReplicationFrameLimits{})
	if err != nil {
		subscription.Close()
		t.Fatal(err)
	}
	defer session.Close()
	if resync, err := session.AcceptHello(ReplicationFrame{Type: ReplicationHelloFrame, DatabaseID: identity, AfterToken: 0, MaxBytes: 1 << 20}); err != nil || resync == nil || resync.Reason != "snapshot_required" {
		t.Fatalf("stale hello resync=%+v err=%v", resync, err)
	}
	session.Close()

	subscription, err = db.OpenDurableDatabaseChanges(context.Background(), "peer", 2)
	if err != nil {
		t.Fatal(err)
	}
	session, err = NewReplicationSourceSession(db, subscription, ReplicationFrameLimits{})
	if err != nil {
		subscription.Close()
		t.Fatal(err)
	}
	defer session.Close()
	if resync, err := session.AcceptHello(ReplicationFrame{Type: ReplicationHelloFrame, DatabaseID: identity, AfterToken: 1, MaxBytes: 1 << 20}); err != nil || resync != nil {
		t.Fatalf("valid hello resync=%+v err=%v", resync, err)
	}
	if _, err := db.Collection("items").UpdateOne(context.Background(), Filter{"value": Int(1)}, Update{"$set": map[string]any{"value": 2}}); err != nil {
		t.Fatal(err)
	}
	frame, err := session.NextFrame(context.Background())
	if err != nil || frame == nil || frame.Type != ReplicationBatchFrame || frame.Batch == nil || frame.Batch.Token != 2 {
		t.Fatalf("next frame=%+v err=%v", frame, err)
	}
	if _, err := session.NextFrame(context.Background()); !errors.Is(err, ErrReplicaProtocol) {
		t.Fatalf("second in-flight next err=%v", err)
	}
	if err := session.AcceptAck(ReplicationFrame{Type: ReplicationAckFrame, DatabaseID: identity, AckToken: 1}); !errors.Is(err, ErrReplicaProtocol) {
		t.Fatalf("stale ack err=%v", err)
	}
	if checkpoint, err := session.Checkpoint(); err != nil || checkpoint != 1 {
		t.Fatalf("checkpoint before matching ack=%d err=%v", checkpoint, err)
	}
	if err := session.AcceptAck(ReplicationFrame{Type: ReplicationAckFrame, DatabaseID: identity, AckToken: frame.Batch.Token}); err != nil {
		t.Fatal(err)
	}
	if checkpoint, err := session.Checkpoint(); err != nil || checkpoint != frame.Batch.Token {
		t.Fatalf("checkpoint after matching ack=%d err=%v", checkpoint, err)
	}
}
