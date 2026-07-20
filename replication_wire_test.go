package meldbase

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestReplicationFrameRoundTripPreservesTypedBatch(t *testing.T) {
	id := DocumentID{1}
	before := Document{"_id": ID(id), "value": Int(1), "at": Time(time.Unix(1_700_000_000, 0))}
	after := Document{"_id": ID(id), "value": Int(2), "payload": Binary([]byte{1, 2, 3})}
	batch := &DurableDatabaseChangeBatch{
		Token: 7, TransactionID: [16]byte{9}, CommittedAt: time.UnixMilli(1_700_000_000_123).UTC(),
		Changes: []Change{
			{Collection: "items", Operation: CreateCollectionOperation, ChangedPaths: []string{"_catalog"}},
			{Collection: "items", Operation: UpdateOperation, DocumentID: id, Before: &before, After: &after, ChangedPaths: []string{"payload", "value"}},
			{Collection: "items", Operation: CreateIndexOperation, Index: &IndexDefinition{Name: "by_value", Field: "value", Order: 1, Fields: []IndexField{{Field: "value", Order: 1}}, Unique: true}, ChangedPaths: []string{"_indexes.by_value"}},
		},
	}
	identity := [16]byte{7}
	encoded, err := MarshalReplicationFrame(ReplicationFrame{Type: ReplicationBatchFrame, DatabaseID: identity, Batch: batch}, ReplicationFrameLimits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalReplicationFrame(encoded, ReplicationFrameLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type != ReplicationBatchFrame || decoded.DatabaseID != identity || decoded.Batch == nil || decoded.Batch.Token != batch.Token || decoded.Batch.TransactionID != batch.TransactionID || !decoded.Batch.CommittedAt.Equal(batch.CommittedAt) || len(decoded.Batch.Changes) != len(batch.Changes) {
		t.Fatalf("decoded frame=%+v", decoded)
	}
	change := decoded.Batch.Changes[1]
	if change.Operation != UpdateOperation || change.DocumentID != id || change.Before == nil || !change.Before.Equal(before) || change.After == nil || !change.After.Equal(after) || strings.Join(change.ChangedPaths, ",") != "payload,value" {
		t.Fatalf("decoded change=%+v", change)
	}
	index := decoded.Batch.Changes[2].Index
	if index == nil || !equalIndexDefinitions(*index, *batch.Changes[2].Index) {
		t.Fatalf("decoded index=%+v", index)
	}
}

func TestReplicationFrameRejectsMalformedOrAmbiguousInput(t *testing.T) {
	identity := [16]byte{1}
	hello, err := MarshalReplicationFrame(ReplicationFrame{Type: ReplicationHelloFrame, DatabaseID: identity, AfterToken: 4, MaxBytes: 1 << 20}, ReplicationFrameLimits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{
		[]byte(`{"v":1,"type":"hello","databaseId":"01000000000000000000000000000000","afterToken":4,"maxBytes":1048576,"extra":true}`),
		[]byte(`{"v":1,"type":"hello","databaseId":"01000000000000000000000000000000","afterToken":4,"afterToken":5,"maxBytes":1048576}`),
		[]byte(strings.Replace(string(hello), `"databaseId":"01000000000000000000000000000000"`, `"databaseId":"0100000000000000000000000000000A"`, 1)),
	} {
		if _, err := UnmarshalReplicationFrame(raw, ReplicationFrameLimits{}); !errors.Is(err, ErrReplicaProtocol) {
			t.Fatalf("malformed frame %s err=%v", raw, err)
		}
	}
	if _, err := UnmarshalReplicationFrame(hello, ReplicationFrameLimits{MaxFrameBytes: 64 << 10}); err != nil {
		t.Fatalf("valid hello err=%v", err)
	}
}

func TestReplicationWireBatchDrivesFollower(t *testing.T) {
	directory := t.TempDir()
	source, err := OpenV2(directory + "/source.meld2")
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	id, err := source.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, stream, err := source.BeginV2Archive(context.Background(), "wire", directory+"/bootstrap.meld2", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	follower, err := OpenV2Follower(directory+"/bootstrap.meld2", V2Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	if _, err := source.Collection("items").UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": 2}}); err != nil {
		t.Fatal(err)
	}
	batch := receiveDurableDatabaseBatch(t, stream)
	identity := source.databaseID
	encoded, err := MarshalReplicationFrame(ReplicationFrame{Type: ReplicationBatchFrame, DatabaseID: identity, Batch: &batch}, ReplicationFrameLimits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalReplicationFrame(encoded, ReplicationFrameLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if decoded.DatabaseID != identity || decoded.Batch.Token <= bootstrap.SnapshotToken {
		t.Fatalf("decoded=%+v bootstrap=%+v", decoded, bootstrap)
	}
	if err := follower.ApplyFrame(context.Background(), decoded); err != nil {
		t.Fatal(err)
	}
	decoded.DatabaseID[0] ^= 0xff
	if err := follower.ApplyFrame(context.Background(), decoded); !errors.Is(err, ErrDatabaseIdentity) {
		t.Fatalf("mismatched replication identity err=%v", err)
	}
	document, err := follower.DB().Collection("items").FindOne(context.Background(), Filter{"_id": id})
	value, ok := document["value"].Int64()
	if err != nil || !ok || value != 2 {
		t.Fatalf("wire-applied follower document=%v err=%v", document, err)
	}
}
