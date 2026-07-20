package meldbase

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

type testPromotionAuthority struct {
	fence FollowerPromotionFence
	err   error
	seen  FollowerPromotionRequest
}

type nonBindingPrimaryWriteFence struct{}

func (nonBindingPrimaryWriteFence) ValidatePrimaryWrite(PrimaryWriteFenceRequest) error { return nil }

func (authority *testPromotionAuthority) AuthorizeFollowerPromotion(_ context.Context, request FollowerPromotionRequest) (FollowerPromotionFence, error) {
	authority.seen = request
	return authority.fence, authority.err
}

func TestFollowerAppliesArchiveTailInOrderAndRejectsWrites(t *testing.T) {
	directory := t.TempDir()
	source, err := Open(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	id, err := source.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	_, tail, err := source.BeginArchive(context.Background(), "follower", filepath.Join(directory, "bootstrap.meld2"), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer tail.Close()
	follower, err := OpenFollower(filepath.Join(directory, "bootstrap.meld2"), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	query, err := CompileQuery(Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	reactive, err := follower.DB().Collection("items").SubscribeQuery(context.Background(), query, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer reactive.Close()
	if initial := receiveSnapshot(t, reactive.Snapshots); initial.Token != follower.DB().Stats().CommitSequence || len(initial.Documents) != 1 {
		t.Fatalf("follower initial reactive snapshot=%+v", initial)
	}
	if _, err := follower.DB().Collection("items").InsertOne(context.Background(), Document{"value": Int(9)}); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("follower accepted application write: %v", err)
	}

	if _, err := source.Collection("items").UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": 2}}); err != nil {
		t.Fatal(err)
	}
	update := receiveDurableDatabaseBatch(t, tail)
	if err := follower.Apply(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	if applied := receiveSnapshot(t, reactive.Snapshots); applied.Token != update.Token || len(applied.Documents) != 1 {
		t.Fatalf("follower reactive applied snapshot=%+v", applied)
	}
	if err := tail.Ack(update.Token); err != nil {
		t.Fatal(err)
	}
	if err := source.Collection("items").CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	index := receiveDurableDatabaseBatch(t, tail)
	if err := follower.Apply(context.Background(), index); err != nil {
		t.Fatal(err)
	}
	if err := tail.Ack(index.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Collection("items").UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": 3}}); err != nil {
		t.Fatal(err)
	}
	secondUpdate := receiveDurableDatabaseBatch(t, tail)
	if err := follower.Apply(context.Background(), secondUpdate); err != nil {
		t.Fatal(err)
	}
	if err := tail.Ack(secondUpdate.Token); err != nil {
		t.Fatal(err)
	}
	document, err := follower.DB().Collection("items").FindOne(context.Background(), Filter{"_id": id})
	value, valueOK := document["value"].Int64()
	if err != nil || !valueOK || value != 3 || follower.DB().Stats().CommitSequence != secondUpdate.Token {
		t.Fatalf("follower document=%v sequence=%d source=%d err=%v", document, follower.DB().Stats().CommitSequence, secondUpdate.Token, err)
	}
	if err := follower.Apply(context.Background(), secondUpdate); !errors.Is(err, ErrReplicaSequence) {
		t.Fatalf("duplicate follower batch err=%v", err)
	}
	if err := follower.Apply(context.Background(), DurableDatabaseChangeBatch{Token: secondUpdate.Token + 2, TransactionID: [16]byte{1}, CommittedAt: time.Now()}); !errors.Is(err, ErrReplicaSequence) {
		t.Fatalf("gapped follower batch err=%v", err)
	}
	// A private-only source position has no public changes. The follower stores
	// a private target marker so later public positions remain contiguous.
	if err := follower.Apply(context.Background(), DurableDatabaseChangeBatch{Token: secondUpdate.Token + 1, TransactionID: [16]byte{2}, CommittedAt: time.Now()}); err != nil {
		t.Fatalf("empty follower batch err=%v", err)
	}
	if follower.DB().Stats().CommitSequence != secondUpdate.Token+1 {
		t.Fatalf("empty follower batch sequence=%d", follower.DB().Stats().CommitSequence)
	}
}

func TestFollowerPromotionRequiresMatchingExternalFence(t *testing.T) {
	directory := t.TempDir()
	source, err := Open(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Backup(context.Background(), filepath.Join(directory, "bootstrap.meld2")); err != nil {
		t.Fatal(err)
	}
	follower, err := OpenFollower(filepath.Join(directory, "bootstrap.meld2"), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	if _, err := follower.Promote(context.Background(), nil); !errors.Is(err, ErrReplicaPromotionAuthority) {
		t.Fatalf("missing authority err=%v", err)
	}
	request := FollowerPromotionRequest{DatabaseID: follower.DB().DatabaseID(), CommitSequence: follower.DB().Stats().CommitSequence}
	withoutGuard := &testPromotionAuthority{fence: FollowerPromotionFence{DatabaseID: request.DatabaseID, CommitSequence: request.CommitSequence, Epoch: "epoch-without-local-guard"}}
	if _, err := follower.Promote(context.Background(), withoutGuard); !errors.Is(err, ErrReplicaPromotionWriteFence) {
		t.Fatalf("promotion without local write fence err=%v", err)
	}
	if withoutGuard.seen != (FollowerPromotionRequest{}) {
		t.Fatalf("authority was called before local fence preflight: %+v", withoutGuard.seen)
	}
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	follower, err = OpenFollower(filepath.Join(directory, "bootstrap.meld2"), OpenOptions{PrimaryWriteFence: nonBindingPrimaryWriteFence{}})
	if err != nil {
		t.Fatal(err)
	}
	withoutBinder := &testPromotionAuthority{fence: FollowerPromotionFence{DatabaseID: follower.DB().DatabaseID(), CommitSequence: follower.DB().Stats().CommitSequence, Epoch: "epoch-without-binder"}}
	if _, err := follower.Promote(context.Background(), withoutBinder); !errors.Is(err, ErrReplicaPromotionWriteFence) {
		t.Fatalf("promotion without fence binder err=%v", err)
	}
	if withoutBinder.seen != (FollowerPromotionRequest{}) {
		t.Fatalf("authority was called before fence-binder preflight: %+v", withoutBinder.seen)
	}
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	bindErr := errors.New("local epoch store unavailable")
	bindFailure := &recordingPrimaryWriteFence{bindErr: bindErr}
	follower, err = OpenFollower(filepath.Join(directory, "bootstrap.meld2"), OpenOptions{PrimaryWriteFence: bindFailure})
	if err != nil {
		t.Fatal(err)
	}
	bindFailureAuthority := &testPromotionAuthority{fence: FollowerPromotionFence{DatabaseID: follower.DB().DatabaseID(), CommitSequence: follower.DB().Stats().CommitSequence, Epoch: "epoch-bind-failure"}}
	if _, err := follower.Promote(context.Background(), bindFailureAuthority); !errors.Is(err, ErrReplicaPromotionFence) || !errors.Is(err, bindErr) {
		t.Fatalf("promotion with failed local bind err=%v", err)
	}
	if bound := bindFailure.boundPromotions(); len(bound) != 1 || bound[0] != bindFailureAuthority.fence {
		t.Fatalf("failed local bind did not receive authority fence: %+v", bound)
	}
	if _, err := follower.DB().Collection("items").InsertOne(context.Background(), Document{"value": Int(2)}); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("failed local bind made follower writable: %v", err)
	}
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	writeFence := &recordingPrimaryWriteFence{}
	follower, err = OpenFollower(filepath.Join(directory, "bootstrap.meld2"), OpenOptions{PrimaryWriteFence: writeFence})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	wrong := &testPromotionAuthority{fence: FollowerPromotionFence{DatabaseID: follower.DB().DatabaseID(), CommitSequence: follower.DB().Stats().CommitSequence + 1, Epoch: "epoch-1"}}
	if _, err := follower.Promote(context.Background(), wrong); !errors.Is(err, ErrReplicaPromotionFence) {
		t.Fatalf("mismatched fence err=%v", err)
	}
	if _, err := follower.DB().Collection("items").InsertOne(context.Background(), Document{"value": Int(2)}); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("mismatched fence made follower writable: %v", err)
	}
	request = FollowerPromotionRequest{DatabaseID: follower.DB().DatabaseID(), CommitSequence: follower.DB().Stats().CommitSequence}
	authority := &testPromotionAuthority{fence: FollowerPromotionFence{DatabaseID: request.DatabaseID, CommitSequence: request.CommitSequence, Epoch: "epoch-2"}}
	fence, err := follower.Promote(context.Background(), authority)
	if err != nil || fence != authority.fence || authority.seen != request {
		t.Fatalf("promotion fence=%+v seen=%+v err=%v", fence, authority.seen, err)
	}
	if bound := writeFence.boundPromotions(); len(bound) != 1 || bound[0] != authority.fence {
		t.Fatalf("promotion was not bound into local write fence: %+v", bound)
	}
	if _, err := follower.DB().Collection("items").InsertOne(context.Background(), Document{"value": Int(3)}); err != nil {
		t.Fatalf("promoted follower write err=%v", err)
	}
	if stats := follower.DB().Stats().PrimaryWriteFence; !stats.Configured || !stats.Enforced || stats.Checks != 1 || stats.Rejected != 0 {
		t.Fatalf("promoted follower fence stats=%+v", stats)
	}
	writeFence.setError(errors.New("promotion lease revoked"))
	if _, err := follower.DB().Collection("items").InsertOne(context.Background(), Document{"value": Int(4)}); !errors.Is(err, ErrPrimaryWriteFence) {
		t.Fatalf("promoted follower ignored revoked write fence err=%v", err)
	}
	if err := follower.Apply(context.Background(), DurableDatabaseChangeBatch{Token: follower.DB().Stats().CommitSequence + 1, TransactionID: [16]byte{7}, CommittedAt: time.Now()}); !errors.Is(err, ErrReplicaPromoted) {
		t.Fatalf("promoted follower accepted replication err=%v", err)
	}
}
