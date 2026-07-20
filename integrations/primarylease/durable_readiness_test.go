package primarylease_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/crapthings/meldbase/core"
	"github.com/crapthings/meldbase/integrations/primarylease"
)

func TestDurableConsumerPromotionReadinessRequiresExactDurableAckAndSourcePosition(t *testing.T) {
	source, err := meldbase.Open(filepath.Join(t.TempDir(), "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	id, err := source.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := source.CreateDurableDatabaseChanges(context.Background(), "follower-a", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	batch := receiveReadinessBatch(t, consumer)
	if err := consumer.Ack(batch.Token); err != nil {
		t.Fatal(err)
	}
	consumer.Close()
	request := meldbase.FollowerPromotionRequest{DatabaseID: source.DatabaseID(), CommitSequence: batch.Token}
	record := primarylease.LeaseRecord{DatabaseID: request.DatabaseID, Owner: "writer-a", Epoch: 1, CommitSequence: request.CommitSequence, NotAfter: time.Now().UTC().Add(time.Minute)}
	readiness := primarylease.DurableConsumerPromotionReadiness{Source: source, ConsumerName: "follower-a"}
	if err := readiness.VerifyV2FollowerPromotion(context.Background(), request, record, true); err != nil {
		t.Fatalf("exact durable acknowledgement readiness err=%v", err)
	}
	if _, err := source.Collection("items").UpdateOne(context.Background(), meldbase.Filter{"_id": id}, meldbase.Update{"$set": map[string]any{"value": 2}}); err != nil {
		t.Fatal(err)
	}
	if err := readiness.VerifyV2FollowerPromotion(context.Background(), request, record, true); !errors.Is(err, primarylease.ErrLeasePromotionReadiness) {
		t.Fatalf("source advanced past follower checkpoint err=%v", err)
	}
	if err := (primarylease.DurableConsumerPromotionReadiness{Source: source, ConsumerName: "missing"}).VerifyV2FollowerPromotion(context.Background(), request, record, true); !errors.Is(err, primarylease.ErrLeasePromotionReadiness) {
		t.Fatalf("missing consumer readiness err=%v", err)
	}
	wrongRequest := request
	wrongRequest.DatabaseID[0]++
	if err := readiness.VerifyV2FollowerPromotion(context.Background(), wrongRequest, record, true); !errors.Is(err, primarylease.ErrLeasePromotionReadiness) {
		t.Fatalf("identity mismatch readiness err=%v", err)
	}
}

func receiveReadinessBatch(t *testing.T, subscription *meldbase.DurableDatabaseChangeSubscription) meldbase.DurableDatabaseChangeBatch {
	t.Helper()
	select {
	case batch, ok := <-subscription.Batches:
		if !ok {
			t.Fatal("durable consumer closed before batch")
		}
		return batch
	case err := <-subscription.Errors:
		t.Fatalf("durable consumer err=%v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for durable consumer")
	}
	return meldbase.DurableDatabaseChangeBatch{}
}
