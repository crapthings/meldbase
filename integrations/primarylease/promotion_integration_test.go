package primarylease_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/crapthings/meldbase"
	"github.com/crapthings/meldbase/integrations/primarylease"
)

func TestDurableACKReadinessPromotesCaughtUpFollowerAfterSafeHandoff(t *testing.T) {
	directory := t.TempDir()
	source, follower, readiness, authority, publicKey, clock := promotionFixture(t, directory)
	defer source.Close()
	defer follower.Close()
	guard, err := primarylease.NewGuard(publicKey, primarylease.GuardOptions{Owner: "writer-b", Clock: func() time.Time { return *clock }})
	if err != nil {
		t.Fatal(err)
	}
	// Reopen the follower with the real guard; the fixture's first follower is
	// intentionally unguarded so its bootstrap token can be captured first.
	bootstrap := filepath.Join(directory, "bootstrap.meld2")
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	follower, err = meldbase.OpenFollower(bootstrap, meldbase.OpenOptions{PrimaryWriteFence: guard})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	seed, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: follower.DB().DatabaseID(), Owner: "writer-a", CommitSequence: follower.DB().Stats().CommitSequence})
	if err != nil {
		t.Fatal(err)
	}
	*clock = seed.Record.NotAfter.Add(time.Second)
	fence, err := follower.Promote(context.Background(), primarylease.PromotionAuthority{Authority: authority, Owner: "writer-b", Readiness: readiness})
	if err != nil || fence.CommitSequence != seed.Record.CommitSequence || fence.Epoch == "" {
		t.Fatalf("caught-up promotion fence=%+v err=%v", fence, err)
	}
	if _, err := follower.DB().Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(2)}); err != nil {
		t.Fatalf("promoted follower write err=%v", err)
	}
}

func TestDurableACKReadinessRejectsFollowerWhenSourceAdvancedAfterACK(t *testing.T) {
	directory := t.TempDir()
	source, follower, readiness, authority, publicKey, clock := promotionFixture(t, directory)
	defer source.Close()
	defer follower.Close()
	if _, err := source.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(9)}); err != nil {
		t.Fatal(err)
	}
	guard, err := primarylease.NewGuard(publicKey, primarylease.GuardOptions{Owner: "writer-b", Clock: func() time.Time { return *clock }})
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := filepath.Join(directory, "bootstrap.meld2")
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	follower, err = meldbase.OpenFollower(bootstrap, meldbase.OpenOptions{PrimaryWriteFence: guard})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	seed, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: follower.DB().DatabaseID(), Owner: "writer-a", CommitSequence: follower.DB().Stats().CommitSequence})
	if err != nil {
		t.Fatal(err)
	}
	*clock = seed.Record.NotAfter.Add(time.Second)
	if _, err := follower.Promote(context.Background(), primarylease.PromotionAuthority{Authority: authority, Owner: "writer-b", Readiness: readiness}); !errors.Is(err, primarylease.ErrLeasePromotionReadiness) {
		t.Fatalf("stale follower promotion err=%v", err)
	}
	if _, err := follower.DB().Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(3)}); !errors.Is(err, meldbase.ErrReplicaReadOnly) {
		t.Fatalf("rejected promotion made follower writable: %v", err)
	}
}

func promotionFixture(t *testing.T, directory string) (*meldbase.DB, *meldbase.Follower, primarylease.DurableConsumerPromotionReadiness, *primarylease.Authority, ed25519.PublicKey, *time.Time) {
	t.Helper()
	source, err := meldbase.Open(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)}); err != nil {
		source.Close()
		t.Fatal(err)
	}
	consumer, err := source.CreateDurableDatabaseChanges(context.Background(), "follower-a", 0, 1)
	if err != nil {
		source.Close()
		t.Fatal(err)
	}
	batch := receiveReadinessBatch(t, consumer)
	if err := consumer.Ack(batch.Token); err != nil {
		consumer.Close()
		source.Close()
		t.Fatal(err)
	}
	consumer.Close()
	bootstrap := filepath.Join(directory, "bootstrap.meld2")
	if _, err := source.Backup(context.Background(), bootstrap); err != nil {
		source.Close()
		t.Fatal(err)
	}
	follower, err := meldbase.OpenFollower(bootstrap, meldbase.OpenOptions{})
	if err != nil {
		source.Close()
		t.Fatal(err)
	}
	if follower.DB().Stats().CommitSequence != batch.Token {
		follower.Close()
		source.Close()
		t.Fatalf("bootstrap token=%d ack=%d", follower.DB().Stats().CommitSequence, batch.Token)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		follower.Close()
		source.Close()
		t.Fatal(err)
	}
	clock := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	authority, err := primarylease.NewAuthority(primarylease.AuthorityOptions{Store: primarylease.NewMemoryStore(), PrivateKey: privateKey, Clock: func() time.Time { return clock }})
	if err != nil {
		follower.Close()
		source.Close()
		t.Fatal(err)
	}
	return source, follower, primarylease.DurableConsumerPromotionReadiness{Source: source, ConsumerName: "follower-a"}, authority, publicKey, &clock
}
