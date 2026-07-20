package primarylease_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/crapthings/meldbase/core"
	"github.com/crapthings/meldbase/integrations/primarylease"
)

type unrelatedRuntimeFence struct{}

func (unrelatedRuntimeFence) ValidateV2PrimaryWrite(meldbase.PrimaryWriteFenceRequest) error {
	return nil
}

func TestOpenV2PrimaryWiresOnlyTheRenewedGuard(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	authority, err := primarylease.NewAuthority(primarylease.AuthorityOptions{Store: primarylease.NewMemoryStore(), PrivateKey: privateKey, LeaseDuration: 3 * time.Second, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	client := &renewalClient{grant: func(ctx context.Context, databaseID [16]byte, sequence uint64) (primarylease.Grant, error) {
		return authority.Grant(ctx, primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-a", CommitSequence: sequence})
	}}
	runtime, err := primarylease.OpenV2Primary(filepath.Join(t.TempDir(), "primary.meld2"), primarylease.PrimaryV2Options{
		PublicKey: publicKey, GuardOptions: primarylease.GuardOptions{Owner: "writer-a", Clock: func() time.Time { return now }}, RenewalClient: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	stats := runtime.DB.Stats().PrimaryWriteFence
	if !stats.Configured || !stats.Enforced {
		t.Fatalf("primary runtime fence stats=%+v", stats)
	}
	if _, err := runtime.DB.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)}); !errors.Is(err, meldbase.ErrPrimaryWriteFence) {
		t.Fatalf("unrenewed runtime accepted write: %v", err)
	}
	if err := runtime.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.DB.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(2)}); err != nil {
		t.Fatalf("renewed runtime write err=%v", err)
	}
	if status := runtime.Guard.LeaseStatus(); !status.Installed || status.CommitSequence != 0 {
		t.Fatalf("runtime installed lease=%+v", status)
	}
	if sequences := client.sequences(); len(sequences) != 1 || sequences[0] != 0 {
		t.Fatalf("runtime renewal sequence=%v", sequences)
	}
}

func TestOpenV2PrimaryRejectsConflictingFenceOrFollower(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	client := &renewalClient{grant: func(context.Context, [16]byte, uint64) (primarylease.Grant, error) { return primarylease.Grant{}, nil }}
	for _, options := range []primarylease.PrimaryV2Options{
		{PublicKey: publicKey, GuardOptions: primarylease.GuardOptions{Owner: "writer-a"}, RenewalClient: client, OpenOptions: meldbase.OpenOptions{PrimaryWriteFence: unrelatedRuntimeFence{}}},
		{PublicKey: publicKey, GuardOptions: primarylease.GuardOptions{Owner: "writer-a"}, RenewalClient: client, OpenOptions: meldbase.OpenOptions{Follower: true}},
		{PublicKey: publicKey, GuardOptions: primarylease.GuardOptions{Owner: "writer-a"}},
	} {
		if runtime, err := primarylease.OpenV2Primary(filepath.Join(t.TempDir(), "invalid.meld2"), options); runtime != nil || !errors.Is(err, primarylease.ErrPrimaryRuntimeConfiguration) {
			t.Fatalf("invalid runtime=%v err=%v", runtime, err)
		}
	}
}
