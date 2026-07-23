package primarylease_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/crapthings/meldbase"
	"github.com/crapthings/meldbase/integrations/primarylease"
)

type renewalClient struct {
	mu    sync.Mutex
	grant func(context.Context, [16]byte, uint64) (primarylease.Grant, error)
	calls [][16]byte
	seqs  []uint64
}

func (client *renewalClient) Grant(ctx context.Context, databaseID [16]byte, sequence uint64) (primarylease.Grant, error) {
	client.mu.Lock()
	client.calls = append(client.calls, databaseID)
	client.seqs = append(client.seqs, sequence)
	grant := client.grant
	client.mu.Unlock()
	return grant(ctx, databaseID, sequence)
}

func (client *renewalClient) sequences() []uint64 {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([]uint64(nil), client.seqs...)
}

func TestRenewerInstallsCurrentCheckpointAndFailsClosedAtExpiry(t *testing.T) {
	db, guard, authority, clock := renewalFixture(t)
	defer db.Close()
	client := &renewalClient{grant: func(ctx context.Context, databaseID [16]byte, sequence uint64) (primarylease.Grant, error) {
		return authority.Grant(ctx, primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-a", CommitSequence: sequence})
	}}
	renewer, err := primarylease.NewRenewer(primarylease.RenewerOptions{DB: db, Guard: guard, Client: client, Clock: func() time.Time { return *clock }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)}); !errors.Is(err, meldbase.ErrPrimaryWriteFence) {
		t.Fatalf("closed guard write err=%v", err)
	}
	if err := renewer.Renew(context.Background()); err != nil {
		t.Fatalf("initial renewal err=%v", err)
	}
	status := guard.LeaseStatus()
	if !status.Installed || status.CommitSequence != 0 || status.Epoch != 1 {
		t.Fatalf("initial lease status=%+v", status)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(2)}); err != nil {
		t.Fatalf("renewed write err=%v", err)
	}
	if err := renewer.Renew(context.Background()); err != nil {
		t.Fatalf("post-write renewal err=%v", err)
	}
	status = guard.LeaseStatus()
	if !status.Installed || status.CommitSequence != 1 || status.Epoch != 2 {
		t.Fatalf("post-write lease status=%+v", status)
	}
	if sequences := client.sequences(); len(sequences) != 2 || sequences[0] != 0 || sequences[1] != 1 {
		t.Fatalf("renewal checkpoint sequences=%v", sequences)
	}
	client.mu.Lock()
	client.grant = func(context.Context, [16]byte, uint64) (primarylease.Grant, error) {
		return primarylease.Grant{}, errors.New("controller unavailable")
	}
	client.mu.Unlock()
	if err := renewer.Renew(context.Background()); err == nil {
		t.Fatal("failed controller renewal succeeded")
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(3)}); err != nil {
		t.Fatalf("transient renewal failure closed a still-valid lease: %v", err)
	}
	*clock = status.NotAfter
	if _, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(4)}); !errors.Is(err, meldbase.ErrPrimaryWriteFence) {
		t.Fatalf("expired lease accepted write err=%v", err)
	}
	stats := renewer.Stats()
	if stats.Attempts != 3 || stats.Succeeded != 2 || stats.Failed != 1 || stats.InstallFailed != 0 || stats.ConsecutiveFailures != 1 || stats.Running {
		t.Fatalf("renewer stats=%+v", stats)
	}
}

func TestRenewerRunCancellationRevokesLocalGuard(t *testing.T) {
	db, guard, authority, clock := renewalFixture(t)
	defer db.Close()
	called := make(chan struct{}, 1)
	client := &renewalClient{grant: func(ctx context.Context, databaseID [16]byte, sequence uint64) (primarylease.Grant, error) {
		select {
		case called <- struct{}{}:
		default:
		}
		return authority.Grant(ctx, primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-a", CommitSequence: sequence})
	}}
	renewer, err := primarylease.NewRenewer(primarylease.RenewerOptions{DB: db, Guard: guard, Client: client, Clock: func() time.Time { return *clock }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- renewer.Run(ctx) }()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("renewer did not make initial request")
	}
	deadline := time.After(time.Second)
	for !guard.LeaseStatus().Installed {
		select {
		case <-deadline:
			t.Fatal("renewer did not install its initial lease")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run exit err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("renewer did not stop after cancellation")
	}
	if status := guard.LeaseStatus(); status.Installed {
		t.Fatalf("stopped renewer left local lease installed: %+v", status)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)}); !errors.Is(err, meldbase.ErrPrimaryWriteFence) {
		t.Fatalf("stopped renewer left database writable: %v", err)
	}
	stats := renewer.Stats()
	if stats.Attempts == 0 || stats.Succeeded == 0 || stats.Running {
		t.Fatalf("stopped renewer stats=%+v", stats)
	}
}

func TestRenewerRejectsMissingFenceOrInvalidIntervals(t *testing.T) {
	db, err := meldbase.Open(filepath.Join(t.TempDir(), "unfenced.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := primarylease.NewGuard(publicKey, primarylease.GuardOptions{Owner: "writer-a"})
	if err != nil {
		t.Fatal(err)
	}
	client := &renewalClient{grant: func(context.Context, [16]byte, uint64) (primarylease.Grant, error) { return primarylease.Grant{}, nil }}
	if _, err := primarylease.NewRenewer(primarylease.RenewerOptions{DB: db, Guard: guard, Client: client}); !errors.Is(err, primarylease.ErrRenewalConfiguration) {
		t.Fatalf("unfenced DB created renewer err=%v", err)
	}
}

func renewalFixture(t *testing.T) (*meldbase.DB, *primarylease.Guard, *primarylease.Authority, *time.Time) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	guard, err := primarylease.NewGuard(publicKey, primarylease.GuardOptions{Owner: "writer-a", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	db, err := meldbase.OpenWithOptions(filepath.Join(t.TempDir(), "renew.meld2"), meldbase.OpenOptions{PrimaryWriteFence: guard})
	if err != nil {
		t.Fatal(err)
	}
	authority, err := primarylease.NewAuthority(primarylease.AuthorityOptions{Store: primarylease.NewMemoryStore(), PrivateKey: privateKey, LeaseDuration: 3 * time.Second, Clock: func() time.Time { return now }})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db, guard, authority, &now
}
