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

type promotionAuthority struct {
	fence meldbase.FollowerPromotionFence
}

func (authority promotionAuthority) AuthorizeFollowerPromotion(_ context.Context, request meldbase.FollowerPromotionRequest) (meldbase.FollowerPromotionFence, error) {
	if authority.fence.DatabaseID != request.DatabaseID || authority.fence.CommitSequence != request.CommitSequence {
		return meldbase.FollowerPromotionFence{}, errors.New("unexpected promotion request")
	}
	return authority.fence, nil
}

func TestGuardVerifiesSignedLeaseAndFailsClosed(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	databaseID := [16]byte{15: 1}
	encoded := signedCertificate(t, privateKey, primarylease.Certificate{
		DatabaseID: databaseID, Owner: "writer-a", Epoch: 7, CommitSequence: 12,
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute),
	})
	if len(encoded) > primarylease.CertificateTextLimit() || len(encoded) > 256 {
		t.Fatalf("encoded certificate length=%d", len(encoded))
	}
	guard, err := primarylease.NewGuard(publicKey, primarylease.GuardOptions{Owner: "writer-a", MaxLeaseDuration: 5 * time.Minute, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	request := meldbase.PrimaryWriteFenceRequest{DatabaseID: databaseID, NextCommitSequence: 13}
	if err := guard.ValidateV2PrimaryWrite(request); err == nil {
		t.Fatal("uninstalled guard accepted a write")
	}
	if err := guard.Install(encoded); err != nil {
		t.Fatal(err)
	}
	if err := guard.ValidateV2PrimaryWrite(request); err != nil {
		t.Fatalf("installed guard rejected next write: %v", err)
	}
	if err := guard.ValidateV2PrimaryWrite(meldbase.PrimaryWriteFenceRequest{DatabaseID: databaseID, NextCommitSequence: 12}); err == nil {
		t.Fatal("guard accepted certificate sequence again")
	}
	if err := guard.ValidateV2PrimaryWrite(meldbase.PrimaryWriteFenceRequest{DatabaseID: [16]byte{15: 2}, NextCommitSequence: 13}); err == nil {
		t.Fatal("guard accepted a different database")
	}
	tamperedSuffix := "A"
	if encoded[len(encoded)-1] == 'A' {
		tamperedSuffix = "B"
	}
	if _, err := primarylease.Parse(encoded[:len(encoded)-1]+tamperedSuffix, publicKey); !errors.Is(err, primarylease.ErrCertificate) {
		t.Fatalf("tampered certificate err=%v", err)
	}
	guard.Revoke()
	if err := guard.ValidateV2PrimaryWrite(request); err == nil {
		t.Fatal("revoked guard accepted a write")
	}
	if err := guard.Install(encoded); !errors.Is(err, primarylease.ErrLeaseEpoch) {
		t.Fatalf("revoked guard accepted replayed epoch: %v", err)
	}
	newer := signedCertificate(t, privateKey, primarylease.Certificate{
		DatabaseID: databaseID, Owner: "writer-a", Epoch: 8, CommitSequence: 12,
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute),
	})
	if err := guard.Install(newer); err != nil {
		t.Fatalf("guard rejected successor epoch: %v", err)
	}
}

func TestGuardRejectsExpiredAndWrongOwnerCertificates(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	guard, err := primarylease.NewGuard(publicKey, primarylease.GuardOptions{Owner: "writer-a", MaxLeaseDuration: 5 * time.Minute, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	expired := signedCertificate(t, privateKey, primarylease.Certificate{DatabaseID: [16]byte{1}, Owner: "writer-a", Epoch: 1, NotBefore: now.Add(-2 * time.Minute), NotAfter: now.Add(-time.Minute)})
	if err := guard.Install(expired); !errors.Is(err, primarylease.ErrLeaseExpired) {
		t.Fatalf("expired certificate err=%v", err)
	}
	wrongOwner := signedCertificate(t, privateKey, primarylease.Certificate{DatabaseID: [16]byte{1}, Owner: "writer-b", Epoch: 2, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute)})
	if err := guard.Install(wrongOwner); !errors.Is(err, primarylease.ErrLeaseOwner) {
		t.Fatalf("wrong-owner certificate err=%v", err)
	}
	defaultGuard, err := primarylease.NewGuard(publicKey, primarylease.GuardOptions{Owner: "writer-a", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	tooLong := signedCertificate(t, privateKey, primarylease.Certificate{DatabaseID: [16]byte{1}, Owner: "writer-a", Epoch: 3, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute)})
	if err := defaultGuard.Install(tooLong); !errors.Is(err, primarylease.ErrCertificate) {
		t.Fatalf("default guard accepted overlong lease: %v", err)
	}
}

func TestSignedLeaseGuardBindsFollowerPromotionAndExpiresWrites(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	clock := now
	guard, err := primarylease.NewGuard(publicKey, primarylease.GuardOptions{Owner: "writer-a", MaxLeaseDuration: 5 * time.Minute, Clock: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	source, err := meldbase.Open(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)}); err != nil {
		t.Fatal(err)
	}
	bootstrap := filepath.Join(directory, "bootstrap.meld2")
	if _, err := source.BackupV2(context.Background(), bootstrap); err != nil {
		t.Fatal(err)
	}
	follower, err := meldbase.OpenV2Follower(bootstrap, meldbase.OpenOptions{PrimaryWriteFence: guard})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	wrongSequenceCertificate := signedCertificate(t, privateKey, primarylease.Certificate{
		DatabaseID: follower.DB().DatabaseID(), Owner: "writer-a", Epoch: 8, CommitSequence: follower.DB().Stats().CommitSequence + 1,
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute),
	})
	wrongSequenceFence := meldbase.FollowerPromotionFence{DatabaseID: follower.DB().DatabaseID(), CommitSequence: follower.DB().Stats().CommitSequence, Epoch: wrongSequenceCertificate}
	if _, err := follower.Promote(context.Background(), promotionAuthority{fence: wrongSequenceFence}); !errors.Is(err, meldbase.ErrReplicaPromotionFence) {
		t.Fatalf("promotion accepted a certificate for another sequence: %v", err)
	}
	if _, err := follower.DB().Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(0)}); !errors.Is(err, meldbase.ErrReplicaReadOnly) {
		t.Fatalf("mismatched certificate made follower writable: %v", err)
	}
	issuer, err := primarylease.NewAuthority(primarylease.AuthorityOptions{
		Store: primarylease.NewMemoryStore(), PrivateKey: privateKey, Clock: func() time.Time { return clock },
	})
	if err != nil {
		t.Fatal(err)
	}
	promotionRequest := meldbase.FollowerPromotionRequest{DatabaseID: follower.DB().DatabaseID(), CommitSequence: follower.DB().Stats().CommitSequence}
	seed, err := issuer.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: promotionRequest.DatabaseID, Owner: "writer-a", CommitSequence: promotionRequest.CommitSequence})
	if err != nil {
		t.Fatal(err)
	}
	clock = seed.Record.NotAfter.Add(time.Second)
	if _, err := follower.Promote(context.Background(), primarylease.PromotionAuthority{Authority: issuer, Owner: "writer-a", Readiness: primarylease.PromotionReadinessFunc(exactPromotionReadiness)}); err != nil {
		t.Fatalf("controller-backed signed promotion err=%v", err)
	}
	if _, err := follower.DB().Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(2)}); err != nil {
		t.Fatalf("promoted signed write err=%v", err)
	}
	clock = now.Add(2 * time.Minute)
	if _, err := follower.DB().Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(3)}); !errors.Is(err, meldbase.ErrPrimaryWriteFence) {
		t.Fatalf("expired signed lease write err=%v", err)
	}
}

func TestGuardWriteCheckDoesNotAllocate(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	databaseID := [16]byte{15: 1}
	guard, err := primarylease.NewGuard(publicKey, primarylease.GuardOptions{Owner: "writer-a", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	encoded := signedCertificate(t, privateKey, primarylease.Certificate{DatabaseID: databaseID, Owner: "writer-a", Epoch: 1, NotBefore: now.Add(-time.Second), NotAfter: now.Add(time.Second)})
	if err := guard.Install(encoded); err != nil {
		t.Fatal(err)
	}
	request := meldbase.PrimaryWriteFenceRequest{DatabaseID: databaseID, NextCommitSequence: 1}
	if allocations := testing.AllocsPerRun(1_000, func() {
		if err := guard.ValidateV2PrimaryWrite(request); err != nil {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatalf("write guard allocations=%f", allocations)
	}
}

func TestAuthorityIssuesMonotonicCertificatesAndPreservesHandoffWindow(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	store := primarylease.NewMemoryStore()
	authority, err := primarylease.NewAuthority(primarylease.AuthorityOptions{
		Store: store, PrivateKey: privateKey, LeaseDuration: 10 * time.Second, MaxClockSkew: 2 * time.Second,
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{8: 1}
	first, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-a", CommitSequence: 5})
	if err != nil || first.Record.Epoch != 1 || first.Record.NotAfter != now.Add(10*time.Second) {
		t.Fatalf("first grant=%+v err=%v", first, err)
	}
	firstCertificate, err := primarylease.Parse(first.Certificate, publicKey)
	if err != nil || firstCertificate.Owner != "writer-a" || firstCertificate.Epoch != 1 || firstCertificate.CommitSequence != 5 {
		t.Fatalf("first certificate=%+v err=%v", firstCertificate, err)
	}
	if _, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-a", CommitSequence: 4}); !errors.Is(err, primarylease.ErrLeaseSequence) {
		t.Fatalf("sequence regression err=%v", err)
	}
	blocked, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-b", CommitSequence: 5})
	if !errors.Is(err, primarylease.ErrLeaseActive) || !blocked.RetryAfter.Equal(first.Record.NotAfter.Add(2*time.Second)) {
		t.Fatalf("active handoff grant=%+v err=%v", blocked, err)
	}
	renewed, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-a", CommitSequence: 6})
	if err != nil || renewed.Record.Epoch != 2 || renewed.Record.CommitSequence != 6 {
		t.Fatalf("same-owner renewal=%+v err=%v", renewed, err)
	}
	now = renewed.Record.NotAfter.Add(2 * time.Second)
	handoff, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-b", CommitSequence: 6})
	if err != nil || handoff.Record.Epoch != 3 || handoff.Record.Owner != "writer-b" {
		t.Fatalf("handoff=%+v err=%v", handoff, err)
	}
}

func TestAuthorityRevocationDoesNotShortenUnreachableOwnerLease(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	authority, err := primarylease.NewAuthority(primarylease.AuthorityOptions{
		Store: primarylease.NewMemoryStore(), PrivateKey: privateKey, LeaseDuration: 10 * time.Second, MaxClockSkew: time.Second,
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{8: 2}
	grant, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-a", CommitSequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := authority.Revoke(context.Background(), databaseID, grant.Record.Epoch)
	if err != nil || !revoked.Revoked || revoked.Owner != "" || revoked.Epoch != grant.Record.Epoch+1 || !revoked.NotAfter.Equal(grant.Record.NotAfter) {
		t.Fatalf("revoke=%+v err=%v", revoked, err)
	}
	blocked, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-b", CommitSequence: 1})
	if !errors.Is(err, primarylease.ErrLeaseActive) || !blocked.RetryAfter.Equal(grant.Record.NotAfter.Add(time.Second)) {
		t.Fatalf("revocation shortened old lease: grant=%+v err=%v", blocked, err)
	}
	now = blocked.RetryAfter
	newOwner, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-b", CommitSequence: 1})
	if err != nil || newOwner.Record.Epoch != revoked.Epoch+1 || newOwner.Record.Owner != "writer-b" {
		t.Fatalf("post-revoke handoff=%+v err=%v", newOwner, err)
	}
}

func TestPromotionAuthorityIssuesFenceForExactFollowerRequest(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	issuer, err := primarylease.NewAuthority(primarylease.AuthorityOptions{
		Store: primarylease.NewMemoryStore(), PrivateKey: privateKey, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	request := meldbase.FollowerPromotionRequest{DatabaseID: [16]byte{8: 3}, CommitSequence: 42}
	seed, err := issuer.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: request.DatabaseID, Owner: "writer-a", CommitSequence: request.CommitSequence})
	if err != nil {
		t.Fatal(err)
	}
	now = seed.Record.NotAfter.Add(time.Second)
	if _, err := (primarylease.PromotionAuthority{Authority: issuer, Owner: "writer-a"}).AuthorizeFollowerPromotion(context.Background(), request); !errors.Is(err, primarylease.ErrLeasePromotionReadiness) {
		t.Fatalf("promotion without readiness err=%v", err)
	}
	fence, err := (primarylease.PromotionAuthority{Authority: issuer, Owner: "writer-a", Readiness: primarylease.PromotionReadinessFunc(exactPromotionReadiness)}).AuthorizeFollowerPromotion(context.Background(), request)
	if err != nil || fence.DatabaseID != request.DatabaseID || fence.CommitSequence != request.CommitSequence {
		t.Fatalf("promotion fence=%+v err=%v", fence, err)
	}
	certificate, err := primarylease.Parse(fence.Epoch, publicKey)
	if err != nil || certificate.DatabaseID != request.DatabaseID || certificate.CommitSequence != request.CommitSequence || certificate.Owner != "writer-a" {
		t.Fatalf("promotion certificate=%+v err=%v", certificate, err)
	}
}

func exactPromotionReadiness(_ context.Context, request meldbase.FollowerPromotionRequest, record primarylease.LeaseRecord, exists bool) error {
	if !exists || record.DatabaseID != request.DatabaseID || record.CommitSequence != request.CommitSequence {
		return errors.New("follower position is not the controller checkpoint")
	}
	return nil
}

func TestLeaseHandoffHasNoWriteOverlapWithinDeclaredClockSkew(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	controllerClock := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	const skew = 2 * time.Second
	authority, err := primarylease.NewAuthority(primarylease.AuthorityOptions{
		Store: primarylease.NewMemoryStore(), PrivateKey: privateKey, LeaseDuration: 10 * time.Second, MaxClockSkew: skew,
		Clock: func() time.Time { return controllerClock },
	})
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{8: 4}
	oldGrant, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-a", CommitSequence: 7})
	if err != nil {
		t.Fatal(err)
	}
	oldGuard, err := primarylease.NewGuard(publicKey, primarylease.GuardOptions{Owner: "writer-a", MaxLeaseDuration: time.Minute, Clock: func() time.Time { return controllerClock.Add(-skew) }})
	if err != nil {
		t.Fatal(err)
	}
	// A slow primary waits until its local clock reaches NotBefore before it
	// installs a controller certificate. Its clock remains skewed afterwards.
	controllerClock = controllerClock.Add(skew)
	if err := oldGuard.Install(oldGrant.Certificate); err != nil {
		t.Fatal(err)
	}
	write := meldbase.PrimaryWriteFenceRequest{DatabaseID: databaseID, NextCommitSequence: 8}
	if err := oldGuard.ValidateV2PrimaryWrite(write); err != nil {
		t.Fatalf("old owner rejected before expiry: %v", err)
	}
	blocked, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-b", CommitSequence: 7})
	if !errors.Is(err, primarylease.ErrLeaseActive) || !blocked.RetryAfter.Equal(oldGrant.Record.NotAfter.Add(skew)) {
		t.Fatalf("early handoff grant=%+v err=%v", blocked, err)
	}
	controllerClock = blocked.RetryAfter
	newGrant, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-b", CommitSequence: 7})
	if err != nil {
		t.Fatal(err)
	}
	newGuard, err := primarylease.NewGuard(publicKey, primarylease.GuardOptions{Owner: "writer-b", MaxLeaseDuration: time.Minute, Clock: func() time.Time { return controllerClock.Add(skew) }}) // worst permitted fast new primary.
	if err != nil {
		t.Fatal(err)
	}
	if err := newGuard.Install(newGrant.Certificate); err != nil {
		t.Fatal(err)
	}
	if err := oldGuard.ValidateV2PrimaryWrite(write); err == nil {
		t.Fatal("slow old owner still accepted a write at new-owner handoff")
	}
	if err := newGuard.ValidateV2PrimaryWrite(write); err != nil {
		t.Fatalf("fast new owner rejected at handoff: %v", err)
	}
}

func TestAuthorityStatsExposeOnlyFixedControlPlaneOutcomes(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	authority, err := primarylease.NewAuthority(primarylease.AuthorityOptions{Store: primarylease.NewMemoryStore(), PrivateKey: privateKey, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{8: 5}
	first, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-a", CommitSequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-b", CommitSequence: 1}); !errors.Is(err, primarylease.ErrLeaseActive) {
		t.Fatalf("handoff wait err=%v", err)
	}
	if _, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-a", CommitSequence: 0}); !errors.Is(err, primarylease.ErrLeaseSequence) {
		t.Fatalf("sequence regression err=%v", err)
	}
	request := meldbase.FollowerPromotionRequest{DatabaseID: databaseID, CommitSequence: 1}
	if _, err := (primarylease.PromotionAuthority{Authority: authority, Owner: "writer-b"}).AuthorizeFollowerPromotion(context.Background(), request); !errors.Is(err, primarylease.ErrLeasePromotionReadiness) {
		t.Fatalf("missing readiness err=%v", err)
	}
	if _, err := authority.Revoke(context.Background(), databaseID, first.Record.Epoch); err != nil {
		t.Fatal(err)
	}
	stats := authority.Stats()
	if stats.GrantAttempts != 3 || stats.Granted != 1 || stats.HandoffWaits != 1 || stats.SequenceRejected != 1 || stats.PromotionAttempts != 1 || stats.PromotionReadinessRejected != 1 || stats.StoreFailures != 0 || stats.CASConflicts != 0 || stats.RevokeAttempts != 1 || stats.Revoked != 1 {
		t.Fatalf("authority stats=%+v", stats)
	}
	if zero := (*primarylease.Authority)(nil).Stats(); zero != (primarylease.AuthorityStats{}) {
		t.Fatalf("nil authority stats=%+v", zero)
	}
}

func signedCertificate(t *testing.T, privateKey ed25519.PrivateKey, certificate primarylease.Certificate) string {
	t.Helper()
	encoded, err := primarylease.Sign(certificate, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
