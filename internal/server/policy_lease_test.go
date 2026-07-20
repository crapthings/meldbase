package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/crapthings/meldbase/core"
)

func TestQueryPolicyLeaseRevocationLinearizesWithAuthorizedWork(t *testing.T) {
	lease, err := NewQueryPolicyLease("policy-v1")
	if err != nil {
		t.Fatal(err)
	}
	if !lease.Valid() || !lease.acquire() {
		t.Fatal("new lease is not acquirable")
	}
	revoked := make(chan error, 1)
	go func() { revoked <- lease.Revoke(context.Background()) }()
	select {
	case <-lease.Done():
	case <-time.After(time.Second):
		t.Fatal("revocation did not close Done")
	}
	select {
	case err := <-revoked:
		t.Fatalf("revocation returned before active work drained: %v", err)
	default:
	}
	if lease.Valid() || lease.acquire() {
		t.Fatal("revoked lease accepted new work")
	}
	lease.release()
	if err := <-revoked; err != nil {
		t.Fatal(err)
	}
	if err := lease.Revoke(context.Background()); err != nil {
		t.Fatalf("idempotent revoke: %v", err)
	}
}

func TestQueryPolicyLeaseCanceledWaitRemainsRevoked(t *testing.T) {
	lease, _ := NewQueryPolicyLease("policy-v1")
	if !lease.acquire() {
		t.Fatal("could not acquire lease")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := lease.Revoke(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("revoke error = %v", err)
	}
	if lease.Valid() || lease.acquire() {
		t.Fatal("canceled wait undid revocation")
	}
	lease.release()
	if err := lease.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQueryPolicyLeaseVersionValidation(t *testing.T) {
	invalid := []string{"", strings.Repeat("x", 129), string([]byte{0xff})}
	for _, version := range invalid {
		if _, err := NewQueryPolicyLease(version); !errors.Is(err, ErrInvalidPolicyLease) {
			t.Fatalf("version %q error = %v", version, err)
		}
	}
	lease, _ := NewQueryPolicyLease("policy-v1")
	query, err := meldbase.CompileQuery(meldbase.Filter{}, meldbase.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyPolicy(query, QueryPolicy{PolicyVersion: "policy-v2", Lease: lease, MaxResults: 1, AllowAllQueryPaths: true}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("mismatched lease version error = %v", err)
	}
}

func BenchmarkQueryPolicyLeaseGuard(b *testing.B) {
	action := func() error { return nil }
	b.Run("Static", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if authorized, err := underQueryPolicyLease(nil, action); !authorized || err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Revocable", func(b *testing.B) {
		lease, _ := NewQueryPolicyLease("policy-v1")
		b.ReportAllocs()
		for range b.N {
			if authorized, err := underQueryPolicyLease(lease, action); !authorized || err != nil {
				b.Fatal(err)
			}
		}
	})
}
