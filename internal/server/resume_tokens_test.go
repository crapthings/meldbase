package server

import (
	"testing"
	"time"

	"github.com/crapthings/meldbase"
)

func TestResumeTokenBindsSecurityContextAndExpires(t *testing.T) {
	service := newResumeTokenService([]byte("0123456789abcdef0123456789abcdef"), time.Minute)
	now := time.Unix(1_700_000_000, 0)
	service.now = func() time.Time { return now }
	database := [16]byte{1, 2, 3}
	principal := Principal{Subject: "user-1", Tenant: "tenant-1"}
	query, err := meldbase.CompileQuery(meldbase.Filter{"rank": map[string]any{"$gte": int64(2)}}, meldbase.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	token, err := service.issue(database, principal, "items", query, "policy-v1", 7)
	if err != nil {
		t.Fatal(err)
	}
	position, err := service.validate(token, database, principal, "items", query, "policy-v1")
	if err != nil || position != 7 {
		t.Fatalf("position=%d err=%v", position, err)
	}
	otherQuery, _ := meldbase.CompileQuery(meldbase.Filter{"rank": map[string]any{"$gte": int64(3)}}, meldbase.QueryOptions{})
	cases := []struct {
		name       string
		database   [16]byte
		principal  Principal
		collection string
		query      meldbase.QuerySpec
		policy     string
	}{
		{"database", [16]byte{9}, principal, "items", query, "policy-v1"},
		{"subject", database, Principal{Subject: "user-2", Tenant: "tenant-1"}, "items", query, "policy-v1"},
		{"tenant", database, Principal{Subject: "user-1", Tenant: "tenant-2"}, "items", query, "policy-v1"},
		{"collection", database, principal, "other", query, "policy-v1"},
		{"query", database, principal, "items", otherQuery, "policy-v1"},
		{"policy", database, principal, "items", query, "policy-v2"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := service.validate(token, test.database, test.principal, test.collection, test.query, test.policy); err == nil {
				t.Fatal("context-bound token was accepted")
			}
		})
	}
	service.now = func() time.Time { return now.Add(time.Minute) }
	if _, err := service.validate(token, database, principal, "items", query, "policy-v1"); err == nil {
		t.Fatal("expired token was accepted")
	}
}
