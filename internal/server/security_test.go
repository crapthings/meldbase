package server

import (
	"context"
	"errors"
	"testing"

	"github.com/crapthings/meldbase/core"
)

func TestFreezeQueryPolicyOwnsMutableAuthorizerInputs(t *testing.T) {
	constraint, err := meldbase.CompileQuery(meldbase.Filter{"tenant": "mine"}, meldbase.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	queryPaths := map[string]struct{}{"title": {}}
	resultFields := map[string]struct{}{"title": {}}
	frozen := freezeQueryPolicy(QueryPolicy{
		PolicyVersion: "policy-v1", Constraint: &constraint, MaxResults: 10,
		AllowedQueryPaths: queryPaths, AllowedResultFields: resultFields,
	})
	queryPaths["secret"] = struct{}{}
	resultFields["secret"] = struct{}{}
	constraint, _ = meldbase.CompileQuery(meldbase.Filter{"tenant": "other"}, meldbase.QueryOptions{})

	secretQuery, _ := meldbase.CompileQuery(meldbase.Filter{"secret": "value"}, meldbase.QueryOptions{})
	if _, err := applyPolicy(secretQuery, frozen); !errors.Is(err, ErrForbidden) {
		t.Fatalf("mutated query path changed frozen policy: %v", err)
	}
	all, _ := meldbase.CompileQuery(meldbase.Filter{}, meldbase.QueryOptions{})
	effective, err := applyPolicy(all, frozen)
	if err != nil {
		t.Fatal(err)
	}
	documents := effective.Execute([]meldbase.Document{
		{"tenant": meldbase.String("mine"), "title": meldbase.String("visible")},
		{"tenant": meldbase.String("other"), "title": meldbase.String("hidden")},
	})
	if len(documents) != 1 {
		t.Fatalf("mutated constraint changed frozen policy: %+v", documents)
	}
	projected := project(meldbase.Document{"title": meldbase.String("visible"), "secret": meldbase.String("hidden")}, frozen)
	if _, leaked := projected["secret"]; leaked {
		t.Fatal("mutated result field changed frozen projection")
	}
}

func TestIntersectQueryPoliciesCanOnlyNarrowAndHonorsBothLeases(t *testing.T) {
	baseLease, _ := NewQueryPolicyLease("base-lease")
	workerLease, _ := NewQueryPolicyLease("worker-lease")
	baseConstraint, _ := meldbase.CompileQuery(meldbase.Filter{"tenant": "mine"}, meldbase.QueryOptions{})
	workerConstraint, _ := meldbase.CompileQuery(meldbase.Filter{"state": "open"}, meldbase.QueryOptions{})
	policy, err := intersectQueryPolicies(QueryPolicy{
		PolicyVersion: "base-v1", Lease: baseLease, Constraint: &baseConstraint, MaxResults: 100,
		AllowedQueryPaths:   map[string]struct{}{"state": {}, "rank": {}},
		AllowedResultFields: map[string]struct{}{"state": {}, "secret": {}},
	}, QueryPolicy{
		PolicyVersion: "worker-v1", Lease: workerLease, Constraint: &workerConstraint, MaxResults: 10,
		AllowedQueryPaths:   map[string]struct{}{"state": {}},
		AllowedResultFields: map[string]struct{}{"state": {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if policy.MaxResults != 10 || len(policy.AllowedQueryPaths) != 1 || len(policy.AllowedResultFields) != 1 || policy.additionalLease != workerLease {
		t.Fatalf("intersection=%+v", policy)
	}
	query, _ := meldbase.CompileQuery(meldbase.Filter{}, meldbase.QueryOptions{})
	effective, err := applyPolicy(query, policy)
	if err != nil {
		t.Fatal(err)
	}
	visible := effective.Execute([]meldbase.Document{
		{"tenant": meldbase.String("mine"), "state": meldbase.String("open")},
		{"tenant": meldbase.String("mine"), "state": meldbase.String("closed")},
		{"tenant": meldbase.String("other"), "state": meldbase.String("open")},
	})
	if len(visible) != 1 {
		t.Fatalf("intersection visible=%+v", visible)
	}
	if err := workerLease.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if authorized, err := underQueryPolicy(policy, func() error { return nil }); authorized || err != nil {
		t.Fatalf("revoked additional lease authorized=%v err=%v", authorized, err)
	}
}

func TestFreezeMutationPoliciesOwnMapsAndDocuments(t *testing.T) {
	input := map[string]struct{}{"title": {}}
	result := map[string]struct{}{"title": {}}
	setFields := meldbase.Document{"tenant": meldbase.String("mine")}
	insert := freezeInsertPolicy(InsertPolicy{AllowedInputFields: input, AllowedResultFields: result, SetFields: setFields})
	input["secret"], result["secret"] = struct{}{}, struct{}{}
	setFields["tenant"] = meldbase.String("other")
	if _, allowed := insert.AllowedInputFields["secret"]; allowed {
		t.Fatal("frozen insert input map changed")
	}
	if tenant, _ := insert.SetFields["tenant"].StringValue(); tenant != "mine" {
		t.Fatalf("frozen server field = %q", tenant)
	}

	updates := map[string]struct{}{"title": {}}
	update := freezeUpdatePolicy(UpdatePolicy{
		QueryPolicy:        QueryPolicy{PolicyVersion: "policy-v1", MaxResults: 1, AllowAllQueryPaths: true},
		AllowedUpdatePaths: updates, MaxAffected: 1,
	})
	updates["secret"] = struct{}{}
	mutation, _ := meldbase.CompileUpdate(meldbase.Update{"$set": map[string]any{"secret": "value"}})
	query, _ := meldbase.CompileQuery(meldbase.Filter{}, meldbase.QueryOptions{})
	if _, err := applyUpdatePolicy(query, mutation, update); !errors.Is(err, ErrForbidden) {
		t.Fatalf("mutated update map changed frozen policy: %v", err)
	}

	// Compile-time assertion that testAuthorizer still satisfies the complete
	// interface after policy freezing remains at the handler boundary.
	var _ Authorizer = testAuthorizer{}
}
