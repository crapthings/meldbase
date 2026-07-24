package server

import (
	"context"
	"errors"
	"testing"

	"github.com/crapthings/meldbase"
)

func TestFreezeQueryPolicyOwnsMutableAuthorizerInputs(t *testing.T) {
	constraint, err := meldbase.CompileQuery(meldbase.Filter{"workspace": "mine"}, meldbase.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	queryPaths := map[string]struct{}{"title": {}}
	aggregateFields := map[string]struct{}{"title": {}}
	resultFields := map[string]struct{}{"title": {}}
	frozen := freezeQueryPolicy(QueryPolicy{
		PolicyVersion: "policy-v1", Constraint: &constraint, MaxResults: 10,
		AllowedQueryPaths: queryPaths, AllowedAggregateFields: aggregateFields, AllowedResultFields: resultFields,
	})
	queryPaths["secret"] = struct{}{}
	aggregateFields["secret"] = struct{}{}
	resultFields["secret"] = struct{}{}
	constraint, _ = meldbase.CompileQuery(meldbase.Filter{"workspace": "other"}, meldbase.QueryOptions{})

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
		{"workspace": meldbase.String("mine"), "title": meldbase.String("visible")},
		{"workspace": meldbase.String("other"), "title": meldbase.String("hidden")},
	})
	if len(documents) != 1 {
		t.Fatalf("mutated constraint changed frozen policy: %+v", documents)
	}
	projected := project(meldbase.Document{"title": meldbase.String("visible"), "secret": meldbase.String("hidden")}, frozen)
	if _, leaked := projected["secret"]; leaked {
		t.Fatal("mutated result field changed frozen projection")
	}
	if _, allowed := frozen.AllowedAggregateFields["secret"]; allowed {
		t.Fatal("mutated aggregate field changed frozen policy")
	}
}

func TestSeekPaginationRequiresSortFieldsInResultProjection(t *testing.T) {
	query, err := meldbase.DecodeQuerySpecJSON([]byte(`{"version":1,"where":{"op":"true"},"sort":[{"path":"rank","direction":1}],"limit":2,"seek":true}`), meldbase.DefaultQueryLimits)
	if err != nil {
		t.Fatal(err)
	}
	_, err = applyPolicy(query, QueryPolicy{PolicyVersion: "policy-v1", MaxResults: 10, AllowAllQueryPaths: true, AllowedResultFields: map[string]struct{}{"title": {}}})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("error = %v, want ErrForbidden", err)
	}
	projected, err := applyPolicy(query, QueryPolicy{PolicyVersion: "policy-v1", MaxResults: 10, AllowAllQueryPaths: true, AllowedResultFields: map[string]struct{}{"rank": {}}})
	if err != nil || !projected.UsesSeekPagination() {
		t.Fatalf("query=%+v err=%v", projected, err)
	}
}

func TestQueryPolicySeparatesFilterOperatorsFromSortPaths(t *testing.T) {
	policy := QueryPolicy{
		PolicyVersion: "policy-v1", MaxResults: 10,
		AllowedFilterPaths:     map[string]struct{}{"rank": {}},
		AllowedFilterOperators: map[string]map[string]struct{}{"rank": {"eq": {}, "size": {}, "type": {}, "all": {}, "elem_match": {}}},
		AllowedSortPaths:       map[string]struct{}{"title": {}},
	}
	equality, _ := meldbase.CompileQuery(meldbase.Filter{"rank": int64(1)}, meldbase.QueryOptions{Sort: []meldbase.SortField{{Path: "title", Direction: 1}}})
	if _, err := applyPolicy(equality, policy); err != nil {
		t.Fatalf("equality query denied: %v", err)
	}
	sizeQuery, _ := meldbase.CompileQuery(meldbase.Filter{"rank": map[string]any{"$size": 0}}, meldbase.QueryOptions{})
	if _, err := applyPolicy(sizeQuery, policy); err != nil {
		t.Fatalf("size query denied: %v", err)
	}
	typeQuery, _ := meldbase.CompileQuery(meldbase.Filter{"rank": map[string]any{"$type": "array"}}, meldbase.QueryOptions{})
	if _, err := applyPolicy(typeQuery, policy); err != nil {
		t.Fatalf("type query denied: %v", err)
	}
	allQuery, _ := meldbase.CompileQuery(meldbase.Filter{"rank": map[string]any{"$all": []any{int64(1)}}}, meldbase.QueryOptions{})
	if _, err := applyPolicy(allQuery, policy); err != nil {
		t.Fatalf("all query denied: %v", err)
	}
	elemMatchQuery, _ := meldbase.CompileQuery(meldbase.Filter{"rank": map[string]any{"$elemMatch": map[string]any{"$gte": int64(1)}}}, meldbase.QueryOptions{})
	if _, err := applyPolicy(elemMatchQuery, policy); err != nil {
		t.Fatalf("elem match query denied: %v", err)
	}
	rangeQuery, _ := meldbase.CompileQuery(meldbase.Filter{"rank": map[string]any{"$gt": int64(1)}}, meldbase.QueryOptions{})
	if _, err := applyPolicy(rangeQuery, policy); !errors.Is(err, ErrForbidden) {
		t.Fatalf("range query error=%v", err)
	}
	deniedSort, _ := meldbase.CompileQuery(meldbase.Filter{"rank": int64(1)}, meldbase.QueryOptions{Sort: []meldbase.SortField{{Path: "rank", Direction: 1}}})
	if _, err := applyPolicy(deniedSort, policy); !errors.Is(err, ErrForbidden) {
		t.Fatalf("sort query error=%v", err)
	}
}

func TestProjectFieldsSupportsNestedAllowedPaths(t *testing.T) {
	projected := projectFields(meldbase.Document{"_id": meldbase.ID(meldbase.DocumentID{1}), "profile": meldbase.Object(meldbase.Document{"city": meldbase.String("Shanghai"), "secret": meldbase.String("hidden")})}, false, map[string]struct{}{"profile.city": {}})
	profile, ok := projected["profile"].ObjectValue()
	if !ok || len(profile) != 1 || !profile["city"].Equal(meldbase.String("Shanghai")) {
		t.Fatalf("projected = %+v", projected)
	}
}

func TestIntersectQueryPoliciesCanOnlyNarrowAndHonorsBothLeases(t *testing.T) {
	baseLease, _ := NewQueryPolicyLease("base-lease")
	workerLease, _ := NewQueryPolicyLease("worker-lease")
	baseConstraint, _ := meldbase.CompileQuery(meldbase.Filter{"workspace": "mine"}, meldbase.QueryOptions{})
	workerConstraint, _ := meldbase.CompileQuery(meldbase.Filter{"state": "open"}, meldbase.QueryOptions{})
	policy, err := intersectQueryPolicies(QueryPolicy{
		PolicyVersion: "base-v1", Lease: baseLease, Constraint: &baseConstraint, MaxResults: 100,
		AllowedQueryPaths:      map[string]struct{}{"state": {}, "rank": {}},
		AllowedAggregateFields: map[string]struct{}{"state": {}, "rank": {}},
		AllowedResultFields:    map[string]struct{}{"state": {}, "secret": {}},
	}, QueryPolicy{
		PolicyVersion: "worker-v1", Lease: workerLease, Constraint: &workerConstraint, MaxResults: 10,
		AllowedQueryPaths:      map[string]struct{}{"state": {}},
		AllowedAggregateFields: map[string]struct{}{"state": {}},
		AllowedResultFields:    map[string]struct{}{"state": {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if policy.MaxResults != 10 || len(policy.AllowedQueryPaths) != 1 || len(policy.AllowedAggregateFields) != 1 || len(policy.AllowedResultFields) != 1 || policy.additionalLease != workerLease {
		t.Fatalf("intersection=%+v", policy)
	}
	query, _ := meldbase.CompileQuery(meldbase.Filter{}, meldbase.QueryOptions{})
	effective, err := applyPolicy(query, policy)
	if err != nil {
		t.Fatal(err)
	}
	visible := effective.Execute([]meldbase.Document{
		{"workspace": meldbase.String("mine"), "state": meldbase.String("open")},
		{"workspace": meldbase.String("mine"), "state": meldbase.String("closed")},
		{"workspace": meldbase.String("other"), "state": meldbase.String("open")},
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
	setFields := meldbase.Document{"workspace": meldbase.String("mine")}
	insert := freezeInsertPolicy(InsertPolicy{AllowedInputFields: input, AllowedResultFields: result, SetFields: setFields})
	input["secret"], result["secret"] = struct{}{}, struct{}{}
	setFields["workspace"] = meldbase.String("other")
	if _, allowed := insert.AllowedInputFields["secret"]; allowed {
		t.Fatal("frozen insert input map changed")
	}
	if workspace, _ := insert.SetFields["workspace"].StringValue(); workspace != "mine" {
		t.Fatalf("frozen server field = %q", workspace)
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
