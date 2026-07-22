package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"github.com/crapthings/meldbase/core"
)

var (
	ErrUnauthenticated = errors.New("meldbase server: unauthenticated")
	ErrForbidden       = errors.New("meldbase server: forbidden")
)

// Actor is the authenticated application identity for one request. ID is the
// stable user or service identifier; WorkspaceID is the active workspace
// selected by the verified credential.
type Actor struct {
	ID          string
	WorkspaceID string
}

type Authenticator interface {
	AuthenticateHTTP(*http.Request) (Actor, error)
}

type QueryPolicy struct {
	PolicyVersion           string
	Lease                   *QueryPolicyLease
	Constraint              *meldbase.QuerySpec
	MaxResults              int
	AllowAllQueryPaths      bool
	AllowedQueryPaths       map[string]struct{}
	AllowAllAggregateFields bool
	AllowedAggregateFields  map[string]struct{}
	AllowAllResultFields    bool
	AllowedResultFields     map[string]struct{}
	additionalLease         *QueryPolicyLease
	compositeLeases         bool
}

// QueryPolicyResolver adds a dynamic, data-only visibility policy after the
// application's Authorizer has allowed a query. When configured, a missing
// resolution fails closed. Implementations may never return documents; they
// only narrow row membership, query paths, result fields and result count.
type QueryPolicyResolver interface {
	ResolveQueryPolicy(context.Context, Actor, string, meldbase.QuerySpec) (QueryPolicy, bool, error)
}

type Authorizer interface {
	AuthorizeQuery(context.Context, Actor, string, meldbase.QuerySpec) (QueryPolicy, error)
	AuthorizeInsert(context.Context, Actor, string, meldbase.Document) (InsertPolicy, error)
	AuthorizeUpdate(context.Context, Actor, string, meldbase.QuerySpec, meldbase.MutationSpec) (UpdatePolicy, error)
	AuthorizeDelete(context.Context, Actor, string, meldbase.QuerySpec) (DeletePolicy, error)
}

type UpdatePolicy struct {
	QueryPolicy
	AllowAllUpdatePaths bool
	AllowedUpdatePaths  map[string]struct{}
	DeniedUpdatePaths   map[string]struct{}
	MaxAffected         int
}

type DeletePolicy struct {
	QueryPolicy
	MaxAffected int
}

type InsertPolicy struct {
	AllowAllInputFields  bool
	AllowedInputFields   map[string]struct{}
	SetFields            meldbase.Document
	AllowAllResultFields bool
	AllowedResultFields  map[string]struct{}
}

// Authorizer implementations may reuse their input maps. Freeze every returned
// policy at the trust boundary so later mutation cannot race with or silently
// change an in-flight query/subscription. QuerySpec internals are immutable, but
// the optional pointer itself is copied into server-owned storage.
func freezeQueryPolicy(policy QueryPolicy) QueryPolicy {
	policy.AllowedQueryPaths = cloneStringSet(policy.AllowedQueryPaths)
	policy.AllowedAggregateFields = cloneStringSet(policy.AllowedAggregateFields)
	policy.AllowedResultFields = cloneStringSet(policy.AllowedResultFields)
	if policy.Constraint != nil {
		constraint := *policy.Constraint
		policy.Constraint = &constraint
	}
	return policy
}

func freezeInsertPolicy(policy InsertPolicy) InsertPolicy {
	policy.AllowedInputFields = cloneStringSet(policy.AllowedInputFields)
	policy.AllowedResultFields = cloneStringSet(policy.AllowedResultFields)
	policy.SetFields = policy.SetFields.Clone()
	return policy
}

func freezeUpdatePolicy(policy UpdatePolicy) UpdatePolicy {
	policy.QueryPolicy = freezeQueryPolicy(policy.QueryPolicy)
	policy.AllowedUpdatePaths = cloneStringSet(policy.AllowedUpdatePaths)
	policy.DeniedUpdatePaths = cloneStringSet(policy.DeniedUpdatePaths)
	return policy
}

func freezeDeletePolicy(policy DeletePolicy) DeletePolicy {
	policy.QueryPolicy = freezeQueryPolicy(policy.QueryPolicy)
	return policy
}

func cloneStringSet(source map[string]struct{}) map[string]struct{} {
	if source == nil {
		return nil
	}
	result := make(map[string]struct{}, len(source))
	for value := range source {
		result[value] = struct{}{}
	}
	return result
}

func applyPolicy(query meldbase.QuerySpec, policy QueryPolicy) (meldbase.QuerySpec, error) {
	if !validPolicyVersion(policy.PolicyVersion) ||
		(policy.Lease != nil && (!validPolicyVersion(policy.Lease.Version()) || (!policy.compositeLeases && policy.Lease.Version() != policy.PolicyVersion))) ||
		(policy.additionalLease != nil && !validPolicyVersion(policy.additionalLease.Version())) {
		return meldbase.QuerySpec{}, ErrForbidden
	}
	if policy.MaxResults <= 0 || policy.MaxResults > meldbase.DefaultQueryLimits.MaxLimit {
		return meldbase.QuerySpec{}, ErrForbidden
	}
	if !policy.AllowAllQueryPaths {
		for _, path := range query.Paths() {
			if path == "_id" {
				continue
			}
			if _, allowed := policy.AllowedQueryPaths[path]; !allowed {
				return meldbase.QuerySpec{}, ErrForbidden
			}
		}
	}
	if policy.Constraint != nil {
		query = query.Constrain(*policy.Constraint)
	}
	return query.Capped(policy.MaxResults), nil
}

func intersectQueryPolicies(base, additional QueryPolicy) (QueryPolicy, error) {
	if !validPolicyVersion(base.PolicyVersion) || !validPolicyVersion(additional.PolicyVersion) {
		return QueryPolicy{}, ErrForbidden
	}
	versionInput := base.PolicyVersion + "\x00" + additional.PolicyVersion
	versionHash := sha256.Sum256([]byte(versionInput))
	result := QueryPolicy{
		PolicyVersion:           "intersection-" + hex.EncodeToString(versionHash[:]),
		MaxResults:              min(base.MaxResults, additional.MaxResults),
		AllowAllQueryPaths:      base.AllowAllQueryPaths && additional.AllowAllQueryPaths,
		AllowedQueryPaths:       intersectStringSets(base.AllowAllQueryPaths, base.AllowedQueryPaths, additional.AllowAllQueryPaths, additional.AllowedQueryPaths),
		AllowAllAggregateFields: base.AllowAllAggregateFields && additional.AllowAllAggregateFields,
		AllowedAggregateFields:  intersectStringSets(base.AllowAllAggregateFields, base.AllowedAggregateFields, additional.AllowAllAggregateFields, additional.AllowedAggregateFields),
		AllowAllResultFields:    base.AllowAllResultFields && additional.AllowAllResultFields,
		AllowedResultFields:     intersectStringSets(base.AllowAllResultFields, base.AllowedResultFields, additional.AllowAllResultFields, additional.AllowedResultFields),
		compositeLeases:         true,
	}
	if base.Constraint != nil && additional.Constraint != nil {
		constraint := base.Constraint.Constrain(*additional.Constraint)
		result.Constraint = &constraint
	} else if base.Constraint != nil {
		constraint := *base.Constraint
		result.Constraint = &constraint
	} else if additional.Constraint != nil {
		constraint := *additional.Constraint
		result.Constraint = &constraint
	}
	result.Lease = base.Lease
	if result.Lease == nil {
		result.Lease = additional.Lease
	} else if additional.Lease != nil && additional.Lease != result.Lease {
		result.additionalLease = additional.Lease
	}
	return freezeQueryPolicy(result), nil
}

func intersectStringSets(firstAll bool, first map[string]struct{}, secondAll bool, second map[string]struct{}) map[string]struct{} {
	if firstAll {
		return cloneStringSet(second)
	}
	if secondAll {
		return cloneStringSet(first)
	}
	result := make(map[string]struct{}, min(len(first), len(second)))
	for value := range first {
		if _, ok := second[value]; ok {
			result[value] = struct{}{}
		}
	}
	return result
}

func applyUpdatePolicy(query meldbase.QuerySpec, mutation meldbase.MutationSpec, policy UpdatePolicy) (meldbase.QuerySpec, error) {
	for _, path := range mutation.Paths() {
		if policyPathDenied(path, policy.DeniedUpdatePaths) {
			return meldbase.QuerySpec{}, ErrForbidden
		}
	}
	if !policy.AllowAllUpdatePaths {
		for _, path := range mutation.Paths() {
			if _, allowed := policy.AllowedUpdatePaths[path]; !allowed {
				return meldbase.QuerySpec{}, ErrForbidden
			}
		}
	}
	return applyMutationQueryPolicy(query, policy.QueryPolicy, policy.MaxAffected)
}

func policyPathDenied(path string, denied map[string]struct{}) bool {
	for prefix := range denied {
		if path == prefix || strings.HasPrefix(path, prefix+".") {
			return true
		}
	}
	return false
}

func applyDeletePolicy(query meldbase.QuerySpec, policy DeletePolicy) (meldbase.QuerySpec, error) {
	return applyMutationQueryPolicy(query, policy.QueryPolicy, policy.MaxAffected)
}

func applyMutationQueryPolicy(query meldbase.QuerySpec, policy QueryPolicy, maxAffected int) (meldbase.QuerySpec, error) {
	if maxAffected <= 0 || maxAffected > meldbase.DefaultQueryLimits.MaxLimit {
		return meldbase.QuerySpec{}, ErrForbidden
	}
	if !policy.AllowAllQueryPaths {
		for _, path := range query.Paths() {
			if path == "_id" {
				continue
			}
			if _, allowed := policy.AllowedQueryPaths[path]; !allowed {
				return meldbase.QuerySpec{}, ErrForbidden
			}
		}
	}
	if policy.Constraint != nil {
		query = query.Constrain(*policy.Constraint)
	}
	return query, nil
}

func project(document meldbase.Document, policy QueryPolicy) meldbase.Document {
	return projectFields(document, policy.AllowAllResultFields, policy.AllowedResultFields)
}

func projectInsert(document meldbase.Document, policy InsertPolicy) meldbase.Document {
	return projectFields(document, policy.AllowAllResultFields, policy.AllowedResultFields)
}

func projectFields(document meldbase.Document, allowAll bool, allowedFields map[string]struct{}) meldbase.Document {
	if allowAll {
		return document.Clone()
	}
	result := meldbase.Document{}
	if id, ok := document["_id"]; ok {
		result["_id"] = id.Clone()
	}
	for field := range allowedFields {
		if value, ok := document[field]; ok && field != "_id" {
			result[field] = value.Clone()
		}
	}
	return result
}
