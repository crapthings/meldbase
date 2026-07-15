package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/crapthings/meldbase"
)

var (
	ErrUnauthenticated = errors.New("meldbase server: unauthenticated")
	ErrForbidden       = errors.New("meldbase server: forbidden")
)

type Principal struct {
	Subject string
	Tenant  string
}

type Authenticator interface {
	AuthenticateHTTP(*http.Request) (Principal, error)
}

type QueryPolicy struct {
	PolicyVersion        string
	Constraint           *meldbase.QuerySpec
	MaxResults           int
	AllowAllQueryPaths   bool
	AllowedQueryPaths    map[string]struct{}
	AllowAllResultFields bool
	AllowedResultFields  map[string]struct{}
}

type Authorizer interface {
	AuthorizeQuery(context.Context, Principal, string, meldbase.QuerySpec) (QueryPolicy, error)
	AuthorizeInsert(context.Context, Principal, string, meldbase.Document) (InsertPolicy, error)
	AuthorizeUpdate(context.Context, Principal, string, meldbase.QuerySpec, meldbase.MutationSpec) (UpdatePolicy, error)
	AuthorizeDelete(context.Context, Principal, string, meldbase.QuerySpec) (DeletePolicy, error)
}

type UpdatePolicy struct {
	QueryPolicy
	AllowAllUpdatePaths bool
	AllowedUpdatePaths  map[string]struct{}
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

func applyPolicy(query meldbase.QuerySpec, policy QueryPolicy) (meldbase.QuerySpec, error) {
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

func applyUpdatePolicy(query meldbase.QuerySpec, mutation meldbase.MutationSpec, policy UpdatePolicy) (meldbase.QuerySpec, error) {
	if !policy.AllowAllUpdatePaths {
		for _, path := range mutation.Paths() {
			if _, allowed := policy.AllowedUpdatePaths[path]; !allowed {
				return meldbase.QuerySpec{}, ErrForbidden
			}
		}
	}
	return applyMutationQueryPolicy(query, policy.QueryPolicy, policy.MaxAffected)
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
