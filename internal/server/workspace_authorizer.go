package server

import (
	"context"
	"errors"
	"regexp"

	"github.com/crapthings/meldbase/core"
)

var workspaceIdentifier = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,127}$`)

// WorkspaceAuthorizerConfig declares which collections are scoped to the
// authenticated principal's current workspace. The workspace field is owned by
// the server: inserts set it and updates may never modify it.
type WorkspaceAuthorizerConfig struct {
	Collections    []string
	WorkspaceField string
	MaxResults     int
	MaxAffected    int
}

// WorkspaceAuthorizer is a data-only Authorizer for ordinary application
// collections. It is intentionally not a user or membership store; an external
// identity provider supplies Principal.Tenant from the active workspace claim.
type WorkspaceAuthorizer struct {
	collections    map[string]struct{}
	workspaceField string
	maxResults     int
	maxAffected    int
}

func NewWorkspaceAuthorizer(config WorkspaceAuthorizerConfig) (*WorkspaceAuthorizer, error) {
	if !workspaceIdentifier.MatchString(config.WorkspaceField) {
		return nil, errors.New("workspace field must be a simple document field name")
	}
	if len(config.Collections) == 0 || len(config.Collections) > 4096 {
		return nil, errors.New("workspace authorizer requires between one and 4096 collections")
	}
	collections := make(map[string]struct{}, len(config.Collections))
	for _, collection := range config.Collections {
		if !workspaceIdentifier.MatchString(collection) {
			return nil, errors.New("workspace collection name is invalid")
		}
		if _, duplicate := collections[collection]; duplicate {
			return nil, errors.New("workspace collections must be unique")
		}
		collections[collection] = struct{}{}
	}
	if config.MaxResults == 0 {
		config.MaxResults = meldbase.DefaultQueryLimits.MaxLimit
	}
	if config.MaxAffected == 0 {
		config.MaxAffected = meldbase.DefaultQueryLimits.MaxLimit
	}
	if config.MaxResults < 1 || config.MaxResults > meldbase.DefaultQueryLimits.MaxLimit ||
		config.MaxAffected < 1 || config.MaxAffected > meldbase.DefaultQueryLimits.MaxLimit {
		return nil, errors.New("workspace policy limits are outside query limits")
	}
	return &WorkspaceAuthorizer{
		collections: collections, workspaceField: config.WorkspaceField,
		maxResults: config.MaxResults, maxAffected: config.MaxAffected,
	}, nil
}

func (a *WorkspaceAuthorizer) AuthorizeQuery(_ context.Context, principal Principal, collection string, _ meldbase.QuerySpec) (QueryPolicy, error) {
	constraint, err := a.constraint(principal, collection)
	if err != nil {
		return QueryPolicy{}, err
	}
	return QueryPolicy{
		PolicyVersion: "workspace-v1", Constraint: &constraint, MaxResults: a.maxResults,
		AllowAllQueryPaths: true, AllowAllResultFields: true,
	}, nil
}

func (a *WorkspaceAuthorizer) AuthorizeInsert(_ context.Context, principal Principal, collection string, _ meldbase.Document) (InsertPolicy, error) {
	if err := a.allow(principal, collection); err != nil {
		return InsertPolicy{}, err
	}
	return InsertPolicy{
		AllowAllInputFields:  true,
		SetFields:            meldbase.Document{a.workspaceField: meldbase.String(principal.Tenant)},
		AllowAllResultFields: true,
	}, nil
}

func (a *WorkspaceAuthorizer) AuthorizeUpdate(ctx context.Context, principal Principal, collection string, query meldbase.QuerySpec, _ meldbase.MutationSpec) (UpdatePolicy, error) {
	base, err := a.AuthorizeQuery(ctx, principal, collection, query)
	if err != nil {
		return UpdatePolicy{}, err
	}
	return UpdatePolicy{
		QueryPolicy: base, AllowAllUpdatePaths: true,
		DeniedUpdatePaths: map[string]struct{}{a.workspaceField: {}}, MaxAffected: a.maxAffected,
	}, nil
}

func (a *WorkspaceAuthorizer) AuthorizeDelete(ctx context.Context, principal Principal, collection string, query meldbase.QuerySpec) (DeletePolicy, error) {
	base, err := a.AuthorizeQuery(ctx, principal, collection, query)
	if err != nil {
		return DeletePolicy{}, err
	}
	return DeletePolicy{QueryPolicy: base, MaxAffected: a.maxAffected}, nil
}

// AuthorizeRPC fails closed until an application supplies an explicit
// method-level authorizer. Workspace collection membership alone must not grant
// access to trusted server methods.
func (*WorkspaceAuthorizer) AuthorizeRPC(context.Context, Principal, string) error {
	return ErrForbidden
}

func (a *WorkspaceAuthorizer) constraint(principal Principal, collection string) (meldbase.QuerySpec, error) {
	if err := a.allow(principal, collection); err != nil {
		return meldbase.QuerySpec{}, err
	}
	return meldbase.CompileQuery(meldbase.Filter{a.workspaceField: principal.Tenant}, meldbase.QueryOptions{})
}

func (a *WorkspaceAuthorizer) allow(principal Principal, collection string) error {
	if principal.Subject == "" || principal.Tenant == "" {
		return ErrForbidden
	}
	if _, ok := a.collections[collection]; !ok {
		return ErrForbidden
	}
	return nil
}
