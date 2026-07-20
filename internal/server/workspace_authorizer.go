package server

import (
	"context"
	"errors"
	"regexp"

	"github.com/crapthings/meldbase/core"
)

var workspaceIdentifier = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,127}$`)

// CollectionAccessMode defines one of the small, server-enforced generic data
// API surfaces for a collection. Modes only produce the existing policy types;
// they do not introduce a second authorization engine or client-side checks.
type CollectionAccessMode string

const (
	// CollectionAccessCollaborative allows every verified workspace member to
	// read and mutate documents in the collection. The server owns the workspace
	// field, so this mode is suitable only for genuinely collaborative data.
	CollectionAccessCollaborative CollectionAccessMode = "collaborative"
	// CollectionAccessOwner allows a principal to access only documents it owns
	// inside its verified workspace. The server owns both the workspace and owner
	// fields for inserts and makes them immutable afterwards.
	CollectionAccessOwner CollectionAccessMode = "owner"
	// CollectionAccessRPCOnly rejects every generic query and mutation. An
	// application may still expose named RPC methods with its own RPCAuthorizer.
	CollectionAccessRPCOnly CollectionAccessMode = "rpc_only"
)

// CollectionAccess declares the generic data API surface for one collection.
// OwnerField is required only for CollectionAccessOwner.
type CollectionAccess struct {
	Collection string               `json:"collection"`
	Mode       CollectionAccessMode `json:"mode"`
	OwnerField string               `json:"ownerField,omitempty"`
}

// WorkspaceAuthorizerConfig declares which collections are scoped to the
// authenticated principal's current workspace. CollectionAccess is the
// explicit form. Collections remains for compatibility and makes every listed
// collection CollectionAccessCollaborative. The workspace field is owned by
// the server: inserts set it and updates may never modify it.
type WorkspaceAuthorizerConfig struct {
	Collections      []string
	CollectionAccess []CollectionAccess
	WorkspaceField   string
	MaxResults       int
	MaxAffected      int
}

// WorkspaceAuthorizer is a data-only Authorizer for ordinary application
// collections. It is intentionally not a user or membership store; an external
// identity provider supplies Principal.Tenant from the active workspace claim.
type WorkspaceAuthorizer struct {
	collections    map[string]CollectionAccess
	workspaceField string
	maxResults     int
	maxAffected    int
}

func NewWorkspaceAuthorizer(config WorkspaceAuthorizerConfig) (*WorkspaceAuthorizer, error) {
	if !workspaceIdentifier.MatchString(config.WorkspaceField) {
		return nil, errors.New("workspace field must be a simple document field name")
	}
	if len(config.Collections) > 0 && len(config.CollectionAccess) > 0 {
		return nil, errors.New("workspace authorizer accepts collections or collection access, not both")
	}
	access := config.CollectionAccess
	if len(access) == 0 {
		access = make([]CollectionAccess, 0, len(config.Collections))
		for _, collection := range config.Collections {
			access = append(access, CollectionAccess{Collection: collection, Mode: CollectionAccessCollaborative})
		}
	}
	if len(access) == 0 || len(access) > 4096 {
		return nil, errors.New("workspace authorizer requires between one and 4096 collection access declarations")
	}
	collections := make(map[string]CollectionAccess, len(access))
	for _, rule := range access {
		if !workspaceIdentifier.MatchString(rule.Collection) {
			return nil, errors.New("workspace collection name is invalid")
		}
		if _, duplicate := collections[rule.Collection]; duplicate {
			return nil, errors.New("workspace collection access declarations must be unique")
		}
		switch rule.Mode {
		case CollectionAccessCollaborative, CollectionAccessRPCOnly:
			if rule.OwnerField != "" {
				return nil, errors.New("only owner collection access may declare an owner field")
			}
		case CollectionAccessOwner:
			if !workspaceIdentifier.MatchString(rule.OwnerField) || rule.OwnerField == config.WorkspaceField {
				return nil, errors.New("owner collection access requires a distinct simple owner field name")
			}
		default:
			return nil, errors.New("workspace collection access mode is invalid")
		}
		collections[rule.Collection] = rule
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
	rule, err := a.allow(principal, collection)
	if err != nil {
		return QueryPolicy{}, err
	}
	constraint, err := a.constraint(principal, rule)
	if err != nil {
		return QueryPolicy{}, err
	}
	return QueryPolicy{
		PolicyVersion: "workspace-" + string(rule.Mode) + "-v1", Constraint: &constraint, MaxResults: a.maxResults,
		AllowAllQueryPaths: true, AllowAllResultFields: true,
	}, nil
}

func (a *WorkspaceAuthorizer) AuthorizeInsert(_ context.Context, principal Principal, collection string, _ meldbase.Document) (InsertPolicy, error) {
	rule, err := a.allow(principal, collection)
	if err != nil {
		return InsertPolicy{}, err
	}
	setFields := meldbase.Document{a.workspaceField: meldbase.String(principal.Tenant)}
	if rule.Mode == CollectionAccessOwner {
		setFields[rule.OwnerField] = meldbase.String(principal.Subject)
	}
	return InsertPolicy{
		AllowAllInputFields:  true,
		SetFields:            setFields,
		AllowAllResultFields: true,
	}, nil
}

func (a *WorkspaceAuthorizer) AuthorizeUpdate(ctx context.Context, principal Principal, collection string, query meldbase.QuerySpec, _ meldbase.MutationSpec) (UpdatePolicy, error) {
	rule, err := a.allow(principal, collection)
	if err != nil {
		return UpdatePolicy{}, err
	}
	base, err := a.AuthorizeQuery(ctx, principal, collection, query)
	if err != nil {
		return UpdatePolicy{}, err
	}
	denied := map[string]struct{}{a.workspaceField: {}}
	if rule.Mode == CollectionAccessOwner {
		denied[rule.OwnerField] = struct{}{}
	}
	return UpdatePolicy{
		QueryPolicy: base, AllowAllUpdatePaths: true,
		DeniedUpdatePaths: denied, MaxAffected: a.maxAffected,
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

func (a *WorkspaceAuthorizer) constraint(principal Principal, rule CollectionAccess) (meldbase.QuerySpec, error) {
	filter := meldbase.Filter{a.workspaceField: principal.Tenant}
	if rule.Mode == CollectionAccessOwner {
		filter[rule.OwnerField] = principal.Subject
	}
	return meldbase.CompileQuery(filter, meldbase.QueryOptions{})
}

func (a *WorkspaceAuthorizer) allow(principal Principal, collection string) (CollectionAccess, error) {
	if principal.Subject == "" || principal.Tenant == "" {
		return CollectionAccess{}, ErrForbidden
	}
	rule, ok := a.collections[collection]
	if !ok || rule.Mode == CollectionAccessRPCOnly {
		return CollectionAccess{}, ErrForbidden
	}
	return rule, nil
}
