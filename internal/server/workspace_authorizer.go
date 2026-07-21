package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/crapthings/meldbase/core"
)

var workspaceIdentifier = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,127}$`)
var rpcMethodIdentifier = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,127}$`)

// CollectionAccessMode defines one of the small, server-enforced generic data
// API surfaces for a collection. Modes only produce the existing policy types;
// they do not introduce a second authorization engine or client-side checks.
type CollectionAccessMode string

const (
	// CollectionAccessCollaborative allows every verified workspace member to
	// read and mutate documents in the collection. The server owns the workspace
	// field, so this mode is suitable only for genuinely collaborative data.
	CollectionAccessCollaborative CollectionAccessMode = "collaborative"
	// CollectionAccessOwner allows an actor to access only documents it owns
	// inside its verified workspace. The server owns both the workspace and owner
	// fields for inserts and makes them immutable afterwards.
	CollectionAccessOwner CollectionAccessMode = "owner"
	// CollectionAccessRPCOnly rejects every generic query and mutation. An
	// application may still expose named RPC methods with its own RPCAuthorizer.
	CollectionAccessRPCOnly CollectionAccessMode = "rpc_only"
	// CollectionAccessReadOnly permits generic workspace-scoped reads and
	// subscriptions, but rejects every generic mutation. It is intended for
	// business records whose writes are named server RPC operations.
	CollectionAccessReadOnly CollectionAccessMode = "read_only"
)

// CollectionAccess declares the generic data API surface for one collection.
// OwnerField is required only for CollectionAccessOwner.
type CollectionAccess struct {
	Collection string                  `json:"collection"`
	Mode       CollectionAccessMode    `json:"mode"`
	OwnerField string                  `json:"ownerField,omitempty"`
	Fields     *CollectionAccessFields `json:"fields,omitempty"`
}

// CollectionAccessFields is an optional static field boundary for generic
// client access. A nil list allows every field for that operation; an explicit
// empty list allows none. Server-owned workspace and owner fields remain
// immutable regardless of these declarations.
type CollectionAccessFields struct {
	QueryPaths      []string `json:"queryPaths,omitempty"`
	AggregateFields []string `json:"aggregateFields,omitempty"`
	ResultFields    []string `json:"resultFields,omitempty"`
	InputFields     []string `json:"inputFields,omitempty"`
	UpdatePaths     []string `json:"updatePaths,omitempty"`
}

// WorkspaceAuthorizerConfig declares the manifest-provided collections scoped
// to the authenticated actor's current workspace. The workspace field is
// owned by the server: inserts set it and updates may never modify it.
type WorkspaceAuthorizerConfig struct {
	CollectionAccess []CollectionAccess
	WorkspaceField   string
	RPCMethods       []string
	MaxResults       int
	MaxAffected      int
}

// WorkspaceAuthorizer is a data-only Authorizer for ordinary application
// collections. It is intentionally not a user or membership store; an external
// identity provider supplies Actor.TenantID from the active workspace claim.
type WorkspaceAuthorizer struct {
	collections    map[string]workspaceCollectionAccess
	rpcMethods     map[string]struct{}
	workspaceField string
	maxResults     int
	maxAffected    int
}

type workspaceCollectionAccess struct {
	CollectionAccess
	policyVersion string
}

func NewWorkspaceAuthorizer(config WorkspaceAuthorizerConfig) (*WorkspaceAuthorizer, error) {
	if !workspaceIdentifier.MatchString(config.WorkspaceField) {
		return nil, errors.New("workspace field must be a simple document field name")
	}
	access := config.CollectionAccess
	if len(access) == 0 || len(access) > 4096 {
		return nil, errors.New("workspace authorizer requires between one and 4096 collection access declarations")
	}
	collections := make(map[string]workspaceCollectionAccess, len(access))
	for index, rule := range access {
		declaration := fmt.Sprintf("collection access[%d]", index)
		if rule.Collection != "" {
			declaration += fmt.Sprintf(" %q", rule.Collection)
		}
		if !workspaceIdentifier.MatchString(rule.Collection) {
			return nil, fmt.Errorf("%s: collection name is invalid", declaration)
		}
		if _, duplicate := collections[rule.Collection]; duplicate {
			return nil, fmt.Errorf("%s: collection declarations must be unique", declaration)
		}
		switch rule.Mode {
		case CollectionAccessCollaborative, CollectionAccessRPCOnly, CollectionAccessReadOnly:
			if rule.OwnerField != "" {
				return nil, fmt.Errorf("%s: only owner access may declare ownerField", declaration)
			}
		case CollectionAccessOwner:
			if !workspaceIdentifier.MatchString(rule.OwnerField) || rule.OwnerField == config.WorkspaceField {
				return nil, fmt.Errorf("%s: owner access requires a distinct simple ownerField", declaration)
			}
		default:
			return nil, fmt.Errorf("%s: mode is invalid", declaration)
		}
		fields, err := normalizeCollectionAccessFields(rule.Fields)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", declaration, err)
		}
		if rule.Mode == CollectionAccessRPCOnly && fields != nil {
			return nil, fmt.Errorf("%s: rpc_only access cannot declare fields", declaration)
		}
		rule.Fields = fields
		canonical, err := json.Marshal(struct {
			WorkspaceField string           `json:"workspaceField"`
			Rule           CollectionAccess `json:"rule"`
		}{WorkspaceField: config.WorkspaceField, Rule: rule})
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(canonical)
		collections[rule.Collection] = workspaceCollectionAccess{
			CollectionAccess: rule, policyVersion: "workspace-" + hex.EncodeToString(digest[:]),
		}
	}
	rpcMethods := make(map[string]struct{}, len(config.RPCMethods))
	for index, method := range config.RPCMethods {
		if !rpcMethodIdentifier.MatchString(method) {
			return nil, fmt.Errorf("RPC method[%d] is invalid", index)
		}
		if _, duplicate := rpcMethods[method]; duplicate {
			return nil, fmt.Errorf("RPC method %q is duplicated", method)
		}
		rpcMethods[method] = struct{}{}
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
		collections: collections, rpcMethods: rpcMethods, workspaceField: config.WorkspaceField,
		maxResults: config.MaxResults, maxAffected: config.MaxAffected,
	}, nil
}

func (a *WorkspaceAuthorizer) AuthorizeQuery(_ context.Context, actor Actor, collection string, _ meldbase.QuerySpec) (QueryPolicy, error) {
	rule, err := a.allow(actor, collection)
	if err != nil {
		return QueryPolicy{}, err
	}
	constraint, err := a.constraint(actor, rule)
	if err != nil {
		return QueryPolicy{}, err
	}
	allowAllQueryPaths, allowedQueryPaths := collectionAccessFieldSet(rule.Fields, func(fields *CollectionAccessFields) []string { return fields.QueryPaths })
	allowAllAggregateFields, allowedAggregateFields := collectionAccessAggregateFieldSet(rule.Fields)
	allowAllResultFields, allowedResultFields := collectionAccessFieldSet(rule.Fields, func(fields *CollectionAccessFields) []string { return fields.ResultFields })
	return QueryPolicy{
		PolicyVersion: rule.policyVersion, Constraint: &constraint, MaxResults: a.maxResults,
		AllowAllQueryPaths: allowAllQueryPaths, AllowedQueryPaths: allowedQueryPaths,
		AllowAllAggregateFields: allowAllAggregateFields, AllowedAggregateFields: allowedAggregateFields,
		AllowAllResultFields: allowAllResultFields, AllowedResultFields: allowedResultFields,
	}, nil
}

func (a *WorkspaceAuthorizer) AuthorizeInsert(_ context.Context, actor Actor, collection string, _ meldbase.Document) (InsertPolicy, error) {
	rule, err := a.allow(actor, collection)
	if err != nil {
		return InsertPolicy{}, err
	}
	if rule.Mode == CollectionAccessReadOnly {
		return InsertPolicy{}, ErrForbidden
	}
	setFields := meldbase.Document{a.workspaceField: meldbase.String(actor.TenantID)}
	if rule.Mode == CollectionAccessOwner {
		setFields[rule.OwnerField] = meldbase.String(actor.ID)
	}
	allowAllInputFields, allowedInputFields := collectionAccessFieldSet(rule.Fields, func(fields *CollectionAccessFields) []string { return fields.InputFields })
	allowAllResultFields, resultFields := collectionAccessFieldSet(rule.Fields, func(fields *CollectionAccessFields) []string { return fields.ResultFields })
	if !allowAllInputFields {
		for field := range setFields {
			allowedInputFields[field] = struct{}{}
		}
	}
	return InsertPolicy{
		AllowAllInputFields: allowAllInputFields, AllowedInputFields: allowedInputFields,
		SetFields:            setFields,
		AllowAllResultFields: allowAllResultFields, AllowedResultFields: resultFields,
	}, nil
}

func (a *WorkspaceAuthorizer) AuthorizeUpdate(ctx context.Context, actor Actor, collection string, query meldbase.QuerySpec, _ meldbase.MutationSpec) (UpdatePolicy, error) {
	rule, err := a.allow(actor, collection)
	if err != nil {
		return UpdatePolicy{}, err
	}
	if rule.Mode == CollectionAccessReadOnly {
		return UpdatePolicy{}, ErrForbidden
	}
	base, err := a.AuthorizeQuery(ctx, actor, collection, query)
	if err != nil {
		return UpdatePolicy{}, err
	}
	denied := map[string]struct{}{a.workspaceField: {}}
	if rule.Mode == CollectionAccessOwner {
		denied[rule.OwnerField] = struct{}{}
	}
	allowAllUpdatePaths, allowedUpdatePaths := collectionAccessFieldSet(rule.Fields, func(fields *CollectionAccessFields) []string { return fields.UpdatePaths })
	return UpdatePolicy{
		QueryPolicy: base, AllowAllUpdatePaths: allowAllUpdatePaths, AllowedUpdatePaths: allowedUpdatePaths,
		DeniedUpdatePaths: denied, MaxAffected: a.maxAffected,
	}, nil
}

func (a *WorkspaceAuthorizer) AuthorizeDelete(ctx context.Context, actor Actor, collection string, query meldbase.QuerySpec) (DeletePolicy, error) {
	rule, err := a.allow(actor, collection)
	if err != nil {
		return DeletePolicy{}, err
	}
	if rule.Mode == CollectionAccessReadOnly {
		return DeletePolicy{}, ErrForbidden
	}
	base, err := a.AuthorizeQuery(ctx, actor, collection, query)
	if err != nil {
		return DeletePolicy{}, err
	}
	return DeletePolicy{QueryPolicy: base, MaxAffected: a.maxAffected}, nil
}

// AuthorizeRPC accepts only exact method names declared by the manifest, for a
// verified workspace actor. The allowlist deliberately grants no role or
// record-level authority; those decisions remain in the named RPC handler.
func (a *WorkspaceAuthorizer) AuthorizeRPC(_ context.Context, actor Actor, method string) error {
	if actor.ID == "" || actor.TenantID == "" {
		return ErrForbidden
	}
	if _, allowed := a.rpcMethods[method]; !allowed {
		return ErrForbidden
	}
	return nil
}

func (a *WorkspaceAuthorizer) constraint(actor Actor, rule workspaceCollectionAccess) (meldbase.QuerySpec, error) {
	filter := meldbase.Filter{a.workspaceField: actor.TenantID}
	if rule.Mode == CollectionAccessOwner {
		filter[rule.OwnerField] = actor.ID
	}
	return meldbase.CompileQuery(filter, meldbase.QueryOptions{})
}

func (a *WorkspaceAuthorizer) allow(actor Actor, collection string) (workspaceCollectionAccess, error) {
	if actor.ID == "" || actor.TenantID == "" {
		return workspaceCollectionAccess{}, ErrForbidden
	}
	rule, ok := a.collections[collection]
	if !ok || rule.Mode == CollectionAccessRPCOnly {
		return workspaceCollectionAccess{}, ErrForbidden
	}
	return rule, nil
}

func normalizeCollectionAccessFields(fields *CollectionAccessFields) (*CollectionAccessFields, error) {
	if fields == nil {
		return nil, nil
	}
	result := &CollectionAccessFields{
		QueryPaths: cloneCollectionAccessStrings(fields.QueryPaths), AggregateFields: cloneCollectionAccessStrings(fields.AggregateFields),
		ResultFields: cloneCollectionAccessStrings(fields.ResultFields), InputFields: cloneCollectionAccessStrings(fields.InputFields),
		UpdatePaths: cloneCollectionAccessStrings(fields.UpdatePaths),
	}
	if err := validateCollectionAccessPathList(result.QueryPaths, "query"); err != nil {
		return nil, err
	}
	if err := validateCollectionAccessPathList(result.UpdatePaths, "update"); err != nil {
		return nil, err
	}
	if err := validateCollectionAccessFieldList(result.ResultFields, "result"); err != nil {
		return nil, err
	}
	if err := validateCollectionAccessFieldList(result.AggregateFields, "aggregate"); err != nil {
		return nil, err
	}
	if err := validateCollectionAccessFieldList(result.InputFields, "input"); err != nil {
		return nil, err
	}
	return result, nil
}

func cloneCollectionAccessStrings(values []string) []string {
	if values == nil {
		return nil
	}
	return append([]string{}, values...)
}

func validateCollectionAccessPathList(paths []string, kind string) error {
	if paths == nil {
		return nil
	}
	if len(paths) > 256 {
		return errors.New("collection access " + kind + " paths exceed 256 entries")
	}
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if _, duplicate := seen[path]; duplicate {
			return errors.New("collection access " + kind + " paths must be unique")
		}
		expression, _ := json.Marshal(map[string]any{"version": 1, "where": map[string]any{"op": "exists", "path": path, "value": true}})
		if _, err := meldbase.DecodeQuerySpecJSON(expression, meldbase.QueryLimits{}); err != nil || path == "_id" || strings.HasPrefix(path, "_id.") {
			return errors.New("collection access " + kind + " path is invalid")
		}
		seen[path] = struct{}{}
	}
	sort.Strings(paths)
	return nil
}

func validateCollectionAccessFieldList(fields []string, kind string) error {
	if fields == nil {
		return nil
	}
	if len(fields) > 256 {
		return errors.New("collection access " + kind + " fields exceed 256 entries")
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if _, duplicate := seen[field]; duplicate || strings.Contains(field, ".") || (meldbase.Document{field: meldbase.Null()}).Validate() != nil {
			return errors.New("collection access " + kind + " field is invalid")
		}
		seen[field] = struct{}{}
	}
	sort.Strings(fields)
	return nil
}

func collectionAccessFieldSet(fields *CollectionAccessFields, selectFields func(*CollectionAccessFields) []string) (bool, map[string]struct{}) {
	if fields == nil {
		return true, nil
	}
	values := selectFields(fields)
	if values == nil {
		return true, nil
	}
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return false, result
}

// Aggregates enumerate values and can reveal distributions a normal filtered
// read does not. Unlike the ordinary field lists, built-in manifest aggregate
// access is therefore opt-in: omitted means none, including when fields itself
// is omitted. Custom Go Authorizers may still grant AllowAllAggregateFields.
func collectionAccessAggregateFieldSet(fields *CollectionAccessFields) (bool, map[string]struct{}) {
	if fields == nil || fields.AggregateFields == nil {
		return false, map[string]struct{}{}
	}
	return false, collectionAccessStringSet(fields.AggregateFields)
}

func collectionAccessStringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
