// Package server gives applications authenticated HTTP, live-query, and RPC access to Meldbase.
//
// Applications opt into this package separately from the storage engine.
//
// Stability: Alpha. This Go API can change between alpha releases. The
// versioned HTTP and realtime wire contracts have their own compatibility
// policy; see https://meldbase.com/protocol-stability.
package server

import (
	"github.com/crapthings/meldbase"
	internal "github.com/crapthings/meldbase/internal/server"
)

type (
	// Config sets the database, security policies, transport limits, and optional RPC services.
	Config = internal.Config
	// Handler serves authorized database operations and manages realtime connections.
	Handler = internal.Handler
	// Actor holds the authenticated application identity and active workspace.
	Actor = internal.Actor
	// Authenticator verifies HTTP credentials and returns an application identity.
	Authenticator = internal.Authenticator
	// Authorizer supplies server-enforced document read and mutation policies.
	Authorizer = internal.Authorizer
	// QueryPolicy defines query constraints, field permissions, limits, and revocation leases.
	QueryPolicy = internal.QueryPolicy
	// QueryPolicyResolver optionally supplies an additional policy that narrows an authorized query.
	QueryPolicyResolver = internal.QueryPolicyResolver
	// PolicyGenerationStore reads a collection's persisted policy generation.
	PolicyGenerationStore = internal.PolicyGenerationStore
	// DurablePolicyGenerationStore reads policy generations from database system records.
	DurablePolicyGenerationStore = internal.DurablePolicyGenerationStore
	// InsertPolicy defines accepted input fields, server-owned values, and permitted result fields.
	InsertPolicy = internal.InsertPolicy
	// UpdatePolicy combines row-selection permissions with writable paths and an affected-document limit.
	UpdatePolicy = internal.UpdatePolicy
	// DeletePolicy combines row-selection permissions with an affected-document limit.
	DeletePolicy = internal.DeletePolicy
	// QueryPolicyLease coordinates policy revocation with authorized response delivery.
	QueryPolicyLease = internal.QueryPolicyLease
	// RPCMethod handles an authenticated application call whose effects are not atomic with its result.
	RPCMethod = internal.RPCMethod
	// RPCTransactionalMethod stages database writes that commit together with a successful RPC result.
	RPCTransactionalMethod = internal.RPCTransactionalMethod
	// RPCMethodResolver looks up dynamic methods when the local method registry has no match.
	RPCMethodResolver = internal.RPCMethodResolver
	// RPCTransactionalMethodResolver looks up dynamic transaction-aware methods.
	RPCTransactionalMethodResolver = internal.RPCTransactionalMethodResolver
	// RPCAuthorizer checks call permission before input decoding or application execution.
	RPCAuthorizer = internal.RPCAuthorizer
	// MeldbaseError carries an expected business error code and optional safe data for the caller.
	MeldbaseError = internal.MeldbaseError
	// MeldbaseInternalError carries a server-owned RPC error code and HTTP status.
	MeldbaseInternalError = internal.MeldbaseInternalError
	// RPCIdempotencyStore durably claims requests and records their outcomes through conditional transitions.
	RPCIdempotencyStore = internal.RPCIdempotencyStore
	// RPCIdempotencyClaim identifies a pending request and its ownership using hashes and tokens.
	RPCIdempotencyClaim = internal.RPCIdempotencyClaim
	// RPCIdempotencyCompletion pairs a request claim with the result or error to persist.
	RPCIdempotencyCompletion = internal.RPCIdempotencyCompletion
	// RPCIdempotencyDecision carries an execution decision or a recorded result or error.
	RPCIdempotencyDecision = internal.RPCIdempotencyDecision
	// RPCIdempotencyDecisionKind identifies whether an RPC may execute or must use a recorded outcome.
	RPCIdempotencyDecisionKind = internal.RPCIdempotencyDecisionKind
	// RPCIdempotencyMaintenance removes expired terminal records while preserving pending requests.
	RPCIdempotencyMaintenance = internal.RPCIdempotencyMaintenance
	// DurableRPCIdempotencyStore combines durable request ownership with expired-record maintenance.
	DurableRPCIdempotencyStore = internal.DurableRPCIdempotencyStore
	// ServerStats reports aggregate server activity without application identities or payloads.
	ServerStats = internal.ServerStats
	// WorkerIdentity identifies an authenticated control-plane worker.
	WorkerIdentity = internal.WorkerIdentity
	// WorkerAuthenticator verifies worker credentials separately from application credentials.
	WorkerAuthenticator = internal.WorkerAuthenticator
	// WorkerHubConfig sets worker authentication, registration limits, and dynamic policy controls.
	WorkerHubConfig = internal.WorkerHubConfig
	// WorkerHub routes dynamically registered methods from separately authenticated workers.
	WorkerHub = internal.WorkerHub
	// HS256JWTAuthenticatorConfig sets signing secrets and claim rules for HS256 token verification.
	HS256JWTAuthenticatorConfig = internal.HS256JWTAuthenticatorConfig
	// HS256JWTAuthenticator verifies bearer JWTs and maps their identity and workspace claims to an actor.
	HS256JWTAuthenticator = internal.HS256JWTAuthenticator
	// RS256JWKSAuthenticatorConfig sets the public key endpoint and claim rules for RS256 token verification.
	RS256JWKSAuthenticatorConfig = internal.RS256JWKSAuthenticatorConfig
	// RS256JWKSAuthenticator verifies bearer JWTs using cached RSA keys from a public key endpoint.
	RS256JWKSAuthenticator = internal.RS256JWKSAuthenticator
	// WorkspaceAuthorizerConfig configures collection and RPC access within an authenticated workspace.
	WorkspaceAuthorizerConfig = internal.WorkspaceAuthorizerConfig
	// WorkspaceAuthorizer enforces collection and RPC permissions within the actor's active workspace.
	WorkspaceAuthorizer = internal.WorkspaceAuthorizer
	// CollectionAccessMode identifies the generic data access mode for a collection.
	CollectionAccessMode = internal.CollectionAccessMode
	// CollectionAccess declares a collection's generic data access rules.
	CollectionAccess = internal.CollectionAccess
	// CollectionAccessFields defines optional field permissions for generic client operations.
	CollectionAccessFields = internal.CollectionAccessFields
	// CollectionAccessManifest declares generic client data access rules without executable policy callbacks.
	CollectionAccessManifest = internal.CollectionAccessManifest
	// WorkerHubStats reports worker connections, execution, protocol failures, and policy activity.
	WorkerHubStats = internal.WorkerHubStats
)

const (
	// ProtocolVersion identifies the current realtime and RPC envelope version.
	ProtocolVersion = internal.ProtocolVersion
	// RPCIdempotencyExecute grants ownership of a new pending request.
	RPCIdempotencyExecute = internal.RPCIdempotencyExecute
	// RPCIdempotencyReplayResult returns a previously recorded successful result.
	RPCIdempotencyReplayResult = internal.RPCIdempotencyReplayResult
	// RPCIdempotencyReplayError returns a previously recorded error.
	RPCIdempotencyReplayError = internal.RPCIdempotencyReplayError
	// RPCIdempotencyInProgress reports that the same request is pending in the current session.
	RPCIdempotencyInProgress = internal.RPCIdempotencyInProgress
	// RPCIdempotencyOutcomeUnknown reports an uncertain outcome that prevents safe re-execution.
	RPCIdempotencyOutcomeUnknown = internal.RPCIdempotencyOutcomeUnknown
	// RPCIdempotencyConflict reports that the key was used for a different request.
	RPCIdempotencyConflict = internal.RPCIdempotencyConflict
	// CollectionAccessCollaborative allows verified workspace members to read and mutate the collection.
	CollectionAccessCollaborative = internal.CollectionAccessCollaborative
	// CollectionAccessOwner limits access to documents the actor owns within its workspace.
	CollectionAccessOwner = internal.CollectionAccessOwner
	// CollectionAccessRPCOnly rejects generic data operations while allowing separately authorized RPC methods.
	CollectionAccessRPCOnly = internal.CollectionAccessRPCOnly
	// CollectionAccessManifestVersion identifies the supported collection access manifest format.
	CollectionAccessManifestVersion = internal.CollectionAccessManifestVersion
	// CollectionAccessManifestSchemaURL identifies the published collection access manifest schema.
	CollectionAccessManifestSchemaURL = internal.CollectionAccessManifestSchemaURL
)

var (
	// ErrUnauthenticated reports missing or invalid authentication.
	ErrUnauthenticated = internal.ErrUnauthenticated
	// ErrForbidden reports an operation rejected by access policy.
	ErrForbidden = internal.ErrForbidden
)

// New validates server settings and registers the HTTP routes.
func New(config Config) (*Handler, error) { return internal.New(config) }

// NewHS256JWTAuthenticator creates a JWT authenticator from the configured signing secrets.
func NewHS256JWTAuthenticator(config HS256JWTAuthenticatorConfig) (*HS256JWTAuthenticator, error) {
	return internal.NewHS256JWTAuthenticator(config)
}

// NewRS256JWKSAuthenticator creates a JWT authenticator that retrieves and caches public signing keys.
func NewRS256JWKSAuthenticator(config RS256JWKSAuthenticatorConfig) (*RS256JWKSAuthenticator, error) {
	return internal.NewRS256JWKSAuthenticator(config)
}

// NewWorkspaceAuthorizer creates workspace-scoped access policies from validated collection rules.
func NewWorkspaceAuthorizer(config WorkspaceAuthorizerConfig) (*WorkspaceAuthorizer, error) {
	return internal.NewWorkspaceAuthorizer(config)
}

// ParseCollectionAccessManifestJSON validates and decodes a collection access manifest.
func ParseCollectionAccessManifestJSON(data []byte) (CollectionAccessManifest, error) {
	return internal.ParseCollectionAccessManifestJSON(data)
}

// NewDurableRPCIdempotencyStore creates an RPC idempotency store backed by database system records.
func NewDurableRPCIdempotencyStore(db *meldbase.DB) (DurableRPCIdempotencyStore, error) {
	return internal.NewDurableRPCIdempotencyStore(db)
}

// NewDurablePolicyGenerationStore creates a policy generation reader backed by an open database.
func NewDurablePolicyGenerationStore(db *meldbase.DB) (*DurablePolicyGenerationStore, error) {
	return internal.NewDurablePolicyGenerationStore(db)
}

// NewWorkerTokenAuthenticator creates a constant-time bearer token authenticator for workers.
func NewWorkerTokenAuthenticator(token string) (WorkerAuthenticator, error) {
	return internal.NewWorkerTokenAuthenticator(token)
}

// NewWorkerHub validates worker settings and creates an empty worker registration hub.
func NewWorkerHub(config WorkerHubConfig) (*WorkerHub, error) {
	return internal.NewWorkerHub(config)
}

// NewQueryPolicyLease creates an active lease for a nonempty policy version.
func NewQueryPolicyLease(version string) (*QueryPolicyLease, error) {
	return internal.NewQueryPolicyLease(version)
}
