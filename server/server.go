// Package server exposes Meldbase's authenticated HTTP, WebSocket realtime and
// data-only RPC transport. Protocol implementation remains isolated from the
// storage engine; applications opt into this package explicitly.
package server

import (
	"github.com/crapthings/meldbase/core"
	internal "github.com/crapthings/meldbase/internal/server"
)

type (
	Config                         = internal.Config
	Handler                        = internal.Handler
	Principal                      = internal.Principal
	Authenticator                  = internal.Authenticator
	Authorizer                     = internal.Authorizer
	QueryPolicy                    = internal.QueryPolicy
	QueryPolicyResolver            = internal.QueryPolicyResolver
	PolicyGenerationStore          = internal.PolicyGenerationStore
	DurablePolicyGenerationStore   = internal.DurablePolicyGenerationStore
	InsertPolicy                   = internal.InsertPolicy
	UpdatePolicy                   = internal.UpdatePolicy
	DeletePolicy                   = internal.DeletePolicy
	QueryPolicyLease               = internal.QueryPolicyLease
	RPCMethod                      = internal.RPCMethod
	RPCTransactionalMethod         = internal.RPCTransactionalMethod
	RPCMethodResolver              = internal.RPCMethodResolver
	RPCTransactionalMethodResolver = internal.RPCTransactionalMethodResolver
	RPCAuthorizer                  = internal.RPCAuthorizer
	RPCError                       = internal.RPCError
	RPCIdempotencyStore            = internal.RPCIdempotencyStore
	RPCIdempotencyClaim            = internal.RPCIdempotencyClaim
	RPCIdempotencyCompletion       = internal.RPCIdempotencyCompletion
	RPCIdempotencyDecision         = internal.RPCIdempotencyDecision
	RPCIdempotencyDecisionKind     = internal.RPCIdempotencyDecisionKind
	RPCIdempotencyMaintenance      = internal.RPCIdempotencyMaintenance
	DurableRPCIdempotencyStore     = internal.DurableRPCIdempotencyStore
	ServerStats                    = internal.ServerStats
	WorkerPrincipal                = internal.WorkerPrincipal
	WorkerAuthenticator            = internal.WorkerAuthenticator
	WorkerHubConfig                = internal.WorkerHubConfig
	WorkerHub                      = internal.WorkerHub
	HS256JWTAuthenticatorConfig    = internal.HS256JWTAuthenticatorConfig
	HS256JWTAuthenticator          = internal.HS256JWTAuthenticator
	RS256JWKSAuthenticatorConfig   = internal.RS256JWKSAuthenticatorConfig
	RS256JWKSAuthenticator         = internal.RS256JWKSAuthenticator
	WorkspaceAuthorizerConfig      = internal.WorkspaceAuthorizerConfig
	WorkspaceAuthorizer            = internal.WorkspaceAuthorizer
	WorkerHubStats                 = internal.WorkerHubStats
)

const (
	ProtocolVersion              = internal.ProtocolVersion
	RPCIdempotencyExecute        = internal.RPCIdempotencyExecute
	RPCIdempotencyReplayResult   = internal.RPCIdempotencyReplayResult
	RPCIdempotencyReplayError    = internal.RPCIdempotencyReplayError
	RPCIdempotencyInProgress     = internal.RPCIdempotencyInProgress
	RPCIdempotencyOutcomeUnknown = internal.RPCIdempotencyOutcomeUnknown
	RPCIdempotencyConflict       = internal.RPCIdempotencyConflict
)

var (
	ErrUnauthenticated = internal.ErrUnauthenticated
	ErrForbidden       = internal.ErrForbidden
)

func New(config Config) (*Handler, error) { return internal.New(config) }

func NewHS256JWTAuthenticator(config HS256JWTAuthenticatorConfig) (*HS256JWTAuthenticator, error) {
	return internal.NewHS256JWTAuthenticator(config)
}

func NewRS256JWKSAuthenticator(config RS256JWKSAuthenticatorConfig) (*RS256JWKSAuthenticator, error) {
	return internal.NewRS256JWKSAuthenticator(config)
}

func NewWorkspaceAuthorizer(config WorkspaceAuthorizerConfig) (*WorkspaceAuthorizer, error) {
	return internal.NewWorkspaceAuthorizer(config)
}

func NewDurableRPCIdempotencyStore(db *meldbase.DB) (DurableRPCIdempotencyStore, error) {
	return internal.NewDurableRPCIdempotencyStore(db)
}

func NewDurablePolicyGenerationStore(db *meldbase.DB) (*DurablePolicyGenerationStore, error) {
	return internal.NewDurablePolicyGenerationStore(db)
}

func NewWorkerTokenAuthenticator(token string) (WorkerAuthenticator, error) {
	return internal.NewWorkerTokenAuthenticator(token)
}

func NewWorkerHub(config WorkerHubConfig) (*WorkerHub, error) {
	return internal.NewWorkerHub(config)
}

func NewQueryPolicyLease(version string) (*QueryPolicyLease, error) {
	return internal.NewQueryPolicyLease(version)
}
