# Security and workspace isolation

This document describes the enforced single-node server boundary. It does not
turn the alpha server into a hosted multi-tenant service: key rotation and
per-token revocation remain deployment responsibilities or future work.

## Identity and transport

`meld serve` requires either an explicit development-only `--dev-no-auth` mode
or a JWT authenticator. Production single-node deployments use HS256 with a
private 32+ byte secret file and require an exact issuer, audience, expiration,
subject, and workspace claim. Tokens with a missing or expired claim, wrong
issuer/audience, malformed signature, or absent workspace are rejected before
data access.

Keep the database listener on loopback and terminate TLS in an
application-owned reverse proxy. Configure exact HTTP origins and matching
WebSocket origin patterns in the service environment; do not use wildcard
origins. See [Single-node deployment](/production-single-node) for the systemd
layout and proxy boundary.

HS256 supports a bounded rotation overlap. Keep the new signing secret in
`--jwt-hs256-secret-file` and set the old one with
`--jwt-hs256-previous-secret-file`; both must contain at least 32 bytes and are
verified without selecting a key from untrusted JWT metadata. The library API
also permits up to four distinct active secrets for a controlled integration,
but the supplied systemd launcher deliberately exposes one previous secret.

Rotate by deploying the new primary plus old previous secret, switching the
identity provider to the new primary, waiting for the maximum token lifetime
plus clock-skew window, then removing the previous-secret setting and restarting
once more. Keep issued token lifetimes bounded. This is an overlap mechanism,
not built-in per-token revocation: for immediate revocation, use an OIDC/JWKS
identity provider or a revocation-aware gateway ahead of Meldbase.

## OIDC / JWKS identity providers

For an identity provider that issues RS256 OIDC-style access tokens, configure
the provider's **JWKS endpoint**, expected issuer, API audience, and a signed
workspace claim. Meldbase does not perform an interactive OAuth/OIDC login,
create users, or choose an active organization: the application or identity
provider must complete that flow before a bearer token reaches the data API.

```sh
meld serve --db /srv/meldbase/data/app.meld \
  --jwt-jwks-url https://identity.example/.well-known/jwks.json \
  --jwt-issuer https://identity.example/ \
  --jwt-audience meldbase-api \
  --jwt-workspace-claim active_workspace \
  --access-policy-file /etc/meldbase/access-policy.json
```

The JWKS URL must be an absolute HTTPS URL; issuer and audience remain exact
checks. Tokens require `sub`, a future `exp`, and the configured workspace
claim. The claim must be a scalar, non-reserved identifier compatible with the
server's workspace policy. Set `--jwt-workspace-claim` only when the provider
cannot issue the default `workspace_id`; do not derive a workspace from an
unverified email, display name, or client-supplied request value.

The supplied systemd launcher supports either configuration. Set
`MELDBASE_JWT_JWKS_URL` and leave both HS256 secret variables blank for JWKS,
or set the HS256 secret (and optional previous secret) and leave the JWKS URL
blank. It fails closed when both or neither authentication modes are set.

For providers that rotate signing keys, keep their JWKS endpoint reachable from
the server through the controlled egress/TLS policy. Meldbase caches validated
keys for a bounded interval and refreshes on an unknown key ID; an unavailable
provider can therefore reject a newly rotated key until it becomes reachable.
Monitor the provider separately and rehearse key rotation before changing the
issuer configuration.

## Workspace and field boundary

The collection access manifest is a versioned, strict JSON configuration. It
names the server-owned workspace field and explicitly declares every exposed
collection. Generic data access is denied when a collection is not present.

For every authenticated REST query, count, group-count, insert, update, delete,
realtime ticket, and realtime subscription, the server derives the workspace
from the verified identity. The client cannot select it. Inserts overwrite the
workspace field; updates cannot modify it; owner-mode collections also protect
the server-owned owner field. Realtime tickets and resume tokens remain bound to
the actor and workspace that created them.

Field policy is independent across query paths, input fields, update paths,
result fields, and aggregate/group fields. Sorting, filters, grouping, and
returned documents are validated against their relevant allowlists. Aggregate
fields are deny-by-default in the built-in manifest because distributions can
reveal data that ordinary result projection does not.

Validate a candidate manifest before deployment and retain its reviewed source:

```sh
meld access-policy validate --file /etc/meldbase/access-policy.json
meld access-policy explain --file /etc/meldbase/access-policy.json \
  --actor-id user-a --workspace-id team-a --collection tasks
```

The second command is a policy explanation tool; it is not an authorization
bypass and should be run only by an operator with access to the manifest.

## Resource and diagnostic boundaries

The HTTP server bounds request bodies, JSON shape/depth, query work, result
bytes, realtime frame bytes, outbound realtime buffering, subscriptions per
connection, and concurrent RPC work. Query execution has independent document,
physical-key, sort, predicate, and retained-candidate budgets. A rejected query
uses a stable error code rather than returning engine details.

The server can enforce an optional, bounded per-client token bucket with
`--rate-limit-per-minute`, `--rate-limit-burst`, and
`--rate-limit-subjects`. It is disabled by default. A direct client is keyed by
its TCP peer address. To use it behind a proxy, set
`--trusted-proxy-cidrs` only to proxy networks that overwrite
`X-Forwarded-For`; forwarded headers from every other peer are ignored. Health
and readiness probes remain exempt. Keep an outer rate limiter and request-size
limit at the reverse proxy as well: the server boundary is not a substitute for
network admission control.

Prometheus server metrics are fixed-cardinality and omit actor, workspace,
method arguments, result content, and error text. Database diagnostics are
opt-in, bounded, and record operation classes and execution work rather than
collection names, document IDs, filters, or values. The embedded admin surface
is separately authenticated. Treat its token and diagnostic access as
operational secrets.

Monitor `meldbase_http_unauthenticated_total`,
`meldbase_http_forbidden_total`, and `meldbase_http_rate_limited_total` for
authentication, authorization/origin, and admission anomalies. These are
aggregate detection signals, not a substitute for a retained identity-provider
or reverse-proxy audit trail.

Every data API rejection with status 401, 403, or 429 is also written as a
JSONL security journal event. It retains only timestamp, method, templated
route, status, and outcome (`unauthenticated`, `forbidden`, or `rate_limited`).
Concrete collection names, query strings, headers, tokens, request bodies, and
client addresses are excluded. Preserve and forward this stream according to
the operator's retention policy, and correlate it with identity-provider or
proxy logs when an actor identity is needed.

The service log level affects only ordinary lifecycle and backup events. It
does not suppress this security journal or the separately authenticated admin
audit journal.

When the embedded admin listener is enabled, each admin HTTP request is also
written as a JSONL journal event. It records only a timestamp, method, path
without query parameters, HTTP status, and outcome (`success`,
`unauthenticated`, `forbidden`, or `failure`). It never records the bearer
token, headers, request body, diagnostics content, client address, or query
string. Preserve and forward this journal stream according to the operator's
retention policy.

## Verification and incident response

The server test suite covers cross-workspace REST and realtime isolation,
field/filter/sort/aggregate restrictions, server-owned workspace writes,
bounded request and response behavior, redaction in realtime deltas and
diagnostics, and stable external errors. Re-run the normal verification gate
after access-policy or authentication changes:

```sh
go test ./internal/server ./server ./cmd/meld
pnpm check
```

On a suspected credential or policy incident, remove public routing first,
preserve service and proxy logs, rotate the upstream signing secret or key,
review the access manifest, restart the service under the new configuration,
then validate `/readyz` and a least-privilege workspace test before restoring
traffic.
