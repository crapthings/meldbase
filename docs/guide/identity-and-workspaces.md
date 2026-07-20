# Identity, users, and workspaces

Meldbase enforces the data boundary for a verified application identity. It is
not an identity provider, a user directory, or a membership product. Your
application continues to own account creation, credentials, roles, workspace
membership, and token issuance.

Use this split even when all of those concerns live in the same application:

| Your identity service | Meldbase |
| --- | --- |
| Creates users and verifies sign-in credentials. | Verifies the access token before every HTTP or realtime session. |
| Decides whether a user may enter a workspace. | Reads the active workspace from the verified token. |
| Issues short-lived JWTs and revokes/refreshes them. | Constrains configured collection reads, writes, deletes, and subscriptions to that workspace. |
| Owns roles and method-level authorization. | Writes the server-owned workspace field on inserts and prevents clients from changing it. |

## The token is the active workspace

For the built-in workspace authorizer, a token must contain these claims:

```json
{
  "sub": "user_42",
  "workspace_id": "team_a",
  "iss": "https://identity.example/",
  "aud": "meldbase-api",
  "exp": 1784563200
}
```

`sub` identifies the signed-in principal; `workspace_id` identifies the
workspace that principal is currently using. The server verifies `exp`, `iss`,
and `aud`, then derives its internal principal from `sub` and `workspace_id`.
It never trusts a workspace passed as a query parameter, request field, or
WebSocket URL.

Changing workspace means your identity service first checks membership, then
issues a new token with the new `workspace_id`. Do not let the browser edit this
claim or treat a client-side workspace picker as authorization.

## What gets isolated

Configure only the collections that hold tenant-scoped data:

```sh
meld serve \
  --db /srv/meldbase/data/app.meld \
  --addr 127.0.0.1:8080 \
  --jwt-jwks-url https://identity.example/.well-known/jwks.json \
  --jwt-issuer https://identity.example/ \
  --jwt-audience meldbase-api \
  --workspace-collections projects,tasks,comments,memberships \
  --workspace-field workspaceId
```

For every listed collection, the authorizer:

1. adds `workspaceId = <verified workspace_id>` before a query, update, delete,
   or subscription reaches the database;
2. writes `workspaceId` itself on each insert, replacing any client-provided
   value; and
3. rejects updates to `workspaceId` or any of its nested paths.

The client can use ordinary queries. It should not send a tenant selector, and
it cannot escape the injected constraint by adding a conflicting filter. The
same rule applies to realtime snapshots and deltas.

For a developer- and tool-readable declaration that can also express
owner-only and RPC-only collections, use the versioned
[collection access manifest](./access-policies). The older flag form above is
the compatible shorthand for collaborative workspace collections.

A workspace is a logical security boundary in one database file; it is not a
SQLite file, MongoDB database, or independently backed-up physical partition.
Choose separate Meldbase instances only when you need distinct operational
ownership, failure domains, or retention boundaries.

## Where user records belong

Keep global account records—login identifiers, password/OIDC credentials,
password-reset state, and cross-workspace membership decisions—in your identity
service. Meldbase does not expose a user-management API or make a `users`
collection special.

It is fine to store application-facing, workspace-scoped member profiles in
Meldbase, for example a `memberships` collection containing display name,
workspace role, and product preferences. Include that collection in
`--workspace-collections`; the server will scope it just like `projects`.
Your trusted application code must still decide whether a role permits an
operation. The built-in workspace authorizer deliberately rejects RPC methods
until the application provides an explicit method-level authorizer.

## Choose a verifier

Use one verifier mode per server:

- **HS256 secret file** is appropriate for a local bundle or an identity service
  that can keep the shared secret private. `meld init` generates this mode.
- **RS256 JWKS** is usually the better boundary for a deployed service: Meldbase
  receives only public signing keys from an HTTPS JWKS endpoint, while the
  identity service retains its private key.

Both modes require `--jwt-issuer`, `--jwt-audience`, and either
`--workspace-collections` or `--access-policy-file`. Do not pass `--dev-no-auth`
outside disposable local development, and never serve the API or operator
dashboard directly on a public interface without an application-owned TLS
boundary.

## Verify the boundary

Test isolation with two real tokens, not merely two browser tabs:

1. Obtain one token for `team_a` and one for `team_b` from the identity service.
2. Insert a document with each token into the same scoped collection.
3. Query as each token: each response must contain only its own document.
4. Attempt to insert `{"workspaceId":"team_b"}` while authenticated as
   `team_a`: the stored document must have `workspaceId: "team_a"`.
5. Attempt an update to `workspaceId`: it must be rejected.

Use the [HTTP and realtime reference](../reference/http) and the full
[client protocol](../client-protocol) when testing a non-SDK client. For the
host, JWT configuration, probes, backup, and recovery drill, continue with the
[single-node deployment guide](../single-node-deployment).
