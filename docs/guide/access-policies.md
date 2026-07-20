# Collection access policies

Meldbase gives a browser or mobile client access to a **data API**, not to a
database file or an administrator credential. Every request first verifies a
JWT, then the server derives the principal's `sub` and active workspace claim.
The client may request a query, but it never chooses a trusted workspace,
owner, result projection, or generic write permission.

The built-in collection access manifest is intentionally small and data-only.
It is a safe default for common application data; it is not a replacement for
your application's membership, role, or business rules.

## Use a strict manifest

Pass a versioned JSON file to `meld serve`:

```sh
meld serve \
  --db /srv/meldbase/data/app.meld \
  --jwt-jwks-url https://identity.example/.well-known/jwks.json \
  --jwt-issuer https://identity.example/ \
  --jwt-audience meldbase-api \
  --access-policy-file /etc/meldbase/access-policy.json
```

```json
{
  "$schema": "https://crapthings.github.io/meldbase/schemas/collection-access-manifest-v1.schema.json",
  "version": 1,
  "workspaceField": "workspaceId",
  "collections": [
    {
      "collection": "tasks",
      "mode": "collaborative",
      "fields": {
        "queryPaths": ["title", "done"],
        "resultFields": ["title", "done"],
        "inputFields": ["title", "done"],
        "updatePaths": ["title", "done"]
      }
    },
    {"collection": "private_notes", "mode": "owner", "ownerField": "ownerId"},
    {"collection": "payroll", "mode": "rpc_only"}
  ]
}
```

The manifest rejects unknown fields, trailing JSON, duplicate collections,
unknown modes, invalid field names, and unsupported versions before the server
starts. `meld init` generates this file with `collaborative` entries for its
default local collections. The older `--workspace-collections` form remains a
compatible shorthand for an all-`collaborative` manifest.

Validate or inspect a manifest without opening a database or starting a server:

```sh
meld access-policy validate --file /etc/meldbase/access-policy.json
meld access-policy explain \
  --file /etc/meldbase/access-policy.json \
  --subject user_42 \
  --workspace team_a \
  --collection private_notes
```

`validate` prints the canonical manifest. `explain` prints the effective
generic query constraint, server-owned insert fields, immutable update paths,
and operation allowance for that simulated principal. It is a static review
tool: it does not validate a JWT, open a database, or evaluate a custom role or
membership resolver.

For editor autocomplete, CI checks, or AI-generated configuration, the
versioned [JSON Schema](/schemas/collection-access-manifest-v1.schema.json) is
published with the documentation. Add the shown `"$schema"` line to opt into
editor autocomplete. It catches the portable manifest shape; the server's
strict parser remains the final authority for every semantic check.

### Optional field boundary

`fields` is a composable restriction, not another access mode. Each omitted
field list means all fields for that operation; an explicit empty list means no
application fields. All entries are bounded, unique, and validated at startup.

| Declaration | Applies to | Meaning |
| --- | --- | --- |
| `queryPaths` | fetch, mutation target, subscription | Client may filter or sort only by these document paths. `_id` remains a safe direct lookup. |
| `resultFields` | fetch, insert response, subscription snapshot/delta | Only these top-level fields are returned, plus `_id`. |
| `inputFields` | insert | Only these top-level client fields are accepted. |
| `updatePaths` | update | Only these document paths may be changed. |

The server still accepts and overwrites declared `workspaceId` / `ownerId`
input values so a client cannot turn a field whitelist into a tenant or owner
selection mechanism. Those server-owned fields are always immutable on update.

## Stable modes

| Mode | Generic reads and subscriptions | Generic writes | Server-owned fields |
| --- | --- | --- | --- |
| `collaborative` | Any verified member of the active workspace | Insert, update, delete inside that workspace | `workspaceId` on insert; immutable afterwards |
| `owner` | Only the active workspace member whose `ownerField` equals `sub` | Only that same owner may mutate or delete | `workspaceId` and `ownerField` on insert; both immutable afterwards |
| `rpc_only` | Denied | Denied | None; expose only explicit application RPC methods if needed |

Every listed mode is enforced by the same Go policy engine. Modes are presets,
and `fields` only narrows those presets; neither creates another query
implementation or a client-side callback.

## What a client request means

For a user with `sub = user_42` and `workspace_id = team_a`, this request:

```ts
db.collection("private_notes").find({ archived: false }).subscribe(render)
```

under the `owner` policy above becomes the effective server query:

```text
workspaceId = "team_a"
AND ownerId = "user_42"
AND archived = false
```

The server applies that same effective query to the initial snapshot, every
realtime delta, updates, and deletes. A conflicting client filter only makes
the result empty; it cannot weaken a server constraint.

For writes, the server distinguishes the existing target from the proposed
document:

- an update/delete must first target a document visible to the policy;
- an `owner` insert overwrites supplied `workspaceId` and `ownerId` values;
- updates may not change either server-owned field;
- collection-wide writes remain bounded by the configured result limit.

This is the equivalent of a row-level policy's existing-row check plus
new-row check, without a client-side `allow` / `deny` callback order.

## When not to use generic collection access

Use `rpc_only` and named RPC methods for approvals, payments, ownership
transfers, membership changes, credential records, or any operation whose
authorization depends on more than the document's workspace and owner fields.
`rpc_only` does not make RPC public: the application still installs an explicit
`RPCAuthorizer` for every method.

For read visibility based on memberships, roles, sharing links, or another
collection, use a Go `Authorizer` or a server Worker `publish()` policy. Those
components return the same bounded row constraint, field projection, and result
limit that the Go server intersects with the manifest policy. If such a policy
depends on other data, commit `invalidatePublication()` with the membership or
role change so existing subscriptions resynchronize before stale visibility can
continue.

A Worker `publish()` policy (and `QueryPolicyResolver`) is deliberately
**read-only**: it governs HTTP queries and subscriptions, never generic
inserts, updates, or deletes. For a role- or membership-dependent write, use a
full Go `Authorizer`, or set the collection to `rpc_only` and expose a named
RPC method with its own `RPCAuthorizer`. This keeps a write decision explicit
instead of accidentally deriving it from read visibility.

Keep account credentials, password hashes, refresh tokens, and global account
membership decisions out of generic client collection access. See
[Identity, users, and workspaces](./identity-and-workspaces) for the identity
boundary.
