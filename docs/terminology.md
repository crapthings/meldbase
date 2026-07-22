# Terminology and semantic boundaries

This is Meldbase's normative vocabulary. It defines what public product words,
SDK names, protocol fields, and operational terms mean. It is not a tutorial or
a list of interchangeable synonyms.

Use it in two directions:

- **Product and business meaning** keeps application developers clear about the
  promise they are relying on.
- **Engineering contract** keeps Go, TypeScript, protocols, documentation, and
  operations descriptions aligned with that promise.

When a proposed API name, protocol field, or document uses a word differently
from this page, change the proposal or update this page deliberately in the
same change. Do not add an unshipped compatibility synonym merely to preserve
an earlier internal name.

## Core rules

1. One public term has one primary meaning. Qualify a different meaning instead
   of overloading the word.
2. Product language names the user-visible boundary; implementation language
   may add precision but must not replace it with a competing concept.
3. `workspace` is Meldbase's only public isolation term. Do not introduce
   `tenant`, `principal`, or a client-selected scope as aliases.
4. A protocol field is a public API. Renaming an unshipped field requires its
   encoder, decoder, contract fixture, SDKs, examples, and tests to change
   together.
5. Internal storage mechanics may be more detailed than product language, but
   must be labelled as operational concepts rather than business features.

## Data model

| Term | Product and business meaning | Engineering contract |
| --- | --- | --- |
| **Database** | One application-owned document database. In the durable mode it is one local file and its history; it is not a hosted account or a workspace. | Go `DB` is the authoritative embedded engine. The HTTP/WebSocket server is an optional access boundary over a `DB`, not a second database. |
| **Database identity** | The durable lineage of a database. It lets backup, recovery, and replication reject an unrelated file. | A physical backup preserves it; `Compact` intentionally creates an independent database with a new identity. It is a namespace binding, not authentication or a secret. |
| **Collection** | A named namespace of application documents. | `DB.Collection(name)` is durable database data. `RemoteCollection` is an authenticated handle to one such collection. Collection policy is evaluated per request. |
| **Local collection** | Application-owned in-memory state with the same query vocabulary as Meldbase. | `LocalCollection` is explicitly imported from `@meldbase/client/local`. It is neither a remote cache nor a replica, has no server policy, and does not synchronize automatically. |
| **Document** | One typed application record in a collection. | A `Document` is a safe object of Meldbase `Value`s. Its canonical `_id` is the document identity and is distinct from every actor, workspace, or worker ID. |
| **Document ID** | The stable identity of one document. | Stored and queried as `_id`; TypeScript exposes the canonical 32-character lower-case hexadecimal form. Use `_id` for document identity, not a generic business `id` field. |
| **Value** | A data value that can safely cross storage and protocol boundaries. | The closed value model preserves types such as Int64/`bigint`, dates, binary, arrays, and objects. It never evaluates callbacks, source text, prototypes, or arbitrary classes. |
| **Index** | A declared access path for a collection query. | An index is database metadata, not an application collection or an authorization rule. Its field definition, publication, and recovery are durable engine concerns. |

## Queries and mutations

| Term | Product and business meaning | Engineering contract |
| --- | --- | --- |
| **Filter** | The data-only condition an application asks a query or mutation to match. | A developer-facing input that is compiled and validated; it is not executable JavaScript or a server-side callback. |
| **Query spec** | The exact, deterministic query to execute. | `QuerySpec` is the compiled form shared by local TypeScript evaluation, remote transport, and Go execution. Server policy may constrain it further. |
| **Query** | A way to read a result set, once or continuously. | Client `find()` returns a query object, not an eager array. Go `Find` returns a snapshot cursor; the language-specific return type is deliberate. |
| **Find / findOne** | `find` works with a result set; `findOne` asks for at most one matching document. | Do not rename `find` to `findMany`: its result may fetch, page, or subscribe. `findOne` is the single-document convenience method. |
| **Update** | A developer's requested partial document change. | `Update` is compiled to the canonical `MutationSpec`; both are data-only and bounded. |
| **Mutation spec** | The precise update/delete operation the engine can safely apply. | `MutationSpec` is the compiled/wire representation. Generic remote replacement and filter-based upsert are intentionally absent. |
| **One / many** | The cardinality of an effectful mutation target. | `updateOne` and `deleteOne` stop after one permitted match; `Many` variants may affect every permitted match within server limits. `insertOne` is strict creation. |
| **Transaction** | One all-or-nothing Meldbase database change. | `WriteTransaction` reads a pinned snapshot and stages point operations. It detects conflicts and does not silently rerun application code. It does not make external side effects atomic. |

## Realtime data and durable changes

| Term | Product and business meaning | Engineering contract |
| --- | --- | --- |
| **Reactive query** | The engine capability that maintains query results after commits. | An internal/core behavior built from ordered committed changes and shared views. It is not a transport protocol by itself. |
| **Live query** | A query whose result can stay current for an application or UI. | `LiveQuery` and `RemoteLiveQuery` are the public client query handles. A live query may `fetch`, page, or `subscribe`. |
| **Realtime** | Network delivery of a live query to another process or browser. | HTTP obtains snapshots; the authenticated WebSocket protocol delivers snapshots, deltas, resume, and resync controls. Realtime does not add a second query or authorization model. |
| **Snapshot** | A complete query result at one visible position. | The first delta-mode delivery is a `QuerySnapshot` / `snapshot` frame. It establishes the base for later ordered deltas. |
| **Delta** | An ordered transformation from one visible query result to a later one. | `QueryDelta` has an exact `fromToken` → `token` chain and operations such as add, move, change, and remove. A malformed or discontinuous delta requires resync. |
| **Subscription** | A caller's interest in a live query result. | `subscribe` is for UI/application query delivery. It is not a durable background job and does not acknowledge external side effects. |
| **Watch** | A process-local observation of raw committed changes. | `WatchChanges` is ephemeral and bounded. A slow watcher can fail; it must not block a committed write. Do not describe it as a durable consumer. |
| **Change consumer** | A named, crash-resumable background reader for reliable downstream work such as an outbox, audit sink, or indexer. | Durable change APIs persist a checkpoint and require `Ack` only after the external effect is durable. Delivery is at least once; `ErrHistoryLost` requires explicit resynchronization. A consumer is not a frontend subscription or generic collection hook. |
| **Resync** | Discarding an uncertain local live-query position and obtaining an authorized fresh result. | `resync_required` is a safety control, not an error to patch around. It is emitted for expired, invalid, unavailable, or policy-incompatible resume history. |

## Identity, workspaces, and access

| Term | Product and business meaning | Engineering contract |
| --- | --- | --- |
| **Identity service** | The application component that owns accounts, credentials, membership, roles, and token issuance. | Meldbase is not an identity provider or user directory. It verifies credentials at its access boundary. |
| **Actor** | The verified application identity for one request or Worker invocation. | Go `Actor` and TypeScript `context.actor` contain `id` and `workspaceId`/`WorkspaceID`. An actor may represent a user or a service. |
| **Actor ID** | The stable identity of the current user or service. | Derived from the verified JWT `sub` claim in the built-in authenticators. `sub` is identity-provider language; application handlers use `actor.id` or `Actor.ID`. |
| **Workspace** | The active, application-defined isolation scope in which an actor is operating. It may represent a team, organization, project, or another collaborative boundary chosen by the application. | The verified JWT `workspace_id` maps to `actor.workspaceId` / `Actor.WorkspaceID`. The built-in authorizer injects and protects the configured `workspaceId` document field. Clients never choose a trusted workspace in a query or URL. |
| **Workspace isolation** | The guarantee that scoped data is only visible or mutable within the actor's active workspace. | It is a logical boundary within one database, not a separate file, backup, or physical partition. Switching workspaces requires a newly issued credential. |
| **Collection access manifest** | A small declarative baseline for generic browser/mobile collection access. | It defines collection modes, field boundaries, the server-owned workspace/owner fields, and an optional RPC allowlist. It is validated at startup and only narrows the server's authority model. |
| **Authorizer** | Application/server code that decides whether an actor may perform a requested operation. | Go `Authorizer` handles query, insert, update, and delete admission; `RPCAuthorizer` admits named RPC. Authorization is server-side and re-evaluated per request. |
| **Read policy** | The restriction that determines which rows, fields, query paths, and result count an authorized caller may read. | Current Go names are `QueryPolicy`, `QueryPolicyResolver`, and `QueryPolicyLease`; the Worker SDK calls the read-only dynamic declaration `publish`. They may only narrow visibility and never grant generic writes or return documents directly. |
| **Read-policy invalidation** | A deliberate re-authorization when visibility depends on data outside the collection being read. | A transactional Worker call uses `invalidatePublication(collection)` after the related business change. It revokes affected subscriptions so they resync instead of continuing under stale visibility. |

## Application extension boundary

| Term | Product and business meaning | Engineering contract |
| --- | --- | --- |
| **RPC** | A named application business operation. | `client.call()` invokes a named RPC over HTTP by default or realtime when selected. A Go method map or a trusted Worker may implement it. RPC is not synonymous with Worker. |
| **Transactional RPC** | A named business operation whose Meldbase point mutations commit atomically with its terminal result. | The handler receives a `WriteTransaction`. It must not charge cards, send email, call another service, or rely on automatic retries within that transaction. |
| **Idempotency key** | The caller-supplied identity for one logical RPC attempt across explicit retries. | It is distinct from a request ID and from every token. When durable idempotency is configured, completed terminals replay and interrupted work becomes outcome-unknown rather than being silently rerun. |
| **Worker** | A separately authenticated, trusted application process that can implement RPC handlers and dynamic read policies. | `MeldbaseWorker` connects to the private Worker control endpoint. It is not browser code, an end-user identity, or a database replica. |
| **Worker ID** | A logical identifier for one Worker registration. | `workerId` scopes control-plane registration and replacement. It is separate from the Worker credential and from the per-call actor. |
| **Signal** | Cancellation information for one in-flight handler invocation. | `context.signal` is an `AbortSignal`; cancellation is best-effort and does not prove that a database mutation or external effect did not happen. |

## Errors and protocol identifiers

| Term | Product and business meaning | Engineering contract |
| --- | --- | --- |
| **Meldbase error** | An expected, application-owned business outcome, such as an order already being paid. | `MeldbaseError` has a namespaced lower-case code and optional safe typed data. It is serialized as error kind `business`. |
| **Meldbase internal error** | A safe failure owned by Meldbase or the transport boundary; it is not a leaked implementation detail. | `MeldbaseInternalError` carries a fixed engine/transport code, status and operation context. Error kind `internal` never carries application data. `outcome_unknown` requires reconciliation before retry. |
| **Access token** | The credential used to authenticate a browser or service request. | Usually a JWT sent in HTTP authorization. It is distinct from a realtime ticket and is never placed in the WebSocket URL. |
| **Realtime ticket** | A short-lived, single-use credential for opening an authenticated realtime WebSocket. | The SDK obtains it from the ticket endpoint after normal authentication. It is not a resume token. |
| **Resume token** | An opaque proof of one authorized live-query position that may be used after reconnect. | Bound to database identity, actor, workspace, collection, effective query, read policy, position, and expiry. Never interpret it as a commit number or expose its contents. |
| **Commit token** | A globally ordered logical position in committed database change history. | Core `ChangeBatch`, durable change feeds, replay, archive handoff, and replication order by this position. Use this term when discussing logical history. |
| **Commit sequence** | The persisted storage/observability number for the current durable commit position. | In the current durable engine it represents the commit-token timeline, but is used in receipts, metadata, and operational statistics rather than as a browser resume token. |
| **Generation** | A physical storage metadata generation. | It can advance for maintenance such as reclamation or index publication without representing a new business document commit. Do not treat it as a commit token. |
| **Page cursor** | An opaque position for seek pagination. | Created from a deterministic sort and used as `after`; it is unrelated to realtime resume or durable change acknowledgement. |
| **Request ID** | A per-transport request correlation value. | It matches a request to its terminal protocol frame. It does not give retries an idempotent meaning. |

## Operations and recovery

| Term | Product and business meaning | Engineering contract |
| --- | --- | --- |
| **Physical backup** | A recoverable copy of a durable database and its lineage. | It preserves database identity and commit history; source access is offline and exclusive. |
| **Logical archive** | A portable export of application documents, collections, and index definitions. | It intentionally excludes database identity, pages, credentials, and commit history. Import creates a new database lineage. |
| **Follower** | A read-only database target that applies ordered replication changes from a primary. | `OpenFollower` rejects ordinary writes; `Follower.Apply` accepts only the next committed token. Browser realtime is not replication. |
| **Replication** | Trusted server-to-server bootstrap and ordered commit-tail delivery. | It has a separate protocol and authentication boundary from browser realtime. A durable change consumer checkpoint provides source progress. |
| **Primary write fence** | External evidence that this database is authorized to publish the next write. | The core validates it at the durable commit boundary. It is not leader election or a claim of automatic high availability. |
| **Rollback anchor** | External retained evidence that prevents accepting a database rolled back behind an acknowledged state. | It binds database identity, commit sequence, and generation. It is an advanced deployment control, not an application authorization feature. |

## Change checklist

Before merging a change that adds or changes a public concept, answer these
questions in the pull request or commit description:

1. Which term on this page owns the concept? If none does, add its definition
   before exposing the API.
2. Is the product promise distinct from the engineering mechanism? State both.
3. Do Go, TypeScript, HTTP/WebSocket/Worker frames, CLI output, examples, and
   docs use the same name and field meaning?
4. Could this be mistaken for an existing term—especially collection vs local
   collection, subscription vs consumer, workspace vs actor, or resume token
   vs commit token? If so, qualify or rename it.
5. Does the term affect authorization, durability, retry safety, or recovery?
   If it does, update the relevant contract tests and operational documentation
   in the same change.

The terminology review is complete only when an application developer can
describe the behavior without knowing the implementation, and an implementer
can find the exact contract without inventing a competing name.
