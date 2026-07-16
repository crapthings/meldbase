# Observability

Observability is a database contract, but exporters and dashboards are optional
consumers. Meldbase does not invoke user callbacks, serialize JSON, or send
network messages from a storage, query, commit, or reactive hot path.

## Point-in-time statistics

`DB.Stats()` returns a bounded, allocation-free, O(1) snapshot suitable for
periodic admin sampling. Collection, document and index gauges are initialized
once during open and maintained on the same commit visibility boundary as the
logical state; sampling never walks the live collection or index catalogs:

```go
stats := db.Stats()
log.Printf("sequence=%d ixscan=%d collscan=%d slow=%d",
    stats.CommitSequence,
    stats.Queries.IndexScans,
    stats.Queries.CollectionScans,
    stats.Realtime.SlowConsumers,
)
```

The snapshot contains process-lifetime counters for commits, query plans and
work, reactive views/deltas/queues, slow consumers and durability. It also
reports whether a fail-stop durability error has disabled writes and the fixed
reactive pending-batch/change capacities. Counters reset
when the database is reopened. Persistent gauges such as commit sequence come
from the selected database state.

Public optimistic write transactions add a fixed aggregate with current active
callbacks plus started, committed, no-op, point-conflict and other-abort
totals. Every started callback reaches exactly one terminal counter. The same
fields appear in the developer panel, Prometheus and OpenTelemetry without
method, collection, document, principal or error labels. Callback timing and
business values are not recorded unless the separate bounded diagnostics
session is explicitly enabled.

For legacy V1, durability statistics include current WAL bytes/commits,
checkpoint attempts/completions/failures, automatic checkpoint triggers and
successful checkpoint total/maximum latency. These expose threshold behavior
and fail-stop maintenance errors without exporting the sidecar path. V2 reports
zero for the V1-only families because every logical write is already a COW
checkpoint.

The V2 backend used for new default databases additionally exposes:

- physical page count and page size;
- page-cache capacity, residency, hits, misses and evictions;
- decoded-document-cache entry/byte capacity, residency, hits, misses and
  evictions;
- Commit Log head and oldest-retained sequence;
- active snapshot readers and replay leases;
- persistent collection/document counts read from the current root without a
  tree traversal;
- backend commit attempts, committed/rejected transactions, total latency and
  maximum latency for the current process;
- B+Tree node splits and sibling merges published in the current process session;
- persistent FreeSpace snapshot state, physical publications, load attempts,
  fail-safe load failures and candidate page-header checks.
- active lazy query cursors;
- compaction active/attempt/completed/failed counters, last input/output bytes
  and last duration.
- reclamation operations, complete graph scans including retries, exhausted
  online conflicts, last mode/attempt count, reachable/reclaimable pages and
  duration.
- physical V2 backup active/attempt/completed/failed counters, last byte count
  and last duration.
- persistent index-build phase/entry/byte gauges plus scheduler run/yield/failure
  counters and a retention-lease gauge, read from an immutable aggregate
  snapshot rather than scanning the BuildCatalog on each sample.

These values contain no path, collection name, document ID, query literal,
principal, tenant, authorization value, or document content. `Stats()` is for a
sampler, not for invocation on every database operation.

The dashboard, Prometheus schema and OpenTelemetry aggregate adapter expose
maintenance and backup only through those fixed fields. A background maintenance handle
also provides local run/completion/conflict/failure totals without page IDs or
dynamic labels.

Every successful constructor also freezes a schema-versioned `RecoveryReport`.
It records the engine, selected/valid Meta slots, selected sequence, V1 WAL
records replayed, provably incomplete main/WAL tail bytes removed, V2 fallback
to an older root and optional acceleration degradation. It contains no path or
business data, performs no I/O when read, and is included in `DBStats`. The
dashboard, Prometheus `meldbase_recovery_*` families and OpenTelemetry
`meldbase.recovery.*` gauges expose the same startup receipt. These are gauges
for this process session, not counters that increase while the process runs.
`RecoveryRequireClean` returns before a DB/admin sampler exists, so operators
observe that rejection through process startup handling and offline
`inspect`/`verify`, not by pretending a failed constructor produced live stats.

## Derived health contract

Admin schema version 2 adds a fixed-cardinality health assessment. It is derived
after `DB.Stats()` on the sampler goroutine; no health policy executes in a
storage, query, commit or reactive hot path. Levels are stable enums:

- `healthy` — no current state or latest-window engine signal;
- `degraded` — operation continues, but an engine/control-plane fallback or
  pressure signal needs attention;
- `critical` — the database is closed, writes are fail-stopped, or a reactive
  pending queue is at least 90% full;
- `unavailable` — that optional component, currently transport, was not attached
  to the sampler and is excluded from overall severity.

The assessment has fixed database, durability, storage, realtime, telemetry and
transport components plus fixed boolean explanations. Realtime pressure becomes
degraded at 50% and critical at 90% of the engine-reported capacities. A discarded
persistent FreeSpace map remains storage-degraded until it is republished.
Queue overflow, slow-consumer, WAL/checkpoint failure, telemetry replacement,
transport busy, outcome-unknown and worker-protocol signals use counter increases
between adjacent samples; they clear after a quiet window and are ignored across
database/server session resets. Application RPC failures, collection scans and
cache hit ratios are intentionally not classified as engine health failures.

A durable index build in its terminal `failed` state keeps Storage and Overall
`degraded` until it is inspected and aborted. An active build lease is not itself
unhealthy: it becomes a separate `indexBuildRetentionPressure` signal only when
the Commit Log is also beyond its configured count or byte budget and that
build's oldest watermark is the binding retention boundary. Both states
leave Database and Durability healthy and do not alter `/readyz`; ordinary reads
and writes remain available.

Prometheus exports `meldbase_health_status{component=...}` and OpenTelemetry
exports separate `meldbase.health.*` gauges. Numeric severity is 0 healthy,
1 degraded and 2 critical; unavailable components are omitted. Component names
and signal fields are engine-owned enums, never dynamic labels.

This rich authenticated admin health is distinct from public service probes.
The server's `/livez` never enters the DB; `/readyz` and its `/health` alias read
only the allocation-free `OperationalState` and return 503 unless the database
is both readable and writable. Probe bodies expose no derived signals or error
details, so deployment orchestration does not become an observability data leak.

## Bounded admin sampler and realtime stream

The optional `admin` package samples `Stats()` on a fixed interval, retains a
fixed number of samples, derives process-session rates, and exposes a latest-only
subscription. Every subscriber owns one replaceable channel slot. If a dashboard
is slow, its stale sample is overwritten and `droppedDeliveries` increases; the
sampler never waits for it.

Latest-value replacement is non-blocking at every step. In particular, the
consumer may drain its slot between the sampler's failed send and stale-value
removal; the sampler therefore uses a non-blocking drain and a non-blocking
replacement instead of assuming the slot is still full. A concurrent regression
test drives this race under the Go race detector. This is an architectural
invariant: a telemetry consumer may lose an intermediate sample, but it must
never freeze sampling, subscription cleanup, or database work.

`SamplerOptions.Server` may additionally reference the public `*server.Handler`.
Its fixed-cardinality snapshot adds authenticated realtime connection gauges and
aggregate HTTP/WebSocket RPC requests, active/success/failure/cancel/reject/busy
counters, bytes, arguments and latency. Optional RPC idempotency adds fixed
claim, replay, conflict, in-progress, outcome-unknown and store-failure counters;
transactional RPC adds atomic commit, rollback and successful no-op counters.
It never exports keys or fingerprints. The sample omits the `server` field when
no source is configured. It never includes method, principal, tenant, argument,
result or error strings. The CLI connects this source automatically when its
admin listener is enabled.

When the handler uses a `WorkerHub`, the same server snapshot includes fixed
worker gauges/totals: connected workers, registered methods/publications,
active/started/successful/failed/canceled/busy calls, policy evaluation
outcomes, protocol failures, control bytes, transaction operations and committed
policy invalidations. This exposes unexpected authorization-driven resync
pressure without collection, worker or principal labels.
Prometheus and the embedded dashboard consume these
fields without worker-ID or method-name labels. Worker handlers never run on the
sampler goroutine.

Creating or opening a database does not start this package. The sampler does not
use a normal Meldbase collection or the user-data reactive pipeline, so telemetry
cannot recursively create commits or reactive work.

```go
sampler, err := admin.NewSampler(db, admin.SamplerOptions{
    Interval:       time.Second,
    HistorySize:    300, // five minutes
    MaxSubscribers: 8,
    Server:         applicationHandler, // optional *server.Handler stats source
})
if err != nil {
    log.Fatal(err)
}
defer sampler.Close()

authorize, err := admin.NewBearerTokenAuthorizer(os.Getenv("MELDBASE_ADMIN_TOKEN"))
if err != nil {
    log.Fatal(err)
}
handler, err := admin.NewHandler(admin.HandlerOptions{
    Sampler:        sampler,
    Authorize:      authorize,
    ServeDashboard: true,
    ServeMetrics:   true,
    Diagnostics:    db, // follows an explicitly enabled diagnostic session
})
if err != nil {
    log.Fatal(err)
}

server := &http.Server{
    Addr:              "127.0.0.1:9091",
    Handler:           handler,
    ReadHeaderTimeout: 5 * time.Second,
}
log.Fatal(server.ListenAndServe())
```

There is deliberately no `Listen` convenience function: the application must
choose the bind address, TLS and shutdown policy explicitly. The handler exposes:

- `GET /v1/stats` — latest versioned sample;
- `GET /v1/stats/history` — oldest-to-newest bounded history;
- `GET /v1/stats/stream` — independent Server-Sent Events stream;
- `GET /v1/diagnostics?after=N&limit=M` — optional incremental diagnostic ring;
- `GET /metrics` — optional Prometheus text-format aggregate metrics.

With `ServeDashboard: true`, `GET /` and its two embedded assets provide the
developer Observatory panel. The static shell is intentionally readable without
credentials and contains no database state. The user enters the bearer token in
the page; JavaScript keeps it only in tab memory and sends it in API headers. The
stats, history and stream endpoints remain authenticated. The page has no remote
fonts, scripts, analytics or other network dependencies and is protected by a
restrictive Content Security Policy.

The development CLI can launch the same panel on a separate loopback listener:

```sh
export MELDBASE_ADMIN_TOKEN='replace-with-at-least-32-random-bytes'
go run ./cmd/meld serve \
  --db ./dev.meld \
  --dev-no-auth \
  --admin-addr 127.0.0.1:9091 \
  --admin-diagnostics \
  --admin-metrics
```

Open `http://127.0.0.1:9091/` and paste the token. The CLI refuses wildcard or
non-loopback admin addresses. Production remote administration should construct
the package directly behind an authenticated TLS endpoint.

`--admin-diagnostics` records only slow and failed operations with fixed default
thresholds. `--admin-diagnostics-all` implies it and records every query and
durable commit; it is for short development sessions, not unattended production.
`--admin-metrics` independently enables the authenticated `/metrics` endpoint.

All data endpoints require an explicit authorizer. The built-in bearer helper
requires at least 32 bytes, compares the `Authorization` header in constant time,
and never accepts credentials in a URL. Cross-origin access is denied unless its
exact HTTP(S) origin was configured; wildcard origins are rejected. Browser
preflight permits only `GET` and `Authorization`.

The admin snapshot schema is version 10 and uses camel-case JSON fields. Version 2
added the fixed health assessment, fail-stop write state and reactive capacities;
version 3 adds the immutable startup recovery receipt; version 4 adds resource
admission, Commit Log byte retention and physical storage quota fields/signals;
version 5 adds index-build entry/byte budgets; version 6 adds fixed index-build
activity, outcome, size and latency signals; versions 7 and 8 add durable
index-build phase/size and scheduler signals; version 9 adds the aggregate
index-build retention lease and its fixed degraded-health explanations; version
10 adds public optimistic write-transaction lifecycle aggregates.
The checked-in `admin/testdata/admin-schema-v10.json` fixture pins every reachable
`Sample` object and scalar path, JSON wire type, optionality and nullability in
deterministic order. A test also compares that reflected contract with Go's
actual JSON encoder. Adding, removing, renaming or changing a field requires an
explicit `SchemaVersion` increment and a new fixture; historical fixtures are
never rewritten, including version 9. Additive readers should continue ignoring fields introduced by
newer versions, while a producer must never emit a changed contract under an old
version number.

Durations whose names end in `Nanos` are integer nanoseconds; timestamps are RFC 3339 JSON
timestamps. Counters are operational telemetry, not a source for database resume
tokens or control-plane decisions.

## Bounded diagnostic events

Detailed events are disabled by default. Enabling them installs one atomic
diagnostic-session pointer and a fixed-capacity ring:

```go
diagnostics, err := db.EnableDiagnostics(meldbase.DiagnosticsOptions{
    Capacity:            256,
    SlowQueryThreshold:  50 * time.Millisecond,
    SlowCommitThreshold: 100 * time.Millisecond,
    SampleEvery:         1000, // optional deterministic one-in-N fast sample
})
if err != nil {
    log.Fatal(err)
}
defer diagnostics.Close()
```

Failed operations are retained unless `ExcludeFailures` is set. `RecordAll`
records every observed query and durable commit. Closing the handle atomically
removes the fast-path timer; its retained snapshot remains readable. A later
session receives a new monotonic session number and `startedAt` boundary, then
restarts event sequence numbers.

Events contain only fixed enums and aggregate work: kind, outcome, sanitized
error class, planner stage, duration, documents examined/returned and mutation
count. They never contain collection or field names, query AST/literals,
document IDs/content, original error strings, principals, tenants or credentials.
V2 lazy COLLSCAN events span actual cursor execution through exhaustion, error or
explicit close; they are not mislabeled as planner-only latency.

`SnapshotAfter` and the authenticated admin endpoint return chronological events
after a sequence with bounded pagination. `truncated` reports that the requested
cursor fell behind the ring; `hasMore` requests another bounded page. The panel
fetches at most 256 events per stats interval, starts from the most recent 256,
keeps only its latest 30 rendered rows and safely resets across diagnostic
sessions.

Aggregate exporters remain cold consumers of the sampler. Prometheus exposition
and the separate OpenTelemetry integration both consume the latest immutable
sample. Go runtime traces and profiles remain explicit, time/byte-bounded admin
library operations with no default network route.

## Prometheus exporter

`ServeMetrics` exposes the sampler's latest immutable snapshot as Prometheus text
format 0.0.4. The response uses the required absolute content type
`text/plain; version=0.0.4; charset=utf-8`, UTF-8/LF records, `HELP` and `TYPE`
metadata before samples, and a final newline. A scrape does not call `DB.Stats()`
or enter a database lock; sampling cost remains on the configured sampler tick.

The namespace is `meldbase_`. Counters end in `_total`; durations use seconds and
sizes use bytes. Exported families cover database/session health, commits,
planner stages, query work, reactive queues, current WAL/checkpoint health,
WAL/storage duration, page/document
caches, compaction, reclamation, backup, diagnostics, optional aggregate server/RPC work
and the admin sampler itself.
Labels are limited to these engine-owned enums:

- `engine="memory|v1|v2"`;
- `stage="collection_scan|index_scan|id_lookup"`;
- `outcome="committed|rejected"`;
- `kind="query|commit"`;
- `component="overall|database|durability|storage|realtime|telemetry|transport"`.

Unknown engine strings are mapped to a safe engine family and never serialized.
There are no path, database, collection, index, query, user, tenant, error-text
or document labels. Every unique label combination is therefore statically
bounded. The endpoint uses the same admin authorizer and exact-origin boundary;
it is disabled unless `ServeMetrics` or `--admin-metrics` is explicitly set.

Current rendering cost with the optional server/RPC and health families present
is approximately 20.9–22.3 us, 42.1KB and 30 allocations for a 36.6KB response.
The exporter pre-grows one bounded buffer and appends integer values directly;
normal CI pins an allocation/capacity budget so adding metric families cannot
silently restore repeated buffer growth. This work occurs at scrape frequency,
not per operation.
Format, family ordering, counter suffixes, fixed labels, uint64 nanosecond
conversion, authentication, opt-in routing and concurrent determinism are test
gates.

`server.Handler.Stats()` currently measures approximately 90.8 ns with zero
allocations. Updating aggregate RPC active/count/byte/duration/max counters around
one method execution measures approximately 159 ns with zero allocations on
the same development machine. Neither path creates dynamic labels or waits for
the admin sampler.

## OpenTelemetry aggregate adapter

`integrations/otel` registers a fixed `meldbase.*` asynchronous aggregate schema
against an explicitly supplied `metric.MeterProvider`. It consumes
`admin.Sampler.Latest()`, so an OTel reader callback never invokes `DB.Stats()` or
enters a database lock. The adapter does not instantiate an SDK, exporter,
reader, resource, goroutine or global provider. `SchemaVersion` and
`Instruments()` make the contract inspectable, and `Adapter.Stats()` reports
collection, unavailable-sample and uint64-to-int64 saturation events.

The schema contains no application-controlled attributes. Server/RPC/Worker
instruments are omitted from a collection when the sampler has no server source.
OpenTelemetry's stable `db.client.*` convention describes calls as observed by a
database client; using it for an embedded engine aggregate would describe the
wrong side. Meldbase therefore uses its own namespace and does not emit query
text, collection, path, document, principal, tenant or error attributes. The
core database package and default admin package import neither the OTel API nor
SDK; only the integration package imports the stable Metrics API.

The adapter's complete in-process mapping callback currently measures about
4.83 us with zero heap allocations against an immutable synthetic sample.
Reader/exporter work remains owned by the caller's OTel SDK and is not included
in that number.

## Bounded Go runtime captures

`admin.NewRuntimeProfiler` provides explicit CPU profile, heap profile and Go
runtime trace capture to a caller-owned `io.Writer`. CPU/trace windows default to
10 seconds, must be at least 100 ms and are capped by a configured maximum no
greater than five minutes. Output defaults to a 64 MiB limit and can never be
configured above 1 GiB. All profiler instances share a non-blocking process-wide
slot; a concurrent capture receives `ErrRuntimeCaptureBusy` rather than waiting.

The controller starts no listener and the default admin handler exposes no
profile endpoint. Embeddings must authorize the request, choose storage and
control artifact retention themselves. Runtime traces and profiles can contain
function names, source paths and process topology and add process-wide overhead;
they are intended for short, attended diagnostic windows, not continuous
telemetry.

## Performance budget

Always-on instrumentation is limited to fixed-schema atomic counters and gauges.
Latency distributions and high-detail events are sampled. Raw query,
collection, user and request labels are forbidden in default metrics because
their unbounded cardinality would turn observability into an uncontrolled memory
consumer.

The same sample exposes configured document/transaction/index-build admission limits, the
aggregate number of resource-limit rejections, and V2 Commit Log count/byte
budgets, retained logical bytes, overage and pressure. These are available in the embedded panel, Prometheus
and OpenTelemetry without per-collection labels. A legitimate oversized request
increments a counter but does not make health degraded; current retention
pressure is degraded because a replay pin is actively preventing the configured
history bound.

For durable online index builds, `retentionLeaseActive` identifies whether at
least one non-failed build watermark is behind the current commit head. It is a
single database-level boolean: collection and index names never become metric
labels. Health attributes retention pressure to index building only when this
lease is the binding retention boundary and the storage pressure state is true;
a coincident older replay reader is not misattributed to the build. Failed
builds release their Commit Log lease but remain degraded maintenance state
until explicitly aborted.

V2 additionally exposes physical high-water bytes, configured quota, overage,
current exhaustion and pre-I/O quota rejections. Exhaustion is a degraded storage
state, not a durability failure: committed reads remain valid and writes may
resume after reclaiming reusable pages, compacting to another file, or reopening
with a larger quota.

`BenchmarkStatsSnapshot`, `BenchmarkV2StatsSnapshot` and
`BenchmarkV2StatsSnapshotWithPersistentIndexBuilds` are regression gates for
snapshot cost and allocations. The dedicated-runner sampling gate compares the
median throughput of the same synthetic publication-lock writer with sampling
disabled and with `Stats()` called every millisecond—1,000 times the normal admin
frequency—and rejects more than 5% relative loss. The current three local runs
measured 0.1–0.3%. If shared atomic counters become measurable on many-core
systems, their implementation can move to sharded/per-P accumulators without
changing the public `DBStats` schema.

The derived health assessment itself measures approximately 124 ns with zero
heap allocations and runs only once per sampler tick. Current
development-machine measurements are approximately 217–244 ns for the
in-memory/V1 snapshot and 540 ns for the V2 snapshot, both with zero heap
allocations. A dedicated cardinality benchmark measures the same range with one
and 10,000 collections, proving that sampling does not traverse their catalogs. Eight
durable unfinished index builds remain approximately 539 ns with zero
allocations because sampling does not traverse their catalog.
Sampling V1 while a diagnostic ring is enabled is approximately 241 ns, also
allocation-free. A sampler capture including rates and health over a synthetic
source is approximately 501 ns with zero allocations. V2 additionally samples its 16
page-cache shards, decoded-document cache and active reader pins. These numbers
describe snapshot and sampler reads; they do not replace the required
instrumentation-on/off concurrent throughput comparison.

The default-disabled diagnostic hook is approximately 8.3 ns with zero
allocations on the same machine. Compared with an approximately 1.33 us in-memory compiled
point query, that isolated fast-path cost is below one percent. Opt-in filtered
diagnostics measure approximately 1.54 us/query; record-all measures
approximately 1.56 us/query, both without additional allocations.

`BenchmarkPointQueryDiagnosticsModes` and `BenchmarkDiagnosticHookModes` retain
throughput, allocation and custom p99 metrics. A dedicated runner can enforce the
filtered-mode p99 budget with:

```sh
MELDBASE_PERF_GATE=1 GOMAXPROCS=1 go test . \
  -run '^(TestDiagnosticsPerformanceBudget|TestStatsSamplingPerformanceBudget)$' \
  -count=5
```

The current opt-in gate rejects a filtered-diagnostics p99 increase above 35%.
It disables GC only inside each bounded measurement window, alternates mode
order, and compares the median of seven p99 rounds; the latest ten local gate
runs measured approximately -3% to +23% relative overhead despite CPU-frequency
changes producing two distinct absolute latency bands. Default CI leaves this
test skipped because nanosecond p99 assertions are not meaningful on arbitrary
shared runners.

The manual `Observability performance` workflow makes this gate reproducible on
a repository-owned runner labeled `meldbase-performance`. It fixes
`GOMAXPROCS=1`, runs repeated relative p99 and aggressive Stats-sampling
throughput comparisons, records every core, sampler, exporter, server and
OpenTelemetry benchmark, and archives the revision, Go environment and raw logs.
It intentionally has no shared-runner schedule: a queued workflow without that
dedicated runner is not performance evidence.
