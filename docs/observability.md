# Metrics and OpenTelemetry

Meldbase exposes bounded, low-cardinality operational metrics. They deliberately
exclude database paths, collection/index names, query values, document IDs,
actors, workspaces, tokens, request bodies, and error text. Metrics are an
operator signal, not an audit log.

## Prometheus scrape endpoint

Enable the embedded admin listener and metrics explicitly:

```sh
meld serve \
  --db /srv/meldbase/data/app.meld \
  --admin-addr 127.0.0.1:9091 \
  --admin-metrics
```

The endpoint is `GET /metrics` on the admin listener. It requires the admin
bearer token and returns Prometheus text exposition `0.0.4` with
`Cache-Control: no-store`. Keep this listener on loopback or an equivalent
authenticated operator network; do not expose it through the public data API.

```sh
export MELDBASE_ADMIN_TOKEN="$(cat /etc/meldbase/admin.token)"
curl --fail --silent --show-error \
  -H "Authorization: Bearer $MELDBASE_ADMIN_TOKEN" \
  http://127.0.0.1:9091/metrics
```

The fixed metric families cover liveness, commits, durable storage and quota,
recovery, backup, index builds, query work/budget pressure, realtime queues,
request/RPC aggregates, security rejection totals, and health. Alert on
`meldbase_writes_disabled`, failed/rejected backups, sustained resource-limit
or query-budget rejections, storage exhaustion, and rising realtime queue
overflows. Combine these with host filesystem/inode alerts and the safe JSONL
audit stream described in [security](/production-security).

## OpenTelemetry Go Metrics adapter

`github.com/crapthings/meldbase/integrations/otel` (`meldotel`) adapts the same
immutable `admin.Sampler` snapshot to an application-owned OpenTelemetry Go
`MeterProvider`. It does not instantiate an SDK/exporter, use a global provider,
open a network connection, start a goroutine, or call `DB.Stats()` from a metric
callback. The application owns exporter, resource, reader, collection interval,
and credential configuration.

```go
sampler, err := admin.NewSampler(db, admin.SamplerOptions{
    Interval: time.Second,
    Server: handler, // optional server/RPC/realtime aggregates
})
if err != nil { log.Fatal(err) }
defer sampler.Close()

adapter, err := meldotel.New(sampler, meldotel.Options{
    MeterProvider: applicationMeterProvider,
})
if err != nil { log.Fatal(err) }
defer adapter.Close()
```

The adapter exposes a fixed aggregate instrument contract through
`meldotel.SchemaVersion` and `meldotel.Instruments()`. It intentionally emits
engine metrics, not `db.client.*` spans or application request traces. Add
application tracing around your own HTTP/RPC handlers when request-level traces
are required.

See the package [adapter README](https://github.com/crapthings/meldbase/tree/main/integrations/otel)
for the current instrument schema and integration details.
