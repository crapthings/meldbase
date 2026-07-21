# Meldbase OpenTelemetry metrics

`meldotel` bridges the latest immutable `admin.Sampler` snapshot to the stable
OpenTelemetry Go Metrics API. It accepts an application-owned `MeterProvider`;
it never constructs an SDK/exporter, uses the global provider, opens a network
connection, starts a goroutine or calls `DB.Stats()` from an OTel callback.

```go
sampler, err := admin.NewSampler(db, admin.SamplerOptions{
    Interval: time.Second,
    Server:   handler, // optional aggregate server/RPC/Worker metrics
})
if err != nil {
    log.Fatal(err)
}
defer sampler.Close()

adapter, err := meldotel.New(sampler, meldotel.Options{
    MeterProvider:          applicationMeterProvider,
    InstrumentationVersion: "0.1.0",
})
if err != nil {
    log.Fatal(err)
}
defer adapter.Close()
```

`SchemaVersion` and `Instruments()` expose the fixed contract. Instrument names
start with `meldbase.` and have no attributes containing database paths,
collection/index names, query text, document IDs, errors, actors or tenants.
Server instruments are simply not observed when the sampler has no server
source. Values above the OTel API's signed `int64` range saturate monotonically;
`Adapter.Stats()` reports that condition.

Contract version 7 includes the aggregate
`meldbase.index.build.retention_lease.active` and
`meldbase.index.build.retention.pressure` gauges. They carry no dynamic
attributes; the latter is set only when the build watermark is the binding
Commit Log retention boundary, distinguishing normal online-build lag from a
budget that needs operator attention.

Contract version 8 adds physical-generation and external rollback-anchor
sequence/generation, lag, failure, timeout and synchronous-latency instruments.
These remain fixed-cardinality engine aggregates and expose no database or
anchor-service identity.

These are embedded-engine aggregates, so the adapter deliberately does not emit
`db.client.*`. The OpenTelemetry database semantic conventions describe calls as
observed by a database client, while this adapter observes the engine itself.

The package targets OpenTelemetry Go Metrics API v1.38, the newest stable release
in the supported Go 1.23 toolchain line. Applications own SDK, reader, exporter,
resource and collection interval choices.
