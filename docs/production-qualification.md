# Production qualification

This page describes repeatable evidence for a **single durable Meldbase node**.
It is a release-engineering runbook, not a claim that every filesystem or power
controller has been qualified. Run it only from a clean, revision-stamped build
and retain the generated receipts with the release record.

## Readiness boundary

The repository's internal-use gate is covered by automated query/update,
durable recovery, SDK, realtime, backup/restore, and monitoring contracts.
That establishes a reviewed development baseline; it does not substitute for
the target-machine evidence below. A small production rollout additionally
requires the selected storage/backup drill, security/policy review, upgrade
rehearsal, and capacity evidence for its actual workload. Do not infer a
throughput, subscription, or availability commitment from a green unit-test
run alone.

## Continuous checks

The ordinary test gate exercises the database contract, including the
memory/durable concurrent query, write, and realtime convergence test:

```sh
go test ./...
go test -race ./...
go vet ./...
```

For machine-neutral throughput and allocation trends, run the narrow core
benchmark set and keep the complete output. It is intentionally not a release
threshold: compare only results taken on the same hardware, operating system,
Go version, and workload shape.

```sh
bash scripts/benchmark-core-runtime.sh -count=5
```

This includes indexed point/range reads, durable write-and-sync, index creation,
independent concurrent commits, public writes, shared realtime fan-out, and
reactive rebuild work. Capture the Go JSON form alongside the human output when
comparing a target platform:

```sh
bash scripts/benchmark-core-runtime.sh -count=5 -json > core-runtime-benchmark.json
```

The query observer records successful-run average, P50, P95, and P99 latency
per scenario in its normal JSON and table reports. Use at least enough measured
iterations for a percentile to be meaningful; `json-stable` intentionally
omits all timing values and remains the structural-regression format.

For a data-size matrix across both executors, create a new evidence directory:

```sh
bash scripts/query-capacity-matrix.sh \
  --out-dir /var/lib/meldbase-qualification/query-capacity-20260724 \
  --documents 1024,8192,65536 \
  --iterations 200 --warmup 20
```

It retains one normal JSON report per size. Preserve the command, hardware and
filesystem details, Go version, source revision, and all output files together.

For concurrent writes and realtime fan-out, use the runtime matrix runner:

```sh
bash scripts/runtime-capacity-matrix.sh \
  --out-dir /var/lib/meldbase-qualification/runtime-capacity-20260724 \
  --count 5 --benchtime 3s
```

It records Go version, revision, dirty-state evidence, and Go's benchmark JSON.
The benchmark includes durable independent commits at parallelism 1/4/16,
public writes, shared realtime fan-out, and reactive rebuilds.

For query-plan and budget regressions, use the observer in both executors. Its
stable JSON output is suitable for an explicitly reviewed CI baseline; timing
output is trend evidence, not a golden value.

```sh
go run ./cmd/meld-query-observer \
  -profile matrix -backend both -format json-stable \
  > query-observer-stable.json
```

See [Query semantics](/query-semantics#synthetic-workload-observer)
for scenario meanings and the separate query-budget pressure runs.

## Long-running storage evidence

The scheduled `storage-soak` workflow builds a race-enabled, VCS-stamped
binary and verifies concurrent writes, snapshot reads, online index catch-up,
page reclamation, and repeated close/reopen recovery. Its `release` profile is
deliberately substantial: at least four hours, 10,000 documents, and 12
reopens. The workflow stores the versioned receipt and log as CI artifacts.

For a manual release run, use a dedicated writable target directory on the
actual filesystem class to qualify. It must contain no production database;
the command creates and later removes its own temporary probe directory.

```sh
revision=$(git rev-parse HEAD)
go build -race -o ./meld-qualification ./cmd/meld
./meld-qualification storage-soak \
  --dir /mnt/meldbase-qualification-target \
  --out /var/lib/meldbase-qualification/storage-soak.json \
  --profile release \
  --source-revision "$revision" \
  --require-clean-source
```

Do not substitute short `custom` or `sentinel` runs for release evidence. They
are useful exploratory checks only.

## Destructive fault campaigns

Process-kill, ENOSPC, corruption, block-write I/O error, and power-cut tools
are intentionally separated from normal tests because they destroy their
target volume or VM image. The Linux foundation script establishes an ordered,
receipt-bound campaign covering durability, the release soak, abrupt process
termination, ENOSPC boundaries, and corruption sampling. It then stops before
the separately controlled power phase.

Use a disposable filesystem, an independent artifact directory, and an
operator approval/evidence file. Never point either path at a live database,
and do not run the command as root.

```sh
scripts/qualification-linux-foundation.sh \
  /var/lib/meldbase-qualification-artifacts \
  /mnt/disposable-meldbase-volume \
  /usr/local/bin/meld \
  "$(git rev-parse HEAD)" \
  linux-ext4-nvme \
  qemu-system-reset \
  /var/lib/meldbase-qualification-artifacts/operator-approval.json
```

For real physical power controllers, the script requires an attested controller
public key and target identity digest in addition to those arguments. The QEMU
matrix and I/O-error helpers are under `scripts/destructive-*.sh`; run them
only after the foundation receipts have been recorded in the qualification
session. The commands fail closed on unsafe paths, missing evidence, stale
revisions, and unbound receipts.

## What to retain and how to decide

Retain the source revision, binary identity, target filesystem identity, soak
receipt, destructive receipts, controller evidence, logs, and benchmark/
observer reports. Investigate any failed verification, recovery, or invariant
before retrying; a successful rerun does not erase the failed evidence.

This evidence validates selected single-node paths. It does not establish an
availability SLA, cross-node failover, automatic restore drills, a universal
power-loss guarantee, or capacity figures for a different workload. Use the
observed receipts to set deployment-specific limits, then revisit them when the
hardware, Go version, storage class, or access pattern changes.
