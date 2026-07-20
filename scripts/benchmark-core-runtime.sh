#!/usr/bin/env bash
set -euo pipefail

# Keep this runner intentionally narrow and machine-neutral. Use a dedicated
# target volume and record the resulting benchmark/pprof evidence before making
# a commit-path change; do not compare laptop numbers as release gates.
go test ./... -run '^$' -bench '^(BenchmarkV2ConcurrentIndependentCommits|BenchmarkV2PublicInsertPair|BenchmarkV2PublicWriteTransactionPair|BenchmarkV2SharedRealtimeFanout|BenchmarkV2ReactiveViewRebuild)$' -benchmem "$@"
