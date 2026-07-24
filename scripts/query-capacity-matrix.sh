#!/usr/bin/env bash
set -euo pipefail
umask 077

usage() {
  cat <<'EOF'
Usage:
  bash scripts/query-capacity-matrix.sh --out-dir NEW_DIRECTORY [--documents 1024,8192,65536] [--iterations 200] [--warmup 20] [--profile standard] [--scenarios all] [--timeout 10m]

Runs the synthetic query observer at each requested data size for both memory
and durable backends. The output directory must not exist. Each JSON report
contains average/P50/P95/P99 successful-query latency plus Explain work and
resource-budget outcomes; it is platform evidence, not an SLA.
EOF
}

out_dir=""
documents="1024,8192,65536"
iterations=200
warmup=20
profile=standard
scenarios=all
timeout=10m
while (($#)); do
  case "$1" in
    --out-dir) out_dir=${2:?missing value for --out-dir}; shift 2 ;;
    --documents) documents=${2:?missing value for --documents}; shift 2 ;;
    --iterations) iterations=${2:?missing value for --iterations}; shift 2 ;;
    --warmup) warmup=${2:?missing value for --warmup}; shift 2 ;;
    --profile) profile=${2:?missing value for --profile}; shift 2 ;;
    --scenarios) scenarios=${2:?missing value for --scenarios}; shift 2 ;;
    --timeout) timeout=${2:?missing value for --timeout}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$out_dir" && ! -e "$out_dir" ]] || { usage >&2; exit 2; }
[[ "$documents" =~ ^[0-9]+(,[0-9]+)*$ ]] || { printf '%s\n' '--documents must be a comma-separated integer list' >&2; exit 2; }
for document_count in ${documents//,/ }; do
  (( 10#$document_count >= 128 && 10#$document_count <= 1000000 )) || { printf '%s\n' '--documents values must be between 128 and 1000000' >&2; exit 2; }
done
mkdir -m 700 "$out_dir"

IFS=, read -r -a sizes <<<"$documents"
for size in "${sizes[@]}"; do
  printf 'query capacity matrix: documents=%s\n' "$size"
  go run ./cmd/meld-query-observer \
    -backend both -documents "$size" -iterations "$iterations" -warmup "$warmup" \
    -profile "$profile" -scenarios "$scenarios" -timeout "$timeout" -format json \
    >"$out_dir/query-capacity-${size}.json"
done

printf 'Query capacity matrix complete: %s\n' "$out_dir"
