#!/usr/bin/env bash
set -euo pipefail
umask 077

# Prints the runtime capacity benchmark options.
usage() {
  cat <<'EOF'
Usage:
  bash scripts/runtime-capacity-matrix.sh --out-dir NEW_DIRECTORY [--count 5] [--benchtime 3s]

Captures the core runtime capacity benchmark matrix as Go JSON. The benchmark
contains independent durable commits at parallelism 1/4/16, public write paths,
shared realtime fan-out, and reactive rebuild work. Output is evidence for one
machine and workload shape, not an SLA.
EOF
}

# Holds the new directory where benchmark evidence will be saved.
out_dir=""
# Sets how many times each benchmark runs.
count=5
# Sets the duration of each benchmark run.
benchtime=3s
while (($#)); do
  case "$1" in
    --out-dir) out_dir=${2:?missing value for --out-dir}; shift 2 ;;
    --count) count=${2:?missing value for --count}; shift 2 ;;
    --benchtime) benchtime=${2:?missing value for --benchtime}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$out_dir" && ! -e "$out_dir" ]] || { usage >&2; exit 2; }
[[ "$count" =~ ^[1-9][0-9]*$ ]] || { printf '%s\n' '--count must be positive' >&2; exit 2; }
mkdir -m 700 "$out_dir"

go version >"$out_dir/go-version.txt"
git rev-parse HEAD >"$out_dir/source-revision.txt"
git status --porcelain=v1 >"$out_dir/source-status.txt"
bash scripts/benchmark-core-runtime.sh -count="$count" -benchtime="$benchtime" -json >"$out_dir/core-runtime.json"

printf 'Runtime capacity matrix complete: %s\n' "$out_dir"
