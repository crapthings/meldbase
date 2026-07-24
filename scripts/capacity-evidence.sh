#!/usr/bin/env bash
set -euo pipefail
umask 077

usage() {
  cat <<'EOF'
Usage:
  bash scripts/capacity-evidence.sh --out-dir NEW_DIRECTORY \
    [--documents 1024,8192,65536] [--iterations 200] \
    [--runtime-count 5] [--runtime-benchtime 3s] [--timeout 10m] \
    [--resource-limits-file PATH]

Captures an auditable target-machine capacity evidence bundle. It records a
non-identifying host capability summary, source revision/status, query observer
matrix, core-runtime benchmark matrix, and (when supplied) a SHA-256 digest of
the exact resource-limit configuration. The output represents only this
hardware and synthetic workload; it is not an SLA or a release gate.
EOF
}

out_dir=""
documents="1024,8192,65536"
iterations=200
runtime_count=5
runtime_benchtime=3s
timeout=10m
resource_limits_file=""
while (($#)); do
  case "$1" in
    --out-dir) out_dir=${2:?missing value for --out-dir}; shift 2 ;;
    --documents) documents=${2:?missing value for --documents}; shift 2 ;;
    --iterations) iterations=${2:?missing value for --iterations}; shift 2 ;;
    --runtime-count) runtime_count=${2:?missing value for --runtime-count}; shift 2 ;;
    --runtime-benchtime) runtime_benchtime=${2:?missing value for --runtime-benchtime}; shift 2 ;;
    --timeout) timeout=${2:?missing value for --timeout}; shift 2 ;;
    --resource-limits-file) resource_limits_file=${2:?missing value for --resource-limits-file}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$out_dir" && ! -e "$out_dir" ]] || { usage >&2; exit 2; }
[[ "$documents" =~ ^[0-9]+(,[0-9]+)*$ ]] || { printf '%s\n' '--documents must be a comma-separated integer list' >&2; exit 2; }
[[ "$iterations" =~ ^[1-9][0-9]*$ && "$runtime_count" =~ ^[1-9][0-9]*$ ]] || { printf '%s\n' 'iteration and count values must be positive' >&2; exit 2; }
for document_count in ${documents//,/ }; do
  (( 10#$document_count >= 128 && 10#$document_count <= 1000000 )) || { printf '%s\n' '--documents values must be between 128 and 1000000' >&2; exit 2; }
done
if [[ -n "$resource_limits_file" ]]; then
  [[ -f "$resource_limits_file" && -r "$resource_limits_file" ]] || { printf '%s\n' '--resource-limits-file must be a readable regular file' >&2; exit 2; }
fi

mkdir -m 700 "$out_dir"
host="$out_dir/host-capacity.txt"
{
  printf 'capturedAtUtc='; date -u +%Y-%m-%dT%H:%M:%SZ
  # Do not retain a hostname: capacity evidence needs OS/kernel/architecture,
  # not a host identifier.
  printf 'system\n'; uname -srm
  printf '\ngo version\n'; go version
  printf '\nfilesystem\n'; df -Pk "$out_dir"
  printf '\nresource limits\n'; ulimit -a
  if command -v lscpu >/dev/null 2>&1; then
    printf '\nlscpu\n'; lscpu
  elif command -v sysctl >/dev/null 2>&1; then
    printf '\nhardware\n'
    sysctl -n hw.model 2>/dev/null || true
    sysctl -n hw.ncpu 2>/dev/null || true
    sysctl -n hw.memsize 2>/dev/null || true
  fi
  if command -v free >/dev/null 2>&1; then
    printf '\nmemory\n'; free -b
  fi
} >"$host"
git rev-parse HEAD >"$out_dir/source-revision.txt"
git status --porcelain=v1 >"$out_dir/source-status.txt"
if [[ -n "$resource_limits_file" ]]; then
  if command -v sha256sum >/dev/null 2>&1; then
    digest=$(sha256sum "$resource_limits_file" | awk '{print $1}')
  elif command -v shasum >/dev/null 2>&1; then
    digest=$(shasum -a 256 "$resource_limits_file" | awk '{print $1}')
  else
    printf '%s\n' 'cannot hash resource limits: need sha256sum or shasum' >&2
    exit 1
  fi
  bytes=$(wc -c <"$resource_limits_file" | tr -d '[:space:]')
  {
    printf 'sha256=%s\n' "$digest"
    printf 'bytes=%s\n' "$bytes"
  } >"$out_dir/resource-limits.sha256"
fi

bash scripts/query-capacity-matrix.sh \
  --out-dir "$out_dir/query" --documents "$documents" --iterations "$iterations" \
  --timeout "$timeout"
bash scripts/runtime-capacity-matrix.sh \
  --out-dir "$out_dir/runtime" --count "$runtime_count" --benchtime "$runtime_benchtime"

printf 'Capacity evidence bundle complete: %s\n' "$out_dir"
