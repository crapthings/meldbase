#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  single-node-backup-restore-drill.sh --meld /path/to/meld --db /path/to/app.meld2 --out-dir /new/rehearsal-dir [--timeout 10m] [--max-bytes N]

The database must be offline. The script creates --out-dir exactly once and
retains the backup, receipt, restored database, and verification reports as
rehearsal evidence. It never replaces an existing file.
EOF
}

meld=""
database=""
out_dir=""
timeout="10m"
max_bytes=""
while (($#)); do
  case "$1" in
    --meld) meld=${2:?missing value for --meld}; shift 2 ;;
    --db) database=${2:?missing value for --db}; shift 2 ;;
    --out-dir) out_dir=${2:?missing value for --out-dir}; shift 2 ;;
    --timeout) timeout=${2:?missing value for --timeout}; shift 2 ;;
    --max-bytes) max_bytes=${2:?missing value for --max-bytes}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$meld" || -z "$database" || -z "$out_dir" ]]; then
  usage >&2
  exit 2
fi
if [[ ! -x "$meld" ]]; then
  echo "meld binary is not executable: $meld" >&2
  exit 2
fi
if [[ ! -f "$database" ]]; then
  echo "database is not a regular file: $database" >&2
  exit 2
fi
if [[ -e "$out_dir" ]]; then
  echo "rehearsal output directory already exists: $out_dir" >&2
  exit 2
fi

mkdir "$out_dir"
artifact="$out_dir/physical-backup.meld2"
receipt="$out_dir/backup-receipt.json"
restored="$out_dir/restored.meld2"
restore_receipt="$out_dir/restore-receipt.json"

"$meld" inspect --db "$database" --require-compatible >"$out_dir/source-inspect.json"
"$meld" verify --db "$database" --timeout "$timeout" >"$out_dir/source-verify.json"
"$meld" backup --db "$database" --out "$artifact" --timeout "$timeout" >"$receipt"

restore_args=(restore --in "$artifact" --receipt "$receipt" --out "$restored" --timeout "$timeout")
if [[ -n "$max_bytes" ]]; then
 restore_args+=(--max-bytes "$max_bytes")
fi
"$meld" "${restore_args[@]}" >"$restore_receipt"
cmp "$receipt" "$restore_receipt"
"$meld" inspect --db "$restored" --require-compatible >"$out_dir/restored-inspect.json"
"$meld" verify --db "$restored" --timeout "$timeout" >"$out_dir/restored-verify.json"

printf 'Backup and restore drill passed. Evidence retained in %s\n' "$out_dir"
