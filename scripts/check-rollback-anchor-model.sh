#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
jar=${TLA2TOOLS_JAR:-}
workers=${TLC_WORKERS:-1}

if [[ -z "$jar" || ! -f "$jar" ]]; then
  echo "set TLA2TOOLS_JAR to an official tla2tools.jar" >&2
  exit 2
fi
if [[ ! "$workers" =~ ^[1-9][0-9]*$ ]]; then
  echo "TLC_WORKERS must be a positive integer" >&2
  exit 2
fi

java -cp "$jar" tla2sany.SANY "$root/specs/rollback_anchor_static.tla"
java -XX:+UseParallelGC -cp "$jar" tlc2.TLC \
  -deadlock \
  -workers "$workers" \
  -config "$root/specs/rollback_anchor_static.cfg" \
  "$root/specs/rollback_anchor_static.tla"
