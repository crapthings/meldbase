# Stable CLI contract

The following `meld` command names form the stable operational CLI contract:

| Command | Purpose | Machine-readable result |
| --- | --- | --- |
| `access-policy` | Validate or explain a strict access manifest | Human explanation / validation result |
| `init` | Create a local development bundle | Filesystem bundle |
| `serve` | Run the single-node HTTP/realtime service | Long-running process and JSONL logs |
| `inspect` | Read durable storage metadata | Versioned JSON report |
| `verify` | Verify offline storage structure and indexes | Versioned JSON report |
| `backup` / `restore` | Create or import physical recovery artifacts | `schemaVersion` + artifact receipt |
| `export` / `import` | Transfer portable logical archives | `schemaVersion` + archive receipt |
| `index-build` | Start, inspect, resume, abort, or rebuild an online index | `schemaVersion: 1` result |

`meld help`, `meld --help`, and `meld -h` succeed and list this set. Scripts
should invoke a concrete command with explicit flags, not parse the help text.
For commands that emit JSON, parse `schemaVersion` and `artifactKind`/`action`
before using fields; an incompatible output change requires a new schema version
and a changelog/migration entry.

Operational failures return a nonzero exit status and a concise `meld:` error
on stderr. Existing destination paths are never overwritten by backup, restore,
export, import, index-build artifacts, or similar publishing commands.

`demo` is instructional only. `anchor-*`, `qualification-*`, `storage-soak`,
and every `destructive-*` command are explicitly experimental operator tools:
they may change independently, require controlled infrastructure, or target
disposable storage. Do not place them in application deployment scripts.

The command set is stable; the database and Go server APIs remain alpha unless
their own lifecycle documentation says otherwise. See [API lifecycle]
(/api-lifecycle) and [production qualification](/production-qualification).
