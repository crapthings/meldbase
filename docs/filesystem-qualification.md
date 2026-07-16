# Filesystem qualification

Meldbase currently tests Linux and macOS local-file behavior, but does not yet
claim that every filesystem, kernel, controller, virtualization layer or mount
policy on those operating systems is production-qualified. Filesystem evidence
is tied to one source revision and one actual target volume; an OS-level green
check is not transferable to another mount.

## Evidence levels

1. **Deterministic storage contracts** — unit, race, fault-injection, abrupt
   subprocess-exit and simulated `ENOSPC` tests prove that the implementation
   accepts only a complete old or complete new generation at every modeled
   boundary. These run on Linux and macOS CI.
2. **Volume capability receipt** — `meld durability-check` exercises the actual
   target directory's file/directory `fsync`, advisory-lock exclusion and
   close-release, no-overwrite link, same-directory rename, indexed V2
   commit/reopen and offline full-graph/index verification. Schema 2 binds the
   receipt to an optional claimed source revision, records the binary's actual
   VCS revision/dirty flag, and records OS/architecture/Go,
   filesystem type/name, device, block/capacity values, individual timings and
   the verified database SHA-256.
3. **Concurrent release soak** — the schema-4 `release` soak runs at least four
   measured hours of concurrent workers with race detection, 10,000 documents,
   12 complete reopen phases,
   writer/read/index-build/reclamation activity and a real reclamation conflict.
   It proves the live shadow index before abort and the final graph afterward,
   and records the temp directory's device, filesystem type/name and block size.
4. **Destructive platform qualification** — controlled power interruption and
   real capacity exhaustion on a disposable volume, using the intended kernel,
   filesystem, mount options, virtual/block device, cache/controller and flush
   policy. This cannot safely run inside `durability-check` on an application
   volume and remains external release evidence.

Levels 1–3 do not substitute for level 4. In particular, a successful `fsync`
return proves an API result, not that a particular controller survives loss of
power according to its advertised ordering guarantees.

## Qualification packet

For every production candidate and filesystem class, retain all of the
following against the exact release revision:

- a schema-2 volume receipt with every fixed check passed, a nonempty indexed
  database proof, matching claimed/build revisions and `buildModified=false`;
- a schema-4 release-soak receipt from the same target volume, not a `sentinel`
  or `custom` run;
- kernel/OS version, filesystem implementation/version, mount options, device or
  virtual-disk type, controller/cache/flush policy and test host identity in the
  operator's secured test record;
- real capacity-exhaustion results at each storage publication boundary and
  reopen verification;
- controlled process kill and power-cut results with complete old-or-new state,
  lock reacquisition and full offline verification;
- links or immutable hashes for logs and receipts, plus the date and responsible
  operator.

The public receipt deliberately does not record hostname, mount source, mount
options or controller identity because those can disclose infrastructure. The
secured qualification record must carry them separately.

## Current matrix

| Platform/filesystem class | CI contracts | Capability runner | Multi-hour release receipt | Real ENOSPC/power cut | Status |
|---|---:|---:|---:|---:|---|
| GitHub-hosted Linux runner filesystem | Yes | Automated on the same soak volume | Manual Level 3 packet available | No | portability evidence only |
| GitHub-hosted macOS APFS | Yes | Automated per revision | Not release evidence | No | portability sentinel only |
| Operator-selected Linux ext-family | Available | Available | Available | Pending | not qualified |
| Operator-selected Linux XFS/Btrfs | Available | Available | Available | Pending | not qualified |
| Operator-selected macOS APFS | Available | Available | Available | Pending | not qualified |
| Network/FUSE/overlay filesystems | Partial | Capability probe only | Not sufficient | Pending | unsupported for production claims |

“Available” means the repository contains the runner; it does not mean a valid
receipt has been produced and reviewed. Update this matrix only when immutable
evidence for the intended revision actually exists.

Run the non-destructive volume probe as:

```sh
go build -race -o /tmp/meldbase-qualification ./cmd/meld
/tmp/meldbase-qualification durability-check \
  --dir /path/to/target-volume \
  --source-revision "$(git rev-parse HEAD)" \
  --require-clean-source \
  > durability-check.json
```

Run it only in an existing directory on the exact target mount. The command
creates and removes a private temporary directory and never opens an existing
database. Build an explicit executable for qualification: some Go toolchain
`go run` paths omit VCS settings from the temporary binary, and
`--require-clean-source` correctly rejects evidence whose build revision cannot
be proven.

Run `meld storage-soak --dir` on that same target volume; its private database
and schema-4 receipt will then carry the same device, filesystem type/name and
block size as the capability receipt. The executable must be built explicitly
with `go build -race`; `go run` and Go test binaries may omit VCS settings and
cannot satisfy the clean release provenance check.

The command writes bounded aggregate progress heartbeats to stderr while the
canonical no-overwrite receipt remains isolated on stdout/`--out`. Retain the
heartbeat log for diagnosis, but treat only the schema-4 receipt as soak input
to `qualification-check`.

After both runs, verify their exact revision, profile, workload floors,
per-phase activity, semantic proofs and volume identity together:

```sh
/tmp/meldbase-qualification qualification-check \
  --source-revision "$(git rev-parse HEAD)" \
  --durability-receipt durability-check.json \
  --soak-receipt storage-soak-receipt.json \
  --require-level 3 \
  > qualification-level-3.json
```

The resulting schema-1 packet contains hashes of the exact input receipts and
sets `productionQualified` to false. Level 3 proves the normal capability and
concurrent soak contract; it is not power-loss qualification.

For Level 4, the secured destructive-test system produces a schema-1 JSON record
containing the same source and target-volume identity, the two exact receipt
hashes, start/finish times, a bounded `platformClass`, and these boolean fields:

- `capacityExhaustion`, `processKill`, `powerCut` and
  `publicationBoundariesCovered`;
- `lockReacquisition`, `oldOrNewStateOnly` and `offlineVerification`;
- `kernelAndMountRecorded`, `controllerPolicyRecorded` and
  `hostAndOperatorRecorded`.

It also contains `securedArtifactsSha256`, the SHA-256 of the access-controlled
logs and detailed infrastructure record. The public verifier does not ingest or
re-emit hostnames, mount sources, controller identity or operator identity:

```sh
/tmp/meldbase-qualification qualification-check \
  --source-revision "$(git rev-parse HEAD)" \
  --durability-receipt durability-check.json \
  --soak-receipt storage-soak-receipt.json \
  --destructive-record destructive-record.json \
  --require-level 4 \
  > qualification-level-4.json
```

The verifier rejects unknown receipt fields, oversized/non-regular inputs,
revision drift, legacy/sentinel/custom soaks, incomplete phase totals, mismatched
volumes, broken hashes and incomplete destructive assertions. A Level 4 packet
is therefore a deterministic index over operator-controlled evidence, not a
replacement for performing or reviewing the destructive tests.
