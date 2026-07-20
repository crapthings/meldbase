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
   close-release, no-overwrite link, same-directory rename, indexed
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
5. **Rollback-protection qualification** — one complete five-phase signed
   healthy/degraded/minority/recovered/rollback-rejected chain plus a separate
   schema-3 controller history produced by at least two mTLS agents. Every
   controller and agent must be a clean build of the same release revision;
   every operation must have an independently signed fragment and the history
   must pass the bounded linearizability checker.

Levels 1–3 do not substitute for level 4, and level 4 alone does not qualify the
remote rollback-protection trust plane. In particular, a successful `fsync`
return proves an API result, not that a particular controller survives loss of
power according to its advertised ordering guarantees. Only Level 5 sets
`productionQualified=true`.

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
- five ordered Ed25519-signed rollback-anchor phase receipts;
- a separate signed multi-agent concurrent-history receipt and exact schema-3
  controller history;
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

The resulting schema-2 packet contains hashes of the exact input receipts and
sets `productionQualified` to false. Level 3 proves the normal capability and
concurrent soak contract; it is not power-loss qualification.

For Level 4, the secured destructive-test system produces a schema-6 JSON
manifest containing the same source and target-volume identity, the two exact
receipt hashes, start/finish times and a bounded `platformClass`. It does not
accept operator-entered success booleans. Instead, `trials` contains one
machine-generated record per destructive event with:

- a unique ID, destructive kind and publication boundary;
- exact old, new and recovered commit sequences;
- distinct old/new logical-state hashes and the recovered-state hash;
- a derived `old` or `new` outcome;
- lock-reacquisition and full offline-verification results;
- hashes of the recovered database and that trial's secured artifacts.

Capacity-exhaustion and power-cut evidence must each include at least three
trials at all five publication boundaries. Process-kill evidence must include
at least twenty real asynchronous kills. Every
trial must recover exactly its old or new sequence and matching logical state;
one missing boundary, mismatched outcome, failed lock reacquisition or absent
offline proof rejects the whole record.

The manifest binds a strict machine-generated environment record. On Linux,
`qualification-environment-capture` records the clean binary revision, eligible
target/control devices, destructive token, exact mountinfo entry, kernel and
boot identity hashes, the complete sysfs block-device/slave chain, logical and
physical sectors, write-cache/FUA/rotational/scheduler/discard/stable-write
policy, the selected external controller method, a private host fingerprint and
the exact operator authorization artifact. The manifest derives separate
kernel/mount, controller and host/operator section hashes from that schema; it
does not accept arbitrary infrastructure text or operator-entered hashes.

Schema 6 additionally binds the immutable session-plan hash, final event hash
and exact retained executable hash. Manifest assembly and every Level 4/5
recomputation require all 20 indexed journal events, replay their hash chain,
bind their receipt digests to the manifest in the fixed order, check the five
power trial IDs/boundaries and prove non-overlapping completion chronology. A
generic artifact index containing the same receipts but no complete session is
not Level 4 evidence.

The environment record and authorization artifact are included in the
machine-generated content index for the complete access-controlled artifact
directory. The index uses
canonical relative paths, sizes and SHA-256 digests and rejects symlinks,
special files, path escape, missing files, added files and changed bytes. The
public verifier does not ingest or re-emit hostnames, mount sources, controller
identity or operator identity:

```sh
/tmp/meldbase-qualification qualification-check \
  --source-revision "$(git rev-parse HEAD)" \
  --durability-receipt durability-check.json \
  --soak-receipt storage-soak-receipt.json \
  --destructive-record destructive-record.json \
  --environment-record /secure/meldbase-campaign/infrastructure/qualification-environment.json \
  --artifacts-root /secure/meldbase-campaign \
  --artifacts-index secured-artifacts-index.json \
  --require-level 4 \
  > qualification-level-4.json
```

The verifier rejects unknown receipt fields, oversized/non-regular inputs,
revision drift, legacy/sentinel/custom soaks, incomplete phase totals, mismatched
volumes, broken hashes, legacy boolean-only destructive records, incomplete
publication-boundary coverage and inconsistent old/new state. A Level 4 packet
sets `storageQualified=true` but keeps `productionQualified=false`. It is a
deterministic index over machine-generated, operator-controlled evidence, not a
replacement for reviewing the secured test artifacts. The verifier uniquely
locates the process, capacity, corruption and every power receipt by digest in
the artifact index, reruns their artifact/database validators and reconstructs
the complete schema-6 manifest. It rejects a hand-edited manifest even when the
edited summary remains internally plausible.

## Assemble Level 5 release evidence

Run the five rollback-anchor phases and the separate multi-agent concurrent
history workflow from [rollback-anchor service qualification](rollback-anchor-service.md).
All phase receipts, the history receipt, controller and every agent fragment
must name the exact release revision and a clean binary build. The phase chain
and concurrent history must use different run IDs and different disposable
anchor configurations. Their external-evidence digests must also be distinct.

Generate a separate release-packet key. Do not reuse the rollback-anchor
qualification key:

```sh
/tmp/meldbase-qualification qualification-packet-keygen \
  --private /secure/release-packet.key \
  --public /secure/release-packet.pub
```

Bind every exact object into the final signed release envelope. The verifier
binary itself must be a clean build of the revision being qualified:

```sh
/tmp/meldbase-qualification qualification-check \
  --source-revision "$(git rev-parse HEAD)" \
  --durability-receipt durability-check.json \
  --soak-receipt storage-soak-receipt.json \
  --destructive-record destructive-record.json \
  --environment-record /secure/meldbase-campaign/infrastructure/qualification-environment.json \
  --artifacts-root /secure/meldbase-campaign \
  --artifacts-index secured-artifacts-index.json \
  --anchor-public-key anchor-qualification.pub \
  --anchor-phase-receipt 01-healthy.json \
  --anchor-phase-receipt 02-degraded.json \
  --anchor-phase-receipt 03-minority.json \
  --anchor-phase-receipt 04-recovered.json \
  --anchor-phase-receipt 05-rollback-rejected.json \
  --anchor-history-receipt history-receipt.json \
  --anchor-history controller-history.json \
  --require-level 5 \
  --release-signing-key /secure/release-packet.key \
  --out qualification-level-5.json \
  > /dev/null
```

The schema-1 release envelope embeds the schema-2 qualification result and
records the exact verifier build revision, dirty status, runtime identity,
signing time and independent release public key. Its qualification result hashes
all five phase receipts separately, the history receipt, exact controller
history, Anchor public key, destructive manifest, soak and volume receipt. It
sets `storageQualified`, `rollbackProtectionQualified` and
`productionQualified` only after all checks pass.

Offline verification requires every original evidence object. It checks the
release signature and then reruns the complete Level 5 verifier; checking only
the envelope signature is intentionally insufficient:

```sh
/tmp/meldbase-qualification qualification-packet-verify \
  --packet qualification-level-5.json \
  --release-public-key /secure/release-packet.pub \
  --source-revision "$(git rev-parse HEAD)" \
  --durability-receipt durability-check.json \
  --soak-receipt storage-soak-receipt.json \
  --destructive-record destructive-record.json \
  --environment-record /secure/meldbase-campaign/infrastructure/qualification-environment.json \
  --artifacts-root /secure/meldbase-campaign \
  --artifacts-index secured-artifacts-index.json \
  --anchor-public-key anchor-qualification.pub \
  --anchor-phase-receipt 01-healthy.json \
  --anchor-phase-receipt 02-degraded.json \
  --anchor-phase-receipt 03-minority.json \
  --anchor-phase-receipt 04-recovered.json \
  --anchor-phase-receipt 05-rollback-rejected.json \
  --anchor-history-receipt history-receipt.json \
  --anchor-history controller-history.json
```

The offline command itself must also be a clean build of the qualified
revision; this is checked before evidence is recomputed.

Missing partial evidence, wrong or reused keys, phase reordering/replay, dirty
or old verifier/agent builds, reused external evidence, changed controller
ordinals, a re-signed forged result and reuse of the phase anchor configuration
for concurrency testing all fail closed. The packet is created with
no-overwrite publication, file sync and parent-directory sync.

## Real asynchronous process-kill evidence

Build an explicit binary and run the process-kill controller on the disposable
target volume. The command creates and retains a private trial directory, starts
a separate writer, waits for its append-only oracle to be synced, sends the
worker a real `SIGKILL`, reacquires the database lock and performs complete
offline graph/index verification:

```sh
go build -o /tmp/meldbase-qualification ./cmd/meld
/tmp/meldbase-qualification destructive-process-check \
  --dir /path/to/disposable-target-volume \
  --out process-kill-receipt.json \
  --source-revision "$(git rev-parse HEAD)" \
  --require-clean-source
```

The schema-2 process receipt contains twenty independent
`process-kill`/`asynchronous` trials by default, which meets the destructive
manifest's minimum and can be incorporated into its `trials` set. Trials
alternate a kill after the synced `prepared` record and after the synced
`committed` record, proving lock release from both a pending-operation and an
acknowledged-operation process state without depending on whether a platform
delays `SIGKILL` while a physical flush is in progress. Its
append-only oracle records a synced `prepared` state before every database
commit and a synced `committed` state afterward. Recovery must match one of
those two adjacent states as well as the independently computed logical-state
hash. Each untouched crash image and oracle is copied to `--artifacts-dir`
(the receipt parent by default), reverified there, and the private target trial
directory is removed. This command proves process termination only; it does not claim real
capacity exhaustion or power-cut behavior.

## Real capacity-exhaustion evidence

Real ENOSPC qualification is Linux-only and fail-closed. It refuses the root or
workspace device, bind mounts sharing the parent device, nonempty targets,
targets larger than 16 GiB, unsupported filesystems, a control directory on the
target device, a control device with less than 64 MiB available and execution
as root. The operator must prepare a disposable
64 MiB–16 GiB ext-family, XFS or Btrfs volume, mount it as a distinct device,
make its root writable by an unprivileged qualification user, and keep the
control/evidence directory on another device.

First run the read-only preflight. It changes neither volume:

```sh
/tmp/meldbase-qualification destructive-volume-check \
  --dir /mnt/meldbase-disposable \
  --control-dir /var/lib/meldbase-qualification
```

The output binds the canonical mount path, device, filesystem and exact geometry
to a `destructiveToken`. Re-run preflight after any remount or reformat. Then
pass that exact token to the destructive runner:

```sh
/tmp/meldbase-qualification destructive-enospc-check \
  --dir /mnt/meldbase-disposable \
  --control-dir /var/lib/meldbase-qualification \
  --out /var/lib/meldbase-qualification/enospc-receipt.json \
  --destructive-token meldbase-enospc-... \
  --source-revision "$(git rev-parse HEAD)" \
  --require-clean-source
```

The controller runs at least three independent trials at each of the five
publication boundaries. A repository-internal synchronous hook records the
exact reached boundary on the separate control device. The controller then
uses real `fallocate` allocation down to one filesystem block until the kernel
returns `ENOSPC`, terminates the boundary-blocked worker with `SIGKILL`, removes
only its private fill file, copies the untouched crash image to the control
volume, runs the read-only protected-graph/index audit before any normal open,
then reacquires the database lock and accepts only the old or new generation.
Crash images and markers remain on the control volume after the target trial
directory is removed.

The command emits schema-1 capacity evidence and qualification trials suitable
for the schema-6 destructive manifest. Simulation tests exercise controller
logic in ordinary CI, but they are explicitly labeled `simulated-test-only` and
are never accepted as real Level 4 evidence. A valid operational receipt must
record `enospcOperation: "fallocate"` for every trial.

## External power-cut protocol

Power-cut evidence is deliberately two-stage because the process performing the
write must not survive to write its own success claim. It is Linux-only today.
For each trial, start `destructive-power-prepare` on the disposable volume:

```sh
/tmp/meldbase-qualification destructive-power-prepare \
  --dir /mnt/meldbase-disposable \
  --control-dir /var/lib/meldbase-qualification \
  --marker /var/lib/meldbase-qualification/power-001-marker.json \
  --trial-id power-001 \
  --boundary after-data-sync \
  --destructive-token meldbase-enospc-... \
  --source-revision "$(git rev-parse HEAD)" \
  --require-clean-source
```

The command seeds a complete old generation, reaches the selected physical
publication boundary, writes and syncs a schema-1 marker on the separate
control device, then blocks permanently. An external controller must hash that
exact marker and perform one of the accepted non-graceful actions:

- `qemu-system-reset`;
- `qemu-host-sigkill`;
- `hypervisor-hard-reset`;
- `ipmi-chassis-power-cycle`;
- `pdu-power-cycle`;
- `redfish-computer-system-power-cycle`.

QEMU methods use their native schema-1 QMP/process proof. Physical methods must
never use a hand-authored event: they require a schema-2 Ed25519-signed event
from an external controller agent. Generate its independent key on that host;
the private key must never be copied to the database host:

```sh
/opt/meldbase-controller destructive-power-controller-keygen \
  --private /secure/controller-agent.key \
  --public /secure/controller-agent.pub

/opt/meldbase-controller destructive-power-controller-run \
  --marker /shared-control/power-01-01-marker.json \
  --method ipmi-chassis-power-cycle \
  --target-identity-sha256 '<sha256-of-approved-server-chassis-or-outlet>' \
  --adapter /opt/meldbase/adapters/site-ipmi-power-cycle \
  --signing-key /secure/controller-agent.key \
  --proof /shared-control/power-01-01-controller-proof.json \
  --out /shared-control/power-01-01-controller.json \
  --source-revision "$(git rev-parse HEAD)" \
  --timeout 10m
```

The proof records the clean controller-agent revision/runtime, exact adapter
executable SHA-256, strict request/response, stderr hash, provider operation,
target identity hash and retained hardware-evidence hash. The signed event
binds that proof and marker. See
[physical controller adapter protocol v1](../specs/power-controller-adapter-v1.md).
Controller timestamps remain a separate clock domain from the guest marker;
causality comes from the marker hash and signed adapter exchange. Graceful
reboot, unsigned legacy physical events and key drift are rejected.

For QEMU, do not hand-author that event. Start the host-side controller before
or while the guest prepare command is blocked:

```sh
/tmp/meldbase-qualification destructive-qemu-reset \
  --marker /var/lib/meldbase-qualification/power-001-marker.json \
  --qmp-socket /run/qemu/meldbase-qmp.sock \
  --proof /var/lib/meldbase-qualification/power-001-qmp-proof.json \
  --out /var/lib/meldbase-qualification/power-001-controller.json \
  --timeout 2m
```

It performs the QMP capability handshake, captures `query-block`, sends exactly
`system_reset`, and requires both the command response and a host-originated
`RESET` event with `guest: false`. The ordered QMP transcript is written before
the controller event and its exact hash is bound into the event. Guest reboot,
an empty block inventory, reordered/duplicated commands or events, and a
modified transcript fail closed. The controller proof must be readable by the
non-root qualification UID; use the same UID on both sides or an access-
controlled group rather than making private evidence world-readable.

QMP `system_reset` is a guest hard reset, but the QEMU process survives. For a
stronger whole-VM power-loss analogue, let Meldbase supervise, kill and restart
the QEMU process itself:

```sh
/tmp/meldbase-qualification destructive-qemu-process-kill \
  --marker /control/power-001-marker.json \
  --qmp-socket /control/qmp.sock \
  --proof /control/power-001-qmp-proof.json \
  --out /control/power-001-controller.json \
  --qemu-log /control/power-001-qemu-process.log \
  --recovery-receipt /control/power-001-recovery.json \
  --target-image /control/target.img \
  --artifact-uid 65534 --artifact-gid 65534 --timeout 5m -- \
  /usr/bin/qemu-system-x86_64 '<bounded QEMU arguments>'
```

This controller requires the exact target image to appear in `query-block` as
writable with `direct=true` and `no-flush=false`; records hashes of the QEMU
executable and arguments; sends a real host `SIGKILL`; and requires the child
wait status to be signal 9. It starts a new QEMU PID with `-S`, proves the VM is
paused, publishes and ownership-syncs the private evidence, then sends QMP
`cont`. Every QMP exchange timestamp must be monotonic and inside the relevant
kill or restart phase. The command returns success only after the restarted
guest emits a power receipt whose retained artifacts pass full re-verification.

After the target boots again, run recovery with the same source build and target
geometry:

```sh
/tmp/meldbase-qualification destructive-power-recover \
  --dir /mnt/meldbase-disposable \
  --control-dir /var/lib/meldbase-qualification \
  --marker /var/lib/meldbase-qualification/power-001-marker.json \
  --controller-event /var/lib/meldbase-qualification/power-001-controller.json \
  --controller-proof /var/lib/meldbase-qualification/power-001-controller-proof.json \
  --controller-public-key /var/lib/meldbase-qualification/controller-agent.pub \
  --out /var/lib/meldbase-qualification/power-001-recovery.json \
  --destructive-token meldbase-enospc-...
```

Recovery rejects an unchanged `/proc/sys/kernel/random/boot_id`, a build or
volume mismatch, a marker not bound by the controller event, graceful methods,
unexpected files on the target and any state other than the complete old or new
generation. Before normal open, it copies the untouched crash image and runs
the full read-only graph/index audit; only then does it reacquire the writer
lock and verify logical state. Repeat at least three times for each of the five
boundaries. The resulting trial uses `triggerPoint: "external-power-cut"`.
Each receipt can be independently rechecked, including all retained artifacts:

For QEMU controller events, omit `--controller-public-key`; the trusted key is
mandatory only for physical controller methods.

```sh
/tmp/meldbase-qualification destructive-power-receipt-check \
  --receipt /var/lib/meldbase-qualification/power-001-recovery.json
```

After collecting the full set, verify duplicate receipt/trial/boot-transition
protection, a single controller method, common build/runtime/volume identity
and three-per-boundary coverage before assembly:

```sh
/tmp/meldbase-qualification destructive-power-matrix-check \
  --receipt power-001-recovery.json \
  --receipt power-002-recovery.json
```

Repeat `--receipt` for all 15 or more trials. The result contains the common
controller method, every exact receipt hash and a deterministic aggregate hash.

For an isolated QEMU/TCG integration matrix, the repository includes
`scripts/destructive-qemu-matrix.sh` and its minimal initramfs guest entrypoint.
The matrix script requires explicit Linux amd64 Meldbase, Alpine virt kernel,
base initramfs and modloop inputs; uses digest-pinned containers, a fresh 128
MiB ext4 image, QEMU `cache=none`, and runs all 15 trials. Passing a sixth source
revision argument enables `--require-clean-source`. Runs without that argument
are development evidence only and cannot qualify a release.
Set `MELDBASE_QEMU_CUT_MODE=host-sigkill` to run the stronger process-loss
matrix; the default remains `system-reset` so the two platform behaviors stay
separate and their evidence cannot be confused.

The QEMU matrix can append its power segment to an existing strict session.
That session must have been initialized and completed through the corruption
step inside the same Alpine guest runtime/device class as UID 65534, with the
same `/control/meld` bytes and a `qemu-system-reset` or `qemu-host-sigkill`
controller matching `MELDBASE_QEMU_CUT_MODE`:

```sh
MELDBASE_QUALIFICATION_PLAN="$CONTROL_DIR/.qualification-session/plan.json" \
MELDBASE_QUALIFICATION_UID=65534 \
MELDBASE_QEMU_CUT_MODE=host-sigkill \
  scripts/destructive-qemu-matrix.sh \
  "$CONTROL_DIR" "$MELD_LINUX_AMD64" "$VMLINUZ" "$INITRAMFS" "$MODLOOP" \
  "$(git rev-parse HEAD)"
```

Session mode uses the planned `power-01-01` through `power-05-03` identities,
checks the next boundary before each cut, validates and records every recovery
receipt, and skips already journaled trials on restart. If a valid recovery
receipt exists but was not yet journaled, it is revalidated and appended. If a
cut was interrupted after its marker, the target image is retained and the
script resumes recovery from the existing controller evidence; partial or
contradictory evidence fails closed and is preserved for inspection. The
legacy `matrix-*` mode remains an isolated emulator matrix and is not accepted
as a strict session.

## Silent-corruption campaign

Power interruption and short-write tests do not by themselves prove that an
accidental persistent bit flip is detected. Run the deterministic corruption
campaign against a checksum-valid, offline database:

```sh
/tmp/meldbase-qualification destructive-corruption-check \
  --database /var/lib/meldbase-qualification/source.meld \
  --page-samples 128 \
  --out /var/lib/meldbase-qualification/corruption-receipt.json

/tmp/meldbase-qualification destructive-corruption-receipt-check \
  --receipt /var/lib/meldbase-qualification/corruption-receipt.json
```

The source is never mutated and must begin with two fully valid Meta roots and
no partial tail. A private temporary copy is changed one bit at a time at 14
fixed offsets covering the Meta/page envelope, payload, checksum
region and page tail. Databases of at most 128 pages exercise every physical
page; larger inputs use a deterministic evenly-spaced 128-page sample including
both endpoints. After every mutation the complete read-only graph, fallback
Meta roots, published secondary indexes and shadow index builds are audited.
The only accepted results are a detected incompatible/corrupt file or a valid
generation named by one of the source's checksum-valid Meta slots. The verifier
restores the temporary byte and syncs it before the next independent mutation.

The receipt binds the exact source hash, page selection, offsets, baseline
verification and outcome counts. Its checker repeats the entire campaign and
requires the same result. This supplements but does not replace real power-cut,
controller-cache and media-error testing. SHA-256 detects accidental corruption;
it is not an authentication mechanism against an attacker who can rewrite both
content and checksums.

## Kernel-visible block write EIO

The corruption campaign changes bytes after the fact; it does not exercise a
live filesystem returning `EIO`. The repository therefore includes a separate
QEMU `blkdebug` runner:

```sh
scripts/destructive-qemu-eio.sh \
  /var/lib/meldbase-qualification/eio-control \
  /tmp/meldbase-linux-amd64 \
  vmlinuz-virt initramfs-virt modloop-virt
```

The runner creates a current-format fixture with a persisted reusable-page pool, places it
inside a fresh ext4 image, asks `debugfs` for the file's actual allocated block
numbers and generates one canonical rule for every corresponding 512-byte
sector. Every rule is explicitly `event="write_aio"`, `iotype="write"`,
`errno="5"` and `once="on"`; reads and unrelated filesystem sectors are not
eligible. The image is attached through QEMU with `cache=none`, so the host page
cache is bypassed while flushes remain enabled.

The guest performs a normal copy-on-write update that reuses those mapped
pages. Qualification passes only if the guest observes `EIO` for the first
write, the file remains write-poisoned, the previously committed value remains
readable, and a full offline graph/index/FreeSpace verification selects the old
sequence. The recovered database is copied to the independent 9p control
volume before the result is published.

The host-side controller independently requires one or more QMP
`BLOCK_IO_ERROR` events with `operation="write"`, `action="report"` and no
ENOSPC indication. Both pre-error and post-error `query-block` responses must
bind the exact `blkdebug:CONFIG:IMAGE` string, a writable raw device,
`direct=true` and `no-flush=false`. The strict config parser rejects default
all-I/O behavior, read injection, duplicate sectors, non-EIO errno and
repeat-forever rules. Recheck retained evidence with both commands:

```sh
/tmp/meldbase-linux-amd64 destructive-eio-result-check \
  --result /control/eio-write-aio-1-result.json
/tmp/meldbase-linux-amd64 destructive-qemu-eio-proof-check \
  --proof /control/eio-write-aio-1-proof.json \
  --result /control/eio-write-aio-1-result.json
/tmp/meldbase-linux-amd64 destructive-eio-bundle-check \
  --seed /control/eio-seed.json \
  --result /control/eio-write-aio-1-result.json \
  --proof /control/eio-write-aio-1-proof.json
```

The bundle check prevents evidence splicing: seed and guest must share the same
build, initial database hash and sequence; the QMP proof must bind the exact
guest-result hash; and the config, target image and recovered database must all
still exist and revalidate.

This is real guest-kernel write-error coverage, but remains separate from
flush-error, stale-read and hardware-media qualification. A write EIO result
must never be used as evidence that a device honors successful flushes.

## Kernel-visible flush-state EIO

Run the persistence-barrier failure case separately from the sector write
campaign:

```sh
scripts/destructive-qemu-flush-eio.sh \
  /var/lib/meldbase-qualification/flush-eio-control \
  /tmp/meldbase-linux-amd64 \
  vmlinuz-virt initramfs-virt modloop-virt
```

This runner creates the same reusable-page fixture inside ext4 and attaches the
image through an explicit named QEMU block graph with direct I/O, host flushes
enabled and virtio write cache advertised. The guest verifies the source and
durably publishes a `ready` receipt on the independent 9p control volume. Only
then does the controller create a new `blkdebug` node with the exact rule
`event=flush_to_disk`, `iotype=flush`, `errno=5`, `once=true`, atomically switch
the live raw node to it, re-query the graph, and durably publish an `armed`
receipt. The unprivileged worker cannot begin its update until it reads that
receipt.

QEMU 8.2 reports the matched ext4/virtio compound persistence request as
`BLOCK_IO_ERROR operation="write"`, even though the recorded injection rule is
flush-only. The proof preserves both facts instead of relabeling the event;
validators accept only the observed `write` or `flush` classification with
`action="report"`, non-ENOSPC and a reason. The tested failure is therefore a
QEMU flush-to-disk state failure, not evidence that the guest issued a standalone
FLUSH opcode.

Qualification is deliberately split across two boots. During the fault boot,
the first Meldbase transaction must return `EIO`, a second transaction must
return the retained poisoned `EIO` without publishing a generation, and the old
value must remain readable through the already-open fail-stop handle. The guest
durably publishes a fault receipt, waits for the controller's QMP proof, and is
then powered off without treating the aborted ext4 mount as an offline recovery
environment. A second guest boot runs without injection. Before mounting ext4,
it hashes the entire raw `/dev/vda` device and must match the size and SHA-256
captured by the host after the fault VM stopped. Only that exact stopped image
may proceed to journal replay, offline graph/FreeSpace verification and copying
the recovered database to the independent control volume. Fault and recovery
boot IDs must differ.

This separation is required because Linux may abort the ext4 journal and
remount the filesystem read-only when the persistence barrier reports EIO. A
read failure from that damaged live mount is not evidence that the database
cannot recover after reboot. The bundle cryptographically binds seed, ready,
armed, fault, QMP proof, stopped-image recovery plan, pre-mount raw-device
receipt and recovery result. The result binds all transition receipts, while
`armed.qmpArmSha256` binds the exact QMP exchange prefix through the post-arm
graph query. Recheck with:

```sh
/tmp/meldbase-linux-amd64 destructive-flush-eio-result-check \
  --result /control/flush-eio-1-result.json
/tmp/meldbase-linux-amd64 destructive-qemu-flush-eio-proof-check \
  --proof /control/flush-eio-1-proof.json \
  --ready /control/flush-eio-1-ready.json \
  --armed /control/flush-eio-1-armed.json \
  --fault /control/flush-eio-1-fault.json
/tmp/meldbase-linux-amd64 destructive-flush-eio-bundle-check \
  --seed /control/flush-eio-seed.json \
  --ready /control/flush-eio-1-ready.json \
  --armed /control/flush-eio-1-armed.json \
  --fault /control/flush-eio-1-fault.json \
  --recovery-plan /control/flush-eio-1-recovery-plan.json \
  --recovery-ready /control/flush-eio-1-recovery-ready.json \
  --result /control/flush-eio-1-result.json \
  --proof /control/flush-eio-1-proof.json
```

This does not test a device that acknowledges a flush but silently fails to
make it durable, nor a controller that later returns stale data. Those require
power removal beneath the relevant volatile cache and independent post-reboot
reads; neither may be inferred from a reported-EIO result.

## Flush-lie and rollback negative control

A database cannot make a durability guarantee when the storage layer reports a
successful persistence barrier but discards the acknowledged bytes. It also
cannot distinguish a checksum-valid old image from the current image using
only state stored inside that image. These are environmental and rollback
threats, not another recoverable torn-write boundary.

The repository includes a deliberately unsafe QEMU negative control:

```sh
scripts/destructive-qemu-volatile-loss.sh \
  /var/lib/meldbase-qualification/volatile-loss-control \
  /tmp/meldbase-linux-amd64 \
  vmlinuz-virt initramfs-virt modloop-virt
```

The durable ext4 base contains sequence 1. The first guest runs with QEMU's
explicit `-snapshot` temporary qcow2 overlay, commits sequence 2 successfully,
closes and fully verifies that generation, then durably publishes an
acknowledgement marker on independent 9p storage. The controller proves through
QMP that the writable qcow2 node has the exact durable base as its backing
image, sends real `SIGKILL`, verifies the process wait status, and requires the
base image hash to remain byte-for-byte unchanged. It copies that unchanged
base before starting a replacement QEMU process without `-snapshot`.

The replacement boot has a distinct boot ID. It first opens with
`MinimumCommitSequence=2`; qualification passes only when storage returns
`ErrStaleSnapshot` before changing the file. The negative-control recovery then
opens without that floor solely to prove the unsafe medium contains the valid
old sequence 1 and old logical value. A result is successful only as a
*negative control* when all three fields are true:

- `acknowledgedCommitLost`
- `monotonicFloorRejected`
- `unsafeStorageDetected`

It must never be added to the normal power matrix as passing durability
evidence. Its purpose is to prove that the qualification harness rejects a
flush-lie environment instead of calling an internally consistent old
generation safe.

The public open lifecycle exposes three non-format-breaking static rollback
gates for trusted callers:

- `RollbackProtection.ExpectedDatabaseID` rejects substitution of another
 database with `ErrDatabaseIdentity`.
- `RollbackProtection.MinimumCommitSequence` rejects a checksum-valid
  generation below an externally remembered sequence with
  `ErrRollbackDetected`.
- `RollbackProtection.MinimumGeneration` rejects loss of an acknowledged
  physical maintenance generation even when the logical sequence is unchanged.

Both checks occur after read-only graph selection but before recovery
truncation, FreeSpace repair or any other file mutation. A server, replication
group or operator must persist the identity, sequence and generation floor outside the
possibly stale device. Setting the floor from the same database file provides
no rollback protection.

For normal server operation, prefer `RollbackAnchorStore` instead of manually
supplying those static fields. `NewFileRollbackAnchorStore` atomically replaces,
fsyncs and reads back a checksummed identity/sequence/generation record,
serializes competing writers, and rejects identity changes or regression of
either monotonic coordinate. The
first audited provisioning uses `InitializeAnchor: true`; every later open must
find the anchor and omit that flag. Each acknowledged document, RPC/system
record, HTTP or WebSocket commit crosses the database durability barrier and
then the independent anchor barrier. Acknowledged index-build progress and
persistent reclamation generations cross the same barrier even when they do not
advance logical sequence. Each context-aware `Load`/`Advance`/read-back cycle is
bounded by `OperationTimeout` (10 seconds by default). Failure of the second
barrier is fail-stop:
the physical commit outcome is deliberately treated as ambiguous until restart
loads the ahead database and advances the anchor.

Compaction, migration and restore that produce a new database identity require
an explicit operator-controlled anchor initialization. Restoring an older copy
under an existing identity must remain rejected by the retained anchor; deleting
or resetting the anchor is a trust decision outside automatic recovery.

Recheck retained negative-control evidence with:

```sh
/tmp/meldbase-linux-amd64 destructive-qemu-volatile-loss-proof-check \
  --proof /control/volatile-loss-1-proof.json \
  --marker /control/volatile-loss-1-marker.json \
  --recovery-ready /control/volatile-loss-1-recovery-ready.json
/tmp/meldbase-linux-amd64 destructive-volatile-loss-bundle-check \
  --seed /control/volatile-loss-seed.json \
  --marker /control/volatile-loss-1-marker.json \
  --recovery-ready /control/volatile-loss-1-recovery-ready.json \
  --proof /control/volatile-loss-1-proof.json \
  --event /control/volatile-loss-1-controller.json \
  --result /control/volatile-loss-1-result.json
```

This deterministic emulator proves the rejection and evidence paths. Final
hardware qualification still requires cutting power below the actual device or
controller volatile cache. A real stale-read campaign also requires an
independent trusted sequence floor; otherwise returning a valid old image is
not locally observable as stale.

## Assemble Level 4 evidence

Do not hand-author the schema-6 destructive manifest. Before the first trial,
place the approved operator/change record under the secured control directory
and capture the still-empty target volume from the exact clean release binary:

```sh
/tmp/meldbase-qualification destructive-power-controller-keygen \
  --private /secure/controller-host/controller-agent.key \
  --public /secure/meldbase-campaign/infrastructure/controller-agent.pub

/tmp/meldbase-qualification qualification-environment-capture \
  --dir /mnt/meldbase-qualification \
  --control-dir /secure/meldbase-campaign \
  --controller-method ipmi-chassis-power-cycle \
  --controller-public-key /secure/meldbase-campaign/infrastructure/controller-agent.pub \
  --controller-target-identity-sha256 '<sha256-of-approved-server-chassis-or-outlet>' \
  --operator-evidence /secure/meldbase-campaign/infrastructure/operator-authorization.json \
  --source-revision "$(git rev-parse HEAD)" \
  --out /secure/meldbase-campaign/infrastructure/qualification-environment.json
```

The capture command is Linux-only and repeats the destructive-volume safety
preflight: the caller must be non-root, the target must be an empty independent
mount of bounded size, and the control/workspace devices must be separate.
It hashes host/model identifiers before publication, but it is not TPM remote
attestation and cannot make a compromised qualification host truthful. The
secured operator record, independent controller evidence and physical campaign
boundary remain part of the trust model.

Initialize the campaign journal before running the durability probe. The journal
freezes the clean revision, platform class, environment digest, target/control
volume, controller method, runtime and non-root operator UID. It also copies and
hashes the exact executing binary into the session directory, then creates a
fixed 20-step sequence: durability, soak, process-kill, ENOSPC, corruption,
then three real power interruptions at each of the five publication boundaries.
Every later session command must run as the same UID from byte-identical code.

On a prepared Linux qualification host, the repository driver runs and
resumes the five pre-power stages without weakening any destructive preflight:

```sh
scripts/qualification-linux-foundation.sh \
  /secure/meldbase-campaign \
  /mnt/meldbase-qualification \
  /tmp/meldbase-qualification \
  "$(git rev-parse HEAD)" \
  linux-ext4-nvme \
  ipmi-chassis-power-cycle \
  /secure/meldbase-campaign/infrastructure/operator-authorization.json \
  /secure/meldbase-campaign/infrastructure/controller-agent.pub \
  '<sha256-of-approved-server-chassis-or-outlet>'
```

This is intentionally Linux/Bash 4+ and refuses root. It captures the
environment, initializes the session, runs the durable capability receipt,
release soak, 20-process-kill set, real ENOSPC matrix and corruption campaign,
recording each result before advancing. It stops at `power-01-01`; it never
attempts a physical power operation. A restart revalidates and records a fully
published receipt that was produced just before interruption. Partial files or
dirty target state are preserved and cause the underlying no-overwrite/safety
checks to fail rather than being silently deleted. The durability command's
`--out` path uses exclusive create, file `fsync` and parent-directory `fsync`,
so it does not depend on unsafe shell redirection for session evidence.

```sh
/tmp/meldbase-qualification qualification-session-init \
  --artifacts-root /secure/meldbase-campaign \
  --environment-record /secure/meldbase-campaign/infrastructure/qualification-environment.json \
  --source-revision "$(git rev-parse HEAD)" \
  --platform-class linux-ext4-nvme

/tmp/meldbase-qualification qualification-session-status \
  --plan /secure/meldbase-campaign/.qualification-session/plan.json
```

After each command durably publishes its receipt and referenced artifacts under
the campaign root, record exactly the `kind` reported by `status`:

```sh
/tmp/meldbase-qualification qualification-session-record \
  --plan /secure/meldbase-campaign/.qualification-session/plan.json \
  --kind durability \
  --receipt /secure/meldbase-campaign/receipts/durability-check.json
```

For power steps, use the `powerTrialId` and `publicationBoundary` returned by
`status` as the destructive-power trial identity and boundary. Each append
fully revalidates the existing chain and the new receipt. Events are
no-overwrite files linked by hashes and bind the receipt path, digest and
completion chronology; duplicate receipts, boot transitions, revision/runtime
or volume drift, controller substitution, missing artifacts and out-of-order
steps fail closed. The session directory itself cannot be used as a receipt
location.

For a physical-controller session, do not transcribe those fields manually.
At any point, inspect the current trial phase and canonical paths with:

```sh
/tmp/meldbase-qualification qualification-session-power-status \
  --plan /secure/meldbase-campaign/.qualification-session/plan.json
```

It reports exactly one of `prepare`, `controller`, `recover` or `record` and
cryptographically validates every artifact required to reach that phase. A
half-published controller pair, stale marker, wrong signing key or target
identity fails instead of advancing the suggested action.

On the database host, this command derives the exact next trial, boundary,
volume token and evidence paths from the immutable plan, verifies that none of
the four trial outputs already exists, publishes the marker and waits for the
external cut:

```sh
/tmp/meldbase-qualification qualification-session-power-prepare \
  --plan /secure/meldbase-campaign/.qualification-session/plan.json
```

For `power-01-01`, the independent controller host then consumes
`/secure/meldbase-campaign/power-01-01-marker.json` and must publish
`power-01-01-controller-proof.json` plus `power-01-01-controller.json` in the
same shared control directory using `destructive-power-controller-run`. Pass
the same target identity frozen in the session environment and plan.

After the database host boots again, the session-level recovery command checks
the retained public key and controller proof target against the immutable plan
before touching the crash image. It derives all remaining paths, creates the
recovery receipt, fully validates it and appends the exact next journal event:

```sh
/tmp/meldbase-qualification qualification-session-power-recover \
  --plan /secure/meldbase-campaign/.qualification-session/plan.json \
  --controller-public-key /secure/meldbase-campaign/infrastructure/controller-agent.pub
```

If recovery durably published its receipt but the process stopped before the
journal append, rerunning the same command revalidates and records that receipt
without reopening the recovered database. Repeat prepare, external controller
run and recovery until `qualification-session-status` reports
`readyToSeal: true`.

Once `readyToSeal` is true, semantically replay the entire campaign and create
the exact-tree index outside the indexed root:

```sh
/tmp/meldbase-qualification qualification-session-seal \
  --plan /secure/meldbase-campaign/.qualification-session/plan.json \
  --out secured-artifacts-index.json
```

The journal prevents accidental or partial evidence mixing and makes changes
detectable by the final index and Level 4/5 signatures. It is not a trusted
timestamp or protection against an administrator who can rewrite the complete
tree and recompute the whole unsigned journal. Keep the campaign root on
access-controlled append-only/WORM storage or externally anchor periodic event
hashes when that stronger adversary is in scope.

After collecting one
schema-2 process receipt, one schema-1 capacity receipt, one reproducible
schema-1 corruption receipt and enough schema-1 power recovery receipts, place
every receipt, crash image, oracle, marker and controller proof under the same
quiescent access-controlled directory. The
index itself and final manifest must be outside that directory to avoid a
self-reference. Level 4 requires the session seal above. The generic commands
below remain useful for independently rechecking the sealed tree, but cannot
upgrade a legacy/manual campaign that lacks the complete session journal:

```sh
/tmp/meldbase-qualification qualification-artifacts-index-build \
  --root /secure/meldbase-campaign \
  --source-revision "$(git rev-parse HEAD)" \
  --out secured-artifacts-index.json

/tmp/meldbase-qualification qualification-artifacts-index-verify \
  --root /secure/meldbase-campaign \
  --index secured-artifacts-index.json \
  --source-revision "$(git rev-parse HEAD)"
```

The complete directory may later be copied or mounted at a different root
without rewriting any retained receipt. Pass the relocated root and relocated
top-level durability, soak and environment paths to the verifier. References
inside old receipts remain byte-identical: the verifier resolves them by exact
content digest and the longest matching canonical relative-path suffix in the
index. It rejects flattening, renamed layouts, path escape and any repeated
digest that cannot be uniquely disambiguated. The index and final manifest stay
outside the tree and remain unchanged.

Then build the manifest from those indexed artifacts:

```sh
/tmp/meldbase-qualification destructive-manifest-build \
  --durability-receipt /secure/meldbase-campaign/receipts/durability-check.json \
  --soak-receipt /secure/meldbase-campaign/receipts/storage-soak-receipt.json \
  --process-receipt /secure/meldbase-campaign/receipts/process-kill-receipt.json \
  --capacity-receipt /secure/meldbase-campaign/receipts/enospc-receipt.json \
  --corruption-receipt /secure/meldbase-campaign/receipts/corruption-receipt.json \
  --power-receipt /secure/meldbase-campaign/receipts/power-001-recovery.json \
  --power-receipt /secure/meldbase-campaign/receipts/power-002-recovery.json \
  --source-revision "$(git rev-parse HEAD)" \
  --platform-class linux-ext4-nvme \
  --artifacts-root /secure/meldbase-campaign \
  --artifacts-index secured-artifacts-index.json \
  --environment-record /secure/meldbase-campaign/infrastructure/qualification-environment.json \
  --out destructive-record.json
```

Repeat `--power-receipt` for every trial. The assembler reopens and rehashes
every retained crash image, oracle, marker, controller event and controller
proof; repeats the complete silent-corruption campaign; validates clean
source/runtime/volume identity; requires the 20 balanced process kills and the
three-per-boundary capacity/power floors; derives all old/new outcomes; and
stores the exact process, capacity, corruption and power receipt hashes in the
manifest. Every input and every artifact path referenced by those receipts must
be present in the exact index; the assembler rehashes the whole directory again
before publication so files cannot be added or changed during assembly.
`qualification-check --require-level 4` then binds that manifest to the exact
Level 2 and Level 3 receipts, uniquely locates all original destructive
receipts by their indexed digests, repeats their offline checks, rebuilds the
manifest and sets only `storageQualified=true`. Unknown fields, missing or
duplicate digests, reordered/changed trials, changed aggregate timing, missing
artifacts and manually invented success booleans fail closed.
Production release qualification additionally requires the Level 5 trust-plane
inputs described above.
