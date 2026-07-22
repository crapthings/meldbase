# Physical power controller adapter protocol v1

This protocol is the stable boundary between Meldbase qualification evidence
and hardware-specific Redfish, IPMI, hypervisor or managed-PDU integrations.
Database and receipt schemas must not depend on a vendor API.

Meldbase includes a reference `meld-power-redfish-adapter` implementation for
the `redfish-computer-system-power-cycle` method.

The controller agent runs on a machine outside the system being cut. It
executes one preconfigured adapter executable with no command-line arguments.
Credentials are supplied by that controller host's service manager; they must
never appear in request, response, event or proof files. The adapter reads
exactly one JSON object from stdin and writes exactly one JSON object to stdout.
Extra JSON, unknown fields, output over 1 MiB, nonzero exit or timeout fail
closed.

Request:

```json
{
  "schemaVersion": 1,
  "controllerRunId": "<32 lowercase hex>",
  "trialId": "power-01-01",
  "method": "ipmi-chassis-power-cycle",
  "markerSha256": "<64 lowercase hex>",
  "bootIdBefore": "<marker boot ID>",
  "targetIdentitySha256": "<pre-approved server/chassis/outlet identity hash>",
  "requestedAt": "<RFC 3339 timestamp>"
}
```

Successful response:

```json
{
  "schemaVersion": 1,
  "controllerRunId": "<exact request value>",
  "trialId": "<exact request value>",
  "method": "<exact request value>",
  "operationId": "<bounded provider operation ID>",
  "targetIdentitySha256": "<hash of the configured hardware target identity>",
  "acceptedAt": "<provider accepted time>",
  "powerLostAt": "<independently observed power-off time>",
  "powerRestoredAt": "<independently observed power-on time>",
  "hardwareEvidenceSha256": "<hash of the retained provider/hardware log>",
  "hardwareEvidenceBase64": "<base64 of that exact redacted log, at most 512 KiB decoded>",
  "success": true
}
```

`acceptedAt >= requestedAt`, `powerLostAt >= acceptedAt`, and
`powerRestoredAt > powerLostAt` are mandatory. An API returning success is not
by itself a power-loss observation. The adapter must use a provider task,
chassis power state, PDU outlet telemetry or an equivalent independent signal
and retain the exact evidence whose SHA-256 it reports.
The controller agent decodes the bounded `hardwareEvidenceBase64`, recomputes
`hardwareEvidenceSha256` and embeds both in its signed proof, making the
qualification packet independently verifiable even if the controller host is
later unavailable. Adapter logs must therefore be redacted before return.
Before requesting power loss, the adapter must compare its configured target
with the request's `targetIdentitySha256`; the response must repeat it exactly.

The Meldbase controller agent hashes the exact adapter executable, captures the
strict exchange and stderr hash, requires a clean build from the qualified
revision, and signs the schema-2 event with an independent Ed25519 key. The
Linux controller executes the adapter through the same open file descriptor it
hashed, preventing path replacement between measurement and execution. The
qualification environment and immutable session plan bind that public-key
fingerprint and the pre-approved `targetIdentitySha256` before the first test.
The adapter response must identify that exact server, chassis or PDU outlet.
Recovery, the matrix and final manifest
reject unsigned physical events, key drift, response substitution and replay
across controller runs.

The signature attests what the configured adapter observed; it cannot make a
compromised adapter truthful. Protect the controller host and key separately
from the database host, restrict adapter replacement, and retain provider logs
on append-only or WORM storage when administrator tampering is in scope.
