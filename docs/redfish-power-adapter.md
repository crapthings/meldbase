# Redfish physical power adapter

`meld-power-redfish-adapter` is the reference implementation of physical power
controller protocol v1 for a Redfish `ComputerSystem`. It runs only on the
independent controller host. Never install its credentials or evidence
directory on the database host.

It deliberately does not use `ForceRestart`. For every accepted request it:

1. reads the configured `ComputerSystem` over authenticated HTTPS;
2. computes a stable identity from its exact system URI, UUID, serial number,
   manufacturer and model;
3. compares that digest with the pre-approved request target before any action;
4. requires the service to advertise `ForceOff` and `On` or `ForceOn`;
5. posts `ForceOff` and independently observes `PowerState: Off`;
6. posts `On` or `ForceOn` and independently observes `PowerState: On`;
7. durably creates a redacted hardware log and returns both its SHA-256 and a
   bounded base64 copy for inclusion in the signed controller proof.

This follows DMTF's definition of `ForceOff` as an immediate non-graceful power
off and its requirement that completed off/on transitions be visible through
`PowerState` in the
[Redfish Data Model Specification](https://redfish.dmtf.org/schemas/v1/DSP0268_2025.3.pdf)
and uses the standard `ComputerSystem.Reset` action described by the
[Redfish Specification](https://www.dmtf.org/sites/default/files/standards/documents/DSP0266_1.20.2.html).
A Redfish `Off` state can still leave components on auxiliary
power. If the qualification claim specifically requires loss of power below a
drive or controller volatile cache, use an independently metered PDU outlet or
another controller that proves that lower electrical boundary.

## Build and configure

Build the adapter from the same clean revision as the controller agent:

```sh
go build -trimpath -o /opt/meldbase-controller/meld-power-redfish-adapter \
  ./cmd/meld-power-redfish-adapter
```

Supply credentials through the controller service manager, never command-line
arguments:

```sh
export MELDBASE_REDFISH_BASE_URL='https://bmc.example.internal'
export MELDBASE_REDFISH_SYSTEM_URI='/redfish/v1/Systems/System.Embedded.1'
export MELDBASE_REDFISH_USERNAME='qualification-operator'
export MELDBASE_REDFISH_PASSWORD='<service-secret>'
export MELDBASE_REDFISH_CA_FILE='/secure/controller/redfish-ca.pem'
export MELDBASE_REDFISH_EVIDENCE_DIR='/secure/controller/redfish-evidence'
```

HTTPS hostname verification is mandatory. Redirects are rejected so Basic
credentials cannot be redirected to another origin. The optional timing knobs
are `MELDBASE_REDFISH_POLL_INTERVAL` (default `2s`),
`MELDBASE_REDFISH_REQUEST_TIMEOUT` (default `15s`) and
`MELDBASE_REDFISH_OPERATION_TIMEOUT` (default `10m`); all have hard bounds.

Use a dedicated least-privilege BMC account that can read the selected system
and invoke only its reset action. Protect the evidence directory with exclusive
controller-host access and append-only or WORM retention when administrator
tampering is in scope.

## Freeze the target identity

Before environment capture, query the identity without changing power state:

```sh
/opt/meldbase-controller/meld-power-redfish-adapter --identity
```

Review the returned URI, UUID, serial, manufacturer and model against inventory.
Copy only its `sha256` value into
`--controller-target-identity-sha256` during environment capture. That digest
is frozen into the environment record and immutable session plan.

## Execute a trial

After the database host publishes the exact session marker, run on the
controller host:

```sh
/opt/meldbase-controller/meld destructive-power-controller-run \
  --marker /shared/meldbase-campaign/power-01-01-marker.json \
  --method redfish-computer-system-power-cycle \
  --target-identity-sha256 '<frozen-identity-sha256>' \
  --adapter /opt/meldbase-controller/meld-power-redfish-adapter \
  --signing-key /secure/controller/controller-agent.key \
  --proof /shared/meldbase-campaign/power-01-01-controller-proof.json \
  --out /shared/meldbase-campaign/power-01-01-controller.json \
  --source-revision '<clean-source-revision>' \
  --timeout 15m
```

The controller agent hashes and executes the adapter through the same open file
descriptor. The adapter refuses a pre-existing hardware-log path before it
sends `ForceOff`. On failures after a possible off request, it makes a bounded
best-effort `On` request and retains a failed-attempt log; it never reports a
successful protocol response without observing both states and durably syncing
the successful log. The self-contained proof remains independently auditable
even if the controller host's retained copy later becomes unavailable.
