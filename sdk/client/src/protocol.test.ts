import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { decodeProtocolDescriptor, MELDBASE_PROTOCOL_VERSION, supportsProtocol } from "./protocol.js";

interface FrameContract {
  readonly type: string;
  readonly required: readonly string[];
  readonly optional: readonly string[];
}

interface ProtocolV1Contract {
  readonly formatVersion: number;
  readonly protocolVersion: number;
  readonly realtimeTicketAccept: string;
  readonly realtimeCapabilities: { readonly base: readonly string[]; readonly conditional: readonly string[] };
  readonly workerCapabilities: readonly string[];
  readonly fixedErrorCodes: readonly string[];
  readonly clientFrames: readonly FrameContract[];
  readonly serverFrames: readonly FrameContract[];
  readonly nestedShapes: readonly { readonly name: string; readonly required: readonly string[]; readonly optional: readonly string[] }[];
}

test("TypeScript and Go share the immutable realtime protocol v1 contract", async () => {
  const url = new URL("../../../testdata/protocol-v1-contract.json", import.meta.url);
  const contract = JSON.parse(await readFile(url, "utf8")) as ProtocolV1Contract;
  assert.equal(contract.formatVersion, 1);
  assert.equal(contract.protocolVersion, MELDBASE_PROTOCOL_VERSION);
  assert.equal(contract.realtimeTicketAccept, "application/vnd.meldbase.realtime-ticket+json; capabilities=1");
  assert.deepEqual(decodeProtocolDescriptor({
    versions: [contract.protocolVersion],
    capabilities: [...contract.realtimeCapabilities.base],
  }), {
    versions: [MELDBASE_PROTOCOL_VERSION],
    capabilities: ["query.delta", "query.resume", "rpc", "rpc.cancel"],
  });
  assert.deepEqual(contract.realtimeCapabilities.conditional, ["rpc.idempotency", "rpc.transactional"]);
  assert.deepEqual(contract.workerCapabilities, [
    "cancel", "publication.policy", "rpc", "rpc.transactional",
    "transaction.compiled_update", "transaction.invalidate_publication", "transaction.point_operations",
  ]);
  assert.deepEqual(contract.fixedErrorCodes, [
    "cancelled", "database_unavailable", "delta_failed", "duplicate_key", "forbidden",
    "initial_snapshot_failed", "internal", "invalid_query", "invalid_request", "invalid_rpc_argument",
    "invalid_update", "mutation_limit_exceeded", "resource_limit_exceeded", "resume_failed", "rpc_busy",
    "rpc_canceled", "rpc_duplicate_request", "rpc_idempotency_conflict", "rpc_idempotency_required",
    "rpc_idempotency_unavailable", "rpc_in_progress", "rpc_not_found", "rpc_outcome_unknown",
    "rpc_result_invalid", "rpc_transaction_conflict", "rpc_transaction_requires_write", "snapshot_failed",
    "subscription_ended", "subscription_failed", "subscription_limit_or_duplicate", "worker_busy",
  ]);
  assert.deepEqual(contract.clientFrames, [
    { type: "authenticate", required: ["ticket", "type", "v"], optional: [] },
    { type: "call", required: ["arguments", "method", "requestId", "type", "v"], optional: ["idempotencyKey"] },
    { type: "cancel", required: ["requestId", "type", "v"], optional: [] },
    { type: "ping", required: ["type", "v"], optional: [] },
    { type: "subscribe", required: ["collection", "query", "requestId", "type", "v"], optional: ["mode", "resumeToken"] },
    { type: "unsubscribe", required: ["subscriptionId", "type", "v"], optional: [] },
  ]);
  assert.deepEqual(contract.serverFrames, [
    { type: "authenticated", required: ["type", "v"], optional: [] },
    { type: "delta", required: ["fromToken", "operations", "requestId", "subscriptionId", "token", "type", "v"], optional: [] },
    { type: "error", required: ["error", "requestId", "type", "v"], optional: [] },
    { type: "pong", required: ["type", "v"], optional: [] },
    { type: "result", required: ["requestId", "result", "type", "v"], optional: [] },
    { type: "resumed", required: ["requestId", "subscriptionId", "token", "type", "v"], optional: [] },
    { type: "resync_required", required: ["requestId", "type", "v"], optional: [] },
    { type: "snapshot", required: ["documents", "requestId", "subscriptionId", "token", "type", "v"], optional: [] },
  ]);
  assert.deepEqual(contract.nestedShapes, [
    { name: "delta.operation", required: ["id", "op"], optional: ["before", "document"] },
    { name: "error", required: ["code"], optional: [] },
  ]);
});

test("protocol descriptors are canonical, immutable, and forward additive", () => {
  const descriptor = decodeProtocolDescriptor({
    versions: [1, 2],
    capabilities: ["query.delta", "rpc", "rpc.future_extension"],
  });
  assert.deepEqual(descriptor, {
    versions: [1, 2],
    capabilities: ["query.delta", "rpc", "rpc.future_extension"],
  });
  assert.equal(Object.isFrozen(descriptor), true);
  assert.equal(Object.isFrozen(descriptor.versions), true);
  assert.equal(Object.isFrozen(descriptor.capabilities), true);
  assert.equal(supportsProtocol(descriptor, 1, ["query.delta", "rpc"]), true);
  assert.equal(supportsProtocol(descriptor, 3), false);
  assert.equal(supportsProtocol(descriptor, 1, ["rpc.missing"]), false);
});

test("protocol descriptors reject ambiguous or unbounded inputs", () => {
  for (const descriptor of [
    { versions: [1, 1], capabilities: [] },
    { versions: [2, 1], capabilities: [] },
    { versions: [1], capabilities: ["rpc", "query.delta"] },
    { versions: [1], capabilities: ["rpc", "rpc"] },
    { versions: [1], capabilities: ["Bad Capability"] },
    { versions: [1], capabilities: [], extra: true },
    { versions: Array.from({ length: 9 }, (_, index) => index + 1), capabilities: [] },
  ]) {
    assert.throws(() => decodeProtocolDescriptor(descriptor), /protocol/i);
  }
});
