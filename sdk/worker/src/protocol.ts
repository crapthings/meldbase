import { decodeProtocolDescriptor, MELDBASE_PROTOCOL_VERSION, supportsProtocol } from "@meldbase/client/internal";
import type { ProtocolDescriptor } from "@meldbase/client";

import { MeldbaseWorkerProtocolError } from "./errors.js";
import type { ReadPolicyDefinition, RPCDefinition } from "./types.js";

export function validateWorkerProtocol(
  rawDescriptor: unknown | undefined,
  methods: ReadonlyMap<string, RPCDefinition>,
  readPolicies: ReadonlyMap<string, ReadPolicyDefinition>,
): ProtocolDescriptor {
  if (rawDescriptor === undefined) {
    throw new MeldbaseWorkerProtocolError(["protocol.discovery"]);
  }
  let descriptor: ProtocolDescriptor;
  try {
    descriptor = decodeProtocolDescriptor(rawDescriptor);
  } catch {
    throw new MeldbaseWorkerProtocolError(Object.freeze(["valid_descriptor"]));
  }
  const required = new Set<string>();
  if ([...methods.values()].some((method) => method.mode === "rpc")) required.add("rpc");
  if ([...methods.values()].some((method) => method.mode === "transactional")) {
    required.add("rpc.transactional");
    required.add("transaction.compiled_update");
    required.add("transaction.invalidate_read_policy");
    required.add("transaction.point_operations");
  }
  if (readPolicies.size > 0) required.add("read_policy");
  const missing = [...required].filter((capability) => !descriptor.capabilities.includes(capability));
  if (!supportsProtocol(descriptor, MELDBASE_PROTOCOL_VERSION) || missing.length > 0) {
    if (!descriptor.versions.includes(MELDBASE_PROTOCOL_VERSION))
      missing.unshift(`version.${MELDBASE_PROTOCOL_VERSION}`);
    throw new MeldbaseWorkerProtocolError(Object.freeze(missing));
  }
  return descriptor;
}
