import { DEFAULT_QUERY_LIMITS } from "@meldbase/client";
import { decodeQuerySpec } from "@meldbase/client/internal";

import type {
  ReadPolicyDefinition,
  ReadPolicyHandler,
  ReadPolicyOptions,
  RPCDefinition,
  RPCHandler,
  TransactionalRPCHandler,
} from "./types.js";

function standardRPC(handler: RPCHandler): RPCDefinition {
  if (typeof handler !== "function") throw new TypeError("RPC handler must be a function");
  return { mode: "rpc", handler };
}

function transactionalRPC(handler: TransactionalRPCHandler): RPCDefinition {
  if (typeof handler !== "function") throw new TypeError("Transactional RPC handler must be a function");
  return { mode: "transactional", handler };
}

/** Declares a named RPC handler. `rpc.transactional` adds atomic Meldbase point writes. */
export const rpc = Object.assign(standardRPC, { transactional: transactionalRPC });

export function readPolicy(options: ReadPolicyOptions, handler: ReadPolicyHandler): ReadPolicyDefinition {
  if (typeof handler !== "function") throw new TypeError("Read-policy handler must be a function");
  validateReadPolicyOptions(options);
  return { ...options, handler };
}

export function validateReadPolicyOptions(options: ReadPolicyOptions): void {
  const encodedVersion =
    options && typeof options.version === "string" ? new TextEncoder().encode(options.version) : new Uint8Array();
  if (
    !options ||
    typeof options.version !== "string" ||
    options.version.length === 0 ||
    encodedVersion.byteLength > 128 ||
    new TextDecoder().decode(encodedVersion) !== options.version
  ) {
    throw new TypeError("Read-policy version must contain between 1 and 128 UTF-8 bytes");
  }
  if (
    !Number.isSafeInteger(options.maxResults) ||
    options.maxResults <= 0 ||
    options.maxResults > DEFAULT_QUERY_LIMITS.maxLimit
  ) {
    throw new TypeError("Read-policy maxResults is outside query limits");
  }
  validatePolicyFields(options.queryPaths, true);
  validatePolicyFields(options.resultFields, false);
}

function validatePolicyFields(value: "*" | readonly string[], paths: boolean): void {
  if (value === "*") return;
  if (!Array.isArray(value) || value.length > 256)
    throw new TypeError("Read-policy field declaration must be '*' or a bounded array");
  const seen = new Set<string>();
  for (const field of value) {
    if (typeof field !== "string" || seen.has(field)) throw new TypeError("Read-policy fields must be unique strings");
    if (paths) {
      if (field.includes("\0")) throw new TypeError("Read-policy query paths cannot contain NUL");
      decodeQuerySpec({ version: 1, where: { op: "exists", path: field, value: true } });
    } else if (
      !field ||
      field.includes("\0") ||
      field.includes(".") ||
      field.startsWith("$") ||
      field === "__proto__" ||
      field === "prototype" ||
      field === "constructor"
    ) {
      throw new TypeError(`Unsafe read-policy result field: ${JSON.stringify(field)}`);
    }
    seen.add(field);
  }
}
