import { DEFAULT_QUERY_LIMITS, decodeQuerySpec } from "@meldbase/client";

import type { MethodDefinition, MethodHandler, PublicationDefinition, PublicationHandler, PublicationOptions, TransactionalMethodHandler } from "./types.js";

export function rpc(handler: MethodHandler): MethodDefinition {
  if (typeof handler !== "function") throw new TypeError("RPC handler must be a function");
  return { mode: "rpc", handler };
}

export function transactional(handler: TransactionalMethodHandler): MethodDefinition {
  if (typeof handler !== "function") throw new TypeError("Transactional RPC handler must be a function");
  return { mode: "transactional", handler };
}

export function publish(options: PublicationOptions, handler: PublicationHandler): PublicationDefinition {
  if (typeof handler !== "function") throw new TypeError("Publication handler must be a function");
  validatePublicationOptions(options);
  return { ...options, handler };
}

export function validatePublicationOptions(options: PublicationOptions): void {
  const encodedVersion = options && typeof options.version === "string" ? new TextEncoder().encode(options.version) : new Uint8Array();
  if (!options || typeof options.version !== "string" || options.version.length === 0 || encodedVersion.byteLength > 128 || new TextDecoder().decode(encodedVersion) !== options.version) {
    throw new TypeError("Publication version must contain between 1 and 128 UTF-8 bytes");
  }
  if (!Number.isSafeInteger(options.maxResults) || options.maxResults <= 0 || options.maxResults > DEFAULT_QUERY_LIMITS.maxLimit) {
    throw new TypeError("Publication maxResults is outside query limits");
  }
  validatePolicyFields(options.queryPaths, true);
  validatePolicyFields(options.resultFields, false);
}

function validatePolicyFields(value: "*" | readonly string[], paths: boolean): void {
  if (value === "*") return;
  if (!Array.isArray(value) || value.length > 256) throw new TypeError("Publication field policy must be '*' or a bounded array");
  const seen = new Set<string>();
  for (const field of value) {
    if (typeof field !== "string" || seen.has(field)) throw new TypeError("Publication fields must be unique strings");
    if (paths) {
      if (field.includes("\0")) throw new TypeError("Publication query paths cannot contain NUL");
      decodeQuerySpec({ version: 1, where: { op: "exists", path: field, value: true } });
    } else if (!field || field.includes("\0") || field.includes(".") || field.startsWith("$") || field === "__proto__" || field === "prototype" || field === "constructor") {
      throw new TypeError(`Unsafe publication result field: ${JSON.stringify(field)}`);
    }
    seen.add(field);
  }
}
