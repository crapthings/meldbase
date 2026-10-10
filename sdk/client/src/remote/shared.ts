import { decodeValue } from "../wire.js";
import type { Value } from "../types.js";
import { MeldbaseInternalError, type MeldbaseErrorData } from "./errors.js";

/** Checks whether a value is a non-null object other than an array. */
export function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** Preserves Error instances and converts other failures into an Error. */
export function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

/** Returns the signal’s Error reason or a generic RPC cancellation error. */
export function abortReason(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) return signal.reason;
  const error = new Error("RPC call aborted");
  error.name = "AbortError";
  return error;
}

/** Throws the cancellation reason when the supplied signal is already aborted. */
export function throwIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) throw abortReason(signal);
}

/** Checks the allowed shape of an internal RPC error code. */
export function validRPCErrorCode(value: unknown): value is string {
  return typeof value === "string" && /^[a-z][a-z0-9_]{0,63}$/.test(value);
}

/** Checks the dotted name format used by application error codes. */
export function validBusinessErrorCode(value: unknown): value is string {
  return typeof value === "string" && /^[a-z][a-z0-9_]{0,31}(?:\.[a-z][a-z0-9_]{0,31})+$/.test(value);
}

/** Represents a validated internal or application error from the server. */
export type WireError =
  | { readonly kind: "internal"; readonly code: string }
  | { readonly kind: "business"; readonly code: string; readonly data?: MeldbaseErrorData };

/** Validates a server error and decodes optional application error details. */
export function decodeWireError(value: unknown): WireError {
  if (!record(value) || typeof value.kind !== "string" || typeof value.code !== "string")
    throw new Error("Malformed Meldbase error");
  if (value.kind === "internal") {
    if (!exactKeys(value, ["kind", "code"]) || !validRPCErrorCode(value.code))
      throw new Error("Malformed internal Meldbase error");
    return { kind: "internal", code: value.code };
  }
  if (
    value.kind !== "business" ||
    !validBusinessErrorCode(value.code) ||
    !exactKeys(value, value.data === undefined ? ["kind", "code"] : ["kind", "code", "data"])
  ) {
    throw new Error("Malformed business Meldbase error");
  }
  if (value.data === undefined) return { kind: "business", code: value.code };
  const data = decodeValue(value.data);
  if (!record(data)) throw new Error("Business Meldbase error data must be an object");
  return { kind: "business", code: value.code, data: data as Readonly<Record<string, Value>> };
}

/** Checks the allowed characters and length of an RPC idempotency key. */
export function validRPCIdempotencyKey(value: string): boolean {
  return /^[A-Za-z0-9_-]{22,128}$/.test(value);
}

/** Checks whether a value is a nonnegative safe integer. */
export function boundedCount(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

/** Rejects collection names with invalid characters or length. */
export function assertCollectionName(value: string): void {
  if (!/^[A-Za-z][A-Za-z0-9_-]{0,127}$/.test(value)) throw new Error("Invalid collection name");
}

/** Validates an HTTP base URL and removes its trailing slash. */
export function normalizeBaseURL(value: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error("baseUrl must be an http(s) URL");
  }
  if (
    (url.protocol !== "http:" && url.protocol !== "https:") ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  ) {
    throw new Error("baseUrl must be an http(s) URL without credentials, query, or fragment");
  }
  return url.toString().replace(/\/$/, "");
}

/** Validates a WebSocket origin without credentials, paths, queries, or fragments. */
export function normalizeRealtimeOrigin(value: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error("allowedRealtimeOrigins must contain ws(s) origins");
  }
  if (
    (url.protocol !== "ws:" && url.protocol !== "wss:") ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.pathname !== "/"
  ) {
    throw new Error("allowedRealtimeOrigins must contain ws(s) origins");
  }
  return url.origin;
}

/** Reads a bounded internal error response and throws a typed remote failure. */
export async function throwRemoteError(response: Response, maxBytes: number, operation: string): Promise<never> {
  const body = await boundedJSON(response, maxBytes);
  if (!record(body) || !exactKeys(body, ["error"])) {
    throw new Error(`Malformed ${operation} error response`);
  }
  const error = decodeWireError(body.error);
  if (error.kind !== "internal") throw new Error(`Malformed ${operation} error response`);
  throw new MeldbaseInternalError(error.code, response.status, operation);
}

/** Applies a default limit and checks that the result is a positive safe integer. */
export function positiveLimit(value: number | undefined, fallback: number, name: string): number {
  const limit = value ?? fallback;
  if (!Number.isSafeInteger(limit) || limit <= 0) throw new Error(`${name} must be a positive safe integer`);
  return limit;
}

/** Checks that an object contains exactly the expected field names. */
export function exactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  return actual.length === wanted.length && actual.every((key, index) => key === wanted[index]);
}

/** Reads a response within its byte limit and parses strictly decoded UTF-8 JSON. */
export async function boundedJSON(response: Response, maxBytes: number): Promise<unknown> {
  const declared = response.headers.get("content-length");
  if (declared !== null && (!/^[0-9]+$/.test(declared) || BigInt(declared) > BigInt(maxBytes))) {
    try {
      await response.body?.cancel("Response exceeds safety limit");
    } catch {
      /* best-effort transport cancellation */
    }
    throw new Error("Response exceeds safety limit");
  }

  const reader = response.body?.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  if (reader) {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value.byteLength > maxBytes - total) {
        try {
          await reader.cancel("Response exceeds safety limit");
        } catch {
          /* best-effort transport cancellation */
        }
        throw new Error("Response exceeds safety limit");
      }
      chunks.push(value);
      total += value.byteLength;
    }
  }
  const encoded = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    encoded.set(chunk, offset);
    offset += chunk.byteLength;
  }
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(encoded);
  } catch {
    throw new Error("Malformed JSON response");
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new Error("Malformed JSON response");
  }
}
