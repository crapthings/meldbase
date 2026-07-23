import { decodeValue } from "../wire.js";
import type { Value } from "../types.js";
import { MeldbaseInternalError, type MeldbaseErrorData } from "./errors.js";

export function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

export function abortReason(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) return signal.reason;
  const error = new Error("RPC call aborted");
  error.name = "AbortError";
  return error;
}

export function throwIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) throw abortReason(signal);
}

export function validRPCErrorCode(value: unknown): value is string {
  return typeof value === "string" && /^[a-z][a-z0-9_]{0,63}$/.test(value);
}

export function validBusinessErrorCode(value: unknown): value is string {
  return typeof value === "string" && /^[a-z][a-z0-9_]{0,31}(?:\.[a-z][a-z0-9_]{0,31})+$/.test(value);
}

export type WireError =
  | { readonly kind: "internal"; readonly code: string }
  | { readonly kind: "business"; readonly code: string; readonly data?: MeldbaseErrorData };

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

export function validRPCIdempotencyKey(value: string): boolean {
  return /^[A-Za-z0-9_-]{22,128}$/.test(value);
}

export function boundedCount(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

export function assertCollectionName(value: string): void {
  if (!/^[A-Za-z][A-Za-z0-9_-]{0,127}$/.test(value)) throw new Error("Invalid collection name");
}

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

export async function throwRemoteError(response: Response, maxBytes: number, operation: string): Promise<never> {
  const body = await boundedJSON(response, maxBytes);
  if (!record(body) || !exactKeys(body, ["error"])) {
    throw new Error(`Malformed ${operation} error response`);
  }
  const error = decodeWireError(body.error);
  if (error.kind !== "internal") throw new Error(`Malformed ${operation} error response`);
  throw new MeldbaseInternalError(error.code, response.status, operation);
}

export function positiveLimit(value: number | undefined, fallback: number, name: string): number {
  const limit = value ?? fallback;
  if (!Number.isSafeInteger(limit) || limit <= 0) throw new Error(`${name} must be a positive safe integer`);
  return limit;
}

export function exactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  return actual.length === wanted.length && actual.every((key, index) => key === wanted[index]);
}

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
