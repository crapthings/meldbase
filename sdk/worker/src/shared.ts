export const METHOD_PATTERN = /^[A-Za-z][A-Za-z0-9_.-]{0,127}$/;
export const WORKER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;
export const COLLECTION_PATTERN = /^[A-Za-z][A-Za-z0-9_-]{0,127}$/;
export const ID_PATTERN = /^[0-9a-f]{32}$/;
export const ERROR_PATTERN = /^[a-z][a-z0-9_]{0,63}$/;
// Keep the SDK's inbound worker frame ceiling aligned with the largest frame
// the Go worker hub will accept. A worker must never parse an unbounded string
// supplied by its control-plane peer.
export const MAX_WORKER_FRAME_BYTES = 16 << 20;

const MAX_WORKER_JSON_DEPTH = 128;
const ZERO_DOCUMENT_ID = "00000000000000000000000000000000";
const encoder = new TextEncoder();

export interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

export function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((ok, fail) => {
    resolve = ok;
    reject = fail;
  });
  return { promise, resolve, reject };
}

export function parseWorkerURL(raw: string): string {
  const url = new URL(raw);
  if (!url.host || url.username || url.password || url.search || url.hash) {
    throw new TypeError(
      "Worker URL must be an absolute ws(s) URL or a meldbase:// authority without credentials, query parameters, or a fragment",
    );
  }
  if (url.protocol === "meldbase:") {
    if (url.pathname !== "" && url.pathname !== "/") {
      throw new TypeError("meldbase:// worker URLs cannot include a path");
    }
    return new URL(`/v1/workers`, `wss://${url.host}`).toString();
  }
  if (url.protocol !== "ws:" && url.protocol !== "wss:") {
    throw new TypeError("Worker URL must use ws:, wss:, or meldbase:");
  }
  return url.toString();
}

export function isCanonicalDocumentID(value: unknown): value is string {
  return typeof value === "string" && ID_PATTERN.test(value) && value !== ZERO_DOCUMENT_ID;
}

/** Return a strict UTF-8 byte length without silently replacing lone surrogates. */
export function utf8ByteLength(value: unknown, label = "value"): number {
  if (typeof value !== "string") throw new TypeError(`${label} must be a string`);
  assertWellFormedString(value, label);
  return encoder.encode(value).byteLength;
}

export function record(value: unknown): value is Record<string, any> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function parseJSONRecord(raw: string): Record<string, unknown> {
  let byteLength: number;
  try {
    byteLength = utf8ByteLength(raw, "worker JSON");
  } catch {
    throw new Error("Malformed worker JSON");
  }
  if (byteLength === 0) throw new Error("Malformed worker JSON");
  if (byteLength > MAX_WORKER_FRAME_BYTES) throw new Error("Worker frame exceeds the 16 MiB safety limit");

  let value: unknown;
  try {
    validateStrictJSON(raw);
    value = JSON.parse(raw);
  } catch {
    throw new Error("Malformed worker JSON");
  }
  if (!record(value)) throw new Error("Worker frame must be an object");
  return value;
}

export function exactKeys(record_: Record<string, unknown>, expected: readonly string[]): void {
  const actual = Object.keys(record_).sort();
  const wanted = [...expected].sort();
  if (actual.length !== wanted.length || actual.some((key, index) => key !== wanted[index]))
    throw new Error("Worker frame contains unknown or missing fields");
}

export function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error("Unknown Meldbase worker failure");
}

export function abortableDelay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(done, milliseconds);
    signal.addEventListener("abort", done, { once: true });
    function done(): void {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    }
  });
}

function assertWellFormedString(value: string, label: string): void {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code < 0xd800 || code > 0xdfff) continue;
    if (code <= 0xdbff && index + 1 < value.length) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        index += 1;
        continue;
      }
    }
    throw new TypeError(`Invalid UTF-16 ${label}`);
  }
}

// JSON.parse intentionally accepts duplicate keys and silently keeps the last
// value. The Go hub rejects those frames, so validate the complete JSON token
// stream first to keep the worker-side control-plane boundary symmetric.
function validateStrictJSON(raw: string): void {
  let index = 0;

  const malformed = (): never => {
    throw new Error("Malformed JSON");
  };
  const skipWhitespace = (): void => {
    while (true) {
      const code = raw.charCodeAt(index);
      if (code !== 0x20 && code !== 0x09 && code !== 0x0a && code !== 0x0d) return;
      index += 1;
    }
  };
  const digit = (value: string): boolean => value >= "0" && value <= "9";
  const parseString = (): string => {
    if (raw.charAt(index) !== '"') malformed();
    index += 1;
    let value = "";
    while (index < raw.length) {
      const character = raw.charAt(index);
      index += 1;
      if (character === '"') {
        assertWellFormedString(value, "worker JSON string");
        return value;
      }
      if (character === "\\") {
        const escape = raw.charAt(index);
        index += 1;
        switch (escape) {
          case '"':
            value += '"';
            break;
          case "\\":
            value += "\\";
            break;
          case "/":
            value += "/";
            break;
          case "b":
            value += "\b";
            break;
          case "f":
            value += "\f";
            break;
          case "n":
            value += "\n";
            break;
          case "r":
            value += "\r";
            break;
          case "t":
            value += "\t";
            break;
          case "u": {
            const hex = raw.slice(index, index + 4);
            if (!/^[0-9a-fA-F]{4}$/.test(hex)) malformed();
            value += String.fromCharCode(Number.parseInt(hex, 16));
            index += 4;
            break;
          }
          default:
            malformed();
        }
        continue;
      }
      if (character.charCodeAt(0) < 0x20) malformed();
      value += character;
    }
    return malformed();
  };
  const parseNumber = (): void => {
    if (raw.charAt(index) === "-") index += 1;
    if (raw.charAt(index) === "0") {
      index += 1;
    } else {
      if (raw.charAt(index) < "1" || raw.charAt(index) > "9") malformed();
      do {
        index += 1;
      } while (digit(raw.charAt(index)));
    }
    if (raw.charAt(index) === ".") {
      index += 1;
      if (!digit(raw.charAt(index))) malformed();
      do {
        index += 1;
      } while (digit(raw.charAt(index)));
    }
    if (raw.charAt(index) === "e" || raw.charAt(index) === "E") {
      index += 1;
      if (raw.charAt(index) === "+" || raw.charAt(index) === "-") index += 1;
      if (!digit(raw.charAt(index))) malformed();
      do {
        index += 1;
      } while (digit(raw.charAt(index)));
    }
  };
  const parseValue = (depth: number): void => {
    if (depth > MAX_WORKER_JSON_DEPTH) malformed();
    skipWhitespace();
    switch (raw.charAt(index)) {
      case "{": {
        index += 1;
        skipWhitespace();
        const keys = new Set<string>();
        if (raw.charAt(index) === "}") {
          index += 1;
          return;
        }
        while (true) {
          skipWhitespace();
          const key = parseString();
          if (keys.has(key)) malformed();
          keys.add(key);
          skipWhitespace();
          if (raw.charAt(index) !== ":") malformed();
          index += 1;
          parseValue(depth + 1);
          skipWhitespace();
          const separator = raw.charAt(index);
          if (separator === "}") {
            index += 1;
            return;
          }
          if (separator !== ",") malformed();
          index += 1;
        }
      }
      case "[": {
        index += 1;
        skipWhitespace();
        if (raw.charAt(index) === "]") {
          index += 1;
          return;
        }
        while (true) {
          parseValue(depth + 1);
          skipWhitespace();
          const separator = raw.charAt(index);
          if (separator === "]") {
            index += 1;
            return;
          }
          if (separator !== ",") malformed();
          index += 1;
        }
      }
      case '"':
        parseString();
        return;
      case "t":
        if (!raw.startsWith("true", index)) malformed();
        index += 4;
        return;
      case "f":
        if (!raw.startsWith("false", index)) malformed();
        index += 5;
        return;
      case "n":
        if (!raw.startsWith("null", index)) malformed();
        index += 4;
        return;
      default:
        parseNumber();
    }
  };

  skipWhitespace();
  parseValue(0);
  skipWhitespace();
  if (index !== raw.length) malformed();
}
