import type { Document, QueryLimits, Value } from "./types.js";
import { QueryValidationError } from "./types.js";

const forbiddenKeys = new Set(["__proto__", "prototype", "constructor"]);
const encoder = new TextEncoder();
const DOCUMENT_ID_PATTERN = /^[0-9a-f]{32}$/;
const ZERO_DOCUMENT_ID = "00000000000000000000000000000000";

// newDocumentID creates the portable 128-bit lowercase-hex identity used by
// both local and remote inserts.
export function newDocumentID(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function isDocumentID(value: unknown): value is string {
  return typeof value === "string" && DOCUMENT_ID_PATTERN.test(value) && value !== ZERO_DOCUMENT_ID;
}

export function assertDocumentID(value: unknown, label = "Document _id"): asserts value is string {
  if (!isDocumentID(value)) throw new QueryValidationError(`${label} must be a non-zero 32-character lowercase hexadecimal ID`);
}

export function assertSafeKey(key: string, label = "field"): void {
  assertWellFormedString(key, label);
  if (!key || key.includes("\0") || key.includes(".") || key.startsWith("$") || forbiddenKeys.has(key)) {
    throw new QueryValidationError(`Unsafe ${label}: ${JSON.stringify(key)}`);
  }
}

export function assertPath(path: string): void {
  assertWellFormedString(path, "field path");
  if (encoder.encode(path).byteLength > 512 || path.includes("\0")) throw new QueryValidationError("Field path is too long");
  const parts = path.split(".");
  if (parts.some((part) => !part || part.startsWith("$") || forbiddenKeys.has(part))) {
    throw new QueryValidationError(`Invalid field path: ${JSON.stringify(path)}`);
  }
}

export function cloneValue(value: Value, depth = 0): Value {
  if (depth > 64) throw new QueryValidationError("Document nesting is too deep");
  if (value === null || typeof value === "boolean") return value;
  if (typeof value === "string") { assertWellFormedString(value, "string"); return value; }
  if (typeof value === "bigint") {
    if (value < -(1n << 63n) || value > (1n << 63n) - 1n) throw new QueryValidationError("bigint is outside Int64 range");
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new QueryValidationError("Numbers must be finite");
    return value;
  }
  if (value instanceof Date) {
    if (!Number.isFinite(value.getTime())) throw new QueryValidationError("Invalid Date value");
    return new Date(value.getTime());
  }
  if (value instanceof Uint8Array) return value.slice();
  if (Array.isArray(value)) return value.map((item) => cloneValue(item, depth + 1));
  if (typeof value === "object") {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new QueryValidationError("Objects must use a plain object prototype");
    }
    const source = value as { readonly [key: string]: Value | undefined };
    const out: Record<string, Value> = Object.create(null) as Record<string, Value>;
    for (const key of Object.keys(value)) {
      assertSafeKey(key);
      const item = source[key];
      if (item === undefined) throw new QueryValidationError(`Undefined value at ${key}`);
      out[key] = cloneValue(item, depth + 1);
    }
    return out;
  }
  throw new QueryValidationError(`Unsupported value type: ${typeof value}`);
}

export function assertWellFormedString(value: string, label = "string"): void {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code < 0xd800 || code > 0xdfff) continue;
    if (code <= 0xdbff && index + 1 < value.length) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) { index += 1; continue; }
    }
    throw new QueryValidationError(`Invalid UTF-16 ${label}`);
  }
}

export function cloneDocument<T extends Document>(document: T): T {
  const cloned = cloneValue(document) as T;
  if (typeof cloned._id !== "string" || cloned._id.length === 0) {
    throw new QueryValidationError("Document _id must be a non-empty string");
  }
  return cloned;
}

export function valueByteLength(value: Value): number {
  if (value === null) return 1;
  if (typeof value === "boolean") return 1;
  if (typeof value === "number") return 8;
  if (typeof value === "bigint") return 8;
  if (typeof value === "string") return encoder.encode(value).byteLength;
  if (value instanceof Date) return 8;
  if (value instanceof Uint8Array) return value.byteLength;
  if (Array.isArray(value)) return value.reduce((sum, item) => sum + valueByteLength(item), 0);
  return Object.entries(value).reduce((sum, [key, item]) => sum + encoder.encode(key).byteLength + valueByteLength(item), 0);
}

export function assertQueryValue(value: Value, limits: QueryLimits, depth = 0): void {
  if (depth > limits.maxDepth) throw new QueryValidationError("Query value nesting is too deep");
  if (Array.isArray(value)) {
    if (value.length > limits.maxArrayItems) throw new QueryValidationError("Query array has too many items");
    for (const item of value) assertQueryValue(item, limits, depth + 1);
  } else if (value !== null && typeof value === "object" && !(value instanceof Date) && !(value instanceof Uint8Array)) {
    const entries = Object.entries(value);
    if (entries.length > limits.maxArrayItems) throw new QueryValidationError("Query object has too many fields");
    for (const [key, item] of entries) {
      assertSafeKey(key);
      assertQueryValue(item, limits, depth + 1);
    }
  }
  if (valueByteLength(value) > limits.maxStringBytes) throw new QueryValidationError("Query value is too large");
}

export function getPath(document: Document, path: string): { found: boolean; value?: Value } {
  let current: Value = document;
  for (const part of path.split(".")) {
    if (current === null || Array.isArray(current) || current instanceof Date || current instanceof Uint8Array || typeof current !== "object") {
      return { found: false };
    }
    if (!Object.prototype.hasOwnProperty.call(current, part)) return { found: false };
    const object = current as { readonly [key: string]: Value | undefined };
    const next: Value | undefined = object[part];
    if (next === undefined) return { found: false };
    current = next;
  }
  return { found: true, value: current };
}

export function valueEquals(left: Value, right: Value): boolean {
  if ((typeof left === "number" || typeof left === "bigint") && (typeof right === "number" || typeof right === "bigint")) {
    return compareNumeric(left, right) === 0;
  }
  if (left instanceof Date && right instanceof Date) return left.getTime() === right.getTime();
  if (left instanceof Uint8Array && right instanceof Uint8Array) {
    return left.length === right.length && left.every((byte, i) => byte === right[i]);
  }
  if (Array.isArray(left) && Array.isArray(right)) {
    return left.length === right.length && left.every((item, i) => valueEquals(item, right[i] as Value));
  }
  if (left && right && typeof left === "object" && typeof right === "object" && !Array.isArray(left) && !Array.isArray(right)) {
    if (left instanceof Date || right instanceof Date || left instanceof Uint8Array || right instanceof Uint8Array) return false;
    const leftObject = left as { readonly [key: string]: Value };
    const rightObject = right as { readonly [key: string]: Value };
    const lk = Object.keys(leftObject).sort();
    const rk = Object.keys(rightObject).sort();
    return lk.length === rk.length && lk.every((key, i) => key === rk[i] && valueEquals(leftObject[key] as Value, rightObject[key] as Value));
  }
  return left === right;
}

export function compareValues(left: Value, right: Value): number | undefined {
  const leftRank = scalarRank(left);
  const rightRank = scalarRank(right);
  if (leftRank === undefined || rightRank === undefined) return undefined;
  if ((typeof left === "number" || typeof left === "bigint") && (typeof right === "number" || typeof right === "bigint")) {
    return compareNumeric(left, right);
  }
  if (leftRank !== rightRank) return leftRank < rightRank ? -1 : 1;
  if (left === null && right === null) return 0;
  if (typeof left === "string" && typeof right === "string") return compareUTF8(left, right);
  if (typeof left === "boolean" && typeof right === "boolean") return left === right ? 0 : left ? 1 : -1;
  if (left instanceof Date && right instanceof Date) return left.getTime() === right.getTime() ? 0 : left < right ? -1 : 1;
  if (left instanceof Uint8Array && right instanceof Uint8Array) return compareBytes(left, right);
  return undefined;
}

// compareSortValues supplies a total order for query sorting. Arrays and
// objects deliberately retain insertion order among themselves, but their type
// ranks prevent mixed values from producing a non-transitive comparator.
export function compareSortValues(left: Value, right: Value): number {
  const leftRank = valueRank(left);
  const rightRank = valueRank(right);
  if (leftRank !== rightRank) return leftRank < rightRank ? -1 : 1;
  return compareValues(left, right) ?? 0;
}

function scalarRank(value: Value): number | undefined {
  if (value === null) return 0;
  if (typeof value === "boolean") return 1;
  if (typeof value === "number" || typeof value === "bigint") return 2;
  if (typeof value === "string") return 3;
  if (value instanceof Date) return 4;
  if (value instanceof Uint8Array) return 6;
  return undefined;
}

function valueRank(value: Value): number {
  const scalar = scalarRank(value);
  if (scalar !== undefined) return scalar;
  if (Array.isArray(value)) return 7;
  return 8;
}

function compareUTF8(left: string, right: string): number {
  return compareBytes(encoder.encode(left), encoder.encode(right));
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
  const length = Math.min(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    if (left[index] !== right[index]) return left[index]! < right[index]! ? -1 : 1;
  }
  return left.length === right.length ? 0 : left.length < right.length ? -1 : 1;
}

function compareNumeric(left: number | bigint, right: number | bigint): number {
  if (typeof left === "bigint" && typeof right === "bigint") return left === right ? 0 : left < right ? -1 : 1;
  if (typeof left === "number" && typeof right === "number") return left === right ? 0 : left < right ? -1 : 1;
  // Relational comparison between finite number and bigint is exact according
  // to ECMAScript's abstract relational comparison; strict equality is not.
  return left == right ? 0 : left < right ? -1 : 1;
}
