import type { Document, Value } from "./types.js";
import { QueryValidationError } from "./types.js";

const forbiddenKeys = new Set(["__proto__", "prototype", "constructor"]);
const encoder = new TextEncoder();

export function assertSafeKey(key: string, label = "field"): void {
  if (!key || key.includes("\0") || key.includes(".") || key.startsWith("$") || forbiddenKeys.has(key)) {
    throw new QueryValidationError(`Unsafe ${label}: ${JSON.stringify(key)}`);
  }
}

export function assertPath(path: string): void {
  if (path.length > 512) throw new QueryValidationError("Field path is too long");
  const parts = path.split(".");
  if (parts.some((part) => !part || part.startsWith("$") || forbiddenKeys.has(part))) {
    throw new QueryValidationError(`Invalid field path: ${JSON.stringify(path)}`);
  }
}

export function cloneValue(value: Value, depth = 0): Value {
  if (depth > 64) throw new QueryValidationError("Document nesting is too deep");
  if (value === null || typeof value === "boolean" || typeof value === "string") return value;
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
  if ((typeof left === "number" || typeof left === "bigint") && (typeof right === "number" || typeof right === "bigint")) {
    return compareNumeric(left, right);
  }
  if (typeof left === "string" && typeof right === "string") return left === right ? 0 : left < right ? -1 : 1;
  if (typeof left === "boolean" && typeof right === "boolean") return left === right ? 0 : left ? 1 : -1;
  if (left instanceof Date && right instanceof Date) return left.getTime() === right.getTime() ? 0 : left < right ? -1 : 1;
  return undefined;
}

function compareNumeric(left: number | bigint, right: number | bigint): number {
  if (typeof left === "bigint" && typeof right === "bigint") return left === right ? 0 : left < right ? -1 : 1;
  if (typeof left === "number" && typeof right === "number") return left === right ? 0 : left < right ? -1 : 1;
  // Relational comparison between finite number and bigint is exact according
  // to ECMAScript's abstract relational comparison; strict equality is not.
  return left == right ? 0 : left < right ? -1 : 1;
}
