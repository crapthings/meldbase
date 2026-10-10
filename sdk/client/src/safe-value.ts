import type { Document, QueryLimits, QueryTypeName, Value } from "./types.js";
import { DEFAULT_QUERY_LIMITS, QueryValidationError } from "./types.js";

/** Lists object keys that could interfere with JavaScript prototypes. */
const forbiddenKeys = new Set(["__proto__", "prototype", "constructor"]);
/** Encodes strings as UTF-8 for byte limits and comparisons. */
const encoder = new TextEncoder();
/** Matches the lowercase hexadecimal shape of a document ID. */
const DOCUMENT_ID_PATTERN = /^[0-9a-f]{32}$/;
/** Identifies the all-zero document ID that validation rejects. */
const ZERO_DOCUMENT_ID = "00000000000000000000000000000000";
// The server applies the value limits below to every typed wire value, not
// just query operands. Values may be nested more deeply than a query AST, but
// otherwise the transport cardinality and byte budgets are identical.
/** Sets size and nesting limits for values sent over the transport. */
const DEFAULT_TRANSPORT_VALUE_LIMITS: QueryLimits = Object.freeze({
  ...DEFAULT_QUERY_LIMITS,
  maxDepth: 64,
});
/** Defines the canonical order of query type names. */
const QUERY_TYPE_ORDER = [
  "null",
  "boolean",
  "int64",
  "float64",
  "string",
  "date",
  "id",
  "binary",
  "array",
  "object",
] as const satisfies readonly QueryTypeName[];
/** Provides the accepted names for query type validation. */
const QUERY_TYPE_NAMES = new Set<string>(QUERY_TYPE_ORDER);

// DocumentID represents an ID-valued generic field. It is intentionally
// separate from string: a canonical-looking string is still a string unless a
// caller explicitly wraps it, while decoded `t: "id"` values preserve their
// wire kind. A document's persisted `_id` stays a string for ergonomic APIs.
/** Preserves the ID value type for fields other than the document ID. */
export class DocumentID {
  readonly value: string;

  constructor(value: string) {
    assertDocumentID(value, "DocumentID");
    this.value = value;
    Object.freeze(this);
  }

  toString(): string {
    return this.value;
  }
}

/** Wraps a validated string as an ID-valued field. */
export function documentID(value: string): DocumentID {
  return new DocumentID(value);
}

/** Checks whether a value is an explicit document ID wrapper. */
export function isDocumentIDValue(value: unknown): value is DocumentID {
  return value instanceof DocumentID;
}

/** Generates a random 128-bit document ID as lowercase hexadecimal text. */
export function newDocumentID(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

/** Checks for a nonzero lowercase hexadecimal document ID. */
export function isDocumentID(value: unknown): value is string {
  return typeof value === "string" && DOCUMENT_ID_PATTERN.test(value) && value !== ZERO_DOCUMENT_ID;
}

/** Rejects values that are not valid document ID strings. */
export function assertDocumentID(value: unknown, label = "Document _id"): asserts value is string {
  if (!isDocumentID(value))
    throw new QueryValidationError(`${label} must be a non-zero 32-character lowercase hexadecimal ID`);
}

/** Rejects malformed or reserved document field names. */
export function assertSafeKey(key: string, label = "field"): void {
  assertWellFormedString(key, label);
  if (!key || key.includes("\0") || key.includes(".") || key.startsWith("$") || forbiddenKeys.has(key)) {
    throw new QueryValidationError(`Unsafe ${label}: ${JSON.stringify(key)}`);
  }
}

/** Checks the byte limit and allowed segments of a dotted field path. */
export function assertPath(path: string): void {
  assertWellFormedString(path, "field path");
  if (encoder.encode(path).byteLength > 512 || path.includes("\0"))
    throw new QueryValidationError("Field path is too long");
  const parts = path.split(".");
  if (parts.some((part) => !part || part.startsWith("$") || forbiddenKeys.has(part))) {
    throw new QueryValidationError(`Invalid field path: ${JSON.stringify(path)}`);
  }
}

/** Copies supported values while rejecting invalid types, fields, and nesting. */
export function cloneValue(value: Value, depth = 0): Value {
  if (depth > 64) throw new QueryValidationError("Document nesting is too deep");
  if (value === null || typeof value === "boolean") return value;
  if (isDocumentIDValue(value)) return value;
  if (typeof value === "string") {
    assertWellFormedString(value, "string");
    return value;
  }
  if (typeof value === "bigint") {
    if (value < -(1n << 63n) || value > (1n << 63n) - 1n)
      throw new QueryValidationError("bigint is outside Int64 range");
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

// cloneTransportValue is the public-transport boundary for user-supplied
// values. Cloning first means a hostile or mutable input cannot change between
// validation and encoding, while the common limits prevent JSON.stringify from
// turning an unsupported value into a different wire meaning.
/** Copies an input value and checks that it is safe to send over the transport. */
export function cloneTransportValue(value: unknown): Value {
  const cloned = cloneValue(value as Value);
  assertTransportValue(cloned);
  return cloned;
}

/** Rejects strings containing unpaired UTF-16 surrogates. */
export function assertWellFormedString(value: string, label = "string"): void {
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
    throw new QueryValidationError(`Invalid UTF-16 ${label}`);
  }
}

/** Copies a document and checks that its ID is a nonempty string. */
export function cloneDocument<T extends Document>(document: T): T {
  const cloned = cloneValue(document) as T;
  if (typeof cloned._id !== "string" || cloned._id.length === 0) {
    throw new QueryValidationError("Document _id must be a non-empty string");
  }
  return cloned;
}

/** Counts value bytes and object key bytes for resource limits. */
export function valueByteLength(value: Value): number {
  if (value === null) return 1;
  if (typeof value === "boolean") return 1;
  if (typeof value === "number") return 8;
  if (typeof value === "bigint") return 8;
  if (isDocumentIDValue(value)) return 16;
  if (typeof value === "string") return encoder.encode(value).byteLength;
  if (value instanceof Date) return 8;
  if (value instanceof Uint8Array) return value.byteLength;
  if (Array.isArray(value)) return value.reduce((sum, item) => sum + valueByteLength(item), 0);
  return Object.entries(value).reduce(
    (sum, [key, item]) => sum + encoder.encode(key).byteLength + valueByteLength(item),
    0,
  );
}

/** Checks a query value against the supplied size and nesting limits. */
export function assertQueryValue(value: Value, limits: QueryLimits, depth = 0): void {
  assertBoundedValue(value, limits, depth, "Query value");
}

/** Checks a value against the shared transport size and nesting limits. */
export function assertTransportValue(value: Value): void {
  assertBoundedValue(value, DEFAULT_TRANSPORT_VALUE_LIMITS, 0, "Value");
}

/** Checks nested values against depth, item count, field name, and byte limits. */
function assertBoundedValue(value: Value, limits: QueryLimits, depth: number, label: string): void {
  if (depth > limits.maxDepth) throw new QueryValidationError(`${label} nesting is too deep`);
  if (Array.isArray(value)) {
    if (value.length > limits.maxArrayItems) throw new QueryValidationError(`${label} array has too many items`);
    for (const item of value) assertBoundedValue(item, limits, depth + 1, label);
  } else if (
    value !== null &&
    typeof value === "object" &&
    !isDocumentIDValue(value) &&
    !(value instanceof Date) &&
    !(value instanceof Uint8Array)
  ) {
    const entries = Object.entries(value);
    if (entries.length > limits.maxArrayItems) throw new QueryValidationError(`${label} object has too many fields`);
    for (const [key, item] of entries) {
      assertSafeKey(key);
      assertBoundedValue(item, limits, depth + 1, label);
    }
  }
  if (valueByteLength(value) > limits.maxStringBytes) throw new QueryValidationError(`${label} is too large`);
}

/** Reads a dotted path through own object fields and reports missing values. */
export function getPath(document: Document, path: string): { found: boolean; value?: Value } {
  let current: Value = document;
  for (const part of path.split(".")) {
    if (
      current === null ||
      Array.isArray(current) ||
      isDocumentIDValue(current) ||
      current instanceof Date ||
      current instanceof Uint8Array ||
      typeof current !== "object"
    ) {
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

/** Validates query type names and returns a deduplicated list in canonical order. */
export function normalizeQueryTypes(raw: unknown, maxItems = DEFAULT_QUERY_LIMITS.maxArrayItems): QueryTypeName[] {
  const values = typeof raw === "string" ? [raw] : raw;
  if (!Array.isArray(values) || values.length === 0 || values.length > maxItems)
    throw new QueryValidationError("$type expects a non-empty bounded type list");
  const selected = new Set<QueryTypeName>();
  for (const value of values) {
    if (typeof value !== "string" || !QUERY_TYPE_NAMES.has(value))
      throw new QueryValidationError(`Unknown query type: ${String(value)}`);
    selected.add(value as QueryTypeName);
  }
  return QUERY_TYPE_ORDER.filter((value) => selected.has(value));
}

/** Finds a value’s query type while recognizing valid document ID strings at the ID path. */
export function queryTypeNameOf(value: Value, path: string): QueryTypeName {
  if (value === null) return "null";
  if (typeof value === "boolean") return "boolean";
  if (typeof value === "bigint") return "int64";
  if (typeof value === "number") return "float64";
  if (typeof value === "string") return path === "_id" && isDocumentID(value) ? "id" : "string";
  if (value instanceof Date) return "date";
  if (isDocumentIDValue(value)) return "id";
  if (value instanceof Uint8Array) return "binary";
  if (Array.isArray(value)) return "array";
  return "object";
}

/** Compares supported values recursively with exact numeric comparison across number and bigint. */
export function valueEquals(left: Value, right: Value): boolean {
  if (isDocumentIDValue(left) || isDocumentIDValue(right)) {
    return isDocumentIDValue(left) && isDocumentIDValue(right) && left.value === right.value;
  }
  if (
    (typeof left === "number" || typeof left === "bigint") &&
    (typeof right === "number" || typeof right === "bigint")
  ) {
    return compareNumeric(left, right) === 0;
  }
  if (left instanceof Date && right instanceof Date) return left.getTime() === right.getTime();
  if (left instanceof Uint8Array && right instanceof Uint8Array) {
    return left.length === right.length && left.every((byte, i) => byte === right[i]);
  }
  if (Array.isArray(left) && Array.isArray(right)) {
    return left.length === right.length && left.every((item, i) => valueEquals(item, right[i] as Value));
  }
  if (
    left &&
    right &&
    typeof left === "object" &&
    typeof right === "object" &&
    !Array.isArray(left) &&
    !Array.isArray(right)
  ) {
    if (left instanceof Date || right instanceof Date || left instanceof Uint8Array || right instanceof Uint8Array)
      return false;
    const leftObject = left as { readonly [key: string]: Value };
    const rightObject = right as { readonly [key: string]: Value };
    const lk = Object.keys(leftObject).sort();
    const rk = Object.keys(rightObject).sort();
    return (
      lk.length === rk.length &&
      lk.every((key, i) => key === rk[i] && valueEquals(leftObject[key] as Value, rightObject[key] as Value))
    );
  }
  return left === right;
}

/** Compares scalar values and returns no ordering for arrays or objects. */
export function compareValues(left: Value, right: Value): number | undefined {
  const leftRank = scalarRank(left);
  const rightRank = scalarRank(right);
  if (leftRank === undefined || rightRank === undefined) return undefined;
  if (
    (typeof left === "number" || typeof left === "bigint") &&
    (typeof right === "number" || typeof right === "bigint")
  ) {
    return compareNumeric(left, right);
  }
  if (leftRank !== rightRank) return leftRank < rightRank ? -1 : 1;
  if (left === null && right === null) return 0;
  if (typeof left === "string" && typeof right === "string") return compareUTF8(left, right);
  if (typeof left === "boolean" && typeof right === "boolean") return left === right ? 0 : left ? 1 : -1;
  if (left instanceof Date && right instanceof Date)
    return left.getTime() === right.getTime() ? 0 : left < right ? -1 : 1;
  if (isDocumentIDValue(left) && isDocumentIDValue(right)) return compareUTF8(left.value, right.value);
  if (left instanceof Uint8Array && right instanceof Uint8Array) return compareBytes(left, right);
  return undefined;
}

// compareSortValues supplies a total order for query sorting. Arrays and
// objects deliberately retain insertion order among themselves, but their type
// ranks prevent mixed values from producing a non-transitive comparator.
/** Orders query values by type and compares scalar values within each type. */
export function compareSortValues(left: Value, right: Value): number {
  const leftRank = valueRank(left);
  const rightRank = valueRank(right);
  if (leftRank !== rightRank) return leftRank < rightRank ? -1 : 1;
  return compareValues(left, right) ?? 0;
}

/** Assigns scalar types their comparison order. */
function scalarRank(value: Value): number | undefined {
  if (value === null) return 0;
  if (typeof value === "boolean") return 1;
  if (typeof value === "number" || typeof value === "bigint") return 2;
  if (typeof value === "string") return 3;
  if (value instanceof Date) return 4;
  if (isDocumentIDValue(value)) return 5;
  if (value instanceof Uint8Array) return 6;
  return undefined;
}

/** Extends scalar type order with array and object ranks. */
function valueRank(value: Value): number {
  const scalar = scalarRank(value);
  if (scalar !== undefined) return scalar;
  if (Array.isArray(value)) return 7;
  return 8;
}

/** Compares strings by their UTF-8 bytes. */
function compareUTF8(left: string, right: string): number {
  return compareBytes(encoder.encode(left), encoder.encode(right));
}

/** Compares byte arrays in lexicographic order. */
function compareBytes(left: Uint8Array, right: Uint8Array): number {
  const length = Math.min(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    if (left[index] !== right[index]) return left[index]! < right[index]! ? -1 : 1;
  }
  return left.length === right.length ? 0 : left.length < right.length ? -1 : 1;
}

/** Compares number and bigint values without converting their numeric representation. */
function compareNumeric(left: number | bigint, right: number | bigint): number {
  if (typeof left === "bigint" && typeof right === "bigint") return left === right ? 0 : left < right ? -1 : 1;
  if (typeof left === "number" && typeof right === "number") return left === right ? 0 : left < right ? -1 : 1;
  // Relational comparison between finite number and bigint is exact according
  // to ECMAScript's abstract relational comparison; strict equality is not.
  return left == right ? 0 : left < right ? -1 : 1;
}
