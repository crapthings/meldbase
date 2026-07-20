import type { Document, InputDocument, QueryExpr, QueryLimits, QuerySpec, SortField, Value } from "./types.js";
import { DEFAULT_QUERY_LIMITS, QueryValidationError } from "./types.js";
import { assertPath, assertSafeKey, cloneDocument, valueByteLength } from "./safe-value.js";

export type WireValue =
  | { readonly t: "null" }
  | { readonly t: "bool"; readonly v: boolean }
  | { readonly t: "number"; readonly v: number }
  | { readonly t: "int64"; readonly v: string }
  | { readonly t: "string"; readonly v: string }
  | { readonly t: "id"; readonly v: string }
  | { readonly t: "date"; readonly v: string }
  | { readonly t: "binary"; readonly v: string }
  | { readonly t: "array"; readonly v: readonly WireValue[] }
  | { readonly t: "object"; readonly v: readonly (readonly [string, WireValue])[] };

export type WireQueryExpr =
  | { readonly op: "true" }
  | { readonly op: "and" | "or"; readonly args: readonly WireQueryExpr[] }
  | { readonly op: "not"; readonly arg: WireQueryExpr }
  | { readonly op: "compare"; readonly cmp: "eq" | "ne" | "gt" | "gte" | "lt" | "lte"; readonly path: string; readonly value: WireValue }
  | { readonly op: "in" | "nin"; readonly path: string; readonly values: readonly WireValue[] }
  | { readonly op: "exists"; readonly path: string; readonly value: boolean };

export interface WireQuerySpec {
  readonly version: 1;
  readonly where: WireQueryExpr;
  readonly sort?: readonly SortField[];
  readonly skip?: number;
  readonly limit?: number;
}

export function encodeValue(value: Value): WireValue {
  if (value === null) return { t: "null" };
  if (typeof value === "boolean") return { t: "bool", v: value };
  if (typeof value === "number") return { t: "number", v: value };
  if (typeof value === "bigint") return { t: "int64", v: value.toString() };
  if (typeof value === "string") return { t: "string", v: value };
  if (value instanceof Date) return { t: "date", v: value.toISOString() };
  if (value instanceof Uint8Array) return { t: "binary", v: bytesToBase64(value) };
  if (Array.isArray(value)) return { t: "array", v: value.map(encodeValue) };
  const object = value as { readonly [key: string]: Value };
  return { t: "object", v: Object.keys(object).sort().map((key) => {
    assertSafeKey(key);
    return [key, encodeValue(object[key] as Value)] as const;
  }) };
}

export function decodeValue(input: unknown, depth = 0): Value {
  if (depth > 64 || !record(input) || typeof input.t !== "string") throw new QueryValidationError("Malformed wire value");
  switch (input.t) {
    case "null": return null;
    case "bool": if (typeof input.v === "boolean") return input.v; break;
    case "number": if (typeof input.v === "number" && Number.isFinite(input.v)) return input.v; break;
    case "int64": {
      if (typeof input.v !== "string" || input.v === "-0" || !/^-?(0|[1-9][0-9]*)$/.test(input.v)) break;
      const value = BigInt(input.v);
      if (value >= -(1n << 63n) && value <= (1n << 63n) - 1n) return value;
      break;
    }
    case "string": if (typeof input.v === "string") return input.v; break;
    case "id": if (typeof input.v === "string" && isDocumentID(input.v)) return input.v; break;
    case "date": {
      if (typeof input.v !== "string") break;
      const date = new Date(input.v);
      if (Number.isFinite(date.getTime()) && date.toISOString() === input.v) return date;
      break;
    }
    case "binary": if (typeof input.v === "string") return base64ToBytes(input.v); break;
    case "array": if (Array.isArray(input.v)) return input.v.map((item) => decodeValue(item, depth + 1)); break;
    case "object": {
      if (!Array.isArray(input.v)) break;
      const out: Record<string, Value> = Object.create(null) as Record<string, Value>;
      for (const entry of input.v) {
        if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== "string") throw new QueryValidationError("Malformed object entry");
        assertSafeKey(entry[0]);
        if (Object.prototype.hasOwnProperty.call(out, entry[0])) throw new QueryValidationError("Duplicate object field");
        out[entry[0]] = decodeValue(entry[1], depth + 1);
      }
      return out;
    }
  }
  throw new QueryValidationError("Malformed wire value");
}

export function encodeDocument(document: Document): WireValue {
  return encodeDocumentFields(document, true);
}

export function encodeInputDocument(document: InputDocument): WireValue {
  return encodeDocumentFields(document, false);
}

function encodeDocumentFields(document: InputDocument, requireID: boolean): WireValue {
  if (requireID && typeof document._id !== "string") throw new QueryValidationError("Persisted document requires _id");
  const entries = Object.keys(document).sort().map((key) => {
    assertSafeKey(key);
    const value = document[key];
    if (value === undefined) throw new QueryValidationError(`Undefined document value at ${key}`);
    if (key === "_id") {
      if (typeof value !== "string" || !isDocumentID(value)) throw new QueryValidationError("Persisted _id must be a 32-character lowercase hexadecimal ID");
      return [key, { t: "id", v: value } satisfies WireValue] as const;
    }
    return [key, encodeValue(value)] as const;
  });
  return { t: "object", v: entries };
}

export function decodeDocument(input: unknown): Document {
  const value = decodeValue(input);
  if (value === null || typeof value !== "object" || Array.isArray(value) || value instanceof Date || value instanceof Uint8Array) {
    throw new QueryValidationError("Wire document must be an object");
  }
  return cloneDocument(value as Document);
}

export function encodeQuerySpec(spec: QuerySpec): WireQuerySpec {
  return {
    version: 1,
    where: encodeExpression(spec.where),
    ...(spec.sort ? { sort: spec.sort.map((item) => ({ ...item })) } : {}),
    ...(spec.skip !== undefined ? { skip: spec.skip } : {}),
    ...(spec.limit !== undefined ? { limit: spec.limit } : {}),
  };
}

export function decodeQuerySpec(input: unknown, overrides: Partial<QueryLimits> = {}): QuerySpec {
  const limits: QueryLimits = { ...DEFAULT_QUERY_LIMITS, ...overrides };
  if (!record(input)) throw new QueryValidationError("Wire query must be an object");
  onlyKeys(input, ["version", "where", "sort", "skip", "limit"]);
  if (input.version !== 1) throw new QueryValidationError("Unsupported query version");
  let nodes = 0;
  const decodeExpression = (raw: unknown, depth: number): QueryExpr => {
    if (depth > limits.maxDepth) throw new QueryValidationError("Query nesting is too deep");
    nodes += 1;
    if (nodes > limits.maxNodes) throw new QueryValidationError("Query has too many expression nodes");
    if (!record(raw) || typeof raw.op !== "string") throw new QueryValidationError("Malformed query expression");
    switch (raw.op) {
      case "true": onlyKeys(raw, ["op"]); return { op: "true" };
      case "and": case "or": {
        onlyKeys(raw, ["op", "args"]);
        if (!Array.isArray(raw.args) || raw.args.length === 0 || raw.args.length > limits.maxArrayItems) throw new QueryValidationError("Logical args outside limits");
        return { op: raw.op, args: raw.args.map((arg) => decodeExpression(arg, depth + 1)) };
      }
      case "not": onlyKeys(raw, ["op", "arg"]); return { op: "not", arg: decodeExpression(raw.arg, depth + 1) };
      case "exists": {
        onlyKeys(raw, ["op", "path", "value"]); const path = wirePath(raw.path);
        if (typeof raw.value !== "boolean") throw new QueryValidationError("exists expects boolean");
        return { op: "exists", path, value: raw.value };
      }
      case "compare": {
        onlyKeys(raw, ["op", "cmp", "path", "value"]); const path = wirePath(raw.path);
        if (raw.cmp !== "eq" && raw.cmp !== "ne" && raw.cmp !== "gt" && raw.cmp !== "gte" && raw.cmp !== "lt" && raw.cmp !== "lte") throw new QueryValidationError("Unknown comparison");
        const value = boundedDecodedValue(raw.value, limits);
        return { op: "compare", cmp: raw.cmp, path, value };
      }
      case "in": case "nin": {
        onlyKeys(raw, ["op", "path", "values"]); const path = wirePath(raw.path);
        if (!Array.isArray(raw.values) || raw.values.length > limits.maxArrayItems) throw new QueryValidationError("Membership values outside limits");
        return { op: raw.op, path, values: raw.values.map((item) => boundedDecodedValue(item, limits)) };
      }
      default: throw new QueryValidationError(`Unknown query operator: ${raw.op}`);
    }
  };
  const where = decodeExpression(input.where, 0);
  let sort: SortField[] | undefined;
  if (input.sort !== undefined) {
    if (!Array.isArray(input.sort) || input.sort.length > limits.maxSortFields) throw new QueryValidationError("Sort fields outside limits");
    sort = input.sort.map((raw) => {
      if (!record(raw)) throw new QueryValidationError("Malformed sort field");
      onlyKeys(raw, ["path", "direction"]); const path = wirePath(raw.path);
      if (raw.direction !== 1 && raw.direction !== -1) throw new QueryValidationError("Invalid sort direction");
      return { path, direction: raw.direction };
    });
  }
  const skip = optionalBoundedInteger(input.skip, 0, Number.MAX_SAFE_INTEGER, "skip");
  const limit = optionalBoundedInteger(input.limit, 0, limits.maxLimit, "limit");
  return { version: 1, where, ...(sort ? { sort } : {}), ...(skip !== undefined ? { skip } : {}), ...(limit !== undefined ? { limit } : {}) };
}

function encodeExpression(expression: QueryExpr): WireQueryExpr {
  switch (expression.op) {
    case "true": return expression;
    case "and": case "or": return { op: expression.op, args: expression.args.map(encodeExpression) };
    case "not": return { op: "not", arg: encodeExpression(expression.arg) };
    case "exists": return { ...expression };
    case "compare": return { ...expression, value: encodeQueryValue(expression.path, expression.value) };
    case "in": case "nin": return { ...expression, values: expression.values.map((value) => encodeQueryValue(expression.path, value)) };
  }
}

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function onlyKeys(value: Record<string, unknown>, allowed: readonly string[]): void {
  const permitted = new Set(allowed);
  for (const key of Object.keys(value)) if (!permitted.has(key)) throw new QueryValidationError(`Unexpected query field: ${key}`);
}

function wirePath(value: unknown): string {
  if (typeof value !== "string") throw new QueryValidationError("Query path must be a string");
  assertPath(value); return value;
}

function boundedDecodedValue(value: unknown, limits: QueryLimits): Value {
  const decoded = decodeValue(value);
  if (valueByteLength(decoded) > limits.maxStringBytes) throw new QueryValidationError("Query value is too large");
  return decoded;
}

function optionalBoundedInteger(value: unknown, min: number, max: number, name: string): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max) throw new QueryValidationError(`Invalid ${name}`);
  return value;
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function base64ToBytes(value: string): Uint8Array {
  let binary: string;
  try { binary = atob(value); } catch { throw new QueryValidationError("Malformed base64 value"); }
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

function encodeQueryValue(path: string, value: Value): WireValue {
  if (path === "_id") {
    if (typeof value !== "string" || !isDocumentID(value)) throw new QueryValidationError("_id query value must be a 32-character lowercase hexadecimal ID");
    return { t: "id", v: value };
  }
  return encodeValue(value);
}

function isDocumentID(value: string): boolean { return /^[0-9a-f]{32}$/.test(value); }
