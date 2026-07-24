import type {
  Document,
  ElementQueryExpr,
  InputDocument,
  QueryExpr,
  QueryLimits,
  QuerySpec,
  QueryTypeName,
  SortField,
  Value,
} from "./types.js";
import { DEFAULT_QUERY_LIMITS, QueryValidationError } from "./types.js";
import {
  assertPath,
  assertQueryValue,
  assertSafeKey,
  assertWellFormedString,
  cloneDocument,
  cloneTransportValue,
  documentID,
  isDocumentID,
  isDocumentIDValue,
  normalizeQueryTypes,
  valueEquals,
} from "./safe-value.js";

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
  | {
      readonly op: "compare";
      readonly cmp: "eq" | "ne" | "gt" | "gte" | "lt" | "lte";
      readonly path: string;
      readonly value: WireValue;
    }
  | { readonly op: "in" | "nin"; readonly path: string; readonly values: readonly WireValue[] }
  | { readonly op: "exists"; readonly path: string; readonly value: boolean }
  | { readonly op: "size"; readonly path: string; readonly size: number }
  | { readonly op: "type"; readonly path: string; readonly types: readonly QueryTypeName[] }
  | { readonly op: "all"; readonly path: string; readonly values: readonly WireValue[] }
  | { readonly op: "elem_match"; readonly path: string; readonly mode: "scalar"; readonly arg: WireElementQueryExpr }
  | { readonly op: "elem_match"; readonly path: string; readonly mode: "object"; readonly arg: WireQueryExpr };

export type WireElementQueryExpr =
  | { readonly op: "and" | "or"; readonly args: readonly WireElementQueryExpr[] }
  | { readonly op: "not"; readonly arg: WireElementQueryExpr }
  | { readonly op: "compare"; readonly cmp: "eq" | "ne" | "gt" | "gte" | "lt" | "lte"; readonly value: WireValue }
  | { readonly op: "in" | "nin"; readonly values: readonly WireValue[] };

export interface WireQuerySpec {
  readonly version: 1;
  readonly where: WireQueryExpr;
  readonly sort?: readonly SortField[];
  readonly skip?: number;
  readonly limit?: number;
  readonly seek?: true;
}

export function encodeValue(value: Value): WireValue {
  return encodeCheckedValue(cloneTransportValue(value));
}

function encodeCheckedValue(value: Value): WireValue {
  if (value === null) return { t: "null" };
  if (typeof value === "boolean") return { t: "bool", v: value };
  if (typeof value === "number") return { t: "number", v: value };
  if (typeof value === "bigint") return { t: "int64", v: value.toString() };
  if (isDocumentIDValue(value)) return { t: "id", v: value.value };
  if (typeof value === "string") return { t: "string", v: value };
  if (value instanceof Date) return { t: "date", v: value.toISOString() };
  if (value instanceof Uint8Array) return { t: "binary", v: bytesToBase64(value) };
  if (Array.isArray(value)) return { t: "array", v: value.map(encodeCheckedValue) };
  const object = value as { readonly [key: string]: Value };
  return {
    t: "object",
    v: Object.keys(object)
      .sort()
      .map((key) => {
        assertSafeKey(key);
        return [key, encodeCheckedValue(object[key] as Value)] as const;
      }),
  };
}

export function decodeValue(input: unknown, depth = 0): Value {
  if (depth > 64 || !record(input) || typeof input.t !== "string")
    throw new QueryValidationError("Malformed wire value");
  switch (input.t) {
    case "null":
      onlyKeys(input, ["t"]);
      return null;
    case "bool":
      if (has(input, "v") && typeof input.v === "boolean") {
        onlyKeys(input, ["t", "v"]);
        return input.v;
      }
      break;
    case "number":
      if (has(input, "v") && typeof input.v === "number" && Number.isFinite(input.v)) {
        onlyKeys(input, ["t", "v"]);
        return input.v;
      }
      break;
    case "int64": {
      if (!has(input, "v") || typeof input.v !== "string" || input.v === "-0" || !/^-?(0|[1-9][0-9]*)$/.test(input.v))
        break;
      onlyKeys(input, ["t", "v"]);
      const value = BigInt(input.v);
      if (value >= -(1n << 63n) && value <= (1n << 63n) - 1n) return value;
      break;
    }
    case "string":
      if (has(input, "v") && typeof input.v === "string") {
        onlyKeys(input, ["t", "v"]);
        assertWellFormedString(input.v);
        return input.v;
      }
      break;
    case "id":
      if (has(input, "v") && typeof input.v === "string" && isDocumentID(input.v)) {
        onlyKeys(input, ["t", "v"]);
        return documentID(input.v);
      }
      break;
    case "date": {
      if (!has(input, "v") || typeof input.v !== "string") break;
      onlyKeys(input, ["t", "v"]);
      const date = new Date(input.v);
      if (Number.isFinite(date.getTime()) && date.toISOString() === input.v) return date;
      break;
    }
    case "binary":
      if (has(input, "v") && typeof input.v === "string") {
        onlyKeys(input, ["t", "v"]);
        const bytes = base64ToBytes(input.v);
        if (bytesToBase64(bytes) === input.v) return bytes;
      }
      break;
    case "array":
      if (has(input, "v") && Array.isArray(input.v)) {
        onlyKeys(input, ["t", "v"]);
        return input.v.map((item) => decodeValue(item, depth + 1));
      }
      break;
    case "object": {
      if (!has(input, "v") || !Array.isArray(input.v)) break;
      onlyKeys(input, ["t", "v"]);
      const out: Record<string, Value> = Object.create(null) as Record<string, Value>;
      for (const entry of input.v) {
        if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== "string")
          throw new QueryValidationError("Malformed object entry");
        assertSafeKey(entry[0]);
        if (Object.prototype.hasOwnProperty.call(out, entry[0]))
          throw new QueryValidationError("Duplicate object field");
        out[entry[0]] = decodeValue(entry[1], depth + 1);
      }
      return out;
    }
  }
  throw new QueryValidationError("Malformed wire value");
}

export function encodeDocument(document: Document): WireValue {
  return encodeDocumentFields(cloneTransportValue(document) as Document, true);
}

export function encodeInputDocument(document: InputDocument): WireValue {
  return encodeDocumentFields(cloneTransportValue(document) as InputDocument, false);
}

function encodeDocumentFields(document: InputDocument, requireID: boolean): WireValue {
  if (requireID && typeof document._id !== "string") throw new QueryValidationError("Persisted document requires _id");
  const entries = Object.keys(document)
    .sort()
    .map((key) => {
      assertSafeKey(key);
      const value = document[key];
      if (value === undefined) throw new QueryValidationError(`Undefined document value at ${key}`);
      if (key === "_id") {
        if (!isDocumentID(value))
          throw new QueryValidationError("Persisted _id must be a non-zero 32-character lowercase hexadecimal ID");
        return [key, { t: "id", v: value } satisfies WireValue] as const;
      }
      return [key, encodeCheckedValue(value)] as const;
    });
  return { t: "object", v: entries };
}

export function decodeDocument(input: unknown): Document {
  const value = decodeValue(input);
  if (
    value === null ||
    typeof value !== "object" ||
    isDocumentIDValue(value) ||
    Array.isArray(value) ||
    value instanceof Date ||
    value instanceof Uint8Array
  ) {
    throw new QueryValidationError("Wire document must be an object");
  }
  const fields = value as Record<string, Value>;
  const id = fields._id;
  if (!isDocumentIDValue(id)) throw new QueryValidationError("Persisted document _id must use the id wire type");
  const document: Record<string, Value> = Object.create(null) as Record<string, Value>;
  for (const [key, field] of Object.entries(fields)) document[key] = key === "_id" ? id.value : field;
  return cloneDocument(document as Document);
}

export function encodeQuerySpec(spec: QuerySpec): WireQuerySpec {
  const encoded = {
    version: 1 as const,
    where: encodeExpression(spec.where),
    ...(spec.sort ? { sort: spec.sort.map((item) => ({ ...item })) } : {}),
    ...(spec.skip !== undefined ? { skip: spec.skip } : {}),
    ...(spec.limit !== undefined ? { limit: spec.limit } : {}),
    ...(spec.seek ? { seek: true as const } : {}),
  };
  if (wireQueryByteLength(encoded) > DEFAULT_QUERY_LIMITS.maxWireBytes)
    throw new QueryValidationError("Query exceeds wire size limit");
  return encoded;
}

export function decodeQuerySpec(input: unknown, overrides: Partial<QueryLimits> = {}): QuerySpec {
  const limits: QueryLimits = { ...DEFAULT_QUERY_LIMITS, ...overrides };
  if (wireQueryByteLength(input) > limits.maxWireBytes) throw new QueryValidationError("Query exceeds wire size limit");
  if (!record(input)) throw new QueryValidationError("Wire query must be an object");
  onlyKeys(input, ["version", "where", "sort", "skip", "limit", "seek"]);
  if (input.version !== 1) throw new QueryValidationError("Unsupported query version");
  let nodes = 0;
  const decodeElementExpression = (raw: unknown, depth: number): ElementQueryExpr => {
    if (depth > limits.maxDepth) throw new QueryValidationError("Query nesting is too deep");
    nodes += 1;
    if (nodes > limits.maxNodes) throw new QueryValidationError("Query has too many expression nodes");
    if (!record(raw) || typeof raw.op !== "string") throw new QueryValidationError("Malformed scalar elem match expression");
    switch (raw.op) {
      case "and":
      case "or":
        onlyKeys(raw, ["op", "args"]);
        if (!Array.isArray(raw.args) || raw.args.length === 0 || raw.args.length > limits.maxArrayItems)
          throw new QueryValidationError("Scalar elem match args outside limits");
        return { op: raw.op, args: raw.args.map((arg) => decodeElementExpression(arg, depth + 1)) };
      case "not":
        onlyKeys(raw, ["op", "arg"]);
        return { op: "not", arg: decodeElementExpression(raw.arg, depth + 1) };
      case "in":
      case "nin":
        onlyKeys(raw, ["op", "values"]);
        if (!Array.isArray(raw.values) || raw.values.length > limits.maxArrayItems)
          throw new QueryValidationError("Scalar elem match membership outside limits");
        return { op: raw.op, values: raw.values.map((item) => boundedDecodedValue(item, limits)) };
      case "compare": {
        onlyKeys(raw, ["op", "cmp", "value"]);
        if (raw.cmp !== "eq" && raw.cmp !== "ne" && raw.cmp !== "gt" && raw.cmp !== "gte" && raw.cmp !== "lt" && raw.cmp !== "lte")
          throw new QueryValidationError("Unknown scalar elem match comparison");
        return { op: "compare", cmp: raw.cmp, value: boundedDecodedValue(raw.value, limits) };
      }
      default:
        throw new QueryValidationError(`Unknown scalar elem match operator: ${raw.op}`);
    }
  };
  const decodeExpression = (raw: unknown, depth: number): QueryExpr => {
    if (depth > limits.maxDepth) throw new QueryValidationError("Query nesting is too deep");
    nodes += 1;
    if (nodes > limits.maxNodes) throw new QueryValidationError("Query has too many expression nodes");
    if (!record(raw) || typeof raw.op !== "string") throw new QueryValidationError("Malformed query expression");
    switch (raw.op) {
      case "true":
        onlyKeys(raw, ["op"]);
        return { op: "true" };
      case "and":
      case "or": {
        onlyKeys(raw, ["op", "args"]);
        if (!Array.isArray(raw.args) || raw.args.length === 0 || raw.args.length > limits.maxArrayItems)
          throw new QueryValidationError("Logical args outside limits");
        return { op: raw.op, args: raw.args.map((arg) => decodeExpression(arg, depth + 1)) };
      }
      case "not":
        onlyKeys(raw, ["op", "arg"]);
        return { op: "not", arg: decodeExpression(raw.arg, depth + 1) };
      case "exists": {
        onlyKeys(raw, ["op", "path", "value"]);
        const path = wirePath(raw.path);
        if (typeof raw.value !== "boolean") throw new QueryValidationError("exists expects boolean");
        return { op: "exists", path, value: raw.value };
      }
      case "size": {
        onlyKeys(raw, ["op", "path", "size"]);
        const path = wirePath(raw.path);
        if (typeof raw.size !== "number" || !Number.isSafeInteger(raw.size) || raw.size < 0)
          throw new QueryValidationError("size expects a non-negative safe integer");
        return { op: "size", path, size: raw.size };
      }
      case "type": {
        onlyKeys(raw, ["op", "path", "types"]);
        const path = wirePath(raw.path);
        if (!Array.isArray(raw.types)) throw new QueryValidationError("type expects a type-name array");
        return { op: "type", path, types: normalizeQueryTypes(raw.types, limits.maxArrayItems) };
      }
      case "compare": {
        onlyKeys(raw, ["op", "cmp", "path", "value"]);
        const path = wirePath(raw.path);
        if (
          raw.cmp !== "eq" &&
          raw.cmp !== "ne" &&
          raw.cmp !== "gt" &&
          raw.cmp !== "gte" &&
          raw.cmp !== "lt" &&
          raw.cmp !== "lte"
        )
          throw new QueryValidationError("Unknown comparison");
        const value = boundedDecodedValue(raw.value, limits, path);
        return { op: "compare", cmp: raw.cmp, path, value };
      }
      case "in":
      case "nin":
      case "all": {
        onlyKeys(raw, ["op", "path", "values"]);
        const path = wirePath(raw.path);
        if (!Array.isArray(raw.values) || raw.values.length > limits.maxArrayItems || (raw.op === "all" && raw.values.length === 0))
          throw new QueryValidationError("Membership values outside limits");
        const values = raw.values.map((item) => boundedDecodedValue(item, limits, path));
        return raw.op === "all" ? { op: "all", path, values: dedupeValues(values) } : { op: raw.op, path, values };
      }
      case "elem_match": {
        onlyKeys(raw, ["op", "path", "mode", "arg"]);
        const path = wirePath(raw.path);
        if (raw.mode === "scalar") return { op: "elem_match", path, mode: "scalar", arg: decodeElementExpression(raw.arg, depth + 1) };
        if (raw.mode === "object") return { op: "elem_match", path, mode: "object", arg: decodeExpression(raw.arg, depth + 1) };
        throw new QueryValidationError("Invalid elem match mode");
      }
      default:
        throw new QueryValidationError(`Unknown query operator: ${raw.op}`);
    }
  };
  const where = decodeExpression(input.where, 0);
  let sort: SortField[] | undefined;
  if (input.sort !== undefined) {
    if (!Array.isArray(input.sort) || input.sort.length > limits.maxSortFields)
      throw new QueryValidationError("Sort fields outside limits");
    sort = input.sort.map((raw) => {
      if (!record(raw)) throw new QueryValidationError("Malformed sort field");
      onlyKeys(raw, ["path", "direction"]);
      const path = wirePath(raw.path);
      if (raw.direction !== 1 && raw.direction !== -1) throw new QueryValidationError("Invalid sort direction");
      return { path, direction: raw.direction };
    });
    if (new Set(sort.map((field) => field.path)).size !== sort.length)
      throw new QueryValidationError("Sort paths must be unique");
  }
  const skip = optionalBoundedInteger(input.skip, 0, Number.MAX_SAFE_INTEGER, "skip");
  const limit = optionalBoundedInteger(input.limit, 0, limits.maxLimit, "limit");
  if (input.seek !== undefined && input.seek !== true) throw new QueryValidationError("seek must be true");
  if (input.seek === true && (!sort || limit === undefined))
    throw new QueryValidationError("seek pagination requires sort and limit");
  return {
    version: 1,
    where,
    ...(sort ? { sort } : {}),
    ...(skip !== undefined ? { skip } : {}),
    ...(limit !== undefined ? { limit } : {}),
    ...(input.seek === true ? { seek: true } : {}),
  };
}

export function wireQueryByteLength(value: unknown): number {
  try {
    return new TextEncoder().encode(JSON.stringify(value)).byteLength;
  } catch {
    throw new QueryValidationError("Query must be JSON-serializable");
  }
}

function encodeExpression(expression: QueryExpr): WireQueryExpr {
  switch (expression.op) {
    case "true":
      return expression;
    case "and":
    case "or":
      return { op: expression.op, args: expression.args.map(encodeExpression) };
    case "not":
      return { op: "not", arg: encodeExpression(expression.arg) };
    case "exists":
      return { ...expression };
    case "size":
      assertPath(expression.path);
      if (!Number.isSafeInteger(expression.size) || expression.size < 0)
        throw new QueryValidationError("size expects a non-negative safe integer");
      return { op: "size", path: expression.path, size: expression.size };
    case "type":
      assertPath(expression.path);
      return { op: "type", path: expression.path, types: normalizeQueryTypes(expression.types) };
    case "all":
      assertPath(expression.path);
      if (expression.values.length === 0) throw new QueryValidationError("all expects a non-empty array");
      return { op: "all", path: expression.path, values: dedupeValues(expression.values).map((value) => encodeQueryValue(expression.path, value)) };
    case "elem_match":
      assertPath(expression.path);
      return expression.mode === "scalar"
        ? { op: "elem_match", path: expression.path, mode: "scalar", arg: encodeElementExpression(expression.arg) }
        : { op: "elem_match", path: expression.path, mode: "object", arg: encodeExpression(expression.arg) };
    case "compare":
      return { ...expression, value: encodeQueryValue(expression.path, expression.value) };
    case "in":
    case "nin":
      return { ...expression, values: expression.values.map((value) => encodeQueryValue(expression.path, value)) };
  }
}

function encodeElementExpression(expression: ElementQueryExpr): WireElementQueryExpr {
  switch (expression.op) {
    case "and":
    case "or":
      return { op: expression.op, args: expression.args.map(encodeElementExpression) };
    case "not":
      return { op: "not", arg: encodeElementExpression(expression.arg) };
    case "in":
    case "nin":
      return { op: expression.op, values: expression.values.map(encodeValue) };
    case "compare":
      return { op: "compare", cmp: expression.cmp, value: encodeValue(expression.value) };
  }
}

function record(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function has(value: Record<string, unknown>, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function onlyKeys(value: Record<string, unknown>, allowed: readonly string[]): void {
  const permitted = new Set(allowed);
  for (const key of Object.keys(value))
    if (!permitted.has(key)) throw new QueryValidationError(`Unexpected query field: ${key}`);
}

function wirePath(value: unknown): string {
  if (typeof value !== "string") throw new QueryValidationError("Query path must be a string");
  assertPath(value);
  return value;
}

function boundedDecodedValue(value: unknown, limits: QueryLimits, path?: string): Value {
  const decoded = decodeValue(value);
  const queryValue = path === "_id" ? decodePersistedID(decoded, "_id query value") : decoded;
  assertQueryValue(queryValue, limits);
  return queryValue;
}

function decodePersistedID(value: Value, label: string): string {
  if (!isDocumentIDValue(value)) throw new QueryValidationError(`${label} must use the id wire type`);
  return value.value;
}

function optionalBoundedInteger(value: unknown, min: number, max: number, name: string): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max)
    throw new QueryValidationError(`Invalid ${name}`);
  return value;
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function base64ToBytes(value: string): Uint8Array {
  let binary: string;
  try {
    binary = atob(value);
  } catch {
    throw new QueryValidationError("Malformed base64 value");
  }
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

function encodeQueryValue(path: string, value: Value): WireValue {
  if (path === "_id") {
    if (!isDocumentID(value))
      throw new QueryValidationError("_id query value must be a non-zero 32-character lowercase hexadecimal ID");
    return { t: "id", v: value };
  }
  return encodeValue(value);
}

function dedupeValues(values: readonly Value[]): Value[] {
  const result: Value[] = [];
  for (const value of values) {
    if (!result.some((candidate) => valueEquals(candidate, value))) result.push(value);
  }
  return result;
}
