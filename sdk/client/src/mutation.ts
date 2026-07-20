import type { Document, MutationOperation, MutationSpec, QueryLimits, Update, Value } from "./types.js";
import { DEFAULT_QUERY_LIMITS, QueryValidationError } from "./types.js";
import { assertPath, cloneDocument, cloneValue, getPath, valueByteLength, valueEquals } from "./safe-value.js";
import { decodeValue, encodeValue, type WireValue } from "./wire.js";

export type WireMutationOperation =
  | { readonly op: "set" | "inc" | "push" | "pull"; readonly path: string; readonly value: WireValue }
  | { readonly op: "unset"; readonly path: string };

export interface WireMutationSpec {
  readonly version: 1;
  readonly operations: readonly WireMutationOperation[];
}

const operatorOrder = ["$inc", "$pull", "$push", "$set", "$unset"] as const;

export function compileUpdate(update: Update, overrides: Partial<QueryLimits> = {}): MutationSpec {
  if (!plainObject(update)) throw new QueryValidationError("Update must be an object");
  const limits = { ...DEFAULT_QUERY_LIMITS, ...overrides };
  const unknown = Object.keys(update).filter((key) => !(operatorOrder as readonly string[]).includes(key));
  if (unknown.length) throw new QueryValidationError(`Unknown update operator: ${unknown[0]}`);
  const operations: MutationOperation[] = [];
  const paths: string[] = [];
  for (const operator of operatorOrder) {
    const raw = update[operator];
    if (raw === undefined) continue;
    if (operator === "$unset") {
      const unsetPaths = Array.isArray(raw) ? [...raw] : plainObject(raw) ? Object.entries(raw).map(([path, enabled]) => {
        if (enabled !== true) throw new QueryValidationError("$unset object values must be true"); return path;
      }) : fail("$unset expects a path array or object");
      if (unsetPaths.length === 0) throw new QueryValidationError("$unset cannot be empty");
      for (const path of unsetPaths.sort()) { validateMutationPath(path, paths); operations.push({ op: "unset", path }); }
      continue;
    }
    if (!plainObject(raw) || Object.keys(raw).length === 0) throw new QueryValidationError(`${operator} expects a non-empty object`);
    for (const path of Object.keys(raw).sort()) {
      validateMutationPath(path, paths);
      const operand = raw[path];
      if (operand === undefined) throw new QueryValidationError(`Missing update value at ${path}`);
      const value = cloneValue(operand as Value);
      if (valueByteLength(value) > limits.maxStringBytes) throw new QueryValidationError("Update value is too large");
      if (operator === "$inc" && typeof value !== "number" && typeof value !== "bigint") throw new QueryValidationError("$inc expects numeric values");
      operations.push({ op: operator.slice(1) as "set" | "inc" | "push" | "pull", path, value });
    }
  }
  if (operations.length === 0) throw new QueryValidationError("Update cannot be empty");
  if (operations.length > limits.maxNodes) throw new QueryValidationError("Update has too many operations");
  return { version: 1, operations };
}

export function encodeMutationSpec(spec: MutationSpec): WireMutationSpec {
  return { version: 1, operations: spec.operations.map((operation) => operation.op === "unset" ? { ...operation } : { ...operation, value: encodeValue(operation.value) }) };
}

export function decodeMutationSpec(input: unknown, overrides: Partial<QueryLimits> = {}): MutationSpec {
  const limits = { ...DEFAULT_QUERY_LIMITS, ...overrides };
  if (!plainObject(input)) throw new QueryValidationError("Wire mutation must be an object");
  onlyKeys(input, ["version", "operations"]);
  if (input.version !== 1 || !Array.isArray(input.operations) || input.operations.length === 0 || input.operations.length > limits.maxNodes) throw new QueryValidationError("Malformed mutation envelope");
  const paths: string[] = [];
  const operations = input.operations.map((raw): MutationOperation => {
    if (!plainObject(raw) || typeof raw.op !== "string" || typeof raw.path !== "string") throw new QueryValidationError("Malformed mutation operation");
    validateMutationPath(raw.path, paths);
    if (raw.op === "unset") { onlyKeys(raw, ["op", "path"]); return { op: "unset", path: raw.path }; }
    if (raw.op !== "set" && raw.op !== "inc" && raw.op !== "push" && raw.op !== "pull") throw new QueryValidationError(`Unknown mutation operation: ${raw.op}`);
    onlyKeys(raw, ["op", "path", "value"]);
    const value = decodeValue(raw.value);
    if (valueByteLength(value) > limits.maxStringBytes) throw new QueryValidationError("Update value is too large");
    if (raw.op === "inc" && typeof value !== "number" && typeof value !== "bigint") throw new QueryValidationError("inc expects numeric wire value");
    return { op: raw.op, path: raw.path, value };
  });
  return { version: 1, operations };
}

export function applyMutation<T extends Document>(document: T, spec: MutationSpec): T {
  const output = cloneDocument(document) as T & Record<string, Value>;
  for (const operation of spec.operations) {
    if (operation.op === "unset") { unsetPath(output, operation.path); continue; }
    if (operation.op === "set") { setPath(output, operation.path, operation.value); continue; }
    if (operation.op === "inc") {
      const current = getPath(output, operation.path);
      setPath(output, operation.path, current.found ? addNumbers(current.value as Value, operation.value) : operation.value);
      continue;
    }
    if (operation.op === "push") {
      const current = getPath(output, operation.path);
      if (!current.found) setPath(output, operation.path, [operation.value]);
      else if (!Array.isArray(current.value)) throw new QueryValidationError("push target is not an array");
      else setPath(output, operation.path, [...current.value, operation.value]);
      continue;
    }
    const current = getPath(output, operation.path);
    if (!current.found) continue;
    if (!Array.isArray(current.value)) throw new QueryValidationError("pull target is not an array");
    setPath(output, operation.path, current.value.filter((item) => !valueEquals(item, operation.value)));
  }
  return cloneDocument(output) as T;
}

function validateMutationPath(path: string, previous: string[]): void {
  assertPath(path);
  if (path === "_id" || path.startsWith("_id.")) throw new QueryValidationError("_id is immutable");
  for (const existing of previous) if (existing === path || existing.startsWith(`${path}.`) || path.startsWith(`${existing}.`)) throw new QueryValidationError(`Conflicting update paths: ${existing} and ${path}`);
  previous.push(path);
}

function setPath(document: Record<string, Value>, path: string, raw: Value): void {
  const parts = path.split("."); let current = document;
  for (const part of parts.slice(0, -1)) {
    const value = current[part];
    if (value === undefined) { const child = Object.create(null) as Record<string, Value>; current[part] = child; current = child; continue; }
    if (value === null || Array.isArray(value) || value instanceof Date || value instanceof Uint8Array || typeof value !== "object") throw new QueryValidationError("Update path traverses a non-object");
    current = value as Record<string, Value>;
  }
  current[parts.at(-1) as string] = cloneValue(raw);
}

function unsetPath(document: Record<string, Value>, path: string): void {
  const parts = path.split("."); let current = document;
  for (const part of parts.slice(0, -1)) {
    const value = current[part];
    if (value === undefined || value === null || Array.isArray(value) || value instanceof Date || value instanceof Uint8Array || typeof value !== "object") return;
    current = value as Record<string, Value>;
  }
  delete current[parts.at(-1) as string];
}

function addNumbers(left: Value, right: Value): number | bigint {
  if ((typeof left !== "number" && typeof left !== "bigint") || (typeof right !== "number" && typeof right !== "bigint")) throw new QueryValidationError("inc target is not numeric");
  if (typeof left === "bigint" && typeof right === "bigint") {
    const result = left + right;
    if (result < -(1n << 63n) || result > (1n << 63n) - 1n) throw new QueryValidationError("Int64 increment overflow");
    return result;
  }
  const leftNumber = typeof left === "bigint" ? exactNumber(left) : left;
  const rightNumber = typeof right === "bigint" ? exactNumber(right) : right;
  const result = leftNumber + rightNumber;
  if (!Number.isFinite(result)) throw new QueryValidationError("Float64 increment is not finite");
  return result;
}

function exactNumber(value: bigint): number {
  const converted = Number(value);
  if (!Number.isSafeInteger(converted) || BigInt(converted) !== value) throw new QueryValidationError("Mixed Int64/Float64 increment would lose precision");
  return converted;
}

function plainObject(value: unknown): value is Record<string, unknown> { if (value === null || typeof value !== "object" || Array.isArray(value) || value instanceof Date || value instanceof Uint8Array) return false; const prototype = Object.getPrototypeOf(value); return prototype === Object.prototype || prototype === null; }
function onlyKeys(value: Record<string, unknown>, allowed: readonly string[]): void { const set = new Set(allowed); for (const key of Object.keys(value)) if (!set.has(key)) throw new QueryValidationError(`Unexpected mutation field: ${key}`); }
function fail(message: string): never { throw new QueryValidationError(message); }
