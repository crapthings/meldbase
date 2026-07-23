import type { Comparison, Document, Filter, SortField, Value } from "./types.js";
import { QueryValidationError } from "./types.js";
import { getPath, isDocumentIDValue } from "./safe-value.js";
import { decodeValue, encodeValue, type WireValue } from "./wire.js";

interface CursorMissing {
  readonly missing: true;
}
type CursorValue = WireValue | CursorMissing;
interface CursorPayload {
  readonly version: 1;
  readonly sort: readonly SortField[];
  readonly values: readonly CursorValue[];
}
interface CursorComponent {
  readonly found: boolean;
  readonly value?: Value;
}

const MAX_CURSOR_CHARACTERS = 16_384;
const MAX_CURSOR_PAYLOAD_BYTES = Math.floor(MAX_CURSOR_CHARACTERS / 4) * 3;

export interface PageResult<T extends Document> {
  readonly documents: readonly T[];
  readonly nextCursor?: string;
}

export function pageCursorFor(document: Document, sort: readonly SortField[]): string {
  const normalized = normalizePageSort(sort);
  const values = normalized.map((field) => {
    const found = getPath(document, field.path);
    if (!found.found) return { missing: true } satisfies CursorMissing;
    if (!isCursorScalar(found.value as Value))
      throw new QueryValidationError(`Cannot create cursor: ${field.path} is not scalar`);
    return encodeValue(found.value as Value);
  });
  return encodeCursor({ version: 1, sort: normalized, values });
}

export function pageFilterAfter(cursor: string, sort: readonly SortField[]): Filter {
  const normalized = normalizePageSort(sort);
  const payload = decodeCursor(cursor);
  if (!sameSort(payload.sort, normalized) || payload.values.length !== normalized.length)
    throw new QueryValidationError("Page cursor does not match this query sort");
  const values = payload.values.map(decodeCursorValue);
  const branches: Filter[] = [];
  for (let index = 0; index < normalized.length; index += 1) {
    const field = normalized[index]!;
    const after = afterFilter(field, values[index]!);
    if (after === undefined) continue;
    const conditions: Filter[] = [];
    for (let previous = 0; previous < index; previous += 1)
      conditions.push(equalFilter(normalized[previous]!, values[previous]!));
    conditions.push(after);
    branches.push(conditions.length === 1 ? conditions[0]! : { $and: conditions });
  }
  if (branches.length === 0) return { $and: [{ _id: { $exists: true } }, { _id: { $exists: false } }] };
  return branches.length === 1 ? branches[0]! : { $or: branches };
}

export function normalizePageSort(sort: readonly SortField[]): readonly SortField[] {
  if (!Array.isArray(sort) || sort.length === 0)
    throw new QueryValidationError("Seek pagination requires at least one sort field");
  const normalized = sort.map((field) => ({ ...field }));
  const paths = new Set<string>();
  for (const field of normalized) {
    if (paths.has(field.path)) throw new QueryValidationError(`Page sort cannot contain ${field.path} more than once`);
    paths.add(field.path);
  }
  const ids = normalized.filter((field) => field.path === "_id");
  if (ids.length === 0) normalized.push({ path: "_id", direction: 1 });
  return Object.freeze(normalized);
}

function equalFilter(field: SortField, value: CursorComponent): Filter {
  return value.found ? { [field.path]: value.value! } : { [field.path]: { $exists: false } };
}

function afterFilter(field: SortField, value: CursorComponent): Filter | undefined {
  if (!value.found) return field.direction === 1 ? { [field.path]: { $exists: true } } : undefined;
  const comparison: Comparison = field.direction === 1 ? { $gt: value.value! } : { $lt: value.value! };
  if (field.direction === 1) return { [field.path]: comparison };
  return { $or: [{ [field.path]: comparison }, { [field.path]: { $exists: false } }] };
}

function isCursorScalar(value: Value): boolean {
  return (
    !Array.isArray(value) &&
    (value === null ||
      typeof value !== "object" ||
      isDocumentIDValue(value) ||
      value instanceof Date ||
      value instanceof Uint8Array)
  );
}

function decodeCursorValue(value: CursorValue): CursorComponent {
  if (isCursorMissing(value)) return { found: false };
  const decoded = decodeValue(value);
  if (!isCursorScalar(decoded)) throw new QueryValidationError("Malformed page cursor");
  return { found: true, value: decoded };
}

function isCursorMissing(value: unknown): value is CursorMissing {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    Object.keys(value).length === 1 &&
    (value as Record<string, unknown>).missing === true
  );
}

function encodeCursor(payload: CursorPayload): string {
  const bytes = new TextEncoder().encode(JSON.stringify(payload));
  if (bytes.byteLength > MAX_CURSOR_PAYLOAD_BYTES) throw new QueryValidationError("Page cursor exceeds size limit");
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  const cursor = btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
  if (cursor.length > MAX_CURSOR_CHARACTERS) throw new QueryValidationError("Page cursor exceeds size limit");
  return cursor;
}

function decodeCursor(cursor: string): CursorPayload {
  if (
    typeof cursor !== "string" ||
    cursor.length === 0 ||
    cursor.length > MAX_CURSOR_CHARACTERS ||
    !/^[A-Za-z0-9_-]+$/.test(cursor)
  )
    throw new QueryValidationError("Malformed page cursor");
  try {
    const normalized = cursor.replaceAll("-", "+").replaceAll("_", "/");
    const binary = atob(normalized + "=".repeat((4 - (normalized.length % 4)) % 4));
    if (binary.length > MAX_CURSOR_PAYLOAD_BYTES) throw new Error("invalid");
    const payload = JSON.parse(
      new TextDecoder("utf-8", { fatal: true }).decode(Uint8Array.from(binary, (character) => character.charCodeAt(0))),
    );
    if (!payload || payload.version !== 1 || !Array.isArray(payload.sort) || !Array.isArray(payload.values))
      throw new Error("invalid");
    const sort = payload.sort.map((field: unknown) => {
      if (!field || typeof field !== "object") throw new Error("invalid");
      const candidate = field as Record<string, unknown>;
      if (typeof candidate.path !== "string" || (candidate.direction !== 1 && candidate.direction !== -1))
        throw new Error("invalid");
      return { path: candidate.path, direction: candidate.direction } satisfies SortField;
    });
    return { version: 1, sort, values: payload.values as WireValue[] };
  } catch {
    throw new QueryValidationError("Malformed page cursor");
  }
}

function sameSort(left: readonly SortField[], right: readonly SortField[]): boolean {
  return (
    left.length === right.length &&
    left.every((field, index) => field.path === right[index]!.path && field.direction === right[index]!.direction)
  );
}
