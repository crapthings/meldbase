import type {
  Comparison,
  Filter,
  QueryExpr,
  QueryLimits,
  QuerySpec,
  SortField,
  Value,
} from "./types.js";
import { DEFAULT_QUERY_LIMITS, QueryValidationError } from "./types.js";
import {
  assertDocumentID,
  assertPath,
  assertQueryValue,
  cloneValue,
  compareSortValues,
  compareValues,
  getPath,
  normalizeQueryTypes,
  queryTypeNameOf,
  valueEquals,
} from "./safe-value.js";
import { normalizePageSort, pageFilterAfter } from "./cursor.js";
import { encodeQuerySpec, wireQueryByteLength } from "./wire.js";

const fieldOperators = new Set([
  "$eq",
  "$ne",
  "$gt",
  "$gte",
  "$lt",
  "$lte",
  "$in",
  "$nin",
  "$exists",
  "$size",
  "$type",
  "$all",
  "$elemMatch",
  "$not",
]);

export interface QueryOptions {
  readonly sort?: readonly SortField[];
  readonly skip?: number;
  readonly limit?: number;
  readonly first?: number;
  readonly after?: string;
  readonly limits?: Partial<QueryLimits>;
}

/** Options for selecting one document. Pagination is intentionally excluded. */
export interface FindOneOptions {
  readonly sort?: readonly SortField[];
}

function limitsWith(overrides?: Partial<QueryLimits>): QueryLimits {
  return Object.freeze({ ...DEFAULT_QUERY_LIMITS, ...overrides });
}

export function compileQuery(filter: Filter = {}, options: QueryOptions = {}): QuerySpec {
  const limits = limitsWith(options.limits);
  if (options.limit !== undefined && options.first !== undefined)
    throw new QueryValidationError("Use either limit or first, not both");
  if (options.after !== undefined && options.first === undefined)
    throw new QueryValidationError("after requires first");
  if (options.after !== undefined && options.skip !== undefined)
    throw new QueryValidationError("Seek pagination cannot be combined with skip");
  let nodes = 0;
  const addNode = (): void => {
    nodes += 1;
    if (nodes > limits.maxNodes) throw new QueryValidationError("Query has too many expression nodes");
  };

  const compileFilter = (input: Filter, depth: number): QueryExpr => {
    if (depth > limits.maxDepth) throw new QueryValidationError("Query nesting is too deep");
    if (!plainObject(input)) throw new QueryValidationError("Filter must be an object");
    const args: QueryExpr[] = [];
    for (const [key, raw] of Object.entries(input)) {
      if (raw === undefined) throw new QueryValidationError(`Undefined filter value at ${key}`);
      if (key === "$and" || key === "$or") {
        if (!Array.isArray(raw) || raw.length === 0 || raw.length > limits.maxArrayItems) {
          throw new QueryValidationError(`${key} expects a non-empty bounded array`);
        }
        addNode();
        args.push({
          op: key === "$and" ? "and" : "or",
          args: raw.map((part) => compileFilter(part as Filter, depth + 1)),
        });
      } else if (key === "$not") {
        addNode();
        args.push({ op: "not", arg: compileFilter(raw as Filter, depth + 1) });
      } else if (key.startsWith("$")) {
        throw new QueryValidationError(`Unknown logical operator: ${key}`);
      } else {
        assertPath(key);
        args.push(...compileField(key, raw as Value | Comparison, depth + 1));
      }
    }
    if (args.length === 0) {
      addNode();
      return { op: "true" };
    }
    if (args.length === 1) return args[0] as QueryExpr;
    addNode();
    return { op: "and", args };
  };

  const checkedValue = (path: string, raw: unknown): Value => {
    if (path === "_id") assertDocumentID(raw, "_id query value");
    const value = cloneValue(raw as Value);
    assertQueryValue(value, limits);
    return value;
  };

  const compileElement = (path: string, raw: unknown, depth: number): import("./types.js").ElementQueryExpr => {
    if (depth > limits.maxDepth || !plainObject(raw) || Object.keys(raw).length === 0)
      throw new QueryValidationError("scalar $elemMatch expects a non-empty object");
    const args: import("./types.js").ElementQueryExpr[] = [];
    for (const [operator, operand] of Object.entries(raw)) {
      if (operand === undefined) throw new QueryValidationError(`Missing operand for ${operator}`);
      addNode();
      if (operator === "$and" || operator === "$or") {
        if (!Array.isArray(operand) || operand.length === 0 || operand.length > limits.maxArrayItems)
          throw new QueryValidationError(`${operator} expects a non-empty bounded condition array`);
        args.push({ op: operator.slice(1) as "and" | "or", args: operand.map((item) => compileElement(path, item, depth + 1)) });
      } else if (operator === "$not") {
        args.push({ op: "not", arg: compileElement(path, operand, depth + 1) });
      } else if (operator === "$in" || operator === "$nin") {
        if (!Array.isArray(operand) || operand.length > limits.maxArrayItems)
          throw new QueryValidationError(`${operator} expects a bounded array`);
        args.push({ op: operator.slice(1) as "in" | "nin", values: operand.map((item) => checkedValue(path, item)) });
      } else if (operator === "$eq" || operator === "$ne" || operator === "$gt" || operator === "$gte" || operator === "$lt" || operator === "$lte") {
        args.push({ op: "compare", cmp: operator.slice(1) as "eq" | "ne" | "gt" | "gte" | "lt" | "lte", value: checkedValue(path, operand) });
      } else {
        throw new QueryValidationError(`Unsupported scalar $elemMatch operator: ${operator}`);
      }
    }
    return args.length === 1 ? args[0] as import("./types.js").ElementQueryExpr : { op: "and", args };
  };

  const compileField = (path: string, raw: Value | Comparison, depth: number): QueryExpr[] => {
    if (depth > limits.maxDepth) throw new QueryValidationError("Query nesting is too deep");
    const isOperators = plainObject(raw) && Object.keys(raw).some((key) => key.startsWith("$"));
    if (!isOperators) {
      addNode();
      return [{ op: "compare", cmp: "eq", path, value: checkedValue(path, raw) }];
    }
    const result: QueryExpr[] = [];
    for (const [operator, operand] of Object.entries(raw as Comparison)) {
      if (!fieldOperators.has(operator)) throw new QueryValidationError(`Unknown field operator: ${operator}`);
      if (operand === undefined) throw new QueryValidationError(`Missing operand for ${operator}`);
      addNode();
      if (operator === "$exists") {
        if (typeof operand !== "boolean") throw new QueryValidationError("$exists expects a boolean");
        result.push({ op: "exists", path, value: operand });
      } else if (operator === "$size") {
        if (typeof operand !== "number" || !Number.isSafeInteger(operand) || operand < 0)
          throw new QueryValidationError("$size expects a non-negative safe integer");
        result.push({ op: "size", path, size: operand });
      } else if (operator === "$type") {
        result.push({ op: "type", path, types: normalizeQueryTypes(operand, limits.maxArrayItems) });
      } else if (operator === "$all") {
        if (!Array.isArray(operand) || operand.length === 0 || operand.length > limits.maxArrayItems)
          throw new QueryValidationError("$all expects a non-empty bounded array");
        result.push({ op: "all", path, values: dedupeValues(operand.map((item) => checkedValue(path, item))) });
      } else if (operator === "$elemMatch") {
        if (!plainObject(operand) || Object.keys(operand).length === 0)
          throw new QueryValidationError("$elemMatch expects a non-empty object");
        const scalar = Object.keys(operand).some((key) => key.startsWith("$"));
        if (scalar) {
          if (Object.keys(operand).some((key) => !key.startsWith("$")))
            throw new QueryValidationError("scalar $elemMatch cannot mix field keys and operators");
          result.push({ op: "elem_match", path, mode: "scalar", arg: compileElement(path, operand, depth + 1) });
        } else {
          result.push({ op: "elem_match", path, mode: "object", arg: compileFilter(operand as Filter, depth + 1) });
        }
      } else if (operator === "$in" || operator === "$nin") {
        if (!Array.isArray(operand) || operand.length > limits.maxArrayItems)
          throw new QueryValidationError(`${operator} expects a bounded array`);
        result.push({
          op: operator.slice(1) as "in" | "nin",
          path,
          values: operand.map((item) => checkedValue(path, item)),
        });
      } else if (operator === "$not") {
        const nested = compileField(path, operand as Value | Comparison, depth + 1);
        if (nested.length > 1) addNode();
        const expression: QueryExpr = nested.length === 1 ? (nested[0] as QueryExpr) : { op: "and", args: nested };
        result.push({ op: "not", arg: expression });
      } else {
        result.push({
          op: "compare",
          cmp: operator.slice(1) as "eq" | "ne" | "gt" | "gte" | "lt" | "lte",
          path,
          value: checkedValue(path, operand),
        });
      }
    }
    if (result.length === 0) throw new QueryValidationError("Operator object cannot be empty");
    return result;
  };

  let sort = options.sort?.map((field) => {
    assertPath(field.path);
    if (field.direction !== 1 && field.direction !== -1)
      throw new QueryValidationError("Sort direction must be 1 or -1");
    return { ...field };
  });
  if (sort && sort.length > limits.maxSortFields) throw new QueryValidationError("Too many sort fields");
  if (sort && new Set(sort.map((field) => field.path)).size !== sort.length)
    throw new QueryValidationError("Sort paths must be unique");
  if (options.after !== undefined || options.first !== undefined) {
    if (!sort) throw new QueryValidationError("Seek pagination requires sort");
    sort = [...normalizePageSort(sort)];
    if (sort.length > limits.maxSortFields)
      throw new QueryValidationError("Seek pagination needs room for the _id tie-breaker");
  }
  const skip = options.skip;
  const limit = options.first ?? options.limit;
  if (skip !== undefined && (!Number.isSafeInteger(skip) || skip < 0))
    throw new QueryValidationError("skip must be a non-negative integer");
  if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 0 || limit > limits.maxLimit))
    throw new QueryValidationError("limit is outside the allowed range");
  const scopedFilter =
    options.after === undefined
      ? filter
      : { $and: [filter, pageFilterAfter(options.after, sort as readonly SortField[])] };
  const spec = {
    version: 1 as const,
    where: compileFilter(scopedFilter, 0),
    ...(sort ? { sort } : {}),
    ...(skip !== undefined ? { skip } : {}),
    ...(limit !== undefined ? { limit } : {}),
    ...(options.first !== undefined ? { seek: true as const } : {}),
  };
  if (wireQueryByteLength(encodeQuerySpec(spec)) > limits.maxWireBytes)
    throw new QueryValidationError("Query exceeds wire size limit");
  return spec;
}

export function matches(document: import("./types.js").Document, expression: QueryExpr): boolean {
  switch (expression.op) {
    case "true":
      return true;
    case "and":
      return expression.args.every((arg) => matches(document, arg));
    case "or":
      return expression.args.some((arg) => matches(document, arg));
    case "not":
      return !matches(document, expression.arg);
    case "exists":
      return getPath(document, expression.path).found === expression.value;
    case "size": {
      const found = getPath(document, expression.path);
      return found.found && Array.isArray(found.value) && found.value.length === expression.size;
    }
    case "type": {
      const found = getPath(document, expression.path);
      return (
        found.found &&
        expression.types.includes(queryTypeNameOf(found.value as Value, expression.path))
      );
    }
    case "all": {
      const found = getPath(document, expression.path);
      return found.found && Array.isArray(found.value) && expression.values.every((candidate) =>
        (found.value as readonly Value[]).some((item) => valueEquals(item, candidate)),
      );
    }
    case "elem_match": {
      const found = getPath(document, expression.path);
      if (!found.found || !Array.isArray(found.value)) return false;
      return (found.value as readonly Value[]).some((item) =>
        expression.mode === "scalar"
          ? matchesElement(item, expression.arg)
          : plainObject(item) && matches(item as import("./types.js").Document, expression.arg),
      );
    }
    case "in":
    case "nin": {
      const found = getPath(document, expression.path);
      const hit = found.found && expression.values.some((candidate) => fieldEquals(found.value as Value, candidate));
      return expression.op === "in" ? hit : !hit;
    }
    case "compare": {
      const found = getPath(document, expression.path);
      if (!found.found) return expression.cmp === "ne";
      const value = found.value as Value;
      if (expression.cmp === "eq") return fieldEquals(value, expression.value);
      if (expression.cmp === "ne") return !fieldEquals(value, expression.value);
      const order = compareValues(value, expression.value);
      if (order === undefined) return false;
      if (expression.cmp === "gt") return order > 0;
      if (expression.cmp === "gte") return order >= 0;
      if (expression.cmp === "lt") return order < 0;
      return order <= 0;
    }
  }
}

export function executeQuery<T extends import("./types.js").Document>(documents: Iterable<T>, spec: QuerySpec): T[] {
  let result = [...documents].filter((document) => matches(document, spec.where));
  if (spec.sort?.length) {
    result = result
      .map((document, position) => ({ document, position }))
      .sort((a, b) => {
        for (const field of spec.sort ?? []) {
          const av = getPath(a.document, field.path);
          const bv = getPath(b.document, field.path);
          if (!av.found || !bv.found) {
            if (av.found !== bv.found) return (av.found ? 1 : -1) * field.direction;
            continue;
          }
          const order = compareSortValues(av.value as Value, bv.value as Value);
          if (order !== 0) return order * field.direction;
        }
        return a.position - b.position;
      })
      .map(({ document }) => document);
  }
  return result.slice(spec.skip ?? 0, spec.limit === undefined ? undefined : (spec.skip ?? 0) + spec.limit);
}

function fieldEquals(field: Value, candidate: Value): boolean {
  if (Array.isArray(field) && !Array.isArray(candidate)) return field.some((item) => valueEquals(item, candidate));
  return valueEquals(field, candidate);
}

function dedupeValues(values: readonly Value[]): Value[] {
  const result: Value[] = [];
  for (const value of values)
    if (!result.some((candidate) => valueEquals(candidate, value))) result.push(value);
  return result;
}

function matchesElement(value: Value, expression: import("./types.js").ElementQueryExpr): boolean {
  switch (expression.op) {
    case "and": return expression.args.every((arg) => matchesElement(value, arg));
    case "or": return expression.args.some((arg) => matchesElement(value, arg));
    case "not": return !matchesElement(value, expression.arg);
    case "in": return expression.values.some((candidate) => fieldEquals(value, candidate));
    case "nin": return !expression.values.some((candidate) => fieldEquals(value, candidate));
    case "compare": {
      if (expression.cmp === "eq") return fieldEquals(value, expression.value);
      if (expression.cmp === "ne") return !fieldEquals(value, expression.value);
      const order = compareValues(value, expression.value);
      if (order === undefined) return false;
      if (expression.cmp === "gt") return order > 0;
      if (expression.cmp === "gte") return order >= 0;
      if (expression.cmp === "lt") return order < 0;
      return order <= 0;
    }
  }
}

function plainObject(value: unknown): value is Record<string, unknown> {
  if (
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    value instanceof Date ||
    value instanceof Uint8Array
  )
    return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
