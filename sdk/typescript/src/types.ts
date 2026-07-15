// bigint is the lossless JavaScript representation of Meldbase Int64. number
// maps to Float64; the wire format never silently rounds an integer.
export type Primitive = null | boolean | number | bigint | string | Date | Uint8Array;
export type Value = Primitive | readonly Value[] | { readonly [key: string]: Value };
export type Document = { readonly _id: string; readonly [key: string]: Value };
export type InputDocument = { readonly _id?: string; readonly [key: string]: Value | undefined };

export type Comparison = {
  readonly $eq?: Value;
  readonly $ne?: Value;
  readonly $gt?: Value;
  readonly $gte?: Value;
  readonly $lt?: Value;
  readonly $lte?: Value;
  readonly $in?: readonly Value[];
  readonly $nin?: readonly Value[];
  readonly $exists?: boolean;
  readonly $not?: Value | Comparison;
};

export type Filter = {
  readonly [field: string]: Value | Comparison | readonly Filter[] | Filter | undefined;
  readonly $and?: readonly Filter[];
  readonly $or?: readonly Filter[];
  readonly $not?: Filter;
};

export interface Update {
  readonly $set?: Readonly<Record<string, Value>>;
  readonly $unset?: readonly string[] | Readonly<Record<string, true>>;
  readonly $inc?: Readonly<Record<string, number | bigint>>;
  readonly $push?: Readonly<Record<string, Value>>;
  readonly $pull?: Readonly<Record<string, Value>>;
}

export type MutationOperation =
  | { readonly op: "set" | "inc" | "push" | "pull"; readonly path: string; readonly value: Value }
  | { readonly op: "unset"; readonly path: string };

export interface MutationSpec {
  readonly version: 1;
  readonly operations: readonly MutationOperation[];
}

export interface MutationResult {
  readonly matchedCount: number;
  readonly modifiedCount: number;
}

export interface DeleteResult {
  readonly deletedCount: number;
}

export type CompareOperator = "eq" | "ne" | "gt" | "gte" | "lt" | "lte";

// QueryExpr is the transport contract shared by local execution and the server.
// It is data-only by design: no callbacks, source strings, or executable values.
export type QueryExpr =
  | { readonly op: "true" }
  | { readonly op: "and" | "or"; readonly args: readonly QueryExpr[] }
  | { readonly op: "not"; readonly arg: QueryExpr }
  | { readonly op: "compare"; readonly cmp: CompareOperator; readonly path: string; readonly value: Value }
  | { readonly op: "in" | "nin"; readonly path: string; readonly values: readonly Value[] }
  | { readonly op: "exists"; readonly path: string; readonly value: boolean };

export interface SortField {
  readonly path: string;
  readonly direction: 1 | -1;
}

export interface QuerySpec {
  readonly version: 1;
  readonly where: QueryExpr;
  readonly sort?: readonly SortField[];
  readonly skip?: number;
  readonly limit?: number;
}

export interface QueryLimits {
  readonly maxDepth: number;
  readonly maxNodes: number;
  readonly maxArrayItems: number;
  readonly maxStringBytes: number;
  readonly maxSortFields: number;
  readonly maxLimit: number;
}

export const DEFAULT_QUERY_LIMITS: QueryLimits = Object.freeze({
  maxDepth: 16,
  maxNodes: 128,
  maxArrayItems: 256,
  maxStringBytes: 16_384,
  maxSortFields: 4,
  maxLimit: 10_000,
});

export class QueryValidationError extends Error {
  override readonly name = "QueryValidationError";
}
