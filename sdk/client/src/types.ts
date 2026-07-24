import type { DocumentID } from "./safe-value.js";

// bigint is the lossless JavaScript representation of Meldbase Int64. number
// maps to Float64; the wire format never silently rounds an integer.
//
// DocumentID is distinct from string so a generic ID-valued field retains its
// `id` wire tag. Persisted document `_id` deliberately remains a string at the
// ergonomic document boundary.
export type Primitive = null | boolean | number | bigint | string | Date | Uint8Array | DocumentID;
export type Value = Primitive | readonly Value[] | { readonly [key: string]: Value };
export type Document = { readonly _id: string; readonly [key: string]: Value };
export type InputDocument = { readonly _id?: string; readonly [key: string]: Value | undefined };

export type QueryTypeName =
  | "null"
  | "boolean"
  | "int64"
  | "float64"
  | "string"
  | "date"
  | "id"
  | "binary"
  | "array"
  | "object";

// InsertDocument carries the schema of a typed collection through an insert
// while allowing the SDK to allocate `_id`. `U` is inferred from the supplied
// value; adding an ID must make it assignable to T. This avoids losing T's
// required named fields to Document's string index signature.
export type InsertDocument<T extends Document, U extends InputDocument> = U &
  (U & { readonly _id: T["_id"] } extends T ? unknown : never);

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
  readonly $size?: number;
  readonly $type?: QueryTypeName | readonly QueryTypeName[];
  readonly $all?: readonly Value[];
  readonly $elemMatch?: ElemMatchScalar | Filter;
  readonly $not?: Value | Comparison;
};

export type ElemMatchScalar = {
  readonly $eq?: Value;
  readonly $ne?: Value;
  readonly $gt?: Value;
  readonly $gte?: Value;
  readonly $lt?: Value;
  readonly $lte?: Value;
  readonly $in?: readonly Value[];
  readonly $nin?: readonly Value[];
  readonly $and?: readonly ElemMatchScalar[];
  readonly $or?: readonly ElemMatchScalar[];
  readonly $not?: ElemMatchScalar;
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

export interface CountResult {
  readonly count: number;
  // True means count is a policy-capped lower bound, not an exact total.
  readonly capped: boolean;
}

export interface GroupCountResult {
  readonly groups: readonly { readonly key: Value; readonly count: number }[];
  readonly capped: boolean;
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
  | { readonly op: "exists"; readonly path: string; readonly value: boolean }
  | { readonly op: "size"; readonly path: string; readonly size: number }
  | { readonly op: "type"; readonly path: string; readonly types: readonly QueryTypeName[] }
  | { readonly op: "all"; readonly path: string; readonly values: readonly Value[] }
  | { readonly op: "elem_match"; readonly path: string; readonly mode: "scalar"; readonly arg: ElementQueryExpr }
  | { readonly op: "elem_match"; readonly path: string; readonly mode: "object"; readonly arg: QueryExpr };

export type ElementQueryExpr =
  | { readonly op: "and" | "or"; readonly args: readonly ElementQueryExpr[] }
  | { readonly op: "not"; readonly arg: ElementQueryExpr }
  | { readonly op: "compare"; readonly cmp: CompareOperator; readonly value: Value }
  | { readonly op: "in" | "nin"; readonly values: readonly Value[] };

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
  // An internal transport marker used to make server-side result projection
  // safe for SDK-managed seek cursors.
  readonly seek?: true;
}

export interface QueryLimits {
  readonly maxWireBytes: number;
  readonly maxDepth: number;
  readonly maxNodes: number;
  readonly maxArrayItems: number;
  readonly maxStringBytes: number;
  readonly maxSortFields: number;
  readonly maxLimit: number;
}

export const DEFAULT_QUERY_LIMITS: QueryLimits = Object.freeze({
  maxWireBytes: 1_048_576,
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
