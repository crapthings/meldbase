/**
 * Shared public types for @meldbase/client.
 *
 * @beta These types are versioned with the client SDK and can change before a
 * non-zero major release.
 */
import type { DocumentID } from "./safe-value.js";

// bigint is the lossless JavaScript representation of Meldbase Int64. number
// maps to Float64; the wire format never silently rounds an integer.
//
// DocumentID is distinct from string so a generic ID-valued field retains its
// `id` wire tag. Persisted document `_id` deliberately remains a string at the
// ergonomic document boundary.
/** Represents a scalar value supported by the client and wire format. */
export type Primitive = null | boolean | number | bigint | string | Date | Uint8Array | DocumentID;
/** Represents a supported scalar, array, or nested object value. */
export type Value = Primitive | readonly Value[] | { readonly [key: string]: Value };
/** Represents stored document fields with a required string ID. */
export type Document = { readonly _id: string; readonly [key: string]: Value };
/** Represents document input whose ID may be assigned by the SDK. */
export type InputDocument = { readonly _id?: string; readonly [key: string]: Value | undefined };

/** Names the value types recognized by type predicates. */
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
/** Checks that an insert matches the collection schema after an ID is assigned. */
export type InsertDocument<T extends Document, U extends InputDocument> = U &
  (U & { readonly _id: T["_id"] } extends T ? unknown : never);

/** Declares field comparisons and array, type, and existence predicates. */
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

/** Declares conditions that must match a single array element. */
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

/** Combines field conditions with logical query operators. */
export type Filter = {
  readonly [field: string]: Value | Comparison | readonly Filter[] | Filter | undefined;
  readonly $and?: readonly Filter[];
  readonly $or?: readonly Filter[];
  readonly $not?: Filter;
};

/** Declares field assignments, removals, increments, and array changes. */
export interface Update {
  readonly $set?: Readonly<Record<string, Value>>;
  readonly $unset?: readonly string[] | Readonly<Record<string, true>>;
  readonly $inc?: Readonly<Record<string, number | bigint>>;
  readonly $push?: Readonly<Record<string, Value>>;
  readonly $pull?: Readonly<Record<string, Value>>;
}

/** Represents one compiled field update or removal. */
export type MutationOperation =
  | { readonly op: "set" | "inc" | "push" | "pull"; readonly path: string; readonly value: Value }
  | { readonly op: "unset"; readonly path: string };

/** Groups ordered mutation operations under a format version. */
export interface MutationSpec {
  readonly version: 1;
  readonly operations: readonly MutationOperation[];
}

/** Reports how many documents matched an update and how many changed. */
export interface MutationResult {
  readonly matchedCount: number;
  readonly modifiedCount: number;
}

/** Reports how many documents were deleted. */
export interface DeleteResult {
  readonly deletedCount: number;
}

/** Reports a document count and whether a policy cap limits its precision. */
export interface CountResult {
  readonly count: number;
  // True means count is a policy-capped lower bound, not an exact total.
  readonly capped: boolean;
}

/** Reports counts grouped by value and whether the result is capped. */
export interface GroupCountResult {
  readonly groups: readonly { readonly key: Value; readonly count: number }[];
  readonly capped: boolean;
}

/** Names the comparison operations used by compiled predicates. */
export type CompareOperator = "eq" | "ne" | "gt" | "gte" | "lt" | "lte";

// QueryExpr is the transport contract shared by local execution and the server.
// It is data-only by design: no callbacks, source strings, or executable values.
/** Represents a query condition as data shared by local execution and the server. */
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

/** Represents a compiled predicate applied to one array element. */
export type ElementQueryExpr =
  | { readonly op: "and" | "or"; readonly args: readonly ElementQueryExpr[] }
  | { readonly op: "not"; readonly arg: ElementQueryExpr }
  | { readonly op: "compare"; readonly cmp: CompareOperator; readonly value: Value }
  | { readonly op: "in" | "nin"; readonly values: readonly Value[] };

/** Selects a field path and ascending or descending sort direction. */
export interface SortField {
  readonly path: string;
  readonly direction: 1 | -1;
}

/** Combines a compiled predicate with ordering and pagination settings. */
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

/** Bounds query encoding, nesting, operands, sorting, and result size. */
export interface QueryLimits {
  readonly maxWireBytes: number;
  readonly maxDepth: number;
  readonly maxNodes: number;
  readonly maxArrayItems: number;
  readonly maxStringBytes: number;
  readonly maxSortFields: number;
  readonly maxLimit: number;
}

/** Provides the immutable default bounds for query validation. */
export const DEFAULT_QUERY_LIMITS: QueryLimits = Object.freeze({
  maxWireBytes: 1_048_576,
  maxDepth: 16,
  maxNodes: 128,
  maxArrayItems: 256,
  maxStringBytes: 16_384,
  maxSortFields: 4,
  maxLimit: 10_000,
});

/** Reports invalid query or value input. */
export class QueryValidationError extends Error {
  override readonly name = "QueryValidationError";
}
