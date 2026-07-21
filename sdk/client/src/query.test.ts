import assert from "node:assert/strict";
import test from "node:test";
import { compileQuery, encodeQuerySpec, executeQuery, pageCursorFor, QueryValidationError } from "./index.js";
import type { Document } from "./index.js";

const documents: Document[] = [
  { _id: "a", age: 17, profile: { city: "Shanghai" }, tags: ["new"] },
  { _id: "b", age: 24, profile: { city: "Hangzhou" }, tags: ["active", "new"] },
  { _id: "c", age: 30, profile: { city: "Shanghai" }, tags: [] },
];

test("compiles and executes the same data-only query AST", () => {
  const query = compileQuery({
    age: { $gte: 18 },
    $or: [{ "profile.city": "Shanghai" }, { tags: "active" }],
  }, { sort: [{ path: "age", direction: -1 }], limit: 2 });
  assert.deepEqual(executeQuery(documents, query).map((item) => item._id), ["c", "b"]);
  assert.equal(JSON.parse(JSON.stringify(query)).version, 1);
});

test("distinguishes missing from null and defines direct array membership", () => {
  const values: Document[] = [{ _id: "a", x: null }, { _id: "b" }, { _id: "c", x: [1, 2] }];
  assert.deepEqual(executeQuery(values, compileQuery({ x: { $exists: false } })).map((x) => x._id), ["b"]);
  assert.deepEqual(executeQuery(values, compileQuery({ x: 2 })).map((x) => x._id), ["c"]);
});

test("compares Int64 bigint and Float64 number without rounding Int64", () => {
  const values: Document[] = [{ _id: "a", n: 9_007_199_254_740_993n }, { _id: "b", n: 10 }];
  assert.deepEqual(executeQuery(values, compileQuery({ n: { $gt: 9_007_199_254_740_992 } })).map((x) => x._id), ["a"]);
  assert.deepEqual(executeQuery(values, compileQuery({ n: 10n })).map((x) => x._id), ["b"]);
});

test("encodes persisted _id query values with their distinct wire type", () => {
  const id = "00000000000000000000000000000001";
  const wire = encodeQuerySpec(compileQuery({ _id: id }));
  assert.deepEqual(wire.where, { op: "compare", cmp: "eq", path: "_id", value: { t: "id", v: id } });
});

test("seek pagination uses a stable _id tie-breaker without skip", () => {
  const values: Document[] = [
    { _id: "00000000000000000000000000000002", rank: 1 },
    { _id: "00000000000000000000000000000001", rank: 1 },
    { _id: "00000000000000000000000000000003", rank: 2 },
    { _id: "00000000000000000000000000000004", rank: 3 },
  ];
  const sort = [{ path: "rank", direction: 1 }] as const;
  const first = executeQuery(values, compileQuery({}, { sort, first: 2 }));
  const cursor = pageCursorFor(first.at(-1)!, sort);
  const second = executeQuery(values, compileQuery({}, { sort, first: 2, after: cursor }));
  assert.deepEqual(first.map((item) => item._id), ["00000000000000000000000000000001", "00000000000000000000000000000002"]);
  assert.deepEqual(second.map((item) => item._id), ["00000000000000000000000000000003", "00000000000000000000000000000004"]);
});

test("seek pagination rejects a cursor used with a different sort", () => {
  const document: Document = { _id: "00000000000000000000000000000001", rank: 1 };
  const cursor = pageCursorFor(document, [{ path: "rank", direction: 1 }]);
  assert.throws(() => compileQuery({}, { sort: [{ path: "rank", direction: -1 }], first: 10, after: cursor }), QueryValidationError);
});

test("rejects executable, dangerous, unknown, and expensive filters", () => {
  assert.throws(() => compileQuery({ x: (() => true) as never }), QueryValidationError);
  assert.throws(() => compileQuery({ "__proto__.polluted": true }), QueryValidationError);
  assert.throws(() => compileQuery({ x: { $where: "evil" } as never }), /Unknown field operator/);
  assert.throws(() => compileQuery({ $or: new Array(300).fill({ x: 1 }) }), /bounded array/);
});
