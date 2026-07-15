import assert from "node:assert/strict";
import test from "node:test";
import { compileQuery, encodeQuerySpec, executeQuery, QueryValidationError } from "./index.js";
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

test("rejects executable, dangerous, unknown, and expensive filters", () => {
  assert.throws(() => compileQuery({ x: (() => true) as never }), QueryValidationError);
  assert.throws(() => compileQuery({ "__proto__.polluted": true }), QueryValidationError);
  assert.throws(() => compileQuery({ x: { $where: "evil" } as never }), /Unknown field operator/);
  assert.throws(() => compileQuery({ $or: new Array(300).fill({ x: 1 }) }), /bounded array/);
});
