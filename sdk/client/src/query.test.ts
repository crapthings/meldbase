import assert from "node:assert/strict";
import test from "node:test";
import { compileQuery, decodeQuerySpec, encodeQuerySpec, executeQuery, pageCursorFor, QueryValidationError } from "./index.js";
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

test("uses UTF-8 scalar ordering for ranges and a transitive mixed-value sort", () => {
  const unicode: Document[] = [
    { _id: "non-bmp", x: "\u{10000}" },
    { _id: "bmp", x: "\uE000" },
  ];
  assert.deepEqual(executeQuery(unicode, compileQuery({ x: { $gt: "\uE000" } })).map((item) => item._id), ["non-bmp"]);

  const values: Document[] = [
    { _id: "string-z", value: "z" },
    { _id: "number", value: 0 },
    { _id: "string-a", value: "a" },
    { _id: "null", value: null },
    { _id: "boolean", value: true },
    { _id: "time", value: new Date(0) },
    { _id: "binary", value: new Uint8Array([1]) },
    { _id: "array", value: [] },
    { _id: "object", value: {} },
  ];
  assert.deepEqual(
    executeQuery(values, compileQuery({}, { sort: [{ path: "value", direction: 1 }] })).map((item) => item._id),
    ["null", "boolean", "number", "string-a", "string-z", "time", "binary", "array", "object"],
  );
});

test("encodes persisted _id query values with their distinct wire type", () => {
  const id = "00000000000000000000000000000001";
  const wire = encodeQuerySpec(compileQuery({ _id: id }));
  assert.deepEqual(wire.where, { op: "compare", cmp: "eq", path: "_id", value: { t: "id", v: id } });
});

test("requires canonical non-zero document IDs in every query compiler path", () => {
  assert.throws(() => compileQuery({ _id: "00000000000000000000000000000000" }), QueryValidationError);
  assert.throws(() => compileQuery({ _id: { $in: ["0000000000000000000000000000000A"] } }), QueryValidationError);
});

test("marks SDK-managed seek queries on the wire", () => {
  const wire = encodeQuerySpec(compileQuery({}, { sort: [{ path: "rank", direction: 1 }], first: 2 }));
  assert.equal(wire.seek, true);
  assert.throws(() => decodeQuerySpec({ version: 1, where: { op: "true" }, seek: true }), QueryValidationError);
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

test("seek pagination traverses missing sort values without gaps", () => {
  const values: Document[] = [
    { _id: "00000000000000000000000000000003" },
    { _id: "00000000000000000000000000000002", rank: 1 },
    { _id: "00000000000000000000000000000001" },
    { _id: "00000000000000000000000000000004", rank: 1 },
    { _id: "00000000000000000000000000000005", rank: 2 },
  ];
  for (const direction of [1, -1] as const) {
    const sort = [{ path: "rank", direction }] as const;
    const ids: string[] = [];
    let after: string | undefined;
    do {
      const page = executeQuery(values, compileQuery({}, { sort, first: 2, ...(after ? { after } : {}) }));
      ids.push(...page.map((item) => item._id));
      after = page.length === 0 ? undefined : pageCursorFor(page.at(-1)!, sort);
    } while (after !== undefined && ids.length < values.length);
    assert.deepEqual(ids, direction === 1
      ? ["00000000000000000000000000000001", "00000000000000000000000000000003", "00000000000000000000000000000002", "00000000000000000000000000000004", "00000000000000000000000000000005"]
      : ["00000000000000000000000000000005", "00000000000000000000000000000002", "00000000000000000000000000000004", "00000000000000000000000000000001", "00000000000000000000000000000003"],
    );
  }
});

test("seek pagination refuses array and object sort cursor values", () => {
  const document: Document = { _id: "00000000000000000000000000000001", rank: [1] };
  assert.throws(() => pageCursorFor(document, [{ path: "rank", direction: 1 }]), QueryValidationError);
});

test("seek pagination rejects a cursor used with a different sort", () => {
  const document: Document = { _id: "00000000000000000000000000000001", rank: 1 };
  const cursor = pageCursorFor(document, [{ path: "rank", direction: 1 }]);
  assert.throws(() => compileQuery({}, { sort: [{ path: "rank", direction: -1 }], first: 10, after: cursor }), QueryValidationError);
});

test("seek cursors require first", () => {
  const document: Document = { _id: "00000000000000000000000000000001", rank: 1 };
  const cursor = pageCursorFor(document, [{ path: "rank", direction: 1 }]);
  assert.throws(() => compileQuery({}, { sort: [{ path: "rank", direction: 1 }], after: cursor }), /after requires first/);
});

test("rejects duplicate sort paths in compiled and wire queries", () => {
  assert.throws(() => compileQuery({}, { sort: [{ path: "rank", direction: 1 }, { path: "rank", direction: -1 }] }), QueryValidationError);
  assert.throws(() => decodeQuerySpec({
    version: 1,
    where: { op: "true" },
    sort: [{ path: "rank", direction: 1 }, { path: "rank", direction: -1 }],
  }), QueryValidationError);
});

test("rejects executable, dangerous, unknown, and expensive filters", () => {
  assert.throws(() => compileQuery({ x: (() => true) as never }), QueryValidationError);
  assert.throws(() => compileQuery({ "__proto__.polluted": true }), QueryValidationError);
  assert.throws(() => compileQuery({ x: { $where: "evil" } as never }), /Unknown field operator/);
  assert.throws(() => compileQuery({ $or: new Array(300).fill({ x: 1 }) }), /bounded array/);
});

test("uses the same nested value limits for compiled and wire queries", () => {
  const limits = { maxArrayItems: 2, maxDepth: 2, maxStringBytes: 4 };
  assert.throws(() => compileQuery({ value: "abcde" }, { limits }), QueryValidationError);
  assert.throws(() => compileQuery({ value: [null, null, null] }, { limits }), QueryValidationError);
  assert.throws(() => compileQuery({ value: [[[null]]] }, { limits }), QueryValidationError);
  assert.throws(() => decodeQuerySpec({
    version: 1,
    where: { op: "compare", cmp: "eq", path: "value", value: { t: "array", v: [{ t: "null" }, { t: "null" }, { t: "null" }] } },
  }, limits), QueryValidationError);
});

test("enforces the final query wire-size limit", () => {
  assert.throws(() => compileQuery({ title: "x".repeat(100) }, { limits: { maxWireBytes: 100 } }), QueryValidationError);
  assert.throws(() => decodeQuerySpec({ version: 1, where: { op: "true" }, padding: "x".repeat(100) }, { maxWireBytes: 100 }), QueryValidationError);
});
