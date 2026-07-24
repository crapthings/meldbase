import assert from "node:assert/strict";
import test from "node:test";
import { pageCursorFor, pageFilterAfter } from "./cursor.js";
import { compileQuery, executeQuery } from "./query.js";
import { documentID, isDocumentIDValue } from "./safe-value.js";
import { QueryValidationError } from "./types.js";
import { decodeQuerySpec, encodeQuerySpec } from "./wire.js";
import type { Document } from "./index.js";

const documents: Document[] = [
  { _id: "a", age: 17, profile: { city: "Shanghai" }, tags: ["new"] },
  { _id: "b", age: 24, profile: { city: "Hangzhou" }, tags: ["active", "new"] },
  { _id: "c", age: 30, profile: { city: "Shanghai" }, tags: [] },
];

test("compiles and executes the same data-only query AST", () => {
  const query = compileQuery(
    {
      age: { $gte: 18 },
      $or: [{ "profile.city": "Shanghai" }, { tags: "active" }],
    },
    { sort: [{ path: "age", direction: -1 }], limit: 2 },
  );
  assert.deepEqual(
    executeQuery(documents, query).map((item) => item._id),
    ["c", "b"],
  );
  assert.equal(JSON.parse(JSON.stringify(query)).version, 1);
});

test("distinguishes missing from null and defines direct array membership", () => {
  const values: Document[] = [{ _id: "a", x: null }, { _id: "b" }, { _id: "c", x: [1, 2] }];
  assert.deepEqual(
    executeQuery(values, compileQuery({ x: { $exists: false } })).map((x) => x._id),
    ["b"],
  );
  assert.deepEqual(
    executeQuery(values, compileQuery({ x: 2 })).map((x) => x._id),
    ["c"],
  );
});

test("$size and $type use exact bounded residual semantics", () => {
  const values: Document[] = [
    { _id: "empty", items: [], value: null },
    { _id: "two-int64", items: [1, 2], value: 1n },
    { _id: "two-float64", items: [1, 2], value: 1 },
    { _id: "object", items: {}, value: {} },
    { _id: "missing" },
  ];
  assert.deepEqual(
    executeQuery(values, compileQuery({ items: { $size: 0 } })).map((item) => item._id),
    ["empty"],
  );
  assert.deepEqual(
    executeQuery(values, compileQuery({ value: { $type: ["array", "int64"] } })).map((item) => item._id),
    ["two-int64"],
  );
  assert.deepEqual(
    executeQuery(values, compileQuery({ value: { $not: { $type: "float64" } } })).map((item) => item._id),
    ["empty", "two-int64", "object", "missing"],
  );

  const query = compileQuery({ value: { $type: ["object", "int64", "int64"] } });
  assert.deepEqual(query.where, { op: "type", path: "value", types: ["int64", "object"] });
  assert.deepEqual(encodeQuerySpec(query).where, {
    op: "type",
    path: "value",
    types: ["int64", "object"],
  });
  assert.deepEqual(
    decodeQuerySpec({
      version: 1,
      where: { op: "type", path: "value", types: ["object", "int64", "int64"] },
    }).where,
    { op: "type", path: "value", types: ["int64", "object"] },
  );

  const persistedID = "00000000000000000000000000000001";
  assert.equal(executeQuery([{ _id: persistedID }], compileQuery({ _id: { $type: "id" } })).length, 1);

  for (const filter of [
    { items: { $size: -1 } },
    { items: { $size: 1.5 } },
    { items: { $size: Number.MAX_SAFE_INTEGER + 1 } },
    { value: { $type: [] } },
    { value: { $type: "number" } },
    { value: { $type: ["int64", 1] } },
  ]) {
    assert.throws(() => compileQuery(filter as never), QueryValidationError);
  }
  for (const where of [
    { op: "size", path: "items", size: 1.5 },
    { op: "size", path: "items", size: -1 },
    { op: "type", path: "value", types: [] },
    { op: "type", path: "value", types: ["number"] },
    { op: "type", path: "value", types: "int64" },
  ]) {
    assert.throws(() => decodeQuerySpec({ version: 1, where }), QueryValidationError);
  }
});

test("$all requires every deduplicated value in one array", () => {
  const values: Document[] = [
    { _id: "all", tags: ["one", "two", "three"] },
    { _id: "pair", tags: ["one", "two"] },
    { _id: "missing", tags: ["one"] },
    { _id: "scalar", tags: "one" },
    { _id: "absent" },
  ];
  const query = compileQuery({ tags: { $all: ["one", "two", "one"] } });
  assert.deepEqual(executeQuery(values, query).map((item) => item._id), ["all", "pair"]);
  assert.deepEqual(query.where, { op: "all", path: "tags", values: ["one", "two"] });
  assert.deepEqual(encodeQuerySpec(query).where, {
    op: "all",
    path: "tags",
    values: [{ t: "string", v: "one" }, { t: "string", v: "two" }],
  });
  assert.deepEqual(
    decodeQuerySpec({
      version: 1,
      where: {
        op: "all",
        path: "tags",
        values: [{ t: "string", v: "one" }, { t: "string", v: "two" }, { t: "string", v: "one" }],
      },
    }).where,
    { op: "all", path: "tags", values: ["one", "two"] },
  );
  assert.throws(() => compileQuery({ tags: { $all: [] } }), QueryValidationError);
  assert.throws(() => decodeQuerySpec({ version: 1, where: { op: "all", path: "tags", values: [] } }), QueryValidationError);
});

test("$elemMatch keeps scalar and object conditions on one array element", () => {
  const values: Document[] = [
    { _id: "split", scores: [80, 93, 101], parts: [{ kind: "a", qty: 1 }, { kind: "b", qty: 5 }] },
    { _id: "same", scores: [85, 100], parts: [{ kind: "a", qty: 5 }] },
    { _id: "near", scores: [91], parts: [{ kind: "a", qty: 2 }, { kind: "b", qty: 9 }] },
    { _id: "scalar", scores: 93, parts: [] },
  ];
  const scalar = compileQuery({ scores: { $elemMatch: { $gte: 90, $lt: 100 } } });
  assert.deepEqual(executeQuery(values, scalar).map((item) => item._id), ["split", "near"]);
  const object = compileQuery({ parts: { $elemMatch: { kind: "a", qty: { $gte: 5 } } } });
  assert.deepEqual(executeQuery(values, object).map((item) => item._id), ["same"]);
  assert.deepEqual(decodeQuerySpec(encodeQuerySpec(scalar)), scalar);
  assert.throws(() => compileQuery({ parts: { $elemMatch: {} } }), QueryValidationError);
  assert.throws(() => compileQuery({ parts: { $elemMatch: { $gte: 1, kind: "a" } } }), QueryValidationError);
  assert.throws(() => decodeQuerySpec({ version: 1, where: { op: "elem_match", path: "parts", mode: "scalar", arg: { op: "compare", cmp: "eq", path: "bad", value: { t: "number", v: 1 } } } }), QueryValidationError);
});

test("compares Int64 bigint and Float64 number without rounding Int64", () => {
  const values: Document[] = [
    { _id: "a", n: 9_007_199_254_740_993n },
    { _id: "b", n: 10 },
  ];
  assert.deepEqual(
    executeQuery(values, compileQuery({ n: { $gt: 9_007_199_254_740_992 } })).map((x) => x._id),
    ["a"],
  );
  assert.deepEqual(
    executeQuery(values, compileQuery({ n: 10n })).map((x) => x._id),
    ["b"],
  );
});

test("uses UTF-8 scalar ordering for ranges and a transitive mixed-value sort", () => {
  const unicode: Document[] = [
    { _id: "non-bmp", x: "\u{10000}" },
    { _id: "bmp", x: "\uE000" },
  ];
  assert.deepEqual(
    executeQuery(unicode, compileQuery({ x: { $gt: "\uE000" } })).map((item) => item._id),
    ["non-bmp"],
  );

  const values: Document[] = [
    { _id: "string-z", value: "z" },
    { _id: "number", value: 0 },
    { _id: "string-a", value: "a" },
    { _id: "null", value: null },
    { _id: "boolean", value: true },
    { _id: "time", value: new Date(0) },
    { _id: "id-later", value: documentID("00000000000000000000000000000002") },
    { _id: "id-earlier", value: documentID("00000000000000000000000000000001") },
    { _id: "binary", value: new Uint8Array([1]) },
    { _id: "array", value: [] },
    { _id: "object", value: {} },
  ];
  assert.deepEqual(
    executeQuery(values, compileQuery({}, { sort: [{ path: "value", direction: 1 }] })).map((item) => item._id),
    [
      "null",
      "boolean",
      "number",
      "string-a",
      "string-z",
      "time",
      "id-earlier",
      "id-later",
      "binary",
      "array",
      "object",
    ],
  );
});

test("generic DocumentID query and cursor values retain the id wire type", () => {
  const owner = documentID("00000000000000000000000000000002");
  const document: Document = { _id: "00000000000000000000000000000001", owner };
  const query = compileQuery({ owner });
  const wire = encodeQuerySpec(query);
  assert.deepEqual(wire.where, { op: "compare", cmp: "eq", path: "owner", value: { t: "id", v: owner.value } });

  const decoded = decodeQuerySpec(wire);
  assert.equal(decoded.where.op, "compare");
  if (decoded.where.op === "compare") assert.equal(isDocumentIDValue(decoded.where.value), true);
  assert.deepEqual(executeQuery([document], decoded), [document]);

  const cursor = pageCursorFor(document, [{ path: "owner", direction: 1 }]);
  const after = encodeQuerySpec(compileQuery(pageFilterAfter(cursor, [{ path: "owner", direction: 1 }])));
  assert.match(JSON.stringify(after), /"t":"id","v":"00000000000000000000000000000002"/);
});

test("encodes persisted _id query values with their distinct wire type", () => {
  const id = "00000000000000000000000000000001";
  const wire = encodeQuerySpec(compileQuery({ _id: id }));
  assert.deepEqual(wire.where, { op: "compare", cmp: "eq", path: "_id", value: { t: "id", v: id } });
  const decoded = decodeQuerySpec(wire);
  assert.equal(decoded.where.op, "compare");
  if (decoded.where.op === "compare") assert.equal(decoded.where.value, id);
  assert.deepEqual(encodeQuerySpec(decoded), wire);
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

test("direct query encoding validates values before JSON serialization", () => {
  assert.throws(
    () =>
      encodeQuerySpec({
        version: 1,
        where: { op: "compare", cmp: "eq", path: "rank", value: Number.NaN as never },
      }),
    QueryValidationError,
  );
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
  assert.deepEqual(
    first.map((item) => item._id),
    ["00000000000000000000000000000001", "00000000000000000000000000000002"],
  );
  assert.deepEqual(
    second.map((item) => item._id),
    ["00000000000000000000000000000003", "00000000000000000000000000000004"],
  );
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
    assert.deepEqual(
      ids,
      direction === 1
        ? [
            "00000000000000000000000000000001",
            "00000000000000000000000000000003",
            "00000000000000000000000000000002",
            "00000000000000000000000000000004",
            "00000000000000000000000000000005",
          ]
        : [
            "00000000000000000000000000000005",
            "00000000000000000000000000000002",
            "00000000000000000000000000000004",
            "00000000000000000000000000000001",
            "00000000000000000000000000000003",
          ],
    );
  }
});

test("seek pagination refuses array and object sort cursor values", () => {
  const document: Document = { _id: "00000000000000000000000000000001", rank: [1] };
  assert.throws(() => pageCursorFor(document, [{ path: "rank", direction: 1 }]), QueryValidationError);
});

test("seek cursors stay within their decoder budget", () => {
  const sort = [{ path: "rank", direction: 1 }] as const;
  const supported: Document = { _id: "00000000000000000000000000000001", rank: "x".repeat(12_000) };
  const cursor = pageCursorFor(supported, sort);
  assert.doesNotThrow(() => pageFilterAfter(cursor, sort));

  const oversized: Document = { _id: "00000000000000000000000000000001", rank: "x".repeat(13_000) };
  assert.throws(() => pageCursorFor(oversized, sort), /exceeds size limit/);
});

test("seek pagination rejects a cursor used with a different sort", () => {
  const document: Document = { _id: "00000000000000000000000000000001", rank: 1 };
  const cursor = pageCursorFor(document, [{ path: "rank", direction: 1 }]);
  assert.throws(
    () => compileQuery({}, { sort: [{ path: "rank", direction: -1 }], first: 10, after: cursor }),
    QueryValidationError,
  );
});

test("seek cursors require first", () => {
  const document: Document = { _id: "00000000000000000000000000000001", rank: 1 };
  const cursor = pageCursorFor(document, [{ path: "rank", direction: 1 }]);
  assert.throws(
    () => compileQuery({}, { sort: [{ path: "rank", direction: 1 }], after: cursor }),
    /after requires first/,
  );
});

test("rejects duplicate sort paths in compiled and wire queries", () => {
  assert.throws(
    () =>
      compileQuery(
        {},
        {
          sort: [
            { path: "rank", direction: 1 },
            { path: "rank", direction: -1 },
          ],
        },
      ),
    QueryValidationError,
  );
  assert.throws(
    () =>
      decodeQuerySpec({
        version: 1,
        where: { op: "true" },
        sort: [
          { path: "rank", direction: 1 },
          { path: "rank", direction: -1 },
        ],
      }),
    QueryValidationError,
  );
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
  assert.throws(
    () =>
      decodeQuerySpec(
        {
          version: 1,
          where: {
            op: "compare",
            cmp: "eq",
            path: "value",
            value: { t: "array", v: [{ t: "null" }, { t: "null" }, { t: "null" }] },
          },
        },
        limits,
      ),
    QueryValidationError,
  );
});

test("enforces the final query wire-size limit", () => {
  assert.throws(
    () => compileQuery({ title: "x".repeat(100) }, { limits: { maxWireBytes: 100 } }),
    QueryValidationError,
  );
  assert.throws(
    () => decodeQuerySpec({ version: 1, where: { op: "true" }, padding: "x".repeat(100) }, { maxWireBytes: 100 }),
    QueryValidationError,
  );
});
