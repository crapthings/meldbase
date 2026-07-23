import assert from "node:assert/strict";
import test from "node:test";
import { decodeDocument, decodeValue, encodeDocument, encodeValue, QueryValidationError } from "./index.js";
import type { Document } from "./index.js";

test("wire values preserve dates, binary, arrays, and objects without JSON ambiguity", () => {
  const input: Document = {
    _id: "00000000000000000000000000000001",
    createdAt: new Date("2026-07-15T00:00:00.000Z"),
    bytes: new Uint8Array([0, 127, 255]),
    exact: 9_223_372_036_854_775_807n,
    nested: { t: "date", v: "ordinary user object" },
  };
  const transported = JSON.parse(JSON.stringify(encodeDocument(input))) as unknown;
  const output = decodeDocument(transported);
  assert.equal((output.createdAt as Date).toISOString(), "2026-07-15T00:00:00.000Z");
  assert.equal(output._id, "00000000000000000000000000000001");
  assert.deepEqual([...output.bytes as Uint8Array], [0, 127, 255]);
  assert.equal(output.exact, 9_223_372_036_854_775_807n);
  assert.deepEqual({ ...(output.nested as object) }, { t: "date", v: "ordinary user object" });
});

test("wire decoder rejects duplicate and prototype-polluting object fields", () => {
  assert.throws(() => decodeValue({ t: "object", v: [["x", { t: "null" }], ["x", { t: "null" }]] }), /Duplicate/);
  assert.throws(() => decodeValue({ t: "object", v: [["__proto__", { t: "null" }]] }), QueryValidationError);
  assert.deepEqual(decodeValue(JSON.parse(JSON.stringify(encodeValue([1, null, "x"])))), [1, null, "x"]);
  assert.throws(() => decodeValue({ t: "int64", v: "9223372036854775808" }), /Malformed/);
  assert.throws(() => decodeValue({ t: "string", v: "x", extra: true }), QueryValidationError);
  assert.throws(() => decodeValue({ t: "null", v: null }), QueryValidationError);
  assert.throws(() => decodeValue({ t: "binary", v: "AQ" }), QueryValidationError);
  assert.throws(() => decodeValue({ t: "id", v: "00000000000000000000000000000000" }), QueryValidationError);
  assert.throws(() => encodeDocument({ _id: "temporary-id" }), /Persisted _id/);
  assert.throws(() => decodeDocument({ t: "object", v: [["_id", { t: "string", v: "temporary-id" }]] }), /Persisted document _id/);
});
