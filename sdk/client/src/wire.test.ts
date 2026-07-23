import assert from "node:assert/strict";
import test from "node:test";
import { documentID, isDocumentIDValue } from "./safe-value.js";
import { QueryValidationError, type Document } from "./types.js";
import { decodeDocument, decodeValue, encodeDocument, encodeInputDocument, encodeValue } from "./wire.js";

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
  assert.deepEqual([...(output.bytes as Uint8Array)], [0, 127, 255]);
  assert.equal(output.exact, 9_223_372_036_854_775_807n);
  assert.deepEqual({ ...(output.nested as object) }, { t: "date", v: "ordinary user object" });
});

test("generic DocumentID values preserve their wire kind while document _id stays a string", () => {
  const documentIDValue = "00000000000000000000000000000002";
  const persistedID = "00000000000000000000000000000001";
  const genericID = documentID(documentIDValue);
  assert.deepEqual(encodeValue(genericID), { t: "id", v: documentIDValue });
  assert.deepEqual(encodeValue(documentIDValue), { t: "string", v: documentIDValue });

  const decoded = decodeValue({ t: "id", v: documentIDValue });
  assert.equal(isDocumentIDValue(decoded), true);
  assert.equal((decoded as { readonly value: string }).value, documentIDValue);
  assert.deepEqual(encodeValue(decoded), { t: "id", v: documentIDValue });

  const roundTripped = decodeDocument(
    encodeDocument({
      _id: persistedID,
      owner: genericID,
      nested: { ids: [genericID] },
    }),
  );
  assert.equal(roundTripped._id, persistedID);
  assert.equal(isDocumentIDValue(roundTripped.owner), true);
  assert.equal((roundTripped.owner as { readonly value: string }).value, documentIDValue);
  const nested = roundTripped.nested as { ids: readonly { readonly value: string }[] };
  assert.equal(isDocumentIDValue(nested.ids[0]), true);
  assert.equal(nested.ids[0]?.value, documentIDValue);
});

test("wire decoder rejects duplicate and prototype-polluting object fields", () => {
  assert.throws(
    () =>
      decodeValue({
        t: "object",
        v: [
          ["x", { t: "null" }],
          ["x", { t: "null" }],
        ],
      }),
    /Duplicate/,
  );
  assert.throws(() => decodeValue({ t: "object", v: [["__proto__", { t: "null" }]] }), QueryValidationError);
  assert.deepEqual(decodeValue(JSON.parse(JSON.stringify(encodeValue([1, null, "x"])))), [1, null, "x"]);
  assert.throws(() => decodeValue({ t: "int64", v: "9223372036854775808" }), /Malformed/);
  assert.throws(() => decodeValue({ t: "string", v: "x", extra: true }), QueryValidationError);
  assert.throws(() => decodeValue({ t: "null", v: null }), QueryValidationError);
  assert.throws(() => decodeValue({ t: "binary", v: "AQ" }), QueryValidationError);
  assert.throws(() => decodeValue({ t: "id", v: "00000000000000000000000000000000" }), QueryValidationError);
  assert.throws(() => encodeDocument({ _id: "temporary-id" }), /Persisted _id/);
  assert.throws(
    () => decodeDocument({ t: "object", v: [["_id", { t: "string", v: "temporary-id" }]] }),
    /Persisted document _id/,
  );
  assert.throws(
    () => decodeDocument({ t: "object", v: [["_id", { t: "string", v: "00000000000000000000000000000001" }]] }),
    /id wire type/,
  );
});

test("public outbound encoders reject values that JSON would change or overrun server limits", () => {
  const invalidDate = new Date(Number.NaN);
  const unsupportedObject = new (class UnsupportedValue {})();
  const oversizedArray = Array.from({ length: 257 }, () => null);
  const oversizedString = "x".repeat(16_385);
  const tooDeep = Array.from({ length: 65 }).reduce<unknown>((value) => [value], null);

  for (const value of [
    Number.NaN,
    Number.POSITIVE_INFINITY,
    invalidDate,
    1n << 63n,
    unsupportedObject,
    oversizedArray,
    oversizedString,
    tooDeep,
  ]) {
    assert.throws(() => encodeValue(value as never), QueryValidationError);
  }

  assert.throws(
    () => encodeDocument({ _id: "00000000000000000000000000000001", value: Number.NaN as never }),
    QueryValidationError,
  );
  assert.throws(() => encodeInputDocument({ value: oversizedArray }), QueryValidationError);
});
