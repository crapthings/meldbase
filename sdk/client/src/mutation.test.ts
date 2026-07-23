import assert from "node:assert/strict";
import test from "node:test";
import { applyMutation, compileUpdate, decodeMutationSpec, encodeMutationSpec } from "./mutation.js";
import { documentID, isDocumentIDValue } from "./safe-value.js";
import { QueryValidationError } from "./types.js";
import type { Document } from "./index.js";

test("mutation compiler is canonical and round-trips through data-only wire form", () => {
  const mutation = compileUpdate({
    $set: { "profile.city": "B" },
    $unset: ["obsolete"],
    $inc: { count: 2n },
    $push: { notes: "new" },
    $pull: { tags: "old" },
  });
  assert.deepEqual(
    mutation.operations.map((item) => `${item.op}:${item.path}`),
    ["inc:count", "pull:tags", "push:notes", "set:profile.city", "unset:obsolete"],
  );
  const wire = JSON.parse(JSON.stringify(encodeMutationSpec(mutation))) as unknown;
  assert.deepEqual(decodeMutationSpec(wire), mutation);
  const before: Document = {
    _id: "00000000000000000000000000000001",
    count: 1n,
    profile: { city: "A" },
    obsolete: true,
    notes: [],
    tags: ["old", "keep"],
  };
  const after = applyMutation(before, mutation);
  assert.equal(after.count, 3n);
  assert.deepEqual({ ...(after.profile as object) }, { city: "B" });
  assert.equal("obsolete" in after, false);
  assert.deepEqual(after.notes, ["new"]);
  assert.deepEqual(after.tags, ["keep"]);
});

test("mutation compiler rejects unsafe, ambiguous, executable, and lossy updates", () => {
  assert.throws(() => compileUpdate({ $set: { _id: "x" } }), /immutable/);
  assert.throws(() => compileUpdate({ $set: { profile: {} as never, "profile.city": "x" } }), /Conflicting/);
  assert.throws(() => compileUpdate({ $set: { "__proto__.admin": true } }), QueryValidationError);
  assert.throws(() => compileUpdate({ $set: { x: (() => true) as never } }), /Unsupported/);
  const document: Document = { _id: "00000000000000000000000000000001", n: 9_007_199_254_740_993n };
  assert.throws(() => applyMutation(document, compileUpdate({ $inc: { n: 0.5 } })), /lose precision/);
});

test("mutations preserve generic DocumentID values", () => {
  const owner = documentID("00000000000000000000000000000002");
  const mutation = compileUpdate({ $set: { owner }, $push: { owners: owner } });
  const wire = encodeMutationSpec(mutation);
  assert.deepEqual(wire.operations, [
    { op: "push", path: "owners", value: { t: "id", v: owner.value } },
    { op: "set", path: "owner", value: { t: "id", v: owner.value } },
  ]);
  const decoded = decodeMutationSpec(wire);
  const document: Document = { _id: "00000000000000000000000000000001", owners: [] };
  const after = applyMutation(document, decoded);
  assert.equal(isDocumentIDValue(after.owner), true);
  assert.equal(isDocumentIDValue((after.owners as readonly unknown[])[0]), true);
});

test("direct mutation encoding validates values before JSON serialization", () => {
  assert.throws(
    () =>
      encodeMutationSpec({
        version: 1,
        operations: [{ op: "set", path: "value", value: Number.NaN as never }],
      }),
    QueryValidationError,
  );
});
