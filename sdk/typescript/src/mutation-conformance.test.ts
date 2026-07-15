import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { applyMutation, decodeDocument, decodeMutationSpec } from "./index.js";

interface CorpusCase { name: string; document: unknown; mutation: unknown; expected: unknown }

test("TypeScript executes the shared Go/TypeScript mutation corpus", async () => {
  const url = new URL("../../../testdata/mutation-conformance.json", import.meta.url);
  const corpus = JSON.parse(await readFile(url, "utf8")) as { version: number; cases: CorpusCase[] };
  assert.equal(corpus.version, 1);
  for (const item of corpus.cases) {
    const actual = applyMutation(decodeDocument(item.document), decodeMutationSpec(item.mutation));
    assert.deepEqual(actual, decodeDocument(item.expected), item.name);
  }
});
