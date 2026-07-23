import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { executeQuery } from "./query.js";
import { decodeQuerySpec, decodeValue } from "./wire.js";
import type { Document } from "./index.js";

interface CorpusCase {
  name: string;
  documents: unknown[];
  query: unknown;
  expectedIds: string[];
}

test("TypeScript executes the shared Go/TypeScript conformance corpus", async () => {
  const url = new URL("../../../testdata/query-conformance.json", import.meta.url);
  const corpus = JSON.parse(await readFile(url, "utf8")) as { version: number; cases: CorpusCase[] };
  assert.equal(corpus.version, 1);
  for (const item of corpus.cases) {
    // Query fixtures use plain string labels for readability. They exercise the
    // generic value/query codec rather than persisted-document ID validation.
    const documents = item.documents.map((document) => decodeValue(document) as Document);
    const result = executeQuery(documents, decodeQuerySpec(item.query));
    assert.deepEqual(
      result.map((document) => document._id),
      item.expectedIds,
      item.name,
    );
  }
});
