import assert from "node:assert/strict";
import test from "node:test";

import * as client from "@meldbase/client";
import { LocalCollection } from "@meldbase/client/local";

test("the root client entry is remote-first and local state has an explicit subpath", () => {
  assert.equal("MeldbaseClient" in client, true);
  assert.equal("LocalCollection" in client, false);
  assert.equal(typeof LocalCollection, "function");
});
