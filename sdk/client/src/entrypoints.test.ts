import assert from "node:assert/strict";
import test from "node:test";

import * as client from "@meldbase/client";
import * as local from "@meldbase/client/local";

test("the root client entry is remote-first and local state has an explicit subpath", () => {
  assert.equal("MeldbaseClient" in client, true);
  assert.equal("LocalCollection" in client, false);
  assert.equal(typeof local.LocalCollection, "function");
  for (const implementationDetail of [
    "DocumentID",
    "RemoteCollection",
    "RemoteLiveQuery",
    "applyMutation",
    "compileQuery",
    "compileUpdate",
    "decodeDocument",
    "decodeValue",
    "encodeDocument",
    "encodeValue",
    "executeQuery",
    "pageCursorFor",
    "pageFilterAfter",
  ]) {
    assert.equal(implementationDetail in client, false, `${implementationDetail} must stay off the root entry point`);
  }
  assert.equal("LiveQuery" in local, false, "LocalCollection.find() owns live-query construction");
});
