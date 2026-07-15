import assert from "node:assert/strict";
import test from "node:test";
import { JSDOM } from "jsdom";
import { act, createElement } from "react";
import { createRoot } from "react-dom/client";
import { LocalCollection } from "@meldbase/client/local";
import type { Document } from "@meldbase/client/types";
import { useLiveQuery } from "./index.js";

type Item = Document & { readonly rank: bigint; readonly title: string };

test("useLiveQuery follows one local query and stops after unmount", async (t) => {
	const dom = new JSDOM("<!doctype html><div id=app></div>");
	Object.defineProperty(globalThis, "window", { configurable: true, value: dom.window });
	Object.defineProperty(globalThis, "document", { configurable: true, value: dom.window.document });
	Object.defineProperty(globalThis, "IS_REACT_ACT_ENVIRONMENT", { configurable: true, value: true });
	t.after(() => {
		dom.window.close();
		Reflect.deleteProperty(globalThis, "window");
		Reflect.deleteProperty(globalThis, "document");
		Reflect.deleteProperty(globalThis, "IS_REACT_ACT_ENVIRONMENT");
	});
  const collection = new LocalCollection<Item>([
    { _id: "00000000000000000000000000000001", rank: 2n, title: "second" },
  ]);
  const query = collection.find({}, { sort: [{ path: "rank", direction: 1 }] });
  const renders: string[][] = [];

  function View() {
    const result = useLiveQuery(query);
    renders.push(result.documents.map((document) => document.title));
    return null;
  }

  const container = document.querySelector("#app");
	assert.ok(container);
	const root = createRoot(container);
  await act(async () => { root.render(createElement(View)); });
  assert.deepEqual(renders.at(-1), ["second"]);

  await act(async () => {
    collection.insert({ _id: "00000000000000000000000000000002", rank: 1n, title: "first" });
    await Promise.resolve();
  });
  assert.deepEqual(renders.at(-1), ["first", "second"]);

  await act(async () => { root.unmount(); });
  const renderCount = renders.length;
  collection.insert({ _id: "00000000000000000000000000000003", rank: 3n, title: "third" });
  await Promise.resolve();
  assert.equal(renders.length, renderCount);
});
