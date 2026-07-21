import assert from "node:assert/strict";
import test from "node:test";
import { JSDOM } from "jsdom";
import { act, createElement } from "react";
import { createRoot } from "react-dom/client";
import { encodeDocument, MeldbaseClient } from "@meldbase/client";
import { LocalCollection } from "@meldbase/client/local";
import type { WebSocketLike } from "@meldbase/client/remote";
import type { Document } from "@meldbase/client/types";
import { useLiveQuery } from "./index.js";

type Item = Document & { readonly rank: bigint; readonly title: string };

class FakeRemoteSocket implements WebSocketLike {
  readyState = 0;
  readonly sent: string[] = [];
  closed?: { readonly code?: number; readonly reason?: string };
  readonly #listeners = new Map<string, Array<(event: Event | MessageEvent) => void>>();

  addEventListener(type: "open" | "close" | "error" | "message", listener: (event: Event | MessageEvent) => void): void {
    const listeners = this.#listeners.get(type) ?? [];
    listeners.push(listener);
    this.#listeners.set(type, listeners);
  }

  removeEventListener(type: "open" | "close" | "error" | "message", listener: (event: Event | MessageEvent) => void): void {
    this.#listeners.set(type, (this.#listeners.get(type) ?? []).filter((candidate) => candidate !== listener));
  }

  send(data: string): void { this.sent.push(data); }
  close(code?: number, reason?: string): void {
    this.closed = { ...(code === undefined ? {} : { code }), ...(reason === undefined ? {} : { reason }) };
    this.readyState = 3;
  }
  open(): void { this.readyState = 1; this.#emit("open", {} as Event); }
  message(value: unknown): void { this.#emit("message", { data: JSON.stringify(value) } as MessageEvent); }

  #emit(type: string, event: Event | MessageEvent): void {
    for (const listener of this.#listeners.get(type) ?? []) listener(event);
  }
}

async function settle(): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
}

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

test("useLiveQuery renders remote initial data, transitions live, and unsubscribes on unmount", async (t) => {
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
  const sockets: FakeRemoteSocket[] = [];
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({
      url: "wss://db.example/realtime",
      ticket: "single-use-ticket",
      protocol: { versions: [1], capabilities: ["query.delta", "query.resume", "rpc", "rpc.cancel", "rpc.idempotency"] },
    }),
    webSocketFactory: () => {
      const socket = new FakeRemoteSocket();
      sockets.push(socket);
      return socket;
    },
  });
  const query = client.collection<Item>("todos").find();
  const renders: Array<{ readonly titles: string[]; readonly status: string; readonly token?: string }> = [];

  function View() {
    const result = useLiveQuery(query, { initialData: [{ _id: "00000000000000000000000000000001", rank: 0n, title: "cached" }] });
    renders.push({
      titles: result.documents.map((document) => document.title),
      status: result.status,
      ...(result.token ? { token: result.token } : {}),
    });
    return null;
  }

  const container = document.querySelector("#app");
  assert.ok(container);
  const root = createRoot(container);
  try {
    await act(async () => { root.render(createElement(View)); await settle(); });
    assert.deepEqual(renders[0], { titles: ["cached"], status: "idle" });
    const socket = sockets[0];
    assert.ok(socket);

    await act(async () => {
      socket.open();
      socket.message({ v: 1, type: "authenticated" });
      const subscribe = JSON.parse(socket.sent.at(-1)!) as { readonly requestId: string; readonly type: string };
      assert.equal(subscribe.type, "subscribe");
      socket.message({
        v: 1, type: "snapshot", requestId: subscribe.requestId, subscriptionId: "todos-subscription", token: "token-1",
        documents: [encodeDocument({ _id: "00000000000000000000000000000002", rank: 1n, title: "live" })],
      });
      await settle();
    });
    assert.deepEqual(renders.at(-1), { titles: ["live"], status: "live", token: "token-1" });

    await act(async () => { root.unmount(); await settle(); });
    const renderCount = renders.length;
    assert.equal(socket.sent.some((frame) => (JSON.parse(frame) as { readonly type?: string }).type === "unsubscribe"), true);
    assert.equal(socket.closed?.reason, "no active work");
    socket.message({
      v: 1, type: "snapshot", requestId: "ignored", subscriptionId: "ignored", token: "token-2",
      documents: [encodeDocument({ _id: "00000000000000000000000000000003", rank: 2n, title: "ignored" })],
    });
    await settle();
    assert.equal(renders.length, renderCount);
  } finally {
    client.close();
  }
});
