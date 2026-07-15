import assert from "node:assert/strict";
import test from "node:test";
import { encodeDocument, MeldbaseClient } from "./index.js";
import type { Document, SyncState, WebSocketLike } from "./index.js";

class FakeSocket implements WebSocketLike {
  readyState = 0;
  sent: string[] = [];
  closed?: { code?: number; reason?: string };
  readonly listeners = new Map<string, Array<(event: Event | MessageEvent) => void>>();

  addEventListener(type: "open" | "close" | "error" | "message", listener: (event: Event | MessageEvent) => void): void {
    const listeners = this.listeners.get(type) ?? [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }
  send(data: string): void { this.sent.push(data); }
  close(code?: number, reason?: string): void { this.closed = { ...(code === undefined ? {} : { code }), ...(reason === undefined ? {} : { reason }) }; this.readyState = 3; }
  open(): void { this.readyState = 1; this.emit("open", new Event("open")); }
  disconnect(): void { this.readyState = 3; this.emit("close", new Event("close")); }
  message(value: unknown): void { this.emit("message", new MessageEvent("message", { data: JSON.stringify(value) })); }
  private emit(type: string, event: Event | MessageEvent): void { for (const listener of this.listeners.get(type) ?? []) listener(event); }
}

const settle = async () => { await new Promise<void>((resolve) => setTimeout(resolve, 0)); };

test("remote query uses HTTP AST and realtime ticket keeps credentials out of WebSocket URL", async () => {
  const sockets: FakeSocket[] = [];
  const requests: Array<{ url: string; init?: RequestInit }> = [];
  const fetcher: typeof fetch = async (input, init) => {
    const url = String(input);
    requests.push({ url, ...(init ? { init } : {}) });
    if (url.endsWith("/v1/realtime/tickets")) return Response.json({ url: "wss://db.example/realtime", ticket: "single-use-secret" });
    if (url.endsWith("/documents")) return Response.json({ document: encodeDocument({ _id: "00000000000000000000000000000003", title: "created" }) }, { status: 201 });
    if (url.endsWith("/mutations")) {
      const body = JSON.parse(init?.body as string) as { action: string };
      return body.action.startsWith("update") ? Response.json({ matchedCount: 2, modifiedCount: 2 }) : Response.json({ deletedCount: 1 });
    }
    return Response.json({ documents: [encodeDocument({ _id: "00000000000000000000000000000001", done: false })] });
  };
  const client = new MeldbaseClient({
    baseUrl: "https://db.example/",
    accessToken: () => "access-secret",
    fetch: fetcher,
    webSocketFactory: (url) => { assert.equal(url, "wss://db.example/realtime"); const socket = new FakeSocket(); sockets.push(socket); return socket; },
  });
  const query = client.collection<Document>("todos").find({ done: false });
  assert.deepEqual((await query.fetch()).map((item) => item._id), ["00000000000000000000000000000001"]);
  const inserted = await client.collection<Document>("todos").insertOne({ title: "created" });
  assert.equal(inserted._id, "00000000000000000000000000000003");
  const insertBody = JSON.parse(requests[1]?.init?.body as string) as { document: { v: Array<[string, unknown]> } };
  assert.equal(insertBody.document.v.some(([field]) => field === "_id"), false);
  assert.deepEqual(await client.collection("todos").updateMany({ done: false }, { $set: { done: true } }), { matchedCount: 2, modifiedCount: 2 });
  assert.deepEqual(await client.collection("todos").deleteOne({ done: true }), { deletedCount: 1 });

  const snapshots: string[][] = [];
  const states: SyncState[] = [];
  const unsubscribe = query.subscribe((items) => snapshots.push(items.map((item) => item._id)), { onStatus: (status) => states.push(status.state) });
  await settle();
  assert.equal(sockets.length, 1);
  const socket = sockets[0] as FakeSocket;
  socket.open();
  assert.deepEqual(JSON.parse(socket.sent[0] as string), { v: 1, type: "authenticate", ticket: "single-use-secret" });
  socket.message({ v: 1, type: "authenticated" });
  const subscribe = JSON.parse(socket.sent[1] as string) as Record<string, unknown>;
  assert.equal(subscribe.type, "subscribe");
  assert.equal(subscribe.collection, "todos");
  assert.equal(JSON.stringify(subscribe).includes("access-secret"), false);
  socket.message({ v: 1, type: "snapshot", requestId: subscribe.requestId, subscriptionId: "server-sub", token: "opaque-1", documents: [encodeDocument({ _id: "00000000000000000000000000000002", done: false })] });
  await settle();
  assert.deepEqual(snapshots, [["00000000000000000000000000000002"]]);
  assert.equal(states.at(-1), "live");
  assert.equal(requests[0]?.init?.headers instanceof Headers, false);
  assert.equal((requests[0]?.init?.headers as Record<string, string>).authorization, "Bearer access-secret");
  unsubscribe();
	assert.equal(socket.closed?.reason, "no active subscriptions");
	const unsubscribeAgain = query.subscribe(() => {});
	await settle();
	assert.equal(sockets.length, 2);
	unsubscribeAgain();
  client.close();
});

test("malformed or oversized snapshots fail closed", async () => {
  let socket: FakeSocket | undefined;
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({ url: "wss://db.example/realtime", ticket: "ticket" }),
    webSocketFactory: () => { socket = new FakeSocket(); return socket; },
    maxSnapshotDocuments: 1,
  });
  const states: SyncState[] = [];
  client.collection("todos").find().subscribe(() => {}, { onStatus: (status) => states.push(status.state) });
  await settle();
  const active = socket as unknown as FakeSocket;
  active.open();
  active.message({ v: 1, type: "authenticated" });
  const subscribe = JSON.parse(active.sent[1] as string) as Record<string, unknown>;
  active.message({ v: 1, type: "snapshot", requestId: subscribe.requestId, subscriptionId: "s", token: "t", documents: [encodeDocument({ _id: "00000000000000000000000000000001" }), encodeDocument({ _id: "00000000000000000000000000000002" })] });
  await settle();
  assert.equal(states.at(-1), "error");
  assert.equal(active.closed?.code, 1002);
  client.close();
});

test("reconnect presents the last opaque token and cleanly handles resync_required", async () => {
  const sockets: FakeSocket[] = [];
  let ticket = 0;
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({ url: "wss://db.example/realtime", ticket: `ticket-${++ticket}` }),
    webSocketFactory: () => { const socket = new FakeSocket(); sockets.push(socket); return socket; },
    reconnect: { minDelayMs: 1, maxDelayMs: 1 },
  });
  const states: SyncState[] = [];
  const unsubscribe = client.collection("todos").find({ done: false }).subscribe(() => {}, { onStatus: (status) => states.push(status.state) });
  await settle();
  const first = sockets[0] as FakeSocket;
  first.open();
  first.message({ v: 1, type: "authenticated" });
  const initialSubscribe = JSON.parse(first.sent[1] as string) as Record<string, unknown>;
  first.message({ v: 1, type: "snapshot", requestId: initialSubscribe.requestId, subscriptionId: "server-1", token: "opaque.signed-token", documents: [] });
  await settle();

  first.disconnect();
  await settle();
  await settle();
  assert.equal(sockets.length, 2);
  const second = sockets[1] as FakeSocket;
  second.open();
  assert.deepEqual(JSON.parse(second.sent[0] as string), { v: 1, type: "authenticate", ticket: "ticket-2" });
  second.message({ v: 1, type: "authenticated" });
  const resumed = JSON.parse(second.sent[1] as string) as Record<string, unknown>;
  assert.equal(resumed.requestId, initialSubscribe.requestId);
  assert.equal(resumed.resumeToken, "opaque.signed-token");
  await settle();
  assert.equal(states.includes("stale"), true);
  assert.equal(states.includes("resyncing"), true);

  second.message({ v: 1, type: "resync_required", requestId: resumed.requestId });
  const clean = JSON.parse(second.sent[2] as string) as Record<string, unknown>;
  assert.equal(clean.type, "subscribe");
  assert.equal("resumeToken" in clean, false);
  unsubscribe();
  client.close();
});
