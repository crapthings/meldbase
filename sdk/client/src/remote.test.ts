import assert from "node:assert/strict";
import test from "node:test";
import { encodeDocument, encodeValue, MeldbaseClient, MeldbaseClientClosedError, MeldbaseError, MeldbaseInternalError, MeldbaseProtocolError } from "./index.js";
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
  error(): void { this.emit("error", new Event("error")); }
  disconnect(): void { this.readyState = 3; this.emit("close", new Event("close")); }
  message(value: unknown): void { this.emit("message", new MessageEvent("message", { data: JSON.stringify(value) })); }
  private emit(type: string, event: Event | MessageEvent): void { for (const listener of this.listeners.get(type) ?? []) listener(event); }
}

const settle = async () => { await new Promise<void>((resolve) => setTimeout(resolve, 0)); };
const currentProtocol = { versions: [1], capabilities: ["query.delta", "query.resume", "rpc", "rpc.cancel", "rpc.idempotency"] };

test("remote query uses HTTP AST and realtime ticket keeps credentials out of WebSocket URL", async () => {
  const sockets: FakeSocket[] = [];
  const requests: Array<{ url: string; init?: RequestInit }> = [];
  const fetcher: typeof fetch = async (input, init) => {
    const url = String(input);
    requests.push({ url, ...(init ? { init } : {}) });
    if (url.endsWith("/v1/realtime/tickets")) return Response.json({ url: "wss://db.example/realtime", ticket: "single-use-secret", protocol: currentProtocol });
    if (url.endsWith("/documents")) {
      const body = JSON.parse(init?.body as string) as { document: { v: Array<[string, { t: string; v: string }]> } };
      const id = body.document.v.find(([field]) => field === "_id")?.[1]?.v;
      if (typeof id !== "string") throw new Error("remote insert omitted client document ID");
      return Response.json({ version: 1, document: encodeDocument({ _id: id, title: "created" }) }, { status: 201 });
    }
    if (url.endsWith("/mutations")) {
      const body = JSON.parse(init?.body as string) as { action: string };
      return body.action.startsWith("update") ? Response.json({ version: 1, matchedCount: 2, modifiedCount: 2 }) : Response.json({ version: 1, deletedCount: 1 });
    }
    return Response.json({ version: 1, documents: [encodeDocument({ _id: "00000000000000000000000000000001", done: false })] });
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
  assert.match(inserted._id, /^[0-9a-f]{32}$/);
  const insertBody = JSON.parse(requests[1]?.init?.body as string) as { document: { v: Array<[string, unknown]> } };
  assert.equal(insertBody.document.v.some(([field]) => field === "_id"), true);
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
  assert.equal(subscribe.mode, "delta");
  assert.equal(JSON.stringify(subscribe).includes("access-secret"), false);
  socket.message({ v: 1, type: "snapshot", requestId: subscribe.requestId, subscriptionId: "server-sub", token: "opaque-1", documents: [encodeDocument({ _id: "00000000000000000000000000000002", done: false })] });
  await settle();
  assert.deepEqual(snapshots, [["00000000000000000000000000000002"]]);
  assert.equal(states.at(-1), "live");
  assert.equal(requests[0]?.init?.headers instanceof Headers, false);
  assert.equal((requests[0]?.init?.headers as Record<string, string>).authorization, "Bearer access-secret");
	assert.equal((requests.at(-1)?.init?.headers as Record<string, string>).accept, "application/vnd.meldbase.realtime-ticket+json; capabilities=1");
  unsubscribe();
  assert.equal(socket.closed?.reason, "no active work");
  const unsubscribeAgain = query.subscribe(() => {});
  await settle();
  assert.equal(sockets.length, 2);
  unsubscribeAgain();
  client.close();
});

test("remote group count uses a bounded, typed aggregate envelope", async () => {
  const requests: Array<{ url: string; init?: RequestInit }> = [];
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    accessToken: () => "access-secret",
    fetch: async (input, init) => {
      requests.push({ url: String(input), ...(init ? { init } : {}) });
      return Response.json({
        version: 1,
        groups: [
          { key: encodeValue("open"), count: 3 },
          { key: encodeValue(7n), count: 1 },
        ],
        capped: false,
      });
    },
  });
  const result = await client.collection("todos").groupCount({ done: false }, "status");
  assert.deepEqual(result, { groups: [{ key: "open", count: 3 }, { key: 7n, count: 1 }], capped: false });
  assert.equal(requests[0]?.url, "https://db.example/v1/collections/todos/group-count");
  assert.deepEqual(JSON.parse(requests[0]?.init?.body as string), {
    version: 1,
    query: { version: 1, where: { op: "compare", cmp: "eq", path: "done", value: { t: "bool", v: false } } },
    groupBy: "status",
  });
  await assert.rejects(client.collection("todos").groupCount({}, "nested.field"), /Unsafe group field/);
  client.close();
});

test("realtime capability discovery fails closed without reconnect churn", async () => {
	const sockets: FakeSocket[] = [];
	const states: Array<{ state: SyncState; error?: Error }> = [];
	const client = new MeldbaseClient({
		baseUrl: "https://db.example",
		fetch: async () => Response.json({
			url: "wss://db.example/realtime", ticket: "ticket",
			protocol: { versions: [1], capabilities: ["query.resume", "rpc", "rpc.cancel"] },
		}),
		webSocketFactory: () => { const socket = new FakeSocket(); sockets.push(socket); return socket; },
	});
	const unsubscribe = client.collection("todos").find().subscribe(() => {}, {
		onStatus: (status) => states.push(status),
	});
	await settle();
	await settle();
	assert.equal(sockets.length, 0);
	assert.equal(states.at(-1)?.state, "error");
	assert.equal(states.at(-1)?.error instanceof MeldbaseProtocolError, true);
	assert.deepEqual((states.at(-1)?.error as MeldbaseProtocolError).required, ["query.delta"]);
	assert.deepEqual(client.realtimeProtocol, {
		versions: [1], capabilities: ["query.resume", "rpc", "rpc.cancel"],
	});
	await settle();
	assert.equal(sockets.length, 0);
	unsubscribe();
	client.close();
});

test("capabilities are enforced for work added after realtime authentication", async () => {
	const sockets: FakeSocket[] = [];
	const client = new MeldbaseClient({
		baseUrl: "https://db.example",
		fetch: async () => Response.json({
			url: "wss://db.example/realtime", ticket: "ticket",
			protocol: { versions: [1], capabilities: ["query.delta", "query.resume", "rpc", "rpc.cancel"] },
		}),
		webSocketFactory: () => { const socket = new FakeSocket(); sockets.push(socket); return socket; },
	});
	const unsubscribe = client.collection("todos").find().subscribe(() => {});
	await settle();
	const socket = sockets[0]!;
	socket.open();
	socket.message({ v: 1, type: "authenticated" });
	assert.equal(socket.sent.length, 2);
	await assert.rejects(
		client.call("orders.create", null, { transport: "realtime", idempotencyKey: "abcdefghijklmnopqrstuv" }),
		(error: unknown) => error instanceof MeldbaseProtocolError && error.required[0] === "rpc.idempotency",
	);
	assert.equal(socket.sent.length, 2);
	unsubscribe();
	client.close();
});

test("realtime protocol discovery is required", async () => {
	const sockets: FakeSocket[] = [];
	let terminal: Error | undefined;
	const client = new MeldbaseClient({
		baseUrl: "https://db.example",
		fetch: async () => Response.json({ url: "wss://db.example/realtime", ticket: "legacy-ticket" }),
		webSocketFactory: () => { const socket = new FakeSocket(); sockets.push(socket); return socket; },
	});
	const unsubscribe = client.collection("todos").find().subscribe(() => {}, {
		onStatus: (status) => { terminal = status.error; },
	});
	await settle();
	await settle();
	assert.equal(sockets.length, 0);
	assert.equal(terminal instanceof MeldbaseProtocolError, true);
	assert.deepEqual((terminal as MeldbaseProtocolError).required, ["protocol.discovery"]);
	unsubscribe();
	client.close();
});

test("realtime safety limits must be positive safe integers", () => {
  for (const options of [
    { maxInboundBytes: 0 },
    { maxSnapshotDocuments: -1 },
    { maxDeltaOperations: Number.MAX_SAFE_INTEGER + 1 },
  ]) {
		assert.throws(() => new MeldbaseClient({ baseUrl: "https://db.example", ...options }), /(positive safe integer|must be boolean)/);
  }
});

test("client configuration has explicit URL and reconnect boundaries", () => {
  for (const baseUrl of ["ftp://db.example", "https://user:pass@db.example", "https://db.example?mode=test", "https://db.example#fragment"]) {
    assert.throws(() => new MeldbaseClient({ baseUrl }), /baseUrl must be an http\(s\) URL/);
  }
  assert.throws(() => new MeldbaseClient({ baseUrl: "https://db.example", allowedRealtimeOrigins: ["https://db.example"] }), /ws\(s\) origins/);
  assert.throws(() => new MeldbaseClient({ baseUrl: "https://db.example", reconnect: { minDelayMs: 0 } }), /positive safe integer/);
  assert.throws(() => new MeldbaseClient({ baseUrl: "https://db.example", reconnect: { minDelayMs: 2, maxDelayMs: 1 } }), /must not exceed/);
});

test("HTTP responses are streamed under the byte limit and use exact versioned envelopes", async () => {
  let pulls = 0;
  let canceled = false;
  const oversized = new MeldbaseClient({
    baseUrl: "https://db.example",
    maxInboundBytes: 64,
    fetch: async () => new Response(new ReadableStream<Uint8Array>({
      pull(controller) {
        pulls += 1;
        controller.enqueue(new TextEncoder().encode("x".repeat(40)));
        if (pulls === 100) controller.close();
      },
      cancel() { canceled = true; },
    })),
  });
  await assert.rejects(oversized.collection("todos").find().fetch(), /Response exceeds safety limit/);
  assert.equal(canceled, true);
  assert.equal(pulls < 100, true);
  oversized.close();

  for (const scenario of [
    { operation: "query", body: { version: 1, documents: [], extra: true } },
    { operation: "insert", body: { document: encodeDocument({ _id: "00000000000000000000000000000001" }) } },
    { operation: "delete", body: { version: 2, deletedCount: 0 } },
  ] as const) {
    const client = new MeldbaseClient({ baseUrl: "https://db.example", fetch: async () => Response.json(scenario.body) });
    const collection = client.collection("todos");
    const operation = scenario.operation === "query" ? collection.find().fetch()
      : scenario.operation === "insert" ? collection.insertOne({ title: "x" })
      : collection.deleteOne({});
    if (scenario.operation === "insert" || scenario.operation === "delete") {
      await assert.rejects(operation, (error: unknown) => error instanceof MeldbaseInternalError && error.code === "outcome_unknown" && error.cause instanceof Error && new RegExp(`Malformed ${scenario.operation} response`).test(error.cause.message));
    } else {
      await assert.rejects(operation, /Malformed query response/);
    }
    client.close();
  }

  let ticketError: Error | undefined;
  let sockets = 0;
  const invalidTicket = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({ url: "wss://db.example/realtime", ticket: "ticket", extra: true }),
    webSocketFactory: () => { sockets += 1; return new FakeSocket(); },
  });
  const unsubscribe = invalidTicket.collection("todos").find().subscribe(() => {}, {
    onStatus: (status) => { ticketError = status.error; },
  });
  await settle();
  await settle();
  assert.match(ticketError?.message ?? "", /Malformed realtime ticket response/);
  assert.equal(sockets, 0);
  unsubscribe();
  invalidTicket.close();
});

test("data operations and subscriptions expose stable remote error codes", async () => {
  const sockets: FakeSocket[] = [];
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async (input) => String(input).endsWith("/v1/realtime/tickets")
      ? Response.json({ url: "wss://db.example/realtime", ticket: "ticket", protocol: currentProtocol })
      : Response.json({ error: { kind: "internal", code: "database_unavailable" } }, { status: 503 }),
    webSocketFactory: () => { const socket = new FakeSocket(); sockets.push(socket); return socket; },
  });
  await assert.rejects(client.collection("todos").find().fetch(), (error: unknown) =>
    error instanceof MeldbaseInternalError && error.code === "database_unavailable" && error.status === 503 && error.operation === "query",
  );
  await assert.rejects(client.collection("todos").insertOne({ title: "x" }), (error: unknown) =>
    error instanceof MeldbaseInternalError && error.code === "database_unavailable" && error.operation === "insert",
  );
  await assert.rejects(client.collection("todos").deleteOne({ done: true }), (error: unknown) =>
    error instanceof MeldbaseInternalError && error.code === "database_unavailable" && error.operation === "mutation",
  );

  let subscriptionError: Error | undefined;
  const unsubscribe = client.collection("todos").find().subscribe(() => {}, { onStatus: (status) => { subscriptionError = status.error; } });
  await settle();
  const socket = sockets[0] as FakeSocket;
  socket.open();
  socket.message({ v: 1, type: "authenticated" });
  const subscribe = JSON.parse(socket.sent[1] as string) as { requestId: string };
  socket.message({ v: 1, type: "error", requestId: subscribe.requestId, error: { kind: "internal", code: "database_unavailable" } });
  await settle();
  assert.equal(subscriptionError instanceof MeldbaseInternalError, true);
  assert.equal((subscriptionError as MeldbaseInternalError).code, "database_unavailable");
  unsubscribe();
  client.close();
});

test("remote insert owns its ID before transport and marks an unverifiable result as unknown", async () => {
  let suppliedID: string | undefined;
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async (_input, init) => {
      const body = JSON.parse(init?.body as string) as { document: { v: Array<[string, { v: string }]> } };
      suppliedID = body.document.v.find(([field]) => field === "_id")?.[1]?.v;
      return Response.json({ version: 1, document: encodeDocument({ _id: "00000000000000000000000000000009", title: "wrong" }) }, { status: 201 });
    },
  });
  await assert.rejects(client.collection("todos").insertOne({ title: "owned" }), (error: unknown) =>
    error instanceof MeldbaseInternalError && error.code === "outcome_unknown" && error.operation === `insert ${suppliedID}` && error.cause instanceof Error && /changed document ID/.test(error.cause.message),
  );
  assert.match(suppliedID ?? "", /^[0-9a-f]{32}$/);
  client.close();
});

test("remote insert exposes its assigned ID when transport admission is unknown", async () => {
  const suppliedID = "00000000000000000000000000000001";
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => { throw new Error("connection lost"); },
  });
  await assert.rejects(client.collection("todos").insertOne({ _id: suppliedID, title: "owned" }), (error: unknown) =>
    error instanceof MeldbaseInternalError && error.code === "outcome_unknown" && error.operation === `insert ${suppliedID}` && error.cause instanceof Error && /connection lost/.test(error.cause.message),
  );
  client.close();
});

test("HTTP mutations and RPC calls expose an unknown outcome after a transport loss", async () => {
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => { throw new Error("connection lost"); },
  });
  await assert.rejects(client.collection("todos").updateOne({ _id: "00000000000000000000000000000001" }, { $inc: { attempts: 1 } }), (error: unknown) =>
    error instanceof MeldbaseInternalError && error.code === "outcome_unknown" && error.operation === "updateOne todos" && error.cause instanceof Error && /connection lost/.test(error.cause.message),
  );
  await assert.rejects(client.call("billing.charge", null), (error: unknown) =>
    error instanceof MeldbaseInternalError && error.code === "outcome_unknown" && /^RPC [0-9a-f-]{36}$/.test(error.operation) && error.cause instanceof Error && /connection lost/.test(error.cause.message),
  );
  client.close();
});

test("malformed post-dispatch error envelopes leave write outcomes unknown", async () => {
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({ error: { kind: "internal", code: "untrusted" }, unexpected: true }, { status: 502 }),
  });
  const unknown = (operation: RegExp, message: RegExp) => (error: unknown) =>
    error instanceof MeldbaseInternalError && error.code === "outcome_unknown" && operation.test(error.operation) && error.cause instanceof Error && message.test(error.cause.message);
  await assert.rejects(client.collection("todos").insertOne({ title: "owned" }), unknown(/^insert [0-9a-f]{32}$/, /Malformed insert error response/));
  await assert.rejects(client.collection("todos").updateOne({ _id: "00000000000000000000000000000001" }, { $inc: { attempts: 1 } }), unknown(/^updateOne todos$/, /Malformed mutation error response/));
  await assert.rejects(client.call("billing.charge", null), unknown(/^RPC [0-9a-f-]{36}$/, /Malformed RPC error response/));
  client.close();
});

test("pre-dispatch write failures are not reported as unknown results", async () => {
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    accessToken: () => { throw new Error("token refresh failed"); },
    fetch: async () => { throw new Error("fetch should not run"); },
  });
  await assert.rejects(client.collection("todos").insertOne({ title: "owned" }), (error: unknown) =>
    error instanceof Error && !(error instanceof MeldbaseInternalError) && /token refresh failed/.test(error.message),
  );
  await assert.rejects(client.collection("todos").updateOne({ _id: "00000000000000000000000000000001" }, { $set: { title: "updated" } }), (error: unknown) =>
    error instanceof Error && !(error instanceof MeldbaseInternalError) && /token refresh failed/.test(error.message),
  );
  await assert.rejects(client.call("billing.charge", null), (error: unknown) =>
    error instanceof Error && !(error instanceof MeldbaseInternalError) && /token refresh failed/.test(error.message),
  );
  client.close();
});

test("closed clients reject every new operation", async () => {
  const client = new MeldbaseClient({ baseUrl: "https://db.example", fetch: async () => Response.json({ version: 1, documents: [] }) });
  const todos = client.collection("todos");
  client.close();
  assert.throws(() => client.collection("later"), MeldbaseClientClosedError);
  await assert.rejects(todos.find().fetch(), MeldbaseClientClosedError);
  assert.throws(() => todos.find().subscribe(() => {}), MeldbaseClientClosedError);
	await assert.rejects(client.call("echo", null), MeldbaseClientClosedError);
});

test("RPC call preserves typed values and returns structured safe errors", async () => {
	const requests: Array<{ url: string; init?: RequestInit }> = [];
	let fail = false;
	const fetcher: typeof fetch = async (input, init) => {
		requests.push({ url: String(input), ...(init ? { init } : {}) });
		const call = JSON.parse(init?.body as string) as { requestId: string };
		if (fail) return Response.json({ v: 1, type: "error", requestId: call.requestId, error: { kind: "business", code: "billing.quota_exceeded", data: encodeValue({ retryAfter: 60n }) } }, { status: 400 });
		return Response.json({
			v: 1, type: "result", requestId: call.requestId,
			result: encodeValue({ total: 9223372036854775807n, at: new Date("2026-07-16T00:00:00.000Z"), bytes: new Uint8Array([0, 255]) }),
		});
	};
	const client = new MeldbaseClient({ baseUrl: "https://db.example", fetch: fetcher, accessToken: () => "secret" });
	const idempotencyKey = "abcdefghijklmnopqrstuv";
	const result = await client.call<{ readonly total: bigint; readonly at: Date; readonly bytes: Uint8Array }>(
		"billing.calculate", { amount: 9223372036854775807n, at: new Date("2026-07-16T00:00:00.000Z"), bytes: new Uint8Array([1, 2]) },
		{ idempotencyKey },
	);
	assert.equal(result.total, 9223372036854775807n);
	assert.equal(result.at.toISOString(), "2026-07-16T00:00:00.000Z");
	assert.deepEqual([...result.bytes], [0, 255]);
	assert.equal(requests[0]?.url, "https://db.example/v1/rpc");
	const request = requests[0]?.init;
	assert.equal((request?.headers as Record<string, string>).authorization, "Bearer secret");
	const callEnvelope = JSON.parse(request?.body as string) as Record<string, unknown>;
	assert.equal(typeof callEnvelope.requestId, "string");
	delete callEnvelope.requestId;
	assert.deepEqual(callEnvelope, {
		v: 1, type: "call", idempotencyKey, method: "billing.calculate",
		input: { t: "object", v: [
			["amount", { t: "int64", v: "9223372036854775807" }],
			["at", { t: "date", v: "2026-07-16T00:00:00.000Z" }],
			["bytes", { t: "binary", v: "AQI=" }],
		] },
	});

	fail = true;
	await assert.rejects(client.call("billing.calculate", null), (error: unknown) =>
		error instanceof MeldbaseError && error.code === "billing.quota_exceeded" && error.data?.retryAfter === 60n,
	);
	await assert.rejects(client.call("bad/name", null), /Invalid RPC method name/);
	await assert.rejects(client.call("billing.calculate", null, { idempotencyKey: "too-short" }), /Invalid RPC idempotency key/);
	await assert.rejects(client.call("billing.calculate", undefined as never), /Unsupported value type/);
	await assert.rejects(client.call("billing.calculate", new (class Input {})() as never), /plain object prototype/);
	client.close();
});

test("RPC call preserves generic internal errors raised before its envelope is decoded", async () => {
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({ error: { kind: "internal", code: "unauthenticated" } }, { status: 401 }),
  });
	await assert.rejects(client.call("billing.calculate", null), (error: unknown) =>
    error instanceof MeldbaseInternalError && error.code === "unauthenticated" && error.status === 401 && error.operation === "RPC",
  );
  client.close();
});

test("realtime ticket errors use the same internal error contract", async () => {
  let statusError: Error | undefined;
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({ error: { kind: "internal", code: "unauthenticated" } }, { status: 401 }),
    webSocketFactory: () => { throw new Error("socket must not open"); },
  });
  const unsubscribe = client.collection("todos").find().subscribe(() => {}, { onStatus: (status) => { statusError = status.error; } });
  await settle();
  await settle();
  assert.equal(statusError instanceof MeldbaseInternalError, true);
  assert.equal((statusError as MeldbaseInternalError).code, "unauthenticated");
  assert.equal((statusError as MeldbaseInternalError).operation, "realtime ticket");
  unsubscribe();
  client.close();
});

test("RPC call propagates AbortSignal and marks malformed success responses unknown", async () => {
	const controller = new AbortController();
	const client = new MeldbaseClient({
		baseUrl: "https://db.example",
		fetch: async (_input, init) => {
			assert.equal(init?.signal, controller.signal);
			return Response.json({ v: 1, type: "result", requestId: "wrong", result: encodeValue("ok"), unexpected: true });
		},
	});
	await assert.rejects(client.call("echo", null, { signal: controller.signal }), (error: unknown) =>
		error instanceof MeldbaseInternalError && error.code === "outcome_unknown" && error.cause instanceof Error && /Malformed RPC response/.test(error.cause.message),
	);
	client.close();
});

test("realtime RPC preserves typed values and closes a call-only socket when settled", async () => {
  const sockets: FakeSocket[] = [];
  let ticketRequests = 0;
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async (input) => {
      assert.equal(String(input).endsWith("/v1/realtime/tickets"), true);
      ticketRequests += 1;
      return Response.json({ url: "wss://db.example/realtime", ticket: "ticket", protocol: currentProtocol });
    },
    webSocketFactory: () => { const socket = new FakeSocket(); sockets.push(socket); return socket; },
  });
  const pending = client.call<{ readonly total: bigint; readonly bytes: Uint8Array }>(
    "billing.calculate", 9223372036854775807n, { transport: "realtime", idempotencyKey: "abcdefghijklmnopqrstuv" },
  );
  await settle();
  assert.equal(ticketRequests, 1);
  assert.equal(sockets.length, 1);
  const socket = sockets[0] as FakeSocket;
  socket.open();
  socket.message({ v: 1, type: "authenticated" });
  const call = JSON.parse(socket.sent[1] as string) as Record<string, unknown>;
  assert.equal(call.idempotencyKey, "abcdefghijklmnopqrstuv");
  assert.deepEqual(call.input, { t: "int64", v: "9223372036854775807" });
  socket.message({
    v: 1, type: "result", requestId: call.requestId,
    result: encodeValue({ total: 9223372036854775807n, bytes: new Uint8Array([0, 255]) }),
  });
  const result = await pending;
  assert.equal(result.total, 9223372036854775807n);
  assert.deepEqual([...result.bytes], [0, 255]);
  assert.equal(socket.closed?.reason, "no active work");
  client.close();
});

test("realtime RPC multiplexes with subscriptions, exposes errors, and sends cancellation", async () => {
  const sockets: FakeSocket[] = [];
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({ url: "wss://db.example/realtime", ticket: "ticket", protocol: currentProtocol }),
    webSocketFactory: () => { const socket = new FakeSocket(); sockets.push(socket); return socket; },
  });
  const unsubscribe = client.collection("todos").find().subscribe(() => {});
  await settle();
  const socket = sockets[0] as FakeSocket;
  socket.open();
  socket.message({ v: 1, type: "authenticated" });
  assert.equal((JSON.parse(socket.sent[1] as string) as { type: string }).type, "subscribe");

  const successful = client.call("echo", "ok", { transport: "realtime" });
  const successCall = JSON.parse(socket.sent[2] as string) as Record<string, unknown>;
  socket.message({ v: 1, type: "result", requestId: successCall.requestId, result: encodeValue("ok") });
  assert.equal(await successful, "ok");
  assert.equal(socket.readyState, 1);

  const failed = client.call("fail", null, { transport: "realtime" });
  const failedCall = JSON.parse(socket.sent[3] as string) as Record<string, unknown>;
  socket.message({ v: 1, type: "error", requestId: failedCall.requestId, error: { kind: "business", code: "billing.quota_exceeded" } });
  await assert.rejects(failed, (error: unknown) =>
    error instanceof MeldbaseError && error.code === "billing.quota_exceeded",
  );

  const controller = new AbortController();
  const reason = new Error("caller stopped waiting");
  const canceled = client.call("slow", null, { transport: "realtime", signal: controller.signal });
  const canceledCall = JSON.parse(socket.sent[4] as string) as Record<string, unknown>;
  controller.abort(reason);
  await assert.rejects(canceled, (error: unknown) => error === reason);
  assert.deepEqual(JSON.parse(socket.sent[5] as string), { v: 1, type: "cancel", requestId: canceledCall.requestId });
  assert.equal(sockets.length, 1);

  // Work added after authentication is sent immediately on the same socket.
  const unsubscribeSecond = client.collection("notes").find().subscribe(() => {});
  assert.equal((JSON.parse(socket.sent[6] as string) as { collection: string }).collection, "notes");
  unsubscribeSecond();
  unsubscribe();
  assert.equal(socket.closed?.reason, "no active work");
  client.close();
});

test("realtime RPC disconnect reports unknown result and is never automatically retried", async () => {
  const sockets: FakeSocket[] = [];
  let tickets = 0;
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({ url: "wss://db.example/realtime", ticket: `ticket-${++tickets}`, protocol: currentProtocol }),
    webSocketFactory: () => { const socket = new FakeSocket(); sockets.push(socket); return socket; },
    reconnect: { minDelayMs: 1, maxDelayMs: 1 },
  });
  const pending = client.call("charge", 10n, { transport: "realtime" });
  await settle();
  const socket = sockets[0] as FakeSocket;
  socket.open();
  socket.message({ v: 1, type: "authenticated" });
  const call = JSON.parse(socket.sent[1] as string) as { requestId: string };
  socket.disconnect();
  await assert.rejects(pending, (error: unknown) =>
    error instanceof MeldbaseInternalError && error.code === "outcome_unknown" && error.operation === `RPC ${call.requestId}`,
  );
  await settle();
  await settle();
  assert.equal(tickets, 1);
  assert.equal(sockets.length, 1);
  client.close();
});

test("realtime RPC socket errors fail pending calls without retrying", async () => {
  const sockets: FakeSocket[] = [];
  let tickets = 0;
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({ url: "wss://db.example/realtime", ticket: `ticket-${++tickets}`, protocol: currentProtocol }),
    webSocketFactory: () => { const socket = new FakeSocket(); sockets.push(socket); return socket; },
    reconnect: { minDelayMs: 1, maxDelayMs: 1 },
  });
  const pending = client.call("charge", null, { transport: "realtime" });
  await settle();
  const socket = sockets[0] as FakeSocket;
  socket.open();
  socket.message({ v: 1, type: "authenticated" });
  socket.error();
  await assert.rejects(pending, MeldbaseInternalError);
  await settle();
  await settle();
  assert.equal(socket.closed?.code, 1011);
  assert.equal(tickets, 1);
  assert.equal(sockets.length, 1);
  client.close();
});

test("malformed realtime RPC terminal frames fail the shared connection closed", async () => {
  let socket: FakeSocket | undefined;
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({ url: "wss://db.example/realtime", ticket: "ticket", protocol: currentProtocol }),
    webSocketFactory: () => { socket = new FakeSocket(); return socket; },
  });
  const pending = client.call("echo", null, { transport: "realtime" });
  await settle();
  const active = socket as unknown as FakeSocket;
  active.open();
  active.message({ v: 1, type: "authenticated" });
  const call = JSON.parse(active.sent[1] as string) as Record<string, unknown>;
  active.message({ v: 1, type: "result", requestId: call.requestId, result: encodeValue("ok"), unexpected: true });
  await assert.rejects(pending, /Malformed RPC result/);
  assert.equal(active.closed?.code, 1002);
  client.close();
});

test("ordered deltas apply strictly and listener mutation cannot corrupt client state", async () => {
  let socket: FakeSocket | undefined;
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({ url: "wss://db.example/realtime", ticket: "ticket", protocol: currentProtocol }),
    webSocketFactory: () => { socket = new FakeSocket(); return socket; },
  });
  const snapshots: Document[][] = [];
  client.collection<Document>("todos").find().subscribe((documents) => {
   snapshots.push(documents as Document[]);
    if (snapshots.length === 1) (documents[0] as unknown as Record<string, unknown>).value = "listener-mutated";
  });
  await settle();
  const active = socket as unknown as FakeSocket;
  active.open();
  active.message({ v: 1, type: "authenticated" });
  const subscribe = JSON.parse(active.sent[1] as string) as Record<string, unknown>;
  const id1 = "00000000000000000000000000000001";
  const id2 = "00000000000000000000000000000002";
  const id3 = "00000000000000000000000000000003";
  const id4 = "00000000000000000000000000000004";
  active.message({
    v: 1, type: "snapshot", requestId: subscribe.requestId, subscriptionId: "server", token: "token-1",
    documents: [encodeDocument({ _id: id1, value: "one" }), encodeDocument({ _id: id2, value: "two" }), encodeDocument({ _id: id3, value: "three" })],
  });
  await settle();
  active.message({
    v: 1, type: "delta", requestId: subscribe.requestId, subscriptionId: "server", fromToken: "token-1", token: "token-2",
    operations: [
      { op: "remove", id: id2 },
      { op: "move_before", id: id3, before: id1 },
      { op: "add_before", id: id4, before: id1, document: encodeDocument({ _id: id4, value: "four" }) },
      { op: "change", id: id1, document: encodeDocument({ _id: id1, value: "server-change" }) },
    ],
  });
  await settle();
  assert.deepEqual(snapshots[1]?.map((document) => `${document._id}:${String(document.value)}`), [
    `${id3}:three`, `${id4}:four`, `${id1}:server-change`,
  ]);
  active.message({
    v: 1, type: "delta", requestId: subscribe.requestId, subscriptionId: "server", fromToken: "token-2", token: "token-3",
    operations: [{ op: "change", id: id1, document: encodeDocument({ _id: id1, value: "still-safe" }) }],
  });
  await settle();
  assert.equal(snapshots[2]?.[2]?.value, "still-safe");
  client.close();
});

test("delta token mismatch fails the realtime connection closed", async () => {
  let socket: FakeSocket | undefined;
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({ url: "wss://db.example/realtime", ticket: "ticket", protocol: currentProtocol }),
    webSocketFactory: () => { socket = new FakeSocket(); return socket; },
  });
  client.collection("todos").find().subscribe(() => {});
  await settle();
  const active = socket as unknown as FakeSocket;
  active.open(); active.message({ v: 1, type: "authenticated" });
  const subscribe = JSON.parse(active.sent[1] as string) as Record<string, unknown>;
  active.message({ v: 1, type: "snapshot", requestId: subscribe.requestId, subscriptionId: "server", token: "token-1", documents: [] });
  active.message({
    v: 1, type: "delta", requestId: subscribe.requestId, subscriptionId: "server", fromToken: "wrong-token", token: "token-2",
    operations: [{ op: "add_before", id: "00000000000000000000000000000001", document: encodeDocument({ _id: "00000000000000000000000000000001" }) }],
  });
  await settle();
  assert.equal(active.closed?.code, 1002);
  client.close();
});

test("malformed or oversized deltas fail closed without partial publication", async () => {
  const id1 = "00000000000000000000000000000001";
  const id2 = "00000000000000000000000000000002";
  const id3 = "00000000000000000000000000000003";
  const cases: Array<{ name: string; operations: unknown[]; maxDeltaOperations?: number; initial?: Document[] }> = [
    { name: "missing remove target", operations: [{ op: "remove", id: id2 }] },
    { name: "unknown add anchor", operations: [{ op: "add_before", id: id2, before: id3, document: encodeDocument({ _id: id2 }) }] },
    { name: "change ID mismatch", operations: [{ op: "change", id: id1, document: encodeDocument({ _id: id2 }) }] },
    { name: "no-op move", operations: [{ op: "move_before", id: id1 }] },
    { name: "unknown operation field", operations: [{ op: "change", id: id1, document: encodeDocument({ _id: id1 }), unexpected: true }] },
    {
      name: "operation limit",
      operations: [{ op: "remove", id: id1 }, { op: "remove", id: id2 }],
      maxDeltaOperations: 1,
      initial: [{ _id: id1 }, { _id: id2 }],
    },
  ];

  for (const scenario of cases) {
    let socket: FakeSocket | undefined;
    let publications = 0;
    const client = new MeldbaseClient({
      baseUrl: "https://db.example",
      fetch: async () => Response.json({ url: "wss://db.example/realtime", ticket: "ticket", protocol: currentProtocol }),
      webSocketFactory: () => { socket = new FakeSocket(); return socket; },
      ...(scenario.maxDeltaOperations ? { maxDeltaOperations: scenario.maxDeltaOperations } : {}),
    });
    client.collection("todos").find().subscribe(() => { publications += 1; });
    await settle();
    const active = socket as unknown as FakeSocket;
    active.open();
    active.message({ v: 1, type: "authenticated" });
    const subscribe = JSON.parse(active.sent[1] as string) as Record<string, unknown>;
    active.message({
      v: 1, type: "snapshot", requestId: subscribe.requestId, subscriptionId: "server", token: "token-1",
      documents: (scenario.initial ?? [{ _id: id1 }]).map(encodeDocument),
    });
    await settle();
    active.message({
      v: 1, type: "delta", requestId: subscribe.requestId, subscriptionId: "server", fromToken: "token-1", token: "token-2",
      operations: scenario.operations,
    });
    await settle();
    assert.equal(active.closed?.code, 1002, scenario.name);
    assert.equal(publications, 1, scenario.name);
    client.close();
  }
});

test("malformed or oversized snapshots fail closed", async () => {
  let socket: FakeSocket | undefined;
  const client = new MeldbaseClient({
    baseUrl: "https://db.example",
    fetch: async () => Response.json({ url: "wss://db.example/realtime", ticket: "ticket", protocol: currentProtocol }),
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
    fetch: async () => Response.json({ url: "wss://db.example/realtime", ticket: `ticket-${++ticket}`, protocol: currentProtocol }),
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

  second.message({ v: 1, type: "resumed", requestId: resumed.requestId, subscriptionId: "server-2", token: "opaque.signed-token" });
  await settle();
  assert.equal(states.at(-1), "live");

  second.message({ v: 1, type: "resync_required", requestId: resumed.requestId });
  const clean = JSON.parse(second.sent[2] as string) as Record<string, unknown>;
  assert.equal(clean.type, "subscribe");
  assert.equal("resumeToken" in clean, false);
  unsubscribe();
  client.close();
});

test("discovered server without resume capability reconnects with a clean snapshot", async () => {
	const sockets: FakeSocket[] = [];
	let ticket = 0;
	const client = new MeldbaseClient({
		baseUrl: "https://db.example",
		fetch: async () => Response.json({
			url: "wss://db.example/realtime", ticket: `ticket-${++ticket}`,
			protocol: {
				versions: [1],
				capabilities: ticket === 1
					? ["query.delta", "query.resume", "rpc", "rpc.cancel"]
					: ["query.delta", "rpc", "rpc.cancel"],
			},
		}),
		webSocketFactory: () => { const socket = new FakeSocket(); sockets.push(socket); return socket; },
		reconnect: { minDelayMs: 1, maxDelayMs: 1 },
	});
	const unsubscribe = client.collection("todos").find().subscribe(() => {});
	await settle();
	const first = sockets[0]!;
	first.open();
	first.message({ v: 1, type: "authenticated" });
	const initial = JSON.parse(first.sent[1]!) as Record<string, unknown>;
	first.message({ v: 1, type: "snapshot", requestId: initial.requestId, subscriptionId: "server-1", token: "opaque-1", documents: [] });
	await settle();
	first.disconnect();
	await settle();
	await settle();
	const second = sockets[1]!;
	second.open();
	second.message({ v: 1, type: "authenticated" });
	const clean = JSON.parse(second.sent[1]!) as Record<string, unknown>;
	assert.equal(clean.type, "subscribe");
	assert.equal("resumeToken" in clean, false);
	assert.equal(client.realtimeProtocol?.capabilities.includes("query.resume"), false);
	unsubscribe();
	client.close();
});
