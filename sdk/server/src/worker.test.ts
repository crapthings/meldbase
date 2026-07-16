import assert from "node:assert/strict";
import test from "node:test";

import { compileQuery, compileUpdate } from "@meldbase/client";
import { MeldbaseMethodError, MeldbaseWorker, MeldbaseWorkerProtocolError, publish, rpc, transactional } from "./worker.js";
import type { WorkerSocket, WorkerSocketFactory } from "./worker.js";

class FakeSocket implements WorkerSocket {
  readyState = 0;
  readonly sent: string[] = [];
  readonly listeners = new Map<string, Set<(event: any) => void>>();

  send(data: string): void {
    if (this.readyState !== 1) throw new Error("socket is not open");
    this.sent.push(data);
  }

  close(code = 1000, reason = ""): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.emit("close", { code, reason });
  }

  addEventListener(type: string, listener: (event: any) => void): void {
    let listeners = this.listeners.get(type);
    if (!listeners) this.listeners.set(type, listeners = new Set());
    listeners.add(listener);
  }

  removeEventListener(type: string, listener: (event: any) => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  open(): void {
    this.readyState = 1;
    this.emit("open", {});
  }

  message(value: unknown): void {
    this.emit("message", { data: JSON.stringify(value) });
  }

  emit(type: string, event: any): void {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}

function harness(methods: NonNullable<ConstructorParameters<typeof MeldbaseWorker>[0]["methods"]>, requireProtocol = false) {
  const sockets: FakeSocket[] = [];
  const factoryCalls: Array<{ url: string; headers: Readonly<Record<string, string>> }> = [];
  const factory: WorkerSocketFactory = (url, options) => {
    factoryCalls.push({ url, headers: options.headers });
    const socket = new FakeSocket();
    sockets.push(socket);
    return socket;
  };
  const states: string[] = [];
  const worker = new MeldbaseWorker({
    url: "wss://control.example.test/v1/workers",
    token: "worker-control-token-0123456789abcdef",
    workerId: "orders-worker",
    methods,
		requireProtocol,
    webSocketFactory: factory,
    reconnectMinMs: 10,
    reconnectMaxMs: 20,
    onStateChange: (state) => states.push(state),
  });
  return { worker, sockets, factoryCalls, states };
}

async function startHarness(value: ReturnType<typeof harness>): Promise<FakeSocket> {
  const started = value.worker.start();
  await waitFor(() => value.sockets.length === 1);
  const socket = value.sockets[0]!;
  socket.open();
  await waitFor(() => socket.sent.length === 1);
  const registration = JSON.parse(socket.sent.shift()!);
  assert.equal(registration.v, 1);
  assert.equal(registration.type, "register");
  assert.equal(registration.workerId, "orders-worker");
  assert.equal(Array.isArray(registration.methods) && registration.methods.length > 0, true);
  socket.message({ v: 1, type: "registered", sessionId: "session-1", limits: { maxPendingCalls: 64, maxOperationsPerCall: 256 } });
  await started;
  return socket;
}

test("server worker registers without URL credentials and handles typed RPC", async () => {
  const value = harness({
    "math.echo": rpc((context, arguments_) => {
      assert.deepEqual(context.principal, { subject: "user-1", tenant: "tenant-a" });
      return arguments_[0]!;
    }),
    "orders.reject": rpc(() => { throw new MeldbaseMethodError("not_ready"); }),
  });
  const socket = await startHarness(value);
  assert.equal(value.factoryCalls[0]!.url, "wss://control.example.test/v1/workers");
  assert.equal(value.factoryCalls[0]!.headers.authorization, "Bearer worker-control-token-0123456789abcdef");
	assert.equal(value.factoryCalls[0]!.headers["meldbase-protocol"], "capabilities-v1");
  assert.equal(value.factoryCalls[0]!.url.includes("token"), false);

  socket.message({
    v: 1, type: "invoke", callId: "call-1", method: "math.echo", mode: "rpc",
    principal: { subject: "user-1", tenant: "tenant-a" }, arguments: [{ t: "int64", v: "42" }],
  });
  await waitFor(() => socket.sent.length === 1);
  assert.deepEqual(JSON.parse(socket.sent.shift()!), {
    v: 1, type: "result", callId: "call-1", result: { t: "int64", v: "42" },
  });

  socket.message({
    v: 1, type: "invoke", callId: "call-2", method: "orders.reject", mode: "rpc",
    principal: { subject: "user-1", tenant: "" }, arguments: [],
  });
  await waitFor(() => socket.sent.length === 1);
  assert.deepEqual(JSON.parse(socket.sent.shift()!), {
    v: 1, type: "error", callId: "call-2", error: { code: "not_ready" },
  });
  await value.worker.stop();
  assert.equal(value.worker.state, "stopped");
});

test("worker capability discovery accepts additive features and rejects missing required support once", async () => {
	const compatible = harness({ echo: rpc(() => "ok") });
	const started = compatible.worker.start();
	await waitFor(() => compatible.sockets.length === 1);
	const socket = compatible.sockets[0]!;
	socket.open();
	await waitFor(() => socket.sent.length === 1);
	socket.sent.shift();
	socket.message({
		v: 1, type: "registered", sessionId: "capability-session", limits: {},
		protocol: { versions: [1, 2], capabilities: ["cancel", "rpc", "rpc.future_extension"] },
	});
	await started;
	assert.deepEqual(compatible.worker.protocol, {
		versions: [1, 2], capabilities: ["cancel", "rpc", "rpc.future_extension"],
	});
	await compatible.worker.stop();

	const incompatible = harness({ echo: rpc(() => "ok") });
	const rejected = incompatible.worker.start();
	await waitFor(() => incompatible.sockets.length === 1);
	const missing = incompatible.sockets[0]!;
	missing.open();
	await waitFor(() => missing.sent.length === 1);
	missing.sent.shift();
	missing.message({
		v: 1, type: "registered", sessionId: "missing-session", limits: {},
		protocol: { versions: [1], capabilities: ["cancel"] },
	});
	await assert.rejects(rejected, (error: unknown) =>
		error instanceof MeldbaseWorkerProtocolError && error.required.length === 1 && error.required[0] === "rpc",
	);
	await waitFor(() => incompatible.worker.state === "stopped");
	await new Promise((resolve) => setTimeout(resolve, 30));
	assert.equal(incompatible.sockets.length, 1);

	const oldTransactions = harness({ mutate: transactional(async () => null) });
	const oldTransactionStart = oldTransactions.worker.start();
	await waitFor(() => oldTransactions.sockets.length === 1);
	const oldTransactionSocket = oldTransactions.sockets[0]!;
	oldTransactionSocket.open();
	await waitFor(() => oldTransactionSocket.sent.length === 1);
	oldTransactionSocket.sent.shift();
	oldTransactionSocket.message({
		v: 1, type: "registered", sessionId: "old-transaction-session", limits: {},
		protocol: { versions: [1], capabilities: [
			"cancel", "rpc", "rpc.transactional", "transaction.invalidate_publication", "transaction.point_operations",
		] },
	});
	await assert.rejects(oldTransactionStart, (error: unknown) =>
		error instanceof MeldbaseWorkerProtocolError && error.required.length === 1 && error.required[0] === "transaction.compiled_update",
	);
	await waitFor(() => oldTransactions.worker.state === "stopped");

	const downgrade = harness({ echo: rpc(() => "ok") }, true);
	const downgradeStart = downgrade.worker.start();
	await waitFor(() => downgrade.sockets.length === 1);
	const legacy = downgrade.sockets[0]!;
	legacy.open();
	await waitFor(() => legacy.sent.length === 1);
	legacy.sent.shift();
	legacy.message({ v: 1, type: "registered", sessionId: "legacy-session", limits: {} });
	await assert.rejects(downgradeStart, (error: unknown) =>
		error instanceof MeldbaseWorkerProtocolError && error.required[0] === "protocol.discovery",
	);
	await waitFor(() => downgrade.worker.state === "stopped");
});

test("transactional worker serializes point operations and returns one result", async () => {
  const documentID = "00112233445566778899aabbccddeeff";
  const value = harness({
    "orders.create": transactional(async (_context, _arguments, tx) => {
      const id = await tx.insert("orders", { status: "created" });
      const document = await tx.get("orders", id);
      await tx.replace("orders", id, { status: "confirmed" });
      await tx.update("orders", id, compileUpdate({ $set: { status: "paid" }, $inc: { attempts: 1n } }));
      await tx.invalidatePublication("orders");
      assert.equal(document.status, "created");
      return id;
    }),
  });
  const socket = await startHarness(value);
  socket.message({
    v: 1, type: "invoke", callId: "call-tx", method: "orders.create", mode: "transactional",
    principal: { subject: "user-1", tenant: "" }, arguments: [],
  });

  await waitFor(() => socket.sent.length === 1);
  const insert = JSON.parse(socket.sent.shift()!);
  assert.equal(insert.type, "tx_op");
  assert.equal(insert.operation, "insert");
  assert.deepEqual(insert.document, { t: "object", v: [["status", { t: "string", v: "created" }]] });
  socket.message({ v: 1, type: "tx_result", callId: "call-tx", opId: insert.opId, result: { t: "id", v: documentID } });

  await waitFor(() => socket.sent.length === 1);
  const get = JSON.parse(socket.sent.shift()!);
  assert.equal(get.operation, "get");
  assert.equal(get.id, documentID);
  socket.message({
    v: 1, type: "tx_result", callId: "call-tx", opId: get.opId,
    result: { t: "object", v: [["_id", { t: "id", v: documentID }], ["status", { t: "string", v: "created" }]] },
  });

  await waitFor(() => socket.sent.length === 1);
  const replace = JSON.parse(socket.sent.shift()!);
  assert.equal(replace.operation, "replace");
  socket.message({ v: 1, type: "tx_result", callId: "call-tx", opId: replace.opId, result: { t: "null" } });

  await waitFor(() => socket.sent.length === 1);
  const update = JSON.parse(socket.sent.shift()!);
  assert.equal(update.operation, "update");
  assert.equal(update.id, documentID);
  assert.deepEqual(update.mutation, {
    version: 1,
    operations: [
      { op: "inc", path: "attempts", value: { t: "int64", v: "1" } },
      { op: "set", path: "status", value: { t: "string", v: "paid" } },
    ],
  });
  socket.message({ v: 1, type: "tx_result", callId: "call-tx", opId: update.opId, result: { t: "null" } });

  await waitFor(() => socket.sent.length === 1);
  const invalidation = JSON.parse(socket.sent.shift()!);
  assert.equal(invalidation.operation, "invalidate_publication");
  assert.equal(invalidation.collection, "orders");
  assert.equal("id" in invalidation, false);
  socket.message({ v: 1, type: "tx_result", callId: "call-tx", opId: invalidation.opId, result: { t: "null" } });

  await waitFor(() => socket.sent.length === 1);
  assert.deepEqual(JSON.parse(socket.sent.shift()!), {
    v: 1, type: "result", callId: "call-tx", result: { t: "string", v: documentID },
  });
  await value.worker.stop();
});

test("worker cancellation aborts handler and reconnect re-registers", async () => {
  let aborted = false;
  const value = harness({
    slow: rpc(async ({ signal }) => {
      await new Promise<void>((resolve) => signal.addEventListener("abort", () => { aborted = true; resolve(); }, { once: true }));
      return "late";
    }),
  });
  const first = await startHarness(value);
  first.message({
    v: 1, type: "invoke", callId: "slow-call", method: "slow", mode: "rpc",
    principal: { subject: "user-1", tenant: "" }, arguments: [],
  });
  first.message({ v: 1, type: "cancel", callId: "slow-call" });
  await waitFor(() => aborted);
  assert.equal(first.sent.length, 0);
  first.close(1006, "network lost");
  await waitFor(() => value.sockets.length === 2);
  const second = value.sockets[1]!;
  second.open();
  await waitFor(() => second.sent.length === 1);
  const registration = JSON.parse(second.sent.shift()!);
  assert.equal(registration.type, "register");
  second.message({ v: 1, type: "registered", sessionId: "session-2", limits: { maxPendingCalls: 64, maxOperationsPerCall: 256 } });
  await waitFor(() => value.worker.state === "ready");
  await value.worker.stop();
});

test("publication returns only a data constraint and static visibility declaration", async () => {
  const sockets: FakeSocket[] = [];
  const queryPaths = ["status"];
  const worker = new MeldbaseWorker({
    url: "wss://control.example.test/v1/workers",
    token: "worker-control-token-0123456789abcdef",
    workerId: "publication-worker",
    publications: {
      orders: publish({ version: "orders-v1", maxResults: 50, queryPaths, resultFields: ["status", "description"] }, ({ principal, query }) => {
        assert.equal(query.where.op, "true");
        if (principal.tenant === "blocked") return null;
        assert.equal(principal.tenant, "tenant-a");
        return compileQuery({ tenant: principal.tenant });
      }),
    },
    webSocketFactory: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
    reconnectMinMs: 10,
    reconnectMaxMs: 10,
  });
  queryPaths.push("unsafe-after-construction");
  const started = worker.start();
  await waitFor(() => sockets.length === 1);
  const socket = sockets[0]!;
  socket.open();
  await waitFor(() => socket.sent.length === 1);
  const registration = JSON.parse(socket.sent.shift()!);
  assert.deepEqual(registration.methods, []);
  assert.deepEqual(registration.publications, [{
    collection: "orders", version: "orders-v1", maxResults: 50,
    queryPaths: ["status"], resultFields: ["status", "description"],
  }]);
  socket.message({ v: 1, type: "registered", sessionId: "publication-session", limits: {} });
  await started;
  socket.message({
    v: 1, type: "authorize_query", callId: "policy-1", collection: "orders",
    principal: { subject: "user-1", tenant: "tenant-a" }, query: { version: 1, where: { op: "true" } },
  });
  await waitFor(() => socket.sent.length === 1);
  assert.deepEqual(JSON.parse(socket.sent.shift()!), {
    v: 1, type: "policy", callId: "policy-1",
    constraint: { version: 1, where: { op: "compare", cmp: "eq", path: "tenant", value: { t: "string", v: "tenant-a" } } },
  });
  socket.message({
    v: 1, type: "authorize_query", callId: "policy-2", collection: "orders",
    principal: { subject: "user-2", tenant: "blocked" }, query: { version: 1, where: { op: "true" } },
  });
  await waitFor(() => socket.sent.length === 1);
  assert.deepEqual(JSON.parse(socket.sent.shift()!), {
    v: 1, type: "policy_error", callId: "policy-2", error: { code: "forbidden" },
  });
  await worker.stop();
});

test("worker rejects unsafe configuration and protocol frames", async () => {
  assert.throws(() => harness({ "bad/name": rpc(() => null) }), /Invalid worker method/);
  assert.throws(() => new MeldbaseMethodError("Bad-Code"), /Invalid/);
  assert.throws(() => publish({ version: "bad", maxResults: 0, queryPaths: "*", resultFields: "*" }, () => compileQuery({})), /maxResults/);
  assert.throws(() => publish({ version: "bad", maxResults: 1, queryPaths: ["bad\0path"], resultFields: "*" }, () => compileQuery({})), /NUL/);
  const value = harness({ ok: rpc(() => null) });
  const errors: Error[] = [];
  const configured = new MeldbaseWorker({
    url: "wss://control.example.test/v1/workers",
    token: "worker-control-token-0123456789abcdef",
    workerId: "safe-worker",
    methods: { ok: rpc(() => null) },
    webSocketFactory: (_url, _options) => {
      const socket = new FakeSocket();
      value.sockets.push(socket);
      return socket;
    },
    onError: (error) => errors.push(error),
    reconnectMinMs: 10,
    reconnectMaxMs: 10,
  });
  const started = configured.start();
  await waitFor(() => value.sockets.length === 1);
  const socket = value.sockets[0]!;
  socket.open();
  await waitFor(() => socket.sent.length === 1);
  socket.sent.shift();
  socket.message({ v: 1, type: "registered", sessionId: "session", limits: {} });
  await started;
  socket.message({ v: 1, type: "unknown" });
  await waitFor(() => errors.length > 0);
  await configured.stop();
});

test("registration rejection is observable while the worker reconnects", async () => {
  const value = harness({ ok: rpc(() => null) });
  const errors: Error[] = [];
  const worker = new MeldbaseWorker({
    url: "wss://control.example.test/v1/workers",
    token: "worker-control-token-0123456789abcdef",
    workerId: "rejected-worker",
    methods: { ok: rpc(() => null) },
    webSocketFactory: (_url, _options) => {
      const socket = new FakeSocket();
      value.sockets.push(socket);
      return socket;
    },
    onError: (error) => errors.push(error),
    reconnectMinMs: 10,
    reconnectMaxMs: 10,
  });
  const started = worker.start();
  void started.catch(() => undefined);
  await waitFor(() => value.sockets.length === 1);
  const rejected = value.sockets[0]!;
  rejected.open();
  await waitFor(() => rejected.sent.length === 1);
  rejected.close(1008, "registration rejected");
  await waitFor(() => errors.length === 1 && value.sockets.length === 2);
  assert.match(errors[0]!.message, /registration rejected/);
  await worker.stop();
});

async function waitFor(predicate: () => boolean, timeoutMs = 1_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for worker state");
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}
