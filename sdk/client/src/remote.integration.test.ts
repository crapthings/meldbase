import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { execFile, spawn } from "node:child_process";
import test from "node:test";
import { promisify } from "node:util";
import { MeldbaseClient, MeldbaseError } from "./index.js";
import type { Document, WebSocketLike } from "./index.js";

const repository = fileURLToPath(new URL("../../../", import.meta.url));
const execFileAsync = promisify(execFile);
const productionSecret = "0123456789abcdef0123456789abcdef";
const workerToken = "0123456789abcdef0123456789abcdef";

type ServerCommand = { readonly args: string[]; readonly env?: Readonly<Record<string, string>> };
type RunningServer = { readonly baseURL: string; readonly workerURL: string | undefined; stop(): Promise<void> };
type RunningWorker = { stop(): Promise<void> };

async function startServer(command: (directory: string) => Promise<ServerCommand>): Promise<RunningServer> {
  const directory = await mkdtemp(`${tmpdir()}/meldbase-client-integration-`);
  const binary = `${directory}/meld`;
  let configuration: ServerCommand;
  try {
    configuration = await command(directory);
    await execFileAsync("go", ["build", "-o", binary, "./cmd/meld"], { cwd: repository });
  } catch (error) {
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
  const child = spawn(binary, configuration.args, {
    cwd: repository,
    env: { ...process.env, ...configuration.env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  child.stdout.setEncoding("utf8");
  const stop = async (): Promise<void> => {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child, "exit");
      child.kill("SIGTERM");
      await exited;
    }
    await rm(directory, { recursive: true, force: true });
  };
  let baseURL: string;
  let workerURL: string | undefined;
  try {
    baseURL = await new Promise<string>((resolve, reject) => {
      let output = "";
      const timer = setTimeout(() => reject(new Error(`Meldbase development server did not start: ${stderr}`)), 15_000);
      child.once("error", (error) => {
        clearTimeout(timer);
        reject(error);
      });
      child.once("exit", (code, signal) => {
        clearTimeout(timer);
        reject(
          new Error(`Meldbase development server exited before startup (code=${code} signal=${signal}): ${stderr}`),
        );
      });
      child.stdout.on("data", (chunk: string) => {
        output += chunk;
        const workerMatch = /^Meldbase worker control listening on (ws:\/\/[^\s]+)/m.exec(output);
        if (workerMatch?.[1]) workerURL = workerMatch[1];
        const match = /^Meldbase listening on http:\/\/([^\s]+)/m.exec(output);
        if (!match?.[1]) return;
        clearTimeout(timer);
        resolve(`http://${match[1]}`);
      });
    });
  } catch (error) {
    await stop();
    throw error;
  }
  return {
    baseURL,
    get workerURL() {
      return workerURL;
    },
    stop,
  };
}

function startDevelopmentServer(): Promise<RunningServer> {
  return startServer(async (directory) => ({
    args: ["serve", "--db", `${directory}/app.meld`, "--addr", "127.0.0.1:0", "--dev-no-auth"],
  }));
}

function startWorkerDevelopmentServer(): Promise<RunningServer> {
  return startServer(async (directory) => ({
    args: [
      "serve",
      "--db",
      `${directory}/app.meld`,
      "--addr",
      "127.0.0.1:0",
      "--dev-no-auth",
      "--worker-addr",
      "127.0.0.1:0",
      "--worker-read-policies",
      "items",
    ],
    env: { MELDBASE_WORKER_TOKEN: workerToken },
  }));
}

function signedProductionJWT(workspace: string): string {
  const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const unsigned = `${encode({ alg: "HS256", typ: "JWT" })}.${encode({
    iss: "https://identity.example/",
    aud: "app-api",
    sub: "integration-user",
    workspace_id: workspace,
    exp: Math.floor(Date.now() / 1000) + 60,
  })}`;
  return `${unsigned}.${createHmac("sha256", productionSecret).update(unsigned).digest("base64url")}`;
}

function startProductionServer(): Promise<RunningServer> {
  return startServer(async (directory) => {
    const secretPath = `${directory}/jwt.secret`;
    const policyPath = `${directory}/access-policy.json`;
    await writeFile(secretPath, productionSecret, { mode: 0o600 });
    await writeFile(
      policyPath,
      JSON.stringify({
        version: 1,
        workspaceField: "workspaceId",
        collections: [{ collection: "todos", mode: "collaborative" }],
      }),
      { mode: 0o600 },
    );
    return {
      args: [
        "serve",
        "--db",
        `${directory}/app.meld`,
        "--addr",
        "127.0.0.1:0",
        "--jwt-hs256-secret-file",
        secretPath,
        "--jwt-issuer",
        "https://identity.example/",
        "--jwt-audience",
        "app-api",
        "--access-policy-file",
        policyPath,
      ],
    };
  });
}

async function waitFor(predicate: () => boolean, description: string): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${description}`);
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
}

async function startSDKWorker(server: RunningServer): Promise<RunningWorker> {
  await waitFor(() => server.workerURL !== undefined, "the worker control endpoint");
  await execFileAsync("pnpm", ["--filter", "@meldbase/worker", "build"], { cwd: repository });
  const script = fileURLToPath(new URL("../../worker/test/worker-hub-e2e.mjs", import.meta.url));
  const child = spawn(process.execPath, [script], {
    cwd: repository,
    env: { ...process.env, MELDBASE_WORKER_URL: server.workerURL!, MELDBASE_WORKER_TOKEN: workerToken },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    output += chunk;
  });
  child.stderr.on("data", (chunk: string) => {
    output += chunk;
  });
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Meldbase SDK worker did not start: ${output}`)), 15_000);
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("exit", (code, signal) => {
      clearTimeout(timer);
      reject(new Error(`Meldbase SDK worker exited before startup (code=${code} signal=${signal}): ${output}`));
    });
    child.stdout.on("data", () => {
      if (!output.includes("ready\n")) return;
      clearTimeout(timer);
      resolve();
    });
  });
  return {
    async stop(): Promise<void> {
      if (child.exitCode !== null || child.signalCode !== null) return;
      const exited = once(child, "exit");
      child.kill("SIGTERM");
      await exited;
    },
  };
}

test(
  "TypeScript remote client interoperates with the live Go HTTP and realtime server",
  { timeout: 30_000 },
  async () => {
    const server = await startDevelopmentServer();
    const client = new MeldbaseClient({
      baseUrl: server.baseURL,
      webSocketFactory: (url) => new WebSocket(url) as unknown as WebSocketLike,
    });
    try {
      const todos = client.collection<Document>("todos");
      const snapshots: string[][] = [];
      const states: string[] = [];
      const unsubscribe = todos.find({ done: false }).subscribe(
        (documents) => {
          snapshots.push(documents.map((document) => document._id));
        },
        {
          onStatus: (status) => {
            states.push(status.error ? `${status.state}:${status.error.message}` : status.state);
          },
        },
      );
      try {
        await waitFor(() => snapshots.length > 0, "the initial realtime snapshot");
      } catch (error) {
        throw new Error(`${(error as Error).message}; states=${JSON.stringify(states)}`);
      }

      const first = await todos.insertOne({ title: "first", done: false });
      await waitFor(() => snapshots.some((snapshot) => snapshot.includes(first._id)), "the first realtime delta");
      const second = await todos.insertOne({ title: "second", done: false });
      await waitFor(
        () => snapshots.some((snapshot) => snapshot.includes(first._id) && snapshot.includes(second._id)),
        "the second realtime delta",
      );

      const fetched = await todos.find({ done: false }).fetch();
      assert.deepEqual(fetched.map((document) => document._id).sort(), [first._id, second._id].sort());

      const pages = client.collection<Document>("pages");
      const pageDocuments = [
        { _id: "00000000000000000000000000000003" },
        { _id: "00000000000000000000000000000002", rank: 1 },
        { _id: "00000000000000000000000000000001" },
        { _id: "00000000000000000000000000000004", rank: 1 },
        { _id: "00000000000000000000000000000005", rank: 2 },
      ];
      for (const document of pageDocuments) await pages.insertOne(document);
      const pagedIDs: string[] = [];
      let after: string | undefined;
      do {
        const page = await pages
          .find({}, { sort: [{ path: "rank", direction: 1 }], first: 2, ...(after ? { after } : {}) })
          .fetchPage();
        pagedIDs.push(...page.documents.map((document) => document._id));
        after = page.nextCursor;
      } while (after !== undefined && pagedIDs.length < pageDocuments.length);
      assert.deepEqual(pagedIDs, [
        "00000000000000000000000000000001",
        "00000000000000000000000000000003",
        "00000000000000000000000000000002",
        "00000000000000000000000000000004",
        "00000000000000000000000000000005",
      ]);

      assert.deepEqual(await todos.count({ done: false }), { count: 2, capped: false });
      assert.deepEqual(await todos.groupCount({ done: false }, "done"), {
        groups: [{ key: false, count: 2 }],
        capped: false,
      });
      assert.equal(client.realtimeProtocol?.capabilities.includes("query.delta"), true);
      unsubscribe();
    } finally {
      client.close();
      await server.stop();
    }
  },
);

test(
  "TypeScript remote mutations and RPC interoperate with live Go and Node worker servers",
  { timeout: 30_000 },
  async () => {
    const server = await startWorkerDevelopmentServer();
    let worker: RunningWorker | undefined;
    const client = new MeldbaseClient({
      baseUrl: server.baseURL,
      webSocketFactory: (url) => new WebSocket(url) as unknown as WebSocketLike,
    });
    try {
      worker = await startSDKWorker(server);
      const todos = client.collection<Document>("todos");
      const inserted = await todos.insertOne({ title: "pending", done: false });
      assert.deepEqual(await todos.updateOne({ _id: inserted._id }, { $set: { done: true, title: "complete" } }), {
        matchedCount: 1,
        modifiedCount: 1,
      });
      const updated = await todos.find({ _id: inserted._id }).fetch();
      assert.equal(updated.length, 1);
      assert.equal(updated[0]?._id, inserted._id);
      assert.equal(updated[0]?.title, "complete");
      assert.equal(updated[0]?.done, true);
      assert.deepEqual(await todos.deleteOne({ _id: inserted._id }), { deletedCount: 1 });
      assert.deepEqual(await todos.find({ _id: inserted._id }).fetch(), []);

      assert.equal(await client.call("sdk.echo", 42n), 42n);
      assert.equal(await client.call("sdk.echo", 7n, { transport: "realtime" }), 7n);
      await assert.rejects(
        client.call("sdk.reject", null),
        (error: unknown) =>
          error instanceof MeldbaseError && error.code === "orders.already_paid" && error.data?.retryAfter === 60n,
      );
      await assert.rejects(
        client.call("sdk.reject", null, { transport: "realtime" }),
        (error: unknown) =>
          error instanceof MeldbaseError && error.code === "orders.already_paid" && error.data?.retryAfter === 60n,
      );
    } finally {
      client.close();
      await worker?.stop();
      await server.stop();
    }
  },
);

test(
  "TypeScript client preserves JWT workspace isolation against the live Go server",
  { timeout: 30_000 },
  async () => {
    const server = await startProductionServer();
    const clientA = new MeldbaseClient({
      baseUrl: server.baseURL,
      accessToken: () => signedProductionJWT("team-a"),
      webSocketFactory: (url) => new WebSocket(url) as unknown as WebSocketLike,
    });
    const clientB = new MeldbaseClient({
      baseUrl: server.baseURL,
      accessToken: () => signedProductionJWT("team-b"),
      webSocketFactory: (url) => new WebSocket(url) as unknown as WebSocketLike,
    });
    try {
      const todosA = clientA.collection<Document>("todos");
      const todosB = clientB.collection<Document>("todos");
      const snapshotsA: string[][] = [];
      const snapshotsB: string[][] = [];
      const unsubscribeA = todosA.find().subscribe((documents) => {
        snapshotsA.push(documents.map((document) => document._id));
      });
      const unsubscribeB = todosB.find().subscribe((documents) => {
        snapshotsB.push(documents.map((document) => document._id));
      });
      await waitFor(() => snapshotsA.length > 0 && snapshotsB.length > 0, "initial workspace snapshots");
      assert.deepEqual(snapshotsA[0], []);
      assert.deepEqual(snapshotsB[0], []);

      const inserted = await todosA.insertOne({ title: "team-a only", done: false });
      await waitFor(() => snapshotsA.some((snapshot) => snapshot.includes(inserted._id)), "the scoped realtime delta");
      await new Promise<void>((resolve) => setTimeout(resolve, 50));
      assert.equal(
        snapshotsB.every((snapshot) => snapshot.length === 0),
        true,
      );
      assert.deepEqual(await todosB.find().fetch(), []);

      unsubscribeA();
      unsubscribeB();
    } finally {
      clientA.close();
      clientB.close();
      await server.stop();
    }
  },
);
