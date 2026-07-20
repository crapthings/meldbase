import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { execFile, spawn } from "node:child_process";
import test from "node:test";
import { promisify } from "node:util";
import { MeldbaseClient } from "./index.js";
import type { Document, WebSocketLike } from "./index.js";

const repository = fileURLToPath(new URL("../../../", import.meta.url));
const execFileAsync = promisify(execFile);

async function startDevelopmentServer(): Promise<{ readonly baseURL: string; stop(): Promise<void> }> {
  const directory = await mkdtemp(`${tmpdir()}/meldbase-client-integration-`);
  const binary = `${directory}/meld`;
  try {
    await execFileAsync("go", ["build", "-o", binary, "./cmd/meld"], { cwd: repository });
  } catch (error) {
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
  const child = spawn(binary, ["serve", "--db", `${directory}/app.meld`, "--addr", "127.0.0.1:0", "--dev-no-auth"], {
    cwd: repository,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk: string) => { stderr += chunk; });
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
        reject(new Error(`Meldbase development server exited before startup (code=${code} signal=${signal}): ${stderr}`));
      });
      child.stdout.on("data", (chunk: string) => {
        output += chunk;
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
    stop,
  };
}

async function waitFor(predicate: () => boolean, description: string): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${description}`);
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
}

test("TypeScript remote client interoperates with the live Go HTTP and realtime server", { timeout: 30_000 }, async () => {
  const server = await startDevelopmentServer();
  const client = new MeldbaseClient({
    baseUrl: server.baseURL,
    webSocketFactory: (url) => new WebSocket(url) as unknown as WebSocketLike,
  });
  try {
    const todos = client.collection<Document>("todos");
    const snapshots: string[][] = [];
    const states: string[] = [];
    const unsubscribe = todos.find({ done: false }).subscribe((documents) => {
      snapshots.push(documents.map((document) => document._id));
    }, { onStatus: (status) => { states.push(status.error ? `${status.state}:${status.error.message}` : status.state); } });
    try {
      await waitFor(() => snapshots.length > 0, "the initial realtime snapshot");
    } catch (error) {
      throw new Error(`${(error as Error).message}; states=${JSON.stringify(states)}`);
    }

    const first = await todos.insertOne({ title: "first", done: false });
    await waitFor(() => snapshots.some((snapshot) => snapshot.includes(first._id)), "the first realtime delta");
    const second = await todos.insertOne({ title: "second", done: false });
    await waitFor(() => snapshots.some((snapshot) => snapshot.includes(first._id) && snapshot.includes(second._id)), "the second realtime delta");

    const fetched = await todos.find({ done: false }).fetch();
    assert.deepEqual(fetched.map((document) => document._id).sort(), [first._id, second._id].sort());
    assert.equal(client.realtimeProtocol?.capabilities.includes("query.delta"), true);
    unsubscribe();
  } finally {
    client.close();
    await server.stop();
  }
});
