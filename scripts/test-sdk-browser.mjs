import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";
import {
  launchChrome,
  createChromeTarget,
  availablePort,
  waitFor,
  stopProcess,
  DevTools,
} from "./lib/browser-test.mjs";

// Locates the repository used to start the browser test fixture.
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
// Identifies the document returned by the simulated realtime server.
const documentID = "00000000000000000000000000000001";
// Creates an isolated temporary Chrome profile for the browser test.
const profile = mkdtempSync(join(tmpdir(), "meldbase-browser-profile-"));
// Retains the simulated API server for cleanup.
let api;
// Retains the Vite process for cleanup.
let vite;
// Retains the Chrome process for cleanup.
let chrome;
// Retains the browser debugging connection for cleanup.
let devtools;

// Checks realtime delivery and React hydration in Chrome and releases the test resources.
async function main() {
  try {
    api = await startRealtimeServer();
    const vitePort = await availablePort();
    const viteURL = `http://127.0.0.1:${vitePort}`;
    vite = startVite(vitePort, api.url);
    await waitFor(async () => {
      const response = await fetch(`${viteURL}/hydrate.html`).catch(() => undefined);
      return response?.ok === true;
    }, "Vite hydration fixture did not start");

    const debugPort = await availablePort();
    chrome = launchChrome(debugPort, profile);
    const version = await waitFor(async () => {
      const response = await fetch(`http://127.0.0.1:${debugPort}/json/version`).catch(() => undefined);
      return response?.ok ? response.json() : undefined;
    }, "Chrome remote debugging did not start");
    assert.equal(typeof version.webSocketDebuggerUrl, "string", "Chrome did not expose a debugger endpoint");

    const target = await createChromeTarget(debugPort, "about:blank");
    devtools = await DevTools.connect(target.webSocketDebuggerUrl);
    const consoleErrors = [];
    devtools.on("Runtime.consoleAPICalled", (event) => {
      if (event.type === "error")
        consoleErrors.push(event.args.map((arg) => arg.value ?? arg.description ?? "").join(" "));
    });
    devtools.on("Log.entryAdded", (event) => {
      if (event.entry.level === "error") consoleErrors.push(event.entry.text);
    });
    await devtools.send("Runtime.enable");
    await devtools.send("Page.enable");
    await devtools.send("Log.enable");
    await devtools.send("Page.navigate", { url: `${viteURL}/hydrate.html` });

    let view;
    try {
      view = await waitFor(async () => {
        const result = await devtools.evaluate(browserViewExpression());
        if (typeof result !== "string") return undefined;
        const parsed = JSON.parse(result);
        return parsed.status === "live" && parsed.todos.length === 1 && parsed.todos[0] === "live" ? parsed : undefined;
      }, "Browser did not hydrate and receive the realtime snapshot");
    } catch (error) {
      const diagnostic = await devtools.evaluate(browserViewExpression()).catch(() => "unavailable");
      throw new Error(
        `${error.message}; DOM=${diagnostic}; frames=${JSON.stringify(api.frames)}; console=${consoleErrors.join(" | ")}`,
      );
    }

    assert.deepEqual(view, { status: "live", todos: ["live"], title: "Meldbase hydration compatibility" });
    assert.deepEqual(consoleErrors, [], `Browser reported console errors: ${consoleErrors.join(" | ")}`);
    console.log("verified real-browser HTTP ticket, realtime snapshot, and React hydration");
  } finally {
    await devtools?.close();
    await stopProcess(chrome);
    await stopProcess(vite);
    await api?.close();
    rmSync(profile, { recursive: true, force: true });
  }
}

// Starts the example’s local Vite server against the simulated API endpoint.
function startVite(port, baseURL) {
  const child = spawn(
    process.execPath,
    [
      join(root, "examples/realtime-todos/node_modules/vite/bin/vite.js"),
      "--host",
      "127.0.0.1",
      "--port",
      String(port),
      "--strictPort",
    ],
    {
      cwd: join(root, "examples/realtime-todos"),
      env: { ...process.env, VITE_MELDBASE_URL: baseURL },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  child.stderr.on("data", () => {});
  return child;
}

// Starts a simulated ticket and WebSocket server that returns one query snapshot.
async function startRealtimeServer() {
  const server = createServer((request, response) => {
    response.setHeader("access-control-allow-origin", "*");
    response.setHeader("access-control-allow-methods", "POST, OPTIONS");
    response.setHeader("access-control-allow-headers", "content-type, authorization, accept");
    if (request.method === "OPTIONS") {
      response.writeHead(204).end();
      return;
    }
    if (request.method !== "POST" || request.url !== "/v1/realtime/tickets") {
      response.writeHead(404).end();
      return;
    }
    const address = server.address();
    assert(address && typeof address === "object");
    response.setHeader("content-type", "application/json");
    response.end(
      JSON.stringify({
        url: `ws://127.0.0.1:${address.port}/realtime`,
        ticket: "browser-compatibility-ticket",
        protocol: {
          versions: [1],
          capabilities: ["query.delta", "query.resume", "rpc", "rpc.cancel", "rpc.idempotency"],
        },
      }),
    );
  });
  const sockets = new Set();
  const frames = [];
  const websocket = new WebSocketServer({ noServer: true });
  server.on("upgrade", (request, socket, head) => {
    if (new URL(request.url ?? "/", "http://127.0.0.1").pathname !== "/realtime") {
      socket.destroy();
      return;
    }
    websocket.handleUpgrade(request, socket, head, (connection) => websocket.emit("connection", connection, request));
  });
  websocket.on("connection", (connection) => {
    sockets.add(connection);
    connection.on("close", () => sockets.delete(connection));
    connection.on("message", (raw) => {
      const frame = JSON.parse(raw.toString());
      frames.push(frame);
      if (frame.type === "authenticate") {
        connection.send(JSON.stringify({ v: 1, type: "authenticated" }));
        return;
      }
      if (frame.type !== "subscribe") return;
      connection.send(
        JSON.stringify({
          v: 1,
          type: "snapshot",
          requestId: frame.requestId,
          subscriptionId: "browser-subscription",
          token: "browser-token-1",
          documents: [
            {
              t: "object",
              v: [
                ["_id", { t: "id", v: documentID }],
                ["title", { t: "string", v: "live" }],
                ["done", { t: "bool", v: false }],
              ],
            },
          ],
        }),
      );
    });
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert(address && typeof address === "object");
  return {
    url: `http://127.0.0.1:${address.port}`,
    frames,
    async close() {
      for (const socket of sockets) socket.terminate();
      websocket.close();
      await new Promise((resolve) => server.close(resolve));
    },
  };
}

// Builds the browser expression used to inspect rendered query state.
function browserViewExpression() {
  return `JSON.stringify({
    status: document.querySelector("#status")?.textContent,
    todos: [...document.querySelectorAll("#todos li")].map((node) => node.textContent),
    title: document.title,
  })`;
}

await main();
