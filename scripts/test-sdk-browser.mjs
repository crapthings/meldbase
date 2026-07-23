import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { once } from "node:events";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import WebSocket, { WebSocketServer } from "ws";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const documentID = "00000000000000000000000000000001";
const profile = mkdtempSync(join(tmpdir(), "meldbase-browser-profile-"));
let api;
let vite;
let chrome;
let devtools;

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
    chrome = launchChrome(debugPort);
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

function startVite(port, baseURL) {
  const child = spawn(
    "pnpm",
    [
      "--filter",
      "@meldbase/example-realtime-todos",
      "exec",
      "vite",
      "--host",
      "127.0.0.1",
      "--port",
      String(port),
      "--strictPort",
    ],
    {
      cwd: root,
      env: { ...process.env, VITE_MELDBASE_URL: baseURL },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  child.stderr.on("data", () => {});
  return child;
}

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

function browserViewExpression() {
  return `JSON.stringify({
    status: document.querySelector("#status")?.textContent,
    todos: [...document.querySelectorAll("#todos li")].map((node) => node.textContent),
    title: document.title,
  })`;
}

function launchChrome(debugPort) {
  const executable = chromeExecutable();
  return spawn(
    executable,
    [
      "--headless=new",
      "--no-first-run",
      "--no-default-browser-check",
      "--disable-background-networking",
      `--remote-debugging-port=${debugPort}`,
      `--user-data-dir=${profile}`,
      "about:blank",
    ],
    { stdio: ["ignore", "pipe", "pipe"] },
  );
}

function chromeExecutable() {
  const candidates = [
    process.env.CHROME_BIN,
    process.env.CHROME_PATH,
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "google-chrome",
    "google-chrome-stable",
    "chromium",
  ].filter(Boolean);
  for (const candidate of candidates) {
    if (!candidate.includes("/") || existsSync(candidate)) return candidate;
  }
  throw new Error("Google Chrome or Chromium is required for the browser compatibility test");
}

async function createChromeTarget(port, url) {
  const endpoint = `http://127.0.0.1:${port}/json/new?${encodeURIComponent(url)}`;
  let response = await fetch(endpoint, { method: "PUT" });
  if (!response.ok) response = await fetch(endpoint);
  if (!response.ok) throw new Error(`Chrome could not open browser target: ${response.status}`);
  return response.json();
}

async function availablePort() {
  const server = createNetServer();
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert(address && typeof address === "object");
  const { port } = address;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

async function waitFor(check, message) {
  const deadline = Date.now() + 20_000;
  let lastError;
  while (Date.now() < deadline) {
    try {
      const result = await check();
      if (result) return result;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`${message}${lastError ? `: ${lastError.message}` : ""}`);
}

async function stopProcess(child) {
  if (!child || child.exitCode !== null || child.killed) return;
  child.kill("SIGTERM");
  await Promise.race([once(child, "exit"), new Promise((resolve) => setTimeout(resolve, 2_000))]);
  if (child.exitCode === null) child.kill("SIGKILL");
}

class DevTools {
  #socket;
  #nextID = 1;
  #pending = new Map();
  #listeners = new Map();

  static async connect(url) {
    const socket = new WebSocket(url);
    await once(socket, "open");
    return new DevTools(socket);
  }

  constructor(socket) {
    this.#socket = socket;
    socket.on("message", (raw) => this.#receive(JSON.parse(raw.toString())));
  }

  on(method, listener) {
    const listeners = this.#listeners.get(method) ?? [];
    listeners.push(listener);
    this.#listeners.set(method, listeners);
  }

  send(method, params = {}) {
    const id = this.#nextID++;
    this.#socket.send(JSON.stringify({ id, method, params }));
    return new Promise((resolve, reject) => this.#pending.set(id, { resolve, reject }));
  }

  async evaluate(expression) {
    const response = await this.send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    if (response.exceptionDetails) throw new Error(response.exceptionDetails.text ?? "Browser evaluation failed");
    return response.result?.value;
  }

  async close() {
    if (this.#socket.readyState === WebSocket.OPEN) this.#socket.close();
    await once(this.#socket, "close").catch(() => {});
  }

  #receive(message) {
    if (message.id !== undefined) {
      const pending = this.#pending.get(message.id);
      if (!pending) return;
      this.#pending.delete(message.id);
      if (message.error) pending.reject(new Error(message.error.message));
      else pending.resolve(message.result);
      return;
    }
    for (const listener of this.#listeners.get(message.method) ?? []) listener(message.params);
  }
}

await main();
