import { spawn } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdirSync } from "node:fs";
import { createServer as createNetServer } from "node:net";
import assert from "node:assert/strict";
import WebSocket from "ws";

// Starts headless Chrome with the isolated test profile and debugging port.
export function launchChrome(debugPort, profile) {
  const executable = chromeExecutable();
  mkdirSync(profile, { recursive: true, mode: 0o700 });
  const child = spawn(
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
  // Retain startup diagnostics so CI failures identify the browser's actual error.
  child.stderr.on("data", (data) => process.stderr.write(data));
  child.stdout.resume();
  child.on("error", (error) => console.error(`Chrome startup failed: ${error.message}`));
  return child;
}

// Selects a configured or conventional Chrome executable candidate.
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

// Creates a browser tab through Chrome’s local debugging endpoint.
export async function createChromeTarget(port, url) {
  const endpoint = `http://127.0.0.1:${port}/json/new?${encodeURIComponent(url)}`;
  let response = await fetch(endpoint, { method: "PUT" });
  if (!response.ok) response = await fetch(endpoint);
  if (!response.ok) throw new Error(`Chrome could not open browser target: ${response.status}`);
  return response.json();
}

// Finds a currently available local TCP port and releases it.
export async function availablePort() {
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

// Polls an asynchronous check for a truthy result until the test deadline.
export async function waitFor(check, message) {
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

// Requests process termination and forcefully stops it if it outlasts the grace period.
export async function stopProcess(child) {
  if (!child || child.exitCode !== null || child.killed) return;
  child.kill("SIGTERM");
  await Promise.race([once(child, "exit"), new Promise((resolve) => setTimeout(resolve, 2_000))]);
  if (child.exitCode === null) child.kill("SIGKILL");
}

// Matches Chrome debugging responses to requests and dispatches browser events.
export class DevTools {
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
