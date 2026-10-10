import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve, basename } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import WebSocket from "ws";
import {
  launchChrome,
  createChromeTarget,
  availablePort,
  waitFor,
  stopProcess,
  DevTools,
} from "./lib/browser-test.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const directory = mkdtempSync(join(tmpdir(), "meldbase-npm-starter-"));
const application = join(directory, "application");
let starter;
let chrome;
let browser;
let client;
let otherClient;
let deniedClient;
try {
  // Copy a standalone app: no workspace links, existing dependencies, data, or secrets.
  cpSync(join(root, "examples/npm-react-todos"), application, {
    recursive: true,
    filter: (path) => !["node_modules", "dist", ".meldbase-starter"].includes(basename(path)),
  });
  execFileSync("npm", ["ci", "--no-audit", "--no-fund"], { cwd: application, stdio: "inherit" });
  const lock = JSON.parse(readFileSync(join(application, "package-lock.json"), "utf8"));
  const manifest = JSON.parse(readFileSync(join(application, "package.json"), "utf8"));
  for (const name of ["client", "react"]) {
    const entry = lock.packages[`node_modules/@meldbase/${name}`];
    const expected = manifest.dependencies[`@meldbase/${name}`];
    assert.match(expected, /^\d+\.\d+\.\d+-beta\.\d+$/);
    assert.equal(entry.version, expected);
    assert(entry.resolved.startsWith("https://registry.npmjs.org/"));
    assert(entry.integrity.startsWith("sha512-"));
  }
  execFileSync("npm", ["run", "build"], { cwd: application, stdio: "inherit" });
  const binary = join(directory, "meld");
  execFileSync("go", ["build", "-o", binary, "./cmd/meld"], { cwd: root, stdio: "inherit" });
  const { startStarter } = await import(pathToFileURL(join(application, "scripts/dev.mjs")));
  starter = await startStarter({ binary, database: join(directory, "test.meld"), port: 0 });
  assert.deepEqual(starter.checks, ["session", "collection query", "realtime ticket"]);
  const { checkStarter } = await import(pathToFileURL(join(application, "scripts/preflight.mjs")));
  await assert.rejects(
    checkStarter(starter.url, { fetch: () => Promise.resolve(new Response(null, { status: 403 })) }),
    /session \(HTTP 403\).*changeOrigin: false/,
  );
  await assert.rejects(
    checkStarter(starter.url, {
      fetch: (input, init) =>
        String(input).endsWith("/session")
          ? Promise.resolve(Response.json({ accessToken: "invalid-token" }))
          : fetch(input, init),
    }),
    /collection query \(HTTP 401\).*no trim/,
  );
  await assert.rejects(
    checkStarter(starter.url, {
      fetch: (input, init) =>
        String(input).endsWith("/v1/realtime/tickets")
          ? Promise.resolve(new Response(null, { status: 403 }))
          : fetch(input, init),
    }),
    /realtime ticket \(HTTP 403\)/,
  );
  assert.equal(
    (await fetch(`${starter.url}/session`, { headers: { origin: "https://untrusted.example" } })).status,
    403,
  );
  assert.equal((await fetch(`${starter.url}/session`, { headers: { "sec-fetch-site": "cross-site" } })).status, 403);

  const { MeldbaseClient } = await import(
    pathToFileURL(join(application, "node_modules/@meldbase/client/dist/index.js"))
  );
  const options = { baseUrl: `${starter.url}/api`, webSocketFactory: (url) => new WebSocket(url) };
  client = new MeldbaseClient({ ...options, accessToken: () => starter.backend.token() });
  otherClient = new MeldbaseClient({ ...options, accessToken: () => starter.backend.token("another-workspace") });
  deniedClient = new MeldbaseClient(options);
  await assert.rejects(deniedClient.collection("todos").find({}).fetch(), (error) => error.status === 401);
  await assert.rejects(client.collection("unlisted").find({}).fetch(), (error) => error.status === 403);
  const todos = client.collection("todos");
  const created = await todos.insertOne({ title: "registry task", done: false });
  assert.equal(created.workspaceId, "starter");
  assert.equal((await otherClient.collection("todos").find({}).fetch()).length, 0);

  const debugPort = await availablePort();
  chrome = launchChrome(debugPort, join(directory, "chrome"));
  await waitFor(
    async () => (await fetch(`http://127.0.0.1:${debugPort}/json/version`).catch(() => undefined))?.ok,
    "Chrome did not start",
  );
  const target = await createChromeTarget(debugPort, "about:blank");
  browser = await DevTools.connect(target.webSocketDebuggerUrl);
  const errors = [];
  browser.on("Runtime.exceptionThrown", (event) => errors.push(event.exceptionDetails.text));
  await browser.send("Runtime.enable");
  await browser.send("Page.enable");
  await browser.send("Network.enable");
  // Capture only the fresh test browser's WebSockets to force a transport break.
  await browser.send("Page.addScriptToEvaluateOnNewDocument", {
    source: `
    window.__sockets = [];
    const Original = window.WebSocket;
    window.WebSocket = class extends Original {
      constructor(...args) { super(...args); window.__sockets.push(this); }
    };
  `,
  });
  await browser.send("Page.navigate", { url: starter.url });
  const view = async () =>
    JSON.parse(
      await browser.evaluate(`JSON.stringify({
    status: document.querySelector('#status')?.dataset.state,
    tasks: [...document.querySelectorAll('#todos li')].map(node => ({ id: node.dataset.id, done: node.dataset.done, title: node.querySelector('span')?.textContent })),
    error: document.querySelector('[role="alert"]')?.textContent
  })`),
    );
  try {
    await waitFor(async () => {
      const v = await view();
      return v.status === "live" && v.tasks[0]?.title === "registry task";
    }, "React did not render the authenticated live snapshot");
  } catch (error) {
    throw new Error(`${error.message}; view=${JSON.stringify(await view())}; exceptions=${JSON.stringify(errors)}`);
  }

  // Exercise React's actual form and controls, observing the database through npm.
  await browser.evaluate(`(() => {
    const input = document.querySelector('#title');
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(input, 'browser task');
    input.dispatchEvent(new Event('input', { bubbles: true }));
  })()`);
  await browser.evaluate(`document.querySelector('form').requestSubmit()`);
  await waitFor(
    async () => (await todos.find({ title: "browser task" }).fetch()).length === 1,
    "Browser insert did not reach Go",
  );
  await waitFor(async () => (await view()).tasks.length === 2, "React did not receive the browser insert");
  await browser.evaluate(`document.querySelector('#todos li[data-id="${created._id}"] input').click()`);
  await waitFor(
    async () => (await todos.findOne({ _id: created._id }))?.done === true,
    "Browser update did not reach Go",
  );
  await waitFor(
    async () => (await view()).tasks.find((task) => task.id === created._id)?.done === "true",
    "React did not show completion",
  );

  await browser.send("Network.emulateNetworkConditions", {
    offline: true,
    latency: 0,
    downloadThroughput: -1,
    uploadThroughput: -1,
  });
  await browser.evaluate(`window.__sockets.forEach(socket => socket.close())`);
  await waitFor(async () => (await view()).status === "stale", "Offline subscription did not become stale");
  assert.equal((await view()).tasks.length, 2, "Offline view must retain its last snapshot");
  await todos.insertOne({ title: "added while offline", done: false });
  await browser.send("Network.emulateNetworkConditions", {
    offline: false,
    latency: 0,
    downloadThroughput: -1,
    uploadThroughput: -1,
  });
  await waitFor(async () => {
    const v = await view();
    return v.status === "live" && v.tasks.length === 3 && v.tasks.some((task) => task.title === "added while offline");
  }, "Subscription did not recover after transport interruption");
  await browser.evaluate(`document.querySelector('#todos li[data-id="${created._id}"] button').click()`);
  await waitFor(
    async () => (await todos.findOne({ _id: created._id })) === undefined,
    "Browser delete did not reach Go",
  );
  await waitFor(async () => (await view()).tasks.length === 2, "React did not receive deletion");
  assert.equal((await view()).error, undefined);
  assert.deepEqual(errors, []);
  if (process.env.MELDBASE_STARTER_SCREENSHOT) {
    const screenshot = await browser.send("Page.captureScreenshot", { format: "png" });
    writeFileSync(process.env.MELDBASE_STARTER_SCREENSHOT, Buffer.from(screenshot.data, "base64"));
  }
  console.log(
    "Verified standalone npm installation, strict types, real Go auth/CRUD/workspace isolation, React rendering and offline subscription recovery.",
  );
} finally {
  client?.close();
  otherClient?.close();
  deniedClient?.close();
  await browser?.close();
  await stopProcess(chrome);
  await starter?.close();
  rmSync(directory, { recursive: true, force: true });
}
