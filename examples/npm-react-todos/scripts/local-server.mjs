import { spawn } from "node:child_process";
import { once } from "node:events";
import { createHmac, randomBytes } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// A local identity fixture, not a login service. Only the Go server receives the key.
export async function startLocalServer({ binary, database, origin }) {
  const directory = mkdtempSync(join(tmpdir(), "meldbase-starter-auth-"));
  const secret = randomBytes(48).toString("hex");
  const secretFile = join(directory, "jwt.secret");
  const policyFile = join(directory, "policy.json");
  writeFileSync(secretFile, secret, { mode: 0o600 });
  writeFileSync(
    policyFile,
    JSON.stringify({
      version: 1,
      workspaceField: "workspaceId",
      collections: [{ collection: "todos", mode: "collaborative" }],
    }),
    { mode: 0o600 },
  );
  const child = spawn(
    binary,
    [
      "serve",
      "--db",
      database,
      "--addr",
      "127.0.0.1:0",
      "--jwt-hs256-secret-file",
      secretFile,
      "--jwt-issuer",
      "meldbase-local-starter",
      "--jwt-audience",
      "meldbase-starter-api",
      "--access-policy-file",
      policyFile,
      "--public-realtime-url",
      `${origin.replace(/^http/, "ws")}/v1/realtime`,
      "--http-origins",
      origin,
      "--realtime-origin-patterns",
      origin,
    ],
    { stdio: ["ignore", "pipe", "pipe"] },
  );
  let output = "";
  let startupError;
  child.on("error", (error) => {
    startupError = error;
  });
  for (const stream of [child.stdout, child.stderr]) {
    stream.on("data", (chunk) => {
      output = (output + chunk.toString()).slice(-12_000);
    });
  }
  async function close() {
    await stopProcess(child);
    rmSync(directory, { recursive: true, force: true });
  }
  try {
    const baseUrl = await waitFor(() => {
      if (startupError) throw startupError;
      if (child.exitCode !== null || child.signalCode !== null)
        throw new Error(`Meldbase exited during startup: ${output}`);
      const match = /^Meldbase listening on (http:\/\/[^\s]+)/m.exec(output);
      return match?.[1];
    }, "Meldbase did not start. Install meld or set MELDBASE_BIN.");
    return {
      baseUrl,
      child,
      close,
      token(workspace = "starter") {
        const encode = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
        const now = Math.floor(Date.now() / 1000);
        const payload = `${encode({ alg: "HS256", typ: "JWT" })}.${encode({
          iss: "meldbase-local-starter",
          aud: "meldbase-starter-api",
          sub: "local-demo-user",
          workspace_id: workspace,
          iat: now,
          exp: now + 300,
        })}`;
        return `${payload}.${createHmac("sha256", secret).update(payload).digest("base64url")}`;
      },
    };
  } catch (error) {
    await close();
    throw error;
  }
}

export async function waitFor(check, message, timeout = 30_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const result = await check();
    if (result) return result;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(message);
}

export async function stopProcess(child) {
  if (!child || !child.pid || child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGTERM");
  let timer;
  await Promise.race([
    exited,
    new Promise((resolve) => {
      timer = setTimeout(resolve, 2_000);
    }),
  ]);
  clearTimeout(timer);
  if (child.exitCode === null && child.signalCode === null) {
    child.kill("SIGKILL");
    await exited;
  }
}
