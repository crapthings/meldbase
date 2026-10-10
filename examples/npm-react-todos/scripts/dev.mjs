import { createServer } from "vite";
import { mkdirSync } from "node:fs";
import { createServer as createNetServer } from "node:net";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { startLocalServer } from "./local-server.mjs";
import { checkStarter } from "./preflight.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

// Used by npm dev and by the registry-installed end-to-end check.
export async function startStarter({ binary = process.env.MELDBASE_BIN ?? "meld", database, port = 5173 } = {}) {
  if (port === 0) {
    const reservation = createNetServer();
    await new Promise((resolve, reject) => {
      reservation.once("error", reject);
      reservation.listen(0, "127.0.0.1", resolve);
    });
    port = reservation.address().port;
    await new Promise((resolve) => reservation.close(resolve));
  }
  const url = `http://127.0.0.1:${port}`;
  const dataDirectory = join(root, ".meldbase-starter");
  if (!database) mkdirSync(dataDirectory, { recursive: true, mode: 0o700 });
  const backend = await startLocalServer({
    binary,
    database: database ?? join(dataDirectory, "todos.meld"),
    origin: url,
  });
  let vite;
  try {
    vite = await createServer({
      root,
      server: {
        host: "127.0.0.1",
        port,
        strictPort: true,
        proxy: {
          "/api": { target: backend.baseUrl, changeOrigin: false, rewrite: (path) => path.replace(/^\/api/, "") },
          "/v1/realtime": { target: backend.baseUrl, changeOrigin: false, ws: true },
        },
      },
      plugins: [
        {
          name: "local-demo-session",
          configureServer(server) {
            server.middlewares.use((request, response, next) => {
              if (request.url !== "/session") return next();
              // Prevent another website or DNS-rebinding request from obtaining a demo token.
              if (
                request.method !== "GET" ||
                request.headers.host !== `127.0.0.1:${port}` ||
                (request.headers.origin && request.headers.origin !== url) ||
                (request.headers["sec-fetch-site"] && request.headers["sec-fetch-site"] !== "same-origin")
              ) {
                response.writeHead(403).end();
                return;
              }
              response.setHeader("content-type", "application/json");
              response.setHeader("cache-control", "no-store");
              response.end(JSON.stringify({ accessToken: backend.token() }));
            });
          },
        },
      ],
    });
    await vite.listen();
    const checks = await checkStarter(url);
    return {
      url,
      checks,
      backend,
      async close() {
        await vite.close();
        await backend.close();
      },
    };
  } catch (error) {
    await vite?.close();
    await backend.close();
    throw error;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const starter = await startStarter();
  console.log(`Startup checks passed: ${starter.checks.join(", ")}.`);
  console.log(`Meldbase starter: ${starter.url}`);
  console.log("Local JWT demo identity; data stays in .meldbase-starter/todos.meld. Ctrl+C to stop.");
  let stopping = false;
  async function stop(code = 0) {
    if (stopping) return;
    stopping = true;
    await starter.close();
    process.exitCode = code;
  }
  for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, () => void stop());
  starter.backend.child.once("exit", () => void stop(1));
}
