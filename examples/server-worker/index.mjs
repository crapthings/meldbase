import WebSocket from "ws";

import { compileQuery } from "@meldbase/client";
import { MeldbaseMethodError, MeldbaseWorker, publish, rpc, transactional } from "@meldbase/server";

const token = process.env.MELDBASE_WORKER_TOKEN;
if (!token) throw new Error("MELDBASE_WORKER_TOKEN is required");

const worker = new MeldbaseWorker({
  url: process.env.MELDBASE_WORKER_URL ?? "ws://127.0.0.1:9092/v1/workers",
  token,
  workerId: "example-orders-worker",
  webSocketFactory: (url, { headers }) => new WebSocket(url, { headers }),
  onStateChange: (state) => console.log(`[meldbase worker] ${state}`),
  onError: (error) => console.error("[meldbase worker]", error.message),
  methods: {
    "system.ping": rpc(({ principal }) => ({ ok: true, subject: principal.subject })),
    "orders.create": transactional(async ({ principal }, [description], tx) => {
      if (typeof description !== "string" || description.length === 0 || description.length > 500) {
        throw new MeldbaseMethodError("invalid_description");
      }
      const id = await tx.insert("orders", {
        owner: principal.subject,
        description,
        status: "created",
      });
      return tx.get("orders", id);
    }),
  },
  publications: {
    orders: publish({
      version: "orders-owner-v1",
      maxResults: 100,
      queryPaths: ["status", "description"],
      resultFields: ["owner", "description", "status"],
    }, ({ principal }) => compileQuery({ owner: principal.subject })),
  },
});

await worker.start();
console.log("Meldbase example worker registered");

async function shutdown() {
  await worker.stop();
  process.exit(0);
}

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
