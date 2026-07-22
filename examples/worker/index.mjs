import WebSocket from "ws";

import { compileQuery } from "@meldbase/client";
import { MeldbaseError, MeldbaseWorker, readPolicy, rpc } from "@meldbase/worker";

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
    "system.ping": rpc(({ actor }) => ({ ok: true, id: actor.id })),
    "orders.create": rpc.transactional(async ({ actor }, [description], tx) => {
      if (typeof description !== "string" || description.length === 0 || description.length > 500) {
        throw new MeldbaseError("orders.invalid_description");
      }
      const id = await tx.insert("orders", {
        owner: actor.id,
        description,
        status: "created",
      });
      return tx.get("orders", id);
    }),
  },
  readPolicies: {
    orders: readPolicy({
      version: "orders-owner-v1",
      maxResults: 100,
      queryPaths: ["status", "description"],
      resultFields: ["owner", "description", "status"],
    }, ({ actor }) => compileQuery({ owner: actor.id })),
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
