import WebSocket from "ws";

import { compileQuery } from "@meldbase/client";
import { MeldbaseWorker, publish, rpc, transactional } from "../dist/index.js";

const url = process.env.MELDBASE_WORKER_URL;
const token = process.env.MELDBASE_WORKER_TOKEN;
if (!url || !token) throw new Error("MELDBASE_WORKER_URL and MELDBASE_WORKER_TOKEN are required");

const worker = new MeldbaseWorker({
  url,
  token,
  workerId: "go-hub-e2e-worker",
  webSocketFactory: (workerURL, { headers }) => new WebSocket(workerURL, { headers }),
  methods: {
    "sdk.echo": rpc((_context, arguments_) => arguments_[0] ?? null),
    "sdk.create": transactional(async ({ actor }, _arguments, transaction) => {
      const id = await transaction.insert("items", { rank: 7n, tenant: actor.tenantId, title: "created" });
      return transaction.get("items", id);
    }),
  },
  publications: {
    items: publish({
      version: "sdk-owner-v1",
      maxResults: 10,
      queryPaths: ["title"],
      resultFields: ["rank", "title"],
    }, ({ actor }) => compileQuery({ tenant: actor.tenantId })),
  },
});

await worker.start();
process.stdout.write("ready\n");

async function shutdown() {
  await worker.stop();
  process.exit(0);
}

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
