import WebSocket from "ws";

import { compileQuery, compileUpdate } from "@meldbase/client";
import { MeldbaseError, MeldbaseWorker, publish, rpc, transactional } from "../dist/index.js";

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
    "sdk.reject": rpc(() => { throw new MeldbaseError("orders.already_paid", { retryAfter: 60n }); }),
    "sdk.create": transactional(async ({ actor }, _arguments, transaction) => {
      const id = await transaction.insert("items", { rank: 7n, workspace: actor.workspaceId, title: "created" });
      return transaction.get("items", id);
    }),
    "sdk.exercise": transactional(async ({ actor }, _arguments, transaction) => {
      const id = await transaction.insert("items", { rank: 1n, workspace: actor.workspaceId, title: "temporary" });
      await transaction.replace("items", id, { rank: 2n, workspace: actor.workspaceId, title: "replaced" });
      await transaction.update("items", id, compileUpdate({ $set: { title: "updated" } }));
      const updated = await transaction.get("items", id);
      await transaction.delete("items", id);
      await transaction.insert("items", { rank: 3n, workspace: actor.workspaceId, title: "committed" });
      await transaction.invalidatePublication("items");
      return updated;
    }),
  },
  publications: {
    items: publish({
      version: "sdk-owner-v1",
      maxResults: 10,
      queryPaths: ["title"],
      resultFields: ["rank", "title"],
    }, ({ actor }) => compileQuery({ workspace: actor.workspaceId })),
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
