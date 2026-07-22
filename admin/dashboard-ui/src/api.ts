import type { AdminSample, DiagnosticResponse, HistoryResponse, IndexCatalogResponse } from "./types";

export const adminSchemaVersion = 17;

async function authorized(path: string, token: string, signal?: AbortSignal): Promise<Response> {
  return fetch(path, {
    headers: { Authorization: `Bearer ${token}` },
    cache: "no-store",
    signal,
  });
}

export async function loadHistory(token: string): Promise<HistoryResponse> {
  const response = await authorized("/v1/stats/history", token);
  if (!response.ok) throw new Error(response.status === 401 ? "Invalid admin token" : `Admin API returned ${response.status}`);
  const history = await response.json() as HistoryResponse;
  if (history.version !== adminSchemaVersion || !Array.isArray(history.samples)) throw new Error("Unsupported admin protocol");
  return history;
}

export async function loadDiagnostics(token: string, after: number, signal?: AbortSignal): Promise<DiagnosticResponse | null> {
  const response = await authorized(`/v1/diagnostics?after=${after}&limit=64`, token, signal);
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`Diagnostics returned ${response.status}`);
  const snapshot = await response.json() as DiagnosticResponse;
  if (snapshot.version !== 1 || !Array.isArray(snapshot.events)) throw new Error("Unsupported diagnostics protocol");
  return snapshot;
}

export async function loadIndexCatalog(token: string, signal?: AbortSignal): Promise<IndexCatalogResponse | null> {
  const response = await authorized("/v1/indexes", token, signal);
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`Index catalog returned ${response.status}`);
  const catalog = await response.json() as IndexCatalogResponse;
  if (catalog.version !== 1 || !Array.isArray(catalog.indexes) || catalog.indexes.some((index) =>
    typeof index.collection !== "string" || typeof index.name !== "string" || typeof index.unique !== "boolean" || !Array.isArray(index.fields) ||
    index.fields.some((field) => typeof field.path !== "string" || (field.order !== 1 && field.order !== -1)),
  )) throw new Error("Unsupported index catalog protocol");
  return catalog;
}

function sleep(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timeout = window.setTimeout(done, milliseconds);
    function done() {
      window.clearTimeout(timeout);
      signal.removeEventListener("abort", done);
      resolve();
    }
    signal.addEventListener("abort", done, { once: true });
  });
}

function consumeEvents(chunk: string, onSample: (sample: AdminSample) => void): void {
  for (const block of chunk.split("\n\n")) {
    const line = block.split("\n").find((item) => item.startsWith("data: "));
    if (!line) continue;
    const sample = JSON.parse(line.slice(6)) as AdminSample;
    if (sample.version === adminSchemaVersion) onSample(sample);
  }
}

export async function streamStats(
  token: string,
  signal: AbortSignal,
  onSample: (sample: AdminSample) => void,
  onState: (state: "live" | "retrying", retryDelay?: number) => void,
): Promise<void> {
  let retryDelay = 500;
  while (!signal.aborted) {
    try {
      const response = await fetch("/v1/stats/stream", {
        headers: { Authorization: `Bearer ${token}`, Accept: "text/event-stream" },
        cache: "no-store",
        signal,
      });
      if (!response.ok || !response.body) throw new Error(`Stream returned ${response.status}`);
      onState("live");
      retryDelay = 500;
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (!signal.aborted) {
        const { value, done } = await reader.read();
        if (done) throw new Error("Stream closed");
        buffer += decoder.decode(value, { stream: true });
        const cutoff = buffer.lastIndexOf("\n\n");
        if (cutoff < 0) continue;
        consumeEvents(buffer.slice(0, cutoff), onSample);
        buffer = buffer.slice(cutoff + 2);
      }
    } catch {
      if (signal.aborted) return;
      onState("retrying", retryDelay);
      await sleep(retryDelay, signal);
      retryDelay = Math.min(retryDelay * 2, 10_000);
    }
  }
}
