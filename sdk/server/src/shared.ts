export const METHOD_PATTERN = /^[A-Za-z][A-Za-z0-9_.-]{0,127}$/;
export const WORKER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;
export const COLLECTION_PATTERN = /^[A-Za-z][A-Za-z0-9_-]{0,127}$/;
export const ID_PATTERN = /^[0-9a-f]{32}$/;
export const ERROR_PATTERN = /^[a-z][a-z0-9_]{0,63}$/;

export interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

export function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((ok, fail) => { resolve = ok; reject = fail; });
  return { promise, resolve, reject };
}

export function parseWorkerURL(raw: string): string {
  const url = new URL(raw);
  if ((url.protocol !== "ws:" && url.protocol !== "wss:") || !url.host || url.username || url.password || url.search || url.hash) {
    throw new TypeError("Worker URL must be an absolute ws(s) URL without credentials or query parameters");
  }
  return url.toString();
}

export function record(value: unknown): value is Record<string, any> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function parseJSONRecord(raw: string): Record<string, unknown> {
  let value: unknown;
  try { value = JSON.parse(raw); } catch { throw new Error("Malformed worker JSON"); }
  if (!record(value)) throw new Error("Worker frame must be an object");
  return value;
}

export function exactKeys(record_: Record<string, unknown>, expected: readonly string[]): void {
  const actual = Object.keys(record_).sort();
  const wanted = [...expected].sort();
  if (actual.length !== wanted.length || actual.some((key, index) => key !== wanted[index])) throw new Error("Worker frame contains unknown or missing fields");
}

export function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error("Unknown Meldbase worker failure");
}

export function abortableDelay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(done, milliseconds);
    signal.addEventListener("abort", done, { once: true });
    function done(): void {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    }
  });
}
