import type { ProtocolDescriptor } from "../protocol.js";

export type SyncState = "idle" | "authenticating" | "connecting" | "live" | "stale" | "resyncing" | "error" | "closed";

export interface SyncStatus {
  readonly state: SyncState;
  readonly error?: Error;
  readonly token?: string;
}

export interface RealtimeTicket {
  readonly url: string;
  readonly ticket: string;
  readonly protocol?: ProtocolDescriptor;
}

export interface WebSocketLike {
  readonly readyState: number;
  addEventListener(type: "open" | "close" | "error" | "message", listener: (event: Event | MessageEvent) => void): void;
  send(data: string): void;
  close(code?: number, reason?: string): void;
}

export interface ClientOptions {
  readonly baseUrl: string;
  readonly accessToken?: () => string | undefined | Promise<string | undefined>;
  readonly fetch?: typeof globalThis.fetch;
  readonly webSocketFactory?: (url: string) => WebSocketLike;
  // Defaults to the baseUrl host with ws/wss. Cross-origin realtime endpoints
  // must be opted into explicitly.
  readonly allowedRealtimeOrigins?: readonly string[];
  readonly maxInboundBytes?: number;
  readonly maxSnapshotDocuments?: number;
  readonly maxDeltaOperations?: number;
  readonly reconnect?: { readonly minDelayMs?: number; readonly maxDelayMs?: number };
}

export interface SubscribeOptions {
  readonly onStatus?: (status: SyncStatus) => void;
}

export type RPCTransport = "http" | "realtime";

export interface CallOptions {
  readonly signal?: AbortSignal;
  // HTTP remains the default. Realtime calls reuse the subscription socket but
  // are never retried after a transport failure.
  readonly transport?: RPCTransport;
  // Identifies one logical operation across explicit caller retries. Meldbase
  // never retries RPC calls automatically.
  readonly idempotencyKey?: string;
}
