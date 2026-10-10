import type { ProtocolDescriptor } from "../protocol.js";

/** Names the stages of a realtime subscription connection. */
export type SyncState = "idle" | "authenticating" | "connecting" | "live" | "stale" | "resyncing" | "error" | "closed";

/** Reports a subscription’s connection state, error, and resume token. */
export interface SyncStatus {
  readonly state: SyncState;
  readonly error?: Error;
  readonly token?: string;
}

/** Provides the endpoint and temporary ticket needed to open a realtime connection. */
export interface RealtimeTicket {
  readonly url: string;
  readonly ticket: string;
  readonly protocol?: ProtocolDescriptor;
}

/** Defines the socket operations required by the realtime client. */
export interface WebSocketLike {
  readonly readyState: number;
  addEventListener(type: "open" | "close" | "error" | "message", listener: (event: Event | MessageEvent) => void): void;
  send(data: string): void;
  close(code?: number, reason?: string): void;
}

/** Configures remote transport, authentication, reconnect timing, and response limits. */
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

/** Configures notifications about a subscription’s connection state. */
export interface SubscribeOptions {
  readonly onStatus?: (status: SyncStatus) => void;
}

/** Selects HTTP or the existing realtime connection for an RPC call. */
export type RPCTransport = "http" | "realtime";

/** Configures RPC cancellation, transport, and caller-controlled idempotency. */
export interface CallOptions {
  readonly signal?: AbortSignal;
  // HTTP remains the default. Realtime calls reuse the subscription socket but
  // are never retried after a transport failure.
  readonly transport?: RPCTransport;
  // Identifies one logical operation across explicit caller retries. Meldbase
  // never retries RPC calls automatically.
  readonly idempotencyKey?: string;
}
