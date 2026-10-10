import type { Document, InputDocument, MutationSpec, QuerySpec, Value } from "@meldbase/client";

/** The authenticated application identity for the current call. */
export interface Actor {
  readonly id: string;
  readonly workspaceId: string;
}

/** The authenticated context of one RPC invocation. */
export interface RPCContext {
  readonly actor: Actor;
  readonly signal: AbortSignal;
}

/** Provides the authenticated actor, cancellation signal, and query for a read policy decision. */
export interface ReadPolicyContext {
  readonly actor: Actor;
  readonly signal: AbortSignal;
  readonly collection: string;
  readonly query: QuerySpec;
}

/** Provides document point operations and read policy invalidation within one atomic transaction. */
export interface WriteTransaction {
  get(collection: string, id: string): Promise<Document>;
  insert(collection: string, document: InputDocument): Promise<string>;
  /** Fully replace an existing document at id. Rejects with `not_found` when it is absent; this is not an upsert. */
  replace(collection: string, id: string, document: InputDocument): Promise<void>;
  update(collection: string, id: string, mutation: MutationSpec): Promise<void>;
  delete(collection: string, id: string): Promise<void>;
  invalidateReadPolicy(collection: string): Promise<void>;
}

/** Handles an RPC call using its authenticated context and typed input. */
export type RPCHandler = (context: RPCContext, input: Value) => Value | Promise<Value>;
/** Handles an RPC call with access to an atomic write transaction. */
export type TransactionalRPCHandler = (
  context: RPCContext,
  input: Value,
  transaction: WriteTransaction,
) => Value | Promise<Value>;

/** Declares an ordinary or transactional RPC handler. */
export type RPCDefinition =
  | { readonly mode: "rpc"; readonly handler: RPCHandler }
  | { readonly mode: "transactional"; readonly handler: TransactionalRPCHandler };

/** Declares a read policy’s version, result limit, and allowed query and result fields. */
export interface ReadPolicyOptions {
  readonly version: string;
  readonly maxResults: number;
  readonly queryPaths: "*" | readonly string[];
  readonly resultFields: "*" | readonly string[];
}

/** Returns an authorized query or denies a read by returning null. */
export type ReadPolicyHandler = (context: ReadPolicyContext) => QuerySpec | null | Promise<QuerySpec | null>;

/** Combines read policy limits with the handler that decides access. */
export interface ReadPolicyDefinition extends ReadPolicyOptions {
  readonly handler: ReadPolicyHandler;
}

/** Defines the WebSocket operations needed by a worker connection. */
export interface WorkerSocket {
  readonly readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open" | "close" | "error" | "message", listener: (event: any) => void): void;
  removeEventListener(type: "open" | "close" | "error" | "message", listener: (event: any) => void): void;
}

/** Creates a worker socket with authentication headers. */
export type WorkerSocketFactory = (
  url: string,
  options: { readonly headers: Readonly<Record<string, string>> },
) => WorkerSocket;
/** Names the stages of a worker connection lifecycle. */
export type WorkerState = "idle" | "connecting" | "registering" | "ready" | "stopped";

/** Configures worker identity, handlers, transport, reconnect timing, and lifecycle callbacks. */
export interface WorkerOptions {
  /**
   * A full `ws(s)://` worker control endpoint, or `meldbase://host[:port]`.
   * The Meldbase authority form always resolves to `wss://host[:port]/v1/workers`.
   */
  readonly url: string;
  readonly token: string;
  readonly workerId: string;
  readonly methods?: Readonly<Record<string, RPCDefinition>>;
  readonly readPolicies?: Readonly<Record<string, ReadPolicyDefinition>>;
  readonly webSocketFactory: WorkerSocketFactory;
  readonly reconnectMinMs?: number;
  readonly reconnectMaxMs?: number;
  readonly onStateChange?: (state: WorkerState) => void;
  readonly onError?: (error: Error) => void;
}
