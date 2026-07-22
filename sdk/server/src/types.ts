import type { Document, InputDocument, MutationSpec, QuerySpec, Value } from "@meldbase/client";

/** The authenticated application identity for the current call. */
export interface Actor {
  readonly id: string;
  readonly workspaceId: string;
}

export interface MethodContext {
  readonly actor: Actor;
  readonly signal: AbortSignal;
}

export interface PublicationContext extends MethodContext {
  readonly collection: string;
  readonly query: QuerySpec;
}

export interface WriteTransaction {
  get(collection: string, id: string): Promise<Document>;
  insert(collection: string, document: InputDocument): Promise<string>;
  /** Fully replace an existing document at id. Rejects with `not_found` when it is absent; this is not an upsert. */
  replace(collection: string, id: string, document: InputDocument): Promise<void>;
  update(collection: string, id: string, mutation: MutationSpec): Promise<void>;
  delete(collection: string, id: string): Promise<void>;
  invalidatePublication(collection: string): Promise<void>;
}

export type MethodHandler = (context: MethodContext, arguments_: readonly Value[]) => Value | Promise<Value>;
export type TransactionalMethodHandler = (context: MethodContext, arguments_: readonly Value[], transaction: WriteTransaction) => Value | Promise<Value>;

export type MethodDefinition =
  | { readonly mode: "rpc"; readonly handler: MethodHandler }
  | { readonly mode: "transactional"; readonly handler: TransactionalMethodHandler };

export interface PublicationOptions {
  readonly version: string;
  readonly maxResults: number;
  readonly queryPaths: "*" | readonly string[];
  readonly resultFields: "*" | readonly string[];
}

export type PublicationHandler = (context: PublicationContext) => QuerySpec | null | Promise<QuerySpec | null>;

export interface PublicationDefinition extends PublicationOptions {
  readonly handler: PublicationHandler;
}

export interface WorkerSocket {
  readonly readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open" | "close" | "error" | "message", listener: (event: any) => void): void;
  removeEventListener(type: "open" | "close" | "error" | "message", listener: (event: any) => void): void;
}

export type WorkerSocketFactory = (url: string, options: { readonly headers: Readonly<Record<string, string>> }) => WorkerSocket;
export type WorkerState = "idle" | "connecting" | "registering" | "ready" | "stopped";

export interface WorkerOptions {
  /**
   * A full `ws(s)://` worker control endpoint, or `meldbase://host[:port]`.
   * The Meldbase authority form always resolves to `wss://host[:port]/v1/workers`.
   */
  readonly url: string;
  readonly token: string;
  readonly workerId: string;
  readonly methods?: Readonly<Record<string, MethodDefinition>>;
  readonly publications?: Readonly<Record<string, PublicationDefinition>>;
  readonly webSocketFactory: WorkerSocketFactory;
  readonly reconnectMinMs?: number;
  readonly reconnectMaxMs?: number;
  readonly onStateChange?: (state: WorkerState) => void;
  readonly onError?: (error: Error) => void;
}
