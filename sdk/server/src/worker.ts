import {
  decodeDocument,
  decodeQuerySpec,
  decodeValue,
  encodeInputDocument,
  encodeMutationSpec,
  encodeQuerySpec,
  encodeValue,
  DEFAULT_QUERY_LIMITS,
  QueryValidationError,
  decodeProtocolDescriptor,
  MELDBASE_PROTOCOL_VERSION,
  supportsProtocol,
} from "@meldbase/client";
import type { Document, InputDocument, MutationSpec, ProtocolDescriptor, QuerySpec, Value, WireValue } from "@meldbase/client";

const METHOD_PATTERN = /^[A-Za-z][A-Za-z0-9_.-]{0,127}$/;
const WORKER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;
const COLLECTION_PATTERN = /^[A-Za-z][A-Za-z0-9_-]{0,127}$/;
const ID_PATTERN = /^[0-9a-f]{32}$/;
const ERROR_PATTERN = /^[a-z][a-z0-9_]{0,63}$/;

export interface Principal {
  readonly subject: string;
  readonly tenant: string;
}

export interface MethodContext {
  readonly principal: Principal;
  readonly signal: AbortSignal;
}

export interface PublicationContext extends MethodContext {
  readonly collection: string;
  readonly query: QuerySpec;
}

export interface WriteTransaction {
  get(collection: string, id: string): Promise<Document>;
  insert(collection: string, document: InputDocument): Promise<string>;
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

export function rpc(handler: MethodHandler): MethodDefinition {
  if (typeof handler !== "function") throw new TypeError("RPC handler must be a function");
  return { mode: "rpc", handler };
}

export function transactional(handler: TransactionalMethodHandler): MethodDefinition {
  if (typeof handler !== "function") throw new TypeError("Transactional RPC handler must be a function");
  return { mode: "transactional", handler };
}

export function publish(options: PublicationOptions, handler: PublicationHandler): PublicationDefinition {
  if (typeof handler !== "function") throw new TypeError("Publication handler must be a function");
  validatePublicationOptions(options);
  return { ...options, handler };
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
  // Require the Hub to acknowledge the opt-in capability discovery request.
  readonly requireProtocol?: boolean;
}

export class MeldbaseMethodError extends Error {
  readonly code: string;

  constructor(code: string) {
    if (!ERROR_PATTERN.test(code)) throw new TypeError("Invalid Meldbase method error code");
    super(`Meldbase method failed: ${code}`);
    this.name = "MeldbaseMethodError";
    this.code = code;
  }
}

export class MeldbaseWorkerProtocolError extends Error {
  readonly required: readonly string[];
  constructor(required: readonly string[]) {
    super(`Meldbase worker protocol does not support: ${required.join(", ")}`);
    this.name = "MeldbaseWorkerProtocolError";
    this.required = Object.freeze([...required]);
  }
}

interface ActiveCall {
  readonly controller: AbortController;
  readonly transaction?: RemoteWriteTransaction;
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((ok, fail) => { resolve = ok; reject = fail; });
  return { promise, resolve, reject };
}

export class MeldbaseWorker {
  readonly #options: Required<Pick<WorkerOptions, "reconnectMinMs" | "reconnectMaxMs">> & WorkerOptions;
  readonly #methods: ReadonlyMap<string, MethodDefinition>;
  readonly #publications: ReadonlyMap<string, PublicationDefinition>;
  readonly #stopController = new AbortController();
  #socket: WorkerSocket | undefined;
  #state: WorkerState = "idle";
  #loop?: Promise<void>;
  #ready?: Deferred<void>;
  #calls = new Map<string, ActiveCall>();
  #session = 0;
  #protocol: ProtocolDescriptor | undefined;
  #fatalError: Error | undefined;

  constructor(options: WorkerOptions) {
    if (!options || typeof options.webSocketFactory !== "function") throw new TypeError("Worker WebSocket factory is required");
		if (options.requireProtocol !== undefined && typeof options.requireProtocol !== "boolean") throw new TypeError("requireProtocol must be boolean");
    const url = parseWorkerURL(options.url);
    if (options.token.length < 32 || options.token.length > 4096) throw new TypeError("Worker token must contain between 32 and 4096 bytes");
    if (!WORKER_PATTERN.test(options.workerId)) throw new TypeError("Invalid worker ID");
    const methods = new Map<string, MethodDefinition>();
    for (const [name, definition] of Object.entries(options.methods ?? {})) {
      if (!METHOD_PATTERN.test(name) || !definition || (definition.mode !== "rpc" && definition.mode !== "transactional") || typeof definition.handler !== "function") {
        throw new TypeError(`Invalid worker method ${name}`);
      }
      methods.set(name, definition);
    }
    if (methods.size > 4096) throw new TypeError("Worker cannot register more than 4096 methods");
    const publications = new Map<string, PublicationDefinition>();
    for (const [collection, definition] of Object.entries(options.publications ?? {})) {
      if (!COLLECTION_PATTERN.test(collection) || !definition || typeof definition.handler !== "function") throw new TypeError(`Invalid worker publication ${collection}`);
      validatePublicationOptions(definition);
      publications.set(collection, {
        ...definition,
        queryPaths: definition.queryPaths === "*" ? "*" : Object.freeze([...definition.queryPaths]),
        resultFields: definition.resultFields === "*" ? "*" : Object.freeze([...definition.resultFields]),
      });
    }
    if (publications.size > 4096) throw new TypeError("Worker cannot register more than 4096 publications");
    if (methods.size === 0 && publications.size === 0) throw new TypeError("Worker requires at least one method or publication");
    const reconnectMinMs = options.reconnectMinMs ?? 250;
    const reconnectMaxMs = options.reconnectMaxMs ?? 10_000;
    if (!Number.isSafeInteger(reconnectMinMs) || !Number.isSafeInteger(reconnectMaxMs) || reconnectMinMs < 10 || reconnectMaxMs < reconnectMinMs || reconnectMaxMs > 60_000) {
      throw new TypeError("Invalid worker reconnect bounds");
    }
    this.#options = { ...options, url, reconnectMinMs, reconnectMaxMs };
    this.#methods = methods;
    this.#publications = publications;
  }

  get state(): WorkerState { return this.#state; }
  get protocol(): ProtocolDescriptor | undefined { return this.#protocol; }

  async start(): Promise<void> {
    if (this.#state === "stopped") throw new Error("Meldbase worker is stopped");
    if (!this.#loop) {
      this.#ready = deferred<void>();
      this.#loop = this.#run();
    }
    await this.#ready!.promise;
  }

  async stop(): Promise<void> {
    if (this.#state === "stopped") return;
    this.#stopController.abort();
    this.#ready?.reject(new Error("Meldbase worker stopped before registration"));
    this.#socket?.close(1000, "worker stopped");
    this.#abortCalls(new Error("Meldbase worker stopped"));
    if (this.#loop) await this.#loop;
    this.#setState("stopped");
  }

  async #run(): Promise<void> {
    let delay = this.#options.reconnectMinMs;
    while (!this.#stopController.signal.aborted) {
      try {
        await this.#openSession();
        delay = this.#options.reconnectMinMs;
      } catch (error) {
        if (this.#stopController.signal.aborted) break;
        this.#options.onError?.(asError(error));
      }
      if (this.#stopController.signal.aborted) break;
      await abortableDelay(delay, this.#stopController.signal);
      delay = Math.min(this.#options.reconnectMaxMs, delay * 2);
    }
		if (this.#fatalError) this.#setState("stopped");
  }

  async #openSession(): Promise<void> {
    this.#setState("connecting");
    const socket = this.#options.webSocketFactory(this.#options.url, {
      headers: { authorization: `Bearer ${this.#options.token}`, "meldbase-protocol": "capabilities-v1" },
    });
    this.#socket = socket;
    const session = ++this.#session;
    const closed = deferred<void>();
    const opened = deferred<void>();
    const onOpen = () => opened.resolve();
    const onClose = (event: { code?: unknown; reason?: unknown }) => {
      if (this.#state === "ready" || this.#stopController.signal.aborted) {
        closed.resolve();
        return;
      }
      const code = typeof event.code === "number" ? ` (${event.code})` : "";
      const reason = typeof event.reason === "string" && event.reason.length > 0 ? `: ${event.reason}` : "";
      closed.reject(new Error(`Meldbase worker closed before registration${code}${reason}`));
    };
    const onError = () => closed.reject(new Error("Meldbase worker socket failed"));
    const onMessage = (event: { data?: unknown }) => {
      void this.#receive(session, event.data).catch((error) => {
		const failure = asError(error);
		if (failure instanceof MeldbaseWorkerProtocolError) {
			this.#fatalError = failure;
			this.#ready?.reject(failure);
			this.#stopController.abort(failure);
		}
		this.#options.onError?.(failure);
        socket.close(1008, "protocol violation");
      });
    };
    socket.addEventListener("open", onOpen);
    socket.addEventListener("close", onClose);
    socket.addEventListener("error", onError);
    socket.addEventListener("message", onMessage);
    try {
      await Promise.race([opened.promise, closed.promise]);
      if (this.#stopController.signal.aborted) return;
      this.#setState("registering");
      this.#send({
        v: MELDBASE_PROTOCOL_VERSION, type: "register", workerId: this.#options.workerId,
        methods: [...this.#methods].map(([name, definition]) => ({ name, mode: definition.mode })),
        publications: [...this.#publications].map(([collection, definition]) => ({
          collection, version: definition.version, maxResults: definition.maxResults,
          queryPaths: definition.queryPaths, resultFields: definition.resultFields,
        })),
      });
      await closed.promise;
    } finally {
      socket.removeEventListener("open", onOpen);
      socket.removeEventListener("close", onClose);
      socket.removeEventListener("error", onError);
      socket.removeEventListener("message", onMessage);
      if (this.#socket === socket) this.#socket = undefined;
      this.#abortCalls(new Error("Meldbase worker connection closed"));
      if (!this.#stopController.signal.aborted) this.#setState("connecting");
    }
  }

  async #receive(session: number, data: unknown): Promise<void> {
    if (session !== this.#session || typeof data !== "string") throw new Error("Worker frame must be text");
    const frame = parseJSONRecord(data);
    if (frame.v !== MELDBASE_PROTOCOL_VERSION || typeof frame.type !== "string") throw new Error("Invalid worker frame header");
    switch (frame.type) {
      case "registered":
		exactKeys(frame, frame.protocol === undefined ? ["v", "type", "sessionId", "limits"] : ["v", "type", "sessionId", "limits", "protocol"]);
        if (typeof frame.sessionId !== "string" || !record(frame.limits)) throw new Error("Invalid registered frame");
		if (frame.protocol === undefined && this.#options.requireProtocol) throw new MeldbaseWorkerProtocolError(["protocol.discovery"]);
		if (frame.protocol !== undefined) {
			let descriptor: ProtocolDescriptor;
			try { descriptor = decodeProtocolDescriptor(frame.protocol); }
			catch { throw new MeldbaseWorkerProtocolError(Object.freeze(["valid_descriptor"])); }
			const required = new Set<string>();
			if ([...this.#methods.values()].some((method) => method.mode === "rpc")) required.add("rpc");
			if ([...this.#methods.values()].some((method) => method.mode === "transactional")) {
				required.add("rpc.transactional");
				required.add("transaction.compiled_update");
				required.add("transaction.invalidate_publication");
				required.add("transaction.point_operations");
			}
			if (this.#publications.size > 0) required.add("publication.policy");
			const missing = [...required].filter((capability) => !descriptor.capabilities.includes(capability));
			if (!supportsProtocol(descriptor, MELDBASE_PROTOCOL_VERSION) || missing.length > 0) {
				if (!descriptor.versions.includes(MELDBASE_PROTOCOL_VERSION)) missing.unshift(`version.${MELDBASE_PROTOCOL_VERSION}`);
				throw new MeldbaseWorkerProtocolError(Object.freeze(missing));
			}
			this.#protocol = descriptor;
		}
        this.#setState("ready");
        this.#ready?.resolve();
        return;
      case "invoke":
        await this.#invoke(frame);
        return;
      case "authorize_query":
        await this.#authorizeQuery(frame);
        return;
      case "cancel":
        exactKeys(frame, ["v", "type", "callId"]);
        if (typeof frame.callId !== "string") throw new Error("Invalid cancel frame");
        this.#calls.get(frame.callId)?.controller.abort();
        return;
      case "tx_result":
      case "tx_error": {
        exactKeys(frame, frame.type === "tx_result" ? ["v", "type", "callId", "opId", "result"] : ["v", "type", "callId", "opId", "error"]);
        if (typeof frame.callId !== "string" || typeof frame.opId !== "string") throw new Error("Invalid transaction response");
        const transaction = this.#calls.get(frame.callId)?.transaction;
        if (!transaction) throw new Error("Transaction response has no active call");
        transaction.receive(frame);
        return;
      }
      default:
        throw new Error("Unknown worker frame type");
    }
  }

  async #invoke(frame: Record<string, unknown>): Promise<void> {
    exactKeys(frame, ["v", "type", "callId", "method", "mode", "principal", "arguments"]);
    if (typeof frame.callId !== "string" || typeof frame.method !== "string" || (frame.mode !== "rpc" && frame.mode !== "transactional") ||
        !record(frame.principal) || typeof frame.principal.subject !== "string" || typeof frame.principal.tenant !== "string" || !Array.isArray(frame.arguments)) {
      throw new Error("Invalid invoke frame");
    }
    const definition = this.#methods.get(frame.method);
    if (!definition || definition.mode !== frame.mode || this.#calls.has(frame.callId)) throw new Error("Invoke does not match registration");
    const arguments_ = frame.arguments.map((argument) => decodeValue(argument));
    const controller = new AbortController();
    const context: MethodContext = {
      principal: Object.freeze({ subject: frame.principal.subject, tenant: frame.principal.tenant }),
      signal: controller.signal,
    };
    const active: ActiveCall = definition.mode === "transactional"
      ? { controller, transaction: new RemoteWriteTransaction(frame.callId, (value) => this.#send(value)) }
      : { controller };
    this.#calls.set(frame.callId, active);
    try {
      const result = definition.mode === "transactional"
        ? await definition.handler(context, arguments_, active.transaction!)
        : await definition.handler(context, arguments_);
      if (controller.signal.aborted) return;
      this.#send({ v: MELDBASE_PROTOCOL_VERSION, type: "result", callId: frame.callId, result: encodeValue(result) });
    } catch (error) {
      if (controller.signal.aborted) return;
      const code = error instanceof MeldbaseMethodError ? error.code : "internal";
      this.#send({ v: MELDBASE_PROTOCOL_VERSION, type: "error", callId: frame.callId, error: { code } });
    } finally {
      active.transaction?.close(new Error("Transaction method completed"));
      this.#calls.delete(frame.callId);
    }
  }

  async #authorizeQuery(frame: Record<string, unknown>): Promise<void> {
    exactKeys(frame, ["v", "type", "callId", "collection", "principal", "query"]);
    if (typeof frame.callId !== "string" || typeof frame.collection !== "string" ||
        !record(frame.principal) || typeof frame.principal.subject !== "string" || typeof frame.principal.tenant !== "string") {
      throw new Error("Invalid query authorization frame");
    }
    const definition = this.#publications.get(frame.collection);
    if (!definition || this.#calls.has(frame.callId)) throw new Error("Query authorization does not match registration");
    const query = decodeQuerySpec(frame.query);
    const controller = new AbortController();
    const active: ActiveCall = { controller };
    const context: PublicationContext = {
      collection: frame.collection,
      principal: Object.freeze({ subject: frame.principal.subject, tenant: frame.principal.tenant }),
      query,
      signal: controller.signal,
    };
    this.#calls.set(frame.callId, active);
    try {
      const constraint = await definition.handler(context);
      if (controller.signal.aborted) return;
      if (constraint === null) {
        this.#send({ v: MELDBASE_PROTOCOL_VERSION, type: "policy_error", callId: frame.callId, error: { code: "forbidden" } });
        return;
      }
      if (constraint.sort !== undefined || constraint.skip !== undefined || constraint.limit !== undefined) {
        throw new Error("Publication constraints cannot sort or paginate");
      }
      const encoded = encodeQuerySpec(constraint);
      decodeQuerySpec(encoded);
      this.#send({ v: MELDBASE_PROTOCOL_VERSION, type: "policy", callId: frame.callId, constraint: encoded });
    } catch (error) {
      if (controller.signal.aborted) return;
      const code = error instanceof MeldbaseMethodError && error.code === "forbidden" ? "forbidden" : "internal";
      this.#send({ v: MELDBASE_PROTOCOL_VERSION, type: "policy_error", callId: frame.callId, error: { code } });
    } finally {
      this.#calls.delete(frame.callId);
    }
  }

  #send(value: unknown): void {
    if (!this.#socket || this.#state === "stopped") throw new Error("Meldbase worker is not connected");
    this.#socket.send(JSON.stringify(value));
  }

  #abortCalls(error: Error): void {
    for (const call of this.#calls.values()) {
      call.controller.abort();
      call.transaction?.close(error);
    }
    this.#calls.clear();
  }

  #setState(state: WorkerState): void {
    if (this.#state === state) return;
    this.#state = state;
    this.#options.onStateChange?.(state);
  }
}

class RemoteWriteTransaction implements WriteTransaction {
  readonly #callId: string;
  readonly #send: (value: unknown) => void;
  #nextOperation = 1;
  #pending: { readonly opId: string; readonly deferred: Deferred<Value> } | undefined;
  #closed?: Error;
  readonly #invalidatedPublications = new Set<string>();

  constructor(callId: string, send: (value: unknown) => void) {
    this.#callId = callId;
    this.#send = send;
  }

  async get(collection: string, id: string): Promise<Document> {
    const value = await this.#operation("get", collection, id);
    return decodeDocument(encodeValue(value));
  }

  async insert(collection: string, document: InputDocument): Promise<string> {
    const value = await this.#operation("insert", collection, undefined, document);
    if (typeof value !== "string" || !ID_PATTERN.test(value)) throw new Error("Invalid inserted document ID");
    return value;
  }

  async replace(collection: string, id: string, document: InputDocument): Promise<void> {
    await this.#operation("replace", collection, id, document);
  }

  async update(collection: string, id: string, mutation: MutationSpec): Promise<void> {
    await this.#operation("update", collection, id, undefined, mutation);
  }

  async delete(collection: string, id: string): Promise<void> {
    await this.#operation("delete", collection, id);
  }

  async invalidatePublication(collection: string): Promise<void> {
    if (this.#invalidatedPublications.has(collection)) throw new Error("Publication was already invalidated in this transaction");
    this.#invalidatedPublications.add(collection);
    try {
      await this.#operation("invalidate_publication", collection);
    } catch (error) {
      this.#invalidatedPublications.delete(collection);
      throw error;
    }
  }

  receive(frame: Record<string, unknown>): void {
    if (!this.#pending || frame.opId !== this.#pending.opId) throw new Error("Unexpected transaction operation response");
    const pending = this.#pending;
    this.#pending = undefined;
    if (frame.type === "tx_result") {
      pending.deferred.resolve(decodeValue(frame.result));
      return;
    }
    if (!record(frame.error) || typeof frame.error.code !== "string" || !ERROR_PATTERN.test(frame.error.code)) {
      pending.deferred.reject(new Error("Invalid transaction error"));
      return;
    }
    pending.deferred.reject(new MeldbaseMethodError(frame.error.code));
  }

  close(error: Error): void {
    if (this.#closed) return;
    this.#closed = error;
    this.#pending?.deferred.reject(error);
    this.#pending = undefined;
  }

  async #operation(operation: string, collection: string, id?: string, document?: InputDocument, mutation?: MutationSpec): Promise<Value> {
    if (this.#closed) throw this.#closed;
    if (this.#pending) throw new Error("Transactional operations must be awaited sequentially");
    if (!COLLECTION_PATTERN.test(collection)) throw new TypeError("Invalid collection name");
    if (id !== undefined && !ID_PATTERN.test(id)) throw new TypeError("Invalid document ID");
    const opId = `op-${this.#nextOperation++}`;
    const response = deferred<Value>();
    this.#pending = { opId, deferred: response };
    try {
      this.#send({
        v: MELDBASE_PROTOCOL_VERSION, type: "tx_op", callId: this.#callId, opId, operation, collection,
        ...(id !== undefined ? { id } : {}),
        ...(document !== undefined ? { document: encodeInputDocument(document) } : {}),
        ...(mutation !== undefined ? { mutation: encodeMutationSpec(mutation) } : {}),
      });
    } catch (error) {
      this.#pending = undefined;
      throw error;
    }
    return response.promise;
  }
}

function parseWorkerURL(raw: string): string {
  const url = new URL(raw);
  if ((url.protocol !== "ws:" && url.protocol !== "wss:") || !url.host || url.username || url.password || url.search || url.hash) {
    throw new TypeError("Worker URL must be an absolute ws(s) URL without credentials or query parameters");
  }
  return url.toString();
}

function validatePublicationOptions(options: PublicationOptions): void {
  const encodedVersion = options && typeof options.version === "string" ? new TextEncoder().encode(options.version) : new Uint8Array();
  if (!options || typeof options.version !== "string" || options.version.length === 0 || encodedVersion.byteLength > 128 || new TextDecoder().decode(encodedVersion) !== options.version) {
    throw new TypeError("Publication version must contain between 1 and 128 UTF-8 bytes");
  }
  if (!Number.isSafeInteger(options.maxResults) || options.maxResults <= 0 || options.maxResults > DEFAULT_QUERY_LIMITS.maxLimit) {
    throw new TypeError("Publication maxResults is outside query limits");
  }
  validatePolicyFields(options.queryPaths, true);
  validatePolicyFields(options.resultFields, false);
}

function validatePolicyFields(value: "*" | readonly string[], paths: boolean): void {
  if (value === "*") return;
  if (!Array.isArray(value) || value.length > 256) throw new TypeError("Publication field policy must be '*' or a bounded array");
  const seen = new Set<string>();
  for (const field of value) {
    if (typeof field !== "string" || seen.has(field)) throw new TypeError("Publication fields must be unique strings");
    if (paths) {
      if (field.includes("\0")) throw new TypeError("Publication query paths cannot contain NUL");
      decodeQuerySpec({ version: 1, where: { op: "exists", path: field, value: true } });
    } else if (!field || field.includes("\0") || field.includes(".") || field.startsWith("$") || field === "__proto__" || field === "prototype" || field === "constructor") {
      throw new TypeError(`Unsafe publication result field: ${JSON.stringify(field)}`);
    }
    seen.add(field);
  }
}

function record(value: unknown): value is Record<string, any> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function parseJSONRecord(raw: string): Record<string, unknown> {
  let value: unknown;
  try { value = JSON.parse(raw); } catch { throw new Error("Malformed worker JSON"); }
  if (!record(value)) throw new Error("Worker frame must be an object");
  return value;
}

function exactKeys(record_: Record<string, unknown>, expected: readonly string[]): void {
  const actual = Object.keys(record_).sort();
  const wanted = [...expected].sort();
  if (actual.length !== wanted.length || actual.some((key, index) => key !== wanted[index])) throw new Error("Worker frame contains unknown or missing fields");
}

function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error("Unknown Meldbase worker failure");
}

function abortableDelay(milliseconds: number, signal: AbortSignal): Promise<void> {
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

// Keep the imported wire type part of the generated public declaration graph.
export type ServerWireValue = WireValue;
