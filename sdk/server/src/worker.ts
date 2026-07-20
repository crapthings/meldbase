import {
  decodeQuerySpec,
  decodeValue,
  encodeQuerySpec,
  encodeValue,
  MELDBASE_PROTOCOL_VERSION,
} from "@meldbase/client";
import type { ProtocolDescriptor, WireValue } from "@meldbase/client";

import { publish, rpc, transactional, validatePublicationOptions } from "./definitions.js";
import { MeldbaseMethodError, MeldbaseWorkerProtocolError } from "./errors.js";
import { validateWorkerProtocol } from "./protocol.js";
import {
  abortableDelay,
  asError,
  COLLECTION_PATTERN,
  deferred,
  type Deferred,
  exactKeys,
  METHOD_PATTERN,
  parseJSONRecord,
  parseWorkerURL,
  record,
  WORKER_PATTERN,
} from "./shared.js";
import { RemoteWriteTransaction } from "./transaction.js";
import type {
  MethodContext,
  MethodDefinition,
  PublicationContext,
  PublicationDefinition,
  WorkerOptions,
  WorkerSocket,
  WorkerState,
} from "./types.js";

export { publish, rpc, transactional } from "./definitions.js";
export { MeldbaseMethodError, MeldbaseWorkerProtocolError } from "./errors.js";
export type {
  MethodContext,
  MethodDefinition,
  MethodHandler,
  Principal,
  PublicationContext,
  PublicationDefinition,
  PublicationHandler,
  PublicationOptions,
  TransactionalMethodHandler,
  WorkerOptions,
  WorkerSocket,
  WorkerSocketFactory,
  WorkerState,
  WriteTransaction,
} from "./types.js";

interface ActiveCall {
  readonly controller: AbortController;
  readonly transaction?: RemoteWriteTransaction;
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
      case "registered": {
        exactKeys(frame, frame.protocol === undefined ? ["v", "type", "sessionId", "limits"] : ["v", "type", "sessionId", "limits", "protocol"]);
        if (typeof frame.sessionId !== "string" || !record(frame.limits)) throw new Error("Invalid registered frame");
        const descriptor = validateWorkerProtocol(frame.protocol, this.#options.requireProtocol, this.#methods, this.#publications);
        if (descriptor) this.#protocol = descriptor;
        this.#setState("ready");
        this.#ready?.resolve();
        return;
      }
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

// Keep the imported wire type part of the generated public declaration graph.
export type ServerWireValue = WireValue;
