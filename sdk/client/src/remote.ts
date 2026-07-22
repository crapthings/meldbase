import type { CountResult, DeleteResult, Document, Filter, GroupCountResult, InputDocument, MutationResult, MutationSpec, QuerySpec, Update, Value } from "./types.js";
import type { FindOneOptions, QueryOptions } from "./query.js";
import type { SnapshotListener, Unsubscribe } from "./local.js";
import { compileQuery } from "./query.js";
import { compileUpdate, encodeMutationSpec } from "./mutation.js";
import { assertDocumentID, assertSafeKey, cloneDocument, isDocumentID, newDocumentID } from "./safe-value.js";
import { decodeDocument, decodeValue, encodeInputDocument, encodeQuerySpec, encodeValue, type WireQuerySpec, type WireValue } from "./wire.js";
import { decodeProtocolDescriptor, MELDBASE_PROTOCOL_VERSION, supportsProtocol, type ProtocolDescriptor } from "./protocol.js";
import { pageCursorFor, type PageResult } from './cursor.js';

const REALTIME_TICKET_ACCEPT = "application/vnd.meldbase.realtime-ticket+json; capabilities=1";

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
	readonly maxRPCArguments?: number;
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

export class MeldbaseRemoteError extends Error {
	constructor(readonly code: string, readonly status: number, readonly operation: string) {
		super(`Meldbase ${operation} failed: ${code}`);
		this.name = "MeldbaseRemoteError";
	}
}

/** The client has been closed permanently. Create a new client to reconnect. */
export class MeldbaseClientClosedError extends Error {
  constructor() {
    super("Meldbase client is closed; create a new client to reconnect");
    this.name = "MeldbaseClientClosedError";
  }
}

/**
 * The insert may have reached the server, but the client could not verify its
 * result. Use documentId to reconcile before retrying the same logical write.
 */
export class MeldbaseInsertUnknownResultError extends Error {
  readonly cause: unknown;
  constructor(readonly documentId: string, cause: unknown) {
    super(`Insert result is unknown for document ${documentId}; the document may have been created`);
    this.name = "MeldbaseInsertUnknownResultError";
    this.cause = cause;
  }
}

export class MeldbaseRPCError extends MeldbaseRemoteError {
	constructor(code: string, status: number) {
		super(code, status, "RPC");
		this.name = "MeldbaseRPCError";
	}
}

export class MeldbaseRPCUnknownResultError extends Error {
  constructor(readonly requestId: string) {
    super("Realtime RPC connection closed before a result was received; the method may have executed");
    this.name = "MeldbaseRPCUnknownResultError";
  }
}

export class MeldbaseProtocolError extends Error {
  readonly required: readonly string[];
  constructor(required: readonly string[]) {
    super(`Meldbase realtime protocol does not support: ${required.join(", ")}`);
    this.name = "MeldbaseProtocolError";
    this.required = Object.freeze([...required]);
  }
}

interface ActiveSubscription {
  readonly requestId: string;
  readonly collection: string;
  readonly query: WireQuerySpec;
  readonly listener: SnapshotListener<Document>;
  readonly onStatus?: (status: SyncStatus) => void;
  token: string | undefined;
  serverId: string | undefined;
  documents: Document[];
}

interface ActiveRPCCall {
  readonly requestId: string;
  readonly method: string;
  readonly arguments: readonly WireValue[];
  readonly idempotencyKey?: string;
  readonly resolve: (value: Value) => void;
  readonly reject: (error: Error) => void;
  readonly signal?: AbortSignal;
  readonly abort?: () => void;
  sent: boolean;
}

export class MeldbaseClient {
  readonly #baseUrl: string;
  readonly #fetch: typeof globalThis.fetch;
  readonly #accessToken?: ClientOptions["accessToken"];
  readonly #realtime: RealtimeConnection;
  readonly #maxInboundBytes: number;
  readonly #maxSnapshotDocuments: number;
  readonly #allowedRealtimeOrigins: ReadonlySet<string>;
	readonly #maxRPCArguments: number;
  #closed = false;

  constructor(options: ClientOptions) {
    this.#baseUrl = normalizeBaseURL(options.baseUrl);
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.#accessToken = options.accessToken;
    this.#maxInboundBytes = positiveLimit(options.maxInboundBytes, 4 * 1024 * 1024, "maxInboundBytes");
    this.#maxSnapshotDocuments = positiveLimit(options.maxSnapshotDocuments, 10_000, "maxSnapshotDocuments");
		this.#maxRPCArguments = positiveLimit(options.maxRPCArguments, 32, "maxRPCArguments");
    const base = new URL(this.#baseUrl);
    const defaultRealtimeOrigin = `${base.protocol === "https:" ? "wss:" : "ws:"}//${base.host}`;
    this.#allowedRealtimeOrigins = new Set((options.allowedRealtimeOrigins ?? [defaultRealtimeOrigin]).map(normalizeRealtimeOrigin));
    this.#realtime = new RealtimeConnection(options, () => this.createTicket());
  }

  collection<T extends Document = Document>(name: string): RemoteCollection<T> {
    this.assertOpen();
    assertCollectionName(name);
    return new RemoteCollection(name, this);
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#realtime.close();
  }

  get realtimeProtocol(): ProtocolDescriptor | undefined { return this.#realtime.protocol; }

	async call<T extends Value = Value>(method: string, args: readonly Value[] = [], options: CallOptions = {}): Promise<T> {
		this.assertOpen();
		if (!/^[A-Za-z][A-Za-z0-9_.-]{0,127}$/.test(method)) throw new Error("Invalid RPC method name");
		if (args.length > this.#maxRPCArguments) throw new Error("RPC argument limit exceeded");
		if (options.transport !== undefined && options.transport !== "http" && options.transport !== "realtime") throw new Error("Invalid RPC transport");
		if (options.idempotencyKey !== undefined && !validRPCIdempotencyKey(options.idempotencyKey)) throw new Error("Invalid RPC idempotency key");
		if (options.transport === "realtime") return this.#realtime.call<T>(method, args, options.signal, options.idempotencyKey);
		const requestId = crypto.randomUUID();
		const response = await this.#fetch(`${this.#baseUrl}/v1/rpc`, {
			method: "POST", headers: await this.headers(),
			body: JSON.stringify({ v: MELDBASE_PROTOCOL_VERSION, type: "call", requestId, ...(options.idempotencyKey ? { idempotencyKey: options.idempotencyKey } : {}), method, arguments: args.map(encodeValue) }),
			...(options.signal ? { signal: options.signal } : {}),
		});
		const body = await boundedJSON(response, this.#maxInboundBytes);
		if (!response.ok) {
			if (record(body) && body.v !== undefined) {
				if (!exactKeys(body, ["v", "type", "requestId", "error"]) || body.v !== MELDBASE_PROTOCOL_VERSION || body.type !== "error" || body.requestId !== requestId ||
					!record(body.error) || !exactKeys(body.error, ["code"]) || !validRPCErrorCode(body.error.code)) {
					throw new Error("Malformed RPC error response");
				}
			}
			const code = record(body) && record(body.error) && validRPCErrorCode(body.error.code) ? body.error.code : "unknown";
			throw new MeldbaseRPCError(code, response.status);
		}
		if (!record(body) || !exactKeys(body, ["v", "type", "requestId", "result"]) || body.v !== MELDBASE_PROTOCOL_VERSION || body.type !== "result" || body.requestId !== requestId || body.result === undefined) {
			throw new Error("Malformed RPC response");
		}
		return decodeValue(body.result) as T;
	}

  async fetchQuery<T extends Document>(collection: string, query: QuerySpec, signal?: AbortSignal): Promise<T[]> {
    this.assertOpen();
    assertCollectionName(collection);
    const response = await this.#fetch(`${this.#baseUrl}/v1/collections/${encodeURIComponent(collection)}/query`, {
      method: "POST",
      headers: await this.headers(),
      body: JSON.stringify({ version: 1, query: encodeQuerySpec(query) }),
      ...(signal ? { signal } : {}),
    });
    if (!response.ok) await throwRemoteError(response, this.#maxInboundBytes, "query");
    const body = await boundedJSON(response, this.#maxInboundBytes);
    if (!record(body) || !exactKeys(body, ["version", "documents"]) || body.version !== 1 || !Array.isArray(body.documents)) throw new Error("Malformed query response");
    if (body.documents.length > this.#maxSnapshotDocuments) throw new Error("Query response document limit exceeded");
    return body.documents.map((item) => {
      const document = decodeDocument(item) as T;
      assertDocumentID(document._id, "Remote document _id");
      return document;
    });
  }

  async count(collection: string, query: QuerySpec, signal?: AbortSignal): Promise<CountResult> {
    this.assertOpen();
    assertCollectionName(collection);
    const response = await this.#fetch(`${this.#baseUrl}/v1/collections/${encodeURIComponent(collection)}/count`, {
      method: "POST", headers: await this.headers(), body: JSON.stringify({ version: 1, query: encodeQuerySpec(query) }), ...(signal ? { signal } : {}),
    });
    if (!response.ok) await throwRemoteError(response, 64 * 1024, "count");
    const body = await boundedJSON(response, 64 * 1024);
    if (!record(body) || !exactKeys(body, ["version", "count", "capped"]) || body.version !== 1 || !boundedCount(body.count) || typeof body.capped !== "boolean") throw new Error("Malformed count response");
    return { count: body.count, capped: body.capped };
  }

  async groupCount(collection: string, query: QuerySpec, groupBy: string, signal?: AbortSignal): Promise<GroupCountResult> {
    this.assertOpen();
    assertCollectionName(collection);
    assertSafeKey(groupBy, "group field");
    const response = await this.#fetch(`${this.#baseUrl}/v1/collections/${encodeURIComponent(collection)}/group-count`, {
      method: "POST", headers: await this.headers(), body: JSON.stringify({ version: 1, query: encodeQuerySpec(query), groupBy }), ...(signal ? { signal } : {}),
    });
    if (!response.ok) await throwRemoteError(response, this.#maxInboundBytes, "group count");
    const body = await boundedJSON(response, this.#maxInboundBytes);
    if (!record(body) || !exactKeys(body, ["version", "groups", "capped"]) || body.version !== 1 || !Array.isArray(body.groups) || body.groups.length > 100 || typeof body.capped !== "boolean") throw new Error("Malformed group count response");
    const groups = body.groups.map((item) => {
      if (!record(item) || !exactKeys(item, ["key", "count"]) || item.key === undefined || !boundedCount(item.count)) throw new Error("Malformed group count entry");
      return { key: decodeValue(item.key), count: item.count };
    });
    return { groups, capped: body.capped };
  }

  async insertOne<T extends Document>(collection: string, document: InputDocument, signal?: AbortSignal): Promise<T> {
    this.assertOpen();
    assertCollectionName(collection);
    const id = document._id ?? newDocumentID();
    const input: InputDocument = document._id === undefined ? { ...document, _id: id } : document;
    const body = JSON.stringify({ version: 1, document: encodeInputDocument(input) });
    const headers = await this.headers();
    let response: Response;
    try {
      response = await this.#fetch(`${this.#baseUrl}/v1/collections/${encodeURIComponent(collection)}/documents`, {
        method: "POST", headers, body, ...(signal ? { signal } : {}),
      });
    } catch (error) {
      throw new MeldbaseInsertUnknownResultError(id, error);
    }
    if (!response.ok) await throwRemoteError(response, this.#maxInboundBytes, "insert");
    try {
      const result = await boundedJSON(response, this.#maxInboundBytes);
      if (!record(result) || !exactKeys(result, ["version", "document"]) || result.version !== 1 || result.document === undefined) throw new Error("Malformed insert response");
      const inserted = decodeDocument(result.document) as T;
      assertDocumentID(inserted._id, "Remote document _id");
      if (inserted._id !== id) throw new Error("Insert response changed document ID");
      return inserted;
    } catch (error) {
      throw new MeldbaseInsertUnknownResultError(id, error);
    }
  }

  async executeMutation(collection: string, action: "updateOne" | "updateMany" | "deleteOne" | "deleteMany", query: QuerySpec, update?: MutationSpec, signal?: AbortSignal): Promise<MutationResult | DeleteResult> {
    this.assertOpen();
    assertCollectionName(collection);
    if (action !== "updateOne" && action !== "updateMany" && action !== "deleteOne" && action !== "deleteMany") throw new Error("Invalid mutation action");
    if ((action.startsWith("update") && update === undefined) || (action.startsWith("delete") && update !== undefined)) throw new Error("Invalid mutation payload");
    const response = await this.#fetch(`${this.#baseUrl}/v1/collections/${encodeURIComponent(collection)}/mutations`, {
      method: "POST", headers: await this.headers(), body: JSON.stringify({ version: 1, action, query: encodeQuerySpec(query), ...(update ? { update: encodeMutationSpec(update) } : {}) }), ...(signal ? { signal } : {}),
    });
    if (!response.ok) await throwRemoteError(response, 64 * 1024, "mutation");
    const body = await boundedJSON(response, 64 * 1024);
    if (!record(body)) throw new Error("Malformed mutation response");
    if (action === "deleteOne" || action === "deleteMany") {
      if (!exactKeys(body, ["version", "deletedCount"]) || body.version !== 1 || !boundedCount(body.deletedCount)) throw new Error("Malformed delete response"); return { deletedCount: body.deletedCount };
    }
    if (!exactKeys(body, ["version", "matchedCount", "modifiedCount"]) || body.version !== 1 || !boundedCount(body.matchedCount) || !boundedCount(body.modifiedCount) || body.modifiedCount > body.matchedCount) throw new Error("Malformed update response");
    return { matchedCount: body.matchedCount, modifiedCount: body.modifiedCount };
  }

  subscribe(collection: string, query: QuerySpec, listener: SnapshotListener<Document>, options: SubscribeOptions): Unsubscribe {
    this.assertOpen();
    assertCollectionName(collection);
    return this.#realtime.subscribe(collection, encodeQuerySpec(query), listener, options.onStatus);
  }

  private assertOpen(): void {
    if (this.#closed) throw new MeldbaseClientClosedError();
  }

  private async headers(): Promise<Record<string, string>> {
    const token = await this.#accessToken?.();
    return { "content-type": "application/json", ...(token ? { authorization: `Bearer ${token}` } : {}) };
  }

  private async createTicket(): Promise<RealtimeTicket> {
    const response = await this.#fetch(`${this.#baseUrl}/v1/realtime/tickets`, {
      method: "POST", headers: { ...(await this.headers()), accept: REALTIME_TICKET_ACCEPT },
    });
    if (!response.ok) throw new Error(`Realtime ticket failed (${response.status})`);
    const body = await boundedJSON(response, 64 * 1024);
    if (!record(body) || !exactKeys(body, body.protocol === undefined ? ["url", "ticket"] : ["url", "ticket", "protocol"]) ||
        typeof body.url !== "string" || body.url.length === 0 || body.url.length > 4096 ||
        typeof body.ticket !== "string" || body.ticket.length === 0 || body.ticket.length > 4096) {
      throw new Error("Malformed realtime ticket response");
    }
    let realtimeURL: URL;
    try { realtimeURL = new URL(body.url); } catch { throw new Error("Malformed realtime URL"); }
    if ((realtimeURL.protocol !== "wss:" && realtimeURL.protocol !== "ws:") || !this.#allowedRealtimeOrigins.has(realtimeURL.origin)) {
      throw new Error("Realtime URL origin is not allowed");
    }
    if (new URL(this.#baseUrl).protocol === "https:" && realtimeURL.protocol !== "wss:") throw new Error("Secure baseUrl requires wss realtime");
    let protocol: ProtocolDescriptor | undefined;
    try {
      protocol = body.protocol === undefined ? undefined : decodeProtocolDescriptor(body.protocol);
    } catch {
      throw new MeldbaseProtocolError(Object.freeze(["valid_descriptor"]));
    }
    return { url: body.url, ticket: body.ticket, ...(protocol ? { protocol } : {}) };
  }
}

export class RemoteCollection<T extends Document> {
  constructor(readonly name: string, private readonly client: MeldbaseClient) {}
  find(filter: Filter = {}, options: QueryOptions = {}): RemoteLiveQuery<T> {
    return new RemoteLiveQuery(this.client, this.name, compileQuery(filter, options), options.first !== undefined);
  }
  async findOne(filter: Filter = {}, options: FindOneOptions & { readonly signal?: AbortSignal } = {}): Promise<T | undefined> {
    const { signal, ...queryOptions } = options;
    const documents = await this.client.fetchQuery<T>(this.name, compileQuery(filter, { ...queryOptions, limit: 1 }), signal);
    return documents[0];
  }
  count(filter: Filter = {}, options: { readonly signal?: AbortSignal } = {}): Promise<CountResult> { return this.client.count(this.name, compileQuery(filter), options.signal); }
  groupCount(filter: Filter | undefined, groupBy: string, options: { readonly signal?: AbortSignal } = {}): Promise<GroupCountResult> { return this.client.groupCount(this.name, compileQuery(filter ?? {}), groupBy, options.signal); }
  insertOne(document: InputDocument, options: { readonly signal?: AbortSignal } = {}): Promise<T> { return this.client.insertOne<T>(this.name, document, options.signal); }
  updateOne(filter: Filter, update: Update, options: { readonly signal?: AbortSignal } = {}): Promise<MutationResult> { return this.client.executeMutation(this.name, "updateOne", compileQuery(filter), compileUpdate(update), options.signal) as Promise<MutationResult>; }
  updateMany(filter: Filter, update: Update, options: { readonly signal?: AbortSignal } = {}): Promise<MutationResult> { return this.client.executeMutation(this.name, "updateMany", compileQuery(filter), compileUpdate(update), options.signal) as Promise<MutationResult>; }
  deleteOne(filter: Filter, options: { readonly signal?: AbortSignal } = {}): Promise<DeleteResult> { return this.client.executeMutation(this.name, "deleteOne", compileQuery(filter), undefined, options.signal) as Promise<DeleteResult>; }
  deleteMany(filter: Filter, options: { readonly signal?: AbortSignal } = {}): Promise<DeleteResult> { return this.client.executeMutation(this.name, "deleteMany", compileQuery(filter), undefined, options.signal) as Promise<DeleteResult>; }
}

export class RemoteLiveQuery<T extends Document> {
  readonly mode = "remote" as const;
  constructor(private readonly client: MeldbaseClient, readonly collection: string, readonly spec: QuerySpec, private readonly seekPagination = false) {}
  fetch(options: { readonly signal?: AbortSignal } = {}): Promise<T[]> { return this.client.fetchQuery<T>(this.collection, this.spec, options.signal); }
  async fetchPage(options: { readonly signal?: AbortSignal } = {}): Promise<PageResult<T>> {
    if (!this.seekPagination) throw new Error("fetchPage requires a query created with first");
    const documents = await this.fetch(options); const last = documents.at(-1);
    return { documents, ...(last && this.spec.sort ? { nextCursor: pageCursorFor(last, this.spec.sort) } : {}) };
  }
  subscribe(listener: SnapshotListener<T>, options: SubscribeOptions = {}): Unsubscribe {
    return this.client.subscribe(this.collection, this.spec, listener as SnapshotListener<Document>, options);
  }
}

class RealtimeConnection {
  readonly #socketFactory: (url: string) => WebSocketLike;
  readonly #ticket: () => Promise<RealtimeTicket>;
  readonly #subscriptions = new Map<string, ActiveSubscription>();
  readonly #calls = new Map<string, ActiveRPCCall>();
  readonly #maxInboundBytes: number;
  readonly #maxSnapshotDocuments: number;
  readonly #maxDeltaOperations: number;
  readonly #minDelay: number;
  readonly #maxDelay: number;
  #socket: WebSocketLike | undefined;
  #authenticated = false;
  #connecting = false;
  #epoch = 0;
  #attempt = 0;
  #closed = false;
  #protocol: ProtocolDescriptor | undefined;
  #protocolError: MeldbaseProtocolError | undefined;
  #retryTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(options: ClientOptions, ticket: () => Promise<RealtimeTicket>) {
    this.#socketFactory = options.webSocketFactory ?? ((url) => new WebSocket(url));
    this.#ticket = ticket;
    this.#maxInboundBytes = positiveLimit(options.maxInboundBytes, 4 * 1024 * 1024, "maxInboundBytes");
    this.#maxSnapshotDocuments = positiveLimit(options.maxSnapshotDocuments, 10_000, "maxSnapshotDocuments");
    const defaultDeltaOperations = Math.min(Number.MAX_SAFE_INTEGER, this.#maxSnapshotDocuments * 4);
    this.#maxDeltaOperations = positiveLimit(options.maxDeltaOperations, defaultDeltaOperations, "maxDeltaOperations");
    this.#minDelay = positiveLimit(options.reconnect?.minDelayMs, 250, "reconnect.minDelayMs");
    this.#maxDelay = positiveLimit(options.reconnect?.maxDelayMs, 15_000, "reconnect.maxDelayMs");
    if (this.#minDelay > this.#maxDelay) throw new Error("reconnect.minDelayMs must not exceed reconnect.maxDelayMs");
  }

  subscribe(collection: string, query: WireQuerySpec, listener: SnapshotListener<Document>, onStatus?: (status: SyncStatus) => void): Unsubscribe {
    if (this.#closed) throw new MeldbaseClientClosedError();
    const requestId = crypto.randomUUID();
    const subscription: ActiveSubscription = { requestId, collection, query, listener, token: undefined, serverId: undefined, documents: [], ...(onStatus ? { onStatus } : {}) };
		const capabilityError = this.capabilityError(["query.delta"]);
		if (capabilityError) {
			this.status(subscription, { state: "error", error: capabilityError });
			return () => this.status(subscription, { state: "closed" });
		}
    this.#subscriptions.set(requestId, subscription);
    this.status(subscription, { state: "idle" });
    if (this.#protocolError) this.status(subscription, { state: "error", error: this.#protocolError });
    else if (this.#authenticated) this.sendSubscription(subscription);
    else void this.ensureConnected();
    return () => {
      const active = this.#subscriptions.get(requestId);
      if (!active) return;
      this.#subscriptions.delete(requestId);
      if (active.serverId) this.send({ v: MELDBASE_PROTOCOL_VERSION, type: "unsubscribe", subscriptionId: active.serverId });
      this.status(active, { state: "closed" });
      this.closeIdleSocket();
    };
  }

  call<T extends Value>(method: string, args: readonly Value[], signal?: AbortSignal, idempotencyKey?: string): Promise<T> {
    if (this.#closed) return Promise.reject(new Error("Meldbase client is closed"));
    if (this.#protocolError) return Promise.reject(this.#protocolError);
		const capabilityError = this.capabilityError(["rpc", ...(signal ? ["rpc.cancel"] : []), ...(idempotencyKey ? ["rpc.idempotency"] : [])]);
		if (capabilityError) return Promise.reject(capabilityError);
    if (signal?.aborted) return Promise.reject(abortReason(signal));
    const requestId = crypto.randomUUID();
    return new Promise<T>((resolve, reject) => {
      const abort = signal ? () => {
        const active = this.#calls.get(requestId);
        if (!active) return;
        this.#calls.delete(requestId);
        if (active.sent) this.send({ v: MELDBASE_PROTOCOL_VERSION, type: "cancel", requestId });
        this.cleanupCall(active);
        reject(abortReason(signal));
        this.closeIdleSocket();
      } : undefined;
      const call: ActiveRPCCall = {
        requestId,
        method,
        arguments: args.map(encodeValue),
        ...(idempotencyKey ? { idempotencyKey } : {}),
        resolve: (value) => resolve(value as T),
        reject,
        ...(signal ? { signal } : {}),
        ...(abort ? { abort } : {}),
        sent: false,
      };
      this.#calls.set(requestId, call);
      if (signal && abort) signal.addEventListener("abort", abort, { once: true });
      if (this.#authenticated) this.sendCall(call);
      else void this.ensureConnected();
    });
  }

  close(): void {
    this.#closed = true;
    this.#epoch += 1;
    if (this.#retryTimer) clearTimeout(this.#retryTimer);
    this.#socket?.close(1000, "client closed");
    for (const subscription of this.#subscriptions.values()) this.status(subscription, { state: "closed" });
    this.#subscriptions.clear();
    this.rejectCalls(new Error("Meldbase client is closed"));
    this.#socket = undefined;
    this.#authenticated = false;
    this.#connecting = false;
  }

  private async ensureConnected(): Promise<void> {
    if (this.#closed || this.#socket || this.#connecting || !this.hasWork()) return;
    this.#connecting = true;
    const epoch = ++this.#epoch;
    this.statusAll({ state: "authenticating" });
    try {
      const ticket = await this.#ticket();
      if (epoch !== this.#epoch || this.#closed) return;
      this.validateProtocol(ticket.protocol);
      this.statusAll({ state: "connecting" });
      const socket = this.#socketFactory(ticket.url);
      this.#socket = socket;
      this.#authenticated = false;
      socket.addEventListener("open", () => {
        if (epoch !== this.#epoch) return;
        this.send({ v: MELDBASE_PROTOCOL_VERSION, type: "authenticate", ticket: ticket.ticket });
      });
      socket.addEventListener("message", (event) => { if (epoch === this.#epoch) this.onMessage(event as MessageEvent); });
      socket.addEventListener("close", () => { if (epoch === this.#epoch) this.disconnected(); });
      socket.addEventListener("error", () => { if (epoch === this.#epoch) this.connectionFailure(new Error("Realtime connection error")); });
    } catch (error) {
      if (epoch !== this.#epoch) return;
      const connectionError = asError(error);
		if (connectionError instanceof MeldbaseProtocolError) {
			this.#protocolError = connectionError;
			this.statusAll({ state: "error", error: connectionError });
			this.rejectCalls(connectionError);
			return;
		}
      this.statusAll({ state: "error", error: connectionError });
      this.rejectCalls(connectionError);
      this.scheduleRetry();
    } finally {
      if (epoch === this.#epoch) this.#connecting = false;
    }
  }

  private onMessage(event: MessageEvent): void {
    if (typeof event.data !== "string" || new TextEncoder().encode(event.data).byteLength > this.#maxInboundBytes) {
      this.protocolFailure("Realtime message exceeds safety limits"); return;
    }
    let message: unknown;
    try { message = JSON.parse(event.data); } catch { this.protocolFailure("Malformed realtime JSON"); return; }
    if (!record(message) || message.v !== MELDBASE_PROTOCOL_VERSION || typeof message.type !== "string") { this.protocolFailure("Malformed realtime envelope"); return; }
    if (message.type === "authenticated") {
      if (!exactKeys(message, ["v", "type"])) { this.protocolFailure("Malformed authenticated response"); return; }
      this.#authenticated = true;
      this.#attempt = 0;
      for (const subscription of this.#subscriptions.values()) this.sendSubscription(subscription);
      for (const call of this.#calls.values()) if (!call.sent) this.sendCall(call);
      return;
    }
    if (message.type === "result") {
      if (typeof message.requestId !== "string") { this.protocolFailure("Malformed RPC result"); return; }
      const call = this.#calls.get(message.requestId);
      if (!call) return;
      if (!exactKeys(message, ["v", "type", "requestId", "result"]) || message.result === undefined) {
        this.protocolFailure("Malformed RPC result"); return;
      }
      try {
        this.settleCall(call, decodeValue(message.result));
      } catch (error) {
        this.protocolFailure(asError(error).message);
      }
      return;
    }
    if (message.type === "snapshot") {
      if (typeof message.requestId !== "string" || typeof message.subscriptionId !== "string" || typeof message.token !== "string" || !Array.isArray(message.documents)) {
        this.protocolFailure("Malformed snapshot"); return;
      }
      if (message.documents.length > this.#maxSnapshotDocuments) { this.protocolFailure("Snapshot document limit exceeded"); return; }
      const subscription = this.#subscriptions.get(message.requestId);
      if (!subscription) return;
      try {
        const documents = message.documents.map(decodeDocument).map(cloneDocument);
        subscription.serverId = message.subscriptionId;
        subscription.token = message.token;
        subscription.documents = documents;
        queueMicrotask(() => subscription.listener(documents.map(cloneDocument)));
        this.status(subscription, { state: "live", token: message.token });
      } catch (error) { this.protocolFailure(asError(error).message); }
      return;
    }
    if (message.type === "resumed") {
      if (typeof message.requestId !== "string" || typeof message.subscriptionId !== "string" || typeof message.token !== "string") {
        this.protocolFailure("Malformed resumed response"); return;
      }
      const subscription = this.#subscriptions.get(message.requestId);
      if (!subscription) return;
      if (!subscription.token || subscription.token !== message.token) {
        this.protocolFailure("Resume token chain mismatch"); return;
      }
      subscription.serverId = message.subscriptionId;
      this.status(subscription, { state: "live", token: message.token });
      return;
    }
    if (message.type === "delta") {
      if (typeof message.requestId !== "string" || typeof message.subscriptionId !== "string" ||
        typeof message.fromToken !== "string" || typeof message.token !== "string" || !Array.isArray(message.operations)) {
        this.protocolFailure("Malformed delta"); return;
      }
      if (message.operations.length === 0 || message.operations.length > this.#maxDeltaOperations) {
        this.protocolFailure("Delta operation limit exceeded"); return;
      }
      const subscription = this.#subscriptions.get(message.requestId);
      if (!subscription) return;
      if (!subscription.token || subscription.serverId !== message.subscriptionId || subscription.token !== message.fromToken || message.token === message.fromToken) {
        this.protocolFailure("Delta token chain mismatch"); return;
      }
      try {
        const documents = applyWireDelta(subscription.documents, message.operations, this.#maxSnapshotDocuments);
        subscription.documents = documents;
        subscription.token = message.token;
        queueMicrotask(() => subscription.listener(documents.map(cloneDocument)));
        this.status(subscription, { state: "live", token: message.token });
      } catch (error) { this.protocolFailure(asError(error).message); }
      return;
    }
    if (message.type === "resync_required" && typeof message.requestId === "string") {
      const subscription = this.#subscriptions.get(message.requestId);
      if (subscription) { subscription.token = undefined; subscription.serverId = undefined; this.status(subscription, { state: "resyncing" }); this.send({ v: MELDBASE_PROTOCOL_VERSION, type: "subscribe", mode: "delta", requestId: subscription.requestId, collection: subscription.collection, query: subscription.query }); }
      return;
    }
    if (message.type === "error" && typeof message.requestId === "string") {
      const call = this.#calls.get(message.requestId);
      if (call) {
        if (!exactKeys(message, ["v", "type", "requestId", "error"]) || !record(message.error) ||
          !exactKeys(message.error, ["code"]) || !validRPCErrorCode(message.error.code)) {
          this.protocolFailure("Malformed RPC error"); return;
        }
        this.settleCall(call, new MeldbaseRPCError(message.error.code, 0));
        return;
      }
      const subscription = this.#subscriptions.get(message.requestId);
      if (subscription) {
        if (!exactKeys(message, ["v", "type", "requestId", "error"]) || !record(message.error) ||
          !exactKeys(message.error, ["code"]) || !validRPCErrorCode(message.error.code)) {
          this.protocolFailure("Malformed subscription error"); return;
        }
        this.status(subscription, { state: "error", error: new MeldbaseRemoteError(message.error.code, 0, "subscription") });
      }
    }
  }

  private disconnected(): void {
    this.#socket = undefined;
    this.#authenticated = false;
    this.#connecting = false;
    this.rejectCallsUnknown();
    this.statusAll({ state: "stale" });
    this.scheduleRetry();
  }

  private connectionFailure(error: Error): void {
    const socket = this.#socket;
    this.#socket = undefined;
    this.#authenticated = false;
    this.#connecting = false;
    this.#epoch += 1;
    this.rejectCallsUnknown();
    this.statusAll({ state: "stale", error });
    socket?.close(1011, "realtime connection error");
    this.scheduleRetry();
  }

  private scheduleRetry(): void {
    if (this.#closed || this.#subscriptions.size === 0 || this.#retryTimer) return;
    const base = Math.min(this.#maxDelay, this.#minDelay * 2 ** this.#attempt++);
    const delay = Math.floor(base * (0.75 + Math.random() * 0.5));
    this.#retryTimer = setTimeout(() => { this.#retryTimer = undefined; void this.ensureConnected(); }, delay);
  }

  private protocolFailure(reason: string): void {
    const socket = this.#socket;
    this.#socket = undefined;
    this.#authenticated = false;
    this.#connecting = false;
    this.#epoch += 1;
    const error = new Error(reason);
    this.rejectCalls(error);
    this.statusAll({ state: "error", error });
    socket?.close(1002, reason.slice(0, 100));
  }

  private send(message: object): void {
    if (this.#socket?.readyState === 1) this.#socket.send(JSON.stringify(message));
  }

  private closeIdleSocket(): void {
    if (this.hasWork()) return;
    this.#epoch += 1;
    if (this.#retryTimer) {
      clearTimeout(this.#retryTimer);
      this.#retryTimer = undefined;
    }
    const socket = this.#socket;
    this.#socket = undefined;
    this.#authenticated = false;
    this.#connecting = false;
    socket?.close(1000, "no active work");
  }

  private hasWork(): boolean { return this.#subscriptions.size !== 0 || this.#calls.size !== 0; }

  get protocol(): ProtocolDescriptor | undefined { return this.#protocol; }

  private validateProtocol(protocol: ProtocolDescriptor | undefined): void {
		this.#protocol = protocol;
		if (!protocol) {
			throw new MeldbaseProtocolError(["protocol.discovery"]);
		}
		const required = new Set<string>();
		if (this.#subscriptions.size > 0) required.add("query.delta");
		if (this.#calls.size > 0) required.add("rpc");
		for (const call of this.#calls.values()) {
			if (call.signal) required.add("rpc.cancel");
			if (call.idempotencyKey) required.add("rpc.idempotency");
		}
		const missing = [...required].filter((capability) => !protocol.capabilities.includes(capability));
		if (!supportsProtocol(protocol, MELDBASE_PROTOCOL_VERSION) || missing.length > 0) {
			if (!protocol.versions.includes(MELDBASE_PROTOCOL_VERSION)) missing.unshift(`version.${MELDBASE_PROTOCOL_VERSION}`);
			throw new MeldbaseProtocolError(Object.freeze(missing));
		}
	}

	private capabilityError(required: readonly string[]): MeldbaseProtocolError | undefined {
		if (!this.#protocol) return undefined;
		const missing = required.filter((capability) => !this.#protocol!.capabilities.includes(capability));
		return missing.length === 0 ? undefined : new MeldbaseProtocolError(missing);
	}

  private sendSubscription(subscription: ActiveSubscription): void {
		const canResume = !this.#protocol || this.#protocol.capabilities.includes("query.resume");
		const resumeToken = canResume ? subscription.token : undefined;
		if (!canResume) subscription.token = undefined;
		this.status(subscription, { state: resumeToken ? "resyncing" : "connecting", ...(resumeToken ? { token: resumeToken } : {}) });
		this.send({ v: MELDBASE_PROTOCOL_VERSION, type: "subscribe", mode: "delta", requestId: subscription.requestId, collection: subscription.collection, query: subscription.query, ...(resumeToken ? { resumeToken } : {}) });
  }

  private sendCall(call: ActiveRPCCall): void {
    if (call.sent) return;
    call.sent = true;
    this.send({ v: MELDBASE_PROTOCOL_VERSION, type: "call", requestId: call.requestId, ...(call.idempotencyKey ? { idempotencyKey: call.idempotencyKey } : {}), method: call.method, arguments: call.arguments });
  }

  private settleCall(call: ActiveRPCCall, outcome: Value | Error): void {
    if (!this.#calls.delete(call.requestId)) return;
    this.cleanupCall(call);
    if (outcome instanceof Error) call.reject(outcome);
    else call.resolve(outcome);
    this.closeIdleSocket();
  }

  private cleanupCall(call: ActiveRPCCall): void {
    if (call.signal && call.abort) call.signal.removeEventListener("abort", call.abort);
  }

  private rejectCalls(error: Error): void {
    const calls = [...this.#calls.values()];
    this.#calls.clear();
    for (const call of calls) {
      this.cleanupCall(call);
      call.reject(error);
    }
  }

  private rejectCallsUnknown(): void {
    const calls = [...this.#calls.values()];
    this.#calls.clear();
    for (const call of calls) {
      this.cleanupCall(call);
      call.reject(new MeldbaseRPCUnknownResultError(call.requestId));
    }
  }

  private statusAll(status: SyncStatus): void { for (const subscription of this.#subscriptions.values()) this.status(subscription, status); }
  private status(subscription: ActiveSubscription, status: SyncStatus): void { if (subscription.onStatus) queueMicrotask(() => subscription.onStatus?.(status)); }
}

function record(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value); }
function asError(error: unknown): Error { return error instanceof Error ? error : new Error(String(error)); }
function abortReason(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) return signal.reason;
  const error = new Error("RPC call aborted");
  error.name = "AbortError";
  return error;
}
function validRPCErrorCode(value: unknown): value is string { return typeof value === "string" && /^[a-z][a-z0-9_]{0,63}$/.test(value); }
function validRPCIdempotencyKey(value: string): boolean { return /^[A-Za-z0-9_-]{22,128}$/.test(value); }
function boundedCount(value: unknown): value is number { return typeof value === "number" && Number.isSafeInteger(value) && value >= 0; }

function assertCollectionName(value: string): void {
  if (!/^[A-Za-z][A-Za-z0-9_-]{0,127}$/.test(value)) throw new Error("Invalid collection name");
}

function normalizeBaseURL(value: string): string {
  let url: URL;
  try { url = new URL(value); } catch { throw new Error("baseUrl must be an http(s) URL"); }
  if ((url.protocol !== "http:" && url.protocol !== "https:") || url.username || url.password || url.search || url.hash) {
    throw new Error("baseUrl must be an http(s) URL without credentials, query, or fragment");
  }
  return url.toString().replace(/\/$/, "");
}

function normalizeRealtimeOrigin(value: string): string {
  let url: URL;
  try { url = new URL(value); } catch { throw new Error("allowedRealtimeOrigins must contain ws(s) origins"); }
  if ((url.protocol !== "ws:" && url.protocol !== "wss:") || url.username || url.password || url.search || url.hash || url.pathname !== "/") {
    throw new Error("allowedRealtimeOrigins must contain ws(s) origins");
  }
  return url.origin;
}

async function throwRemoteError(response: Response, maxBytes: number, operation: string): Promise<never> {
  const body = await boundedJSON(response, maxBytes);
  if (!record(body) || !exactKeys(body, ["error"]) || !record(body.error) ||
    !exactKeys(body.error, ["code"]) || !validRPCErrorCode(body.error.code)) {
    throw new Error(`Malformed ${operation} error response`);
  }
  throw new MeldbaseRemoteError(body.error.code, response.status, operation);
}

function positiveLimit(value: number | undefined, fallback: number, name: string): number {
  const limit = value ?? fallback;
  if (!Number.isSafeInteger(limit) || limit <= 0) throw new Error(`${name} must be a positive safe integer`);
  return limit;
}

interface WireDeltaNode {
  readonly id: string;
  document: Document;
  previous: WireDeltaNode | undefined;
  next: WireDeltaNode | undefined;
}

function applyWireDelta(current: readonly Document[], operations: readonly unknown[], maximum: number): Document[] {
  if (current.length > maximum) throw new Error("Invalid local delta state");
  const byId = new Map<string, WireDeltaNode>();
  let head: WireDeltaNode | undefined;
  let tail: WireDeltaNode | undefined;
  const insertBefore = (node: WireDeltaNode, anchor: WireDeltaNode | undefined): void => {
    if (anchor === undefined) {
      node.previous = tail; node.next = undefined;
      if (tail) tail.next = node; else head = node;
      tail = node;
    } else {
      node.previous = anchor.previous; node.next = anchor;
      if (anchor.previous) anchor.previous.next = node; else head = node;
      anchor.previous = node;
    }
    byId.set(node.id, node);
  };
  const remove = (node: WireDeltaNode): void => {
    if (node.previous) node.previous.next = node.next; else head = node.next;
    if (node.next) node.next.previous = node.previous; else tail = node.previous;
    byId.delete(node.id);
    node.previous = undefined; node.next = undefined;
  };
  for (const document of current) {
    if (!isDocumentID(document._id) || byId.has(document._id)) throw new Error("Invalid local delta state");
    insertBefore({ id: document._id, document, previous: undefined, next: undefined }, undefined);
  }
  for (const raw of operations) {
    if (!record(raw) || typeof raw.op !== "string" || !isDocumentID(raw.id)) throw new Error("Malformed delta operation");
    const before = raw.before;
    if (before !== undefined && !isDocumentID(before)) throw new Error("Malformed delta before anchor");
    const anchor = before === undefined ? undefined : byId.get(before);
    if (before !== undefined && (!anchor || before === raw.id)) throw new Error("Unknown delta before anchor");
    const node = byId.get(raw.id);
    switch (raw.op) {
      case "remove":
        if (!exactKeys(raw, ["op", "id"]) || !node) throw new Error("Invalid delta remove");
        remove(node);
        break;
      case "add_before": {
        if (!exactKeys(raw, before === undefined ? ["op", "id", "document"] : ["op", "id", "before", "document"]) || node) throw new Error("Invalid delta add");
        const document = cloneDocument(decodeDocument(raw.document));
        if (document._id !== raw.id) throw new Error("Delta add ID mismatch");
        insertBefore({ id: raw.id, document, previous: undefined, next: undefined }, anchor);
        break;
      }
      case "move_before":
        if (!exactKeys(raw, before === undefined ? ["op", "id"] : ["op", "id", "before"]) || !node || node.next === anchor) throw new Error("Invalid delta move");
        remove(node); insertBefore(node, anchor);
        break;
      case "change": {
        if (!exactKeys(raw, ["op", "id", "document"]) || !node) throw new Error("Invalid delta change");
        const document = cloneDocument(decodeDocument(raw.document));
        if (document._id !== raw.id) throw new Error("Delta change ID mismatch");
        node.document = document;
        break;
      }
      default: throw new Error("Unknown delta operation");
    }
    if (byId.size > maximum) throw new Error("Delta result document limit exceeded");
  }
  const result: Document[] = [];
  for (let node = head; node; node = node.next) {
    result.push(node.document);
    if (result.length > maximum) throw new Error("Delta result document limit exceeded");
  }
  if (result.length !== byId.size) throw new Error("Corrupt delta linked list");
  return result;
}

function exactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  return actual.length === wanted.length && actual.every((key, index) => key === wanted[index]);
}

async function boundedJSON(response: Response, maxBytes: number): Promise<unknown> {
  const declared = response.headers.get("content-length");
  if (declared !== null && (!/^[0-9]+$/.test(declared) || BigInt(declared) > BigInt(maxBytes))) {
    try { await response.body?.cancel("Response exceeds safety limit"); } catch { /* best-effort transport cancellation */ }
    throw new Error("Response exceeds safety limit");
  }

  const reader = response.body?.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  if (reader) {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value.byteLength > maxBytes - total) {
        try { await reader.cancel("Response exceeds safety limit"); } catch { /* best-effort transport cancellation */ }
        throw new Error("Response exceeds safety limit");
      }
      chunks.push(value);
      total += value.byteLength;
    }
  }
  const encoded = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    encoded.set(chunk, offset);
    offset += chunk.byteLength;
  }
  let text: string;
  try { text = new TextDecoder("utf-8", { fatal: true }).decode(encoded); } catch { throw new Error("Malformed JSON response"); }
  try { return JSON.parse(text) as unknown; } catch { throw new Error("Malformed JSON response"); }
}
