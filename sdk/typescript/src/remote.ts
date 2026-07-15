import type { DeleteResult, Document, Filter, InputDocument, MutationResult, MutationSpec, QuerySpec, Update } from "./types.js";
import type { QueryOptions } from "./query.js";
import type { SnapshotListener, Unsubscribe } from "./local.js";
import { compileQuery } from "./query.js";
import { compileUpdate, encodeMutationSpec } from "./mutation.js";
import { cloneDocument } from "./safe-value.js";
import { decodeDocument, encodeInputDocument, encodeQuerySpec, type WireQuerySpec } from "./wire.js";

export type SyncState = "idle" | "authenticating" | "connecting" | "live" | "stale" | "resyncing" | "error" | "closed";

export interface SyncStatus {
  readonly state: SyncState;
  readonly error?: Error;
  readonly token?: string;
}

export interface RealtimeTicket {
  readonly url: string;
  readonly ticket: string;
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
  readonly reconnect?: { readonly minDelayMs?: number; readonly maxDelayMs?: number };
}

export interface SubscribeOptions {
  readonly onStatus?: (status: SyncStatus) => void;
}

interface ActiveSubscription {
  readonly requestId: string;
  readonly collection: string;
  readonly query: WireQuerySpec;
  readonly listener: SnapshotListener<Document>;
  readonly onStatus?: (status: SyncStatus) => void;
  token: string | undefined;
  serverId: string | undefined;
}

export class MeldbaseClient {
  readonly #baseUrl: string;
  readonly #fetch: typeof globalThis.fetch;
  readonly #accessToken?: ClientOptions["accessToken"];
  readonly #realtime: RealtimeConnection;
  readonly #maxInboundBytes: number;
  readonly #maxSnapshotDocuments: number;
  readonly #allowedRealtimeOrigins: ReadonlySet<string>;

  constructor(options: ClientOptions) {
    this.#baseUrl = options.baseUrl.replace(/\/$/, "");
    if (!/^https?:\/\//.test(this.#baseUrl)) throw new Error("baseUrl must be an http(s) URL");
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.#accessToken = options.accessToken;
    this.#maxInboundBytes = options.maxInboundBytes ?? 4 * 1024 * 1024;
    this.#maxSnapshotDocuments = options.maxSnapshotDocuments ?? 10_000;
    const base = new URL(this.#baseUrl);
    const defaultRealtimeOrigin = `${base.protocol === "https:" ? "wss:" : "ws:"}//${base.host}`;
    this.#allowedRealtimeOrigins = new Set(options.allowedRealtimeOrigins ?? [defaultRealtimeOrigin]);
    this.#realtime = new RealtimeConnection(options, () => this.createTicket());
  }

  collection<T extends Document = Document>(name: string): RemoteCollection<T> {
    if (!/^[A-Za-z][A-Za-z0-9_-]{0,127}$/.test(name)) throw new Error("Invalid collection name");
    return new RemoteCollection(name, this);
  }

  close(): void { this.#realtime.close(); }

  async fetchQuery<T extends Document>(collection: string, query: QuerySpec, signal?: AbortSignal): Promise<T[]> {
    const response = await this.#fetch(`${this.#baseUrl}/v1/collections/${encodeURIComponent(collection)}/query`, {
      method: "POST",
      headers: await this.headers(),
      body: JSON.stringify({ version: 1, query: encodeQuerySpec(query) }),
      ...(signal ? { signal } : {}),
    });
    if (!response.ok) throw new Error(`Meldbase query failed (${response.status})`);
    const body = await boundedJSON(response, this.#maxInboundBytes);
    if (!record(body) || !Array.isArray(body.documents)) throw new Error("Malformed query response");
    if (body.documents.length > this.#maxSnapshotDocuments) throw new Error("Query response document limit exceeded");
    return body.documents.map((item) => decodeDocument(item) as T);
  }

  async insertOne<T extends Document>(collection: string, document: InputDocument, signal?: AbortSignal): Promise<T> {
    const response = await this.#fetch(`${this.#baseUrl}/v1/collections/${encodeURIComponent(collection)}/documents`, {
      method: "POST", headers: await this.headers(), body: JSON.stringify({ version: 1, document: encodeInputDocument(document) }), ...(signal ? { signal } : {}),
    });
    if (!response.ok) throw new Error(`Meldbase insert failed (${response.status})`);
    const body = await boundedJSON(response, this.#maxInboundBytes);
    if (!record(body) || body.document === undefined) throw new Error("Malformed insert response");
    return decodeDocument(body.document) as T;
  }

  async executeMutation(collection: string, action: "updateOne" | "updateMany" | "deleteOne" | "deleteMany", query: QuerySpec, update?: MutationSpec, signal?: AbortSignal): Promise<MutationResult | DeleteResult> {
    const response = await this.#fetch(`${this.#baseUrl}/v1/collections/${encodeURIComponent(collection)}/mutations`, {
      method: "POST", headers: await this.headers(), body: JSON.stringify({ version: 1, action, query: encodeQuerySpec(query), ...(update ? { update: encodeMutationSpec(update) } : {}) }), ...(signal ? { signal } : {}),
    });
    if (!response.ok) throw new Error(`Meldbase mutation failed (${response.status})`);
    const body = await boundedJSON(response, 64 * 1024);
    if (!record(body)) throw new Error("Malformed mutation response");
    if (action === "deleteOne" || action === "deleteMany") {
      if (!boundedCount(body.deletedCount)) throw new Error("Malformed delete response"); return { deletedCount: body.deletedCount };
    }
    if (!boundedCount(body.matchedCount) || !boundedCount(body.modifiedCount) || body.modifiedCount > body.matchedCount) throw new Error("Malformed update response");
    return { matchedCount: body.matchedCount, modifiedCount: body.modifiedCount };
  }

  subscribe(collection: string, query: QuerySpec, listener: SnapshotListener<Document>, options: SubscribeOptions): Unsubscribe {
    return this.#realtime.subscribe(collection, encodeQuerySpec(query), listener, options.onStatus);
  }

  private async headers(): Promise<Record<string, string>> {
    const token = await this.#accessToken?.();
    return { "content-type": "application/json", ...(token ? { authorization: `Bearer ${token}` } : {}) };
  }

  private async createTicket(): Promise<RealtimeTicket> {
    const response = await this.#fetch(`${this.#baseUrl}/v1/realtime/tickets`, { method: "POST", headers: await this.headers() });
    if (!response.ok) throw new Error(`Realtime ticket failed (${response.status})`);
    const body = await boundedJSON(response, 64 * 1024);
    if (!record(body) || typeof body.url !== "string" || typeof body.ticket !== "string") throw new Error("Malformed realtime ticket response");
    let realtimeURL: URL;
    try { realtimeURL = new URL(body.url); } catch { throw new Error("Malformed realtime URL"); }
    if ((realtimeURL.protocol !== "wss:" && realtimeURL.protocol !== "ws:") || !this.#allowedRealtimeOrigins.has(realtimeURL.origin)) {
      throw new Error("Realtime URL origin is not allowed");
    }
    if (new URL(this.#baseUrl).protocol === "https:" && realtimeURL.protocol !== "wss:") throw new Error("Secure baseUrl requires wss realtime");
    return { url: body.url, ticket: body.ticket };
  }
}

export class RemoteCollection<T extends Document> {
  constructor(readonly name: string, private readonly client: MeldbaseClient) {}
  find(filter: Filter = {}, options: QueryOptions = {}): RemoteLiveQuery<T> {
    return new RemoteLiveQuery(this.client, this.name, compileQuery(filter, options));
  }
  async findOne(filter: Filter = {}, options: QueryOptions & { readonly signal?: AbortSignal } = {}): Promise<T | undefined> {
    const { signal, ...queryOptions } = options;
    const documents = await this.client.fetchQuery<T>(this.name, compileQuery(filter, { ...queryOptions, limit: 1 }), signal);
    return documents[0];
  }
  insertOne(document: InputDocument, options: { readonly signal?: AbortSignal } = {}): Promise<T> { return this.client.insertOne<T>(this.name, document, options.signal); }
  updateOne(filter: Filter, update: Update, options: { readonly signal?: AbortSignal } = {}): Promise<MutationResult> { return this.client.executeMutation(this.name, "updateOne", compileQuery(filter), compileUpdate(update), options.signal) as Promise<MutationResult>; }
  updateMany(filter: Filter, update: Update, options: { readonly signal?: AbortSignal } = {}): Promise<MutationResult> { return this.client.executeMutation(this.name, "updateMany", compileQuery(filter), compileUpdate(update), options.signal) as Promise<MutationResult>; }
  deleteOne(filter: Filter, options: { readonly signal?: AbortSignal } = {}): Promise<DeleteResult> { return this.client.executeMutation(this.name, "deleteOne", compileQuery(filter), undefined, options.signal) as Promise<DeleteResult>; }
  deleteMany(filter: Filter, options: { readonly signal?: AbortSignal } = {}): Promise<DeleteResult> { return this.client.executeMutation(this.name, "deleteMany", compileQuery(filter), undefined, options.signal) as Promise<DeleteResult>; }
}

export class RemoteLiveQuery<T extends Document> {
	readonly mode = "remote" as const;
  constructor(private readonly client: MeldbaseClient, readonly collection: string, readonly spec: QuerySpec) {}
  fetch(options: { readonly signal?: AbortSignal } = {}): Promise<T[]> { return this.client.fetchQuery<T>(this.collection, this.spec, options.signal); }
  subscribe(listener: SnapshotListener<T>, options: SubscribeOptions = {}): Unsubscribe {
    return this.client.subscribe(this.collection, this.spec, listener as SnapshotListener<Document>, options);
  }
}

class RealtimeConnection {
  readonly #socketFactory: (url: string) => WebSocketLike;
  readonly #ticket: () => Promise<RealtimeTicket>;
  readonly #subscriptions = new Map<string, ActiveSubscription>();
  readonly #maxInboundBytes: number;
  readonly #maxSnapshotDocuments: number;
  readonly #minDelay: number;
  readonly #maxDelay: number;
  #socket: WebSocketLike | undefined;
  #epoch = 0;
  #attempt = 0;
  #closed = false;
  #retryTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(options: ClientOptions, ticket: () => Promise<RealtimeTicket>) {
    this.#socketFactory = options.webSocketFactory ?? ((url) => new WebSocket(url));
    this.#ticket = ticket;
    this.#maxInboundBytes = options.maxInboundBytes ?? 4 * 1024 * 1024;
    this.#maxSnapshotDocuments = options.maxSnapshotDocuments ?? 10_000;
    this.#minDelay = options.reconnect?.minDelayMs ?? 250;
    this.#maxDelay = options.reconnect?.maxDelayMs ?? 15_000;
  }

  subscribe(collection: string, query: WireQuerySpec, listener: SnapshotListener<Document>, onStatus?: (status: SyncStatus) => void): Unsubscribe {
    const requestId = crypto.randomUUID();
    const subscription: ActiveSubscription = { requestId, collection, query, listener, token: undefined, serverId: undefined, ...(onStatus ? { onStatus } : {}) };
    this.#subscriptions.set(requestId, subscription);
    this.status(subscription, { state: "idle" });
    void this.ensureConnected();
    return () => {
      const active = this.#subscriptions.get(requestId);
      if (!active) return;
      this.#subscriptions.delete(requestId);
      if (active.serverId) this.send({ v: 1, type: "unsubscribe", subscriptionId: active.serverId });
      this.status(active, { state: "closed" });
		if (this.#subscriptions.size === 0) this.closeIdleSocket();
    };
  }

  close(): void {
    this.#closed = true;
    this.#epoch += 1;
    if (this.#retryTimer) clearTimeout(this.#retryTimer);
    this.#socket?.close(1000, "client closed");
    for (const subscription of this.#subscriptions.values()) this.status(subscription, { state: "closed" });
    this.#subscriptions.clear();
  }

  private async ensureConnected(): Promise<void> {
    if (this.#closed || this.#socket || this.#subscriptions.size === 0) return;
    const epoch = ++this.#epoch;
    this.statusAll({ state: "authenticating" });
    try {
      const ticket = await this.#ticket();
      if (epoch !== this.#epoch || this.#closed) return;
      this.statusAll({ state: "connecting" });
      const socket = this.#socketFactory(ticket.url);
      this.#socket = socket;
      socket.addEventListener("open", () => {
        if (epoch !== this.#epoch) return;
        this.send({ v: 1, type: "authenticate", ticket: ticket.ticket });
      });
      socket.addEventListener("message", (event) => { if (epoch === this.#epoch) this.onMessage(event as MessageEvent); });
      socket.addEventListener("close", () => { if (epoch === this.#epoch) this.disconnected(); });
      socket.addEventListener("error", () => { if (epoch === this.#epoch) this.statusAll({ state: "stale", error: new Error("Realtime connection error") }); });
    } catch (error) {
      if (epoch !== this.#epoch) return;
      this.statusAll({ state: "error", error: asError(error) });
      this.scheduleRetry();
    }
  }

  private onMessage(event: MessageEvent): void {
    if (typeof event.data !== "string" || new TextEncoder().encode(event.data).byteLength > this.#maxInboundBytes) {
      this.protocolFailure("Realtime message exceeds safety limits"); return;
    }
    let message: unknown;
    try { message = JSON.parse(event.data); } catch { this.protocolFailure("Malformed realtime JSON"); return; }
    if (!record(message) || message.v !== 1 || typeof message.type !== "string") { this.protocolFailure("Malformed realtime envelope"); return; }
    if (message.type === "authenticated") {
      this.#attempt = 0;
      for (const subscription of this.#subscriptions.values()) {
        this.status(subscription, { state: subscription.token ? "resyncing" : "connecting", ...(subscription.token ? { token: subscription.token } : {}) });
        this.send({ v: 1, type: "subscribe", requestId: subscription.requestId, collection: subscription.collection, query: subscription.query, ...(subscription.token ? { resumeToken: subscription.token } : {}) });
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
        queueMicrotask(() => subscription.listener(documents));
        this.status(subscription, { state: "live", token: message.token });
      } catch (error) { this.protocolFailure(asError(error).message); }
      return;
    }
    if (message.type === "resync_required" && typeof message.requestId === "string") {
      const subscription = this.#subscriptions.get(message.requestId);
      if (subscription) { subscription.token = undefined; this.status(subscription, { state: "resyncing" }); this.send({ v: 1, type: "subscribe", requestId: subscription.requestId, collection: subscription.collection, query: subscription.query }); }
      return;
    }
    if (message.type === "error" && typeof message.requestId === "string") {
      const subscription = this.#subscriptions.get(message.requestId);
      if (subscription) this.status(subscription, { state: "error", error: new Error(typeof message.message === "string" ? message.message : "Subscription error") });
    }
  }

  private disconnected(): void {
    this.#socket = undefined;
    this.statusAll({ state: "stale" });
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
    this.#epoch += 1;
    this.statusAll({ state: "error", error: new Error(reason) });
    socket?.close(1002, reason.slice(0, 100));
  }

  private send(message: object): void {
    if (this.#socket?.readyState === 1) this.#socket.send(JSON.stringify(message));
  }

	private closeIdleSocket(): void {
		if (this.#subscriptions.size !== 0) return;
		this.#epoch += 1;
		if (this.#retryTimer) {
			clearTimeout(this.#retryTimer);
			this.#retryTimer = undefined;
		}
		const socket = this.#socket;
		this.#socket = undefined;
		socket?.close(1000, "no active subscriptions");
	}

  private statusAll(status: SyncStatus): void { for (const subscription of this.#subscriptions.values()) this.status(subscription, status); }
  private status(subscription: ActiveSubscription, status: SyncStatus): void { if (subscription.onStatus) queueMicrotask(() => subscription.onStatus?.(status)); }
}

function record(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value); }
function asError(error: unknown): Error { return error instanceof Error ? error : new Error(String(error)); }
function boundedCount(value: unknown): value is number { return typeof value === "number" && Number.isSafeInteger(value) && value >= 0; }

async function boundedJSON(response: Response, maxBytes: number): Promise<unknown> {
  const declared = response.headers.get("content-length");
  if (declared !== null && Number(declared) > maxBytes) throw new Error("Response exceeds safety limit");
  const text = await response.text();
  if (new TextEncoder().encode(text).byteLength > maxBytes) throw new Error("Response exceeds safety limit");
  try { return JSON.parse(text) as unknown; } catch { throw new Error("Malformed JSON response"); }
}
