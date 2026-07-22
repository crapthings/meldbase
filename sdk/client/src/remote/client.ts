import type { CountResult, DeleteResult, Document, GroupCountResult, InputDocument, MutationResult, MutationSpec, QuerySpec, Value } from "../types.js";
import type { SnapshotListener, Unsubscribe } from "../observer.js";
import { encodeMutationSpec } from "../mutation.js";
import { assertDocumentID, assertSafeKey, cloneValue, newDocumentID } from "../safe-value.js";
import { decodeDocument, decodeValue, encodeInputDocument, encodeQuerySpec, encodeValue } from "../wire.js";
import { decodeProtocolDescriptor, MELDBASE_PROTOCOL_VERSION, type ProtocolDescriptor } from "../protocol.js";
import { RemoteCollection } from "./collection.js";
import { MeldbaseClientClosedError, MeldbaseError, MeldbaseInternalError, MeldbaseProtocolError } from "./errors.js";
import { RealtimeConnection } from "./realtime.js";
import { assertCollectionName, boundedCount, boundedJSON, decodeWireError, exactKeys, normalizeBaseURL, normalizeRealtimeOrigin, positiveLimit, record, throwRemoteError, validRPCIdempotencyKey } from "./shared.js";
import type { CallOptions, ClientOptions, RealtimeTicket, SubscribeOptions } from "./types.js";

const REALTIME_TICKET_ACCEPT = "application/vnd.meldbase.realtime-ticket+json; capabilities=1";

export class MeldbaseClient {
  readonly #baseUrl: string;
  readonly #fetch: typeof globalThis.fetch;
  readonly #accessToken?: ClientOptions["accessToken"];
  readonly #realtime: RealtimeConnection;
  readonly #maxInboundBytes: number;
  readonly #maxSnapshotDocuments: number;
  readonly #allowedRealtimeOrigins: ReadonlySet<string>;
  #closed = false;

  constructor(options: ClientOptions) {
    this.#baseUrl = normalizeBaseURL(options.baseUrl);
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.#accessToken = options.accessToken;
    this.#maxInboundBytes = positiveLimit(options.maxInboundBytes, 4 * 1024 * 1024, "maxInboundBytes");
    this.#maxSnapshotDocuments = positiveLimit(options.maxSnapshotDocuments, 10_000, "maxSnapshotDocuments");
    const base = new URL(this.#baseUrl);
    const defaultRealtimeOrigin = `${base.protocol === "https:" ? "wss:" : "ws:"}//${base.host}`;
    this.#allowedRealtimeOrigins = new Set((options.allowedRealtimeOrigins ?? [defaultRealtimeOrigin]).map(normalizeRealtimeOrigin));
    this.#realtime = new RealtimeConnection(options, () => this.createTicket());
  }

  collection<T extends Document = Document>(name: string): RemoteCollection<T> { this.assertOpen(); assertCollectionName(name); return new RemoteCollection(name, this); }
  close(): void { if (this.#closed) return; this.#closed = true; this.#realtime.close(); }
  get realtimeProtocol(): ProtocolDescriptor | undefined { return this.#realtime.protocol; }

  async call<T extends Value = Value>(method: string, input: Value, options: CallOptions = {}): Promise<T> {
    this.assertOpen();
    if (!/^[A-Za-z][A-Za-z0-9_.-]{0,127}$/.test(method)) throw new Error("Invalid RPC method name");
    const safeInput = cloneValue(input);
    if (options.transport !== undefined && options.transport !== "http" && options.transport !== "realtime") throw new Error("Invalid RPC transport");
    if (options.idempotencyKey !== undefined && !validRPCIdempotencyKey(options.idempotencyKey)) throw new Error("Invalid RPC idempotency key");
    if (options.transport === "realtime") return this.#realtime.call<T>(method, safeInput, options.signal, options.idempotencyKey);
    const requestId = crypto.randomUUID();
    const response = await this.#fetch(`${this.#baseUrl}/v1/rpc`, { method: "POST", headers: await this.headers(), body: JSON.stringify({ v: MELDBASE_PROTOCOL_VERSION, type: "call", requestId, ...(options.idempotencyKey ? { idempotencyKey: options.idempotencyKey } : {}), method, input: encodeValue(safeInput) }), ...(options.signal ? { signal: options.signal } : {}) });
    const body = await boundedJSON(response, this.#maxInboundBytes);
    if (!response.ok) {
      if (record(body) && exactKeys(body, ["v", "type", "requestId", "error"]) && body.v === MELDBASE_PROTOCOL_VERSION && body.type === "error" && body.requestId === requestId) {
        const error = decodeWireError(body.error);
        throw error.kind === "business" ? new MeldbaseError(error.code, error.data) : new MeldbaseInternalError(error.code, response.status, "RPC");
      }
      if (record(body) && exactKeys(body, ["error"])) {
        const error = decodeWireError(body.error);
        if (error.kind === "internal") throw new MeldbaseInternalError(error.code, response.status, "RPC");
      }
      throw new Error("Malformed RPC error response");
    }
    if (!record(body) || !exactKeys(body, ["v", "type", "requestId", "result"]) || body.v !== MELDBASE_PROTOCOL_VERSION || body.type !== "result" || body.requestId !== requestId || body.result === undefined) throw new Error("Malformed RPC response");
    return decodeValue(body.result) as T;
  }

  async fetchQuery<T extends Document>(collection: string, query: QuerySpec, signal?: AbortSignal): Promise<T[]> {
    this.assertOpen(); assertCollectionName(collection);
    const response = await this.#fetch(`${this.#baseUrl}/v1/collections/${encodeURIComponent(collection)}/query`, { method: "POST", headers: await this.headers(), body: JSON.stringify({ version: 1, query: encodeQuerySpec(query) }), ...(signal ? { signal } : {}) });
    if (!response.ok) await throwRemoteError(response, this.#maxInboundBytes, "query");
    const body = await boundedJSON(response, this.#maxInboundBytes);
    if (!record(body) || !exactKeys(body, ["version", "documents"]) || body.version !== 1 || !Array.isArray(body.documents)) throw new Error("Malformed query response");
    if (body.documents.length > this.#maxSnapshotDocuments) throw new Error("Query response document limit exceeded");
    return body.documents.map((item) => { const document = decodeDocument(item) as T; assertDocumentID(document._id, "Remote document _id"); return document; });
  }

  async count(collection: string, query: QuerySpec, signal?: AbortSignal): Promise<CountResult> {
    this.assertOpen(); assertCollectionName(collection);
    const response = await this.#fetch(`${this.#baseUrl}/v1/collections/${encodeURIComponent(collection)}/count`, { method: "POST", headers: await this.headers(), body: JSON.stringify({ version: 1, query: encodeQuerySpec(query) }), ...(signal ? { signal } : {}) });
    if (!response.ok) await throwRemoteError(response, 64 * 1024, "count");
    const body = await boundedJSON(response, 64 * 1024);
    if (!record(body) || !exactKeys(body, ["version", "count", "capped"]) || body.version !== 1 || !boundedCount(body.count) || typeof body.capped !== "boolean") throw new Error("Malformed count response");
    return { count: body.count, capped: body.capped };
  }

  async groupCount(collection: string, query: QuerySpec, groupBy: string, signal?: AbortSignal): Promise<GroupCountResult> {
    this.assertOpen(); assertCollectionName(collection); assertSafeKey(groupBy, "group field");
    const response = await this.#fetch(`${this.#baseUrl}/v1/collections/${encodeURIComponent(collection)}/group-count`, { method: "POST", headers: await this.headers(), body: JSON.stringify({ version: 1, query: encodeQuerySpec(query), groupBy }), ...(signal ? { signal } : {}) });
    if (!response.ok) await throwRemoteError(response, this.#maxInboundBytes, "group count");
    const body = await boundedJSON(response, this.#maxInboundBytes);
    if (!record(body) || !exactKeys(body, ["version", "groups", "capped"]) || body.version !== 1 || !Array.isArray(body.groups) || body.groups.length > 100 || typeof body.capped !== "boolean") throw new Error("Malformed group count response");
    const groups = body.groups.map((item) => { if (!record(item) || !exactKeys(item, ["key", "count"]) || item.key === undefined || !boundedCount(item.count)) throw new Error("Malformed group count entry"); return { key: decodeValue(item.key), count: item.count }; });
    return { groups, capped: body.capped };
  }

  async insertOne<T extends Document>(collection: string, document: InputDocument, signal?: AbortSignal): Promise<T> {
    this.assertOpen(); assertCollectionName(collection);
    const id = document._id ?? newDocumentID(); const input: InputDocument = document._id === undefined ? { ...document, _id: id } : document;
    const body = JSON.stringify({ version: 1, document: encodeInputDocument(input) }); const headers = await this.headers();
    let response: Response;
    try { response = await this.#fetch(`${this.#baseUrl}/v1/collections/${encodeURIComponent(collection)}/documents`, { method: "POST", headers, body, ...(signal ? { signal } : {}) }); } catch (error) { throw new MeldbaseInternalError("outcome_unknown", 0, `insert ${id}`, error); }
    if (!response.ok) await throwRemoteError(response, this.#maxInboundBytes, "insert");
    try { const result = await boundedJSON(response, this.#maxInboundBytes); if (!record(result) || !exactKeys(result, ["version", "document"]) || result.version !== 1 || result.document === undefined) throw new Error("Malformed insert response"); const inserted = decodeDocument(result.document) as T; assertDocumentID(inserted._id, "Remote document _id"); if (inserted._id !== id) throw new Error("Insert response changed document ID"); return inserted; } catch (error) { throw new MeldbaseInternalError("outcome_unknown", 0, `insert ${id}`, error); }
  }

  async executeMutation(collection: string, action: "updateOne" | "updateMany" | "deleteOne" | "deleteMany", query: QuerySpec, update?: MutationSpec, signal?: AbortSignal): Promise<MutationResult | DeleteResult> {
    this.assertOpen(); assertCollectionName(collection);
    if (action !== "updateOne" && action !== "updateMany" && action !== "deleteOne" && action !== "deleteMany") throw new Error("Invalid mutation action");
    if ((action.startsWith("update") && update === undefined) || (action.startsWith("delete") && update !== undefined)) throw new Error("Invalid mutation payload");
    const response = await this.#fetch(`${this.#baseUrl}/v1/collections/${encodeURIComponent(collection)}/mutations`, { method: "POST", headers: await this.headers(), body: JSON.stringify({ version: 1, action, query: encodeQuerySpec(query), ...(update ? { update: encodeMutationSpec(update) } : {}) }), ...(signal ? { signal } : {}) });
    if (!response.ok) await throwRemoteError(response, 64 * 1024, "mutation");
    const body = await boundedJSON(response, 64 * 1024); if (!record(body)) throw new Error("Malformed mutation response");
    if (action === "deleteOne" || action === "deleteMany") { if (!exactKeys(body, ["version", "deletedCount"]) || body.version !== 1 || !boundedCount(body.deletedCount)) throw new Error("Malformed delete response"); return { deletedCount: body.deletedCount }; }
    if (!exactKeys(body, ["version", "matchedCount", "modifiedCount"]) || body.version !== 1 || !boundedCount(body.matchedCount) || !boundedCount(body.modifiedCount) || body.modifiedCount > body.matchedCount) throw new Error("Malformed update response"); return { matchedCount: body.matchedCount, modifiedCount: body.modifiedCount };
  }

  subscribe(collection: string, query: QuerySpec, listener: SnapshotListener<Document>, options: SubscribeOptions): Unsubscribe { this.assertOpen(); assertCollectionName(collection); return this.#realtime.subscribe(collection, encodeQuerySpec(query), listener, options.onStatus); }

  private assertOpen(): void { if (this.#closed) throw new MeldbaseClientClosedError(); }
  private async headers(): Promise<Record<string, string>> { const token = await this.#accessToken?.(); return { "content-type": "application/json", ...(token ? { authorization: `Bearer ${token}` } : {}) }; }
  private async createTicket(): Promise<RealtimeTicket> {
    const response = await this.#fetch(`${this.#baseUrl}/v1/realtime/tickets`, { method: "POST", headers: { ...(await this.headers()), accept: REALTIME_TICKET_ACCEPT } });
    if (!response.ok) await throwRemoteError(response, 64 * 1024, "realtime ticket");
    const body = await boundedJSON(response, 64 * 1024);
    if (!record(body) || !exactKeys(body, body.protocol === undefined ? ["url", "ticket"] : ["url", "ticket", "protocol"]) || typeof body.url !== "string" || body.url.length === 0 || body.url.length > 4096 || typeof body.ticket !== "string" || body.ticket.length === 0 || body.ticket.length > 4096) throw new Error("Malformed realtime ticket response");
    let realtimeURL: URL; try { realtimeURL = new URL(body.url); } catch { throw new Error("Malformed realtime URL"); }
    if ((realtimeURL.protocol !== "wss:" && realtimeURL.protocol !== "ws:") || !this.#allowedRealtimeOrigins.has(realtimeURL.origin)) throw new Error("Realtime URL origin is not allowed");
    if (new URL(this.#baseUrl).protocol === "https:" && realtimeURL.protocol !== "wss:") throw new Error("Secure baseUrl requires wss realtime");
    let protocol: ProtocolDescriptor | undefined; try { protocol = body.protocol === undefined ? undefined : decodeProtocolDescriptor(body.protocol); } catch { throw new MeldbaseProtocolError(Object.freeze(["valid_descriptor"])); }
    return { url: body.url, ticket: body.ticket, ...(protocol ? { protocol } : {}) };
  }
}
