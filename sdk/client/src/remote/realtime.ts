import type { Document, Value } from "../types.js";
import type { SnapshotListener, Unsubscribe } from "../observer.js";
import { cloneDocument, isDocumentID } from "../safe-value.js";
import { decodeDocument, decodeValue, encodeValue, type WireQuerySpec, type WireValue } from "../wire.js";
import { MELDBASE_PROTOCOL_VERSION, supportsProtocol, type ProtocolDescriptor } from "../protocol.js";
import { MeldbaseClientClosedError, MeldbaseError, MeldbaseInternalError, MeldbaseProtocolError } from "./errors.js";
import { abortReason, asError, decodeWireError, exactKeys, positiveLimit, record } from "./shared.js";
import type { ClientOptions, RealtimeTicket, SyncStatus, WebSocketLike } from "./types.js";

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
  readonly input: WireValue;
  readonly idempotencyKey?: string;
  readonly resolve: (value: Value) => void;
  readonly reject: (error: Error) => void;
  readonly signal?: AbortSignal;
  readonly abort?: () => void;
  sent: boolean;
}

export class RealtimeConnection {
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

  subscribe(
    collection: string,
    query: WireQuerySpec,
    listener: SnapshotListener<Document>,
    onStatus?: (status: SyncStatus) => void,
  ): Unsubscribe {
    if (this.#closed) throw new MeldbaseClientClosedError();
    const requestId = crypto.randomUUID();
    const subscription: ActiveSubscription = {
      requestId,
      collection,
      query,
      listener,
      token: undefined,
      serverId: undefined,
      documents: [],
      ...(onStatus ? { onStatus } : {}),
    };
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
      if (active.serverId)
        this.send({ v: MELDBASE_PROTOCOL_VERSION, type: "unsubscribe", subscriptionId: active.serverId });
      this.status(active, { state: "closed" });
      this.closeIdleSocket();
    };
  }

  call<T extends Value>(method: string, input: Value, signal?: AbortSignal, idempotencyKey?: string): Promise<T> {
    if (this.#closed) return Promise.reject(new MeldbaseClientClosedError());
    if (this.#protocolError) return Promise.reject(this.#protocolError);
    const capabilityError = this.capabilityError([
      "rpc",
      ...(signal ? ["rpc.cancel"] : []),
      ...(idempotencyKey ? ["rpc.idempotency"] : []),
    ]);
    if (capabilityError) return Promise.reject(capabilityError);
    if (signal?.aborted) return Promise.reject(abortReason(signal));
    const requestId = crypto.randomUUID();
    return new Promise<T>((resolve, reject) => {
      const abort = signal
        ? () => {
            const active = this.#calls.get(requestId);
            if (!active) return;
            this.#calls.delete(requestId);
            if (active.sent) this.send({ v: MELDBASE_PROTOCOL_VERSION, type: "cancel", requestId });
            this.cleanupCall(active);
            reject(abortReason(signal));
            this.closeIdleSocket();
          }
        : undefined;
      const call: ActiveRPCCall = {
        requestId,
        method,
        input: encodeValue(input),
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
    this.rejectCalls(new MeldbaseClientClosedError());
    this.#socket = undefined;
    this.#authenticated = false;
    this.#connecting = false;
  }

  get protocol(): ProtocolDescriptor | undefined {
    return this.#protocol;
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
        if (epoch === this.#epoch)
          this.send({ v: MELDBASE_PROTOCOL_VERSION, type: "authenticate", ticket: ticket.ticket });
      });
      socket.addEventListener("message", (event) => {
        if (epoch === this.#epoch) this.onMessage(event as MessageEvent);
      });
      socket.addEventListener("close", () => {
        if (epoch === this.#epoch) this.disconnected();
      });
      socket.addEventListener("error", () => {
        if (epoch === this.#epoch) this.connectionFailure(new Error("Realtime connection error"));
      });
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
    if (typeof event.data !== "string" || new TextEncoder().encode(event.data).byteLength > this.#maxInboundBytes)
      return this.protocolFailure("Realtime message exceeds safety limits");
    let message: unknown;
    try {
      message = JSON.parse(event.data);
    } catch {
      return this.protocolFailure("Malformed realtime JSON");
    }
    if (!record(message) || message.v !== MELDBASE_PROTOCOL_VERSION || typeof message.type !== "string")
      return this.protocolFailure("Malformed realtime envelope");
    if (message.type === "authenticated") {
      if (!exactKeys(message, ["v", "type"])) return this.protocolFailure("Malformed authenticated response");
      this.#authenticated = true;
      this.#attempt = 0;
      for (const subscription of this.#subscriptions.values()) this.sendSubscription(subscription);
      for (const call of this.#calls.values()) if (!call.sent) this.sendCall(call);
      return;
    }
    if (message.type === "result") {
      if (typeof message.requestId !== "string") return this.protocolFailure("Malformed RPC result");
      const call = this.#calls.get(message.requestId);
      if (!call) return;
      if (!exactKeys(message, ["v", "type", "requestId", "result"]) || message.result === undefined)
        return this.protocolFailure("Malformed RPC result");
      try {
        this.settleCall(call, decodeValue(message.result));
      } catch (error) {
        this.protocolFailure(asError(error).message);
      }
      return;
    }
    if (message.type === "snapshot") {
      if (
        typeof message.requestId !== "string" ||
        typeof message.subscriptionId !== "string" ||
        typeof message.token !== "string" ||
        !Array.isArray(message.documents)
      )
        return this.protocolFailure("Malformed snapshot");
      if (message.documents.length > this.#maxSnapshotDocuments)
        return this.protocolFailure("Snapshot document limit exceeded");
      const subscription = this.#subscriptions.get(message.requestId);
      if (!subscription) return;
      try {
        const documents = message.documents.map(decodeDocument).map(cloneDocument);
        subscription.serverId = message.subscriptionId;
        subscription.token = message.token;
        subscription.documents = documents;
        queueMicrotask(() => subscription.listener(documents.map(cloneDocument)));
        this.status(subscription, { state: "live", token: message.token });
      } catch (error) {
        this.protocolFailure(asError(error).message);
      }
      return;
    }
    if (message.type === "resumed") {
      if (
        typeof message.requestId !== "string" ||
        typeof message.subscriptionId !== "string" ||
        typeof message.token !== "string"
      )
        return this.protocolFailure("Malformed resumed response");
      const subscription = this.#subscriptions.get(message.requestId);
      if (!subscription) return;
      if (!subscription.token || subscription.token !== message.token)
        return this.protocolFailure("Resume token chain mismatch");
      subscription.serverId = message.subscriptionId;
      this.status(subscription, { state: "live", token: message.token });
      return;
    }
    if (message.type === "delta") {
      if (
        typeof message.requestId !== "string" ||
        typeof message.subscriptionId !== "string" ||
        typeof message.fromToken !== "string" ||
        typeof message.token !== "string" ||
        !Array.isArray(message.operations)
      )
        return this.protocolFailure("Malformed delta");
      if (message.operations.length === 0 || message.operations.length > this.#maxDeltaOperations)
        return this.protocolFailure("Delta operation limit exceeded");
      const subscription = this.#subscriptions.get(message.requestId);
      if (!subscription) return;
      if (
        !subscription.token ||
        subscription.serverId !== message.subscriptionId ||
        subscription.token !== message.fromToken ||
        message.token === message.fromToken
      )
        return this.protocolFailure("Delta token chain mismatch");
      try {
        const documents = applyWireDelta(subscription.documents, message.operations, this.#maxSnapshotDocuments);
        subscription.documents = documents;
        subscription.token = message.token;
        queueMicrotask(() => subscription.listener(documents.map(cloneDocument)));
        this.status(subscription, { state: "live", token: message.token });
      } catch (error) {
        this.protocolFailure(asError(error).message);
      }
      return;
    }
    if (message.type === "resync_required" && typeof message.requestId === "string") {
      const subscription = this.#subscriptions.get(message.requestId);
      if (subscription) {
        subscription.token = undefined;
        subscription.serverId = undefined;
        this.status(subscription, { state: "resyncing" });
        this.send({
          v: MELDBASE_PROTOCOL_VERSION,
          type: "subscribe",
          mode: "delta",
          requestId: subscription.requestId,
          collection: subscription.collection,
          query: subscription.query,
        });
      }
      return;
    }
    if (message.type === "error" && typeof message.requestId === "string") {
      const call = this.#calls.get(message.requestId);
      if (call) {
        if (!exactKeys(message, ["v", "type", "requestId", "error"]))
          return this.protocolFailure("Malformed RPC error");
        try {
          const error = decodeWireError(message.error);
          this.settleCall(
            call,
            error.kind === "business"
              ? new MeldbaseError(error.code, error.data)
              : new MeldbaseInternalError(error.code, 0, "RPC"),
          );
        } catch {
          this.protocolFailure("Malformed RPC error");
        }
        return;
      }
      const subscription = this.#subscriptions.get(message.requestId);
      if (subscription) {
        if (!exactKeys(message, ["v", "type", "requestId", "error"]))
          return this.protocolFailure("Malformed subscription error");
        try {
          const error = decodeWireError(message.error);
          if (error.kind !== "internal") return this.protocolFailure("Malformed subscription error");
          this.status(subscription, {
            state: "error",
            error: new MeldbaseInternalError(error.code, 0, "subscription"),
          });
        } catch {
          this.protocolFailure("Malformed subscription error");
        }
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
    this.#retryTimer = setTimeout(() => {
      this.#retryTimer = undefined;
      void this.ensureConnected();
    }, delay);
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
  private hasWork(): boolean {
    return this.#subscriptions.size !== 0 || this.#calls.size !== 0;
  }
  private validateProtocol(protocol: ProtocolDescriptor | undefined): void {
    this.#protocol = protocol;
    if (!protocol) throw new MeldbaseProtocolError(["protocol.discovery"]);
    const required = new Set<string>();
    if (this.#subscriptions.size > 0) required.add("query.delta");
    if (this.#calls.size > 0) required.add("rpc");
    for (const call of this.#calls.values()) {
      if (call.signal) required.add("rpc.cancel");
      if (call.idempotencyKey) required.add("rpc.idempotency");
    }
    const missing = [...required].filter((capability) => !protocol.capabilities.includes(capability));
    if (!supportsProtocol(protocol, MELDBASE_PROTOCOL_VERSION) || missing.length > 0) {
      if (!protocol.versions.includes(MELDBASE_PROTOCOL_VERSION))
        missing.unshift(`version.${MELDBASE_PROTOCOL_VERSION}`);
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
    this.status(subscription, {
      state: resumeToken ? "resyncing" : "connecting",
      ...(resumeToken ? { token: resumeToken } : {}),
    });
    this.send({
      v: MELDBASE_PROTOCOL_VERSION,
      type: "subscribe",
      mode: "delta",
      requestId: subscription.requestId,
      collection: subscription.collection,
      query: subscription.query,
      ...(resumeToken ? { resumeToken } : {}),
    });
  }
  private sendCall(call: ActiveRPCCall): void {
    if (call.sent) return;
    call.sent = true;
    this.send({
      v: MELDBASE_PROTOCOL_VERSION,
      type: "call",
      requestId: call.requestId,
      ...(call.idempotencyKey ? { idempotencyKey: call.idempotencyKey } : {}),
      method: call.method,
      input: call.input,
    });
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
      call.reject(new MeldbaseInternalError("outcome_unknown", 0, `RPC ${call.requestId}`));
    }
  }
  private statusAll(status: SyncStatus): void {
    for (const subscription of this.#subscriptions.values()) this.status(subscription, status);
  }
  private status(subscription: ActiveSubscription, status: SyncStatus): void {
    if (subscription.onStatus) queueMicrotask(() => subscription.onStatus?.(status));
  }
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
      node.previous = tail;
      node.next = undefined;
      if (tail) tail.next = node;
      else head = node;
      tail = node;
    } else {
      node.previous = anchor.previous;
      node.next = anchor;
      if (anchor.previous) anchor.previous.next = node;
      else head = node;
      anchor.previous = node;
    }
    byId.set(node.id, node);
  };
  const remove = (node: WireDeltaNode): void => {
    if (node.previous) node.previous.next = node.next;
    else head = node.next;
    if (node.next) node.next.previous = node.previous;
    else tail = node.previous;
    byId.delete(node.id);
    node.previous = undefined;
    node.next = undefined;
  };
  for (const document of current) {
    if (!isDocumentID(document._id) || byId.has(document._id)) throw new Error("Invalid local delta state");
    insertBefore({ id: document._id, document, previous: undefined, next: undefined }, undefined);
  }
  for (const raw of operations) {
    if (!record(raw) || typeof raw.op !== "string" || !isDocumentID(raw.id))
      throw new Error("Malformed delta operation");
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
        if (
          !exactKeys(raw, before === undefined ? ["op", "id", "document"] : ["op", "id", "before", "document"]) ||
          node
        )
          throw new Error("Invalid delta add");
        const document = cloneDocument(decodeDocument(raw.document));
        if (document._id !== raw.id) throw new Error("Delta add ID mismatch");
        insertBefore({ id: raw.id, document, previous: undefined, next: undefined }, anchor);
        break;
      }
      case "move_before":
        if (
          !exactKeys(raw, before === undefined ? ["op", "id"] : ["op", "id", "before"]) ||
          !node ||
          node.next === anchor
        )
          throw new Error("Invalid delta move");
        remove(node);
        insertBefore(node, anchor);
        break;
      case "change": {
        if (!exactKeys(raw, ["op", "id", "document"]) || !node) throw new Error("Invalid delta change");
        const document = cloneDocument(decodeDocument(raw.document));
        if (document._id !== raw.id) throw new Error("Delta change ID mismatch");
        node.document = document;
        break;
      }
      default:
        throw new Error("Unknown delta operation");
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
