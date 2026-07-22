import {
  decodeDocument,
  decodeValue,
  encodeInputDocument,
  encodeMutationSpec,
  encodeValue,
  MELDBASE_PROTOCOL_VERSION,
} from "@meldbase/client";
import type { Document, InputDocument, MutationSpec, Value } from "@meldbase/client";

import { MeldbaseMethodError } from "./errors.js";
import { COLLECTION_PATTERN, deferred, type Deferred, ERROR_PATTERN, ID_PATTERN, record } from "./shared.js";
import type { WriteTransaction } from "./types.js";

export class RemoteWriteTransaction implements WriteTransaction {
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
    // The Go WriteTransaction rejects an absent id. Keep this wire operation
    // strict: browser-local upsert is intentionally not a server transaction API.
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
