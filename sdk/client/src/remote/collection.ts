import type {
  CountResult,
  DeleteResult,
  Document,
  Filter,
  GroupCountResult,
  InputDocument,
  InsertDocument,
  MutationResult,
  Update,
} from "../types.js";
import type { FindOneOptions, QueryOptions } from "../query.js";
import type { SnapshotListener, Unsubscribe } from "../observer.js";
import { compileQuery } from "../query.js";
import { compileUpdate } from "../mutation.js";
import { pageCursorFor, type PageResult } from "../cursor.js";
import type { MeldbaseClient } from "./client.js";
import type { SubscribeOptions } from "./types.js";

export class RemoteCollection<T extends Document> {
  constructor(
    readonly name: string,
    private readonly client: MeldbaseClient,
  ) {}

  find(filter: Filter = {}, options: QueryOptions = {}): RemoteLiveQuery<T> {
    return new RemoteLiveQuery(this.client, this.name, compileQuery(filter, options), options.first !== undefined);
  }

  async findOne(
    filter: Filter = {},
    options: FindOneOptions & { readonly signal?: AbortSignal } = {},
  ): Promise<T | undefined> {
    const { signal, ...queryOptions } = options;
    const documents = await this.client.fetchQuery<T>(
      this.name,
      compileQuery(filter, { ...queryOptions, limit: 1 }),
      signal,
    );
    return documents[0];
  }

  count(filter: Filter = {}, options: { readonly signal?: AbortSignal } = {}): Promise<CountResult> {
    return this.client.count(this.name, compileQuery(filter), options.signal);
  }
  groupCount(
    filter: Filter | undefined,
    groupBy: string,
    options: { readonly signal?: AbortSignal } = {},
  ): Promise<GroupCountResult> {
    return this.client.groupCount(this.name, compileQuery(filter ?? {}), groupBy, options.signal);
  }
  async insertOne<U extends InputDocument>(
    document: InsertDocument<T, U>,
    options: { readonly signal?: AbortSignal } = {},
  ): Promise<T> {
    return (await this.client.insertOne(this.name, document, options.signal)) as T;
  }
  updateOne(filter: Filter, update: Update, options: { readonly signal?: AbortSignal } = {}): Promise<MutationResult> {
    return this.client.executeMutation(
      this.name,
      "updateOne",
      compileQuery(filter),
      compileUpdate(update),
      options.signal,
    ) as Promise<MutationResult>;
  }
  updateMany(filter: Filter, update: Update, options: { readonly signal?: AbortSignal } = {}): Promise<MutationResult> {
    return this.client.executeMutation(
      this.name,
      "updateMany",
      compileQuery(filter),
      compileUpdate(update),
      options.signal,
    ) as Promise<MutationResult>;
  }
  deleteOne(filter: Filter, options: { readonly signal?: AbortSignal } = {}): Promise<DeleteResult> {
    return this.client.executeMutation(
      this.name,
      "deleteOne",
      compileQuery(filter),
      undefined,
      options.signal,
    ) as Promise<DeleteResult>;
  }
  deleteMany(filter: Filter, options: { readonly signal?: AbortSignal } = {}): Promise<DeleteResult> {
    return this.client.executeMutation(
      this.name,
      "deleteMany",
      compileQuery(filter),
      undefined,
      options.signal,
    ) as Promise<DeleteResult>;
  }
}

export class RemoteLiveQuery<T extends Document> {
  readonly mode = "remote" as const;
  constructor(
    private readonly client: MeldbaseClient,
    readonly collection: string,
    readonly spec: import("../types.js").QuerySpec,
    private readonly seekPagination = false,
  ) {}
  fetch(options: { readonly signal?: AbortSignal } = {}): Promise<T[]> {
    return this.client.fetchQuery<T>(this.collection, this.spec, options.signal);
  }
  async fetchPage(options: { readonly signal?: AbortSignal } = {}): Promise<PageResult<T>> {
    if (!this.seekPagination) throw new Error("fetchPage requires a query created with first");
    const documents = await this.fetch(options);
    const last = documents.at(-1);
    return { documents, ...(last && this.spec.sort ? { nextCursor: pageCursorFor(last, this.spec.sort) } : {}) };
  }
  subscribe(listener: SnapshotListener<T>, options: SubscribeOptions = {}): Unsubscribe {
    return this.client.subscribe(this.collection, this.spec, listener as SnapshotListener<Document>, options);
  }
}
