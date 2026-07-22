import type { DeleteResult, Document, Filter, InputDocument, MutationResult, QuerySpec, Update, Value } from "./types.js";
import type { FindOneOptions, QueryOptions } from "./query.js";
import { assertDocumentID, cloneDocument, cloneValue, newDocumentID, valueEquals } from "./safe-value.js";
import { compileQuery, executeQuery, matches } from "./query.js";
import { applyMutation, compileUpdate } from "./mutation.js";
import { pageCursorFor, type PageResult } from './cursor.js';
import type { SnapshotListener, Unsubscribe } from "./observer.js";

export type { SnapshotListener, Unsubscribe } from "./observer.js";

export class LiveQuery<T extends Document> {
	readonly mode = "local" as const;
  readonly spec: QuerySpec;
  readonly #source: LocalCollection<T>;
  readonly #seekPagination: boolean;

  constructor(source: LocalCollection<T>, filter: Filter, options: QueryOptions = {}) {
    this.#source = source;
    this.spec = compileQuery(filter, options);
    this.#seekPagination = options.first !== undefined;
  }

  fetch(): T[] {
    return this.#source.execute(this.spec);
  }

  fetchPage(): PageResult<T> {
    if (!this.#seekPagination) throw new Error("fetchPage requires a query created with first");
    const documents = this.fetch(); const last = documents.at(-1);
    return { documents, ...(last && this.spec.sort ? { nextCursor: pageCursorFor(last, this.spec.sort) } : {}) };
  }

  subscribe(listener: SnapshotListener<T>): Unsubscribe {
    let last = this.fetch();
    listener(cloneSnapshot(last));
    return this.#source.onChange(() => {
      const next = this.fetch();
      if (!snapshotEquals(last, next)) {
        last = next;
        listener(cloneSnapshot(next));
      }
    });
  }
}

export class LocalCollection<T extends Document = Document> {
  readonly #documents = new Map<string, T>();
  readonly #listeners = new Set<() => void>();
  #batchDepth = 0;
  #changed = false;

  constructor(initial: readonly T[] = []) {
    this.batch(() => { for (const document of initial) this.insert(document); });
  }

  insert(document: T): void {
    const copy = cloneDocument(document);
    assertDocumentID(copy._id);
    if (this.#documents.has(copy._id)) throw new Error(`Duplicate _id: ${copy._id}`);
    this.#documents.set(copy._id, copy);
    this.markChanged();
  }

  insertOne(document: InputDocument): T {
    const fields = cloneValue(document as { readonly [key: string]: Value }) as Record<string, Value>;
    if (fields._id === undefined) fields._id = newDocumentID();
    const copy = cloneDocument(fields as T);
    this.insert(copy);
    return cloneDocument(copy);
  }

  /** Create or fully replace one local document addressed by its _id. */
  upsert(document: T): void {
    const copy = cloneDocument(document);
    assertDocumentID(copy._id);
    this.#documents.set(copy._id, copy);
    this.markChanged();
  }

  remove(id: string): boolean {
    assertDocumentID(id);
    const removed = this.#documents.delete(id);
    if (removed) this.markChanged();
    return removed;
  }

  find(filter: Filter = {}, options: QueryOptions = {}): LiveQuery<T> {
    return new LiveQuery(this, filter, options);
  }

  findOne(filter: Filter = {}, options: FindOneOptions = {}): T | undefined {
    const documents = this.execute(compileQuery(filter, { ...options, limit: 1 }));
    return documents[0];
  }

  updateOne(filter: Filter, update: Update): MutationResult { return this.updateMatching(filter, update, true); }
  updateMany(filter: Filter, update: Update): MutationResult { return this.updateMatching(filter, update, false); }
  deleteOne(filter: Filter): DeleteResult { return this.deleteMatching(filter, true); }
  deleteMany(filter: Filter): DeleteResult { return this.deleteMatching(filter, false); }

  batch(action: () => void): void {
    this.#batchDepth += 1;
    try { action(); } finally {
      this.#batchDepth -= 1;
      if (this.#batchDepth === 0 && this.#changed) this.flushChange();
    }
  }

  execute(spec: QuerySpec): T[] {
    return executeQuery(this.queryCandidates(spec), spec).map(cloneDocument);
  }

  onChange(listener: () => void): Unsubscribe {
    this.#listeners.add(listener);
    return () => { this.#listeners.delete(listener); };
  }

  private updateMatching(filter: Filter, update: Update, one: boolean): MutationResult {
    const query = compileQuery(filter); const mutation = compileUpdate(update); const staged: T[] = []; let matchedCount = 0;
    for (const document of this.queryCandidates(query)) {
      if (!matches(document, query.where) || (one && matchedCount > 0)) continue;
      matchedCount += 1; const after = applyMutation(document, mutation); if (!valueEquals(document, after)) staged.push(after);
    }
    this.batch(() => { for (const document of staged) this.upsert(document); });
    return { matchedCount, modifiedCount: staged.length };
  }

  private deleteMatching(filter: Filter, one: boolean): DeleteResult {
    const query = compileQuery(filter); const ids: string[] = [];
    for (const document of this.queryCandidates(query)) { if (matches(document, query.where) && (!one || ids.length === 0)) ids.push(document._id); }
    this.batch(() => { for (const id of ids) this.remove(id); }); return { deletedCount: ids.length };
  }

  private queryCandidates(spec: QuerySpec): Iterable<T> {
    const id = requiredDocumentID(spec.where);
    if (id === undefined) return this.#documents.values();
    const document = this.#documents.get(id);
    return document === undefined ? [] : [document];
  }

  private markChanged(): void {
    this.#changed = true;
    if (this.#batchDepth === 0) this.flushChange();
  }

  private flushChange(): void {
    this.#changed = false;
    for (const listener of [...this.#listeners]) queueMicrotask(listener);
  }
}

// An equality condition on _id is a required primary-key constraint only when
// it occurs outside OR/not. The remaining predicate still runs below, so an
// additional condition or contradictory _id comparison preserves exact query
// semantics while avoiding a collection scan.
function requiredDocumentID(expression: QuerySpec["where"]): string | undefined {
  if (expression.op === "compare" && expression.cmp === "eq" && expression.path === "_id" && typeof expression.value === "string") {
    return expression.value;
  }
  if (expression.op !== "and") return undefined;
  for (const argument of expression.args) {
    const id = requiredDocumentID(argument);
    if (id !== undefined) return id;
  }
  return undefined;
}


function cloneSnapshot<T extends Document>(documents: readonly T[]): T[] {
  return documents.map(cloneDocument);
}

function snapshotEquals<T extends Document>(left: readonly T[], right: readonly T[]): boolean {
  return left.length === right.length && left.every((document, i) => valueEquals(document, right[i] as T));
}
