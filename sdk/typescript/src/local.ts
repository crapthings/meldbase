import type { DeleteResult, Document, Filter, InputDocument, MutationResult, QuerySpec, Update, Value } from "./types.js";
import type { QueryOptions } from "./query.js";
import { cloneDocument, cloneValue, valueEquals } from "./safe-value.js";
import { compileQuery, executeQuery, matches } from "./query.js";
import { applyMutation, compileUpdate } from "./mutation.js";

export type Unsubscribe = () => void;
export type SnapshotListener<T extends Document> = (documents: readonly T[]) => void;

export class LiveQuery<T extends Document> {
	readonly mode = "local" as const;
  readonly spec: QuerySpec;
  readonly #source: LocalCollection<T>;

  constructor(source: LocalCollection<T>, filter: Filter, options: QueryOptions = {}) {
    this.#source = source;
    this.spec = compileQuery(filter, options);
  }

  fetch(): T[] {
    return this.#source.execute(this.spec);
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
    if (this.#documents.has(copy._id)) throw new Error(`Duplicate _id: ${copy._id}`);
    this.#documents.set(copy._id, copy);
    this.markChanged();
  }

  insertOne(document: InputDocument): T {
    const fields = cloneValue(document as { readonly [key: string]: Value }) as Record<string, Value>;
    if (fields._id === undefined) fields._id = newLocalID();
    const copy = cloneDocument(fields as T);
    this.insert(copy);
    return cloneDocument(copy);
  }

  replace(document: T): void {
    const copy = cloneDocument(document);
    this.#documents.set(copy._id, copy);
    this.markChanged();
  }

  remove(id: string): boolean {
    const removed = this.#documents.delete(id);
    if (removed) this.markChanged();
    return removed;
  }

  find(filter: Filter = {}, options: QueryOptions = {}): LiveQuery<T> {
    return new LiveQuery(this, filter, options);
  }

  findOne(filter: Filter = {}, options: QueryOptions = {}): T | undefined {
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
    return executeQuery(this.#documents.values(), spec).map(cloneDocument);
  }

  onChange(listener: () => void): Unsubscribe {
    this.#listeners.add(listener);
    return () => { this.#listeners.delete(listener); };
  }

  private updateMatching(filter: Filter, update: Update, one: boolean): MutationResult {
    const query = compileQuery(filter); const mutation = compileUpdate(update); const staged: T[] = []; let matchedCount = 0;
    for (const document of this.#documents.values()) {
      if (!matches(document, query.where) || (one && matchedCount > 0)) continue;
      matchedCount += 1; const after = applyMutation(document, mutation); if (!valueEquals(document, after)) staged.push(after);
    }
    this.batch(() => { for (const document of staged) this.replace(document); });
    return { matchedCount, modifiedCount: staged.length };
  }

  private deleteMatching(filter: Filter, one: boolean): DeleteResult {
    const query = compileQuery(filter); const ids: string[] = [];
    for (const document of this.#documents.values()) { if (matches(document, query.where) && (!one || ids.length === 0)) ids.push(document._id); }
    this.batch(() => { for (const id of ids) this.remove(id); }); return { deletedCount: ids.length };
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

function newLocalID(): string { const bytes = crypto.getRandomValues(new Uint8Array(16)); return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join(""); }

function cloneSnapshot<T extends Document>(documents: readonly T[]): T[] {
  return documents.map(cloneDocument);
}

function snapshotEquals<T extends Document>(left: readonly T[], right: readonly T[]): boolean {
  return left.length === right.length && left.every((document, i) => valueEquals(document, right[i] as T));
}
