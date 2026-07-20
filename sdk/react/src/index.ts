import { useMemo, useSyncExternalStore } from "react";
import type { LiveQuery, Unsubscribe } from "@meldbase/client/local";
import type { RemoteLiveQuery } from "@meldbase/client/remote";
import type { SyncState, SyncStatus } from "@meldbase/client/remote";
import type { Document } from "@meldbase/client/types";

const EMPTY_DOCUMENTS: readonly never[] = Object.freeze([]);

export interface UseLiveQueryOptions<T extends Document> {
  // Supply the same snapshot on the server and first client render when the
  // query is remote. This avoids hydration drift without creating a second
  // query language or cache.
  readonly initialData?: readonly T[];
}

export interface LiveQueryResult<T extends Document> {
  readonly documents: readonly T[];
  readonly status: SyncState;
  readonly error?: Error;
  readonly token?: string;
}

export type SupportedLiveQuery<T extends Document> = LiveQuery<T> | RemoteLiveQuery<T>;

export function useLiveQuery<T extends Document>(
  query: SupportedLiveQuery<T>,
  options: UseLiveQueryOptions<T> = {},
): LiveQueryResult<T> {
  const initialData = options.initialData;
  const observer = useMemo(
    () => new QueryObserver(query, initialData),
    // initialData is deliberately initial-only. Changing data for the same
    // query must come through its live subscription, not prop mirroring.
    [query],
  );
  return useSyncExternalStore(observer.subscribe, observer.getSnapshot, observer.getServerSnapshot);
}

class QueryObserver<T extends Document> {
  readonly #query: SupportedLiveQuery<T>;
  readonly #listeners = new Set<() => void>();
  readonly #serverSnapshot: LiveQueryResult<T>;
  #snapshot: LiveQueryResult<T>;
  #stop: Unsubscribe | undefined;

  constructor(query: SupportedLiveQuery<T>, initialData?: readonly T[]) {
    this.#query = query;
    const documents = initialData ?? EMPTY_DOCUMENTS;
		this.#snapshot = query.mode === "local"
      ? Object.freeze({ documents: query.fetch(), status: "live" })
      : Object.freeze({ documents, status: "idle" });
    this.#serverSnapshot = this.#snapshot;
  }

  readonly subscribe = (listener: () => void): Unsubscribe => {
    this.#listeners.add(listener);
    if (!this.#stop) this.#start();
    return () => {
      this.#listeners.delete(listener);
      if (this.#listeners.size === 0) {
        this.#stop?.();
        this.#stop = undefined;
      }
    };
  };

  readonly getSnapshot = (): LiveQueryResult<T> => this.#snapshot;
  readonly getServerSnapshot = (): LiveQueryResult<T> => this.#serverSnapshot;

  #start(): void {
		if (this.#query.mode === "remote") {
      this.#stop = this.#query.subscribe(
        (documents) => this.#publish({ documents, status: "live" }),
        { onStatus: (status) => this.#status(status) },
      );
      return;
    }
    this.#stop = this.#query.subscribe((documents) => this.#publish({ documents, status: "live" }));
  }

  #status(status: SyncStatus): void {
    this.#publish({
      documents: this.#snapshot.documents,
      status: status.state,
      ...(status.error ? { error: status.error } : {}),
      ...(status.token ? { token: status.token } : {}),
    });
  }

  #publish(snapshot: LiveQueryResult<T>): void {
    if (sameSnapshot(this.#snapshot, snapshot)) return;
    this.#snapshot = Object.freeze(snapshot);
    for (const listener of this.#listeners) listener();
  }
}

function sameSnapshot<T extends Document>(left: LiveQueryResult<T>, right: LiveQueryResult<T>): boolean {
  return left.documents === right.documents
    && left.status === right.status
    && left.error === right.error
    && left.token === right.token;
}
