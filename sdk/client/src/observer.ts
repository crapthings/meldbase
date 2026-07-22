import type { Document } from "./types.js";

/** Stops a local or remote query observer. */
export type Unsubscribe = () => void;

/** Receives an immutable snapshot whenever a query result changes. */
export type SnapshotListener<T extends Document> = (documents: readonly T[]) => void;
