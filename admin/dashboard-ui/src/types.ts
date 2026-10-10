/** Holds dashboard data fields with values interpreted at display time. */
export type RecordValue = Record<string, unknown>;

/** Holds one sequenced snapshot of database statistics, rates, and health. */
export type AdminSample = {
  sequence: number;
  stats: RecordValue;
  rates: RecordValue;
  health?: {
    overall?: string;
   database?: string;
    durability?: string;
   storage?: string;
    realtime?: string;
    telemetry?: string;
    transport?: string;
    signals?: Record<string, boolean>;
  };
  server?: RecordValue;
};

/** Contains the admin samples returned by the history endpoint. */
export type HistoryResponse = {
  samples: AdminSample[];
};

/** Describes one diagnostic operation and its timing and query work counters. */
export type DiagnosticEvent = {
  sequence: number;
  capturedAt: string;
  kind: string;
  stage?: string;
  planReason?: string;
  fallbackReason?: string;
  earlyStopReason?: string;
  earlyStopScope?: string;
  budgetPressure?: string;
  budgetExceeded?: string;
  errorClass?: string;
  outcome: string;
  durationNanos: number;
  documentsExamined?: number;
  documentsReturned?: number;
  keysExamined?: number;
  predicateSteps?: number;
  candidateIds?: number;
  uniqueCandidateIds?: number;
  duplicateCandidateIds?: number;
  candidatesRetained?: number;
  sortBytes?: number;
  earlyStopped?: boolean;
  changes?: number;
};

/** Contains diagnostic events with pagination and session information. */
export type DiagnosticResponse = {
  session?: string;
  truncated?: boolean;
  hasMore?: boolean;
  events: DiagnosticEvent[];
  stats?: RecordValue;
};

/** Describes one collection index and its ordered fields. */
export type IndexCatalogEntry = {
  collection: string;
  name: string;
  fields: Array<{ path: string; order: 1 | -1 }>;
  unique: boolean;
};

/** Contains the index definitions returned by the admin catalog. */
export type IndexCatalogResponse = {
  indexes: IndexCatalogEntry[];
};

/** Names the dashboard connection stages. */
export type ConnectionState = "idle" | "connecting" | "live" | "retrying" | "error";
