export type RecordValue = Record<string, unknown>;

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

export type HistoryResponse = {
  samples: AdminSample[];
};

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

export type DiagnosticResponse = {
  session?: string;
  truncated?: boolean;
  hasMore?: boolean;
  events: DiagnosticEvent[];
  stats?: RecordValue;
};

export type IndexCatalogEntry = {
  collection: string;
  name: string;
  fields: Array<{ path: string; order: 1 | -1 }>;
  unique: boolean;
};

export type IndexCatalogResponse = {
  indexes: IndexCatalogEntry[];
};

export type ConnectionState = "idle" | "connecting" | "live" | "retrying" | "error";
