export type RecordValue = Record<string, unknown>;

export type AdminSample = {
  version: number;
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
  version: number;
  samples: AdminSample[];
};

export type DiagnosticEvent = {
  sequence: number;
  capturedAt: string;
  kind: string;
  stage?: string;
  errorClass?: string;
  outcome: string;
  durationNanos: number;
  documentsExamined?: number;
  documentsReturned?: number;
  changes?: number;
};

export type DiagnosticResponse = {
  version: number;
  session?: string;
  truncated?: boolean;
  hasMore?: boolean;
  events: DiagnosticEvent[];
  stats?: RecordValue;
};

export type ConnectionState = "idle" | "connecting" | "live" | "retrying" | "error";
