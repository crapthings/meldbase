export class MeldbaseRemoteError extends Error {
  constructor(readonly code: string, readonly status: number, readonly operation: string) {
    super(`Meldbase ${operation} failed: ${code}`);
    this.name = "MeldbaseRemoteError";
  }
}

/** The client has been closed permanently. Create a new client to reconnect. */
export class MeldbaseClientClosedError extends Error {
  constructor() {
    super("Meldbase client is closed; create a new client to reconnect");
    this.name = "MeldbaseClientClosedError";
  }
}

/**
 * The insert may have reached the server, but the client could not verify its
 * result. Use documentId to reconcile before retrying the same logical write.
 */
export class MeldbaseInsertUnknownResultError extends Error {
  readonly cause: unknown;
  constructor(readonly documentId: string, cause: unknown) {
    super(`Insert result is unknown for document ${documentId}; the document may have been created`);
    this.name = "MeldbaseInsertUnknownResultError";
    this.cause = cause;
  }
}

export class MeldbaseRPCError extends MeldbaseRemoteError {
  constructor(code: string, status: number) {
    super(code, status, "RPC");
    this.name = "MeldbaseRPCError";
  }
}

export class MeldbaseRPCUnknownResultError extends Error {
  constructor(readonly requestId: string) {
    super("Realtime RPC connection closed before a result was received; the method may have executed");
    this.name = "MeldbaseRPCUnknownResultError";
  }
}

export class MeldbaseProtocolError extends Error {
  readonly required: readonly string[];
  constructor(required: readonly string[]) {
    super(`Meldbase realtime protocol does not support: ${required.join(", ")}`);
    this.name = "MeldbaseProtocolError";
    this.required = Object.freeze([...required]);
  }
}
