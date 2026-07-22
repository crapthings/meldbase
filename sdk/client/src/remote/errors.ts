import type { Value } from "../types.js";

export type MeldbaseErrorData = Readonly<Record<string, Value>>;

/** An expected, application-owned RPC outcome. */
export class MeldbaseError extends Error {
  readonly data?: MeldbaseErrorData;

  constructor(readonly code: string, data?: MeldbaseErrorData) {
    if (!/^[a-z][a-z0-9_]{0,31}(?:\.[a-z][a-z0-9_]{0,31})+$/.test(code)) throw new TypeError("Invalid Meldbase business error code");
    super(`Meldbase operation failed: ${code}`);
    this.name = "MeldbaseError";
    if (data !== undefined) this.data = data;
  }
}

/** A Meldbase-owned failure or an outcome that could not be determined safely. */
export class MeldbaseInternalError extends Error {
  readonly cause: unknown;

  constructor(readonly code: string, readonly status = 0, readonly operation = "operation", cause?: unknown) {
    if (!/^[a-z][a-z0-9_]{0,63}$/.test(code)) throw new TypeError("Invalid Meldbase internal error code");
    super(`Meldbase ${operation} failed: ${code}`);
    this.name = "MeldbaseInternalError";
    this.cause = cause;
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
export class MeldbaseProtocolError extends Error {
  readonly required: readonly string[];
  constructor(required: readonly string[]) {
    super(`Meldbase realtime protocol does not support: ${required.join(", ")}`);
    this.name = "MeldbaseProtocolError";
    this.required = Object.freeze([...required]);
  }
}
