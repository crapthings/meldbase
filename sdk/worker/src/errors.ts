export { MeldbaseError, MeldbaseInternalError } from "@meldbase/client";

/** A local compatibility failure: the Worker hub lacks a required capability. */
export class MeldbaseWorkerProtocolError extends Error {
  readonly required: readonly string[];

  constructor(required: readonly string[]) {
    super(`Meldbase worker protocol does not support: ${required.join(", ")}`);
    this.name = "MeldbaseWorkerProtocolError";
    this.required = Object.freeze([...required]);
  }
}
