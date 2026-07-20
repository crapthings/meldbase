import { ERROR_PATTERN } from "./shared.js";

export class MeldbaseMethodError extends Error {
  readonly code: string;

  constructor(code: string) {
    if (!ERROR_PATTERN.test(code)) throw new TypeError("Invalid Meldbase method error code");
    super(`Meldbase method failed: ${code}`);
    this.name = "MeldbaseMethodError";
    this.code = code;
  }
}

export class MeldbaseWorkerProtocolError extends Error {
  readonly required: readonly string[];

  constructor(required: readonly string[]) {
    super(`Meldbase worker protocol does not support: ${required.join(", ")}`);
    this.name = "MeldbaseWorkerProtocolError";
    this.required = Object.freeze([...required]);
  }
}
