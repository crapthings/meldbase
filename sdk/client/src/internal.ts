/**
 * @internal
 *
 * Shared protocol primitives used by the Meldbase-maintained worker package.
 * They are deliberately isolated from the task-oriented client entry point.
 * Applications should use collections, `fetchPage`, and named RPCs instead of
 * constructing protocol envelopes directly.
 */
export { applyMutation, compileUpdate, decodeMutationSpec, encodeMutationSpec } from "./mutation.js";
export { MELDBASE_PROTOCOL_VERSION, decodeProtocolDescriptor, supportsProtocol } from "./protocol.js";
export { compileQuery, executeQuery, matches } from "./query.js";
export {
  decodeDocument,
  decodeQuerySpec,
  decodeValue,
  encodeDocument,
  encodeInputDocument,
  encodeQuerySpec,
  encodeValue,
} from "./wire.js";
export { assertDocumentID, isDocumentIDValue } from "./safe-value.js";
export type { ProtocolDescriptor } from "./protocol.js";
export type { WireMutationOperation, WireMutationSpec } from "./mutation.js";
export type { WireQueryExpr, WireQuerySpec, WireValue } from "./wire.js";
