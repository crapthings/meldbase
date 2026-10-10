/**
 * Public entry point for @meldbase/client.
 *
 * @beta Public signatures are reviewed against sdk/api before each release.
 * Breaking changes require an explicit beta migration note. Do not import ./internal from an
 * application; that entry point is experimental.
 */
export type { FindOneOptions, QueryOptions } from "./query.js";
export type { PageResult } from "./cursor.js";
export { QueryValidationError, DEFAULT_QUERY_LIMITS } from "./types.js";
export { MeldbaseClient, MeldbaseClientClosedError, MeldbaseError, MeldbaseInternalError } from "./remote.js";
export type { RemoteCollection, RemoteLiveQuery } from "./remote.js";
export type { MeldbaseErrorData } from "./remote.js";
export { MeldbaseProtocolError } from "./remote.js";
export type {
  CallOptions,
  ClientOptions,
  RealtimeTicket,
  RPCTransport,
  SubscribeOptions,
  SyncState,
  SyncStatus,
  WebSocketLike,
} from "./remote.js";
export type { ProtocolDescriptor } from "./protocol.js";
export { documentID, isDocumentID, isDocumentIDValue, newDocumentID } from "./safe-value.js";
export type { DocumentID } from "./safe-value.js";
export type {
  CompareOperator,
  Comparison,
  CountResult,
  DeleteResult,
  Document,
  Filter,
  GroupCountResult,
  InputDocument,
  InsertDocument,
  MutationOperation,
  MutationResult,
  MutationSpec,
  Primitive,
  QueryExpr,
  QueryLimits,
  QuerySpec,
  QueryTypeName,
  SortField,
  Update,
  Value,
} from "./types.js";
