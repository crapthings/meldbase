// Internal facade used by the root client entry. Public consumers import the
// remote-first root package; local state is available only from ./local.
export { MeldbaseClient } from "./remote/client.js";
export type { RemoteCollection, RemoteLiveQuery } from "./remote/collection.js";
export {
  MeldbaseClientClosedError,
  MeldbaseError,
  MeldbaseInternalError,
  MeldbaseProtocolError,
} from "./remote/errors.js";
export type { MeldbaseErrorData } from "./remote/errors.js";
export type {
  CallOptions,
  ClientOptions,
  RealtimeTicket,
  RPCTransport,
  SubscribeOptions,
  SyncState,
  SyncStatus,
  WebSocketLike,
} from "./remote/types.js";
