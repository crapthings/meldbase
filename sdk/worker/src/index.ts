/**
 * Public trusted-worker SDK entry point.
 *
 * @beta Public signatures are reviewed against sdk/api before each release.
 * Breaking changes require an explicit beta migration note.
 */
export {
  MeldbaseError,
  MeldbaseInternalError,
  MeldbaseWorkerProtocolError,
  MeldbaseWorker,
  readPolicy,
  rpc,
} from "./worker.js";
export type {
  RPCContext,
  RPCDefinition,
  RPCHandler,
  Actor,
  ReadPolicyContext,
  ReadPolicyDefinition,
  ReadPolicyHandler,
  ReadPolicyOptions,
  TransactionalRPCHandler,
  WorkerOptions,
  WorkerSocket,
  WorkerSocketFactory,
  WorkerState,
  WriteTransaction,
} from "./worker.js";
