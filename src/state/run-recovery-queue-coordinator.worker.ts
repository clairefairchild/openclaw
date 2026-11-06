/** Broker backend for the global recovery admission ledger. No producer is cut over here. */
import type { SqliteWorkerCommand, SqliteWorkerPreparedBackend } from "../infra/sqlite-worker-contract.js";
import { acquireGatewayStateOwner } from "../infra/gateway-state-owner.js";
import {
  openRunRecoveryQueueCoordinator,
  readRecoveryQueueIncarnation,
  reserveRecoveryQueueWork,
  settleRecoveryQueueWork,
  startRecoveryQueueWork,
  type RecoveryQueueReservation,
} from "./run-recovery-queue-coordinator.js";

export type RecoveryQueueWorkerOperations = {
  "recoveryQueue.incarnation": { input: undefined; output: string };
  "recoveryQueue.reserve": {
    input: RecoveryQueueReservation;
    output: "reserved" | "repeated";
  };
  "recoveryQueue.start": {
    input: { incarnation: string; workId: string };
    output: "started" | "repeated";
  };
  "recoveryQueue.settle": {
    input: { incarnation: string; workId: string; terminalReceiptId: string };
    output: "settled" | "repeated";
  };
};

export function createSqliteWorkerBackend(
  _input: undefined,
  context: { databasePath: string },
): SqliteWorkerPreparedBackend<RecoveryQueueWorkerOperations> {
  // The physical owner lives with the native connection. A second process
  // cannot publish a competing ledger, and failed native close retains custody.
  const owner = acquireGatewayStateOwner({ databasePath: context.databasePath });
  let db: ReturnType<typeof openRunRecoveryQueueCoordinator>;
  try {
    db = openRunRecoveryQueueCoordinator(context.databasePath, owner);
  } catch (error) {
    // A failed open may have created or changed the file. Keep the owner until
    // process exit rather than offering another actor ambiguous write custody.
    throw error;
  }
  return {
    execute(command: SqliteWorkerCommand<RecoveryQueueWorkerOperations>) {
      owner.assertCurrent();
      switch (command.type) {
        case "recoveryQueue.incarnation":
          return readRecoveryQueueIncarnation(db);
        case "recoveryQueue.reserve":
          return reserveRecoveryQueueWork(db, command.input, owner);
        case "recoveryQueue.start":
          return startRecoveryQueueWork(db, command.input.incarnation, command.input.workId, owner);
        case "recoveryQueue.settle":
          return settleRecoveryQueueWork(
            db, command.input.incarnation, command.input.workId, command.input.terminalReceiptId, owner,
          );
      }
    },
    close() {
      db.close();
      owner.release();
    },
  };
}
