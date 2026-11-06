/** Broker backend for the global recovery admission ledger. No producer is cut over here. */
import type { SqliteWorkerCommand, SqliteWorkerPreparedBackend } from "../infra/sqlite-worker-contract.js";
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
  const db = openRunRecoveryQueueCoordinator(context.databasePath);
  return {
    execute(command: SqliteWorkerCommand<RecoveryQueueWorkerOperations>) {
      switch (command.type) {
        case "recoveryQueue.incarnation":
          return readRecoveryQueueIncarnation(db);
        case "recoveryQueue.reserve":
          return reserveRecoveryQueueWork(db, command.input);
        case "recoveryQueue.start":
          return startRecoveryQueueWork(db, command.input.incarnation, command.input.workId);
        case "recoveryQueue.settle":
          return settleRecoveryQueueWork(
            db, command.input.incarnation, command.input.workId, command.input.terminalReceiptId,
          );
      }
    },
    close() {
      db.close();
    },
  };
}
