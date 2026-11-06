/** Async host access to the global queue ledger. This does not activate producer cutover. */
import { openSqliteWorkerStore } from "../infra/sqlite-worker-store.js";
import {
  resolveRunRecoveryQueueCoordinatorPath,
  type RecoveryQueueReservation,
} from "./run-recovery-queue-coordinator.js";
import type { RecoveryQueueWorkerOperations } from "./run-recovery-queue-coordinator.worker.js";

export async function openRecoveryQueueCoordinatorBroker(
  databasePath = resolveRunRecoveryQueueCoordinatorPath(),
) {
  const store = await openSqliteWorkerStore<RecoveryQueueWorkerOperations>({
    moduleUrl: new URL("./run-recovery-queue-coordinator.worker.ts", import.meta.url),
    databasePath,
    input: undefined,
  });
  const incarnation = await store.execute({ type: "recoveryQueue.incarnation", input: undefined });
  return {
    incarnation,
    reserve: (work: Omit<RecoveryQueueReservation, "incarnation">) =>
      store.execute({ type: "recoveryQueue.reserve", input: { ...work, incarnation } }),
    start: (workId: string) =>
      store.execute({ type: "recoveryQueue.start", input: { incarnation, workId } }),
    settle: (workId: string, terminalReceiptId: string) =>
      store.execute({
        type: "recoveryQueue.settle",
        input: { incarnation, workId, terminalReceiptId },
      }),
    close: () => store.close(),
  };
}
