/** Async host access to the global queue ledger. This does not activate producer cutover. */
import { acquireGatewayStateOwner } from "../infra/gateway-state-owner.js";
import {
  openSqliteWorkerStore,
  runSqliteWorkerStoreOperation,
  runSqliteWorkerStoreWrite,
  type SqliteWorkerStore,
} from "../infra/sqlite-worker-store.js";
import {
  resolveRunRecoveryQueueCoordinatorPath,
  type RecoveryQueueReservation,
} from "./run-recovery-queue-coordinator.js";
import type { RecoveryQueueWorkerOperations } from "./run-recovery-queue-coordinator.worker.js";

export async function openRecoveryQueueCoordinatorBroker(
  databasePath = resolveRunRecoveryQueueCoordinatorPath(),
) {
  // The host owns the physical path. Native writes borrow this authority at
  // transaction and commit grants; a second host cannot publish a rival ledger.
  const owner = acquireGatewayStateOwner({ databasePath, role: "gateway" });
  let store: SqliteWorkerStore<RecoveryQueueWorkerOperations> | undefined;
  let incarnation: string;
  try {
    store = await openSqliteWorkerStore<RecoveryQueueWorkerOperations>({
      moduleUrl: new URL("./run-recovery-queue-coordinator.worker.ts", import.meta.url),
      databasePath,
      input: undefined,
    });
    incarnation = await runSqliteWorkerStoreOperation(
      store,
      (scope) => scope.execute({ type: "recoveryQueue.incarnation", input: undefined }),
      undefined,
      () => owner.assertCurrent(),
    );
  } catch (error) {
    // An uncertain worker open/close is not proof that native custody was
    // released. Retain the physical owner unless a known store closes cleanly.
    if (store) {
      try {
        await store.close();
        owner.release();
      } catch {
        // The original admission failure remains the actionable error.
      }
    } else {
      owner.release();
    }
    throw error;
  }
  const activeStore = store;
  if (!activeStore) {
    throw new Error("Recovery queue coordinator broker opened without a store");
  }
  const write = <T>(operation: (scope: Pick<typeof activeStore, "execute">) => Promise<T>): Promise<T> =>
    runSqliteWorkerStoreWrite(activeStore, operation, () => owner.assertCurrent(), [databasePath]);
  return {
    incarnation,
    reserve: (work: Omit<RecoveryQueueReservation, "incarnation">) =>
      write((scope) => scope.execute({ type: "recoveryQueue.reserve", input: { ...work, incarnation } })),
    start: (workId: string) =>
      write((scope) => scope.execute({ type: "recoveryQueue.start", input: { incarnation, workId } })),
    settle: (workId: string, terminalReceiptId: string) =>
      write((scope) => scope.execute({
        type: "recoveryQueue.settle",
        input: { incarnation, workId, terminalReceiptId },
      })),
    close: async () => {
      await activeStore.close();
      owner.release();
    },
  };
}
