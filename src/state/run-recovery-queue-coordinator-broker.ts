/** Async host access to the global queue ledger. This does not activate producer cutover. */
import { acquireGatewayStateOwner } from "../infra/gateway-state-owner.js";
import {
  openSqliteWorkerStore,
  runSqliteWorkerStoreOperation,
  runSqliteWorkerStoreWrite,
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
  const store = await openSqliteWorkerStore<RecoveryQueueWorkerOperations>({
    moduleUrl: new URL("./run-recovery-queue-coordinator.worker.ts", import.meta.url),
    databasePath,
    input: undefined,
  });
  const incarnation = await runSqliteWorkerStoreOperation(
    store,
    (scope) => scope.execute({ type: "recoveryQueue.incarnation", input: undefined }),
    undefined,
    () => owner.assertCurrent(),
  );
  const write = <T>(operation: (scope: Pick<typeof store, "execute">) => Promise<T>): Promise<T> =>
    runSqliteWorkerStoreWrite(store, operation, () => owner.assertCurrent(), [databasePath]);
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
      await store.close();
      owner.release();
    },
  };
}
