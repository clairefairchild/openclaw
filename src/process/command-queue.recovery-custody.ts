/** Opt-in command-lane custody bridge. This does not declare producer cutover. */
import { enqueueCommandInLane, isCommandLaneTaskMarkerCurrent } from "./command-queue.js";
import type { CommandLaneTaskMarker } from "./command-queue.state.js";
import type { CommandQueueEnqueueOptions } from "./command-queue.types.js";
import { getRecoveryQueueCoordinatorBroker } from "../state/run-recovery-queue-coordinator-broker.js";
import type { RecoveryWorkScope } from "../state/run-recovery-queue-coordinator.js";

/**
 * Reserve before publication, start before task effects, and settle only after
 * a current-generation task resolves or a queued task is definitively rejected.
 * Failed, timed-out, reset, or ambiguous work remains in the ledger for review.
 * A caller must provide a stable work ID that can be reconciled after restart.
 */
export async function enqueueCommandInLaneWithRecoveryCustody<T>(
  lane: string,
  task: (marker: CommandLaneTaskMarker) => Promise<T>,
  custody: { workId: string; scope: RecoveryWorkScope },
  opts?: CommandQueueEnqueueOptions,
): Promise<T> {
  const broker = await getRecoveryQueueCoordinatorBroker();
  await broker.reserve({ workId: custody.workId, queue: "command-lane", scope: custody.scope });
  let startAttempted = false;
  try {
    return await enqueueCommandInLane(lane, async (marker) => {
      startAttempted = true;
      await broker.start(custody.workId);
      return await task(marker);
    }, {
      ...opts,
      onTaskSuccess: async (marker) => {
        await opts?.onTaskSuccess?.(marker);
        if (isCommandLaneTaskMarkerCurrent(marker)) {
          await broker.settle(custody.workId, `completed:${custody.workId}`);
        }
      },
    });
  } catch (error) {
    // No dequeue/start attempt means the queue itself rejected the reservation.
    // Once start is attempted, even a transport error is an ambiguous outcome.
    if (!startAttempted) {
      await broker.settle(custody.workId, `rejected:${custody.workId}`);
    }
    throw error;
  }
}
