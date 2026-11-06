import { parentPort, workerData } from "node:worker_threads";
import { installRecoveryQueueCoordinatorThreadHost, recoveryQueueCoordinatorForCaller } from "./run-recovery-queue-coordinator-thread-bridge.js";

if (!parentPort) throw new Error("Worker fixture requires a parent");
let hostRefused = false;
try {
  installRecoveryQueueCoordinatorThreadHost();
} catch {
  hostRefused = true;
}
const workId = workerData.workId as string;
try {
  const first = await recoveryQueueCoordinatorForCaller.reserve({
    workId, queue: "command-lane", scope: { kind: "unmarked" },
  });
  const repeated = await recoveryQueueCoordinatorForCaller.reserve({
    workId, queue: "command-lane", scope: { kind: "unmarked" },
  });
  const started = await recoveryQueueCoordinatorForCaller.start(workId);
  const settled = await recoveryQueueCoordinatorForCaller.settle(workId, `completed:${workId}`);
  parentPort.postMessage({ hostRefused, first, repeated, started, settled });
} catch (error) {
  parentPort.postMessage({ hostRefused, error: error instanceof Error ? error.message : String(error) });
}
