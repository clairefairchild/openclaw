import { afterEach, describe, expect, it } from "vitest";
import {
  addSession,
  deleteSession,
  hasTrackedProcessForRecoverySession,
  settleExecSessionFinalization,
  type ProcessSession,
} from "../agents/bash-process-registry.js";
import { enqueueCommandInLane, setCommandLaneConcurrency } from "../process/command-queue.js";
import { hasQueuedCommandForRecoverySession } from "../process/command-queue.state.js";
import { resetCommandQueueStateForTest } from "../process/command-queue.test-support.js";

const key = "agent:main:main";

afterEach(() => resetCommandQueueStateForTest());

describe("run recovery process-local presence reads", () => {
  it("sees queued work for the target and requester, then loses it on cancellation", async () => {
    const lane = "recovery-custody-test";
    setCommandLaneConcurrency(lane, 0);
    const controller = new AbortController();
    const queued = enqueueCommandInLane(lane, async () => undefined, {
      abortSignal: controller.signal,
      taskIdentity: { taskKind: "turn", sessionKey: key, runId: "run-1" },
    });
    expect(hasQueuedCommandForRecoverySession(key)).toBe(true);
    expect(hasQueuedCommandForRecoverySession("agent:other:main")).toBe(false);
    controller.abort();
    await expect(queued).rejects.toThrow();
    expect(hasQueuedCommandForRecoverySession(key)).toBe(false);
  });

  it("includes an unbackgrounded or finalizing process but never infers clear", () => {
    const session = { id: "recovery-process-test", agentId: "main", sessionKey: key,
      backgrounded: false, finalizing: true } as ProcessSession;
    addSession(session);
    try {
      expect(hasTrackedProcessForRecoverySession("main", key)).toBe(true);
      expect(hasTrackedProcessForRecoverySession("other", key)).toBe(false);
      expect(hasTrackedProcessForRecoverySession("main", "agent:other:main")).toBe(false);
      deleteSession(session.id);
      expect(hasTrackedProcessForRecoverySession("main", key)).toBe(true);
    } finally {
      settleExecSessionFinalization(session);
      deleteSession(session.id);
    }
    expect(hasTrackedProcessForRecoverySession("main", key)).toBe(false);
  });
});
