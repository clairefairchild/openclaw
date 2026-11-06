import { afterEach, describe, expect, it } from "vitest";
import {
  addSession,
  deleteSession,
  hasTrackedProcessForRecoverySession,
  isTrackedProcessRecoverySnapshotCurrent,
  readTrackedProcessRecoverySnapshot,
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

  it("binds process observations to a registry lifetime and invalidates a pre-spawn empty read", () => {
    const empty = readTrackedProcessRecoverySnapshot("main", key);
    expect(empty.active).toBe(false);
    expect(isTrackedProcessRecoverySnapshotCurrent(empty)).toBe(true);
    const session = { id: "recovery-process-generation", agentId: "main",
      sessionKey: key, backgrounded: false } as ProcessSession;
    addSession(session);
    const occupied = readTrackedProcessRecoverySnapshot("main", key);
    try {
      expect(occupied.active).toBe(true);
      expect(occupied.instance).toBe(empty.instance);
      expect(occupied.version).not.toBe(empty.version);
      expect(isTrackedProcessRecoverySnapshotCurrent(empty)).toBe(false);
      expect(() => addSession({ ...session } as ProcessSession)).toThrow(
        "process session id is already owned");
      expect(readTrackedProcessRecoverySnapshot("main", key)).toEqual(occupied);
      // Post-registration metadata edits cannot change the captured owner.
      session.agentId = "other";
      session.sessionKey = "agent:other:main";
      expect(readTrackedProcessRecoverySnapshot("main", key).active).toBe(true);
      expect(readTrackedProcessRecoverySnapshot("other", key).active).toBe(false);
      deleteSession(session.id);
      expect(readTrackedProcessRecoverySnapshot("main", key).active).toBe(true);
    } finally {
      settleExecSessionFinalization(session);
      deleteSession(session.id);
    }
    const retired = readTrackedProcessRecoverySnapshot("main", key);
    expect(retired.active).toBe(false);
    expect(retired.version).not.toBe(empty.version);
    expect(isTrackedProcessRecoverySnapshotCurrent(occupied)).toBe(false);
    expect(isTrackedProcessRecoverySnapshotCurrent({ ...retired, instance: "old-gateway" }))
      .toBe(false);
  });

  it("vetoes a process whose agent or session attribution is missing", () => {
    for (const [index, owner] of [
      { agentId: undefined, sessionKey: key },
      { agentId: "main", sessionKey: undefined },
      { agentId: undefined, sessionKey: undefined },
    ].entries()) {
      const session = { id: `recovery-ambiguous-${index}`, ...owner,
        backgrounded: false } as ProcessSession;
      addSession(session);
      try {
        expect(hasTrackedProcessForRecoverySession("main", key)).toBe(true);
      } finally {
        settleExecSessionFinalization(session);
        deleteSession(session.id);
      }
    }
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
