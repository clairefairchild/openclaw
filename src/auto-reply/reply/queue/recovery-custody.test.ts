import { randomUUID } from "node:crypto";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createQueueTestRun } from "../queue.test-helpers.js";
import type { FollowupRun, QueueSettings } from "./types.js";
import { enqueueFollowupWithRecoveryReservation } from "./enqueue.js";
import { clearFollowupQueue, getExistingFollowupQueue } from "./state.js";

const broker = vi.hoisted(() => ({
  reserve: vi.fn(),
  start: vi.fn(),
  settle: vi.fn(),
  steerHandoff: vi.fn(),
  readWork: vi.fn(),
}));
vi.mock("../../../state/run-recovery-queue-coordinator-broker.js", () => ({
  getRecoveryQueueCoordinatorBroker: async () => broker,
}));

import {
  beginFollowupPublication,
  bindFollowupAggregateRecoverySources,
  carryFollowupRecoveryWork,
  followupRecoverySources,
  reserveFollowupRunRecoveryWork,
  recordFollowupSteerHandoff,
  settleFollowupRunRecoveryWork,
  startFollowupRunRecoveryWork,
} from "./recovery-custody.js";

const run = (prompt: string): FollowupRun => createQueueTestRun({ prompt });

beforeEach(() => {
  broker.reserve.mockReset().mockResolvedValue("reserved");
  broker.start.mockReset().mockResolvedValue("started");
  broker.settle.mockReset().mockResolvedValue("settled");
  broker.steerHandoff.mockReset().mockResolvedValue("recorded");
  broker.readWork.mockReset().mockResolvedValue(undefined);
});

describe("follow-up durable custody", () => {
  it("keeps an uncertain reservation on the same work ID before publishing", async () => {
    const source = run("accepted input");
    broker.reserve.mockRejectedValueOnce(new Error("ack lost"));
    await expect(reserveFollowupRunRecoveryWork(source)).rejects.toThrow("ack lost");
    expect(beginFollowupPublication(source)).toBe(true);
    await reserveFollowupRunRecoveryWork(source);
    expect(broker.reserve).toHaveBeenCalledTimes(2);
    expect(broker.reserve.mock.calls[0]?.[0]).toEqual(broker.reserve.mock.calls[1]?.[0]);
    expect(beginFollowupPublication(source)).toBe(false);
    expect(broker.start).not.toHaveBeenCalled();
    expect(broker.settle).not.toHaveBeenCalled();
  });

  it("never republishes after an uncertain terminal acknowledgment", async () => {
    const source = run("terminal attempt");
    await reserveFollowupRunRecoveryWork(source);
    broker.settle.mockRejectedValueOnce(new Error("terminal ack lost"));
    await expect(settleFollowupRunRecoveryWork(source, "rejected")).rejects.toThrow("terminal ack lost");
    await expect(reserveFollowupRunRecoveryWork(source)).rejects.toThrow("terminal attempt");
    await settleFollowupRunRecoveryWork(source, "rejected");
    expect(broker.settle.mock.calls[0]).toEqual(broker.settle.mock.calls[1]);
    await expect(settleFollowupRunRecoveryWork(source, "different")).rejects.toThrow("receipt changed");
  });

  it("starts before terminal settlement and carries source identity through summary compaction", async () => {
    const source = run("original");
    const compact = run("summary");
    const aggregate = run("aggregate");
    await reserveFollowupRunRecoveryWork(source);
    carryFollowupRecoveryWork(source, compact);
    bindFollowupAggregateRecoverySources(aggregate, [compact]);
    await startFollowupRunRecoveryWork(compact);
    await settleFollowupRunRecoveryWork(followupRecoverySources(aggregate)[0]!, "terminal:run-1:completed");
    const workId = broker.reserve.mock.calls[0]?.[0].workId;
    expect(broker.start).toHaveBeenCalledWith(workId);
    expect(broker.settle).toHaveBeenCalledWith(workId, `terminal:run-1:completed:${workId}`);
  });

  it("reconciles an ambiguous accepted handoff by exact work ID without settlement", async () => {
    const source = run("steered source");
    const steer = {
      sourceTurnId: "channel:message-1",
      targetRunId: "active-run-1",
      routeJson: JSON.stringify({ channel: "webchat", to: "original-conversation" }),
    };
    await reserveFollowupRunRecoveryWork(source, steer);
    const reservation = broker.reserve.mock.calls[0]?.[0];
    expect(reservation.scope).toEqual({
      kind: "exact", agentId: source.run.agentId, sessionId: source.run.sessionId,
    });
    broker.steerHandoff.mockRejectedValueOnce(new Error("ack lost"));
    broker.readWork.mockResolvedValueOnce({ steer: { ...steer, outcome: "accepted" } });
    await recordFollowupSteerHandoff(source, "accepted");
    expect(broker.steerHandoff).toHaveBeenCalledWith(reservation.workId, "accepted");
    expect(broker.readWork).toHaveBeenCalledWith(reservation.workId);
    expect(broker.settle).not.toHaveBeenCalled();
    await expect(recordFollowupSteerHandoff(source, "indeterminate"))
      .rejects.toThrow("outcome changed");
  });

  it("fails closed when an indeterminate handoff cannot be read back", async () => {
    const source = run("uncertain source");
    const steer = {
      sourceTurnId: "channel:message-2",
      targetRunId: "active-run-2",
      routeJson: JSON.stringify({ channel: "webchat", to: "original-conversation" }),
    };
    await reserveFollowupRunRecoveryWork(source, steer);
    broker.steerHandoff.mockRejectedValueOnce(new Error("write unknown"));
    await expect(recordFollowupSteerHandoff(source, "indeterminate"))
      .rejects.toThrow("write unknown");
    expect(broker.settle).not.toHaveBeenCalled();
    expect(broker.reserve.mock.calls[0]?.[0].steer).toEqual(steer);
  });

  it("keeps accepted overflow and cleared queue work unresolved", async () => {
    const key = `recovery-overflow-${randomUUID()}`;
    const settings: QueueSettings = { mode: "followup", debounceMs: 0, cap: 1, dropPolicy: "old" };
    const first = run("first accepted input");
    const second = run("second accepted input");
    try {
      expect(await enqueueFollowupWithRecoveryReservation(key, first, settings, "none", undefined, false)).toBe(true);
      expect(await enqueueFollowupWithRecoveryReservation(key, second, settings, "none", undefined, false)).toBe(true);
      expect(getExistingFollowupQueue(key)?.items).toEqual([second]);
      expect(broker.settle).not.toHaveBeenCalled();
      clearFollowupQueue(key);
      expect(broker.settle).not.toHaveBeenCalled();
    } finally {
      clearFollowupQueue(key);
    }
  });

  it("settles only definitive pre-publication abort and dedupe rejections", async () => {
    const key = `recovery-dedupe-${randomUUID()}`;
    const settings: QueueSettings = { mode: "followup", debounceMs: 0, cap: 2, dropPolicy: "new" };
    const first = { ...run("first"), messageId: "message-1" };
    const duplicate = { ...run("duplicate"), messageId: "message-1" };
    const abort = new AbortController();
    abort.abort();
    const cancelled = { ...run("cancelled"), abortSignal: abort.signal };
    try {
      expect(await enqueueFollowupWithRecoveryReservation(key, first, settings, "message-id", undefined, false)).toBe(true);
      expect(await enqueueFollowupWithRecoveryReservation(key, duplicate, settings, "message-id", undefined, false)).toBe(false);
      expect(await enqueueFollowupWithRecoveryReservation(key, cancelled, settings, "none", undefined, false)).toBe(false);
      expect(broker.settle).toHaveBeenCalledTimes(2);
      expect(broker.settle.mock.calls.map((call) => call[0])).not.toContain(broker.reserve.mock.calls[0]?.[0].workId);
    } finally {
      clearFollowupQueue(key);
    }
  });

  it("does not invent terminal custody for cleared, aborted, restarted, or unknown work", async () => {
    const accepted = run("accepted");
    await reserveFollowupRunRecoveryWork(accepted);
    // Clearing an in-memory queue or losing a process-local index is not a receipt.
    const afterRestart = run("preexisting without index");
    await startFollowupRunRecoveryWork(afterRestart);
    await settleFollowupRunRecoveryWork(afterRestart, "not-a-receipt");
    expect(broker.start).not.toHaveBeenCalled();
    expect(broker.settle).not.toHaveBeenCalled();
    expect(broker.reserve).toHaveBeenCalledTimes(1);
  });
});
