import { afterEach, expect, it, vi } from "vitest";
import { createDeferredCore } from "../shared/deferred.js";
import { enqueueCommandInLane, getCommandLaneSnapshot, resetCommandLane } from "./command-queue.js";
import {
  enqueueCommandInLaneWithGeneratedRecoveryCustody,
  enqueueCommandInLaneWithRecoveryCustody,
} from "./command-queue.recovery-custody.js";

const broker = vi.hoisted(() => ({
  reserve: vi.fn(async (_work: { workId: string; scope: { kind: string } }) => "reserved"),
  start: vi.fn(async (_workId: string) => "started"),
  settle: vi.fn(async (_workId: string, _receipt: string) => "settled"),
}));
vi.mock("../state/run-recovery-queue-coordinator-thread-bridge.js", () => ({
  recoveryQueueCoordinatorForCaller: broker,
}));

let sequence = 0;
const next = () => ({ lane: `custody-test-${++sequence}`, workId: `work-${sequence}` });
const custody = (workId: string) => ({ workId, scope: { kind: "unmarked" as const } });
afterEach(() => vi.clearAllMocks());

it("reserves before publishing, starts before effects, and settles a current terminal task", async () => {
  const { lane, workId } = next();
  let published = false;
  broker.reserve.mockImplementationOnce(async () => {
    expect(getCommandLaneSnapshot(lane).queuedCount).toBe(0);
    expect(getCommandLaneSnapshot(lane).activeCount).toBe(0);
    return "reserved";
  });
  const result = await enqueueCommandInLaneWithRecoveryCustody(
    lane,
    async () => {
      expect(broker.start).toHaveBeenCalledWith(workId);
      return "done";
    },
    custody(workId),
    { onQueued: () => { published = true; } },
  );
  expect(result).toBe("done");
  expect(published).toBe(false); // It started immediately, without a queued callback.
  expect(broker.reserve).toHaveBeenCalledWith({ workId, queue: "command-lane", scope: { kind: "unmarked" } });
  expect(broker.settle).toHaveBeenCalledWith(workId, `completed:${workId}`);
  expect(broker.reserve.mock.invocationCallOrder[0]).toBeLessThan(broker.start.mock.invocationCallOrder[0]!);
});

it("settles a queued abort without starting effects", async () => {
  const { lane, workId } = next();
  const gate = createDeferredCore<void>();
  const blocker = enqueueCommandInLane(lane, () => gate.promise);
  const abort = new AbortController();
  const published = createDeferredCore<void>();
  const queued = enqueueCommandInLaneWithRecoveryCustody(
    lane, async () => "unexpected", custody(workId), { abortSignal: abort.signal, onQueued: () => published.resolve(undefined) },
  );
  await published.promise;
  expect(getCommandLaneSnapshot(lane).queuedCount).toBe(1);
  abort.abort();
  await expect(queued).rejects.toThrow();
  expect(broker.start).not.toHaveBeenCalled();
  expect(broker.settle).toHaveBeenCalledWith(workId, `rejected:${workId}`);
  gate.resolve(undefined);
  await blocker;
});

it("leaves failed and reset-generation tasks unresolved for reconciliation", async () => {
  const failed = next();
  await expect(enqueueCommandInLaneWithRecoveryCustody(
    failed.lane, async () => { throw new Error("ambiguous side effect"); }, custody(failed.workId),
  )).rejects.toThrow("ambiguous side effect");
  expect(broker.settle).not.toHaveBeenCalled();

  const reset = next();
  const gate = createDeferredCore<void>();
  const started = createDeferredCore<void>();
  const running = enqueueCommandInLaneWithRecoveryCustody(
    reset.lane, () => { started.resolve(undefined); return gate.promise; }, custody(reset.workId),
  );
  await started.promise;
  expect(getCommandLaneSnapshot(reset.lane).activeCount).toBe(1);
  resetCommandLane(reset.lane);
  gate.resolve(undefined);
  await running;
  expect(broker.settle).not.toHaveBeenCalled();
});

it("does not settle when the queue times out before the task unwinds", async () => {
  const { lane, workId } = next();
  const release = new AbortController();
  const gate = createDeferredCore<void>();
  const started = createDeferredCore<void>();
  const running = enqueueCommandInLaneWithRecoveryCustody(
    lane,
    async () => { started.resolve(undefined); await gate.promise; return "late"; },
    custody(workId),
    { taskTimeoutMs: 60_000, taskTimeoutReleaseSignal: release.signal },
  );
  await started.promise;
  release.abort();
  await expect(running).rejects.toThrow(/timed out/);
  gate.resolve(undefined);
  await Promise.resolve();
  expect(broker.settle).not.toHaveBeenCalled();
});

it("never publishes after an ambiguous reserve result", async () => {
  const { lane, workId } = next();
  broker.reserve.mockRejectedValueOnce(new Error("broker acknowledgement lost"));
  await expect(enqueueCommandInLaneWithRecoveryCustody(
    lane, async () => "unexpected", custody(workId),
  )).rejects.toThrow("broker acknowledgement lost");
  expect(getCommandLaneSnapshot(lane).activeCount).toBe(0);
  expect(getCommandLaneSnapshot(lane).queuedCount).toBe(0);
  expect(broker.start).not.toHaveBeenCalled();
  expect(broker.settle).not.toHaveBeenCalled();
});

it("gives separate unmarked custody to otherwise unidentified production tasks", async () => {
  const lane = `generated-custody-${++sequence}`;
  const first = await enqueueCommandInLaneWithGeneratedRecoveryCustody(lane, async () => "first");
  const second = await enqueueCommandInLaneWithGeneratedRecoveryCustody(lane, async () => "second");
  expect([first, second]).toEqual(["first", "second"]);
  const workIds = broker.reserve.mock.calls.map(([work]) => work.workId as string);
  expect(workIds).toHaveLength(2);
  expect(workIds[0]).not.toBe(workIds[1]);
  expect(workIds.every((id) => id.startsWith("command-lane:"))).toBe(true);
  expect(broker.reserve.mock.calls.map(([work]) => work.scope)).toEqual([
    { kind: "unmarked" },
    { kind: "unmarked" },
  ]);
  expect(broker.start.mock.calls.map(([workId]) => workId)).toEqual(workIds);
  expect(broker.settle.mock.calls.map(([workId]) => workId)).toEqual(workIds);
});
