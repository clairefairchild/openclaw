import { describe, expect, it } from "vitest";
import type { InternalSessionEntry } from "../config/sessions.js";
import type { WorkerSessionPlacementRecord } from "./worker-environments/placement-record.js";
import { inspectGatewayRunRecovery } from "./session-run-recovery-inspect-service.js";

const entry = {
  sessionId: "s1",
  updatedAt: 1,
  lifecycleRevision: "revision-1",
  lifecycleRunId: "run-1",
  status: "running",
  pluginExtensions: { private: { payload: { secret: "do-not-leak" } } },
} as unknown as InternalSessionEntry;

const noOwners = () => ({ registryVersion: 1, lifecycleGeneration: "gateway-1", consistent: true,
  activeRunIds: [], exactOwners: [] });
const noAdmissions = () => ({ active: false, pending: false });
function inspect(overrides: Partial<Parameters<typeof inspectGatewayRunRecovery>[0]> = {}) {
  return inspectGatewayRunRecovery({
    agentId: "main", key: "agent:main:main", storePath: "/store", entry,
    readRunOwners: noOwners, readAdmissions: noAdmissions, ...overrides,
  });
}

describe("read-only run recovery inspection", () => {
  it("keeps missing run and old running status unknown, with no token or transcript", () => {
    for (const selected of [entry, { ...entry, lifecycleRunId: undefined, lastRunId: undefined }]) {
      const result = inspect({ entry: selected });
      expect(Object.values(result.evidence).every((family) => family.state === "unknown")).toBe(true);
      expect(result.reviewToken).toBeUndefined();
      expect(result.executionId).toBeUndefined();
      expect(JSON.stringify(result)).not.toContain("do-not-leak");
    }
  });
  it("binds a fenced restart-recovery execution to the exact observed session run", () => {
    const bound = { ...entry, restartRecoveryRuns: [{ runId: "run-1",
      lifecycleGeneration: "gateway-1" }], mainRestartRecovery: {
      cycleId: "cycle-1", revision: 1, chargedAttempts: 1,
      executionIdentity: { tokenVersion: 1, contextId: "context-1",
        executionId: "execution-1", runId: "run-1", createdAt: 1 },
    } } as InternalSessionEntry;
    expect(inspect({ entry: bound })).toMatchObject({ executionId: "execution-1",
      metadataCompleteness: { execution: true } });
    expect(inspect({ entry: { ...bound, lifecycleRunId: "run-2" } })).toMatchObject({
      metadataCompleteness: { execution: false },
    });
    expect(inspect({ entry: { ...bound, restartRecoveryRuns: [] } }).executionId).toBeUndefined();
  });
  it("accepts terminal proof only for exact durable execution and owner lineage", () => {
    const token = { tokenVersion: 1 as const, contextId: "context-1",
      executionId: "execution-1", runId: "run-1", createdAt: 1 };
    const bound = { ...entry, lifecycleRunId: undefined, lastRunId: "run-1",
      status: "done" as const, restartRecoveryRuns: [{ runId: "run-1",
        lifecycleGeneration: "gateway-1" }], mainRestartRecovery: {
        cycleId: "cycle-1", revision: 1, chargedAttempts: 1, executionIdentity: token,
      }, runRecoveryTerminalReceipt: { sessionId: "s1", runId: "run-1",
        lifecycleRevision: "revision-1", lifecycleGeneration: "gateway-1",
        executionId: "execution-1", ownerEpoch: 7, status: "done" as const,
        endedAt: 2_000 } } as InternalSessionEntry;
    expect(inspect({ entry: bound }).evidence.terminal)
      .toEqual({ complete: true, state: "clear" });
    for (const changed of [
      { ...bound, status: "running" as const },
      { ...bound, lastRunId: "run-2" },
      { ...bound, sessionId: "s2" },
      { ...bound, lifecycleRevision: "revision-2" },
      { ...bound, restartRecoveryRuns: [] },
      { ...bound, restartRecoveryRuns: [{ runId: "run-1",
        lifecycleGeneration: "gateway-2" }] },
      { ...bound, runRecoveryTerminalReceipt: {
        ...bound.runRecoveryTerminalReceipt!, executionId: "wrong" } },
      { ...bound, runRecoveryTerminalReceipt: {
        ...bound.runRecoveryTerminalReceipt!, ownerEpoch: undefined } },
      { ...bound, runRecoveryTerminalReceipt: undefined },
    ]) {
      expect(inspect({ entry: changed }).evidence.terminal)
        .toEqual({ complete: false, state: "unknown" });
    }
    expect(inspect({ entry: bound, readRunOwners: () => ({ ...noOwners(),
      activeRunIds: ["run-1"] }) }).evidence.terminal.state).toBe("unknown");
    expect(inspect({ entry: bound, readRunOwners: () => ({ ...noOwners(),
      consistent: false }) }).evidence.terminal.state).toBe("unknown");
    expect(inspect({ entry: bound }).evidence.sourceRoute.state).toBe("unknown");
    expect(inspect({ entry: bound }).evidence.inputLineage.state).toBe("unknown");
  });
  it("accepts an exact owner-claim receipt, but never promotes a changed session or run", () => {
    const bound = { ...entry, lifecycleRunId: undefined, lastRunId: "run-1",
      status: "done" as const, runRecoveryTerminalReceipt: {
        sessionId: "s1", runId: "run-1", lifecycleRevision: "revision-1",
        lifecycleGeneration: "gateway-1", executionId: "claim-1",
        executionSource: "owner-claim" as const, ownerEpoch: 7,
        status: "done" as const, endedAt: 2_000,
      } } as InternalSessionEntry;
    expect(inspect({ entry: bound })).toMatchObject({ executionId: "claim-1",
      metadataCompleteness: { execution: true },
      evidence: { terminal: { complete: true, state: "clear" },
        inputLineage: { complete: false, state: "unknown" },
        sourceRoute: { complete: false, state: "unknown" } } });
    for (const changed of [
      { ...bound, sessionId: "s2" },
      { ...bound, lastRunId: "run-2" },
      { ...bound, lifecycleRevision: "revision-2" },
      { ...bound, runRecoveryTerminalReceipt: { ...bound.runRecoveryTerminalReceipt!,
        executionSource: undefined } },
      { ...bound, runRecoveryTerminalReceipt: { ...bound.runRecoveryTerminalReceipt!,
        ownerEpoch: undefined } },
    ]) {
      const result = inspect({ entry: changed });
      expect(result.executionId).toBeUndefined();
      expect(result.evidence.terminal).toEqual({ complete: false, state: "unknown" });
    }
    expect(inspect({ entry: bound, readRunOwners: () => ({ ...noOwners(),
      activeRunIds: ["run-1"] }) }).evidence.terminal.state).toBe("unknown");
  });
  it("reports only a matching owner-maintained placement generation", () => {
    const placement = { agentId: "main", sessionKey: "agent:main:main",
      sessionId: "s1", generation: 8, turnClaim: null } as WorkerSessionPlacementRecord;
    expect(inspect({ placement })).toMatchObject({ placementGeneration: 8,
      metadataCompleteness: { placement: true } });
    for (const mismatch of [{ ...placement, agentId: "other" },
      { ...placement, sessionKey: "agent:other:main" },
      { ...placement, sessionId: "other-session" },
      { ...placement, turnClaim: { generation: 7 } }]) {
      const result = inspect({ placement: mismatch as WorkerSessionPlacementRecord });
      expect(result.placementGeneration).toBeUndefined();
      expect(result.metadataCompleteness.placement).toBe(false);
    }
    // A new owner generation replaces the old value; no stale generation is cached.
    expect(inspect({ placement: { ...placement, generation: 9 } }).placementGeneration).toBe(9);
  });
  it("reports only the exact observed run owner epoch", () => {
    const result = inspect({ readRunOwners: () => ({ registryVersion: 2,
      lifecycleGeneration: "gateway-1", consistent: true, activeRunIds: ["run-1"],
      exactOwners: [{ runId: "run-1", ownerEpoch: 7 }] }) });
    expect(result).toMatchObject({ runId: "run-1", ownerEpoch: 7,
      metadataCompleteness: { owner: true },
      evidence: { direct: { complete: true, state: "active" } } });
    expect(result.ownerLifecycleGeneration).toBe("gateway-1");
    expect(result.reviewToken).toBeUndefined();
  });
  it("vetoes a live other run without attributing its epoch to the selected run", () => {
    const result = inspect({ readRunOwners: () => ({ registryVersion: 3,
      lifecycleGeneration: "gateway-1", consistent: true, activeRunIds: ["run-2"],
      exactOwners: [{ runId: "run-2", ownerEpoch: 8 }] }) });
    expect(result.evidence.direct.state).toBe("active");
    expect(result.ownerEpoch).toBeUndefined();
    expect(result.metadataCompleteness.owner).toBe(false);
  });
  it("vetoes exact controller and followup queue custody without leaking contents", () => {
    const registration = { agentId: "main", sessionKey: "agent:main:main",
      sessionId: "s1", controller: new AbortController(), startedAtMs: 1, expiresAtMs: 2 };
    const result = inspect({
      chatAbortControllers: new Map([["run-1", registration]]),
      chatQueuedTurns: new Map([["run-2", { ...registration, privateText: "queue-secret" }]]),
    });
    expect(result.evidence.direct).toEqual({ complete: true, state: "active" });
    expect(result.evidence.followupQueue).toEqual({ complete: true, state: "active" });
    expect(JSON.stringify(result)).not.toContain("queue-secret");
  });
  it("ignores other-agent controller and queue entries", () => {
    const registration = { agentId: "other", sessionKey: "agent:main:main",
      sessionId: "s1", controller: new AbortController(), startedAtMs: 1, expiresAtMs: 2 };
    const result = inspect({
      chatAbortControllers: new Map([["run-other", registration]]),
      chatQueuedTurns: new Map([["run-other", registration]]),
    });
    expect(result.evidence.direct.state).toBe("unknown");
    expect(result.evidence.followupQueue.state).toBe("unknown");
  });
  it("does not trust a snapshot whose owner generation changed during read", () => {
    const result = inspect({ readRunOwners: () => ({ registryVersion: 2,
      lifecycleGeneration: "gateway-1", consistent: false, activeRunIds: ["run-1"],
      exactOwners: [{ runId: "run-1", ownerEpoch: 7 }] }) });
    expect(result.ownerEpoch).toBeUndefined();
    expect(result.evidence.direct).toEqual({ complete: false, state: "unknown" });
    expect(result.reviewToken).toBeUndefined();
  });
  it("reports only an exact durable pending input as active custody", () => {
    const result = inspect({ pendingInputReceipts: [
      { runId: "run-1", state: "pending" },
    ] });
    expect(result.evidence.pendingInput).toEqual({ complete: true, state: "active" });
    expect(result.evidence.terminal).toEqual({ complete: false, state: "unknown" });
    expect(result.evidence.inputLineage).toEqual({ complete: false, state: "unknown" });
    expect(result.originalInputReceiptId).toBeUndefined();
    expect(result.storeRevision).toBeUndefined();
  });
  it("does not attribute a different run, cancelled, or consumed input to this run", () => {
    for (const receipt of [
      { runId: "run-2", state: "pending" as const },
      { runId: "run-1", state: "pending" as const, cancelled: true as const },
      { runId: "run-1", state: "consumed" as const, consumedByEventId: "event-1" },
    ]) {
      expect(inspect({ pendingInputReceipts: [receipt] }).evidence.pendingInput)
        .toEqual({ complete: false, state: "unknown" });
    }
  });
  it("does not reuse a prior receipt across a changed run or missing read", () => {
    const receipt = [{ runId: "run-1", state: "pending" as const }];
    expect(inspect({ pendingInputReceipts: receipt }).evidence.pendingInput.state).toBe("active");
    expect(inspect({ entry: { ...entry, lifecycleRunId: "run-2" },
      pendingInputReceipts: receipt }).evidence.pendingInput.state).toBe("unknown");
    expect(inspect({ pendingInputReceipts: undefined }).evidence.pendingInput.state).toBe("unknown");
  });
  it("vetoes pending and acquired admissions before run registration", () => {
    for (const snapshot of [{ active: false, pending: true },
      { active: true, pending: false }]) {
      expect(inspect({ readAdmissions: () => snapshot }).evidence.pendingAdmission)
        .toEqual({ complete: true, state: "active" });
    }
  });
});
