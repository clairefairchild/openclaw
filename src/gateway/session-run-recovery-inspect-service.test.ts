import { describe, expect, it } from "vitest";
import type { InternalSessionEntry } from "../config/sessions.js";
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
  it("vetoes pending and acquired admissions before run registration", () => {
    for (const snapshot of [{ active: false, pending: true },
      { active: true, pending: false }]) {
      expect(inspect({ readAdmissions: () => snapshot }).evidence.pendingAdmission)
        .toEqual({ complete: true, state: "active" });
    }
  });
});
