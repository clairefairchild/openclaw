import { describe, expect, it } from "vitest";
import type { InternalSessionEntry } from "../config/sessions.js";
import { inspectGatewayRunRecovery } from "./session-run-recovery-inspect-service.js";

const entry = {
  sessionId: "s1",
  updatedAt: 1,
  lifecycleRevision: "revision-1",
  lifecycleRunId: "run-1",
  status: "running",
  // Content-bearing fields must never be spread into the RPC result.
  pluginExtensions: { private: { payload: { secret: "do-not-leak" } } },
} as unknown as InternalSessionEntry;

function inspect(overrides: Partial<Parameters<typeof inspectGatewayRunRecovery>[0]> = {}) {
  return inspectGatewayRunRecovery({
    agentId: "main", key: "agent:main:main", entry,
    listRuns: () => [], getRunContext: () => undefined, hasExecutionOwner: () => false,
    ...overrides,
  });
}

describe("read-only run recovery inspection", () => {
  it("keeps all absent custody and terminal facts unknown, even for an old running row", () => {
    const result = inspect();
    expect(result.runId).toBe("run-1");
    expect(result.evidence.direct).toEqual({ complete: false, state: "unknown" });
    expect(Object.values(result.evidence).every((family) => family.state === "unknown")).toBe(true);
    expect(result.reviewToken).toBeUndefined();
    expect(JSON.stringify(result)).not.toContain("do-not-leak");
  });
  it("reports an exact live owner as active without inventing an owner epoch", () => {
    const result = inspect({
      listRuns: () => [{ runId: "run-1", lifecycleGeneration: "generation-1" }],
      getRunContext: () => ({ sessionKey: "agent:main:main", sessionId: "s1",
        agentId: "main" }),
      hasExecutionOwner: () => true,
    });
    expect(result.evidence.direct).toEqual({ complete: true, state: "active" });
    expect(result.ownerEpoch).toBeUndefined();
  });
  it("rejects wrong agent or changed session generation as owner evidence", () => {
    for (const context of [
      { sessionKey: "agent:main:main", sessionId: "s1", agentId: "other" },
      { sessionKey: "agent:main:main", sessionId: "replacement", agentId: "main" },
    ]) {
      expect(inspect({
        listRuns: () => [{ runId: "run-1", lifecycleGeneration: "generation-1" }],
        getRunContext: () => context,
        hasExecutionOwner: () => true,
      }).evidence.direct.state).toBe("unknown");
    }
  });
});
