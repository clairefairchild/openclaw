import { afterEach, describe, expect, it } from "vitest";
import { claimAgentRunContext, readAgentRunRecoveryOwnerSnapshot,
  releaseAgentRunContext, resetAgentRunRegistryForTest } from "./agent-run-registry.js";

const target = { agentId: "main", sessionKey: "agent:main:main", sessionId: "s1" };
const read = () => readAgentRunRecoveryOwnerSnapshot(target);
afterEach(() => resetAgentRunRegistryForTest());

describe("run recovery owner snapshot", () => {
  it("captures an immutable exact owner generation across owner replacement", () => {
    const firstClaim = claimAgentRunContext("run-1", target, { trackOwner: true,
      ownsContext: true });
    expect(firstClaim).toBeDefined();
    const first = read();
    expect(first.activeRunIds).toEqual(["run-1"]);
    expect(first.exactOwners).toHaveLength(1);
    releaseAgentRunContext("run-1", firstClaim);
    const secondClaim = claimAgentRunContext("run-1", target, { trackOwner: true,
      ownsContext: true });
    const second = read();
    expect(second.exactOwners[0]?.ownerEpoch).toBeGreaterThan(first.exactOwners[0]!.ownerEpoch);
    expect(first.exactOwners[0]?.ownerEpoch).not.toBe(second.exactOwners[0]?.ownerEpoch);
    releaseAgentRunContext("run-1", secondClaim);
  });
  it("reports a live same-process stale-looking run and never proves terminal", () => {
    const claim = claimAgentRunContext("run-1", { ...target, registeredAt: 1,
      lastActiveAt: 1 }, { trackOwner: true, ownsContext: true });
    expect(read().activeRunIds).toEqual(["run-1"]);
    releaseAgentRunContext("run-1", claim);
    expect(read().activeRunIds).toEqual([]);
  });
  it("does not project another agent or session onto this target", () => {
    const otherAgent = claimAgentRunContext("other-agent", { ...target, agentId: "other" },
      { trackOwner: true, ownsContext: true });
    const otherSession = claimAgentRunContext("other-session", { ...target, sessionId: "s2" },
      { trackOwner: true, ownsContext: true });
    expect(read().activeRunIds).toEqual([]);
    releaseAgentRunContext("other-agent", otherAgent);
    releaseAgentRunContext("other-session", otherSession);
  });
});
