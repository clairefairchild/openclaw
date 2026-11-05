import { describe, expect, it } from "vitest";
import {
  validateSessionsRecoveryInspectParams,
  validateSessionsRecoveryInspectResult,
} from "../index.js";

describe("sessions.recovery.inspect protocol", () => {
  it("requires an explicit agent and rejects message/content parameters", () => {
    expect(validateSessionsRecoveryInspectParams({ agentId: "main", key: "global" })).toBe(true);
    expect(validateSessionsRecoveryInspectParams({ key: "global" })).toBe(false);
    expect(validateSessionsRecoveryInspectParams({ agentId: "main", key: "global", message: "hi" })).toBe(false);
  });
  it("accepts unknown custody but never transcript content", () => {
    const unknown = { complete: false, state: "unknown" };
    const evidence = Object.fromEntries([
      "direct", "pendingAdmission", "followupQueue", "laneQueue", "pendingInput",
      "descendant", "process", "worker", "terminal", "sourceRoute",
      "inputLineage", "delivery", "progress",
    ].map((name) => [name, unknown]));
    const result = { agentId: "main", key: "global", sessionId: "s1", evidence,
      metadataCompleteness: { identity: true, execution: false, store: false,
        owner: false, placement: false, route: false } };
    expect(validateSessionsRecoveryInspectResult(result)).toBe(true);
    expect(validateSessionsRecoveryInspectResult({ ...result, transcript: "secret" })).toBe(false);
    expect(validateSessionsRecoveryInspectResult({ ...result, evidence: {
      ...evidence, direct: { complete: false, state: "clear" },
    } })).toBe(false);
  });
});
