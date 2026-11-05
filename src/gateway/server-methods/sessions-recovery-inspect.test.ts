import { beforeEach, describe, expect, it, vi } from "vitest";

const fixtures = vi.hoisted(() => {
  const entry = { sessionId: "s1", lifecycleRevision: "r1", updatedAt: 1,
    lifecycleRunId: "run-1", pluginExtensions: { secret: { value: "private-content" } } };
  return {
    entry,
    resolvedAgentId: "main",
    visible: true,
    target: { agentId: "main", canonicalKey: "agent:main:main", storeKey: "agent:main:main",
      storePath: "/test/store", entry },
    full: { agentId: "main", canonicalKey: "agent:main:main",
      storePath: "/test/store", store: { "agent:main:main": entry } },
  };
});
vi.mock("../session-request-agent.js", () => ({
  resolveRequestedSessionAgentId: () => ({ ok: true, agentId: fixtures.resolvedAgentId }),
}));
vi.mock("../session-sharing.js", () => ({
  resolveSessionSharingTarget: () => fixtures.target,
  authorizeIncognitoSessionTarget: () => null,
  createSessionListEntryFilter: () => () => fixtures.visible,
}));
vi.mock("../session-utils-store-lookup.js", () => ({
  resolveGatewaySessionStoreTargetWithStore: () => fixtures.full,
}));

import { sessionRecoveryInspectHandlers } from "./sessions-recovery-inspect.js";

function invoke(params: Record<string, unknown>) {
  const respond = vi.fn();
  sessionRecoveryInspectHandlers["sessions.recovery.inspect"]!({
    params, respond, client: null,
    context: { getRuntimeConfig: () => ({}) },
  } as never);
  return respond;
}

beforeEach(() => {
  fixtures.resolvedAgentId = "main";
  fixtures.visible = true;
  fixtures.full.store["agent:main:main"] = fixtures.entry;
});

describe("sessions.recovery.inspect handler", () => {
  const params = { agentId: "main", key: "agent:main:main", sessionId: "s1" };
  it("rejects an agent/key mismatch before reading metadata", () => {
    fixtures.resolvedAgentId = "other";
    expect(invoke(params).mock.calls[0]?.[0]).toBe(false);
  });
  it("hides invisible sessions", () => {
    fixtures.visible = false;
    const response = invoke(params);
    expect(response.mock.calls[0]?.[0]).toBe(false);
    expect(JSON.stringify(response.mock.calls)).not.toContain("private-content");
  });
  it("rejects changed generation and exact run mismatch", () => {
    fixtures.full.store["agent:main:main"] = { ...fixtures.entry, sessionId: "s2" };
    expect(invoke(params).mock.calls[0]?.[0]).toBe(false);
    fixtures.full.store["agent:main:main"] = { ...fixtures.entry,
      lifecycleRevision: "replacement-revision" };
    expect(invoke(params).mock.calls[0]?.[0]).toBe(false);
    fixtures.full.store["agent:main:main"] = fixtures.entry;
    expect(invoke({ ...params, runId: "different" }).mock.calls[0]?.[0]).toBe(false);
  });
  it("returns only bounded metadata", () => {
    const response = invoke(params);
    expect(response.mock.calls[0]?.[0]).toBe(true);
    expect(JSON.stringify(response.mock.calls)).not.toContain("private-content");
    expect(response.mock.calls[0]?.[1]).toMatchObject({ evidence: {
      descendant: { complete: false, state: "unknown" },
    } });
  });
});
