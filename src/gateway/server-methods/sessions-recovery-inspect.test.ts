import { beforeEach, describe, expect, it, vi } from "vitest";

const fixtures = vi.hoisted(() => {
  const entry = { sessionId: "s1", lifecycleRevision: "r1", updatedAt: 1,
    lifecycleRunId: "run-1", pluginExtensions: { secret: { value: "private-content" } } };
  return {
    entry,
    resolvedAgentId: "main",
    visible: true,
    pendingRead: vi.fn(async (_scope: unknown, _options: unknown) => [] as Array<{ runId: string; state: "pending" }>),
    target: { agentId: "main", canonicalKey: "agent:main:main", storeKey: "agent:main:main",
      storePath: "/test/store", entry },
    full: { agentId: "main", canonicalKey: "agent:main:main",
      storePath: "/test/store", store: { "agent:main:main": entry } },
  };
});
vi.mock("../../state/openclaw-state-db-readonly.js", () => ({
  executeExistingOpenClawStateRead: vi.fn(async () => undefined),
}));
vi.mock("../../config/sessions/session-pending-input-receipts.js", () => ({
  readSessionPendingInputReceiptsInWorker: (scope: unknown, options: unknown) =>
    fixtures.pendingRead(scope, options),
}));
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

async function invoke(params: Record<string, unknown>) {
  const respond = vi.fn();
  await sessionRecoveryInspectHandlers["sessions.recovery.inspect"]!({
    params, respond, client: null,
    context: { getRuntimeConfig: () => ({}) },
  } as never);
  return respond;
}

beforeEach(() => {
  fixtures.resolvedAgentId = "main";
  fixtures.visible = true;
  fixtures.pendingRead.mockReset();
  fixtures.pendingRead.mockResolvedValue([]);
  fixtures.full.store["agent:main:main"] = fixtures.entry;
});

describe("sessions.recovery.inspect handler", () => {
  const params = { agentId: "main", key: "agent:main:main", sessionId: "s1" };
  it("rejects an agent/key mismatch before reading metadata", async () => {
    fixtures.resolvedAgentId = "other";
    expect((await invoke(params)).mock.calls[0]?.[0]).toBe(false);
  });
  it("hides invisible sessions", async () => {
    fixtures.visible = false;
    const response = await invoke(params);
    expect(response.mock.calls[0]?.[0]).toBe(false);
    expect(JSON.stringify(response.mock.calls)).not.toContain("private-content");
  });
  it("rejects changed generation and exact run mismatch", async () => {
    fixtures.full.store["agent:main:main"] = { ...fixtures.entry, sessionId: "s2" };
    expect((await invoke(params)).mock.calls[0]?.[0]).toBe(false);
    fixtures.full.store["agent:main:main"] = { ...fixtures.entry,
      lifecycleRevision: "replacement-revision" };
    expect((await invoke(params)).mock.calls[0]?.[0]).toBe(false);
    fixtures.full.store["agent:main:main"] = fixtures.entry;
    expect((await invoke({ ...params, runId: "different" })).mock.calls[0]?.[0]).toBe(false);
  });
  it("rechecks the session after a pending receipt read changes its generation", async () => {
    fixtures.pendingRead.mockImplementationOnce(async () => {
      fixtures.full.store["agent:main:main"] = { ...fixtures.entry,
        lifecycleRevision: "next-revision" };
      return [{ runId: "run-1", state: "pending" }];
    });
    const response = await invoke(params);
    expect(response.mock.calls[0]?.[0]).toBe(false);
  });
  it("reports only exact pending custody and fails closed on a storage error", async () => {
    fixtures.pendingRead.mockResolvedValueOnce([{ runId: "run-1", state: "pending" }]);
    expect((await invoke(params)).mock.calls[0]?.[1]).toMatchObject({ evidence: {
      pendingInput: { complete: true, state: "active" },
      terminal: { complete: false, state: "unknown" },
    } });
    fixtures.pendingRead.mockRejectedValueOnce(new Error("unavailable"));
    expect((await invoke(params)).mock.calls[0]?.[1]).toMatchObject({ evidence: {
      pendingInput: { complete: false, state: "unknown" },
    } });
  });
  it("returns only bounded metadata", async () => {
    const response = await invoke(params);
    expect(response.mock.calls[0]?.[0]).toBe(true);
    expect(JSON.stringify(response.mock.calls)).not.toContain("private-content");
    expect(response.mock.calls[0]?.[1]).toMatchObject({ evidence: {
      descendant: { complete: false, state: "unknown" },
    } });
  });
});
