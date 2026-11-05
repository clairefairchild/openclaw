import type { SessionsRecoveryInspectResult } from "../../packages/gateway-protocol/src/schema/sessions-recovery-inspect.js";
import type { InternalSessionEntry, SessionEntry } from "../config/sessions.js";
import {
  getAgentRunContext,
  hasAgentRunContextExecutionOwner,
  listAgentRunsForSession,
} from "../infra/agent-run-registry.js";

const UNKNOWN = { complete: false, state: "unknown" } as const;
const ACTIVE = { complete: true, state: "active" } as const;

/** Read-only, deliberately incomplete evidence. A missing registry owner is not proof of quiescence. */
export function inspectGatewayRunRecovery(params: {
  agentId: string;
  key: string;
  entry: SessionEntry & Partial<Pick<InternalSessionEntry, "lifecycleRunId" | "lastRunId">>;
  listRuns?: typeof listAgentRunsForSession;
  getRunContext?: typeof getAgentRunContext;
  hasExecutionOwner?: typeof hasAgentRunContextExecutionOwner;
}): SessionsRecoveryInspectResult {
  const { agentId, key, entry } = params;
  const listRuns = params.listRuns ?? listAgentRunsForSession;
  const getRunContext = params.getRunContext ?? getAgentRunContext;
  const hasExecutionOwner = params.hasExecutionOwner ?? hasAgentRunContextExecutionOwner;
  const observedRunId = entry.lifecycleRunId || entry.lastRunId;
  const registered = listRuns({ sessionKey: key, sessionId: entry.sessionId });
  const matchingOwners = registered.filter(({ runId }) => {
    const context = getRunContext(runId);
    return context?.sessionKey === key &&
      context.sessionId === entry.sessionId &&
      context.agentId === agentId &&
      hasExecutionOwner(runId);
  });
  // A live owner of another run in this exact session is still a direct veto.
  const direct = matchingOwners.length > 0 ? ACTIVE : UNKNOWN;
  // lifecycleRunId and lastRunId are observations, not an execution identity or a
  // terminal receipt. Missing queue/child/worker records cannot be called empty.
  return {
    agentId,
    key,
    sessionId: entry.sessionId,
    ...(entry.lifecycleRevision ? { lifecycleRevision: entry.lifecycleRevision } : {}),
    ...(observedRunId ? { runId: observedRunId } : {}),
    ...(entry.status ? { observedSessionStatus: entry.status } : {}),
    metadataCompleteness: {
      identity: true,
      execution: false,
      store: false,
      owner: false,
      placement: false,
      route: false,
    },
    evidence: {
      direct,
      pendingAdmission: UNKNOWN,
      followupQueue: UNKNOWN,
      laneQueue: UNKNOWN,
      pendingInput: UNKNOWN,
      descendant: UNKNOWN,
      process: UNKNOWN,
      worker: UNKNOWN,
      terminal: UNKNOWN,
      sourceRoute: UNKNOWN,
      inputLineage: UNKNOWN,
      delivery: UNKNOWN,
      progress: UNKNOWN,
    },
    // No review token can be issued until exact execution, owner, placement,
    // input, and route generations have authoritative Gateway owners.
  };
}
