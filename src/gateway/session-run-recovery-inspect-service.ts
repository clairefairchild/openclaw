import type { SessionsRecoveryInspectResult } from "../../packages/gateway-protocol/src/schema/sessions-recovery-inspect.js";
import type { InternalSessionEntry, SessionEntry } from "../config/sessions.js";
import type { GatewayRequestContext } from "./server-methods/shared-types.js";
import type { WorkerSessionPlacementRecord } from "./worker-environments/placement-record.js";
import { readAgentRunRecoveryOwnerSnapshot } from "../infra/agent-run-registry.js";
import { readSessionRecoveryAdmissionSnapshot } from "../sessions/session-lifecycle-admission.js";

const UNKNOWN = { complete: false, state: "unknown" } as const;
const ACTIVE = { complete: true, state: "active" } as const;

/**
 * Metadata-only read. Each process-local family is captured by its own owner
 * synchronously, without an await between the session row and custody reads.
 * Absence from a process-local index never proves terminal or global quiescence.
 */
export function inspectGatewayRunRecovery(params: {
  agentId: string;
  key: string;
  storePath: string;
  entry: SessionEntry & Partial<Pick<InternalSessionEntry, "lifecycleRunId" | "lastRunId" | "mainRestartRecovery">>;
  placement?: WorkerSessionPlacementRecord;
  chatAbortControllers?: GatewayRequestContext["chatAbortControllers"];
  chatQueuedTurns?: GatewayRequestContext["chatQueuedTurns"];
  readRunOwners?: typeof readAgentRunRecoveryOwnerSnapshot;
  readAdmissions?: typeof readSessionRecoveryAdmissionSnapshot;
}): SessionsRecoveryInspectResult {
  const { agentId, key, entry } = params;
  const observedRunId = entry.lifecycleRunId || entry.lastRunId;
  // Restart recovery persists its admission token only after binding it to
  // this session's fenced run. Ordinary audit contexts lack a session binding.
  const recoveryToken = entry.mainRestartRecovery?.executionIdentity;
  const exactExecution = observedRunId && recoveryToken?.runId === observedRunId &&
    entry.restartRecoveryRuns?.some((run) => run.runId === observedRunId) &&
    recoveryToken.executionId && recoveryToken.contextId
    ? recoveryToken.executionId : undefined;
  // A placement row belongs to its exact agent/key/session generation. An
  // inconsistent turn claim cannot establish the current generation.
  const placement = params.placement;
  const exactPlacement = placement?.sessionId === entry.sessionId &&
    placement.agentId === agentId && placement.sessionKey === key &&
    Number.isSafeInteger(placement.generation) && placement.generation >= 0 &&
    (!placement.turnClaim || placement.turnClaim.generation === placement.generation)
    ? placement : undefined;
  const owners = (params.readRunOwners ?? readAgentRunRecoveryOwnerSnapshot)({
    agentId, sessionKey: key, sessionId: entry.sessionId,
  });
  const admissions = (params.readAdmissions ?? readSessionRecoveryAdmissionSnapshot)({
    scope: params.storePath, sessionKey: key, sessionId: entry.sessionId,
  });
  // These Gateway maps are owner-owned and sampled in this same synchronous
  // call. They prove presence only, never the absence of a different owner.
  const matches = (candidate: { agentId?: string; sessionKey: string; sessionId: string }) =>
    candidate.agentId === agentId && candidate.sessionKey === key &&
    candidate.sessionId === entry.sessionId;
  const directControllerRunIds = [...(params.chatAbortControllers?.entries() ?? [])]
    .filter(([, controller]) => matches(controller)).map(([runId]) => runId);
  const directControllerActive = directControllerRunIds.length > 0;
  const queuedTurnActive = [...(params.chatQueuedTurns?.values() ?? [])]
    .some(matches);
  const exactOwner = owners.consistent && observedRunId && owners.exactOwners.length === 1 &&
    owners.exactOwners[0]?.runId === observedRunId &&
    owners.activeRunIds.length === 1 && owners.activeRunIds[0] === observedRunId &&
    directControllerRunIds.every((runId) => runId === observedRunId)
    ? owners.exactOwners[0] : undefined;
  return {
    agentId,
    key,
    sessionId: entry.sessionId,
    ...(entry.lifecycleRevision ? { lifecycleRevision: entry.lifecycleRevision } : {}),
    ...(observedRunId ? { runId: observedRunId } : {}),
    ...(exactExecution ? { executionId: exactExecution } : {}),
    ...(exactPlacement ? { placementGeneration: exactPlacement.generation } : {}),
    ...(exactOwner ? { ownerEpoch: exactOwner.ownerEpoch,
      ownerLifecycleGeneration: owners.lifecycleGeneration } : {}),
    ...(entry.status ? { observedSessionStatus: entry.status } : {}),
    metadataCompleteness: {
      identity: true,
      execution: Boolean(exactExecution),
      store: false,
      owner: Boolean(exactOwner),
      placement: Boolean(exactPlacement),
      route: false,
    },
    evidence: {
      direct: (owners.consistent && owners.activeRunIds.length > 0) ||
        directControllerActive ? ACTIVE : UNKNOWN,
      pendingAdmission: admissions.pending || admissions.active ? ACTIVE : UNKNOWN,
      followupQueue: queuedTurnActive ? ACTIVE : UNKNOWN,
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
    // A live owner can never receive a recovery token. The remaining families
    // also cannot prove absence or stable input/source generations yet.
  };
}
