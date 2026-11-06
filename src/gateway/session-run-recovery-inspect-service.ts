import type { SessionsRecoveryInspectResult } from "../../packages/gateway-protocol/src/schema/sessions-recovery-inspect.js";
import type { InternalSessionEntry, SessionEntry } from "../config/sessions.js";
import type { GatewayRequestContext } from "./server-methods/shared-types.js";
import type { WorkerSessionPlacementRecord } from "./worker-environments/placement-record.js";
import { readAgentRunRecoveryOwnerSnapshot } from "../infra/agent-run-registry.js";
import { readSessionRecoveryAdmissionSnapshot } from "../sessions/session-lifecycle-admission.js";
import { fingerprintRunRecoverySourceRoute } from "../config/sessions/run-recovery-source-route.js";
import { hasQueuedCommandForRecoverySession } from "../process/command-queue.state.js";
import { hasTrackedProcessForRecoverySession } from "../agents/bash-process-registry.js";

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
  entry: SessionEntry & Partial<Pick<InternalSessionEntry, "lifecycleRunId" | "lastRunId" | "mainRestartRecovery" | "runRecoveryTerminalReceipt" | "runRecoveryOriginalInputReceipt" | "runRecoverySourceRouteReceipt">>;
  placement?: WorkerSessionPlacementRecord;
  chatAbortControllers?: GatewayRequestContext["chatAbortControllers"];
  chatQueuedTurns?: GatewayRequestContext["chatQueuedTurns"];
  /** Exact-key, read-only storage receipts; absent/failed reads are not empty custody. */
  pendingInputReceipts?: ReadonlyArray<
    | { runId: string; state: "pending"; cancelled?: true }
    | { runId: string; state: "consumed"; consumedByEventId: string }
  >;
  /** Caller-visible, exact original route, revalidated by the RPC handler. */
  sourceRouteAuthorization?: Readonly<{
    agentId: string;
    key: string;
    sessionId: string;
    deliveryDigest: string;
  }>;
  readRunOwners?: typeof readAgentRunRecoveryOwnerSnapshot;
  readAdmissions?: typeof readSessionRecoveryAdmissionSnapshot;
  readQueuedCommand?: typeof hasQueuedCommandForRecoverySession;
  readTrackedProcess?: typeof hasTrackedProcessForRecoverySession;
}): SessionsRecoveryInspectResult {
  const { agentId, key, entry } = params;
  const observedRunId = entry.lifecycleRunId || entry.lastRunId;
  // Restart recovery persists its admission token only after binding it to
  // this session's fenced run. Ordinary audit contexts lack a session binding.
  const recoveryToken = entry.mainRestartRecovery?.executionIdentity;
  const restartExecution = observedRunId && recoveryToken?.runId === observedRunId &&
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
  const queuedCommandActive = (params.readQueuedCommand ??
    hasQueuedCommandForRecoverySession)(key);
  const trackedProcessActive = (params.readTrackedProcess ??
    hasTrackedProcessForRecoverySession)(agentId, key);
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
  // The input owner stores receipts under the exact session key and ID. A
  // pending receipt proves presence only. Consumed/cancelled/absent receipts do
  // not prove clear custody. This store does not bind an execution ID or retain
  // the original input ID after final completion, so terminal, lineage, and
  // store revision remain unknown.
  const pendingInputActive = Boolean(observedRunId &&
    params.pendingInputReceipts?.some((receipt) =>
      receipt.runId === observedRunId && receipt.state === "pending" &&
      receipt.cancelled !== true));
  const exactOwner = owners.consistent && observedRunId && owners.exactOwners.length === 1 &&
    owners.exactOwners[0]?.runId === observedRunId &&
    owners.activeRunIds.length === 1 && owners.activeRunIds[0] === observedRunId &&
    directControllerRunIds.every((runId) => runId === observedRunId)
    ? owners.exactOwners[0] : undefined;
  const receipt = entry.runRecoveryTerminalReceipt;
  const ownerClaimExecution = receipt?.executionSource === "owner-claim" &&
    receipt.ownerEpoch !== undefined && typeof receipt.executionId === "string" &&
    receipt.executionId.length > 0 ? receipt.executionId : undefined;
  const exactExecution = restartExecution ?? ownerClaimExecution;
  const terminalBound = receipt?.sessionId === entry.sessionId &&
    receipt.runId === observedRunId && receipt.executionId === exactExecution &&
    (receipt.executionSource === "owner-claim" ? Boolean(ownerClaimExecution) :
      Boolean(restartExecution)) && receipt.ownerEpoch !== undefined &&
    receipt.lifecycleRevision === entry.lifecycleRevision &&
    (receipt.executionSource === "owner-claim" ||
      entry.restartRecoveryRuns?.some((run) => run.runId === receipt.runId &&
        run.lifecycleGeneration === receipt.lifecycleGeneration)) &&
    owners.consistent &&
    typeof receipt.lifecycleGeneration === "string" &&
    receipt.lifecycleGeneration.length > 0 &&
    Number.isFinite(receipt.endedAt) && receipt.endedAt > 0 &&
    entry.status === receipt.status &&
    owners.activeRunIds.length === 0 && !directControllerActive;
  const input = entry.runRecoveryOriginalInputReceipt;
  const inputBound = terminalBound && input && !input.invalidated &&
    input.sessionKey === key && input.sessionId === entry.sessionId &&
    input.runId === receipt?.runId &&
    input.lifecycleRevision === entry.lifecycleRevision &&
    input.lifecycleGeneration === receipt?.lifecycleGeneration &&
    input.inputId === receipt?.originalInputReceiptId &&
    input.inputGeneration === receipt?.inputGeneration &&
    typeof input.inputId === "string" && input.inputId.length > 0 &&
    typeof input.inputGeneration === "string" && input.inputGeneration.length > 0;
  const route = entry.runRecoverySourceRouteReceipt;
  const routeBound = inputBound && route && !route.invalidated &&
    route.sessionKey === key && route.sessionId === entry.sessionId &&
    route.runId === observedRunId && route.lifecycleRevision === entry.lifecycleRevision &&
    route.lifecycleGeneration === receipt?.lifecycleGeneration &&
    route.inputGeneration === input?.inputGeneration &&
    route.executionId === exactExecution &&
    typeof route.sourceRouteGeneration === "string" &&
    route.sourceRouteGeneration.length > 0 &&
    typeof route.storeRevision === "string" && route.storeRevision.length > 0 &&
    route.deliveryDigest === fingerprintRunRecoverySourceRoute(entry.delivery);
  // A persisted digest establishes identity, not caller or channel authority.
  // External routes have no live authorization owner here and remain unknown.
  const routeAuthorized = Boolean(routeBound && entry.delivery?.kind === "internal" &&
    params.sourceRouteAuthorization?.agentId === agentId &&
    params.sourceRouteAuthorization.key === key &&
    params.sourceRouteAuthorization.sessionId === entry.sessionId &&
    params.sourceRouteAuthorization.deliveryDigest === route?.deliveryDigest);
  return {
    agentId,
    key,
    sessionId: entry.sessionId,
    ...(entry.lifecycleRevision ? { lifecycleRevision: entry.lifecycleRevision } : {}),
    ...(observedRunId ? { runId: observedRunId } : {}),
    ...(inputBound && input ? { originalInputReceiptId: input.inputId,
      inputGeneration: input.inputGeneration } : {}),
    ...(routeBound && route ? { sourceRouteGeneration: route.sourceRouteGeneration,
      storeRevision: route.storeRevision } : {}),
    ...(terminalBound && exactExecution ? { executionId: exactExecution } :
      restartExecution ? { executionId: restartExecution } : {}),
    ...(terminalBound && receipt?.ownerEpoch !== undefined
      ? { ownerEpoch: receipt.ownerEpoch } : {}),
    ...(exactPlacement ? { placementGeneration: exactPlacement.generation } : {}),
    ...(exactOwner ? { ownerEpoch: exactOwner.ownerEpoch,
      ownerLifecycleGeneration: owners.lifecycleGeneration } : {}),
    ...(entry.status ? { observedSessionStatus: entry.status } : {}),
    metadataCompleteness: {
      identity: true,
      execution: Boolean(restartExecution || (terminalBound && ownerClaimExecution)),
      store: Boolean(routeBound),
      owner: Boolean(exactOwner || terminalBound),
      placement: Boolean(exactPlacement),
      route: Boolean(routeBound),
    },
    evidence: {
      direct: (owners.consistent && owners.activeRunIds.length > 0) ||
        directControllerActive ? ACTIVE : UNKNOWN,
      pendingAdmission: admissions.pending || admissions.active ? ACTIVE : UNKNOWN,
      followupQueue: queuedTurnActive ? ACTIVE : UNKNOWN,
      laneQueue: queuedCommandActive ? ACTIVE : UNKNOWN,
      pendingInput: pendingInputActive ? ACTIVE : UNKNOWN,
      descendant: UNKNOWN,
      process: trackedProcessActive ? ACTIVE : UNKNOWN,
      worker: (exactPlacement?.state === "active" || exactPlacement?.state === "draining") &&
        exactPlacement.turnClaim?.owner === "worker" &&
        exactPlacement.turnClaim.ownerEpoch === exactPlacement.activeOwnerEpoch ? ACTIVE : UNKNOWN,
      terminal: terminalBound ? { complete: true, state: "clear" } : UNKNOWN,
      sourceRoute: routeAuthorized ? { complete: true, state: "clear" } : UNKNOWN,
      inputLineage: inputBound ? { complete: true, state: "clear" } : UNKNOWN,
      delivery: UNKNOWN,
      progress: UNKNOWN,
    },
    // A live owner can never receive a recovery token. The remaining families
    // also cannot prove absence or stable input/source generations yet.
  };
}
