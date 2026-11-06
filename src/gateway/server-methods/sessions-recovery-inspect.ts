import {
  ErrorCodes,
  errorShape,
  validateSessionsRecoveryInspectParams,
} from "../../../packages/gateway-protocol/src/index.js";
import { executeExistingOpenClawStateRead } from "../../state/openclaw-state-db-readonly.js";
import type { WorkerSessionPlacementRecord } from "../worker-environments/placement-record.js";
import { inspectGatewayRunRecovery } from "../session-run-recovery-inspect-service.js";
import { resolveGatewaySessionStoreTargetWithStore } from "../session-utils-store-lookup.js";
import { resolveRequestedSessionAgentId } from "../session-request-agent.js";
import {
  authorizeIncognitoSessionTarget,
  createSessionListEntryFilter,
  resolveSessionSharingTarget,
} from "../session-sharing.js";
import type { GatewayRequestHandlers } from "./types.js";
import { assertValidParams } from "./validation.js";

export const sessionRecoveryInspectHandlers: GatewayRequestHandlers = {
  "sessions.recovery.inspect": async ({ params, respond, context, client, sessionMutationAuthorization }) => {
    if (!assertValidParams(params, validateSessionsRecoveryInspectParams,
      "sessions.recovery.inspect", respond)) {
      return;
    }
    const cfg = context.getRuntimeConfig();
    const resolved = resolveRequestedSessionAgentId(cfg, params.key, params.agentId);
    if (!resolved.ok) {
      respond(false, undefined, resolved.error);
      return;
    }
    if (resolved.agentId !== params.agentId) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "agent does not match key"));
      return;
    }
    const target = resolveSessionSharingTarget({ cfg, sessionKey: params.key,
      agentId: resolved.agentId });
    const notFound = errorShape(ErrorCodes.INVALID_REQUEST, "session not found");
    if (!target || target.agentId !== resolved.agentId ||
        (params.sessionId && target.entry.sessionId !== params.sessionId) ||
        authorizeIncognitoSessionTarget({ client: client ?? null,
          sessionKey: params.key, target }) ||
        createSessionListEntryFilter({ client: client ?? null,
          cfg: context.getCommittedRuntimeConfig?.() ?? cfg })?.(target.storeKey, target.entry) === false) {
      respond(false, undefined, notFound);
      return;
    }
    // Sharing uses a public list projection, which strips private run IDs. Fetch
    // the exact full metadata row read-only only after authorization succeeds.
    const fullTarget = resolveGatewaySessionStoreTargetWithStore({
      cfg, key: target.canonicalKey, agentId: target.agentId,
      exactRead: true, readOnly: true, readConsistency: "latest",
    });
    const entry = fullTarget.store[target.storeKey];
    if (fullTarget.canonicalKey !== target.canonicalKey ||
        fullTarget.storePath !== target.storePath ||
        !entry || entry.sessionId !== target.entry.sessionId ||
        entry.lifecycleRevision !== target.entry.lifecycleRevision ||
        createSessionListEntryFilter({ client: client ?? null,
          cfg: context.getCommittedRuntimeConfig?.() ?? cfg })?.(target.storeKey, entry) === false) {
      respond(false, undefined, notFound);
      return;
    }
    // The state worker supplies a committed, read-only placement projection.
    // An unavailable store is unknown, never an empty/clear placement.
    let placement: WorkerSessionPlacementRecord | undefined;
    try {
      const reply = await executeExistingOpenClawStateRead(
        {},
        { type: "workers.placementProjection", sessionIds: [entry.sessionId], conflictBindings: [] },
        { current: true },
      );
      if (reply?.ok && reply.type === "workers.placementProjection") {
        placement = reply.result.projection.placements.get(entry.sessionId);
      }
    } catch {
      // Keep the metadata read fail-closed without exposing state-store errors.
    }
    // Authorization and the session row may change while awaiting the worker.
    sessionMutationAuthorization?.assertCurrent();
    const refreshedSharing = resolveSessionSharingTarget({ cfg: context.getRuntimeConfig(),
      sessionKey: params.key, agentId: resolved.agentId });
    if (!refreshedSharing || refreshedSharing.agentId !== target.agentId ||
        refreshedSharing.canonicalKey !== target.canonicalKey ||
        refreshedSharing.storePath !== target.storePath ||
        refreshedSharing.entry.sessionId !== target.entry.sessionId ||
        authorizeIncognitoSessionTarget({ client: client ?? null,
          sessionKey: params.key, target: refreshedSharing })) {
      respond(false, undefined, notFound);
      return;
    }
    const refreshed = resolveGatewaySessionStoreTargetWithStore({
      cfg, key: target.canonicalKey, agentId: target.agentId,
      exactRead: true, readOnly: true, readConsistency: "latest",
    });
    const currentEntry = refreshed.store[target.storeKey];
    if (refreshed.canonicalKey !== target.canonicalKey ||
        refreshed.storePath !== target.storePath || !currentEntry ||
        currentEntry.sessionId !== entry.sessionId ||
        currentEntry.lifecycleRevision !== entry.lifecycleRevision ||
        currentEntry.lifecycleRunId !== entry.lifecycleRunId ||
        currentEntry.lastRunId !== entry.lastRunId ||
        JSON.stringify(currentEntry.mainRestartRecovery) !== JSON.stringify(entry.mainRestartRecovery) ||
        JSON.stringify(currentEntry.restartRecoveryRuns) !== JSON.stringify(entry.restartRecoveryRuns) ||
        createSessionListEntryFilter({ client: client ?? null,
          cfg: context.getCommittedRuntimeConfig?.() ?? cfg })?.(target.storeKey, currentEntry) === false) {
      respond(false, undefined, notFound);
      return;
    }
    const result = inspectGatewayRunRecovery({
      agentId: target.agentId,
      key: target.canonicalKey,
      storePath: target.storePath,
      entry: currentEntry,
      placement,
      chatAbortControllers: context.chatAbortControllers,
      chatQueuedTurns: context.chatQueuedTurns,
    });
    // Exact run selection never falls back to another run sharing the key.
    if (params.runId && result.runId !== params.runId) {
      respond(false, undefined, notFound);
      return;
    }
    sessionMutationAuthorization?.assertCurrent();
    respond(true, result);
  },
};
