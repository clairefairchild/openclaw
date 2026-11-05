import {
  ErrorCodes,
  errorShape,
  validateSessionsRecoveryInspectParams,
} from "../../../packages/gateway-protocol/src/index.js";
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
  "sessions.recovery.inspect": ({ params, respond, context, client, sessionMutationAuthorization }) => {
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
    const result = inspectGatewayRunRecovery({
      agentId: target.agentId,
      key: target.canonicalKey,
      entry,
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
