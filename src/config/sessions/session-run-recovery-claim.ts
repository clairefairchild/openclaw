import { randomUUID } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";
import { executeSqliteQuerySync, executeSqliteQueryTakeFirstSync, getNodeSqliteKysely } from "../../infra/kysely-sync.js";
import { readOpenClawAgentDatabaseIdentity } from "../../state/openclaw-agent-db-identity.js";
import { runOpenClawAgentWriteTransaction, type OpenClawAgentDatabase, type OpenClawAgentDatabaseOptions } from "../../state/openclaw-agent-db.js";
import { ensureSessionRunRecoveryClaimsSchema } from "../../state/openclaw-agent-pending-inputs-schema.js";
import { fingerprintRunRecoverySourceRoute } from "./run-recovery-source-route.js";
import { readSessionEntryRow } from "./session-accessor.sqlite-entry-read.js";

export type SessionRunRecoveryClaimExpected = {
  agentId: string;
  databaseIdentity: string;
  databaseBirthtime: string;
  sessionKey: string;
  sessionId: string;
  executionId: string;
  runId: string;
  lifecycleRevision: string;
  lifecycleGeneration: string;
  ownerEpoch: number;
  storeRevision: string;
  sourceRouteGeneration: string;
  deliveryDigest: string;
  originalInputReceiptId: string;
  inputGeneration: string;
};

type ClaimRow = {
  claim_id: string;
  database_identity: string;
  database_birthtime: string;
  session_key: string;
  session_id: string;
  execution_id: string;
  run_id: string;
  lifecycle_revision: string;
  lifecycle_generation: string;
  owner_epoch: number;
  store_revision: string;
  source_route_generation: string;
  delivery_digest: string;
  original_input_receipt_id: string;
  input_generation: string;
  idempotency_key: string;
  state: "prepared" | "needs_reconciliation";
  prepared_at: number;
};
type ClaimDb = { session_run_recovery_claims: ClaimRow };
function claims(db: DatabaseSync) {
  return getNodeSqliteKysely<ClaimDb>(db);
}

/** Called inside the same IMMEDIATE transaction as normal input admission. */
export function hasSessionRunRecoveryClaim(db: DatabaseSync, sessionKey: string, sessionId: string): boolean {
  ensureSessionRunRecoveryClaimsSchema(db);
  return Boolean(executeSqliteQueryTakeFirstSync(db, claims(db)
    .selectFrom("session_run_recovery_claims").select("claim_id")
    .where("session_key", "=", sessionKey).where("session_id", "=", sessionId)));
}

export type SessionRunRecoveryClaimResult =
  | { kind: "prepared"; claimId: string; state: "prepared" | "needs_reconciliation"; repeated: boolean }
  | { kind: "held"; reason: "changed_generation" | "unconsumed_input" | "existing_claim" | "ambiguous_store" };

/** A durable reservation only. It never accepts an input, dispatches, or proves
 * process-local custody. The Gateway must keep every other evidence family UNKNOWN. */
export function prepareSessionRunRecoveryClaimInWorker(
  database: OpenClawAgentDatabase,
  options: OpenClawAgentDatabaseOptions,
  expected: SessionRunRecoveryClaimExpected,
  idempotencyKey: string,
  assertCurrent: () => void,
): SessionRunRecoveryClaimResult {
  if (!idempotencyKey || !Number.isSafeInteger(expected.ownerEpoch) || expected.ownerEpoch < 0) {
    return { kind: "held", reason: "changed_generation" };
  }
  return runOpenClawAgentWriteTransaction<SessionRunRecoveryClaimResult>((current) => {
    assertCurrent();
    const physical = readOpenClawAgentDatabaseIdentity(current);
    if (current.db !== database.db || current.agentId !== expected.agentId ||
        physical.identity !== expected.databaseIdentity ||
        physical.birthtime !== expected.databaseBirthtime) {
      return { kind: "held", reason: "ambiguous_store" };
    }
    ensureSessionRunRecoveryClaimsSchema(current.db);
    const db = claims(current.db);
    const node = executeSqliteQueryTakeFirstSync(current.db, getNodeSqliteKysely<{
      session_nodes: { session_key: string; current_session_id: string | null };
    }>(current.db).selectFrom("session_nodes").select("current_session_id")
      .where("session_key", "=", expected.sessionKey));
    const entry = readSessionEntryRow(current, expected.sessionKey)?.entry;
    const terminal = entry?.runRecoveryTerminalReceipt;
    const original = entry?.runRecoveryOriginalInputReceipt;
    const route = entry?.runRecoverySourceRouteReceipt;
    if (node?.current_session_id !== expected.sessionId || entry?.sessionId !== expected.sessionId ||
        entry.lifecycleRevision !== expected.lifecycleRevision ||
        (entry.lifecycleRunId || entry.lastRunId) !== expected.runId ||
        terminal?.sessionId !== expected.sessionId || terminal.executionId !== expected.executionId ||
        terminal.runId !== expected.runId || terminal.lifecycleRevision !== expected.lifecycleRevision ||
        terminal.lifecycleGeneration !== expected.lifecycleGeneration ||
        terminal.ownerEpoch !== expected.ownerEpoch ||
        original?.invalidated || original?.inputId !== expected.originalInputReceiptId ||
        original.inputGeneration !== expected.inputGeneration ||
        original.lifecycleGeneration !== expected.lifecycleGeneration ||
        route?.invalidated || route?.executionId !== expected.executionId ||
        route.sourceRouteGeneration !== expected.sourceRouteGeneration ||
        route.storeRevision !== expected.storeRevision ||
        route.deliveryDigest !== expected.deliveryDigest ||
        route.inputGeneration !== expected.inputGeneration ||
        fingerprintRunRecoverySourceRoute(entry.delivery) !== expected.deliveryDigest) {
      return { kind: "held", reason: "changed_generation" };
    }
    const existing = executeSqliteQuerySync(current.db, db.selectFrom("session_run_recovery_claims")
      .selectAll().where("session_key", "=", expected.sessionKey)
      .where("session_id", "=", expected.sessionId).limit(2)).rows;
    if (existing.length) {
      const row = existing[0];
      if (existing.length === 1 && row?.execution_id === expected.executionId &&
          row.run_id === expected.runId &&
          row.database_identity === expected.databaseIdentity &&
          row.database_birthtime === expected.databaseBirthtime && row.lifecycle_revision === expected.lifecycleRevision &&
          row.lifecycle_generation === expected.lifecycleGeneration && row.owner_epoch === expected.ownerEpoch &&
          row.store_revision === expected.storeRevision &&
          row.source_route_generation === expected.sourceRouteGeneration &&
          row.delivery_digest === expected.deliveryDigest &&
          row.original_input_receipt_id === expected.originalInputReceiptId &&
          row.input_generation === expected.inputGeneration && row.idempotency_key === idempotencyKey) {
        return { kind: "prepared", claimId: row.claim_id, state: row.state, repeated: true };
      }
      return { kind: "held", reason: "existing_claim" };
    }
    const pending = executeSqliteQueryTakeFirstSync(current.db, getNodeSqliteKysely<{
      session_pending_inputs: { input_id: string; session_key: string; session_id: string; consumed_event_id: string | null };
    }>(current.db).selectFrom("session_pending_inputs").select("input_id")
      .where("session_key", "=", expected.sessionKey).where("session_id", "=", expected.sessionId)
      .where("consumed_event_id", "is", null));
    if (pending) {
      return { kind: "held", reason: "unconsumed_input" };
    }
    const claimId = randomUUID();
    executeSqliteQuerySync(current.db, db.insertInto("session_run_recovery_claims").values({
      claim_id: claimId, database_identity: expected.databaseIdentity,
      database_birthtime: expected.databaseBirthtime, session_key: expected.sessionKey, session_id: expected.sessionId,
      execution_id: expected.executionId, run_id: expected.runId,
      lifecycle_revision: expected.lifecycleRevision, lifecycle_generation: expected.lifecycleGeneration,
      owner_epoch: expected.ownerEpoch, store_revision: expected.storeRevision,
      source_route_generation: expected.sourceRouteGeneration, delivery_digest: expected.deliveryDigest,
      original_input_receipt_id: expected.originalInputReceiptId, input_generation: expected.inputGeneration,
      idempotency_key: idempotencyKey, state: "prepared", prepared_at: Date.now(),
    }));
    assertCurrent();
    return { kind: "prepared", claimId, state: "prepared", repeated: false };
  }, options, { operationLabel: "session.run-recovery.prepare" });
}
