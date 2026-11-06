import type { DatabaseSync } from "node:sqlite";
import { executeSqliteQueryTakeFirstSync, getNodeSqliteKysely } from "../../infra/kysely-sync.js";
import type { OpenClawAgentDatabase, OpenClawAgentDatabaseOptions } from "../../state/openclaw-agent-db.js";
import { ensureSessionRunRecoveryClaimsSchema } from "../../state/openclaw-agent-pending-inputs-schema.js";

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
  | { kind: "held"; reason: "changed_generation" | "unconsumed_input" | "existing_claim" | "ambiguous_store" | "queue_not_cut_over" };

/** Legacy agent-local claim entry point is intentionally unreachable in Stage 1.
 * Its agent DB transaction cannot atomically compare a separate global queue
 * coordinator. A later bridge must use one attached-DB transaction or move the
 * claim into the coordinator and atomically bind the accepted-input outbox.
 * Never treat a process-local queue snapshot as a substitute. */
export function prepareSessionRunRecoveryClaimInWorker(
  _database: OpenClawAgentDatabase,
  _options: OpenClawAgentDatabaseOptions,
  _expected: SessionRunRecoveryClaimExpected,
  _idempotencyKey: string,
  _assertCurrent: () => void,
): SessionRunRecoveryClaimResult {
  return { kind: "held", reason: "queue_not_cut_over" };
}

/** The agent row is only the compatibility fence for the existing pending-input
 * consumer. The global coordinator remains the claim and outbox authority. */
export function installSessionRunRecoveryHandoffFence(
  db: DatabaseSync, expected: SessionRunRecoveryClaimExpected,
  idempotencyKey: string, claimId: string,
): void {
  ensureSessionRunRecoveryClaimsSchema(db);
  const store = claims(db);
  const existing = executeSqliteQueryTakeFirstSync(db, store.selectFrom("session_run_recovery_claims")
    .selectAll().where("session_key", "=", expected.sessionKey)
    .where("session_id", "=", expected.sessionId));
  if (existing) {
    if (existing.claim_id !== claimId || existing.idempotency_key !== idempotencyKey ||
        existing.database_identity !== expected.databaseIdentity ||
        existing.database_birthtime !== expected.databaseBirthtime ||
        existing.execution_id !== expected.executionId || existing.run_id !== expected.runId ||
        existing.lifecycle_revision !== expected.lifecycleRevision ||
        existing.lifecycle_generation !== expected.lifecycleGeneration ||
        existing.owner_epoch !== expected.ownerEpoch ||
        existing.store_revision !== expected.storeRevision ||
        existing.source_route_generation !== expected.sourceRouteGeneration ||
        existing.delivery_digest !== expected.deliveryDigest ||
        existing.original_input_receipt_id !== expected.originalInputReceiptId ||
        existing.input_generation !== expected.inputGeneration) {
      throw new Error("Recovery handoff conflicts with an existing claim fence");
    }
    return;
  }
  executeSqliteQueryTakeFirstSync(db, store.insertInto("session_run_recovery_claims").values({
    claim_id: claimId, database_identity: expected.databaseIdentity,
    database_birthtime: expected.databaseBirthtime, session_key: expected.sessionKey,
    session_id: expected.sessionId, execution_id: expected.executionId,
    run_id: expected.runId, lifecycle_revision: expected.lifecycleRevision,
    lifecycle_generation: expected.lifecycleGeneration, owner_epoch: expected.ownerEpoch,
    store_revision: expected.storeRevision, source_route_generation: expected.sourceRouteGeneration,
    delivery_digest: expected.deliveryDigest, original_input_receipt_id: expected.originalInputReceiptId,
    input_generation: expected.inputGeneration, idempotency_key: idempotencyKey,
    state: "prepared", prepared_at: Date.now(),
  }).returning("claim_id"));
}
