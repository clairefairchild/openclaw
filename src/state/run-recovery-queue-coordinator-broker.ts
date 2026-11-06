/** Async host access to the global queue ledger. This does not activate producer cutover. */
import { createHash } from "node:crypto";
import { redactTranscriptMessageForStorage } from "../config/sessions/session-accessor.sqlite-transcript-store.js";
import {
  readRecoveryHandoffPendingInput,
  stageSessionPendingInput,
} from "../config/sessions/session-accessor.pending-inputs.js";
import {
  evaluateRunRecoveryFence,
  verifyRunRecoveryReviewToken,
  type RunRecoverySemanticReview,
  type RunRecoverySnapshot,
} from "../gateway/session-run-recovery-fence.js";
import { acquireGatewayStateOwner } from "../infra/gateway-state-owner.js";
import {
  openSqliteWorkerStore,
  runSqliteWorkerStoreOperation,
  runSqliteWorkerStoreWrite,
  type SqliteWorkerStore,
} from "../infra/sqlite-worker-store.js";
import {
  resolveRunRecoveryQueueCoordinatorPath,
  type RecoveryQueueReservation,
  type RecoveryClaimOutboxInput,
} from "./run-recovery-queue-coordinator.js";
import type { RecoveryQueueWorkerOperations } from "./run-recovery-queue-coordinator.worker.js";

export type RecoveryClaimHostAuthority = {
  callerId: string;
  reviewToken: string;
  reviewSecret: Buffer;
  review: RunRecoverySemanticReview;
  /** A fresh owner-held read at each transaction and commit grant. */
  readCurrent(): {
    snapshot: RunRecoverySnapshot;
    databaseIdentity: string;
    databaseBirthtime: string;
    lifecycleGeneration: string;
    inputGeneration: string;
    deliveryDigest: string;
  };
  assertCallerCurrent(): void;
};

export function assertRecoveryClaimHostCurrent(
  input: RecoveryClaimOutboxInput, authority: RecoveryClaimHostAuthority,
  phase: "claim" | "admitted" = "claim",
): void {
  authority.assertCallerCurrent();
  const current = authority.readCurrent();
  const expected = input.expected;
  const identity = current.snapshot.identity;
  if (identity.agentId !== expected.agentId || identity.key !== expected.sessionKey ||
      identity.sessionId !== expected.sessionId || identity.executionId !== expected.executionId ||
      identity.runId !== expected.runId || identity.lifecycleRevision !== expected.lifecycleRevision ||
      identity.storeRevision !== expected.storeRevision || identity.ownerEpoch !== expected.ownerEpoch ||
      identity.sourceRouteGeneration !== expected.sourceRouteGeneration ||
      identity.originalInputReceiptId !== expected.originalInputReceiptId ||
      current.databaseIdentity !== expected.databaseIdentity ||
      current.databaseBirthtime !== expected.databaseBirthtime ||
      current.lifecycleGeneration !== expected.lifecycleGeneration ||
      current.inputGeneration !== expected.inputGeneration ||
      current.deliveryDigest !== expected.deliveryDigest ||
      createHash("sha256").update(JSON.stringify(input.route)).digest("hex") !== expected.deliveryDigest) {
    throw new Error("Recovery claim source generation changed");
  }
  if (phase === "claim" && !verifyRunRecoveryReviewToken({ token: authority.reviewToken, snapshot: current.snapshot,
    callerId: authority.callerId, secret: authority.reviewSecret, now: Date.now() })) {
    throw new Error("Recovery claim review or caller authority expired");
  }
  // After handoff, the exact owned pending input is expected to be active. The
  // agent-local fence prevents a second input; all other custody stays live.
  const snapshot = phase === "admitted" ? { ...current.snapshot,
    custody: { ...current.snapshot.custody,
      pendingInput: { complete: true, active: false } } } : current.snapshot;
  const fence = evaluateRunRecoveryFence({ expected: identity, snapshot,
    review: authority.review });
  if (!fence.eligible) throw new Error(`Recovery claim is not eligible: ${fence.reason}`);
}

export async function openRecoveryQueueCoordinatorBroker(
  databasePath = resolveRunRecoveryQueueCoordinatorPath(),
) {
  // The host owns the physical path. Native writes borrow this authority at
  // transaction and commit grants; a second host cannot publish a rival ledger.
  const owner = acquireGatewayStateOwner({ databasePath, role: "gateway" });
  let store: SqliteWorkerStore<RecoveryQueueWorkerOperations> | undefined;
  let incarnation: string;
  try {
    store = await openSqliteWorkerStore<RecoveryQueueWorkerOperations>({
      moduleUrl: new URL("./run-recovery-queue-coordinator.worker.ts", import.meta.url),
      databasePath,
      input: undefined,
    });
    incarnation = await runSqliteWorkerStoreOperation(
      store,
      (scope) => scope.execute({ type: "recoveryQueue.incarnation", input: undefined }),
      undefined,
      () => owner.assertCurrent(),
    );
  } catch (error) {
    // An uncertain worker open/close is not proof that native custody was
    // released. Retain the physical owner unless a known store closes cleanly.
    if (store) {
      try {
        await store.close();
        owner.release();
      } catch {
        // The original admission failure remains the actionable error.
      }
    } else {
      owner.release();
    }
    throw error;
  }
  const activeStore = store;
  if (!activeStore) {
    throw new Error("Recovery queue coordinator broker opened without a store");
  }
  const write = <T>(operation: (scope: Pick<typeof activeStore, "execute">) => Promise<T>,
    assertCurrent: () => void = () => owner.assertCurrent()): Promise<T> =>
    runSqliteWorkerStoreWrite(activeStore, operation, assertCurrent, [databasePath]);
  return {
    incarnation,
    claim: (input: Omit<RecoveryClaimOutboxInput, "incarnation">, authority: RecoveryClaimHostAuthority) => {
      const command = { ...input, incarnation,
        message: redactTranscriptMessageForStorage(input.message, {}) };
      const assertCurrent = () => { owner.assertCurrent(); assertRecoveryClaimHostCurrent(command, authority); };
      return write((scope) => scope.execute({ type: "recoveryQueue.claim", input: command }), assertCurrent);
    },
    readClaim: (agentId: string, sessionId: string, idempotencyKey: string) =>
      runSqliteWorkerStoreOperation(activeStore,
        (scope) => scope.execute({ type: "recoveryQueue.readClaim",
          input: { agentId, sessionId, idempotencyKey } }), undefined, () => owner.assertCurrent()),
    /** Readback precedes every retry. Existing custody is never dispatched again. */
    handoff: async (params: {
      agentId: string; sessionKey: string; sessionId: string; storePath: string;
      idempotencyKey: string; claimId: string;
      authority: RecoveryClaimHostAuthority;
    }) => {
      const row = await runSqliteWorkerStoreOperation(activeStore,
        (scope) => scope.execute({ type: "recoveryQueue.readClaim",
          input: { agentId: params.agentId, sessionId: params.sessionId,
            idempotencyKey: params.idempotencyKey } }), undefined, () => owner.assertCurrent());
      if (!row || row.claimId !== params.claimId || row.expected.sessionKey !== params.sessionKey) {
        throw new Error("Recovery handoff has no exact outbox claim");
      }
      const scope = { agentId: params.agentId, sessionKey: params.sessionKey,
        sessionId: params.sessionId, storePath: params.storePath };
      const present = readRecoveryHandoffPendingInput(scope, params.idempotencyKey);
      if (present) {
        if (present.requestHash !== `request:${row.claimId}` ||
            present.runId !== `${row.expected.runId}:recovery` ||
            JSON.stringify(present.message) !== JSON.stringify(redactTranscriptMessageForStorage(row.message, {}))) {
          throw new Error("Recovery handoff idempotency key conflicts with accepted input");
        }
        await write((worker) => worker.execute({ type: "recoveryQueue.handoff",
          input: { incarnation, agentId: params.agentId, sessionId: params.sessionId,
            idempotencyKey: params.idempotencyKey, claimId: row.claimId,
            inputId: present.inputId } }));
        return { kind: "reconciled" as const, inputId: present.inputId };
      }
      if (row.state !== "pending") {
        throw new Error("Recovery handoff receipt has no matching pending input");
      }
      assertRecoveryClaimHostCurrent({ ...row, incarnation }, params.authority);
      const receipt = await stageSessionPendingInput(scope, {
        runId: `${row.expected.runId}:recovery`, message: row.message,
        requestFingerprint: row.claimId,
        recoveryClaim: { claimId: row.claimId, expected: row.expected },
        assertCurrent: () => assertRecoveryClaimHostCurrent({ ...row, incarnation }, params.authority),
        assertAdmittedCurrent: () => assertRecoveryClaimHostCurrent(
          { ...row, incarnation }, params.authority, "admitted"),
      });
      if (!receipt || receipt.state !== "queued") {
        throw new Error("Recovery handoff was not accepted by pending-input owner");
      }
      const accepted = readRecoveryHandoffPendingInput(scope, params.idempotencyKey);
      if (!accepted || accepted.inputId !== receipt.inputId ||
          accepted.requestHash !== `request:${row.claimId}`) {
        throw new Error("Recovery handoff acceptance could not be read back");
      }
      const acknowledged = await write((worker) => worker.execute({ type: "recoveryQueue.handoff",
        input: { incarnation, agentId: params.agentId, sessionId: params.sessionId,
          idempotencyKey: params.idempotencyKey, claimId: row.claimId,
          inputId: receipt.inputId } }));
      if (acknowledged.state !== "handed_off") {
        receipt.finish("interrupted");
        throw new Error("Recovery handoff was invalidated by newer queue work");
      }
      return { kind: "staged" as const, inputId: receipt.inputId, receipt };
    },
    reserve: (work: Omit<RecoveryQueueReservation, "incarnation">) =>
      write((scope) => scope.execute({ type: "recoveryQueue.reserve", input: { ...work, incarnation } })),
    start: (workId: string) =>
      write((scope) => scope.execute({ type: "recoveryQueue.start", input: { incarnation, workId } })),
    settle: (workId: string, terminalReceiptId: string) =>
      write((scope) => scope.execute({
        type: "recoveryQueue.settle",
        input: { incarnation, workId, terminalReceiptId },
      })),
    close: async () => {
      await activeStore.close();
      owner.release();
    },
  };
}

// All queue producers in a Gateway share one physical owner and worker.
// A rejected open is retained: retrying an uncertain open could bypass custody.
let sharedBroker: ReturnType<typeof openRecoveryQueueCoordinatorBroker> | undefined;
export function getRecoveryQueueCoordinatorBroker() {
  sharedBroker ??= openRecoveryQueueCoordinatorBroker();
  return sharedBroker;
}
