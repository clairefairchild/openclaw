/** Durable custody of follow-up identities accepted by the in-memory publisher. */
import { randomUUID } from "node:crypto";
import { getRecoveryQueueCoordinatorBroker } from "../../../state/run-recovery-queue-coordinator-broker.js";
import type { FollowupRun } from "./types.js";

// The ledger, not this process-local index, is authoritative. Lost indexes after
// restart leave ledger reservations unresolved until an explicit reconciliation.
type RecoveryWork = { workId: string; publicationAttempted: boolean; terminalReceipt?: string };
const work = new WeakMap<FollowupRun, RecoveryWork>();
const aggregateSources = new WeakMap<FollowupRun, readonly FollowupRun[]>();

export function carryFollowupRecoveryWork(source: FollowupRun, replacement: FollowupRun): void {
  const sourceWork = work.get(source);
  if (sourceWork) {
    work.set(replacement, sourceWork);
  }
}

export function bindFollowupAggregateRecoverySources(
  aggregate: FollowupRun,
  sources: readonly FollowupRun[],
): void {
  aggregateSources.set(aggregate, sources);
}

export function followupRecoverySources(run: FollowupRun): readonly FollowupRun[] {
  return aggregateSources.get(run) ?? [run];
}

export function beginFollowupPublication(run: FollowupRun): boolean {
  const sourceWork = work.get(run);
  if (!sourceWork || sourceWork.terminalReceipt) {
    throw new Error("Follow-up publication has no open recovery reservation");
  }
  const firstAttempt = !sourceWork.publicationAttempted;
  sourceWork.publicationAttempted = true;
  return firstAttempt;
}

export async function reserveFollowupRunRecoveryWork(run: FollowupRun): Promise<void> {
  let sourceWork = work.get(run);
  if (!sourceWork) {
    sourceWork = { workId: randomUUID(), publicationAttempted: false };
    work.set(run, sourceWork);
  }
  if (sourceWork.terminalReceipt) {
    throw new Error("Follow-up recovery work already has a terminal attempt");
  }
  const broker = await getRecoveryQueueCoordinatorBroker();
  // A repeated exact reservation reconciles an uncertain broker acknowledgment.
  await broker.reserve({ workId: sourceWork.workId, queue: "followup", scope: { kind: "unmarked" } });
}

export async function startFollowupRunRecoveryWork(run: FollowupRun): Promise<void> {
  const sourceWork = work.get(run);
  if (!sourceWork) {
    // Pre-cutover or synthetic work has no durable identity. The producer marker
    // remains off, so this cannot be used as absence proof.
    return;
  }
  if (sourceWork.terminalReceipt) {
    throw new Error("Follow-up recovery work already has a terminal attempt");
  }
  const broker = await getRecoveryQueueCoordinatorBroker();
  await broker.start(sourceWork.workId);
}

export async function settleFollowupRunRecoveryWork(
  run: FollowupRun,
  terminalReceipt: string,
): Promise<void> {
  const sourceWork = work.get(run);
  if (!sourceWork) {
    return;
  }
  const receipt = `${terminalReceipt}:${sourceWork.workId}`;
  if (sourceWork.terminalReceipt && sourceWork.terminalReceipt !== receipt) {
    throw new Error("Follow-up terminal receipt changed");
  }
  // Preserve the exact receipt even when the broker write acknowledgment is lost.
  sourceWork.terminalReceipt = receipt;
  const broker = await getRecoveryQueueCoordinatorBroker();
  await broker.settle(sourceWork.workId, receipt);
}
