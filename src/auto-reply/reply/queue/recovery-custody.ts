/** Durable custody of follow-up identities accepted by the in-memory publisher. */
import { randomUUID } from "node:crypto";
import { getRecoveryQueueCoordinatorBroker } from "../../../state/run-recovery-queue-coordinator-broker.js";
import type { FollowupRun } from "./types.js";

// The ledger, not this process-local index, is authoritative. Lost indexes after
// restart leave ledger reservations unresolved until an explicit reconciliation.
const workIds = new WeakMap<FollowupRun, string>();
const publicationAttempted = new WeakSet<FollowupRun>();
const aggregateSources = new WeakMap<FollowupRun, readonly FollowupRun[]>();

export function carryFollowupRecoveryWork(source: FollowupRun, replacement: FollowupRun): void {
  const workId = workIds.get(source);
  if (workId) {
    workIds.set(replacement, workId);
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
  const firstAttempt = !publicationAttempted.has(run);
  publicationAttempted.add(run);
  return firstAttempt;
}

export async function reserveFollowupRunRecoveryWork(run: FollowupRun): Promise<void> {
  let workId = workIds.get(run);
  if (!workId) {
    workId = randomUUID();
    workIds.set(run, workId);
  }
  const broker = await getRecoveryQueueCoordinatorBroker();
  // A repeated exact reservation reconciles an uncertain broker acknowledgment.
  await broker.reserve({ workId, queue: "followup", scope: { kind: "unmarked" } });
}

export async function startFollowupRunRecoveryWork(run: FollowupRun): Promise<void> {
  const workId = workIds.get(run);
  if (!workId) {
    // Pre-cutover or synthetic work has no durable identity. The producer marker
    // remains off, so this cannot be used as absence proof.
    return;
  }
  const broker = await getRecoveryQueueCoordinatorBroker();
  await broker.start(workId);
}

export async function settleFollowupRunRecoveryWork(
  run: FollowupRun,
  terminalReceipt: string,
): Promise<void> {
  const workId = workIds.get(run);
  if (!workId) {
    return;
  }
  const broker = await getRecoveryQueueCoordinatorBroker();
  await broker.settle(workId, `${terminalReceipt}:${workId}`);
}
