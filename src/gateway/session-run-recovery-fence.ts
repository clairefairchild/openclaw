/** Fail-closed, metadata-only eligibility predicate for an exact Gateway run. */
export type RunRecoveryIdentity = {
  agentId: string;
  key: string;
  sessionId: string;
  executionId: string;
  runId: string;
  lifecycleRevision: string;
  storeRevision: string;
  ownerEpoch: number;
  placementGeneration: number;
  sourceRouteGeneration: string;
  originalInputReceiptId: string;
};

export type RunRecoveryCustodyFamily =
  | "direct"
  | "pendingAdmission"
  | "followupQueue"
  | "laneQueue"
  | "pendingInput"
  | "descendant"
  | "process"
  | "worker";

export type RunRecoveryCustody = Record<
  RunRecoveryCustodyFamily,
  { complete: boolean; active: boolean }
>;

export type RunRecoverySnapshot = {
  identity: RunRecoveryIdentity;
  custody: RunRecoveryCustody;
  terminal: { complete: boolean; state: "terminal" | "running" | "unknown" };
  sourceRoute: { complete: boolean; authorized: boolean; generation: string };
  inputLineage: { complete: boolean; unchanged: boolean; receiptId: string };
  delivery: { complete: boolean; answered: boolean };
};

/** This is an explicit human/agent semantic decision, not a status inferred from a timeout. */
export type RunRecoverySemanticReview = {
  reviewedIdentity: RunRecoveryIdentity;
  originalRequestReviewed: boolean;
  laterSteeringReviewed: boolean;
  repliesReviewed: boolean;
  childOutcomesReviewed: boolean;
  obligation: "unfinished" | "completed" | "awaiting_user" | "cancelled" | "unknown";
};

export type RunRecoveryFenceResult = { eligible: true } | { eligible: false; reason: string };

const CUSTODY_FAMILIES: readonly RunRecoveryCustodyFamily[] = [
  "direct",
  "pendingAdmission",
  "followupQueue",
  "laneQueue",
  "pendingInput",
  "descendant",
  "process",
  "worker",
];

export function sameRunRecoveryIdentity(
  expected: RunRecoveryIdentity,
  current: RunRecoveryIdentity,
): boolean {
  return (
    expected.agentId === current.agentId &&
    expected.key === current.key &&
    expected.sessionId === current.sessionId &&
    expected.executionId === current.executionId &&
    expected.runId === current.runId &&
    expected.lifecycleRevision === current.lifecycleRevision &&
    expected.storeRevision === current.storeRevision &&
    expected.ownerEpoch === current.ownerEpoch &&
    expected.placementGeneration === current.placementGeneration &&
    expected.sourceRouteGeneration === current.sourceRouteGeneration &&
    expected.originalInputReceiptId === current.originalInputReceiptId
  );
}

/** Must be called again within the owning lifecycle/SQLite commit fence. */
export function evaluateRunRecoveryFence(params: {
  expected: RunRecoveryIdentity;
  snapshot: RunRecoverySnapshot;
  review: RunRecoverySemanticReview;
}): RunRecoveryFenceResult {
  const { expected, snapshot, review } = params;
  if (!sameRunRecoveryIdentity(expected, snapshot.identity)) {
    return { eligible: false, reason: "identity_changed" };
  }
  if (!sameRunRecoveryIdentity(expected, review.reviewedIdentity)) {
    return { eligible: false, reason: "semantic_review_stale" };
  }
  if (!snapshot.terminal.complete || snapshot.terminal.state !== "terminal") {
    return { eligible: false, reason: "terminal_unproven" };
  }
  for (const family of CUSTODY_FAMILIES) {
    const evidence = snapshot.custody[family];
    if (!evidence?.complete) {
      return { eligible: false, reason: `${family}_unknown` };
    }
    if (evidence.active) {
      return { eligible: false, reason: `${family}_active` };
    }
  }
  if (
    !snapshot.sourceRoute.complete ||
    !snapshot.sourceRoute.authorized ||
    snapshot.sourceRoute.generation !== expected.sourceRouteGeneration
  ) {
    return { eligible: false, reason: "source_route_unproven" };
  }
  if (
    !snapshot.inputLineage.complete ||
    !snapshot.inputLineage.unchanged ||
    snapshot.inputLineage.receiptId !== expected.originalInputReceiptId
  ) {
    return { eligible: false, reason: "input_lineage_changed" };
  }
  if (!snapshot.delivery.complete || snapshot.delivery.answered) {
    return { eligible: false, reason: "answer_state_unproven_or_completed" };
  }
  if (
    !review.originalRequestReviewed ||
    !review.laterSteeringReviewed ||
    !review.repliesReviewed ||
    !review.childOutcomesReviewed ||
    review.obligation !== "unfinished"
  ) {
    return { eligible: false, reason: "unfinished_obligation_unproven" };
  }
  return { eligible: true };
}
