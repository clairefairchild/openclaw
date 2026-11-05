import { createHash, createHmac, timingSafeEqual } from "node:crypto";

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


/** Binds a short-lived review to one caller and the complete metadata generation. */
export function fingerprintRunRecoverySnapshot(snapshot: RunRecoverySnapshot): string {
  const canonical = [
    snapshot.identity.agentId,
    snapshot.identity.key,
    snapshot.identity.sessionId,
    snapshot.identity.executionId,
    snapshot.identity.runId,
    snapshot.identity.lifecycleRevision,
    snapshot.identity.storeRevision,
    snapshot.identity.ownerEpoch,
    snapshot.identity.placementGeneration,
    snapshot.identity.sourceRouteGeneration,
    snapshot.identity.originalInputReceiptId,
    ...CUSTODY_FAMILIES.flatMap((family) => [
      snapshot.custody[family]?.complete ?? false,
      snapshot.custody[family]?.active ?? true,
    ]),
    snapshot.terminal.complete,
    snapshot.terminal.state,
    snapshot.sourceRoute.complete,
    snapshot.sourceRoute.authorized,
    snapshot.sourceRoute.generation,
    snapshot.inputLineage.complete,
    snapshot.inputLineage.unchanged,
    snapshot.inputLineage.receiptId,
    snapshot.delivery.complete,
    snapshot.delivery.answered,
  ];
  return createHash("sha256").update(JSON.stringify(canonical)).digest("hex");
}

const REVIEW_TOKEN_MAX_AGE_MS = 60_000;

export function issueRunRecoveryReviewToken(params: {
  snapshot: RunRecoverySnapshot;
  callerId: string;
  secret: Buffer;
  now: number;
  expiresAt: number;
}): string {
  if (
    !params.callerId ||
    params.secret.length < 32 ||
    !Number.isSafeInteger(params.now) ||
    !Number.isSafeInteger(params.expiresAt) ||
    params.expiresAt <= params.now ||
    params.expiresAt - params.now > REVIEW_TOKEN_MAX_AGE_MS
  ) {
    throw new Error("Invalid run recovery review token issuance");
  }
  const payload = Buffer.from(
    JSON.stringify([params.callerId, params.expiresAt, fingerprintRunRecoverySnapshot(params.snapshot)]),
  ).toString("base64url");
  const signature = createHmac("sha256", params.secret).update(payload).digest("base64url");
  return `${payload}.${signature}`;
}

/** Recheck this after the lifecycle lock, immediately before any claim transaction. */
export function verifyRunRecoveryReviewToken(params: {
  token: string;
  snapshot: RunRecoverySnapshot;
  callerId: string;
  secret: Buffer;
  now: number;
}): boolean {
  const pieces = params.token.split(".");
  if (pieces.length !== 2 || params.token.length > 1024 || params.secret.length < 32) {
    return false;
  }
  const [payload, signature] = pieces;
  if (!payload || !signature) {
    return false;
  }
  const expected = createHmac("sha256", params.secret).update(payload).digest();
  const supplied = Buffer.from(signature, "base64url");
  if (supplied.length !== expected.length || !timingSafeEqual(supplied, expected)) {
    return false;
  }
  try {
    const parsed: unknown = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    return (
      Array.isArray(parsed) &&
      parsed.length === 3 &&
      parsed[0] === params.callerId &&
      typeof parsed[1] === "number" &&
      Number.isSafeInteger(parsed[1]) &&
      params.now < parsed[1] &&
      parsed[1] - params.now <= REVIEW_TOKEN_MAX_AGE_MS &&
      parsed[2] === fingerprintRunRecoverySnapshot(params.snapshot)
    );
  } catch {
    return false;
  }
}
