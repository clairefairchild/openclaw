import { describe, expect, it } from "vitest";
import {
  evaluateRunRecoveryFence,
  issueRunRecoveryReviewToken,
  verifyRunRecoveryReviewToken,
  type RunRecoveryCustodyFamily,
  type RunRecoveryIdentity,
  type RunRecoverySemanticReview,
  type RunRecoverySnapshot,
} from "./session-run-recovery-fence.js";

const identity: RunRecoveryIdentity = {
  agentId: "main",
  key: "agent:main:main",
  sessionId: "session-1",
  executionId: "execution-1",
  runId: "run-1",
  lifecycleRevision: "revision-2",
  storeRevision: "revision-3",
  ownerEpoch: 4,
  placementGeneration: 5,
  sourceRouteGeneration: "route-6",
  originalInputReceiptId: "input-7",
};
const families: RunRecoveryCustodyFamily[] = [
  "direct",
  "pendingAdmission",
  "followupQueue",
  "laneQueue",
  "pendingInput",
  "descendant",
  "process",
  "worker",
];
function eligible() {
  const snapshot: RunRecoverySnapshot = {
    identity: { ...identity },
    custody: Object.fromEntries(
      families.map((family) => [family, { complete: true, active: false }]),
    ) as RunRecoverySnapshot["custody"],
    terminal: { complete: true, state: "terminal" },
    sourceRoute: { complete: true, authorized: true, generation: "route-6" },
    inputLineage: { complete: true, unchanged: true, receiptId: "input-7" },
    delivery: { complete: true, answered: false },
  };
  const review: RunRecoverySemanticReview = {
    reviewedIdentity: { ...identity },
    originalRequestReviewed: true,
    laterSteeringReviewed: true,
    repliesReviewed: true,
    childOutcomesReviewed: true,
    obligation: "unfinished",
  };
  return { expected: { ...identity }, snapshot, review };
}

describe("run recovery fence", () => {
  it("permits only a terminal, fully evidenced, semantically unfinished obligation", () => {
    expect(evaluateRunRecoveryFence(eligible())).toEqual({ eligible: true });
  });
  it.each(families)("vetoes active and unknown %s custody", (family) => {
    const active = eligible();
    active.snapshot.custody[family].active = true;
    expect(evaluateRunRecoveryFence(active)).toEqual({
      eligible: false,
      reason: `${family}_active`,
    });
    const unknown = eligible();
    unknown.snapshot.custody[family].complete = false;
    expect(evaluateRunRecoveryFence(unknown)).toEqual({
      eligible: false,
      reason: `${family}_unknown`,
    });
  });
  it("vetoes a running ghost and an unanswered-state gap", () => {
    const running = eligible();
    running.snapshot.terminal.state = "running";
    expect(evaluateRunRecoveryFence(running)).toMatchObject({ eligible: false });
    const answerUnknown = eligible();
    answerUnknown.snapshot.delivery.complete = false;
    expect(evaluateRunRecoveryFence(answerUnknown)).toMatchObject({ eligible: false });
  });
  it.each([
    "sessionId",
    "executionId",
    "runId",
    "lifecycleRevision",
    "storeRevision",
    "ownerEpoch",
    "placementGeneration",
    "sourceRouteGeneration",
    "originalInputReceiptId",
  ] as const)("vetoes changed %s", (field) => {
    const changed = eligible();
    (changed.snapshot.identity as unknown as Record<string, unknown>)[field] = "changed";
    expect(evaluateRunRecoveryFence(changed)).toEqual({
      eligible: false,
      reason: "identity_changed",
    });
  });
  it("vetoes completed, awaiting-user, cancelled and unreviewed obligations", () => {
    for (const obligation of ["completed", "awaiting_user", "cancelled", "unknown"] as const) {
      const input = eligible();
      input.review.obligation = obligation;
      expect(evaluateRunRecoveryFence(input)).toMatchObject({ eligible: false });
    }
    const input = eligible();
    input.review.repliesReviewed = false;
    expect(evaluateRunRecoveryFence(input)).toMatchObject({ eligible: false });
  });
  it("vetoes changed route, new input, final answer and stale review", () => {
    for (const mutate of [
      (input: ReturnType<typeof eligible>) => { input.snapshot.sourceRoute.authorized = false; },
      (input: ReturnType<typeof eligible>) => { input.snapshot.inputLineage.unchanged = false; },
      (input: ReturnType<typeof eligible>) => { input.snapshot.delivery.answered = true; },
      (input: ReturnType<typeof eligible>) => { input.review.reviewedIdentity.ownerEpoch++; },
    ]) {
      const input = eligible();
      mutate(input);
      expect(evaluateRunRecoveryFence(input)).toMatchObject({ eligible: false });
    }
  });
  it("binds a short-lived review token to caller and complete metadata generation", () => {
    const secret = Buffer.alloc(32, 7);
    const snapshot = eligible().snapshot;
    const token = issueRunRecoveryReviewToken({
      snapshot, callerId: "reviewer-1", secret, now: 1000, expiresAt: 61_000,
    });
    const verify = (candidate: RunRecoverySnapshot, callerId = "reviewer-1", now = 1000) =>
      verifyRunRecoveryReviewToken({ token, snapshot: candidate, callerId, secret, now });
    expect(verify(snapshot)).toBe(true);
    expect(verify(snapshot, "reviewer-2")).toBe(false);
    expect(verify(snapshot, "reviewer-1", 61_000)).toBe(false);
    const spawned = eligible().snapshot;
    spawned.custody.descendant.active = true;
    expect(verify(spawned)).toBe(false);
    const rebound = eligible().snapshot;
    rebound.sourceRoute.generation = "new-route";
    expect(verify(rebound)).toBe(false);
    expect(verifyRunRecoveryReviewToken({
      token: `${token.slice(0, -1)}${token.endsWith("x") ? "y" : "x"}`, snapshot, callerId: "reviewer-1", secret, now: 1000,
    })).toBe(false);
  });
});
