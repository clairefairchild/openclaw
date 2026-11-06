import type { Static } from "typebox";
import { Type } from "typebox";
import { closedObject } from "./closed-object.js";
import { NonEmptyString } from "./primitives.js";

/** Metadata only. Unknown means that the Gateway cannot prove absence of custody. */
export const SessionsRecoveryInspectParamsSchema = closedObject({
  agentId: NonEmptyString,
  key: NonEmptyString,
  sessionId: Type.Optional(NonEmptyString),
  runId: Type.Optional(NonEmptyString),
});

const EvidenceSchema = Type.Union([
  closedObject({ complete: Type.Literal(false), state: Type.Literal("unknown") }),
  closedObject({ complete: Type.Literal(true),
    state: Type.Union([Type.Literal("active"), Type.Literal("clear")]) }),
]);

export const SessionsRecoveryInspectResultSchema = closedObject({
  agentId: NonEmptyString,
  key: NonEmptyString,
  sessionId: NonEmptyString,
  lifecycleRevision: Type.Optional(NonEmptyString),
  runId: Type.Optional(NonEmptyString),
  executionId: Type.Optional(NonEmptyString),
  storeRevision: Type.Optional(NonEmptyString),
  ownerEpoch: Type.Optional(Type.Integer({ minimum: 0 })),
  ownerLifecycleGeneration: Type.Optional(NonEmptyString),
  placementGeneration: Type.Optional(Type.Integer({ minimum: 0 })),
  sourceRouteGeneration: Type.Optional(NonEmptyString),
  originalInputReceiptId: Type.Optional(NonEmptyString),
  inputGeneration: Type.Optional(NonEmptyString),
  observedSessionStatus: Type.Optional(NonEmptyString),
  metadataCompleteness: closedObject({
    identity: Type.Literal(true),
    execution: Type.Boolean(),
    store: Type.Boolean(),
    owner: Type.Boolean(),
    placement: Type.Boolean(),
    route: Type.Boolean(),
  }),
  evidence: closedObject({
    direct: EvidenceSchema,
    pendingAdmission: EvidenceSchema,
    followupQueue: EvidenceSchema,
    laneQueue: EvidenceSchema,
    pendingInput: EvidenceSchema,
    descendant: EvidenceSchema,
    process: EvidenceSchema,
    worker: EvidenceSchema,
    terminal: EvidenceSchema,
    sourceRoute: EvidenceSchema,
    inputLineage: EvidenceSchema,
    delivery: EvidenceSchema,
    progress: EvidenceSchema,
  }),
  reviewToken: Type.Optional(NonEmptyString),
});

export type SessionsRecoveryInspectParams = Static<typeof SessionsRecoveryInspectParamsSchema>;
export type SessionsRecoveryInspectResult = Static<typeof SessionsRecoveryInspectResultSchema>;
