/**
 * Stage 1 durable admission fence for Gateway recovery. This is deliberately
 * separate from agent databases: lane tasks can be unmarked or cross-agent.
 * Producers must reserve here before publishing to either in-memory queue.
 * No producer is cut over yet, so production claim intent is always held.
 */
import { randomUUID } from "node:crypto";
import type { PersistedUserTurnMessage } from "../sessions/user-turn-transcript.types.js";
import { MAX_PAYLOAD_BYTES } from "../gateway/server-constants.js";
import type { SessionRunRecoveryClaimExpected } from "../config/sessions/session-run-recovery-claim.js";
import { existsSync, mkdirSync } from "node:fs";
import path from "node:path";
import type { DatabaseSync } from "node:sqlite";
import { resolveStateDir } from "../config/paths.js";
import { openNodeSqliteDatabase } from "../infra/node-sqlite.js";
import { assertSqliteIntegrity } from "../infra/sqlite-integrity.js";
import { runSqliteImmediateTransactionSync } from "../infra/sqlite-transaction.js";

export type RecoveryQueueKind = "followup" | "command-lane";
export type RecoveryWorkScope =
  | { kind: "exact"; agentId: string; sessionId: string }
  | { kind: "unmarked" };
export type RecoveryQueueReservation = {
  incarnation: string;
  workId: string;
  queue: RecoveryQueueKind;
  scope: RecoveryWorkScope;
};
export type RecoveryQueueWorkReadback = {
  workId: string;
  queue: RecoveryQueueKind;
  scope: RecoveryWorkScope;
  state: "queued" | "in_flight" | "settled";
  terminalReceiptId: string | null;
};
export type RecoveryClaimIdentity = {
  incarnation: string;
  agentId: string;
  sessionId: string;
  obligationId: string;
  generation: string;
};
export type RecoveryClaimIntentResult =
  | { kind: "prepared"; intentId: string; repeated: boolean }
  | { kind: "held"; reason: "queue_not_cut_over" | "unresolved_work" | "invalidated" | "changed_generation" };

type MetaRow = { incarnation: string };
type ReservationRow = {
  work_id: string;
  queue_kind: RecoveryQueueKind;
  scope_kind: "exact" | "unmarked";
  agent_id: string | null;
  session_id: string | null;
  state: "queued" | "in_flight" | "settled";
  terminal_receipt_id: string | null;
};
type IntentRow = { intent_id: string; generation: string; state: "prepared" | "invalidated" };
export type RecoveryQueueWriteAuthority = {
  assertCurrent?(): void;
  admitTransaction?(): void;
  admitCommit?(): void;
};

function writeTransaction<T>(db: DatabaseSync, authority: RecoveryQueueWriteAuthority | undefined, operation: () => T): T {
  authority?.assertCurrent?.();
  return runSqliteImmediateTransactionSync(db, () => {
    authority?.admitTransaction?.();
    authority?.assertCurrent?.();
    return operation();
  }, {
    withCommit: authority ? (commit) => {
      authority.admitCommit?.();
      authority.assertCurrent?.();
      commit();
    } : undefined,
  });
}

/** A global file, not an agent DB. Never point this at an incognito agent store. */
export function resolveRunRecoveryQueueCoordinatorPath(env: NodeJS.ProcessEnv = process.env): string {
  return path.join(resolveStateDir(env), "run-recovery-queue-coordinator.sqlite");
}

/** Caller owns the returned connection. Opening does not assert producer cutover. */
export function openRunRecoveryQueueCoordinator(
  pathname = resolveRunRecoveryQueueCoordinatorPath(), authority?: RecoveryQueueWriteAuthority,
): DatabaseSync {
  authority?.assertCurrent?.();
  const existing = existsSync(pathname);
  mkdirSync(path.dirname(pathname), { recursive: true, mode: 0o700 });
  const db = openNodeSqliteDatabase(pathname);
  try {
    db.exec("PRAGMA busy_timeout = 5000;"); // sqlite-allow-raw -- Connection pragma.
    if (existing) {
      // Never recreate a missing identity or table in an existing ledger.
      assertSqliteIntegrity(db, "run recovery queue coordinator");
      assertRecoveryQueueSchema(db);
      readRecoveryQueueIncarnation(db);
      // Versionless additive migration: preserve every existing reservation and intent.
      ensureRunRecoveryQueueCoordinatorSchema(db, authority);
    } else {
      db.exec("PRAGMA journal_mode = WAL;"); // sqlite-allow-raw -- New ledger connection policy.
      ensureRunRecoveryQueueCoordinatorSchema(db, authority);
      assertSqliteIntegrity(db, "run recovery queue coordinator");
    }
    readRecoveryQueueIncarnation(db);
    db.exec("PRAGMA journal_mode = WAL;"); // sqlite-allow-raw -- Only after integrity admission.
    authority?.assertCurrent?.();
    return db;
  } catch (error) {
    db.close();
    throw error;
  }
}

const REQUIRED_TABLES = [
  "recovery_queue_meta", "recovery_queue_cutovers", "recovery_queue_reservations", "recovery_claim_intents",
] as const;

function assertRecoveryQueueSchema(db: DatabaseSync): void {
  const rows = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'recovery_%'")
    .all() as Array<{ name: string }>;
  const names = new Set(rows.map((row) => row.name));
  if (REQUIRED_TABLES.some((name) => !names.has(name))) {
    throw new Error("Recovery queue coordinator schema is missing or unknown");
  }
}

export function ensureRunRecoveryQueueCoordinatorSchema(db: DatabaseSync, authority?: RecoveryQueueWriteAuthority): void {
  writeTransaction(db, authority, () => {
    db.exec(`
      CREATE TABLE IF NOT EXISTS recovery_queue_meta (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        incarnation TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS recovery_queue_cutovers (
        queue_kind TEXT PRIMARY KEY CHECK (queue_kind IN ('followup', 'command-lane')),
        completed_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS recovery_queue_reservations (
        work_id TEXT PRIMARY KEY,
        queue_kind TEXT NOT NULL CHECK (queue_kind IN ('followup', 'command-lane')),
        scope_kind TEXT NOT NULL CHECK (scope_kind IN ('exact', 'unmarked')),
        agent_id TEXT,
        session_id TEXT,
        state TEXT NOT NULL CHECK (state IN ('queued', 'in_flight', 'settled')),
        terminal_receipt_id TEXT,
        reserved_at INTEGER NOT NULL,
        started_at INTEGER,
        settled_at INTEGER,
        CHECK ((scope_kind = 'unmarked' AND agent_id IS NULL AND session_id IS NULL) OR
               (scope_kind = 'exact' AND agent_id IS NOT NULL AND session_id IS NOT NULL))
      );
      CREATE INDEX IF NOT EXISTS recovery_queue_unresolved ON recovery_queue_reservations
        (state, scope_kind, agent_id, session_id);
      CREATE TABLE IF NOT EXISTS recovery_claim_outbox (
        claim_id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        obligation_id TEXT NOT NULL,
        expected_json TEXT NOT NULL,
        message_json TEXT NOT NULL,
        route_json TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('pending', 'handed_off', 'needs_reconciliation')),
        created_at INTEGER NOT NULL,
        handed_off_at INTEGER,
        handoff_input_id TEXT,
        UNIQUE (agent_id, session_id, idempotency_key),
        UNIQUE (agent_id, session_id, obligation_id)
      ) STRICT;
      CREATE TABLE IF NOT EXISTS recovery_claim_intents (
        intent_id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        obligation_id TEXT NOT NULL,
        generation TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('prepared', 'invalidated')),
        prepared_at INTEGER NOT NULL,
        invalidated_at INTEGER,
        UNIQUE (agent_id, session_id, obligation_id)
      );
    `); // sqlite-allow-raw -- Additive coordinator schema, no application rows.
    db.prepare("INSERT OR IGNORE INTO recovery_queue_meta (singleton, incarnation) VALUES (1, ?)")
      .run(randomUUID()); // sqlite-allow-raw -- Parameterized identity initialization.
  });
}

export function readRecoveryQueueIncarnation(db: DatabaseSync): string {
  const rows = db.prepare("SELECT incarnation FROM recovery_queue_meta").all() as MetaRow[]; // sqlite-allow-raw -- Exact singleton identity.
  const [row] = rows;
  if (!row || rows.length !== 1 || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(row.incarnation)) {
    throw new Error("Recovery queue coordinator identity is missing or invalid");
  }
  return row.incarnation;
}

function assertRecoveryQueueLedgerKnown(db: DatabaseSync): void {
  const invalidCutover = db.prepare("SELECT 1 FROM recovery_queue_cutovers WHERE queue_kind IS NULL OR queue_kind NOT IN ('followup', 'command-lane') LIMIT 1").get();
  const invalidReservation = db.prepare(`SELECT 1 FROM recovery_queue_reservations WHERE
    work_id IS NULL OR trim(work_id) = ''
    OR queue_kind IS NULL OR queue_kind NOT IN ('followup', 'command-lane')
    OR state IS NULL OR state NOT IN ('queued', 'in_flight', 'settled')
    OR scope_kind IS NULL OR scope_kind NOT IN ('exact', 'unmarked')
    OR (scope_kind = 'unmarked' AND (agent_id IS NOT NULL OR session_id IS NOT NULL))
    OR (scope_kind = 'exact' AND (agent_id IS NULL OR trim(agent_id) = '' OR session_id IS NULL OR trim(session_id) = ''))
    OR (state = 'settled' AND (terminal_receipt_id IS NULL OR trim(terminal_receipt_id) = ''))
    OR (state <> 'settled' AND terminal_receipt_id IS NOT NULL) LIMIT 1`).get();
  const invalidOutbox = db.prepare(`SELECT 1 FROM recovery_claim_outbox WHERE
    trim(claim_id) = '' OR trim(agent_id) = '' OR trim(session_id) = '' OR
    trim(idempotency_key) = '' OR trim(obligation_id) = '' OR state NOT IN ('pending', 'handed_off', 'needs_reconciliation') OR
    (state = 'handed_off' AND (handed_off_at IS NULL OR trim(handoff_input_id) = '')) LIMIT 1`).get();
  const invalidIntent = db.prepare(`SELECT 1 FROM recovery_claim_intents WHERE
    state IS NULL OR state NOT IN ('prepared', 'invalidated')
    OR trim(agent_id) = '' OR trim(session_id) = '' OR trim(obligation_id) = ''
    OR trim(generation) = '' LIMIT 1`).get();
  if (invalidCutover || invalidReservation || invalidIntent || invalidOutbox) {
    throw new Error("Recovery queue coordinator contains unknown custody state");
  }
}

function assertIncarnation(db: DatabaseSync, expected: string): void {
  if (!expected || readRecoveryQueueIncarnation(db) !== expected) {
    throw new Error("Recovery queue coordinator incarnation changed");
  }
  assertRecoveryQueueLedgerKnown(db);
}

function assertIdentity(value: string, label: string): void {
  if (!value?.trim()) {
    throw new Error(`Recovery queue ${label} is required`);
  }
}

function readReservation(db: DatabaseSync, workId: string): ReservationRow | undefined {
  return db.prepare("SELECT work_id, queue_kind, scope_kind, agent_id, session_id, state, terminal_receipt_id FROM recovery_queue_reservations WHERE work_id = ?")
    .get(workId) as ReservationRow | undefined; // sqlite-allow-raw -- Exact reservation lookup.
}

/** Exact-ID readback for an uncertain write or a later owner reconciliation.
 * Absence is only an exact ledger fact, never proof that pre-cutover work vanished.
 */
export function readRecoveryQueueWork(
  db: DatabaseSync, incarnation: string, workId: string,
): RecoveryQueueWorkReadback | undefined {
  assertIdentity(workId, "work ID");
  assertIncarnation(db, incarnation);
  const row = readReservation(db, workId);
  if (!row) return undefined;
  if (row.scope_kind === "exact" && (!row.agent_id || !row.session_id)) {
    throw new Error("Recovery queue exact scope is incomplete");
  }
  return {
    workId: row.work_id,
    queue: row.queue_kind,
    scope: row.scope_kind === "unmarked"
      ? { kind: "unmarked" }
      : { kind: "exact", agentId: row.agent_id!, sessionId: row.session_id! },
    state: row.state,
    terminalReceiptId: row.terminal_receipt_id,
  };
}

/** Reserve before queue publication. Repeated exact identity is safe; collision fails. */
export function reserveRecoveryQueueWork(db: DatabaseSync, input: RecoveryQueueReservation, authority?: RecoveryQueueWriteAuthority): "reserved" | "repeated" {
  assertIdentity(input.workId, "work ID");
  if (input.scope.kind === "exact") {
    assertIdentity(input.scope.agentId, "agent ID");
    assertIdentity(input.scope.sessionId, "session ID");
  }
  return writeTransaction<"reserved" | "repeated">(db, authority, () => {
    assertIncarnation(db, input.incarnation);
    const existing = readReservation(db, input.workId);
    const agentId = input.scope.kind === "exact" ? input.scope.agentId : null;
    const sessionId = input.scope.kind === "exact" ? input.scope.sessionId : null;
    if (existing) {
      if (existing.queue_kind !== input.queue || existing.scope_kind !== input.scope.kind ||
          existing.agent_id !== agentId || existing.session_id !== sessionId) {
        throw new Error("Recovery queue work ID collision");
      }
      return "repeated";
    }
    db.prepare(`INSERT INTO recovery_queue_reservations
      (work_id, queue_kind, scope_kind, agent_id, session_id, state, reserved_at)
      VALUES (?, ?, ?, ?, ?, 'queued', ?)`)
      .run(input.workId, input.queue, input.scope.kind, agentId, sessionId, Date.now()); // sqlite-allow-raw -- Parameterized reservation.
    // A new user task must be accepted. Invalidate an earlier recovery intent
    // atomically rather than blocking or losing that task.
    if (input.scope.kind === "unmarked") {
      db.prepare("UPDATE recovery_claim_intents SET state = 'invalidated', invalidated_at = ? WHERE state = 'prepared'")
        .run(Date.now()); // sqlite-allow-raw -- Global conservative invalidation.
    } else {
      db.prepare(`UPDATE recovery_claim_intents SET state = 'invalidated', invalidated_at = ?
        WHERE state = 'prepared' AND agent_id = ? AND session_id = ?`)
        .run(Date.now(), agentId, sessionId); // sqlite-allow-raw -- Exact invalidation.
    }
    if (input.scope.kind === "unmarked") {
      db.prepare("UPDATE recovery_claim_outbox SET state = 'needs_reconciliation' WHERE state IN ('pending', 'handed_off')")
        .run(); // sqlite-allow-raw -- New global work revokes every recovery handoff.
    } else {
      db.prepare(`UPDATE recovery_claim_outbox SET state = 'needs_reconciliation'
        WHERE state IN ('pending', 'handed_off') AND agent_id = ? AND session_id = ?`)
        .run(agentId, sessionId); // sqlite-allow-raw -- Exact work revokes this session's handoff.
    }
    return "reserved";
  });
}

/** The queue owner calls this immediately before starting its task. */
export function startRecoveryQueueWork(db: DatabaseSync, incarnation: string, workId: string, authority?: RecoveryQueueWriteAuthority): "started" | "repeated" {
  return writeTransaction<"started" | "repeated">(db, authority, () => {
    assertIncarnation(db, incarnation);
    const row = readReservation(db, workId);
    if (!row || row.state === "settled") {
      throw new Error("Recovery queue work is absent or settled");
    }
    if (row.state === "in_flight") {
      return "repeated";
    }
    db.prepare("UPDATE recovery_queue_reservations SET state = 'in_flight', started_at = ? WHERE work_id = ?")
      .run(Date.now(), workId); // sqlite-allow-raw -- Exact state transition.
    return "started";
  });
}

/** A terminal custody receipt is required. Mere dequeue or process death is not settlement. */
export function settleRecoveryQueueWork(
  db: DatabaseSync, incarnation: string, workId: string, terminalReceiptId: string, authority?: RecoveryQueueWriteAuthority,
): "settled" | "repeated" {
  assertIdentity(terminalReceiptId, "terminal receipt");
  return writeTransaction<"settled" | "repeated">(db, authority, () => {
    assertIncarnation(db, incarnation);
    const row = readReservation(db, workId);
    if (!row) {
      throw new Error("Recovery queue reservation is absent");
    }
    if (row.state === "settled") {
      if (row.terminal_receipt_id !== terminalReceiptId) {
        throw new Error("Recovery queue settlement receipt changed");
      }
      return "repeated";
    }
    db.prepare(`UPDATE recovery_queue_reservations
      SET state = 'settled', terminal_receipt_id = ?, settled_at = ? WHERE work_id = ?`)
      .run(terminalReceiptId, Date.now(), workId); // sqlite-allow-raw -- Exact terminal settlement.
    return "settled";
  });
}

/**
 * Internal migration gate. It may be marked only after a producer cutover has
 * reserved every publish/start path and reconciled pre-existing queue work.
 * No production caller marks either kind in Stage 1.
 */
export function markRecoveryQueueProducerCutOver(
  db: DatabaseSync, incarnation: string, queue: RecoveryQueueKind, authority?: RecoveryQueueWriteAuthority,
): void {
  writeTransaction(db, authority, () => {
    assertIncarnation(db, incarnation);
    db.prepare("INSERT OR IGNORE INTO recovery_queue_cutovers (queue_kind, completed_at) VALUES (?, ?)")
      .run(queue, Date.now()); // sqlite-allow-raw -- Migration gate.
  });
}

/** Intent and reservation absence are compared under one BEGIN IMMEDIATE lock. */
export function prepareRecoveryClaimIntent(
  db: DatabaseSync, identity: RecoveryClaimIdentity, authority?: RecoveryQueueWriteAuthority,
): RecoveryClaimIntentResult {
  for (const [label, value] of Object.entries(identity)) {
    assertIdentity(value, label);
  }
  return writeTransaction<RecoveryClaimIntentResult>(db, authority, () => {
    assertIncarnation(db, identity.incarnation);
    const cutovers = db.prepare("SELECT COUNT(*) AS count FROM recovery_queue_cutovers WHERE queue_kind IN ('followup', 'command-lane')")
      .get() as { count: number }; // sqlite-allow-raw -- Both producer families required.
    if (cutovers.count !== 2) {
      return { kind: "held", reason: "queue_not_cut_over" };
    }
    const unresolved = db.prepare(`SELECT 1 FROM recovery_queue_reservations
      WHERE state <> 'settled' AND (scope_kind = 'unmarked' OR
        (agent_id = ? AND session_id = ?)) LIMIT 1`)
      .get(identity.agentId, identity.sessionId); // sqlite-allow-raw -- Exact plus global conservative veto.
    if (unresolved) {
      return { kind: "held", reason: "unresolved_work" };
    }
    const existing = db.prepare(`SELECT intent_id, generation, state FROM recovery_claim_intents
      WHERE agent_id = ? AND session_id = ? AND obligation_id = ?`)
      .get(identity.agentId, identity.sessionId, identity.obligationId) as IntentRow | undefined; // sqlite-allow-raw -- Exact obligation lookup.
    if (existing) {
      if (existing.generation !== identity.generation) {
        return { kind: "held", reason: "changed_generation" };
      }
      return existing.state === "prepared"
        ? { kind: "prepared", intentId: existing.intent_id, repeated: true }
        : { kind: "held", reason: "invalidated" };
    }
    const intentId = randomUUID();
    db.prepare(`INSERT INTO recovery_claim_intents
      (intent_id, agent_id, session_id, obligation_id, generation, state, prepared_at)
      VALUES (?, ?, ?, ?, ?, 'prepared', ?)`)
      .run(intentId, identity.agentId, identity.sessionId, identity.obligationId,
        identity.generation, Date.now()); // sqlite-allow-raw -- Atomic claim intent.
    return { kind: "prepared", intentId, repeated: false };
  });
}

/** The coordinator is the sole claim and outbox owner. Agent-local pending input is
 * a delivery projection, never evidence that a second claim may be created. */
export type RecoveryClaimOutboxInput = {
  incarnation: string;
  expected: SessionRunRecoveryClaimExpected;
  idempotencyKey: string;
  message: PersistedUserTurnMessage;
  /** The exact original internal delivery route, not a newly selected channel. */
  route: { kind: "internal"; [key: string]: unknown };
};
export type RecoveryClaimOutboxRow = {
  claimId: string;
  expected: SessionRunRecoveryClaimExpected;
  idempotencyKey: string;
  message: PersistedUserTurnMessage;
  route: RecoveryClaimOutboxInput["route"];
  state: "pending" | "handed_off" | "needs_reconciliation";
  handoffInputId?: string;
};
type StoredOutboxRow = {
  claim_id: string;
  agent_id: string;
  session_id: string;
  idempotency_key: string;
  obligation_id: string;
  expected_json: string;
  message_json: string;
  route_json: string;
  state: RecoveryClaimOutboxRow["state"];
  handoff_input_id: string | null;
};
function outboxRow(row: StoredOutboxRow): RecoveryClaimOutboxRow {
  return { claimId: row.claim_id,
    expected: JSON.parse(row.expected_json) as SessionRunRecoveryClaimExpected,
    idempotencyKey: row.idempotency_key,
    message: JSON.parse(row.message_json) as PersistedUserTurnMessage,
    route: JSON.parse(row.route_json) as RecoveryClaimOutboxInput["route"],
    state: row.state, ...(row.handoff_input_id ? { handoffInputId: row.handoff_input_id } : {}) };
}
function readStoredOutbox(db: DatabaseSync, agentId: string, sessionId: string, key: string) {
  return db.prepare(`SELECT * FROM recovery_claim_outbox
    WHERE agent_id = ? AND session_id = ? AND idempotency_key = ?`)
    .get(agentId, sessionId, key) as StoredOutboxRow | undefined; // sqlite-allow-raw -- Immutable exact-key readback.
}
/** An ambiguous acknowledgment is reconciled by this immutable key; it is never replayed blind. */
export function readRecoveryClaimOutbox(db: DatabaseSync, agentId: string, sessionId: string, key: string): RecoveryClaimOutboxRow | undefined {
  const row = readStoredOutbox(db, agentId, sessionId, key);
  if (!row) return undefined;
  assertRecoveryQueueLedgerKnown(db);
  return outboxRow(row);
}

/** The caller's authority callback must re-read the exact source, semantic review,
 * route, and caller at both worker grants, including the one just before COMMIT.
 * Queue custody and outbox acceptance are compared under this single WAL lock. */
export function claimRecoveryOutbox(
  db: DatabaseSync, input: RecoveryClaimOutboxInput,
  authority: RecoveryQueueWriteAuthority & { assertClaimCurrent(): void },
): { kind: "prepared"; row: RecoveryClaimOutboxRow; repeated: boolean } |
   { kind: "held"; reason: "queue_not_cut_over" | "unresolved_work" | "existing_intent" | "existing_claim" | "invalidated" } {
  const { expected } = input;
  for (const [label, value] of Object.entries(expected)) {
    if (label !== "ownerEpoch") {
      if (typeof value !== "string") throw new Error(`Recovery queue ${label} is required`);
      assertIdentity(value, label);
    }
  }
  assertIdentity(input.idempotencyKey, "idempotency key");
  if (input.message.role !== "user" || input.message.idempotencyKey !== input.idempotencyKey ||
      input.route.kind !== "internal" || !Number.isSafeInteger(expected.ownerEpoch) || expected.ownerEpoch < 0 ||
      Buffer.byteLength(JSON.stringify(input.message), "utf8") > MAX_PAYLOAD_BYTES) {
    throw new Error("Recovery outbox has invalid input or route identity");
  }
  // Host grants are the current-authority boundary; these are not stale token checks.
  const checked = { ...authority, assertCurrent: () => {
    authority.assertCurrent?.();
    authority.assertClaimCurrent();
  } };
  return writeTransaction(db, checked, () => {
    assertIncarnation(db, input.incarnation);
    const stored = readStoredOutbox(db, expected.agentId, expected.sessionId, input.idempotencyKey) ??
      db.prepare(`SELECT * FROM recovery_claim_outbox WHERE agent_id = ? AND session_id = ?
        AND obligation_id = ?`).get(expected.agentId, expected.sessionId,
          expected.originalInputReceiptId) as StoredOutboxRow | undefined;
    if (stored) {
      const same = stored.expected_json === JSON.stringify(expected) &&
        stored.message_json === JSON.stringify(input.message) &&
        stored.route_json === JSON.stringify(input.route);
      if (!same) return { kind: "held", reason: "existing_claim" };
      if (stored.state === "needs_reconciliation") return { kind: "held", reason: "invalidated" };
      return { kind: "prepared", row: outboxRow(stored), repeated: true };
    }
    const cutovers = db.prepare("SELECT COUNT(*) AS count FROM recovery_queue_cutovers WHERE queue_kind IN ('followup', 'command-lane')")
      .get() as { count: number }; // sqlite-allow-raw -- Both producer families required.
    if (cutovers.count !== 2) return { kind: "held", reason: "queue_not_cut_over" };
    if (db.prepare(`SELECT 1 FROM recovery_queue_reservations WHERE state <> 'settled' AND
      (scope_kind = 'unmarked' OR (agent_id = ? AND session_id = ?)) LIMIT 1`)
      .get(expected.agentId, expected.sessionId)) return { kind: "held", reason: "unresolved_work" };
    // Stage-1 intents have no accepted input. Never silently adopt one as an outbox.
    if (db.prepare(`SELECT 1 FROM recovery_claim_intents WHERE agent_id = ? AND session_id = ?
      AND obligation_id = ? LIMIT 1`).get(expected.agentId, expected.sessionId,
        expected.originalInputReceiptId)) return { kind: "held", reason: "existing_intent" };
    const claimId = randomUUID();
    db.prepare(`INSERT INTO recovery_claim_outbox
      (claim_id, agent_id, session_id, idempotency_key, obligation_id, expected_json, message_json,
       route_json, state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)`)
      .run(claimId, expected.agentId, expected.sessionId, input.idempotencyKey,
        expected.originalInputReceiptId, JSON.stringify(expected), JSON.stringify(input.message), JSON.stringify(input.route), Date.now()); // sqlite-allow-raw -- Single-owner claim and accepted input.
    return { kind: "prepared", row: { claimId, expected, idempotencyKey: input.idempotencyKey,
      message: input.message, route: input.route, state: "pending" }, repeated: false };
  });
}

/** Called only after exact agent-local pending-input readback. This receipt cannot send. */
export function recordRecoveryClaimHandoff(
  db: DatabaseSync, incarnation: string, agentId: string, sessionId: string,
  idempotencyKey: string, claimId: string, inputId: string, authority?: RecoveryQueueWriteAuthority,
): RecoveryClaimOutboxRow {
  assertIdentity(inputId, "handoff input ID");
  return writeTransaction(db, authority, () => {
    assertIncarnation(db, incarnation);
    const stored = readStoredOutbox(db, agentId, sessionId, idempotencyKey);
    if (!stored || stored.claim_id !== claimId ||
        (stored.handoff_input_id && stored.handoff_input_id !== inputId)) {
      throw new Error("Recovery handoff claim changed");
    }
    if (stored.state === "pending") {
      db.prepare(`UPDATE recovery_claim_outbox SET state = 'handed_off', handed_off_at = ?, handoff_input_id = ?
        WHERE claim_id = ? AND state = 'pending'`).run(Date.now(), inputId, claimId); // sqlite-allow-raw -- Exact handoff acknowledgment.
    }
    return stored.state === "pending"
      ? { ...outboxRow(stored), state: "handed_off", handoffInputId: inputId }
      : outboxRow(stored);
  });
}
