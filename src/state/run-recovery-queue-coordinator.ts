/**
 * Stage 1 durable admission fence for Gateway recovery. This is deliberately
 * separate from agent databases: lane tasks can be unmarked or cross-agent.
 * Producers must reserve here before publishing to either in-memory queue.
 * No producer is cut over yet, so production claim intent is always held.
 */
import { randomUUID } from "node:crypto";
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
  const invalidIntent = db.prepare(`SELECT 1 FROM recovery_claim_intents WHERE
    state IS NULL OR state NOT IN ('prepared', 'invalidated')
    OR trim(agent_id) = '' OR trim(session_id) = '' OR trim(obligation_id) = ''
    OR trim(generation) = '' LIMIT 1`).get();
  if (invalidCutover || invalidReservation || invalidIntent) {
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
