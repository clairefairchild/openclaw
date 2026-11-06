import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import {
  claimRecoveryOutbox,
  readRecoveryClaimOutbox,
  recordRecoveryClaimHandoff,
  markRecoveryQueueProducerCutOver,
  openRunRecoveryQueueCoordinator,
  prepareRecoveryClaimIntent,
  readRecoveryQueueIncarnation,
  reserveRecoveryQueueWork,
  settleRecoveryQueueWork,
  startRecoveryQueueWork,
} from "./run-recovery-queue-coordinator.js";

const roots: string[] = [];
const handles: DatabaseSync[] = [];
function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-coordinator-"));
  roots.push(root);
  const pathname = path.join(root, "coordinator.sqlite");
  const db = openRunRecoveryQueueCoordinator(pathname);
  handles.push(db);
  const incarnation = readRecoveryQueueIncarnation(db);
  const claim = { incarnation, agentId: "agent-a", sessionId: "session-a",
    obligationId: "input-1", generation: "generation-1" };
  const exact = (workId: string, agentId = "agent-a") => ({ incarnation, workId,
    queue: "followup" as const, scope: { kind: "exact" as const, agentId, sessionId: "session-a" } });
  const unmarked = (workId: string) => ({ incarnation, workId,
    queue: "command-lane" as const, scope: { kind: "unmarked" as const } });
  const cutOver = () => {
    markRecoveryQueueProducerCutOver(db, incarnation, "followup");
    markRecoveryQueueProducerCutOver(db, incarnation, "command-lane");
  };
  return { root, pathname, db, incarnation, claim, exact, unmarked, cutOver };
}
afterEach(() => {
  for (const handle of handles.splice(0)) handle.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("durable recovery queue admission", () => {
  it("fails closed without both producer cutovers, even for an empty database", () => {
    const f = fixture();
    expect(prepareRecoveryClaimIntent(f.db, f.claim)).toEqual({ kind: "held", reason: "queue_not_cut_over" });
    markRecoveryQueueProducerCutOver(f.db, f.incarnation, "followup");
    expect(prepareRecoveryClaimIntent(f.db, f.claim)).toEqual({ kind: "held", reason: "queue_not_cut_over" });
  });

  it("holds exact and unmarked work, including after restart, and settles idempotently", () => {
    const f = fixture();
    f.cutOver();
    expect(reserveRecoveryQueueWork(f.db, f.exact("work-1"))).toBe("reserved");
    expect(reserveRecoveryQueueWork(f.db, f.exact("work-1"))).toBe("repeated");
    expect(prepareRecoveryClaimIntent(f.db, f.claim)).toEqual({ kind: "held", reason: "unresolved_work" });
    expect(startRecoveryQueueWork(f.db, f.incarnation, "work-1")).toBe("started");
    expect(startRecoveryQueueWork(f.db, f.incarnation, "work-1")).toBe("repeated");
    const reopened = openRunRecoveryQueueCoordinator(f.pathname);
    handles.push(reopened);
    expect(readRecoveryQueueIncarnation(reopened)).toBe(f.incarnation);
    expect(prepareRecoveryClaimIntent(reopened, f.claim)).toEqual({ kind: "held", reason: "unresolved_work" });
    expect(settleRecoveryQueueWork(reopened, f.incarnation, "work-1", "terminal-1")).toBe("settled");
    expect(settleRecoveryQueueWork(reopened, f.incarnation, "work-1", "terminal-1")).toBe("repeated");
    expect(() => settleRecoveryQueueWork(reopened, f.incarnation, "work-1", "different"))
      .toThrow("receipt changed");
    expect(reserveRecoveryQueueWork(reopened, f.unmarked("global-1"))).toBe("reserved");
    expect(prepareRecoveryClaimIntent(reopened, { ...f.claim, agentId: "other" }))
      .toEqual({ kind: "held", reason: "unresolved_work" });
    expect(settleRecoveryQueueWork(reopened, f.incarnation, "global-1", "terminal-global"))
      .toBe("settled");
    expect(prepareRecoveryClaimIntent(reopened, f.claim)).toMatchObject({ kind: "prepared", repeated: false });
  });

  it("rejects wrong incarnation and work-ID collision without mutating state", () => {
    const f = fixture();
    expect(() => reserveRecoveryQueueWork(f.db, { ...f.exact("work-1"), incarnation: "wrong" }))
      .toThrow("incarnation changed");
    expect(reserveRecoveryQueueWork(f.db, f.exact("work-1"))).toBe("reserved");
    expect(() => reserveRecoveryQueueWork(f.db, f.exact("work-1", "other")))
      .toThrow("work ID collision");
    expect(() => startRecoveryQueueWork(f.db, "wrong", "work-1"))
      .toThrow("incarnation changed");
    expect(() => settleRecoveryQueueWork(f.db, "wrong", "work-1", "terminal"))
      .toThrow("incarnation changed");
  });

  it("atomically invalidates a claim intent when later user work is accepted", () => {
    const f = fixture();
    f.cutOver();
    const first = prepareRecoveryClaimIntent(f.db, f.claim);
    expect(first).toMatchObject({ kind: "prepared", repeated: false });
    expect(prepareRecoveryClaimIntent(f.db, f.claim)).toEqual({ ...first, repeated: true });
    expect(reserveRecoveryQueueWork(f.db, f.exact("later-user-work"))).toBe("reserved");
    expect(prepareRecoveryClaimIntent(f.db, f.claim)).toEqual({ kind: "held", reason: "unresolved_work" });
    expect(settleRecoveryQueueWork(f.db, f.incarnation, "later-user-work", "user-terminal"))
      .toBe("settled");
    expect(prepareRecoveryClaimIntent(f.db, f.claim)).toEqual({ kind: "held", reason: "invalidated" });
    expect(prepareRecoveryClaimIntent(f.db, { ...f.claim, generation: "new" }))
      .toEqual({ kind: "held", reason: "changed_generation" });
    // Reservation is still present: accepting work did not drop or purge it.
    expect(f.db.prepare("SELECT state FROM recovery_queue_reservations WHERE work_id = ?")
      .get("later-user-work")).toEqual({ state: "settled" });
  });

  it("rejects missing identity, missing schema, and corrupt file on reopen", () => {
    const f = fixture();
    f.db.prepare("DELETE FROM recovery_queue_meta").run();
    expect(() => openRunRecoveryQueueCoordinator(f.pathname)).toThrow(/identity is missing or invalid/);
    f.db.prepare("INSERT INTO recovery_queue_meta (singleton, incarnation) VALUES (1, ?)").run(f.incarnation);
    f.db.exec("DROP TABLE recovery_queue_cutovers");
    expect(() => openRunRecoveryQueueCoordinator(f.pathname)).toThrow(/schema is missing or unknown/);

    const damaged = path.join(f.root, "damaged.sqlite");
    fs.writeFileSync(damaged, Buffer.from("not a SQLite database"));
    expect(() => openRunRecoveryQueueCoordinator(damaged)).toThrow();
    expect(fs.readFileSync(damaged)).toEqual(Buffer.from("not a SQLite database"));
  });

  it("refuses unknown custody and a revoked write before commit", () => {
    const f = fixture();
    f.db.exec("PRAGMA ignore_check_constraints = ON");
    f.db.prepare(`INSERT INTO recovery_queue_reservations
      (work_id, queue_kind, scope_kind, agent_id, session_id, state, reserved_at)
      VALUES ('unknown', 'followup', 'exact', 'agent-a', 'session-a', 'unknown', 1)`).run();
    expect(() => reserveRecoveryQueueWork(f.db, f.exact("later"))).toThrow(/unknown custody state/);
    expect(f.db.prepare("SELECT work_id FROM recovery_queue_reservations WHERE work_id = 'later'").get())
      .toBeUndefined();
    f.db.prepare("DELETE FROM recovery_queue_reservations WHERE work_id = 'unknown'").run();
    let checks = 0;
    const authority = { assertCurrent: () => {
      if (++checks === 3) throw new Error("owner revoked before commit");
    } };
    expect(() => reserveRecoveryQueueWork(f.db, f.exact("later"), authority))
      .toThrow(/owner revoked before commit/);
    expect(f.db.prepare("SELECT work_id FROM recovery_queue_reservations WHERE work_id = 'later'").get())
      .toBeUndefined();
  });

  it("rejects a changed but well-formed incarnation after restart", () => {
    const f = fixture();
    f.db.prepare("UPDATE recovery_queue_meta SET incarnation = ? WHERE singleton = 1").run(randomUUID());
    const reopened = openRunRecoveryQueueCoordinator(f.pathname);
    handles.push(reopened);
    expect(() => reserveRecoveryQueueWork(reopened, f.exact("later"))).toThrow(/incarnation changed/);
  });

  it("serializes concurrent reservations from distinct processes", async () => {
    const f = fixture();
    const source = `const { DatabaseSync } = require('node:sqlite');
      const db = new DatabaseSync(process.argv[1]);
      db.exec('PRAGMA busy_timeout = 5000; BEGIN IMMEDIATE');
      try {
        db.prepare("INSERT OR IGNORE INTO recovery_queue_reservations (work_id, queue_kind, scope_kind, agent_id, session_id, state, reserved_at) VALUES ('shared', 'command-lane', 'unmarked', NULL, NULL, 'queued', 1)").run();
        db.exec('COMMIT');
        process.stdout.write('ok');
      } catch (error) { db.exec('ROLLBACK'); throw error; } finally { db.close(); }`;
    const child = () => new Promise<string>((resolve, reject) => {
      const processHandle = spawn(process.execPath, ["-e", source, f.pathname], { stdio: ["ignore", "pipe", "pipe"] });
      let output = "";
      let error = "";
      processHandle.stdout.on("data", (chunk: Buffer) => { output += chunk.toString(); });
      processHandle.stderr.on("data", (chunk: Buffer) => { error += chunk.toString(); });
      processHandle.on("error", reject);
      processHandle.on("close", (code) => code === 0 ? resolve(output) : reject(new Error(error)));
    });
    expect(await Promise.all([child(), child()])).toEqual(["ok", "ok"]);
    expect(f.db.prepare("SELECT COUNT(*) AS count FROM recovery_queue_reservations WHERE work_id = 'shared'")
      .get()).toEqual({ count: 1 });
    f.cutOver();
    expect(prepareRecoveryClaimIntent(f.db, f.claim)).toEqual({ kind: "held", reason: "unresolved_work" });
  });
});

function outboxInput(incarnation: string) {
  const expected = {
    agentId: "agent-a", databaseIdentity: "physical-a", databaseBirthtime: "birth-a",
    sessionKey: "agent:agent-a:main", sessionId: "session-a", executionId: "execution-a",
    runId: "run-a", lifecycleRevision: "revision-a", lifecycleGeneration: "lifecycle-a",
    ownerEpoch: 7, storeRevision: "store-a", sourceRouteGeneration: "route-a",
    deliveryDigest: "digest-a", originalInputReceiptId: "input-a", inputGeneration: "input-gen-a",
  };
  return { incarnation, expected, idempotencyKey: "recovery-key-a",
    message: { role: "user" as const, content: "Continue", timestamp: 1,
      idempotencyKey: "recovery-key-a" }, route: { kind: "internal" as const } };
}

describe("single-owner recovery claim and accepted input", () => {
  it("commits claim and actual accepted bytes together, or neither on authority loss", () => {
    const f = fixture();
    const input = outboxInput(f.incarnation);
    const authority = { assertClaimCurrent: () => {} };
    expect(claimRecoveryOutbox(f.db, input, authority)).toEqual({
      kind: "held", reason: "queue_not_cut_over" });
    expect(readRecoveryClaimOutbox(f.db, input.expected.agentId, input.expected.sessionId,
      input.idempotencyKey)).toBeUndefined();
    f.cutOver();
    let checks = 0;
    expect(() => claimRecoveryOutbox(f.db, input, { assertClaimCurrent: () => {
      if (++checks === 3) throw new Error("source changed before commit");
    } })).toThrow("source changed before commit");
    expect(readRecoveryClaimOutbox(f.db, input.expected.agentId, input.expected.sessionId,
      input.idempotencyKey)).toBeUndefined();
    const first = claimRecoveryOutbox(f.db, input, authority);
    expect(first).toMatchObject({ kind: "prepared", repeated: false,
      row: { message: input.message, state: "pending" } });
    expect(claimRecoveryOutbox(f.db, input, authority)).toMatchObject({
      kind: "prepared", repeated: true, row: { claimId: first.kind === "prepared" ? first.row.claimId : "" },
    });
    expect(claimRecoveryOutbox(f.db, { ...input, message: { ...input.message, content: "changed" } },
      authority)).toEqual({ kind: "held", reason: "existing_claim" });
  });

  it("reconciles immutable key across restart and never re-admits invalidated custody", () => {
    const f = fixture();
    f.cutOver();
    const input = outboxInput(f.incarnation);
    const first = claimRecoveryOutbox(f.db, input, { assertClaimCurrent: () => {} });
    if (first.kind !== "prepared") throw new Error("claim did not prepare");
    const reopened = openRunRecoveryQueueCoordinator(f.pathname);
    handles.push(reopened);
    const same = readRecoveryClaimOutbox(reopened, input.expected.agentId,
      input.expected.sessionId, input.idempotencyKey);
    expect(same).toMatchObject({ claimId: first.row.claimId, message: input.message });
    expect(recordRecoveryClaimHandoff(reopened, f.incarnation, input.expected.agentId,
      input.expected.sessionId, input.idempotencyKey, first.row.claimId, "accepted-1"))
      .toMatchObject({ state: "handed_off", handoffInputId: "accepted-1" });
    expect(() => recordRecoveryClaimHandoff(reopened, f.incarnation, input.expected.agentId,
      input.expected.sessionId, input.idempotencyKey, first.row.claimId, "accepted-2"))
      .toThrow("claim changed");
    reserveRecoveryQueueWork(reopened, f.unmarked("later-unmarked"));
    expect(readRecoveryClaimOutbox(reopened, input.expected.agentId,
      input.expected.sessionId, input.idempotencyKey)?.state).toBe("needs_reconciliation");
    expect(claimRecoveryOutbox(reopened, input, { assertClaimCurrent: () => {} }))
      .toEqual({ kind: "held", reason: "invalidated" });
  });

  it("serializes two owners on one WAL claim key", () => {
    const f = fixture();
    f.cutOver();
    const second = openRunRecoveryQueueCoordinator(f.pathname);
    handles.push(second);
    const input = outboxInput(f.incarnation);
    const authority = { assertClaimCurrent: () => {} };
    const a = claimRecoveryOutbox(f.db, input, authority);
    const b = claimRecoveryOutbox(second, input, authority);
    expect(a).toMatchObject({ kind: "prepared", repeated: false });
    expect(b).toMatchObject({ kind: "prepared", repeated: true });
    expect(second.prepare("SELECT COUNT(*) AS count FROM recovery_claim_outbox").get())
      .toEqual({ count: 1 });
  });
});
