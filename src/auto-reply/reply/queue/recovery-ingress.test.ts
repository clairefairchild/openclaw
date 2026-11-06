import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { expect, it } from "vitest";

const source = `
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { enqueueActiveRunFollowupWithRecoveryReservation } from './src/auto-reply/reply/queue/enqueue.ts';
import { getExistingFollowupQueue } from './src/auto-reply/reply/queue/state.ts';
const dbPath = process.argv[1];
const key = 'agent:a:main';
const settings = { mode: 'followup', cap: 1, dropPolicy: 'new' };
const runFollowup = async () => {};
const createRun = (messageId, onDeferred) => ({
  prompt: 'hello', messageId, enqueuedAt: Date.now(),
  originatingChannel: 'webchat', originatingTo: 'local',
  turnAdoptionLifecycle: { admission: 'cancel-only', onAdopted: () => {}, onDeferred },
  run: { agentId: 'a', sessionId: 'session-a' },
});
const readRows = () => {
  const db = new DatabaseSync(dbPath);
  try { return db.prepare('SELECT state, scope_kind, terminal_receipt_id FROM recovery_queue_reservations ORDER BY rowid').all(); }
  finally { db.close(); }
};
const first = createRun('m1', () => {
  const rows = readRows();
  assert.equal(rows.length, 1);
  assert.equal(rows[0].state, 'queued');
  assert.equal(rows[0].scope_kind, 'unmarked');
  return true;
});
assert.equal(await enqueueActiveRunFollowupWithRecoveryReservation(key, first, settings, runFollowup), true);
assert.equal(getExistingFollowupQueue(key)?.items.length, 1);
const duplicate = createRun('m1');
assert.equal(await enqueueActiveRunFollowupWithRecoveryReservation(key, duplicate, settings, runFollowup), false);
const capped = createRun('m2');
assert.equal(await enqueueActiveRunFollowupWithRecoveryReservation(key, capped, settings, runFollowup), false);
const rows = readRows();
assert.deepEqual(rows.map(row => row.state), ['queued', 'settled', 'settled']);
assert.ok(rows[1].terminal_receipt_id?.startsWith('rejected:'));
const db = new DatabaseSync(dbPath);
db.prepare('UPDATE recovery_queue_meta SET incarnation = ? WHERE singleton = 1').run('00000000-0000-4000-8000-000000000001');
db.close();
await assert.rejects(
  enqueueActiveRunFollowupWithRecoveryReservation(key, createRun('m3'), settings, runFollowup),
  /incarnation changed/,
);
assert.equal(getExistingFollowupQueue(key)?.items.length, 1);
process.stdout.write('ok');
process.exit(0);
`;

it("reserves before active-run publication, settles definite rejection, and holds on broker mismatch", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-ingress-"));
  const dbPath = path.join(root, "run-recovery-queue-coordinator.sqlite");
  try {
    const result = await new Promise<{ code: number | null; stdout: string; stderr: string }>(
      (resolve, reject) => {
        const child = spawn(process.execPath, [
          "--import", "./scripts/tsx.mjs", "--input-type=module", "--eval", source, dbPath,
        ], {
          cwd: process.cwd(),
          env: { ...process.env, OPENCLAW_STATE_DIR: root },
          stdio: ["ignore", "pipe", "pipe"],
        });
        let stdout = "";
        let stderr = "";
        child.stdout.on("data", (chunk: Buffer) => { stdout += chunk.toString(); });
        child.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });
        child.on("error", reject);
        child.on("close", (code) => resolve({ code, stdout, stderr }));
      },
    );
    expect(result, result.stderr).toMatchObject({ code: 0, stdout: "ok" });
    // A fresh process still sees unresolved accepted custody. No cutover marker
    // was installed, so the recovery claim remains unavailable.
    const db = new DatabaseSync(dbPath);
    try {
      expect(db.prepare("SELECT state FROM recovery_queue_reservations WHERE state = 'queued'").all())
        .toHaveLength(1);
      expect(db.prepare("SELECT queue_kind FROM recovery_queue_cutovers").all()).toHaveLength(0);
    } finally {
      db.close();
    }
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
