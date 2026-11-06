import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it } from "vitest";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// The broker is a host-only API. Vitest's unit pool is itself a Worker, so
// exercise it in a fresh main-thread host process instead of bypassing that gate.
const source = `
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { openRecoveryQueueCoordinatorBroker } from './src/state/run-recovery-queue-coordinator-broker.ts';
const databasePath = process.argv[1];
const work = { workId: 'followup-1', queue: 'followup',
  scope: { kind: 'exact', agentId: 'agent-a', sessionId: 'session-a' } };
const first = await openRecoveryQueueCoordinatorBroker(databasePath);
try {
  assert.equal(await first.reserve(work), 'reserved');
  const contender = spawnSync(process.execPath, [
    '--import', './scripts/tsx.mjs', '--input-type=module', '--eval',
    "import { openRecoveryQueueCoordinatorBroker } from './src/state/run-recovery-queue-coordinator-broker.ts'; try { await openRecoveryQueueCoordinatorBroker(process.argv[1]); process.exit(0); } catch (error) { console.error(error.message); process.exit(7); }",
    databasePath,
  ], { cwd: process.cwd(), encoding: 'utf8', timeout: 15000 });
  assert.equal(contender.status, 7, contender.stderr);
  assert.match(contender.stderr, /busy|Contention|ownership/i);
  assert.equal(await first.reserve(work), 'repeated');
  const read = new DatabaseSync(databasePath);
  assert.equal(read.prepare('SELECT state FROM recovery_queue_reservations WHERE work_id = ?')
    .get(work.workId)?.state, 'queued');
  read.close();
  await first.close();
  const second = await openRecoveryQueueCoordinatorBroker(databasePath);
  try {
    assert.equal(second.incarnation, first.incarnation);
    assert.equal(await second.start(work.workId), 'started');
    assert.equal(await second.start(work.workId), 'repeated');
    assert.equal(await second.settle(work.workId, 'terminal-1'), 'settled');
    assert.equal(await second.settle(work.workId, 'terminal-1'), 'repeated');
    await assert.rejects(second.settle(work.workId, 'terminal-other'), /receipt changed/);
    await assert.rejects(second.reserve({ ...work, scope: { kind: 'unmarked' } }), /work ID collision/);
    const db = new DatabaseSync(databasePath);
    db.prepare('UPDATE recovery_queue_meta SET incarnation = ? WHERE singleton = 1')
      .run(randomUUID());
    db.close();
    await assert.rejects(second.reserve({ ...work, workId: 'later' }), /incarnation changed/);
  } finally { await second.close(); }
} finally { await first.close(); }
process.stdout.write('ok');
`;

it("brokers durable reserve, restart, start, settle, collision, and incarnation fencing", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-broker-"));
  roots.push(root);
  const databasePath = path.join(root, "coordinator.sqlite");
  const result = await new Promise<{ code: number | null; stdout: string; stderr: string }>(
    (resolve, reject) => {
      const child = spawn(process.execPath, [
        "--import", "./scripts/tsx.mjs", "--input-type=module", "--eval", source, databasePath,
      ], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
      let stdout = "";
      let stderr = "";
      child.stdout.on("data", (chunk: Buffer) => { stdout += chunk.toString(); });
      child.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });
      child.on("error", reject);
      child.on("close", (code) => resolve({ code, stdout, stderr }));
    },
  );
  expect(result, result.stderr).toMatchObject({ code: 0, stdout: "ok" });
});
