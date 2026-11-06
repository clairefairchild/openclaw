import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it } from "vitest";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

// Vitest tests run in an application Worker. The production Gateway caller
// and its SQLite broker instead live on the host main thread.
const source = `
import assert from 'node:assert/strict';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { isMainThread } from 'node:worker_threads';
assert.equal(isMainThread, true);
const { getAgentEventLifecycleGeneration } = await import('./src/infra/agent-events.ts');
const { createEmbeddedRunLaneController } = await import('./src/agents/embedded-agent-runner/run/lane-controller.ts');
const databasePath = path.join(process.env.OPENCLAW_STATE_DIR, 'run-recovery-queue-coordinator.sqlite');
let params = { sessionId: 'session-main-thread', sessionFile: 'session-main-thread.jsonl',
  workspaceDir: process.env.OPENCLAW_STATE_DIR, prompt: 'test', timeoutMs: 1000,
  runId: 'run-main-thread', trigger: 'user' };
let generation = getAgentEventLifecycleGeneration();
const controller = createEmbeddedRunLaneController({
  getLifecycleGeneration: () => generation, getParams: () => params,
  globalLane: 'proof-global', sessionLane: 'proof-session',
  initialQueuedLifecycleGeneration: generation,
  setLifecycleGeneration: (value) => { generation = value; },
  setParams: (value) => { params = value; },
});
let effects = 0;
const result = await controller.enqueueSession(async () => {
  const db = new DatabaseSync(databasePath);
  try {
    const row = db.prepare('SELECT state, scope_kind FROM recovery_queue_reservations').get();
    assert.equal(row.state, 'in_flight');
    assert.equal(row.scope_kind, 'unmarked');
  } finally { db.close(); }
  effects += 1;
  return 'done';
});
assert.equal(result, 'done');
assert.equal(effects, 1);
const db = new DatabaseSync(databasePath);
try {
  const rows = db.prepare('SELECT state, terminal_receipt_id FROM recovery_queue_reservations').all();
  assert.equal(rows.length, 1);
  assert.equal(rows[0].state, 'settled');
  assert.match(rows[0].terminal_receipt_id, /^completed:command-lane:/);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM recovery_queue_cutovers').get().n, 0);
} finally { db.close(); }
const { getRecoveryQueueCoordinatorBroker } = await import('./src/state/run-recovery-queue-coordinator-broker.ts');
await (await getRecoveryQueueCoordinatorBroker()).close();
process.stdout.write('ok');
`;

it("reserves, starts, and settles an embedded session lane through the real host broker", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "embedded-lane-main-thread-"));
  roots.push(root);
  const result = await new Promise<{ code: number | null; stdout: string; stderr: string }>(
    (resolve, reject) => {
      const child = spawn(process.execPath, [
        "--import", "./scripts/tsx.mjs", "--input-type=module", "--eval", source,
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
});
