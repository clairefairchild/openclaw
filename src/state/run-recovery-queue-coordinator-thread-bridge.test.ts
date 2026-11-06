import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it } from "vitest";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Vitest tests themselves run in a Worker. A fresh process exercises the real
// application Worker -> main-thread host -> native SQLite worker chain.
const source = `
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { Worker } from 'node:worker_threads';
import { pathToFileURL } from 'node:url';
import { createRecoveryQueueCoordinatorWorkerConnection, recoveryQueueCoordinatorForCaller } from './src/state/run-recovery-queue-coordinator-thread-bridge.ts';
import { getRecoveryQueueCoordinatorBroker } from './src/state/run-recovery-queue-coordinator-broker.ts';
const root = process.argv[1];
process.env.OPENCLAW_STATE_DIR = root;
const runWorker = (workId, connection) => new Promise((resolve, reject) => {
  const worker = new Worker(pathToFileURL(process.cwd() + '/src/state/run-recovery-queue-coordinator-thread-bridge.test-worker.ts'), { workerData: { workId, port: connection?.port }, transferList: connection ? [connection.port] : [], execArgv: ['--import', './scripts/tsx.mjs'] });
  worker.once('message', resolve);
  worker.once('error', reject);
});
const connection = createRecoveryQueueCoordinatorWorkerConnection();
const host = await recoveryQueueCoordinatorForCaller.reserve({
  workId: 'host-work', queue: 'command-lane', scope: { kind: 'unmarked' },
});
assert.equal(host, 'reserved');
const worker = await runWorker('stable-worker-id', connection);
assert.deepEqual(worker, { hostRefused: false, first: 'reserved', repeated: 'repeated', started: 'started', settled: 'settled' });
const db = new DatabaseSync(root + '/run-recovery-queue-coordinator.sqlite');
assert.deepEqual(db.prepare('SELECT work_id, state FROM recovery_queue_reservations ORDER BY work_id').all(), [
  { work_id: 'host-work', state: 'queued' },
  { work_id: 'stable-worker-id', state: 'settled' },
]);
assert.equal(db.prepare('SELECT COUNT(*) AS count FROM recovery_queue_cutovers').get().count, 0);
db.close();
await connection.close();
const refused = await runWorker('refused-work');
assert.equal(refused.hostRefused, true);
assert.match(refused.error, /host|worker|message/i);
await (await getRecoveryQueueCoordinatorBroker()).close();
process.stdout.write('ok');
`;

it("routes worker and host through one owned ledger, preserves IDs, and refuses absent host", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-thread-bridge-"));
  roots.push(root);
  const result = await new Promise<{ code: number | null; stdout: string; stderr: string }>(
    (resolve, reject) => {
      const child = spawn(process.execPath, [
        "--import", "./scripts/tsx.mjs", "--input-type=module", "--eval", source, root,
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
}, 45_000);
