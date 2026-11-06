import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { openRecoveryQueueCoordinatorBroker } from "./run-recovery-queue-coordinator-broker.js";

const roots: string[] = [];
const brokers: Array<{ close(): Promise<void> }> = [];
function fixturePath() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-broker-"));
  roots.push(root);
  return path.join(root, "coordinator.sqlite");
}
afterEach(async () => {
  for (const broker of brokers.splice(0)) await broker.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("brokered recovery queue ledger", () => {
  it("reserves before any caller publication and retains unresolved work across broker reopen", async () => {
    const databasePath = fixturePath();
    const first = await openRecoveryQueueCoordinatorBroker(databasePath);
    brokers.push(first);
    const work = {
      workId: "followup-1",
      queue: "followup" as const,
      scope: { kind: "exact" as const, agentId: "agent-a", sessionId: "session-a" },
    };
    expect(await first.reserve(work)).toBe("reserved");
    expect(await first.reserve(work)).toBe("repeated");
    const observed = new DatabaseSync(databasePath);
    expect(observed.prepare("SELECT state FROM recovery_queue_reservations WHERE work_id = ?")
      .get(work.workId)).toEqual({ state: "queued" });
    observed.close();
    await first.close();
    brokers.splice(brokers.indexOf(first), 1);
    const second = await openRecoveryQueueCoordinatorBroker(databasePath);
    brokers.push(second);
    expect(second.incarnation).toBe(first.incarnation);
    expect(await second.start(work.workId)).toBe("started");
    expect(await second.start(work.workId)).toBe("repeated");
    expect(await second.settle(work.workId, "terminal-1")).toBe("settled");
    expect(await second.settle(work.workId, "terminal-1")).toBe("repeated");
    await expect(second.settle(work.workId, "terminal-other")).rejects.toThrow("receipt changed");
  });

  it("fails closed on identity collision and wrong incarnation", async () => {
    const databasePath = fixturePath();
    const broker = await openRecoveryQueueCoordinatorBroker(databasePath);
    brokers.push(broker);
    expect(await broker.reserve({ workId: "same", queue: "followup", scope: { kind: "unmarked" } }))
      .toBe("reserved");
    await expect(broker.reserve({
      workId: "same", queue: "followup",
      scope: { kind: "exact", agentId: "agent-a", sessionId: "session-a" },
    })).rejects.toThrow("work ID collision");
    const db = new DatabaseSync(databasePath);
    db.prepare("UPDATE recovery_queue_meta SET incarnation = ? WHERE singleton = 1").run("replacement");
    db.close();
    await expect(broker.reserve({
      workId: "later", queue: "followup", scope: { kind: "unmarked" },
    })).rejects.toThrow("incarnation changed");
  });
});
