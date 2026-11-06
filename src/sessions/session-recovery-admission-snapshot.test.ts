import { describe, expect, it } from "vitest";
import { beginSessionWorkAdmission, readSessionRecoveryAdmissionSnapshot,
  runWithSessionRecoveryAdmissionInspection } from
  "./session-lifecycle-admission.js";

const target = { scope: "/tmp/recovery-admission-test.json",
  sessionKey: "agent:main:main", sessionId: "session-1" };
const read = () => readSessionRecoveryAdmissionSnapshot(target);

describe("session recovery admission snapshot", () => {
  it("observes pending and acquired ownership, then releases without proving global clear", async () => {
    const admission = beginSessionWorkAdmission({ scope: target.scope,
      identities: [target.sessionKey, target.sessionId], assertAllowed: () => {} });
    expect(read().pending || read().active).toBe(true);
    const lease = await admission;
    try {
      expect(read()).toEqual({ pending: false, active: true });
      expect(readSessionRecoveryAdmissionSnapshot({ ...target, sessionId: "other" }))
        .toEqual({ pending: false, active: true });
    } finally {
      lease.release();
    }
    expect(read()).toEqual({ pending: false, active: false });
  });
});


describe("locked session recovery admission inspection", () => {
  it("treats a competing incarnation on the same key as active", async () => {
    const lease = await beginSessionWorkAdmission({ scope: target.scope,
      identities: [target.sessionKey, "session-other"], assertAllowed: () => {} });
    try {
      const custody = await runWithSessionRecoveryAdmissionInspection({ ...target,
        run: () => read() });
      expect(custody).toEqual({ pending: false, active: true });
    } finally {
      lease.release();
    }
  });

  it("sees a pending reservation and only clears after release", async () => {
    let allow!: () => void;
    let started!: () => void;
    const entered = new Promise<void>((resolve) => { started = resolve; });
    const blocked = new Promise<void>((resolve) => { allow = resolve; });
    const pending = beginSessionWorkAdmission({ scope: target.scope,
      identities: [target.sessionKey, target.sessionId],
      assertAllowed: async () => { started(); await blocked; } });
    await entered;
    try {
      expect(await runWithSessionRecoveryAdmissionInspection({ ...target,
        run: () => read() })).toEqual({ pending: true, active: false });
    } finally {
      allow();
    }
    const lease = await pending;
    lease.release();
    expect(await runWithSessionRecoveryAdmissionInspection({ ...target,
      run: () => read() })).toEqual({ pending: false, active: false });
  });
});
