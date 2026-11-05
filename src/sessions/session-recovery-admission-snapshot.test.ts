import { describe, expect, it } from "vitest";
import { beginSessionWorkAdmission, readSessionRecoveryAdmissionSnapshot } from
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
        .toEqual({ pending: false, active: false });
    } finally {
      lease.release();
    }
    expect(read()).toEqual({ pending: false, active: false });
  });
});
