/** Process-local application-worker route to the main-thread recovery owner. */
import { randomUUID } from "node:crypto";
import { isMainThread, MessageChannel, MessagePort, postMessageToThread } from "node:worker_threads";
import { getRecoveryQueueCoordinatorBroker } from "./run-recovery-queue-coordinator-broker.js";
import type { RecoveryQueueReservation } from "./run-recovery-queue-coordinator.js";

const REQUEST = "openclaw.recovery-queue.request.v1";
const REPLY_TIMEOUT_MS = 5_000;

type Broker = Awaited<ReturnType<typeof getRecoveryQueueCoordinatorBroker>>;
type Request =
  | { type: "reserve"; work: Omit<RecoveryQueueReservation, "incarnation"> }
  | { type: "start"; workId: string }
  | { type: "settle"; workId: string; terminalReceiptId: string };
type Reply = { ok: true; result: string } | { ok: false; error: string };

type Message = { tag: typeof REQUEST; id: string; request: Request; reply: MessagePort };

function isRequest(value: unknown): value is Request {
  if (!value || typeof value !== "object") return false;
  const request = value as Partial<Request>;
  if (request.type === "reserve") {
    const work = request.work;
    return !!work && typeof work.workId === "string" &&
      (work.queue === "followup" || work.queue === "command-lane") &&
      (work.scope?.kind === "unmarked" ||
        (work.scope?.kind === "exact" && typeof work.scope.agentId === "string" &&
          typeof work.scope.sessionId === "string"));
  }
  if (request.type === "start") return typeof request.workId === "string";
  if (request.type === "settle") return typeof request.workId === "string" &&
    typeof request.terminalReceiptId === "string";
  return false;
}

async function execute(broker: Broker, request: Request): Promise<string> {
  switch (request.type) {
    case "reserve": return broker.reserve(request.work);
    case "start": return broker.start(request.workId);
    case "settle": return broker.settle(request.workId, request.terminalReceiptId);
  }
}

/** Install only in the owning main thread. No worker can open or impersonate the store. */
let hostReferences = 0;
let removeHostListener: (() => Promise<void>) | undefined;

export function installRecoveryQueueCoordinatorThreadHost(): () => Promise<void> {
  if (!isMainThread) throw new Error("Recovery queue thread host requires the main thread");
  hostReferences += 1;
  let released = false;
  const release = async () => {
    if (released) return;
    released = true;
    await releaseHostReference();
  };
  if (removeHostListener) return release;
  let active = true;
  const pending = new Set<Promise<void>>();
  const onMessage = (value: unknown, source: number) => {
    if (!value || typeof value !== "object" || (value as Partial<Message>).tag !== REQUEST) return;
    const message = value as Partial<Message>;
    if (!(message.reply instanceof MessagePort)) return;
    const reply = message.reply;
    if (!active || source <= 0 || typeof message.id !== "string" || !isRequest(message.request)) {
      reply.postMessage({ ok: false, error: "Recovery queue host request refused" } satisfies Reply);
      reply.close();
      return;
    }
    const respond = (response: Reply) => {
      try { reply.postMessage(response); } catch { /* Reply loss leaves custody ambiguous. */ }
    };
    const operation = (async () => {
      try {
        // This is the only route to the existing owner, integrity admission,
        // incarnation fence, and native SQLite worker. Worker input never names a DB.
        const broker = await getRecoveryQueueCoordinatorBroker();
        const result = await execute(broker, message.request!);
        respond({ ok: true, result });
      } catch (error) {
        respond({ ok: false, error: error instanceof Error ? error.message : "Recovery queue host failed" });
      } finally {
        reply.close();
      }
    })();
    pending.add(operation);
    void operation.finally(() => pending.delete(operation));
  };
  process.on("workerMessage", onMessage);
  removeHostListener = async () => {
    active = false;
    process.off("workerMessage", onMessage);
    await Promise.allSettled(pending);
  };
  return release;
}

async function releaseHostReference(): Promise<void> {
  hostReferences -= 1;
  if (hostReferences === 0) {
    const remove = removeHostListener;
    removeHostListener = undefined;
    await remove?.();
  }
}

/** An absent or refusing host fails closed. An uncertain reply must be reconciled by work ID. */
async function requestHost(request: Request): Promise<string> {
  if (isMainThread) {
    return execute(await getRecoveryQueueCoordinatorBroker(), request);
  }
  const { port1, port2 } = new MessageChannel();
  const id = randomUUID();
  try {
    const reply = new Promise<string>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Recovery queue host reply uncertain for ${id}`)), REPLY_TIMEOUT_MS);
      port1.once("message", (value: Reply) => {
        clearTimeout(timer);
        if (value?.ok === true) resolve(value.result);
        else reject(new Error(value?.error ?? "Recovery queue host refused"));
      });
      port1.once("close", () => {
        clearTimeout(timer);
        reject(new Error(`Recovery queue host reply uncertain for ${id}`));
      });
    });
    void reply.catch(() => undefined); // Delivery refusal can precede awaiting the reply.
    await postMessageToThread(0, { tag: REQUEST, id, request, reply: port2 } satisfies Message, [port2], REPLY_TIMEOUT_MS);
    return await reply;
  } finally {
    port1.close();
    port2.close();
  }
}

export const recoveryQueueCoordinatorForCaller = {
  reserve: (work: Omit<RecoveryQueueReservation, "incarnation">) => requestHost({ type: "reserve", work }),
  start: (workId: string) => requestHost({ type: "start", workId }),
  settle: (workId: string, terminalReceiptId: string) => requestHost({ type: "settle", workId, terminalReceiptId }),
};
