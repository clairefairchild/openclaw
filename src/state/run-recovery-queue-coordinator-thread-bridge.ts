/** Explicit capability route from an application Worker to its main-thread recovery owner. */
import { randomUUID } from "node:crypto";
import { isMainThread, MessageChannel, MessagePort } from "node:worker_threads";
import { getRecoveryQueueCoordinatorBroker } from "./run-recovery-queue-coordinator-broker.js";
import type { RecoveryQueueReservation } from "./run-recovery-queue-coordinator.js";

const REPLY_TIMEOUT_MS = 5_000;
type Broker = Awaited<ReturnType<typeof getRecoveryQueueCoordinatorBroker>>;
type Request =
  | { type: "reserve"; work: Omit<RecoveryQueueReservation, "incarnation"> }
  | { type: "start"; workId: string }
  | { type: "settle"; workId: string; terminalReceiptId: string };
type Reply = { ok: true; result: string } | { ok: false; error: string };
type Message = { id: string; request: Request; reply: MessagePort };

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

/** The host must transfer this port to one specifically admitted Worker. */
export function createRecoveryQueueCoordinatorWorkerConnection(): {
  port: MessagePort;
  close: () => Promise<void>;
} {
  if (!isMainThread) throw new Error("Recovery queue connection requires the main-thread host");
  const { port1: host, port2: port } = new MessageChannel();
  const pending = new Set<Promise<void>>();
  let closed = false;
  const onMessage = (value: unknown) => {
    if (!value || typeof value !== "object") return;
    const message = value as Partial<Message>;
    if (!(message.reply instanceof MessagePort)) return;
    const reply = message.reply;
    const respond = (response: Reply) => {
      try { reply.postMessage(response); } catch { /* Reply loss leaves custody ambiguous. */ }
    };
    if (closed || typeof message.id !== "string" || !isRequest(message.request)) {
      respond({ ok: false, error: "Recovery queue host request refused" });
      reply.close();
      return;
    }
    const operation = (async () => {
      try {
        // The existing host broker retains physical ownership, integrity and
        // incarnation checks, and native worker write admission. No DB path or
        // authority travels over this connection.
        const broker = await getRecoveryQueueCoordinatorBroker();
        respond({ ok: true, result: await execute(broker, message.request!) });
      } catch (error) {
        respond({ ok: false, error: error instanceof Error ? error.message : "Recovery queue host failed" });
      } finally {
        reply.close();
      }
    })();
    pending.add(operation);
    void operation.finally(() => pending.delete(operation));
  };
  host.on("message", onMessage);
  return {
    port,
    close: async () => {
      if (closed) return;
      closed = true;
      host.off("message", onMessage);
      host.close();
      await Promise.allSettled(pending);
    },
  };
}

let workerConnection: MessagePort | undefined;
/** Install only the port transferred by the parent that owns this Worker's custody. */
export function installRecoveryQueueCoordinatorWorkerConnection(port: MessagePort): void {
  if (isMainThread || workerConnection || !(port instanceof MessagePort)) {
    throw new Error("Recovery queue worker connection refused");
  }
  workerConnection = port;
  port.unref();
}

/** Missing host capability fails closed; ambiguous replies require stable-ID readback. */
async function requestHost(request: Request): Promise<string> {
  if (isMainThread) return execute(await getRecoveryQueueCoordinatorBroker(), request);
  const connection = workerConnection;
  if (!connection) throw new Error("Recovery queue worker has no authorized host connection");
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
    void reply.catch(() => undefined);
    connection.postMessage({ id, request, reply: port2 } satisfies Message, [port2]);
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
