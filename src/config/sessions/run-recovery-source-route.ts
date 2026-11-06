import { createHash } from "node:crypto";
import type { InternalSessionEntry } from "./types.js";

/** A digest of persisted delivery facts, never a route authorization decision. */
export function fingerprintRunRecoverySourceRoute(
  delivery: InternalSessionEntry["delivery"],
): string | undefined {
  if (delivery?.kind !== "internal" && delivery?.kind !== "external") {
    return undefined;
  }
  return createHash("sha256").update(JSON.stringify(delivery)).digest("hex");
}
