import { createHash } from "node:crypto";
import type { InternalSessionEntry } from "./types.js";

/** Internal source identity only. External address registry changes need their own route revision. */
export function fingerprintRunRecoverySourceRoute(
  delivery: InternalSessionEntry["delivery"],
): string | undefined {
  if (delivery?.kind !== "internal") {
    return undefined;
  }
  return createHash("sha256").update(JSON.stringify(delivery)).digest("hex");
}
