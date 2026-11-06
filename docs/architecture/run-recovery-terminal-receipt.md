# Run recovery terminal receipt, source checkpoint

This checkpoint adds a private receipt to the existing session metadata row. The lifecycle writer commits it in the same store mutation as a definitive terminal status, only when the event matches the current session ID, run ID, and Gateway lifecycle generation. A new run, session reset, fork, or transcript cut clears the field. No new ledger or recovery input is created.

Read-only `sessions.recovery.inspect` reports terminal `clear` only for a receipt bound to the current session ID, run ID, lifecycle revision, restart-recovery execution ID or proven ordinary owner claim ID, lifecycle generation, and owner epoch, with no observed active direct owner. The session row survives store reopen. Older rows and runs lacking an exact execution ID or owner epoch remain `unknown`. The pending-input owner now commits a metadata-only accepted input ID and random input generation in the same transaction as new input admission. A second distinct input for the same run invalidates this bounded original-input record. Reset/revision replacement clears it, and a later run replaces it. The terminal owner copies this ID and generation only when its exact execution, run, session, key, lifecycle revision and Gateway generation match. Inspect reports input lineage `clear` only while both durable copies still match; otherwise it remains `unknown`. This does not imply the input was consumed, that no later pending input exists, or that continuation is eligible. An ordinary execution ID is recorded only when the terminal event claim matches the currently tracked exact agent, key, session, run and lifecycle owner. A terminal receipt does not prove all other custody clear or authorize a continuation.

## Unresolved source seams

- Ordinary direct runs with no live matching owner claim still lack an exact execution ID. A terminal event from an unowned or mismatched claim remains unknown.
- The bounded session-row input record proves only the latest exact owner-accepted input ID/generation, when a terminal owner copied that identity. It does not retain all accepted inputs or prove transcript consumption. Store revision remains unknown.
- Original source-route generation and final delivery receipt are not bound to this lifecycle writer. Inspect keeps route and delivery unknown.
- Descendant, process, worker, followup/lane queue and admission absence are not proven by this receipt. A read-only inspect result is not a recovery claim.
- Store read failure remains unavailable/unknown. No recovery commit, owner retirement, token issuance, or dispatch is implemented.

A future commit must join these facts at their respective owners and revalidate the exact binding under the lifecycle and store transaction before admitting any continuation. A Gateway restart/deploy is required to consume this source change and has not been authorized by this checkpoint.
