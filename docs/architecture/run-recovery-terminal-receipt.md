# Run recovery terminal receipt, source checkpoint

This checkpoint adds a private receipt to the existing session metadata row. The lifecycle writer commits it in the same store mutation as a definitive terminal status, only when the event matches the current session ID, run ID, and Gateway lifecycle generation. A new run, session reset, fork, or transcript cut clears the field. No new ledger or recovery input is created.

Read-only `sessions.recovery.inspect` reports terminal `clear` only for a receipt bound to the current session ID, run ID, lifecycle revision, restart-recovery execution ID or proven ordinary owner claim ID, lifecycle generation, and owner epoch, with no observed active direct owner. The session row survives store reopen. Older rows and runs lacking an exact execution ID or owner epoch remain `unknown`. An ordinary execution ID is recorded only when the terminal event claim matches the currently tracked exact agent, key, session, run and lifecycle owner. A terminal receipt does not prove all other custody clear or authorize a continuation.

## Unresolved source seams

- Ordinary direct runs with no live matching owner claim still lack an exact execution ID. A terminal event from an unowned or mismatched claim remains unknown.
- The pending-input receipt store does not retain the original input receipt ID or bind it to the execution after final processing. It cannot establish original input lineage or a durable store revision here.
- Original source-route generation and final delivery receipt are not bound to this lifecycle writer. Inspect keeps route and delivery unknown.
- Descendant, process, worker, followup/lane queue and admission absence are not proven by this receipt. A read-only inspect result is not a recovery claim.
- Store read failure remains unavailable/unknown. No recovery commit, owner retirement, token issuance, or dispatch is implemented.

A future commit must join these facts at their respective owners and revalidate the exact binding under the lifecycle and store transaction before admitting any continuation. A Gateway restart/deploy is required to consume this source change and has not been authorized by this checkpoint.
