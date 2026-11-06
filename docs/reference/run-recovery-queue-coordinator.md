# Gateway Run recovery queue coordinator, Stage 1

This source change adds a durable queue-admission coordinator, not recovery activation. The Gateway and both in-memory queues remain unchanged in production. Do not interpret their empty snapshots as absence proof. Existing agent-local `prepareSessionRunRecoveryClaimInWorker` is held with `queue_not_cut_over`, so it cannot prepare a claim during this incomplete migration.

## Identity and lock owner

The coordinator owns one SQLite database at `<state-dir>/run-recovery-queue-coordinator.sqlite`, separate from every per-agent database. A per-agent owner cannot cover a lane task with no session/agent marker, or work for another agent. Its persisted incarnation UUID identifies the physical coordinator epoch; every producer operation must compare it before a write. WAL and `BEGIN IMMEDIATE` serialize reservations, state transitions, and claim intent across processes. No in-memory queue registry or agent-local transaction is used as a queue absence proof.

A producer first reserves a stable work ID, exact `(agentId, sessionId)` or conservative global `unmarked` scope, *before* publishing to a followup queue or starting a command-lane task. A later dequeue transitions `queued` to `in_flight`. Only a terminal custody receipt settles it. A crash leaves unresolved reservations in place. An unmarked reservation vetoes claims for every agent. Duplicated reserve/start/settle operations are idempotent only for the same identity or receipt. A collision or wrong incarnation fails closed. A new reservation after a prepared intent atomically invalidates that intent; accepted user work is not rejected or purged.

Claim intent compares both producer cutover markers and all unresolved relevant reservations in the same `BEGIN IMMEDIATE` transaction that inserts the intent. Neither producer marker has a production caller in Stage 1. An absent marker returns `queue_not_cut_over`, including when the table is empty. The migration marker API must be called only after all publish/start paths for that producer have been fenced and pre-existing queue work reconciled. This API has no automatic timeout or garbage collection of unresolved work.

## Remaining bridge before recovery is possible

1. Move all followup enqueue, overflow, abort, drain and lifecycle terminal paths onto the coordinator. Reserve before any in-memory publication, preserve accepted input receipts, and settle only with authoritative terminal custody. Reconcile work already in the queue at cutover.
2. Move all command-lane enqueue/start, abort, timeout, group drain and terminal paths onto the coordinator, including tasks without `taskIdentity`. Do not infer agent attribution for unmarked tasks. Reconcile pre-cutover work.
3. Run coordinator writes through the SQLite worker broker, not the Gateway main thread. Bind worker ownership/connection lifecycle and integrity admission to the global state database owner.
4. Atomically bridge the coordinator's prepared intent to the existing per-agent claim and accepted-input outbox. The current agent-local claim transaction cannot compare a separate coordinator DB. Use an attached-DB transaction with a reviewed lock order, or move the claim/outbox into a single durable owner, then revalidate session and source generations in that transaction. Until this is implemented, the legacy claim stays unreachable.
5. Add authoritative reconciliation for ambiguous terminal receipts and accepted input/delivery. Never clear a reservation merely because its producer process died. Review source-route and semantic eligibility separately before any recovery send.

No live Gateway code, install, restart, cron, Run, or source-delivery path is changed by Stage 1. Rollback is to leave the new coordinator unused and retain its SQLite rows, claims, and receipts for later reconciliation. Never delete the ledger to force a clear result.

## Stage 2 guarded broker foundation (incomplete)

The coordinator now has a SQLite worker-broker backend and an async host facade for reserve, start, and settle. Its incarnation is captured on open and checked by each write transaction. This moves the future producer's SQLite I/O off the Gateway main thread, but it is **not** producer cutover. No production followup enqueue, drain, abort, overflow, collect, steering, or terminal path calls the facade yet. Existing in-memory work is not reconciled and no producer cutover marker is written. The authoritative recovery answer remains `queue_not_cut_over` (UNKNOWN), regardless of empty queue snapshots.

The followup cutover must make all production ingress async before publication, reconcile rejected/deduplicated work and all terminal custody receipts, then account for summary and collect source ownership before marking the producer. The broker itself must also be bound to the global state database owner and its integrity admission before a production caller uses it. A crash after an ambiguous broker write requires readback by stable work ID before retry. Do not settle on process death or merely because the queue was cleared.

## Stage 3 owner and integrity admission (source only)

The broker host now acquires the canonical Gateway state-owner sidecar for the global coordinator path before opening its native SQLite worker. It retains physical ownership until native close succeeds. A competing broker process is refused. Each broker write receives host authority grants after `BEGIN IMMEDIATE` and immediately before `COMMIT`; the host revalidates its physical owner at both grants. Loss of ownership holds the operation. An uncertain close or failed open is not treated as proof that custody was released.

Existing ledgers undergo full SQLite integrity and foreign-key checks before schema or identity admission. A missing schema or malformed/missing incarnation is refused, not silently recreated. Unknown persisted queue or claim states veto writes and claim intent. A fresh broker ledger initializes behind the host owner; it still has no producer cutover markers. A broker transport error after a write may be ambiguous: callers must reconcile by stable work ID and exact receipt before any retry. This stage adds no production queue caller, marker, cross-database claim bridge, or recovery send. `queue_not_cut_over` remains the production recovery answer.

## Stage 4 first ingress reservation (source only, incomplete)

The active-run `enqueue-followup` branch in `src/auto-reply/reply/agent-runner-run.ts` now awaits a global broker reservation before calling the existing synchronous queue publisher. It uses a stable per-run work ID and conservative `unmarked` scope because the queued run can be rebound to a later canonical session. A definitive dedupe, cap, or cancellation rejection receives a unique rejection receipt; a publication exception, uncertain reserve acknowledgment, or accepted queue item remains unresolved. The broker is process-owned and retains the global state-owner sidecar. The queue's existing dedupe, cap, and scheduling behavior is otherwise unchanged.

This is deliberately only a producer subset. The steering/park path in `agent-runner-steer-adoption.ts`, final-reply retry in `agent-runner-result-complete.ts`, source-delivery retry in `followup-delivery.ts`, and any direct `enqueueFollowupRun` callers remain unfenced. Existing queue contents, collect/summary ownership, drain start and terminal settlement, abort, overflow of previously accepted items, and command-lane work remain unreconciled. Do not set either cutover marker or activate recovery. Accepted active-run reservations intentionally remain unresolved across restart until authoritative terminal custody and reconciliation are implemented. Rollback is to revert this source patch before deployment while retaining all coordinator rows; never delete rows to manufacture absence.

## Stage 5 remaining followup ingress reservations (source only, incomplete)

The steering park path, stranded final-reply retry, and source-delivery retry now use the same async broker pre-publication reservation as the active-run producer. Each source has a stable in-process work ID and conservative global `unmarked` scope. The synchronous queue publisher and steering park helper are retained only for queue tests; production calls fail closed if they try to use either helper without a reservation. Definitive local rejection settles its own reservation. Accepted publication, a publication exception, and an ambiguous broker result remain unresolved, including across restart. The broker owner and queue ordering/dedupe/overflow behavior remain unchanged.

This does not set the followup cutover marker. Existing in-memory queue contents, collect/summary ownership, drain start and authoritative terminal settlement, abort/overflow of previously accepted items, pre-existing work reconciliation, and command-lane reservations remain incomplete. A production cutover still requires those paths and the cross-database claim bridge before any recovery claim can activate. Rollback is to revert this source patch before deployment while retaining coordinator rows for reconciliation.

## Stage 6 opt-in command-lane custody bridge (source only, incomplete)

`enqueueCommandInLaneWithRecoveryCustody` is an opt-in bridge over the existing lane queue. A caller supplies a stable work ID and conservative or exact scope. The shared broker reserves before invoking the synchronous publisher, records start before task effects, and settles only a current-generation successful task or a definitive pre-start rejection. A failed task, timeout, reset generation, uncertain broker acknowledgment, and stale completion remain unresolved for reconciliation. Followup ingress and this bridge share one process-owned broker and physical coordinator owner.

No production command-lane caller is switched to the bridge by this stage. The broad command lane has nested, unmarked, timeout, abort, clear, reset, and group-drain paths whose stable work identities and terminal receipts need an owner-by-owner cutover. Existing lane contents are not reconciled. Neither producer cutover marker is written, and recovery remains held with `queue_not_cut_over` (UNKNOWN). Rollback leaves any ledger rows intact for exact-ID reconciliation and reverts only source before deployment.

## Stage 7 limited Gateway command-lane ingress (source only, incomplete)

The Gateway-owned system-agent RPC and cron manual-run command-lane callers use
the guarded ingress. Each submitted task receives a distinct durable work ID
with conservative `unmarked` scope, reserved before queue publication. The
bridge starts before task effects, settles definitive pre-start rejection and
successful current-generation task completion, and retains failure, timeout,
reset, lost acknowledgment, or stale completion for reconciliation.

The broader production caller migration is **blocked**. An exact-source caller
suite showed embedded agent lane admission executes in an application worker.
That worker cannot open the coordinator's SQLite store: the host broker
connection is required, and `openSqliteWorkerStore` explicitly refuses a
non-main-thread open. Do not mock, bypass, or weaken that boundary. Embedded
run, compaction, session placement, context-engine maintenance, and generic
background-work callers therefore remain on the original command lane until a
reviewed worker-to-host coordinator route is available. The isolated source
branch is not a deployable complete cutover.

No command-lane producer marker is written. Pre-existing in-memory queue work
at a future deployment boundary has no durable work ID and cannot be cleared
from an empty snapshot after restart. Its authoritative owner receipts, as
well as abort, clear, group drain, timeout, and downstream terminal receipts,
need reconciliation before the marker can be set. Retain all unresolved ledger
rows for exact-ID review. Recovery remains held with `queue_not_cut_over`.

## Stage 7 application-worker host transport (source only, incomplete)

A process-local thread bridge now routes opted-in command custody operations from application Workers to the Gateway main-thread host. The host owns the existing coordinator broker, physical state-owner sidecar, integrity admission, incarnation check, and native SQLite worker. Worker requests carry only reserve/start/settle facts, never a database path, authority grant, or cutover marker. The Gateway installs the listener only while its main-thread state owner is active, joins accepted operations on close, and refuses a missing or invalid host. An uncertain reply leaves the stable work ID unresolved for reconciliation. Direct host calls use the same broker. Source tests exercise the real worker-to-host path, host path, absence refusal, and repeated stable ID.

This transport does not move the embedded run controller or generic background enqueue back onto custody yet. Their unit and runtime paths require an explicit producer-by-producer migration with terminal receipt and preexisting queue reconciliation. The command cutover marker remains off; no recovery claim or send becomes eligible. The followup marker, cross-database claim bridge, and other Stage 1-6 gaps also remain outstanding.
