# FBref janitor: bounded publication-lock wait (#1322)

The daily FBref janitor now waits for the midnight ingest publication lock for
up to 90 minutes per task attempt, polling every 30 seconds. The bound uses a
monotonic clock; the read-only lock probe uses PostgreSQL's `active` flag.
The existing janitor still acquires the lock atomically. If another publisher
wins between the probe and acquisition, only that specific busy-owner conflict
is retried within the same deadline. Other errors fail immediately.

While the lock is busy, no cleanup or maintenance control run is started. A
lost acquisition race can create one failed zero-budget maintenance run; the
existing wrapper finalizes it without releasing the other owner's lock.
At the deadline the task raises `TimeoutError`, never skips or reports success.
After successful acquisition, existing per-DROP renewal, database-clock owner
assertion, stage eligibility checks, release and run finalization are unchanged.

Only `janitor_fbref_generic_stages` gets a 120-minute execution timeout, leaving
30 minutes after the maximum wait. This is an overall task limit, not a separate
30-minute cleanup timer. Existing two retries and five-minute retry delay remain:
three attempts have a worst-case Airflow execution budget of 6 hours 10 minutes
(excluding scheduler/queue delay). Normal uncontended execution starts at once.
Waiting occupies one worker slot. Existing serial dependencies mean downstream
maintenance waits for FBref to finish, including retries; the shared 02:00 UTC
(05:00 МСК) cron, DAG graph, pools, other task limits and retention are unchanged.

## Change and verification boundary

Runtime delivery consists of exactly:

- `dags/utils/fbref_maintenance.py` (new FBref-only wrapper);
- `dags/dag_iceberg_maintenance_daily.py` (call wrapper and task-only timeout).

The shared `dags/utils/maintenance_tasks.py` is pinned by the WhoScored runtime
contract and is byte-identical to the base. No runtime contract, image trust
root, store/policy/pipeline, parser version, batch identity or SQL pin changes.
Unit coverage exercises the actual shared janitor wrapper with fake storage and
clock: busy/release, deadline and oversleep, immediate free lock, acquisition
race/release and race/timeout, unrelated errors and per-DROP ownership failure.
A real Airflow DagBag smoke checks imports, topology and effective task limits.

## Delivery after separate owner approval

1. Obtain separate merge and delivery authorization. Require green mandatory CI
   on the reviewed PR head; merge normally, without bypass. #1322 stays open.
2. Re-read `/root/SHARED-STACK-PROTOCOL.md` and current mounts/configs. Inspection
   on 2026-10-03 found `/root/dpf-whoscored-merge/dags` mounted read-only into
   `airflow-scheduler:/opt/airflow/dags`. Verify this again; do not treat this
   historical path as permission to write. This task does not change production.
3. Use the authorized source delivery process and a coordinated quiet window.
   Both files belong to the shared scheduler tree. Verify no active daily
   maintenance task/run and no active FBref task/run; wait naturally without
   stopping ingest, forcing lock release, or changing pauses. If no such window
   is available, defer delivery. Confirm the runtime process handles this exact
   two-file patch; otherwise prepare the scoped delivery and obtain authorization
   for it. Do not invoke an older delivery script with a different file list.
4. Pin the merged SHA. Record the current two destination files (including absence
   of the new file), hashes, owners/modes and scheduler state. Check runtime drift
   against the merge base before copying: the shared DAG must retain other
   sources' current changes. Any unexplained drift blocks delivery. Snapshot
   shared maintenance and runtime-contract hashes as unchanged evidence.
5. Deliver the new module first, then the DAG, from that exact SHA, using the
   approved delivery mechanism. No Compose recreation, scheduler restart, seal
   refresh, production Python/pytest or other-source file replacement is needed.
6. Verify delivered bytes and preserved non-target hashes. Confirm the scheduler
   parsed/serialized this DAG *after* delivery, no new import errors, unchanged
   cron/task graph/pauses and 120 minutes only on FBref (30 on the other tasks).
   A local DagBag import alone is insufficient delivery evidence.
7. Observe the next scheduled nightly run without trigger/clear/unpause. During
   contention expect wait messages, unchanged ingest owner and no cleanup. After
   natural release expect atomic acquire and normal fencing/cleanup. A retained
   old stage may still fail its existing recovery gate: do not equate removal of
   this lock incident with a green janitor or accepted #1322. Record timestamps,
   control run IDs, logs and database evidence. History remains stopped.
8. Continue #1322 acceptance: three-day schedule median <=6.5 h, played matches
   fetched <=24 h on the peak day, no lock conflicts. Record the stopped-history
   limitation. Close #1322 only after actual acceptance, not merge or delivery.

## Rollback

Requires authorization and the same quiet-window checks. Restore the previous
DAG first, then remove the new wrapper only if it was absent in the snapshot and
no parsed/executing task can still reference it. Restore exact bytes/modes,
verify hashes and scheduler reparse/import errors, preserve all pauses and other
sources. Rollback restores immediate lock-conflict behavior; report this openly.
Never remove a live publisher lock or stop collection to make rollback possible.
