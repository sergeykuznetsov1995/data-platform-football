# Trino: completed INSERT retention (#1620)

## Change and scope

Set `query.min-expire-age=10s` and `task.info.max-age=10s` together. Keep
`query.max-history` at its default 100, heap/cgroup/query-memory limits,
authentication, connectors and concurrency unchanged. This bounds retention of
completed query graphs; it does not limit active query memory or fix every
possible source of JVM OOM. Recent task details expire sooner.

This is a shared-service change, separate from FotMob PR #1618. It requires the
owner's shared-Trino delivery approval and runtime acceptance. No production
configuration or delivery protection was changed during preparation.

## Evidence

On 2–3 October 2026, 11 Java heap OOMs interrupted parallel Bronze activity.
The latest recorded failure was `2026-10-03T09:10:11Z` (12:10:11 MSK), restart 23.
The JVM limit was 5 GiB, cgroup 12 GiB, per-query memory 3 GiB, headroom 384 MiB;
cgroup `oom` and `oom_kill` were zero. The October failure affects FotMob,
SofaScore and other shared clients; coincidence with a query is not proof that
that query caused OOM.

An isolated Trino 482 instance using the production image digest
`sha256:90b35b7c603eaa1f889bf03981a62b75f998ee6c0f851d9f4e341b49a57022b6`
reproduced `Java heap space` by sending 280 KB literal INSERTs to the blackhole
connector. The sink stores no rows. Four clients, 1500 MiB heap, 3 GiB cgroup,
2 CPUs and no network access or production mounts were used throughout.

- Default retention: OOM after more than 800 completed INSERTs.
- Query history alone (`max-history=20`, `min-expire-age=10s`): OOM after more
  than 800 INSERTs, despite the smaller visible query history.
- Task retention alone (`task.info.max-age=10s`, default query history): OOM
  after more than 1000 INSERTs.
- Both expiry settings, default max-history 100: all 2500 INSERTs passed in
  152.7 seconds; after expiry, 220 additional small queries, a 15-second delayed
  result consumer and test-only full GC, heap used was 152,511,984 bytes with
  zero query reservation. The delayed client received its original result.

A completed test heap dump after 400 INSERTs contains 401 FINISHED query state
machines and 1202 FINISHED tasks. A strong reference path, excluding weak
references, proves that `SqlTaskManager.tasks` holds completed `SqlTask` objects;
`TaskStateMachine.sourceTaskFailureListeners` reaches a stage scheduler and the
finished `QueryStateMachine`. Large SQL text, AST literals and plan JSON remain
reachable. The query memory pool reports zero after the writes complete, so its
limits do not account for this retained graph.

The older production dump (`2026-09-29T04:10:44.103Z`) has a different dominant
path: 32 WindowOperators reach 4.124 GB of unique byte-array payload. This is
neither measured retained size nor proof of the October owner. No current
production heap dump was forced. The synthetic result establishes a real
retention failure in the installed engine; its contribution to each historical
October crash cannot be quantified from the available production logs.

Official Trino 482 sources:

- [QueryTracker expiration](https://github.com/trinodb/trino/blob/482/core/trino-main/src/main/java/io/trino/execution/QueryTracker.java)
- [SqlTaskManager completed-task cache](https://github.com/trinodb/trino/blob/482/core/trino-main/src/main/java/io/trino/execution/SqlTaskManager.java)
- [TaskManagerConfig property name](https://github.com/trinodb/trino/blob/482/core/trino-main/src/main/java/io/trino/execution/TaskManagerConfig.java)

Local evidence: `/root/fotmob-heap-analysis/`. The exact 17-edge path is in
`repro/task-manager-to-finished-query-path.json`; `repro/load.py` is the synthetic
client, `repro/*-load.log` and `repro/*-server.log` record comparisons. The test
container is deliberately separate (`trino-1620-repro`, network none). Do not
run the workload against production.

## Delivery and acceptance

After approval, verify the actual Trino mount and Compose labels again under
`/root/SHARED-STACK-PROTOCOL.md`. Prepare a copy of the live config, require an
exact preimage match, add only these two properties, and save the old file for
rollback. Restart only Trino in a coordinated quiet window; do not recreate the
shared Compose project or repoint scheduler mounts. The existing live heap dump
must remain intact. Verify effective properties in bootstrap logs and health,
then observe normal parallel source traffic; do not launch new ingestion waves.

Rollback restores the exact saved config and restarts only Trino in the same
coordinated procedure. A failed query, a new JVM OOM or inability to consume
results requires investigation; changing retention is not permission to hide
errors, delete staging tables, bypass delivery guards or rewrite C1 state.

Acceptance requires an OOM-free representative period of parallel Bronze
traffic, successful interrupted-source writes, and the existing FotMob gates:
#1618 delivered by the approved mechanism, no source import errors, three
consecutive successful post-delivery ingest runs, preserved 13 old Silver
tables/inactive Silver DAGs, #1284's compaction and two refresh-wave criteria,
and three consecutive C1 days at least 99%. Merge alone is not acceptance.

## Pre-delivery verification (2026-10-03)

`/root/.venvs/dpf-test/bin/pytest tests/unit -q` completed: 13,365 passed,
24 skipped, 10 failed (1006.99 s). All 10 failures are in
`tests/unit/scripts/test_bench_whoscored_capacity.py`; the exact failed subset
also fails on clean base `1efea301960e1c02dcf0fa3911d1558f7668ca3b` (2.08 s).
They concern the host PID-namespace helper hash/runtime identity and are not
introduced by these configuration changes. This comparison does not waive CI
or turn the full suite green. Logs: `unit-suite-1620.log` and
`baseline-failed-subset-1620.log` under the local evidence directory.

Separate review checked the configuration and reproduction, then verified the
fix for one delivery-script defect: configuration replacement is now atomic,
with metadata preservation and file/directory fsync. Six isolated fault/guard
checks pass; an additional reviewer check confirms rollback after directory
fsync failure. Reports: `review-1620-round1.md` and `review-1620-round2.md`.
The local delivery package is `/root/trino-1620-delivery-20261003`; its default
`apply.py check` is read-only and passed against the current mount, image and
configuration preimage. Applying it still requires explicit shared-Trino
approval and a coordinated quiet window.
