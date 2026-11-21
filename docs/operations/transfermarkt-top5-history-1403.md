# Transfermarkt #1403: top-5 native history and bounded canary

## Scope

This document defines the offline acceptance matrix and the gate for one later GB1/2025 historical canary. It does not authorize a live request, Airflow trigger, backfill, quarantine, deletion or production deployment.

The implementation reuses `dag_backfill_transfermarkt`: native-only writes, raw-first capture, frozen standing policy, checkpointed claims and snapshot-pinned finalizer DQ. It does not add a scope selector or change the queue. `max_batch=1` limits batch size but does not prove which existing/recoverable scope will run.

## Deterministic target

`dags/configs/transfermarkt/top5_history_acceptance.json` is the versioned source of truth:

- GB1, ES1, IT1, L1, FR1;
- edition IDs 2015…2025, meaning 2015/16…2025/26;
- five player entities: squad memberships, attributes, contracts, market values and transfers;
- exactly 55 competition/edition scopes and deterministic `scope_id` values;
- legacy league/season mappings for bidirectional reconciliation.

From an isolated verified worktree, validate and print the matrix without Trino or source access:

```bash
PYTHONPATH=dags:. /root/.venvs/dpf-test/bin/python \
  dags/scripts/transfermarkt_top5_history.py matrix > matrix.json
```

The expected scope count is 55. Current GB1/2026 and FR1/2026 rows are outside this target and never count toward it.

## Offline SQL generation

Create `coverage-pins.json` as an object whose keys are exactly the five native table names in the matrix and whose values are positive integer Iceberg snapshot IDs selected after the measured batch stopped writing. Then print the SELECT:

```bash
PYTHONPATH=dags:. /root/.venvs/dpf-test/bin/python \
  dags/scripts/transfermarkt_top5_history.py coverage-sql \
  --snapshots-json coverage-pins.json > coverage.sql
```

For one roster reconciliation, create `roster-pins.json` with snapshots for squad, attributes, contracts and `iceberg.bronze.transfermarkt_players`:

```bash
PYTHONPATH=dags:. /root/.venvs/dpf-test/bin/python \
  dags/scripts/transfermarkt_top5_history.py reconciliation-sql \
  --competition-id GB1 --edition-id 2025 \
  --snapshots-json roster-pins.json > roster-reconciliation.sql
```

Both outputs are SELECT statements with explicit `FOR VERSION AS OF` pins. Execute them read-only and save the returned JSON arrays without player-level row dumps in model packages.

## Before any GB1/2025 canary

The existing history order puts GB1/2025 first when it is the newest pending historical target and no retry/recovery takes precedence. The unit contract is `tests/unit/utils/test_transfermarkt_history_order.py`; the production claim still requires fresh state evidence.

Before the separately authorized run, record:

1. Actual deployed writer/parser/schema revision and promoted registry snapshot.
2. The 55-scope matrix hash and all required standing-policy caps.
3. Current campaign, scope and batch readback proving that the single new claim is GB1/2025. An unrelated claimed/running/retryable batch is recovered first and must not be relabelled as this canary.
4. No platform block, no unrelated open incident, available bounded history stream and sufficient reserved proxy budget.
5. Explicit owner authorization for the paid source capture and native production writes.

Only then use the existing DAG with this exact run configuration:

```json
{"max_batch": 1, "resume_platform_block": false}
```

Do not trigger it from this development task without that separate authorization.

## Canary evidence and native gate

After the mapped batch has stopped writing, require the actual finalizer receipt and physical readback to agree on:

- `competition_id=GB1`, `edition_id=2025` and the deterministic matrix `scope_id`;
- one campaign/batch/claim generation and the child cycle IDs;
- successful native-only scope manifest and finalizer DQ;
- raw capture IDs/body hashes matching Bronze lineage;
- request/retry/byte counters within the frozen standing policy;
- no Silver trigger and no second scope.

Run the generated coverage SELECT and save its five GB1/2025 rows as `gb1-2025-coverage.json`. Run the roster reconciliation SELECT and save its three rows as `gb1-2025-reconciliation.json`.

```bash
PYTHONPATH=dags:. /root/.venvs/dpf-test/bin/python \
  dags/scripts/transfermarkt_top5_history.py check-coverage \
  --rows-json gb1-2025-coverage.json \
  --competition-id GB1 --edition-id 2025 --gate native

PYTHONPATH=dags:. /root/.venvs/dpf-test/bin/python \
  dags/scripts/transfermarkt_top5_history.py check-reconciliation \
  --rows-json gb1-2025-reconciliation.json
```

The native gate requires nonzero native rows, complete raw lineage and bidirectional roster equality. Before quarantine it may return `NATIVE_PASS_QUARANTINE_HOLD`; this is not final issue PASS. The pre-existing NULL rows remain listed in `quarantine_holds`.

Also run a SELECT proving that no new NULL-scope row has `cycle_id`/`scope_id` from the canary. A successful current writer or zero post-cutoff DQ does not erase the old cohort.

## Final #1403 gate and remaining work

The final gate is deliberately stricter:

```bash
PYTHONPATH=dags:. /root/.venvs/dpf-test/bin/python \
  dags/scripts/transfermarkt_top5_history.py check-coverage \
  --rows-json top5-coverage.json --gate final
```

It requires all 275 entity/scope results, nonzero native rows, complete lineage and zero target NULL-scope rows. `PASS` from this command alone still does not close the issue: record legacy reconciliation and every source-caused discrepancy.

Remaining before closure:

- complete and reconcile all 55 native scopes, not only GB1/2025;
- obtain source completeness evidence for 1516 because legacy has no rows for that season;
- classify the residual NULL cohort, including 9,050 rows without legacy key matches and 12,643 coach stints;
- only after native replacement and residual classification, obtain separate authorization for quarantine or deletion and rerun the final gate;
- retain the issue open until actual production evidence satisfies every criterion.

No DML or quarantine statement is supplied by this runbook.
