# FBref legacy Silver retirement (#1634)

Status: code change prepared; production delivery is not part of this document.

## Owner decision

On 2026-10-05 the owner retired the current FBref Silver implementation. A new
Silver methodology will be designed separately; the old producer must not keep
creating runs in the meantime.

## Removed behavior

- `dag_transform_fbref_silver` is no longer present.
- FBref current, backfill, and replay DAGs no longer trigger a Silver child.
- Those Bronze DAGs end after validated publication-scope export and the
  existing fail-closed publication-lock finalizer.
- The old `dags/sql/silver/fbref_*.sql` implementation and its OpenMetadata
  descriptions are removed.

The old DAG also materialised `silver.whoscored_player_unavailable`; that table
is no longer refreshed by this path. Its SQL remains available for the future
methodology because it is not an FBref transform.

## Preserved state

- Existing `iceberg.silver.fbref_*` tables and their data are not dropped or
  mutated. They are historical snapshots and must not be treated as current.
- FBref Bronze ingestion, publication-scope export, locks, budgets, and source
  acceptance are unchanged.
- Shared `dags/utils/silver_tasks.py`, xref, E3/E4, and FBref Gold code remain.
- The retained xref/E3/E4 code is dormant recovery evidence, not an approved
  current publication path. It must not read the frozen FBref Silver snapshots
  as if they were current.
- Dormant schedule/tag compatibility keys remain in `dags/utils/config.py`;
  without the DAG file they cannot create or run the retired producer.

## Cross-source publication hold

The owner confirmed on 2026-10-05 that cross-source xref/E3/E4 publication
must remain stopped until the replacement FBref Silver methodology is defined.
At `2026-10-05T11:19:49Z` (`14:19:49 МСК`) production had no running or queued
xref/E3/E4 runs, and all five entry points were paused:

- `dag_master_pipeline`;
- `dag_sofascore_pipeline`;
- `dag_transform_xref`;
- `dag_transform_e3`;
- `dag_transform_e4`.

Do not unpause or manually trigger those DAGs, and do not run a FotMob
activation path that unpauses `dag_sofascore_pipeline`, until replacement
Silver-to-xref/Gold contracts are reviewed. Existing `silver.fbref_*` tables
remain frozen rollback evidence only; their presence is not freshness proof.

## Rollback

Before delivery, revert the retirement commit. After delivery, restoring the
old producer additionally requires the normal reviewed scheduler delivery and
must not be done by manually triggering a retained Airflow DagRun. Warehouse
data requires no rollback because this change does not alter it.
