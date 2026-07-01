# football_transform — dbt-trino (Tier-2 pilot)

Pilot porting the **fbref_silver** transforms from inline Trino CTAS
(`dags/utils/silver_tasks.py`) to versioned dbt models with tests + lineage.
Scope: the 9 `fbref_*` Silver models. Orchestrated in Airflow via
astronomer-cosmos (`dags/dag_dbt_fbref_silver.py`).

## Layout
- `models/silver/*.sql` — one model per legacy `dags/sql/silver/fbref_*.sql`.
  bronze refs use `{{ source('bronze', ...) }}`; `_silver_created_at` mirrors
  the legacy CTAS wrapper; partitioning `(league, season)` per-model config.
- `models/silver/_sources.yml` — Bronze source tables.
- `models/silver/_silver__models.yml` — tests mirroring the DAG's ERROR DQ.
- `profiles.yml` — Trino over HTTPS (self-signed cert via `TRINO_CERT_PATH`).

## Run
```bash
export DBT_PROFILES_DIR=$(pwd)
dbt deps
dbt parse                       # structural validation (no warehouse needed)
dbt debug                       # connection test (needs Trino cert)
dbt build --select silver       # needs Bronze data present
```
Target schema is `silver_dbt` (isolated from live `silver` for A/B parity).
Cutover flips the profile schema to `silver` and retires the legacy DAG.
