"""
FBref Silver Layer Transformation DAG
======================================

Transforms Bronze FBref data into Silver layer Iceberg tables.

Runs after dag_ingest_fbref completes (trigger-only, no schedule).
Uses CTAS (CREATE TABLE AS SELECT) via Trino to deduplicate Bronze data,
join multiple tables, cast types, and produce clean analytical tables.

Architecture:
    Triggered by dag_ingest_fbref via TriggerDagRunOperator (or manual trigger)
        |
        v
    TaskGroup: silver_transforms (7 SEQUENTIAL tasks, max_active_tasks=1)
        ├── player_season_profile  — wide player profile per season
        ├── keeper_profile         — goalkeeper profile per season
        ├── match_enriched         — enriched match data
        ├── player_match_stats     — per-match player stats with correct types
        ├── match_events           — detailed match events (goals, cards, subs)
        ├── match_lineups          — detailed lineup entries per match
        └── shot_events            — per-shot xG data (optional Bronze table)
        |
        v
    validate_silver  — row count checks
        |
        v
    validate_silver_quality  — data quality checks (null rate, ref integrity, ranges)

Silver Tables Created:
    iceberg.silver.fbref_player_season_profile  — player stats + shooting + playingtime + misc
    iceberg.silver.fbref_keeper_profile         — goalkeeper stats + keeper + shooting + misc
    iceberg.silver.fbref_match_enriched         — schedule + team_stats + events + lineups
    iceberg.silver.fbref_player_match_stats     — per-match stats with TRY_CAST types
    iceberg.silver.fbref_match_events           — detailed match events (deduplicated)
    iceberg.silver.fbref_match_lineups          — detailed lineup entries (deduplicated)
    iceberg.silver.fbref_shot_events            — per-shot xG data (if Bronze exists)
"""

from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from utils.default_args import SILVER_ARGS

# ---------------------------------------------------------------------------
# Silver transform definitions
# ---------------------------------------------------------------------------
# Each entry: (task_id, sql_file relative to /opt/airflow/, target table name)
SILVER_TRANSFORMS = [
    (
        'player_season_profile',
        'dags/sql/silver/fbref_player_season_profile.sql',
        'fbref_player_season_profile',
    ),
    (
        'keeper_profile',
        'dags/sql/silver/fbref_keeper_profile.sql',
        'fbref_keeper_profile',
    ),
    (
        'match_enriched',
        'dags/sql/silver/fbref_match_enriched.sql',
        'fbref_match_enriched',
    ),
    (
        'player_match_stats',
        'dags/sql/silver/fbref_player_match_stats.sql',
        'fbref_player_match_stats',
    ),
    (
        'match_events',
        'dags/sql/silver/fbref_match_events.sql',
        'fbref_match_events',
    ),
    (
        'match_lineups',
        'dags/sql/silver/fbref_match_lineups.sql',
        'fbref_match_lineups',
    ),
    (
        'shot_events',
        'dags/sql/silver/fbref_shot_events.sql',
        'fbref_shot_events',
    ),
    (
        'team_season_profile',
        'dags/sql/silver/fbref_team_season_profile.sql',
        'fbref_team_season_profile',
    ),
]

# Expected minimum row counts per Silver table (for validation)
SILVER_MIN_ROWS = {
    'fbref_player_season_profile': 100,
    'fbref_keeper_profile': 10,
    'fbref_match_enriched': 50,
    'fbref_player_match_stats': 100,
    'fbref_match_events': 50,
    'fbref_match_lineups': 100,
    'fbref_shot_events': 50,
    'fbref_team_season_profile': 10,
}

# Bronze tables that may not exist yet — skip transform with warning if absent
OPTIONAL_BRONZE_TABLES = {
    'fbref_shot_events': 'fbref_shot_events',  # Silver table -> Bronze table
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _run_transform(sql_file: str, table_name: str, **context) -> Dict[str, Any]:
    """
    PythonOperator callable — run a single Silver CTAS transform.

    Imports are inside the function to avoid import errors at DAG parse time
    (scrapers/ may not be importable on the scheduler).
    """
    from utils.silver_tasks import check_bronze_table_exists, run_silver_transform

    # Check if Bronze source table is optional and may not exist
    bronze_table = OPTIONAL_BRONZE_TABLES.get(table_name)
    if bronze_table:
        import logging
        log = logging.getLogger(__name__)
        if not check_bronze_table_exists(bronze_table):
            log.warning(
                f"Bronze table iceberg.bronze.{bronze_table} does not exist — "
                f"skipping Silver transform for {table_name}"
            )
            return {
                'table': f'iceberg.silver.{table_name}',
                'rows': 0,
                'status': 'skipped',
                'error': f'Bronze table {bronze_table} not found',
            }

    result = run_silver_transform(
        sql_file=sql_file,
        table_name=table_name,
        schema='silver',
    )
    return result


def _validate_silver(**context) -> Dict[str, Any]:
    """
    PythonOperator callable — validate all Silver tables after transforms.

    Checks row counts and logs warnings for tables below threshold.
    """
    import logging

    from airflow.exceptions import AirflowException
    from utils.silver_tasks import validate_silver_tables

    logger = logging.getLogger(__name__)

    # Exclude optional tables (no Bronze data) from strict validation
    required_tables = {
        k: v for k, v in SILVER_MIN_ROWS.items()
        if k not in OPTIONAL_BRONZE_TABLES
    }

    validation = validate_silver_tables(
        tables=required_tables,
        min_rows=1,
    )

    logger.info(f"Silver validation result: {validation['status']}")
    logger.info(f"Table details: {validation['details']}")
    logger.info(f"Total rows: {validation.get('total_rows', 0)}")

    if validation['warnings']:
        for w in validation['warnings']:
            logger.warning(f"  {w}")

    # Fail the task if any REQUIRED table is below its threshold
    if validation['warnings']:
        raise AirflowException(
            f"Silver validation FAILED: {len(validation['warnings'])} table(s) "
            f"below threshold. Warnings: {validation['warnings']}"
        )

    return validation


def _validate_silver_quality(**context) -> Dict[str, Any]:
    """
    PythonOperator callable — run data quality checks on Silver tables.

    Critical checks (ERROR): no_nulls on PKs, PK uniqueness, referential
    integrity. Failures raise AirflowException so dirty data never reaches Gold.
    Soft checks (WARNING): freshness, value ranges (logged, not blocking).
    """
    import logging

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    logger = logging.getLogger(__name__)

    # Freshness threshold: ingestion runs weekly (Monday). 48h gives a 2-day
    # post-ingest grace window in which Silver should always be fresh; outside
    # that window staleness is expected, so the severity stays WARNING.
    FRESH_HOURS = 48

    checks = [
        # Primary-key nulls — ERROR (joins/dedup logic break otherwise)
        CHECK.no_nulls('silver.fbref_match_enriched', cols=['match_id', 'date']),
        CHECK.no_nulls('silver.fbref_player_season_profile', cols=['player_id', 'league', 'season']),
        CHECK.no_nulls('silver.fbref_keeper_profile', cols=['player_id', 'league', 'season']),
        CHECK.no_nulls('silver.fbref_player_match_stats', cols=['match_id']),
        CHECK.no_nulls('silver.fbref_match_events', cols=['match_id']),
        CHECK.no_nulls('silver.fbref_match_lineups', cols=['match_id', 'player_id']),
        CHECK.no_nulls('silver.fbref_team_season_profile', cols=['team', 'league', 'season']),

        # PK uniqueness — ERROR (duplicates would explode downstream joins / facts)
        CHECK.no_duplicates('silver.fbref_match_enriched', pk=['match_id']),
        CHECK.no_duplicates('silver.fbref_player_season_profile', pk=['player_id', 'league', 'season']),
        CHECK.no_duplicates('silver.fbref_keeper_profile', pk=['player_id', 'league', 'season']),
        CHECK.no_duplicates('silver.fbref_match_lineups', pk=['match_id', 'player_id']),
        CHECK.no_duplicates('silver.fbref_team_season_profile', pk=['team', 'league', 'season']),
        # player_match_stats rows can repeat across player+match when player switches teams
        # mid-game (rare); team is part of the natural key.
        CHECK.no_duplicates(
            'silver.fbref_player_match_stats',
            pk=['match_id', 'player_id', 'team'],
            where='player_id IS NOT NULL',
        ),
        CHECK.no_duplicates(
            'silver.fbref_match_events',
            pk=['match_id', 'minute', 'player_id', 'event_type'],
            where='player_id IS NOT NULL',
        ),

        # Referential integrity — ERROR (orphans would silently drop in fact joins)
        CHECK.ref_integrity('silver.fbref_player_match_stats', 'silver.fbref_match_enriched', 'match_id'),
        CHECK.ref_integrity('silver.fbref_match_events', 'silver.fbref_match_enriched', 'match_id'),
        CHECK.ref_integrity('silver.fbref_match_lineups', 'silver.fbref_match_enriched', 'match_id'),

        # Freshness — WARNING (ingestion is weekly; >48h after Monday is normal mid-week)
        CHECK.freshness('silver.fbref_match_enriched', ts_col='_bronze_ingested_at',
                        max_age_hours=FRESH_HOURS, severity='WARNING'),
        CHECK.freshness('silver.fbref_player_season_profile', ts_col='_bronze_ingested_at',
                        max_age_hours=FRESH_HOURS, severity='WARNING'),
        CHECK.freshness('silver.fbref_keeper_profile', ts_col='_bronze_ingested_at',
                        max_age_hours=FRESH_HOURS, severity='WARNING'),
        CHECK.freshness('silver.fbref_player_match_stats', ts_col='_bronze_ingested_at',
                        max_age_hours=FRESH_HOURS, severity='WARNING'),
        CHECK.freshness('silver.fbref_match_events', ts_col='_bronze_ingested_at',
                        max_age_hours=FRESH_HOURS, severity='WARNING'),
        CHECK.freshness('silver.fbref_match_lineups', ts_col='_bronze_ingested_at',
                        max_age_hours=FRESH_HOURS, severity='WARNING'),
        CHECK.freshness('silver.fbref_team_season_profile', ts_col='_bronze_ingested_at',
                        max_age_hours=FRESH_HOURS, severity='WARNING'),

        # Value ranges — WARNING (legitimate outliers possible; for monitoring only)
        CHECK.value_range('silver.fbref_player_season_profile', 'goals', min_val=0, severity='WARNING'),
        CHECK.value_range('silver.fbref_player_season_profile', 'minutes',
                          min_val=0, max_val=5000, severity='WARNING'),
        CHECK.value_range('silver.fbref_keeper_profile', 'save_pct',
                          min_val=0, max_val=100, severity='WARNING'),
        CHECK.value_range('silver.fbref_team_season_profile', 'possession',
                          min_val=0, max_val=100, severity='WARNING'),
        CHECK.value_range('silver.fbref_team_season_profile', 'goals',
                          min_val=0, severity='WARNING'),
    ]

    # Run with raise_on_error=False so we can push a summary before failing.
    report = run_checks(checks, raise_on_error=False)
    logger.info(f"Silver DQ: {report.summary()}")

    # Telegram summary (no-op if token not configured)
    telegram_dq_summary(report, header="Silver DQ")

    if report.errors:
        from airflow.exceptions import AirflowException
        raise AirflowException(
            f"Silver DQ failed: {len(report.errors)} error(s). "
            + "; ".join(f"{r.name}: {r.details or r.error}" for r in report.errors[:5])
        )

    return {
        'passed': len(report.passed),
        'total': len(report.results),
        'errors': [r.name for r in report.errors],
        'warnings': [r.name for r in report.warnings],
    }


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='dag_transform_fbref_silver',
    default_args=SILVER_ARGS,
    description='Transform Bronze FBref data into Silver Iceberg tables via Trino CTAS',
    schedule=None,  # Trigger-only (called after ingestion)
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['transform', 'fbref', 'silver', 'football', 'trino'],
    max_active_runs=1,
    max_active_tasks=1,  # Sequential execution to prevent OOM (each CTAS + import ~1.2GB)
    doc_md="""
    ## FBref Silver Transformation

    Transforms raw Bronze-layer FBref data into clean, deduplicated Silver tables.

    ### Trigger

    This DAG has **no schedule** — it is triggered after `dag_ingest_fbref`
    completes via `TriggerDagRunOperator` or manual trigger.

    ### Silver Tables

    | Table | Description | Sources |
    |-------|-------------|---------|
    | `fbref_player_season_profile` | Wide player profile per season | stats, shooting, playingtime, misc |
    | `fbref_keeper_profile` | Goalkeeper profile per season | stats (GK), keeper, shooting, misc |
    | `fbref_match_enriched` | Match data + team stats + events | schedule, team_stats, events, lineups |
    | `fbref_player_match_stats` | Per-match player stats (typed) | match_player_stats |
    | `fbref_match_events` | Detailed match events | match_events |
    | `fbref_match_lineups` | Detailed lineup entries | lineups |
    | `fbref_shot_events` | Per-shot xG data (optional) | shot_events |

    ### Transformations Applied

    - **Deduplication**: ROW_NUMBER by natural key, keep latest `_ingested_at`
    - **Type casting**: `TRY_CAST` for VARCHAR numeric columns
    - **Joins**: Multi-table LEFT JOINs for profile tables
    - **Aggregation**: Event counts via `FILTER(WHERE ...)`
    - **Renaming**: All columns renamed to readable snake_case aliases
    - **Lineage**: `_bronze_ingested_at` preserved from source

    ### Data Quality Checks

    After transforms and row count validation, the pipeline runs quality checks:
    - **PK NULLs / uniqueness** (ERROR): blocks the DAG to prevent dirty data flowing to Gold
    - **Referential integrity** (ERROR): orphan match_id in child tables fails the run
    - **Freshness** (WARNING): `_bronze_ingested_at` within 48h of Monday ingest
    - **Value ranges** (WARNING): outliers for monitoring, do not block

    ### Manual Trigger

    ```bash
    airflow dags trigger dag_transform_fbref_silver
    ```
    """,
) as dag:

    # =========================================================================
    # TaskGroup: Silver Transforms (SEQUENTIAL — max_active_tasks=1 to prevent OOM)
    # =========================================================================
    with TaskGroup(group_id='silver_transforms') as transforms_group:
        for task_id, sql_file, table_name in SILVER_TRANSFORMS:
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={
                    'sql_file': sql_file,
                    'table_name': table_name,
                },
            )

    # =========================================================================
    # Validation: check row counts in Silver tables
    # =========================================================================
    validate_silver = PythonOperator(
        task_id='validate_silver',
        python_callable=_validate_silver,
        trigger_rule='all_done',  # Run even if some transforms fail
    )

    # =========================================================================
    # Quality: data quality checks (null rate, ref integrity, ranges)
    # =========================================================================
    validate_quality = PythonOperator(
        task_id='validate_silver_quality',
        python_callable=_validate_silver_quality,
        trigger_rule='all_done',  # Run even if validation fails
    )

    # =========================================================================
    # Trigger Gold layer after Silver DQ passes
    # =========================================================================
    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_gold',
        trigger_dag_id='dag_transform_fbref_gold',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # =========================================================================
    # Dependencies
    # =========================================================================
    transforms_group >> validate_silver >> validate_quality >> trigger_gold
