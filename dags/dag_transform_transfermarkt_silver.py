"""
Transfermarkt Silver Layer Transformation DAG
=============================================

Transforms Bronze Transfermarkt data into Silver layer Iceberg tables.

Runs after dag_ingest_transfermarkt completes (trigger-only, no schedule).
Uses CTAS via Trino to deduplicate Bronze, join silver.xref_player for
canonical_id, and rename columns to issue #60 DoD spec.

Architecture:
    Triggered by dag_ingest_transfermarkt via TriggerDagRunOperator
        |
        v
    TaskGroup: silver_transforms
        └── players  — typed player snapshot with canonical_id
        |
        v
    validate_silver  — row count checks
        |
        v
    validate_silver_quality  — DQ checks (PK, ref integrity WARNING, ranges, orphan coverage)

Silver Tables Created:
    iceberg.silver.transfermarkt_players  — typed snapshot + canonical_id (issue #60)
"""

from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.default_args import SILVER_ARGS

# ---------------------------------------------------------------------------
# Silver transform definitions
# ---------------------------------------------------------------------------
# Each entry: (task_id, sql_file relative to /opt/airflow/, target table name)
SILVER_TRANSFORMS = [
    (
        'players',
        'dags/sql/silver/transfermarkt_players.sql',
        'transfermarkt_players',
    ),
]

# Expected minimum row counts per Silver table (for validation)
# APL 2025/26 Bronze = 528 rows; DoD floor = 400.
SILVER_MIN_ROWS = {
    'transfermarkt_players': 400,
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _run_transform(sql_file: str, table_name: str, **context) -> Dict[str, Any]:
    """PythonOperator callable — run a single Silver CTAS transform."""
    from utils.silver_tasks import run_silver_transform

    return run_silver_transform(
        sql_file=sql_file,
        table_name=table_name,
        schema='silver',
    )


def _validate_silver(**context) -> Dict[str, Any]:
    """PythonOperator callable — validate row counts in Silver tables."""
    import logging

    from airflow.exceptions import AirflowException
    from utils.silver_tasks import validate_silver_tables

    logger = logging.getLogger(__name__)

    validation = validate_silver_tables(
        tables=SILVER_MIN_ROWS,
        min_rows=1,
    )

    logger.info(f"Silver validation: {validation['status']}")
    logger.info(f"Details: {validation['details']}")

    if validation['warnings']:
        for w in validation['warnings']:
            logger.warning(f"  {w}")
        raise AirflowException(
            f"Silver validation FAILED: {len(validation['warnings'])} "
            f"table(s) below threshold. {validation['warnings']}"
        )

    return validation


def _validate_silver_quality(**context) -> Dict[str, Any]:
    """PythonOperator callable — run DQ checks on Silver tables.

    ERROR-level: PK NULLs / PK uniqueness — block DAG to prevent dirty Gold.
    WARNING-level: ref_integrity (orphan tolerated), freshness, value ranges,
                   canonical_id coverage (orphan-rate proxy).
    """
    import logging

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    logger = logging.getLogger(__name__)

    # TM ingest is weekly (Monday 04:00 UTC, per utils/config.py SCHEDULES).
    # 48h grace covers Monday→Wednesday; staleness past that is observable
    # but not a blocker (severity=WARNING).
    FRESH_HOURS = 48

    checks = [
        # PK + critical NULLs — ERROR (PK breaks downstream JOINs).
        # canonical_id is EXCLUDED — TM has ~10% structural orphans (loan-out,
        # new transfers) per feedback_xref_player_tm_capology.md.
        CHECK.no_nulls(
            'silver.transfermarkt_players',
            cols=['player_id', 'name', 'league', 'season'],
        ),

        # PK uniqueness — ERROR (duplicates would explode downstream facts).
        # PK is (player_id, league, season), NOT canonical_id — orphans
        # carry canonical_id=NULL which would break uniqueness on that key.
        CHECK.no_duplicates(
            'silver.transfermarkt_players',
            pk=['player_id', 'league', 'season'],
        ),

        # canonical_id coverage — WARNING/ERROR by ratio.
        # Live APL 2025/26 measurement: 89.8% non-orphan.
        #   warn_threshold=0.88 → above 88% PASS
        #   error_threshold=0.80 → 80-88% WARNING, <80% ERROR
        # See feedback_xref_player_tm_capology.md.
        CHECK.coverage(
            'silver.transfermarkt_players',
            column='canonical_id',
            warn_threshold=0.88,
            error_threshold=0.80,
            severity='WARNING',
            name='canonical_coverage[silver.transfermarkt_players]',
        ),

        # Ref integrity to xref_player — WARNING (orphan rows expected).
        CHECK.ref_integrity(
            'silver.transfermarkt_players',
            'silver.xref_player',
            'canonical_id',
            severity='WARNING',
        ),

        # Freshness — WARNING (weekly ingest, 48h grace).
        CHECK.freshness(
            'silver.transfermarkt_players',
            ts_col='_bronze_ingested_at',
            max_age_hours=FRESH_HOURS,
            severity='WARNING',
        ),

        # Value ranges — WARNING (outlier observability).
        CHECK.value_range(
            'silver.transfermarkt_players', 'height_cm',
            min_val=150, max_val=220, severity='WARNING',
        ),
        CHECK.value_range(
            'silver.transfermarkt_players', 'age',
            min_val=14, max_val=50, severity='WARNING',
        ),
        CHECK.value_range(
            'silver.transfermarkt_players', 'current_market_value_eur',
            min_val=0, severity='WARNING',
        ),
    ]

    # Run with raise_on_error=False so the Telegram summary always lands.
    report = run_checks(checks, raise_on_error=False)
    logger.info(f"Silver DQ: {report.summary()}")

    telegram_dq_summary(report, header="TM Silver DQ")

    if report.errors:
        from airflow.exceptions import AirflowException
        raise AirflowException(
            f"Silver DQ failed: {len(report.errors)} error(s). "
            + "; ".join(
                f"{r.name}: {r.details or r.error}" for r in report.errors[:5]
            )
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
    dag_id='dag_transform_transfermarkt_silver',
    default_args=SILVER_ARGS,
    description='Transform Bronze Transfermarkt data into Silver Iceberg tables via Trino CTAS',
    schedule=None,  # Trigger-only (called after dag_ingest_transfermarkt)
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['transform', 'transfermarkt', 'silver', 'football', 'trino'],
    max_active_runs=1,
    max_active_tasks=1,  # Sequential CTAS to prevent OOM (~1.2GB per task)
    doc_md="""
    ## Transfermarkt Silver Transformation

    Transforms Bronze TM player snapshots into typed Silver tables with
    `canonical_id` bridging via `silver.xref_player`.

    ### Trigger

    Triggered by `dag_ingest_transfermarkt` via TriggerDagRunOperator.

    ### Silver Tables

    | Table | Description | Bronze sources |
    |-------|-------------|----------------|
    | `transfermarkt_players` | Typed player snapshot + canonical_id (issue #60) | `transfermarkt_players` |

    ### Data Quality Checks

    - **PK NULLs / uniqueness** (ERROR): blocks DAG to protect downstream Gold
    - **canonical_id coverage** (WARNING/ERROR by ratio): ≥88% non-orphan PASS,
      80–88% WARNING, <80% ERROR. Live APL 2025/26 = 89.8%.
    - **ref_integrity to xref_player** (WARNING): orphan TM players expected
    - **Freshness** (WARNING): 48h grace post-Monday ingest
    - **Value ranges** (WARNING): height_cm 150–220, age 14–50, MV ≥ 0
    """,
) as dag:

    # =========================================================================
    # TaskGroup: Silver Transforms (single task for now — extend as TM_mv,
    # TM_transfers, etc. land per #59 closing note).
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
    # Validation: row count
    # =========================================================================
    validate_silver = PythonOperator(
        task_id='validate_silver',
        python_callable=_validate_silver,
        trigger_rule='all_done',
    )

    # =========================================================================
    # Quality: DQ checks
    # =========================================================================
    validate_quality = PythonOperator(
        task_id='validate_silver_quality',
        python_callable=_validate_silver_quality,
        trigger_rule='all_done',
    )

    transforms_group >> validate_silver >> validate_quality
