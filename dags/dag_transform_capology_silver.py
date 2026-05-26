"""
Capology Silver Layer Transformation DAG
========================================

Transforms Bronze Capology salary data into Silver layer Iceberg tables.

Runs after dag_ingest_capology completes (trigger-only, no schedule).
Uses CTAS via Trino to deduplicate Bronze, join silver.xref_player for
canonical_id, type-cast salary fields to DECIMAL, and emit one row per
(player_slug, league, season) with active+loan filter applied.

Architecture:
    Triggered by dag_ingest_capology via TriggerDagRunOperator
        |
        v
    TaskGroup: silver_transforms
        └── player_salaries — typed salary snapshot with canonical_id (issue #63)
        |
        v
    validate_silver  — row count checks
        |
        v
    validate_silver_quality  — DQ checks (PK, ref integrity WARNING, ranges, orphan coverage)

Silver Tables Created:
    iceberg.silver.capology_player_salaries  — typed salary snapshot + canonical_id (issue #63)
"""

import logging
from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.default_args import SILVER_ARGS

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Silver transform definitions
# ---------------------------------------------------------------------------
# Each entry: (task_id, sql_file relative to /opt/airflow/, target table name).
SILVER_TRANSFORMS = [
    (
        'player_salaries',
        'dags/sql/silver/capology_player_salaries.sql',
        'capology_player_salaries',
    ),
]

# Expected minimum row counts per Silver table (for validation).
# capology_player_salaries: Bronze APL 2025/26 = 730 rows; после filter
# (currency='GBP' AND (active OR loan)) live ≈ 525 rows. Floor = 400
# защищает от broken CTAS / scrape regression.
SILVER_MIN_ROWS = {
    'capology_player_salaries': 400,
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
    from airflow.exceptions import AirflowException
    from utils.silver_tasks import validate_silver_tables

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
    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    # Capology ingest weekly cadence (см. dag_ingest_capology). 48h grace
    # covers Monday→Wednesday; staleness past that is observable, не блокер.
    FRESH_HOURS = 48

    checks = [
        # ---------------------------------------------------------------
        # silver.capology_player_salaries (issue #63)
        # ---------------------------------------------------------------

        # PK + critical NULLs — ERROR (PK breaks downstream JOINs).
        # canonical_id EXCLUDED — Capology ~10.5% structural orphans
        # (release/youth/no-FBref-counterpart) per
        # feedback_xref_player_tm_capology.md.
        CHECK.no_nulls(
            'silver.capology_player_salaries',
            cols=['player_slug', 'weekly_gross_gbp', 'league', 'season'],
        ),

        # PK uniqueness — ERROR. Natural key (player_slug, league, season).
        CHECK.no_duplicates(
            'silver.capology_player_salaries',
            pk=['player_slug', 'league', 'season'],
        ),

        # Canonical-grain dedup — ERROR. WHERE filter обязателен:
        # canonical_id NULL для orphans иначе чек упадёт на NULL'ах.
        CHECK.no_duplicates(
            'silver.capology_player_salaries',
            pk=['canonical_id', 'season'],
            where='canonical_id IS NOT NULL',
        ),

        # Row count floor — WARNING (DoD ≥500; floor 400 защищает от
        # broken CTAS, реальный live ≈525 после фильтра).
        CHECK.row_count(
            'silver.capology_player_salaries',
            min_rows=400, severity='WARNING',
        ),

        # canonical_id coverage — WARNING (parity с TM policy).
        # warn_threshold=0.88 → выше 88% PASS
        # error_threshold=0.80 → 80-88% WARNING, <80% ERROR
        # DoD ≤7% orphan (≥93% coverage) — aspirational; live = 89.5%.
        # Followup issue для resolver-tune.
        CHECK.coverage(
            'silver.capology_player_salaries',
            column='canonical_id',
            warn_threshold=0.88,
            error_threshold=0.80,
            severity='WARNING',
            name='canonical_coverage[silver.capology_player_salaries]',
        ),

        # Ref integrity to xref_player — WARNING (orphan rows expected).
        CHECK.ref_integrity(
            'silver.capology_player_salaries',
            'silver.xref_player',
            'canonical_id',
            severity='WARNING',
        ),

        # Freshness — WARNING (weekly ingest, 48h grace).
        CHECK.freshness(
            'silver.capology_player_salaries',
            ts_col='_bronze_ingested_at',
            max_age_hours=FRESH_HOURS,
            severity='WARNING',
        ),

        # Value ranges — WARNING (outlier observability).
        # weekly_gross 1..1_000_000 GBP per DoD.
        CHECK.value_range(
            'silver.capology_player_salaries', 'weekly_gross_gbp',
            min_val=1, max_val=1000000, severity='WARNING',
        ),
        CHECK.value_range(
            'silver.capology_player_salaries', 'age',
            min_val=14, max_val=50, severity='WARNING',
        ),
    ]

    # Run with raise_on_error=False so the Telegram summary always lands.
    report = run_checks(checks, raise_on_error=False)
    logger.info(f"Silver DQ: {report.summary()}")

    telegram_dq_summary(report, header="Capology Silver DQ")

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
    dag_id='dag_transform_capology_silver',
    default_args=SILVER_ARGS,
    description='Transform Bronze Capology data into Silver Iceberg tables via Trino CTAS',
    schedule=None,  # Trigger-only (called after dag_ingest_capology)
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['transform', 'capology', 'silver', 'football', 'trino'],
    max_active_runs=1,
    max_active_tasks=1,  # Sequential CTAS to prevent OOM (~1.2GB per task)
    doc_md="""
    ## Capology Silver Transformation

    Transforms Bronze Capology salary snapshots into typed Silver tables with
    `canonical_id` bridging via `silver.xref_player`.

    ### Trigger

    Triggered by `dag_ingest_capology` via TriggerDagRunOperator.

    ### Silver Tables

    | Table | Description | Bronze sources |
    |-------|-------------|----------------|
    | `capology_player_salaries` | Typed salary snapshot + canonical_id (issue #63) | `capology_player_salaries` |

    ### Filters applied in CTE

    - `currency = 'GBP'` — MVP single-currency.
    - `active = true OR loan = true` — exclude ~28% inactive Bronze rows
      (release/academy/youth) without FBref counterpart.

    ### Data Quality Checks

    - **PK NULLs / uniqueness** (ERROR): blocks DAG to protect downstream Gold.
    - **canonical-grain dedup** (ERROR, WHERE canonical_id IS NOT NULL).
    - **canonical_id coverage** (WARNING/ERROR by ratio): ≥88% non-orphan PASS,
      80–88% WARNING, <80% ERROR. Live APL 2025/26 = 89.5%.
      DoD ≤7% orphan (≥93%) — aspirational; tracked via resolver-tune followup.
    - **ref_integrity to xref_player** (WARNING): orphan Capology rows expected.
    - **Freshness** (WARNING): 48h grace post-Monday ingest.
    - **Value ranges** (WARNING): weekly_gross 1–1_000_000, age 14–50.
    """,
) as dag:

    # =========================================================================
    # TaskGroup: Silver Transforms
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
