"""
SoFIFA Silver Layer Transformation DAG
======================================

Transforms Bronze SoFIFA data into the Silver layer (issue #42).

Runs after Bronze ingest + dag_transform_xref (canonical_id bridge). Uses CTAS
via Trino to join bronze.sofifa_player_ratings (attributes) with
bronze.sofifa_players (identity), map FIFA/FC edition → football-season slug,
and LEFT JOIN silver.xref_player (source='sofifa', non-orphan) for canonical_id.

Architecture:
    Triggered by dag_master_pipeline (after E1 xref) via TriggerDagRunOperator
        |
        v
    player_profile          — typed per-edition snapshot + canonical_id
        |
        v
    validate_silver         — row count
        |
        v
    validate_silver_quality — DQ (PK, ranges 0-99, canonical coverage WARNING)

Silver Tables Created:
    iceberg.silver.sofifa_player_profile  — game-side FIFA ratings + canonical_id
"""

import logging
from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.default_args import SILVER_ARGS

logger = logging.getLogger(__name__)

# (task_id, sql_file relative to /opt/airflow/, target table name)
SILVER_TRANSFORMS = [
    (
        'player_profile',
        'dags/sql/silver/sofifa_player_profile.sql',
        'sofifa_player_profile',
    ),
]

# APL 2025/26 (FC 26) Bronze ≈ 545 players; floor protects against broken CTAS.
SILVER_MIN_ROWS = {
    'sofifa_player_profile': 100,
}

# issue #180: bronze.sofifa_player_ratings frozen (Cloudflare Turnstile). When
# absent, build an empty-but-typed sofifa_player_profile so Silver and Gold
# (dim_player_attributes LEFT JOIN) stay intact; auto-resumes when Bronze
# returns. Mirrors the Gold fallback pattern (gold_tasks.run_gold_transform).
SOFIFA_REQUIRE_BRONZE = 'sofifa_player_ratings'
SOFIFA_FALLBACK_SQL = 'dags/sql/silver/sofifa_player_profile_empty.sql'


def _run_transform(sql_file: str, table_name: str, **context) -> Dict[str, Any]:
    """PythonOperator callable — run a single Silver CTAS transform.

    issue #180: if bronze.sofifa_player_ratings is missing, build the empty
    typed fallback instead of failing the DROP + CTAS. Returns fallback=True so
    _validate_silver relaxes the row-count floor for this run.
    """
    from utils.silver_tasks import check_bronze_table_exists, run_silver_transform

    if not check_bronze_table_exists(table_name=SOFIFA_REQUIRE_BRONZE, schema='bronze'):
        logger.warning(
            "bronze.%s not found — building empty fallback '%s' for silver.%s "
            "(issue #180). Will auto-resume real build when Bronze returns.",
            SOFIFA_REQUIRE_BRONZE, SOFIFA_FALLBACK_SQL, table_name,
        )
        result = run_silver_transform(
            sql_file=SOFIFA_FALLBACK_SQL,
            table_name=table_name,
            schema='silver',
        )
        result['fallback'] = True
        result['fallback_reason'] = f"missing bronze table: {SOFIFA_REQUIRE_BRONZE}"
        return result

    return run_silver_transform(
        sql_file=sql_file,
        table_name=table_name,
        schema='silver',
    )


def _validate_silver(**context) -> Dict[str, Any]:
    """PythonOperator callable — validate row counts in Silver tables.

    issue #180: when player_profile fell back to the empty SQL (Bronze frozen),
    relax the row-count floor so a legitimately-empty table is not flagged.
    """
    from airflow.exceptions import AirflowException
    from utils.silver_tasks import validate_silver_tables

    ti = context['ti']
    transform_result = ti.xcom_pull(task_ids='player_profile') or {}
    fellback = bool(transform_result.get('fallback'))

    tables = dict(SILVER_MIN_ROWS)
    min_rows = 1
    if fellback:
        tables['sofifa_player_profile'] = 0   # 0 => use min_rows as floor
        min_rows = 0                            # 0-row empty table is OK
        logger.warning(
            "sofifa_player_profile built via empty fallback — row-count floor "
            "relaxed to 0 (issue #180)."
        )

    validation = validate_silver_tables(tables=tables, min_rows=min_rows)
    logger.info(f"Silver validation: {validation['status']} — {validation['details']}")

    if validation['warnings']:
        for w in validation['warnings']:
            logger.warning(f"  {w}")
        raise AirflowException(
            f"Silver validation FAILED: {validation['warnings']}"
        )
    return validation


def _validate_silver_quality(**context) -> Dict[str, Any]:
    """PythonOperator callable — DQ checks on silver.sofifa_player_profile.

    ERROR: PK NULLs / uniqueness. WARNING: canonical coverage (SoFIFA carries
    ~15% structural orphans — loan-out / U21), ref_integrity, freshness, and
    rating value-ranges (FIFA ratings are bounded 0-99).
    """
    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    # SoFIFA ingest is weekly (Sunday 06:00 UTC); 48h grace post-Sunday.
    FRESH_HOURS = 48

    rating_cols = (
        'overall', 'potential', 'pace', 'shooting', 'passing', 'dribbling',
        'defending', 'physical', 'gk_diving', 'gk_handling', 'gk_kicking',
        'gk_positioning', 'gk_reflexes',
    )

    checks = [
        # PK + critical NULLs — ERROR. canonical_id EXCLUDED (~15% orphan).
        CHECK.no_nulls(
            'silver.sofifa_player_profile',
            cols=['player_id', 'league', 'season'],
        ),
        # PK uniqueness — ERROR. PK = (player_id, league, season).
        CHECK.no_duplicates(
            'silver.sofifa_player_profile',
            pk=['player_id', 'league', 'season'],
        ),
        # canonical_id coverage — WARNING by ratio. Resolver dry-run (issue #42)
        # measured ~84% non-orphan APL 2025/26.
        CHECK.coverage(
            'silver.sofifa_player_profile',
            column='canonical_id',
            warn_threshold=0.80,
            error_threshold=0.50,
            severity='WARNING',
            name='canonical_coverage[silver.sofifa_player_profile]',
        ),
        # Ref integrity to xref_player — WARNING (orphans expected).
        CHECK.ref_integrity(
            'silver.sofifa_player_profile',
            'silver.xref_player',
            'canonical_id',
            severity='WARNING',
        ),
        # Freshness — WARNING (weekly ingest, 48h grace).
        CHECK.freshness(
            'silver.sofifa_player_profile',
            ts_col='_bronze_ingested_at',
            max_age_hours=FRESH_HOURS,
            severity='WARNING',
        ),
        # FIFA ratings are bounded 0-99 — ERROR on out-of-range.
        *[
            CHECK.value_range(
                'silver.sofifa_player_profile', col,
                min_val=0, max_val=99, severity='ERROR',
            )
            for col in rating_cols
        ],
        # Game-side financials — WARNING (outlier observability).
        CHECK.value_range(
            'silver.sofifa_player_profile', 'value_eur',
            min_val=0, severity='WARNING',
        ),
        CHECK.value_range(
            'silver.sofifa_player_profile', 'height_cm',
            min_val=150, max_val=220, severity='WARNING',
        ),
    ]

    report = run_checks(checks, raise_on_error=False)
    logger.info(f"Silver DQ: {report.summary()}")
    telegram_dq_summary(report, header="SoFIFA Silver DQ")

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


with DAG(
    dag_id='dag_transform_sofifa_silver',
    default_args=SILVER_ARGS,
    description='Transform Bronze SoFIFA data into Silver Iceberg tables via Trino CTAS (issue #42)',
    schedule=None,  # Trigger-only (called by dag_master_pipeline after E1 xref)
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['transform', 'sofifa', 'silver', 'football', 'trino'],
    max_active_runs=1,
    max_active_tasks=1,
    doc_md="""
    ## SoFIFA Silver Transformation (issue #42)

    Builds `iceberg.silver.sofifa_player_profile` — game-side EA Sports FC
    ratings (overall/potential, main-6 card aggregates, GK skills, value/wage/
    contract, profile) with `canonical_id` bridging via `silver.xref_player`
    (source='sofifa').

    ### Trigger

    Triggered by `dag_master_pipeline` after `dag_transform_xref` (needs fresh
    `silver.xref_player` sofifa rows for canonical_id).

    ### Data Quality

    - **PK NULLs / uniqueness** (ERROR): protects downstream Gold.
    - **canonical coverage** (WARNING): ~84% non-orphan APL 2025/26.
    - **rating value_range 0-99** (ERROR): FIFA ratings are bounded.
    - **ref_integrity / freshness / financials** (WARNING).
    """,
) as dag:

    transform_task = PythonOperator(
        task_id='player_profile',
        python_callable=_run_transform,
        op_kwargs={
            'sql_file': SILVER_TRANSFORMS[0][1],
            'table_name': SILVER_TRANSFORMS[0][2],
        },
    )

    validate_silver = PythonOperator(
        task_id='validate_silver',
        python_callable=_validate_silver,
        trigger_rule='all_done',
    )

    validate_quality = PythonOperator(
        task_id='validate_silver_quality',
        python_callable=_validate_silver_quality,
        trigger_rule='all_done',
    )

    transform_task >> validate_silver >> validate_quality
