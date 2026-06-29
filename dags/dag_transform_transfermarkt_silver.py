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
        ├── players              — typed player snapshot with canonical_id
        ├── market_value_history — typed MV timeline with canonical_id
        └── transfers            — typed transfer events + player/club canonical_ids
        |
        v
    validate_silver  — row count checks
        |
        v
    validate_silver_quality  — DQ checks (PK, ref integrity WARNING, ranges, orphan coverage)

Silver Tables Created:
    iceberg.silver.transfermarkt_players               — typed snapshot + canonical_id (issue #60)
    iceberg.silver.transfermarkt_market_value_history  — typed MV timeline + canonical_id (issue #61)
    iceberg.silver.transfermarkt_transfers             — typed transfer events + canonical_ids (issue #62)
"""

import logging
import os
import tempfile
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
# When sql_file ends with `.sql.j2`, _run_transform renders it via
# medallion_config.render_sql_template() before passing to run_silver_transform
# (same pattern as dag_transform_xref._run_xref_team).
SILVER_TRANSFORMS = [
    (
        'players',
        'dags/sql/silver/transfermarkt_players.sql',
        'transfermarkt_players',
    ),
    (
        'market_value_history',
        'dags/sql/silver/transfermarkt_market_value_history.sql',
        'transfermarkt_market_value_history',
    ),
    (
        'transfers',
        'dags/sql/silver/transfermarkt_transfers.sql.j2',
        'transfermarkt_transfers',
    ),
    # issue #434/#619: head coaches (dob/nationality) for gold.dim_manager
    # enrichment. `.sql.j2` — embeds manager_aliases VALUES (issue #619).
    (
        'coaches',
        'dags/sql/silver/transfermarkt_coaches.sql.j2',
        'transfermarkt_coaches',
    ),
]

# Expected minimum row counts per Silver table (for validation)
# transfermarkt_players: APL 2025/26 Bronze = 528 rows; DoD floor = 400.
# transfermarkt_market_value_history: ~21 точка на игрока × 528 ≈ 10 888
#   rows full-state APL 2025/26; live floor = 1000 защищает от broken CTAS,
#   DoD-инвариант ≥5000 проверяется отдельным DQ check (issue #61).
# transfermarkt_transfers: live = 750 rows / 100 игроков — Bronze ограничен
#   TRANSFERS_DAILY_LIMIT=100 + replace_partitions wipe (#486); полный ростер
#   ≈4 116 rows (DoD #62) недостижим до фикса #486. Floor = 600 защищает от
#   broken CTAS / коллапса скрейпа; revisit after #486 (#493).
SILVER_MIN_ROWS = {
    'transfermarkt_players': 400,
    'transfermarkt_market_value_history': 1000,
    'transfermarkt_transfers': 600,
    # transfermarkt_coaches: ~20 head coaches per APL season (1 per club).
    # Floor 15 protects against a broken CTAS / scrape collapse.
    'transfermarkt_coaches': 15,
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _run_transform(sql_file: str, table_name: str, **context) -> Dict[str, Any]:
    """PythonOperator callable — run a single Silver CTAS transform.

    If sql_file ends with `.sql.j2`, render it through
    ``medallion_config.render_sql_template`` (embedding the team-alias
    VALUES via ``{{ team_aliases_values_sql }}``) and pass the rendered
    SQL to ``run_silver_transform`` via a tempfile. This mirrors the
    pattern used in ``dag_transform_xref._run_xref_team``.
    """
    from pathlib import Path

    from utils.silver_tasks import run_silver_transform

    if not sql_file.endswith('.sql.j2'):
        return run_silver_transform(
            sql_file=sql_file,
            table_name=table_name,
            schema='silver',
        )

    from utils.medallion_config import (
        get_manager_alias_sql_values,
        get_team_alias_sql_values,
        render_sql_template,
    )

    template_path = Path('/opt/airflow') / sql_file
    if not template_path.exists():
        raise FileNotFoundError(f"Silver template not found: {template_path}")

    # Each template references exactly one alias placeholder. transfermarkt_
    # coaches → manager aliases (issue #619); the others → team aliases. Pass
    # only the kwarg the template needs (render_sql_template ignores extras but
    # would fail on a referenced-yet-missing key).
    if 'transfermarkt_coaches' in sql_file:
        render_kwargs = {
            'manager_aliases_values_sql': get_manager_alias_sql_values(
                source='transfermarkt',
            ),
        }
    else:
        render_kwargs = {'team_aliases_values_sql': get_team_alias_sql_values()}

    rendered_sql = render_sql_template(template_path, **render_kwargs)
    logger.info(
        "Rendered %s — %d chars (%d alias pairs embedded)",
        template_path.name,
        len(rendered_sql),
        rendered_sql.count("),\n"),
    )

    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix=f'_{table_name}.sql',
        delete=False,
        encoding='utf-8',
    ) as tmp:
        tmp.write(rendered_sql)
        tmp_path = tmp.name

    try:
        return run_silver_transform(
            sql_file=tmp_path,
            table_name=table_name,
            schema='silver',
        )
    finally:
        try:
            os.unlink(tmp_path)
        except OSError as e:
            logger.warning("Failed to cleanup temp file %s: %s", tmp_path, e)


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
            # #788: меряем покрытие только за последний (текущий) сезон — это
            # health-сигнал на толстом current-season FBref-spine. Canonical теперь
            # историзирован за все сезоны (#788), но за старые сезоны покрытие
            # структурно низкое (тонкий spine) и красило бы ERROR не по вине
            # резолва — исторический градиент сглаживается в #825.
            where="season = (SELECT max(season) FROM iceberg.silver.transfermarkt_players)",
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

        # ---------------------------------------------------------------
        # silver.transfermarkt_market_value_history (issue #61)
        # ---------------------------------------------------------------
        # Row_count floor — WARNING. Live = 2 121 rows: Bronze ограничен
        # MV_HISTORY_DAILY_LIMIT=100 + replace_partitions wipe (#486), а не
        # «догоняет постепенно»; DoD ~10 888 rows недостижим до фикса #486.
        # Floor 1500 ловит коллапс; revisit after #486 (#493).
        CHECK.row_count(
            'silver.transfermarkt_market_value_history',
            min_rows=1500, severity='WARNING',
        ),

        # PK + critical NULLs — ERROR. canonical_id EXCLUDED (TM orphan
        # rate ≈ 10% — see coverage check below).
        CHECK.no_nulls(
            'silver.transfermarkt_market_value_history',
            cols=['player_id', 'mv_date', 'value_eur', 'league', 'season'],
        ),

        # Bronze-grain dedup — ERROR (defensive against replace_partitions
        # double-write).
        CHECK.no_duplicates(
            'silver.transfermarkt_market_value_history',
            pk=['player_id', 'mv_date', 'league', 'season'],
        ),

        # DoD canonical-grain dedup — ERROR. WHERE filter обязателен:
        # canonical_id NULL для orphans, иначе чек упадёт на множественных
        # NULL-ах. См. feedback_xref_player_tm_capology.md.
        CHECK.no_duplicates(
            'silver.transfermarkt_market_value_history',
            pk=['canonical_id', 'mv_date'],
            where='canonical_id IS NOT NULL',
        ),

        # canonical_id coverage — sibling-policy (как players).
        CHECK.coverage(
            'silver.transfermarkt_market_value_history',
            column='canonical_id',
            warn_threshold=0.88,
            error_threshold=0.80,
            severity='WARNING',
            # #788: market_value_history остаётся scoped на текущий сезон (в
            # отличие от players/transfers) — Bronze повторяет полную MV-историю
            # в каждом сезонном snapshot (×3.18 дубли), поэтому canonical
            # историзируется только для per-season таблиц. Правильная
            # историзация MV (дедуп по player_id+mv_date) = followup.
            where="season = (SELECT max(season) FROM iceberg.silver.transfermarkt_market_value_history)",
            name='canonical_coverage[silver.transfermarkt_market_value_history]',
        ),

        # Ref integrity to xref_player — WARNING.
        CHECK.ref_integrity(
            'silver.transfermarkt_market_value_history',
            'silver.xref_player',
            'canonical_id',
            severity='WARNING',
        ),

        # Freshness — WARNING. DoD: ≤14 дней (336h).
        CHECK.freshness(
            'silver.transfermarkt_market_value_history',
            ts_col='_bronze_ingested_at',
            max_age_hours=336,
            severity='WARNING',
        ),

        # Value ranges — WARNING.
        CHECK.value_range(
            'silver.transfermarkt_market_value_history', 'value_eur',
            min_val=0, severity='WARNING',
        ),
        CHECK.value_range(
            'silver.transfermarkt_market_value_history', 'age',
            min_val=14, max_val=50, severity='WARNING',
        ),

        # ---------------------------------------------------------------
        # silver.transfermarkt_transfers (issue #62)
        # ---------------------------------------------------------------
        # Row_count floor — WARNING. Live = 750 rows (100 игроков, cap #486);
        # full APL ≈ 4 116 rows недостижим до фикса #486. Floor 600 ловит
        # broken CTAS / scrape collapse; revisit after #486 (#493).
        CHECK.row_count(
            'silver.transfermarkt_transfers',
            min_rows=600, severity='WARNING',
        ),

        # PK + critical NULLs — ERROR. canonical_id EXCLUDED (TM orphan
        # rate ≈10% по xref_player_resolver).
        CHECK.no_nulls(
            'silver.transfermarkt_transfers',
            cols=['player_id', 'transfer_date', 'league', 'season'],
        ),

        # PK uniqueness — ERROR. Natural key is per-event:
        # (player_id, transfer_date, from_club_name, to_club_name). Per
        # DoD spec — orphan rows (canonical_id NULL) don't break this
        # since PK doesn't include canonical_id.
        CHECK.no_duplicates(
            'silver.transfermarkt_transfers',
            pk=['player_id', 'transfer_date', 'from_club_name', 'to_club_name'],
        ),

        # canonical_id coverage (player) — sibling-policy (players/mv:
        # 0.88/0.80). Per-event orphan rate структурно выше per-player
        # (~10-15%): orphan'ы (youth/loan/backup GK) имеют непропорционально
        # много transfer-событий. Live 2026-06-12: 614/750 = 81.9%
        # (15 структурных orphan-игроков из 100) → WARNING. DoD #62 (≥95%)
        # на event-grain недостижим; revisit after #486 full-roster (#493).
        CHECK.coverage(
            'silver.transfermarkt_transfers',
            column='canonical_id',
            warn_threshold=0.88,
            error_threshold=0.80,
            severity='WARNING',
            # #788: health-сигнал на current-season spine. Canonical историзирован
            # за все сезоны, исторический градиент покрытия сглаживается в #825.
            where="season = (SELECT max(season) FROM iceberg.silver.transfermarkt_transfers)",
            name='canonical_coverage[silver.transfermarkt_transfers]',
        ),

        # Ref integrity to xref_player — WARNING.
        CHECK.ref_integrity(
            'silver.transfermarkt_transfers',
            'silver.xref_player',
            'canonical_id',
            severity='WARNING',
        ),

        # Team canonical coverage — WARNING-only (per DoD: TM transfers
        # contain non-APL клубы которые xref_team / team_aliases.yaml
        # не покрывают; high orphan tolerated for observability).
        CHECK.coverage(
            'silver.transfermarkt_transfers',
            column='from_club_id_canonical',
            warn_threshold=0.0,
            error_threshold=0.0,
            severity='WARNING',
            name='canonical_coverage[silver.transfermarkt_transfers.from_club_id_canonical]',
        ),
        CHECK.coverage(
            'silver.transfermarkt_transfers',
            column='to_club_id_canonical',
            warn_threshold=0.0,
            error_threshold=0.0,
            severity='WARNING',
            name='canonical_coverage[silver.transfermarkt_transfers.to_club_id_canonical]',
        ),

        # Freshness — WARNING (weekly ingest, 48h grace).
        CHECK.freshness(
            'silver.transfermarkt_transfers',
            ts_col='_bronze_ingested_at',
            max_age_hours=FRESH_HOURS,
            severity='WARNING',
        ),

        # Value ranges — WARNING. fee_eur может быть NULL (free transfer);
        # value_range игнорирует NULL по definition.
        CHECK.value_range(
            'silver.transfermarkt_transfers', 'fee_eur',
            min_val=0, severity='WARNING',
        ),
        CHECK.value_range(
            'silver.transfermarkt_transfers', 'market_value_eur',
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
    | `transfermarkt_market_value_history` | Typed MV timeline + canonical_id (issue #61) | `transfermarkt_market_value_history` |
    | `transfermarkt_transfers` | Typed transfer events + player/club canonical_ids (issue #62) | `transfermarkt_transfers` |

    ### Data Quality Checks

    - **PK NULLs / uniqueness** (ERROR): blocks DAG to protect downstream Gold
    - **canonical_id coverage** (WARNING/ERROR by ratio): ≥88% non-orphan PASS,
      80–88% WARNING, <80% ERROR — единая политика для всех трёх таблиц (#493).
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
