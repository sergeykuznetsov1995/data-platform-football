"""
FotMob Silver Layer Transformation DAG
=======================================

Transforms Bronze FotMob data into Silver layer Iceberg tables.

Runs after dag_ingest_fotmob completes (trigger-only, no schedule):
dag_ingest_fotmob триггерит его через TriggerDagRunOperator в конце ingest.

Architecture:
    Triggered by TriggerDagRunOperator from dag_ingest_fotmob
        |
        v
    TaskGroup: silver_transforms (2 tasks SEQUENTIAL, max_active_tasks=1)
        ├── player_season_profile  — полевые игроки per-season
        │                            (PIVOT stats long -> wide, без GK-stats,
        │                             фильтр NOT is_coach AND pos != 'keeper')
        └── keeper_profile         — вратари per-season с GK-stats
                                     (clean_sheets, save%, saves_per_90, ...)
        |
        v
    validate_silver         — row count check
        |
        v
    validate_silver_quality — DQ checks (no_nulls, no_duplicates,
                              row_count, freshness, value_range)

Silver Tables Created:
    iceberg.silver.fotmob_player_season_profile
    iceberg.silver.fotmob_player_profile
    iceberg.silver.fotmob_keeper_profile
    iceberg.silver.fotmob_player_market_value_history
    iceberg.silver.fotmob_team_match
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
SILVER_TRANSFORMS = [
    (
        'player_season_profile',
        'dags/sql/silver/fotmob_player_season_profile.sql',
        'fotmob_player_season_profile',
    ),
    (
        'player_profile',
        'dags/sql/silver/fotmob_player_profile.sql',
        'fotmob_player_profile',
    ),
    (
        'keeper_profile',
        'dags/sql/silver/fotmob_keeper_profile.sql',
        'fotmob_keeper_profile',
    ),
    # issue #434: тренеры (WHERE is_coach) — nationality/dob для gold.dim_manager.
    # Зеркало player_profile; coachId (player_id) совпадает с xref_manager.source_id.
    (
        'manager_profile',
        'dags/sql/silver/fotmob_manager_profile.sql',
        'fotmob_manager_profile',
    ),
    # issue #11: timeline market_value из bronze.fotmob_player_details
    # .market_values_json (UNNEST). Питает gold.fct_player_market_value.
    (
        'player_market_value_history',
        'dags/sql/silver/fotmob_player_market_value_history.sql',
        'fotmob_player_market_value_history',
    ),
    # issue #97 Phase A: team-match stats из bronze.fotmob_match_details
    # (stats_json + player_stats_json for xA). Питает gold.fct_team_match
    # (5-source) и audit-таблицу — Phase B после gates DQ.
    (
        'team_match',
        'dags/sql/silver/fotmob_team_match.sql',
        'fotmob_team_match',
    ),
    # issue #290: судья матча + СТРАНА (FotMob-only) из match_facts_json.
    # Future namesake-дизамбигуатор для xref_referee при мульти-лиговом scope.
    (
        'match_referee',
        'dags/sql/silver/fotmob_match_referee.sql',
        'fotmob_match_referee',
    ),
]

# FotMob Bronze coverage: только сезон 2025 (572 player в details, ~10K rows
# в stats long-table). Без тренеров (-20) outfield+keeper = 552 row;
# outfield = ~487 row, GK = ~65 row.
#
# market_value_history: ~500 игроков × несколько точек FotMob timeline (полная
# история каждого player); APL 2025/26 floor ≥1000.
SILVER_MIN_ROWS = {
    'fotmob_player_season_profile': 450,
    'fotmob_player_profile': 500,
    'fotmob_keeper_profile': 40,
    # manager_profile: ~18 coaches APL 2025/26 (1 head coach per club). Floor 15.
    'fotmob_manager_profile': 15,
    'fotmob_player_market_value_history': 1000,
    # team_match: ~338 finished matches × 2 sides = 676 rows для APL 2025/26
    # (7% бронзы без stats_json — cancelled / not finished). Floor 600 c headroom.
    'fotmob_team_match': 600,
    # match_referee: ~380 матчей APL 2025/26 со 100% покрытием судьи (issue #290).
    # Floor 300 c headroom под матчи без referee.text.
    'fotmob_match_referee': 300,
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _run_transform(sql_file: str, table_name: str, **context) -> Dict[str, Any]:
    """Run a single Silver CTAS transform (imports lazy to keep parse-time light)."""
    from utils.silver_tasks import run_silver_transform

    return run_silver_transform(
        sql_file=sql_file,
        table_name=table_name,
        schema='silver',
    )


def _validate_silver(**context) -> Dict[str, Any]:
    """Row-count gate: проваливаем DAG если silver-таблица меньше threshold."""
    import logging

    from airflow.exceptions import AirflowException
    from utils.silver_tasks import validate_silver_tables

    logger = logging.getLogger(__name__)

    validation = validate_silver_tables(
        tables=SILVER_MIN_ROWS,
        min_rows=1,
    )

    logger.info(f"Silver validation result: {validation['status']}")
    logger.info(f"Table details: {validation['details']}")

    if validation['warnings']:
        for w in validation['warnings']:
            logger.warning(f"  {w}")
        raise AirflowException(
            f"FotMob Silver validation FAILED: {validation['warnings']}"
        )

    return validation


def _validate_silver_quality(**context) -> Dict[str, Any]:
    """DQ checks: PK uniqueness, no_nulls на PK, row_count, freshness, value_range."""
    import logging

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    logger = logging.getLogger(__name__)

    FRESH_HOURS = 48

    checks = [
        # --- player_season_profile (полевые игроки) ---
        # ERROR severity — блокируют downstream
        CHECK.no_nulls(
            'silver.fotmob_player_season_profile',
            cols=['player_id', 'league', 'season'],
        ),
        CHECK.no_duplicates(
            'silver.fotmob_player_season_profile',
            pk=['player_id', 'league', 'season'],
        ),
        CHECK.row_count(
            'silver.fotmob_player_season_profile',
            min_rows=450,
        ),
        # issue #177: ловим регресс «все статы NULL» (stale Silver / битый
        # Bronze join). value_range пропускает NULL, поэтому нужен явный floor
        # по non-null minutes_played — самой населённой стате: если она NULL
        # везде, мёртв весь FotMob-блок в gold.fct_player_season_stats.
        CHECK.row_count(
            'silver.fotmob_player_season_profile',
            min_rows=400,
            where='minutes_played IS NOT NULL',
        ),
        # WARNING severity — мониторинг, не блокируют
        CHECK.freshness(
            'silver.fotmob_player_season_profile',
            ts_col='_bronze_ingested_at',
            max_age_hours=FRESH_HOURS,
            severity='WARNING',
        ),
        CHECK.value_range(
            'silver.fotmob_player_season_profile',
            'minutes_played',
            min_val=0,
            max_val=5000,
            severity='WARNING',
        ),
        CHECK.value_range(
            'silver.fotmob_player_season_profile',
            'goals',
            min_val=0,
            severity='WARNING',
        ),
        CHECK.value_range(
            'silver.fotmob_player_season_profile',
            'fotmob_rating',
            min_val=0,
            max_val=10,
            severity='WARNING',
        ),

        # --- player_profile (time-invariant атрибуты: рост/dob/нация/foot) ---
        CHECK.no_nulls(
            'silver.fotmob_player_profile',
            cols=['player_id', 'league', 'season'],
        ),
        CHECK.no_duplicates(
            'silver.fotmob_player_profile',
            pk=['player_id', 'league', 'season'],
        ),
        CHECK.row_count(
            'silver.fotmob_player_profile',
            min_rows=500,
        ),
        CHECK.freshness(
            'silver.fotmob_player_profile',
            ts_col='_bronze_ingested_at',
            max_age_hours=FRESH_HOURS,
            severity='WARNING',
        ),
        CHECK.value_range(
            'silver.fotmob_player_profile',
            'height_cm',
            min_val=140,
            max_val=220,
            severity='WARNING',
        ),
        CHECK.coverage(
            'silver.fotmob_player_profile',
            column='height_cm',
            warn_threshold=0.80,
            error_threshold=0.50,
        ),
        CHECK.coverage(
            'silver.fotmob_player_profile',
            column='foot',
            warn_threshold=0.90,
            error_threshold=0.60,
        ),

        # --- keeper_profile (вратари) ---
        CHECK.no_nulls(
            'silver.fotmob_keeper_profile',
            cols=['player_id', 'league', 'season'],
        ),
        CHECK.no_duplicates(
            'silver.fotmob_keeper_profile',
            pk=['player_id', 'league', 'season'],
        ),
        CHECK.row_count(
            'silver.fotmob_keeper_profile',
            min_rows=40,
        ),
        CHECK.freshness(
            'silver.fotmob_keeper_profile',
            ts_col='_bronze_ingested_at',
            max_age_hours=FRESH_HOURS,
            severity='WARNING',
        ),
        CHECK.value_range(
            'silver.fotmob_keeper_profile',
            'save_percentage',
            min_val=0,
            max_val=100,
            severity='WARNING',
        ),
        CHECK.value_range(
            'silver.fotmob_keeper_profile',
            'minutes_played',
            min_val=0,
            max_val=5000,
            severity='WARNING',
        ),
        CHECK.value_range(
            'silver.fotmob_keeper_profile',
            'fotmob_rating',
            min_val=0,
            max_val=10,
            severity='WARNING',
        ),

        # --- player_market_value_history (timeline MV из market_values_json) ---
        # issue #11: один row per (player_id, value_date, league, season).
        # ERROR: no-NULL на PK, no-duplicates на PK, row_count floor.
        # WARNING: value_range plausibility, freshness.
        CHECK.no_nulls(
            'silver.fotmob_player_market_value_history',
            cols=['player_id', 'value_date', 'league', 'season'],
        ),
        CHECK.no_duplicates(
            'silver.fotmob_player_market_value_history',
            pk=['player_id', 'value_date', 'league', 'season'],
        ),
        CHECK.row_count(
            'silver.fotmob_player_market_value_history',
            min_rows=1000,
        ),
        CHECK.freshness(
            'silver.fotmob_player_market_value_history',
            ts_col='_bronze_ingested_at',
            max_age_hours=FRESH_HOURS,
            severity='WARNING',
        ),
        CHECK.value_range(
            'silver.fotmob_player_market_value_history',
            'market_value_eur',
            min_val=0,
            max_val=500_000_000,
            severity='WARNING',
        ),

        # --- team_match (issue #97 Phase A) ---
        # ERROR: PK uniqueness + floor count.
        # WARNING: freshness, plausibility ranges, coverage для критичных
        # metrics (xG/xA, possession). expected_assists — raison d'être #97,
        # держим coverage WARN высоко (вынуждаем reingest если падает).
        CHECK.no_nulls(
            'silver.fotmob_team_match',
            cols=['match_id', 'team_id', 'league', 'season'],
        ),
        CHECK.no_duplicates(
            'silver.fotmob_team_match',
            pk=['match_id', 'team_id', 'league', 'season'],
        ),
        CHECK.row_count(
            'silver.fotmob_team_match',
            min_rows=600,
        ),
        CHECK.freshness(
            'silver.fotmob_team_match',
            ts_col='_bronze_ingested_at',
            max_age_hours=FRESH_HOURS,
            severity='WARNING',
        ),
        CHECK.value_range(
            'silver.fotmob_team_match',
            'possession_pct',
            min_val=0,
            max_val=100,
            severity='WARNING',
        ),
        CHECK.value_range(
            'silver.fotmob_team_match',
            'expected_goals',
            min_val=0,
            max_val=10,
            severity='WARNING',
        ),
        CHECK.value_range(
            'silver.fotmob_team_match',
            'expected_assists',
            min_val=0,
            max_val=10,
            severity='WARNING',
        ),
        CHECK.coverage(
            'silver.fotmob_team_match',
            column='expected_goals',
            warn_threshold=0.95,
            error_threshold=0.80,
        ),
        CHECK.coverage(
            'silver.fotmob_team_match',
            column='expected_assists',
            warn_threshold=0.95,
            error_threshold=0.80,
        ),

        # --- match_referee (issue #290) ---
        # ERROR: PK uniqueness на (match_id, league, season) + floor count.
        # WARNING: freshness + coverage(referee_country) — полнота страны судьи
        # это raison d'être задачи (referee_name не-NULL гарантирован SQL-фильтром).
        CHECK.no_nulls(
            'silver.fotmob_match_referee',
            cols=['match_id', 'league', 'season'],
        ),
        CHECK.no_duplicates(
            'silver.fotmob_match_referee',
            pk=['match_id', 'league', 'season'],
        ),
        CHECK.row_count(
            'silver.fotmob_match_referee',
            min_rows=300,
        ),
        CHECK.freshness(
            'silver.fotmob_match_referee',
            ts_col='_bronze_ingested_at',
            max_age_hours=FRESH_HOURS,
            severity='WARNING',
        ),
        CHECK.coverage(
            'silver.fotmob_match_referee',
            column='referee_country',
            warn_threshold=0.90,
            error_threshold=0.50,
        ),
    ]

    report = run_checks(checks, raise_on_error=False)
    logger.info(f"FotMob Silver DQ: {report.summary()}")

    telegram_dq_summary(report, header="FotMob Silver DQ")

    if report.errors:
        from airflow.exceptions import AirflowException
        raise AirflowException(
            f"FotMob Silver DQ failed: {len(report.errors)} error(s). "
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
    dag_id='dag_transform_fotmob_silver',
    default_args=SILVER_ARGS,
    description='Transform Bronze FotMob data into Silver Iceberg tables via Trino CTAS',
    schedule=None,  # Trigger-only
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['transform', 'fotmob', 'silver', 'football', 'trino'],
    max_active_runs=1,
    max_active_tasks=1,
    doc_md="""
    ## FotMob Silver Transformation

    Materialises Silver tables from raw Bronze FotMob data.

    ### Trigger
    Манульный или из master pipeline / ingest DAG.

    ### Silver Tables

    | Table | Description | Sources |
    |-------|-------------|---------|
    | `fotmob_player_season_profile` | Полевые игроки per-season (без GK, без тренеров) | fotmob_player_details + fotmob_player_stats (PIVOTED) |
    | `fotmob_player_profile` | Time-invariant snapshot: height_cm/dob/nationality/foot | fotmob_team_squad + fotmob_player_details (foot из JSON) |
    | `fotmob_keeper_profile` | Вратари per-season с GK-stats | fotmob_player_details + fotmob_player_stats (PIVOTED) |
    | `fotmob_team_match` | Per (match, team_id) team-level stats + xG/xA | fotmob_match_details.stats_json + .player_stats_json (SUM xA) — issue #97 Phase A |
    | `fotmob_match_referee` | Per-match судья + страна (FotMob-only) | fotmob_match_details.match_facts_json ($.infoBox.Referee) — issue #290 |

    ### Transformations
    - **Dedup** на (player_id, league, season) через ROW_NUMBER + ORDER BY _ingested_at DESC
    - **PIVOT** stats из long в wide (~37 stat-колонок: Top Stat / Attacking / Defending / Discipline / Goalkeeping)
    - Только **per-season атрибуты** в identity-блоке (player_name, primary_position, primary_team_*).
      Time-invariant поля (birth_date/height/foot/country) и pass-through JSON не хранятся
      здесь — уйдут в snapshot-таблицу `silver.fotmob_player_profile` (T4 backlog).

    ### DQ Gates
    - PK uniqueness (player_id, league, season) — ERROR
    - no_nulls на (player_id, league, season) — ERROR
    - row_count ≥ 500 — ERROR
    - freshness 48h — WARNING
    - value_range minutes_played / goals / fotmob_rating — WARNING

    ### Manual Trigger
    ```bash
    airflow dags trigger dag_transform_fotmob_silver
    ```
    """,
) as dag:

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

    transforms_group >> validate_silver >> validate_quality
