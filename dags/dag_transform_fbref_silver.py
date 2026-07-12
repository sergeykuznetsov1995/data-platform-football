"""
FBref Silver Layer Transformation DAG
======================================

Transforms Bronze FBref data into Silver layer Iceberg tables.

Runs from dag_master_pipeline after ingestion completes (trigger-only, no schedule).
Uses CTAS (CREATE TABLE AS SELECT) via Trino to deduplicate Bronze data,
join multiple tables, cast types, and produce clean analytical tables.

Architecture:
    Triggered by dag_master_pipeline (or manual trigger)
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
        |
        v
    trigger_xref  — blocking, fail-closed identity handoff

Gold publication is deliberately not triggered from this DAG.  The master
pipeline is the single owner of the final Gold run because it first publishes
fresh xref and E3 tables; a nested trigger here could race that ordered path.

Silver Tables Created:
    iceberg.silver.fbref_player_season_profile  — player stats + shooting + playingtime + misc
    iceberg.silver.fbref_keeper_profile         — goalkeeper stats + keeper + shooting + misc
    iceberg.silver.fbref_match_enriched         — schedule + team_stats + events + lineups
    iceberg.silver.fbref_player_match_stats     — per-match stats with TRY_CAST types
    iceberg.silver.fbref_match_events           — detailed match events (deduplicated)
    iceberg.silver.fbref_match_lineups          — detailed lineup entries (deduplicated)
    iceberg.silver.fbref_shot_events            — per-shot xG data (if Bronze exists)
"""

from datetime import datetime, timedelta
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
        'player_identity',
        'dags/sql/silver/fbref_player_identity.sql',
        'fbref_player_identity',
    ),
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
        'keeper_match_stats',
        'dags/sql/silver/fbref_keeper_match_stats.sql',
        'fbref_keeper_match_stats',
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
    # E5: WhoScored confirmed-absences feed (per-match player unavailability).
    # Reads manifest-filtered bronze.whoscored_missing_players_current plus
    # bronze.whoscored_schedule.
    # Optional Bronze — see OPTIONAL_BRONZE_TABLES below.
    (
        'whoscored_player_unavailable',
        'dags/sql/silver/whoscored_player_unavailable.sql',
        'whoscored_player_unavailable',
    ),
    # issue #613: FBref match officials (referee + ar1/ar2/4th/var), unpivoted
    # from wide bronze.fbref_match_officials. Optional Bronze (graceful skip).
    (
        'match_officials',
        'dags/sql/silver/fbref_match_officials.sql',
        'fbref_match_officials',
    ),
]

# Expected minimum row counts per Silver table (for validation)
SILVER_MIN_ROWS = {
    'fbref_player_identity': 100,
    'fbref_player_season_profile': 100,
    'fbref_keeper_profile': 10,
    'fbref_match_enriched': 50,
    'fbref_player_match_stats': 100,
    'fbref_keeper_match_stats': 20,
    'fbref_match_events': 50,
    'fbref_match_lineups': 100,
    'fbref_shot_events': 50,
    'fbref_team_season_profile': 10,
    # E5: ~20-50 confirmed absences/gameweek across APL → 100 rows is a
    # conservative floor for a season's worth. Threshold is intentionally
    # not strict — the table is in OPTIONAL_BRONZE_TABLES so it's excluded
    # from `validate_silver` row-count enforcement (see _validate_silver).
    'whoscored_player_unavailable': 100,
    # issue #613: ~5 officials/match × matches; conservative floor. Optional
    # Bronze → excluded from strict row-count enforcement (see _validate_silver).
    'fbref_match_officials': 50,
}

# Bronze tables that may not exist yet — skip transform with warning if absent.
# Silver-table-name -> Bronze-table-name. The Silver task graceful-skips when
# the listed Bronze table is absent in iceberg.bronze.
OPTIONAL_BRONZE_TABLES = {
    'fbref_shot_events': 'fbref_shot_events',
    # E5: WhoScored ingest is paused on some deployments (see
    # project_whoscored_cloudflare.md) — Silver must skip gracefully when the
    # Bronze source is absent rather than failing the whole DAG.
    'whoscored_player_unavailable': 'whoscored_missing_players_current',
    # issue #613: combined_match_data may not have populated officials yet on
    # some deployments — skip the Silver transform gracefully if absent.
    'fbref_match_officials': 'fbref_match_officials',
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
    import os
    import re
    import tempfile
    from pathlib import Path

    import yaml

    from utils.silver_tasks import check_bronze_table_exists, run_silver_transform

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

    rendered_path = None
    if table_name == 'fbref_match_enriched':
        sql_path = Path('/opt/airflow') / sql_file
        config_dir = Path(os.environ.get(
            'MEDALLION_CONFIG_DIR', '/opt/airflow/configs/medallion',
        ))
        config_path = config_dir / 'match_result_overrides.yaml'
        if not sql_path.exists():
            raise FileNotFoundError(f'FBref match SQL not found: {sql_path}')
        if not config_path.exists():
            raise FileNotFoundError(
                f'Official-result override registry not found: {config_path}'
            )

        payload = yaml.safe_load(config_path.read_text(encoding='utf-8')) or {}
        if payload.get('version') != 1:
            raise ValueError('match_result_overrides.yaml version must be 1')
        overrides = payload.get('overrides')
        if not isinstance(overrides, list) or not overrides:
            raise ValueError('match_result_overrides.yaml overrides must be non-empty')

        required = {
            'match_id', 'league', 'season', 'official_home_score',
            'official_away_score', 'authority', 'reference', 'reason',
        }
        seen = set()
        rows = []

        def quote(value: object) -> str:
            return "'" + str(value).replace("'", "''") + "'"

        for index, item in enumerate(overrides):
            if not isinstance(item, dict):
                raise ValueError(f'override #{index} must be a mapping')
            missing = sorted(k for k in required if item.get(k) in (None, ''))
            if missing:
                raise ValueError(
                    f'override #{index} is missing required evidence: {missing}'
                )
            match_id = str(item['match_id'])
            if not re.fullmatch(r'[a-f0-9]{8}', match_id):
                raise ValueError(f'override #{index} has invalid match_id {match_id!r}')
            if match_id in seen:
                raise ValueError(f'duplicate official-result override: {match_id}')
            seen.add(match_id)
            home = item['official_home_score']
            away = item['official_away_score']
            if (not isinstance(home, int) or isinstance(home, bool) or home < 0
                    or not isinstance(away, int) or isinstance(away, bool) or away < 0):
                raise ValueError(
                    f'override {match_id} official scores must be non-negative integers'
                )
            rows.append(
                '(' + ', '.join([
                    quote(match_id), quote(item['league']), quote(item['season']),
                    str(home), str(away), quote(item['authority']),
                    quote(item['reference']), quote(item['reason']),
                ]) + ')'
            )

        source_sql = sql_path.read_text(encoding='utf-8')
        typed_empty_row = (
            "(CAST(NULL AS varchar), CAST(NULL AS varchar), CAST(NULL AS varchar),\n"
            "         CAST(NULL AS integer), CAST(NULL AS integer), CAST(NULL AS varchar),\n"
            "         CAST(NULL AS varchar), CAST(NULL AS varchar))"
        )
        if source_sql.count(typed_empty_row) != 1:
            raise ValueError(
                'fbref_match_enriched override render anchor missing or duplicated'
            )
        rendered_sql = source_sql.replace(typed_empty_row, ',\n        '.join(rows))
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='_fbref_match_enriched.sql', delete=False,
            encoding='utf-8',
        ) as tmp:
            tmp.write(rendered_sql)
            rendered_path = tmp.name
        sql_file = rendered_path

    try:
        return run_silver_transform(
            sql_file=sql_file,
            table_name=table_name,
            schema='silver',
        )
    finally:
        if rendered_path:
            Path(rendered_path).unlink(missing_ok=True)


def _ensure_fbref_source_identity_columns() -> Dict[str, Any]:
    """Add nullable source-native identity columns before Silver reads them."""

    from scrapers.base.trino_manager import TrinoTableManager
    from scrapers.fbref.typed_bronze import (
        MATCH_AVAILABILITY_TABLE,
        MATCH_DATASET_TABLES,
        SEASON_DATASET_TABLES,
    )

    manager = TrinoTableManager()
    if not manager.table_exists("bronze", MATCH_AVAILABILITY_TABLE):
        manager.create_iceberg_table(
            "bronze",
            MATCH_AVAILABILITY_TABLE,
            {
                "match_id": "VARCHAR",
                "dataset": "VARCHAR",
                "availability": "VARCHAR",
                "reason": "VARCHAR",
                "league": "VARCHAR",
                "season": "BIGINT",
                "source_competition_id": "VARCHAR",
                "source_season_id": "VARCHAR",
                "_source": "VARCHAR",
                "_entity_type": "VARCHAR",
                "_ingested_at": "TIMESTAMP(6)",
                "_batch_id": "VARCHAR",
            },
            partition_columns=["league", "season"],
        )
    tables = sorted({
        "fbref_schedule",
        *MATCH_DATASET_TABLES.values(),
        *SEASON_DATASET_TABLES.values(),
    })
    added = []
    for table in tables:
        if not manager.table_exists("bronze", table):
            continue
        existing = {
            str(column).casefold()
            for column in manager.get_table_columns("bronze", table)
        }
        for column in ("source_competition_id", "source_season_id"):
            if column not in existing:
                manager.add_column("bronze", table, column, "VARCHAR")
                added.append(f"{table}.{column}")
    return {"tables_checked": len(tables), "columns_added": added}


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
    from utils.silver_tasks import check_bronze_table_exists

    logger = logging.getLogger(__name__)

    # Freshness threshold: ingestion runs weekly (Monday). 48h gives a 2-day
    # post-ingest grace window in which Silver should always be fresh; outside
    # that window staleness is expected, so the severity stays WARNING.
    FRESH_HOURS = 48

    checks = [
        # Primary-key nulls — ERROR (joins/dedup logic break otherwise)
        CHECK.no_nulls('silver.fbref_match_enriched', cols=['match_id', 'date']),
        CHECK.no_nulls(
            'silver.fbref_player_identity',
            cols=['player_id', 'player_name', 'team_name', 'league', 'season'],
        ),
        # #463: squad — компонент PK профилей (per-(player, squad) grain).
        CHECK.no_nulls('silver.fbref_player_season_profile', cols=['player_id', 'squad', 'league', 'season']),
        CHECK.no_nulls('silver.fbref_keeper_profile', cols=['player_id', 'squad', 'league', 'season']),
        CHECK.no_nulls('silver.fbref_player_match_stats', cols=['match_id']),
        CHECK.no_nulls(
            'silver.fbref_keeper_match_stats',
            cols=['match_id', 'player_id', 'team_side', 'league', 'season'],
        ),
        CHECK.no_nulls('silver.fbref_match_events', cols=['match_id']),
        CHECK.no_nulls('silver.fbref_match_lineups', cols=['match_id', 'player_id']),
        CHECK.no_nulls('silver.fbref_team_season_profile', cols=['team', 'league', 'season']),

        # PK uniqueness — ERROR (duplicates would explode downstream joins / facts)
        CHECK.no_duplicates('silver.fbref_match_enriched', pk=['match_id']),
        CHECK.no_duplicates(
            'silver.fbref_player_identity',
            pk=['player_id', 'team_name', 'league', 'season'],
        ),
        # #463: профили per-(player, squad, league, season) — зимний трансфер
        # внутри лиги даёт 2 строки на игрока-сезон (по одной на клуб).
        CHECK.no_duplicates('silver.fbref_player_season_profile', pk=['player_id', 'squad', 'league', 'season']),
        CHECK.no_duplicates('silver.fbref_keeper_profile', pk=['player_id', 'squad', 'league', 'season']),
        # #463: team в ключе — зеркало дедупа (parse-артефакт «игрок в обеих
        # командах» теперь не схлопывается молча, а ловится этим чеком).
        CHECK.no_duplicates(
            'silver.fbref_match_lineups',
            pk=['match_id', 'player_id', 'team'],
            where='player_id IS NOT NULL',
        ),
        CHECK.no_duplicates('silver.fbref_team_season_profile', pk=['team', 'league', 'season']),
        # player_match_stats rows can repeat across player+match when player switches teams
        # mid-game (rare); team is part of the natural key.
        CHECK.no_duplicates(
            'silver.fbref_player_match_stats',
            pk=['match_id', 'player_id', 'team'],
            where='player_id IS NOT NULL',
        ),
        CHECK.no_duplicates(
            'silver.fbref_keeper_match_stats',
            pk=['match_id', 'player_id', 'team_side'],
        ),
        CHECK.no_duplicates(
            'silver.fbref_match_events',
            pk=['match_id', 'minute', 'player_id', 'event_type'],
            where='player_id IS NOT NULL',
        ),

        # #898: home_score/away_score must reconstruct the Bronze score string once
        # the shoot-out counts are stripped. Guards the parser in
        # fbref_match_enriched.sql — the old REGEXP silently read '(5) 0–3 (6)' as
        # 5–0. Live baseline after the fix: 0 violating rows.
        CHECK.row_count(
            'silver.fbref_match_enriched',
            min_rows=0, max_rows=0,
            where=(r"score IS NOT NULL AND score <> '' AND ("
                   r"home_score IS NULL OR away_score IS NULL OR "
                   r"TRIM(REGEXP_REPLACE(score, '\(\d+\)\s*', '')) <> "
                   r"CAST(home_score AS varchar) || CHR(8211) || CAST(away_score AS varchar))"),
            severity='ERROR',
            name='score_roundtrip[silver.fbref_match_enriched]',
        ),

        # #901: compare source score to the correctly credited on-field event
        # score only where that comparison is meaningful.  Awarded matches and
        # shootouts have separate contracts below.
        CHECK.row_count(
            'silver.fbref_match_enriched',
            min_rows=0, max_rows=0,
            where=(
                "source_home_score IS NOT NULL AND source_away_score IS NOT NULL "
                "AND NOT is_awarded AND home_shootout_score IS NULL "
                "AND COALESCE(event_row_count, 0) > 0 AND ("
                "source_home_score IS DISTINCT FROM on_field_home_score OR "
                "source_away_score IS DISTINCT FROM on_field_away_score)"
            ),
            severity='ERROR',
            name='score_event_mismatch[silver.fbref_match_enriched]',
        ),
        CHECK.row_count(
            'silver.fbref_match_enriched',
            min_rows=0, max_rows=0,
            where=(
                "source_home_score IS NOT NULL AND source_away_score IS NOT NULL "
                "AND COALESCE(event_row_count, 0) = 0 "
                "AND COALESCE(event_availability, 'unknown') "
                "NOT IN ('restricted', 'not_applicable')"
            ),
            severity='ERROR',
            name='scored_match_without_events[silver.fbref_match_enriched]',
        ),
        CHECK.row_count(
            'silver.fbref_match_enriched',
            min_rows=0, max_rows=0,
            where=(
                "source_home_score IS NOT NULL AND source_away_score IS NOT NULL "
                "AND COALESCE(event_row_count, 0) = 0 "
                "AND event_availability IN ('restricted', 'not_applicable')"
            ),
            severity='WARNING',
            name='restricted_match_events[silver.fbref_match_enriched]',
        ),
        # #902: awarded results are evidence-backed finite decisions.  A new
        # awarded match blocks promotion until its authority/reference is
        # registered; it never silently inherits FBref's inconsistent score.
        CHECK.row_count(
            'silver.fbref_match_enriched',
            min_rows=0, max_rows=0,
            where=(
                "is_awarded AND (official_home_score IS NULL OR "
                "official_away_score IS NULL OR official_score_authority IS NULL OR "
                "official_score_reference IS NULL OR "
                "official_score_provenance <> 'medallion_override')"
            ),
            severity='ERROR',
            name='awarded_result_override_missing[silver.fbref_match_enriched]',
        ),
        CHECK.row_count(
            'silver.fbref_match_enriched',
            min_rows=0, max_rows=0,
            where=(
                "REGEXP_LIKE(source_score_raw, '\\(\\d+\\).*\\(\\d+\\)') "
                "AND (home_shootout_score IS NULL OR away_shootout_score IS NULL)"
            ),
            severity='ERROR',
            name='shootout_score_parse[silver.fbref_match_enriched]',
        ),

        # Referential integrity — ERROR (#258, restored from WARNING #240). The
        # orphan match_ids were duplicate alternate-hex scrapes, not lost matches:
        # bronze.fbref_schedule used to emit the SAME fixture as two rows with
        # DIFFERENT match-page hex ids — one fully-populated (lands in enriched)
        # and one url-only "skeleton" (date NULL → filtered out, yet its match
        # page was still scraped, so child tables carried the alternate hex as an
        # orphan). Root cause fixed upstream (#241/PR#257 align match_url) and
        # Bronze fully re-ingested; clean-re-ingest gate confirmed orphan=0 live
        # (2026-06-03, APL 2025/26). Restored to ERROR so dirty data again blocks
        # the DAG.
        CHECK.ref_integrity('silver.fbref_player_match_stats', 'silver.fbref_match_enriched', 'match_id', severity='ERROR'),
        CHECK.ref_integrity('silver.fbref_keeper_match_stats', 'silver.fbref_match_enriched', 'match_id', severity='ERROR'),
        CHECK.ref_integrity('silver.fbref_match_events', 'silver.fbref_match_enriched', 'match_id', severity='ERROR'),
        CHECK.ref_integrity('silver.fbref_match_lineups', 'silver.fbref_match_enriched', 'match_id', severity='ERROR'),

        # Freshness — WARNING (ingestion is weekly; >48h after Monday is normal mid-week)
        CHECK.freshness('silver.fbref_match_enriched', ts_col='_bronze_ingested_at',
                        max_age_hours=FRESH_HOURS, severity='WARNING'),
        CHECK.freshness('silver.fbref_player_season_profile', ts_col='_bronze_ingested_at',
                        max_age_hours=FRESH_HOURS, severity='WARNING'),
        CHECK.freshness('silver.fbref_keeper_profile', ts_col='_bronze_ingested_at',
                        max_age_hours=FRESH_HOURS, severity='WARNING'),
        CHECK.freshness('silver.fbref_player_match_stats', ts_col='_bronze_ingested_at',
                        max_age_hours=FRESH_HOURS, severity='WARNING'),
        CHECK.freshness('silver.fbref_keeper_match_stats', ts_col='_bronze_ingested_at',
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
        CHECK.value_range('silver.fbref_keeper_match_stats', 'save_pct',
                          min_val=0, max_val=100, severity='WARNING'),
        CHECK.value_range('silver.fbref_team_season_profile', 'possession',
                          min_val=0, max_val=100, severity='WARNING'),
        CHECK.value_range('silver.fbref_team_season_profile', 'goals',
                          min_val=0, severity='WARNING'),
    ]

    # E5: WhoScored unavailability is optional Bronze (see OPTIONAL_BRONZE_TABLES).
    # Probe before appending checks — otherwise run_checks records SQL failures
    # as ERRORs and raises in deployments where the WhoScored DAG is paused.
    if check_bronze_table_exists(
        table_name='whoscored_player_unavailable', schema='silver',
    ):
        # `team_name` is part of the natural key — a player can change clubs
        # mid-season, so (match_id, player_id) alone isn't cross-team unique.
        checks.extend([
            CHECK.no_nulls(
                'silver.whoscored_player_unavailable',
                cols=['match_id', 'team_name', 'ws_player_id', 'match_date'],
            ),
            CHECK.no_duplicates(
                'silver.whoscored_player_unavailable',
                pk=['match_id', 'team_name', 'ws_player_id'],
            ),
            # 168h (7d) tolerance — WhoScored ingest is weekly and sometimes
            # paused (project_whoscored_cloudflare.md); staleness past that
            # is observable but not a blocker for Gold.
            CHECK.freshness(
                'silver.whoscored_player_unavailable',
                ts_col='_bronze_ingested_at',
                max_age_hours=168,
                severity='WARNING',
            ),
        ])
    else:
        logger.warning(
            "silver.whoscored_player_unavailable not found — skipping E5 DQ "
            "checks (Bronze whoscored_missing_players_current likely unavailable)."
        )

    # issue #613: FBref officials is optional Bronze (combined_match_data may
    # not have populated it yet) — probe before appending checks.
    if check_bronze_table_exists(
        table_name='fbref_match_officials', schema='silver',
    ):
        checks.extend([
            CHECK.no_nulls(
                'silver.fbref_match_officials',
                cols=['match_id', 'role', 'official_name', 'league', 'season'],
            ),
            CHECK.no_duplicates(
                'silver.fbref_match_officials',
                pk=['match_id', 'role'],
            ),
            CHECK.freshness(
                'silver.fbref_match_officials',
                ts_col='_bronze_ingested_at',
                max_age_hours=FRESH_HOURS,
                severity='WARNING',
            ),
        ])
    else:
        logger.warning(
            "silver.fbref_match_officials not found — skipping #613 officials "
            "DQ checks (Bronze fbref_match_officials likely absent)."
        )

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
    schedule=None,  # Trigger-only (master-owned after ingestion)
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['transform', 'fbref', 'silver', 'football', 'trino'],
    max_active_runs=1,
    max_active_tasks=1,  # Sequential execution to prevent OOM (each CTAS + import ~1.2GB)
    # issue #530: cap run wall-clock so a stuck/abandoned run auto-fails instead
    # of lingering forever (orphaned up_for_retry TIs accrued under runs that
    # never reached a terminal state). ~10 sequential CTAS @ 30m timeout each.
    dagrun_timeout=timedelta(hours=8),
    doc_md="""
    ## FBref Silver Transformation

    Transforms raw Bronze-layer FBref data into clean, deduplicated Silver tables.

    ### Trigger

    This DAG has **no schedule** — it is triggered after `dag_ingest_fbref`
    completes via `TriggerDagRunOperator` or manual trigger.  A successful run
    includes a successful, fully validated `dag_transform_xref` run; the xref
    player resolver reads `silver.fbref_player_identity` produced here.

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
    | `whoscored_player_unavailable` | Confirmed player absences per match (optional) | whoscored_missing_players_current |

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

    Both validation tasks are fail-closed. Gold is published separately by
    `dag_master_pipeline`, after xref and E3 have completed successfully.

    ### Manual Trigger

    ```bash
    airflow dags trigger dag_transform_fbref_silver
    ```
    """,
) as dag:

    ensure_source_identity_columns = PythonOperator(
        task_id='ensure_source_identity_columns',
        python_callable=_ensure_fbref_source_identity_columns,
    )

    # =========================================================================
    # TaskGroup: Silver Transforms (SEQUENTIAL — max_active_tasks=1 to prevent OOM)
    # =========================================================================
    with TaskGroup(group_id='silver_transforms') as transforms_group:
        transform_tasks = {}
        for task_id, sql_file, table_name in SILVER_TRANSFORMS:
            transform_tasks[table_name] = PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={
                    'sql_file': sql_file,
                    'table_name': table_name,
                },
            )

        # Profiles resolve missing source ids through the shared identity
        # universe.  max_active_tasks=1 limits concurrency but does not define
        # execution order, so the dependency must be explicit.
        transform_tasks['fbref_player_identity'] >> [
            transform_tasks['fbref_player_season_profile'],
            transform_tasks['fbref_keeper_profile'],
            transform_tasks['fbref_player_match_stats'],
            transform_tasks['fbref_match_lineups'],
            transform_tasks['fbref_keeper_match_stats'],
        ]

    # =========================================================================
    # Validation: check row counts in Silver tables
    # =========================================================================
    validate_silver = PythonOperator(
        task_id='validate_silver',
        python_callable=_validate_silver,
    )

    # =========================================================================
    # Quality: data quality checks (null rate, ref integrity, ranges)
    # =========================================================================
    validate_quality = PythonOperator(
        task_id='validate_silver_quality',
        python_callable=_validate_silver_quality,
    )

    # =========================================================================
    # Trigger the identity layer after Silver DQ passes.  This handoff must be
    # synchronous: xref_player reads silver.fbref_player_identity, and the
    # master pipeline must not promote stale xref tables to Gold.
    # =========================================================================
    trigger_xref = TriggerDagRunOperator(
        task_id='trigger_xref_transform',
        trigger_dag_id='dag_transform_xref',
        trigger_run_id='fbref_xref__{{ dag.dag_id }}__{{ run_id }}',
        logical_date='{{ ti.start_date }}',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=False,
        # SILVER_ARGS is tuned for individual CTAS tasks (30 minutes, two
        # retries).  A blocking child-DAG handoff needs its own wall clock and
        # must never retry by resetting an already-running xref DAG.
        execution_timeout=timedelta(hours=4),
        retries=0,
        trigger_rule='all_success',
    )

    # =========================================================================
    # Dependencies
    # =========================================================================
    ensure_source_identity_columns >> transforms_group
    transforms_group >> validate_silver >> validate_quality >> trigger_xref
