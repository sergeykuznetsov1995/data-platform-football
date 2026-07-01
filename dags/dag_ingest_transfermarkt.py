"""
Transfermarkt Data Ingestion DAG
================================

Weekly Bronze ingest of Transfermarkt player + transfer data (issue #43).

Tasks (run sequentially — players is the anchor entity):

    scrape_players
        ↓
        ├─► scrape_market_value_history  ─┐
        └─► scrape_transfers              ─┴► validate_data (all_done)

``transfers`` and ``market_value_history`` resolve their per-player roster
from ``bronze.transfermarkt_players``; if ``scrape_players`` exits with the
TM_FALLBACK soft-success (exit code 2 → bash wrapper exits 0), those tasks
will themselves fall back gracefully and validate_data records the surface.

Architecture mirrors ``dag_ingest_sofascore.py``: BashOperator to run the
scraper in an isolated subprocess (LocalExecutor fork-memory contention),
PythonOperator for cross-task validation.

Schedule: weekly, Monday 04:00 UTC (one hour before Capology, two hours
before FBref).
"""

from datetime import datetime
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.config import CURRENT_SEASON, DAG_TAGS, LEAGUES, SCHEDULES
from utils.default_args import SCRAPER_ARGS


PLAYERS_RESULT_PATH = '/tmp/transfermarkt_players_result.json'
MV_HISTORY_RESULT_PATH = '/tmp/transfermarkt_mv_history_result.json'
TRANSFERS_RESULT_PATH = '/tmp/transfermarkt_transfers_result.json'
COACHES_RESULT_PATH = '/tmp/transfermarkt_coaches_result.json'

# Smoke caps for the first weeks of the DAG — APL has ~600 player rows per
# season, so PLAYERS_DAILY_LIMIT = None means full crawl. market_value_history
# and transfers fan out per-player via the ceapi JSON endpoints (1 HTTP call per
# player); 100 players ≈ 8 min at 12 req/min. This cap is the default for the
# UI-configurable `mv_transfers_limit` Param: the weekly run keeps the rotating
# 100-player window (#620), while a historical backfill overrides it with 0 (no
# cap = full roster in one run, #793).
PLAYERS_DAILY_LIMIT: int = None  # None == full league roster
MV_HISTORY_DAILY_LIMIT: int = 100


def _load_result(path: str, logger) -> Dict[str, Any]:
    import json
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("Results file %s not found", path)
        return {}
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in %s: %s", path, e)
        return {}


def validate_data(**context) -> Dict[str, Any]:
    """Aggregate per-entity row counts + flag soft-fallbacks."""
    import logging

    logger = logging.getLogger(__name__)
    players = _load_result(PLAYERS_RESULT_PATH, logger)
    mv = _load_result(MV_HISTORY_RESULT_PATH, logger)
    transfers = _load_result(TRANSFERS_RESULT_PATH, logger)
    coaches = _load_result(COACHES_RESULT_PATH, logger)

    if not players:
        raise AirflowException(
            f"Players results file {PLAYERS_RESULT_PATH} missing — anchor entity broken"
        )

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'players_rows': players.get('rows', 0),
            'players_with_rows': players.get('players_with_rows', 0),
            'players_fallback': players.get('fallback', False),
            'mv_history_rows': mv.get('rows', 0),
            'mv_history_players': mv.get('players_with_rows', 0),
            'mv_history_fallback': mv.get('fallback', False),
            'transfers_rows': transfers.get('rows', 0),
            'transfers_players': transfers.get('players_with_rows', 0),
            'transfers_fallback': transfers.get('fallback', False),
            'coaches_rows': coaches.get('rows', 0),
            'coaches_fallback': coaches.get('fallback', False),
            'tables': (
                players.get('tables', [])
                + mv.get('tables', [])
                + transfers.get('tables', [])
                + coaches.get('tables', [])
            ),
        },
    }

    errors: List[str] = []
    errors.extend(players.get('errors', []) or [])
    errors.extend(mv.get('errors', []) or [])
    errors.extend(transfers.get('errors', []) or [])
    errors.extend(coaches.get('errors', []) or [])
    if errors:
        validation['warnings'] = errors
        total_rows = sum([
            validation['summary']['players_rows'],
            validation['summary']['mv_history_rows'],
            validation['summary']['transfers_rows'],
            validation['summary']['coaches_rows'],
        ])
        validation['status'] = 'partial_success' if total_rows > 0 else 'failed'

    # Minimum thresholds (APL: 20 clubs × ~25 players ≈ 500 players/season).
    if validation['summary']['players_rows'] < 400:
        if validation['summary']['players_fallback']:
            validation['warnings'].append(
                f"players TM_FALLBACK: rows={validation['summary']['players_rows']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low players row count: {validation['summary']['players_rows']} < 400"
            )

    # MV history ≥ ~10 timeline points per player on average.
    if validation['summary']['mv_history_rows'] < 500:
        if validation['summary']['mv_history_fallback']:
            validation['warnings'].append(
                f"mv_history TM_FALLBACK: rows={validation['summary']['mv_history_rows']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low mv_history row count: {validation['summary']['mv_history_rows']} < 500"
            )

    # Transfers: typical APL player has 1–4 transfer events; smoke cap=100 players.
    if validation['summary']['transfers_rows'] < 50:
        if validation['summary']['transfers_fallback']:
            validation['warnings'].append(
                f"transfers TM_FALLBACK: rows={validation['summary']['transfers_rows']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low transfers row count: {validation['summary']['transfers_rows']} < 50"
            )

    # Coaches: ~20 head coaches + mid-season replacements per season (#434).
    if validation['summary']['coaches_rows'] < 15:
        if validation['summary']['coaches_fallback']:
            validation['warnings'].append(
                f"coaches TM_FALLBACK: rows={validation['summary']['coaches_rows']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low coaches row count: {validation['summary']['coaches_rows']} < 15"
            )

    logger.info("Validation: status=%s summary=%s", validation['status'], validation['summary'])
    if validation['warnings']:
        logger.warning("Warnings: %s", validation['warnings'])

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")
    return validation


with DAG(
    dag_id='dag_ingest_transfermarkt',
    default_args=SCRAPER_ARGS,
    description='Ingest Transfermarkt player + transfer Bronze (issue #43)',
    schedule=SCHEDULES.get('dag_ingest_transfermarkt', '0 4 * * 1'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get(
        'transfermarkt',
        ['scraping', 'transfermarkt', 'bronze', 'football'],
    ),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        # Season is UI-configurable: the weekly scheduled run uses the default
        # (CURRENT_SEASON); to backfill a past season, use "Trigger DAG w/ config"
        # and set season (e.g. 2016 = 2016/17). New (league, season) partitions
        # are written via replace_partitions, so backfilling an early season
        # leaves all other partitions untouched (issue #717, epic #708).
        'season': Param(
            default=CURRENT_SEASON,
            type='integer',
            minimum=2000,
            maximum=CURRENT_SEASON,
            title='Season (start year)',
            description=(
                'APL season start year (2016 = 2016/17 season). '
                'Default = current season for the weekly run. Override here to '
                'backfill a past season (e.g. 2016…2024 for 10-season history).'
            ),
        ),
        # market_value_history / transfers fan out per-player, so the weekly run
        # caps each at MV_HISTORY_DAILY_LIMIT (100) and rotates the roster window
        # (#620) — the full roster accumulates over ~6 weeks. That one-shot
        # window is too shallow for a historical backfill (~100 of ~750 players),
        # so the cap is UI-configurable: set 0 = no cap = full roster in one run
        # (issue #793). The default leaves the weekly window behaviour unchanged.
        'mv_transfers_limit': Param(
            default=MV_HISTORY_DAILY_LIMIT,
            type='integer',
            minimum=0,
            title='MV/transfers roster cap',
            description=(
                'Players per run for market_value_history / transfers. '
                '100 = weekly rotating window (#620). 0 = full roster in one '
                'run (historical backfill, #793).'
            ),
        ),
    },
    doc_md="""
    ## Transfermarkt Data Ingestion (Issue #43)

    Bronze ingest for Transfermarkt player snapshots, market-value history,
    and transfer events.

    ### Architecture

    - BashOperator per entity (isolated subprocess for memory safety)
    - ``scrape_players`` is the anchor — MV history and transfers resolve
      their player_id roster from ``bronze.transfermarkt_players``
    - ``validate_data`` (PythonOperator, ``trigger_rule='all_done'``)
      aggregates per-entity row counts and flags soft TM_FALLBACK exits

    ### Bronze tables written
    - ``iceberg.bronze.transfermarkt_players`` (partition: league/season)
    - ``iceberg.bronze.transfermarkt_market_value_history`` (partition: league/season)
    - ``iceberg.bronze.transfermarkt_transfers`` (partition: league/season)

    ``players`` is a full crawl written with whole-partition replace
    (``replace_partitions=['league','season']``). ``market_value_history`` and
    ``transfers`` are rate-capped to ~100 players/run, so each run scrapes the
    NEXT rotating roster window (``--as-of-date {{ ds }}`` → window index) and
    **upserts by player** (``replace_partitions=['league','season','player_id']``)
    so prior windows accumulate — full roster covered over ~6 weekly runs (#620).

    ### Backfill past seasons (issue #717, epic #708)

    Season is UI-configurable via the ``season`` Param. Use "Trigger DAG w/
    config" and set ``season`` to a start year (e.g. 2016 = 2016/17) to ingest a
    past season; ``replace_partitions`` keeps other ``(league, season)``
    partitions untouched. ``players`` and ``coaches`` are full per-season crawls.
    ``market_value_history`` / ``transfers`` default to one rotating ~100-player
    window per run (keyed on ``{{ ds }}``); for a historical backfill that one
    window is too shallow (~100 of ~750 players), so set ``mv_transfers_limit=0``
    in the trigger config to scrape the **full roster in a single run** (issue
    #793). Example backfill config: ``{"season": 2018, "mv_transfers_limit": 0}``.
    """,
) as dag:

    league = LEAGUES[0]
    # --season is rendered at runtime from params.season (Jinja), so the season
    # is UI-configurable ("Trigger DAG w/ config") without a separate backfill
    # DAG. The f-string escapes {{ }} as {{{{ }}}} so the literal Jinja tag
    # survives into the rendered command (mirrors dag_ingest_matchhistory #710).
    players_limit_arg = f' --limit {PLAYERS_DAILY_LIMIT}' if PLAYERS_DAILY_LIMIT else ''

    scrape_players_task = BashOperator(
        task_id='scrape_players',
        bash_command=f"""
cd /opt/airflow && \\
rm -f {PLAYERS_RESULT_PATH} && \\
python dags/scripts/run_transfermarkt_scraper.py \\
    --entity players \\
    --league "{league}" \\
    --season {{{{ params.season }}}}{players_limit_arg} \\
    --output {PLAYERS_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "TM_FALLBACK exit-code 2 (players) — propagating as soft success."
    exit 0
fi
exit $rc
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
    )

    scrape_mv_history_task = BashOperator(
        task_id='scrape_market_value_history',
        bash_command=f"""
cd /opt/airflow && \\
rm -f {MV_HISTORY_RESULT_PATH} && \\
python dags/scripts/run_transfermarkt_scraper.py \\
    --entity market_value_history \\
    --league "{league}" \\
    --season {{{{ params.season }}}} \\
    --limit {{{{ params.mv_transfers_limit }}}} \\
    --as-of-date {{{{ ds }}}} \\
    --output {MV_HISTORY_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "TM_FALLBACK exit-code 2 (mv_history) — propagating as soft success."
    exit 0
fi
exit $rc
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
    )

    scrape_transfers_task = BashOperator(
        task_id='scrape_transfers',
        bash_command=f"""
cd /opt/airflow && \\
rm -f {TRANSFERS_RESULT_PATH} && \\
python dags/scripts/run_transfermarkt_scraper.py \\
    --entity transfers \\
    --league "{league}" \\
    --season {{{{ params.season }}}} \\
    --limit {{{{ params.mv_transfers_limit }}}} \\
    --as-of-date {{{{ ds }}}} \\
    --output {TRANSFERS_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "TM_FALLBACK exit-code 2 (transfers) — propagating as soft success."
    exit 0
fi
exit $rc
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
    )

    # issue #434: head coaches → bronze.transfermarkt_coaches. Independent of
    # scrape_players (discovers via club staff pages, not the players table).
    scrape_coaches_task = BashOperator(
        task_id='scrape_coaches',
        bash_command=f"""
cd /opt/airflow && \\
rm -f {COACHES_RESULT_PATH} && \\
python dags/scripts/run_transfermarkt_scraper.py \\
    --entity coaches \\
    --league "{league}" \\
    --season {{{{ params.season }}}} \\
    --output {COACHES_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "TM_FALLBACK exit-code 2 (coaches) — propagating as soft success."
    exit 0
fi
exit $rc
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
    )

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        trigger_rule='all_done',
    )

    def _check_traffic_guard(**ctx):
        """Residential-MB kill-switch per entity (mirrors the FBref guard).

        Thresholds are sized for the squad-first crawl (players ≈ 4-5 MB,
        coaches ≈ 3-4 MB, ceapi entities ≈ 0.5-7 MB incl. full-roster
        backfills) with headroom for retries. Override per entity via
        Airflow Variable ``tm_proxy_mb_threshold_<entity>`` or globally via
        ``tm_proxy_mb_threshold``.
        """
        from utils.proxy_traffic import check_result_traffic_guard

        return check_result_traffic_guard(
            entity_paths={
                'players': PLAYERS_RESULT_PATH,
                'market_value_history': MV_HISTORY_RESULT_PATH,
                'transfers': TRANSFERS_RESULT_PATH,
                'coaches': COACHES_RESULT_PATH,
            },
            default_thresholds={
                'players': 10.0,
                'market_value_history': 4.0,
                'transfers': 8.0,
                'coaches': 6.0,
            },
        )

    # Parallel leaf, not part of the validate → silver chain: a budget breach
    # turns the run red (Telegram via SCRAPER_ARGS callbacks) but does NOT
    # block promoting the already-paid-for data to Silver.
    traffic_guard_task = PythonOperator(
        task_id='check_traffic_guard',
        python_callable=_check_traffic_guard,
        trigger_rule='all_done',
    )

    def _validate_bronze_quality(**ctx) -> None:
        """Trino-level CHECK gate over the 3 Transfermarkt Bronze tables.

        transfermarkt_players checks (row_count/no_duplicates/no_nulls/
        freshness) are ERROR-severity (promoted after green weekly runs,
        issue #48). market_value_history / transfers row_count stay
        WARNING — a TM_FALLBACK soft exit upstream would otherwise
        legitimately fail them.
        """
        from utils.data_quality import CHECK, run_checks

        # Season comes from params (UI-configurable, #717): a backfill run gates
        # against the season it actually scraped, not CURRENT_SEASON — otherwise
        # the checks query an empty (league, current-season) partition and the
        # ERROR row_count gate fails the historical run.
        season = int(ctx['params']['season'])
        season_short = (
            f"{str(season)[2:4]}"
            f"{(int(str(season)[2:4]) + 1) % 100:02d}"
        )
        where = f"league = '{LEAGUES[0]}' AND season = '{season_short}'"
        checks = [
            CHECK.row_count(
                'bronze.transfermarkt_players',
                min_rows=400, where=where, severity='ERROR',
            ),
            # PK includes current_club_id: a player can legitimately appear in
            # two clubs' squads within one historical season (mid-season
            # transfer / loan — e.g. Sancho, Rashford in 2024/25), so the raw
            # Bronze grain is (league, season, player_id, club). Keying on
            # player_id alone false-flagged ~9–31 such rows per backfilled
            # season as duplicates (#717). Silver dedups to per-player.
            CHECK.no_duplicates(
                'bronze.transfermarkt_players',
                pk=['league', 'season', 'player_id', 'current_club_id'],
                where=where, severity='ERROR',
            ),
            CHECK.no_nulls(
                'bronze.transfermarkt_players',
                cols=['player_id', 'name'],
                where=where, severity='ERROR',
            ),
            CHECK.freshness(
                'bronze.transfermarkt_players',
                ts_col='_ingested_at', max_age_hours=48,
                where=where, severity='ERROR',
            ),
            CHECK.row_count(
                'bronze.transfermarkt_market_value_history',
                min_rows=500, where=where, severity='WARNING',
            ),
            CHECK.row_count(
                'bronze.transfermarkt_transfers',
                min_rows=50, where=where, severity='WARNING',
            ),
            # coaches (issue #434): ~20 head coaches/season. WARNING — a
            # TM_FALLBACK soft exit upstream would otherwise fail it.
            CHECK.row_count(
                'bronze.transfermarkt_coaches',
                min_rows=15, where=where, severity='WARNING',
            ),
            # Rotating-window roster coverage (#620): every player of the
            # scraped season should accumulate mv_history/transfers rows
            # within ~6 weekly runs. WARNING-only (error_threshold=0):
            # the first weeks after a season start / fresh backfill are
            # legitimately below target and must not block Silver.
            CHECK.coverage(
                'bronze.transfermarkt_players',
                condition=(
                    'player_id IN (SELECT player_id FROM '
                    'iceberg.bronze.transfermarkt_market_value_history)'
                ),
                where=where,
                warn_threshold=0.90,
                error_threshold=0.0,
                name='mv_history_roster_coverage',
            ),
            CHECK.coverage(
                'bronze.transfermarkt_players',
                condition=(
                    'player_id IN (SELECT player_id FROM '
                    'iceberg.bronze.transfermarkt_transfers)'
                ),
                where=where,
                warn_threshold=0.90,
                error_threshold=0.0,
                name='transfers_roster_coverage',
            ),
        ]
        report = run_checks(checks, raise_on_error=True)
        import logging
        logging.getLogger(__name__).info(
            "validate_bronze_quality: %s", report.summary(),
        )

    validate_bronze_quality_task = PythonOperator(
        task_id='validate_bronze_quality',
        python_callable=_validate_bronze_quality,
        trigger_rule='all_done',
    )

    # Cascade Bronze→Silver: triggers dag_transform_transfermarkt_silver
    # (issue #60). wait_for_completion=False keeps Bronze DAG short; the
    # Silver DAG runs its own DQ gate.
    trigger_silver_task = TriggerDagRunOperator(
        task_id='trigger_silver',
        trigger_dag_id='dag_transform_transfermarkt_silver',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    scrape_players_task >> scrape_mv_history_task
    scrape_players_task >> scrape_transfers_task
    [
        scrape_players_task,
        scrape_mv_history_task,
        scrape_transfers_task,
        scrape_coaches_task,
    ] >> validate_task >> validate_bronze_quality_task >> trigger_silver_task
    [
        scrape_players_task,
        scrape_mv_history_task,
        scrape_transfers_task,
        scrape_coaches_task,
    ] >> traffic_guard_task
