"""
SofaScore Data Ingestion DAG
============================

Airflow DAG for scraping football statistics from SofaScore.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Schedules daily at 11 AM UTC.

Data collected:
- Match schedules and results
- Team season statistics
- Player season statistics

All data is written to Iceberg Bronze layer tables (via Parquet fallback).
"""

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import LEAGUES, CURRENT_SEASON, SCHEDULES, DAG_TAGS
from utils.default_args import DEFAULT_ARGS


SCHEDULE_RESULT_PATH = '/tmp/sofascore_result.json'
PLAYER_RATINGS_RESULT_PATH = '/tmp/sofascore_player_ratings_result.json'
SHOTMAP_RESULT_PATH = '/tmp/sofascore_shotmap_result.json'
EVENT_PLAYER_STATS_RESULT_PATH = '/tmp/sofascore_event_player_stats_result.json'
MATCH_STATS_RESULT_PATH = '/tmp/sofascore_match_stats_result.json'
PLAYER_SEASON_STATS_RESULT_PATH = '/tmp/sofascore_player_season_stats_result.json'
PLAYER_PROFILE_RESULT_PATH = '/tmp/sofascore_player_profile_result.json'


def _env_int(name: str) -> Optional[int]:
    """Read an int from ENV; empty / unparseable / non-positive → None."""
    raw = os.environ.get(name, '').strip()
    if not raw:
        return None
    try:
        v = int(raw)
    except ValueError:
        return None
    return v if v > 0 else None


# Per-endpoint safety knobs. Default = None (no cap, full coverage).
# Override via ENV on dev / staging to keep runs bounded. Issue #69.
SHOTMAP_DAILY_LIMIT = _env_int('SS_SHOTMAP_LIMIT')
EVENT_PLAYER_STATS_DAILY_LIMIT = _env_int('SS_EPS_LIMIT')
MATCH_STATS_DAILY_LIMIT = _env_int('SS_MATCH_STATS_LIMIT')
PLAYER_SEASON_STATS_DAILY_LIMIT = _env_int('SS_PSS_LIMIT')
PLAYER_PROFILE_DAILY_LIMIT = _env_int('SS_PP_LIMIT')


def _limit_arg(limit: Optional[int]) -> str:
    """Render ``--limit N`` only when limit is set; else empty string."""
    return f"--limit {int(limit)}" if limit else ""


def _load_result(path: str, logger) -> Dict[str, Any]:
    """Load a runner JSON output. Missing file → empty dict (treated as failure)."""
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
    """
    Validate scraped data quality across both scrape tasks (schedule+league_table
    and player_ratings).
    """
    import logging

    logger = logging.getLogger(__name__)

    schedule_result = _load_result(SCHEDULE_RESULT_PATH, logger)
    ratings_result = _load_result(PLAYER_RATINGS_RESULT_PATH, logger)
    shotmap_result = _load_result(SHOTMAP_RESULT_PATH, logger)
    eps_result = _load_result(EVENT_PLAYER_STATS_RESULT_PATH, logger)
    match_stats_result = _load_result(MATCH_STATS_RESULT_PATH, logger)
    pss_result = _load_result(PLAYER_SEASON_STATS_RESULT_PATH, logger)
    pp_result = _load_result(PLAYER_PROFILE_RESULT_PATH, logger)

    if not schedule_result:
        raise AirflowException(
            f"Schedule results file {SCHEDULE_RESULT_PATH} missing or unreadable"
        )

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'schedule_rows': schedule_result.get('schedule_rows', 0),
            'league_table_rows': schedule_result.get('league_table_rows', 0),
            'player_ratings_rows': ratings_result.get('rows', 0),
            'player_ratings_matches': ratings_result.get('matches_with_ratings', 0),
            'player_ratings_fallback': ratings_result.get('fallback', False),
            'shotmap_rows': shotmap_result.get('rows', 0),
            'shotmap_matches': shotmap_result.get('matches_with_rows', 0),
            'shotmap_fallback': shotmap_result.get('fallback', False),
            'event_player_stats_rows': eps_result.get('rows', 0),
            'event_player_stats_matches': eps_result.get('matches_with_rows', 0),
            'event_player_stats_fallback': eps_result.get('fallback', False),
            'match_stats_rows': match_stats_result.get('rows', 0),
            'match_stats_matches': match_stats_result.get('matches_with_rows', 0),
            'match_stats_fallback': match_stats_result.get('fallback', False),
            'player_season_stats_rows': pss_result.get('rows', 0),
            'player_season_stats_players': pss_result.get('players_with_rows', 0),
            'player_season_stats_fallback': pss_result.get('fallback', False),
            'player_profile_rows': pp_result.get('rows', 0),
            'player_profile_players': pp_result.get('players_with_rows', 0),
            'player_profile_fallback': pp_result.get('fallback', False),
            'tables': (
                schedule_result.get('tables', [])
                + ratings_result.get('tables', [])
                + shotmap_result.get('tables', [])
                + eps_result.get('tables', [])
                + match_stats_result.get('tables', [])
                + pss_result.get('tables', [])
                + pp_result.get('tables', [])
            ),
        }
    }

    errors: List[str] = []
    errors.extend(schedule_result.get('errors', []) or [])
    errors.extend(ratings_result.get('errors', []) or [])
    errors.extend(shotmap_result.get('errors', []) or [])
    errors.extend(eps_result.get('errors', []) or [])
    errors.extend(match_stats_result.get('errors', []) or [])
    errors.extend(pss_result.get('errors', []) or [])
    errors.extend(pp_result.get('errors', []) or [])
    if errors:
        validation['warnings'] = errors
        total_rows = sum([
            validation['summary']['schedule_rows'],
            validation['summary']['league_table_rows'],
            validation['summary']['player_ratings_rows'],
            validation['summary']['shotmap_rows'],
            validation['summary']['event_player_stats_rows'],
            validation['summary']['match_stats_rows'],
            validation['summary']['player_season_stats_rows'],
            validation['summary']['player_profile_rows'],
        ])
        validation['status'] = 'partial_success' if total_rows > 0 else 'failed'

    # Minimum thresholds
    if validation['summary']['schedule_rows'] < 100:
        validation['warnings'].append("Low schedule row count - possible scraping issue")

    if validation['summary']['league_table_rows'] < 10:
        validation['warnings'].append("Low league_table row count - possible scraping issue")

    # APL has ~300 matches/season; ratings emit ~25K rows. Anything < 300 rows
    # means we scraped at most a handful of matches → DAG defect or hard CF block.
    if validation['summary']['player_ratings_rows'] < 300:
        if validation['summary']['player_ratings_fallback']:
            validation['warnings'].append(
                f"player_ratings R0.2B_FALLBACK: rows="
                f"{validation['summary']['player_ratings_rows']} matches="
                f"{validation['summary']['player_ratings_matches']}"
            )
            # Fallback is a soft failure — keep status non-failed so dependent
            # DAGs see partial_success, not hard-fail.
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low player_ratings row count: "
                f"{validation['summary']['player_ratings_rows']} < 300"
            )

    # Shotmap: full APL season ≈ 380 matches × ~25 shots/match ≈ 9.5K rows.
    # WARN-only threshold = 300 (issue #69; covers first few gameweeks too).
    if validation['summary']['shotmap_rows'] < 300:
        if validation['summary']['shotmap_fallback']:
            validation['warnings'].append(
                f"shotmap R0.2B_FALLBACK: rows="
                f"{validation['summary']['shotmap_rows']} matches="
                f"{validation['summary']['shotmap_matches']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low shotmap row count: "
                f"{validation['summary']['shotmap_rows']} < 300"
            )

    # event_player_stats: full APL season ≈ 380 matches × ~25 played players
    # ≈ 9.5K rows. WARN-only threshold = 10K (issue #69).
    if validation['summary']['event_player_stats_rows'] < 10000:
        if validation['summary']['event_player_stats_fallback']:
            validation['warnings'].append(
                f"event_player_stats R0.2B_FALLBACK: rows="
                f"{validation['summary']['event_player_stats_rows']} matches="
                f"{validation['summary']['event_player_stats_matches']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low event_player_stats row count: "
                f"{validation['summary']['event_player_stats_rows']} < 10000"
            )

    # match_stats: full APL season ≈ 380 matches × 3 periods × ~30 stats
    # ≈ 34K rows. WARN-only threshold = 10K (issue #69).
    if validation['summary']['match_stats_rows'] < 10000:
        if validation['summary']['match_stats_fallback']:
            validation['warnings'].append(
                f"match_stats R0.2B_FALLBACK: rows="
                f"{validation['summary']['match_stats_rows']} matches="
                f"{validation['summary']['match_stats_matches']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low match_stats row count: "
                f"{validation['summary']['match_stats_rows']} < 10000"
            )

    # player_season_stats: 1 row per player. APL ≈ 526 active players.
    # WARN-only threshold = 400 (issue #69).
    if validation['summary']['player_season_stats_rows'] < 400:
        if validation['summary']['player_season_stats_fallback']:
            validation['warnings'].append(
                f"player_season_stats R0.2B_FALLBACK: rows="
                f"{validation['summary']['player_season_stats_rows']} players="
                f"{validation['summary']['player_season_stats_players']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low player_season_stats row count: "
                f"{validation['summary']['player_season_stats_rows']} < 400"
            )

    # player_profile: 1 row per player. APL ≈ 526 active players.
    # WARN-only threshold = 400 (issue #69).
    if validation['summary']['player_profile_rows'] < 400:
        if validation['summary']['player_profile_fallback']:
            validation['warnings'].append(
                f"player_profile R0.2B_FALLBACK: rows="
                f"{validation['summary']['player_profile_rows']} players="
                f"{validation['summary']['player_profile_players']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low player_profile row count: "
                f"{validation['summary']['player_profile_rows']} < 400"
            )

    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {validation['summary']}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")

    return validation


# Build arguments for bash command
leagues_str = ','.join(LEAGUES)

# DAG definition
with DAG(
    dag_id='dag_ingest_sofascore',
    default_args=DEFAULT_ARGS,
    description='Ingest football statistics from SofaScore',
    schedule=SCHEDULES.get('dag_ingest_sofascore', '0 11 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('sofascore', ['scraping', 'sofascore', 'bronze']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        'season': CURRENT_SEASON,
    },
    doc_md="""
    ## SofaScore Data Ingestion

    This DAG scrapes football statistics from SofaScore.

    ### Architecture

    Uses BashOperator to run scraper in isolated subprocess,
    preventing LocalExecutor fork memory issues.

    ### Data Collected

    - **Schedule**: Match dates, teams, scores, venues
    - **Team Stats**: Season-level team statistics
    - **Player Stats**: Season-level player statistics

    ### Daily limits (issue #69)

    No per-endpoint cap by default. Override via ENV on dev/staging:
    `SS_SHOTMAP_LIMIT`, `SS_EPS_LIMIT`, `SS_MATCH_STATS_LIMIT`,
    `SS_PSS_LIMIT`, `SS_PP_LIMIT` (positive int → cap).

    ### Skip-existing (event-grain)

    `shotmap`, `event_player_stats`, `match_stats` skip match_ids already
    materialised in their bronze table → APPEND mode. Snapshot endpoints
    (`player_season_stats`, `player_profile`) keep full refresh
    (`replace_partitions=['league','season']`).

    **Manual full refresh**: `TRUNCATE iceberg.bronze.sofascore_<table>`
    via `make shell-trino`, then trigger the DAG.

    ### Notes

    - Uses soccerdata library wrapper
    - Written to Parquet fallback (PyIceberg disabled for stability)
    """,
) as dag:

    scrape_data_task = BashOperator(
        task_id='scrape_sofascore_data',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_sofascore_scraper.py \\
    --leagues "{leagues_str}" \\
    --season {CURRENT_SEASON} \\
    --output {SCHEDULE_RESULT_PATH}
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
    )

    # R0.2B player_ratings: depends on freshly written bronze.sofascore_schedule
    # (runner reads finished match_ids from there). Exit code 2 = graceful
    # R0.2B_FALLBACK; treat as success at the bash level so validate_data runs.
    scrape_player_ratings_task = BashOperator(
        task_id='scrape_player_ratings',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity player_ratings \\
    --league "{LEAGUES[0]}" \\
    --season {CURRENT_SEASON} \\
    --output {PLAYER_RATINGS_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "R0.2B_FALLBACK exit-code 2 — propagating as soft success."
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

    # #22 — per-shot xG/coords/situation. No daily cap by default (#69);
    # set `SS_SHOTMAP_LIMIT` ENV to bound on dev. Skip-existing in runner
    # keeps steady-state runs cheap.
    scrape_shotmap_task = BashOperator(
        task_id='scrape_shotmap',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity shotmap \\
    --league "{LEAGUES[0]}" \\
    --season {CURRENT_SEASON} \\
    {_limit_arg(SHOTMAP_DAILY_LIMIT)} \\
    --output {SHOTMAP_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "R0.2B_FALLBACK exit-code 2 (shotmap) — propagating as soft success."
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

    # #25 — team-level per-match stats (per-period long-form). Reads
    # match_ids from bronze.sofascore_schedule; runs in parallel with
    # shotmap (independent of player_ratings). No daily cap by default (#69).
    scrape_match_stats_task = BashOperator(
        task_id='scrape_match_stats',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity match_stats \\
    --league "{LEAGUES[0]}" \\
    --season {CURRENT_SEASON} \\
    {_limit_arg(MATCH_STATS_DAILY_LIMIT)} \\
    --output {MATCH_STATS_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "R0.2B_FALLBACK exit-code 2 (match_stats) — propagating as soft success."
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

    # #24 — season-aggregate per-player Opta stats. Depends on fresh
    # bronze.sofascore_player_ratings (provides DISTINCT player_ids).
    # Snapshot grain — full refresh every run (no skip-existing).
    scrape_player_season_stats_task = BashOperator(
        task_id='scrape_player_season_stats',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity player_season_stats \\
    --league "{LEAGUES[0]}" \\
    --season {CURRENT_SEASON} \\
    {_limit_arg(PLAYER_SEASON_STATS_DAILY_LIMIT)} \\
    --output {PLAYER_SEASON_STATS_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "R0.2B_FALLBACK exit-code 2 (player_season_stats) — propagating as soft success."
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

    # #23 — biographical snapshot per player. Cheap, runs after ratings.
    # Snapshot grain — full refresh every run (no skip-existing).
    scrape_player_profile_task = BashOperator(
        task_id='scrape_player_profile',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity player_profile \\
    --league "{LEAGUES[0]}" \\
    --season {CURRENT_SEASON} \\
    {_limit_arg(PLAYER_PROFILE_DAILY_LIMIT)} \\
    --output {PLAYER_PROFILE_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "R0.2B_FALLBACK exit-code 2 (player_profile) — propagating as soft success."
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

    # #21 — per-(match, player) Opta stats. Depends on fresh
    # bronze.sofascore_player_ratings (provides the player_id list per match).
    # No daily cap by default (#69); runner skip-existing keeps steady-state
    # runs ~10 min.
    scrape_event_player_stats_task = BashOperator(
        task_id='scrape_event_player_stats',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity event_player_stats \\
    --league "{LEAGUES[0]}" \\
    --season {CURRENT_SEASON} \\
    {_limit_arg(EVENT_PLAYER_STATS_DAILY_LIMIT)} \\
    --output {EVENT_PLAYER_STATS_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "R0.2B_FALLBACK exit-code 2 (event_player_stats) — propagating as soft success."
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

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        trigger_rule='all_done',
    )

    # schedule writes match_ids → ratings (depends on schedule),
    # then [shotmap, match_stats] in parallel (both need only schedule),
    # then [event_player_stats, player_season_stats, player_profile]
    # in parallel (all three need ratings for player_id list),
    # then validate_data on all_done.
    scrape_data_task >> scrape_player_ratings_task
    scrape_data_task >> scrape_shotmap_task
    scrape_data_task >> scrape_match_stats_task
    scrape_player_ratings_task >> scrape_event_player_stats_task
    scrape_player_ratings_task >> scrape_player_season_stats_task
    scrape_player_ratings_task >> scrape_player_profile_task
    [
        scrape_player_ratings_task,
        scrape_shotmap_task,
        scrape_match_stats_task,
        scrape_event_player_stats_task,
        scrape_player_season_stats_task,
        scrape_player_profile_task,
    ] >> validate_data_task
