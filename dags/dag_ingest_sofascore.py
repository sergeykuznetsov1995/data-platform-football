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

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import LEAGUES, CURRENT_SEASON, SCHEDULES, DAG_TAGS
from utils.default_args import SCRAPER_ARGS


SCHEDULE_RESULT_PATH = '/tmp/sofascore_result.json'
# #751 PR1+PR2 — one consolidated Camoufox capture per match writes ALL FOUR
# per-match tables: player_ratings, event_player_stats, match_stats, shotmap
# (replaces four separate tls passes).
MATCH_CAPTURE_RESULT_PATH = '/tmp/sofascore_match_capture_result.json'
# #751 PR3 — per-player profile + season_stats moved OUT of this daily DAG into
# the weekly dag_ingest_sofascore_players (~526 Camoufox navs is too heavy daily).


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
    # #751 PR1+PR2: ratings + event_player_stats + match_stats + shotmap now all
    # come from ONE consolidated capture run (single result file carrying
    # `rows`/`matches_with_ratings`, `eps_rows`/`eps_matches`,
    # `match_stats_rows`/`match_stats_matches`, `shotmap_rows`/`shotmap_matches`).
    capture_result = _load_result(MATCH_CAPTURE_RESULT_PATH, logger)

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
            'player_ratings_rows': capture_result.get('rows', 0),
            'player_ratings_matches': capture_result.get('matches_with_ratings', 0),
            'player_ratings_fallback': capture_result.get('fallback', False),
            'shotmap_rows': capture_result.get('shotmap_rows', 0),
            'shotmap_matches': capture_result.get('shotmap_matches', 0),
            'shotmap_fallback': capture_result.get('fallback', False),
            'event_player_stats_rows': capture_result.get('eps_rows', 0),
            'event_player_stats_matches': capture_result.get('eps_matches', 0),
            'event_player_stats_fallback': capture_result.get('fallback', False),
            'match_stats_rows': capture_result.get('match_stats_rows', 0),
            'match_stats_matches': capture_result.get('match_stats_matches', 0),
            'match_stats_fallback': capture_result.get('fallback', False),
            'tables': (
                schedule_result.get('tables', [])
                + capture_result.get('tables', [])
            ),
        }
    }

    errors: List[str] = []
    errors.extend(schedule_result.get('errors', []) or [])
    errors.extend(capture_result.get('errors', []) or [])
    if errors:
        validation['warnings'] = errors
        total_rows = sum([
            validation['summary']['schedule_rows'],
            validation['summary']['league_table_rows'],
            validation['summary']['player_ratings_rows'],
            validation['summary']['shotmap_rows'],
            validation['summary']['event_player_stats_rows'],
            validation['summary']['match_stats_rows'],
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

    # player_season_stats + player_profile moved to the weekly
    # dag_ingest_sofascore_players (#751 PR3) — validated there, not here.

    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {validation['summary']}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")

    return validation


def validate_bronze_freshness(**context) -> None:
    """Telegram-alert when bronze.sofascore_* stops refreshing (issue #751).

    The scrape tasks soft-exit (R0.2B_FALLBACK, exit 2) when SofaScore's
    anti-bot returns 403, so the DAG stays green while data silently goes
    stale (match-data stalled 26 days before anyone noticed). ``validate_data``
    only checks the row_count of the *current* run's JSON output — pre-existing
    stale rows still pass that floor. A direct MAX(_ingested_at) freshness
    check is what surfaces a multi-day ingestion stall.

    WARNING-severity (not ERROR) on purpose: the goal is to stop being silent
    (ping Telegram), not to hard-fail the DAG while the scraper fix (FlareSolverr
    migration, PR B) lands. Promote to ERROR after that yields green runs.
    """
    import logging

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    logger = logging.getLogger(__name__)

    # Global table freshness (MAX(_ingested_at), no season filter) — robust to
    # SofaScore's varchar season slug and catches any ingestion stall. 48h gives
    # one missed daily run of grace before alerting.
    checks = [
        CHECK.freshness(
            'bronze.sofascore_match_stats',
            ts_col='_ingested_at', max_age_hours=48, severity='WARNING',
        ),
        CHECK.freshness(
            'bronze.sofascore_event_player_stats',
            ts_col='_ingested_at', max_age_hours=48, severity='WARNING',
        ),
        CHECK.freshness(
            'bronze.sofascore_player_ratings',
            ts_col='_ingested_at', max_age_hours=48, severity='WARNING',
        ),
    ]
    report = run_checks(checks, raise_on_error=False)
    logger.info("validate_bronze_freshness: %s", report.summary())
    telegram_dq_summary(report, header='SofaScore Bronze freshness')


# Build arguments for bash command
leagues_str = ','.join(LEAGUES)

# DAG definition
with DAG(
    dag_id='dag_ingest_sofascore',
    default_args=SCRAPER_ARGS,
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

    Per-player profile + season-aggregate stats are NOT here — they run weekly
    in `dag_ingest_sofascore_players` (#751 PR3; ~526 Camoufox navs is too heavy
    to carry daily).

    ### Daily limits (issue #69)

    No per-endpoint cap by default. Override via ENV on dev/staging:
    `SS_SHOTMAP_LIMIT`, `SS_EPS_LIMIT`, `SS_MATCH_STATS_LIMIT` (positive int → cap).

    ### Full-state refresh

    The consolidated `match_capture` rewrites each `(league, season)` partition
    wholesale every run (`replace_partitions=['league','season']` + completeness
    guard) — every finished match is re-captured, so the run is idempotent.

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
rm -f {SCHEDULE_RESULT_PATH} && \\
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

    # #751 PR1 — consolidated per-match capture: ONE Camoufox nav/match writes
    # BOTH bronze.sofascore_player_ratings and bronze.sofascore_event_player_stats
    # from the same /lineups (+/event) payload. Depends on freshly written
    # bronze.sofascore_schedule (runner reads finished match_ids there; falls
    # back to capture discovery when empty). Exit 2 = graceful R0.2B_FALLBACK
    # (soft success so validate_data runs); exit 3 = completeness-guard refusal
    # (propagates as a real failure).
    scrape_match_capture_task = BashOperator(
        task_id='scrape_match_capture',
        bash_command=f"""
cd /opt/airflow && \\
rm -f {MATCH_CAPTURE_RESULT_PATH} && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity match_capture \\
    --league "{LEAGUES[0]}" \\
    --season {CURRENT_SEASON} \\
    --output {MATCH_CAPTURE_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "R0.2B_FALLBACK exit-code 2 (match_capture) — propagating as soft success."
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

    # #751 PR2: shotmap (#22) + match_stats (#25) no longer have their own tls
    # tasks — both come from the consolidated Camoufox capture above (same nav
    # also clicks the Statistics/Shotmap tabs). The dead tls path 403'd silently.
    # #751 PR3: player_season_stats (#24) + player_profile (#23) moved to the
    # weekly dag_ingest_sofascore_players (~526 Camoufox navs too heavy daily).

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        trigger_rule='all_done',
    )

    # Freshness gate over the Bronze tables themselves (issue #751). Runs
    # all_done so a 403 soft-fail upstream still triggers the staleness alert.
    validate_bronze_freshness_task = PythonOperator(
        task_id='validate_bronze_freshness',
        python_callable=validate_bronze_freshness,
        trigger_rule='all_done',
    )

    # schedule → match_capture (ratings + event_player_stats + match_stats +
    # shotmap from ONE nav/match — #751 PR2), then validate_data on all_done.
    # Per-player profile + season_stats run in the weekly player DAG (#751 PR3).
    scrape_data_task >> scrape_match_capture_task
    scrape_match_capture_task >> validate_data_task >> validate_bronze_freshness_task
