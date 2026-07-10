"""
WhoScored Data Ingestion DAG
============================

W3 (Wave 3) — FlareSolverr-backed ingest (see scrapers/whoscored/scraper.py).

Schedule: from ``SCHEDULES['dag_ingest_whoscored']`` (daily 10:00 UTC).
Steady-state runs are cheap: events/lineups are APPEND-only with
skip-existing, so only new matches are fetched.

Fan-out: ONE BashOperator per league in ``utils.config.WHOSCORED_LEAGUES``,
chained sequentially (single FlareSolverr instance — parallel heavy sessions
crash its 512 MB container) with ``trigger_rule='all_done'`` so one league's
crash doesn't block the rest. Each task runs `run_whoscored_scraper.py`
which sequentially calls:

    * scrape_schedule()         — fixtures + integer game_id  (full N seasons)
    * scrape_missing_players()  — pre-match injuries / suspensions
    * scrape_season_stages()    — cup/league stage metadata
    * scrape_events()           — Opta events + lineups/ratings for ALL
                                  configured seasons, skip-existing per match

Validation runs as Trino COUNT(*) tasks against MIN_ROW_THRESHOLDS so a
crash in events doesn't mask a healthy schedule write.
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.bronze_validation import validate_table
from utils.config import (
    DAG_TAGS,
    SCHEDULES,
    SEASONS_STR,
    WHOSCORED_LEAGUES,
)
from utils.default_args import SELENIUM_ARGS
from utils.ingest_helpers import league_slug as _league_slug


# Extended timeout for WhoScored due to Cloudflare + heavy events scrape.
# The timeout is PER league task; a first-time league backfill (~1,900
# matches) that hits it resumes incrementally on retry via skip-existing.
WHOSCORED_ARGS = {
    **SELENIUM_ARGS,
    'execution_timeout': timedelta(hours=6),
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}


def validate_schedule(**context) -> Dict[str, Any]:
    """Hard threshold check for whoscored_schedule (~1900 rows/league).

    #920 Phase 2: per-league floors — every league in WHOSCORED_LEAGUES must
    individually clear its competitions.yaml-derived floor (a missing league
    used to hide behind the whole-table aggregate).
    """
    return validate_table(
        'whoscored_schedule', 'whoscored_schedule', leagues=WHOSCORED_LEAGUES
    )


def validate_events(**context) -> Dict[str, Any]:
    """Hard threshold check for whoscored_events (wipe-floor, see config)."""
    return validate_table('whoscored_events', 'whoscored_events')


def validate_player_profile(**context) -> Dict[str, Any]:
    """Hard threshold check for whoscored_player_profile (~531 players/league, #37)."""
    return validate_table('whoscored_player_profile', 'whoscored_player_profile')


leagues_str = ','.join(WHOSCORED_LEAGUES)

# DAG definition
with DAG(
    dag_id='dag_ingest_whoscored',
    default_args=WHOSCORED_ARGS,
    description='Ingest WhoScored fixtures + Opta events/lineups (per-league fan-out)',
    schedule=SCHEDULES.get('dag_ingest_whoscored', '0 4 * * 1'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('whoscored', ['scraping', 'whoscored', 'bronze', 'selenium', 'spadl']),
    max_active_runs=1,
    params={
        'leagues': WHOSCORED_LEAGUES,
        'seasons': SEASONS_STR,
    },
    doc_md=f"""
    ## WhoScored Data Ingestion (W3)

    FlareSolverr-backed scraper (Cloudflare solved by the FlareSolverr
    service, proxy-less by default — #616). Schedule:
    `{SCHEDULES.get('dag_ingest_whoscored')}`.

    ### Pipeline

    One BashOperator PER league in `WHOSCORED_LEAGUES`, chained sequentially
    (single FlareSolverr instance). Each runs `run_whoscored_scraper.py`:

    1. `scrape_schedule()`         — full {SEASONS_STR} fixtures
    2. `scrape_missing_players()`  — pre-match injuries
    3. `scrape_season_stages()`    — cup/league stages
    4. `scrape_events()`           — Opta events + lineups/ratings, ALL
       configured seasons; skip-existing per match keeps daily runs cheap
       and lets season gaps self-heal (#715)

    ### Validation

    Row counts are checked via Trino COUNT(*) (NOT via JSON output) so a crash
    in `scrape_events` doesn't mask a healthy schedule write:

    * `validate_schedule` — floor scales with len(WHOSCORED_LEAGUES)
    * `validate_events`   — wipe-floor 500k (raise after #708 backfill)
    """,
) as dag:

    # Proxy-less by design (#616, decision 2026-06-18): empty --proxy-file +
    # unset PROXY_FILTER_URL means no residential proxy — FlareSolverr solves
    # Cloudflare itself (probe: 30/30 pages, 0 CF failures). To re-enable a
    # proxy as a fallback, set PROXY_FILTER_URL=http://proxy_filter:8899
    # (ad-tech filter, #652) or pass a non-empty --proxy-file. See
    # docs/research/flaresolverr-proxy-traffic-audit.md.
    _task_env = {
        'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
        'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
        'HOME': '/home/airflow',
    }

    scrape_tasks = []
    for _league in WHOSCORED_LEAGUES:
        _slug = _league_slug(_league)
        _task = BashOperator(
            task_id=f'scrape_whoscored_{_slug}',
            bash_command=(
                'cd /opt/airflow && '
                f'python dags/scripts/run_whoscored_scraper.py '
                f'--leagues "{_league}" '
                f'--seasons "{SEASONS_STR}" '
                f'--proxy-file "" '
                f'--flaresolverr-url http://flaresolverr:8191 '
                f'--output /tmp/whoscored_result_{_slug}.json'
            ),
            env=_task_env,
            append_env=True,
            # a crashed league must not block the remaining leagues
            trigger_rule='all_done' if scrape_tasks else 'all_success',
        )
        if scrape_tasks:
            scrape_tasks[-1] >> _task
        scrape_tasks.append(_task)

    validate_schedule_task = PythonOperator(
        task_id='validate_schedule',
        python_callable=validate_schedule,
        trigger_rule='all_done',
    )

    validate_events_task = PythonOperator(
        task_id='validate_events',
        python_callable=validate_events,
        trigger_rule='all_done',
    )

    # player_profile — biographical /Players/{id} snapshot (issue #37). Runs
    # AFTER the scrape chain because it resolves player_ids from the freshly
    # written bronze.whoscored_events (latest season, per league). One task
    # covers every league: the scraper resolves rosters per league and tags
    # each partition itself. Separate FlareSolverr session; proxy-less.
    scrape_player_profile_task = BashOperator(
        task_id='scrape_player_profile',
        bash_command=(
            'cd /opt/airflow && '
            f'python dags/scripts/run_whoscored_scraper.py '
            f'--player-profile '
            f'--leagues "{leagues_str}" '
            f'--seasons "{SEASONS_STR}" '
            f'--proxy-file "" '
            f'--flaresolverr-url http://flaresolverr:8191 '
            f'--output /tmp/whoscored_player_profile_result.json'
        ),
        env=_task_env,
        append_env=True,
        trigger_rule='all_done',
    )

    validate_player_profile_task = PythonOperator(
        task_id='validate_player_profile',
        python_callable=validate_player_profile,
        trigger_rule='all_done',
    )

    scrape_tasks[-1] >> [validate_schedule_task, validate_events_task]
    scrape_tasks[-1] >> scrape_player_profile_task >> validate_player_profile_task
