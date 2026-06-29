"""
SofaScore Data Ingestion DAG
============================

Airflow DAG for scraping football statistics from SofaScore.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Schedules daily at 11 AM UTC.

One source = one DAG (#782): the former weekly ``dag_ingest_sofascore_players``
is folded in here as a gated branch (pattern #710 MatchHistory / #716 ClubElo —
parametrize the ingest DAG instead of spawning a second one).

Data collected:
- Match schedule + league table (daily)
- Per-match capture: player_ratings, event_player_stats, match_stats, shotmap (daily)
- Per-player profile + season-aggregate stats (weekly — heavy, gated; see below)

The per-player capture (~526 Camoufox navigations, hours long) is too heavy to
run daily, so a ``ShortCircuitOperator`` gates it: it runs only on the DAG's OWN
Saturday scheduled run, or on a manual "Trigger DAG w/ config" with
``run_players=True``. It is skipped on weekday scheduled runs and whenever an
external trigger (e.g. ``dag_master_pipeline``) fires this DAG, so the daily
pipeline never waits on the heavy player run.

All data is written to Iceberg Bronze layer tables (via Parquet fallback).
"""

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from utils.config import LEAGUES, CURRENT_SEASON, SCHEDULES, DAG_TAGS
from utils.default_args import SCRAPER_ARGS


SCHEDULE_RESULT_PATH = '/tmp/sofascore_result.json'
# #751 PR1+PR2 — one consolidated Camoufox capture per match writes ALL FOUR
# per-match tables: player_ratings, event_player_stats, match_stats, shotmap
# (replaces four separate tls passes).
MATCH_CAPTURE_RESULT_PATH = '/tmp/sofascore_match_capture_result.json'
# #782 — per-player profile + season_stats capture (formerly the weekly
# dag_ingest_sofascore_players) now runs here behind the Saturday/manual gate.
PLAYER_CAPTURE_RESULT_PATH = '/tmp/sofascore_player_capture_result.json'


def _env_int(name: str):
    """Read a positive int from ENV; empty/unparseable/non-positive → None."""
    raw = os.environ.get(name, '').strip()
    if not raw:
        return None
    try:
        v = int(raw)
    except ValueError:
        return None
    return v if v > 0 else None


# Smoke/dev cap for the player capture — None = full coverage (~526 players).
# Issue #69 convention.
PLAYER_CAPTURE_LIMIT = _env_int('SS_PLAYER_CAPTURE_LIMIT')


def _limit_arg(limit) -> str:
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

    # player_season_stats + player_profile are validated by validate_player_data
    # (the gated weekly branch below), not here.

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


# ---------------------------------------------------------------------------
# Per-player capture (#782) — folded from the former weekly players DAG.
# ---------------------------------------------------------------------------

def _gate_player_capture(**context) -> bool:
    """ShortCircuitOperator hook — TRUE means "run the per-player capture".

    The ~526-player capture is heavy (hours, residential proxy). It must NOT
    run on every daily run, nor when ``dag_master_pipeline`` triggers this DAG
    (that would stall the daily pipeline). So it runs only when:

      - a manual "Trigger DAG w/ config" sets ``run_players=True`` (on demand); or
      - this is the DAG's OWN Saturday scheduled run (the weekly cadence the
        former ``dag_ingest_sofascore_players`` had).

    Skipped otherwise (weekday scheduled run, or any external/master trigger).
    Returning False short-circuits the downstream player tasks to ``skipped``.
    """
    import logging

    logger = logging.getLogger(__name__)

    params = context.get('params') or {}
    if params.get('run_players'):
        logger.info("run_players=True → running per-player capture on demand.")
        return True

    dag_run = context.get('dag_run')
    if getattr(dag_run, 'external_trigger', False):
        logger.info(
            "External trigger (e.g. dag_master_pipeline) → skip per-player "
            "capture to keep the daily pipeline fast."
        )
        return False

    logical_date = context.get('logical_date') or context.get('execution_date')
    if logical_date is not None and logical_date.weekday() == 5:  # Saturday
        logger.info("Saturday scheduled run → running weekly per-player capture.")
        return True

    logger.info("Not Saturday and not forced → skip per-player capture.")
    return False


def validate_player_data(**context) -> Dict[str, Any]:
    """Row-floor + fallback validation for the consolidated player capture."""
    import logging

    logger = logging.getLogger(__name__)
    result = _load_result(PLAYER_CAPTURE_RESULT_PATH, logger)

    if not result:
        raise AirflowException(
            f"player_capture results file {PLAYER_CAPTURE_RESULT_PATH} "
            f"missing or unreadable"
        )

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'player_profile_rows': result.get('rows', 0),
            'player_profile_players': result.get('profile_players', 0),
            'player_season_stats_rows': result.get('season_stats_rows', 0),
            'player_season_stats_players': result.get('season_stats_players', 0),
            'fallback': result.get('fallback', False),
            'tables': result.get('tables', []),
        },
    }

    errors: List[str] = result.get('errors', []) or []
    if errors:
        validation['warnings'] = list(errors)
        total_rows = validation['summary']['player_profile_rows']
        validation['status'] = 'partial_success' if total_rows > 0 else 'failed'

    # APL ≈ 526 active players → 1 profile row each. WARN-only floor = 400 (issue
    # #69); a fallback keeps the DAG non-failed (soft).
    rows = validation['summary']['player_profile_rows']
    if rows < 400:
        if validation['summary']['fallback']:
            validation['warnings'].append(
                f"player_profile R0.2B_FALLBACK: rows={rows} "
                f"players={validation['summary']['player_profile_players']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low player_profile row count: {rows} < 400")

    # player_season_stats (#751 PR3b) — a strict subset of profile (the Season
    # picker can miss for transferred/multi-competition players). WARN-only
    # floor: low coverage never fails the run, it just flags a possible picker
    # regression. 300 is a conservative floor below ~526 active APL players.
    season_rows = validation['summary']['player_season_stats_rows']
    if season_rows < 300:
        validation['warnings'].append(
            f"Low player_season_stats row count: {season_rows} < 300 "
            f"(Season-tab picker coverage)")

    logger.info("Player data validation complete: %s", validation['status'])
    logger.info("Summary: %s", validation['summary'])
    if validation['warnings']:
        logger.warning("Warnings: %s", validation['warnings'])

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")
    return validation


def validate_player_freshness(**context) -> None:
    """Hard-fail when the player bronze tables stop refreshing (#751).

    The scrape task soft-exits (R0.2B_FALLBACK, exit 2) when SofaScore's anti-bot
    returns 403, so the DAG stays green while data silently goes stale. A direct
    MAX(_ingested_at) check surfaces a multi-week stall. ERROR-severity: a stale
    table fails the task (the Telegram summary fires first). 8-day window gives
    one missed weekly run of grace.

    Only runs when the gate let the player capture through (Saturday / manual),
    so it never fires on weekday daily runs that skip the player branch.
    """
    import logging

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    logger = logging.getLogger(__name__)

    checks = [
        CHECK.freshness(
            'bronze.sofascore_player_profile',
            ts_col='_ingested_at', max_age_hours=192, severity='ERROR',
        ),
        CHECK.freshness(
            'bronze.sofascore_player_season_stats',
            ts_col='_ingested_at', max_age_hours=192, severity='ERROR',
        ),
    ]
    # raise_on_error=False so the Telegram summary lands before we re-raise on
    # ERROR-severity failures (same pattern as dag_transform_e4).
    report = run_checks(checks, raise_on_error=False)
    logger.info("validate_player_freshness: %s", report.summary())
    telegram_dq_summary(report, header='SofaScore player Bronze freshness')

    if report.errors:
        raise AirflowException(
            f"SofaScore player Bronze freshness failed: {len(report.errors)} error(s). "
            + "; ".join(f"{r.name}: {r.details or r.error}" for r in report.errors)
        )


# Build arguments for bash command
leagues_str = ','.join(LEAGUES)

# DAG definition
with DAG(
    dag_id='dag_ingest_sofascore',
    default_args=SCRAPER_ARGS,
    description='Ingest football statistics from SofaScore (matches daily + players weekly)',
    schedule=SCHEDULES.get('dag_ingest_sofascore', '0 11 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('sofascore', ['scraping', 'sofascore', 'bronze']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        # UI-configurable season for the 10-season backfill (#711, epic #708).
        # Default = CURRENT_SEASON so the daily scheduled run is unchanged;
        # override via "Trigger DAG w/ config" to ingest a past season. The
        # season is the APL start year (2016 = 2016/17); the runner derives the
        # soccerdata short form ("1617") from it.
        'season': Param(
            default=CURRENT_SEASON,
            type='integer',
            minimum=2000,
            maximum=CURRENT_SEASON,
            title='Season (start year)',
            description=(
                'APL season start year (2016 = 2016/17). Default = current '
                'season for the daily run. Override here to backfill a past '
                'season (2016…2024 closes the 10-season history → unblocks '
                'fouls home-vs-away for #558).'
            ),
        ),
        # #782: force the heavy per-player capture in this run. Normally it
        # auto-runs only on the DAG's own Saturday scheduled run (see
        # _gate_player_capture); set True via "Trigger DAG w/ config" for an
        # on-demand refresh. Ignored when dag_master_pipeline triggers this DAG.
        'run_players': False,
    },
    doc_md="""
    ## SofaScore Data Ingestion

    This DAG scrapes football statistics from SofaScore.

    ### Architecture

    Uses BashOperator to run scraper in isolated subprocess,
    preventing LocalExecutor fork memory issues.

    ### Data Collected

    - **Schedule**: Match dates, teams, scores, venues (daily)
    - **Per-match capture**: player_ratings, event_player_stats, match_stats,
      shotmap — ONE Camoufox nav/match (daily)
    - **Per-player**: profile + season-aggregate stats (weekly, gated — see below)

    ### One source = one DAG (#782)

    The former weekly `dag_ingest_sofascore_players` is folded in here. The heavy
    per-player capture (~526 Camoufox navs, hours long) is gated by a
    `ShortCircuitOperator`:

    - auto-runs only on the DAG's **own Saturday scheduled run** (weekly cadence);
    - or on demand via **"Trigger DAG w/ config"** with `run_players=true`;
    - **skipped** on weekday scheduled runs and whenever `dag_master_pipeline`
      triggers this DAG (so the daily pipeline never waits on the heavy run).

    ### Daily limits (issue #69)

    No per-endpoint cap by default. Override via ENV on dev/staging:
    `SS_SHOTMAP_LIMIT`, `SS_EPS_LIMIT`, `SS_MATCH_STATS_LIMIT`,
    `SS_PLAYER_CAPTURE_LIMIT` (positive int → cap).

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
    --season {{{{ params.season }}}} \\
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
    --season {{{{ params.season }}}} \\
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

    # ---- Per-player capture (#782) — gated to Saturday / manual -------------
    # Folded from the former weekly dag_ingest_sofascore_players. The gate
    # short-circuits the player tasks to `skipped` on weekday runs and on
    # external/master triggers (keeps the daily pipeline fast).
    gate_player_capture_task = ShortCircuitOperator(
        task_id='gate_player_capture',
        python_callable=_gate_player_capture,
        # all_done: player ids come from bronze.sofascore_player_ratings (written
        # by match_capture); even a soft-failed match_capture leaves prior rows,
        # so let the gate decide regardless of upstream state.
        trigger_rule='all_done',
    )

    scrape_player_capture_task = BashOperator(
        task_id='scrape_player_capture',
        bash_command=f"""
cd /opt/airflow && \\
rm -f {PLAYER_CAPTURE_RESULT_PATH} && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity player_capture \\
    --league "{LEAGUES[0]}" \\
    --season {{{{ params.season }}}} \\
    {_limit_arg(PLAYER_CAPTURE_LIMIT)} \\
    --output {PLAYER_CAPTURE_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "R0.2B_FALLBACK exit-code 2 (player_capture) — propagating as soft success."
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

    validate_player_data_task = PythonOperator(
        task_id='validate_player_data',
        python_callable=validate_player_data,
        trigger_rule='all_done',
    )

    validate_player_freshness_task = PythonOperator(
        task_id='validate_player_freshness',
        python_callable=validate_player_freshness,
        trigger_rule='all_done',
    )

    # Matches chain (daily): schedule → match_capture → validate → freshness.
    scrape_data_task >> scrape_match_capture_task
    scrape_match_capture_task >> validate_data_task >> validate_bronze_freshness_task

    # Per-player branch (weekly/manual), gated after match_capture so the player
    # ids in bronze.sofascore_player_ratings are fresh; skipped on weekday runs
    # and on master-pipeline triggers.
    scrape_match_capture_task >> gate_player_capture_task >> scrape_player_capture_task
    scrape_player_capture_task >> validate_player_data_task >> validate_player_freshness_task
