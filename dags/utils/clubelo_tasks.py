"""
Task callables of the ClubElo ingestion DAG (``dag_ingest_clubelo``).

They live here rather than in the DAG module — importable for unit tests
without parsing the DAG, and without a cross-DAG import (#488). Kept free of
scraper imports: this module runs in the scheduler's Python, the collection in
``/opt/legacy-scraper-venv``.
"""

from datetime import date, datetime, timezone
from typing import Any, Dict

from airflow.exceptions import AirflowException

# Same numbers as scrapers/clubelo/daily.py (EXPECTED_CLUBS, MIN_COMPLETENESS):
# eloData clubs of /Ranking on 2026-09-22 and the completeness floor.
EXPECTED_CLUBS = 1741
MIN_COMPLETENESS = 0.95


def gate_history(**context) -> bool:
    """ShortCircuit hook of the club-page history branch (#1462).

    TRUE only when a manual "Trigger DAG w/ config" sets ``run_history=True``.
    No calendar, no external-trigger rule: ``dag_master_pipeline`` triggers
    without conf, so the default ``False`` keeps the branch off (R-02, R-61).
    """
    params = context.get('params') or {}
    return bool(params.get('run_history'))


def gate_daily(**context) -> bool:
    """ShortCircuit hook in front of the daily current-ratings task (#1462).

    A manual history run (``run_history=True``) skips the daily chain. Every
    other run (scheduled, ``dag_master_pipeline`` trigger) keeps it.
    """
    params = context.get('params') or {}
    return not params.get('run_history')


def validate_data(results_path: str, **context) -> Dict[str, Any]:
    """Check the result JSON of THIS run's ``scrape_daily`` (#1463).

    ``results_path`` carries the run_id, so another run's file is never read;
    ``fetched_at`` must not be older than this DAG run's start, so a stale
    file left from an earlier try is rejected too. Any violation fails the task
    (no warning-only thresholds).
    """
    import json
    import logging

    logger = logging.getLogger(__name__)

    try:
        with open(results_path, 'r') as f:
            result = json.load(f)
    except FileNotFoundError:
        raise AirflowException(f"Results file not found: {results_path}")
    except json.JSONDecodeError as e:
        raise AirflowException(f"Invalid JSON in {results_path}: {e}")

    problems = []
    for key in ('check', 'error', 'blocked'):
        if result.get(key):
            problems.append(f"{key}: {result[key]}")
    if not result.get('written'):
        problems.append("snapshot not written")
    try:
        date.fromisoformat(str(result.get('rating_date')))
    except ValueError:
        problems.append(f"rating_date is not a date: {result.get('rating_date')!r}")
    try:
        fetched_at = datetime.fromisoformat(str(result.get('fetched_at')))
    except ValueError:
        fetched_at = None
        problems.append(f"fetched_at is not a timestamp: {result.get('fetched_at')!r}")
    dag_run = context.get('dag_run')
    started = getattr(dag_run, 'start_date', None)
    if fetched_at is not None:
        if started is None:
            problems.append("DAG run start is unknown: cannot prove the file is fresh")
        else:
            if started.tzinfo is not None:
                started = started.astimezone(timezone.utc).replace(tzinfo=None)
            if fetched_at < started:
                problems.append(
                    f"stale result: fetched_at {fetched_at.isoformat()} is before "
                    f"this DAG run start {started.isoformat()}"
                )
    rows = result.get('rows') or 0
    if rows < MIN_COMPLETENESS * EXPECTED_CLUBS:
        problems.append(
            f"rows {rows} < {MIN_COMPLETENESS:.0%} of {EXPECTED_CLUBS} clubs"
        )

    summary = {k: result.get(k) for k in (
        'rating_date', 'rows', 'provisional', 'levels_matched_pct', 'results_rows',
        'wire_bytes', 'wire_bytes_daily', 'history_new_fetched', 'same_date')}
    logger.info(f"ClubElo daily result: {summary}")
    if problems:
        raise AirflowException(f"ClubElo daily validation failed: {problems}")
    return {'status': 'success', 'summary': summary}
