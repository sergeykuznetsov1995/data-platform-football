"""
Shared task callables for the ClubElo ingestion DAG.

One source = one DAG (#716): the former weekly ``dag_ingest_clubelo_full`` is
folded into ``dag_ingest_clubelo`` as a gated branch. ``validate_data`` (daily
current ratings) and ``gate_full_ratings`` (the Sunday/manual ShortCircuit gate
for the heavy historical scrape) both live here rather than in the DAG module —
keeping them importable for unit tests without parsing the DAG, and avoiding the
cross-DAG import that made DagBag drop a duplicate (#488).
"""

from typing import Any, Dict

from airflow.exceptions import AirflowException

RESULTS_PATH = '/tmp/clubelo_result.json'

# Low-rows floor per league: a full league snapshot is ~20 clubs; 15 leaves
# 25% headroom. Scaled by len(LEAGUES) at validation time (sofifa precedent,
# utils/config.py) — the old hard-coded 100 fired on every single-league run.
MIN_ROWS_PER_LEAGUE = 15


def gate_full_ratings(**context) -> bool:
    """ShortCircuitOperator hook — TRUE means "run the heavy historical scrape".

    The historical-ratings scrape is full-state and weekly-sampled (a one-time
    #716 backfill can span ~520 weekly snapshots, ~10 APL seasons), too heavy to
    run on the daily path. It is gated so it runs only when:

      - a manual "Trigger DAG w/ config" sets ``run_full=True`` (on demand —
        used for the deep backfill, usually with ``days_back``/``force_replace``); or
      - this is the DAG's OWN Sunday scheduled run — the weekly cadence the
        former ``dag_ingest_clubelo_full`` had (``0 4 * * 0``).

    Skipped otherwise: weekday scheduled runs, or any external trigger (e.g.
    ``dag_master_pipeline``) — so the daily pipeline never waits on the heavy
    historical scrape. Returning False short-circuits the downstream scrape to
    ``skipped``.
    """
    import logging

    logger = logging.getLogger(__name__)

    params = context.get('params') or {}
    if params.get('run_full'):
        logger.info("run_full=True → running historical scrape on demand.")
        return True

    dag_run = context.get('dag_run')
    if getattr(dag_run, 'external_trigger', False):
        logger.info(
            "External trigger (e.g. dag_master_pipeline) → skip historical "
            "scrape to keep the daily pipeline fast."
        )
        return False

    logical_date = context.get('logical_date') or context.get('execution_date')
    if logical_date is not None and logical_date.weekday() == 6:  # Sunday
        logger.info("Sunday scheduled run → running weekly historical scrape.")
        return True

    logger.info("Not Sunday and not forced → skip historical scrape.")
    return False


def validate_data(**context) -> Dict[str, Any]:
    """
    Validate scraped data quality.

    Returns:
        Validation results
    """
    import json
    import logging

    from utils.config import LEAGUES

    logger = logging.getLogger(__name__)

    try:
        with open(RESULTS_PATH, 'r') as f:
            ratings_result = json.load(f)
    except FileNotFoundError:
        logger.error("Results file not found - scraping may have failed")
        raise AirflowException("Results file not found - scraping failed")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in results: {e}")
        raise AirflowException(f"Invalid JSON in results: {e}")

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'ratings_rows': ratings_result.get('rows', 0),
            'history_rows': ratings_result.get('history_rows', 0),
            'rating_date': ratings_result.get('rating_date'),
            'tables': ratings_result.get('tables', []),
        }
    }

    if ratings_result.get('errors'):
        validation['warnings'] = ratings_result['errors']
        validation['status'] = (
            'partial_success'
            if validation['summary']['ratings_rows'] > 0
            else 'failed'
        )

    # A full league snapshot is ~20 clubs → threshold scales with the number
    # of configured leagues instead of a hard-coded 100 (which fired on every
    # healthy single-league run).
    min_rows = MIN_ROWS_PER_LEAGUE * len(LEAGUES)
    if validation['summary']['ratings_rows'] < min_rows:
        validation['warnings'].append(
            f"Low ratings count - possible scraping issue "
            f"({validation['summary']['ratings_rows']} < {min_rows})"
        )

    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {validation['summary']}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")

    return validation
