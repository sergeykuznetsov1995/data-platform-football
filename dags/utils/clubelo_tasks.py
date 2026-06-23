"""
Shared task callables for the ClubElo ingestion DAG.

``validate_data`` is used by ``dag_ingest_clubelo`` (daily current ratings +
UI-triggered ``mode=full`` historical backfill — #716). It lives here rather
than inline in the DAG file as a leftover of the former two-DAG split
(``dag_ingest_clubelo_full`` was folded into the daily DAG in #716); keeping it
in utils also avoids any cross-DAG import footgun should a second DAG ever
reuse it (#488).
"""

from typing import Any, Dict

from airflow.exceptions import AirflowException


def validate_data(**context) -> Dict[str, Any]:
    """
    Validate scraped data quality.

    Returns:
        Validation results
    """
    import json
    import logging

    logger = logging.getLogger(__name__)

    try:
        with open('/tmp/clubelo_result.json', 'r') as f:
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

    # ClubElo should have ratings for many clubs
    if validation['summary']['ratings_rows'] < 100:
        validation['warnings'].append("Low ratings count - possible scraping issue")

    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {validation['summary']}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")

    return validation
