"""
Data Validation Utilities
=========================

Common validation functions for DAG tasks.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def validate_scrape_results(
    task_results: Dict[str, Dict[str, Any]],
    min_thresholds: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """
    Validate scraping results from multiple tasks.

    Args:
        task_results: Dictionary mapping task_id to result dict with 'rows' and 'errors'
        min_thresholds: Optional minimum row thresholds per task

    Returns:
        Validation result with status, warnings, and summary
    """
    min_thresholds = min_thresholds or {}

    validation = {
        'status': 'success',
        'warnings': [],
        'errors': [],
        'summary': {},
    }

    total_rows = 0
    all_errors = []

    for task_id, result in task_results.items():
        if result is None:
            validation['warnings'].append(f"No result from {task_id}")
            validation['summary'][task_id] = 0
            continue

        rows = result.get('rows', 0)
        errors = result.get('errors', [])

        validation['summary'][task_id] = rows
        total_rows += rows

        if errors:
            all_errors.extend(errors)

        # Check minimum threshold
        threshold = min_thresholds.get(task_id, 0)
        if rows < threshold:
            validation['warnings'].append(
                f"Low row count for {task_id}: {rows} < {threshold}"
            )

    validation['summary']['total_rows'] = total_rows

    if all_errors:
        validation['errors'] = all_errors
        validation['status'] = 'partial_success' if total_rows > 0 else 'failed'

    return validation


def collect_task_results(
    ti,
    task_ids: List[str],
) -> Dict[str, Dict[str, Any]]:
    """
    Collect XCom results from multiple upstream tasks.

    Args:
        ti: Airflow TaskInstance
        task_ids: List of task IDs to collect from

    Returns:
        Dictionary mapping task_id to result
    """
    results = {}
    for task_id in task_ids:
        try:
            result = ti.xcom_pull(task_ids=task_id)
            results[task_id] = result
        except Exception as e:
            logger.warning(f"Could not pull XCom from {task_id}: {e}")
            results[task_id] = None
    return results


def log_validation_results(validation: Dict[str, Any]) -> None:
    """
    Log validation results in a standardized format.

    Args:
        validation: Validation result dictionary
    """
    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {validation['summary']}")

    if validation.get('warnings'):
        for warning in validation['warnings']:
            logger.warning(f"Warning: {warning}")

    if validation.get('errors'):
        for error in validation['errors']:
            logger.error(f"Error: {error}")


def create_validation_task_callable(
    upstream_task_ids: List[str],
    min_thresholds: Optional[Dict[str, int]] = None,
):
    """
    Create a validation callable for PythonOperator.

    Args:
        upstream_task_ids: List of task IDs to validate
        min_thresholds: Optional minimum row thresholds

    Returns:
        Callable for PythonOperator
    """
    def validate_data(**context) -> Dict[str, Any]:
        ti = context['ti']
        task_results = collect_task_results(ti, upstream_task_ids)
        validation = validate_scrape_results(task_results, min_thresholds)
        log_validation_results(validation)
        return validation

    return validate_data
