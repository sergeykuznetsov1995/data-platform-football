"""
FBref DAG Callbacks
===================

Callback and callable functions for FBref DAG PythonOperator tasks.

- validate_all_data: Validate all scraped data after TaskGroups complete
"""

from typing import Any, Dict


def validate_all_data(**context) -> Dict[str, Any]:
    """
    Validate all scraped data from all TaskGroups.

    Checks /tmp/fbref_*.json result files and validates minimum
    data thresholds (at least 12 out of expected ~26 tables).

    Expected tables (26 total):
    - 9 player stats + 9 team stats + 2 keeper stats = 20
    - 6 match data: schedule, shot_events, match_events, lineups,
      match_team_stats, match_player_stats

    Args:
        **context: Airflow context (passed by PythonOperator)

    Returns:
        Validation results dictionary

    Raises:
        AirflowException: If validation fails (no tables collected)
    """
    import json
    import logging
    from pathlib import Path

    from airflow.exceptions import AirflowException

    logger = logging.getLogger(__name__)

    validation = {
        'status': 'success',
        'warnings': [],
        'tables_collected': [],
        'errors': [],
        'fallback_files': [],
        'missing_match_tables': [],
    }

    # Check for fallback JSON files (created when Trino was unavailable during batch save)
    result_dir = Path('/tmp')

    for fallback_file in result_dir.glob('fbref_batch_*.json'):
        validation['fallback_files'].append(str(fallback_file))
        logger.warning(
            f"Fallback JSON detected: {fallback_file.name} — "
            f"data was saved locally because Trino was unavailable during batch save. "
            f"This data needs to be re-ingested into Iceberg."
        )

    if validation['fallback_files']:
        validation['warnings'].append(
            f"{len(validation['fallback_files'])} fallback JSON file(s) found — "
            f"Trino was unavailable during batch save"
        )

    # Check all result files
    for result_file in result_dir.glob('fbref_*.json'):
        # Skip fallback files (already handled above)
        if result_file.name.startswith('fbref_batch_'):
            continue
        try:
            with open(result_file, 'r') as f:
                result = json.load(f)

            if not isinstance(result, dict):
                logger.debug(f"Skipping {result_file.name}: not a result dict")
                continue

            tables = result.get('tables', [])
            errors = result.get('errors', [])

            if tables:
                validation['tables_collected'].extend(tables)

            if errors:
                validation['errors'].extend(errors)
                validation['warnings'].append(
                    f"{result_file.name}: {len(errors)} error(s)"
                )

        except (FileNotFoundError, json.JSONDecodeError) as e:
            validation['warnings'].append(f"Error reading {result_file}: {e}")

    # Check minimum data thresholds
    total_tables = len(validation['tables_collected'])

    # We expect 26 tables in total:
    # - 9 player tables + 9 team tables + 2 keeper tables = 20
    # - 6 match data tables (schedule, shot_events, match_events, lineups,
    #   match_team_stats, match_player_stats)
    # Some may fail — we accept >= 12 as partial success.
    if total_tables == 0:
        validation['status'] = 'failed'
        validation['warnings'].append('No tables were collected')
    elif total_tables < 12:
        validation['status'] = 'partial_success'
        validation['warnings'].append(
            f"Only {total_tables} tables collected (expected ~26)"
        )
    else:
        logger.info(f"Collected {total_tables} tables successfully")

    # Explicit check for the two new match-level tables added in Feb 2026.
    # Silver DAG (fbref_match_enriched.sql) depends on them — missing data
    # here will cascade into CTAS failures downstream.
    required_match_tables = {
        'fbref_match_team_stats',
        'fbref_match_player_stats',
        'fbref_match_events',
        'fbref_lineups',
    }
    tables_set = set(validation['tables_collected'])
    for tbl in required_match_tables:
        if tbl not in tables_set:
            validation['missing_match_tables'].append(tbl)
            validation['warnings'].append(
                f"Missing match table: {tbl} — Silver CTAS may fail"
            )

    if validation['errors']:
        if validation['status'] == 'success':
            validation['status'] = 'partial_success'

    logger.info(f"Validation complete: {validation['status']}")
    logger.info(f"Tables collected: {total_tables}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation}")

    return validation


def check_traffic_guard(
    traffic_path: str = '/tmp/fbref_traffic_match_all_data.json',
    label: str = 'match_all_data',
    threshold_variable: str = 'fbref_proxy_mb_threshold',
    default_threshold_mb: float = 500.0,
    **context,
) -> Dict[str, Any]:
    """Read a per-task traffic summary, push metrics to XCom, and raise when
    real proxy MB exceeds the configured threshold.

    Issue #44: this callable is now parameterized so it can guard ANY task
    in the FBref DAG, not only `match_all_data`. Each task writes its own
    `/tmp/fbref_traffic_<label>.json` and the guard reads it.

    Threshold lookup order:
      1. Airflow Variable ``fbref_proxy_mb_threshold_<label>`` (per-task).
      2. Airflow Variable ``fbref_proxy_mb_threshold`` (global fallback).
      3. ``default_threshold_mb`` argument (500 by default).

    Set via:
        airflow variables set fbref_proxy_mb_threshold_player_stats 60
        airflow variables set fbref_proxy_mb_threshold 800  # global

    Behaviour:
    - Missing JSON file is a warning (task may have failed before writing).
    - Threshold breach raises AirflowException (hard fail — user is paying
      $4/GB, so crossing the budget matters).
    - Uses module-level imports only from airflow + stdlib (no scrapers/
      import — keeps Airflow scheduler process slim per CLAUDE.md).

    Args:
        traffic_path: Path to the per-task traffic JSON.
        label: Short identifier used for per-task Variable lookup and XCom
            keys. Should match the suffix in the JSON filename
            (e.g. `match_all_data`, `match_schedule`, `player_stats`).
        threshold_variable: Name of the global Airflow Variable used as
            fallback when the per-task one is missing.
        default_threshold_mb: Fallback when both Variables are missing.
    """
    import json
    import logging
    from pathlib import Path

    from airflow.exceptions import AirflowException
    from airflow.models import Variable

    logger = logging.getLogger(__name__)

    summary_path = Path(traffic_path)

    if not summary_path.exists():
        logger.warning(
            f"Traffic summary not found at {summary_path}. "
            f"Upstream task ({label}) may have failed before writing it."
        )
        return {'status': 'missing', 'label': label, 'real_proxy_mb': None}

    try:
        with open(summary_path) as f:
            summary = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Could not read traffic summary {summary_path}: {e}")
        return {'status': 'unreadable', 'label': label, 'real_proxy_mb': None}

    real_mb = float(summary.get('real_proxy_mb') or 0.0)
    requests = int(summary.get('real_proxy_requests') or 0)
    # `matches_successes` only exists in combined_match_data summaries;
    # other modes ship `successes` instead. Fall back to either.
    successes = int(
        summary.get('matches_successes')
        or summary.get('successes')
        or 0
    )
    cf_attempts = int(summary.get('cf_challenge_attempts') or 0)
    cf_passed = int(summary.get('cf_challenges_passed') or 0)
    cf_failed = int(summary.get('cf_challenges_failed') or 0)
    restart_reasons = summary.get('restart_reasons') or {}
    mb_by_rtype = summary.get('real_proxy_mb_by_resource_type') or {}

    # Push to XCom so Airflow UI / downstream tasks can read current run cost.
    ti = context.get('ti') or context.get('task_instance')
    if ti is not None:
        ti.xcom_push(key='real_proxy_mb', value=real_mb)
        ti.xcom_push(key='real_proxy_requests', value=requests)
        ti.xcom_push(key='matches_scraped', value=successes)
        ti.xcom_push(key='cf_challenge_attempts', value=cf_attempts)
        ti.xcom_push(key='cf_challenges_passed', value=cf_passed)
        ti.xcom_push(key='cf_challenges_failed', value=cf_failed)
        ti.xcom_push(key='restart_reasons', value=restart_reasons)
        ti.xcom_push(key='real_proxy_mb_by_resource_type', value=mb_by_rtype)

    # Per-task threshold takes precedence over the global one.
    per_task_var = f"{threshold_variable}_{label}"
    raw_threshold = Variable.get(per_task_var, default_var=None)
    if raw_threshold is None:
        raw_threshold = Variable.get(
            threshold_variable, default_var=str(default_threshold_mb)
        )
    try:
        threshold_mb = float(raw_threshold)
    except (ValueError, TypeError):
        threshold_mb = default_threshold_mb

    logger.info(
        f"Traffic guard [{label}]: real_proxy_mb={real_mb:.2f}, "
        f"requests={requests}, successes={successes}, "
        f"cf_attempts={cf_attempts}/passed={cf_passed}/failed={cf_failed}, "
        f"restarts={dict(restart_reasons)}, threshold={threshold_mb:.2f} MB"
    )

    if real_mb > threshold_mb:
        raise AirflowException(
            f"Proxy traffic {real_mb:.2f} MB for {label} exceeded threshold "
            f"{threshold_mb:.2f} MB. Review Airflow Variable "
            f"`{per_task_var}` or `{threshold_variable}`, or investigate "
            f"the run."
        )

    return {
        'status': 'ok',
        'label': label,
        'real_proxy_mb': real_mb,
        'real_proxy_requests': requests,
        'matches_scraped': successes,
        'cf_challenge_attempts': cf_attempts,
        'cf_challenges_passed': cf_passed,
        'cf_challenges_failed': cf_failed,
        'restart_reasons': dict(restart_reasons),
        'real_proxy_mb_by_resource_type': dict(mb_by_rtype),
        'threshold_mb': threshold_mb,
    }


def report_proxy_traffic(**context) -> Dict[str, Any]:
    """Aggregate this run's FBref residential-proxy bytes into one log line (#789).

    Reads the per-task ``/tmp/fbref_traffic_*.json`` files the scraper already
    writes (#44/#624) and logs a single ``PROXY_TRAFFIC source=fbref total=…``
    line so the residential spend (~$4/GB) is visible per run. Passive — it never
    raises (a reporting task must not fail the ingest DAG) and changes neither the
    scrape path nor bronze row-counts.
    """
    import logging

    from utils.proxy_traffic import (
        log_traffic_summary,
        record_traffic_run,
        summarize_fbref_traffic,
    )

    logger = logging.getLogger(__name__)

    try:
        summary = summarize_fbref_traffic()
        log_traffic_summary(summary)
    except Exception as exc:  # noqa: BLE001 — reporting must never fail the DAG
        logger.warning("report_proxy_traffic failed: %s", exc)
        return {'source': 'fbref', 'total_mb': None}

    # Phase 2 (#789): persist this run to iceberg.ops.proxy_traffic_runs so the
    # daily DAG can roll up per-source spend. record_traffic_run never raises.
    dag_run_id = ''
    dag_run = context.get('dag_run')
    if dag_run is not None:
        dag_run_id = getattr(dag_run, 'run_id', '') or ''
    record_traffic_run(summary, dag_run_id=dag_run_id)

    ti = context.get('ti') or context.get('task_instance')
    if ti is not None:
        ti.xcom_push(key='proxy_total_mb', value=summary['total_mb'])

    return summary
