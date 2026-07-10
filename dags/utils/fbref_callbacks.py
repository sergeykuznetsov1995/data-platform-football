"""
FBref DAG Callbacks
===================

Callback and callable functions for FBref DAG PythonOperator tasks.

- validate_all_data: Validate all scraped data after TaskGroups complete
"""

from typing import Any, Dict


def validate_all_data(
    result_dir: str = '/tmp',
    manifest_suffixes: list[str] | None = None,
    **context,
) -> Dict[str, Any]:
    """Validate every scope manifest produced by the current DAG run.

    Counting every fbref_*.json in shared /tmp let successful files from an
    older run hide a failed producer. The DAG now supplies a run-scoped
    directory and explicit club/tournament suffixes. Each scope must either
    contain the current core tables or be consistently marked out-of-window.
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
        'missing_tables': [],
        'missing_tables_by_scope': {},
        'skipped_scopes': [],
    }

    required_tables = {
        'fbref_player_stats',
        'fbref_player_shooting',
        'fbref_player_playingtime',
        'fbref_player_misc',
        'fbref_team_stats',
        'fbref_team_shooting',
        'fbref_team_playingtime',
        'fbref_team_misc',
        'fbref_keeper_keeper',
        'fbref_schedule',
        'fbref_match_team_stats',
        'fbref_match_player_stats',
        'fbref_match_events',
        'fbref_lineups',
    }
    suffixes = list(manifest_suffixes or [''])
    result_path = Path(result_dir)

    for suffix in suffixes:
        scope = suffix.lstrip('_') or 'club'
        expected_manifests = (
            f'fbref_season_stats{suffix}.json',
            f'fbref_match_schedule{suffix}.json',
            f'fbref_match_all_data{suffix}.json',
        )
        scope_tables = []
        scope_skip_reasons = []
        readable_manifests = 0

        for filename in expected_manifests:
            result_file = result_path / filename
            if not result_file.exists():
                validation['errors'].append(
                    f'Missing current-run result manifest: {result_file}'
                )
                continue
            try:
                with open(result_file, 'r') as f:
                    result = json.load(f)

                if not isinstance(result, dict):
                    validation['errors'].append(
                        f'{result_file.name}: result is not a JSON object'
                    )
                    continue

                readable_manifests += 1
                tables = result.get('tables', [])
                errors = result.get('errors', [])
                fallbacks = result.get('fallback_files', [])
                skipped = result.get('skipped')

                if tables:
                    scope_tables.extend(tables)
                    validation['tables_collected'].extend(tables)

                if errors:
                    validation['errors'].extend(
                        f'{scope}: {error}' for error in errors
                    )
                if fallbacks:
                    validation['fallback_files'].extend(fallbacks)
                if skipped:
                    scope_skip_reasons.append(skipped)

            except (FileNotFoundError, json.JSONDecodeError) as e:
                validation['errors'].append(
                    f"Error reading {result_file}: {e}"
                )

        if scope_skip_reasons:
            if (
                readable_manifests == len(expected_manifests)
                and scope_skip_reasons == ['out_of_window'] * len(expected_manifests)
            ):
                validation['skipped_scopes'].append(scope)
                validation['missing_tables_by_scope'][scope] = []
                continue
            validation['errors'].append(
                f'{scope}: inconsistent skipped manifests '
                f'{scope_skip_reasons}'
            )

        scope_table_names = {
            str(table).rsplit('.', 1)[-1]
            for table in scope_tables
        }
        scope_missing = sorted(required_tables - scope_table_names)
        validation['missing_tables_by_scope'][scope] = scope_missing
        if scope_missing:
            validation['missing_tables'].extend(
                f'{scope}:{table}' for table in scope_missing
            )
            validation['errors'].append(
                f'{scope}: Missing required tables: {scope_missing}'
            )

    tables_set = {
        str(table).rsplit('.', 1)[-1]
        for table in validation['tables_collected']
    }

    if validation['fallback_files']:
        validation['errors'].append(
            f"{len(validation['fallback_files'])} local fallback file(s) "
            f"were produced instead of Iceberg writes"
        )
    if validation['errors']:
        validation['status'] = 'failed'

    logger.info(f"Validation complete: {validation['status']}")
    logger.info(f"Tables collected: {len(tables_set)}")

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
    - Missing/unreadable JSON is a hard failure: accepting it would let an
      all_done observer hide the producer failure or consume a stale artifact.
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
        raise AirflowException(
            f"Traffic summary not found at {summary_path}. "
            f"Upstream task ({label}) may have failed before writing it."
        )

    try:
        with open(summary_path) as f:
            summary = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        raise AirflowException(
            f"Could not read traffic summary {summary_path}: {e}"
        ) from e

    real_mb = float(summary.get('real_proxy_mb') or 0.0)
    # HTTP fast-path bytes (curl_cffi after CF bypass) go through the SAME
    # paid residential proxy but are tracked separately from the browser's
    # CDP counter. Guarding only real_proxy_mb left them invisible — the
    # threshold now applies to the total.
    http_mb = float(summary.get('http_mb_downloaded') or 0.0)
    total_mb = real_mb + http_mb
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
        ti.xcom_push(key='http_mb', value=http_mb)
        ti.xcom_push(key='total_proxy_mb', value=total_mb)
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
        f"http_mb={http_mb:.2f}, total_mb={total_mb:.2f}, "
        f"requests={requests}, successes={successes}, "
        f"cf_attempts={cf_attempts}/passed={cf_passed}/failed={cf_failed}, "
        f"restarts={dict(restart_reasons)}, threshold={threshold_mb:.2f} MB"
    )

    if total_mb > threshold_mb:
        raise AirflowException(
            f"Proxy traffic {total_mb:.2f} MB (browser {real_mb:.2f} + "
            f"http fast-path {http_mb:.2f}) for {label} exceeded threshold "
            f"{threshold_mb:.2f} MB. Review Airflow Variable "
            f"`{per_task_var}` or `{threshold_variable}`, or investigate "
            f"the run."
        )

    return {
        'status': 'ok',
        'label': label,
        'real_proxy_mb': real_mb,
        'http_mb': http_mb,
        'total_proxy_mb': total_mb,
        'real_proxy_requests': requests,
        'matches_scraped': successes,
        'cf_challenge_attempts': cf_attempts,
        'cf_challenges_passed': cf_passed,
        'cf_challenges_failed': cf_failed,
        'restart_reasons': dict(restart_reasons),
        'real_proxy_mb_by_resource_type': dict(mb_by_rtype),
        'threshold_mb': threshold_mb,
    }


def report_proxy_traffic(
    glob_pattern: str = '/tmp/fbref_traffic_*.json',
    **context,
) -> Dict[str, Any]:
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
        summary = summarize_fbref_traffic(glob_pattern=glob_pattern)
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
