"""Airflow callables for the durable FBref pipeline.

The functions keep DAG files declarative and recompute the deterministic
control run UUID instead of passing scheduler-local files or mutable XCom
payloads between workers.
"""

from __future__ import annotations

import json
import logging
from typing import Optional, Sequence

from scrapers.fbref.settings import (
    DEFAULT_BYTE_LIMIT,
    DEFAULT_REQUEST_LIMIT,
    DEFAULT_REQUEST_RESERVATION_BYTES,
    DEFAULT_SHARD_SIZE,
    MIB,
)


logger = logging.getLogger(__name__)


def _pipeline():
    from scrapers.fbref.pipeline import FBrefPipeline

    return FBrefPipeline.from_env()


def _control_store():
    from scrapers.fbref.control import ControlStore

    return ControlStore.from_env()


def _record_control_traffic(summary: dict, *, airflow_run_id: str) -> None:
    """Bridge durable control metrics into the existing daily cost rollup.

    This is deliberately passive.  The control-plane budget and validation
    gates have already run; an unavailable telemetry table must not turn a
    successful, fully validated ingest into a producer failure.
    """

    try:
        from utils.proxy_traffic import log_traffic_summary, record_traffic_run

        total_mb = float(summary.get("bytes_used") or 0) / MIB
        traffic_summary = {
            "source": "fbref",
            "total_mb": round(total_mb, 6),
            # Control stores browser-vs-HTTP and page-kind breakdowns, not a
            # host attribution. Do not mislabel Turnstile asset bytes as
            # fbref.com traffic.
            "top_domains": [],
            "files_read": 0,
        }
        log_traffic_summary(traffic_summary)
        record_traffic_run(
            traffic_summary,
            dag_run_id=str(airflow_run_id),
            replace_existing=True,
        )
    except Exception as exc:  # noqa: BLE001 - telemetry is non-fatal
        logger.warning("FBref proxy-traffic telemetry skipped: %s", exc)


def _settings(
    *,
    run_type: str,
    request_limit=DEFAULT_REQUEST_LIMIT,
    byte_limit_mb=DEFAULT_BYTE_LIMIT // MIB,
    shard_size=DEFAULT_SHARD_SIZE,
    reservation_mb=DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
    domain_interval_seconds=3.0,
    proxy_file: Optional[str] = "/opt/airflow/proxys.txt",
) -> object:
    from scrapers.fbref.pipeline import PipelineSettings

    return PipelineSettings(
        run_type=str(run_type),
        request_limit=int(request_limit),
        byte_limit=int(byte_limit_mb) * MIB,
        shard_size=int(shard_size),
        request_reservation_bytes=int(reservation_mb) * MIB,
        domain_interval_seconds=float(domain_interval_seconds),
        proxy_file=proxy_file,
    )


def _control_run_id(*, airflow_run_id: str, dag_id: str) -> str:
    from scrapers.fbref.control import make_control_run_id

    return make_control_run_id(airflow_run_id, dag_id=dag_id)


def initialize_fbref_run(
    *,
    airflow_run_id: str,
    dag_id: str,
    run_type: str,
    request_limit=DEFAULT_REQUEST_LIMIT,
    byte_limit_mb=DEFAULT_BYTE_LIMIT // MIB,
    shard_size=DEFAULT_SHARD_SIZE,
    reservation_mb=DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
    domain_interval_seconds=3.0,
) -> str:
    settings = _settings(
        run_type=run_type,
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
        reservation_mb=reservation_mb,
        domain_interval_seconds=domain_interval_seconds,
    )
    run_id = _pipeline().initialize_run(
        airflow_run_id=airflow_run_id,
        dag_id=dag_id,
        settings=settings,
    )
    logger.info("FBref control run initialized: %s (%s)", run_id, run_type)
    return run_id


def seed_fbref_competition_index(
    *, airflow_run_id: str, dag_id: str
) -> str:
    run_id = _control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    target_id = _pipeline().seed_competition_index()
    logger.info("FBref run %s seeded %s", run_id, target_id)
    return target_id


def seed_fbref_historical_seasons(
    *,
    airflow_run_id: str,
    dag_id: str,
    request_limit=DEFAULT_REQUEST_LIMIT,
    byte_limit_mb=DEFAULT_BYTE_LIMIT // MIB,
    shard_size=DEFAULT_SHARD_SIZE,
    reservation_mb=DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
) -> dict:
    run_id = _control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    settings = _settings(
        run_type="backfill",
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
        reservation_mb=reservation_mb,
    )
    result = _pipeline().seed_historical_seasons(
        run_id=run_id, settings=settings, limit=int(shard_size)
    )
    logger.info("FBref backfill seed for %s: %s", run_id, result)
    return result


def fetch_fbref_wave(
    *,
    airflow_run_id: str,
    dag_id: str,
    worker_id: str,
    page_kinds: Sequence[str],
    run_type: str,
    request_limit=DEFAULT_REQUEST_LIMIT,
    byte_limit_mb=DEFAULT_BYTE_LIMIT // MIB,
    shard_size=DEFAULT_SHARD_SIZE,
    reservation_mb=DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
    domain_interval_seconds=3.0,
) -> dict:
    settings = _settings(
        run_type=run_type,
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
        reservation_mb=reservation_mb,
        domain_interval_seconds=domain_interval_seconds,
    )
    result = _pipeline().fetch_wave(
        _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id),
        worker_id=worker_id,
        page_kinds=list(page_kinds),
        settings=settings,
    ).as_dict()
    logger.info("FBref fetch wave: %s", json.dumps(result, sort_keys=True))
    return result


def parse_fbref_wave(
    *,
    airflow_run_id: str,
    dag_id: str,
    page_kinds: Sequence[str],
    run_type: str,
    source_control_run_id: Optional[str] = None,
    request_limit=DEFAULT_REQUEST_LIMIT,
    byte_limit_mb=DEFAULT_BYTE_LIMIT // MIB,
    shard_size=DEFAULT_SHARD_SIZE,
    reservation_mb=DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
) -> dict:
    settings = _settings(
        run_type=run_type,
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
        reservation_mb=reservation_mb,
    )
    normalized_source_run_id = (
        None
        if source_control_run_id is None
        or not str(source_control_run_id).strip()
        else str(source_control_run_id).strip()
    )
    if settings.run_type == "replay" and normalized_source_run_id is None:
        raise ValueError("Replay requires source_control_run_id")
    result = _pipeline().parse_wave(
        _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id),
        page_kinds=list(page_kinds),
        settings=settings,
        source_run_id=normalized_source_run_id,
    ).as_dict()
    logger.info("FBref offline parse wave: %s", json.dumps(result, sort_keys=True))
    return result


def validate_fbref_run(
    *,
    airflow_run_id: str,
    dag_id: str,
    source_control_run_id: Optional[str] = None,
) -> dict:
    summary = _pipeline().validate_and_finish(
        _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id),
        replay_source_run_id=(
            None
            if source_control_run_id is None
            or not str(source_control_run_id).strip()
            else str(source_control_run_id).strip()
        ),
    )
    logger.info(
        "FBref validated run: requests=%s bytes=%s targets=%s datasets=%s",
        summary.get("requests_used"),
        summary.get("bytes_used"),
        summary.get("target_counts"),
        summary.get("dataset_validation_counts"),
    )
    logger.info("FBref control metrics: %s", json.dumps(summary, default=str))
    _record_control_traffic(summary, airflow_run_id=airflow_run_id)
    return summary


def abort_fbref_run(
    *,
    airflow_run_id: str,
    dag_id: str,
    error_class: str = "AirflowDagFailure",
    error_message: str = "Airflow DAG reached a terminal failure",
) -> dict:
    """Abort one deterministic control run without constructing data clients."""

    result = _control_store().abort_run(
        _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id),
        error_class=error_class,
        error_message=error_message,
    )
    logger.warning(
        "FBref control run abort result: status=%s released=%s settled=%s",
        result.get("status"),
        result.get("targets_released"),
        result.get("reservations_settled"),
    )
    return result


def fbref_dag_failure_callback(context: dict) -> None:
    """Best-effort state cleanup; task callbacks retain alert ownership."""

    try:
        dag_run = context.get("dag_run")
        dag = context.get("dag")
        task_instance = context.get("task_instance") or context.get("ti")
        airflow_run_id = (
            getattr(dag_run, "run_id", None) or context.get("run_id")
        )
        dag_id = (
            getattr(dag_run, "dag_id", None)
            or getattr(dag, "dag_id", None)
        )
        if not airflow_run_id or not dag_id:
            logger.warning("FBref DAG failure callback lacks run identity")
            return
        task_id = getattr(task_instance, "task_id", None) or "unknown"
        abort_fbref_run(
            airflow_run_id=str(airflow_run_id),
            dag_id=str(dag_id),
            error_class="AirflowDagFailure",
            error_message=f"Airflow DAG failed after task {task_id}",
        )
    except Exception:  # noqa: BLE001 - callbacks must not mask DAG state
        logger.exception("FBref control-run abort callback failed")


__all__ = [
    "abort_fbref_run",
    "fetch_fbref_wave",
    "fbref_dag_failure_callback",
    "initialize_fbref_run",
    "parse_fbref_wave",
    "seed_fbref_competition_index",
    "seed_fbref_historical_seasons",
    "validate_fbref_run",
]
